use sbroad::ir::operator::ConflictStrategy;
use tarantool::auth::AuthDef;
use tarantool::error::{Error as TntError, TarantoolErrorCode as TntErrorCode};
use tarantool::index::FieldType as IndexFieldType;
#[allow(unused_imports)]
use tarantool::index::Metadata as IndexMetadata;
use tarantool::index::Part;
use tarantool::index::{Index, IndexId, IndexIterator, IndexType, IteratorType};
use tarantool::session::UserId;
use tarantool::space::UpdateOps;
use tarantool::space::{FieldType, Space, SpaceId, SpaceType, SystemSpace};
use tarantool::tlua;
use tarantool::tuple::{DecodeOwned, KeyDef, ToTupleBuffer};
use tarantool::tuple::{RawBytes, Tuple};

use crate::catalog::governor_queue::GovernorQueue;
use crate::config::{self, AlterSystemParameters};
use crate::failure_domain::FailureDomain;
use crate::info::PICODATA_VERSION;
use crate::instance::{self, Instance};
use crate::plugin::PluginIdentifier;
use crate::plugin::PluginOp;
use crate::replicaset::Replicaset;
use crate::schema::PluginConfigRecord;
use crate::schema::PluginMigrationRecord;
use crate::schema::PrivilegeType;
use crate::schema::SchemaObjectType;
use crate::schema::ServiceDef;
use crate::schema::ServiceRouteItem;
use crate::schema::ServiceRouteKey;
use crate::schema::{IndexDef, IndexOption, TableDef};
use crate::schema::{PluginDef, INITIAL_SCHEMA_VERSION};
use crate::schema::{PrivilegeDef, RoutineDef, UserDef};
use crate::static_ref;
use crate::storage::snapshot::SnapshotCache;
use crate::system_parameter_name;
use crate::tarantool::box_schema_version;
use crate::tier::Tier;
use crate::tlog;
use crate::traft;
use crate::traft::error::{Error, IdOfInstance};
use crate::traft::op::Dml;
use crate::traft::op::{Ddl, RenameMapping};
use crate::traft::RaftId;
use crate::traft::Result;
use crate::util::Uppercase;

use crate::config::observer::AtomicObserver;
use rmpv::Utf8String;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::ops::RangeInclusive;
use std::rc::Rc;
use std::time::Duration;

pub mod schema;
pub mod snapshot;

/// A helper macro for getting name of the struct field in a type safe phasion.
#[macro_export]
macro_rules! column_name {
    ($record:ty, $name:ident) => {{
        #[allow(dead_code)]
        /// A helper which makes sure that the struct has a field with the given name.
        /// BTW we use the macro from `tarantool` and not from `std::mem`
        /// because rust-analyzer works with our macro but not with the builtin one ¯\_(ツ)_/¯.
        const DUMMY: usize = ::tarantool::offset_of!($record, $name);
        ::std::stringify!($name)
    }};
}

#[inline(always)]
fn space_by_id_unchecked(space_id: SpaceId) -> Space {
    unsafe { Space::from_id_unchecked(space_id) }
}

pub fn space_by_id(space_id: SpaceId) -> tarantool::Result<Space> {
    let sys_space = Space::from(SystemSpace::Space);
    if sys_space.get(&[space_id])?.is_none() {
        tarantool::set_error!(
            tarantool::error::TarantoolErrorCode::NoSuchSpace,
            "no such space #{}",
            space_id
        );
        return Err(tarantool::error::TarantoolError::last().into());
    }

    Ok(space_by_id_unchecked(space_id))
}

pub fn space_by_name(space_name: &str) -> tarantool::Result<Space> {
    let space = Space::find(space_name).ok_or_else(|| {
        tarantool::set_error!(
            tarantool::error::TarantoolErrorCode::NoSuchSpace,
            "no such space \"{}\"",
            space_name
        );
        tarantool::error::TarantoolError::last()
    })?;
    Ok(space)
}

pub fn storage_is_initialized() -> bool {
    // FIXME: this is a quick and dirty sanity check for when we're booting up
    // the instance to help catch some bugs. A better approach would be to
    // develop an upgrade system which verifies the storage against the
    // system_catalog_version from _pico_property, but this should really be a
    // part of the whole upgrade system...
    // See also <https://git.picodata.io/core/picodata/-/issues/961>
    space_by_name(Instances::TABLE_NAME).is_ok()
        && space_by_name(crate::traft::raft_storage::RaftSpaceAccess::SPACE_RAFT_STATE).is_ok()
}

////////////////////////////////////////////////////////////////////////////////
// System tables IDs
////////////////////////////////////////////////////////////////////////////////
/// 512 - _pico_table
/// 513 - _pico_index
/// 514 - _pico_peer_address
/// 515 - _pico_instance
/// 516 - _pico_property
/// 517 - _pico_replicaset
/// 518 - _raft_log
/// 519 - _raft_state
/// 520 - _pico_user
/// 521 - _pico_privilege
/// 522 - _pico_governor_queue
/// 523 - _pico_tier
/// 524 - _pico_routine
/// 525 - AVAILABLE_SPACE_ID
/// 526 - _pico_plugin
/// 527 - _pico_service
/// 528 - _pico_service_route
/// 529 - _pico_plugin_migration
/// 530 - _pico_plugin_config
/// 531 - _pico_db_config
/// 532 - _bucket
/// 533 or first available space id - can be used in tests (testplug)
////////////////////////////////////////////////////////////////////////////////
pub const SYSTEM_TABLES_ID_RANGE: RangeInclusive<u32> = 512..=SPACE_ID_INTERNAL_MAX;

/// The latest system catalog version this version of picodata is aware of.
///
/// If this version of the executable bootstraps a new picodata cluster this
/// will be the value of 'system_catalog_version' field in `_pico_property`.
/// Otherwise this will be the version the cluster is going to be upgrading to.
pub const LATEST_SYSTEM_CATALOG_VERSION: &'static str = "25.3.3";

////////////////////////////////////////////////////////////////////////////////
// Catalog
////////////////////////////////////////////////////////////////////////////////

/// Top level structure to easily access system tables.
#[derive(Debug, Clone)]
pub struct Catalog {
    pub snapshot_cache: Rc<SnapshotCache>,
    // It's ok to lose this information during restart.
    pub login_attempts: Rc<RefCell<HashMap<String, usize>>>,
    pub pico_table: PicoTable,
    pub indexes: Indexes,
    pub peer_addresses: PeerAddresses,
    pub instances: Instances,
    pub properties: Properties,
    pub replicasets: Replicasets,
    pub users: Users,
    pub privileges: Privileges,
    pub tiers: Tiers,
    pub routines: Routines,
    pub plugins: Plugins,
    pub services: Services,
    pub service_route_table: ServiceRouteTable,
    pub plugin_migrations: PluginMigrations,
    pub plugin_config: PluginConfig,
    pub db_config: DbConfig,
    pub governor_queue: GovernorQueue,
}

/// Id of system table `_bucket`. Note that we don't add in to `Clusterwide`
/// because we don't control how it's created. Instead we use vshard to create
/// it.
///
/// The id is reserved because it must be the same on all instances.
pub const TABLE_ID_BUCKET: SpaceId = 532;

impl Catalog {
    /// Initialize the clusterwide storage.
    ///
    /// This function is private because it should only be called once
    /// per picodata instance on boot.
    #[inline(always)]
    fn initialize() -> tarantool::Result<Self> {
        // SAFETY: safe as long as only called from tx thread.
        static mut WAS_CALLED: bool = false;
        unsafe {
            assert!(!WAS_CALLED, "Catalog must only be initialized once");
            WAS_CALLED = true;
        }

        Ok(Self {
            pico_table: PicoTable::new()?,
            indexes: Indexes::new()?,
            peer_addresses: PeerAddresses::new()?,
            instances: Instances::new()?,
            properties: Properties::new()?,
            replicasets: Replicasets::new()?,
            users: Users::new()?,
            privileges: Privileges::new()?,
            tiers: Tiers::new()?,
            routines: Routines::new()?,
            plugins: Plugins::new()?,
            services: Services::new()?,
            service_route_table: ServiceRouteTable::new()?,
            plugin_migrations: PluginMigrations::new()?,
            plugin_config: PluginConfig::new()?,
            db_config: DbConfig::new()?,
            governor_queue: GovernorQueue::new()?,
            snapshot_cache: Default::default(),
            login_attempts: Default::default(),
        })
    }

    pub fn system_space_name_by_id(id: SpaceId) -> Option<&'static str> {
        match id {
            PicoTable::TABLE_ID => Some(PicoTable::TABLE_NAME),
            Indexes::TABLE_ID => Some(Indexes::TABLE_NAME),
            PeerAddresses::TABLE_ID => Some(PeerAddresses::TABLE_NAME),
            Instances::TABLE_ID => Some(Instances::TABLE_NAME),
            Properties::TABLE_ID => Some(Properties::TABLE_NAME),
            Replicasets::TABLE_ID => Some(Replicasets::TABLE_NAME),
            Users::TABLE_ID => Some(Users::TABLE_NAME),
            Privileges::TABLE_ID => Some(Privileges::TABLE_NAME),
            Tiers::TABLE_ID => Some(Tiers::TABLE_NAME),
            Routines::TABLE_ID => Some(Routines::TABLE_NAME),
            Plugins::TABLE_ID => Some(Plugins::TABLE_NAME),
            Services::TABLE_ID => Some(Services::TABLE_NAME),
            ServiceRouteTable::TABLE_ID => Some(ServiceRouteTable::TABLE_NAME),
            PluginMigrations::TABLE_ID => Some(PluginMigrations::TABLE_NAME),
            PluginConfig::TABLE_ID => Some(PluginConfig::TABLE_NAME),
            DbConfig::TABLE_ID => Some(DbConfig::TABLE_NAME),
            GovernorQueue::TABLE_ID => Some(GovernorQueue::TABLE_NAME),
            _ => None,
        }
    }

    fn global_table_name(&self, id: SpaceId) -> Result<Cow<'static, str>> {
        let Some(table_def) = self.pico_table.get(id)? else {
            return Err(Error::other(format!("global space #{id} not found")));
        };
        Ok(table_def.name.into())
    }

    /// Get a reference to a global instance of clusterwide storage.
    ///
    /// If `init` is true, this will
    /// - create a global variable `STORAGE`, which may later be accessed by
    ///   calling [`Self::try_get`]`(false)`
    /// - create the system tables in the tarantool's storage engine if they are
    ///   not yet created
    ///
    /// Calling this with `init = true` should only be done at instance
    /// initialization in [`crate::bootstrap_storage_on_master`] and
    /// [`crate::get_initialized_storage`].
    ///
    /// Returns an error
    ///   - if `init` is `false` and storage is not initialized.
    ///   - if `init` is `true` and storage initialization failed.
    #[inline]
    pub fn try_get(init: bool) -> Result<&'static Self> {
        // It is trivial to keep track of accesses to this variable, because
        // it can only be accessed via this function
        static mut STORAGE: Option<Catalog> = None;

        // SAFETY: this is safe as long as we only use it in tx thread.
        unsafe {
            if static_ref!(const STORAGE).is_none() {
                if !init {
                    return Err(Error::Uninitialized);
                }
                STORAGE = Some(Self::initialize()?);
            }
            Ok(static_ref!(const STORAGE).as_ref().unwrap())
        }
    }

    /// Get a reference to a global instance of clusterwide storage.
    ///
    /// # Panicking
    /// Will panic if storage is not initialized before a call to this function.
    /// Consider using [`Catalog::try_get`] if this is a problem.
    #[inline(always)]
    pub fn get() -> &'static Self {
        Self::try_get(false).expect("shouldn't be calling this until it's initialized")
    }

    /// Get an instance of clusterwide storage for use in unit tests.
    ///
    /// Should only be used in tests.
    pub(crate) fn for_tests() -> Self {
        let storage = Self::try_get(true).unwrap();
        storage.governor_queue.create_space().unwrap();

        if storage.pico_table.space.len().unwrap() != 0 {
            // Already initialized by other tests.
            return storage.clone();
        }

        // Add system tables
        for (table, index_defs) in crate::schema::system_table_definitions() {
            storage.pico_table.put(&table).unwrap();
            for index in index_defs {
                storage.indexes.put(&index).unwrap();
            }
        }

        storage.clone()
    }

    /// Perform the `dml` operation on the local storage.
    /// When possible, return the new tuple produced by the operation:
    ///   * `Some(tuple)` in case of insert (except for on conflict do nothing) and replace;
    ///   * `Some(tuple)` or `None` depending on update's result (it may be NOP);
    ///   * `None` in case of delete (because the tuple is gone) and 'insert on conflict do nothing'.
    #[inline]
    pub fn do_dml(&self, dml: &Dml) -> tarantool::Result<Option<Tuple>> {
        let space = space_by_id_unchecked(dml.space());
        use ConflictStrategy::*;
        match dml {
            Dml::Insert {
                tuple,
                conflict_strategy: DoFail,
                ..
            } => space.insert(tuple).map(Some),
            Dml::Insert {
                tuple,
                conflict_strategy: DoReplace,
                ..
            } => space.replace(tuple).map(Some),
            Dml::Insert {
                tuple,
                conflict_strategy: DoNothing,
                ..
            } => {
                let index = space.primary_key();
                let metadata = index.meta()?;
                let key_def = metadata.to_key_def();
                let key = key_def.extract_key(&Tuple::from(&tuple.to_tuple_buffer()?))?;
                match space.get(&key)? {
                    Some(_) => Ok(None),
                    None => space.replace(tuple).map(Some),
                }
            }
            Dml::Replace { tuple, .. } => space.replace(tuple).map(Some),
            Dml::Update { key, ops, .. } => space.update(key, ops),
            Dml::Delete { key, .. } => space.delete(key).map(|_| None),
        }
    }
}

/// Return a `KeyDef` to be used for comparing **tuples** of the corresponding global table.
///
/// The return value is cached for system tables.
#[inline(always)]
pub fn cached_key_def(space_id: SpaceId, index_id: IndexId) -> tarantool::Result<Rc<KeyDef>> {
    cached_key_def_impl(space_id, index_id, KeyDefKind::Regular)
}

/// Return a `KeyDef` to be used for comparing **keys** of the corresponding global table.
///
/// The return value is cached for system tables.
#[inline(always)]
pub fn cached_key_def_for_key(
    space_id: SpaceId,
    index_id: IndexId,
) -> tarantool::Result<Rc<KeyDef>> {
    cached_key_def_impl(space_id, index_id, KeyDefKind::ForKey)
}

fn cached_key_def_impl(
    space_id: SpaceId,
    index_id: IndexId,
    kind: KeyDefKind,
) -> tarantool::Result<Rc<KeyDef>> {
    // We borrow global variables here. It's only safe as long as we don't yield from here.
    #[cfg(debug_assertions)]
    let _guard = crate::util::NoYieldsGuard::new();

    let id = (space_id, index_id, kind);

    static mut SYSTEM_KEY_DEFS: Option<KeyDefCache> = None;
    // SAFETY: it is perfectly safe to make a &'static mut in this case, because
    // it only lives for a short time during this function call which is only
    // called from main thread, and we know for a fact that the fiber doesn't
    // yield while holding this reference, because of the NoYieldsGuard above.
    let system_key_defs: &'static mut _ = unsafe { static_ref!(mut SYSTEM_KEY_DEFS) };
    let system_key_defs = system_key_defs.get_or_insert_with(HashMap::new);
    if SYSTEM_TABLES_ID_RANGE.contains(&space_id) {
        // System table definition's never change during a single
        // execution, so it's safe to cache these
        let key_def = get_or_create_key_def(system_key_defs, id)?;
        return Ok(key_def);
    }

    static mut USER_KEY_DEFS: Option<(u64, KeyDefCache)> = None;
    // SAFETY: it is perfectly safe to make a &'static mut in this case, because
    // it only lives for a short time during this function call which is only
    // called from main thread, and we know for a fact that the fiber doesn't
    // yield while holding this reference, because of the NoYieldsGuard above.
    let user_key_defs: &'static mut _ = unsafe { static_ref!(mut USER_KEY_DEFS) };
    let (schema_version, user_key_defs) = user_key_defs.get_or_insert_with(|| (0, HashMap::new()));
    let box_schema_version = box_schema_version();
    if *schema_version != box_schema_version {
        user_key_defs.clear();
        *schema_version = box_schema_version;
    }

    let key_def = get_or_create_key_def(user_key_defs, id)?;
    Ok(key_def)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum KeyDefKind {
    /// [`IndexMetadata::to_key_def_for_key`] should be called.
    ///
    /// This kind of key def is used for comparing the keys to each other via
    /// [`KeyDef::compare`], not the tuples, nor keys to the tuples.
    ///
    /// The difference is that positions of key parts are different when the
    /// parts are in a key vs when they're in a tuple.
    ForKey,
    /// [`IndexMetadata::to_key_def`] should be called.
    ///
    /// This kind of key def is used for comparing the tuples to each other via
    /// [`KeyDef::compare`], or keys to tuples via [`KeyDef::compare_with_key`].
    Regular,
}

type KeyDefCache = HashMap<KeyDefId, Rc<KeyDef>>;
type KeyDefId = (SpaceId, IndexId, KeyDefKind);

fn get_or_create_key_def(cache: &mut KeyDefCache, id: KeyDefId) -> tarantool::Result<Rc<KeyDef>> {
    match cache.entry(id) {
        Entry::Occupied(o) => {
            let key_def = o.get().clone();
            Ok(key_def)
        }
        Entry::Vacant(v) => {
            let (table_id, index_id, kind) = id;
            // Safety: this is always safe actually, we just made this function unsafe for no good reason
            let index = unsafe { Index::from_ids_unchecked(table_id, index_id) };
            let index_metadata = index.meta()?;
            let key_def = match kind {
                KeyDefKind::ForKey => index_metadata.to_key_def_for_key(),
                KeyDefKind::Regular => index_metadata.to_key_def(),
            };
            let key_def = Rc::new(key_def);
            v.insert(key_def.clone());
            Ok(key_def)
        }
    }
}

/// Types implementing this trait represent system tables/spaces.
pub trait SystemTable {
    const TABLE_NAME: &'static str;
    const TABLE_ID: SpaceId;
    const DESCRIPTION: &'static str = "";

    fn format() -> Vec<tarantool::space::Field>;

    fn index_definitions() -> Vec<crate::schema::IndexDef>;
}

////////////////////////////////////////////////////////////////////////////////
// PicoTable
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing contents of `_pico_table` system table.
/// `_pico_table` contains definitions of all picodata tables.
#[derive(Debug, Clone)]
pub struct PicoTable {
    pub space: Space,
    pub index_name: Index,
    pub index_id: Index,
    pub index_owner_id: Index,
}

impl SystemTable for PicoTable {
    const TABLE_NAME: &'static str = "_pico_table";
    const TABLE_ID: SpaceId = 512;
    const DESCRIPTION: &'static str = "Stores metadata of all the cluster tables in picodata.";

    fn format() -> Vec<tarantool::space::Field> {
        TableDef::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![
            IndexDef {
                table_id: Self::TABLE_ID,
                // Primary index
                id: 0,
                name: "_pico_table_id".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![Part::from(("id", IndexFieldType::Unsigned)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: "_pico_table_name".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![Part::from(("name", IndexFieldType::String)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 2,
                name: "_pico_table_owner_id".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(false)],
                parts: vec![Part::from(("owner", IndexFieldType::Unsigned)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
        ]
    }
}

impl PicoTable {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index_id = space
            .index_builder("_pico_table_id")
            .unique(true)
            .part("id")
            .if_not_exists(true)
            .create()?;

        let index_name = space
            .index_builder("_pico_table_name")
            .unique(true)
            .part("name")
            .if_not_exists(true)
            .create()?;

        let index_owner_id = space
            .index_builder("_pico_table_owner_id")
            .unique(false)
            .part("owner")
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space,
            index_id,
            index_name,
            index_owner_id,
        })
    }

    #[inline(always)]
    pub fn rename(&self, id: u32, new_name: &str) -> tarantool::Result<()> {
        // We can't use UpdateOps as we use custom encoding
        let mut table_def = self.get(id)?.expect("should exist");
        table_def.name = new_name.to_string();
        self.put(&table_def)?;
        Ok(())
    }

    #[inline]
    pub fn get(&self, id: SpaceId) -> tarantool::Result<Option<TableDef>> {
        use ::tarantool::msgpack;

        let Some(tuple) = self.space.get(&[id])? else {
            return Ok(None);
        };
        let buf = tuple.to_vec();
        let table_def = msgpack::decode(&buf)?;
        Ok(Some(table_def))
    }

    #[inline]
    pub fn put(&self, table_def: &TableDef) -> tarantool::Result<()> {
        use ::tarantool::msgpack;

        self.space
            .replace(RawBytes::new(&msgpack::encode(table_def)))?;
        Ok(())
    }

    #[inline]
    pub fn insert(&self, table_def: &TableDef) -> tarantool::Result<()> {
        use ::tarantool::msgpack;

        self.space
            .insert(RawBytes::new(&msgpack::encode(table_def)))?;
        Ok(())
    }

    #[inline]
    pub fn update_operable(&self, id: SpaceId, operable: bool) -> tarantool::Result<()> {
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(column_name!(TableDef, operable), operable)?;
        self.space.update(&[id], ops)?;
        Ok(())
    }

    #[inline]
    pub fn update_format(
        &self,
        id: SpaceId,
        format: &[tarantool::space::Field],
        column_renames: &RenameMapping,
    ) -> tarantool::Result<()> {
        // We can't use UpdateOps as we use custom encoding
        let mut table_def = self.get(id)?.expect("should exist");
        // apply renames to the distribution
        column_renames.transform_distribution_columns(&mut table_def.distribution);
        table_def.format = format.to_vec();
        self.put(&table_def)?;
        Ok(())
    }

    /// Change the field value of `schema_version` in the metadata of the specified table
    ///
    /// Changing the `schema_version` is necessary when changing the schema of the table to
    /// invalidate various caches, like the prepared statement cache.
    ///
    /// # Usage Example
    ///
    /// ```no_run
    /// # use tarantool::space::SpaceId;
    /// let tables = picodata::storage::PicoTable::new().unwrap();
    ///
    /// let table_id = 28;
    /// let new_schema_version = 42;
    ///
    /// tables
    ///     .update_schema_version(table_id, new_schema_version)
    ///     .expect("updating schema_version failed");
    /// ```
    #[inline]
    pub fn update_schema_version(&self, id: SpaceId, schema_version: u64) -> tarantool::Result<()> {
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(column_name!(TableDef, schema_version), schema_version)?;
        self.space.update(&[id], ops)?;
        Ok(())
    }

    #[inline]
    pub fn delete(&self, id: SpaceId) -> tarantool::Result<Option<Tuple>> {
        self.space.delete(&[id])
    }

    #[inline]
    pub fn by_name(&self, name: &str) -> tarantool::Result<Option<TableDef>> {
        use ::tarantool::msgpack;

        let tuple = self.index_name.get(&[name])?;
        if let Some(tuple) = tuple {
            let table_def = msgpack::decode(&tuple.to_vec())?;
            Ok(Some(table_def))
        } else {
            Ok(None)
        }
    }

    pub fn by_owner_id(
        &self,
        owner_id: UserId,
    ) -> tarantool::Result<EntryIter<TableDef, MP_CUSTOM>> {
        let iter = self.index_owner_id.select(IteratorType::Eq, &[owner_id])?;
        Ok(EntryIter::new(iter))
    }
}

impl ToEntryIter<MP_CUSTOM> for PicoTable {
    type Entry = TableDef;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// Indexes
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing definitions of all picodata indexes.
#[derive(Debug, Clone)]
pub struct Indexes {
    pub space: Space,
    pub index_id: Index,
    pub index_name: Index,
}

impl SystemTable for Indexes {
    const TABLE_NAME: &'static str = "_pico_index";
    const TABLE_ID: SpaceId = 513;

    fn format() -> Vec<tarantool::space::Field> {
        IndexDef::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![
            IndexDef {
                table_id: Self::TABLE_ID,
                // Primary index
                id: 0,
                name: "_pico_index_id".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![
                    Part::from(("table_id", IndexFieldType::Unsigned)).is_nullable(false),
                    Part::from(("id", IndexFieldType::Unsigned)).is_nullable(false),
                ],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: "_pico_index_name".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![Part::from(("name", IndexFieldType::String)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
        ]
    }
}

impl Indexes {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index_id = space
            .index_builder("_pico_index_id")
            .unique(true)
            .part("table_id")
            .part("id")
            .if_not_exists(true)
            .create()?;

        let index_name = space
            .index_builder("_pico_index_name")
            .unique(true)
            .part("name")
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space,
            index_id,
            index_name,
        })
    }

    #[inline]
    pub fn get(&self, space_id: SpaceId, index_id: IndexId) -> tarantool::Result<Option<IndexDef>> {
        let tuple = self.space.get(&(space_id, index_id))?;
        tuple.as_ref().map(Tuple::decode).transpose()
    }

    #[inline]
    pub fn put(&self, index_def: &IndexDef) -> tarantool::Result<()> {
        self.space.replace(index_def)?;
        Ok(())
    }

    #[inline]
    pub fn insert(&self, index_def: &IndexDef) -> tarantool::Result<()> {
        self.space.insert(index_def)?;
        Ok(())
    }

    #[inline]
    pub fn update_operable(
        &self,
        space_id: SpaceId,
        index_id: IndexId,
        operable: bool,
    ) -> tarantool::Result<()> {
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(column_name!(IndexDef, operable), operable)?;
        self.space.update(&(space_id, index_id), ops)?;
        Ok(())
    }

    #[inline]
    pub fn delete(&self, space_id: SpaceId, index_id: IndexId) -> tarantool::Result<Option<Tuple>> {
        self.space.delete(&[space_id, index_id])
    }

    #[inline]
    pub fn by_name(&self, name: &str) -> tarantool::Result<Option<IndexDef>> {
        let tuple = self.index_name.get(&[name])?;
        tuple.as_ref().map(Tuple::decode).transpose()
    }

    #[inline]
    pub fn by_space_id(
        &self,
        space_id: SpaceId,
    ) -> tarantool::Result<EntryIter<IndexDef, MP_SERDE>> {
        let iter = self.space.select(IteratorType::Eq, &[space_id])?;
        Ok(EntryIter::new(iter))
    }
}

impl ToEntryIter<MP_SERDE> for Indexes {
    type Entry = IndexDef;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// PeerAddresses
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing storage of peer addresses.
#[derive(Debug, Clone)]
pub struct PeerAddresses {
    pub space: Space,
    pub index: Index,
}

impl SystemTable for PeerAddresses {
    const TABLE_NAME: &'static str = "_pico_peer_address";
    const TABLE_ID: SpaceId = 514;

    fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("raft_id", FieldType::Unsigned)),
            Field::from(("address", FieldType::String)),
            Field::from(("connection_type", FieldType::String)),
        ]
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            // Primary index
            id: 0,
            name: "_pico_peer_address_raft_id".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![
                Part::from(("raft_id", IndexFieldType::Unsigned)).is_nullable(false),
                Part::from(("connection_type", IndexFieldType::String)).is_nullable(false),
            ],
            operable: true,
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
        }]
    }
}

impl PeerAddresses {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index = space
            .index_builder("_pico_peer_address_raft_id")
            .unique(true)
            .part("raft_id")
            .part("connection_type")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, index })
    }

    #[inline]
    pub fn put(
        &self,
        raft_id: RaftId,
        address: &traft::Address,
        connection_type: &traft::ConnectionType,
    ) -> tarantool::Result<()> {
        self.space.replace(&(raft_id, address, connection_type))?;
        Ok(())
    }

    #[allow(dead_code)]
    #[inline]
    pub fn delete(
        &self,
        raft_id: RaftId,
        connection_type: &traft::ConnectionType,
    ) -> tarantool::Result<()> {
        self.space.delete(&(raft_id, connection_type))?;
        Ok(())
    }

    #[inline(always)]
    pub fn get(
        &self,
        raft_id: RaftId,
        connection_type: &traft::ConnectionType,
    ) -> Result<Option<traft::Address>> {
        let Some(tuple) = self.space.get(&(raft_id, connection_type))? else {
            return Ok(None);
        };
        tuple.field(1).map_err(Into::into)
    }

    #[inline(always)]
    pub fn try_get(
        &self,
        raft_id: RaftId,
        connection_type: &traft::ConnectionType,
    ) -> Result<traft::Address> {
        self.get(raft_id, connection_type)?
            .ok_or(Error::AddressUnknownForRaftId(raft_id))
    }

    #[inline]
    pub fn addresses_by_ids(
        &self,
        ids: impl IntoIterator<Item = RaftId>,
    ) -> Result<HashSet<traft::Address>> {
        ids.into_iter()
            .map(|id| self.try_get(id, &traft::ConnectionType::Iproto))
            .collect()
    }
}

impl ToEntryIter<MP_SERDE> for PeerAddresses {
    type Entry = traft::PeerAddress;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// PropertyName
////////////////////////////////////////////////////////////////////////////////

::tarantool::define_str_enum! {
    /// An enumeration of [`SystemTable::Property`] key names.
    pub enum PropertyName {
        /// Pending ddl operation which is to be either committed or aborted.
        ///
        /// Is only present during the time between the last ddl prepare
        /// operation and the corresponding ddl commit or abort operation.
        PendingSchemaChange = "pending_schema_change",

        /// Schema version of the pending ddl prepare operation.
        /// This will be the current schema version if the next entry is a ddl
        /// commit operation.
        ///
        /// Is only present during the time between the last ddl prepare
        /// operation and the corresponding ddl commit or abort operation.
        PendingSchemaVersion = "pending_schema_version",

        /// Current schema version. Increases with every ddl commit operation.
        ///
        /// This is equal to [`local_schema_version`] during the time between
        /// the last ddl commit or abort operation and the next ddl prepare
        /// operation.
        GlobalSchemaVersion = "global_schema_version",

        /// Schema version which should be used for the next ddl prepare
        /// operation. This increases with every ddl prepare operation in the
        /// log no matter if it is committed or aborted.
        /// This guards us from some painfull corner cases.
        NextSchemaVersion = "next_schema_version",

        /// Pending operation on the plugin system.
        ///
        /// See [`PluginOp`].
        PendingPluginOperation = "pending_plugin_operation",

        /// Current system catalog version.
        ///
        /// Used for checking compatibility and upgrading system tables
        /// between picodata versions.
        SystemCatalogVersion = "system_catalog_version",

        /// Current cluster version.
        ///
        /// The lowest version number among all instances currently in the cluster.
        /// Instance versions are stored in the `_pico_instance` table under the `picodata_version` column.
        ///
        /// Note that picodata only supports clusters of instances with 2 successive versions.
        /// Therefore, each instance's version is either equal to `cluster_version` or `cluster_version + 1`.
        ClusterVersion = "cluster_version",

        /// The id of the governor operation which is currently being applied.
        ///
        /// This is id from the `_pico_governor_queue` table.
        /// Used to track the progress of the DDL operations.
        PendingGovernorOpId = "pending_governor_operation_id",

        /// Pending system catalog version.
        ///
        /// This indicates that all nodes in the cluster have been updated
        /// to the new cluster version, and the system catalog now needs
        /// to be upgraded to the pending version (`PendingCatalogVersion`).
        PendingCatalogVersion = "pending_catalog_version",
    }
}

impl PropertyName {
    /// Returns `true` if this property cannot be deleted. These are usually the
    /// values only updated by picodata and some subsystems rely on them always
    /// being set (or not being unset).
    #[inline(always)]
    fn must_not_delete(&self) -> bool {
        matches!(
            self,
            Self::GlobalSchemaVersion | Self::NextSchemaVersion | Self::ClusterVersion
        )
    }

    /// Verify type of the property value being inserted (or replaced) in the
    /// _pico_property table. `self` is the first field of the `new` tuple.
    #[inline]
    fn verify_new_tuple(&self, new: &Tuple) -> Result<()> {
        // TODO: some of these properties are only supposed to be updated by
        // picodata. Maybe for these properties we should check the effective
        // user id and if it's not admin we deny the change. We'll have to set
        // effective user id to the one who requested the dml in proc_cas_v2.

        let map_err = |e: TntError| -> Error {
            // Make the msgpack decoding error message a little bit more human readable.
            match e {
                TntError::Decode { error, .. } => {
                    Error::other(format!("incorrect type of property {self}: {error}"))
                }
                e => e.into(),
            }
        };

        match self {
            #[rustfmt::skip]
            Self::NextSchemaVersion
            | Self::PendingSchemaVersion
            | Self::GlobalSchemaVersion
            | Self::PendingGovernorOpId => {
                // Check it's an unsigned integer.
                _ = new.field::<u64>(1).map_err(map_err)?;
            }
            Self::PendingSchemaChange => {
                // Check it decodes into Ddl.
                _ = new.field::<Ddl>(1).map_err(map_err)?;
            }
            Self::PendingPluginOperation => {
                // Check it decodes into `PluginOp`.
                _ = new.field::<PluginOp>(1).map_err(map_err)?;
            }
            Self::ClusterVersion | Self::SystemCatalogVersion | Self::PendingCatalogVersion => {
                // Check it's a String
                _ = new.field::<String>(1).map_err(map_err)?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////
// Instance
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing information about all the cluster instances.
#[derive(Debug, Clone)]
pub struct Instances {
    pub space: Space,
    pub index_instance_name: Index,
    pub index_instance_uuid: Index,
    pub index_raft_id: Index,
    pub index_replicaset_name: Index,
}

impl SystemTable for Instances {
    const TABLE_NAME: &'static str = "_pico_instance";
    const TABLE_ID: SpaceId = 515;

    fn format() -> Vec<tarantool::space::Field> {
        Instance::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![
            IndexDef {
                table_id: Self::TABLE_ID,
                // Primary index
                id: 0,
                name: "_pico_instance_name".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![Part::from(("name", IndexFieldType::String)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: "_pico_instance_uuid".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![Part::from(("uuid", IndexFieldType::String)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 2,
                name: "_pico_instance_raft_id".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![Part::from(("raft_id", IndexFieldType::Unsigned)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 3,
                name: "_pico_instance_replicaset_name".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(false)],
                parts: vec![
                    Part::from(("replicaset_name", IndexFieldType::String)).is_nullable(false)
                ],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
        ]
    }
}

impl Instances {
    pub fn new() -> tarantool::Result<Self> {
        let space_instances = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index_instance_name = space_instances
            .index_builder("_pico_instance_name")
            .unique(true)
            .part("name")
            .if_not_exists(true)
            .create()?;

        let index_instance_uuid = space_instances
            .index_builder("_pico_instance_uuid")
            .unique(true)
            .part("uuid")
            .if_not_exists(true)
            .create()?;

        let index_raft_id = space_instances
            .index_builder("_pico_instance_raft_id")
            .unique(true)
            .part("raft_id")
            .if_not_exists(true)
            .create()?;

        let index_replicaset_name = space_instances
            .index_builder("_pico_instance_replicaset_name")
            .unique(false)
            .part("replicaset_name")
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space: space_instances,
            index_instance_name,
            index_instance_uuid,
            index_raft_id,
            index_replicaset_name,
        })
    }

    #[inline]
    pub fn put(&self, instance: &Instance) -> tarantool::Result<()> {
        self.space.replace(instance)?;
        Ok(())
    }

    #[allow(dead_code)]
    #[inline]
    pub fn delete(&self, instance_name: &str) -> tarantool::Result<()> {
        self.space.delete(&[instance_name])?;
        Ok(())
    }

    /// Finds an instance by `name` or `raft_id` (see trait [`InstanceName`]).
    #[inline(always)]
    pub fn get(&self, name: &impl InstanceName) -> Result<Instance> {
        let res = name
            .find_in(self)?
            .decode()
            .expect("failed to decode instance");
        Ok(res)
    }

    /// Finds an instance by `name` or `raft_id` (see trait [`InstanceName`]).
    #[inline(always)]
    pub fn try_get(&self, name: &impl InstanceName) -> Result<Option<Instance>> {
        let res = name.find_in(self);
        let tuple = match res {
            Ok(tuple) => tuple,
            Err(Error::NoSuchInstance { .. }) => return Ok(None),
            Err(e) => return Err(e),
        };

        let res = tuple.decode()?;
        Ok(res)
    }

    /// Returns the tuple without deserializing.
    #[inline(always)]
    pub fn get_raw(&self, name: &impl InstanceName) -> Result<Tuple> {
        let res = name.find_in(self)?;
        Ok(res)
    }

    #[inline(always)]
    pub fn by_uuid(&self, uuid: &str) -> tarantool::Result<Option<Instance>> {
        let tuple = self.index_instance_uuid.get(&[uuid])?;
        tuple.as_ref().map(Tuple::decode).transpose()
    }

    /// Checks if an instance with `name` (see trait [`InstanceName`]) is present.
    #[inline]
    pub fn contains(&self, name: &impl InstanceName) -> Result<bool> {
        match name.find_in(self) {
            Ok(_) => Ok(true),
            Err(Error::NoSuchInstance { .. }) => Ok(false),
            Err(err) => Err(err),
        }
    }

    #[inline]
    pub fn all_instances(&self) -> tarantool::Result<Vec<Instance>> {
        self.space
            .select(IteratorType::All, &())?
            .map(|tuple| tuple.decode())
            .collect()
    }

    pub fn replicaset_instances(
        &self,
        replicaset_name: &str,
    ) -> tarantool::Result<EntryIter<Instance, MP_SERDE>> {
        let iter = self
            .index_replicaset_name
            .select(IteratorType::Eq, &[replicaset_name])?;
        Ok(EntryIter::new(iter))
    }

    pub fn max_raft_id(&self) -> Result<RaftId> {
        let Some(tuple) = self.index_raft_id.max(&())? else {
            return Ok(0);
        };
        let Some(raft_id) = tuple.field(Instance::FIELD_RAFT_ID)? else {
            return Err(Error::StorageCorrupted {
                field: "raft_id".into(),
                table: "_pico_instance".into(),
            });
        };
        Ok(raft_id)
    }

    pub fn failure_domain_names(&self) -> Result<HashSet<Uppercase>> {
        let mut res = HashSet::with_capacity(16);
        for tuple in self.space.select(IteratorType::All, &())? {
            let Some(failure_domain) = tuple.field(Instance::FIELD_FAILURE_DOMAIN)? else {
                return Err(Error::StorageCorrupted {
                    field: "failure_domain".into(),
                    table: "_pico_instance".into(),
                });
            };
            let failure_domain: FailureDomain = failure_domain;
            for name in failure_domain.data.into_keys() {
                res.insert(name);
            }
        }
        Ok(res)
    }
}

impl ToEntryIter<MP_SERDE> for Instances {
    type Entry = Instance;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// InstanceName
////////////////////////////////////////////////////////////////////////////////

/// Types implementing this trait can be used to identify a `Instance` when accessing storage.
pub trait InstanceName: serde::Serialize {
    fn find_in(&self, instances: &Instances) -> Result<Tuple>;
}

impl InstanceName for RaftId {
    #[inline(always)]
    fn find_in(&self, instances: &Instances) -> Result<Tuple> {
        instances
            .index_raft_id
            .get(&[self])?
            .ok_or(Error::NoSuchInstance(IdOfInstance::RaftId(*self)))
    }
}

impl InstanceName for instance::InstanceName {
    #[inline(always)]
    fn find_in(&self, instances: &Instances) -> Result<Tuple> {
        instances
            .index_instance_name
            .get(&[self])?
            .ok_or_else(|| Error::NoSuchInstance(IdOfInstance::Name(self.clone())))
    }
}

////////////////////////////////////////////////////////////////////////////////
// Properties
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing cluster-wide key-value properties
#[derive(Debug, Clone)]
pub struct Properties {
    pub space: Space,
    pub index: Index,
}

impl SystemTable for Properties {
    const TABLE_NAME: &'static str = "_pico_property";
    const TABLE_ID: SpaceId = 516;

    fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("key", FieldType::String)),
            Field::from(("value", FieldType::Any)),
        ]
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            // Primary index
            id: 0,
            name: "_pico_property_key".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![Part::from(("key", IndexFieldType::String)).is_nullable(false)],
            operable: true,
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
        }]
    }
}

impl Properties {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index = space
            .index_builder("_pico_property_key")
            .unique(true)
            .part("key")
            .if_not_exists(true)
            .create()?;

        on_replace(space.id(), Self::on_replace)?;

        Ok(Self { space, index })
    }

    /// Callback which is called when data in _pico_property is updated.
    pub fn on_replace(old: Option<Tuple>, new: Option<Tuple>) -> Result<()> {
        match (old, new) {
            (Some(old), None) => {
                // Delete
                let Ok(Some(key)) = old.field::<PropertyName>(0) else {
                    // Not a builtin property.
                    return Ok(());
                };

                if key.must_not_delete() {
                    return Err(Error::other(format!("property {key} cannot be deleted")));
                }
            }
            (old, Some(new)) => {
                // Insert or Update
                let key = new
                    .field::<&str>(0)
                    .expect("key has type string")
                    .expect("key is not nullable");

                let Ok(key) = key.parse::<PropertyName>() else {
                    // Not a builtin property.
                    // Cannot be a wrong type error, because tarantool checks
                    // the format for us.
                    if old.is_none() {
                        // Insert
                        // FIXME: this is currently printed twice
                        tlog!(Warning, "non builtin property inserted into _pico_property, this may be an error in a future version of picodata");
                    }
                    return Ok(());
                };

                let field_count = new.len();
                if field_count != 2 {
                    return Err(Error::other(format!(
                        "too many fields: got {field_count}, expected 2"
                    )));
                }

                key.verify_new_tuple(&new)?;
            }
            (None, None) => unreachable!(),
        }

        Ok(())
    }

    #[inline]
    pub fn get<T>(&self, key: &'static str) -> tarantool::Result<Option<T>>
    where
        T: DecodeOwned,
    {
        match self.space.get(&[key])? {
            Some(t) => t.field(1),
            None => Ok(None),
        }
    }

    #[inline]
    pub fn put(&self, key: PropertyName, value: &impl serde::Serialize) -> tarantool::Result<()> {
        self.space.put(&(key, value))?;
        Ok(())
    }

    /// Returns the deleted tuple if it was deleted, `None` if there was no
    /// tuple with the given `key`.
    #[inline]
    pub fn delete(&self, key: PropertyName) -> tarantool::Result<Option<Tuple>> {
        self.space.delete(&[key])
    }

    #[inline]
    pub fn pending_schema_change(&self) -> tarantool::Result<Option<Ddl>> {
        self.get(PropertyName::PendingSchemaChange.as_str())
    }

    #[inline]
    pub fn pending_schema_version(&self) -> tarantool::Result<Option<u64>> {
        self.get(PropertyName::PendingSchemaVersion.as_str())
    }

    #[inline]
    pub fn global_schema_version(&self) -> tarantool::Result<u64> {
        let res = self
            .get(PropertyName::GlobalSchemaVersion.as_str())?
            .unwrap_or(INITIAL_SCHEMA_VERSION);
        Ok(res)
    }

    #[inline]
    pub fn pending_plugin_op(&self) -> tarantool::Result<Option<PluginOp>> {
        self.get(PropertyName::PendingPluginOperation.as_str())
    }

    #[inline]
    pub fn next_schema_version(&self) -> tarantool::Result<u64> {
        let res = if let Some(version) = self.get(PropertyName::NextSchemaVersion.as_str())? {
            version
        } else {
            let current = self.global_schema_version()?;
            current + 1
        };
        Ok(res)
    }

    #[inline]
    pub fn cluster_version(&self) -> tarantool::Result<String> {
        let res: String = self
            .get::<String>(PropertyName::ClusterVersion.as_str())?
            .expect("ClusterVersion should be initialized");
        Ok(res)
    }

    #[inline]
    pub fn system_catalog_version(&self) -> tarantool::Result<Option<String>> {
        self.get(PropertyName::SystemCatalogVersion.as_str())
    }

    #[inline]
    pub fn pending_governor_op_id(&self) -> tarantool::Result<Option<u64>> {
        self.get(PropertyName::PendingGovernorOpId.as_str())
    }

    #[inline]
    pub fn pending_catalog_version(&self) -> tarantool::Result<Option<String>> {
        self.get(PropertyName::PendingCatalogVersion.as_str())
    }
}

////////////////////////////////////////////////////////////////////////////////
// Replicasets
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing replicaset info from storage
#[derive(Debug, Clone)]
pub struct Replicasets {
    pub space: Space,
    pub index_replicaset_name: Index,
    pub index_replicaset_uuid: Index,
}

impl SystemTable for Replicasets {
    const TABLE_NAME: &'static str = "_pico_replicaset";
    const TABLE_ID: SpaceId = 517;

    fn format() -> Vec<tarantool::space::Field> {
        Replicaset::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![
            IndexDef {
                table_id: Self::TABLE_ID,
                // Primary index
                id: 0,
                name: "_pico_replicaset_name".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![Part::from(("name", IndexFieldType::String)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: "_pico_replicaset_uuid".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![Part::from(("uuid", IndexFieldType::String)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
        ]
    }
}

impl Replicasets {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index_replicaset_name = space
            .index_builder("_pico_replicaset_name")
            .unique(true)
            .part("name")
            .if_not_exists(true)
            .create()?;

        let index_replicaset_uuid = space
            .index_builder("_pico_replicaset_uuid")
            .unique(true)
            .part("uuid")
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space,
            index_replicaset_name,
            index_replicaset_uuid,
        })
    }

    #[inline]
    pub fn put(&self, replicaset: &Replicaset) -> tarantool::Result<()> {
        self.space.replace(replicaset)?;
        Ok(())
    }

    #[allow(unused)]
    #[inline]
    pub fn get(&self, replicaset_name: &str) -> tarantool::Result<Option<Replicaset>> {
        let tuple = self.space.get(&[replicaset_name])?;
        tuple.as_ref().map(Tuple::decode).transpose()
    }

    #[inline(always)]
    pub fn get_raw(&self, replicaset_name: &str) -> Result<Tuple> {
        let Some(tuple) = self.space.get(&[replicaset_name])? else {
            return Err(Error::NoSuchReplicaset {
                name: replicaset_name.into(),
                id_is_uuid: false,
            });
        };
        Ok(tuple)
    }

    #[allow(unused)]
    #[inline]
    pub fn by_uuid_raw(&self, replicaset_uuid: &str) -> Result<Tuple> {
        let Some(tuple) = self.index_replicaset_uuid.get(&[replicaset_uuid])? else {
            return Err(Error::NoSuchReplicaset {
                name: replicaset_uuid.into(),
                id_is_uuid: true,
            });
        };
        Ok(tuple)
    }
}

impl ToEntryIter<MP_SERDE> for Replicasets {
    type Entry = Replicaset;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// EntryIter
////////////////////////////////////////////////////////////////////////////////

// In the current Rust version it is not possible to use negative
// trait bounds so we have to rely on const generics to allow
// both encode implementations.
/// Msgpack encoder/decoder implementation.
type MpImpl = bool;
/// rmp-serde based implementation.
const MP_SERDE: MpImpl = false;
/// Custom implementation from [`::tarantool::msgpack`]
const MP_CUSTOM: MpImpl = true;

/// This trait is implemented for storage structs for iterating over the entries
/// from that storage.
pub trait ToEntryIter<const MP: MpImpl> {
    /// Target type for entry deserialization.
    /// Entry should be encoded with the selected `MP` implementation.
    type Entry;

    fn index_iter(&self) -> tarantool::Result<IndexIterator>;

    #[inline(always)]
    fn iter(&self) -> tarantool::Result<EntryIter<Self::Entry, MP>> {
        Ok(EntryIter::new(self.index_iter()?))
    }
}

/// An iterator struct for automatically deserializing tuples into a given type.
/// Tuples should be encoded with the selected `MP` implementation.
///
/// # Panics
/// Will panic in case deserialization fails on a given iteration.
pub struct EntryIter<T, const MP: MpImpl> {
    iter: IndexIterator,
    marker: PhantomData<T>,
}

impl<T, const MP: MpImpl> EntryIter<T, MP> {
    pub fn new(iter: IndexIterator) -> Self {
        Self {
            iter,
            marker: PhantomData,
        }
    }
}

impl<T> Iterator for EntryIter<T, MP_SERDE>
where
    T: DecodeOwned,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        let tuple = self.iter.next()?;
        let entry = Tuple::decode(&tuple).expect("entry should decode correctly");
        Some(entry)
    }
}

impl<T> Iterator for EntryIter<T, MP_CUSTOM>
where
    for<'de> T: ::tarantool::msgpack::Decode<'de>,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        let tuple = self.iter.next()?.to_vec();
        let entry = ::tarantool::msgpack::decode(&tuple).expect("entry should decode correctly");
        Some(entry)
    }
}

impl<T, const MP: MpImpl> std::fmt::Debug for EntryIter<T, MP> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .finish_non_exhaustive()
    }
}

////////////////////////////////////////////////////////////////////////////////
// Users
////////////////////////////////////////////////////////////////////////////////

/// The hard upper bound (32) for max users comes from tarantool BOX_USER_MAX
const MAX_USERS: usize = 128;

/// A struct for accessing info of all the picodata users.
#[derive(Debug, Clone)]
pub struct Users {
    pub space: Space,
    pub index_name: Index,
    pub index_id: Index,
    pub index_owner_id: Index,
}

impl SystemTable for Users {
    const TABLE_NAME: &'static str = "_pico_user";
    // FIXME: gap in ids
    const TABLE_ID: SpaceId = 520;

    fn format() -> Vec<tarantool::space::Field> {
        UserDef::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![
            IndexDef {
                table_id: Self::TABLE_ID,
                // Primary index
                id: 0,
                name: "_pico_user_id".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![Part::from(("id", IndexFieldType::Unsigned)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: "_pico_user_name".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![Part::from(("name", IndexFieldType::String)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 2,
                name: "_pico_user_owner_id".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(false)],
                parts: vec![Part::from(("owner", IndexFieldType::Unsigned)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
        ]
    }
}

impl Users {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index_id = space
            .index_builder("_pico_user_id")
            .unique(true)
            .part("id")
            .if_not_exists(true)
            .create()?;

        let index_name = space
            .index_builder("_pico_user_name")
            .unique(true)
            .part("name")
            .if_not_exists(true)
            .create()?;

        let index_owner_id = space
            .index_builder("_pico_user_owner_id")
            .unique(false)
            .part("owner")
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space,
            index_id,
            index_name,
            index_owner_id,
        })
    }

    #[inline]
    pub fn by_id(&self, user_id: UserId) -> tarantool::Result<Option<UserDef>> {
        let tuple = self.space.get(&[user_id])?;
        tuple.as_ref().map(Tuple::decode).transpose()
    }

    #[inline]
    pub fn by_name(&self, user_name: &str) -> tarantool::Result<Option<UserDef>> {
        let tuple = self.index_name.get(&[user_name])?;
        tuple.as_ref().map(Tuple::decode).transpose()
    }

    #[inline]
    pub fn by_owner_id(&self, owner_id: UserId) -> tarantool::Result<EntryIter<UserDef, MP_SERDE>> {
        let iter = self.index_owner_id.select(IteratorType::Eq, &[owner_id])?;
        Ok(EntryIter::new(iter))
    }

    #[inline]
    pub fn max_user_id(&self) -> tarantool::Result<Option<UserId>> {
        match self.index_id.max(&())? {
            Some(user) => Ok(user.get(0).unwrap()),
            None => Ok(None),
        }
    }

    #[inline]
    pub fn replace(&self, user_def: &UserDef) -> tarantool::Result<()> {
        self.space.replace(user_def)?;
        Ok(())
    }

    #[inline]
    pub fn insert(&self, user_def: &UserDef) -> tarantool::Result<()> {
        self.space.insert(user_def)?;
        Ok(())
    }

    #[inline]
    pub fn delete(&self, user_id: UserId) -> tarantool::Result<()> {
        self.space.delete(&[user_id])?;
        Ok(())
    }

    #[inline]
    pub fn update_name(&self, user_id: UserId, name: &str) -> tarantool::Result<()> {
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(column_name!(UserDef, name), name)?;
        self.space.update(&[user_id], ops)?;
        Ok(())
    }

    #[inline]
    pub fn update_auth(&self, user_id: UserId, auth: &AuthDef) -> tarantool::Result<()> {
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(column_name!(UserDef, auth), auth)?;
        self.space.update(&[user_id], ops)?;
        Ok(())
    }

    #[inline]
    pub fn check_user_limit(&self) -> traft::Result<()> {
        if self.space.len()? >= MAX_USERS {
            return Err(Error::Other(
                format!("a limit on the total number of users has been reached: {MAX_USERS}")
                    .into(),
            ));
        }
        Ok(())
    }
}

impl ToEntryIter<MP_SERDE> for Users {
    type Entry = UserDef;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// Privileges
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing info of all privileges granted to users.
#[derive(Debug, Clone)]
pub struct Privileges {
    pub space: Space,
    pub primary_key: Index,
    pub object_idx: Index,
}

impl SystemTable for Privileges {
    const TABLE_NAME: &'static str = "_pico_privilege";
    const TABLE_ID: SpaceId = 521;

    fn format() -> Vec<tarantool::space::Field> {
        PrivilegeDef::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![
            IndexDef {
                table_id: Self::TABLE_ID,
                // Primary index
                id: 0,
                name: "_pico_privilege_primary".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![
                    Part::from(("grantee_id", IndexFieldType::Unsigned)).is_nullable(false),
                    Part::from(("object_type", IndexFieldType::String)).is_nullable(false),
                    Part::from(("object_id", IndexFieldType::Integer)).is_nullable(false),
                    Part::from(("privilege", IndexFieldType::String)).is_nullable(false),
                ],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: "_pico_privilege_object".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(false)],
                parts: vec![
                    Part::from(("object_type", IndexFieldType::String)).is_nullable(false),
                    Part::from(("object_id", IndexFieldType::Integer)).is_nullable(false),
                ],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
        ]
    }
}

impl Privileges {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let primary_key = space
            .index_builder("_pico_privilege_primary")
            .unique(true)
            .parts(["grantee_id", "object_type", "object_id", "privilege"])
            .if_not_exists(true)
            .create()?;

        let object_idx = space
            .index_builder("_pico_privilege_object")
            .unique(false)
            .part("object_type")
            .part("object_id")
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space,
            primary_key,
            object_idx,
        })
    }

    #[inline(always)]
    pub fn get(
        &self,
        grantee_id: UserId,
        object_type: &str,
        object_id: i64,
        privilege: &str,
    ) -> tarantool::Result<Option<PrivilegeDef>> {
        let tuple = self
            .space
            .get(&(grantee_id, object_type, object_id, privilege))?;
        tuple.as_ref().map(Tuple::decode).transpose()
    }

    #[inline(always)]
    pub fn by_grantee_id(
        &self,
        grantee_id: UserId,
    ) -> tarantool::Result<EntryIter<PrivilegeDef, MP_SERDE>> {
        let iter = self.primary_key.select(IteratorType::Eq, &[grantee_id])?;
        Ok(EntryIter::new(iter))
    }

    #[inline(always)]
    pub fn by_object(
        &self,
        object_type: SchemaObjectType,
        object_id: i64,
    ) -> tarantool::Result<EntryIter<PrivilegeDef, MP_SERDE>> {
        let iter = self
            .object_idx
            .select(IteratorType::Eq, &(object_type, object_id))?;
        Ok(EntryIter::new(iter))
    }

    #[inline(always)]
    pub fn replace(&self, priv_def: &PrivilegeDef) -> tarantool::Result<()> {
        self.space.replace(priv_def)?;
        Ok(())
    }

    /// If `if_not_exists == true` the function will not return an error if this privilege already exists.
    #[inline(always)]
    pub fn insert(&self, priv_def: &PrivilegeDef, if_not_exists: bool) -> tarantool::Result<()> {
        let res = self.space.insert(priv_def).map(|_| ());
        if if_not_exists {
            ignore_only_error(res, TntErrorCode::TupleFound)
        } else {
            res
        }
    }

    /// This function will not return an error if this privilege does not exists.
    /// As deletion in tarantool is idempotent.
    #[inline(always)]
    pub fn delete(
        &self,
        grantee_id: UserId,
        object_type: &str,
        object_id: i64,
        privilege: &str,
    ) -> tarantool::Result<()> {
        self.space
            .delete(&(grantee_id, object_type, object_id, privilege))?;
        Ok(())
    }

    /// Remove any privilege definitions granted to the given grantee.
    #[inline]
    pub fn delete_all_by_grantee_id(&self, grantee_id: UserId) -> tarantool::Result<()> {
        for priv_def in self.by_grantee_id(grantee_id)? {
            self.delete(
                priv_def.grantee_id(),
                &priv_def.object_type(),
                priv_def.object_id_raw(),
                &priv_def.privilege(),
            )?;
        }
        Ok(())
    }

    /// Remove any privilege definitions assigning the given role.
    #[inline]
    pub fn delete_all_by_granted_role(&self, role_id: u32) -> Result<()> {
        // here we failed to deserialize the privilege that has alter on universe
        for priv_def in self.iter()? {
            if priv_def.privilege() == PrivilegeType::Execute
                && priv_def.object_type() == SchemaObjectType::Role
                && priv_def.object_id() == Some(role_id)
            {
                self.delete(
                    priv_def.grantee_id(),
                    &priv_def.object_type(),
                    priv_def.object_id_raw(),
                    &priv_def.privilege(),
                )?;
            }
        }
        Ok(())
    }

    /// Remove any privilege definitions granted to the given object.
    #[inline]
    pub fn delete_all_by_object(
        &self,
        object_type: SchemaObjectType,
        object_id: i64,
    ) -> tarantool::Result<()> {
        for priv_def in self.by_object(object_type, object_id)? {
            debug_assert_eq!(priv_def.object_id_raw(), object_id);
            self.delete(
                priv_def.grantee_id(),
                &priv_def.object_type(),
                object_id,
                &priv_def.privilege(),
            )?;
        }
        Ok(())
    }
}

impl ToEntryIter<MP_SERDE> for Privileges {
    type Entry = PrivilegeDef;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tiers
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing info of all tiers in cluster.
#[derive(Debug, Clone)]
pub struct Tiers {
    pub space: Space,
    pub index_name: Index,
}

impl SystemTable for Tiers {
    const TABLE_NAME: &'static str = "_pico_tier";
    const TABLE_ID: SpaceId = 523;

    fn format() -> Vec<tarantool::space::Field> {
        Tier::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            // Primary index
            id: 0,
            name: "_pico_tier_name".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![Part::from(("name", IndexFieldType::String)).is_nullable(false)],
            operable: true,
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
        }]
    }
}
impl Tiers {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index_name = space
            .index_builder("_pico_tier_name")
            .unique(true)
            .part("name")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, index_name })
    }

    #[inline(always)]
    pub fn by_name(&self, tier_name: &str) -> tarantool::Result<Option<Tier>> {
        let tuple = self.index_name.get(&[tier_name])?;
        tuple.as_ref().map(Tuple::decode).transpose()
    }

    #[inline]
    pub fn put(&self, tier: &Tier) -> tarantool::Result<()> {
        self.space.replace(tier)?;
        Ok(())
    }
}

impl ToEntryIter<MP_SERDE> for Tiers {
    type Entry = Tier;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// Routines
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing info of all the user-defined routines.
#[derive(Debug, Clone)]
pub struct Routines {
    pub space: Space,
    pub index_id: Index,
    pub index_name: Index,
    pub index_owner_id: Index,
}

impl SystemTable for Routines {
    const TABLE_NAME: &'static str = "_pico_routine";
    const TABLE_ID: SpaceId = 524;

    fn format() -> Vec<tarantool::space::Field> {
        RoutineDef::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![
            IndexDef {
                table_id: Self::TABLE_ID,
                // Primary index
                id: 0,
                name: "_pico_routine_id".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![Part::from(("id", IndexFieldType::Unsigned)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: "_pico_routine_name".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![Part::from(("name", IndexFieldType::String)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 2,
                name: "_pico_routine_owner_id".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(false)],
                parts: vec![Part::from(("owner", IndexFieldType::Unsigned)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
        ]
    }
}

impl Routines {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index_id = space
            .index_builder("_pico_routine_id")
            .unique(true)
            .part("id")
            .if_not_exists(true)
            .create()?;

        let index_name = space
            .index_builder("_pico_routine_name")
            .unique(true)
            .part("name")
            .if_not_exists(true)
            .create()?;

        let index_owner_id = space
            .index_builder("_pico_routine_owner_id")
            .unique(false)
            .part("owner")
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space,
            index_id,
            index_name,
            index_owner_id,
        })
    }

    #[inline(always)]
    pub fn rename(&self, id: u32, new_name: &str) -> tarantool::Result<Option<RoutineDef>> {
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(column_name!(RoutineDef, name), new_name)?;
        let tuple = self.space.update(&[id], ops)?;
        tuple.as_ref().map(Tuple::decode).transpose()
    }

    #[inline(always)]
    pub fn by_name(&self, routine_name: &str) -> tarantool::Result<Option<RoutineDef>> {
        let tuple = self.index_name.get(&[routine_name])?;
        tuple.as_ref().map(Tuple::decode).transpose()
    }

    #[inline]
    pub fn by_id(&self, routine_id: RoutineId) -> tarantool::Result<Option<RoutineDef>> {
        let tuple = self.space.get(&[routine_id])?;
        tuple.as_ref().map(Tuple::decode).transpose()
    }

    #[inline]
    pub fn put(&self, routine: &RoutineDef) -> tarantool::Result<()> {
        self.space.replace(routine)?;
        Ok(())
    }

    #[inline]
    pub fn delete(&self, routine_id: RoutineId) -> tarantool::Result<()> {
        self.space.delete(&[routine_id])?;
        Ok(())
    }

    #[inline]
    pub fn update_operable(&self, routine_id: RoutineId, operable: bool) -> tarantool::Result<()> {
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(column_name!(RoutineDef, operable), operable)?;
        self.space.update(&[routine_id], ops)?;
        Ok(())
    }

    pub fn by_owner_id(
        &self,
        owner_id: UserId,
    ) -> tarantool::Result<EntryIter<RoutineDef, MP_SERDE>> {
        let iter = self.index_owner_id.select(IteratorType::Eq, &[owner_id])?;
        Ok(EntryIter::new(iter))
    }
}

impl ToEntryIter<MP_SERDE> for Routines {
    type Entry = RoutineDef;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

pub fn make_routine_not_found(routine_id: RoutineId) -> tarantool::error::TarantoolError {
    tarantool::set_error!(
        tarantool::error::TarantoolErrorCode::TupleNotFound,
        "routine with id {routine_id} not found",
    );
    tarantool::error::TarantoolError::last()
}

pub type RoutineId = u32;

////////////////////////////////////////////////////////////////////////////////
// Plugins
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing info of all known plugins.
#[derive(Debug, Clone)]
pub struct Plugins {
    pub space: Space,
    pub primary_key: Index,
}

impl SystemTable for Plugins {
    const TABLE_NAME: &'static str = "_pico_plugin";
    const TABLE_ID: SpaceId = 526;

    fn format() -> Vec<tarantool::space::Field> {
        PluginDef::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            id: 0,
            name: "_pico_plugin_name".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![
                Part::from(("name", IndexFieldType::String)).is_nullable(false),
                Part::from(("version", IndexFieldType::String)).is_nullable(false),
            ],
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
            operable: true,
        }]
    }
}
impl Plugins {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let primary_key = space
            .index_builder("_pico_plugin_name")
            .unique(true)
            .part("name")
            .part("version")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, primary_key })
    }

    #[inline]
    pub fn put(&self, plugin: &PluginDef) -> tarantool::Result<()> {
        self.space.replace(plugin)?;
        Ok(())
    }

    #[inline]
    pub fn delete(&self, ident: &PluginIdentifier) -> tarantool::Result<()> {
        self.space.delete(&(&ident.name, &ident.version))?;
        Ok(())
    }

    #[inline]
    pub fn contains(&self, ident: &PluginIdentifier) -> tarantool::Result<bool> {
        Ok(self.space.get(&(&ident.name, &ident.version))?.is_some())
    }

    #[inline]
    pub fn get(&self, ident: &PluginIdentifier) -> tarantool::Result<Option<PluginDef>> {
        let plugin = self.space.get(&(&ident.name, &ident.version))?;
        plugin.as_ref().map(Tuple::decode).transpose()
    }

    #[inline]
    pub fn get_all_versions(&self, plugin_name: &str) -> tarantool::Result<Vec<PluginDef>> {
        self.space
            .select(IteratorType::Eq, &(plugin_name,))?
            .map(|tuple| tuple.decode())
            .collect()
    }

    #[inline]
    pub fn get_all(&self) -> tarantool::Result<Vec<PluginDef>> {
        self.space
            .select(IteratorType::All, &())?
            .map(|tuple| tuple.decode())
            .collect::<tarantool::Result<Vec<_>>>()
    }

    #[inline]
    pub fn all_enabled(&self) -> tarantool::Result<Vec<PluginDef>> {
        let it = self.space.select(IteratorType::All, &())?;
        let mut result = vec![];
        for tuple in it {
            let def = tuple.decode::<PluginDef>()?;
            if def.enabled {
                result.push(def);
            }
        }
        Ok(result)
    }
}

impl ToEntryIter<MP_SERDE> for Plugins {
    type Entry = PluginDef;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// Services
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing info of all known plugin services.
#[derive(Debug, Clone)]
pub struct Services {
    pub space: Space,
    pub index_name: Index,
}

impl SystemTable for Services {
    const TABLE_NAME: &'static str = "_pico_service";
    const TABLE_ID: SpaceId = 527;

    fn format() -> Vec<tarantool::space::Field> {
        ServiceDef::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            id: 0,
            name: "_pico_service_name".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![
                Part::from(("plugin_name", IndexFieldType::String)).is_nullable(false),
                Part::from(("name", IndexFieldType::String)).is_nullable(false),
                Part::from(("version", IndexFieldType::String)).is_nullable(false),
            ],
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
            operable: true,
        }]
    }
}
impl Services {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index_name = space
            .index_builder("_pico_service_name")
            .unique(true)
            .part("plugin_name")
            .part("name")
            .part("version")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, index_name })
    }

    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        ServiceDef::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            id: 0,
            name: "_pico_service_name".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![
                Part::from(("plugin_name", IndexFieldType::String)).is_nullable(false),
                Part::from(("name", IndexFieldType::String)).is_nullable(false),
                Part::from(("version", IndexFieldType::String)).is_nullable(false),
            ],
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
            operable: true,
        }]
    }

    #[inline]
    pub fn put(&self, service: &ServiceDef) -> tarantool::Result<()> {
        self.space.replace(service)?;
        Ok(())
    }

    #[inline]
    pub fn get(
        &self,
        plugin_ident: &PluginIdentifier,
        name: &str,
    ) -> tarantool::Result<Option<ServiceDef>> {
        let service = self
            .space
            .get(&(&plugin_ident.name, name, &plugin_ident.version))?;
        service.as_ref().map(Tuple::decode).transpose()
    }

    #[inline]
    pub fn get_by_plugin(&self, ident: &PluginIdentifier) -> tarantool::Result<Vec<ServiceDef>> {
        let it = self.space.select(IteratorType::Eq, &(&ident.name,))?;

        let mut result = vec![];
        for tuple in it {
            let svc = tuple.decode::<ServiceDef>()?;
            if svc.version == ident.version {
                result.push(svc)
            }
        }
        Ok(result)
    }

    #[inline]
    pub fn get_by_tier(&self, tier_name: &String) -> tarantool::Result<Vec<ServiceDef>> {
        let all_services = self.space.select(IteratorType::All, &())?;
        let mut result = vec![];
        for tuple in all_services {
            let svc = tuple.decode::<ServiceDef>()?;
            if svc.tiers.contains(tier_name) {
                result.push(svc)
            }
        }
        Ok(result)
    }

    #[inline]
    pub fn delete(&self, plugin: &str, service: &str, version: &str) -> tarantool::Result<()> {
        self.space.delete(&[plugin, service, version])?;
        Ok(())
    }
}

impl ToEntryIter<MP_SERDE> for Services {
    type Entry = ServiceDef;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// ServiceRouteTable
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing info of plugin services routing table.
#[derive(Debug, Clone)]
pub struct ServiceRouteTable {
    pub space: Space,
    pub primary_key: Index,
}

impl SystemTable for ServiceRouteTable {
    const TABLE_NAME: &'static str = "_pico_service_route";
    const TABLE_ID: SpaceId = 528;

    fn format() -> Vec<tarantool::space::Field> {
        ServiceRouteItem::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            id: 0,
            name: "_pico_service_routing_key".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![
                Part::from(("plugin_name", IndexFieldType::String)).is_nullable(false),
                Part::from(("plugin_version", IndexFieldType::String)).is_nullable(false),
                Part::from(("service_name", IndexFieldType::String)).is_nullable(false),
                Part::from(("instance_name", IndexFieldType::String)).is_nullable(false),
            ],
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
            operable: true,
        }]
    }
}
impl ServiceRouteTable {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let primary_key = space
            .index_builder("_pico_service_routing_key")
            .unique(true)
            .part("plugin_name")
            .part("plugin_version")
            .part("service_name")
            .part("instance_name")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, primary_key })
    }

    #[inline]
    pub fn put(&self, routing_item: &ServiceRouteItem) -> tarantool::Result<()> {
        self.space.replace(routing_item)?;
        Ok(())
    }

    #[inline]
    pub fn get(&self, key: &ServiceRouteKey) -> tarantool::Result<Option<ServiceRouteItem>> {
        let item = self.space.get(key)?;
        item.as_ref().map(Tuple::decode).transpose()
    }

    #[inline(always)]
    pub fn get_raw(&self, key: &ServiceRouteKey) -> tarantool::Result<Option<Tuple>> {
        let tuple = self.space.get(key)?;
        Ok(tuple)
    }

    pub fn delete_by_plugin(&self, plugin: &PluginIdentifier) -> tarantool::Result<()> {
        let all_routes = self
            .space
            .select(IteratorType::Eq, &(&plugin.name, &plugin.version))?;
        for tuple in all_routes {
            let item = tuple.decode::<ServiceRouteItem>()?;
            self.space.delete(&(
                item.plugin_name,
                item.plugin_version,
                item.service_name,
                item.instance_name,
            ))?;
        }
        Ok(())
    }

    pub fn get_by_instance(
        &self,
        i: &instance::InstanceName,
    ) -> tarantool::Result<Vec<ServiceRouteItem>> {
        let mut result = vec![];

        let iter = self.space.select(IteratorType::All, &())?;
        for tuple in iter {
            let item: ServiceRouteItem = tuple.decode()?;
            if item.instance_name != i {
                continue;
            }
            result.push(item);
        }

        Ok(result)
    }

    pub fn get_available_instances(
        &self,
        plugin: &PluginIdentifier,
        service: &str,
    ) -> tarantool::Result<Vec<instance::InstanceName>> {
        let mut result = vec![];

        let iter = self
            .space
            .select(IteratorType::All, &(&plugin.name, &plugin.version, service))?;
        for tuple in iter {
            let item: ServiceRouteItem = tuple.decode()?;
            if item.poison {
                continue;
            }
            result.push(item.instance_name);
        }
        Ok(result)
    }
}

impl ToEntryIter<MP_SERDE> for ServiceRouteTable {
    type Entry = ServiceRouteItem;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// PluginMigration
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing info of applied plugin migrations.
#[derive(Debug, Clone)]
pub struct PluginMigrations {
    pub space: Space,
    pub primary_key: Index,
}

impl SystemTable for PluginMigrations {
    const TABLE_NAME: &'static str = "_pico_plugin_migration";
    const TABLE_ID: SpaceId = 529;

    fn format() -> Vec<tarantool::space::Field> {
        PluginMigrationRecord::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            id: 0,
            name: "_pico_plugin_migration_primary_key".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![
                Part::from(("plugin_name", IndexFieldType::String)).is_nullable(false),
                Part::from(("migration_file", IndexFieldType::String)).is_nullable(false),
            ],
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
            operable: true,
        }]
    }
}
impl PluginMigrations {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let primary_key = space
            .index_builder("_pico_plugin_migration_primary_key")
            .unique(true)
            .part("plugin_name")
            .part("migration_file")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, primary_key })
    }

    pub fn get_by_plugin(
        &self,
        plugin_name: &str,
    ) -> tarantool::Result<Vec<PluginMigrationRecord>> {
        self.space
            .select(IteratorType::Eq, &(plugin_name,))?
            .map(|t| t.decode())
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////
// PluginConfig
////////////////////////////////////////////////////////////////////////////////

/// Structure for storing plugin configuration.
/// Configuration is represented by a set of key-value pairs
/// belonging to either service or extension.
#[derive(Debug, Clone)]
pub struct PluginConfig {
    pub space: Space,
    pub primary: Index,
}

impl SystemTable for PluginConfig {
    const TABLE_NAME: &'static str = "_pico_plugin_config";
    const TABLE_ID: SpaceId = 530;

    fn format() -> Vec<tarantool::space::Field> {
        PluginConfigRecord::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            id: 0,
            name: "_pico_plugin_config_pk".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![
                Part::from(("plugin", IndexFieldType::String)).is_nullable(false),
                Part::from(("version", IndexFieldType::String)).is_nullable(false),
                Part::from(("entity", IndexFieldType::String)).is_nullable(false),
                Part::from(("key", IndexFieldType::String)).is_nullable(false),
            ],
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
            operable: true,
        }]
    }
}
impl PluginConfig {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let primary = space
            .index_builder("_pico_plugin_config_pk")
            .unique(true)
            .part("plugin")
            .part("version")
            .part("entity")
            .part("key")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, primary })
    }

    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        PluginConfigRecord::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            id: 0,
            name: "_pico_plugin_config_pk".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![
                Part::from(("plugin", IndexFieldType::String)).is_nullable(false),
                Part::from(("version", IndexFieldType::String)).is_nullable(false),
                Part::from(("entity", IndexFieldType::String)).is_nullable(false),
                Part::from(("key", IndexFieldType::String)).is_nullable(false),
            ],
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
            operable: true,
        }]
    }

    /// Replace the whole service configuration to the new one.
    #[inline]
    pub fn replace(
        &self,
        ident: &PluginIdentifier,
        entity: &str,
        config: rmpv::Value,
    ) -> tarantool::Result<()> {
        debug_assert!(tarantool::transaction::is_in_transaction());
        let new_config_records = PluginConfigRecord::from_config(ident, entity, config)?;

        // delete an old config first
        let old_cfg_records = self
            .space
            .select(IteratorType::Eq, &(&ident.name, &ident.version, entity))?;
        for tuple in old_cfg_records {
            let record: PluginConfigRecord = tuple.decode()?;
            self.space.delete(&record.pk())?;
        }

        // now save a new config
        for config_rec in new_config_records {
            self.space.put(&config_rec)?;
        }

        Ok(())
    }

    pub fn get(
        &self,
        ident: &PluginIdentifier,
        entity: &str,
        key: &str,
    ) -> tarantool::Result<Option<PluginConfigRecord>> {
        self.space
            .get(&(&ident.name, &ident.version, entity, key))?
            .map(|t| t.decode())
            .transpose()
    }

    pub fn get_by_entity(
        &self,
        ident: &PluginIdentifier,
        entity: &str,
    ) -> tarantool::Result<HashMap<String, rmpv::Value>> {
        let cfg_records = self
            .space
            .select(IteratorType::Eq, &(&ident.name, &ident.version, entity))?;

        let mut result = HashMap::new();
        for tuple in cfg_records {
            let cfg_record: PluginConfigRecord = tuple.decode()?;
            result.insert(cfg_record.key, cfg_record.value);
        }

        Ok(result)
    }

    /// Return configuration for service or extension.
    /// Result is represented as message pack Map, in case there is nothing mp Nil is used.
    #[inline]
    pub fn get_by_entity_as_mp(
        &self,
        ident: &PluginIdentifier,
        entity: &str,
    ) -> tarantool::Result<rmpv::Value> {
        let cfg_records = self
            .space
            .select(IteratorType::Eq, &(&ident.name, &ident.version, entity))?;

        let mut result = vec![];
        for tuple in cfg_records {
            let cfg_record: PluginConfigRecord = tuple.decode()?;
            result.push((
                rmpv::Value::String(Utf8String::from(cfg_record.key)),
                cfg_record.value,
            ));
        }

        if result.is_empty() {
            return Ok(rmpv::Value::Nil);
        }

        Ok(rmpv::Value::Map(result))
    }

    /// Remove configuration.
    #[inline]
    pub fn remove_by_entity(
        &self,
        ident: &PluginIdentifier,
        entity: &str,
    ) -> tarantool::Result<()> {
        debug_assert!(tarantool::transaction::is_in_transaction());
        let cfg_records = self
            .space
            .select(IteratorType::Eq, &(&ident.name, &ident.version, entity))?;

        for tuple in cfg_records {
            let record: PluginConfigRecord = tuple.decode()?;
            self.space.delete(&record.pk())?;
        }

        Ok(())
    }

    pub fn replace_many(
        &self,
        ident: &PluginIdentifier,
        entity: &str,
        kv_pairs: Vec<(String, rmpv::Value)>,
    ) -> tarantool::Result<()> {
        let new_config_records = kv_pairs.into_iter().map(|(k, v)| PluginConfigRecord {
            plugin: ident.name.to_string(),
            version: ident.version.to_string(),
            entity: entity.to_string(),
            key: k,
            value: v,
        });

        // now save a new config
        for config_rec in new_config_records {
            self.space.put(&config_rec)?;
        }
        Ok(())
    }

    /// Return configuration for the whole plugin.
    #[inline]
    pub fn all_entities(
        &self,
        ident: &PluginIdentifier,
    ) -> tarantool::Result<HashMap<String, rmpv::Value>> {
        let cfg_records = self
            .space
            .select(IteratorType::Eq, &(&ident.name, &ident.version))?;

        let mut grouped_records = HashMap::new();
        for tuple in cfg_records {
            let cfg_record: PluginConfigRecord = tuple.decode()?;
            let entry = grouped_records
                .entry(cfg_record.entity.clone())
                .or_insert(vec![]);
            entry.push((rmpv::Value::String(cfg_record.key.into()), cfg_record.value));
        }
        Ok(grouped_records
            .into_iter()
            .map(|(k, v)| (k, rmpv::Value::Map(v)))
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////
// DbConfig
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing storage of the cluster-wide and tier-wide configs that
/// can be modified via alter system.
#[derive(Debug, Clone)]
pub struct DbConfig {
    pub space: Space,
    pub primary: Index,   // by key + scope
    pub secondary: Index, // by key
}

impl SystemTable for DbConfig {
    const TABLE_NAME: &'static str = "_pico_db_config";
    const TABLE_ID: SpaceId = 531;

    fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("key", FieldType::String)),
            Field::from(("scope", FieldType::String)),
            Field::from(("value", FieldType::Any)),
        ]
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![
            IndexDef {
                table_id: Self::TABLE_ID,
                // Primary index
                id: 0,
                name: "_pico_db_config_pk".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![
                    Part::from(("key", IndexFieldType::String)).is_nullable(false),
                    Part::from(("scope", IndexFieldType::String)).is_nullable(false),
                ],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: "_pico_db_config_key".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(false)],
                parts: vec![Part::from(("key", IndexFieldType::String)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
        ]
    }
}
impl DbConfig {
    pub const GLOBAL_SCOPE: &'static str = "";

    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let primary = space
            .index_builder("_pico_db_config_pk")
            .unique(true)
            .part("key")
            .part("scope")
            .if_not_exists(true)
            .create()?;

        let secondary = space
            .index_builder("_pico_db_config_key")
            .unique(false)
            .part("key")
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space,
            primary,
            secondary,
        })
    }

    #[inline]
    pub fn get_or_default<T>(&self, key: &'static str, target_scope: &str) -> tarantool::Result<T>
    where
        T: DecodeOwned,
        T: serde::de::DeserializeOwned,
    {
        let global_scope = AlterSystemParameters::has_scope_global(key)
            .map_err(|err| ::tarantool::error::Error::other(err.to_string()))?;

        for (index, tuple) in self.by_key(key)?.enumerate() {
            // parameters with global scope are unique by it's name
            if cfg!(debug_assertions) && global_scope {
                assert!(index < 1);
            }

            let res: T = tuple
                .field(AlterSystemParameters::FIELD_VALUE)?
                .expect("field scope exists");

            if global_scope {
                return Ok(res);
            }

            let current_scope: &str = tuple
                .field(AlterSystemParameters::FIELD_SCOPE)?
                .expect("field scope exists");

            if target_scope == current_scope {
                return Ok(res);
            }
        }

        // it's not unreachable since some of parameters may be needed before filling _pico_db_config

        let value = config::get_default_value_of_alter_system_parameter(key)
            .expect("parameter name is validated using system_parameter_name! macro");
        let res = rmpv::ext::from_value(value).expect("default value type is correct");
        Ok(res)
    }

    #[inline(always)]
    pub fn by_key(&self, key: &str) -> tarantool::Result<EntryIter<Tuple, MP_SERDE>> {
        let iter = self.secondary.select(IteratorType::Eq, &[key])?;
        Ok(EntryIter::new(iter))
    }

    #[inline]
    pub fn auth_password_length_min(&self) -> tarantool::Result<usize> {
        self.get_or_default(
            system_parameter_name!(auth_password_length_min),
            Self::GLOBAL_SCOPE,
        )
    }

    #[inline]
    pub fn auth_password_enforce_uppercase(&self) -> tarantool::Result<bool> {
        self.get_or_default(
            system_parameter_name!(auth_password_enforce_uppercase),
            Self::GLOBAL_SCOPE,
        )
    }

    #[inline]
    pub fn auth_password_enforce_lowercase(&self) -> tarantool::Result<bool> {
        self.get_or_default(
            system_parameter_name!(auth_password_enforce_lowercase),
            Self::GLOBAL_SCOPE,
        )
    }

    #[inline]
    pub fn auth_password_enforce_digits(&self) -> tarantool::Result<bool> {
        self.get_or_default(
            system_parameter_name!(auth_password_enforce_digits),
            Self::GLOBAL_SCOPE,
        )
    }

    #[inline]
    pub fn auth_password_enforce_specialchars(&self) -> tarantool::Result<bool> {
        self.get_or_default(
            system_parameter_name!(auth_password_enforce_specialchars),
            Self::GLOBAL_SCOPE,
        )
    }

    #[inline]
    pub fn auth_login_attempt_max(&self) -> tarantool::Result<usize> {
        self.get_or_default(
            system_parameter_name!(auth_login_attempt_max),
            Self::GLOBAL_SCOPE,
        )
    }

    // NOTE: unlike the others, the four functions below do not access the underlying tarantool table
    // instead they are relying on `config::apply_parameter` putting up-to-date values into the static variables
    #[inline]
    pub fn pg_statement_max(&self) -> usize {
        config::DYNAMIC_CONFIG.pg_statement_max.current_value()
    }

    #[inline]
    pub fn observe_pg_statement_max(&self) -> AtomicObserver<usize> {
        config::DYNAMIC_CONFIG.pg_statement_max.make_observer()
    }

    #[inline]
    pub fn pg_portal_max(&self) -> usize {
        config::DYNAMIC_CONFIG.pg_statement_max.current_value()
    }

    #[inline]
    pub fn observe_pg_portal_max(&self) -> AtomicObserver<usize> {
        config::DYNAMIC_CONFIG.pg_statement_max.make_observer()
    }

    #[inline]
    pub fn raft_snapshot_chunk_size_max(&self) -> tarantool::Result<usize> {
        self.get_or_default(
            system_parameter_name!(raft_snapshot_chunk_size_max),
            Self::GLOBAL_SCOPE,
        )
    }

    #[inline]
    pub fn raft_snapshot_read_view_close_timeout(&self) -> tarantool::Result<Duration> {
        #[rustfmt::skip]
        let res: f64 = self.get_or_default(system_parameter_name!(raft_snapshot_read_view_close_timeout), Self::GLOBAL_SCOPE)?;
        Ok(Duration::from_secs_f64(res))
    }

    #[inline]
    pub fn governor_auto_offline_timeout(&self) -> tarantool::Result<Duration> {
        #[rustfmt::skip]
        let res: f64 = self.get_or_default(system_parameter_name!(governor_auto_offline_timeout), Self::GLOBAL_SCOPE)?;
        Ok(Duration::from_secs_f64(res))
    }

    #[inline]
    pub fn sql_motion_row_max(&self) -> i64 {
        config::DYNAMIC_CONFIG.sql_motion_row_max.current_value()
    }

    #[inline]
    pub fn sql_vdbe_opcode_max(&self) -> i64 {
        config::DYNAMIC_CONFIG.sql_vdbe_opcode_max.current_value()
    }

    /// Gets `sql_vdbe_opcode_max` and `sql_motion_row_max` options as [`sbroad::ir::options::Options`] struct
    #[inline]
    pub fn sql_query_options(&self) -> sbroad::ir::options::Options {
        let sql_vdbe_opcode_max = self.sql_vdbe_opcode_max();
        let sql_motion_row_max = self.sql_motion_row_max();
        sbroad::ir::options::Options {
            sql_motion_row_max,
            sql_vdbe_opcode_max,
        }
    }

    /// `tier` argument should be from set of existing tiers.
    pub fn sql_storage_cache_count_max(&self, tier: &str) -> tarantool::Result<usize> {
        self.get_or_default(system_parameter_name!(sql_storage_cache_count_max), tier)
    }

    #[inline]
    pub fn raft_wal_size_max(&self) -> tarantool::Result<u64> {
        self.get_or_default(
            system_parameter_name!(raft_wal_size_max),
            Self::GLOBAL_SCOPE,
        )
    }

    #[inline]
    pub fn raft_wal_count_max(&self) -> tarantool::Result<u64> {
        self.get_or_default(
            system_parameter_name!(raft_wal_count_max),
            Self::GLOBAL_SCOPE,
        )
    }

    #[inline]
    pub fn governor_raft_op_timeout(&self) -> tarantool::Result<f64> {
        self.get_or_default(
            system_parameter_name!(governor_raft_op_timeout),
            Self::GLOBAL_SCOPE,
        )
    }

    #[inline]
    pub fn governor_common_rpc_timeout(&self) -> tarantool::Result<f64> {
        self.get_or_default(
            system_parameter_name!(governor_common_rpc_timeout),
            Self::GLOBAL_SCOPE,
        )
    }

    #[inline]
    pub fn governor_plugin_rpc_timeout(&self) -> tarantool::Result<f64> {
        self.get_or_default(
            system_parameter_name!(governor_plugin_rpc_timeout),
            Self::GLOBAL_SCOPE,
        )
    }

    #[inline]
    pub fn shredding(&self) -> tarantool::Result<Option<bool>> {
        if let Some(shredding) = self.by_key(config::SHREDDING_PARAM_NAME)?.next() {
            Ok(Some(
                shredding
                    .field(AlterSystemParameters::FIELD_VALUE)?
                    .expect("field value exists"),
            ))
        } else {
            Ok(None)
        }
    }
}

impl ToEntryIter<MP_SERDE> for DbConfig {
    type Entry = Tuple;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

/// Ignore specific tarantool error.
/// E.g. if `res` contains an `ignored` error, it will be
/// transformed to ok instead.
pub(in crate::storage) fn ignore_only_error(
    res: tarantool::Result<()>,
    ignored: TntErrorCode,
) -> tarantool::Result<()> {
    if let Err(err) = res {
        // FIXME: use IntoBoxError::error_code
        if let TntError::Tarantool(tnt_err) = &err {
            if tnt_err.error_code() == ignored as u32 {
                return Ok(());
            }
        }
        return Err(err);
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// local schema version
////////////////////////////////////////////////////////////////////////////////

pub fn local_schema_version() -> tarantool::Result<u64> {
    let space_schema = Space::from(SystemSpace::Schema);
    let tuple = space_schema.get(&["local_schema_version"])?;
    let mut res = INITIAL_SCHEMA_VERSION;
    if let Some(tuple) = tuple {
        if let Some(v) = tuple.field(1)? {
            res = v;
        }
    }
    Ok(res)
}

pub fn set_local_schema_version(v: u64) -> tarantool::Result<()> {
    let space_schema = Space::from(SystemSpace::Schema);
    space_schema.replace(&("local_schema_version", v))?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// max space id
////////////////////////////////////////////////////////////////////////////////

pub const SPACE_ID_INTERNAL_MAX: u32 = 1024;

////////////////////////////////////////////////////////////////////////////////
// misc
////////////////////////////////////////////////////////////////////////////////

/// Add a on_replace trigger for space with `space_id` so that `cb` is called
/// each time the space is updated.
#[inline]
pub fn on_replace<F>(space_id: SpaceId, mut cb: F) -> tarantool::Result<()>
where
    F: FnMut(Option<Tuple>, Option<Tuple>) -> Result<()> + 'static,
{
    // FIXME: rewrite using ffi-api when it's available
    // See: https://git.picodata.io/picodata/picodata/tarantool-module/-/issues/130
    let lua = ::tarantool::lua_state();
    let on_replace = tlua::Function::new(
        move |old: Option<Tuple>, new: Option<Tuple>, lua: tlua::LuaState| {
            if let Err(e) = cb(old, new) {
                tlua::error!(lua, "{}", e);
            }
        },
    );
    lua.exec_with(
        "local space_id, callback = ...
        box.space[space_id]:on_replace(callback)",
        (space_id, on_replace),
    )
    .map_err(tlua::LuaError::from)?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

macro_rules! assert_err {
    ($expr:expr, $err:expr) => {
        assert_eq!($expr.unwrap_err().to_string(), $err)
    };
}

mod tests {
    use crate::tier::DEFAULT_TIER;

    use super::*;
    use crate::schema::fields_to_format;
    use tarantool::index::Metadata as IndexMetadata;
    use tarantool::space::Metadata as SpaceMetadata;

    #[rustfmt::skip]
    #[::tarantool::test]
    fn test_storage_instances() {
        use crate::instance::StateVariant::*;
        use crate::instance::InstanceName;
        use crate::failure_domain::FailureDomain;

        let storage = Catalog::for_tests();
        let storage_peer_addresses = PeerAddresses::new().unwrap();
        let space_peer_addresses = storage_peer_addresses.space.clone();

        let faildom = FailureDomain::from([("a", "b")]);
        let picodata_version = PICODATA_VERSION.to_string();

        for instance in vec![
            // r1
            ("i1", "i1-uuid", 1u64, "r1", "r1-uuid", (Online, 0), (Online, 0), &faildom, DEFAULT_TIER, &picodata_version),
            ("i2", "i2-uuid", 2u64, "r1", "r1-uuid", (Online, 0), (Online, 0), &faildom, DEFAULT_TIER, &picodata_version),
            // r2
            ("i3", "i3-uuid", 3u64, "r2", "r2-uuid", (Online, 0), (Online, 0), &faildom, DEFAULT_TIER, &picodata_version),
            ("i4", "i4-uuid", 4u64, "r2", "r2-uuid", (Online, 0), (Online, 0), &faildom, DEFAULT_TIER, &picodata_version),
            // r3
            ("i5", "i5-uuid", 5u64, "r3", "r3-uuid", (Online, 0), (Online, 0), &faildom, DEFAULT_TIER, &picodata_version),
        ] {
            space_by_name(Instances::TABLE_NAME).unwrap().put(&instance).unwrap();
            let (_, _, raft_id, ..) = instance;
            space_peer_addresses.put(&(raft_id, format!("addr:{raft_id}"), &traft::ConnectionType::Iproto)).unwrap();
        }

        let instance = storage.instances.all_instances().unwrap();
        assert_eq!(
            instance.iter().map(|p| &p.name).collect::<Vec<_>>(),
            vec!["i1", "i2", "i3", "i4", "i5"]
        );

        assert_err!(
            storage.instances.put(&Instance {
                raft_id: 1,
                name: "i99".into(),
                tier: DEFAULT_TIER.into(),
                picodata_version: PICODATA_VERSION.to_string(),
                ..Instance::default()
            }),
            format!(
                concat!(
                    "box error:",
                    " TupleFound: Duplicate key exists",
                    " in unique index \"_pico_instance_raft_id\"",
                    " in space \"_pico_instance\"",
                    " with old tuple",
                    r#" - ["i1", "i1-uuid", 1, "r1", "r1-uuid", ["{gon}", 0], ["{tgon}", 0], {{"A": "B"}}, "default", "{picodata_version}"]"#,
                    " and new tuple",
                    r#" - ["i99", "", 1, "", "", ["{goff}", 0], ["{tgoff}", 0], {{}}, "default", "{picodata_version}"]"#,
                ),
                gon = Online,
                goff = Offline,
                tgon = Online,
                tgoff = Offline,
                picodata_version = PICODATA_VERSION.to_string(),
            )
        );

        {
            // Ensure traft storage doesn't impose restrictions
            // on peer_address uniqueness.
            storage_peer_addresses.put(10, &traft::Address::from("addr:collision"), &traft::ConnectionType::Iproto).unwrap();
            storage_peer_addresses.put(11, &traft::Address::from("addr:collision"), &traft::ConnectionType::Iproto).unwrap();
        }

        {
            // Check accessing instances by 'raft_id'
            assert_eq!(storage.instances.get(&1).unwrap().name, "i1");
            assert_eq!(storage.instances.get(&2).unwrap().name, "i2");
            assert_eq!(storage.instances.get(&3).unwrap().name, "i3");
            assert_eq!(storage.instances.get(&4).unwrap().name, "i4");
            assert_eq!(storage.instances.get(&5).unwrap().name, "i5");
            assert_err!(storage.instances.get(&6), "instance with raft_id 6 not found");
        }

        {
            // Check accessing instances by 'instance name'
            assert_eq!(storage.instances.get(&InstanceName::from("i1")).unwrap().raft_id, 1);
            assert_eq!(storage.instances.get(&InstanceName::from("i2")).unwrap().raft_id, 2);
            assert_eq!(storage.instances.get(&InstanceName::from("i3")).unwrap().raft_id, 3);
            assert_eq!(storage.instances.get(&InstanceName::from("i4")).unwrap().raft_id, 4);
            assert_eq!(storage.instances.get(&InstanceName::from("i5")).unwrap().raft_id, 5);
            assert_err!(
                storage.instances.get(&InstanceName::from("i6")),
                "instance with name \"i6\" not found"
            );
        }

        let box_replication = |replicaset_name: &str| -> Vec<traft::Address> {
            storage.instances.replicaset_instances(replicaset_name).unwrap()
                .map(|instance| storage_peer_addresses.try_get(instance.raft_id, &traft::ConnectionType::Iproto).unwrap())
                .collect::<Vec<_>>()
        };

        {
            assert_eq!(box_replication("XX"), Vec::<&str>::new());
            assert_eq!(box_replication("r1"), ["addr:1", "addr:2"]);
            assert_eq!(box_replication("r2"), ["addr:3", "addr:4"]);
            assert_eq!(box_replication("r3"), ["addr:5"]);
        }

        let space = space_by_name(Instances::TABLE_NAME).unwrap();
        space.drop().unwrap();

        assert_err!(
            storage.instances.all_instances(),
            format!(
                "box error: NoSuchSpace: Space '{}' does not exist",
                space.id(),
            )
        );
    }

    #[::tarantool::test]
    fn clusterwide_space_index() {
        let storage = Catalog::for_tests();

        space_by_name(PeerAddresses::TABLE_NAME)
            .unwrap()
            .insert(&(1, "foo", &traft::ConnectionType::Iproto))
            .unwrap();
        space_by_name(PeerAddresses::TABLE_NAME)
            .unwrap()
            .insert(&(2, "bar", &traft::ConnectionType::Pgproto))
            .unwrap();

        assert_eq!(
            storage
                .peer_addresses
                .get(1, &traft::ConnectionType::Iproto)
                .unwrap()
                .unwrap(),
            "foo"
        );
        assert_eq!(
            storage
                .peer_addresses
                .get(2, &traft::ConnectionType::Pgproto)
                .unwrap()
                .unwrap(),
            "bar"
        );

        let inst = |raft_id, instance_name: &str| Instance {
            raft_id,
            name: instance_name.into(),
            ..Instance::default()
        };
        storage.instances.put(&inst(1, "bob")).unwrap();

        let t = storage.instances.index_raft_id.get(&[1]).unwrap().unwrap();
        let i: Instance = t.decode().unwrap();
        assert_eq!(i.raft_id, 1);
        assert_eq!(i.name, "bob");
    }

    #[track_caller]
    fn get_field_index(part: &Part<String>, space_fields: &[tarantool::space::Field]) -> usize {
        let name = &part.field;

        let found = space_fields
            .iter()
            .zip(0..)
            .find(|(field, _)| &field.name == name);

        let Some((_, index)) = found else {
            panic!("index part `{name}` doesn't correspond to any field in {space_fields:?}");
        };

        return index;
    }

    #[track_caller]
    fn check_index_parts_match(
        index_def: &IndexDef,
        picodata_index_parts: &[Part<String>],
        tarantool_index_parts: &[Part<u32>],
        space_fields: &[tarantool::space::Field],
    ) {
        assert_eq!(
            picodata_index_parts.len(),
            tarantool_index_parts.len(),
            "{index_def:?}"
        );

        for (pd_part, tt_part) in picodata_index_parts.iter().zip(tarantool_index_parts) {
            if let (Some(pd_type), Some(tt_type)) = (pd_part.r#type, tt_part.r#type) {
                assert_eq!(pd_type, tt_type, "{index_def:?}");
            } else {
                // Ignore
            }

            assert_eq!(pd_part.collation, tt_part.collation, "{index_def:?}");
            assert_eq!(
                pd_part.is_nullable.unwrap_or(false),
                tt_part.is_nullable.unwrap_or(false),
                "{index_def:?}"
            );
            assert_eq!(pd_part.path, tt_part.path, "{index_def:?}");

            let pd_field_index = get_field_index(pd_part, space_fields);
            let tt_field_index = tt_part.field as usize;
            assert_eq!(pd_field_index, tt_field_index, "{index_def:?}");
        }
    }

    #[track_caller]
    fn get_number_of_indexes_defined_for_space(space_id: SpaceId) -> usize {
        let sys_index = SystemSpace::Index.as_space();
        let iter = sys_index.select(IteratorType::Eq, &[space_id]).unwrap();
        return iter.count();
    }

    #[::tarantool::test]
    fn builtin_clusterwide_schema() {
        // Make sure storage is initialized
        let storage = Catalog::for_tests();

        let sys_space = SystemSpace::Space.as_space();
        let sys_index = SystemSpace::Index.as_space();

        for sys_table in storage.pico_table.iter().unwrap() {
            //
            // box.space._space
            //

            // Check space metadata is in tarantool's "_space"
            let tuple = sys_space.get(&[sys_table.id]).unwrap().unwrap();
            let tt_space_def: SpaceMetadata = tuple.decode().unwrap();

            // This check is a bit redundant, but better safe than sorry
            assert_eq!(tt_space_def.id, sys_table.id);
            assert_eq!(tt_space_def.name, sys_table.name);

            // Check "_space" agrees with `SystemTable.format`
            assert_eq!(tt_space_def.format, fields_to_format(&sys_table.format));

            //
            // box.space._pico_table
            //

            // Check table definition is in picodata's "_pico_table"
            let pico_table_def = storage.pico_table.get(sys_table.id).unwrap().unwrap();

            // Check picodata & tarantool agree on the definition
            assert_eq!(pico_table_def.to_space_metadata().unwrap(), tt_space_def);

            let index_definitions: Vec<_> =
                storage.indexes.by_space_id(sys_table.id).unwrap().collect();
            assert_eq!(
                index_definitions.len(),
                get_number_of_indexes_defined_for_space(sys_table.id),
                "Mismatched number of indexes defined for table '{}'",
                sys_table.name,
            );
            for mut index_def in index_definitions {
                assert_eq!(index_def.table_id, sys_table.id);

                //
                // box.space._pico_index
                //

                // Check index definition is in picodata's "_pico_index"
                #[rustfmt::skip]
                let pico_index_def = storage.indexes.get(index_def.table_id, index_def.id).unwrap().unwrap();

                // Check "_pico_index" agrees with `SystemTable.index_definitions`
                assert_eq!(pico_index_def, index_def);

                //
                // box.space._index
                //

                // Check index metadata is in tarantool's "_index"
                #[rustfmt::skip]
                let tuple = sys_index.get(&[index_def.table_id, index_def.id]).unwrap().unwrap();
                let mut tt_index_def: IndexMetadata = tuple.decode().unwrap();

                // Check parts separately from other fields
                let parts_as_known_by_picodata = std::mem::take(&mut index_def.parts);
                let parts_as_known_by_tarantool = std::mem::take(&mut tt_index_def.parts);

                check_index_parts_match(
                    &index_def,
                    &parts_as_known_by_picodata,
                    &parts_as_known_by_tarantool,
                    &sys_table.format,
                );

                // Check "_index" agrees with `SystemTable.index_definitions`
                let pico_index_def = index_def.to_index_metadata(&pico_table_def);
                assert_eq!(pico_index_def.space_id, tt_index_def.space_id);
                assert_eq!(pico_index_def.index_id, tt_index_def.index_id);
                assert_eq!(pico_index_def.r#type, tt_index_def.r#type);
                assert_eq!(pico_index_def.opts, tt_index_def.opts);
                assert_eq!(pico_index_def.parts, tt_index_def.parts);
            }
        }
    }

    #[::tarantool::test]
    fn system_space_name_by_id() {
        // Make sure storage is initialized
        let storage = Catalog::for_tests();

        for table in storage.pico_table.iter().unwrap() {
            assert_eq!(
                SYSTEM_TABLES_ID_RANGE.contains(&table.id),
                Catalog::system_space_name_by_id(table.id).is_some()
            )
        }
    }
}
