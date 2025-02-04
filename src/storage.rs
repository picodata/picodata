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
use tarantool::tuple::DecodeOwned;
use tarantool::tuple::KeyDef;
use tarantool::tuple::{RawBytes, ToTupleBuffer, Tuple};
use tarantool::util::NumOrStr;

use crate::config;
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
use crate::storage::snapshot::SnapshotCache;
use crate::system_parameter_name;
use crate::tarantool::box_schema_version;
use crate::tier::Tier;
use crate::tlog;
use crate::traft;
use crate::traft::error::{Error, IdOfInstance};
use crate::traft::op::Ddl;
use crate::traft::op::Dml;
use crate::traft::RaftId;
use crate::traft::Result;
use crate::util::Uppercase;

use rmpv::Utf8String;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use pico_proc_macro::get_doc_literal;

pub mod schema;
pub mod snapshot;

macro_rules! define_clusterwide_tables {
    (
        $(#[$Clusterwide_meta:meta])*
        pub struct $Clusterwide:ident {
            pub #space_name_lower: #space_name_upper,
            $(
                $(#[$Clusterwide_extra_field_meta:meta])*
                pub $Clusterwide_extra_field:ident: $Clusterwide_extra_field_type:ty,
            )*
        }

        $(#[$ClusterwideTable_meta:meta])*
        pub enum $ClusterwideTable:ident {
            $(
                $(#[$cw_field_meta:meta])*
                $cw_space_var:ident = $cw_space_id:expr, $cw_space_name:expr => {
                    $_Clusterwide:ident :: $Clusterwide_field:ident;

                    $(#[$space_struct_meta:meta])*
                    pub struct $space_struct:ident {
                        $space_field:ident: $space_ty:ty,
                        #[primary]
                        $index_field_pk:ident: $index_ty_pk:ty => $index_name_pk:expr,
                        $( $index_field:ident: $index_ty:ty => $index_name:expr, )*
                    }
                }
            )+
        }
    ) => {
        ////////////////////////////////////////////////////////////////////////
        // ClusterwideTable
        ::tarantool::define_str_enum! {
            $(#[$ClusterwideTable_meta])*
            pub enum $ClusterwideTable {
                $(
                    $(#[$cw_field_meta])*
                    $cw_space_var = $cw_space_name = $cw_space_id,
                )+
            }
        }

        impl $ClusterwideTable {
            pub fn format(&self) -> Vec<tarantool::space::Field> {
                match self {
                    $( Self::$cw_space_var => $space_struct::format(), )+
                }
            }

            pub fn index_definitions(&self) -> Vec<$crate::schema::IndexDef> {
                match self {
                    $( Self::$cw_space_var => $space_struct::index_definitions(), )+
                }
            }

            const fn index_names(&self) -> &'static [&'static str] {
                match self {
                    $( Self::$cw_space_var => $space_struct::INDEX_NAMES, )+
                }
            }

            const fn struct_name(&self) -> &'static str {
                match self {
                    $( Self::$cw_space_var => ::std::stringify!($space_struct), )+
                }
            }

            pub fn description(&self) -> String {
                match self {
                    $( Self::$cw_space_var => $space_struct::description_from_doc_comments(), )+
                }
            }

            #[allow(unused)]
            const fn doc_comments_raw(&self) -> &'static [&'static str] {
                match self {
                    $( Self::$cw_space_var => $space_struct::DOC_COMMENTS_RAW, )+
                }
            }
        }

        const _TEST_ID_AND_NAME_ARE_CORRECT: () = {
            use $crate::util::str_eq;
            $(
                assert!($ClusterwideTable::$cw_space_var.id() == $cw_space_id);
                assert!(str_eq($ClusterwideTable::$cw_space_var.name(), $cw_space_name));
            )+
        };

        $( const _: $crate::util::CheckIsSameType<$_Clusterwide, $Clusterwide> = (); )+

        ////////////////////////////////////////////////////////////////////////
        // Clusterwide
        $(#[$Clusterwide_meta])*
        #[derive(Clone, Debug)]
        pub struct $Clusterwide {
            $( pub $Clusterwide_field: $space_struct, )+
            $(
                $(#[$Clusterwide_extra_field_meta])*
                pub $Clusterwide_extra_field: $Clusterwide_extra_field_type,
            )*
        }

        impl $Clusterwide {
            /// Initialize the clusterwide storage.
            ///
            /// This function is private because it should only be called once
            /// per picodata instance on boot.
            #[inline(always)]
            fn initialize() -> tarantool::Result<Self> {
                // SAFETY: safe as long as only called from tx thread.
                static mut WAS_CALLED: bool = false;
                unsafe {
                    assert!(!WAS_CALLED, "Clusterwide storage must only be initialized once");
                    WAS_CALLED = true;
                }

                Ok(Self {
                    $( $Clusterwide_field: $space_struct::new()?, )+
                    $( $Clusterwide_extra_field: Default::default(), )*
                })
            }

            #[inline(always)]
            fn space_by_name(&self, space: impl AsRef<str>) -> tarantool::Result<Space> {
                let space = space.as_ref();
                match space {
                    $( $cw_space_name => Ok(self.$Clusterwide_field.space.clone()), )+
                    _ => space_by_name(space),
                }
            }

            #[inline(always)]
            fn space_by_id(&self, space: SpaceId) -> tarantool::Result<Space> {
                match space {
                    $( $cw_space_id => Ok(self.$Clusterwide_field.space.clone()), )+
                    _ => space_by_id(space),
                }
            }

            /// Apply `cb` to each clusterwide space.
            #[inline(always)]
            pub fn for_each_space(
                &self,
                mut cb: impl FnMut(&Space) -> tarantool::Result<()>,
            ) -> tarantool::Result<()>
            {
                $( cb(&self.$Clusterwide_field.space)?; )+
                Ok(())
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // Instances, Replicasets, etc.
        $(
            $(#[$space_struct_meta])*
            #[derive(Clone, Debug)]
            pub struct $space_struct {
                $space_field: $space_ty,
                #[allow(unused)]
                $index_field_pk: $index_ty_pk,
                $( $index_field: $index_ty, )*
            }

            impl TClusterwideTable for $space_struct {
                const TABLE_NAME: &'static str = $cw_space_name;
                const TABLE_ID: SpaceId = $cw_space_id;
                const INDEX_NAMES: &'static [&'static str] = &[
                    $index_name_pk,
                    $( $index_name, )*
                ];
                const DOC_COMMENTS_RAW: &'static [&'static str] = &[
                    $(get_doc_literal!($space_struct_meta), )*
                ];
            }
        )+
    }
}

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

////////////////////////////////////////////////////////////////////////////////
// Clusterwide
////////////////////////////////////////////////////////////////////////////////

define_clusterwide_tables! {
    pub struct Clusterwide {
        pub #space_name_lower: #space_name_upper,
        pub snapshot_cache: Rc<SnapshotCache>,
        // It's ok to lose this information during restart.
        pub login_attempts: Rc<RefCell<HashMap<String, usize>>>,
    }

    /// An enumeration of builtin cluster-wide tables.
    ///
    /// Use [`Self::id`] to get [`SpaceId`].
    /// Use [`Self::name`] to get space name.
    /// Each variant (e.g. system space) should have `new` and `format` methods in its implementation.
    pub enum ClusterwideTable {
        Table = 512, "_pico_table" => {
            Clusterwide::tables;

            /// A struct for accessing definitions of all the user-defined tables.
            ///
            /// # Table description
            ///
            /// Stores metadata of all the cluster tables in picodata.
            pub struct Tables {
                space: Space,
                #[primary]
                index_id:       Index => "_pico_table_id",
                index_name:     Index => "_pico_table_name",
                index_owner_id: Index => "_pico_table_owner_id",
            }
        }
        Index = 513, "_pico_index" => {
            Clusterwide::indexes;

            /// A struct for accessing definitions of all the user-defined indexes.
            pub struct Indexes {
                space: Space,
                #[primary]
                index_id:   Index => "_pico_index_id",
                index_name: Index => "_pico_index_name",
            }
        }
        Address = 514, "_pico_peer_address" => {
            Clusterwide::peer_addresses;

            /// A struct for accessing storage of peer addresses.
            pub struct PeerAddresses {
                space: Space,
                #[primary]
                index: Index => "_pico_peer_address_raft_id",
            }
        }
        Instance = 515, "_pico_instance" => {
            Clusterwide::instances;

            /// A struct for accessing storage of all the cluster instances.
            pub struct Instances {
                space: Space,
                #[primary]
                index_instance_name:   Index => "_pico_instance_name",
                index_instance_uuid:   Index => "_pico_instance_uuid",
                index_raft_id:       Index => "_pico_instance_raft_id",
                index_replicaset_name: Index => "_pico_instance_replicaset_name",
            }
        }
        Property = 516, "_pico_property" => {
            Clusterwide::properties;

            /// A struct for accessing storage of the cluster-wide key-value properties
            pub struct Properties {
                space: Space,
                #[primary]
                index: Index => "_pico_property_key",
            }
        }
        Replicaset = 517, "_pico_replicaset" => {
            Clusterwide::replicasets;

            /// A struct for accessing replicaset info from storage
            pub struct Replicasets {
                space: Space,
                #[primary]
                index_replicaset_name:   Index => "_pico_replicaset_name",
                index_replicaset_uuid: Index => "_pico_replicaset_uuid",
            }
        }

        User = 520, "_pico_user" => {
            Clusterwide::users;

            /// A struct for accessing info of all the user-defined users.
            pub struct Users {
                space: Space,
                #[primary]
                index_id:   Index => "_pico_user_id",
                index_name: Index => "_pico_user_name",
                index_owner_id: Index => "_pico_user_owner_id",
            }
        }
        Privilege = 521, "_pico_privilege" => {
            Clusterwide::privileges;

            /// A struct for accessing info of all privileges granted to
            /// user-defined users.
            pub struct Privileges {
                space: Space,
                #[primary]
                primary_key: Index => "_pico_privilege_primary",
                object_idx:  Index => "_pico_privilege_object",
            }
        }
        Tier = 523, "_pico_tier" => {
            Clusterwide::tiers;

            /// A struct for accessing info of all tiers in cluster.
            pub struct Tiers {
                space: Space,
                #[primary]
                index_name: Index => "_pico_tier_name",
            }
        }
        Routine = 524, "_pico_routine" => {
            Clusterwide::routines;

            /// A struct for accessing info of all the user-defined routines.
            pub struct Routines {
                space: Space,
                #[primary]
                index_id:       Index => "_pico_routine_id",
                index_name:     Index => "_pico_routine_name",
                index_owner_id: Index => "_pico_routine_owner_id",
            }
        }
        Plugin = 526, "_pico_plugin" => {
            Clusterwide::plugins;

            /// A struct for accessing info of all known plugins.
            pub struct Plugins {
                space: Space,
                #[primary]
                primary_key: Index => "_pico_plugin_name",
            }
        }
        Service = 527, "_pico_service" => {
            Clusterwide::services;

            /// A struct for accessing info of all known plugin services.
            pub struct Services {
                space: Space,
                #[primary]
                index_name: Index => "_pico_service_name",
            }
        }
        ServiceRouteTable = 528, "_pico_service_route" => {
            Clusterwide::service_route_table;

            /// A struct for accessing info of plugin services routing table.
            pub struct ServiceRouteTable {
                space: Space,
                #[primary]
                primary_key: Index => "_pico_service_routing_key",
            }
        }
        PluginMigration = 529, "_pico_plugin_migration" => {
            Clusterwide::plugin_migrations;

            /// A struct for accessing info of applied plugin migrations.
            pub struct PluginMigration {
                space: Space,
                #[primary]
                primary_key: Index => "_pico_plugin_migration_primary_key",
            }
        }
        PluginConfig = 530, "_pico_plugin_config" => {
            Clusterwide::plugin_config;

            /// Structure for storing plugin configuration.
            /// Configuration is represented by a set of key-value pairs
            /// belonging to either service or extension.
            pub struct PluginConfig {
                space: Space,
                #[primary]
                primary: Index => "_pico_plugin_config_pk",
            }
        }
        DbConfig = 531, "_pico_db_config" => {
            Clusterwide::db_config;

            /// A struct for accessing storage of the cluster-wide and tier-wide configs that
            /// can be modified via alter system.
            pub struct DbConfig {
                space: Space,
                #[primary]
                index: Index => "_pico_db_config_key",
            }
        }
    }
}

/// Id of system table `_bucket`. Note that we don't add in to `Clusterwide`
/// because we don't control how it's created. Instead we use vshard to create
/// it.
///
/// The id is reserved because it must be the same on all instances.
pub const TABLE_ID_BUCKET: SpaceId = 532;

impl Clusterwide {
    /// Get a reference to a global instance of clusterwide storage.
    /// If `init` is true, will do the initialization which may involve creation
    /// of system spaces. This should only be done at instance initialization in
    /// init_common.
    /// Returns an error
    ///   - if `init` is `false` and storage is not initialized.
    ///   - if `init` is `true` and storage initialization failed.
    #[inline]
    pub fn try_get(init: bool) -> Result<&'static Self> {
        static mut STORAGE: Option<Clusterwide> = None;

        // Safety: this is safe as long as we only use it in tx thread.
        unsafe {
            if STORAGE.is_none() {
                if !init {
                    return Err(Error::Uninitialized);
                }
                STORAGE = Some(Self::initialize()?);
            }
            Ok(STORAGE.as_ref().unwrap())
        }
    }

    /// Get a reference to a global instance of clusterwide storage.
    ///
    /// # Panicking
    /// Will panic if storage is not initialized before a call to this function.
    /// Consider using [`Clusterwide::try_get`] if this is a problem.
    #[inline(always)]
    pub fn get() -> &'static Self {
        Self::try_get(false).expect("shouldn't be calling this until it's initialized")
    }

    /// Get an instance of clusterwide storage for use in unit tests.
    ///
    /// Should only be used in tests.
    pub(crate) fn for_tests() -> Self {
        let storage = Self::initialize().unwrap();

        // Add system tables
        for (table, index_defs) in crate::schema::system_table_definitions() {
            storage.tables.put(&table).unwrap();
            for index in index_defs {
                storage.indexes.put(&index).unwrap();
            }
        }

        storage
    }

    fn global_table_name(&self, id: SpaceId) -> Result<Cow<'static, str>> {
        let Some(space_def) = self.tables.get(id)? else {
            return Err(Error::other(format!("global space #{id} not found")));
        };
        Ok(space_def.name.into())
    }

    /// Perform the `dml` operation on the local storage.
    /// When possible, return the new tuple produced by the operation:
    ///   * `Some(tuple)` in case of insert and replace;
    ///   * `Some(tuple)` or `None` depending on update's result (it may be NOP);
    ///   * `None` in case of delete (because the tuple is gone).
    #[inline]
    pub fn do_dml(&self, dml: &Dml) -> tarantool::Result<Option<Tuple>> {
        let space = space_by_id_unchecked(dml.space());
        match dml {
            Dml::Insert { tuple, .. } => space.insert(tuple).map(Some),
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
    // Safety: this is only called from main thread
    let system_key_defs = unsafe { SYSTEM_KEY_DEFS.get_or_insert_with(HashMap::new) };
    if ClusterwideTable::try_from(space_id).is_ok() {
        // System table definition's never change during a single
        // execution, so it's safe to cache these
        let key_def = get_or_create_key_def(system_key_defs, id)?;
        return Ok(key_def);
    }

    static mut USER_KEY_DEFS: Option<(u64, KeyDefCache)> = None;
    let (schema_version, user_key_defs) =
        // Safety: this is only called from main thread
        unsafe { USER_KEY_DEFS.get_or_insert_with(|| (0, HashMap::new())) };
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

////////////////////////////////////////////////////////////////////////////////
// ClusterwideTable
////////////////////////////////////////////////////////////////////////////////

impl ClusterwideTable {
    /// Id of the corresponding system global space.
    #[inline(always)]
    pub const fn id(&self) -> SpaceId {
        *self as _
    }

    /// Name of the corresponding system global space.
    #[inline(always)]
    pub const fn name(&self) -> &'static str {
        self.as_str()
    }

    /// A slice of all possible variants of `Self`.
    /// Guaranteed to return spaces in ascending order of their id
    #[inline(always)]
    pub const fn all_tables() -> &'static [Self] {
        Self::VARIANTS
    }

    #[inline]
    pub(crate) fn get(&self) -> tarantool::Result<Space> {
        Space::find_cached(self.as_str()).ok_or_else(|| {
            tarantool::set_error!(
                tarantool::error::TarantoolErrorCode::NoSuchSpace,
                "no such space \"{}\"",
                self
            );
            tarantool::error::TarantoolError::last().into()
        })
    }

    #[inline]
    pub fn insert(&self, tuple: &impl ToTupleBuffer) -> tarantool::Result<Tuple> {
        self.get()?.insert(tuple)
    }

    #[inline]
    pub fn replace(&self, tuple: &impl ToTupleBuffer) -> tarantool::Result<Tuple> {
        self.get()?.replace(tuple)
    }
}

/// Types implementing this trait represent clusterwide spaces.
pub trait TClusterwideTable {
    const TABLE_NAME: &'static str;
    const TABLE_ID: SpaceId;
    const INDEX_NAMES: &'static [&'static str];
    const DOC_COMMENTS_RAW: &'static [&'static str];

    fn description_from_doc_comments() -> String {
        let mut res = String::with_capacity(256);

        let mut lines = Self::DOC_COMMENTS_RAW.iter();
        for line in &mut lines {
            if line.trim() == "# Table description" {
                break;
            }
        }

        for line in lines {
            let line = line.trim();
            if res.is_empty() && line.is_empty() {
                continue;
            }
            res.push_str(line.trim());
            res.push('\n');
        }

        // Remove any trailing whitespace
        while let Some(last) = res.pop() {
            if !last.is_whitespace() {
                res.push(last);
                break;
            }
        }

        res
    }
}

impl From<ClusterwideTable> for SpaceId {
    #[inline(always)]
    fn from(space: ClusterwideTable) -> SpaceId {
        space.id()
    }
}

////////////////////////////////////////////////////////////////////////////////
// PropertyName
////////////////////////////////////////////////////////////////////////////////

::tarantool::define_str_enum! {
    /// An enumeration of [`ClusterwideTable::Property`] key names.
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
    }
}

impl PropertyName {
    /// Returns `true` if this property cannot be deleted. These are usually the
    /// values only updated by picodata and some subsystems rely on them always
    /// being set (or not being unset).
    #[inline(always)]
    fn must_not_delete(&self) -> bool {
        matches!(self, |Self::GlobalSchemaVersion| Self::NextSchemaVersion
            | Self::ClusterVersion)
    }

    /// Verify type of the property value being inserted (or replaced) in the
    /// _pico_property table. `self` is the first field of the `new` tuple.
    #[inline]
    fn verify_new_tuple(&self, new: &Tuple) -> Result<()> {
        // TODO: some of these properties are only supposed to be updated by
        // picodata. Maybe for these properties we should check the effective
        // user id and if it's not admin we deny the change. We'll have to set
        // effective user id to the one who requested the dml in proc_cas.

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
            | Self::SystemCatalogVersion => {
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
            Self::ClusterVersion => {
                // Check it's a String
                _ = new.field::<String>(1).map_err(map_err)?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////
// Properties
////////////////////////////////////////////////////////////////////////////////

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

    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("key", FieldType::String)),
            Field::from(("value", FieldType::Any)),
        ]
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
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
}

////////////////////////////////////////////////////////////////////////////////
// Replicasets
////////////////////////////////////////////////////////////////////////////////

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

    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        Replicaset::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
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
// PeerAddresses
////////////////////////////////////////////////////////////////////////////////

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
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, index })
    }

    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("raft_id", FieldType::Unsigned)),
            Field::from(("address", FieldType::String)),
        ]
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            // Primary index
            id: 0,
            name: "_pico_peer_address_raft_id".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![Part::from(("raft_id", IndexFieldType::Unsigned)).is_nullable(false)],
            operable: true,
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
        }]
    }

    #[inline]
    pub fn put(&self, raft_id: RaftId, address: &traft::Address) -> tarantool::Result<()> {
        self.space.replace(&(raft_id, address))?;
        Ok(())
    }

    #[allow(dead_code)]
    #[inline]
    pub fn delete(&self, raft_id: RaftId) -> tarantool::Result<()> {
        self.space.delete(&[raft_id])?;
        Ok(())
    }

    #[inline(always)]
    pub fn get(&self, raft_id: RaftId) -> Result<Option<traft::Address>> {
        let Some(tuple) = self.space.get(&[raft_id])? else {
            return Ok(None);
        };
        tuple.field(1).map_err(Into::into)
    }

    #[inline(always)]
    pub fn try_get(&self, raft_id: RaftId) -> Result<traft::Address> {
        self.get(raft_id)?
            .ok_or(Error::AddressUnknownForRaftId(raft_id))
    }

    #[inline]
    pub fn addresses_by_ids(
        &self,
        ids: impl IntoIterator<Item = RaftId>,
    ) -> Result<HashSet<traft::Address>> {
        ids.into_iter().map(|id| self.try_get(id)).collect()
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
// Instance
////////////////////////////////////////////////////////////////////////////////

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

    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        Instance::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
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

    /// Finds an instance by `name` (see trait [`InstanceName`]).
    #[inline(always)]
    pub fn get(&self, name: &impl InstanceName) -> Result<Instance> {
        let res = name
            .find_in(self)?
            .decode()
            .expect("failed to decode instance");
        Ok(res)
    }

    /// Finds an instance by `name` (see trait [`InstanceName`]).
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
// Tables
////////////////////////////////////////////////////////////////////////////////

impl Tables {
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
    pub fn format() -> Vec<tarantool::space::Field> {
        TableDef::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
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

impl ToEntryIter<MP_CUSTOM> for Tables {
    type Entry = TableDef;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// Indexes
////////////////////////////////////////////////////////////////////////////////

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

    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        IndexDef::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
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
// Users
////////////////////////////////////////////////////////////////////////////////

/// The hard upper bound (32) for max users comes from tarantool BOX_USER_MAX
const MAX_USERS: usize = 32;

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

    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        UserDef::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
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
    pub fn format() -> Vec<tarantool::space::Field> {
        PrivilegeDef::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
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
    pub fn format() -> Vec<tarantool::space::Field> {
        Tier::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
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
    pub fn format() -> Vec<tarantool::space::Field> {
        RoutineDef::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
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

    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        PluginDef::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
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

    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        ServiceRouteItem::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
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

impl PluginMigration {
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

    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        PluginMigrationRecord::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
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

impl DbConfig {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index = space
            .index_builder("_pico_db_config_key")
            .unique(true)
            .part("key")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, index })
    }

    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        use tarantool::space::Field;
        vec![
            Field::from(("key", FieldType::String)),
            Field::from(("value", FieldType::Any)),
        ]
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            // Primary index
            id: 0,
            name: "_pico_db_config_key".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![Part::from(("key", IndexFieldType::String)).is_nullable(false)],
            operable: true,
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
        }]
    }

    #[inline]
    pub fn get_or_default<T>(&self, key: &'static str) -> tarantool::Result<T>
    where
        T: DecodeOwned,
        T: serde::de::DeserializeOwned,
    {
        if let Some(t) = self.space.get(&[key])? {
            if let Some(res) = t.field(1)? {
                return Ok(res);
            }
        }

        let value = config::get_default_value_of_alter_system_parameter(key)
            .expect("parameter name is validated using system_parameter_name! macro");
        let res = rmpv::ext::from_value(value).expect("default value type is correct");
        Ok(res)
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
    pub fn auth_password_length_min(&self) -> tarantool::Result<usize> {
        self.get_or_default(system_parameter_name!(auth_password_length_min))
    }

    #[inline]
    pub fn auth_password_enforce_uppercase(&self) -> tarantool::Result<bool> {
        self.get_or_default(system_parameter_name!(auth_password_enforce_uppercase))
    }

    #[inline]
    pub fn auth_password_enforce_lowercase(&self) -> tarantool::Result<bool> {
        self.get_or_default(system_parameter_name!(auth_password_enforce_lowercase))
    }

    #[inline]
    pub fn auth_password_enforce_digits(&self) -> tarantool::Result<bool> {
        self.get_or_default(system_parameter_name!(auth_password_enforce_digits))
    }

    #[inline]
    pub fn auth_password_enforce_specialchars(&self) -> tarantool::Result<bool> {
        self.get_or_default(system_parameter_name!(auth_password_enforce_specialchars))
    }

    #[inline]
    pub fn auth_login_attempt_max(&self) -> tarantool::Result<usize> {
        self.get_or_default(system_parameter_name!(auth_login_attempt_max))
    }

    #[inline]
    pub fn pg_statement_max(&self) -> tarantool::Result<usize> {
        let cached = config::MAX_PG_STATEMENTS.load(Ordering::Relaxed);
        if cached != 0 {
            return Ok(cached);
        }

        let res = self.get_or_default(system_parameter_name!(pg_statement_max))?;

        // Cache the value.
        config::MAX_PG_STATEMENTS.store(res, Ordering::Relaxed);
        Ok(res)
    }

    #[inline]
    pub fn pg_portal_max(&self) -> tarantool::Result<usize> {
        let cached = config::MAX_PG_PORTALS.load(Ordering::Relaxed);
        if cached != 0 {
            return Ok(cached);
        }

        let res = self.get_or_default(system_parameter_name!(pg_portal_max))?;

        // Cache the value.
        config::MAX_PG_PORTALS.store(res, Ordering::Relaxed);
        Ok(res)
    }

    #[inline]
    pub fn raft_snapshot_chunk_size_max(&self) -> tarantool::Result<usize> {
        self.get_or_default(system_parameter_name!(raft_snapshot_chunk_size_max))
    }

    #[inline]
    pub fn raft_snapshot_read_view_close_timeout(&self) -> tarantool::Result<Duration> {
        #[rustfmt::skip]
        let res: f64 = self.get_or_default(system_parameter_name!(raft_snapshot_read_view_close_timeout))?;
        Ok(Duration::from_secs_f64(res))
    }

    #[inline]
    pub fn governor_auto_offline_timeout(&self) -> tarantool::Result<Duration> {
        #[rustfmt::skip]
        let res: f64 = self.get_or_default(system_parameter_name!(governor_auto_offline_timeout))?;
        Ok(Duration::from_secs_f64(res))
    }

    #[inline]
    pub fn sql_motion_row_max(&self) -> tarantool::Result<u64> {
        self.get_or_default(system_parameter_name!(sql_motion_row_max))
    }

    #[inline]
    pub fn sql_vdbe_opcode_max(&self) -> tarantool::Result<u64> {
        self.get_or_default(system_parameter_name!(sql_vdbe_opcode_max))
    }

    #[inline]
    pub fn memtx_checkpoint_count(&self) -> tarantool::Result<u64> {
        self.get_or_default(system_parameter_name!(memtx_checkpoint_count))
    }

    #[inline]
    pub fn memtx_checkpoint_interval(&self) -> tarantool::Result<u64> {
        self.get_or_default(system_parameter_name!(memtx_checkpoint_interval))
    }

    #[inline]
    pub fn iproto_net_msg_max(&self) -> tarantool::Result<u64> {
        self.get_or_default(system_parameter_name!(iproto_net_msg_max))
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

        let storage = Clusterwide::for_tests();
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
            storage.space_by_name(ClusterwideTable::Instance).unwrap().put(&instance).unwrap();
            let (_, _, raft_id, ..) = instance;
            space_peer_addresses.put(&(raft_id, format!("addr:{raft_id}"))).unwrap();
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
            storage_peer_addresses.put(10, &traft::Address::from("addr:collision")).unwrap();
            storage_peer_addresses.put(11, &traft::Address::from("addr:collision")).unwrap();
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
                .map(|instance| storage_peer_addresses.try_get(instance.raft_id).unwrap())
                .collect::<Vec<_>>()
        };

        {
            assert_eq!(box_replication("XX"), Vec::<&str>::new());
            assert_eq!(box_replication("r1"), ["addr:1", "addr:2"]);
            assert_eq!(box_replication("r2"), ["addr:3", "addr:4"]);
            assert_eq!(box_replication("r3"), ["addr:5"]);
        }

        let space = storage.space_by_name(ClusterwideTable::Instance).unwrap();
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
        let storage = Clusterwide::for_tests();

        storage
            .space_by_name(ClusterwideTable::Address)
            .unwrap()
            .insert(&(1, "foo"))
            .unwrap();
        storage
            .space_by_name(ClusterwideTable::Address)
            .unwrap()
            .insert(&(2, "bar"))
            .unwrap();

        assert_eq!(storage.peer_addresses.get(1).unwrap().unwrap(), "foo");
        assert_eq!(storage.peer_addresses.get(2).unwrap().unwrap(), "bar");

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

    #[::tarantool::test]
    fn system_clusterwide_spaces_are_ordered_by_id() {
        let all_tables: Vec<SpaceId> = ClusterwideTable::all_tables()
            .iter()
            .map(|s| s.id())
            .collect();

        let mut sorted = all_tables.clone();
        sorted.sort_unstable();
        assert_eq!(all_tables, sorted);
    }

    #[track_caller]
    fn get_field_index(part: &Part, space_fields: &[tarantool::space::Field]) -> usize {
        match &part.field {
            NumOrStr::Num(index) => {
                return *index as _;
            }
            NumOrStr::Str(name) => {
                let found = space_fields
                    .iter()
                    .zip(0..)
                    .find(|(field, _)| &field.name == name);

                let Some((_, index)) = found else {
                    panic!(
                        "index part `{name}` doesn't correspond to any field in {space_fields:?}"
                    );
                };

                return index;
            }
        }
    }

    #[track_caller]
    fn check_index_parts_match(
        index_def: &IndexDef,
        picodata_index_parts: &[Part],
        tarantool_index_parts: &[Part],
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
            let tt_field_index = get_field_index(tt_part, space_fields);
            assert_eq!(pd_field_index, tt_field_index, "{index_def:?}");
        }
    }

    #[track_caller]
    fn check_macro_matches_index_definitions(sys_table: &ClusterwideTable, index_def: &IndexDef) {
        let from_macro = sys_table.index_names()[index_def.id as usize];
        let from_function = &index_def.name;
        assert_eq!(
            from_macro,
            from_function,
            r#"{struct}::INDEX_NAMES doesn't match {struct}::index_definitions()

    You must make sure that this part in the macro:
    | define_clusterwide_tables! {{
    | ...
    |    {sys_table:?} = {table_id}, "{table_name}" => {{
    | ...
    |        <index_name>: Index => "{from_macro}"
    |                                {from_macro_underline}
    Matches with this part in {struct}::index_definitions():
    | impl {struct} {{
    | ...
    |     pub fn index_definitions() -> Vec<IndexDef> {{
    | ...
    |         name: "{from_function}".into(),
    |                {from_function_unreline}
    don't ask why
"#,
            struct = sys_table.struct_name(),
            table_id = sys_table.id(),
            table_name = sys_table.name(),
            from_function_unreline = Highlight(&"^".repeat(from_function.len())),
            from_macro_underline = Highlight(&"^".repeat(from_macro.len())),
        );

        struct Highlight<'a>(&'a str);
        impl std::fmt::Display for Highlight<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("\x1b[1;31m")?;
                f.write_str(self.0)?;
                f.write_str("\x1b[0m")
            }
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
        let storage = Clusterwide::for_tests();

        let sys_space = SystemSpace::Space.as_space();
        let sys_index = SystemSpace::Index.as_space();

        for sys_table in ClusterwideTable::all_tables() {
            //
            // box.space._space
            //

            // Check space metadata is in tarantool's "_space"
            let tuple = sys_space.get(&[sys_table.id()]).unwrap().unwrap();
            let tt_space_def: SpaceMetadata = tuple.decode().unwrap();

            // This check is a bit redundant, but better safe than sorry
            assert_eq!(tt_space_def.id, sys_table.id());
            assert_eq!(tt_space_def.name, sys_table.name());

            // Check "_space" agrees with `ClusterwideTable.format`
            assert_eq!(tt_space_def.format, fields_to_format(&sys_table.format()));

            //
            // box.space._pico_table
            //

            // Check table definition is in picodata's "_pico_table"
            let pico_table_def = storage.tables.get(sys_table.id()).unwrap().unwrap();

            // Check picodata & tarantool agree on the definition
            assert_eq!(pico_table_def.to_space_metadata().unwrap(), tt_space_def);

            let index_definitions = sys_table.index_definitions();
            assert_eq!(
                index_definitions.len(),
                get_number_of_indexes_defined_for_space(sys_table.id()),
                "Mismatched number of indexes defined for table '{}'",
                sys_table.name(),
            );
            assert_eq!(
                index_definitions.len(),
                sys_table.index_names().len(),
                "Mismatched number of indexes defined for table '{}'",
                sys_table.name(),
            );
            for mut index_def in index_definitions {
                assert_eq!(index_def.table_id, sys_table.id());
                check_macro_matches_index_definitions(sys_table, &index_def);

                //
                // box.space._pico_index
                //

                // Check index definition is in picodata's "_pico_index"
                #[rustfmt::skip]
                let pico_index_def = storage.indexes.get(index_def.table_id, index_def.id).unwrap().unwrap();

                // Check "_pico_index" agrees with `ClusterwideTable.index_definitions`
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
                    &sys_table.format(),
                );

                // Check "_index" agrees with `ClusterwideTable.index_definitions`
                let pico_index_def = index_def.to_index_metadata(&pico_table_def);
                assert_eq!(pico_index_def.space_id, tt_index_def.space_id);
                assert_eq!(pico_index_def.index_id, tt_index_def.index_id);
                assert_eq!(pico_index_def.r#type, tt_index_def.r#type);
                assert_eq!(pico_index_def.opts, tt_index_def.opts);
                assert_eq!(pico_index_def.parts, tt_index_def.parts);
            }
        }
    }
}
