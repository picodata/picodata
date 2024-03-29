use tarantool::auth::AuthDef;
use tarantool::error::{Error as TntError, TarantoolErrorCode as TntErrorCode};
use tarantool::fiber;
use tarantool::index::Part;
use tarantool::index::{Index, IndexId, IndexIterator, IteratorType};
use tarantool::msgpack::{ArrayWriter, ValueIter};
use tarantool::read_view::ReadView;
use tarantool::read_view::ReadViewIterator;
use tarantool::session::UserId;
use tarantool::space::UpdateOps;
use tarantool::space::{FieldType, Space, SpaceId, SpaceType, SystemSpace};
use tarantool::time::Instant;
use tarantool::tlua::{self, LuaError};
use tarantool::tuple::KeyDef;
use tarantool::tuple::{Decode, DecodeOwned, Encode};
use tarantool::tuple::{RawBytes, ToTupleBuffer, Tuple, TupleBuffer};

use crate::access_control::{user_by_id, UserMetadataKind};
use crate::failure_domain::FailureDomain;
use crate::instance::{self, Instance};
use crate::replicaset::Replicaset;
use crate::schema::INITIAL_SCHEMA_VERSION;
use crate::schema::{Distribution, PrivilegeType, SchemaObjectType};
use crate::schema::{IndexDef, TableDef};
use crate::schema::{PrivilegeDef, RoutineDef, UserDef};
use crate::schema::{ADMIN_ID, PUBLIC_ID, UNIVERSE_ID};
use crate::sql::pgproto::{DEFAULT_MAX_PG_PORTALS, DEFAULT_MAX_PG_STATEMENTS};
use crate::tier::Tier;
use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::op::Ddl;
use crate::traft::op::Dml;
use crate::traft::RaftEntryId;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::traft::Result;
use crate::util::Uppercase;
use crate::vshard::VshardConfig;
use crate::warn_or_panic;

use std::borrow::Cow;
use std::cell::{RefCell, UnsafeCell};
use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};
use std::iter::Peekable;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

use self::acl::{on_master_drop_role, on_master_drop_user};

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
                $(#[$Clusterwide_extra_field_meta:meta])*
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
            }
        )+
    }
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
            pub struct Tables {
                space: Space,
                #[primary]
                index_id: Index => "id",
                index_name: Index => "name",
            }
        }
        Index = 513, "_pico_index" => {
            Clusterwide::indexes;

            /// A struct for accessing definitions of all the user-defined indexes.
            pub struct Indexes {
                space: Space,
                #[primary]
                index_id: Index => "id",
                index_name: Index => "name",
            }
        }
        Address = 514, "_pico_peer_address" => {
            Clusterwide::peer_addresses;

            /// A struct for accessing storage of peer addresses.
            pub struct PeerAddresses {
                space: Space,
                #[primary]
                index: Index => "raft_id",
            }
        }
        Instance = 515, "_pico_instance" => {
            Clusterwide::instances;

            /// A struct for accessing storage of all the cluster instances.
            pub struct Instances {
                space: Space,
                #[primary]
                index_instance_id:   Index => "instance_id",
                index_raft_id:       Index => "raft_id",
                index_replicaset_id: Index => "replicaset_id",
            }
        }
        Property = 516, "_pico_property" => {
            Clusterwide::properties;

            /// A struct for accessing storage of the cluster-wide key-value properties
            pub struct Properties {
                space: Space,
                #[primary]
                index: Index => "key",
            }
        }
        Replicaset = 517, "_pico_replicaset" => {
            Clusterwide::replicasets;

            /// A struct for accessing replicaset info from storage
            pub struct Replicasets {
                space: Space,
                #[primary]
                index: Index => "replicaset_id",
            }
        }

        User = 520, "_pico_user" => {
            Clusterwide::users;

            /// A struct for accessing info of all the user-defined users.
            pub struct Users {
                space: Space,
                #[primary]
                index_id: Index => "id",
                index_name: Index => "name",
            }
        }
        Privilege = 521, "_pico_privilege" => {
            Clusterwide::privileges;

            /// A struct for accessing info of all privileges granted to
            /// user-defined users.
            pub struct Privileges {
                space: Space,
                #[primary]
                primary_key: Index => "primary",
                object_idx: Index => "object",
            }
        }
        Tier = 523, "_pico_tier" => {
            Clusterwide::tiers;

            /// A struct for accessing info of all tiers in cluster.
            pub struct Tiers {
                space: Space,
                #[primary]
                index_name: Index => "name",
            }
        }
        Routine = 524, "_pico_routine" => {
            Clusterwide::routines;

            /// A struct for accessing info of all the user-defined routines.
            pub struct Routines {
                space: Space,
                #[primary]
                index_id: Index => "id",
                index_name: Index => "name",
            }
        }
    }
}

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

    fn open_read_view(&self, entry_id: RaftEntryId) -> Result<SnapshotReadView> {
        let mut space_indexes = Vec::with_capacity(32);

        for space_def in self.tables.iter()? {
            if !matches!(space_def.distribution, Distribution::Global) {
                continue;
            }
            space_indexes.push((space_def.id, 0));
        }

        let schema_version = self.properties.global_schema_version()?;

        let read_view = ReadView::for_space_indexes(space_indexes)?;
        Ok(SnapshotReadView {
            entry_id,
            schema_version,
            time_opened: fiber::clock(),
            unfinished_iterators: Default::default(),
            read_view,
            ref_count: 0,
        })
    }

    /// Generate the next snapshot chunk starting a `position`.
    ///
    /// Will cache the iterator into `rv` so that chunk generation can resume
    /// later from the same position.
    pub fn next_snapshot_data_chunk_impl(
        &self,
        rv: &mut SnapshotReadView,
        mut position: SnapshotPosition,
    ) -> Result<SnapshotData> {
        let global_spaces = rv.read_view.space_indexes().unwrap();
        let mut space_dumps = Vec::with_capacity(global_spaces.len());
        let mut total_size = 0;
        let mut hit_threashold = false;
        let snapshot_chunk_max_size = self.properties.snapshot_chunk_max_size()?;
        let t0 = std::time::Instant::now();
        for &(space_id, index_id) in global_spaces {
            debug_assert_eq!(index_id, 0, "only primary keys should be added");

            // NOTE: we do linear search here, but it shouldn't matter, unless
            // somebody decides to have a lot of global spaces. But in that case
            // I doubt this is going to be the slowest part of picodata...
            if space_id < position.space_id {
                continue;
            }

            let mut iter: Option<Peekable<ReadViewIterator<'static>>> = None;
            if space_id == position.space_id {
                // Get a cached iterator
                if let Entry::Occupied(mut kv) = rv.unfinished_iterators.entry(position) {
                    let t = kv.get_mut();
                    iter = t.pop();
                    if t.is_empty() {
                        kv.remove_entry();
                    }
                }
                if iter.is_none() {
                    let entry_id = rv.entry_id;
                    warn_or_panic!("missing iterator for a non-zero snapshot position ({entry_id}, {position})");
                    return Err(Error::other(format!(
                        "missing iterator for snapshot position ({entry_id}, {position})"
                    )));
                }
            } else {
                // Get a new iterator from the read view
                let it = rv.read_view.iter_all(space_id, 0)?.map(Iterator::peekable);
                if it.is_none() {
                    warn_or_panic!("read view didn't contain space #{space_id}");
                    continue;
                }
                // SAFETY: It is only required that `iter` is never used (other
                // than `drop`) once the corresponding `read_view` is dropped.
                // This assumption holds as long as iterators are always stored
                // next to the corresponding read views (which they are) and
                // that iterators are dropped at the same time as read views
                // (which they are). So it's safe.
                unsafe { iter = std::mem::transmute(it) };
            }
            let mut iter = iter.expect("just made sure it's Some");

            let mut array_writer = ArrayWriter::from_vec(Vec::with_capacity(32 * 1024));
            while let Some(tuple_data) = iter.peek() {
                if total_size + tuple_data.len() > snapshot_chunk_max_size && total_size != 0 {
                    hit_threashold = true;
                    break;
                }
                array_writer.push_raw(tuple_data)?;
                total_size += tuple_data.len();
                position.tuple_offset += 1;

                // Actually advance the iterator.
                _ = iter.next();
            }
            let count = array_writer.len();
            let tuples = array_writer.finish()?.into_inner();
            // SAFETY: ArrayWriter guarantees the result is a valid msgpack array
            let tuples = unsafe { TupleBuffer::from_vec_unchecked(tuples) };

            let space_name = self
                .global_table_name(space_id)
                .expect("this id is from _pico_table");
            tlog!(
                Info,
                "generated snapshot chunk for clusterwide table '{space_name}': {} tuples [{} bytes]",
                count,
                tuples.len(),
            );
            space_dumps.push(SpaceDump { space_id, tuples });

            position.space_id = space_id;
            if hit_threashold {
                #[rustfmt::skip]
                match rv.unfinished_iterators.entry(position) {
                    Entry::Occupied(mut slot) => { slot.get_mut().push(iter); }
                    Entry::Vacant(slot)       => { slot.insert(vec![iter]); }
                };
                break;
            } else {
                // This space is finished, reset the offset.
                position.tuple_offset = 0;
            }
        }

        let is_final = !hit_threashold;
        tlog!(Debug, "generating snapshot chunk took {:?}", t0.elapsed();
            "is_final" => is_final,
        );

        let snapshot_data = SnapshotData {
            schema_version: rv.schema_version,
            space_dumps,
            next_chunk_position: hit_threashold.then_some(position),
        };
        Ok(snapshot_data)
    }

    pub fn next_snapshot_data_chunk(
        &self,
        entry_id: RaftEntryId,
        position: SnapshotPosition,
    ) -> Result<SnapshotData> {
        // We keep a &mut on the read views container here,
        // so we mustn't yield for the duration of that borrow.
        #[cfg(debug_assertions)]
        let _guard = crate::util::NoYieldsGuard::new();

        let Some(rv) = self.snapshot_cache.read_views().get_mut(&entry_id) else {
            warn_or_panic!("read view for entry {entry_id} is not available");
            return Err(Error::other(format!(
                "read view not available for {entry_id}"
            )));
        };

        let snapshot_data = self.next_snapshot_data_chunk_impl(rv, position)?;
        if snapshot_data.next_chunk_position.is_none() {
            // This customer has been served.
            rv.ref_count -= 1;
        }
        Ok(snapshot_data)
    }

    pub fn first_snapshot_data_chunk(
        &self,
        entry_id: RaftEntryId,
        compacted_index: RaftIndex,
    ) -> Result<(SnapshotData, RaftEntryId)> {
        // We keep a &mut on the read views container here,
        // so we mustn't yield for the duration of that borrow.
        #[cfg(debug_assertions)]
        let _guard = crate::util::NoYieldsGuard::new();

        if entry_id.index >= compacted_index {
            // Reuse the read view, if it's already open for the requested index.
            // TODO: actually we can reuse even older read_views, as long as
            // they have the same conf_state, although it doesn't seem likely...
            let read_view = self.snapshot_cache.read_views().get_mut(&entry_id);
            if let Some(rv) = read_view {
                tlog!(Info, "reusing a snapshot read view for {entry_id}");
                let position = SnapshotPosition::default();
                let snapshot_data = self.next_snapshot_data_chunk_impl(rv, position)?;
                // A new instance has started applying this snapshot,
                // so we must keep it open until it finishes.
                rv.ref_count += 1;
                return Ok((snapshot_data, rv.entry_id));
            }
        }

        let mut rv = self.open_read_view(entry_id)?;
        tlog!(Info, "opened snapshot read view for {entry_id}");
        let position = SnapshotPosition::default();
        let snapshot_data = self.next_snapshot_data_chunk_impl(&mut rv, position)?;

        if snapshot_data.next_chunk_position.is_none() {
            // Don't increase the reference count, so that it's cleaned up next
            // time a new snapshot is generated.
        } else {
            rv.ref_count = 1;
        }

        let timeout = self.properties.snapshot_read_view_close_timeout()?;
        let now = fiber::clock();
        // Clear old snapshots.
        // This is where we clear any stale snapshots with no references
        // and snapshots with (probably) dangling references
        // which outlived the allotted time.
        #[rustfmt::skip]
        self.snapshot_cache.read_views().retain(|entry_id, rv| {
            if rv.ref_count == 0 {
                tlog!(Info, "closing snapshot read view for {entry_id:?}, reason: no references");
                return false;
            }
            if now.duration_since(rv.time_opened) > timeout {
                tlog!(Info, "closing snapshot read view for {entry_id:?}, reason: timeout");
                return false;
            }
            true
        });

        // Save this read view for later
        self.snapshot_cache.read_views().insert(rv.entry_id, rv);

        Ok((snapshot_data, entry_id))
    }

    fn global_table_name(&self, id: SpaceId) -> Result<Cow<'static, str>> {
        let Some(space_def) = self.tables.get(id)? else {
            return Err(Error::other(format!("global space #{id} not found")));
        };
        Ok(space_def.name.into())
    }

    /// Applies contents of the raft snapshot. This includes
    /// * Contents of internal clusterwide spaces.
    /// * If `is_master` is `true`, definitions of user-defined global spaces.
    /// * If `is_master` is `true`, contents of user-defined global spaces.
    ///
    /// This function is called from [`NodeImpl::advance`] when a snapshot was
    /// received from the current raft leader and it was determined that the
    /// snapshot should be applied.
    ///
    /// This should be called within a transaction.
    ///
    #[allow(rustdoc::private_intra_doc_links)]
    /// [`NodeImpl::advance`]: crate::traft::node::NodeImpl::advance
    pub fn apply_snapshot_data(&self, data: &SnapshotData, is_master: bool) -> Result<()> {
        debug_assert!(unsafe { ::tarantool::ffi::tarantool::box_txn() });

        // These need to be saved before we truncate the corresponding spaces.
        let mut old_table_versions = HashMap::new();
        let mut old_user_versions = HashMap::new();
        let mut old_priv_versions = HashMap::new();

        for def in self.tables.iter()? {
            old_table_versions.insert(def.id, def.schema_version);
        }
        for def in self.users.iter()? {
            old_user_versions.insert(def.id, def.schema_version);
        }
        for def in self.privileges.iter()? {
            let schema_version = def.schema_version();
            old_priv_versions.insert(def, schema_version);
        }

        // There can be mutliple space dumps for a given space in SnapshotData,
        // because snapshots arrive in chunks. So when restoring space dumps we
        // shouldn't truncate spaces which previously already restored chunk of
        // space dump. The simplest way to solve this problem I found is to just
        // store the ids of spaces we trunacted in a HashSet.
        let mut truncated_spaces = HashSet::new();

        let mut tuples_count = 0;
        let t0 = std::time::Instant::now();
        for space_dump in &data.space_dumps {
            let space_id = space_dump.space_id;
            if !SCHEMA_DEFINITION_SPACES.contains(&space_id) {
                // First we restore contents of global schema spaces
                // (_pico_table, _pico_user, etc.), so that we can apply the
                // schema changes to them.
                continue;
            }
            let space = self
                .space_by_id(space_id)
                .expect("global schema definition spaces should always be present");

            if !truncated_spaces.contains(&space_id) {
                space.truncate()?;
                truncated_spaces.insert(space_id);
            }

            let tuples = space_dump.tuples.as_ref();
            let tuples = ValueIter::from_array(tuples).map_err(TntError::from)?;
            tuples_count += tuples.len().expect("ValueIter::from_array sets the length");

            for tuple in tuples {
                space.insert(RawBytes::new(tuple))?;
            }
        }
        tlog!(
            Debug,
            "restoring global schema defintions took {:?} [{} tuples]",
            t0.elapsed(),
            tuples_count
        );

        // If we're not the replication master, the rest of the data will come
        // via tarantool replication.
        //
        // v_local == v_snapshot could happen for example in case of an
        // unfortunately timed replication master failover, in which case the
        // schema portion of the snapshot was already applied and we should skip
        // it here.
        let v_local = local_schema_version()?;
        let v_snapshot = data.schema_version;
        if is_master && v_local < v_snapshot {
            let t0 = std::time::Instant::now();

            self.apply_schema_changes_on_master(self.tables.iter()?, &old_table_versions)?;
            // TODO: secondary indexes
            self.apply_schema_changes_on_master(self.users.iter()?, &old_user_versions)?;
            self.apply_schema_changes_on_master(self.privileges.iter()?, &old_priv_versions)?;
            set_local_schema_version(v_snapshot)?;

            tlog!(
                Debug,
                "applying global schema changes took {:?}",
                t0.elapsed()
            );
        }

        let mut tuples_count = 0;
        let t0 = std::time::Instant::now();
        for space_dump in &data.space_dumps {
            let space_id = space_dump.space_id;
            if SCHEMA_DEFINITION_SPACES.contains(&space_id) {
                // Already handled in the loop above.
                continue;
            }
            let Ok(space) = self.space_by_id(space_id) else {
                warn_or_panic!(
                    "a dump for a non existent space #{} arrived via snapshot",
                    space_id
                );
                continue;
            };

            if !truncated_spaces.contains(&space_id) {
                space.truncate()?;
                truncated_spaces.insert(space_id);
            }

            let tuples = space_dump.tuples.as_ref();
            let tuples = ValueIter::from_array(tuples).map_err(TntError::from)?;
            tuples_count += tuples.len().expect("ValueIter::from_array sets the length");

            for tuple in tuples {
                space.insert(RawBytes::new(tuple))?;
            }
            // Debug:   restoring rest of snapshot data took 5.9393054s [4194323 tuples]
            // Release: restoring rest of snapshot data took 3.748683s [4194323 tuples]

            // This works better in debug builds, but requires exporting mp_next
            // or (better) porting it to rust. rmp::decode is based on the Read
            // trait, which makes it really painfull to work with and also suck
            // at debug build performance...
            //
            // let mut data = space_dump.tuples.as_ref();
            // let end = data.as_ptr_range().end;
            // let len = rmp::decode::read_array_len(&mut data).map_err(TntError::from)?;
            // tuples_count += len;
            // let mut p = data.as_ptr();
            // while p < end {
            //     let s = p;
            //     unsafe {
            //         box_mp_next(&mut p);
            //         let rc = tarantool::ffi::tarantool::box_insert(space_id, s as _, p as _, std::ptr::null_mut());
            //         if rc != 0 { return Err(tarantool::error::TarantoolError::last().into()); }
            //     }
            // }
            // extern "C" {
            //     fn box_mp_next(data: *mut *const u8); // just calls mp_next from msgpuck
            // }
            // Debug:   restoring rest of snapshot data took 3.5061713s [4194323 tuples]
            // Release: restoring rest of snapshot data took 3.5179512s [4194323 tuples]
        }
        tlog!(
            Debug,
            "restoring rest of snapshot data took {:?} [{} tuples]",
            t0.elapsed(),
            tuples_count
        );

        return Ok(());

        const SCHEMA_DEFINITION_SPACES: &[SpaceId] = &[
            ClusterwideTable::Table.id(),
            ClusterwideTable::Index.id(),
            ClusterwideTable::User.id(),
            ClusterwideTable::Privilege.id(),
        ];
    }

    /// Updates local storage of the given schema entity in accordance with
    /// contents of the global storage. This function is called during snapshot
    /// application and is responsible for performing schema change operations
    /// (acl or ddl) for a given entity type (space, user, etc.) based on the
    /// contents of the snapshot and the global storage at the moment snapshot
    /// is received.
    ///
    /// `iter` is an iterator of records representing given schema entities
    /// ([`TableDef`], [`UserDef`]).
    ///
    /// `old_versions` provides information of entity versions for determining
    /// which entities should be dropped and/or recreated.
    fn apply_schema_changes_on_master<T>(
        &self,
        iter: impl Iterator<Item = T>,
        old_versions: &HashMap<T::Key, u64>,
    ) -> traft::Result<()>
    where
        T: SchemaDef,
    {
        debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });

        let mut new_defs = Vec::new();
        let mut new_keys = HashSet::new();
        for def in iter {
            new_keys.insert(def.key());
            new_defs.push(def);
        }

        // First we drop all schema entities which have been dropped.
        for old_key in old_versions.keys() {
            if new_keys.contains(old_key) {
                // Will be handled later.
                continue;
            }

            // Schema definition was dropped.
            if let Err(e) = T::on_delete(old_key, self) {
                tlog!(
                    Error,
                    "failed handling delete of {} with key {old_key:?}: {e}",
                    std::any::type_name::<T>(),
                );
                return Err(e);
            }
        }

        // Now create any new schema entities, or replace ones that changed.
        for def in &new_defs {
            let key = def.key();
            let v_new = def.schema_version();

            if v_new == INITIAL_SCHEMA_VERSION {
                // Schema entity is already up to date (created on boot).
                continue;
            }

            if let Some(&v_old) = old_versions.get(&key) {
                assert!(v_old <= v_new);

                if v_old == v_new {
                    // Schema entity is up to date.
                    continue;
                }

                // Schema entity changed, need to drop it an recreate.
                if let Err(e) = T::on_delete(&key, self) {
                    tlog!(
                        Error,
                        "failed handling delete of {} with key {key:?}: {e}",
                        std::any::type_name::<T>(),
                    );
                    return Err(e);
                }
            } else {
                // New schema entity.
            }

            if !def.is_operable() {
                // If it so happens, that we receive an unfinished schema change via snapshot,
                // which will somehow get finished without the governor sending us
                // a proc_apply_schema_change rpc, it will be a very sad day.
                continue;
            }

            if let Err(e) = T::on_insert(def, self) {
                tlog!(
                    Error,
                    "failed handling insert of {} with key {key:?}: {e}",
                    std::any::type_name::<T>(),
                );
                return Err(e);
            }
        }

        Ok(())
    }

    /// Return a `KeyDef` to be used for comparing **tuples** of the
    /// corresponding global space.
    pub(crate) fn key_def(
        &self,
        space_id: SpaceId,
        index_id: IndexId,
    ) -> tarantool::Result<Rc<KeyDef>> {
        static mut KEY_DEF: Option<HashMap<(ClusterwideTable, IndexId), Rc<KeyDef>>> = None;
        let key_defs = unsafe { KEY_DEF.get_or_insert_with(HashMap::new) };
        if let Ok(sys_space_id) = ClusterwideTable::try_from(space_id) {
            let key_def = match key_defs.entry((sys_space_id, index_id)) {
                Entry::Occupied(o) => o.into_mut(),
                Entry::Vacant(v) => {
                    let index = unsafe { Index::from_ids_unchecked(space_id, index_id) };
                    let key_def = index.meta()?.to_key_def();
                    // System space definition's never change during a single
                    // execution, so it's safe to cache these
                    v.insert(Rc::new(key_def))
                }
            };
            return Ok(key_def.clone());
        }

        let index = unsafe { Index::from_ids_unchecked(space_id, index_id) };
        let key_def = index.meta()?.to_key_def();
        Ok(Rc::new(key_def))
    }

    /// Return a `KeyDef` to be used for comparing **keys** of the
    /// corresponding global space.
    pub(crate) fn key_def_for_key(
        &self,
        space_id: SpaceId,
        index_id: IndexId,
    ) -> tarantool::Result<Rc<KeyDef>> {
        static mut KEY_DEF: Option<HashMap<(ClusterwideTable, IndexId), Rc<KeyDef>>> = None;
        let key_defs = unsafe { KEY_DEF.get_or_insert_with(HashMap::new) };
        if let Ok(sys_space_id) = ClusterwideTable::try_from(space_id) {
            let key_def = match key_defs.entry((sys_space_id, index_id)) {
                Entry::Occupied(o) => o.into_mut(),
                Entry::Vacant(v) => {
                    let index = unsafe { Index::from_ids_unchecked(space_id, index_id) };
                    let key_def = index.meta()?.to_key_def_for_key();
                    // System space definition's never change during a single
                    // execution, so it's safe to cache these
                    v.insert(Rc::new(key_def))
                }
            };
            return Ok(key_def.clone());
        }

        let index = unsafe { Index::from_ids_unchecked(space_id, index_id) };
        let key_def = index.meta()?.to_key_def_for_key();
        Ok(Rc::new(key_def))
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

////////////////////////////////////////////////////////////////////////////////
// ClusterwideTable
////////////////////////////////////////////////////////////////////////////////

impl ClusterwideTable {
    /// Id of the corrseponding system global space.
    #[inline(always)]
    pub const fn id(&self) -> SpaceId {
        *self as _
    }

    /// Name of the corrseponding system global space.
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
        VshardBootstrapped = "vshard_bootstrapped",

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

        PasswordMinLength = "password_min_length",

        /// Password should contain at least one uppercase letter
        PasswordEnforceUppercase = "password_enforce_uppercase",

        /// Password should contain at least one lowercase letter
        PasswordEnforceLowercase = "password_enforce_lowercase",

        /// Password should contain at least one digit
        PasswordEnforceDigits = "password_enforce_digits",

        /// Password should contain at least one special symbol.
        /// Special symbols - &, |, ?, !, $, @
        PasswordEnforceSpecialchars = "password_enforce_specialchars",

        /// Maximum number of login attempts through `picodata connect`.
        /// Each failed login attempt increases a local per user counter of failed attempts.
        /// When the counter reaches the value of this property any subsequent logins
        /// of this user will be denied.
        /// Local counter for a user is reset on successful login.
        ///
        /// Default value is [`DEFAULT_MAX_LOGIN_ATTEMPTS`].
        MaxLoginAttempts = "max_login_attempts",

        /// Number of seconds to wait before automatically changing an
        /// unresponsive instance's grade to Offline.
        AutoOfflineTimeout = "auto_offline_timeout",

        /// Maximum number of seconds to wait before sending another heartbeat
        /// to an unresponsive instance.
        MaxHeartbeatPeriod = "max_heartbeat_period",

        /// PG statement storage size.
        MaxPgStatements = "max_pg_statements",

        /// PG portal storage size.
        MaxPgPortals = "max_pg_portals",

        /// Raft snapshot will be sent out in chunks not bigger than this threshold.
        /// Note: actual snapshot size may exceed this threshold. In most cases
        /// it will just add a couple of dozen metadata bytes. But in extreme
        /// cases if there's a tuple larger than this threshold, it will be sent
        /// in one piece whatever size it has. Please don't store tuples of size
        /// greater than this.
        SnapshotChunkMaxSize = "snapshot_chunk_max_size",

        /// Max size for batch cas operation in bytes. When using batch CAS,
        /// we put multiple dml operations in one raft entry (tuple).
        /// The user must guarantee that this value <= memtx_max_tuple_size
        /// for all nodes in cluster.
        BatchCasMaxSize = "raft_entry_max_size",

        /// Snapshot read views with live reference counts will be forcefully
        /// closed after this number of seconds. This is necessary if followers
        /// do not properly finalize the snapshot application.
        // NOTE: maybe we should instead track the instance/raft ids of
        // followers which requested the snapshots and automatically close the
        // read views if the corresponding instances are *deteremined* to not
        // need them anymore. Or maybe timeouts is the better way..
        SnapshotReadViewCloseTimeout = "snapshot_read_view_close_timeout",

        /// Vshard configuration which has most recently been applied on all the
        /// instances of the cluster.
        CurrentVshardConfig = "current_vshard_config",

        /// Vshard configuration which should be applied on all instances of the
        /// cluster. If this is equal to current_vshard_config, then the
        /// configuration is up to date.
        // TODO: don't store the complete target vshard config, instead store
        // the current global vshard config version and a current applied vshard
        // config version of each instance.
        TargetVshardConfig = "target_vshard_config",
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
            Self::VshardBootstrapped | Self::GlobalSchemaVersion | Self::NextSchemaVersion
        )
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
            Self::PasswordEnforceUppercase
            | Self::PasswordEnforceLowercase
            | Self::PasswordEnforceDigits
            | Self::PasswordEnforceSpecialchars
            | Self::VshardBootstrapped => {
                // Check it's a bool.
                _ = new.field::<bool>(1)?;
            }
            Self::NextSchemaVersion
            | Self::PendingSchemaVersion
            | Self::GlobalSchemaVersion
            | Self::PasswordMinLength
            | Self::MaxLoginAttempts
            | Self::MaxPgPortals
            | Self::MaxPgStatements
            | Self::BatchCasMaxSize
            | Self::SnapshotChunkMaxSize => {
                // Check it's an unsigned integer.
                _ = new.field::<u64>(1).map_err(map_err)?;
            }
            Self::AutoOfflineTimeout
            | Self::MaxHeartbeatPeriod
            | Self::SnapshotReadViewCloseTimeout => {
                // Check it's a floating point number.
                // NOTE: serde implicitly converts integers to floats for us here.
                _ = new.field::<f64>(1).map_err(map_err)?;
            }
            Self::CurrentVshardConfig | Self::TargetVshardConfig => {
                // Check it decodes into VshardConfig.
                _ = new.field::<VshardConfig>(1).map_err(map_err)?;
            }
            Self::PendingSchemaChange => {
                // Check it decodes into Ddl.
                _ = new.field::<Ddl>(1).map_err(map_err)?;
            }
        }

        Ok(())
    }

    /// Try decoding property's value specifically for audit log.
    /// Returns `Some(string)` if it should be logged, `None` otherwise.
    pub fn should_be_audited(&self, tuple: &Tuple) -> Result<Option<String>> {
        let bad_value = || Error::other("bad value");
        Ok(match self {
            Self::PasswordMinLength
            | Self::MaxLoginAttempts
            | Self::SnapshotChunkMaxSize
            | Self::MaxPgStatements
            | Self::MaxPgPortals => {
                let v = tuple.field::<usize>(1)?.ok_or_else(bad_value)?;
                Some(format!("{v}"))
            }
            _ => None,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
// Properties
////////////////////////////////////////////////////////////////////////////////

pub const DEFAULT_PASSWORD_MIN_LENGTH: usize = 8;
pub const DEFAULT_PASSWORD_ENFORCE_UPPERCASE: bool = true;
pub const DEFAULT_PASSWORD_ENFORCE_LOWERCASE: bool = true;
pub const DEFAULT_PASSWORD_ENFORCE_DIGITS: bool = true;
pub const DEFAULT_PASSWORD_ENFORCE_SPECIALCHARS: bool = false;
pub const DEFAULT_AUTO_OFFLINE_TIMEOUT: f64 = 5.0;
pub const DEFAULT_MAX_HEARTBEAT_PERIOD: f64 = 5.0;
pub const DEFAULT_SNAPSHOT_CHUNK_MAX_SIZE: usize = 16 * 1024 * 1024;
pub const DEFAULT_CAS_BATCH_MAX_SIZE: u64 = 1_000_000;
pub const DEFAULT_SNAPSHOT_READ_VIEW_CLOSE_TIMEOUT: f64 = (24 * 3600) as _;
pub const DEFAULT_MAX_LOGIN_ATTEMPTS: usize = 4;

impl Properties {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index = space
            .index_builder("key")
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
            name: "key".into(),
            parts: vec![Part::from("key")],
            unique: true,
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
            operable: true,
            local: true,
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
                let Ok(Some(key)) = new.field::<PropertyName>(0) else {
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

                key.verify_new_tuple(&new)?;

                let field_count = new.len();
                if field_count != 2 {
                    return Err(Error::other(format!(
                        "too many fields: got {field_count}, expected 2"
                    )));
                }
            }
            (None, None) => unreachable!(),
        }

        Ok(())
    }

    #[inline]
    pub fn get<T>(&self, key: PropertyName) -> tarantool::Result<Option<T>>
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

    #[inline]
    pub fn delete(&self, key: PropertyName) -> tarantool::Result<()> {
        self.space.delete(&[key])?;
        Ok(())
    }

    #[inline]
    pub fn target_vshard_config(&self) -> tarantool::Result<VshardConfig> {
        Ok(self
            .get(PropertyName::TargetVshardConfig)?
            .unwrap_or_default())
    }

    #[inline]
    pub fn current_vshard_config(&self) -> tarantool::Result<VshardConfig> {
        Ok(self
            .get(PropertyName::CurrentVshardConfig)?
            .unwrap_or_default())
    }

    #[inline]
    pub fn vshard_bootstrapped(&self) -> tarantool::Result<bool> {
        Ok(self
            .get(PropertyName::VshardBootstrapped)?
            .unwrap_or_default())
    }

    #[inline]
    pub fn password_min_length(&self) -> tarantool::Result<usize> {
        let res = self
            .get(PropertyName::PasswordMinLength)?
            .unwrap_or(DEFAULT_PASSWORD_MIN_LENGTH);
        Ok(res)
    }

    #[inline]
    pub fn password_enforce_uppercase(&self) -> tarantool::Result<bool> {
        let res = self
            .get(PropertyName::PasswordEnforceUppercase)?
            .unwrap_or(DEFAULT_PASSWORD_ENFORCE_UPPERCASE);
        Ok(res)
    }

    #[inline]
    pub fn password_enforce_lowercase(&self) -> tarantool::Result<bool> {
        let res = self
            .get(PropertyName::PasswordEnforceLowercase)?
            .unwrap_or(DEFAULT_PASSWORD_ENFORCE_LOWERCASE);
        Ok(res)
    }

    #[inline]
    pub fn password_enforce_digits(&self) -> tarantool::Result<bool> {
        let res = self
            .get(PropertyName::PasswordEnforceDigits)?
            .unwrap_or(DEFAULT_PASSWORD_ENFORCE_DIGITS);
        Ok(res)
    }

    #[inline]
    pub fn password_enforce_specialchars(&self) -> tarantool::Result<bool> {
        let res = self
            .get(PropertyName::PasswordEnforceSpecialchars)?
            .unwrap_or(DEFAULT_PASSWORD_ENFORCE_SPECIALCHARS);
        Ok(res)
    }

    /// See [`PropertyName::MaxLoginAttempts`]
    #[inline]
    pub fn max_login_attempts(&self) -> tarantool::Result<usize> {
        let res = self
            .get(PropertyName::MaxLoginAttempts)?
            .unwrap_or(DEFAULT_MAX_LOGIN_ATTEMPTS);
        Ok(res)
    }

    #[inline]
    pub fn pending_schema_change(&self) -> tarantool::Result<Option<Ddl>> {
        self.get(PropertyName::PendingSchemaChange)
    }

    #[inline]
    pub fn pending_schema_version(&self) -> tarantool::Result<Option<u64>> {
        self.get(PropertyName::PendingSchemaVersion)
    }

    #[inline]
    pub fn global_schema_version(&self) -> tarantool::Result<u64> {
        let res = self
            .get(PropertyName::GlobalSchemaVersion)?
            .unwrap_or(INITIAL_SCHEMA_VERSION);
        Ok(res)
    }

    #[inline]
    pub fn max_pg_statements(&self) -> tarantool::Result<usize> {
        let res = self
            .get(PropertyName::MaxPgStatements)?
            .unwrap_or(DEFAULT_MAX_PG_STATEMENTS);
        Ok(res)
    }

    #[inline]
    pub fn max_pg_portals(&self) -> tarantool::Result<usize> {
        let res = self
            .get(PropertyName::MaxPgPortals)?
            .unwrap_or(DEFAULT_MAX_PG_PORTALS);
        Ok(res)
    }

    #[inline]
    pub fn snapshot_chunk_max_size(&self) -> tarantool::Result<usize> {
        let res = self
            .get(PropertyName::SnapshotChunkMaxSize)?
            // Just in case user deletes this property.
            .unwrap_or(DEFAULT_SNAPSHOT_CHUNK_MAX_SIZE);
        Ok(res)
    }

    #[inline]
    pub fn cas_batch_max_size(&self) -> tarantool::Result<u64> {
        let res = self
            .get(PropertyName::BatchCasMaxSize)?
            // Just in case user deletes this property.
            .unwrap_or(DEFAULT_CAS_BATCH_MAX_SIZE);
        Ok(res)
    }

    #[inline]
    pub fn snapshot_read_view_close_timeout(&self) -> tarantool::Result<Duration> {
        let res = self
            .get(PropertyName::SnapshotReadViewCloseTimeout)?
            // Just in case user deletes this property.
            .unwrap_or(DEFAULT_SNAPSHOT_READ_VIEW_CLOSE_TIMEOUT);
        Ok(Duration::from_secs_f64(res))
    }

    #[inline]
    pub fn next_schema_version(&self) -> tarantool::Result<u64> {
        let res = if let Some(version) = self.get(PropertyName::NextSchemaVersion)? {
            version
        } else {
            let current = self.global_schema_version()?;
            current + 1
        };
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

        let index = space
            .index_builder("replicaset_id")
            .unique(true)
            .part("replicaset_id")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, index })
    }

    #[inline(always)]
    pub fn format() -> Vec<tarantool::space::Field> {
        Replicaset::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            // Primary index
            id: 0,
            name: "replicaset_id".into(),
            parts: vec![Part::from("replicaset_id")],
            unique: true,
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
            operable: true,
            local: true,
        }]
    }

    #[allow(unused)]
    #[inline]
    pub fn get(&self, replicaset_id: &str) -> tarantool::Result<Option<Replicaset>> {
        let tuple = self.space.get(&[replicaset_id])?;
        tuple.as_ref().map(Tuple::decode).transpose()
    }
}

impl ToEntryIter for Replicasets {
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
            .index_builder("raft_id")
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
            name: "raft_id".into(),
            parts: vec![Part::from("raft_id")],
            unique: true,
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
            operable: true,
            local: true,
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

impl ToEntryIter for PeerAddresses {
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

        let index_instance_id = space_instances
            .index_builder("instance_id")
            .unique(true)
            .part("instance_id")
            .if_not_exists(true)
            .create()?;

        let index_raft_id = space_instances
            .index_builder("raft_id")
            .unique(true)
            .part("raft_id")
            .if_not_exists(true)
            .create()?;

        let index_replicaset_id = space_instances
            .index_builder("replicaset_id")
            .unique(false)
            .part("replicaset_id")
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space: space_instances,
            index_instance_id,
            index_raft_id,
            index_replicaset_id,
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
                name: "instance_id".into(),
                parts: vec![Part::from("instance_id")],
                unique: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
                operable: true,
                local: true,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: "raft_id".into(),
                parts: vec![Part::from("raft_id")],
                unique: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
                operable: true,
                local: true,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 2,
                name: "replicaset_id".into(),
                parts: vec![Part::from("replicaset_id")],
                unique: false,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
                operable: true,
                local: true,
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
    pub fn delete(&self, instance_id: &str) -> tarantool::Result<()> {
        self.space.delete(&[instance_id])?;
        Ok(())
    }

    /// Finds an instance by `id` (see trait [`InstanceId`]).
    #[inline(always)]
    pub fn get(&self, id: &impl InstanceId) -> Result<Instance> {
        let res = id
            .find_in(self)?
            .decode()
            .expect("failed to decode instance");
        Ok(res)
    }

    /// Finds an instance by `id` (see trait [`InstanceId`]).
    /// Returns the tuple without deserializing.
    #[inline(always)]
    pub fn get_raw(&self, id: &impl InstanceId) -> Result<Tuple> {
        let res = id.find_in(self)?;
        Ok(res)
    }

    /// Checks if an instance with `id` (see trait [`InstanceId`]) is present.
    #[inline]
    pub fn contains(&self, id: &impl InstanceId) -> Result<bool> {
        match id.find_in(self) {
            Ok(_) => Ok(true),
            Err(Error::NoInstanceWithInstanceId(_)) => Ok(false),
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
        replicaset_id: &str,
    ) -> tarantool::Result<EntryIter<Instance>> {
        let iter = self
            .index_replicaset_id
            .select(IteratorType::Eq, &[replicaset_id])?;
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

impl ToEntryIter for Instances {
    type Entry = Instance;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// InstanceId
////////////////////////////////////////////////////////////////////////////////

/// Types implementing this trait can be used to identify a `Instance` when
/// accessing storage.
pub trait InstanceId: serde::Serialize {
    fn find_in(&self, instances: &Instances) -> Result<Tuple>;
}

impl InstanceId for RaftId {
    #[inline(always)]
    fn find_in(&self, instances: &Instances) -> Result<Tuple> {
        instances
            .index_raft_id
            .get(&[self])?
            .ok_or(Error::NoInstanceWithRaftId(*self))
    }
}

impl InstanceId for instance::InstanceId {
    #[inline(always)]
    fn find_in(&self, instances: &Instances) -> Result<Tuple> {
        instances
            .index_instance_id
            .get(&[self])?
            .ok_or_else(|| Error::NoInstanceWithInstanceId(self.clone()))
    }
}

////////////////////////////////////////////////////////////////////////////////
// EntryIter
////////////////////////////////////////////////////////////////////////////////

/// This trait is implemented for storage structs for iterating over the entries
/// from that storage.
pub trait ToEntryIter {
    /// Target type for entry deserialization.
    type Entry;

    fn index_iter(&self) -> tarantool::Result<IndexIterator>;

    #[inline(always)]
    fn iter(&self) -> tarantool::Result<EntryIter<Self::Entry>> {
        Ok(EntryIter::new(self.index_iter()?))
    }
}

/// An iterator struct for automatically deserializing tuples into a given type.
///
/// # Panics
/// Will panic in case deserialization fails on a given iteration.
pub struct EntryIter<T> {
    iter: IndexIterator,
    marker: PhantomData<T>,
}

impl<T> EntryIter<T> {
    pub fn new(iter: IndexIterator) -> Self {
        Self {
            iter,
            marker: PhantomData,
        }
    }
}

impl<T> Iterator for EntryIter<T>
where
    T: DecodeOwned,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iter.next().as_ref().map(Tuple::decode);
        res.map(|res| res.expect("entry should decode correctly"))
    }
}

impl<T> std::fmt::Debug for EntryIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .finish_non_exhaustive()
    }
}

////////////////////////////////////////////////////////////////////////////////
// SnapshotData
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct SnapshotCache {
    read_views: UnsafeCell<BTreeMap<RaftEntryId, SnapshotReadView>>,
}

impl SnapshotCache {
    #[allow(clippy::mut_from_ref)]
    fn read_views(&self) -> &mut BTreeMap<RaftEntryId, SnapshotReadView> {
        // Safety: this is safe as long as we only use it in tx thread.
        unsafe { &mut *self.read_views.get() }
    }
}

pub struct SnapshotReadView {
    /// Applied entry id at the time the read view was opened.
    entry_id: RaftEntryId,

    /// Global schema version at the moment of read view openning.
    schema_version: u64,

    /// [`fiber::clock()`] value at the moment snapshot read view was opened.
    /// Can be used to close stale read views with dangling references, which
    /// may occur if followers drop off during chunkwise snapshot application.
    time_opened: Instant,

    /// The read view onto the global spaces.
    read_view: ReadView,

    /// A table of in progress iterators. Whenever a snapshot chunk hits the
    /// size threshold, if the iterator didn't reach the end it's inserted into
    /// this table so that snapshot generation can be resumed.
    ///
    /// It would be nice if read view iterators supported pagination (there's
    /// actually no reason they shouldn't be able to) but they currently don't.
    /// If they did we wouldn't have to store the iterators, which is a pain
    /// because of rust, but for now it is what it is.
    ///
    /// NOTE: there's a 'static lifetime which is a bit of a lie. The iterators
    /// are actually borrowing the `read_view` field of this struct, but rust
    /// doesn't think we should be able to do that. NBD though, see the SAFETY
    /// note next to a `transmute` in `next_snapshot_data_chunk_impl` for more
    /// details.
    ///
    /// Fun fact: did you know that the underlying implementation of HashMap
    /// allows for it to be used as a multimap with multiple values associated
    /// with a single key? But there's no api for it, so we must do this ugly
    /// ass shit. Oh well..
    unfinished_iterators: HashMap<SnapshotPosition, Vec<Peekable<ReadViewIterator<'static>>>>,

    // TODO: this only works, if everybody who requested a snapshot always
    // reports when the last snapshot piece is received. But if an instance
    // suddenly diseapears it's ref_count will never be decremented and we'll
    // be holding on to a stale snapshot forever (until reboot).
    //
    // To fix this maybe we want to also store the id of an instance who
    // requested the snapshot (btw in raft-rs 0.7 raft::Storage::snapshot also
    // accepts a raft id of the requestor) and clear the references when the
    // instance is *determined* to not need it.
    ref_count: u32,
}

#[derive(Clone, Debug, Default, ::serde::Serialize, ::serde::Deserialize)]
pub struct SnapshotData {
    pub schema_version: u64,
    pub space_dumps: Vec<SpaceDump>,
    /// Position in the read view of the next chunk. If this is `None` the
    /// current chunk is the last one.
    pub next_chunk_position: Option<SnapshotPosition>,
}
impl Encode for SnapshotData {}

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct SpaceDump {
    pub space_id: SpaceId,
    #[serde(with = "serde_bytes")]
    pub tuples: TupleBuffer,
}
impl Encode for SpaceDump {}

/// Descriptor of a position of an iterator over a space which is in an open
/// read view.
#[rustfmt::skip]
#[derive(Debug, Default, Clone, Copy, Hash, PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct SnapshotPosition {
    /// Id of the space for which the last space dump was generated.
    pub space_id: SpaceId,

    /// Number of tuples from the last space which were sent out so far.
    /// Basically it's an offset of the first tuple which hasn't been sent yet.
    pub tuple_offset: u32,
}

impl std::fmt::Display for SnapshotPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "[space_id: {}, tuple_offset: {}]",
            self.space_id, self.tuple_offset
        )
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
            .index_builder("id")
            .unique(true)
            .part("id")
            .if_not_exists(true)
            .create()?;

        let index_name = space
            .index_builder("name")
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
        TableDef::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
        vec![
            IndexDef {
                table_id: Self::TABLE_ID,
                // Primary index
                id: 0,
                name: "id".into(),
                parts: vec![Part::from("id")],
                unique: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
                operable: true,
                local: true,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: "name".into(),
                parts: vec![Part::from("name")],
                unique: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
                operable: true,
                local: true,
            },
        ]
    }

    #[inline]
    pub fn get(&self, id: SpaceId) -> tarantool::Result<Option<TableDef>> {
        let tuple = self.space.get(&[id])?;
        tuple.as_ref().map(Tuple::decode).transpose()
    }

    #[inline]
    pub fn put(&self, space_def: &TableDef) -> tarantool::Result<()> {
        self.space.replace(space_def)?;
        Ok(())
    }

    #[inline]
    pub fn insert(&self, space_def: &TableDef) -> tarantool::Result<()> {
        self.space.insert(space_def)?;
        Ok(())
    }

    #[inline]
    pub fn update_operable(&self, id: SpaceId, operable: bool) -> tarantool::Result<()> {
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(TableDef::FIELD_OPERABLE, operable)?;
        self.space.update(&[id], ops)?;
        Ok(())
    }

    #[inline]
    pub fn delete(&self, id: SpaceId) -> tarantool::Result<Option<Tuple>> {
        self.space.delete(&[id])
    }

    #[inline]
    pub fn by_name(&self, name: &str) -> tarantool::Result<Option<TableDef>> {
        let tuple = self.index_name.get(&[name])?;
        tuple.as_ref().map(Tuple::decode).transpose()
    }
}

impl ToEntryIter for Tables {
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
            .index_builder("id")
            .unique(true)
            .part("table_id")
            .part("id")
            .if_not_exists(true)
            .create()?;

        let index_name = space
            .index_builder("name")
            .unique(true)
            .part("table_id")
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
                name: "id".into(),
                parts: vec![Part::from("table_id"), Part::from("id")],
                unique: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
                operable: true,
                local: true,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: "name".into(),
                parts: vec![Part::from("table_id"), Part::from("name")],
                unique: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
                operable: true,
                local: true,
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
        ops.assign(IndexDef::FIELD_OPERABLE, operable)?;
        self.space.update(&(space_id, index_id), ops)?;
        Ok(())
    }

    #[inline]
    pub fn delete(&self, space_id: SpaceId, index_id: IndexId) -> tarantool::Result<Option<Tuple>> {
        self.space.delete(&[space_id, index_id])
    }

    #[inline]
    pub fn by_name(&self, space_id: SpaceId, name: String) -> tarantool::Result<Option<IndexDef>> {
        let tuple = self.index_name.get(&(space_id, name))?;
        tuple.as_ref().map(Tuple::decode).transpose()
    }

    #[inline]
    pub fn by_space_id(&self, space_id: SpaceId) -> tarantool::Result<EntryIter<IndexDef>> {
        let iter = self.space.select(IteratorType::Eq, &[space_id])?;
        Ok(EntryIter::new(iter))
    }
}

impl ToEntryIter for Indexes {
    type Entry = IndexDef;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// ddl meta
////////////////////////////////////////////////////////////////////////////////

/// Updates the field `"operable"` for a space with id `space_id` and any
/// necessary entities (currently all existing indexes).
///
/// This function is called when applying the different ddl operations.
pub fn ddl_meta_space_update_operable(
    storage: &Clusterwide,
    space_id: SpaceId,
    operable: bool,
) -> traft::Result<()> {
    storage.tables.update_operable(space_id, operable)?;
    let iter = storage.indexes.by_space_id(space_id)?;
    for index in iter {
        storage
            .indexes
            .update_operable(index.table_id, index.id, operable)?;
    }
    Ok(())
}

/// Deletes the picodata internal metadata for a space with id `space_id`.
///
/// This function is called when applying the different ddl operations.
pub fn ddl_meta_drop_space(storage: &Clusterwide, space_id: SpaceId) -> traft::Result<()> {
    storage
        .privileges
        .delete_all_by_object(SchemaObjectType::Table, space_id as i64)?;
    let iter = storage.indexes.by_space_id(space_id)?;
    for index in iter {
        storage.indexes.delete(index.table_id, index.id)?;
    }
    storage.tables.delete(space_id)?;
    Ok(())
}

/// Deletes the picodata internal metadata for a routine with id `routine_id`.
pub fn ddl_meta_drop_routine(storage: &Clusterwide, routine_id: RoutineId) -> traft::Result<()> {
    storage
        .privileges
        .delete_all_by_object(SchemaObjectType::Routine, routine_id.into())?;
    storage.routines.delete(routine_id)?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// ddl
////////////////////////////////////////////////////////////////////////////////

pub fn ddl_abort_on_master(ddl: &Ddl, version: u64) -> traft::Result<()> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let sys_space = Space::from(SystemSpace::Space);
    let sys_index = Space::from(SystemSpace::Index);
    let sys_func = Space::from(SystemSpace::Func);

    match *ddl {
        Ddl::CreateTable { id, .. } => {
            sys_index.delete(&[id, 1])?;
            sys_index.delete(&[id, 0])?;
            sys_space.delete(&[id])?;
            set_local_schema_version(version)?;
        }

        Ddl::DropTable { .. } => {
            // Actual drop happens only on commit, so there's nothing to abort.
        }

        Ddl::CreateProcedure { id, .. } => {
            sys_func.delete(&[id])?;
            set_local_schema_version(version)?;
        }

        Ddl::DropProcedure { .. } => {
            // Actual drop happens only on commit, so there's nothing to abort.
        }

        Ddl::RenameProcedure {
            routine_id,
            ref old_name,
            ref new_name,
            ..
        } => {
            let lua = ::tarantool::lua_state();
            lua.exec_with(
                "local new_name = ...
                box.schema.func.drop(new_name, {if_exists = true})
                ",
                (new_name,),
            )
            .map_err(LuaError::from)?;

            lua.exec_with(LUA_CREATE_PROC_SCRIPT, (routine_id, old_name, true))
                .map_err(LuaError::from)?;

            set_local_schema_version(version)?;
        }

        _ => {
            todo!();
        }
    }

    Ok(())
}

// This script is reused between create and rename procedure,
// when applying changes on master.
const LUA_CREATE_PROC_SCRIPT: &str = r#"
    local func_id, func_name, not_exists = ...
    local def = {
        language = 'LUA',
        body = string.format(
            [[function() error("function %s is used internally by picodata") end]],
            func_name
        ),
        id = func_id,
        if_not_exists = not_exists,
    }
    box.schema.func.create(func_name, def)"#;

/// Create tarantool function which throws an error if it's called.
/// Tarantool function is created with `if_not_exists = true`, so it's
/// safe to call this rust function multiple times.
pub fn ddl_create_function_on_master(func_id: u32, func_name: &str) -> traft::Result<()> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let lua = ::tarantool::lua_state();
    lua.exec_with(LUA_CREATE_PROC_SCRIPT, (func_id, func_name, true))
        .map_err(LuaError::from)?;
    Ok(())
}

pub fn ddl_rename_function_on_master(id: u32, old_name: &str, new_name: &str) -> traft::Result<()> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });

    // We can't do update/replace on _func space, so in order to rename
    // our dummy lua function we need to drop it and create it with a
    // new name.
    let lua = ::tarantool::lua_state();
    lua.exec_with(
        "local func_name = ...
        box.schema.func.drop(func_name, {if_exists = true})
        ",
        (old_name,),
    )
    .map_err(LuaError::from)?;

    lua.exec_with(LUA_CREATE_PROC_SCRIPT, (id, new_name, false))
        .map_err(LuaError::from)?;

    Ok(())
}

/// Drop tarantool function created by ddl_create_function_on_master and it's privileges.
/// Dropping a non-existent function is not an error.
///
// FIXME: this function returns 2 kinds of errors: retryable and non-retryable.
// Currently this is impelemnted by returning one kind of errors as Err(e) and
// the other as Ok(Some(e)). This was the simplest solution at the time this
// function was implemented, as it requires the least amount of boilerplate and
// error forwarding code. But this signature is not intuitive, so maybe there's
// room for improvement.
pub fn ddl_drop_function_on_master(func_id: u32) -> traft::Result<Option<TntError>> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let sys_func = Space::from(SystemSpace::Func);
    let sys_priv = Space::from(SystemSpace::Priv);

    let priv_idx: Index = sys_priv
        .index("object")
        .expect("index 'object' not found in space '_priv'");
    let iter = priv_idx.select(IteratorType::Eq, &("function", func_id))?;
    let mut priv_ids = Vec::new();
    for tuple in iter {
        let grantee: u32 = tuple
            .field(1)?
            .expect("decoding metadata should never fail");
        priv_ids.push((grantee, "function", func_id));
    }

    let res = (|| -> tarantool::Result<()> {
        for pk_tuple in priv_ids.iter().rev() {
            sys_priv.delete(pk_tuple)?;
        }
        sys_func.delete(&[func_id])?;
        Ok(())
    })();
    Ok(res.err())
}

/// Create tarantool space and any required indexes. Currently it creates a
/// primary index and a `bucket_id` index if it's a sharded space.
///
/// Return values:
/// * `Ok(None)` in case of success.
/// * `Ok(Some(abort_reason))` in case of error which should result in a ddl abort.
/// * `Err(e)` in case of retryable errors.
///
// FIXME: this function returns 2 kinds of errors: retryable and non-retryable.
// Currently this is impelemnted by returning one kind of errors as Err(e) and
// the other as Ok(Some(e)). This was the simplest solution at the time this
// function was implemented, as it requires the least amount of boilerplate and
// error forwarding code. But this signature is not intuitive, so maybe there's
// room for improvement.
pub fn ddl_create_space_on_master(
    storage: &Clusterwide,
    space_id: SpaceId,
) -> traft::Result<Option<TntError>> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let sys_space = Space::from(SystemSpace::Space);
    let sys_index = Space::from(SystemSpace::Index);

    let pico_space_def = storage
        .tables
        .get(space_id)?
        .ok_or_else(|| Error::other(format!("space with id {space_id} not found")))?;
    // TODO: set defaults
    let tt_space_def = pico_space_def.to_space_metadata()?;

    let pico_pk_def = storage.indexes.get(space_id, 0)?.ok_or_else(|| {
        Error::other(format!(
            "primary index for space {} not found",
            pico_space_def.name
        ))
    })?;
    let tt_pk_def = pico_pk_def.to_index_metadata();

    // For now we just assume that during space creation index with id 1
    // exists if and only if it is a bucket_id index.
    let mut tt_bucket_id_def = None;
    let pico_bucket_id_def = storage.indexes.get(space_id, 1)?;
    if let Some(def) = &pico_bucket_id_def {
        tt_bucket_id_def = Some(def.to_index_metadata());
    }

    let res = (|| -> tarantool::Result<()> {
        if tt_pk_def.parts.is_empty() {
            return Err(tarantool::set_and_get_error!(
                tarantool::error::TarantoolErrorCode::ModifyIndex,
                "can't create index '{}' in space '{}': parts list cannot be empty",
                tt_pk_def.name,
                tt_space_def.name,
            )
            .into());
        }
        sys_space.insert(&tt_space_def)?;
        sys_index.insert(&tt_pk_def)?;
        if let Some(def) = tt_bucket_id_def {
            sys_index.insert(&def)?;
        }

        Ok(())
    })();
    Ok(res.err())
}

/// Drop tarantool space and any entities which depend on it (indexes, privileges
/// and truncates).
///
/// Return values:
/// * `Ok(None)` in case of success.
/// * `Ok(Some(abort_reason))` in case of error which should result in a ddl abort.
/// * `Err(e)` in case of retryable errors.
///
// FIXME: this function returns 2 kinds of errors: retryable and non-retryable.
// Currently this is impelemnted by returning one kind of errors as Err(e) and
// the other as Ok(Some(e)). This was the simplest solution at the time this
// function was implemented, as it requires the least amount of boilerplate and
// error forwarding code. But this signature is not intuitive, so maybe there's
// room for improvement.
pub fn ddl_drop_space_on_master(space_id: SpaceId) -> traft::Result<Option<TntError>> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let sys_space = Space::from(SystemSpace::Space);
    let sys_index = Space::from(SystemSpace::Index);
    let sys_truncate = Space::from(SystemSpace::Truncate);
    let sys_priv = Space::from(SystemSpace::Priv);

    let iter = sys_index.select(IteratorType::Eq, &[space_id])?;
    let mut index_ids = Vec::with_capacity(4);
    for tuple in iter {
        let index_id: IndexId = tuple
            .field(1)?
            .expect("decoding metadata should never fail");
        // Primary key is handled explicitly.
        if index_id != 0 {
            index_ids.push(index_id);
        }
    }

    let priv_idx: Index = sys_priv
        .index("object")
        .expect("index 'object' not found in space '_priv'");
    let iter = priv_idx.select(IteratorType::Eq, &("space", space_id))?;
    let mut priv_ids = Vec::new();
    for tuple in iter {
        let grantee: u32 = tuple
            .field(1)?
            .expect("decoding metadata should never fail");
        priv_ids.push((grantee, "space", space_id));
    }

    let res = (|| -> tarantool::Result<()> {
        for pk_tuple in priv_ids.iter().rev() {
            sys_priv.delete(pk_tuple)?;
        }
        for iid in index_ids.iter().rev() {
            sys_index.delete(&(space_id, iid))?;
        }
        // Primary key must be dropped last.
        sys_index.delete(&(space_id, 0))?;
        sys_truncate.delete(&[space_id])?;
        sys_space.delete(&[space_id])?;

        Ok(())
    })();
    Ok(res.err())
}

////////////////////////////////////////////////////////////////////////////////
// Users
////////////////////////////////////////////////////////////////////////////////

impl Users {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index_id = space
            .index_builder("id")
            .unique(true)
            .part("id")
            .if_not_exists(true)
            .create()?;

        let index_name = space
            .index_builder("name")
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
        UserDef::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
        vec![
            IndexDef {
                table_id: Self::TABLE_ID,
                // Primary index
                id: 0,
                name: "id".into(),
                parts: vec![Part::from("id")],
                unique: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
                operable: true,
                local: true,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: "name".into(),
                parts: vec![Part::from("name")],
                unique: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
                operable: true,
                local: true,
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
        ops.assign(UserDef::FIELD_NAME, name)?;
        self.space.update(&[user_id], ops)?;
        Ok(())
    }

    #[inline]
    pub fn update_auth(&self, user_id: UserId, auth: &AuthDef) -> tarantool::Result<()> {
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(UserDef::FIELD_AUTH, auth)?;
        self.space.update(&[user_id], ops)?;
        Ok(())
    }
}

impl ToEntryIter for Users {
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
            .index_builder("primary")
            .unique(true)
            .parts(["grantee_id", "object_type", "object_id", "privilege"])
            .if_not_exists(true)
            .create()?;

        let object_idx = space
            .index_builder("object")
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
                name: "primary".into(),
                parts: vec![
                    Part::from("grantee_id"),
                    Part::from("object_type"),
                    Part::from("object_id"),
                    Part::from("privilege"),
                ],
                unique: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
                operable: true,
                local: true,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: "object".into(),
                parts: vec![Part::from("object_type"), Part::from("object_id")],
                unique: false,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
                operable: true,
                local: true,
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
    pub fn by_grantee_id(&self, grantee_id: UserId) -> tarantool::Result<EntryIter<PrivilegeDef>> {
        let iter = self.primary_key.select(IteratorType::Eq, &[grantee_id])?;
        Ok(EntryIter::new(iter))
    }

    #[inline(always)]
    pub fn by_object(
        &self,
        object_type: SchemaObjectType,
        object_id: i64,
    ) -> tarantool::Result<EntryIter<PrivilegeDef>> {
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

impl ToEntryIter for Privileges {
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
            .index_builder("name")
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
            name: "name".into(),
            parts: vec![Part::from("name")],
            unique: true,
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
            operable: true,
            local: true,
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

impl ToEntryIter for Tiers {
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
            .index_builder("id")
            .unique(true)
            .part("id")
            .if_not_exists(true)
            .create()?;

        let index_name = space
            .index_builder("name")
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
        RoutineDef::format()
    }

    #[inline]
    pub fn index_definitions() -> Vec<IndexDef> {
        vec![
            IndexDef {
                table_id: Self::TABLE_ID,
                // Primary index
                id: 0,
                name: "id".into(),
                parts: vec![Part::from("id")],
                unique: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
                operable: true,
                local: true,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: "name".into(),
                parts: vec![Part::from("name")],
                unique: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
                operable: true,
                local: true,
            },
        ]
    }

    #[inline(always)]
    pub fn rename(&self, id: u32, new_name: &str) -> tarantool::Result<Option<RoutineDef>> {
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(RoutineDef::FIELD_NAME, new_name)?;
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
        ops.assign(RoutineDef::FIELD_OPERABLE, operable)?;
        self.space.update(&[routine_id], ops)?;
        Ok(())
    }
}

impl ToEntryIter for Routines {
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
// SchemaDef
////////////////////////////////////////////////////////////////////////////////

/// Types implemeting this trait describe different kinds of schema definitions
/// (both ddl and acl).
///
/// This trait is currently only used to minimize code duplication in the
/// [`Clusterwide::apply_snapshot_data`] function.
trait SchemaDef {
    /// Type of unique key used to identify entities for the purpose of
    /// associating the schema version with.
    type Key: std::hash::Hash + Eq + std::fmt::Debug;

    /// Extract unique key from the entity.
    fn key(&self) -> Self::Key;

    /// Extract the schema version at which the entity was created.
    fn schema_version(&self) -> u64;

    /// Checks if the entity is currently operable. Is only applicable for ddl
    /// entities, i.e. spaces and indexes, hence returns `true` for everything
    /// else.
    fn is_operable(&self) -> bool {
        true
    }

    /// Is called when the entity is being created in the local storage.
    ///
    /// Should perform the necessary tarantool api calls and or tarantool system
    /// space updates.
    fn on_insert(&self, storage: &Clusterwide) -> traft::Result<()>;

    /// Is called when the entity is being dropped in the local storage.
    /// If the entity is being changed, first `Self::on_delete` is called and
    /// then `Self::on_insert` is called for it.
    ///
    /// Should perform the necessary tarantool api calls and or tarantool system
    /// space updates.
    fn on_delete(key: &Self::Key, storage: &Clusterwide) -> traft::Result<()>;
}

/// Ignore specific tarantool error.
/// E.g. if `res` contains an `ignored` error, it will be
/// transformed to ok instead.
fn ignore_only_error(res: tarantool::Result<()>, ignored: TntErrorCode) -> tarantool::Result<()> {
    if let Err(err) = res {
        if let TntError::Tarantool(tnt_err) = &err {
            if tnt_err.error_code() == ignored as u32 {
                return Ok(());
            }
        }
        return Err(err);
    }
    Ok(())
}

impl SchemaDef for TableDef {
    type Key = SpaceId;

    #[inline(always)]
    fn key(&self) -> SpaceId {
        self.id
    }

    #[inline(always)]
    fn schema_version(&self) -> u64 {
        self.schema_version
    }

    #[inline(always)]
    fn is_operable(&self) -> bool {
        self.operable
    }

    #[inline(always)]
    fn on_insert(&self, storage: &Clusterwide) -> traft::Result<()> {
        let space_id = self.id;
        if let Some(abort_reason) = ddl_create_space_on_master(storage, space_id)? {
            return Err(Error::other(format!(
                "failed to create table {space_id}: {abort_reason}"
            )));
        }
        Ok(())
    }

    #[inline(always)]
    fn on_delete(space_id: &SpaceId, storage: &Clusterwide) -> traft::Result<()> {
        _ = storage;
        if let Some(abort_reason) = ddl_drop_space_on_master(*space_id)? {
            return Err(Error::other(format!(
                "failed to drop table {space_id}: {abort_reason}"
            )));
        }
        Ok(())
    }
}

impl SchemaDef for UserDef {
    type Key = UserId;

    #[inline(always)]
    fn key(&self) -> UserId {
        self.id
    }

    #[inline(always)]
    fn schema_version(&self) -> u64 {
        self.schema_version
    }

    #[inline(always)]
    fn on_insert(&self, storage: &Clusterwide) -> traft::Result<()> {
        _ = storage;
        let res = {
            if self.is_role() {
                acl::on_master_create_role(self)
            } else {
                acl::on_master_create_user(self, true)
            }
        };
        ignore_only_error(res, TntErrorCode::TupleFound)?;
        Ok(())
    }

    #[inline(always)]
    fn on_delete(user_id: &UserId, storage: &Clusterwide) -> traft::Result<()> {
        _ = storage;

        let user = user_by_id(*user_id)?;
        match user.ty {
            UserMetadataKind::User => on_master_drop_user(*user_id)?,
            UserMetadataKind::Role => on_master_drop_role(*user_id)?,
        }

        Ok(())
    }
}

impl SchemaDef for PrivilegeDef {
    type Key = Self;

    #[inline(always)]
    fn key(&self) -> Self {
        self.clone()
    }

    #[inline(always)]
    fn schema_version(&self) -> u64 {
        PrivilegeDef::schema_version(self)
    }

    #[inline(always)]
    fn on_insert(&self, storage: &Clusterwide) -> traft::Result<()> {
        _ = storage;
        acl::on_master_grant_privilege(self)?;
        Ok(())
    }

    #[inline(always)]
    fn on_delete(this: &Self, storage: &Clusterwide) -> traft::Result<()> {
        _ = storage;
        acl::on_master_revoke_privilege(this)?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////
// acl
////////////////////////////////////////////////////////////////////////////////

pub mod acl {
    use tarantool::clock;

    use crate::access_control::user_by_id;

    use super::*;

    impl PrivilegeDef {
        /// Resolve grantee's type and name and return them as strings.
        /// Panics if the storage's invariants do not uphold.
        fn grantee_type_and_name(
            &self,
            storage: &Clusterwide,
        ) -> tarantool::Result<(&'static str, String)> {
            let user_def = storage.users.by_id(self.grantee_id())?;
            let Some(user_def) = user_def else {
                panic!("found neither user nor role for grantee_id")
            };

            if user_def.is_role() {
                Ok(("role", user_def.name))
            } else {
                Ok(("user", user_def.name))
            }
        }
    }

    fn get_default_privileges_for_user(
        user_def: &UserDef,
        grantor_id: UserId,
    ) -> [PrivilegeDef; 3] {
        [
            // SQL: GRANT 'public' TO <user_name>
            PrivilegeDef::new(
                PrivilegeType::Execute,
                SchemaObjectType::Role,
                PUBLIC_ID as _,
                user_def.id,
                grantor_id,
                user_def.schema_version,
            )
            .expect("valid"),
            // SQL: ALTER USER <user_name> WITH LOGIN
            PrivilegeDef::new(
                PrivilegeType::Login,
                SchemaObjectType::Universe,
                UNIVERSE_ID,
                user_def.id,
                ADMIN_ID,
                user_def.schema_version,
            )
            .expect("valid"),
            // SQL: GRANT ALTER ON <user_name> TO <user_name>
            PrivilegeDef::new(
                PrivilegeType::Alter,
                SchemaObjectType::User,
                user_def.id as _,
                user_def.id,
                grantor_id,
                user_def.schema_version,
            )
            .expect("valid"),
        ]
    }

    ////////////////////////////////////////////////////////////////////////////
    // acl in global storage
    ////////////////////////////////////////////////////////////////////////////

    /// Persist a user definition with it's default privileges in the internal clusterwide storage.
    pub fn global_create_user(storage: &Clusterwide, user_def: &UserDef) -> tarantool::Result<()> {
        storage.users.insert(user_def)?;

        let owner_def = user_by_id(user_def.owner)?;

        let user = &user_def.name;
        crate::audit!(
            message: "created user `{user}`",
            title: "create_user",
            severity: High,
            auth_type: user_def.auth.method.as_str(),
            user: user,
            initiator: owner_def.name,
        );

        for user_priv in get_default_privileges_for_user(user_def, user_def.owner) {
            global_grant_privilege(storage, &user_priv)?;
        }

        Ok(())
    }

    /// Change user's auth info in the internal clusterwide storage.
    pub fn global_change_user_auth(
        storage: &Clusterwide,
        user_id: UserId,
        auth: &AuthDef,
        initiator: UserId,
    ) -> tarantool::Result<()> {
        storage.users.update_auth(user_id, auth)?;

        let user_def = storage.users.by_id(user_id)?.expect("failed to get user");
        let user = &user_def.name;

        let initiator_def = user_by_id(initiator)?;

        crate::audit!(
            message: "password of user `{user}` was changed",
            title: "change_password",
            severity: High,
            auth_type: auth.method.as_str(),
            user: user,
            initiator: initiator_def.name,
        );

        Ok(())
    }

    /// Change user's name in the internal clusterwide storage.
    pub fn global_rename_user(
        storage: &Clusterwide,
        user_id: UserId,
        new_name: &str,
    ) -> tarantool::Result<()> {
        let user_with_old_name = storage.users.by_id(user_id)?.expect("failed to get user");
        let old_name = &user_with_old_name.name;
        storage.users.update_name(user_id, new_name)?;

        // TODO: missing initiator, should it be a required field?
        crate::audit!(
            message: "name of user `{old_name}` was changed to `{new_name}`",
            title: "rename_user",
            severity: High,
            old_name: old_name,
            new_name: new_name,
        );

        Ok(())
    }

    /// Remove a user definition and any entities owned by it from the internal
    /// clusterwide storage.
    pub fn global_drop_user(
        storage: &Clusterwide,
        user_id: UserId,
        initiator: UserId,
    ) -> tarantool::Result<()> {
        let user_def = storage.users.by_id(user_id)?.expect("failed to get user");
        storage.privileges.delete_all_by_grantee_id(user_id)?;
        storage.users.delete(user_id)?;

        let initiator_def = user_by_id(initiator)?;

        let user = &user_def.name;
        crate::audit!(
            message: "dropped user `{user}`",
            title: "drop_user",
            severity: Medium,
            user: user,
            initiator: initiator_def.name,
        );

        Ok(())
    }

    /// Persist a role definition in the internal clusterwide storage.
    pub fn global_create_role(storage: &Clusterwide, role_def: &UserDef) -> tarantool::Result<()> {
        storage.users.insert(role_def)?;

        let initiator_def = user_by_id(role_def.owner)?;

        let role = &role_def.name;
        crate::audit!(
            message: "created role `{role}`",
            title: "create_role",
            severity: High,
            role: role,
            initiator: initiator_def.name,
        );

        Ok(())
    }

    /// Remove a role definition and any entities owned by it from the internal
    /// clusterwide storage.
    pub fn global_drop_role(
        storage: &Clusterwide,
        role_id: UserId,
        initiator: UserId,
    ) -> Result<()> {
        let role_def = storage.users.by_id(role_id)?.expect("role should exist");
        storage.privileges.delete_all_by_grantee_id(role_id)?;
        storage.privileges.delete_all_by_granted_role(role_id)?;
        storage.users.delete(role_id)?;

        let initiator_def = user_by_id(initiator)?;

        let role = &role_def.name;
        crate::audit!(
            message: "dropped role `{role}`",
            title: "drop_role",
            severity: Medium,
            role: role,
            initiator: initiator_def.name,
        );
        Ok(())
    }

    /// Persist a privilege definition in the internal clusterwide storage.
    pub fn global_grant_privilege(
        storage: &Clusterwide,
        priv_def: &PrivilegeDef,
    ) -> tarantool::Result<()> {
        storage.privileges.insert(priv_def, true)?;

        let privilege = &priv_def.privilege();
        let object = priv_def
            .resolve_object_name(storage)
            .expect("target object should exist");
        let object_type = &priv_def.object_type();
        let (grantee_type, grantee) = priv_def.grantee_type_and_name(storage)?;
        let initiator_def = user_by_id(priv_def.grantor_id())?;

        // Reset login attempts counter for a user on session grant
        if *privilege == PrivilegeType::Login {
            // Borrowing will not panic as there are no yields while it's borrowed
            storage.login_attempts.borrow_mut().remove(&grantee);
        }

        // Emit audit log
        match (privilege.as_str(), object_type.as_str()) {
            ("execute", "role") => {
                let object = object.expect("should be set");
                crate::audit!(
                    message: "granted role `{object}` to {grantee_type} `{grantee}`",
                    title: "grant_role",
                    severity: High,
                    role: &object,
                    grantee: &grantee,
                    grantee_type: grantee_type,
                    initiator: initiator_def.name,
                );
            }
            _ => {
                let object = object.as_deref().unwrap_or("*");
                crate::audit!(
                    message: "granted privilege {privilege} \
                        on {object_type} `{object}` \
                        to {grantee_type} `{grantee}`",
                    title: "grant_privilege",
                    severity: High,
                    privilege: privilege.as_str(),
                    object: object,
                    object_type: object_type.as_str(),
                    grantee: &grantee,
                    grantee_type: grantee_type,
                    initiator: initiator_def.name,
                );
            }
        }

        Ok(())
    }

    /// Remove a privilege definition from the internal clusterwide storage.
    pub fn global_revoke_privilege(
        storage: &Clusterwide,
        priv_def: &PrivilegeDef,
        initiator: UserId,
    ) -> tarantool::Result<()> {
        storage.privileges.delete(
            priv_def.grantee_id(),
            &priv_def.object_type(),
            priv_def.object_id_raw(),
            &priv_def.privilege(),
        )?;

        let privilege = &priv_def.privilege();
        let object = priv_def
            .resolve_object_name(storage)
            .expect("target object should exist");
        let object_type = &priv_def.object_type();
        let (grantee_type, grantee) = priv_def.grantee_type_and_name(storage)?;
        let initiator_def = user_by_id(initiator)?;

        match (privilege.as_str(), object_type.as_str()) {
            ("execute", "role") => {
                let object = object.expect("should be set");
                crate::audit!(
                    message: "revoked role `{object}` from {grantee_type} `{grantee}`",
                    title: "revoke_role",
                    severity: High,
                    role: &object,
                    grantee: &grantee,
                    grantee_type: grantee_type,
                    initiator: initiator_def.name,
                );
            }
            _ => {
                let object = object.as_deref().unwrap_or("*");
                crate::audit!(
                    message: "revoked privilege {privilege} \
                        on {object_type} `{object}` \
                        from {grantee_type} `{grantee}`",
                    title: "revoke_privilege",
                    severity: High,
                    privilege: privilege.as_str(),
                    object: object,
                    object_type: object_type.as_str(),
                    grantee: &grantee,
                    grantee_type: grantee_type,
                    initiator: initiator_def.name,
                );
            }
        }

        Ok(())
    }

    ////////////////////////////////////////////////////////////////////////////
    // acl in local storage on replicaset leader
    ////////////////////////////////////////////////////////////////////////////

    /// Create a tarantool user.
    ///
    /// If `basic_privileges` is `true` the new user is granted the following:
    /// - Role "public"
    /// - Alter self
    /// - Session access on "universe"
    /// - Usage access on "universe"
    pub fn on_master_create_user(
        user_def: &UserDef,
        basic_privileges: bool,
    ) -> tarantool::Result<()> {
        let sys_user = Space::from(SystemSpace::User);
        let user_id = user_def.id;

        // This implementation was copied from box.schema.user.create excluding the
        // password hashing.

        // Tarantool expects auth info to be a map of form `{ method: data }`,
        // and currently the simplest way to achieve this is to use a HashMap.
        let auth_map = HashMap::from([(user_def.auth.method, &user_def.auth.data)]);
        sys_user.insert(&(
            user_id,
            user_def.owner,
            &user_def.name,
            "user",
            auth_map,
            &[(); 0],
            0,
        ))?;

        if !basic_privileges {
            return Ok(());
        }

        let lua = ::tarantool::lua_state();
        lua.exec_with("box.schema.user.grant(...)", (user_id, "public"))
            .map_err(LuaError::from)?;
        lua.exec_with(
            "box.schema.user.grant(...)",
            (user_id, "alter", "user", user_id),
        )
        .map_err(LuaError::from)?;
        lua.exec_with(
            "box.session.su('admin', box.schema.user.grant, ...)",
            (
                user_id,
                "session,usage",
                "universe",
                tlua::Nil,
                tlua::AsTable((("if_not_exists", true),)),
            ),
        )
        .map_err(LuaError::from)?;

        Ok(())
    }

    /// Update a tarantool user's authentication details.
    pub fn on_master_change_user_auth(user_id: UserId, auth: &AuthDef) -> tarantool::Result<()> {
        const USER_FIELD_AUTH: i32 = 4;
        const USER_FIELD_LAST_MODIFIED: i32 = 6;
        let sys_user = Space::from(SystemSpace::User);

        // Tarantool expects auth info to be a map of form `{ method: data }`,
        // and currently the simplest way to achieve this is to use a HashMap.
        let auth_map = HashMap::from([(auth.method, &auth.data)]);
        let mut ops = UpdateOps::with_capacity(2);
        ops.assign(USER_FIELD_AUTH, auth_map)?;
        ops.assign(USER_FIELD_LAST_MODIFIED, clock::time64())?;
        sys_user.update(&[user_id], ops)?;
        Ok(())
    }

    /// Rename a tarantool user.
    pub fn on_master_rename_user(user_id: UserId, new_name: &str) -> tarantool::Result<()> {
        const USER_FIELD_NAME: i32 = 2;
        let sys_user = Space::from(SystemSpace::User);

        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(USER_FIELD_NAME, new_name)?;
        sys_user.update(&[user_id], ops)?;
        Ok(())
    }

    /// Drop a tarantool user and any entities (spaces, etc.) owned by it.
    pub fn on_master_drop_user(user_id: UserId) -> tarantool::Result<()> {
        let lua = ::tarantool::lua_state();
        lua.exec_with("box.schema.user.drop(...)", user_id)
            .map_err(LuaError::from)?;

        Ok(())
    }

    /// Create a tarantool role.
    pub fn on_master_create_role(role_def: &UserDef) -> tarantool::Result<()> {
        let sys_user = Space::from(SystemSpace::User);

        // This implementation was copied from box.schema.role.create.

        // Tarantool expects auth info to be a map `{}`, and currently the simplest
        // way to achieve this is to use a HashMap.
        sys_user.insert(&(
            role_def.id,
            role_def.owner,
            &role_def.name,
            "role",
            HashMap::<(), ()>::new(),
            &[(); 0],
            0,
        ))?;

        Ok(())
    }

    /// Drop a tarantool role and revoke it from anybody it was assigned to.
    pub fn on_master_drop_role(role_id: UserId) -> tarantool::Result<()> {
        let lua = ::tarantool::lua_state();
        lua.exec_with("box.schema.role.drop(...)", role_id)
            .map_err(LuaError::from)?;

        Ok(())
    }

    /// Grant a tarantool user or role the privilege defined by `priv_def`.
    /// Is idempotent: will not return an error even if the privilege is already granted.
    pub fn on_master_grant_privilege(priv_def: &PrivilegeDef) -> tarantool::Result<()> {
        let lua = ::tarantool::lua_state();
        lua.exec_with(
            "local grantee_id, privilege, object_type, object_id = ...
            local grantee_def = box.space._user:get(grantee_id)
            if grantee_def.type == 'user' then
                box.schema.user.grant(grantee_id, privilege, object_type, object_id, {if_not_exists=true})
            else
                box.schema.role.grant(grantee_id, privilege, object_type, object_id, {if_not_exists=true})
            end",
            (
                priv_def.grantee_id(),
                priv_def.privilege().as_tarantool(),
                priv_def.object_type().as_tarantool(),
                priv_def.object_id(),
            ),
        )
        .map_err(LuaError::from)?;

        Ok(())
    }

    /// Revoke a privilege from a tarantool user or role.
    /// Is idempotent: will not return an error even if the privilege was not granted.
    pub fn on_master_revoke_privilege(priv_def: &PrivilegeDef) -> tarantool::Result<()> {
        let lua = ::tarantool::lua_state();
        lua.exec_with(
            "local grantee_id, privilege, object_type, object_id = ...
            local grantee_def = box.space._user:get(grantee_id)
            if not grantee_def then
                -- Grantee already dropped -> privileges already revoked
                return
            end
            if grantee_def.type == 'user' then
                box.schema.user.revoke(grantee_id, privilege, object_type, object_id, {if_exists=true})
            else
                box.schema.role.revoke(grantee_id, privilege, object_type, object_id, {if_exists=true})
            end",
            (
                priv_def.grantee_id(),
                priv_def.privilege().as_tarantool(),
                priv_def.object_type().as_tarantool(),
                priv_def.object_id(),
            ),
        )
        .map_err(LuaError::from)?;

        Ok(())
    }
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
    use tarantool::transaction::transaction;

    #[rustfmt::skip]
    #[::tarantool::test]
    fn test_storage_instances() {
        use crate::instance::GradeVariant::*;
        use crate::instance::InstanceId;
        use crate::failure_domain::FailureDomain;

        let storage = Clusterwide::for_tests();
        let storage_peer_addresses = PeerAddresses::new().unwrap();
        let space_peer_addresses = storage_peer_addresses.space.clone();

        let faildom = FailureDomain::from([("a", "b")]);

        for instance in vec![
            // r1
            ("i1", "i1-uuid", 1u64, "r1", "r1-uuid", (Online, 0), (Online, 0), &faildom, DEFAULT_TIER,),
            ("i2", "i2-uuid", 2u64, "r1", "r1-uuid", (Online, 0), (Online, 0), &faildom, DEFAULT_TIER,),
            // r2
            ("i3", "i3-uuid", 3u64, "r2", "r2-uuid", (Online, 0), (Online, 0), &faildom, DEFAULT_TIER,),
            ("i4", "i4-uuid", 4u64, "r2", "r2-uuid", (Online, 0), (Online, 0), &faildom, DEFAULT_TIER,),
            // r3
            ("i5", "i5-uuid", 5u64, "r3", "r3-uuid", (Online, 0), (Online, 0), &faildom, DEFAULT_TIER,),
        ] {
            storage.space_by_name(ClusterwideTable::Instance).unwrap().put(&instance).unwrap();
            let (_, _, raft_id, ..) = instance;
            space_peer_addresses.put(&(raft_id, format!("addr:{raft_id}"))).unwrap();
        }

        let instance = storage.instances.all_instances().unwrap();
        assert_eq!(
            instance.iter().map(|p| &p.instance_id).collect::<Vec<_>>(),
            vec!["i1", "i2", "i3", "i4", "i5"]
        );

        assert_err!(
            storage.instances.put(&Instance {
                raft_id: 1,
                instance_id: "i99".into(),
                tier: DEFAULT_TIER.into(),
                ..Instance::default()
            }),
            format!(
                concat!(
                    "tarantool error:",
                    " TupleFound: Duplicate key exists",
                    " in unique index \"raft_id\"",
                    " in space \"_pico_instance\"",
                    " with old tuple",
                    r#" - ["i1", "i1-uuid", 1, "r1", "r1-uuid", ["{gon}", 0], ["{tgon}", 0], {{"A": "B"}}, "default"]"#,
                    " and new tuple",
                    r#" - ["i99", "", 1, "", "", ["{goff}", 0], ["{tgoff}", 0], {{}}, "default"]"#,
                ),
                gon = Online,
                goff = Offline,
                tgon = Online,
                tgoff = Offline,
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
            assert_eq!(storage.instances.get(&1).unwrap().instance_id, "i1");
            assert_eq!(storage.instances.get(&2).unwrap().instance_id, "i2");
            assert_eq!(storage.instances.get(&3).unwrap().instance_id, "i3");
            assert_eq!(storage.instances.get(&4).unwrap().instance_id, "i4");
            assert_eq!(storage.instances.get(&5).unwrap().instance_id, "i5");
            assert_err!(storage.instances.get(&6), "instance with id 6 not found");
        }

        {
            // Check accessing instances by 'instance_id'
            assert_eq!(storage.instances.get(&InstanceId::from("i1")).unwrap().raft_id, 1);
            assert_eq!(storage.instances.get(&InstanceId::from("i2")).unwrap().raft_id, 2);
            assert_eq!(storage.instances.get(&InstanceId::from("i3")).unwrap().raft_id, 3);
            assert_eq!(storage.instances.get(&InstanceId::from("i4")).unwrap().raft_id, 4);
            assert_eq!(storage.instances.get(&InstanceId::from("i5")).unwrap().raft_id, 5);
            assert_err!(
                storage.instances.get(&InstanceId::from("i6")),
                "instance with id \"i6\" not found"
            );
        }

        let box_replication = |replicaset_id: &str| -> Vec<traft::Address> {
            storage.instances.replicaset_instances(replicaset_id).unwrap()
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
                "tarantool error: NoSuchSpace: Space '{}' does not exist",
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

        let inst = |raft_id, instance_id: &str| Instance {
            raft_id,
            instance_id: instance_id.into(),
            ..Instance::default()
        };
        storage.instances.put(&inst(1, "bob")).unwrap();

        let t = storage.instances.index_raft_id.get(&[1]).unwrap().unwrap();
        let i: Instance = t.decode().unwrap();
        assert_eq!(i.raft_id, 1);
        assert_eq!(i.instance_id, "bob");
    }

    #[::tarantool::test]
    fn snapshot_data() {
        let storage = Clusterwide::for_tests();

        let i = Instance {
            raft_id: 1,
            instance_id: "i".into(),
            ..Instance::default()
        };
        storage.instances.put(&i).unwrap();

        storage
            .peer_addresses
            .space
            .insert(&(1, "google.com"))
            .unwrap();
        storage.peer_addresses.space.insert(&(2, "ya.ru")).unwrap();

        storage.properties.space.insert(&("foo", "bar")).unwrap();

        let r = Replicaset::for_tests();
        storage.replicasets.space.insert(&r).unwrap();

        let (snapshot_data, _) = storage
            .first_snapshot_data_chunk(Default::default(), Default::default())
            .unwrap();
        assert!(snapshot_data.next_chunk_position.is_none());

        let space_dumps = snapshot_data.space_dumps;
        let n_internal_spaces = ClusterwideTable::values().len();
        assert_eq!(space_dumps.len(), n_internal_spaces);

        for space_dump in &space_dumps {
            match space_dump.space_id {
                s if s == ClusterwideTable::Instance.id() => {
                    let [instance]: [Instance; 1] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(instance, i);
                }

                s if s == ClusterwideTable::Address.id() => {
                    let addrs: [(i32, String); 2] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(
                        addrs,
                        [(1, "google.com".to_string()), (2, "ya.ru".to_string())]
                    );
                }

                s if s == ClusterwideTable::Property.id() => {
                    let [property]: [(String, String); 1] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(property, ("foo".to_owned(), "bar".to_owned()));
                }

                s if s == ClusterwideTable::Replicaset.id() => {
                    let [replicaset]: [Replicaset; 1] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(replicaset, r);
                }

                s_id => {
                    dbg!(s_id);
                    #[rustfmt::skip]
                    assert!(ClusterwideTable::all_tables().iter().any(|s| s.id() == s_id));
                    let []: [(); 0] = Decode::decode(space_dump.tuples.as_ref()).unwrap();
                }
            }
        }
    }

    #[::tarantool::test]
    fn apply_snapshot_data() {
        let storage = Clusterwide::for_tests();

        let mut data = SnapshotData {
            schema_version: 0,
            space_dumps: vec![],
            next_chunk_position: None,
        };

        let i = Instance {
            raft_id: 1,
            instance_id: "i".into(),
            ..Instance::default()
        };
        let tuples = [&i].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space_id: ClusterwideTable::Instance.into(),
            tuples,
        });

        let tuples = [(1, "google.com"), (2, "ya.ru")].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space_id: ClusterwideTable::Address.into(),
            tuples,
        });

        let tuples = [("foo", "bar")].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space_id: ClusterwideTable::Property.into(),
            tuples,
        });

        let r = Replicaset::for_tests();
        let tuples = [&r].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space_id: ClusterwideTable::Replicaset.into(),
            tuples,
        });

        storage.for_each_space(|s| s.truncate()).unwrap();
        if let Err(e) =
            transaction(|| -> traft::Result<()> { storage.apply_snapshot_data(&data, true) })
        {
            println!("{e}");
            panic!();
        }

        assert_eq!(storage.instances.space.len().unwrap(), 1);
        let instance = storage.instances.get(&1).unwrap();
        assert_eq!(instance, i);

        assert_eq!(storage.peer_addresses.space.len().unwrap(), 2);
        let addr = storage.peer_addresses.get(1).unwrap().unwrap();
        assert_eq!(addr, "google.com");
        let addr = storage.peer_addresses.get(2).unwrap().unwrap();
        assert_eq!(addr, "ya.ru");

        assert_eq!(storage.replicasets.space.len().unwrap(), 1);
        let replicaset = storage.replicasets.get("r1").unwrap().unwrap();
        assert_eq!(replicaset, r);
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
        use tarantool::util::NumOrStr;

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
        picodata_index_parts: &[Part],
        tarantool_index_parts: &[Part],
        space_fields: &[tarantool::space::Field],
    ) {
        assert_eq!(picodata_index_parts.len(), tarantool_index_parts.len());

        for (pd_part, tt_part) in picodata_index_parts.iter().zip(tarantool_index_parts) {
            if let (Some(pd_type), Some(tt_type)) = (pd_part.r#type, tt_part.r#type) {
                assert_eq!(pd_type, tt_type);
            } else {
                // Ignore
            }

            assert_eq!(pd_part.collation, tt_part.collation);
            assert_eq!(pd_part.is_nullable, tt_part.is_nullable);
            assert_eq!(pd_part.path, tt_part.path);

            let pd_field_index = get_field_index(pd_part, space_fields);
            let tt_field_index = get_field_index(tt_part, space_fields);
            assert_eq!(pd_field_index, tt_field_index);
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
            for mut index_def in index_definitions {
                assert_eq!(index_def.table_id, sys_table.id());

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
                    &parts_as_known_by_picodata,
                    &parts_as_known_by_tarantool,
                    &sys_table.format(),
                );

                // Check "_index" agrees with `ClusterwideTable.index_definitions`
                assert_eq!(index_def.to_index_metadata(), tt_index_def);
            }
        }
    }
}
