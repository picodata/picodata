use ::tarantool::auth::AuthDef;
use ::tarantool::error::Error as TntError;
use ::tarantool::fiber;
use ::tarantool::index::{Index, IndexId, IndexIterator, IteratorType};
use ::tarantool::msgpack::{ArrayWriter, ValueIter};
use ::tarantool::read_view::ReadView;
use ::tarantool::read_view::ReadViewIterator;
use ::tarantool::space::UpdateOps;
use ::tarantool::space::{FieldType, Space, SpaceId, SpaceType, SystemSpace};
use ::tarantool::time::Instant;
use ::tarantool::tlua::{self, LuaError};
use ::tarantool::tuple::KeyDef;
use ::tarantool::tuple::{Decode, DecodeOwned, Encode};
use ::tarantool::tuple::{RawBytes, ToTupleBuffer, Tuple, TupleBuffer};

use crate::failure_domain as fd;
use crate::instance::{self, grade, Instance};
use crate::replicaset::{Replicaset, ReplicasetId};
use crate::schema::Distribution;
use crate::schema::{IndexDef, SpaceDef};
use crate::schema::{PrivilegeDef, RoleDef, UserDef, UserId};
use crate::sql::pgproto::DEFAULT_MAX_PG_PORTALS;
use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::op::Ddl;
use crate::traft::RaftEntryId;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::traft::Result;
use crate::util::Uppercase;
use crate::warn_or_panic;

use std::borrow::Cow;
use std::cell::UnsafeCell;
use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};
use std::iter::Peekable;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

macro_rules! define_clusterwide_spaces {
    (
        $(#[$Clusterwide_meta:meta])*
        pub struct $Clusterwide:ident {
            pub #space_name_lower: #space_name_upper,
            $(
                $(#[$Clusterwide_extra_field_meta:meta])*
                pub $Clusterwide_extra_field:ident: $Clusterwide_extra_field_type:ty,
            )*
        }

        $(#[$ClusterwideSpace_meta:meta])*
        pub enum $ClusterwideSpace:ident {
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
        // ClusterwideSpace
        ::tarantool::define_str_enum! {
            $(#[$ClusterwideSpace_meta])*
            pub enum $ClusterwideSpace {
                $(
                    $(#[$cw_field_meta])*
                    $cw_space_var = $cw_space_name,
                )+
            }
        }

        impl $ClusterwideSpace {
            /// Id of the corrseponding system global space.
            pub const fn id(&self) -> SpaceId {
                match self {
                    $( Self::$cw_space_var => $cw_space_id, )+
                }
            }

            /// Name of the corrseponding system global space.
            pub const fn name(&self) -> &'static str {
                match self {
                    $( Self::$cw_space_var => $cw_space_name, )+
                }
            }

            /// A slice of all possible variants of `Self`.
            pub const fn all_spaces() -> &'static [Self] {
                &[ $( Self::$cw_space_var, )+ ]
            }
        }

        impl TryFrom<SpaceId> for $ClusterwideSpace {
            type Error = SpaceId;

            #[inline(always)]
            fn try_from(id: SpaceId) -> ::std::result::Result<$ClusterwideSpace, Self::Error> {
                match id {
                    $( $cw_space_id => Ok(Self::$cw_space_var), )+
                    _ => Err(id),
                }
            }
        }

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
            #[inline(always)]
            pub fn new() -> tarantool::Result<Self> {
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

            impl TClusterwideSpace for $space_struct {
                const SPACE_NAME: &'static str = $cw_space_name;
                const SPACE_ID: SpaceId = $cw_space_id;
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

define_clusterwide_spaces! {
    pub struct Clusterwide {
        pub #space_name_lower: #space_name_upper,
        pub snapshot_cache: Rc<SnapshotCache>,
    }

    /// An enumeration of builtin cluster-wide spaces.
    ///
    /// Use [`Self::id`] to get [`SpaceId`].
    /// Use [`Self::name`] to get space name.
    pub enum ClusterwideSpace {
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
        Address = 514, "_pico_peer_address" => {
            Clusterwide::peer_addresses;

            /// A struct for accessing storage of peer addresses.
            pub struct PeerAddresses {
                space: Space,
                #[primary]
                index: Index => "raft_id",
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
        Space = 512, "_pico_space" => {
            Clusterwide::spaces;

            /// A struct for accessing definitions of all the user-defined spaces.
            pub struct Spaces {
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
        Role = 522, "_pico_role" => {
            Clusterwide::roles;

            /// A struct for accessing info of all the user-defined roles.
            pub struct Roles {
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
                STORAGE = Some(Self::new()?);
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

    fn open_read_view(&self, entry_id: RaftEntryId) -> Result<SnapshotReadView> {
        let mut space_indexes = Vec::with_capacity(32);
        for space in ClusterwideSpace::all_spaces() {
            space_indexes.push((space.id(), 0));
        }
        // ClusterwideSpace::all_spaces is not guaranteed to iterate in order
        // of ascending space ids, so we sort it explicitly.
        // TODO: we should probably fix the enum so that we don't have to sort.
        space_indexes.sort_unstable();

        for space_def in self.spaces.iter()? {
            if !matches!(space_def.distribution, Distribution::Global) {
                continue;
            }
            space_indexes.push((space_def.id, 0));
        }

        let read_view = ReadView::for_space_indexes(space_indexes)?;
        Ok(SnapshotReadView {
            entry_id,
            time_opened: fiber::clock(),
            unfinished_iterators: Default::default(),
            read_view: Some(read_view),
            ref_count: 0,
            sole_chunk: Default::default(),
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
        let read_view = rv
            .read_view
            .as_ref()
            .expect("shouldn't be None if this function is called");
        let global_spaces = read_view.space_indexes().unwrap();
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
                let it = read_view.iter_all(space_id, 0)?.map(Iterator::peekable);
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
                .global_space_name(space_id)
                .expect("this id is from _pico_space");
            tlog!(
                Info,
                "generated snapshot chunk for clusterwide space '{space_name}': {} tuples [{} bytes]",
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
            // Only the first chunk contains the schema_version, after that
            // there's no need for it.
            schema_version: u64::MAX,
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

        if rv.sole_chunk.is_some() || rv.read_view.is_none() {
            warn_or_panic!(
                "next chunk was requested for a single chunk snapshot for entry {entry_id}"
            );
            return Err(Error::other(format!(
                "read view not available for {entry_id}: single chunk"
            )));
        }

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

        // Find the latest available snapshot which was opened after the last
        // raft log compaction.
        let mut snapshots = self.snapshot_cache.read_views().iter_mut().rev();
        let snapshot = snapshots.find(|(entry_id, _)| entry_id.index >= compacted_index);

        if let Some((_, rv)) = snapshot {
            if let Some(sole_chunk) = &rv.sole_chunk {
                #[rustfmt::skip]
                debug_assert_eq!(rv.ref_count, 0, "single chunk snapshots don't need reference counting");
                tlog!(Debug, "returning a cached snapshot chunk"; "entry_id" => %rv.entry_id);
                return Ok((sole_chunk.clone(), rv.entry_id));
            } else {
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
        let mut snapshot_data = self.next_snapshot_data_chunk_impl(&mut rv, position)?;
        snapshot_data.schema_version = self.properties.global_schema_version()?;

        if snapshot_data.next_chunk_position.is_none() {
            // If snapshot fits into a single chunk, cache it.
            rv.sole_chunk = Some(snapshot_data.clone());
            drop(rv.read_view.take());
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

    fn global_space_name(&self, id: SpaceId) -> Result<Cow<'static, str>> {
        if let Ok(space) = ClusterwideSpace::try_from(id) {
            Ok(space.name().into())
        } else {
            let Some(space_def) = self.spaces.get(id)? else {
                return Err(Error::other(format!("global space #{id} not found")));
            };
            Ok(space_def.name.into())
        }
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
        let mut old_space_versions = HashMap::new();
        let mut old_user_versions = HashMap::new();
        let mut old_role_versions = HashMap::new();
        let mut old_priv_versions = HashMap::new();

        for def in self.spaces.iter()? {
            old_space_versions.insert(def.id, def.schema_version);
        }
        for def in self.users.iter()? {
            old_user_versions.insert(def.id, def.schema_version);
        }
        for def in self.roles.iter()? {
            old_role_versions.insert(def.id, def.schema_version);
        }
        for def in self.privileges.iter()? {
            let schema_version = def.schema_version;
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
                // (_pico_space, _pico_user, etc.), so that we can apply the
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

            self.apply_schema_changes_on_master(self.spaces.iter()?, &old_space_versions)?;
            // TODO: secondary indexes
            self.apply_schema_changes_on_master(self.users.iter()?, &old_user_versions)?;
            self.apply_schema_changes_on_master(self.roles.iter()?, &old_role_versions)?;
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
            ClusterwideSpace::Space.id(),
            ClusterwideSpace::Index.id(),
            ClusterwideSpace::User.id(),
            ClusterwideSpace::Role.id(),
            ClusterwideSpace::Privilege.id(),
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
    /// ([`SpaceDef`], [`UserDef`]).
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
            if let Some(&v_old) = old_versions.get(&key) {
                let v_new = def.schema_version();
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
        static mut KEY_DEF: Option<HashMap<(ClusterwideSpace, IndexId), Rc<KeyDef>>> = None;
        let key_defs = unsafe { KEY_DEF.get_or_insert_with(HashMap::new) };
        if let Ok(sys_space_id) = ClusterwideSpace::try_from(space_id) {
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
        static mut KEY_DEF: Option<HashMap<(ClusterwideSpace, IndexId), Rc<KeyDef>>> = None;
        let key_defs = unsafe { KEY_DEF.get_or_insert_with(HashMap::new) };
        if let Ok(sys_space_id) = ClusterwideSpace::try_from(space_id) {
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

    pub(crate) fn insert(
        &self,
        space_id: SpaceId,
        tuple: &TupleBuffer,
    ) -> tarantool::Result<Tuple> {
        let space = space_by_id_unchecked(space_id);
        let res = space.insert(tuple)?;
        Ok(res)
    }

    pub(crate) fn replace(
        &self,
        space_id: SpaceId,
        tuple: &TupleBuffer,
    ) -> tarantool::Result<Tuple> {
        let space = space_by_id_unchecked(space_id);
        let res = space.replace(tuple)?;
        Ok(res)
    }

    pub(crate) fn update(
        &self,
        space_id: SpaceId,
        key: &TupleBuffer,
        ops: &[TupleBuffer],
    ) -> tarantool::Result<Option<Tuple>> {
        let space = space_by_id_unchecked(space_id);
        let res = space.update(key, ops)?;
        Ok(res)
    }

    pub(crate) fn delete(
        &self,
        space_id: SpaceId,
        key: &TupleBuffer,
    ) -> tarantool::Result<Option<Tuple>> {
        let space = space_by_id_unchecked(space_id);
        let res = space.delete(key)?;
        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////
// ClusterwideSpace
////////////////////////////////////////////////////////////////////////////////

impl ClusterwideSpace {
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
pub trait TClusterwideSpace {
    const SPACE_NAME: &'static str;
    const SPACE_ID: SpaceId;
    const INDEX_NAMES: &'static [&'static str];
}

impl From<ClusterwideSpace> for SpaceId {
    #[inline(always)]
    fn from(space: ClusterwideSpace) -> SpaceId {
        space.id()
    }
}

////////////////////////////////////////////////////////////////////////////////
// PropertyName
////////////////////////////////////////////////////////////////////////////////

::tarantool::define_str_enum! {
    /// An enumeration of [`ClusterwideSpace::Property`] key names.
    pub enum PropertyName {
        ReplicationFactor = "replication_factor",
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

        /// Number of seconds to wait before automatically changing an
        /// unresponsive instance's grade to Offline.
        AutoOfflineTimeout = "auto_offline_timeout",

        /// Maximum number of seconds to wait before sending another heartbeat
        /// to an unresponsive instance.
        MaxHeartbeatPeriod = "max_heartbeat_period",

        /// PG portal storage size.
        MaxPgPortals = "max_pg_portals",

        /// Raft snapshot will be sent out in chunks not bigger than this threshold.
        /// Note: actual snapshot size may exceed this threshold. In most cases
        /// it will just add a couple of dozen metadata bytes. But in extreme
        /// cases if there's a tuple larger than this threshold, it will be sent
        /// in one piece whatever size it has. Please don't store tuples of size
        /// greater than this.
        SnapshotChunkMaxSize = "snapshot_chunk_max_size",

        /// Snapshot read views with live reference counts will be forcefully
        /// closed after this number of seconds. This is necessary if followers
        /// do not properly finalize the snapshot application.
        // NOTE: maybe we should instead track the instance/raft ids of
        // followers which requested the snapshots and automatically close the
        // read views if the corresponding instances are *deteremined* to not
        // need them anymore. Or maybe timeouts is the better way..
        SnapshotReadViewCloseTimeout = "snapshot_read_view_close_timeout",
    }
}

////////////////////////////////////////////////////////////////////////////////
// Properties
////////////////////////////////////////////////////////////////////////////////

pub const DEFAULT_PASSWORD_MIN_LENGTH: usize = 8;
pub const DEFAULT_AUTO_OFFLINE_TIMEOUT: f64 = 5.0;
pub const DEFAULT_MAX_HEARTBEAT_PERIOD: f64 = 5.0;
pub const DEFAULT_SNAPSHOT_CHUNK_MAX_SIZE: usize = 16 * 1024 * 1024;
pub const DEFAULT_SNAPSHOT_READ_VIEW_CLOSE_TIMEOUT: f64 = (24 * 3600) as _;

impl Properties {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .id(Self::SPACE_ID)
            .space_type(SpaceType::DataLocal)
            .field(("key", FieldType::String))
            .field(("value", FieldType::Any))
            .if_not_exists(true)
            .create()?;

        let index = space
            .index_builder("key")
            .unique(true)
            .part("key")
            .if_not_exists(true)
            .create()?;

        Ok(Self { space, index })
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
    pub fn vshard_bootstrapped(&self) -> tarantool::Result<bool> {
        Ok(self
            .get(PropertyName::VshardBootstrapped)?
            .unwrap_or_default())
    }

    #[inline]
    pub fn replication_factor(&self) -> tarantool::Result<usize> {
        let res = self
            .get(PropertyName::ReplicationFactor)?
            .expect("replication_factor must be set at boot");
        Ok(res)
    }

    #[inline]
    pub fn password_min_length(&self) -> tarantool::Result<usize> {
        let res = self
            .get(PropertyName::PasswordMinLength)?
            .unwrap_or_default();
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
            .unwrap_or_default();
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
        let space = Space::builder(Self::SPACE_NAME)
            .id(Self::SPACE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Replicaset::format())
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

    #[allow(unused)]
    #[inline]
    pub fn get(&self, replicaset_id: &str) -> tarantool::Result<Option<Replicaset>> {
        match self.space.get(&[replicaset_id])? {
            Some(tuple) => tuple.decode().map(Some),
            None => Ok(None),
        }
    }
}

impl ToEntryIter for Replicasets {
    type Entry = Replicaset;

    #[inline(always)]
    fn index_iter(&self) -> Result<IndexIterator> {
        Ok(self.space.select(IteratorType::All, &())?)
    }
}

////////////////////////////////////////////////////////////////////////////////
// PeerAddresses
////////////////////////////////////////////////////////////////////////////////

impl PeerAddresses {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .id(Self::SPACE_ID)
            .space_type(SpaceType::DataLocal)
            .field(("raft_id", FieldType::Unsigned))
            .field(("address", FieldType::String))
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
    fn index_iter(&self) -> Result<IndexIterator> {
        Ok(self.space.select(IteratorType::All, &())?)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Instance
////////////////////////////////////////////////////////////////////////////////

impl Instances {
    pub fn new() -> tarantool::Result<Self> {
        let space_instances = Space::builder(Self::SPACE_NAME)
            .id(Self::SPACE_ID)
            .space_type(SpaceType::DataLocal)
            .format(instance_format())
            .if_not_exists(true)
            .create()?;

        let index_instance_id = space_instances
            .index_builder("instance_id")
            .unique(true)
            .part(instance_field::InstanceId)
            .if_not_exists(true)
            .create()?;

        let index_raft_id = space_instances
            .index_builder("raft_id")
            .unique(true)
            .part(instance_field::RaftId)
            .if_not_exists(true)
            .create()?;

        let index_replicaset_id = space_instances
            .index_builder("replicaset_id")
            .unique(false)
            .part(instance_field::ReplicasetId)
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space: space_instances,
            index_instance_id,
            index_raft_id,
            index_replicaset_id,
        })
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

    /// Checks if an instance with `id` (see trait [`InstanceId`]) is present.
    #[inline]
    pub fn contains(&self, id: &impl InstanceId) -> Result<bool> {
        match id.find_in(self) {
            Ok(_) => Ok(true),
            Err(Error::NoInstanceWithInstanceId(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Finds an instance by `id` (see `InstanceId`) and return a single field
    /// specified by `F` (see `InstanceFieldDef` & `instance_field` module).
    #[inline(always)]
    pub fn field<F>(&self, id: &impl InstanceId) -> Result<F::Type>
    where
        F: InstanceFieldDef,
    {
        let tuple = id.find_in(self)?;
        let res = F::get_in(&tuple)?;
        Ok(res)
    }

    /// Returns an iterator over all instances. Items of the iterator are
    /// specified by `F` (see `InstanceFieldDef` & `instance_field` module).
    #[inline(always)]
    pub fn instances_fields<F>(&self) -> Result<InstancesFields<F>>
    where
        F: InstanceFieldDef,
    {
        let iter = self.space.select(IteratorType::All, &())?;
        Ok(InstancesFields::new(iter))
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

    pub fn replicaset_fields<T>(
        &self,
        replicaset_id: &ReplicasetId,
    ) -> tarantool::Result<Vec<T::Type>>
    where
        T: InstanceFieldDef,
    {
        self.index_replicaset_id
            .select(IteratorType::Eq, &[replicaset_id])?
            .map(|tuple| T::get_in(&tuple))
            .collect()
    }

    pub fn max_raft_id(&self) -> tarantool::Result<RaftId> {
        match self.index_raft_id.max(&())? {
            None => Ok(0),
            Some(tuple) => instance_field::RaftId::get_in(&tuple),
        }
    }

    pub fn failure_domain_names(&self) -> Result<HashSet<Uppercase>> {
        Ok(self
            .instances_fields::<instance_field::FailureDomain>()?
            .flat_map(|fd| fd.names().cloned().collect::<Vec<_>>())
            .collect())
    }
}

impl ToEntryIter for Instances {
    type Entry = Instance;

    #[inline(always)]
    fn index_iter(&self) -> Result<IndexIterator> {
        Ok(self.space.select(IteratorType::All, &())?)
    }
}

////////////////////////////////////////////////////////////////////////////////
// InstanceField
////////////////////////////////////////////////////////////////////////////////

macro_rules! define_instance_fields {
    ($($field:ident: $ty:ty = ($name:literal, $tt_ty:path))+) => {
        ::tarantool::define_str_enum! {
            /// An enumeration of raft_space field names
            pub enum InstanceField {
                $($field = $name,)+
            }
        }

        pub mod instance_field {
            use super::*;
            $(
                /// Helper struct that represents
                #[doc = stringify!($name)]
                /// field of [`Instance`].
                ///
                /// It's rust type is
                #[doc = concat!("`", stringify!($ty), "`")]
                /// and it's tarantool type is
                #[doc = concat!("`", stringify!($tt_ty), "`")]
                ///
                /// [`Instance`]: crate::instance::Instance
                pub struct $field;

                impl InstanceFieldDef for $field {
                    type Type = $ty;

                    fn get_in(tuple: &Tuple) -> tarantool::Result<Self::Type> {
                        Ok(tuple.try_get($name)?.expect("instance fields aren't nullable"))
                    }
                }

                impl From<$field> for ::tarantool::index::Part {
                    #[inline(always)]
                    fn from(_: $field) -> ::tarantool::index::Part {
                        $name.into()
                    }
                }

                impl From<$field> for ::tarantool::space::Field {
                    #[inline(always)]
                    fn from(_: $field) -> ::tarantool::space::Field {
                        ($name, $tt_ty).into()
                    }
                }

                impl ::tarantool::tuple::TupleIndex for $field {
                    #[inline(always)]
                    fn get_field<'a, T>(self, tuple: &'a Tuple) -> ::tarantool::Result<Option<T>>
                    where
                        T: ::tarantool::tuple::Decode<'a>,
                    {
                        $name.get_field(tuple)
                    }
                }
            )+
        }

        fn instance_format() -> Vec<::tarantool::space::Field> {
            vec![
                $( ::tarantool::space::Field::from(($name, $tt_ty)), )+
            ]
        }
    };
}

define_instance_fields! {
    InstanceId     : instance::InstanceId = ("instance_id",     FieldType::String)
    InstanceUuid   : String               = ("instance_uuid",   FieldType::String)
    RaftId         : traft::RaftId        = ("raft_id",         FieldType::Unsigned)
    ReplicasetId   : String               = ("replicaset_id",   FieldType::String)
    ReplicasetUuid : String               = ("replicaset_uuid", FieldType::String)
    CurrentGrade   : grade::CurrentGrade  = ("current_grade",   FieldType::Array)
    TargetGrade    : grade::TargetGrade   = ("target_grade",    FieldType::Array)
    FailureDomain  : fd::FailureDomain    = ("failure_domain",  FieldType::Map)
}

impl tarantool::tuple::TupleIndex for InstanceField {
    fn get_field<'a, T>(self, tuple: &'a Tuple) -> tarantool::Result<Option<T>>
    where
        T: tarantool::tuple::Decode<'a>,
    {
        self.as_str().get_field(tuple)
    }
}

/// A helper trait for type-safe and efficient access to a Instance's fields
/// without deserializing the whole tuple.
///
/// This trait contains information needed to define and use a given tuple field.
pub trait InstanceFieldDef {
    /// Rust type of the field.
    ///
    /// Used when decoding the field.
    type Type: tarantool::tuple::DecodeOwned;

    /// Get the field in `tuple`.
    fn get_in(tuple: &Tuple) -> tarantool::Result<Self::Type>;
}

macro_rules! define_instance_field_def_for_tuples {
    () => {};
    ($h:ident $($t:ident)*) => {
        impl<$h, $($t),*> InstanceFieldDef for ($h, $($t),*)
        where
            $h: InstanceFieldDef,
            $h::Type: serde::de::DeserializeOwned,
            $(
                $t: InstanceFieldDef,
                $t::Type: serde::de::DeserializeOwned,
            )*
        {
            type Type = ($h::Type, $($t::Type),*);

            fn get_in(tuple: &Tuple) -> tarantool::Result<Self::Type> {
                Ok(($h::get_in(&tuple)?, $($t::get_in(&tuple)?,)*))
            }
        }

        define_instance_field_def_for_tuples!{ $($t)* }
    };
}

define_instance_field_def_for_tuples! {
    T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13 T14 T15
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
// InstancesFields
////////////////////////////////////////////////////////////////////////////////

pub struct InstancesFields<F> {
    iter: IndexIterator,
    marker: PhantomData<F>,
}

impl<F> InstancesFields<F> {
    fn new(iter: IndexIterator) -> Self {
        Self {
            iter,
            marker: PhantomData,
        }
    }
}

impl<F> Iterator for InstancesFields<F>
where
    F: InstanceFieldDef,
{
    type Item = F::Type;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iter.next().as_ref().map(F::get_in);
        res.map(|res| res.expect("instance should decode correctly"))
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

    fn index_iter(&self) -> Result<IndexIterator>;

    #[inline(always)]
    fn iter(&self) -> Result<EntryIter<Self::Entry>> {
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

    /// [`fiber::clock()`] value at the moment snapshot read view was opened.
    /// Can be used to close stale read views with dangling references, which
    /// may occur if followers drop off during chunkwise snapshot application.
    time_opened: Instant,

    /// The read view onto the global spaces. If the snapshot fits into a single
    /// chunk, this is set to `None` so that the read view is immidiately
    /// closed.
    read_view: Option<ReadView>,

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

    /// If the snapshot fits into a single chunk, we cache it so that we don't
    /// have to generated it again, if someone requests a snapshot for the same
    /// entry id. If it doesn't fit into a single chunk we don't store it,
    /// because of the problems with unfinished iterators.
    sole_chunk: Option<SnapshotData>,
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
// Spaces
////////////////////////////////////////////////////////////////////////////////

impl Spaces {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .id(Self::SPACE_ID)
            .space_type(SpaceType::DataLocal)
            .field(("id", FieldType::Unsigned))
            .field(("name", FieldType::String))
            .field(("distribution", FieldType::Array))
            .field(("format", FieldType::Array))
            .field(("schema_version", FieldType::Unsigned))
            .field(("operable", FieldType::Boolean))
            .field(("engine", FieldType::String))
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

    #[inline]
    pub fn get(&self, id: SpaceId) -> tarantool::Result<Option<SpaceDef>> {
        match self.space.get(&[id])? {
            Some(tuple) => tuple.decode().map(Some),
            None => Ok(None),
        }
    }

    #[inline]
    pub fn put(&self, space_def: &SpaceDef) -> tarantool::Result<()> {
        self.space.replace(space_def)?;
        Ok(())
    }

    #[inline]
    pub fn insert(&self, space_def: &SpaceDef) -> tarantool::Result<()> {
        self.space.insert(space_def)?;
        Ok(())
    }

    #[inline]
    pub fn update_operable(&self, id: SpaceId, operable: bool) -> tarantool::Result<()> {
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(SpaceDef::FIELD_OPERABLE, operable)?;
        self.space.update(&[id], ops)?;
        Ok(())
    }

    #[inline]
    pub fn delete(&self, id: SpaceId) -> tarantool::Result<Option<Tuple>> {
        self.space.delete(&[id])
    }

    #[inline]
    pub fn by_name(&self, name: &str) -> tarantool::Result<Option<SpaceDef>> {
        match self.index_name.get(&[name])? {
            Some(tuple) => tuple.decode().map(Some),
            None => Ok(None),
        }
    }
}

impl ToEntryIter for Spaces {
    type Entry = SpaceDef;

    #[inline(always)]
    fn index_iter(&self) -> Result<IndexIterator> {
        Ok(self.space.select(IteratorType::All, &())?)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Indexes
////////////////////////////////////////////////////////////////////////////////

impl Indexes {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .id(Self::SPACE_ID)
            .space_type(SpaceType::DataLocal)
            .field(("space_id", FieldType::Unsigned))
            .field(("id", FieldType::Unsigned))
            .field(("name", FieldType::String))
            .field(("local", FieldType::Boolean))
            .field(("parts", FieldType::Array))
            .field(("schema_version", FieldType::Unsigned))
            .field(("operable", FieldType::Boolean))
            .field(("unique", FieldType::Boolean))
            .if_not_exists(true)
            .create()?;

        let index_id = space
            .index_builder("id")
            .unique(true)
            .part("space_id")
            .part("id")
            .if_not_exists(true)
            .create()?;

        let index_name = space
            .index_builder("name")
            .unique(true)
            .part("space_id")
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
        match self.space.get(&(space_id, index_id))? {
            Some(tuple) => tuple.decode().map(Some),
            None => Ok(None),
        }
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
        match self.index_name.get(&(space_id, name))? {
            Some(tuple) => tuple.decode().map(Some),
            None => Ok(None),
        }
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
    fn index_iter(&self) -> Result<IndexIterator> {
        Ok(self.space.select(IteratorType::All, &())?)
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
    storage.spaces.update_operable(space_id, operable)?;
    let iter = storage.indexes.by_space_id(space_id)?;
    for index in iter {
        storage
            .indexes
            .update_operable(index.space_id, index.id, operable)?;
    }
    Ok(())
}

/// Deletes the picodata internal metadata for a space with id `space_id`.
///
/// This function is called when applying the different ddl operations.
pub fn ddl_meta_drop_space(storage: &Clusterwide, space_id: SpaceId) -> traft::Result<()> {
    if let Some(space_name) = storage.spaces.get(space_id)?.map(|s| s.name) {
        storage
            .privileges
            .delete_all_by_object("space", &space_name)?;
    }
    let iter = storage.indexes.by_space_id(space_id)?;
    for index in iter {
        storage.indexes.delete(index.space_id, index.id)?;
    }
    storage.spaces.delete(space_id)?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// ddl
////////////////////////////////////////////////////////////////////////////////

pub fn ddl_abort_on_master(ddl: &Ddl, version: u64) -> traft::Result<()> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let sys_space = Space::from(SystemSpace::Space);
    let sys_index = Space::from(SystemSpace::Index);

    match *ddl {
        Ddl::CreateSpace { id, .. } => {
            sys_index.delete(&[id, 1])?;
            sys_index.delete(&[id, 0])?;
            sys_space.delete(&[id])?;
            set_local_schema_version(version)?;
        }

        Ddl::DropSpace { .. } => {
            // Actual drop happens only on commit, so there's nothing to abort.
        }

        _ => {
            todo!();
        }
    }

    Ok(())
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
        .spaces
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
        let space = Space::builder(Self::SPACE_NAME)
            .id(Self::SPACE_ID)
            .space_type(SpaceType::DataLocal)
            .field(("id", FieldType::Unsigned))
            .field(("name", FieldType::String))
            .field(("schema_version", FieldType::Unsigned))
            .field(("auth", FieldType::Array))
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

    #[inline]
    pub fn by_id(&self, user_id: UserId) -> tarantool::Result<Option<UserDef>> {
        let tuple = self.space.get(&[user_id])?;
        let mut res = None;
        if let Some(tuple) = tuple {
            res = Some(tuple.decode()?);
        }
        Ok(res)
    }

    #[inline]
    pub fn by_name(&self, user_name: &str) -> tarantool::Result<Option<UserDef>> {
        let tuple = self.index_name.get(&[user_name])?;
        let mut res = None;
        if let Some(tuple) = tuple {
            res = Some(tuple.decode()?);
        }
        Ok(res)
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
    fn index_iter(&self) -> Result<IndexIterator> {
        Ok(self.space.select(IteratorType::All, &())?)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Roles
////////////////////////////////////////////////////////////////////////////////

impl Roles {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .id(Self::SPACE_ID)
            .space_type(SpaceType::DataLocal)
            .field(("id", FieldType::Unsigned))
            .field(("name", FieldType::String))
            .field(("schema_version", FieldType::Unsigned))
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

    #[inline]
    pub fn by_id(&self, role_id: UserId) -> tarantool::Result<Option<RoleDef>> {
        let tuple = self.space.get(&[role_id])?;
        let mut res = None;
        if let Some(tuple) = tuple {
            res = Some(tuple.decode()?);
        }
        Ok(res)
    }

    #[inline]
    pub fn by_name(&self, role_name: &str) -> tarantool::Result<Option<RoleDef>> {
        let tuple = self.index_name.get(&[role_name])?;
        let mut res = None;
        if let Some(tuple) = tuple {
            res = Some(tuple.decode()?);
        }
        Ok(res)
    }

    #[inline]
    pub fn max_role_id(&self) -> tarantool::Result<Option<UserId>> {
        match self.index_id.max(&())? {
            Some(role) => Ok(role.get(0).unwrap()),
            None => Ok(None),
        }
    }

    #[inline]
    pub fn replace(&self, role_def: &RoleDef) -> tarantool::Result<()> {
        self.space.replace(role_def)?;
        Ok(())
    }

    #[inline]
    pub fn insert(&self, role_def: &RoleDef) -> tarantool::Result<()> {
        self.space.insert(role_def)?;
        Ok(())
    }

    #[inline]
    pub fn delete(&self, role_id: UserId) -> tarantool::Result<()> {
        self.space.delete(&[role_id])?;
        Ok(())
    }
}

impl ToEntryIter for Roles {
    type Entry = RoleDef;

    #[inline(always)]
    fn index_iter(&self) -> Result<IndexIterator> {
        Ok(self.space.select(IteratorType::All, &())?)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Privileges
////////////////////////////////////////////////////////////////////////////////

impl Privileges {
    pub fn new() -> tarantool::Result<Self> {
        let space = Space::builder(Self::SPACE_NAME)
            .id(Self::SPACE_ID)
            .space_type(SpaceType::DataLocal)
            .field(("grantor_id", FieldType::Unsigned))
            .field(("grantee_id", FieldType::Unsigned))
            .field(("object_type", FieldType::String))
            .field(("object_name", FieldType::String))
            .field(("privilege", FieldType::String))
            .field(("schema_version", FieldType::Unsigned))
            .if_not_exists(true)
            .create()?;

        let primary_key = space
            .index_builder("primary")
            .unique(true)
            .parts(["grantee_id", "object_type", "object_name", "privilege"])
            .if_not_exists(true)
            .create()?;

        let object_idx = space
            .index_builder("object")
            .unique(false)
            .part("object_type")
            .part("object_name")
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
        object_name: &str,
    ) -> tarantool::Result<Option<PrivilegeDef>> {
        let tuple = self.space.get(&(grantee_id, object_type, object_name))?;
        let mut res = None;
        if let Some(tuple) = tuple {
            res = Some(tuple.decode()?);
        }
        Ok(res)
    }

    #[inline(always)]
    pub fn by_grantee_id(&self, grantee_id: UserId) -> tarantool::Result<EntryIter<PrivilegeDef>> {
        let iter = self.primary_key.select(IteratorType::Eq, &[grantee_id])?;
        Ok(EntryIter::new(iter))
    }

    #[inline(always)]
    pub fn by_object(
        &self,
        object_type: &str,
        object_name: &str,
    ) -> tarantool::Result<EntryIter<PrivilegeDef>> {
        let iter = self
            .object_idx
            .select(IteratorType::Eq, &(object_type, object_name))?;
        Ok(EntryIter::new(iter))
    }

    #[inline(always)]
    pub fn replace(&self, priv_def: &PrivilegeDef) -> tarantool::Result<()> {
        self.space.replace(priv_def)?;
        Ok(())
    }

    #[inline(always)]
    pub fn insert(&self, priv_def: &PrivilegeDef) -> tarantool::Result<()> {
        self.space.insert(priv_def)?;
        Ok(())
    }

    #[inline(always)]
    pub fn delete(
        &self,
        grantee_id: UserId,
        object_type: &str,
        object_name: &str,
        privilege: &str,
    ) -> tarantool::Result<()> {
        self.space
            .delete(&(grantee_id, object_type, object_name, privilege))?;
        Ok(())
    }

    /// Remove any privilege definitions granted to the given grantee.
    #[inline]
    pub fn delete_all_by_grantee_id(&self, grantee_id: UserId) -> tarantool::Result<()> {
        for priv_def in self.by_grantee_id(grantee_id)? {
            self.delete(
                priv_def.grantee_id,
                &priv_def.object_type,
                &priv_def.object_name,
                &priv_def.privilege,
            )?;
        }
        Ok(())
    }

    /// Remove any privilege definitions assigning the given role.
    #[inline]
    pub fn delete_all_by_granted_role(&self, role_name: &str) -> Result<()> {
        for priv_def in self.iter()? {
            if priv_def.privilege == "execute"
                && priv_def.object_type == "role"
                && priv_def.object_name == role_name
            {
                self.delete(
                    priv_def.grantee_id,
                    &priv_def.object_type,
                    &priv_def.object_name,
                    &priv_def.privilege,
                )?;
            }
        }
        Ok(())
    }

    /// Remove any privilege definitions granted to the given object.
    #[inline]
    pub fn delete_all_by_object(
        &self,
        object_type: &str,
        object_name: &str,
    ) -> tarantool::Result<()> {
        for priv_def in self.by_object(object_type, object_name)? {
            self.delete(
                priv_def.grantee_id,
                &priv_def.object_type,
                &priv_def.object_name,
                &priv_def.privilege,
            )?;
        }
        Ok(())
    }
}

impl ToEntryIter for Privileges {
    type Entry = PrivilegeDef;

    #[inline(always)]
    fn index_iter(&self) -> Result<IndexIterator> {
        Ok(self.space.select(IteratorType::All, &())?)
    }
}

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

impl SchemaDef for SpaceDef {
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
                "failed to create space {space_id}: {abort_reason}"
            )));
        }
        Ok(())
    }

    #[inline(always)]
    fn on_delete(space_id: &SpaceId, storage: &Clusterwide) -> traft::Result<()> {
        _ = storage;
        if let Some(abort_reason) = ddl_drop_space_on_master(*space_id)? {
            return Err(Error::other(format!(
                "failed to drop space {space_id}: {abort_reason}"
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
        acl::on_master_create_user(self)?;
        Ok(())
    }

    #[inline(always)]
    fn on_delete(user_id: &UserId, storage: &Clusterwide) -> traft::Result<()> {
        _ = storage;
        acl::on_master_drop_user(*user_id)?;
        Ok(())
    }
}

impl SchemaDef for RoleDef {
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
        acl::on_master_create_role(self)?;
        Ok(())
    }

    #[inline(always)]
    fn on_delete(user_id: &UserId, storage: &Clusterwide) -> traft::Result<()> {
        _ = storage;
        acl::on_master_drop_role(*user_id)?;
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
        self.schema_version
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

    use super::*;

    ////////////////////////////////////////////////////////////////////////////
    // acl in global storage
    ////////////////////////////////////////////////////////////////////////////

    /// Persist a user definition in the internal clusterwide storage.
    pub fn global_create_user(storage: &Clusterwide, user_def: &UserDef) -> tarantool::Result<()> {
        storage.users.insert(user_def)?;
        Ok(())
    }

    /// Remove a user definition and any entities owned by it from the internal
    /// clusterwide storage.
    pub fn global_change_user_auth(
        storage: &Clusterwide,
        user_id: UserId,
        auth: &AuthDef,
    ) -> tarantool::Result<()> {
        storage.users.update_auth(user_id, auth)?;
        Ok(())
    }

    /// Remove a user definition and any entities owned by it from the internal
    /// clusterwide storage.
    pub fn global_drop_user(storage: &Clusterwide, user_id: UserId) -> tarantool::Result<()> {
        storage.privileges.delete_all_by_grantee_id(user_id)?;
        storage.users.delete(user_id)?;
        Ok(())
    }

    /// Persist a role definition in the internal clusterwide storage.
    pub fn global_create_role(storage: &Clusterwide, role_def: &RoleDef) -> tarantool::Result<()> {
        storage.roles.insert(role_def)?;
        Ok(())
    }

    /// Remove a role definition and any entities owned by it from the internal
    /// clusterwide storage.
    pub fn global_drop_role(storage: &Clusterwide, role_id: UserId) -> Result<()> {
        storage.privileges.delete_all_by_grantee_id(role_id)?;
        if let Some(role_def) = storage.roles.by_id(role_id)? {
            // Revoke the role from any grantees.
            storage
                .privileges
                .delete_all_by_granted_role(&role_def.name)?;
            storage.roles.delete(role_id)?;
        }
        Ok(())
    }

    /// Persist a privilege definition in the internal clusterwide storage.
    pub fn global_grant_privilege(
        storage: &Clusterwide,
        priv_def: &PrivilegeDef,
    ) -> tarantool::Result<()> {
        storage.privileges.insert(priv_def)?;
        Ok(())
    }

    /// Remove a privilege definition from the internal clusterwide storage.
    pub fn global_revoke_privilege(
        storage: &Clusterwide,
        priv_def: &PrivilegeDef,
    ) -> tarantool::Result<()> {
        // FIXME: currently there's no way to revoke a default privilege
        storage.privileges.delete(
            priv_def.grantee_id,
            &priv_def.object_type,
            &priv_def.object_name,
            &priv_def.privilege,
        )?;

        Ok(())
    }

    ////////////////////////////////////////////////////////////////////////////
    // acl in local storage on replicaset leader
    ////////////////////////////////////////////////////////////////////////////

    /// Create a tarantool user. Grant it default privileges.
    pub fn on_master_create_user(user_def: &UserDef) -> tarantool::Result<()> {
        let sys_user = Space::from(SystemSpace::User);

        // This implementation was copied from box.schema.user.create excluding the
        // password hashing.
        let user_id = user_def.id;
        let euid = ::tarantool::session::euid()?;

        // Tarantool expects auth info to be a map of form `{ method: data }`,
        // and currently the simplest way to achieve this is to use a HashMap.
        let auth_map = HashMap::from([(user_def.auth.method, &user_def.auth.data)]);
        sys_user.insert(&(user_id, euid, &user_def.name, "user", auth_map, &[(); 0], 0))?;

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

    /// Drop a tarantool user and any entities (spaces, etc.) owned by it.
    pub fn on_master_drop_user(user_id: UserId) -> tarantool::Result<()> {
        let lua = ::tarantool::lua_state();
        lua.exec_with("box.schema.user.drop(...)", user_id)
            .map_err(LuaError::from)?;

        Ok(())
    }

    /// Create a tarantool role.
    pub fn on_master_create_role(role_def: &RoleDef) -> tarantool::Result<()> {
        let sys_user = Space::from(SystemSpace::User);

        // This implementation was copied from box.schema.role.create.

        // Tarantool expects auth info to be a map `{}`, and currently the simplest
        // way to achieve this is to use a HashMap.
        sys_user.insert(&(
            role_def.id,
            ::tarantool::session::euid()?,
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

    /// Grant a tarantool user some privilege defined by `priv_def`.
    pub fn on_master_grant_privilege(priv_def: &PrivilegeDef) -> tarantool::Result<()> {
        let lua = ::tarantool::lua_state();
        lua.exec_with(
            "local grantee_id, privilege, object_type, object_name = ...
            local grantee_def = box.space._user:get(grantee_id)
            if grantee_def.type == 'user' then
                box.schema.user.grant(grantee_id, privilege, object_type, object_name)
            else
                box.schema.role.grant(grantee_id, privilege, object_type, object_name)
            end",
            (
                priv_def.grantee_id,
                &priv_def.privilege,
                &priv_def.object_type,
                &priv_def.object_name,
            ),
        )
        .map_err(LuaError::from)?;

        Ok(())
    }

    /// Revoke a privilege from a tarantool user.
    pub fn on_master_revoke_privilege(priv_def: &PrivilegeDef) -> tarantool::Result<()> {
        let lua = ::tarantool::lua_state();
        lua.exec_with(
            "local grantee_id, privilege, object_type, object_name = ...
            local grantee_def = box.space._user:get(grantee_id)
            if not grantee_def then
                -- Grantee already dropped -> privileges already revoked
                return
            end
            if grantee_def.type == 'user' then
                box.schema.user.revoke(grantee_id, privilege, object_type, object_name)
            else
                box.schema.role.revoke(grantee_id, privilege, object_type, object_name)
            end",
            (
                priv_def.grantee_id,
                &priv_def.privilege,
                &priv_def.object_type,
                &priv_def.object_name,
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
    let mut res = 0;
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
// tests
////////////////////////////////////////////////////////////////////////////////

macro_rules! assert_err {
    ($expr:expr, $err:expr) => {
        assert_eq!($expr.unwrap_err().to_string(), $err)
    };
}

mod tests {
    use super::*;
    use ::tarantool::transaction::transaction;

    #[rustfmt::skip]
    #[::tarantool::test]
    fn test_storage_instances() {
        use crate::instance::grade::{CurrentGradeVariant as CGV, TargetGradeVariant as TGV};
        use crate::instance::InstanceId;
        use crate::failure_domain::FailureDomain;

        let storage = Clusterwide::new().unwrap();
        let storage_peer_addresses = PeerAddresses::new().unwrap();
        let space_peer_addresses = storage_peer_addresses.space.clone();

        let faildom = FailureDomain::from([("a", "b")]);

        for instance in vec![
            // r1
            ("i1", "i1-uuid", 1u64, "r1", "r1-uuid", (CGV::Online, 0), (TGV::Online, 0), &faildom,),
            ("i2", "i2-uuid", 2u64, "r1", "r1-uuid", (CGV::Online, 0), (TGV::Online, 0), &faildom,),
            // r2
            ("i3", "i3-uuid", 3u64, "r2", "r2-uuid", (CGV::Online, 0), (TGV::Online, 0), &faildom,),
            ("i4", "i4-uuid", 4u64, "r2", "r2-uuid", (CGV::Online, 0), (TGV::Online, 0), &faildom,),
            // r3
            ("i5", "i5-uuid", 5u64, "r3", "r3-uuid", (CGV::Online, 0), (TGV::Online, 0), &faildom,),
        ] {
            storage.space_by_name(ClusterwideSpace::Instance).unwrap().put(&instance).unwrap();
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
                ..Instance::default()
            }),
            format!(
                concat!(
                    "tarantool error:",
                    " TupleFound: Duplicate key exists",
                    " in unique index \"raft_id\"",
                    " in space \"_pico_instance\"",
                    " with old tuple",
                    r#" - ["i1", "i1-uuid", 1, "r1", "r1-uuid", ["{gon}", 0], ["{tgon}", 0], {{"A": "B"}}]"#,
                    " and new tuple",
                    r#" - ["i99", "", 1, "", "", ["{goff}", 0], ["{tgoff}", 0], {{}}]"#,
                ),
                gon = CGV::Online,
                goff = CGV::Offline,
                tgon = TGV::Online,
                tgoff = TGV::Offline,
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

        let space = storage.space_by_name(ClusterwideSpace::Instance).unwrap();
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
        let storage = Clusterwide::new().unwrap();

        storage
            .space_by_name(ClusterwideSpace::Address)
            .unwrap()
            .insert(&(1, "foo"))
            .unwrap();
        storage
            .space_by_name(ClusterwideSpace::Address)
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
        let storage = Clusterwide::new().unwrap();
        storage.for_each_space(|s| s.truncate()).unwrap();

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

        let r = Replicaset {
            replicaset_id: "r1".into(),
            replicaset_uuid: "r1-uuid".into(),
            master_id: "i".into(),
            weight: crate::replicaset::weight::Info::default(),
        };
        storage.replicasets.space.insert(&r).unwrap();

        let (snapshot_data, _) = storage
            .first_snapshot_data_chunk(Default::default(), Default::default())
            .unwrap();
        assert!(snapshot_data.next_chunk_position.is_none());

        let space_dumps = snapshot_data.space_dumps;
        let n_internal_spaces = ClusterwideSpace::values().len();
        assert_eq!(space_dumps.len(), n_internal_spaces);

        for space_dump in &space_dumps {
            match space_dump.space_id {
                s if s == ClusterwideSpace::Instance.id() => {
                    let [instance]: [Instance; 1] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(instance, i);
                }

                s if s == ClusterwideSpace::Address.id() => {
                    let addrs: [(i32, String); 2] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(
                        addrs,
                        [(1, "google.com".to_string()), (2, "ya.ru".to_string())]
                    );
                }

                s if s == ClusterwideSpace::Property.id() => {
                    let [property]: [(String, String); 1] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(property, ("foo".to_owned(), "bar".to_owned()));
                }

                s if s == ClusterwideSpace::Replicaset.id() => {
                    let [replicaset]: [Replicaset; 1] =
                        Decode::decode(space_dump.tuples.as_ref()).unwrap();
                    assert_eq!(replicaset, r);
                }

                s_id => {
                    dbg!(s_id);
                    #[rustfmt::skip]
                    assert!(ClusterwideSpace::all_spaces().iter().any(|s| s.id() == s_id));
                    let []: [(); 0] = Decode::decode(space_dump.tuples.as_ref()).unwrap();
                }
            }
        }
    }

    #[::tarantool::test]
    fn apply_snapshot_data() {
        let storage = Clusterwide::new().unwrap();

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
            space_id: ClusterwideSpace::Instance.into(),
            tuples,
        });

        let tuples = [(1, "google.com"), (2, "ya.ru")].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space_id: ClusterwideSpace::Address.into(),
            tuples,
        });

        let tuples = [("foo", "bar")].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space_id: ClusterwideSpace::Property.into(),
            tuples,
        });

        let r = Replicaset {
            replicaset_id: "r1".into(),
            replicaset_uuid: "r1-uuid".into(),
            master_id: "i".into(),
            weight: crate::replicaset::weight::Info::default(),
        };
        let tuples = [&r].to_tuple_buffer().unwrap();
        data.space_dumps.push(SpaceDump {
            space_id: ClusterwideSpace::Replicaset.into(),
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
}
