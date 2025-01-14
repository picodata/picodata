use crate::access_control::{user_by_id, UserMetadataKind};
use crate::schema::Distribution;
use crate::schema::PrivilegeDef;
use crate::schema::TableDef;
use crate::schema::UserDef;
use crate::schema::INITIAL_SCHEMA_VERSION;
use crate::storage::ignore_only_error;
use crate::storage::local_schema_version;
use crate::storage::schema::acl;
use crate::storage::schema::ddl_create_space_on_master;
use crate::storage::schema::ddl_drop_space_on_master;
use crate::storage::set_local_schema_version;
use crate::storage::Clusterwide;
use crate::storage::ClusterwideTable;
use crate::storage::ToEntryIter;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::RaftEntryId;
use crate::traft::RaftIndex;
use crate::traft::Result;
use crate::warn_or_panic;
use std::cell::UnsafeCell;
use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::Peekable;
use tarantool::error::Error as TntError;
use tarantool::error::TarantoolErrorCode as TntErrorCode;
use tarantool::fiber;
use tarantool::msgpack::{ArrayWriter, ValueIter};
use tarantool::read_view::ReadView;
use tarantool::read_view::ReadViewIterator;
use tarantool::session::UserId;
use tarantool::space::SpaceId;
use tarantool::time::Instant;
use tarantool::tuple::Encode;
use tarantool::tuple::RawBytes;
use tarantool::tuple::TupleBuffer;

////////////////////////////////////////////////////////////////////////////////
// impl Clusterwide
////////////////////////////////////////////////////////////////////////////////

// FIXME: this is bad style (impl block in a separate file from struct definition),
// it makes it hard to look through code. It's better if the functions are
// standalone functions, if you want methods, just keep them all in one file and
// redirect to standalone functions, for example:
// ```
// impl Clusterwide {
//     #[inline(always)]
//     pub fn apply_snapshot_data(&self, data: &SnapshotData, is_master: bool) -> Result<()> {
//         storage::apply_snapshot_data(self, data, is_master)
//     }
//
//     ...
// }
// ```
// But you don't actually want these functions to be methods, just trust me bro
impl Clusterwide {
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
        let snapshot_chunk_max_size = self.db_config.raft_snapshot_chunk_size_max()?;
        let t0 = std::time::Instant::now();
        for &(space_id, index_id) in global_spaces {
            debug_assert_eq!(index_id, 0, "only primary keys should be added");

            // NOTE: we do linear search here, but it shouldn't matter, unless
            // somebody decides to have a lot of global spaces. But in that case
            // I doubt this is going to be the slowest part of picodata...
            if space_id < position.space_id {
                continue;
            }

            type IteratorType<'a> = Option<Peekable<ReadViewIterator<'a>>>;
            let mut iter: IteratorType<'static> = None;
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
                unsafe { iter = std::mem::transmute::<IteratorType, IteratorType<'static>>(it) };
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

        let timeout = self.db_config.raft_snapshot_read_view_close_timeout()?;
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
    /// Returns changed dynamic parameters as key-value tuples from `_pico_db_config`.
    ///
    #[allow(rustdoc::private_intra_doc_links)]
    /// [`NodeImpl::advance`]: crate::traft::node::NodeImpl::advance
    pub fn apply_snapshot_data(
        &self,
        data: &SnapshotData,
        is_master: bool,
    ) -> Result<Vec<Vec<u8>>> {
        debug_assert!(unsafe { ::tarantool::ffi::tarantool::box_txn() });

        // These need to be saved before we truncate the corresponding spaces.
        let mut old_table_versions = HashMap::new();
        let mut old_user_versions = HashMap::new();
        let mut old_priv_versions = HashMap::new();

        let mut old_dynamic_parameters = HashSet::new();
        let mut changed_parameters = Vec::new();

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

        for parameter in self.db_config.iter()? {
            old_dynamic_parameters.insert(parameter.to_vec());
        }

        // There can be multiple space dumps for a given space in SnapshotData,
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
            "restoring global schema definitions took {:?} [{} tuples]",
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
                // We do not support removing tuples from _pico_db_config, but if we did we would have
                // to keep track of which tuples were removed in the snapshot. Keep this in mind if
                // in the future some ALTER SYSTEM parameters are removed and/or renamed
                if space_id == PICO_DB_CONFIG && !old_dynamic_parameters.contains(tuple) {
                    changed_parameters.push(tuple.to_vec())
                }

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

        return Ok(changed_parameters);

        const SCHEMA_DEFINITION_SPACES: &[SpaceId] = &[
            ClusterwideTable::Table.id(),
            ClusterwideTable::Index.id(),
            ClusterwideTable::User.id(),
            ClusterwideTable::Privilege.id(),
        ];

        const PICO_DB_CONFIG: SpaceId = ClusterwideTable::DbConfig.id();
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
    ) -> Result<()>
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
    fn on_insert(&self, storage: &Clusterwide) -> Result<()>;

    /// Is called when the entity is being dropped in the local storage.
    /// If the entity is being changed, first `Self::on_delete` is called and
    /// then `Self::on_insert` is called for it.
    ///
    /// Should perform the necessary tarantool api calls and or tarantool system
    /// space updates.
    fn on_delete(key: &Self::Key, storage: &Clusterwide) -> Result<()>;
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
    fn on_insert(&self, storage: &Clusterwide) -> Result<()> {
        let space_id = self.id;
        if let Some(abort_reason) = ddl_create_space_on_master(storage, space_id)? {
            return Err(Error::other(format!(
                "failed to create table {space_id}: {abort_reason}"
            )));
        }
        Ok(())
    }

    #[inline(always)]
    fn on_delete(space_id: &SpaceId, storage: &Clusterwide) -> Result<()> {
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
    fn on_insert(&self, storage: &Clusterwide) -> Result<()> {
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
    fn on_delete(user_id: &UserId, storage: &Clusterwide) -> Result<()> {
        _ = storage;

        let user = user_by_id(*user_id)?;
        match user.ty {
            UserMetadataKind::User => acl::on_master_drop_user(*user_id)?,
            UserMetadataKind::Role => acl::on_master_drop_role(*user_id)?,
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
    fn on_insert(&self, storage: &Clusterwide) -> Result<()> {
        _ = storage;
        acl::on_master_grant_privilege(self)?;
        Ok(())
    }

    #[inline(always)]
    fn on_delete(this: &Self, storage: &Clusterwide) -> Result<()> {
        _ = storage;
        acl::on_master_revoke_privilege(this)?;
        Ok(())
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
// tests
////////////////////////////////////////////////////////////////////////////////

mod tests {
    use super::*;
    use crate::config::PicodataConfig;
    use crate::instance::Instance;
    use crate::replicaset::Replicaset;
    use tarantool::transaction::transaction;
    use tarantool::tuple::Decode;
    use tarantool::tuple::ToTupleBuffer;

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
            name: "i".into(),
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

        if let Err(e) = transaction(|| -> Result<_> { storage.apply_snapshot_data(&data, true) }) {
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
    fn snapshot_data() {
        let storage = Clusterwide::for_tests();

        let i = Instance {
            raft_id: 1,
            name: "i".into(),
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

        PicodataConfig::init_for_tests();

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
                    // FIXME: https://git.picodata.io/picodata/picodata/picodata/-/issues/570
                    // let []: [(); 0] = Decode::decode(space_dump.tuples.as_ref()).unwrap();
                }
            }
        }
    }
}
