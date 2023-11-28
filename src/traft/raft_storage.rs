use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::raft::StorageError;
use ::tarantool::index::IteratorType;
use ::tarantool::space::{Space, SpaceId, SpaceType};
use ::tarantool::tuple::ToTupleBuffer;
use std::cmp::Ordering;
use std::convert::TryFrom as _;

use crate::instance::InstanceId;
use crate::tlog;
use crate::traft;
use crate::traft::RaftEntryId;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;

#[derive(Clone, Debug)]
pub struct RaftSpaceAccess {
    space_raft_log: Space,
    space_raft_state: Space,
}

impl RaftSpaceAccess {
    const SPACE_RAFT_LOG: &'static str = "_raft_log";
    const SPACE_ID_RAFT_LOG: SpaceId = 518;
    const SPACE_RAFT_STATE: &'static str = "_raft_state";
    const SPACE_ID_RAFT_STATE: SpaceId = 519;
    const FIELD_STATE_VALUE: u32 = 1;
    const FIELD_ENTRY_INDEX: u32 = 1;
    const FIELD_ENTRY_TERM: u32 = 2;

    pub fn new() -> tarantool::Result<Self> {
        use tarantool::space::Field;

        let space_raft_log = Space::builder(Self::SPACE_RAFT_LOG)
            .id(Self::SPACE_ID_RAFT_LOG)
            .space_type(SpaceType::DataLocal)
            .field(Field::unsigned("entry_type"))
            .field(Field::unsigned("index"))
            .field(Field::unsigned("term"))
            .field(Field::any("data"))
            .field(Field::any("context"))
            .if_not_exists(true)
            .create()?;

        space_raft_log
            .index_builder("pk")
            .unique(true)
            .part("index")
            .if_not_exists(true)
            .create()?;

        let space_raft_state = Space::builder(Self::SPACE_RAFT_STATE)
            .id(Self::SPACE_ID_RAFT_STATE)
            .space_type(SpaceType::DataLocal)
            .field(Field::string("key"))
            .field(Field::any("value"))
            .if_not_exists(true)
            .create()?;

        space_raft_state
            .index_builder("pk")
            .unique(true)
            .part("key")
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space_raft_log,
            space_raft_state,
        })
    }

    #[inline(always)]
    pub fn try_get_raft_state<T>(&self, key: &str) -> tarantool::Result<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let tuple = self.space_raft_state.get(&[key])?;
        let mut res = None;
        if let Some(t) = tuple {
            res = t.field(Self::FIELD_STATE_VALUE)?;
        }
        Ok(res)
    }

    // Find the meaning of the fields here:
    // https://github.com/etcd-io/etcd/blob/main/raft/raftpb/raft.pb.go

    #[inline(always)]
    pub fn raft_id(&self) -> tarantool::Result<Option<RaftId>> {
        let res = self.try_get_raft_state("raft_id")?;
        Ok(res)
    }

    #[inline(always)]
    pub fn instance_id(&self) -> tarantool::Result<Option<InstanceId>> {
        let res = self.try_get_raft_state("instance_id")?;
        Ok(res)
    }

    #[inline(always)]
    pub fn tier(&self) -> tarantool::Result<Option<String>> {
        let res = self.try_get_raft_state("tier")?;
        Ok(res)
    }

    #[inline(always)]
    pub fn cluster_id(&self) -> tarantool::Result<String> {
        let res = self.try_get_raft_state("cluster_id")?;
        let res = res.expect("cluster_id should always be set");
        Ok(res)
    }

    /// Node generation i.e. the number of restarts.
    #[inline(always)]
    pub fn gen(&self) -> tarantool::Result<u64> {
        let res = self.try_get_raft_state("gen")?;
        let res = res.unwrap_or(0);
        Ok(res)
    }

    #[inline(always)]
    pub(crate) fn term(&self) -> tarantool::Result<RaftTerm> {
        let res = self.try_get_raft_state("term")?;
        let res = res.unwrap_or(0);
        Ok(res)
    }

    #[inline(always)]
    fn vote(&self) -> tarantool::Result<RaftId> {
        let res = self.try_get_raft_state("vote")?;
        let res = res.unwrap_or(0);
        Ok(res)
    }

    #[inline(always)]
    pub(crate) fn commit(&self) -> tarantool::Result<RaftIndex> {
        let res = self.try_get_raft_state("commit")?;
        let res = res.unwrap_or(0);
        Ok(res)
    }

    #[inline(always)]
    pub fn applied(&self) -> tarantool::Result<RaftIndex> {
        let res = self.try_get_raft_state("applied")?;
        let res = res.unwrap_or(0);
        Ok(res)
    }

    #[inline(always)]
    pub fn applied_entry_id(&self) -> tarantool::Result<RaftEntryId> {
        Ok(RaftEntryId {
            index: self.applied()?,
            term: self.term()?,
        })
    }

    #[inline(always)]
    pub(crate) fn voters(&self) -> tarantool::Result<Vec<RaftId>> {
        let res = self.try_get_raft_state("voters")?;
        let res = res.unwrap_or_default();
        Ok(res)
    }

    #[inline(always)]
    pub(crate) fn learners(&self) -> tarantool::Result<Vec<RaftId>> {
        let res = self.try_get_raft_state("learners")?;
        let res = res.unwrap_or_default();
        Ok(res)
    }

    #[inline(always)]
    fn voters_outgoing(&self) -> tarantool::Result<Vec<RaftId>> {
        let res = self.try_get_raft_state("voters_outgoing")?;
        let res = res.unwrap_or_default();
        Ok(res)
    }

    #[inline(always)]
    fn learners_next(&self) -> tarantool::Result<Vec<RaftId>> {
        let res = self.try_get_raft_state("learners_next")?;
        let res = res.unwrap_or_default();
        Ok(res)
    }

    #[inline(always)]
    fn auto_leave(&self) -> tarantool::Result<bool> {
        let res = self.try_get_raft_state("auto_leave")?;
        let res = res.unwrap_or(false);
        Ok(res)
    }

    /// Returns `compacted_index` that is `first_index - 1`.
    #[inline(always)]
    fn compacted_index(&self) -> tarantool::Result<RaftIndex> {
        let res = self.try_get_raft_state("compacted_index")?;
        let res = res.unwrap_or(0);
        Ok(res)
    }

    /// Returns the term of entry at `compacted_index`.
    #[inline(always)]
    fn compacted_term(&self) -> tarantool::Result<RaftTerm> {
        let res = self.try_get_raft_state("compacted_term")?;
        let res = res.unwrap_or(0);
        Ok(res)
    }

    #[inline(always)]
    pub fn conf_state(&self) -> tarantool::Result<raft::ConfState> {
        Ok(raft::ConfState {
            voters: self.voters()?,
            learners: self.learners()?,
            voters_outgoing: self.voters_outgoing()?,
            learners_next: self.learners_next()?,
            auto_leave: self.auto_leave()?,
            ..Default::default()
        })
    }

    pub fn hard_state(&self) -> tarantool::Result<raft::HardState> {
        Ok(raft::HardState {
            term: self.term()?,
            vote: self.vote()?,
            commit: self.commit()?,
            ..Default::default()
        })
    }

    /// Returns a slice of log entries in the range `[low, high)`.
    ///
    /// It also can be thought of as `[first_index, last_index+1)`.
    ///
    /// This function doesn't perform bound checks. In case some entries
    /// are missing, the returned `Vec` would be shorter than expected.
    /// This behavior differs from the similar function from
    /// [`raft::Storage`] trait.
    ///
    /// # Panics
    ///
    /// Panics if `high` < `low`.
    ///
    pub fn entries(&self, low: RaftIndex, high: RaftIndex) -> tarantool::Result<Vec<traft::Entry>> {
        let iter = self.space_raft_log.select(IteratorType::GE, &(low,))?;
        let mut ret = Vec::with_capacity((high - low) as _);

        for tuple in iter {
            let row = tuple.decode::<traft::Entry>()?;
            if row.index >= high {
                break;
            }
            ret.push(row);
        }

        Ok(ret)
    }

    pub fn all_traft_entries(&self) -> tarantool::Result<Vec<traft::Entry>> {
        self.space_raft_log
            .select(IteratorType::All, &())?
            .map(|tuple| tuple.decode())
            .collect()
    }

    #[inline(always)]
    pub fn persist_raft_id(&self, raft_id: RaftId) -> tarantool::Result<()> {
        self.space_raft_state.insert(&("raft_id", raft_id))?;
        Ok(())
    }

    #[inline(always)]
    pub fn persist_instance_id(&self, instance_id: &InstanceId) -> tarantool::Result<()> {
        self.space_raft_state
            .insert(&("instance_id", instance_id))?;
        Ok(())
    }

    #[inline(always)]
    pub fn persist_tier(&self, tier: &str) -> tarantool::Result<()> {
        self.space_raft_state.insert(&("tier", tier))?;
        Ok(())
    }

    #[inline(always)]
    pub fn persist_cluster_id(&self, cluster_id: &str) -> tarantool::Result<()> {
        self.space_raft_state.insert(&("cluster_id", cluster_id))?;
        Ok(())
    }

    #[inline(always)]
    pub fn persist_gen(&self, gen: u64) -> tarantool::Result<()> {
        self.space_raft_state.replace(&("gen", gen))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_term(&self, term: RaftTerm) -> tarantool::Result<()> {
        self.space_raft_state.replace(&("term", term))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_vote(&self, vote: RaftId) -> tarantool::Result<()> {
        self.space_raft_state.replace(&("vote", vote))?;
        Ok(())
    }

    #[inline(always)]
    pub fn persist_commit(&self, commit: RaftId) -> tarantool::Result<()> {
        self.space_raft_state.replace(&("commit", commit))?;
        Ok(())
    }

    #[inline(always)]
    pub fn persist_applied(&self, applied: RaftId) -> tarantool::Result<()> {
        self.space_raft_state.replace(&("applied", applied))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_voters(&self, voters: &[RaftId]) -> tarantool::Result<()> {
        self.space_raft_state.replace(&("voters", voters))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_learners(&self, learners: &[RaftId]) -> tarantool::Result<()> {
        self.space_raft_state.replace(&("learners", learners))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_voters_outgoing(&self, voters_outgoing: &[RaftId]) -> tarantool::Result<()> {
        self.space_raft_state
            .replace(&("voters_outgoing", voters_outgoing))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_learners_next(&self, learners_next: &[RaftId]) -> tarantool::Result<()> {
        self.space_raft_state
            .replace(&("learners_next", learners_next))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_auto_leave(&self, auto_leave: bool) -> tarantool::Result<()> {
        self.space_raft_state.replace(&("auto_leave", auto_leave))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_compacted_term(&self, compacted_term: RaftTerm) -> tarantool::Result<()> {
        self.space_raft_state
            .replace(&("compacted_term", compacted_term))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_compacted_index(&self, compacted_index: RaftTerm) -> tarantool::Result<()> {
        self.space_raft_state
            .replace(&("compacted_index", compacted_index))?;
        Ok(())
    }

    /// Persists the `ConfState` (voters, learners, etc.).
    ///
    /// # Panics
    ///
    /// In debug mode panics if invoked out of a transaction.
    ///
    pub fn persist_conf_state(&self, cs: &raft::ConfState) -> tarantool::Result<()> {
        debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
        self.persist_voters(&cs.voters)?;
        self.persist_learners(&cs.learners)?;
        self.persist_voters_outgoing(&cs.voters_outgoing)?;
        self.persist_learners_next(&cs.learners_next)?;
        self.persist_auto_leave(cs.auto_leave)?;
        Ok(())
    }

    /// Persists the `HardState` (term, vote, and commit index).
    ///
    /// # Panics
    ///
    /// In debug mode panics if invoked out of a transaction.
    ///
    pub fn persist_hard_state(&self, hs: &raft::HardState) -> tarantool::Result<()> {
        debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
        self.persist_term(hs.term)?;
        self.persist_vote(hs.vote)?;
        self.persist_commit(hs.commit)?;
        Ok(())
    }

    /// Persists a slice of entries.
    ///
    /// # Panics
    ///
    /// Panics if one `raft::Entry` of provided fails to be converted
    /// into `traft::Entry`.
    pub fn persist_entries(&self, entries: &[raft::Entry]) -> tarantool::Result<()> {
        for e in entries {
            let row = traft::Entry::try_from(e).unwrap();
            self.space_raft_log.replace(&row)?;
        }
        Ok(())
    }

    /// Handles snapshot metadata from raft-rs.
    ///
    /// Currently this truncates the raft log and persists
    /// `compacted_term`, `compacted_index`, `applied` index and `conf_state`.
    ///
    /// # Panics
    ///
    /// In debug mode panics if invoked outside of a transaction.
    ///
    pub fn handle_snapshot_metadata(&self, meta: &raft::SnapshotMetadata) -> tarantool::Result<()> {
        // We don't want to have a hole in the log, so we clear everything
        // before applying the snapshot
        self.compact_log(meta.index + 1)?;

        let compacted_index = self.compacted_index()?;
        assert!(meta.index > compacted_index);
        // We must set these explicitly, because compact_log only sets them to
        // the coordinates of the last entry which was in our log.
        self.persist_compacted_term(meta.term)?;
        self.persist_compacted_index(meta.index)?;

        let applied = self.applied()?;
        assert!(meta.index > applied);
        self.persist_applied(meta.index)?;
        self.persist_conf_state(meta.get_conf_state())?;
        Ok(())
    }

    /// Trims raft log up to the given index (excluding the index
    /// itself).
    ///
    /// Returns the new `first_index` after log compaction. It may
    /// differ from the requested one if the corresponding entry doesn't
    /// exist (either `up_to > last_index+1` or `up_to < first_index`).
    /// Yet both cases are valid.
    ///
    /// It also updates the `compacted_index` & `compacted_term`
    /// raft-state values, so it **should be invoked within a
    /// transaction**.
    ///
    /// # Panics
    ///
    /// In debug mode panics if invoked out of a transaction.
    ///
    pub fn compact_log(&self, up_to: RaftIndex) -> tarantool::Result<u64> {
        debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });

        // We cannot drop entries, which weren't applied yet
        let applied = self.applied()?;
        let up_to = up_to.min(applied + 1);

        // IteratorType::LT means tuples are returned in descending order
        let mut iter = self.space_raft_log.select(IteratorType::LT, &(up_to,))?;

        let Some(tuple) = iter.next() else {
            let compacted_index = self.compacted_index()?;
            return Ok(1 + compacted_index);
        };

        let compacted_index = tuple
            .field::<RaftIndex>(Self::FIELD_ENTRY_INDEX)?
            .expect("index is non-nullable");
        let compacted_term = tuple
            .field::<RaftTerm>(Self::FIELD_ENTRY_TERM)?
            .expect("term is non-nullable");
        self.space_raft_log.delete(&(compacted_index,))?;
        self.persist_compacted_index(compacted_index)?;
        self.persist_compacted_term(compacted_term)?;

        let mut num_deleted = 1;
        for tuple in iter {
            let index = tuple
                .field::<RaftIndex>(Self::FIELD_ENTRY_INDEX)?
                .expect("index is non-nullable");
            self.space_raft_log.delete(&(index,))?;
            num_deleted += 1;
        }

        let new_first_index = 1 + compacted_index;
        tlog!(Info, "compacted raft log: deleted {num_deleted} entries, new first entry index is {new_first_index}");

        Ok(new_first_index)
    }
}

impl raft::Storage for RaftSpaceAccess {
    fn initial_state(&self) -> Result<raft::RaftState, RaftError> {
        let hs = self.hard_state().cvt_err()?;
        let cs = self.conf_state().cvt_err()?;
        let ret = raft::RaftState::new(hs, cs);
        Ok(ret)
    }

    /// Returns a slice of log entries in the range `[low, high)`.
    ///
    /// As distinct from [`RaftSpaceAccess::entries`] this function
    /// returns either _all_ entries or an error:
    ///
    /// - `Err(Compacted)` if `low <= compacted_index`. Notice that an
    /// entry for `compacted_index` doesn't exist, despite its term is
    /// known.
    ///
    /// - `Err(Unavailable)` in case at least one entry in the range is
    /// missing (either `high > last_index` or it's really missing).
    /// Raft-rs will panic in this case, but it's fair.
    ///
    /// # See also
    ///
    /// Result handling [@github].
    ///
    /// [@github]:
    ///     https://github.com/tikv/raft-rs/blob/v0.6.0/src/raft_log.rs#L583
    ///
    /// # Panics
    ///
    /// Panics if `high` < `low`.
    ///
    fn entries(
        &self,
        low: RaftIndex,
        high: RaftIndex,
        _max_size: impl Into<Option<u64>>,
    ) -> Result<Vec<raft::Entry>, RaftError> {
        if low <= self.compacted_index().cvt_err()? {
            return Err(RaftError::Store(StorageError::Compacted));
        }

        let ret: Vec<traft::Entry> = self.entries(low, high).cvt_err()?;
        if ret.len() < (high - low) as usize {
            return Err(RaftError::Store(StorageError::Unavailable));
        }
        Ok(ret.into_iter().map(Into::into).collect())
    }

    /// Returns the term of entry `idx`.
    ///
    /// The valid range is `[compacted_index, last_index()]`, that's why
    /// we also persist `compacted_term`. Besides the range returns an
    /// error:
    ///
    /// - `Err(Compacted)` if `idx < compacted_index`.
    /// - `Err(Unavailable)` if `idx > last_index()`.
    ///
    /// # See also
    ///
    /// Result handling [@github].
    ///
    /// [@github]:
    ///     https://github.com/tikv/raft-rs/blob/v0.6.0/src/raft_log.rs#L134
    ///
    fn term(&self, idx: RaftIndex) -> Result<RaftTerm, RaftError> {
        let compacted_index = self.compacted_index().cvt_err()?;
        let compacted_term = self.compacted_term().cvt_err()?;

        match idx.cmp(&compacted_index) {
            Ordering::Less => return Err(RaftError::Store(StorageError::Compacted)),
            Ordering::Equal => return Ok(compacted_term),
            Ordering::Greater => {} // go on
        }

        let tuple = self.space_raft_log.get(&(idx,)).cvt_err()?;
        if let Some(tuple) = tuple {
            return Ok(tuple
                .field(Self::FIELD_ENTRY_TERM)
                .cvt_err()?
                .expect("term is non-nullable"));
        }

        Err(RaftError::Store(StorageError::Unavailable))
    }

    /// Returns `compacted_index` plus 1.
    ///
    /// The naming is actually misleading, as returned index might not
    /// exist. In that case `last_index < first_index` and it indicates
    /// that the log was compacted. Raft-rs treats it the same way.
    ///
    /// Empty log is considered compacted at index 0 with term 0.
    ///
    fn first_index(&self) -> Result<RaftIndex, RaftError> {
        let compacted_index = self.compacted_index().cvt_err()?;
        Ok(1 + compacted_index)
    }

    /// Returns the last index which term is available.
    ///
    /// And again, the naming is actually misleading. The entry itself
    /// might not exist if the whole log was compacted or is actually
    /// empty. That case also implies `last_index < first_index`.
    ///
    /// Empty log is considered compacted at index 0 with term 0.
    ///
    fn last_index(&self) -> Result<RaftIndex, RaftError> {
        let tuple = self.space_raft_log.primary_key().max(&()).cvt_err()?;

        if let Some(t) = tuple {
            Ok(t.field(Self::FIELD_ENTRY_INDEX)
                .cvt_err()?
                .expect("index is non-nullabe"))
        } else {
            Ok(self.compacted_index().cvt_err()?)
        }
    }

    fn snapshot(&self, idx: RaftIndex) -> Result<raft::Snapshot, RaftError> {
        let applied = self.applied_entry_id().cvt_err()?;
        if applied.index < idx {
            crate::warn_or_panic!(
                "requested snapshot for index {idx} greater than applied {}",
                applied.index,
            );
            return Err(StorageError::SnapshotTemporarilyUnavailable.into());
        }

        let compacted_index = self.compacted_index().cvt_err()?;

        let storage = crate::storage::Clusterwide::get();

        // NOTE: There's a problem. Ideally we would want to cache the generated
        // snapshots and reuse them when someone else requests the compatible
        // snapshot. But at the moment there's no good way to do this.
        //
        // If we do cache the snapshots, we also need to cache the conf_state
        // at the moment of snapshot creation, otherwise there's a non-zero
        // probability to break the cluster. The problem is raft-rs will
        // silently ignore the snapshot if it's conf_state doesn't contain the
        // receiving instance. The comment in the source states that this is
        // just a precaution, and it's not obvious, that this should actually be
        // done, but here we are.
        // See <https://github.com/tikv/raft-rs/blob/f60fb9e143e5b93f7db8917ea376cda04effcbb4/src/raft.rs#L2589>
        // We could work around this by adding more crutches and implementing
        // our own system to report to the leader that the snapshot must be
        // regenerated, but I guess this is a TODO.
        let (data, entry_id) = storage
            .first_snapshot_data_chunk(applied, compacted_index)
            .cvt_err()?;

        let mut snapshot = raft::Snapshot::new();
        let meta = snapshot.mut_metadata();
        meta.index = entry_id.index;
        meta.term = entry_id.term;
        // XXX: make sure this conf state corresponds to the snapshot data.
        *meta.mut_conf_state() = self.conf_state().cvt_err()?;

        let data = data.to_tuple_buffer().cvt_err()?;
        *snapshot.mut_data() = Vec::from(data).into();

        // SAFETY: this is safe as long as we only use tlog from tx thread.
        unsafe {
            crate::tlog::DONT_LOG_KV_FOR_NEXT_SENDING_FROM = true;
        }

        Ok(snapshot)
    }
}

/// Convert errors from tarantool to raft-rs types.
/// The only purpose of this trait is to reduce boilerplate.
trait CvtErr {
    type T;
    fn cvt_err(self) -> Result<Self::T, StorageError>;
}

impl<T> CvtErr for Result<T, tarantool::error::Error> {
    type T = T;
    #[inline(always)]
    fn cvt_err(self) -> Result<T, StorageError> {
        self.map_err(|e| StorageError::Other(Box::new(e)))
    }
}

// This is needed because traft::error::Error doesn't implement Sync & Send,
// because it has the `Other` variant which isn't Sync & Send.
#[derive(thiserror::Error, Debug)]
#[error("{0}")]
struct SyncSendError(String);

impl<T> CvtErr for Result<T, traft::error::Error> {
    type T = T;
    #[inline(always)]
    fn cvt_err(self) -> Result<T, StorageError> {
        self.map_err(|e| StorageError::Other(Box::new(SyncSendError(e.to_string()))))
    }
}

macro_rules! assert_err {
    ($expr:expr, $err:expr) => {
        assert_eq!($expr.map_err(|e| format!("{e}")), Err($err.into()))
    };
}

macro_rules! assert_err_starts_with {
    ($actual:expr, $expected:expr) => {{
        let actual = $actual.unwrap_err().to_string();
        let expected = $expected;
        assert_eq!(&actual[..expected.len()], expected)
    }};
}

mod tests {
    use super::*;
    use ::tarantool::transaction::transaction;

    fn dummy_entry(index: RaftIndex, term: RaftTerm) -> raft::Entry {
        raft::Entry {
            term,
            index,
            ..Default::default()
        }
    }

    #[::tarantool::test]
    fn test_cloning() {
        // It's fine to create two access objects
        let s1 = RaftSpaceAccess::new().unwrap();
        let s2 = RaftSpaceAccess::new().unwrap();
        assert_eq!(s1.space_raft_log.id(), s2.space_raft_log.id());
        assert_eq!(s1.space_raft_state.id(), s2.space_raft_state.id());

        // Though it's better to clone one
        #[allow(clippy::redundant_clone)]
        let _s3 = s2.clone();
    }

    #[::tarantool::test]
    fn test_log() {
        use ::raft::Storage as S;

        // Part 1. No log, no snapshot.
        let storage = RaftSpaceAccess::new().unwrap();

        assert_eq!(S::first_index(&storage), Ok(1));
        assert_eq!(S::last_index(&storage), Ok(0));

        assert_eq!(S::term(&storage, 0), Ok(0));
        assert_err!(S::term(&storage, 1), "log unavailable");

        // Part 2. Whole log was compacted.
        transaction(|| -> tarantool::Result<()> {
            storage.persist_compacted_term(9)?;
            storage.persist_compacted_index(99)?;
            Ok(())
        })
        .unwrap();

        assert_eq!(S::first_index(&storage), Ok(100));
        assert_eq!(S::last_index(&storage), Ok(99));

        assert_err!(S::term(&storage, 0), "log compacted");
        assert_eq!(S::term(&storage, 99), Ok(9));
        assert_err!(S::term(&storage, 100), "log unavailable");

        assert_eq!(S::entries(&storage, 100, 100, u64::MAX), Ok(vec![]));

        // Part 3. Add some new entries.
        let test_entries = vec![
            dummy_entry(100, 10),
            dummy_entry(101, 10),
            dummy_entry(102, 10),
        ];
        storage.persist_entries(&test_entries).unwrap();

        let (first, last) = (100, 102);
        assert_eq!(S::first_index(&storage), Ok(first));
        assert_eq!(S::last_index(&storage), Ok(last));

        assert_eq!(S::term(&storage, first - 1), Ok(9));
        assert_eq!(S::term(&storage, first), Ok(10));
        assert_eq!(S::term(&storage, last), Ok(10));
        assert_err!(S::term(&storage, last + 1), "log unavailable");

        assert_err!(
            S::entries(&storage, first - 1, last + 1, u64::MAX),
            "log compacted"
        );

        assert_err!(
            S::entries(&storage, first, last + 2, u64::MAX),
            "log unavailable"
        );

        // `first, last + 1` is what raft-rs literally does, see [@github].
        //
        // [@github]: https://github.com/tikv/raft-rs/blob/v0.6.0/src/raft_log.rs#L388
        //
        assert_eq!(
            S::entries(&storage, first, last + 1, u64::MAX),
            Ok(test_entries)
        );
    }

    #[::tarantool::test]
    fn test_damaged_storage() {
        use ::raft::Storage as S;

        let storage = RaftSpaceAccess::new().unwrap();
        let raft_log = storage.space_raft_log.clone();
        let term = 666;

        let (first, last) = (1, 1);
        raft_log.put(&(1337, first, term, "", ())).unwrap();
        assert_err_starts_with!(
            S::entries(&storage, first, last + 1, u64::MAX),
            "unknown error failed to decode tuple: unknown entry type (1337)"
        );

        raft_log.put(&(0, first, term, "", false)).unwrap();
        assert_err_starts_with!(
            S::entries(&storage, first, last + 1, u64::MAX),
            concat!(
                "unknown error",
                " failed to decode tuple:",
                " data did not match any variant",
                " of untagged enum EntryContext"
            )
        );

        raft_log.put(&(0, 1, term, "", ())).unwrap();
        // skip index 2
        raft_log.put(&(0, 3, term, "", ())).unwrap();
        assert_err!(S::entries(&storage, 1, 4, u64::MAX), "log unavailable");

        raft_log.primary_key().drop().unwrap();
        assert_err_starts_with!(
            S::entries(&storage, first, last + 1, u64::MAX),
            concat!(
                "unknown error",
                " tarantool error:",
                " NoSuchIndexID:",
                " No index #0 is defined in space '_raft_log'"
            )
        );

        raft_log.drop().unwrap();
        assert_err_starts_with!(
            S::entries(&storage, first, last + 1, u64::MAX),
            format!(
                concat!(
                    "unknown error",
                    " tarantool error:",
                    " NoSuchSpace:",
                    " Space '{}' does not exist",
                ),
                storage.space_raft_log.id()
            )
        );
    }

    #[::tarantool::test]
    fn test_log_compaction() {
        use ::raft::Storage as S;

        let storage = RaftSpaceAccess::new().unwrap();

        let (first, last) = (1, 10);
        for i in first..=last {
            storage.persist_entries(&[dummy_entry(i, i)]).unwrap();
        }
        let commit = 8;
        storage.persist_commit(commit).unwrap();
        let entries = |lo, hi| S::entries(&storage, lo, hi, u64::MAX);
        let compact_log = |up_to| transaction(|| storage.compact_log(up_to));

        assert_eq!(S::first_index(&storage), Ok(first));
        assert_eq!(S::last_index(&storage), Ok(last));
        assert_eq!(entries(first, last + 1).unwrap().len(), 10);

        let first = 6;
        assert_eq!(compact_log(first).unwrap(), first);
        assert_eq!(S::first_index(&storage), Ok(first));
        assert_eq!(S::last_index(&storage), Ok(last));
        assert_eq!(S::term(&storage, 5), Ok(5));
        assert_eq!(S::term(&storage, first), Ok(6));
        assert_err!(S::term(&storage, 4), "log compacted");
        assert_err!(entries(1, last + 1), "log compacted");
        assert_eq!(entries(first, last + 1).unwrap().len(), 5);

        let tuple = storage
            .space_raft_log
            .primary_key()
            .min(&())
            .unwrap()
            .unwrap();

        let first_entry = tuple.decode::<traft::Entry>().unwrap();
        assert_eq!(first_entry.index, first);

        // check idempotency
        assert_eq!(compact_log(0).unwrap(), first);
        assert_eq!(compact_log(first).unwrap(), first);

        // cannot compact past commit
        assert_eq!(compact_log(commit + 2).unwrap(), commit + 1);

        storage.persist_commit(last).unwrap();

        // trim to the end
        assert_eq!(compact_log(u64::MAX).unwrap(), last + 1);
        assert_eq!(storage.space_raft_log.len().unwrap(), 0);
    }

    #[::tarantool::test]
    fn test_hard_state() {
        use ::raft::Storage as S;

        let storage = RaftSpaceAccess::new().unwrap();

        assert_eq!(
            S::initial_state(&storage).unwrap().hard_state,
            raft::HardState::default()
        );

        let hs = raft::HardState {
            term: 9,
            vote: 1,
            commit: 98,
            ..Default::default()
        };
        transaction(|| storage.persist_hard_state(&hs)).unwrap();
        assert_eq!(S::initial_state(&storage).unwrap().hard_state, hs);
    }

    #[::tarantool::test]
    fn test_conf_state() {
        use ::raft::Storage as S;

        let storage = RaftSpaceAccess::new().unwrap();

        assert_eq!(
            S::initial_state(&storage).unwrap().conf_state,
            raft::ConfState::default()
        );

        let cs = raft::ConfState {
            auto_leave: true,
            voters: vec![1],
            learners: vec![2],
            learners_next: vec![3, 4],
            voters_outgoing: vec![5, 6],
            ..Default::default()
        };

        transaction(|| storage.persist_conf_state(&cs)).unwrap();
        assert_eq!(S::initial_state(&storage).unwrap().conf_state, cs);
    }

    #[::tarantool::test]
    fn test_other_state() {
        let storage = RaftSpaceAccess::new().unwrap();
        let raft_state = Space::find(RaftSpaceAccess::SPACE_RAFT_STATE).unwrap();

        storage.persist_cluster_id("test_cluster").unwrap();
        storage.persist_cluster_id("test_cluster_2").unwrap_err();
        assert_eq!(storage.cluster_id().unwrap(), "test_cluster".to_string());

        raft_state.delete(&("id",)).unwrap();
        assert_eq!(storage.raft_id().unwrap(), None);

        storage.persist_raft_id(16).unwrap();
        assert_err!(
            storage.persist_raft_id(32),
            concat!(
                "tarantool error:",
                " TupleFound:",
                " Duplicate key exists in unique index \"pk\" in space \"_raft_state\"",
                " with old tuple - [\"raft_id\", 16]",
                " and new tuple - [\"raft_id\", 32]"
            )
        );

        raft_state.drop().unwrap();
        assert_err!(
            storage.commit(),
            format!(
                concat!(
                    "tarantool error:",
                    " NoSuchSpace:",
                    " Space '{}' does not exist",
                ),
                storage.space_raft_state.id()
            )
        );
    }
}
