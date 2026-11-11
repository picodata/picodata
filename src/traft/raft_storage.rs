use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::raft::GetEntriesContext;
use ::raft::StorageError;
use ::tarantool::error::TarantoolError;
use ::tarantool::index::IteratorType;
use ::tarantool::space::{Space, SpaceId, SpaceType};
use ::tarantool::transaction::{
    set_force_async, set_sync_wal, transaction, transaction_force_async, TransactionError,
};
use ::tarantool::tuple::ToTupleBuffer;
use std::cmp::Ordering;
use std::convert::TryFrom as _;

use crate::instance::InstanceName;
use crate::metrics;
use crate::tlog;
use crate::traft;
use crate::traft::RaftEntryId;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;

/// Runs `f` in a transaction suitable for modifying `_raft_log` / `_raft_state`.
///
/// Such a transaction is:
///
/// - **force async**, so it does not enter the synchronous replication limbo
///   and cannot be rolled back by a leader change.
///   For details see [`tarantool::transaction::transaction_force_async`]
/// - **flushed to stable storage** before it is reported as committed. Raft may
///   not ack a vote, a term or a snapshot to the leader until it survives a
///   crash. The default `wal_mode = write` does not guarantee durability in
///   case of a host crash, and losing the vote breaks election safety.
///
/// Every write of the term, the vote, a snapshot or the instance identity
/// must go through this.
/// Use [`transaction_raft_replayable`] for writes that raft can lose,
/// including the log entries, which follow `wal_mode`; the current
/// status of the acks and the plan to close the gap are documented
/// there.
pub fn transaction_raft<T, E, F>(f: F) -> Result<T, TransactionError<E>>
where
    F: FnOnce() -> Result<T, E>,
    E: From<TarantoolError>,
{
    transaction(|| -> Result<T, E> {
        set_force_async()?;
        set_sync_wal()?;
        f()
    })
}

/// Runs `f` in a transaction for the raft state that may be lost on a host
/// crash: the applied index, the commit index, the conf state and the log
/// entries.
///
/// Such a transaction is **force async** like [`transaction_raft`], but is not
/// flushed to stable storage. Raft promises none of this state to other
/// nodes, and what a crash takes away is restored from the durable raft log:
///
/// - the WAL is append-only, so a crash loses a tail of it and never a write
///   in the middle. A write is durable once any later [`transaction_raft`] is.
/// - the applied index is written in the same transaction as the effects of
///   the entry, so the two are lost together, and the entry is applied again
///   on restart.
/// - the commit index is written before the entries it covers are applied, so
///   the restored applied index never exceeds the restored commit index, as
///   raft-rs requires. A lost commit index is learned from the leader again.
/// - the conf state is written together with the applied index, and the conf
///   change that produced it is applied again as well.
/// - the log entries which the node has acked can be lost with the WAL tail.
///   The paper prescribes the opposite - a follower puts an entry on stable
///   storage before it responds - and fsyncing the log entries here. The fix
///   is planned behind several batching related optimizations, because otherwise
///   global table insert performance degrades by 10x+.
///
/// The term, the vote and snapshots are promises to other nodes, and must go
/// through [`transaction_raft`] instead.
pub fn transaction_raft_replayable<T, E, F>(f: F) -> Result<T, TransactionError<E>>
where
    F: FnOnce() -> Result<T, E>,
    E: From<TarantoolError>,
{
    transaction_force_async(f)
}

#[derive(Clone, Debug)]
pub struct RaftSpaceAccess {
    space_raft_log: Space,
    space_raft_state: Space,
}

impl RaftSpaceAccess {
    const SPACE_RAFT_LOG: &'static str = "_raft_log";
    pub const SPACE_ID_RAFT_LOG: SpaceId = 518;
    pub const SPACE_RAFT_STATE: &'static str = "_raft_state";
    const SPACE_ID_RAFT_STATE: SpaceId = 519;
    pub const FIELD_STATE_VALUE: u32 = 1;
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

    /// Initializes the raft state system spaces for testing purposes.
    pub fn for_tests() -> Self {
        let raft_storage = Self::new().unwrap();

        if raft_storage.raft_id().unwrap().is_some() {
            return raft_storage;
        }

        transaction_raft(|| -> tarantool::Result<()> {
            let raft_id = 1;
            raft_storage.persist_raft_id(raft_id).unwrap();
            raft_storage
                .persist_instance_name(&InstanceName::from("default_1_1"))
                .unwrap();
            raft_storage.persist_tier("default").unwrap();
            raft_storage.persist_cluster_name("test_cluster").unwrap();
            raft_storage
                .persist_cluster_uuid("b071480d-080d-49fd-b7ae-8c5645783428")
                .unwrap();

            let mut cs = raft::ConfState::default();
            cs.voters = vec![raft_id];
            raft_storage.persist_conf_state(&cs).unwrap();

            let hs = raft::HardState::default();
            raft_storage.persist_hard_state(&hs, true).unwrap();

            Ok(())
        })
        .unwrap();

        raft_storage
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

    /// Returns the persisted `InstanceName` of the current instance.
    /// This should be persisted before the global [`Node`] is initialized.
    ///
    /// [`Node`]: crate::traft::node::Node
    #[inline(always)]
    pub fn instance_name(&self) -> tarantool::Result<Option<InstanceName>> {
        let res = self.try_get_raft_state("instance_name")?;
        Ok(res)
    }

    #[inline(always)]
    pub fn instance_uuid(&self) -> tarantool::Result<Option<String>> {
        let res = self.try_get_raft_state("instance_uuid")?;
        Ok(res)
    }

    #[inline(always)]
    pub fn tier(&self) -> tarantool::Result<Option<String>> {
        let res = self.try_get_raft_state("tier")?;
        Ok(res)
    }

    /// Lookup `cluster_name` in `_raft_state` system space.
    ///
    /// NOTE: you probably instead want to use `node.catalog_cache.cluster_name`
    /// if you have access to [`crate::traft::node::Node`].
    #[inline(always)]
    pub fn cluster_name(&self) -> tarantool::Result<String> {
        let res = self.try_get_raft_state("cluster_name")?;
        let res = res.expect("cluster_name should always be set");
        Ok(res)
    }

    /// Lookup `cluster_uuid` in `_raft_state` system space.
    ///
    /// NOTE: you probably instead want to use `node.catalog_cache.cluster_uuid`
    /// if you have access to [`crate::traft::node::Node`].
    #[inline(always)]
    pub fn cluster_uuid(&self) -> tarantool::Result<String> {
        // Use empty string as default for backward compatibility.
        // This allows safe upgrade from previous versions where cluster_uuid might not be set.
        let res = self.try_get_raft_state("cluster_uuid")?;
        let res = res.unwrap_or_default();
        Ok(res)
    }

    /// Node generation i.e. the number of restarts.
    #[inline(always)]
    pub fn gen(&self) -> tarantool::Result<u64> {
        let res = self.try_get_raft_state("gen")?;
        let res = res.unwrap_or(0);
        Ok(res)
    }

    /// Returns the current term.
    #[inline(always)]
    pub(crate) fn term(&self) -> tarantool::Result<RaftTerm> {
        let res = self.try_get_raft_state("term")?;
        let res = res.unwrap_or(0);
        Ok(res)
    }

    /// Returns the term of the last applied entry.
    #[inline]
    pub(crate) fn applied_term(&self) -> tarantool::Result<RaftTerm> {
        let res = self.try_get_raft_state("applied_term")?;
        let Some(term) = res else {
            return self.applied_term_compatibility_fallback();
        };
        Ok(term)
    }

    /// This function is needed to maintain compatibility with picodata clusters
    /// deployed before 25.4.3 (inclusive).
    fn applied_term_compatibility_fallback(&self) -> tarantool::Result<RaftTerm> {
        let applied = self.applied()?;
        let res = ::raft::Storage::term(self, applied);
        let term = match res {
            Ok(v) => v,
            Err(RaftError::Store(StorageError::Compacted | StorageError::Unavailable)) => {
                // Cannot afford to not return a value in these cases. Fallback to the current term.
                return self.term();
            }
            Err(e) => {
                // This is a recoverable error (hopefully)
                return Err(crate::traft::error::to_error_other(e).into());
            }
        };

        Ok(term)
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
            term: self.applied_term()?,
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
    pub fn compacted_index(&self) -> tarantool::Result<RaftIndex> {
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
    pub fn join_state(&self) -> tarantool::Result<Option<String>> {
        let res = self.try_get_raft_state("join_state")?;
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
    pub fn entries(
        &self,
        low: RaftIndex,
        high: RaftIndex,
        limit: Option<usize>,
    ) -> tarantool::Result<Vec<traft::Entry>> {
        let iter = self.space_raft_log.select_v2(IteratorType::GE, &(low,))?;
        let limit = limit.unwrap_or(usize::MAX);
        // Put a sane threshold on `cap` in case `high` is insanely big
        let cap = limit.min((high - low) as _).min(1024);
        let mut ret = Vec::with_capacity(cap);

        for tuple in iter {
            let row = tuple?.decode::<traft::Entry>()?;
            if row.index >= high {
                break;
            }
            if ret.len() == limit {
                break;
            }
            ret.push(row);
        }

        Ok(ret)
    }

    pub fn all_traft_entries(&self) -> tarantool::Result<Vec<traft::Entry>> {
        self.space_raft_log
            .select_v2(IteratorType::All, &())?
            .map(|tuple| tuple.map_err(Into::into).and_then(|t| t.decode()))
            .collect()
    }

    /// The `_raft_state` space, for modification.
    ///
    /// Reads may use the field directly; every write must go through here or
    /// through [`Self::state_for_replayable_write`], so that the durability
    /// assertion below cannot be bypassed by accident.
    fn state_for_write(&self) -> tarantool::Result<&Space> {
        Self::assert_durable_txn()?;

        Ok(&self.space_raft_state)
    }

    /// The `_raft_state` space, for modifying the keys that may be lost on a
    /// host crash, see [`transaction_raft_replayable`].
    fn state_for_replayable_write(&self) -> &Space {
        Self::debug_assert_txn();
        &self.space_raft_state
    }

    /// The `_raft_log` space, for modification. See [`Self::state_for_write`].
    fn log_for_write(&self) -> tarantool::Result<&Space> {
        Self::assert_durable_txn()?;

        Ok(&self.space_raft_log)
    }

    /// The `_raft_log` space, for modifying the entries that may be lost on a
    /// host crash, see [`transaction_raft_replayable`].
    fn log_for_replayable_write(&self) -> &Space {
        Self::debug_assert_txn();
        &self.space_raft_log
    }

    /// Raft may not ack a vote, a term or a snapshot to the leader before
    /// they have reached stable storage, so those modifications of the raft
    /// spaces have to run in a transaction opened by [`transaction_raft`].
    /// The log entries follow `wal_mode` instead, see
    /// [`Self::persist_entries_replayable`].
    ///
    /// The check is not debug-only: the durable writes are what election
    /// safety rests on, so a misplaced write must fail in release too,
    /// not silently skip the fsync.
    fn assert_durable_txn() -> tarantool::Result<()> {
        if !::tarantool::transaction::is_sync_wal() {
            return Err(tarantool::error::Error::other(
                "raft storage must only be modified inside transaction_raft()",
            ));
        }
        Ok(())
    }

    #[inline(always)]
    fn debug_assert_txn() {
        debug_assert!(
            tarantool::transaction::is_in_transaction(),
            "raft storage must only be modified inside a transaction",
        );
    }

    #[inline(always)]
    pub fn persist_raft_id(&self, raft_id: RaftId) -> tarantool::Result<()> {
        self.state_for_write()?.insert(&("raft_id", raft_id))?;
        Ok(())
    }

    #[inline(always)]
    pub fn persist_instance_name(&self, instance_name: &InstanceName) -> tarantool::Result<()> {
        self.state_for_write()?
            .insert(&("instance_name", instance_name))?;
        Ok(())
    }

    #[inline(always)]
    pub fn persist_instance_uuid(&self, instance_uuid: &str) -> tarantool::Result<()> {
        self.state_for_write()?
            .insert(&("instance_uuid", instance_uuid))?;
        Ok(())
    }

    #[inline(always)]
    pub fn persist_tier(&self, tier: &str) -> tarantool::Result<()> {
        self.state_for_write()?.insert(&("tier", tier))?;
        Ok(())
    }

    #[inline(always)]
    pub fn persist_cluster_name(&self, cluster_name: &str) -> tarantool::Result<()> {
        self.state_for_write()?
            .insert(&("cluster_name", cluster_name))?;
        Ok(())
    }

    #[inline(always)]
    pub fn persist_cluster_uuid(&self, cluster_uuid: &str) -> tarantool::Result<()> {
        self.state_for_write()?
            .insert(&("cluster_uuid", cluster_uuid))?;
        Ok(())
    }

    #[inline(always)]
    pub fn persist_gen(&self, gen: u64) -> tarantool::Result<()> {
        self.state_for_write()?.replace(&("gen", gen))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_term(&self, term: RaftTerm) -> tarantool::Result<()> {
        self.state_for_write()?.replace(&("term", term))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_vote(&self, vote: RaftId) -> tarantool::Result<()> {
        self.state_for_write()?.replace(&("vote", vote))?;
        Ok(())
    }

    #[inline(always)]
    pub fn persist_commit(&self, commit: RaftId) -> tarantool::Result<()> {
        self.state_for_replayable_write()
            .replace(&("commit", commit))?;
        metrics::record_raft_commit_index(commit);
        Ok(())
    }

    #[inline(always)]
    pub fn persist_applied(&self, index: RaftId, term: RaftTerm) -> tarantool::Result<()> {
        self.state_for_replayable_write()
            .replace(&("applied", index))?;
        self.state_for_replayable_write()
            .replace(&("applied_term", term))?;
        metrics::record_raft_applied_index(index);
        Ok(())
    }

    #[inline(always)]
    fn persist_voters(&self, voters: &[RaftId]) -> tarantool::Result<()> {
        self.state_for_replayable_write()
            .replace(&("voters", voters))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_learners(&self, learners: &[RaftId]) -> tarantool::Result<()> {
        self.state_for_replayable_write()
            .replace(&("learners", learners))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_voters_outgoing(&self, voters_outgoing: &[RaftId]) -> tarantool::Result<()> {
        self.state_for_replayable_write()
            .replace(&("voters_outgoing", voters_outgoing))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_learners_next(&self, learners_next: &[RaftId]) -> tarantool::Result<()> {
        self.state_for_replayable_write()
            .replace(&("learners_next", learners_next))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_auto_leave(&self, auto_leave: bool) -> tarantool::Result<()> {
        self.state_for_replayable_write()
            .replace(&("auto_leave", auto_leave))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_compacted_term(&self, compacted_term: RaftTerm) -> tarantool::Result<()> {
        self.state_for_write()?
            .replace(&("compacted_term", compacted_term))?;
        Ok(())
    }

    #[inline(always)]
    pub fn persist_join_state(&self, join_state: String) -> tarantool::Result<()> {
        self.state_for_write()?
            .replace(&("join_state", join_state))?;
        Ok(())
    }

    #[inline(always)]
    fn persist_compacted_index(&self, compacted_index: RaftTerm) -> tarantool::Result<()> {
        self.state_for_write()?
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
        debug_assert!(tarantool::transaction::is_in_transaction());
        self.persist_voters(&cs.voters)?;
        self.persist_learners(&cs.learners)?;
        self.persist_voters_outgoing(&cs.voters_outgoing)?;
        self.persist_learners_next(&cs.learners_next)?;
        self.persist_auto_leave(cs.auto_leave)?;
        Ok(())
    }

    /// Whether the term or the vote in `hs` differs from the stored ones.
    /// Only their change makes the persist transaction durable, and the
    /// same method drives that decision in `NodeImpl::advance`, so the
    /// two cannot drift apart.
    pub fn term_or_vote_changed(&self, hs: &raft::HardState) -> tarantool::Result<bool> {
        let changed = |key: &str, value: u64| -> tarantool::Result<bool> {
            Ok(self.try_get_raft_state::<u64>(key)? != Some(value))
        };
        Ok(changed("term", hs.term)? || changed("vote", hs.vote)?)
    }

    /// Persists the `HardState` (term, vote, and commit index).
    ///
    /// `term_vote_changed` must be the decision of
    /// [`Self::term_or_vote_changed`] for this hard state: only their
    /// change is written, and the same decision picks the transaction
    /// kind in `NodeImpl::advance`. The check runs once in `advance`
    /// and is passed down, so the raft hot path does not read the
    /// stored term and vote twice per ready.
    ///
    /// # Panics
    ///
    /// In debug mode panics if invoked out of a transaction.
    pub fn persist_hard_state(
        &self,
        hs: &raft::HardState,
        term_vote_changed: bool,
    ) -> tarantool::Result<()> {
        debug_assert!(tarantool::transaction::is_in_transaction());
        // The term is recorded here, on change; the metric below tracks
        // it on every hard state regardless.
        if term_vote_changed {
            self.persist_term(hs.term)?;
            self.persist_vote(hs.vote)?;
        }
        self.persist_commit(hs.commit)?;
        metrics::record_raft_term(hs.term);
        Ok(())
    }

    /// Persists a slice of entries durably.
    ///
    /// Use it for the entries which must be durable regardless of
    /// `wal_mode`: bootstrap and join. The persist transaction of a raft [`raft::Ready`] picks
    /// between this function and [`Self::persist_entries_replayable`],
    /// because the log entries follow `wal_mode`.
    ///
    /// # Panics
    ///
    /// Panics if one `raft::Entry` of provided fails to be converted
    /// into `traft::Entry`.
    pub fn persist_entries(&self, entries: &[raft::Entry]) -> tarantool::Result<()> {
        self.persist_entries_via(self.log_for_write()?, entries)
    }

    /// Same as [`Self::persist_entries`], but for a transaction opened by
    /// [`transaction_raft_replayable`]: for data that may be lost on a host
    /// crash but can be replayed. Most commonly this is the entry that is durable
    /// in the log but its effect is not, so it is applied and the applied
    /// index is written in the same replayable transaction so it is lost together
    /// with the effect and the entry is applied again on restart. For details see
    /// `transaction_raft_replayable`.
    pub fn persist_entries_replayable(&self, entries: &[raft::Entry]) -> tarantool::Result<()> {
        self.persist_entries_via(self.log_for_replayable_write(), entries)
    }

    fn persist_entries_via(&self, log: &Space, entries: &[raft::Entry]) -> tarantool::Result<()> {
        let err_with_ctx = |error, entry| format!("{error}: {entry:?}");
        for entry in entries {
            // FIXME: this deserialize -> serialize step is redundant
            let row = traft::Entry::try_from(entry)
                .map_err(|error| err_with_ctx(error, entry))
                .unwrap();
            log.replace(&row)?;
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
        let meta_index = meta.index;

        #[cfg(debug_assertions)]
        let applied = self.applied()?;
        #[cfg(debug_assertions)]
        #[rustfmt::skip]
        debug_assert!(meta_index >= applied, "meta_index: {meta_index}, applied: {applied}");
        // Must bump the applied index before doing log compaction,
        // because log compaction only removes applied entries
        self.persist_applied(meta_index, meta.term)?;
        self.persist_conf_state(meta.conf_state())?;

        // We don't want to have a hole in the log, so we clear everything
        // before applying the snapshot
        self.compact_log(meta.index + 1)?;

        let compacted_index = self.compacted_index()?;
        #[rustfmt::skip]
        debug_assert!(meta_index >= compacted_index, "meta_index: {meta_index}, compacted_index: {compacted_index}");
        // We must set these explicitly, because compact_log only sets them to
        // the coordinates of the last entry which was in our log.
        self.persist_compacted_term(meta.term)?;
        self.persist_compacted_index(meta.index)?;
        Ok(())
    }

    /// Returns the total size in bytes the tuples of _raft_log system space are
    /// occupying. This **DOES NOT** include memory used for the index.
    #[inline]
    pub fn raft_log_bsize(&self) -> tarantool::Result<u64> {
        // SAFETY: tarantool api is only safe to be called from tx thread.
        // Also the pointer to the space struct is not held across yields, so
        // there's no possibility of use after free.
        let space_size = unsafe {
            let space = space_by_id(self.space_raft_log.id());
            assert!(!space.is_null(), "space _raft_log must not be dropped");
            space_bsize(space)
        };

        return Ok(space_size as _);

        extern "C" {
            /// Try to look up a space by space number in the space cache.
            /// FFI-friendly no-exception-thrown space lookup function.
            ///
            /// Return NULL if space not found, otherwise space object.
            ///
            /// # Safety
            /// The caller must make sure not to hold on to the pointer for too
            /// long as the space object may deleted at some point after which
            /// the derefencing the pointer will be **undefined behavior**.
            fn space_by_id(id: u32) -> *mut box_space;

            /// Returns number of bytes used in memory by tuples in the space.
            fn space_bsize(space: *mut box_space) -> usize;
        }

        #[repr(C)]
        struct box_space {
            unused: [u8; 0],
        }
    }

    /// Returns the number of tuples currently stored in _raft_log system space.
    #[inline(always)]
    pub fn raft_log_count(&self) -> tarantool::Result<u64> {
        let count = self.space_raft_log.len()?;
        Ok(count as _)
    }

    /// Trims raft log up to the given index `up_to` (excluding the index
    /// itself).
    ///
    /// Note that `up_to` must not be greater than current applied index + 1,
    /// because it doesn't make sense to compact entries which haven't been
    /// applied yet.
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
    /// In debug mode panics in case:
    /// - invoked outside of a transaction
    /// - `up_to > applied + 1`
    ///
    pub fn compact_log(&self, up_to: RaftIndex) -> tarantool::Result<u64> {
        debug_assert!(tarantool::transaction::is_in_transaction());

        // We cannot drop entries, which weren't applied yet
        #[cfg(debug_assertions)]
        let applied = self.applied()?;
        #[cfg(debug_assertions)]
        debug_assert!(up_to <= applied + 1, "up_to: {up_to}, applied: {applied}");

        // IteratorType::LT means tuples are returned in descending order
        let mut iter = self.space_raft_log.select_v2(IteratorType::LT, &(up_to,))?;

        let Some(tuple) = iter.next() else {
            let compacted_index = self.compacted_index()?;
            return Ok(1 + compacted_index);
        };

        let tuple = tuple?;
        let compacted_index = tuple
            .field::<RaftIndex>(Self::FIELD_ENTRY_INDEX)?
            .expect("index is non-nullable");
        let compacted_term = tuple
            .field::<RaftTerm>(Self::FIELD_ENTRY_TERM)?
            .expect("term is non-nullable");
        // The compaction is replayable: a crash which loses it loses it
        // together with the entries it removed, so the log simply stays
        // longer and is compacted again. The compacted indexes are
        // written through the replayable accessor directly - the shared
        // persist_compacted_* helpers are durable for the sake of the
        // snapshot path.
        let log = self.log_for_replayable_write();
        log.delete(&(compacted_index,))?;
        self.state_for_replayable_write()
            .replace(&("compacted_index", compacted_index))?;
        self.state_for_replayable_write()
            .replace(&("compacted_term", compacted_term))?;

        let mut num_deleted = 1;
        for tuple in iter {
            let index = tuple?
                .field::<RaftIndex>(Self::FIELD_ENTRY_INDEX)?
                .expect("index is non-nullable");
            log.delete(&(index,))?;
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
    ///   entry for `compacted_index` doesn't exist, despite its term is
    ///   known.
    ///
    /// - `Err(Unavailable)` in case at least one entry in the range is
    ///   missing (either `high > last_index` or it's really missing).
    ///   Raft-rs will panic in this case, but it's fair.
    ///
    /// - Arguments - `context` sets entry fetching mode (sync/async)
    ///   Currently, only sync fetching is supported
    ///   `context` is ignored and by default: `GetEntriesContext::empty(false)`
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
        limit: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> Result<Vec<raft::Entry>, RaftError> {
        if low <= self.compacted_index().cvt_err()? {
            return Err(RaftError::Store(StorageError::Compacted));
        }

        let limit = limit.into().map(|l| l as usize);
        let ret: Vec<traft::Entry> = self.entries(low, high, limit).cvt_err()?;
        if ret.len() < limit.unwrap_or(usize::MAX).min((high - low) as usize) {
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

    /// Returns the last index for which the term is available.
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

    /// Currently we don't use `_to` parameter
    /// because it is ignored in the raft-rs(0-7-0) implementation.
    fn snapshot(&self, idx: RaftIndex, _to: u64) -> Result<raft::Snapshot, RaftError> {
        let applied = self.applied_entry_id().cvt_err()?;
        if applied.index < idx {
            crate::warn_or_panic!(
                "requested snapshot for index {idx} greater than applied {}",
                applied.index,
            );
            return Err(StorageError::SnapshotTemporarilyUnavailable.into());
        }

        let compacted_index = self.compacted_index().cvt_err()?;

        let node = crate::traft::node::global()
            .map_err(tarantool::error::Error::from)
            .cvt_err()?;
        let parameters = &node.alter_system_parameters;

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
        let (data, entry_id) = node
            .storage
            .first_snapshot_data_chunk(applied, compacted_index, parameters)
            .cvt_err()?;

        let mut snapshot = raft::Snapshot::new();
        let meta = snapshot.mut_metadata();
        meta.index = entry_id.index;
        meta.term = entry_id.term;
        // XXX: make sure this conf state corresponds to the snapshot data.
        *meta.mut_conf_state() = self.conf_state().cvt_err()?;

        let data = data.to_tuple_buffer().cvt_err()?;
        snapshot.set_data(Vec::from(data));

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

    fn dummy_entry(index: RaftIndex, term: RaftTerm) -> raft::Entry {
        raft::Entry {
            term,
            index,
            ..Default::default()
        }
    }

    /// Make sure `TXN_SYNC_WAL` matches tarantool constant
    #[::tarantool::test]
    fn test_sync_wal_flag_matches_tarantool() {
        let lua = ::tarantool::lua_state();
        let expected: u32 = lua
            .eval("return box.internal.TXN_SYNC_WAL")
            .expect("tarantool must publish the flag value");

        transaction_raft(|| -> tarantool::Result<()> {
            let flags = unsafe { ::tarantool::ffi::tarantool::box_txn_flags() };
            assert_eq!(
                flags & expected,
                expected,
                "transaction_raft must set TXN_SYNC_WAL (0x{expected:x}), \
                 got flags 0x{flags:x}",
            );
            Ok(())
        })
        .unwrap();
    }

    /// The replayable writes are not flushed to stable storage: the
    /// transaction is force async, like [`transaction_raft`], but does
    /// not sync.
    #[::tarantool::test]
    fn test_replayable_txn_does_not_sync() {
        let lua = ::tarantool::lua_state();
        let sync_wal: u32 = lua
            .eval("return box.internal.TXN_SYNC_WAL")
            .expect("tarantool must publish the flag value");
        let force_async: u32 = lua
            .eval("return box.internal.TXN_FORCE_ASYNC")
            .expect("tarantool must publish the flag value");

        transaction_raft_replayable(|| -> tarantool::Result<()> {
            let flags = unsafe { ::tarantool::ffi::tarantool::box_txn_flags() };
            assert_eq!(flags & sync_wal, 0, "got flags 0x{flags:x}");
            assert_eq!(
                flags & force_async,
                force_async,
                "the replayable transaction must be force async, \
                 got flags 0x{flags:x}"
            );
            Ok(())
        })
        .unwrap();
    }

    /// A hard state that only advances the commit index is persisted without
    /// a sync, see `raft::Ready::must_sync`.
    #[::tarantool::test]
    fn test_commit_only_hard_state_needs_no_sync() {
        let storage = RaftSpaceAccess::new().unwrap();
        let hs = |term, vote, commit| raft::HardState {
            term,
            vote,
            commit,
            ..Default::default()
        };
        transaction_raft(|| storage.persist_hard_state(&hs(2, 1, 10), true)).unwrap();

        // Would trip the durability assertion if it rewrote the term or vote.
        transaction_raft_replayable(|| storage.persist_hard_state(&hs(2, 1, 11), false)).unwrap();
        assert_eq!(storage.term().unwrap(), 2);
        assert_eq!(storage.commit().unwrap(), 11);
    }

    /// A change of the term or the vote outside [`transaction_raft`] fails
    /// errors out instead of silently skipping the sync.
    #[::tarantool::test]
    fn test_changed_term_or_vote_requires_durable_txn() {
        let storage = RaftSpaceAccess::new().unwrap();
        let hs = raft::HardState {
            term: 3,
            vote: 2,
            commit: 1,
            ..Default::default()
        };
        assert_err_starts_with!(
            transaction_raft_replayable(|| storage.persist_hard_state(&hs, true)),
            concat!(
                "transaction rolled-back: ",
                "raft storage must only be modified inside transaction_raft()",
            )
        );
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
        transaction_raft(|| -> tarantool::Result<()> {
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

        assert_eq!(
            S::entries(
                &storage,
                100,
                100,
                u64::MAX,
                GetEntriesContext::empty(false)
            ),
            Ok(vec![])
        );

        // Part 3. Add some new entries.
        let test_entries = vec![
            dummy_entry(100, 10),
            dummy_entry(101, 10),
            dummy_entry(102, 10),
        ];
        transaction_raft(|| storage.persist_entries(&test_entries)).unwrap();

        let (first, last) = (100, 102);
        assert_eq!(S::first_index(&storage), Ok(first));
        assert_eq!(S::last_index(&storage), Ok(last));

        assert_eq!(S::term(&storage, first - 1), Ok(9));
        assert_eq!(S::term(&storage, first), Ok(10));
        assert_eq!(S::term(&storage, last), Ok(10));
        assert_err!(S::term(&storage, last + 1), "log unavailable");

        assert_err!(
            S::entries(
                &storage,
                first - 1,
                last + 1,
                u64::MAX,
                GetEntriesContext::empty(false)
            ),
            "log compacted"
        );

        assert_err!(
            S::entries(
                &storage,
                first,
                last + 2,
                u64::MAX,
                GetEntriesContext::empty(false)
            ),
            "log unavailable"
        );

        // `first, last + 1` is what raft-rs literally does, see [@github].
        //
        // [@github]: https://github.com/tikv/raft-rs/blob/v0.6.0/src/raft_log.rs#L388
        //
        assert_eq!(
            S::entries(
                &storage,
                first,
                last + 1,
                u64::MAX,
                GetEntriesContext::empty(false)
            ),
            Ok(test_entries)
        );

        // TEMP: assert that we have enough information to debug on panic.
        // NOTE: see <https://git.picodata.io/core/picodata/-/issues/2646#note_198362>.
        let _ = std::panic::take_hook();
        let panicking_raft_entry = std::panic::catch_unwind(|| {
            transaction_raft(|| {
                storage.persist_entries(&[raft::Entry {
                    term: 103,
                    index: 10,
                    context: vec![0x00, 0x00, 0x00, 0x00],
                    ..Default::default()
                }])
            })
        });
        let error = panicking_raft_entry.unwrap_err();
        let msg = error.downcast_ref::<String>().unwrap();
        assert!(msg.contains("failed to decode tuple"));
        assert!(msg.contains("data did not match any variant of untagged enum EntryContext"));
        assert!(msg.contains("Entry {"));
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
            S::entries(
                &storage,
                first,
                last + 1,
                u64::MAX,
                GetEntriesContext::empty(false)
            ),
            "unknown error failed to decode tuple: unknown entry type (1337)"
        );

        raft_log.put(&(0, first, term, "", false)).unwrap();
        assert_err_starts_with!(
            S::entries(
                &storage,
                first,
                last + 1,
                u64::MAX,
                GetEntriesContext::empty(false),
            ),
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
        assert_err!(
            S::entries(&storage, 1, 4, u64::MAX, GetEntriesContext::empty(false),),
            "log unavailable"
        );

        raft_log.primary_key().drop().unwrap();
        assert_err_starts_with!(
            S::entries(
                &storage,
                first,
                last + 1,
                u64::MAX,
                GetEntriesContext::empty(false),
            ),
            concat!(
                "unknown error",
                " box error:",
                " NoSuchIndexID:",
                " No index #0 is defined in space '_raft_log'"
            )
        );

        raft_log.drop().unwrap();
        assert_err_starts_with!(
            S::entries(
                &storage,
                first,
                last + 1,
                u64::MAX,
                GetEntriesContext::empty(false),
            ),
            format!(
                concat!(
                    "unknown error",
                    " box error:",
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
        let applied = 8;
        transaction_raft(|| -> tarantool::Result<()> {
            for i in first..=last {
                storage.persist_entries(&[dummy_entry(i, i)])?;
            }
            storage.persist_applied(applied, 1)
        })
        .unwrap();
        let entries =
            |lo, hi| S::entries(&storage, lo, hi, u64::MAX, GetEntriesContext::empty(false));
        let compact_log = |up_to| transaction_raft(|| storage.compact_log(up_to));

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

        // cannot compact past applied, this call will panic
        // Note that we could use `#[tarantool::test(should_panic)]` but in this
        // case a panic would happen inside a tarantool transaction, and
        // unwinding from within it will leave an unfinished transaction, which
        // we don't want to have
        // _ = compact_log(applied + 2);
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
        transaction_raft(|| storage.persist_hard_state(&hs, true)).unwrap();
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

        transaction_raft(|| storage.persist_conf_state(&cs)).unwrap();
        assert_eq!(S::initial_state(&storage).unwrap().conf_state, cs);
    }

    #[::tarantool::test]
    fn test_other_state() {
        let storage = RaftSpaceAccess::new().unwrap();
        let raft_state = Space::find(RaftSpaceAccess::SPACE_RAFT_STATE).unwrap();

        transaction_raft(|| storage.persist_cluster_name("test_cluster")).unwrap();
        transaction_raft(|| storage.persist_cluster_name("test_cluster_2")).unwrap_err();
        assert_eq!(storage.cluster_name().unwrap(), "test_cluster".to_string());

        raft_state.delete(&("id",)).unwrap();
        assert_eq!(storage.raft_id().unwrap(), None);

        transaction_raft(|| storage.persist_raft_id(16)).unwrap();
        assert_err!(
            transaction_raft(|| storage.persist_raft_id(32)),
            concat!(
                // transaction_raft wraps the inner error
                "transaction rolled-back:",
                " box error:",
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
                concat!("box error:", " NoSuchSpace:", " Space '{}' does not exist",),
                storage.space_raft_state.id()
            )
        );
    }
}
