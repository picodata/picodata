use crate::simulation::instance::PretendInstance;
use crate::vshard::VshardBucketRecord;
use crate::Result;
use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;
use tarantool::error::BoxError;
use tarantool::error::TarantoolErrorCode;

/// One replicaset's sharded-replication WAL, shared by every [`PretendInstance`].
#[derive(Default)]
pub struct ShardedWal {
    pub entries: RefCell<Vec<ShardedEntry>>,
}

impl ShardedWal {
    pub fn new() -> Self {
        Self {
            entries: RefCell::new(Vec::new()),
        }
    }

    pub fn push(&self, entry: ShardedEntry) {
        self.entries.borrow_mut().push(entry);
    }

    pub fn len(&self) -> usize {
        self.entries.borrow().len()
    }

    pub fn entry(&self, index: usize) -> ShardedEntry {
        self.entries.borrow()[index].clone()
    }
}

/// One logical sharded-data write a replicaset master performs, appended to
/// its replicaset's [`ShardedWal`].
#[derive(Clone, Debug)]
pub enum ShardedEntry {
    WriteBuckets { recs: Vec<VshardBucketRecord> },
}

/// One transaction an instance has applied to its own memory and whose WAL
/// entry hasn't persisted yet.
///
/// See also [`PretendInstance::unstable_wal`].
pub struct WalWrite {
    /// The entry which will be added to the [`ShardedWal`].
    pub entry: ShardedEntry,

    /// For the purposes of transaction rollback simulation.
    pub undo: WalUndo,

    /// Where the scheduler writes the decision, and the writing fiber reads it
    /// (see [`FiberState::WaitWalWrite`](crate::simulation::fiber::FiberState::WaitWalWrite)).
    pub outcome: WalOutcomeSlot,
}

/// The pre-image of everything one [`WalWrite`] touched, captured as it was
/// applied.
///
/// See also [`undo_sharded_statement`].
///
/// `buckets` is in *application* order and holds one entry per write, including
/// repeats of the same bucket.
pub struct WalUndo {
    pub buckets: Vec<(u64, Option<VshardBucketRecord>)>,
}

pub type WalOutcomeSlot = Rc<Cell<Option<WalOutcome>>>;

/// What [`PretendAction::CommitWalWrite`](crate::simulation::action::PretendAction::CommitWalWrite)
/// decided about one [`WalWrite`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalOutcome {
    /// The entry reached the disk. Its `on_commit` triggers run, it joins the
    /// replicaset's [`ShardedWal`], and the writing fiber's `transaction(..)`
    /// returns `Ok`.
    Persisted,

    /// The WAL write failed, so the transaction is rolled back and the writer
    /// gets `ER_WAL_IO`.
    ///
    /// Only ever drawn while
    /// [`PretendCluster::disk_write_fault_probability`](crate::simulation::cluster::PretendCluster::disk_write_fault_probability)
    /// is nonzero.
    Failed,
}

////////////////////////////////////////////////////////////////////////////////
// WAL simulation
////////////////////////////////////////////////////////////////////////////////

/// Performs one sharded write as this replicaset's master:
///
/// 1. the statements are applied to local memory
///    ([`apply_sharded_statement`]);
/// 2. the fiber waits for the WAL entry to persist
/// 3. on commit the entry joins the replicaset's [`PretendInstance::sharded_wal`],
///    where replication reads it from ([`commit_sharded_entry`]).
///
/// A failed WAL write rolls step 1 back and reports `ER_WAL_IO`.
pub fn write_sharded_entry(instance: &PretendInstance, entry: ShardedEntry) -> Result<()> {
    let fiber = instance.current_fiber()?;

    let undo = apply_sharded_statement(instance, &entry);
    let outcome: WalOutcomeSlot = Rc::new(Cell::new(None));
    instance.unstable_wal.borrow_mut().push_back(WalWrite {
        entry,
        undo,
        outcome: outcome.clone(),
    });

    match fiber.wait_for_wal_outcome(&outcome)? {
        WalOutcome::Persisted => Ok(()),
        WalOutcome::Failed => Err(BoxError::new(
            TarantoolErrorCode::WalIo,
            "simulation: failed to write to disk",
        )
        .into()),
    }
}

/// The commit half of [`write_sharded_entry`], run by
/// [`PretendAction::CommitWalWrite`](crate::simulation::action::PretendAction::CommitWalWrite)
/// once the entry has persisted.
pub fn commit_sharded_entry(instance: &PretendInstance, entry: ShardedEntry) {
    // Only well-defined for a master which has applied everything before
    // it, which the clean-switchover rule guarantees. Checked here rather
    // than when the transaction started, because here is where the entry
    // takes its place in the order.
    assert_eq!(
        instance.sharded_lsn.get(),
        instance.sharded_wal.len(),
        "[{}] master committing while behind its replicaset's sharded WAL",
        instance.name
    );
    instance.sharded_wal.push(entry);
    instance.sharded_lsn.set(instance.sharded_lsn.get() + 1);
}

/// Applies the next entry of [`PretendInstance::sharded_wal`] and advances
/// [`PretendInstance::sharded_lsn`] past it: how a *replica*'s local sharded state
/// changes, a master having applied its own writes as it made them.
///
/// Both halves at once, unlike the master. A replica's applier fiber
/// writes its own WAL too.
pub fn apply_next_sharded_entry(instance: &PretendInstance) {
    let lsn = instance.sharded_lsn.get();
    assert!(
        lsn < instance.sharded_wal.len(),
        "[{}] nothing to apply at sharded lsn {lsn}",
        instance.name
    );
    let entry = instance.sharded_wal.entry(lsn);
    apply_sharded_statement(instance, &entry);
    instance.sharded_lsn.set(lsn + 1);
}

/// Applies the *statement* half of a sharded entry: what memtx does
/// inside `space:replace()` itself, before the transaction commits.
///
/// Picodata runs memtx without MVCC, so the tuple is already in the index and
/// visible to every other fiber of this instance while the writing
/// fiber is still parked on its WAL entry. The commit half is
/// [`commit_sharded_entry`].
///
/// Returns what it takes to put the local state back, for the WAL-failure
/// path.
pub fn apply_sharded_statement(instance: &PretendInstance, entry: &ShardedEntry) -> WalUndo {
    let mut buckets = instance.local_buckets.borrow_mut();
    let mut undo = WalUndo {
        buckets: Vec::new(),
    };
    match entry {
        ShardedEntry::WriteBuckets { recs } => {
            for rec in recs {
                let pre_image = buckets.insert(rec.bucket_id, rec.clone());
                undo.buckets.push((rec.bucket_id, pre_image));
            }
        }
    }
    undo
}

/// Loses every transaction which hadn't reached the disk, rolling back
/// the statements each had applied to local memory.
///
/// This is called when crashing an instance.
///
/// Entries dropped in reverse order.
pub fn lose_unstable_wal(instance: &PretendInstance) {
    let writes: Vec<_> = instance.unstable_wal.borrow_mut().drain(..).collect();
    for write in writes.into_iter().rev() {
        undo_sharded_statement(instance, write.undo);
    }
}

pub fn undo_sharded_statement(instance: &PretendInstance, undo: WalUndo) {
    let mut buckets = instance.local_buckets.borrow_mut();
    for (bucket_id, pre_image) in undo.buckets.into_iter().rev() {
        match pre_image {
            Some(rec) => buckets.insert(bucket_id, rec),
            None => buckets.remove(&bucket_id),
        };
    }
}
