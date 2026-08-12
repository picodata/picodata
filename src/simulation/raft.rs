use crate::cas;
use crate::schema::ADMIN_ID;
use crate::simulation::cluster::PretendCluster;
#[allow(unused_imports)]
use crate::simulation::fiber::FiberState;
use crate::storage::Catalog;
use crate::traft::error::Error;
use crate::traft::op::Dml;
use crate::traft::op::Op;
use crate::traft::RaftIndex;
use crate::Result;
use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;

/// Committed entries of the cluster's raft log.
/// Shared between all instances of the [`PretendCluster`].
///
/// Note that the `persist -> replicate -> acknowledge -> commit` part of the
/// raft algorithm is not currently moddelled in the simulation engine. Once the
/// CAS request passes the predicate check it is instantly committed to the log
/// and every instance in the cluster then has to apply that entry to it's local
/// state separately.
pub struct PretendRaftLog {
    pub committed: RefCell<Vec<CommittedEntry>>,
}

/// One committed batch and the raft index it got.
#[derive(Clone)]
pub struct CommittedEntry {
    pub index: RaftIndex,
    pub dmls: Rc<[Dml]>,
}

impl PretendRaftLog {
    pub fn new() -> Self {
        Self {
            committed: RefCell::new(Vec::new()),
        }
    }

    /// Appends `dmls` as one committed batch and returns the index it got.
    pub fn commit(&self, dmls: Vec<Dml>) -> RaftIndex {
        let index = self.next_index();
        self.committed.borrow_mut().push(CommittedEntry {
            index,
            dmls: dmls.into(),
        });
        index
    }

    /// Index of the last committed entry, or 0 if there are none.
    pub fn last_index(&self) -> RaftIndex {
        self.committed.borrow().last().map_or(0, |e| e.index)
    }

    /// The index the next committed entry will get.
    pub fn next_index(&self) -> RaftIndex {
        self.last_index() + 1
    }

    /// Every entry from `index` onwards, in order.
    pub fn entries_from(&self, index: RaftIndex) -> Vec<CommittedEntry> {
        // First entry in log has index `1`.
        let i = (index - 1) as usize;
        let committed = self.committed.borrow();
        let Some(entries) = committed.get(i..) else {
            return vec![];
        };

        let entries = entries.to_vec();
        // Slicing at exactly `len` is legal and yields nothing, which is what
        // a proposer already at the tip gets.
        if let Some(first) = entries.first() {
            assert_eq!(first.index, index);
        }
        entries
    }

    /// The one entry an instance at `index` applies next, if any.
    pub fn entry(&self, index: RaftIndex) -> Option<CommittedEntry> {
        // First entry in log has index `1`.
        let i = (index - 1) as usize;
        let entry = self.committed.borrow().get(i)?.clone();
        assert_eq!(entry.index, index);
        Some(entry)
    }
}

////////////////////////////////////////////////////////////////////////////////
// PretendCasRequest
////////////////////////////////////////////////////////////////////////////////

/// One CAS proposal an instance has made and nobody has decided on yet.
pub struct PretendCasRequest {
    /// Applied index of the index who made the request.
    pub index: RaftIndex,

    pub dmls: Vec<Dml>,

    /// Where the scheduler writes the decision, and the proposer reads it
    /// (see [`FiberState::WaitCasOutcome`]).
    pub outcome: CasOutcomeSlot,
}

/// "The fate of this one CAS proposal, once it has one", shared between the
/// proposing fiber and the scheduler.
pub type CasOutcomeSlot = Rc<Cell<Option<PretendCasOutcome>>>;

/// See [`resolve_cas_request`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PretendCasOutcome {
    /// Passed the predicate check and committed to [`PretendRaftLog`] at `index`.
    Committed { index: RaftIndex },

    /// CAS predicate check failed.
    ConflictFound { conflict_index: RaftIndex },

    /// Fault injection: CAS request didn't reach the raft leader.
    Dropped,
}

pub fn check_cas_predicate(
    request: &cas::Request,
    raft_log: &PretendRaftLog,
    storage: &Catalog,
) -> Result<()> {
    let mut predicates = cas::CasPredicates::default();
    predicates.extend_ranges(request.predicate.ranges.iter().cloned());
    predicates.extend_ranges(cas::Range::for_op(&request.op)?);

    let requested = request.predicate.index;
    for entry in raft_log.entries_from(requested + 1) {
        let entry_op = Op::single_dml_or_batch(entry.dmls.to_vec());
        cas::check_predicate(entry.index, &entry_op, &predicates, storage)?;
    }

    Ok(())
}

pub fn pretend_cas_request(index: RaftIndex, dmls: &[Dml]) -> cas::Request {
    cas::Request {
        cluster_name: "simulation".into(),
        predicate: cas::Predicate {
            index,
            term: 1,
            ranges: vec![],
        },
        op: Op::single_dml_or_batch(dmls.into()),
        as_user: ADMIN_ID,
    }
}

/// Takes the first [`PretendCasRequest`] from the given instance's request queue
/// and decides it's fate.
pub fn resolve_cas_request(
    cluster: &PretendCluster,
    instance_name: &str,
    should_drop: bool,
) -> PretendCasOutcome {
    let instance = cluster.instance(instance_name);
    let request = instance
        .cas_outbox
        .borrow_mut()
        .pop_front()
        .unwrap_or_else(|| {
            panic!(
                "simulation: [{}] has no outstanding CAS request to resolve",
                instance.name,
            )
        });

    let outcome_slot = request.outcome.clone();
    // A dropped proposal never reaches the leader, so nothing decides it.
    let outcome = if should_drop {
        PretendCasOutcome::Dropped
    } else {
        decide_cas_request(cluster, request)
    };
    outcome_slot.set(Some(outcome));
    outcome
}

/// The decision itself, split out of [`resolve_cas_request`] so the
/// proposal can be consumed by whichever fate it gets.
pub fn decide_cas_request(
    cluster: &PretendCluster,
    request: PretendCasRequest,
) -> PretendCasOutcome {
    // Only ever reached by a proposal which got to the leader, so from
    // here on the verdict is the leader's alone.
    //
    // The most applied instance stands in for the leader's committed
    // state, which is whose storage a real leader checks against.
    let storage = cluster.most_applied_instance();
    let res = check_cas_predicate(
        &pretend_cas_request(request.index, &request.dmls),
        &cluster.raft_log,
        &storage.catalog,
    );
    match res {
        Err(Error::Cas(cas::Error::ConflictFound { conflict_index })) => {
            PretendCasOutcome::ConflictFound { conflict_index }
        }
        Err(error) => panic!(
            "simulation: the tail check can only reject a proposal by finding a \
             conflict, got {error}"
        ),
        Ok(()) => PretendCasOutcome::Committed {
            index: cluster.raft_log.commit(request.dmls),
        },
    }
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

mod tests {
    use super::*;
    use crate::catalog::pico_bucket::BucketRecord;
    use crate::catalog::pico_bucket::PicoBucket;
    use crate::storage::SystemTable as _;

    #[::tarantool::test]
    fn test_check_cas_predicate() {
        let catalog = Catalog::for_tests();
        let raft_log = PretendRaftLog::new();

        let committed = BucketRecord::new_active("storage".into(), 1, 100, "r1".into());
        let other = BucketRecord::new_active("storage".into(), 101, 200, "r1".into());
        let applied = raft_log.commit(vec![PicoBucket::dml_insert(&committed)]);

        // A proposal which has already seen that entry doesn't check it at all.
        let dmls = [PicoBucket::dml_delete(&committed.primary_key())];
        check_cas_predicate(&pretend_cas_request(applied, &dmls), &raft_log, &catalog).unwrap();

        // One made before it and touching the same row conflicts with it, ...
        let err =
            check_cas_predicate(&pretend_cas_request(0, &dmls), &raft_log, &catalog).unwrap_err();
        let Error::Cas(cas::Error::ConflictFound { conflict_index }) = err else {
            panic!("expected a CAS conflict, got {err}");
        };
        assert_eq!(conflict_index, applied as RaftIndex);

        // ... one touching a different row doesn't.
        let dmls = [PicoBucket::dml_delete(&other.primary_key())];
        check_cas_predicate(&pretend_cas_request(0, &dmls), &raft_log, &catalog).unwrap();
    }
}
