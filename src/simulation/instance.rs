use crate::cas;
use crate::config::AlterSystemParameters;
use crate::config::AlterSystemParametersRef;
use crate::instance::InstanceName;
use crate::resharding_loop::ReshardingLoopState;
use crate::resharding_loop::ReshardingStatus;
#[allow(unused_imports)]
use crate::simulation::cluster::PretendCluster;
use crate::simulation::fiber::simulation_fiber_is_stopped;
use crate::simulation::fiber::FiberState;
use crate::simulation::fiber::PretendFiber;
use crate::simulation::fiber::WaitOutcome;
use crate::simulation::platform::Platform;
use crate::simulation::raft::CasOutcomeSlot;
use crate::simulation::raft::PretendCasOutcome;
use crate::simulation::raft::PretendCasRequest;
#[allow(unused_imports)]
use crate::simulation::raft::PretendRaftLog;
use crate::simulation::sharded_wal::write_sharded_entry;
use crate::simulation::sharded_wal::ShardedEntry;
use crate::simulation::sharded_wal::ShardedWal;
use crate::simulation::sharded_wal::WalWrite;
use crate::storage::Catalog;
use crate::topology_cache::TopologyCache;
use crate::traft::error::Error;
use crate::traft::op::Dml;
use crate::traft::RaftIndex;
use crate::util::NoYieldsRefCell;
use crate::vshard::VshardBucketRecord;
use crate::vshard::VshardBucketState;
use crate::Result;
use ::tarantool::fiber::r#async::watch;
use ::tarantool::fiber::FiberId;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;
#[allow(unused_imports)]
use tarantool::space::SpaceType;

/// One simulated instance: its state, its fibers, and the [`Platform`] impl.
///
/// Every [`Platform`] method borrows this instance's state briefly and
/// never holds a borrow across a park.
///
/// Fully isolated from every other instance; the only state it shares is
/// [`PretendRaftLog`] (shared between all instances of cluster)
/// and the [`ShardedWal`] (shared between all replicas in replicaset).
///
/// All inter-instance communication happens on the [`PretendCluster`] level.
///
/// Several fibers can act for one instance: each is in [`Self::fibers`], and
/// [`Self::current_fiber_id`] says which of them the scheduler switched to.
pub struct PretendInstance {
    pub name: InstanceName,
    pub topology: TopologyCache,
    pub parameters: AlterSystemParametersRef,

    /// This instance's storage: [`SpaceType::Temporary`] versions of all the
    /// system tables.
    pub catalog: Catalog,

    pub local_buckets: RefCell<BTreeMap<u64, VshardBucketRecord>>,

    ////////////////////////////////////////////////////////////////////////////
    // CAS/raft
    ////////////////////////////////////////////////////////////////////////////
    /// How many entries of [`PretendCluster::raft_log`] this instance
    /// has applied so far.
    pub applied_index: Cell<RaftIndex>,

    /// A queue of outcomming CAS proposals.
    pub cas_outbox: RefCell<VecDeque<PretendCasRequest>>,

    ////////////////////////////////////////////////////////////////////////////
    // sharded replication
    ////////////////////////////////////////////////////////////////////////////
    /// Shared with every other instance of the same replicaset.
    pub sharded_wal: Rc<ShardedWal>,

    /// How many entries of [`Self::sharded_wal`] this instance has applied,
    /// i.e. tarantool's per-instance LSN. A master is always caught up, since
    /// it applies its own writes as it makes them; a replica trails.
    pub sharded_lsn: Cell<usize>,

    /// Fault injection.
    pub sharded_replication_paused: Cell<bool>,

    /// A queue of transactions applied to this instance's memory whose WAL
    /// entry hasn't persisted yet.
    pub unstable_wal: RefCell<VecDeque<WalWrite>>,

    ////////////////////////////////////////////////////////////////////////////
    // fibers
    ////////////////////////////////////////////////////////////////////////////
    pub fibers: RefCell<BTreeMap<FiberId, Rc<PretendFiber>>>,

    /// Which of `fibers` is running right now.
    /// Is set by the scheduler immediately before it switches to that fiber.
    /// Used in [`Platform`] methods for accessing the current fiber's state.
    pub current_fiber_id: Cell<Option<FiberId>>,

    /// Spawn counter for this instance's own fibers; ids are only unique
    /// within one instance (see [`PretendFiber::id`]).
    pub next_fiber_id: Cell<FiberId>,

    ////////////////////////////////////////////////////////////////////////////
    // resharding_loop
    ////////////////////////////////////////////////////////////////////////////
    pub requested_status_tx: RefCell<watch::Sender<(ReshardingStatus, u64)>>,
    pub actual_status_rx: RefCell<watch::Receiver<(ReshardingStatus, u64)>>,

    pub resharding_loop_fiber_id: Cell<Option<FiberId>>,

    pub loop_state: Rc<NoYieldsRefCell<ReshardingLoopState>>,
}

impl PretendInstance {
    pub fn new(catalog: Catalog, topology: TopologyCache, sharded_wal: Rc<ShardedWal>) -> Self {
        let (requested_status_tx, _) = watch::channel((ReshardingStatus::Idle, 0));
        let (_, actual_status_rx) = watch::channel((ReshardingStatus::Idle, 0));

        Self {
            name: topology.my_instance_name().into(),
            catalog,
            topology,
            parameters: AlterSystemParameters::for_tests(),
            local_buckets: RefCell::new(BTreeMap::new()),
            applied_index: Cell::new(0),
            sharded_wal,
            sharded_lsn: Cell::new(0),
            sharded_replication_paused: Cell::new(false),
            cas_outbox: RefCell::new(VecDeque::new()),
            unstable_wal: RefCell::new(VecDeque::new()),
            requested_status_tx: RefCell::new(requested_status_tx),
            actual_status_rx: RefCell::new(actual_status_rx),
            fibers: RefCell::new(BTreeMap::new()),
            current_fiber_id: Cell::new(None),
            resharding_loop_fiber_id: Cell::new(None),
            next_fiber_id: Cell::new(100),
            loop_state: Rc::new(NoYieldsRefCell::new(ReshardingLoopState::default())),
        }
    }

    pub fn is_master(&self) -> bool {
        self.topology.with(|topology_ref| {
            topology_ref
                .this_replicaset()
                .effective_master_name()
                .map(|n| &**n)
                == Some(&*self.name)
        })
    }

    pub fn is_down(&self) -> bool {
        self.resharding_loop_fiber_id.get().is_none()
    }

    ////////////////////////////////////////////////////////////////////////////
    // wait_index
    ////////////////////////////////////////////////////////////////////////////

    /// Parks the calling fiber until this instance's `applied_index` reaches
    /// `needed`. The caller picks which state to register.
    pub fn wait_index(&self, needed: RaftIndex, state: FiberState) -> Result<()> {
        assert!(
            matches!(state, FiberState::WaitAppliedIndex { needed: n, .. } | FiberState::WaitUntilMaster { next_index: n, .. } if n == needed),
            "simulation: wait_index({needed}, {state:?}) - the state's own `needed` must match"
        );

        let fiber = self.current_fiber()?;
        // Nothing to wait for if the index is already there.
        if self.applied_index.get() >= needed {
            return Ok(());
        }

        fiber.park(state).to_result()?;

        assert!(
            self.applied_index.get() >= needed,
            "[{}] an applied-index wait was satisfied at {}, short of {needed}",
            self.name,
            self.applied_index.get(),
        );

        Ok(())
    }

    ////////////////////////////////////////////////////////////////////////////
    // fibers
    ////////////////////////////////////////////////////////////////////////////

    /// The fiber currently running on behalf of this instance.
    pub fn current_fiber(&self) -> Result<Rc<PretendFiber>> {
        let id = self
            .current_fiber_id
            .get()
            .ok_or_else(simulation_fiber_is_stopped)?;
        let fiber = self.fibers.borrow().get(&id).cloned();
        let Some(fiber) = fiber else {
            panic!("[{}] current fiber {id} is not registered", self.name);
        };
        Ok(fiber)
    }

    /// Adds a freshly spawned fiber to this instance's registry and makes it
    /// the current one - the scheduler is switching to it right now (nothing
    /// else runs until it parks or exits).
    pub fn register_fiber(&self, fiber: Rc<PretendFiber>) {
        let id = fiber.id.id;
        let old = self.fibers.borrow_mut().insert(id, fiber);
        assert!(old.is_none(), "[{}] duplicate fiber id {id}", self.name);
        self.current_fiber_id.set(Some(id));
    }

    pub fn take_next_fiber_id(&self) -> FiberId {
        let id = self.next_fiber_id.get();
        self.next_fiber_id.set(id + 1);
        id
    }

    /// Every fiber currently acting on behalf of this instance, in id (=
    /// spawn) order. Cloned out of the registry so the caller isn't holding
    /// it borrowed while fibers run.
    pub fn all_fibers(&self) -> Vec<Rc<PretendFiber>> {
        self.fibers.borrow().values().cloned().collect()
    }

    /// Joins and forgets every fiber of this instance which has exited.
    pub fn prune_finished_fibers(&self) {
        let finished: Vec<Rc<PretendFiber>> = {
            let mut fibers = self.fibers.borrow_mut();
            let ids: Vec<FiberId> = fibers
                .iter()
                .filter(|(_, f)| f.is_finished())
                .map(|(id, _)| *id)
                .collect();
            ids.iter().filter_map(|id| fibers.remove(id)).collect()
        };
        for fiber in finished {
            // Already exited, so this doesn't block.
            fiber.join();
        }
    }

    /// Is called when crashing the instance.
    ///
    /// Cancells all the pretend fibers such that each [`Platform`] method call
    /// after that immediately returns `Err(fiber was cancelled)`. Assumming all
    /// production code propagates such erros up the stack this results in the
    /// quickest possible finishing of all remaining fibers.
    pub fn cancel_all_fibers(&self) {
        self.current_fiber_id.set(None);
        for fiber in self.all_fibers() {
            fiber.unpark(WaitOutcome::Cancelled);
        }
        // Every fiber has been cancelled by now: join only after that, so a
        // fiber parked behind another one's exit can't deadlock this loop.
        for fiber in self.all_fibers() {
            fiber.join();
        }
        self.fibers.borrow_mut().clear();
        self.resharding_loop_fiber_id.set(None);
    }

    ////////////////////////////////////////////////////////////////////////////
    // resharding_loop
    ////////////////////////////////////////////////////////////////////////////

    /// This instance's `resharding_loop` fiber.
    pub fn resharding_loop_fiber(&self) -> Rc<PretendFiber> {
        let Some(id) = self.resharding_loop_fiber_id.get() else {
            panic!("[{}] has no resharding loop fiber", self.name);
        };
        let fiber = self.fibers.borrow().get(&id).cloned();
        let Some(fiber) = fiber else {
            panic!(
                "[{}] resharding loop fiber {id} is not registered",
                self.name
            );
        };
        fiber
    }

    pub fn set_requested_resharding_status(&self, status: ReshardingStatus) {
        let version = self.actual_status_rx.borrow().get().1;
        let waiting_for_version = version + 1;
        _ = self
            .requested_status_tx
            .borrow()
            .send((status, waiting_for_version));
    }

    pub fn last_requested_resharding_status(&self) -> ReshardingStatus {
        self.requested_status_tx.borrow().get().0
    }

    ////////////////////////////////////////////////////////////////////////////
    // assert_converged
    ////////////////////////////////////////////////////////////////////////////

    pub fn assert_converged(&self) {
        let topology_ref = self.topology.get();
        let this_replicaset = topology_ref.this_replicaset();
        assert_eq!(
            this_replicaset.current_bucket_state_version,
            this_replicaset.target_bucket_state_version,
            "[{}] replicaset '{}' bucket_state_version not actualized",
            self.name,
            this_replicaset.name
        );

        let mut expected_bucket_count = 0u64;
        for range in topology_ref
            .buckets_info(&this_replicaset.tier)
            .expect("tier must exist")
            .ranges
            .iter()
        {
            if this_replicaset.name == range.current_replicaset_name && range.state.is_active() {
                expected_bucket_count += range.count();
            }
        }
        drop(topology_ref);

        let buckets = self.local_buckets.borrow();
        assert_eq!(
            buckets.len() as u64,
            expected_bucket_count,
            "[{}] local bucket count doesn't match owned _pico_bucket ranges: {buckets:?}",
            self.name
        );
        assert!(
            buckets
                .values()
                .all(|b| b.state == VshardBucketState::Active),
            "[{}] not all local buckets are Active: {buckets:?}",
            self.name
        );
    }
}

////////////////////////////////////////////////////////////////////////////////
// Platform
////////////////////////////////////////////////////////////////////////////////

impl Platform for PretendInstance {
    fn topology_cache(&self) -> &TopologyCache {
        &self.topology
    }

    fn alter_system_parameters(&self) -> &AlterSystemParametersRef {
        &self.parameters
    }

    async fn wait_action_requested(
        &self,
        _requested_status: &mut watch::Receiver<(ReshardingStatus, u64)>,
    ) -> Result<()> {
        let fiber = self.current_fiber()?;
        let wait_outcome = fiber.park(FiberState::WaitIdle);
        assert!(
            !wait_outcome.is_timeout(),
            "simulation: WaitIdle cannot timeout"
        );
        wait_outcome.to_result()
    }

    fn applied_index(&self) -> RaftIndex {
        self.applied_index.get()
    }

    fn do_cas(&self, applied: RaftIndex, dmls: Vec<Dml>, timeout: Duration) -> Result<()> {
        let outcome: CasOutcomeSlot = Rc::new(Cell::new(None));
        self.cas_outbox.borrow_mut().push_back(PretendCasRequest {
            index: applied,
            dmls,
            outcome: outcome.clone(),
        });

        let fiber = self.current_fiber()?;
        let index = match fiber.wait_for_cas_outcome(&outcome, timeout)? {
            PretendCasOutcome::Committed { index } => index,
            PretendCasOutcome::ConflictFound { conflict_index } => {
                return Err(cas::Error::ConflictFound(conflict_index).into());
            }
            PretendCasOutcome::Dropped => {
                return Err(Error::timeout());
            }
        };

        self.wait_index(
            index,
            FiberState::WaitAppliedIndex {
                needed: index,
                timeout,
            },
        )
    }

    fn wait_until_master(&self, timeout: Duration) -> Result<RaftIndex> {
        let next_index = self.applied_index.get() + 1;
        self.wait_index(
            next_index,
            FiberState::WaitUntilMaster {
                next_index,
                timeout,
            },
        )?;
        Ok(self.applied_index.get())
    }

    fn read_local_buckets(&self, start: u64, end: u64) -> Result<Vec<VshardBucketRecord>> {
        Ok(self
            .local_buckets
            .borrow()
            .range(start..=end)
            .map(|(_, v)| v.clone())
            .collect())
    }

    fn write_local_buckets(&self, recs: Vec<VshardBucketRecord>) -> Result<()> {
        // A replica getting here means the loop's own
        // `i_am_replicaset_master` check was somehow bypassed. See
        // `write_sharded_entry` for which races this does and doesn't catch.
        assert!(
            self.is_master(),
            "[{}] sharded write from a non-master",
            self.name
        );
        write_sharded_entry(self, ShardedEntry::WriteBuckets { recs })
    }

    fn wait_router_discovery_complete(&self, _tier: &str, _timeout: Duration) -> Result<()> {
        // Nothing to discover: the simulation has no vshard router.
        Ok(())
    }
}
