use crate::catalog::pico_bucket::BucketRecord;
use crate::instance::Instance;
use crate::instance::InstanceName;
use crate::replicaset::Replicaset;
use crate::simulation::action::PretendAction;
use crate::simulation::catalog::create_pretend_catalog;
#[allow(unused_imports)]
use crate::simulation::engine::do_action;
use crate::simulation::fiber::FiberState;
use crate::simulation::fiber::PretendFiber;
use crate::simulation::instance::PretendInstance;
use crate::simulation::raft::PretendRaftLog;
use crate::simulation::sharded_wal::ShardedWal;
use crate::simulation::sharded_wal::WalOutcome;
use crate::simulation::sharding::spawn_resharding_loop_fiber;
use crate::storage::Catalog;
use crate::tier::Tier;
use crate::topology_cache::TopologyCache;
use rand::rngs::StdRng;
use rand::RngExt;
use rand::SeedableRng;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::rc::Rc;
use tarantool::fiber;
use tarantool::fiber::FiberId;

/// State of a single simulated cluster.
pub struct PretendCluster {
    /// Single random number generator per simulation. Seeded once at the start.
    pub rng: RefCell<StdRng>,

    /// Every instance of the cluster, keyed by its name .
    pub pretend_instances: RefCell<InstanceMap>,

    /// Committed raft log of the cluster. Only stores entries. Each instance
    /// keeps track of it's own [`PretendInstance::applied_index`].
    pub raft_log: PretendRaftLog,

    /// Stores each action performed by [`do_action`].
    pub trace: RefCell<Vec<PretendAction>>,

    /// The probability that [`PretendAction::ResolveCasRequest`] drops the proposal.
    pub cas_fault_probability: Cell<f64>,

    /// The probability that [`step_once`](crate::simulation::engine::step_once) decides to
    /// timeout one of the waiting fibers.
    pub timeout_probability: Cell<f64>,

    /// The probability that [`PretendAction::CommitWalWrite`] fails a WAL write.
    pub disk_write_fault_probability: Cell<f64>,
}

/// A collection of all instances in a [`PretendCluster`].
///
/// Uses [`DefaultHasher::default`] for reproducible iteration order.
pub type InstanceMap =
    HashMap<InstanceName, Rc<PretendInstance>, BuildHasherDefault<DefaultHasher>>;

impl PretendCluster {
    pub fn new(seed: u64) -> Rc<Self> {
        Rc::new(Self {
            pretend_instances: RefCell::new(InstanceMap::default()),
            raft_log: PretendRaftLog::new(),
            rng: RefCell::new(StdRng::seed_from_u64(seed)),
            trace: RefCell::new(Vec::new()),
            cas_fault_probability: Cell::new(0.0),
            timeout_probability: Cell::new(0.0),
            disk_write_fault_probability: Cell::new(0.0),
        })
    }

    ////////////////////////////////////////////////////////////////////////////
    // setup
    ////////////////////////////////////////////////////////////////////////////

    /// Initializes a cluster according to provided topology definitions.
    ///
    /// Initializes all needed [`PretendInstance`] instances with catalog and
    /// topology.
    /// Spawns a `resharding_loop` fiber for each instance.
    ///
    /// All fibers in the returned cluster are parked.
    pub fn setup(
        seed: u64,
        instances: &[Instance],
        replicasets: &[Replicaset],
        tiers: &[Tier],
        bucket_records: &[BucketRecord],
    ) -> Rc<Self> {
        let cluster = Self::new(seed);

        for this_instance in instances {
            let catalog = create_pretend_catalog(&this_instance.name);
            for instance in instances {
                catalog.instances.put(instance).unwrap();
            }
            for replicaset in replicasets {
                catalog.replicasets.put(replicaset).unwrap();
            }
            for tier in tiers {
                catalog.tiers.put(tier).unwrap();
            }
            for bucket_record in bucket_records {
                catalog.pico_bucket.space.insert(bucket_record).unwrap();
            }

            let topology = TopologyCache::load(
                &catalog,
                this_instance.raft_id,
                "pretend-cluster",
                "cluster-uuid",
            )
            .unwrap();

            cluster.add_instance(catalog, topology);
        }

        assert!(
            cluster.running_fiber().is_none(),
            "simulation: a runner fiber hadn't parked by the end of setup"
        );

        cluster
    }

    ////////////////////////////////////////////////////////////////////////////
    // fault injection
    ////////////////////////////////////////////////////////////////////////////

    /// Returns `true` with given `probability`.
    pub fn roll(&self, probability: f64) -> bool {
        if probability == 0.0 {
            return false;
        }
        self.rng.borrow_mut().random_bool(probability)
    }

    pub fn should_timeout_a_wait(&self) -> bool {
        self.roll(self.timeout_probability.get())
    }

    pub fn should_drop_cas_request(&self) -> bool {
        self.roll(self.cas_fault_probability.get())
    }

    pub fn roll_wal_outcome(&self) -> WalOutcome {
        if self.roll(self.disk_write_fault_probability.get()) {
            WalOutcome::Failed
        } else {
            WalOutcome::Persisted
        }
    }

    pub fn set_timeout_probability(&self, probability: f64) {
        assert!((0.0..=1.0).contains(&probability));
        self.timeout_probability.set(probability);
    }

    pub fn set_cas_fault_probability(&self, probability: f64) {
        assert!((0.0..=1.0).contains(&probability));
        self.cas_fault_probability.set(probability);
    }

    ////////////////////////////////////////////////////////////////////////////
    // instances
    ////////////////////////////////////////////////////////////////////////////

    pub fn add_instance(&self, catalog: Catalog, topology: TopologyCache) {
        let sharded_wal = self
            .sharded_wal_of_replicaset(topology.my_replicaset_name())
            .unwrap_or_default();
        let pretend_instance = Rc::new(PretendInstance::new(catalog, topology, sharded_wal));
        let name = pretend_instance.name.clone();

        spawn_resharding_loop_fiber(&pretend_instance);

        let old = self
            .pretend_instances
            .borrow_mut()
            .insert(name.clone(), pretend_instance);
        assert!(old.is_none(), "duplicate instance name '{name}'");
    }

    #[track_caller]
    pub fn instance(&self, instance_name: &str) -> Rc<PretendInstance> {
        let instance = self.pretend_instances.borrow().get(instance_name).cloned();
        let Some(instance) = instance else {
            panic!("simulation: unknown instance '{instance_name}'");
        };
        assert_eq!(&instance.name, instance_name);
        instance
    }

    pub fn instances(&self) -> Vec<Rc<PretendInstance>> {
        self.pretend_instances.borrow().values().cloned().collect()
    }

    /// The instance with the highest `applied_index`.
    ///
    /// Can be used in tests in place of a raft leader for example when a test
    /// needs to propose a CAS request.
    pub fn most_applied_instance(&self) -> Rc<PretendInstance> {
        self.instances()
            .into_iter()
            .max_by_key(|i| i.applied_index.get())
            .expect("cluster has no instances")
    }

    pub fn instance_names(&self) -> Vec<InstanceName> {
        self.pretend_instances.borrow().keys().cloned().collect()
    }

    /// The sharded-replication WAL of `replicaset_name`.
    pub fn sharded_wal_of_replicaset(&self, replicaset_name: &str) -> Option<Rc<ShardedWal>> {
        for instance in self.instances() {
            if instance.topology.my_replicaset_name() == replicaset_name {
                return Some(instance.sharded_wal.clone());
            }
        }

        None
    }

    ////////////////////////////////////////////////////////////////////////////
    // fibers
    ////////////////////////////////////////////////////////////////////////////

    /// Reschedules (yields) the current fiber until the current running
    /// [`PretendFiber`] runs to it's next park.
    pub fn run_to_next_park(&self) {
        // Note: there's only one running fiber at any moment, so ideally we
        // only reschedule once like this:
        // `scheduler -(yield)-> running_fiber -(yield)-> scheduler`,
        // but because there could be other fibers from tarantool's runtime, we
        // could potentially yield to them and back several times, so we have to
        // loop until we get exactly what we want.
        while self.running_fiber().is_some() {
            fiber::reschedule();
        }
    }

    /// Returns the only currently running [`PretendFiber`] in the cluster or
    /// `None`.
    ///
    /// Asserts that there is in fact at most 1 running fiber.
    pub fn running_fiber(&self) -> Option<(Rc<PretendInstance>, Rc<PretendFiber>)> {
        let mut running: Option<(Rc<PretendInstance>, Rc<PretendFiber>)> = None;
        for (instance, fiber) in self.all_fibers() {
            if !fiber.is_running() {
                continue;
            }

            if let Some((_, other_fiber)) = &running {
                panic!(
                    "simulation: fiber {} and fiber {} are running at the same time",
                    other_fiber.id, fiber.id,
                );
            }
            running = Some((instance, fiber));
        }
        running
    }

    /// Joins and forgets every fiber which has exited
    pub fn prune_finished_fibers(&self) {
        for instance in self.instances() {
            instance.prune_finished_fibers();
        }
    }

    /// Every currently live fiber in the cluster along with the instance it
    /// belongs to.
    ///
    /// Returns items in deterministic order.
    pub fn all_fibers(&self) -> Vec<(Rc<PretendInstance>, Rc<PretendFiber>)> {
        let mut all = Vec::new();
        for instance in self.instances() {
            for fiber in instance.all_fibers() {
                all.push((instance.clone(), fiber));
            }
        }
        all
    }

    pub fn find_fiber(&self, instance_name: &str, id: FiberId) -> Rc<PretendFiber> {
        let instance = self.instance(instance_name);
        let fiber = instance.fibers.borrow().get(&id).cloned();
        let Some(fiber) = fiber else {
            panic!("simulation: unknown fiber [{instance_name}]/{id}");
        };
        assert_eq!(&fiber.id.instance, instance_name);
        fiber
    }

    /// Diagnostic dump of every currently parked fiber.
    pub fn dump_fiber_registry(&self) -> String {
        let mut lines = Vec::new();
        for (_, fiber) in self.all_fibers() {
            let state = fiber.state();
            if state.is_parked() {
                lines.push(format!("fiber {}: waiting {state:?}", fiber.id));
            }
        }
        lines.join("\n")
    }

    ////////////////////////////////////////////////////////////////////////////
    // assert_resting
    ////////////////////////////////////////////////////////////////////////////

    pub fn assert_resting(&self) {
        self.prune_finished_fibers();

        for instance in self.instances() {
            assert!(
                !instance.is_down(),
                "[{}] converged while still down; trace: {:#?}",
                instance.name,
                self.trace.borrow(),
            );
            let live_fibers = instance.all_fibers();
            assert_eq!(
                live_fibers.len(),
                1,
                "[{}] converged with fibers other than its runner ({:?}); trace: {:#?}",
                instance.name,
                live_fibers.iter().map(|f| f.id.id).collect::<Vec<_>>(),
                self.trace.borrow(),
            );
            assert_eq!(
                instance.sharded_lsn.get(),
                instance.sharded_wal.len(),
                "[{}] converged with an unapplied sharded-replication WAL tail; trace: {:#?}",
                instance.name,
                self.trace.borrow(),
            );
            assert!(
                instance.cas_outbox.borrow().is_empty(),
                "[{}] converged with an undecided CAS proposal; trace: {:#?}",
                instance.name,
                self.trace.borrow(),
            );
            assert!(
                instance.unstable_wal.borrow().is_empty(),
                "[{}] converged with an unpersisted WAL write; trace: {:#?}",
                instance.name,
                self.trace.borrow(),
            );
            assert_eq!(
                instance.applied_index.get(),
                self.raft_log.last_index(),
                "[{}] converged with an unapplied global stream tail; trace: {:#?}",
                instance.name,
                self.trace.borrow(),
            );

            // The resting wait differs by role: a master rests in `Idle`; a
            // non-master that was ever driven rests in the not-a-master
            // branch's `wait_index_change`, waiting for an entry which
            // doesn't exist yet and so unsatisfiable at convergence. A
            // never-driven non-master never left its initial `Idle` park,
            // which is equally at rest.
            let wait = instance.resharding_loop_fiber().state();
            if instance.is_master() {
                assert_eq!(
                    wait,
                    FiberState::WaitIdle,
                    "[{}] nothing left to do but the loop fiber is not resting \
                     (parked on {wait:?} with nothing left to wake it); trace: {:#?}",
                    instance.name,
                    self.trace.borrow(),
                );
            } else {
                let expected_needed = self.raft_log.next_index();
                let resting = match wait {
                    FiberState::WaitUntilMaster { next_index, .. } => next_index == expected_needed,
                    FiberState::WaitIdle => true,
                    _ => false,
                };
                assert!(
                    resting,
                    "[{}] nothing left to do but the non-master loop fiber is not resting \
                     (parked on {wait:?}, expected NotAMaster {{ needed: {expected_needed}, .. }} or Idle); trace: {:#?}",
                    instance.name,
                    self.trace.borrow(),
                );
            }
        }
    }
}

// Note: Drop is called when the test fails.
impl Drop for PretendCluster {
    fn drop(&mut self) {
        for instance in self.instances() {
            instance.cancel_all_fibers();
        }
    }
}
