use crate::instance::InstanceName;
use crate::resharding_loop::ReshardingStatus;
#[allow(unused_imports)]
use crate::simulation::action::PretendAction;
use crate::simulation::cluster::PretendCluster;
#[allow(unused_imports)]
use crate::simulation::engine::do_action;
use crate::simulation::instance::PretendInstance;
use crate::simulation::raft::CasOutcomeSlot;
use crate::simulation::raft::PretendCasOutcome;
use crate::simulation::sharded_wal::WalOutcome;
use crate::simulation::sharded_wal::WalOutcomeSlot;
use crate::traft::error::Error;
use crate::traft::RaftIndex;
use crate::Result;
use ::tarantool::fiber;
use ::tarantool::fiber::r#async::watch;
use ::tarantool::fiber::FiberId;
use smol_str::SmolStr;
use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;
use std::rc::Weak;
use std::time::Duration;

/// State associated with every fiber spawned in the simulation to run tested
/// code.
pub struct PretendFiber {
    pub id: PretendFiberId,

    pub state: RefCell<FiberState>,

    /// Whenever the tested code is doing any yielding operation through the
    /// [`Platform`] interface, the current fiber will "park"
    /// ([`PretendFiber::park`]) by setting it's [`PretendFiber::state`]
    /// accordingly and then will block trying to receive from this channel.
    ///
    /// It will eventually be awoken when the simulation engine sends an outcome
    /// into it.
    ///
    /// [`Platform`]: crate::simulation::platform::Platform
    pub wait_outcome: watch::Sender<WaitOutcome>,

    /// [`fiber::csw`] value recorded at the moment of last unpark. Used to detect
    /// yields which didn't go through [`Self::park`].
    pub csw_at_unpark: Cell<u64>,

    /// The instance which owns this fiber.
    pub instance: Weak<PretendInstance>,

    /// A join handle for the purposes of teardown.
    pub join_handle: RefCell<Option<fiber::JoinHandle<'static, ()>>>,
}

/// Uniquely and deterministically identifies a [`PretendFiber`] in the cluster.
#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub struct PretendFiberId {
    pub instance: InstanceName,

    /// Id assigned deterministically inside the `instance`s fiber id space.
    ///
    /// Is deliberately different from the actual [`fiber::id`] of the
    /// underyling physical fiber.
    pub id: FiberId,

    /// Same as [`fiber::name`] of the physical fiber.
    pub name: SmolStr,
}

impl PretendFiber {
    /// New fibers a created as [`FiberState::Running`] so it's safe to start
    /// the actual physical fiber without violating the invariants.
    pub fn new(instance: &Rc<PretendInstance>, name: &str) -> Self {
        let (wait_outcome, _) = watch::channel(WaitOutcome::Ok);
        let id = PretendFiberId {
            instance: instance.name.clone(),
            id: instance.take_next_fiber_id(),
            name: name.into(),
        };
        Self {
            id,
            state: RefCell::new(FiberState::Running),
            wait_outcome,
            csw_at_unpark: Cell::new(0),
            instance: Rc::downgrade(instance),
            join_handle: RefCell::new(None),
        }
    }

    pub fn set_join_handle(&self, join_handle: fiber::JoinHandle<'static, ()>) {
        let old = self.join_handle.borrow_mut().replace(join_handle);
        assert!(old.is_none(), "simulation: fiber {} spawned twice", self.id);
    }

    pub fn instance(&self) -> Rc<PretendInstance> {
        let Some(instance) = self.instance.upgrade() else {
            panic!("simulation: fiber {} outlived its instance", self.id);
        };
        instance
    }

    pub fn state(&self) -> FiberState {
        self.state.borrow().clone()
    }

    pub fn is_running(&self) -> bool {
        matches!(*self.state.borrow(), FiberState::Running)
    }

    pub fn is_finished(&self) -> bool {
        matches!(*self.state.borrow(), FiberState::Finished)
    }

    /// Marks this fiber as exited, from within its own body right before it
    /// returns.
    pub fn set_finished(&self) {
        *self.state.borrow_mut() = FiberState::Finished;
    }

    /// This is true when the fiber gets cancelled due to simulation teardown or
    /// instance shutdown.
    pub fn is_stopped(&self) -> bool {
        self.instance().current_fiber_id.get().is_none()
    }

    /// Joins this fiber's tarantool fiber.
    pub fn join(&self) {
        assert!(
            self.is_finished() || self.is_stopped(),
            "simulation: attempt to join fiber {} which is neither cancelled nor finished",
            self.id
        );
        let join_handle = self.join_handle.borrow_mut().take();
        let Some(join_handle) = join_handle else {
            panic!("simulation: fiber {} was never spawned", self.id);
        };
        join_handle.join();
    }

    /// This function is only needed after the initial fiber start.
    /// After that fibers are always parked in between engine actions.
    pub fn wait_parked(&self) {
        // Note: there's only one running fiber at any moment, so ideally we
        // only reschedule once like this:
        // `scheduler -(yield)-> running_fiber -(yield)-> scheduler`,
        // but because there could be other fibers from tarantool's runtime, we
        // could potentially yield to them and back several times, so we have to
        // loop until we get exactly what we want.
        while self.is_running() {
            fiber::reschedule();
        }
    }

    /// Registers `state` as this fiber's current park and blocks until the
    /// scheduler resolves it, returning its [`WaitOutcome`].
    pub fn park(&self, state: FiberState) -> WaitOutcome {
        assert!(state.is_parked(), "simulation: park({state:?})");

        // In teardown, return ASAP.
        if self.is_stopped() {
            return WaitOutcome::Cancelled;
        }

        // Invariant: simulation engine always set's PretendInstance::current_fiber_id correctly
        let instance = self.instance();
        assert_eq!(
            instance.current_fiber_id.get(),
            Some(self.id.id),
            "fiber {} parked while the scheduler had switched to another",
            self.id,
        );

        *self.state.borrow_mut() = state;
        let mut wait_outcome = self.wait_outcome.subscribe();

        let expected = self.csw_at_unpark.get();
        let actual = fiber::csw();
        if expected != actual {
            panic!(
                "simulation: fiber {} yielded without parking ({expected} != {actual})",
                self.id
            );
        }

        // Wait for the engine to determine the outcome.
        fiber::block_on(wait_outcome.changed()).expect("the sender is owned by this same fiber");

        // NOTE: in an ideal world this csw would be equal to old_csw + 1,
        // but in reality block_on could do several yields because of spurious wake ups.
        self.csw_at_unpark.set(fiber::csw());

        wait_outcome.get()
    }

    /// Resolve the fiber's `wait_outcome`.
    ///
    /// Is called at the end of [`wake_fiber`].
    pub fn unpark(&self, wait_outcome: WaitOutcome) {
        self.wait_outcome
            .send(wait_outcome)
            .expect("nobody holds a reference to the current wait_outcome");
    }

    pub fn wait_for_wal_outcome(&self, outcome: &WalOutcomeSlot) -> Result<WalOutcome> {
        assert!(
            outcome.get().is_none(),
            "simulation: a transaction is decided only while its writer is parked on it"
        );

        let state = FiberState::WaitWalWrite {
            outcome: outcome.clone(),
        };
        let wait_outcome = self.park(state);

        assert!(
            !wait_outcome.is_timeout(),
            "simulation: a WAL write has no timeout to fire"
        );
        wait_outcome.to_result()?;

        let Some(outcome) = outcome.get() else {
            panic!("simulation: a WAL write was satisfied without an outcome")
        };

        Ok(outcome)
    }

    pub fn wait_for_cas_outcome(
        &self,
        outcome: &CasOutcomeSlot,
        timeout: Duration,
    ) -> Result<PretendCasOutcome> {
        assert!(
            outcome.get().is_none(),
            "simulation: a proposal is decided only while its proposer is parked on it"
        );

        let state = FiberState::WaitCasOutcome {
            outcome: outcome.clone(),
            timeout,
        };
        self.park(state).to_result()?;

        let Some(outcome) = outcome.get() else {
            panic!("simulation: a CAS wait was satisfied without an outcome")
        };

        Ok(outcome)
    }
}

impl std::fmt::Display for PretendFiberId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.instance, self.id, self.name)
    }
}

impl std::fmt::Debug for PretendFiberId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.instance, self.id, self.name)
    }
}

////////////////////////////////////////////////////////////////////////////////
// FiberState
////////////////////////////////////////////////////////////////////////////////

/// What a simulation fiber is doing right now: running, parked on one
/// registered wait, or exited.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FiberState {
    /// Freshly spawned, or woken by the scheduler and not yet back at its
    /// next park. At most one fiber cluster-wide is ever in this state
    /// between scheduler actions.
    ///
    /// See also [`PretendCluster::run_to_next_park`].
    Running,

    /// The resharding_loop fiber's resting park: either genuinely idle
    /// (`requested_status` unchanged) or a "retry after backoff/error"
    /// recheck.
    WaitIdle,

    /// Fiber is waiting until instance applies the raft entry at `needed` index.
    ///
    /// See also [`PretendInstance::wait_index`].
    WaitAppliedIndex {
        needed: RaftIndex,
        timeout: Duration,
    },

    /// Same as `WaitAppliedIndex`, but in a different context.
    /// This is only used when a replica is waiting idly to become a master.
    ///
    /// It needs to be a separate state so that the simulation engine can detect
    /// this as an idle wait.
    WaitUntilMaster {
        next_index: RaftIndex,
        timeout: Duration,
    },

    /// Parked until a corresponding [`PretendAction::CommitWalWrite`] is handled
    /// in [`do_action`].
    ///
    /// `outcome` is set in [`do_action`] as well.
    ///
    /// See also [`write_sharded_entry`](crate::simulation::sharded_wal::write_sharded_entry).
    WaitWalWrite { outcome: WalOutcomeSlot },

    /// Parked until a corresponding [`PretendAction::ResolveCasRequest`] is handled in [`do_action`].
    ///
    /// `outcome` is set in [`do_action`] as well.
    WaitCasOutcome {
        outcome: CasOutcomeSlot,
        timeout: Duration,
    },

    /// The fiber function has exited.
    Finished,
}

impl FiberState {
    /// Whether this is one of the `Wait*` variants, i.e. the fiber is parked
    /// on a registered wait right now.
    pub fn is_parked(&self) -> bool {
        !matches!(self, Self::Running | Self::Finished)
    }

    /// Whether this is a wait with a timeout parameter.
    pub fn is_wait_timeoutable(&self) -> bool {
        matches!(
            self,
            FiberState::WaitAppliedIndex { .. }
                | FiberState::WaitCasOutcome { .. }
                | FiberState::WaitUntilMaster { .. }
        )
    }

    pub fn is_idle_wait(&self) -> bool {
        matches!(
            self,
            FiberState::WaitIdle | FiberState::WaitUntilMaster { .. }
        )
    }

    /// Make a copy of the state to put into the trace.
    ///
    /// This solves the problem that some variants store a `outcome` `Rc`
    /// which can change after the action is saved to the trace. So this function
    /// detaches those rc-s.
    pub fn snapshot(&self) -> Self {
        match self {
            Self::WaitCasOutcome { outcome, timeout } => Self::WaitCasOutcome {
                outcome: Rc::new(Cell::new(outcome.get())),
                timeout: *timeout,
            },
            Self::WaitWalWrite { outcome } => Self::WaitWalWrite {
                outcome: Rc::new(Cell::new(outcome.get())),
            },
            other => other.clone(),
        }
    }
}

/// The scheduler's resolution of one registered wait, returned by
/// [`PretendFiber::park`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitOutcome {
    /// The wait predicate is satisfied.
    ///
    /// See also [`PretendAction::SatisfyWait`].
    Ok,

    /// A timeoutable wait has timed out.
    ///
    /// See also [`PretendAction::TimeoutWait`].
    Timeout,

    /// This fiber was cancelled for the purposes of teardown.
    ///
    /// See also [`PretendInstance::cancel_all_fibers`].
    Cancelled,
}

impl WaitOutcome {
    pub fn is_timeout(&self) -> bool {
        matches!(self, WaitOutcome::Timeout)
    }

    pub fn to_result(self) -> Result<()> {
        match self {
            WaitOutcome::Ok => Ok(()),
            WaitOutcome::Timeout => Err(Error::timeout()),
            WaitOutcome::Cancelled => Err(fiber_was_cancelled()),
        }
    }
}

/// Resolves `fiber`'s current park with `wait_outcome`:
///
/// - puts the `fiber` back into [`FiberState::Running`]
/// - sends the `wait_outcome` which unblocks the [`PretendFiber::park`] call
/// - sets its instance's [`PretendInstance::current_fiber_id`]
///
/// After this is called the simulation engine must call [`PretendCluster::run_to_next_park`]
/// before performing any other [`PretendAction`].
pub fn wake_fiber(
    cluster: &PretendCluster,
    instance: &PretendInstance,
    fiber: &Rc<PretendFiber>,
    wait_outcome: WaitOutcome,
) {
    if let Some((_, running_fiber)) = cluster.running_fiber() {
        panic!(
            "fiber {} was woken while fiber {} was still running",
            fiber.id, running_fiber.id,
        );
    }

    let mut state = fiber.state.borrow_mut();
    assert!(
        state.is_parked(),
        "fiber {} was woken while {state:?} - only a parked fiber has \
         a wait to resolve, so the caller picked the wrong fiber",
        fiber.id,
    );

    *state = FiberState::Running;
    drop(state);

    instance.current_fiber_id.set(Some(fiber.id.id));

    fiber.unpark(wait_outcome)
}

/// Whether the wait `state` describes can be satisfied right now.
pub fn is_wait_satisfiable(instance: &PretendInstance, state: &FiberState) -> bool {
    match state {
        FiberState::WaitIdle => {
            let (want_status, next_version) = instance.requested_status_tx.borrow().get();
            let (_, curr_version) = instance.actual_status_rx.borrow().get();
            want_status != ReshardingStatus::Idle && next_version != curr_version
        }
        FiberState::WaitAppliedIndex { needed: index, .. }
        | FiberState::WaitUntilMaster {
            next_index: index, ..
        } => instance.applied_index.get() >= *index,
        FiberState::WaitCasOutcome { outcome, .. } => outcome.get().is_some(),
        FiberState::WaitWalWrite { outcome } => outcome.get().is_some(),
        // Not parked on anything, so there's no wait to satisfy.
        FiberState::Running | FiberState::Finished => false,
    }
}

pub fn simulation_fiber_is_stopped() -> Error {
    Error::other("simulation: this fiber's instance is gone")
}

pub fn fiber_was_cancelled() -> Error {
    Error::other("simulation: fiber was cancelled")
}
