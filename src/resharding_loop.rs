use crate::tlog;
use crate::traft::error::Error;
use crate::traft::node;
use crate::util::NoYieldsRefCell;
use crate::Result;
use std::rc::Rc;
use std::time::Duration;
use tarantool::fiber;
use tarantool::fiber::r#async::timeout::IntoTimeout;
use tarantool::fiber::r#async::watch;
use tarantool::time::Instant;

////////////////////////////////////////////////////////////////////////////////
// ReshardingLoop
////////////////////////////////////////////////////////////////////////////////

pub struct ReshardingLoop {
    /// Fiber id of the resharding_loop. Is used for waking up the fiber.
    fiber_id: fiber::FiberId,

    /// When governor sends us a RPC with a request to do resharding
    /// [`Self::do_resharding`] method is called which updates value in this
    /// field.
    /// Afterwards the value is checked in [`resharding_loop`] where the request is
    /// handled.
    /// Once the request is handled `actual_status` is updated.
    requested_status: watch::Sender<(ReshardingStatus, u64)>,

    /// Is updated to the value from `requested_status` in [`resharding_loop`]
    /// once the request is handled.
    pub actual_status: watch::Receiver<(ReshardingStatus, u64)>,

    /// Mutable long-living state of the resharding loop which is observable
    /// from other fibers.
    pub state: Rc<NoYieldsRefCell<ReshardingState>>,
}

/// [`resharding_loop`] sleeps for this many seconds when it encounters an
/// unhandled error before going on another loop iteration.
const RESHARDING_LOOP_SHORT_RETRY: Duration = Duration::from_millis(300);

impl ReshardingLoop {
    pub fn start() -> Self {
        let (requested_status_tx, requested_status_rx) =
            watch::channel((ReshardingStatus::Idle, 0));
        let (actual_status_tx, actual_status_rx) = watch::channel((ReshardingStatus::Idle, 0));
        let state = ReshardingState::default();
        let state = Rc::new(NoYieldsRefCell::new(state));
        let state_tx = state.clone();

        let fiber_id = fiber::Builder::new()
            .name("resharding_loop")
            .func_async(async move {
                let mut requested_status_rx = requested_status_rx;
                let mut actual_status_tx = actual_status_tx;
                loop {
                    let res =
                        resharding_loop(&mut requested_status_rx, &mut actual_status_tx, &state_tx)
                            .await;
                    match res {
                        Ok(()) => {
                            // TODO backoff
                        }
                        Err(e) => {
                            tlog!(Warning, "unhandled error: {e}");
                            _ = requested_status_rx
                                .changed()
                                .timeout(RESHARDING_LOOP_SHORT_RETRY)
                                .await;
                        }
                    }
                }
            })
            .defer_non_joinable()
            .expect("starting a fiber shouldn't fail")
            .expect("fiber id is supported");

        Self {
            fiber_id,
            requested_status: requested_status_tx,
            actual_status: actual_status_rx,
            state,
        }
    }

    pub async fn do_resharding(&self, deadline: Instant) -> Result<bool> {
        // Request is handled in `resharding_loop`.
        let waiting_for_version = self.request_action(ReshardingStatus::Resharding);

        loop {
            let (status, current_version) = self.actual_status.get();
            tlog!(Debug, "resharding status: {status:?}:{current_version}");
            if current_version >= waiting_for_version && status == ReshardingStatus::Idle {
                break;
            }

            // Value is updated in `resharding_loop`
            _ = self
                .actual_status
                .clone()
                .changed()
                .deadline(deadline)
                .await;

            if fiber::clock() > deadline {
                tlog!(Debug, "timed out waiting for resharding to complete");
                return Err(Error::timeout());
            }
        }

        tlog!(Debug, "resharding completed");
        Ok(true)
    }

    #[inline]
    pub fn request_action(&self, status: ReshardingStatus) -> u64 {
        let version = self.actual_status.get().1;
        let waiting_for_version = version + 1;

        _ = self.requested_status.send((status, waiting_for_version));

        // The above call to `send` will awake the fiber if it's currently
        // blocked on the status channel. But it could also be blocked on a call
        // to `wait_index`, in which case this call will wake that fiber up.
        tlog!(Info, "wake resharding loop up");
        fiber::wakeup(self.fiber_id);

        waiting_for_version
    }

    /// A dummy instance of this struct to be used in tests which don't care
    /// about the real behaviour.
    pub fn for_tests() -> Self {
        let (requested_status, _) = watch::channel((ReshardingStatus::Idle, 0));
        let (_, actual_status) = watch::channel((ReshardingStatus::Idle, 0));

        Self {
            fiber_id: 0,
            requested_status,
            actual_status,
            state: Default::default(),
        }
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReshardingStatus {
    #[default]
    Idle,
    ReceivingBuckets,
    ReshardingEnqueued,
    Resharding,
    CleaningUp,
}

impl ReshardingStatus {
    #[inline(always)]
    pub fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }
}

#[derive(Default, Debug)]
pub struct ReshardingState {
    // TODO
}

////////////////////////////////////////////////////////////////////////////////
// resharding_loop
////////////////////////////////////////////////////////////////////////////////

/// The main loop for the resharding logic. It handles the requests which come
/// from governor in the form of updates to value in `requested_status`.
///
/// Once the request is handled the value in `actual_status` is updated to one
/// from `requested_status`.
///
/// `actual_status` will also be updated with intermidiate values mainly for
/// debugging purposes at this point.
///
/// `state` is the mutable state with information about the inner goings on of
/// the resharding loop mainly for debugging purposes.
async fn resharding_loop(
    requested_status: &mut watch::Receiver<(ReshardingStatus, u64)>,
    actual_status: &mut watch::Sender<(ReshardingStatus, u64)>,
    state: &Rc<NoYieldsRefCell<ReshardingState>>,
) -> Result<()> {
    let (_curr_status, curr_version) = actual_status.get();
    // Is updated in `ReshardingLoop::do_resharding`
    let (want_status, next_version) = requested_status.get();
    tlog!(Debug, "requested status: {want_status:?} {next_version}");

    if want_status == ReshardingStatus::Idle || next_version == curr_version {
        // Sleep until a resharding action is requested
        _ = requested_status.changed().await;
        return Ok(());
    }
    debug_assert_eq!(want_status, ReshardingStatus::Resharding);

    let node = node::global()?;
    let my_instance_name = node.topology_cache.my_instance_name();
    let i_am_replicaset_master = node.topology_cache.with(|topology_ref| {
        topology_ref
            .this_replicaset()
            .effective_master_name()
            .map(|n| &**n)
            == Some(my_instance_name)
    });

    // Only master has the right to do resharding
    if !i_am_replicaset_master {
        tlog!(Debug, "not replicaset master, going to sleep");

        _ = node.wait_index_change(Duration::from_secs(10));
        return Ok(());
    }

    // Check what action is needed
    let action = plan_a_resharding_action(state)?;

    // Execute the action
    type Action = ReshardingAction;
    match action {
        Action::GoIdle => {
            tlog!(Info, "resharding_loop_status = 'GoIdle'");

            _ = actual_status.send((ReshardingStatus::Idle, next_version));
            return Ok(());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// action plan
////////////////////////////////////////////////////////////////////////////////

enum ReshardingAction {
    // No action is needed, go to sleep until an action is requested by governor
    GoIdle,
}

fn plan_a_resharding_action(state: &NoYieldsRefCell<ReshardingState>) -> Result<ReshardingAction> {
    let node = node::global()?;

    let topology_ref = node.topology_cache.get();
    let target_bucket_state_version = topology_ref.this_replicaset().target_bucket_state_version;
    let current_bucket_state_version = topology_ref.this_replicaset().current_bucket_state_version;

    ////////////////////////////////////////////////////////////////////////////
    //
    // check if bucket state version is up to date
    //
    if target_bucket_state_version == current_bucket_state_version {
        // Version is already actualized which means no action is needed
        tlog!(Info, "replicaset distribution version is actualized");

        return Ok(ReshardingAction::GoIdle);
    }

    _ = state;

    tlog!(Warning, "resharding not yet implemented");
    return Ok(ReshardingAction::GoIdle);
}
