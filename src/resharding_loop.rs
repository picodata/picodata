use crate::cas;
use crate::schema::ADMIN_ID;
use crate::simulation::platform::Platform;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::op::Dml;
use crate::traft::op::Op;
use crate::traft::RaftIndex;
use crate::util::NoYieldsRefCell;
use crate::Result;
use ::tarantool::fiber::r#async::watch;
use std::rc::Rc;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::error::IntoBoxError;
use tarantool::fiber;
use tarantool::fiber::r#async::timeout::IntoTimeout;
use tarantool::time::Instant;

pub mod action;
pub mod bucket_ranges;
mod bucket_version;
mod debug;
mod do_action;
mod find_sharded_bucket_updates;
mod initialize;
pub mod local_bucket;
mod plan;
pub mod test;

pub use action::ReshardingActionKind;

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
    pub state: Rc<NoYieldsRefCell<ReshardingLoopState>>,
}

/// [`resharding_loop`] sleeps for this many seconds when it encounters an
/// unhandled error before going on another loop iteration.
const RESHARDING_LOOP_SHORT_RETRY: Duration = Duration::from_millis(300);
pub const RESHARDING_LOOP_LONG_SLEEP: Duration = Duration::from_secs(10);

impl ReshardingLoop {
    pub fn start() -> Self {
        let (requested_status_tx, requested_status_rx) =
            watch::channel((ReshardingStatus::Idle, 0));
        let (actual_status_tx, actual_status_rx) = watch::channel((ReshardingStatus::Idle, 0));
        let state = ReshardingLoopState::default();
        let state = Rc::new(NoYieldsRefCell::new(state));
        let state_tx = state.clone();

        let fiber_id = fiber::Builder::new()
            .name("resharding_loop")
            .func_async(async move {
                let mut requested_status_rx = requested_status_rx;
                let mut actual_status_tx = actual_status_tx;

                // The fiber starts before the global node is initialized, but
                // resharding_loop requires it, so we wait explicitly. Thankfully
                // we only have to do this once at program start.
                let node;
                loop {
                    let Ok(n) = node::global() else {
                        _ = requested_status_rx
                            .changed()
                            .timeout(RESHARDING_LOOP_SHORT_RETRY)
                            .await;
                        continue;
                    };

                    node = n;
                    break;
                }

                loop {
                    let res = resharding_loop(
                        node,
                        &state_tx,
                        &mut requested_status_rx,
                        &mut actual_status_tx,
                    )
                    .await;
                    match res {
                        Ok(()) => {
                            // TODO backoff
                        }
                        Err(e) => {
                            tlog!(Warning, "unhandled error: {e}");

                            state_tx.borrow_mut().last_error = Some(e.into_box_error());

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

    pub async fn do_resharding(
        &self,
        deadline: Instant,
        request_status: ReshardingStatus,
    ) -> Result<()> {
        // Request is handled in `resharding_loop`.
        let waiting_for_version = self.request_action(request_status);

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

                if let Some(last_error) = self.state.borrow().last_error.clone() {
                    return Err(last_error.into());
                }

                return Err(Error::timeout());
            }
        }

        tlog!(Debug, "resharding completed");

        Ok(())
    }

    #[inline]
    pub fn request_action(&self, status: ReshardingStatus) -> u64 {
        let version = self.actual_status.get().1;
        let waiting_for_version = version + 1;

        _ = self.requested_status.send((status, waiting_for_version));

        // The above call to `send` will awake the fiber if it's currently
        // blocked on the status channel. But it could also be blocked on a call
        // to `wait_index`, in which case this call will wake that fiber up.
        tlog!(Debug, "wake resharding loop up");
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

/// An enumeration of states the `resharding_loop` can be in.
///
/// This is mainly internal implementation details, subject to change. Code in
/// other modules in general should not rely on these states to determine if
/// rebalancing is finished in the cluster and/or repliaset, instead the source
/// of truth should be consulted, i.e. `current_bucket_state_version` &
/// `target_bucket_state_version` columns in `_pico_tier` & `_pico_replicaset`.
///
/// See [`resharding_loop`] for illustrative usage examples.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReshardingStatus {
    /// No resharding action is being performed at the moment.
    #[default]
    Idle,
    /// Initial bucket distribution should be or is being created on this
    /// replicaset.
    Initialize,
    /// Buckets should be rebalanced between this and some other
    /// replicasets, but the action cannot be performed yet.
    ReshardingEnqueued,
    /// Buckets should be or are being rebalanced between this and some other
    /// replicasets.
    Resharding,
    /// Bucket rebalancing is being finalized on this replicaset.
    CleaningUp,
}

impl ReshardingStatus {
    #[inline(always)]
    pub fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }
}

#[derive(Default, Debug)]
pub struct ReshardingLoopState {
    pub last_action: Option<action::ReshardingActionKind>,

    /// If resharding_loop's iteration resulted in an error, it's value is
    /// stored here.
    pub last_error: Option<BoxError>,
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
pub(crate) async fn resharding_loop(
    platform: &impl Platform,
    state: &Rc<NoYieldsRefCell<ReshardingLoopState>>,
    requested_status: &mut watch::Receiver<(ReshardingStatus, u64)>,
    actual_status: &mut watch::Sender<(ReshardingStatus, u64)>,
) -> Result<()> {
    let (_curr_status, _curr_version) = actual_status.get();
    // Is updated in `ReshardingLoop::do_resharding`
    let (want_status, next_version) = requested_status.get();
    // This is needed so the next requested_status.changed().await call is only
    // awaken by changes happnening after this point
    requested_status.mark_seen();
    tlog!(Debug, "requested status: {want_status:?} {next_version}");

    let applied = platform.applied_index();

    //
    // Plan an action
    //

    let action = plan::plan_resharding_action(platform, state, want_status)?;

    //
    // Execute action
    //
    do_action::do_action(
        platform,
        state,
        requested_status,
        actual_status,
        applied,
        action,
    )
    .await
}

////////////////////////////////////////////////////////////////////////////////
// miscellaneous
////////////////////////////////////////////////////////////////////////////////

// FIXME currently all resharding steps are going to be serialized:
// - send data to r1
// - do cas and wait
// - send data to r2
// - do cas and wait
// ...
//
// It's better if we don't block awaiting the CAS response and instead save that
// future into an array and go send a data chunk to another replicaset. So it
// looks more like this:
// - send data to r1
// - do cas don't wait
// - send data to r2
// - do cas don't wait
// - ...
// - wait for all cas responses at once
pub fn do_cas_requests(applied: RaftIndex, dmls: Vec<Dml>, timeout: Duration) -> Result<()> {
    tlog!(Debug, "sending cas request: {dmls:?}");
    let op = Op::single_dml_or_batch(dmls);
    let predicate = cas::Predicate::new(applied, []);
    let request = cas::Request::new(op, predicate, ADMIN_ID)?;
    let deadline = fiber::clock().saturating_add(timeout);
    let res = cas::compare_and_swap_and_wait(&request, deadline)?;
    // If a retriable error happens we just go to the next loop iteration and
    // try again
    res.no_retries()?;

    Ok(())
}
