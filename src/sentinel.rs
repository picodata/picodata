use crate::has_states;
use crate::instance::StateVariant::*;
use crate::proc_name;
use crate::reachability::InstanceReachabilityManagerRef;
use crate::rpc;
use crate::rpc::update_instance::proc_update_instance;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::network::ConnectionPool;
use crate::traft::RaftIndex;
use crate::traft::{node, RaftSpaceAccess};
use crate::util::NoYieldsRefCell;
use ::tarantool::fiber;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;
use ::tarantool::fiber::r#async::watch;
use picodata_plugin::error_code::error_code_is_retriable_for_cas;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::error::IntoBoxError;
use tarantool::time::Instant;

impl Loop {
    /// A value for non-urgent timeouts, e.g. nothing needed to be done during
    /// a loop iteration.
    const SENTINEL_LONG_SLEEP: Duration = Duration::from_secs(1);

    /// A value for urgent timeouts, e.g. retry of failed update peer request.
    const SENTINEL_SHORT_RETRY: Duration = Duration::from_millis(300);

    const UPDATE_INSTANCE_TIMEOUT: Duration = Duration::from_secs(3);

    /// A timeout for waiting for the raft log to progress before sending
    /// another request to the raft leader. This needs to be a big
    /// number, because we only do this when we really expect a raft
    /// entry to be applied.
    /// It's not set to infinity only because I'm worried about unforseen bugs.
    const RAFT_LOG_BARRIER_TIMEOUT: Duration = Duration::from_secs(60 * 60);

    /// When doing an exponential backoff this is the maximum value.
    const SENTINEL_BACKOFF_MAX_DURATION: Duration = Duration::from_secs(10 * 60);

    async fn iter_fn(state: &mut State) -> ControlFlow<()> {
        let pool = &state.pool;
        let raft_storage = &state.raft_storage;
        let raft_status = &state.raft_status;
        let status = &mut state.status;
        let instance_reachability = &state.instance_reachability;
        let stats = &mut state.stats;

        if status.get() == SentinelStatus::Initial || node::global().is_err() {
            tlog!(Debug, "waiting until initialized...");
            _ = status.changed().timeout(Self::SENTINEL_LONG_SLEEP).await;
            return ControlFlow::Continue(());
        }

        let node = node::global().expect("just checked it's ok");
        let cluster_name = raft_storage.cluster_name().expect("storage shouldn't fail");
        let cluster_uuid = raft_storage.cluster_uuid().expect("storage shouldn't fail");

        ////////////////////////////////////////////////////////////////////////
        // Awoken during graceful shutdown.
        // Should change own target state to Offline and finish.
        if status.get() == SentinelStatus::ShuttingDown {
            set_action_kind(&mut stats.borrow_mut(), ActionKind::Shutdown);

            let my_target_state = node.topology_cache.my_target_state();
            if my_target_state.variant == Expelled {
                tlog!(Debug, "instance has been expelled, sentinel out");
                return ControlFlow::Break(());
            }

            // Topology cache state corresponds to this applied index.
            let index = node.get_index();
            let instance_name = node.topology_cache.my_instance_name().into();
            let req = rpc::update_instance::Request::new(instance_name, cluster_name, cluster_uuid)
                .with_target_state(Offline);

            let mut attempt_number = 0;
            loop {
                attempt_number += 1;
                tlog!(
                    Info,
                    "setting own target state Offline, attempt number {attempt_number}"
                );
                // Adjust the timeout based on the number of retries
                let timeout =
                    current_fail_streak_timeout(&stats.borrow(), Self::SENTINEL_SHORT_RETRY);

                let now = fiber::clock();
                let res = async {
                    let Some(leader_id) = raft_status.get().leader_id else {
                        return Err(Error::LeaderUnknown);
                    };
                    pool.call(&leader_id, proc_name!(proc_update_instance), &req, timeout)?
                        .await?;
                    Ok(())
                }
                .await;
                match res {
                    Ok(_) => {
                        // NOTE: in this case the result is not going to affect
                        // anything because the sentinel loop is finishing after
                        // it confirms that the instance's target_state has been
                        // changed to Offline. But we still register the result
                        // for consistency
                        register_successful_connection(&mut stats.borrow_mut(), index);
                        return ControlFlow::Break(());
                    }
                    Err(e) => {
                        tlog!(Warning,
                            "failed setting own target state Offline: {e}, retrying ...";
                        );
                        register_failed_attempt(&mut stats.borrow_mut(), e.into_box_error());
                        fiber::sleep(timeout.saturating_sub(now.elapsed()));
                        continue;
                    }
                }
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // When running on leader, find any unreachable instances which need to
        // have their state automatically changed.
        if raft_status.get().raft_state.is_leader() {
            let topology_ref = node.topology_cache.get();
            let instances = topology_ref.all_instances();
            let index = node.get_index();
            let unreachables = instance_reachability.borrow().get_unreachables(index);
            let mut instance_to_downgrade = None;
            for instance in instances {
                if has_states!(instance, * -> Online) && unreachables.contains(&instance.raft_id) {
                    instance_to_downgrade = Some(instance.name.clone());
                    break;
                }
            }
            // Must not hold across yields
            drop(topology_ref);

            let Some(instance_name) = instance_to_downgrade else {
                _ = status.changed().timeout(Self::SENTINEL_LONG_SLEEP).await;
                return ControlFlow::Continue(());
            };

            // Topology cache state corresponds to this applied index.
            let index = node.get_index();
            set_action_kind(&mut stats.borrow_mut(), ActionKind::AutoOfflineByLeader);

            tlog!(Info, "setting target state Offline"; "instance_name" => %instance_name);
            let req = rpc::update_instance::Request::new(
                instance_name.clone(),
                cluster_name,
                cluster_uuid,
            )
            // We only try setting the state once and if a CaS conflict
            // happens we should reassess the situation, because somebody
            // else could have changed this particular instance's target state.
            .with_dont_retry(true)
            .with_target_state(Offline);

            let res = rpc::update_instance::handle_update_instance_request_and_wait(
                req,
                Self::UPDATE_INSTANCE_TIMEOUT,
            );
            if let Err(e) = res {
                tlog!(Warning,
                    "failed setting target state Offline: {e}";
                    "instance_name" => %instance_name,
                );
                // NOTE: we only track failures in this case for debuggin purposes,
                // we don't do exponential backoff when running on leader because
                // the request is handled locally, so there's no likelihood of
                // overwhelming someone else with our RPC requests and network
                // connectivity should not affect us as much. Though there's
                // always a possiblity that we didn't think of something and in
                // the future this may change...
                register_failed_attempt(&mut stats.borrow_mut(), e.into_box_error());
            } else {
                register_successful_connection(&mut stats.borrow_mut(), index);
            }

            _ = status.changed().timeout(Self::SENTINEL_SHORT_RETRY).await;
            return ControlFlow::Continue(());
        }

        ////////////////////////////////////////////////////////////////////////
        // When running not on leader, check if own target has automatically
        // changed to Offline and try to update it to Online.
        let my_target_state = node.topology_cache.my_target_state();
        if my_target_state.variant == Offline {
            set_action_kind(&mut stats.borrow_mut(), ActionKind::AutoOnlineBySelf);

            if exponential_backoff_before_retry(&stats.borrow(), Self::SENTINEL_SHORT_RETRY)
                && raft_log_barrier_is_passed(&stats.borrow(), node)
            {
                // Topology cache state corresponds to this applied index.
                let index = node.get_index();
                let instance_name = node.topology_cache.my_instance_name().into();

                tlog!(Info, "setting own target state Online");
                let req =
                    rpc::update_instance::Request::new(instance_name, cluster_name, cluster_uuid)
                        // We only try setting the state once and if a CaS conflict
                        // happens we should reassess the situation, because somebody
                        // else could have changed this particular instance's target state.
                        .with_dont_retry(true)
                        .with_target_state(Online);
                let res = async {
                    let Some(leader_id) = raft_status.get().leader_id else {
                        return Err(Error::LeaderUnknown);
                    };
                    crate::error_injection!("SENTINEL_CONNECTION_POOL_CALL_FAILURE" =>
                        return Err(BoxError::new(crate::error_code::ErrorCode::Other, "injected error").into()));
                    pool.call(
                        &leader_id,
                        proc_name!(proc_update_instance),
                        &req,
                        Self::UPDATE_INSTANCE_TIMEOUT,
                    )?
                    .await?;
                    Ok(())
                }
                .await;

                match res {
                    Ok(()) => {
                        register_successful_connection(&mut stats.borrow_mut(), index);
                    }
                    Err(e) => {
                        tlog!(Warning, "failed setting own target state Online: {e}");

                        let e = e.into_box_error();
                        if error_code_is_retriable_for_cas(e.error_code()) {
                            // NOTE: even though we get an error, this is a
                            // special kind of error for our purposes, because
                            // it means that the request has reached the
                            // destination, but there was a problem with the
                            // request, so it should be reconstructed after
                            // synchronizing with the raft log. For this reason
                            // we say the connection was successful
                            register_successful_connection(&mut stats.borrow_mut(), index);
                        } else {
                            register_failed_attempt(&mut stats.borrow_mut(), e);
                        }
                    }
                }
            }

            _ = status.changed().timeout(Self::SENTINEL_SHORT_RETRY).await;
            return ControlFlow::Continue(());
        }

        _ = status.changed().timeout(Self::SENTINEL_LONG_SLEEP).await;
        return ControlFlow::Continue(());
    }

    pub fn start(
        pool: Rc<ConnectionPool>,
        raft_status: watch::Receiver<node::Status>,
        raft_storage: RaftSpaceAccess,
        instance_reachability: InstanceReachabilityManagerRef,
    ) -> Self {
        let (status_tx, status_rx) = watch::channel(SentinelStatus::Initial);

        let stats = ContinuityTracker::default();
        let stats = Rc::new(NoYieldsRefCell::new(stats));

        let state = State {
            pool,
            raft_storage,
            raft_status,
            status: status_rx,
            instance_reachability,
            stats: stats.clone(),
        };

        Self {
            fiber_id: crate::loop_start!("sentinel_loop", Self::iter_fn, state),
            status: status_tx,
            stats,
        }
    }

    #[inline]
    pub fn for_tests() -> Self {
        let (status, _) = watch::channel(SentinelStatus::Initial);

        Self {
            fiber_id: 0,
            status,
            stats: Default::default(),
        }
    }

    pub fn on_shut_down(&self) {
        self.status
            .send(SentinelStatus::ShuttingDown)
            .expect("we shouldn't be holding references to the value")
    }

    pub fn on_self_activate(&self) {
        self.status
            .send(SentinelStatus::Activated)
            .expect("we shouldn't be holding references to the value")
    }
}

pub struct Loop {
    #[allow(dead_code)]
    fiber_id: fiber::FiberId,
    status: watch::Sender<SentinelStatus>,
    pub stats: Rc<NoYieldsRefCell<ContinuityTracker>>,
}

/// Describes possible states of the current instance with respect to what
/// sentinel should be doing.
///
/// TODO: maybe this should be merged with [`node::Status`].
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
enum SentinelStatus {
    /// Instance has started, but didn't yet receive confirmation from the
    /// leader that it was activated.
    #[default]
    Initial,

    /// Instance has been activated, sentinel is doing it's normal job.
    Activated,

    /// Instance is currently gracefully shutting down.
    ShuttingDown,
}

struct State {
    pool: Rc<ConnectionPool>,
    raft_storage: RaftSpaceAccess,
    raft_status: watch::Receiver<node::Status>,
    status: watch::Receiver<SentinelStatus>,
    instance_reachability: InstanceReachabilityManagerRef,
    stats: Rc<NoYieldsRefCell<ContinuityTracker>>,
}

#[derive(Default)]
pub struct ContinuityTracker {
    /// Keeps track of the last action attempted for the purpose of fail streak handling.
    pub last_action_kind: Option<ActionKind>,

    /// Applied index and timestamp at the moment when the last successful
    /// attempt was made. This is used for the raft log barrier before
    /// successive requests to prevent a storm of requests in case of asymmetric
    /// connectivity failure.
    pub last_successful_attempt: Option<(RaftIndex, Instant)>,

    pub fail_streak: Option<FailStreakInfo>,
}

#[inline(always)]
fn set_action_kind(stats: &mut ContinuityTracker, kind: ActionKind) {
    let Some(last_action_kind) = stats.last_action_kind else {
        // This is our first action
        stats.last_action_kind = Some(kind);
        debug_assert!(
            stats.last_successful_attempt.is_none(),
            "{:?}",
            stats.last_successful_attempt
        );
        debug_assert!(stats.fail_streak.is_none(), "{:?}", stats.fail_streak);
        return;
    };

    if last_action_kind != kind {
        // Last action was of a different kind, reset the fail streak
        stats.last_action_kind = Some(kind);
        stats.fail_streak = None;
    }

    // Last action was of the same kind, nothing to change
}

fn register_successful_connection(stats: &mut ContinuityTracker, index: RaftIndex) {
    stats.fail_streak = None;

    stats.last_successful_attempt = Some((index, fiber::clock()));
}

fn register_failed_attempt(stats: &mut ContinuityTracker, error: BoxError) {
    let Some(fail_streak) = &mut stats.fail_streak else {
        // This is the first failure so far, start a new fail streak
        stats.fail_streak = Some(FailStreakInfo::start(error));
        return;
    };

    if fail_streak.error.error_code() != error.error_code() {
        // There already was a fail streak but the error code changed,
        // so we start a new fail streak
        *fail_streak = FailStreakInfo::start(error);
        return;
    }

    // The same fail streak is continuing
    fail_streak.count += 1;
    fail_streak.mult = fail_streak.mult.saturating_mul(2);
    fail_streak.last_try = fiber::clock();
    // Update the error anyway, because we didn't check the error message
    // and it could change and it's a good idea to keep up with the latest info
    fail_streak.error = error;
}

/// Returns `true` if we consider the raft log barrier to be passed, meaning
/// that it's ok to send another request
fn raft_log_barrier_is_passed(stats: &ContinuityTracker, node: &node::Node) -> bool {
    let Some((index, time)) = stats.last_successful_attempt else {
        // No successful requests have happened yet, so barrier is not needed
        return true;
    };

    if index < node.get_index() {
        // A new raft entry was applied since our last successful request, so it's
        // ok to issue another one. This is important for the CAS to work properly
        return true;
    }

    if fiber::clock().duration_since(time) > Loop::RAFT_LOG_BARRIER_TIMEOUT {
        // Our local raft log is not progressing, but it's been a while since last attempt.
        // This is a failsafe in case we have a bug somewhere so that we don't block indefinitely
        return true;
    }

    return false;
}

fn exponential_backoff_before_retry(stats: &ContinuityTracker, base_timeout: Duration) -> bool {
    let Some(fail_streak) = &stats.fail_streak else {
        // No failures so far, it's ok to send the request
        return true;
    };

    let elapsed_since_last_try = fiber::clock().duration_since(fail_streak.last_try);

    // Only try again once it's been `base_timeout` times two to the power of <number of failed attempts>
    // since the last failed attempt.
    //
    // For example let's say `base_timeout` = 300ms, then
    // 2nd attempt happens 300ms after 1st one,
    // 3rd attempt happens 600ms after 2nd one, hence 900ms after 1st one
    // 4th attempt happens 1.2s after 3rd one, hence 2.1s after 1st one, etc...
    let its_time_to_try_again =
        elapsed_since_last_try > current_fail_streak_timeout(stats, base_timeout);
    return its_time_to_try_again;
}

/// Time to wait since last failed attempt before making another attempt
#[inline(always)]
fn current_fail_streak_timeout(stats: &ContinuityTracker, base_timeout: Duration) -> Duration {
    let Some(fail_streak) = &stats.fail_streak else {
        // No failures so far, the base timeout is applicable
        return base_timeout;
    };

    Loop::SENTINEL_BACKOFF_MAX_DURATION.min(base_timeout.saturating_mul(fail_streak.mult))
}

tarantool::define_str_enum! {
    pub enum ActionKind {
        /// Attempt to set this instance's target_state to Offline in the process of
        /// graceful shutdown.
        /// Happens on any instance.
        Shutdown = "shutdown",

        /// Attempt to set this instance's target_state to Online after a temporary
        /// connectivity failure.
        /// Happens on any instance.
        AutoOnlineBySelf = "auto online by self",

        /// Attempt to set another instance's target_state to Offline when a
        /// connectivity faliure is detected.
        /// Only happens when running on raft leader.
        AutoOfflineByLeader = "auto offline by leader",
    }
}

#[derive(Debug, Clone)]
pub struct FailStreakInfo {
    /// Number of consequitive failures with the same error.
    pub count: u32,
    /// Multiplier for the timeout. This is the "exponential" part.
    pub mult: u32,
    /// Time of the first failure in the fail streak.
    pub start: Instant,
    /// Time of the last failure in the fail streak.
    pub last_try: Instant,
    /// The error value of the fail streak.
    /// If the error code changes the fail streak is reset.
    pub error: BoxError,
}

impl FailStreakInfo {
    #[inline(always)]
    pub fn start(error: BoxError) -> Self {
        let start = fiber::clock();
        Self {
            count: 1,
            mult: 1,
            start,
            last_try: start,
            error,
        }
    }
}
