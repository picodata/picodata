use crate::config::AlterSystemParametersRef;
use crate::instance::Instance;
use crate::tlog;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::util::NoYieldsRefCell;
use smol_str::format_smolstr;
use smol_str::SmolStr;
use std::borrow::Cow;
use std::cell::Cell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::fiber;
use tarantool::time::Instant;

////////////////////////////////////////////////////////////////////////////////
// InstanceReachabilityManager
////////////////////////////////////////////////////////////////////////////////

/// A struct holding information about reported attempts to communicate with
/// all known instances.
#[derive(Debug)]
pub struct InstanceReachabilityManager {
    parameters: AlterSystemParametersRef,
    infos: HashMap<RaftId, InstanceReachabilityInfo>,
    lagging_applied: VecDeque<(RaftIndex, Instant)>,
    applied: RaftIndex,
}

pub type InstanceReachabilityManagerRef = Rc<NoYieldsRefCell<InstanceReachabilityManager>>;

#[inline(always)]
pub fn instance_reachability_manager(
    parameters: AlterSystemParametersRef,
    applied: RaftIndex,
) -> InstanceReachabilityManagerRef {
    Rc::new(NoYieldsRefCell::new(InstanceReachabilityManager::new(
        parameters, applied,
    )))
}

impl InstanceReachabilityManager {
    const MAX_HEARTBEAT_PERIOD: Duration = Duration::from_secs(5);

    pub fn new(parameters: AlterSystemParametersRef, applied: RaftIndex) -> Self {
        Self {
            parameters,
            infos: Default::default(),
            lagging_applied: VecDeque::new(),
            applied,
        }
    }

    pub fn update_instance(&mut self, old: Option<&Instance>, new: Option<&Instance>) {
        match (old, new) {
            (Some(old), Some(new)) => {
                if old.raft_id != new.raft_id {
                    // Raft id changed (this is possible when old instance was
                    // expelled and then we added another instance with old
                    // instance name, because instance_name is the primary key)
                    if let Some(info) = self.infos.remove(&old.raft_id) {
                        tlog!(
                            Debug,
                            "dropped reachability info for instance {old:?}, {info:?}"
                        );
                    }
                }

                if old.target_state.incarnation < new.target_state.incarnation {
                    // Instance attempts to change it's state to Online, let's
                    // reset it's replication error and give it a chances to
                    // come back (perhaps the admin has fixed the error manually)
                    self.reset_replication_error(new.raft_id);
                }
            }

            (Some(old), None) => {
                // Instance was removed, let's clean up info about it
                if let Some(info) = self.infos.remove(&old.raft_id) {
                    tlog!(
                        Debug,
                        "dropped reachability info for instance {old:?}, {info:?}"
                    );
                }
            }

            _ => {
                // Other cases do not concern reachability manager
            }
        }
    }

    /// Is called from a connection pool worker loop to report results of raft
    /// messages sent to other instances. For example a timeout is considered
    /// a failure. Updates info for the given instance.
    ///
    /// `applied` is the index of the last applied raft log entry of that instance.
    pub fn report_communication_result(
        &mut self,
        raft_id: RaftId,
        success: bool,
        applied: Option<RaftIndex>,
        is_leader_unknown: Option<bool>,
    ) {
        let now = fiber::clock();
        let info = self.infos.entry(raft_id).or_default();
        info.raft_id = raft_id;
        info.last_attempt = Some(now);

        // Even if it was previously reported as unreachable another message was
        // sent to it, so raft node state may have changed and another
        // report_unreachable may be needed.
        info.is_reported.set(false);

        if let Some(is_leader_unknown) = is_leader_unknown {
            info.is_leader_unknown = is_leader_unknown;
        }

        if success {
            info.last_success = Some(now);
            info.fail_streak = 0;
            info.fail_streak_start = None;
            // If was previously reported as unreachable, it's now reachable so
            // next time it should again be reported as unreachable.
        } else {
            if info.fail_streak == 0 {
                info.fail_streak_start = Some(now);
            }
            info.fail_streak += 1;
        }

        if let Some(applied) = applied {
            let (last_applied, time_changed) =
                info.last_applied_index.get_or_insert((applied, now));
            if *last_applied != applied {
                *last_applied = applied;
                *time_changed = now;
            }
        }
    }

    /// Is called at the beginning of the raft main loop to get information
    /// about which instances should be reported as unreachable to the raft node.
    pub fn take_unreachables_to_report(&mut self) -> Vec<RaftId> {
        let auto_offline_timeout = self.parameters.borrow().governor_auto_offline_timeout();
        let check_replication_error = self.parameters.borrow().governor_check_replication_error;

        let mut res = Vec::new();
        for (&raft_id, info) in &self.infos {
            if info.is_reported.get() {
                // Don't report nodes repeatedly.
                continue;
            }
            let status =
                self.check_reachability(auto_offline_timeout, check_replication_error, info, false);
            if matches!(status, Unreachable(_)) {
                res.push(raft_id);
                info.is_reported.set(true);
            }
        }
        res
    }

    /// Is called by sentinel to get information about which instances should be
    /// automatically assigned a different state.
    ///
    /// Returns a mapping from `raft_id` to unreachability reason.
    pub fn get_unreachables(&self) -> HashMap<RaftId, SmolStr> {
        let auto_offline_timeout = self.parameters.borrow().governor_auto_offline_timeout();
        let check_replication_error = self.parameters.borrow().governor_check_replication_error;

        let mut res = HashMap::new();
        for (&raft_id, info) in &self.infos {
            let status =
                self.check_reachability(auto_offline_timeout, check_replication_error, info, true);
            if let Unreachable(reason) = status {
                res.insert(raft_id, reason);
            }
        }
        return res;
    }

    /// Make a decision on the given instance's reachability based on the `info`.
    ///
    /// This is an internal function.
    fn check_reachability(
        &self,
        auto_offline_timeout: Duration,
        check_replication_error: bool,
        info: &InstanceReachabilityInfo,
        need_reason: bool,
    ) -> ReachabilityState {
        let reachability =
            Self::check_reachability_via_rpc(auto_offline_timeout, info, need_reason);
        if matches!(reachability, Unreachable(_)) {
            return reachability;
        }

        let now = fiber::clock();
        if check_replication_error {
            if let Some((err, start)) = info.replication_error.as_deref() {
                let since_start = now.duration_since(*start);
                let reason = if need_reason {
                    format_smolstr!("Replication broken for {since_start:.3?}: {err}")
                } else {
                    "".into()
                };
                return Unreachable(reason);
            }
        }

        let Some((their_applied, they_applied_at)) = info.last_applied_index else {
            // Applied index not known yet, can't make a decision based on that info
            return reachability;
        };

        let raft_id = info.raft_id;
        let our_applied = self.applied;

        if their_applied >= our_applied {
            // Applied index is up to date with ours, all good
            return reachability;
        }

        let now = fiber::clock();
        let since_they_applied = now.duration_since(they_applied_at);
        if since_they_applied < auto_offline_timeout {
            // They have applied their latest entry recently enough,
            // assume they still have ability to apply other entries
            return reachability;
        }

        // Their applied index is lagging behind ours

        let Some(since_we_applied_next) = self.since_we_applied_this(now, their_applied + 1) else {
            // We don't have history yet. Let's not make a decision yet
            return reachability;
        };

        if since_we_applied_next < auto_offline_timeout {
            // The next entry which they haven't yet applied has been added
            // not so long ago. Let's give them some time to catch up
            return reachability;
        }

        // Applied index is lagging and hasn't changed for too long, this
        // means that the instance's raft_main_loop is probably stuck and
        // can't progress for some reason. This could be caused by storage
        // corruption because of an admin's mistake or a bug in our code.
        // Anyway we want to mark such instances as 'Offline' so that it's
        // easier to notice them and also so that they don't affect the
        // cluster in a negative way.
        if info.rate_limit_broken_raft_error_message(now, their_applied) {
            tlog!(Warning, "broken raft main loop detected on instance: raft_id: {raft_id}, their_applied: {their_applied}, since_they_applied: {since_they_applied:?}, our_applied: {our_applied}, since_we_applied_next: {since_we_applied_next:?}");
        }
        let reason = if need_reason {
            format_smolstr!("Applied index {their_applied} hasn't changed for {since_they_applied:.3?} (expected {since_we_applied_next:.3?})")
        } else {
            "".into()
        };
        Unreachable(reason)
    }

    /// Make a decision on the given instance's reachability based on the
    /// `info` about recent RPC communication attempts.
    /// This is an internal function.
    fn check_reachability_via_rpc(
        auto_offline_timeout: Duration,
        info: &InstanceReachabilityInfo,
        need_reason: bool,
    ) -> ReachabilityState {
        if let Some(last_success) = info.last_success {
            if info.fail_streak == 0 {
                // Didn't fail once, so can't be unreachable.
                return Reachable;
            }
            let now = fiber::clock();
            let since_success = now.duration_since(last_success);
            if since_success > auto_offline_timeout {
                let reason = if need_reason {
                    format_smolstr!("No successful RPC for {since_success:.3?}")
                } else {
                    "".into()
                };
                return Unreachable(reason);
            } else {
                return Reachable;
            }
        }

        if let Some(first_fail) = info.fail_streak_start {
            let now = fiber::clock();
            let since_start = now.duration_since(first_fail);
            if since_start > auto_offline_timeout {
                let reason = if need_reason {
                    format_smolstr!("No successful RPC (at all) for {since_start:.3?}")
                } else {
                    "".into()
                };
                return Unreachable(reason);
            }
        }

        // Don't make decisions about instances which didn't previously
        // respond once so as to not interrupt the process of booting up.
        return Undecided;
    }

    /// Is called from raft main loop when handling raft messages
    ///  to determine whether to actually send a heartbeat to an instance.
    ///
    /// If an instance was previously determined unreachable, heartbeats will be sent less often
    ///  (with an exponential backoff).
    ///
    /// It will also limit the frequency of heartbeats to online learners to approximately
    ///  three times per `auto_offline_timeout`
    pub fn should_send_heartbeat_this_tick(&self, to: RaftId, is_learner: bool) -> bool {
        let Some(info) = self.infos.get(&to) else {
            // No attempts were registered yet.
            return true;
        };

        if info.fail_streak == 0 {
            // we want to reduce the amount of traffic sent to online learners
            if let Some(last_attempt) = info.last_attempt {
                // Do not delay heartbeats to nodes that claim to not know who the leader is
                // The heartbeat is required for them to learn that we are the leader
                if info.is_leader_unknown {
                    return true;
                }

                if is_learner {
                    let auto_offline_timeout =
                        self.parameters.borrow().governor_auto_offline_timeout();
                    let learner_heartbeat_period = auto_offline_timeout / 3;

                    let now = fiber::clock();
                    return now > last_attempt + learner_heartbeat_period;
                }
            }

            // Last attempt was successful, keep going.
            return true;
        }

        let Some(last_success) = info.last_success else {
            // Didn't succeed once, keep trying.
            return true;
        };

        let last_attempt = info
            .last_attempt
            .expect("this should be set if info was reported");

        // Expontential backoff.
        // time: -----*---------*---------*-------------------*---------------->
        //            ^         ^         ^                   ^
        // last_success         attempt1  attempt2            attempt3   ...
        //
        //            |<------->|<------->|
        //                D1         D1
        //            |<----------------->|<----------------->|
        //                     D2                   D2
        //            |<------------------------------------->|
        //                                D3
        //                                ...
        // DN == attemptN.duration_since(last_success)
        //
        let now = fiber::clock();
        let since_last_success = last_attempt.duration_since(last_success);
        let delay = since_last_success.min(Self::MAX_HEARTBEAT_PERIOD);
        if now > last_attempt + delay {
            return true;
        }

        return false;
    }

    /// Called each time our applied index changes. Keeps track of times when
    /// raft entries were applied for some amount of entries in the past.
    ///
    /// This info then used in [`Self::since_we_applied_this`].
    pub fn on_applied(&mut self, new_applied: RaftIndex) {
        let now = fiber::clock();
        self.applied = new_applied;
        self.lagging_applied.push_back((new_applied, now));
        self.cleanup_applied_index_history(now);

        debug_assert!(
            self.lagging_applied
                .iter()
                .is_sorted_by(|(li, lt), (ri, rt)| li <= ri && lt <= rt),
            "{:?}",
            self.lagging_applied
        );
    }

    /// Returns the duration since we have applied the given `index`.
    /// This is used to determine instances which are lagging behind our
    /// raft main loop.
    ///
    /// See usage in [`Self::check_reachability`] for more details.
    pub fn since_we_applied_this(&self, now: Instant, index: RaftIndex) -> Option<Duration> {
        let Some(&(oldest_known, _)) = self.lagging_applied.front() else {
            // We don't have the history yet. This is possible right after
            // restart. In this case we probably don't want to make any
            // decisitions about other people's raft main loop viability
            return None;
        };

        if index < oldest_known {
            // We applied this index too long ago to remember
            return Some(Duration::MAX);
        }

        for &(old_index, time) in &self.lagging_applied {
            if index == old_index {
                return Some(now.duration_since(time));
            }
        }

        // This shouldn't be happening, but I don't want to add an avoidable panic.
        // Just pretend we've just applied that entry
        Some(Duration::ZERO)
    }

    pub fn cleanup_applied_index_history(&mut self, now: Instant) {
        let auto_offline_timeout = self.parameters.borrow().governor_auto_offline_timeout();
        let too_far_back = now.saturating_sub(auto_offline_timeout);

        // NOTE: I wanted to use `retain` at first, but it's optimized for some
        // other case, and this is better
        while let Some(&(_, time)) = self.lagging_applied.front() {
            if time > too_far_back {
                // Keep a recent history of applied indexes
                break;
            }

            if self.lagging_applied.len() <= 2 {
                // Want to keep track of at least one index behind the latest
                // one, so that if there's no raft entries for a long time we
                // don't immediately make everyone Offline.
                break;
            }

            self.lagging_applied.pop_front();
        }
    }

    pub fn set_replication_error<'a>(
        &mut self,
        raft_id: RaftId,
        error: impl Into<Cow<'a, BoxError>>,
    ) {
        let info = self.infos.entry(raft_id).or_default();
        if info.replication_error.is_some() {
            // Only keep the first occurence of the error for the timestamp
            return;
        }

        let now = fiber::clock();
        let error = error.into().into_owned();
        info.replication_error = Some(Box::new((error, now)));
    }

    pub fn reset_replication_error(&mut self, raft_id: RaftId) {
        if let Some(info) = self.infos.get_mut(&raft_id) {
            info.replication_error = None;
        }
    }

    pub fn get_replication_error(&self, raft_id: RaftId) -> Option<&(BoxError, Instant)> {
        let info = self.infos.get(&raft_id)?;
        info.replication_error.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReachabilityState {
    Undecided,
    Reachable,
    Unreachable(SmolStr),
}
use ReachabilityState::*;

/// Information about recent attempts to communicate with a single given instance.
#[derive(Debug, Default, Clone)]
pub struct InstanceReachabilityInfo {
    pub raft_id: RaftId,

    pub last_success: Option<Instant>,
    pub last_attempt: Option<Instant>,

    pub fail_streak_start: Option<Instant>,
    pub fail_streak: u32,

    /// A pair of index of last applied raft log entry and the instant when
    /// this index changed.
    pub last_applied_index: Option<(RaftIndex, Instant)>,

    /// The node claims to not know who the leader is.
    ///
    /// Do not delay heartbeats to it, so it can learn that we are the leader.
    pub is_leader_unknown: bool,

    /// If this is `Some`, then we have detected a replication error (probably
    /// conflict) which prevents this instance from synchronizing with the rest
    /// of the replicaset.
    pub replication_error: Option<Box<(BoxError, Instant)>>,

    /// If this is true then this instance was reported to `raft-rs` as
    /// unreachable. We keep this flag to avoid redundant reports.
    pub is_reported: Cell<bool>,

    /// Info needed for rate limiting error reports in logs
    pub last_warning: Cell<Option<(Instant, RaftIndex)>>,
}

impl InstanceReachabilityInfo {
    /// This functions helps reduce spamming of messages about broken raft main loop
    fn rate_limit_broken_raft_error_message(&self, now: Instant, applied: RaftIndex) -> bool {
        // If didn't report once, then should report
        let Some((reported_at, reported_applied)) = self.last_warning.get() else {
            self.last_warning.set(Some((now, applied)));
            return true;
        };

        // Should report if their applied index changed, but they're still broken
        if reported_applied != applied {
            self.last_warning.set(Some((now, applied)));
            return true;
        }

        // Don't report more often than once per minute
        if now.duration_since(reported_at) > Duration::from_secs(60) {
            self.last_warning.set(Some((now, applied)));
            return true;
        }

        false
    }
}
