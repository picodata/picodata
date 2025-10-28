use crate::storage::Catalog;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::util::NoYieldsRefCell;
use std::cell::Cell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::Duration;
use tarantool::fiber;
use tarantool::time::Instant;

////////////////////////////////////////////////////////////////////////////////
// InstanceReachabilityManager
////////////////////////////////////////////////////////////////////////////////

/// A struct holding information about reported attempts to communicate with
/// all known instances.
#[derive(Debug)]
pub struct InstanceReachabilityManager {
    storage: Catalog,
    // TODO: we should cache the whole db_config in a static variable and update
    // it from raft_main_loop the same way we handle TopologyCache.
    auto_offline_timeout: Cell<Duration>,
    infos: HashMap<RaftId, InstanceReachabilityInfo>,
}

pub type InstanceReachabilityManagerRef = Rc<NoYieldsRefCell<InstanceReachabilityManager>>;

#[inline(always)]
pub fn instance_reachability_manager(storage: Catalog) -> InstanceReachabilityManagerRef {
    Rc::new(NoYieldsRefCell::new(InstanceReachabilityManager::new(
        storage,
    )))
}

impl InstanceReachabilityManager {
    const MAX_HEARTBEAT_PERIOD: Duration = Duration::from_secs(5);

    pub fn new(storage: Catalog) -> Self {
        let auto_offline_timeout = storage
            .db_config
            .governor_auto_offline_timeout()
            .expect("storage aint gonna fail");
        Self {
            storage,
            auto_offline_timeout: Cell::new(auto_offline_timeout),
            infos: Default::default(),
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
        info.last_attempt = Some(now);

        // Even if it was previously reported as unreachable another message was
        // sent to it, so raft node state may have changed and another
        // report_unreachable me be needed.
        info.is_reported = false;

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
    ///
    /// `applied` is the index of the last applied raft log entry of the current instance.
    pub fn take_unreachables_to_report(&mut self, applied: RaftIndex) -> Vec<RaftId> {
        let auto_offline_timeout = self.auto_offline_timeout.get();

        let mut res = Vec::new();
        for (&raft_id, info) in &mut self.infos {
            if info.is_reported {
                // Don't report nodes repeatedly.
                continue;
            }
            let status = Self::check_reachability(auto_offline_timeout, applied, info);
            if status == Unreachable {
                res.push(raft_id);
                info.is_reported = true;
            }
        }
        res
    }

    /// Is called by sentinel to get information about which instances should be
    /// automatically assigned a different state.
    ///
    /// `applied` is the index of the last applied raft log entry of the current instance.
    pub fn get_unreachables(&self, applied: RaftIndex) -> HashSet<RaftId> {
        let auto_offline_timeout = self.auto_offline_timeout.get();

        let mut res = HashSet::new();
        for (&raft_id, info) in &self.infos {
            let status = Self::check_reachability(auto_offline_timeout, applied, info);
            if status == Unreachable {
                res.insert(raft_id);
            }
        }
        return res;
    }

    /// Make a decision on the given instance's reachability based on the `info`.
    ///
    /// `applied` is the index of the last applied raft log entry of the current instance.
    ///
    /// This is an internal function.
    fn check_reachability(
        auto_offline_timeout: Duration,
        our_applied: RaftIndex,
        info: &InstanceReachabilityInfo,
    ) -> ReachabilityState {
        let reachability = Self::check_reachability_via_rpc(auto_offline_timeout, info);
        if reachability == Unreachable {
            return Unreachable;
        }

        let Some((their_applied, applied_changed)) = info.last_applied_index else {
            // Applied index not known yet, can't make a decision based on that info
            return reachability;
        };

        if their_applied >= our_applied {
            // Applied index is up to date with ours, all good
            return reachability;
        }

        let now = fiber::clock();
        if now.duration_since(applied_changed) > auto_offline_timeout {
            // Applied index is lagging and hasn't changed for too long, this
            // means that the instance's raft_main_loop is probably stuck and
            // can't progress for some reason. This could be caused by storage
            // corruption because of an admin's mistake or a bug in our code.
            // Anyway we want to mark such instances as 'Offline' so that it's
            // easier to notice them and also so that they don't affect the
            // cluster in a negative way.
            return Unreachable;
        }

        reachability
    }

    /// Make a decision on the given instance's reachability based on the
    /// `info` about recent RPC communication attempts.
    /// This is an internal function.
    fn check_reachability_via_rpc(
        auto_offline_timeout: Duration,
        info: &InstanceReachabilityInfo,
    ) -> ReachabilityState {
        if let Some(last_success) = info.last_success {
            if info.fail_streak == 0 {
                // Didn't fail once, so can't be unreachable.
                return Reachable;
            }
            let now = fiber::clock();
            if now.duration_since(last_success) > auto_offline_timeout {
                return Unreachable;
            } else {
                return Reachable;
            }
        }

        if let Some(first_fail) = info.fail_streak_start {
            let now = fiber::clock();
            if now.duration_since(first_fail) > auto_offline_timeout {
                return Unreachable;
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
        self.update_auto_offline_timeout();

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
                    let learner_heartbeat_period = self.auto_offline_timeout.get() / 3;

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

    fn update_auto_offline_timeout(&self) {
        let new_value = self
            .storage
            .db_config
            .governor_auto_offline_timeout()
            .expect("storage aint gonna fail");
        self.auto_offline_timeout.set(new_value);
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReachabilityState {
    Undecided,
    Reachable,
    Unreachable,
}
use ReachabilityState::*;

/// Information about recent attempts to communicate with a single given instance.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InstanceReachabilityInfo {
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
    pub is_reported: bool,
}
