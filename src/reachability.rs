use crate::storage;
use crate::storage::Clusterwide;
use crate::storage::PropertyName;
use crate::traft::RaftId;
use std::cell::RefCell;
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
#[derive(Debug, Default)]
pub struct InstanceReachabilityManager {
    // TODO: Will be used to read configuration from
    #[allow(unused)]
    storage: Option<Clusterwide>,
    infos: HashMap<RaftId, InstanceReachabilityInfo>,
}

pub type InstanceReachabilityManagerRef = Rc<RefCell<InstanceReachabilityManager>>;

impl InstanceReachabilityManager {
    pub fn new(storage: Clusterwide) -> Self {
        Self {
            storage: Some(storage),
            infos: Default::default(),
        }
    }

    /// Is called from a connection pool worker loop to report results of raft
    /// messages sent to other instances. For example a timeout is considered
    /// a failure. Updates info for the given instance.
    pub fn report_result(&mut self, raft_id: RaftId, success: bool) {
        let now = fiber::clock();
        let info = self.infos.entry(raft_id).or_insert_with(Default::default);
        info.last_attempt = Some(now);

        // Even if it was previously reported as unreachable another message was
        // sent to it, so raft node state may have changed and another
        // report_unreachable me be needed.
        info.is_reported = false;

        if success {
            info.last_success = Some(now);
            info.fail_streak = 0;
            // If was previously reported as unreachable, it's now reachable so
            // next time it should again be reported as unreachable.
        } else {
            info.fail_streak += 1;
        }
    }

    /// Is called at the beginning of the raft main loop to get information
    /// about which instances should be reported as unreachable to the raft node.
    pub fn take_unreachables_to_report(&mut self) -> Vec<RaftId> {
        // This is how you turn off the borrow checker by the way.
        let this = self as *const Self;

        let mut res = Vec::with_capacity(16);
        for (raft_id, info) in &mut self.infos {
            if info.is_reported {
                // Don't report nodes repeatedly.
                continue;
            }
            // SAFETY: this is safe because we're not accessing `self.infos` in
            // this function.
            if unsafe { &*this }.determine_reachability(info) == Unreachable {
                res.push(*raft_id);
                info.is_reported = true;
            }
        }
        res
    }

    /// Is called by sentinel to get information about which instances should be
    /// automatically assigned a different grade.
    pub fn get_unreachables(&self) -> HashSet<RaftId> {
        let mut res = HashSet::with_capacity(self.infos.len() / 3);
        for (raft_id, info) in &self.infos {
            if self.determine_reachability(info) == Unreachable {
                res.insert(*raft_id);
            }
        }
        return res;
    }

    /// Make a descision on the given instance's reachability based on the
    /// provided `info`. This is an internal function.
    fn determine_reachability(&self, info: &InstanceReachabilityInfo) -> ReachabilityState {
        let Some(last_success) = info.last_success else {
            // Don't make decisions about instances which didn't previously
            // respond once so as to not interrup the process of booting up.
            // TODO: report unreachable if fail_streak is big enough.
            return Undecided;
        };
        if info.fail_streak == 0 {
            // Didn't fail once, so can't be unreachable.
            return Reachable;
        }
        let now = fiber::clock();
        if now.duration_since(last_success) > self.auto_offline_timeout() {
            Unreachable
        } else {
            Reachable
        }
    }

    /// Is called from raft main loop when handling raft messages, passing a
    /// raft id of an instance which was previously determined to be unreachable.
    /// This function makes a decision about how often raft hearbeat messages
    /// should be sent to such instances.
    pub fn should_send_heartbeat_this_tick(&self, to: RaftId) -> bool {
        let Some(info) = self.infos.get(&to) else {
            // No attempts were registered yet.
            return true;
        };

        if info.fail_streak == 0 {
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

        // Expontential decay.
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
        let delay = since_last_success.min(self.max_heartbeat_period());
        if now > last_attempt + delay {
            return true;
        }

        return false;
    }

    fn auto_offline_timeout(&self) -> Duration {
        // TODO: it would be better for cache locality and overall performance
        // if we don't look this value up in the storage every time. Instead we
        // could store it in a field of this struct and only update it's value
        // once per raft loop iteration by calling a method update_configuration
        // or something like that.
        if let Some(storage) = &self.storage {
            // FIXME: silently ignoring an error if the user specified a value
            // of the wrong type.
            if let Ok(Some(t)) = storage.properties.get(PropertyName::AutoOfflineTimeout) {
                return Duration::from_secs_f64(t);
            }
        };
        Duration::from_secs_f64(storage::DEFAULT_AUTO_OFFLINE_TIMEOUT)
    }

    fn max_heartbeat_period(&self) -> Duration {
        // TODO: it would be better for cache locality and overall performance
        // if we don't look this value up in the storage every time. Instead we
        // could store it in a field of this struct and only update it's value
        // once per raft loop iteration by calling a method update_configuration
        // or something like that.
        if let Some(storage) = &self.storage {
            // FIXME: silently ignoring an error if the user specified a value
            // of the wrong type.
            if let Ok(Some(t)) = storage.properties.get(PropertyName::MaxHeartbeatPeriod) {
                return Duration::from_secs_f64(t);
            }
        };
        Duration::from_secs_f64(storage::DEFAULT_MAX_HEARTBEAT_PERIOD)
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
    pub fail_streak: u32,
    pub is_reported: bool,
}
