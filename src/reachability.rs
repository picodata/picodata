use crate::storage::Catalog;
use crate::traft::RaftId;
use crate::util::NoYieldsRefCell;
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
    storage: Option<Catalog>,
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
        let info = self.infos.entry(raft_id).or_default();
        info.last_attempt = Some(now);

        // Even if it was previously reported as unreachable another message was
        // sent to it, so raft node state may have changed and another
        // report_unreachable me be needed.
        info.is_reported = false;

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
    /// automatically assigned a different state.
    pub fn get_unreachables(&self) -> HashSet<RaftId> {
        let mut res = HashSet::with_capacity(self.infos.len() / 3);
        for (raft_id, info) in &self.infos {
            let status = self.determine_reachability(info);
            if status == Unreachable {
                res.insert(*raft_id);
            }
        }
        return res;
    }

    /// Make a decision on the given instance's reachability based on the
    /// provided `info`. This is an internal function.
    fn determine_reachability(&self, info: &InstanceReachabilityInfo) -> ReachabilityState {
        if let Some(last_success) = info.last_success {
            if info.fail_streak == 0 {
                // Didn't fail once, so can't be unreachable.
                return Reachable;
            }
            let now = fiber::clock();
            if now.duration_since(last_success) > self.auto_offline_timeout() {
                return Unreachable;
            } else {
                return Reachable;
            }
        }

        if let Some(first_fail) = info.fail_streak_start {
            let now = fiber::clock();
            if now.duration_since(first_fail) > self.auto_offline_timeout() {
                return Unreachable;
            }
        }

        // Don't make decisions about instances which didn't previously
        // respond once so as to not interrupt the process of booting up.
        return Undecided;
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
        let delay = since_last_success.min(Self::MAX_HEARTBEAT_PERIOD);
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
        let storage = self
            .storage
            .as_ref()
            .unwrap_or_else(|| Catalog::try_get(false).expect("should be initialized by now"));
        storage
            .db_config
            .governor_auto_offline_timeout()
            .expect("storage aint gonna fail")
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
    pub is_reported: bool,
}
