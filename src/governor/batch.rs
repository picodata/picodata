use crate::governor::plan::stage::ActionKind;
use crate::governor::plan::stage::Plan;
use crate::instance::InstanceName;
use crate::tier::Tier;
use crate::tlog;
use smol_str::SmolStr;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use tarantool::fiber;
use tarantool::time::Instant;

////////////////////////////////////////////////////////////////////////////////
// LastStepInfo
////////////////////////////////////////////////////////////////////////////////

/// This struct keeps track of partial results of the most recent governor step.
/// The results are generally reset if governor decided to execute a step of
/// different kind. See how `last_step_info` is used in governor/mod.rs,
/// governor/plan.rs etc.
pub struct LastStepInfo {
    step_kind: ActionKind,
    ok_instances: HashSet<InstanceName>,
    err_instances: HashMap<InstanceName, ErrorTracker>,
    all_instances: HashSet<InstanceName>,
    target_vshard_config_versions: HashMap<SmolStr, u64>,
}

impl LastStepInfo {
    pub fn new() -> Self {
        Self {
            step_kind: ActionKind::GoIdle,
            ok_instances: HashSet::new(),
            err_instances: HashMap::new(),
            all_instances: HashSet::new(),
            target_vshard_config_versions: HashMap::new(),
        }
    }

    /// Is called every time governor chooses actions for the next iteration.
    /// Resets the results if `step` kind changes.
    pub fn on_next_step(&mut self, step: &Plan) {
        if matches!(step, Plan::SleepDueToBackoff { .. }) {
            // Sleeping due to backoff should not reset previous step Ok results
            return;
        }

        let kind = step.kind();
        if self.step_kind == kind {
            return;
        }

        self.reset_rpc_results("step kind changed");
        self.target_vshard_config_versions.clear();

        self.step_kind = kind;
    }

    pub fn reset_rpc_results(&mut self, reason: &str) {
        if !self.ok_instances.is_empty() {
            let count = self.ok_instances.len();
            tlog!(Info, "clearing {count} successfull RPC results: {reason}");
        }
        self.ok_instances.clear();
        self.err_instances.clear();
        self.all_instances.clear();
    }

    pub fn on_ok_instance(&mut self, instance_name: InstanceName) {
        self.err_instances.remove(&instance_name);
        self.ok_instances.insert(instance_name);
    }

    pub fn on_err_instance(&mut self, instance_name: &InstanceName) -> &ErrorTracker {
        self.err_instances
            .entry(instance_name.clone())
            .and_modify(ErrorTracker::on_error)
            .or_insert_with(ErrorTracker::new)
    }

    pub fn set_pending(&mut self, instances: &[InstanceName]) {
        self.all_instances.clear();
        for instance in instances {
            self.all_instances.insert(instance.clone());
        }
    }

    pub fn report_stats(&self) {
        tlog!(
            Info,
            "RPC batching stats for step {:?}: total: {}, ok: {}, err: {}",
            self.step_kind,
            self.all_instances.len(),
            self.ok_instances.len(),
            self.err_instances.len(),
        )
    }

    /// Check if target_vshard_config_version version has changed for any of the
    /// `tiers`, if so we must throw away any successful RPC from previous
    /// attempts, because everybody needs to apply the newer configuration.
    ///
    /// `step_kind` is provided as a sort of hack, because if we don't set it
    /// here, then the call to `on_next_step` in governor loop will reset the
    /// vshard config versions and the first batch of RPCs will always need to
    /// be resent. I couldn't come up with a better solution unfortunately
    pub fn update_vshard_config_versions(
        &mut self,
        tiers: &HashMap<&str, &Tier>,
        step_kind: ActionKind,
    ) {
        self.step_kind = step_kind;

        let mut something_changed = false;
        let mut reason = "<uknown reason>";
        let was_empty = self.target_vshard_config_versions.is_empty();

        for tier in tiers.values() {
            match self.target_vshard_config_versions.entry(tier.name.clone()) {
                Entry::Occupied(mut entry) => {
                    if entry.get() != &tier.target_vshard_config_version {
                        entry.insert(tier.target_vshard_config_version);
                        something_changed = true;
                        reason = "target_vshard_config_version changed";
                    } else {
                        // Config version of this tier didn't change since last time
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(tier.target_vshard_config_version);
                    something_changed = true;
                    if was_empty {
                        reason = "target_vshard_config_version was unknown";
                    } else {
                        reason = "new tier added";
                    }
                }
            }
        }

        // Cleanup info about tiers which have been removed
        self.target_vshard_config_versions.retain(|tier_name, _| {
            let retained = tiers.contains_key(&**tier_name);
            if !retained {
                something_changed = true;
                reason = "tier was removed";
            }
            retained
        });

        tlog!(
            Debug,
            "saved vshard config versions: {:?}",
            self.target_vshard_config_versions
        );

        if something_changed {
            // One of the tier's configurations changed, must notify every
            // instance about it
            self.reset_rpc_results(reason);
        }
    }

    #[inline]
    pub fn instance_ok(&self, instance_name: &InstanceName) -> bool {
        self.ok_instances.contains(instance_name)
    }

    #[inline]
    pub fn backoff_for_instance_will_end_at(
        &self,
        instance_name: &InstanceName,
    ) -> Option<Instant> {
        Some(self.err_instances.get(instance_name)?.next_try())
    }

    pub fn all_instances_ok(&self, all_instances: &[InstanceName]) -> bool {
        for instance in all_instances {
            if !self.ok_instances.contains(instance) {
                tlog!(Debug, "{instance} still not ok");
                return false;
            }
        }

        true
    }
}

////////////////////////////////////////////////////////////////////////////////
// ErrorTracker
////////////////////////////////////////////////////////////////////////////////

pub struct ErrorTracker {
    pub last_try: Instant,
    pub streak: u32,
    pub current_timeout: Duration,
}

impl ErrorTracker {
    const BACKOFF_TIMEOUT_BASE: Duration = Duration::from_secs(1);
    const BACKOFF_TIMEOUT_MAX: Duration = Duration::from_secs(60);
    const BACKOFF_TIMEOUT_MULT: f64 = 2.0;

    fn new() -> Self {
        let now = fiber::clock();
        Self {
            last_try: now,
            streak: 1,
            current_timeout: Self::BACKOFF_TIMEOUT_BASE,
        }
    }

    fn on_error(&mut self) {
        self.last_try = fiber::clock();
        self.streak += 1;

        let old_secs = self.current_timeout.as_secs_f64();
        let new_secs = old_secs * Self::BACKOFF_TIMEOUT_MULT;
        let new_timeout = Duration::from_secs_f64(new_secs);
        self.current_timeout = new_timeout.min(Self::BACKOFF_TIMEOUT_MAX);
    }

    fn next_try(&self) -> Instant {
        self.last_try + self.current_timeout
    }
}

////////////////////////////////////////////////////////////////////////////////
// ...
////////////////////////////////////////////////////////////////////////////////

pub type BatchOrSleep = Result<Vec<InstanceName>, Instant>;

/// Returns a batch of `targets_total` of at most given `batch_size` based on `last_step_info`.
///
/// Instances marked "ok" in `last_step_info` are skipped if `skip_if_already_ok` is `true`.
/// Instances marked "err" will only be added to the batch after all
/// instances which aren't marked at all (i.e. no RPC attempts registered yet).
/// Instances marked "err" will only be added after the back-off timeout has
/// elapsed.
///
/// If there are no suitable instances for RPC right now (because everybody is
/// in back-off) then `Err(next_try)` is returned, which is an instant when at
/// least one of the instances' back-off timeouts runs out.
///
/// Otherwise `Ok(batch)` is returned, which is allowed to be shorter then
/// `batch_size`. If returned `batch` is empty then no RPCs are probably needed
/// but something else probably needs to happen (like a finalizing CAS request)
/// probably...
///
/// # Panicking
/// Will panic if `targets_total` is empty.
pub fn get_next_batch(
    targets_total: &[InstanceName],
    last_step_info: &LastStepInfo,
    batch_size: usize,
    skip_if_already_ok: bool,
) -> BatchOrSleep {
    debug_assert!(!targets_total.is_empty());

    let now = fiber::clock();

    let mut only_new_ones = Vec::with_capacity(batch_size);
    let mut already_tried = vec![];
    let mut closest_next_try = None;

    for name in targets_total {
        if last_step_info.instance_ok(name) {
            if !skip_if_already_ok && already_tried.len() < batch_size {
                // If we don't need to outright skip the instances which already responded Ok,
                // then we'll make sure to add these to the end of the queue
                already_tried.push(name);
            }

            continue;
        }

        if let Some(next_try) = last_step_info.backoff_for_instance_will_end_at(name) {
            if now < next_try {
                // This instance is in backoff, not going to send RPC this time

                if *closest_next_try.get_or_insert(next_try) > next_try {
                    closest_next_try = Some(next_try);
                }

                continue;
            }

            if already_tried.len() < batch_size {
                already_tried.push(name);
            }
        } else {
            only_new_ones.push(name.clone());
            if only_new_ones.len() >= batch_size {
                break;
            }
        }
    }

    // First try sending to target to whom we didn't send a RPC yet
    let mut targets_batch = only_new_ones;
    if targets_batch.len() < batch_size {
        // If there's not enough new ones, time to send to ones we've already tried sending to
        let need_more = batch_size - targets_batch.len();
        let tail = already_tried.into_iter().take(need_more).cloned();
        targets_batch.extend(tail);
    }

    if !targets_batch.is_empty() {
        // We only need to know the next try moment if we didn't send the
        // RPC to anyone this time around
        return Ok(targets_batch);
    }

    if let Some(next_try) = closest_next_try {
        // If we skipped someone due to back-off and now we don't have any
        // targets let's just go to sleep until it's time to bother someone
        return Err(next_try);
    }

    // It could be that the precondition for the RPCs is met but there's all
    // targets of the RPC have already answered the request. This can happend
    // for example if a finalizing CAS request has failed due to CAS conflict.
    // In this case we return an empty array of targets and the caller knows
    // that no RPCs are needed, but the finalizign CAS is probably needed.
    Ok(vec![])
}
