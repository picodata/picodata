use crate::simulation::action::PretendAction;
use crate::simulation::catalog::apply_dml;
use crate::simulation::cluster::PretendCluster;
use crate::simulation::fiber::is_wait_satisfiable;
use crate::simulation::fiber::wake_fiber;
use crate::simulation::fiber::FiberState;
use crate::simulation::fiber::WaitOutcome;
use crate::simulation::instance::PretendInstance;
use crate::simulation::raft::resolve_cas_request;
use crate::simulation::raft::PretendCasOutcome;
use crate::simulation::sharded_wal::apply_next_sharded_entry;
use crate::simulation::sharded_wal::commit_sharded_entry;
use crate::simulation::sharded_wal::lose_unstable_wal;
use crate::simulation::sharded_wal::undo_sharded_statement;
use crate::simulation::sharded_wal::WalOutcome;
use crate::simulation::sharding::is_governor_poke_enabled;
use crate::simulation::sharding::spawn_resharding_loop_fiber;
use crate::tlog;
use crate::vshard::VshardBucketState;
use rand::RngExt;

////////////////////////////////////////////////////////////////////////////////
// do_action
////////////////////////////////////////////////////////////////////////////////

/// This is the main simulation engine function. Applies the `action` to the
/// `cluster` and records the action along with it's outcome (if any) to
/// [`PretendCluster::trace`].
///
/// This is the only place where actions must be applied and saved to trace.
///
/// The `action` should have been returned by [`determine_potential_actions`].
///
/// Some tests call this directly with ad-hoc actions, in this case they should
/// make sure not to violate the simulation invariants.
///
/// An action can have a pre-filled outcome slot
/// ([`PretendAction::ResolveCasRequest`], [`PretendAction::CommitWalWrite`],
/// etc.), which allows for fault injection.
///
/// This function should also check as many invariants as it can.
///
/// If the action wakes up a fiber, it will not be run automatically, a separate
/// call to [`PretendCluster::run_to_next_park`] is needed.
pub fn do_action(cluster: &PretendCluster, mut action: PretendAction) {
    // Invariant: all fibers must be parked while the engine performs an action.
    if let Some((_, fiber)) = cluster.running_fiber() {
        panic!(
            "simulation: {action:?} was applied while fiber {} was still running",
            fiber.id,
        );
    }

    // Perform the action
    match &mut action {
        PretendAction::AdvanceGlobal {
            instance,
            index,
            dmls,
        } => {
            let instance = cluster.instance(instance);

            assert_eq!(
                instance.applied_index.get() + 1,
                *index,
                "[{}] stale AdvanceGlobal",
                instance.name
            );
            assert!(dmls.is_none(), "must be set here");

            let entry = cluster
                .raft_log
                .entry(*index)
                .expect("checked non-empty by `enabled_actions`");
            for dml in &*entry.dmls {
                apply_dml(&instance, dml);
            }
            instance.applied_index.set(entry.index);
            *dmls = Some(entry.dmls);
        }

        PretendAction::AdvanceSharded {
            instance,
            lsn,
            entry,
        } => {
            let instance = cluster.instance(instance);
            assert_eq!(
                instance.sharded_lsn.get() + 1,
                *lsn,
                "[{}] stale AdvanceSharded",
                instance.name
            );
            assert!(entry.is_none(), "must be set here");
            let summary = apply_next_sharded_entry(&instance);
            *entry = Some(summary);
        }

        PretendAction::SatisfyWait {
            fiber: fiber_id,
            state,
        } => {
            let instance = cluster.instance(&fiber_id.instance);
            let fiber = cluster.find_fiber(&instance.name, fiber_id.id);
            assert_eq!(&fiber.id, fiber_id);
            assert_eq!(&fiber.state().snapshot(), state, "stale SatisfyWait");
            wake_fiber(cluster, &instance, &fiber, WaitOutcome::Ok);
        }

        PretendAction::TimeoutWait {
            fiber: fiber_id,
            state,
        } => {
            let instance = cluster.instance(&fiber_id.instance);
            let fiber = cluster.find_fiber(&instance.name, fiber_id.id);
            assert_eq!(&fiber.id, fiber_id);
            assert!(
                fiber.state().is_wait_timeoutable(),
                "simulation: {state:?} is not a Timeout-candidate"
            );
            cluster
                .timed_out_waits
                .borrow_mut()
                .insert(fiber.park_fingerprint());
            wake_fiber(cluster, &instance, &fiber, WaitOutcome::Timeout);
        }

        PretendAction::ResolveCasRequest { instance, outcome } => {
            // Enumerated with no outcome, which means "draw for it". A
            // test forcing the drop pre-fills the outcome it injects, and
            // nothing else may pre-fill this field.
            let should_drop = match outcome {
                None => cluster.should_drop_cas_request(),
                Some(PretendCasOutcome::Dropped) => true,
                Some(decided) => panic!(
                    "simulation: {decided:?} is the leader's verdict, not the test's to force"
                ),
            };

            *outcome = Some(resolve_cas_request(cluster, instance, should_drop));
        }

        PretendAction::CommitWalWrite { instance, outcome } => {
            assert!(
                outcome.is_none(),
                "simulation: a WAL write carries its outcome only once committed"
            );
            let instance = cluster.instance(instance);
            // The head, always: WAL entries persist in submission order,
            // so this is not a choice the scheduler gets to make.
            let write = instance
                .unstable_wal
                .borrow_mut()
                .pop_front()
                .expect("checked non-empty by `enabled_actions`");

            let decided = cluster.roll_wal_outcome();
            match decided {
                WalOutcome::Persisted => commit_sharded_entry(&instance, write.entry),
                WalOutcome::Failed => undo_sharded_statement(&instance, write.undo),
            }

            write.outcome.set(Some(decided));
            *outcome = Some(decided);
        }

        PretendAction::RequestReshardingStatus { instance, status } => {
            let instance = cluster.instance(instance);
            instance.set_requested_resharding_status(*status);

            // A poke is `fiber::wakeup`, which only *resolves* the status watch.
            let loop_fiber = instance.resharding_loop_fiber();
            if matches!(loop_fiber.state(), FiberState::WaitIdle) {
                wake_fiber(cluster, &instance, &loop_fiber, WaitOutcome::Ok);
            }
        }

        PretendAction::SetShardedReplicationPaused { instance, paused } => {
            cluster
                .instance(instance)
                .sharded_replication_paused
                .set(*paused);
        }

        PretendAction::CrashInstance { instance } => {
            let instance = cluster.instance(instance);
            instance.cancel_all_fibers();
            lose_unstable_wal(&instance);
        }

        PretendAction::RestartInstance { instance } => {
            let instance = cluster.instance(instance);
            assert!(
                instance.is_down(),
                "[{}] restarted without having crashed",
                instance.name
            );
            spawn_resharding_loop_fiber(&instance);
        }

        PretendAction::GovernorPoke { instance } => {
            let instance = cluster.instance(instance);
            instance.set_requested_resharding_status(instance.last_requested_resharding_status());
            wake_fiber(
                cluster,
                &instance,
                &instance.resharding_loop_fiber(),
                WaitOutcome::Ok,
            );
        }
    }

    tlog!(Info, "PretendCluster::do_action({action:?})");
    // Save the action to the trace
    cluster.trace.borrow_mut().push(action);
}

////////////////////////////////////////////////////////////////////////////////
// determine_potential_actions
////////////////////////////////////////////////////////////////////////////////

/// Enumerates every `PretendAction` available in the `cluster` as this moment.
///
/// The actions are returned in a deterministic order.
pub fn determine_potential_actions(cluster: &PretendCluster) -> Vec<PretendAction> {
    let mut actions = Vec::new();

    for (name, instance) in cluster.pretend_instances.borrow().iter() {
        // A CAS proposal outlives its proposer: it sits at the leader,
        // which decides it whether or not the instance is still there.
        if !instance.cas_outbox.borrow().is_empty() {
            actions.push(PretendAction::ResolveCasRequest {
                instance: name.clone(),
                outcome: None,
            });
        }

        if instance.is_down() {
            // Nothing else is a dead process's to do.
            continue;
        }

        let applied = instance.applied_index.get();
        if applied < cluster.raft_log.last_index() {
            actions.push(PretendAction::AdvanceGlobal {
                instance: name.clone(),
                index: applied + 1,
                dmls: None,
            });
        }

        let lsn = instance.sharded_lsn.get();
        if lsn < instance.sharded_wal.len() && !instance.sharded_replication_paused.get() {
            actions.push(PretendAction::AdvanceSharded {
                instance: name.clone(),
                lsn: lsn + 1,
                entry: None,
            });
        }

        if !instance.unstable_wal.borrow().is_empty() {
            actions.push(PretendAction::CommitWalWrite {
                instance: name.clone(),
                outcome: None,
            });
        }

        if is_governor_poke_enabled(instance) {
            actions.push(PretendAction::GovernorPoke {
                instance: name.clone(),
            });
        }
    }

    for (instance, fiber) in cluster.all_fibers() {
        let state = fiber.state();
        if is_wait_satisfiable(&instance, &state) {
            actions.push(PretendAction::SatisfyWait {
                fiber: fiber.id.clone(),
                state: state.snapshot(),
            });
        }

        if state.is_wait_timeoutable()
            && !(cluster.timeout_each_wait_once.get() && cluster.was_wait_timed_out(&fiber))
        {
            actions.push(PretendAction::TimeoutWait {
                fiber: fiber.id.clone(),
                state: state.snapshot(),
            });
        }
    }

    actions
}

////////////////////////////////////////////////////////////////////////////////
// step_once
////////////////////////////////////////////////////////////////////////////////

/// One scheduler step: enumerates every enabled action and picks one with
/// the seeded RNG.
///
/// Timeout candidates are handled a bit differently.
///
/// If [`PretendCluster::timeout_each_wait_once`] is enabled we only enumerate
/// waits which haven't already been picked before, and if there are any such
/// timeouts we pick one of them as an action to perform.
///
/// Otherwise all timeouts are enumerated, then we choose if we're going to
/// perform the TimeoutWait action with probability [`PretendCluster::timeout_probability`]
/// and if rolled succesfully we pick an action from all enumerated timeouts.
/// That way the probability of a timeout action is not a function of the number
/// of enabled actions at any point in time.
///
/// Returns `false` once nothing is enabled.
///
/// Panics with a registry dump if the *only* thing enabled is a timeout if
/// timeout probability is 0.
pub fn step_once(cluster: &PretendCluster) -> bool {
    let actions = determine_potential_actions(cluster);
    if actions.is_empty() {
        // No more actions, simulation converged
        return false;
    }

    // Timeouts are drawn from a pool of their own: a step
    // first decides *whether* it times something out, and only then which wait,
    // that way the injected timeout rate doesn't depend on how many fibers
    // happen to be parked right now.
    let mut regular = vec![];
    let mut timeouts = vec![];
    let mut have_non_idle_timeouts = false;
    for action in actions {
        match &action {
            PretendAction::TimeoutWait { state, .. } => {
                if !state.is_idle_wait() {
                    have_non_idle_timeouts = true;
                }
                timeouts.push(action);
            }
            _ => regular.push(action),
        }
    }

    let have_regular = !regular.is_empty();
    let have_timeouts = !timeouts.is_empty();

    if !have_regular && !have_non_idle_timeouts {
        // Only idle waits remain, simulation converged
        return false;
    }

    let take_timeout =
        have_timeouts && (cluster.timeout_each_wait_once.get() || cluster.roll_wait_timeout());

    let mut candidates;
    if take_timeout || !have_regular {
        assert!(have_timeouts);
        assert!(
            have_regular
                || cluster.timeout_probability.get() > 0.0
                || cluster.timeout_each_wait_once.get(),
            "simulation: deadlock - every parked fiber can only be resolved by \
             a Timeout, but timeout_probability is 0\n{}",
            cluster.dump_state(),
        );

        // Note: if timeout_each_wait_once is enabled, the `timeouts` only contains
        // unique timeouts which haven't been picked before.
        candidates = timeouts;
    } else {
        candidates = regular;
    }

    // Pick a random action
    let pick = cluster.rng.borrow_mut().random_range(0..candidates.len());
    let action = candidates.remove(pick);

    // Perform the action
    do_action(cluster, action);

    // The action could have unparked a fiber, now we need to wait until it parks
    cluster.run_to_next_park();
    cluster.prune_finished_fibers();
    assert_step_was_not_a_livelock(cluster);

    true
}

////////////////////////////////////////////////////////////////////////////////
// run_until_converged
////////////////////////////////////////////////////////////////////////////////

/// An upper bound on the number of iterations for [`run_until_converged`].
/// This is needed to detect live locks and other infinite loops.
pub const MAX_SAFE_ITERATIONS: usize = 50_000;

/// Simulation considered to be live-locked after this many iterations with no
/// change in any state.
const MAX_STEPS_WITHOUT_PROGRESS: usize = 20;

/// How many trailing trace entries a livelock report prints.
const LIVELOCK_TRACE_TAIL: usize = 200;

/// Run the `cluster` simulation until cluster is resting.
pub fn run_until_converged(cluster: &PretendCluster) {
    let mut steps = 0;
    loop {
        let running = step_once(cluster);
        if !running {
            break;
        }

        steps += 1;
        if steps >= MAX_SAFE_ITERATIONS {
            panic!(
                "simulation did not converge after {MAX_SAFE_ITERATIONS} iterations\n{}",
                cluster.dump_state(),
            );
        }
    }

    for instance in cluster.instances() {
        assert_converged(&instance);
    }
}

pub fn run_until(cluster: &PretendCluster, exit_condition: impl Fn() -> bool, panic_message: &str) {
    let mut steps = 0;
    loop {
        if exit_condition() {
            break;
        }

        let running = step_once(cluster);
        assert!(running, "{panic_message}\n{}", cluster.dump_state(),);

        steps += 1;
        if steps >= MAX_SAFE_ITERATIONS {
            panic!(
                "simulation did not converge after {MAX_SAFE_ITERATIONS} iterations\n{}",
                cluster.dump_state(),
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// assert_converged
////////////////////////////////////////////////////////////////////////////////

pub fn assert_converged(instance: &PretendInstance) {
    instance.prune_finished_fibers();

    let instance_name = &instance.name;
    let cluster = instance
        .cluster()
        .expect("assert_converged called during teardown");
    let topology_ref = instance.topology.get();

    let panic_message = |message: &str| -> String {
        format!("[{instance_name}] {message}\n{}", cluster.dump_state())
    };

    assert!(
        !instance.is_down(),
        "{}",
        panic_message("converged while still down")
    );

    let live_fibers: Vec<_> = instance
        .fibers
        .borrow()
        .values()
        .map(|fiber| fiber.id.clone())
        .collect();
    let resharding_loop_fiber = instance.resharding_loop_fiber().id.clone();
    assert_eq!(
        live_fibers,
        [resharding_loop_fiber],
        "{}",
        panic_message(&format!(
            "converged with fibers other than its runner ({live_fibers:?})"
        )),
    );

    assert_eq!(
        instance.sharded_lsn.get(),
        instance.sharded_wal.len(),
        "{}",
        panic_message("converged with an unapplied sharded-replication WAL tail"),
    );

    assert!(
        instance.cas_outbox.borrow().is_empty(),
        "{}",
        panic_message("converged with an undecided CAS proposal"),
    );

    assert!(
        instance.unstable_wal.borrow().is_empty(),
        "{}",
        panic_message("converged with an unpersisted WAL write"),
    );

    assert_eq!(
        instance.applied_index.get(),
        cluster.raft_log.last_index(),
        "{}",
        panic_message("converged with an unapplied raft log tail"),
    );

    // The resting wait differs by role: a master rests in `Idle`; a
    // non-master that was ever driven rests in the not-a-master
    // branch's `wait_index_change`, waiting for an entry which
    // doesn't exist yet and so unsatisfiable at convergence. A
    // never-driven non-master never left its initial `Idle` park,
    // which is equally at rest.
    let fiber_state = instance.resharding_loop_fiber().state();
    assert!(
        fiber_state.is_idle_wait(),
        "{}",
        panic_message(&format!(
            "resharding_loop is not in idle state: {fiber_state:?}"
        )),
    );

    let this_replicaset = topology_ref.this_replicaset();
    assert_eq!(
        this_replicaset.current_bucket_state_version,
        this_replicaset.target_bucket_state_version,
        "{}",
        panic_message(&format!(
            "replicaset '{}' bucket_state_version not actualized",
            this_replicaset.name
        )),
    );

    let mut expected_bucket_count = 0u64;
    for range in topology_ref
        .buckets_info(&this_replicaset.tier)
        .expect("tier must exist")
        .ranges
        .iter()
    {
        if this_replicaset.name == range.current_replicaset_name && range.state.is_active() {
            expected_bucket_count += range.count();
        }
    }

    let buckets = instance.local_buckets.borrow();
    assert_eq!(
        buckets.len() as u64,
        expected_bucket_count,
        "{}",
        panic_message(&format!(
            "local bucket count doesn't match owned _pico_bucket ranges: {buckets:?}"
        )),
    );

    assert!(
        buckets
            .values()
            .all(|b| b.state == VshardBucketState::Active),
        "{}",
        panic_message(&format!("not all local buckets are Active: {buckets:?}")),
    );

    let bucket_refs: Vec<_> = instance
        .bucket_refs
        .borrow()
        .iter()
        .filter(|(_, bucket_ref)| bucket_ref.ro != 0 || bucket_ref.rw != 0)
        .map(|(&bucket_id, &bucket_ref)| (bucket_id, bucket_ref))
        .collect();
    assert_eq!(
        bucket_refs,
        [],
        "{}",
        panic_message(&format!("unresolved bucket refs remain: {bucket_refs:?}")),
    );

    if !instance.is_master() {
        let replicaset = topology_ref.this_replicaset();
        let Some(master_name) = replicaset.effective_master_name() else {
            panic!(
                "{}",
                panic_message(&format!(
                    "replicaset {} is in master switchover",
                    replicaset.name
                ))
            );
        };

        let master = cluster.instance(master_name);

        assert_eq!(
            *instance.local_buckets.borrow(),
            *master.local_buckets.borrow(),
            "{}",
            panic_message(&format!(
                "replica's local _bucket diverged from its master's ({master_name})"
            ))
        );
    }
}

////////////////////////////////////////////////////////////////////////////////
// assert_step_was_not_a_livelock
////////////////////////////////////////////////////////////////////////////////

fn assert_step_was_not_a_livelock(cluster: &PretendCluster) {
    if cluster.expect_no_progress.get() {
        return;
    }

    let fingerprint = cluster.fingerprint();
    let last_fingerprint = cluster.last_fingerprint.take();
    if last_fingerprint.as_ref() != Some(&fingerprint) {
        cluster.last_fingerprint.set(Some(fingerprint));
        cluster.steps_without_progress.set(0);
        return;
    } else {
        cluster.last_fingerprint.set(last_fingerprint);
    }

    let steps = cluster.steps_without_progress.get() + 1;
    cluster.steps_without_progress.set(steps);
    assert!(
        steps < MAX_STEPS_WITHOUT_PROGRESS,
        "simulation: livelock — {steps} steps in a row changed nothing about the world\n{}",
        cluster.dump_state_with_trace_tail(LIVELOCK_TRACE_TAIL),
    );
}
