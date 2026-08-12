use crate::simulation::action::PretendAction;
use crate::simulation::catalog::apply_dml;
use crate::simulation::cluster::PretendCluster;
use crate::simulation::fiber::is_wait_satisfiable;
use crate::simulation::fiber::wake_fiber;
use crate::simulation::fiber::FiberState;
use crate::simulation::fiber::WaitOutcome;
use crate::simulation::raft::resolve_cas_request;
use crate::simulation::raft::PretendCasOutcome;
use crate::simulation::sharded_wal::apply_next_sharded_entry;
use crate::simulation::sharded_wal::commit_sharded_entry;
use crate::simulation::sharded_wal::lose_unstable_wal;
use crate::simulation::sharded_wal::undo_sharded_statement;
use crate::simulation::sharded_wal::WalOutcome;
use crate::simulation::sharding::is_governor_poke_enabled;
use crate::simulation::sharding::spawn_resharding_loop_fiber;
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
        PretendAction::AdvanceGlobal { instance, applied } => {
            let instance = cluster.instance(instance);
            assert_eq!(
                instance.applied_index.get(),
                *applied,
                "[{}] stale AdvanceGlobal",
                instance.name
            );
            let entry = cluster
                .raft_log
                .entry(*applied + 1)
                .expect("checked non-empty by `enabled_actions`");
            for dml in &*entry.dmls {
                apply_dml(&instance, dml);
            }
            instance.applied_index.set(entry.index);
        }

        PretendAction::AdvanceSharded { instance, lsn } => {
            let instance = cluster.instance(instance);
            assert_eq!(
                instance.sharded_lsn.get(),
                *lsn,
                "[{}] stale AdvanceSharded",
                instance.name
            );
            apply_next_sharded_entry(&instance);
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
                applied,
            });
        }

        let lsn = instance.sharded_lsn.get();
        if lsn < instance.sharded_wal.len() && !instance.sharded_replication_paused.get() {
            actions.push(PretendAction::AdvanceSharded {
                instance: name.clone(),
                lsn,
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

        if state.is_wait_timeoutable() {
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
/// the seeded RNG. Timeout candidates are enumerated unconditionally but
/// taken only with probability [`PretendCluster::timeout_probability`], so they are
/// never *picked* unless a fault campaign raised it.
///
/// Returns `false` once nothing is enabled.
///
/// Panics with a registry dump if the *only* thing enabled is a timeout if
/// timeout probability is 0.
pub fn step_once(cluster: &PretendCluster) -> bool {
    cluster.prune_finished_fibers();

    let actions = determine_potential_actions(cluster);
    if actions.is_empty() {
        // No more actions, simulation converged
        return false;
    }

    if actions.iter().all(|action| {
        matches!(
            action,
            PretendAction::TimeoutWait {
                state,
                ..
            } if state.is_idle_wait()
        )
    }) {
        // Only idle waits remain, simulation converged
        return false;
    }

    // Timeouts are drawn from a pool of their own: a step first decides
    // *whether* it times something out, and only then which wait, that way
    // the injected timeout rate doesn't depend on how many fibers happen to be
    // parked right now.
    let (timeouts, regular): (Vec<_>, Vec<_>) = actions
        .into_iter()
        .partition(|action| matches!(action, PretendAction::TimeoutWait { .. }));

    let have_regular = !regular.is_empty();
    let have_timeouts = !timeouts.is_empty();

    let mut candidates;
    if have_regular && !(have_timeouts && cluster.should_timeout_a_wait()) {
        candidates = regular;
    } else {
        assert!(have_timeouts);
        assert!(
            !have_regular || cluster.timeout_probability.get() > 0.0,
            "simulation: deadlock - every parked fiber can only be resolved by \
             a Timeout, but timeout_probability is 0; registry dump:\n{}\ntrace: {:#?}",
            cluster.dump_fiber_registry(),
            cluster.trace.borrow(),
        );

        candidates = timeouts;
    }

    // Pick a random action
    let pick = cluster.rng.borrow_mut().random_range(0..candidates.len());
    let action = candidates.remove(pick);

    // Perform the action
    do_action(cluster, action);

    // The action could have unparked a fiber, now we need to wait until it parks
    cluster.run_to_next_park();

    true
}

////////////////////////////////////////////////////////////////////////////////
// run_until_converged
////////////////////////////////////////////////////////////////////////////////

/// An upper bound on the number of iterations for [`run_until_converged`].
/// This is needed to detect live locks and other infinite loops.
pub const MAX_SAFE_ITERATIONS: usize = 50_000;

/// Run the `cluster` simulation until cluster is resting.
pub fn run_until_converged(cluster: &PretendCluster) {
    for _ in 0..MAX_SAFE_ITERATIONS {
        let done_something = step_once(cluster);

        if !done_something {
            cluster.assert_resting();

            for instance in cluster.instances() {
                instance.assert_converged();
            }

            return;
        }
    }

    panic!(
        "simulation did not converge after {MAX_SAFE_ITERATIONS} iterations; \
         registry dump:\n{}\ntrace: {:#?}",
        cluster.dump_fiber_registry(),
        cluster.trace.borrow()
    );
}

pub fn run_until(cluster: &PretendCluster, exit_condition: impl Fn() -> bool, panic_message: &str) {
    let mut steps = 0;
    loop {
        if exit_condition() {
            break;
        }

        let running = step_once(cluster);
        assert!(
            running,
            "{panic_message};\ntrace: {:#?}",
            cluster.trace.borrow(),
        );

        steps += 1;
        if steps >= MAX_SAFE_ITERATIONS {
            panic!(
                "simulation did not converge after {MAX_SAFE_ITERATIONS} iterations\n\
                registry dump:{}\ntrace: {:#?}",
                cluster.dump_fiber_registry(),
                cluster.trace.borrow(),
            );
        }
    }
}
