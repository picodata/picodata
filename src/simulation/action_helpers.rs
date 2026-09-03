use crate::column_name;
use crate::instance::InstanceName;
use crate::replicaset::Replicaset;
use crate::resharding_loop::ReshardingActionKind;
use crate::resharding_loop::ReshardingStatus;
use crate::simulation::action::PretendAction;
use crate::simulation::cluster::PretendCluster;
use crate::simulation::engine::do_action;
use crate::simulation::engine::run_until;
use crate::simulation::engine::MAX_SAFE_ITERATIONS;
use crate::simulation::fiber::is_wait_satisfiable;
use crate::simulation::fiber::FiberState;
use crate::simulation::raft::CasOutcomeSlot;
use crate::simulation::raft::PretendCasOutcome;
use crate::simulation::raft::PretendCasRequest;
use crate::storage::Replicasets;
use crate::storage::SystemTable;
use ::tarantool::space::UpdateOps;
use std::cell::Cell;
use std::rc::Rc;

pub fn crash_instance(cluster: &PretendCluster, instance_name: &str) {
    let instance = cluster.instance(instance_name);
    do_action(
        cluster,
        PretendAction::CrashInstance {
            instance: instance.name.clone(),
        },
    );
}

pub fn restart_instance(cluster: &PretendCluster, instance_name: &str) {
    let instance = cluster.instance(instance_name);
    do_action(
        cluster,
        PretendAction::RestartInstance {
            instance: instance.name.clone(),
        },
    );
    cluster.run_to_next_park();
}

/// World action: proposes the DML batch a real governor's promotion would
/// eventually produce for `replicaset`, and sees it through to committed
/// by CAS. Uses [`PretendCluster::most_applied_instance`] as a leader stand-in.
///
/// Proposed and resolved before this returns, in one attempt which must
/// commit.
// TODO: we should model topology changes as distinct actions
// see also <https://git.picodata.io/core/picodata/-/work_items/3144>
pub fn commit_master_switchover(cluster: &PretendCluster, replicaset_name: &str, new_master: &str) {
    let replicaset = {
        let most_applied = cluster.most_applied_instance();
        let topology_ref = most_applied.topology.get();
        topology_ref
            .replicaset_by_name(replicaset_name)
            .expect("must exist")
            .clone()
    };
    let new_master_name: InstanceName = new_master.into();

    // No *transaction* may span the flip either: an in-progress WAL write
    // can't be aborted, so a demoted master holding one would land it
    // after the flip, on top of whatever the new master wrote meanwhile.
    // That is a real production race, and one these tests pin as out of
    // scope.
    // FIXME we actually want to check what happens without this
    settle_pending_writes(cluster, &replicaset.current_master_name);
    for name in cluster.instance_names() {
        if cluster.instance(&name).topology.my_replicaset_name() == &*replicaset.name {
            catch_up_on_sharded_wal(cluster, &name);
        }
    }

    // The proposal has to be the head of the outbox.
    let leader = cluster.most_applied_instance();
    assert!(
        leader.cas_outbox.borrow().is_empty(),
        "[{}] stands in for the leader, so it must be free to propose the promotion, \
         but it has a proposal of its own outstanding",
        leader.name,
    );

    let counter = {
        let topology_ref = leader.topology.get();
        topology_ref
            .replicaset_by_name(&replicaset.name)
            .expect("must exist")
            .master_change_counter
    };
    let outcome: CasOutcomeSlot = Rc::new(Cell::new(None));
    leader.cas_outbox.borrow_mut().push_back(PretendCasRequest {
        index: leader.applied_index.get(),
        dmls: vec![Replicasets::dml_update(
            &[&*replicaset.name],
            UpdateOps::new()
                .into_assign(
                    column_name!(Replicaset, current_master_name),
                    new_master_name.clone(),
                )
                .expect("valid update op")
                .into_assign(
                    column_name!(Replicaset, target_master_name),
                    new_master_name.clone(),
                )
                .expect("valid update op")
                .into_assign(column_name!(Replicaset, master_change_counter), counter + 1)
                .expect("valid update op"),
        )],
        outcome: outcome.clone(),
    });

    do_action(
        cluster,
        PretendAction::ResolveCasRequest {
            instance: leader.name.clone(),
            outcome: None,
        },
    );

    // Must land in a single attempt.
    let decided = outcome.get().expect("just resolved");
    assert!(
        matches!(decided, PretendCasOutcome::Committed { .. }),
        "the promotion of {new_master_name} got {decided:?} instead of committing; \
         trace: {:#?}",
        cluster.trace.borrow(),
    );
}

/// World action: explicitly drops `instance`'s first CAS request in the queue.
pub fn drop_next_cas_request(cluster: &PretendCluster, instance_name: &str) {
    let instance = cluster.instance(instance_name);
    do_action(
        cluster,
        PretendAction::ResolveCasRequest {
            instance: instance.name.clone(),
            outcome: Some(PretendCasOutcome::Dropped),
        },
    );
}

/// World action: times out the `instance`'s first CAS request in the queue.
pub fn expire_cas_wait(cluster: &PretendCluster, instance_name: &str) {
    let instance = cluster.instance(instance_name);
    let fiber = instance.resharding_loop_fiber();
    let state = fiber.state();
    assert!(
        matches!(state, FiberState::WaitCasOutcome { .. }),
        "[{instance_name}] isn't waiting on a CAS proposal, it's {state:?}"
    );
    do_action(
        cluster,
        PretendAction::TimeoutWait {
            fiber: fiber.id.clone(),
            state: state.snapshot(),
        },
    );
    cluster.run_to_next_park();
}

/// Drives the scheduler until `instance` has a CAS proposal outstanding,
/// i.e. until there is something for [`drop_next_cas_request`] to
/// drop.
pub fn run_until_cas_request_is_outstanding(cluster: &PretendCluster, instance_name: &str) {
    let instance = cluster.instance(instance_name);
    run_until(
        cluster,
        || !instance.cas_outbox.borrow().is_empty(),
        &format!("simulation converged before instance {instance_name} proposed a CAS"),
    );
}

/// Drives the scheduler until instance's last_action is `target`.
pub fn run_until_last_action(
    cluster: &PretendCluster,
    instance_name: &str,
    target: ReshardingActionKind,
) {
    let instance = cluster.instance(instance_name);
    run_until(
        cluster,
        || instance.loop_state.borrow().last_action == Some(target),
        &format!("simulation converged before instance {instance_name} reach action {target:?}"),
    );
}

/// Applies every committed global entry to every instance's applied index
/// *without* waking anyone.
pub fn catch_up_on_raft_log(cluster: &PretendCluster) {
    for name in cluster.instance_names() {
        let instance = cluster.instance(&name);
        while instance.applied_index.get() < cluster.raft_log.last_index() {
            do_action(
                cluster,
                PretendAction::AdvanceGlobal {
                    instance: name.clone(),
                    applied: instance.applied_index.get(),
                },
            );
        }
    }
}

/// World action: the harness playing governor.
/// Mirrors `ReshardingLoop::request_action` + `fiber::wakeup`.
pub fn request_resharding_status(
    cluster: &PretendCluster,
    instance_name: &str,
    status: ReshardingStatus,
) {
    let instance = cluster.instance(instance_name);
    do_action(
        cluster,
        PretendAction::RequestReshardingStatus {
            instance: instance.name.clone(),
            status,
        },
    );
    cluster.run_to_next_park();
}

/// World action: pauses/resumes one instance's sharded-replication
/// stream. See [`PretendInstance::sharded_replication_paused`](crate::simulation::instance::PretendInstance::sharded_replication_paused).
pub fn set_sharded_replication_paused(cluster: &PretendCluster, instance_name: &str, paused: bool) {
    let instance = cluster.instance(instance_name);
    do_action(
        cluster,
        PretendAction::SetShardedReplicationPaused {
            instance: instance.name.clone(),
            paused,
        },
    );
}

/// Catches `instance` up with its replicaset's sharded-replication WAL,
/// applying every entry it hasn't yet, in order.
pub fn catch_up_on_sharded_wal(cluster: &PretendCluster, instance_name: &str) {
    let instance = cluster.instance(instance_name);
    while instance.sharded_lsn.get() < instance.sharded_wal.len() {
        do_action(
            cluster,
            PretendAction::AdvanceSharded {
                instance: instance.name.clone(),
                lsn: instance.sharded_lsn.get(),
            },
        );
    }
}

/// Drives `instance` until nothing of its is mid-transaction: commits
/// whatever its WAL still holds and lets each writer run on to its next
/// park. What a switchover needs of the master it demotes, since a
/// transaction can't be made to span the flip.
pub fn settle_pending_writes(cluster: &PretendCluster, instance_name: &str) {
    let instance = cluster.instance(instance_name);
    for _ in 0..MAX_SAFE_ITERATIONS {
        let writing = instance
            .all_fibers()
            .into_iter()
            .find(|fiber| matches!(fiber.state(), FiberState::WaitWalWrite { .. }));
        let Some(fiber) = writing else {
            return;
        };
        let state = fiber.state();
        if is_wait_satisfiable(&instance, &state) {
            do_action(
                cluster,
                PretendAction::SatisfyWait {
                    fiber: fiber.id.clone(),
                    state: state.snapshot(),
                },
            );
            cluster.run_to_next_park();
        } else {
            // Not this fiber's turn yet: the head of the WAL belongs to
            // some other fiber of the same instance.
            do_action(
                cluster,
                PretendAction::CommitWalWrite {
                    instance: instance.name.clone(),
                    outcome: None,
                },
            );
        }
    }
    panic!(
        "simulation: [{instance_name}] never stopped writing; trace: {:#?}",
        cluster.trace.borrow(),
    );
}
