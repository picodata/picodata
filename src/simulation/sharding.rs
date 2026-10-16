use crate::catalog::pico_bucket::BucketIdRange;
use crate::catalog::pico_bucket::BUCKET_ID_MIN;
use crate::resharding_loop::resharding_loop;
use crate::resharding_loop::ReshardingLoopState;
use crate::resharding_loop::ReshardingStatus;
use crate::simulation::fiber::is_wait_satisfiable;
use crate::simulation::fiber::FiberState;
use crate::simulation::fiber::PretendFiber;
use crate::simulation::fiber::WaitOutcome;
use crate::simulation::instance::PretendInstance;
use crate::tlog;
use ::tarantool::fiber;
use ::tarantool::fiber::r#async::watch;
use std::rc::Rc;
use tarantool::error::IntoBoxError;

////////////////////////////////////////////////////////////////////////////////
// spawn_resharding_loop_fiber
////////////////////////////////////////////////////////////////////////////////

/// Spawns the instance's [`resharding_loop`] fiber.
pub fn spawn_resharding_loop_fiber(instance: &Rc<PretendInstance>) {
    let (requested_status_tx, mut requested_status_rx) =
        watch::channel((ReshardingStatus::Idle, 0));
    let (mut actual_status_tx, actual_status_rx) = watch::channel((ReshardingStatus::Idle, 0));

    *instance.requested_status_tx.borrow_mut() = requested_status_tx;
    *instance.actual_status_rx.borrow_mut() = actual_status_rx;
    *instance.loop_state.borrow_mut() = ReshardingLoopState::default();

    let fiber = Rc::new(PretendFiber::new(instance, "pretend_resharding_loop"));
    instance.register_fiber(fiber.clone());
    let old = instance.resharding_loop_fiber_id.replace(Some(fiber.id.id));
    assert!(
        old.is_none(),
        "[{}] already has a resharding loop fiber",
        instance.name
    );

    let instance = instance.clone();
    let fiber_copy = fiber.clone();
    let join_handle = fiber::Builder::new()
        .name(fiber.id.to_string())
        .func_async(async move {
            loop {
                // The only check outside a park: `resharding_loop` can return
                // without having parked at all, so teardown would otherwise
                // only be noticed one full iteration later.
                if fiber.is_stopped() {
                    break;
                }

                let res = resharding_loop(
                    &*instance,
                    &instance.loop_state,
                    &mut requested_status_rx,
                    &mut actual_status_tx,
                )
                .await;

                match res {
                    Ok(()) => {}
                    Err(e) => {
                        tlog!(Warning, "[{}] unhandled error: {e}", instance.name);
                        instance.loop_state.borrow_mut().last_error = Some(e.into_box_error());

                        if fiber.park(FiberState::WaitIdle) == WaitOutcome::Cancelled {
                            break;
                        }
                    }
                }
            }

            // Only ever reached at teardown.
            fiber.set_finished();
        })
        .start()
        .expect("starting a fiber shouldn't fail");
    fiber_copy.set_join_handle(join_handle);

    assert!(
        !fiber_copy.is_running(),
        "simulation: fiber {} didn't park after start",
        fiber_copy.id
    );

    assert!(
        !fiber_copy.is_stopped() && !fiber_copy.is_finished(),
        "simulation: fiber {} finished immediately after start",
        fiber_copy.id
    );
}

pub fn is_governor_poke_enabled(instance: &PretendInstance) -> bool {
    if instance.last_requested_resharding_status() == ReshardingStatus::Idle {
        // The harness never drove this instance; a real governor
        // wouldn't be retrying anything either.
        return false;
    }

    let fiber = instance.resharding_loop_fiber();
    if fiber.state() != FiberState::WaitIdle {
        return false;
    }

    if is_wait_satisfiable(instance, &FiberState::WaitIdle) {
        // A pending request already exists; waking it is the
        // `SatisfyWait` action's job.
        return false;
    }

    let this_replicaset = instance.topology.get().this_replicaset().clone();
    if this_replicaset.current_bucket_state_version == this_replicaset.target_bucket_state_version {
        return false;
    }

    true
}

pub fn full_range(bucket_count: u64) -> BucketIdRange {
    BUCKET_ID_MIN..=(BUCKET_ID_MIN + bucket_count - 1)
}

pub fn first_half(bucket_count: u64) -> BucketIdRange {
    BUCKET_ID_MIN..=(BUCKET_ID_MIN + bucket_count / 2)
}

pub fn second_half(bucket_count: u64) -> BucketIdRange {
    (BUCKET_ID_MIN + bucket_count / 2 + 1)..=(BUCKET_ID_MIN + bucket_count - 1)
}
