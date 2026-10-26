use crate::resharding_loop::action::ReshardingAction;
use crate::resharding_loop::action::*;
use crate::resharding_loop::debug;
use crate::resharding_loop::ReshardingLoopState;
use crate::resharding_loop::ReshardingStatus;
use crate::simulation::platform::Platform;
use crate::tlog;
use crate::traft::RaftIndex;
use crate::util::NoYieldsRefCell;
use crate::Result;
use ::tarantool::fiber::r#async::watch;
use std::rc::Rc;
use tarantool::fiber;

pub async fn do_action(
    platform: &impl Platform,
    state: &Rc<NoYieldsRefCell<ReshardingLoopState>>,
    requested_status: &mut watch::Receiver<(ReshardingStatus, u64)>,
    actual_status: &mut watch::Sender<(ReshardingStatus, u64)>,
    applied: RaftIndex,
    action: ReshardingAction,
) -> Result<()> {
    let (_, next_version) = requested_status.get();

    // XXX Maybe let's have a separate parameter for resharding timeouts?
    // But than again maybe let's not...
    let cas_timeout = platform
        .alter_system_parameters()
        .borrow()
        .governor_raft_op_timeout();

    let action_kind = action.kind();
    let csw = fiber::csw();

    state.with_mut(|state| {
        // Guard against inifinite loops
        if let Some((last_action, last_csw)) = state.last_action {
            debug_assert_ne!((last_action, last_csw), (action_kind, csw));
        }
        state.last_action = Some((action_kind, csw));
    });

    type Action = ReshardingAction;
    match action {
        Action::WaitUntilMaster(WaitUntilMaster { timeout }) => {
            actual_status.send_modify(|(status, _)| *status = ReshardingStatus::ReshardingEnqueued);
            tlog!(
                Debug,
                "resharding_loop_status = '{action_kind}' ({timeout:?})"
            );

            _ = platform.wait_until_master(timeout);
        }

        Action::ActualizeShardedState(ActualizeShardedState {
            from_state,
            to_state,
            range,
            changes,
        }) => {
            actual_status.send_modify(|(status, _)| *status = ReshardingStatus::Resharding);
            let (start, end) = range.into_inner();
            tlog!(Debug, "resharding_loop_status = '{action_kind}' (_bucket update {start}..{end} {from_state:?} -> '{to_state}')");

            debug::log_bucket_changes(&changes, &platform.catalog_cache().get())?; // TODO(resharding): remove from final implementation

            platform.write_local_buckets(changes)?;

            debug::inspect_space_bucket(platform)?; // TODO(resharding): remove from final implementation
        }

        Action::ActualizeBucketStateVersion(ActualizeBucketStateVersion { version_bump }) => {
            actual_status.send_modify(|(status, _)| *status = ReshardingStatus::CleaningUp);
            tlog!(
                Debug,
                "resharding_loop_status = '{action_kind}' (_pico_replicaset set current_bucket_state_version = target_bucket_state_version)"
            );

            // Wait for vshard discovery to complete before bumping version
            // This ensures route_map is up-to-date on this instance
            let tier_name = platform.catalog_cache().my_tier_name();
            platform.wait_router_discovery_complete(tier_name, cas_timeout)?;

            platform.do_cas(applied, version_bump.into_iter().collect(), cas_timeout)?;
        }

        Action::GoIdle(GoIdle { did_something }) => {
            tlog!(Debug, "resharding_loop_status = '{action_kind}'");

            if did_something {
                debug::inspect_space_bucket(platform)?; // TODO(resharding): remove from final implementation
                debug::assert_all_buckets_rw(platform)?; // TODO(resharding): remove from final implementation
            }

            _ = actual_status.send((ReshardingStatus::Idle, next_version));

            // Sleep until a resharding action is requested
            platform.wait_action_requested(requested_status).await?;
        }
    }

    tlog!(Debug, "done action");

    Ok(())
}
