use crate::debug_assert_action_kind;
use crate::resharding_loop::action::ReshardingAction;
use crate::resharding_loop::action::*;
use crate::resharding_loop::initialize::initialize_sharded_states;
use crate::resharding_loop::ReshardingLoopState;
use crate::resharding_loop::ReshardingStatus;
use crate::resharding_loop::RESHARDING_LOOP_LONG_SLEEP;
use crate::simulation::platform::Platform;
use crate::tlog;
use crate::util::NoYieldsRefCell;
use crate::Result;

////////////////////////////////////////////////////////////////////////////////
// plan_resharding_action
////////////////////////////////////////////////////////////////////////////////

pub fn plan_resharding_action(
    platform: &impl Platform,
    _state: &NoYieldsRefCell<ReshardingLoopState>,
    want_status: ReshardingStatus,
) -> Result<ReshardingAction> {
    let _guard = tarantool::fiber::NoYieldsGuard::with_message("no yields allowed here!");

    // resharding_loop is idle until governor wakes it up via RPC
    if want_status == ReshardingStatus::Idle {
        return Ok(GoIdle {
            did_something: false,
        }
        .into());
    }

    let topology_cache = platform.topology_cache();
    let my_instance_name = topology_cache.my_instance_name();
    let topology_ref = topology_cache.get();
    let this_replicaset = topology_ref.this_replicaset();
    let i_am_replicaset_master = topology_cache.with(|topology_ref| {
        topology_ref
            .this_replicaset()
            .effective_master_name()
            .map(|n| &**n)
            == Some(my_instance_name)
    });

    // resharding_loop is only running on replicaset master
    if !i_am_replicaset_master {
        return Ok(WaitUntilMaster {
            timeout: RESHARDING_LOOP_LONG_SLEEP,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    //
    // check if bucket state version is already up to date
    //
    if this_replicaset.target_bucket_state_version == this_replicaset.current_bucket_state_version {
        // Version is already actualized which means no action is needed
        tlog!(Debug, "replicaset distribution version is actualized");

        return Ok(GoIdle {
            did_something: true,
        }
        .into());
    }

    //
    // handle initial bucket distribution
    //
    if want_status == ReshardingStatus::Initialize {
        let action = initialize_sharded_states(platform, this_replicaset, &topology_ref)?;
        debug_assert_action_kind!(
            action,
            ReshardingAction::ActualizeBucketStateVersion { .. }
                | ReshardingAction::ActualizeShardedState { .. }
        );
        return Ok(action);
    }

    tlog!(Warning, "resharding not yet implemented");
    return Ok(GoIdle {
        did_something: true,
    }
    .into());
}
