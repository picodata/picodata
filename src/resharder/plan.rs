use crate::debug_assert_action_kind;
use crate::resharder::action::ReshardingAction;
use crate::resharder::action::*;
use crate::resharder::initialize::initialize_sharded_states;
use crate::resharder::sharded_tables::ShardedTableInfo;
use crate::resharder::ResharderLoopState;
use crate::resharder::ReshardingStatus;
use crate::resharder::RESHARDER_LONG_SLEEP;
use crate::simulation::platform::Platform;
use crate::tlog;
use crate::util::NoYieldsRefCell;
use crate::Result;

/// A helper struct which stores data necessary to plan a resharding action
/// during a resharder_loop iteration.
pub struct ReshardingPlanContext<'a> {
    pub sharded_tables: &'a [ShardedTableInfo],
    pub resharder_chunk_size_max: u64,
    pub resharder_bucket_count_max: u64,
}

////////////////////////////////////////////////////////////////////////////////
// plan_resharding_action
////////////////////////////////////////////////////////////////////////////////

pub fn plan_resharding_action(
    platform: &impl Platform,
    _state: &NoYieldsRefCell<ResharderLoopState>,
    want_status: ReshardingStatus,
) -> Result<ReshardingAction> {
    let _guard = tarantool::fiber::NoYieldsGuard::with_message("no yields allowed here!");

    // resharder_loop is idle until governor wakes it up via RPC
    if want_status == ReshardingStatus::Idle {
        return Ok(GoIdle {
            did_something: false,
        }
        .into());
    }

    let catalog_cache = platform.catalog_cache();
    let my_instance_name = catalog_cache.my_instance_name();
    let topology_ref = catalog_cache.get();
    let this_replicaset = topology_ref.this_replicaset();
    let i_am_replicaset_master = catalog_cache.with(|topology_ref| {
        topology_ref
            .this_replicaset()
            .effective_master_name()
            .map(|n| &**n)
            == Some(my_instance_name)
    });

    // resharder_loop is only running on replicaset master
    if !i_am_replicaset_master {
        return Ok(WaitUntilMaster {
            timeout: RESHARDER_LONG_SLEEP,
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
