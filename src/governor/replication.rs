use crate::cas;
use crate::column_name;
use crate::governor::plan::get_replicaset_config_version_bump_op;
use crate::governor::plan::stage::Plan;
use crate::governor::plan::stage::*;
use crate::has_states;
use crate::replicaset::Replicaset;
use crate::rpc;
use crate::schema::ADMIN_ID;
use crate::storage;
use crate::storage::SystemTable;
use crate::sync::GetVclockRpc;
use crate::tier::Tier;
use crate::topology_cache::TopologyCacheRef;
use crate::traft::op::Dml;
use crate::traft::RaftTerm;
use crate::traft::Result;
use crate::warn_or_panic;
use tarantool::space::UpdateOps;

pub fn handle_replicaset_master_switchover<'i>(
    topology_ref: &TopologyCacheRef,
    term: RaftTerm,
    sync_timeout: std::time::Duration,
) -> Result<Option<Plan<'i>>> {
    for replicaset in topology_ref.all_replicasets() {
        if replicaset.current_master_name == replicaset.target_master_name {
            continue;
        }

        let new_master_name = replicaset.target_master_name.clone();
        let Ok(new_master) = topology_ref.instance_by_name(&new_master_name) else {
            warn_or_panic!("No info for instance {new_master_name}");
            continue;
        };

        let old_master_name = replicaset.current_master_name.clone();
        let Ok(old_master) = topology_ref.instance_by_name(&old_master_name) else {
            warn_or_panic!("No info for instance {old_master_name}");
            continue;
        };

        let new_master_may_respond = new_master.may_respond();
        let old_master_going_expelled = has_states!(old_master, * -> Expelled);
        if !new_master_may_respond && !old_master_going_expelled {
            // Target master is not going to respond, so there's no point in
            // trying. Note that if it were possible to choose a better
            // target_master_name this would've happened on another governor step.
            //
            // XXX The exception is the case when old master is getting
            // Expelled. If there is still an instance of the replicaset we must
            // make sure it synchronizes with the old master. And if the new
            // master is Offline we just wait until it wakes up.
            //
            // This is needed to avoid a case when new master temporarily goes
            // Offline while synchronizing with the old master.
            continue;
        }

        let Ok(tier) = topology_ref.tier_by_name(&replicaset.tier) else {
            warn_or_panic!("No info for tier {}", replicaset.tier);
            continue;
        };

        let replicaset_name = replicaset.name.clone();
        let promotion_vclock = replicaset.promotion_vclock.clone();

        let mut replicaset_dml = UpdateOps::new();
        replicaset_dml.assign(
            column_name!(Replicaset, current_master_name),
            &new_master_name,
        )?;

        let mut bump_dml = vec![];

        let replicaset_config_version_bump = get_replicaset_config_version_bump_op(replicaset);
        bump_dml.push(replicaset_config_version_bump);

        let vshard_config_version_bump = Tier::get_vshard_config_version_bump_op(tier)?;
        bump_dml.push(vshard_config_version_bump);

        let ranges = vec![
            // We make a decision based on these instances' state so the operation
            // should fail in case there's a change to it in the uncommitted log
            cas::Range::new(storage::Instances::TABLE_ID).eq([&old_master_name]),
            cas::Range::new(storage::Instances::TABLE_ID).eq([&new_master_name]),
        ];

        if !old_master.may_respond() {
            let get_vclock_rpc = GetVclockRpc {};

            return Ok(Some(
                ReplicasetMasterFailover {
                    old_master_name,
                    new_master_name,
                    get_vclock_rpc,
                    replicaset_name,
                    replicaset_dml,
                    bump_dml,
                    ranges,
                }
                .into(),
            ));
        }

        let demote_rpc = rpc::replication::DemoteRequest { term };
        let sync_rpc = rpc::replication::ReplicationSyncRequest {
            term,
            vclock: promotion_vclock.clone(),
            timeout: sync_timeout,
        };

        let master_actualize_dml = Dml::update(
            storage::Replicasets::TABLE_ID,
            &[&replicaset_name],
            replicaset_dml,
            ADMIN_ID,
        )?;

        return Ok(Some(
            ReplicasetMasterConsistentSwitchover {
                replicaset_name,
                old_master_name,
                demote_rpc,
                new_master_name,
                sync_rpc,
                promotion_vclock,
                master_actualize_dml,
                bump_dml,
                ranges,
            }
            .into(),
        ));
    }

    Ok(None)
}
