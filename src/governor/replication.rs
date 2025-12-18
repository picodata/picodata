use crate::cas;
use crate::column_name;
use crate::governor::plan::get_replicaset_config_version_bump_op;
use crate::governor::plan::stage::Plan;
use crate::governor::plan::stage::*;
use crate::instance::Instance;
use crate::replicaset::Replicaset;
use crate::replicaset::ReplicasetName;
use crate::rpc;
use crate::schema::ADMIN_ID;
use crate::storage;
use crate::storage::SystemTable;
use crate::sync::GetVclockRpc;
use crate::tier::Tier;
use crate::traft::op::Dml;
use crate::traft::RaftTerm;
use crate::traft::Result;
use std::collections::HashMap;
use tarantool::space::UpdateOps;

pub fn handle_replicaset_master_switchover<'i>(
    instances: &'i [Instance],
    replicasets: &HashMap<&ReplicasetName, &'i Replicaset>,
    tiers: &HashMap<&str, &'i Tier>,
    term: RaftTerm,
    sync_timeout: std::time::Duration,
) -> Result<Option<Plan<'i>>> {
    let new_current_master = replicasets
        .values()
        .find(|r| r.current_master_name != r.target_master_name);
    if let Some(r) = new_current_master {
        let replicaset_name = &r.name;
        let old_master_name = &r.current_master_name;
        let new_master_name = &r.target_master_name;
        let promotion_vclock = &r.promotion_vclock;

        let mut replicaset_dml = UpdateOps::new();
        replicaset_dml.assign(
            column_name!(Replicaset, current_master_name),
            new_master_name,
        )?;

        let mut bump_dml = vec![];

        let replicaset_config_version_bump = get_replicaset_config_version_bump_op(r);
        bump_dml.push(replicaset_config_version_bump);

        let tier_name = &r.tier;
        let tier = tiers
            .get(tier_name.as_str())
            .expect("tier for instance should exists");

        let vshard_config_version_bump = Tier::get_vshard_config_version_bump_op(tier)?;
        bump_dml.push(vshard_config_version_bump);

        let ranges = vec![
            // We make a decision based on this instance's state so the operation
            // should fail in case there's a change to it in the uncommitted log
            cas::Range::new(storage::Instances::TABLE_ID).eq([old_master_name]),
        ];

        let old_master_may_respond = instances
            .iter()
            .find(|i| i.name == old_master_name)
            .map(|i| i.may_respond());
        if let Some(true) = old_master_may_respond {
            let demote_rpc = rpc::replication::DemoteRequest { term };
            let sync_rpc = rpc::replication::ReplicationSyncRequest {
                term,
                vclock: promotion_vclock.clone(),
                timeout: sync_timeout,
            };

            let master_actualize_dml = Dml::update(
                storage::Replicasets::TABLE_ID,
                &[replicaset_name],
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
        } else {
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
    }

    Ok(None)
}
