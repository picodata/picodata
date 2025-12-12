use crate::cas;
use crate::column_name;
use crate::governor::plan::get_replicaset_config_version_bump_op;
use crate::governor::plan::stage::Plan;
use crate::governor::plan::stage::*;
use crate::has_states;
use crate::instance::Instance;
use crate::replicaset::Replicaset;
use crate::rpc;
use crate::schema::ADMIN_ID;
use crate::storage;
use crate::storage::SystemTable;
use crate::sync::GetVclockRpc;
use crate::tier::Tier;
use crate::topology_cache::TopologyCacheRef;
use crate::traft::op::Dml;
use crate::traft::op::Op;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;
use crate::traft::Result;
use crate::version::version_is_new_enough;
use crate::warn_or_panic;
use smol_str::SmolStr;
use tarantool::space::UpdateOps;

////////////////////////////////////////////////////////////////////////////////
// handle_replicaset_master_switchover
////////////////////////////////////////////////////////////////////////////////

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

        // After master was switched over we must call proc_replication again
        // so that the current_master becomes writable (right now nobody is writable)
        let replicaset_config_version_bump = get_replicaset_config_version_bump_op(replicaset);
        bump_dml.push(replicaset_config_version_bump);

        // Vshard configuration must also be updated (it keeps track of replicaset masters)
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

////////////////////////////////////////////////////////////////////////////////
// handle_replicaset_sync
////////////////////////////////////////////////////////////////////////////////

pub fn handle_replicaset_sync<'a>(
    topology_ref: &TopologyCacheRef,
    term: RaftTerm,
    applied: RaftIndex,
    global_catalog_version: &SmolStr,
    sync_timeout: std::time::Duration,
) -> Result<Option<Plan<'a>>> {
    if !version_is_new_enough(
        global_catalog_version,
        &Instance::SYNC_INCARNATION_AVAILABLE_SINCE,
    )? {
        // Replication sync involves updating the `sync_incarnation` column
        // in `_pico_instance` which is only added after upgrade, but this
        // step could theoretically run before the schema is upgraded, so
        // we need an explicit version check.
        return Ok(None);
    }

    let Some((replicaset, targets)) = get_replicaset_to_sync(topology_ref) else {
        return Ok(None);
    };

    let replicaset_name = &replicaset.name;
    let master_name = replicaset
        .effective_master_name()
        .expect("master must be actualized");
    let get_vclock_rpc = GetVclockRpc {};

    let promotion_vclock = &replicaset.promotion_vclock;
    let sync_rpc = rpc::replication::ReplicationSyncRequest {
        term,
        vclock: promotion_vclock.clone(),
        timeout: sync_timeout,
    };

    let mut bump_dml = vec![];

    // After all laggers have syncrhonized with master we must add them to
    // the full-mesh configuration (currently they're not in other replicas' upstreams)
    let bump = get_replicaset_config_version_bump_op(replicaset);
    bump_dml.push(bump);

    let tier_name = &replicaset.tier;
    let tier = topology_ref
        .tier_by_name(tier_name)
        .expect("tier for instance should exists");

    // Update vshard configuraion so it also knows that new replicas are available
    let bump = Tier::get_vshard_config_version_bump_op(tier)?;
    bump_dml.push(bump);

    let master = targets.iter().find(|target| target.name == master_name);
    if let Some(master) = master {
        // No need to send sync RPC to current master, it's always synchronized with itself

        let mut ops = bump_dml;

        let mut update_ops = UpdateOps::new();
        update_ops.assign(
            column_name!(Instance, sync_incarnation),
            master.target_state.incarnation,
        )?;
        let dml = Dml::update(
            storage::Instances::TABLE_ID,
            &[master_name],
            update_ops,
            ADMIN_ID,
        )?;
        ops.push(dml);

        let op = Op::single_dml_or_batch(ops);
        // Implicit ranges are sufficient
        let predicate = cas::Predicate::new(applied, []);
        let cas = cas::Request::new(op, predicate, ADMIN_ID)?;

        return Ok(Some(
            ActualizeMasterSyncIncarnation {
                replicaset_name: replicaset_name.clone(),
                master_name: master_name.clone(),
                cas,
            }
            .into(),
        ));
    }

    let mut laggers = Vec::with_capacity(targets.len());
    let mut raft_ids = Vec::with_capacity(targets.len());
    let mut dmls = Vec::with_capacity(targets.len());

    for target in targets {
        let mut update_ops = UpdateOps::new();
        update_ops.assign(
            column_name!(Instance, sync_incarnation),
            target.target_state.incarnation,
        )?;
        let dml = Dml::update(
            storage::Instances::TABLE_ID,
            &[&target.name],
            update_ops,
            ADMIN_ID,
        )?;
        laggers.push(target.name.clone());
        raft_ids.push(target.raft_id);
        dmls.push(dml);
    }

    return Ok(Some(
        ReplicationSync {
            replicaset_name: replicaset_name.clone(),
            master_name: master_name.clone(),
            get_vclock_rpc,
            laggers,
            raft_ids,
            dmls,
            sync_rpc,
            bump_dml,
        }
        .into(),
    ));
}

fn get_replicaset_to_sync<'i>(
    topology_ref: &'i TopologyCacheRef,
) -> Option<(&'i Replicaset, Vec<&'i Instance>)> {
    let mut replicaset: Option<&Replicaset> = None;
    let mut targets = Vec::new();
    for instance in topology_ref.all_instances() {
        let instance_name = &instance.name;
        let replicaset_name = &instance.replicaset_name;

        if !instance.may_respond() {
            // Don't send RPC to instance who will probably not reply to it
            continue;
        }

        if !instance.replication_sync_needed() {
            // We're looking specifically for these guys in here
            continue;
        }

        if let Some(replicaset) = &replicaset {
            if replicaset_name != &replicaset.name {
                // Only handle instances from one replicaset at a time
                continue;
            }
        } else {
            replicaset = topology_ref.replicaset_by_name(replicaset_name).ok();
            if replicaset.is_none() {
                warn_or_panic!("replicaset '{replicaset_name}' info not found (needed for instance '{instance_name}')");
                continue;
            }
        }

        targets.push(instance);
    }

    if targets.is_empty() {
        // Nobody needs syncing
        return None;
    }

    let replicaset = replicaset.expect("already checked");
    Some((replicaset, targets))
}
