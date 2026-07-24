use crate::cas;
use crate::column_name;
use crate::config::AlterSystemParameters;
use crate::governor::plan::get_replicaset_config_version_bump_op;
use crate::governor::plan::stage::Plan;
use crate::governor::plan::stage::*;
use crate::has_states;
use crate::instance::Instance;
use crate::instance::InstanceName;
use crate::replicaset::{has_synchro_quorum, Replicaset};
use crate::rpc;
use crate::schema::ADMIN_ID;
use crate::storage;
use crate::storage::SystemTable;
use crate::sync::GetVclockRpc;
use crate::tarantool::box_is_ro;
use crate::tier::Tier;
use crate::tlog;
use crate::topology_cache::TopologyCacheRef;
use crate::traft::op::Dml;
use crate::traft::op::Op;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;
use crate::traft::Result;
use crate::version::version_is_new_enough;
use crate::warn_or_panic;
use smol_str::SmolStr;
use std::collections::HashMap;
use tarantool::space::UpdateOps;

////////////////////////////////////////////////////////////////////////////////
// handle_self_read_only
////////////////////////////////////////////////////////////////////////////////

pub fn handle_self_read_only<'i>(
    topology_ref: &TopologyCacheRef,
    db_config: &AlterSystemParameters,
) -> Option<Plan<'i>> {
    if !box_is_ro() {
        return None;
    }

    // Governor (raft leader) is the master of it's replicaset but it's
    // currently read_only. This could be a problem, because if there are any
    // unapplied DDL operations, our raft_main_loop will be blocked. Normally
    // read_only flag is controlled via RPC proc_replication, but in order for
    // governor to call this RPC it needs to see that target_config_version !=
    // current_config_version for a given replicaset, but that will only be true
    // if a corresponding global DML is applied in the raft_main_loop, which
    // could be a problem if this instance is read_only. Anyway this is a
    // situation which we encountered in the rolling upgrade rollback test and
    // now we want to fix it.
    //
    // And the fix is simple. If governor sees that it's the master and has
    // read_only flag set to true, then it will simply set that flag to false
    // without any RPC and/or global DML. Those will still be performed on the
    // corresponding step.

    let synchronous_replication_enabled = db_config.is_synchronous_replication();
    // In a sync tier writability of the replicaset is gated by tarantool raft
    // elections and flipping "read_only" here cannot lift the forced read-only
    // state of a non-leader. The equivalent self-heal - the designated master
    // promoting itself back to election leadership - is performed by the
    // election watcher on the instance itself, no governor step is needed.
    if synchronous_replication_enabled {
        return None;
    }

    let this_instance = topology_ref.try_this_instance()?;
    let this_replicaset = topology_ref.try_this_replicaset()?;

    if this_replicaset.effective_master_name() != Some(&this_instance.name) {
        return None;
    }

    Some(SelfReadOnlyFalse {}.into())
}

////////////////////////////////////////////////////////////////////////////////
// handle_replication_config
////////////////////////////////////////////////////////////////////////////////

pub fn handle_replication_config<'i>(
    topology_ref: &TopologyCacheRef,
    db_config: &AlterSystemParameters,
    peer_addresses: &'i HashMap<RaftId, SmolStr>,
    applied: RaftIndex,
) -> Result<Option<Plan<'i>>> {
    let Some((replicaset, targets, replicaset_peers)) =
        get_replicaset_to_configure(topology_ref, db_config, peer_addresses)
    else {
        return Ok(None);
    };

    // Targets must not be empty, otherwise we would bump the version
    // without actually calling the RPC.
    debug_assert!(!targets.is_empty());
    let replicaset_name = replicaset.name.clone();

    let master_name = replicaset.effective_master_name().cloned();

    let mut ops = UpdateOps::new();
    ops.assign(
        column_name!(Replicaset, current_config_version),
        replicaset.target_config_version,
    )?;
    let dml = Dml::update(
        storage::Replicasets::TABLE_ID,
        &[&replicaset_name],
        ops,
        ADMIN_ID,
    )?;
    // Implicit ranges are sufficient
    let predicate = cas::Predicate::new(applied, []);
    let cas = cas::Request::new(dml, predicate, ADMIN_ID)?;
    let replication_config_version_actualize = cas;

    Ok(Some(
        ConfigureReplication {
            replicaset_name,
            targets,
            master_name,
            replicaset_peers,
            replication_config_version_actualize,
        }
        .into(),
    ))
}

#[allow(clippy::type_complexity)]
fn get_replicaset_to_configure<'t>(
    topology_ref: &'t TopologyCacheRef,
    db_config: &AlterSystemParameters,
    peer_addresses: &HashMap<RaftId, SmolStr>,
) -> Option<(&'t Replicaset, Vec<(InstanceName, RaftId)>, Vec<SmolStr>)> {
    for replicaset in topology_ref.all_replicasets() {
        if replicaset.current_config_version == replicaset.target_config_version {
            // Already configured
            continue;
        }

        // In a sync tier replicas follow the elected leader and a deposed
        // master cannot introduce replication conflicts (synchronous
        // replication + PROMOTE guarantee a linear history), so the
        // sync-isolation of waking up instances below is not needed.
        let governor_driven_sync = !db_config.replication_mode(&replicaset.tier).is_sync();

        let replicaset_name = &replicaset.name;
        let mut targets = Vec::new();
        let mut replication_config = Vec::new();
        // FIXME: for better perf we should keep a mapping from replicaset to
        // it's instances, so that we don't have to go over all instances in
        // cluster per each replicaset
        for instance in topology_ref.all_instances() {
            let instance_name = &instance.name;
            if instance.replicaset_name != replicaset_name {
                continue;
            }

            if !instance.may_respond() {
                // Don't send RPC to instance who will probably not reply to it
                continue;
            }

            targets.push((instance_name.clone(), instance.raft_id));

            if governor_driven_sync
                && instance.replication_sync_needed()
                && Some(instance_name) != replicaset.effective_master_name()
            {
                // Don't add the waking up instance to other replica
                // box.cfg.replication configs, until it synchronizes. This
                // helps us isolate the healthy portion of the replicaset from
                // potential replication conflicts from deposed masters.
            } else if let Some(address) = peer_addresses.get(&instance.raft_id) {
                replication_config.push(address.clone());
            } else {
                warn_or_panic!("replica `{instance_name}` address unknown, will be excluded from box.cfg.replication of replicaset `{replicaset_name}`");
            }
        }

        // If replication_config is empty but there are targets, it means all
        // instances need sync (cold restart scenario) or master is transitioning.
        // Only include the master to preserve conflict isolation: replicas sync
        // FROM master first, preventing conflicts from spreading to healthy instances.
        if replication_config.is_empty() && !targets.is_empty() {
            let current_master = &replicaset.current_master_name;
            let target_master = &replicaset.target_master_name;

            let find_master_addr = |master_name: &InstanceName| -> Option<SmolStr> {
                targets
                    .iter()
                    .find(|(name, _)| name == master_name)
                    .and_then(|(_, raft_id)| peer_addresses.get(raft_id).cloned())
            };

            if let Some(addr) = find_master_addr(current_master) {
                tlog!(
                    Info,
                    "all instances in replicaset {replicaset_name} need sync, \
                            using current master {current_master} for replication"
                );
                replication_config.push(addr);
            } else if let Some(addr) = find_master_addr(target_master) {
                tlog!(
                    Info,
                    "all instances in replicaset {replicaset_name} need sync, \
                            current master {current_master} not responsive, \
                            using target master {target_master} for replication"
                );
                replication_config.push(addr);
            } else {
                tlog!(
                    Warning,
                    "all instances in replicaset {replicaset_name} need sync, \
                               but neither current master {current_master} nor \
                               target master {target_master} is responsive, \
                               waiting for master or failover"
                );
            }
        }

        if !targets.is_empty() {
            return Some((replicaset, targets, replication_config));
        }

        #[rustfmt::skip]
        tlog!(Warning, "all replicas in {replicaset_name} are offline, skipping replication configuration");
    }

    // No replication configuration needed
    None
}

////////////////////////////////////////////////////////////////////////////////
// handle_sync_master_election_promote
////////////////////////////////////////////////////////////////////////////////

/// Handles a master change in a synchronous-replication tier:
/// `target_master_name` != `current_master_name`. This covers both the
/// voluntary switchover (manual retargeting via CAS, expel of a live master)
/// and dead-master failover - when the master goes offline the governor
/// retargets mastership to an online replica and this stage transfers it
/// there.
///
/// In a sync tier replicaset writability is gated by tarantool raft elections
/// in `manual` mode, so the transfer is performed by promoting the target via
/// [`rpc::replication::proc_replication_promote`] (`box.ctl.promote`): the
/// target wins an election with a higher term and the incumbent (if still
/// alive) is deposed by it, becoming read-only. The election vclock rules
/// guarantee the target has caught up before it can win.
///
/// The promotion is only attempted when enough replicaset members are alive
/// to plausibly win the election. An election below the quorum can never
/// succeed, and retrying it forever would wedge the governor loop - the
/// replicaset must simply stay read-only (fenced) until enough members
/// return.
pub fn handle_sync_master_election_promote<'i>(
    topology_ref: &TopologyCacheRef,
    db_config: &AlterSystemParameters,
    term: RaftTerm,
) -> Result<Option<Plan<'i>>> {
    for replicaset in topology_ref.all_replicasets() {
        if replicaset.current_master_name == replicaset.target_master_name {
            continue;
        }

        let Ok(tier) = topology_ref.tier_by_name(&replicaset.tier) else {
            warn_or_panic!("No info for tier {}", replicaset.tier);
            continue;
        };
        if !db_config.replication_mode(&tier.name).is_sync() {
            continue;
        }

        let new_master_name = replicaset.target_master_name.clone();
        let Ok(new_master) = topology_ref.instance_by_name(&new_master_name) else {
            warn_or_panic!("No info for instance {new_master_name}");
            continue;
        };

        if !new_master.may_respond() {
            // Can't promote an instance which is down; the failure detector
            // will retarget mastership if this goes on.
            continue;
        }

        let old_master_name = replicaset.current_master_name.clone();

        if !has_synchro_quorum(replicaset, topology_ref) {
            tlog!(
                Info,
                "not promoting {new_master_name} as master of replicaset {}: not enough live members to win the election",
                replicaset.name,
            );
            continue;
        }

        let replicaset_name = replicaset.name.clone();

        let mut replicaset_dml = UpdateOps::new();
        replicaset_dml.assign(
            column_name!(Replicaset, current_master_name),
            &new_master_name,
        )?;

        let ranges = vec![
            // We make a decision based on these instances' state so the operation
            // should fail in case there's a change to it in the uncommitted log
            cas::Range::new(storage::Instances::TABLE_ID).eq([&old_master_name]),
            cas::Range::new(storage::Instances::TABLE_ID).eq([&new_master_name]),
        ];

        let promote_rpc = rpc::replication::PromoteRequest { term };
        let fallback_candidates = deterministic_fallback_candidates(
            replicaset,
            &new_master_name,
            topology_ref.all_instances(),
        );

        return Ok(Some(
            ReplicasetMasterElectionPromote {
                replicaset_name,
                old_master_name,
                new_master_name,
                promote_rpc,
                replicaset_dml,
                fallback_candidates,
                ranges,
            }
            .into(),
        ));
    }

    Ok(None)
}

/// Return eligible alternatives in deterministic "next after failed target"
/// order. The reported election leader can override this order at execution
/// time, once the promotion RPC returns it.
fn deterministic_fallback_candidates<'a>(
    replicaset: &Replicaset,
    failed_target: &InstanceName,
    instances: impl IntoIterator<Item = &'a Instance>,
) -> Vec<PromotionCandidate> {
    let mut eligible: Vec<_> = instances
        .into_iter()
        .filter(|instance| {
            instance.replicaset_name == replicaset.name
                && instance.may_respond()
                && has_states!(instance, * -> Online)
        })
        .collect();
    eligible.sort_by(|left, right| left.name.cmp(&right.name));

    if let Some(position) = eligible
        .iter()
        .position(|instance| instance.name == failed_target)
    {
        let len = eligible.len();
        eligible.rotate_left((position + 1) % len);
    }

    eligible
        .into_iter()
        .filter(|instance| instance.name != failed_target)
        .map(|instance| PromotionCandidate {
            name: instance.name.clone(),
            uuid: instance.uuid.clone(),
        })
        .collect()
}

pub(super) fn choose_fallback_candidate<'a>(
    candidates: &'a [PromotionCandidate],
    reported_leader_uuid: Option<&str>,
) -> Option<&'a PromotionCandidate> {
    reported_leader_uuid
        .and_then(|uuid| candidates.iter().find(|candidate| candidate.uuid == uuid))
        .or_else(|| candidates.first())
}

////////////////////////////////////////////////////////////////////////////////
// handle_replicaset_master_switchover
////////////////////////////////////////////////////////////////////////////////

pub fn handle_replicaset_master_switchover<'i>(
    topology_ref: &TopologyCacheRef,
    db_config: &AlterSystemParameters,
    term: RaftTerm,
    sync_timeout: std::time::Duration,
) -> Result<Option<Plan<'i>>> {
    for replicaset in topology_ref.all_replicasets() {
        if replicaset.current_master_name == replicaset.target_master_name {
            continue;
        }

        let Ok(tier) = topology_ref.tier_by_name(&replicaset.tier) else {
            warn_or_panic!("No info for tier {}", replicaset.tier);
            continue;
        };
        if db_config.replication_mode(&tier.name).is_sync() {
            // Mastership of sync-tier replicasets is decided by tarantool raft
            // elections, see handle_sync_master_election_promote.
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
        if tier.has_buckets() {
            let vshard_config_version_bump = Tier::get_vshard_config_version_bump_op(tier)?;
            bump_dml.push(vshard_config_version_bump);
        }

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

        let new_master_raft_id = topology_ref.instance_by_name(&new_master_name)?.raft_id;

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
                new_master_raft_id,
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
    db_config: &AlterSystemParameters,
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

    let Some((replicaset, targets)) = get_replicaset_to_sync(topology_ref, db_config) else {
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
    if tier.has_buckets() {
        let bump = Tier::get_vshard_config_version_bump_op(tier)?;
        bump_dml.push(bump);
    }

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
    db_config: &AlterSystemParameters,
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
            let Ok(found) = topology_ref.replicaset_by_name(replicaset_name) else {
                warn_or_panic!("replicaset '{replicaset_name}' info not found (needed for instance '{instance_name}')");
                continue;
            };
            if db_config.replication_mode(&found.tier).is_sync() {
                // Sync-tier replicas follow the elected leader on their own;
                // the governor doesn't orchestrate replication sync for them.
                continue;
            }
            replicaset = Some(found);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ReplicationMode;
    use crate::instance::{State, StateVariant};
    use crate::tier::TierConfig;
    use crate::topology_cache::TopologyCache;

    #[test]
    fn fallback_candidates_rotate_deterministically_and_prefer_reported_leader() {
        let mut replicaset = Replicaset::for_tests();
        replicaset.name = "r".into();

        let mut instances = Vec::new();
        for name in ["i3", "i1", "i2", "i4"] {
            let mut instance = Instance::for_tests();
            instance.name = name.into();
            instance.uuid = format!("{name}-uuid").into();
            instance.replicaset_name = replicaset.name.clone();
            if name == "i4" {
                instance.target_state = State::new(StateVariant::Expelled, 1);
            }
            instances.push(instance);
        }

        let candidates = deterministic_fallback_candidates(&replicaset, &"i2".into(), &instances);
        let names: Vec<_> = candidates
            .iter()
            .map(|candidate| candidate.name.as_ref())
            .collect();
        assert_eq!(names, ["i3", "i1"]);

        let selected = choose_fallback_candidate(&candidates, Some("i1-uuid")).unwrap();
        assert_eq!(selected.name, "i1");
        let selected = choose_fallback_candidate(&candidates, Some("unknown")).unwrap();
        assert_eq!(selected.name, "i3");
    }

    #[test]
    fn master_switchover_plan_depends_on_replication_mode() {
        let topology = TopologyCache::for_tests();

        let mut old_master = Instance::for_tests();
        old_master.name = "i1".into();
        old_master.uuid = "i1-uuid".into();
        old_master.raft_id = 1;
        old_master.replicaset_name = "r".into();
        old_master.replicaset_uuid = "r-uuid".into();
        old_master.tier = "storage".into();

        let mut new_master = old_master.clone();
        new_master.name = "i2".into();
        new_master.uuid = "i2-uuid".into();
        new_master.raft_id = 2;

        let mut replicaset = Replicaset::for_tests();
        replicaset.name = "r".into();
        replicaset.uuid = "r-uuid".into();
        replicaset.current_master_name = old_master.name.clone();
        replicaset.target_master_name = new_master.name.clone();
        replicaset.tier = "storage".into();

        let mut tier = Tier::default();
        tier.name = "storage".into();
        tier.replication_factor = 2;

        topology.update_instance(None, Some(old_master));
        topology.update_instance(None, Some(new_master));
        topology.update_replicaset(None, Some(replicaset));
        topology.update_tier(None, Some(tier.clone()));
        let topology_ref = topology.get();

        let mut db_config = AlterSystemParameters::default();
        let mut tier_config = TierConfig::for_tier(&tier);
        tier_config.replication_mode = ReplicationMode::Async;
        db_config
            .per_tier
            .insert(tier.name.clone(), tier_config.clone());

        let legacy_plan = handle_replicaset_master_switchover(
            &topology_ref,
            &db_config,
            7,
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        assert!(matches!(
            legacy_plan,
            Some(Plan::ReplicasetMasterConsistentSwitchover { .. })
        ));
        assert!(
            handle_sync_master_election_promote(&topology_ref, &db_config, 7)
                .unwrap()
                .is_none()
        );

        tier_config.replication_mode = ReplicationMode::Sync;
        db_config.per_tier.insert(tier.name.clone(), tier_config);

        assert!(handle_replicaset_master_switchover(
            &topology_ref,
            &db_config,
            7,
            std::time::Duration::from_secs(1),
        )
        .unwrap()
        .is_none());
        let election_plan =
            handle_sync_master_election_promote(&topology_ref, &db_config, 7).unwrap();
        assert!(matches!(
            election_plan,
            Some(Plan::ReplicasetMasterElectionPromote { .. })
        ));
    }
}
