use super::conf_change::raft_conf_change;
use crate::cas;
use crate::column_name;
use crate::has_states;
use crate::instance::state::State;
use crate::instance::state::StateVariant;
use crate::instance::{Instance, InstanceName};
use crate::plugin::PluginIdentifier;
use crate::plugin::PluginOp;
use crate::plugin::TopologyUpdateOpKind;
use crate::replicaset::ReplicasetState;
use crate::replicaset::WeightOrigin;
use crate::replicaset::{Replicaset, ReplicasetName};
use crate::rpc;
use crate::rpc::update_instance::prepare_update_instance_cas_request;
use crate::schema::{
    PluginConfigRecord, PluginDef, ServiceDef, ServiceRouteItem, ServiceRouteKey, ADMIN_ID,
};
use crate::storage;
use crate::storage::PropertyName;
use crate::storage::TClusterwideTable;
use crate::sync::GetVclockRpc;
use crate::tier::Tier;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::error::IdOfInstance;
use crate::traft::op::Dml;
use crate::traft::op::Op;
#[allow(unused_imports)]
use crate::traft::op::PluginRaftOp;
use crate::traft::Result;
use crate::traft::{RaftId, RaftIndex, RaftTerm};
use crate::util::Uppercase;
use crate::warn_or_panic;
use ::tarantool::space::UpdateOps;
use std::collections::HashMap;
use std::collections::HashSet;
use tarantool::vclock::Vclock;

#[allow(clippy::too_many_arguments)]
pub(super) fn action_plan<'i>(
    term: RaftTerm,
    applied: RaftIndex,
    cluster_name: String,
    instances: &'i [Instance],
    existing_fds: &HashSet<Uppercase>,
    peer_addresses: &'i HashMap<RaftId, String>,
    voters: &[RaftId],
    learners: &[RaftId],
    replicasets: &HashMap<&ReplicasetName, &'i Replicaset>,
    tiers: &HashMap<&str, &'i Tier>,
    my_raft_id: RaftId,
    has_pending_schema_change: bool,
    plugins: &HashMap<PluginIdentifier, PluginDef>,
    services: &HashMap<PluginIdentifier, Vec<&'i ServiceDef>>,
    plugin_op: Option<&'i PluginOp>,
    sync_timeout: std::time::Duration,
    global_cluster_version: String,
) -> Result<Plan<'i>> {
    // This function is specifically extracted, to separate the task
    // construction from any IO and/or other yielding operations.
    #[cfg(debug_assertions)]
    let _guard = crate::util::NoYieldsGuard::new();

    ////////////////////////////////////////////////////////////////////////////
    // transfer raft leadership
    let Some(this_instance) = instances
        .iter()
        .find(|instance| instance.raft_id == my_raft_id)
    else {
        return Err(Error::NoSuchInstance(IdOfInstance::RaftId(my_raft_id)));
    };
    if has_states!(this_instance, * -> Offline) || has_states!(this_instance, * -> Expelled) {
        let mut new_leader = None;
        for instance in instances {
            if has_states!(instance, * -> Offline) || has_states!(instance, * -> Expelled) {
                continue;
            }

            if !voters.contains(&instance.raft_id) {
                continue;
            }

            new_leader = Some(instance);
            break;
        }
        if let Some(new_leader) = new_leader {
            return Ok(TransferLeadership { to: new_leader }.into());
        } else {
            tlog!(Warning, "leader is going offline and no substitution is found";
                "leader_raft_id" => my_raft_id,
                "voters" => ?voters,
            );
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // conf change
    if let Some(conf_change) = raft_conf_change(instances, voters, learners, tiers) {
        return Ok(ConfChange { conf_change }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // instance going offline non-gracefully
    // TODO: it should be possible to move this step to where Expell is handled
    let to_downgrade = instances
        .iter()
        .find(|instance| has_states!(instance, not Offline -> Offline));

    if let Some(instance) = to_downgrade {
        let instance_name = &instance.name;
        let new_current_state = instance.target_state.variant.as_str();

        let replicaset = *replicasets
            .get(&instance.replicaset_name)
            .expect("replicaset info is always present");
        let tier = *tiers
            .get(&*instance.tier)
            .expect("tier info is always present");

        // TODO: if this is replication leader, we should demote it and wait
        // until someone is promoted in it's place (which implies synchronizing
        // vclocks). Except that we can't do this reliably, as tarantool will
        // stop accepting incoming connections once the on_shutdown even happens.
        // This means that we can't reliably send rpc requests to instances with
        // target state Offline and basically there's nothing we can do about
        // such instances.
        //
        // Therefore basically the user should never expect that turning off a
        // replication leader is safe. Instead it should always first transfer
        // the replication leadership to another instance.

        let req = rpc::update_instance::Request::new(instance_name.clone(), cluster_name)
            .with_current_state(instance.target_state);
        let cas_parameters = prepare_update_instance_cas_request(
            &req,
            instance,
            replicaset,
            tier,
            existing_fds,
            &global_cluster_version,
        )?;

        let (ops, ranges) = cas_parameters.expect("already check current state is different");
        let predicate = cas::Predicate::new(applied, ranges);
        let op = Op::single_dml_or_batch(ops);
        let cas = cas::Request::new(op, predicate, ADMIN_ID)?;
        return Ok(Downgrade {
            instance_name,
            new_current_state,
            cas,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // update target replicaset master
    let new_target_master = get_new_replicaset_master_if_needed(instances, replicasets);
    if let Some((to, replicaset)) = new_target_master {
        debug_assert_eq!(to.replicaset_name, replicaset.name);
        let mut ops = UpdateOps::new();
        ops.assign(column_name!(Replicaset, target_master_name), &to.name)?;
        let dml = Dml::update(
            storage::Replicasets::TABLE_ID,
            &[&to.replicaset_name],
            ops,
            ADMIN_ID,
        )?;
        let ranges = vec![
            cas::Range::new(storage::Instances::TABLE_ID).eq([&to.name]),
            cas::Range::new(storage::Instances::TABLE_ID).eq([&replicaset.target_master_name]),
        ];
        let predicate = cas::Predicate::new(applied, ranges);
        let cas = cas::Request::new(dml, predicate, ADMIN_ID)?;
        return Ok(UpdateTargetReplicasetMaster { cas }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // configure replication
    if let Some((replicaset, targets, replicaset_peers)) =
        get_replicaset_to_configure(instances, peer_addresses, replicasets)
    {
        // Targets must not be empty, otherwise we would bump the version
        // without actually calling the RPC.
        debug_assert!(!targets.is_empty());
        let replicaset_name = &replicaset.name;

        let mut master_name = None;
        if replicaset.current_master_name == replicaset.target_master_name {
            master_name = Some(&replicaset.current_master_name);
        }

        let mut ops = UpdateOps::new();
        ops.assign(
            column_name!(Replicaset, current_config_version),
            replicaset.target_config_version,
        )?;
        let dml = Dml::update(
            storage::Replicasets::TABLE_ID,
            &[replicaset_name],
            ops,
            ADMIN_ID,
        )?;
        // Implicit ranges are sufficient
        let predicate = cas::Predicate::new(applied, []);
        let cas = cas::Request::new(dml, predicate, ADMIN_ID)?;
        let replication_config_version_actualize = cas;

        return Ok(ConfigureReplication {
            replicaset_name,
            targets,
            master_name,
            replicaset_peers,
            replication_config_version_actualize,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // replicaset master switchover
    //
    // This must be done after instances have (re)configured replication
    // because master switchover requires synchronizing via tarantool replication.
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

        if let Some(bump) =
            get_replicaset_config_version_bump_op_if_needed(replicasets, replicaset_name)
        {
            bump_dml.push(bump);
        }

        let tier_name = &r.tier;
        let tier = tiers
            .get(tier_name.as_str())
            .expect("tier for instance should exists");

        let vshard_config_version_bump = Tier::get_vshard_config_version_bump_op_if_needed(tier)?;
        if let Some(bump) = vshard_config_version_bump {
            bump_dml.push(bump);
        }

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

            return Ok(ReplicasetMasterConsistentSwitchover {
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
            .into());
        } else {
            let get_vclock_rpc = GetVclockRpc {};

            return Ok(ReplicasetMasterFailover {
                old_master_name,
                new_master_name,
                get_vclock_rpc,
                replicaset_name,
                replicaset_dml,
                bump_dml,
                ranges,
            }
            .into());
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // proposing automatic replicaset state & weight change
    let to_change_weights = get_replicaset_state_change(instances, replicasets, tiers);
    if let Some((replicaset_name, tier, need_to_update_weight)) = to_change_weights {
        let mut uops = UpdateOps::new();
        if need_to_update_weight {
            uops.assign(column_name!(Replicaset, weight), 1.)?;
        }
        uops.assign(column_name!(Replicaset, state), ReplicasetState::Ready)?;
        let dml = Dml::update(
            storage::Replicasets::TABLE_ID,
            &[replicaset_name],
            uops,
            ADMIN_ID,
        )?;

        let mut ranges = vec![];
        let mut ops = vec![];
        ranges.push(cas::Range::for_dml(&dml)?);
        ops.push(dml);

        let vshard_config_version_bump = Tier::get_vshard_config_version_bump_op_if_needed(tier)?;
        if let Some(bump) = vshard_config_version_bump {
            ranges.push(cas::Range::for_dml(&bump)?);
            ops.push(bump);
        }

        let op = Op::single_dml_or_batch(ops);
        let predicate = cas::Predicate::new(applied, ranges);
        let cas = cas::Request::new(op, predicate, ADMIN_ID)?;

        return Ok(ProposeReplicasetStateChanges { cas }.into());
    }

    for (&tier_name, &tier) in tiers.iter() {
        ////////////////////////////////////////////////////////////////////////////
        // update current vshard config
        let mut first_ready_replicaset = None;
        if !tier.vshard_bootstrapped {
            first_ready_replicaset =
                get_first_ready_replicaset_in_tier(instances, replicasets, tier_name);
        }

        // Note: the following is a hack stemming from the fact that we have to work around vshard's weird quirks.
        // Everything having to deal with bootstrapping vshard should be removed completely once we migrate to our custom sharding solution.
        //
        // Vshard will fail if we configure it with all replicaset weights set to 0.
        // But we don't set a replicaset's weight until it's filled up to the replication factor.
        // So we wait until at least one replicaset is filled (i.e. `first_ready_replicaset.is_some()`).
        //
        // Also if vshard has already been bootstrapped, the user can mess this up by setting all replicasets' weights to 0,
        // which will break vshard configuration, but this will be the user's fault probably, not sure we can do something about it
        let ok_to_configure_vshard = tier.vshard_bootstrapped || first_ready_replicaset.is_some();

        if ok_to_configure_vshard
            && tier.current_vshard_config_version != tier.target_vshard_config_version
        {
            // Note at this point all the instances should have their replication configured,
            // so it's ok to configure sharding for them
            let targets = maybe_responding(instances)
                .map(|instance| &instance.name)
                .collect();
            let rpc = rpc::sharding::Request {
                term,
                applied,
                timeout: sync_timeout,
            };

            let mut uops = UpdateOps::new();
            uops.assign(
                column_name!(Tier, current_vshard_config_version),
                tier.target_vshard_config_version,
            )?;

            let bump = Dml::update(storage::Tiers::TABLE_ID, &[tier_name], uops, ADMIN_ID)?;

            let ranges = vec![cas::Range::for_dml(&bump)?];
            let predicate = cas::Predicate::new(applied, ranges);
            let cas = cas::Request::new(bump, predicate, ADMIN_ID)?;

            return Ok(UpdateCurrentVshardConfig {
                targets,
                rpc,
                cas,
                tier_name: tier_name.into(),
            }
            .into());
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // bootstrap sharding on each tier
    for (&tier_name, &tier) in tiers.iter() {
        if tier.vshard_bootstrapped {
            continue;
        }

        if let Some(r) = get_first_ready_replicaset_in_tier(instances, replicasets, tier_name) {
            debug_assert!(
                !tier.vshard_bootstrapped,
                "bucket distribution only needs to be bootstrapped once"
            );
            let target = &r.current_master_name;
            let tier_name = &r.tier;
            let rpc = rpc::sharding::bootstrap::Request {
                term,
                applied,
                timeout: sync_timeout,
                tier: tier_name.into(),
            };

            let mut uops = UpdateOps::new();
            uops.assign(column_name!(Tier, vshard_bootstrapped), true)?;

            let dml = Dml::update(storage::Tiers::TABLE_ID, &[tier_name], uops, ADMIN_ID)?;

            let ranges = vec![cas::Range::for_dml(&dml)?];
            let predicate = cas::Predicate::new(applied, ranges);
            let cas = cas::Request::new(dml, predicate, ADMIN_ID)?;

            return Ok(ShardingBoot {
                target,
                rpc,
                cas,
                tier_name: tier_name.into(),
            }
            .into());
        };
    }

    ////////////////////////////////////////////////////////////////////////////
    // expel replicaset
    if let Some((master, replicaset, tier)) =
        get_replicaset_being_expelled(instances, replicasets, tiers)
    {
        debug_assert_eq!(replicaset.state, ReplicasetState::ToBeExpelled);

        let master_name = &master.name;
        let target = &replicaset.current_master_name;
        debug_assert_eq!(master_name, target);

        let replicaset_name = replicaset.name.clone();

        let rpc = rpc::sharding::WaitBucketCountRequest {
            term,
            applied,
            timeout: sync_timeout,
            expected_bucket_count: 0,
        };

        // Mark last instance as expelled
        let mut req = rpc::update_instance::Request::new(master_name.clone(), cluster_name);

        if has_states!(master, * -> Expelled) {
            req = req.with_current_state(master.target_state);
        } else {
            // Replicaset's state is ToBeExpelled, this could only happen if
            // the last Instance in it had it's target state Expelled (and no
            // instances can join such a replicaset). But now the instance's target
            // state is not Expelled. This could happen if instance goes Offline
            // while the replicaset is being expelled, or something else.
            // The solution is to change the state back to Expelled. It is safe
            // to do so, because the replicaset is already in a state of being
            // expelled.
            let incarnation = master.target_state.incarnation;
            req = req
                .with_target_state(StateVariant::Expelled)
                .with_current_state(State::new(StateVariant::Expelled, incarnation));
        }

        let update_instance = prepare_update_instance_cas_request(
            &req,
            master,
            replicaset,
            tier,
            existing_fds,
            &global_cluster_version,
        )?;
        let (mut ops, ranges) = update_instance.expect("already checked target state != current");

        // Mark replicaset as expelled
        let mut update_ops = UpdateOps::new();
        update_ops.assign(column_name!(Replicaset, state), ReplicasetState::Expelled)?;
        let dml = Dml::update(
            storage::Replicasets::TABLE_ID,
            &[&replicaset_name],
            update_ops,
            ADMIN_ID,
        )?;
        ops.push(dml);

        let predicate = cas::Predicate::new(applied, ranges);
        let dml = Op::BatchDml { ops };
        let cas = cas::Request::new(dml, predicate, ADMIN_ID)?;
        return Ok(ExpelReplicaset {
            replicaset_name,
            target,
            rpc,
            cas,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // prepare a replicaset for expel
    if let Some((tier, replicaset)) = get_replicaset_to_expel(instances, replicasets, tiers) {
        // Master switchover happens on a governor step with higher priority
        debug_assert_eq!(
            replicaset.current_master_name,
            replicaset.target_master_name
        );
        let replicaset_name = replicaset.name.clone();

        let mut update_ops = UpdateOps::new();
        update_ops.assign(column_name!(Replicaset, weight), 0.0)?;
        #[rustfmt::skip]
        update_ops.assign(column_name!(Replicaset, state), ReplicasetState::ToBeExpelled)?;

        let mut ops = vec![];
        let dml = Dml::update(
            storage::Replicasets::TABLE_ID,
            &[&replicaset_name],
            update_ops,
            ADMIN_ID,
        )?;
        ops.push(dml);

        if let Some(bump) = Tier::get_vshard_config_version_bump_op_if_needed(tier)? {
            ops.push(bump);
        }

        let ranges = vec![
            // Decision was made based on this instance's state so we must make sure it was up to date.
            cas::Range::new(storage::Instances::TABLE_ID).eq([&replicaset.current_master_name]),
            // The rest of the ranges are implicit.
        ];

        let predicate = cas::Predicate::new(applied, ranges);
        let dml = Op::BatchDml { ops };
        let cas = cas::Request::new(dml, predicate, ADMIN_ID)?;
        let replicaset_name = replicaset.name.clone();
        return Ok(PrepareReplicasetForExpel {
            replicaset_name,
            cas,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // expel instance
    let target = instances
        .iter()
        .find(|instance| has_states!(instance, not Expelled -> Expelled));

    if let Some(instance) = target {
        let instance_name = &instance.name;
        let new_current_state = instance.target_state.variant.as_str();

        let replicaset = *replicasets
            .get(&instance.replicaset_name)
            .expect("replicaset info is always present");
        let tier = *tiers
            .get(&*instance.tier)
            .expect("tier info is always present");

        let req = rpc::update_instance::Request::new(instance_name.clone(), cluster_name)
            .with_current_state(instance.target_state);
        let cas_parameters = prepare_update_instance_cas_request(
            &req,
            instance,
            replicaset,
            tier,
            existing_fds,
            &global_cluster_version,
        )?;

        let (ops, ranges) = cas_parameters.expect("already check current state is different");
        let predicate = cas::Predicate::new(applied, ranges);
        let op = Op::single_dml_or_batch(ops);
        let cas = cas::Request::new(op, predicate, ADMIN_ID)?;
        return Ok(Downgrade {
            instance_name,
            new_current_state,
            cas,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // to online
    let to_online = instances
        .iter()
        .find(|instance| has_states!(instance, not Online -> Online) || instance.is_reincarnated());

    if let Some(instance) = to_online {
        let instance_name = &instance.name;
        let target_state = instance.target_state;
        debug_assert_eq!(target_state.variant, StateVariant::Online);
        let new_current_state = target_state.variant.as_str();

        let replicaset = *replicasets
            .get(&instance.replicaset_name)
            .expect("replicaset info is always present");
        let tier = *tiers
            .get(&*instance.tier)
            .expect("tier info is always present");

        let plugin_rpc = rpc::enable_all_plugins::Request {
            term,
            applied,
            timeout: sync_timeout,
        };

        let req = rpc::update_instance::Request::new(instance_name.clone(), cluster_name)
            .with_current_state(target_state);
        let cas_parameters = prepare_update_instance_cas_request(
            &req,
            instance,
            replicaset,
            tier,
            existing_fds,
            &global_cluster_version,
        )?;

        let (ops, ranges) = cas_parameters.expect("already check current state is different");
        let predicate = cas::Predicate::new(applied, ranges);
        let op = Op::single_dml_or_batch(ops);
        let cas = cas::Request::new(op, predicate, ADMIN_ID)?;

        return Ok(ToOnline {
            target: instance_name,
            new_current_state,
            plugin_rpc,
            cas,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // ddl
    if has_pending_schema_change {
        let targets = rpc::replicasets_masters(replicasets, instances);
        let rpc = rpc::ddl_apply::Request {
            term,
            applied,
            timeout: sync_timeout,
        };
        return Ok(ApplySchemaChange { rpc, targets }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // install plugin
    if let Some(PluginOp::CreatePlugin {
        manifest,
        inherit_topology,
    }) = plugin_op
    {
        let ident = manifest.plugin_identifier();
        if plugins.get(&ident).is_some() {
            warn_or_panic!(
                "received a request to install a plugin which is already installed {ident:?}"
            );
        }

        let mut targets = Vec::with_capacity(instances.len());
        for i in maybe_responding(instances) {
            targets.push(&i.name);
        }

        let rpc = rpc::load_plugin_dry_run::Request {
            term,
            applied,
            timeout: sync_timeout,
        };

        let plugin_def = manifest.plugin_def();
        let mut ranges = vec![];
        let mut ops = vec![];

        let dml = Dml::replace(storage::Plugins::TABLE_ID, &plugin_def, ADMIN_ID)?;
        ranges.push(cas::Range::for_dml(&dml)?);
        ops.push(dml);

        let ident = plugin_def.into_identifier();
        for mut service_def in manifest.service_defs() {
            if let Some(service_topology) = inherit_topology.get(&service_def.name) {
                service_def.tiers = service_topology.clone();
            }
            let dml = Dml::replace(storage::Services::TABLE_ID, &service_def, ADMIN_ID)?;
            ranges.push(cas::Range::for_dml(&dml)?);
            ops.push(dml);

            let config = manifest
                .get_default_config(&service_def.name)
                .expect("configuration should exist");
            let config_records =
                PluginConfigRecord::from_config(&ident, &service_def.name, config.clone())?;

            for config_rec in config_records {
                let dml = Dml::replace(storage::PluginConfig::TABLE_ID, &config_rec, ADMIN_ID)?;
                ranges.push(cas::Range::for_dml(&dml)?);
                ops.push(dml);
            }
        }

        let dml = Dml::delete(
            storage::Properties::TABLE_ID,
            &[PropertyName::PendingPluginOperation],
            ADMIN_ID,
        )?;
        ranges.push(cas::Range::for_dml(&dml)?);
        ops.push(dml);

        let success_dml = Op::BatchDml { ops };
        return Ok(CreatePlugin {
            targets,
            rpc,
            success_dml,
            ranges,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // enable plugin
    if let Some(PluginOp::EnablePlugin {
        plugin,
        timeout: on_start_timeout,
    }) = plugin_op
    {
        let service_defs = services.get(plugin).map(|v| &**v).unwrap_or(&[]);

        let targets = maybe_responding(instances).map(|i| &i.name).collect();

        let rpc = rpc::enable_plugin::Request {
            term,
            applied,
            timeout: sync_timeout,
        };

        let mut ranges = vec![];
        let mut success_dml = vec![];
        let mut enable_ops = UpdateOps::new();
        enable_ops.assign(column_name!(PluginDef, enabled), true)?;
        let dml = Dml::update(
            storage::Plugins::TABLE_ID,
            &[&plugin.name, &plugin.version],
            enable_ops,
            ADMIN_ID,
        )?;
        ranges.push(cas::Range::for_dml(&dml)?);
        success_dml.push(dml);

        for i in instances {
            for svc in service_defs {
                if !svc.tiers.contains(&i.tier) {
                    continue;
                }
                let dml = Dml::replace(
                    storage::ServiceRouteTable::TABLE_ID,
                    &ServiceRouteItem::new_healthy(i.name.clone(), plugin, &svc.name),
                    ADMIN_ID,
                )?;
                ranges.push(cas::Range::for_dml(&dml)?);
                success_dml.push(dml);
            }
        }

        let dml = Dml::delete(
            storage::Properties::TABLE_ID,
            &[PropertyName::PendingPluginOperation],
            ADMIN_ID,
        )?;
        ranges.push(cas::Range::for_dml(&dml)?);
        success_dml.push(dml);
        let success_dml = Op::BatchDml { ops: success_dml };

        return Ok(EnablePlugin {
            rpc,
            targets,
            on_start_timeout: *on_start_timeout,
            ident: plugin,
            success_dml,
            ranges,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // update service tiers
    if let Some(PluginOp::AlterServiceTiers {
        plugin,
        service,
        tier,
        kind,
    }) = plugin_op
    {
        let mut enable_targets = Vec::with_capacity(instances.len());
        let mut disable_targets = Vec::with_capacity(instances.len());
        let mut on_success_dml = vec![];
        let mut ranges = vec![];

        let plugin_def = plugins
            .get(plugin)
            .expect("operation for non existent plugin");
        let service_def = *services
            .get(plugin)
            .expect("operation for non existent service")
            .iter()
            .find(|s| &s.name == service)
            .expect("operation for non existent service");

        let mut new_service_def = service_def.clone();
        let new_tiers = &mut new_service_def.tiers;
        match kind {
            TopologyUpdateOpKind::Add => {
                if new_tiers.iter().all(|t| t != tier) {
                    new_tiers.push(tier.clone());
                }
            }
            TopologyUpdateOpKind::Remove => {
                new_tiers.retain(|t| t != tier);
            }
        }

        let old_tiers = &service_def.tiers;

        // note: no need to enable/disable service and update routing table if plugin disabled
        if plugin_def.enabled {
            for i in maybe_responding(instances) {
                // if instance in both new and old tiers - do nothing
                if new_tiers.contains(&i.tier) && old_tiers.contains(&i.tier) {
                    continue;
                }

                if new_tiers.contains(&i.tier) {
                    enable_targets.push(&i.name);
                    let dml = Dml::replace(
                        storage::ServiceRouteTable::TABLE_ID,
                        &ServiceRouteItem::new_healthy(i.name.clone(), plugin, &service_def.name),
                        ADMIN_ID,
                    )?;
                    ranges.push(cas::Range::for_dml(&dml)?);
                    on_success_dml.push(dml);
                }

                if old_tiers.contains(&i.tier) {
                    disable_targets.push(&i.name);
                    let key = ServiceRouteKey {
                        instance_name: &i.name,
                        plugin_name: &plugin.name,
                        plugin_version: &plugin.version,
                        service_name: &service_def.name,
                    };
                    let dml = Dml::delete(storage::ServiceRouteTable::TABLE_ID, &key, ADMIN_ID)?;
                    ranges.push(cas::Range::for_dml(&dml)?);
                    on_success_dml.push(dml);
                }
            }
        }

        let dml = Dml::replace(storage::Services::TABLE_ID, &new_service_def, ADMIN_ID)?;
        ranges.push(cas::Range::for_dml(&dml)?);
        on_success_dml.push(dml);

        let dml = Dml::delete(
            storage::Properties::TABLE_ID,
            &[PropertyName::PendingPluginOperation],
            ADMIN_ID,
        )?;
        ranges.push(cas::Range::for_dml(&dml)?);
        on_success_dml.push(dml);
        let success_dml = Op::BatchDml {
            ops: on_success_dml,
        };

        let enable_rpc = rpc::enable_service::Request {
            term,
            applied,
            timeout: sync_timeout,
        };
        let disable_rpc = rpc::disable_service::Request {
            term,
            applied,
            timeout: sync_timeout,
        };

        return Ok(AlterServiceTiers {
            enable_targets,
            disable_targets,
            enable_rpc,
            disable_rpc,
            success_dml,
            ranges,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // upgrade _pico_property.cluster_version
    let mut new_cluster_version_candidate = None;
    let mut new_candidate_counter = 0;
    let mut expelled_instances = 0;
    for instance in instances {
        if has_states!(instance, Expelled -> *) {
            expelled_instances += 1;
            continue;
        }

        let instance_version = &instance.picodata_version;
        match rpc::join::compare_picodata_versions(&global_cluster_version, instance_version) {
            Ok(0) => {
                // if at least one version is equal to global_cluster_version
                // don't upgrade the global_cluster_version
                break;
            }
            Ok(1) => {
                // Version is exactly one higher, collect it
                new_candidate_counter += 1;
                new_cluster_version_candidate = Some(instance_version);
            }
            Err(e) => {
                tlog!(
                    Warning,
                    "Instance {} has incompatible version {} compared to cluster version {}",
                    instance.name,
                    instance_version,
                    global_cluster_version
                );
                return Err(e);
            }
            _ => {}
        }
    }

    // number of instances that has states either Online or Offline
    let retained_instances = instances.len() - expelled_instances;

    // If all instances have new version, update the cluster version
    if new_candidate_counter == retained_instances {
        if let Some(new_version) = new_cluster_version_candidate {
            let mut ops = UpdateOps::new();
            ops.assign("value", new_version)?;

            let dml = Dml::replace(
                storage::Properties::TABLE_ID,
                &(PropertyName::ClusterVersion, new_version),
                ADMIN_ID,
            )?;

            let ranges = vec![cas::Range::for_dml(&dml)?];
            let predicate = cas::Predicate::new(applied, ranges);
            let cas = cas::Request::new(dml, predicate, ADMIN_ID)?;

            return Ok(UpdateClusterVersion { cas }.into());
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // no action needed
    Ok(Plan::None)
}

macro_rules! define_plan {
    (
        $(
            pub struct $stage:ident $(<$lt:tt>)? {
                $(
                    $(#[$field_meta:meta])*
                    pub $field:ident: $field_ty:ty,
                )+
            }
        )+
    ) => {
        $(
            pub struct $stage $(<$lt>)? {
                $(
                    $(#[$field_meta])*
                    pub $field: $field_ty,
                )+
            }

            impl<'i> From<$stage $(<$lt>)?> for Plan<'i> {
                fn from(s: $stage $(<$lt>)?) -> Self {
                    Self::$stage(s)
                }
            }
        )+

        pub enum Plan<'i> {
            None,
            $(
                $stage ( $stage $(<$lt>)? ),
            )+
        }

    }
}

use stage::*;

pub mod stage {
    use super::*;
    use std::time::Duration;

    define_plan! {
        pub struct ConfChange {
            pub conf_change: raft::prelude::ConfChangeV2,
        }

        pub struct UpdateCurrentVshardConfig<'i> {
            /// Instances to send the `rpc` request to.
            pub targets: Vec<&'i InstanceName>,
            /// Request to call [`rpc::sharding::proc_sharding`] on `targets`.
            pub rpc: rpc::sharding::Request,
            /// Global DML operation which updates `current_vshard_config_version` in corresponding record of table `_pico_tier`.
            pub cas: cas::Request,
            /// Tier name to which the vshard configuration applies
            pub tier_name: String,
        }

        pub struct TransferLeadership<'i> {
            /// This instance should be nominated as next raft leader.
            pub to: &'i Instance,
        }

        pub struct UpdateTargetReplicasetMaster {
            /// Global DML operation which updates `target_master_name` in table `_pico_replicaset`.
            pub cas: cas::Request,
        }

        pub struct UpdateClusterVersion {
            /// Global DML operation in _pico_property to update _cluster_version
            pub cas: cas::Request,
        }

        pub struct ReplicasetMasterFailover<'i> {
            /// This replicaset is changing it's master.
            pub replicaset_name: &'i ReplicasetName,

            /// This instance was master, but is now non responsive. Name only used for logging.
            pub old_master_name: &'i InstanceName,

            /// Request to call [`proc_get_vclock`] on the new master. This
            /// vclock is going to be peristed as `promotion_vclock` (if it's
            /// not already equal) of the given replicaset.
            ///
            /// [`proc_get_vclock`]: crate::sync::proc_get_vclock
            pub get_vclock_rpc: GetVclockRpc,

            /// This is the new master. It will be sent a RPC [`proc_get_vclock`] to set the
            /// promotion vclock in case the old master is not available.
            ///
            /// [`proc_get_vclock`]: crate::sync::proc_get_vclock
            pub new_master_name: &'i InstanceName,

            /// Part of the global DML operation which updates a `_pico_replicaset` record
            /// with the new values for `current_master_name` & `promotion_vclock`.
            /// Note: it is only the part of the operation, because we don't know the promotion_vclock yet.
            pub replicaset_dml: UpdateOps,

            /// Optional operations to bump versions of replication and sharding configs.
            pub bump_dml: Vec<Dml>,

            /// Cas ranges for the operation.
            pub ranges: Vec<cas::Range>,
        }

        pub struct ReplicasetMasterConsistentSwitchover<'i> {
            /// This replicaset is changing it's master.
            pub replicaset_name: &'i ReplicasetName,

            /// This instance will be demoted.
            pub old_master_name: &'i InstanceName,

            /// Request to call [`rpc::replication::proc_replication_demote`] on old master.
            pub demote_rpc: rpc::replication::DemoteRequest,

            /// This is the new master. It will be sent a RPC [`proc_get_vclock`] to set the
            /// promotion vclock in case the old master is not available.
            ///
            /// [`proc_get_vclock`]: crate::sync::proc_get_vclock
            pub new_master_name: &'i InstanceName,

            /// Request to call [`rpc::replication::proc_replication_sync`] on new master.
            pub sync_rpc: rpc::replication::ReplicationSyncRequest,

            /// Current `promotion_vclock` value of given replicaset.
            pub promotion_vclock: &'i Vclock,

            /// Global DML operation which updates a `_pico_replicaset` record
            /// with the new values for `current_master_name`.
            /// Note: this dml will only be applied if after RPC requests it
            /// turns out that the old master's vclock is not behind the current
            /// promotion vclock. Otherwise the vclock from old master is going
            /// to be persisted as the new `promotion_vclock`.
            pub master_actualize_dml: Dml,

            /// Optional operations to bump versions of replication and sharding configs.
            pub bump_dml: Vec<Dml>,

            /// Cas ranges for the operation.
            pub ranges: Vec<cas::Range>,
        }

        // TODO: rename, after we renamed `grade` -> `state` this step's name makes no sense at all
        pub struct Downgrade<'i> {
            /// This instance is being downgraded. The name is only used for logging.
            pub instance_name: &'i InstanceName,
            /// The state which is going to be set as target's new current state. Is only used for loggin.
            pub new_current_state: &'i str,
            /// Global DML which updates `current_state` to `Offline` in `_pico_instance` for a given instance.
            pub cas: cas::Request,
        }

        pub struct ConfigureReplication<'i> {
            /// This replicaset is being (re)configured. The id is only used for logging.
            pub replicaset_name: &'i ReplicasetName,
            /// These instances belong to one replicaset and will be sent a
            /// request to call [`rpc::replication::proc_replication`].
            pub targets: Vec<&'i InstanceName>,
            /// This instance will also become the replicaset master.
            /// This will be `None` if replicaset's current_master_name != target_master_name.
            pub master_name: Option<&'i InstanceName>,
            /// This is an explicit list of peer addresses.
            pub replicaset_peers: Vec<String>,
            /// Global DML operation which updates `current_config_version` in table `_pico_replicaset` for the given replicaset.
            pub replication_config_version_actualize: cas::Request,
        }

        pub struct ShardingBoot<'i> {
            /// This instance will be initializing the bucket distribution.
            pub target: &'i InstanceName,
            /// Request to call [`rpc::sharding::bootstrap::proc_sharding_bootstrap`] on `target`.
            pub rpc: rpc::sharding::bootstrap::Request,
            /// Global DML operation which updates `vshard_bootstrapped` in corresponding record of table `_pico_tier`.
            pub cas: cas::Request,
            /// Tier name where vshard.bootstrap takes place.
            pub tier_name: String,
        }

        pub struct ProposeReplicasetStateChanges {
            /// Global DML operation which updates `weight` to `1` & `state` to `ready`
            /// in table `_pico_replicaset` for given replicaset.
            ///
            /// This also optionally includes version bumps for replicaset and vshard.
            pub cas: cas::Request,
        }

        pub struct PrepareReplicasetForExpel {
            /// This replicaset is being prepared for expel. Id is only used for logging.
            pub replicaset_name: ReplicasetName,
            /// Global batch DML to do the following:
            /// - update replicaset record:
            ///     - set `weight` to 0
            ///     - set `state` to ToBeExpelled
            /// - bump the corresponding tier's vhsard config version
            pub cas: cas::Request,
        }

        pub struct ExpelReplicaset<'i> {
            /// This replicaset is being expelled. Id is only used for logging.
            pub replicaset_name: ReplicasetName,
            /// This is the master and the last instance in the replicaset being expelled.
            pub target: &'i InstanceName,
            /// Request to call [`rpc::sharding::proc_wait_bucket_count`] on `target`.
            pub rpc: rpc::sharding::WaitBucketCountRequest,
            /// Global batch DML to do the following:
            /// - update replicaset record:
            ///     - set `state` to Expelled
            /// - update master's instance record:
            ///     - set `current_state` to Expelled
            pub cas: cas::Request,
        }

        pub struct ToOnline<'i> {
            /// This instance's is becomming online. Name is only used for logging.
            pub target: &'i InstanceName,
            /// This is going to be the new current state of the instance. Only used for logging.
            pub new_current_state: &'i str,
            /// Request to call [`rpc::enable_all_plugins::proc_enable_all_plugins`] on `target`.
            /// It is not optional, although it probably should be.
            pub plugin_rpc: rpc::enable_all_plugins::Request,
            /// Update instance request which translates into a global DML operation
            /// which updates `current_state` to `Online` in table `_pico_instance` for a given instance.
            pub cas: cas::Request,
        }

        pub struct ApplySchemaChange<'i> {
            /// These are masters of all the replicasets in the cluster.
            pub targets: Vec<&'i InstanceName>,
            /// Request to call [`rpc::ddl_apply::proc_apply_schema_change`] on `targets`.
            pub rpc: rpc::ddl_apply::Request,
        }

        pub struct CreatePlugin<'i> {
            /// This is every instance which is currently online.
            pub targets: Vec<&'i InstanceName>,
            /// Request to call [`rpc::load_plugin_dry_run::proc_load_plugin_dry_run`] on `targets`.
            pub rpc: rpc::load_plugin_dry_run::Request,
            /// Global batch DML operation which creates records in `_pico_plugin`, `_pico_service`, `_pico_plugin_config`
            /// and removes "pending_plugin_operation" from `_pico_property` in case of success.
            pub success_dml: Op,
            /// Ranges for both the `success_dml` and the rollback_op which may
            /// occur if creating the plugin fails.
            pub ranges: Vec<cas::Range>,
        }

        pub struct EnablePlugin<'i> {
            /// This is every instance which is currently online.
            pub targets: Vec<&'i InstanceName>,
            /// Request to call [`rpc::enable_plugin::proc_enable_plugin`] on `targets`.
            pub rpc: rpc::enable_plugin::Request,
            /// Rpc response must arrive within this timeout. Otherwise the operation is rolled back.
            pub on_start_timeout: Duration,
            /// Identifier of the plugin. This will be used to construct a [`PluginRaftOp::DisablePlugin`]
            /// raft operation if the RPC fails on some of the targets.
            pub ident: &'i PluginIdentifier,
            /// Global batch DML operation which updates records in `_pico_service`, `_pico_service_route`
            /// and removes "pending_plugin_operation" from `_pico_property` in case of success.
            pub success_dml: Op,
            /// Ranges for both the `success_dml` and the rollback_op which may
            /// occur if enabling the plugin fails.
            pub ranges: Vec<cas::Range>,
        }

        pub struct AlterServiceTiers<'i> {
            /// This is the list of instances on which we want to enable the service.
            pub enable_targets: Vec<&'i InstanceName>,
            /// This is the list of instances on which we want to disable the service.
            pub disable_targets: Vec<&'i InstanceName>,
            /// Request to call [`rpc::enable_service::proc_enable_service`] on `enable_targets`.
            pub enable_rpc: rpc::enable_service::Request,
            /// Request to call [`rpc::disable_service::proc_disable_service`] on `disable_targets`.
            pub disable_rpc: rpc::disable_service::Request,
            /// Global batch DML operation which updates records in `_pico_service`, `_pico_service_route`
            /// and removes "pending_plugin_operation" from `_pico_property` in case of success.
            pub success_dml: Op,
            /// Ranges for both the `success_dml` and the rollback_op which may
            /// occur if enabling the services fails.
            pub ranges: Vec<cas::Range>,
        }
    }
}

fn get_replicaset_to_configure<'i>(
    instances: &'i [Instance],
    peer_addresses: &'i HashMap<RaftId, String>,
    replicasets: &HashMap<&ReplicasetName, &'i Replicaset>,
) -> Option<(&'i Replicaset, Vec<&'i InstanceName>, Vec<String>)> {
    for replicaset in replicasets.values() {
        if replicaset.current_config_version == replicaset.target_config_version {
            // Already configured
            continue;
        }

        let replicaset_name = &replicaset.name;
        let mut rpc_targets = Vec::new();
        let mut replication_peers = Vec::new();
        for instance in instances {
            if has_states!(instance, Expelled -> *) {
                // Expelled instances are ignored for everything,
                // we only store them for history
                continue;
            }

            if instance.replicaset_name != replicaset_name {
                continue;
            }

            let instance_name = &instance.name;
            if let Some(address) = peer_addresses.get(&instance.raft_id) {
                replication_peers.push(address.clone());
            } else {
                warn_or_panic!("replica `{instance_name}` address unknown, will be excluded from box.cfg.replication of replicaset `{replicaset_name}`");
            }

            if instance.may_respond() {
                rpc_targets.push(instance_name);
            }
        }

        if !rpc_targets.is_empty() {
            return Some((replicaset, rpc_targets, replication_peers));
        }

        #[rustfmt::skip]
        tlog!(Warning, "all replicas in {replicaset_name} are offline, skipping replication configuration");
    }

    // No replication configuration needed
    None
}

/// Checks if there's replicaset whose master is offline and tries to find a
/// replica to promote.
///
/// This covers the case when a replicaset is waking up.
#[inline(always)]
fn get_new_replicaset_master_if_needed<'i>(
    instances: &'i [Instance],
    replicasets: &HashMap<&ReplicasetName, &'i Replicaset>,
) -> Option<(&'i Instance, &'i Replicaset)> {
    // TODO: construct a map from replicaset name to instance to improve performance
    for &r in replicasets.values() {
        if r.state == ReplicasetState::ToBeExpelled || r.state == ReplicasetState::Expelled {
            continue;
        }

        #[rustfmt::skip]
        let Some(master) = instances.iter().find(|i| i.name == r.target_master_name) else {
            #[rustfmt::skip]
            warn_or_panic!("couldn't find instance with name {}, which is chosen as next master of replicaset {}",
                           r.target_master_name, r.name);
            continue;
        };

        if master.replicaset_name != r.name {
            #[rustfmt::skip]
            tlog!(Warning, "target master {} of replicaset {} is from different a replicaset {}: trying to choose a new one",
                  master.name, master.replicaset_name, r.name);
        } else if has_states!(master, * -> not Online) {
            #[rustfmt::skip]
            tlog!(Info, "target master {} of replicaset {} is going {}: trying to choose a new one",
                  master.name, master.replicaset_name, master.target_state.variant);
        } else {
            continue;
        }

        let mut offline_replica = None;
        for instance in instances {
            if instance.replicaset_name != r.name {
                continue;
            }

            if has_states!(instance, * -> Online) {
                // Found a replacement for new replicaset master
                return Some((instance, r));
            }

            if offline_replica.is_none() && has_states!(instance, * -> Offline) {
                offline_replica = Some(instance);
            }
        }

        if has_states!(master, * -> Expelled) {
            // If current master is going Expelled and there's no Online
            // replicas, the new master will be the Offline instance.
            if let Some(new_master) = offline_replica {
                return Some((new_master, r));
            }
        }

        #[rustfmt::skip]
        tlog!(Warning, "there are no instances suitable as master of replicaset {}", r.name);
    }

    None
}

#[inline(always)]
fn get_replicaset_state_change<'i>(
    instances: &'i [Instance],
    replicasets: &HashMap<&ReplicasetName, &Replicaset>,
    tiers: &HashMap<&str, &'i Tier>,
) -> Option<(&'i ReplicasetName, &'i Tier, bool)> {
    let mut replicaset_sizes = HashMap::new();
    for instance in maybe_responding(instances) {
        let instance_name = &instance.name;
        if has_states!(instance, Expelled -> *) {
            continue;
        }

        let replicaset_name = &instance.replicaset_name;
        let tier = &instance.tier;

        let replicaset_size = replicaset_sizes.entry(replicaset_name).or_insert(0);
        *replicaset_size += 1;
        let Some(r) = replicasets.get(replicaset_name) else {
            #[rustfmt::skip]
            warn_or_panic!("replicaset info not found for replicaset '{replicaset_name}', set as replicaset of instance '{instance_name}'");
            continue;
        };
        if r.state != ReplicasetState::NotReady {
            continue;
        }

        let tier_info = tiers
            .get(tier.as_str())
            .expect("tier for instance should exists");

        // TODO: set replicaset.state = NotReady if it was Ready but is no
        // longer full
        // See <https://git.picodata.io/core/picodata/-/issues/188>
        if *replicaset_size < tier_info.replication_factor {
            continue;
        }
        let need_to_update_weight = r.weight_origin != WeightOrigin::User;
        return Some((replicaset_name, tier_info, need_to_update_weight));
    }
    None
}

pub fn get_first_ready_replicaset_in_tier<'r>(
    instances: &[Instance],
    replicasets: &HashMap<&ReplicasetName, &'r Replicaset>,
    tier_name: &str,
) -> Option<&'r Replicaset> {
    for Instance {
        replicaset_name, ..
    } in maybe_responding(instances).filter(|instance| instance.tier == tier_name)
    {
        let Some(replicaset) = replicasets.get(replicaset_name) else {
            continue;
        };

        if replicaset.state == ReplicasetState::Ready && replicaset.weight > 0. {
            return Some(replicaset);
        }
    }
    None
}

/// Constructs a global Dml operation to bump the target_config_version field
/// in the given replicaset, if it's not already bumped.
fn get_replicaset_config_version_bump_op_if_needed(
    replicasets: &HashMap<&ReplicasetName, &Replicaset>,
    replicaset_name: &ReplicasetName,
) -> Option<Dml> {
    let Some(replicaset) = replicasets.get(replicaset_name) else {
        warn_or_panic!("replicaset info for `{replicaset_name}` is missing");
        return None;
    };

    // Only bump the version if it's not already bumped.
    if replicaset.current_config_version != replicaset.target_config_version {
        return None;
    }

    let mut ops = UpdateOps::new();
    ops.assign(
        column_name!(Replicaset, target_config_version),
        replicaset.target_config_version + 1,
    )
    .expect("won't fail");
    let dml = Dml::update(
        storage::Replicasets::TABLE_ID,
        &[replicaset_name],
        ops,
        ADMIN_ID,
    )
    .expect("can't fail");
    Some(dml)
}

pub fn get_replicaset_to_expel<'r>(
    instances: &[Instance],
    replicasets: &HashMap<&ReplicasetName, &'r Replicaset>,
    tiers: &HashMap<&str, &'r Tier>,
) -> Option<(&'r Tier, &'r Replicaset)> {
    for instance in instances {
        if !has_states!(instance, not Expelled -> Expelled) {
            continue;
        }
        let instance_name = &instance.name;
        let replicaset_name = &instance.replicaset_name;
        let tier_id = &*instance.tier;

        let Some(replicaset) = replicasets.get(replicaset_name) else {
            #[rustfmt::skip]
            warn_or_panic!("replicaset info not found for replicaset '{replicaset_name}', set as replicaset of instance '{instance_name}'");
            continue;
        };

        if replicaset.state == ReplicasetState::ToBeExpelled {
            // Replicaset is already is the process of being expelled.
            continue;
        }

        // If replicaset is expelled, all of it's instances must already be expelled as well.
        debug_assert_ne!(replicaset.state, ReplicasetState::Expelled);

        if replicaset.current_master_name != instance_name {
            // Expelled instance is not a master, nothing else to do
            continue;
        } else {
            // Master of the replicaset is being expelled. This means that this
            // is the last instance in the replicaset....
            debug_assert_eq!(
                instances
                    .iter()
                    .filter(|i| i.replicaset_name == replicaset_name)
                    .filter(|i| has_states!(i, not Expelled -> *))
                    .count(),
                1
            );
        }

        let Some(tier) = tiers.get(tier_id) else {
            #[rustfmt::skip]
            warn_or_panic!("tier info not found for tier '{tier_id}', set as tier of instance '{instance_name}'");
            continue;
        };

        return Some((tier, replicaset));
    }

    None
}

pub fn get_replicaset_being_expelled<'r>(
    instances: &'r [Instance],
    replicasets: &HashMap<&ReplicasetName, &'r Replicaset>,
    tiers: &HashMap<&str, &'r Tier>,
) -> Option<(&'r Instance, &'r Replicaset, &'r Tier)> {
    for replicaset in replicasets.values() {
        let tier_id = &replicaset.tier;
        debug_assert_eq!(
            replicaset.current_master_name,
            replicaset.target_master_name
        );
        let master_name = &replicaset.current_master_name;
        let replicaset_name = &replicaset.name;

        if replicaset.state != ReplicasetState::ToBeExpelled {
            continue;
        }

        let master = instances.iter().find(|i| i.name == master_name);
        let Some(master) = master else {
            #[rustfmt::skip]
            warn_or_panic!("instance info not found for instance named '{master_name}', which is chosen as master of replicaset '{replicaset_name}'");
            continue;
        };

        if has_states!(master, * -> not Expelled) {
            let master_state = master.target_state.variant;
            tlog!(Warning, "replicaset '{replicaset_name}' was going to be expelled, but it's master '{master_name}' is no longer going to be expelled (target state = {master_state})");
        }

        let Some(tier) = tiers.get(&**tier_id) else {
            #[rustfmt::skip]
            warn_or_panic!("tier info not found for tier '{tier_id}', set as tier of replicaset '{replicaset_name}'");
            continue;
        };

        debug_assert_eq!(&master.tier, tier_id);

        return Some((master, *replicaset, *tier));
    }

    None
}

#[inline(always)]
fn maybe_responding(instances: &[Instance]) -> impl Iterator<Item = &Instance> {
    instances.iter().filter(|instance| instance.may_respond())
}
