use crate::cas;
use crate::catalog::governor_queue::GovernorOperationDef;
use crate::column_name;
use crate::governor::batch::get_next_batch_for_to_online;
use crate::governor::batch::LastStepInfo;
use crate::governor::conf_change::raft_conf_change;
use crate::governor::ddl::handle_pending_ddl;
use crate::governor::queue::handle_governor_queue;
use crate::governor::replication::handle_replicaset_master_switchover;
use crate::governor::replication::handle_replicaset_sync;
use crate::governor::replication::handle_replication_config;
use crate::governor::replication::handle_self_read_only;
use crate::governor::sharding::{handle_sharding, handle_sharding_bootstrap};
use crate::has_states;
use crate::instance::state::{State, StateVariant};
use crate::instance::{Instance, InstanceName};
use crate::plugin::{PluginIdentifier, PluginOp, TopologyUpdateOpKind};
use crate::replicaset::{Replicaset, ReplicasetName, ReplicasetState, WeightOrigin};
use crate::rpc;
use crate::rpc::update_instance::prepare_update_instance_cas_request;
use crate::schema::{
    PluginConfigRecord, PluginDef, ServiceDef, ServiceRouteItem, ServiceRouteKey, TableDef,
    ADMIN_ID,
};
use crate::sentinel::SentinelStatus;
use crate::storage;
use crate::storage::{PropertyName, SystemTable};
use crate::sync::GetVclockRpc;
use crate::tier::Tier;
use crate::tlog;
use crate::topology_cache::TopologyCacheRef;
use crate::traft::error::{Error, IdOfInstance};
#[allow(unused_imports)]
use crate::traft::op::PluginRaftOp;
use crate::traft::op::{Ddl, Dml, Op};
use crate::traft::{RaftId, RaftIndex, RaftTerm, Result};
use crate::warn_or_panic;
use smol_str::SmolStr;
use std::collections::HashMap;
use std::time::Duration;
use tarantool::space::{SpaceId, UpdateOps};
use tarantool::time::Instant;
use tarantool::vclock::Vclock;

macro_rules! debug_assert_plan_kind {
    ($plan:expr, $pattern:pat) => {
        debug_assert!(
            matches!($plan, $pattern),
            "Unexpected ActionKind: {:?}, expected this: {}",
            $plan.kind(),
            stringify!($pattern)
        );
    };
}

#[allow(clippy::too_many_arguments)]
pub(super) fn action_plan<'i>(
    term: RaftTerm,
    applied: RaftIndex,
    cluster_name: &'static str,
    cluster_uuid: &'static str,
    sentinel_status: SentinelStatus,
    topology_ref: &TopologyCacheRef,
    instances: &'i [Instance],
    peer_addresses: &'i HashMap<RaftId, SmolStr>,
    voters: &[RaftId],
    learners: &[RaftId],
    replicasets: &HashMap<&ReplicasetName, &'i Replicaset>,
    tiers: &HashMap<&str, &'i Tier>,
    my_raft_id: RaftId,
    pending_ddl: &'i Option<(Ddl, u64)>,
    tables: &HashMap<SpaceId, &'i TableDef>,
    plugins: &HashMap<PluginIdentifier, PluginDef>,
    services: &HashMap<PluginIdentifier, Vec<&'i ServiceDef>>,
    plugin_op: Option<&'i PluginOp>,
    sync_timeout: Duration,
    batch_size: usize,
    global_cluster_version: SmolStr,
    schema_version: u64,
    governor_operations: &'i [GovernorOperationDef],
    global_catalog_version: SmolStr,
    pending_catalog_version: Option<SmolStr>,
    last_step_info: &mut LastStepInfo,
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

    let activation_finished = sentinel_status != SentinelStatus::Initial;

    let not_online = |instance: &Instance| {
        has_states!(instance, * -> Offline) || has_states!(instance, * -> Expelled)
    };

    if !activation_finished && not_online(this_instance) {
        tlog!(
            Debug,
            "activation not finished yet, not transferring raft leadership!"
        );
    } else if not_online(this_instance) {
        let mut new_leader = None;
        for instance in instances {
            if not_online(instance) {
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
    // special case read_only = false for this instance
    if let Some(plan) = handle_self_read_only(topology_ref) {
        debug_assert_plan_kind!(plan, Plan::SelfReadOnlyFalse { .. });

        return Ok(plan);
    }

    ////////////////////////////////////////////////////////////////////////////
    // instance going offline non-gracefully
    // TODO: it should be possible to move this step to where Expell is handled
    let to_downgrade = instances
        .iter()
        .find(|instance| has_states!(instance, not Offline -> Offline));

    if let Some(instance) = to_downgrade {
        let instance_name = &instance.name;
        let new_current_state = instance.target_state.variant;

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

        let req =
            rpc::update_instance::Request::new(instance_name.clone(), cluster_name, cluster_uuid)
                .with_current_state(instance.target_state);

        let cas_parameters = prepare_update_instance_cas_request(
            &req,
            instance,
            replicaset,
            tier,
            None,
            &global_cluster_version,
            &global_catalog_version,
        )?;

        let (ops, ranges) = cas_parameters.expect("already check current state is different");
        let predicate = cas::Predicate::new(applied, ranges.into_vec());
        let op = Op::single_dml_or_batch(ops.into_vec());
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
    if let Some(plan) = handle_replication_config(topology_ref, peer_addresses, applied)? {
        debug_assert_plan_kind!(plan, Plan::ConfigureReplication { .. });

        return Ok(plan);
    }

    ////////////////////////////////////////////////////////////////////////////
    // replicaset master switchover
    //
    // This must be done after instances have (re)configured replication
    // because master switchover requires synchronizing via tarantool replication.
    if let Some(plan) = handle_replicaset_master_switchover(topology_ref, term, sync_timeout)? {
        debug_assert_plan_kind!(
            plan,
            Plan::ReplicasetMasterConsistentSwitchover { .. }
                | Plan::ReplicasetMasterFailover { .. }
        );

        return Ok(plan);
    }

    ////////////////////////////////////////////////////////////////////////////
    // replication sync for waking up instances
    //
    // This must be done after
    // - instances have (re)configured replication
    // - replicaset master is actualized
    if let Some(plan) = handle_replicaset_sync(
        topology_ref,
        term,
        applied,
        &global_catalog_version,
        sync_timeout,
    )? {
        debug_assert_plan_kind!(
            plan,
            Plan::ReplicationSync { .. } | Plan::ActualizeMasterSyncIncarnation { .. }
        );

        return Ok(plan);
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

        let mut ops = vec![];
        ops.push(dml);

        // Replicaset's weight may have changed, let vshard know about it so it starts rebalancing buckets
        let vshard_config_version_bump = Tier::get_vshard_config_version_bump_op(tier)?;
        ops.push(vshard_config_version_bump);

        let op = Op::single_dml_or_batch(ops);
        let predicate = cas::Predicate::new(applied, []);
        let cas = cas::Request::new(op, predicate, ADMIN_ID)?;

        return Ok(ProposeReplicasetStateChanges { cas }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // sharding on each tier, update current vshard config
    if let Some(plan) = handle_sharding(
        last_step_info,
        term,
        applied,
        tiers,
        instances,
        replicasets,
        sync_timeout,
        batch_size,
    )? {
        debug_assert_plan_kind!(
            plan,
            Plan::UpdateCurrentVshardConfig { .. }
                | Plan::SleepDueToBackoff(SleepDueToBackoff {
                    step_kind: ActionKind::UpdateCurrentVshardConfig,
                    ..
                })
        );

        return Ok(plan);
    }

    ////////////////////////////////////////////////////////////////////////////
    // bootstrap sharding on each tier
    if let Some(plan) =
        handle_sharding_bootstrap(term, applied, tiers, instances, replicasets, sync_timeout)?
    {
        debug_assert_plan_kind!(plan, Plan::ShardingBoot { .. });

        return Ok(plan);
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
        let mut req =
            rpc::update_instance::Request::new(master_name.clone(), cluster_name, cluster_uuid);

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
                .with_target_state_reason("replicaset expel")
                .with_current_state(State::new(StateVariant::Expelled, incarnation));
        }

        let update_instance = prepare_update_instance_cas_request(
            &req,
            master,
            replicaset,
            tier,
            None,
            &global_cluster_version,
            &global_catalog_version,
        )?;
        let (mut ops, ranges) = update_instance.expect("already checked target state != current");

        // Bump tier configuration version, because the replicaset must now be
        // removed from router configurations of all other replicasets
        let vshard_config_version_bump = Tier::get_vshard_config_version_bump_op(tier)?;
        ops.push(vshard_config_version_bump);

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

        let predicate = cas::Predicate::new(applied, ranges.into_vec());
        let dml = Op::single_dml_or_batch(ops.into_vec());
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

        // Replicaset's weight has changed, let vshard know about it so it starts rebalancing buckets
        let vshard_config_version_bump = Tier::get_vshard_config_version_bump_op(tier)?;
        ops.push(vshard_config_version_bump);

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
        let new_current_state = instance.target_state.variant;

        let replicaset = *replicasets
            .get(&instance.replicaset_name)
            .expect("replicaset info is always present");
        let tier = *tiers
            .get(&*instance.tier)
            .expect("tier info is always present");

        let req =
            rpc::update_instance::Request::new(instance_name.clone(), cluster_name, cluster_uuid)
                .with_current_state(instance.target_state);
        let cas_parameters = prepare_update_instance_cas_request(
            &req,
            instance,
            replicaset,
            tier,
            None,
            &global_cluster_version,
            &global_catalog_version,
        )?;

        let (mut ops, ranges) = cas_parameters.expect("already check current state is different");

        // After instance was expelled we must call proc_replication on all the
        // replicas again so that they all remove the expelled instance from
        // `_cluster` system space.
        let replicaset_config_version_bump = get_replicaset_config_version_bump_op(replicaset);
        ops.push(replicaset_config_version_bump);

        let predicate = cas::Predicate::new(applied, ranges.into_vec());
        let op = Op::single_dml_or_batch(ops.into_vec());
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
    if let Some(plan) = handle_instances_becoming_online(
        last_step_info,
        topology_ref,
        term,
        applied,
        sync_timeout,
        &global_cluster_version,
        &global_catalog_version,
        cluster_name,
        cluster_uuid,
        batch_size,
    )? {
        debug_assert_plan_kind!(
            plan,
            Plan::ToOnline { .. }
                | Plan::SleepDueToBackoff(SleepDueToBackoff {
                    step_kind: ActionKind::ToOnline,
                    ..
                })
        );

        return Ok(plan);
    }

    ////////////////////////////////////////////////////////////////////////////
    // ddl
    if let Some(plan) = handle_pending_ddl(
        last_step_info,
        topology_ref,
        tables,
        pending_ddl,
        schema_version,
        term,
        applied,
        sync_timeout,
        batch_size,
    )? {
        debug_assert_plan_kind!(
            plan,
            Plan::ApplySchemaChange { .. }
                | Plan::ApplyBackup { .. }
                | Plan::CommitTruncateGlobalTable { .. }
                | Plan::ApplyTruncateTable { .. }
                | Plan::SleepDueToBackoff(SleepDueToBackoff {
                    step_kind: ActionKind::ApplySchemaChange
                        | ActionKind::ApplyBackup
                        | ActionKind::ApplyTruncateTable,
                    ..
                })
        );

        return Ok(plan);
    }

    ////////////////////////////////////////////////////////////////////////////
    // install plugin
    if let Some(PluginOp::CreatePlugin {
        manifest,
        inherit_entities,
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
        let mut ops = vec![];

        let dml = Dml::replace(storage::Plugins::TABLE_ID, &plugin_def, ADMIN_ID)?;
        ops.push(dml);

        let ident = plugin_def.into_identifier();
        for mut service_def in manifest.service_defs() {
            if let Some(service_topology) = inherit_topology.get(&service_def.name) {
                service_def.tiers = service_topology.clone();
            }
            let dml = Dml::replace(storage::Services::TABLE_ID, &service_def, ADMIN_ID)?;
            ops.push(dml);

            let config = manifest
                .get_default_config(&service_def.name)
                .expect("configuration should exist");
            let config_records =
                PluginConfigRecord::from_config(&ident, &service_def.name, config.clone())?;

            for config_rec in config_records {
                let dml = Dml::replace(storage::PluginConfig::TABLE_ID, &config_rec, ADMIN_ID)?;
                ops.push(dml);
            }
        }

        for (entity, config) in inherit_entities {
            let config_records = PluginConfigRecord::from_config(&ident, entity, config.clone())?;
            for config_rec in config_records {
                let dml = Dml::replace(storage::PluginConfig::TABLE_ID, &config_rec, ADMIN_ID)?;
                ops.push(dml);
            }
        }

        let dml = Dml::delete(
            storage::Properties::TABLE_ID,
            &[PropertyName::PendingPluginOperation],
            ADMIN_ID,
            None,
        )?;
        ops.push(dml);

        let success_dml = Op::BatchDml { ops };
        return Ok(CreatePlugin {
            targets,
            rpc,
            success_dml,
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

        let mut success_dml = vec![];
        let mut enable_ops = UpdateOps::new();
        enable_ops.assign(column_name!(PluginDef, enabled), true)?;
        let dml = Dml::update(
            storage::Plugins::TABLE_ID,
            &[&plugin.name, &plugin.version],
            enable_ops,
            ADMIN_ID,
        )?;
        success_dml.push(dml);

        for i in instances {
            for svc in service_defs {
                if !svc.tiers.contains(&i.tier) {
                    continue;
                }
                let dml = Dml::replace(
                    storage::ServiceRouteTable::TABLE_ID,
                    &ServiceRouteItem::new_healthy(i.name.clone(), plugin, svc.name.clone()),
                    ADMIN_ID,
                )?;
                success_dml.push(dml);
            }
        }

        let dml = Dml::delete(
            storage::Properties::TABLE_ID,
            &[PropertyName::PendingPluginOperation],
            ADMIN_ID,
            None,
        )?;
        success_dml.push(dml);
        let success_dml = Op::BatchDml { ops: success_dml };

        return Ok(EnablePlugin {
            rpc,
            targets,
            on_start_timeout: *on_start_timeout,
            ident: plugin,
            success_dml,
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
                        &ServiceRouteItem::new_healthy(
                            i.name.clone(),
                            plugin,
                            service_def.name.clone(),
                        ),
                        ADMIN_ID,
                    )?;
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
                    let dml =
                        Dml::delete(storage::ServiceRouteTable::TABLE_ID, &key, ADMIN_ID, None)?;
                    on_success_dml.push(dml);
                }
            }
        }

        let dml = Dml::replace(storage::Services::TABLE_ID, &new_service_def, ADMIN_ID)?;
        on_success_dml.push(dml);

        let dml = Dml::delete(
            storage::Properties::TABLE_ID,
            &[PropertyName::PendingPluginOperation],
            ADMIN_ID,
            None,
        )?;
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

        // FIXME: everytime governor does an iteration we parse each instance's
        // version from string. We should instead store the parsed version in
        // Instance which goes into topology cache.
        // Same with global_cluster_version, global_catalog_version etc.
        let instance_version = &instance.picodata_version;
        match rpc::join::compare_picodata_versions(&global_cluster_version, instance_version) {
            Ok(false) => {
                // if at least one version is equal to global_cluster_version
                // don't upgrade the global_cluster_version
                break;
            }
            Ok(true) => {
                // Version changed, collect it
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
        }
    }

    // number of instances that has states either Online or Offline
    let retained_instances = instances.len() - expelled_instances;

    // If all instances have new version, update the cluster version
    if new_candidate_counter == retained_instances {
        if let Some(new_version) = new_cluster_version_candidate {
            tlog!(
                Info,
                "update cluster version from {} to {}",
                global_cluster_version,
                new_version
            );

            let mut ops = vec![];
            ops.push(Dml::replace(
                storage::Properties::TABLE_ID,
                &(PropertyName::ClusterVersion, new_version),
                ADMIN_ID,
            )?);
            if global_catalog_version != storage::LATEST_SYSTEM_CATALOG_VERSION {
                ops.push(Dml::replace(
                    storage::Properties::TABLE_ID,
                    &(
                        PropertyName::PendingCatalogVersion,
                        storage::LATEST_SYSTEM_CATALOG_VERSION,
                    ),
                    ADMIN_ID,
                )?);
            }

            let op = Op::single_dml_or_batch(ops);
            let predicate = cas::Predicate::new(applied, []);
            let cas = cas::Request::new(op, predicate, ADMIN_ID)?;

            return Ok(UpdateClusterVersion { cas }.into());
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // run operations from `_pico_governor_queue` table
    if let Some(plan) = handle_governor_queue(
        topology_ref,
        governor_operations,
        &global_cluster_version,
        pending_catalog_version,
        global_catalog_version,
        applied,
        sync_timeout,
    )? {
        return Ok(plan);
    }

    ////////////////////////////////////////////////////////////////////////////
    // no action needed
    Ok(Plan::GoIdle)
}

macro_rules! define_plan {
    (
        $(
            $(#[$struct_meta:meta])*
            pub struct $stage:ident $(<$lt:tt>)? {
                $(
                    $(#[$field_meta:meta])*
                    pub $field:ident: $field_ty:ty,
                )*
            }
        )+
    ) => {
        $(
            $(#[$struct_meta])*
            pub struct $stage $(<$lt>)? {
                $(
                    $(#[$field_meta])*
                    pub $field: $field_ty,
                )*
            }

            impl<'i> From<$stage $(<$lt>)?> for Plan<'i> {
                fn from(s: $stage $(<$lt>)?) -> Self {
                    Self::$stage(s)
                }
            }

            impl $(<$lt>)? $stage $(<$lt>)? {
                #[allow(dead_code)]
                pub const KIND: ActionKind = ActionKind::$stage;
            }
        )+

        // We don't care about `large_enum_variant` in this case, because this
        // enum is only stored on the stack and never even gets copied.
        #[allow(clippy::large_enum_variant)]
        pub enum Plan<'i> {
            GoIdle,
            $(
                $(#[$struct_meta])*
                $stage ( $stage $(<$lt>)? ),
            )+
        }

        impl Plan<'_> {
            pub fn kind(&self) -> ActionKind {
                match self {
                    Self::GoIdle => ActionKind::GoIdle,
                    $(
                        Self::$stage { .. } => ActionKind::$stage,
                    )+
                }
            }
        }

        #[derive(Default, Debug, PartialEq, Eq, Hash, Clone, Copy)]
        pub enum ActionKind {
            #[default]
            GoIdle,
            $(
                $(#[$struct_meta])*
                $stage,
            )+
        }
    }
}

use stage::*;

pub mod stage {
    use super::*;

    define_plan! {
        pub struct ConfChange {
            pub conf_change: raft::prelude::ConfChangeV2,
        }

        pub struct SleepDueToBackoff {
            /// We would want to execute this step kind, but can't at the moment because of back-off.
            pub step_kind: ActionKind,
            /// We will be able to execute `step_kind` at this moment in the future.
            pub next_try: Instant,
        }

        /// See comments in [`handle_self_read_only`] for explanation.
        pub struct SelfReadOnlyFalse {}

        pub struct UpdateCurrentVshardConfig {
            /// All instances which need to handle `rpc` request before `cas` can be applied.
            pub targets_total: Vec<InstanceName>,
            /// A batch of instances to send the `rpc` request to on this iteration.
            pub targets_batch: Vec<InstanceName>,
            /// Request to call [`rpc::sharding::proc_sharding`] on `targets`.
            pub rpc: rpc::sharding::Request,
            /// Global DML operation which updates `current_vshard_config_version` in corresponding record of table `_pico_tier`.
            pub cas: cas::Request,
            /// Tier name to which the vshard configuration applies
            pub tier_name: SmolStr,
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

        pub struct ReplicasetMasterFailover {
            /// This replicaset is changing it's master.
            pub replicaset_name: ReplicasetName,

            /// This instance was master, but is now non responsive. Name only used for logging.
            pub old_master_name: InstanceName,

            /// Request to call [`proc_get_vclock`] on the new master. This
            /// vclock is going to be persisted as `promotion_vclock` (if it's
            /// not already equal) of the given replicaset.
            ///
            /// [`proc_get_vclock`]: crate::sync::proc_get_vclock
            pub get_vclock_rpc: GetVclockRpc,

            /// This is the new master. It will be sent a RPC [`proc_get_vclock`] to set the
            /// promotion vclock in case the old master is not available.
            ///
            /// [`proc_get_vclock`]: crate::sync::proc_get_vclock
            pub new_master_name: InstanceName,

            /// Part of the global DML operation which updates a `_pico_replicaset` record
            /// with the new values for `current_master_name` & `promotion_vclock`.
            /// Note: it is only the part of the operation, because we don't know the promotion_vclock yet.
            pub replicaset_dml: UpdateOps,

            /// Optional operations to bump versions of replication and sharding configs.
            pub bump_dml: Vec<Dml>,

            /// Cas ranges for the operation.
            pub ranges: Vec<cas::Range>,
        }

        pub struct ReplicasetMasterConsistentSwitchover {
            /// This replicaset is changing it's master.
            pub replicaset_name: ReplicasetName,

            /// This instance will be demoted.
            pub old_master_name: InstanceName,

            /// Request to call [`rpc::replication::proc_replication_demote`] on old master.
            pub demote_rpc: rpc::replication::DemoteRequest,

            /// This is the new master. It will be sent a RPC [`proc_get_vclock`] to set the
            /// promotion vclock in case the old master is not available.
            ///
            /// [`proc_get_vclock`]: crate::sync::proc_get_vclock
            pub new_master_name: InstanceName,

            /// Raft id of the new master. This is used to update reachability
            /// info about that instance if it's replication is broken.
            pub new_master_raft_id: RaftId,

            /// Request to call [`rpc::replication::proc_replication_sync`] on new master.
            pub sync_rpc: rpc::replication::ReplicationSyncRequest,

            /// Current `promotion_vclock` value of given replicaset.
            pub promotion_vclock: Vclock,

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
            /// The state which is going to be set as target's new current state. Is only used for logging.
            pub new_current_state: StateVariant,
            /// Global DML which updates `current_state` to `Offline` in `_pico_instance` for a given instance.
            pub cas: cas::Request,
        }

        pub struct ConfigureReplication {
            /// This replicaset is being (re)configured. The id is only used for logging.
            pub replicaset_name: ReplicasetName,
            /// These instances belong to one replicaset and will be sent a
            /// request to call [`rpc::replication::proc_replication`].
            pub targets: Vec<(InstanceName, RaftId)>,
            /// This instance will also become the replicaset master.
            /// This will be `None` if replicaset's current_master_name != target_master_name.
            pub master_name: Option<InstanceName>,
            /// This is an explicit list of peer addresses.
            pub replicaset_peers: Vec<SmolStr>,
            /// Global DML operation which updates `current_config_version` in table `_pico_replicaset` for the given replicaset.
            pub replication_config_version_actualize: cas::Request,
        }

        pub struct ActualizeMasterSyncIncarnation {
            /// This replicaset is being synchronized. The name is only used for logging.
            pub replicaset_name: ReplicasetName,

            /// This is the current replicaset master. It needs it's `sync_incarnation`
            /// actualized. This doesn't involve any RPCs because master is
            /// always synchronized with itself.
            pub master_name: InstanceName,

            /// Operation to actualize master's `sync_incarnation`.
            pub cas: cas::Request,
        }

        pub struct ReplicationSync {
            /// This replicaset is being synchronized. The name is only used for logging.
            pub replicaset_name: ReplicasetName,

            /// This is the current replicaset master. We will get the latest vclock from it.
            pub master_name: InstanceName,

            /// Request to call [`proc_get_vclock`] the replicaset master. This
            /// vclock is going to be persisted as `promotion_vclock` (if it's
            /// not already equal) of the given replicaset.
            ///
            /// [`proc_get_vclock`]: crate::sync::proc_get_vclock
            pub get_vclock_rpc: GetVclockRpc,

            /// These instances need to synchronize replication before becoming online.
            pub laggers: Vec<InstanceName>,

            /// Raft ids of `laggers` one for each.
            pub raft_ids: Vec<RaftId>,

            /// Global DML into `_pico_instance` to actualize `sync_incarnation`.
            /// One for each instance in `laggers`.
            pub dmls: Vec<Dml>,

            /// Request to call [`rpc::replication::proc_replication_sync`] on `laggers`.
            pub sync_rpc: rpc::replication::ReplicationSyncRequest,

            /// Operations to bump versions of replication and sharding configs.
            pub bump_dml: Vec<Dml>,
        }

        pub struct ShardingBoot<'i> {
            /// This instance will be initializing the bucket distribution.
            pub target: &'i InstanceName,
            /// Request to call [`rpc::sharding::bootstrap::proc_sharding_bootstrap`] on `target`.
            pub rpc: rpc::sharding::bootstrap::Request,
            /// Global DML operation which updates `vshard_bootstrapped` in corresponding record of table `_pico_tier`.
            pub cas: cas::Request,
            /// Tier name where vshard.bootstrap takes place.
            pub tier_name: SmolStr,
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

        pub struct ToOnline {
            /// This is all the instances that are ready to become online.
            pub targets_total: Vec<InstanceName>,
            /// This is batch of `targets_total`. Names are only used for logging.
            /// Dmls are updates for instance current states.
            pub targets_batch: Vec<(InstanceName, Dml)>,
            /// This is going to be the new current state of the instances. Only used for logging.
            pub new_current_state: &'static str,
            /// Request to call [`rpc::enable_all_plugins::proc_enable_all_plugins`] on `targets`.
            pub rpc: rpc::enable_all_plugins::Request,
            /// Update instance request which translates into a global DML operation
            /// which updates `current_state` to `Online` in table `_pico_instance` for a given instance.
            pub predicate: cas::Predicate,
        }

        pub struct ApplySchemaChange<'i> {
            /// Current value of 'global_schema_version' from _pico_property.
            /// Only used for debugging purposes.
            pub schema_version: u64,
            /// This DDL operation is being applied. Only used for logging.
            pub ddl: &'i Ddl,
            /// This will be the next schema version if the operation succeeds.
            pub pending_schema_version: u64,
            /// These are masters of all the replicasets in the cluster
            pub targets_total: Vec<InstanceName>,
            /// A batch of `targets_total` to send the `rpc` request to on this iteration.
            pub targets_batch: Vec<InstanceName>,
            /// Request to call [`rpc::ddl_apply::proc_apply_schema_change`] on `targets_batch`.
            pub rpc: rpc::ddl_apply::Request,
        }

        pub struct CommitTruncateGlobalTable {
            /// Only used for logging
            pub pending_schema_version: u64,
            /// Unconditional global DdlCommit operation.
            pub cas: cas::Request,
        }

        pub struct ApplyTruncateTable {
            /// Current value of 'global_schema_version' from _pico_property.
            /// Only used for debugging purposes.
            pub schema_version: u64,
            /// Only used for logging
            pub pending_schema_version: u64,
            /// Tier name on of the sharded table which we're truncating.
            /// Note that if that there's a separate ActionKind for TRUNCATE on
            /// global tables.
            pub tier: SmolStr,
            /// The number of masters of replicasets of the given `tier`.
            /// These master will be sent the `rpc` via the map_callrw vshard api
            /// and this value is used to check that everybody received the request.
            pub tier_masters_count: usize,
            /// These are masters of all tiers other then `tier`. These will be
            /// sent the `rpc` so that they update their schema_versions without
            /// actually truncating the table, because they don't store any data
            /// of this table.
            pub other_tiers_masters_total: Vec<InstanceName>,
            /// A batch of `other_tiers_masters_total` to send the `rpc` request to on this iteration.
            pub other_tiers_masters_batch: Vec<InstanceName>,
            /// Request to call [`rpc::ddl_apply::proc_apply_schema_change`] on `other_tier_masters_batch`.
            pub rpc: rpc::ddl_apply::Request,
            /// Global DdlCommit operation which should be applied once all
            /// targets successfully apply the TRUNCATE operation.
            pub success_cas: cas::Request,
        }

        pub struct ApplyBackup {
            /// Only used for logging
            pub pending_schema_version: u64,
            /// These are masters of all the replicasets in the cluster.
            /// They handle `rpc_master` before any `replicas_total` handle `rpc_replica`.
            pub masters_total: Vec<InstanceName>,
            /// A batch of `masters_total` to send the `rpc_master` request to on this iteration.
            pub masters_batch: Vec<InstanceName>,
            /// Request to call `proc_apply_backup` on `masters`
            pub rpc_master: rpc::ddl_backup::Request,
            /// These are read_only replicas of all the replicasets in the cluster.
            /// They handle `rpc_replica` after all `masters_total` handle `rpc_replica`.
            pub replicas_total: Vec<InstanceName>,
            /// A batch of `replicas_total` to send the `rpc_replica` request to on this iteration.
            pub replicas_batch: Vec<InstanceName>,
            /// Request to call `proc_apply_backup` on `replicas`
            pub rpc_replica: rpc::ddl_backup::Request,
            /// Request to call `proc_backup_abort_clear` on all `masters` &
            /// `replicas` in case backup operation is aborted.
            pub rpc_clear: rpc::ddl_backup::RequestClear,
        }

        pub struct CreatePlugin<'i> {
            /// This is every instance which is currently online.
            pub targets: Vec<&'i InstanceName>,
            /// Request to call [`rpc::load_plugin_dry_run::proc_load_plugin_dry_run`] on `targets`.
            pub rpc: rpc::load_plugin_dry_run::Request,
            /// Global batch DML operation which creates records in `_pico_plugin`, `_pico_service`, `_pico_plugin_config`
            /// and removes "pending_plugin_operation" from `_pico_property` in case of success.
            pub success_dml: Op,
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
        }

        pub struct InsertUpgradeOperation {
            /// DML operation for inserting upgrade operation
            /// into `_pico_governor_queue` table
            pub cas: cas::Request,
        }

        pub struct RunSqlOperationStep<'i> {
            /// ID of the governor operation to run.
            /// This is the ID of the operation in the `_pico_governor_queue` table.
            pub operation_id: u64,
            /// This is the SQL query to run.
            pub query: &'i str,
            /// DML operation to update operation status on success.
            pub cas_on_success: cas::Request,
        }

        pub struct RunProcNameOperationStep<'i> {
            /// ID of the governor operation to run.
            /// This is the ID of the operation in the `_pico_governor_queue` table.
            pub operation_id: u64,
            /// This is the procedure name for creation.
            pub proc_name: &'i str,
            /// These are masters of all the replicasets in the cluster.
            pub targets: Vec<InstanceName>,
            /// Request to call [`rpc::ddl_apply::proc_apply_schema_change`] on `targets`.
            pub rpc: rpc::ddl_apply::Request,
            /// DML operation to update operation status on success.
            pub cas_on_success: cas::Request,
        }

        pub struct RunExecScriptOperationStep<'i> {
            /// ID of the governor operation to run.
            /// This is the ID of the operation in the `_pico_governor_queue` table.
            pub operation_id: u64,
            /// This is the internal script name for executing.
            pub script_name: &'i str,
            /// These are instances in the cluster.
            pub targets: Vec<InstanceName>,
            /// Request to call [`crate::governor::upgrade_operations::proc_internal_script`] on `targets`.
            pub rpc: crate::governor::upgrade_operations::Request,
            /// DML operation to update operation status on success.
            pub cas_on_success: cas::Request,
        }

        pub struct FinishCatalogUpgrade {
            // DML operations to update state in `_pico_property`
            pub cas: cas::Request,
        }
    }

    impl SleepDueToBackoff {
        #[inline]
        pub fn new(next_try: Instant, step_kind: ActionKind) -> Self {
            Self {
                next_try,
                step_kind,
            }
        }
    }
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
/// in the given replicaset.
pub(super) fn get_replicaset_config_version_bump_op(replicaset: &Replicaset) -> Dml {
    let mut ops = UpdateOps::new();
    ops.assign(
        column_name!(Replicaset, target_config_version),
        replicaset.target_config_version + 1,
    )
    .expect("won't fail");
    let dml = Dml::update(
        storage::Replicasets::TABLE_ID,
        &[&replicaset.name],
        ops,
        ADMIN_ID,
    )
    .expect("can't fail");

    dml
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
        if replicaset.state != ReplicasetState::ToBeExpelled {
            continue;
        }

        let tier_id = &replicaset.tier;
        debug_assert_eq!(
            replicaset.current_master_name,
            replicaset.target_master_name
        );
        let master_name = &replicaset.current_master_name;
        let replicaset_name = &replicaset.name;

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

pub fn handle_instances_becoming_online<'i>(
    last_step_info: &mut LastStepInfo,
    topology_ref: &TopologyCacheRef,
    term: RaftTerm,
    applied: RaftIndex,
    sync_timeout: Duration,
    global_cluster_version: &SmolStr,
    global_catalog_version: &SmolStr,
    cluster_name: &'static str,
    cluster_uuid: &'static str,
    batch_size: usize,
) -> Result<Option<Plan<'i>>> {
    let mut targets_total = vec![];
    let mut new_current_state = None;

    //
    // Find all instances which want to become Online
    //
    for instance in topology_ref.all_instances() {
        if !(has_states!(instance, not Online -> Online) || instance.is_reincarnated()) {
            continue;
        }

        let target_state = instance.target_state;
        debug_assert_eq!(target_state.variant, StateVariant::Online);

        #[allow(clippy::unnecessary_unwrap)]
        if new_current_state.is_none() {
            new_current_state = Some(target_state.variant);
        } else if new_current_state.expect("just checked") != target_state.variant {
            continue;
        };

        targets_total.push(instance.name.clone());
    }

    // No such instances
    if targets_total.is_empty() {
        return Ok(None);
    }

    //
    // Pick a batch to send RPCs to
    //
    // NOTE: must not skip instances which were already seen as OK, because in
    // this case each successful RPC must be finalized by a CAS, which may fail
    // due to CAS conflict even after successful RPC, in which case we'll just
    // send the RPC again, no problem. After the finalizing CAS is applied
    // successfully the instance's state changes and next time around in this
    // function that instance will not be in `targets_total` anymore.
    let res = get_next_batch_for_to_online(&targets_total, last_step_info, batch_size);
    let names_batch = match res {
        Ok(v) => v,
        Err(next_try) => {
            return Ok(Some(
                SleepDueToBackoff::new(next_try, ActionKind::ToOnline).into(),
            ));
        }
    };

    debug_assert!(!names_batch.is_empty());

    let mut targets_batch = Vec::with_capacity(names_batch.len());
    let mut common_ranges = None;

    //
    // Prepare global DMLs in case of successful RPCs
    //
    for instance_name in names_batch {
        let instance = topology_ref
            .instance_by_name(&instance_name)
            .expect("instance name came from here");
        let replicaset = topology_ref
            .replicaset_by_name(&instance.replicaset_name)
            .expect("replicaset info is always present");
        let tier = topology_ref
            .tier_by_name(&instance.tier)
            .expect("tier info is always present");

        let req =
            rpc::update_instance::Request::new(instance.name.clone(), cluster_name, cluster_uuid)
                .with_current_state(instance.target_state);

        let cas_parameters = prepare_update_instance_cas_request(
            &req,
            instance,
            replicaset,
            tier,
            None,
            global_cluster_version,
            global_catalog_version,
        )?;

        let (ops, ranges) = cas_parameters.expect("already check current state is different");
        if common_ranges.is_none() {
            common_ranges = Some(ranges);
        } else {
            debug_assert_eq!(common_ranges.as_ref(), Some(&ranges));
        }

        // When changing current state only one DML operation is needed
        assert_eq!(ops.len(), 1, "{ops:?}");
        let op = ops.into_iter().next().expect("just checked");

        targets_batch.push((instance.name.clone(), op));
        if targets_batch.len() >= batch_size {
            break;
        }
    }

    // Must not be empty if targets_total wasn't empty
    debug_assert!(!targets_batch.is_empty());

    let new_current_state = new_current_state.expect("is set if targets not empty");
    let new_current_state = new_current_state.as_str();
    let common_ranges = common_ranges.expect("is set if targets not empty");

    // rpc params are wrapped into another rpc params because
    // `proc_enable_all_plugins` is considered softly deprecated
    let rpc = rpc::enable_all_plugins::Request(rpc::before_online::Request {
        term,
        applied,
        timeout: sync_timeout,
    });

    let predicate = cas::Predicate::new(applied, common_ranges.into_vec());

    return Ok(Some(
        ToOnline {
            targets_total,
            targets_batch,
            new_current_state,
            rpc,
            predicate,
        }
        .into(),
    ));
}

#[inline(always)]
pub fn maybe_responding(instances: &[Instance]) -> impl Iterator<Item = &Instance> {
    instances.iter().filter(|instance| instance.may_respond())
}
