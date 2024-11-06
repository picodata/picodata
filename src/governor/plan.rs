use super::conf_change::raft_conf_change;
use crate::cas;
use crate::column_name;
use crate::has_states;
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
use crate::storage::{ClusterwideTable, PropertyName};
use crate::tier::Tier;
use crate::tlog;
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
    tiers: &HashMap<&str, &Tier>,
    my_raft_id: RaftId,
    has_pending_schema_change: bool,
    plugins: &HashMap<PluginIdentifier, PluginDef>,
    services: &HashMap<PluginIdentifier, Vec<&'i ServiceDef>>,
    plugin_op: Option<&'i PluginOp>,
    sync_timeout: std::time::Duration,
) -> Result<Plan<'i>> {
    // This function is specifically extracted, to separate the task
    // construction from any IO and/or other yielding operations.
    #[cfg(debug_assertions)]
    let _guard = crate::util::NoYieldsGuard::new();

    ////////////////////////////////////////////////////////////////////////////
    // conf change
    if let Some(conf_change) = raft_conf_change(instances, voters, learners, tiers) {
        return Ok(ConfChange { conf_change }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // instance going offline non-gracefully
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

        ////////////////////////////////////////////////////////////////////////
        // transfer leadership, if we're the one who goes offline
        if instance.raft_id == my_raft_id {
            let new_leader = maybe_responding(instances).find(|i| voters.contains(&i.raft_id));
            if let Some(new_leader) = new_leader {
                return Ok(TransferLeadership { to: new_leader }.into());
            } else {
                tlog!(Warning, "leader is going offline and no substitution is found";
                    "leader_raft_id" => my_raft_id,
                    "voters" => ?voters,
                );
            }
        }

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

        ////////////////////////////////////////////////////////////////////////
        // update instance's current state
        let req = rpc::update_instance::Request::new(instance_name.clone(), cluster_name)
            .with_current_state(instance.target_state);
        let cas_parameters =
            prepare_update_instance_cas_request(&req, instance, replicaset, tier, existing_fds)?;

        let (op, ranges) = cas_parameters.expect("already check current state is different");
        let predicate = cas::Predicate::new(applied, ranges);
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
            ClusterwideTable::Replicaset,
            &[&to.replicaset_name],
            ops,
            ADMIN_ID,
        )?;
        let ranges = vec![
            cas::Range::for_dml(&dml)?,
            cas::Range::new(ClusterwideTable::Instance).eq([&to.name]),
            cas::Range::new(ClusterwideTable::Instance).eq([&replicaset.target_master_name]),
        ];
        let predicate = cas::Predicate::new(applied, ranges);
        let cas = cas::Request::new(dml, predicate, ADMIN_ID)?;
        return Ok(UpdateTargetReplicasetMaster { cas }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // configure replication
    let replicaset_to_configure = replicasets
        .values()
        .find(|replicaset| replicaset.current_config_version != replicaset.target_config_version);
    if let Some(replicaset) = replicaset_to_configure {
        let replicaset_name = &replicaset.name;
        let mut targets = Vec::new();
        let mut replicaset_peers = Vec::new();
        let mut master_is_added = false;
        for instance in instances {
            if instance.replicaset_name != replicaset_name {
                continue;
            }
            if let Some(address) = peer_addresses.get(&instance.raft_id) {
                replicaset_peers.push(address.clone());
            } else {
                warn_or_panic!("replica `{}` address unknown, will be excluded from box.cfg.replication of replicaset `{replicaset_name}`", instance.name);
            }
            if instance.may_respond() {
                targets.push(&instance.name);
                master_is_added |= instance.name == replicaset.current_master_name;
            }
        }

        let mut master_name = None;
        if replicaset.current_master_name == replicaset.target_master_name {
            master_name = Some(&replicaset.current_master_name);

            // If master is already chosen (current == target) but is not Online
            // we send it an RPC request to the master anyway, otherwise an old
            // master being expelled while other replicas are offline would lead
            // to loss of data.
            // NOTE that this is a hack stemming from the fact that we switch
            // current replicaset master before it is synchronized with the old
            // master, as result at the moment it synchronizes there's no
            // information about who it synchronizes with in the global state.
            if !master_is_added {
                targets.push(&replicaset.current_master_name);
            }
        }

        let mut ops = UpdateOps::new();
        ops.assign(
            column_name!(Replicaset, current_config_version),
            replicaset.target_config_version,
        )?;
        let dml = Dml::update(
            ClusterwideTable::Replicaset,
            &[replicaset_name],
            ops,
            ADMIN_ID,
        )?;
        let ranges = vec![cas::Range::for_dml(&dml)?];
        let predicate = cas::Predicate::new(applied, ranges);
        let cas = cas::Request::new(dml, predicate, ADMIN_ID)?;
        let replication_config_version_actualize = cas;

        let promotion_vclock = &replicaset.promotion_vclock;
        return Ok(ConfigureReplication {
            replicaset_name,
            targets,
            master_name,
            replicaset_peers,
            promotion_vclock,
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

        let mut update_ops = UpdateOps::new();
        update_ops.assign(
            column_name!(Replicaset, current_master_name),
            new_master_name,
        )?;

        let mut demote = None;
        let old_master_may_respond = instances
            .iter()
            .find(|i| i.name == old_master_name)
            .map(|i| i.may_respond());
        if let Some(true) = old_master_may_respond {
            demote = Some(rpc::replication::DemoteRequest {});
        }

        let mut ops = vec![];

        if let Some(bump) =
            get_replicaset_config_version_bump_op_if_needed(replicasets, replicaset_name)
        {
            ops.push(bump);
        }

        let tier_name = &r.tier;
        let tier = tiers
            .get(tier_name.as_str())
            .expect("tier for instance should exists");

        let vshard_config_version_bump = Tier::get_vshard_config_version_bump_op_if_needed(tier)?;
        if let Some(bump) = vshard_config_version_bump {
            ops.push(bump);
        }

        return Ok(UpdateCurrentReplicasetMaster {
            old_master_name,
            demote,
            new_master_name,
            replicaset_name,
            update_ops,
            // Implicit ranges are sufficient
            bump_ranges: vec![],
            bump_ops: ops,
        }
        .into());
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
            ClusterwideTable::Replicaset,
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
            let targets = maybe_responding(instances)
                // Note at this point all the instances should have their replication configured,
                // so it's ok to configure sharding for them
                .filter(|instance| has_states!(instance, * -> Online))
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

            let bump = Dml::update(ClusterwideTable::Tier, &[tier_name], uops, ADMIN_ID)?;

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

            let dml = Dml::update(ClusterwideTable::Tier, &[tier_name], uops, ADMIN_ID)?;

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
        let cas_parameters =
            prepare_update_instance_cas_request(&req, instance, replicaset, tier, existing_fds)?;

        let (op, ranges) = cas_parameters.expect("already check current state is different");
        let predicate = cas::Predicate::new(applied, ranges);
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
        let cas_parameters =
            prepare_update_instance_cas_request(&req, instance, replicaset, tier, existing_fds)?;

        let (op, ranges) = cas_parameters.expect("already check current state is different");
        let predicate = cas::Predicate::new(applied, ranges);
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

        let dml = Dml::replace(ClusterwideTable::Plugin, &plugin_def, ADMIN_ID)?;
        ranges.push(cas::Range::for_dml(&dml)?);
        ops.push(dml);

        let ident = plugin_def.into_identifier();
        for mut service_def in manifest.service_defs() {
            if let Some(service_topology) = inherit_topology.get(&service_def.name) {
                service_def.tiers = service_topology.clone();
            }
            let dml = Dml::replace(ClusterwideTable::Service, &service_def, ADMIN_ID)?;
            ranges.push(cas::Range::for_dml(&dml)?);
            ops.push(dml);

            let config = manifest
                .get_default_config(&service_def.name)
                .expect("configuration should exist");
            let config_records =
                PluginConfigRecord::from_config(&ident, &service_def.name, config.clone())?;

            for config_rec in config_records {
                let dml = Dml::replace(ClusterwideTable::PluginConfig, &config_rec, ADMIN_ID)?;
                ranges.push(cas::Range::for_dml(&dml)?);
                ops.push(dml);
            }
        }

        let dml = Dml::delete(
            ClusterwideTable::Property,
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
            ClusterwideTable::Plugin,
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
                    ClusterwideTable::ServiceRouteTable,
                    &ServiceRouteItem::new_healthy(i.name.clone(), plugin, &svc.name),
                    ADMIN_ID,
                )?;
                ranges.push(cas::Range::for_dml(&dml)?);
                success_dml.push(dml);
            }
        }

        let dml = Dml::delete(
            ClusterwideTable::Property,
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
                        ClusterwideTable::ServiceRouteTable,
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
                    let dml = Dml::delete(ClusterwideTable::ServiceRouteTable, &key, ADMIN_ID)?;
                    ranges.push(cas::Range::for_dml(&dml)?);
                    on_success_dml.push(dml);
                }
            }
        }

        let dml = Dml::replace(ClusterwideTable::Service, &new_service_def, ADMIN_ID)?;
        ranges.push(cas::Range::for_dml(&dml)?);
        on_success_dml.push(dml);

        let dml = Dml::delete(
            ClusterwideTable::Property,
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

        pub struct UpdateCurrentReplicasetMaster<'i> {
            /// This replicaset is changing it's master.
            pub replicaset_name: &'i ReplicasetName,
            /// This instance will be demoted.
            pub old_master_name: &'i InstanceName,
            /// Request to call [`rpc::replication::proc_replication_demote`] on old master.
            /// It is optional because we don't try demoting the old master if it's already offline.
            pub demote: Option<rpc::replication::DemoteRequest>,
            /// This is the new master. It will be sent a RPC [`proc_get_vclock`] to set the
            /// promotion vclock in case the old master is not available.
            ///
            /// [`proc_get_vclock`]: crate::sync::proc_get_vclock
            pub new_master_name: &'i InstanceName,
            /// Part of the global DML operation which updates a `_pico_replicaset` record
            /// with the new values for `current_master_name` & `promotion_vclock`.
            /// Note: it is only the part of the operation, because we don't know the promotion_vclock yet.
            pub update_ops: UpdateOps,
            /// Cas ranges for the `bump_ops` operations.
            pub bump_ranges: Vec<cas::Range>,
            /// Optional operations to bump versions of replication and sharding configs.
            pub bump_ops: Vec<Dml>,
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
            /// The value of `promotion_vclock` column of the given replicaset.
            /// It's used to synchronize new master before making it writable.
            pub promotion_vclock: &'i Vclock,
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
            tlog!(Info, "target master {} of replicaset {} is not online: trying to choose a new one",
                  master.name, master.replicaset_name);
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
        tlog!(Warning, "there are no instances suitable as master of replicaset {}", r.replicaset_id);
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
    for Instance {
        replicaset_name,
        tier,
        ..
    } in maybe_responding(instances)
    {
        let replicaset_size = replicaset_sizes.entry(replicaset_name).or_insert(0);
        *replicaset_size += 1;
        let Some(r) = replicasets.get(replicaset_name) else {
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
        if *replicaset_size < tier_info.replication_factor {
            continue;
        }
        let need_to_update_weight = r.weight_origin != WeightOrigin::User;
        return Some((replicaset_name, tier_info, need_to_update_weight));
    }
    None
}

#[inline(always)]
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
        ClusterwideTable::Replicaset,
        &[replicaset_name],
        ops,
        ADMIN_ID,
    )
    .expect("can't fail");
    Some(dml)
}

#[inline(always)]
fn maybe_responding(instances: &[Instance]) -> impl Iterator<Item = &Instance> {
    instances.iter().filter(|instance| instance.may_respond())
}
