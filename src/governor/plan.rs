use crate::instance::state::State;
use crate::instance::state::StateVariant::*;
use crate::instance::{Instance, InstanceId};
use crate::replicaset::ReplicasetState;
use crate::replicaset::WeightOrigin;
use crate::replicaset::{Replicaset, ReplicasetId};
use crate::rpc;
use crate::schema::{PluginDef, ServiceDef, ServiceRouteItem, ServiceRouteKey, ADMIN_ID};
use crate::storage::{ClusterwideTable, PropertyName};
use crate::tier::Tier;
use crate::tlog;
use crate::traft::op::{Dml, Op};
use crate::traft::Result;
use crate::traft::{RaftId, RaftIndex, RaftTerm};
use crate::vshard::VshardConfig;
use crate::{has_states, plugin};
use ::tarantool::space::UpdateOps;
use std::collections::HashMap;
use std::mem;
use std::time::Duration;

use super::cc::raft_conf_change;
use super::Loop;

pub(super) struct EnablePluginConfig {
    pub ident: PluginIdentifier,
    pub installed_plugins: Vec<PluginDef>,
    pub services: Vec<ServiceDef>,
    pub applied_migrations: Vec<String>,
    pub timeout: Duration,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn action_plan<'i>(
    term: RaftTerm,
    applied: RaftIndex,
    cluster_id: String,
    instances: &'i [Instance],
    peer_addresses: &'i HashMap<RaftId, String>,
    voters: &[RaftId],
    learners: &[RaftId],
    replicasets: &HashMap<&ReplicasetId, &'i Replicaset>,
    tiers: &HashMap<&str, &Tier>,
    my_raft_id: RaftId,
    current_vshard_config_version: u64,
    target_vshard_config_version: u64,
    vshard_bootstrapped: bool,
    has_pending_schema_change: bool,
    install_plugin: Option<&'i (Option<PluginDef>, plugin::Manifest)>,
    enable_plugin: Option<&'i EnablePluginConfig>,
    disable_plugin: Option<&'i [ServiceRouteItem]>,
    update_plugin_topology: Option<(PluginDef, ServiceDef, TopologyUpdateOp)>,
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

    // TODO: reduce number of iterations over all instances

    ////////////////////////////////////////////////////////////////////////////
    // downgrading
    let to_downgrade = instances
        .iter()
        // TODO: process them all, not just the first one
        .find(|instance| {
            has_states!(instance, not Offline -> Offline)
                || has_states!(instance, not Expelled -> Expelled)
        });
    if let Some(Instance {
        raft_id,
        instance_id,
        target_state,
        ..
    }) = to_downgrade
    {
        ////////////////////////////////////////////////////////////////////////
        // transfer leadership, if we're the one who goes offline
        if *raft_id == my_raft_id {
            let new_leader = maybe_responding(instances)
                // FIXME: linear search
                .find(|instance| voters.contains(&instance.raft_id));
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
        let req = rpc::update_instance::Request::new(instance_id.clone(), cluster_id)
            .with_current_state(*target_state);

        let mut vshard_config_version_bump = None;
        #[rustfmt::skip]
        if target_vshard_config_version == current_vshard_config_version {
            // Only bump the version if it's not already bumped.
            vshard_config_version_bump = Some(Dml::replace(
                    ClusterwideTable::Property,
                    &(&PropertyName::TargetVshardConfigVersion, target_vshard_config_version + 1),
                    ADMIN_ID,
            )?);
        };

        return Ok(Downgrade {
            req,
            vshard_config_version_bump,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // update target replicaset master
    let new_target_master = get_new_replicaset_master_if_needed(instances, replicasets);
    if let Some(to) = new_target_master {
        let mut ops = UpdateOps::new();
        ops.assign("target_master_id", &to.instance_id)?;
        let op = Dml::update(
            ClusterwideTable::Replicaset,
            &[&to.replicaset_id],
            ops,
            ADMIN_ID,
        )?;
        return Ok(UpdateTargetReplicasetMaster { op }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // replication
    let to_replicate = instances
        .iter()
        // TODO: find all such instances in a given replicaset,
        // not just the first one
        .find(|instance| has_states!(instance, Offline -> Online) || instance.is_reincarnated());
    if let Some(Instance {
        instance_id,
        replicaset_id,
        target_state,
        ..
    }) = to_replicate
    {
        let mut targets = Vec::new();
        let mut replicaset_peers = Vec::new();
        for instance in instances {
            if instance.replicaset_id != replicaset_id {
                continue;
            }
            if let Some(address) = peer_addresses.get(&instance.raft_id) {
                replicaset_peers.push(address.clone());
            } else {
                tlog!(Warning, "replica {} address unknown, will be excluded from box.cfg.replication", instance.instance_id;
                    "replicaset_id" => %replicaset_id,
                );
            }
            if instance.may_respond() {
                targets.push(&instance.instance_id);
            }
        }
        let replicaset = replicasets
            .get(replicaset_id)
            .expect("replicaset info should be available at this point");
        let master_id = &replicaset.current_master_id;
        let req = rpc::update_instance::Request::new(instance_id.clone(), cluster_id)
            .with_current_state(State::new(Replicated, target_state.incarnation));

        let mut vshard_config_version_bump = None;
        #[rustfmt::skip]
        if target_vshard_config_version == current_vshard_config_version {
            // Only bump the version if it's not already bumped.
            vshard_config_version_bump = Some(Dml::replace(
                    ClusterwideTable::Property,
                    &(&PropertyName::TargetVshardConfigVersion, target_vshard_config_version + 1),
                    ADMIN_ID,
            )?);
        };

        return Ok(Replication {
            targets,
            master_id,
            replicaset_peers,
            req,
            vshard_config_version_bump,
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
        .find(|r| r.current_master_id != r.target_master_id);
    if let Some(r) = new_current_master {
        let old_master_id = &r.current_master_id;
        let new_master_id = &r.target_master_id;

        let mut ops = UpdateOps::new();
        ops.assign("current_master_id", new_master_id)?;
        let op = Dml::update(
            ClusterwideTable::Replicaset,
            &[&r.replicaset_id],
            ops,
            ADMIN_ID,
        )?;

        let mut demote = None;
        let old_master_may_respond = instances
            .iter()
            .find(|i| i.instance_id == old_master_id)
            .map(|i| i.may_respond());
        if let Some(true) = old_master_may_respond {
            demote = Some(rpc::replication::DemoteRequest {});
        }

        let sync_and_promote = rpc::replication::SyncAndPromoteRequest {
            // This will be set to the value returned by old master.
            vclock: None,
            timeout: Loop::SYNC_TIMEOUT,
        };

        let mut vshard_config_version_bump = None;
        #[rustfmt::skip]
        if target_vshard_config_version == current_vshard_config_version {
            // Only bump the version if it's not already bumped.
            vshard_config_version_bump = Some(Dml::replace(
                    ClusterwideTable::Property,
                    &(&PropertyName::TargetVshardConfigVersion, target_vshard_config_version + 1),
                    ADMIN_ID,
            )?);
        };

        let replicaset_id = &r.replicaset_id;
        return Ok(UpdateCurrentReplicasetMaster {
            old_master_id,
            demote,
            new_master_id,
            sync_and_promote,
            replicaset_id,
            op,
            vshard_config_version_bump,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // proposing automatic replicaset state & weight change
    let to_change_weights = get_replicaset_state_change(instances, replicasets, tiers);
    if let Some((replicaset_id, need_to_update_weight)) = to_change_weights {
        let mut uops = UpdateOps::new();
        if need_to_update_weight {
            uops.assign("weight", 1.)?;
        }
        uops.assign("state", ReplicasetState::Ready)?;
        let op = Dml::update(
            ClusterwideTable::Replicaset,
            &[replicaset_id],
            uops,
            ADMIN_ID,
        )?;

        let mut vshard_config_version_bump = None;
        #[rustfmt::skip]
        if target_vshard_config_version == current_vshard_config_version {
            // Only bump the version if it's not already bumped.
            vshard_config_version_bump = Some(Dml::replace(
                    ClusterwideTable::Property,
                    &(&PropertyName::TargetVshardConfigVersion, target_vshard_config_version + 1),
                    ADMIN_ID,
            )?);
        };

        return Ok(ProposeReplicasetStateChanges {
            op,
            vshard_config_version_bump,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // update current vshard config
    let mut first_ready_replicaset = None;
    if !vshard_bootstrapped {
        first_ready_replicaset = get_first_ready_replicaset(instances, replicasets);
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
    let ok_to_configure_vshard = vshard_bootstrapped || first_ready_replicaset.is_some();

    if ok_to_configure_vshard && current_vshard_config_version != target_vshard_config_version {
        let vshard_config = VshardConfig::new(instances, peer_addresses, replicasets);
        let targets = maybe_responding(instances)
            .filter(|instance| instance.current_state.variant >= Replicated)
            .map(|instance| &instance.instance_id)
            .collect();
        let rpc = rpc::sharding::Request {
            term,
            applied,
            timeout: Loop::SYNC_TIMEOUT,
            do_reconfigure: true,
        };

        // Note: currently the "current_vshard_config" is only stored in "_pico_property" for debugging purposes,
        // nobody actually uses it directly to configure vshard, instead the actual config is generated
        // based on the contents of "_pico_instance", "_pico_replicaset" & "_pico_peer_address" tables.
        let dml = Dml::replace(
            ClusterwideTable::Property,
            // FIXME: encode as map
            &(&PropertyName::CurrentVshardConfig, vshard_config),
            ADMIN_ID,
        )?;

        #[rustfmt::skip]
        let vshard_config_version_actualize = Dml::replace(
            ClusterwideTable::Property,
            &(&PropertyName::CurrentVshardConfigVersion, target_vshard_config_version),
            ADMIN_ID,
        )?;

        return Ok(UpdateCurrentVshardConfig {
            targets,
            rpc,
            dml,
            vshard_config_version_actualize,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // bootstrap sharding
    if let Some(r) = first_ready_replicaset {
        debug_assert!(
            !vshard_bootstrapped,
            "bucket distribution only needs to be bootstrapped once"
        );

        let target = &r.current_master_id;
        let rpc = rpc::sharding::bootstrap::Request {
            term,
            applied,
            timeout: Loop::SYNC_TIMEOUT,
        };
        let op = Dml::replace(
            ClusterwideTable::Property,
            &(PropertyName::VshardBootstrapped, true),
            ADMIN_ID,
        )?;
        return Ok(ShardingBoot { target, rpc, op }.into());
    };

    ////////////////////////////////////////////////////////////////////////////
    // to online
    let to_online = instances
        .iter()
        .find(|instance| has_states!(instance, Replicated -> Online));
    if let Some(Instance {
        instance_id,
        target_state,
        ..
    }) = to_online
    {
        let target = instance_id;
        let mut rpc = None;
        if vshard_bootstrapped {
            rpc = Some(rpc::sharding::Request {
                term,
                applied,
                timeout: Loop::SYNC_TIMEOUT,
                do_reconfigure: false,
            });
        }
        let plugin_rpc = rpc::enable_all_plugins::Request {
            term,
            applied,
            timeout: Loop::SYNC_TIMEOUT,
        };
        let req = rpc::update_instance::Request::new(instance_id.clone(), cluster_id)
            .with_current_state(State::new(Online, target_state.incarnation));
        return Ok(ToOnline {
            target,
            rpc,
            plugin_rpc,
            req,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // ddl
    if has_pending_schema_change {
        let mut targets = Vec::with_capacity(replicasets.len());
        // TODO: invert this loop to improve performance
        // `for instances { replicasets.get() }` instead of `for replicasets { instances.find() }`
        for r in replicasets.values() {
            #[rustfmt::skip]
            let Some(master) = instances.iter().find(|i| i.instance_id == r.current_master_id) else {
                tlog!(
                    Warning,
                    "couldn't find instance with id {}, which is chosen as master of replicaset {}",
                    r.current_master_id,
                    r.replicaset_id,
                );
                // Send them a request anyway just to be safe
                targets.push(&r.current_master_id);
                continue;
            };
            if has_states!(master, Expelled -> *) {
                continue;
            }
            targets.push(&master.instance_id);
        }

        let rpc = rpc::ddl_apply::Request {
            term,
            applied,
            timeout: Loop::SYNC_TIMEOUT,
        };
        return Ok(ApplySchemaChange { rpc, targets }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // install plugin
    if let Some((maybe_existing_plugin, manifest)) = install_plugin {
        // if plugin with a same version already exists and already enabled - do nothing
        let should_install = maybe_existing_plugin
            .as_ref()
            .map(|p| !p.enabled)
            .unwrap_or(true);

        let install_substep = should_install
            .then(|| -> tarantool::Result<_> {
                let rpc = rpc::load_plugin_dry_run::Request {
                    term,
                    applied,
                    timeout: Loop::SYNC_TIMEOUT,
                };

                let mut targets = Vec::with_capacity(instances.len());
                for i in instances {
                    if i.may_respond() {
                        targets.push(&i.instance_id);
                    }
                }
                let plugin_def = manifest.plugin_def();
                let mut ops = vec![Dml::replace(
                    ClusterwideTable::Plugin,
                    &plugin_def,
                    ADMIN_ID,
                )?];

                for service_def in manifest.service_defs() {
                    ops.push(Dml::replace(
                        ClusterwideTable::Service,
                        &service_def,
                        ADMIN_ID,
                    )?)
                }

                Ok((targets, rpc, Op::BatchDml { ops }))
            })
            .transpose()?;

        return Ok(InstallPlugin {
            install_substep,
            finalize_op: Op::Dml(Dml::delete(
                ClusterwideTable::Property,
                &[PropertyName::PluginInstall],
                ADMIN_ID,
            )?),
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // enable plugin
    if let Some(EnablePluginConfig {
        ident,
        installed_plugins,
        services,
        applied_migrations,
        timeout: on_start_timeout,
    }) = enable_plugin
    {
        let rpc = rpc::enable_plugin::Request {
            term,
            applied,
            timeout: Loop::SYNC_TIMEOUT,
        };
        let rollback_op = Op::PluginDisable {
            ident: ident.clone(),
        };

        let mut targets = vec![];
        let mut success_dml = vec![];

        let already_enabled_plugin = installed_plugins.iter().find(|p| p.enabled);
        let same_version_plugin = installed_plugins
            .iter()
            .find(|p| p.version == ident.version);

        fn is_subset(source: &[String], needle: &[String]) -> bool {
            if needle.len() > source.len() {
                return false;
            }
            for i in 0..needle.len() {
                if source[i] != needle[i] {
                    return false;
                }
            }
            return true;
        }

        match (already_enabled_plugin, same_version_plugin) {
            (Some(already_enabled_plugin), Some(same_version_plugin))
                if already_enabled_plugin.version == same_version_plugin.version =>
            {
                // plugin already exists - do nothing
            }
            (Some(_already_enabled_plugin), _) => {
                // already enabled plugin with a different version
                tlog!(Error, "Trying to enable plugin but different version of the same plugin already enabled");
            }
            (_, Some(same_version_plugin))
                if !is_subset(applied_migrations, &same_version_plugin.migration_list) =>
            {
                // migration is partially applied - do nothing
                tlog!(Error, "Trying to enable a non-fully installed plugin (migration is partially applied)");
            }
            (_, None) => {
                // plugin isn't installed
                tlog!(Error, "Trying to enable a non-installed plugin");
            }
            (_, Some(plugin)) => {
                targets = Vec::with_capacity(instances.len());
                for i in instances {
                    if i.may_respond() {
                        targets.push(&i.instance_id);
                    }
                }

                let mut enable_ops = UpdateOps::new();
                enable_ops.assign(PluginDef::FIELD_ENABLE, true)?;

                success_dml = vec![Dml::update(
                    ClusterwideTable::Plugin,
                    &[&plugin.name, &plugin.version],
                    enable_ops,
                    ADMIN_ID,
                )?];

                for i in instances {
                    let topology_ctx = TopologyContext::for_instance(i);

                    let active_services = services
                        .iter()
                        .filter(|svc| topology::probe_service(&topology_ctx, svc));

                    for svc in active_services {
                        success_dml.push(Dml::replace(
                            ClusterwideTable::ServiceRouteTable,
                            &ServiceRouteItem::new_healthy(i.instance_id.clone(), ident, &svc.name),
                            ADMIN_ID,
                        )?);
                    }
                }
            }
        }

        return Ok(EnablePlugin {
            rpc,
            targets,
            on_start_timeout: *on_start_timeout,
            rollback_op,
            success_dml,
            finalize_dml: Dml::delete(
                ClusterwideTable::Property,
                &[PropertyName::PendingPluginEnable],
                ADMIN_ID,
            )?,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // disable plugin
    if let Some(routes) = disable_plugin {
        let routing_keys_to_del = routes.iter().map(|route| route.key()).collect::<Vec<_>>();
        let mut ops: Vec<_> = routing_keys_to_del
            .iter()
            .map(|routing_key| {
                Dml::delete(ClusterwideTable::ServiceRouteTable, &routing_key, ADMIN_ID)
                    .expect("encoding should not fail")
            })
            .collect();
        ops.push(Dml::delete(
            ClusterwideTable::Property,
            &[PropertyName::PendingPluginDisable],
            ADMIN_ID,
        )?);

        let op = Op::BatchDml { ops };
        return Ok(DisablePlugin { op }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // update plugin topology
    if let Some((plugin_def, mut service_def, op)) = update_plugin_topology {
        let mut enable_targets = Vec::with_capacity(instances.len());
        let mut disable_targets = Vec::with_capacity(instances.len());
        let mut on_success_dml = vec![];

        let mut new_tiers = service_def.tiers.clone();
        if matches!(op, TopologyUpdateOp::Append { .. }) {
            if new_tiers.iter().all(|t| t != op.tier()) {
                new_tiers.push(op.tier().to_string());
            }
        } else if new_tiers.iter().any(|t| t == op.tier()) {
            new_tiers.retain(|t| t != op.tier());
        }

        let old_tiers = mem::replace(&mut service_def.tiers, new_tiers);
        let new_tiers = &service_def.tiers;

        // note: no need to enable/disable service and update routing table if plugin disabled
        if plugin_def.enabled {
            let plugin_ident = plugin_def.into_identity();
            for i in instances {
                if !i.may_respond() {
                    continue;
                }

                // if instance in both new and old tiers - do nothing
                if new_tiers.contains(&i.tier) && old_tiers.contains(&i.tier) {
                    continue;
                }

                if new_tiers.contains(&i.tier) {
                    enable_targets.push(&i.instance_id);
                    on_success_dml.push(Dml::replace(
                        ClusterwideTable::ServiceRouteTable,
                        &ServiceRouteItem::new_healthy(
                            i.instance_id.clone(),
                            &plugin_ident,
                            &service_def.name,
                        ),
                        ADMIN_ID,
                    )?);
                }

                if old_tiers.contains(&i.tier) {
                    disable_targets.push(&i.instance_id);
                    let key = ServiceRouteKey {
                        instance_id: &i.instance_id,
                        plugin_name: &plugin_ident.name,
                        plugin_version: &plugin_ident.version,
                        service_name: &service_def.name,
                    };
                    on_success_dml.push(Dml::delete(
                        ClusterwideTable::ServiceRouteTable,
                        &key,
                        ADMIN_ID,
                    )?);
                }
            }
        }

        on_success_dml.push(Dml::replace(
            ClusterwideTable::Service,
            &service_def,
            ADMIN_ID,
        )?);

        let enable_rpc = rpc::enable_service::Request {
            term,
            applied,
            timeout: Loop::SYNC_TIMEOUT,
        };
        let disable_rpc = rpc::disable_service::Request {
            term,
            applied,
            timeout: Loop::SYNC_TIMEOUT,
        };

        return Ok(UpdatePluginTopology {
            enable_targets,
            disable_targets,
            enable_rpc,
            disable_rpc,
            success_dml: on_success_dml,
            finalize_dml: Dml::delete(
                ClusterwideTable::Property,
                &[PropertyName::PendingPluginTopologyUpdate],
                ADMIN_ID,
            )?,
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

use crate::plugin::topology::TopologyContext;
use crate::plugin::{topology, PluginIdentifier, TopologyUpdateOp};
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
            pub targets: Vec<&'i InstanceId>,
            /// Request to call [`rpc::sharding::proc_sharding`] on `targets`.
            pub rpc: rpc::sharding::Request,
            /// Global DML operation which updates `current_vshard_config` in table `_pico_property`.
            pub dml: Dml,
            /// Global DML operation which updates `current_vshard_config_version` in table `_pico_property`.
            pub vshard_config_version_actualize: Dml,
        }

        pub struct TransferLeadership<'i> {
            /// This instance should be nominated as next raft leader.
            pub to: &'i Instance,
        }

        pub struct UpdateTargetReplicasetMaster {
            /// Global DML operation which updates `target_master_id` in table `_pico_replicaset`.
            pub op: Dml,
        }

        pub struct UpdateCurrentReplicasetMaster<'i> {
            /// This replicaset is changing it's master.
            pub replicaset_id: &'i ReplicasetId,
            /// This instance will be demoted.
            pub old_master_id: &'i InstanceId,
            /// Request to call [`rpc::replication::proc_replication_demote`] on old master.
            /// It is optional because we don't try demoting the old master if it's already offline.
            pub demote: Option<rpc::replication::DemoteRequest>,
            /// This instance will be promoted.
            pub new_master_id: &'i InstanceId,
            /// Request to call [`rpc::replication::proc_replication`] on new master.
            pub sync_and_promote: rpc::replication::SyncAndPromoteRequest,
            /// Global DML operation which updates `current_master_id` in table `_pico_replicaset`.
            pub op: Dml,
            /// Global DML operation which updates `target_vshard_config_version` in table `_pico_property`.
            pub vshard_config_version_bump: Option<Dml>,
        }

        pub struct Downgrade {
            /// Update instance request which translates into a global DML operation
            /// which updates `current_state` to `Offline` in table `_pico_instance` for a given instance.
            pub req: rpc::update_instance::Request,
            /// Global DML operation which updates `target_vshard_config_version` in table `_pico_property`.
            pub vshard_config_version_bump: Option<Dml>,
        }

        pub struct Replication<'i> {
            /// These instances belong to one replicaset and will be sent a
            /// request to call [`rpc::replication::proc_replication`].
            pub targets: Vec<&'i InstanceId>,
            /// This instance will also become the replicaset master.
            pub master_id: &'i InstanceId,
            /// This is an explicit list of peer addresses (one for each target).
            pub replicaset_peers: Vec<String>,
            /// Update instance request which translates into a global DML operation
            /// which updates `current_state` to `Replicated` in table `_pico_instance` for a given instance.
            pub req: rpc::update_instance::Request,
            /// Global DML operation which updates `target_vshard_config_version` in table `_pico_property`.
            pub vshard_config_version_bump: Option<Dml>,
        }

        pub struct ShardingBoot<'i> {
            /// This instance will be initializing the bucket distribution.
            pub target: &'i InstanceId,
            /// Request to call [`rpc::sharding::bootstrap::proc_sharding_bootstrap`] on `target`.
            pub rpc: rpc::sharding::bootstrap::Request,
            /// Global DML operation which updates value for `vshard_bootstrapped` to `true` in table `_pico_property`.
            pub op: Dml,
        }

        pub struct ProposeReplicasetStateChanges {
            /// Global DML operation which updates `weight` to `1` & `state` to `ready`
            /// in table `_pico_replicaset` for given replicaset.
            pub op: Dml,
            /// Global DML operation which updates `target_vshard_config_version` in table `_pico_property`.
            pub vshard_config_version_bump: Option<Dml>,
        }

        pub struct ToOnline<'i> {
            pub target: &'i InstanceId,
            /// Request to call [`rpc::sharding::proc_sharding`] on `target`.
            /// It is optional, because we don't do this RPC if `vshard_bootstrapped` is `false`.
            pub rpc: Option<rpc::sharding::Request>,
            /// Request to call [`rpc::enable_all_plugins::proc_enable_all_plugins`] on `target`.
            /// It is not optional, although it probably should be.
            pub plugin_rpc: rpc::enable_all_plugins::Request,
            /// Update instance request which translates into a global DML operation
            /// which updates `current_state` to `Online` in table `_pico_instance` for a given instance.
            pub req: rpc::update_instance::Request,
        }

        pub struct ApplySchemaChange<'i> {
            /// These are masters of all the replicasets in the cluster.
            pub targets: Vec<&'i InstanceId>,
            /// Request to call [`rpc::ddl_apply::proc_apply_schema_change`] on `targets`.
            pub rpc: rpc::ddl_apply::Request,
        }

        pub struct InstallPlugin<'i> {
            pub install_substep: Option<(Vec<&'i InstanceId>, rpc::load_plugin_dry_run::Request, Op)>,
            pub finalize_op: Op,
        }

        pub struct EnablePlugin<'i> {
            pub targets: Vec<&'i InstanceId>,
            pub rpc: rpc::enable_plugin::Request,
            pub rollback_op: Op,
            pub on_start_timeout: Duration,
            pub success_dml: Vec<Dml>,
            pub finalize_dml: Dml,
        }

        pub struct DisablePlugin {
            pub op: Op,
        }

        pub struct UpdatePluginTopology<'i> {
            pub enable_targets: Vec<&'i InstanceId>,
            pub disable_targets: Vec<&'i InstanceId>,
            pub enable_rpc: rpc::enable_service::Request,
            pub disable_rpc: rpc::disable_service::Request,
            pub success_dml: Vec<Dml>,
            pub finalize_dml: Dml,
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
    replicasets: &HashMap<&ReplicasetId, &Replicaset>,
) -> Option<&'i Instance> {
    // TODO: construct a map from replicaset id to instance to improve performance
    for r in replicasets.values() {
        // XXX:                                        is this correct? vvvvvv
        #[rustfmt::skip]
        let Some(master) = instances.iter().find(|i| i.instance_id == r.target_master_id) else {
            crate::warn_or_panic!(
                "couldn't find instance with id {}, which is chosen as next master of replicaset {}",
                r.target_master_id,
                r.replicaset_id,
            );
            continue;
        };

        if master.replicaset_id != r.replicaset_id {
            tlog!(
                Warning,
                "target master {} of replicaset {} is from different a replicaset {}: trying to choose a new one",
                master.instance_id,
                master.replicaset_id,
                r.replicaset_id,
            );
        } else if !master.may_respond() {
            tlog!(
                Info,
                "target master {} of replicaset {} is not online: trying to choose a new one",
                master.instance_id,
                master.replicaset_id,
            );
        } else {
            continue;
        }

        let Some(new_master) =
            maybe_responding(instances).find(|i| i.replicaset_id == r.replicaset_id)
        else {
            tlog!(
                Warning,
                "there are no instances suitable as master of replicaset {}",
                r.replicaset_id,
            );
            continue;
        };

        return Some(new_master);
    }

    None
}

#[inline(always)]
fn get_replicaset_state_change<'i>(
    instances: &'i [Instance],
    replicasets: &HashMap<&ReplicasetId, &Replicaset>,
    tiers: &HashMap<&str, &Tier>,
) -> Option<(&'i ReplicasetId, bool)> {
    let mut replicaset_sizes = HashMap::new();
    for Instance { replicaset_id, .. } in maybe_responding(instances) {
        let replicaset_size = replicaset_sizes.entry(replicaset_id).or_insert(0);
        *replicaset_size += 1;
        let Some(r) = replicasets.get(replicaset_id) else {
            continue;
        };
        if r.state != ReplicasetState::NotReady {
            continue;
        }
        let Some(tier_info) = tiers.get(r.tier.as_str()) else {
            continue;
        };
        // TODO: set replicaset.state = NotReady if it was Ready but is no
        // longer full
        if *replicaset_size < tier_info.replication_factor {
            continue;
        }
        let need_to_update_weight = r.weight_origin != WeightOrigin::User;
        return Some((replicaset_id, need_to_update_weight));
    }
    None
}

#[inline(always)]
fn get_first_ready_replicaset<'r>(
    instances: &[Instance],
    replicasets: &HashMap<&ReplicasetId, &'r Replicaset>,
) -> Option<&'r Replicaset> {
    for Instance { replicaset_id, .. } in maybe_responding(instances) {
        let Some(replicaset) = replicasets.get(replicaset_id) else {
            continue;
        };
        if replicaset.state == ReplicasetState::Ready && replicaset.weight > 0. {
            return Some(replicaset);
        }
    }
    None
}

#[inline(always)]
fn maybe_responding(instances: &[Instance]) -> impl Iterator<Item = &Instance> {
    instances.iter().filter(|instance| instance.may_respond())
}
