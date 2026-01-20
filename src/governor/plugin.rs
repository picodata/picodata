use crate::column_name;
use crate::governor::batch::get_next_batch;
use crate::governor::batch::LastStepInfo;
use crate::governor::plan::stage::Plan;
use crate::governor::plan::stage::*;
use crate::plugin::Manifest;
use crate::plugin::PluginIdentifier;
use crate::plugin::PluginOp;
use crate::plugin::TopologyUpdateOpKind;
use crate::rpc;
use crate::schema::PluginConfigRecord;
use crate::schema::PluginDef;
use crate::schema::ServiceDef;
use crate::schema::ServiceRouteItem;
use crate::schema::ServiceRouteKey;
use crate::schema::ADMIN_ID;
use crate::storage;
use crate::storage::PropertyName;
use crate::storage::SystemTable;
use crate::topology_cache::TopologyCacheRef;
use crate::traft::op::Dml;
use crate::traft::op::Op;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;
use crate::traft::Result;
use crate::warn_or_panic;
use picodata_plugin::plugin::interface::ServiceId;
use rmpv::Value;
use smol_str::SmolStr;
use std::collections::HashMap;
use std::time::Duration;
use tarantool::space::UpdateOps;

////////////////////////////////////////////////////////////////////////////////
// handle_plugin_op
////////////////////////////////////////////////////////////////////////////////

pub fn handle_plugin_op<'i>(
    plugin_op: Option<&'i PluginOp>,
    last_step_info: &mut LastStepInfo,
    topology_ref: &TopologyCacheRef,
    plugins: &HashMap<PluginIdentifier, PluginDef>,
    services: &HashMap<PluginIdentifier, Vec<&ServiceDef>>,
    term: RaftTerm,
    applied: RaftIndex,
    sync_timeout: Duration,
    batch_size: usize,
) -> Result<Option<Plan<'i>>> {
    let Some(plugin_op) = plugin_op else {
        return Ok(None);
    };

    last_step_info.update_plugin_op(plugin_op);

    match plugin_op {
        PluginOp::CreatePlugin {
            manifest,
            inherit_entities,
            inherit_topology,
        } => {
            return handle_create_plugin(
                plugin_op,
                manifest,
                inherit_entities,
                inherit_topology,
                last_step_info,
                topology_ref,
                plugins,
                term,
                applied,
                sync_timeout,
                batch_size,
            );
        }

        PluginOp::EnablePlugin {
            plugin,
            timeout: on_start_timeout,
        } => {
            return handle_enable_plugin(
                plugin_op,
                plugin.clone(),
                *on_start_timeout,
                last_step_info,
                topology_ref,
                services,
                term,
                applied,
                sync_timeout,
                batch_size,
            );
        }

        PluginOp::AlterServiceTiers {
            plugin,
            service,
            tier,
            kind,
        } => {
            return handle_alter_service_tiers(
                plugin_op,
                plugin.clone(),
                service,
                tier,
                *kind,
                last_step_info,
                topology_ref,
                plugins,
                services,
                term,
                applied,
                sync_timeout,
                batch_size,
            );
        }

        PluginOp::MigrationLock { .. } => {
            // Governor ignores migration lock
        }
    }

    Ok(None)
}

////////////////////////////////////////////////////////////////////////////////
// handle_create_plugin
////////////////////////////////////////////////////////////////////////////////

pub fn handle_create_plugin<'i>(
    plugin_op: &'i PluginOp,
    manifest: &Manifest,
    inherit_entities: &HashMap<SmolStr, Value>,
    inherit_topology: &HashMap<SmolStr, Vec<SmolStr>>,
    last_step_info: &mut LastStepInfo,
    topology_ref: &TopologyCacheRef,
    plugins: &HashMap<PluginIdentifier, PluginDef>,
    term: RaftTerm,
    applied: RaftIndex,
    sync_timeout: Duration,
    batch_size: usize,
) -> Result<Option<Plan<'i>>> {
    // We must check if step kind is different from the one we tried on
    // previous iteration, so that we know not to use irrelevant results
    type Action<'i> = CreatePlugin<'i>;
    let step_kind = Action::KIND;
    last_step_info.update_step_kind(step_kind);

    let ident = manifest.plugin_identifier();
    if plugins.get(&ident).is_some() {
        warn_or_panic!(
            "received a request to install a plugin which is already installed {ident:?}"
        );
    }

    let targets_total: Vec<_> = topology_ref
        .all_instances()
        .filter(|instance| instance.may_respond())
        .map(|instance| instance.name.clone())
        .collect();

    let res = get_next_batch(&targets_total, last_step_info, batch_size);
    let targets_batch = match res {
        Ok(v) => v,
        Err(next_try) => {
            return Ok(Some(SleepDueToBackoff::new(next_try, step_kind).into()));
        }
    };

    let rpc = rpc::load_plugin_dry_run::Request {
        term,
        applied,
        timeout: sync_timeout,
    };

    let plugin_def = manifest.plugin_def();
    let mut ops = vec![];

    let dml = Dml::replace(storage::Plugins::TABLE_ID, &plugin_def, ADMIN_ID)?;
    ops.push(dml);

    let plugin = plugin_def.into_identifier();
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
            PluginConfigRecord::from_config(&plugin, &service_def.name, config.clone())?;

        for config_rec in config_records {
            let dml = Dml::replace(storage::PluginConfig::TABLE_ID, &config_rec, ADMIN_ID)?;
            ops.push(dml);
        }
    }

    for (entity, config) in inherit_entities {
        let config_records = PluginConfigRecord::from_config(&plugin, entity, config.clone())?;
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
    Ok(Some(
        Action {
            plugin,
            plugin_op,
            targets_total,
            targets_batch,
            rpc,
            success_dml,
        }
        .into(),
    ))
}

////////////////////////////////////////////////////////////////////////////////
// handle_enable_plugin
////////////////////////////////////////////////////////////////////////////////

pub fn handle_enable_plugin<'i>(
    plugin_op: &'i PluginOp,
    plugin: PluginIdentifier,
    on_start_timeout: Duration,
    last_step_info: &mut LastStepInfo,
    topology_ref: &TopologyCacheRef,
    services: &HashMap<PluginIdentifier, Vec<&ServiceDef>>,
    term: RaftTerm,
    applied: RaftIndex,
    sync_timeout: Duration,
    batch_size: usize,
) -> Result<Option<Plan<'i>>> {
    // We must check if step kind is different from the one we tried on
    // previous iteration, so that we know not to use irrelevant results
    type Action<'i> = EnablePlugin<'i>;
    let step_kind = Action::KIND;
    last_step_info.update_step_kind(step_kind);

    let service_defs = services.get(&plugin).map(|v| &**v).unwrap_or(&[]);

    let mut targets_total = vec![];
    let mut success_dml = vec![];

    for instance in topology_ref.all_instances() {
        if !instance.may_respond() {
            continue;
        }

        let tier = &instance.tier;
        let name = &instance.name;

        for svc in service_defs {
            if !svc.tiers.contains(tier) {
                continue;
            }
            let dml = Dml::replace(
                storage::ServiceRouteTable::TABLE_ID,
                &ServiceRouteItem::new_healthy(name.clone(), &plugin, svc.name.clone()),
                ADMIN_ID,
            )?;
            success_dml.push(dml);
        }

        targets_total.push(name.clone());
    }

    let res = get_next_batch(&targets_total, last_step_info, batch_size);
    let targets_batch = match res {
        Ok(v) => v,
        Err(next_try) => {
            return Ok(Some(SleepDueToBackoff::new(next_try, step_kind).into()));
        }
    };

    let rpc = rpc::enable_plugin::Request {
        term,
        applied,
        timeout: sync_timeout,
    };

    let mut enable_ops = UpdateOps::new();
    enable_ops.assign(column_name!(PluginDef, enabled), true)?;
    let dml = Dml::update(
        storage::Plugins::TABLE_ID,
        &[&plugin.name, &plugin.version],
        enable_ops,
        ADMIN_ID,
    )?;
    success_dml.push(dml);

    let dml = Dml::delete(
        storage::Properties::TABLE_ID,
        &[PropertyName::PendingPluginOperation],
        ADMIN_ID,
        None,
    )?;
    success_dml.push(dml);
    let success_dml = Op::BatchDml { ops: success_dml };

    Ok(Some(
        Action {
            plugin_op,
            rpc,
            targets_total,
            targets_batch,
            on_start_timeout,
            plugin,
            success_dml,
        }
        .into(),
    ))
}

////////////////////////////////////////////////////////////////////////////////
// handle_alter_service_tiers
////////////////////////////////////////////////////////////////////////////////

pub fn handle_alter_service_tiers<'i>(
    plugin_op: &'i PluginOp,
    plugin: PluginIdentifier,
    service: &SmolStr,
    tier: &SmolStr,
    kind: TopologyUpdateOpKind,
    last_step_info: &mut LastStepInfo,
    topology_ref: &TopologyCacheRef,
    plugins: &HashMap<PluginIdentifier, PluginDef>,
    services: &HashMap<PluginIdentifier, Vec<&ServiceDef>>,
    term: RaftTerm,
    applied: RaftIndex,
    sync_timeout: Duration,
    batch_size: usize,
) -> Result<Option<Plan<'i>>> {
    // We must check if step kind is different from the one we tried on
    // previous iteration, so that we know not to use irrelevant results
    type Action<'i> = AlterServiceTiers<'i>;
    let step_kind = Action::KIND;
    last_step_info.update_step_kind(step_kind);

    let mut enable_targets_total = vec![];
    let mut disable_targets_total = vec![];
    let mut on_success_dml = vec![];

    let plugin_def = plugins
        .get(&plugin)
        .expect("operation for non existent plugin");
    let service_def = *services
        .get(&plugin)
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
        for instance in topology_ref.all_instances() {
            if !instance.may_respond() {
                continue;
            }

            let tier = &instance.tier;
            let instance_name = &instance.name;

            // if instance in both new and old tiers - do nothing
            if new_tiers.contains(tier) && old_tiers.contains(tier) {
                continue;
            }

            if new_tiers.contains(tier) {
                enable_targets_total.push(instance_name.clone());
                let dml = Dml::replace(
                    storage::ServiceRouteTable::TABLE_ID,
                    &ServiceRouteItem::new_healthy(
                        instance_name.clone(),
                        &plugin,
                        service_def.name.clone(),
                    ),
                    ADMIN_ID,
                )?;
                on_success_dml.push(dml);
            }

            if old_tiers.contains(tier) {
                disable_targets_total.push(instance_name.clone());
                let key = ServiceRouteKey {
                    instance_name,
                    plugin_name: &plugin.name,
                    plugin_version: &plugin.version,
                    service_name: &service_def.name,
                };
                let dml = Dml::delete(storage::ServiceRouteTable::TABLE_ID, &key, ADMIN_ID, None)?;
                on_success_dml.push(dml);
            }
        }
    }

    let mut enable_targets_batch = vec![];
    // get_next_batch expects a non-empty input
    if !enable_targets_total.is_empty() {
        let res = get_next_batch(&enable_targets_total, last_step_info, batch_size);
        enable_targets_batch = match res {
            Ok(v) => v,
            Err(next_try) => {
                return Ok(Some(SleepDueToBackoff::new(next_try, step_kind).into()));
            }
        };
    }

    let mut disable_targets_batch = vec![];
    // We first handle all enable RPCs because they may result in operation abortion
    let all_enablings_done = enable_targets_batch.is_empty();
    // get_next_batch expects a non-empty input
    let have_disablings = !disable_targets_total.is_empty();
    if all_enablings_done && have_disablings {
        let res = get_next_batch(&disable_targets_total, last_step_info, batch_size);
        disable_targets_batch = match res {
            Ok(v) => v,
            Err(next_try) => {
                return Ok(Some(SleepDueToBackoff::new(next_try, step_kind).into()));
            }
        };
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

    Ok(Some(
        Action {
            service: ServiceId::new(plugin.name, service.clone(), plugin.version),
            plugin_op,
            enable_targets_total,
            enable_targets_batch,
            disable_targets_total,
            disable_targets_batch,
            enable_rpc,
            disable_rpc,
            success_dml,
        }
        .into(),
    ))
}
