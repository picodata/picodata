use super::plan::{get_first_ready_replicaset_in_tier, maybe_responding};
use super::{Plan, ShardingBoot, UpdateCurrentVshardConfig};
use crate::cas;
use crate::column_name;
use crate::instance::Instance;
use crate::replicaset::{Replicaset, ReplicasetName};
use crate::rpc;
use crate::schema::ADMIN_ID;
use crate::storage::{SystemTable, Tiers};
use crate::tier::Tier;
use crate::traft::op::Dml;
use crate::traft::{RaftIndex, RaftTerm, Result};
use std::collections::HashMap;
use tarantool::space::UpdateOps;

pub(super) fn handle_sharding<'i>(
    term: RaftTerm,
    applied: RaftIndex,
    tiers: &HashMap<&str, &'i Tier>,
    instances: &'i [Instance],
    replicasets: &HashMap<&ReplicasetName, &'i Replicaset>,
    timeout: std::time::Duration,
) -> Result<Option<Plan<'i>>> {
    for (&tier_name, &tier) in tiers.iter() {
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
        if !ok_to_configure_vshard
            || tier.current_vshard_config_version == tier.target_vshard_config_version
        {
            continue;
        }

        // Note at this point all the instances should have their replication configured,
        // so it's ok to configure sharding for them
        let targets = maybe_responding(instances)
            .map(|instance| &instance.name)
            .collect();
        let rpc = rpc::sharding::Request {
            term,
            applied,
            timeout,
        };

        let mut uops = UpdateOps::new();
        uops.assign(
            column_name!(Tier, current_vshard_config_version),
            tier.target_vshard_config_version,
        )?;

        let bump = Dml::update(Tiers::TABLE_ID, &[tier_name], uops, ADMIN_ID)?;
        let predicate = cas::Predicate::new(applied, []);
        let cas = cas::Request::new(bump, predicate, ADMIN_ID)?;

        return Ok(Some(
            UpdateCurrentVshardConfig {
                targets,
                rpc,
                cas,
                tier_name: tier_name.into(),
            }
            .into(),
        ));
    }

    Ok(None)
}

pub(super) fn handle_sharding_bootstrap<'i>(
    term: RaftTerm,
    applied: RaftIndex,
    tiers: &HashMap<&str, &'i Tier>,
    instances: &'i [Instance],
    replicasets: &HashMap<&ReplicasetName, &'i Replicaset>,
    timeout: std::time::Duration,
) -> Result<Option<Plan<'i>>> {
    for (&tier_name, &tier) in tiers.iter() {
        if tier.vshard_bootstrapped {
            continue;
        }
        let Some(r) = get_first_ready_replicaset_in_tier(instances, replicasets, tier_name) else {
            continue;
        };

        debug_assert!(
            !tier.vshard_bootstrapped,
            "bucket distribution only needs to be bootstrapped once"
        );
        let target = &r.current_master_name;
        let tier_name = &r.tier;
        let rpc = rpc::sharding::bootstrap::Request {
            term,
            applied,
            timeout,
            tier: tier_name.clone(),
        };

        let mut uops = UpdateOps::new();
        uops.assign(column_name!(Tier, vshard_bootstrapped), true)?;

        let dml = Dml::update(Tiers::TABLE_ID, &[tier_name], uops, ADMIN_ID)?;
        let predicate = cas::Predicate::new(applied, []);
        let cas = cas::Request::new(dml, predicate, ADMIN_ID)?;

        return Ok(Some(
            ShardingBoot {
                target,
                rpc,
                cas,
                tier_name: tier_name.clone(),
            }
            .into(),
        ));
    }

    Ok(None)
}
