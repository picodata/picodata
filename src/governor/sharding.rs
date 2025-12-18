use super::plan::{get_first_ready_replicaset_in_tier, maybe_responding};
use super::{Plan, ShardingBoot, UpdateCurrentVshardConfig};
use crate::cas;
use crate::column_name;
use crate::governor::LastStepInfo;
use crate::instance::Instance;
use crate::replicaset::{Replicaset, ReplicasetName};
use crate::rpc;
use crate::schema::ADMIN_ID;
use crate::storage::{SystemTable, Tiers};
use crate::tier::Tier;
use crate::traft::op::Dml;
use crate::traft::{RaftIndex, RaftTerm, Result};
use std::collections::HashMap;
use tarantool::fiber;
use tarantool::space::UpdateOps;

pub(super) fn handle_sharding<'i>(
    last_step_info: &mut LastStepInfo,
    term: RaftTerm,
    applied: RaftIndex,
    tiers: &HashMap<&str, &'i Tier>,
    instances: &'i [Instance],
    replicasets: &HashMap<&ReplicasetName, &'i Replicaset>,
    timeout: std::time::Duration,
    batch_size: usize,
) -> Result<Option<Plan<'i>>> {
    // FIXME: at some point this outer loop became redundant. We always
    // configure vshard in all tiers at once, so it doesn't make sense to update
    // their target/current versions separately. We should either refactor the
    // outer loop or look into configuring separate tiers separately
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

        // XXX: I don't like that we're mutating `last_step_info` in here, but
        // we must check if vshard config versions have changed before choosing
        // targets for RPC. Maybe we should instead call this before
        // `action_plan` altogether, but I don't like how this complicates
        // things. This is good enough for the first version I guess...
        last_step_info.update_vshard_config_versions(tiers);

        let targets_total: Vec<_> = maybe_responding(instances)
            .map(|instance| &instance.name)
            .collect();

        if targets_total.is_empty() {
            // No online instances in the cluster. Can't send anybody an RPC
            return Ok(None);
        }

        let now = fiber::clock();

        // Note at this point all the instances should have their replication configured,
        // so it's ok to configure sharding for them
        let mut only_new_ones = Vec::with_capacity(batch_size);
        let mut already_tried = vec![];
        let mut closest_next_try = None;

        for name in &targets_total {
            if last_step_info.instance_ok(name) {
                continue;
            }

            if let Some(next_try) = last_step_info.backoff_for_instance_will_end_at(name) {
                if now < next_try {
                    // This instance is in backoff, not going to send RPC this time

                    if *closest_next_try.get_or_insert(next_try) > next_try {
                        closest_next_try = Some(next_try);
                    }

                    continue;
                }

                if already_tried.len() < batch_size {
                    already_tried.push(*name);
                }
            } else {
                only_new_ones.push(*name);
                if only_new_ones.len() >= batch_size {
                    break;
                }
            }
        }

        // First try sending to target to whom we didn't send a RPC yet
        let mut targets_batch = only_new_ones;
        if targets_batch.len() < batch_size {
            // If there's not enough new ones, time to send to ones we've already tried sending to
            let remaining = already_tried.len().min(batch_size - targets_batch.len());
            targets_batch.extend_from_slice(&already_tried[0..remaining]);
        }

        if !targets_batch.is_empty() {
            // We only need to know the next try moment if we didn't send the
            // RPC to anyone this time around
            closest_next_try = None;
        } else {
            // If nobody gets RPC this time around then we must have skipped
            // someone due to backoff
            debug_assert!(closest_next_try.is_some());
        }

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
                targets_total,
                targets_batch,
                rpc,
                cas,
                tier_name: tier_name.into(),
                next_try: closest_next_try,
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
