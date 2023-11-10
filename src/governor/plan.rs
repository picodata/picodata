use crate::has_grades;
use crate::instance::grade::Grade;
use crate::instance::grade::GradeVariant::*;
use crate::instance::{Instance, InstanceId};
use crate::replicaset::weight;
use crate::replicaset::{Replicaset, ReplicasetId};
use crate::rpc;
use crate::storage::{ClusterwideSpace, PropertyName};
use crate::tier::Tier;
use crate::tlog;
use crate::traft::op::Dml;
use crate::traft::Result;
use crate::traft::{RaftId, RaftIndex, RaftTerm};
use ::tarantool::space::UpdateOps;
use std::collections::HashMap;

use super::cc::raft_conf_change;
use super::Loop;

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
    tiers: &HashMap<&String, &Tier>,
    my_raft_id: RaftId,
    vshard_bootstrapped: bool,
    has_pending_schema_change: bool,
) -> Result<Plan<'i>> {
    // This function is specifically extracted, to separate the task
    // construction from any IO and/or other yielding operations.
    #[cfg(debug_assertions)]
    let _guard = crate::util::NoYieldsGuard::new();

    ////////////////////////////////////////////////////////////////////////////
    // conf change
    if let Some(conf_change) = raft_conf_change(instances, voters, learners) {
        return Ok(Plan::ConfChange(ConfChange { conf_change }));
    }

    // TODO: reduce number of iterations over all instances

    ////////////////////////////////////////////////////////////////////////////
    // downgrading
    let to_downgrade = instances
        .iter()
        // TODO: process them all, not just the first one
        .find(|instance| {
            has_grades!(instance, not Offline -> Offline)
                || has_grades!(instance, not Expelled -> Expelled)
        });
    if let Some(Instance {
        raft_id,
        instance_id,
        replicaset_id,
        target_grade,
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
                return Ok(Plan::TransferLeadership(TransferLeadership {
                    to: new_leader,
                }));
            } else {
                tlog!(Warning, "leader is going offline and no substitution is found";
                    "leader_raft_id" => my_raft_id,
                    "voters" => ?voters,
                );
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // choose a new replicaset master if needed and promote it
        let replicaset = replicasets.get(replicaset_id);
        if matches!(replicaset, Some(replicaset) if replicaset.master_id == instance_id) {
            let new_master = maybe_responding(instances).find(|p| p.replicaset_id == replicaset_id);
            if let Some(to) = new_master {
                let rpc = rpc::replication::promote::Request {};
                let mut ops = UpdateOps::new();
                ops.assign("master_id", &to.instance_id)?;
                let op = Dml::update(ClusterwideSpace::Replicaset, &[&to.replicaset_id], ops)?;
                return Ok(TransferMastership { to, rpc, op }.into());
            } else {
                tlog!(Warning, "replicaset master is going offline and no substitution is found";
                    "master_id" => %instance_id,
                    "replicaset_id" => %replicaset_id,
                );
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // reconfigure vshard storages and routers
        // and update instance's CurrentGrade afterwards
        let targets = maybe_responding(instances)
            .filter(|instance| {
                has_grades!(instance, ShardingInitialized -> *)
                    || has_grades!(instance, Online -> *)
            })
            .map(|instance| &instance.instance_id)
            .collect();
        let rpc = rpc::sharding::Request {
            term,
            applied,
            timeout: Loop::SYNC_TIMEOUT,
        };
        let req = rpc::update_instance::Request::new(instance_id.clone(), cluster_id)
            .with_current_grade(*target_grade);
        return Ok(ReconfigureShardingAndDowngrade { targets, rpc, req }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // create new replicaset
    let to_create_replicaset = instances
        .iter()
        .filter(|instance| has_grades!(instance, Offline -> Online) || instance.is_reincarnated())
        .find(|instance| replicasets.get(&instance.replicaset_id).is_none());
    if let Some(Instance {
        instance_id: master_id,
        replicaset_id,
        replicaset_uuid,
        tier,
        ..
    }) = to_create_replicaset
    {
        let rpc = rpc::replication::promote::Request {};
        let op = Dml::insert(
            ClusterwideSpace::Replicaset,
            &Replicaset {
                replicaset_id: replicaset_id.clone(),
                replicaset_uuid: replicaset_uuid.clone(),
                master_id: master_id.clone(),
                weight: weight::Info {
                    value: 0.,
                    origin: weight::Origin::Auto,
                    state: weight::State::Initial,
                },
                tier: tier.clone(),
            },
        )?;
        #[rustfmt::skip]
        return Ok(CreateReplicaset { master_id, replicaset_id, rpc, op }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // replicaset master switchover
    let to_promote = get_new_replicaset_master_if_needed(instances, replicasets);
    if let Some(to) = to_promote {
        let rpc = rpc::replication::promote::Request {};
        let mut ops = UpdateOps::new();
        ops.assign("master_id", &to.instance_id)?;
        let op = Dml::update(ClusterwideSpace::Replicaset, &[&to.replicaset_id], ops)?;
        return Ok(TransferMastership { to, rpc, op }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // replication
    let to_replicate = instances
        .iter()
        // TODO: find all such instances in a given replicaset,
        // not just the first one
        .find(|instance| has_grades!(instance, Offline -> Online) || instance.is_reincarnated());
    if let Some(Instance {
        instance_id,
        replicaset_id,
        target_grade,
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
        let master_id = &replicaset.master_id;
        let req = rpc::update_instance::Request::new(instance_id.clone(), cluster_id)
            .with_current_grade(Grade::new(Replicated, target_grade.incarnation));

        return Ok(Replication {
            targets,
            master_id,
            replicaset_peers,
            req,
        }
        .into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // init sharding
    let to_shard = instances
        .iter()
        .filter(|i| has_grades!(i, Replicated -> Online))
        .find(|i| {
            vshard_bootstrapped
                || replicasets
                    .get(&i.replicaset_id)
                    .map(|r| r.weight.value != 0.)
                    .unwrap_or(false)
        });
    if let Some(Instance {
        instance_id,
        target_grade,
        ..
    }) = to_shard
    {
        let targets = maybe_responding(instances)
            .map(|instance| &instance.instance_id)
            .collect();
        let rpc = rpc::sharding::Request {
            term,
            applied,
            timeout: Loop::SYNC_TIMEOUT,
        };
        let req = rpc::update_instance::Request::new(instance_id.clone(), cluster_id)
            .with_current_grade(Grade::new(ShardingInitialized, target_grade.incarnation));
        return Ok(ShardingInit { targets, rpc, req }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // bootstrap sharding
    let to_bootstrap = (!vshard_bootstrapped)
        .then(|| get_first_replicaset_with_weight(instances, replicasets))
        .flatten();
    if let Some(Replicaset { master_id, .. }) = to_bootstrap {
        let target = master_id;
        let rpc = rpc::sharding::bootstrap::Request {
            term,
            applied,
            timeout: Loop::SYNC_TIMEOUT,
        };
        let op = Dml::replace(
            ClusterwideSpace::Property,
            &(PropertyName::VshardBootstrapped, true),
        )?;
        return Ok(ShardingBoot { target, rpc, op }.into());
    };

    ////////////////////////////////////////////////////////////////////////////
    // proposing automatic sharding weight changes
    let to_change_weights = get_first_auto_weight_change(instances, replicasets, tiers);
    if let Some(replicaset_id) = to_change_weights {
        let mut uops = UpdateOps::new();
        uops.assign(weight::Value::PATH, 1.)?;
        let state = if vshard_bootstrapped {
            // need to reconfigure sharding on all instances
            weight::State::Updating
        } else {
            // ok to just change the weights
            weight::State::UpToDate
        };
        uops.assign(weight::State::PATH, state)?;
        let op = Dml::update(ClusterwideSpace::Replicaset, &[replicaset_id], uops)?;
        return Ok(ProposeWeightChanges { op }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // skip sharding
    let to_online = instances
        .iter()
        .find(|i| has_grades!(i, Replicated -> Online));
    if let Some(Instance {
        instance_id,
        target_grade,
        ..
    }) = to_online
    {
        // If we got here, there are no replicasets with non zero weights
        // (i.e. filled up to the replication factor) yet,
        // so we can't configure sharding.
        // So we just upgrade the instances to Online.
        let req = rpc::update_instance::Request::new(instance_id.clone(), cluster_id)
            .with_current_grade(Grade::new(Online, target_grade.incarnation));
        return Ok(SkipSharding { req }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // applying proposed sharding weight changes
    let to_update_weights: Vec<_> = replicasets
        .values()
        .filter_map(|r| (r.weight.state == weight::State::Updating).then_some(&r.replicaset_id))
        .collect();
    if !to_update_weights.is_empty() {
        let targets = maybe_responding(instances)
            .map(|instance| &instance.instance_id)
            .collect();
        let rpc = rpc::sharding::Request {
            term,
            applied,
            timeout: Loop::SYNC_TIMEOUT,
        };
        let mut ops = vec![];
        for replicaset_id in to_update_weights {
            let mut uops = UpdateOps::new();
            uops.assign(weight::State::PATH, weight::State::UpToDate)?;
            let op = Dml::update(ClusterwideSpace::Replicaset, &[replicaset_id], uops)?;
            ops.push(op);
        }
        return Ok(UpdateWeights { targets, rpc, ops }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // to online
    let to_online = instances
        .iter()
        .find(|instance| has_grades!(instance, ShardingInitialized -> Online));
    if let Some(Instance {
        instance_id,
        target_grade,
        ..
    }) = to_online
    {
        let req = rpc::update_instance::Request::new(instance_id.clone(), cluster_id)
            .with_current_grade(Grade::new(Online, target_grade.incarnation));
        return Ok(ToOnline { req }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // ddl
    if has_pending_schema_change {
        let mut targets = Vec::with_capacity(replicasets.len());
        // TODO: invert this loop to improve performance
        // `for instances { replicasets.get() }` instead of `for replicasets { instances.find() }`
        for r in replicasets.values() {
            let Some(master) = instances.iter().find(|i| i.instance_id == r.master_id) else {
                tlog!(
                    Warning,
                    "couldn't find instance with id {}, which is chosen as master of replicaset {}",
                    r.master_id,
                    r.replicaset_id,
                );
                // Send them a request anyway just to be safe
                targets.push(&r.master_id);
                continue;
            };
            if has_grades!(master, Expelled -> *) {
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

    Ok(Plan::None)
}

macro_rules! define_plan {
    (
        $(
            pub struct $stage:ident $(<$lt:tt>)? {
                $(pub $field:ident: $field_ty:ty,)+
            }
        )+
    ) => {
        $(
            pub struct $stage $(<$lt>)? {
                $(pub $field: $field_ty,)+
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

    define_plan! {
        pub struct ConfChange {
            pub conf_change: raft::prelude::ConfChangeV2,
        }

        pub struct TransferLeadership<'i> {
            pub to: &'i Instance,
        }

        pub struct TransferMastership<'i> {
            pub to: &'i Instance,
            pub rpc: rpc::replication::promote::Request,
            pub op: Dml,
        }

        pub struct ReconfigureShardingAndDowngrade<'i> {
            pub targets: Vec<&'i InstanceId>,
            pub rpc: rpc::sharding::Request,
            pub req: rpc::update_instance::Request,
        }

        pub struct CreateReplicaset<'i> {
            pub master_id: &'i InstanceId,
            pub replicaset_id: &'i ReplicasetId,
            pub rpc: rpc::replication::promote::Request,
            pub op: Dml,
        }

        pub struct Replication<'i> {
            pub targets: Vec<&'i InstanceId>,
            pub master_id: &'i InstanceId,
            pub replicaset_peers: Vec<String>,
            pub req: rpc::update_instance::Request,
        }

        pub struct ShardingInit<'i> {
            pub targets: Vec<&'i InstanceId>,
            pub rpc: rpc::sharding::Request,
            pub req: rpc::update_instance::Request,
        }

        pub struct SkipSharding {
            pub req: rpc::update_instance::Request,
        }

        pub struct ShardingBoot<'i> {
            pub target: &'i InstanceId,
            pub rpc: rpc::sharding::bootstrap::Request,
            pub op: Dml,
        }

        pub struct ProposeWeightChanges {
            pub op: Dml,
        }

        pub struct UpdateWeights<'i> {
            pub targets: Vec<&'i InstanceId>,
            pub rpc: rpc::sharding::Request,
            pub ops: Vec<Dml>,
        }

        pub struct ToOnline {
            pub req: rpc::update_instance::Request,
        }

        pub struct ApplySchemaChange<'i> {
            pub targets: Vec<&'i InstanceId>,
            pub rpc: rpc::ddl_apply::Request,
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
        let Some(master) = instances.iter().find(|i| i.instance_id == r.master_id) else {
            crate::warn_or_panic!(
                "couldn't find instance with id {}, which is chosen as master of replicaset {}",
                r.master_id,
                r.replicaset_id,
            );
            continue;
        };
        if !has_grades!(master, * -> Offline) {
            continue;
        }
        let Some(new_master) =
            maybe_responding(instances).find(|i| i.replicaset_id == r.replicaset_id)
        else {
            continue;
        };

        return Some(new_master);
    }

    None
}

#[inline(always)]
fn get_first_auto_weight_change<'i>(
    instances: &'i [Instance],
    replicasets: &HashMap<&ReplicasetId, &Replicaset>,
    tiers: &HashMap<&String, &Tier>,
) -> Option<&'i ReplicasetId> {
    let mut replicaset_sizes = HashMap::new();
    for Instance { replicaset_id, .. } in maybe_responding(instances) {
        let replicaset_size = replicaset_sizes.entry(replicaset_id).or_insert(0);
        *replicaset_size += 1;
        let Some(Replicaset { weight, tier, .. }) = replicasets.get(replicaset_id) else {
            continue;
        };
        if weight.origin == weight::Origin::User || weight.state == weight::State::Updating {
            continue;
        }
        let Some(tier_info) = tiers.get(tier) else {
            continue;
        };
        if *replicaset_size >= tier_info.replication_factor && weight.value == 0. {
            return Some(replicaset_id);
        }
    }
    None
}

#[inline(always)]
fn get_first_replicaset_with_weight<'r>(
    instances: &[Instance],
    replicasets: &HashMap<&ReplicasetId, &'r Replicaset>,
) -> Option<&'r Replicaset> {
    for Instance { replicaset_id, .. } in maybe_responding(instances) {
        let Some(replicaset) = replicasets.get(replicaset_id) else {
            continue;
        };
        if replicaset.weight.state == weight::State::UpToDate && replicaset.weight.value > 0. {
            return Some(replicaset);
        }
    }
    None
}

#[inline(always)]
fn maybe_responding(instances: &[Instance]) -> impl Iterator<Item = &Instance> {
    instances.iter().filter(|instance| instance.may_respond())
}
