use crate::has_grades;
use crate::instance::grade::Grade;
use crate::instance::grade::GradeVariant::*;
use crate::instance::{Instance, InstanceId};
use crate::replicaset::ReplicasetState;
use crate::replicaset::WeightOrigin;
use crate::replicaset::{Replicaset, ReplicasetId};
use crate::rpc;
use crate::schema::ADMIN_ID;
use crate::storage::{ClusterwideTable, PropertyName};
use crate::tier::Tier;
use crate::tlog;
use crate::traft::op::Dml;
use crate::traft::Result;
use crate::traft::{RaftId, RaftIndex, RaftTerm};
use crate::vshard::VshardConfig;
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
    current_vshard_config: &VshardConfig,
    target_vshard_config: &VshardConfig,
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
        // update instance's current grade
        let req = rpc::update_instance::Request::new(instance_id.clone(), cluster_id)
            .with_current_grade(*target_grade);
        return Ok(Downgrade { req }.into());
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
            ClusterwideTable::Replicaset,
            &Replicaset {
                replicaset_id: replicaset_id.clone(),
                replicaset_uuid: replicaset_uuid.clone(),
                master_id: master_id.clone(),
                weight: 0.,
                weight_origin: WeightOrigin::Auto,
                state: ReplicasetState::NotReady,
                tier: tier.clone(),
            },
            ADMIN_ID,
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
        let op = Dml::update(
            ClusterwideTable::Replicaset,
            &[&to.replicaset_id],
            ops,
            ADMIN_ID,
        )?;
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
        return Ok(ProposeReplicasetStateChanges { op }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // update target vshard config
    let vshard_config =
        VshardConfig::new(instances, peer_addresses, replicasets, vshard_bootstrapped);
    if &vshard_config != target_vshard_config {
        let dml = Dml::replace(
            ClusterwideTable::Property,
            // FIXME: encode as map
            &(&PropertyName::TargetVshardConfig, vshard_config),
            ADMIN_ID,
        )?;
        return Ok(UpdateTargetVshardConfig { dml }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // update current vshard config
    if current_vshard_config != target_vshard_config {
        let targets = maybe_responding(instances)
            .filter(|instance| instance.current_grade.variant >= Replicated)
            .map(|instance| &instance.instance_id)
            .collect();
        let rpc = rpc::sharding::Request {
            term,
            applied,
            timeout: Loop::SYNC_TIMEOUT,
            do_reconfigure: true,
        };
        let dml = Dml::replace(
            ClusterwideTable::Property,
            // FIXME: encode as map
            &(&PropertyName::CurrentVshardConfig, target_vshard_config),
            ADMIN_ID,
        )?;
        return Ok(UpdateCurrentVshardConfig { targets, rpc, dml }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // bootstrap sharding
    let to_bootstrap = (!vshard_bootstrapped)
        .then(|| get_first_ready_replicaset(instances, replicasets))
        .flatten();
    if let Some(Replicaset { master_id, .. }) = to_bootstrap {
        let target = master_id;
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
        .find(|instance| has_grades!(instance, Replicated -> Online));
    if let Some(Instance {
        instance_id,
        target_grade,
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
        let req = rpc::update_instance::Request::new(instance_id.clone(), cluster_id)
            .with_current_grade(Grade::new(Online, target_grade.incarnation));
        return Ok(ToOnline { target, rpc, req }.into());
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

        pub struct UpdateTargetVshardConfig {
            pub dml: Dml,
        }

        pub struct UpdateCurrentVshardConfig<'i> {
            pub targets: Vec<&'i InstanceId>,
            pub rpc: rpc::sharding::Request,
            pub dml: Dml,
        }

        pub struct TransferLeadership<'i> {
            pub to: &'i Instance,
        }

        pub struct TransferMastership<'i> {
            pub to: &'i Instance,
            pub rpc: rpc::replication::promote::Request,
            pub op: Dml,
        }

        pub struct Downgrade {
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

        pub struct ProposeReplicasetStateChanges {
            pub op: Dml,
        }

        pub struct ToOnline<'i> {
            pub target: &'i InstanceId,
            pub rpc: Option<rpc::sharding::Request>,
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
        if master.may_respond() {
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
fn get_replicaset_state_change<'i>(
    instances: &'i [Instance],
    replicasets: &HashMap<&ReplicasetId, &Replicaset>,
    tiers: &HashMap<&String, &Tier>,
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
        let Some(tier_info) = tiers.get(&r.tier) else {
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
