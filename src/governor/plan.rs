use crate::replicaset::weight;
use crate::replicaset::{Replicaset, ReplicasetId};
use crate::storage::{ClusterwideSpace, PropertyName};
use crate::tlog;
use crate::traft::rpc;
use crate::traft::rpc::{replication, sharding, sync, update_instance};
use crate::traft::OpDML;
use crate::traft::Result;
use crate::traft::{CurrentGrade, CurrentGradeVariant, TargetGradeVariant};
use crate::traft::{Instance, InstanceId};
use crate::traft::{RaftId, RaftIndex, RaftTerm};
use ::tarantool::space::UpdateOps;
use std::collections::{HashMap, HashSet};

use super::cc::raft_conf_change;
use super::migration::get_pending_migration;
use super::Loop;

#[allow(clippy::too_many_arguments)]
pub(super) fn action_plan<'i>(
    term: RaftTerm,
    commit: RaftIndex,
    cluster_id: String,
    instances: &'i [Instance],
    voters: &[RaftId],
    learners: &[RaftId],
    replicasets: &HashMap<&ReplicasetId, &'i Replicaset>,
    migration_ids: Vec<u64>,
    my_raft_id: RaftId,
    vshard_bootstrapped: bool,
    replication_factor: usize,
    desired_schema_version: u64,
) -> Result<Plan<'i>> {
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
        .filter(|instance| instance.current_grade != CurrentGradeVariant::Offline)
        // TODO: process them all, not just the first one
        .find(|instance| {
            let (target, current) = (
                instance.target_grade.variant,
                instance.current_grade.variant,
            );
            matches!(target, TargetGradeVariant::Offline)
                || !matches!(current, CurrentGradeVariant::Expelled)
                    && matches!(target, TargetGradeVariant::Expelled)
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
                let rpc = replication::promote::Request {
                    term,
                    commit,
                    timeout: Loop::SYNC_TIMEOUT,
                };
                let mut ops = UpdateOps::new();
                ops.assign("master_id", &to.instance_id)?;
                let op = OpDML::update(ClusterwideSpace::Replicaset, &[&to.replicaset_id], ops)?;
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
                instance.current_grade == CurrentGradeVariant::ShardingInitialized
                    || instance.current_grade == CurrentGradeVariant::Online
            })
            .map(|instance| &instance.instance_id)
            .collect();
        let rpc = sharding::Request {
            term,
            commit,
            timeout: Loop::SYNC_TIMEOUT,
        };
        let req = update_instance::Request::new(instance_id.clone(), cluster_id)
            .with_current_grade((*target_grade).into());
        return Ok(ReconfigureShardingAndDowngrade { targets, rpc, req }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // raft sync
    let to_sync = instances.iter().find(|instance| {
        instance.has_grades(CurrentGradeVariant::Offline, TargetGradeVariant::Online)
            || instance.is_reincarnated()
    });
    if let Some(Instance {
        instance_id,
        target_grade,
        ..
    }) = to_sync
    {
        let rpc = sync::Request {
            commit,
            timeout: Loop::SYNC_TIMEOUT,
        };
        let req = update_instance::Request::new(instance_id.clone(), cluster_id)
            .with_current_grade(CurrentGrade::raft_synced(target_grade.incarnation));
        #[rustfmt::skip]
        return Ok(RaftSync { instance_id, rpc, req }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // create new replicaset
    let to_create_replicaset = instances
        .iter()
        .filter(|instance| {
            instance.has_grades(CurrentGradeVariant::RaftSynced, TargetGradeVariant::Online)
        })
        .find(|instance| replicasets.get(&instance.replicaset_id).is_none());
    if let Some(Instance {
        instance_id: master_id,
        replicaset_id,
        replicaset_uuid,
        ..
    }) = to_create_replicaset
    {
        let rpc = replication::promote::Request {
            term,
            commit,
            timeout: Loop::SYNC_TIMEOUT,
        };
        let weight = if vshard_bootstrapped { 0. } else { 1. };
        let op = OpDML::insert(
            ClusterwideSpace::Replicaset,
            &Replicaset {
                replicaset_id: replicaset_id.clone(),
                replicaset_uuid: replicaset_uuid.clone(),
                master_id: master_id.clone(),
                weight: weight::Info {
                    value: weight,
                    origin: weight::Origin::Auto,
                    state: weight::State::Initial,
                },
                current_schema_version: 0,
            },
        )?;
        #[rustfmt::skip]
        return Ok(CreateReplicaset { master_id, replicaset_id, rpc, op }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // replication
    let to_replicate = instances
        .iter()
        // TODO: find all such instances in a given replicaset,
        // not just the first one
        .find(|instance| {
            instance.has_grades(CurrentGradeVariant::RaftSynced, TargetGradeVariant::Online)
        });
    if let Some(Instance {
        instance_id,
        replicaset_id,
        target_grade,
        ..
    }) = to_replicate
    {
        let targets = maybe_responding(instances)
            .filter(|instance| instance.replicaset_id == replicaset_id)
            .map(|instance| &instance.instance_id)
            .collect();
        let rpc = replication::Request {
            term,
            commit,
            timeout: Loop::SYNC_TIMEOUT,
        };
        let req = update_instance::Request::new(instance_id.clone(), cluster_id)
            .with_current_grade(CurrentGrade::replicated(target_grade.incarnation));

        return Ok(Replication { targets, rpc, req }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // init sharding
    let to_shard = instances.iter().find(|instance| {
        instance.has_grades(CurrentGradeVariant::Replicated, TargetGradeVariant::Online)
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
        let rpc = sharding::Request {
            term,
            commit,
            timeout: Loop::SYNC_TIMEOUT,
        };
        let req = update_instance::Request::new(instance_id.clone(), cluster_id)
            .with_current_grade(CurrentGrade::sharding_initialized(target_grade.incarnation));
        return Ok(ShardingInit { targets, rpc, req }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // bootstrap sharding
    let to_bootstrap = (!vshard_bootstrapped)
        .then(|| get_first_full_replicaset(instances, replicasets, replication_factor))
        .flatten();
    if let Some(Replicaset { master_id, .. }) = to_bootstrap {
        let target = master_id;
        let rpc = sharding::bootstrap::Request {
            term,
            commit,
            timeout: Loop::SYNC_TIMEOUT,
        };
        let op = OpDML::replace(
            ClusterwideSpace::Property,
            &(PropertyName::VshardBootstrapped, true),
        )?;
        return Ok(ShardingBoot { target, rpc, op }.into());
    };

    ////////////////////////////////////////////////////////////////////////////
    // proposing automatic sharding weight changes
    let to_change_weights = get_auto_weight_changes(instances, replicasets, replication_factor);
    if !to_change_weights.is_empty() {
        let mut ops = vec![];
        for replicaset_id in to_change_weights {
            let mut uops = UpdateOps::new();
            uops.assign(weight::Value::PATH, 1.)?;
            uops.assign(weight::State::PATH, weight::State::Updating)?;
            let op = OpDML::update(ClusterwideSpace::Replicaset, &[replicaset_id], uops)?;
            ops.push(op);
        }
        return Ok(ProposeWeightChanges { ops }.into());
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
        let rpc = sharding::Request {
            term,
            commit,
            timeout: Loop::SYNC_TIMEOUT,
        };
        let mut ops = vec![];
        for replicaset_id in to_update_weights {
            let mut uops = UpdateOps::new();
            uops.assign(weight::State::PATH, weight::State::UpToDate)?;
            let op = OpDML::update(ClusterwideSpace::Replicaset, &[replicaset_id], uops)?;
            ops.push(op);
        }
        return Ok(UpdateWeights { targets, rpc, ops }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // to online
    let to_online = instances.iter().find(|instance| {
        instance.has_grades(
            CurrentGradeVariant::ShardingInitialized,
            TargetGradeVariant::Online,
        )
    });
    if let Some(Instance {
        instance_id,
        target_grade,
        ..
    }) = to_online
    {
        let req = update_instance::Request::new(instance_id.clone(), cluster_id)
            .with_current_grade(CurrentGrade::online(target_grade.incarnation));
        return Ok(ToOnline { req }.into());
    }

    ////////////////////////////////////////////////////////////////////////////
    // migration
    let replicasets: Vec<_> = replicasets.values().copied().collect();
    let to_apply = get_pending_migration(migration_ids, &replicasets, desired_schema_version);
    if let Some((migration_id, target)) = to_apply {
        let rpc = rpc::migration::apply::Request {
            term,
            commit,
            timeout: Loop::SYNC_TIMEOUT,
            migration_id,
        };
        let mut ops = UpdateOps::new();
        ops.assign("current_schema_version", migration_id)?;
        let op = OpDML::update(ClusterwideSpace::Replicaset, &[&target.replicaset_id], ops)?;
        return Ok(ApplyMigration { target, rpc, op }.into());
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
            pub rpc: replication::promote::Request,
            pub op: OpDML,
        }

        pub struct ReconfigureShardingAndDowngrade<'i> {
            pub targets: Vec<&'i InstanceId>,
            pub rpc: sharding::Request,
            pub req: update_instance::Request,
        }

        pub struct RaftSync<'i> {
            pub instance_id: &'i InstanceId,
            pub rpc: sync::Request,
            pub req: update_instance::Request,
        }

        pub struct CreateReplicaset<'i> {
            pub master_id: &'i InstanceId,
            pub replicaset_id: &'i ReplicasetId,
            pub rpc: replication::promote::Request,
            pub op: OpDML,
        }

        pub struct Replication<'i> {
            pub targets: Vec<&'i InstanceId>,
            pub rpc: replication::Request,
            pub req: update_instance::Request,
        }

        pub struct ShardingInit<'i> {
            pub targets: Vec<&'i InstanceId>,
            pub rpc: sharding::Request,
            pub req: update_instance::Request,
        }

        pub struct ShardingBoot<'i> {
            pub target: &'i InstanceId,
            pub rpc: sharding::bootstrap::Request,
            pub op: OpDML,
        }

        pub struct ProposeWeightChanges {
            pub ops: Vec<OpDML>,
        }

        pub struct UpdateWeights<'i> {
            pub targets: Vec<&'i InstanceId>,
            pub rpc: sharding::Request,
            pub ops: Vec<OpDML>,
        }

        pub struct ToOnline {
            pub req: update_instance::Request,
        }

        pub struct ApplyMigration<'i> {
            pub target: &'i Replicaset,
            pub rpc: rpc::migration::apply::Request,
            pub op: OpDML,
        }
    }
}

#[inline(always)]
fn get_auto_weight_changes<'i>(
    instances: &'i [Instance],
    replicasets: &HashMap<&ReplicasetId, &Replicaset>,
    replication_factor: usize,
) -> HashSet<&'i ReplicasetId> {
    let mut replicaset_sizes = HashMap::new();
    let mut weight_changes = HashSet::new();
    for Instance { replicaset_id, .. } in maybe_responding(instances) {
        let replicaset_size = replicaset_sizes.entry(replicaset_id).or_insert(0);
        *replicaset_size += 1;
        let Some(Replicaset { weight, .. }) = replicasets.get(replicaset_id) else {
            continue;
        };
        if weight.origin == weight::Origin::User || weight.state == weight::State::Updating {
            continue;
        }
        if *replicaset_size >= replication_factor && weight.value == 0. {
            weight_changes.insert(replicaset_id);
        }
    }
    weight_changes
}

#[inline(always)]
fn get_first_full_replicaset<'r>(
    instances: &[Instance],
    replicasets: &HashMap<&ReplicasetId, &'r Replicaset>,
    replication_factor: usize,
) -> Option<&'r Replicaset> {
    let mut replicaset_sizes = HashMap::new();
    let mut full_replicaset_id = None;
    for Instance { replicaset_id, .. } in maybe_responding(instances) {
        let replicaset_size = replicaset_sizes.entry(replicaset_id).or_insert(0);
        *replicaset_size += 1;
        if *replicaset_size >= replication_factor {
            full_replicaset_id = Some(replicaset_id);
        }
    }

    full_replicaset_id
        .and_then(|id| replicasets.get(id))
        .copied()
}

#[inline(always)]
fn maybe_responding(instances: &[Instance]) -> impl Iterator<Item = &Instance> {
    instances.iter().filter(|instance| instance.may_respond())
}
