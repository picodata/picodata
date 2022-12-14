use std::collections::HashMap;
use std::iter::repeat;
use std::time::Duration;

use ::tarantool::fiber;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;
use ::tarantool::fiber::r#async::watch;
use ::tarantool::space::UpdateOps;

use crate::event::{self, Event};
use crate::r#loop::FlowControl::{self, Continue};
use crate::storage::ToEntryIter as _;
use crate::storage::{Clusterwide, ClusterwideSpace, PropertyName};
use crate::tlog;
use crate::traft::network::{ConnectionPool, IdOfInstance};
use crate::traft::node::global;
use crate::traft::node::Status;
use crate::traft::raft_storage::RaftSpaceAccess;
use crate::traft::rpc;
use crate::traft::rpc::sharding::cfg::ReplicasetWeights;
use crate::traft::rpc::{replication, sharding, sync, update_instance};
use crate::traft::InstanceId;
use crate::traft::OpDML;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;
use crate::traft::ReplicasetId;
use crate::traft::Result;
use crate::traft::{CurrentGrade, CurrentGradeVariant, TargetGradeVariant};
use crate::traft::{Instance, Replicaset};
use crate::unwrap_ok_or;

use actions::*;

use futures::future::{join_all, try_join_all};

pub(crate) mod cc;
pub(crate) mod migration;

pub(crate) use cc::raft_conf_change;
pub(crate) use migration::waiting_migrations;

macro_rules! governor_step {
    ($desc:literal $([ $($kv:tt)* ])? async { $($body:tt)+ }) => {
        tlog!(Info, $desc $(; $($kv)*)?);
        #[allow(redundant_semicolons)]
        let res: Result<_> = async {
            $($body)+;
            Ok(())
        }
        .await;
        if let Err(e) = res {
            tlog!(Warning, ::std::concat!("failed ", $desc, ": {}"), e, $(; $($kv)*)?);
            event::wait_timeout(Event::TopologyChanged, Loop::RETRY_TIMEOUT).unwrap();
            return Continue;
        }
    }
}

impl Loop {
    const SYNC_TIMEOUT: Duration = Duration::from_secs(10);
    const RETRY_TIMEOUT: Duration = Duration::from_millis(250);

    async fn iter_fn(
        Args {
            storage,
            raft_storage,
        }: &Args,
        State { status, pool }: &mut State,
    ) -> FlowControl {
        if !status.get().raft_state.is_leader() {
            status.changed().await.unwrap();
            return Continue;
        }

        let instances = storage.instances.all_instances().unwrap();
        let instances = &instances[..];
        let voters = raft_storage.voters().unwrap().unwrap_or_default();
        let learners = raft_storage.learners().unwrap().unwrap_or_default();
        let replicasets: Vec<_> = storage.replicasets.iter().unwrap().collect();
        let replicasets: HashMap<_, _> = replicasets
            .iter()
            .map(|rs| (&rs.replicaset_id, rs))
            .collect();

        let term = status.get().term;
        let commit = raft_storage.commit().unwrap().unwrap();
        let cluster_id = raft_storage.cluster_id().unwrap().unwrap();
        let node = global().expect("must be initialized");
        let vshard_bootstrapped = storage.properties.vshard_bootstrapped().unwrap();

        let plan = action_plan(
            term,
            commit,
            cluster_id.clone(),
            instances,
            &voters,
            &learners,
            &replicasets,
            node.raft_id,
            vshard_bootstrapped,
        );
        let plan = unwrap_ok_or!(plan,
            Err(e) => {
                tlog!(Warning, "failed constructing an action plan: {e}");
                // TODO don't hard code timeout
                event::wait_timeout(Event::TopologyChanged, Loop::RETRY_TIMEOUT).unwrap();
                return Continue;
            }
        );

        // TODO: remove this once all plans are implemented
        let mut did_something = true;
        match plan {
            Plan::ConfChange(conf_change) => {
                // main_loop gives the warranty that every ProposeConfChange
                // will sometimes be handled and there's no need in timeout.
                // It also guarantees that the notification will arrive only
                // after the node leaves the joint state.
                tlog!(Info, "proposing conf_change"; "cc" => ?conf_change);
                if let Err(e) = node.propose_conf_change_and_wait(term, conf_change) {
                    tlog!(Warning, "failed proposing conf_change: {e}");
                    fiber::sleep(Duration::from_secs(1));
                }
            }

            Plan::TransferLeadership(TransferLeadership { to }) => {
                tlog!(Info, "transferring leadership to {}", to.instance_id);
                node.transfer_leadership_and_yield(to.raft_id);
                event::wait_timeout(Event::TopologyChanged, Loop::RETRY_TIMEOUT).unwrap();
            }

            Plan::TransferMastership(TransferMastership { to, rpc, op }) => {
                #[rustfmt::skip]
                let Instance { instance_id, replicaset_id, .. } = to;
                tlog!(Info, "transferring replicaset mastership to {instance_id}");

                governor_step! {
                    "promoting new master" [
                        "master_id" => %instance_id,
                        "replicaset_id" => %replicaset_id,
                    ]
                    async {
                        pool.call(instance_id, &rpc)?
                            // TODO: don't hard code timeout
                            .timeout(Duration::from_secs(3))
                            .await??
                    }
                }

                governor_step! {
                    "proposing replicaset master change" [
                        "master_id" => %instance_id,
                        "replicaset_id" => %replicaset_id,
                    ]
                    async {
                        node.propose_and_wait(op, Duration::from_secs(3))??
                    }
                }
            }

            Plan::ReconfigureShardingAndDowngrade(ReconfigureShardingAndDowngrade {
                targets,
                rpc,
                req,
            }) => {
                tlog!(Info, "downgrading instance {}", req.instance_id);

                governor_step! {
                    "reconfiguring sharding"
                    async {
                        let mut fs = vec![];
                        for instance_id in targets {
                            tlog!(Info, "calling rpc::sharding"; "instance_id" => %instance_id);
                            let resp = pool.call(instance_id, &rpc)?;
                            fs.push(async move {
                                resp.await.map_err(|e| {
                                    tlog!(Warning, "failed calling rpc::sharding: {e}";
                                        "instance_id" => %instance_id
                                    );
                                    e
                                })
                            });
                        }
                        // TODO: don't hard code timeout
                        try_join_all(fs).timeout(Duration::from_secs(3)).await??
                    }
                }

                let instance_id = req.instance_id.clone();
                let current_grade = req.current_grade.expect("must be set");
                governor_step! {
                    "handling instance grade change" [
                        "instance_id" => %instance_id,
                        "current_grade" => %current_grade,
                    ]
                    async {
                        node.handle_update_instance_request_and_wait(req)?
                    }
                }
            }

            Plan::RaftSync(RaftSync {
                instance_id,
                rpc,
                req,
            }) => {
                governor_step! {
                    "syncing raft log" [
                        "instance_id" => %instance_id
                    ]
                    async {
                        let sync::Response { commit } = pool
                            .call(instance_id, &rpc)?
                            .timeout(Loop::SYNC_TIMEOUT)
                            .await??;
                        tlog!(Info, "instance's commit index is {commit}"; "instance_id" => %instance_id);
                        node.handle_update_instance_request_and_wait(req)?
                    }
                }
            }

            Plan::CreateReplicaset(CreateReplicaset {
                master_id,
                replicaset_id,
                rpc,
                op,
            }) => {
                governor_step! {
                    "promoting new replicaset master" [
                        "master_id" => %master_id,
                        "replicaset_id" => %replicaset_id,
                    ]
                    async {
                        pool.call(master_id, &rpc)?
                            .timeout(Duration::from_secs(3))
                            .await??
                    }
                }

                governor_step! {
                    "creating new replicaset" [
                        "replicaset_id" => %replicaset_id,
                    ]
                    async {
                        node.propose_and_wait(op, Duration::from_secs(3))??;
                    }
                }
            }

            Plan::Replication(Replication { targets, rpc, req }) => {
                governor_step! {
                    "configuring replication"
                    async {
                        let mut fs = vec![];
                        for instance_id in targets {
                            tlog!(Info, "calling rpc::replication"; "instance_id" => %instance_id);
                            let resp = pool.call(instance_id, &rpc)?;
                            fs.push(async move {
                                match resp.await {
                                    Ok(resp) => {
                                        tlog!(Info, "configured replication with instance";
                                            "instance_id" => %instance_id,
                                            "lsn" => resp.lsn,
                                        );
                                        Ok(())
                                    }
                                    Err(e) => {
                                        tlog!(Warning, "failed calling rpc::replication: {e}";
                                            "instance_id" => %instance_id
                                        );
                                        Err(e)
                                    }
                                }
                            });
                        }
                        // TODO: don't hard code timeout
                        try_join_all(fs).timeout(Duration::from_secs(3)).await??
                    }
                }

                let instance_id = req.instance_id.clone();
                let current_grade = req.current_grade.expect("must be set");
                governor_step! {
                    "handling instance grade change" [
                        "instance_id" => %instance_id,
                        "current_grade" => %current_grade,
                    ]
                    async {
                        node.handle_update_instance_request_and_wait(req)?
                    }
                }
            }

            Plan::ShardingInit(ShardingInit { targets, rpc, req }) => {
                governor_step! {
                    "configuring sharding"
                    async {
                        let mut fs = vec![];
                        for instance_id in targets {
                            tlog!(Info, "calling rpc::sharding"; "instance_id" => %instance_id);
                            let resp = pool.call(instance_id, &rpc)?;
                            fs.push(async move {
                                match resp.await {
                                    Ok(_) => {
                                        tlog!(Info, "configured sharding with instance";
                                            "instance_id" => %instance_id,
                                        );
                                        Ok(())
                                    }
                                    Err(e) => {
                                        tlog!(Warning, "failed calling rpc::sharding: {e}";
                                            "instance_id" => %instance_id
                                        );
                                        Err(e)
                                    }
                                }
                            });
                        }
                        // TODO: don't hard code timeout
                        try_join_all(fs).timeout(Duration::from_secs(3)).await??
                    }
                }

                let instance_id = req.instance_id.clone();
                let current_grade = req.current_grade.expect("must be set");
                governor_step! {
                    "handling instance grade change" [
                        "instance_id" => %instance_id,
                        "current_grade" => %current_grade,
                    ]
                    async {
                        node.handle_update_instance_request_and_wait(req)?
                    }
                }
            }

            Plan::None => {
                tlog!(Info, "nothing to do");
                did_something = false;
            }
        }

        if did_something {
            return Continue;
        }

        ////////////////////////////////////////////////////////////////////////
        // bootstrap sharding
        let to_bootstrap = get_first_full_replicaset(instances, storage);
        if let Err(e) = to_bootstrap {
            tlog!(
                Warning,
                "failed checking if bucket bootstrapping is needed: {e}"
            );
            // TODO: don't hard code timeout
            event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
            return Continue;
        }
        if let Ok(Some(Replicaset { master_id, .. })) = to_bootstrap {
            // TODO: change `Info` to `Debug`
            tlog!(Info, "bootstrapping bucket distribution";
                "instance_id" => %master_id,
            );
            let res: Result<_> = async {
                let req = sharding::bootstrap::Request {
                    term,
                    commit: raft_storage.commit()?.unwrap(),
                    timeout: Self::SYNC_TIMEOUT,
                };
                pool.call(&master_id, &req)?
                    // TODO: don't hard code timeout
                    .timeout(Duration::from_secs(3))
                    .await??;

                let op = OpDML::replace(
                    ClusterwideSpace::Property,
                    &(PropertyName::VshardBootstrapped, true),
                )?;
                // TODO: don't hard code timeout
                node.propose_and_wait(op, Duration::from_secs(3))??;

                Ok(())
            }
            .await;
            if let Err(e) = res {
                tlog!(Warning, "failed bootstrapping bucket distribution: {e}");
                // TODO: don't hard code timeout
                event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                return Continue;
            }

            // TODO: change `Info` to `Debug`
            tlog!(Info, "bootstrapped bucket distribution";
                "instance_id" => %master_id,
            );

            return Continue;
        };

        ////////////////////////////////////////////////////////////////////////
        // sharding weights
        let to_update_weights = instances.iter().find(|instance| {
            instance.has_grades(
                CurrentGradeVariant::ShardingInitialized,
                TargetGradeVariant::Online,
            )
        });
        if let Some(instance) = to_update_weights {
            let res = if let Some(added_weights) =
                get_weight_changes(maybe_responding(instances), storage)
            {
                async {
                    for (replicaset_id, weight) in added_weights {
                        let mut ops = UpdateOps::new();
                        ops.assign("weight", weight)?;
                        node.propose_and_wait(
                            OpDML::update(ClusterwideSpace::Replicaset, &[replicaset_id], ops)?,
                            // TODO: don't hard code the timeout
                            Duration::from_secs(3),
                        )??;
                    }

                    let instance_ids =
                        maybe_responding(instances).map(|instance| instance.instance_id.clone());
                    let commit = raft_storage.commit()?.unwrap();
                    let reqs = instance_ids.zip(repeat(sharding::Request {
                        term,
                        commit,
                        timeout: Self::SYNC_TIMEOUT,
                    }));
                    // TODO: don't hard code timeout
                    let res = call_all(pool, reqs, Duration::from_secs(3)).await?;

                    for (instance_id, resp) in res {
                        resp?;
                        // TODO: change `Info` to `Debug`
                        tlog!(Info, "instance is online"; "instance_id" => %instance_id);
                    }

                    let req =
                        update_instance::Request::new(instance.instance_id.clone(), cluster_id)
                            .with_current_grade(CurrentGrade::online(
                                instance.target_grade.incarnation,
                            ));
                    node.handle_update_instance_request_and_wait(req)?;
                    Ok(())
                }
                .await
            } else {
                (|| -> Result<()> {
                    let to_online = instances.iter().filter(|instance| {
                        instance.has_grades(
                            CurrentGradeVariant::ShardingInitialized,
                            TargetGradeVariant::Online,
                        )
                    });
                    for Instance {
                        instance_id,
                        target_grade,
                        ..
                    } in to_online
                    {
                        let cluster_id = cluster_id.clone();
                        let req = update_instance::Request::new(instance_id.clone(), cluster_id)
                            .with_current_grade(CurrentGrade::online(target_grade.incarnation));
                        node.handle_update_instance_request_and_wait(req)?;
                        // TODO: change `Info` to `Debug`
                        tlog!(Info, "instance is online"; "instance_id" => %instance_id);
                    }
                    Ok(())
                })()
            };
            if let Err(e) = res {
                tlog!(Warning, "updating sharding weights failed: {e}");

                // TODO: don't hard code timeout
                event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                return Continue;
            }

            tlog!(Info, "sharding is configured");

            return Continue;
        }

        ////////////////////////////////////////////////////////////////////////
        // applying migrations
        let desired_schema_version = storage.properties.desired_schema_version().unwrap();
        let replicasets = storage.replicasets.iter().unwrap().collect::<Vec<_>>();
        let mut migrations = storage.migrations.iter().unwrap().collect::<Vec<_>>();
        let commit = raft_storage.commit().unwrap().unwrap();
        for (mid, rids) in waiting_migrations(&mut migrations, &replicasets, desired_schema_version)
        {
            let migration = storage.migrations.get(mid).unwrap().unwrap();
            for rid in rids {
                let replicaset = storage
                    .replicasets
                    .get(rid.to_string().as_str())
                    .unwrap()
                    .unwrap();
                let instance = storage.instances.get(&replicaset.master_id).unwrap();
                let req = rpc::migration::apply::Request {
                    term,
                    commit,
                    timeout: Self::SYNC_TIMEOUT,
                    migration_id: migration.id,
                };
                let res: Result<_> = async {
                    let rpc::migration::apply::Response {} = pool
                        .call(&instance.raft_id, &req)?
                        // TODO: don't hard code timeout
                        .timeout(Duration::from_secs(3))
                        .await??;
                    let mut ops = UpdateOps::new();
                    ops.assign("current_schema_version", migration.id)?;
                    let op = OpDML::update(
                        ClusterwideSpace::Replicaset,
                        &[replicaset.replicaset_id.clone()],
                        ops,
                    )?;
                    node.propose_and_wait(op, Duration::MAX)??;
                    tlog!(
                        Info,
                        "Migration {0} applied to replicaset {1}",
                        migration.id,
                        replicaset.replicaset_id
                    );
                    Ok(())
                }
                .await;
                if let Err(e) = res {
                    tlog!(
                        Warning,
                        "Could not apply migration {0} to replicaset {1}, error: {2}",
                        migration.id,
                        replicaset.replicaset_id,
                        e
                    );
                    return Continue;
                }
            }
        }
        event::broadcast(Event::MigrateDone);

        event::wait_any(&[Event::TopologyChanged, Event::ClusterStateChanged])
            .expect("Events system must be initialized");

        Continue
    }
}

#[allow(clippy::too_many_arguments)]
fn action_plan<'i>(
    term: RaftTerm,
    commit: RaftIndex,
    cluster_id: String,
    instances: &'i [Instance],
    voters: &[RaftId],
    learners: &[RaftId],
    replicasets: &HashMap<&ReplicasetId, &'i Replicaset>,
    my_raft_id: RaftId,
    vshard_bootstrapped: bool,
) -> Result<Plan<'i>> {
    ////////////////////////////////////////////////////////////////////////////
    // conf change
    if let Some(conf_change) = raft_conf_change(instances, voters, learners) {
        return Ok(Plan::ConfChange(conf_change));
    }

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
        let op = OpDML::insert(
            ClusterwideSpace::Replicaset,
            &Replicaset {
                replicaset_id: replicaset_id.clone(),
                replicaset_uuid: replicaset_uuid.clone(),
                master_id: master_id.clone(),
                weight: if vshard_bootstrapped { 0. } else { 1. },
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
    Ok(Plan::None)
}

impl Loop {
    pub fn start(
        status: watch::Receiver<Status>,
        storage: Clusterwide,
        raft_storage: RaftSpaceAccess,
    ) -> Self {
        let args = Args {
            storage,
            raft_storage,
        };

        let state = State {
            status,
            pool: ConnectionPool::builder(args.storage.clone())
                .call_timeout(Duration::from_secs(1))
                .connect_timeout(Duration::from_millis(500))
                .inactivity_timeout(Duration::from_secs(60))
                .build(),
        };

        Self {
            _loop: crate::loop_start!("governor_loop", Self::iter_fn, args, state),
        }
    }
}

pub struct Loop {
    _loop: Option<fiber::UnitJoinHandle<'static>>,
}

struct Args {
    storage: Clusterwide,
    raft_storage: RaftSpaceAccess,
}

struct State {
    status: watch::Receiver<Status>,
    pool: ConnectionPool,
}

#[allow(clippy::type_complexity)]
async fn call_all<R, I>(
    pool: &mut ConnectionPool,
    reqs: impl IntoIterator<Item = (I, R)>,
    timeout: Duration,
) -> Result<Vec<(I, Result<R::Response>)>>
where
    R: rpc::Request,
    I: IdOfInstance + 'static,
{
    let reqs = reqs.into_iter().collect::<Vec<_>>();
    if reqs.is_empty() {
        return Ok(vec![]);
    }
    let mut fs = vec![];
    let mut ids = vec![];
    for (id, req) in reqs {
        fs.push(pool.call(&id, &req)?);
        ids.push(id);
    }
    let responses = join_all(fs).timeout(timeout).await?;
    Ok(ids.into_iter().zip(responses).collect())
}

#[inline(always)]
fn get_weight_changes<'p>(
    instances: impl IntoIterator<Item = &'p Instance>,
    storage: &Clusterwide,
) -> Option<ReplicasetWeights> {
    let replication_factor = storage
        .properties
        .replication_factor()
        .expect("storage error");
    let replicaset_weights = storage.replicasets.weights().expect("storage error");
    let mut replicaset_sizes = HashMap::new();
    let mut weight_changes = HashMap::new();
    for instance @ Instance { replicaset_id, .. } in instances {
        if !instance.may_respond() {
            continue;
        }
        let replicaset_size = replicaset_sizes.entry(replicaset_id.clone()).or_insert(0);
        *replicaset_size += 1;
        if *replicaset_size >= replication_factor && replicaset_weights[replicaset_id] == 0. {
            weight_changes.entry(replicaset_id.clone()).or_insert(1.);
        }
    }
    (!weight_changes.is_empty()).then_some(weight_changes)
}

#[inline(always)]
fn get_first_full_replicaset(
    instances: &[Instance],
    storage: &Clusterwide,
) -> Result<Option<Replicaset>> {
    if storage.properties.vshard_bootstrapped()? {
        return Ok(None);
    }

    let replication_factor = storage.properties.replication_factor()?;
    let mut replicaset_sizes = HashMap::new();
    let mut full_replicaset_id = None;
    for Instance { replicaset_id, .. } in maybe_responding(instances) {
        let replicaset_size = replicaset_sizes.entry(replicaset_id).or_insert(0);
        *replicaset_size += 1;
        if *replicaset_size >= replication_factor {
            full_replicaset_id = Some(replicaset_id);
        }
    }

    let Some(replicaset_id) = full_replicaset_id else { return Ok(None); };
    let res = storage.replicasets.get(replicaset_id)?;
    Ok(res)
}

#[inline(always)]
fn maybe_responding(instances: &[Instance]) -> impl Iterator<Item = &Instance> {
    instances.iter().filter(|instance| instance.may_respond())
}

mod actions {
    use super::*;
    use raft::prelude::ConfChangeV2;

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

    pub enum Plan<'i> {
        None,
        ConfChange(ConfChangeV2),
        TransferLeadership(TransferLeadership<'i>),
        TransferMastership(TransferMastership<'i>),
        ReconfigureShardingAndDowngrade(ReconfigureShardingAndDowngrade<'i>),
        RaftSync(RaftSync<'i>),
        CreateReplicaset(CreateReplicaset<'i>),
        Replication(Replication<'i>),
        ShardingInit(ShardingInit<'i>),
    }

    impl From<ConfChangeV2> for Plan<'_> {
        fn from(a: ConfChangeV2) -> Self {
            Self::ConfChange(a)
        }
    }

    impl<'i> From<TransferLeadership<'i>> for Plan<'i> {
        fn from(a: TransferLeadership<'i>) -> Self {
            Self::TransferLeadership(a)
        }
    }

    impl<'i> From<TransferMastership<'i>> for Plan<'i> {
        fn from(a: TransferMastership<'i>) -> Self {
            Self::TransferMastership(a)
        }
    }

    impl<'i> From<ReconfigureShardingAndDowngrade<'i>> for Plan<'i> {
        fn from(a: ReconfigureShardingAndDowngrade<'i>) -> Self {
            Self::ReconfigureShardingAndDowngrade(a)
        }
    }

    impl<'i> From<RaftSync<'i>> for Plan<'i> {
        fn from(a: RaftSync<'i>) -> Self {
            Self::RaftSync(a)
        }
    }

    impl<'i> From<CreateReplicaset<'i>> for Plan<'i> {
        fn from(a: CreateReplicaset<'i>) -> Self {
            Self::CreateReplicaset(a)
        }
    }

    impl<'i> From<Replication<'i>> for Plan<'i> {
        fn from(a: Replication<'i>) -> Self {
            Self::Replication(a)
        }
    }

    impl<'i> From<ShardingInit<'i>> for Plan<'i> {
        fn from(a: ShardingInit<'i>) -> Self {
            Self::ShardingInit(a)
        }
    }
}
