use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::iter::repeat;
use std::rc::Rc;
use std::time::Duration;

use ::tarantool::fiber;
use ::tarantool::fiber::r#async::watch;
use ::tarantool::space::UpdateOps;
use ::tarantool::util::IntoClones as _;

use crate::event::{self, Event};
use crate::r#loop::FlowControl::{self, Continue};
use crate::storage::{Clusterwide, ClusterwideSpace, PropertyName};
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::network::{ConnectionPool, IdOfInstance};
use crate::traft::node::global;
use crate::traft::node::Status;
use crate::traft::raft_storage::RaftSpaceAccess;
use crate::traft::rpc;
use crate::traft::rpc::sharding::cfg::ReplicasetWeights;
use crate::traft::rpc::{replication, sharding, sync, update_instance};
use crate::traft::OpDML;
use crate::traft::Result;
use crate::traft::{CurrentGrade, CurrentGradeVariant, TargetGradeVariant};
use crate::traft::{Instance, Replicaset};

pub(crate) mod cc;
pub(crate) mod migration;

pub(crate) use cc::raft_conf_change;
pub(crate) use migration::waiting_migrations;

impl Loop {
    const SYNC_TIMEOUT: Duration = Duration::from_secs(10);

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
        let term = status.get().term;
        let cluster_id = raft_storage.cluster_id().unwrap().unwrap();
        let node = global().expect("must be initialized");

        ////////////////////////////////////////////////////////////////////////
        // conf change
        let voters = raft_storage.voters().unwrap().unwrap_or_default();
        let learners = raft_storage.learners().unwrap().unwrap_or_default();
        if let Some(conf_change) = raft_conf_change(&instances, &voters, &learners) {
            // main_loop gives the warranty that every ProposeConfChange
            // will sometimes be handled and there's no need in timeout.
            // It also guarantees that the notification will arrive only
            // after the node leaves the joint state.
            tlog!(Info, "proposing conf_change"; "cc" => ?conf_change);
            if let Err(e) = node.propose_conf_change_and_wait(term, conf_change) {
                tlog!(Warning, "failed proposing conf_change: {e}");
                fiber::sleep(Duration::from_secs(1));
            }
            return Continue;
        }

        ////////////////////////////////////////////////////////////////////////
        // offline/expel
        let to_offline = instances
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
        if let Some(instance) = to_offline {
            tlog!(
                Info,
                "processing {} {} -> {}",
                instance.instance_id,
                instance.current_grade,
                instance.target_grade
            );

            // transfer leadership, if we're the one who goes offline
            if instance.raft_id == node.raft_id {
                if let Some(new_leader) = maybe_responding(&instances).find(|instance| {
                    // FIXME: linear search
                    voters.contains(&instance.raft_id)
                }) {
                    tlog!(
                        Info,
                        "transferring leadership to {}",
                        new_leader.instance_id
                    );
                    node.transfer_leadership_and_yield(new_leader.raft_id);
                    event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                    return Continue;
                }
            }

            let replicaset_id = &instance.replicaset_id;
            // choose a new replicaset master if needed
            let res = (|| -> Result<_> {
                let replicaset = storage.replicasets.get(replicaset_id)?;
                if replicaset
                    .map(|r| r.master_id == instance.instance_id)
                    .unwrap_or(false)
                {
                    let new_master =
                        maybe_responding(&instances).find(|p| p.replicaset_id == replicaset_id);
                    if let Some(instance) = new_master {
                        let mut ops = UpdateOps::new();
                        ops.assign("master_id", &instance.instance_id)?;

                        let op =
                            OpDML::update(ClusterwideSpace::Replicaset, &[replicaset_id], ops)?;
                        tlog!(Info, "proposing replicaset master change"; "op" => ?op);
                        // TODO: don't hard code the timeout
                        node.propose_and_wait(op, Duration::from_secs(3))??;
                    } else {
                        tlog!(Info, "skip proposing replicaset master change");
                    }
                }
                Ok(())
            })();
            if let Err(e) = res {
                tlog!(Warning, "failed proposing replicaset master change: {e}");
                // TODO: don't hard code timeout
                event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                return Continue;
            }

            // reconfigure vshard storages and routers
            let res = (|| -> Result<_> {
                let commit = raft_storage.commit()?.unwrap();
                let reqs = maybe_responding(&instances)
                    .filter(|instance| {
                        instance.current_grade == CurrentGradeVariant::ShardingInitialized
                            || instance.current_grade == CurrentGradeVariant::Online
                    })
                    .map(|instance| {
                        tlog!(Info,
                            "calling rpc::sharding";
                            "instance_id" => %instance.instance_id
                        );
                        (
                            instance.instance_id.clone(),
                            sharding::Request {
                                term,
                                commit,
                                timeout: Self::SYNC_TIMEOUT,
                                bootstrap: false,
                            },
                        )
                    });
                // TODO: don't hard code timeout
                let res = call_all(pool, reqs, Duration::from_secs(3))?;
                for (_, resp) in res {
                    let sharding::Response {} = resp?;
                }
                Ok(())
            })();
            if let Err(e) = res {
                tlog!(Warning, "failed calling rpc::sharding: {e}");
                // TODO: don't hard code timeout
                event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                return Continue;
            }

            // update instance's CurrentGrade
            let req = update_instance::Request::new(instance.instance_id.clone(), cluster_id)
                .with_current_grade(instance.target_grade.into());
            tlog!(Info,
                "handling update_instance::Request";
                "current_grade" => %req.current_grade.expect("just set"),
                "instance_id" => %req.instance_id,
            );
            if let Err(e) = node.handle_update_instance_request_and_wait(req) {
                tlog!(Warning,
                    "failed handling update_instance::Request: {e}";
                    "instance_id" => %instance.instance_id,
                );
                // TODO: don't hard code timeout
                event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                return Continue;
            }

            let replicaset_instances = storage
                .instances
                .replicaset_instances(replicaset_id)
                .expect("storage error")
                .filter(|instance| !instance.is_expelled())
                .collect::<Vec<_>>();
            let may_respond = replicaset_instances
                .iter()
                .filter(|instance| instance.may_respond());
            // Check if it makes sense to call box.ctl.promote,
            // otherwise we risk unpredictable delays
            if replicaset_instances.len() / 2 + 1 > may_respond.count() {
                tlog!(Warning,
                    "replicaset lost quorum";
                    "replicaset_id" => %replicaset_id,
                );
                return Continue;
            }

            let res = (|| -> Result<_> {
                // Promote the replication leader again
                // because of tarantool bugs
                if let Some(replicaset) = storage.replicasets.get(replicaset_id)? {
                    tlog!(Info,
                        "calling rpc::replication::promote";
                        "instance_id" => %replicaset.master_id
                    );
                    let commit = raft_storage.commit()?.unwrap();
                    pool.call_and_wait_timeout(
                        &replicaset.master_id,
                        replication::promote::Request {
                            term,
                            commit,
                            timeout: Self::SYNC_TIMEOUT,
                        },
                        // TODO: don't hard code timeout
                        Duration::from_secs(3),
                    )?;
                }
                Ok(())
            })();
            if let Err(e) = res {
                tlog!(Warning,
                    "failed calling rpc::replication::promote: {e}";
                    "replicaset_id" => %replicaset_id,
                );
            }

            return Continue;
        }

        ////////////////////////////////////////////////////////////////////////
        // raft sync
        // TODO: putting each stage in a different function
        // will make the control flow more readable
        let to_sync = instances.iter().find(|instance| {
            instance.has_grades(CurrentGradeVariant::Offline, TargetGradeVariant::Online)
                || instance.is_reincarnated()
        });
        if let Some(instance) = to_sync {
            let (rx, tx) = fiber::Channel::new(1).into_clones();
            let commit = raft_storage.commit().unwrap().unwrap();
            pool.call(
                &instance.raft_id,
                sync::Request {
                    commit,
                    timeout: Self::SYNC_TIMEOUT,
                },
                move |res| tx.send(res).expect("mustn't fail"),
            )
            .expect("shouldn't fail");
            let res = rx.recv().expect("ought not fail");
            let res = res.and_then(|sync::Response { commit }| {
                // TODO: change `Info` to `Debug`
                tlog!(Info, "instance synced";
                    "commit" => commit,
                    "instance_id" => &*instance.instance_id,
                );

                let req = update_instance::Request::new(instance.instance_id.clone(), cluster_id)
                    .with_current_grade(CurrentGrade::raft_synced(
                        instance.target_grade.incarnation,
                    ));
                global()
                    .expect("can't be deinitialized")
                    .handle_update_instance_request_and_wait(req)
            });
            match res {
                Ok(()) => {
                    tlog!(Info, "raft sync processed");
                }
                Err(e) => {
                    tlog!(Warning, "raft sync failed: {e}";
                        "instance_id" => %instance.instance_id,
                    );

                    // TODO: don't hard code timeout
                    event::wait_timeout(Event::TopologyChanged, Duration::from_millis(300))
                        .unwrap();
                }
            }

            return Continue;
        }

        ////////////////////////////////////////////////////////////////////////
        // replication
        let to_replicate = instances
            .iter()
            // TODO: find all such instances in a given replicaset,
            // not just the first one
            .find(|instance| {
                instance.has_grades(CurrentGradeVariant::RaftSynced, TargetGradeVariant::Online)
            });
        if let Some(instance) = to_replicate {
            let replicaset_id = &instance.replicaset_id;
            let replicaset_iids = maybe_responding(&instances)
                .filter(|instance| instance.replicaset_id == replicaset_id)
                .map(|instance| instance.instance_id.clone())
                .collect::<Vec<_>>();

            let res = (|| -> Result<_> {
                let commit = raft_storage.commit()?.unwrap();
                let reqs = replicaset_iids
                    .iter()
                    .cloned()
                    .zip(repeat(replication::Request {
                        term,
                        commit,
                        timeout: Self::SYNC_TIMEOUT,
                        replicaset_instances: replicaset_iids.clone(),
                        replicaset_id: replicaset_id.clone(),
                    }));
                // TODO: don't hard code timeout
                let res = call_all(pool, reqs, Duration::from_secs(3))?;

                for (instance_id, resp) in res {
                    let replication::Response { lsn } = resp?;
                    // TODO: change `Info` to `Debug`
                    tlog!(Info, "configured replication with instance";
                        "instance_id" => %instance_id,
                        "lsn" => lsn,
                    );
                }

                let req = update_instance::Request::new(instance.instance_id.clone(), cluster_id)
                    .with_current_grade(CurrentGrade::replicated(
                        instance.target_grade.incarnation,
                    ));
                node.handle_update_instance_request_and_wait(req)?;

                Ok(())
            })();
            if let Err(e) = res {
                tlog!(Warning, "failed to configure replication: {e}");
                // TODO: don't hard code timeout
                event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                return Continue;
            }

            let res = (|| -> Result<_> {
                let master_id =
                    if let Some(replicaset) = storage.replicasets.get(&instance.replicaset_id)? {
                        Cow::Owned(replicaset.master_id)
                    } else {
                        let vshard_bootstrapped = storage.state.vshard_bootstrapped()?;
                        let req = OpDML::insert(
                            ClusterwideSpace::Replicaset,
                            &Replicaset {
                                replicaset_id: instance.replicaset_id.clone(),
                                replicaset_uuid: instance.replicaset_uuid.clone(),
                                master_id: instance.instance_id.clone(),
                                weight: if vshard_bootstrapped { 0. } else { 1. },
                                current_schema_version: 0,
                            },
                        )?;
                        // TODO: don't hard code the timeout
                        node.propose_and_wait(req, Duration::from_secs(3))??;
                        Cow::Borrowed(&instance.instance_id)
                    };

                let commit = raft_storage.commit()?.unwrap();
                pool.call_and_wait_timeout(
                    &*master_id,
                    replication::promote::Request {
                        term,
                        commit,
                        timeout: Self::SYNC_TIMEOUT,
                    },
                    // TODO: don't hard code timeout
                    Duration::from_secs(3),
                )?;
                tlog!(Debug, "promoted replicaset master";
                    "instance_id" => %master_id,
                    "replicaset_id" => %instance.replicaset_id,
                );
                Ok(())
            })();
            if let Err(e) = res {
                tlog!(Warning, "failed to promote replicaset master: {e}";
                    "replicaset_id" => %replicaset_id,
                );
            }

            tlog!(Info, "configured replication"; "replicaset_id" => %replicaset_id);

            return Continue;
        }

        ////////////////////////////////////////////////////////////////////////
        // init sharding
        let to_shard = instances.iter().find(|instance| {
            instance.has_grades(CurrentGradeVariant::Replicated, TargetGradeVariant::Online)
        });
        if let Some(instance) = to_shard {
            let res = (|| -> Result<()> {
                let vshard_bootstrapped = storage.state.vshard_bootstrapped()?;
                let commit = raft_storage.commit()?.unwrap();
                let reqs = maybe_responding(&instances).map(|instance| {
                    (
                        instance.instance_id.clone(),
                        sharding::Request {
                            term,
                            commit,
                            timeout: Self::SYNC_TIMEOUT,
                            bootstrap: !vshard_bootstrapped && instance.raft_id == node.raft_id,
                        },
                    )
                });
                // TODO: don't hard code timeout
                let res = call_all(pool, reqs, Duration::from_secs(3))?;

                for (instance_id, resp) in res {
                    let sharding::Response {} = resp?;

                    // TODO: change `Info` to `Debug`
                    tlog!(Info, "initialized sharding with instance";
                        "instance_id" => %instance_id,
                    );
                }

                let req = update_instance::Request::new(instance.instance_id.clone(), cluster_id)
                    .with_current_grade(CurrentGrade::sharding_initialized(
                        instance.target_grade.incarnation,
                    ));
                node.handle_update_instance_request_and_wait(req)?;

                if !vshard_bootstrapped {
                    // TODO: if this fails, it will only rerun next time vshard
                    // gets reconfigured
                    node.propose_and_wait(
                        OpDML::replace(
                            ClusterwideSpace::Property,
                            &(PropertyName::VshardBootstrapped, true),
                        )?,
                        // TODO: don't hard code the timeout
                        Duration::from_secs(3),
                    )??;
                }

                Ok(())
            })();
            if let Err(e) = res {
                tlog!(Warning, "failed to initialize sharding: {e}");
                // TODO: don't hard code timeout
                event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                return Continue;
            }

            let res = (|| -> Result<()> {
                // Promote the replication leaders again
                // because of tarantool bugs
                let replicasets = storage.replicasets.iter()?;
                let masters = replicasets.map(|r| r.master_id).collect::<HashSet<_>>();
                let commit = raft_storage.commit()?.unwrap();
                let reqs = maybe_responding(&instances)
                    .filter(|instance| masters.contains(&instance.instance_id))
                    .map(|instance| instance.instance_id.clone())
                    .zip(repeat(replication::promote::Request {
                        term,
                        commit,
                        timeout: Self::SYNC_TIMEOUT,
                    }));
                // TODO: don't hard code timeout
                let res = call_all(pool, reqs, Duration::from_secs(3))?;
                for (instance_id, resp) in res {
                    resp?;
                    tlog!(Debug, "promoted replicaset master"; "instance_id" => %instance_id);
                }
                Ok(())
            })();
            if let Err(e) = res {
                tlog!(Warning, "failed to promote replicaset masters: {e}");
            }

            tlog!(Info, "sharding is initialized");

            return Continue;
        }

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
                get_weight_changes(maybe_responding(&instances), storage)
            {
                (|| -> Result<()> {
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
                        maybe_responding(&instances).map(|instance| instance.instance_id.clone());
                    let commit = raft_storage.commit()?.unwrap();
                    let reqs = instance_ids.zip(repeat(sharding::Request {
                        term,
                        commit,
                        timeout: Self::SYNC_TIMEOUT,
                        bootstrap: false,
                    }));
                    // TODO: don't hard code timeout
                    let res = call_all(pool, reqs, Duration::from_secs(3))?;

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
                })()
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
        let desired_schema_version = storage.state.desired_schema_version().unwrap();
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
                let res = pool.call_and_wait(&instance.raft_id, req);
                match res {
                    Ok(_) => {
                        let mut ops = UpdateOps::new();
                        ops.assign("current_schema_version", migration.id).unwrap();
                        let op = OpDML::update(
                            ClusterwideSpace::Replicaset,
                            &[replicaset.replicaset_id.clone()],
                            ops,
                        )
                        .unwrap();
                        node.propose_and_wait(op, Duration::MAX).unwrap().unwrap();
                        tlog!(
                            Info,
                            "Migration {0} applied to replicaset {1}",
                            migration.id,
                            replicaset.replicaset_id
                        );
                    }
                    Err(e) => {
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
        }
        event::broadcast(Event::MigrateDone);

        event::wait_any(&[Event::TopologyChanged, Event::ClusterStateChanged])
            .expect("Events system must be initialized");

        Continue
    }

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
fn call_all<R, I>(
    pool: &mut ConnectionPool,
    reqs: impl IntoIterator<Item = (I, R)>,
    timeout: Duration,
) -> Result<Vec<(I, Result<R::Response>)>>
where
    R: rpc::Request,
    I: IdOfInstance + 'static,
{
    // TODO: this crap is only needed to wait until results of all
    // the calls are ready. There are several ways to rafactor this:
    // - we could use a std-style channel that unblocks the reading end
    //   once all the writing ends have dropped
    //   (fiber::Channel cannot do that for now)
    // - using the std Futures we could use futures::join!
    //
    // Those things aren't implemented yet, so this is what we do
    let reqs = reqs.into_iter().collect::<Vec<_>>();
    if reqs.is_empty() {
        return Ok(vec![]);
    }
    static mut SENT_COUNT: usize = 0;
    unsafe { SENT_COUNT = 0 };
    let (cond_rx, cond_tx) = Rc::new(fiber::Cond::new()).into_clones();
    let instance_count = reqs.len();
    let (rx, tx) = fiber::Channel::new(instance_count as _).into_clones();
    for (id, req) in reqs {
        let tx = tx.clone();
        let cond_tx = cond_tx.clone();
        let id_copy = id.clone();
        pool.call(&id, req, move |res| {
            tx.send((id_copy, res)).expect("mustn't fail");
            unsafe { SENT_COUNT += 1 };
            if unsafe { SENT_COUNT } == instance_count {
                cond_tx.signal()
            }
        })
        .expect("shouldn't fail");
    }
    // TODO: don't hard code timeout
    if !cond_rx.wait_timeout(timeout) {
        return Err(Error::Timeout);
    }

    Ok(rx.into_iter().take(instance_count).collect())
}

#[inline(always)]
fn get_weight_changes<'p>(
    instances: impl IntoIterator<Item = &'p Instance>,
    storage: &Clusterwide,
) -> Option<ReplicasetWeights> {
    let replication_factor = storage.state.replication_factor().expect("storage error");
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
fn maybe_responding(instances: &[Instance]) -> impl Iterator<Item = &Instance> {
    instances.iter().filter(|instance| instance.may_respond())
}
