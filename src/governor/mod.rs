use std::collections::HashMap;
use std::time::Duration;

use ::tarantool::fiber;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;
use ::tarantool::fiber::r#async::watch;

use crate::event::{self, Event};
use crate::r#loop::FlowControl::{self, Continue};
use crate::storage::Clusterwide;
use crate::storage::ToEntryIter as _;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::network::ConnectionPool;
use crate::traft::node::global;
use crate::traft::node::Status;
use crate::traft::raft_storage::RaftSpaceAccess;
use crate::traft::rpc::sync;
use crate::traft::Instance;
use crate::traft::Result;
use crate::unwrap_ok_or;

use futures::future::try_join_all;

pub(crate) mod cc;
pub(crate) mod migration;
pub(crate) mod plan;

use plan::action_plan;
use plan::stage::*;

impl Loop {
    const SYNC_TIMEOUT: Duration = Duration::from_secs(10);
    const RETRY_TIMEOUT: Duration = Duration::from_millis(250);

    async fn iter_fn(
        Args {
            storage,
            raft_storage,
        }: &Args,
        State {
            status,
            waker,
            pool,
        }: &mut State,
    ) -> FlowControl {
        if !status.get().raft_state.is_leader() {
            status.changed().await.unwrap();
            return Continue;
        }

        let instances = storage.instances.all_instances().unwrap();
        let voters = raft_storage.voters().unwrap().unwrap_or_default();
        let learners = raft_storage.learners().unwrap().unwrap_or_default();
        let replicasets: Vec<_> = storage.replicasets.iter().unwrap().collect();
        let replicasets: HashMap<_, _> = replicasets
            .iter()
            .map(|rs| (&rs.replicaset_id, rs))
            .collect();
        let migration_ids = storage.migrations.iter().unwrap().map(|m| m.id).collect();

        let term = status.get().term;
        let commit = raft_storage.commit().unwrap().unwrap();
        let cluster_id = raft_storage.cluster_id().unwrap().unwrap();
        let node = global().expect("must be initialized");
        let vshard_bootstrapped = storage.properties.vshard_bootstrapped().unwrap();
        let replication_factor = storage.properties.replication_factor().unwrap();
        let desired_schema_version = storage.properties.desired_schema_version().unwrap();

        let plan = action_plan(
            term,
            commit,
            cluster_id,
            &instances,
            &voters,
            &learners,
            &replicasets,
            migration_ids,
            node.raft_id,
            vshard_bootstrapped,
            replication_factor,
            desired_schema_version,
        );
        let plan = unwrap_ok_or!(plan,
            Err(e) => {
                tlog!(Warning, "failed constructing an action plan: {e}");
                _ = waker.changed().timeout(Loop::RETRY_TIMEOUT).await;
                return Continue;
            }
        );

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
                    // TODO: better api needed in library
                    if waker.has_changed() {
                        // This resolves immediately
                        _ = waker.changed().await;
                    }
                    _ = waker.changed().timeout(Loop::RETRY_TIMEOUT).await;
                    return Continue;
                }
            }
        }

        match plan {
            Plan::ConfChange(ConfChange { conf_change }) => {
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
                _ = waker.changed().timeout(Loop::RETRY_TIMEOUT).await;
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

            Plan::ShardingBoot(ShardingBoot { target, rpc, op }) => {
                governor_step! {
                    "bootstrapping bucket distribution" [
                        "instance_id" => %target,
                    ]
                    async {
                        pool
                            .call(target, &rpc)?
                            .timeout(Loop::SYNC_TIMEOUT)
                            .await??;
                        node.propose_and_wait(op, Duration::from_secs(3))??
                    }
                }
            }

            Plan::TargetWeights(TargetWeights { ops }) => {
                for op in ops {
                    governor_step! {
                        "proposing target replicaset weights change"
                        async {
                            node.propose_and_wait(op, Duration::from_secs(3))??;
                        }
                    }
                }
            }

            Plan::CurrentWeights(CurrentWeights { targets, rpc, ops }) => {
                governor_step! {
                    "updating sharding weights"
                    async {
                        let mut fs = vec![];
                        for instance_id in targets {
                            tlog!(Info, "calling rpc::sharding"; "instance_id" => %instance_id);
                            let resp = pool.call(instance_id, &rpc)?;
                            fs.push(async move {
                                match resp.await {
                                    Ok(_) => {
                                        tlog!(Info, "updated weights on instance";
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

                for op in ops {
                    governor_step! {
                        "proposing current replicaset weights change"
                        async {
                            node.propose_and_wait(op, Duration::from_secs(3))??;
                        }
                    }
                }
            }

            Plan::ToOnline(ToOnline { req }) => {
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

            Plan::ApplyMigration(ApplyMigration { target, rpc, op }) => {
                let migration_id = rpc.migration_id;
                governor_step! {
                    "applying migration on a replicaset" [
                        "replicaset_id" => %target.replicaset_id,
                        "migration_id" => %migration_id,
                    ]
                    async {
                        pool
                            .call(&target.master_id, &rpc)?
                            .timeout(Loop::SYNC_TIMEOUT)
                            .await??;
                    }
                }

                governor_step! {
                    "proposing replicaset current schema version change" [
                        "replicaset_id" => %target.replicaset_id,
                        "schema_version" => %migration_id,
                    ]
                    async {
                        node.propose_and_wait(op, Duration::from_secs(3))??
                    }
                }

                event::broadcast(Event::MigrateDone);
            }

            Plan::None => {
                tlog!(Info, "nothing to do, waiting for events to handle");
                _ = waker.changed().await;
            }
        }

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

        let (waker_tx, waker_rx) = watch::channel(());

        let state = State {
            status,
            waker: waker_rx,
            pool: ConnectionPool::builder(args.storage.clone())
                .call_timeout(Duration::from_secs(1))
                .connect_timeout(Duration::from_millis(500))
                .inactivity_timeout(Duration::from_secs(60))
                .build(),
        };

        Self {
            _loop: crate::loop_start!("governor_loop", Self::iter_fn, args, state),
            waker: waker_tx,
        }
    }

    pub fn wakeup(&self) -> Result<()> {
        self.waker.send(()).map_err(|_| Error::GovernorStopped)
    }

    pub async fn awoken(&self) -> Result<()> {
        self.waker
            .subscribe()
            .changed()
            .await
            .map_err(|_| Error::GovernorStopped)
    }
}

pub struct Loop {
    _loop: Option<fiber::UnitJoinHandle<'static>>,
    waker: watch::Sender<()>,
}

struct Args {
    storage: Clusterwide,
    raft_storage: RaftSpaceAccess,
}

struct State {
    status: watch::Receiver<Status>,
    waker: watch::Receiver<()>,
    pool: ConnectionPool,
}
