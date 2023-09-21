use std::collections::HashMap;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::time::Duration;

use ::tarantool::fiber;
use ::tarantool::fiber::r#async::timeout::Error as TimeoutError;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;
use ::tarantool::fiber::r#async::watch;

use crate::op::Op;
use crate::rpc;
use crate::rpc::update_instance::handle_update_instance_request_and_wait;
use crate::storage::Clusterwide;
use crate::storage::ToEntryIter as _;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::network::ConnectionPool;
use crate::traft::node::global;
use crate::traft::node::Status;
use crate::traft::raft_storage::RaftSpaceAccess;
use crate::traft::Result;
use crate::unwrap_ok_or;

use futures::future::try_join_all;

pub(crate) mod cc;
pub(crate) mod plan;

use plan::action_plan;
use plan::stage::*;

impl Loop {
    const RPC_TIMEOUT: Duration = Duration::from_secs(1);
    const SYNC_TIMEOUT: Duration = Duration::from_secs(10);
    const RETRY_TIMEOUT: Duration = Duration::from_millis(250);
    const UPDATE_INSTANCE_TIMEOUT: Duration = Duration::from_secs(3);

    async fn iter_fn(
        State {
            governor_status,
            storage,
            raft_storage,
            raft_status,
            waker,
            pool,
        }: &mut State,
    ) -> ControlFlow<()> {
        if !raft_status.get().raft_state.is_leader() {
            set_status(governor_status, "not a leader");
            raft_status.changed().await.unwrap();
            return ControlFlow::Continue(());
        }

        let instances = storage
            .instances
            .all_instances()
            .expect("storage should never fail");
        let peer_addresses: HashMap<_, _> = storage
            .peer_addresses
            .iter()
            .unwrap()
            .map(|pa| (pa.raft_id, pa.address))
            .collect();
        let voters = raft_storage.voters().expect("storage should never fail");
        let learners = raft_storage.learners().expect("storage should never fail");
        let replicasets: Vec<_> = storage
            .replicasets
            .iter()
            .expect("storage should never fail")
            .collect();
        let replicasets: HashMap<_, _> = replicasets
            .iter()
            .map(|rs| (&rs.replicaset_id, rs))
            .collect();
        let current_vshard_config = storage
            .properties
            .current_vshard_config()
            .expect("storage error");
        let target_vshard_config = storage
            .properties
            .target_vshard_config()
            .expect("storage error");

        let tiers: Vec<_> = storage
            .tiers
            .iter()
            .expect("storage should never fail")
            .collect();
        let tiers: HashMap<_, _> = tiers.iter().map(|tier| (&tier.name, tier)).collect();

        let term = raft_status.get().term;
        let applied = raft_storage.applied().expect("storage should never fail");
        let cluster_id = raft_storage
            .cluster_id()
            .expect("storage should never fail");
        let node = global().expect("must be initialized");
        let vshard_bootstrapped = storage
            .properties
            .vshard_bootstrapped()
            .expect("storage should never fail");
        let pending_schema_change = storage
            .properties
            .pending_schema_change()
            .expect("storage should never fail");
        let has_pending_schema_change = pending_schema_change.is_some();

        let plan = action_plan(
            term,
            applied,
            cluster_id,
            &instances,
            &peer_addresses,
            &voters,
            &learners,
            &replicasets,
            &tiers,
            node.raft_id,
            &current_vshard_config,
            &target_vshard_config,
            vshard_bootstrapped,
            has_pending_schema_change,
        );
        let plan = unwrap_ok_or!(plan,
            Err(e) => {
                tlog!(Warning, "failed constructing an action plan: {e}");
                waker.mark_seen();
                _ = waker.changed().timeout(Loop::RETRY_TIMEOUT).await;
                return ControlFlow::Continue(());
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
                    waker.mark_seen();
                    _ = waker.changed().timeout(Loop::RETRY_TIMEOUT).await;
                    return ControlFlow::Continue(());
                }
            }
        }

        match plan {
            Plan::ConfChange(ConfChange { conf_change }) => {
                set_status(governor_status, "conf change");
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
                set_status(governor_status, "transfer raft leader");
                tlog!(Info, "transferring leadership to {}", to.instance_id);
                node.transfer_leadership_and_yield(to.raft_id);
                _ = waker.changed().timeout(Loop::RETRY_TIMEOUT).await;
            }

            Plan::UpdateTargetReplicasetMaster(UpdateTargetReplicasetMaster { op }) => {
                set_status(governor_status, "update target replication leader");
                governor_step! {
                    "proposing replicaset target master change"
                    async {
                        node.propose_and_wait(op, Duration::from_secs(3))?;
                    }
                }
            }

            Plan::UpdateCurrentReplicasetMaster(UpdateCurrentReplicasetMaster {
                old_master_id,
                demote,
                new_master_id,
                mut sync_and_promote,
                replicaset_id,
                op,
            }) => {
                set_status(governor_status, "transfer replication leader");
                tlog!(
                    Info,
                    "transferring replicaset mastership from {old_master_id} to {new_master_id}"
                );

                if let Some(rpc) = demote {
                    governor_step! {
                        "demoting old master" [
                            "old_master_id" => %old_master_id,
                            "replicaset_id" => %replicaset_id,
                        ]
                        async {
                            let resp = pool.call(old_master_id, &rpc, Self::RPC_TIMEOUT)?
                                .timeout(Self::RPC_TIMEOUT)
                                .await?;
                            sync_and_promote.vclock = Some(resp.vclock);
                        }
                    }
                }

                governor_step! {
                    "promoting new master" [
                        "new_master_id" => %new_master_id,
                        "replicaset_id" => %replicaset_id,
                        "vclock" => ?sync_and_promote.vclock,
                    ]
                    async {
                        pool.call(new_master_id, &sync_and_promote, Self::SYNC_TIMEOUT)?
                            .timeout(Self::SYNC_TIMEOUT)
                            .await?
                    }
                }

                governor_step! {
                    "proposing replicaset current master change" [
                        "current_master_id" => %new_master_id,
                        "replicaset_id" => %replicaset_id,
                    ]
                    async {
                        node.propose_and_wait(op, Duration::from_secs(3))?
                    }
                }
            }

            Plan::Downgrade(Downgrade { req }) => {
                set_status(governor_status, "update instance grade to offline");
                tlog!(Info, "downgrading instance {}", req.instance_id);

                let instance_id = req.instance_id.clone();
                let current_grade = req.current_grade.expect("must be set");
                governor_step! {
                    "handling instance grade change" [
                        "instance_id" => %instance_id,
                        "current_grade" => %current_grade,
                    ]
                    async {
                        handle_update_instance_request_and_wait(req, Loop::UPDATE_INSTANCE_TIMEOUT)?
                    }
                }
            }

            Plan::CreateReplicaset(CreateReplicaset {
                master_id,
                replicaset_id,
                rpc,
                op,
            }) => {
                set_status(governor_status, "create new replicaset");
                governor_step! {
                    "promoting new replicaset master" [
                        "master_id" => %master_id,
                        "replicaset_id" => %replicaset_id,
                    ]
                    async {
                        pool.call(master_id, &rpc, Self::RPC_TIMEOUT)?
                            .timeout(Duration::from_secs(3))
                            .await?
                    }
                }

                governor_step! {
                    "creating new replicaset" [
                        "replicaset_id" => %replicaset_id,
                    ]
                    async {
                        node.propose_and_wait(op, Duration::from_secs(3))?;
                    }
                }
            }

            Plan::Replication(Replication {
                targets,
                master_id,
                replicaset_peers,
                req,
            }) => {
                set_status(governor_status, "configure replication");
                governor_step! {
                    "configuring replication"
                    async {
                        let mut fs = vec![];
                        let mut rpc = rpc::replication::Request {
                            is_master: false,
                            replicaset_peers,
                        };
                        for instance_id in targets {
                            tlog!(Info, "calling rpc::replication"; "instance_id" => %instance_id);
                            rpc.is_master = instance_id == master_id;
                            let resp = pool.call(instance_id, &rpc, Self::RPC_TIMEOUT)?;
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
                        try_join_all(fs).timeout(Duration::from_secs(3)).await?
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
                        handle_update_instance_request_and_wait(req, Loop::UPDATE_INSTANCE_TIMEOUT)?
                    }
                }
            }

            Plan::ShardingInit(ShardingInit { targets, rpc, req }) => {
                set_status(governor_status, "configure sharding");
                governor_step! {
                    "configuring sharding"
                    async {
                        let mut fs = vec![];
                        for instance_id in targets {
                            tlog!(Info, "calling rpc::sharding"; "instance_id" => %instance_id);
                            let resp = pool.call(instance_id, &rpc, Self::RPC_TIMEOUT)?;
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
                        try_join_all(fs).timeout(Duration::from_secs(3)).await?
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
                        handle_update_instance_request_and_wait(req, Loop::UPDATE_INSTANCE_TIMEOUT)?
                    }
                }
            }

            Plan::ShardingBoot(ShardingBoot { target, rpc, op }) => {
                set_status(governor_status, "bootstrap bucket distribution");
                governor_step! {
                    "bootstrapping bucket distribution" [
                        "instance_id" => %target,
                    ]
                    async {
                        pool
                            .call(target, &rpc, Self::SYNC_TIMEOUT)?
                            .timeout(Self::SYNC_TIMEOUT)
                            .await?;
                        node.propose_and_wait(op, Duration::from_secs(3))?
                    }
                }
            }

            Plan::ProposeReplicasetStateChanges(ProposeReplicasetStateChanges { op }) => {
                set_status(governor_status, "update replicaset state");
                governor_step! {
                    "proposing replicaset state change"
                    async {
                        node.propose_and_wait(op, Duration::from_secs(3))?;
                    }
                }
            }

            Plan::ToOnline(ToOnline { target, rpc, req }) => {
                set_status(governor_status, "update instance grade to online");
                if let Some(rpc) = rpc {
                    governor_step! {
                        "updating sharding config" [
                            "instance_id" => %target,
                        ]
                        async {
                            pool.call(target, &rpc, Self::RPC_TIMEOUT)?
                                .timeout(Duration::from_secs(3))
                                .await?
                        }
                    }
                }
                let current_grade = req.current_grade.expect("must be set");
                governor_step! {
                    "handling instance grade change" [
                        "instance_id" => %target,
                        "current_grade" => %current_grade,
                    ]
                    async {
                        handle_update_instance_request_and_wait(req, Loop::UPDATE_INSTANCE_TIMEOUT)?
                    }
                }
            }

            Plan::ApplySchemaChange(ApplySchemaChange { targets, rpc }) => {
                set_status(governor_status, "apply clusterwide schema change");
                let mut next_op = Op::Nop;
                governor_step! {
                    "applying pending schema change"
                    async {
                        let mut fs = vec![];
                        for instance_id in targets {
                            tlog!(Info, "calling proc_apply_schema_change"; "instance_id" => %instance_id);
                            let resp = pool.call(instance_id, &rpc, Self::RPC_TIMEOUT)?;
                            fs.push(async move {
                                match resp.await {
                                    Ok(rpc::ddl_apply::Response::Ok) => {
                                        tlog!(Info, "applied schema change on instance";
                                            "instance_id" => %instance_id,
                                        );
                                        Ok(())
                                    }
                                    Ok(rpc::ddl_apply::Response::Abort { reason }) => {
                                        tlog!(Error, "failed to apply schema change on instance: {reason}";
                                            "instance_id" => %instance_id,
                                        );
                                        Err(OnError::Abort)
                                    }
                                    Err(e) => {
                                        tlog!(Warning, "failed calling proc_apply_schema_change: {e}";
                                            "instance_id" => %instance_id
                                        );
                                        Err(OnError::Retry(e))
                                    }
                                }
                            });
                        }
                        // TODO: don't hard code timeout
                        let res = try_join_all(fs).timeout(Duration::from_secs(3)).await;
                        if let Err(TimeoutError::Failed(OnError::Abort)) = res {
                            next_op = Op::DdlAbort;
                            return Ok(());
                        }

                        res?;

                        next_op = Op::DdlCommit;

                        enum OnError { Retry(Error), Abort }
                        impl From<OnError> for Error {
                            fn from(e: OnError) -> Error {
                                match e {
                                    OnError::Retry(e) => e,
                                    OnError::Abort => Error::other("schema change was aborted"),
                                }
                            }
                        }
                    }
                }

                let op_name = next_op.to_string();
                governor_step! {
                    "finalizing schema change" [
                        "op" => &op_name,
                    ]
                    async {
                        assert!(matches!(next_op, Op::DdlAbort | Op::DdlCommit));
                        node.propose_and_wait(next_op, Duration::from_secs(3))?;
                    }
                }
            }

            Plan::UpdateTargetVshardConfig(UpdateTargetVshardConfig { dml }) => {
                set_status(governor_status, "update target sharding configuration");
                governor_step! {
                    "updating target vshard config"
                    async {
                        node.propose_and_wait(dml, Duration::from_secs(3))?;
                    }
                }
            }

            Plan::UpdateCurrentVshardConfig(UpdateCurrentVshardConfig { targets, rpc, dml }) => {
                set_status(governor_status, "update current sharding configuration");
                governor_step! {
                    "applying vshard config changes"
                    async {
                        let mut fs = vec![];
                        for instance_id in targets {
                            tlog!(Info, "calling rpc::sharding"; "instance_id" => %instance_id);
                            let resp = pool.call(instance_id, &rpc, Self::RPC_TIMEOUT)?;
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
                        try_join_all(fs).timeout(Duration::from_secs(3)).await?
                    }
                }

                governor_step! {
                    "updating current vshard config"
                    async {
                        node.propose_and_wait(dml, Duration::from_secs(3))?;
                    }
                }
            }

            Plan::None => {
                set_status(governor_status, "idle");
                tlog!(Info, "nothing to do, waiting for events to handle");
                waker.mark_seen();
                _ = waker.changed().await;
            }
        }

        ControlFlow::Continue(())
    }

    pub fn start(
        pool: Rc<ConnectionPool>,
        raft_status: watch::Receiver<Status>,
        storage: Clusterwide,
        raft_storage: RaftSpaceAccess,
    ) -> Self {
        let (waker_tx, waker_rx) = watch::channel(());
        let (governor_status_tx, governor_status_rx) = watch::channel(GovernorStatus {
            governor_loop_status: "initializing",
        });

        let state = State {
            governor_status: governor_status_tx,
            storage,
            raft_storage,
            raft_status,
            waker: waker_rx,
            pool,
        };

        Self {
            _loop: crate::loop_start!("governor_loop", Self::iter_fn, state),
            waker: waker_tx,
            status: governor_status_rx,
        }
    }

    pub fn wakeup(&self) -> Result<()> {
        self.waker.send(()).map_err(|_| Error::GovernorStopped)
    }
}

#[inline(always)]
fn set_status(status: &mut watch::Sender<GovernorStatus>, msg: &'static str) {
    if status.get().governor_loop_status == msg {
        return;
    }
    tlog!(Debug, "governor_loop_status = '{msg}'");
    status
        .send_modify(|s| s.governor_loop_status = msg)
        .expect("status shouldn't ever be borrowed across yields");
}

pub struct Loop {
    _loop: Option<fiber::JoinHandle<'static, ()>>,
    waker: watch::Sender<()>,

    /// Current status of governor loop.
    ///
    // XXX: maybe this shouldn't be a watch::Receiver, but it's not much worse
    // than a Rc, so ...
    pub status: watch::Receiver<GovernorStatus>,
}

struct State {
    governor_status: watch::Sender<GovernorStatus>,
    storage: Clusterwide,
    raft_storage: RaftSpaceAccess,
    raft_status: watch::Receiver<Status>,
    waker: watch::Receiver<()>,
    pool: Rc<ConnectionPool>,
}

#[derive(Debug, Clone, Copy)]
pub struct GovernorStatus {
    /// Current state of the governor loop.
    ///
    /// Is set by governor to explain the reason why it has yielded.
    pub governor_loop_status: &'static str,
}
