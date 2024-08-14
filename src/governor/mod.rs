use std::collections::HashMap;
use std::ops::{Add, ControlFlow};
use std::rc::Rc;
use std::time::Duration;

use ::tarantool::fiber;
use ::tarantool::fiber::r#async::timeout::Error as TimeoutError;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;
use ::tarantool::fiber::r#async::watch;

use crate::op::Op;
use crate::plugin::PluginOp;
use crate::proc_name;
use crate::rpc;
use crate::rpc::ddl_apply::proc_apply_schema_change;
use crate::rpc::disable_service::proc_disable_service;
use crate::rpc::enable_all_plugins::proc_enable_all_plugins;
use crate::rpc::enable_plugin::proc_enable_plugin;
use crate::rpc::enable_service::proc_enable_service;
use crate::rpc::load_plugin_dry_run::proc_load_plugin_dry_run;
use crate::rpc::replication::proc_replication;
use crate::rpc::replication::proc_replication_demote;
use crate::rpc::replication::proc_replication_promote;
use crate::rpc::sharding::bootstrap::proc_sharding_bootstrap;
use crate::rpc::sharding::proc_sharding;
use crate::rpc::update_instance::handle_update_instance_request_in_governor_and_also_wait_too;
use crate::storage::Clusterwide;
use crate::storage::ToEntryIter as _;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::error::ErrorInfo;
use crate::traft::network::ConnectionPool;
use crate::traft::node::global;
use crate::traft::node::Status;
use crate::traft::op::PluginRaftOp;
use crate::traft::raft_storage::RaftSpaceAccess;
use crate::traft::Result;
use crate::unwrap_ok_or;
use plan::action_plan;
use plan::stage::*;
use plan::EnablePluginConfig;
use plan::PreparedPluginOp;

use futures::future::try_join_all;

pub(crate) mod cc;
pub(crate) mod plan;

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
        let current_vshard_config_version = storage
            .properties
            .current_vshard_config_version()
            .expect("storage error");
        let target_vshard_config_version = storage
            .properties
            .target_vshard_config_version()
            .expect("storage error");

        let tiers: Vec<_> = storage
            .tiers
            .iter()
            .expect("storage should never fail")
            .collect();
        let tiers: HashMap<_, _> = tiers
            .iter()
            .map(|tier| (tier.name.as_str(), tier))
            .collect();

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

        let plugins: HashMap<_, _> = storage
            .plugins
            .iter()
            .expect("storage should never fail")
            .map(|plugin_def| (plugin_def.identifier(), plugin_def))
            .collect();
        let plugin_op = get_info_for_pending_plugin_op(storage);

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
            current_vshard_config_version,
            target_vshard_config_version,
            vshard_bootstrapped,
            has_pending_schema_change,
            &plugins,
            &plugin_op,
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

        #[derive(Debug)]
        enum OnError {
            Retry(Error),
            Abort(ErrorInfo),
        }
        impl From<OnError> for Error {
            fn from(e: OnError) -> Error {
                match e {
                    OnError::Retry(e) => e,
                    OnError::Abort(_) => unreachable!("we never convert Abort to Error"),
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
                vshard_config_version_bump,
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
                            let resp = pool.call(old_master_id, proc_name!(proc_replication_demote), &rpc, Self::RPC_TIMEOUT)?
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
                        pool.call(new_master_id, proc_name!(proc_replication_promote), &sync_and_promote, Self::SYNC_TIMEOUT)?
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
                        let op = match vshard_config_version_bump {
                            Some(vshard_config_version_bump) => Op::BatchDml { ops: vec![op, vshard_config_version_bump] },
                            None => Op::Dml(op),
                        };
                        node.propose_and_wait(op, Duration::from_secs(3))?
                    }
                }
            }

            Plan::Downgrade(Downgrade {
                req,
                vshard_config_version_bump,
            }) => {
                set_status(governor_status, "update instance state to offline");
                tlog!(Info, "downgrading instance {}", req.instance_id);

                let instance_id = req.instance_id.clone();
                let current_state = req.current_state.expect("must be set");
                governor_step! {
                    "handling instance state change" [
                        "instance_id" => %instance_id,
                        "current_state" => %current_state,
                    ]
                    async {
                        handle_update_instance_request_in_governor_and_also_wait_too(
                            req,
                            vshard_config_version_bump.as_ref(),
                            Loop::UPDATE_INSTANCE_TIMEOUT,
                        )?
                    }
                }
            }

            Plan::Replication(Replication {
                targets,
                master_id,
                replicaset_peers,
                req,
                vshard_config_version_bump,
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
                            let resp = pool.call(instance_id, proc_name!(proc_replication), &rpc, Self::RPC_TIMEOUT)?;
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
                let current_state = req.current_state.expect("must be set");
                governor_step! {
                    "handling instance state change" [
                        "instance_id" => %instance_id,
                        "current_state" => %current_state,
                    ]
                    async {
                        handle_update_instance_request_in_governor_and_also_wait_too(
                            req,
                            vshard_config_version_bump.as_ref(),
                            Loop::UPDATE_INSTANCE_TIMEOUT,
                        )?
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
                            .call(target, proc_name!(proc_sharding_bootstrap), &rpc, Self::SYNC_TIMEOUT)?
                            .timeout(Self::SYNC_TIMEOUT)
                            .await?;
                        node.propose_and_wait(op, Duration::from_secs(3))?
                    }
                }
            }

            Plan::ProposeReplicasetStateChanges(ProposeReplicasetStateChanges {
                op,
                vshard_config_version_bump,
            }) => {
                set_status(governor_status, "update replicaset state");
                governor_step! {
                    "proposing replicaset state change"
                    async {
                        let op = match vshard_config_version_bump {
                            Some(vshard_config_version_bump) => Op::BatchDml { ops: vec![op, vshard_config_version_bump] },
                            None => Op::Dml(op),
                        };
                        node.propose_and_wait(op, Duration::from_secs(3))?;
                    }
                }
            }

            Plan::ToOnline(ToOnline {
                target,
                rpc,
                plugin_rpc,
                req,
            }) => {
                set_status(governor_status, "update instance state to online");
                if let Some(rpc) = rpc {
                    governor_step! {
                        "updating sharding config" [
                            "instance_id" => %target,
                        ]
                        async {
                            pool.call(target, proc_name!(proc_sharding), &rpc, Self::RPC_TIMEOUT)?
                                .timeout(Duration::from_secs(3))
                                .await?
                        }
                    }
                }

                governor_step! {
                    "enable plugins on instance" [
                        "instance_id" => %target,
                    ]
                    async {
                        pool.call(target, proc_name!(proc_enable_all_plugins), &plugin_rpc, Self::RPC_TIMEOUT)?
                            // TODO looks like we need a big timeout here
                            .timeout(Duration::from_secs(10))
                            .await?
                    }
                }

                let current_state = req.current_state.expect("must be set");
                governor_step! {
                    "handling instance state change" [
                        "instance_id" => %target,
                        "current_state" => %current_state,
                    ]
                    async {
                        handle_update_instance_request_in_governor_and_also_wait_too(req, None, Loop::UPDATE_INSTANCE_TIMEOUT)?
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
                            let resp = pool.call(instance_id, proc_name!(proc_apply_schema_change), &rpc, Self::RPC_TIMEOUT)?;
                            fs.push(async move {
                                match resp.await {
                                    Ok(rpc::ddl_apply::Response::Ok) => {
                                        tlog!(Info, "applied schema change on instance";
                                            "instance_id" => %instance_id,
                                        );
                                        Ok(())
                                    }
                                    Ok(rpc::ddl_apply::Response::Abort { cause }) => {
                                        tlog!(Error, "failed to apply schema change on instance: {cause}";
                                            "instance_id" => %instance_id,
                                        );
                                        Err(OnError::Abort(cause))
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
                        if let Err(TimeoutError::Failed(OnError::Abort(cause))) = res {
                            next_op = Op::DdlAbort { cause };
                            return Ok(());
                        }

                        res?;

                        next_op = Op::DdlCommit;
                    }
                }

                let op_name = next_op.to_string();
                governor_step! {
                    "finalizing schema change" [
                        "op" => &op_name,
                    ]
                    async {
                        assert!(matches!(next_op, Op::DdlAbort { .. } | Op::DdlCommit));
                        node.propose_and_wait(next_op, Duration::from_secs(3))?;
                    }
                }
            }

            Plan::InstallPlugin(InstallPlugin {
                targets,
                rpc,
                mut success_ops,
                cleanup_op,
            }) => {
                set_status(governor_status, "install new plugin");

                let mut ops = vec![cleanup_op];
                governor_step! {
                    "checking if plugin is ready for installation on instances"
                    async {
                        let mut fs = vec![];
                        for instance_id in targets {
                            tlog!(Info, "calling proc_load_plugin_dry_run"; "instance_id" => %instance_id);
                            let resp = pool.call(instance_id, proc_name!(proc_load_plugin_dry_run), &rpc, Duration::from_secs(5))?;
                            fs.push(async move {
                                match resp.await {
                                    Ok(_) => {
                                        tlog!(Info, "instance is ready to install plugin";
                                            "instance_id" => %instance_id,
                                        );
                                        Ok(())
                                    }
                                    Err(e) => {
                                        tlog!(Error, "failed to call proc_load_plugin_dry_run: {e}";
                                            "instance_id" => %instance_id
                                        );
                                        Err(e)
                                    }
                                }
                            });
                        }

                        if let Err(e) = try_join_all(fs).timeout(Duration::from_secs(5)).await {
                            tlog!(Error, "Plugin installation aborted: {e}");
                            return Ok(());
                        }

                        ops.append(&mut success_ops);
                    }
                }

                let op = Op::BatchDml { ops };
                governor_step! {
                    "finalizing plugin installing"
                    async {
                        node.propose_and_wait(op, Duration::from_secs(3))?;
                    }
                }
            }

            Plan::EnablePlugin(EnablePlugin {
                targets,
                rpc,
                ident,
                on_start_timeout,
                success_dml,
            }) => {
                set_status(governor_status, "enable plugin");
                let mut next_op = None;

                governor_step! {
                    "enabling plugin"
                    async {
                        let mut fs = vec![];
                        for &instance_id in &targets {
                            tlog!(Info, "calling enable_plugin"; "instance_id" => %instance_id);
                            let resp = pool.call(instance_id, proc_name!(proc_enable_plugin), &rpc, on_start_timeout)?;
                            fs.push(async move {
                                match resp.await {
                                    Ok(rpc::enable_plugin::Response::Ok) => {
                                        tlog!(Info, "enable plugin on instance"; "instance_id" => %instance_id);
                                        Ok(())
                                    }
                                    Ok(rpc::enable_plugin::Response::Abort { cause }) => {
                                        tlog!(Error, "failed to enable plugin at instance: {cause}";
                                            "instance_id" => %instance_id,
                                        );
                                        Err(OnError::Abort(cause))
                                    }
                                    Err(Error::Timeout) => {
                                        tlog!(Error, "failed to enable plugin at instance: timeout";
                                            "instance_id" => %instance_id,
                                        );
                                        Err(OnError::Abort(ErrorInfo::timeout(instance_id.clone(), "no response")))
                                    }
                                    Err(e) => {
                                        tlog!(Warning, "failed calling proc_load_plugin: {e}";
                                            "instance_id" => %instance_id
                                        );
                                        Err(OnError::Retry(e))
                                    }
                                }
                            });
                        }

                        let enable_result = try_join_all(fs).timeout(on_start_timeout.add(Duration::from_secs(1))).await;
                        if let Err(TimeoutError::Failed(OnError::Abort(cause))) = enable_result {
                            let rollback_op = PluginRaftOp::DisablePlugin {
                                ident: ident.clone(),
                                cause: Some(cause),
                            };
                            next_op = Some(Op::Plugin(rollback_op));
                            return Ok(());
                        }

                        // Return error if this is a retriable error
                        enable_result?;

                        next_op = Some(success_dml);
                    }
                }

                governor_step! {
                    "finalizing plugin enabling"
                    async {
                        let op = next_op.expect("is set on the first substep");
                        node.propose_and_wait(op, Duration::from_secs(3))?;
                    }
                }
            }

            Plan::UpdatePluginTopology(UpdatePluginTopology {
                enable_targets,
                disable_targets,
                enable_rpc,
                disable_rpc,
                success_dml,
                finalize_dml,
            }) => {
                set_status(governor_status, "update plugin service topology");
                let mut next_ops = vec![finalize_dml];

                // FIXME: this step is overcomplicated and there's probably some
                // corner cases in which it may lead to inconsistent state.
                // For example in case of network partition it may lead to
                // services being enabled on instances for which the corresponding
                // records in _pico_service_route show otherwise.
                // Perhaps it's easier to fix these types of issues by
                // introducing the plugin healthcheck system, but it's
                // nevertheless concerning that there could be cases where this
                // type of inconsistency could lead to some scary things.
                governor_step! {
                    "enabling/disabling service at new tiers"
                    async {
                        let mut fs = vec![];
                        for &instance_id in &enable_targets {
                            tlog!(Info, "calling proc_enable_service"; "instance_id" => %instance_id);
                            let resp = pool.call(instance_id, proc_name!(proc_enable_service), &enable_rpc, Duration::from_secs(5))?;
                            fs.push(async move {
                                match resp.await {
                                    Ok(_) => {
                                        tlog!(Info, "instance enable service"; "instance_id" => %instance_id);
                                        Ok(())
                                    }
                                    Err(e) => {
                                        tlog!(Error, "failed to call proc_enable_service: {e}";
                                            "instance_id" => %instance_id
                                        );
                                        Err(e)
                                    }
                                }
                            });
                        }

                        if let Err(e) = try_join_all(fs).timeout(Duration::from_secs(5)).await {
                            tlog!(Error, "Enabling plugins fail with: {e}, rollback and abort");
                            // try to disable plugins at all instances
                            // where it was enabled previously
                            let mut fs = vec![];
                            for instance_id in enable_targets {
                                let resp = pool.call(instance_id, proc_name!(proc_disable_service), &disable_rpc, Duration::from_secs(5))?;
                                fs.push(resp);
                            }
                            _ = try_join_all(fs).timeout(Duration::from_secs(5)).await;
                            return Ok(());
                        }

                        let mut fs = vec![];
                        for instance_id in disable_targets {
                            tlog!(Info, "calling proc_disable_service"; "instance_id" => %instance_id);
                            let resp = pool.call(instance_id, proc_name!(proc_disable_service), &disable_rpc, Duration::from_secs(5))?;
                            fs.push(resp);
                        }
                        try_join_all(fs).timeout(Duration::from_secs(5)).await?;

                        next_ops.extend(success_dml);
                    }
                }

                governor_step! {
                    "finalizing topology update"
                    async {
                        let op = Op::BatchDml {
                            ops: next_ops,
                        };
                        node.propose_and_wait(op, Duration::from_secs(3))?;
                    }
                }
            }

            Plan::UpdateCurrentVshardConfig(UpdateCurrentVshardConfig {
                targets,
                rpc,
                dml,
                vshard_config_version_actualize,
            }) => {
                set_status(governor_status, "update current sharding configuration");
                governor_step! {
                    "applying vshard config changes"
                    async {
                        let mut fs = vec![];
                        for instance_id in targets {
                            tlog!(Info, "calling proc_sharding"; "instance_id" => %instance_id);
                            let resp = pool.call(instance_id, proc_name!(proc_sharding), &rpc, Self::RPC_TIMEOUT)?;
                            fs.push(async move {
                                resp.await.map_err(|e| {
                                    tlog!(Warning, "failed calling proc_sharding: {e}";
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
                        let batch = Op::BatchDml { ops: vec![dml, vshard_config_version_actualize] };
                        node.propose_and_wait(batch, Duration::from_secs(3))?;
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

// FIXME: don't copy-paste this code please, this should instead further
// simplified. No need to clump up the data in here instead just dump all the
// tables and pass the data to action_plan directly.
fn get_info_for_pending_plugin_op(storage: &Clusterwide) -> PreparedPluginOp {
    let Some(plugin_op) = storage
        .properties
        .pending_plugin_op()
        .expect("i just want to feel something")
    else {
        return PreparedPluginOp::None;
    };

    match plugin_op {
        PluginOp::InstallPlugin { manifest } => PreparedPluginOp::InstallPlugin { manifest },
        PluginOp::EnablePlugin {
            plugin,
            services,
            timeout,
        } => {
            let installed_plugins = storage
                .plugins
                .get_all_versions(&plugin.name)
                .expect("storage should not fail");
            let applied_migrations = storage
                .plugin_migrations
                .get_files_by_plugin(&plugin.name)
                .expect("storage should not fail")
                .into_iter()
                .map(|record| record.migration_file)
                .collect();

            let info = EnablePluginConfig {
                ident: plugin,
                installed_plugins,
                services,
                applied_migrations,
                timeout,
            };
            PreparedPluginOp::EnablePlugin(info)
        }
        PluginOp::UpdateTopology(op) => {
            let plugin_def = storage
                .plugins
                .get(op.plugin_identity())
                .expect("storage should not fail")
                .expect("client should check that plugin exists");

            let service_def = storage
                .services
                .get(op.plugin_identity(), op.service_name())
                .expect("storage should not fail")
                .expect("client should check that service exists");

            PreparedPluginOp::UpdatePluginTopology {
                plugin_def,
                service_def,
                op,
            }
        }
    }
}
