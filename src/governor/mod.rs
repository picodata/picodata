use std::collections::HashMap;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::time::Duration;

use ::tarantool::fiber;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;
use ::tarantool::fiber::r#async::watch;
use ::tarantool::space::UpdateOps;

use crate::cas;
use crate::column_name;
use crate::op::Op;
use crate::proc_name;
use crate::replicaset::Replicaset;
use crate::rpc;
use crate::rpc::ddl_apply::proc_apply_schema_change;
use crate::rpc::disable_service::proc_disable_service;
use crate::rpc::enable_all_plugins::proc_enable_all_plugins;
use crate::rpc::enable_plugin::proc_enable_plugin;
use crate::rpc::enable_service::proc_enable_service;
use crate::rpc::load_plugin_dry_run::proc_load_plugin_dry_run;
use crate::rpc::replication::proc_replication;
use crate::rpc::replication::proc_replication_demote;
use crate::rpc::replication::proc_replication_sync;
use crate::rpc::sharding::bootstrap::proc_sharding_bootstrap;
use crate::rpc::sharding::proc_sharding;
use crate::schema::ADMIN_ID;
use crate::storage::Clusterwide;
use crate::storage::ClusterwideTable;
use crate::storage::ToEntryIter as _;
use crate::sync::proc_get_vclock;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::error::ErrorInfo;
use crate::traft::network::ConnectionPool;
use crate::traft::node::global;
use crate::traft::node::Status;
use crate::traft::op::Dml;
use crate::traft::op::PluginRaftOp;
use crate::traft::raft_storage::RaftSpaceAccess;
use crate::traft::Result;
use crate::unwrap_ok_or;
use futures::future::try_join;
use futures::future::try_join_all;
use plan::action_plan;
use plan::stage::*;

pub(crate) mod conf_change;
pub(crate) mod plan;

impl Loop {
    const RETRY_TIMEOUT: Duration = Duration::from_millis(250);

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

        let v: f64 = storage
            .db_config
            .get_or_default(crate::system_parameter_name!(governor_raft_op_timeout))
            .expect("storage should never ever fail");
        let raft_op_timeout = Duration::from_secs_f64(v);

        let v: f64 = storage
            .db_config
            .get_or_default(crate::system_parameter_name!(governor_common_rpc_timeout))
            .expect("storage should never ever fail");
        let rpc_timeout = Duration::from_secs_f64(v);

        let v: f64 = storage
            .db_config
            .get_or_default(crate::system_parameter_name!(governor_plugin_rpc_timeout))
            .expect("storage should never ever fail");
        let plugin_rpc_timeout = Duration::from_secs_f64(v);

        let instances = storage
            .instances
            .all_instances()
            .expect("storage should never fail");
        let existing_fds = storage
            .instances
            .failure_domain_names()
            .expect("storage ain't bouta fail");
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
        let replicasets: HashMap<_, _> = replicasets.iter().map(|rs| (&rs.name, rs)).collect();

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
        let cluster_name = raft_storage
            .cluster_name()
            .expect("storage should never fail");
        let node = global().expect("must be initialized");
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
        let all_services: Vec<_> = storage
            .services
            .iter()
            .expect("storage should never fail")
            .collect();
        let mut services = HashMap::new();
        for service_def in &all_services {
            let e = services.entry(service_def.plugin());
            e.or_insert_with(Vec::new).push(service_def);
        }
        let plugin_op = storage
            .properties
            .pending_plugin_op()
            .expect("i just want to feel something");

        let plan = action_plan(
            term,
            applied,
            cluster_name,
            &instances,
            &existing_fds,
            &peer_addresses,
            &voters,
            &learners,
            &replicasets,
            &tiers,
            node.raft_id,
            has_pending_schema_change,
            &plugins,
            &services,
            plugin_op.as_ref(),
            rpc_timeout,
        );
        let plan = unwrap_ok_or!(plan,
            Err(e) => {
                tlog!(Warning, "failed constructing an action plan: {e}");
                waker.mark_seen();
                _ = waker.changed().timeout(Loop::RETRY_TIMEOUT).await;
                return ControlFlow::Continue(());
            }
        );

        // NOTE: this is a macro, because borrow checker is hot garbage
        macro_rules! set_status {
            ($status:expr) => {
                set_status(governor_status, $status);
            };
        }

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
                set_status!("conf change");
                // main_loop gives the warranty that every ProposeConfChange
                // will sometimes be handled and there's no need in timeout.
                // It also guarantees that the notification will arrive only
                // after the node leaves the joint state.
                tlog!(Info, "proposing conf_change"; "cc" => ?conf_change);
                if let Err(e) = node.propose_conf_change_and_wait(term, conf_change) {
                    tlog!(Warning, "failed proposing conf_change: {e}");
                    fiber::sleep(Loop::RETRY_TIMEOUT);
                }
            }

            Plan::TransferLeadership(TransferLeadership { to }) => {
                set_status!("transfer raft leader");
                tlog!(Info, "transferring leadership to {}", to.name);
                node.transfer_leadership_and_yield(to.raft_id);
                _ = waker.changed().timeout(Loop::RETRY_TIMEOUT).await;
            }

            Plan::UpdateTargetReplicasetMaster(UpdateTargetReplicasetMaster { cas }) => {
                set_status!("update target replication leader");
                governor_step! {
                    "proposing replicaset target master change"
                    async {
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::ReplicasetMasterFailover(ReplicasetMasterFailover {
                old_master_name,
                new_master_name,
                get_vclock_rpc,
                replicaset_name,
                mut replicaset_dml,
                bump_dml,
                ranges,
            }) => {
                set_status!("transfer replication leader");
                tlog!(
                    Info,
                    "transferring replicaset mastership from {old_master_name} to {new_master_name} (offline)"
                );

                let mut promotion_vclock = None;
                governor_step! {
                    "getting promotion vclock from new master" [
                        "new_master_name" => %new_master_name,
                        "replicaset_name" => %replicaset_name,
                    ]
                    async {
                        let vclock = pool.call(new_master_name, proc_name!(proc_get_vclock), &get_vclock_rpc, rpc_timeout)?.await?;
                        promotion_vclock = Some(vclock);
                    }
                }

                let promotion_vclock = promotion_vclock.expect("was just assigned");
                let promotion_vclock = promotion_vclock.ignore_zero();
                governor_step! {
                    "proposing replicaset current master change" [
                        "current_master_name" => %new_master_name,
                        "replicaset_name" => %replicaset_name,
                    ]
                    async {
                        let mut ops = bump_dml;
                        replicaset_dml.assign(
                            column_name!(Replicaset, promotion_vclock), &promotion_vclock
                        ).expect("shan't fail");
                        let op = Dml::update(
                            ClusterwideTable::Replicaset,
                            &[replicaset_name],
                            replicaset_dml,
                            ADMIN_ID,
                        )?;
                        ops.push(op);

                        let op = Op::single_dml_or_batch(ops);
                        let predicate = cas::Predicate::new(applied, ranges);
                        let cas = cas::Request::new(op, predicate, ADMIN_ID)?;
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::ReplicasetMasterConsistentSwitchover(ReplicasetMasterConsistentSwitchover {
                replicaset_name,
                old_master_name,
                demote_rpc,
                new_master_name,
                sync_rpc,
                promotion_vclock,
                master_actualize_dml,
                bump_dml,
                ranges,
            }) => {
                set_status!("transfer replication leader");
                tlog!(
                    Info,
                    "transferring replicaset mastership from {old_master_name} to {new_master_name}"
                );

                let mut demotion_vclock = None;
                governor_step! {
                    "demoting old master and synchronizing new master" [
                        "old_master_name" => %old_master_name,
                        "new_master_name" => %new_master_name,
                        "replicaset_name" => %replicaset_name,
                    ]
                    async {
                        tlog!(Info, "calling proc_replication_demote on current master: {old_master_name}");
                        let f_demote = pool.call(old_master_name, proc_name!(proc_replication_demote), &demote_rpc, rpc_timeout)?;

                        tlog!(Info, "calling proc_replication_sync on target master: {new_master_name}");
                        let f_sync = pool.call(new_master_name, proc_name!(proc_replication_sync), &sync_rpc, rpc_timeout)?;

                        let (demote_response, _) = try_join(f_demote, f_sync).await?;
                        demotion_vclock = Some(demote_response.vclock);
                    }
                }

                let demotion_vclock = demotion_vclock.expect("is always set on a previous step");
                if &demotion_vclock > promotion_vclock {
                    let new_promotion_vclock = demotion_vclock;
                    governor_step! {
                        "updating replicaset promotion vclock" [
                            "replicaset_name" => %replicaset_name,
                            "promotion_vclock" => ?new_promotion_vclock,
                        ]
                        async {
                            let mut ops = bump_dml;

                            // Note: we drop the master_actualize_dml because switchover is not finished yet.
                            // We just update the promotion_vclock value and retry synchronizing on next governor step.
                            let mut replicaset_dml = UpdateOps::new();
                            replicaset_dml.assign(
                                column_name!(Replicaset, promotion_vclock), &new_promotion_vclock
                            ).expect("shan't fail");
                            let op = Dml::update(
                                ClusterwideTable::Replicaset,
                                &[replicaset_name],
                                replicaset_dml,
                                ADMIN_ID,
                            )?;
                            ops.push(op);

                            let op = Op::single_dml_or_batch(ops);
                            let predicate = cas::Predicate::new(applied, ranges);
                            let cas = cas::Request::new(op, predicate, ADMIN_ID)?;
                            let deadline = fiber::clock().saturating_add(raft_op_timeout);
                            cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                        }
                    }
                } else {
                    // Vclock on the new master is up to date with old master,
                    // so switchover is compelete.
                    governor_step! {
                        "updating replicaset current master id" [
                            "replicaset_name" => %replicaset_name,
                            "current_master_name" => ?new_master_name,
                        ]
                        async {
                            let mut ops = bump_dml;
                            ops.push(master_actualize_dml);

                            let op = Op::single_dml_or_batch(ops);
                            let predicate = cas::Predicate::new(applied, ranges);
                            let cas = cas::Request::new(op, predicate, ADMIN_ID)?;
                            let deadline = fiber::clock().saturating_add(raft_op_timeout);
                            cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                        }
                    }
                }
            }

            Plan::Downgrade(Downgrade {
                instance_name,
                new_current_state,
                cas,
            }) => {
                set_status!("update instance state to offline");
                tlog!(Info, "downgrading instance {instance_name}");

                governor_step! {
                    "handling instance state change" [
                        "instance_name" => %instance_name,
                        "current_state" => %new_current_state,
                    ]
                    async {
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::ConfigureReplication(ConfigureReplication {
                replicaset_name,
                targets,
                master_name,
                replicaset_peers,
                replication_config_version_actualize,
            }) => {
                set_status!("configure replication");
                governor_step! {
                    "configuring replication"
                    async {
                        let mut fs = vec![];
                        let mut rpc = rpc::replication::ConfigureReplicationRequest {
                            // Is only specified for the master replica
                            is_master: false,
                            replicaset_peers,
                        };

                        for instance_name in targets {
                            rpc.is_master = Some(instance_name) == master_name;
                            tlog!(Info, "calling proc_replication"; "instance_name" => %instance_name, "is_master" => rpc.is_master);

                            let resp = pool.call(instance_name, proc_name!(proc_replication), &rpc, rpc_timeout)?;
                            fs.push(async move {
                                match resp.await {
                                    Ok(_) => {
                                        tlog!(Info, "configured replication with instance";
                                            "instance_name" => %instance_name,
                                        );
                                        Ok(())
                                    }
                                    Err(e) => {
                                        tlog!(Warning, "failed calling rpc::replication: {e}";
                                            "instance_name" => %instance_name
                                        );
                                        Err(e)
                                    }
                                }
                            });
                        }
                        try_join_all(fs).await?
                    }
                }

                governor_step! {
                    "actualizing replicaset configuration version" [
                        "replicaset_name" => %replicaset_name,
                    ]
                    async {
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&replication_config_version_actualize, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::ShardingBoot(ShardingBoot {
                target,
                rpc,
                cas,
                tier_name,
            }) => {
                set_status(governor_status, "bootstrap bucket distribution");
                governor_step! {
                    "bootstrapping bucket distribution" [
                        "instance_name" => %target,
                        "tier" => %tier_name,
                    ]
                    async {
                        pool.call(target, proc_name!(proc_sharding_bootstrap), &rpc, rpc_timeout)?.await?;
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::ProposeReplicasetStateChanges(ProposeReplicasetStateChanges { cas }) => {
                set_status!("update replicaset state");
                governor_step! {
                    "proposing replicaset state change"
                    async {
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::ToOnline(ToOnline {
                target,
                new_current_state,
                plugin_rpc,
                cas,
            }) => {
                set_status!("update instance state to online");
                governor_step! {
                    "enable plugins on instance" [
                        "instance_name" => %target,
                    ]
                    async {
                        pool.call(target, proc_name!(proc_enable_all_plugins), &plugin_rpc, plugin_rpc_timeout)?.await?
                    }
                }

                governor_step! {
                    "handling instance state change" [
                        "instance_name" => %target,
                        "current_state" => %new_current_state,
                    ]
                    async {
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::ApplySchemaChange(ApplySchemaChange { targets, rpc }) => {
                set_status!("apply clusterwide schema change");
                let mut next_op = Op::Nop;
                governor_step! {
                    "applying pending schema change"
                    async {
                        let mut fs = vec![];
                        for instance_name in targets {
                            tlog!(Info, "calling proc_apply_schema_change"; "instance_name" => %instance_name);
                            let resp = pool.call(instance_name, proc_name!(proc_apply_schema_change), &rpc, rpc_timeout)?;
                            fs.push(async move {
                                match resp.await {
                                    Ok(rpc::ddl_apply::Response::Ok) => {
                                        tlog!(Info, "applied schema change on instance";
                                            "instance_name" => %instance_name,
                                        );
                                        Ok(())
                                    }
                                    Ok(rpc::ddl_apply::Response::Abort { cause }) => {
                                        tlog!(Error, "failed to apply schema change on instance: {cause}";
                                            "instance_name" => %instance_name,
                                        );
                                        Err(OnError::Abort(cause))
                                    }
                                    Err(e) => {
                                        tlog!(Warning, "failed calling proc_apply_schema_change: {e}";
                                            "instance_name" => %instance_name
                                        );
                                        Err(OnError::Retry(e))
                                    }
                                }
                            });
                        }
                        let res = try_join_all(fs).await;
                        if let Err(OnError::Abort(cause)) = res {
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
                        let ranges = cas::Range::for_op(&next_op)?;
                        let predicate = cas::Predicate::new(applied, ranges);
                        let cas = cas::Request::new(next_op, predicate, ADMIN_ID)?;
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::CreatePlugin(CreatePlugin {
                targets,
                rpc,
                success_dml,
                ranges,
            }) => {
                set_status!("install new plugin");

                let mut next_op = None;
                governor_step! {
                    "checking if plugin is ready for installation on instances"
                    async {
                        let mut fs = vec![];
                        for instance_name in targets {
                            tlog!(Info, "calling proc_load_plugin_dry_run"; "instance_name" => %instance_name);
                            let resp = pool.call(instance_name, proc_name!(proc_load_plugin_dry_run), &rpc, plugin_rpc_timeout)?;
                            fs.push(async move {
                                match resp.await {
                                    Ok(_) => {
                                        tlog!(Info, "instance is ready to install plugin";
                                            "instance_name" => %instance_name,
                                        );
                                        Ok(())
                                    }
                                    Err(e) => {
                                        tlog!(Error, "failed to call proc_load_plugin_dry_run: {e}";
                                            "instance_name" => %instance_name
                                        );
                                        Err(ErrorInfo::new(instance_name.clone(), e))
                                    }
                                }
                            });
                        }

                        if let Err(cause) = try_join_all(fs).await {
                            tlog!(Error, "Plugin installation aborted: {cause}");
                            next_op = Some(Op::Plugin(PluginRaftOp::Abort { cause }));
                            return Ok(());
                        }

                        next_op = Some(success_dml);
                    }
                }

                governor_step! {
                    "finalizing plugin installing"
                    async {
                        let op = next_op.expect("is set on the first substep");
                        let predicate = cas::Predicate::new(applied, ranges);
                        let cas = cas::Request::new(op, predicate, ADMIN_ID)?;
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::EnablePlugin(EnablePlugin {
                targets,
                rpc,
                ident,
                on_start_timeout,
                success_dml,
                ranges,
            }) => {
                set_status!("enable plugin");
                let mut next_op = None;

                governor_step! {
                    "enabling plugin"
                    async {
                        let mut fs = vec![];
                        for &instance_name in &targets {
                            tlog!(Info, "calling enable_plugin"; "instance_name" => %instance_name);
                            let resp = pool.call(instance_name, proc_name!(proc_enable_plugin), &rpc, on_start_timeout)?;
                            fs.push(async move {
                                match resp.await {
                                    Ok(rpc::enable_plugin::Response::Ok) => {
                                        tlog!(Info, "enabled plugin on instance"; "instance_name" => %instance_name);
                                        Ok(())
                                    }
                                    Ok(rpc::enable_plugin::Response::Abort { cause }) => {
                                        tlog!(Error, "failed to enable plugin at instance: {cause}";
                                            "instance_name" => %instance_name,
                                        );
                                        Err(OnError::Abort(cause))
                                    }
                                    Err(Error::Timeout) => {
                                        tlog!(Error, "failed to enable plugin at instance: timeout";
                                            "instance_name" => %instance_name,
                                        );
                                        Err(OnError::Abort(ErrorInfo::timeout(instance_name.clone(), "no response")))
                                    }
                                    Err(e) => {
                                        tlog!(Warning, "failed calling proc_load_plugin: {e}";
                                            "instance_name" => %instance_name
                                        );
                                        Err(OnError::Retry(e))
                                    }
                                }
                            });
                        }

                        let enable_result = try_join_all(fs).await;
                        if let Err(OnError::Abort(cause)) = enable_result {
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
                        let predicate = cas::Predicate::new(applied, ranges);
                        let cas = cas::Request::new(op, predicate, ADMIN_ID)?;
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::AlterServiceTiers(AlterServiceTiers {
                enable_targets,
                disable_targets,
                enable_rpc,
                disable_rpc,
                success_dml,
                ranges,
            }) => {
                set_status!("update plugin service topology");
                let mut next_op = None;

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
                        for &instance_name in &enable_targets {
                            tlog!(Info, "calling proc_enable_service"; "instance_name" => %instance_name);
                            let resp = pool.call(instance_name, proc_name!(proc_enable_service), &enable_rpc, plugin_rpc_timeout)?;
                            fs.push(async move {
                                match resp.await {
                                    Ok(_) => {
                                        tlog!(Info, "instance enable service"; "instance_name" => %instance_name);
                                        Ok(())
                                    }
                                    Err(e) => {
                                        tlog!(Error, "failed to call proc_enable_service: {e}";
                                            "instance_name" => %instance_name
                                        );
                                        Err(ErrorInfo::new(instance_name.clone(), e))
                                    }
                                }
                            });
                        }

                        if let Err(cause) = try_join_all(fs).await {
                            tlog!(Error, "Enabling plugins fail with: {cause}, rollback and abort");
                            next_op = Some(Op::Plugin(PluginRaftOp::Abort { cause }));

                            // try to disable plugins at all instances
                            // where it was enabled previously
                            let mut fs = vec![];
                            for instance_name in enable_targets {
                                let resp = pool.call(instance_name, proc_name!(proc_disable_service), &disable_rpc, plugin_rpc_timeout)?;
                                fs.push(resp);
                            }
                            // FIXME: over here we completely ignore the result of the RPC above.
                            // This means that the service may still be enabled on some (or even all) instances
                            // while the global state says that it's disabled everywhere
                            // https://git.picodata.io/picodata/picodata/picodata/-/issues/600
                            _ = try_join_all(fs).await;
                            return Ok(());
                        }

                        let mut fs = vec![];
                        for instance_name in disable_targets {
                            tlog!(Info, "calling proc_disable_service"; "instance_name" => %instance_name);
                            let resp = pool.call(instance_name, proc_name!(proc_disable_service), &disable_rpc, plugin_rpc_timeout)?;
                            fs.push(resp);
                        }
                        try_join_all(fs).await?;

                        next_op = Some(success_dml);
                    }
                }

                governor_step! {
                    "finalizing topology update"
                    async {
                        let op = next_op.expect("is set on the first substep");
                        let predicate = cas::Predicate::new(applied, ranges);
                        let cas = cas::Request::new(op, predicate, ADMIN_ID)?;
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::UpdateCurrentVshardConfig(UpdateCurrentVshardConfig {
                targets,
                rpc,
                cas,
                tier_name,
            }) => {
                set_status(governor_status, "update current sharding configuration");
                governor_step! {
                    "applying vshard config changes" [
                        "tier" => %tier_name
                    ]
                    async {
                        let mut fs = vec![];
                        for instance_name in targets {
                            tlog!(Info, "calling proc_sharding"; "instance_name" => %instance_name);
                            let resp = pool.call(instance_name, proc_name!(proc_sharding), &rpc, rpc_timeout)?;
                            fs.push(async move {
                                resp.await.map_err(|e| {
                                    tlog!(Warning, "failed calling proc_sharding: {e}";
                                        "instance_name" => %instance_name
                                    );
                                    e
                                })
                            });
                        }
                        try_join_all(fs).await?
                    }
                }

                governor_step! {
                    "updating current vshard config"
                    async {
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::None => {
                set_status!("idle");
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
