use self::upgrade_operations::proc_internal_script;
use crate::backoff::SimpleBackoffManager;
use crate::cas;
use crate::column_name;
use crate::instance::InstanceName;
use crate::metrics;
use crate::op::Op;
use crate::proc_name;
use crate::replicaset::Replicaset;
use crate::rpc;
use crate::rpc::ddl_apply::proc_apply_schema_change;
use crate::rpc::ddl_backup;
use crate::rpc::ddl_backup::proc_apply_backup;
use crate::rpc::ddl_backup::proc_backup_abort_clear;
use crate::rpc::ddl_backup::RequestClear;
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
use crate::rpc::sharding::proc_wait_bucket_count;
use crate::schema::ADMIN_ID;
use crate::sql;
use crate::storage;
use crate::storage::get_backup_dir_name;
use crate::storage::Catalog;
use crate::storage::SystemTable;
use crate::storage::ToEntryIter;
use crate::sync::proc_get_vclock;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::error::Error as TraftError;
use crate::traft::error::ErrorInfo;
use crate::traft::network::ConnectionPool;
use crate::traft::node::global;
use crate::traft::node::Status;
use crate::traft::op::Ddl;
use crate::traft::op::Dml;
use crate::traft::op::PluginRaftOp;
use crate::traft::raft_storage::RaftSpaceAccess;
use crate::traft::{ConnectionType, Result};
use crate::unwrap_ok_or;
use crate::vshard;
use ::tarantool::error::BoxError;
use ::tarantool::error::IntoBoxError;
use ::tarantool::error::TarantoolErrorCode::Timeout;
use ::tarantool::fiber;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;
use ::tarantool::fiber::r#async::watch;
use ::tarantool::space::UpdateOps;
use futures::future::try_join;
use futures::future::try_join_all;
use plan::action_plan;
use plan::stage::*;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

mod conf_change;
pub(crate) mod plan;
mod queue;
mod sharding;
pub mod upgrade_operations;

/// Helper enum for ApplySchemaChange handling.
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

impl Loop {
    const RETRY_TIMEOUT: Duration = Duration::from_millis(250);

    async fn collect_proc_apply_schema_change(
        targets: Vec<(InstanceName, String)>,
        rpc: rpc::ddl_apply::Request,
        pool: Rc<ConnectionPool>,
        rpc_timeout: Duration,
    ) -> Result<Result<Vec<()>, OnError>, TraftError> {
        let mut fs = vec![];
        for (instance_name, _) in targets {
            tlog!(Info, "calling proc_apply_schema_change"; "instance_name" => %instance_name);
            let resp = pool.call(
                &instance_name,
                proc_name!(proc_apply_schema_change),
                &rpc,
                rpc_timeout,
            )?;
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
        Ok::<_, TraftError>(res)
    }

    async fn collect_proc_apply_backup(
        targets: Vec<(InstanceName, String)>,
        rpc: rpc::ddl_backup::Request,
        pool: Rc<ConnectionPool>,
        rpc_timeout: Duration,
    ) -> Result<Result<Vec<(InstanceName, PathBuf)>, OnError>, TraftError> {
        let mut fs = vec![];
        for (instance_name, _) in targets {
            tlog!(Info, "calling proc_apply_backup"; "instance_name" => %instance_name);
            let resp = pool.call(
                &instance_name,
                proc_name!(proc_apply_backup),
                &rpc,
                rpc_timeout,
            )?;
            fs.push(async move {
                match resp.await {
                    Ok(rpc::ddl_backup::Response::BackupPath(r)) => {
                        tlog!(Info, "applied backup on instance";
                            "instance_name" => %instance_name,
                        );
                        Ok((instance_name, r))
                    }
                    Ok(rpc::ddl_backup::Response::Abort { cause }) => {
                        tlog!(Error, "failed to apply backup on instance: {cause}";
                            "instance_name" => %instance_name,
                        );
                        Err(OnError::Abort(cause))
                    }
                    Err(e) => {
                        tlog!(Warning, "failed calling proc_apply_backup: {e}";
                            "instance_name" => %instance_name
                        );
                        Err(OnError::Retry(e))
                    }
                }
            });
        }

        let res = try_join_all(fs).await;
        Ok::<_, TraftError>(res)
    }

    async fn collect_proc_backup_abort_clear(
        targets: Vec<(InstanceName, String)>,
        rpc: rpc::ddl_backup::RequestClear,
        pool: Rc<ConnectionPool>,
        rpc_timeout: Duration,
    ) -> Result<Result<Vec<()>, OnError>, TraftError> {
        let mut fs = vec![];
        for (instance_name, _) in targets {
            tlog!(Info, "calling proc_backup_abort_clear"; "instance_name" => %instance_name);
            let resp = pool.call(
                &instance_name,
                proc_name!(proc_backup_abort_clear),
                &rpc,
                rpc_timeout,
            )?;
            fs.push(async move {
                match resp.await {
                    Ok(rpc::ddl_backup::ResponseClear::Ok) => {
                        tlog!(Info, "cleared partially backuped data on instance";
                            "instance_name" => %instance_name,
                        );
                        Ok(())
                    }
                    Err(e) => {
                        tlog!(Warning, "failed calling collect_proc_backup_abort_clear: {e}";
                            "instance_name" => %instance_name
                        );
                        Err(OnError::Retry(e))
                    }
                }
            });
        }
        let res = try_join_all(fs).await;
        Ok::<_, TraftError>(res)
    }

    async fn iter_fn(
        State {
            governor_status,
            storage,
            raft_storage,
            raft_status,
            waker,
            pool,
            backoff_manager,
        }: &mut State,
    ) -> ControlFlow<()> {
        if !raft_status.get().raft_state.is_leader() {
            set_status(governor_status, "not a leader");
            raft_status.changed().await.unwrap();
            return ControlFlow::Continue(());
        }

        let v: f64 = storage
            .db_config
            .governor_raft_op_timeout()
            .expect("storage should never ever fail");
        let raft_op_timeout = Duration::from_secs_f64(v);

        let v: f64 = storage
            .db_config
            .governor_common_rpc_timeout()
            .expect("storage should never ever fail");
        let rpc_timeout = Duration::from_secs_f64(v);

        let v: f64 = storage
            .db_config
            .governor_plugin_rpc_timeout()
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
            .filter(|peer| peer.connection_type == ConnectionType::Iproto)
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
        let cluster_uuid = raft_storage
            .cluster_uuid()
            .expect("storage should never fail");
        let node = global().expect("must be initialized");

        let sentinel_status = node.sentinel_loop.status();

        let pending_schema_change = storage
            .properties
            .pending_schema_change()
            .expect("storage should never fail");
        let tables: Vec<_> = storage
            .pico_table
            .iter()
            .expect("storage should never fail")
            .collect();
        let tables: HashMap<_, _> = tables.iter().map(|t| (t.id, t)).collect();

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

        let global_cluster_version = storage
            .properties
            .cluster_version()
            .expect("storage should never fail");

        let next_schema_version = storage
            .properties
            .next_schema_version()
            .expect("getting of next schema version should never fail");

        let governor_operations: Vec<_> = storage
            .governor_queue
            .all_operations()
            .expect("getting of governor operations should never fail");

        let global_catalog_version = storage
            .properties
            .system_catalog_version()
            .expect("getting of system_catalog_version should never fail");
        let pending_catalog_version = storage
            .properties
            .pending_catalog_version()
            .expect("getting of pending_catalog_version should never fail");

        let plan = action_plan(
            term,
            applied,
            cluster_name,
            cluster_uuid,
            sentinel_status,
            &instances,
            &existing_fds,
            &peer_addresses,
            &voters,
            &learners,
            &replicasets,
            &tiers,
            node.raft_id,
            &pending_schema_change,
            &tables,
            &plugins,
            &services,
            plugin_op.as_ref(),
            rpc_timeout,
            global_cluster_version,
            next_schema_version,
            &governor_operations,
            global_catalog_version,
            pending_catalog_version,
            backoff_manager,
        );
        let plan = unwrap_ok_or!(plan,
            Err(e) => {
                tlog!(Warning, "failed constructing an action plan: {e}");
                waker.mark_seen();
                _ = waker.changed().timeout(backoff_manager.timeout()).await;
                return ControlFlow::Continue(());
            }
        );

        // Flag used to indicate whether ApplySchemaChange application for
        // BACKUP has failed and we should abort it.
        let mut backup_abort_info: Option<(String, ErrorInfo)> = None;

        // NOTE: this is a macro, because borrow checker is hot garbage
        macro_rules! set_status {
            ($status:expr) => {
                set_status(governor_status, $status);
            };
        }

        macro_rules! governor_substep {
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

                    governor_status
                        .send_modify(|s| s.last_error = Some(e.into_box_error()))
                        .expect("status shouldn't ever be borrowed across yields");

                    waker.mark_seen();
                    _ = waker.changed().timeout(backoff_manager.timeout()).await;
                    return ControlFlow::Continue(());
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
                governor_substep! {
                    "proposing conf_change" [ "cc" => ?conf_change ]
                    async {
                        node.propose_conf_change_and_wait(term, conf_change.clone())?;
                    }
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
                governor_substep! {
                    "proposing replicaset target master change"
                    async {
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::UpdateClusterVersion(UpdateClusterVersion { cas }) => {
                set_status!("update global cluster version");
                governor_substep! {
                    "updating cluster version"
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
                governor_substep! {
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
                governor_substep! {
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
                            storage::Replicasets::TABLE_ID,
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
                governor_substep! {
                    "demoting old master and synchronizing new master" [
                        "old_master_name" => %old_master_name,
                        "new_master_name" => %new_master_name,
                        "replicaset_name" => %replicaset_name,
                    ]
                    async {
                        tlog!(Info, "calling proc_replication_demote on current master: {old_master_name}");
                        crate::error_injection!(block "BLOCK_REPLICATION_DEMOTE");
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
                    governor_substep! {
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
                                storage::Replicasets::TABLE_ID,
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
                    governor_substep! {
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
                tier,
                cas,
            }) => {
                set_status!("update instance state to offline");
                metrics::record_instance_state(tier, instance_name, new_current_state);
                tlog!(Info, "downgrading instance {instance_name}");

                governor_substep! {
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
                governor_substep! {
                    "configuring replication"
                    async {
                        crate::error_injection!(block "BLOCK_GOVERNOR_BEFORE_REPLICATION_CALL");

                        let mut fs = vec![];
                        let mut rpc = rpc::replication::ConfigureReplicationRequest {
                            term,
                            // Is only specified for the master replica
                            is_master: false,
                            replicaset_peers,
                        };

                        for instance_name in targets {
                            rpc.is_master = Some(instance_name) == master_name;
                            tlog!(Info, "calling proc_replication"; "instance_name" => %instance_name, "is_master" => rpc.is_master);

                            crate::error_injection!(block "BLOCK_REPLICATION_RPC_ON_CLIENT");

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
                                        tlog!(Warning, "failed calling proc_replication: {e}";
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

                governor_substep! {
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
                governor_substep! {
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
                governor_substep! {
                    "proposing replicaset state change"
                    async {
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::PrepareReplicasetForExpel(PrepareReplicasetForExpel {
                replicaset_name,
                cas,
            }) => {
                set_status!("prepare replicaset for expel");
                governor_substep! {
                    "preparing replicaset for expel" [
                        "replicaset_name" => %replicaset_name,
                    ]
                    async {
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::ExpelReplicaset(ExpelReplicaset {
                replicaset_name,
                target,
                rpc,
                cas,
            }) => {
                set_status!("transfer buckets from replicaset");
                governor_substep! {
                    "waiting for replicaset to transfer all buckets" [
                        "replicaset_name" => %replicaset_name,
                    ]
                    async {
                        pool.call(target, proc_name!(proc_wait_bucket_count), &rpc, rpc_timeout)?.await?;
                    }
                }

                governor_substep! {
                    "finalizing replicaset expel" [
                        "replicaset_name" => %replicaset_name,
                    ]
                    async {
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::ToOnline(ToOnline {
                instance_name,
                new_current_state,
                tier,
                plugin_rpc,
                cas,
            }) => {
                set_status!("update instance state to online");
                metrics::record_instance_state(tier, instance_name, new_current_state);
                governor_substep! {
                    "finalizing instance initialization" [
                        "instance_name" => %instance_name,
                    ]
                    async {
                        pool.call(instance_name, proc_name!(proc_enable_all_plugins), &plugin_rpc, plugin_rpc_timeout)?.await?
                    }
                }

                governor_substep! {
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

            Plan::ApplySchemaChange(ApplySchemaChange { tier, targets, rpc }) => {
                set_status!("apply clusterwide schema change");
                let mut next_op: Op = Op::Nop;
                governor_substep! {
                    "applying pending schema change"
                    async {
                        let ddl = pending_schema_change.expect("pending schema should exist");
                        tlog!(Info, "handling ApplySchemaChange for {ddl:?}");

                        let Some(rpc) = rpc else {
                            // This is a TRUNCATE on global table. RPC is not required, the
                            // operation is applied locally on each instance of the cluster
                            // when the corresponding DdlCommit is applied in raft_main_loop
                            debug_assert!(ddl.is_truncate_on_global_table(storage));

                            next_op = Op::DdlCommit;

                            return Ok(());
                        };

                        if let Some(tier) = tier {
                            // DDL should be applied only on a specific tier
                            // (e.g. case of TRUNCATE on sharded tables).
                            let map_callrw_res = vshard::ddl_map_callrw(tier, proc_name!(proc_apply_schema_change), rpc_timeout, &rpc);

                            // `ddl_map_callrw` sends requests to all replicaset masters in
                            // the tier to which ddl table belongs but we should update
                            // local_schema_change on all masters. That's why we make additional
                            // rpc calls via custom connection pool.
                            let other_targets: Vec<_> = targets
                                .iter()
                                .cloned()
                                .filter(|(_, tier_name)| tier_name != &tier)
                                .map(|(i_name, tier_name)| (i_name.clone(), tier_name.clone()))
                                .collect();
                            let res = Self::collect_proc_apply_schema_change(other_targets, rpc.clone(), pool.clone(), rpc_timeout).await?;
                            // In case it's abort error, return Ok(()) so that governor_step
                            // stop retrying execution
                            if let Err(OnError::Abort(cause)) = res {
                                next_op = Op::DdlAbort { cause };
                                crate::error_injection!(block "BLOCK_GOVERNOR_BEFORE_DDL_ABORT");
                                return Ok(());
                            }
                            // Otherwise unwrap Err so that next governor step is executed.
                            res?;

                            let proc_rpc_res_vec = match map_callrw_res {
                                Ok(proc_rpc_res_vec) => proc_rpc_res_vec,
                                Err(e) => {
                                    // E.g. we faced with timeout.
                                    tlog!(Error, "failed to execute map_callrw for TRUNCATE: {e}";);
                                    return Err(e)
                                }
                            };

                            let expected_tier_masters_num = targets.iter().filter(|(_, tier_name)| tier_name == &tier).count();
                            let actual_tier_masters_num = proc_rpc_res_vec.len();
                            if actual_tier_masters_num != expected_tier_masters_num {
                                // Some of the replicasets' masters went down so we've executed our
                                // `proc_apply_schema_change` only on some of them. Have to retry the query.
                                tlog!(
                                    Error,
                                    "failed to execute map_callrw for TRUNCATE: some masters are down";
                                    "expected" => %expected_tier_masters_num,
                                    "actual" => %actual_tier_masters_num
                                );
                                return Err(Error::other(
                                    format!("failed to execute map_callrw for TRUNCATE: expected {expected_tier_masters_num} masters, got {actual_tier_masters_num}")
                                ))
                            }
                            for res in proc_rpc_res_vec {
                                match res.response {
                                    rpc::ddl_apply::Response::Ok => {},
                                    rpc::ddl_apply::Response::Abort { .. } => {
                                        unreachable!("TRUNCATE can't cause Abort on `proc_apply_schema_change` call")
                                    },
                                }
                            }
                        } else {
                            let targets_cloned: Vec<_> = targets
                                .iter()
                                .cloned()
                                .map(|(i_name, tier_name)| (i_name.clone(), tier_name.clone()))
                                .collect();
                            if let Ddl::Backup { timestamp } = ddl {
                                let backup_dir_name = get_backup_dir_name(timestamp);

                                let replicas: Vec<_> = storage
                                    .instances
                                    .iter()?
                                    .map(|i| (i.name, i.tier))
                                    .filter(|p| !targets_cloned.contains(p))
                                    .collect();

                                // Vec of pairs (instance_name, backup_path).
                                let mut backup_paths = HashMap::<InstanceName, PathBuf>::new();

                                let rpc_master = ddl_backup::Request {
                                    term: rpc.term,
                                    applied: rpc.applied,
                                    timeout: rpc.timeout,
                                    is_master: true
                                };
                                // 1. Call `proc_apply_schema_change` on all masters.
                                tlog!(Info, "calling BACKUP on masters");
                                let res = Self::collect_proc_apply_backup(targets_cloned.clone(), rpc_master.clone(), pool.clone(), rpc_timeout).await?;
                                if let Err(OnError::Abort(ref cause)) = res {
                                    backup_abort_info = Some((backup_dir_name, cause.clone()));
                                    return Ok(());
                                }
                                backup_paths.extend(res?);

                                // 2. Call `proc_apply_schema_change` on all replicas.
                                if !replicas.is_empty() {
                                    crate::error_injection!(block "BLOCK_GOVERNOR_BEFORE_BACKUP_ON_REPLICAS");

                                    let mut rpc_replica = rpc_master.clone();
                                    rpc_replica.is_master = false;

                                    tlog!(Info, "calling BACKUP on replicas");
                                    let res = Self::collect_proc_apply_backup(replicas.clone(), rpc_replica, pool.clone(), rpc_timeout).await?;
                                    if let Err(OnError::Abort(ref cause)) = res {
                                        backup_abort_info = Some((backup_dir_name, cause.clone()));
                                        return Ok(())
                                    }
                                    backup_paths.extend(res?);
                                }

                                let backup_paths_yaml = serde_yaml::to_string(&backup_paths)
                                    .expect("yaml conversion should not fail");
                                tlog!(Info, "BACKUP is finished successfully with the following paths:\n{backup_paths_yaml}");
                            } else {
                                let res = Self::collect_proc_apply_schema_change(targets_cloned, rpc.clone(), pool.clone(), rpc_timeout).await?;
                                if let Err(OnError::Abort(cause)) = res {
                                    next_op = Op::DdlAbort { cause };
                                    crate::error_injection!(block "BLOCK_GOVERNOR_BEFORE_DDL_ABORT");
                                    return Ok(());
                                }
                                res?;
                            }
                        }

                        next_op = Op::DdlCommit;

                        crate::error_injection!(block "BLOCK_GOVERNOR_BEFORE_DDL_COMMIT");
                    }
                }

                if let Some((backup_dir_name, cause)) = backup_abort_info {
                    governor_substep! {
                        "clearing backup"
                        async {
                            // In case backup finished with DdlAbort we have to make
                            // additional rpc to clear partially backuped up data.
                            let rpc_clear = RequestClear { backup_dir_name: backup_dir_name.clone() };
                            // Retry infinitely until data is cleared.
                            let targets = storage
                                .instances
                                .iter()?
                                .map(|i| (i.name, i.tier))
                                .collect();
                            Self::collect_proc_backup_abort_clear(
                                targets,
                                rpc_clear,
                                pool.clone(),
                                rpc_timeout,
                            ).await??;
                            next_op = Op::DdlAbort { cause };
                            crate::error_injection!(block "BLOCK_GOVERNOR_BEFORE_DDL_ABORT");
                        }
                    }
                }

                let op_name = next_op.to_string();
                governor_substep! {
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
                governor_substep! {
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

                governor_substep! {
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

                governor_substep! {
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
                                    Err(e) if e.error_code() == Timeout as u32 => {
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

                governor_substep! {
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
                governor_substep! {
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

                governor_substep! {
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
                governor_substep! {
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
                        if let Err(e) = try_join_all(fs).await {
                            backoff_manager.sharding.handle_failure();
                            return Err(e);
                        }

                    }
                }

                governor_substep! {
                    "updating current vshard config"
                    async {
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        if let Err(e) = cas::compare_and_swap_local(&cas, deadline)?.no_retries() {
                            backoff_manager.sharding.handle_failure();
                            return Err(e);
                        }
                        backoff_manager.sharding.handle_success();
                    }
                }
            }

            Plan::CreateGovernorQueue(CreateGovernorQueue { cas }) => {
                set_status(governor_status, "create _pico_governor_queue table");
                governor_substep! {
                    "creating _pico_governor_queue table"
                    async {
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::InsertUpgradeOperation(InsertUpgradeOperation { cas }) => {
                set_status(governor_status, "insert upgrade operation");
                governor_substep! {
                    "inserting upgrade operation"
                    async {
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::RunSqlOperationStep(RunSqlOperationStep {
                operation_id,
                query,
                cas_on_success,
            }) => {
                set_status(governor_status, "run sql operation step");
                governor_substep! {
                    "running sql operation step" [
                        "operation_id" => %operation_id,
                        "query" => %query,
                    ]
                    async {
                        match sql::parse_and_dispatch(query, vec![], None, Some(operation_id)) {
                            Ok(tuple) => {
                                // check if we have no-op (row_count = 0)
                                // mark governor operation as successful in such case
                                #[derive(serde::Deserialize)]
                                struct RowCount {
                                    row_count: usize,
                                }
                                if let Ok(Some(res)) = tuple.field::<RowCount>(0) {
                                    if res.row_count == 0 {
                                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                                        cas::compare_and_swap_local(&cas_on_success, deadline)?.no_retries()?;
                                    }
                                }
                                // we will change governor operation status for DDL with row_count > 0
                                // in raft main loop on DdlCommit handling
                            }
                            Err(e) => {
                                let cas = queue::make_change_status_cas(operation_id, applied, true, Some(e.to_string()))?;
                                let deadline = fiber::clock().saturating_add(raft_op_timeout);
                                cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                                return Err(e);
                            }
                        }
                    }
                }
            }

            Plan::RunProcNameOperationStep(RunProcNameOperationStep {
                operation_id,
                proc_name,
                targets,
                rpc,
                cas_on_success,
            }) => {
                set_status(governor_status, "run proc_name operation step");
                let mut error_message = None;

                governor_substep! {
                    "creating procedure" [
                        "operation_id" => %operation_id,
                        "proc_name" => %proc_name,
                    ]
                    async {
                        let mut fs = vec![];
                        for instance_name in targets {
                            tlog!(Info, "calling proc_apply_schema_change"; "instance_name" => %instance_name);
                            let resp = pool.call(instance_name, proc_name!(proc_apply_schema_change), &rpc, rpc_timeout)?;
                            fs.push(async move {
                                match resp.await {
                                    Ok(_) => Ok(()),
                                    Err(e) => {
                                        tlog!(Warning, "failed calling proc_apply_schema_change: {e}";
                                            "instance_name" => %instance_name
                                        );
                                        Err(e)
                                    }
                                }
                            });
                        }
                        if let Err(e) = try_join_all(fs).await {
                            error_message = Some(e.to_string());
                        }
                    }
                }

                let is_success = error_message.is_none();
                governor_substep! {
                    "updating operation status" [
                        "operation_id" => %operation_id,
                        "proc_name" => %proc_name,
                        "success" => %is_success,
                    ]
                    async {
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        let cas = match error_message {
                            Some(_) => queue::make_change_status_cas(operation_id, applied, true, error_message)?,
                            None => cas_on_success,
                        };
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::RunExecScriptOperationStep(RunExecScriptOperationStep {
                operation_id,
                script_name,
                targets,
                rpc,
                cas_on_success,
            }) => {
                set_status(governor_status, "run exec_script operation step");
                let mut error_message = None;

                governor_substep! {
                    "executing script" [
                        "operation_id" => %operation_id,
                        "script_name" => %script_name,
                    ]
                    async {
                        let mut fs = vec![];
                        for instance_name in targets {
                            tlog!(Info, "calling proc_internal_script"; "instance_name" => %instance_name);
                            let resp = pool.call(instance_name, proc_name!(proc_internal_script), &rpc, rpc_timeout)?;
                            fs.push(async move {
                                match resp.await {
                                    Ok(_) => Ok(()),
                                    Err(e) => {
                                        tlog!(Warning, "failed calling proc_internal_script: {e}";
                                            "instance_name" => %instance_name
                                        );
                                        Err(e)
                                    }
                                }
                            });
                        }
                        if let Err(e) = try_join_all(fs).await {
                            error_message = Some(e.to_string());
                        }
                    }
                }

                let is_success = error_message.is_none();
                governor_substep! {
                    "updating operation status" [
                        "operation_id" => %operation_id,
                        "script_name" => %script_name,
                        "success" => %is_success,
                    ]
                    async {
                        let deadline = fiber::clock().saturating_add(raft_op_timeout);
                        let cas = match error_message {
                            Some(_) => queue::make_change_status_cas(operation_id, applied, true, error_message)?,
                            None => cas_on_success,
                        };
                        cas::compare_and_swap_local(&cas, deadline)?.no_retries()?;
                    }
                }
            }

            Plan::FinishCatalogUpgrade(FinishCatalogUpgrade { cas }) => {
                set_status(governor_status, "finish system catalog upgrade");
                governor_substep! {
                    "finishing system catalog upgrade"
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

        // The step ended successfully
        governor_status
            .send_modify(|s| {
                s.step_counter += 1;
                s.last_error = None;
            })
            .expect("status shouldn't ever be borrowed across yields");
        ControlFlow::Continue(())
    }

    pub fn start(
        pool: Rc<ConnectionPool>,
        raft_status: watch::Receiver<Status>,
        storage: Catalog,
        raft_storage: RaftSpaceAccess,
    ) -> Self {
        let (waker_tx, waker_rx) = watch::channel(());
        let (governor_status_tx, governor_status_rx) = watch::channel(GovernorStatus {
            governor_loop_status: "initializing",
            last_error: None,
            step_counter: 0,
        });

        let state = State {
            governor_status: governor_status_tx,
            storage,
            raft_storage,
            raft_status,
            waker: waker_rx,
            pool,
            backoff_manager: GovernorBackoffManager::new(),
        };

        Self {
            fiber_id: crate::loop_start!("governor_loop", Self::iter_fn, state),
            waker: waker_tx,
            status: governor_status_rx,
        }
    }

    pub fn for_tests() -> Self {
        let (waker, _) = watch::channel(());
        let (_, status) = watch::channel(GovernorStatus {
            governor_loop_status: "uninitialized",
            last_error: None,
            step_counter: 0,
        });
        Self {
            fiber_id: 0,
            waker,
            status,
        }
    }

    #[inline(always)]
    pub fn wakeup(&self) -> Result<()> {
        self.waker.send(()).map_err(|_| Error::GovernorStopped)
    }
}

#[inline(always)]
fn set_status(status: &mut watch::Sender<GovernorStatus>, msg: &'static str) {
    let status_ref = status.borrow();
    if status_ref.governor_loop_status == msg {
        return;
    }

    let counter = status_ref.step_counter;
    tlog!(Debug, "governor_loop_status = #{counter} '{msg}'");
    drop(status_ref);

    status
        .send_modify(|s| s.governor_loop_status = msg)
        .expect("status shouldn't ever be borrowed across yields");

    metrics::record_governor_change();
}

pub struct Loop {
    #[allow(dead_code)]
    fiber_id: fiber::FiberId,
    waker: watch::Sender<()>,

    /// Current status of governor loop.
    ///
    // XXX: maybe this shouldn't be a watch::Receiver, but it's not much worse
    // than a Rc, so ...
    pub status: watch::Receiver<GovernorStatus>,
}

struct State {
    governor_status: watch::Sender<GovernorStatus>,
    storage: Catalog,
    raft_storage: RaftSpaceAccess,
    raft_status: watch::Receiver<Status>,
    waker: watch::Receiver<()>,
    pool: Rc<ConnectionPool>,
    backoff_manager: GovernorBackoffManager,
}

#[derive(Debug, Clone)]
pub struct GovernorStatus {
    /// Current state of the governor loop.
    ///
    /// Is set by governor to explain the reason why it has yielded.
    pub governor_loop_status: &'static str,

    /// If the last governor step ended with an error, this is the corresponding
    /// error value.
    ///
    /// If the last governor step ended successfully this will be `None`.
    pub last_error: Option<BoxError>,

    /// Number of times the current instance has successfully executed a
    /// governor step. Is reset on restart.
    ///
    /// This value is only used for testing purposes.
    pub step_counter: u64,
}

/// Manages backoff strategies and timeouts for the different stages of the governor.
struct GovernorBackoffManager {
    /// Sharding stage (where `proc_sharding` is called).
    pub sharding: SimpleBackoffManager,
}

impl GovernorBackoffManager {
    const BASE_TIMEOUT: Duration = Duration::from_millis(125);
    const MAX_TIMEOUT: Duration = Duration::from_secs(600);

    pub fn new() -> Self {
        Self {
            sharding: SimpleBackoffManager::new("sharding", Self::BASE_TIMEOUT, Self::MAX_TIMEOUT),
        }
    }

    /// Returns the current timeout.
    pub fn timeout(&self) -> Duration {
        Loop::RETRY_TIMEOUT.max(self.sharding.timeout())
    }
}
