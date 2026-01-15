#[allow(unused_imports)]
use crate::governor::ddl::handle_pending_ddl;
use crate::governor::ddl::OnError;
use crate::governor::plan::stage::Plan;
use crate::governor::plan::stage::*;
use crate::has_states;
use crate::instance::InstanceName;
use crate::proc_name;
use crate::rpc;
use crate::rpc::ddl_backup::proc_apply_backup;
use crate::rpc::ddl_backup::proc_backup_abort_clear;
use crate::storage::get_backup_dir_name;
use crate::tlog;
use crate::topology_cache::TopologyCacheRef;
use crate::traft::ConnectionPool;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;
use crate::traft::Result;
use crate::warn_or_panic;
use futures::future::try_join_all;
use std::path::PathBuf;
use std::time::Duration;

////////////////////////////////////////////////////////////////////////////////
// handle_backup
////////////////////////////////////////////////////////////////////////////////

/// Prepares governor actions for applying a pending BACKUP operation.
///
/// This function is called from [`handle_pending_ddl`]
pub fn handle_backup<'i>(
    topology_ref: &TopologyCacheRef,
    timestamp: i64,
    term: RaftTerm,
    applied: RaftIndex,
    sync_timeout: Duration,
) -> Result<Option<Plan<'i>>> {
    let backup_dir_name = get_backup_dir_name(timestamp);

    let mut masters = Vec::new();
    let mut replicas = Vec::new();

    for instance in topology_ref.all_instances() {
        if has_states!(instance, Expelled -> *) {
            continue;
        }

        if let Ok(replicaset) = topology_ref.replicaset_by_uuid(&instance.replicaset_uuid) {
            if replicaset.current_master_name == instance.name {
                masters.push(instance.name.clone());
            } else {
                replicas.push(instance.name.clone());
            }
        } else {
            warn_or_panic!("unknown replicaset info for instance {}", instance.name);
            // Let's just in case treat it as a replica
            replicas.push(instance.name.clone());
        }
    }

    let rpc_master = rpc::ddl_backup::Request {
        term,
        applied,
        timeout: sync_timeout,
        is_master: true,
    };

    let mut rpc_replica = rpc_master.clone();
    rpc_replica.is_master = false;

    let rpc_clear = rpc::ddl_backup::RequestClear { backup_dir_name };

    Ok(Some(
        ApplyBackup {
            masters,
            rpc_master,
            replicas,
            rpc_replica,
            rpc_clear,
        }
        .into(),
    ))
}

////////////////////////////////////////////////////////////////////////////////
// collect_proc_apply_backup
////////////////////////////////////////////////////////////////////////////////

pub async fn collect_proc_apply_backup(
    targets: &[InstanceName],
    rpc: &rpc::ddl_backup::Request,
    pool: &ConnectionPool,
    rpc_timeout: Duration,
) -> Result<Result<Vec<(InstanceName, PathBuf)>, OnError>> {
    let mut fs = vec![];
    for instance_name in targets {
        tlog!(Info, "calling proc_apply_backup"; "instance_name" => %instance_name);
        let resp = pool.call(
            instance_name,
            proc_name!(proc_apply_backup),
            rpc,
            rpc_timeout,
        )?;
        fs.push(async move {
            match resp.await {
                Ok(rpc::ddl_backup::Response::BackupPath(r)) => {
                    tlog!(Info, "applied backup on instance";
                        "instance_name" => %instance_name,
                    );
                    Ok((instance_name.clone(), r))
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
    Ok(res)
}

////////////////////////////////////////////////////////////////////////////////
// collect_proc_backup_abort_clear
////////////////////////////////////////////////////////////////////////////////

pub async fn collect_proc_backup_abort_clear(
    targets: &[InstanceName],
    rpc: &rpc::ddl_backup::RequestClear,
    pool: &ConnectionPool,
    rpc_timeout: Duration,
) -> Result<Result<Vec<()>, OnError>> {
    let mut fs = vec![];
    for instance_name in targets {
        tlog!(Info, "calling proc_backup_abort_clear"; "instance_name" => %instance_name);
        let resp = pool.call(
            instance_name,
            proc_name!(proc_backup_abort_clear),
            rpc,
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
    Ok(res)
}
