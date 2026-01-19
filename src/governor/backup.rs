use crate::governor::batch::get_next_batch;
use crate::governor::batch::LastStepInfo;
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
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use std::time::Duration;

////////////////////////////////////////////////////////////////////////////////
// handle_backup
////////////////////////////////////////////////////////////////////////////////

/// Prepares governor actions for applying a pending BACKUP operation.
///
/// This function is called from [`handle_pending_ddl`]
pub fn handle_backup<'i>(
    last_step_info: &mut LastStepInfo,
    topology_ref: &TopologyCacheRef,
    pending_schema_version: u64,
    timestamp: i64,
    term: RaftTerm,
    applied: RaftIndex,
    sync_timeout: Duration,
    batch_size: usize,
) -> Result<Option<Plan<'i>>> {
    // We must check if step kind is different from the one we tried on
    // previous iteration, so that we know not to use irrelevant results
    type Action = ApplyBackup;
    let step_kind = Action::KIND;
    last_step_info.update_step_kind(step_kind);

    let backup_dir_name = get_backup_dir_name(timestamp);

    let mut masters_total = Vec::new();
    let mut replicas_total = Vec::new();

    for instance in topology_ref.all_instances() {
        if has_states!(instance, Expelled -> *) {
            continue;
        }

        if let Ok(replicaset) = topology_ref.replicaset_by_uuid(&instance.replicaset_uuid) {
            if replicaset.current_master_name == instance.name {
                masters_total.push(instance.name.clone());
            } else {
                replicas_total.push(instance.name.clone());
            }
        } else {
            warn_or_panic!("unknown replicaset info for instance {}", instance.name);
            // Let's just in case treat it as a replica
            replicas_total.push(instance.name.clone());
        }
    }

    let mut masters_batch = vec![];
    // get_next_batch expects a non-empty input
    if !masters_total.is_empty() {
        let res = get_next_batch(&masters_total, last_step_info, batch_size);
        masters_batch = match res {
            Ok(v) => v,
            Err(next_try) => {
                return Ok(Some(SleepDueToBackoff::new(next_try, step_kind).into()));
            }
        };
    }

    let mut replicas_batch = vec![];
    // We first handle all masters and only after that we handle replicas
    let all_masters_ok = masters_batch.is_empty();
    // get_next_batch expects a non-empty input
    let have_replicas = !replicas_total.is_empty();
    if all_masters_ok && have_replicas {
        let res = get_next_batch(&replicas_total, last_step_info, batch_size);
        replicas_batch = match res {
            Ok(v) => v,
            Err(next_try) => {
                return Ok(Some(SleepDueToBackoff::new(next_try, step_kind).into()));
            }
        };
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
        Action {
            pending_schema_version,
            masters_total,
            masters_batch,
            rpc_master,
            replicas_total,
            replicas_batch,
            rpc_replica,
            rpc_clear,
        }
        .into(),
    ))
}

////////////////////////////////////////////////////////////////////////////////
// collect_proc_apply_backup
////////////////////////////////////////////////////////////////////////////////

/// Sends `rpc` [`proc_apply_backup`] to the provided `targets`.
///
/// This function waits until responses to every request are received (or
/// timeout is exceeded). `last_step_info` is updated with with info about
/// results of every RPC, so that it can be used for the purposes of batching
/// and back-off on following iterations.
///
/// Returns `Err(Abort(reason))` if any instance replies with `Ok(Response::Abort { .. })`.
/// Returns `Err(Retry(e))` if RPC to any instance fails for any other reason.
/// Otherwise returns `Ok(())`
pub async fn collect_proc_apply_backup(
    last_step_info: &mut LastStepInfo,
    targets: Vec<InstanceName>,
    rpc: &rpc::ddl_backup::Request,
    pool: &ConnectionPool,
    rpc_timeout: Duration,
) -> Result<(), OnError> {
    let mut fs = FuturesUnordered::new();
    for instance_name in targets {
        tlog!(Info, "calling proc_apply_backup"; "instance_name" => %instance_name);
        let resp = pool
            .call(
                &instance_name,
                proc_name!(proc_apply_backup),
                rpc,
                rpc_timeout,
            )
            .map_err(OnError::Retry)?;
        fs.push(async move { (instance_name, resp.await) });
    }

    let mut abort_cause = None;
    let mut first_error = None;

    while let Some((instance_name, res)) = fs.next().await {
        match res {
            Ok(rpc::ddl_backup::Response::BackupPath(backup_path)) => {
                tlog!(Info, "applied backup on instance"; "instance_name" => %instance_name);
                last_step_info
                    .backup_paths
                    .insert(instance_name.clone(), backup_path);
                last_step_info.on_ok_instance(instance_name);
            }
            Ok(rpc::ddl_backup::Response::Abort { cause }) => {
                tlog!(Error, "failed to apply backup on instance: {cause}"; "instance_name" => %instance_name);
                // For now we only keep the error from the first failed
                // instance but maybe we should save them from all of them
                if abort_cause.is_none() {
                    abort_cause = Some(cause);
                }
            }
            Err(e) => {
                let info = last_step_info.on_err_instance(&instance_name);
                let streak = info.streak;
                tlog!(Warning, "failed calling proc_apply_schema_change (fail streak: {streak}): {e}"; "instance_name" => %instance_name);
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
    }

    if let Some(cause) = abort_cause {
        return Err(OnError::Abort(cause));
    }

    if let Some(error) = first_error {
        return Err(OnError::Retry(error));
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// collect_proc_backup_abort_clear
////////////////////////////////////////////////////////////////////////////////

/// Sends `rpc` [`proc_backup_abort_clear`] to the provided `targets`.
pub async fn collect_proc_backup_abort_clear(
    targets: &[InstanceName],
    rpc: &rpc::ddl_backup::RequestClear,
    pool: &ConnectionPool,
    rpc_timeout: Duration,
) -> Result<Result<Vec<()>>> {
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
                    Err(e)
                }
            }
        });
    }
    let res = try_join_all(fs).await;
    Ok(res)
}
