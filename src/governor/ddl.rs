use crate::cas;
use crate::governor::backup::handle_backup;
use crate::governor::batch::get_next_batch;
use crate::governor::batch::LastStepInfo;
use crate::governor::plan::stage::Plan;
use crate::governor::plan::stage::*;
use crate::has_states;
use crate::instance::InstanceName;
use crate::proc_name;
use crate::rpc;
use crate::rpc::ddl_apply::proc_apply_schema_change;
use crate::schema::TableDef;
use crate::schema::ADMIN_ID;
use crate::tlog;
use crate::topology_cache::TopologyCacheRef;
use crate::traft::error::Error;
use crate::traft::error::ErrorInfo;
use crate::traft::op::Ddl;
use crate::traft::op::Op;
use crate::traft::ConnectionPool;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;
use crate::traft::Result;
use crate::vshard;
use crate::warn_or_panic;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::time::Duration;
use tarantool::space::SpaceId;

////////////////////////////////////////////////////////////////////////////////
// handle_pending_ddl
////////////////////////////////////////////////////////////////////////////////

/// Prepares governor actions for applying a `pending_schema_change` operation
/// from `_pico_property` including
/// - regular DDL operations like CREATE TABLE
/// - TRUNCATE TABLE operation (implemented as DDL in tarantool)
/// - BACKUP operation
pub fn handle_pending_ddl<'i>(
    last_step_info: &mut LastStepInfo,
    topology_ref: &TopologyCacheRef,
    tables: &HashMap<SpaceId, &'i TableDef>,
    pending_ddl: &'i Option<(Ddl, u64)>,
    schema_version: u64,
    term: RaftTerm,
    applied: RaftIndex,
    sync_timeout: Duration,
    batch_size: usize,
) -> Result<Option<Plan<'i>>> {
    let Some((ref ddl, pending_schema_version)) = *pending_ddl else {
        return Ok(None);
    };

    // Clear previous results if schema version changed, because it means that
    // we're applying a different operation
    last_step_info.update_schema_info(schema_version, pending_schema_version);

    if let Ddl::Backup { timestamp } = ddl {
        return handle_backup(
            last_step_info,
            topology_ref,
            pending_schema_version,
            *timestamp,
            term,
            applied,
            sync_timeout,
            batch_size,
        );
    }

    if let Ddl::TruncateTable { id, .. } = ddl {
        return handle_truncate_table(
            last_step_info,
            topology_ref,
            tables,
            schema_version,
            pending_schema_version,
            *id,
            term,
            applied,
            sync_timeout,
            batch_size,
        );
    }

    // We must check if step kind is different from the one we tried on
    // previous iteration, so that we know not to use irrelevant results
    type Action<'a> = ApplySchemaChange<'a>;
    let step_kind = Action::KIND;
    last_step_info.update_step_kind(step_kind);

    let targets_total = rpc::replicasets_masters(topology_ref);
    debug_assert!(!targets_total.is_empty());

    let res = get_next_batch(&targets_total, last_step_info, batch_size);
    let targets_batch = match res {
        Ok(v) => v,
        Err(next_try) => {
            return Ok(Some(SleepDueToBackoff::new(next_try, step_kind).into()));
        }
    };

    let rpc = rpc::ddl_apply::Request {
        term,
        applied,
        timeout: sync_timeout,
    };

    Ok(Some(
        Action {
            ddl,
            pending_schema_version,
            schema_version,
            rpc,
            targets_total,
            targets_batch,
        }
        .into(),
    ))
}

////////////////////////////////////////////////////////////////////////////////
// handle_truncate_table
////////////////////////////////////////////////////////////////////////////////

/// Prepares governor actions for applying a pending TRUNCATE TABLE operation.
///
/// Note that for global tables the only action is to immediately commit the
/// operation as it will be applied locally on all instances when the
/// corresponding raft entry is applied.
///
/// Meanwhile truncate for sharded tables involves RPC to masters of all the
/// replicasets in cluster.
///
/// This function is called from [`handle_pending_ddl`]
fn handle_truncate_table<'i>(
    last_step_info: &mut LastStepInfo,
    topology_ref: &TopologyCacheRef,
    tables: &HashMap<SpaceId, &'i TableDef>,
    schema_version: u64,
    pending_schema_version: u64,
    table_id: SpaceId,
    term: RaftTerm,
    applied: RaftIndex,
    sync_timeout: Duration,
    batch_size: usize,
) -> Result<Option<Plan<'i>>> {
    let table_def = tables.get(&table_id).expect("failed to get table_def");
    let tier = table_def.distribution.in_tier().cloned();

    let Some(tier) = tier else {
        // This is a TRUNCATE on global table. RPC is not required, the
        // operation is applied locally on each instance of the cluster
        // when the corresponding DdlCommit is applied in raft_main_loop
        let ranges = cas::Range::for_op(&Op::DdlCommit)?;
        let predicate = cas::Predicate::new(applied, ranges);
        let cas = cas::Request::new(Op::DdlCommit, predicate, ADMIN_ID)?;
        return Ok(Some(
            CommitTruncateGlobalTable {
                pending_schema_version,
                cas,
            }
            .into(),
        ));
    };

    // We must check if step kind is different from the one we tried on
    // previous iteration, so that we know not to use irrelevant results
    type Action = ApplyTruncateTable;
    let step_kind = Action::KIND;
    last_step_info.update_step_kind(step_kind);

    let mut tier_masters_count = 0;
    let mut other_tiers_masters_total = vec![];

    for replicaset in topology_ref.all_replicasets() {
        let Ok(master) = topology_ref.instance_by_name(&replicaset.current_master_name) else {
            warn_or_panic!(
                "couldn't find instance with name {}, which is chosen as master of replicaset {}",
                replicaset.current_master_name,
                replicaset.name,
            );
            // Send them a request anyway just to be safe
            if replicaset.tier == tier {
                tier_masters_count += 1;
            } else {
                other_tiers_masters_total.push(replicaset.current_master_name.clone());
            }
            continue;
        };

        if has_states!(master, Expelled -> *) {
            continue;
        }

        if replicaset.tier == tier {
            tier_masters_count += 1;
        } else {
            other_tiers_masters_total.push(master.name.clone());
        }
    }

    let mut other_tiers_masters_batch = vec![];
    if
    // As first substep we do the vshard map_callrw for the actual
    // implementation of TRUNCATE. And only after it was done we send the
    // dummy proc_apply_schema_change RPC to other tiers' masters so that
    // they bump their schema versions
    last_step_info.truncate_map_callrw_ok
        // get_next_batch expects a non-empty input
        && !other_tiers_masters_total.is_empty()
    {
        let res = get_next_batch(&other_tiers_masters_total, last_step_info, batch_size);
        other_tiers_masters_batch = match res {
            Ok(v) => v,
            Err(next_try) => {
                return Ok(Some(SleepDueToBackoff::new(next_try, step_kind).into()));
            }
        };
    }

    let rpc = rpc::ddl_apply::Request {
        term,
        applied,
        timeout: sync_timeout,
    };

    let ranges = cas::Range::for_op(&Op::DdlCommit)?;
    let predicate = cas::Predicate::new(applied, ranges);
    let success_cas = cas::Request::new(Op::DdlCommit, predicate, ADMIN_ID)?;

    Ok(Some(
        Action {
            schema_version,
            pending_schema_version,
            tier,
            rpc,
            tier_masters_count,
            other_tiers_masters_total,
            other_tiers_masters_batch,
            success_cas,
        }
        .into(),
    ))
}

////////////////////////////////////////////////////////////////////////////////
// collect_vshard_map_callrw_proc_apply_schema_change
////////////////////////////////////////////////////////////////////////////////

/// Sends `rpc` [`proc_apply_schema_change`] to all masters in `tier` via the
/// vshard router `map_callrw` API. This is different from all other governor
/// actions because TRUNCATE must block bucket rebalancing and vshard helps us
/// with that. This may change in future.
///
/// Truncate operation can never result in a `DdlAbort` so if this function
/// returns any error it is safe to attempt the exact same RPC later.
pub fn collect_vshard_map_callrw_proc_apply_schema_change(
    tier: &str,
    expected_response_count: usize,
    rpc: &rpc::ddl_apply::Request,
    rpc_timeout: Duration,
) -> Result<()> {
    let res = vshard::ddl_map_callrw(
        tier,
        proc_name!(proc_apply_schema_change),
        rpc_timeout,
        &rpc,
    );

    let responses = match res {
        Ok(v) => v,
        Err(e) => {
            // E.g. we faced with timeout.
            tlog!(Error, "failed to execute map_callrw for TRUNCATE: {e}";);
            return Err(e);
        }
    };

    let actual_response_count = responses.len();
    if actual_response_count != expected_response_count {
        // Some of the replicasets' masters went down so we've executed our
        // `proc_apply_schema_change` only on some of them. Have to retry the query.
        tlog!(
            Error,
            "failed to execute map_callrw for TRUNCATE: some masters are down";
            "expected" => %expected_response_count,
            "actual" => %actual_response_count
        );
        return Err(Error::other(
            format!("failed to execute map_callrw for TRUNCATE: expected {expected_response_count} masters, got {actual_response_count}")
        ));
    }

    #[cfg(debug_assertions)]
    for res in responses {
        // Applying TRUNCATE never results in an Abort, and if the retriable
        // errors are handled above.
        debug_assert!(
            matches!(res.response, rpc::ddl_apply::Response::Ok),
            "{res:?}"
        );
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// collect_proc_apply_schema_change
////////////////////////////////////////////////////////////////////////////////

/// Sends `rpc` [`proc_apply_schema_change`] to the provided `targets`.
///
/// Note that governor only calls this function with `targets` being the masters
/// of all replicasets from all tiers except the one that the table belongs to.
/// We do it this way because only the affected tier stores the data, but all
/// other tiers must bump their `local_schema_version` in the `_schema` system
/// space. This is a side-effect of using DdlPrepare/DdlCommit raft entries
/// for the truncate operation.
///
/// This function waits until responses to every request are received (or
/// timeout is exceeded). `last_step_info` is updated with with info about
/// results of every RPC, so that it can be used for the purposes of batching
/// and back-off on following iterations.
///
/// Truncate operation can never result in a `DdlAbort` so if this function
/// returns any error it is safe to attempt the exact same RPC later.
pub async fn collect_proc_apply_schema_change(
    last_step_info: &mut LastStepInfo,
    targets: Vec<InstanceName>,
    rpc: &rpc::ddl_apply::Request,
    pool: &ConnectionPool,
    rpc_timeout: Duration,
) -> Result<(), OnError> {
    let mut fs = FuturesUnordered::new();
    for instance_name in targets {
        tlog!(Info, "calling proc_apply_schema_change"; "instance_name" => %instance_name);
        let resp = pool
            .call(
                &instance_name,
                proc_name!(proc_apply_schema_change),
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
            Ok(rpc::ddl_apply::Response::Ok) => {
                tlog!(Info, "applied schema change on instance"; "instance_name" => %instance_name);
                last_step_info.on_ok_instance(instance_name);
            }
            Ok(rpc::ddl_apply::Response::Abort { cause }) => {
                tlog!(Error, "failed to apply schema change on instance: {cause}"; "instance_name" => %instance_name);
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
// OnError
////////////////////////////////////////////////////////////////////////////////

/// Helper enum for ApplySchemaChange handling.
#[derive(Debug)]
pub enum OnError {
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
