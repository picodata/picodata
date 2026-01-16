use crate::cas;
use crate::governor::backup::handle_backup;
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
use futures::future::try_join_all;
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
    topology_ref: &TopologyCacheRef,
    tables: &HashMap<SpaceId, &'i TableDef>,
    pending_schema_change: &'i Option<Ddl>,
    term: RaftTerm,
    applied: RaftIndex,
    sync_timeout: Duration,
) -> Result<Option<Plan<'i>>> {
    let Some(ddl) = pending_schema_change else {
        return Ok(None);
    };

    if let Ddl::Backup { timestamp } = ddl {
        return handle_backup(topology_ref, *timestamp, term, applied, sync_timeout);
    }

    if let Ddl::TruncateTable { id, .. } = ddl {
        return handle_truncate_table(topology_ref, tables, *id, term, applied, sync_timeout);
    }

    let targets = rpc::replicasets_masters(topology_ref);

    let rpc = rpc::ddl_apply::Request {
        term,
        applied,
        timeout: sync_timeout,
    };

    Ok(Some(ApplySchemaChange { ddl, rpc, targets }.into()))
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
    topology_ref: &TopologyCacheRef,
    tables: &HashMap<SpaceId, &'i TableDef>,
    table_id: SpaceId,
    term: RaftTerm,
    applied: RaftIndex,
    sync_timeout: Duration,
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
        return Ok(Some(CommitTruncateGlobalTable { cas }.into()));
    };

    let mut tier_masters_count = 0;
    let mut other_tiers_masters = vec![];

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
                other_tiers_masters.push(replicaset.current_master_name.clone());
            }
            continue;
        };

        if has_states!(master, Expelled -> *) {
            continue;
        }

        if replicaset.tier == tier {
            tier_masters_count += 1;
        } else {
            other_tiers_masters.push(master.name.clone());
        }
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
        ApplyTruncateTable {
            tier,
            rpc,
            tier_masters_count,
            other_tiers_masters,
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
/// Truncate operation can never result in a `DdlAbort` so if this function
/// returns any error it is safe to attempt the exact same RPC later.
pub async fn collect_proc_apply_schema_change(
    targets: &[InstanceName],
    rpc: &rpc::ddl_apply::Request,
    pool: &ConnectionPool,
    rpc_timeout: Duration,
) -> Result<Result<Vec<()>, OnError>> {
    let mut fs = vec![];
    for instance_name in targets {
        tlog!(Info, "calling proc_apply_schema_change"; "instance_name" => %instance_name);
        let resp = pool.call(
            instance_name,
            proc_name!(proc_apply_schema_change),
            rpc,
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
    Ok(res)
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
