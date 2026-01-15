use crate::governor::backup::handle_backup;
use crate::governor::plan::stage::Plan;
use crate::governor::plan::stage::*;
use crate::rpc;
use crate::schema::TableDef;
use crate::topology_cache::TopologyCacheRef;
use crate::traft::error::Error;
use crate::traft::error::ErrorInfo;
use crate::traft::op::Ddl;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;
use crate::traft::Result;
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
    pending_schema_change: &Option<Ddl>,
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

    let mut tier = None;

    if let Ddl::TruncateTable { id, .. } = ddl {
        let table_def = tables.get(id).expect("failed to get table_def");
        tier = table_def.distribution.in_tier().cloned();

        if tier.is_none() {
            // This is a TRUNCATE on global table. RPC is not required, the
            // operation is applied locally on each instance of the cluster
            // when the corresponding DdlCommit is applied in raft_main_loop
            return Ok(Some(
                ApplySchemaChange {
                    tier: None,
                    rpc: None,
                    targets: vec![],
                }
                .into(),
            ));
        }
    }

    let targets = rpc::replicasets_masters(topology_ref);

    let rpc = Some(rpc::ddl_apply::Request {
        tier: tier.clone(),
        term,
        applied,
        timeout: sync_timeout,
    });

    return Ok(Some(ApplySchemaChange { tier, rpc, targets }.into()));
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
