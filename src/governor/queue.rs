use crate::cas;
use crate::catalog::governor_queue::{
    GovernorOpFormat, GovernorOpKind, GovernorOpStatus, GovernorOperationDef, GovernorQueue,
};
use crate::column_name;
use crate::governor::{
    upgrade_operations::CATALOG_UPGRADE_LIST, CreateGovernorQueue, FinishCatalogUpgrade,
    InsertUpgradeOperation, Plan, RunProcNameOperationStep, RunSqlOperationStep,
};
use crate::instance::Instance;
use crate::replicaset::{Replicaset, ReplicasetName};
use crate::rpc;
use crate::schema::{Distribution, TableDef, ADMIN_ID};
use crate::storage::{Properties, PropertyName, SystemTable};
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::op::{Ddl, Dml, Op};
use crate::traft::{RaftIndex, Result};
use crate::version::Version;
use tarantool::index::Part;
use tarantool::space::{SpaceEngineType, SpaceId, UpdateOps};

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

const MIN_PICODATA_VERSION_WITH_G_QUEUE: Version = Version::new_clean(25, 3, 0);

/// Handles operations from `_pico_governor_queue` table.
pub(super) fn handle_governor_queue<'i>(
    tables: &HashMap<SpaceId, &'i TableDef>,
    governor_operations: &'i [GovernorOperationDef],
    next_schema_version: u64,
    global_cluster_version: &str,
    pending_catalog_version: Option<String>,
    applied: RaftIndex,
    replicasets: &HashMap<&ReplicasetName, &'i Replicaset>,
    instances: &'i [Instance],
    sync_timeout: Duration,
) -> Result<Option<Plan<'i>>> {
    // check cluster version
    let global_cluster_version = Version::try_from(global_cluster_version)
        .expect("got from system table, should be already verified");
    if global_cluster_version
        .cmp_up_to_patch(&MIN_PICODATA_VERSION_WITH_G_QUEUE)
        .is_lt()
    {
        tlog!(
            Debug,
            "governor operations are available since version {} only",
            MIN_PICODATA_VERSION_WITH_G_QUEUE
        );
        return Ok(None);
    }

    // check if we have failed operations
    if let Some(failed) = governor_operations
        .iter()
        .find(|op| op.status == GovernorOpStatus::Failed)
    {
        tlog!(
            Error,
            "governor operation with id {} failed, stop to apply operations",
            failed.id
        );
        return Ok(None);
    }

    // check if we have pending system catalog upgrade
    if let Some(pending_catalog_version) = pending_catalog_version {
        return handle_catalog_upgrade(
            tables,
            governor_operations,
            next_schema_version,
            pending_catalog_version,
            applied,
            replicasets,
            instances,
            sync_timeout,
        );
    }

    // other (non-upgrade) operations
    if let Some(next_op) = governor_operations
        .iter()
        .find(|op| op.status == GovernorOpStatus::Pending && op.kind != GovernorOpKind::Upgrade)
    {
        return run_governor_operation(next_op, applied, replicasets, instances, sync_timeout);
    }

    return Ok(None);
}

/// Handles system catalog upgrade to version `pending_catalog_version`.
fn handle_catalog_upgrade<'i>(
    tables: &HashMap<SpaceId, &'i TableDef>,
    governor_operations: &'i [GovernorOperationDef],
    next_schema_version: u64,
    pending_catalog_version: String,
    applied: RaftIndex,
    replicasets: &HashMap<&ReplicasetName, &'i Replicaset>,
    instances: &'i [Instance],
    sync_timeout: Duration,
) -> Result<Option<Plan<'i>>> {
    tlog!(
        Info,
        "there is a pending system catalog upgrade to version {}",
        pending_catalog_version,
    );

    let create_table_plan =
        create_governor_table_if_not_exists(tables, next_schema_version, applied)?;
    if create_table_plan.is_some() {
        return Ok(create_table_plan);
    }

    // count system catalog upgrade operations
    let mut has_upgrade_operations = false;
    let mut pending_upgrade_operation = None;
    for operation in governor_operations
        .iter()
        .filter(|op| op.batch_id == pending_catalog_version)
    {
        has_upgrade_operations = true;
        if operation.status == GovernorOpStatus::Pending {
            pending_upgrade_operation = Some(operation);
            break;
        }
    }

    // check if we do not have upgrade operations in `_pico_governor_queue`
    // insert them in such case
    if !has_upgrade_operations {
        let insert_ops_plan = insert_catalog_upgrade_operations(
            governor_operations,
            &pending_catalog_version,
            applied,
        )?;
        if insert_ops_plan.is_some() {
            return Ok(insert_ops_plan);
        }
    }

    // check if we have at least one pending upgrade operation
    // run it in such case
    if let Some(next_op) = pending_upgrade_operation {
        return run_governor_operation(next_op, applied, replicasets, instances, sync_timeout);
    }

    // we have all upgrade operations completed here
    return finish_catalog_upgrade(pending_catalog_version, applied);
}

/// Runs operation from `_pico_governor_queue` table.
///
/// Returns one of governor's plans to run the operation.
fn run_governor_operation<'i>(
    op: &'i GovernorOperationDef,
    applied: RaftIndex,
    replicasets: &HashMap<&ReplicasetName, &'i Replicaset>,
    instances: &'i [Instance],
    sync_timeout: Duration,
) -> Result<Option<Plan<'i>>> {
    tlog!(Info, "next governor operation to apply: {}", op);
    match op.op_format {
        GovernorOpFormat::Sql => Ok(Some(
            RunSqlOperationStep {
                operation_id: op.id,
                query: &op.op,
                cas_on_success: make_change_status_cas(op.id, applied, false, None)?,
            }
            .into(),
        )),
        GovernorOpFormat::ProcName => {
            let cas_on_success = make_change_status_cas(op.id, applied, false, None)?;
            let rpc = rpc::ddl_apply::Request {
                term: cas_on_success.predicate.term,
                applied,
                timeout: sync_timeout,
                tier: None,
            };
            let masters: Vec<_> = rpc::replicasets_masters(replicasets, instances)
                .iter()
                .map(|(name, _)| *name)
                .collect();
            Ok(Some(
                RunProcNameOperationStep {
                    operation_id: op.id,
                    proc_name: &op.op,
                    targets: masters,
                    rpc,
                    cas_on_success,
                }
                .into(),
            ))
        }
    }
}

/// Creates `_pico_governor_queue` table if it does not exist yet.
/// Verifies that the table is operable.
///
/// Returns `CreateGovernorQueue` plan if the table needs to be created.
/// Returns `None` if the table already exists and is operable.
/// Returns error if the table exists but not operable.
fn create_governor_table_if_not_exists<'i>(
    tables: &HashMap<SpaceId, &'i TableDef>,
    next_schema_version: u64,
    applied: RaftIndex,
) -> Result<Option<Plan<'i>>> {
    let Some(governor_table) = tables.get(&GovernorQueue::TABLE_ID) else {
        tlog!(Info, "_pico_governor_queue table is missing, create it");

        let ddl = Ddl::CreateTable {
            id: GovernorQueue::TABLE_ID,
            name: GovernorQueue::TABLE_NAME.to_string(),
            format: GovernorQueue::format(),
            primary_key: vec![Part::field("id")],
            distribution: Distribution::Global,
            engine: SpaceEngineType::Memtx,
            owner: ADMIN_ID,
        };
        let ddl_prepare = Op::DdlPrepare {
            schema_version: next_schema_version,
            ddl,
            governor_op_id: None,
        };
        let predicate = cas::Predicate::new(applied, cas::schema_change_ranges());
        let cas = cas::Request::new(ddl_prepare, predicate, ADMIN_ID)?;

        return Ok(Some(CreateGovernorQueue { cas }.into()));
    };
    // we cannot get this case typically, but check it to be sure
    if !governor_table.operable {
        tlog!(Error, "_pico_governor_queue table exists, but not operable");
        return Err(Error::Other(
            "_pico_governor_queue table exists, but not operable".into(),
        ));
    }

    Ok(None)
}

/// Inserts system catalog upgrade operations into `_pico_governor_queue` table.
/// The operations are taken from `UPGRADE_OPERATIONS_MAP` static variable.
///
/// Returns `InsertUpgradeOperation` plan if there are upgrade operations to insert.
/// Returns `None` if there are no upgrade operations to insert.
fn insert_catalog_upgrade_operations<'i>(
    governor_operations: &'i [GovernorOperationDef],
    pending_catalog_version: &str,
    applied: RaftIndex,
) -> Result<Option<Plan<'i>>> {
    tlog!(
        Info,
        "insert governor operations for system catalog upgrade to version {}",
        pending_catalog_version
    );
    let Some((_, ops)) = CATALOG_UPGRADE_LIST
        .iter()
        .find(|u| u.0 == pending_catalog_version)
    else {
        tlog!(
            Warning,
            "no governor operations to insert for the system catalog upgrade to version {}",
            pending_catalog_version,
        );
        return Ok(None);
    };

    let last_operation_id;
    if let Some(last_operation) = governor_operations.iter().last() {
        last_operation_id = last_operation.id;
    } else {
        last_operation_id = 0;
    }

    let mut dmls = vec![];
    for (index, (op_format, op)) in ops.iter().enumerate() {
        let insert_def = GovernorOperationDef::new_upgrade(
            pending_catalog_version,
            index as u64 + last_operation_id + 1,
            op,
            GovernorOpFormat::from_str(op_format)
                .expect("got from constant, converting to GovernorOpFormat should never fail"),
        );
        let dml = Dml::replace(GovernorQueue::TABLE_ID, &insert_def, ADMIN_ID)?;
        dmls.push(dml);
    }

    let op = Op::single_dml_or_batch(dmls);
    let predicate = cas::Predicate::new(applied, []);
    let cas = cas::Request::new(op, predicate, ADMIN_ID)?;

    return Ok(Some(InsertUpgradeOperation { cas }.into()));
}

/// Finishes system catalog upgrade:
/// - remove `pending_catalog_version` from `_pico_property``
/// - update `system_catalog_version` in `_pico_property`.
///
/// Returns `FinishCatalogUpgrade` plan.
fn finish_catalog_upgrade<'i>(
    pending_catalog_version: String,
    applied: RaftIndex,
) -> Result<Option<Plan<'i>>> {
    tlog!(
        Info,
        "all upgrade operations are done successfully, need to update system_catalog_version"
    );
    let ops = vec![
        Dml::delete(
            Properties::TABLE_ID,
            &[PropertyName::PendingCatalogVersion],
            ADMIN_ID,
        )?,
        Dml::replace(
            Properties::TABLE_ID,
            &(PropertyName::SystemCatalogVersion, pending_catalog_version),
            ADMIN_ID,
        )?,
    ];
    let op = Op::BatchDml { ops };
    let predicate = cas::Predicate::new(applied, []);
    let cas = cas::Request::new(op, predicate, ADMIN_ID)?;

    return Ok(Some(FinishCatalogUpgrade { cas }.into()));
}

pub fn make_change_status_cas(
    operation_id: u64,
    applied: RaftIndex,
    is_error: bool,
    error_message: Option<String>,
) -> Result<cas::Request> {
    let mut ops = UpdateOps::new();
    let status = if is_error {
        GovernorOpStatus::Failed
    } else {
        GovernorOpStatus::Done
    };
    ops.assign(column_name!(GovernorOperationDef, status), status)?;
    if let Some(message) = error_message {
        ops.assign(
            column_name!(GovernorOperationDef, status_description),
            message,
        )?;
    }

    let dml = Dml::update(GovernorQueue::TABLE_ID, &[operation_id], ops, ADMIN_ID)?;
    let predicate = cas::Predicate::new(applied, vec![]);
    let cas = cas::Request::new(dml, predicate, ADMIN_ID)?;

    Ok(cas)
}
