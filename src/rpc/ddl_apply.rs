use crate::catalog::governor_queue::GovernorOpFormat;
use crate::op::Ddl;
use crate::preemption::sql_preemption;
use crate::sql::storage::StorageRuntime;
use crate::storage::schema::ddl_change_format_on_master;
use crate::storage::schema::ddl_create_function_on_master;
use crate::storage::schema::ddl_create_index_on_master;
use crate::storage::schema::ddl_create_space_on_master;
use crate::storage::schema::ddl_create_tt_proc_on_master;
use crate::storage::schema::ddl_drop_function_on_master;
use crate::storage::schema::ddl_drop_index_on_master;
use crate::storage::schema::ddl_drop_space_on_master;
use crate::storage::schema::ddl_rename_function_on_master;
use crate::storage::schema::ddl_rename_index_on_master;
use crate::storage::schema::ddl_rename_table_on_master;
use crate::storage::schema::ddl_truncate_space_on_master;
use crate::storage::Catalog;
use crate::storage::{local_schema_version, set_local_schema_version};
use crate::tlog;
use crate::traft::error::Error as TraftError;
use crate::traft::error::ErrorInfo;
use crate::traft::node;
use crate::traft::{RaftIndex, RaftTerm};
use sql::executor::engine::QueryCache;
use std::rc::Rc;
use std::time::Duration;
use tarantool::error::{BoxError, TarantoolErrorCode};
use tarantool::fiber;
use tarantool::transaction::{transaction, TransactionError};

// A global lock for schema changes that solves the following issue:
// An expensive index creation can cause RPC timeouts, resulting in re-sending of the
// same RPC even though the operation is still in progress on the master. The second RPC
// attempts to create the same index, but it realizes that the index already exists,
// even if the operation has not been completed yet. To handle this scenario, a global
// lock was added for `apply_schema_change` to prevent concurrent schema changes.
//
// Note: The issue was discovered in
// `<https://git.picodata.io/picodata/picodata/picodata/-/issues/748>`.
thread_local! {
    pub static LOCK_APPLY_SCHEMA_CHANGE: Rc<fiber::Mutex<()>> = Rc::new(fiber::Mutex::new(()));
}

crate::define_rpc_request! {
    /// Forces the target instance to actually apply the pending schema change locally.
    ///
    /// Should be called by a governor on every replicaset master in the cluster
    /// at the corresponding stage of the schema change algorithm.
    ///
    /// Returns errors in the following cases:
    /// 1. Raft node on a receiving peer is not yet initialized
    /// 2. Storage failure
    /// 3. Timeout while waiting for an index from the request
    /// 4. Request has an incorrect term - leader changed
    /// 5. The procedure was called on a read_only instance
    /// 6. Failed to apply the schema change
    fn proc_apply_schema_change(req: Request) -> crate::traft::Result<Response> {
        let node = node::global()?;
        node.wait_index(req.applied, req.timeout)?;
        node.status().check_term(req.term)?;

        let storage = &node.storage;

        // While the schema change is being applied, repeated RPCs will be blocked by this lock.
        // Once the change is applied and the lock is released, repeated RPC will finish quickly
        // after checking the schema versions.
        let lock = LOCK_APPLY_SCHEMA_CHANGE.with(Rc::clone);
        let _guard = lock.lock();

        if node.is_readonly() {
            let e = BoxError::new(
                TarantoolErrorCode::Readonly,
                "cannot apply schema change on a read only instance"
            );
            return Err(e.into());
        }

        let Some(pending_schema_version) = storage.properties.pending_schema_version()? else {
            let pending_catalog_version = storage.properties.pending_catalog_version()?;
            if let Some(next_op) = storage.governor_queue.next_pending_operation(pending_catalog_version.as_deref())? {
                if next_op.op_format == GovernorOpFormat::ProcName {
                    ddl_create_tt_proc_on_master(&next_op.op)?;
                    return Ok(Response::Ok);
                }
            }
            return Err(TraftError::other("pending schema version not found"));
        };

        // Already applied.
        if local_schema_version()? >= pending_schema_version {
            return Ok(Response::Ok);
        }

        let ddl = storage.properties.pending_schema_change()?
            .ok_or_else(|| TraftError::other("pending schema change not found"))?;

        let my_tier_name = node.topology_cache.my_tier_name();


        let sql_runtime;
        let _sql_guard;
        if sql_preemption() {
            // We want to lock DQL operations, so we don't have scenarios where we yield DQL execution,
            // change a space and then resume DQL on the corrupted space. It can lead to undefined results.
            sql_runtime = StorageRuntime::new();
            _sql_guard = sql_runtime.cache().lock();
        }

        let res = transaction(|| apply_schema_change(storage, &ddl, pending_schema_version, false, my_tier_name));
        match res {
            Ok(()) => {}
            Err(TransactionError::RolledBack(Error::Aborted(err))) => {
                tlog!(Warning, "schema change aborted: {err}");

                let instance_name = node.raft_storage.instance_name()?.expect("should be persisted before procs are defined");
                let cause = ErrorInfo {
                    error_code: err.error_code(),
                    message: err.to_string(),
                    instance_name,
                };
                return Ok(Response::Abort { cause });
            }
            Err(err) => {
                tlog!(Warning, "applying schema change failed: {err}");
                return Err(err.into());
            }
        }

        crate::error_injection!(exit "EXIT_AFTER_PROC_APPLY_SCHEMA_CHANGE");

        Ok(Response::Ok)
    }

    pub struct Request {
        pub term: RaftTerm,
        pub applied: RaftIndex,
        pub timeout: Duration,
    }

    pub enum Response {
        /// Schema change applied successfully on this instance.
        Ok,
        /// Schema change failed on this instance and should be aborted on the
        /// whole cluster.
        Abort { cause: ErrorInfo },
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Schema change failed on this instance and should be aborted on the
    /// whole cluster.
    #[error("{0}")]
    Aborted(TraftError),

    #[error("{0}")]
    Other(TraftError),
}

/// Applies the schema change described by `ddl` to the tarantool storage. This
/// function is only called on replicaset masters, other replicas get the
/// changes via tarantool replication.
///
/// In case of successful schema change the local schema version will be set to
/// `version`. In case of [`Ddl::DropTable`] and [`Ddl::DropIndex`] schema is
/// only changed if `is_commit` is `true`.
///
/// The space and index definitions are extracted from picodata storage via
/// `storage`.
///
/// `is_commit` is
/// - `false` when called from [`proc_apply_schema_change`] RPC by governor
/// - `true` when called from `handle_committed_normal_entry` in `raft_main_loop`
///   when applying a [`DdlCommit`] raft entry
///
/// [`DdlCommit`]: crate::traft::op::Op::DdlCommit
pub fn apply_schema_change(
    storage: &Catalog,
    ddl: &Ddl,
    version: u64,
    is_commit: bool,
    my_tier_name: &str,
) -> Result<(), Error> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });

    // Injection allowing to block the transaction for an indefinite period of time.
    crate::error_injection!(block "BLOCK_APPLY_SCHEMA_CHANGE_TRANSACTION");

    match *ddl {
        Ddl::Backup { .. } => {
            if !is_commit {
                unreachable!("BACKUP should not be met under apply_schema_change call unless it's a catch up")
            }
            // TODO: See https://git.picodata.io/core/picodata/-/issues/2183.

            // Backup is implemented in a different way compared to other DDL
            // operations (because it's not really a DDL), governor calls
            // `apply_backup` instead of `apply_schema_change`. We will only
            // get here if a catching up instance will encounter a Ddl::Backup
            // entry in the raft log after the operation was already applied, so
            // we simply ignore the operation and just update the
            // local_schema_version at the end.
        }

        Ddl::CreateTable { id, .. } => {
            let abort_reason = ddl_create_space_on_master(storage, id).map_err(Error::Other)?;
            if let Some(e) = abort_reason {
                if let tarantool::error::Error::Tarantool(e) = &e {
                    if let Some((file, line)) = e.file().zip(e.line()) {
                        tlog!(Error, "{}:{}: {e}", file, line);
                    }
                }
                return Err(Error::Aborted(e.into()));
            }
        }

        Ddl::DropTable { id, .. } => {
            // That means function called from governor.
            if !is_commit {
                crate::vshard::disable_rebalancer().map_err(Error::Other)?;

                // Space is only dropped on commit.
                // Don't change local_schema_version because the change will be
                // appied later in raft_main_loop.
                return Ok(());
            }

            let abort_reason = ddl_drop_space_on_master(id).map_err(Error::Other)?;
            if let Some(e) = abort_reason {
                return Err(Error::Aborted(e.into()));
            }

            crate::vshard::enable_rebalancer().map_err(Error::Other)?;
        }

        Ddl::RenameTable {
            table_id,
            ref new_name,
            ..
        } => {
            if let Err(e) = ddl_rename_table_on_master(table_id, new_name) {
                return Err(Error::Aborted(e));
            }
        }

        Ddl::TruncateTable { id, .. } => {
            let table = storage
                .pico_table
                .get(id)
                .map_err(|e| Error::Aborted(e.into()))?
                .expect("failed to get space");
            // We have to skip truncate application in case it should be applied
            // only on a specific tier (case of sharded table).
            let should_apply = if let Some(tier) = table.distribution.in_tier() {
                tier == my_tier_name
            } else {
                table.distribution.is_global()
            };

            if should_apply {
                let res = ddl_truncate_space_on_master(id).map_err(Error::Other)?;
                if let Some(e) = res {
                    return Err(Error::Other(e.into()));
                }
            }
        }

        Ddl::ChangeFormat {
            table_id,
            ref new_format,
            // we don't need to care about column renames here because tarantool operates on column indices under the hood, yay
            ..
        } => {
            if let Err(e) = ddl_change_format_on_master(table_id, new_format) {
                return Err(Error::Aborted(e.into()));
            }
        }

        Ddl::CreateProcedure { id, .. } => {
            if let Err(e) = ddl_create_function_on_master(storage, id) {
                return Err(Error::Aborted(e));
            }
        }

        Ddl::DropProcedure { id, .. } => {
            if !is_commit {
                // Don't change local_schema_version because the change will be
                // appied later in raft_main_loop.
                return Ok(());
            }

            let abort_reason = ddl_drop_function_on_master(id).map_err(Error::Other)?;
            if let Some(e) = abort_reason {
                return Err(Error::Aborted(e.into()));
            }
        }

        Ddl::RenameProcedure {
            routine_id,
            ref new_name,
            ..
        } => {
            if let Err(e) = ddl_rename_function_on_master(storage, routine_id, new_name) {
                return Err(Error::Aborted(e));
            }
        }

        Ddl::CreateIndex {
            space_id, index_id, ..
        } => {
            if let Err(e) = ddl_create_index_on_master(storage, space_id, index_id) {
                return Err(Error::Aborted(e));
            }
        }

        Ddl::DropIndex {
            space_id, index_id, ..
        } => {
            if !is_commit {
                // Don't change local_schema_version because the change will be
                // appied later in raft_main_loop.
                return Ok(());
            }

            if let Err(e) = ddl_drop_index_on_master(space_id, index_id) {
                return Err(Error::Aborted(e));
            }
        }

        Ddl::RenameIndex {
            space_id,
            index_id,
            ref new_name,
            ..
        } => {
            if let Err(e) = ddl_rename_index_on_master(space_id, index_id, new_name) {
                return Err(Error::Aborted(e));
            }
        }
    }

    if let Err(e) = set_local_schema_version(version, "DDL") {
        return Err(Error::Aborted(e.into()));
    }

    Ok(())
}
