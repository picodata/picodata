use crate::op::Ddl;
use crate::storage::schema::ddl_change_format_on_master;
use crate::storage::schema::ddl_create_function_on_master;
use crate::storage::schema::ddl_create_index_on_master;
use crate::storage::schema::ddl_create_space_on_master;
use crate::storage::schema::ddl_drop_function_on_master;
use crate::storage::schema::ddl_drop_index_on_master;
use crate::storage::schema::ddl_drop_space_on_master;
use crate::storage::schema::ddl_rename_function_on_master;
use crate::storage::schema::ddl_rename_table_on_master;
use crate::storage::schema::ddl_truncate_space_on_master;
use crate::storage::Catalog;
use crate::storage::{local_schema_version, set_local_schema_version};
use crate::tlog;
use crate::traft::error::Error as TraftError;
use crate::traft::error::ErrorInfo;
use crate::traft::node;
use crate::traft::{RaftIndex, RaftTerm};
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
    static LOCK: Rc<fiber::Mutex<()>> = Rc::new(fiber::Mutex::new(()));
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
        let lock = LOCK.with(Rc::clone);
        let _guard = lock.lock();

        let pending_schema_version = storage.properties.pending_schema_version()?
            .ok_or_else(|| TraftError::other("pending schema version not found"))?;
        // Already applied.
        if local_schema_version()? >= pending_schema_version {
            return Ok(Response::Ok);
        }

        if crate::tarantool::eval("return box.info.ro")? {
            let e = BoxError::new(
                TarantoolErrorCode::Readonly,
                "cannot apply schema change on a read only instance"
            );
            return Err(e.into());
        }

        let ddl = storage.properties.pending_schema_change()?
            .ok_or_else(|| TraftError::other("pending schema change not found"))?;

        let res = transaction(|| apply_schema_change(storage, &ddl, pending_schema_version, false));
        match res {
            Ok(()) => Ok(Response::Ok),
            Err(TransactionError::RolledBack(Error::Aborted(err))) => {
                tlog!(Warning, "schema change aborted: {err}");

                let instance_name = node.raft_storage.instance_name()?.expect("should be persisted before procs are defined");
                let cause = ErrorInfo {
                    error_code: err.error_code(),
                    message: err.to_string(),
                    instance_name,
                };
                Ok(Response::Abort { cause })
            }
            Err(err) => {
                tlog!(Warning, "applying schema change failed: {err}");
                Err(err.into())
            }
        }
    }

    pub struct Request {
        pub term: RaftTerm,
        pub applied: RaftIndex,
        pub timeout: Duration,
        pub tier: Option<String>,
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
/// `is_commit` is `true` if schema change is being applied in response to a
/// [`DdlCommit`] raft entry, else it's `false`.
///
/// [`DdlCommit`]: crate::traft::op::Op::DdlCommit
pub fn apply_schema_change(
    storage: &Catalog,
    ddl: &Ddl,
    version: u64,
    is_commit: bool,
) -> Result<(), Error> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });

    // Injection allowing to block the transaction for an indefinite period of time.
    crate::error_injection!(block "BLOCK_APPLY_SCHEMA_CHANGE_TRANSACTION");

    match *ddl {
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
            let space = storage
                .tables
                .get(id)
                .map_err(|e| Error::Aborted(e.into()))?
                .expect("failed to get space");
            // We have to skip truncate application in case it should be applied
            // only on a specific tier (case of sharded table).
            let should_apply = match &space.distribution {
                crate::schema::Distribution::Global => true,
                crate::schema::Distribution::ShardedImplicitly { tier: tier_ddl, .. }
                | crate::schema::Distribution::ShardedByField { tier: tier_ddl, .. } => {
                    let node = node::global().map_err(|e| Error::Aborted(e.into()))?;
                    let tier_node = node.topology_cache.my_tier_name();

                    tier_node == tier_ddl.as_str()
                }
            };

            if should_apply {
                let abort_reason = ddl_truncate_space_on_master(id).map_err(Error::Other)?;
                if let Some(e) = abort_reason {
                    return Err(Error::Aborted(e.into()));
                }
            }
        }

        Ddl::ChangeFormat {
            table_id,
            ref new_format,
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
                return Ok(());
            }

            if let Err(e) = ddl_drop_index_on_master(space_id, index_id) {
                return Err(Error::Aborted(e));
            }
        }
    }

    if let Err(e) = set_local_schema_version(version) {
        return Err(Error::Aborted(e.into()));
    }

    Ok(())
}
