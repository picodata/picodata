use crate::op::Ddl;
use crate::storage::{
    ddl_create_function_on_master, ddl_create_index_on_master, ddl_create_space_on_master,
    ddl_drop_function_on_master, ddl_drop_index_on_master, ddl_drop_space_on_master,
};
use crate::storage::{ddl_rename_function_on_master, Clusterwide};
use crate::storage::{local_schema_version, set_local_schema_version};
use crate::tlog;
use crate::traft::error::Error as TraftError;
use crate::traft::node;
use crate::traft::{RaftIndex, RaftTerm};
use std::time::Duration;
use tarantool::error::TarantoolErrorCode;
use tarantool::transaction::{transaction, TransactionError};

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

        let pending_schema_version = storage.properties.pending_schema_version()?
            .ok_or_else(|| TraftError::other("pending schema version not found"))?;
        // Already applied.
        if local_schema_version()? >= pending_schema_version {
            return Ok(Response::Ok);
        }

        if crate::tarantool::eval("return box.info.ro")? {
            let e = tarantool::error::BoxError::new(
                TarantoolErrorCode::Readonly,
                "cannot apply schema change on a read only instance"
            );
            return Err(e.into());
        }

        let ddl = storage.properties.pending_schema_change()?
            .ok_or_else(|| TraftError::other("pending schema change not found"))?;


        // TODO: transaction may have already started, if we're in a process of
        // creating a big index. If governor sends a repeat rpc request to us we
        // should handle this correctly
        let res = transaction(|| apply_schema_change(storage, &ddl, pending_schema_version, false));
        match res {
            Ok(()) => Ok(Response::Ok),
            Err(TransactionError::RolledBack(Error::Aborted(err))) => {
                tlog!(Warning, "schema change aborted: {err}");
                Ok(Response::Abort { reason: err})
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
    }

    pub enum Response {
        /// Schema change applied successfully on this instance.
        Ok,
        /// Schema change failed on this instance and should be aborted on the
        /// whole cluster.
        Abort { reason: String },
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Schema change failed on this instance and should be aborted on the
    /// whole cluster.
    #[error("{0}")]
    Aborted(String),

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
    storage: &Clusterwide,
    ddl: &Ddl,
    version: u64,
    is_commit: bool,
) -> Result<(), Error> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });

    match *ddl {
        Ddl::CreateTable { id, .. } => {
            let abort_reason = ddl_create_space_on_master(storage, id).map_err(Error::Other)?;
            if let Some(e) = abort_reason {
                return Err(Error::Aborted(e.to_string()));
            }
        }

        Ddl::DropTable { id, .. } => {
            if !is_commit {
                // Space is only dropped on commit.
                return Ok(());
            }

            let abort_reason = ddl_drop_space_on_master(id).map_err(Error::Other)?;
            if let Some(e) = abort_reason {
                return Err(Error::Aborted(e.to_string()));
            }
        }

        Ddl::CreateProcedure { id, ref name, .. } => {
            if let Err(e) = ddl_create_function_on_master(id, name) {
                return Err(Error::Aborted(e.to_string()));
            }
        }

        Ddl::DropProcedure { id, .. } => {
            if !is_commit {
                return Ok(());
            }

            let abort_reason = ddl_drop_function_on_master(id).map_err(Error::Other)?;
            if let Some(e) = abort_reason {
                return Err(Error::Aborted(e.to_string()));
            }
        }

        Ddl::RenameProcedure {
            routine_id,
            ref old_name,
            ref new_name,
            ..
        } => {
            if let Err(e) = ddl_rename_function_on_master(routine_id, old_name, new_name) {
                return Err(Error::Aborted(e.to_string()));
            }
        }

        Ddl::CreateIndex {
            space_id, index_id, ..
        } => {
            if let Err(e) = ddl_create_index_on_master(storage, space_id, index_id) {
                return Err(Error::Aborted(e.to_string()));
            }
        }

        Ddl::DropIndex {
            space_id, index_id, ..
        } => {
            if !is_commit {
                return Ok(());
            }

            if let Err(e) = ddl_drop_index_on_master(space_id, index_id) {
                return Err(Error::Aborted(e.to_string()));
            }
        }
    }

    if let Err(e) = set_local_schema_version(version) {
        return Err(Error::Aborted(e.to_string()));
    }

    Ok(())
}
