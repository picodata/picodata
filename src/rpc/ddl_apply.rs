use crate::op::Ddl;
use crate::storage::Clusterwide;
use crate::storage::{ddl_create_space_on_master, ddl_drop_space_on_master};
use crate::storage::{local_schema_version, set_local_schema_version};
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::Result;
use crate::traft::{RaftIndex, RaftTerm};
use std::time::Duration;
use tarantool::error::{TarantoolError, TarantoolErrorCode};
use tarantool::ffi::tarantool as ffi;

crate::define_rpc_request! {
    fn proc_apply_schema_change(req: Request) -> Result<Response> {
        let node = node::global()?;
        node.wait_index(req.applied, req.timeout)?;
        node.status().check_term(req.term)?;

        let storage = &node.storage;

        let pending_schema_version = storage.properties.pending_schema_version()?.ok_or_else(|| Error::other("pending schema version not found"))?;
        // Already applied.
        if local_schema_version()? >= pending_schema_version {
            return Ok(Response::Ok);
        }

        if crate::tarantool::eval("return box.info.ro")? {
            let e = tarantool::set_and_get_error!(
                TarantoolErrorCode::Readonly,
                "cannot apply schema change on a read only instance"
            );
            return Err(e.into());
        }

        let ddl = storage.properties.pending_schema_change()?.ok_or_else(|| Error::other("pending schema change not found"))?;

        // FIXME: start_transaction api is awful, it would be too ugly to
        // use here in the state it's currently in
        let rc = unsafe { ffi::box_txn_begin() };
        assert_eq!(rc, 0, "we're not in a transaction currently");

        // TODO: transaction may have already started, if we're in a process of
        // creating a big index. If governor sends a repeat rpc request to us we
        // should handle this correctly
        let res = apply_schema_change(storage, &ddl, pending_schema_version, false);
        match res {
            Ok(Response::Abort { .. }) | Err(_) => {
                let rc = unsafe { ffi::box_txn_rollback() };
                if rc != 0 {
                    let e = TarantoolError::last();
                    tlog!(Warning, "failed to rollback transaction: {e}");
                }
            }
            Ok(Response::Ok) => {
                let rc = unsafe { ffi::box_txn_commit() };
                if rc != 0 {
                    let e = TarantoolError::last();
                    tlog!(Warning, "failed to commit transaction: {e}");
                }
            }
        }

        res
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

/// Applies the schema change described by `ddl` to the tarantool storage. This
/// function is only called on replicaset masters, other replicas get the
/// changes via tarantool replication.
///
/// In case of successful schema change the local schema version will be set to
/// `version`. In case of [`Ddl::DropSpace`] and [`Ddl::DropIndex`] schema is
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
) -> Result<Response> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });

    match *ddl {
        Ddl::CreateSpace { id, .. } => {
            let abort_reason = ddl_create_space_on_master(storage, id)?;
            if let Some(e) = abort_reason {
                // We return Ok(error) because currently this is the only
                // way to report an application level error.
                return Ok(Response::Abort {
                    reason: e.to_string(),
                });
            }
        }

        Ddl::DropSpace { id } => {
            if !is_commit {
                // Space is only dropped on commit.
                return Ok(Response::Ok);
            }

            let abort_reason = ddl_drop_space_on_master(id)?;
            if let Some(e) = abort_reason {
                // We return Ok(error) because currently this is the only
                // way to report an application level error.
                return Ok(Response::Abort {
                    reason: e.to_string(),
                });
            }
        }

        _ => {
            todo!();
        }
    }

    if let Err(e) = set_local_schema_version(version) {
        // We return Ok(error) because currently this is the only
        // way to report an application level error.
        return Ok(Response::Abort {
            reason: e.to_string(),
        });
    }

    Ok(Response::Ok)
}
