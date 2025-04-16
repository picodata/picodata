use std::time::Duration;
use tarantool::transaction::{transaction, TransactionError};

use crate::catalog::governor_queue::GovernorOpFormat;
use crate::rpc::ddl_apply::LOCK_APPLY_SCHEMA_CHANGE;
use crate::storage::schema::ddl_create_tt_proc_on_master;
use crate::storage::{get_backup_dir_name, local_schema_version, schema};
use crate::tlog;
use crate::traft::error::{Error as TraftError, ErrorInfo};
use crate::traft::op::Ddl;
use crate::traft::{node, RaftIndex, RaftTerm};
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;

fn get_backup_path(timestamp: i64) -> PathBuf {
    let config = &crate::config::PicodataConfig::get().instance;
    let backup_dir = config.backup_dir();
    let backup_dir_name = get_backup_dir_name(timestamp);
    backup_dir.join(&backup_dir_name)
}

crate::define_rpc_request! {
    /// Forces the target instance to actually apply the backup locally.
    ///
    /// Should be called by a governor on:
    /// 1. every replicaset master in the cluster
    /// 2. all other instances (replicas)
    ///
    /// Returns errors in the following cases:
    /// 1. Raft node on a receiving peer is not yet initialized
    /// 2. Storage failure
    /// 3. Timeout while waiting for an index from the request
    /// 4. Request has an incorrect term - leader changed
    /// 5. The procedure was called on a read_only instance
    /// 6. Failed to apply the backup
    fn proc_apply_backup(req: Request) -> crate::traft::Result<Response> {
        let node = node::global()?;
        node.wait_index(req.applied, req.timeout)?;
        node.status().check_term(req.term)?;

        let storage = &node.storage;

        // While the backup is being applied, repeated RPCs will be blocked by this lock.
        // Once the backup is applied and the lock is released, repeated RPC will finish quickly
        // after checking the schema versions.
        let lock = LOCK_APPLY_SCHEMA_CHANGE.with(Rc::clone);
        let _guard = lock.lock();

        let ddl = storage.properties.pending_schema_change()?
            .ok_or_else(|| TraftError::other("pending schema change not found"))?;
        let Ddl::Backup { timestamp } = ddl else {
            unreachable!("BACKUP Ddl should be pending on proc_apply_backup call")
        };
        let backup_path = get_backup_path(timestamp);

        let is_master = req.is_master;

        let Some(pending_schema_version) = storage.properties.pending_schema_version()? else {
            let pending_catalog_version = storage.properties.pending_catalog_version()?;
            if let Some(next_op) = storage.governor_queue.next_pending_operation(pending_catalog_version)? {
                if next_op.op_format == GovernorOpFormat::ProcName {
                    ddl_create_tt_proc_on_master(&next_op.op)?;
                    return Ok(Response::BackupPath(backup_path));
                }
            }
            return Err(TraftError::other("pending schema version not found"));
        };


        if local_schema_version()? >= pending_schema_version && is_master {
            // In case of backup we need to execute it on replica even if
            // the local schema version is already greater than the pending one
            // (that's why we check `is_master` flag here).
            // We update local_schema_version only on masters.
            return Ok(Response::BackupPath(backup_path));
        }

        let res = transaction(|| apply_backup(&backup_path, pending_schema_version));
        match res {
            Ok(()) => Ok(Response::BackupPath(backup_path)),
            Err(TransactionError::RolledBack(Error::Aborted(err))) => {
                tlog!(Warning, "backup aborted: {err}");

                let instance_name = node.raft_storage.instance_name()?.expect("should be persisted before procs are defined");
                let cause = ErrorInfo {
                    error_code: err.error_code(),
                    message: err.to_string(),
                    instance_name,
                };
                Ok(Response::Abort { cause })
            }
            Err(err) => {
                tlog!(Warning, "applying backup failed: {err}");
                Err(err.into())
            }
        }
    }

    pub struct Request {
        pub term: RaftTerm,
        pub applied: RaftIndex,
        pub timeout: Duration,
        pub is_master: bool,
    }

    pub enum Response {
        /// Backup applied successfully on this instance.
        BackupPath(PathBuf),
        /// Backup failed on this instance and should be aborted on the
        /// whole cluster.
        Abort { cause: ErrorInfo },
    }
}

/// Applies the backup to the tarantool storage. This
/// function is called on both replicaset masters and replicas.
///
/// In case of successful backup application on masters the local
/// schema version will be set to `version`.
///
/// The space and index definitions are extracted from picodata storage via
/// `storage`.
pub fn apply_backup(backup_path: &PathBuf, _version: u64) -> Result<(), Error> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });

    // Injection allowing to block the transaction for an indefinite period of time.
    crate::error_injection!(block "BLOCK_BACKUP_TRANSACTION");

    let config = &crate::config::PicodataConfig::get().instance;

    // TODO: See https://git.picodata.io/core/picodata/-/issues/2182.
    let config_file = config.config_file.clone();
    if config_file.is_none() {
        return Err(Error::Aborted(TraftError::other(
            "No config found for local backup (it was not found on instance start)",
        )));
    }
    if !config_file.unwrap().exists() {
        return Err(Error::Aborted(TraftError::other(
            "Config file which instance is referencing, does not exist",
        )));
    }

    let abort_reason = schema::backup_local(config, backup_path).map_err(Error::Other)?;
    if let Some(e) = abort_reason {
        return Err(Error::Aborted(e.into()));
    }

    return Ok(());
}

crate::define_rpc_request! {
    /// Forces the target instance to clear backup directory that was possibly
    /// created and filled with backup data. Backup finished with DdlAbort so
    /// that we have to remove partially backuped data.
    ///
    /// Should be called by a governor on all instances.
    fn proc_backup_abort_clear(req: RequestClear) -> crate::traft::Result<ResponseClear> {
        let config = &crate::config::PicodataConfig::get().instance;
        let backup_dir = config.backup_dir();
        let backup_path = backup_dir.join(req.backup_dir_name);

        if backup_path.exists() {
            fs::remove_dir_all(&backup_path)?;
            tlog!(Info, "cleared data on backup abort on {}", backup_path.display());
        }

        Ok(ResponseClear::Ok)
    }

    pub struct RequestClear {
        pub backup_dir_name: String,
    }

    pub enum ResponseClear {
        Ok,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Backup failed on this instance and should be aborted on the
    /// whole cluster.
    #[error("{0}")]
    Aborted(TraftError),

    #[error("{0}")]
    Other(TraftError),
}
