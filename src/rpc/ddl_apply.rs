use crate::op::Ddl;
use crate::storage::Clusterwide;
use crate::storage::{local_schema_version, set_local_schema_version};
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::Result;
use crate::traft::{RaftIndex, RaftTerm};
use std::time::Duration;
use tarantool::error::{TarantoolError, TarantoolErrorCode};
use tarantool::ffi::tarantool as ffi;
use tarantool::space::{Space, SystemSpace};

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
        let res = apply_schema_change(storage, &ddl, pending_schema_version);
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

// TODO: move this to crate::schema maybe?
pub fn apply_schema_change(storage: &Clusterwide, ddl: &Ddl, version: u64) -> Result<Response> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let sys_space = Space::from(SystemSpace::Space);
    let sys_index = Space::from(SystemSpace::Index);

    match *ddl {
        Ddl::CreateSpace { id, .. } => {
            let pico_space_def = storage
                .spaces
                .get(id)?
                .ok_or_else(|| Error::other(format!("space with id #{id} not found")))?;
            // TODO: set defaults
            let tt_space_def = pico_space_def.to_space_metadata()?;

            let pico_pk_def = storage.indexes.get(id, 0)?.ok_or_else(|| {
                Error::other(format!(
                    "primary index for space {} not found",
                    pico_space_def.name
                ))
            })?;
            let tt_pk_def = pico_pk_def.to_index_metadata();

            // For now we just assume that during space creation index with id 1
            // exists if and only if it is a bucket_id index.
            let mut tt_bucket_id_def = None;
            let pico_bucket_id_def = storage.indexes.get(id, 1)?;
            if let Some(def) = &pico_bucket_id_def {
                tt_bucket_id_def = Some(def.to_index_metadata());
            }

            let res = (|| -> tarantool::Result<()> {
                if tt_pk_def.parts.is_empty() {
                    return Err(tarantool::set_and_get_error!(
                        tarantool::error::TarantoolErrorCode::ModifyIndex,
                        "can't create index '{}' in space '{}': parts list cannot be empty",
                        tt_pk_def.name,
                        tt_space_def.name,
                    )
                    .into());
                }
                sys_space.insert(&tt_space_def)?;
                sys_index.insert(&tt_pk_def)?;
                if let Some(def) = tt_bucket_id_def {
                    sys_index.insert(&def)?;
                }
                set_local_schema_version(version)?;

                Ok(())
            })();
            if let Err(e) = res {
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

    Ok(Response::Ok)
}
