use crate::tlog;
use crate::traft::error::Error as TraftError;
use crate::traft::node;
use crate::traft::{RaftIndex, RaftTerm};
use std::time::Duration;

crate::define_rpc_request! {
    /// Forces the target instance to actually enable the plugin locally.
    ///
    /// Should be called by a governor on every instance in the cluster.
    ///
    /// Returns errors in the following cases:
    /// 1. Raft node on a receiving peer is not yet initialized
    /// 2. Storage failure
    /// 3. Timeout while waiting for an index from request
    /// 4. Request has an incorrect term - leader changed
    /// 5. Can't load plugin services from shared objects
    /// 6. One or more services return errors in `on_start` callback
    fn proc_enable_plugin(req: Request) -> crate::traft::Result<Response> {
        let node = node::global()?;
        node.wait_index(req.applied, req.timeout)?;
        node.status().check_term(req.term)?;

        let storage = &node.storage;

        let (plugin_name, _, _) = storage.properties.pending_plugin_enable()?
            .ok_or_else(|| TraftError::other("pending plugin not found"))?;

        let load_result = node.plugin_manager.try_load(&plugin_name);

        match load_result {
             Ok(()) => Ok(Response::Ok),
             Err(err) => {
                tlog!(Warning, "plugin enabling aborted: {err}");
                Ok(Response::Abort { reason: err.to_string() })
            }
        }
    }

    pub struct Request {
        pub term: RaftTerm,
        pub applied: RaftIndex,
        pub timeout: Duration,
    }

    pub enum Response {
        /// Plugin loaded on this instance.
        Ok,
        /// Plugin loaded failed on this instance and should be aborted on the
        /// whole cluster.
        Abort { reason: String },
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Plugin loaded failed on this instance and should be aborted on the
    /// whole cluster.
    #[error("{0}")]
    Aborted(String),
}
