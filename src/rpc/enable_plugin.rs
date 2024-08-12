use crate::plugin::PluginOp;
use crate::tlog;
use crate::traft::error::Error;
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

        let Some(plugin_op) = node.storage.properties.pending_plugin_op()? else {
            return Err(Error::other("pending plugin operation not found"));
        };

        let PluginOp::EnablePlugin { plugin, .. } = plugin_op else {
            #[rustfmt::skip]
            return Err(Error::other("found unexpected plugin operation expected EnablePlugin, found {plugin_op:?}"));
        };

        let load_result = node.plugin_manager.try_load(&plugin);

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
