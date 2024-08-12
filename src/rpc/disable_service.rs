use crate::plugin::PluginEvent;
use crate::plugin::PluginOp;
use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::{RaftIndex, RaftTerm};
use std::time::Duration;

crate::define_rpc_request! {
    /// Forces the target instance to disable plugin service.
    /// This means call `on_stop` callback and drop service from memory.
    ///
    /// Returns errors in the following cases:
    /// 1. Raft node on a receiving peer is not yet initialized
    /// 2. Storage failure
    /// 3. Timeout while waiting for an index from request
    /// 4. Request has an incorrect term - leader changed
    fn proc_disable_service(req: Request) -> crate::traft::Result<Response> {
        let node = node::global()?;
        node.wait_index(req.applied, req.timeout)?;
        node.status().check_term(req.term)?;

        let Some(plugin_op) = node.storage.properties.pending_plugin_op()? else {
            return Err(Error::other("pending plugin operation not found"));
        };

        let PluginOp::UpdateTopology(op) = plugin_op else {
            #[rustfmt::skip]
            return Err(Error::other("found unexpected plugin operation expected UpdateTopology, found {plugin_op:?}"));
        };

        // reaction at `ServiceDisabled` is idempotent, so no errors occurred
        _ = node.plugin_manager.handle_event_sync(PluginEvent::ServiceDisabled {
            ident: op.plugin_identity(),
            service: op.service_name(),
        });

        Ok(Response {})
    }

    pub struct Request {
        pub term: RaftTerm,
        pub applied: RaftIndex,
        pub timeout: Duration,
    }

    pub struct Response {}
}
