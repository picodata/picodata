use crate::plugin::PluginEvent;
use crate::traft::error::Error as TraftError;
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

        let op = node.storage.properties.pending_plugin_topology_update()?
            .ok_or_else(|| TraftError::other("pending plugin topology not found"))?;

        // reaction at `ServiceDisabled` is idempotent, so no errors occurred
        _ = node.plugin_manager.handle_event_sync(PluginEvent::ServiceDisabled {
            plugin: op.plugin_name(),
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