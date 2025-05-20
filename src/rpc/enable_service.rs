use crate::plugin::PluginOp;
use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::{RaftIndex, RaftTerm};
use std::time::Duration;

crate::define_rpc_request! {
    /// Forces the target instance to enable plugin service.
    /// This means load service from dynamic lib and call `on_start` callback.
    ///
    /// Returns errors in the following cases:
    /// 1. Raft node on a receiving peer is not yet initialized
    /// 2. Storage failure
    /// 3. Timeout while waiting for an index from request
    /// 4. Request has an incorrect term - leader changed
    /// 5. Can't load plugin service from dynamic lib
    /// 6. Services return errors in `on_start` callback
    fn proc_enable_service(req: Request) -> crate::traft::Result<Response> {
        let node = node::global()?;
        node.wait_index(req.applied, req.timeout)?;
        node.status().check_term(req.term)?;

        let Some(plugin_op) = node.storage.properties.pending_plugin_op()? else {
            return Err(Error::other("pending plugin operation not found"));
        };

        let PluginOp::AlterServiceTiers { plugin, service, .. } = &plugin_op else {
            #[rustfmt::skip]
            return Err(Error::other("found unexpected plugin operation expected AlterServiceTiers, found {plugin_op:?}"));
        };

        node.plugin_manager.handle_service_enabled(plugin, service)?;

        Ok(Response {})
    }

    pub struct Request {
        pub term: RaftTerm,
        pub applied: RaftIndex,
        pub timeout: Duration,
    }

    pub struct Response {}
}
