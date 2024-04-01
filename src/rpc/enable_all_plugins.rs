use crate::plugin::PluginEvent;
use crate::tlog;
use crate::traft::node;
use crate::traft::{RaftIndex, RaftTerm};
use std::time::Duration;

crate::define_rpc_request! {
    /// Enable all "enabled" plugins on instance locally.
    ///
    /// Called by governor at newly online instance.
    ///
    /// Returns errors in the following cases:
    /// 1. Raft node on a receiving peer is not yet initialized
    /// 2. Storage failure
    /// 3. Timeout while waiting for an index from request
    /// 4. Request has an incorrect term - leader changed
    /// 5. Any of "enabled" was loaded with errors
    fn proc_enable_all_plugins(req: Request) -> crate::traft::Result<Response> {
        let node = node::global()?;
        node.wait_index(req.applied, req.timeout)?;
        node.status().check_term(req.term)?;

        let result = node.plugin_manager.handle_event_sync(PluginEvent::InstanceOnline);
        if let Err(e) = result {
            tlog!(Error, "failed initializing plugin system: {e}");
            return Err(e.into());
        }

        Ok(Response {})
    }

    pub struct Request {
        pub term: RaftTerm,
        pub applied: RaftIndex,
        pub timeout: Duration,
    }

    pub struct Response {}
}
