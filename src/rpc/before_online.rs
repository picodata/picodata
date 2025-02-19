use crate::config::PicodataConfig;
use crate::pgproto;
use crate::tlog;
use crate::traft::node;
use crate::traft::{RaftIndex, RaftTerm};

use std::time::Duration;

crate::define_rpc_request! {
    /// Enables communication by PostgreSQL protocol and all
    /// plugins on instance locally that are marked as "enabled".
    ///
    /// Called by governor at newly online instance.
    ///
    /// Returns errors in the following cases:
    /// 1. Raft node on a receiving peer is not yet initialized
    /// 2. Storage failure
    /// 3. Timeout while waiting for an index from request
    /// 4. Request has an incorrect term - leader changed
    /// 5. Address for pgproto cannot be parsed or is busy
    /// 6. Any of "enabled" plugins was loaded with errors
    fn proc_before_online(req: Request) -> crate::traft::Result<Response> {
        let node = node::global()?;
        node.wait_index(req.applied, req.timeout)?;
        node.status().check_term(req.term)?;

        let instance_config = &PicodataConfig::get().instance;
        pgproto::start(&instance_config.pg, instance_config.instance_dir(), &node.storage)?;

        let result = node.plugin_manager.handle_instance_online();
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
