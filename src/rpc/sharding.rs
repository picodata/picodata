use ::tarantool::tlua;

use crate::traft::error::Error;
use crate::traft::Result;
use crate::traft::{node, RaftIndex, RaftTerm};
use crate::vshard::VshardConfig;

use std::time::Duration;

crate::define_rpc_request! {
    /// (Re)configures sharding. Sets up the vshard storage and vshard router
    /// components on the target instance. The configuration for them is taken
    /// from local storage.
    ///
    /// Returns errors in the following cases:
    /// 1. Raft node on a receiving instance is not yet initialized
    /// 2. Storage failure
    /// 3. Timeout while waiting for an index from the request
    /// 4. Request has an incorrect term - leader changed
    /// 5. Lua error during vshard setup
    fn proc_sharding(req: Request) -> Result<Response> {
        let node = node::global()?;
        node.wait_index(req.applied, req.timeout)?;
        node.status().check_term(req.term)?;

        let storage = &node.storage;
        let cfg = VshardConfig::from_storage(storage)?;
        crate::tlog!(Debug, "vshard config: {cfg:?}");

        let lua = ::tarantool::lua_state();
        // TODO: fix user's permissions
        lua.exec("box.session.su('admin')")?;
        // TODO: only done on instances with corresponding roles
        lua.exec_with(
            "vshard = require('vshard')
            vshard.storage.cfg(..., box.info.uuid)",
            &cfg,
        )
        .map_err(tlua::LuaError::from)?;
        // TODO: only done on instances with corresponding roles
        lua.exec_with(
            "vshard = require('vshard')
            vshard.router.cfg(...)",
            &cfg,
        )
        .map_err(tlua::LuaError::from)?;

        // After reconfiguring vshard leaves behind net.box.connection objects,
        // which try reconnecting every 0.5 seconds. Garbage collecting them helps
        lua.exec("collectgarbage()")?;

        Ok(Response {})
    }

    /// Request to configure vshard.
    #[derive(Default)]
    pub struct Request {
        /// Current term of the sender.
        pub term: RaftTerm,
        /// Current applied index of the sender.
        pub applied: RaftIndex,
        pub timeout: Duration,
    }

    /// Response to [`sharding::Request`].
    ///
    /// [`sharding::Request`]: Request
    pub struct Response {}
}

pub mod bootstrap {
    use super::*;

    crate::define_rpc_request! {
        /// Calls `vshard.router.bootstrap()` on the target instance.
        /// See [tarantool documentaion](https://www.tarantool.io/en/doc/latest/reference/reference_rock/vshard/vshard_router/#lua-function.vshard.router.bootstrap)
        /// for more information on vshard router bootstrap process.
        ///
        /// Returns errors in the following cases:
        /// 1. Raft node on a receiving instance is not yet initialized
        /// 2. Timeout while waiting for an index from the request
        /// 3. Request has an incorrect term - leader changed
        /// 4. Lua error during vshard router bootstrap
        fn proc_sharding_bootstrap(req: Request) -> Result<Response> {
            let node = node::global()?;
            node.wait_index(req.applied, req.timeout)?;
            node.status().check_term(req.term)?;

            let lua = tarantool::lua_state();
            let (ok, err): (Option<tlua::True>, Option<tlua::ToString>) =
                lua.eval("return vshard.router.bootstrap()")?;

            match (ok, err) {
                (Some(tlua::True), None) => {}
                (None, Some(tlua::ToString(e))) => {
                    return Err(Error::other(format!(
                        "vshard.router.bootstrap() failed: {e}"
                    )));
                }
                res => unreachable!("{res:?}"),
            }

            Ok(Response {})
        }

        /// Request to bootstrap bucket distribution.
        #[derive(Default)]
        pub struct Request {
            pub term: RaftTerm,
            pub applied: RaftIndex,
            pub timeout: Duration,
        }

        /// Response to [`sharding::bootstrap::Request`].
        ///
        /// [`sharding::bootstrap::Request`]: Request
        pub struct Response {}
    }
}
