use ::tarantool::tlua;

use crate::traft::error::Error;
use crate::traft::Result;
use crate::traft::{node, RaftIndex, RaftTerm};

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

        let lua = ::tarantool::lua_state();

        let mut cfg = node.storage.properties.target_vshard_config()?;
        cfg.listen = Some(lua.eval("return box.info.listen")?);
        cfg.set_password_in_uris();

        if !req.do_reconfigure {
            if let Some(tlua::True) = lua.eval("return pico._vshard_is_configured")? {
                return Ok(Response {});
            }
        }

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

        lua.exec("pico._vshard_is_configured = true")?;

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
        /// If this is `false`, check if the runtime vshard router object is
        /// already created and don't reconfigure. This is only used when an
        /// instance silently restarts without acquiring the Offline state.
        pub do_reconfigure: bool,
    }

    /// Response to [`sharding::Request`].
    ///
    /// [`sharding::Request`]: Request
    pub struct Response {}
}

pub mod bootstrap {
    use crate::error_injection;

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
                lua.eval("
                local lerror = require('vshard.error')
                local ok, err = vshard.router.bootstrap()
                if not ok and type(err) == 'table' then
                    if err.code == lerror.code.NON_EMPTY then
                        return true
                    end
                end

                return ok, err
                ")?;

            match (ok, err) {
                (Some(tlua::True), None) => {}
                (None, Some(tlua::ToString(e))) => {
                    return Err(Error::other(format!(
                        "vshard.router.bootstrap() failed: {e}"
                    )));
                }
                res => unreachable!("{res:?}"),
            }

            // We return error after successful bootstrap to check error handling loginc in lua above
            if error_injection::is_enabled("SHARDING_BOOTSTRAP_SPURIOUS_FAILURE") {
                // disable inject, so it is triggered only once and retry succeeds
                error_injection::enable("SHARDING_BOOTSTRAP_SPURIOUS_FAILURE", false);
                return Err(Error::other("Injection: SHARDING_BOOTSTRAP_SPURIOUS_FAILURE"));
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
