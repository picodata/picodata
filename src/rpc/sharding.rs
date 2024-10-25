use ::tarantool::tlua;

use crate::governor::plan::get_first_ready_replicaset_in_tier;
use crate::storage::ToEntryIter;
use crate::traft::error::Error;
use crate::traft::Result;
use crate::traft::{node, RaftIndex, RaftTerm};
use crate::vshard::VshardConfig;

use std::collections::HashMap;
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

        let lua = ::tarantool::lua_state();
        let current_instance_tier = node.raft_storage.tier()?.expect("tier for instance should exists");

        let instances = storage
            .instances
            .all_instances()
            .expect("storage should never fail");

        let replicasets: Vec<_> = storage
            .replicasets
            .iter()
            .expect("storage should never fail")
            .collect();
        let replicasets: HashMap<_, _> = replicasets
            .iter()
            .map(|rs| (&rs.replicaset_name, rs))
            .collect();

        for tier in node.storage.tiers.iter().expect("tiers shouldn't fail, at least one tier always exists") {
            let first_ready_replicaset = get_first_ready_replicaset_in_tier(&instances, &replicasets, &tier.name);
            let ok_to_configure_vshard = tier.vshard_bootstrapped || first_ready_replicaset.is_some();

            // Note: the following is a hack stemming from the fact that we have to work around vshard's weird quirks.
            // Everything having to deal with bootstrapping vshard should be removed completely once we migrate to our custom sharding solution.
            //
            // Vshard will fail if we configure it with all replicaset weights set to 0.
            // But we don't set a replicaset's weight until it's filled up to the replication factor.
            // So we wait until at least one replicaset is filled (i.e. `first_ready_replicaset.is_some()`).
            //
            // Also if vshard has already been bootstrapped, the user can mess this up by setting all replicasets' weights to 0,
            // which will break vshard configuration, but this will be the user's fault probably, not sure we can do something about it
            if !ok_to_configure_vshard {
                continue;
            }

            let mut config = VshardConfig::from_storage(storage, &tier.name)?;

            config.listen = Some(lua.eval("return box.info.listen")?);
            config.set_password_in_uris();

            if current_instance_tier == tier.name {
                lua.exec_with(
                    "vshard = require('vshard')
                    vshard.storage.cfg(..., box.info.uuid)",
                    &config,
                )
                .map_err(tlua::LuaError::from)?;
            }

            lua.exec_with(
                "vshard = require('vshard')
                local tier_name, cfg = ...
                local router = pico.router[tier_name]
                if router ~= null then
                    router:cfg(cfg)
                else
                    pico.router[tier_name] = vshard.router.new(tier_name, cfg)
                end",
                (&tier.name, &config),
            )
            .map_err(tlua::LuaError::from)?;
        }

        // After reconfiguring vshard leaves behind net.box.connection objects,
        // which try reconnecting every 0.5 seconds. Garbage collecting them helps
        lua.exec("collectgarbage()")?;

        crate::error_injection!("PROC_SHARDING_SPURIOUS_FAILURE" => return Err(Error::other("injected error")));

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
        /// Calls `vshard.router.bootstrap()` on the specified tier router on the target instance.
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
            let tier_name = &req.tier;

            #[cfg(debug_assertions)]
            {
                let current_instance_tier = node.raft_storage.tier()?.expect("tier for instance should exists");
                assert_eq!(current_instance_tier, req.tier);
            }

            let lua = tarantool::lua_state();
            let (ok, err): (Option<tlua::True>, Option<tlua::ToString>) =
                lua.eval_with("
                local lerror = require('vshard.error')
                local tier_name = ...
                local ok, err = pico.router[tier_name]:bootstrap()
                if not ok and type(err) == 'table' then
                    if err.code == lerror.code.NON_EMPTY then
                        return true
                    end
                end

                return ok, err
                ",
                tier_name
            ).map_err(tlua::LuaError::from)?;

            match (ok, err) {
                (Some(tlua::True), None) => {}
                (None, Some(tlua::ToString(e))) => {
                    return Err(Error::other(format!(
                        "pico.router[{tier_name}]:bootstrap() failed: {e}"
                    )));
                }
                res => unreachable!("{res:?}"),
            }

            // We return error after successful bootstrap to check error handling logic in lua above
            crate::error_injection!("SHARDING_BOOTSTRAP_SPURIOUS_FAILURE" => return Err(Error::other("error injection")));

            Ok(Response {})
        }

        /// Request to bootstrap bucket distribution on specified tier.
        #[derive(Default)]
        pub struct Request {
            pub term: RaftTerm,
            pub applied: RaftIndex,
            pub timeout: Duration,
            pub tier: String,
        }

        /// Response to [`sharding::bootstrap::Request`].
        ///
        /// [`sharding::bootstrap::Request`]: Request
        pub struct Response {}
    }
}
