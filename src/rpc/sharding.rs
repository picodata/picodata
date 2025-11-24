use crate::config::PicodataConfig;
use crate::governor::plan::get_first_ready_replicaset_in_tier;
use crate::storage::space_by_name;
use crate::storage::ToEntryIter;
use crate::storage::TABLE_ID_BUCKET;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::Result;
use crate::traft::{node, RaftIndex, RaftTerm};
use crate::vshard::VshardErrorCode;
use crate::vshard::{ReplicasetSpec, VshardConfig};
use smol_str::SmolStr;
use std::collections::HashMap;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::fiber;
use tarantool::index::IteratorType;
use tarantool::space::Space;
use tarantool::tlua;

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
            .map(|rs| (&rs.name, rs))
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

            let mut config = VshardConfig::from_storage(storage, &tier.name, tier.bucket_count)?;

            let tls_config = &PicodataConfig::get().instance.iproto_tls;
            let listen_config = crate::tarantool::ListenConfig::new(
                lua.eval("return box.info.listen")?,
                tls_config);
            config.listen = Some(listen_config);
            if !tls_config.enabled() {
                config.set_password_in_uris();
            }

            crate::error_injection!("BROKEN_REPLICATION" => { config.sharding.clear(); });

            // We do not want to configure the vshard router and storage each time
            // because these actions call box.cfg internally.
            // Therefore, we check for differences between the current and
            // new configurations before calling router:cfg() or storage:cfg().

            if current_instance_tier == tier.name {
                let current_sharding_param: Option<HashMap<SmolStr, ReplicasetSpec>> = lua.eval(
                    "vshard = require('vshard')
                    if vshard.storage.internal.current_cfg ~= nil then
                        return vshard.storage.internal.current_cfg.sharding
                    else
                        return nil
                    end",
                )?;
                if current_sharding_param.is_none() || current_sharding_param.unwrap() != config.sharding {
                    lua.exec_with(
                        "vshard = require('vshard')
                        vshard.storage.cfg(..., box.info.uuid)",
                        &config,
                    )
                    .map_err(tlua::LuaError::from)?;

                    // We explicitly pass TABLE_ID_BUCKET as id of space _bucket
                    // in the `config` above, but just to make it explicit here we
                    // add an assert.
                    let space = space_by_name("_bucket")?;
                    assert_eq!(space.id(), TABLE_ID_BUCKET);
                }
            }

            let current_sharding_param: Option<HashMap<SmolStr, ReplicasetSpec>> = lua.eval_with(
                "vshard = require('vshard')
                local tier_name, cfg = ...
                local router = pico.router[tier_name]
                if router ~= nil then
                    return router.current_cfg.sharding
                else
                    pico.router[tier_name] = vshard.router.new(tier_name, cfg)
                    return nil
                end",
                (&tier.name, &config),
            )
            .map_err(tlua::LuaError::from)?;

            if let Some(param) = current_sharding_param {
                if param != config.sharding {
                    lua.exec_with(
                        "vshard = require('vshard')
                        local tier_name, cfg = ...
                        pico.router[tier_name]:cfg(cfg)",
                        (&tier.name, &config),
                    )
                    .map_err(tlua::LuaError::from)?;
                }
            }
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
            let (ok, err): (Option<tlua::True>, Option<BoxError>) =
                lua.eval_with("return pico.router[...]:bootstrap()", tier_name)
                    .map_err(tlua::LuaError::from)?;

            match (ok, err) {
                (Some(tlua::True), None) => {}
                (None, Some(e)) if e.error_code() == VshardErrorCode::NonEmpty as u32 => {}
                (None, Some(e)) => {
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
            pub tier: SmolStr,
        }

        /// Response to [`sharding::bootstrap::Request`].
        ///
        /// [`sharding::bootstrap::Request`]: Request
        pub struct Response {}
    }
}

crate::define_rpc_request! {
    /// Waits until there's no buckets on this replicaset.
    fn proc_wait_bucket_count(req: WaitBucketCountRequest) -> Result<WaitBucketCountResponse> {
        let deadline = fiber::clock().saturating_add(req.timeout);
        let node = node::global()?;
        node.wait_index(req.applied, req.timeout)?;
        node.status().check_term(req.term)?;

        let Some(space_bucket) = Space::find("_bucket") else {
            return Err(Error::other("vshard is not yet initialized"));
        };

        let index_status = space_bucket.index("status").expect("space _bucket should have a 'status' index");

        crate::error_injection!("TIMEOUT_IN_PROC_WAIT_BUCKET_COUNT" => return Err(Error::timeout()));

        loop {
            let total_count = space_bucket.len()?;
            // After sending a bucket to another instance vshard first marks the
            // buckets as 'sent', and afterwards attempts to asynchronously
            // clean up (remove) such buckets. It will fail to remove them in
            // some cases, for example when a replica stopped responding.
            // (probably because it can't make sure that the bucket is removed from that replica).
            // To be honest it kinda looks like a bug in vshard, because when changing the
            // replicaset weight we also remove the non-responsive instances from the vhsard config
            // but vshard still thinks that it should sync up with that replica.
            //
            // Anyway, in our case we only call this procedure when expelling a
            // replicaset, which means all replica instances are expelled as well.
            // This means we can safely ignore buckets marked as 'sent' when
            // counting up the total amount of buckets currently stored on this
            // instance.
            let garbage_count = index_status.count(IteratorType::Eq, &("sent",))?;
            let effective_count = total_count - garbage_count;

            let expected_count = req.expected_bucket_count;
            if effective_count == expected_count as usize {
                #[rustfmt::skip]
                tlog!(Debug, "done waiting for bucket count 0");
                break;
            }

            if fiber::clock() < deadline {
                fiber::sleep(crate::traft::node::MainLoop::TICK);
            } else {
                #[rustfmt::skip]
                tlog!(Debug, "failed waiting for bucket count {expected_count}, current is {effective_count}");
                return Err(Error::timeout());
            }
        }

        Ok(WaitBucketCountResponse {})
    }

    /// Request to configure vshard.
    #[derive(Default)]
    pub struct WaitBucketCountRequest {
        /// Current term of the sender.
        pub term: RaftTerm,
        /// Current applied index of the sender.
        pub applied: RaftIndex,
        pub timeout: Duration,
        pub expected_bucket_count: u32,
    }

    /// Response to [`WaitBucketCountRequest`].
    pub struct WaitBucketCountResponse {}
}
