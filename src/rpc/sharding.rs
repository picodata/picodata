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

        let storage = &node.storage;
        let cfg = cfg::Cfg::from_storage(storage)?;
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

#[rustfmt::skip]
pub mod cfg {
    use crate::storage::Clusterwide;
    use crate::storage::ToEntryIter as _;
    use crate::traft::Result;
    use crate::replicaset::Weight;

    use ::tarantool::tlua;

    use std::collections::HashMap;

    #[derive(Default, Clone, Debug, PartialEq)]
    #[derive(tlua::PushInto, tlua::Push, tlua::LuaRead)]
    pub struct Cfg {
        sharding: HashMap<String, Replicaset>,
        discovery_mode: DiscoveryMode,
    }

    #[derive(Default, Clone, Debug, PartialEq)]
    #[derive(tlua::PushInto, tlua::Push, tlua::LuaRead)]
    struct Replicaset {
        replicas: HashMap<String, Replica>,
        weight: Option<Weight>,
    }

    impl Replicaset {
        #[inline]
        pub fn with_weight(weight: impl Into<Option<Weight>>) -> Self {
            Self {
                weight: weight.into(),
                ..Default::default()
            }
        }
    }

    #[derive(Default, Clone, Debug, PartialEq, Eq)]
    #[derive(tlua::PushInto, tlua::Push, tlua::LuaRead)]
    struct Replica {
        uri: String,
        name: String,
        master: bool,
    }

    #[derive(Default, Copy, Clone, Debug, PartialEq, Eq)]
    #[derive(tlua::PushInto, tlua::Push, tlua::LuaRead)]
    /// Specifies the mode of operation for the bucket discovery fiber of vshard
    /// router.
    ///
    /// See [`vshard.router.discovery_set`] for more details.
    ///
    /// [`vshard.router.discovery_set`]: https://www.tarantool.io/en/doc/latest/reference/reference_rock/vshard/vshard_router/#router-api-discovery-set
    pub enum DiscoveryMode {
        Off,
        #[default]
        On,
        Once,
    }

    impl Cfg {
        #[inline]
        pub fn from_storage(storage: &Clusterwide) -> Result<Self> {
            let replicasets: HashMap<_, _> = storage.replicasets.iter()?
                .map(|r| (r.replicaset_id.clone(), r))
                .collect();
            let mut sharding: HashMap<String, Replicaset> = HashMap::new();
            for peer in storage.instances.iter()? {
                if !peer.may_respond() {
                    continue;
                }
                let Some(address) = storage.peer_addresses.get(peer.raft_id)? else {
                    crate::tlog!(Warning, "address not found for peer";
                        "raft_id" => peer.raft_id,
                    );
                    continue;
                };
                let Some(replicaset_info) = replicasets.get(&peer.replicaset_id) else {
                    crate::tlog!(Debug, "skipping instance: replicaset not initialized yet";
                        "instance_id" => %peer.instance_id,
                    );
                    continue;
                };

                let weight = replicaset_info.weight.value;
                let replicaset = sharding.entry(peer.replicaset_uuid)
                    .or_insert_with(|| Replicaset::with_weight(weight));

                replicaset.replicas.insert(
                    peer.instance_uuid,
                    Replica {
                        uri: format!("guest:@{address}"),
                        master: replicaset_info.master_id == peer.instance_id,
                        name: peer.instance_id.into(),
                    },
                );
            }
            Ok(Self {
                sharding,
                discovery_mode: DiscoveryMode::On,
            })
        }
    }
}
