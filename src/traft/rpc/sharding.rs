use ::tarantool::tlua;

use crate::traft::rpc::sync::wait_for_index_timeout;
use crate::traft::Result;
use crate::traft::{node, RaftIndex, RaftTerm};

use std::time::Duration;

crate::define_rpc_request! {
    fn proc_sharding(req: Request) -> Result<Response> {
        let node = node::global()?;
        node.status().check_term(req.term)?;
        wait_for_index_timeout(req.commit, &node.raft_storage, req.timeout)?;

        let storage = &node.storage;
        let cfg = cfg::Cfg::from_storage(storage)?;

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
        pub term: RaftTerm,
        pub commit: RaftIndex,
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
        fn proc_sharding_bootstrap(req: Request) -> Result<Response> {
            let node = node::global()?;
            node.status().check_term(req.term)?;
            wait_for_index_timeout(req.commit, &node.raft_storage, req.timeout)?;

            ::tarantool::lua_state().exec("vshard.router.bootstrap()")?;

            Ok(Response {})
        }

        /// Request to bootstrap bucket distribution.
        #[derive(Default)]
        pub struct Request {
            pub term: RaftTerm,
            pub commit: RaftIndex,
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

    pub type Weight = f64;

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
                let (weight, is_master) = match replicasets.get(&peer.replicaset_id) {
                    Some(r) => (Some(r.target_weight), r.master_id == peer.instance_id),
                    None => (None, false),
                };
                let replicaset = sharding.entry(peer.replicaset_uuid)
                    .or_insert_with(|| Replicaset::with_weight(weight));
                replicaset.replicas.insert(
                    peer.instance_uuid,
                    Replica {
                        uri: format!("guest:@{address}"),
                        name: peer.instance_id.into(),
                        master: is_master,
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
