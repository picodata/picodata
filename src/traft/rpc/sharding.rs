use ::tarantool::{proc, tlua};

use crate::traft::{error::Error, node, RaftIndex, RaftTerm};

use std::time::Duration;

#[proc(packed_args)]
fn proc_sharding(req: Request) -> Result<Response, Error> {
    let node = node::global()?;
    node.status().check_term(req.term)?;
    super::sync::wait_for_index_timeout(req.commit, &node.storage.raft, req.timeout)?;

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

    if req.bootstrap {
        lua.exec("vshard.router.bootstrap()")?;
    }

    // After reconfiguring vshard leaves behind net.box.connection objects,
    // which try reconnecting every 0.5 seconds. Garbage collecting them helps
    lua.exec("collectgarbage()")?;

    Ok(Response {})
}

/// Request to configure vshard.
#[derive(Clone, Default, Debug, serde::Serialize, serde::Deserialize)]
pub struct Request {
    pub term: RaftTerm,
    pub commit: RaftIndex,
    pub timeout: Duration,
    pub bootstrap: bool,
}
impl ::tarantool::tuple::Encode for Request {}

/// Response to [`sharding::Request`].
///
/// [`sharding::Request`]: Request
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Response {}
impl ::tarantool::tuple::Encode for Response {}

impl super::Request for Request {
    const PROC_NAME: &'static str = crate::stringify_cfunc!(proc_sharding);
    type Response = Response;
}

#[rustfmt::skip]
pub mod cfg {
    use crate::traft::error::Error;
    use crate::traft::storage::Storage;

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

    pub type ReplicasetWeights = HashMap<String, Weight>;
    pub type Weight = f64;

    impl Cfg {
        #[inline]
        pub fn from_storage(storage: &Storage) -> Result<Self, Error> {
            let replicaset_weights = storage.state.replicaset_weights()?;
            let mut sharding: HashMap<String, Replicaset> = HashMap::new();
            for peer in storage.peers.iter()? {
                if !peer.may_respond() {
                    continue;
                }
                let replicaset_id = peer.replicaset_id;
                let replicaset = sharding.entry(peer.replicaset_uuid).or_insert_with(||
                    Replicaset::with_weight(replicaset_weights.get(&replicaset_id).copied())
                );
                replicaset.replicas.insert(
                    peer.instance_uuid,
                    Replica {
                        uri: format!("guest:@{}", peer.peer_address),
                        name: peer.instance_id.into(),
                        master: peer.is_master,
                    },
                );
            }
            Ok(Self {
                sharding,
                discovery_mode: DiscoveryMode::Off,
            })
        }
    }
}
