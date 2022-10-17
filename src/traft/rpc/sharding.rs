use ::tarantool::{proc, tlua};

use crate::traft::storage::StateKey;
use crate::traft::{error::Error, node, RaftId, RaftTerm};

#[proc(packed_args)]
fn proc_sharding(req: Request) -> Result<Response, Error> {
    let node = node::global()?;
    let leader_id = node.status().leader_id.ok_or(Error::LeaderUnknown)?;
    if req.leader_id != leader_id {
        return Err(Error::LeaderIdMismatch {
            expected: leader_id,
            actual: req.leader_id,
        });
    }
    // TODO: check term matches
    let _ = req.term;

    let storage = &node.storage;
    let cfg = cfg::Cfg::from_storage(&storage.peers)?;

    let lua = ::tarantool::lua_state();
    // TODO: fix user's permissions
    lua.exec("box.session.su('admin')")?;
    // TODO: only done on instances with corresponding roles
    lua.exec_with("vshard.storage.cfg(..., box.info.uuid)", &cfg)
        .map_err(tlua::LuaError::from)?;
    // TODO: only done on instances with corresponding roles
    lua.exec_with("vshard.router.cfg(...)", &cfg)
        .map_err(tlua::LuaError::from)?;

    // TODO: governor should decide who does this, and propose a OpDML entry
    // afterwards
    if !storage
        .state
        .get(StateKey::VshardBootstrapped)?
        .unwrap_or(false)
    {
        lua.exec("vshard.router.bootstrap()")?;
        storage.state.put(StateKey::VshardBootstrapped, &true)?;
    }

    Ok(Response {})
}

/// Request to configure vshard.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Request {
    pub leader_id: RaftId,
    pub term: RaftTerm,
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
    use crate::traft::storage::peer_field;
    use crate::traft::storage::Peers;

    use ::tarantool::tlua;

    use std::collections::HashMap;

    #[derive(Default, Clone, Debug, PartialEq, Eq)]
    #[derive(tlua::PushInto, tlua::Push, tlua::LuaRead)]
    pub struct Cfg {
        sharding: HashMap<String, Replicaset>,
        discovery_mode: DiscoveryMode,
    }

    #[derive(Default, Clone, Debug, PartialEq, Eq)]
    #[derive(tlua::PushInto, tlua::Push, tlua::LuaRead)]
    struct Replicaset {
        replicas: HashMap<String, Replica>,
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
    pub enum DiscoveryMode {
        #[default]
        Off,
        On,
        Once,
    }

    impl Cfg {
        pub fn from_storage(peers: &Peers) -> Result<Self, Error> {
            use peer_field::{InstanceId, InstanceUuid, PeerAddress, ReplicasetUuid, IsMaster};
            type Fields = (InstanceId, InstanceUuid, PeerAddress, ReplicasetUuid, IsMaster);
            let mut sharding: HashMap<String, Replicaset> = HashMap::new();
            for (id, uuid, addr, rset, is_master) in peers.peers_fields::<Fields>()? {
                let replicaset = sharding.entry(rset).or_default();
                replicaset.replicas.insert(
                    uuid,
                    Replica {
                        uri: format!("guest:@{addr}"),
                        name: id.into(),
                        master: is_master,
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
