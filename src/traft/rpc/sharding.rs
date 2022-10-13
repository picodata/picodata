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
