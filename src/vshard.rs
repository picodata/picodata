use crate::replicaset::Weight;
use crate::storage::Clusterwide;
use crate::storage::ToEntryIter as _;
use crate::traft::Result;
use ::tarantool::tlua;
use std::collections::HashMap;

#[derive(Default, Clone, Debug, PartialEq, tlua::PushInto, tlua::Push, tlua::LuaRead)]
pub struct VshardConfig {
    sharding: HashMap<String, ReplicasetSpec>,
    discovery_mode: DiscoveryMode,
}

#[derive(Default, Clone, Debug, PartialEq, tlua::PushInto, tlua::Push, tlua::LuaRead)]
struct ReplicasetSpec {
    replicas: HashMap<String, ReplicaSpec>,
    weight: Option<Weight>,
}

#[derive(Default, Clone, Debug, PartialEq, Eq, tlua::PushInto, tlua::Push, tlua::LuaRead)]
struct ReplicaSpec {
    uri: String,
    name: String,
    master: bool,
}

#[derive(Default, Copy, Clone, Debug, PartialEq, Eq, tlua::PushInto, tlua::Push, tlua::LuaRead)]
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

impl VshardConfig {
    #[inline]
    pub fn from_storage(storage: &Clusterwide) -> Result<Self> {
        let replicasets: HashMap<_, _> = storage
            .replicasets
            .iter()?
            .map(|r| (r.replicaset_id.clone(), r))
            .collect();
        let mut sharding: HashMap<String, ReplicasetSpec> = HashMap::new();
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

            let weight = replicaset_info.weight;
            let replicaset =
                sharding
                    .entry(peer.replicaset_uuid)
                    .or_insert_with(|| ReplicasetSpec {
                        weight: Some(weight),
                        ..Default::default()
                    });

            replicaset.replicas.insert(
                peer.instance_uuid,
                ReplicaSpec {
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
