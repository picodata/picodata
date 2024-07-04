use crate::instance::Instance;
use crate::instance::StateVariant::*;
use crate::pico_service::pico_service_password;
use crate::replicaset::Replicaset;
use crate::replicaset::ReplicasetId;
use crate::replicaset::Weight;
use crate::schema::PICO_SERVICE_USER_NAME;
use crate::traft::RaftId;
use ::tarantool::tlua;
use std::collections::HashMap;

#[rustfmt::skip]
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Default, Clone, Debug, PartialEq, tlua::PushInto, tlua::Push, tlua::LuaRead)]
pub struct VshardConfig {
    sharding: HashMap<String, ReplicasetSpec>,
    discovery_mode: DiscoveryMode,

    /// This field is not stored in the global storage, instead
    /// it is set right before the config is passed into vshard.*.cfg,
    /// otherwise vshard will override it with an incorrect value.
    #[serde(skip_serializing_if="Option::is_none")]
    #[serde(default)]
    pub listen: Option<String>,
}

#[rustfmt::skip]
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Default, Clone, Debug, PartialEq, tlua::PushInto, tlua::Push, tlua::LuaRead)]
struct ReplicasetSpec {
    replicas: HashMap<String, ReplicaSpec>,
    weight: Option<Weight>,
}

#[rustfmt::skip]
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Default, Clone, Debug, PartialEq, Eq, tlua::PushInto, tlua::Push, tlua::LuaRead)]
struct ReplicaSpec {
    uri: String,
    name: String,
    master: bool,
}

tarantool::define_str_enum! {
    /// Specifies the mode of operation for the bucket discovery fiber of vshard
    /// router.
    ///
    /// See [`vshard.router.discovery_set`] for more details.
    ///
    /// [`vshard.router.discovery_set`]: https://www.tarantool.io/en/doc/latest/reference/reference_rock/vshard/vshard_router/#router-api-discovery-set
    #[derive(Default)]
    pub enum DiscoveryMode {
        Off = "off",
        #[default]
        On = "on",
        Once = "once",
    }
}

impl VshardConfig {
    pub fn new(
        instances: &[Instance],
        peer_addresses: &HashMap<RaftId, String>,
        replicasets: &HashMap<&ReplicasetId, &Replicaset>,
        vshard_bootstrapped: bool,
    ) -> Self {
        let mut found_ready_replicaset = false;
        let mut sharding: HashMap<String, ReplicasetSpec> = HashMap::new();
        for peer in instances {
            if !peer.may_respond() || peer.current_state.variant < Replicated {
                continue;
            }
            let Some(address) = peer_addresses.get(&peer.raft_id) else {
                crate::tlog!(Warning, "skipping instance: address not found for peer";
                    "raft_id" => peer.raft_id,
                );
                continue;
            };
            let Some(r) = replicasets.get(&peer.replicaset_id) else {
                crate::tlog!(Debug, "skipping instance: replicaset not initialized yet";
                    "instance_id" => %peer.instance_id,
                );
                continue;
            };
            use crate::replicaset::ReplicasetState::Ready;
            if r.weight > 0.0 && r.state == Ready {
                found_ready_replicaset = true;
            }

            let replicaset = sharding
                .entry(peer.replicaset_uuid.clone())
                .or_insert_with(|| ReplicasetSpec {
                    weight: Some(r.weight),
                    ..Default::default()
                });

            replicaset.replicas.insert(
                peer.instance_uuid.clone(),
                ReplicaSpec {
                    uri: format!("{PICO_SERVICE_USER_NAME}@{address}"),
                    master: r.current_master_id == peer.instance_id,
                    name: peer.instance_id.to_string(),
                },
            );
        }
        if !vshard_bootstrapped && !found_ready_replicaset {
            // Vshard will fail if we configure it with all replicaset weights set to 0.
            // But we don't set a replicaset's weight until it's filled up to the replication factor.
            // (NOTE: we don't actually check the replication factor here,
            //  this is handled elsewhere and here we just check weigth.state).
            // So we wait until at least one replicaset is filled (i.e. `found_ready_replicaset == true`).
            //
            // Also if vshard has already been bootstrapped, the user can mess
            // this up by setting all replicasets' weights to 0, which will
            // break vshard configuration, but this will be the user's fault
            // probably, not sure we can do something about it
            return Self::default();
        }

        Self {
            listen: None,
            sharding,
            discovery_mode: DiscoveryMode::On,
        }
    }

    /// Set the `pico_service` password in the uris of all the replicas in this
    /// config. This is needed, because currently we store this config in a
    /// global table, but we don't want to store the passwords in plain text, so
    /// we only set them after the config is deserialized and before the config
    /// is applied.
    pub fn set_password_in_uris(&mut self) {
        let password = pico_service_password();

        for replicaset in self.sharding.values_mut() {
            for replica in replicaset.replicas.values_mut() {
                // FIXME: this is kinda stupid, we could as easily store the
                // username and address in separate fields, or even not store
                // the username at all, because it's always `pico_service`,
                // but this also works... for now.
                let Some((username, address)) = replica.uri.split_once('@') else {
                    continue;
                };
                if username != PICO_SERVICE_USER_NAME {
                    continue;
                }
                replica.uri = format!("{username}:{password}@{address}");
            }
        }
    }
}
