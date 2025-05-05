use crate::instance::Instance;
use crate::instance::InstanceName;
use crate::pico_service::pico_service_password;
use crate::replicaset::Replicaset;
use crate::replicaset::ReplicasetName;
use crate::replicaset::Weight;
use crate::rpc::ddl_apply::{Request, Response};
use crate::schema::PICO_SERVICE_USER_NAME;
use crate::sql::router;
use crate::storage::Catalog;
use crate::storage::ToEntryIter as _;
use crate::storage::TABLE_ID_BUCKET;
use crate::traft::error::Error as TraftError;
use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::ConnectionType;
use crate::traft::RaftId;
use crate::traft::Result;
use ::tarantool::msgpack::ViaMsgpack;
use sbroad::executor::engine::Vshard;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tarantool::space::SpaceId;
use tarantool::tlua;

use tarantool::tuple::Tuple;

fn is_vshard_not_initialized() -> crate::traft::Result<bool> {
    crate::tarantool::eval("return vshard == nil").map_err(TraftError::from)
}

fn is_rebalancing_in_progress() -> crate::traft::Result<bool> {
    crate::tarantool::eval("return vshard.storage.rebalancing_is_in_progress()")
        .map_err(TraftError::from)
}

fn enable_vshard_rebalancer() -> crate::traft::Result<()> {
    crate::tarantool::exec("vshard.storage.rebalancer_enable()").map_err(TraftError::from)
}

fn disable_vshard_rebalancer() -> crate::traft::Result<()> {
    crate::tarantool::exec("vshard.storage.rebalancer_disable()").map_err(TraftError::from)
}

fn is_rebalancer_here() -> crate::traft::Result<bool> {
    crate::tarantool::eval("return vshard.storage.internal.rebalancer_fiber ~= nil")
        .map_err(TraftError::from)
}

/// Enable the rebalancer if it's active on this instance.
pub(crate) fn enable_rebalancer() -> crate::traft::Result<()> {
    if is_vshard_not_initialized()? {
        return Ok(());
    }

    // There’s no need to verify the rebalancer fiber’s
    // presence — rebalancer_enable() is a no-op if the fiber isn’t active here.
    enable_vshard_rebalancer()?;

    Ok(())
}

/// Disable the rebalancer if it's active on this instance.
///
/// Return error if rebalancing is in progress or instance is not a replicaset leader.
pub(crate) fn disable_rebalancer() -> crate::traft::Result<()> {
    if is_vshard_not_initialized()? {
        return Ok(());
    }

    // Here we must check if the rebalancing fiber exists — the 'rebalancing in progress'
    // flag is only relevant for instances with an active rebalancer.
    if is_rebalancer_here()? {
        if is_rebalancing_in_progress()? {
            return Err(TraftError::other("Rebalancing is in progress"));
        }

        disable_vshard_rebalancer()?;
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DdlMapCallRwRes {
    pub uuid: String,
    pub response: Response,
}

/// This function **yields**
pub fn ddl_map_callrw(
    tier: &str,
    function_name: &str,
    rpc_timeout: Duration,
    req: &Request,
) -> Result<Vec<DdlMapCallRwRes>, Error> {
    let lua = tarantool::lua_state();
    let pico: tlua::LuaTable<_> = lua
        .get("pico")
        .ok_or_else(|| Error::other("pico lua module disappeared"))?;

    let func: tlua::LuaFunction<_> = pico.try_get("_ddl_map_callrw")?;
    let args = Tuple::new(req)?;

    let res_raw = func
        .call_with_args::<HashMap<String, [ViaMsgpack<Response>; 1]>, _>((
            tier,
            rpc_timeout.as_secs_f64(),
            function_name,
            args,
        ))
        .map_err(|e| Error::from(tlua::LuaError::from(e)))?;
    let res = res_raw
        .into_iter()
        .map(|(uuid, response_arr)| {
            let [ViaMsgpack(response)] = response_arr;
            DdlMapCallRwRes { uuid, response }
        })
        .collect();
    Ok(res)
}

/// This function **never yields**
pub fn get_replicaset_priority_list(
    tier: &str,
    replicaset_uuid: &str,
) -> Result<Vec<InstanceName>, Error> {
    #[cfg(debug_assertions)]
    let _guard = tarantool::fiber::safety::NoYieldsGuard::new();

    let lua = tarantool::lua_state();
    let pico: tlua::LuaTable<_> = lua
        .get("pico")
        .ok_or_else(|| Error::other("pico lua module disappeared"))?;

    let func: tlua::LuaFunction<_> = pico.try_get("_replicaset_priority_list")?;
    let res = func.call_with_args((tier, replicaset_uuid));
    if res.is_err() {
        // Check if tier exists, return corresponding error in that case
        node::global()
            .expect("initilized by this point")
            .topology_cache
            .get()
            .tier_by_name(tier)?;
    }
    res.map_err(|e| tlua::LuaError::from(e).into())
}

/// Returns the replicaset uuid and an array of replicas in descending priority
/// order.
///
/// This function **may yield** if vshard needs to update it's bucket mapping.
pub fn get_replicaset_uuid_by_bucket_id(tier: &str, bucket_id: u64) -> Result<String, Error> {
    let info = router::get_tier_info(tier)?;

    let max_bucket_id = info.bucket_count();
    if bucket_id < 1 || bucket_id > max_bucket_id {
        #[rustfmt::skip]
        return Err(Error::other(format!("invalid bucket id: must be within 1..{max_bucket_id}, got {bucket_id}")));
    }

    let lua = tarantool::lua_state();
    let pico: tlua::LuaTable<_> = lua
        .get("pico")
        .ok_or_else(|| Error::other("pico lua module disappeared"))?;

    let func: tlua::LuaFunction<_> = pico.try_get("_replicaset_uuid_by_bucket_id")?;
    let res = func.call_with_args((tier, bucket_id));
    if res.is_err() {
        // Check if tier exists, return corresponding error in that case
        node::global()
            .expect("initilized by this point")
            .topology_cache
            .get()
            .tier_by_name(tier)?;
    }
    res.map_err(|e| tlua::LuaError::from(e).into())
}

#[rustfmt::skip]
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Default, Clone, Debug, PartialEq, tlua::PushInto, tlua::Push, tlua::LuaRead)]
pub struct VshardConfig {
    sharding: HashMap<String, ReplicasetSpec>,
    discovery_mode: DiscoveryMode,

    /// Id of system table `_bucket`.
    space_bucket_id: SpaceId,

    /// Total number of virtual buckets on each tier.
    bucket_count: u64,

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
    pub fn from_storage(
        storage: &Catalog,
        tier_name: &str,
        bucket_count: u64,
    ) -> Result<Self, Error> {
        let instances = storage.instances.all_instances()?;
        let peer_addresses: HashMap<_, _> = storage
            .peer_addresses
            .iter()?
            .filter(|peer| peer.connection_type == ConnectionType::Iproto)
            .map(|pa| (pa.raft_id, pa.address))
            .collect();
        let replicasets: Vec<_> = storage.replicasets.iter()?.collect();
        let replicasets: HashMap<_, _> = replicasets.iter().map(|rs| (&rs.name, rs)).collect();

        let result = Self::new(
            &instances,
            &peer_addresses,
            &replicasets,
            tier_name,
            bucket_count,
        );
        Ok(result)
    }

    pub fn new(
        instances: &[Instance],
        peer_addresses: &HashMap<RaftId, String>,
        replicasets: &HashMap<&ReplicasetName, &Replicaset>,
        tier_name: &str,
        bucket_count: u64,
    ) -> Self {
        let mut sharding: HashMap<String, ReplicasetSpec> = HashMap::new();
        for peer in instances {
            if !peer.may_respond() || peer.tier != tier_name {
                continue;
            }
            let Some(address) = peer_addresses.get(&peer.raft_id) else {
                crate::tlog!(Warning, "skipping instance: address not found for peer";
                    "raft_id" => peer.raft_id,
                );
                continue;
            };
            let Some(r) = replicasets.get(&peer.replicaset_name) else {
                crate::tlog!(Debug, "skipping instance: replicaset not initialized yet";
                    "instance_name" => %peer.name,
                );
                continue;
            };

            let replicaset = sharding
                .entry(peer.replicaset_uuid.clone())
                .or_insert_with(|| ReplicasetSpec {
                    weight: Some(r.weight),
                    ..Default::default()
                });

            replicaset.replicas.insert(
                peer.uuid.clone(),
                ReplicaSpec {
                    uri: format!("{PICO_SERVICE_USER_NAME}@{address}"),
                    master: r.current_master_name == peer.name,
                    name: peer.name.to_string(),
                },
            );
        }

        Self {
            listen: None,
            sharding,
            discovery_mode: DiscoveryMode::On,
            space_bucket_id: TABLE_ID_BUCKET,
            bucket_count,
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
