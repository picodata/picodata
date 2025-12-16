use crate::config::PicodataConfig;
use crate::instance::InstanceName;
use crate::pico_service::pico_service_password;
use crate::replicaset::Weight;
use crate::rpc::ddl_apply::Response;
use crate::schema::PICO_SERVICE_USER_NAME;
use crate::sql::router;
use crate::storage::ToEntryIter as _;
use crate::storage::TABLE_ID_BUCKET;
use crate::tarantool::ListenConfig;
use crate::topology_cache::TopologyCacheRef;
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
use tarantool::define_str_enum;
use tarantool::space::SpaceId;
use tarantool::tlua;
use tarantool::tuple::ToTupleBuffer;

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
pub fn ddl_map_callrw<T>(
    tier: &str,
    function_name: &str,
    rpc_timeout: Duration,
    req: &T,
) -> Result<Vec<DdlMapCallRwRes>, Error>
where
    T: ToTupleBuffer + ?Sized,
{
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

////////////////////////////////////////////////////////////////////////////////
// VshardConfig
////////////////////////////////////////////////////////////////////////////////

/// See `cfg_template` in `vshard/vshard/cfg.lua`
#[rustfmt::skip]
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Default, Clone, Debug, PartialEq, tlua::PushInto, tlua::Push, tlua::LuaRead)]
pub struct VshardConfig {
    pub sharding: HashMap<String, ReplicasetSpec>,
    discovery_mode: DiscoveryMode,

    /// Id of system table `_bucket`.
    space_bucket_id: SpaceId,

    /// Total number of virtual buckets on each tier.
    bucket_count: u64,

    /// Timeout value used when sending health check RPCs between replicas.
    ///
    /// Default in vshard is `5` seconds.
    failover_ping_timeout: f64,

    /// The period in seconds with which replicas are sending each other health checks.
    ///
    /// Default in vshard is `1` second.
    failover_interval: f64,

    /// If set to `manual` vshard will not call box.cfg when being reconfigured,
    /// which is what we want.
    box_cfg_mode: BoxCfgMode,

    /// Passed to net.box. If `true` then net.box will fetch the schema upon
    /// connection. We don't need that.
    connection_fetch_schema: bool,

    /// This field is different for each instance, so we don't set it when
    /// generating the common config.
    #[serde(skip_serializing_if="Option::is_none")]
    #[serde(default)]
    pub listen: Option<ListenConfig>,
}

const VSHARD_FAILOVER_INTERVAL: f64 = 10.0;

define_str_enum! {
    #[derive(Default)]
    pub enum BoxCfgMode {
        #[default]
        Auto = "auto",
        Manual = "manual",
    }
}

#[rustfmt::skip]
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Default, Clone, Debug, PartialEq, tlua::PushInto, tlua::Push, tlua::LuaRead)]
pub struct ReplicasetSpec {
    replicas: HashMap<String, ReplicaSpec>,
    weight: Option<Weight>,
}

#[rustfmt::skip]
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Default, Clone, Debug, PartialEq, tlua::PushInto, tlua::Push, tlua::LuaRead)]
pub struct ReplicaSpec {
    uri: ListenConfig,
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
        node: &node::Node,
        tier_name: &str,
        bucket_count: u64,
    ) -> Result<Self, Error> {
        let topology = node.topology_cache.get();
        let peer_addresses: HashMap<_, _> = node
            .storage
            .peer_addresses
            .iter()?
            .filter(|peer| peer.connection_type == ConnectionType::Iproto)
            .map(|pa| (pa.raft_id, pa.address))
            .collect();

        let result = Self::new(topology, &peer_addresses, tier_name, bucket_count);
        Ok(result)
    }

    pub fn new(
        topology: TopologyCacheRef,
        peer_addresses: &HashMap<RaftId, String>,
        tier_name: &str,
        bucket_count: u64,
    ) -> Self {
        let mut sharding: HashMap<String, ReplicasetSpec> = HashMap::new();
        for peer in topology.all_instances() {
            if !peer.may_respond() || peer.tier != tier_name {
                continue;
            }
            let Some(address) = peer_addresses.get(&peer.raft_id) else {
                crate::tlog!(Warning, "skipping instance: address not found for peer";
                    "raft_id" => peer.raft_id,
                );
                continue;
            };
            let Ok(r) = topology.replicaset_by_name(&peer.replicaset_name) else {
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

            let tls_config = &PicodataConfig::get().instance.iproto_tls;
            replicaset.replicas.insert(
                peer.uuid.clone(),
                ReplicaSpec {
                    uri: ListenConfig::new(
                        format!("{PICO_SERVICE_USER_NAME}@{address}"),
                        tls_config,
                    ),
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
            failover_ping_timeout: VSHARD_FAILOVER_INTERVAL,
            failover_interval: VSHARD_FAILOVER_INTERVAL,
            // We don't need vshard to be calling box.cfg() for us,
            // governor always calls proc_replication before proc_sharding.
            box_cfg_mode: BoxCfgMode::Manual,
            // We don't need vshard net.box connections to have up-to-date schema definitions
            connection_fetch_schema: false,
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
                let Some((username, address)) = replica.uri.uri.split_once('@') else {
                    continue;
                };
                if username != PICO_SERVICE_USER_NAME {
                    continue;
                }
                replica.uri.uri = format!("{username}:{password}@{address}");
            }
        }
    }
}
