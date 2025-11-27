use crate::config::PicodataConfig;
use crate::instance::InstanceName;
use crate::luamod::lua_function;
use crate::pico_service::pico_service_password;
use crate::replicaset::Weight;
use crate::rpc::ddl_apply::Response;
use crate::schema::PICO_SERVICE_USER_NAME;
use crate::sql::router;
use crate::storage::ToEntryIter as _;
use crate::storage::{SPACE_ID_INTERNAL_MAX, TABLE_ID_BUCKET};
use crate::tarantool::ListenConfig;
use crate::topology_cache::TopologyCacheRef;
use crate::traft::error::Error as TraftError;
use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::ConnectionType;
use crate::traft::RaftId;
use crate::traft::Result;
use ::tarantool::msgpack::ViaMsgpack;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use sql::executor::engine::Vshard;
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
    pub uuid: SmolStr,
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
    // `pico._ddl_map_callrw` is defined in `src/vshard_helpers.lua`
    let func = lua_function("_ddl_map_callrw")?;
    let args = Tuple::new(req)?;

    let res_raw = func
        .call_with_args::<HashMap<SmolStr, [ViaMsgpack<Response>; 1]>, _>((
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

    // `pico._replicaset_priority_list` is defined in `src/vshard_helpers.lua`
    let func = lua_function("_replicaset_priority_list")?;
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
pub fn get_replicaset_uuid_by_bucket_id(tier: &str, bucket_id: u64) -> Result<SmolStr, Error> {
    let info = router::get_tier_info(tier)?;

    let max_bucket_id = info.bucket_count();
    if bucket_id < 1 || bucket_id > max_bucket_id {
        #[rustfmt::skip]
        return Err(Error::other(format!("invalid bucket id: must be within 1..{max_bucket_id}, got {bucket_id}")));
    }

    // `pico._replicaset_uuid_by_bucket_id` is defined in `src/vshard_helpers.lua`
    let func = lua_function("_replicaset_uuid_by_bucket_id")?;
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
    pub sharding: HashMap<SmolStr, ReplicasetSpec>,
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

    /// When this is enabled, the vshard can use primary key as shard index
    /// in addition to the "shard_index" parameter.
    #[serde(skip_serializing_if="Option::is_none")]
    #[serde(default)]
    pub use_pk_as_shard_index: Option<bool>,

    /// Upper limit for internal space IDs - prevents internal spaces
    /// from being sharded when numeric shard_index used.
    #[serde(skip_serializing_if="Option::is_none")]
    #[serde(default)]
    pub max_internal_space_id: Option<u32>,
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
    replicas: HashMap<SmolStr, ReplicaSpec>,
    weight: Option<Weight>,
}

#[rustfmt::skip]
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Default, Clone, Debug, PartialEq, tlua::PushInto, tlua::Push, tlua::LuaRead)]
pub struct ReplicaSpec {
    uri: ListenConfig,
    name: SmolStr,
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
        peer_addresses: &HashMap<RaftId, SmolStr>,
        tier_name: &str,
        bucket_count: u64,
    ) -> Self {
        let mut sharding: HashMap<SmolStr, ReplicasetSpec> = HashMap::new();
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

            let is_master = Some(&peer.name) == r.effective_master_name();

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
                    master: is_master,
                    name: peer.name.clone().into(),
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
            use_pk_as_shard_index: Some(true),
            max_internal_space_id: Some(SPACE_ID_INTERNAL_MAX),
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

////////////////////////////////////////////////////////////////////////////////
// VshardErrorCode
////////////////////////////////////////////////////////////////////////////////

/// Copied from `vshard/vshard/error.lua`
pub enum VshardErrorCode {
    /// msg = 'Cannot perform action with bucket %d, reason: %s'
    WrongBucket = 1,
    /// msg = 'Replica %s is not a master for replicaset %s anymore'
    NonMaster = 2,
    /// msg = 'Bucket %d already exists'
    BucketAlreadyExists = 3,
    /// msg = 'Replicaset %s not found'
    NoSuchReplicaset = 4,
    /// msg = 'Cannot move: bucket %d is already on replicaset %s'
    MoveToSelf = 5,
    /// msg = 'Master is not configured for replicaset %s'
    MissingMaster = 6,
    /// msg = 'Bucket %d is transferring to replicaset %s'
    TransferIsInProgress = 7,
    /// msg = 'There is no active replicas in replicaset %s'
    UnreachableReplicaset = 8,
    /// msg = 'Bucket %d cannot be found. Is rebalancing in progress?'
    NoRouteToBucket = 9,
    /// msg = 'Cluster is already bootstrapped'
    NonEmpty = 10,
    /// msg = 'Master of replicaset %s is unreachable: %s'
    UnreachableMaster = 11,
    /// msg = 'Replica is out of sync'
    OutOfSync = 12,
    /// msg = 'High replication lag: %f'
    HighReplicationLag = 13,
    /// msg = "Replica %s isn't active"
    UnreachableReplica = 14,
    /// msg = 'Only one replica is active'
    LowRedundancy = 15,
    /// msg = 'Sending and receiving buckets at same time is not allowed'
    InvalidRebalancing = 16,
    /// msg = 'A current read replica in replicaset %s is not optimal'
    SuboptimalReplica = 17,
    /// msg = '%d buckets are not discovered'
    UnknownBuckets = 18,
    /// msg = 'Replicaset is locked'
    ReplicasetIsLocked = 19,
    /// msg = 'Object is outdated after module reload/reconfigure. Use new instance.'
    ObjectIsOutdated = 20,
    /// msg = 'Router with name %s already exists'
    RouterAlreadyExists = 21,
    /// msg = 'Bucket %d is locked'
    BucketIsLocked = 22,
    /// msg = 'Invalid configuration: %s'
    InvalidCfg = 23,
    /// msg = 'Bucket %d is pinned'
    BucketIsPinned = 24,
    /// msg = 'Too many receiving buckets at once, please, throttle'
    TooManyReceiving = 25,
    /// msg = 'Storage is referenced'
    StorageIsReferenced = 26,
    /// msg = 'Can not add a storage ref: %s'
    StorageRefAdd = 27,
    /// msg = 'Can not use a storage ref: %s'
    StorageRefUse = 28,
    /// msg = 'Can not delete a storage ref: %s'
    StorageRefDel = 29,
    /// msg = 'Can not receive the bucket %s data in space "%s" at tuple %s: %s'
    BucketRecvDataError = 30,
    /// msg = 'Replicaset %s is in backoff, can\'t take requests right now. Last error was %s'
    ReplicasetInBackoff = 32,
    /// msg = 'Storage is disabled: %s'
    StorageIsDisabled = 33,
    /// msg = 'Bucket %d is corrupted: %s'
    /// -- That is similar to WRONG_BUCKET, but the latter is not critical. It
    /// -- usually can be retried. Corruption is a critical error, it requires
    /// -- more attention.
    BucketIsCorrupted = 34,
    /// msg = 'Router is disabled: %s'
    RouterIsDisabled = 35,
    /// msg = 'Error during bucket GC: %s'
    BucketGcError = 36,
    /// msg = 'Configuration of the storage is in progress'
    StorageCfgIsInProgress = 37,
    /// msg = 'Configuration of the router with name %s is in progress'
    RouterCfgIsInProgress = 38,
    /// msg = 'Bucket %s update is invalid: %s'
    BucketInvalidUpdate = 39,
    /// msg = 'Handshake with %s have not been completed yet'
    VhandshakeNotComplete = 40,
    /// msg = 'Mismatch server name: expected "%s", but got "%s"'
    InstanceNameMismatch = 41,
}

////////////////////////////////////////////////////////////////////////////////
// lua helpers
////////////////////////////////////////////////////////////////////////////////

pub fn init_lua_helpers() {
    let lua = tarantool::lua_state();

    () = tlua::LuaFunction::load_file_contents(
        &lua,
        include_str!("vshard_helpers.lua"),
        "src/vshard_helpers.lua",
    )
    .expect("loading a file known at compile time")
    .into_call()
    .expect("loading a file known at compile time");
}
