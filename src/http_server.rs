use crate::info::{RuntimeInfo, VersionInfo};
use crate::instance::{Instance, InstanceName, StateVariant};
use crate::replicaset::{Replicaset, ReplicasetName, ReplicasetState};
use crate::storage::Catalog;
use crate::storage::ToEntryIter as _;
use crate::tier::Tier;
use crate::traft::network::ConnectionPool;
use crate::traft::{self, node, Result};
use crate::util::Uppercase;
use crate::{has_states, tlog, unwrap_ok_or};
use futures::future::join_all;
use http::StatusCode;
use raft::Storage as _;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use smol_str::{format_smolstr, ToSmolStr};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use tarantool::fiber;
use tarantool::fiber::r#async::timeout::IntoTimeout;
use tarantool::index::IteratorType;
use tarantool::session::with_su;
use tarantool::space::Space;
use tarantool::tlua::{self, Index as _};

const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const AUTH_TOKEN_EXPIRY: chrono::Duration = chrono::Duration::hours(24);
const REFRESH_TOKEN_EXPIRY: chrono::Duration = chrono::Duration::days(365);
const CONTENT_TYPE_JSON: &'static str = "application/json";

extern "C" {
    fn tarantool_uptime() -> f64;
}

/// Newtype wrapper around `http::Response<String>` for HTTP API responses.
/// We have to make it a newtype and not just an alias to provide From/Into
/// imlementations for HttpResponseTable and not hit the orphan rule.
pub(crate) struct HttpResponse(pub(crate) http::Response<String>);

impl HttpResponse {
    /// Creates a JSON response with the given status code and body.
    pub fn to_json_response(status: StatusCode, body: String) -> Self {
        /* Building HTTP response can't fail here because it would fail only for invalid status
         * code (outside 100-999 range), invalid HTTP version or header name/value issues such as
         * \r\n characters. None of these can possibly happen here.
         */
        Self(
            http::Response::builder()
                .status(status)
                .header(http::header::CONTENT_TYPE, CONTENT_TYPE_JSON)
                .body(body)
                .expect("building HTTP response should not fail"),
        )
    }
}

/// Type alias for the `AsTable` structure expected by Lua HTTP server.
pub(crate) type HttpResponseTable = tarantool::tlua::AsTable<(
    (&'static str, u16),
    (&'static str, String),
    (
        &'static str,
        tarantool::tlua::AsTable<((&'static str, String),)>,
    ),
)>;

impl From<HttpResponse> for HttpResponseTable {
    fn from(response: HttpResponse) -> Self {
        let status = response.0.status().as_u16();
        let content_type = response
            .0
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(CONTENT_TYPE_JSON)
            .to_owned();
        let body = response.0.into_body();

        tarantool::tlua::AsTable((
            ("status", status),
            ("body", body),
            (
                "headers",
                tarantool::tlua::AsTable((("content-type", content_type),)),
            ),
        ))
    }
}

fn as_admin<T>(f: impl FnOnce() -> T) -> T {
    with_su(1, f).expect("becoming admin should not fail")
}

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    typ: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    roles: Option<Vec<String>>,
}

#[derive(Debug, Default, Serialize)]
pub(crate) struct TokenResponse {
    auth: String,
    refresh: String,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
    #[serde(rename = "errorMessage")]
    error_message: String,
}

impl std::fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(value) => write!(f, "{}", value),
            Err(e) => write!(f, r#"{{"error":"{}","errorMessage":"Internal error"}}"#, e),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct AuthContext {
    pub username: String,
    pub roles: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AuthError {
    #[error("{0}")]
    Inner(String),
    #[error("auth disabled")]
    Disabled,
    #[error("invalid authorization header")]
    InvalidHeader,
    #[error("invalid token type")]
    InvalidType,
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("insufficient privileges")]
    InsufficientPrivileges,
    #[error("failed to encode jwt: {0}")]
    JWTEncoding(jwt_compact::CreationError),
    #[error("failed to decode jwt: {0}")]
    JWTParsing(jwt_compact::ParseError),
    #[error("invalid jwt: {0}")]
    JWTValidation(jwt_compact::ValidationError),
    #[error("failed to find user jwt: {0}")]
    UserLookup(String),
    #[error("failed to find privileges: {0}")]
    PrivilegesLookup(String),
}

impl AuthError {
    fn error_type(&self) -> String {
        String::from(match self {
            AuthError::Inner(..) => "error",
            AuthError::InvalidCredentials => "wrongCredentials",
            AuthError::JWTValidation(jwt_compact::ValidationError::Expired) => "sessionExpired",
            AuthError::Disabled => "authDisabled",
            _ => "authError",
        })
    }
    fn status_code(&self) -> StatusCode {
        match self {
            AuthError::Inner(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AuthError::InsufficientPrivileges | AuthError::PrivilegesLookup(_) => {
                StatusCode::FORBIDDEN
            }
            _ => StatusCode::UNAUTHORIZED,
        }
    }

    fn error_msg(&self) -> String {
        self.to_string()
    }
}

type AuthResult<T> = std::result::Result<T, AuthError>;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct HealthCheckError(SmolStr);

impl From<HealthCheckError> for HttpResponse {
    fn from(value: HealthCheckError) -> Self {
        let body = format!(r#"{{"status":"not_ready","reason":"{}"}}"#, value.0);
        HttpResponse::to_json_response(http::StatusCode::SERVICE_UNAVAILABLE, body)
    }
}

type HealthCheckResult = std::result::Result<HealthCheckOk, HealthCheckError>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApiError {
    #[error("{0}")]
    Auth(#[from] AuthError),
    #[error("{0}")]
    Internal(#[from] crate::traft::error::Error),
    #[error("{0}")]
    HealthCheck(#[from] HealthCheckError),
}

impl From<AuthError> for HttpResponse {
    fn from(value: AuthError) -> Self {
        let status = value.status_code();
        let body = ErrorResponse {
            error: value.error_type(),
            error_message: value.error_msg(),
        }
        .to_string();

        HttpResponse::to_json_response(status, body)
    }
}

impl From<crate::traft::error::Error> for HttpResponse {
    fn from(value: crate::traft::error::Error) -> Self {
        let body = ErrorResponse {
            error: String::from("error"),
            error_message: value.to_string(),
        }
        .to_string();
        HttpResponse::to_json_response(StatusCode::INTERNAL_SERVER_ERROR, body)
    }
}

impl From<ApiError> for HttpResponse {
    fn from(value: ApiError) -> Self {
        match value {
            ApiError::Auth(e) => e.into(),
            ApiError::Internal(e) => e.into(),
            ApiError::HealthCheck(e) => e.into(),
        }
    }
}

pub(crate) type ApiResult<T> = std::result::Result<T, ApiError>;

/// Response from instances:
/// - `raft_id`: instance raft_id to find Instance to store data
/// - `httpd_address`: host:port of hhtpd on instance if exists or empty string
/// - `mem_usable`: quota_size from box.slab.info
/// - `mem_used`: quota_used from box.slab.info
#[derive(Deserialize)]
struct InstanceDataResponse {
    mem_usable: u64,
    mem_used: u64,
}

#[derive(Clone, Default)]
struct InstanceAddresses {
    iproto: SmolStr,
    pgproto: SmolStr,
    http: SmolStr,
}

/// Memory info struct for server responces
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct MemoryInfo {
    usable: u64,
    used: u64,
}

// From the Lua version:
//
// --[[
//     export interface InstanceType {
//     name: string;
//     targetState: string;
//     currentState: string;
//     failureDomain: Record<string, string>;
//     version: string;
//     isLeader: boolean;
//     binaryAddress: string;
//     httpAddress: string;
// }
// ]]
//
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct InstanceInfo {
    http_address: SmolStr,
    version: SmolStr,
    failure_domain: HashMap<Uppercase, Uppercase>,
    is_leader: bool,
    is_voter: bool,
    is_raft_leader: bool,
    current_state: StateVariant,
    target_state: StateVariant,
    name: InstanceName,
    binary_address: SmolStr,
    pg_address: SmolStr,
}

// From the Lua version:
//
// --[[
// export interface ReplicasetType {
//     name: string;
//     instanceCount: number;
//     instances: InstanceType[];
//     version: string;
//     state: string;
//     capacity: number;
// }
// ]]
//
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReplicasetInfo {
    version: SmolStr,
    // TECHDEBT: This field is misnamed — it actually represents the current state of the
    // replicaset's leader instance (i.e. `StateVariant`), not the replicaset itself.
    // It is kept as `state` in the response for backward API compatibility.
    // The actual replicaset state from `_pico_replicaset` is exposed as `replicasetState`.
    state: StateVariant,
    // The actual replicaset state from the `_pico_replicaset` system table.
    // This cannot be named `state` due to the existing field above (see comment there).
    replicaset_state: ReplicasetState,
    instance_count: usize,
    uuid: SmolStr,
    instances: Vec<InstanceInfo>,
    capacity_usage: f64,
    memory: MemoryInfo,
    name: ReplicasetName,
    #[serde(skip)]
    tier: SmolStr,
}

// From the Lua version:
//
// [[ the expected response:
// export type TierType = {
//     id: string;
//     plugins: string[];
//     replicasetCount: number;
//     instanceCount: number;
//     rf: number;
//     canVoter: boolean;
//     replicasets: ReplicasetType[];
// };
// ]]
//
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TierInfo {
    replicasets: Vec<ReplicasetInfo>,
    replicaset_count: usize,
    rf: u8,
    bucket_count: u64,
    instance_count: usize,
    #[serde(rename = "can_vote")] // for compatibility with lua version
    can_vote: bool,
    name: SmolStr,
    services: Vec<SmolStr>,
    memory: MemoryInfo,
    capacity_usage: f64,
}

// From the Lua version:
//
// -- the expected response:
// -- --------------------------------------------------
// -- export interface ClusterInfoType {
// --     capacityUsage: string;
// --     memory: {
// --      used: string;
// --      usable: string;
// --     };
// --     replicasetsCount: number;
// --     instancesCurrentStateOnline: number;
// --     instancesCurrentStateOffline: number;
// --     currentInstaceVersion: string;
// -- }
// -- --------------------------------------------------
//
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterInfo {
    capacity_usage: f64,
    cluster_name: &'static str,
    cluster_version: SmolStr,
    #[serde(rename = "currentInstaceVersion")] // for compatibility with lua version
    current_instance_version: SmolStr,
    replicasets_count: usize,
    instances_current_state_offline: usize,
    memory: MemoryInfo,
    instances_current_state_online: usize,
    // list of serialized plugin identifiers - "<plugin_name> <plugin_version>"
    plugins: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UiConfig {
    is_auth_enabled: bool,
}

pub(crate) struct HealthCheckOk;

impl Serialize for HealthCheckOk {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Helper {
            status: &'static str,
        }
        Helper { status: "ok" }.serialize(serializer)
    }
}

fn get_replicasets(storage: &Catalog) -> traft::Result<HashMap<ReplicasetName, Replicaset>> {
    let i = storage.replicasets.iter()?;
    Ok(i.map(|item| (item.name.clone(), item)).collect())
}

/// Reads instance addresses from `_pico_peer_address`.
///
/// When `only_leaders` is true, addresses are only returned for replicaset leaders.
/// When false, addresses are returned for all instances.
fn get_peer_addresses(
    storage: &Catalog,
    replicasets: &HashMap<ReplicasetName, Replicaset>,
    instances: &[Instance],
    only_leaders: bool,
) -> traft::Result<HashMap<u64, InstanceAddresses>> {
    let targets: HashMap<_, _> = instances
        .iter()
        .filter(|item| {
            !only_leaders
                || replicasets
                    .get(&item.replicaset_name)
                    .is_some_and(|r| r.current_master_name == item.name)
        })
        .map(|item| (item.raft_id, true))
        .collect();
    use crate::traft::{ConnectionType, SystemConnectionType};
    let i = storage.peer_addresses.iter()?.filter(|peer| {
        matches!(
            peer.connection_type,
            ConnectionType::System(SystemConnectionType::Iproto)
                | ConnectionType::System(SystemConnectionType::Pgproto)
                | ConnectionType::System(SystemConnectionType::Http)
        )
    });
    Ok(i.filter(|pa| targets.contains_key(&pa.raft_id))
        .fold(HashMap::new(), |mut acc, pa| {
            let addrs = acc.entry(pa.raft_id).or_default();
            match pa.connection_type {
                ConnectionType::System(SystemConnectionType::Iproto) => {
                    addrs.iproto = pa.address.clone()
                }
                ConnectionType::System(SystemConnectionType::Pgproto) => {
                    addrs.pgproto = pa.address.clone()
                }
                ConnectionType::System(SystemConnectionType::Http) => {
                    addrs.http = pa.address.clone()
                }
                ConnectionType::Plugin { .. } => {}
            };
            acc
        }))
}

/// Get memory info from replicaset leaders via RPC.
///
/// Only queries the specified leader instances to minimize RPC calls.
async fn get_instances_data(
    pool: &ConnectionPool,
    instances: &[Instance],
    leaders: &HashSet<InstanceName>,
) -> HashMap<u64, InstanceDataResponse> {
    let mut fs = vec![];
    for instance in instances {
        if has_states!(instance, Expelled -> *) {
            continue;
        }
        if !leaders.contains(&instance.name) {
            continue;
        }

        let res = pool.call_raw(
            &instance.name,
            ".proc_runtime_info_v2",
            &(),
            DEFAULT_TIMEOUT,
        );
        let future = unwrap_ok_or!(res,
            Err(e) => {
                tlog!(Error, "webui: error on calling .proc_runtime_info_v2 on instance {}: {e}", instance.name);
                continue;
            }
        // we have to add timeout directly to future due
        // to the bug in connection pool that does not consider
        // timeout when establishing TCP connection
        // See https://git.picodata.io/picodata/picodata/picodata/-/issues/943
        ).timeout(DEFAULT_TIMEOUT);
        fs.push({
            async move {
                let mut data = InstanceDataResponse {
                    mem_usable: 0u64,
                    mem_used: 0u64,
                };
                match future.await {
                    Ok(info) => {
                        let info: RuntimeInfo = info;
                        data.mem_usable = info.slab_info.quota_size;
                        data.mem_used = info.slab_info.quota_used;
                    }
                    Err(e) => {
                        tlog!(
                            Error,
                            "webui: error on calling .proc_runtime_info_v2 on instance {}: {e}",
                            instance.name,
                        );
                    }
                }
                (instance.raft_id, data)
            }
        })
    }
    join_all(fs)
        .await
        .into_iter()
        .collect::<HashMap<u64, InstanceDataResponse>>()
}

// Collect detailed information from replicasets and instances
//
fn get_replicasets_info(
    storage: &Catalog,
    only_leaders: bool,
) -> traft::Result<Vec<ReplicasetInfo>> {
    let node = crate::traft::node::global()?;
    let instances = storage.instances.all_instances()?;
    let replicasets = get_replicasets(storage)?;

    let voters: HashSet<u64> = node
        .raft_storage
        .voters()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let raft_leader_id = node.status().leader_id;

    // Compute leaders set for RPC filtering
    let leaders: HashSet<InstanceName> = replicasets
        .values()
        .map(|r| r.current_master_name.clone())
        .collect();

    // RPC only to leaders for memory info
    let instances_props = fiber::block_on(get_instances_data(&node.pool, &instances, &leaders));
    // Addresses from storage (http, iproto, pgproto)
    let addresses = get_peer_addresses(storage, &replicasets, &instances, only_leaders)?;

    let mut res: HashMap<ReplicasetName, ReplicasetInfo> =
        HashMap::with_capacity(replicasets.len());

    for instance in instances {
        let addrs = addresses
            .get(&instance.raft_id)
            .cloned()
            .unwrap_or_default();

        let replicaset_name = instance.replicaset_name;
        let mut is_leader = false;
        let mut replicaset_uuid = SmolStr::default();
        let mut tier = instance.tier.clone();
        let mut replicaset_state = ReplicasetState::default();
        if let Some(replicaset) = replicasets.get(&replicaset_name) {
            is_leader = replicaset.current_master_name == instance.name;
            replicaset_uuid = replicaset.uuid.clone();
            debug_assert_eq!(replicaset.tier, instance.tier);
            tier.clone_from(&replicaset.tier);
            replicaset_state = replicaset.state;
        }

        // Memory info from RPC (only available for leaders)
        let mut mem_usable: u64 = 0u64;
        let mut mem_used: u64 = 0u64;
        if let Some(rpc_data) = instances_props.get(&instance.raft_id) {
            mem_usable = rpc_data.mem_usable;
            mem_used = rpc_data.mem_used;
        }

        let is_voter = voters.contains(&instance.raft_id);
        let is_raft_leader = raft_leader_id == Some(instance.raft_id);

        let instance_info = InstanceInfo {
            http_address: addrs.http,
            version: instance.picodata_version.clone(),
            failure_domain: instance.failure_domain.data,
            is_leader,
            is_voter,
            is_raft_leader,
            current_state: instance.current_state.variant,
            target_state: instance.target_state.variant,
            name: instance.name.clone(),
            binary_address: addrs.iproto,
            pg_address: addrs.pgproto,
        };

        let replicaset_info = res
            .entry(replicaset_name)
            .or_insert_with_key(|replicaset_name| ReplicasetInfo {
                version: instance_info.version.clone(),
                state: instance_info.current_state,
                replicaset_state,
                instance_count: 0,
                uuid: replicaset_uuid,
                capacity_usage: 0_f64,
                instances: Vec::new(),
                memory: MemoryInfo { usable: 0, used: 0 },
                name: replicaset_name.clone(),
                tier,
            });

        if is_leader {
            replicaset_info.capacity_usage = get_capacity_usage(mem_usable, mem_used);
            replicaset_info.memory.usable = mem_usable;
            replicaset_info.memory.used = mem_used;

            replicaset_info.state = instance_info.current_state;

            replicaset_info.version = instance_info.version.clone();
        }
        replicaset_info.instances.push(instance_info);
        replicaset_info.instance_count += 1;
    }

    Ok(res.into_values().collect())
}

fn get_capacity_usage(mem_usable: u64, mem_used: u64) -> f64 {
    if mem_usable == 0 {
        0_f64
    } else {
        ((mem_used as f64) / (mem_usable as f64) * 10000_f64).round() / 100_f64
    }
}

fn http_api_cluster() -> traft::Result<ClusterInfo> {
    let version = VersionInfo::current().picodata_version.clone();

    let storage = Catalog::get();
    let replicasets = get_replicasets_info(storage, true)?;

    let plugins = storage
        .plugins
        .get_all()
        .expect("storage shouldn't fail")
        .iter()
        .map(|plugin| format!("{} {}", plugin.name, plugin.version))
        .collect();

    let cluster_version = storage.properties.cluster_version()?;

    let mut instances = 0;
    let mut instances_online = 0;
    let mut replicasets_count = 0;
    let mut mem_info = MemoryInfo { usable: 0, used: 0 };

    for replicaset in replicasets {
        replicasets_count += 1;
        instances += replicaset.instance_count;
        mem_info.usable += replicaset.memory.usable;
        mem_info.used += replicaset.memory.used;
        replicaset.instances.iter().for_each(|i| {
            instances_online += if i.current_state == StateVariant::Online {
                1
            } else {
                0
            }
        });
    }

    let cluster_name = node::global()?.topology_cache.cluster_name;

    let res = ClusterInfo {
        capacity_usage: get_capacity_usage(mem_info.usable, mem_info.used),
        cluster_name,
        cluster_version,
        current_instance_version: version,
        replicasets_count,
        instances_current_state_offline: (instances - instances_online),
        memory: mem_info,
        instances_current_state_online: instances_online,
        plugins,
    };

    Ok(res)
}

#[derive(Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
struct ReplicasetMemoryInfo {
    name: ReplicasetName,
    usable: u64,
    used: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TierMemoryInfo {
    name: SmolStr,
    usable: u64,
    used: u64,
    replicasets: BTreeSet<ReplicasetMemoryInfo>,
}

fn http_api_memory() -> traft::Result<Vec<TierMemoryInfo>> {
    let storage = Catalog::get();
    let replicasets = get_replicasets_info(storage, true)?;
    let tiers = storage.tiers.iter()?;

    let mut resp: HashMap<SmolStr, TierMemoryInfo> = tiers
        .map(|tier: Tier| {
            (
                tier.name.clone(),
                TierMemoryInfo {
                    name: tier.name,
                    usable: 0,
                    used: 0,
                    replicasets: BTreeSet::new(),
                },
            )
        })
        .collect();

    for replicaset in replicasets {
        let Some(tier) = resp.get_mut(&replicaset.tier) else {
            tlog!(
                Warning,
                "replicaset `{}` is assigned tier `{}`, which is not found in _pico_tier",
                replicaset.name,
                replicaset.tier,
            );
            continue;
        };

        tier.usable += replicaset.memory.usable;
        tier.used += replicaset.memory.used;
        tier.replicasets.insert(ReplicasetMemoryInfo {
            name: replicaset.name,
            usable: replicaset.memory.usable,
            used: replicaset.memory.used,
        });
    }

    Ok(resp.into_values().collect())
}

fn http_api_tiers() -> traft::Result<Vec<TierInfo>> {
    let storage = Catalog::get();
    let replicasets = get_replicasets_info(storage, false)?;
    let tiers = storage.tiers.iter()?;

    let mut res: HashMap<_, _> = tiers
        .map(|item: Tier| {
            (
                item.name.clone(),
                TierInfo {
                    replicasets: Vec::new(),
                    replicaset_count: 0,
                    rf: item.replication_factor,
                    bucket_count: item.bucket_count,
                    instance_count: 0,
                    can_vote: item.can_vote,
                    name: item.name.clone(),
                    memory: MemoryInfo { usable: 0, used: 0 },
                    capacity_usage: 0_f64,
                    services: storage
                        .services
                        .get_by_tier(&item.name)
                        .expect("storage shouldn't fail")
                        .iter()
                        .map(|service| service.name.clone())
                        .collect(),
                },
            )
        })
        .collect();

    for replicaset in replicasets {
        let Some(tier) = res.get_mut(&replicaset.tier) else {
            tlog!(
                Warning,
                "replicaset `{}` is assigned tier `{}`, which is not found in _pico_tier",
                replicaset.name,
                replicaset.tier,
            );
            continue;
        };

        tier.replicaset_count += 1;
        tier.instance_count += replicaset.instances.len();

        tier.memory.usable += replicaset.memory.usable;
        tier.memory.used += replicaset.memory.used;

        tier.replicasets.push(replicaset);
    }

    res.iter_mut().for_each(|(_, tier)| {
        tier.replicasets.sort_by_key(|rs| rs.name.clone());
        tier.capacity_usage = get_capacity_usage(tier.memory.usable, tier.memory.used);
    });

    Ok(res.into_values().collect())
}

fn get_jwt_secret() -> AuthResult<Option<jwt_compact::alg::Hs256Key>> {
    let node = crate::traft::node::global().map_err(|e| AuthError::Inner(e.to_string()))?;
    let secret = node.alter_system_parameters.borrow().jwt_secret.clone();

    // Given successful db connection, secret cannot technically be None at this point,
    // as it is set to empty string to disable auth instead of NULL
    if secret.is_empty() {
        return Ok(None);
    }

    let secret = jwt_compact::alg::Hs256Key::new(secret.as_bytes());

    Ok(Some(secret))
}

fn get_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn create_jwt_token(
    username: &str,
    token_type: &str,
    roles: Option<Vec<String>>,
) -> AuthResult<String> {
    use jwt_compact::AlgorithmExt as _;

    let Some(secret_key) = get_jwt_secret()? else {
        return Err(AuthError::Disabled);
    };

    let time_options = jwt_compact::TimeOptions::default();

    let duration = match token_type {
        "auth" => AUTH_TOKEN_EXPIRY,
        "refresh" => REFRESH_TOKEN_EXPIRY,
        _ => return Err(AuthError::InvalidType),
    };

    let claims = JwtClaims {
        sub: username.to_string(),
        typ: token_type.to_string(),
        roles,
    };

    let header = jwt_compact::Header::empty();
    let claims = jwt_compact::Claims::new(claims).set_duration(&time_options, duration);

    jwt_compact::alg::Hs256
        .token(&header, &claims, &secret_key)
        .map_err(AuthError::JWTEncoding)
}

fn get_jwt_claims(auth_header: &str) -> AuthResult<JwtClaims> {
    use jwt_compact::AlgorithmExt as _;

    let Some(secret_key) = get_jwt_secret()? else {
        return Err(AuthError::Disabled);
    };

    let time_options = jwt_compact::TimeOptions::default();

    let token = auth_header
        .strip_prefix("Bearer ")
        .ok_or(AuthError::InvalidHeader)?;

    let validator = jwt_compact::alg::Hs256.validator(&secret_key);

    let token = jwt_compact::UntrustedToken::new(&token).map_err(AuthError::JWTParsing)?;

    let token = validator
        .validate(&token)
        .map_err(AuthError::JWTValidation)?;

    let (_, claims) = token.into_parts();

    claims
        .validate_expiration(&time_options)
        .map_err(AuthError::JWTValidation)?;

    Ok(claims.custom)
}

fn authenticate_user(username: &str, password: Option<&str>) -> AuthResult<Vec<String>> {
    if username.is_empty() {
        return Err(AuthError::InvalidCredentials);
    }

    let storage = Catalog::get();

    let user = storage
        .users
        .by_name(username)
        .map_err(|e| AuthError::UserLookup(e.to_string()))?
        .ok_or(AuthError::InvalidCredentials)?;

    let privileges = storage
        .privileges
        .by_grantee_id(user.id)
        .map_err(|e| AuthError::PrivilegesLookup(e.to_string()))?
        .map(|p| p.privilege().as_str().to_string())
        .collect::<Vec<String>>();

    if !privileges.contains(&"login".to_string()) {
        return Err(AuthError::InsufficientPrivileges);
    }

    if let Some(pwd) = password {
        crate::auth::authenticate_with_password(username, pwd)
            .map_err(|_| AuthError::InvalidCredentials)?;
    }

    Ok(privileges)
}

macro_rules! allow_disabled_auth {
    ($maybe_auth_disabled_error:expr, $fallback:expr) => {
        match $maybe_auth_disabled_error {
            Ok(tokens) => Ok(tokens),
            Err(e) => match e {
                AuthError::Disabled => Ok($fallback),
                _ => Err(e),
            },
        }
    };
}

pub(crate) fn http_api_login(username: String, password: String) -> AuthResult<TokenResponse> {
    allow_disabled_auth!(
        as_admin(|| {
            let roles = authenticate_user(&username, Some(password.as_str()))?;

            Ok(TokenResponse {
                auth: create_jwt_token(&username, "auth", Some(roles))?,
                refresh: create_jwt_token(&username, "refresh", None)?,
            })
        }),
        TokenResponse::default()
    )
}

pub(crate) fn http_api_refresh_session(auth_header: String) -> AuthResult<TokenResponse> {
    allow_disabled_auth!(
        as_admin(|| {
            let claims = get_jwt_claims(&auth_header)?;

            if claims.typ != "refresh" {
                return Err(AuthError::InvalidType);
            }

            let user_roles = authenticate_user(&claims.sub, None)?;

            let auth_token = create_jwt_token(&claims.sub, "auth", Some(user_roles))?;
            let refresh_token = create_jwt_token(&claims.sub, "refresh", None)?;

            Ok(TokenResponse {
                auth: auth_token,
                refresh: refresh_token,
            })
        }),
        TokenResponse::default()
    )
}

fn validate_auth(auth_header: &str) -> AuthResult<AuthContext> {
    let claims = get_jwt_claims(auth_header)?;

    if claims.typ != "auth" {
        return Err(AuthError::InvalidType);
    }

    let roles = claims.roles.unwrap_or_default();
    if !roles.contains(&"login".to_string()) {
        return Err(AuthError::InsufficientPrivileges);
    }

    Ok(AuthContext {
        username: claims.sub,
        roles,
    })
}

pub(crate) fn wrap_api_result<T, E>(result: Result<T, E>) -> HttpResponseTable
where
    T: Serialize,
    E: Into<HttpResponse>,
{
    let response: HttpResponse = match result {
        Ok(res) => match serde_json::to_string(&res) {
            Ok(body) => HttpResponse::to_json_response(StatusCode::OK, body),
            Err(err) => HttpResponse::to_json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(r#"{{"error":"{err}","errorMessage":"Internal error"}}"#),
            ),
        },
        Err(err) => err.into(),
    };

    response.into()
}

fn auth_middleware<F, T, E>(auth_header: String, handler: F) -> ApiResult<T>
where
    F: FnOnce() -> Result<T, E>,
    ApiError: From<E>,
{
    allow_disabled_auth!(validate_auth(&auth_header), AuthContext::default())?;
    Ok(handler()?)
}

pub(crate) fn http_api_tiers_with_auth(auth_header: String) -> ApiResult<Vec<TierInfo>> {
    as_admin(|| auth_middleware(auth_header, http_api_tiers))
}

pub(crate) fn http_api_cluster_with_auth(auth_header: String) -> ApiResult<ClusterInfo> {
    as_admin(|| auth_middleware(auth_header, http_api_cluster))
}

pub(crate) fn http_api_memory_with_auth(auth_header: String) -> ApiResult<Vec<TierMemoryInfo>> {
    as_admin(|| auth_middleware(auth_header, http_api_memory))
}

pub(crate) fn http_api_config() -> AuthResult<UiConfig> {
    let is_auth_enabled = get_jwt_secret().is_ok_and(|secret| secret.is_some());

    Ok(UiConfig { is_auth_enabled })
}

/// Liveness probe endpoint - always returns 200 OK
///
/// This endpoint verifies that the Tarantool event loop is running
/// and the HTTP server is responsive.
pub(crate) fn http_api_health_live() -> HealthCheckResult {
    Ok(HealthCheckOk)
}

/// Readiness probe endpoint - returns 200 if instance can serve traffic
///
/// Conditions for 200:
/// - current_state == Online
/// - Raft leader is known
pub(crate) fn http_api_health_ready() -> HealthCheckResult {
    check_instance_ready()?;
    Ok(HealthCheckOk)
}

/// Startup probe endpoint - returns 200 when instance has completed initialization
///
/// Conditions for 200:
/// - current_state == Online
/// - Raft leader is known
/// - Replicaset is in "ready" state (replicas >= replication_factor)
pub(crate) fn http_api_health_startup() -> HealthCheckResult {
    use crate::replicaset::ReplicasetState;

    let node = node::global().map_err(|e| HealthCheckError(format_smolstr!("not inited: {e}")))?;

    // Fast path: startup already complete (latched)
    if node.is_startup_complete() {
        return Ok(HealthCheckOk);
    }

    // Slow path: check common readiness conditions (Online + leader known)
    check_instance_ready()?;

    // Additional startup condition: replicaset must be ready
    let topology = node.topology_cache.get();
    let replicaset = topology
        .try_this_replicaset()
        .ok_or_else(|| HealthCheckError(SmolStr::new_static("replicaset unavailable")))?;

    if replicaset.state != ReplicasetState::Ready {
        return Err(HealthCheckError(format_smolstr!(
            "replicaset state: {}",
            replicaset.state
        )));
    }

    Ok(HealthCheckOk)
}

/// Common health checks for readiness and startup probes.
fn check_instance_ready() -> Result<(), HealthCheckError> {
    let node = node::global().map_err(|e| HealthCheckError(format_smolstr!("not inited: {e}")))?;

    let current_state = node.topology_cache.my_current_state();
    if current_state.variant != StateVariant::Online {
        return Err(HealthCheckError(format_smolstr!(
            "state is {}",
            current_state.variant
        )));
    }

    if node.status().leader_id.is_none() {
        return Err(HealthCheckError(SmolStr::new_static(
            "no Raft leader known",
        )));
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
enum HealthStatusLevel {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RaftStatus {
    state: &'static str,
    term: u64,
    leader_id: u64,
    leader_name: SmolStr,
    applied_index: u64,
    commited_index: u64,
    compacted_index: u64,
    persisted_index: u64,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct BucketStatus {
    active: usize,
    pinned: usize,
    sending: usize,
    receiving: usize,
    garbage: usize,
    total: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterStatus {
    uuid: &'static str,
    version: SmolStr,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HealthStatus {
    status: HealthStatusLevel,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    reasons: Vec<SmolStr>,
    timestamp: u64,
    uptime_seconds: u64,
    name: SmolStr,
    uuid: SmolStr,
    version: &'static str,
    raft_id: u64,
    tier: SmolStr,
    replicaset: SmolStr,
    current_state: &'static str,
    target_state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_state_reason: Option<SmolStr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_state_change_time: Option<SmolStr>,
    limbo_owner: u64,
    raft: RaftStatus,
    buckets: BucketStatus,
    cluster: ClusterStatus,
}

fn get_bucket_status(total: usize) -> Result<BucketStatus, tarantool::error::Error> {
    let space_bucket = Space::find("_bucket")
        .ok_or_else(|| tarantool::error::Error::other("can't access _bucket space"))?;
    let index_status = space_bucket.index("status").ok_or_else(|| {
        tarantool::error::Error::other("space _bucket should have a 'status' index")
    })?;
    let active = index_status.count(IteratorType::Eq, &("active",))?;
    let pinned = index_status.count(IteratorType::Eq, &("pinned",))?;
    let sending = index_status.count(IteratorType::Eq, &("sending",))?;
    let receiving = index_status.count(IteratorType::Eq, &("receiving",))?;
    let garbage = index_status.count(IteratorType::Eq, &("garbage",))?;

    Ok(BucketStatus {
        active,
        pinned,
        sending,
        receiving,
        garbage,
        total,
    })
}

fn get_limbo_owner() -> Result<u64, tlua::LuaError> {
    let lua = tarantool::lua_state();
    let the_box: tlua::LuaTable<_> = lua.get("box").ok_or_else(tlua::WrongType::default)?;
    let info: tlua::Indexable<_> = the_box.try_get("info")?;
    let synchro: tlua::Indexable<_> = info.try_get("synchro")?;
    let queue: tlua::Indexable<_> = synchro.try_get("queue")?;
    queue.try_get("owner")
}

fn determine_health_status(
    current_state: StateVariant,
    target_state: StateVariant,
    leader_id: Option<u64>,
    buckets_sending: usize,
    limbo_owner: u64,
) -> (HealthStatusLevel, Vec<SmolStr>) {
    let mut reasons = Vec::new();
    let mut level = HealthStatusLevel::Healthy;

    // Check degraded conditions first
    if buckets_sending > 0 {
        level = HealthStatusLevel::Degraded;
        reasons.push(SmolStr::new_static("resharding in progress"));
    }

    if current_state == StateVariant::Online && target_state == StateVariant::Offline {
        level = HealthStatusLevel::Degraded;
        reasons.push(format_smolstr!(
            "instance is transitioning Online -> Offline"
        ));
    }

    if limbo_owner != 0 {
        level = HealthStatusLevel::Degraded;
        reasons.push(format_smolstr!(
            "limbo is owned by instance {}",
            limbo_owner
        ));
    }

    // Check unhealthy conditions (always overrides degraded)
    if current_state != StateVariant::Online {
        level = HealthStatusLevel::Unhealthy;
        reasons.push(format_smolstr!("instance state is {}", current_state));
    }

    if leader_id.is_none() {
        level = HealthStatusLevel::Unhealthy;
        reasons.push(SmolStr::new_static("no Raft leader known"));
    }

    (level, reasons)
}

/// Health status endpoint - returns comprehensive health information.
///
/// This endpoint always returns HTTP 200 after startup completion.
/// The actual health status is sent in the response body, allowing
/// monitoring systems to always receive meaningful data.
fn http_api_health_status() -> traft::Result<HealthStatus> {
    // If node is not yet initialized, fail with an error.
    // Once node is available, all topology data should be present.
    let node = node::global()?;
    let storage = Catalog::get();
    let topology = node.topology_cache.get();

    // Use try_* methods to avoid panics if data is not yet available
    // This can happen if health status is queried before instance startup completion
    let instance = topology
        .try_this_instance()
        .ok_or_else(|| traft::error::Error::other("instance info not yet available"))?;
    let tier = topology
        .try_this_tier()
        .ok_or_else(|| traft::error::Error::other("tier info not yet available"))?;

    let raft_status = node.status();
    let raft_storage = &node.raft_storage;

    // Leader ID (0 means leader is unknown)
    let leader_id = raft_status.leader_id.unwrap_or(0);
    let leader_name = if leader_id != 0 {
        topology
            .all_instances()
            .find(|i| i.raft_id == leader_id)
            .map(|i| i.name.0.clone())
            .unwrap_or_default()
    } else {
        SmolStr::default()
    };

    // Track retrieval errors - if any occur, status becomes unhealthy
    // They can only happen under extreme circumstances, e.g., when the storage is corrupted
    // Still, we want to report them in a meaningful and uniform way
    let mut retrieval_errors: Vec<SmolStr> = Vec::new();

    let applied_index = node.get_index();
    let commited_index = raft_storage.commit().unwrap_or_else(|e| {
        retrieval_errors.push(format_smolstr!("failed to get commit index: {e}"));
        0
    });
    let compacted_index = raft_storage.compacted_index().unwrap_or_else(|e| {
        retrieval_errors.push(format_smolstr!("failed to get compacted index: {e}"));
        0
    });
    let persisted_index = raft_storage.last_index().unwrap_or_else(|e| {
        retrieval_errors.push(format_smolstr!("failed to get persisted index: {e}"));
        0
    });

    let uptime_seconds = unsafe { tarantool_uptime() } as u64;

    let cluster_uuid: &'static str = node.topology_cache.cluster_uuid;
    let cluster_version = storage.properties.cluster_version().unwrap_or_else(|e| {
        retrieval_errors.push(format_smolstr!("failed to get cluster version: {e}"));
        SmolStr::default()
    });

    let total_bucket_count = tier.bucket_count as usize;
    let bucket_status = get_bucket_status(total_bucket_count).unwrap_or_else(|e| {
        retrieval_errors.push(format_smolstr!("failed to get bucket status: {e}"));
        BucketStatus {
            total: total_bucket_count,
            ..Default::default()
        }
    });

    let limbo_owner = get_limbo_owner().unwrap_or_else(|e| {
        retrieval_errors.push(format_smolstr!("failed to get limbo owner: {e}"));
        0
    });

    let (mut status_level, mut reasons) = determine_health_status(
        instance.current_state.variant,
        instance.target_state.variant,
        raft_status.leader_id,
        bucket_status.sending,
        limbo_owner,
    );

    if !retrieval_errors.is_empty() {
        status_level = HealthStatusLevel::Unhealthy;
        reasons.extend(retrieval_errors);
    }

    let timestamp = get_unix_timestamp();

    Ok(HealthStatus {
        status: status_level,
        reasons,
        timestamp,
        uptime_seconds,
        name: instance.name.0.clone(),
        uuid: instance.uuid.clone(),
        version: crate::info::PICODATA_VERSION,
        raft_id: instance.raft_id,
        tier: instance.tier.clone(),
        replicaset: instance.replicaset_name.0.clone(),
        current_state: instance.current_state.variant.as_str(),
        target_state: instance.target_state.variant.as_str(),
        target_state_reason: instance.target_state_reason.clone(),
        target_state_change_time: instance.target_state_change_time.map(|dt| dt.to_smolstr()),
        limbo_owner,
        raft: RaftStatus {
            state: raft_status.raft_state.as_str(),
            term: raft_status.term,
            leader_id,
            leader_name,
            applied_index,
            commited_index,
            compacted_index,
            persisted_index,
        },
        buckets: bucket_status,
        cluster: ClusterStatus {
            uuid: cluster_uuid,
            version: cluster_version,
        },
    })
}

pub(crate) fn http_api_health_status_with_auth(auth_header: String) -> ApiResult<HealthStatus> {
    as_admin(|| auth_middleware(auth_header, http_api_health_status))
}
