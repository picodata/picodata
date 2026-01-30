use crate::info::{RuntimeInfo, VersionInfo};
use crate::instance::{Instance, InstanceName, StateVariant};
use crate::replicaset::{Replicaset, ReplicasetName};
use crate::storage::Catalog;
use crate::storage::ToEntryIter as _;
use crate::tier::Tier;
use crate::traft::network::ConnectionPool;
use crate::traft::{node, ConnectionType, Result};
use crate::util::Uppercase;
use crate::{has_states, introspection::Introspection, tlog, unwrap_ok_or};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;
use smol_str::SmolStr;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tarantool::fiber;
use tarantool::fiber::r#async::timeout::IntoTimeout;
use tarantool::session::with_su;

const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const AUTH_TOKEN_EXPIRY_HOURS: u64 = 24;
const REFRESH_TOKEN_EXPIRY_DAYS: u64 = 365;

#[derive(
    Clone, Debug, Default, Eq, Introspection, PartialEq, serde::Deserialize, serde::Serialize,
)]
pub struct HttpsConfig {
    #[introspection(config_default = false)]
    pub enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_file: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_file: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password_file: Option<PathBuf>,
}

impl HttpsConfig {
    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }
}

fn as_admin<T>(f: impl FnOnce() -> T) -> T {
    with_su(1, f).expect("becoming admin should not fail")
}

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    exp: u64,
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
pub(crate) struct ErrorResponse {
    error: String,
    #[serde(rename = "errorMessage")]
    error_message: String,
    #[serde(skip)]
    pub(crate) status: u64,
}

impl std::fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(value) => write!(f, "{}", value),
            Err(e) => write!(f, r#"{{"error":"{}","errorMessage":"Internal error"}}"#, e),
        }
    }
}

impl From<AuthError> for ErrorResponse {
    fn from(value: AuthError) -> Self {
        let error_message = value.error_msg();
        let error = value.error_type();
        let status = value.status_code();
        Self {
            error,
            error_message,
            status,
        }
    }
}

impl From<crate::traft::error::Error> for ErrorResponse {
    fn from(value: crate::traft::error::Error) -> Self {
        Self {
            error: String::from("error"),
            error_message: value.to_string(),
            status: 500,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct AuthContext {
    pub username: String,
    pub roles: Vec<String>,
}

pub(crate) enum AuthError {
    Inner(String),
    Disabled,
    InvalidHeader,
    InvalidType,
    Expired,
    InvalidCredentials,
    InsufficientPrivileges,
    JWTEncoding(String),
    JWTValidation(String),
    UserLookup(String),
    PrivilegesLookup(String),
}

impl AuthError {
    fn error_type(&self) -> String {
        String::from(match self {
            AuthError::Inner(..) => "error",
            AuthError::InvalidCredentials => "wrongCredentials",
            AuthError::Expired => "sessionExpired",
            AuthError::Disabled => "authDisabled",
            _ => "authError",
        })
    }
    fn status_code(&self) -> u64 {
        match self {
            AuthError::Inner(_) => 500,
            AuthError::InsufficientPrivileges | AuthError::PrivilegesLookup(_) => 403,
            _ => 401,
        }
    }

    fn error_msg(&self) -> String {
        match self {
            AuthError::Disabled => String::from("auth disabled"),
            AuthError::InvalidHeader => String::from("invalid authorization header"),
            AuthError::InvalidType => String::from("invalid token type"),
            AuthError::Expired => String::from("token expired"),
            AuthError::InsufficientPrivileges => String::from("insufficient privileges"),
            AuthError::JWTEncoding(v) => format!("failed to encode jwt: {}", v),
            AuthError::JWTValidation(v) => format!("invalid jwt: {}", v),
            AuthError::UserLookup(v) => format!("failed to find user jwt: {}", v),
            AuthError::PrivilegesLookup(v) => format!("failed to find privileges: {}", v),
            AuthError::InvalidCredentials => String::from("invalid credentials"),
            AuthError::Inner(v) => v.to_owned(),
        }
    }
}

type AuthResult<T> = std::result::Result<T, AuthError>;

/// Response from instances:
/// - `raft_id`: instance raft_id to find Instance to store data
/// - `httpd_address`: host:port of hhtpd on instance if exists or empty string
/// - `version`: picodata version or empty string
/// - `mem_usable`: quota_size from box.slab.info
/// - `mem_used`: quota_used from box.slab.info
#[derive(Deserialize)]
struct InstanceDataResponse {
    httpd_address: SmolStr,
    version: SmolStr,
    mem_usable: u64,
    mem_used: u64,
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
    state: StateVariant,
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

fn get_replicasets(storage: &Catalog) -> Result<HashMap<ReplicasetName, Replicaset>> {
    let i = storage.replicasets.iter()?;
    Ok(i.map(|item| (item.name.clone(), item)).collect())
}

fn get_peer_addresses(
    storage: &Catalog,
    replicasets: &HashMap<ReplicasetName, Replicaset>,
    instances: &[Instance],
    only_leaders: bool, // get data from leaders only or from all instances
) -> Result<HashMap<u64, (SmolStr, SmolStr)>> {
    let leaders: HashMap<_, _> = instances
        .iter()
        .filter(|item| {
            !only_leaders
                || replicasets
                    .get(&item.replicaset_name)
                    .is_some_and(|r| r.current_master_name == item.name)
        })
        .map(|item| (item.raft_id, true))
        .collect();
    let i = storage.peer_addresses.iter()?.filter(|peer| {
        peer.connection_type == ConnectionType::Iproto
            || peer.connection_type == ConnectionType::Pgproto
    });
    Ok(i.filter(|pa| leaders.get(&pa.raft_id) == Some(&true)).fold(
        HashMap::new(),
        |mut acc, pa| {
            acc.entry(pa.raft_id)
                .and_modify(|(iproto, pgproto)| {
                    // Destructure the tuple for clarity
                    match pa.connection_type {
                        ConnectionType::Iproto => *iproto = pa.address.clone(),
                        ConnectionType::Pgproto => *pgproto = pa.address.clone(),
                    }
                })
                .or_insert_with(|| {
                    // Use or_insert_with for lazy evaluation
                    match pa.connection_type {
                        ConnectionType::Iproto => (pa.address, "".into()),
                        ConnectionType::Pgproto => ("".into(), pa.address),
                    }
                });
            acc // Only inserts if key doesn't exist
        },
    ))
}

// Get data from instances: memory, PICO_VERSION, httpd address if exists
//
async fn get_instances_data(
    pool: &ConnectionPool,
    instances: &[Instance],
) -> HashMap<u64, InstanceDataResponse> {
    let mut fs = vec![];
    for instance in instances {
        if has_states!(instance, Expelled -> *) {
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
                    httpd_address: Default::default(),
                    version: Default::default(),
                    mem_usable: 0u64,
                    mem_used: 0u64,
                };
                match future.await {
                    Ok(info) => {
                        let info: RuntimeInfo = info;
                        if let Some(http) = info.http {
                            data.httpd_address = format_smolstr!("{}:{}", &http.host, &http.port);
                        }
                        data.version = info.version_info.picodata_version.clone();
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
fn get_replicasets_info(storage: &Catalog, only_leaders: bool) -> Result<Vec<ReplicasetInfo>> {
    let node = crate::traft::node::global()?;
    let instances = storage.instances.all_instances()?;
    let instances_props = fiber::block_on(get_instances_data(&node.pool, &instances));
    let replicasets = get_replicasets(storage)?;
    let addresses = get_peer_addresses(storage, &replicasets, &instances, only_leaders)?;

    let mut res: HashMap<ReplicasetName, ReplicasetInfo> =
        HashMap::with_capacity(replicasets.len());

    for instance in instances {
        let address = addresses
            .get(&instance.raft_id)
            .cloned()
            .unwrap_or_default();

        let replicaset_name = instance.replicaset_name;
        let mut is_leader = false;
        let mut replicaset_uuid = SmolStr::default();
        let mut tier = instance.tier.clone();
        if let Some(replicaset) = replicasets.get(&replicaset_name) {
            is_leader = replicaset.current_master_name == instance.name;
            replicaset_uuid = replicaset.uuid.clone();
            debug_assert_eq!(replicaset.tier, instance.tier);
            tier.clone_from(&replicaset.tier);
        }

        let mut http_address = SmolStr::default();
        let mut version = SmolStr::default();
        let mut mem_usable: u64 = 0u64;
        let mut mem_used: u64 = 0u64;
        if let Some(data) = instances_props.get(&instance.raft_id) {
            http_address = data.httpd_address.clone();
            version = data.version.clone();
            mem_usable = data.mem_usable;
            mem_used = data.mem_used;
        }

        let instance_info = InstanceInfo {
            http_address,
            version,
            failure_domain: instance.failure_domain.data,
            is_leader,
            current_state: instance.current_state.variant,
            target_state: instance.target_state.variant,
            name: instance.name.clone(),
            binary_address: address.0,
            pg_address: address.1,
        };

        let replicaset_info = res
            .entry(replicaset_name)
            .or_insert_with_key(|replicaset_name| ReplicasetInfo {
                version: instance_info.version.clone(),
                state: instance_info.current_state,
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

pub(crate) fn http_api_cluster() -> Result<ClusterInfo> {
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

pub(crate) fn http_api_tiers() -> Result<Vec<TierInfo>> {
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

fn get_jwt_secret() -> AuthResult<Option<String>> {
    let node = crate::traft::node::global().map_err(|e| AuthError::Inner(e.to_string()))?;
    let secret = node.alter_system_parameters.borrow().jwt_secret.clone();

    // Given successful db connection, secret cannot technically be None at this point,
    // as it is set to empty string to disable auth instead of NULL
    if secret.is_empty() {
        Ok(None)
    } else {
        Ok(Some(secret))
    }
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
    let Some(secret) = get_jwt_secret()? else {
        return Err(AuthError::Disabled);
    };

    let expiry = match token_type {
        "auth" => get_unix_timestamp() + (AUTH_TOKEN_EXPIRY_HOURS * 3600),
        "refresh" => get_unix_timestamp() + (REFRESH_TOKEN_EXPIRY_DAYS * 24 * 3600),
        _ => return Err(AuthError::InvalidType),
    };

    let claims = JwtClaims {
        sub: username.to_string(),
        exp: expiry,
        typ: token_type.to_string(),
        roles,
    };

    let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    jsonwebtoken::encode(
        &header,
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(secret.as_ref()),
    )
    .map_err(|e| AuthError::JWTEncoding(e.to_string()))
}

fn get_jwt_claims(auth_header: &str) -> AuthResult<JwtClaims> {
    let Some(secret) = get_jwt_secret()? else {
        return Err(AuthError::Disabled);
    };

    let token = auth_header
        .strip_prefix("Bearer ")
        .ok_or(AuthError::InvalidHeader)?;

    let validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    jsonwebtoken::decode::<JwtClaims>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(secret.as_ref()),
        &validation,
    )
    .map(|token_data| token_data.claims)
    .map_err(|e| AuthError::JWTValidation(e.to_string()))
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

            if claims.exp < get_unix_timestamp() {
                return Err(AuthError::Expired);
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

pub(crate) fn validate_auth(auth_header: &str) -> AuthResult<AuthContext> {
    let claims = get_jwt_claims(auth_header)?;

    if claims.typ != "auth" {
        return Err(AuthError::InvalidType);
    }

    if claims.exp < get_unix_timestamp() {
        return Err(AuthError::Expired);
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

macro_rules! wrap_api_result {
    ($api_result:expr) => {{
        let mut status = 200;
        let content_type = "application/json";
        let content: String;
        match $api_result {
            Ok(res) => match serde_json::to_string(&res) {
                Ok(value) => content = value,
                Err(err) => {
                    content = format!(r#"{{"error":"{err}","errorMessage":"Internal error"}}"#);
                    status = 500
                }
            },
            Err(err) => {
                let error_resp = crate::http_server::ErrorResponse::from(err);
                status = error_resp.status;
                content = error_resp.to_string();
            }
        }
        tlua::AsTable((
            ("status", status),
            ("body", content),
            ("headers", tlua::AsTable((("content-type", content_type),))),
        ))
    }};
}

pub(crate) use wrap_api_result;

pub(crate) fn auth_middleware<F, T>(auth_header: String, handler: F) -> AuthResult<T>
where
    F: FnOnce() -> Result<T>,
    T: serde::Serialize,
{
    allow_disabled_auth!(validate_auth(&auth_header), AuthContext::default())?;

    handler().map_err(|x| AuthError::Inner(x.to_string()))
}

pub(crate) fn http_api_tiers_with_auth(auth_header: String) -> AuthResult<Vec<TierInfo>> {
    as_admin(|| auth_middleware(auth_header, http_api_tiers))
}

pub(crate) fn http_api_cluster_with_auth(auth_header: String) -> AuthResult<ClusterInfo> {
    as_admin(|| auth_middleware(auth_header, http_api_cluster))
}

pub(crate) fn http_api_config() -> AuthResult<UiConfig> {
    let is_auth_enabled = get_jwt_secret().is_ok_and(|secret| secret.is_some());

    Ok(UiConfig { is_auth_enabled })
}
