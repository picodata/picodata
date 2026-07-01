use std::time::{SystemTime, UNIX_EPOCH};

use super::auth::auth_middleware;
use super::{as_admin, ApiResult, HttpResponse};
use crate::instance::StateVariant;
use crate::traft::{self, node, Result};
use raft::Storage as _;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use smol_str::{format_smolstr, ToSmolStr};
use tarantool::index::IteratorType;
use tarantool::space::Space;
use tarantool::tlua::{self, Index as _};

extern "C" {
    fn tarantool_uptime() -> f64;
}

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

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum HealthStatusLevel {
    Healthy,
    Degraded,
    Broken,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RaftStatus {
    state: SmolStr,
    term: u64,
    leader_id: u64,
    leader_name: SmolStr,
    applied_index: u64,
    commited_index: u64,
    compacted_index: u64,
    persisted_index: u64,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct BucketStatus {
    active: usize,
    pinned: usize,
    sending: usize,
    receiving: usize,
    garbage: usize,
    total: usize,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterStatus {
    uuid: SmolStr,
    version: SmolStr,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HealthStatus {
    status: HealthStatusLevel,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    issues: Vec<SmolStr>,
    timestamp: u64,
    uptime_seconds: u64,
    name: SmolStr,
    uuid: SmolStr,
    version: SmolStr,
    raft_id: u64,
    tier: SmolStr,
    replicaset: SmolStr,
    current_state: SmolStr,
    target_state: SmolStr,
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
    synchronous_replication_enabled: bool,
) -> (HealthStatusLevel, Vec<SmolStr>) {
    let mut issues = Vec::new();
    let mut level = HealthStatusLevel::Healthy;

    // Check degraded conditions first
    if buckets_sending > 0 {
        level = HealthStatusLevel::Degraded;
        issues.push(SmolStr::new_static("resharding in progress"));
    }

    if current_state == StateVariant::Online && target_state == StateVariant::Offline {
        level = HealthStatusLevel::Degraded;
        issues.push(format_smolstr!(
            "instance is transitioning Online -> Offline"
        ));
    }

    if limbo_owner != 0 && !synchronous_replication_enabled {
        level = HealthStatusLevel::Degraded;
        issues.push(format_smolstr!(
            "limbo is owned by instance {limbo_owner} but synchronous replication is disabled",
        ));
    }

    // Check unhealthy conditions (always overrides degraded)
    if current_state != StateVariant::Online {
        level = HealthStatusLevel::Broken;
        issues.push(format_smolstr!("instance state is {}", current_state));
    }

    if leader_id.is_none() {
        level = HealthStatusLevel::Broken;
        issues.push(SmolStr::new_static("no Raft leader known"));
    }

    (level, issues)
}

fn get_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Health status endpoint - returns comprehensive health information.
///
/// This endpoint always returns HTTP 200 after startup completion.
/// The actual health status is sent in the response body, allowing
/// monitoring systems to always receive meaningful data.
pub(crate) fn http_api_health_status() -> traft::Result<HealthStatus> {
    // If node is not yet initialized, fail with an error.
    // Once node is available, all topology data should be present.
    let node = node::global()?;
    let storage = crate::storage::Catalog::get();
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
    let synchronous_replication_enabled = node
        .alter_system_parameters
        .borrow()
        .is_synchronous_replication();

    let (mut status_level, mut issues) = determine_health_status(
        instance.current_state.variant,
        instance.target_state.variant,
        raft_status.leader_id,
        bucket_status.sending,
        limbo_owner,
        synchronous_replication_enabled,
    );

    if !retrieval_errors.is_empty() {
        status_level = HealthStatusLevel::Broken;
        issues.extend(retrieval_errors);
    }

    let timestamp = get_unix_timestamp();

    Ok(HealthStatus {
        status: status_level,
        issues,
        timestamp,
        uptime_seconds,
        name: instance.name.0.clone(),
        uuid: instance.uuid.clone(),
        version: crate::info::PICODATA_VERSION.into(),
        raft_id: instance.raft_id,
        tier: instance.tier.clone(),
        replicaset: instance.replicaset_name.0.clone(),
        current_state: instance.current_state.variant.to_smolstr(),
        target_state: instance.target_state.variant.to_smolstr(),
        target_state_reason: instance.target_state_reason.clone(),
        target_state_change_time: instance.target_state_change_time.map(|dt| dt.to_smolstr()),
        limbo_owner,
        raft: RaftStatus {
            state: raft_status.raft_state.to_smolstr(),
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
            uuid: cluster_uuid.to_smolstr(),
            version: cluster_version,
        },
    })
}

pub(crate) fn http_api_health_status_with_auth(auth_header: String) -> ApiResult<HealthStatus> {
    as_admin(|| auth_middleware(auth_header, http_api_health_status))
}
