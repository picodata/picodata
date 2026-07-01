use super::auth::auth_middleware;
use super::{as_admin, ApiError, ApiResult, DEFAULT_TIMEOUT};
use crate::instance::StateVariant;
use crate::traft::{self, node};
use serde::Serialize;
use smol_str::SmolStr;
use std::collections::HashMap;
use tarantool::fiber;

////////////////////////////////////////////////////////////////////////////////
// /api/v1/instance/:uuid
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InstanceResponse {
    uuid: SmolStr,
    name: SmolStr,
    raft_id: u64,
    replicaset_name: SmolStr,
    replicaset_uuid: SmolStr,
    current_state: StateVariant,
    target_state: StateVariant,
    tier: SmolStr,
    picodata_version: SmolStr,
    instance_dir: String,
    backup_dir: String,
    admin_socket: String,
    share_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    audit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    log: Option<crate::info::LogDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    vinyl: Option<crate::info::VinylDetails>,
    replication: HashMap<u64, crate::info::ReplicationDetails>,
}

fn http_api_instance_detail(uuid: String) -> ApiResult<InstanceResponse> {
    let node = node::global().map_err(traft::error::Error::from)?;
    let my_uuid = node.topology_cache.my_instance_uuid();

    let topology = node.topology_cache.get();
    let instance = match topology.instance_by_uuid(&uuid) {
        Ok(i) => i.clone(),
        Err(traft::error::Error::NoSuchInstance(_)) => {
            return Err(ApiError::NotFound(format!(
                "instance with uuid '{uuid}' not found"
            )));
        }
        Err(e) => return Err(ApiError::Internal(e)),
    };
    // drop before yield point to avoid NoYieldRef crashes
    drop(topology);

    let is_local = uuid == my_uuid;

    let info = if is_local {
        crate::info::impl_proc_instance_details()?
    } else {
        let future = async {
            node.pool
                .call_raw::<(), crate::info::InstanceDetails>(
                    &instance.name,
                    ".proc_instance_details",
                    &(),
                    DEFAULT_TIMEOUT,
                )?
                .await
        };
        fiber::block_on(future)?
    };

    Ok(InstanceResponse {
        uuid: instance.uuid.clone(),
        name: instance.name.0.clone(),
        raft_id: instance.raft_id,
        replicaset_name: instance.replicaset_name.0.clone(),
        replicaset_uuid: instance.replicaset_uuid.clone(),
        current_state: instance.current_state.variant,
        target_state: instance.target_state.variant,
        tier: instance.tier.clone(),
        picodata_version: instance.picodata_version.clone(),
        instance_dir: info.instance_dir,
        backup_dir: info.backup_dir,
        admin_socket: info.admin_socket,
        share_dir: info.share_dir,
        audit: info.audit,
        log: info.log.into_option(),
        vinyl: info.vinyl.into_option(),
        replication: info.replication,
    })
}

pub(crate) fn http_api_instance_detail_with_auth(
    auth_header: String,
    uuid: String,
) -> ApiResult<InstanceResponse> {
    as_admin(|| auth_middleware(auth_header, || http_api_instance_detail(uuid)))
}
