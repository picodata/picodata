use super::auth::auth_middleware;
use super::{as_admin, ApiResult, DEFAULT_TIMEOUT};
use crate::info::{RuntimeInfo, VersionInfo};
use crate::instance::{Instance, InstanceName, StateVariant};
use crate::replicaset::{Replicaset, ReplicasetName, ReplicasetState};
use crate::storage::Catalog;
use crate::storage::ToEntryIter as _;
use crate::tier::Tier;
use crate::traft::network::ConnectionPool;
use crate::traft::{self, node};
use crate::util::Uppercase;
use crate::{has_states, tlog, unwrap_ok_or};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::collections::{BTreeSet, HashMap, HashSet};
use tarantool::fiber;
use tarantool::fiber::r#async::timeout::IntoTimeout;

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
    uuid: SmolStr,
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
            uuid: instance.uuid.clone(),
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

pub(crate) fn http_api_tiers_with_auth(auth_header: String) -> ApiResult<Vec<TierInfo>> {
    as_admin(|| auth_middleware(auth_header, http_api_tiers))
}

pub(crate) fn http_api_cluster_with_auth(auth_header: String) -> ApiResult<ClusterInfo> {
    as_admin(|| auth_middleware(auth_header, http_api_cluster))
}

pub(crate) fn http_api_memory_with_auth(auth_header: String) -> ApiResult<Vec<TierMemoryInfo>> {
    as_admin(|| auth_middleware(auth_header, http_api_memory))
}
