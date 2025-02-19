use crate::info::{RuntimeInfo, VersionInfo};
use crate::instance::{Instance, InstanceName, StateVariant};
use crate::replicaset::{Replicaset, ReplicasetName};
use crate::storage::Clusterwide;
use crate::storage::ToEntryIter as _;
use crate::tier::Tier;
use crate::traft::network::ConnectionPool;
use crate::traft::{ConnectionType, Result};
use crate::util::Uppercase;
use crate::{has_states, tlog, unwrap_ok_or};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tarantool::fiber;
use tarantool::fiber::r#async::timeout::IntoTimeout;

const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// Response from instances:
/// - `raft_id`: instance raft_id to find Instance to store data
/// - `httpd_address`: host:port of hhtpd on instance if exists or empty string
/// - `version`: picodata version or empty string
/// - `mem_usable`: quota_size from box.slab.info
/// - `mem_used`: quota_used from box.slab.info
#[derive(Deserialize)]
struct InstanceDataResponse {
    httpd_address: String,
    version: String,
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
    http_address: String,
    version: String,
    failure_domain: HashMap<Uppercase, Uppercase>,
    is_leader: bool,
    current_state: StateVariant,
    target_state: StateVariant,
    name: InstanceName,
    binary_address: String,
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
    version: String,
    state: StateVariant,
    instance_count: usize,
    uuid: String,
    instances: Vec<InstanceInfo>,
    capacity_usage: f64,
    memory: MemoryInfo,
    name: ReplicasetName,
    #[serde(skip)]
    tier: String,
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
    name: String,
    services: Vec<String>,
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
    #[serde(rename = "currentInstaceVersion")] // for compatibility with lua version
    current_instance_version: String,
    replicasets_count: usize,
    instances_current_state_offline: usize,
    memory: MemoryInfo,
    instances_current_state_online: usize,
    // list of serialized plugin identifiers - "<plugin_name> <plugin_version>"
    plugins: Vec<String>,
}

fn get_replicasets(storage: &Clusterwide) -> Result<HashMap<ReplicasetName, Replicaset>> {
    let i = storage.replicasets.iter()?;
    Ok(i.map(|item| (item.name.clone(), item)).collect())
}

fn get_peer_addresses(
    storage: &Clusterwide,
    replicasets: &HashMap<ReplicasetName, Replicaset>,
    instances: &[Instance],
    only_leaders: bool, // get data from leaders only or from all instances
) -> Result<HashMap<u64, String>> {
    let leaders: HashMap<u64, bool> = instances
        .iter()
        .filter(|item| {
            !only_leaders
                || replicasets
                    .get(&item.replicaset_name)
                    .is_some_and(|r| r.current_master_name == item.name)
        })
        .map(|item| (item.raft_id, true))
        .collect();
    let i = storage
        .peer_addresses
        .iter()?
        .filter(|peer| peer.connection_type == ConnectionType::Iproto);
    Ok(i.filter(|pa| leaders.get(&pa.raft_id) == Some(&true))
        .map(|pa| (pa.raft_id, pa.address))
        .collect())
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

        let res = pool.call_raw(&instance.name, ".proc_runtime_info", &(), DEFAULT_TIMEOUT);
        let future = unwrap_ok_or!(res,
            Err(e) => {
                tlog!(Error, "webui: error on calling .proc_runtime_info on instance {}: {e}", instance.name);
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
                    httpd_address: String::new(),
                    version: String::new(),
                    mem_usable: 0u64,
                    mem_used: 0u64,
                };
                match future.await {
                    Ok(info) => {
                        let info: RuntimeInfo = info;
                        if let Some(http) = info.http {
                            data.httpd_address = format!("{}:{}", &http.host, &http.port);
                        }
                        data.version = info.version_info.picodata_version.to_string();
                        data.mem_usable = info.slab_info.quota_size;
                        data.mem_used = info.slab_info.quota_used;
                    }
                    Err(e) => {
                        tlog!(
                            Error,
                            "webui: error on calling .proc_runtime_info on instance {}: {e}",
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
fn get_replicasets_info(storage: &Clusterwide, only_leaders: bool) -> Result<Vec<ReplicasetInfo>> {
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
        let mut replicaset_uuid = String::new();
        let mut tier = instance.tier.clone();
        if let Some(replicaset) = replicasets.get(&replicaset_name) {
            is_leader = replicaset.current_master_name == instance.name;
            replicaset_uuid.clone_from(&replicaset.uuid);
            debug_assert_eq!(replicaset.tier, instance.tier);
            tier.clone_from(&replicaset.tier);
        }

        let mut http_address = String::new();
        let mut version = String::new();
        let mut mem_usable: u64 = 0u64;
        let mut mem_used: u64 = 0u64;
        if let Some(data) = instances_props.get(&instance.raft_id) {
            http_address.clone_from(&data.httpd_address);
            version.clone_from(&data.version);
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
            binary_address: address,
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
            replicaset_info.capacity_usage = if mem_usable == 0 {
                0_f64
            } else {
                ((mem_used as f64) / (mem_usable as f64) * 10000_f64).round() / 100_f64
            };
            replicaset_info.memory.usable = mem_usable;
            replicaset_info.memory.used = mem_used;
        }
        replicaset_info.instances.push(instance_info);
        replicaset_info.instance_count += 1;
    }

    Ok(res.into_values().collect())
}

pub(crate) fn http_api_cluster() -> Result<ClusterInfo> {
    let version = String::from(VersionInfo::current().picodata_version);

    let storage = Clusterwide::get();
    let replicasets = get_replicasets_info(storage, true)?;

    let plugins = storage
        .plugins
        .get_all()
        .expect("storage shouldn't fail")
        .iter()
        .map(|plugin| [plugin.name.clone(), plugin.version.clone()].join(" "))
        .collect();

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

    let res = ClusterInfo {
        capacity_usage: if mem_info.usable == 0 {
            0_f64
        } else {
            ((mem_info.used as f64) / (mem_info.usable as f64) * 10000_f64).round() / 100_f64
        },
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
    let storage = Clusterwide::get();
    let replicasets = get_replicasets_info(storage, false)?;
    let tiers = storage.tiers.iter()?;

    let mut res: HashMap<String, TierInfo> = tiers
        .map(|item: Tier| {
            (
                item.name.clone(),
                TierInfo {
                    replicasets: Vec::new(),
                    replicaset_count: 0,
                    rf: item.replication_factor,
                    bucket_count: item.bucket_count,
                    instance_count: 0,
                    can_vote: true,
                    name: item.name.clone(),
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
        tier.replicasets.push(replicaset);
    }

    Ok(res.into_values().collect())
}

macro_rules! wrap_api_result {
    ($api_result:expr) => {{
        let mut status = 200;
        let mut content_type = "application/json";
        let content: String;
        match $api_result {
            Ok(res) => match serde_json::to_string(&res) {
                Ok(value) => content = value,
                Err(err) => {
                    content = err.to_string();
                    content_type = "plain/text";
                    status = 500
                }
            },
            Err(err) => {
                content = err.to_string();
                content_type = "plain/text";
                status = 500
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
