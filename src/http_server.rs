use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error, fmt};

use ::tarantool::net_box::{Conn, ConnOptions, Options};
use ::tarantool::tuple::Tuple;

use crate::info::VersionInfo;
use crate::instance::{GradeVariant, Instance, InstanceId};
use crate::replicaset::{Replicaset, ReplicasetId};
use crate::storage::Clusterwide;
use crate::storage::ToEntryIter as _;
use crate::tier::Tier;
use crate::tlog;
use crate::util::Uppercase;

const DEFAULT_TIMEOUT: u64 = 60;
const PICO_USER: &'static str = "pico_service";

#[derive(Debug, Serialize)]
struct HttpError(String);

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for HttpError {}

/// Struct for box.slab.info() result
#[derive(Deserialize)]
struct SlabInfo {
    #[allow(dead_code)]
    items_size: u64,
    #[allow(dead_code)]
    items_used_ratio: String,
    quota_size: u64,
    #[allow(dead_code)]
    quota_used_ratio: String,
    #[allow(dead_code)]
    arena_used_ratio: String,
    #[allow(dead_code)]
    items_used: u64,
    quota_used: u64,
    #[allow(dead_code)]
    arena_size: u64,
    #[allow(dead_code)]
    arena_used: u64,
}

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
//     targetGrade: string;
//     currentGrade: string;
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
    current_grade: GradeVariant,
    target_grade: GradeVariant,
    name: InstanceId,
    binary_address: String,
}

// From the Lua version:
//
// --[[
// export interface ReplicasetType {
//     id: string;
//     instanceCount: number;
//     instances: InstanceType[];
//     version: string;
//     grade: string;
//     capacity: number;
// }
// ]]
//
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReplicasetInfo {
    version: String,
    grade: GradeVariant,
    instance_count: usize,
    uuid: String,
    instances: Vec<InstanceInfo>,
    capacity_usage: f64,
    memory: MemoryInfo,
    id: ReplicasetId,
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
    instance_count: usize,
    #[serde(rename = "can_vote")] // for compatibility with lua version
    can_vote: bool,
    name: String,
    plugins: Vec<String>,
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
// --     instancesCurrentGradeOnline: number;
// --     instancesCurrentGradeOffline: number;
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
    instances_current_grade_offline: usize,
    memory: MemoryInfo,
    instances_current_grade_online: usize,
}

fn get_replicasets(
    storage: &Clusterwide,
) -> Result<HashMap<ReplicasetId, Replicaset>, Box<dyn Error>> {
    let i = storage.replicasets.iter()?;
    Ok(i.map(|item| (item.replicaset_id.clone(), item)).collect())
}

fn get_instances(storage: &Clusterwide) -> Result<Vec<Instance>, Box<dyn Error>> {
    storage
        .instances
        .all_instances()
        .or(Err(Box::new(HttpError("failed to get".into()))))
}

fn get_tiers(storage: &Clusterwide) -> Result<Vec<Tier>, Box<dyn Error>> {
    let i = storage.tiers.iter()?;
    Ok(i.collect())
}

fn get_peer_addresses(
    storage: &Clusterwide,
    replicasets: &HashMap<ReplicasetId, Replicaset>,
    instances: &Vec<Instance>,
    only_leaders: bool, // get data from leaders only or from all instances
) -> Result<HashMap<u64, String>, Box<dyn Error>> {
    let leaders: HashMap<u64, bool> = instances
        .iter()
        .filter(|item| {
            !only_leaders
                || replicasets
                    .get(&item.replicaset_id)
                    .is_some_and(|r| r.current_master_id == item.instance_id)
        })
        .map(|item| (item.raft_id, true))
        .collect();
    let i = storage.peer_addresses.iter()?;
    Ok(i.filter(|pa| leaders.get(&pa.raft_id) == Some(&true))
        .map(|pa| (pa.raft_id, pa.address))
        .collect())
}

// Get data from instance: memory, PICO_VERSION, httpd address if exists
//
fn get_instance_data(full_address: &String) -> InstanceDataResponse {
    let mut res = InstanceDataResponse {
        httpd_address: String::new(),
        version: String::new(),
        mem_usable: 0u64,
        mem_used: 0u64,
    };
    match Conn::new(
        full_address,
        ConnOptions {
            user: PICO_USER.to_string(),
            ..ConnOptions::default()
        },
        None,
    ) {
        Ok(conn) => {
            match conn.call(
                ".proc_runtime_info",
                &(),
                &Options {
                    timeout: Some(std::time::Duration::from_secs(DEFAULT_TIMEOUT)),
                    ..Options::default()
                },
            ) {
                Ok(info_res) => {
                    if let Ok(info_data) = info_res
                        .as_ref()
                        .map(Tuple::decode::<(crate::info::RuntimeInfo,)>)
                        .transpose()
                    {
                        if let Some((ri,)) = info_data {
                            if let Some(http) = ri.http {
                                res.httpd_address.push_str(&http.host);
                                res.httpd_address.push_str(&String::from(":"));
                                res.httpd_address.push_str(&http.port.to_string());
                            }
                            res.version = ri.version_info.picodata_version.to_string();
                        }
                    }
                }
                Err(e) => tlog!(
                    Error,
                    "webui: error on calling .proc_runtime_info on {full_address}: {e}"
                ),
            }
            match conn.call(
                "box.slab.info",
                &(),
                &Options {
                    timeout: Some(std::time::Duration::from_secs(DEFAULT_TIMEOUT)),
                    ..Options::default()
                },
            ) {
                Ok(slab_res) => {
                    if let Ok(slab_data) = slab_res
                        .as_ref()
                        .map(Tuple::decode::<(SlabInfo,)>)
                        .transpose()
                    {
                        if let Some((si,)) = slab_data {
                            res.mem_usable = si.quota_size;
                            res.mem_used = si.quota_used;
                        }
                    };
                }
                Err(e) => tlog!(
                    Error,
                    "webui: error on calling box.slab.info on {full_address}: {e}"
                ),
            }
        }
        Err(e) => tlog!(Error, "webui: can't connect with {full_address}: {e}"),
    }
    return res;
}

// Collect detailed information from replicasets and instances
//
fn get_replicasets_info(
    storage: &Clusterwide,
    only_leaders: bool,
) -> Result<Vec<ReplicasetInfo>, Box<dyn Error>> {
    let instances = get_instances(storage)?;
    let replicasets = get_replicasets(storage)?;
    let addresses = get_peer_addresses(storage, &replicasets, &instances, only_leaders)?;
    let instances_props: HashMap<u64, InstanceDataResponse> = addresses
        .iter()
        .map(|(raft_id, full_address)| (raft_id.clone(), get_instance_data(full_address)))
        .collect();

    let mut res: HashMap<ReplicasetId, ReplicasetInfo> = HashMap::with_capacity(replicasets.len());

    for instance in instances {
        let address = addresses.get(&instance.raft_id).cloned().unwrap();
        let replicaset_id = instance.replicaset_id;
        let replicaset = replicasets.get(&replicaset_id).unwrap();
        let mut http_address = String::new();
        let mut version = String::new();
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
            is_leader: instance.instance_id == replicaset.current_master_id,
            current_grade: instance.current_grade.variant,
            target_grade: instance.target_grade.variant,
            name: instance.instance_id.clone(),
            binary_address: address,
        };

        let mut replica = res.get(&replicaset_id).cloned().unwrap_or(ReplicasetInfo {
            version: instance_info.version.clone(),
            grade: instance_info.current_grade,
            instance_count: 0,
            uuid: replicaset.replicaset_uuid.clone(),
            capacity_usage: 0.0,
            instances: Vec::new(),
            memory: MemoryInfo { usable: 0, used: 0 },
            id: replicaset_id.clone(),
            tier: replicaset.tier.to_string(),
        });

        if &instance.instance_id == &replicaset.current_master_id {
            replica.capacity_usage = if mem_usable == 0 {
                0.0
            } else {
                ((mem_used as f64) / (mem_usable as f64) * 10000_f64).round() / 100_f64
            };
            replica.memory.usable = mem_usable;
            replica.memory.used = mem_used;
        }
        replica.instances.push(instance_info);
        replica.instance_count = replica.instance_count + 1;

        res.insert(replicaset_id, replica);
    }

    Ok(res.values().cloned().collect())
}

pub(crate) fn http_api_cluster() -> Result<ClusterInfo, Box<dyn Error>> {
    let version = String::from(VersionInfo::current().picodata_version);

    let storage = Clusterwide::get();
    let replicasets = get_replicasets_info(storage, true)?;

    let mut instances = 0;
    let mut instances_online = 0;
    let mut replicasets_count = 0;
    let mut mem_info = MemoryInfo { usable: 0, used: 0 };

    for replicaset in replicasets {
        replicasets_count = replicasets_count + 1;
        instances = instances + replicaset.instance_count;
        mem_info.usable = mem_info.usable + replicaset.memory.usable;
        mem_info.used = mem_info.used + replicaset.memory.used;
        _ = replicaset.instances.iter().for_each(|i| {
            instances_online = instances_online
                + if i.current_grade == GradeVariant::Online {
                    1
                } else {
                    0
                }
        })
    }

    let res = ClusterInfo {
        capacity_usage: if mem_info.usable == 0 {
            0.0
        } else {
            ((mem_info.used as f64) / (mem_info.usable as f64) * 10000_f64).round() / 100_f64
        },
        current_instance_version: version,
        replicasets_count: replicasets_count,
        instances_current_grade_offline: (instances - instances_online),
        memory: mem_info,
        instances_current_grade_online: instances_online,
    };

    Ok(res)
}

pub(crate) fn http_api_tiers() -> Result<Vec<TierInfo>, Box<dyn Error>> {
    let storage = Clusterwide::get();
    let replicasets = get_replicasets_info(storage, false)?;
    let tiers = get_tiers(storage)?;

    let mut res: HashMap<String, TierInfo> = tiers
        .iter()
        .map(|item: &Tier| {
            (
                item.name.clone(),
                TierInfo {
                    replicasets: Vec::new(),
                    replicaset_count: 0,
                    rf: item.replication_factor,
                    instance_count: 0,
                    can_vote: true,
                    name: item.name.clone(),
                    plugins: Vec::new(),
                },
            )
        })
        .collect();

    for replicaset in replicasets {
        let tier_name = replicaset.tier.clone();
        let mut tier = res.get(&replicaset.tier).cloned().unwrap();
        tier.replicaset_count = tier.replicaset_count + 1;
        tier.instance_count = tier.instance_count + replicaset.instances.len();
        tier.replicasets.push(replicaset);
        res.insert(tier_name, tier);
    }

    Ok(res.values().cloned().collect())
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
