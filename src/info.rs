use crate::config::PicodataConfig;
use crate::instance::Grade;
use crate::instance::InstanceId;
use crate::replicaset::ReplicasetId;
use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;
use std::borrow::Cow;
use tarantool::proc;

pub const PICODATA_VERSION: &'static str = std::env!("GIT_DESCRIBE");
pub const RPC_API_VERSION: &'static str = "0.1.0";

////////////////////////////////////////////////////////////////////////////////
// VersionInfo
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct VersionInfo<'a> {
    pub picodata_version: Cow<'a, str>,
    pub rpc_api_version: Cow<'a, str>,
}

impl tarantool::tuple::Encode for VersionInfo<'_> {}

impl tarantool::proc::Return for VersionInfo<'_> {
    #[inline(always)]
    fn ret(self, ctx: tarantool::tuple::FunctionCtx) -> std::os::raw::c_int {
        tarantool::proc::ReturnMsgpack(self).ret(ctx)
    }
}

impl VersionInfo<'static> {
    #[inline(always)]
    pub fn current() -> Self {
        Self {
            picodata_version: PICODATA_VERSION.into(),
            rpc_api_version: RPC_API_VERSION.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// .proc_version_info
////////////////////////////////////////////////////////////////////////////////

#[proc]
pub fn proc_version_info() -> VersionInfo<'static> {
    VersionInfo::current()
}

////////////////////////////////////////////////////////////////////////////////
// InstanceInfo
////////////////////////////////////////////////////////////////////////////////

/// Info returned from [`.proc_instance_info`].
///
/// [`.proc_instance_info`]: proc_instance_info
#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct InstanceInfo {
    pub raft_id: RaftId,
    pub advertise_address: String,
    pub instance_id: InstanceId,
    pub instance_uuid: String,
    pub replicaset_id: ReplicasetId,
    pub replicaset_uuid: String,
    pub cluster_id: String,
    pub current_grade: Grade,
    pub target_grade: Grade,
    pub tier: String,
}

impl tarantool::tuple::Encode for InstanceInfo {}

impl InstanceInfo {
    pub fn try_get(node: &node::Node, instance_id: Option<&InstanceId>) -> Result<Self, Error> {
        let instance;
        match instance_id {
            None => {
                let instance_id = node.raft_storage.instance_id()?;
                let instance_id =
                    instance_id.expect("should be persisted before Node is initialized");
                instance = node.storage.instances.get(&instance_id)?;
            }
            Some(instance_id) => {
                instance = node.storage.instances.get(instance_id)?;
            }
        }

        let peer_address = node
            .storage
            .peer_addresses
            .get(instance.raft_id)?
            .unwrap_or_else(|| "<unknown>".into());

        let cluster_id = node.raft_storage.cluster_id()?;

        Ok(InstanceInfo {
            raft_id: instance.raft_id,
            advertise_address: peer_address,
            instance_id: instance.instance_id,
            instance_uuid: instance.instance_uuid,
            replicaset_id: instance.replicaset_id,
            replicaset_uuid: instance.replicaset_uuid,
            cluster_id,
            current_grade: instance.current_grade,
            target_grade: instance.target_grade,
            tier: instance.tier,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
// .proc_instance_info
////////////////////////////////////////////////////////////////////////////////

#[proc(packed_args)]
pub fn proc_instance_info(request: InstanceInfoRequest) -> Result<InstanceInfo, Error> {
    let node = node::global()?;

    let instance_id = match &request {
        InstanceInfoRequest::CurrentInstance(_) => None,
        InstanceInfoRequest::ByInstanceId([instance_id]) => Some(instance_id),
    };
    InstanceInfo::try_get(node, instance_id)
}

#[derive(Debug, ::serde::Deserialize, ::serde::Serialize)]
#[serde(untagged)]
enum InstanceInfoRequest {
    // FIXME: this is the simplest way I found to support a single optional
    // parameter to the stored procedure. We should probably do something about
    // it in our custom `Encode`/`Decode` traits.
    CurrentInstance([(); 0]),
    ByInstanceId([InstanceId; 1]),
}

impl ::tarantool::tuple::Encode for InstanceInfoRequest {}

impl crate::rpc::RequestArgs for InstanceInfoRequest {
    const PROC_NAME: &'static str = crate::stringify_cfunc!(proc_instance_info);
    type Response = InstanceInfo;
}

////////////////////////////////////////////////////////////////////////////////
// RaftInfo
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct RaftInfo {
    pub id: RaftId,
    pub term: RaftTerm,
    pub applied: RaftIndex,
    pub leader_id: RaftId,
    pub state: node::RaftState,
}

impl tarantool::tuple::Encode for RaftInfo {}

impl RaftInfo {
    #[inline]
    pub fn get(node: &node::Node) -> Self {
        let raft_status = node.status();
        RaftInfo {
            id: raft_status.id,
            term: raft_status.term,
            applied: node.get_index(),
            leader_id: raft_status.leader_id.unwrap_or(0),
            state: raft_status.raft_state,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// .proc_raft_info
////////////////////////////////////////////////////////////////////////////////

#[proc]
pub fn proc_raft_info() -> Result<RaftInfo, Error> {
    let node = node::global()?;

    Ok(RaftInfo::get(node))
}

////////////////////////////////////////////////////////////////////////////////
// InternalInfo
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct InternalInfo<'a> {
    pub main_loop_status: Cow<'a, str>,
    pub governor_loop_status: Cow<'a, str>,
}

impl tarantool::tuple::Encode for InternalInfo<'_> {}

impl InternalInfo<'static> {
    pub fn get(node: &node::Node) -> Self {
        InternalInfo {
            main_loop_status: node.status().main_loop_status.into(),
            governor_loop_status: node.governor_loop.status.get().governor_loop_status.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// RuntimeInfo
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct HttpServerInfo {
    pub host: String,
    pub port: u16,
}

impl tarantool::tuple::Encode for HttpServerInfo {}

/// Info returned from [`.proc_runtime_info`].
///
/// [`.proc_runtime_info`]: proc_runtime_info
#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct RuntimeInfo<'a> {
    pub raft: RaftInfo,
    pub internal: InternalInfo<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http: Option<HttpServerInfo>,
    pub version_info: VersionInfo<'a>,
}

impl tarantool::tuple::Encode for RuntimeInfo<'_> {}

impl RuntimeInfo<'static> {
    pub fn try_get(node: &node::Node) -> Result<Self, Error> {
        let lua = tarantool::lua_state();
        let host_port: Option<(String, String)> = lua.eval(
            "if pico.httpd ~= nil then
                return pico.httpd.host, pico.httpd.port
            else
                return nil
            end",
        )?;
        let mut http = None;
        if let Some((host, port)) = host_port {
            let port = port.parse::<u16>().map_err(Error::other)?;
            http = Some(HttpServerInfo { host, port });
        }

        Ok(RuntimeInfo {
            raft: RaftInfo::get(node),
            internal: InternalInfo::get(node),
            http,
            version_info: VersionInfo::current(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
// .proc_runtime_info
////////////////////////////////////////////////////////////////////////////////

#[proc]
pub fn proc_runtime_info() -> Result<RuntimeInfo<'static>, Error> {
    let node = node::global()?;

    RuntimeInfo::try_get(node)
}

////////////////////////////////////////////////////////////////////////////////
// .proc_get_config
////////////////////////////////////////////////////////////////////////////////

#[proc]
pub fn proc_get_config() -> Result<rmpv::Value, Error> {
    Ok(PicodataConfig::get().parameters_with_sources_as_rmpv())
}
