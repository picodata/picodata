use crate::config::PicodataConfig;
use crate::instance::InstanceName;
use crate::instance::State;
use crate::replicaset::ReplicasetName;
use crate::tlua;
use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;
use crate::vshard::VshardConfig;
use std::borrow::Cow;
use tarantool::proc;
use tarantool::tuple::RawByteBuf;

pub const PICODATA_VERSION: &'static str = std::env!("GIT_DESCRIBE");
pub const RPC_API_VERSION: &'static str = "1.0.0";

/// Note: this returns a `&'static str` because of clap's requirements.
pub fn version_for_help() -> &'static str {
    static VERSION_OUTPUT: std::sync::OnceLock<String> = std::sync::OnceLock::new();

    VERSION_OUTPUT.get_or_init(|| {
        let mut result = PICODATA_VERSION.to_string();
        result.push_str(", ");

        result.push_str(env!("BUILD_TYPE"));
        result.push_str(", ");

        result.push_str(env!("BUILD_PROFILE"));
        result.push('\n');

        result.push_str("tarantool (fork) version: ");
        result.push_str(crate::tarantool::version());
        result.push('\n');

        result.push_str("target: ");
        result.push_str(env!("OS_VERSION"));
        result
    })
}

////////////////////////////////////////////////////////////////////////////////
// VersionInfo
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct VersionInfo<'a> {
    pub picodata_version: Cow<'a, str>,
    pub rpc_api_version: Cow<'a, str>,
    pub build_type: Cow<'a, str>,
    pub build_profile: Cow<'a, str>,
    pub tarantool_version: Cow<'a, str>,
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
            build_type: env!("BUILD_TYPE").into(),
            build_profile: env!("BUILD_PROFILE").into(),
            tarantool_version: crate::tarantool::version().into(),
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
    pub name: InstanceName,
    pub uuid: String,
    pub replicaset_name: ReplicasetName,
    pub replicaset_uuid: String,
    pub cluster_name: String,
    pub current_state: State,
    pub target_state: State,
    pub tier: String,
}

impl tarantool::tuple::Encode for InstanceInfo {}

impl InstanceInfo {
    pub fn try_get(node: &node::Node, instance_name: Option<&InstanceName>) -> Result<Self, Error> {
        let instance;
        match instance_name {
            None => {
                let instance_name = node.raft_storage.instance_name()?;
                let instance_name =
                    instance_name.expect("should be persisted before Node is initialized");
                instance = node.storage.instances.get(&instance_name)?;
            }
            Some(instance_name) => {
                instance = node.storage.instances.get(instance_name)?;
            }
        }

        let peer_address = node
            .storage
            .peer_addresses
            .get(instance.raft_id)?
            .unwrap_or_else(|| "<unknown>".into());

        let cluster_name = node.raft_storage.cluster_name()?;

        Ok(InstanceInfo {
            raft_id: instance.raft_id,
            advertise_address: peer_address,
            name: instance.name,
            uuid: instance.uuid,
            replicaset_name: instance.replicaset_name,
            replicaset_uuid: instance.replicaset_uuid,
            cluster_name,
            current_state: instance.current_state,
            target_state: instance.target_state,
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

    let instance_name = match &request {
        InstanceInfoRequest::CurrentInstance(_) => None,
        InstanceInfoRequest::ByInstanceName([instance_name]) => Some(instance_name),
    };
    InstanceInfo::try_get(node, instance_name)
}

#[derive(Debug, ::serde::Deserialize, ::serde::Serialize)]
#[serde(untagged)]
enum InstanceInfoRequest {
    // FIXME: this is the simplest way I found to support a single optional
    // parameter to the stored procedure. We should probably do something about
    // it in our custom `Encode`/`Decode` traits.
    CurrentInstance([(); 0]),
    ByInstanceName([InstanceName; 1]),
}

impl ::tarantool::tuple::Encode for InstanceInfoRequest {}

impl crate::rpc::RequestArgs for InstanceInfoRequest {
    const PROC_NAME: &'static str = crate::proc_name!(proc_instance_info);
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
    pub governor_step_counter: u64,
}

impl tarantool::tuple::Encode for InternalInfo<'_> {}

impl InternalInfo<'static> {
    pub fn get(node: &node::Node) -> Self {
        let governor = node.governor_loop.status.get();
        InternalInfo {
            main_loop_status: node.status().main_loop_status.into(),
            governor_loop_status: governor.governor_loop_status.into(),
            governor_step_counter: governor.step_counter,
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

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead)]
pub struct SlabInfo {
    #[allow(dead_code)]
    pub items_size: u64,
    #[allow(dead_code)]
    pub items_used_ratio: String,
    pub quota_size: u64,
    #[allow(dead_code)]
    pub quota_used_ratio: String,
    #[allow(dead_code)]
    pub arena_used_ratio: String,
    #[allow(dead_code)]
    pub items_used: u64,
    pub quota_used: u64,
    #[allow(dead_code)]
    pub arena_size: u64,
    #[allow(dead_code)]
    pub arena_used: u64,
}

impl tarantool::tuple::Encode for SlabInfo {}

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
    pub slab_info: SlabInfo,
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
        let slab_info = lua.eval("return box.slab.info();")?;

        Ok(RuntimeInfo {
            raft: RaftInfo::get(node),
            internal: InternalInfo::get(node),
            http,
            version_info: VersionInfo::current(),
            slab_info,
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

////////////////////////////////////////////////////////////////////////////////
// .proc_get_vshard_config
////////////////////////////////////////////////////////////////////////////////

#[proc]
pub fn proc_get_vshard_config(tier_name: Option<String>) -> Result<RawByteBuf, Error> {
    let node = node::global()?;
    let tier_name = if let Some(tier_name) = tier_name {
        let tier = node.storage.tiers.by_name(&tier_name)?;
        if tier.is_none() {
            return Err(Error::NoSuchTier(tier_name));
        };

        tier_name
    } else {
        node.raft_storage
            .tier()?
            .expect("tier for instance should exists")
    };

    let config = VshardConfig::from_storage(&node.storage, &tier_name)?;
    let data = rmp_serde::to_vec_named(&config).map_err(Error::other)?;
    Ok(RawByteBuf::from(data))
}
