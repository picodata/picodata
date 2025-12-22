use crate::config::PicodataConfig;
use crate::instance::InstanceName;
use crate::instance::State;
use crate::replicaset::ReplicasetName;
use crate::sentinel::ActionKind;
use crate::sentinel::FailStreakInfo;
use crate::tlua;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;
use crate::vshard::VshardConfig;
use smol_str::SmolStr;
use std::borrow::Cow;
use std::cell::OnceCell;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::fiber;
use tarantool::proc;
use tarantool::tuple::RawByteBuf;

pub const PICODATA_VERSION: &'static str = std::env!("GIT_DESCRIBE");
pub const RPC_API_VERSION: &'static str = "1.0.0";
const REDIRECT_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Render a message for `picodata -v`.
pub fn render_version() -> String {
    format!(
        "picodata {}, {}, {}\ntarantool {}, {}\n",
        PICODATA_VERSION.to_string(),
        env!("PICO_LINKAGE"),
        env!("PICO_BUILD_PROFILE"),
        crate::tarantool::version(),
        env!("TNT_BUILD_PROFILE"),
    )
}

/// Render a message for `picodata -VV`.
pub fn render_long_version() -> String {
    // XXX: explicit type lets us define the order of fields.
    #[derive(serde::Serialize)]
    struct Info {
        cargo_cfg: serde_json::Value,
        cargo_feature: serde_json::Value,
        rustflags: serde_json::Value,
        linkage: &'static str,
        tarantool: serde_json::Value,
        picodata: serde_json::Value,
    }

    let info = Info {
        cargo_cfg: serde_json::from_str(include_str!(env!("CARGO_CFG_FILE"))).unwrap(),
        cargo_feature: serde_json::from_str(include_str!(env!("CARGO_FEATURE_FILE"))).unwrap(),
        rustflags: serde_json::from_str(include_str!(env!("RUSTFLAGS_FILE"))).unwrap(),
        linkage: env!("PICO_LINKAGE"),
        tarantool: serde_json::json!({
            "build_profile": env!("TNT_BUILD_PROFILE"),
            "version": crate::tarantool::version(),
        }),
        picodata: serde_json::json!({
            "build_profile": env!("PICO_BUILD_PROFILE"),
            "version": PICODATA_VERSION.to_string(),
        }),
    };

    serde_yaml::to_string(&info).unwrap()
}

////////////////////////////////////////////////////////////////////////////////
// VersionInfo
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct VersionInfo {
    pub picodata_version: SmolStr,
    pub rpc_api_version: SmolStr,
    pub build_type: SmolStr,
    pub build_profile: SmolStr,
    pub tarantool_version: SmolStr,
}

impl tarantool::tuple::Encode for VersionInfo {}

impl tarantool::proc::Return for &'static VersionInfo {
    #[inline(always)]
    fn ret(self, ctx: tarantool::tuple::FunctionCtx) -> std::os::raw::c_int {
        tarantool::proc::ReturnMsgpack(self).ret(ctx)
    }
}

impl VersionInfo {
    #[inline(always)]
    #[allow(static_mut_refs)]
    pub fn current() -> &'static Self {
        static mut GLOBAL: OnceCell<VersionInfo> = OnceCell::new();

        // SAFETY:
        // - only called from tx thread
        // - mutated only once during initialization
        unsafe {
            GLOBAL.get_or_init(|| Self {
                picodata_version: SmolStr::new_static(PICODATA_VERSION),
                rpc_api_version: SmolStr::new_static(RPC_API_VERSION),
                build_type: SmolStr::new_static(env!("PICO_LINKAGE")),
                build_profile: SmolStr::new_static(env!("PICO_BUILD_PROFILE")),
                tarantool_version: SmolStr::new_static(crate::tarantool::version()),
            })
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// .proc_picodata_version
////////////////////////////////////////////////////////////////////////////////

#[proc]
pub fn proc_picodata_version() -> &'static str {
    PICODATA_VERSION
}

////////////////////////////////////////////////////////////////////////////////
// .proc_version_info
////////////////////////////////////////////////////////////////////////////////

#[proc]
pub fn proc_version_info() -> &'static VersionInfo {
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
    pub iproto_advertise: SmolStr,
    pub name: InstanceName,
    pub uuid: SmolStr,
    pub replicaset_name: ReplicasetName,
    pub replicaset_uuid: SmolStr,
    pub cluster_name: SmolStr,
    pub current_state: State,
    pub target_state: State,
    pub tier: SmolStr,
    pub picodata_version: SmolStr,
    pub cluster_uuid: SmolStr,
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
            .get(instance.raft_id, &traft::ConnectionType::Iproto)?
            .unwrap_or_else(|| "<unknown>".into());

        let cluster_name = node.topology_cache.cluster_name.into();
        let cluster_uuid = node.topology_cache.cluster_uuid.into();

        Ok(InstanceInfo {
            raft_id: instance.raft_id,
            iproto_advertise: peer_address,
            name: instance.name,
            uuid: instance.uuid,
            replicaset_name: instance.replicaset_name,
            replicaset_uuid: instance.replicaset_uuid,
            cluster_name,
            cluster_uuid,
            current_state: instance.current_state,
            target_state: instance.target_state,
            tier: instance.tier,
            picodata_version: instance.picodata_version,
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
        InstanceInfoRequest::OneArgument([Some(instance_name)]) => Some(instance_name),
        _ => None,
    };
    InstanceInfo::try_get(node, instance_name)
}

#[derive(Debug, ::serde::Deserialize, ::serde::Serialize)]
#[serde(untagged)]
pub(crate) enum InstanceInfoRequest {
    // FIXME: this is the simplest way I found to support a single optional
    // parameter to the stored procedure. We should probably do something about
    // it in our custom `Encode`/`Decode` traits.
    /// For example in lua:
    /// ```lua
    /// net_box:call('.proc_instance_info') -- current instance
    /// ```
    /// or in python:
    /// ```python
    /// instance.call('.proc_instance_info') # current instance
    /// ```
    NoArguments([(); 0]),

    /// For example in lua:
    /// ```lua
    /// net_box:call('.proc_instance_info', {instance_name}) -- given instance
    /// net_box:call('.proc_instance_info', {box.NULL})      -- current instance
    /// ```
    /// or in python:
    /// ```python
    /// instance.call('.proc_instance_info', instance_name) # given instance
    /// instance.call('.proc_instance_info', None)          # current instance
    /// ```
    OneArgument([Option<InstanceName>; 1]),
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
// .proc_raft_leader_id
////////////////////////////////////////////////////////////////////////////////

#[proc]
pub fn proc_raft_leader_id() -> Result<u64, Error> {
    let node = node::global()?;
    let raft = RaftInfo::get(node);

    Ok(raft.leader_id)
}

////////////////////////////////////////////////////////////////////////////////
// .proc_raft_leader_uuid
////////////////////////////////////////////////////////////////////////////////

#[proc]
pub fn proc_raft_leader_uuid() -> Result<SmolStr, Error> {
    let node = node::global()?;
    let raft = RaftInfo::get(node);

    let storage = &node.storage.instances;
    let leader = storage.get(&raft.leader_id)?;
    Ok(leader.uuid)
}

////////////////////////////////////////////////////////////////////////////////
// .proc_instance_uuid
////////////////////////////////////////////////////////////////////////////////

#[proc]
pub fn proc_instance_uuid() -> Result<SmolStr, Error> {
    let node = node::global()?;
    Ok(node.topology_cache.my_instance_uuid().into())
}

////////////////////////////////////////////////////////////////////////////////
// .proc_instance_name
////////////////////////////////////////////////////////////////////////////////

#[proc]
fn proc_instance_name(uuid: SmolStr) -> Result<Option<SmolStr>, Error> {
    let node = node::global()?;
    let cache = node.topology_cache.get();
    match cache.instance_by_uuid(&uuid) {
        Ok(instance) => Ok(Some(instance.name.clone().into())),
        Err(traft::error::Error::NoSuchInstance(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

////////////////////////////////////////////////////////////////////////////////
// .proc_replicaset_name
////////////////////////////////////////////////////////////////////////////////

#[proc]
fn proc_replicaset_name(uuid: SmolStr) -> Result<Option<SmolStr>, Error> {
    let node = node::global()?;
    let cache = node.topology_cache.get();
    match cache.instance_by_uuid(&uuid) {
        Ok(instance) => Ok(Some(instance.replicaset_name.clone().into())),
        Err(traft::error::Error::NoSuchInstance(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

////////////////////////////////////////////////////////////////////////////////
// .proc_tier_name
////////////////////////////////////////////////////////////////////////////////

#[proc]
fn proc_tier_name(uuid: SmolStr) -> Result<Option<SmolStr>, Error> {
    let node = node::global()?;
    let cache = node.topology_cache.get();
    match cache.instance_by_uuid(&uuid) {
        Ok(instance) => Ok(Some(instance.tier.clone())),
        Err(traft::error::Error::NoSuchInstance(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

////////////////////////////////////////////////////////////////////////////////
// .proc_instance_dir
////////////////////////////////////////////////////////////////////////////////

fn impl_proc_instance_dir() -> Result<Option<String>, Error> {
    let picodata_config = crate::config::PicodataConfig::get();
    let instance_config = &picodata_config.instance;

    if let Some(working_path) = &instance_config.instance_dir {
        if working_path.is_absolute() {
            Ok(Some(working_path.display().to_string()))
        } else {
            // NOTE(kbezuglyi): `std::path::absolute` does not resolve symlinks,
            // and may succeed even if the path does not exist. Symlink
            // resolution is avoided by occasion, meanwhile path existence
            // confirmation is done intentionally, because if the cluster
            // started without explicit specification of a config file path, we
            // should return `null`, that's why we can't return `null` (the path
            // does not exist anymore), to avoid confusing the end user.
            let absolute_path = std::path::absolute(working_path)?;
            Ok(Some(absolute_path.display().to_string()))
        }
    } else {
        Ok(None)
    }
}

#[proc]
fn proc_instance_dir(uuid: SmolStr) -> Result<Option<String>, Error> {
    let node = node::global()?;
    let requested_uuid = uuid.to_string();
    let my_uuid = node.topology_cache.my_instance_uuid();

    let is_local = requested_uuid == my_uuid;
    if is_local {
        return impl_proc_instance_dir();
    }

    let topology_ref = node.topology_cache.get();
    if let Ok(instance) = topology_ref.instance_by_uuid(&requested_uuid) {
        let future = async {
            node.pool
                .call_raw(
                    &instance.name,
                    crate::proc_name!(proc_instance_dir),
                    &(uuid,),
                    REDIRECT_RPC_TIMEOUT,
                )?
                .await
        };
        Ok(fiber::block_on(future)?)
    } else {
        Ok(None)
    }
}

////////////////////////////////////////////////////////////////////////////////
// .proc_config_file
////////////////////////////////////////////////////////////////////////////////

fn impl_proc_config_file() -> Result<Option<String>, Error> {
    let picodata_config = crate::config::PicodataConfig::get();
    let instance_config = &picodata_config.instance;

    if let Some(config_path) = &instance_config.config_file {
        if config_path.is_absolute() {
            return Ok(Some(config_path.display().to_string()));
        } else {
            // NOTE(kbezuglyi): `std::path::absolute` does not resolve symlinks,
            // and may succeed even if the path does not exist. Symlink
            // resolution is avoided by occasion, meanwhile path existence
            // confirmation is done intentionally, because if the cluster
            // started without explicit specification of a config file path, we
            // should return `null`, that's why we can't return `null` (the path
            // does not exist anymore), to avoid confusing the end user.
            let absolute_path = std::path::absolute(config_path)?;
            return Ok(Some(absolute_path.display().to_string()));
        }
    } else {
        return Ok(None);
    }
}

#[proc]
fn proc_config_file(uuid: SmolStr) -> Result<Option<String>, Error> {
    let node = node::global()?;
    let requested_uuid = uuid.to_string();
    let my_uuid = node.topology_cache.my_instance_uuid();

    let is_local = requested_uuid == my_uuid;
    if is_local {
        return impl_proc_config_file();
    }

    let topology_ref = node.topology_cache.get();
    if let Ok(instance) = topology_ref.instance_by_uuid(&requested_uuid) {
        let future = async {
            node.pool
                .call_raw(
                    &instance.name,
                    crate::proc_name!(proc_config_file),
                    &(uuid,),
                    REDIRECT_RPC_TIMEOUT,
                )?
                .await
        };
        Ok(fiber::block_on(future)?)
    } else {
        Ok(None)
    }
}

////////////////////////////////////////////////////////////////////////////////
// InternalInfo
////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct InternalInfo<'a> {
    pub main_loop_status: Cow<'a, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub main_loop_last_entry: Option<RaftEntryInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub main_loop_last_error: Option<SerializableErrorInfo>,

    pub governor_loop_status: Cow<'a, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub governor_loop_last_error: Option<SerializableErrorInfo>,
    pub governor_step_counter: u64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sentinel_last_action: Option<ActionKind>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sentinel_last_action_reason: Option<SmolStr>,

    /// Applied index at the moment of last successful action by sentinel.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sentinel_index_of_last_success: Option<RaftIndex>,

    /// Number of seconds elapsed since the last successful action.
    /// Note that we return the duration since instead of a timestamp because
    /// we only store the monotonic timestamp which is not very human-friendly
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sentinel_time_since_last_success: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sentinel_fail_streak: Option<SerializableFailStreakInfo>,
}

impl tarantool::tuple::Encode for InternalInfo<'_> {}

impl InternalInfo<'static> {
    pub fn get(node: &node::Node) -> Self {
        let governor = node.governor_loop.status.borrow();

        let mut info = InternalInfo {
            main_loop_status: node.status().main_loop_status.into(),
            governor_loop_status: governor.governor_loop_status.into(),
            governor_step_counter: governor.step_counter,
            ..Default::default()
        };

        let main_loop_info = node.main_loop_info.borrow();
        if let Some(last_entry) = &main_loop_info.last_entry {
            info.main_loop_last_entry = Some(RaftEntryInfo::new(last_entry));
        }
        if let Some(last_error) = &main_loop_info.last_error {
            info.main_loop_last_error = Some(SerializableErrorInfo::new(&last_error.error));
        }

        if let Some(error) = &governor.last_error {
            info.governor_loop_last_error = Some(SerializableErrorInfo::new(error));
        }

        let sentinel = node.sentinel_loop.stats.borrow();
        info.sentinel_last_action = sentinel.last_action_kind;
        info.sentinel_last_action_reason = sentinel.last_action_reason.clone();

        if let Some((index, time)) = sentinel.last_successful_attempt {
            info.sentinel_index_of_last_success = Some(index);
            let elapsed = fiber::clock().duration_since(time).as_secs_f64();
            info.sentinel_time_since_last_success = Some(elapsed);
        }

        if let Some(fail_streak) = &sentinel.fail_streak {
            info.sentinel_fail_streak = Some(SerializableFailStreakInfo::new(fail_streak));
        }

        info
    }
}

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct SerializableFailStreakInfo {
    /// Number of consequitive failures with the same error.
    pub count: u32,
    /// Seconds since the first failure in the fail streak.
    pub time_since_start: f64,
    /// Seconds since the last failure in the fail streak.
    pub time_since_last_try: f64,
    /// The error value of the fail streak.
    /// If the error code changes the fail streak is reset.
    pub error: SerializableErrorInfo,
}

impl SerializableFailStreakInfo {
    #[inline(always)]
    fn new(info: &FailStreakInfo) -> Self {
        let now = fiber::clock();
        Self {
            count: info.count,
            time_since_start: now.duration_since(info.start).as_secs_f64(),
            time_since_last_try: now.duration_since(info.last_try).as_secs_f64(),
            error: SerializableErrorInfo::new(&info.error),
        }
    }
}

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct SerializableErrorInfo {
    code: u32,
    message: String,
    file: Option<String>,
    line: Option<u32>,
}

impl SerializableErrorInfo {
    #[inline(always)]
    fn new(e: &BoxError) -> Self {
        Self {
            code: e.error_code(),
            message: e.message().into(),
            file: e.file().map(String::from),
            line: e.line(),
        }
    }
}

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct RaftEntryInfo {
    term: RaftTerm,
    index: RaftIndex,
    payload: String,
}

impl RaftEntryInfo {
    fn new(entry: &crate::traft::Entry) -> Self {
        Self {
            term: entry.term,
            index: entry.index,
            payload: entry.payload().to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// RuntimeInfoDeprecated
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct HttpServerInfo {
    pub host: SmolStr,
    pub port: u16,
}

impl tarantool::tuple::Encode for HttpServerInfo {}

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead)]
pub struct SlabInfo {
    #[allow(dead_code)]
    pub items_size: u64,
    #[allow(dead_code)]
    pub items_used_ratio: SmolStr,
    pub quota_size: u64,
    #[allow(dead_code)]
    pub quota_used_ratio: SmolStr,
    #[allow(dead_code)]
    pub arena_used_ratio: SmolStr,
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
pub struct RuntimeInfoDeprecated<'a> {
    pub raft: RaftInfo,
    pub internal: InternalInfo<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http: Option<HttpServerInfo>,
    pub version_info: VersionInfo,
    pub slab_info: SlabInfo,
}

impl tarantool::tuple::Encode for RuntimeInfoDeprecated<'_> {}

impl RuntimeInfoDeprecated<'static> {
    pub fn try_get(node: &node::Node) -> Result<Self, Error> {
        let info = RuntimeInfo::try_get(node)?;

        Ok(RuntimeInfoDeprecated {
            raft: info.raft,
            internal: info.internal,
            http: info.http,
            version_info: info.version_info,
            slab_info: info.slab_info,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
// .proc_runtime_info
////////////////////////////////////////////////////////////////////////////////

#[deprecated(note = "Use `proc_runtime_info_v2` instead")]
#[proc]
pub fn proc_runtime_info() -> Result<RuntimeInfoDeprecated<'static>, Error> {
    let node = node::global()?;

    RuntimeInfoDeprecated::try_get(node)
}

/// Info returned from [`.proc_runtime_info_v2`].
///
/// [`.proc_runtime_info_v2`]: proc_runtime_info_v2
#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct RuntimeInfo<'a> {
    pub raft: RaftInfo,
    pub internal: InternalInfo<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http: Option<HttpServerInfo>,
    pub version_info: VersionInfo,
    pub slab_info: SlabInfo,
    pub slab_system_info: SlabInfo,
}

impl tarantool::tuple::Encode for RuntimeInfo<'_> {}

impl RuntimeInfo<'static> {
    pub fn try_get(node: &node::Node) -> Result<Self, Error> {
        let lua = tarantool::lua_state();
        let host_port: Option<(SmolStr, SmolStr)> = lua.eval(
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
        let slab_system_info = lua.eval("return box.slab.system_info();")?;

        Ok(RuntimeInfo {
            raft: RaftInfo::get(node),
            internal: InternalInfo::get(node),
            http,
            version_info: VersionInfo::current().clone(),
            slab_info,
            slab_system_info,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
// .proc_runtime_info_v2
////////////////////////////////////////////////////////////////////////////////

#[proc]
pub fn proc_runtime_info_v2() -> Result<RuntimeInfo<'static>, Error> {
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
pub fn proc_get_vshard_config(tier_name: Option<SmolStr>) -> Result<RawByteBuf, Error> {
    let node = node::global()?;
    let tier_name = if let Some(tier_name) = tier_name {
        tier_name
    } else {
        node.topology_cache.my_tier_name().into()
    };
    let Some(tier) = node.storage.tiers.by_name(&tier_name)? else {
        return Err(Error::NoSuchTier(tier_name));
    };

    let config = VshardConfig::from_storage(node, &tier.name, tier.bucket_count)?;
    let data = rmp_serde::to_vec_named(&config).map_err(Error::other)?;
    Ok(RawByteBuf::from(data))
}
