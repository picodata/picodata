use crate::config::{BootstrapStrategy, ByteSize, ElectionMode, PicodataConfig};
use crate::config_parameter_path;
use crate::instance::Instance;
use crate::introspection::Introspection;
use crate::pico_service::pico_service_password;
use crate::rpc::join;
use crate::schema::PICO_SERVICE_USER_NAME;
use crate::sql::port::{dispatch_dump_mp, PicoPortOwned};
use crate::tlog;
use crate::traft::{self, error::Error};
use ::tarantool::error::{Error as TntError, IntoBoxError};
use ::tarantool::ffi::uuid::tt_uuid;
use ::tarantool::fiber;
use ::tarantool::lua_state;
use ::tarantool::msgpack;
use ::tarantool::network::protocol::{codec, iproto_key};
use ::tarantool::tlua::{self, LuaError, LuaFunction, LuaRead, LuaTable, LuaThread, PushGuard};
pub use ::tarantool::trigger::on_shutdown;
use file_shred::*;
use rmpv::Value as RmpvValue;
use serde::Deserialize;
use smallvec::SmallVec;
use sql::executor::Port as SqlPort;
use std::collections::HashMap;
use std::ffi::CStr;
use std::io::Cursor;
use std::mem;
use std::ops::Range;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::slice;
use std::sync::atomic::{AtomicBool, Ordering};
use tlua::CallError;

#[macro_export]
macro_rules! stringify_last_token {
    ($tail:tt) => { std::stringify!($tail) };
    ($head:tt $($tail:tt)+) => { $crate::stringify_last_token!($($tail)+) };
}

/// Checks that the given function exists and returns it's name suitable for
/// calling it via tarantool rpc.
///
/// The argument can be a full path to the function.
#[macro_export]
macro_rules! proc_name {
    ( $($func_name:tt)+ ) => {{
        use ::tarantool::tuple::FunctionArgs;
        use ::tarantool::tuple::FunctionCtx;
        use libc::c_int;

        const _: unsafe extern "C" fn(FunctionCtx, FunctionArgs) -> c_int = $($func_name)+;
        concat!(".", $crate::stringify_last_token!($($func_name)+))
    }};
}

macro_rules! unwrap_or_report_and_propagate_err {
    ($res:expr) => {
        match $res {
            Ok(o) => o,
            Err(e) => {
                e.set_last_error();
                return IprotoHandlerStatus::IPROTO_HANDLER_ERROR;
            }
        }
    };
}

mod ffi {
    use libc::c_char;
    use libc::c_int;
    use libc::c_void;

    extern "C" {
        pub fn tarantool_version() -> *const c_char;
        pub fn tarantool_package() -> *const c_char;

        pub fn tarantool_exit(code: c_int);
        pub fn tarantool_main(
            argc: i32,
            argv: *mut *mut c_char,
            cb: Option<extern "C" fn(*mut c_void)>,
            cb_data: *mut c_void,
        ) -> c_int;
    }
}
pub use ffi::tarantool_main as main;

pub fn version() -> &'static str {
    let c_ptr = unsafe { ffi::tarantool_version() };
    let c_str = unsafe { CStr::from_ptr(c_ptr) };
    return c_str.to_str().unwrap();
}

pub fn package() -> &'static str {
    let c_ptr = unsafe { ffi::tarantool_package() };
    let c_str = unsafe { CStr::from_ptr(c_ptr) };
    return c_str.to_str().unwrap();
}

mod tests {
    use super::*;

    #[::tarantool::test]
    fn test_version() {
        let l = lua_state();
        let t: LuaTable<_> = l.eval("return require('tarantool')").unwrap();
        assert_eq!(version(), t.get::<String, _>("version").unwrap());
        assert_eq!(package(), t.get::<String, _>("package").unwrap());
    }

    #[::tarantool::test]
    fn test_write_uint() {
        let mut data: SmallVec<[u8; RESERVED_SIZE]> = SmallVec::new();

        write_uint(0, &mut data);
        assert_eq!(0u8.to_ne_bytes(), [data[0]]);
        data.clear();

        write_uint(i8::MAX as u64, &mut data);
        assert_eq!(i8::MAX.to_ne_bytes(), [data[0]]);
        data.clear();

        let written = i8::MAX as u64 + 1;
        write_uint(written, &mut data);
        let mut expected = vec![0xCC];
        expected.extend_from_slice((written as u8).to_ne_bytes().as_ref());
        assert_eq!(expected, data[..2]);
        data.clear();

        let written = u8::MAX as u64;
        write_uint(written, &mut data);
        let mut expected = vec![0xCC];
        expected.extend_from_slice((written as u8).to_ne_bytes().as_ref());
        assert_eq!(expected, data[..2]);
        data.clear();

        let written = u8::MAX as u64 + 1;
        write_uint(written, &mut data);
        let mut expected = vec![0xCD];
        expected.extend_from_slice((written as u16).to_ne_bytes().as_ref());
        assert_eq!(expected, data[..3]);
        data.clear();

        let written = u16::MAX as u64;
        write_uint(written, &mut data);
        let mut expected = vec![0xCD];
        expected.extend_from_slice((written as u16).to_ne_bytes().as_ref());
        assert_eq!(expected, data[..3]);
        data.clear();

        let written = u16::MAX as u64 + 1;
        write_uint(written, &mut data);
        let mut expected = vec![0xCE];
        expected.extend_from_slice((written as u32).to_ne_bytes().as_ref());
        assert_eq!(expected, data[..5]);
        data.clear();

        let written = u32::MAX as u64;
        write_uint(written, &mut data);
        let mut expected = vec![0xCE];
        expected.extend_from_slice((written as u32).to_ne_bytes().as_ref());
        assert_eq!(expected, data[..5]);
        data.clear();

        let written = u32::MAX as u64 + 1;
        write_uint(written, &mut data);
        let mut expected = vec![0xCF];
        expected.extend_from_slice(written.to_ne_bytes().as_ref());
        assert_eq!(expected, data[..9]);
        data.clear();

        let written = u64::MAX;
        write_uint(written, &mut data);
        let mut expected = vec![0xCF];
        expected.extend_from_slice(written.to_ne_bytes().as_ref());
        assert_eq!(expected, data[..9]);
        data.clear();
    }

    #[::tarantool::test]
    fn box_error_from_lua() {
        use tarantool::error::BoxError;
        use tarantool::error::TarantoolErrorCode;

        let lua = tarantool::lua_state();

        // Implicit conversion from string
        let e: BoxError = lua.eval("return 'error from string'").unwrap();
        assert_eq!(e.error_type(), "Unknown");
        assert_eq!(e.error_code(), TarantoolErrorCode::ProcLua as u32);
        assert_eq!(e.message(), "error from string");

        // struct box_error * -> BoxError
        let e: BoxError = lua.eval("return box.error.new(box.error.UNSUPPORTED, 'picodata', 'synchronous replication')").unwrap();
        assert_eq!(e.error_type(), "ClientError");
        assert_eq!(e.error_code(), TarantoolErrorCode::Unsupported as u32);
        assert_eq!(
            e.message(),
            "picodata does not support synchronous replication"
        );

        // Vshard-style error objects
        let e: BoxError = lua.eval("return { type = 'CustomType', code = 420, message = 'vshard uses this type of errors'}").unwrap();
        assert_eq!(e.error_type(), "CustomType");
        assert_eq!(e.error_code(), 420);
        assert_eq!(e.message(), "vshard uses this type of errors");

        //
        // Error cases
        //
        let e = lua.eval::<BoxError>("return nil").unwrap_err();
        assert_eq!(
            e.to_string(),
            "failed reading value(s) returned by Lua: tarantool::error::BoxError expected, got nil"
        );

        let e = lua.eval::<BoxError>("return 69").unwrap_err();
        assert_eq!(e.to_string(), "failed reading value(s) returned by Lua: tarantool::error::BoxError expected, got number");

        let e = lua.eval::<BoxError>("return {}").unwrap_err();
        assert_eq!(
            e.to_string(),
            "failed reading Lua value: alloc::string::String expected, got nil
    while decoding error object: field 'type' of type string expected, got nil
    while reading value(s) returned by Lua: tarantool::error::BoxError expected, got table"
        );

        let e = lua
            .eval::<BoxError>(
                "return { type = 'CustomType', code = 420, msg = 'incorrect field name'}",
            )
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "failed reading Lua value: alloc::string::String expected, got nil
    while decoding error object: field 'message' of type string expected, got nil
    while reading value(s) returned by Lua: tarantool::error::BoxError expected, got table"
        );

        let e = lua
            .eval::<BoxError>(
                "return { type = 'CustomType', code = 'wrong type', message = 'missing fields'}",
            )
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "failed reading Lua value: u32 expected, got string
    while decoding error object: field 'code' of type u32 expected, got string
    while reading value(s) returned by Lua: tarantool::error::BoxError expected, got table"
        );
    }
}

#[derive(
    Clone,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    tlua::LuaRead,
    tlua::Push,
    tlua::PushInto,
)]
pub struct ListenConfigParams {
    pub transport: String,
    pub ssl_cert_file: Option<PathBuf>,
    pub ssl_key_file: Option<PathBuf>,
    pub ssl_ca_file: Option<PathBuf>,
}

#[derive(
    Clone,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    tlua::LuaRead,
    tlua::Push,
    tlua::PushInto,
)]
pub struct ListenConfig {
    pub uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<ListenConfigParams>,
}

impl ListenConfig {
    pub fn new(uri: String, config: &crate::iproto::TlsConfig) -> Self {
        let mut result = Self { uri, params: None };
        if config.enabled() {
            result.params = Some(ListenConfigParams {
                transport: "ssl".to_string(),
                ssl_cert_file: config.cert_file.clone(),
                ssl_key_file: config.key_file.clone(),
                ssl_ca_file: config.ca_file.clone(),
            });
        }
        result
    }

    pub fn new_for_pico_service(uri: &str, tls_config: &crate::iproto::TlsConfig) -> Self {
        if tls_config.enabled() {
            return Self::new(format!("{PICO_SERVICE_USER_NAME}@{uri}"), tls_config);
        }

        let password = pico_service_password();
        Self::new(
            format!("{PICO_SERVICE_USER_NAME}:{password}@{uri}"),
            tls_config,
        )
    }
}

/// Tarantool configuration.
/// See <https://www.tarantool.io/en/doc/latest/reference/configuration/#configuration-parameters>
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Cfg {
    pub instance_uuid: Option<String>,
    pub replicaset_uuid: Option<String>,

    pub listen: Option<ListenConfig>,

    pub read_only: bool,
    pub replication: Vec<ListenConfig>,

    pub bootstrap_strategy: Option<BootstrapStrategy>,

    pub election_mode: ElectionMode,

    pub log: Option<String>,
    pub log_level: Option<u8>,

    #[serde(flatten)]
    pub user_configured_fields: HashMap<String, RmpvValue>,
}

impl Cfg {
    pub fn for_restore(config: &PicodataConfig) -> Result<Self, Error> {
        let mut res: Cfg = Self {
            // We don't need any of the fields below.
            instance_uuid: None,
            replicaset_uuid: None,
            listen: None,

            // On restore all storages should be writable.
            read_only: false,

            // During restore we don't set up the replication.
            bootstrap_strategy: Some(BootstrapStrategy::Auto),
            election_mode: ElectionMode::Off,

            ..Default::default()
        };

        res.set_core_parameters(config)?;

        // Leave only the one snapshot file.
        res.user_configured_fields
            .insert("checkpoint_count".into(), 1.into());

        Ok(res)
    }

    /// Temporary minimal configuration. After initializing with this
    /// configuration we either will go into discovery phase after which we will
    /// rebootstrap and go to the next phase (either boot or join), or if the
    /// storage is already initialize we will go into the post join phase.
    pub fn for_discovery(config: &PicodataConfig) -> Result<Self, Error> {
        let mut res = Self {
            // These will either be chosen on the next phase or are already
            // chosen and will be restored from the local storage.
            instance_uuid: None,
            replicaset_uuid: None,

            // Listen port will be set a bit later.
            listen: None,

            // On discovery stage the local storage needs to be bootstrapped,
            // but if we're restarting this will be changed to `true`, because
            // we can't be a replication master at that point.
            read_only: false,

            // During discovery phase we either don't set up the replication, or
            // the replicaset is already bootstrapped, so bootstrap strategy is
            // irrelevant.
            bootstrap_strategy: Some(BootstrapStrategy::Auto),

            election_mode: ElectionMode::Off,

            // If this is a restart, replication will be configured by governor
            // before our state changes to Online.
            ..Default::default()
        };

        res.set_core_parameters(config)?;
        Ok(res)
    }

    /// Initial configuration for the cluster bootstrap phase.
    pub fn for_cluster_bootstrap(
        config: &PicodataConfig,
        leader: &Instance,
    ) -> Result<Self, Error> {
        let mut res = Self {
            // At this point uuids must be valid, it will be impossible to
            // change them until the instance is expelled.
            instance_uuid: Some(leader.uuid.clone()),
            replicaset_uuid: Some(leader.replicaset_uuid.clone()),

            // Listen port will be set after the global raft node is initialized.
            listen: None,

            // Must be writable, we're going to initialize the storage.
            read_only: false,

            // During bootstrap the replicaset contains only one instance,
            // so bootstrap strategy is irrelevant.
            bootstrap_strategy: Some(BootstrapStrategy::Auto),

            election_mode: ElectionMode::Off,

            // Replication will be configured by governor when another replica
            // joins.
            ..Default::default()
        };

        res.set_core_parameters(config)?;
        Ok(res)
    }

    /// Initial configuration for the new instance joining to an already
    /// initialized cluster.
    pub fn for_instance_join(
        config: &PicodataConfig,
        resp: &join::Response,
    ) -> Result<Self, Error> {
        let mut replication_cfg = Vec::with_capacity(resp.box_replication.len());
        let tls_config = &config.instance.iproto_tls;
        for address in &resp.box_replication {
            replication_cfg.push(ListenConfig::new_for_pico_service(address, tls_config));
        }

        let mut res = Self {
            // At this point uuids must be valid, it will be impossible to
            // change them until the instance is expelled.
            instance_uuid: Some(resp.instance.uuid.clone()),
            replicaset_uuid: Some(resp.instance.replicaset_uuid.clone()),

            // Needs to be set, because an applier will attempt to connect to
            // self and will block box.cfg() call until it succeeds.
            listen: Some(ListenConfig::new(
                config.instance.iproto_listen().to_host_port(),
                tls_config,
            )),

            // Raft leader determines who is booting up in read_only mode.
            read_only: !resp.is_master,

            // Always contains the current instance.
            replication: replication_cfg,

            // TODO: in tarantool-3.0 there's a new bootstrap_strategy = "config"
            // which allows to specify the bootstrap leader explicitly. This is
            // what we want to use, so we should switch to that when that
            // feature is available in our tarantool fork.
            //
            // Use `bootstrap_strategy = legacy` instead of `auto` to
            // work around `ER_BOOTSTRAP_CONNECTION_NOT_TO_ALL`, which is
            // triggered only for `bootstrap_strategy = auto`.
            // Note: Combine with `replication_connect_quorum = 0`. See below
            // about it.
            bootstrap_strategy: Some(BootstrapStrategy::Legacy),

            election_mode: ElectionMode::Off,

            ..Default::default()
        };

        res.set_core_parameters(config)?;

        // This option is required when using `bootstrap_strategy = legacy`.
        //
        // Setting this value to `0` disables the check for successful replica connections
        // in Tarantool. This does not prevent connections to replicas or prevent the
        // instance from proceeding with initialization.
        //
        // When `box.cfg()` succeeds, it indicates that the replica has successfully
        // connected to at least one other replica and obtained a replication snapshot.
        // If `box.cfg()` fails, Tarantool will panic, requiring a replica restart.
        //
        // Only after successful `box.cfg()` does an instance proceed further -
        // it activates itself by changing its own state to 'Online' via RPC.
        res.user_configured_fields
            .insert("replication_connect_quorum".into(), 0.into());
        Ok(res)
    }

    pub fn for_instance_pre_join(
        config: &PicodataConfig,
        instance_uuid: String,
    ) -> Result<Self, Error> {
        let mut res = Self {
            instance_uuid: Some(instance_uuid),

            // As long as we are going to rebootstrap a little bit later,
            // it is fine to skip initialization on our side. Anyway,
            // Tarantool will still generate it, but we don't really care
            // for now on what value would it be initialized by Tarantool.
            replicaset_uuid: None,

            // We don't expect any incoming connections because we will
            // rebootstrap later.
            listen: None,

            // Even though we rebootstrap later, we need to persist data to Raft space.
            read_only: false,

            // This is temporary configuration, we don't really need replication,
            // because we will rebootstrap a little bit later.
            replication: Vec::new(),

            ..Default::default()
        };

        res.set_core_parameters(config)?;
        Ok(res)
    }

    pub fn set_core_parameters(&mut self, config: &PicodataConfig) -> Result<(), Error> {
        self.log.clone_from(&config.instance.log.destination);
        self.log_level = Some(config.instance.log_level() as _);
        crate::error_injection!("USE_SHORT_REPLICATION_CONNECT_TIMEOUT" => {
            self.user_configured_fields
                .insert("replication_connect_timeout".into(), 2.into());
        });

        // here we handle fields with `ByteSize` types that are needed to be
        // converted into `String`, and then parsed as `rmpv::Value::Integer`
        #[rustfmt::skip]
        const BYTESIZE_FIELDS: &[(&str, &str)] = &[
            ("memtx_memory",                config_parameter_path!(instance.memtx.memory)),
            ("memtx_system_memory",         config_parameter_path!(instance.memtx.system_memory)),
            ("memtx_max_tuple_size",        config_parameter_path!(instance.memtx.max_tuple_size)),
            ("vinyl_memory",                config_parameter_path!(instance.vinyl.memory)),
            ("vinyl_cache",                 config_parameter_path!(instance.vinyl.cache)),
            ("vinyl_max_tuple_size",        config_parameter_path!(instance.vinyl.max_tuple_size)),
            ("vinyl_page_size",             config_parameter_path!(instance.vinyl.page_size)),
            ("vinyl_range_size",            config_parameter_path!(instance.vinyl.range_size)),
        ];

        for (box_field, picodata_field) in BYTESIZE_FIELDS {
            let bytesize_value = config
                .get_field_as_rmpv(picodata_field)
                .map_err(|e| Error::other(format!("internal error: {e}")))?;
            let deser_value: u64 = ByteSize::deserialize(bytesize_value)
                .as_ref()
                .expect("ByteSize should represent correct unsigned integer")
                .into();
            self.user_configured_fields
                .insert((*box_field).into(), deser_value.into());
        }

        // here we handle fields with primitive types that are not
        // needed to be converted twice to be parsed as `rmpv::Value`
        #[rustfmt::skip]
        const FIELDS: &[(&str, &str)] = &[
            // other instance.log.* parameters are set explicitly above
            ("log_format",                  config_parameter_path!(instance.log.format)),
            ("wal_dir",                     config_parameter_path!(instance.instance_dir)),
            ("memtx_dir",                   config_parameter_path!(instance.instance_dir)),
            ("vinyl_dir",                   config_parameter_path!(instance.instance_dir)),
            ("vinyl_bloom_fpr",             config_parameter_path!(instance.vinyl.bloom_fpr)),
            ("vinyl_run_count_per_level",   config_parameter_path!(instance.vinyl.run_count_per_level)),
            ("vinyl_run_size_ratio",        config_parameter_path!(instance.vinyl.run_size_ratio)),
            ("vinyl_read_threads",          config_parameter_path!(instance.vinyl.read_threads)),
            ("vinyl_write_threads",         config_parameter_path!(instance.vinyl.write_threads)),
            ("vinyl_timeout",               config_parameter_path!(instance.vinyl.timeout)),
        ];

        for (box_field, picodata_field) in FIELDS {
            let value = config
                .get_field_as_rmpv(picodata_field)
                .map_err(|e| Error::other(format!("internal error: {e}")))?;
            debug_assert_ne!(value, RmpvValue::Nil);
            self.user_configured_fields
                .insert((*box_field).into(), value);
        }

        Ok(())
    }
}

pub fn is_box_configured() -> bool {
    let lua = lua_state();
    let box_: Option<LuaTable<_>> = lua.get("box");
    let Some(box_) = box_ else {
        return false;
    };
    let box_cfg: Option<LuaTable<_>> = box_.get("cfg");
    box_cfg.is_some()
}

#[track_caller]
pub fn set_cfg(cfg: &Cfg) -> Result<(), Error> {
    let lua = lua_state();
    let res = lua.exec_with("return box.cfg(...)", msgpack::ViaMsgpack(cfg));
    match res {
        Err(CallError::PushError(e)) => {
            crate::tlog!(Error, "failed to push box configuration via msgpack: {e}");
            return Err(Error::other(e));
        }
        Err(CallError::LuaError(e)) => {
            return Err(Error::other(e));
        }
        Ok(()) => {}
    }
    Ok(())
}

#[allow(dead_code)]
pub fn cfg_field<T>(field: &str) -> Option<T>
where
    T: LuaRead<PushGuard<LuaTable<PushGuard<LuaTable<PushGuard<LuaThread>>>>>>,
{
    let l = lua_state();
    let b: LuaTable<_> = l.into_get("box").ok()?;
    let cfg: LuaTable<_> = b.into_get("cfg").ok()?;
    cfg.into_get(field).ok()
}

pub fn set_cfg_field_from_rmpv(field: &str, value: rmpv::Value) -> Result<(), tlua::LuaError> {
    match value {
        rmpv::Value::Nil => {
            set_cfg_field(field, tlua::Null)?;
        }
        rmpv::Value::Boolean(v) => {
            set_cfg_field(field, v)?;
        }
        rmpv::Value::Integer(v) => {
            if let Some(v) = v.as_i64() {
                set_cfg_field(field, v)?;
            } else if let Some(v) = v.as_u64() {
                set_cfg_field(field, v)?;
            } else {
                unreachable!()
            }
        }
        rmpv::Value::F32(v) => {
            set_cfg_field(field, v)?;
        }
        rmpv::Value::F64(v) => {
            set_cfg_field(field, v)?;
        }
        rmpv::Value::String(v) => {
            let s = tlua::AnyLuaString(v.into_bytes());
            set_cfg_field(field, s)?;
        }
        rmpv::Value::Binary(v) => {
            let s = tlua::AnyLuaString(v);
            set_cfg_field(field, s)?;
        }
        rmpv::Value::Array { .. } => {
            unimplemented!("we don't need this for now");
        }
        rmpv::Value::Map { .. } => {
            unimplemented!("we don't need this for now");
        }
        rmpv::Value::Ext { .. } => {
            unimplemented!("we don't need this for now");
        }
    }

    Ok(())
}

#[inline]
pub fn set_cfg_field<T>(field: &str, value: T) -> Result<(), tlua::LuaError>
where
    T: tlua::PushOneInto<tlua::LuaState>,
    tlua::Void: From<T::Err>,
{
    set_cfg_fields(((field, value),))
}

pub fn set_cfg_fields<T>(table: T) -> Result<(), tlua::LuaError>
where
    tlua::AsTable<T>: tlua::PushInto<tlua::LuaState>,
{
    use tlua::Call;

    let l = lua_state();
    let b: LuaTable<_> = l.get("box").expect("can't fail under tarantool");
    let cfg: tlua::Callable<_> = b.get("cfg").expect("can't fail under tarantool");
    cfg.call_with(tlua::AsTable(table)).map_err(|e| match e {
        CallError::PushError(_) => unreachable!("cannot fail during push"),
        CallError::LuaError(e) => e,
    })
}

#[track_caller]
pub fn exec(code: &str) -> Result<(), LuaError> {
    let l = lua_state();
    l.exec(code)
}

#[track_caller]
pub fn eval<T>(code: &str) -> Result<T, LuaError>
where
    T: for<'l> LuaRead<PushGuard<LuaFunction<PushGuard<&'l LuaThread>>>>,
{
    let l = lua_state();
    l.eval(code)
}

/// Analogue of tarantool's `os.exit(code)`. Use this function if tarantool's
/// [`on_shutdown`] triggers must run. If instead you want to skip on_shutdown
/// triggers, use [`std::process::exit`] instead.
///
/// [`on_shutdown`]: ::tarantool::trigger::on_shutdown
pub fn exit(code: i32) -> ! {
    unsafe { ffi::tarantool_exit(code) }
    loop {
        fiber::fiber_yield()
    }
}

extern "C" {
    /// This variable need to replace the implemetation of function
    /// uses by xlog_remove_file() to removes an .xlog and .snap files.
    /// <https://git.picodata.io/picodata/tarantool/-/blob/2.11.2-picodata/src/box/xlog.c#L2145>
    ///
    /// In default implementation:
    /// On success, set the 'existed' flag to true if the file existed and was
    /// actually deleted or to false otherwise and returns 0. On failure, sets
    /// diag and returns -1.
    ///
    /// Note that default function didn't treat ENOENT as error and same behavior
    /// mostly recommended.
    pub static mut xlog_remove_file_impl: extern "C" fn(
        filename: *const std::os::raw::c_char,
        existed: &mut bool,
    ) -> std::os::raw::c_int;
}

/// Replaces default `tarantool`'s xlog removal implementation
///   with a one that will respect the setting shredding setting
///
/// Will panic if [`xlog_set_remove_file_impl`] was already called before
pub fn xlog_set_remove_file_impl(enable_shredding: bool) {
    static SHREDDING_INITIALIZED: AtomicBool = AtomicBool::new(false);

    if SHREDDING_INITIALIZED.swap(true, Ordering::Relaxed) {
        panic!("`xlog_set_remove_file_impl` was already called before")
    }

    if enable_shredding {
        crate::tlog!(Info, "shredding enabled");
        unsafe { xlog_remove_file_impl = xlog_shredding_remove_cb }
    } else {
        // keep the default shredding implementation
    }
}

extern "C" fn xlog_shredding_remove_cb(
    filename: *const std::os::raw::c_char,
    existed: &mut bool,
) -> std::os::raw::c_int {
    const OVERWRITE_COUNT: u32 = 6;
    const RENAME_COUNT: u32 = 4;

    let c_str = unsafe { std::ffi::CStr::from_ptr(filename) };
    let os_str = std::ffi::OsStr::from_bytes(c_str.to_bytes());
    let path: &std::path::Path = os_str.as_ref();

    let filename = path.display();
    crate::tlog!(Info, "shredding started for: {filename}");
    crate::audit!(
        message: "shredding started for {filename}",
        title: "shredding_started",
        severity: Low,
        filename: &filename,
    );

    let path_exists = path.exists();
    *existed = path_exists;

    if !path_exists {
        return 0;
    }

    let config = ShredConfig::<std::path::PathBuf>::non_interactive(
        vec![std::path::PathBuf::from(path)],
        Verbosity::Debug,
        crate::error_injection::is_enabled("KEEP_FILES_AFTER_SHREDDING"),
        OVERWRITE_COUNT,
        RENAME_COUNT,
    );

    return match shred(&config) {
        Ok(_) => {
            crate::tlog!(Info, "shredding finished for: {filename}");
            crate::audit!(
                message: "shredding finished for {filename}",
                title: "shredding_finished",
                severity: Low,
                filename: &filename,
            );
            0
        }
        Err(err) => {
            crate::tlog!(Error, "shredding failed due to: {err}");
            crate::audit!(
                message: "shredding failed for {filename}",
                title: "shredding_failed",
                severity: Low,
                error: &err,
                filename: &filename,
            );
            -1
        }
    };
}

pub fn rm_tarantool_files(
    instance_dir: impl AsRef<std::path::Path>,
) -> Result<(), tarantool::error::Error> {
    let entries = std::fs::read_dir(instance_dir)?;
    for entry in entries {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }

        let Some(ext) = path.extension() else {
            continue;
        };

        if ext != "xlog" && ext != "snap" {
            continue;
        }

        crate::tlog!(Info, "removing file: {}", path.display());
        std::fs::remove_file(path)?;
    }
    Ok(())
}

pub fn box_schema_version() -> u64 {
    mod ffi {
        extern "C" {
            pub fn box_schema_version() -> u64;
        }
    }

    // Safety: always safe
    unsafe { ffi::box_schema_version() }
}

#[inline(always)]
pub fn box_is_ro() -> bool {
    mod ffi {
        extern "C" {
            pub fn box_is_ro() -> bool;
        }
    }

    // SAFETY: always safe
    unsafe { ffi::box_is_ro() }
}

#[inline(always)]
pub fn box_ro_reason() -> Option<&'static str> {
    mod ffi {
        extern "C" {
            pub fn box_ro_reason() -> *const std::ffi::c_char;
        }
    }

    // SAFETY: always safe
    let ptr = unsafe { ffi::box_ro_reason() };
    if ptr.is_null() {
        // This means instance is not read-only
        return None;
    }

    // SAFETY: tarantool always returns a valid nul-terminated string
    let cstr = unsafe { std::ffi::CStr::from_ptr(ptr) };
    let res = cstr
        .to_str()
        .expect("tarantool always returns a valid utf-8 string");

    Some(res)
}

/// Return codes for IPROTO request handlers.
#[derive(Debug)]
#[repr(C)]
pub enum IprotoHandlerStatus {
    IPROTO_HANDLER_OK = 0,
    IPROTO_HANDLER_ERROR = 1,
    IPROTO_HANDLER_FALLBACK = 2,
}

/// Status of handlers of IPROTO requests when
/// request path of handling is overwritten.
type iproto_handler_status = IprotoHandlerStatus;

/// Type of callback for a IPROTO request handler.
type iproto_handler_t = extern "C" fn(
    header: *const u8,
    header_end: *const u8,
    body: *const u8,
    body_end: *const u8,
    ctx: *mut (),
) -> iproto_handler_status;

/// Type of destroy callback for a IPROTO request handler.
type iproto_handler_destroy_t = extern "C" fn(ctx: *mut ());

extern "C" {
    /// Sets an IPROTO request handler with the provided
    /// context for the given request type.
    pub fn box_iproto_override(
        req_type: u32,
        handler: Option<iproto_handler_t>,
        destroy: Option<iproto_handler_destroy_t>,
        ctx: *mut (),
    ) -> i32;

    /// Sends a packaet with the given header and body
    /// over the IPROTO sessions's socket.
    /// Returns -1 on error, 0 on success.
    pub fn box_iproto_send(
        sid: u64,
        header: *const u8,
        header_end: *const u8,
        body: *const u8,
        body_end: *const u8,
    ) -> i32;
}

/// Callback for overwritten handlers of unsupported IPROTO requests.
/// Sets diagnostic message and returns an error to register it.
pub(crate) extern "C" fn iproto_override_cb_unsupported_requests(
    _header: *const u8,
    _header_end: *const u8,
    _body: *const u8,
    _body_end: *const u8,
    ctx: *mut (),
) -> iproto_handler_status {
    // SAFETY: see guarantees on a caller side, that is [`tarantool::forbid_unsupported_iproto_requests`]
    let request_type = unsafe { *(ctx as *const () as *const crate::ForbiddenIprotoTypes) };
    ::tarantool::set_error!(
        ::tarantool::error::TarantoolErrorCode::Unsupported,
        "picodata does not support IPROTO_{} request type",
        request_type.variant_name()
    );
    iproto_handler_status::IPROTO_HANDLER_ERROR
}

/// Writes an unsigned integer to a `SmallVec` in a most efficient
/// way according to MessagePack specification. Used in scenarios,
/// when `RmpWrite` trait from `rmp` crate is not implemented for
/// used buffer type.
/// Do not use this function if you need to write MessagePack markers,
/// because they have to ignore primitive type byte prefix rule.
#[inline]
fn write_uint(src: u64, dest: &mut SmallVec<[u8; RESERVED_SIZE]>) {
    if src <= i8::MAX as u64 {
        dest.push(src as u8);
    } else {
        // pattern matching does not support type casts, so
        // we use maximums of sized integers in constants
        match src {
            0..=0xFF => dest.push(0xCC),
            0x0100..=0xFFFF => dest.push(0xCD),
            0x00010000..=0xFFFFFFFF => dest.push(0xCE),
            0x0000000100000000..=0xFFFFFFFFFFFFFFFF => dest.push(0xCF),
        }
        dest.extend_from_slice(&src.to_ne_bytes());
    }
}

// TODO(kbezuglyi): see [reduce collision probability for prepared statements](https://git.picodata.io/picodata/tarantool/-/issues/59)
// INFO: left as future implementation template
#[allow(unused)]
/// Handles prepared statement from IPROTO_EXECUTE request body.
#[inline]
fn iproto_execute_handle_prepared_statement<'p>(
    mut _body: &[u8],
    _port: &mut impl SqlPort<'p>,
) -> traft::Result<()> {
    let error_message = String::from("IPROTO_EXECUTE on prepared statements");
    let error_help = String::from("temporarily disabled");

    Err(traft::error::Error::Unsupported(
        traft::error::Unsupported::new(error_message, Some(error_help)),
    ))
}

/// Decodes an `IPROTO_EXECUTE` request body to extract and execute an SQL query.
/// Validates the presence of SQL_TEXT, then dispatches the query with parameters to the SQL engine.
/// Returns either the execution result tuple or an error if decoding fails or SQL_TEXT is missing.
/// See <https://www.tarantool.io/en/doc/2.11/dev_guide/internals/iproto/sql/#internals-iproto-sql>
/// on "SQL string" (p.s. prepared statements are handled with [`iproto_execute_handle_prepared_statement`]).
#[inline]
fn iproto_execute_handle_sql_query<'p>(
    body: &[u8],
    port: &mut impl SqlPort<'p>,
) -> traft::Result<()> {
    // this algorithm looks over the data with cursor and
    // decodes directly on a slice from data: that means
    // we use the cursor position to specify start of a
    // deserializable data (value of messagepack map entry)

    let mut pattern = None;
    let mut bind = None;

    let cur = &mut Cursor::new(body);

    // step 1: look over the map length
    let map_len = rmp::decode::read_map_len(cur)
        .map_err(|err| traft::error::Error::Tarantool(tarantool::error::Error::ValueRead(err)))?;
    // step 2: iterate over each entry of messagepack map
    for _ in 0..map_len {
        // step 3: look over the key of current map entry
        let diff_byte = rmp::decode::read_pfix(cur).map_err(|err| {
            traft::error::Error::Tarantool(tarantool::error::Error::ValueRead(err))
        })?;
        // step 4: take only the part of data where value of current map entry starts
        let mut remaining_data = &body[cur.position() as usize..];
        let old_len = remaining_data.len();
        // step 5: decode the value data part (or skip if unknown)
        match diff_byte {
            iproto_key::SQL_TEXT => {
                pattern = Some(
                    msgpack::Decode::decode(&mut remaining_data, &msgpack::Context::DEFAULT)
                        .map_err(tarantool::error::Error::MsgpackDecode)?,
                );
            }
            iproto_key::SQL_BIND => {
                bind = Some(
                    msgpack::Decode::decode(&mut remaining_data, &msgpack::Context::DEFAULT)
                        .map_err(tarantool::error::Error::MsgpackDecode)?,
                );
            }
            _ => msgpack::skip_value(cur)?,
        }
        // step 6: calculate the amount of decoded bytes to put cursor on the correct placement
        let displacement = old_len - remaining_data.len();
        cur.set_position(cur.position() + displacement as u64);
    }

    let Some(pattern) = pattern else {
        return Err(traft::error::Error::Tarantool(
            ::tarantool::error::Error::MsgpackDecode(msgpack::DecodeError::new::<u8>(
                "bad msgpack request body, missing sql pattern field",
            )),
        ));
    };

    let bind = bind.unwrap_or_default();
    crate::sql::parse_and_dispatch(pattern, bind, None, None, port)?;
    Ok(())
}

/// Returns swapped `MP_FIXSTR` representation of keys in [`tarantool::tuple::Tuple`]
/// with appropriate IPROTO keys specified by IPROTO in a new `Vec<u8>` of
/// correct MessagePack bytes. It is useful when we want to give back to an old-plain
/// Tarantool client IPROTO_EXECUTE response in a IPROTO-compatible format. Works
/// only for `SELECT` and `VALUES` request.
fn iproto_execute_rewrite_response_keys(
    tuple: &[u8],
    into: &mut SmallVec<[u8; RESERVED_SIZE]>,
) -> Result<(), ::tarantool::error::Error> {
    // we don't need the MP_FIXMAP byte, because it is always guaranteed
    let tuple = &tuple[1..];
    let cur = &mut Cursor::new(tuple);

    // following lines are written with
    // knowing that order of a key-values in a map
    // are guaranteed, the same as the map itself:
    // { "metadata": ..., "rows": ... }

    msgpack::skip_value(cur)?; // skip "metadata" key
    let metadata_start_pos = cur.position() as usize; // save start position
    msgpack::skip_value(cur)?; // skip metadata value to calculate offset
    let metadata_end_pos = cur.position() as usize; // save end position
    let metadata_bytes = &tuple[metadata_start_pos..metadata_end_pos];

    msgpack::skip_value(cur)?; // skip "rows" key
    let rows_start_pos = cur.position() as usize; // save start position
    msgpack::skip_value(cur)?; // skip rows value to calculate offset
    let rows_end_pos = cur.position() as usize; // save end position
    let rows_bytes = &tuple[rows_start_pos..rows_end_pos];

    // { IPROTO_METADATA(u8): MP_ARRAY(u32)
    //   IPROTO_DATA(u8): MP_ARRAY(u32) }
    // = MP_FIXMAP(u8)
    into.push(0x82 /* MP_FIXMAP(2) */);
    write_uint(0x32 /* IPROTO_METADATA */, into);
    into.extend_from_slice(metadata_bytes);
    write_uint(iproto_key::DATA as u64, into);
    into.extend_from_slice(rows_bytes);

    Ok(())
}

/// Recreates IPROTO-compliant body response from internal tuple request.
/// Works only for non-`SELECT` and non-`VALUES` requests.
fn iproto_execute_reformat_response(
    tuple: &[u8],
    into: &mut SmallVec<[u8; RESERVED_SIZE]>,
) -> Result<(), ::tarantool::error::Error> {
    // we don't need the MP_FIXMAP byte, because it is always guaranteed
    let tuple = &tuple[1..];
    let cur = &mut Cursor::new(tuple);

    // following lines are written with
    // knowing that order of a key-values in a map
    // are guaranteed, the same as the map itself:
    // { "row_count": ... }

    msgpack::skip_value(cur)?; // skip "row_count" key
    let count_start_pos = cur.position() as usize; // save start position
    msgpack::skip_value(cur)?; // skip row count value to calculate offset
    let count_end_pos = cur.position() as usize; // save end position
    let row_count_bytes = &tuple[count_start_pos..count_end_pos];

    // { SQL_INFO(u8): {
    //   SQL_INFO_ROW_COUNT(u8): MP_UINT(u32) } }
    // = MP_FIXMAP(u8)
    into.push(0x81 /* MP_FIXMAP(1) */);
    write_uint(iproto_key::SQL_INFO as u64, into);
    into.push(0x81 /* MP_FIXMAP(1) */);
    write_uint(0x00 /* SQL_INFO_ROW_COUNT */, into);
    into.extend_from_slice(row_count_bytes);

    Ok(())
}

const RESERVED_SIZE: usize = 21;

/// Handles valid response from `IPROTO_EXECUTE` request as SQL query or prepared statement.
fn iproto_execute_handle_valid_internal_response(
    header: &mut codec::Header,
    response_mp: &[u8],
) -> IprotoHandlerStatus {
    // { IPROTO_REQUEST_TYPE(u8): MP_POSFIXINT(u8),
    //   IPROTO_SYNC(u8): MP_UINT(u64),
    //   IPROTO_SCHEMA_VERSION(u8): MP_UINT(u64) }
    // = MP_FIXMAP(u8)
    let mut response_header: SmallVec<[u8; RESERVED_SIZE]> = SmallVec::new();
    response_header.push(0x83 /* MP_FIXMAP(3) */);
    write_uint(iproto_key::REQUEST_TYPE as u64, &mut response_header);
    write_uint(0x00 /* IPROTO_OK */, &mut response_header);
    write_uint(iproto_key::SYNC as u64, &mut response_header);
    write_uint(header.sync.next_index().get(), &mut response_header);
    write_uint(iproto_key::SCHEMA_VERSION as u64, &mut response_header);
    write_uint(header.schema_version, &mut response_header);

    let Range {
        start: response_header_start_ptr,
        end: response_header_end_ptr,
    } = response_header.as_ptr_range();

    // we skip first byte (MP_FIXARRAY) because `Tuple` by default
    // puts value into an array, that we can ignore, because it doesn't matter
    let response_data = &response_mp[1..];
    let mut response_body: SmallVec<[u8; RESERVED_SIZE]> = SmallVec::new();
    // either `SELECT` or `VALUES` (sub-)expression
    unwrap_or_report_and_propagate_err!(iproto_execute_rewrite_response_keys(
        response_data,
        &mut response_body
    )
    // neither `SELECT` nor `VALUES` (sub-)expression
    .or_else(|_| iproto_execute_reformat_response(response_data, &mut response_body)));

    let Range {
        start: response_body_start_ptr,
        end: response_body_end_ptr,
    } = response_body.as_ptr_range();

    // SAFETY: always safe
    let session_id = unsafe { tarantool::ffi::tarantool::box_session_id() };

    // SAFETY: function is properly exported, passed
    // pointers are valid data and aligned properly
    let res = unsafe {
        box_iproto_send(
            session_id,
            response_header_start_ptr,
            response_header_end_ptr,
            response_body_start_ptr,
            response_body_end_ptr,
        )
    };

    match res {
        -1 => IprotoHandlerStatus::IPROTO_HANDLER_ERROR,
        0 => IprotoHandlerStatus::IPROTO_HANDLER_OK,
        _ => unreachable!("Can't be more than 0 or less than -1 due to C FFI signature"),
    }
}

/// Returns a single byte as `u8` that represents either [`tarantool::network::protocol::codec::iproto_key::STMT_ID`]
/// or [`tarantool::network::protocol::codec::iproto_key::SQL_TEXT`] from `tarantool::network::protocol::codec::iproto_key` module.
pub fn iproto_execute_distinguish_type(body: &[u8]) -> Result<u8, ::tarantool::error::Error> {
    use rmp::Marker;

    let mut res = None;

    let cur = &mut Cursor::new(body);
    let map_len = rmp::decode::read_map_len(cur)?;
    for _ in 0..map_len {
        match Marker::from_u8(body[cur.position() as usize]) {
            Marker::FixPos(val) if val == iproto_key::STMT_ID || val == iproto_key::SQL_TEXT => {
                res = Some(val);
                break;
            }
            _ => msgpack::skip_value(cur)?,
        }
    }

    if let Some(res) = res {
        Ok(res)
    } else {
        Err(::tarantool::error::Error::MsgpackDecode(
            msgpack::DecodeError::new::<u8>("bad msgpack request body, missing request field"),
        ))
    }
}

/// Callback for overwritten handler for `IPROTO_EXECUTE`.
/// Redirects execute call to sbroad SQL implementation.
/// Sets diagnostic message on error, and sends a data back
/// to user on success, at return.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn iproto_override_cb_redirect_execute(
    header: *const u8,
    header_end: *const u8,
    body: *const u8,
    body_end: *const u8,
    _ctx: *mut (),
) -> iproto_handler_status {
    // SAFETY: pointers are pointing to valid data
    let header = unsafe {
        slice::from_raw_parts(
            header,
            header_end.offset_from(header) as usize / mem::size_of::<u8>(),
        )
    };

    // SAFETY: pointers are pointing to valid data
    let body = unsafe {
        slice::from_raw_parts(
            body,
            body_end.offset_from(body) as usize / mem::size_of::<u8>(),
        )
    };

    let difference_byte =
        unwrap_or_report_and_propagate_err!(iproto_execute_distinguish_type(body));

    let mut pico_port = PicoPortOwned::new();
    unwrap_or_report_and_propagate_err!(match difference_byte {
        iproto_key::STMT_ID => iproto_execute_handle_prepared_statement(body, &mut pico_port),
        iproto_key::SQL_TEXT => iproto_execute_handle_sql_query(body, &mut pico_port),
        _ => unreachable!("Can't reach that hand because it should error at no difference byte"),
    });
    let mut response_mp = Vec::new();
    unwrap_or_report_and_propagate_err!(
        dispatch_dump_mp(&mut response_mp, pico_port.port_c_mut()).map_err(TntError::IO)
    );

    let mut header =
        unwrap_or_report_and_propagate_err!(codec::Header::decode(&mut Cursor::new(header)));

    iproto_execute_handle_valid_internal_response(&mut header, &response_mp)
}

extern "C" {
    pub static mut use_system_alloc: extern "C" fn(space_id: u32) -> bool;
}

pub fn set_use_system_alloc() {
    unsafe { use_system_alloc = use_system_alloc_cb };
}

extern "C" fn use_system_alloc_cb(space_id: u32) -> bool {
    space_id <= crate::storage::SPACE_ID_INTERNAL_MAX
}

/// Set global cluster UUID for iproto connections (see IPROTO_ID).
/// As a side effect, tarantool's machinery will now validate
/// cluster UUIDs of all nodes trying to connect to this one.
///
/// XXX: this should only be called once, otherwise the process will abort!
pub fn init_cluster_uuid(uuid: ::uuid::Uuid) {
    extern "C" {
        fn iproto_set_cluster_uuid(uuid: *const tt_uuid) -> std::os::raw::c_int;
    }

    tlog!(Info, "enabling cluster_uuid in IPROTO_ID");
    let tt_uuid = ::tarantool::uuid::Uuid::from(uuid).to_tt_uuid();

    // SAFETY: we pass a valid tt_uuid by ref.
    let rc = unsafe { iproto_set_cluster_uuid(&tt_uuid) };
    assert_eq!(rc, 0);
}
