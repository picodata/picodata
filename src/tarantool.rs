use crate::config::ElectionMode;
use crate::config::PicodataConfig;
use crate::instance::Instance;
use crate::rpc::join;
use crate::schema::PICO_SERVICE_USER_NAME;
use crate::traft::error::Error;
use ::tarantool::fiber;
use ::tarantool::lua_state;
use ::tarantool::tlua::{self, LuaError, LuaFunction, LuaRead, LuaTable, LuaThread, PushGuard};
pub use ::tarantool::trigger::on_shutdown;
use file_shred::*;
use std::ffi::CStr;
use std::os::unix::ffi::OsStrExt;

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
macro_rules! stringify_cfunc {
    ( $($func_name:tt)+ ) => {{
        use ::tarantool::tuple::FunctionArgs;
        use ::tarantool::tuple::FunctionCtx;
        use libc::c_int;

        const _: unsafe extern "C" fn(FunctionCtx, FunctionArgs) -> c_int = $($func_name)+;
        concat!(".", $crate::stringify_last_token!($($func_name)+))
    }};
}

mod ffi {
    use libc::c_char;
    use libc::c_int;
    use libc::c_void;

    extern "C" {
        pub fn tarantool_version() -> *const c_char;
        pub fn tarantool_package() -> *const c_char;
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
}

/// Tarantool configuration.
/// See <https://www.tarantool.io/en/doc/latest/reference/configuration/#configuration-parameters>
#[derive(Clone, Debug, Default, tlua::Push, tlua::LuaRead, PartialEq, Eq)]
pub struct Cfg {
    pub instance_uuid: Option<String>,
    pub replicaset_uuid: Option<String>,

    pub listen: Option<String>,

    pub read_only: bool,
    pub replication: Vec<String>,
    pub replication_connect_quorum: u8,
    pub election_mode: ElectionMode,

    pub log: Option<String>,
    pub log_level: Option<u8>,

    pub wal_dir: String,
    pub memtx_dir: String,
    pub vinyl_dir: String,

    pub memtx_memory: u64,
}

impl Cfg {
    /// Temporary minimal configuration. After initializing with this
    /// configuration we either will go into discovery phase after which we will
    /// rebootstrap and go to the next phase (either boot or join), or if the
    /// storage is already initialize we will go into the post join phase.
    pub fn for_discovery(config: &PicodataConfig) -> Self {
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

            // If this is a restart, replication will be configured by governor
            // before our grade changes to Online.
            //
            // `replication_connect_quorum` is not configurable currently.
            replication_connect_quorum: 32,
            election_mode: ElectionMode::Off,

            ..Default::default()
        };

        res.set_core_parameters(config);
        res
    }

    /// Initial configuration for the cluster bootstrap phase.
    pub fn for_cluster_bootstrap(config: &PicodataConfig, leader: &Instance) -> Self {
        let mut res = Self {
            // At this point uuids must be valid, it will be impossible to
            // change them until the instance is expelled.
            instance_uuid: Some(leader.instance_uuid.clone()),
            replicaset_uuid: Some(leader.replicaset_uuid.clone()),

            // Listen port will be set after the global raft node is initialized.
            listen: None,

            // Must be writable, we're going to initialize the storage.
            read_only: false,

            // Replication will be configured by governor when another replica
            // joins.
            replication_connect_quorum: 32,
            election_mode: ElectionMode::Off,

            ..Default::default()
        };

        res.set_core_parameters(config);
        res
    }

    /// Initial configuration for the new instance joining to an already
    /// initialized cluster.
    pub fn for_instance_join(config: &PicodataConfig, resp: &join::Response) -> Self {
        let mut replication_cfg = Vec::with_capacity(resp.box_replication.len());
        let password = crate::pico_service::pico_service_password();
        for address in &resp.box_replication {
            replication_cfg.push(format!("{PICO_SERVICE_USER_NAME}:{password}@{address}"))
        }

        let mut res = Self {
            // At this point uuids must be valid, it will be impossible to
            // change them until the instance is expelled.
            instance_uuid: Some(resp.instance.instance_uuid.clone()),
            replicaset_uuid: Some(resp.instance.replicaset_uuid.clone()),

            // Needs to be set, because an applier will attempt to connect to
            // self and will block box.cfg() call until it succeeds.
            listen: Some(config.instance.listen().to_host_port()),

            // If we're joining to an existing replicaset,
            // then we're the follower.
            read_only: replication_cfg.len() > 1,

            // Always contains the current instance.
            replication: replication_cfg,

            replication_connect_quorum: 32,
            election_mode: ElectionMode::Off,

            ..Default::default()
        };

        res.set_core_parameters(config);
        res
    }

    pub fn set_core_parameters(&mut self, config: &PicodataConfig) {
        self.log = config.instance.log.clone();
        self.log_level = Some(config.instance.log_level() as _);

        self.wal_dir = config.instance.data_dir();
        self.memtx_dir = config.instance.data_dir();
        self.vinyl_dir = config.instance.data_dir();

        self.memtx_memory = config.instance.memtx_memory();
    }
}

pub fn cfg() -> Option<Cfg> {
    let l = lua_state();
    let b: LuaTable<_> = l.get("box")?;
    b.get("cfg")
}

#[track_caller]
pub fn set_cfg(cfg: &Cfg) -> Result<(), Error> {
    let l = lua_state();
    let box_cfg = LuaFunction::load(l, "return box.cfg(...)").unwrap();
    box_cfg.call_with_args(cfg)?;
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
    use tlua::{Call, CallError};

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
    unsafe { tarantool_exit(code) }

    loop {
        fiber::fiber_yield()
    }

    extern "C" {
        fn tarantool_exit(code: i32);
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
        existed: *mut bool,
    ) -> std::os::raw::c_int;
}

pub fn xlog_set_remove_file_impl() {
    unsafe { xlog_remove_file_impl = xlog_remove_cb };
}

extern "C" fn xlog_remove_cb(
    filename: *const std::os::raw::c_char,
    existed: *mut bool,
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
    unsafe { *existed = path_exists };

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
