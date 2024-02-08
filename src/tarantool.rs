use file_shred::*;
use std::ffi::CStr;
use std::os::unix::ffi::OsStrExt;

use ::tarantool::fiber;
use ::tarantool::lua_state;
use ::tarantool::tlua::{self, LuaError, LuaFunction, LuaRead, LuaTable, LuaThread, PushGuard};
pub use ::tarantool::trigger::on_shutdown;

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
#[derive(Clone, Debug, tlua::Push, tlua::LuaRead, PartialEq, Eq)]
pub struct Cfg {
    pub listen: Option<String>,
    pub read_only: bool,
    pub log: Option<String>,

    pub instance_uuid: Option<String>,
    pub replicaset_uuid: Option<String>,
    pub replication: Vec<String>,
    pub replication_connect_quorum: u8,
    pub election_mode: CfgElectionMode,

    pub wal_dir: String,
    pub memtx_dir: String,
    pub vinyl_dir: String,

    pub memtx_memory: u64,

    pub log_level: u8,
}

#[derive(Clone, Debug, tlua::Push, tlua::LuaRead, PartialEq, Eq)]
pub enum CfgElectionMode {
    Off,
    Voter,
    Candidate,
    Manual,
}

impl Default for Cfg {
    fn default() -> Self {
        Self {
            listen: Some("3301".into()),
            read_only: true,
            log: None,

            instance_uuid: None,
            replicaset_uuid: None,
            replication: vec![],
            replication_connect_quorum: 32,
            election_mode: CfgElectionMode::Off,

            wal_dir: ".".into(),
            memtx_dir: ".".into(),
            vinyl_dir: ".".into(),

            // Effectively this is the minimum value. Values less than 64MB will be rounded up to 64MB.
            // See memtx_engine_set_memory
            memtx_memory: 64 * 1024 * 1024,

            log_level: tarantool::log::SayLevel::Info as u8,
        }
    }
}

pub fn cfg() -> Option<Cfg> {
    let l = lua_state();
    let b: LuaTable<_> = l.get("box")?;
    b.get("cfg")
}

pub fn set_cfg(cfg: &Cfg) {
    let l = lua_state();
    let box_cfg = LuaFunction::load(l, "return box.cfg(...)").unwrap();
    box_cfg.call_with_args(cfg).unwrap()
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
