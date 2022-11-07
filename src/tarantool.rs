use std::ffi::CStr;
use std::time::Duration;
use std::time::Instant;

use ::tarantool::fiber;
use ::tarantool::lua_state;
use ::tarantool::net_box;
use ::tarantool::tlua::{self, LuaError, LuaFunction, LuaRead, LuaTable, LuaThread, PushGuard};
pub use ::tarantool::trigger::on_shutdown;
use ::tarantool::tuple::ToTupleBuffer;

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

#[macro_export]
macro_rules! declare_cfunc {
    ( $($func_name:tt)+ ) => {{
        use ::tarantool::tuple::FunctionArgs;
        use ::tarantool::tuple::FunctionCtx;
        use libc::c_int;

        // This is needed to tell the compiler that the functions are really used.
        // Otherwise the functions get optimized out and disappear from the binary
        // and we may get this error: dlsym(RTLD_DEFAULT, some_fn): symbol not found
        fn used(_: unsafe extern "C" fn(FunctionCtx, FunctionArgs) -> c_int) {}

        used($($func_name)+);
        $crate::tarantool::exec(concat!(
            "box.schema.func.create('.",
            $crate::stringify_last_token!($($func_name)+),
            "', {
                language = 'C',
                if_not_exists = true
            });"
        )).unwrap();
    }};
}

#[macro_export]
macro_rules! cleanup_env {
    () => {
        // Tarantool implicitly parses some environment variables.
        // We don't want them to affect the behavior and thus filter them out.

        for (k, _) in std::env::vars() {
            if k.starts_with("TT_") || k.starts_with("TARANTOOL_") {
                std::env::remove_var(k)
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

inventory::submit!(crate::InnerTest {
    name: "test_version",
    body: || {
        let l = lua_state();
        let t: LuaTable<_> = l.eval("return require('tarantool')").unwrap();
        assert_eq!(version(), t.get::<String, _>("version").unwrap());
        assert_eq!(package(), t.get::<String, _>("package").unwrap());
    }
});

#[derive(Clone, Debug, tlua::Push, tlua::LuaRead, PartialEq, Eq)]
pub struct Cfg {
    pub listen: Option<String>,
    pub read_only: bool,

    pub instance_uuid: Option<String>,
    pub replicaset_uuid: Option<String>,
    pub replication: Vec<String>,
    pub replication_connect_quorum: u8,
    pub election_mode: CfgElectionMode,

    pub wal_dir: String,
    pub memtx_dir: String,

    pub memtx_memory: u64,

    pub feedback_enabled: bool,
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

            instance_uuid: None,
            replicaset_uuid: None,
            replication: vec![],
            replication_connect_quorum: 32,
            election_mode: CfgElectionMode::Off,

            wal_dir: ".".into(),
            memtx_dir: ".".into(),

            memtx_memory: 32 * 1024 * 1024,

            feedback_enabled: false,
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

// fn net_box_repeat_call_until_succeed<Args, Res, Addr>(
pub fn net_box_call<Args, Res, Addr>(
    address: Addr,
    fn_name: &str,
    args: &Args,
    timeout: Duration,
) -> Result<Res, ::tarantool::error::Error>
where
    Args: ToTupleBuffer,
    Addr: std::net::ToSocketAddrs + std::fmt::Display,
    Res: serde::de::DeserializeOwned,
{
    let now = Instant::now();

    let conn_opts = net_box::ConnOptions {
        connect_timeout: timeout,
        ..Default::default()
    };

    let conn = net_box::Conn::new(&address, conn_opts, None)?;

    let call_opts = net_box::Options {
        timeout: Some(timeout.saturating_sub(now.elapsed())),
        ..Default::default()
    };

    let tuple = conn
        .call(fn_name, args, &call_opts)?
        .expect("unexpected net_box result Ok(None)");

    tuple.decode().map(|((res,),)| res)
}

#[inline]
pub fn net_box_call_or_log<Args, Res, Addr>(
    address: Addr,
    fn_name: &str,
    args: Args,
    timeout: Duration,
) -> Option<Res>
where
    Args: ToTupleBuffer,
    Addr: std::net::ToSocketAddrs + std::fmt::Display + slog::Value,
    Res: serde::de::DeserializeOwned,
{
    match net_box_call(&address, fn_name, &args, timeout) {
        Ok(res) => Some(res),
        Err(e) => {
            crate::tlog!(Warning, "net_box_call failed: {e}";
                "peer" => &address,
                "fn" => fn_name,
            );
            None
        }
    }
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
