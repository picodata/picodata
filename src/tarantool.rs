use std::ffi::CStr;
use std::time::Duration;
use std::time::Instant;

use ::tarantool::fiber;
use ::tarantool::lua_state;
use ::tarantool::net_box;
use ::tarantool::tlua::{self, LuaFunction, LuaTable};
pub use ::tarantool::trigger::on_shutdown;
use ::tarantool::tuple::AsTuple;

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

        let _: unsafe extern "C" fn(FunctionCtx, FunctionArgs) -> c_int = $($func_name)+;
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
        $crate::tarantool::eval(concat!(
            "box.schema.func.create('.",
            $crate::stringify_last_token!($($func_name)+),
            "', {
                language = 'C',
                if_not_exists = true
            });"
        ));
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

#[derive(Clone, Debug, tlua::Push, tlua::LuaRead, PartialEq)]
pub struct Cfg {
    pub listen: Option<String>,
    pub read_only: bool,

    pub instance_uuid: Option<String>,
    pub replicaset_uuid: Option<String>,
    pub replication: Vec<String>,
    pub replication_connect_quorum: u8,

    pub wal_dir: String,
    pub memtx_dir: String,

    pub feedback_enabled: bool,
    pub log_level: u8,
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

            wal_dir: ".".into(),
            memtx_dir: ".".into(),

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

#[track_caller]
pub fn eval(code: &str) {
    let l = lua_state();
    l.exec(code).unwrap()
}

// fn net_box_repeat_call_until_succeed<Args, Res, Addr>(
pub fn net_box_call<Args, Res, Addr>(
    address: Addr,
    fn_name: &str,
    args: &Args,
    timeout: Duration,
) -> Result<Res, ::tarantool::error::Error>
where
    Args: AsTuple,
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

    tuple.into_struct::<((Res,),)>().map(|res| res.0 .0)
}

#[allow(dead_code)]
pub fn net_box_call_retry<Args, Res, Addr>(address: Addr, fn_name: &str, args: &Args) -> Res
where
    Args: AsTuple,
    Addr: std::net::ToSocketAddrs + std::fmt::Display + slog::Value,
    Res: serde::de::DeserializeOwned,
{
    loop {
        let timeout = Duration::from_millis(500);
        let now = Instant::now();
        match net_box_call(&address, fn_name, args, timeout) {
            Ok(v) => break v,
            Err(e) => {
                crate::tlog!(Warning, "net_box_call failed: {e}";
                    "peer" => &address,
                    "fn" => fn_name,
                );
                fiber::sleep(timeout.saturating_sub(now.elapsed()))
            }
        }
    }
}

#[inline]
pub fn net_box_call_or_log<Args, Res, Addr>(
    address: Addr,
    fn_name: &str,
    args: Args,
    timeout: Duration,
) -> Option<Res>
where
    Args: AsTuple,
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
