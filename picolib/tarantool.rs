use std::ffi::CStr;

use tarantool::global_lua;
use tarantool::tlua::{self, LuaFunction};

mod ffi {
    use libc::c_char;

    extern "C" {
        pub fn tarantool_version() -> *const c_char;
        pub fn tarantool_package() -> *const c_char;
    }
}

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
        let l = global_lua();
        let t: tlua::LuaTable<_> = l.eval("return require('tarantool')").unwrap();
        assert_eq!(version(), t.get::<String, _>("version").unwrap());
        assert_eq!(package(), t.get::<String, _>("package").unwrap());
    }
});

#[derive(Clone, Debug, tlua::Push, tlua::LuaRead, PartialEq)]
pub struct Cfg {
    pub listen: Option<String>,
    pub wal_dir: String,
    pub memtx_dir: String,
    pub feedback_enabled: bool,
}

impl Default for Cfg {
    fn default() -> Self {
        Self {
            listen: Some("3301".to_owned()),
            wal_dir: ".".to_owned(),
            memtx_dir: ".".to_owned(),
            feedback_enabled: false,
        }
    }
}

#[allow(dead_code)]
pub fn cfg() -> Option<Cfg> {
    let l = global_lua();
    let cfg: Result<Cfg, _> = l.eval("return box.cfg");
    match cfg {
        Ok(v) => Some(v),
        Err(_) => None,
    }
}

pub fn set_cfg(cfg: &Cfg) {
    let l = global_lua();
    let box_cfg = LuaFunction::load(l, "return box.cfg(...)").unwrap();
    box_cfg.call_with_args(cfg).unwrap()
}

pub fn eval(code: &str) {
    let l = global_lua();
    let f = LuaFunction::load(l, code).unwrap();
    f.call().unwrap()
}
