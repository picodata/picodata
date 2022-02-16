use std::ffi::CStr;

use ::tarantool::lua_state;
use ::tarantool::tlua::{self, LuaFunction, LuaTable};

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
        let l = lua_state();
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
            listen: Some("3301".into()),
            wal_dir: ".".into(),
            memtx_dir: ".".into(),
            feedback_enabled: false,
        }
    }
}

#[allow(dead_code)]
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

pub fn eval(code: &str) {
    let l = lua_state();
    l.exec(code).unwrap()
}
