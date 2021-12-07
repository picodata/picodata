use std::ffi::CStr;
use tarantool::hlua::{self, Lua, LuaFunction};

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

#[allow(non_snake_case)]
fn tarantool_L() -> Lua {
    unsafe {
        use tarantool::ffi::tarantool::luaT_state;
        Lua::from_existing_state(luaT_state(), false)
    }
}

#[derive(Clone, Debug, hlua::Push, hlua::LuaRead, PartialEq)]
pub struct Cfg {
    pub listen: String,
    pub wal_dir: String,
    pub memtx_dir: String,
}

impl Default for Cfg {
    fn default() -> Self {
        Self {
            listen: "3301".to_owned(),
            wal_dir: ".".to_owned(),
            memtx_dir: ".".to_owned(),
        }
    }
}

#[allow(dead_code)]
pub fn cfg() -> Option<Cfg> {
    let l = tarantool_L();
    let cfg: Result<Cfg, _> = l.execute("return box.cfg");
    match cfg {
        Ok(v) => Some(v),
        Err(_) => None,
    }
}

pub fn set_cfg(cfg: Cfg) {
    let l = tarantool_L();
    let box_cfg = LuaFunction::load(l, "box.cfg(...)").unwrap();
    box_cfg.call_with_args(cfg).unwrap()
}
