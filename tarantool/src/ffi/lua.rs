use std::os::raw::{c_int, c_schar};

use super::types::lua_State;

pub const LUA_GLOBALSINDEX: c_int = -10002;

#[inline(always)]
pub unsafe fn lua_getglobal(state: *mut lua_State, s: *const c_schar) {
    lua_getfield(state, LUA_GLOBALSINDEX, s);
}

extern "C" {
    pub fn lua_newthread(l: *mut lua_State) -> *mut lua_State;
    pub fn lua_pushstring(l: *mut lua_State, s: *const c_schar) -> *const c_schar;
    pub fn lua_tointeger(l: *mut lua_State, idx: c_int) -> isize;
    pub fn lua_tolstring(l: *mut lua_State, idx: c_int, len: *mut usize) -> *const c_schar;
    pub fn lua_getfield(L: *mut lua_State, idx: c_int, k: *const c_schar);
    pub fn lua_gettable(L: *mut lua_State, idx: c_int);
}
