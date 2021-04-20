#![allow(non_camel_case_types)]
use std::os::raw::{c_double, c_int, c_schar};
use std::ptr::{null, null_mut};

/// Module provides FFI bindings for the following constants,
/// types and functions, realted to Lua C API:
/// 1. Plain lua C API
/// 2. lauxlib
/// 3. Lua utitlites, implemented in Tarantool

pub const LUA_GLOBALSINDEX: c_int = -10002;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct lua_State {
    pub _unused: [u8; 0],
}

#[repr(C)]
pub struct luaL_Reg {
    pub name: *const c_schar,
    pub func: lua_CFunction,
}

impl luaL_Reg {
    pub fn end() -> Self {
        luaL_Reg {
            name: null(),
            func: None,
        }
    }
}

pub type lua_CFunction = Option<unsafe extern "C" fn(l: *mut lua_State) -> c_int>;

extern "C" {
    // Lua C API functions.
    pub fn lua_newthread(l: *mut lua_State) -> *mut lua_State;
    pub fn lua_gettop(l: *mut lua_State) -> c_int;
    pub fn lua_settop(l: *mut lua_State, idx: c_int);
    pub fn lua_pushboolean(l: *mut lua_State, n: c_int);
    pub fn lua_pushstring(l: *mut lua_State, s: *const c_schar) -> *const c_schar;
    pub fn lua_pushinteger(l: *mut lua_State, n: isize);
    pub fn lua_pushnumber(l: *mut lua_State, n: c_double);
    pub fn lua_pushcclosure(l: *mut lua_State, fun: lua_CFunction, n: c_int);
    pub fn lua_pushnil(l: *mut lua_State);
    pub fn lua_pushvalue(l: *mut lua_State, idx: c_int);
    pub fn lua_tointeger(l: *mut lua_State, idx: c_int) -> isize;
    pub fn lua_tolstring(l: *mut lua_State, idx: c_int, len: *mut usize) -> *const c_schar;
    pub fn lua_setfield(l: *mut lua_State, idx: c_int, s: *const c_schar);
    pub fn lua_getfield(l: *mut lua_State, idx: c_int, s: *const c_schar);
    pub fn lua_createtable(l: *mut lua_State, narr: c_int, nrec: c_int);
    pub fn lua_gettable(l: *mut lua_State, idx: c_int);
    pub fn lua_settable(l: *mut lua_State, idx: c_int);
    pub fn lua_remove(l: *mut lua_State, idx: c_int);

    // lauxlib functions.
    pub fn luaL_register(l: *mut lua_State, libname: *const c_schar, lr: *const luaL_Reg);
    pub fn luaL_error(l: *mut lua_State, fmt: *const c_schar, ...) -> c_int;

    // Lua Tarantool util functios.
    pub fn luaT_state() -> *mut lua_State;
    pub fn luaT_call(l: *mut lua_State, nargs: c_int, nreturns: c_int) -> isize;
}

#[inline(always)]
pub unsafe fn lua_getglobal(state: *mut lua_State, s: *const c_schar) {
    lua_getfield(state, LUA_GLOBALSINDEX, s);
}

#[inline(always)]
pub unsafe fn lua_pushcfunction(state: *mut lua_State, f: lua_CFunction) {
    lua_pushcclosure(state, f, 0);
}

#[inline(always)]
pub unsafe fn lua_tostring(state: *mut lua_State, i: c_int) -> *const c_schar {
    lua_tolstring(state, i, null_mut())
}

#[inline(always)]
pub unsafe fn lua_newtable(state: *mut lua_State) {
    lua_createtable(state, 0, 0);
}
