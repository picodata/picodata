#![allow(non_camel_case_types)]
use std::os::raw::{c_double, c_int, c_schar};
use std::ptr::null_mut;

/// Module provides FFI bindings for the following constants,
/// types and functions, realted to Lua C API:
/// 1. Plain lua C API
/// 2. lauxlib
/// 3. Lua utitlites, implemented in Tarantool

pub const LUA_REGISTRYINDEX: c_int = -10000;
pub const LUA_ENVIRONINDEX: c_int = -10001;
pub const LUA_GLOBALSINDEX: c_int = -10002;

pub const LUA_OK: c_int = 0;
pub const LUA_YIELD: c_int = 1;
pub const LUA_ERRRUN: c_int = 2;
pub const LUA_ERRSYNTAX: c_int = 3;
pub const LUA_ERRMEM: c_int = 4;
pub const LUA_ERRERR: c_int = 5;

pub const LUA_TNONE: c_int = -1;

pub const LUA_TNIL: c_int = 0;
pub const LUA_TBOOLEAN: c_int = 1;
pub const LUA_TLIGHTUSERDATA: c_int = 2;
pub const LUA_TNUMBER: c_int = 3;
pub const LUA_TSTRING: c_int = 4;
pub const LUA_TTABLE: c_int = 5;
pub const LUA_TFUNCTION: c_int = 6;
pub const LUA_TUSERDATA: c_int = 7;
pub const LUA_TTHREAD: c_int = 8;

pub const LUA_MINSTACK: c_int = 20;

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

pub type lua_Number = libc::c_double;
pub type lua_Integer = libc::ptrdiff_t;

pub type lua_CFunction = unsafe extern "C" fn(l: *mut lua_State) -> c_int;

pub type lua_Alloc = extern "C" fn(
    ud: *mut libc::c_void,
    ptr: *mut libc::c_void,
    osize: libc::size_t,
    nsize: libc::size_t,
) -> *mut libc::c_void;

pub type lua_Reader = extern "C" fn(
    L: *mut lua_State,
    ud: *mut libc::c_void,
    sz: *mut libc::size_t,
) -> *const libc::c_char;

pub type lua_Writer = extern "C" fn(
    L: *mut lua_State,
    p: *const libc::c_void,
    sz: libc::size_t,
    ud: *mut libc::c_void,
) -> libc::c_int;

extern "C" {
    // Lua C API functions.
    pub fn lua_newstate(f: lua_Alloc, ud: *mut libc::c_void) -> *mut lua_State;
    pub fn lua_close(l: *mut lua_State);
    pub fn lua_newthread(l: *mut lua_State) -> *mut lua_State;

    pub fn lua_atpanic(l: *mut lua_State, panicf: lua_CFunction) -> lua_CFunction;

    pub fn lua_version(L: *mut lua_State) -> *const lua_Number;

    pub fn lua_gettop(l: *mut lua_State) -> c_int;
    pub fn lua_settop(l: *mut lua_State, idx: c_int);
    pub fn lua_pushboolean(l: *mut lua_State, n: c_int);
    pub fn lua_pushlstring(l: *mut lua_State, s: *const libc::c_char, l: libc::size_t);
    pub fn lua_pushstring(l: *mut lua_State, s: *const c_schar) -> *const c_schar;
    pub fn lua_pushinteger(l: *mut lua_State, n: isize);
    pub fn lua_pushnumber(l: *mut lua_State, n: c_double);
    pub fn lua_pushcclosure(l: *mut lua_State, fun: lua_CFunction, n: c_int);
    pub fn lua_pushnil(l: *mut lua_State);
    /// [-0, +1, -]
    ///
    /// Pushes a copy of the element at the given valid `index` onto the stack.
    pub fn lua_pushvalue(l: *mut lua_State, index: c_int);
    pub fn lua_tointeger(l: *mut lua_State, idx: c_int) -> isize;
    pub fn lua_toboolean(l: *mut lua_State, idx: c_int) -> c_int;
    pub fn lua_tolstring(l: *mut lua_State, idx: c_int, len: *mut usize) -> *const c_schar;
    pub fn lua_touserdata(l: *mut lua_State, idx: c_int) -> *mut libc::c_void;
    pub fn lua_setfield(l: *mut lua_State, idx: c_int, s: *const c_schar);
    pub fn lua_getfield(l: *mut lua_State, idx: c_int, s: *const c_schar);
    pub fn lua_createtable(l: *mut lua_State, narr: c_int, nrec: c_int);
    pub fn lua_newuserdata(l: *mut lua_State, sz: libc::size_t) -> *mut libc::c_void;
    /// [-1, +1, e]
    ///
    /// Pushes onto the stack the value `t[k]`, where `t` is the value at the
    /// given valid `index` and `k` is the value at the top of the stack.
    ///
    /// This function pops the key from the stack (putting the resulting value
    /// in its place). As in Lua, this function may trigger a metamethod for the
    /// "index" event
    pub fn lua_gettable(l: *mut lua_State, index: c_int);
    pub fn lua_settable(l: *mut lua_State, idx: c_int);
    pub fn lua_type(state: *mut lua_State, index: c_int) -> c_int;
    pub fn lua_setmetatable(l: *mut lua_State, objindex: c_int) -> c_int;
    pub fn lua_getmetatable(l: *mut lua_State, objindex: c_int) -> c_int;

    pub fn lua_tonumberx(l: *mut lua_State, idx: c_int, isnum: *mut c_int) -> lua_Number;
    pub fn lua_tointegerx(l: *mut lua_State, idx: c_int, isnum: *mut c_int) -> lua_Integer;

    pub fn lua_pcall(l: *mut lua_State, nargs: c_int, nresults: c_int, msgh: c_int) -> c_int;
    pub fn lua_load(l: *mut lua_State, reader: lua_Reader, dt: *mut libc::c_void, chunkname: *const libc::c_char, mode: *const libc::c_char) -> c_int;
    pub fn lua_dump(l: *mut lua_State, writer: lua_Writer, data: *mut libc::c_void) -> c_int;

    pub fn lua_error(l: *mut lua_State) -> c_int;
    pub fn lua_next(l: *mut lua_State, idx: c_int) -> c_int;
    pub fn lua_concat(l: *mut lua_State, n: c_int);
    pub fn lua_len(l: *mut lua_State, idx: c_int);

    pub fn lua_insert(l: *mut lua_State, idx: c_int);
    pub fn lua_remove(l: *mut lua_State, idx: c_int);

    pub fn luaopen_base(l: *mut lua_State);
    pub fn luaopen_bit(l: *mut lua_State);
    pub fn luaopen_debug(l: *mut lua_State);
    pub fn luaopen_io(l: *mut lua_State);
    pub fn luaopen_math(l: *mut lua_State);
    pub fn luaopen_os(l: *mut lua_State);
    pub fn luaopen_package(l: *mut lua_State);
    pub fn luaopen_string(l: *mut lua_State);
    pub fn luaopen_table(l: *mut lua_State);

    // lauxlib functions.
    pub fn luaL_newstate() -> *mut lua_State;
    pub fn luaL_register(l: *mut lua_State, libname: *const c_schar, lr: *const luaL_Reg);
    pub fn luaL_error(l: *mut lua_State, fmt: *const c_schar, ...) -> c_int;
    pub fn luaL_openlibs(L: *mut lua_State);
}

#[inline(always)]
pub unsafe fn lua_getglobal(state: *mut lua_State, s: *const c_schar) {
    lua_getfield(state, LUA_GLOBALSINDEX, s);
}

#[inline(always)]
pub unsafe fn lua_setglobal(state: *mut lua_State, s: *const c_schar) {
    lua_setfield(state, LUA_GLOBALSINDEX, s);
}

#[inline(always)]
pub unsafe fn lua_pop(state: *mut lua_State, n: c_int) {
    lua_settop(state, -n - 1);
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

#[inline(always)]
pub fn lua_upvalueindex(i: c_int) -> c_int {
    LUA_GLOBALSINDEX - i
}

#[inline(always)]
pub unsafe fn lua_isfunction(state: *mut lua_State, idx: c_int) -> bool {
    lua_type(state, idx) == LUA_TFUNCTION
}

#[inline(always)]
pub unsafe fn lua_istable(state: *mut lua_State, idx: c_int) -> bool {
    lua_type(state, idx) == LUA_TTABLE
}

#[inline(always)]
pub unsafe fn lua_islightuserdata(state: *mut lua_State, idx: c_int) -> bool {
    lua_type(state, idx) == LUA_TLIGHTUSERDATA
}

#[inline(always)]
pub unsafe fn lua_isnil(state: *mut lua_State, index: c_int) -> bool {
    lua_type(state, index) == LUA_TNIL
}

#[inline(always)]
pub unsafe fn lua_isboolean(state: *mut lua_State, idx: c_int) -> bool {
    lua_type(state, idx) == LUA_TBOOLEAN
}

#[inline(always)]
pub unsafe fn lua_isthread(state: *mut lua_State, idx: c_int) -> bool {
    lua_type(state, idx) == LUA_TTHREAD
}

#[inline(always)]
pub unsafe fn lua_isnone(state: *mut lua_State, idx: c_int) -> bool {
    lua_type(state, idx) == LUA_TNONE
}

#[inline(always)]
pub unsafe fn lua_isnoneornil(state: *mut lua_State, idx: c_int) -> bool {
    lua_type(state, idx) <= 0
}

#[inline(always)]
pub unsafe fn lua_pushglobaltable(state: *mut lua_State) {
    lua_pushvalue(state, LUA_GLOBALSINDEX)
}
