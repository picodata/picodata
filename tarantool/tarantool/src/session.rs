//! Box: session
//!
//! A session is an object associated with each client connection.
//! box.session submodule provides functions to query session state.
//!
//! See also:
//! - [Lua reference: Submodule box.session](https://www.tarantool.io/en/doc/1.10/reference/reference_lua/box_session/)
use std::ffi::CString;
use tlua::AsLua as _;

use crate::error::{Error, TarantoolError};
use crate::ffi::lua as ffi_lua;
use crate::ffi::tarantool::luaT_call;

/// Get the user ID of the current user.
pub fn uid() -> Result<isize, Error> {
    unsafe {
        // Create new stack (just in case - in order no to mess things
        // in current stack).
        let lua = crate::lua_state();
        let uid_state = lua.as_lua();

        // Push box.session.uid function on the stack.
        let name_box = CString::new("box").unwrap();
        ffi_lua::lua_getglobal(uid_state, name_box.as_ptr());
        let name_session = CString::new("session").unwrap();
        ffi_lua::lua_getfield(uid_state, -1, name_session.as_ptr());
        let name_uid = CString::new("uid").unwrap();
        ffi_lua::lua_getfield(uid_state, -1, name_uid.as_ptr());

        if luaT_call(uid_state, 0, 1) == 1 {
            Err(TarantoolError::last().into())
        } else {
            Ok(ffi_lua::lua_tointeger(uid_state, -1))
        }

        // No need to clean uid_state. It will be gc'ed.
    }
}

/// Get the effective user ID of the current user.
pub fn euid() -> Result<isize, Error> {
    unsafe {
        // Create new stack (just in case - in order no to mess things
        // in current stack).
        let lua = crate::lua_state();
        let euid_state = lua.as_lua();

        // Push box.session.euid on the stack.
        let name = CString::new("box").unwrap();
        ffi_lua::lua_getglobal(euid_state, name.as_ptr());
        let name_session = CString::new("session").unwrap();
        ffi_lua::lua_getfield(euid_state, -1, name_session.as_ptr());
        let name_euid = CString::new("euid").unwrap();
        ffi_lua::lua_getfield(euid_state, -1, name_euid.as_ptr());

        if luaT_call(euid_state, 0, 1) == 1 {
            Err(TarantoolError::last().into())
        } else {
            Ok(ffi_lua::lua_tointeger(euid_state, -1))
        }

        // No need to clean euid_state. It will be gc'ed.
    }
}
