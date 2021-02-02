use std::ffi::CString;

use crate::ffi::lua as ffi_lua;

pub fn uid() -> Option<isize> {
    let result = unsafe {
        // Create new stack (just in case - in order no to mess things
        // in current stack).
        let state = ffi_lua::luaT_state();
        let uid_state = ffi_lua::lua_newthread(state);

        // Push box.session.uid function on the stack.
        let name_box = CString::new("box").unwrap();
        ffi_lua::lua_getglobal(uid_state, name_box.as_ptr());
        let name_session = CString::new("session").unwrap();
        ffi_lua::lua_getfield(uid_state, -1, name_session.as_ptr());
        let name_uid = CString::new("uid").unwrap();
        ffi_lua::lua_getfield(uid_state, -1, name_uid.as_ptr());

        let result = if ffi_lua::luaT_call(uid_state, 0, 1) == 1 {
            None
        } else {
            Some(ffi_lua::lua_tointeger(uid_state, -1))
        };

        // No need to clean uid_state. It will be gc'ed.
        result
    };

    result
}

pub fn euid() -> Option<isize> {
    let result = unsafe {
        // Create new stack (just in case - in order no to mess things
        // in current stack).
        let state = ffi_lua::luaT_state();
        let euid_state = ffi_lua::lua_newthread(state);

        // Push box.session.euid on the stack.
        let name = CString::new("box").unwrap();
        ffi_lua::lua_getglobal(euid_state, name.as_ptr());
        let name_session = CString::new("session").unwrap();
        ffi_lua::lua_getfield(euid_state, -1, name_session.as_ptr());
        let name_euid = CString::new("euid").unwrap();
        ffi_lua::lua_getfield(euid_state, -1, name_euid.as_ptr());

        let result = if ffi_lua::luaT_call(euid_state, 0, 1) == 1 {
            None
        } else {
            Some(ffi_lua::lua_tointeger(euid_state, -1))
        };

        // No need to clean euid_state. It will be gc'ed.
        result
    };

    result
}
