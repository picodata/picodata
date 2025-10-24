//! Box: session
//!
//! A session is an object associated with each client connection.
//! box.session submodule provides functions to query session state.
//!
//! See also:
//! - [Lua reference: Submodule box.session](https://www.tarantool.io/en/doc/1.10/reference/reference_lua/box_session/)

pub type UserId = u32;

#[cfg(not(feature = "picodata"))]
mod vanilla {
    use std::convert::TryFrom;
    use std::ffi::CString;
    use tlua::{AsLua as _, LuaError};

    use crate::error::{Error, TarantoolError};
    use crate::ffi::lua as ffi_lua;
    use crate::ffi::tarantool::luaT_call;

    use super::UserId;

    fn user_id_from_lua(id: isize) -> UserId {
        // id in box.space._user has type unsigned
        // user_by_id in user.h uses uint32_t as parameter type
        u32::try_from(id).expect("user id is always valid u32")
    }

    /// Get the user ID from the current session.
    pub fn uid() -> Result<UserId, Error> {
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
                Ok(user_id_from_lua(ffi_lua::lua_tointeger(uid_state, -1)))
            }

            // No need to clean uid_state. It will be gc'ed.
        }
    }

    /// Get the effective user ID of the current session.
    pub fn euid() -> Result<UserId, Error> {
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
                Ok(user_id_from_lua(ffi_lua::lua_tointeger(euid_state, -1)))
            }

            // No need to clean euid_state. It will be gc'ed.
        }
    }

    pub(super) fn su_impl(uid: UserId) -> Result<(), Error> {
        let lua = crate::lua_state();
        lua.exec_with("box.session.su(...)", uid)
            .map_err(LuaError::from)?;

        Ok(())
    }
}

#[cfg(feature = "picodata")]
mod picodata {
    use crate::{
        error::{Error, TarantoolError},
        ffi::tarantool::{
            box_effective_user_id, box_session_su, box_session_user_id, box_user_id_by_name,
        },
    };

    use super::UserId;

    /// Get the user ID of the current user.
    #[inline]
    pub fn uid() -> Result<UserId, Error> {
        unsafe {
            let mut ret: u32 = 0;
            let err = box_session_user_id(&mut ret);
            if err < 0 {
                return Err(Error::Tarantool(TarantoolError::last()));
            }
            Ok(ret)
        }
    }

    /// Get the effective user ID of the current user.
    #[inline]
    pub fn euid() -> Result<UserId, Error> {
        // In picodata this is actually infallible.
        unsafe { Ok(box_effective_user_id()) }
    }

    pub(super) fn su_impl(uid: UserId) -> Result<(), Error> {
        let err = unsafe { box_session_su(uid) };
        if err < 0 {
            return Err(Error::Tarantool(TarantoolError::last()));
        }
        Ok(())
    }

    #[inline]
    pub fn user_id_by_name(name: &str) -> Result<UserId, Error> {
        let name_range = name.as_bytes().as_ptr_range();
        let mut uid: u32 = 0;
        let err = unsafe {
            box_user_id_by_name(name_range.start.cast(), name_range.end.cast(), &mut uid)
        };
        if err < 0 {
            return Err(Error::Tarantool(TarantoolError::last()));
        }
        Ok(uid)
    }
}

use crate::error::Error;

#[cfg(feature = "picodata")]
pub use picodata::*;

#[cfg(not(feature = "picodata"))]
pub use vanilla::*;

pub struct SuGuard {
    pub original_user_id: UserId,
}

impl Drop for SuGuard {
    fn drop(&mut self) {
        su_impl(self.original_user_id).expect("failed to switch back to original user");
    }
}

#[inline]
pub fn su(target_uid: UserId) -> Result<SuGuard, Error> {
    let original_user_id = uid().expect("infallible with c api");
    su_impl(target_uid)?;

    Ok(SuGuard { original_user_id })
}

#[inline]
pub fn with_su<T>(uid: UserId, f: impl FnOnce() -> T) -> Result<T, Error> {
    let _su = su(uid)?;
    Ok(f())
}
