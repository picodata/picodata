#[macro_export]
macro_rules! implement_lua_push {
    ($ty:ty, $cb:expr) => {
        impl<'lua, L> $crate::Push<L> for $ty
        where
            L: $crate::AsLua<'lua>,
        {
            type Err = $crate::Void; // TODO: use ! instead
            #[inline]
            fn push_to_lua(&self, lua: L) -> Result<$crate::PushGuard<L>, ($crate::Void, L)> {
                Ok($crate::push_userdata(self, lua, $cb))
            }
        }

        impl<'lua, L> $crate::PushOne<L> for $ty where L: $crate::AsLua<'lua> {}
    };
}

#[macro_export]
macro_rules! implement_lua_read {
    ($ty:ty) => {
        impl<'s, 'c> tlua::LuaRead<&'c mut tlua::InsideCallback> for &'s mut $ty {
            #[inline]
            fn lua_read_at_position(
                lua: &'c mut tlua::InsideCallback,
                index: i32,
            ) -> Result<&'s mut $ty, &'c mut tlua::InsideCallback> {
                // FIXME:
                unsafe { ::std::mem::transmute($crate::read_userdata::<$ty>(lua, index)) }
            }
        }

        impl<'s, 'c> tlua::LuaRead<&'c mut tlua::InsideCallback> for &'s $ty {
            #[inline]
            fn lua_read_at_position(
                lua: &'c mut tlua::InsideCallback,
                index: i32,
            ) -> Result<&'s $ty, &'c mut tlua::InsideCallback> {
                // FIXME:
                unsafe { ::std::mem::transmute($crate::read_userdata::<$ty>(lua, index)) }
            }
        }

        impl<'s, 'b, 'c> tlua::LuaRead<&'b mut &'c mut tlua::InsideCallback> for &'s mut $ty {
            #[inline]
            fn lua_read_at_position(
                lua: &'b mut &'c mut tlua::InsideCallback,
                index: i32,
            ) -> Result<&'s mut $ty, &'b mut &'c mut tlua::InsideCallback> {
                let ptr_lua = lua as *mut &mut tlua::InsideCallback;
                let deref_lua = unsafe { ::std::ptr::read(ptr_lua) };
                let res = Self::lua_read_at_position(deref_lua, index);
                match res {
                    Ok(x) => Ok(x),
                    _ => Err(lua),
                }
            }
        }

        impl<'s, 'b, 'c> tlua::LuaRead<&'b mut &'c mut tlua::InsideCallback> for &'s $ty {
            #[inline]
            fn lua_read_at_position(
                lua: &'b mut &'c mut tlua::InsideCallback,
                index: i32,
            ) -> Result<&'s $ty, &'b mut &'c mut tlua::InsideCallback> {
                let ptr_lua = lua as *mut &mut tlua::InsideCallback;
                let deref_lua = unsafe { ::std::ptr::read(ptr_lua) };
                let res = Self::lua_read_at_position(deref_lua, index);
                match res {
                    Ok(x) => Ok(x),
                    _ => Err(lua),
                }
            }
        }
    };
}

#[macro_export]
macro_rules! c_str {
    ($s:literal) => {{
        fn f(b: &[u8]) -> &::std::ffi::CStr {
            unsafe { ::std::ffi::CStr::from_bytes_with_nul_unchecked(b) }
        }
        f(::std::concat!($s, "\0").as_bytes())
    }};
}

#[macro_export]
macro_rules! c_ptr {
    ($s:literal) => {
        $crate::c_str!($s).as_ptr().cast::<::std::os::raw::c_char>()
    };
}

/// Throws the lua error with the given message.
/// The first argument is the lua context in which the error should be thrown.
/// When throwing an error from a rust callback use the lua state
/// which was passed to the callback.
///
/// This macro will exit the current function so no code after it will be executed.
///
/// # Example
/// ```no_run
/// let lua = tlua::Lua::new();
/// lua.set("rust_callback_which_may_throw",
///     tlua::Function::new(|arg1: i32, arg2: String, lua: tlua::LuaState| {
///         // - `arg1` & `arg2` are passed by caller from lua
///         // - `lua` is a special argument inserted by tlua.
///         //    Only it should be used with `tlua::error!()`!
///         tlua::error!(lua, "invalid arguments: {arg1}, {arg2}");
///     }));
/// ```
#[macro_export]
macro_rules! error {
    ($l:expr, $($args:tt)+) => {{
        let msg = ::std::format!($($args)+);
        #[allow(unused_unsafe)]
        unsafe {
            let lua = $crate::AsLua::as_lua(&$l);
            $crate::ffi::lua_pushlstring(lua, msg.as_ptr() as _, msg.len());
            $crate::ffi::lua_error($crate::AsLua::as_lua(&$l));
        }
        unreachable!("luaL_error never returns")
    }};
}

#[macro_export]
macro_rules! unwrap_or {
    ($o:expr, $($else:tt)+) => {
        if let Some(v) = $o {
            v
        } else {
            $($else)+
        }
    }
}

#[macro_export]
macro_rules! unwrap_ok_or {
    ($r:expr, $err:pat => $($else:tt)+) => {
        match $r {
            Ok(v) => v,
            $err => $($else)+,
        }
    }
}

#[macro_export]
macro_rules! nzi32 {
    ($i:expr) => {
        #[allow(unused_unsafe)]
        {
            const _: () = assert!($i != 0, "NonZeroI32 cannot be equal to 0");
            unsafe { ::std::num::NonZeroI32::new_unchecked($i) }
        }
    };
}
