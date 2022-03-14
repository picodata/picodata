#[macro_export]
macro_rules! implement_lua_push {
    ($ty:ty, $cb:expr) => {
        impl<'lua, L> $crate::Push<L> for $ty where L: $crate::AsLua<'lua> {
            type Err = $crate::Void;      // TODO: use ! instead
            #[inline]
            fn push_to_lua(&self, lua: L) -> Result<$crate::PushGuard<L>, ($crate::Void, L)> {
                Ok($crate::push_userdata(self, lua, $cb))
            }
        }
        
        impl<'lua, L> $crate::PushOne<L> for $ty where L: $crate::AsLua<'lua> {
        }
    };
}

#[macro_export]
macro_rules! implement_lua_read {
    ($ty:ty) => {
        impl<'s, 'c> tlua::LuaRead<&'c mut tlua::InsideCallback> for &'s mut $ty {
            #[inline]
            fn lua_read_at_position(lua: &'c mut tlua::InsideCallback, index: i32) -> Result<&'s mut $ty, &'c mut tlua::InsideCallback> {
                // FIXME:
                unsafe { ::std::mem::transmute($crate::read_userdata::<$ty>(lua, index)) }
            }
        }

        impl<'s, 'c> tlua::LuaRead<&'c mut tlua::InsideCallback> for &'s $ty {
            #[inline]
            fn lua_read_at_position(lua: &'c mut tlua::InsideCallback, index: i32) -> Result<&'s $ty, &'c mut tlua::InsideCallback> {
                // FIXME:
                unsafe { ::std::mem::transmute($crate::read_userdata::<$ty>(lua, index)) }
            }
        }

        impl<'s, 'b, 'c> tlua::LuaRead<&'b mut &'c mut tlua::InsideCallback> for &'s mut $ty {
            #[inline]
            fn lua_read_at_position(lua: &'b mut &'c mut tlua::InsideCallback, index: i32) -> Result<&'s mut $ty, &'b mut &'c mut tlua::InsideCallback> {
                let ptr_lua = lua as *mut &mut tlua::InsideCallback;
                let deref_lua = unsafe { ::std::ptr::read(ptr_lua) };
                let res = Self::lua_read_at_position(deref_lua, index);
                match res {
                    Ok(x) => Ok(x),
                    _ => Err(lua)
                }
            }
        }

        impl<'s, 'b, 'c> tlua::LuaRead<&'b mut &'c mut tlua::InsideCallback> for &'s $ty {
            #[inline]
            fn lua_read_at_position(lua: &'b mut &'c mut tlua::InsideCallback, index: i32) -> Result<&'s $ty, &'b mut &'c mut tlua::InsideCallback> {
                let ptr_lua = lua as *mut &mut tlua::InsideCallback;
                let deref_lua = unsafe { ::std::ptr::read(ptr_lua) };
                let res = Self::lua_read_at_position(deref_lua, index);
                match res {
                    Ok(x) => Ok(x),
                    _ => Err(lua)
                }
            }
        }
    };
}

#[macro_export]
macro_rules! c_str {
    ($s:literal) => {
        {
            fn f(b: &[u8]) -> &::std::ffi::CStr {
                unsafe { ::std::ffi::CStr::from_bytes_with_nul_unchecked(b) }
            }
            f(::std::concat!($s, "\0").as_bytes())
        }
    };
}

#[macro_export]
macro_rules! c_ptr {
    ($s:literal) => {
        $crate::c_str!($s).as_ptr().cast::<::std::os::raw::c_char>()
    };
}

#[macro_export]
macro_rules! error {
    ($l:expr, $msg:literal) => {
        $crate::error!(@impl $l, $crate::error!(@locz $msg).as_ptr().cast())
    };
    ($l:expr, $f:literal $($args:tt)*) => {
        {
            let msg = ::std::format!(::std::concat![$f, "\0"] $($args)*);
            $crate::error!(@impl $l, $crate::c_ptr!("%s"), msg.as_ptr())
        }
    };
    (@locz $f:literal) => {
        ::std::concat![
            ::std::file!(), ":", ::std::line!(), ":", ::std::column!(), "> ", $f, "\0"
        ]
    };
    (@impl $l:expr, $($args:tt)+) => {
        {
            unsafe {
                $crate::ffi::luaL_error($crate::AsLua::as_lua(&$l), $($args)+);
            }
            unreachable!("luaL_error never returns")
        }
    }
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
macro_rules! nzi32 {
    ($i:literal) => {
        {
            const _: () = assert!($i != 0, "NonZeroI32 cannot be equal to 0");
            unsafe { ::std::num::NonZeroI32::new_unchecked($i) }
        }
    }
}

