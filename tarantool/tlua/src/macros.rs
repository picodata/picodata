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
macro_rules! c_ptr {
    ($s:literal) => {
        ::std::concat!($s, "\0").as_ptr().cast::<i8>()
    };
}

#[macro_export]
macro_rules! lua_error {
    ($l:expr, $msg:literal) => {
        {
            $crate::ffi::luaL_error($l, $crate::c_ptr!($msg));
            unreachable!()
        }
    }
}

