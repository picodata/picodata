use std::any::{Any, TypeId};
use std::marker::PhantomData;
use std::num::NonZeroI32;
use std::ops::{Deref, DerefMut};
use std::mem;
use std::ptr;

use crate::{
    AsLua,
    Push,
    PushGuard,
    LuaRead,
    LuaState,
    InsideCallback,
    LuaTable,
    c_ptr,
};

/// Pushes `value` of type `T` onto the stack as a userdata. The value is
/// put inside a `Option` so that it can be safely moved out of there. Useful
/// for example when passing `FnOnce` as a c closure, because it must be dropped
/// after the call.
/// *[0, +1, -]*
pub unsafe fn push_some_userdata<T>(lua: *mut ffi::lua_State, value: T) {
    type UDBox<T> = Option<T>;
    let ud_ptr = ffi::lua_newuserdata(lua, std::mem::size_of::<UDBox<T>>());
    std::ptr::write(ud_ptr as *mut UDBox<T>, Some(value));

    if std::mem::needs_drop::<T>() {
        // Creating a metatable.
        ffi::lua_newtable(lua);

        // Index "__gc" in the metatable calls the object's destructor.
        ffi::lua_pushstring(lua, c_ptr!("__gc"));
        ffi::lua_pushcfunction(lua, wrap_gc::<T>);
        ffi::lua_settable(lua, -3);

        ffi::lua_setmetatable(lua, -2);
    }

    /// A callback for the "__gc" event. It checks if the value was moved out
    /// and if not it drops the value.
    unsafe extern "C" fn wrap_gc<T>(lua: *mut ffi::lua_State) -> i32 {
        let ud_ptr = ffi::lua_touserdata(lua, 1);
        let ud = (ud_ptr as *mut UDBox<T>)
            .as_mut()
            .expect("__gc called with userdata pointing to NULL");
        drop(ud.take());

        0
    }
}


// Called when an object inside Lua is being dropped.
#[inline]
extern "C" fn destructor_wrapper<T>(lua: *mut ffi::lua_State) -> libc::c_int {
    unsafe {
        let obj = ffi::lua_touserdata(lua, -1);
        ptr::drop_in_place(obj as *mut TypeId);
        ptr::drop_in_place((obj as *mut u8).offset(mem::size_of::<TypeId>() as isize) as *mut T);
        0
    }
}

/// Pushes an object as a user data.
///
/// In Lua, a user data is anything that is not recognized by Lua. When the script attempts to
/// copy a user data, instead only a reference to the data is copied.
///
/// The way a Lua script can use the user data depends on the content of the **metatable**, which
/// is a Lua table linked to the object.
///
/// [See this link for more infos.](http://www.lua.org/manual/5.2/manual.html#2.4)
///
/// # About the Drop trait
///
/// When the Lua context detects that a userdata is no longer needed it calls the function at the
/// `__gc` index in the userdata's metatable, if any. The hlua library will automatically fill this
/// index with a function that invokes the `Drop` trait of the userdata.
///
/// You can replace the function if you wish so, although you are strongly discouraged to do it.
/// It is no unsafe to leak data in Rust, so there is no safety issue in doing so.
///
/// # Arguments
///
///  - `metatable`: Function that fills the metatable of the object.
///
#[inline]
pub fn push_userdata<L, T, F>(data: T, lua: L, metatable: F) -> PushGuard<L>
where
    F: for<'a> FnOnce(LuaTable<&'a PushGuard<L>>),
    L: AsLua,
    T: Send + 'static + Any,
{
    unsafe {
        let typeid = TypeId::of::<T>();

        let lua_data = {
            let tot_size = mem::size_of_val(&typeid) + mem::size_of_val(&data);
            ffi::lua_newuserdata(lua.as_lua(), tot_size as libc::size_t)
        };

        // We check the alignment requirements.
        debug_assert_eq!(lua_data as usize % mem::align_of_val(&data), 0);
        // Since the size of a `TypeId` should always be a usize, this assert should pass every
        // time as well.
        debug_assert_eq!(mem::size_of_val(&typeid) % mem::align_of_val(&data), 0);

        // We write the `TypeId` first, and the data right next to it.
        ptr::write(lua_data as *mut TypeId, typeid);
        let data_loc = (lua_data as *const u8).offset(mem::size_of_val(&typeid) as isize);
        ptr::write(data_loc as *mut _, data);

        // Creating a metatable.
        ffi::lua_newtable(lua.as_lua());

        // Index "__gc" in the metatable calls the object's destructor.

        // TODO: Could use std::intrinsics::needs_drop to avoid that if not needed.
        // After some discussion on IRC, it would be acceptable to add a reexport in libcore
        // without going through the RFC process.
        {
            match "__gc".push_to_lua(lua.as_lua()) {
                Ok(p) => p.forget(),
                Err(_) => unreachable!(),
            };

            ffi::lua_pushcfunction(lua.as_lua(), destructor_wrapper::<T>);
            ffi::lua_settable(lua.as_lua(), -3);
        }

        let lua_state = lua.as_lua();

        // Calling the metatable closure.
        let guard = PushGuard::new(lua, 1);
        metatable(LuaTable::lua_read(&guard).ok().unwrap());

        ffi::lua_setmetatable(lua_state, -2);

        guard
    }
}

#[inline]
pub fn read_userdata<'t, 'c, T>(lua: &'c InsideCallback, index: i32)
    -> Result<&'t mut T, &'c InsideCallback>
where
    T: 'static + Any,
{
    unsafe {
        let data_ptr = ffi::lua_touserdata(lua.as_lua(), index);
        if data_ptr.is_null() {
            return Err(lua);
        }

        let actual_typeid = data_ptr as *const TypeId;
        if *actual_typeid != TypeId::of::<T>() {
            return Err(lua);
        }

        let data = (data_ptr as *const u8).offset(mem::size_of::<TypeId>() as isize);
        Ok(&mut *(data as *mut T))
    }
}

/// Represents a user data located inside the Lua context.
#[derive(Debug)]
pub struct UserdataOnStack<T, L> {
    variable: L,
    index: NonZeroI32,
    marker: PhantomData<T>,
}

impl<T, L> LuaRead<L> for UserdataOnStack<T, L>
where
    L: AsLua,
    T: Any,
{
    #[inline]
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<UserdataOnStack<T, L>, L> {
        unsafe {
            let data_ptr = ffi::lua_touserdata(lua.as_lua(), index.into());
            if data_ptr.is_null() {
                return Err(lua);
            }

            let actual_typeid = data_ptr as *const TypeId;
            if *actual_typeid != TypeId::of::<T>() {
                return Err(lua);
            }

            Ok(UserdataOnStack {
                variable: lua,
                index: index,
                marker: PhantomData,
            })
        }
    }
}

impl<T, L> AsLua for UserdataOnStack<T, L>
where
    L: AsLua,
    T: Any,
{
    #[inline]
    fn as_lua(&self) -> LuaState {
        self.variable.as_lua()
    }
}

impl<T, L> Deref for UserdataOnStack<T, L>
where
    L: AsLua,
    T: Any,
{
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        unsafe {
            let base = ffi::lua_touserdata(self.variable.as_lua(), self.index.into());
            let data = (base as *const u8).offset(mem::size_of::<TypeId>() as _);
            &*(data as *const T)
        }
    }
}

impl<T, L> DerefMut for UserdataOnStack<T, L>
where
    L: AsLua,
    T: Any,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        unsafe {
            let base = ffi::lua_touserdata(self.variable.as_lua(), self.index.into());
            let data = (base as *const u8).offset(mem::size_of::<TypeId>() as _);
            &mut *(data as *mut T)
        }
    }
}
