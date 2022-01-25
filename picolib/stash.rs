//! Tarantool-flavored singleton pattern implementation.
//!
//! Tarantool has a peculiarity - when calling stored procedures with `{language = "C"}` option, it
//! disregards the shared object (`.so` / `.dylib`) already loaded by Lua and makes the second
//! independent `dlopen`. As a result, Lua and C stored procedures can't share state, because even
//! static variables point to different memory locations.
//!
//! As a workaround, this module provides the API for hiding the state inside `lua_State`. Under the
//! hood, the stash consumes a custom struct and leaks the wrapping box. Inside Lua, it's
//! represented by a userdata, which the `access()` function provides access to.
//!
//! Example:
//!
//! ```
//! use std::cell::Ref;
//! use std::cell::RefCell;
//!
//! /// The particular singleton structure
//! #[derive(Default)]
//! struct Stash {
//!     x: RefCell<u32>,
//! }
//!
//! unsafe impl Send for Stash {}
//! unsafe impl Sync for Stash {}
//! impl Stash {
//!     pub fn access() -> &'static Self {
//!         stash::access("my_stash")
//!     }
//!     pub fn set_x(&self, x: u32) {
//!         *self.x.borrow_mut() = x;
//!     }
//!     pub fn x(&self) -> Ref<u32> {
//!         self.x.borrow()
//!     }
//! }
//!
//! #[no_mangle]
//! pub extern "C" fn luaopen_easy(_l: std::ffi::c_void) -> std::os::raw::c_int {
//!     // Tarantool calls this function upon require("easy")
//!     let stash = Stash::access();
//!     stash.set_x(100);
//!     assert_eq!(*stash.x(), 100);
//!     0
//! }
//! ```
//!

use ::tarantool::tlua;
use std::ops::Deref;

#[derive(Default)]
struct Inner<S>(S);

impl<S> std::ops::Deref for Inner<S> {
    type Target = S;

    #[inline]
    fn deref(&self) -> &S {
        &self.0
    }
}

impl<L, S> tlua::PushInto<L> for Inner<S>
where
    L: tlua::AsLua,
    S: 'static,
{
    type Err = tlua::Void;
    fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (tlua::Void, L)> {
        let boxed = Box::new(self);
        let ptr: &'static Inner<S> = Box::leak(boxed);
        Ok(tlua::push_userdata(ptr, lua, |_| {}))
    }
}

impl<L, S> tlua::PushOneInto<L> for Inner<S>
where
    L: tlua::AsLua,
    S: 'static,
{
}

impl<L, S> tlua::LuaRead<L> for &Inner<S>
where
    L: tlua::AsLua,
{
    fn lua_read_at_position(lua: L, index: std::num::NonZeroI32) -> Result<&'static Inner<S>, L> {
        let val: tlua::UserdataOnStack<&Inner<S>, _> =
            tlua::LuaRead::lua_read_at_position(lua, index)?;
        Ok(val.deref())
    }
}

/// Returns reference to the particular singleton structure.
///
/// When called for the first time, initializes it with the default values.
pub fn access<S>(name: &str) -> &'static S
where
    S: Default,
{
    let l = tarantool::global_lua();
    let get = || l.get::<&Inner<S>, _>(name);
    match get() {
        Some(Inner(v)) => v,
        None => {
            l.set(name, Inner::<S>::default());
            get().expect("impossible")
        }
    }
}
