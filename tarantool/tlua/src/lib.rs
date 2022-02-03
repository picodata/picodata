//! High-level zero-cost bindings for Lua (fork of
//! [hlua](https://crates.io/crates/hlua))
//!
//! Lua is an interpreted programming language. This crate allows you to execute Lua code.
//!
//! # General usage
//!
//! In order to execute Lua code you first need a *Lua context*, which is represented in this
//! library with [the `Lua` struct](struct.Lua.html). You can then call the
//! the [`eval`](struct.Lua.html#method.eval) or
//! [`exec`](struct.Lua.html#method.exec) method on this object.
//!
//! For example:
//!
//! ```
//! use tlua::Lua;
//!
//! let mut lua = Lua::new();
//! lua.exec("a = 12 * 5").unwrap();
//! let a: u32 = lua.eval("return a + 1").unwrap();
//! ```
//!
//! This example puts the value `60` in the global variable `a`. The values of all global variables
//! are stored within the `Lua` struct. If you execute multiple Lua scripts on the same context,
//! each script will have access to the same global variables that were modified by the previous
//! scripts.
//!
//! In order to do something actually useful with Lua, we will need to make Lua and Rust
//! communicate with each other. This can be done in four ways:
//!
//! - You can use methods on the `Lua` struct to read or write the values of global variables with
//!   the [`get`](struct.Lua.html#method.get) and [`set`](struct.Lua.html#method.set) methods. For
//!   example you can write to a global variable with a Lua script then read it from Rust, or you
//!   can write to a global variable from Rust then read it from a Lua script.
//!
//! - The Lua script that you evaluate with the [`eval`](struct.Lua.html#method.eval) method
//!   can return a value.
//!
//! - You can set the value of a global variable to a Rust functions or closures, which can then be
//!   invoked with a Lua script. See [the `Function` struct](struct.Function.html) for more
//!   information. For example if you set the value of the global variable `foo` to a Rust
//!   function, you can then call it from Lua with `foo()`.
//!
//! - Similarly you can set the value of a global variable to a Lua function, then call it from
//!   Rust. The function call can return a value.
//!
//! Which method(s) you use depends on which API you wish to expose to your Lua scripts.
//!
//! # Pushing and loading values
//!
//! The interface between Rust and Lua involves two things:
//!
//! - Sending values from Rust to Lua, which is known as *pushing* the value.
//! - Sending values from Lua to Rust, which is known as *loading* the value.
//!
//! Pushing (ie. sending from Rust to Lua) can be done with
//! [the `set` method](struct.Lua.html#method.set):
//!
//! ```
//! # use tlua::Lua;
//! # let mut lua = Lua::new();
//! lua.set("a", 50);
//! ```
//!
//! You can push values that implement [the `Push` trait](trait.Push.html) or
//! [the `PushOne` trait](trait.PushOne.html) depending on the situation:
//!
//! - Integers, floating point numbers and booleans.
//! - `String` and `&str`.
//! - Any Rust function or closure whose parameters and loadable and whose return type is pushable.
//!   See the documentation of [the `Function` struct](struct.Function.html) for more information.
//! - [The `AnyLuaValue` struct](struct.AnyLuaValue.html). This enumeration represents any possible
//!   value in Lua.
//! - The [`LuaCode`](struct.LuaCode.html) and
//!   [`LuaCodeFromReader`](struct.LuaCodeFromReader.html) structs. Since pushing these structs can
//!   result in an error, you need to use [`checked_set`](struct.Lua.html#method.checked_set)
//!   instead of `set`.
//! - `Vec`s and `HashMap`s whose content is pushable.
//! - As a special case, `Result` can be pushed only as the return type of a Rust function or
//!   closure. If they contain an error, the Rust function call is considered to have failed.
//! - As a special case, tuples can be pushed when they are the return type of a Rust function or
//!   closure. They implement `Push` but not `PushOne`.
//! - TODO: userdata
//!
//! Loading (ie. sending from Lua to Rust) can be done with
//! [the `get` method](struct.Lua.html#method.get):
//!
//! ```no_run
//! # use tlua::Lua;
//! # let mut lua = Lua::new();
//! let a: i32 = lua.get("a").unwrap();
//! ```
//!
//! You can load values that implement [the `LuaRead` trait](trait.LuaRead.html):
//!
//! - Integers, floating point numbers and booleans.
//! - `String` and [`StringInLua`](struct.StringInLua.html) (ie. the equivalent of `&str`). Loading
//!   the latter has no cost while loading a `String` performs an allocation.
//! - Any function (Lua or Rust), with [the `LuaFunction` struct](struct.LuaFunction.html). This
//!   can then be used to execute the function.
//! - [The `AnyLuaValue` struct](struct.AnyLuaValue.html). This enumeration represents any possible
//!   value in Lua.
//! - [The `LuaTable` struct](struct.LuaTable.html). This struct represents a table in Lua, where
//!   keys and values can be of different types. The table can then be iterated and individual
//!   elements can be loaded or modified.
//! - As a special case, tuples can be loaded when they are the return type of a Lua function or as
//!   the return type of [`eval`](struct.Lua.html#method.eval).
//! - TODO: userdata
//!
use std::ffi::{CStr, CString};
use std::io::Read;
use std::io::Error as IoError;
use std::borrow::Borrow;
use std::num::NonZeroI32;
use std::error::Error;
use std::fmt;
use std::io;

pub use any::{AnyHashableLuaValue, AnyLuaString, AnyLuaValue};
pub use functions_write::{Function, InsideCallback};
pub use functions_write::{function0, function1, function2, function3, function4, function5};
pub use functions_write::{function6, function7, function8, function9, function10};
pub use lua_functions::LuaFunction;
pub use lua_functions::LuaFunctionCallError;
pub use lua_functions::{LuaCode, LuaCodeFromReader};
pub use lua_tables::{LuaTable, LuaTableIterator, MethodCallError};
pub use rust_tables::{PushIterError, PushIterErrorOf};
pub use tuples::TuplePushError;
pub use userdata::UserdataOnStack;
pub use userdata::{push_userdata, read_userdata, push_some_userdata};
pub use values::{StringInLua, Nil, Null, True, False, Typename, ToString};
pub use ::tlua_derive::*;

pub type LuaTableMap = std::collections::HashMap<AnyHashableLuaValue, AnyLuaValue>;
pub type LuaSequence = Vec<AnyLuaValue>;

mod any;
pub mod debug;
pub mod ffi;
mod functions_write;
mod lua_functions;
mod lua_tables;
mod macros;
mod rust_tables;
mod userdata;
mod values;
mod tuples;

pub type LuaState = *mut ffi::lua_State;

/// A static lua context that must be created from an existing lua state pointer
/// and that will **not** be closed when dropped.
pub type StaticLua = Lua<on_drop::Ignore>;

/// A temporary lua context that will be closed when dropped.
pub type TempLua = Lua<on_drop::Close>;

/// A lua context corresponding to a lua thread (see [`ffi::lua_newthread`])
/// stored in the global [REGISTRY](ffi::LUA_REGISTRYINDEX) and will be removed
/// from there when dropped.
///
/// `LuaThread` currently can only be created from an instance of [`StaticLua`]
/// because closing a state from which a thread has been created is forbidden.
pub type LuaThread = Lua<on_drop::Unref>;

/// Main object of the library.
///
/// The type parameter `OnDrop` specifies what happens with the underlying lua
/// state when the instance gets dropped. There are currently 3 supported cases:
/// - `on_drop::Ignore`: nothing happens
/// - `on_drop::Close`: [`ffi::lua_close`] is called
/// - `on_drop::Unref`: [`ffi::luaL_unref`] is called with the associated value
///
/// # About panic safety
///
/// This type isn't panic safe. This means that if a panic happens while you were using the `Lua`,
/// then it will probably stay in a corrupt state. Trying to use the `Lua` again will most likely
/// result in another panic but shouldn't result in unsafety.
#[derive(Debug)]
pub struct Lua<OnDrop>
where
    OnDrop: on_drop::OnDrop,
{
    lua: LuaState,
    on_drop: OnDrop,
}

mod on_drop {
    use crate::{LuaState, ffi};

    pub trait OnDrop {
        fn on_drop(&mut self, l: LuaState);
    }

    /// See [`StaticLua`].
    #[derive(Debug)]
    pub struct Ignore;

    impl OnDrop for Ignore {
        fn on_drop(&mut self, _: LuaState) {}
    }

    /// See [`TempLua`].
    #[derive(Debug)]
    pub struct Close;

    impl OnDrop for Close {
        fn on_drop(&mut self, l: LuaState) {
            unsafe { ffi::lua_close(l) }
        }
    }

    /// See [`LuaThread`].
    #[derive(Debug)]
    pub struct Unref(pub i32);

    impl OnDrop for Unref {
        fn on_drop(&mut self, l: LuaState) {
            unsafe { ffi::luaL_unref(l, ffi::LUA_REGISTRYINDEX, self.0) }
        }
    }
}

/// RAII guard for a value pushed on the stack.
///
/// You shouldn't have to manipulate this type directly unless you are fiddling with the
/// library's internals.
pub struct PushGuard<L>
where
    L: AsLua,
{
    lua: L,
    top: i32,
    size: i32,
}

impl<L> std::fmt::Debug for PushGuard<L>
where
    L: AsLua,
    L: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let start = unsafe {
            AbsoluteIndex::new_unchecked(
                NonZeroI32::new(self.top - self.size + 1).unwrap()
            )
        };
        f.debug_struct("PushGuard")
            .field("lua", &self.lua)
            .field("size", &self.size)
            .field("lua_type", &typenames(self.lua.as_lua(), start, self.size as _))
            .finish()
    }
}

impl<L: AsLua> PushGuard<L> {
    /// Creates a new `PushGuard` from this Lua context representing `size` items on the stack.
    /// When this `PushGuard` is destroyed, `size` items will be popped.
    ///
    /// # Safety
    /// There must be at least `size` elements on the `lua` stack and it must be
    /// safe to drop them at the same time with this `PushGuard`.
    #[inline]
    pub unsafe fn new(lua: L, size: i32) -> Self {
        PushGuard {
            top: ffi::lua_gettop(lua.as_lua()),
            lua,
            size: size as _,
        }
    }

    #[inline]
    pub fn assert_one_and_forget(self) -> i32 {
        assert_eq!(self.size, 1);
        self.forget_internal()
    }

    /// Returns the number of elements managed by this `PushGuard`.
    #[inline]
    pub fn size(&self) -> i32 {
        self.size
    }

    /// Prevents the value from being popped when the `PushGuard` is destroyed, and returns the
    /// number of elements on the Lua stack.
    ///
    /// # Safety
    /// The values on the stack will not be popped automatically, so the caller
    /// must ensure nothing is leaked.
    #[inline]
    pub unsafe fn forget(self) -> i32 {
        self.forget_internal()
    }

    /// Internal crate-only version of `forget`. It is generally assumed that code within this
    /// crate that calls this method knows what it is doing.
    #[inline]
    fn forget_internal(mut self) -> i32 {
        let size = self.size;
        self.size = 0;
        size
    }

    /// Destroys the guard, popping the value. Returns the inner part,
    /// which returns access when using by-value capture.
    #[inline]
    pub fn into_inner(self) -> L {
        use std::{mem::{self, MaybeUninit}, ptr};

        let mut res = MaybeUninit::uninit();
        unsafe {
            ptr::copy_nonoverlapping(&self.lua, res.as_mut_ptr(), 1);
            if self.size != 0 {
                ffi::lua_pop(self.lua.as_lua(), self.size as _);
            }
        };
        mem::forget(self);

        unsafe { res.assume_init() }
    }
}

/// Trait for objects that have access to a Lua context.
pub trait AsLua {
    fn as_lua(&self) -> *mut ffi::lua_State;

    /// Try to push `v` onto the lua stack.
    ///
    /// In case of success returns a `PushGuard` which captures `self` by value
    /// and stores the amount of values pushed onto the stack.
    ///
    /// In case of failure returns a tuple with 2 elements:
    /// - an error, which occured during the attempt to push
    /// - `self`
    #[inline(always)]
    fn try_push<T>(self, v: T) -> Result<PushGuard<Self>, (<T as PushInto<Self>>::Err, Self)>
    where
        Self: Sized,
        T: PushInto<Self>,
    {
        v.push_into_lua(self)
    }

    /// Push `v` onto the lua stack.
    ///
    /// This method is only available if `T::Err` implements `Into<Void>`, which
    /// means that no error can happen during the attempt to push.
    ///
    /// Returns a `PushGuard` which captures `self` by value and stores the
    /// amount of values pushed onto the stack.
    #[inline(always)]
    fn push<T>(self, v: T) -> PushGuard<Self>
    where
        Self: Sized,
        T: PushInto<Self>,
        <T as PushInto<Self>>::Err: Into<Void>,
    {
        v.push_into_no_err(self)
    }

    /// Try to push `v` onto the lua stack.
    ///
    /// This method is only available if `T` implements `PushOneInto`, which
    /// means that it pushes a single value onto the stack.
    ///
    /// Returns a `PushGuard` which captures `self` by value and stores the
    /// amount of values pushed onto the stack (ideally this will be 1, but it
    /// is the responsibility of the impelemntor to make sure it is so).
    #[inline(always)]
    fn try_push_one<T>(self, v: T) -> Result<PushGuard<Self>, (<T as PushInto<Self>>::Err, Self)>
    where
        Self: Sized,
        T: PushOneInto<Self>,
    {
        v.push_into_lua(self)
    }

    /// Push `v` onto the lua stack.
    ///
    /// This method is only available if
    /// - `T` implements `PushOneInto`, which means that it pushes a single
    /// value onto the stack
    /// - `T::Err` implements `Into<Void>`, which means that no error can happen
    /// during the attempt to push
    ///
    /// Returns a `PushGuard` which captures `self` by value and stores the
    /// amount of values pushed onto the stack (ideally this will be 1, but it
    /// is the responsibility of the impelemntor to make sure it is so).
    #[inline(always)]
    fn push_one<T>(self, v: T) -> PushGuard<Self>
    where
        Self: Sized,
        T: PushOneInto<Self>,
        <T as PushInto<Self>>::Err: Into<Void>,
    {
        v.push_into_no_err(self)
    }

    /// Push `iterator` onto the lua stack as a lua table.
    ///
    /// This method is only available if
    /// - `I::Item` implements `PushInto<LuaState>`, which means that it can be
    /// pushed onto the lua stack by value
    /// - `I::Item::Err` implements `Into<Void>`, which means that no error can
    /// happen during the attempt to push
    ///
    /// If `I::Item` pushes a single value onto the stack, the resulting lua
    /// table is a lua sequence (a table with 1-based integer keys).
    ///
    /// If `I::Item` pushes 2 values onto the stack, the resulting lua table is
    /// a regular lua table with the provided keys.
    ///
    /// If `I::Item` pushes more than 2 values, the function returns `Err(self)`.
    ///
    /// Returns a `PushGuard` which captures `self` by value and stores the
    /// amount of values pushed onto the stack (exactly 1 -- lua table).
    #[inline(always)]
    fn push_iter<I>(self, iterator: I) -> Result<PushGuard<Self>, Self>
    where
        Self: Sized,
        I: Iterator,
        <I as Iterator>::Item: PushInto<LuaState>,
        <<I as Iterator>::Item as PushInto<LuaState>>::Err: Into<Void>,
    {
        rust_tables::push_iter(self, iterator).map_err(|(_, lua)| lua)
    }

    /// Push `iterator` onto the lua stack as a lua table.
    ///
    /// This method is only available if `I::Item` implements
    /// `PushInto<LuaState>`, which means that it can be pushed onto the lua
    /// stack by value.
    ///
    /// If `I::Item` pushes a single value onto the stack, the resulting lua
    /// table is a lua sequence (a table with 1-based integer keys).
    ///
    /// If `I::Item` pushes 2 values onto the stack, the resulting lua table is
    /// a regular lua table with the provided keys.
    ///
    /// If `I::Item` pushes more than 2 values or an error happens during an
    /// attempt to push, the function returns `Err((e, self))` where `e` is a
    /// `PushIterErrorOf`.
    ///
    /// Returns a `PushGuard` which captures `self` by value and stores the
    /// amount of values pushed onto the stack (exactly 1 -- lua table).
    #[inline(always)]
    fn try_push_iter<I>(self, iterator: I)
        -> Result<PushGuard<Self>, (PushIterErrorOf<I>, Self)>
    where
        Self: Sized,
        I: Iterator,
        <I as Iterator>::Item: PushInto<LuaState>,
    {
        rust_tables::push_iter(self, iterator)
    }

    #[inline(always)]
    fn read<T>(self) -> Result<T, Self>
    where
        Self: Sized,
        T: LuaRead<Self>,
    {
        T::lua_read(self)
    }

    #[inline(always)]
    fn read_at<T>(self, index: i32) -> Result<T, Self>
    where
        Self: Sized,
        T: LuaRead<Self>,
    {
        T::lua_read_at_maybe_zero_position(self, index)
    }

    #[inline(always)]
    fn read_at_nz<T>(self, index: NonZeroI32) -> Result<T, Self>
    where
        Self: Sized,
        T: LuaRead<Self>,
    {
        T::lua_read_at_position(self, index)
    }
}

impl<T> AsLua for &'_ T
where
    T: AsLua,
{
    fn as_lua(&self) -> *mut ffi::lua_State {
        T::as_lua(self)
    }
}

impl<D> AsLua for Lua<D>
where
    D: on_drop::OnDrop,
{
    #[inline]
    fn as_lua(&self) -> *mut ffi::lua_State {
        self.lua
    }
}

impl AsLua for *mut ffi::lua_State {
    fn as_lua(&self) -> *mut ffi::lua_State {
        *self
    }
}

impl<L> AsLua for PushGuard<L>
where
    L: AsLua,
{
    #[inline]
    fn as_lua(&self) -> *mut ffi::lua_State {
        self.lua.as_lua()
    }
}

/// Type returned from [`Push::push_to_lua`] function.
pub type PushResult<L, P> = Result<PushGuard<L>, (<P as Push<L>>::Err, L)>;

/// Types implementing this trait can be pushed onto the Lua stack by reference.
pub trait Push<L: AsLua> {
    /// Error that can happen when pushing a value.
    type Err;

    /// Pushes the value on the top of the stack.
    ///
    /// Must return a guard representing the elements that have been pushed.
    ///
    /// You can implement this for any type you want by redirecting to call to
    /// another implementation (for example `5.push_to_lua`) or by calling
    /// `userdata::push_userdata`.
    fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)>;

    /// Same as `push_to_lua` but can only succeed and is only available if
    /// `Err` implements `Into<Void>`.
    #[inline]
    fn push_no_err(&self, lua: L) -> PushGuard<L>
    where
        <Self as Push<L>>::Err: Into<Void>,
    {
        match self.push_to_lua(lua) {
            Ok(p) => p,
            Err(_) => unreachable!("no way to instantiate Void"),
        }
    }
}

impl<T, L> Push<L> for &'_ T
where
    L: AsLua,
    T: ?Sized,
    T: Push<L>,
{
    type Err = T::Err;

    fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        T::push_to_lua(*self, lua)
    }
}

/// Extension trait for `Push`. Guarantees that only one element will be pushed.
///
/// This should be implemented on most types that implement `Push`, except for tuples.
///
/// > **Note**: Implementing this trait on a type that pushes multiple elements will most likely
/// > result in panics.
// Note for the implementation: since this trait is not unsafe, it is mostly a hint. Functions can
// require this trait if they only accept one pushed element, but they must also add a runtime
// assertion to make sure that only one element was actually pushed.
pub trait PushOne<L: AsLua>: Push<L> {}

impl<T, L> PushOne<L> for &'_ T
where
    L: AsLua,
    T: ?Sized,
    T: PushOne<L>,
{
}

/// Type returned from [`PushInto::push_into_lua`] function.
pub type PushIntoResult<L, P> = Result<PushGuard<L>, (<P as PushInto<L>>::Err, L)>;

/// Types implementing this trait can be pushed onto the Lua stack by value.
pub trait PushInto<L>
where
    L: AsLua,
{
    type Err;

    /// Push the value into lua by value
    fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)>;

    /// Same as `push_into_lua` but can only succeed and is only available if
    /// `Err` implements `Into<Void>`.
    #[inline]
    fn push_into_no_err(self, lua: L) -> PushGuard<L>
    where
        Self: Sized,
        <Self as PushInto<L>>::Err: Into<Void>,
    {
        match self.push_into_lua(lua) {
            Ok(p) => p,
            Err(_) => unreachable!("no way to instantiate Void"),
        }
    }
}

impl<T, L> PushInto<L> for &'_ T
where
    L: AsLua,
    T: ?Sized,
    T: Push<L>,
{
    type Err = T::Err;

    fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        self.push_to_lua(lua)
    }
}

/// Extension trait for `PushInto`. Guarantees that only one element will be
/// pushed.
///
/// This should be implemented on most types that implement `PushInto`, except
/// for tuples.
///
/// > **Note**: Implementing this trait on a type that pushes multiple elements
/// > will most likely result in panics.
///
// Note for the implementation: since this trait is not unsafe, it is mostly a
// hint. Functions can require this trait if they only accept one pushed
// element, but they must also add a runtime assertion to make sure that only
// one element was actually pushed.
pub trait PushOneInto<L: AsLua>: PushInto<L> {}

impl<T, L> PushOneInto<L> for &'_ T
where
    L: AsLua,
    T: ?Sized,
    T: PushOne<L>,
{
}

/// Type that cannot be instantiated.
///
/// Will be replaced with `!` eventually (<https://github.com/rust-lang/rust/issues/35121>).
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Void {}

impl fmt::Display for Void {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        unreachable!("Void cannot be instantiated")
    }
}

pub const NEGATIVE_ONE: NonZeroI32 = unsafe { NonZeroI32::new_unchecked(-1) };
pub const NEGATIVE_TWO: NonZeroI32 = unsafe { NonZeroI32::new_unchecked(-2) };

/// Types that can be obtained from a Lua context.
///
/// Most types that implement `Push` also implement `LuaRead`, but this is not always the case
/// (for example `&'static str` implements `Push` but not `LuaRead`).
pub trait LuaRead<L>: Sized {
    #[inline(always)]
    fn n_values_expected() -> i32 {
        1
    }

    /// Reads the data from Lua.
    #[inline]
    fn lua_read(lua: L) -> Result<Self, L> {
        let index = NonZeroI32::new(-Self::n_values_expected()).expect("Invalid n_values_expected");
        Self::lua_read_at_position(lua, index)
    }

    fn lua_read_at_maybe_zero_position(lua: L, index: i32) -> Result<Self, L> {
        if let Some(index) = NonZeroI32::new(index) {
            Self::lua_read_at_position(lua, index)
        } else {
            Err(lua)
        }
    }

    /// Reads the data from Lua at a given position.
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Self, L>;
}

/// Error that can happen when executing Lua code.
#[derive(Debug)]
pub enum LuaError {
    /// There was a syntax error when parsing the Lua code.
    SyntaxError(String),

    /// There was an error during execution of the Lua code
    /// (for example not enough parameters for a function call).
    ExecutionError(String),

    /// There was an IoError while reading the source code to execute.
    ReadError(IoError),

    /// The call to `eval` has requested the wrong type of data.
    WrongType{
        rust_expected: String,
        lua_actual: String,
    },
}

impl LuaError {
    pub fn wrong_type<T, L: AsLua>(lua: L, n_values: i32) -> Self {
        let nz = unsafe { NonZeroI32::new_unchecked(-n_values) };
        let start = AbsoluteIndex::new(nz, lua.as_lua());
        Self::WrongType {
            rust_expected: std::any::type_name::<T>().into(),
            lua_actual: typenames(lua, start, n_values as _),
        }
    }
}

pub fn typename(lua: impl AsLua, index: i32) -> &'static CStr {
    unsafe {
        let lua_type = ffi::lua_type(lua.as_lua(), index);
        let typename = ffi::lua_typename(lua.as_lua(), lua_type);
        CStr::from_ptr(typename)
    }
}

pub fn typenames(lua: impl AsLua, start: AbsoluteIndex, count: u32) -> String {
    let l_ptr = lua.as_lua();
    let single_typename = |i| typename(l_ptr, i as _).to_string_lossy();

    let start = start.get();
    match count {
        0 => return "()".into(),
        1 => return single_typename(start).into_owned(),
        _ => {}
    }

    let mut res = vec![std::borrow::Cow::Borrowed("(")];
    let end = start + count - 1;
    for i in start..end {
        res.push(single_typename(i));
        res.push(", ".into());
    }
    res.push(single_typename(end));
    res.push(")".into());
    res.join("")
}

impl fmt::Display for LuaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use LuaError::*;

        match *self {
            SyntaxError(ref s) => write!(f, "Syntax error: {}", s),
            ExecutionError(ref s) => write!(f, "Execution error: {}", s),
            ReadError(ref e) => write!(f, "Read error: {}", e),
            WrongType{
                rust_expected: ref e1,
                lua_actual: ref e2
            } => write!(f, "Wrong type returned by Lua: {} expected, got {}", e1, e2),
        }
    }
}

impl Error for LuaError {
    fn description(&self) -> &str {
        use LuaError::*;

        match *self {
            SyntaxError(ref s) => s,
            ExecutionError(ref s) => s,
            ReadError(_) => "read error",
            WrongType{rust_expected: _, lua_actual: _} => "wrong type returned by Lua",
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        use LuaError::*;

        match *self {
            SyntaxError(_) => None,
            ExecutionError(_) => None,
            ReadError(ref e) => Some(e),
            WrongType{rust_expected: _, lua_actual: _} => None,
        }
    }
}

impl From<io::Error> for LuaError {
    fn from(e: io::Error) -> Self {
        LuaError::ReadError(e)
    }
}

impl TempLua {
    /// Builds a new empty TempLua context.
    ///
    /// There are no global variables and the registry is totally empty. Even the functions from
    /// the standard library can't be used.
    ///
    /// If you want to use the Lua standard library in the scripts of this context, see
    /// [the openlibs method](#method.openlibs)
    ///
    /// # Example
    ///
    /// ```
    /// use tlua::Lua;
    /// let mut lua = Lua::new();
    /// ```
    ///
    /// # Panic
    ///
    /// The function panics if the underlying call to `lua_newstate` fails
    /// (which indicates lack of memory).
    #[inline]
    pub fn new() -> Self {
        let lua = unsafe { ffi::luaL_newstate() };
        if lua.is_null() {
            panic!("lua_newstate failed");
        }

        // called whenever lua encounters an unexpected error.
        extern "C" fn panic(lua: *mut ffi::lua_State) -> libc::c_int {
            let err = unsafe { ffi::lua_tostring(lua, -1) };
            let err = unsafe { CStr::from_ptr(err) };
            let err = String::from_utf8(err.to_bytes().to_vec()).unwrap();
            panic!("PANIC: unprotected error in call to Lua API ({})\n", err);
        }

        unsafe { ffi::lua_atpanic(lua, panic) };

        unsafe { Self::from_existing(lua) }
    }

    /// Takes an existing `lua_State` and build a TemplLua object from it.
    ///
    /// `lua_close` will be called on the `lua_State` in drop.
    ///
    /// # Safety
    /// A pointer to a valid `lua` context must be provided which is ok to be
    /// closed.
    #[inline]
    pub unsafe fn from_existing<T>(lua: *mut T) -> Self {
        Self {
            lua: lua as _,
            on_drop: on_drop::Close,
        }
    }
}

impl StaticLua {
    /// Takes an existing `lua_State` and build a StaticLua object from it.
    ///
    /// `lua_close` is **NOT** called in `drop`.
    ///
    /// # Safety
    /// A pointer to a valid `lua` context must be provided.
    #[inline]
    pub unsafe fn from_static<T>(lua: *mut T) -> Self {
        Self {
            lua: lua as _,
            on_drop: on_drop::Ignore,
        }
    }

    /// Creates a new Lua thread with an independent stack and runs the provided
    /// function within it. The new state has access to all the global objects
    /// available to `self`.
    pub fn new_thread(&self) -> LuaThread {
        unsafe {
            let lua = ffi::lua_newthread(self.as_lua());
            let r = ffi::luaL_ref(self.as_lua(), ffi::LUA_REGISTRYINDEX);
            LuaThread {
                lua,
                on_drop: on_drop::Unref(r)
            }
        }
    }
}

impl<OnDrop> Lua<OnDrop>
where
    OnDrop: on_drop::OnDrop,
{
    /// Opens all standard Lua libraries.
    ///
    /// See the reference for the standard library here:
    /// <https://www.lua.org/manual/5.2/manual.html#6>
    ///
    /// This is done by calling `luaL_openlibs`.
    ///
    /// # Example
    ///
    /// ```
    /// use tlua::Lua;
    /// let mut lua = Lua::new();
    /// lua.openlibs();
    /// ```
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn openlibs(&self) {
        unsafe { ffi::luaL_openlibs(self.lua) }
    }

    /// Opens base library.
    ///
    /// <https://www.lua.org/manual/5.2/manual.html#pdf-luaopen_base>
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn open_base(&self) {
        unsafe { ffi::luaopen_base(self.lua) }
    }

    /// Opens bit32 library.
    ///
    /// <https://www.lua.org/manual/5.2/manual.html#pdf-luaopen_bit32>
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn open_bit(&self) {
        unsafe { ffi::luaopen_bit(self.lua) }
    }

    /// Opens debug library.
    ///
    /// <https://www.lua.org/manual/5.2/manual.html#pdf-luaopen_debug>
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn open_debug(&self) {
        unsafe { ffi::luaopen_debug(self.lua) }
    }

    /// Opens io library.
    ///
    /// <https://www.lua.org/manual/5.2/manual.html#pdf-luaopen_io>
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn open_io(&self) {
        unsafe { ffi::luaopen_io(self.lua) }
    }

    /// Opens math library.
    ///
    /// <https://www.lua.org/manual/5.2/manual.html#pdf-luaopen_math>
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn open_math(&self) {
        unsafe { ffi::luaopen_math(self.lua) }
    }

    /// Opens os library.
    ///
    /// <https://www.lua.org/manual/5.2/manual.html#pdf-luaopen_os>
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn open_os(&self) {
        unsafe { ffi::luaopen_os(self.lua) }
    }

    /// Opens package library.
    ///
    /// <https://www.lua.org/manual/5.2/manual.html#pdf-luaopen_package>
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn open_package(&self) {
        unsafe { ffi::luaopen_package(self.lua) }
    }

    /// Opens string library.
    ///
    /// <https://www.lua.org/manual/5.2/manual.html#pdf-luaopen_string>
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn open_string(&self) {
        unsafe { ffi::luaopen_string(self.lua) }
    }

    /// Opens table library.
    ///
    /// <https://www.lua.org/manual/5.2/manual.html#pdf-luaopen_table>
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn open_table(&self) {
        unsafe { ffi::luaopen_table(self.lua) }
    }

    /// Executes some Lua code in the context.
    ///
    /// The code will have access to all the global variables you set with methods such as `set`.
    /// Every time you execute some code in the context, the code can modify these global variables.
    ///
    /// The template parameter of this function is the return type of the expression that is being
    /// evaluated.
    /// In order to avoid compilation error, you should call this function either by doing
    /// `lua.eval::<T>(...)` or `let result: T = lua.eval(...);` where `T` is the type of
    /// the expression.
    /// The function will return an error if the actual return type of the expression doesn't
    /// match the template parameter.
    ///
    /// The return type must implement the `LuaRead` trait. See
    /// [the documentation at the crate root](index.html#pushing-and-loading-values) for more
    /// information.
    ///
    /// # Examples
    ///
    /// ```
    /// use tlua::Lua;
    /// let mut lua = Lua::new();
    ///
    /// let twelve: i32 = lua.eval("return 3 * 4;").unwrap();
    /// let sixty = lua.eval::<i32>("return 6 * 10;").unwrap();
    /// ```
    #[inline(always)]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn eval<'lua, T>(&'lua self, code: &str) -> Result<T, LuaError>
    where
        T: LuaRead<PushGuard<LuaFunction<PushGuard<&'lua Self>>>>,
    {
        LuaFunction::load(self, code)?
            .into_call()
    }

    /// Executes some Lua code in the context.
    ///
    /// The code will have access to all the global variables you set with
    /// methods such as `set`.  Every time you execute some code in the context,
    /// the code can modify these global variables.
    ///
    /// # Examples
    ///
    /// ```
    /// use tlua::Lua;
    /// let mut lua = Lua::new();
    /// lua.exec("function multiply_by_two(a) return a * 2 end").unwrap();
    /// lua.exec("twelve = multiply_by_two(6)").unwrap();
    /// ```
    #[inline(always)]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn exec(&self, code: &str) -> Result<(), LuaError> {
        LuaFunction::load(self, code)?
            .into_call()
    }

    /// Executes some Lua code on the context.
    ///
    /// This does the same thing as [the `eval` method](#method.eval), but the
    /// code to evaluate is loaded from an object that implements `Read`.
    ///
    /// Use this method when you potentially have a large amount of code (for example if you read
    /// the code from a file) in order to avoid having to put everything in memory first before
    /// passing it to the Lua interpreter.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::fs::File;
    /// use tlua::Lua;
    ///
    /// let mut lua = Lua::new();
    /// let script = File::open("script.lua").unwrap();
    /// let res: u32 = lua.eval_from(script).unwrap();
    /// ```
    #[inline(always)]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn eval_from<'lua, T>(&'lua self, code: impl Read) -> Result<T, LuaError>
    where
        T: LuaRead<PushGuard<LuaFunction<PushGuard<&'lua Self>>>>,
    {
        LuaFunction::load_from_reader(self, code)?
            .into_call()
    }

    /// Executes some Lua code on the context.
    ///
    /// This does the same thing as [the `exec` method](#method.exec), but the
    /// code to execute is loaded from an object that implements `Read`.
    ///
    /// Use this method when you potentially have a large amount of code (for
    /// example if you read the code from a file) in order to avoid having to
    /// put everything in memory first before passing it to the Lua interpreter.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::fs::File;
    /// use tlua::Lua;
    ///
    /// let mut lua = Lua::new();
    /// let script = File::open("script.lua").unwrap();
    /// lua.exec_from(script).unwrap();
    /// ```
    #[inline(always)]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn exec_from(&self, code: impl Read) -> Result<(), LuaError> {
        LuaFunction::load_from_reader(self, code)?
            .into_call()
    }

    /// Reads the value of a global variable.
    ///
    /// Returns `None` if the variable doesn't exist or has the wrong type.
    ///
    /// The type must implement the `LuaRead` trait. See
    /// [the documentation at the crate root](index.html#pushing-and-loading-values) for more
    /// information.
    ///
    /// # Example
    ///
    /// ```
    /// use tlua::Lua;
    /// let mut lua = Lua::new();
    /// lua.exec("a = 5").unwrap();
    /// let a: i32 = lua.get("a").unwrap();
    /// assert_eq!(a, 5);
    /// ```
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn get<'lua, V, I>(&'lua self, index: I) -> Option<V>
    where
        I: Borrow<str>,
        V: LuaRead<PushGuard<&'lua Self>>,
    {
        let index = CString::new(index.borrow()).unwrap();
        unsafe {
            ffi::lua_getglobal(self.lua, index.as_ptr());
            V::lua_read(PushGuard::new(self, 1)).ok()
        }
    }

    /// Reads the value of a global, capturing the context by value.
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn into_get<V, I>(self, index: I) -> Result<V, PushGuard<Self>>
    where
        I: Borrow<str>,
        V: LuaRead<PushGuard<Self>>,
    {
        let index = CString::new(index.borrow()).unwrap();
        unsafe {
            ffi::lua_getglobal(self.lua, index.as_ptr());
            V::lua_read(PushGuard::new(self, 1))
        }
    }

    /// Modifies the value of a global variable.
    ///
    /// If you want to write an array, you are encouraged to use
    /// [the `empty_array` method](#method.empty_array) instead.
    ///
    /// The type must implement the `PushOne` trait. See
    /// [the documentation at the crate root](index.html#pushing-and-loading-values) for more
    /// information.
    ///
    /// # Example
    ///
    /// ```
    /// use tlua::Lua;
    /// let mut lua = Lua::new();
    ///
    /// lua.set("a", 12);
    /// let six: i32 = lua.eval("return a / 2;").unwrap();
    /// assert_eq!(six, 6);
    /// ```
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn set<'lua, I, V, E>(&'lua self, index: I, value: V)
    where
        I: Borrow<str>,
        V: PushOneInto<&'lua Self, Err = E>,
        E: Into<Void>,
    {
        match self.checked_set(index, value) {
            Ok(_) => (),
            Err(_) => unreachable!(),
        }
    }

    /// Modifies the value of a global variable.
    // TODO: docs
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn checked_set<'lua, I, V>(&'lua self, index: I, value: V)
        -> Result<(), <V as PushInto<&'lua Self>>::Err>
    where
        I: Borrow<str>,
        V: PushOneInto<&'lua Self>,
    {
        unsafe {
            ffi::lua_pushglobaltable(self.lua);
            self.as_lua().push(index.borrow()).assert_one_and_forget();
            match self.try_push(value) {
                Ok(pushed) => {
                    assert_eq!(pushed.size, 1);
                    pushed.forget()
                }
                Err((err, lua)) => {
                    ffi::lua_pop(lua.as_lua(), 2);
                    return Err(err);
                }
            };
            ffi::lua_settable(self.lua, -3);
            ffi::lua_pop(self.lua, 1);
            Ok(())
        }
    }

    /// Sets the value of a global variable to an empty array, then loads it.
    ///
    /// This is the function you should use if you want to set the value of a global variable to
    /// an array. After calling it, you will obtain a `LuaTable` object which you can then fill
    /// with the elements of the array.
    ///
    /// # Example
    ///
    /// ```
    /// use tlua::Lua;
    /// let mut lua = Lua::new();
    /// lua.openlibs();     // Necessary for `ipairs`.
    ///
    /// {
    ///     let mut array = lua.empty_array("my_values");
    ///     array.set(1, 10);       // Don't forget that Lua arrays are indexed from 1.
    ///     array.set(2, 15);
    ///     array.set(3, 20);
    /// }
    ///
    /// let sum: i32 = lua.eval(r#"
    ///     local sum = 0
    ///     for i, val in ipairs(my_values) do
    ///         sum = sum + val
    ///     end
    ///     return sum
    /// "#).unwrap();
    ///
    /// assert_eq!(sum, 45);
    /// ```
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn empty_array<I>(&self, index: I) -> LuaTable<PushGuard<&Self>>
    where
        I: Borrow<str>,
    {
        unsafe {
            ffi::lua_pushglobaltable(self.as_lua());
            match index.borrow().push_to_lua(self.as_lua()) {
                Ok(pushed) => pushed.forget(),
                Err(_) => unreachable!(),
            };
            ffi::lua_newtable(self.as_lua());
            ffi::lua_settable(self.as_lua(), -3);
            ffi::lua_pop(self.as_lua(), 1);

            // TODO: cleaner implementation
            self.get(index).unwrap()
        }
    }

    /// Loads the array containing the global variables.
    ///
    /// In lua, the global variables accessible from the lua code are all part of a table which
    /// you can load here.
    ///
    /// # Examples
    ///
    /// The function can be used to write global variables, just like `set`.
    ///
    /// ```
    /// use tlua::Lua;
    /// let mut lua = Lua::new();
    /// lua.globals_table().set("a", 5);
    /// assert_eq!(lua.get::<i32, _>("a"), Some(5));
    /// ```
    ///
    /// A more useful feature for this function is that it allows you to set the metatable of the
    /// global variables. See TODO for more info.
    ///
    /// ```
    /// use tlua::Lua;
    /// use tlua::AnyLuaValue;
    ///
    /// let mut lua = Lua::new();
    /// {
    ///     let mut metatable = lua.globals_table().get_or_create_metatable();
    ///     metatable.set("__index", tlua::function2(|_: AnyLuaValue, var: String| -> AnyLuaValue {
    ///         println!("The user tried to access the variable {:?}", var);
    ///         AnyLuaValue::LuaNumber(48.0)
    ///     }));
    /// }
    ///
    /// let b: i32 = lua.eval("return b * 2;").unwrap();
    /// // -> The user tried to access the variable "b"
    ///
    /// assert_eq!(b, 96);
    /// ```
    #[inline]
    // TODO(gmoshkin): this method should be part of AsLua
    pub fn globals_table(&self) -> LuaTable<PushGuard<&Self>> {
        unsafe {
            ffi::lua_pushglobaltable(self.lua);
            let guard = PushGuard::new(self, 1);
            LuaRead::lua_read(guard).ok().unwrap()
        }
    }
}

impl Default for TempLua {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for Lua<T>
where
    T: on_drop::OnDrop,
{
    #[inline]
    fn drop(&mut self) {
        self.on_drop.on_drop(self.lua)
    }
}

impl<L: AsLua> Drop for PushGuard<L> {
    #[inline]
    fn drop(&mut self) {
        if self.size != 0 {
            unsafe {
                ffi::lua_pop(self.lua.as_lua(), self.size as _);
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AbsoluteIndex(NonZeroI32);

impl AbsoluteIndex {
    pub fn new<L>(index: NonZeroI32, lua: L) -> Self
    where
        L: AsLua,
    {
        let top = unsafe { ffi::lua_gettop(lua.as_lua()) };
        let i = index.get();
        if ffi::is_relative_index(i) {
            Self(NonZeroI32::new(top + i + 1).expect("Invalid relative index"))
        } else {
            Self(index)
        }
    }

    /// # Safety
    /// `index` must be a valid absolute or relative index into the lua stack
    /// with which it's going to be used
    pub unsafe fn new_unchecked(index: NonZeroI32) -> Self {
        Self(index)
    }

    pub fn get(&self) -> u32 {
        self.0.get() as _
    }
}

impl From<AbsoluteIndex> for i32 {
    fn from(index: AbsoluteIndex) -> i32 {
        index.0.get()
    }
}

