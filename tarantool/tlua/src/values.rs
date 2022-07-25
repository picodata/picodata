use std::convert::TryFrom;
use std::borrow::Cow;
use std::ffi::{CStr, CString};
use std::mem::MaybeUninit;
use std::num::NonZeroI32;
use std::slice;
use std::str;
use std::ops::Deref;
use std::os::raw::{c_int, c_void};
use std::ptr::null_mut;

use crate::{
    ffi,
    AnyLuaString,
    AsLua,
    LuaRead,
    Push,
    PushInto,
    PushGuard,
    PushOne,
    PushOneInto,
    Void,
};

macro_rules! numeric_impl {
    ($t:ident, $push:path, $read:path $(, coerce: $coerce:expr)?) => {
        impl<L> Push<L> for $t
        where
            L: AsLua,
        {
            type Err = Void;      // TODO: use `!` instead (https://github.com/rust-lang/rust/issues/35121)

            #[inline(always)]
            fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
                Self::push_into_lua(*self, lua)
            }
        }

        impl<L> PushOne<L> for $t
        where
            L: AsLua,
        {
        }

        impl<L> PushInto<L> for $t
        where
            L: AsLua,
        {
            type Err = Void;      // TODO: use `!` instead (https://github.com/rust-lang/rust/issues/35121)

            #[inline(always)]
            fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
                unsafe {
                    $push(lua.as_lua(), self as _);
                    Ok(PushGuard::new(lua, 1))
                }
            }
        }

        impl<L> PushOneInto<L> for $t
        where
            L: AsLua,
        {
        }

        impl<L> LuaRead<L> for $t
        where
            L: AsLua,
        {
            #[inline(always)]
            fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<$t, L> {
                return unsafe { read_numeric(lua.as_lua(), index.into()) }
                    .map(|v| v as _)
                    .ok_or(lua);

                #[inline(always)]
                pub unsafe fn read_numeric(l: *mut ffi::lua_State, idx: c_int) -> Option<$t> {
                    match ffi::lua_type(l, idx) {
                        ffi::LUA_TNUMBER => {
                            let number = $read(l, idx);
                            $(
                                let number = $coerce(number);
                            )?
                            Some(number as _)
                        }
                        ffi::LUA_TCDATA => {
                            let mut ctypeid = std::mem::MaybeUninit::uninit();
                            let cdata = ffi::luaL_checkcdata(l, idx, ctypeid.as_mut_ptr());
                            match ctypeid.assume_init() {
                                ffi::CTID_CCHAR => Some(*cdata.cast::<std::os::raw::c_char>() as _),
                                ffi::CTID_INT8 => Some(*cdata.cast::<i8>() as _),
                                ffi::CTID_INT16 => Some(*cdata.cast::<i16>() as _),
                                ffi::CTID_INT32 => Some(*cdata.cast::<i32>() as _),
                                ffi::CTID_INT64 => Some(*cdata.cast::<i64>() as _),
                                ffi::CTID_UINT8 => Some(*cdata.cast::<u8>() as _),
                                ffi::CTID_UINT16 => Some(*cdata.cast::<u16>() as _),
                                ffi::CTID_UINT32 => Some(*cdata.cast::<u32>() as _),
                                ffi::CTID_UINT64 => Some(*cdata.cast::<u64>() as _),
                                ffi::CTID_FLOAT => Some(*cdata.cast::<f32>() as _),
                                ffi::CTID_DOUBLE => Some(*cdata.cast::<f64>() as _),
                                _ => None,
                            }
                        }
                        _ => None,
                    }
                }
            }
        }
    }
}

numeric_impl!{i64, ffi::luaL_pushint64, ffi::lua_tonumber}
numeric_impl!{i32, ffi::lua_pushinteger, ffi::lua_tointeger}
numeric_impl!{i16, ffi::lua_pushinteger, ffi::lua_tointeger}
numeric_impl!{i8, ffi::lua_pushinteger, ffi::lua_tointeger}

numeric_impl!{u64, ffi::luaL_pushuint64, ffi::lua_tonumber,
    coerce: |n| {
        if n >= 0. {
            n as u64
        } else {
            n as i64 as u64
        }
    }
}
numeric_impl!{u32, ffi::lua_pushinteger, ffi::lua_tointeger}
numeric_impl!{u16, ffi::lua_pushinteger, ffi::lua_tointeger}
numeric_impl!{u8, ffi::lua_pushinteger, ffi::lua_tointeger}

numeric_impl!{f64, ffi::lua_pushnumber, ffi::lua_tonumber}
numeric_impl!{f32, ffi::lua_pushnumber, ffi::lua_tonumber}

macro_rules! strict_numeric_impl {
    (@is_valid int $num:tt $t:ty) => {
        $num.is_finite() && $num.fract() == 0.0 &&
        $num >= <$t>::MIN as _ && $num <= <$t>::MAX as _
    };
    (@is_valid float $num:tt $t:ty) => {
        !$num.is_finite() || $num >= <$t>::MIN as _ && $num <= <$t>::MAX as _
    };
    ($k:tt $t:ty) => {
        impl<L> LuaRead<L> for Strict<$t>
        where
            L: AsLua,
        {
            #[inline(always)]
            fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Self, L> {
                let l = lua.as_lua();
                let idx = index.into();
                let res = unsafe {
                    match ffi::lua_type(l, idx) {
                        ffi::LUA_TNUMBER => {
                            let num = ffi::lua_tonumber(l, idx);
                            let is_valid = strict_numeric_impl!(@is_valid $k num $t);
                            if is_valid {
                                Some(num as $t)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                };
                res.map(Strict).ok_or(lua)
            }
        }
    }
}

/// A wrapper type for reading lua numbers of concrete precisions without
/// implicit coercions.
///
/// By default when reading a numeric type (int or float) from a lua number
/// (i.e. calling [`LuaRead::lua_read_at_position`]) the resulting number will
/// be implicitly coerced into the target type. For example if the lua number
/// has a non zero fractional part it will be discarded when reading the number
/// as integer.
/// ```no_run
/// use tlua::Lua;
/// let lua = tlua::Lua::new();
/// let i: Option<i32> = lua.eval("return 3.14").ok();
/// assert_eq!(i, Some(3));
/// ```
///
/// If you don't want the implicit coercision, you can use the `Strict` wrapper:
/// ```no_run
/// # use tlua::Lua;
/// use tlua::Strict;
/// # let lua = Lua::new();
/// let i: Option<Strict<i32>> = lua.eval("return 3.14").ok();
/// assert_eq!(i, None); // would result in loss of data
///
/// let f: Option<Strict<f64>> = lua.eval("return 3.14").ok();
/// assert_eq!(f, Some(Strict(3.14))); // ok
/// ```
///
/// This *strictness* also applies in terms of number sizes:
/// ```no_run
/// # use tlua::{Lua, Strict};
/// # let lua = Lua::new();
/// let i: Option<u8> = lua.eval("return 256").ok();
/// assert_eq!(i, Some(0)); // non-strict => data loss
///
/// let i: Option<Strict<u8>> = lua.eval("return 256").ok();
/// assert_eq!(i, None); // strict => must not lose data
///
/// let i: Option<Strict<u16>> = lua.eval("return 256").ok();
/// assert_eq!(i, Some(Strict(256))); // strict => no data loss
/// ```
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct Strict<T>(pub T);

strict_numeric_impl!{int i8}
strict_numeric_impl!{int i16}
strict_numeric_impl!{int i32}
strict_numeric_impl!{int i64}
strict_numeric_impl!{int u8}
strict_numeric_impl!{int u16}
strict_numeric_impl!{int u32}
strict_numeric_impl!{int u64}
strict_numeric_impl!{float f32}
strict_numeric_impl!{float f64}

impl<T> From<T> for Strict<T> {
    fn from(v: T) -> Self {
        Self(v)
    }
}

macro_rules! impl_push_read {
    (
        $t:ty,
        $(push_to_lua(&$self1:ident, $lua1:ident) { $($push:tt)* })?
        $(push_into_lua($self2:ident, $lua2:ident) { $($push_into:tt)* })?
        $(
            read_at_position($lua3:ident, $index1:ident) { $($read:tt)* }
            $(read_at_maybe_zero_position($lua4:ident, $index2:ident) { $($read_mz:tt)* })?
        )?
    ) => {
        $(
            impl<L> Push<L> for $t
            where
                L: AsLua,
            {
                type Err = Void;      // TODO: use `!` instead (https://github.com/rust-lang/rust/issues/35121)

                #[inline(always)]
                fn push_to_lua(&$self1, $lua1: L) -> Result<PushGuard<L>, (Void, L)> {
                    $($push)*
                }
            }

            impl<L> PushOne<L> for $t
            where
                L: AsLua,
            {
            }
        )?

        $(
            impl<L> PushInto<L> for $t
            where
                L: AsLua,
            {
                type Err = Void;      // TODO: use `!` instead (https://github.com/rust-lang/rust/issues/35121)

                #[inline(always)]
                fn push_into_lua($self2, $lua2: L) -> Result<PushGuard<L>, (Void, L)> {
                    $($push_into)?
                }
            }

            impl<L> PushOneInto<L> for $t
            where
                L: AsLua,
            {
            }
        )?

        $(
            impl<L> LuaRead<L> for $t
            where
                L: AsLua,
            {
                #[inline(always)]
                fn lua_read_at_position($lua3: L, $index1: NonZeroI32) -> Result<Self, L> {
                    $($read)*
                }

                $(
                    #[inline(always)]
                    fn lua_read_at_maybe_zero_position($lua4: L, $index2: i32) -> Result<Self, L> {
                        $($read_mz)*
                    }
                )?
            }
        )?
    }
}

macro_rules! push_string_impl {
    ($self:ident, $lua:ident) => {
        unsafe {
            ffi::lua_pushlstring(
                $lua.as_lua(),
                $self.as_bytes().as_ptr() as _,
                $self.as_bytes().len() as _
            );
            Ok(PushGuard::new($lua, 1))
        }
    }
}

macro_rules! lua_read_string_impl {
    ($lua:ident, $index:ident, $from_slice:expr) => {
        unsafe {
            let mut size = MaybeUninit::uninit();
            let type_code = ffi::lua_type($lua.as_lua(), $index.into());
            // Because this function may be called while iterating over
            // a table we must make sure not to change the value on the
            // stack. So no number to string conversions are supported
            // anymore
            if type_code != ffi::LUA_TSTRING {
                return Err($lua)
            }
            let c_ptr = ffi::lua_tolstring(
                $lua.as_lua(), $index.into(), size.as_mut_ptr()
            );
            if c_ptr.is_null() {
                return Err($lua)
            }
            let slice = slice::from_raw_parts(c_ptr as _, size.assume_init());
            $from_slice(slice, $lua)
        }
    }
}

impl_push_read!{ String,
    push_to_lua(&self, lua) {
        push_string_impl!(self, lua)
    }
    push_into_lua(self, lua) {
        push_string_impl!(self, lua)
    }
    read_at_position(lua, index) {
        lua_read_string_impl!(lua, index,
            |slice: &[u8], lua| String::from_utf8(slice.to_vec()).map_err(|_| lua)
        )
    }
}

impl_push_read!{ CString,
    push_to_lua(&self, lua) {
        push_string_impl!(self, lua)
    }
    push_into_lua(self, lua) {
        push_string_impl!(self, lua)
    }
    read_at_position(lua, index) {
        lua_read_string_impl!(lua, index,
            |slice: &[u8], lua| CString::new(slice).map_err(|_| lua)
        )
    }
}

impl_push_read!{ AnyLuaString,
    push_to_lua(&self, lua) {
        push_string_impl!(self, lua)
    }
    push_into_lua(self, lua) {
        push_string_impl!(self, lua)
    }
    read_at_position(lua, index) {
        lua_read_string_impl!(lua, index,
            |slice: &[u8], _| Ok(AnyLuaString(slice.to_vec()))
        )
    }
}

impl_push_read!{ str,
    push_to_lua(&self, lua) {
        push_string_impl!(self, lua)
    }
}

impl_push_read!{ CStr,
    push_to_lua(&self, lua) {
        unsafe {
            ffi::lua_pushlstring(
                lua.as_lua(),
                self.as_ptr() as _,
                self.to_bytes().len() as _,
            );
            Ok(PushGuard::new(lua, 1))
        }
    }
}

/// String on the Lua stack.
///
/// It is faster -but less convenient- to read a `StringInLua` rather than a `String` because you
/// avoid any allocation.
///
/// The `StringInLua` derefs to `str`.
///
/// # Example
///
/// ```no_run
/// let mut lua = tlua::Lua::new();
/// lua.set("a", "hello");
///
/// let s: tlua::StringInLua<_> = lua.get("a").unwrap();
/// println!("{}", &*s);    // Prints "hello".
/// ```
#[derive(Debug, Eq, Ord, Hash)]
pub struct StringInLua<'a, L: 'a> {
    lua: L,
    str_ref: &'a str,
}

impl<L> StringInLua<'_, L> {
    pub fn into_inner(self) -> L {
        self.lua
    }
}

impl<L> std::cmp::PartialEq for StringInLua<'_, L> {
    fn eq(&self, other: &Self) -> bool {
        self.str_ref.eq(other.str_ref)
    }
}

impl<L> std::cmp::PartialOrd for StringInLua<'_, L> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.str_ref.partial_cmp(other.str_ref)
    }
}

impl<L> std::cmp::PartialEq<&'_ str> for StringInLua<'_, L> {
    fn eq(&self, other: &&str) -> bool {
        self.str_ref.eq(*other)
    }
}

impl<'a, L> LuaRead<L> for StringInLua<'a, L>
where
    L: 'a + AsLua,
{
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Self, L> {
        lua_read_string_impl!(lua, index,
            |slice: &'a [u8], lua|
                match str::from_utf8(slice) {
                    Ok(str_ref) => Ok(StringInLua { lua, str_ref }),
                    Err(_) => Err(lua)
                }
        )
    }
}

impl<'a, L> Deref for StringInLua<'a, L> {
    type Target = str;

    #[inline]
    fn deref(&self) -> &str {
        self.str_ref
    }
}

impl_push_read!{ bool,
    push_to_lua(&self, lua) {
        Self::push_into_lua(*self, lua)
    }
    push_into_lua(self, lua) {
        unsafe {
            ffi::lua_pushboolean(lua.as_lua(), self as _);
            Ok(PushGuard::new(lua, 1))
        }
    }
    read_at_position(lua, index) {
        if !unsafe { ffi::lua_isboolean(lua.as_lua(), index.into()) } {
            return Err(lua);
        }

        Ok(unsafe { ffi::lua_toboolean(lua.as_lua(), index.into()) != 0 })
    }
}

impl_push_read!{ (),
    push_to_lua(&self, lua) {
        ().push_into_lua(lua)
    }
    push_into_lua(self, lua) {
        unsafe { Ok(PushGuard::new(lua, 0)) }
    }
    read_at_position(_lua, _index) {
        Ok(())
    }
    read_at_maybe_zero_position(_lua, _index) {
        Ok(())
    }
}

impl<L, T> Push<L> for Option<T>
where
    T: Push<L>,
    L: AsLua,
{
    type Err = T::Err;

    #[inline]
    fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        match self {
            Some(val) => val.push_to_lua(lua),
            None => Ok(Nil.push_into_no_err(lua)),
        }
    }
}

impl<L, T> PushOne<L> for Option<T>
where
    T: PushOne<L>,
    L: AsLua,
{
}

impl<L, T> PushInto<L> for Option<T>
where
    T: PushInto<L>,
    L: AsLua,
{
    type Err = T::Err;

    #[inline]
    fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        match self {
            Some(val) => val.push_into_lua(lua),
            None => Ok(Nil.push_into_no_err(lua)),
        }
    }
}

impl<L, T> PushOneInto<L> for Option<T>
where
    T: PushOneInto<L>,
    L: AsLua,
{
}

impl<L, T> LuaRead<L> for Option<T>
where
    L: AsLua,
    T: LuaRead<L>,
{
    fn lua_read_at_maybe_zero_position(lua: L, index: i32) -> Result<Option<T>, L> {
        if let Some(index) = NonZeroI32::new(index) {
            Self::lua_read_at_position(lua, index)
        } else {
            Ok(None)
        }
    }

    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Option<T>, L> {
        if unsafe { is_null_or_nil(lua.as_lua(), index.get()) } {
            return Ok(None)
        }
        T::lua_read_at_position(lua, index).map(Some)
    }
}

impl<L, A, B> LuaRead<L> for Result<A, B>
where
    L: AsLua,
    A: for<'a> LuaRead<&'a L>,
    B: for<'b> LuaRead<&'b L>,
{
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Result<A, B>, L> {
        if let Ok(a) = A::lua_read_at_position(&lua, index) {
            return Ok(Ok(a))
        }
        if let Ok(b) = B::lua_read_at_position(&lua, index) {
            return Ok(Err(b))
        }
        Err(lua)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Nil;

impl_push_read!{Nil,
    push_to_lua(&self, lua) {
        Self::push_into_lua(*self, lua)
    }
    push_into_lua(self, lua) {
        unsafe {
            ffi::lua_pushnil(lua.as_lua());
            Ok(PushGuard::new(lua, 1))
        }
    }
    read_at_position(lua, index) {
        if unsafe { ffi::lua_isnil(lua.as_lua(), index.into()) } {
            Ok(Nil)
        } else {
            Err(lua)
        }
    }
    read_at_maybe_zero_position(lua, index) {
        if let Some(index) = NonZeroI32::new(index) {
            Self::lua_read_at_position(lua, index)
        } else {
            Ok(Nil)
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Null;

impl Null {
    unsafe fn is_null(lua: crate::LuaState, index: i32) -> bool {
        if ffi::lua_type(lua, index) == ffi::LUA_TCDATA {
            let mut ctypeid = MaybeUninit::uninit();
            let cdata = ffi::luaL_checkcdata(lua, index, ctypeid.as_mut_ptr());
            if ctypeid.assume_init() == ffi::CTID_P_VOID {
                return (*cdata.cast::<*const c_void>()).is_null()
            }
        }
        false
    }
}

pub unsafe fn is_null_or_nil(lua: crate::LuaState, index: i32) -> bool {
    ffi::lua_isnil(lua, index) || Null::is_null(lua, index)
}

impl_push_read!{Null,
    push_to_lua(&self, lua) {
        Self::push_into_lua(*self, lua)
    }
    push_into_lua(self, lua) {
        unsafe {
            let cdata = ffi::luaL_pushcdata(lua.as_lua(), ffi::CTID_P_VOID);
            *cdata.cast::<*mut c_void>() = null_mut();
            Ok(PushGuard::new(lua, 1))
        }
    }
    read_at_position(lua, index) {
        if unsafe { Null::is_null(lua.as_lua(), index.into()) } {
            Ok(Null)
        } else {
            Err(lua)
        }
    }
    read_at_maybe_zero_position(lua, index) {
        if let Some(index) = NonZeroI32::new(index) {
            Null::lua_read_at_position(lua, index)
        } else {
            Ok(Null)
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(try_from = "bool", into = "bool")]
pub struct True;

impl From<True> for bool {
    fn from(_: True) -> Self {
        true
    }
}

impl TryFrom<bool> for True {
    type Error = False;
    fn try_from(v: bool) -> Result<Self, False> {
        if v {
            Ok(True)
        } else {
            Err(False)
        }
    }
}

impl_push_read!{True,
    push_to_lua(&self, lua) {
        Self::push_into_lua(*self, lua)
    }
    push_into_lua(self, lua) {
        true.push_into_lua(lua)
    }
    read_at_position(lua, index) {
        match bool::lua_read_at_position(&lua, index) {
            Ok(v) if v => Ok(True),
            _ => Err(lua),
        }
    }
}

impl std::fmt::Display for True {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        bool::from(*self).fmt(f)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(try_from = "bool", into = "bool")]
pub struct False;

impl From<False> for bool {
    fn from(_: False) -> Self {
        false
    }
}

impl TryFrom<bool> for False {
    type Error = True;
    fn try_from(v: bool) -> Result<Self, True> {
        if !v {
            Ok(False)
        } else {
            Err(True)
        }
    }
}

impl_push_read!{False,
    push_to_lua(&self, lua) {
        Self::push_into_lua(*self, lua)
    }
    push_into_lua(self, lua) {
        false.push_into_lua(lua)
    }
    read_at_position(lua, index) {
        match bool::lua_read_at_position(&lua, index) {
            Ok(v) if !v => Ok(False),
            _ => Err(lua),
        }
    }
}

impl std::fmt::Display for False {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        bool::from(*self).fmt(f)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Typename(pub &'static str);

impl Typename {
    pub fn get(&self) -> &'static str {
        self.0
    }
}

impl_push_read!{Typename,
    read_at_position(lua, index) {
        Ok(Self(
            match crate::typename(lua.as_lua(), index.into()).to_string_lossy() {
                Cow::Borrowed(s) => s,
                _ => unreachable!("lua typename is a valid unicode string"),
            }
        ))
    }
}

/// String wrapper struct that can be used to read a lua value by converting it
/// to string possibly using `__tostring` metamethod.
#[derive(Debug, Clone)]
pub struct ToString(pub String);

impl From<ToString> for String {
    fn from(other: ToString) -> Self {
        other.0
    }
}

impl<'a> From<ToString> for Cow<'a, str> {
    fn from(other: ToString) -> Self {
        Cow::Owned(other.0)
    }
}

impl std::fmt::Display for ToString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl_push_read!{ToString,
    read_at_position(lua, index) {
        unsafe {
            let mut size = MaybeUninit::uninit();
            let c_ptr = ffi::luaT_tolstring(
                lua.as_lua(), index.into(), size.as_mut_ptr()
            );
            // the newly created string needs to be popped
            let _new_string = PushGuard::new(lua.as_lua(), 1);
            if c_ptr.is_null() {
                return Err(lua)
            }
            let slice = slice::from_raw_parts(c_ptr as _, size.assume_init());
            Ok(Self(String::from_utf8_lossy(slice).into()))
        }
    }
}

