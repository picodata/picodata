use std::convert::TryFrom;
use std::borrow::Cow;
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
                                ffi::CTID_CCHAR | ffi::CTID_INT8 => Some(*cdata.cast::<i8>() as _),
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

macro_rules! push_string_impl {
    ($t:ty) => {
        impl<L> Push<L> for $t
        where
            L: AsLua,
        {
            type Err = Void;      // TODO: use `!` instead (https://github.com/rust-lang/rust/issues/35121)

            #[inline(always)]
            fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
                unsafe {
                    ffi::lua_pushlstring(
                        lua.as_lua(),
                        self.as_bytes().as_ptr() as _,
                        self.as_bytes().len() as _
                    );
                    Ok(PushGuard::new(lua, 1))
                }
            }
        }

        impl<L> PushOne<L> for $t
        where
            L: AsLua,
        {
        }
    }
}

push_string_impl!{ String }
push_string_impl!{ AnyLuaString }
push_string_impl!{ str }

macro_rules! lua_read_string_impl {
    ($(@lt $lt:tt,)? $s:ty, $from_slice:expr) => {
        impl< $($lt,)? L> LuaRead<L> for $s
        where
            $( L: $lt, )?
            L: AsLua,
        {
            #[inline(always)]
            fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<$s, L> {
                unsafe {
                    let mut size = MaybeUninit::uninit();
                    let type_code = ffi::lua_type(lua.as_lua(), index.into());
                    // Because this function may be called while iterating over
                    // a table we must make sure not to change the value on the
                    // stack. So no number to string conversions are supported
                    // anymore
                    if type_code != ffi::LUA_TSTRING {
                        return Err(lua)
                    }
                    let c_ptr = ffi::lua_tolstring(
                        lua.as_lua(), index.into(), size.as_mut_ptr()
                    );
                    if c_ptr.is_null() {
                        return Err(lua)
                    }
                    let slice = slice::from_raw_parts(c_ptr as _, size.assume_init());
                    $from_slice(slice, lua)
                }
            }
        }
    }
}

lua_read_string_impl!{ String,
    |slice: &[u8], lua| String::from_utf8(slice.to_vec()).map_err(|_| lua)
}

lua_read_string_impl!{ AnyLuaString,
    |slice: &[u8], _| Ok(AnyLuaString(slice.to_vec()))
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
/// ```
/// let mut lua = hlua::Lua::new();
/// lua.set("a", "hello");
///
/// let s: hlua::StringInLua<_> = lua.get("a").unwrap();
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

impl<'a, L> std::cmp::PartialEq for StringInLua<'a, L> {
    fn eq(&self, other: &Self) -> bool {
        self.str_ref.eq(other.str_ref)
    }
}

impl<'a, L> std::cmp::PartialOrd for StringInLua<'a, L> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.str_ref.partial_cmp(other.str_ref)
    }
}

impl<'a, L> std::cmp::PartialEq<&'_ str> for StringInLua<'a, L> {
    fn eq(&self, other: &&str) -> bool {
        self.str_ref.eq(*other)
    }
}

lua_read_string_impl!{ @lt 'a, StringInLua<'a, L>,
    |slice: &'a [u8], lua|
        match str::from_utf8(slice) {
            Ok(str_ref) => Ok(StringInLua { lua, str_ref }),
            Err(_) => return Err(lua)
        }
}

impl<'a, L> Deref for StringInLua<'a, L> {
    type Target = str;

    #[inline]
    fn deref(&self) -> &str {
        &self.str_ref
    }
}

macro_rules! impl_push_read {
    (
        $t:ty,
        $(push_to_lua(&$self1:ident, $lua1:ident) { $($push:tt)* })?
        $(push_into_lua($self2:ident, $lua2:ident) { $($push_into:tt)* })?
        read_at_position($lua3:ident, $index1:ident) { $($read:tt)* }
        $(read_at_maybe_zero_position($lua4:ident, $index2:ident) { $($read_mz:tt)* })?
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
        if unsafe { ffi::lua_isboolean(lua.as_lua(), index.into()) } != true {
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

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
pub struct ToString(pub String);

impl From<ToString> for String {
    fn from(other: ToString) -> Self {
        other.0
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

