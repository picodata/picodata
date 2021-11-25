use std::mem::MaybeUninit;
use std::num::NonZeroI32;
use std::slice;
use std::str;
use std::ops::Deref;

use crate::{
    AnyLuaString,
    AsLua,
    LuaRead,
    Nil,
    Push,
    PushGuard,
    PushOne,
    Void,
};

macro_rules! numeric_impl {
    ($t:ident, $push:path, $read:path) => {
        impl<L> Push<L> for $t
        where
            L: AsLua,
        {
            type Err = Void;      // TODO: use `!` instead (https://github.com/rust-lang/rust/issues/35121)

            #[inline]
            fn push_to_lua(self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
                unsafe {
                    $push(lua.as_lua(), self as _);
                    Ok(PushGuard::new(lua, 1))
                }
            }
        }

        impl<L> PushOne<L> for $t
        where
            L: AsLua,
        {
        }

        impl<L> LuaRead<L> for $t
        where
            L: AsLua,
        {
            #[inline]
            fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<$t, L> {
                unsafe { $read(lua.as_lua(), index.into()) }
                    .map(|v| v as _)
                    .ok_or(lua)
            }
        }
    }
}

macro_rules! impl_try_to_numeric {
    ($name:ident, $t:ty, $read:path) => {
        unsafe fn $name(l: *mut ffi::lua_State, idx: i32) -> Option<$t> {
            if ffi::lua_type(l, idx) != ffi::LUA_TNUMBER {
                return None
            }
            let mut success = MaybeUninit::uninit();
            let val = $read(l, idx, success.as_mut_ptr());
            if success.assume_init() == 0 {
                None
            } else {
                Some(val)
            }
        }
    }
}

impl_try_to_numeric!{lua_try_tointeger, isize, ffi::lua_tointegerx}

numeric_impl!{i8, ffi::lua_pushinteger, lua_try_tointeger}
numeric_impl!{i16, ffi::lua_pushinteger, lua_try_tointeger}
numeric_impl!{i32, ffi::lua_pushinteger, lua_try_tointeger}
// integer_impl!(i64)   // data loss

numeric_impl!{u8, ffi::lua_pushinteger, lua_try_tointeger}
numeric_impl!{u16, ffi::lua_pushinteger, lua_try_tointeger}
numeric_impl!{u32, ffi::lua_pushinteger, lua_try_tointeger}
// unsigned_impl!(u64);   // data loss

impl_try_to_numeric!{lua_try_tonumber, f64, ffi::lua_tonumberx}

numeric_impl!{f32, ffi::lua_pushnumber, lua_try_tonumber}
numeric_impl!{f64, ffi::lua_pushnumber, lua_try_tonumber}

macro_rules! push_string_impl {
    ($t:ty) => {
        impl<L> Push<L> for $t
        where
            L: AsLua,
        {
            type Err = Void;      // TODO: use `!` instead (https://github.com/rust-lang/rust/issues/35121)

            #[inline]
            fn push_to_lua(self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
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
push_string_impl!{ &'_ str }

macro_rules! lua_read_string_impl {
    ($(@lt $lt:tt,)? $s:ty, $from_slice:expr) => {
        impl< $($lt,)? L> LuaRead<L> for $s
        where
            $( L: $lt, )?
            L: AsLua,
        {
            #[inline]
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

impl<L> Push<L> for bool
where
    L: AsLua,
{
    type Err = Void;      // TODO: use `!` instead (https://github.com/rust-lang/rust/issues/35121)

    #[inline]
    fn push_to_lua(self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
        unsafe {
            ffi::lua_pushboolean(lua.as_lua(), self as _);
            Ok(PushGuard::new(lua, 1))
        }
    }
}

impl<L> PushOne<L> for bool
where
    L: AsLua,
{
}

impl<L> LuaRead<L> for bool
where
    L: AsLua,
{
    #[inline]
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<bool, L> {
        if unsafe { ffi::lua_isboolean(lua.as_lua(), index.into()) } != true {
            return Err(lua);
        }

        Ok(unsafe { ffi::lua_toboolean(lua.as_lua(), index.into()) != 0 })
    }
}

impl<L> Push<L> for ()
where
    L: AsLua,
{
    type Err = Void;      // TODO: use `!` instead (https://github.com/rust-lang/rust/issues/35121)

    #[inline]
    fn push_to_lua(self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
        unsafe { Ok(PushGuard::new(lua, 0)) }
    }
}

impl<L> LuaRead<L> for ()
where
    L: AsLua,
{
    fn lua_read_at_maybe_zero_position(_: L, _: i32) -> Result<(), L> {
        Ok(())
    }

    #[inline]
    fn lua_read_at_position(_: L, _: NonZeroI32) -> Result<(), L> {
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
    fn push_to_lua(self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        match self {
            Some(val) => val.push_to_lua(lua),
            None => Ok(Nil.push_no_err(lua)),
        }
    }
}

impl<L, T> PushOne<L> for Option<T>
where
    T: PushOne<L>,
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
        if unsafe { ffi::lua_isnil(lua.as_lua(), index.into()) } {
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

#[cfg(test)]
mod tests {
    use crate::{
        AnyLuaString,
        Lua,
        StringInLua,
    };

    #[test]
    fn read_i32s() {
        let mut lua = Lua::new();

        lua.set("a", 2);

        let x: i32 = lua.get("a").unwrap();
        assert_eq!(x, 2);

        let y: i8 = lua.get("a").unwrap();
        assert_eq!(y, 2);

        let z: i16 = lua.get("a").unwrap();
        assert_eq!(z, 2);

        let w: i32 = lua.get("a").unwrap();
        assert_eq!(w, 2);

        let a: u32 = lua.get("a").unwrap();
        assert_eq!(a, 2);

        let b: u8 = lua.get("a").unwrap();
        assert_eq!(b, 2);

        let c: u16 = lua.get("a").unwrap();
        assert_eq!(c, 2);

        let d: u32 = lua.get("a").unwrap();
        assert_eq!(d, 2);
    }

    #[test]
    fn write_i32s() {
        // TODO:

        let mut lua = Lua::new();

        lua.set("a", 2);
        let x: i32 = lua.get("a").unwrap();
        assert_eq!(x, 2);
    }

    #[test]
    fn readwrite_floats() {
        let mut lua = Lua::new();

        lua.set("a", 2.51234 as f32);
        lua.set("b", 3.4123456789 as f64);

        let x: f32 = lua.get("a").unwrap();
        assert!(x - 2.51234 < 0.000001);

        let y: f64 = lua.get("a").unwrap();
        assert!(y - 2.51234 < 0.000001);

        let z: f32 = lua.get("b").unwrap();
        assert!(z - 3.4123456789 < 0.000001);

        let w: f64 = lua.get("b").unwrap();
        assert!(w - 3.4123456789 < 0.000001);
    }

    #[test]
    fn readwrite_bools() {
        let mut lua = Lua::new();

        lua.set("a", true);
        lua.set("b", false);

        let x: bool = lua.get("a").unwrap();
        assert_eq!(x, true);

        let y: bool = lua.get("b").unwrap();
        assert_eq!(y, false);
    }

    #[test]
    fn readwrite_strings() {
        let mut lua = Lua::new();

        lua.set("a", "hello");
        lua.set("b", "hello".to_string());

        let x: String = lua.get("a").unwrap();
        assert_eq!(x, "hello");

        let y: String = lua.get("b").unwrap();
        assert_eq!(y, "hello");

        assert_eq!(lua.execute::<String>("return 'abc'").unwrap(), "abc");
        assert_eq!(lua.execute::<u32>("return #'abc'").unwrap(), 3);
        assert_eq!(lua.execute::<u32>("return #'a\\x00c'").unwrap(), 3);
        assert_eq!(lua.execute::<AnyLuaString>("return 'a\\x00c'").unwrap().0, vec!(97, 0, 99));
        assert_eq!(lua.execute::<AnyLuaString>("return 'a\\x00c'").unwrap().0.len(), 3);
        assert_eq!(lua.execute::<AnyLuaString>("return '\\x01\\xff'").unwrap().0, vec!(1, 255));
        lua.execute::<String>("return 'a\\x00\\xc0'").unwrap_err();
    }

    #[test]
    fn i32_to_string() {
        let mut lua = Lua::new();

        lua.set("a", 2);

        let x: String = lua.get("a").unwrap();
        assert_eq!(x, "2");
    }

    #[test]
    fn string_to_i32() {
        let mut lua = Lua::new();

        lua.set("a", "2");
        lua.set("b", "aaa");

        let x: i32 = lua.get("a").unwrap();
        assert_eq!(x, 2);

        let y: Option<i32> = lua.get("b");
        assert!(y.is_none());
    }

    #[test]
    fn string_on_lua() {
        let mut lua = Lua::new();

        lua.set("a", "aaa");
        {
            let x: StringInLua<_> = lua.get("a").unwrap();
            assert_eq!(&*x, "aaa");
        }

        lua.set("a", 18);
        {
            let x: StringInLua<_> = lua.get("a").unwrap();
            assert_eq!(&*x, "18");
        }
    }

    #[test]
    fn push_opt() {
        let mut lua = Lua::new();

        lua.set("some", crate::function0(|| Some(123)));
        lua.set("none", crate::function0(|| Option::None::<i32>));

        match lua.execute::<i32>("return some()") {
            Ok(123) => {}
            unexpected => panic!("{:?}", unexpected),
        }

        match lua.execute::<Nil>("return none()") {
            Ok(Nil) => {}
            unexpected => panic!("{:?}", unexpected),
        }

        lua.set("no_value", None::<i32>);
        lua.set("some_value", Some("Hello!"));

        assert_eq!(lua.get("no_value"), None::<String>);
        assert_eq!(lua.get("some_value"), Some("Hello!".to_string()));
    }
}
