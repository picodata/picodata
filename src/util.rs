pub use Either::{Left, Right};

use crate::traft::error::Error;

use std::any::{Any, TypeId};

////////////////////////////////////////////////////////////////////////////////
/// A generic enum that contains exactly one of two possible types. Equivalent
/// to `std::result::Result`, but is more intuitive in some cases.
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Either<L, R> {
    #[inline(always)]
    pub fn map_left<F, T>(self, f: F) -> Either<T, R>
    where
        F: FnOnce(L) -> T,
    {
        match self {
            Left(l) => Left(f(l)),
            Right(r) => Right(r),
        }
    }

    #[inline(always)]
    pub fn left(self) -> Option<L> {
        match self {
            Left(l) => Some(l),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn unwrap_left_or(self, default: L) -> L {
        match self {
            Left(l) => l,
            _ => default,
        }
    }

    #[inline(always)]
    pub fn unwrap_left_or_else(self, f: impl FnOnce(R) -> L) -> L {
        match self {
            Left(l) => l,
            Right(r) => f(r),
        }
    }

    #[inline(always)]
    pub fn map_right<F, T>(self, f: F) -> Either<L, T>
    where
        F: FnOnce(R) -> T,
    {
        match self {
            Left(l) => Left(l),
            Right(r) => Right(f(r)),
        }
    }

    #[inline(always)]
    pub fn right(self) -> Option<R> {
        match self {
            Right(r) => Some(r),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn unwrap_right_or(self, default: R) -> R {
        match self {
            Right(r) => r,
            _ => default,
        }
    }

    #[inline(always)]
    pub fn unwrap_right_or_else(self, f: impl FnOnce(L) -> R) -> R {
        match self {
            Left(l) => f(l),
            Right(r) => r,
        }
    }

    #[inline(always)]
    pub fn as_ref(&self) -> Either<&L, &R> {
        match self {
            Left(l) => Left(l),
            Right(r) => Right(r),
        }
    }
}

impl<L, R> From<Result<L, R>> for Either<L, R> {
    fn from(r: Result<L, R>) -> Self {
        match r {
            Ok(l) => Left(l),
            Err(r) => Right(r),
        }
    }
}

impl<L, R> From<Either<L, R>> for Result<L, R> {
    fn from(e: Either<L, R>) -> Self {
        match e {
            Left(l) => Ok(l),
            Right(r) => Err(r),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// macros
////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! unwrap_some_or {
    ($o:expr, $($else:tt)+) => {
        match $o {
            Some(v) => v,
            None => $($else)+,
        }
    }
}

#[macro_export]
macro_rules! unwrap_ok_or {
    ($o:expr, $err:pat => $($else:tt)+) => {
        match $o {
            Ok(v) => v,
            $err => $($else)+,
        }
    }
}

#[macro_export]
macro_rules! warn_or_panic {
    ($($arg:tt)*) => {{
        $crate::tlog!(Warning, $($arg)*);
        if cfg!(debug_assertions) {
            panic!($($arg)*);
        }
    }};
}

#[macro_export]
macro_rules! stringify_debug {
    ($t:ty) => {{
        fn _check_debug<T: std::fmt::Debug>() {}
        _check_debug::<$t>();
        ::std::stringify!($t)
    }};
}

#[macro_export]
macro_rules! define_string_newtype {
    (
        $(#[$meta:meta])*
        pub struct $type:ident ( pub String );
    ) => {
        #[derive(
            Default,
            Debug,
            Eq,
            Clone,
            Hash,
            Ord,
            ::tarantool::tlua::LuaRead,
            ::tarantool::tlua::Push,
            ::tarantool::tlua::PushInto,
            serde::Serialize,
            serde::Deserialize,
        )]
        pub struct $type(pub String);

        impl ::std::fmt::Display for $type {
            fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                ::std::fmt::Display::fmt(&self.0, f)
            }
        }

        impl From<String> for $type {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $type {
            fn from(s: &str) -> Self {
                Self(s.into())
            }
        }

        impl From<$type> for String {
            fn from(i: $type) -> Self {
                i.0
            }
        }

        impl AsRef<str> for $type {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl ::std::borrow::Borrow<str> for $type {
            fn borrow(&self) -> &str {
                &self.0
            }
        }

        impl ::std::ops::Deref for $type {
            type Target = str;
            fn deref(&self) -> &str {
                &self.0
            }
        }

        impl<T> ::std::cmp::PartialEq<T> for $type
        where
            T: ?Sized,
            T: AsRef<str>,
        {
            fn eq(&self, rhs: &T) -> bool {
                self.0 == rhs.as_ref()
            }
        }

        impl<T> ::std::cmp::PartialOrd<T> for $type
        where
            T: ?Sized,
            T: AsRef<str>,
        {
            fn partial_cmp(&self, rhs: &T) -> Option<::std::cmp::Ordering> {
                (*self.0).partial_cmp(rhs.as_ref())
            }
        }

        impl ::std::str::FromStr for $type {
            type Err = ::std::convert::Infallible;

            fn from_str(s: &str) -> ::std::result::Result<Self, ::std::convert::Infallible> {
                Ok(Self(s.into()))
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////
/// A wrapper around `String` that garantees the string is uppercase by
/// converting it to uppercase (if needed) on construction.
#[derive(Default, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, serde::Serialize)]
pub struct Uppercase(String);

impl<'de> serde::Deserialize<'de> for Uppercase {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(String::deserialize(de)?.into())
    }
}

impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::Push<L> for Uppercase {
    type Err = ::tarantool::tlua::Void;

    fn push_to_lua(&self, lua: L) -> Result<tarantool::tlua::PushGuard<L>, (Self::Err, L)> {
        self.0.push_to_lua(lua)
    }
}

impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::PushOne<L> for Uppercase {}

impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::PushInto<L> for Uppercase {
    type Err = ::tarantool::tlua::Void;

    fn push_into_lua(self, lua: L) -> Result<tarantool::tlua::PushGuard<L>, (Self::Err, L)> {
        self.0.push_into_lua(lua)
    }
}

impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::PushOneInto<L> for Uppercase {}

impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::LuaRead<L> for Uppercase {
    fn lua_read_at_position(lua: L, index: std::num::NonZeroI32) -> Result<Self, L> {
        Ok(String::lua_read_at_position(lua, index)?.into())
    }
}

impl From<String> for Uppercase {
    fn from(s: String) -> Self {
        if s.chars().all(char::is_uppercase) {
            Self(s)
        } else {
            Self(s.to_uppercase())
        }
    }
}

impl From<&str> for Uppercase {
    fn from(s: &str) -> Self {
        Self(s.to_uppercase())
    }
}

impl From<Uppercase> for String {
    fn from(u: Uppercase) -> Self {
        u.0
    }
}

impl std::ops::Deref for Uppercase {
    type Target = String;

    fn deref(&self) -> &String {
        &self.0
    }
}

impl std::fmt::Display for Uppercase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::borrow::Borrow<str> for Uppercase {
    fn borrow(&self) -> &str {
        &self.0
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Compare string literals at compile time.

#[allow(dead_code)] // suppress the warning since it's only used at compile time
pub const fn str_eq(lhs: &str, rhs: &str) -> bool {
    let lhs = lhs.as_bytes();
    let rhs = rhs.as_bytes();
    if lhs.len() != rhs.len() {
        return false;
    }
    let mut i = 0;
    loop {
        if i == lhs.len() {
            return true;
        }
        if lhs[i] != rhs[i] {
            return false;
        }
        i += 1;
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Return terminal screen size in rows, columns.
pub fn screen_size() -> (i32, i32) {
    let mut rows = std::mem::MaybeUninit::uninit();
    let mut cols = std::mem::MaybeUninit::uninit();
    unsafe {
        rl_get_screen_size(rows.as_mut_ptr(), cols.as_mut_ptr());
        return (rows.assume_init() as _, cols.assume_init() as _);
    }

    use std::os::raw::c_int;
    extern "C" {
        pub fn rl_get_screen_size(rows: *mut c_int, cols: *mut c_int);
    }
}

////////////////////////////////////////////////////////////////////////////////
/// An extention for [`std::any::Any`] that includes a `type_name` method for
/// getting the type name from a `dyn AnyWithTypeName`.
pub trait AnyWithTypeName: Any {
    fn type_name(&self) -> &'static str;
}

impl<T: Any> AnyWithTypeName for T {
    #[inline]
    fn type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }
}

#[inline]
pub fn downcast<T: 'static>(any: Box<dyn AnyWithTypeName>) -> Result<T, Error> {
    if TypeId::of::<T>() != (*any).type_id() {
        return Err(Error::DowncastError {
            expected: std::any::type_name::<T>(),
            actual: (*any).type_name(),
        });
    }

    unsafe {
        let raw: *mut dyn AnyWithTypeName = Box::into_raw(any);
        Ok(*Box::from_raw(raw as *mut T))
    }
}

////////////////////////////////////////////////////////////////////////////////
/// A helper struct for displaying transitions between 2 values.
pub struct Transition<T, U> {
    pub from: T,
    pub to: U,
}

impl<T, U> std::fmt::Display for Transition<T, U>
where
    T: std::fmt::Display,
    U: std::fmt::Display,
    T: PartialEq<U>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.from == self.to {
            write!(f, "{}", self.to)
        } else {
            write!(f, "{} -> {}", self.from, self.to)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
/// IsSameType

pub trait IsSameType<L, R> {
    type Void;
}

impl<T> IsSameType<T, T> for T {
    type Void = ();
}

#[allow(unused)]
pub type CheckIsSameType<L, R> = <L as IsSameType<L, R>>::Void;

////////////////////////////////////////////////////////////////////////////////
/// tests
#[cfg(test)]
mod tests {
    #[test]
    fn uppercase() {
        use super::Uppercase;
        assert_eq!(&*Uppercase::from(""), "");
        assert_eq!(&*Uppercase::from("hello"), "HELLO");
        assert_eq!(&*Uppercase::from("HELLO"), "HELLO");
        assert_eq!(&*Uppercase::from("123-?!"), "123-?!");
        assert_eq!(&*Uppercase::from(String::from("hello")), "HELLO");
        assert_eq!(&*Uppercase::from(String::from("HELLO")), "HELLO");
    }

    #[test]
    fn str_eq() {
        use super::str_eq;
        assert!(str_eq("", ""));
        assert!(str_eq("a", "a"));
        assert!(str_eq("\0b", "\0b"));
        assert!(str_eq("foobar", concat!("foo", "bar")));

        assert!(!str_eq("", "x"));
        assert!(!str_eq("x", ""));
        assert!(!str_eq("x", "y"));
        assert!(!str_eq("ы", "Ы"));
        assert!(!str_eq("\0x", "\0y"));
        assert!(!str_eq("foo1", "bar1"));
        assert!(!str_eq("foo1", "foo2"));
    }

    #[test]
    fn downcast() {
        assert_eq!(super::downcast::<u8>(Box::new(13_u8)).unwrap(), 13);
        let err = super::downcast::<i8>(Box::new(13_u8)).unwrap_err();
        assert_eq!(
            err.to_string(),
            r#"downcast error: expected "i8", actual: "u8""#
        );
    }
}
