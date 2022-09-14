#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
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
    ($($arg:tt)*) => {
        $crate::tlog!(Warning, $($arg)*);
        if cfg!(debug_assertions) {
            panic!($($arg)*);
        }
    };
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
macro_rules! define_str_enum {
    (
        $(#[$meta:meta])*
        pub enum $enum:ident { $($space:tt = $str:literal,)+ }
        FromStr::Err = $err:ident;
    ) => {
        $(#[$meta])*
        #[derive(Debug, PartialEq, Eq, Clone, Copy, Hash, PartialOrd, Ord)]
        pub enum $enum {
            $( #[doc = $str] $space, )+
        }

        impl $enum {
            pub const fn as_str(&self) -> &str {
                match self {
                    $( Self::$space => $str, )+
                }
            }
        }

        impl AsRef<str> for $enum {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl From<$enum> for String {
            fn from(e: $enum) -> Self {
                e.as_str().into()
            }
        }

        impl std::str::FromStr for $enum {
            type Err = $err;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $( $str => Ok(Self::$space), )+
                    _ => Err($err(s.into())),
                }
            }
        }

        impl std::fmt::Display for $enum {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl serde::Serialize for $enum {
            #[inline]
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $enum {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                use serde::de::Error;
                let tmp = <&str>::deserialize(deserializer)?;
                let res = tmp.parse().map_err(|_| Error::unknown_variant(tmp, &[$($str),+]))?;
                Ok(res)
            }
        }

        impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::Push<L> for $enum {
            type Err = ::tarantool::tlua::Void;
            fn push_to_lua(&self, lua: L) -> ::tarantool::tlua::PushResult<L, Self> {
                ::tarantool::tlua::PushInto::push_into_lua(self.as_str(), lua)
            }
        }
        impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::PushOne<L> for $enum {}

        impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::PushInto<L> for $enum {
            type Err = ::tarantool::tlua::Void;
            fn push_into_lua(self, lua: L) -> ::tarantool::tlua::PushIntoResult<L, Self> {
                ::tarantool::tlua::PushInto::push_into_lua(self.as_str(), lua)
            }
        }
        impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::PushOneInto<L> for $enum {}

        impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::LuaRead<L> for $enum {
            fn lua_read_at_position(lua: L, index: std::num::NonZeroI32) -> Result<Self, L> {
                ::tarantool::tlua::StringInLua::lua_read_at_position(&lua, index).ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or(lua)
            }
        }
    }
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
        &*self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uppercase() {
        assert_eq!(&*Uppercase::from(""), "");
        assert_eq!(&*Uppercase::from("hello"), "HELLO");
        assert_eq!(&*Uppercase::from("HELLO"), "HELLO");
        assert_eq!(&*Uppercase::from("123-?!"), "123-?!");
        assert_eq!(&*Uppercase::from(String::from("hello")), "HELLO");
        assert_eq!(&*Uppercase::from(String::from("HELLO")), "HELLO");
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
