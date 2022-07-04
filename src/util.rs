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
