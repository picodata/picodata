#[macro_export]
macro_rules! define_smolstr_newtype {
    (
        $(#[$meta:meta])*
        pub struct $type:ident ( pub SmolStr );
    ) => {
        #[derive(Default, Debug, Eq, Clone, Hash, Ord, serde::Serialize, serde::Deserialize)]
        pub struct $type(pub ::smol_str::SmolStr);

        impl ::std::fmt::Display for $type {
            fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                ::std::fmt::Display::fmt(&self.0, f)
            }
        }

        impl From<::smol_str::SmolStr> for $type {
            #[inline(always)]
            fn from(s: ::smol_str::SmolStr) -> Self {
                Self(s)
            }
        }

        impl From<String> for $type {
            #[inline(always)]
            fn from(s: String) -> Self {
                Self(s.into())
            }
        }

        impl From<&str> for $type {
            #[inline(always)]
            fn from(s: &str) -> Self {
                Self(s.into())
            }
        }

        impl From<$type> for ::smol_str::SmolStr {
            #[inline(always)]
            fn from(i: $type) -> Self {
                i.0
            }
        }

        impl From<$type> for String {
            #[inline(always)]
            fn from(i: $type) -> Self {
                i.0.into()
            }
        }

        impl AsRef<str> for $type {
            #[inline(always)]
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl ::std::borrow::Borrow<str> for $type {
            #[inline(always)]
            fn borrow(&self) -> &str {
                &self.0
            }
        }

        impl ::std::ops::Deref for $type {
            type Target = str;
            #[inline(always)]
            fn deref(&self) -> &str {
                &self.0
            }
        }

        impl<T> ::std::cmp::PartialEq<T> for $type
        where
            T: ?Sized,
            T: AsRef<str>,
        {
            #[inline(always)]
            fn eq(&self, rhs: &T) -> bool {
                self.0 == rhs.as_ref()
            }
        }

        impl<T> ::std::cmp::PartialOrd<T> for $type
        where
            T: ?Sized,
            T: AsRef<str>,
        {
            #[inline(always)]
            fn partial_cmp(&self, rhs: &T) -> Option<::std::cmp::Ordering> {
                (*self.0).partial_cmp(rhs.as_ref())
            }
        }

        impl ::std::str::FromStr for $type {
            type Err = ::std::convert::Infallible;

            #[inline(always)]
            fn from_str(s: &str) -> ::std::result::Result<Self, ::std::convert::Infallible> {
                Ok(Self(s.into()))
            }
        }

        impl<L> ::tarantool::tlua::LuaRead<L> for $type
        where
            L: ::tarantool::tlua::AsLua,
        {
            fn lua_read_at_position(
                lua: L,
                index: std::num::NonZeroI32,
            ) -> ::tarantool::tlua::ReadResult<Self, L> {
                let string_in_lua =
                    ::tarantool::tlua::StringInLua::lua_read_at_position(lua, index)?;
                Ok(Self::from(&*string_in_lua))
            }
        }

        impl<L> ::tarantool::tlua::Push<L> for $type
        where
            L: ::tarantool::tlua::AsLua,
        {
            type Err = ::tarantool::tlua::Void;

            #[inline(always)]
            fn push_to_lua(&self, lua: L) -> ::tarantool::tlua::PushResult<L, Self> {
                self.as_ref().push_to_lua(lua)
            }
        }

        impl<L> ::tarantool::tlua::PushOne<L> for $type where L: ::tarantool::tlua::AsLua {}

        impl<L> ::tarantool::tlua::PushInto<L> for $type
        where
            L: ::tarantool::tlua::AsLua,
        {
            type Err = ::tarantool::tlua::Void;

            #[inline(always)]
            fn push_into_lua(self, lua: L) -> ::tarantool::tlua::PushResult<L, Self> {
                self.as_ref().push_into_lua(lua)
            }
        }

        impl<L> ::tarantool::tlua::PushOneInto<L> for $type where L: ::tarantool::tlua::AsLua {}
    };
}
