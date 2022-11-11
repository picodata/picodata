#[macro_export]
macro_rules! define_str_enum {
    (
        $(#[$meta:meta])*
        pub enum $enum:ident { $($space:tt = $str:literal,)+ }
        FromStr::Err = $err:ident;
    ) => {
        $(#[$meta])*
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
                let s = s.trim();
                let s = s.to_lowercase();
                let s = s.as_str();
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

        impl<L: tlua::AsLua> tlua::Push<L> for $enum {
            type Err = tlua::Void;
            fn push_to_lua(&self, lua: L) -> tlua::PushResult<L, Self> {
                tlua::PushInto::push_into_lua(self.as_str(), lua)
            }
        }
        impl<L: tlua::AsLua> tlua::PushOne<L> for $enum {}

        impl<L: tlua::AsLua> tlua::PushInto<L> for $enum {
            type Err = tlua::Void;
            fn push_into_lua(self, lua: L) -> tlua::PushIntoResult<L, Self> {
                tlua::PushInto::push_into_lua(self.as_str(), lua)
            }
        }
        impl<L: tlua::AsLua> tlua::PushOneInto<L> for $enum {}

        impl<L: tlua::AsLua> tlua::LuaRead<L> for $enum {
            fn lua_read_at_position(lua: L, index: std::num::NonZeroI32) -> Result<Self, L> {
                tlua::StringInLua::lua_read_at_position(&lua, index).ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or(lua)
            }
        }
    }
}
