use std::any::type_name;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;

#[derive(Debug, PartialEq, Eq)]
pub struct UnknownEnumVariant<E>(pub String, pub PhantomData<E>);

impl<E> Display for UnknownEnumVariant<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let type_name = type_name::<E>();
        let type_name = type_name.rsplit("::").next().unwrap_or(type_name);
        write!(f, "unknown {} {:?}", type_name, self.0)
    }
}

impl<E: Debug> std::error::Error for UnknownEnumVariant<E> {}

#[macro_export]
/// Auto-generate enum that maps to a string.
///
/// It automatically derives/implements the following traits:
///
/// * [`AsRef<str>`],
/// * [`Clone`],
/// * [`Copy`],
/// * [`Into<String>`],
/// * [`PartialEq`], [`Eq`],
/// * [`PartialOrd`], [`Ord`],
/// * [`std::fmt::Debug`],
/// * [`std::fmt::Display`],
/// * [`std::hash::Hash`],
/// * [`std::str::FromStr`],
/// * `serde::Deserialize<'de>`,
/// * `serde::Serialize`,
/// * [`crate::tlua::LuaRead<L>`],
/// * [`crate::tlua::Push<L>`],
/// * [`crate::tlua::PushInto<L>`],
/// * [`crate::tlua::PushOne<L>`],
/// * [`crate::tlua::PushOneInto<L>`],
///
/// # Example
///
/// ```
/// # use tarantool::define_str_enum;
/// define_str_enum! {
///     pub enum Color {
///         Red = "#ff0000",
///         Green = "#00ff00",
///         Blue = "#0000ff",
///     }
/// }
/// ```
///
/// This macro generates the following implementation:
///
/// ```
/// pub enum Color {
///     Red,
///     Green,
///     Blue,
/// }
///
/// impl Color {
///     pub const fn as_str(&self) -> &'static str {
///         match self {
///             Self::Red => "#ff0000",
///             Self::Green => "#00ff00",
///             Self::Blue => "#0000ff",
///         }
///     }
/// }
/// ```
macro_rules! define_str_enum {
    (
        $(#[$emeta:meta])*
        $vis:vis enum $enum:ident {
            $(
                $(#[$varmeta:meta])*
                $variant:ident = $display:literal,
            )+
        }
    ) => {
        $(#[$emeta])*
        #[derive(Debug, PartialEq, Eq, Clone, Copy, Hash, PartialOrd, Ord)]
        $vis enum $enum {
            $(
                $(#[$varmeta])*
                $variant,
            )+
        }

        impl $enum {
            $vis const fn as_str(&self) -> &'static str {
                match self {
                    $(
                        Self::$variant => $display,
                    )+
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

        impl ::std::str::FromStr for $enum {
            type Err = $crate::define_str_enum::UnknownEnumVariant<$enum>;

            fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
                use ::std::marker::PhantomData;
                use $crate::define_str_enum::UnknownEnumVariant;

                let s = s.trim();
                let s = s.to_lowercase();
                let s = s.as_str();
                match s {
                    $(
                        $display => Ok(Self::$variant),
                    )+
                    _ => Err(UnknownEnumVariant(s.into(), PhantomData)),
                }
            }
        }

        impl ::std::fmt::Display for $enum {
            fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl serde::Serialize for $enum {
            #[inline]
            fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $enum {
            fn deserialize<D>(deserializer: D) -> ::std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                use serde::de::Error;
                let tmp = <&str>::deserialize(deserializer)?;
                let res = tmp.parse().map_err(|_| {
                    let expected = &[
                        $(
                            $display
                        ),+
                    ];
                    Error::unknown_variant(tmp, expected)
                })?;
                Ok(res)
            }
        }

        impl<L: $crate::tlua::AsLua> $crate::tlua::Push<L> for $enum {
            type Err = $crate::tlua::Void;
            fn push_to_lua(&self, lua: L) -> $crate::tlua::PushResult<L, Self> {
                $crate::tlua::PushInto::push_into_lua(self.as_str(), lua)
            }
        }
        impl<L: $crate::tlua::AsLua> $crate::tlua::PushOne<L> for $enum {}

        impl<L: $crate::tlua::AsLua> $crate::tlua::PushInto<L> for $enum {
            type Err = $crate::tlua::Void;
            fn push_into_lua(self, lua: L) -> $crate::tlua::PushIntoResult<L, Self> {
                $crate::tlua::PushInto::push_into_lua(self.as_str(), lua)
            }
        }
        impl<L: $crate::tlua::AsLua> $crate::tlua::PushOneInto<L> for $enum {}

        impl<L: $crate::tlua::AsLua> $crate::tlua::LuaRead<L> for $enum {
            fn lua_read_at_position(
                lua: L,
                index: ::std::num::NonZeroI32
            ) -> ::std::result::Result<Self, L> {
                $crate::tlua::StringInLua::lua_read_at_position(&lua, index).ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or(lua)
            }
        }
    };
}
