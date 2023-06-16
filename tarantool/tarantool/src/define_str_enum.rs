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
/// * [`Into<&'static str>`](Into),
/// * [`PartialEq`], [`Eq`],
/// * [`PartialOrd`], [`Ord`],
/// * [`std::fmt::Debug`],
/// * [`std::fmt::Display`],
/// * [`std::hash::Hash`],
/// * [`std::ops::Deref<Target = str>`](std::ops::Deref),
/// * [`std::str::FromStr`],
/// * [`serde::Deserialize<'de>`],
/// * [`serde::Serialize`],
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
///         Red = "#FF0000",
///         Green = "#00FF00",
///         Blue = "#0000FF",
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
///             Self::Red => "#FF0000",
///             Self::Green => "#00FF00",
///             Self::Blue => "#0000FF",
///         }
///     }
/// }
/// ```
///
/// # Implicit string coercion
///
/// `#![coerce_from_str]`
///
/// By default, generated enums are case-sensitive.
///
/// This inner attribute enables implicit string coercion when enum is
/// constructed using `FromStr` trait: the string is trimmed and
/// converted to lower case before matching.
///
/// Note, that in that case string variants must be specified in lower
/// case too.
///
/// ```
/// # use tarantool::define_str_enum;
/// define_str_enum! {
///     #![coerce_from_str]
///     pub enum Season {
///         Summer = "summer",
///     }
/// }
///
/// use std::str::FromStr;
/// assert_eq!(Season::from_str("summer"), Ok(Season::Summer));
/// assert_eq!(Season::from_str("SummeR"), Ok(Season::Summer));
/// assert_eq!(Season::from_str("  SUMMER  "), Ok(Season::Summer));
/// ```
///
/// [`serde::Deserialize<'de>`]: https://docs.rs/serde/latest/serde/trait.Deserialize.html
/// [`serde::Serialize`]: https://docs.rs/serde/latest/serde/trait.Serialize.html
macro_rules! define_str_enum {
    (
        $(#![$macro_attr:ident])?
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

            $vis const fn as_cstr(&self) -> &'static ::std::ffi::CStr {
                match self {
                    $(
                        Self::$variant => unsafe {
                            ::std::ffi::CStr::from_bytes_with_nul_unchecked(
                                ::std::concat!($display, "\0").as_bytes()
                            )
                        }
                    )+
                }
            }

            #[inline(always)]
            $vis const fn values() -> &'static [&'static str] {
                &[ $( $display, )+ ]
            }
        }

        impl ::std::convert::AsRef<str> for $enum {
            #[inline(always)]
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl ::std::ops::Deref for $enum {
            type Target = str;
            #[inline(always)]
            fn deref(&self) -> &str {
                self.as_str()
            }
        }

        impl ::std::convert::From<$enum> for ::std::string::String {
            #[inline(always)]
            fn from(e: $enum) -> Self {
                e.as_str().into()
            }
        }

        impl ::std::convert::From<$enum> for &'static str {
            #[inline(always)]
            fn from(e: $enum) -> Self {
                e.as_str()
            }
        }

        impl ::std::str::FromStr for $enum {
            type Err = $crate::define_str_enum::UnknownEnumVariant<$enum>;

            fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
                use ::std::marker::PhantomData;
                use $crate::define_str_enum::UnknownEnumVariant;
                use ::std::result::Result::{Ok, Err};

                $($crate::define_str_enum! { @attr $macro_attr
                    let s = s.trim();
                    let s = s.to_lowercase();
                    let s = s.as_str();
                })?

                match s {
                    $(
                        $display => Ok(Self::$variant),
                    )+
                    _ => Err(UnknownEnumVariant(s.into(), PhantomData)),
                }
            }
        }

        impl ::std::fmt::Display for $enum {
            #[inline(always)]
            fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl serde::Serialize for $enum {
            #[inline(always)]
            fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $enum {
            #[inline]
            fn deserialize<D>(deserializer: D) -> ::std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                use ::std::result::Result::Ok;
                use serde::de::Error;
                let tmp = <&str>::deserialize(deserializer)?;
                let res = tmp.parse().map_err(|_| {
                    Error::unknown_variant(tmp, Self::values())
                })?;
                Ok(res)
            }
        }

        impl<L: $crate::tlua::AsLua> $crate::tlua::Push<L> for $enum {
            type Err = $crate::tlua::Void;
            #[inline(always)]
            fn push_to_lua(&self, lua: L) -> $crate::tlua::PushResult<L, Self> {
                $crate::tlua::PushInto::push_into_lua(self.as_str(), lua)
            }
        }
        impl<L: $crate::tlua::AsLua> $crate::tlua::PushOne<L> for $enum {}

        impl<L: $crate::tlua::AsLua> $crate::tlua::PushInto<L> for $enum {
            type Err = $crate::tlua::Void;
            #[inline(always)]
            fn push_into_lua(self, lua: L) -> $crate::tlua::PushIntoResult<L, Self> {
                $crate::tlua::PushInto::push_into_lua(self.as_str(), lua)
            }
        }
        impl<L: $crate::tlua::AsLua> $crate::tlua::PushOneInto<L> for $enum {}

        impl<L: $crate::tlua::AsLua> $crate::tlua::LuaRead<L> for $enum {
            #[inline]
            fn lua_read_at_position(
                lua: L,
                index: ::std::num::NonZeroI32
            ) -> $crate::tlua::ReadResult<Self, L> {
                let s = $crate::tlua::StringInLua::lua_read_at_position(lua, index)?;
                match s.parse() {
                    Ok(v) => Ok(v),
                    Err(_) => {
                        let e = $crate::tlua::WrongType::info("reading string enum")
                            .expected(format!("one of {:?}", Self::values()))
                            .actual(format!("string '{}'", &*s));
                        Err((s.into_inner(), e))
                    }
                }
            }
        }
    };

    (@attr coerce_from_str $($then:tt)*) => {
        $($then)*
    };

    (@attr $other:ident $($then:tt)*) => {
        compile_error!(
            concat!("unknown attribute: ", stringify!($other))
        )
    };

}
