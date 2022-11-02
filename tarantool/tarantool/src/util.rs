use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

pub trait IntoClones<Tuple>: Clone {
    fn into_clones(self) -> Tuple;
}

macro_rules! impl_into_clones {
    // [@clones(self) T (...)] => [(... self,)]
    [@clones($self:ident) $h:ident ($($code:tt)*)] => { ($($code)* $self,) };
    // [@clones(self) T T ... T (...)] => [@clones(self) T ... T (... self.clone(),)]
    [@clones($self:ident) $h:ident $($t:ident)+ ($($code:tt)*)] => {
        impl_into_clones![
            @clones($self) $($t)+ ($($code)* $self.clone(),)
        ]
    };
    {$h:ident $($t:ident)*} => {
        impl<$h: Clone> IntoClones<($h $(, $t)*,)> for $h {
            fn into_clones(self) -> ($h $(, $t)*,) {
                // [@clones(self) T T ... T ()]
                impl_into_clones![@clones(self) $h $($t)* ()]
            }
        }
        impl_into_clones!{$($t)*}
    };
    () => {};
}

impl_into_clones! {T T T T T T T T T T T}

#[macro_export]
macro_rules! tuple_from_box_api {
    ($f:path [ $($args:expr),* , @out ]) => {
        {
            let mut result = ::std::mem::MaybeUninit::uninit();
            #[allow(unused_unsafe)]
            unsafe {
                if $f($($args),*, result.as_mut_ptr()) < 0 {
                    return Err($crate::error::TarantoolError::last().into());
                }
                Ok($crate::tuple::Tuple::try_from_ptr(result.assume_init()))
            }
        }
    }
}

#[macro_export]
macro_rules! expr_count {
    () => { 0 };
    ($head:expr $(, $tail:expr)*) => { 1 + $crate::expr_count!($($tail),*) }
}

#[inline]
pub fn rmp_to_vec<T>(val: &T) -> Result<Vec<u8>, Error>
where
    T: Serialize + ?Sized,
{
    Ok(rmp_serde::to_vec(val)?)
}

#[derive(Clone, Debug, Serialize, Deserialize, tlua::Push)]
pub enum NumOrStr {
    Num(u32),
    // TODO(gmoshkin): this should be a `&str` instead, but
    // `#[derive(tlua::Push)]` doesn't support generic parameters yet
    Str(String),
}

impl From<u32> for NumOrStr {
    #[inline(always)]
    fn from(n: u32) -> Self {
        Self::Num(n)
    }
}

impl From<String> for NumOrStr {
    #[inline(always)]
    fn from(s: String) -> Self {
        Self::Str(s)
    }
}

impl<'a> From<&'a str> for NumOrStr {
    #[inline(always)]
    fn from(s: &'a str) -> Self {
        Self::Str(s.into())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum Value<'a> {
    Num(u32),
    Str(Cow<'a, str>),
    Bool(bool),
}

#[macro_export]
macro_rules! unwrap_or {
    ($o:expr, $else:expr) => {
        if let Some(v) = $o {
            v
        } else {
            $else
        }
    };
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
