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
/// This macro expands into something like this:
///
/// ```
/// pub enum Color {
///     Red,
///     Green,
///     Blue,
/// }
///
/// impl Color {
///     pub const VARIANTS: &[Self] = &[Self::Red, Self::Green, Self::Blue];
///
///     pub const fn as_str(&self) -> &'static str {
///         match self {
///             Self::Red => "#FF0000",
///             Self::Green => "#00FF00",
///             Self::Blue => "#0000FF",
///         }
///     }
///
///     // ... for full list see the implementation.
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
// TODO: make this into a derive macro
macro_rules! define_str_enum {
    (
        $(#![$macro_attr:ident])?
        $(#[$emeta:meta])*
        $vis:vis enum $enum:ident {
            $(
                $(#[$varmeta:meta])*
                $variant:ident = $display:literal $(= $num:literal)?,
            )+
        }
    ) => {
        $crate::define_enum_with_introspection! {
            $(#[$emeta])*
            $vis enum $enum {
                $(
                    $(#[$varmeta])*
                    $variant $(= $num)?,
                )+
            }
        }

        #[allow(dead_code)]
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

            /// Returns a slice of string values of all the variants of `Self`.
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

        impl $crate::msgpack::Encode for $enum {
            fn encode(
                &self,
                w: &mut impl std::io::Write,
                _context: &$crate::msgpack::Context,
            ) -> std::result::Result<(), $crate::msgpack::EncodeError> {
                <&str as $crate::msgpack::Encode>::encode(&self.as_str(), w, &Default::default())
            }
        }

        impl $crate::msgpack::Decode for $enum {
            fn decode(r: &mut &[u8], _context: &$crate::msgpack::Context) -> std::result::Result<Self, $crate::msgpack::DecodeError> {
                use $crate::msgpack::rmp;

                let len = rmp::decode::read_str_len(r)
                    .map_err(|err| $crate::msgpack::DecodeError::new::<Self>(err))?;
                let decoded_variant = r.get(0..(len as usize))
                    .ok_or_else(|| $crate::msgpack::DecodeError::new::<Self>("not enough data"))?;
                let decoded_variant_str = std::str::from_utf8(decoded_variant)
                    .map_err(|err| $crate::msgpack::DecodeError::new::<Self>(err))?;
                match decoded_variant_str {
                    $(
                        $display => Ok(Self::$variant),
                    )+
                    v => Err({
                        $crate::msgpack::DecodeError::new::<$enum>(
                            format!("unknown enum variant `{}`, expected on of {:?}", v, Self::values())
                        )
                    }),
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

/// Auto-generate enum with some introspection facilities, including conversion
/// from integers.
///
/// It automatically derives/implements the following traits:
///
/// * [`Clone`],
/// * [`Copy`],
/// * [`PartialEq`], [`Eq`],
/// * [`PartialOrd`], [`Ord`],
/// * [`std::fmt::Debug`],
/// * [`std::hash::Hash`],
///
/// It also defines an inherent impl for the enum.
///
/// # Example
///
/// ```
/// tarantool::define_enum_with_introspection! {
///     pub enum MyEnum {
///         A, B, C
///     }
/// }
/// ```
///
/// This macro expands into something like this:
///
/// ```
/// pub enum MyEnum {
///     A, B, C
/// }
///
/// impl MyEnum {
///     pub const VARIANTS: &[Self] = &[Self::A, Self::B, Self::C];
///     pub const MIN: Self = Self::A;
///     pub const MAX: Self = Self::C;
///
///     pub const fn variant_name(&self) -> &'static str {
///         match self {
///             Self::A => "A",
///             Self::B => "B",
///             Self::C => "C",
///         }
///     }
///
///     pub const fn from_i64(n: i64) -> Option<Self> {
///         if n <= Self::MIN as i64 || n > Self::MAX as i64 {
///             return None;
///         }
///
///         Some(unsafe { std::mem::transmute(n as u8) })
///     }
///
///     // ... for full list see the implementation.
/// }
/// ```
///
/// NOTE: currently when determining the `MIN` & `MAX` constants the enum's
/// variants are cast to `i64`, which means that discriminants with values
/// larger than `i64::MAX` will give incorrect results.
///
// TODO: make this into a derive macro
#[macro_export]
macro_rules! define_enum_with_introspection {
    (
        $(#![$macro_attr:ident])?
        $(#[$emeta:meta])*
        $vis:vis enum $enum:ident {
            $(
                $(#[$varmeta:meta])*
                $variant:ident $(= $discriminant:expr)?
            ),+
            $(,)?
        }
    ) => {
        $(#[$emeta])*
        #[derive(Debug, PartialEq, Eq, Clone, Copy, Hash, PartialOrd, Ord)]
        $vis enum $enum {
            $(
                $(#[$varmeta])*
                $variant $(= $discriminant)?,
            )+
        }

        #[allow(dead_code)]
        impl $enum {
            /// A slice of all possible enum variants.
            ///
            /// These are ordered in the order of definition in the source code.
            pub const VARIANTS: &'static [Self] = &[ $( Self::$variant, )+ ];

            /// The enum variant with the smallest discriminant.
            pub const MIN: Self = {
                let mut i = 1;
                let mut min = $enum::VARIANTS[0];
                while i < $enum::VARIANTS.len() {
                    if ($enum::VARIANTS[i] as i64) < (min as i64) {
                        min = $enum::VARIANTS[i];
                    }
                    i += 1;
                }
                min
            };

            /// The enum variant with the largest discriminant.
            pub const MAX: Self = {
                let mut i = 1;
                let mut max = $enum::VARIANTS[0];
                while i < $enum::VARIANTS.len() {
                    if ($enum::VARIANTS[i] as i64) > (max as i64) {
                        max = $enum::VARIANTS[i];
                    }
                    i += 1;
                }
                max
            };

            /// If this is `true` then all of the enum variants have subsequent
            /// discriminants and converting from integer to enum type is going
            /// to use a more efficient implementation.
            pub const DISCRIMINANTS_ARE_SUBSEQUENT: bool = {
                let len = $enum::VARIANTS.len() as u64;
                assert!(len <= i64::MAX as u64, "that's too many variants, my brother in Christ");
                let actual_span = i64::checked_sub($enum::MAX as _, $enum::MIN as _);
                if let Some(actual_span) = actual_span {
                    actual_span == (len - 1) as i64
                } else {
                    // Actual span exceeds the maximum allowed one of i64::MAX - 1
                    false
                }
            };

            /// Returns the name of the variant as it is spelled in the source
            /// code.
            pub const fn variant_name(&self) -> &'static str {
                match self {
                    $( Self::$variant => ::std::stringify!($variant), )+
                }
            }

            /// Converts integer to enum.
            ///
            /// Returns `None` if no variant of the enum has the corresponding
            /// discriminant.
            pub const fn from_i64(n: i64) -> Option<Self> {
                if !$enum::DISCRIMINANTS_ARE_SUBSEQUENT {
                    return match n {
                        $( n if n == Self::$variant as i64 => Some(Self::$variant), )+
                        _ => None,
                    };
                }

                if n < $enum::MIN as i64 || n > $enum::MAX as i64 {
                    return None;
                }

                // SAFETY: this is safe because
                // `n` is in the range of possible discriminants.
                unsafe {
                    const SIZE: usize = std::mem::size_of::<$enum>();
                    match SIZE {
                        // NOTE: we can't use std::mem::transmute here
                        // because it doesn't compile.
                        // Also note that even though value of `n` may be out of
                        // range of type to which it is being cast
                        // (e.g. `#[repr(u8)] enum E { V = 255 }`), this code is
                        // still valid, because we only care about value
                        // truncation of unsignificant bytes and the bits
                        // themselves are not being changed.
                        8 => Some(*(&(n as i64) as *const _ as *const $enum)),
                        4 => Some(*(&(n as i32) as *const _ as *const $enum)),
                        2 => Some(*(&(n as i16) as *const _ as *const $enum)),
                        1 => Some(*(&(n as i8) as *const _ as *const $enum)),
                        _ => { panic!("unreachable"); }
                    }
                }
            }
        }

        macro_rules! impl_try_from_int {
            ($t:ty) => {
                impl std::convert::TryFrom<$t> for $enum {
                    type Error = $t;
                    #[inline(always)]
                    fn try_from(n: $t) -> std::result::Result<Self, $t> {
                        Self::from_i64(n as _).ok_or(n)
                    }
                }
            }
        }

        impl_try_from_int! { i8 }
        impl_try_from_int! { u8 }
        impl_try_from_int! { i16 }
        impl_try_from_int! { u16 }
        impl_try_from_int! { i32 }
        impl_try_from_int! { u32 }
        impl_try_from_int! { i64 }
        impl_try_from_int! { u64 }
        impl_try_from_int! { isize }
        impl_try_from_int! { usize }
    }
}

#[allow(clippy::assertions_on_constants)]
#[cfg(feature = "internal_test")]
mod tests {
    use super::*;
    use crate::util::str_eq;

    ////////////////////////////////////////////////////////////////////////////

    define_enum_with_introspection! {
        #[allow(clippy::enum_clike_unportable_variant)]
        enum MyEnum {
            First,
            EvenFirster = -1,
            Foo = 1,
            Bar = 2,
            Xxx = 1027,
            Yyy,
            Baz = 3,
            ISizeMax = isize::MAX,
            // Rust only allows discriminants to be in range isize::MIN..=isize::MAX
            // USizeMax = usize::MAX,
        }
    }

    #[rustfmt::skip]
    const _: () = {
        assert!(matches!(MyEnum::MIN, MyEnum::EvenFirster));
        assert!(matches!(MyEnum::MAX, MyEnum::ISizeMax));
        assert!(MyEnum::VARIANTS.len() == 8);
        assert!(!MyEnum::DISCRIMINANTS_ARE_SUBSEQUENT);
        assert!(matches!(MyEnum::from_i64(-1), Some(MyEnum::EvenFirster)));
        assert!(matches!(MyEnum::from_i64(0), Some(MyEnum::First)));
        assert!(matches!(MyEnum::from_i64(1), Some(MyEnum::Foo)));
        assert!(matches!(MyEnum::from_i64(2), Some(MyEnum::Bar)));
        assert!(matches!(MyEnum::from_i64(3), Some(MyEnum::Baz)));
        assert!(matches!(MyEnum::from_i64(1027), Some(MyEnum::Xxx)));
        assert!(matches!(MyEnum::from_i64(1028), Some(MyEnum::Yyy)));
        assert!(matches!(MyEnum::from_i64(isize::MAX as _), Some(MyEnum::ISizeMax)));
        assert!(matches!(MyEnum::from_i64(-2), None));
        assert!(str_eq(MyEnum::EvenFirster.variant_name(), "EvenFirster"));
    };

    ////////////////////////////////////////////////////////////////////////////

    define_enum_with_introspection! {
        #[repr(i64)]
        enum BottomMost3 {
            Smallest = i64::MIN,
            NextSmallest,
            NextNextSmallest,
        }
    }

    #[rustfmt::skip]
    const _: () = {
        assert!(matches!(BottomMost3::MIN, BottomMost3::Smallest));
        assert!(matches!(BottomMost3::MAX, BottomMost3::NextNextSmallest));
        assert!(BottomMost3::VARIANTS.len() == 3);
        assert!(BottomMost3::DISCRIMINANTS_ARE_SUBSEQUENT);
        assert!(matches!(BottomMost3::from_i64(i64::MIN), Some(BottomMost3::Smallest)));
        assert!(matches!(BottomMost3::from_i64(i64::MIN + 1), Some(BottomMost3::NextSmallest)));
        assert!(matches!(BottomMost3::from_i64(i64::MIN + 2), Some(BottomMost3::NextNextSmallest)));
    };

    ////////////////////////////////////////////////////////////////////////////

    define_enum_with_introspection! {
        #[repr(i64)]
        enum TopMost3 {
            PrevPrevLargest = i64::MAX - 2,
            PrevLargest,
            Largest,
        }
    }

    #[rustfmt::skip]
    const _: () = {
        assert!(matches!(TopMost3::MIN, TopMost3::PrevPrevLargest));
        assert!(matches!(TopMost3::MAX, TopMost3::Largest));
        assert!(TopMost3::VARIANTS.len() == 3);
        assert!(TopMost3::DISCRIMINANTS_ARE_SUBSEQUENT);
        assert!(matches!(TopMost3::from_i64(i64::MAX - 2), Some(TopMost3::PrevPrevLargest)));
        assert!(matches!(TopMost3::from_i64(i64::MAX - 1), Some(TopMost3::PrevLargest)));
        assert!(matches!(TopMost3::from_i64(i64::MAX), Some(TopMost3::Largest)));
    };

    ////////////////////////////////////////////////////////////////////////////

    define_enum_with_introspection! {
        #[repr(u8)] enum SingleVariant { U8Max = u8::MAX }
    }

    #[rustfmt::skip]
    const _: () = {
        assert!(matches!(SingleVariant::from_i64(u8::MAX as _), Some(SingleVariant::U8Max)));
        assert!(matches!(SingleVariant::MIN, SingleVariant::U8Max));
        assert!(matches!(SingleVariant::MAX, SingleVariant::U8Max));
        assert!(SingleVariant::VARIANTS.len() == 1);
        assert!(SingleVariant::DISCRIMINANTS_ARE_SUBSEQUENT);
    };

    ////////////////////////////////////////////////////////////////////////////

    define_enum_with_introspection! {
        #[repr(u8)] enum AutoDiscriminantsU8 { A = u8::MIN, B, C = u8::MAX, }
    }
    #[rustfmt::skip]
    const _: () = {
        assert!(matches!(AutoDiscriminantsU8::MIN, AutoDiscriminantsU8::A));
        assert!(matches!(AutoDiscriminantsU8::MAX, AutoDiscriminantsU8::C));
        assert!(matches!(AutoDiscriminantsU8::from_i64(u8::MIN as _), Some(AutoDiscriminantsU8::A)));
        assert!(matches!(AutoDiscriminantsU8::from_i64(u8::MAX as _), Some(AutoDiscriminantsU8::C)));
        assert!(AutoDiscriminantsU8::VARIANTS.len() == 3);
        assert!(!AutoDiscriminantsU8::DISCRIMINANTS_ARE_SUBSEQUENT);
    };

    ////////////////////////////////////////////////////////////////////////////

    define_enum_with_introspection! {
        #[repr(i8)] enum AutoDiscriminantsI8 { A = i8::MIN, B, C = i8::MAX, }
    }
    #[rustfmt::skip]
    const _: () = {
        assert!(matches!(AutoDiscriminantsI8::MIN, AutoDiscriminantsI8::A));
        assert!(matches!(AutoDiscriminantsI8::MAX, AutoDiscriminantsI8::C));
        assert!(matches!(AutoDiscriminantsI8::from_i64(i8::MIN as _), Some(AutoDiscriminantsI8::A)));
        assert!(matches!(AutoDiscriminantsI8::from_i64(i8::MAX as _), Some(AutoDiscriminantsI8::C)));
        assert!(AutoDiscriminantsI8::VARIANTS.len() == 3);
        assert!(!AutoDiscriminantsI8::DISCRIMINANTS_ARE_SUBSEQUENT);
    };

    ////////////////////////////////////////////////////////////////////////////

    define_enum_with_introspection! {
        #[repr(u16)] enum AutoDiscriminantsU16 { A = u16::MIN, B, C = u16::MAX, }
    }
    #[rustfmt::skip]
    const _: () = {
        assert!(matches!(AutoDiscriminantsU16::MIN, AutoDiscriminantsU16::A));
        assert!(matches!(AutoDiscriminantsU16::MAX, AutoDiscriminantsU16::C));
        assert!(matches!(AutoDiscriminantsU16::from_i64(u16::MIN as _), Some(AutoDiscriminantsU16::A)));
        assert!(matches!(AutoDiscriminantsU16::from_i64(u16::MAX as _), Some(AutoDiscriminantsU16::C)));
        assert!(AutoDiscriminantsU16::VARIANTS.len() == 3);
        assert!(!AutoDiscriminantsU16::DISCRIMINANTS_ARE_SUBSEQUENT);
    };

    ////////////////////////////////////////////////////////////////////////////

    define_enum_with_introspection! {
        #[repr(i16)] enum AutoDiscriminantsI16 { A = i16::MIN, B, C = i16::MAX, }
    }
    #[rustfmt::skip]
    const _: () = {
        assert!(matches!(AutoDiscriminantsI16::MIN, AutoDiscriminantsI16::A));
        assert!(matches!(AutoDiscriminantsI16::MAX, AutoDiscriminantsI16::C));
        assert!(matches!(AutoDiscriminantsI16::from_i64(i16::MIN as _), Some(AutoDiscriminantsI16::A)));
        assert!(matches!(AutoDiscriminantsI16::from_i64(i16::MAX as _), Some(AutoDiscriminantsI16::C)));
        assert!(AutoDiscriminantsI16::VARIANTS.len() == 3);
        assert!(!AutoDiscriminantsI16::DISCRIMINANTS_ARE_SUBSEQUENT);
    };

    ////////////////////////////////////////////////////////////////////////////

    define_enum_with_introspection! {
        #[repr(u32)] enum AutoDiscriminantsU32 { A = u32::MIN, B, C = u32::MAX, }
    }
    #[rustfmt::skip]
    const _: () = {
        assert!(matches!(AutoDiscriminantsU32::MIN, AutoDiscriminantsU32::A));
        assert!(matches!(AutoDiscriminantsU32::MAX, AutoDiscriminantsU32::C));
        assert!(matches!(AutoDiscriminantsU32::from_i64(u32::MIN as _), Some(AutoDiscriminantsU32::A)));
        assert!(matches!(AutoDiscriminantsU32::from_i64(u32::MAX as _), Some(AutoDiscriminantsU32::C)));
        assert!(AutoDiscriminantsU32::VARIANTS.len() == 3);
        assert!(!AutoDiscriminantsU32::DISCRIMINANTS_ARE_SUBSEQUENT);
    };

    ////////////////////////////////////////////////////////////////////////////

    define_enum_with_introspection! {
        #[repr(i32)] enum AutoDiscriminantsI32 { A = i32::MIN, B, C = i32::MAX, }
    }
    #[rustfmt::skip]
    const _: () = {
        assert!(matches!(AutoDiscriminantsI32::MIN, AutoDiscriminantsI32::A));
        assert!(matches!(AutoDiscriminantsI32::MAX, AutoDiscriminantsI32::C));
        assert!(matches!(AutoDiscriminantsI32::from_i64(i32::MIN as _), Some(AutoDiscriminantsI32::A)));
        assert!(matches!(AutoDiscriminantsI32::from_i64(i32::MAX as _), Some(AutoDiscriminantsI32::C)));
        assert!(AutoDiscriminantsI32::VARIANTS.len() == 3);
        assert!(!AutoDiscriminantsI32::DISCRIMINANTS_ARE_SUBSEQUENT);
    };

    ////////////////////////////////////////////////////////////////////////////

    define_enum_with_introspection! {
        #[repr(u64)] enum AutoDiscriminantsU64 { A = u64::MIN, B, C = u64::MAX, }
    }
    #[rustfmt::skip]
    const _: () = {
        // FIXME: ### THIS TEST IS WRONG ###
        // discriminants greater than i64::MAX are currently broken
        assert!(matches!(AutoDiscriminantsU64::MIN, AutoDiscriminantsU64::C));
        assert!(matches!(AutoDiscriminantsU64::MAX, AutoDiscriminantsU64::B));
        assert!(matches!(AutoDiscriminantsU64::from_i64(u64::MIN as _), Some(AutoDiscriminantsU64::A)));
        assert!(matches!(AutoDiscriminantsU64::from_i64(u64::MAX as _), Some(AutoDiscriminantsU64::C)));
        assert!(AutoDiscriminantsU64::VARIANTS.len() == 3);
        assert!(AutoDiscriminantsU64::DISCRIMINANTS_ARE_SUBSEQUENT);
    };

    ////////////////////////////////////////////////////////////////////////////

    define_enum_with_introspection! {
        #[repr(i64)] enum AutoDiscriminantsI64 { A = i64::MIN, B, C = i64::MAX, }
    }
    #[rustfmt::skip]
    const _: () = {
        assert!(matches!(AutoDiscriminantsI64::MIN, AutoDiscriminantsI64::A));
        assert!(matches!(AutoDiscriminantsI64::MAX, AutoDiscriminantsI64::C));
        assert!(matches!(AutoDiscriminantsI64::from_i64(i64::MIN as _), Some(AutoDiscriminantsI64::A)));
        assert!(matches!(AutoDiscriminantsI64::from_i64(i64::MAX as _), Some(AutoDiscriminantsI64::C)));
        assert!(AutoDiscriminantsI64::VARIANTS.len() == 3);
        assert!(!AutoDiscriminantsI64::DISCRIMINANTS_ARE_SUBSEQUENT);
    };

    ////////////////////////////////////////////////////////////////////////////

    define_str_enum! {
        enum StrEnumWithIntrospection {
            One = "Two" = 3,
            Next = "autodiscriminant",
            Food = "food" = 0xf00d,
        }
    }
    #[rustfmt::skip]
    const _: () = {
        assert!(matches!(StrEnumWithIntrospection::MIN, StrEnumWithIntrospection::One));
        assert!(matches!(StrEnumWithIntrospection::MAX, StrEnumWithIntrospection::Food));
        assert!(StrEnumWithIntrospection::VARIANTS.len() == 3);
        assert!(!StrEnumWithIntrospection::DISCRIMINANTS_ARE_SUBSEQUENT);

        assert!(str_eq(StrEnumWithIntrospection::One.variant_name(), "One"));
        assert!(str_eq(StrEnumWithIntrospection::One.as_str(), "Two"));
        assert!(StrEnumWithIntrospection::One as i64 == 3);

        assert!(str_eq(StrEnumWithIntrospection::Next.variant_name(), "Next"));
        assert!(str_eq(StrEnumWithIntrospection::Next.as_str(), "autodiscriminant"));
        assert!(StrEnumWithIntrospection::Next as i64 == 4);

        assert!(str_eq(StrEnumWithIntrospection::Food.variant_name(), "Food"));
        assert!(str_eq(StrEnumWithIntrospection::Food.as_str(), "food"));
        assert!(StrEnumWithIntrospection::Food as i64 == 0xf00d);
    };
}
