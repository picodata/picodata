//! This module provides custom traits  [`Encode`] and [`Decode`] for
//! (de)serialization from/to msgpack and corresponding [`encode`], ['decode]
//! functions, which use the traits with default configuration options.
//!
//! The traits are implemented for several
//! common types, for other types they can be easily derived.
//! See trait documentation for more.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::{Read, Write};
use std::ops::Deref;

pub use tarantool_proc::{Decode, Encode};

/// Encodes `value` as a vector of bytes in msgpack.
///
/// See [`Encode`].
#[inline(always)]
pub fn encode(value: &impl Encode) -> Result<Vec<u8>, EncodeError> {
    let mut v = Vec::new();
    value.encode(&mut v, &Context::DEFAULT)?;
    Ok(v)
}

/// Decodes `T` from a slice of bytes in msgpack.
///
/// See [`Decode`].
#[inline(always)]
pub fn decode<T: Decode>(mut bytes: &[u8]) -> Result<T, DecodeError> {
    T::decode(&mut bytes, &Context::DEFAULT)
}

/// Additional parameters that influence (de)serializetion through
/// [`Encode`] and ['Decode'].
pub struct Context {
    /// Defines if the struct itself controls its (de)serialization
    /// or if it is overriden.
    ///
    /// See [`Encode`], ['Decode'].
    pub style: EncodeStyle,
}

impl Context {
    pub const DEFAULT: Self = Self {
        style: EncodeStyle::Default,
    };
}

impl Default for Context {
    #[inline(always)]
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl Context {
    #[inline(always)]
    pub fn new(style: EncodeStyle) -> Self {
        Self { style }
    }
}

/// Defines if the struct itself controls its (de)serialization
/// or if it is overriden.
///
/// See [`Encode`], ['Decode'].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EncodeStyle {
    /// Respects struct level attributes such as `as_map`.
    #[default]
    Default,
    /// Overrides struct level attributes such as `as_map`.
    /// Forces the top level struct to be serialized as `MP_MAP`.
    ForceAsMap,
    /// Overrides struct level attributes such as `as_map`.
    /// Forces the top level struct to be serialized as `MP_ARRAY`.
    ForceAsArray,
}

/// A general purpose trait for msgpack deserialization.
/// Reads `self` from a reader supplied in `r`.
///
/// For most use cases this trait can be derived (`#[derive(Decode)]`).
/// When deriving the trait for a structure it's possible to additionally specify
/// if the structure should be represented as `MP_MAP` or as an `MP_ARRAY`.
/// `MP_ARRAY` is chosen by default for compactness. To deserailize a structure as an `MP_MAP`
/// add [`encode(as_map)`] attribute to it.
///
/// E.g. given `let foo = Foo { a: 1, b: 3}`
/// As `MP_ARRAY` `foo` should be identical to `(1, 3)` during serialization.
/// As `MP_MAP` `foo` should be identical to `HashMap<String, usize>` with
/// keys `"a"` and `"b"` and values `1`, `3` accordingly.
///
/// `context.style` let's you override `as_map` attribute if it is defined for a struct.
/// does not override behavior of std types. To override supply `Encode::ForceAsMap` or
/// `EncodeStyle::ForceAsArray`. To leave the behavior up to the struct set it to `Encode::Default`.
///
/// It should replace `tuple::Decode` when it's ready.
///
/// # Example
/// ```
/// use tarantool::msgpack::Decode;
///
/// #[derive(Decode, Debug, PartialEq)]
/// struct Foo {
///     a: usize,
///     b: usize,
/// };
///
/// let buffer: Vec<u8> = vec![0x92, 0x01, 0x03];
/// let foo = <Foo as Decode>::decode(&mut &buffer[..], &Default::default()).unwrap();
/// assert_eq!(foo, Foo {a: 1, b: 3});
/// ```
// TODO: Use this trait instead of `tuple::Decode`, replace derive `Deserialize` with derive `Decode`
pub trait Decode: Sized {
    fn decode(r: &mut &[u8], context: &Context) -> Result<Self, DecodeError>;
}

// TODO: Provide a similar error type for encode
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError {
    /// Type being decoded.
    ty: &'static str,
    /// Field, element or some other part of the decoded type.
    part: Option<String>,
    // It is just a string for simplicicty as we need Clone, Sync, etc.
    /// The error that is wrapped by this error.
    source: String,
}

impl Display for DecodeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "failed decoding {}", self.ty)?;
        if let Some(ref part) = self.part {
            write!(f, "({})", part)?;
        }
        write!(f, ": {}", self.source)
    }
}

impl std::error::Error for DecodeError {}

impl DecodeError {
    pub fn new<DecodedTy>(source: impl ToString) -> Self {
        Self {
            ty: std::any::type_name::<DecodedTy>(),
            source: source.to_string(),
            part: None,
        }
    }

    #[inline(always)]
    pub fn with_part(mut self, part: impl ToString) -> Self {
        self.part = Some(part.to_string());
        self
    }
}

impl Decode for () {
    #[inline(always)]
    fn decode(r: &mut &[u8], _context: &Context) -> Result<Self, DecodeError> {
        rmp::decode::read_nil(r).map_err(DecodeError::new::<Self>)?;
        Ok(())
    }
}

impl<T> Decode for Vec<T>
where
    T: Decode,
{
    #[inline]
    fn decode(r: &mut &[u8], context: &Context) -> Result<Self, DecodeError> {
        let n = rmp::decode::read_array_len(r).map_err(DecodeError::new::<Self>)? as usize;
        let mut res = Vec::with_capacity(n);
        for i in 0..n {
            res.push(
                T::decode(r, context).map_err(|err| {
                    DecodeError::new::<Self>(err).with_part(format!("element {i}"))
                })?,
            );
        }
        Ok(res)
    }
}

impl<'a, T> Decode for Cow<'a, T>
where
    T: Decode + ToOwned + ?Sized,
{
    // Clippy doesn't notice the type difference
    #[allow(clippy::redundant_clone)]
    #[inline(always)]
    fn decode(r: &mut &[u8], context: &Context) -> Result<Self, DecodeError> {
        Ok(Cow::Owned(
            <T as Decode>::decode(r, context)
                .map_err(DecodeError::new::<Self>)?
                .to_owned(),
        ))
    }
}

impl Decode for String {
    #[inline]
    fn decode(r: &mut &[u8], _context: &Context) -> Result<Self, DecodeError> {
        let n = rmp::decode::read_str_len(r).map_err(DecodeError::new::<Self>)? as usize;
        let mut buf = vec![0; n];
        r.read_exact(&mut buf).map_err(DecodeError::new::<Self>)?;
        String::from_utf8(buf).map_err(DecodeError::new::<Self>)
    }
}

impl<K, V> Decode for BTreeMap<K, V>
where
    K: Decode + Ord,
    V: Decode,
{
    #[inline]
    fn decode(r: &mut &[u8], context: &Context) -> Result<Self, DecodeError> {
        let n = rmp::decode::read_map_len(r).map_err(DecodeError::new::<Self>)?;
        let mut res = BTreeMap::new();
        for i in 0..n {
            let k = K::decode(r, context)
                .map_err(|err| DecodeError::new::<Self>(err).with_part(format!("{i}th key")))?;
            let v = V::decode(r, context)
                .map_err(|err| DecodeError::new::<Self>(err).with_part(format!("{i}th value")))?;
            res.insert(k, v);
        }
        Ok(res)
    }
}

impl Decode for char {
    #[inline(always)]
    fn decode(r: &mut &[u8], context: &Context) -> Result<Self, DecodeError> {
        let s = <String as Decode>::decode(r, context)?;
        if s.len() != 1 {
            Err(DecodeError::new::<char>(format!(
                "expected string length to be 1, got {}",
                s.len()
            )))
        } else {
            Ok(s.chars()
                .next()
                .expect("just checked that there is 1 element"))
        }
    }
}

macro_rules! impl_simple_decode {
    ($(($t:ty, $f:tt))+) => {
        $(
            impl Decode for $t{
                #[inline(always)]
                fn decode(r: &mut &[u8], _context: &Context) -> Result<Self, DecodeError> {
                    let value = rmp::decode::$f(r)
                        .map_err(DecodeError::new::<Self>)?;
                    Ok(value)
                }
            }
        )+
    }
}

impl_simple_decode! {
    (u8, read_int)
    (u16, read_int)
    (u32, read_int)
    (u64, read_int)
    (usize, read_int)
    (i8, read_int)
    (i16, read_int)
    (i32, read_int)
    (i64, read_int)
    (isize, read_int)
    (f32, read_f32)
    (f64, read_f64)
    (bool, read_bool)
}

// TODO: Provide decode for tuples and serde json value

/// A general purpose trait for msgpack serialization.
/// Writes `self` to writer supplied in `w`.
///
/// For most use cases this trait can be derived (`#[derive(Encode)]`).
/// When deriving the trait for a structure it's possible to additionally specify
/// if the structure should be represented as `MP_MAP` or as an `MP_ARRAY`.
/// `MP_ARRAY` is chosen by default for compactness. To serailize a structure as an `MP_MAP`
/// add [`encode(as_map)`] attribute to it.
///
/// E.g. given `let foo = Foo { a: 1, b: 3}`
/// As `MP_ARRAY` `foo` should be identical to `(1, 3)` during serialization.
/// As `MP_MAP` `foo` should be identical to `HashMap<String, usize>` with
/// keys `"a"` and `"b"` and values `1`, `3` accordingly.
///
/// `context.style` let's you override `as_map` attribute if it is defined for a struct.
/// does not override behavior of std types. To override supply `Encode::ForceAsMap` or
/// `EncodeStyle::ForceAsArray`. To leave the behavior up to the struct set it to `Encode::Default`.
///
/// It should replace `tuple::Encode` when it's ready.
///
/// # Example
/// ```
/// use tarantool::msgpack::Encode;
///
/// #[derive(Encode)]
/// #[encode(as_map)]
/// struct Foo {
///     a: usize,
///     b: usize,
/// };
///
/// let mut buffer = vec![];
/// Foo {a: 1, b: 3}.encode(&mut buffer, &Default::default()).unwrap();
/// ```
// TODO: Use this trait instead of `tuple::Encode`, replace derive `Serialize` to derive `Encode`
pub trait Encode {
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError>;
}

// `EncodeError` is just an IO error, but we can't get the underlying
// IO error type from rmp, so we might just as well store it as a `String`
// for simplicity.
// Also as it is an IO error the information about a type or a field
// where it happened is irrelevant.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("failed encoding: {0}")]
pub struct EncodeError(String);

impl From<rmp::encode::ValueWriteError> for EncodeError {
    fn from(err: rmp::encode::ValueWriteError) -> Self {
        Self(err.to_string())
    }
}

impl From<std::io::Error> for EncodeError {
    fn from(err: std::io::Error) -> Self {
        Self(err.to_string())
    }
}

impl Encode for () {
    #[inline(always)]
    fn encode(&self, w: &mut impl Write, _context: &Context) -> Result<(), EncodeError> {
        rmp::encode::write_nil(w)?;
        Ok(())
    }
}

impl<T> Encode for [T]
where
    T: Encode,
{
    #[inline]
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        rmp::encode::write_array_len(w, self.len() as u32)?;
        for v in self.iter() {
            v.encode(w, context)?;
        }
        Ok(())
    }
}

impl<T> Encode for Vec<T>
where
    T: Encode,
{
    #[inline(always)]
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        self[..].as_ref().encode(w, context)
    }
}

impl<'a, T> Encode for Cow<'a, T>
where
    T: Encode + ToOwned + ?Sized,
{
    #[inline(always)]
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        self.deref().encode(w, context)
    }
}

impl Encode for String {
    #[inline(always)]
    fn encode(&self, w: &mut impl Write, _context: &Context) -> Result<(), EncodeError> {
        rmp::encode::write_str(w, self).map_err(Into::into)
    }
}

impl Encode for str {
    #[inline(always)]
    fn encode(&self, w: &mut impl Write, _context: &Context) -> Result<(), EncodeError> {
        rmp::encode::write_str(w, self).map_err(Into::into)
    }
}

impl<K, V> Encode for BTreeMap<K, V>
where
    K: Encode,
    V: Encode,
{
    #[inline]
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        rmp::encode::write_map_len(w, self.len() as u32)?;
        for (k, v) in self.iter() {
            k.encode(w, context)?;
            v.encode(w, context)?;
        }
        Ok(())
    }
}

impl Encode for char {
    #[inline(always)]
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        self.to_string().encode(w, context)
    }
}

macro_rules! impl_simple_encode {
    ($(($t:ty, $f:tt, $conv:ty))+) => {
        $(
            impl Encode for $t {
                #[inline(always)]
                fn encode(&self, w: &mut impl Write, _context: &Context) -> Result<(), EncodeError> {
                    rmp::encode::$f(w, *self as $conv)?;
                    Ok(())
                }
            }
        )+
    }
}

impl_simple_encode! {
    (u8, write_uint, u64)
    (u16, write_uint, u64)
    (u32, write_uint, u64)
    (u64, write_uint, u64)
    (usize, write_uint, u64)
    (i8, write_sint, i64)
    (i16, write_sint, i64)
    (i32, write_sint, i64)
    (i64, write_sint, i64)
    (isize, write_sint, i64)
    (f32, write_f32, f32)
    (f64, write_f64, f64)
    (bool, write_bool, bool)
    (&str, write_str, &str)
}

macro_rules! _impl_array_encode {
    ($($n:literal)+) => {
        $(
            #[allow(clippy::zero_prefixed_literal)]
            impl<T> Encode for [T; $n] where T: Encode {
                #[inline]
                fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
                    rmp::encode::write_array_len(w, $n)?;
                    for item in self {
                        item.encode(w, context)?;
                    }
                    Ok(())
                }
            }
        )+
    }
}

_impl_array_encode! {
    00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15
    16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32
}

macro_rules! impl_tuple_encode {
    () => {};
    ($h:ident $($t:ident)*) => {
        #[allow(non_snake_case)]
        impl<$h, $($t),*> Encode for ($h, $($t),*)
        where
            $h: Encode,
            $($t: Encode,)*
        {
            fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
                let ($h, $($t),*) = self;
                rmp::encode::write_array_len(w, crate::expr_count!($h $(, $t)*))?;
                $h.encode(w, context)?;
                $( $t.encode(w, context)?; )*
                Ok(())
            }
        }

        impl_tuple_encode! { $($t)* }
    }
}

impl_tuple_encode! { A B C D E F G H I J K L M N O P }

impl Encode for serde_json::Value {
    #[inline]
    fn encode(&self, w: &mut impl Write, _context: &Context) -> Result<(), EncodeError> {
        let bytes = rmp_serde::to_vec(self).map_err(|e| EncodeError(e.to_string()))?;
        w.write_all(bytes.as_slice())?;
        Ok(())
    }
}

impl Encode for serde_json::Map<String, serde_json::Value> {
    #[inline]
    fn encode(&self, w: &mut impl Write, _context: &Context) -> Result<(), EncodeError> {
        let bytes = rmp_serde::to_vec(self).map_err(|e| EncodeError(e.to_string()))?;
        w.write_all(bytes.as_slice())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, io::Cursor};

    use super::{decode, encode, Context, Decode, Encode, EncodeStyle};
    use rmp::decode::Bytes;

    #[track_caller]
    fn assert_map(bytes: &[u8]) {
        let marker = rmp::decode::read_marker(&mut Bytes::new(bytes)).unwrap();
        assert!(matches!(
            dbg!(marker),
            rmp::Marker::Map16 | rmp::Marker::Map32 | rmp::Marker::FixMap(_)
        ))
    }

    #[track_caller]
    fn assert_array(bytes: &[u8]) {
        let marker = rmp::decode::read_marker(&mut Bytes::new(bytes)).unwrap();
        assert!(matches!(
            dbg!(marker),
            rmp::Marker::Array16 | rmp::Marker::Array32 | rmp::Marker::FixArray(_)
        ))
    }

    #[track_caller]
    fn assert_value(mut bytes: &[u8], v: rmpv::Value) {
        let got = rmpv::decode::read_value(&mut bytes).unwrap();
        assert_eq!(got, v);
    }

    #[test]
    fn encode_struct() {
        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        struct Test1 {
            b: u32,
        }
        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        struct Test2 {
            not_b: f32,
        }
        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate", as_map)]
        struct Test {
            a: usize,
            b: String,
            c: Test1,
        }

        // Do not override, encode as array
        let test_1 = Test1 { b: 42 };
        let bytes = encode(&test_1).unwrap();
        assert_value(
            &bytes,
            rmpv::Value::Array(vec![rmpv::Value::Integer(42.into())]),
        );
        let test_1_dec: Test1 = decode(bytes.as_slice()).unwrap();
        assert_eq!(test_1_dec, test_1);

        // Try decoding as a different struct
        let err = decode::<Test2>(bytes.as_slice()).unwrap_err();
        assert_eq!(
            err.to_string(),
            "failed decoding tarantool::msgpack::encode::tests::encode_struct::Test2(field not_b): \
            failed decoding f32: the type decoded isn't match with the expected one"
        );

        // Override, encode as map
        let mut bytes = vec![];
        test_1
            .encode(&mut bytes, &Context::new(EncodeStyle::ForceAsMap))
            .unwrap();
        assert_map(&bytes);
        let test_1_dec: Test1 = Decode::decode(
            &mut bytes.as_slice(),
            &Context::new(EncodeStyle::ForceAsMap),
        )
        .unwrap();
        assert_eq!(test_1_dec, test_1);

        // Try decoding as a different struct
        let res: Result<Test2, _> = Decode::decode(
            &mut bytes.as_slice(),
            &Context::new(EncodeStyle::ForceAsMap),
        );
        assert_eq!(
            res.unwrap_err().to_string(),
            "failed decoding tarantool::msgpack::encode::tests::encode_struct::Test2: expected field not_b, got b"
        );

        // Do not override, encode as map
        let test = Test {
            a: 1,
            b: "abc".to_owned(),
            c: test_1,
        };
        let bytes_named = encode(&test).unwrap();
        assert_value(
            &bytes_named,
            rmpv::Value::Map(vec![
                (
                    rmpv::Value::String("a".into()),
                    rmpv::Value::Integer(1.into()),
                ),
                (
                    rmpv::Value::String("b".into()),
                    rmpv::Value::String("abc".into()),
                ),
                (
                    rmpv::Value::String("c".into()),
                    rmpv::Value::Array(vec![rmpv::Value::Integer(42.into())]),
                ),
            ]),
        );
        let test_dec: Test = decode(bytes_named.as_slice()).unwrap();
        assert_eq!(test_dec, test);
        // TODO: add negative tests for nested structs

        // Override, encode as array
        let mut bytes_named = vec![];
        test.encode(&mut bytes_named, &Context::new(EncodeStyle::ForceAsArray))
            .unwrap();
        assert_array(&bytes_named);
        let test_dec: Test = Decode::decode(
            &mut bytes_named.as_slice(),
            &Context::new(EncodeStyle::ForceAsArray),
        )
        .unwrap();
        assert_eq!(test_dec, test);
    }

    #[test]
    fn encode_tuple_struct() {
        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        struct Test(u32, bool);
        let original = Test(0, true);
        let bytes = encode(&original).unwrap();
        assert_value(
            &bytes,
            rmpv::Value::Array(vec![
                rmpv::Value::Integer(0.into()),
                rmpv::Value::Boolean(true),
            ]),
        );
        let decoded: Test = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn encode_unit_struct() {
        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        struct Test;
        let original = Test;
        let bytes = encode(&original).unwrap();
        assert_value(&bytes, rmpv::Value::Nil);
        let decoded: Test = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);
    }

    #[allow(clippy::let_unit_value)]
    #[test]
    fn encode_enum() {
        // TODO: add negative tests
        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        enum Foo {
            BarUnit,
            BarTuple1(bool),
            BarTupleN((), (), ()),
            BarStruct1 { bar: bool },
            BarStructN { bar1: (), bar2: (), bar3: () },
        }
        let original = Foo::BarUnit;
        let bytes = encode(&original).unwrap();
        assert_value(
            &bytes,
            rmpv::Value::Map(vec![(
                rmpv::Value::String("BarUnit".into()),
                rmpv::Value::Nil,
            )]),
        );
        let decoded: Foo = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);

        let original = rmpv::Value::Map(vec![(
            rmpv::Value::String("BarNotHere".into()),
            rmpv::Value::Nil,
        )]);
        let mut bytes = vec![];
        rmpv::encode::write_value(&mut bytes, &original).unwrap();
        let res: Result<Foo, _> = decode(bytes.as_slice());
        assert_eq!(
            res.unwrap_err().to_string(),
            "failed decoding tarantool::msgpack::encode::tests::encode_enum::Foo: enum variant BarNotHere does not exist",
        );

        let original = Foo::BarTuple1(true);
        let bytes = encode(&original).unwrap();
        assert_value(
            &bytes,
            rmpv::Value::Map(vec![(
                rmpv::Value::String("BarTuple1".into()),
                rmpv::Value::Array(vec![rmpv::Value::Boolean(true)]),
            )]),
        );
        let decoded: Foo = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);

        let original = Foo::BarTupleN((), (), ());
        let bytes = encode(&original).unwrap();
        assert_value(
            &bytes,
            rmpv::Value::Map(vec![(
                rmpv::Value::String("BarTupleN".into()),
                rmpv::Value::Array(vec![rmpv::Value::Nil; 3]),
            )]),
        );
        let decoded: Foo = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);

        let original = Foo::BarStruct1 { bar: false };
        let bytes = encode(&original).unwrap();
        assert_value(
            &bytes,
            rmpv::Value::Map(vec![(
                rmpv::Value::String("BarStruct1".into()),
                rmpv::Value::Array(vec![rmpv::Value::Boolean(false)]),
            )]),
        );
        let decoded: Foo = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);

        let original = Foo::BarStructN {
            bar1: (),
            bar2: (),
            bar3: (),
        };
        let bytes = encode(&original).unwrap();
        assert_value(
            &bytes,
            rmpv::Value::Map(vec![(
                rmpv::Value::String("BarStructN".into()),
                rmpv::Value::Array(vec![rmpv::Value::Nil; 3]),
            )]),
        );
        let decoded: Foo = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn encode_named_with_raw_ident() {
        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate", as_map)]
        struct Test {
            r#fn: u32,
        }
        let original = Test { r#fn: 1 };
        let bytes = encode(&original).unwrap();
        let mut bytes = Cursor::new(bytes);
        let marker = rmp::decode::read_marker(&mut bytes).unwrap();
        assert!(matches!(marker, rmp::Marker::FixMap(1)));
        let mut key_bytes = vec![0; 10];
        let key = rmp::decode::read_str(&mut bytes, key_bytes.as_mut_slice()).unwrap();
        assert_eq!(key, "fn");
    }

    #[test]
    fn encode_vec() {
        let original = vec![1u32];
        let bytes = encode(&original).unwrap();
        let decoded: Vec<u32> = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);

        let original = vec![(), (), (), (), ()];
        let bytes = encode(&original).unwrap();
        let decoded: Vec<()> = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn encode_map() {
        let mut original = BTreeMap::new();
        original.insert(1, "abc".to_string());
        original.insert(2, "def".to_string());
        let bytes = encode(&original).unwrap();
        let decoded: BTreeMap<u32, String> = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn encode_str() {
        let original = "hello";
        let bytes = encode(&original).unwrap();
        let decoded: String = decode(&bytes).unwrap();
        assert_eq!(original, decoded);
    }
}
