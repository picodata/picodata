//! This module provides custom traits  [`Encode`] and [`Decode`] for
//! (de)serialization from/to msgpack and corresponding [`encode`], ['decode]
//! functions, which use the traits with default configuration options.
//!
//! The traits are implemented for several
//! common types, for other types they can be easily derived.
//! See trait documentation for more.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::Hash;
use std::io::{Read, Write};
use std::ops::Deref;

pub use tarantool_proc::{Decode, Encode};

/// Encodes `value` as a vector of bytes in msgpack.
///
/// See [`Encode`].
#[inline(always)]
pub fn encode(value: &impl Encode) -> Result<Vec<u8>, EncodeError> {
    // 128 is chosen pretty randomly, we might want to benchmark this to find
    // better values
    let mut v = Vec::with_capacity(128);
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

////////////////////////////////////////////////////////////////////////////////
// Context
////////////////////////////////////////////////////////////////////////////////

/// Additional parameters that influence (de)serializetion through
/// [`Encode`] and ['Decode'].
pub struct Context {
    /// Defines the (de)serialization style for structs.
    struct_style: StructStyle,
    // TODO: parameter which allows encoding/decoding Vec<u8> as string and/or binary
    // TODO: maybe we should allow empty input to be decoded as `Option::None`,
    // but this should be configurable via context & not sure if this may break
    // deserialization in some case, e.g. when doing `untagged` style decoding
    // of enums.
}

impl Context {
    /// A default instance of `Context`.
    ///
    /// This is also an enforcement of the fact that the default `Context` can
    /// be constructed at compile time.
    pub const DEFAULT: Self = Self {
        struct_style: StructStyle::Default,
    };
}

impl Default for Context {
    #[inline(always)]
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl Context {
    /// A builder-style method which sets `struct_style` and returns `self` by
    /// value.
    #[inline(always)]
    pub const fn with_struct_style(mut self, struct_style: StructStyle) -> Self {
        self.struct_style = struct_style;
        self
    }

    /// Returns the style of encoding for structs set by this context.
    #[inline(always)]
    pub fn struct_style(&self) -> StructStyle {
        self.struct_style
    }
}

/// Defines the (de)serialization style for structs.
///
/// See [`Encode`], [`Decode`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StructStyle {
    /// Respects struct level attributes such as `as_map`.
    #[default]
    Default,
    /// Overrides struct level attributes such as `as_map`.
    /// Forces the struct and all nested structs to be serialized as `MP_MAP`.
    ForceAsMap,
    /// Overrides struct level attributes such as `as_map`.
    /// Forces the struct and all nested struct to be serialized as `MP_ARRAY`.
    ForceAsArray,
    // TODO ForceAsMapTopLevel
    // TODO ForceAsArrayTopLevel
    // TODO AllowDecodeAny - to allow decoding both arrays & maps
}

////////////////////////////////////////////////////////////////////////////////
// Decode
////////////////////////////////////////////////////////////////////////////////

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
/// `StructStyle::ForceAsArray`. To leave the behavior up to the struct set it to `Encode::Default`.
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

////////////////////////////////////////////////////////////////////////////////
// DecodeError
////////////////////////////////////////////////////////////////////////////////

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
    #[inline(always)]
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

////////////////////////////////////////////////////////////////////////////////
// impl Decode
////////////////////////////////////////////////////////////////////////////////

impl Decode for () {
    #[inline(always)]
    fn decode(r: &mut &[u8], _context: &Context) -> Result<Self, DecodeError> {
        rmp::decode::read_nil(r).map_err(DecodeError::new::<Self>)?;
        Ok(())
    }
}

impl<T> Decode for Box<T>
where
    T: Decode,
{
    #[inline(always)]
    fn decode(r: &mut &[u8], context: &Context) -> Result<Self, DecodeError> {
        T::decode(r, context).map(Box::new)
    }
}

impl<T> Decode for std::rc::Rc<T>
where
    T: Decode,
{
    #[inline(always)]
    fn decode(r: &mut &[u8], context: &Context) -> Result<Self, DecodeError> {
        T::decode(r, context).map(std::rc::Rc::new)
    }
}

impl<T> Decode for Option<T>
where
    T: Decode,
{
    #[inline(always)]
    fn decode(r: &mut &[u8], context: &Context) -> Result<Self, DecodeError> {
        // In case input is empty, don't return `None` but call the T::decode.
        // This will allow some users to handle empty input the way they want,
        // if they want to.
        if !r.is_empty() && r[0] == super::MARKER_NULL {
            rmp::decode::read_nil(r).map_err(DecodeError::new::<Self>)?;
            Ok(None)
        } else {
            T::decode(r, context).map(Some)
        }
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

impl<T> Decode for HashSet<T>
where
    T: Decode + Hash + Eq,
{
    #[inline]
    fn decode(r: &mut &[u8], context: &Context) -> Result<Self, DecodeError> {
        let n = rmp::decode::read_array_len(r).map_err(DecodeError::new::<Self>)? as usize;
        let mut res = HashSet::with_capacity(n);
        for i in 0..n {
            let v = T::decode(r, context)
                .map_err(|err| DecodeError::new::<Self>(err).with_part(format!("element {i}")))?;
            res.insert(v);
        }
        Ok(res)
    }
}

impl<T> Decode for BTreeSet<T>
where
    T: Decode + Ord + Eq,
{
    #[inline]
    fn decode(r: &mut &[u8], context: &Context) -> Result<Self, DecodeError> {
        let n = rmp::decode::read_array_len(r).map_err(DecodeError::new::<Self>)? as usize;
        let mut res = BTreeSet::new();
        for i in 0..n {
            let v = T::decode(r, context)
                .map_err(|err| DecodeError::new::<Self>(err).with_part(format!("element {i}")))?;
            res.insert(v);
        }
        Ok(res)
    }
}

impl<T, const N: usize> Decode for [T; N]
where
    T: Decode,
{
    fn decode(r: &mut &[u8], context: &Context) -> Result<Self, DecodeError> {
        let n = rmp::decode::read_array_len(r).map_err(DecodeError::new::<Self>)? as usize;
        if n != N {
            return Err(DecodeError::new::<Self>(format!(
                "expected array count {N}, got {n}"
            )));
        }

        let mut res = std::mem::MaybeUninit::uninit();
        let ptr = &mut res as *mut _ as *mut [T; N] as *mut T;
        let mut num_assigned = 0;

        for i in 0..N {
            match T::decode(r, context) {
                Ok(v) => {
                    // SAFETY: safe, because MaybeUninit<[T; N]> has the same
                    // memory representation as [T; N], and we're writing into
                    // the array's elements.
                    unsafe { std::ptr::write(ptr.add(i), v) }
                    num_assigned += 1;
                }
                Err(e) => {
                    for i in 0..num_assigned {
                        // SAFETY: safe, because we assigned all of these elements
                        // a valid value of type T.
                        unsafe { std::ptr::drop_in_place(ptr.add(i)) }
                    }

                    return Err(DecodeError::new::<Self>(e).with_part(format!("element {i}")));
                }
            }
        }

        debug_assert_eq!(num_assigned, N);

        // SAFETY: safe, we've assigned every single element.
        return Ok(unsafe { res.assume_init() });
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

impl<K, V> Decode for HashMap<K, V>
where
    K: Decode + Ord + Hash,
    V: Decode,
{
    #[inline]
    fn decode(r: &mut &[u8], context: &Context) -> Result<Self, DecodeError> {
        let n = rmp::decode::read_map_len(r).map_err(DecodeError::new::<Self>)?;
        let mut res = HashMap::with_capacity(n as _);
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
    fn decode(r: &mut &[u8], _context: &Context) -> Result<Self, DecodeError> {
        let n = rmp::decode::read_str_len(r).map_err(DecodeError::new::<Self>)? as usize;
        if n == 0 {
            return Err(DecodeError::new::<char>(
                "expected a msgpack non-empty string, got string length 0",
            ));
        }
        if n > 4 {
            return Err(DecodeError::new::<char>(format!(
                "expected a msgpack string not longer than 4 characters, got length {n}"
            )));
        }
        let mut buf = [0; 4];
        let buf = &mut buf[0..n];
        r.read_exact(buf).map_err(DecodeError::new::<Self>)?;
        let s = std::str::from_utf8(buf).map_err(DecodeError::new::<Self>)?;
        if s.chars().count() != 1 {
            return Err(DecodeError::new::<char>(format!(
                "expected a single unicode character, got sequence of length {n}"
            )));
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

////////////////////////////////////////////////////////////////////////////////
// Encode
////////////////////////////////////////////////////////////////////////////////

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
/// `StructStyle::ForceAsArray`. To leave the behavior up to the struct set it to `Encode::Default`.
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

////////////////////////////////////////////////////////////////////////////////
// EncodeError
////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////
// impl Encode
////////////////////////////////////////////////////////////////////////////////

impl Encode for () {
    #[inline(always)]
    fn encode(&self, w: &mut impl Write, _context: &Context) -> Result<(), EncodeError> {
        rmp::encode::write_nil(w)?;
        Ok(())
    }
}

impl<T> Encode for &'_ T
where
    T: Encode + ?Sized,
{
    #[inline(always)]
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        (**self).encode(w, context)
    }
}

impl<T> Encode for &'_ mut T
where
    T: Encode + ?Sized,
{
    #[inline(always)]
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        (**self).encode(w, context)
    }
}

impl<T> Encode for Box<T>
where
    T: Encode,
{
    #[inline(always)]
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        (**self).encode(w, context)
    }
}

impl<T> Encode for std::rc::Rc<T>
where
    T: Encode,
{
    #[inline(always)]
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        (**self).encode(w, context)
    }
}

impl<T> Encode for Option<T>
where
    T: Encode,
{
    #[inline(always)]
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        if let Some(v) = self {
            v.encode(w, context)
        } else {
            rmp::encode::write_nil(w)?;
            Ok(())
        }
    }
}

impl<T> Encode for [T]
where
    T: Encode,
{
    #[inline]
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        rmp::encode::write_array_len(w, self.len() as _)?;
        for v in self {
            v.encode(w, context)?;
        }
        Ok(())
    }
}

impl<T> Encode for BTreeSet<T>
where
    T: Encode,
{
    #[inline]
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        rmp::encode::write_array_len(w, self.len() as _)?;
        for v in self {
            v.encode(w, context)?;
        }
        Ok(())
    }
}

impl<T> Encode for HashSet<T>
where
    T: Encode,
{
    #[inline]
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        rmp::encode::write_array_len(w, self.len() as _)?;
        for v in self {
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

impl<K, V> Encode for HashMap<K, V>
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
    fn encode(&self, w: &mut impl Write, _context: &Context) -> Result<(), EncodeError> {
        let mut buf = [0; 4];
        let s = self.encode_utf8(&mut buf);
        rmp::encode::write_str(w, s)?;
        Ok(())
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
}

impl<T, const N: usize> Encode for [T; N]
where
    T: Encode,
{
    #[inline]
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        rmp::encode::write_array_len(w, N as _)?;
        for item in self {
            item.encode(w, context)?;
        }
        Ok(())
    }
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
        // TODO: custom implementation. It is super simple, at some point we
        // will get rid of rmp_serde dependency.
        let bytes = rmp_serde::to_vec(self).map_err(|e| EncodeError(e.to_string()))?;
        w.write_all(bytes.as_slice())?;
        Ok(())
    }
}

impl Encode for serde_json::Map<String, serde_json::Value> {
    #[inline]
    fn encode(&self, w: &mut impl Write, _context: &Context) -> Result<(), EncodeError> {
        // TODO: custom implementation. It is super simple, at some point we
        // will get rid of rmp_serde dependency.
        let bytes = rmp_serde::to_vec(self).map_err(|e| EncodeError(e.to_string()))?;
        w.write_all(bytes.as_slice())?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use rmpv::Value;
    use std::{collections::BTreeMap, io::Cursor};

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
        let ctx_as_map = Context::default().with_struct_style(StructStyle::ForceAsMap);
        let mut bytes = vec![];
        test_1.encode(&mut bytes, &ctx_as_map).unwrap();
        assert_value(
            &bytes,
            Value::Map(vec![(Value::from("b"), Value::from(42))]),
        );
        let test_1_dec = Test1::decode(&mut bytes.as_slice(), &ctx_as_map).unwrap();
        assert_eq!(test_1_dec, test_1);

        // Try decoding as a different struct
        let e = Test2::decode(&mut bytes.as_slice(), &ctx_as_map).unwrap_err();
        assert_eq!(
            e.to_string(),
            "failed decoding tarantool::msgpack::encode::tests::encode_struct::Test2: expected field not_b, got b"
        );
    }

    #[test]
    fn encode_nested_struct() {
        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        // Wants to be encoded as map
        #[encode(as_map)]
        struct Outer {
            i: usize,
            s: String,
            inner: Inner,
        }

        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        // Wants to be encoded as array
        struct Inner {
            i: usize,
            s: String,
        }

        let test = Outer {
            i: 1,
            s: "abc".into(),
            inner: Inner {
                i: 2,
                s: "def".into(),
            },
        };

        // Do not override, encode as map
        let bytes = encode(&test).unwrap();
        assert_value(
            &bytes,
            Value::Map(vec![
                (Value::from("i"), Value::from(1)),
                (Value::from("s"), Value::from("abc")),
                (
                    Value::from("inner"),
                    Value::Array(vec![Value::from(2), Value::from("def")]),
                ),
            ]),
        );
        let test_dec: Outer = decode(bytes.as_slice()).unwrap();
        assert_eq!(test_dec, test);

        // Override, encode as array
        let ctx_as_array = Context::default().with_struct_style(StructStyle::ForceAsArray);
        let mut bytes = vec![];
        test.encode(&mut bytes, &ctx_as_array).unwrap();
        assert_value(
            &bytes,
            Value::Array(vec![
                Value::from(1),
                Value::from("abc"),
                Value::Array(vec![Value::from(2), Value::from("def")]),
            ]),
        );

        // Because we forced as array when encoding, we need to force as array when decoding
        let e = Outer::decode(&mut bytes.as_slice(), &Context::default()).unwrap_err();
        // TODO: better error messages <https://git.picodata.io/picodata/picodata/tarantool-module/-/issues/176>
        assert_eq!(e.to_string(), "failed decoding tarantool::msgpack::encode::tests::encode_nested_struct::Outer: the type decoded isn't match with the expected one");

        let test_dec = Outer::decode(&mut bytes.as_slice(), &ctx_as_array).unwrap();
        assert_eq!(test_dec, test);

        // Override, encode as map
        let ctx_as_map = Context::default().with_struct_style(StructStyle::ForceAsMap);
        let mut bytes = vec![];
        test.encode(&mut bytes, &ctx_as_map).unwrap();
        assert_value(
            &bytes,
            Value::Map(vec![
                (Value::from("i"), Value::from(1)),
                (Value::from("s"), Value::from("abc")),
                (
                    Value::from("inner"),
                    Value::Map(vec![
                        (Value::from("i"), Value::from(2)),
                        (Value::from("s"), Value::from("def")),
                    ]),
                ),
            ]),
        );

        // Because we forced as map when encoding, we need to force as map when decoding
        let e = Outer::decode(&mut bytes.as_slice(), &Context::default()).unwrap_err();
        // TODO: better error messages <https://git.picodata.io/picodata/picodata/tarantool-module/-/issues/176>
        assert_eq!(e.to_string(), "failed decoding tarantool::msgpack::encode::tests::encode_nested_struct::Outer(field inner): failed decoding tarantool::msgpack::encode::tests::encode_nested_struct::Inner: the type decoded isn't match with the expected one");

        let test_dec = Outer::decode(&mut bytes.as_slice(), &ctx_as_map).unwrap();
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
            BarTupleN(i32, f64, String),
            BarStruct1 {
                bar: bool,
            },
            BarStructN {
                bar1: usize,
                bar2: [u8; 3],
                bar3: Box<Foo>,
            },
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

        let original = Foo::BarTupleN(13, 0.37, "hello".into());
        let bytes = encode(&original).unwrap();
        assert_value(
            &bytes,
            rmpv::Value::Map(vec![(
                rmpv::Value::String("BarTupleN".into()),
                rmpv::Value::Array(vec![
                    rmpv::Value::from(13),
                    rmpv::Value::from(0.37),
                    rmpv::Value::from("hello"),
                ]),
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
            bar1: 420,
            bar2: [b'a', b'b', b'c'],
            bar3: Box::new(Foo::BarUnit),
        };
        let bytes = encode(&original).unwrap();
        assert_value(
            &bytes,
            rmpv::Value::Map(vec![(
                rmpv::Value::String("BarStructN".into()),
                rmpv::Value::Array(vec![
                    rmpv::Value::from(420),
                    rmpv::Value::Array(vec![
                        rmpv::Value::from(b'a'),
                        rmpv::Value::from(b'b'),
                        rmpv::Value::from(b'c'),
                    ]),
                    rmpv::Value::Map(vec![(
                        rmpv::Value::String("BarUnit".into()),
                        rmpv::Value::Nil,
                    )]),
                ]),
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

        let original = vec![1, 2, 3, 4, 5];
        let bytes = encode(&original).unwrap();
        let decoded: Vec<i32> = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);

        let original = Vec::<i32>::new();
        let bytes = encode(&original).unwrap();
        let decoded: Vec<i32> = decode(bytes.as_slice()).unwrap();
        assert_eq!(&original, &decoded);
    }

    #[test]
    fn encode_array() {
        let original = [1u32];
        let bytes = encode(&original).unwrap();
        let decoded: [u32; 1] = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);

        let original = [1, 2, 3, 4, 5];
        let bytes = encode(&original).unwrap();
        let decoded: [u32; 5] = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);

        let original = [0_u32; 0];
        let bytes = encode(&original).unwrap();
        let decoded: [u32; 0] = decode(bytes.as_slice()).unwrap();
        assert_eq!(&original, &decoded);

        static mut DROP_COUNT: usize = 0;

        #[derive(Decode, Debug)]
        #[encode(tarantool = "crate")]
        struct DropChecker;

        impl Drop for DropChecker {
            fn drop(&mut self) {
                unsafe { DROP_COUNT += 1 }
            }
        }

        // Decoding a msgpack array [nil, nil, nil] fails early because count doesn't match,
        // and we don't initialize any elements, hence DROP_COUNT == 0.
        let err = decode::<[DropChecker; 4]>(b"\x93\xc0\xc0\xc0").unwrap_err();
        assert_eq!(unsafe { DROP_COUNT }, 0);

        assert_eq!(err.to_string(), "failed decoding [tarantool::msgpack::encode::tests::encode_array::DropChecker; 4]: expected array count 4, got 3");

        // Decoding a msgpack array [nil, nil, 1, nil] fails, after initializing 2 values,
        // so we automatically drop the 2 values, hence the DROP_COUNT == 2.
        let err = decode::<[DropChecker; 4]>(b"\x94\xc0\xc0\x01\xc0").unwrap_err();
        assert_eq!(unsafe { DROP_COUNT }, 2);

        assert_eq!(err.to_string(), "failed decoding [tarantool::msgpack::encode::tests::encode_array::DropChecker; 4](element 2): failed decoding (): the type decoded isn't match with the expected one");
    }

    #[test]
    fn encode_set() {
        let mut original = BTreeSet::new();
        original.insert(30);
        original.insert(10);
        original.insert(20);

        let bytes = encode(&original).unwrap();
        // Set is encoded as array
        assert_value(
            &bytes,
            Value::Array(vec![Value::from(10), Value::from(20), Value::from(30)]),
        );
        assert_eq!(original, decode::<BTreeSet<i32>>(&bytes).unwrap());

        let mut original = HashSet::new();
        original.insert(30);
        original.insert(10);
        original.insert(20);

        let bytes = encode(&original).unwrap();
        // Set is encoded as array
        let len = rmp::decode::read_array_len(&mut &bytes[..]).unwrap();
        assert_eq!(len, 3);
        assert_eq!(original, decode::<HashSet<i32>>(&bytes).unwrap());
    }

    #[test]
    fn encode_map() {
        let mut original = BTreeMap::new();
        original.insert(1, "abc".to_string());
        original.insert(2, "def".to_string());
        let bytes = encode(&original).unwrap();
        let decoded: BTreeMap<u32, String> = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);

        let mut original = HashMap::new();
        original.insert(1, "abc".to_string());
        original.insert(2, "def".to_string());
        let bytes = encode(&original).unwrap();
        let decoded: HashMap<u32, String> = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn encode_str() {
        let original = "hello";

        let bytes = encode(&original).unwrap();
        let decoded: String = decode(&bytes).unwrap();
        assert_eq!(original, decoded);

        let bytes = encode(&Cow::Borrowed(original)).unwrap();
        assert_eq!(original, decode::<String>(&bytes).unwrap());

        let bytes = encode(&String::from(original)).unwrap();
        assert_eq!(original, decode::<String>(&bytes).unwrap());

        let bytes = encode(&Cow::<str>::Owned(original.to_owned())).unwrap();
        assert_eq!(original, decode::<String>(&bytes).unwrap());
    }

    #[test]
    fn encode_char() {
        let bytes = encode(&'a').unwrap();
        assert_eq!(bytes, b"\xa1a");
        assert_eq!('a', decode::<char>(&bytes).unwrap());
        assert_eq!("a", decode::<String>(&bytes).unwrap());

        let bytes = encode(&'я').unwrap();
        assert_eq!(bytes, b"\xa2\xd1\x8f");
        assert_eq!('я', decode::<char>(&bytes).unwrap());
        assert_eq!("я", decode::<String>(&bytes).unwrap());

        let bytes = encode(&'☺').unwrap();
        assert_eq!(bytes, b"\xa3\xe2\x98\xba");
        assert_eq!('☺', decode::<char>(&bytes).unwrap());
        assert_eq!("☺", decode::<String>(&bytes).unwrap());

        let e = decode::<char>(b"").unwrap_err();
        assert_eq!(
            e.to_string(),
            "failed decoding char: failed to read MessagePack marker"
        );

        let e = decode::<char>(b"\xa0").unwrap_err();
        assert_eq!(
            e.to_string(),
            "failed decoding char: expected a msgpack non-empty string, got string length 0"
        );

        let e = decode::<char>(b"\xa1\xff").unwrap_err();
        assert_eq!(
            e.to_string(),
            "failed decoding char: invalid utf-8 sequence of 1 bytes from index 0"
        );

        let e = decode::<char>(b"\xa2hi").unwrap_err();
        assert_eq!(
            e.to_string(),
            "failed decoding char: expected a single unicode character, got sequence of length 2"
        );

        let e = decode::<char>(b"\xa5aaaaa").unwrap_err();
        assert_eq!(
            e.to_string(),
            "failed decoding char: expected a msgpack string not longer than 4 characters, got length 5"
        );
    }

    #[test]
    #[rustfmt::skip]
    fn encode_integer() {
        assert_eq!(&encode(&i8::MAX).unwrap(), &b"\x7f"[..]);
        assert_eq!(&encode(&(i8::MAX as i64)).unwrap(), &b"\x7f"[..]);
        assert_eq!(&encode(&i16::MAX).unwrap(), &b"\xcd\x7f\xff"[..]);
        assert_eq!(&encode(&i32::MAX).unwrap(), &b"\xce\x7f\xff\xff\xff"[..]);
        assert_eq!(&encode(&i64::MAX).unwrap(), &b"\xcf\x7f\xff\xff\xff\xff\xff\xff\xff"[..]);

        assert_eq!(&encode(&u8::MAX).unwrap(), &b"\xcc\xff"[..]);
        assert_eq!(&encode(&(u8::MAX as i64)).unwrap(), &b"\xcc\xff"[..]);
        assert_eq!(&encode(&u16::MAX).unwrap(), &b"\xcd\xff\xff"[..]);
        assert_eq!(&encode(&u32::MAX).unwrap(), &b"\xce\xff\xff\xff\xff"[..]);
        assert_eq!(&encode(&u64::MAX).unwrap(), &b"\xcf\xff\xff\xff\xff\xff\xff\xff\xff"[..]);

        assert_eq!(decode::<i8>(b"\x7f").unwrap(), i8::MAX);
        assert_eq!(decode::<i16>(b"\xcd\x7f\xff").unwrap(), i16::MAX);
        assert_eq!(decode::<i32>(b"\xce\x7f\xff\xff\xff").unwrap(), i32::MAX);
        assert_eq!(decode::<i64>(b"\xcf\x7f\xff\xff\xff\xff\xff\xff\xff").unwrap(), i64::MAX);

        assert_eq!(decode::<u8>(b"\xcc\xff").unwrap(), u8::MAX);
        assert_eq!(decode::<u16>(b"\xcd\xff\xff").unwrap(), u16::MAX);
        assert_eq!(decode::<u32>(b"\xce\xff\xff\xff\xff").unwrap(), u32::MAX);
        assert_eq!(decode::<u64>(b"\xcf\xff\xff\xff\xff\xff\xff\xff\xff").unwrap(), u64::MAX);
    }
}
