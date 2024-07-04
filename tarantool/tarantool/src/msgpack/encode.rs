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

use rmp::decode::{NumValueReadError, ValueReadError};

/// Encodes `value` as a vector of bytes in msgpack.
///
/// See [`Encode`].
#[inline(always)]
pub fn encode(value: &impl Encode) -> Vec<u8> {
    // 128 is chosen pretty randomly, we might want to benchmark this to find
    // better values
    let mut v = Vec::with_capacity(128);
    value
        .encode(&mut v, &Context::DEFAULT)
        .expect("encoding to vector should not fail");
    v
}

/// Decodes `T` from a slice of bytes in msgpack.
///
/// See [`Decode`].
#[inline(always)]
pub fn decode<'de, T: Decode<'de>>(mut bytes: &'de [u8]) -> Result<T, DecodeError> {
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
/// NOTE: switching context on tuple structs to `ForceAsMap`
/// will be silently ignored and forced as `MP_ARRAY`.
///
/// See [`Encode`], [`Decode`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StructStyle {
    /// Respects struct level attributes such as `as_map`.
    #[default]
    Default,
    /// Overrides struct level attributes such as `as_map`.
    /// Forces the struct and all nested structs to be serialized as `MP_MAP`.
    ///
    /// Switching context on tuple structs to `ForceAsMap`
    /// will be silently ignored and forced as `MP_ARRAY`.
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
/// It is also possible to put `#[encode(as_raw)]` attribute on fields of structs or variants of enums
/// to interpret field or variant value as raw MessagePack value. This will validate them at runtime
/// and directly write to or read from buffer.
///
/// Fields with type `Option<T>` can be skipped in msgpack if decoding MP_MAP.
/// In case of an MP_ARRAY (if `#[encode(allow_array_optionals)]` is enabled) only last fields
/// with type of `Option<T>` can be skipped.
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
pub trait Decode<'de>: Sized {
    fn decode(r: &mut &'de [u8], context: &Context) -> Result<Self, DecodeError>;
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
    pub part: Option<String>,
    // It is just a string for simplicicty as we need Clone, Sync, etc.
    /// The error that is wrapped by this error.
    source: String,
}

impl Display for DecodeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "failed decoding {}", self.ty)?;
        if let Some(ref part) = self.part {
            write!(f, " ({})", part)?;
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

    /// VRE is [`rmp::decode::ValueReadError`](https://docs.rs/rmp/latest/rmp/decode/enum.ValueReadError.html)
    #[inline(always)]
    pub fn from_vre<DecodedTy>(value: ValueReadError) -> Self {
        match value {
            ValueReadError::TypeMismatch(marker) => {
                let message = format!("got {marker:?}");
                Self::new::<DecodedTy>(value).with_part(message)
            }
            err @ ValueReadError::InvalidDataRead(_)
            | err @ ValueReadError::InvalidMarkerRead(_) => Self::new::<DecodedTy>(err),
        }
    }

    /// VRE is [`rmp::decode::ValueReadError`](https://docs.rs/rmp/latest/rmp/decode/enum.ValueReadError.html)
    #[inline(always)]
    pub fn from_vre_with_field<DecodedTy>(value: ValueReadError, field: impl ToString) -> Self {
        match value {
            ValueReadError::TypeMismatch(marker) => {
                let message = format!("got {marker:?} in field {}", field.to_string());
                Self::new::<DecodedTy>(value).with_part(message)
            }
            err @ ValueReadError::InvalidDataRead(_)
            | err @ ValueReadError::InvalidMarkerRead(_) => Self::new::<DecodedTy>(err),
        }
    }

    /// NVRE is [`rmp::decode::NumValueReadError`](https://docs.rs/rmp/latest/rmp/decode/enum.NumValueReadError.html)
    #[inline(always)]
    pub fn from_nvre<DecodedTy>(value: NumValueReadError) -> Self {
        match value {
            NumValueReadError::TypeMismatch(marker) => {
                let message = format!("got {marker:?}");
                Self::new::<DecodedTy>(value).with_part(message)
            }
            err @ NumValueReadError::InvalidDataRead(_)
            | err @ NumValueReadError::InvalidMarkerRead(_)
            | err @ NumValueReadError::OutOfRange => Self::new::<DecodedTy>(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// impl Decode
////////////////////////////////////////////////////////////////////////////////

impl<'de> Decode<'de> for () {
    #[inline(always)]
    fn decode(r: &mut &'de [u8], _context: &Context) -> Result<Self, DecodeError> {
        rmp::decode::read_nil(r).map_err(DecodeError::from_vre::<Self>)?;
        Ok(())
    }
}

impl<'de, T> Decode<'de> for Box<T>
where
    T: Decode<'de>,
{
    #[inline(always)]
    fn decode(r: &mut &'de [u8], context: &Context) -> Result<Self, DecodeError> {
        T::decode(r, context).map(Box::new)
    }
}

impl<'de, T> Decode<'de> for std::rc::Rc<T>
where
    T: Decode<'de>,
{
    #[inline(always)]
    fn decode(r: &mut &'de [u8], context: &Context) -> Result<Self, DecodeError> {
        T::decode(r, context).map(std::rc::Rc::new)
    }
}

impl<'de, T> Decode<'de> for Option<T>
where
    T: Decode<'de>,
{
    #[inline(always)]
    fn decode(r: &mut &'de [u8], context: &Context) -> Result<Self, DecodeError> {
        // In case input is empty, don't return `None` but call the T::decode.
        // This will allow some users to handle empty input the way they want,
        // if they want to.
        if !r.is_empty() && r[0] == super::MARKER_NULL {
            rmp::decode::read_nil(r).map_err(DecodeError::from_vre::<Self>)?;
            Ok(None)
        } else {
            T::decode(r, context).map(Some)
        }
    }
}

impl<'de, T> Decode<'de> for Vec<T>
where
    T: Decode<'de>,
{
    #[inline]
    fn decode(r: &mut &'de [u8], context: &Context) -> Result<Self, DecodeError> {
        let n = rmp::decode::read_array_len(r).map_err(DecodeError::from_vre::<Self>)? as usize;
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

impl<'de, T> Decode<'de> for HashSet<T>
where
    T: Decode<'de> + Hash + Eq,
{
    #[inline]
    fn decode(r: &mut &'de [u8], context: &Context) -> Result<Self, DecodeError> {
        let n = rmp::decode::read_array_len(r).map_err(DecodeError::from_vre::<Self>)? as usize;
        let mut res = HashSet::with_capacity(n);
        for i in 0..n {
            let v = T::decode(r, context)
                .map_err(|err| DecodeError::new::<Self>(err).with_part(format!("element {i}")))?;
            res.insert(v);
        }
        Ok(res)
    }
}

impl<'de, T> Decode<'de> for BTreeSet<T>
where
    T: Decode<'de> + Ord + Eq,
{
    #[inline]
    fn decode(r: &mut &'de [u8], context: &Context) -> Result<Self, DecodeError> {
        let n = rmp::decode::read_array_len(r).map_err(DecodeError::from_vre::<Self>)? as usize;
        let mut res = BTreeSet::new();
        for i in 0..n {
            let v = T::decode(r, context)
                .map_err(|err| DecodeError::new::<Self>(err).with_part(format!("element {i}")))?;
            res.insert(v);
        }
        Ok(res)
    }
}

impl<'de, T, const N: usize> Decode<'de> for [T; N]
where
    T: Decode<'de>,
{
    fn decode(r: &mut &'de [u8], context: &Context) -> Result<Self, DecodeError> {
        let n = rmp::decode::read_array_len(r).map_err(DecodeError::from_vre::<Self>)? as usize;
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

impl<'a, 'de, T> Decode<'de> for Cow<'a, T>
where
    T: Decode<'de> + ToOwned + ?Sized,
{
    // Clippy doesn't notice the type difference
    #[allow(clippy::redundant_clone)]
    #[inline(always)]
    fn decode(r: &mut &'de [u8], context: &Context) -> Result<Self, DecodeError> {
        Ok(Cow::Owned(
            <T as Decode>::decode(r, context)
                .map_err(DecodeError::new::<Self>)?
                .to_owned(),
        ))
    }
}

impl<'de> Decode<'de> for String {
    #[inline]
    fn decode(r: &mut &'de [u8], _context: &Context) -> Result<Self, DecodeError> {
        let n = rmp::decode::read_str_len(r).map_err(DecodeError::from_vre::<Self>)? as usize;
        let mut buf = vec![0; n];
        r.read_exact(&mut buf).map_err(DecodeError::new::<Self>)?;
        String::from_utf8(buf).map_err(DecodeError::new::<Self>)
    }
}

impl<'de> Decode<'de> for &'de str {
    #[inline]
    fn decode(r: &mut &'de [u8], _context: &Context) -> Result<Self, DecodeError> {
        let (res, bound) =
            rmp::decode::read_str_from_slice(*r).map_err(DecodeError::new::<Self>)?;
        *r = bound;
        Ok(res)
    }
}

impl<'de, K, V> Decode<'de> for BTreeMap<K, V>
where
    K: Decode<'de> + Ord,
    V: Decode<'de>,
{
    #[inline]
    fn decode(r: &mut &'de [u8], context: &Context) -> Result<Self, DecodeError> {
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

impl<'de, K, V> Decode<'de> for HashMap<K, V>
where
    K: Decode<'de> + Ord + Hash,
    V: Decode<'de>,
{
    #[inline]
    fn decode(r: &mut &'de [u8], context: &Context) -> Result<Self, DecodeError> {
        let n = rmp::decode::read_map_len(r).map_err(DecodeError::from_vre::<Self>)?;
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

impl<'de> Decode<'de> for char {
    #[inline(always)]
    fn decode(r: &mut &'de [u8], _context: &Context) -> Result<Self, DecodeError> {
        let n = rmp::decode::read_str_len(r).map_err(DecodeError::from_vre::<Self>)? as usize;
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

macro_rules! impl_simple_int_decode {
    ($(($t:ty, $f:tt))+) => {
        $(
            impl<'de> Decode<'de> for $t{
                #[inline(always)]
                fn decode(r: &mut &'de [u8], _context: &Context) -> Result<Self, DecodeError> {
                    let value = rmp::decode::$f(r)
                        .map_err(DecodeError::from_nvre::<Self>)?;
                    Ok(value)
                }
            }
        )+
    }
}

macro_rules! impl_simple_decode {
    ($(($t:ty, $f:tt))+) => {
        $(
            impl<'de> Decode<'de> for $t{
                #[inline(always)]
                fn decode(r: &mut &'de [u8], _context: &Context) -> Result<Self, DecodeError> {
                    let value = rmp::decode::$f(r)
                        .map_err(DecodeError::from_vre::<Self>)?;
                    Ok(value)
                }
            }
        )+
    }
}

impl_simple_int_decode! {
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
}

impl_simple_decode! {
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
/// It is also possible to put `#[encode(as_raw)]` attribute on fields of structs or variants of enums
/// to interpret field or variant value as raw MessagePack value. This will validate them at runtime
/// and directly write to or read from buffer.
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

    const MAP_CTX: &Context = &Context::DEFAULT.with_struct_style(StructStyle::ForceAsMap);
    const ARR_CTX: &Context = &Context::DEFAULT.with_struct_style(StructStyle::ForceAsArray);

    #[track_caller]
    fn assert_value(mut bytes: &[u8], v: rmpv::Value) {
        let got = rmpv::decode::read_value(&mut bytes).unwrap();
        assert_eq!(got, v);
    }

    /// Previously str enums did not advance the slice after decoding
    /// a value from it, which lead to errors when str enums were used
    /// as types of fields of structs.
    #[test]
    fn define_str_enum_regression() {
        crate::define_str_enum! {
            enum E {
                A = "a",
                B = "b",
            }
        }

        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        struct Test {
            a: String,
            e: E,
            b: bool,
        }

        // Try (de)encoding as part of a struct
        let test = Test {
            a: "abc".into(),
            e: E::A,
            b: true,
        };
        let bytes = encode(&test);
        let test_dec: Test = decode(bytes.as_slice()).unwrap();
        assert_eq!(test_dec, test);

        // Try (de)encoding as part of a struct as map
        let mut bytes = vec![];
        test.encode(&mut bytes, MAP_CTX).unwrap();
        let test_dec = Test::decode(&mut bytes.as_slice(), MAP_CTX).unwrap();
        assert_eq!(test_dec, test);

        // Try (de)encoding as part of vec
        let test = vec![E::A, E::B, E::A];
        let bytes = encode(&test);
        let test_dec: Vec<E> = decode(bytes.as_slice()).unwrap();
        assert_eq!(test_dec, test);

        // Try (de)encoding as part of map
        let test: HashMap<E, E> = vec![(E::A, E::B), (E::B, E::A)].into_iter().collect();
        let bytes = encode(&test);
        let test_dec: HashMap<E, E> = decode(bytes.as_slice()).unwrap();
        assert_eq!(test_dec, test);
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
        let bytes = encode(&test_1);
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
            "failed decoding tarantool::msgpack::encode::tests::encode_struct::Test2 (field not_b): \
            failed decoding f32 (got FixPos(42)): the type decoded isn't match with the expected one"
        );

        // Override, encode as map
        let mut bytes = vec![];
        test_1.encode(&mut bytes, MAP_CTX).unwrap();
        assert_value(
            &bytes,
            Value::Map(vec![(Value::from("b"), Value::from(42))]),
        );
        let test_1_dec = Test1::decode(&mut bytes.as_slice(), MAP_CTX).unwrap();
        assert_eq!(test_1_dec, test_1);

        // Try decoding as a different struct
        let e = Test2::decode(&mut bytes.as_slice(), MAP_CTX).unwrap_err();
        assert_eq!(
            e.to_string(),
            "failed decoding tarantool::msgpack::encode::tests::encode_struct::Test2: expected field not_b, got b"
        );
    }

    #[test]
    fn decode_optionals() {
        use std::f32::consts::TAU;

        #[derive(Debug, Decode, PartialEq)]
        #[encode(tarantool = "crate")]
        struct TestNamedForbidden {
            a: Option<Vec<String>>,
            b: Vec<i32>,
            c: Option<f32>,
            d: Option<f32>,
        }

        // map context, optional field is null (ok)
        let test_named_forbidden_helper_map = Value::Map(vec![
            (Value::from("a"), Value::Nil),
            (
                Value::from("b"),
                Value::Array(vec![Value::from(42), Value::from(52)]),
            ),
            (Value::from("c"), Value::Nil),
            (Value::from("d"), Value::from(TAU)),
        ]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_named_forbidden_helper_map).unwrap();
        let decoded_map = TestNamedForbidden::decode(&mut encoded.as_slice(), MAP_CTX).unwrap();
        assert_eq!(
            decoded_map,
            TestNamedForbidden {
                a: None,
                b: vec![42, 52],
                c: None,
                d: Some(TAU),
            }
        );

        // map context, optional field is missing (ok)
        let test_named_forbidden_helper_map = Value::Map(vec![
            (
                Value::from("b"),
                Value::Array(vec![Value::from(42), Value::from(52)]),
            ),
            (Value::from("c"), Value::Nil),
            (Value::from("d"), Value::from(TAU)),
        ]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_named_forbidden_helper_map).unwrap();
        let decoded_map = TestNamedForbidden::decode(&mut encoded.as_slice(), MAP_CTX).unwrap();
        assert_eq!(
            decoded_map,
            TestNamedForbidden {
                a: None,
                b: vec![42, 52],
                c: None,
                d: Some(TAU),
            }
        );

        // array context, optional field is null (ok)
        let test_named_forbidden_helper_arr = Value::Array(vec![
            Value::Nil,
            Value::Array(vec![Value::from(42), Value::from(52)]),
            Value::Nil,
            Value::from(TAU),
        ]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_named_forbidden_helper_arr).unwrap();
        let decoded_arr = TestNamedForbidden::decode(&mut encoded.as_slice(), ARR_CTX).unwrap();
        assert_eq!(
            decoded_arr,
            TestNamedForbidden {
                a: None,
                b: vec![42, 52],
                c: None,
                d: Some(TAU),
            }
        );

        // array context, optional field is missing (error)
        let test_named_forbidden_helper_arr = Value::Array(vec![
            Value::Array(vec![Value::from(42), Value::from(52)]),
            Value::Nil,
            Value::from(TAU),
        ]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_named_forbidden_helper_arr).unwrap();
        let err = TestNamedForbidden::decode(&mut encoded.as_slice(), ARR_CTX).unwrap_err();
        assert_eq!(err.to_string(), "failed decoding tarantool::msgpack::encode::tests::decode_optionals::TestNamedForbidden: decoding optional fields in named structs with `AS_ARRAY` context is not allowed without `allow_array_optionals` attribute");

        #[derive(Debug, Decode, PartialEq)]
        #[encode(tarantool = "crate", allow_array_optionals)]
        struct TestNamedAllowed {
            a: Vec<i32>,
            b: Option<f32>,
            c: Option<bool>,
        }

        // map context, optional field is null (ok)
        let test_named_allowed_helper_map = Value::Map(vec![
            (
                Value::from("a"),
                Value::Array(vec![Value::from(42), Value::from(52)]),
            ),
            (Value::from("b"), Value::Nil),
            (Value::from("c"), Value::from(false)),
        ]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_named_allowed_helper_map).unwrap();
        let decoded_map = TestNamedAllowed::decode(&mut encoded.as_slice(), MAP_CTX).unwrap();
        assert_eq!(
            decoded_map,
            TestNamedAllowed {
                a: vec![42, 52],
                b: None,
                c: Some(false),
            }
        );

        // map context, optional field is missing (ok)
        let test_named_allowed_helper_map = Value::Map(vec![
            (
                Value::from("a"),
                Value::Array(vec![Value::from(42), Value::from(52)]),
            ),
            (Value::from("c"), Value::from(false)),
        ]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_named_allowed_helper_map).unwrap();
        let decoded_map = TestNamedAllowed::decode(&mut encoded.as_slice(), MAP_CTX).unwrap();
        assert_eq!(
            decoded_map,
            TestNamedAllowed {
                a: vec![42, 52],
                b: None,
                c: Some(false),
            }
        );

        // array context, optional field is null (ok)
        let test_named_allowed_helper_arr = Value::Array(vec![
            Value::Array(vec![Value::from(42), Value::from(52)]),
            Value::Nil,
            Value::from(false),
        ]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_named_allowed_helper_arr).unwrap();
        let decoded_arr = TestNamedAllowed::decode(&mut encoded.as_slice(), ARR_CTX).unwrap();
        assert_eq!(
            decoded_arr,
            TestNamedAllowed {
                a: vec![42, 52],
                b: None,
                c: Some(false),
            }
        );

        // array context, optional field is missing (err)
        let test_named_allowed_helper_arr = Value::Array(vec![
            Value::Array(vec![Value::from(42), Value::from(52)]),
            Value::from(false),
        ]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_named_allowed_helper_arr).unwrap();
        let err = TestNamedAllowed::decode(&mut encoded.as_slice(), ARR_CTX).unwrap_err();
        assert_eq!(
            err.to_string(),
            "failed decoding f32 (got False): the type decoded isn't match with the expected one"
        );

        // array context, last optional field is missing (ok)
        let test_named_allowed_helper_arr = Value::Array(vec![
            Value::Array(vec![Value::from(42), Value::from(52)]),
            Value::from(TAU),
        ]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_named_allowed_helper_arr).unwrap();
        let decoded_arr = TestNamedAllowed::decode(&mut encoded.as_slice(), ARR_CTX).unwrap();
        assert_eq!(
            decoded_arr,
            TestNamedAllowed {
                a: vec![42, 52],
                b: Some(TAU),
                c: None
            }
        );

        // array context, more than one optional fields in a row are missing (ok)
        let test_named_allowed_helper_arr =
            Value::Array(vec![Value::Array(vec![Value::from(42), Value::from(52)])]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_named_allowed_helper_arr).unwrap();
        let decoded_arr = TestNamedAllowed::decode(&mut encoded.as_slice(), ARR_CTX).unwrap();
        assert_eq!(
            decoded_arr,
            TestNamedAllowed {
                a: vec![42, 52],
                b: None,
                c: None
            }
        );

        #[derive(Debug, Decode, PartialEq)]
        #[encode(tarantool = "crate")]
        struct TestUnnamedForbidden(Option<f32>, i32, Option<i32>, Option<Vec<String>>);

        // array context, optional field is null (ok), equal to the same map context
        let test_unnamed_forbidden_helper_arr = Value::Array(vec![
            Value::Nil,
            Value::from(42),
            Value::Nil,
            Value::Array(vec![Value::from("hello"), Value::from("world")]),
        ]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_unnamed_forbidden_helper_arr).unwrap();
        let decoded_arr = TestUnnamedForbidden::decode(&mut encoded.as_slice(), ARR_CTX).unwrap();
        assert_eq!(
            decoded_arr,
            TestUnnamedForbidden(None, 42, None, Some(vec!["hello".into(), "world".into()]))
        );
        let decoded_map = TestUnnamedForbidden::decode(&mut encoded.as_slice(), MAP_CTX).unwrap();
        assert_eq!(decoded_arr, decoded_map);

        // array context, optional field is missing (err), equal to the same map context
        let test_unnamed_forbidden_helper_arr = Value::Array(vec![
            Value::from(42),
            Value::Array(vec![Value::from("hello"), Value::from("world")]),
        ]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_unnamed_forbidden_helper_arr).unwrap();
        let err_arr = TestUnnamedForbidden::decode(&mut encoded.as_slice(), ARR_CTX).unwrap_err();
        assert_eq!(err_arr.to_string(), "failed decoding tarantool::msgpack::encode::tests::decode_optionals::TestUnnamedForbidden (0): failed decoding f32 (got FixPos(42)): the type decoded isn't match with the expected one");
        let err_map = TestUnnamedForbidden::decode(&mut encoded.as_slice(), MAP_CTX).unwrap_err();
        assert_eq!(err_map.to_string(), "failed decoding tarantool::msgpack::encode::tests::decode_optionals::TestUnnamedForbidden (0): failed decoding f32 (got FixPos(42)): the type decoded isn't match with the expected one");

        #[derive(Debug, Decode, PartialEq)]
        #[encode(tarantool = "crate", allow_array_optionals)]
        struct TestUnnamedAllowed(i32, Option<i32>, Option<Vec<String>>);

        // array context, optional field is null (ok), equal to the same map context
        let test_unnamed_allowed_helper_arr = Value::Array(vec![
            Value::from(42),
            Value::Nil,
            Value::Array(vec![Value::from("hello"), Value::from("world")]),
        ]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_unnamed_allowed_helper_arr).unwrap();
        let decoded_arr = TestUnnamedAllowed::decode(&mut encoded.as_slice(), ARR_CTX).unwrap();
        assert_eq!(
            decoded_arr,
            TestUnnamedAllowed(42, None, Some(vec!["hello".into(), "world".into()]))
        );
        let decoded_map = TestUnnamedAllowed::decode(&mut encoded.as_slice(), MAP_CTX).unwrap();
        assert_eq!(decoded_arr, decoded_map);

        // array context, optional field is missing (err), equal to the same map context
        let test_unnamed_allowed_helper_arr = Value::Array(vec![
            Value::from(42),
            Value::Array(vec![Value::from("hello"), Value::from("world")]),
        ]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_unnamed_allowed_helper_arr).unwrap();
        let err_arr = TestUnnamedAllowed::decode(&mut encoded.as_slice(), ARR_CTX).unwrap_err();
        assert_eq!(err_arr.to_string(), "failed decoding tarantool::msgpack::encode::tests::decode_optionals::TestUnnamedAllowed (1): failed decoding i32 (got FixArray(2)): the type decoded isn't match with the expected one");
        let err_map = TestUnnamedAllowed::decode(&mut encoded.as_slice(), MAP_CTX).unwrap_err();
        assert_eq!(err_map.to_string(), "failed decoding tarantool::msgpack::encode::tests::decode_optionals::TestUnnamedAllowed (1): failed decoding i32 (got FixArray(2)): the type decoded isn't match with the expected one");

        // array context, last optional field is missing (ok)
        let test_unnamed_allowed_helper_arr = Value::Array(vec![Value::from(42), Value::from(52)]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_unnamed_allowed_helper_arr).unwrap();
        let decoded_arr = TestUnnamedAllowed::decode(&mut encoded.as_slice(), ARR_CTX).unwrap();
        assert_eq!(decoded_arr, TestUnnamedAllowed(42, Some(52), None));

        // array context, more than one optional fields in a row are missing (ok)
        let test_unnamed_allowed_helper_arr = Value::Array(vec![Value::from(42)]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &test_unnamed_allowed_helper_arr).unwrap();
        let decoded_arr = TestUnnamedAllowed::decode(&mut encoded.as_slice(), ARR_CTX).unwrap();
        assert_eq!(decoded_arr, TestUnnamedAllowed(42, None, None));
    }

    #[test]
    fn encode_raw() {
        use serde::Serialize;

        #[derive(Serialize, Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        struct TestHelper {
            b: i32,
            c: Vec<String>,
            d: u64,
        }
        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        struct Test1 {
            #[encode(as_raw)]
            b: Vec<u8>,
        }
        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        struct Test2 {
            b: i32,
            #[encode(as_raw)]
            c: Vec<u8>,
            d: i32,
        }
        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        struct Test3 {
            #[encode(as_raw)]
            not_b: Vec<u8>,
        }
        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        struct Test4 {
            b: Vec<String>,
            #[encode(as_raw)]
            c: Vec<u8>,
            d: i32,
        }
        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        struct Test5(#[encode(as_raw)] Vec<u8>);
        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        struct Test6(Vec<String>, #[encode(as_raw)] Vec<u8>, i32);

        let ctx = Context::default().with_struct_style(StructStyle::ForceAsMap);

        // Encode and decode with only field is raw
        let value = rmp_serde::encode::to_vec(&42).unwrap();
        let original = Test1 { b: value };
        let bytes = encode(&original);
        assert_value(&bytes, Value::Array(vec![Value::from(42)]));
        let decoded = decode::<Test1>(&bytes).unwrap();
        assert_eq!(decoded, original);
        // Override struct-level encoding with map context on struct with only field is raw
        let mut bytes = vec![];
        original.encode(&mut bytes, &ctx).unwrap();
        assert_value(
            &bytes,
            Value::Map(vec![(Value::from("b"), Value::from(42))]),
        );
        let decoded = Test1::decode(&mut bytes.as_slice(), &ctx).unwrap();
        assert_eq!(decoded, original);
        // Try to decode as a different struct with only field is also raw
        let err = decode::<Test3>(&bytes).unwrap_err();
        assert_eq!(
            err.to_string(),
            "failed decoding tarantool::msgpack::encode::tests::encode_raw::Test3 (got FixMap(1) in field not_b): the type decoded isn't match with the expected one"
        );
        // Try to decode as a different struct with only field is also raw with different context
        let err = Test3::decode(&mut bytes.as_slice(), &ctx).unwrap_err();
        assert_eq!(
            err.to_string(),
            "failed decoding tarantool::msgpack::encode::tests::encode_raw::Test3: expected field not_b, got b"
        );

        // Encode and decode with more complex members of struct
        let value = rmp_serde::to_vec(&42).unwrap();
        let original = Test2 {
            b: 52,
            c: value,
            d: 42,
        };
        let bytes = encode(&original);
        assert_value(
            &bytes,
            Value::Array(vec![Value::from(52), Value::from(42), Value::from(42)]),
        );
        let decoded = decode::<Test2>(&bytes).unwrap();
        assert_eq!(decoded, original);
        // Override struct-level encoding with map context
        let mut bytes = vec![];
        original.encode(&mut bytes, &ctx).unwrap();
        assert_value(
            &bytes,
            Value::Map(vec![
                (Value::from("b"), Value::from(52)),
                (Value::from("c"), Value::from(42)),
                (Value::from("d"), Value::from(42)),
            ]),
        );
        let decoded = Test2::decode(&mut bytes.as_slice(), &ctx).unwrap();
        assert_eq!(decoded, original);
        // Try to decode as a different struct
        let err = decode::<Test4>(bytes.as_slice()).unwrap_err();
        assert_eq!(
            err.to_string(),
            "failed decoding tarantool::msgpack::encode::tests::encode_raw::Test4 (got FixMap(3) in field b): the type decoded isn't match with the expected one"
        );

        // Encode and decode with complex multibyte members of struct
        let helper = TestHelper {
            b: 42,
            c: vec!["nothing".into(), "here".into()],
            d: 52,
        };
        let value = rmp_serde::to_vec(&helper).unwrap();
        let original = Test4 {
            b: vec!["hello".into(), "world".into()],
            c: value,
            d: 52,
        };
        let bytes = encode(&original);
        assert_value(
            &bytes,
            Value::Array(vec![
                Value::Array(vec![Value::from("hello"), Value::from("world")]),
                Value::Array(vec![
                    Value::from(42),
                    Value::Array(vec![Value::from("nothing"), Value::from("here")]),
                    Value::from(52),
                ]),
                Value::from(52),
            ]),
        );
        let decoded = decode::<Test4>(&bytes).unwrap();
        assert_eq!(decoded, original);
        // Override struct-level encoding with map context
        let mut bytes = vec![];
        original.encode(&mut bytes, &ctx).unwrap();
        assert_value(
            &bytes,
            Value::Map(vec![
                (
                    Value::from("b"),
                    Value::Array(vec![Value::from("hello"), Value::from("world")]),
                ),
                (
                    Value::from("c"),
                    Value::Array(vec![
                        Value::from(42),
                        Value::Array(vec![Value::from("nothing"), Value::from("here")]),
                        Value::from(52),
                    ]),
                ),
                (Value::from("d"), Value::from(52)),
            ]),
        );
        // Try to decode as a different struct
        let err = decode::<Test2>(&bytes).unwrap_err();
        assert_eq!(
            err.to_string(),
            "failed decoding tarantool::msgpack::encode::tests::encode_raw::Test2 (got FixMap(3) in field b): the type decoded isn't match with the expected one"
        );

        // Encode unnamed struct fields with raw attribute
        let value = rmp_serde::encode::to_vec(&42).unwrap();
        let original = Test5(value);
        let bytes = encode(&original);
        assert_value(&bytes, Value::Array(vec![Value::from(42)]));
        let decoded = decode::<Test5>(&bytes).unwrap();
        assert_eq!(decoded, original);
        // Decode as wrong
        let err = decode::<Test6>(&bytes).unwrap_err();
        assert_eq!(
            err.to_string(),
            "failed decoding tarantool::msgpack::encode::tests::encode_raw::Test6 (field 0): \
            failed decoding alloc::vec::Vec<alloc::string::String> (got FixPos(42)): the type decoded isn't match with the expected one"
        );

        // Check for parsing scope of valid msgpack in multibyte scenario
        let value = rmp_serde::encode::to_vec(&helper).unwrap();
        let original = Test6(vec!["hello".into(), "world".into()], value, 42);
        let bytes = encode(&original);
        assert_value(
            &bytes,
            Value::Array(vec![
                Value::Array(vec![Value::from("hello"), Value::from("world")]),
                Value::Array(vec![
                    Value::from(42),
                    Value::Array(vec![Value::from("nothing"), Value::from("here")]),
                    Value::from(52),
                ]),
                Value::from(42),
            ]),
        );
        let decoded = decode::<Test6>(&bytes).unwrap();
        assert_eq!(decoded, original);

        #[derive(Clone, Serialize, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        enum Test7 {
            #[encode(as_raw)]
            Something(Vec<u8>),
        }
        #[derive(Clone, Serialize, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        enum Test8 {
            Something {
                foo: i32,
                #[encode(as_raw)]
                bar: Vec<u8>,
            },
        }

        let mut value = Vec::new();
        rmp_serde::encode::write(&mut value, &42).unwrap();
        let original = Test7::Something(value.clone());
        let bytes = encode(&original);
        assert_value(
            &bytes,
            Value::Map(vec![(
                Value::from("Something"),
                // first array is `Vec<u8>` from field type
                // second array is `encode::write` type repr
                Value::Array(vec![Value::Array(vec![Value::from(42)])]),
            )]),
        );
        let decoded = decode::<Test7>(&bytes).unwrap();
        assert_eq!(decoded, original);

        let original = Test8::Something {
            foo: 52,
            bar: value,
        };
        let bytes = encode(&original);
        assert_value(
            &bytes,
            Value::Map(vec![(
                Value::from("Something"),
                Value::Array(vec![Value::from(52), Value::from(42)]),
            )]),
        );
        let decoded = decode::<Test8>(&bytes).unwrap();
        assert_eq!(decoded, original);
        let err = decode::<Test7>(&bytes).unwrap_err();
        assert_eq!(
            err.to_string(),
            "failed decoding tarantool::msgpack::encode::tests::encode_raw::Test7 (field 0): failed decoding alloc::vec::Vec<u8> (got FixPos(52)): the type decoded isn't match with the expected one"
        );
        let decoded = Test8::decode(&mut bytes.as_slice(), &ctx).unwrap();
        assert_eq!(
            decoded,
            Test8::Something {
                foo: 52,
                bar: vec![42]
            }
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

        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        // Wants to be encoded as map
        #[encode(as_map)]
        struct OuterDeep {
            i: usize,
            s: String,
            inner: Inner1Deep,
        }

        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        struct Inner1Deep {
            i: usize,
            s: String,
            inner: Inner2Deep,
        }

        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        // Wants to be encoded as array
        struct Inner2Deep {
            i: usize,
            s: String,
        }

        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        // Wants to be encoded as map
        #[encode(as_map)]
        struct OuterMismatch {
            i: usize,
            s: String,
            inner: Inner1Mismatch,
        }

        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        struct Inner1Mismatch {
            i: usize,
            s: String,
            inner: Inner2Mismatch,
        }

        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        // Wants to be encoded as array
        struct Inner2Mismatch {
            i: f32,
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
        let bytes = encode(&test);
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
        assert_eq!(e.to_string(), "failed decoding tarantool::msgpack::encode::tests::encode_nested_struct::Outer (got FixArray(3)): the type decoded isn't match with the expected one");

        let test_dec = Outer::decode(&mut bytes.as_slice(), &ctx_as_array).unwrap();
        assert_eq!(test_dec, test);

        // Override, encode as map
        let mut bytes = vec![];
        test.encode(&mut bytes, MAP_CTX).unwrap();
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
        assert_eq!(e.to_string(), "failed decoding tarantool::msgpack::encode::tests::encode_nested_struct::Outer (field inner): failed decoding tarantool::msgpack::encode::tests::encode_nested_struct::Inner (got FixMap(2) in field i): the type decoded isn't match with the expected one");
        let test_dec = Outer::decode(&mut bytes.as_slice(), MAP_CTX).unwrap();
        assert_eq!(test_dec, test);

        // Encode as map with deeply nested error of type mismatch
        let mut bytes = vec![];
        let test_deep = OuterDeep {
            i: 1,
            s: "abc".into(),
            inner: Inner1Deep {
                i: 2,
                s: "def".into(),
                inner: Inner2Deep {
                    i: 3,
                    s: "ghi".into(),
                },
            },
        };
        test_deep.encode(&mut bytes, MAP_CTX).unwrap();
        let e = OuterMismatch::decode(&mut bytes.as_slice(), MAP_CTX).unwrap_err();
        assert_eq!(e.to_string(), "failed decoding tarantool::msgpack::encode::tests::encode_nested_struct::OuterMismatch (field inner): failed decoding tarantool::msgpack::encode::tests::encode_nested_struct::Inner1Mismatch (field inner): failed decoding tarantool::msgpack::encode::tests::encode_nested_struct::Inner2Mismatch (field i): failed decoding f32 (got FixPos(3)): the type decoded isn't match with the expected one")
    }

    #[test]
    fn encode_tuple_struct() {
        #[derive(Clone, Encode, Decode, PartialEq, Debug)]
        #[encode(tarantool = "crate")]
        struct Test(u32, bool);
        let original = Test(0, true);
        let bytes = encode(&original);
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
        let bytes = encode(&original);
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
        let bytes = encode(&original);
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
        let bytes = encode(&original);
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
        let bytes = encode(&original);
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
        let bytes = encode(&original);
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
        let bytes = encode(&original);
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
        let bytes = encode(&original);
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
        let bytes = encode(&original);
        let decoded: Vec<u32> = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);

        let original = vec![1, 2, 3, 4, 5];
        let bytes = encode(&original);
        let decoded: Vec<i32> = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);

        let original = Vec::<i32>::new();
        let bytes = encode(&original);
        let decoded: Vec<i32> = decode(bytes.as_slice()).unwrap();
        assert_eq!(&original, &decoded);
    }

    #[test]
    fn encode_array() {
        let original = [1u32];
        let bytes = encode(&original);
        let decoded: [u32; 1] = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);

        let original = [1, 2, 3, 4, 5];
        let bytes = encode(&original);
        let decoded: [u32; 5] = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);

        let original = [0_u32; 0];
        let bytes = encode(&original);
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

        assert_eq!(err.to_string(), "failed decoding [tarantool::msgpack::encode::tests::encode_array::DropChecker; 4] (element 2): failed decoding () (got FixPos(1)): the type decoded isn't match with the expected one");
    }

    #[test]
    fn encode_set() {
        let mut original = BTreeSet::new();
        original.insert(30);
        original.insert(10);
        original.insert(20);

        let bytes = encode(&original);
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

        let bytes = encode(&original);
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
        let bytes = encode(&original);
        let decoded: BTreeMap<u32, String> = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);

        let mut original = HashMap::new();
        original.insert(1, "abc".to_string());
        original.insert(2, "def".to_string());
        let bytes = encode(&original);
        let decoded: HashMap<u32, String> = decode(bytes.as_slice()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn encode_str() {
        let original = "hello";

        let bytes = encode(&original);
        let decoded: String = decode(&bytes).unwrap();
        assert_eq!(original, decoded);
        let decoded: &str = decode(&bytes).unwrap();
        assert_eq!(original, decoded);

        let bytes = encode(&Cow::Borrowed(original));
        assert_eq!(original, decode::<String>(&bytes).unwrap());

        let bytes = encode(&String::from(original));
        assert_eq!(original, decode::<String>(&bytes).unwrap());

        let bytes = encode(&Cow::<str>::Owned(original.to_owned()));
        assert_eq!(original, decode::<String>(&bytes).unwrap());
    }

    #[test]
    fn decode_borrowed_str_slice() {
        // single lifetime parameter
        #[derive(Debug, Decode, PartialEq)]
        #[encode(tarantool = "crate")]
        struct TestSingle<'a> {
            a: &'a str,
            b: Option<&'a str>,
            c: Vec<&'a str>,
        }
        // multiple lifetime parameters with complexity of where clause
        #[derive(Debug, Decode, PartialEq)]
        #[encode(tarantool = "crate")]
        struct TestMultiple<'a, 'b>
        where
            'a: 'b,
        {
            a: &'a str,
            b: Option<&'b str>,
            c: Vec<&'a str>,
        }

        // arr context - single (ok), multiple (ok)
        let original = Value::Array(vec![
            Value::from("one"),
            Value::from("and"),
            Value::Array(vec![Value::from("only")]),
        ]);
        let mut bytes = Vec::new();
        rmpv::encode::write_value(&mut bytes, &original).unwrap();
        let decoded_single = TestSingle::decode(&mut bytes.as_slice(), ARR_CTX).unwrap();
        assert_eq!(
            decoded_single,
            TestSingle {
                a: "one",
                b: Some("and"),
                c: vec!["only"]
            }
        );
        let decoded_multiple = TestMultiple::decode(&mut bytes.as_slice(), ARR_CTX).unwrap();
        assert_eq!(
            decoded_multiple,
            TestMultiple {
                a: "one",
                b: Some("and"),
                c: vec!["only"]
            }
        );

        // map context - single (ok), multiple (ok)
        let original = Value::Map(vec![
            (Value::from("a"), Value::from("one")),
            (Value::from("b"), Value::from("and")),
            (Value::from("c"), Value::Array(vec![Value::from("only")])),
        ]);
        let mut bytes = Vec::new();
        rmpv::encode::write_value(&mut bytes, &original).unwrap();
        let decoded_single = TestSingle::decode(&mut bytes.as_slice(), MAP_CTX).unwrap();
        assert_eq!(
            decoded_single,
            TestSingle {
                a: "one",
                b: Some("and"),
                c: vec!["only"]
            }
        );
        let decoded_multiple = TestMultiple::decode(&mut bytes.as_slice(), MAP_CTX).unwrap();
        assert_eq!(
            decoded_multiple,
            TestMultiple {
                a: "one",
                b: Some("and"),
                c: vec!["only"]
            }
        );
    }

    #[test]
    fn encode_char() {
        let bytes = encode(&'a');
        assert_eq!(bytes, b"\xa1a");
        assert_eq!('a', decode::<char>(&bytes).unwrap());
        assert_eq!("a", decode::<String>(&bytes).unwrap());

        let bytes = encode(&'я');
        assert_eq!(bytes, b"\xa2\xd1\x8f");
        assert_eq!('я', decode::<char>(&bytes).unwrap());
        assert_eq!("я", decode::<String>(&bytes).unwrap());

        let bytes = encode(&'☺');
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
        assert_eq!(&encode(&i8::MAX), &b"\x7f"[..]);
        assert_eq!(&encode(&(i8::MAX as i64)), &b"\x7f"[..]);
        assert_eq!(&encode(&i16::MAX), &b"\xcd\x7f\xff"[..]);
        assert_eq!(&encode(&i32::MAX), &b"\xce\x7f\xff\xff\xff"[..]);
        assert_eq!(&encode(&i64::MAX), &b"\xcf\x7f\xff\xff\xff\xff\xff\xff\xff"[..]);

        assert_eq!(&encode(&u8::MAX), &b"\xcc\xff"[..]);
        assert_eq!(&encode(&(u8::MAX as i64)), &b"\xcc\xff"[..]);
        assert_eq!(&encode(&u16::MAX), &b"\xcd\xff\xff"[..]);
        assert_eq!(&encode(&u32::MAX), &b"\xce\xff\xff\xff\xff"[..]);
        assert_eq!(&encode(&u64::MAX), &b"\xcf\xff\xff\xff\xff\xff\xff\xff\xff"[..]);

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
