//! Tuples
//!
//! The `tuple` submodule provides read-only access for the tuple userdata type.
//! It allows, for a single tuple: selective retrieval of the field contents, retrieval of information about size,
//! iteration over all the fields, and conversion from/to rust structures
//!
//! See also:
//! - [Tuples](https://www.tarantool.io/en/doc/2.2/book/box/data_model/#tuples)
//! - [Lua reference: Submodule box.tuple](https://www.tarantool.io/en/doc/2.2/reference/reference_lua/box_tuple/)
//! - [C API reference: Module tuple](https://www.tarantool.io/en/doc/2.2/dev_guide/reference_capi/tuple/)
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt::{self, Debug, Formatter};
use std::io::Write;
use std::os::raw::{c_char, c_int};
use std::ops::Range;
use std::ptr::{copy_nonoverlapping, NonNull};

use num_derive::ToPrimitive;
use num_traits::ToPrimitive;
use rmp::Marker;
use serde::Serialize;

use crate::error::{self, Error, Result, TarantoolError};
use crate::ffi::tarantool as ffi;
use crate::tlua as tlua;

/// Tuple
pub struct Tuple {
    ptr: NonNull<ffi::BoxTuple>,
}

impl Debug for Tuple {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if let Ok(v) = self.decode::<rmpv::Value>() {
            f.debug_tuple("Tuple").field(&v).finish()
        } else {
            // Probably will never happen but better safe than sorry
            f.debug_tuple("Tuple").field(&self.as_buffer()).finish()
        }
    }
}

impl Tuple {
    /// Create a new tuple from `value` implementing [`ToTupleBuffer`].
    #[inline]
    pub fn new<T>(value: &T) -> Result<Self>
    where
        T: ToTupleBuffer,
    {
        Ok(Self::from(&value.to_tuple_buffer()?))
    }

    /// Creates new tuple from `value`.
    ///
    /// This function will serialize structure instance `value` of type `T` into tuple internal representation
    ///
    /// See also: [AsTuple](trait.AsTuple.html)
    #[deprecated = "Use `Tuple::new` instead."]
    #[inline]
    pub fn from_struct<T>(value: &T) -> Result<Self>
    where
        T: ToTupleBuffer,
    {
        Self::new(value)
    }

    /// # Safety
    /// `data` must point to a buffer containing `len` bytes representing a
    /// valid messagepack array
    pub unsafe fn from_raw_data(data: *mut c_char, len: u32) -> Self {
        let format = TupleFormat::default();
        let tuple_ptr = ffi::box_tuple_new(
            format.inner,
            data as _,
            data.add(len as _) as _
        );

        Self::from_ptr(NonNull::new_unchecked(tuple_ptr))
    }

    /// # Safety
    /// `data` must represent a valid messagepack array
    pub unsafe fn from_slice(data: &[u8]) -> Self {
        let format = TupleFormat::default();
        let Range { start, end } = data.as_ptr_range();
        let tuple_ptr = ffi::box_tuple_new(format.inner, start as _, end as _);

        Self::from_ptr(NonNull::new_unchecked(tuple_ptr))
    }

    pub fn try_from_slice(data: &[u8]) -> Result<Self> {
        let data = validate_msgpack(data)?;
        unsafe { Ok(Self::from_slice(data)) }
    }

    pub fn from_ptr(mut ptr: NonNull<ffi::BoxTuple>) -> Self {
        unsafe { ffi::box_tuple_ref(ptr.as_mut()) };
        Tuple { ptr }
    }

    pub fn try_from_ptr(ptr: *mut ffi::BoxTuple) -> Option<Self> {
        NonNull::new(ptr).map(Self::from_ptr)
    }

    /// Return the number of fields in tuple (the size of MsgPack Array).
    pub fn len(&self) -> u32 {
        unsafe { ffi::box_tuple_field_count(self.ptr.as_ptr()) }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Will return the number of bytes in the tuple.
    ///
    /// With both the memtx storage engine and the vinyl storage engine the default maximum is one megabyte
    /// (`memtx_max_tuple_size` or `vinyl_max_tuple_size`). Every field has one or more "length" bytes preceding the
    /// actual contents, so `bsize()` returns a value which is slightly greater than the sum of the lengths of the
    /// contents.
    ///
    /// The value does not include the size of "struct tuple"
    /// (for the current size of this structure look in the tuple.h file in Tarantool’s source code).
    pub fn bsize(&self) -> usize {
        unsafe { self.ptr.as_ref().bsize() }
    }

    /// Return the associated format.
    pub fn format(&self) -> TupleFormat {
        TupleFormat {
            inner: unsafe { ffi::box_tuple_format(self.ptr.as_ptr()) },
        }
    }

    /// Allocate and initialize a new `Tuple` iterator. The `Tuple` iterator
    /// allow to iterate over fields at root level of MsgPack array.
    ///
    /// Example:
    /// ```no_run
    /// # fn foo<T: serde::de::DeserializeOwned>(tuple: tarantool::tuple::Tuple) {
    /// let mut it = tuple.iter().unwrap();
    ///
    /// while let Some(field) = it.next::<T>().unwrap() {
    ///     // process data
    /// }
    ///
    /// // rewind iterator to first position
    /// it.rewind();
    /// assert!(it.position() == 0);
    ///
    /// // rewind iterator to first position
    /// let field = it.seek::<T>(3).unwrap();
    /// assert!(it.position() == 4);
    /// }
    /// ```
    pub fn iter(&self) -> Result<TupleIterator> {
        let inner = unsafe { ffi::box_tuple_iterator(self.ptr.as_ptr()) };
        if inner.is_null() {
            Err(TarantoolError::last().into())
        } else {
            Ok(TupleIterator { inner })
        }
    }

    /// Deserialize a tuple field specified by zero-based array index.
    ///
    /// - `fieldno` - zero-based index in MsgPack array.
    ///
    /// Returns:
    /// - `Ok(None)` if `fieldno >= self.len()`
    /// - `Err(e)` if deserialization failed
    /// - `Ok(Some(field value))` otherwise
    ///
    /// See also [`Tuple::try_get`], [`Tuple::get`].
    pub fn field<'a, T>(&'a self, fieldno: u32) -> Result<Option<T>>
    where
        T: Decode<'a>,
    {
        unsafe {
            let field_ptr = ffi::box_tuple_field(self.ptr.as_ptr(), fieldno);
            field_value_from_ptr(field_ptr as _)
        }
    }

    /// Deserialize a tuple field specified by an index implementing
    /// [`TupleIndex`] trait.
    ///
    /// Currently 2 types of indexes are supported:
    /// - `u32` - zero-based index in MsgPack array (See also [`Tuple::field`])
    /// - `&str` - JSON path for tuples with non default formats
    ///
    /// **NOTE**: getting tuple fields by JSON paths is not supported in all
    /// tarantool versions. Use [`tarantool::ffi::has_tuple_field_by_path`] to
    /// check whether it's supported in your case.
    /// If `has_tuple_field_by_path` returns `false` this function will always
    /// return `Err`.
    ///
    /// Returns:
    /// - `Ok(None)` if index wasn't found
    /// - `Err(e)` if deserialization failed (or api not supported)
    /// - `Ok(Some(field value))` otherwise
    ///
    /// See also [`Tuple::get`].
    ///
    /// [`tarantool::ffi::has_tuple_field_by_path`]:
    /// crate::ffi::has_tuple_field_by_path
    #[inline(always)]
    pub fn try_get<'a, I, T>(&'a self, key: I) -> Result<Option<T>>
    where
        I: TupleIndex,
        T: Decode<'a>,
    {
        key.get_field(self)
    }

    /// Deserialize a tuple field specified by an index implementing
    /// [`TupleIndex`] trait.
    ///
    /// Currently 2 types of indexes are supported:
    /// - `u32` - zero-based index in MsgPack array (See also [`Tuple::field`])
    /// - `&str` - JSON path for tuples with non default formats
    ///
    /// **NOTE**: getting tuple fields by JSON paths is not supported in all
    /// tarantool versions. Use [`tarantool::ffi::has_tuple_field_by_path`] to
    /// check whether it's supported in your case.
    /// If `has_tuple_field_by_path` returns `false` this function will always
    /// **panic**.
    ///
    /// Returns:
    /// - `None` if index wasn't found
    /// - **panics** if deserialization failed (or api not supported)
    /// - `Some(field value)` otherwise
    ///
    /// See also [`Tuple::get`].
    ///
    /// [`tarantool::ffi::has_tuple_field_by_path`]:
    /// crate::ffi::has_tuple_field_by_path
    #[inline(always)]
    pub fn get<'a, I, T>(&'a self, key: I) -> Option<T>
    where
        I: TupleIndex,
        T: Decode<'a>,
    {
        self.try_get(key).expect("Error during getting tuple field")
    }

    /// Decode tuple contents as `T`.
    ///
    /// **NOTE**: Because [`Tuple`] implements [`DecodeOwned`], you can do
    /// something like this
    /// ```no_run
    /// use tarantool::tuple::{Decode, Tuple};
    /// let tuple: Tuple;
    /// # tuple = Tuple::new(&[1, 2, 3]).unwrap();
    /// let deep_copy: Tuple = tuple.decode().unwrap();
    /// let inc_ref_count: Tuple = tuple.clone();
    /// ```
    /// "Decoding" a `Tuple` into a `Tuple` will basically perform a **deep
    /// copy** of its contents, while `tuple.clone()` will just increase tuple's
    /// reference count. There's probably no use case for deep copying the
    /// tuple, because there's actully no way to move data out of it, so keep
    /// this in mind.
    #[inline]
    pub fn decode<T>(&self) -> Result<T>
    where
        T: DecodeOwned,
    {
        let raw_data = self.as_buffer();
        Decode::decode(&raw_data)
    }

    /// Deserializes tuple contents into structure of type `T`
    #[deprecated = "Use `Tuple::decode` instead"]
    pub fn as_struct<T>(&self) -> Result<T>
    where
        T: DecodeOwned,
    {
        self.decode()
    }

    #[inline]
    pub(crate) fn as_buffer(&self) -> Vec<u8> {
        let size = self.bsize();
        let mut buf = Vec::with_capacity(size);

        unsafe {
            let actual_size = ffi::box_tuple_to_buf(
                self.ptr.as_ptr(), buf.as_ptr() as _, size,
            );
            buf.set_len(actual_size as usize);
        }

        buf
    }

    /// Deserializes tuple contents into structure of type `T`
    #[deprecated = "Use `Tuple::decode` instead"]
    pub fn into_struct<T>(self) -> Result<T>
    where
        T: DecodeOwned,
    {
        self.decode()
    }

    pub(crate) fn into_ptr(self) -> *mut ffi::BoxTuple {
        self.ptr.as_ptr()
    }
}

////////////////////////////////////////////////////////////////////////////////
/// TupleIndex
////////////////////////////////////////////////////////////////////////////////

/// Types implementing this trait can be used as arguments for the
/// [`Tuple::get`] method.
///
/// This is a helper trait, so you don't want to use it directly.
pub trait TupleIndex {
    fn get_field<'a, T>(self, tuple: &'a Tuple) -> Result<Option<T>>
    where
        T: Decode<'a>;
}

impl TupleIndex for u32 {
    #[inline(always)]
    fn get_field<'a, T>(self, tuple: &'a Tuple) -> Result<Option<T>>
    where
        T: Decode<'a>,
    {
        tuple.field(self)
    }
}

impl TupleIndex for &str {
    #[inline(always)]
    fn get_field<'a, T>(self, tuple: &'a Tuple) -> Result<Option<T>>
    where
        T: Decode<'a>,
    {
        use once_cell::sync::Lazy;
        use std::io::{Error as IOError, ErrorKind};
        static API_AWAILABLE: Lazy<std::result::Result<(), String>> = Lazy::new(|| unsafe {
            crate::ffi::helper::check_symbol(crate::c_str!("tuple_field_raw_by_full_path"))
                .map_err(|e| e.to_string())
        });

        API_AWAILABLE.clone()
            .map_err(|e| Error::IO(IOError::new(ErrorKind::Unsupported, e)))?;

        unsafe {
            let tuple_raw = tuple.ptr.as_ref();
            let field_ptr = ffi::tuple_field_raw_by_full_path(
                tuple.format().inner,
                tuple_raw.data(),
                tuple_raw.field_map(),
                self.as_ptr() as _,
                self.len() as _,
                tlua::util::hash(self),
            );
            field_value_from_ptr(field_ptr as _)
        }
    }
}

impl From<&TupleBuffer> for Tuple {
    fn from(buf: &TupleBuffer) -> Self {
        unsafe {
            Self::from_raw_data(buf.as_ptr() as _, buf.len() as _)
        }
    }
}

impl Drop for Tuple {
    fn drop(&mut self) {
        unsafe { ffi::box_tuple_unref(self.ptr.as_ptr()) };
    }
}

impl Clone for Tuple {
    fn clone(&self) -> Self {
        unsafe { ffi::box_tuple_ref(self.ptr.as_ptr()) };
        Tuple { ptr: self.ptr }
    }
}

////////////////////////////////////////////////////////////////////////////////
/// ToTupleBuffer
////////////////////////////////////////////////////////////////////////////////

/// Types implementing this trait can be converted to tarantool tuple (msgpack
/// array).
pub trait ToTupleBuffer {
    fn to_tuple_buffer(&self) -> Result<TupleBuffer> {
        let mut buf = Vec::with_capacity(128);
        self.write_tuple_data(&mut buf)?;
        TupleBuffer::try_from_vec(buf)
    }

    fn write_tuple_data(&self, w: &mut impl Write) -> Result<()>;
}

impl ToTupleBuffer for Tuple {
    #[inline]
    fn to_tuple_buffer(&self) -> Result<TupleBuffer> {
        Ok(TupleBuffer::Vector(self.as_buffer()))
    }

    #[inline]
    fn write_tuple_data(&self, w: &mut impl Write) -> Result<()> {
        w.write_all(&self.as_buffer()).map_err(Into::into)
    }
}

#[allow(deprecated)]
impl<T> ToTupleBuffer for T
where
    T: ?Sized,
    T: AsTuple,
{
    #[inline]
    fn write_tuple_data(&self, w: &mut impl Write) -> Result<()> {
        self.serialize_to(w)
    }
}

////////////////////////////////////////////////////////////////////////////////
/// AsTuple
////////////////////////////////////////////////////////////////////////////////

#[deprecated = "This is a legacy trait which will be removed in future. \
Implement `Encode` for custom types instead. \
Use `ToTupleBuffer` if you need the tuple data instead."]
/// Must be implemented for types, which will be used with box access methods as data
pub trait AsTuple: Serialize {
    /// Describes how object can be converted to [Tuple](struct.Tuple.html).
    ///
    /// Has default implementation, but can be overloaded for special cases
    #[inline]
    fn serialize_as_tuple(&self) -> Result<TupleBuffer> {
        TupleBuffer::try_from(AsTuple::serialize(self)?)
    }

    #[inline]
    fn serialize(&self) -> Result<Vec<u8>> {
        // TODO(gmoshkin): tuple is required to be a message pack array only on
        // the top layer, but `to_vec` serializes all of the nested structs as
        // arrays, which is very bad. We should implement a custom serializer,
        // which does the correct thing
        let mut vec = Vec::with_capacity(128);
        self.serialize_to(&mut vec)?;
        Ok(vec)
    }

    fn serialize_to(&self, w: &mut impl Write) -> Result<()> {
        rmp_serde::encode::write(w, self).map_err(Into::into)
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Encode
////////////////////////////////////////////////////////////////////////////////

/// Types implementing this trait can be serialized into a valid tarantool tuple
/// (msgpack array).
// TODO: remove this trait when `specialization` feature is stabilized
// https://github.com/rust-lang/rust/issues/31844
pub trait Encode: Serialize {
    fn encode(&self, w: &mut impl Write) -> Result<()> {
        rmp_serde::encode::write(w, self).map_err(Into::into)
    }
}

impl<'a, T> Encode for &'a T
where
    T: Encode,
{
    fn encode(&self, w: &mut impl Write) -> Result<()> {
        T::encode(*self, w)
    }
}

impl Encode for () {
    fn encode(&self, w: &mut impl Write) -> Result<()> {
        rmp_serde::encode::write(w, &Vec::<()>::new()).map_err(Into::into)
    }
}

impl<T> Encode for [T] where T: Serialize {}
impl<T> Encode for Vec<T> where T: Serialize {}

macro_rules! impl_array {
    ($($n:literal)+) => {
        $(
            #[allow(clippy::zero_prefixed_literal)]
            impl<T> Encode for [T; $n] where T: Serialize {}
        )+
    }
}

impl_array! {
    00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15
    16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32
}

macro_rules! impl_tuple {
    () => {};
    ($h:ident $($t:ident)*) => {
        impl<$h, $($t),*> Encode for ($h, $($t),*)
        where
            $h: Serialize,
            $($t: Serialize,)*
        {}

        impl_tuple! { $($t)* }
    }
}

impl_tuple! { A B C D E F G H I J K L M N O P }

#[allow(deprecated)]
impl<T> AsTuple for T
where
    T: ?Sized,
    T: Encode,
{
    fn serialize_to(&self, w: &mut impl Write) -> Result<()> {
        self.encode(w)
    }
}

////////////////////////////////////////////////////////////////////////////////
/// TupleBuffer
////////////////////////////////////////////////////////////////////////////////

/// Buffer containing tuple contents (MsgPack array)
///
/// If buffer is allocated within transaction: will be disposed after transaction ended (committed or dropped).
/// If not: will act as a regular rust `Vec<u8>`
pub enum TupleBuffer {
    // TODO(gmoshkin): use smallvec::SmallVec instead
    Vector(Vec<u8>),
    TransactionScoped { ptr: NonNull<u8>, size: usize },
}

impl TupleBuffer {
    /// Get raw pointer to buffer.
    pub fn as_ptr(&self) -> *const u8 {
        match self {
            TupleBuffer::Vector(vec) => vec.as_ptr(),
            TupleBuffer::TransactionScoped { ptr, .. } => ptr.as_ptr(),
        }
    }

    /// Return the number of bytes used in memory by the tuple.
    pub fn len(&self) -> usize {
        match self {
            TupleBuffer::Vector(vec) => vec.len(),
            TupleBuffer::TransactionScoped { size, .. } => *size,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            TupleBuffer::Vector(vec) => vec.is_empty(),
            TupleBuffer::TransactionScoped { size, .. } => *size == 0,
        }
    }

    /// # Safety
    /// `buf` must be a valid message pack array
    pub unsafe fn from_vec_unchecked(buf: Vec<u8>) -> Self {
        if ffi::box_txn() {
            let size = buf.len();
            let ptr = ffi::box_txn_alloc(size) as _;
            copy_nonoverlapping(buf.as_ptr(), ptr, size);
            let ptr = NonNull::new(ptr).expect("tarantool allocation failed");
            Self::TransactionScoped { ptr, size }
        } else {
            Self::Vector(buf)
        }
    }

    pub fn try_from_vec(data: Vec<u8>) -> Result<Self> {
        let data = validate_msgpack(data)?;
        unsafe { Ok(Self::from_vec_unchecked(data)) }
    }
}

impl AsRef<[u8]> for TupleBuffer {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Vector(v) => v.as_ref(),
            Self::TransactionScoped { ptr, size } => unsafe {
                std::slice::from_raw_parts(ptr.as_ptr(), *size)
            }
        }
    }
}

impl From<TupleBuffer> for Vec<u8> {
    fn from(b: TupleBuffer) -> Self {
        match b {
            TupleBuffer::Vector(v) => v,
            TupleBuffer::TransactionScoped { ptr, size } => unsafe {
                std::slice::from_raw_parts(ptr.as_ptr(), size).into()
            }
        }
    }
}

impl TryFrom<Vec<u8>> for TupleBuffer {
    type Error = Error;

    fn try_from(data: Vec<u8>) -> Result<Self> {
        Self::try_from_vec(data)
    }
}

impl From<Tuple> for TupleBuffer {
    fn from(t: Tuple) -> Self {
        Self::Vector(t.as_buffer())
    }
}

impl Debug for TupleBuffer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_tuple(
            match self {
                Self::Vector(_) => "TupleBuffer::Vector",
                Self::TransactionScoped { .. } => "TupleBuffer::TransactionScoped",
            }
        )
            .field(&Tuple::from(self))
            .finish()
    }
}

////////////////////////////////////////////////////////////////////////////////
/// TupleFormat
////////////////////////////////////////////////////////////////////////////////

/// Tuple format
///
/// Each Tuple has associated format (class). Default format is used to
/// create tuples which are not attach to any particular space.
pub struct TupleFormat {
    inner: *mut ffi::BoxTupleFormat,
}

impl Default for TupleFormat {
    fn default() -> Self {
        TupleFormat {
            inner: unsafe { ffi::box_tuple_format_default() },
        }
    }
}

impl Debug for TupleFormat {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if self.inner == Self::default().inner {
            f.write_str("TupleFormat::default()")
        } else {
            f.debug_tuple("TupleFormat").field(&self.inner).finish()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
/// TupleIterator
////////////////////////////////////////////////////////////////////////////////

/// Tuple iterator
pub struct TupleIterator {
    inner: *mut ffi::BoxTupleIterator,
}

impl Debug for TupleIterator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("TupleIterator")
            .field("position", &self.position())
            .finish()
    }
}

impl TupleIterator {
    /// Return zero-based next position in iterator.
    ///
    /// That is, this function return the field id of field that will be
    /// returned by the next call to `box_tuple_next(it)`. Returned value is zero
    /// after initialization or rewind and `box_tuple_field_count(Tuple)`
    /// after the end of iteration.
    pub fn position(&self) -> u32 {
        unsafe { ffi::box_tuple_position(self.inner) }
    }

    /// Rewind iterator to the initial position.
    pub fn rewind(&mut self) {
        unsafe { ffi::box_tuple_rewind(self.inner) }
    }

    /// Seek the Tuple iterator.
    ///
    /// Requested fieldno returned by next call to `box_tuple_next(it)`.
    ///
    /// - `fieldno` - zero-based position in MsgPack array.
    ///
    /// After call:
    /// - `box_tuple_position(it) == fieldno` if returned value is not `None`
    /// - `box_tuple_position(it) == box_tuple_field_count(Tuple)` if returned value is `None`.
    pub fn seek<'t, T>(&'t mut self, fieldno: u32) -> Result<Option<T>>
    where
        T: Decode<'t>,
    {
        unsafe {
            field_value_from_ptr(ffi::box_tuple_seek(self.inner, fieldno) as _)
        }
    }

    /// Return the next Tuple field from Tuple iterator.
    ///
    /// Returns:
    /// - `None` if `i >= box_tuple_field_count(Tuple)` or if field has a non primitive type
    /// - field value otherwise
    ///
    /// After call:
    /// - `box_tuple_position(it) == fieldno` if returned value is not `None`
    /// - `box_tuple_position(it) == box_tuple_field_count(Tuple)` if returned value is `None`.
    #[allow(clippy::should_implement_trait)]
    pub fn next<'t, T>(&'t mut self) -> Result<Option<T>>
    where
        T: Decode<'t>,
    {
        unsafe {
            field_value_from_ptr(ffi::box_tuple_next(self.inner) as _)
        }
    }

    pub fn update(&mut self) {}
}

impl Drop for TupleIterator {
    fn drop(&mut self) {
        unsafe { ffi::box_tuple_iterator_free(self.inner) }
    }
}

impl TupleIterator {}

////////////////////////////////////////////////////////////////////////////////
/// FieldType
////////////////////////////////////////////////////////////////////////////////

#[repr(u32)]
#[derive(Debug, ToPrimitive, PartialEq, Eq, Hash)]
pub enum FieldType {
    Any = 0,
    Unsigned,
    String,
    Number,
    Double,
    Integer,
    Boolean,
    Varbinary,
    Scalar,
    Decimal,
    Uuid,
    Array,
    Map,
}

////////////////////////////////////////////////////////////////////////////////
/// KeyDef
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct KeyDef {
    inner: *mut ffi::BoxKeyDef,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct KeyDefItem {
    pub field_id: u32,
    pub field_type: FieldType,
}

impl KeyDefItem {
    pub fn new(field_id: u32, field_type: FieldType) -> Self {
        Self {
            field_id,
            field_type,
        }
    }
}

impl From<(u32, FieldType)> for KeyDefItem {
    fn from((field_id, field_type): (u32, FieldType)) -> Self {
        Self {
            field_id,
            field_type,
        }
    }
}

impl KeyDef {
    /// Create key definition with key fields with passed typed on passed positions.
    /// May be used for tuple format creation and/or tuple comparison.
    ///
    /// - `items` - array with key field identifiers and key field types (see [FieldType](struct.FieldType.html))
    #[inline]
    pub fn new(items: impl IntoIterator<Item=impl Into<KeyDefItem>>) -> Self {
        let iter = items.into_iter();
        let (size, _) = iter.size_hint();
        let mut ids = Vec::with_capacity(size);
        let mut types = Vec::with_capacity(size);
        for KeyDefItem { field_id, field_type } in iter.map(Into::into) {
            ids.push(field_id);
            types.push(field_type.to_u32().unwrap());
        }

        KeyDef {
            inner: unsafe {
                ffi::box_key_def_new(ids.as_mut_ptr(), types.as_mut_ptr(), size as u32)
            },
        }
    }

    /// Compare tuples using the key definition.
    ///
    /// - `tuple_a` - first tuple
    /// - `tuple_b` - second tuple
    ///
    /// Returns:
    /// - `Ordering::Equal`   if `key_fields(tuple_a) == key_fields(tuple_b)`
    /// - `Ordering::Less`    if `key_fields(tuple_a) < key_fields(tuple_b)`
    /// - `Ordering::Greater` if `key_fields(tuple_a) > key_fields(tuple_b)`
    pub fn compare(&self, tuple_a: &Tuple, tuple_b: &Tuple) -> Ordering {
        unsafe {
            ffi::box_tuple_compare(tuple_a.ptr.as_ptr(), tuple_b.ptr.as_ptr(), self.inner)
                .cmp(&0)
        }
    }

    /// Compare tuple with key using the key definition.
    ///
    /// - `tuple` - tuple
    /// - `key` - key with MessagePack array header
    ///
    /// Returns:
    /// - `Ordering::Equal`   if `key_fields(tuple) == parts(key)`
    /// - `Ordering::Less`    if `key_fields(tuple) < parts(key)`
    /// - `Ordering::Greater` if `key_fields(tuple) > parts(key)`
    pub fn compare_with_key<K>(&self, tuple: &Tuple, key: &K) -> Ordering
    where
        K: ToTupleBuffer,
    {
        let key_buf = key.to_tuple_buffer().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as _;
        unsafe {
            ffi::box_tuple_compare_with_key(tuple.ptr.as_ptr(), key_buf_ptr, self.inner)
                .cmp(&0)
        }
    }
}

impl Drop for KeyDef {
    fn drop(&mut self) {
        unsafe { ffi::box_key_def_delete(self.inner) }
    }
}

unsafe fn field_value_from_ptr<'de, T>(field_ptr: *mut u8) -> Result<Option<T>>
where
    T: Decode<'de>,
{
    if field_ptr.is_null() {
        return Ok(None);
    }

    // Theoretically this is an exploit point, which would allow reading up to
    // 2gigs of memory in case `value_ptr` happens to point to memory which
    // isn't a field of a tuple, but is a valid messagepack value
    let max_len = u32::MAX >> 1;
    let rough_slice = std::slice::from_raw_parts(field_ptr, max_len as _);
    let mut cursor = std::io::Cursor::new(rough_slice);
    let start = cursor.position() as usize;
    // There's overhead for iterating over the whole msgpack value, but this is
    // necessary.
    crate::msgpack::skip_value(&mut cursor)?;
    let value_range = start..(cursor.position() as usize);
    let rough_slice = cursor.into_inner();
    let value_slice = &rough_slice[value_range];
    Ok(Some(T::decode(value_slice)?))
}

////////////////////////////////////////////////////////////////////////////////
/// FunctionCtx
////////////////////////////////////////////////////////////////////////////////

#[repr(C)]
#[derive(Debug)]
pub struct FunctionCtx {
    inner: *mut ffi::BoxFunctionCtx,
}

impl FunctionCtx {
    /// Return a Tuple from stored procedure.
    ///
    /// Returned Tuple is automatically reference counted by Tarantool.
    ///
    /// - `tuple` - a Tuple to return
    #[inline]
    pub fn return_tuple(&self, tuple: &Tuple) -> Result<c_int> {
        let result = unsafe { ffi::box_return_tuple(self.inner, tuple.ptr.as_ptr()) };
        if result < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(result)
        }
    }

    /// Return a value encoded as MessagePack from a stored procedure.
    ///
    /// MessagePack is not validated, for the sake of speed. It is
    /// expected to be a single encoded object. An attempt to encode
    /// and return multiple objects without wrapping them into an
    /// `MP_ARRAY` or `MP_MAP` is undefined behaviour.
    ///
    /// - `value` - value to be encoded to MessagePack
    #[inline]
    pub fn return_mp<T>(&self, value: &T) -> Result<c_int>
    where
        T: Serialize,
    {
        let buf = rmp_serde::to_vec_named(value)?;
        self.return_bytes(&buf)
    }

    /// Return raw bytes representing a MessagePack value from a stored
    /// procedure.
    ///
    /// MessagePack is not validated, for the sake of speed. It is
    /// expected to be a single encoded object. An attempt to encode
    /// and return multiple objects without wrapping them into an
    /// `MP_ARRAY` or `MP_MAP` is undefined behaviour.
    ///
    /// - `bytes` - raw msgpack bytes to be returned
    #[inline]
    pub fn return_bytes(&self, bytes: &[u8]) -> Result<c_int> {
        let Range { start, end } = bytes.as_ptr_range();
        let result = unsafe {
            ffi::box_return_mp(self.inner, start as _, end as _)
        };

        if result < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(result)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
/// FunctionArgs
////////////////////////////////////////////////////////////////////////////////

#[repr(C)]
pub struct FunctionArgs {
    pub start: *const u8,
    pub end: *const u8,
}

impl Debug for FunctionArgs {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_tuple("FunctionArgs").field(&Tuple::from(self)).finish()
    }
}

impl From<FunctionArgs> for Tuple {
    fn from(args: FunctionArgs) -> Tuple {
        Tuple::from(&args)
    }
}

impl From<&FunctionArgs> for Tuple {
    fn from(args: &FunctionArgs) -> Tuple {
        unsafe {
            Tuple::from_raw_data(
                args.start as _,
                args.end.offset_from(args.start) as _,
            )
        }
    }
}

impl FunctionArgs {
    /// Decode the msgpack value represented by the function args.
    #[inline(always)]
    pub fn decode<'a, T>(&'a self) -> Result<T>
    where
        T: Decode<'a>,
    {
        let slice = unsafe {
            std::slice::from_raw_parts(self.start, self.end.offset_from(self.start) as _)
        };
        T::decode(slice)
    }

    /// Deserialize a tuple reprsented by the function args as `T`.
    #[inline(always)]
    #[deprecated = "Use `FunctionArgs::decode` instead."]
    pub fn as_struct<T>(&self) -> Result<T>
    where
        T: DecodeOwned,
    {
        Tuple::from(self).decode()
    }
}

/// Push MessagePack data into a session data channel - socket,
/// console or whatever is behind the session. Note, that
/// successful push does not guarantee delivery in case it was sent
/// into the network. Just like with `write()`/`send()` system calls.
pub fn session_push<T>(value: &T) -> Result<()>
where
    T: ToTupleBuffer,
{
    let buf = value.to_tuple_buffer().unwrap();
    let buf_ptr = buf.as_ptr() as *const c_char;
    if unsafe { ffi::box_session_push(buf_ptr, buf_ptr.add(buf.len())) } < 0 {
        Err(TarantoolError::last().into())
    } else {
        Ok(())
    }
}

#[inline(always)]
fn validate_msgpack<T>(data: T) -> Result<T>
where
    T: AsRef<[u8]> + Into<Vec<u8>>,
{
    let mut slice = data.as_ref();
    let m = rmp::decode::read_marker(&mut slice)?;
    if !matches!(m, Marker::FixArray(_) | Marker::Array16 | Marker::Array32) {
        return Err(error::Encode::InvalidMP(data.into()).into())
    }
    Ok(data)
}

impl<L> tlua::Push<L> for Tuple
where
    L: tlua::AsLua,
{
    type Err = tlua::Void;

    fn push_to_lua(&self, lua: L) -> tlua::PushResult<L, Self> {
        unsafe {
            ffi::luaT_pushtuple(tlua::AsLua::as_lua(&lua), self.ptr.as_ptr());
            Ok(tlua::PushGuard::new(lua, 1))
        }
    }
}

impl<L> tlua::PushOne<L> for Tuple
where
    L: tlua::AsLua,
{
}

impl<L> tlua::LuaRead<L> for Tuple
where
    L: tlua::AsLua,
{
    fn lua_read_at_position(lua: L, index: std::num::NonZeroI32) -> std::result::Result<Self, L> {
        let ptr = unsafe {
            ffi::luaT_istuple(tlua::AsLua::as_lua(&lua), index.get())
        };
        Self::try_from_ptr(ptr).ok_or(lua)
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Decode
////////////////////////////////////////////////////////////////////////////////

/// Types implementing this trait can be decoded from msgpack.
///
/// [`Tuple`] also implements [`Decode`] with an implementation which just
/// copies the bytes as is (and validates them).
pub trait Decode<'de>: Sized {
    fn decode(data: &'de [u8]) -> Result<Self>;
}

impl<'de, T> Decode<'de> for T
where
    T: serde::Deserialize<'de>,
{
    fn decode(data: &'de [u8]) -> Result<Self> {
        Ok(rmp_serde::from_slice(data)?)
    }
}

impl Decode<'_> for Tuple {
    fn decode(data: &[u8]) -> Result<Self> {
        Self::try_from_slice(data)
    }
}

/// Types implementing this trait can be decoded from msgpack by value.
///
/// `DecodeOwned` is to [`Decode`] what [`DeserializeOwned`] is to
/// [`Deserialize`].
///
/// [`Deserialize`]: serde::Deserialize
/// [`DeserializeOwned`]: serde::de::DeserializeOwned
pub trait DecodeOwned: for<'de> Decode<'de> {}
impl<T> DecodeOwned for T
where
    T: for<'de> Decode<'de>,
{}

////////////////////////////////////////////////////////////////////////////////
/// RawBytes
////////////////////////////////////////////////////////////////////////////////

/// A wrapper type for reading raw bytes from a tuple.
///
/// Can be used to read a field of a tuple as raw bytes:
/// ```no_run
/// use tarantool::{tuple::Tuple, tuple::RawBytes};
/// let tuple = Tuple::new(&(1, (2, 3, 4), 5)).unwrap();
/// let second_field: &RawBytes = tuple.get(1).unwrap();
/// assert_eq!(&**second_field, &[0x93, 2, 3, 4]);
/// ```
///
/// This type also implements [`ToTupleBuffer`] such that `to_tuple_buffer`
/// returns `Ok` only if the underlying bytes represent a valid tuple (msgpack
/// array).
#[derive(Debug)]
#[repr(transparent)]
pub struct RawBytes(pub [u8]);

impl<'de> Decode<'de> for &'de RawBytes {
    #[inline(always)]
    fn decode(data: &'de [u8]) -> Result<Self> {
        // TODO: only read msgpack bytes
        unsafe { Ok(&*(data as *const [u8] as *const RawBytes)) }
    }
}

impl ToTupleBuffer for RawBytes {
    #[inline(always)]
    fn write_tuple_data(&self, w: &mut impl Write) -> Result<()> {
        let data = &**self;
        validate_msgpack(data)?;
        w.write_all(data).map_err(Into::into)
    }
}

impl std::ops::Deref for RawBytes {
    type Target = [u8];
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

////////////////////////////////////////////////////////////////////////////////
/// RawByteBuf
////////////////////////////////////////////////////////////////////////////////

/// A wrapper type for reading raw bytes from a tuple.
///
/// The difference between [`TupleBuffer`] and `RawByteBuf` is that the former
/// involves tarantool built-in memory allocation and can only contain a valid
/// tarantool tuple (msgpack array), while the latter is based on a simple heap
/// allocated `Vec<u8>` and can contain any sequence of bytes.
///
/// This type also implements [`ToTupleBuffer`] such that `to_tuple_buffer`
/// returns `Ok` only if the underlying bytes represent a valid tuple.
#[derive(Debug)]
pub struct RawByteBuf(pub Vec<u8>);

impl From<Vec<u8>> for RawByteBuf {
    #[inline(always)]
    fn from(b: Vec<u8>) -> Self {
        Self(b)
    }
}

impl Decode<'_> for RawByteBuf {
    #[inline(always)]
    fn decode(data: &[u8]) -> Result<Self> {
        // TODO: only read msgpack bytes
        Ok(Self(data.into()))
    }
}

impl ToTupleBuffer for RawByteBuf {
    #[inline(always)]
    fn write_tuple_data(&self, w: &mut impl Write) -> Result<()> {
        let data = self.as_slice();
        validate_msgpack(data)?;
        w.write_all(data).map_err(Into::into)
    }
}

impl std::ops::Deref for RawByteBuf {
    type Target = Vec<u8>;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for RawByteBuf {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

