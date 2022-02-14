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
use std::io::Cursor;
use std::os::raw::{c_char, c_int};
use std::ptr::{copy_nonoverlapping, NonNull};
use std::slice::from_raw_parts;

use num_traits::ToPrimitive;
use rmp::Marker;
use serde::{de::DeserializeOwned, Serialize};

use crate::error::{Result, TarantoolError};
use crate::ffi::tarantool as ffi;
use crate::tlua as tlua;

/// Tuple
pub struct Tuple {
    ptr: NonNull<ffi::BoxTuple>,
}

impl Tuple {
    /// Creates new tuple from `value`.
    ///
    /// This function will serialize structure instance `value` of type `T` into tuple internal representation
    ///
    /// See also: [AsTuple](trait.AsTuple.html)
    pub fn from_struct<T>(value: &T) -> Result<Self>
    where
        T: AsTuple,
    {
        let buf = value.serialize_as_tuple()?;
        unsafe {
            Ok(Self::from_raw_data(buf.as_ptr() as _, buf.len() as _))
        }
    }

    /// # Safety
    /// `data` must point to a buffer containing `len` bytes
    pub unsafe fn from_raw_data(data: *mut c_char, len: u32) -> Self {
        let format = TupleFormat::default();
        let tuple_ptr = ffi::box_tuple_new(
            format.inner,
            data as _,
            data.add(len as _) as _
        );

        Self::from_ptr(NonNull::new_unchecked(tuple_ptr))
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
        unsafe { ffi::box_tuple_bsize(self.ptr.as_ptr()) }
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
    /// ```
    /// let mut it = tuple.iter().unwrap();
    ///
    /// while let Some(field) = it.next().unwrap() {
    ///     // process data
    /// }
    ///
    /// // rewind iterator to first position
    /// it.rewind();
    /// assert!(it.position() == 0);
    ///
    /// // rewind iterator to first position
    /// field = it.seek(3).unwrap();
    /// assert!(it.position() == 4);
    /// ```
    pub fn iter(&self) -> Result<TupleIterator> {
        let inner = unsafe { ffi::box_tuple_iterator(self.ptr.as_ptr()) };
        if inner.is_null() {
            Err(TarantoolError::last().into())
        } else {
            Ok(TupleIterator { inner })
        }
    }

    /// Return the raw Tuple field in MsgPack format.
    ///
    /// The buffer is valid until next call to box_tuple_* functions.
    ///
    /// - `fieldno` - zero-based index in MsgPack array.
    ///
    /// Returns:
    /// - `None` if `i >= box_tuple_field_count(Tuple)` or if field has a non primitive type
    /// - field value otherwise
    pub fn field<T>(&self, fieldno: u32) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let result_ptr = unsafe { ffi::box_tuple_field(self.ptr.as_ptr(), fieldno) };
        field_value_from_ptr(result_ptr as *mut u8)
    }

    /// Deserializes tuple contents into structure of type `T`
    pub fn as_struct<T>(&self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let raw_data = self.as_buffer();
        Ok(rmp_serde::from_read::<_, T>(Cursor::new(raw_data))?)
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
    pub fn into_struct<T>(self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        self.as_struct()
    }

    pub(crate) fn into_ptr(self) -> *mut ffi::BoxTuple {
        self.ptr.as_ptr()
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

/// Must be implemented for types, which will be used with box access methods as data
pub trait AsTuple: Serialize {
    /// Describes how object can be converted to [Tuple](struct.Tuple.html).
    ///
    /// Has default implementation, but can be overloaded for special cases
    fn serialize_as_tuple(&self) -> Result<TupleBuffer> {
        let data = rmp_serde::to_vec(self)?;
        Ok(unsafe { TupleBuffer::from_vec(data) })
    }
}

impl AsTuple for () {
    fn serialize_as_tuple(&self) -> Result<TupleBuffer> {
        let data = rmp_serde::to_vec(&Vec::<()>::new())?;
        Ok(unsafe { TupleBuffer::from_vec(data) })
    }
}

impl<T> AsTuple for [T] where T: Serialize {}
impl<T> AsTuple for Vec<T> where T: Serialize {}

macro_rules! impl_array {
    ($($n:literal)+) => {
        $(
            #[allow(clippy::zero_prefixed_literal)]
            impl<T> AsTuple for [T; $n] where T: Serialize {}
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
        impl<$h, $($t),*> AsTuple for ($h, $($t),*)
        where
            $h: Serialize,
            $($t: Serialize,)*
        {}

        impl_tuple! { $($t)* }
    }
}

impl_tuple! { A B C D E F G H I J K L M N O P }

/// Buffer containing tuple contents (MsgPack array)
///
/// If buffer is allocated within transaction: will be disposed after transaction ended (committed or dropped).
/// If not: will act as a regular rust `Vec<u8>`
pub enum TupleBuffer {
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
    pub unsafe fn from_vec(buf: Vec<u8>) -> Self {
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
}

impl From<Tuple> for TupleBuffer {
    fn from(t: Tuple) -> Self {
        Self::Vector(t.as_buffer())
    }
}

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

/// Tuple iterator
pub struct TupleIterator {
    inner: *mut ffi::BoxTupleIterator,
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
    pub fn seek<T>(&mut self, fieldno: u32) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let result_ptr = unsafe { ffi::box_tuple_seek(self.inner, fieldno) };
        field_value_from_ptr(result_ptr as *mut u8)
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
    pub fn next<T>(&mut self) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let result_ptr = unsafe { ffi::box_tuple_next(self.inner) };
        field_value_from_ptr(result_ptr as *mut u8)
    }

    pub fn update(&mut self) {}
}

impl Drop for TupleIterator {
    fn drop(&mut self) {
        unsafe { ffi::box_tuple_iterator_free(self.inner) }
    }
}

impl TupleIterator {}

#[repr(u32)]
#[derive(Debug, ToPrimitive)]
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

pub struct KeyDef {
    inner: *mut ffi::BoxKeyDef,
}

pub struct KeyDefItem {
    pub field_id: u32,
    pub field_type: FieldType,
}

impl KeyDef {
    /// Create key definition with key fields with passed typed on passed positions.
    /// May be used for tuple format creation and/or tuple comparison.
    ///
    /// - `items` - array with key field identifiers and key field types (see [FieldType](struct.FieldType.html))
    pub fn new(items: Vec<KeyDefItem>) -> Self {
        let size = items.len();
        let mut ids = Vec::with_capacity(size);
        let mut types = Vec::with_capacity(size);
        for item in items {
            ids.push(item.field_id);
            types.push(item.field_type.to_u32().unwrap());
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
        K: AsTuple,
    {
        let key_buf = key.serialize_as_tuple().unwrap();
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

fn field_value_from_ptr<T>(value_ptr: *mut u8) -> Result<Option<T>>
where
    T: DeserializeOwned,
{
    if value_ptr.is_null() {
        return Ok(None);
    }

    let marker = Marker::from_u8(unsafe { *value_ptr });
    Ok(match marker {
        Marker::FixStr(str_len) => {
            let buf = unsafe { from_raw_parts(value_ptr as *mut u8, (str_len + 1) as usize) };
            Some(rmp_serde::from_read_ref::<_, T>(buf)?)
        }

        Marker::Str8 | Marker::Str16 | Marker::Str32 => {
            let head = unsafe { from_raw_parts(value_ptr as *mut u8, 9) };
            let len = rmp::decode::read_str_len(&mut Cursor::new(head))?;

            let buf = unsafe { from_raw_parts(value_ptr as *mut u8, (len + 9) as usize) };
            Some(rmp_serde::from_read_ref::<_, T>(buf)?)
        }

        Marker::FixPos(_)
        | Marker::FixNeg(_)
        | Marker::Null
        | Marker::True
        | Marker::False
        | Marker::U8
        | Marker::U16
        | Marker::U32
        | Marker::U64
        | Marker::I8
        | Marker::I16
        | Marker::I32
        | Marker::I64
        | Marker::F32
        | Marker::F64 => {
            let buf = unsafe { from_raw_parts(value_ptr as *mut u8, 9) };
            Some(rmp_serde::from_read_ref::<_, T>(buf)?)
        }

        _ => None,
    })
}

#[repr(C)]
pub struct FunctionCtx {
    inner: *mut ffi::BoxFunctionCtx,
}

impl FunctionCtx {
    /// Return a Tuple from stored C procedure.
    ///
    /// Returned Tuple is automatically reference counted by Tarantool.
    ///
    /// - `tuple` - a Tuple to return
    pub fn return_tuple(&self, tuple: &Tuple) -> Result<c_int> {
        let result = unsafe { ffi::box_return_tuple(self.inner, tuple.ptr.as_ptr()) };
        if result < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(result)
        }
    }

    /// Return MessagePack from a stored C procedure. The MessagePack
    /// is copied, so it is safe to free/reuse the passed arguments
    /// after the call.
    ///
    /// MessagePack is not validated, for the sake of speed. It is
    /// expected to be a single encoded object. An attempt to encode
    /// and return multiple objects without wrapping them into an
    /// `MP_ARRAY` or `MP_MAP` is undefined behaviour.
    ///
    /// - `value` - value to be encoded to MessagePack
    pub fn return_mp<T>(&self, value: &T) -> Result<c_int>
    where
        T: Serialize,
    {
        let buf = rmp_serde::to_vec_named(value)?;
        let buf_ptr = buf.as_ptr() as *const c_char;
        let result =
            unsafe { ffi::box_return_mp(self.inner, buf_ptr, buf_ptr.add(buf.len())) };

        if result < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(result)
        }
    }
}

#[repr(C)]
pub struct FunctionArgs {
    pub start: *const u8,
    pub end: *const u8,
}

impl From<FunctionArgs> for Tuple {
    fn from(args: FunctionArgs) -> Tuple {
        unsafe {
            Tuple::from_raw_data(
                args.start as _,
                args.end.offset_from(args.start) as _,
            )
        }
    }
}

/// Push MessagePack data into a session data channel - socket,
/// console or whatever is behind the session. Note, that
/// successful push does not guarantee delivery in case it was sent
/// into the network. Just like with `write()`/`send()` system calls.
pub fn session_push<T>(value: &T) -> Result<()>
where
    T: AsTuple,
{
    let buf = value.serialize_as_tuple().unwrap();
    let buf_ptr = buf.as_ptr() as *const c_char;
    if unsafe { ffi::box_session_push(buf_ptr, buf_ptr.add(buf.len())) } < 0 {
        Err(TarantoolError::last().into())
    } else {
        Ok(())
    }
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

