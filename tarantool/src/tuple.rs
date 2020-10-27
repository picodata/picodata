use std::cmp::Ordering;
use std::io::Cursor;
use std::os::raw::c_char;
use std::ptr::copy_nonoverlapping;
use std::slice::from_raw_parts;

use num_traits::ToPrimitive;
use rmp::Marker;
use serde::{de::DeserializeOwned, Serialize};

use crate::error::{Error, TarantoolError};

pub struct Tuple {
    ptr: *mut ffi::BoxTuple,
}

impl Tuple {
    /// Creates new tuple from `value`.
    pub fn new_from_struct<T>(value: &T) -> Result<Self, Error>
    where
        T: AsTuple,
    {
        let format = TupleFormat::default();
        let buf = value.serialize_as_tuple()?;
        let buf_ptr = buf.as_ptr() as *const c_char;
        let tuple_ptr = unsafe {
            ffi::box_tuple_new(format.inner, buf_ptr, buf_ptr.offset(buf.len() as isize))
        };

        unsafe { ffi::box_tuple_ref(tuple_ptr) };
        Ok(Tuple { ptr: tuple_ptr })
    }

    pub(crate) fn from_raw_data(data_ptr: *mut c_char, len: u32) -> Self {
        let format = TupleFormat::default();
        let tuple_ptr =
            unsafe { ffi::box_tuple_new(format.inner, data_ptr, data_ptr.offset(len as isize)) };

        unsafe { ffi::box_tuple_ref(tuple_ptr) };
        Tuple { ptr: tuple_ptr }
    }

    pub(crate) fn from_ptr(ptr: *mut ffi::BoxTuple) -> Self {
        unsafe { ffi::box_tuple_ref(ptr) };
        Tuple { ptr }
    }

    /// Return the number of fields in tuple (the size of MsgPack Array).
    pub fn len(&self) -> u32 {
        unsafe { ffi::box_tuple_field_count(self.ptr) }
    }

    /// Return the number of bytes used to store internal tuple data (MsgPack Array).
    pub fn size(&self) -> usize {
        unsafe { ffi::box_tuple_bsize(self.ptr) }
    }

    /// Return the associated format.
    pub fn format(&self) -> TupleFormat {
        TupleFormat {
            inner: unsafe { ffi::box_tuple_format(self.ptr) },
        }
    }

    /// Allocate and initialize a new `Tuple` iterator. The `Tuple` iterator
    /// allow to iterate over fields at root level of MsgPack array.
    ///
    /// Example:
    /// ```
    /// let mut it = tuple.iterator().unwrap();
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
    pub fn iterator(&self) -> Result<TupleIterator, Error> {
        let inner = unsafe { ffi::box_tuple_iterator(self.ptr) };
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
    pub fn get_field<T>(&self, fieldno: u32) -> Result<Option<T>, Error>
    where
        T: DeserializeOwned,
    {
        let result_ptr = unsafe { ffi::box_tuple_field(self.ptr, fieldno) };
        field_value_from_ptr(result_ptr as *mut u8)
    }

    pub fn into_struct<T>(self) -> Result<T, Error>
    where
        T: DeserializeOwned,
    {
        let raw_data_size = self.size();
        let mut raw_data = Vec::<u8>::with_capacity(raw_data_size);

        let actual_size = unsafe {
            ffi::box_tuple_to_buf(self.ptr, raw_data.as_ptr() as *mut c_char, raw_data_size)
        };
        if actual_size < 0 {
            return Err(TarantoolError::last().into());
        }

        unsafe { raw_data.set_len(actual_size as usize) };
        Ok(rmp_serde::from_read::<_, T>(Cursor::new(raw_data))?)
    }

    pub(crate) fn into_ptr(self) -> *mut ffi::BoxTuple {
        self.ptr
    }
}

impl Drop for Tuple {
    fn drop(&mut self) {
        unsafe { ffi::box_tuple_unref(self.ptr) };
    }
}

impl Clone for Tuple {
    fn clone(&self) -> Self {
        unsafe { ffi::box_tuple_ref(self.ptr) };
        Tuple { ptr: self.ptr }
    }
}

/// Must be implemented for types, which will be used with box access methods as data
pub trait AsTuple: Serialize {
    /// Describes how object can be converted to [Tuple](struct.Tuple.html).
    ///
    /// Has default implementation, but can be overloaded for special cases
    fn serialize_as_tuple(&self) -> Result<TupleBuffer, Error> {
        Ok(rmp_serde::to_vec(self)?.into())
    }
}

impl AsTuple for () {
    fn serialize_as_tuple(&self) -> Result<TupleBuffer, Error> {
        Ok(rmp_serde::to_vec(&Vec::<()>::new())?.into())
    }
}

impl<T> AsTuple for (T,) where T: Serialize {}
impl<T> AsTuple for Vec<T> where T: Serialize {}

impl<Ta, Tb> AsTuple for (Ta, Tb)
where
    Ta: Serialize,
    Tb: Serialize,
{
}

impl<Ta, Tb, Tc> AsTuple for (Ta, Tb, Tc)
where
    Ta: Serialize,
    Tb: Serialize,
    Tc: Serialize,
{
}

impl<Ta, Tb, Tc, Td> AsTuple for (Ta, Tb, Tc, Td)
where
    Ta: Serialize,
    Tb: Serialize,
    Tc: Serialize,
    Td: Serialize,
{
}

pub enum TupleBuffer {
    Vector(Vec<u8>),
    TransactionScoped { ptr: *mut u8, size: usize },
}

impl TupleBuffer {
    pub fn as_ptr(&self) -> *const u8 {
        match self {
            TupleBuffer::Vector(vec) => vec.as_ptr(),
            TupleBuffer::TransactionScoped { ptr, size: _ } => ptr.clone(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            TupleBuffer::Vector(vec) => vec.len(),
            TupleBuffer::TransactionScoped { ptr: _, size } => size.clone(),
        }
    }
}

impl From<Vec<u8>> for TupleBuffer {
    fn from(buf: Vec<u8>) -> Self {
        if unsafe { crate::transaction::ffi::box_txn() } {
            let size = buf.len();
            unsafe {
                let ptr = crate::transaction::ffi::box_txn_alloc(size) as *mut u8;
                copy_nonoverlapping(buf.as_ptr(), ptr, size);

                Self::TransactionScoped { ptr, size }
            }
        } else {
            Self::Vector(buf)
        }
    }
}

/// Tuple Format.
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
    pub fn seek<T>(&mut self, fieldno: u32) -> Result<Option<T>, Error>
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
    pub fn next<T>(&mut self) -> Result<Option<T>, Error>
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
        unsafe { ffi::box_tuple_compare(tuple_a.ptr, tuple_b.ptr, self.inner) }.cmp(&0)
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
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        unsafe { ffi::box_tuple_compare_with_key(tuple.ptr, key_buf_ptr, self.inner) }.cmp(&0)
    }
}

impl Drop for KeyDef {
    fn drop(&mut self) {
        unsafe { ffi::box_key_def_delete(self.inner) }
    }
}

fn field_value_from_ptr<T>(value_ptr: *mut u8) -> Result<Option<T>, Error>
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

pub struct FunctionCtx {
    inner: *mut ffi::BoxFunctionCtx,
}

impl FunctionCtx {
    /// Return a Tuple from stored C procedure.
    ///
    /// Returned Tuple is automatically reference counted by Tarantool.
    ///
    /// - `tuple` - a Tuple to return
    pub fn return_tuple(self, tuple: Tuple) -> Result<(), Error> {
        if unsafe { ffi::box_return_tuple(self.inner, tuple.ptr) } < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(())
        }
    }
}

/// Push MessagePack data into a session data channel - socket,
/// console or whatever is behind the session. Note, that
/// successful push does not guarantee delivery in case it was sent
/// into the network. Just like with `write()`/`send()` system calls.
pub fn session_push<T>(value: &T) -> Result<(), Error>
where
    T: AsTuple,
{
    let buf = value.serialize_as_tuple().unwrap();
    let buf_ptr = buf.as_ptr() as *const c_char;
    if unsafe { ffi::box_session_push(buf_ptr, buf_ptr.offset(buf.len() as isize)) } < 0 {
        Err(TarantoolError::last().into())
    } else {
        Ok(())
    }
}

pub(crate) mod ffi {
    use std::os::raw::{c_char, c_int};

    #[repr(C)]
    #[derive(Debug, Copy, Clone)]
    pub struct BoxTuple {
        _unused: [u8; 0],
    }

    extern "C" {
        pub fn box_tuple_new(
            format: *mut BoxTupleFormat,
            data: *const c_char,
            end: *const c_char,
        ) -> *mut BoxTuple;
        pub fn box_tuple_ref(tuple: *mut BoxTuple) -> c_int;
        pub fn box_tuple_unref(tuple: *mut BoxTuple);
        pub fn box_tuple_field_count(tuple: *const BoxTuple) -> u32;
        pub fn box_tuple_bsize(tuple: *const BoxTuple) -> usize;
        pub fn box_tuple_to_buf(tuple: *const BoxTuple, buf: *mut c_char, size: usize) -> isize;
    }

    #[repr(C)]
    #[derive(Debug, Copy, Clone)]
    pub struct BoxTupleFormat {
        _unused: [u8; 0],
    }

    extern "C" {
        pub fn box_tuple_format_default() -> *mut BoxTupleFormat;
        pub fn box_tuple_format(tuple: *const BoxTuple) -> *mut BoxTupleFormat;
        pub fn box_tuple_field(tuple: *const BoxTuple, fieldno: u32) -> *const c_char;
    }

    #[repr(C)]
    #[derive(Debug, Copy, Clone)]
    pub struct BoxTupleIterator {
        _unused: [u8; 0],
    }

    extern "C" {
        pub fn box_tuple_iterator(tuple: *mut BoxTuple) -> *mut BoxTupleIterator;
        pub fn box_tuple_iterator_free(it: *mut BoxTupleIterator);
        pub fn box_tuple_position(it: *mut BoxTupleIterator) -> u32;
        pub fn box_tuple_rewind(it: *mut BoxTupleIterator);
        pub fn box_tuple_seek(it: *mut BoxTupleIterator, fieldno: u32) -> *const c_char;
        pub fn box_tuple_next(it: *mut BoxTupleIterator) -> *const c_char;
    }

    #[repr(C)]
    #[derive(Debug, Copy, Clone)]
    pub struct BoxKeyDef {
        _unused: [u8; 0],
    }

    extern "C" {
        pub fn box_key_def_new(
            fields: *mut u32,
            types: *mut u32,
            part_count: u32,
        ) -> *mut BoxKeyDef;
        pub fn box_key_def_delete(key_def: *mut BoxKeyDef);
        pub fn box_tuple_compare(
            tuple_a: *mut BoxTuple,
            tuple_b: *mut BoxTuple,
            key_def: *mut BoxKeyDef,
        ) -> c_int;
        pub fn box_tuple_compare_with_key(
            tuple_a: *mut BoxTuple,
            key_b: *const c_char,
            key_def: *mut BoxKeyDef,
        ) -> c_int;
    }

    #[repr(C)]
    #[derive(Debug, Copy, Clone)]
    pub struct BoxFunctionCtx {
        _unused: [u8; 0],
    }

    extern "C" {
        pub fn box_return_tuple(ctx: *mut BoxFunctionCtx, tuple: *mut BoxTuple) -> c_int;
        pub fn box_session_push(data: *const c_char, data_end: *const c_char) -> c_int;
    }
}
