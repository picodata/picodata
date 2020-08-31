use std::io::Cursor;
use std::os::raw::c_char;
use std::ptr::copy_nonoverlapping;

use serde::{de::DeserializeOwned, Serialize};

use crate::error::{Error, TarantoolError};

pub struct Tuple {
    ptr: *mut ffi::BoxTuple,
}

impl Tuple {
    /// Creates new tuple from `value`.
    pub fn new_from_struct<T>(value: &T) -> Result<Self, Error> where T: AsTuple {
        let format_ptr = unsafe { ffi::box_tuple_format_default() };
        let buf = value.serialize_as_tuple()?;
        let buf_ptr = buf.as_ptr() as *const c_char;
        let tuple_ptr = unsafe { ffi::box_tuple_new(
            format_ptr,
            buf_ptr,
            buf_ptr.offset(buf.len() as isize),
        ) };

        unsafe { ffi::box_tuple_ref(tuple_ptr) };
        Ok(Tuple{ptr: tuple_ptr})
    }

    pub(crate) fn from_raw_data(data_ptr: *mut c_char, len: u32) -> Self {
        let format_ptr = unsafe { ffi::box_tuple_format_default() };
        let tuple_ptr = unsafe { ffi::box_tuple_new(
            format_ptr,
            data_ptr,
            data_ptr.offset(len as isize),
        ) };

        unsafe { ffi::box_tuple_ref(tuple_ptr) };
        Tuple{ptr: tuple_ptr}
    }

    pub(crate) fn from_ptr(ptr: *mut ffi::BoxTuple) -> Self {
        unsafe { ffi::box_tuple_ref(ptr) };
        Tuple{ptr}
    }

    /// Return the number of fields in tuple (the size of MsgPack Array).
    pub fn len(&self) -> u32 {
        unsafe { ffi::box_tuple_field_count(self.ptr) }
    }

    /// Return the number of bytes used to store internal tuple data (MsgPack Array).
    pub fn size(&self) -> usize {
        unsafe { ffi::box_tuple_bsize(self.ptr) }
    }

    pub fn into_struct<T>(self) -> Result<T, Error> where T: DeserializeOwned {
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
        Tuple{ptr: self.ptr}
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

impl<Ta, Tb> AsTuple for (Ta, Tb) where
    Ta: Serialize,
    Tb: Serialize {}

impl<Ta, Tb, Tc> AsTuple for (Ta, Tb, Tc) where
    Ta: Serialize,
    Tb: Serialize,
    Tc: Serialize {}

impl<Ta, Tb, Tc, Td> AsTuple for (Ta, Tb, Tc, Td) where
    Ta: Serialize,
    Tb: Serialize,
    Tc: Serialize,
    Td: Serialize {}

pub enum TupleBuffer {
    Vector(Vec<u8>),
    TransactionScoped{
        ptr: *mut u8,
        size: usize,
    },
}

impl TupleBuffer {
    pub fn as_ptr(&self) -> *const u8 {
        match self {
            TupleBuffer::Vector(vec) => vec.as_ptr(),
            TupleBuffer::TransactionScoped { ptr, size: _ } => ptr.clone()
        }
    }

    pub fn len(&self) -> usize {
        match self {
            TupleBuffer::Vector(vec) => vec.len(),
            TupleBuffer::TransactionScoped { ptr: _, size } => size.clone()
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

                Self::TransactionScoped{ptr, size}
            }
        }
        else {
            Self::Vector(buf)
        }
    }
}

pub(crate) mod ffi {
    use std::os::raw::{c_char, c_int};

    pub use crate::c_api::{box_tuple_format_default, BoxTupleFormat};

    #[repr(C)]
    #[derive(Debug, Copy, Clone)]
    pub struct BoxTuple {
        _unused: [u8; 0],
    }

    extern "C" {
        pub fn box_tuple_new(format: *mut BoxTupleFormat, data: *const c_char, end: *const c_char) -> *mut BoxTuple;
        pub fn box_tuple_ref(tuple: *mut BoxTuple) -> c_int;
        pub fn box_tuple_unref(tuple: *mut BoxTuple);
        pub fn box_tuple_field_count(tuple: *const BoxTuple) -> u32;
        pub fn box_tuple_bsize(tuple: *const BoxTuple) -> usize;
        pub fn box_tuple_to_buf(tuple: *const BoxTuple, buf: *mut c_char, size: usize) -> isize;
    }
}
