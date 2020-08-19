use std::io::Cursor;
use std::os::raw::c_char;

use serde::{de::DeserializeOwned, Serialize};

use crate::c_api::{self, BoxTuple};
use crate::Error;

pub struct Tuple {
    ptr: *mut BoxTuple,
}

impl Tuple {
    pub fn new_from_struct<T>(value: &T) -> Result<Self, Error> where T: AsTuple {
        let format_ptr = unsafe { c_api::box_tuple_format_default() };
        let buf = rmp_serde::to_vec(value)?;
        let buf_ptr = buf.as_ptr() as *const c_char;
        let tuple_ptr = unsafe { c_api::box_tuple_new(
            format_ptr,
            buf_ptr,
            buf_ptr.offset(buf.len() as isize),
        ) };

        unsafe { c_api::box_tuple_ref(tuple_ptr) };
        Ok(Tuple{ptr: tuple_ptr})
    }

    pub(crate) fn from_ptr(ptr: *mut BoxTuple) -> Self {
        unsafe { c_api::box_tuple_ref(ptr) };
        Tuple{ptr}
    }

    pub fn field_count(&self) -> u32 {
        unsafe { c_api::box_tuple_field_count(self.ptr) }
    }

    pub fn size(&self) -> usize {
        unsafe { c_api::box_tuple_bsize(self.ptr) }
    }

    pub fn into_struct<T>(self) -> Result<T, Error> where T: DeserializeOwned {
        let raw_data_size = self.size();
        let mut raw_data = Vec::<u8>::with_capacity(raw_data_size);

        let actual_size = unsafe {
            c_api::box_tuple_to_buf(self.ptr, raw_data.as_ptr() as *mut c_char, raw_data_size)
        };
        if actual_size < 0 {
            Error::last()?;
        }

        unsafe { raw_data.set_len(actual_size as usize) };
        Ok(rmp_serde::from_read::<_, T>(Cursor::new(raw_data))?)
    }
}

impl Drop for Tuple {
    fn drop(&mut self) {
        unsafe { c_api::box_tuple_unref(self.ptr) };
    }
}

impl Clone for Tuple {
    fn clone(&self) -> Self {
        unsafe { c_api::box_tuple_ref(self.ptr) };
        Tuple{ptr: self.ptr}
    }
}

pub trait AsTuple: Serialize {
    fn serialize_as_tuple(&self) -> Result<Vec<u8>, Error> {
        Ok(rmp_serde::to_vec(self)?)
    }
}

impl<T> AsTuple for (T,) where T: Serialize {}
impl<T> AsTuple for Vec<T> where T: Serialize {}
