use std::os::raw::c_char;

use std::marker::PhantomData;
use serde::{Serialize, Deserialize};
use serde::de::DeserializeOwned;

use rmp_serde::encode::Error as MsgpackError;

use crate::c_api::{self, BoxTuple};
use std::io::Cursor;

pub struct Tuple {
    ptr: *mut BoxTuple,
}

impl Tuple {
    pub fn new_from_struct<T>(value: &T) -> Self where T: Serialize {
        let format = unsafe { c_api::box_tuple_format_default() };
        let buf = rmp_serde::to_vec(value).unwrap();
        let buf_ptr = buf.as_ptr() as *const c_char;
        let tuple_ptr = unsafe { c_api::box_tuple_new(
            format,
            buf_ptr,
            buf_ptr.offset(buf.len() as isize),
        ) };

        Self{ptr: tuple_ptr}
    }

    pub fn field_count(&self) -> u32 {
        unsafe { c_api::box_tuple_field_count(self.ptr) }
    }

    pub fn size(&self) -> usize {
        unsafe { c_api::box_tuple_bsize(self.ptr) }
    }

    pub fn into_struct<T>(self) -> T where T: DeserializeOwned {
        let raw_data_size = self.size();
        let mut raw_data = Vec::<u8>::with_capacity(raw_data_size);
        let actual_size = unsafe {
            c_api::box_tuple_to_buf(self.ptr, raw_data.as_ptr() as *mut c_char, raw_data_size)
        };

        if actual_size < 0 {
            // TODO: correct handle error
            panic!();
        }
        unsafe { raw_data.set_len(actual_size as usize) };

        let reader = Cursor::new(raw_data);
        rmp_serde::from_read::<_, T>(reader).unwrap()
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
