use std::os::raw::c_char;
use std::ptr::null_mut;

use crate::{AsTuple, Error, Index, Tuple};
use crate::error::TarantoolError;

pub struct Space {
    id: u32
}

impl Space {
    /// Find space id by name.
    ///
    /// This function performs SELECT request to `_vspace` system space.
    /// - `name` - space name
    ///
    /// Returns:
    /// - `None` if not found
    /// - `Some(space)` otherwise
    ///
    /// See also: [index_by_name](#method.index_by_name)
    pub fn find_by_name(name: &str) -> Result<Option<Self>, Error> {
        let id = unsafe { ffi::box_space_id_by_name(
            name.as_ptr() as *const c_char,
            name.len() as u32
        )};

        if id == ffi::BOX_ID_NIL {
            TarantoolError::maybe_last().map(|_| None).map_err(|e| e.into())
        }
        else {
            Ok(Some(Self{id}))
        }
    }

    /// Find index id by name.
    ///
    /// This function performs SELECT request to _vindex system space.
    /// - `name` - index name
    ///
    /// Returns:
    /// - `None` if not found
    /// - `Some(index)` otherwise
    ///
    /// See also: [find_by_name](#method.find_by_name)
    pub fn index_by_name(&self, name: &str) -> Result<Option<Index>, Error> {
        let index_id = unsafe { ffi::box_index_id_by_name(
            self.id,
            name.as_ptr() as *const c_char,
            name.len() as u32
        )};

        if index_id == ffi::BOX_ID_NIL {
            TarantoolError::maybe_last().map(|_| None).map_err(|e| e.into())
        }
        else {
            Ok(Some(Index::new(self.id, index_id)))
        }
    }

    /// Returns index with id = 0
    pub fn primary_key(&self) -> Index {
        Index::new(self.id, 0)
    }

    /// Execute an INSERT request.
    ///
    /// - `value` - tuple value to insert
    /// - `with_result` - indicates if result is required. If `false` - successful result will always contain `None`
    ///
    /// Returns a new tuple.
    ///
    /// See also: `box.space[space_id]:insert(tuple)`
    pub fn insert<T>(&mut self, value: &T, with_result: bool) -> Result<Option<Tuple>, Error>
            where T: AsTuple {
        let buf = value.serialize_as_tuple().unwrap();
        let buf_ptr = buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<ffi::BoxTuple>();

        if unsafe { ffi::box_insert(
            self.id,
            buf_ptr,
            buf_ptr.offset(buf.len() as isize),
            if with_result { &mut result_ptr } else { null_mut() }
        ) } < 0 {
            return Err(TarantoolError::last().into());
        }

        Ok(if with_result {
            Some(Tuple::from_ptr(result_ptr))
        }
        else {
            None
        })
    }

    /// Execute an REPLACE request.
    ///
    /// - `value` - tuple value to replace with
    /// - `with_result` - indicates if result is required. If `false` - successful result will always contain `None`
    ///
    /// Returns a new tuple.
    ///
    /// See also: `box.space[space_id]:replace(tuple)`
    pub fn replace<T>(&mut self, value: &T, with_result: bool) -> Result<Option<Tuple>, Error>
            where T: AsTuple {
        let buf = value.serialize_as_tuple().unwrap();
        let buf_ptr = buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<ffi::BoxTuple>();

        if unsafe { ffi::box_replace(
            self.id,
            buf_ptr,
            buf_ptr.offset(buf.len() as isize),
            if with_result { &mut result_ptr } else { null_mut() }
        ) } < 0 {
            return Err(TarantoolError::last().into());
        }

        Ok(if with_result {
            Some(Tuple::from_ptr(result_ptr))
        }
        else {
            None
        })
    }

    /// Truncate space.
    pub fn truncate(&mut self) -> Result<(), Error> {
        if unsafe { ffi::box_truncate(self.id) } < 0 {
            return Err(TarantoolError::last().into());
        }
        Ok(())
    }
}

#[allow(dead_code)]
mod ffi {
    use std::os::raw::{c_char, c_int};

    pub use crate::tuple::ffi::BoxTuple;

    pub const BOX_SYSTEM_ID_MIN: u32 = 256;
    pub const BOX_SCHEMA_ID: u32 = 272;
    pub const BOX_SPACE_ID: u32 = 280;
    pub const BOX_VSPACE_ID: u32 = 281;
    pub const BOX_INDEX_ID: u32 = 288;
    pub const BOX_VINDEX_ID: u32 = 289;
    pub const BOX_FUNC_ID: u32 = 296;
    pub const BOX_VFUNC_ID: u32 = 297;
    pub const BOX_USER_ID: u32 = 304;
    pub const BOX_VUSER_ID: u32 = 305;
    pub const BOX_PRIV_ID: u32 = 312;
    pub const BOX_VPRIV_ID: u32 = 313;
    pub const BOX_CLUSTER_ID: u32 = 320;
    pub const BOX_SYSTEM_ID_MAX: u32 = 511;
    pub const BOX_ID_NIL: u32 = 2147483647;

    extern "C" {
        pub fn box_space_id_by_name(name: *const c_char, len: u32) -> u32;
        pub fn box_index_id_by_name(space_id: u32, name: *const c_char, len: u32) -> u32;
        pub fn box_insert(
            space_id: u32,
            tuple: *const c_char,
            tuple_end: *const c_char,
            result: *mut *mut BoxTuple
        ) -> c_int;
        pub fn box_replace(
            space_id: u32,
            tuple: *const c_char,
            tuple_end: *const c_char,
            result: *mut *mut BoxTuple
        ) -> c_int;
        pub fn box_truncate(space_id: u32) -> c_int;
    }
}
