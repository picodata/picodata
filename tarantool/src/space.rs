use std::os::raw::c_char;
use std::ptr::null_mut;

use serde::Serialize;

use crate::{c_api, Tuple};
use crate::error::ModuleError;

pub struct Space {
    id: u32
}

impl Space {
    pub fn find_by_name(name: &str) -> Result<Option<Self>, ModuleError> {
        let id = unsafe { c_api::box_space_id_by_name(
            name.as_ptr() as *const c_char,
            name.len() as u32
        )};

        Ok(if id == c_api::BOX_ID_NIL {
            ModuleError::last()?;
            None
        }
        else {
            Some(Self{id})
        })
    }

    pub fn index_by_name(&self, name: &str) -> Result<Option<u32>, ModuleError> {
        let index_id = unsafe { c_api::box_index_id_by_name(
            self.id,
            name.as_ptr() as *const c_char,
            name.len() as u32
        )};

        Ok(if index_id == c_api::BOX_ID_NIL {
            ModuleError::last()?;
            None
        }
        else {
            Some(index_id)
        })
    }

    pub fn insert<T>(&mut self, value: &T, with_result: bool) -> Result<Option<Tuple>, ModuleError>
            where T: Serialize {
        let buf = rmp_serde::to_vec(value).unwrap();
        let buf_ptr = buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<c_api::BoxTuple>();

        if unsafe { c_api::box_insert(
            self.id,
            buf_ptr,
            buf_ptr.offset(buf.len() as isize),
            if with_result { &mut result_ptr } else { null_mut() }
        ) } < 0 {
            return ModuleError::last().map(|_| None);
        }

        Ok(if with_result {
            Some(Tuple::from_ptr(result_ptr))
        }
        else {
            None
        })
    }

    pub fn replace<T>(&mut self, value: &T, with_result: bool) -> Result<Option<Tuple>, ModuleError>
            where T: Serialize {
        let buf = rmp_serde::to_vec(value).unwrap();
        let buf_ptr = buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<c_api::BoxTuple>();

        if unsafe { c_api::box_replace(
            self.id,
            buf_ptr,
            buf_ptr.offset(buf.len() as isize),
            if with_result { &mut result_ptr } else { null_mut() }
        ) } < 0 {
            return ModuleError::last().map(|_| None);
        }

        Ok(if with_result {
            Some(Tuple::from_ptr(result_ptr))
        }
        else {
            None
        })
    }

    pub fn delete(&mut self, index_id: u32, key: &Vec<String>, with_result: bool)
            -> Result<Option<Tuple>, ModuleError> {
        let key_buf = rmp_serde::to_vec(key).unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<c_api::BoxTuple>();

        if unsafe { c_api::box_delete(
            self.id,
            index_id,
            key_buf_ptr,
            key_buf_ptr.offset(key_buf.len() as isize),
            if with_result { &mut result_ptr } else { null_mut() }
        ) } < 0 {
            return ModuleError::last().map(|_| None);
        }

        Ok(if with_result {
            Some(Tuple::from_ptr(result_ptr))
        }
        else {
            None
        })
    }

    pub fn update<Op>(&mut self, index_id: u32, key: &Vec<String>, ops: &Vec<Op>, index_base: i32, with_result: bool)
            -> Result<Option<Tuple>, ModuleError>
            where Op: Serialize {
        let key_buf = rmp_serde::to_vec(key).unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        let ops_buf = rmp_serde::to_vec(ops).unwrap();
        let ops_buf_ptr = key_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<c_api::BoxTuple>();

        if unsafe { c_api::box_update(
            self.id,
            index_id,
            key_buf_ptr,
            key_buf_ptr.offset(key_buf.len() as isize),
            ops_buf_ptr,
            ops_buf_ptr.offset(ops_buf.len() as isize),
            index_base,
            if with_result { &mut result_ptr } else { null_mut() }
        ) } < 0 {
            return ModuleError::last().map(|_| None);
        }


        Ok(if with_result {
            Some(Tuple::from_ptr(result_ptr))
        }
        else {
            None
        })
    }

    pub fn upsert<T, Op>(&mut self, index_id: u32, value: &T, ops: &Vec<Op>, index_base: i32, with_result: bool)
            -> Result<Option<Tuple>, ModuleError>
            where T: Serialize, Op: Serialize {
        let value_buf = rmp_serde::to_vec(value).unwrap();
        let value_buf_ptr = value_buf.as_ptr() as *const c_char;
        let ops_buf = rmp_serde::to_vec(ops).unwrap();
        let ops_buf_ptr = ops_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<c_api::BoxTuple>();

        if unsafe { c_api::box_upsert(
            self.id,
            index_id,
            value_buf_ptr,
            value_buf_ptr.offset(value_buf.len() as isize),
            ops_buf_ptr,
            ops_buf_ptr.offset(ops_buf.len() as isize),
            index_base,
            if with_result { &mut result_ptr } else { null_mut() }
        ) } < 0 {
            return ModuleError::last().map(|_| None);
        }

        Ok(if with_result {
            Some(Tuple::from_ptr(result_ptr))
        }
        else {
            None
        })
    }

    pub fn truncate(&mut self) -> Result<(), ModuleError> {
        if unsafe { c_api::box_truncate(self.id) } < 0 {
            return ModuleError::last();
        }
        Ok(())
    }
}
