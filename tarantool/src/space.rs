use std::os::raw::c_char;
use std::ptr::null_mut;

use crate::{AsTuple, c_api, Error, Index, Tuple};

pub struct Space {
    id: u32
}

impl Space {
    pub fn find_by_name(name: &str) -> Result<Option<Self>, Error> {
        let id = unsafe { c_api::box_space_id_by_name(
            name.as_ptr() as *const c_char,
            name.len() as u32
        )};

        Ok(if id == c_api::BOX_ID_NIL {
            Error::last()?;
            None
        }
        else {
            Some(Self{id})
        })
    }

    pub fn index_by_name(&self, name: &str) -> Result<Option<Index>, Error> {
        let index_id = unsafe { c_api::box_index_id_by_name(
            self.id,
            name.as_ptr() as *const c_char,
            name.len() as u32
        )};

        Ok(if index_id == c_api::BOX_ID_NIL {
            Error::last()?;
            None
        }
        else {
            Some(Index::new(self.id, index_id))
        })
    }

    pub fn primary_key(&self) -> Index {
        Index::new(self.id, 0)
    }

    pub fn insert<T>(&mut self, value: &T, with_result: bool) -> Result<Option<Tuple>, Error>
            where T: AsTuple {
        let buf = value.serialize_as_tuple().unwrap();
        let buf_ptr = buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<c_api::BoxTuple>();

        if unsafe { c_api::box_insert(
            self.id,
            buf_ptr,
            buf_ptr.offset(buf.len() as isize),
            if with_result { &mut result_ptr } else { null_mut() }
        ) } < 0 {
            return Error::last().map(|_| None);
        }

        Ok(if with_result {
            Some(Tuple::from_ptr(result_ptr))
        }
        else {
            None
        })
    }

    pub fn replace<T>(&mut self, value: &T, with_result: bool) -> Result<Option<Tuple>, Error>
            where T: AsTuple {
        let buf = value.serialize_as_tuple().unwrap();
        let buf_ptr = buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<c_api::BoxTuple>();

        if unsafe { c_api::box_replace(
            self.id,
            buf_ptr,
            buf_ptr.offset(buf.len() as isize),
            if with_result { &mut result_ptr } else { null_mut() }
        ) } < 0 {
            return Error::last().map(|_| None);
        }

        Ok(if with_result {
            Some(Tuple::from_ptr(result_ptr))
        }
        else {
            None
        })
    }

    pub fn truncate(&mut self) -> Result<(), Error> {
        if unsafe { c_api::box_truncate(self.id) } < 0 {
            return Error::last();
        }
        Ok(())
    }
}
