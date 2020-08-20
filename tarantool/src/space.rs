use std::os::raw::c_char;
use std::ptr::null_mut;

use num_traits::ToPrimitive;

use crate::{AsTuple, c_api, Error, Tuple};

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

    pub fn index_by_name(&self, name: &str) -> Result<Option<u32>, Error> {
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
            Some(index_id)
        })
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

    pub fn delete<K>(&mut self, index_id: u32, key: &K, with_result: bool)
            -> Result<Option<Tuple>, Error>
            where K: AsTuple {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<c_api::BoxTuple>();

        if unsafe { c_api::box_delete(
            self.id,
            index_id,
            key_buf_ptr,
            key_buf_ptr.offset(key_buf.len() as isize),
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

    pub fn update<K, Op>(&mut self, index_id: u32, key: &K, ops: &Vec<Op>, index_base: i32, with_result: bool)
            -> Result<Option<Tuple>, Error>
            where K: AsTuple, Op: AsTuple {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        let ops_buf = ops.serialize_as_tuple().unwrap();
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
            return Error::last().map(|_| None);
        }


        Ok(if with_result {
            Some(Tuple::from_ptr(result_ptr))
        }
        else {
            None
        })
    }

    pub fn upsert<T, Op>(&mut self, index_id: u32, value: &T, ops: &Vec<Op>, index_base: i32, with_result: bool)
            -> Result<Option<Tuple>, Error>
            where T: AsTuple, Op: AsTuple {
        let value_buf = value.serialize_as_tuple().unwrap();
        let value_buf_ptr = value_buf.as_ptr() as *const c_char;
        let ops_buf = ops.serialize_as_tuple().unwrap();
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

    pub fn get<K>(&self, index_id: u32, key: &K) -> Result<Option<Tuple>, Error> where K: AsTuple {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<c_api::BoxTuple>();

        if unsafe { c_api::box_index_get(
            self.id,
            index_id,
            key_buf_ptr,
            key_buf_ptr.offset(key_buf.len() as isize),
            &mut result_ptr
        ) } < 0 {
            Error::last()?;
        }

        Ok(if result_ptr.is_null() {
            None
        }
        else {
            Some(Tuple::from_ptr(result_ptr))
        })
    }

    pub fn select<K>(&self, index_id: u32, iterator_type: c_api::IteratorType, key: &K)
            -> Result<SpaceIterator, Error>
            where K: AsTuple {

        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;

        let ptr = unsafe {
            c_api::box_index_iterator(
                self.id,
                index_id,
                iterator_type.to_i32().unwrap(),
                key_buf_ptr,
                key_buf_ptr.offset(key_buf.len() as isize),
            )
        };

        if ptr.is_null() {
            Error::last()?;
        }

        Ok(SpaceIterator{ptr})
    }
}

pub struct SpaceIterator {
    ptr: *mut c_api::BoxIterator,
}

impl Iterator for SpaceIterator {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result_ptr = null_mut::<c_api::BoxTuple>();
        if unsafe { c_api::box_iterator_next(self.ptr, &mut result_ptr) } < 0 {
            return None;
        }

        if result_ptr.is_null() {
            None
        }
        else {
            Some(Tuple::from_ptr(result_ptr))
        }
    }
}

impl Drop for SpaceIterator {
    fn drop(&mut self) {
        unsafe { c_api::box_iterator_free(self.ptr) };
    }
}
