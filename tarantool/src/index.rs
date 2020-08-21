use std::os::raw::c_char;
use std::ptr::null_mut;

use num_traits::ToPrimitive;

use crate::{AsTuple, c_api, Error, Tuple};

pub struct Index {
    space_id: u32,
    index_id: u32,
}

impl Index {
    pub(crate) fn new(space_id: u32, index_id: u32) -> Self {
        Index{space_id, index_id}
    }

    pub fn get<K>(&self, key: &K) -> Result<Option<Tuple>, Error> where K: AsTuple {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<c_api::BoxTuple>();

        if unsafe { c_api::box_index_get(
            self.space_id,
            self.index_id,
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

    pub fn select<K>(&self, iterator_type: c_api::IteratorType, key: &K)
            -> Result<IndexIterator, Error>
            where K: AsTuple {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;

        let ptr = unsafe {
            c_api::box_index_iterator(
                self.space_id,
                self.index_id,
                iterator_type.to_i32().unwrap(),
                key_buf_ptr,
                key_buf_ptr.offset(key_buf.len() as isize),
            )
        };

        if ptr.is_null() {
            Error::last()?;
        }

        Ok(IndexIterator {
            ptr,
            _key_data: key_buf
        })
    }

    pub fn len(&self) -> Result<usize, Error> {
        let result = unsafe { c_api::box_index_len(
            self.space_id,
            self.index_id,
        ) };

        if result < 0 {
            Error::last()?;
        }
        Ok(result as usize)
    }

    pub fn size(&self) -> Result<usize, Error> {
        let result = unsafe { c_api::box_index_bsize(
            self.space_id,
            self.index_id,
        ) };

        if result < 0 {
            Error::last()?;
        }
        Ok(result as usize)
    }

    pub fn random(&self, seed: u32) -> Result<Option<Tuple>, Error> {
        let mut result_ptr = null_mut::<c_api::BoxTuple>();
        if unsafe { c_api::box_index_random(
            self.space_id,
            self.index_id,
            seed,
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

    pub fn min<K>(&self, key: &K) -> Result<Option<Tuple>, Error> where K: AsTuple {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<c_api::BoxTuple>();

        if unsafe { c_api::box_index_min(
            self.space_id,
            self.index_id,
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

    pub fn max<K>(&self, key: &K) -> Result<Option<Tuple>, Error> where K: AsTuple {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<c_api::BoxTuple>();

        if unsafe { c_api::box_index_max(
            self.space_id,
            self.index_id,
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

    pub fn count<K>(&self, iterator_type: c_api::IteratorType, key: &K)
        -> Result<usize, Error>
        where K: AsTuple {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;

        let result  = unsafe {
            c_api::box_index_count(
                self.space_id,
                self.index_id,
                iterator_type.to_i32().unwrap(),
                key_buf_ptr,
                key_buf_ptr.offset(key_buf.len() as isize),
            )
        };

        if result < 0 {
            Error::last()?;
        }
        Ok(result as usize)
    }

    pub fn extract_key(&self, tuple: Tuple) -> Tuple {
        let mut result_size: u32 = 0;
        let result_ptr = unsafe {
            c_api::box_tuple_extract_key(
                tuple.into_ptr(),
                self.space_id,
                self.index_id,
                &mut result_size
            )
        };
        Tuple::from_raw_data(result_ptr, result_size)
    }
}

pub struct IndexIterator {
    ptr: *mut c_api::BoxIterator,
    _key_data: Vec<u8>
}

impl Iterator for IndexIterator {
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

impl Drop for IndexIterator {
    fn drop(&mut self) {
        unsafe { c_api::box_iterator_free(self.ptr) };
    }
}
