use std::os::raw::c_char;
use std::ptr::null_mut;

use num_traits::ToPrimitive;

use crate::error::{Error, TarantoolError};
use crate::tuple::{ffi::BoxTuple, AsTuple, Tuple, TupleBuffer};

pub struct Index {
    space_id: u32,
    index_id: u32,
}

/// Controls how to iterate over tuples in an index.
/// Different index types support different iterator types.
/// For example, one can start iteration from a particular value
/// (request key) and then retrieve all tuples where keys are
/// greater or equal (= `GE`) to this key.
///
/// If iterator type is not supported by the selected index type,
/// iterator constructor must fail with `ER_UNSUPPORTED`. To be
/// selectable for primary key, an index must support at least
/// `Eq` and `GE` types.
///
/// `None` value of request key corresponds to the first or last
/// key in the index, depending on iteration direction.
/// (first key for `GE` and `GT` types, and last key for `LE` and `LT`).
/// Therefore, to iterate over all tuples in an index, one can
/// use `GE` or `LE` iteration types with start key equal to `None`.
/// For `EQ`, the key must not be `None`.
#[repr(i32)]
#[derive(Debug, Copy, Clone, ToPrimitive)]
pub enum IteratorType {
    /// key == x ASC order
    Eq = 0,

    /// key == x DESC order
    Req = 1,

    /// all tuples
    All = 2,

    /// key <  x
    LT = 3,

    /// key <= x
    LE = 4,

    /// key >= x
    GE = 5,

    /// key >  x
    GT = 6,

    /// all bits from x are set in key
    BitsAllSet = 7,

    /// at least one x's bit is set
    BitsAnySet = 8,

    /// all bits are not set
    BitsAllNotSet = 9,

    /// key overlaps x
    Ovelaps = 10,

    /// tuples in distance ascending order from specified point
    Neigbor = 11,
}

impl Index {
    pub(crate) fn new(space_id: u32, index_id: u32) -> Self {
        Index { space_id, index_id }
    }

    /// Get a tuple from index by the key.
    ///
    /// Please note that this function works much more faster than [select](#method.select)
    ///
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    ///
    /// Returns a tuple or `None` if index is empty
    pub fn get<K>(&self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
    {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<BoxTuple>();

        if unsafe {
            c_api::box_index_get(
                self.space_id,
                self.index_id,
                key_buf_ptr,
                key_buf_ptr.offset(key_buf.len() as isize),
                &mut result_ptr,
            )
        } < 0
        {
            return Err(TarantoolError::last().into());
        }

        Ok(if result_ptr.is_null() {
            None
        } else {
            Some(Tuple::from_ptr(result_ptr))
        })
    }

    /// Allocate and initialize iterator for index.
    ///
    /// - `type` - iterator type
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    pub fn select<K>(&self, iterator_type: IteratorType, key: &K) -> Result<IndexIterator, Error>
    where
        K: AsTuple,
    {
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
            return Err(TarantoolError::last().into());
        }

        Ok(IndexIterator {
            ptr,
            _key_data: key_buf,
        })
    }

    /// Execute an DELETE request.
    ///
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    /// - `with_result` - indicates if result is required. If `false` - successful result will always contain `None`
    ///
    /// Returns an old tuple
    ///
    /// See also: `box.space[space_id].index[index_id]:delete(key)`
    pub fn delete<K>(&mut self, key: &K, with_result: bool) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
    {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<BoxTuple>();

        if unsafe {
            c_api::box_delete(
                self.space_id,
                self.index_id,
                key_buf_ptr,
                key_buf_ptr.offset(key_buf.len() as isize),
                if with_result {
                    &mut result_ptr
                } else {
                    null_mut()
                },
            )
        } < 0
        {
            return Err(TarantoolError::last().into());
        }

        Ok(if with_result && !result_ptr.is_null() {
            Some(Tuple::from_ptr(result_ptr))
        } else {
            None
        })
    }

    /// Execute an UPDATE request.
    ///
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    /// - `ops` - encoded operations in MsgPack Arrat format, e.g. `[['=', field_id, value], ['!', 2, 'xxx']]`
    /// - `with_result` - indicates if result is required. If `false` - successful result will always contain `None`
    ///
    /// Returns a new tuple.
    ///
    /// See also: `box.space[space_id].index[index_id]:update(key, ops)`, [upsert](#method.upsert)
    pub fn update<K, Op>(
        &mut self,
        key: &K,
        ops: &Vec<Op>,
        with_result: bool,
    ) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
        Op: AsTuple,
    {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        let ops_buf = ops.serialize_as_tuple().unwrap();
        let ops_buf_ptr = ops_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<BoxTuple>();

        if unsafe {
            c_api::box_update(
                self.space_id,
                self.index_id,
                key_buf_ptr,
                key_buf_ptr.offset(key_buf.len() as isize),
                ops_buf_ptr,
                ops_buf_ptr.offset(ops_buf.len() as isize),
                0,
                if with_result {
                    &mut result_ptr
                } else {
                    null_mut()
                },
            )
        } < 0
        {
            return Err(TarantoolError::last().into());
        }

        Ok(if with_result && !result_ptr.is_null() {
            Some(Tuple::from_ptr(result_ptr))
        } else {
            None
        })
    }

    /// Execute an UPSERT request.
    ///
    /// - `value` - encoded tuple in MsgPack Array format (`[field1, field2, ...]`)
    /// - `ops` - encoded operations in MsgPack Arrat format, e.g. `[['=', field_id, value], ['!', 2, 'xxx']]`
    /// - `with_result` - indicates if result is required. If `false` - successful result will always contain `None`
    ///
    /// Returns a new tuple.
    ///
    /// See also: `box.space[space_id].index[index_id]:update(key, ops)`, [update](#method.update)
    pub fn upsert<T, Op>(
        &mut self,
        value: &T,
        ops: &Vec<Op>,
        with_result: bool,
    ) -> Result<Option<Tuple>, Error>
    where
        T: AsTuple,
        Op: AsTuple,
    {
        let value_buf = value.serialize_as_tuple().unwrap();
        let value_buf_ptr = value_buf.as_ptr() as *const c_char;
        let ops_buf = ops.serialize_as_tuple().unwrap();
        let ops_buf_ptr = ops_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<BoxTuple>();

        if unsafe {
            c_api::box_upsert(
                self.space_id,
                self.index_id,
                value_buf_ptr,
                value_buf_ptr.offset(value_buf.len() as isize),
                ops_buf_ptr,
                ops_buf_ptr.offset(ops_buf.len() as isize),
                0,
                if with_result {
                    &mut result_ptr
                } else {
                    null_mut()
                },
            )
        } < 0
        {
            return Err(TarantoolError::last().into());
        }

        Ok(if with_result && !result_ptr.is_null() {
            Some(Tuple::from_ptr(result_ptr))
        } else {
            None
        })
    }

    /// Return the number of elements in the index.
    pub fn len(&self) -> Result<usize, Error> {
        let result = unsafe { c_api::box_index_len(self.space_id, self.index_id) };

        if result < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(result as usize)
        }
    }

    /// Return the number of bytes used in memory by the index.
    pub fn size(&self) -> Result<usize, Error> {
        let result = unsafe { c_api::box_index_bsize(self.space_id, self.index_id) };

        if result < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(result as usize)
        }
    }

    /// Return a random tuple from the index (useful for statistical analysis).
    ///
    /// - `rnd` - random seed
    ///
    /// See also: `box.space[space_id].index[index_id]:random(rnd)`
    pub fn random(&self, seed: u32) -> Result<Option<Tuple>, Error> {
        let mut result_ptr = null_mut::<BoxTuple>();
        if unsafe { c_api::box_index_random(self.space_id, self.index_id, seed, &mut result_ptr) }
            < 0
        {
            return Err(TarantoolError::last().into());
        }

        Ok(if result_ptr.is_null() {
            None
        } else {
            Some(Tuple::from_ptr(result_ptr))
        })
    }

    /// Return a first (minimal) tuple matched the provided key.
    ///
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    ///
    /// Returns a tuple or `None` if index is empty
    pub fn min<K>(&self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
    {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<BoxTuple>();

        if unsafe {
            c_api::box_index_min(
                self.space_id,
                self.index_id,
                key_buf_ptr,
                key_buf_ptr.offset(key_buf.len() as isize),
                &mut result_ptr,
            )
        } < 0
        {
            return Err(TarantoolError::last().into());
        }

        Ok(if result_ptr.is_null() {
            None
        } else {
            Some(Tuple::from_ptr(result_ptr))
        })
    }

    /// Return a last (maximal) tuple matched the provided key.
    ///
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    ///
    /// Returns a tuple or `None` if index is empty
    pub fn max<K>(&self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
    {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<BoxTuple>();

        if unsafe {
            c_api::box_index_max(
                self.space_id,
                self.index_id,
                key_buf_ptr,
                key_buf_ptr.offset(key_buf.len() as isize),
                &mut result_ptr,
            )
        } < 0
        {
            return Err(TarantoolError::last().into());
        }

        Ok(if result_ptr.is_null() {
            None
        } else {
            Some(Tuple::from_ptr(result_ptr))
        })
    }

    /// Count the number of tuple matched the provided key.
    ///
    /// - `type` - iterator type
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    pub fn count<K>(&self, iterator_type: IteratorType, key: &K) -> Result<usize, Error>
    where
        K: AsTuple,
    {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;

        let result = unsafe {
            c_api::box_index_count(
                self.space_id,
                self.index_id,
                iterator_type.to_i32().unwrap(),
                key_buf_ptr,
                key_buf_ptr.offset(key_buf.len() as isize),
            )
        };

        if result < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(result as usize)
        }
    }

    /// Extract key from tuple according to key definition of given
    /// index. Returned buffer is allocated on `box_txn_alloc()` with
    /// this key.
    ///
    /// - `tuple` - tuple from which need to extract key.
    pub fn extract_key(&self, tuple: Tuple) -> Tuple {
        let mut result_size: u32 = 0;
        let result_ptr = unsafe {
            c_api::box_tuple_extract_key(
                tuple.into_ptr(),
                self.space_id,
                self.index_id,
                &mut result_size,
            )
        };
        Tuple::from_raw_data(result_ptr, result_size)
    }
}

pub struct IndexIterator {
    ptr: *mut c_api::BoxIterator,
    _key_data: TupleBuffer,
}

impl Iterator for IndexIterator {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result_ptr = null_mut::<BoxTuple>();
        if unsafe { c_api::box_iterator_next(self.ptr, &mut result_ptr) } < 0 {
            return None;
        }

        if result_ptr.is_null() {
            None
        } else {
            Some(Tuple::from_ptr(result_ptr))
        }
    }
}

impl Drop for IndexIterator {
    fn drop(&mut self) {
        unsafe { c_api::box_iterator_free(self.ptr) };
    }
}

pub mod c_api {
    use std::os::raw::{c_char, c_int};

    use crate::tuple::ffi::BoxTuple;

    #[repr(C)]
    pub struct BoxIterator {
        _unused: [u8; 0],
    }

    extern "C" {
        pub fn box_index_iterator(
            space_id: u32,
            index_id: u32,
            type_: c_int,
            key: *const c_char,
            key_end: *const c_char,
        ) -> *mut BoxIterator;

        pub fn box_iterator_next(iterator: *mut BoxIterator, result: *mut *mut BoxTuple) -> c_int;
        pub fn box_iterator_free(iterator: *mut BoxIterator);
        pub fn box_index_len(space_id: u32, index_id: u32) -> isize;
        pub fn box_index_bsize(space_id: u32, index_id: u32) -> isize;
        pub fn box_index_random(
            space_id: u32,
            index_id: u32,
            rnd: u32,
            result: *mut *mut BoxTuple,
        ) -> c_int;

        pub fn box_index_get(
            space_id: u32,
            index_id: u32,
            key: *const c_char,
            key_end: *const c_char,
            result: *mut *mut BoxTuple,
        ) -> c_int;

        pub fn box_index_min(
            space_id: u32,
            index_id: u32,
            key: *const c_char,
            key_end: *const c_char,
            result: *mut *mut BoxTuple,
        ) -> c_int;

        pub fn box_index_max(
            space_id: u32,
            index_id: u32,
            key: *const c_char,
            key_end: *const c_char,
            result: *mut *mut BoxTuple,
        ) -> c_int;

        pub fn box_index_count(
            space_id: u32,
            index_id: u32,
            type_: c_int,
            key: *const c_char,
            key_end: *const c_char,
        ) -> isize;

        pub fn box_delete(
            space_id: u32,
            index_id: u32,
            key: *const c_char,
            key_end: *const c_char,
            result: *mut *mut BoxTuple,
        ) -> c_int;

        pub fn box_update(
            space_id: u32,
            index_id: u32,
            key: *const c_char,
            key_end: *const c_char,
            ops: *const c_char,
            ops_end: *const c_char,
            index_base: c_int,
            result: *mut *mut BoxTuple,
        ) -> c_int;

        pub fn box_upsert(
            space_id: u32,
            index_id: u32,
            tuple: *const c_char,
            tuple_end: *const c_char,
            ops: *const c_char,
            ops_end: *const c_char,
            index_base: c_int,
            result: *mut *mut BoxTuple,
        ) -> c_int;

        pub fn box_tuple_extract_key(
            tuple: *const BoxTuple,
            space_id: u32,
            index_id: u32,
            key_size: *mut u32,
        ) -> *mut c_char;
    }
}
