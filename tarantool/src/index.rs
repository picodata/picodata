//! Box: indices
//!
//! The `index` submodule provides access for index definitions and index keys.
//! They provide an API for ordered iteration over tuples.
//! This API is a direct binding to corresponding methods of index objects of type `box.index` in the storage engine.
//!
//! See also:
//! - [Indexes](https://www.tarantool.io/en/doc/latest/book/box/data_model/#indexes)
//! - [Lua reference: Submodule box.index](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_index/)
use std::os::raw::{c_char, c_int};
use std::ptr::null_mut;

use num_traits::ToPrimitive;

use crate::error::{Error, TarantoolError};
use crate::ffi::helper::new_c_str;
use crate::ffi::{lua, tarantool as ffi};
use crate::schema::{IndexFieldType, IndexPart, IndexType, RtreeIndexDistanceType};
use crate::tuple::{AsTuple, Tuple, TupleBuffer};

/// An index is a group of key values and pointers.
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
    Overlaps = 10,

    /// tuples in distance ascending order from specified point
    Neighbor = 11,
}

impl Index {
    pub(crate) fn new(space_id: u32, index_id: u32) -> Self {
        Index { space_id, index_id }
    }

    // Drops index.
    pub fn drop(&self) -> Result<(), Error> {
        drop_index(self.space_id, self.index_id)
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
        let mut result_ptr = null_mut::<ffi::BoxTuple>();

        if unsafe {
            ffi::box_index_get(
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
    /// This is an alternative to [space.select()](../space/struct.Space.html#method.select) which goes via a particular
    /// index and can make use of additional parameter that specify the iterator type.
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
            ffi::box_index_iterator(
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

    /// Delete a tuple identified by a key.
    ///
    /// Same as [space.delete()](../space/struct.Space.html#method.delete), but key is searched in this index instead
    /// of in the primary-key index. This index ought to be unique.
    ///
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    ///
    /// Returns the deleted tuple
    pub fn delete<K>(&mut self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
    {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<ffi::BoxTuple>();

        if unsafe {
            ffi::box_delete(
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

    /// Update a tuple.
    ///
    /// Same as [space.update()](../space/struct.Space.html#method.update), but key is searched in this index instead
    /// of primary key. This index ought to be unique.
    ///
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    /// - `ops` - encoded operations in MsgPack array format, e.g. `[['=', field_id, value], ['!', 2, 'xxx']]`
    ///
    /// Returns a new tuple.
    ///
    /// See also: [index.upsert()](#method.upsert)
    pub fn update<K, Op>(&mut self, key: &K, ops: &Vec<Op>) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
        Op: AsTuple,
    {
        let key_buf = key.serialize_as_tuple().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        let ops_buf = ops.serialize_as_tuple().unwrap();
        let ops_buf_ptr = ops_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<ffi::BoxTuple>();

        if unsafe {
            ffi::box_update(
                self.space_id,
                self.index_id,
                key_buf_ptr,
                key_buf_ptr.offset(key_buf.len() as isize),
                ops_buf_ptr,
                ops_buf_ptr.offset(ops_buf.len() as isize),
                0,
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

    /// Execute an UPSERT request.
    ///
    /// Will try to insert tuple. Update if already exists.
    ///
    /// - `value` - encoded tuple in MsgPack Array format (`[field1, field2, ...]`)
    /// - `ops` - encoded operations in MsgPack array format, e.g. `[['=', field_id, value], ['!', 2, 'xxx']]`
    ///
    /// Returns a new tuple.
    ///
    /// See also: [index.update()](#method.update)
    pub fn upsert<T, Op>(&mut self, value: &T, ops: &Vec<Op>) -> Result<Option<Tuple>, Error>
    where
        T: AsTuple,
        Op: AsTuple,
    {
        let value_buf = value.serialize_as_tuple().unwrap();
        let value_buf_ptr = value_buf.as_ptr() as *const c_char;
        let ops_buf = ops.serialize_as_tuple().unwrap();
        let ops_buf_ptr = ops_buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<ffi::BoxTuple>();

        if unsafe {
            ffi::box_upsert(
                self.space_id,
                self.index_id,
                value_buf_ptr,
                value_buf_ptr.offset(value_buf.len() as isize),
                ops_buf_ptr,
                ops_buf_ptr.offset(ops_buf.len() as isize),
                0,
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

    /// Return the number of elements in the index.
    pub fn len(&self) -> Result<usize, Error> {
        let result = unsafe { ffi::box_index_len(self.space_id, self.index_id) };

        if result < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(result as usize)
        }
    }

    /// Return the number of bytes used in memory by the index.
    pub fn bsize(&self) -> Result<usize, Error> {
        let result = unsafe { ffi::box_index_bsize(self.space_id, self.index_id) };

        if result < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(result as usize)
        }
    }

    /// Return a random tuple from the index (useful for statistical analysis).
    ///
    /// - `rnd` - random seed
    pub fn random(&self, seed: u32) -> Result<Option<Tuple>, Error> {
        let mut result_ptr = null_mut::<ffi::BoxTuple>();
        if unsafe { ffi::box_index_random(self.space_id, self.index_id, seed, &mut result_ptr) } < 0
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
        let mut result_ptr = null_mut::<ffi::BoxTuple>();

        if unsafe {
            ffi::box_index_min(
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
        let mut result_ptr = null_mut::<ffi::BoxTuple>();

        if unsafe {
            ffi::box_index_max(
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
            ffi::box_index_count(
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
            ffi::box_tuple_extract_key(
                tuple.into_ptr(),
                self.space_id,
                self.index_id,
                &mut result_size,
            )
        };
        Tuple::from_raw_data(result_ptr, result_size)
    }
}

/// Index iterator. Can be used with `for` statement.
pub struct IndexIterator {
    ptr: *mut ffi::BoxIterator,
    _key_data: TupleBuffer,
}

impl Iterator for IndexIterator {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result_ptr = null_mut::<ffi::BoxTuple>();
        if unsafe { ffi::box_iterator_next(self.ptr, &mut result_ptr) } < 0 {
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
        unsafe { ffi::box_iterator_free(self.ptr) };
    }
}

/// List of options for new or updated index.
///
/// For details see [space_object:create_index - options](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/create_index/).
pub struct IndexOptions {
    pub index_type: Option<IndexType>,
    pub id: Option<u32>,
    pub unique: Option<bool>,
    pub if_not_exists: Option<bool>,
    pub parts: Option<Vec<IndexPart>>,
    pub dimension: Option<u32>,
    pub distance: Option<RtreeIndexDistanceType>,
    pub bloom_fpr: Option<f32>,
    pub page_size: Option<u32>,
    pub range_size: Option<u32>,
    pub run_count_per_level: Option<u32>,
    pub run_size_ratio: Option<f32>,
    pub sequence: Option<IndexSequenceOption>,
    pub func: Option<String>,
    // Only for Tarantool >= 2.6
    // pub hint: Option<bool>,
}

impl Default for IndexOptions {
    fn default() -> Self {
        IndexOptions {
            index_type: Some(IndexType::Tree),
            id: None,
            unique: Some(true),
            if_not_exists: Some(false),
            parts: Some(vec![IndexPart {
                field_index: 1,
                field_type: IndexFieldType::Unsigned,
                collation: None,
                is_nullable: None,
                path: None,
            }]),
            dimension: Some(2),
            distance: Some(RtreeIndexDistanceType::Euclid),
            bloom_fpr: Some(0.05),
            page_size: Some(8 * 1024),
            range_size: None,
            run_count_per_level: Some(2),
            run_size_ratio: Some(3.5),
            sequence: None,
            func: None,
            // Only for Tarantool >= 2.6
            // hint: Some(true),
        }
    }
}

/// Sequence option for new or updated index.
///
/// For details see [specifying a sequence in create_index](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_schema_sequence/create_index/#box-schema-sequence-create-index).
pub enum IndexSequenceOption {
    SeqId {
        seq_id: u32,
        field_index: Option<u32>,
    },
    SeqName {
        seq_name: String,
        field_index: Option<u32>,
    },
    True,
    Empty,
}

/// Create new index for space.
///
/// - `space_id`   - ID of existing space.
/// - `index_name` - name of index to create, which should conform to the rules for object names.
/// - `opts`       - see IndexOptions struct.
///
/// For details see [space_object:create_index](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/create_index/)
pub fn create_index(space_id: u32, index_name: &str, opts: &IndexOptions) -> Result<(), Error> {
    unsafe {
        // Create new stack (just in case - in order no to mess things
        // in current stack).
        let state = lua::luaT_state();
        let ci_state = lua::lua_newthread(state);

        // Execute the following lua Code:
        // -- space = box.space._space:get(space_id)
        // -- space_name = space.name
        // -- box.space[space_name]:create_index(name, opts)

        // -- space = box.space._space:get(space_id)
        lua::lua_getglobal(ci_state, new_c_str("box").as_ptr());
        lua::lua_getfield(ci_state, -1, new_c_str("space").as_ptr());
        lua::lua_getfield(ci_state, -1, new_c_str("_space").as_ptr());
        lua::lua_getfield(ci_state, -1, new_c_str("get").as_ptr());
        lua::lua_pushvalue(ci_state, -2);
        lua::lua_pushinteger(ci_state, space_id as isize);
        if lua::luaT_call(ci_state, 2, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // -- space_name = space.name
        lua::lua_getfield(ci_state, -1, new_c_str("name").as_ptr());
        let space_name = lua::lua_tostring(ci_state, -1);
        lua::lua_remove(ci_state, -1);

        // -- box.space[space_name].create_index(name, opts)
        lua::lua_getglobal(ci_state, new_c_str("box").as_ptr());
        lua::lua_getfield(ci_state, -1, new_c_str("space").as_ptr());
        lua::lua_getfield(ci_state, -1, space_name);
        lua::lua_getfield(ci_state, -1, new_c_str("create_index").as_ptr());

        // Put args on the stack:

        // self
        lua::lua_pushvalue(ci_state, -2);

        // name
        lua::lua_pushstring(ci_state, new_c_str(index_name).as_ptr());

        // options
        lua::lua_newtable(ci_state);

        // opts.index_type
        if let Some(index_type) = opts.index_type {
            let index_type_str = match index_type {
                IndexType::Hash => "hash",
                IndexType::Tree => "tree",
                IndexType::Bitset => "bitset",
                IndexType::Rtree => "rtree",
            };
            lua::lua_pushstring(ci_state, new_c_str(index_type_str).as_ptr());
            lua::lua_setfield(ci_state, -2, new_c_str("type").as_ptr());
        }

        // opts.id
        if let Some(id) = opts.id {
            lua::lua_pushinteger(ci_state, id as isize);
            lua::lua_setfield(ci_state, -2, new_c_str("id").as_ptr());
        }

        // opts.unique
        if let Some(unique) = opts.unique {
            lua::lua_pushboolean(ci_state, unique as c_int);
            lua::lua_setfield(ci_state, -2, new_c_str("unique").as_ptr());
        }

        // opts.if_not_exists
        if let Some(if_not_exists) = opts.if_not_exists {
            lua::lua_pushboolean(ci_state, if_not_exists as c_int);
            lua::lua_setfield(ci_state, -2, new_c_str("if_not_exists").as_ptr());
        }

        // opts.parts
        if let Some(parts) = &opts.parts {
            lua::lua_newtable(ci_state);

            for (idx, p) in parts.iter().enumerate() {
                lua::lua_pushinteger(ci_state, (idx + 1) as isize);
                lua::lua_newtable(ci_state);

                // part.field
                lua::lua_pushinteger(ci_state, p.field_index as isize);
                lua::lua_setfield(ci_state, -2, new_c_str("field").as_ptr());

                // part.type
                let field_type = match p.field_type {
                    IndexFieldType::Unsigned => "unsigned",
                    IndexFieldType::String => "string",
                    IndexFieldType::Integer => "integer",
                    IndexFieldType::Number => "number",
                    IndexFieldType::Double => "double",
                    IndexFieldType::Decimal => "decimal",
                    IndexFieldType::Boolean => "boolean",
                    IndexFieldType::Varbinary => "varbinary",
                    IndexFieldType::Uuid => "uuid",
                    IndexFieldType::Array => "array",
                    IndexFieldType::Scalar => "scalar",
                };
                lua::lua_pushstring(ci_state, new_c_str(field_type).as_ptr());
                lua::lua_setfield(ci_state, -2, new_c_str("type").as_ptr());

                // part.collation
                if let Some(collation) = &p.collation {
                    lua::lua_pushstring(ci_state, new_c_str(collation).as_ptr());
                    lua::lua_setfield(ci_state, -2, new_c_str("collation").as_ptr());
                }

                // part.is_nullable
                if let Some(is_nullable) = &p.is_nullable {
                    lua::lua_pushboolean(ci_state, if *is_nullable { 1 } else { 0 });
                    lua::lua_setfield(ci_state, -2, new_c_str("is_nullable").as_ptr());
                }

                // part.path
                if let Some(path) = &p.path {
                    lua::lua_pushstring(ci_state, new_c_str(path).as_ptr());
                    lua::lua_setfield(ci_state, -2, new_c_str("path").as_ptr());
                }

                lua::lua_settable(ci_state, -3);
            }

            lua::lua_setfield(ci_state, -2, new_c_str("parts").as_ptr())
        }

        // opts.dimension
        if let Some(dimension) = opts.dimension {
            lua::lua_pushinteger(ci_state, dimension as isize);
            lua::lua_setfield(ci_state, -2, new_c_str("dimension").as_ptr());
        }

        // opts.distance
        if let Some(distance) = opts.distance {
            let distance_str = match distance {
                RtreeIndexDistanceType::Euclid => "euclid",
                RtreeIndexDistanceType::Manhattan => "manhattan",
            };
            lua::lua_pushstring(ci_state, new_c_str(distance_str).as_ptr());
            lua::lua_setfield(ci_state, -2, new_c_str("distance").as_ptr());
        }

        // opts.bloom_fpr
        if let Some(bloom_fpr) = opts.bloom_fpr {
            lua::lua_pushnumber(ci_state, bloom_fpr as f64);
            lua::lua_setfield(ci_state, -2, new_c_str("bloom_fpr").as_ptr());
        }

        // opts.page_size
        if let Some(page_size) = opts.page_size {
            lua::lua_pushinteger(ci_state, page_size as isize);
            lua::lua_setfield(ci_state, -2, new_c_str("page_size").as_ptr());
        }

        // opts.range_size
        if let Some(range_size) = opts.range_size {
            lua::lua_pushinteger(ci_state, range_size as isize);
            lua::lua_setfield(ci_state, -2, new_c_str("range_size").as_ptr());
        }

        // opts.run_count_per_level
        if let Some(run_count_per_level) = opts.run_count_per_level {
            lua::lua_pushinteger(ci_state, run_count_per_level as isize);
            lua::lua_setfield(ci_state, -2, new_c_str("run_count_per_level").as_ptr());
        }

        // opts.run_size_ratio
        if let Some(run_size_ratio) = opts.run_size_ratio {
            lua::lua_pushnumber(ci_state, run_size_ratio as f64);
            lua::lua_setfield(ci_state, -2, new_c_str("run_size_ratio").as_ptr());
        }

        // opts.sequence
        if let Some(sequence) = &opts.sequence {
            match sequence {
                // sequence = {id = sequence identifier , field = field number }
                IndexSequenceOption::SeqId {
                    seq_id,
                    field_index,
                } => {
                    lua::lua_newtable(ci_state);
                    lua::lua_pushinteger(ci_state, *seq_id as isize);
                    lua::lua_setfield(ci_state, -2, new_c_str("id").as_ptr());
                    if let Some(fi) = field_index {
                        lua::lua_pushinteger(ci_state, *fi as isize);
                        lua::lua_setfield(ci_state, -2, new_c_str("field").as_ptr());
                    }
                }
                // sequence = {id = sequence name , field = field number }
                IndexSequenceOption::SeqName {
                    seq_name,
                    field_index,
                } => {
                    lua::lua_newtable(ci_state);
                    lua::lua_pushstring(ci_state, new_c_str(seq_name).as_ptr());
                    lua::lua_setfield(ci_state, -2, new_c_str("id").as_ptr());
                    if let Some(fi) = field_index {
                        lua::lua_pushinteger(ci_state, *fi as isize);
                        lua::lua_setfield(ci_state, -2, new_c_str("field").as_ptr());
                    }
                }
                // sequence = true
                IndexSequenceOption::True => {
                    lua::lua_pushboolean(ci_state, true as c_int);
                }
                // sequence = {}
                IndexSequenceOption::Empty => {
                    lua::lua_newtable(ci_state);
                }
            }
            lua::lua_setfield(ci_state, -2, new_c_str("sequence").as_ptr());
        }

        // opts.func
        if let Some(func) = &opts.func {
            lua::lua_pushstring(ci_state, new_c_str(func).as_ptr());
            lua::lua_setfield(ci_state, -2, new_c_str("func").as_ptr());
        }

        // Only for Tarantool >= 2.6
        // opt.hint
        /* if let Some(hint) = opts.hint {
            ffi_lua::lua_pushboolean(ci_state, bool_as_int(hint));
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("hint").as_ptr());
        }
        */

        // Call space_object:create_index.
        if lua::luaT_call(ci_state, 3, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // No need to clean ci_state. It will be gc'ed.
    }

    Ok(())
}

/// Drop existing index.
///
/// - `space_id` - ID of existing space.
/// - `index_name` - ID of existing index.
pub fn drop_index(space_id: u32, index_id: u32) -> Result<(), Error> {
    unsafe {
        // Create new stack (just in case - in order no to mess things
        // in current stack).
        let state = lua::luaT_state();
        let drop_state = lua::lua_newthread(state);

        // Execute the following Lua code:
        // -- space = box.space._space:get(space_id)
        // -- space_name = space.name
        // -- index = box.space._index:get({space_id, index_id})
        // -- index_name = index.name
        // -- box.space.space_name.index.index_name:drop()

        // -- space = box.space._space:get({"id": space_id})
        lua::lua_getglobal(drop_state, new_c_str("box").as_ptr());
        lua::lua_getfield(drop_state, -1, new_c_str("space").as_ptr());
        lua::lua_getfield(drop_state, -1, new_c_str("_space").as_ptr());
        lua::lua_getfield(drop_state, -1, new_c_str("get").as_ptr());
        lua::lua_pushvalue(drop_state, -2);
        lua::lua_pushinteger(drop_state, space_id as isize);
        if lua::luaT_call(drop_state, 2, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // -- space_name = space.name
        lua::lua_getfield(drop_state, -1, new_c_str("name").as_ptr());
        let space_name = lua::lua_tostring(drop_state, -1);
        lua::lua_remove(drop_state, -1);

        // -- index = box.space._index:get({space_id, index_id})
        lua::lua_getglobal(drop_state, new_c_str("box").as_ptr());
        lua::lua_getfield(drop_state, -1, new_c_str("space").as_ptr());
        lua::lua_getfield(drop_state, -1, new_c_str("_index").as_ptr());
        lua::lua_getfield(drop_state, -1, new_c_str("get").as_ptr());
        lua::lua_pushvalue(drop_state, -2);
        lua::lua_newtable(drop_state);
        lua::lua_pushinteger(drop_state, 1);
        lua::lua_pushinteger(drop_state, space_id as isize);
        lua::lua_settable(drop_state, -3);
        lua::lua_pushinteger(drop_state, 2);
        lua::lua_pushinteger(drop_state, index_id as isize);
        lua::lua_settable(drop_state, -3);
        if lua::luaT_call(drop_state, 2, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // -- index_name = index.name
        lua::lua_getfield(drop_state, -1, new_c_str("name").as_ptr());
        let index_name = lua::lua_tostring(drop_state, -1);
        lua::lua_remove(drop_state, -1);

        // -- box.space.space_name.index.index_name:drop()
        lua::lua_getglobal(drop_state, new_c_str("box").as_ptr());
        lua::lua_getfield(drop_state, -1, new_c_str("space").as_ptr());
        lua::lua_getfield(drop_state, -1, space_name);
        lua::lua_getfield(drop_state, -1, new_c_str("index").as_ptr());
        lua::lua_getfield(drop_state, -1, index_name);
        lua::lua_getfield(drop_state, -1, new_c_str("drop").as_ptr());
        lua::lua_pushvalue(drop_state, -2);
        if lua::luaT_call(drop_state, 1, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // No need to clean drop_state. It will be gc'ed.
    }

    Ok(())
}
