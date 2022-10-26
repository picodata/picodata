//! Box: indices
//!
//! The `index` submodule provides access for index definitions and index keys.
//! They provide an API for ordered iteration over tuples.
//! This API is a direct binding to corresponding methods of index objects of type `box.index` in the storage engine.
//!
//! See also:
//! - [Indexes](https://www.tarantool.io/en/doc/latest/book/box/data_model/#indexes)
//! - [Lua reference: Submodule box.index](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_index/)
use std::os::raw::c_char;
use std::ptr::null_mut;
use std::mem::MaybeUninit;

use num_derive::ToPrimitive;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};

use crate::error::{Error, TarantoolError};
use crate::ffi::tarantool as ffi;
use crate::tuple::{ToTupleBuffer, Tuple, TupleBuffer};
use crate::tuple_from_box_api;
use crate::util::NumOrStr;
use crate::msgpack;

/// An index is a group of key values and pointers.
#[derive(Clone, Debug)]
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

////////////////////////////////////////////////////////////////////////////////
// Builder
////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub struct Builder<'a> {
    space_id: u32,
    name: &'a str,
    opts: IndexOptions,
}

macro_rules! define_setters {
    ($( $setter:ident ( $field:ident : $ty:ty ) )+) => {
        $(
            #[inline(always)]
            pub fn $setter(mut self, $field: $ty) -> Self {
                self.opts.$field = Some($field.into());
                self
            }
        )+
    }
}

impl<'a> Builder<'a> {
    /// Creates a new index builder with default options.
    #[inline(always)]
    pub fn new(space_id: u32, name: &'a str) -> Self {
        Self {
            space_id,
            name,
            opts: IndexOptions::default()
        }
    }

    define_setters!{
        index_type(r#type: IndexType)
        id(id: u32)
        unique(unique: bool)
        if_not_exists(if_not_exists: bool)
        dimension(dimension: u32)
        distance(distance: RtreeIndexDistanceType)
        bloom_fpr(bloom_fpr: f32)
        page_size(page_size: u32)
        range_size(range_size: u32)
        run_count_per_level(run_count_per_level: u32)
        run_size_ratio(run_size_ratio: f32)
        sequence(sequence: impl Into<SequenceOpt>)
        func(func: String)
    }

    /// Add a part to the index's parts list.
    ///
    /// Use this method to set each part individually or use [`parts`] to set
    /// parts in bulk. The difference is purely syntactical.
    ///
    /// [`parts`]: Self::parts
    #[inline(always)]
    pub fn part(mut self, part: impl Into<Part>) -> Self {
        self.opts.parts.get_or_insert_with(|| Vec::with_capacity(8))
            .push(part.into());
        self
    }

    /// Add parts to the index's parts list.
    ///
    /// Use this method to set parts in bulk or use [`part`] to set
    /// each part individually. The difference is purely syntactical.
    ///
    /// ```no_run
    /// use tarantool::{space::Space, index::FieldType as FT};
    ///
    /// Space::find("t").unwrap()
    ///     .index_builder("by_index_and_type")
    ///     .parts([(0, FT::Unsigned), (1, FT::String)])
    ///     .create();
    ///
    /// Space::find("t").unwrap()
    ///     .index_builder("by_name")
    ///     .parts(["foo", "bar", "baz"])
    ///     .create();
    /// ```
    ///
    /// [`part`]: Self::part
    #[inline(always)]
    pub fn parts(mut self, parts: impl IntoIterator<Item = impl Into<Part>>) -> Self {
        let iter = parts.into_iter();
        let (size, _) = iter.size_hint();
        self.opts.parts.get_or_insert_with(|| Vec::with_capacity(size))
            .extend(iter.map(Into::into));
        self
    }

    /// Create a new index using the current options.
    #[cfg(feature = "schema")]
    #[inline(always)]
    pub fn create(self) -> crate::Result<Index> {
        crate::schema::index::create_index(self.space_id, self.name, &self.opts)
    }

    /// Destructure the builder struct into a tuple of space_id, name and index
    /// options.
    #[inline(always)]
    pub fn into_parts(self) -> (u32, &'a str, IndexOptions) {
        (self.space_id, self.name, self.opts)
    }
}

////////////////////////////////////////////////////////////////////////////////
// IndexOptions
////////////////////////////////////////////////////////////////////////////////

/// List of options for new or updated index.
///
/// For details see [space_object:create_index - options](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/create_index/).
#[derive(Default, Serialize, tlua::Push)]
pub struct IndexOptions {
    pub r#type: Option<IndexType>,
    pub id: Option<u32>,
    pub unique: Option<bool>,
    pub if_not_exists: Option<bool>,
    pub parts: Option<Vec<Part>>,
    pub dimension: Option<u32>,
    pub distance: Option<RtreeIndexDistanceType>,
    pub bloom_fpr: Option<f32>,
    pub page_size: Option<u32>,
    pub range_size: Option<u32>,
    pub run_count_per_level: Option<u32>,
    pub run_size_ratio: Option<f32>,
    pub sequence: Option<SequenceOpt>,
    pub func: Option<String>,
    // Only for Tarantool >= 2.6
    // pub hint: Option<bool>,
}

////////////////////////////////////////////////////////////////////////////////
// SequenceOpt
////////////////////////////////////////////////////////////////////////////////

#[deprecated = "Use `index::SequenceOpt` instead"]
/// Use [`SequenceOpt`] instead
pub type IndexSequenceOption = SequenceOpt;

/// Sequence option for new or updated index.
///
/// For details see [specifying a sequence in create_index](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_schema_sequence/create_index/#box-schema-sequence-create-index).
#[derive(Serialize, tlua::Push)]
pub enum SequenceOpt {
    IdAndField(SeqSpec),
    AutoGenerated(bool),
}

impl SequenceOpt {
    #[inline(always)]
    pub fn auto() -> Self {
        Self::AutoGenerated(true)
    }

    #[inline(always)]
    pub fn none() -> Self {
        Self::AutoGenerated(false)
    }

    #[inline(always)]
    pub fn field(field: impl Into<NumOrStr>) -> Self {
        Self::IdAndField(SeqSpec::field(field))
    }

    #[inline(always)]
    pub fn id(id: impl Into<NumOrStr>) -> Self {
        Self::IdAndField(SeqSpec::id(id))
    }

    #[inline(always)]
    pub fn spec(s: SeqSpec) -> Self {
        Self::IdAndField(s)
    }
}

impl From<SeqSpec> for SequenceOpt {
    #[inline(always)]
    fn from(s: SeqSpec) -> Self {
        Self::spec(s)
    }
}

impl From<bool> for SequenceOpt {
    #[inline(always)]
    fn from(b: bool) -> Self {
        Self::AutoGenerated(b)
    }
}

#[derive(Serialize, tlua::Push)]
pub struct SeqSpec {
    id: Option<NumOrStr>,
    field: Option<NumOrStr>,
}

impl SeqSpec {
    #[inline(always)]
    pub fn field(field: impl Into<NumOrStr>) -> Self {
        Self {
            id: None,
            field: Some(field.into()),
        }
    }

    #[inline(always)]
    pub fn id(id: impl Into<NumOrStr>) -> Self {
        Self {
            id: Some(id.into()),
            field: None,
        }
    }

    #[inline(always)]
    pub fn and_field(mut self, field: impl Into<NumOrStr>) -> Self {
        self.field = Some(field.into());
        self
    }

    #[inline(always)]
    pub fn and_id(mut self, id: impl Into<NumOrStr>) -> Self {
        self.id = Some(id.into());
        self
    }
}

////////////////////////////////////////////////////////////////////////////////
// IndexType
////////////////////////////////////////////////////////////////////////////////

crate::define_str_enum!{
    /// Type of index.
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub enum IndexType {
        Hash = "hash",
        Tree = "tree",
        Bitset = "bitset",
        Rtree = "rtree",
    }

    FromStr::Err = UnknownIndexType;
}

#[derive(thiserror::Error, Debug)]
#[error("unknown index type {0}")]
pub struct UnknownIndexType(pub String);

////////////////////////////////////////////////////////////////////////////////
// FieldType
////////////////////////////////////////////////////////////////////////////////

#[deprecated = "use index::FieldType instead"]
pub type IndexFieldType = FieldType;

crate::define_str_enum!{
    /// Type of index part.
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub enum FieldType {
        Unsigned  = "unsigned",
        String    = "string",
        Number    = "number",
        Double    = "double",
        Integer   = "integer",
        Boolean   = "boolean",
        Varbinary = "varbinary",
        Scalar    = "scalar",
        Decimal   = "decimal",
        Uuid      = "uuid",
        Datetime  = "datetime",
        Array     = "array",
    }

    FromStr::Err = UnknownFieldType;
}

#[derive(thiserror::Error, Debug)]
#[error("unknown index field type {0}")]
pub struct UnknownFieldType(pub String);

////////////////////////////////////////////////////////////////////////////////
// IndexPart
////////////////////////////////////////////////////////////////////////////////

#[deprecated = "Use `index::Part` instead"]
pub type IndexPart = Part;

/// Index part.
#[derive(Clone, Debug, Serialize, Deserialize, tlua::Push)]
pub struct Part {
    pub field: NumOrStr,
    pub r#type: Option<FieldType>,
    pub collation: Option<String>,
    pub is_nullable: Option<bool>,
    pub path: Option<String>,
}

macro_rules! define_setters {
    ($( $setter:ident ( $field:ident : $ty:ty ) )+) => {
        $(
            #[inline(always)]
            pub fn $setter(mut self, $field: $ty) -> Self {
                self.$field = Some($field.into());
                self
            }
        )+
    }
}

impl Part {
    pub fn field(field: impl Into<NumOrStr>) -> Self {
        Self {
            field: field.into(),
            r#type: None,
            collation: None,
            is_nullable: None,
            path: None,
        }
    }

    define_setters!{
        field_type(r#type: FieldType)
        collation(collation: String)
        is_nullable(is_nullable: bool)
        path(path: String)
    }

    pub fn new(fi: impl Into<NumOrStr>, ft: FieldType) -> Self {
        Self::field(fi).field_type(ft)
    }
}

impl From<&str> for Part {
    #[inline(always)]
    fn from(f: &str) -> Self {
        Self::field(f.to_string())
    }
}

impl From<String> for Part {
    #[inline(always)]
    fn from(f: String) -> Self {
        Self::field(f)
    }
}

impl From<u32> for Part {
    #[inline(always)]
    fn from(f: u32) -> Self {
        Self::field(f)
    }
}

impl From<(u32, FieldType)> for Part {
    #[inline(always)]
    fn from((f, t): (u32, FieldType)) -> Self {
        Self::field(f).field_type(t)
    }
}

impl From<(String, FieldType)> for Part {
    #[inline(always)]
    fn from((f, t): (String, FieldType)) -> Self {
        Self::field(f).field_type(t)
    }
}

impl From<(&str, FieldType)> for Part {
    #[inline(always)]
    fn from((f, t): (&str, FieldType)) -> Self {
        Self::field(f.to_string()).field_type(t)
    }
}

////////////////////////////////////////////////////////////////////////////////
// ...
////////////////////////////////////////////////////////////////////////////////

crate::define_str_enum!{
    /// Type of distance for retree index.
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub enum RtreeIndexDistanceType {
        Euclid = "euclid",
        Manhattan = "manhattan",
    }

    FromStr::Err = UnknownRtreeDistanceType;
}

#[derive(thiserror::Error, Debug)]
#[error("unknown rtree distance type {0}")]
pub struct UnknownRtreeDistanceType(pub String);

impl Index {
    pub(crate) fn new(space_id: u32, index_id: u32) -> Self {
        Index { space_id, index_id }
    }

    // Drops index.
    #[cfg(feature = "schema")]
    pub fn drop(&self) -> Result<(), Error> {
        crate::schema::index::drop_index(self.space_id, self.index_id)
    }

    /// Get a tuple from index by the key.
    ///
    /// Please note that this function works much faster than [select](#method.select)
    ///
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    ///
    /// Returns a tuple or `None` if index is empty
    pub fn get<K>(&self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer,
    {
        let key_buf = key.to_tuple_buffer().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        tuple_from_box_api!(
            ffi::box_index_get[
                self.space_id,
                self.index_id,
                key_buf_ptr,
                key_buf_ptr.add(key_buf.len()),
                @out
            ]
        )
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
        K: ToTupleBuffer,
    {
        let key_buf = key.to_tuple_buffer().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;

        let ptr = unsafe {
            ffi::box_index_iterator(
                self.space_id,
                self.index_id,
                iterator_type.to_i32().unwrap(),
                key_buf_ptr,
                key_buf_ptr.add(key_buf.len()),
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
    /// Same as [space.delete()](../space/struct.Space.html#method.delete), but a key is searched in this index instead
    /// of in the primary-key index. This index ought to be unique.
    ///
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    ///
    /// Returns the deleted tuple
    pub fn delete<K>(&self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer,
    {
        let key_buf = key.to_tuple_buffer().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        tuple_from_box_api!(
            ffi::box_delete[
                self.space_id,
                self.index_id,
                key_buf_ptr,
                key_buf_ptr.add(key_buf.len()),
                @out
            ]
        )
    }

    /// Update a tuple.
    ///
    /// Same as [space.update()](../space/struct.Space.html#method.update), but a key is searched in this index instead
    /// of primary key. This index ought to be unique.
    ///
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    /// - `ops` - encoded operations in MsgPack array format, e.g. `[['=', field_id, value], ['!', 2, 'xxx']]`
    ///
    /// Returns a new tuple.
    ///
    /// See also: [index.upsert()](#method.upsert)
    // TODO(gmoshkin): accept a single Ops argument instead of a slice of ops
    #[inline]
    pub fn update<K, Op>(&self, key: &K, ops: impl AsRef<[Op]>) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer,
        Op: ToTupleBuffer,
    {
        let key_buf = key.to_tuple_buffer().unwrap();
        let mut ops_buf = Vec::with_capacity(4 + ops.as_ref().len() * 4);
        msgpack::write_array(&mut ops_buf, ops.as_ref())?;
        unsafe {
            self.update_raw(key_buf.as_ref(), ops_buf.as_ref())
        }
    }

    /// # Safety
    /// `ops` must be a slice of valid msgpack arrays.
    #[deprecated = "use update_raw instead"]
    pub unsafe fn update_mp<K>(&self, key: &K, ops: &[Vec<u8>]) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer,
    {
        let key_buf = key.to_tuple_buffer().unwrap();
        let mut buf = Vec::with_capacity(128);
        msgpack::write_array(&mut buf, ops)?;
        self.update_raw(key_buf.as_ref(), buf.as_ref())
    }

    /// # Safety
    /// `key` must be a valid msgpack array.
    /// `ops` must be a valid msgpack array of msgpack arrays.
    pub unsafe fn update_raw(&self, key: &[u8], ops: &[u8]) -> Result<Option<Tuple>, Error> {
        let key = key.as_ptr_range();
        let ops = ops.as_ptr_range();
        tuple_from_box_api!(
            ffi::box_update[
                self.space_id,
                self.index_id,
                key.start.cast(), key.end.cast(),
                ops.start.cast(), ops.end.cast(),
                0,
                @out
            ]
        )
    }

    /// Execute an UPSERT request.
    ///
    /// Will try to insert tuple. Update if already exists.
    ///
    /// - `value` - encoded tuple in MsgPack Array format (`[field1, field2, ...]`)
    /// - `ops` - encoded operations in MsgPack array format, e.g. `[['=', field_id, value], ['!', 2, 'xxx']]`
    ///
    /// See also: [index.update()](#method.update)
    #[inline]
    pub fn upsert<T, Op>(&self, value: &T, ops: impl AsRef<[Op]>) -> Result<(), Error>
    where
        T: ToTupleBuffer,
        Op: ToTupleBuffer,
    {
        let value_buf = value.to_tuple_buffer().unwrap();
        let mut ops_buf = Vec::with_capacity(4 + ops.as_ref().len() * 4);
        msgpack::write_array(&mut ops_buf, ops.as_ref())?;
        unsafe {
            self.upsert_raw(value_buf.as_ref(), ops_buf.as_ref())
        }
    }

    /// # Safety
    /// `ops` must be a slice of valid msgpack arrays.
    #[deprecated = "use upsert_raw instead"]
    pub unsafe fn upsert_mp<T>(&self, value: &T, ops: &[Vec<u8>]) -> Result<(), Error>
    where
        T: ToTupleBuffer,
    {
        let value_buf = value.to_tuple_buffer().unwrap();
        let mut buf = Vec::with_capacity(128);
        msgpack::write_array(&mut buf, ops)?;
        self.upsert_raw(value_buf.as_ref(), buf.as_ref())
    }

    /// # Safety
    /// `value` must be a valid msgpack array.
    /// `ops` must be a valid msgpack array of msgpack arrays.
    pub unsafe fn upsert_raw(&self, value: &[u8], ops: &[u8]) -> Result<(), Error> {
        let value = value.as_ptr_range();
        let ops = ops.as_ptr_range();
        tuple_from_box_api!(
            ffi::box_upsert[
                self.space_id,
                self.index_id,
                value.start.cast(), value.end.cast(),
                ops.start.cast(), ops.end.cast(),
                0,
                @out
            ]
        ).map(|t| if t.is_some() {
            unreachable!("Upsert doesn't return a tuple")
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

    #[inline(always)]
    pub fn is_empty(&self) -> Result<bool, Error> {
        self.len().map(|l| l == 0)
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
        tuple_from_box_api!(
            ffi::box_index_random[
                self.space_id,
                self.index_id,
                seed,
                @out
            ]
        )
    }

    /// Return a first (minimal) tuple that matched the provided key.
    ///
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    ///
    /// Returns a tuple or `None` if index is empty
    pub fn min<K>(&self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer,
    {
        let key_buf = key.to_tuple_buffer().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        tuple_from_box_api!(
            ffi::box_index_min[
                self.space_id,
                self.index_id,
                key_buf_ptr,
                key_buf_ptr.add(key_buf.len()),
                @out
            ]
        )
    }

    /// Return a last (maximal) tuple that matched the provided key.
    ///
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    ///
    /// Returns a tuple or `None` if index is empty
    pub fn max<K>(&self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer,
    {
        let key_buf = key.to_tuple_buffer().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;
        tuple_from_box_api!(
            ffi::box_index_max[
                self.space_id,
                self.index_id,
                key_buf_ptr,
                key_buf_ptr.add(key_buf.len()),
                @out
            ]
        )
    }

    /// Count the number of tuples that matched the provided key.
    ///
    /// - `type` - iterator type
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    pub fn count<K>(&self, iterator_type: IteratorType, key: &K) -> Result<usize, Error>
    where
        K: ToTupleBuffer,
    {
        let key_buf = key.to_tuple_buffer().unwrap();
        let key_buf_ptr = key_buf.as_ptr() as *const c_char;

        let result = unsafe {
            ffi::box_index_count(
                self.space_id,
                self.index_id,
                iterator_type.to_i32().unwrap(),
                key_buf_ptr,
                key_buf_ptr.add(key_buf.len()),
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
        unsafe {
            let mut result_size = MaybeUninit::uninit();
            let result_ptr = ffi::box_tuple_extract_key(
                tuple.into_ptr(),
                self.space_id,
                self.index_id,
                result_size.as_mut_ptr(),
            );
            Tuple::from_raw_data(result_ptr, result_size.assume_init())
        }
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
        let mut result_ptr = null_mut();
        if unsafe { ffi::box_iterator_next(self.ptr, &mut result_ptr) } < 0 {
            return None;
        }
        Tuple::try_from_ptr(result_ptr)
    }
}

impl Drop for IndexIterator {
    fn drop(&mut self) {
        unsafe { ffi::box_iterator_free(self.ptr) };
    }
}
