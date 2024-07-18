//! Box: indices
//!
//! The `index` submodule provides access for index definitions and index keys.
//! They provide an API for ordered iteration over tuples.
//! This API is a direct binding to corresponding methods of index objects of type `box.index` in the storage engine.
//!
//! See also:
//! - [Indexes](https://www.tarantool.io/en/doc/latest/book/box/data_model/#indexes)
//! - [Lua reference: Submodule box.index](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_index/)
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::ptr::null_mut;

use serde::{Deserialize, Serialize};

use crate::error::{Error, TarantoolError, TarantoolErrorCode};
use crate::ffi::tarantool as ffi;
use crate::msgpack;
use crate::space::{Space, SpaceId, SystemSpace};
use crate::tuple::{Encode, ToTupleBuffer, Tuple, TupleBuffer};
use crate::tuple::{KeyDef, KeyDefPart};
use crate::tuple_from_box_api;
use crate::unwrap_or;
use crate::util::NumOrStr;
use crate::util::Value;

pub type IndexId = u32;

/// An index is a group of key values and pointers.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Index {
    space_id: SpaceId,
    index_id: IndexId,
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
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
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
    space_id: SpaceId,
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
    pub fn new(space_id: SpaceId, name: &'a str) -> Self {
        Self {
            space_id,
            name,
            opts: IndexOptions::default(),
        }
    }

    define_setters! {
        index_type(r#type: IndexType)
        id(id: SpaceId)
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
        self.opts
            .parts
            .get_or_insert_with(|| Vec::with_capacity(8))
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
        self.opts
            .parts
            .get_or_insert_with(|| Vec::with_capacity(size))
            .extend(iter.map(Into::into));
        self
    }

    /// Create a new index using the current options.
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
#[derive(Clone, Debug, Default, Serialize, tlua::Push, tlua::LuaRead, PartialEq)]
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
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, tlua::Push, tlua::LuaRead, Hash)]
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, tlua::Push, tlua::LuaRead, Hash)]
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

crate::define_str_enum! {
    #![coerce_from_str]
    /// Type of index.
    pub enum IndexType {
        Hash = "hash",
        Tree = "tree",
        Bitset = "bitset",
        Rtree = "rtree",
    }
}

impl Default for IndexType {
    #[inline(always)]
    fn default() -> Self {
        Self::Tree
    }
}

////////////////////////////////////////////////////////////////////////////////
// FieldType
////////////////////////////////////////////////////////////////////////////////

#[deprecated = "use index::FieldType instead"]
pub type IndexFieldType = FieldType;

crate::define_str_enum! {
    #![coerce_from_str]
    /// Type of index part.
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
}

////////////////////////////////////////////////////////////////////////////////
// IndexPart
////////////////////////////////////////////////////////////////////////////////

#[deprecated = "Use `index::Part` instead"]
pub type IndexPart = Part;

/// Index part.
#[derive(
    Clone, Default, Debug, Serialize, Deserialize, tlua::Push, tlua::LuaRead, PartialEq, Eq,
)]
pub struct Part {
    pub field: NumOrStr,
    #[serde(default)]
    pub r#type: Option<FieldType>,
    #[serde(default)]
    pub collation: Option<String>,
    #[serde(default)]
    pub is_nullable: Option<bool>,
    #[serde(default)]
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
    #[inline(always)]
    pub fn field(field: impl Into<NumOrStr>) -> Self {
        Self {
            field: field.into(),
            r#type: None,
            collation: None,
            is_nullable: None,
            path: None,
        }
    }

    define_setters! {
        field_type(r#type: FieldType)
        collation(collation: impl Into<String>)
        is_nullable(is_nullable: bool)
        path(path: impl Into<String>)
    }

    #[inline(always)]
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

crate::define_str_enum! {
    #![coerce_from_str]
    /// Type of distance for retree index.
    pub enum RtreeIndexDistanceType {
        Euclid = "euclid",
        Manhattan = "manhattan",
    }
}

impl Index {
    #[inline(always)]
    pub(crate) fn new(space_id: SpaceId, index_id: IndexId) -> Self {
        Index { space_id, index_id }
    }

    /// Create an `Index` with corresponding space and index `id`s.
    ///
    /// # Safety
    /// `id`s must be valid tarantool space/index id. Only use this function with
    /// ids acquired from tarantool in some way, e.g. from lua code.
    #[inline(always)]
    pub const unsafe fn from_ids_unchecked(space_id: SpaceId, index_id: IndexId) -> Self {
        Self { space_id, index_id }
    }

    /// Return id of this index.
    #[inline(always)]
    pub fn id(&self) -> u32 {
        self.index_id
    }

    /// Return the space id of this index.
    #[inline(always)]
    pub fn space_id(&self) -> u32 {
        self.space_id
    }

    // Return index metadata from system `_index` space.
    #[inline]
    pub fn meta(&self) -> Result<Metadata, Error> {
        let sys_space: Space = SystemSpace::Index.into();
        let tuple = sys_space.get(&[self.space_id, self.index_id])?;
        let Some(tuple) = tuple else {
            return Err(crate::error::BoxError::new(
                TarantoolErrorCode::NoSuchIndexID,
                format!(
                    "index #{} for space #{} not found",
                    self.index_id, self.space_id,
                ),
            )
            .into());
        };
        tuple.decode::<Metadata>()
    }

    // Drops index.
    #[inline(always)]
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
    #[inline]
    pub fn get<K>(&self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer + ?Sized,
    {
        let buf;
        let data = unwrap_or!(key.tuple_data(), {
            // TODO: use region allocation for this
            buf = key.to_tuple_buffer()?;
            buf.as_ref()
        });
        let Range { start, end } = data.as_ptr_range();
        tuple_from_box_api!(
            ffi::box_index_get[
                self.space_id,
                self.index_id,
                start as _,
                end as _,
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
    #[inline]
    pub fn select<K>(&self, iterator_type: IteratorType, key: &K) -> Result<IndexIterator, Error>
    where
        K: ToTupleBuffer + ?Sized,
    {
        let key_buf = key.to_tuple_buffer().unwrap();
        let Range { start, end } = key_buf.as_ref().as_ptr_range();

        let ptr = unsafe {
            ffi::box_index_iterator(
                self.space_id,
                self.index_id,
                iterator_type as _,
                start as _,
                end as _,
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
    /// Returns the deleted tuple or `Ok(None)` if tuple was not found.
    #[inline]
    pub fn delete<K>(&self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer + ?Sized,
    {
        let buf;
        let data = unwrap_or!(key.tuple_data(), {
            // TODO: use region allocation for this
            buf = key.to_tuple_buffer()?;
            buf.as_ref()
        });
        let Range { start, end } = data.as_ptr_range();
        tuple_from_box_api!(
            ffi::box_delete[
                self.space_id,
                self.index_id,
                start as _,
                end as _,
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
        K: ToTupleBuffer + ?Sized,
        Op: ToTupleBuffer,
    {
        let key_buf;
        let key_data = unwrap_or!(key.tuple_data(), {
            // TODO: use region allocation for this
            key_buf = key.to_tuple_buffer()?;
            key_buf.as_ref()
        });
        let mut ops_buf = Vec::with_capacity(4 + ops.as_ref().len() * 4);
        msgpack::write_array(&mut ops_buf, ops.as_ref())?;
        unsafe { self.update_raw(key_data, ops_buf.as_ref()) }
    }

    /// # Safety
    /// `ops` must be a slice of valid msgpack arrays.
    #[deprecated = "use update_raw instead"]
    pub unsafe fn update_mp<K>(&self, key: &K, ops: &[Vec<u8>]) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer + ?Sized,
    {
        let key_buf;
        let key_data = unwrap_or!(key.tuple_data(), {
            // TODO: use region allocation for this
            key_buf = key.to_tuple_buffer()?;
            key_buf.as_ref()
        });
        let mut ops_buf = Vec::with_capacity(128);
        msgpack::write_array(&mut ops_buf, ops)?;
        self.update_raw(key_data, ops_buf.as_ref())
    }

    /// # Safety
    /// `key` must be a valid msgpack array.
    /// `ops` must be a valid msgpack array of msgpack arrays.
    #[inline(always)]
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
        T: ToTupleBuffer + ?Sized,
        Op: ToTupleBuffer,
    {
        let value_buf;
        let value_data = unwrap_or!(value.tuple_data(), {
            // TODO: use region allocation for this
            value_buf = value.to_tuple_buffer()?;
            value_buf.as_ref()
        });
        let mut ops_buf = Vec::with_capacity(4 + ops.as_ref().len() * 4);
        msgpack::write_array(&mut ops_buf, ops.as_ref())?;
        unsafe { self.upsert_raw(value_data, ops_buf.as_ref()) }
    }

    /// # Safety
    /// `ops` must be a slice of valid msgpack arrays.
    #[deprecated = "use upsert_raw instead"]
    pub unsafe fn upsert_mp<T>(&self, value: &T, ops: &[Vec<u8>]) -> Result<(), Error>
    where
        T: ToTupleBuffer + ?Sized,
    {
        let value_buf;
        let value_data = unwrap_or!(value.tuple_data(), {
            // TODO: use region allocation for this
            value_buf = value.to_tuple_buffer()?;
            value_buf.as_ref()
        });
        let mut ops_buf = Vec::with_capacity(128);
        msgpack::write_array(&mut ops_buf, ops)?;
        self.upsert_raw(value_data, ops_buf.as_ref())
    }

    /// # Safety
    /// `value` must be a valid msgpack array.
    /// `ops` must be a valid msgpack array of msgpack arrays.
    #[inline(always)]
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
        )
        .map(|t| {
            if t.is_some() {
                unreachable!("Upsert doesn't return a tuple")
            }
        })
    }

    /// Return the number of elements in the index.
    #[inline(always)]
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
    #[inline(always)]
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
    #[inline(always)]
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
    #[inline]
    pub fn min<K>(&self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer + ?Sized,
    {
        let buf;
        let data = unwrap_or!(key.tuple_data(), {
            // TODO: use region allocation for this
            buf = key.to_tuple_buffer()?;
            buf.as_ref()
        });
        let Range { start, end } = data.as_ptr_range();
        tuple_from_box_api!(
            ffi::box_index_min[
                self.space_id,
                self.index_id,
                start as _,
                end as _,
                @out
            ]
        )
    }

    /// Return a last (maximal) tuple that matched the provided key.
    ///
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    ///
    /// Returns a tuple or `None` if index is empty
    #[inline]
    pub fn max<K>(&self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer + ?Sized,
    {
        let buf;
        let data = unwrap_or!(key.tuple_data(), {
            // TODO: use region allocation for this
            buf = key.to_tuple_buffer()?;
            buf.as_ref()
        });
        let Range { start, end } = data.as_ptr_range();
        tuple_from_box_api!(
            ffi::box_index_max[
                self.space_id,
                self.index_id,
                start as _,
                end as _,
                @out
            ]
        )
    }

    /// Count the number of tuples that matched the provided key.
    ///
    /// - `type` - iterator type
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    #[inline]
    pub fn count<K>(&self, iterator_type: IteratorType, key: &K) -> Result<usize, Error>
    where
        K: ToTupleBuffer + ?Sized,
    {
        let buf;
        let data = unwrap_or!(key.tuple_data(), {
            // TODO: use region allocation for this
            buf = key.to_tuple_buffer()?;
            buf.as_ref()
        });
        let Range { start, end } = data.as_ptr_range();
        let result = unsafe {
            ffi::box_index_count(
                self.space_id,
                self.index_id,
                iterator_type as _,
                start as _,
                end as _,
            )
        };

        if result < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(result as usize)
        }
    }

    /// Extract key from `tuple` according to key definition of given
    /// index.
    ///
    /// # Safety
    /// The current index & it's space must exist and `tuple` must conform to
    /// the space format.
    ///
    /// You should probably use [`KeyDef::extract_key`] instead.
    #[inline(always)]
    pub unsafe fn extract_key(&self, tuple: Tuple) -> Tuple {
        unsafe {
            let mut result_size = MaybeUninit::uninit();
            let result_ptr = ffi::box_tuple_extract_key(
                tuple.as_ptr(),
                self.space_id,
                self.index_id,
                result_size.as_mut_ptr(),
            );
            Tuple::from_raw_data(result_ptr, result_size.assume_init())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Metadata
////////////////////////////////////////////////////////////////////////////////

/// Representation of a tuple holding index metadata in system `_index` space.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct Metadata<'a> {
    pub space_id: SpaceId,
    pub index_id: IndexId,
    pub name: Cow<'a, str>,
    pub r#type: IndexType,
    pub opts: BTreeMap<Cow<'a, str>, Value<'a>>,
    pub parts: Vec<Part>,
}
impl Encode for Metadata<'_> {}

#[derive(thiserror::Error, Debug)]
#[error("field number expected, got string '{0}'")]
pub struct FieldMustBeNumber(pub String);

impl Metadata<'_> {
    /// Construct a [`KeyDef`] instance from index parts.
    ///
    /// # Panicking
    /// Will panic if any of the parts have field name instead of field number.
    /// Normally this doesn't happen, because `Metadata` returned from
    /// `_index` always has field number, but if you got this metadata from
    /// somewhere else, use [`Self::try_to_key_def`] instead, to check for this
    /// error.
    #[inline(always)]
    pub fn to_key_def(&self) -> KeyDef {
        // TODO: we could optimize by caching these things and only recreating
        // then once box_schema_version changes.
        self.try_to_key_def().unwrap()
    }

    /// Construct a [`KeyDef`] instance from index parts. Returns error in case
    /// any of the parts had field name instead of field number.
    #[inline]
    pub fn try_to_key_def(&self) -> Result<KeyDef, FieldMustBeNumber> {
        let mut kd_parts = Vec::with_capacity(self.parts.len());
        for p in &self.parts {
            let kd_p = KeyDefPart::try_from_index_part(p)
                .ok_or_else(|| FieldMustBeNumber(p.field.clone().into()))?;
            kd_parts.push(kd_p);
        }
        Ok(KeyDef::new(&kd_parts).unwrap())
    }

    /// Construct a [`KeyDef`] instance from index parts for comparing keys only.
    ///
    /// The difference between this function and [`Self::to_key_def`] is that
    /// the latter is used to compare tuples of a space, while the former is
    /// used to compare only the keys.
    #[inline]
    pub fn to_key_def_for_key(&self) -> KeyDef {
        let mut kd_parts = Vec::with_capacity(self.parts.len());
        for (p, i) in self.parts.iter().zip(0..) {
            let collation = p.collation.as_deref().map(|s| {
                std::ffi::CString::new(s)
                    .expect("it's your fault if you put '\0' in collation")
                    .into()
            });
            let kd_p = KeyDefPart {
                // `p.field_no` is the location of the key part in the original tuple,
                // but here we only care about the location of the part in the key itself
                field_no: i,
                field_type: p.r#type.map(From::from).unwrap_or_default(),
                collation,
                is_nullable: p.is_nullable.unwrap_or(false),
                // `p.path` describes the location of the key part in the original tuple,
                // but in the key the part will be placed at the top level,
                // hence path is always empty
                path: None,
            };
            kd_parts.push(kd_p);
        }
        KeyDef::new(&kd_parts).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////
// IndexIterator
////////////////////////////////////////////////////////////////////////////////

/// Index iterator. Can be used with `for` statement.
pub struct IndexIterator {
    ptr: *mut ffi::BoxIterator,
    _key_data: TupleBuffer,
}

impl Iterator for IndexIterator {
    type Item = Tuple;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let mut result_ptr = null_mut();
        if unsafe { ffi::box_iterator_next(self.ptr, &mut result_ptr) } < 0 {
            return None;
        }
        Tuple::try_from_ptr(result_ptr)
    }
}

impl Drop for IndexIterator {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe { ffi::box_iterator_free(self.ptr) };
    }
}

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;
    use crate::space;

    #[crate::test(tarantool = "crate")]
    fn index_metadata() {
        let space = Space::builder("test_index_metadata_space")
            .field(("id", space::FieldType::Unsigned))
            .field(("s", space::FieldType::String))
            .field(("map", space::FieldType::Map))
            .create()
            .unwrap();

        let index = space
            .index_builder("pk")
            .index_type(IndexType::Hash)
            .create()
            .unwrap();
        let meta = index.meta().unwrap();
        assert_eq!(
            meta,
            Metadata {
                space_id: space.id(),
                index_id: 0,
                name: "pk".into(),
                r#type: IndexType::Hash,
                opts: BTreeMap::from([("unique".into(), Value::from(true)),]),
                parts: vec![Part {
                    field: 0.into(),
                    r#type: Some(FieldType::Unsigned),
                    ..Default::default()
                }],
            }
        );

        let index = space
            .index_builder("i")
            .unique(false)
            .index_type(IndexType::Tree)
            .part(("s", FieldType::String))
            .part(Part {
                field: NumOrStr::Str("map.key".into()),
                r#type: Some(FieldType::Unsigned),
                is_nullable: Some(true),
                ..Default::default()
            })
            .part(("map.value[1]", FieldType::String))
            .create()
            .unwrap();
        let meta = index.meta().unwrap();
        assert_eq!(
            meta,
            Metadata {
                space_id: space.id(),
                index_id: 1,
                name: "i".into(),
                r#type: IndexType::Tree,
                opts: BTreeMap::from([("unique".into(), Value::from(false)),]),
                parts: vec![
                    Part {
                        field: 1.into(),
                        r#type: Some(FieldType::String),
                        ..Default::default()
                    },
                    Part {
                        field: 2.into(),
                        r#type: Some(FieldType::Unsigned),
                        is_nullable: Some(true),
                        path: Some(".key".into()),
                        ..Default::default()
                    },
                    Part {
                        field: 2.into(),
                        r#type: Some(FieldType::String),
                        path: Some(".value[1]".into()),
                        ..Default::default()
                    },
                ],
            }
        );

        space.drop().unwrap();
    }

    #[crate::test(tarantool = "crate")]
    fn key_def_for_key() {
        let space = Space::builder("test_key_def_for_keys_space")
            .field(("id", space::FieldType::Unsigned))
            .field(("s", space::FieldType::String))
            .field(("map", space::FieldType::Map))
            .create()
            .unwrap();

        space.index_builder("pk").create().unwrap();

        let index = space
            .index_builder("i")
            .unique(false)
            .part(("map.arr[1]", FieldType::String))
            .part(("map.val", FieldType::Unsigned))
            .part(("s", FieldType::String))
            .part(("id", FieldType::Unsigned))
            .create()
            .unwrap();
        let key_def = index.meta().unwrap().to_key_def_for_key();

        assert!(key_def
            .compare_with_key(
                &Tuple::new(&("foo", 13, "bar", 37)).unwrap(),
                &("foo", 13, "bar", 37),
            )
            .is_eq());

        assert!(key_def
            .compare_with_key(
                &Tuple::new(&("foo", 13, "bar", 37)).unwrap(),
                &("foo", 14, "bar", 37),
            )
            .is_lt());

        assert!(key_def
            .compare_with_key(
                &Tuple::new(&("foo", 13, "baz", 37)).unwrap(),
                &("foo", 13, "bar", 37),
            )
            .is_gt());

        space.drop().unwrap();
    }

    #[crate::test(tarantool = "crate")]
    fn sys_index_metadata() {
        let sys_index = Space::from(SystemSpace::Index);
        for tuple in sys_index.select(IteratorType::All, &()).unwrap() {
            // Check index metadata is deserializable from what is actually in _index
            let _meta: Metadata = tuple.decode().unwrap();
        }
    }
}
