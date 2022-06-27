//! Box: spaces
//!
//! **CRUD operations** in Tarantool are implemented by the box.space submodule.
//! It has the data-manipulation functions select, insert, replace, update, upsert, delete, get, put.
//!
//! See also:
//! - [Lua reference: Submodule box.space](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/)
//! - [C API reference: Module box](https://www.tarantool.io/en/doc/latest/dev_guide/reference_capi/box/)
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::os::raw::c_char;

use num_derive::ToPrimitive;
use num_traits::ToPrimitive;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

use crate::error::{Error, TarantoolError};
use crate::ffi::tarantool as ffi;
use crate::index::{self, Index, IndexIterator, IteratorType};
use crate::schema::space::SpaceMetadata;
use crate::tuple::{AsTuple, Tuple};
use crate::tuple_from_box_api;

/// End of the reserved range of system spaces.
pub const SYSTEM_ID_MAX: u32 = 511;

/// Provides access to system spaces
///
/// Example:
/// ```rust
/// use tarantool::space::SystemSpace;
/// use num_traits::ToPrimitive;
/// assert_eq!(SystemSpace::Schema.to_u32(), Some(272))
/// ```
#[repr(u32)]
#[derive(Clone, Debug, PartialEq, Eq, ToPrimitive)]
pub enum SystemSpace {
    /// Space if of _vinyl_deferred_delete.
    VinylDeferredDelete = 257,
    /// Space id of _schema.
    Schema = 272,
    /// Space id of _collation.
    Collation = 276,
    /// Space id of _vcollation.
    VCollation = 277,
    /// Space id of _space.
    Space = 280,
    /// Space id of _vspace view.
    VSpace = 281,
    /// Space id of _sequence.
    Sequence = 284,
    /// Space id of _sequence_data.
    SequenceData = 285,
    /// Space id of _vsequence view.
    VSequence = 286,
    /// Space id of _index.
    Index = 288,
    /// Space id of _vindex view.
    VIndex = 289,
    /// Space id of _func.
    Func = 296,
    /// Space id of _vfunc view.
    VFunc = 297,
    /// Space id of _user.
    User = 304,
    /// Space id of _vuser view.
    VUser = 305,
    /// Space id of _priv.
    Priv = 312,
    /// Space id of _vpriv view.
    VPriv = 313,
    /// Space id of _cluster.
    Cluster = 320,
    /// Space id of _trigger.
    Trigger = 328,
    /// Space id of _truncate.
    Truncate = 330,
    /// Space id of _space_sequence.
    SpaceSequence = 340,
    /// Space id of _fk_constraint.
    FkConstraint = 356,
    /// Space id of _ck_contraint.
    CkConstraint = 364,
    /// Space id of _func_index.
    FuncIndex = 372,
    /// Space id of _session_settings.
    SessionSettings = 380,
}

impl From<SystemSpace> for Space {
    fn from(ss: SystemSpace) -> Self {
        Space {
            id: ss.to_u32().unwrap(),
        }
    }
}

/// Type of engine, used by space.
#[derive(Copy, Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SpaceEngineType {
    Memtx,
    Vinyl,
}

impl<'de> Deserialize<'de> for SpaceEngineType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str = String::deserialize(deserializer)?.trim().to_lowercase();

        const MEMTX: &str = "memtx";
        const VINYL: &str = "vinyl";

        Ok(match str.as_str() {
            MEMTX => Self::Memtx,
            VINYL => Self::Vinyl,
            _ => {
                return Err(serde::de::Error::unknown_variant(
                    &str,
                    &[MEMTX, VINYL],
                ));
            }
        })
    }
}

/// Options for new space, used by Space::create.
/// (for details see [Options for box.schema.space.create](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_schema/space_create/)).
///
/// `format` option is not supported at this moment.
#[derive(Clone, Debug, Serialize)]
pub struct SpaceCreateOptions {
    pub if_not_exists: bool,
    pub engine: SpaceEngineType,
    pub id: Option<u32>,
    pub field_count: u32,
    pub user: Option<String>,
    pub is_local: bool,
    pub is_temporary: bool,
    pub is_sync: bool,
    pub format: Option<Vec<Field>>,
}

impl Default for SpaceCreateOptions {
    fn default() -> Self {
        SpaceCreateOptions {
            if_not_exists: false,
            engine: SpaceEngineType::Memtx,
            id: None,
            field_count: 0,
            user: None,
            is_local: true,
            is_temporary: true,
            is_sync: false,
            format: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Field
////////////////////////////////////////////////////////////////////////////////

#[deprecated = "Use `space::Field` instead"]
pub type SpaceFieldFormat = Field;

#[derive(Clone, Debug, Serialize)]
pub struct Field {
    pub name: String, // TODO(gmoshkin): &str
    #[serde(alias = "type")]
    pub field_type: SpaceFieldType,
    pub is_nullable: bool,
}

macro_rules! define_constructors {
    ($($constructor:ident ($type:path))+) => {
        $(
            #[doc = ::std::concat!(
                "Create a new field format specifier with the given `name` and ",
                "type \"", ::std::stringify!($constructor), "\""
            )]
            pub fn $constructor(name: &str) -> Self {
                Self {
                    name: name.into(),
                    field_type: $type,
                    is_nullable: false,
                }
            }
        )+
    }
}

impl Field {
    #[deprecated = "Use one of `Field::any`, `Field::unsigned`, `Field::string`, etc. instead"]
    /// Create a new field format specifier.
    ///
    /// You should use one of the other constructors instead
    pub fn new(name: &str, ft: SpaceFieldType) -> Self {
        Self {
            name: name.to_string(),
            field_type: ft,
            is_nullable: false,
        }
    }

    /// Specify if the current field can be nullable or not. This method
    /// captures `self` by value and returns it, so it should be used in a
    /// builder fashion.
    /// ```rust
    /// use tarantool::space::Field;
    /// let f = Field::string("middle name").is_nullable(true);
    /// ```
    pub fn is_nullable(mut self, is_nullable: bool) -> Self {
        self.is_nullable = is_nullable;
        self
    }

    define_constructors!{
        any(SpaceFieldType::Any)
        unsigned(SpaceFieldType::Unsigned)
        string(SpaceFieldType::String)
        number(SpaceFieldType::Number)
        double(SpaceFieldType::Double)
        integer(SpaceFieldType::Integer)
        boolean(SpaceFieldType::Boolean)
        decimal(SpaceFieldType::Decimal)
        uuid(SpaceFieldType::Uuid)
        array(SpaceFieldType::Array)
        scalar(SpaceFieldType::Scalar)
    }
}

#[derive(Copy, Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SpaceFieldType {
    Any,
    Unsigned,
    String,
    Number,
    Double,
    Integer,
    Boolean,
    Decimal,
    Uuid,
    Array,
    Scalar,
}

const SPACE_FIELD_TYPE_ANY: &str = "any";
const SPACE_FIELD_TYPE_UNSIGNED: &str = "unsigned";
const SPACE_FIELD_TYPE_STRING: &str = "string";
const SPACE_FIELD_TYPE_NUMBER: &str = "number";
const SPACE_FIELD_TYPE_DOUBLE: &str = "double";
const SPACE_FIELD_TYPE_INTEGER: &str = "integer";
const SPACE_FIELD_TYPE_BOOLEAN: &str = "boolean";
const SPACE_FIELD_TYPE_DECIMAL: &str = "decimal";
const SPACE_FIELD_TYPE_UUID: &str = "uuid";
const SPACE_FIELD_TYPE_ARRAY: &str = "array";
const SPACE_FIELD_TYPE_SCALAR: &str = "scalar";

impl SpaceFieldType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Any => SPACE_FIELD_TYPE_ANY,
            Self::Unsigned => SPACE_FIELD_TYPE_UNSIGNED,
            Self::String => SPACE_FIELD_TYPE_STRING,
            Self::Number => SPACE_FIELD_TYPE_NUMBER,
            Self::Double => SPACE_FIELD_TYPE_DOUBLE,
            Self::Integer => SPACE_FIELD_TYPE_INTEGER,
            Self::Boolean => SPACE_FIELD_TYPE_BOOLEAN,
            Self::Decimal => SPACE_FIELD_TYPE_DECIMAL,
            Self::Uuid => SPACE_FIELD_TYPE_UUID,
            Self::Array => SPACE_FIELD_TYPE_ARRAY,
            Self::Scalar => SPACE_FIELD_TYPE_SCALAR,
        }
    }
}
impl<'de> Deserialize<'de> for SpaceFieldType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str = String::deserialize(deserializer)?.trim().to_lowercase();

        Ok(match str.as_str() {
            SPACE_FIELD_TYPE_ANY => Self::Any,
            SPACE_FIELD_TYPE_UNSIGNED => Self::Unsigned,
            SPACE_FIELD_TYPE_STRING => Self::String,
            SPACE_FIELD_TYPE_NUMBER => Self::Number,
            SPACE_FIELD_TYPE_DOUBLE => Self::Double,
            SPACE_FIELD_TYPE_INTEGER => Self::Integer,
            SPACE_FIELD_TYPE_BOOLEAN => Self::Boolean,
            SPACE_FIELD_TYPE_DECIMAL => Self::Decimal,
            SPACE_FIELD_TYPE_UUID => Self::Uuid,
            SPACE_FIELD_TYPE_ARRAY => Self::Array,
            SPACE_FIELD_TYPE_SCALAR => Self::Scalar,
            _ => {
                return Err(serde::de::Error::unknown_variant(
                    &str,
                    &[
                        SPACE_FIELD_TYPE_ANY,
                        SPACE_FIELD_TYPE_UNSIGNED,
                        SPACE_FIELD_TYPE_STRING,
                        SPACE_FIELD_TYPE_NUMBER,
                        SPACE_FIELD_TYPE_DOUBLE,
                        SPACE_FIELD_TYPE_INTEGER,
                        SPACE_FIELD_TYPE_BOOLEAN,
                        SPACE_FIELD_TYPE_DECIMAL,
                        SPACE_FIELD_TYPE_UUID,
                        SPACE_FIELD_TYPE_ARRAY,
                        SPACE_FIELD_TYPE_SCALAR,
                    ],
                ));
            }
        })
    }
}

impl fmt::Display for SpaceFieldType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct FuncMetadata {
    pub id: u32,
    pub owner: u32,
    pub name: String,
    pub setuid: u32,
    pub language: String,
    pub body: String,
    pub routine_type: String,
    pub param_list: Vec<Value>,
    pub returns: String,
    pub aggregate: String,
    pub sql_data_access: String,
    pub is_deterministic: bool,
    pub is_sandboxed: bool,
    pub is_null_call: bool,
    pub exports: Vec<String>,
    pub opts: Map<String, Value>,
    pub comment: String,
    pub created: String,
    pub last_altered: String,
}

impl AsTuple for FuncMetadata {}

#[derive(Clone, Debug, Serialize)]
pub struct Privilege {
    pub grantor: u32,
    pub grantee: u32,
    pub object_type: String,
    pub object_id: u32,
    pub privilege: u32,
}

impl AsTuple for Privilege {}

struct SpaceCache {
    spaces: RefCell<HashMap<String, Space>>,
    indexes: RefCell<HashMap<(u32, String), Index>>,
}

impl SpaceCache {
    fn new() -> Self {
        Self {
            spaces: RefCell::new(HashMap::new()),
            indexes: RefCell::new(HashMap::new()),
        }
    }

    fn space(&self, name: &str) -> Option<Space> {
        let mut cache = self.spaces.borrow_mut();
        cache.get(name).cloned().or_else(|| {
            Space::find(name).map(|space| {
                cache.insert(name.to_string(), space.clone());
                space
            })
        })
    }

    fn index(&self, space: &Space, name: &str) -> Option<Index> {
        let mut cache = self.indexes.borrow_mut();
        cache.get(&(space.id, name.to_string())).cloned().or_else(|| {
            space.index(name).map(|index| {
                cache.insert((space.id, name.to_string()), index.clone());
                index
            })
        })
    }
}

thread_local! {
    static SPACE_CACHE: SpaceCache = SpaceCache::new();
}

#[derive(Clone, Debug)]
pub struct Space {
    id: u32,
}

impl Space {
    /// Return a space builder.
    ///
    /// - `name` - name of space to be created
    pub fn builder(name: &str) -> Builder {
        Builder::new(name)
    }

    /// Create a space.
    /// (for details see [box.schema.space.create()](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_schema/space_create/)).
    ///
    /// - `name` - name of space, which should conform to the rules for object names.
    /// - `opts` - see SpaceCreateOptions struct.
    ///
    /// Returns a new space.
    #[cfg(feature = "schema")]
    pub fn create(name: &str, opts: &SpaceCreateOptions) -> Result<Space, Error> {
        crate::schema::space::create_space(name, opts)
    }

    /// Drop a space.
    #[cfg(feature = "schema")]
    pub fn drop(&self) -> Result<(), Error> {
        crate::schema::space::drop_space(self.id)
    }

    /// Find space by name.
    ///
    /// This function performs SELECT request to `_vspace` system space.
    /// - `name` - space name
    ///
    /// Returns:
    /// - `None` if not found
    /// - `Some(space)` otherwise
    pub fn find(name: &str) -> Option<Self> {
        let id =
            unsafe { ffi::box_space_id_by_name(name.as_ptr() as *const c_char, name.len() as u32) };

        if id == ffi::BOX_ID_NIL {
            None
        } else {
            Some(Self { id })
        }
    }

    /// Memorized version of [`Space::find`] function.
    ///
    /// The function performs SELECT request to `_vspace` system space only if
    /// it was never called for target space.
    /// - `name` - space name
    ///
    /// Returns:
    /// - `None` if not found
    /// - `Some(space)` otherwise
    pub fn find_cached(name: &str) -> Option<Self> {
        SPACE_CACHE.with(|cache| {
            cache.space(name)
        })
    }

    /// Get space ID.
    pub const fn id(&self) -> u32 {
        self.id
    }

    /// Create new index.
    ///
    /// - `name` - name of index to create, which should conform to the rules for object names.
    /// - `opts` - see schema::IndexOptions struct.
    #[cfg(feature = "schema")]
    pub fn create_index(
        &self,
        name: &str,
        opts: &crate::index::IndexOptions,
    ) -> Result<Index, Error> {
        crate::schema::index::create_index(self.id, name, opts)
    }

    /// Return an index builder.
    ///
    /// - `name` - name of index to create, which should conform to the rules for object names.
    #[cfg(feature = "schema")]
    pub fn index_builder<'a>(&self, name: &'a str) -> index::Builder<'a> {
        index::Builder::new(self.id, name)
    }

    /// Find index by name.
    ///
    /// This function performs SELECT request to `_vindex` system space.
    /// - `name` - index name
    ///
    /// Returns:
    /// - `None` if not found
    /// - `Some(index)` otherwise
    pub fn index(&self, name: &str) -> Option<Index> {
        let index_id = unsafe {
            ffi::box_index_id_by_name(self.id, name.as_ptr() as *const c_char, name.len() as u32)
        };

        if index_id == ffi::BOX_ID_NIL {
            None
        } else {
            Some(Index::new(self.id, index_id))
        }
    }

    /// Memorized version of [`Space::index`] function.
    ///
    /// This function performs SELECT request to `_vindex` system space.
    /// - `name` - index name
    ///
    /// Returns:
    /// - `None` if not found
    /// - `Some(index)` otherwise
    pub fn index_cached(&self, name: &str) -> Option<Index> {
        SPACE_CACHE.with(|cache| {
            cache.index(self, name)
        })
    }

    /// Returns index with id = 0
    #[inline(always)]
    pub fn primary_key(&self) -> Index {
        Index::new(self.id, 0)
    }

    /// Insert a tuple into a space.
    ///
    /// - `value` - tuple value to insert
    ///
    /// Returns a new tuple.
    ///
    /// See also: `box.space[space_id]:insert(tuple)`
    pub fn insert<T>(&mut self, value: &T) -> Result<Tuple, Error>
    where
        T: AsTuple,
    {
        let buf = value.serialize_as_tuple().unwrap();
        let buf_ptr = buf.as_ptr() as *const c_char;
        tuple_from_box_api!(
            ffi::box_insert[
                self.id,
                buf_ptr,
                buf_ptr.add(buf.len()),
                @out
            ]
        )
            .map(|t| t.expect("Returned tuple cannot be null"))
    }

    /// Insert a tuple into a space.
    /// If a tuple with the same primary key already exists, [space.replace()](#method.replace) replaces the existing
    /// tuple with a new one. The syntax variants [space.replace()](#method.replace) and [space.put()](#method.put)
    /// have the same effect;
    /// the latter is sometimes used to show that the effect is the converse of [space.get()](#method.get).
    ///
    /// - `value` - tuple value to replace with
    ///
    /// Returns a new tuple.
    pub fn replace<T>(&mut self, value: &T) -> Result<Tuple, Error>
    where
        T: AsTuple,
    {
        let buf = value.serialize_as_tuple().unwrap();
        let buf_ptr = buf.as_ptr() as *const c_char;
        tuple_from_box_api!(
            ffi::box_replace[
                self.id,
                buf_ptr,
                buf_ptr.add(buf.len()),
                @out
            ]
        )
            .map(|t| t.expect("Returned tuple cannot be null"))
    }

    /// Insert a tuple into a space. If a tuple with the same primary key already exists, it replaces the existing tuple
    /// with a new one. Alias for [space.replace()](#method.replace)
    #[inline(always)]
    pub fn put<T>(&mut self, value: &T) -> Result<Tuple, Error>
    where
        T: AsTuple,
    {
        self.replace(value)
    }

    /// Deletes all tuples. The method is performed in background and doesn’t block consequent requests.
    pub fn truncate(&mut self) -> Result<(), Error> {
        if unsafe { ffi::box_truncate(self.id) } < 0 {
            return Err(TarantoolError::last().into());
        }
        Ok(())
    }

    /// Return the number of tuples in the space.
    ///
    /// Compared with [space.count()](#method.count), this method works faster because [space.len()](#method.len)
    /// does not scan the entire space to count the tuples.
    #[inline(always)]
    pub fn len(&self) -> Result<usize, Error> {
        self.primary_key().len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> Result<bool, Error> {
        self.len().map(|l| l == 0)
    }

    /// Number of bytes in the space.
    ///
    /// This number, which is stored in Tarantool’s internal memory, represents the total number of bytes in all tuples,
    /// excluding index keys. For a measure of index size, see [index.bsize()](../index/struct.Index.html#method.bsize).
    #[inline(always)]
    pub fn bsize(&self) -> Result<usize, Error> {
        self.primary_key().bsize()
    }

    /// Search for a tuple in the given space.
    #[inline(always)]
    pub fn get<K>(&self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
    {
        self.primary_key().get(key)
    }

    /// Search for a tuple or a set of tuples in the given space. This method doesn’t yield
    /// (for details see [Сooperative multitasking](https://www.tarantool.io/en/doc/latest/book/box/atomic_index/#atomic-cooperative-multitasking)).
    ///
    /// - `type` - iterator type
    /// - `key` - encoded key in the MsgPack Array format (`[part1, part2, ...]`).
    #[inline(always)]
    pub fn select<K>(&self, iterator_type: IteratorType, key: &K) -> Result<IndexIterator, Error>
    where
        K: AsTuple,
    {
        self.primary_key().select(iterator_type, key)
    }

    /// Return the number of tuples. Compared with [space.len()](#method.len), this method works slower because
    /// [space.count()](#method.count) scans the entire space to count the tuples.
    ///
    /// - `type` - iterator type
    /// - `key` - encoded key in the MsgPack Array format (`[part1, part2, ...]`).
    pub fn count<K>(&self, iterator_type: IteratorType, key: &K) -> Result<usize, Error>
    where
        K: AsTuple,
    {
        self.primary_key().count(iterator_type, key)
    }

    /// Delete a tuple identified by a primary key.
    ///
    /// - `key` - encoded key in the MsgPack Array format (`[part1, part2, ...]`).
    ///
    /// Returns the deleted tuple
    #[inline(always)]
    pub fn delete<K>(&mut self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
    {
        self.primary_key().delete(key)
    }

    /// Update a tuple.
    ///
    /// The `update` function supports operations on fields — assignment, arithmetic (if the field is numeric),
    /// cutting and pasting fragments of a field, deleting or inserting a field. Multiple operations can be combined in
    /// a single update request, and in this case they are performed atomically and sequentially. Each operation
    /// requires specification of a field number. When multiple operations are present, the field number for each
    /// operation is assumed to be relative to the most recent state of the tuple, that is, as if all previous
    /// operations in a multi-operation update have already been applied.
    /// In other words, it is always safe to merge multiple `update` invocations into a single invocation with no
    /// change in semantics.
    ///
    /// - `key` - encoded key in the MsgPack Array format (`[part1, part2, ...]`).
    /// - `ops` - encoded operations in the MsgPack array format, e.g. `[['=', field_id, value], ['!', 2, 'xxx']]`
    ///
    /// Returns a new tuple.
    ///
    /// See also: [space.upsert()](#method.upsert)
    #[inline(always)]
    pub fn update<K, Op>(&mut self, key: &K, ops: &[Op]) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
        Op: AsTuple,
    {
        self.primary_key().update(key, ops)
    }

    /// Update a tuple using `ops` already encoded in the message pack format.
    ///
    /// This function is similar to [`update`](#method.update) but instead
    /// of a generic type parameter `Op` it accepts preencoded message pack
    /// values. This is usefull when the operations have values of different
    /// types.
    ///
    /// Returns a new tuple.
    #[inline(always)]
    pub fn update_mp<K>(&mut self, key: &K, ops: &[Vec<u8>]) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
    {
        self.primary_key().update_mp(key, ops)
    }

    /// Update or insert a tuple.
    ///
    /// If there is an existing tuple which matches the tuple key fields, then the request has the same effect as
    /// [space.update()](#method.update) and the `{{operator, field_no, value}, ...}` parameter is used.
    /// If there is no existing tuple which matches the tuple key fields, then the request has the same effect as
    /// [space.insert()](#method.insert) and the `{tuple}` parameter is used.
    /// However, unlike `insert` or `update`, `upsert` will not read a tuple and perform error checks before
    /// returning – this is a design feature which enhances throughput but requires more cautious use.
    ///
    /// - `value` - encoded tuple in the MsgPack Array format (`[field1, field2, ...]`)
    /// - `ops` - encoded operations in the MsgPack array format, e.g. `[['=', field_id, value], ['!', 2, 'xxx']]`
    ///
    /// See also: [space.update()](#method.update)
    #[inline(always)]
    pub fn upsert<T, Op>(&mut self, value: &T, ops: &[Op]) -> Result<(), Error>
    where
        T: AsTuple,
        Op: AsTuple,
    {
        self.primary_key().upsert(value, ops)
    }

    /// Upsert a tuple using `ops` already encoded in the message pack format.
    ///
    /// This function is similar to [`upsert`](#method.upsert) but instead
    /// of a generic type parameter `Op` it accepts preencoded message pack
    /// values. This is usefull when the operations have values of different
    /// types.
    #[inline(always)]
    pub fn upsert_mp<K>(&mut self, key: &K, ops: &[Vec<u8>]) -> Result<(), Error>
        where
            K: AsTuple,
    {
        self.primary_key().upsert_mp(key, ops)
    }

    // Return space metadata from system `_space` space.
    #[cfg(feature = "schema")]
    pub fn meta(&self) -> Result<SpaceMetadata, Error> {
        let sys_space: Space = SystemSpace::Space.into();
        let tuple = sys_space.get(&(self.id,))?.ok_or(Error::MetaNotFound)?;
        tuple.as_struct::<SpaceMetadata>()
    }
}

////////////////////////////////////////////////////////////////////////////////
// Builder
////////////////////////////////////////////////////////////////////////////////

pub struct Builder<'a> {
    name: &'a str,
    opts: SpaceCreateOptions,
}

macro_rules! define_setters {
    ($( $setter:ident ( $field:ident : $ty:ty ) )+) => {
        $(
            #[inline(always)]
            pub fn $setter(mut self, $field: $ty) -> Self {
                self.opts.$field = $field.into();
                self
            }
        )+
    }
}

impl<'a> Builder<'a> {
    pub fn new(name: &'a str) -> Self {
        Self {
            name,
            opts: Default::default(),
        }
    }

    define_setters!{
        if_not_exists(if_not_exists: bool)
        engine(engine: SpaceEngineType)
        id(id: u32)
        field_count(field_count: u32)
        user(user: String)
        is_local(is_local: bool)
        is_temporary(is_temporary: bool)
        is_sync(is_sync: bool)
        format(format: Vec<Field>)
    }

    pub fn field(mut self, field: Field) -> Self {
        self.opts.format.get_or_insert_with(|| Vec::with_capacity(16))
            .push(field);
        self
    }

    #[cfg(feature = "schema")]
    pub fn create(self) -> crate::Result<Space> {
        crate::schema::space::create_space(self.name, &self.opts)
    }
}

////////////////////////////////////////////////////////////////////////////////
// macros
////////////////////////////////////////////////////////////////////////////////

/// Update a tuple or index.
///
/// The helper macro with the same semantic as `space.update()`/`index.update()` functions, but supports
/// different types in `ops` argument.
///
/// - `target` - updated space or index.
/// - `key` - encoded key in the MsgPack Array format (`[part1, part2, ...]`).
/// - `ops` - encoded operations in the MsgPack array format, e.g. `[['=', field_id, 100], ['!', 2, 'xxx']]`
///
/// Returns a new tuple.
///
/// See also: [space.update()](#method.update)
#[macro_export]
macro_rules! update {
        ($target:expr, $key: expr, $($op:expr),+ $(,)?) => {
            {
                use std::borrow::Borrow;
                let mut f = || -> $crate::Result<::std::option::Option<$crate::tuple::Tuple>> {
                    let ops = [
                        $(
                            $crate::util::rmp_to_vec($op.borrow())?,
                        )+
                    ];
                    $target.update_mp($key.borrow(), &ops)
                };
                f()
            }
        };
    }

/// Upsert a tuple or index.
///
/// The helper macro with the same semantic as `space.upsert()`/`index.upsert()` functions, but supports
/// different types in `ops` argument.
///
/// - `target` - updated space or index.
/// - `value` - encoded tuple in the MsgPack Array format (`[part1, part2, ...]`).
/// - `ops` - encoded operations in the MsgPack array format, e.g. `[['=', field_id, 100], ['!', 2, 'xxx']]`
///
/// See also: [space.update()](#method.update)
#[macro_export]
macro_rules! upsert {
        ($target:expr, $value: expr, $($op:expr),+ $(,)?) => {
            {
                use std::borrow::Borrow;
                let mut f = || -> $crate::Result<()> {
                    let ops = [
                        $(
                            $crate::util::rmp_to_vec($op.borrow())?,
                        )+
                    ];
                    $target.upsert_mp($value.borrow(), &ops)
                };
                f()
            }
        };
    }
