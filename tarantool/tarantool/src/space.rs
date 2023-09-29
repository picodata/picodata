//! Box: spaces
//!
//! **CRUD operations** in Tarantool are implemented by the box.space submodule.
//! It has the data-manipulation functions select, insert, replace, update, upsert, delete, get, put.
//!
//! See also:
//! - [Lua reference: Submodule box.space](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/)
//! - [C API reference: Module box](https://www.tarantool.io/en/doc/latest/dev_guide/reference_capi/box/)
use crate::error::{Error, TarantoolError};
use crate::ffi::tarantool as ffi;
use crate::index::{Index, IndexIterator, IteratorType};
use crate::tuple::{Encode, ToTupleBuffer, Tuple, TupleBuffer};
use crate::tuple_from_box_api;
use crate::unwrap_or;
use crate::util::Value;
use serde::{Deserialize, Serialize};
use serde_json::Map;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ops::Range;
use std::os::raw::c_char;

/// End of the reserved range of system spaces.
pub const SYSTEM_ID_MAX: SpaceId = 511;

/// Maximum possible space id.
pub const SPACE_ID_MAX: SpaceId = i32::MAX as _;

pub type SpaceId = u32;

/// Provides access to system spaces
///
/// Example:
/// ```no_run
/// use tarantool::space::SystemSpace;
/// assert_eq!(SystemSpace::Schema as u32, 272)
/// ```
#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
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

impl SystemSpace {
    #[inline(always)]
    pub fn as_space(&self) -> Space {
        Space { id: *self as _ }
    }
}

impl From<SystemSpace> for Space {
    #[inline(always)]
    fn from(ss: SystemSpace) -> Self {
        Space { id: ss as _ }
    }
}

crate::define_str_enum! {
    #![coerce_from_str]
    /// Type of engine, used by space.
    pub enum SpaceEngineType {
        /// Spaces with "memtx" engine type store their tuples in memory.
        /// Persistence is implemented via the WAL.
        ///
        /// See <https://www.tarantool.io/en/doc/latest/concepts/engines/memtx/>
        /// for more details.
        Memtx = "memtx",

        /// Spaces with "vinyl" engine type use LSM trees for tuple storage.
        ///
        /// See <https://www.tarantool.io/en/doc/latest/concepts/engines/vinyl/>
        /// for more details.
        Vinyl = "vinyl",

        /// Spaces with "sysview" engine type represent read-only views into a
        /// system space.
        ///
        /// Cannot be used as type of user defined spaces.
        /// Is used as part of [`Metadata`] when decoding tuples from
        /// _space.
        ///
        /// See <https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/system_views/>
        /// for more details.
        SysView = "sysview",

        /// This is a special space engine which is only used by _session_settings.
        /// You shouldn't care about this.
        ///
        /// Cannot be used as type of user defined spaces.
        /// Is used as part of [`Metadata`] when decoding tuples from
        /// _space.
        Service = "service",

        /// This is crutch implemented by tarantool authors to workaround some
        /// internal issue. You shouldn't care about this.
        ///
        /// Cannot be used as type of user defined spaces.
        /// Is used as part of [`Metadata`] when decoding tuples from
        /// _space.
        Blackhole = "blackhole",

    }
}

impl Default for SpaceEngineType {
    #[inline(always)]
    fn default() -> Self {
        Self::Memtx
    }
}

/// Options for new space, used by Space::create.
/// (for details see [Options for box.schema.space.create](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_schema/space_create/)).
#[derive(Default, Clone, Debug)]
pub struct SpaceCreateOptions {
    pub if_not_exists: bool,
    pub engine: SpaceEngineType,
    pub id: Option<SpaceId>,
    pub field_count: u32,
    pub user: Option<String>,
    pub space_type: SpaceType,
    pub format: Option<Vec<Field>>,
}

/// Possible values for the [`SpaceCreateOptions::space_type`] field.
#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub enum SpaceType {
    /// Default space type. Data and meta-data is persisted and replicated.
    #[default]
    Normal,

    /// Space is created on all replicas and exists after restart,
    /// but the data is neither persisted nor replicated.
    ///
    /// Same as `{ type = 'data-temporary' }` in lua.
    DataTemporary,

    /// Space is only created on current instance and doesn't exist after restart,
    /// and the data is also neither persisted nor replicated.
    ///
    /// Such space can be created in readonly mode.
    ///
    /// Same as `{ type = 'temporary' }` in lua.
    Temporary,

    /// Space is created on all replicas and exists after restart,
    /// data is persisted but not replicated.
    ///
    /// Same as `{ is_local = true }` in lua.
    DataLocal,

    /// Space is created on all replicas and exists after restart,
    /// data is persisted and replicated synchronously, see
    /// <https://www.tarantool.io/en/doc/latest/concepts/replication/repl_sync/>
    /// for more info.
    ///
    /// Same as `{ is_sync = true }` in lua.
    Synchronous,
}

////////////////////////////////////////////////////////////////////////////////
// Field
////////////////////////////////////////////////////////////////////////////////

#[deprecated = "Use `space::Field` instead"]
pub type SpaceFieldFormat = Field;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Field {
    pub name: String, // TODO(gmoshkin): &str
    #[serde(alias = "type")]
    pub field_type: FieldType,
    pub is_nullable: bool,
}

impl<S> From<(S, FieldType, IsNullable)> for Field
where
    String: From<S>,
{
    #[inline(always)]
    fn from(args: (S, FieldType, IsNullable)) -> Self {
        let (name, field_type, is_nullable) = args;
        let name = name.into();
        let is_nullable = is_nullable.is_nullable();
        Self {
            name,
            field_type,
            is_nullable,
        }
    }
}

impl<S> From<(S, FieldType)> for Field
where
    String: From<S>,
{
    #[inline(always)]
    fn from(args: (S, FieldType)) -> Self {
        let (name, field_type) = args;
        let name = name.into();
        let is_nullable = false;
        Self {
            name,
            field_type,
            is_nullable,
        }
    }
}

macro_rules! define_constructors {
    ($($constructor:ident ($type:path))+) => {
        $(
            #[doc = ::std::concat!(
                "Create a new field format specifier with the given `name` and ",
                "type \"", ::std::stringify!($constructor), "\""
            )]
            #[inline(always)]
            pub fn $constructor(name: impl Into<String>) -> Self {
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
    #[inline(always)]
    pub fn new(name: &str, ft: FieldType) -> Self {
        Self {
            name: name.to_string(),
            field_type: ft,
            is_nullable: false,
        }
    }

    /// Specify if the current field can be nullable or not. This method
    /// captures `self` by value and returns it, so it should be used in a
    /// builder fashion.
    /// ```no_run
    /// use tarantool::space::Field;
    /// let f = Field::string("middle name").is_nullable(true);
    /// ```
    #[inline(always)]
    pub fn is_nullable(mut self, is_nullable: bool) -> Self {
        self.is_nullable = is_nullable;
        self
    }

    define_constructors! {
        any(FieldType::Any)
        unsigned(FieldType::Unsigned)
        string(FieldType::String)
        number(FieldType::Number)
        double(FieldType::Double)
        integer(FieldType::Integer)
        boolean(FieldType::Boolean)
        varbinary(FieldType::Varbinary)
        scalar(FieldType::Scalar)
        decimal(FieldType::Decimal)
        uuid(FieldType::Uuid)
        datetime(FieldType::Datetime)
        interval(FieldType::Interval)
        array(FieldType::Array)
        map(FieldType::Map)
    }
}

////////////////////////////////////////////////////////////////////////////////
// FieldType
////////////////////////////////////////////////////////////////////////////////

#[deprecated = "use space::FieldType instead"]
pub type SpaceFieldType = FieldType;

crate::define_str_enum! {
    #![coerce_from_str]
    /// Type of a field in the space format definition.
    pub enum FieldType {
        Any       = "any",
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
        Interval  = "interval",
        Array     = "array",
        Map       = "map",
    }
}

////////////////////////////////////////////////////////////////////////////////
// IsNullable
////////////////////////////////////////////////////////////////////////////////

/// An enum specifying whether or not the given space field can be null.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum IsNullable {
    NonNullalbe,
    Nullable,
}

impl IsNullable {
    #[inline(always)]
    const fn is_nullable(&self) -> bool {
        matches!(self, Self::Nullable)
    }
}

////////////////////////////////////////////////////////////////////////////////
// ...
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Serialize)]
pub struct FuncMetadata {
    pub id: u32,
    pub owner: u32,
    pub name: String,
    pub setuid: u32,
    pub language: String,
    pub body: String,
    pub routine_type: String,
    // TODO: Use util::Value instead, extend it if needed.
    pub param_list: Vec<serde_json::Value>,
    pub returns: String,
    pub aggregate: String,
    pub sql_data_access: String,
    pub is_deterministic: bool,
    pub is_sandboxed: bool,
    pub is_null_call: bool,
    pub exports: Vec<String>,
    // TODO: Use util::Value instead, extend it if needed.
    pub opts: Map<String, serde_json::Value>,
    pub comment: String,
    pub created: String,
    pub last_altered: String,
}

impl Encode for FuncMetadata {}

#[derive(Clone, Debug, Serialize)]
pub struct Privilege {
    pub grantor: u32,
    pub grantee: u32,
    pub object_type: String,
    pub object_id: u32,
    pub privilege: u32,
}

impl Encode for Privilege {}

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

    fn clear(&self) {
        self.spaces.borrow_mut().clear();
        self.indexes.borrow_mut().clear();
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
        cache
            .get(&(space.id, name.to_string()))
            .cloned()
            .or_else(|| {
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

/// Clear the space and index cache so that the next call to
/// [`Space::find_cached`] & [`Space::index_cached`] will have to update the
/// cache.
pub fn clear_cache() {
    SPACE_CACHE.with(SpaceCache::clear)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Space {
    id: SpaceId,
}

impl Space {
    /// Return a space builder.
    ///
    /// - `name` - name of space to be created
    #[inline(always)]
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
    #[inline(always)]
    pub fn create(name: &str, opts: &SpaceCreateOptions) -> Result<Space, Error> {
        crate::schema::space::create_space(name, opts)
    }

    /// Drop a space.
    #[inline(always)]
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
    #[inline(always)]
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
    /// **NOTE** the cache can become invalid for a number of reasons. If an
    /// operation with a space returned from this function results in a
    /// [`TarantoolError`] with code [`NoSuchSpace`], try calling [`clear_cache`]
    /// before trying to find the space again.
    ///
    /// Returns:
    /// - `None` if not found
    /// - `Some(space)` otherwise
    ///
    /// [`NoSuchSpace`]: crate::error::TarantoolErrorCode::NoSuchSpace
    #[inline(always)]
    pub fn find_cached(name: &str) -> Option<Self> {
        SPACE_CACHE.with(|cache| cache.space(name))
    }

    /// Create a `Space` with `id`.
    ///
    /// # Safety
    /// `id` must be a valid tarantool space id. Only use this function with
    /// ids acquired from tarantool in some way, e.g. from lua code.
    #[inline(always)]
    pub const unsafe fn from_id_unchecked(id: SpaceId) -> Self {
        Self { id }
    }

    /// Get space ID.
    #[inline(always)]
    pub const fn id(&self) -> SpaceId {
        self.id
    }

    /// Create new index.
    ///
    /// - `name` - name of index to create, which should conform to the rules for object names.
    /// - `opts` - see schema::IndexOptions struct.
    #[inline(always)]
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
    #[inline(always)]
    pub fn index_builder<'a>(&self, name: &'a str) -> crate::index::Builder<'a> {
        crate::index::Builder::new(self.id, name)
    }

    /// Find index by name.
    ///
    /// This function performs SELECT request to `_vindex` system space.
    /// - `name` - index name
    ///
    /// Returns:
    /// - `None` if not found
    /// - `Some(index)` otherwise
    #[inline(always)]
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
    /// **NOTE** the cache can become invalid for a number of reasons. If an
    /// operation with an index returned from this function results in a
    /// [`TarantoolError`] with code [`NoSuchSpace`] or [`NoSuchIndexID`], try
    /// calling [`clear_cache`] before trying to get the index again.
    ///
    /// Returns:
    /// - `None` if not found
    /// - `Some(index)` otherwise
    ///
    /// [`NoSuchSpace`]: crate::error::TarantoolErrorCode::NoSuchSpace
    /// [`NoSuchIndexID`]: crate::error::TarantoolErrorCode::NoSuchIndexID
    #[inline(always)]
    pub fn index_cached(&self, name: &str) -> Option<Index> {
        SPACE_CACHE.with(|cache| cache.index(self, name))
    }

    /// Returns index with id = 0
    #[inline(always)]
    pub fn primary_key(&self) -> Index {
        Index::new(self.id, 0)
    }

    /// Insert a `value` into a space.
    ///
    /// Returns a new tuple.
    ///
    /// See also: `box.space[space_id]:insert(tuple)`
    #[inline]
    pub fn insert<T>(&self, value: &T) -> Result<Tuple, Error>
    where
        T: ToTupleBuffer + ?Sized,
    {
        let buf;
        let data = unwrap_or!(value.tuple_data(), {
            // TODO: use region allocation for this
            buf = value.to_tuple_buffer()?;
            buf.as_ref()
        });
        let Range { start, end } = data.as_ptr_range();
        tuple_from_box_api!(
            ffi::box_insert[
                self.id,
                start as _,
                end as _,
                @out
            ]
        )
        .map(|t| t.expect("Returned tuple cannot be null"))
    }

    /// Insert a `value` into a space passing.
    ///
    /// If a tuple with the same primary key already exists, it is replaced
    /// with a new one.
    ///
    /// Has the same effect as [`Space::put`] but the latter is sometimes used
    /// to show that the effect is the converse of [`Space::get`].
    ///
    /// - `value` - tuple value to replace with
    ///
    /// Returns a new tuple.
    #[inline]
    pub fn replace<T>(&self, value: &T) -> Result<Tuple, Error>
    where
        T: ToTupleBuffer + ?Sized,
    {
        let buf;
        let data = unwrap_or!(value.tuple_data(), {
            // TODO: use region allocation for this
            buf = value.to_tuple_buffer()?;
            buf.as_ref()
        });
        let Range { start, end } = data.as_ptr_range();
        tuple_from_box_api!(
            ffi::box_replace[
                self.id,
                start as _,
                end as _,
                @out
            ]
        )
        .map(|t| t.expect("Returned tuple cannot be null"))
    }

    /// Insert a tuple into a space. If a tuple with the same primary key already exists, it replaces the existing tuple
    /// with a new one. Alias for [space.replace()](#method.replace)
    #[inline(always)]
    pub fn put<T>(&self, value: &T) -> Result<Tuple, Error>
    where
        T: ToTupleBuffer + ?Sized,
    {
        self.replace(value)
    }

    /// Deletes all tuples.
    ///
    /// The method is performed in background and doesn’t block consequent
    /// requests.
    #[inline(always)]
    pub fn truncate(&self) -> Result<(), Error> {
        // SAFETY: this is always safe actually
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
        K: ToTupleBuffer + ?Sized,
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
        K: ToTupleBuffer + ?Sized,
    {
        self.primary_key().select(iterator_type, key)
    }

    /// Return the number of tuples. Compared with [space.len()](#method.len), this method works slower because
    /// [space.count()](#method.count) scans the entire space to count the tuples.
    ///
    /// - `type` - iterator type
    /// - `key` - encoded key in the MsgPack Array format (`[part1, part2, ...]`).
    #[inline(always)]
    pub fn count<K>(&self, iterator_type: IteratorType, key: &K) -> Result<usize, Error>
    where
        K: ToTupleBuffer + ?Sized,
    {
        self.primary_key().count(iterator_type, key)
    }

    /// Delete a tuple identified by a primary `key`.
    ///
    /// The `key` must represent a msgpack array consisting of the appropriate
    /// amount of the primary index's parts.
    ///
    /// Returns the deleted tuple
    #[inline(always)]
    pub fn delete<K>(&self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer + ?Sized,
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
    pub fn update<K, Op>(&self, key: &K, ops: impl AsRef<[Op]>) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer + ?Sized,
        Op: ToTupleBuffer,
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
    ///
    /// # Safety
    /// `ops` must be a slice of valid msgpack arrays.
    #[inline(always)]
    #[deprecated = "use update_raw instead"]
    pub unsafe fn update_mp<K>(&self, key: &K, ops: &[Vec<u8>]) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer + ?Sized,
    {
        #[allow(deprecated)]
        self.primary_key().update_mp(key, ops)
    }

    /// Update a tuple using already encoded arguments.
    ///
    /// This function is similar to [`update`](#method.update) but instead
    /// of generic type parameters `T` & `Op` it accepts preencoded message
    /// pack arrays. This is usefull when the operations have values of
    /// different types.
    ///
    /// # Safety
    /// `key` must be a valid msgpack array.
    /// `ops` must be a valid msgpack array of msgpack arrays.
    #[inline(always)]
    pub unsafe fn update_raw(&self, key: &[u8], ops: &[u8]) -> Result<Option<Tuple>, Error> {
        self.primary_key().update_raw(key, ops)
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
    pub fn upsert<T, Op>(&self, value: &T, ops: impl AsRef<[Op]>) -> Result<(), Error>
    where
        T: ToTupleBuffer + ?Sized,
        Op: ToTupleBuffer,
    {
        self.primary_key().upsert(value, ops)
    }

    /// Upsert a tuple using `ops` already encoded in the message pack format.
    ///
    /// This function is similar to [`upsert`](#method.upsert) but instead
    /// of a generic type parameter `Op` it accepts preencoded message pack
    /// values. This is usefull when the operations have values of different
    /// types.
    ///
    /// # Safety
    /// `ops` must be a slice of valid msgpack arrays.
    #[inline(always)]
    #[deprecated = "use upsert_raw instead"]
    pub unsafe fn upsert_mp<T>(&self, value: &T, ops: &[Vec<u8>]) -> Result<(), Error>
    where
        T: ToTupleBuffer + ?Sized,
    {
        #[allow(deprecated)]
        self.primary_key().upsert_mp(value, ops)
    }

    /// Upsert a tuple using already encoded arguments.
    ///
    /// This function is similar to [`upsert`](#method.upsert) but instead
    /// of generic type parameters `T` & `Op` it accepts preencoded message
    /// pack arrays. This is usefull when the operations have values of
    /// different types.
    ///
    /// # Safety
    /// `value` must be a valid msgpack array.
    /// `ops` must be a valid msgpack array of msgpack arrays.
    #[inline(always)]
    pub unsafe fn upsert_raw(&self, value: &[u8], ops: &[u8]) -> Result<(), Error> {
        self.primary_key().upsert_raw(value, ops)
    }

    // Return space metadata from system `_space` space.
    #[inline(always)]
    pub fn meta(&self) -> Result<Metadata, Error> {
        let sys_space: Space = SystemSpace::Space.into();
        let tuple = sys_space.get(&(self.id,))?.ok_or(Error::MetaNotFound)?;
        tuple.decode::<Metadata>()
    }
}

////////////////////////////////////////////////////////////////////////////////
// Metadata
////////////////////////////////////////////////////////////////////////////////

/// Space metadata. Represents a tuple of a system `_space` space.
#[derive(Default, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Metadata<'a> {
    pub id: u32,
    pub user_id: u32,
    pub name: Cow<'a, str>,
    pub engine: SpaceEngineType,
    pub field_count: u32,
    pub flags: BTreeMap<Cow<'a, str>, Value<'a>>,
    pub format: Vec<BTreeMap<Cow<'a, str>, Value<'a>>>,
}
impl Encode for Metadata<'_> {}

////////////////////////////////////////////////////////////////////////////////
// Builder
////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
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
    #[inline(always)]
    pub fn new(name: &'a str) -> Self {
        Self {
            name,
            opts: Default::default(),
        }
    }

    define_setters! {
        if_not_exists(if_not_exists: bool)
        engine(engine: SpaceEngineType)
        id(id: SpaceId)
        field_count(field_count: u32)
        user(user: String)
        space_type(space_type: SpaceType)
    }

    #[deprecated = "use Builder::space_type instead"]
    #[inline(always)]
    pub fn is_local(mut self, is_local: bool) -> Self {
        if is_local {
            self.opts.space_type = SpaceType::DataLocal
        }
        self
    }

    #[deprecated = "use Builder::space_type instead"]
    #[inline(always)]
    pub fn temporary(mut self, temporary: bool) -> Self {
        if temporary {
            self.opts.space_type = SpaceType::DataTemporary
        }
        self
    }

    #[deprecated = "use Builder::space_type instead"]
    #[inline(always)]
    pub fn is_sync(mut self, is_sync: bool) -> Self {
        if is_sync {
            self.opts.space_type = SpaceType::Synchronous
        }
        self
    }

    /// Add a field to the space's format.
    ///
    /// Use this method to set each field individually or use [`format`] to set
    /// fields in bulk. The difference is purely syntactical.
    ///
    /// [`format`]: Self::format
    #[inline(always)]
    pub fn field(mut self, field: impl Into<Field>) -> Self {
        self.opts
            .format
            .get_or_insert_with(|| Vec::with_capacity(16))
            .push(field.into());
        self
    }

    /// Add fields to the space's format.
    ///
    /// Use this method to set fields in bulk or use [`field`] to set
    /// each field individually. The difference is purely syntactical.
    ///
    /// ```no_run
    /// use tarantool::space::{Space, FieldType as FT, IsNullable};
    ///
    /// let space = Space::builder("user_names")
    ///     .format([
    ///         ("id", FT::Unsigned),
    ///         ("name", FT::String),
    ///     ])
    ///     .field(("nickname", FT::String, IsNullable::Nullable))
    ///     .create();
    /// ```
    ///
    /// [`field`]: Self::field
    #[inline]
    pub fn format(mut self, format: impl IntoIterator<Item = impl Into<Field>>) -> Self {
        let iter = format.into_iter();
        let (size, _) = iter.size_hint();
        self.opts
            .format
            .get_or_insert_with(|| Vec::with_capacity(size))
            .extend(iter.map(Into::into));
        self
    }

    #[inline(always)]
    pub fn create(self) -> crate::Result<Space> {
        crate::schema::space::create_space(self.name, &self.opts)
    }

    /// Destructure the builder struct into a tuple of name and space options.
    #[inline(always)]
    pub fn into_parts(self) -> (&'a str, SpaceCreateOptions) {
        (self.name, self.opts)
    }
}

////////////////////////////////////////////////////////////////////////////////
// UpdateOps
////////////////////////////////////////////////////////////////////////////////

/// A builder-style helper struct for [`Space::update`], [`Space::upsert`],
/// [`Index::update`], [`Index::upsert`] methods.
///
/// Start by calling the [`new`] function, then chain as many operations as
/// needed ([`add`], [`assign`], [`insert`], etc.) after that you can either
/// pass the resulting expression directly into one of the supported methods,
/// or use the data directly after calling [`encode`] or [`into_inner`].
///
/// # Examples
/// ```no_run
/// use tarantool::space::{Space, UpdateOps};
/// let mut space = Space::find("employee").unwrap();
/// space.update(
///     &[1337],
///     UpdateOps::new()
///         .add("strikes", 1).unwrap()
///         .assign("days-since-last-mistake", 0).unwrap(),
/// )
/// .unwrap();
/// ```
///
/// [`new`]: UpdateOps::new
/// [`add`]: UpdateOps::add
/// [`assign`]: UpdateOps::assign
/// [`insert`]: UpdateOps::insert
/// [`encode`]: UpdateOps::encode
/// [`into_inner`]: UpdateOps::into_inner
pub struct UpdateOps {
    ops: Vec<TupleBuffer>,
}

macro_rules! define_bin_ops {
    ($( $(#[$meta:meta])* $op_name:ident, $op_code:literal; )+) => {
        $(
            $(#[$meta])*
            #[inline(always)]
            pub fn $op_name<K, V>(&mut self, field: K, value: V) -> crate::Result<&mut Self>
            where
                K: Serialize,
                V: Serialize,
            {
                self.ops.push(($op_code, field, value).to_tuple_buffer()?);
                Ok(self)
            }
        )+
    }
}

impl UpdateOps {
    #[inline]
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    #[inline(always)]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            ops: Vec::with_capacity(capacity),
        }
    }

    define_bin_ops! {
        /// Assignment operation.
        /// Corresponds to tarantool's `{'=', field, value}`.
        ///
        /// Field indexing is zero based (first field has index 0).
        /// Negative indexes are offset from array's end (last field has index -1).
        assign, '=';

        /// Insertion operation.
        /// Corresponds to tarantool's `{'!', field, value}`.
        ///
        /// Field indexing is zero based (first field has index 0).
        /// Negative indexes are offset from array's end (last field has index -1).
        insert, '!';

        /// Numeric addition operation.
        /// Corresponds to tarantool's `{'+', field, value}`.
        ///
        /// Field indexing is zero based (first field has index 0).
        /// Negative indexes are offset from array's end (last field has index -1).
        add, '+';

        /// Numeric subtraction operation.
        /// Corresponds to tarantool's `{'-', field, value}`.
        ///
        /// Field indexing is zero based (first field has index 0).
        /// Negative indexes are offset from array's end (last field has index -1).
        sub, '-';

        /// Bitwise AND operation.
        /// Corresponds to tarantool's `{'&', field, value}`.
        ///
        /// Field indexing is zero based (first field has index 0).
        /// Negative indexes are offset from array's end (last field has index -1).
        and, '&';

        /// Bitwise OR operation.
        /// Corresponds to tarantool's `{'|', field, value}`.
        ///
        /// Field indexing is zero based (first field has index 0).
        /// Negative indexes are offset from array's end (last field has index -1).
        or, '|';

        /// Bitwise XOR operation.
        /// Corresponds to tarantool's `{'^', field, value}`.
        ///
        /// Field indexing is zero based (first field has index 0).
        /// Negative indexes are offset from array's end (last field has index -1).
        xor, '^';
    }

    /// Deletion operation.
    /// Corresponds to tarantool's `{'#', field, count}`.
    ///
    /// Field indexing is zero based (first field has index 0).
    /// Negative indexes are offset from array's end (last field has index -1).
    #[inline]
    pub fn delete<K>(&mut self, field: K, count: usize) -> crate::Result<&mut Self>
    where
        K: Serialize,
    {
        self.ops.push(('#', field, count).to_tuple_buffer()?);
        Ok(self)
    }

    /// String splicing operation.
    /// Corresponds to tarantool's `{':', field, start, count, value}`.
    ///
    /// Field indexing is zero based (first field has index 0).
    /// Negative indexes are offset from array's end (last field has index -1).
    #[inline]
    pub fn splice<K>(
        &mut self,
        field: K,
        start: isize,
        count: usize,
        value: &str,
    ) -> crate::Result<&mut Self>
    where
        K: Serialize,
    {
        self.ops
            .push((':', field, start, count, value).to_tuple_buffer()?);
        Ok(self)
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[TupleBuffer] {
        &self.ops
    }

    #[inline(always)]
    pub fn into_inner(self) -> Vec<TupleBuffer> {
        self.ops
    }

    #[inline]
    pub fn encode(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(4 + 4 * self.ops.len());
        self.encode_to(&mut res).expect("memory allocation failed");
        res
    }

    #[inline]
    pub fn encode_to(&self, w: &mut impl std::io::Write) -> crate::Result<()> {
        crate::msgpack::write_array_len(w, self.ops.len() as _)?;
        for op in &self.ops {
            op.write_tuple_data(w)?;
        }
        Ok(())
    }
}

impl Default for UpdateOps {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl AsRef<[TupleBuffer]> for UpdateOps {
    #[inline(always)]
    fn as_ref(&self) -> &[TupleBuffer] {
        &self.ops
    }
}

impl From<UpdateOps> for Vec<TupleBuffer> {
    #[inline(always)]
    fn from(ops: UpdateOps) -> Vec<TupleBuffer> {
        ops.ops
    }
}

impl IntoIterator for UpdateOps {
    type Item = TupleBuffer;
    type IntoIter = std::vec::IntoIter<TupleBuffer>;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.ops.into_iter()
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
    ($target:expr, $key:expr, $($op:expr),+ $(,)?) => {{
        use $crate::tuple::ToTupleBuffer;
        let f = || -> $crate::Result<Option<$crate::tuple::Tuple>> {
            let key = $key;
            let buf;
            let key_data = $crate::unwrap_or!(key.tuple_data(), {
                // TODO: use region allocation for this
                buf = key.to_tuple_buffer()?;
                buf.as_ref()
            });

            const LEN: u32 = $crate::expr_count!($($op),+);
            let mut ops_buf = Vec::with_capacity((4 + LEN * 4) as _);
            $crate::msgpack::write_array_len(&mut ops_buf, LEN)?;
            $( $op.write_tuple_data(&mut ops_buf)?; )+
            #[allow(unused_unsafe)]
            unsafe {
                $target.update_raw(key_data, ops_buf.as_ref())
            }
        };
        f()
    }};
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
    ($target:expr, $value: expr, $($op:expr),+ $(,)?) => {{
        use $crate::tuple::ToTupleBuffer;
        let f = || -> $crate::Result<()> {
            let value = $value;
            let buf;
            let value_data = $crate::unwrap_or!(value.tuple_data(), {
                // TODO: use region allocation for this
                buf = value.to_tuple_buffer()?;
                buf.as_ref()
            });

            const LEN: u32 = $crate::expr_count!($($op),+);
            let mut ops_buf = Vec::with_capacity((4 + LEN * 4) as _);
            $crate::msgpack::write_array_len(&mut ops_buf, LEN)?;
            $( $op.write_tuple_data(&mut ops_buf)?; )+
            #[allow(unused_unsafe)]
            unsafe {
                $target.upsert_raw(value_data, ops_buf.as_ref())
            }
        };
        f()
    }};
}

#[cfg(feature = "internal_test")]
mod test {
    use super::*;
    use crate::tuple::RawBytes;

    #[crate::test(tarantool = "crate")]
    fn insert_raw_bytes() {
        let space_name = crate::temp_space_name!();
        let space = Space::builder(&space_name).create().unwrap();
        space.index_builder("pk").create().unwrap();
        space.insert(RawBytes::new(b"\x93*\xa3foo\xa3bar")).unwrap();
        let t = space.get(&(42,)).unwrap().unwrap();
        let t: (u32, String, String) = t.decode().unwrap();
        assert_eq!(t, (42, "foo".to_owned(), "bar".to_owned()));
        space.drop().unwrap();
    }

    #[crate::test(tarantool = "crate")]
    fn sys_space_metadata() {
        let sys_space = Space::from(SystemSpace::Space);
        for tuple in sys_space.select(IteratorType::All, &()).unwrap() {
            // Check space metadata is deserializable from what is actually in _space
            let _meta: Metadata = tuple.decode().unwrap();
        }
    }
}
