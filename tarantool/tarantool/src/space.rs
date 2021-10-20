//! Box: spaces
//!
//! **CRUD operations** in Tarantool are implemented by the box.space submodule.
//! It has the data-manipulation functions select, insert, replace, update, upsert, delete, get, put.
//!
//! See also:
//! - [Lua reference: Submodule box.space](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/)
//! - [C API reference: Module box](https://www.tarantool.io/en/doc/latest/dev_guide/reference_capi/box/)
use std::fmt;
use std::os::raw::c_char;
use std::ptr::null_mut;

use num_traits::ToPrimitive;
use serde::{Serialize, Serializer};
use serde_json::{Map, Value};

use crate::error::{Error, TarantoolError};
use crate::ffi::tarantool as ffi;
use crate::index::{Index, IndexIterator, IteratorType};
use crate::tuple::{AsTuple, Tuple};

/// End of the reserved range of system spaces.
pub const SYSTEM_ID_MAX: u32 = 511;

/// Provides access to system spaces
///
/// Example:
/// ```rust
/// use tarantool::space::SystemSpace;
/// let schema_space = SystemSpace::Schema.into();
/// ```
#[repr(u32)]
#[derive(Debug, Clone, PartialEq, ToPrimitive)]
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

impl Into<Space> for SystemSpace {
    fn into(self) -> Space {
        Space {
            id: self.to_u32().unwrap(),
        }
    }
}

/// Type of engine, used by space.
#[derive(Copy, Clone, Debug)]
pub enum SpaceEngineType {
    Memtx,
    Vinyl,
}

impl Serialize for SpaceEngineType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            SpaceEngineType::Memtx => serializer.serialize_str("memtx"),
            SpaceEngineType::Vinyl => serializer.serialize_str("vinyl"),
        }
    }
}

/// Options for new space, used by Space::create.
/// (for details see [Options for box.schema.space.create](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_schema/space_create/)).
///
/// `format` option is not supported at this moment.
#[derive(Serialize)]
pub struct SpaceCreateOptions {
    pub if_not_exists: bool,
    pub engine: Option<SpaceEngineType>,
    pub id: Option<u32>,
    pub field_count: Option<u32>,
    pub user: Option<String>,
    pub is_local: bool,
    pub is_temporary: bool,
    pub is_sync: bool,
    pub format: Option<Vec<SpaceFieldFormat>>,
}

impl Default for SpaceCreateOptions {
    fn default() -> Self {
        SpaceCreateOptions {
            if_not_exists: false,
            engine: None,
            id: None,
            field_count: None,
            user: None,
            is_local: true,
            is_temporary: true,
            is_sync: false,
            format: None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SpaceFieldFormat {
    pub name: String,
    #[serde(alias = "type")]
    pub field_type: SpaceFieldType,
}

impl SpaceFieldFormat {
    pub fn new(name: &str, ft: SpaceFieldType) -> Self {
        return SpaceFieldFormat {
            name: name.to_string(),
            field_type: ft,
        };
    }
}

#[derive(Copy, Clone, Debug, Serialize)]
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

impl fmt::Display for SpaceFieldType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Serialize, Debug)]
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

#[derive(Serialize, Debug)]
pub struct Privilege {
    pub grantor: u32,
    pub grantee: u32,
    pub object_type: String,
    pub object_id: u32,
    pub privilege: u32,
}

impl AsTuple for Privilege {}

pub struct Space {
    id: u32,
}

impl Space {
    /// Create a space.
    /// (for details see [box.schema.space.create()](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_schema/space_create/)).
    ///
    /// - `name` -  name of space, which should conform to the rules for object names.
    /// - `opts` - see SpaceCreateOptions struct.
    ///
    /// Returns a new space.
    #[cfg(feature = "schema")]
    pub fn create(name: &str, opts: &SpaceCreateOptions) -> Result<Space, Error> {
        return crate::schema::space::create_space(name, opts);
    }

    /// Drop a space.
    #[cfg(feature = "schema")]
    pub fn drop(&self) -> Result<(), Error> {
        return crate::schema::space::drop_space(self.id);
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
        crate::schema::index::create_index(self.id, name, opts)?;
        Ok(self.index(name).unwrap())
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
    pub fn insert<T>(&mut self, value: &T) -> Result<Option<Tuple>, Error>
    where
        T: AsTuple,
    {
        let buf = value.serialize_as_tuple().unwrap();
        let buf_ptr = buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<ffi::BoxTuple>();

        if unsafe {
            ffi::box_insert(
                self.id,
                buf_ptr,
                buf_ptr.offset(buf.len() as isize),
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

    /// Insert a tuple into a space.
    /// If a tuple with the same primary key already exists, [space.replace()](#method.replace) replaces the existing
    /// tuple with a new one. The syntax variants [space.replace()](#method.replace) and [space.put()](#method.put)
    /// have the same effect;
    /// the latter is sometimes used to show that the effect is the converse of [space.get()](#method.get).
    ///
    /// - `value` - tuple value to replace with
    ///
    /// Returns a new tuple.
    pub fn replace<T>(&mut self, value: &T) -> Result<Option<Tuple>, Error>
    where
        T: AsTuple,
    {
        let buf = value.serialize_as_tuple().unwrap();
        let buf_ptr = buf.as_ptr() as *const c_char;
        let mut result_ptr = null_mut::<ffi::BoxTuple>();

        if unsafe {
            ffi::box_replace(
                self.id,
                buf_ptr,
                buf_ptr.offset(buf.len() as isize),
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

    /// Insert a tuple into a space. If a tuple with the same primary key already exists, replaces the existing tuple
    /// with a new one. Alias for [space.replace()](#method.replace)
    #[inline(always)]
    pub fn put<T>(&mut self, value: &T) -> Result<Option<Tuple>, Error>
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
    /// If compared with [space.count()](#method.count), this method works faster because [space.len()](#method.len)
    /// does not scan the entire space to count the tuples.
    #[inline(always)]
    pub fn len(&self) -> Result<usize, Error> {
        self.primary_key().len()
    }

    /// Number of bytes in the space.
    ///
    /// This number, which is stored in Tarantool’s internal memory, represents the total number of bytes in all tuples,
    /// not including index keys. For a measure of index size, see [index.bsize()](../index/struct.Index.html#method.bsize).
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
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    #[inline(always)]
    pub fn select<K>(&self, iterator_type: IteratorType, key: &K) -> Result<IndexIterator, Error>
    where
        K: AsTuple,
    {
        self.primary_key().select(iterator_type, key)
    }

    /// Return the number of tuples. If compared with [space.len()](#method.len), this method works slower because
    /// [space.count()](#method.count) scans the entire space to count the tuples.
    ///
    /// - `type` - iterator type
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    pub fn count<K>(&self, iterator_type: IteratorType, key: &K) -> Result<usize, Error>
    where
        K: AsTuple,
    {
        self.primary_key().count(iterator_type, key)
    }

    /// Delete a tuple identified by a primary key.
    ///
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
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
    /// In other words, it is always safe to merge multiple `update` invocations into a single invocation, with no
    /// change in semantics.
    ///
    /// - `key` - encoded key in MsgPack Array format (`[part1, part2, ...]`).
    /// - `ops` - encoded operations in MsgPack array format, e.g. `[['=', field_id, value], ['!', 2, 'xxx']]`
    ///
    /// Returns a new tuple.
    ///
    /// See also: [space.upsert()](#method.upsert)
    #[inline(always)]
    pub fn update<K, Op>(&mut self, key: &K, ops: &Vec<Op>) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
        Op: AsTuple,
    {
        self.primary_key().update(key, ops)
    }

    /// Update or insert a tuple.
    ///
    /// If there is an existing tuple which matches the key fields of tuple, then the request has the same effect as
    /// [space.update()](#method.update) and the `{{operator, field_no, value}, ...}` parameter is used.
    /// If there is no existing tuple which matches the key fields of tuple, then the request has the same effect as
    /// [space.insert()](#method.insert) and the `{tuple}` parameter is used.
    /// However, unlike `insert` or `update`, `upsert` will not read a tuple and perform error checks before
    /// returning – this is a design feature which enhances throughput but requires more caution on the part of the
    /// user.
    ///
    /// - `value` - encoded tuple in MsgPack Array format (`[field1, field2, ...]`)
    /// - `ops` - encoded operations in MsgPack array format, e.g. `[['=', field_id, value], ['!', 2, 'xxx']]`
    ///
    /// Returns a new tuple.
    ///
    /// See also: [space.update()](#method.update)
    #[inline(always)]
    pub fn upsert<T, Op>(&mut self, value: &T, ops: &Vec<Op>) -> Result<Option<Tuple>, Error>
    where
        T: AsTuple,
        Op: AsTuple,
    {
        self.primary_key().upsert(value, ops)
    }
}
