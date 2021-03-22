//! Box: spaces
//!
//! **CRUD operations** in Tarantool are implemented by the box.space submodule.
//! It has the data-manipulation functions select, insert, replace, update, upsert, delete, get, put.
//!
//! See also:
//! - [Lua reference: Submodule box.space](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/)
//! - [C API reference: Module box](https://www.tarantool.io/en/doc/latest/dev_guide/reference_capi/box/)
use std::cmp::max;
use std::os::raw::c_char;
use std::ptr::null_mut;

use num_traits::ToPrimitive;
use serde_json::{Map, Number, Value};

use crate::error::{set_error, Error, TarantoolError, TarantoolErrorCode};
use crate::ffi::tarantool as ffi;
use crate::index::{self, Index, IndexIterator, IndexOptions, IteratorType};
use crate::schema::{self, SpaceEngineType, SpaceMetadata};
use crate::sequence::Sequence;
use crate::session;
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

pub struct Space {
    id: u32,
}

/// Options for new space, used by Space::create.
/// (for details see [Options for box.schema.space.create](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_schema/space_create/)).
///
/// `format` option is not supported at this moment.
pub struct SpaceCreateOptions {
    pub if_not_exists: bool,
    pub engine: Option<SpaceEngineType>,
    pub id: Option<u32>,
    pub field_count: Option<u32>,
    pub user: Option<String>,
    pub is_local: bool,
    pub is_temporary: bool,
    pub is_sync: bool,
}

impl SpaceCreateOptions {
    /// Create instance of SpaceCreateOptions with default values.
    pub fn default() -> SpaceCreateOptions {
        SpaceCreateOptions {
            if_not_exists: false,
            engine: None,
            id: None,
            field_count: None,
            user: None,
            is_local: true,
            is_temporary: true,
            is_sync: false,
        }
    }
}

impl Space {
    /// Create a space.
    /// (for details see [box.schema.space.create()](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_schema/space_create/)).
    ///
    /// - `name` -  name of space, which should conform to the rules for object names.
    /// - `opts` - see SpaceCreateOptions struct.
    ///
    /// Returns a new space.
    pub fn create(name: &str, opts: &SpaceCreateOptions) -> Result<Space, Error> {
        // Check if space already exists.
        let space = Space::find(name);
        if space.is_some() {
            return if opts.if_not_exists {
                Ok(space.unwrap())
            } else {
                set_error(file!(), line!(), &TarantoolErrorCode::SpaceExists, name);
                Err(TarantoolError::last().into())
            };
        }

        // Resolve ID of user, specified in options, or use ID of current session's user.
        let user_id = match &opts.user {
            None => session::uid()? as u32,
            Some(user) => {
                let resolved_uid = Space::resolve_user_or_role(user.as_str())?;
                match resolved_uid {
                    Some(uid) => uid,
                    None => {
                        set_error(
                            file!(),
                            line!(),
                            &TarantoolErrorCode::NoSuchUser,
                            user.as_str(),
                        );
                        return Err(TarantoolError::last().into());
                    }
                }
            }
        };

        // Resolve ID of new space or use ID, specified in options.
        let space_id = match opts.id {
            None => Space::resolve_new_space_id()?,
            Some(id) => id,
        };

        Space::insert_new_space(space_id, user_id, name, opts)
    }

    fn resolve_new_space_id() -> Result<u32, Error> {
        let sys_space: Space = SystemSpace::Space.into();
        let mut sys_schema: Space = SystemSpace::Schema.into();

        // Try to update max_id in _schema space.
        let new_max_id = sys_schema.update(&("max_id",), &vec![("+".to_string(), 1, 1)])?;

        let space_id = if new_max_id.is_some() {
            // In case of successful update max_id return its value.
            new_max_id.unwrap().field::<u32>(1)?.unwrap()
        } else {
            // Get tuple with greatest id. Increment it and use as id of new space.
            let max_tuple = sys_space.index("primary").unwrap().max(&())?.unwrap();
            let max_tuple_id = max_tuple.field::<u32>(0)?.unwrap();
            let max_id_val = max(max_tuple_id, SYSTEM_ID_MAX);
            // Insert max_id into _schema space.
            let created_max_id = sys_schema
                .insert(&("max_id".to_string(), max_id_val + 1))?
                .unwrap();
            created_max_id.field::<u32>(1)?.unwrap()
        };

        return Ok(space_id);
    }

    fn resolve_user_or_role(user: &str) -> Result<Option<u32>, Error> {
        let space_vuser: Space = SystemSpace::VUser.into();
        let name_idx = space_vuser.index("name").unwrap();
        Ok(match name_idx.get(&(user,))? {
            None => None,
            Some(user_tuple) => Some(user_tuple.field::<u32>(0)?.unwrap()),
        })
    }

    fn insert_new_space(
        id: u32,
        uid: u32,
        name: &str,
        opts: &SpaceCreateOptions,
    ) -> Result<Space, Error> {
        // Update _space with metadata about new space.
        let engine = match opts.engine {
            None => SpaceEngineType::Memtx,
            Some(e) => e,
        };

        let field_count = match opts.field_count {
            None => 0,
            Some(count) => count,
        };

        let mut space_opts = Map::<String, Value>::new();
        if opts.is_local {
            space_opts.insert("group_id".to_string(), Value::Number(Number::from(1)));
        }
        if opts.is_temporary {
            space_opts.insert("temporary".to_string(), Value::Bool(true));
        }
        // Only for Tarantool version >= 2.6
        // space_opts.insert("is_sync".to_string(), Value::Bool(opts.is_sync));

        let new_space = SpaceMetadata {
            id: id,
            uid: uid,
            name: name.to_string(),
            engine: engine,
            field_count: field_count,
            options: space_opts.clone(),
            format: Vec::<Value>::new(),
        };

        let mut sys_space: Space = SystemSpace::Space.into();
        match sys_space.insert(&new_space) {
            Err(e) => Err(e),
            Ok(_) => Ok(Space::find(name).unwrap()),
        }
    }

    /// Drop a space.
    pub fn drop(&self) -> Result<(), Error> {
        // Delete automatically generated sequence.
        let mut sys_space_sequence: Space = SystemSpace::SpaceSequence.into();
        let seq_tuple = sys_space_sequence.delete(&(self.id,))?;
        match seq_tuple {
            None => (),
            Some(t) => {
                let is_generated = t.field::<bool>(2)?.unwrap();
                if is_generated {
                    let seq_id = t.field::<u32>(1)?.unwrap();
                    let seq = Sequence::find_by_id(seq_id)?.unwrap();
                    seq.drop()?;
                }
            }
        }

        // Remove from _trigger.
        let mut sys_trigger: Space = SystemSpace::Trigger.into();
        let sys_space_idx = sys_trigger.index("space_id").unwrap();
        for t in sys_space_idx
            .select(IteratorType::Eq, &(self.id,))?
            .collect::<Vec<Tuple>>()
        {
            let name = t.field::<String>(0)?.unwrap();
            sys_trigger.delete(&(name,))?;
        }

        // Remove from _fk_constraint.
        let mut sys_fk_constraint: Space = SystemSpace::FkConstraint.into();
        let sys_space_idx = sys_fk_constraint.index("child_id").unwrap();
        for t in sys_space_idx
            .select(IteratorType::Eq, &(self.id,))?
            .collect::<Vec<Tuple>>()
        {
            let name = t.field::<String>(0)?.unwrap();
            sys_fk_constraint.delete(&(name, self.id))?;
        }

        // CRemove from _ck_constraint.
        let mut sys_ck_constraint: Space = SystemSpace::CkConstraint.into();
        let sys_space_idx = sys_ck_constraint.index("primary").unwrap();
        for t in sys_space_idx
            .select(IteratorType::Eq, &(self.id,))?
            .collect::<Vec<Tuple>>()
        {
            let name = t.field::<String>(2)?.unwrap();
            sys_ck_constraint.delete(&(self.id, name))?;
        }

        // Remove from _func_index.
        let mut sys_func_index: Space = SystemSpace::FuncIndex.into();
        let sys_space_idx = sys_func_index.index("primary").unwrap();
        for t in sys_space_idx
            .select(IteratorType::Eq, &(self.id,))?
            .collect::<Vec<Tuple>>()
        {
            let index_id = t.field::<u32>(1)?.unwrap();
            sys_func_index.delete(&(self.id, index_id))?;
        }

        // Remove from _index.
        let sys_vindex: Space = SystemSpace::VIndex.into();
        let mut sys_index: Space = SystemSpace::Index.into();
        let keys = sys_vindex
            .select(IteratorType::Eq, &(self.id,))?
            .collect::<Vec<Tuple>>();
        for i in 1..keys.len() + 1 {
            let t_idx = keys.len() - i;
            let t = &keys[t_idx];
            let id = t.field::<u32>(0)?.unwrap();
            let iid = t.field::<u32>(1)?.unwrap();
            sys_index.delete(&(id, iid))?;
        }

        // Revoke priveleges.
        schema::revoke_object_privileges("space", self.id)?;

        // Remove from _truncate.
        let mut sys_truncate: Space = SystemSpace::Truncate.into();
        sys_truncate.delete(&(self.id,))?;

        // Remove from _space.
        let mut sys_space: Space = SystemSpace::Space.into();
        sys_space.delete(&(self.id,))?;

        Ok(())
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
    pub fn create_index(&self, name: &str, opts: &IndexOptions) -> Result<Index, Error> {
        index::create_index(self.id, name, opts)?;
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
