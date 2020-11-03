use std::os::raw::c_char;
use std::ptr::null_mut;

use num_traits::ToPrimitive;

use crate::error::{Error, TarantoolError};
use crate::index::Index;
use crate::tuple::{AsTuple, Tuple};

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

pub struct Space {
    id: u32,
}

impl Space {
    /// Find space by name.
    ///
    /// This function performs SELECT request to `_vspace` system space.
    /// - `name` - space name
    ///
    /// Returns:
    /// - `None` if not found
    /// - `Some(space)` otherwise
    ///
    /// See also: [index_by_name](#method.index_by_name)
    pub fn find(name: &str) -> Option<Self> {
        let id =
            unsafe { ffi::box_space_id_by_name(name.as_ptr() as *const c_char, name.len() as u32) };

        if id == ffi::BOX_ID_NIL {
            None
        } else {
            Some(Self { id })
        }
    }

    /// Access to system space
    pub fn system_space(id: SystemSpace) -> Self {
        Self {
            id: id.to_u32().unwrap(),
        }
    }

    /// Find index by name.
    ///
    /// This function performs SELECT request to _vindex system space.
    /// - `name` - index name
    ///
    /// Returns:
    /// - `None` if not found
    /// - `Some(index)` otherwise
    ///
    /// See also: [find_by_name](#method.find_by_name)
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

    /// Execute an INSERT request.
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

    /// Execute an REPLACE request.
    ///
    /// - `value` - tuple value to replace with
    ///
    /// Returns a new tuple.
    ///
    /// See also: `box.space[space_id]:replace(tuple)`
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
    /// with a new one. Alias for [replace](#method.replace)
    #[inline(always)]
    pub fn put<T>(&mut self, value: &T) -> Result<Option<Tuple>, Error>
    where
        T: AsTuple,
    {
        self.replace(value)
    }

    /// Truncate space.
    pub fn truncate(&mut self) -> Result<(), Error> {
        if unsafe { ffi::box_truncate(self.id) } < 0 {
            return Err(TarantoolError::last().into());
        }
        Ok(())
    }

    #[inline(always)]
    pub fn len(&self) -> Result<usize, Error> {
        self.primary_key().len()
    }

    #[inline(always)]
    pub fn bsize(&self) -> Result<usize, Error> {
        self.primary_key().bsize()
    }

    #[inline(always)]
    pub fn get<K>(&self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
    {
        self.primary_key().get(key)
    }

    #[inline(always)]
    pub fn delete<K>(&mut self, key: &K) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
    {
        self.primary_key().delete(key)
    }

    #[inline(always)]
    pub fn update<K, Op>(&mut self, key: &K, ops: &Vec<Op>) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
        Op: AsTuple,
    {
        self.primary_key().update(key, ops)
    }

    #[inline(always)]
    pub fn upsert<T, Op>(&mut self, value: &T, ops: &Vec<Op>) -> Result<Option<Tuple>, Error>
    where
        T: AsTuple,
        Op: AsTuple,
    {
        self.primary_key().upsert(value, ops)
    }
}

pub mod ffi {
    use std::os::raw::{c_char, c_int};

    pub use crate::tuple::ffi::BoxTuple;

    pub const BOX_SYSTEM_ID_MIN: u32 = 256;
    pub const BOX_SYSTEM_ID_MAX: u32 = 511;
    pub const BOX_ID_NIL: u32 = 2147483647;

    extern "C" {
        pub fn box_space_id_by_name(name: *const c_char, len: u32) -> u32;
        pub fn box_index_id_by_name(space_id: u32, name: *const c_char, len: u32) -> u32;
        pub fn box_insert(
            space_id: u32,
            tuple: *const c_char,
            tuple_end: *const c_char,
            result: *mut *mut BoxTuple,
        ) -> c_int;
        pub fn box_replace(
            space_id: u32,
            tuple: *const c_char,
            tuple_end: *const c_char,
            result: *mut *mut BoxTuple,
        ) -> c_int;
        pub fn box_truncate(space_id: u32) -> c_int;
    }
}
