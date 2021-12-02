use crate::{
    ffi,
    Push,
    PushGuard,
    PushOne,
    AsLua,
    tuples::TuplePushError::{self, First, Other},
    LuaRead,
    LuaState,
    lua_tables::LuaTable,
};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::iter;
use std::num::NonZeroI32;

#[inline]
fn push_iter<L, I>(lua: L, iterator: I)
    -> Result<PushGuard<L>, (<<I as Iterator>::Item as Push<LuaState>>::Err, L)>
where
    L: AsLua,
    I: Iterator,
    <I as Iterator>::Item: Push<LuaState>,
{
    // creating empty table
    unsafe { ffi::lua_newtable(lua.as_lua()) };

    for (elem, index) in iterator.zip(1..) {
        let size = match elem.push_to_lua(lua.as_lua()) {
            Ok(pushed) => pushed.forget_internal(),
            // TODO: wrong   return Err((err, lua)),
            // FIXME: destroy the temporary table
            Err((_err, _lua)) => panic!(),
        };

        match size {
            0 => continue,
            1 => {
                let index = index as u32;
                match index.push_to_lua(lua.as_lua()) {
                    Ok(pushed) => pushed.forget_internal(),
                    Err(_) => unreachable!(),
                };
                unsafe { ffi::lua_insert(lua.as_lua(), -2) }
                unsafe { ffi::lua_settable(lua.as_lua(), -3) }
            }
            2 => unsafe { ffi::lua_settable(lua.as_lua(), -3) },
            _ => unreachable!(),
        }
    }

    unsafe {
        Ok(PushGuard::new(lua, 1))
    }
}

#[inline]
fn push_rec_iter<L, I>(lua: L, iterator: I)
    -> Result<PushGuard<L>, (<<I as Iterator>::Item as Push<LuaState>>::Err, L)>
where
    L: AsLua,
    I: Iterator,
    <I as Iterator>::Item: Push<LuaState>,
{
    let (nrec, _) = iterator.size_hint();

    // creating empty table with pre-allocated non-array elements
    unsafe { ffi::lua_createtable(lua.as_lua(), 0, nrec as i32) };

    for elem in iterator {
        let size = match elem.push_to_lua(lua.as_lua()) {
            Ok(pushed) => pushed.forget_internal(),
            // TODO: wrong   return Err((err, lua)),
            // FIXME: destroy the temporary table
            Err((_err, _lua)) => panic!(),
        };

        match size {
            0 => continue,
            2 => unsafe { ffi::lua_settable(lua.as_lua(), -3) },
            _ => unreachable!(),
        }
    }

    unsafe {
        Ok(PushGuard::new(lua, 1))
    }
}

impl<L, T> Push<L> for Vec<T>
where
    L: AsLua,
    T: Push<LuaState>,
{
    type Err = T::Err;

    #[inline]
    fn push_to_lua(self, lua: L) -> Result<PushGuard<L>, (T::Err, L)> {
        push_iter(lua, self.into_iter())
    }
}

impl<L, T> PushOne<L> for Vec<T>
where
    L: AsLua,
    T: Push<LuaState>,
{
}

impl<L, T> LuaRead<L> for Vec<T>
where
    L: AsLua,
    T: for<'a> LuaRead<&'a LuaTable<L>>,
    T: 'static,
{
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Self, L> {
        // We need this as iteration order isn't guaranteed to match order of
        // keys, even if they're numeric
        // https://www.lua.org/manual/5.2/manual.html#pdf-next
        let table = match LuaTable::lua_read_at_position(lua, index) {
            Ok(table) => table,
            Err(lua) => return Err(lua),
        };
        let mut dict: BTreeMap<i32, T> = BTreeMap::new();

        let mut max_key = i32::MIN;
        let mut min_key = i32::MAX;

        for (key, value) in table.iter::<i32, T>().flatten() {
            max_key = max_key.max(key);
            min_key = min_key.min(key);
            dict.insert(key, value);
        }

        if dict.is_empty() {
            return Ok(vec![])
        }

        if min_key != 1 {
            // Rust doesn't support sparse arrays or arrays with negative
            // indices
            return Err(table.into_inner());
        }

        let mut result = Vec::with_capacity(max_key as _);

        // We expect to start with first element of table and have this
        // be smaller that first key by one
        let mut previous_key = 0;

        // By this point, we actually iterate the map to move values to Vec
        // and check that table represented non-sparse 1-indexed array
        for (k, v) in dict {
            if previous_key + 1 != k {
                return Err(table.into_inner())
            } else {
                // We just push, thus converting Lua 1-based indexing
                // to Rust 0-based indexing
                result.push(v);
                previous_key = k;
            }
        }

        Ok(result)
    }
}

impl<'a, L, T> Push<L> for &'a [T]
where
    L: AsLua,
    T: Push<LuaState>,
    T: Clone,
{
    type Err = T::Err;

    #[inline]
    fn push_to_lua(self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        push_iter(lua, self.into_iter().map(Clone::clone))
    }
}

impl<'a, L, T> PushOne<L> for &'a [T]
where
    L: AsLua,
    T: Push<LuaState>,
    T: Clone,
{
}

impl<L, K, V> LuaRead<L> for HashMap<K, V>
where
    L: AsLua,
    K: 'static + Hash + Eq,
    K: for<'k> LuaRead<&'k LuaTable<L>>,
    V: 'static,
    V: for<'v> LuaRead<&'v LuaTable<L>>,
{
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Self, L> {
        let table = LuaTable::lua_read_at_position(lua, index)?;
        Ok(table.iter().flatten().collect())
    }
}

// TODO: use an enum for the error to allow different error types for K and V
impl<L, K, V> Push<L> for HashMap<K, V>
where
    L: AsLua,
    K: PushOne<LuaState> + Eq + Hash,
    K: std::fmt::Debug,
    V: PushOne<LuaState>,
    V: std::fmt::Debug,
{
    type Err = TuplePushError<
        <K as Push<LuaState>>::Err,
        <V as Push<LuaState>>::Err,
    >;

    #[inline]
    fn push_to_lua(self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        match push_rec_iter(lua, self.into_iter()) {
            Ok(g) => Ok(g),
            Err((TuplePushError::First(err), lua)) => Err((First(err), lua)),
            Err((TuplePushError::Other(err), lua)) => Err((Other(err), lua)),
        }
    }
}

impl<L, K, V, E> PushOne<L> for HashMap<K, V>
where
    L: AsLua,
    K: PushOne<LuaState, Err = E> + Eq + Hash,
    K: std::fmt::Debug,
    V: PushOne<LuaState, Err = E>,
    V: std::fmt::Debug,
{
}

impl<L, K> Push<L> for HashSet<K>
where
    L: AsLua,
    K: PushOne<LuaState> + Eq + Hash,
    K: std::fmt::Debug,
{
    type Err = K::Err;

    #[inline]
    fn push_to_lua(self, lua: L) -> Result<PushGuard<L>, (K::Err, L)> {
        match push_rec_iter(lua, self.into_iter().zip(iter::repeat(true))) {
            Ok(g) => Ok(g),
            Err((TuplePushError::First(err), lua)) => Err((err, lua)),
            Err((TuplePushError::Other(_), _)) => unreachable!(),
        }
    }
}

impl<L, K, E> PushOne<L> for HashSet<K>
where
    L: AsLua,
    K: PushOne<LuaState, Err = E> + Eq + Hash,
    K: std::fmt::Debug,
{
}

