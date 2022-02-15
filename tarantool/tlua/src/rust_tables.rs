use crate::{
    ffi,
    Push,
    PushInto,
    PushGuard,
    PushOne,
    PushOneInto,
    AsLua,
    tuples::TuplePushError::{self, First, Other},
    LuaRead,
    LuaState,
    lua_tables::LuaTable,
    Void,
};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::iter;
use std::num::NonZeroI32;

#[inline]
pub(crate) fn push_iter<L, I>(lua: L, iterator: I)
    -> Result<PushGuard<L>, (PushIterErrorOf<I>, L)>
where
    L: AsLua,
    I: Iterator,
    <I as Iterator>::Item: PushInto<LuaState>
{
    // creating empty table
    unsafe { ffi::lua_newtable(lua.as_lua()) };

    for (elem, index) in iterator.zip(1..) {
        let size = match elem.push_into_lua(lua.as_lua()) {
            Ok(pushed) => pushed.forget_internal(),
            Err((err, _)) => unsafe {
                // TODO(gmoshkin): return an error capturing this push guard
                // drop the lua table
                drop(PushGuard::new(lua.as_lua(), 1));
                return Err((PushIterError::ValuePushError(err), lua))
            }
        };

        match size {
            0 => continue,
            1 => {
                lua.as_lua().push_one(index).forget_internal();
                unsafe { ffi::lua_insert(lua.as_lua(), -2) }
                unsafe { ffi::lua_settable(lua.as_lua(), -3) }
            }
            2 => unsafe { ffi::lua_settable(lua.as_lua(), -3) },
            n => unsafe {
                // TODO(gmoshkin): return an error capturing this push guard
                // n + 1 == n values from the recent push + lua table
                drop(PushGuard::new(lua.as_lua(), n + 1));
                return Err((PushIterError::TooManyValues, lua))
            }
        }
    }

    unsafe {
        Ok(PushGuard::new(lua, 1))
    }
}

pub type PushIterErrorOf<I> = PushIterError<<<I as Iterator>::Item as PushInto<LuaState>>::Err>;

#[derive(Debug, PartialEq, Eq)]
pub enum PushIterError<E> {
    TooManyValues,
    ValuePushError(E),
}

// Note: only the following From<_> for Void implementations are correct,
//       don't add other ones!

// T::Err: Void => no error possible
impl From<PushIterError<Void>> for Void {
    fn from(_: PushIterError<Void>) -> Self {
        unreachable!("no way to create instance of Void")
    }
}

// T::Err: Void; (T,) => no error possible
impl From<PushIterError<TuplePushError<Void, Void>>> for Void {
    fn from(_: PushIterError<TuplePushError<Void, Void>>) -> Self {
        unreachable!("no way to create instance of Void")
    }
}

// T::Err: Void; U::Err: Void; (T, U) => no error possible
impl From<PushIterError<TuplePushError<Void, TuplePushError<Void, Void>>>> for Void {
    fn from(_: PushIterError<TuplePushError<Void, TuplePushError<Void, Void>>>) -> Self {
        unreachable!("no way to create instance of Void")
    }
}

////////////////////////////////////////////////////////////////////////////////
// TableFromIter
////////////////////////////////////////////////////////////////////////////////

/// A wrapper struct for converting arbitrary iterators into lua tables. Use
/// this instead of converting the iterator into a `Vec` to avoid unnecessary
/// allocations
/// # Example
/// ```rust
/// use std::io::BufRead;
/// let lua = tlua::Lua::new();
/// lua.set(
///     "foo",
///     tlua::TableFromIter(std::io::stdin().lock().lines().flatten()),
/// )
/// // Global variable 'foo' now contains an array of lines read from stdin
/// ```
pub struct TableFromIter<I>(pub I);

impl<L, I> PushInto<L> for TableFromIter<I>
where
    L: AsLua,
    I: Iterator,
    <I as Iterator>::Item: PushInto<LuaState>,
{
    type Err = PushIterError<<I::Item as PushInto<LuaState>>::Err>;

    fn push_into_lua(self, lua: L) -> crate::PushIntoResult<L, Self> {
        push_iter(lua, self.0)
    }
}

impl<L, I> PushOneInto<L> for TableFromIter<I>
where
    L: AsLua,
    I: Iterator,
    <I as Iterator>::Item: PushInto<LuaState>,
{}

////////////////////////////////////////////////////////////////////////////////
/// Vec
////////////////////////////////////////////////////////////////////////////////

impl<L, T> Push<L> for Vec<T>
where
    L: AsLua,
    T: Push<LuaState>,
{
    type Err = PushIterError<T::Err>;

    #[inline]
    fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        push_iter(lua, self.iter())
    }
}

impl<L, T> PushOne<L> for Vec<T>
where
    L: AsLua,
    T: Push<LuaState>,
{
}

impl<L, T> PushInto<L> for Vec<T>
where
    L: AsLua,
    T: PushInto<LuaState>,
{
    type Err = PushIterError<T::Err>;

    #[inline]
    fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        push_iter(lua, self.into_iter())
    }
}

impl<L, T> PushOneInto<L> for Vec<T>
where
    L: AsLua,
    T: PushInto<LuaState>,
{
}

impl<L, T> LuaRead<L> for Vec<T>
where
    L: AsLua,
    T: for<'a> LuaRead<PushGuard<&'a LuaTable<L>>>,
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

////////////////////////////////////////////////////////////////////////////////
/// \[T]
////////////////////////////////////////////////////////////////////////////////

impl<L, T> Push<L> for [T]
where
    L: AsLua,
    T: Push<LuaState>,
{
    type Err = PushIterError<T::Err>;

    #[inline]
    fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        push_iter(lua, self.iter())
    }
}

impl<L, T> PushOne<L> for [T]
where
    L: AsLua,
    T: Push<LuaState>,
{
}

////////////////////////////////////////////////////////////////////////////////
/// [T; N]
////////////////////////////////////////////////////////////////////////////////

impl<L, T, const N: usize> Push<L> for [T; N]
where
    L: AsLua,
    T: Push<LuaState>,
{
    type Err = PushIterError<T::Err>;

    #[inline]
    fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        push_iter(lua, self.iter())
    }
}

impl<L, T, const N: usize> PushOne<L> for [T; N]
where
    L: AsLua,
    T: Push<LuaState>,
{
}

impl<L, T, const N: usize> PushInto<L> for [T; N]
where
    L: AsLua,
    T: PushInto<LuaState>,
{
    type Err = PushIterError<T::Err>;

    #[inline]
    fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        push_iter(lua, IntoIterator::into_iter(self))
    }
}

impl<L, T, const N: usize> PushOneInto<L> for [T; N]
where
    L: AsLua,
    T: PushInto<LuaState>,
{
}

////////////////////////////////////////////////////////////////////////////////
/// HashMap
////////////////////////////////////////////////////////////////////////////////

impl<L, K, V> LuaRead<L> for HashMap<K, V>
where
    L: AsLua,
    K: 'static + Hash + Eq,
    K: for<'k> LuaRead<&'k LuaTable<L>>,
    V: 'static,
    V: for<'v> LuaRead<PushGuard<&'v LuaTable<L>>>,
{
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Self, L> {
        let table = LuaTable::lua_read_at_position(lua, index)?;
        Ok(table.iter().flatten().collect())
    }
}

macro_rules! push_hashmap_impl {
    ($self:expr, $lua:expr) => {
        push_iter($lua, $self.into_iter())
            .map_err(|(e, lua)| match e {
                PushIterError::TooManyValues => unreachable!("K and V implement PushOne"),
                PushIterError::ValuePushError(First(e)) => (First(e), lua),
                PushIterError::ValuePushError(Other(e)) => (Other(e.first()), lua),
            })
    }
}

impl<L, K, V> Push<L> for HashMap<K, V>
where
    L: AsLua,
    K: PushOne<LuaState> + Eq + Hash + Debug,
    V: PushOne<LuaState> + Debug,
{
    type Err = TuplePushError<K::Err, V::Err>;

    #[inline]
    fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        push_hashmap_impl!(self, lua)
    }
}

impl<L, K, V> PushOne<L> for HashMap<K, V>
where
    L: AsLua,
    K: PushOne<LuaState> + Eq + Hash + Debug,
    V: PushOne<LuaState> + Debug,
{
}

impl<L, K, V> PushInto<L> for HashMap<K, V>
where
    L: AsLua,
    K: PushOneInto<LuaState> + Eq + Hash + Debug,
    V: PushOneInto<LuaState> + Debug,
{
    type Err = TuplePushError<K::Err, V::Err>;

    #[inline]
    fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        push_hashmap_impl!(self, lua)
    }
}

impl<L, K, V> PushOneInto<L> for HashMap<K, V>
where
    L: AsLua,
    K: PushOneInto<LuaState> + Eq + Hash + Debug,
    V: PushOneInto<LuaState> + Debug,
{
}

////////////////////////////////////////////////////////////////////////////////
/// HashSet
////////////////////////////////////////////////////////////////////////////////

macro_rules! push_hashset_impl {
    ($self:expr, $lua:expr) => {
        push_iter($lua, $self.into_iter().zip(iter::repeat(true)))
            .map_err(|(e, lua)| match e {
                PushIterError::TooManyValues => unreachable!("K implements PushOne"),
                PushIterError::ValuePushError(First(e)) => (e, lua),
                PushIterError::ValuePushError(Other(_)) => {
                    unreachable!("no way to create instance of Void")
                }
            })
    }
}

impl<L, K> Push<L> for HashSet<K>
where
    L: AsLua,
    K: PushOne<LuaState> + Eq + Hash + Debug,
{
    type Err = K::Err;

    #[inline]
    fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (K::Err, L)> {
        push_hashset_impl!(self, lua)
    }
}

impl<L, K> PushOne<L> for HashSet<K>
where
    L: AsLua,
    K: PushOne<LuaState> + Eq + Hash + Debug,
{
}

impl<L, K> PushInto<L> for HashSet<K>
where
    L: AsLua,
    K: PushOneInto<LuaState> + Eq + Hash + Debug,
{
    type Err = K::Err;

    #[inline]
    fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (K::Err, L)> {
        push_hashset_impl!(self, lua)
    }
}

impl<L, K> PushOneInto<L> for HashSet<K>
where
    L: AsLua,
    K: PushOneInto<LuaState> + Eq + Hash + Debug,
{
}

