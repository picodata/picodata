use crate::{
    ffi,
    lua_tables::LuaTable,
    tuples::TuplePushError::{self, First, Other},
    AsLua, LuaRead, LuaState, Push, PushGuard, PushInto, PushOne, PushOneInto, ReadResult, Void,
    WrongType,
};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::iter;
use std::num::NonZeroI32;

#[inline]
pub(crate) fn push_iter<L, I>(lua: L, iterator: I) -> Result<PushGuard<L>, (PushIterErrorOf<I>, L)>
where
    L: AsLua,
    I: Iterator,
    <I as Iterator>::Item: PushInto<LuaState>,
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
                return Err((PushIterError::ValuePushError(err), lua));
            },
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
                return Err((PushIterError::TooManyValues(n), lua));
            },
        }
    }

    unsafe { Ok(PushGuard::new(lua, 1)) }
}

pub type PushIterErrorOf<I> = PushIterError<<<I as Iterator>::Item as PushInto<LuaState>>::Err>;

#[derive(Debug, PartialEq, Eq)]
pub enum PushIterError<E> {
    TooManyValues(i32),
    ValuePushError(E),
}

impl<E> PushIterError<E> {
    pub fn map<F, R>(self, f: F) -> PushIterError<R>
    where
        F: FnOnce(E) -> R,
    {
        match self {
            Self::ValuePushError(e) => PushIterError::ValuePushError(f(e)),
            Self::TooManyValues(n) => PushIterError::TooManyValues(n),
        }
    }
}

impl<E> fmt::Display for PushIterError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::TooManyValues(n) => {
                write!(
                    fmt,
                    "Can only push 1 or 2 values as lua table item, got {} instead",
                    n,
                )
            }
            Self::ValuePushError(e) => {
                write!(fmt, "Pushing iterable item failed: {}", e)
            }
        }
    }
}

// NOTE: only the following From<_> for Void implementations are correct,
//       don't add other ones!

// T::Err: Void => no error possible
// NOTE: making this one generic would conflict with the below implementations.
impl From<PushIterError<Void>> for Void {
    fn from(_: PushIterError<Void>) -> Self {
        unreachable!("no way to create instance of Void")
    }
}

// T::Err: Void; (T,) => no error possible
impl<T> From<PushIterError<TuplePushError<T, Void>>> for Void
where
    T: Into<Void>,
{
    fn from(_: PushIterError<TuplePushError<T, Void>>) -> Self {
        unreachable!("no way to create instance of Void")
    }
}

// K::Err: Void; V::Err: Void; (K, V) => no error possible
impl<K, V> From<PushIterError<TuplePushError<K, TuplePushError<V, Void>>>> for Void
where
    K: Into<Void>,
    V: Into<Void>,
{
    fn from(_: PushIterError<TuplePushError<K, TuplePushError<V, Void>>>) -> Self {
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
/// ```no_run
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
{
}

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
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> ReadResult<Self, L> {
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

        {
            let mut iter = table.iter::<i32, T>();
            while let Some(maybe_kv) = iter.next() {
                let (key, value) = crate::unwrap_ok_or! { maybe_kv,
                    Err(e) => {
                        drop(iter);
                        let lua = table.into_inner();
                        let e = e.when("converting Lua table to Vec<_>")
                            .expected_type::<Self>();
                        return Err((lua, e))
                    }
                };
                max_key = max_key.max(key);
                min_key = min_key.min(key);
                dict.insert(key, value);
            }
        }

        if dict.is_empty() {
            return Ok(vec![]);
        }

        if min_key != 1 {
            // Rust doesn't support sparse arrays or arrays with negative
            // indices
            let e = WrongType::info("converting Lua table to Vec<_>")
                .expected("indexes in range 1..N")
                .actual(format!("value with index {}", min_key));
            return Err((table.into_inner(), e));
        }

        let mut result = Vec::with_capacity(max_key as _);

        // We expect to start with first element of table and have this
        // be smaller that first key by one
        let mut previous_key = 0;

        // By this point, we actually iterate the map to move values to Vec
        // and check that table represented non-sparse 1-indexed array
        for (k, v) in dict {
            if previous_key + 1 != k {
                let e = WrongType::info("converting Lua table to Vec<_>")
                    .expected("indexes in range 1..N")
                    .actual(format!("Lua table with missing index {}", previous_key + 1));
                return Err((table.into_inner(), e));
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

impl<L, T, const N: usize> LuaRead<L> for [T; N]
where
    L: AsLua,
    T: for<'a> LuaRead<PushGuard<&'a LuaTable<L>>>,
    T: 'static,
{
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> ReadResult<Self, L> {
        let table = match LuaTable::lua_read_at_position(lua, index) {
            Ok(table) => table,
            Err(lua) => return Err(lua),
        };

        let mut res = std::mem::MaybeUninit::uninit();
        let ptr = &mut res as *mut _ as *mut [T; N] as *mut T;
        let mut was_assigned = [false; N];
        let mut err = None;

        for maybe_kv in table.iter::<i32, T>() {
            match maybe_kv {
                Ok((key, value)) if 1 <= key && key as usize <= N => {
                    let i = (key - 1) as usize;
                    unsafe { std::ptr::write(ptr.add(i), value) }
                    was_assigned[i] = true;
                }
                Err(e) => {
                    err = Some(Error::Subtype(e));
                    break;
                }
                Ok((index, _)) => {
                    err = Some(Error::WrongIndex(index));
                    break;
                }
            }
        }

        if err.is_none() {
            err = was_assigned
                .iter()
                .zip(1..)
                .find(|(&was_assigned, _)| !was_assigned)
                .map(|(_, i)| Error::MissingIndex(i));
        }

        let err = crate::unwrap_or! { err,
            return Ok(unsafe { res.assume_init() });
        };

        for i in IntoIterator::into_iter(was_assigned)
            .enumerate()
            .flat_map(|(i, was_assigned)| was_assigned.then(|| i))
        {
            unsafe { std::ptr::drop_in_place(ptr.add(i)) }
        }

        let when = "converting Lua table to array";
        let e = match err {
            Error::Subtype(err) => err.when(when).expected_type::<Self>(),
            Error::WrongIndex(index) => WrongType::info(when)
                .expected(format!("indexes in range 1..={}", N))
                .actual(format!("value with index {}", index)),
            Error::MissingIndex(index) => WrongType::info(when)
                .expected(format!("indexes in range 1..={}", N))
                .actual(format!("Lua table with missing index {}", index)),
        };
        return Err((table.into_inner(), e));

        enum Error {
            Subtype(WrongType),
            WrongIndex(i32),
            MissingIndex(i32),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
/// HashMap
////////////////////////////////////////////////////////////////////////////////

impl<L, K, V, S> LuaRead<L> for HashMap<K, V, S>
where
    L: AsLua,
    K: 'static + Hash + Eq,
    K: for<'k> LuaRead<&'k LuaTable<L>>,
    V: 'static,
    V: for<'v> LuaRead<PushGuard<&'v LuaTable<L>>>,
    S: Default,
    S: std::hash::BuildHasher,
{
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> ReadResult<Self, L> {
        let table = LuaTable::lua_read_at_position(lua, index)?;
        let res: Result<_, _> = table.iter().collect();
        res.map_err(|err| {
            let l = table.into_inner();
            let e = err
                .when("converting Lua table to HashMap<_, _>")
                .expected_type::<Self>();
            (l, e)
        })
    }
}

macro_rules! push_hashmap_impl {
    ($self:expr, $lua:expr) => {
        push_iter($lua, $self.into_iter()).map_err(|(e, lua)| match e {
            PushIterError::TooManyValues(_) => unreachable!("K and V implement PushOne"),
            PushIterError::ValuePushError(First(e)) => (First(e), lua),
            PushIterError::ValuePushError(Other(e)) => (Other(e.first()), lua),
        })
    };
}

impl<L, K, V, S> Push<L> for HashMap<K, V, S>
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

impl<L, K, V, S> PushOne<L> for HashMap<K, V, S>
where
    L: AsLua,
    K: PushOne<LuaState> + Eq + Hash + Debug,
    V: PushOne<LuaState> + Debug,
{
}

impl<L, K, V, S> PushInto<L> for HashMap<K, V, S>
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

impl<L, K, V, S> PushOneInto<L> for HashMap<K, V, S>
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
        push_iter($lua, $self.into_iter().zip(iter::repeat(true))).map_err(|(e, lua)| match e {
            PushIterError::TooManyValues(_) => unreachable!("K implements PushOne"),
            PushIterError::ValuePushError(First(e)) => (e, lua),
            PushIterError::ValuePushError(Other(_)) => {
                unreachable!("no way to create instance of Void")
            }
        })
    };
}

impl<L, K, S> Push<L> for HashSet<K, S>
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

impl<L, K, S> PushOne<L> for HashSet<K, S>
where
    L: AsLua,
    K: PushOne<LuaState> + Eq + Hash + Debug,
{
}

impl<L, K, S> PushInto<L> for HashSet<K, S>
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

impl<L, K, S> PushOneInto<L> for HashSet<K, S>
where
    L: AsLua,
    K: PushOneInto<LuaState> + Eq + Hash + Debug,
{
}
