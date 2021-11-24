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
    V: PushOne<LuaState>,
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
    V: PushOne<LuaState, Err = E>
{
}

impl<L, K> Push<L> for HashSet<K>
where
    L: AsLua,
    K: PushOne<LuaState> + Eq + Hash
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
    K: PushOne<LuaState, Err = E> + Eq + Hash
{
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet, BTreeMap};
    use crate::{
        Lua,
        LuaTable,
        AnyLuaValue,
        AnyHashableLuaValue,
    };

    #[test]
    fn write() {
        let mut lua = Lua::new();

        lua.set("a", vec![9, 8, 7]);

        let mut table: LuaTable<_> = lua.get("a").unwrap();

        let values: Vec<(i32, i32)> = table.iter().filter_map(|e| e).collect();
        assert_eq!(values, vec![(1, 9), (2, 8), (3, 7)]);
    }

    #[test]
    fn write_map() {
        let mut lua = Lua::new();

        let mut map = HashMap::new();
        map.insert(5, 8);
        map.insert(13, 21);
        map.insert(34, 55);

        lua.set("a", map.clone());

        let mut table: LuaTable<_> = lua.get("a").unwrap();

        let values: HashMap<i32, i32> = table.iter().filter_map(|e| e).collect();
        assert_eq!(values, map);
    }

    #[test]
    fn write_set() {
        let mut lua = Lua::new();

        let mut set = HashSet::new();
        set.insert(5);
        set.insert(8);
        set.insert(13);
        set.insert(21);
        set.insert(34);
        set.insert(55);

        lua.set("a", set.clone());

        let mut table: LuaTable<_> = lua.get("a").unwrap();

        let values: HashSet<i32> = table.iter()
            .filter_map(|e| e)
            .map(|(elem, set): (i32, bool)| {
                assert!(set);
                elem
            })
            .collect();

        assert_eq!(values, set);
    }

    #[test]
    fn globals_table() {
        let mut lua = Lua::new();

        lua.globals_table().set("a", 12);

        let val: i32 = lua.get("a").unwrap();
        assert_eq!(val, 12);
    }

    #[test]
    fn reading_vec_works() {
        let mut lua = Lua::new();

        let orig = [1., 2., 3.];

        lua.set("v", &orig[..]);

        let read: Vec<_> = lua.get("v").unwrap();
        for (o, r) in orig.iter().zip(read.iter()) {
            if let AnyLuaValue::LuaNumber(ref n) = *r {
                assert_eq!(o, n);
            } else {
                panic!("Unexpected variant");
            }
        }
    }

    #[test]
    fn reading_vec_from_sparse_table_doesnt_work() {
        let mut lua = Lua::new();

        lua.execute::<()>(r#"v = { [-1] = -1, [2] = 2, [42] = 42 }"#).unwrap();

        let read: Option<Vec<_>> = lua.get("v");
        if read.is_some() {
            panic!("Unexpected success");
        }
    }

    #[test]
    fn reading_vec_with_empty_table_works() {
        let mut lua = Lua::new();

        lua.execute::<()>(r#"v = { }"#).unwrap();

        let read: Vec<_> = lua.get("v").unwrap();
        assert_eq!(read.len(), 0);
    }

    #[test]
    fn reading_vec_with_complex_indexes_doesnt_work() {
        let mut lua = Lua::new();

        lua.execute::<()>(r#"v = { [-1] = -1, ["foo"] = 2, [{}] = 42 }"#).unwrap();

        let read: Option<Vec<_>> = lua.get("v");
        if read.is_some() {
            panic!("Unexpected success");
        }
    }

    #[test]
    fn reading_heterogenous_vec_works() {
        let mut lua = Lua::new();

        let orig = [
            AnyLuaValue::LuaNumber(1.),
            AnyLuaValue::LuaBoolean(false),
            AnyLuaValue::LuaNumber(3.),
            // Pushing String to and reading it from makes it a number
            //AnyLuaValue::LuaString(String::from("3"))
        ];

        lua.set("v", &orig[..]);

        let read: Vec<_> = lua.get("v").unwrap();
        assert_eq!(read, orig);
    }

    #[test]
    fn reading_vec_set_from_lua_works() {
        let mut lua = Lua::new();

        lua.execute::<()>(r#"v = { 1, 2, 3 }"#).unwrap();

        let read: Vec<_> = lua.get("v").unwrap();
        assert_eq!(
            read,
            [1., 2., 3.].iter()
                .map(|x| AnyLuaValue::LuaNumber(*x)).collect::<Vec<_>>());
    }

    #[test]
    fn reading_hashmap_works() {
        let mut lua = Lua::new();

        let orig: HashMap<i32, f64> = (0..).zip([1., 2., 3.]).collect();
        let orig_copy = orig.clone();
        // Collect to BTreeMap so that iterator yields values in order
        let orig_btree: BTreeMap<_, _> = orig_copy.into_iter().collect();

        lua.set("v", orig);

        let read: HashMap<AnyHashableLuaValue, AnyLuaValue> = lua.get("v").unwrap();
        // Same as above
        let read_btree: BTreeMap<_, _> = read.into_iter().collect();
        for (o, r) in orig_btree.iter().zip(read_btree.iter()) {
            if let (&AnyHashableLuaValue::LuaNumber(i), &AnyLuaValue::LuaNumber(n)) = r {
                let (&o_i, &o_n) = o;
                assert_eq!(o_i, i);
                assert_eq!(o_n, n);
            } else {
                panic!("Unexpected variant");
            }
        }
    }

    #[test]
    fn reading_hashmap_from_sparse_table_works() {
        let mut lua = Lua::new();

        lua.execute::<()>(r#"v = { [-1] = -1, [2] = 2, [42] = 42 }"#).unwrap();

        let read: HashMap<_, _> = lua.get("v").unwrap();
        assert_eq!(read[&AnyHashableLuaValue::LuaNumber(-1)], AnyLuaValue::LuaNumber(-1.));
        assert_eq!(read[&AnyHashableLuaValue::LuaNumber(2)], AnyLuaValue::LuaNumber(2.));
        assert_eq!(read[&AnyHashableLuaValue::LuaNumber(42)], AnyLuaValue::LuaNumber(42.));
        assert_eq!(read.len(), 3);
    }

    #[test]
    fn reading_hashmap_with_empty_table_works() {
        let mut lua = Lua::new();

        lua.execute::<()>(r#"v = { }"#).unwrap();

        let read: HashMap<_, _> = lua.get("v").unwrap();
        assert_eq!(read.len(), 0);
    }

    #[test]
    fn reading_hashmap_with_complex_indexes_works() {
        let mut lua = Lua::new();

        lua.execute::<()>(r#"v = { [-1] = -1, ["foo"] = 2, [2.] = 42 }"#).unwrap();

        let read: HashMap<_, _> = lua.get("v").unwrap();
        assert_eq!(read[&AnyHashableLuaValue::LuaNumber(-1)], AnyLuaValue::LuaNumber(-1.));
        assert_eq!(read[&AnyHashableLuaValue::LuaString("foo".to_owned())], AnyLuaValue::LuaNumber(2.));
        assert_eq!(read[&AnyHashableLuaValue::LuaNumber(2)], AnyLuaValue::LuaNumber(42.));
        assert_eq!(read.len(), 3);
    }

    #[test]
    fn reading_hashmap_with_floating_indexes_works() {
        let mut lua = Lua::new();

        lua.execute::<()>(r#"v = { [-1.25] = -1, [2.5] = 42 }"#).unwrap();

        let read: HashMap<_, _> = lua.get("v").unwrap();
        // It works by truncating integers in some unspecified way
        // https://www.lua.org/manual/5.2/manual.html#lua_tointegerx
        assert_eq!(read[&AnyHashableLuaValue::LuaNumber(-1)], AnyLuaValue::LuaNumber(-1.));
        assert_eq!(read[&AnyHashableLuaValue::LuaNumber(2)], AnyLuaValue::LuaNumber(42.));
        assert_eq!(read.len(), 2);
    }

    #[test]
    fn reading_heterogenous_hashmap_works() {
        let mut lua = Lua::new();

        let mut orig = HashMap::new();
        orig.insert(AnyHashableLuaValue::LuaNumber(42), AnyLuaValue::LuaNumber(42.));
        orig.insert(AnyHashableLuaValue::LuaString("foo".to_owned()), AnyLuaValue::LuaString("foo".to_owned()));
        orig.insert(AnyHashableLuaValue::LuaBoolean(true), AnyLuaValue::LuaBoolean(true));

        let orig_clone = orig.clone();
        lua.set("v", orig);

        let read: HashMap<_, _> = lua.get("v").unwrap();
        assert_eq!(read, orig_clone);
    }

    #[test]
    fn reading_hashmap_set_from_lua_works() {
        let mut lua = Lua::new();

        lua.execute::<()>(r#"v = { [1] = 2, [2] = 3, [3] = 4 }"#).unwrap();

        let read: HashMap<_, _> = lua.get("v").unwrap();
        assert_eq!(
            read,
            [2., 3., 4.].iter().enumerate()
                .map(|(k, v)| (AnyHashableLuaValue::LuaNumber((k + 1) as i32), AnyLuaValue::LuaNumber(*v))).collect::<HashMap<_, _>>());
    }
}
