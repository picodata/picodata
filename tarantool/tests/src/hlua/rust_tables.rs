use std::collections::{HashMap, HashSet, BTreeMap};
use tarantool::hlua::{
    Lua,
    LuaTable,
    AnyLuaValue,
    AnyHashableLuaValue,
};

pub fn write() {
    let lua = Lua::new();

    lua.set("a", vec![9, 8, 7]);

    let table: LuaTable<_> = lua.get("a").unwrap();

    let values: Vec<(i32, i32)> = table.iter().filter_map(|e| e).collect();
    assert_eq!(values, vec![(1, 9), (2, 8), (3, 7)]);
}

pub fn write_map() {
    let lua = Lua::new();

    let mut map = HashMap::new();
    map.insert(5, 8);
    map.insert(13, 21);
    map.insert(34, 55);

    lua.set("a", map.clone());

    let table: LuaTable<_> = lua.get("a").unwrap();

    let values: HashMap<i32, i32> = table.iter().filter_map(|e| e).collect();
    assert_eq!(values, map);
}

pub fn write_set() {
    let lua = Lua::new();

    let mut set = HashSet::new();
    set.insert(5);
    set.insert(8);
    set.insert(13);
    set.insert(21);
    set.insert(34);
    set.insert(55);

    lua.set("a", set.clone());

    let table: LuaTable<_> = lua.get("a").unwrap();

    let values: HashSet<i32> = table.iter()
        .filter_map(|e| e)
        .map(|(elem, set): (i32, bool)| {
            assert!(set);
            elem
        })
        .collect();

    assert_eq!(values, set);
}

pub fn globals_table() {
    let lua = Lua::new();

    lua.globals_table().set("a", 12);

    let val: i32 = lua.get("a").unwrap();
    assert_eq!(val, 12);
}

pub fn reading_vec_works() {
    let lua = Lua::new();

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

pub fn reading_vec_from_sparse_table_doesnt_work() {
    let lua = Lua::new();

    lua.execute::<()>(r#"v = { [-1] = -1, [2] = 2, [42] = 42 }"#).unwrap();

    let read: Option<Vec<_>> = lua.get("v");
    if read.is_some() {
        panic!("Unexpected success");
    }
}

pub fn reading_vec_with_empty_table_works() {
    let lua = Lua::new();

    lua.execute::<()>(r#"v = { }"#).unwrap();

    let read: Vec<_> = lua.get("v").unwrap();
    assert_eq!(read.len(), 0);
}

pub fn reading_vec_with_complex_indexes_doesnt_work() {
    let lua = Lua::new();

    lua.execute::<()>(r#"v = { [-1] = -1, ["foo"] = 2, [{}] = 42 }"#).unwrap();

    let read: Option<Vec<_>> = lua.get("v");
    if read.is_some() {
        panic!("Unexpected success");
    }
}

pub fn reading_heterogenous_vec_works() {
    let lua = Lua::new();

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

pub fn reading_vec_set_from_lua_works() {
    let lua = Lua::new();

    lua.execute::<()>(r#"v = { 1, 2, 3 }"#).unwrap();

    let read: Vec<_> = lua.get("v").unwrap();
    assert_eq!(
        read,
        [1., 2., 3.].iter()
            .map(|x| AnyLuaValue::LuaNumber(*x)).collect::<Vec<_>>());
}

pub fn reading_hashmap_works() {
    let lua = Lua::new();

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

pub fn reading_hashmap_from_sparse_table_works() {
    let lua = Lua::new();

    lua.execute::<()>(r#"v = { [-1] = -1, [2] = 2, [42] = 42 }"#).unwrap();

    let read: HashMap<_, _> = lua.get("v").unwrap();
    assert_eq!(read[&AnyHashableLuaValue::LuaNumber(-1)], AnyLuaValue::LuaNumber(-1.));
    assert_eq!(read[&AnyHashableLuaValue::LuaNumber(2)], AnyLuaValue::LuaNumber(2.));
    assert_eq!(read[&AnyHashableLuaValue::LuaNumber(42)], AnyLuaValue::LuaNumber(42.));
    assert_eq!(read.len(), 3);
}

pub fn reading_hashmap_with_empty_table_works() {
    let lua = Lua::new();

    lua.execute::<()>(r#"v = { }"#).unwrap();

    let read: HashMap<_, _> = lua.get("v").unwrap();
    assert_eq!(read.len(), 0);
}

pub fn reading_hashmap_with_complex_indexes_works() {
    let lua = Lua::new();

    lua.execute::<()>(r#"v = { [-1] = -1, ["foo"] = 2, [2.] = 42 }"#).unwrap();

    let read: HashMap<_, _> = lua.get("v").unwrap();
    assert_eq!(read[&AnyHashableLuaValue::LuaNumber(-1)], AnyLuaValue::LuaNumber(-1.));
    assert_eq!(read[&AnyHashableLuaValue::LuaString("foo".to_owned())], AnyLuaValue::LuaNumber(2.));
    assert_eq!(read[&AnyHashableLuaValue::LuaNumber(2)], AnyLuaValue::LuaNumber(42.));
    assert_eq!(read.len(), 3);
}

pub fn reading_hashmap_with_floating_indexes_works() {
    let lua = Lua::new();

    lua.execute::<()>(r#"v = { [-1.25] = -1, [2.5] = 42 }"#).unwrap();

    let read: HashMap<_, _> = lua.get("v").unwrap();
    // It works by truncating integers in some unspecified way
    // https://www.lua.org/manual/5.2/manual.html#lua_tointegerx
    assert_eq!(read[&AnyHashableLuaValue::LuaNumber(-1)], AnyLuaValue::LuaNumber(-1.));
    assert_eq!(read[&AnyHashableLuaValue::LuaNumber(2)], AnyLuaValue::LuaNumber(42.));
    assert_eq!(read.len(), 2);
}

pub fn reading_heterogenous_hashmap_works() {
    let lua = Lua::new();

    let mut orig = HashMap::new();
    orig.insert(AnyHashableLuaValue::LuaNumber(42), AnyLuaValue::LuaNumber(42.));
    orig.insert(AnyHashableLuaValue::LuaString("foo".to_owned()), AnyLuaValue::LuaString("foo".to_owned()));
    orig.insert(AnyHashableLuaValue::LuaBoolean(true), AnyLuaValue::LuaBoolean(true));

    let orig_clone = orig.clone();
    lua.set("v", orig);

    let read: HashMap<_, _> = lua.get("v").unwrap();
    assert_eq!(read, orig_clone);
}

pub fn reading_hashmap_set_from_lua_works() {
    let lua = Lua::new();

    lua.execute::<()>(r#"v = { [1] = 2, [2] = 3, [3] = 4 }"#).unwrap();

    let read: HashMap<_, _> = lua.get("v").unwrap();
    assert_eq!(
        read,
        [2., 3., 4.].iter().enumerate()
            .map(|(k, v)| (AnyHashableLuaValue::LuaNumber((k + 1) as i32), AnyLuaValue::LuaNumber(*v))).collect::<HashMap<_, _>>());
}
