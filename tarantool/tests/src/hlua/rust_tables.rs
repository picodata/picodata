use std::collections::{HashMap, HashSet, BTreeMap};
use tarantool::hlua::{
    self,
    AsLua,
    Lua,
    LuaSequence,
    LuaTable,
    LuaTableMap,
    LuaRead,
    AnyLuaValue,
    AnyHashableLuaValue,
    Push,
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

    lua.exec(r#"v = { [-1] = -1, [2] = 2, [42] = 42 }"#).unwrap();

    assert_eq!(lua.get("v"), None::<LuaSequence>);
}

pub fn reading_vec_with_empty_table_works() {
    let lua = Lua::new();

    lua.exec(r#"v = { }"#).unwrap();

    let read: LuaSequence = lua.get("v").unwrap();
    assert!(read.is_empty());
}

pub fn reading_vec_with_complex_indexes_doesnt_work() {
    let lua = Lua::new();

    lua.exec(r#"v = { [-1] = -1, ["foo"] = 2, [{}] = 42 }"#).unwrap();

    assert_eq!(lua.get("v"), None::<LuaSequence>);
}

pub fn reading_heterogenous_vec_works() {
    let lua = Lua::new();

    let orig = [
        AnyLuaValue::LuaNumber(1.),
        AnyLuaValue::LuaBoolean(false),
        AnyLuaValue::LuaNumber(3.),
    ];

    lua.set("v", &orig[..]);

    let read: LuaSequence = lua.get("v").unwrap();
    assert_eq!(read, orig);
}

pub fn reading_vec_set_from_lua_works() {
    let lua = Lua::new();

    lua.exec(r#"v = { 1, 2, 3 }"#).unwrap();

    let read: LuaSequence = lua.get("v").unwrap();
    assert_eq!(
        read,
        [1., 2., 3.].iter().copied()
            .map(|x| AnyLuaValue::LuaNumber(x))
            .collect::<Vec<_>>()
    );

    let read: Vec<i32> = lua.get("v").unwrap();
    assert_eq!(read, vec![1, 2, 3]);

    let read: Vec<u8> = lua.get("v").unwrap();
    assert_eq!(read, vec![1, 2, 3]);

    let read: Vec<f64> = lua.get("v").unwrap();
    assert_eq!(read, vec![1., 2., 3.]);
}

pub fn reading_hashmap_works() {
    let lua = Lua::new();

    let orig: HashMap<i32, f64> = (0..).zip([1., 2., 3.]).collect();
    let orig_copy = orig.clone();
    // Collect to BTreeMap so that iterator yields values in order
    let orig_btree: BTreeMap<_, _> = orig_copy.into_iter().collect();

    lua.set("v", orig);

    let read: LuaTableMap = lua.get("v").unwrap();
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

    lua.exec(r#"v = { [-1] = -1, [2] = 2, [42] = 42 }"#).unwrap();

    let read: LuaTableMap = lua.get("v").unwrap();
    assert_eq!(read[&AnyHashableLuaValue::LuaNumber(-1)], AnyLuaValue::LuaNumber(-1.));
    assert_eq!(read[&AnyHashableLuaValue::LuaNumber(2)], AnyLuaValue::LuaNumber(2.));
    assert_eq!(read[&AnyHashableLuaValue::LuaNumber(42)], AnyLuaValue::LuaNumber(42.));
    assert_eq!(read.len(), 3);

    let read: HashMap<i32, i32> = lua.get("v").unwrap();
    assert_eq!(read[&-1], -1);
    assert_eq!(read[&2], 2);
    assert_eq!(read[&42], 42);
    assert_eq!(read.len(), 3);

    let read: HashMap<i8, f64> = lua.get("v").unwrap();
    assert_eq!(read[&-1], -1.0);
    assert_eq!(read[&2], 2.0);
    assert_eq!(read[&42], 42.0);
    assert_eq!(read.len(), 3);
}

pub fn reading_hashmap_with_empty_table_works() {
    let lua = Lua::new();

    lua.exec(r#"v = { }"#).unwrap();

    let read: LuaTableMap = lua.get("v").unwrap();
    assert!(read.is_empty());
}

pub fn reading_hashmap_with_complex_indexes_works() {
    let lua = Lua::new();

    lua.exec(r#"v = { [-1] = -1, ["foo"] = 2, [2.] = 42 }"#).unwrap();

    let read: LuaTableMap = lua.get("v").unwrap();
    assert_eq!(read[&AnyHashableLuaValue::LuaNumber(-1)], AnyLuaValue::LuaNumber(-1.));
    assert_eq!(read[&AnyHashableLuaValue::LuaString("foo".to_owned())], AnyLuaValue::LuaNumber(2.));
    assert_eq!(read[&AnyHashableLuaValue::LuaNumber(2)], AnyLuaValue::LuaNumber(42.));
    assert_eq!(read.len(), 3);
}

pub fn reading_hashmap_with_floating_indexes_works() {
    let lua = Lua::new();

    lua.exec(r#"v = { [-1.25] = -1, [2.5] = 42 }"#).unwrap();

    let read: LuaTableMap = lua.get("v").unwrap();
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

    lua.exec(r#"v = { [1] = 2, [2] = 3, [3] = 4 }"#).unwrap();

    let read: HashMap<_, _> = lua.get("v").unwrap();
    assert_eq!(
        read,
        [2., 3., 4.].iter().enumerate()
            .map(|(k, v)| (AnyHashableLuaValue::LuaNumber((k + 1) as i32), AnyLuaValue::LuaNumber(*v))).collect::<HashMap<_, _>>());
}

pub fn derive_struct_push() {
    #[derive(Push)]
    struct S {
        i: i32,
        s: String,
        boo: bool,
        table: Vec<u8>,
        r#struct: T,
    }

    #[derive(Push)]
    struct T {
        x: f64,
    }

    let lua = Lua::new();
    let lua = lua.push(
        S {
            i: 69,
            s: "nice".into(),
            boo: true,
            table: vec![11, 12, 13],
            r#struct: T { x: 3.14 },
        }
    );
    let t: LuaTable<_> = lua.read().unwrap();
    assert_eq!(t.get::<i32, _>("i"), Some(69));
    assert_eq!(t.get::<String, _>("s"), Some("nice".into()));
    assert_eq!(t.get::<bool, _>("boo"), Some(true));
    let u: LuaTable<_> = t.get("table").unwrap();
    assert_eq!(u.get::<u8, _>(1), Some(11));
    assert_eq!(u.get::<u8, _>(2), Some(12));
    assert_eq!(u.get::<u8, _>(3), Some(13));
    let v: LuaTable<_> = t.get("r#struct").unwrap();
    assert_eq!(v.get::<f64, _>("x"), Some(3.14));
}

pub fn derive_struct_lua_read() {
    #[derive(Debug, PartialEq, Eq, LuaRead)]
    struct S { i: i32, s: String, boo: bool, o: Option<i32> }

    #[derive(Debug, PartialEq, Eq, LuaRead)]
    struct T { i: i32, s: String }

    let lua = Lua::new();
    lua.exec(r#"t = { i = 69, s = "booboo", boo = true }"#).unwrap();
    let s: S = lua.get("t").unwrap();
    assert_eq!(s, S { i: 69, s: "booboo".into(), boo: true, o: None });

    let t: T = lua.get("t").unwrap();
    assert_eq!(t, T { i: 69, s: "booboo".into() });
}

pub fn derive_enum_push() {
    #[derive(Push)]
    enum E {
        Num(i32),
        Str(String),
        Vec(f32, f32, f32),
        Tuple((f32, f32, f32)),
        S(S),
        Struct {
            i: i32,
            s: String,
        },
    }

    #[derive(Push, LuaRead, PartialEq, Eq, Debug)]
    struct S { foo: i32, bar: String }

    let lua = Lua::new();
    let lua = lua.push(E::Num(69));
    assert_eq!((&lua).read::<i32>().unwrap(), 69);
    let lua = lua.push(E::Str("hello".into()));
    assert_eq!((&lua).read::<String>().unwrap(), "hello");
    let lua = lua.push(E::Vec(3.14, 2.71, 1.62));
    assert_eq!((&lua).read::<(f32, f32, f32)>().unwrap(), (3.14, 2.71, 1.62));
    let lua = lua.push(E::Tuple((2.71, 1.62, 3.14)));
    assert_eq!((&lua).read::<(f32, f32, f32)>().unwrap(), (2.71, 1.62, 3.14));
    let lua = lua.push(E::S(S { foo: 69, bar: "nice".into() }));
    assert_eq!((&lua).read::<S>().unwrap(), S { foo: 69, bar: "nice".into() });
    let lua = lua.push(E::Struct { i: 420, s: "blaze".into() });
    let t: LuaTable<_> = (&lua).read().unwrap();
    assert_eq!(t.get::<i32, _>("i").unwrap(), 420);
    assert_eq!(t.get::<String, _>("s").unwrap(), "blaze");
}

pub fn derive_enum_lua_read() {
    #[derive(LuaRead, Debug, PartialEq)]
    enum E {
        Vec(f32, f32, f32),
        Num(i32),
        Str(String),
        S(S),
        Struct {
            i: i32,
            s: String,
        },
    }

    #[derive(Push, LuaRead, PartialEq, Eq, Debug)]
    struct S { foo: i32, bar: String }

    let lua = Lua::new();
    let res: E = lua.eval("return 7").unwrap();
    assert_eq!(res, E::Num(7));
    let res: E = lua.eval(r#"return "howdy""#).unwrap();
    assert_eq!(res, E::Str("howdy".into()));
    let res: E = lua.eval("return 1.5, 2.5, 3.5").unwrap();
    assert_eq!(res, E::Vec(1.5, 2.5, 3.5));
    let res: E = lua.eval(r#"return { foo = 420, bar = "foo" }"#).unwrap();
    assert_eq!(res, E::S(S { foo: 420, bar: "foo".into() }));
    let res: E = lua.eval(r#"return { i = 69, s = "nice" }"#).unwrap();
    assert_eq!(res, E::Struct { i: 69, s: "nice".into() });

    let lua = lua.push((1, 2, 3));
    assert_eq!((&lua).read::<E>().unwrap(), E::Vec(1., 2., 3.));
    let lua = lua.push(S { foo: 314, bar: "pi".into() });
    assert_eq!((&lua).read::<E>().unwrap(), E::S(S { foo: 314, bar: "pi".into() }));
}

pub fn enum_variants_order_matters() {
    let lua = Lua::new();

    #[derive(LuaRead, PartialEq, Debug)]
    enum A {
        Multiple(f32, f32),
        Single(i32),
    }

    let res: A = lua.eval("return 1, 2").unwrap();
    assert_eq!(res, A::Multiple(1., 2.));

    #[derive(LuaRead, PartialEq, Debug)]
    enum B {
        Single(i32),
        Multiple(f32, f32),
    }

    let res: B = lua.eval("return 1, 2").unwrap();
    assert_eq!(res, B::Single(1));

}

pub fn struct_of_enums_vs_enum_of_structs() {
    let lua = Lua::new();

    lua.exec(r#"v = {
        vec = {
            { x = 69 },
            { y = "hello" },
            { z = true },
        }
    }"#).unwrap();

    let s: S = lua.get("v").unwrap();
    assert_eq!(s,
        S {
            vec: vec![
                V { x: Some(69), .. Default::default() },
                V { y: Some("hello".into()), .. Default::default() },
                V { z: Some(true), .. Default::default() },
            ],
        }
    );

    let t: T = lua.get("v").unwrap();
    assert_eq!(t,
        T {
            vec: vec![
                E::X { x: 69 },
                E::Y { y: "hello".into() },
                E::Z { z: true },
            ],
        }
    );

    #[derive(Debug, PartialEq, LuaRead)]
    struct S { vec: Vec<V> }

    #[derive(Debug, PartialEq, LuaRead, Default)]
    struct V { x: Option<i32>, y: Option<String>, z: Option<bool> }

    #[derive(Debug, PartialEq, LuaRead)]
    struct T { vec: Vec<E> }

    #[derive(Debug, PartialEq, LuaRead)]
    enum E { X { x: i32 }, Y { y: String, }, Z { z: bool } }
}

