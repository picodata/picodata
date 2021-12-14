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
    PushInto,
    PushGuard,
    PushOne,
    TuplePushError,
};
use crate::common::BoolExt;

pub fn push_vec() {
    let lua = Lua::new();

    let orig_vec = vec![9, 8, 7];

    // By reference
    let table: LuaTable<_> = (&lua).push(&orig_vec).read().unwrap();
    let values = table.iter::<i32, i32>().flatten().map(|(_, v)| v).collect::<Vec<_>>();
    assert_eq!(values, orig_vec);

    // By value
    let table: LuaTable<_> = (&lua).push(orig_vec.clone()).read().unwrap();
    let values = table.iter::<i32, i32>().flatten().map(|(_, v)| v).collect::<Vec<_>>();
    assert_eq!(values, orig_vec);
}

pub fn push_hashmap() {
    let lua = Lua::new();

    let mut orig_map = HashMap::new();
    orig_map.insert(5, 8);
    orig_map.insert(13, 21);
    orig_map.insert(34, 55);

    // By reference
    let table: LuaTable<_> = (&lua).push(&orig_map).read().unwrap();
    let values: HashMap<i32, i32> = table.iter().flatten().collect();
    assert_eq!(values, orig_map);

    // By value
    let table: LuaTable<_> = (&lua).push(orig_map.clone()).read().unwrap();
    let values: HashMap<i32, i32> = table.iter().flatten().collect();
    assert_eq!(values, orig_map);
}

pub fn push_hashset() {
    let lua = Lua::new();

    let mut orig_set = HashSet::new();
    orig_set.insert(5);
    orig_set.insert(8);
    orig_set.insert(13);
    orig_set.insert(21);
    orig_set.insert(34);
    orig_set.insert(55);

    // Be reference
    let table: LuaTable<_> = (&lua).push(&orig_set).read().unwrap();
    let values: HashSet<i32> = table.iter::<i32, bool>().flatten()
        .filter_map(|(v, is_set)| is_set.as_some(v)).collect();
    assert_eq!(values, orig_set);

    // Be value
    let table: LuaTable<_> = (&lua).push(orig_set.clone()).read().unwrap();
    let values: HashSet<i32> = table.iter::<i32, bool>().flatten()
        .filter_map(|(v, is_set)| is_set.as_some(v)).collect();
    assert_eq!(values, orig_set);
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
    // Collect to BTreeMap so that iterator yields values in order
    let orig_btree: BTreeMap<_, _> = orig.iter().map(|(&k, &v)| (k, v)).collect();

    lua.set("v", &orig);

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

    lua.set("v", &orig);

    let read: HashMap<_, _> = lua.get("v").unwrap();
    assert_eq!(read, orig);
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
        &S {
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
    let v: LuaTable<_> = t.get("struct").unwrap();
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
    let lua = lua.push(&E::Num(69));
    assert_eq!((&lua).read::<i32>().unwrap(), 69);
    let lua = lua.push(&E::Str("hello".into()));
    assert_eq!((&lua).read::<String>().unwrap(), "hello");
    let lua = lua.push(&E::Vec(3.14, 2.71, 1.62));
    assert_eq!((&lua).read::<(f32, f32, f32)>().unwrap(), (3.14, 2.71, 1.62));
    let lua = lua.push(&E::Tuple((2.71, 1.62, 3.14)));
    assert_eq!((&lua).read::<(f32, f32, f32)>().unwrap(), (2.71, 1.62, 3.14));
    let lua = lua.push(&E::S(S { foo: 69, bar: "nice".into() }));
    assert_eq!((&lua).read::<S>().unwrap(), S { foo: 69, bar: "nice".into() });
    let lua = lua.push(&E::Struct { i: 420, s: "blaze".into() });
    let t: LuaTable<_> = (&lua).read().unwrap();
    assert_eq!(t.get::<i32, _>("i").unwrap(), 420);
    assert_eq!(t.get::<String, _>("s").unwrap(), "blaze");
}

pub fn derive_push_into() {
    #[derive(PushInto)]
    enum E {
        Num(i32),
        Str(String),
        Vec(f32, f32, f32),
        Tuple((f32, f32, f32)),
        S(S),
        Struct {
            i: i32,
            s: String,
            b: bool,
        },
        Hello,
        Goodbye,
    }

    #[derive(PushInto, LuaRead, PartialEq, Eq, Debug)]
    struct S { foo: i32, bar: String }

    let lua = Lua::new();
    let lua = lua.push(E::Num(69));
    assert_eq!((&lua).read().ok(), Some(69));

    let lua = lua.push(E::Str("hello".into()));
    assert_eq!((&lua).read().ok(), Some("hello".to_string()));

    let lua = lua.push(E::Vec(3.14, 2.71, 1.62));
    assert_eq!((&lua).read().ok(), Some((3.14f32, 2.71f32, 1.62f32)));

    let lua = lua.push(E::Tuple((2.71, 1.62, 3.14)));
    assert_eq!((&lua).read().ok(), Some((2.71f32, 1.62f32, 3.14f32)));

    let lua = lua.push(E::S(S { foo: 69, bar: "nice".into() }));
    assert_eq!((&lua).read().ok(), Some(S { foo: 69, bar: "nice".into() }));

    let lua = lua.push(E::Struct { i: 420, s: "blaze".into(), b: true });
    let t: LuaTable<_> = (&lua).read().unwrap();
    assert_eq!(t.get("i"), Some(420));
    assert_eq!(t.get("s"), Some("blaze".to_string()));

    let lua = lua.push(E::Hello);
    assert_eq!((&lua).read().ok(), Some("hello".to_string()));

    let lua = lua.push(E::Goodbye);
    assert_eq!((&lua).read().ok(), Some("goodbye".to_string()));
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
    let lua = lua.push(&S { foo: 314, bar: "pi".into() });
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

pub fn derive_unit_structs_lua_read() {
    #[derive(LuaRead, Debug, PartialEq, Eq)]
    enum E {
        A,
        Foo,
        XXX,
    }

    let lua = Lua::new();
    assert_eq!((&lua).push("a").read().ok(), Some(E::A));
    assert_eq!((&lua).push("A").read().ok(), Some(E::A));
    assert_eq!((&lua).push("FOO").read().ok(), Some(E::Foo));
    assert_eq!((&lua).push("Foo").read().ok(), Some(E::Foo));
    assert_eq!((&lua).push("fo").read().ok(), None::<E>);
    assert_eq!((&lua).push("foo").read().ok(), Some(E::Foo));
    assert_eq!((&lua).push("fooo").read().ok(), None::<E>);
    assert_eq!((&lua).push("XXX").read().ok(), Some(E::XXX));
    assert_eq!((&lua).push("Xxx").read().ok(), Some(E::XXX));
    assert_eq!((&lua).push("xxx").read().ok(), Some(E::XXX));
    assert_eq!((&lua).push("f_oo").read().ok(), None::<E>);

    #[derive(LuaRead, Debug, PartialEq, Eq)]
    struct QueryResult {
        metadata: Vec<Column>,
        rows: Vec<Vec<Value>>,
    }

    #[derive(LuaRead, Debug, PartialEq, Eq)]
    struct Column {
        name: String,
        r#type: Type,
    }

    #[derive(LuaRead, Debug, PartialEq, Eq)]
    enum Type {
        Boolean,
        Integer,
        String,
    }

    #[derive(LuaRead, Debug, PartialEq, Eq)]
    enum Value {
        Boolean(bool),
        Null(hlua::Null),
        Number(u64),
        String(String),
    }

    let lua = tarantool::global_lua();
    let v: QueryResult = lua.eval("return {
        metadata = {
            {
                name = 'id',
                type = 'integer'
            },
            {
                name = 'name',
                type = 'string'
            },
            {
                name = 'product_units',
                type = 'integer'
            }
        },
        rows = {
            {1, '123', box.NULL}
        }
    }").unwrap();

    assert_eq!(
        v,
        QueryResult {
            metadata: vec![
                Column { name: "id".into(), r#type: Type::Integer },
                Column { name: "name".into(), r#type: Type::String },
                Column { name: "product_units".into(), r#type: Type::Integer },
            ],
            rows: vec![
                vec![
                    Value::Number(1),
                    Value::String("123".into()),
                    Value::Null(hlua::Null),
                ]
            ]
        }
    );
}

pub fn derive_unit_structs_push() {
    #[derive(Push, Debug, PartialEq, Eq)]
    enum E {
        A,
        Foo,
        XXX,
    }

    let lua = Lua::new();
    let lua = lua.push(&E::A);
    assert_eq!((&lua).read().ok(), Some(hlua::Typename("string")));
    assert_eq!((&lua).read().ok(), Some("a".to_string()));
    let lua = lua.into_inner().push(&E::Foo);
    assert_eq!((&lua).read().ok(), Some(hlua::Typename("string")));
    assert_eq!((&lua).read().ok(), Some("foo".to_string()));
    let lua = lua.into_inner().push(&E::XXX);
    assert_eq!((&lua).read().ok(), Some(hlua::Typename("string")));
    assert_eq!((&lua).read().ok(), Some("xxx".to_string()));
}

pub fn error_during_push_iter() {
    #[derive(Debug, PartialEq, Eq)]
    struct CustomError;
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct S;
    impl<L: AsLua> Push<L> for S {
        type Err = CustomError;
        fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (CustomError, L)> {
            Err((CustomError, lua))
        }
    }
    impl<L: AsLua> PushOne<L> for S {}

    let lua = Lua::new();

    let lua = {
        let _guard = crate::common::LuaStackIntegrityGuard::new("push_vec_error");
        let (e, lua) = lua.try_push(&vec![S]).unwrap_err();
        assert_eq!(e, hlua::PushIterError::ValuePushError(CustomError));
        lua
    };

    let lua = {
        let _guard = crate::common::LuaStackIntegrityGuard::new("push_hashmap_key_error");
        let mut hm = HashMap::new();
        hm.insert(S, 1);
        let (e, lua) = lua.try_push(&hm).unwrap_err();
        assert_eq!(e, TuplePushError::First(CustomError));
        lua
    };

    let lua = {
        let _guard = crate::common::LuaStackIntegrityGuard::new("push_hashmap_value_error");
        let mut hm = HashMap::new();
        hm.insert(1, S);
        let (e, lua) = lua.try_push(&hm).unwrap_err();
        assert_eq!(e, TuplePushError::Other(CustomError));
        lua
    };

    let lua = {
        let _guard = crate::common::LuaStackIntegrityGuard::new("push_hashset_error");
        let mut hm = HashSet::new();
        hm.insert(S);
        let (e, lua) = lua.try_push(&hm).unwrap_err();
        assert_eq!(e, CustomError);
        lua
    };

    let lua = {
        let _guard = crate::common::LuaStackIntegrityGuard::new("push_iter_error");
        let (e, lua) = lua.try_push_iter(std::iter::once(&S)).unwrap_err();
        assert_eq!(e, hlua::PushIterError::ValuePushError(CustomError));
        lua
    };

    let lua = {
        let _guard = crate::common::LuaStackIntegrityGuard::new("push_vec_too_many");
        let (e, lua) = lua.try_push(vec![(1, 2, 3)]).unwrap_err();
        assert_eq!(e, hlua::PushIterError::TooManyValues);
        lua
    };

    let lua = {
        let _guard = crate::common::LuaStackIntegrityGuard::new("push_iter_too_many");
        let (e, lua) = lua.try_push_iter(std::iter::once((1, 2, 3))).unwrap_err();
        assert_eq!(e, hlua::PushIterError::TooManyValues);
        lua
    };

    drop(lua);
}

pub fn push_custom_iter() {
    let lua = Lua::new();

    let lua = lua.push_iter((1..=5).map(|i| i * i).filter(|i| i % 2 != 0)).unwrap();
    let t: LuaTable<_> = lua.read().unwrap();
    assert_eq!(t.get(1), Some(1_i32));
    assert_eq!(t.get(2), Some(9_i32));
    assert_eq!(t.get(3), Some(25_i32));
    assert_eq!(t.get(4), None::<i32>);

    let lua = t.into_inner().push_iter(
        ["a", "b", "c"].iter().zip(["foo", "bar", "baz"])
    ).unwrap();
    let t: LuaTable<_> = lua.read().unwrap();
    assert_eq!(t.get("a"), Some("foo".to_string()));
    assert_eq!(t.get("b"), Some("bar".to_string()));
    assert_eq!(t.get("c"), Some("baz".to_string()));

    let res = t.into_inner().push_iter([(1,2,3), (4,5,6)].iter());
    assert!(res.is_err());
}

pub fn push_custom_collection() {
    struct MyVec<T> {
        data: [Option<T>; 3],
        last: usize,
    }

    impl<T> MyVec<T> {
        fn new() -> Self {
            Self { data: [None, None, None], last: 0 }
        }

        fn try_push(&mut self, v: T) -> Result<(), T> {
            if self.last < 3 {
                self.data[self.last] = Some(v);
                self.last += 1;
                Ok(())
            } else {
                Err(v)
            }
        }

        fn iter(&self) -> Iter<'_, T> {
            Iter(self.data.iter())
        }
    }

    struct Iter<'a, T>(std::slice::Iter<'a, Option<T>>);

    impl<'a, T> Iterator for Iter<'a, T> {
        type Item = &'a T;

        fn next(&mut self) -> Option<&'a T> {
            while let Some(maybe_v) = self.0.next() {
                if let Some(v) = maybe_v {
                    return Some(&v)
                }
            }
            None
        }
    }

    impl<L, T> Push<L> for MyVec<T>
    where
        L: AsLua,
        T: Push<hlua::LuaState>,
        <T as Push<hlua::LuaState>>::Err: Into<hlua::Void>,
    {
        type Err = ();

        fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, ((), L)> {
            lua.push_iter(self.iter()).map_err(|l| ((), l))
        }
    }

    let lua = Lua::new();

    let mut v = MyVec::new();
    v.try_push(10).unwrap();
    v.try_push(20).unwrap();
    v.try_push(30).unwrap();
    let lua = lua.try_push(&v).unwrap();
    let t: LuaTable<_> = lua.read().unwrap();
    assert_eq!(t.get(1), Some(10_i32));
    assert_eq!(t.get(2), Some(20_i32));
    assert_eq!(t.get(3), Some(30_i32));
    assert_eq!(t.get(4), None::<i32>);
}

