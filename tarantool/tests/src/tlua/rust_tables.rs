use crate::common::LuaStackIntegrityGuard;
use std::{
    any::type_name,
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    num::NonZeroI32,
    rc::Rc,
};
use tarantool::tlua::{
    self, AnyHashableLuaValue, AnyLuaValue, AsLua, Lua, LuaRead, LuaSequence, LuaTable,
    LuaTableMap, Push, PushGuard, PushInto, PushOne, TuplePushError,
};

pub fn push_array() {
    let lua = Lua::new();

    // Slice
    let data: &[i32] = &[9, 8, 7];
    let table: LuaTable<_> = (&lua).push(data).read().unwrap();
    let values = table
        .iter::<i32, i32>()
        .flatten()
        .map(|(_, v)| v)
        .collect::<Vec<_>>();
    assert_eq!(values, vec![9, 8, 7]);

    // By reference
    let data: &[i32; 3] = &[9, 8, 7];
    let table: LuaTable<_> = (&lua).push(data).read().unwrap();
    let values = table
        .iter::<i32, i32>()
        .flatten()
        .map(|(_, v)| v)
        .collect::<Vec<_>>();
    assert_eq!(values, vec![9, 8, 7]);

    // By value
    let table: LuaTable<_> = (&lua).push([9, 8, 7]).read().unwrap();
    let values = table
        .iter::<i32, i32>()
        .flatten()
        .map(|(_, v)| v)
        .collect::<Vec<_>>();
    assert_eq!(values, vec![9, 8, 7]);
}

pub fn push_vec() {
    let lua = Lua::new();

    let orig_vec = vec![9, 8, 7];

    // By reference
    let table: LuaTable<_> = (&lua).push(&orig_vec).read().unwrap();
    let values = table
        .iter::<i32, i32>()
        .flatten()
        .map(|(_, v)| v)
        .collect::<Vec<_>>();
    assert_eq!(values, orig_vec);

    // By value
    let table: LuaTable<_> = (&lua).push(orig_vec.clone()).read().unwrap();
    let values = table
        .iter::<i32, i32>()
        .flatten()
        .map(|(_, v)| v)
        .collect::<Vec<_>>();
    assert_eq!(values, orig_vec);
}

#[derive(Clone, Default)]
struct CustomHasher;
impl std::hash::BuildHasher for CustomHasher {
    type Hasher = std::collections::hash_map::DefaultHasher;
    #[inline(always)]
    fn build_hasher(&self) -> Self::Hasher {
        Self::Hasher::new()
    }
}

pub fn push_hashmap() {
    let lua = Lua::new();

    let mut orig_map = HashMap::with_hasher(CustomHasher);
    orig_map.insert(5, 8);
    orig_map.insert(13, 21);
    orig_map.insert(34, 55);

    // By reference
    let table: LuaTable<_> = (&lua).push(&orig_map).read().unwrap();
    let values: HashMap<i32, i32, CustomHasher> = table.iter().flatten().collect();
    assert_eq!(values, orig_map);

    // By value
    let table: LuaTable<_> = (&lua).push(orig_map.clone()).read().unwrap();
    let values: HashMap<i32, i32, CustomHasher> = table.iter().flatten().collect();
    assert_eq!(values, orig_map);
}

pub fn push_hashset() {
    let lua = Lua::new();

    let mut orig_set = HashSet::with_hasher(CustomHasher);
    orig_set.insert(5);
    orig_set.insert(8);
    orig_set.insert(13);
    orig_set.insert(21);
    orig_set.insert(34);
    orig_set.insert(55);

    // Be reference
    let table: LuaTable<_> = (&lua).push(&orig_set).read().unwrap();
    let values: HashSet<i32, CustomHasher> = table
        .iter::<i32, bool>()
        .flatten()
        .filter_map(|(v, is_set)| is_set.then_some(v))
        .collect();
    assert_eq!(values, orig_set);

    // Be value
    let table: LuaTable<_> = (&lua).push(orig_set.clone()).read().unwrap();
    let values: HashSet<i32, CustomHasher> = table
        .iter::<i32, bool>()
        .flatten()
        .filter_map(|(v, is_set)| is_set.then_some(v))
        .collect();
    assert_eq!(values, orig_set);
}

pub fn globals_table() {
    let lua = Lua::new();

    lua.globals_table().set("a", 12);

    let val: i32 = lua.get("a").unwrap();
    assert_eq!(val, 12);
}

pub fn read_array() {
    let lua = Lua::new();

    assert_eq!(lua.eval("return { 1, 2, 3 }").ok(), Some([1, 2, 3]));
    assert_eq!(
        lua.eval("return { [1] = 1, [2] = 2, [3] = 3 }").ok(),
        Some([1, 2, 3])
    );

    let res = lua.eval::<[i32; 3]>("return { 1, 2 }");
    assert_eq!(
        res.unwrap_err().to_string(),
        "failed converting Lua table to array: indexes in range 1..=3 expected, got Lua table with missing index 3
    while reading value(s) returned by Lua: [i32; 3] expected, got table"
    );

    let res = lua.eval::<[i32; 3]>("return { 1, 2, 3, 4 }");
    assert_eq!(
        res.unwrap_err().to_string(),
        "failed converting Lua table to array: indexes in range 1..=3 expected, got value with index 4
    while reading value(s) returned by Lua: [i32; 3] expected, got table"
    );

    let res = lua.eval::<[i32; 3]>("return { [-1] = 1, [1] = 2, [2] = 3 }");
    assert_eq!(
        res.unwrap_err().to_string(),
        "failed converting Lua table to array: indexes in range 1..=3 expected, got value with index -1
    while reading value(s) returned by Lua: [i32; 3] expected, got table"
    );

    let res = lua.eval::<[i32; 3]>("return { [1] = 1, [3] = 3 }");
    assert_eq!(
        res.unwrap_err().to_string(),
        "failed converting Lua table to array: indexes in range 1..=3 expected, got Lua table with missing index 2
    while reading value(s) returned by Lua: [i32; 3] expected, got table"
    );

    let res = lua.eval::<[i32; 3]>("return { 1, 2, 'foo' }");
    assert_eq!(
        res.unwrap_err().to_string(),
        "failed reading Lua value: i32 expected, got string
    while converting Lua table to array: [i32; 3] expected, got table value of wrong type
    while reading value(s) returned by Lua: [i32; 3] expected, got table"
    );

    let res = lua.eval::<[i32; 3]>("return { [1] = 1, [2] = 2, foo = 3 }");
    assert_eq!(
        res.unwrap_err().to_string(),
        "failed reading Lua value: i32 expected, got string
    while converting Lua table to array: [i32; 3] expected, got table key of wrong type
    while reading value(s) returned by Lua: [i32; 3] expected, got table"
    );

    assert_eq!(lua.eval("return { 1, 2 }").ok(), Some(E::Other(vec![1, 2])));
    assert_eq!(
        lua.eval("return { 1, 2, 3 }").ok(),
        Some(E::Exact([1, 2, 3]))
    );
    assert_eq!(
        lua.eval("return { 1, 2, 3, 4 }").ok(),
        Some(E::Other(vec![1, 2, 3, 4]))
    );

    #[derive(Debug, PartialEq, LuaRead)]
    enum E {
        Exact([i32; 3]),
        Other(Vec<i32>),
    }
}

pub fn read_array_partial() {
    static mut DROPPED: Option<Rc<RefCell<BTreeSet<i32>>>> = None;
    unsafe { DROPPED = Some(Rc::default()) }

    let lua = Lua::new();

    // XXX: strictly speaking lua table iteration key order is not defined so
    // this test may not work in some situations, but it seems to work for now
    assert_eq!(
        lua.eval("return { 1, 2, 'foo', 4 }").ok(),
        None::<[DropCheck; 4]>
    );
    let dropped = unsafe {
        DROPPED
            .as_ref()
            .unwrap()
            .borrow()
            .iter()
            .copied()
            .collect::<Vec<i32>>()
    };
    assert_eq!(dropped, [1, 2]);

    #[derive(Debug, PartialEq)]
    struct DropCheck(i32);

    impl<L: AsLua> LuaRead<L> for DropCheck {
        fn lua_read_at_position(lua: L, idx: NonZeroI32) -> tlua::ReadResult<Self, L> {
            Ok(DropCheck(lua.read_at_nz(idx)?))
        }
    }

    impl Drop for DropCheck {
        fn drop(&mut self) {
            unsafe {
                DROPPED.as_ref().unwrap().borrow_mut().insert(self.0);
            }
        }
    }
}

pub fn read_vec() {
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

    let res = lua.eval::<LuaSequence>("return { [-1] = -1, [2] = 2, [42] = 42 }");
    assert_eq!(
        res.unwrap_err().to_string(),
        format!(
            "failed converting Lua table to Vec<_>: indexes in range 1..N expected, got value with index -1
    while reading value(s) returned by Lua: {seq} expected, got table",
            seq = type_name::<LuaSequence>(),
        ),
    );

    let res = lua.eval::<LuaSequence>("return { [1] = 1, [2] = 2, [42] = 42 }");
    assert_eq!(
        res.unwrap_err().to_string(),
        format!(
            "failed converting Lua table to Vec<_>: indexes in range 1..N expected, got Lua table with missing index 3
    while reading value(s) returned by Lua: {seq} expected, got table",
            seq = type_name::<LuaSequence>(),
        ),
    );

    let res = lua.eval::<LuaSequence>("return { [1] = 1, ['foo'] = 2, [42] = 42 }");
    assert_eq!(
        res.unwrap_err().to_string(),
        format!(
            "failed reading Lua value: i32 expected, got string
    while converting Lua table to Vec<_>: {seq} expected, got table key of wrong type
    while reading value(s) returned by Lua: {seq} expected, got table",
            seq = type_name::<LuaSequence>(),
        ),
    );

    lua.exec(r#"v = { }"#).unwrap();

    let read: LuaSequence = lua.get("v").unwrap();
    assert!(read.is_empty());

    lua.exec(r#"v = { [-1] = -1, ["foo"] = 2, [{}] = 42 }"#)
        .unwrap();

    assert_eq!(lua.get("v"), None::<LuaSequence>);

    let orig = [
        AnyLuaValue::LuaNumber(1.),
        AnyLuaValue::LuaBoolean(false),
        AnyLuaValue::LuaNumber(3.),
    ];

    lua.set("v", &orig[..]);

    let read: LuaSequence = lua.get("v").unwrap();
    assert_eq!(read, orig);

    lua.exec(r#"v = { 1, 2, 3 }"#).unwrap();

    let read: LuaSequence = lua.get("v").unwrap();
    assert_eq!(
        read,
        [1., 2., 3.]
            .iter()
            .copied()
            .map(AnyLuaValue::LuaNumber)
            .collect::<Vec<_>>()
    );

    let read: Vec<i32> = lua.get("v").unwrap();
    assert_eq!(read, vec![1, 2, 3]);

    let read: Vec<u8> = lua.get("v").unwrap();
    assert_eq!(read, vec![1, 2, 3]);

    let read: Vec<f64> = lua.get("v").unwrap();
    assert_eq!(read, vec![1., 2., 3.]);
}

pub fn read_hashmap() {
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

    lua.exec(r#"v = { [-1] = -1, [2] = 2, [42] = 42 }"#)
        .unwrap();

    let read: LuaTableMap = lua.get("v").unwrap();
    assert_eq!(
        read[&AnyHashableLuaValue::LuaNumber(-1)],
        AnyLuaValue::LuaNumber(-1.)
    );
    assert_eq!(
        read[&AnyHashableLuaValue::LuaNumber(2)],
        AnyLuaValue::LuaNumber(2.)
    );
    assert_eq!(
        read[&AnyHashableLuaValue::LuaNumber(42)],
        AnyLuaValue::LuaNumber(42.)
    );
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

    lua.exec(r#"v = { }"#).unwrap();

    let read: LuaTableMap = lua.get("v").unwrap();
    assert!(read.is_empty());

    lua.exec(r#"v = { [-1] = -1, ["foo"] = 2, [2.] = 42 }"#)
        .unwrap();

    let read: LuaTableMap = lua.get("v").unwrap();
    assert_eq!(
        read[&AnyHashableLuaValue::LuaNumber(-1)],
        AnyLuaValue::LuaNumber(-1.)
    );
    assert_eq!(
        read[&AnyHashableLuaValue::LuaString("foo".to_owned())],
        AnyLuaValue::LuaNumber(2.)
    );
    assert_eq!(
        read[&AnyHashableLuaValue::LuaNumber(2)],
        AnyLuaValue::LuaNumber(42.)
    );
    assert_eq!(read.len(), 3);

    lua.exec(r#"v = { [-1.25] = -1, [2.5] = 42 }"#).unwrap();

    let read: LuaTableMap = lua.get("v").unwrap();
    // It works by truncating integers in some unspecified way
    // https://www.lua.org/manual/5.2/manual.html#lua_tointegerx
    assert_eq!(
        read[&AnyHashableLuaValue::LuaNumber(-1)],
        AnyLuaValue::LuaNumber(-1.)
    );
    assert_eq!(
        read[&AnyHashableLuaValue::LuaNumber(2)],
        AnyLuaValue::LuaNumber(42.)
    );
    assert_eq!(read.len(), 2);

    let mut orig = HashMap::new();
    orig.insert(
        AnyHashableLuaValue::LuaNumber(42),
        AnyLuaValue::LuaNumber(42.),
    );
    orig.insert(
        AnyHashableLuaValue::LuaString("foo".to_owned()),
        AnyLuaValue::LuaString("foo".to_owned()),
    );
    orig.insert(
        AnyHashableLuaValue::LuaBoolean(true),
        AnyLuaValue::LuaBoolean(true),
    );

    lua.set("v", &orig);

    let read: HashMap<_, _> = lua.get("v").unwrap();
    assert_eq!(read, orig);

    assert_eq!(lua.get::<HashMap<i32, i32>, _>("v"), None);

    lua.exec(r#"v = { [1] = 2, [2] = 3, [3] = 4 }"#).unwrap();

    let read: HashMap<_, _> = lua.get("v").unwrap();
    assert_eq!(
        read,
        [2., 3., 4.]
            .iter()
            .enumerate()
            .map(|(k, v)| (
                AnyHashableLuaValue::LuaNumber((k + 1) as i32),
                AnyLuaValue::LuaNumber(*v)
            ))
            .collect::<HashMap<_, _>>()
    );

    type HM = HashMap<String, i32>;
    let res = lua.eval::<HM>("return { 1, 2, 'foo' }");
    assert_eq!(
        res.unwrap_err().to_string(),
        format!(
            "failed reading Lua value: alloc::string::String expected, got number
    while converting Lua table to HashMap<_, _>: {hm} expected, got table key of wrong type
    while reading value(s) returned by Lua: {hm} expected, got table",
            hm = type_name::<HM>(),
        ),
    );

    let res = lua.eval::<HM>("return { foo = 1, bar = false }");
    assert_eq!(
        res.unwrap_err().to_string(),
        format!(
            "failed reading Lua value: i32 expected, got boolean
    while converting Lua table to HashMap<_, _>: {hm} expected, got table value of wrong type
    while reading value(s) returned by Lua: {hm} expected, got table",
            hm = type_name::<HM>(),
        ),
    );
}

pub fn read_wrong_type_fail() {
    let lua = Lua::new();

    let res = lua.eval::<i32>("return { 1, 2, 'foo' }");
    assert_eq!(
        res.unwrap_err().to_string(),
        "failed reading value(s) returned by Lua: i32 expected, got table"
    );
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
    let lua = lua.push(&S {
        i: 69,
        s: "nice".into(),
        boo: true,
        table: vec![11, 12, 13],
        r#struct: T { x: 3.14 },
    });
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

pub fn derive_generic_struct_push() {
    #[derive(Push)]
    struct S<A, B, C, K, V, const N: usize> {
        a: A,
        table: Vec<B>,
        r#struct: T<C>,
        array: [i32; N],
        good_fruit: HashMap<K, V>,
    }

    #[derive(Push)]
    struct T<C> {
        x: C,
    }

    let lua = Lua::new();
    let lua = lua.push(&S {
        a: 69,
        table: vec![101, 102, 103],
        r#struct: T {
            x: vec![("hello", 13), ("sailor", 37)],
        },
        array: [3, 2, 1],
        good_fruit: HashMap::from([("apple", true), ("pear", false)]),
    });
    let s: LuaTable<_> = lua.read().unwrap();
    assert_eq!(s.get::<i32, _>("a"), Some(69));
    let t: LuaTable<_> = s.get("table").unwrap();
    assert_eq!(t.get::<u8, _>(1), Some(101));
    assert_eq!(t.get::<u8, _>(2), Some(102));
    assert_eq!(t.get::<u8, _>(3), Some(103));
    let t: LuaTable<_> = s.get("struct").unwrap();
    let t: LuaTable<_> = t.get("x").unwrap();
    assert_eq!(t.get::<u8, _>("hello"), Some(13));
    assert_eq!(t.get::<u8, _>("sailor"), Some(37));
    let t: LuaTable<_> = s.get("array").unwrap();
    assert_eq!(t.get::<u8, _>(1), Some(3));
    assert_eq!(t.get::<u8, _>(2), Some(2));
    assert_eq!(t.get::<u8, _>(3), Some(1));
    let t: LuaTable<_> = s.get("good_fruit").unwrap();
    assert_eq!(t.get::<bool, _>("apple"), Some(true));
    assert_eq!(t.get::<bool, _>("pear"), Some(false));
}

pub fn derive_struct_lua_read() {
    #[derive(Debug, PartialEq, Eq, LuaRead)]
    struct S {
        i: i32,
        s: String,
        boo: bool,
        o: Option<i32>,
    }

    #[derive(Debug, PartialEq, Eq, LuaRead)]
    struct T {
        i: i32,
        s: String,
    }

    let lua = Lua::new();
    lua.exec(r#"t = { i = 69, s = "booboo", boo = true }"#)
        .unwrap();
    let s: S = lua.get("t").unwrap();
    assert_eq!(
        s,
        S {
            i: 69,
            s: "booboo".into(),
            boo: true,
            o: None
        }
    );

    let t: T = lua.get("t").unwrap();
    assert_eq!(
        t,
        T {
            i: 69,
            s: "booboo".into()
        }
    );

    let res = lua.eval::<S>("return 'not a table'");
    assert_eq!(
        res.unwrap_err().to_string(),
        format!(
            "failed converting Lua value to struct: Lua table expected, got string
    while reading value(s) returned by Lua: {s} expected, got string",
            s = type_name::<S>(),
        )
    );
}

pub fn derive_generic_struct_lua_read() {
    #[derive(Debug, LuaRead)]
    struct S<A, B, C, K, V>
    where
        K: std::hash::Hash,
    {
        a: A,
        b: Vec<B>,
        d: Option<C>,
        is_prime: HashMap<K, V>,
    }

    let lua = Lua::new();
    let s: S1 = lua
        .eval(
            "return {
        a = 'hell yeah',
        b = { 1, 2, 3 },
        d = 420,
        is_prime = { [479] = true, [439] = false }
    }",
        )
        .unwrap();
    assert_eq!(s.a, "hell yeah");
    assert_eq!(s.b, [1.0, 2.0, 3.0]);
    assert_eq!(s.d, Some(420));
    assert_eq!(s.is_prime, HashMap::from([(479, true), (439, false)]));

    let res = lua.eval::<S1>(
        "return {
        a = 'hell yeah',
        b = { 1, 2, 3 },
        d = 420,
        is_prime = { [479] = true, [false] = false }
    }",
    );
    assert_eq!(
        res.unwrap_err().to_string(),
        format!(
            "failed reading Lua value: u64 expected, got boolean
    while converting Lua table to HashMap<_, _>: {hm} expected, got table key of wrong type
    while reading value from Lua table: {hm} expected, got table
    while converting Lua table to struct: {s} expected, got wrong field type for key 'is_prime'
    while reading value(s) returned by Lua: {s} expected, got table",
            hm = type_name::<HashMap<u64, bool>>(),
            s = type_name::<S1>(),
        )
    );

    type S1 = S<String, f32, u32, u64, bool>;
}

pub fn derive_enum_push() {
    #[derive(Push)]
    enum E {
        Num(i32),
        Str(String),
        Vec(f32, f32, f32),
        Tuple((f32, f32, f32)),
        S(S),
        Struct { i: i32, s: String },
    }

    #[derive(Push, LuaRead, PartialEq, Eq, Debug)]
    struct S {
        foo: i32,
        bar: String,
    }

    let lua = Lua::new();
    let lua = lua.push(&E::Num(69));
    assert_eq!((&lua).read::<i32>().unwrap(), 69);
    let lua = lua.push(&E::Str("hello".into()));
    assert_eq!((&lua).read::<String>().unwrap(), "hello");
    let lua = lua.push(&E::Vec(3.14, 2.71, 1.62));
    assert_eq!(
        (&lua).read::<(f32, f32, f32)>().unwrap(),
        (3.14, 2.71, 1.62)
    );
    let lua = lua.push(&E::Tuple((2.71, 1.62, 3.14)));
    assert_eq!(
        (&lua).read::<(f32, f32, f32)>().unwrap(),
        (2.71, 1.62, 3.14)
    );
    let lua = lua.push(&E::S(S {
        foo: 69,
        bar: "nice".into(),
    }));
    assert_eq!(
        (&lua).read::<S>().unwrap(),
        S {
            foo: 69,
            bar: "nice".into()
        }
    );
    let lua = lua.push(&E::Struct {
        i: 420,
        s: "blaze".into(),
    });
    let t: LuaTable<_> = (&lua).read().unwrap();
    assert_eq!(t.get::<i32, _>("i").unwrap(), 420);
    assert_eq!(t.get::<String, _>("s").unwrap(), "blaze");
}

pub fn derive_generic_enum_push() {
    #[derive(Push)]
    enum E<'a, A, T, Foo, Bar, const N: usize> {
        A(A),
        B(Foo, Bar),
        Tuple(T),
        S(S<Foo, Bar>),
        Struct { i: Foo, s: Bar },
        Str(&'a str),
        Array([A; N]),
    }

    #[derive(Push, LuaRead)]
    struct S<Foo, Bar> {
        foo: Foo,
        bar: Bar,
    }

    let lua = Lua::new();
    type E1<'a> = E<'a, u32, (f32, f32, f32), i64, String, 6>;
    let lua = lua.push(&E1::A(69));
    assert_eq!((&lua).read::<i32>().unwrap(), 69);
    let lua = lua.push(&E1::B(1337, "leet".into()));
    assert_eq!(
        (&lua).read::<(u32, String)>().unwrap(),
        (1337, "leet".into())
    );
    let lua = lua.push(&E1::Tuple((2.71, 1.62, 3.14)));
    assert_eq!(
        (&lua).read::<(f32, f32, f32)>().unwrap(),
        (2.71, 1.62, 3.14)
    );
    let lua = lua.push(&E1::S(S {
        foo: 69,
        bar: "nice".into(),
    }));
    let s = (&lua).read::<S<i64, String>>().unwrap();
    assert_eq!(s.foo, 69);
    assert_eq!(s.bar, "nice");
    let lua = lua.push(&E1::Struct {
        i: 420,
        s: "blaze".into(),
    });
    let t: LuaTable<_> = (&lua).read().unwrap();
    assert_eq!(t.get::<i32, _>("i").unwrap(), 420);
    assert_eq!(t.get::<String, _>("s").unwrap(), "blaze");
    let lua = lua.push(&E1::Str("stupendous"));
    assert_eq!((&lua).read::<String>().unwrap(), "stupendous");
    let lua = lua.push(&E1::Array([4, 8, 15, 16, 23, 42]));
    assert_eq!((&lua).read::<Vec<u8>>().unwrap(), [4, 8, 15, 16, 23, 42]);
}

pub fn derive_push_into() {
    #[derive(PushInto)]
    enum E {
        Num(i32),
        Str(String),
        Vec(f32, f32, f32),
        Tuple((f32, f32, f32)),
        S(S),
        Struct { i: i32, s: String, b: bool },
        Hello,
        Goodbye,
    }

    #[derive(PushInto, LuaRead, PartialEq, Eq, Debug)]
    struct S {
        foo: i32,
        bar: String,
    }

    let lua = Lua::new();
    let lua = lua.push(E::Num(69));
    assert_eq!((&lua).read().ok(), Some(69));

    let lua = lua.push(E::Str("hello".into()));
    assert_eq!((&lua).read().ok(), Some("hello".to_string()));

    let lua = lua.push(E::Vec(3.14, 2.71, 1.62));
    assert_eq!((&lua).read().ok(), Some((3.14f32, 2.71f32, 1.62f32)));

    let lua = lua.push(E::Tuple((2.71, 1.62, 3.14)));
    assert_eq!((&lua).read().ok(), Some((2.71f32, 1.62f32, 3.14f32)));

    let lua = lua.push(E::S(S {
        foo: 69,
        bar: "nice".into(),
    }));
    assert_eq!(
        (&lua).read().ok(),
        Some(S {
            foo: 69,
            bar: "nice".into()
        })
    );

    let lua = lua.push(E::Struct {
        i: 420,
        s: "blaze".into(),
        b: true,
    });
    let t: LuaTable<_> = (&lua).read().unwrap();
    assert_eq!(t.get("i"), Some(420));
    assert_eq!(t.get("s"), Some("blaze".to_string()));

    let lua = lua.push(E::Hello);
    assert_eq!((&lua).read().ok(), Some("hello".to_string()));

    let lua = lua.push(E::Goodbye);
    assert_eq!((&lua).read().ok(), Some("goodbye".to_string()));
}

pub fn derive_generic_push_into() {
    #[derive(PushInto)]
    enum E0<A, Foo, Bar, T> {
        Num(A),
        Vec(Foo, Bar),
        Tuple(T),
        S(S<Foo, Bar>),
        Struct { i: Foo, s: Bar },
        Hello,
        Goodbye,
    }

    #[derive(PushInto, LuaRead)]
    struct S<Foo, Bar> {
        foo: Foo,
        bar: Bar,
    }

    let lua = Lua::new();
    type E = E0<u32, f32, String, (f32, f32, f32)>;

    let lua = lua.push(E::Num(69));
    assert_eq!((&lua).read().ok(), Some(69));

    let lua = lua.push(E::Vec(3.14, "pi".into()));
    assert_eq!((&lua).read().ok(), Some((3.14f32, "pi".to_string())));

    let lua = lua.push(E::Tuple((2.71, 1.62, 3.14)));
    assert_eq!((&lua).read().ok(), Some((2.71f32, 1.62f32, 3.14f32)));

    let lua = lua.push(E::S(S {
        foo: 69.0,
        bar: "nice".into(),
    }));
    let t: LuaTable<_> = (&lua).read().unwrap();
    assert_eq!(t.get("foo"), Some(69));
    assert_eq!(t.get("bar"), Some("nice".to_string()));

    let lua = lua.push(E::Struct {
        i: 420.0,
        s: "blaze".into(),
    });
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
        Struct { i: i32, s: String },
    }

    #[derive(Push, LuaRead, PartialEq, Eq, Debug)]
    struct S {
        foo: i32,
        bar: String,
    }

    let lua = Lua::new();
    let res: E = lua.eval("return 7").unwrap();
    assert_eq!(res, E::Num(7));
    let res: E = lua.eval(r#"return "howdy""#).unwrap();
    assert_eq!(res, E::Str("howdy".into()));
    let res: E = lua.eval("return 1.5, 2.5, 3.5").unwrap();
    assert_eq!(res, E::Vec(1.5, 2.5, 3.5));
    let res: E = lua.eval(r#"return { foo = 420, bar = "foo" }"#).unwrap();
    assert_eq!(
        res,
        E::S(S {
            foo: 420,
            bar: "foo".into()
        })
    );
    let res: E = lua.eval(r#"return { i = 69, s = "nice" }"#).unwrap();
    assert_eq!(
        res,
        E::Struct {
            i: 69,
            s: "nice".into()
        }
    );

    let lua = lua.push((1, 2, 3));
    assert_eq!((&lua).read::<E>().unwrap(), E::Vec(1., 2., 3.));
    let lua = lua.push(&S {
        foo: 314,
        bar: "pi".into(),
    });
    assert_eq!(
        (&lua).read::<E>().unwrap(),
        E::S(S {
            foo: 314,
            bar: "pi".into()
        })
    );

    let res = lua.into_inner().into_inner().eval::<E>("return { s = 0 }");
    assert_eq!(
        res.unwrap_err().to_string(),
        format!(
            "variant #1: failed reading Lua value: f32 expected, got table
    while reading one of multiple values: f32 at index 1 (1-based) expected, got incorrect value
    while reading enum variant: Vec expected, got (table, no value, no value)
variant #2: failed reading Lua value: i32 expected, got table
    while reading enum variant: Num expected, got table
variant #3: failed reading Lua value: alloc::string::String expected, got table
    while reading enum variant: Str expected, got table
variant #4: failed reading value from Lua table: i32 expected, got nil
    while converting Lua table to struct: {s} expected, got wrong field type for key 'foo'
    while reading enum variant: S expected, got table
variant #5: failed reading value from Lua table: i32 expected, got nil
    while converting Lua table to struct: struct with fields {{ i: i32, s: String }} expected, got wrong field type for key 'i'
    while reading enum variant: Struct expected, got table
    while reading any of the variants: {e} expected, got something else
    while reading value(s) returned by Lua: {e} expected, got table",
            s = type_name::<S>(),
            e = type_name::<E>(),
        )
    );
}

pub fn derive_generic_enum_lua_read() {
    #[derive(Debug, PartialEq, Eq, LuaRead)]
    enum E<A, B, F, G, H, J, K, L, M> {
        A(A),
        B(Vec<B>),
        D { f: F, g: Vec<G> },
        H(H, Vec<J>, Option<K>),
        L(S<L, M>),
    }

    #[derive(LuaRead, PartialEq, Eq, Debug)]
    struct S<A, B> {
        foo: A,
        bar: B,
    }

    let lua = Lua::new();
    type E1 = E<f64, String, String, u8, String, u8, u64, String, Vec<u8>>;
    let e: E1 = lua.eval("return 3.14").unwrap();
    assert_eq!(e, E::A(3.14));
    let e: E1 = lua.eval("return {'apple', 'banana', 'cytrus'}").unwrap();
    assert_eq!(
        e,
        E::B(vec!["apple".into(), "banana".into(), "cytrus".into()])
    );
    let e: E1 = lua.eval("return { f = 'hi', g = { 1, 2, 3 } }").unwrap();
    assert_eq!(
        e,
        E::D {
            f: "hi".into(),
            g: vec![1, 2, 3]
        }
    );
    let e: E1 = lua
        .eval("return { foo = 'foo', bar = { 0x62, 0x61, 0x72 } }")
        .unwrap();
    assert_eq!(
        e,
        E::L(S {
            foo: "foo".into(),
            bar: b"bar".as_slice().into()
        })
    );

    let res = lua.eval::<E1>("return { foo = { 'wrong', 'type' } }");
    assert_eq!(
        res.unwrap_err().to_string(),
        "variant #1: failed reading Lua value: f64 expected, got table
    while reading enum variant: A expected, got table
variant #2: failed reading Lua value: i32 expected, got string
    while converting Lua table to Vec<_>: alloc::vec::Vec<alloc::string::String> expected, got table key of wrong type
    while reading enum variant: B expected, got table
variant #3: failed reading value from Lua table: alloc::string::String expected, got nil
    while converting Lua table to struct: struct with fields { f: F, g: Vec < G > } expected, got wrong field type for key 'f'
    while reading enum variant: D expected, got table
variant #4: failed reading Lua value: alloc::string::String expected, got table
    while reading one of multiple values: alloc::string::String at index 1 (1-based) expected, got incorrect value
    while reading enum variant: H expected, got (table, no value, no value)
variant #5: failed reading value from Lua table: alloc::string::String expected, got table
    while converting Lua table to struct: tarantool_module_test_runner::tlua::rust_tables::derive_generic_enum_lua_read::S<alloc::string::String, alloc::vec::Vec<u8>> expected, got wrong field type for key 'foo'
    while reading enum variant: L expected, got table
    while reading any of the variants: tarantool_module_test_runner::tlua::rust_tables::derive_generic_enum_lua_read::E<f64, alloc::string::String, alloc::string::String, u8, alloc::string::String, u8, u64, alloc::string::String, alloc::vec::Vec<u8>> expected, got something else
    while reading value(s) returned by Lua: tarantool_module_test_runner::tlua::rust_tables::derive_generic_enum_lua_read::E<f64, alloc::string::String, alloc::string::String, u8, alloc::string::String, u8, u64, alloc::string::String, alloc::vec::Vec<u8>> expected, got table"
    );
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

    lua.exec(
        r#"v = {
        vec = {
            { x = 69 },
            { y = "hello" },
            { z = true },
        }
    }"#,
    )
    .unwrap();

    let s: S = lua.get("v").unwrap();
    assert_eq!(
        s,
        S {
            vec: vec![
                V {
                    x: Some(69),
                    ..Default::default()
                },
                V {
                    y: Some("hello".into()),
                    ..Default::default()
                },
                V {
                    z: Some(true),
                    ..Default::default()
                },
            ],
        }
    );

    let t: T = lua.get("v").unwrap();
    assert_eq!(
        t,
        T {
            vec: vec![E::X { x: 69 }, E::Y { y: "hello".into() }, E::Z { z: true },],
        }
    );

    #[derive(Debug, PartialEq, LuaRead)]
    struct S {
        vec: Vec<V>,
    }

    #[derive(Debug, PartialEq, LuaRead, Default)]
    struct V {
        x: Option<i32>,
        y: Option<String>,
        z: Option<bool>,
    }

    #[derive(Debug, PartialEq, LuaRead)]
    struct T {
        vec: Vec<E>,
    }

    #[derive(Debug, PartialEq, LuaRead)]
    enum E {
        X { x: i32 },
        Y { y: String },
        Z { z: bool },
    }
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

    let res = (&lua).push("f_oo").read::<E>();
    assert_eq!(
        res.unwrap_err().1.to_string(),
        "variant #1: failed reading unit struct: case incensitive match with 'a' expected, got 'f_oo'
variant #2: failed reading unit struct: case incensitive match with 'foo' expected, got 'f_oo'
variant #3: failed reading unit struct: case incensitive match with 'xxx' expected, got 'f_oo'
    while reading any of the variants: tarantool_module_test_runner::tlua::rust_tables::derive_unit_structs_lua_read::E expected, got something else",
    );

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
        Null(tlua::Null),
        Number(u64),
        String(String),
    }

    let lua = tarantool::lua_state();

    let v: QueryResult = lua
        .eval(
            "return {
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
    }",
        )
        .unwrap();

    assert_eq!(
        v,
        QueryResult {
            metadata: vec![
                Column {
                    name: "id".into(),
                    r#type: Type::Integer
                },
                Column {
                    name: "name".into(),
                    r#type: Type::String
                },
                Column {
                    name: "product_units".into(),
                    r#type: Type::Integer
                },
            ],
            rows: vec![vec![
                Value::Number(1),
                Value::String("123".into()),
                Value::Null(tlua::Null),
            ]]
        }
    );

    let msg = lua
        .eval::<QueryResult>(
            "return {
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
                        name = 0xcafebabe,
                        type = 'integer'
                    }
                },
                rows = {
                    {1, '123', box.NULL}
                }
            }",
        )
        .unwrap_err()
        .to_string();
    assert_eq!(
        msg,
        format!(
            "failed reading value from Lua table: alloc::string::String expected, got number
    while converting Lua table to struct: {col} expected, got wrong field type for key 'name'
    while converting Lua table to Vec<_>: alloc::vec::Vec<{col}> expected, got table value of wrong type
    while reading value from Lua table: alloc::vec::Vec<{col}> expected, got table
    while converting Lua table to struct: {qr} expected, got wrong field type for key 'metadata'
    while reading value(s) returned by Lua: {qr} expected, got table",
            col = type_name::<Column>(),
            qr = type_name::<QueryResult>(),
        )
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
    assert_eq!((&lua).read().ok(), Some(tlua::Typename("string")));
    assert_eq!((&lua).read().ok(), Some("a".to_string()));
    let lua = lua.into_inner().push(&E::Foo);
    assert_eq!((&lua).read().ok(), Some(tlua::Typename("string")));
    assert_eq!((&lua).read().ok(), Some("foo".to_string()));
    let lua = lua.into_inner().push(&E::XXX);
    assert_eq!((&lua).read().ok(), Some(tlua::Typename("string")));
    assert_eq!((&lua).read().ok(), Some("xxx".to_string()));
}

pub fn error_during_push_iter() {
    #[derive(Debug, PartialEq, Eq)]
    struct CustomError;

    impl std::fmt::Display for CustomError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("CustomError")
        }
    }

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
        let _guard = LuaStackIntegrityGuard::new("push_vec_error", &lua);
        let (e, lua) = lua.try_push(&vec![S]).unwrap_err();
        assert_eq!(e, tlua::PushIterError::ValuePushError(CustomError));
        assert_eq!(e.to_string(), "Pushing iterable item failed: CustomError");
        lua
    };

    let lua = {
        let _guard = LuaStackIntegrityGuard::new("push_hashmap_key_error", &lua);
        let mut hm = HashMap::new();
        hm.insert(S, 1);
        let (e, lua) = lua.try_push(&hm).unwrap_err();
        assert_eq!(e, TuplePushError::First(CustomError));
        assert_eq!(
            e.to_string(),
            "Error during attempt to push multiple values: (CustomError, ...)"
        );
        lua
    };

    let lua = {
        let _guard = LuaStackIntegrityGuard::new("push_hashmap_value_error", &lua);
        let mut hm = HashMap::new();
        hm.insert(1, S);
        let (e, lua) = lua.try_push(&hm).unwrap_err();
        assert_eq!(e, TuplePushError::Other(CustomError));
        assert_eq!(
            e.to_string(),
            "Error during attempt to push multiple values: (ok, CustomError, ...)"
        );
        lua
    };

    let lua = {
        let _guard = LuaStackIntegrityGuard::new("push_hashset_error", &lua);
        let mut hm = HashSet::new();
        hm.insert(S);
        let (e, lua) = lua.try_push(&hm).unwrap_err();
        assert_eq!(e, CustomError);
        lua
    };

    let lua = {
        let _guard = LuaStackIntegrityGuard::new("push_iter_error", &lua);
        let (e, lua) = lua.try_push_iter(std::iter::once(&S)).unwrap_err();
        assert_eq!(e, tlua::PushIterError::ValuePushError(CustomError));
        lua
    };

    let lua = {
        let _guard = LuaStackIntegrityGuard::new("push_vec_too_many", &lua);
        let (e, lua) = lua.try_push(vec![(1, 2, 3)]).unwrap_err();
        assert_eq!(e, tlua::PushIterError::TooManyValues(3));
        assert_eq!(
            e.to_string(),
            "Can only push 1 or 2 values as lua table item, got 3 instead"
        );
        lua
    };

    let lua = {
        let _guard = LuaStackIntegrityGuard::new("push_iter_too_many", &lua);
        let (e, lua) = lua.try_push_iter(std::iter::once((1, 2, 3))).unwrap_err();
        assert_eq!(e, tlua::PushIterError::TooManyValues(3));
        lua
    };

    drop(lua);
}

pub fn push_custom_iter() {
    let lua = Lua::new();

    let lua = lua
        .push_iter((1..=5).map(|i| i * i).filter(|i| i % 2 != 0))
        .unwrap();
    let t: LuaTable<_> = lua.read().unwrap();
    assert_eq!(t.get(1), Some(1_i32));
    assert_eq!(t.get(2), Some(9_i32));
    assert_eq!(t.get(3), Some(25_i32));
    assert_eq!(t.get(4), None::<i32>);

    let lua = t
        .into_inner()
        .push_iter(["a", "b", "c"].iter().zip(["foo", "bar", "baz"]))
        .unwrap();
    let t: LuaTable<_> = lua.read().unwrap();
    assert_eq!(t.get("a"), Some("foo".to_string()));
    assert_eq!(t.get("b"), Some("bar".to_string()));
    assert_eq!(t.get("c"), Some("baz".to_string()));

    let res = t.into_inner().push_iter([(1, 2, 3), (4, 5, 6)].iter());
    assert!(res.is_err());
}

pub fn push_custom_collection() {
    struct MyVec<T> {
        data: [Option<T>; 3],
        last: usize,
    }

    impl<T> MyVec<T> {
        fn new() -> Self {
            Self {
                data: [None, None, None],
                last: 0,
            }
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
            self.0.by_ref().flatten().next()
        }
    }

    impl<L, T> Push<L> for MyVec<T>
    where
        L: AsLua,
        T: Push<tlua::LuaState>,
        <T as Push<tlua::LuaState>>::Err: Into<tlua::Void>,
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

pub fn table_from_iter() {
    let lua = Lua::new();
    lua.set("foo", tlua::TableFromIter([1, 2, 3].iter().map(|&v| v + 1)));
    let t: LuaTable<_> = lua.get("foo").unwrap();
    assert_eq!(t.get(1), Some(2));
    assert_eq!(t.get(2), Some(3));
    assert_eq!(t.get(3), Some(4));
    assert_eq!(t.get(4), None::<i32>);
}

pub fn push_struct_of_nones() {
    #[derive(PushInto, Default)]
    struct OfNones {
        a: Option<i32>,
        b: Option<String>,
    }

    let lua = Lua::new();
    lua.set("push_struct_of_nones", OfNones::default());
    let t: LuaTable<_> = lua.get("push_struct_of_nones").unwrap();
    assert_eq!(t.get("a"), None::<i32>);
    assert_eq!(t.get("b"), None::<String>);
}

pub fn derive_tuple_structs() {
    #[derive(Debug, PartialEq, Push, PushInto, LuaRead)]
    struct Int(i32);

    let lua = Lua::new();

    // LuaRead
    lua.set("derive_tuple_structs", 1337);
    assert_eq!(lua.get("derive_tuple_structs"), Some(Int(1337)));

    assert_eq!(
        lua.eval::<Int>("return 'not a number'").unwrap_err().to_string(),
        "failed reading Lua value: i32 expected, got string
    while reading value(s) returned by Lua: tarantool_module_test_runner::tlua::rust_tables::derive_tuple_structs::Int expected, got string"
    );

    // PushInto
    lua.set("derive_tuple_structs", Int(420));
    assert_eq!(lua.get("derive_tuple_structs"), Some(420));

    // Push
    lua.set("derive_tuple_structs", &Int(69));
    assert_eq!(lua.get("derive_tuple_structs"), Some(69));
}
