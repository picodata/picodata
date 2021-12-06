use tarantool::hlua::{
    AnyLuaValue,
    AnyHashableLuaValue,
    AnyLuaString,
};

pub fn read_numbers() {
    let lua = tarantool::global_lua();

    lua.set("a", "-2");
    lua.set("b", 3.5f32);
    lua.set("c", -2.0f32);

    let x: AnyLuaValue = lua.get("a").unwrap();
    assert_eq!(x, AnyLuaValue::LuaString("-2".to_owned()));

    let y: AnyLuaValue = lua.get("b").unwrap();
    assert_eq!(y, AnyLuaValue::LuaNumber(3.5));

    let z: AnyLuaValue = lua.get("c").unwrap();
    assert_eq!(z, AnyLuaValue::LuaNumber(-2.0));
}

pub fn read_hashable_numbers() {
    let lua = tarantool::global_lua();

    lua.set("a", -2.0f32);
    lua.set("b", 4.0f32);
    lua.set("c", "4");

    let x: AnyHashableLuaValue = lua.get("a").unwrap();
    assert_eq!(x, AnyHashableLuaValue::LuaNumber(-2));

    let y: AnyHashableLuaValue = lua.get("b").unwrap();
    assert_eq!(y, AnyHashableLuaValue::LuaNumber(4));

    let z: AnyHashableLuaValue = lua.get("c").unwrap();
    assert_eq!(z, AnyHashableLuaValue::LuaString("4".to_owned()));
}

pub fn read_strings() {
    let lua = tarantool::global_lua();

    lua.set("a", "hello");
    lua.set("b", "3x");
    lua.set("c", "false");

    let x: AnyLuaValue = lua.get("a").unwrap();
    assert_eq!(x, AnyLuaValue::LuaString("hello".to_string()));

    let y: AnyLuaValue = lua.get("b").unwrap();
    assert_eq!(y, AnyLuaValue::LuaString("3x".to_string()));

    let z: AnyLuaValue = lua.get("c").unwrap();
    assert_eq!(z, AnyLuaValue::LuaString("false".to_string()));
}

pub fn read_hashable_strings() {
    let lua = tarantool::global_lua();

    lua.set("a", "hello");
    lua.set("b", "3x");
    lua.set("c", "false");

    let x: AnyHashableLuaValue = lua.get("a").unwrap();
    assert_eq!(x, AnyHashableLuaValue::LuaString("hello".to_string()));

    let y: AnyHashableLuaValue = lua.get("b").unwrap();
    assert_eq!(y, AnyHashableLuaValue::LuaString("3x".to_string()));

    let z: AnyHashableLuaValue = lua.get("c").unwrap();
    assert_eq!(z, AnyHashableLuaValue::LuaString("false".to_string()));
}

pub fn read_booleans() {
    let lua = tarantool::global_lua();

    lua.set("a", true);
    lua.set("b", false);

    let x: AnyLuaValue = lua.get("a").unwrap();
    assert_eq!(x, AnyLuaValue::LuaBoolean(true));

    let y: AnyLuaValue = lua.get("b").unwrap();
    assert_eq!(y, AnyLuaValue::LuaBoolean(false));
}

pub fn read_hashable_booleans() {
    let lua = tarantool::global_lua();

    lua.set("a", true);
    lua.set("b", false);

    let x: AnyHashableLuaValue = lua.get("a").unwrap();
    assert_eq!(x, AnyHashableLuaValue::LuaBoolean(true));

    let y: AnyHashableLuaValue = lua.get("b").unwrap();
    assert_eq!(y, AnyHashableLuaValue::LuaBoolean(false));
}

pub fn read_tables() {
    let lua = tarantool::global_lua();
    lua.exec("
    a = {x = 12, y = 19}
    b = {z = a, w = 'test string'}
    c = {'first', 'second'}
    ").unwrap();

    fn get<'a>(table: &'a AnyLuaValue, key: &str) -> &'a AnyLuaValue {
        let test_key = AnyLuaValue::LuaString(key.to_owned());
        match table {
            &AnyLuaValue::LuaArray(ref vec) => {
                let &(_, ref value) = vec.iter().find(|&&(ref key, _)| key == &test_key).expect("key not found");
                value
            },
            _ => panic!("not a table")
        }
    }

    fn get_numeric<'a>(table: &'a AnyLuaValue, key: usize) -> &'a AnyLuaValue {
        let test_key = AnyLuaValue::LuaNumber(key as f64);
        match table {
            &AnyLuaValue::LuaArray(ref vec) => {
                let &(_, ref value) = vec.iter().find(|&&(ref key, _)| key == &test_key).expect("key not found");
                value
            },
            _ => panic!("not a table")
        }
    }

    let a: AnyLuaValue = lua.get("a").unwrap();
    assert_eq!(get(&a, "x"), &AnyLuaValue::LuaNumber(12.0));
    assert_eq!(get(&a, "y"), &AnyLuaValue::LuaNumber(19.0));

    let b: AnyLuaValue = lua.get("b").unwrap();
    assert_eq!(get(&get(&b, "z"), "x"), get(&a, "x"));
    assert_eq!(get(&get(&b, "z"), "y"), get(&a, "y"));

    let c: AnyLuaValue = lua.get("c").unwrap();
    assert_eq!(get_numeric(&c, 1), &AnyLuaValue::LuaString("first".to_owned()));
    assert_eq!(get_numeric(&c, 2), &AnyLuaValue::LuaString("second".to_owned()));
}

pub fn read_hashable_tables() {
    let lua = tarantool::global_lua();
    lua.exec("
    a = {x = 12, y = 19}
    b = {z = a, w = 'test string'}
    c = {'first', 'second'}
    ").unwrap();

    fn get<'a>(table: &'a AnyHashableLuaValue, key: &str) -> &'a AnyHashableLuaValue {
        let test_key = AnyHashableLuaValue::LuaString(key.to_owned());
        match table {
            &AnyHashableLuaValue::LuaArray(ref vec) => {
                let &(_, ref value) = vec.iter().find(|&&(ref key, _)| key == &test_key).expect("key not found");
                value
            },
            _ => panic!("not a table")
        }
    }

    fn get_numeric<'a>(table: &'a AnyHashableLuaValue, key: usize) -> &'a AnyHashableLuaValue {
        let test_key = AnyHashableLuaValue::LuaNumber(key as i32);
        match table {
            &AnyHashableLuaValue::LuaArray(ref vec) => {
                let &(_, ref value) = vec.iter().find(|&&(ref key, _)| key == &test_key).expect("key not found");
                value
            },
            _ => panic!("not a table")
        }
    }

    let a: AnyHashableLuaValue = lua.get("a").unwrap();
    assert_eq!(get(&a, "x"), &AnyHashableLuaValue::LuaNumber(12));
    assert_eq!(get(&a, "y"), &AnyHashableLuaValue::LuaNumber(19));

    let b: AnyHashableLuaValue = lua.get("b").unwrap();
    assert_eq!(get(&get(&b, "z"), "x"), get(&a, "x"));
    assert_eq!(get(&get(&b, "z"), "y"), get(&a, "y"));

    let c: AnyHashableLuaValue = lua.get("c").unwrap();
    assert_eq!(get_numeric(&c, 1), &AnyHashableLuaValue::LuaString("first".to_owned()));
    assert_eq!(get_numeric(&c, 2), &AnyHashableLuaValue::LuaString("second".to_owned()));
}

pub fn push_numbers() {
    let lua = tarantool::global_lua();

    lua.set("a", AnyLuaValue::LuaNumber(3.0));

    let x: i32 = lua.get("a").unwrap();
    assert_eq!(x, 3);
}

pub fn push_hashable_numbers() {
    let lua = tarantool::global_lua();

    lua.set("a", AnyHashableLuaValue::LuaNumber(3));

    let x: i32 = lua.get("a").unwrap();
    assert_eq!(x, 3);
}

pub fn push_strings() {
    let lua = tarantool::global_lua();

    lua.set("a", AnyLuaValue::LuaString("hello".to_string()));

    let x: String = lua.get("a").unwrap();
    assert_eq!(x, "hello");
}

pub fn push_hashable_strings() {
    let lua = tarantool::global_lua();

    lua.set("a", AnyHashableLuaValue::LuaString("hello".to_string()));

    let x: String = lua.get("a").unwrap();
    assert_eq!(x, "hello");
}

pub fn push_booleans() {
    let lua = tarantool::global_lua();

    lua.set("a", AnyLuaValue::LuaBoolean(true));

    let x: bool = lua.get("a").unwrap();
    assert_eq!(x, true);
}

pub fn push_hashable_booleans() {
    let lua = tarantool::global_lua();

    lua.set("a", AnyHashableLuaValue::LuaBoolean(true));

    let x: bool = lua.get("a").unwrap();
    assert_eq!(x, true);
}

pub fn push_nil() {
    let lua = tarantool::global_lua();

    lua.set("a", AnyLuaValue::LuaNil);

    let x: Option<i32> = lua.get("a");
    assert!(x.is_none(),
            "x is a Some value when it should be a None value. X: {:?}",
            x);
}

pub fn push_hashable_nil() {
    let lua = tarantool::global_lua();

    lua.set("a", AnyHashableLuaValue::LuaNil);

    let x: Option<i32> = lua.get("a");
    assert!(x.is_none(),
            "x is a Some value when it should be a None value. X: {:?}",
            x);
}

pub fn non_utf_8_string() {
    let lua = tarantool::global_lua();
    let a = lua.eval::<AnyLuaValue>(r"return '\xff\xfe\xff\xfe'").unwrap();
    match a {
        AnyLuaValue::LuaAnyString(AnyLuaString(v)) => {
            assert_eq!(Vec::from(&b"\xff\xfe\xff\xfe"[..]), v);
        },
        _ => panic!("Decoded to wrong variant"),
    }
}
