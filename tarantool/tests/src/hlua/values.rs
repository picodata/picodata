use tarantool::hlua::{
    AsLua,
    AnyLuaValue,
    AnyLuaString,
    Lua,
    StringInLua,
    function0,
    Nil,
};

pub fn read_i32s() {
    let lua = Lua::new();

    lua.set("a", 2);

    let x: i32 = lua.get("a").unwrap();
    assert_eq!(x, 2);

    let y: i8 = lua.get("a").unwrap();
    assert_eq!(y, 2);

    let z: i16 = lua.get("a").unwrap();
    assert_eq!(z, 2);

    let w: i32 = lua.get("a").unwrap();
    assert_eq!(w, 2);

    let a: u32 = lua.get("a").unwrap();
    assert_eq!(a, 2);

    let b: u8 = lua.get("a").unwrap();
    assert_eq!(b, 2);

    let c: u16 = lua.get("a").unwrap();
    assert_eq!(c, 2);

    let d: u32 = lua.get("a").unwrap();
    assert_eq!(d, 2);
}

pub fn write_i32s() {
    // TODO:

    let lua = Lua::new();

    lua.set("a", 2);
    let x: i32 = lua.get("a").unwrap();
    assert_eq!(x, 2);
}

pub fn int64() {
    let lua = crate::hlua::global();
    let res: i64 = (&lua).push(-0x69).read().unwrap();
    assert_eq!(res, -0x69);

    let res: i64 = (&lua).push(0x77bbccddeeff0011i64).read().unwrap();
    assert_eq!(res, 0x77bbccddeeff0011i64);

    let res: u64 = (&lua).push(0xaabbccddeeff0011u64).read().unwrap();
    assert_eq!(res, 0xaabbccddeeff0011u64);

    let res: u64 = (&lua).push(f64::INFINITY).read().unwrap();
    assert_eq!(res, u64::MAX);

    let res: i64 = (&lua).push(f64::NEG_INFINITY).read().unwrap();
    assert_eq!(res, i64::MIN);

    let err = lua.execute::<i32>("return 0ull").unwrap_err();
    assert_eq!(err.to_string(), "Wrong type returned by Lua: i32 expected, got cdata");

    let res = lua.execute::<i64>("return 0ull").unwrap();
    assert_eq!(res, 0);
}

pub fn readwrite_floats() {
    let lua = Lua::new();

    lua.set("a", 2.51234 as f32);
    lua.set("b", 3.4123456789 as f64);

    let x: f32 = lua.get("a").unwrap();
    assert!(x - 2.51234 < 0.000001);

    let y: f64 = lua.get("a").unwrap();
    assert!(y - 2.51234 < 0.000001);

    let z: f32 = lua.get("b").unwrap();
    assert!(z - 3.4123456789 < 0.000001);

    let w: f64 = lua.get("b").unwrap();
    assert!(w - 3.4123456789 < 0.000001);
}

pub fn readwrite_bools() {
    let lua = Lua::new();

    lua.set("a", true);
    lua.set("b", false);

    let x: bool = lua.get("a").unwrap();
    assert_eq!(x, true);

    let y: bool = lua.get("b").unwrap();
    assert_eq!(y, false);
}

pub fn readwrite_strings() {
    let lua = Lua::new();

    lua.set("a", "hello");
    lua.set("b", "hello".to_string());

    let x: String = lua.get("a").unwrap();
    assert_eq!(x, "hello");

    let y: String = lua.get("b").unwrap();
    assert_eq!(y, "hello");

    assert_eq!(lua.execute::<String>("return 'abc'").unwrap(), "abc");
    assert_eq!(lua.execute::<u32>("return #'abc'").unwrap(), 3);
    assert_eq!(lua.execute::<u32>("return #'a\\x00c'").unwrap(), 3);
    assert_eq!(lua.execute::<AnyLuaString>("return 'a\\x00c'").unwrap().0, vec!(97, 0, 99));
    assert_eq!(lua.execute::<AnyLuaString>("return 'a\\x00c'").unwrap().0.len(), 3);
    assert_eq!(lua.execute::<AnyLuaString>("return '\\x01\\xff'").unwrap().0, vec!(1, 255));
    lua.execute::<String>("return 'a\\x00\\xc0'").unwrap_err();
}

pub fn i32_to_string() {
    let lua = Lua::new();

    lua.set("a", 2);

    assert_eq!(lua.get("a"), None::<String>);
}

pub fn string_to_i32() {
    let lua = Lua::new();

    lua.set("a", "2");
    lua.set("b", "aaa");

    assert_eq!(lua.get("a"), None::<i32>);
    assert_eq!(lua.get("b"), None::<i32>);
}

pub fn string_on_lua() {
    let lua = Lua::new();

    lua.set("a", "aaa");
    {
        let x: StringInLua<_> = lua.get("a").unwrap();
        assert_eq!(&*x, "aaa");
    }

    lua.set("a", 18);
    {
        assert_eq!(lua.get("a"), None::<StringInLua<_>>);
    }
}

pub fn push_opt() {
    let lua = Lua::new();

    lua.set("some", function0(|| Some(123)));
    lua.set("none", function0(|| Option::None::<i32>));

    match lua.execute::<i32>("return some()") {
        Ok(123) => {}
        unexpected => panic!("{:?}", unexpected),
    }

    match lua.execute::<AnyLuaValue>("return none()") {
        Ok(AnyLuaValue::LuaNil) => {}
        unexpected => panic!("{:?}", unexpected),
    }

    lua.set("no_value", None::<i32>);
    lua.set("some_value", Some("Hello!"));

    assert_eq!(lua.get("no_value"), None::<String>);
    assert_eq!(lua.get("some_value"), Some("Hello!".to_string()));
}

pub fn read_nil() {
    let lua = Lua::new();
    assert_eq!(lua.execute::<Nil>("return nil").unwrap(), Nil);
    assert_eq!(lua.execute::<Option<i32>>("return nil").unwrap(), None);

    lua.set("v", None::<i32>);
    assert_eq!(lua.get::<i32, _>("v"), None);
    assert_eq!(lua.get::<Option<i32>, _>("v"), Some(None));
    assert_eq!(lua.get::<Option<Option<i32>>, _>("v"), Some(None));
}

