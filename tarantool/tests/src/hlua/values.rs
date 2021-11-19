use tarantool::hlua::{
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

    let x: String = lua.get("a").unwrap();
    assert_eq!(x, "2");
}

pub fn string_to_i32() {
    let lua = Lua::new();

    lua.set("a", "2");
    lua.set("b", "aaa");

    let x: i32 = lua.get("a").unwrap();
    assert_eq!(x, 2);

    let y: Option<i32> = lua.get("b");
    assert!(y.is_none());
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
        let x: StringInLua<_> = lua.get("a").unwrap();
        assert_eq!(&*x, "18");
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
}

