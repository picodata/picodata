use tarantool::tlua::{
    AsLua,
    AnyLuaValue,
    AnyLuaString,
    Lua,
    StringInLua,
    function0,
    Nil,
    Null,
    True,
    False,
    Typename,
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
    let lua = tarantool::global_lua();

    let lua = lua.push(-69);
    assert_eq!((&lua).read::<i64>().unwrap(), -69);
    assert_eq!((&lua).read::<i32>().unwrap(), -69);
    assert_eq!((&lua).read::<i16>().unwrap(), -69);
    assert_eq!((&lua).read::<i8>().unwrap(),  -69);
    assert_eq!((&lua).read::<u64>().unwrap(), u64::MAX - 69 + 1);
    assert_eq!((&lua).read::<u32>().unwrap(), u32::MAX - 69 + 1);
    assert_eq!((&lua).read::<u16>().unwrap(), u16::MAX - 69 + 1);
    assert_eq!((&lua).read::<u8 >().unwrap(), u8 ::MAX - 69 + 1);
    assert_eq!((&lua).read::<f64>().unwrap(), -69.);
    assert_eq!((&lua).read::<f32>().unwrap(), -69.);

    let lua = lua.into_inner().push(3.14);
    assert_eq!((&lua).read::<i64>().unwrap(), 3);
    assert_eq!((&lua).read::<i32>().unwrap(), 3);
    assert_eq!((&lua).read::<i16>().unwrap(), 3);
    assert_eq!((&lua).read::<i8 >().unwrap(), 3);
    assert_eq!((&lua).read::<u64>().unwrap(), 3);
    assert_eq!((&lua).read::<u32>().unwrap(), 3);
    assert_eq!((&lua).read::<u16>().unwrap(), 3);
    assert_eq!((&lua).read::<u8 >().unwrap(), 3);
    assert_eq!((&lua).read::<f64>().unwrap(), 3.14);
    assert_eq!((&lua).read::<f32>().unwrap(), 3.14);

    let lua = lua.into_inner().push(-1.5);
    assert_eq!((&lua).read::<i64>().unwrap(), -1);
    assert_eq!((&lua).read::<i32>().unwrap(), -1);
    assert_eq!((&lua).read::<i16>().unwrap(), -1);
    assert_eq!((&lua).read::<i8 >().unwrap(), -1);
    assert_eq!((&lua).read::<u64>().unwrap(), u64::MAX - 1 + 1);
    assert_eq!((&lua).read::<u32>().unwrap(), u32::MAX - 1 + 1);
    assert_eq!((&lua).read::<u16>().unwrap(), u16::MAX - 1 + 1);
    assert_eq!((&lua).read::<u8 >().unwrap(), u8 ::MAX - 1 + 1);
    assert_eq!((&lua).read::<f64>().unwrap(), -1.5);
    assert_eq!((&lua).read::<f32>().unwrap(), -1.5);

    let lua = lua.into_inner().push(0x77bbccddeeff0011i64);
    assert_eq!((&lua).read::<i64>().unwrap(), 0x77bbccddeeff0011i64);
    assert_eq!((&lua).read::<i32>().unwrap(), 0x77bbccddeeff0011i64 as i32);
    assert_eq!((&lua).read::<i16>().unwrap(), 0x77bbccddeeff0011i64 as i16);
    assert_eq!((&lua).read::<i8 >().unwrap(), 0x77bbccddeeff0011i64 as i8);
    assert_eq!((&lua).read::<u64>().unwrap(), 0x77bbccddeeff0011i64 as u64);
    assert_eq!((&lua).read::<u32>().unwrap(), 0x77bbccddeeff0011i64 as u32);
    assert_eq!((&lua).read::<u16>().unwrap(), 0x77bbccddeeff0011i64 as u16);
    assert_eq!((&lua).read::<u8 >().unwrap(), 0x77bbccddeeff0011i64 as u8);
    assert_eq!((&lua).read::<f64>().unwrap(), 0x77bbccddeeff0011i64 as f64);
    assert_eq!((&lua).read::<f32>().unwrap(), 0x77bbccddeeff0011i64 as f32);

    let lua = lua.into_inner().push(0xaabbccddeeff0011u64);
    assert_eq!((&lua).read::<i64>().unwrap(), 0xaabbccddeeff0011u64 as i64);
    assert_eq!((&lua).read::<i32>().unwrap(), 0xaabbccddeeff0011u64 as i32);
    assert_eq!((&lua).read::<i16>().unwrap(), 0xaabbccddeeff0011u64 as i16);
    assert_eq!((&lua).read::<i8 >().unwrap(), 0xaabbccddeeff0011u64 as i8);
    assert_eq!((&lua).read::<u64>().unwrap(), 0xaabbccddeeff0011u64);
    assert_eq!((&lua).read::<u32>().unwrap(), 0xaabbccddeeff0011u64 as u32);
    assert_eq!((&lua).read::<u16>().unwrap(), 0xaabbccddeeff0011u64 as u16);
    assert_eq!((&lua).read::<u8 >().unwrap(), 0xaabbccddeeff0011u64 as u8);
    assert_eq!((&lua).read::<f64>().unwrap(), 0xaabbccddeeff0011u64 as f64);
    assert_eq!((&lua).read::<f32>().unwrap(), 0xaabbccddeeff0011u64 as f32);

    let lua = lua.into_inner().push(f64::INFINITY);
    assert_eq!((&lua).read::<i64>().unwrap(), i64::MAX);
    assert_eq!((&lua).read::<i32>().unwrap(), 0);
    assert_eq!((&lua).read::<i16>().unwrap(), 0);
    assert_eq!((&lua).read::<i8 >().unwrap(), 0);
    assert_eq!((&lua).read::<u64>().unwrap(), u64::MAX);
    assert_eq!((&lua).read::<u32>().unwrap(), 0);
    assert_eq!((&lua).read::<u16>().unwrap(), 0);
    assert_eq!((&lua).read::<u8 >().unwrap(), 0);
    assert_eq!((&lua).read::<f64>().unwrap(), f64::INFINITY);
    assert_eq!((&lua).read::<f32>().unwrap(), f32::INFINITY);

    let lua = lua.into_inner().push(f64::NEG_INFINITY);
    assert_eq!((&lua).read::<i64>().unwrap(), i64::MIN);
    assert_eq!((&lua).read::<i32>().unwrap(), 0);
    assert_eq!((&lua).read::<i16>().unwrap(), 0);
    assert_eq!((&lua).read::<i8 >().unwrap(), 0);
    assert_eq!((&lua).read::<u64>().unwrap(), i64::MIN as u64);
    assert_eq!((&lua).read::<u32>().unwrap(), 0);
    assert_eq!((&lua).read::<u16>().unwrap(), 0);
    assert_eq!((&lua).read::<u8 >().unwrap(), 0);
    assert_eq!((&lua).read::<f64>().unwrap(), f64::NEG_INFINITY);
    assert_eq!((&lua).read::<f32>().unwrap(), f32::NEG_INFINITY);
}

pub fn cdata_numbers() {
    let lua = tarantool::global_lua();

    lua.exec("tmp = 0ull").unwrap();
    assert_eq!(lua.get::<i64, _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<i32, _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<i16, _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<i8 , _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<u64, _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<u32, _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<u16, _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<u8 , _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<f64, _>("tmp").unwrap(), 0.);
    assert_eq!(lua.get::<f32, _>("tmp").unwrap(), 0.);

    lua.exec("tmp = require('ffi').new('double', 3.14)").unwrap();
    assert_eq!(lua.get::<i64, _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<i32, _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<i16, _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<i8 , _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<u64, _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<u32, _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<u16, _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<u8 , _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<f64, _>("tmp").unwrap(), 3.14);
    assert_eq!(lua.get::<f32, _>("tmp").unwrap(), 3.14);

    lua.exec("tmp = require('ffi').new('int8_t', 69)").unwrap();
    assert_eq!(lua.get::<i64, _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<i32, _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<i16, _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<i8 , _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<u64, _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<u32, _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<u16, _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<u8 , _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<f64, _>("tmp").unwrap(), 69.);
    assert_eq!(lua.get::<f32, _>("tmp").unwrap(), 69.);

    lua.exec("tmp = require('ffi').new('int16_t', 420)").unwrap();
    assert_eq!(lua.get::<i64, _>("tmp").unwrap(), 420);
    assert_eq!(lua.get::<i32, _>("tmp").unwrap(), 420);
    assert_eq!(lua.get::<i16, _>("tmp").unwrap(), 420);
    assert_eq!(lua.get::<i8 , _>("tmp").unwrap(), 420i16 as i8);
    assert_eq!(lua.get::<u64, _>("tmp").unwrap(), 420);
    assert_eq!(lua.get::<u32, _>("tmp").unwrap(), 420);
    assert_eq!(lua.get::<u16, _>("tmp").unwrap(), 420);
    assert_eq!(lua.get::<u8 , _>("tmp").unwrap(), 420i16 as u8);
    assert_eq!(lua.get::<f64, _>("tmp").unwrap(), 420.);
    assert_eq!(lua.get::<f32, _>("tmp").unwrap(), 420.);

    lua.exec("tmp = require('ffi').new('uint32_t', -1)").unwrap();
    assert_eq!(lua.get::<i64, _>("tmp").unwrap(), u32::MAX as i64);
    assert_eq!(lua.get::<i32, _>("tmp").unwrap(), -1);
    assert_eq!(lua.get::<i16, _>("tmp").unwrap(), -1);
    assert_eq!(lua.get::<i8 , _>("tmp").unwrap(), -1);
    assert_eq!(lua.get::<u64, _>("tmp").unwrap(), u32::MAX as u64);
    assert_eq!(lua.get::<u32, _>("tmp").unwrap(), u32::MAX);
    assert_eq!(lua.get::<u16, _>("tmp").unwrap(), u16::MAX);
    assert_eq!(lua.get::<u8 , _>("tmp").unwrap(), u8::MAX);
    assert_eq!(lua.get::<f64, _>("tmp").unwrap(), u32::MAX as f64);
    assert_eq!(lua.get::<f32, _>("tmp").unwrap(), u32::MAX as f32);
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

    let lua = lua.push(true);
    assert_eq!((&lua).read::<bool>().ok(), Some(true));
    assert_eq!((&lua).read::<True>().ok(), Some(True));
    assert_eq!((&lua).read::<False>().ok(), None);

    let lua = lua.into_inner();

    let lua = lua.push(false);
    assert_eq!((&lua).read::<bool>().ok(), Some(false));
    assert_eq!((&lua).read::<True>().ok(), None);
    assert_eq!((&lua).read::<False>().ok(), Some(False));
}

pub fn readwrite_strings() {
    let lua = Lua::new();

    lua.set("a", "hello");
    lua.set("b", &"hello".to_string());

    let x: String = lua.get("a").unwrap();
    assert_eq!(x, "hello");

    let y: String = lua.get("b").unwrap();
    assert_eq!(y, "hello");

    assert_eq!(lua.eval::<String>("return 'abc'").unwrap(), "abc");
    assert_eq!(lua.eval::<u32>("return #'abc'").unwrap(), 3);
    assert_eq!(lua.eval::<u32>("return #'a\\x00c'").unwrap(), 3);
    assert_eq!(lua.eval::<AnyLuaString>("return 'a\\x00c'").unwrap().0, vec!(97, 0, 99));
    assert_eq!(lua.eval::<AnyLuaString>("return 'a\\x00c'").unwrap().0.len(), 3);
    assert_eq!(lua.eval::<AnyLuaString>("return '\\x01\\xff'").unwrap().0, vec!(1, 255));
    lua.eval::<String>("return 'a\\x00\\xc0'").unwrap_err();
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

    match lua.eval::<i32>("return some()") {
        Ok(123) => {}
        unexpected => panic!("{:?}", unexpected),
    }

    match lua.eval::<AnyLuaValue>("return none()") {
        Ok(AnyLuaValue::LuaNil) => {}
        unexpected => panic!("{:?}", unexpected),
    }

    lua.set("no_value", None::<i32>);
    lua.set("some_value", Some("Hello!"));

    assert_eq!(lua.get("no_value"), None::<String>);
    assert_eq!(lua.get("some_value"), Some("Hello!".to_string()));
}

pub fn read_nil() {
    let lua = tarantool::global_lua();
    assert_eq!(lua.eval::<Nil>("return nil").unwrap(), Nil);
    assert_eq!(lua.eval::<Option<i32>>("return nil").unwrap(), None);
    assert_eq!(lua.eval::<Null>("return box.NULL").unwrap(), Null);
    assert_eq!(lua.eval::<Option<i32>>("return box.NULL").unwrap(), None);

    lua.set("v", None::<i32>);
    assert_eq!(lua.get::<i32, _>("v"), None);
    assert_eq!(lua.get::<Option<i32>, _>("v"), Some(None));
    assert_eq!(lua.get::<Option<Option<i32>>, _>("v"), Some(None));
}

pub fn typename() {
    let lua = Lua::new();
    assert_eq!((&lua).push("hello").read::<Typename>().unwrap().get(), "string");
    assert_eq!((&lua).push(3.14).read::<Typename>().unwrap().get(), "number");
    assert_eq!((&lua).push(true).read::<Typename>().unwrap().get(), "boolean");
    assert_eq!((&lua).push(vec![1, 2, 3]).read::<Typename>().unwrap().get(), "table");
}

