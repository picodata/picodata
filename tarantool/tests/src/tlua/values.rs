use tarantool::tlua::{
    c_str,
    c_ptr,
    ffi,
    AsCData,
    AsLua,
    AsTable,
    AnyLuaValue,
    AnyLuaString,
    Lua,
    LuaTable,
    LuaFunction,
    StringInLua,
    function0,
    Nil,
    Null,
    True,
    False,
    Typename,
    ToString,
    Strict,
    CData,
    CDataOnStack,
};
use std::ffi::CString;
use std::os::raw::{c_char, c_void};

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
    let lua = tarantool::lua_state();

    let lua = lua.push(3);
    assert_eq!((&lua).read::<       isize >().ok(), Some(3));
    assert_eq!((&lua).read::<       i64   >().ok(), Some(3));
    assert_eq!((&lua).read::<       i32   >().ok(), Some(3));
    assert_eq!((&lua).read::<       i16   >().ok(), Some(3));
    assert_eq!((&lua).read::<       i8    >().ok(), Some(3));
    assert_eq!((&lua).read::<       usize >().ok(), Some(3));
    assert_eq!((&lua).read::<       u64   >().ok(), Some(3));
    assert_eq!((&lua).read::<       u32   >().ok(), Some(3));
    assert_eq!((&lua).read::<       u16   >().ok(), Some(3));
    assert_eq!((&lua).read::<       u8    >().ok(), Some(3));
    assert_eq!((&lua).read::<       f64   >().ok(), Some(3.0));
    assert_eq!((&lua).read::<       f32   >().ok(), Some(3.0));
    assert_eq!((&lua).read::<Strict<i8   >>().ok(), Some(Strict(3)));
    assert_eq!((&lua).read::<Strict<i16  >>().ok(), Some(Strict(3)));
    assert_eq!((&lua).read::<Strict<i32  >>().ok(), Some(Strict(3)));
    assert_eq!((&lua).read::<Strict<i64  >>().ok(), Some(Strict(3)));
    assert_eq!((&lua).read::<Strict<isize>>().ok(), Some(Strict(3)));
    assert_eq!((&lua).read::<Strict<u8   >>().ok(), Some(Strict(3)));
    assert_eq!((&lua).read::<Strict<u16  >>().ok(), Some(Strict(3)));
    assert_eq!((&lua).read::<Strict<u32  >>().ok(), Some(Strict(3)));
    assert_eq!((&lua).read::<Strict<u64  >>().ok(), Some(Strict(3)));
    assert_eq!((&lua).read::<Strict<usize>>().ok(), Some(Strict(3)));
    assert_eq!((&lua).read::<Strict<f32  >>().ok(), Some(Strict(3.0)));
    assert_eq!((&lua).read::<Strict<f64  >>().ok(), Some(Strict(3.0)));
    assert_eq!((&lua).read::<CData< i8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< i16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< isize>>().ok(), None);
    assert_eq!((&lua).read::<CData< u8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< u16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< usize>>().ok(), None);
    assert_eq!((&lua).read::<CData< f32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< f64  >>().ok(), None);

    let lua = lua.push(-69);
    assert_eq!((&lua).read::<isize>().unwrap(), -69);
    assert_eq!((&lua).read::<i64>().unwrap(), -69);
    assert_eq!((&lua).read::<i32>().unwrap(), -69);
    assert_eq!((&lua).read::<i16>().unwrap(), -69);
    assert_eq!((&lua).read::<i8>().unwrap(),  -69);
    assert_eq!((&lua).read::<usize>().unwrap(), usize::MAX - 69 + 1);
    assert_eq!((&lua).read::<u64>().unwrap(), u64::MAX - 69 + 1);
    assert_eq!((&lua).read::<u32>().unwrap(), u32::MAX - 69 + 1);
    assert_eq!((&lua).read::<u16>().unwrap(), u16::MAX - 69 + 1);
    assert_eq!((&lua).read::<u8 >().unwrap(), u8 ::MAX - 69 + 1);
    assert_eq!((&lua).read::<f64>().unwrap(), -69.);
    assert_eq!((&lua).read::<f32>().unwrap(), -69.);
    assert_eq!((&lua).read::<Strict<i8   >>().ok(), Some(Strict(-69)));
    assert_eq!((&lua).read::<Strict<i16  >>().ok(), Some(Strict(-69)));
    assert_eq!((&lua).read::<Strict<i32  >>().ok(), Some(Strict(-69)));
    assert_eq!((&lua).read::<Strict<i64  >>().ok(), Some(Strict(-69)));
    assert_eq!((&lua).read::<Strict<isize>>().ok(), Some(Strict(-69)));
    assert_eq!((&lua).read::<Strict<u8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u16  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u64  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<usize>>().ok(), None);
    assert_eq!((&lua).read::<Strict<f32  >>().ok(), Some(Strict(-69.0)));
    assert_eq!((&lua).read::<Strict<f64  >>().ok(), Some(Strict(-69.0)));
    assert_eq!((&lua).read::<CData< i8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< i16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< isize>>().ok(), None);
    assert_eq!((&lua).read::<CData< u8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< u16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< usize>>().ok(), None);
    assert_eq!((&lua).read::<CData< f32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< f64  >>().ok(), None);

    let lua = lua.push(420);
    assert_eq!((&lua).read::<       isize >().ok(), Some(420));
    assert_eq!((&lua).read::<       i64   >().ok(), Some(420));
    assert_eq!((&lua).read::<       i32   >().ok(), Some(420));
    assert_eq!((&lua).read::<       i16   >().ok(), Some(420));
    assert_eq!((&lua).read::<       i8    >().ok(), Some(-92));
    assert_eq!((&lua).read::<       usize >().ok(), Some(420));
    assert_eq!((&lua).read::<       u64   >().ok(), Some(420));
    assert_eq!((&lua).read::<       u32   >().ok(), Some(420));
    assert_eq!((&lua).read::<       u16   >().ok(), Some(420));
    assert_eq!((&lua).read::<       u8    >().ok(), Some(164));
    assert_eq!((&lua).read::<       f64   >().ok(), Some(420.0));
    assert_eq!((&lua).read::<       f32   >().ok(), Some(420.0));
    assert_eq!((&lua).read::<Strict<i8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i16  >>().ok(), Some(Strict(420)));
    assert_eq!((&lua).read::<Strict<i32  >>().ok(), Some(Strict(420)));
    assert_eq!((&lua).read::<Strict<i64  >>().ok(), Some(Strict(420)));
    assert_eq!((&lua).read::<Strict<isize>>().ok(), Some(Strict(420)));
    assert_eq!((&lua).read::<Strict<u8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u16  >>().ok(), Some(Strict(420)));
    assert_eq!((&lua).read::<Strict<u32  >>().ok(), Some(Strict(420)));
    assert_eq!((&lua).read::<Strict<u64  >>().ok(), Some(Strict(420)));
    assert_eq!((&lua).read::<Strict<usize>>().ok(), Some(Strict(420)));
    assert_eq!((&lua).read::<Strict<f32  >>().ok(), Some(Strict(420.0)));
    assert_eq!((&lua).read::<Strict<f64  >>().ok(), Some(Strict(420.0)));
    assert_eq!((&lua).read::<CData< i8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< i16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< isize>>().ok(), None);
    assert_eq!((&lua).read::<CData< u8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< u16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< usize>>().ok(), None);
    assert_eq!((&lua).read::<CData< f32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< f64  >>().ok(), None);

    let lua = lua.into_inner().push(3.14);
    assert_eq!((&lua).read::<i64>().unwrap(), 3);
    assert_eq!((&lua).read::<isize>().unwrap(), 3);
    assert_eq!((&lua).read::<i32>().unwrap(), 3);
    assert_eq!((&lua).read::<i16>().unwrap(), 3);
    assert_eq!((&lua).read::<i8 >().unwrap(), 3);
    assert_eq!((&lua).read::<u64>().unwrap(), 3);
    assert_eq!((&lua).read::<usize>().unwrap(), 3);
    assert_eq!((&lua).read::<u32>().unwrap(), 3);
    assert_eq!((&lua).read::<u16>().unwrap(), 3);
    assert_eq!((&lua).read::<u8 >().unwrap(), 3);
    assert_eq!((&lua).read::<f64>().unwrap(), 3.14);
    assert_eq!((&lua).read::<f32>().unwrap(), 3.14);
    assert_eq!((&lua).read::<Strict<i8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i16  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i64  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<isize>>().ok(), None);
    assert_eq!((&lua).read::<Strict<u8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u16  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u64  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<usize>>().ok(), None);
    assert_eq!((&lua).read::<Strict<f32  >>().ok(), Some(Strict(3.14)));
    assert_eq!((&lua).read::<Strict<f64  >>().ok(), Some(Strict(3.14)));
    assert_eq!((&lua).read::<CData< i8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< i16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< isize>>().ok(), None);
    assert_eq!((&lua).read::<CData< u8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< u16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< usize>>().ok(), None);
    assert_eq!((&lua).read::<CData< f32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< f64  >>().ok(), None);

    let lua = lua.into_inner().push(-1.5);
    assert_eq!((&lua).read::<i64>().unwrap(), -1);
    assert_eq!((&lua).read::<isize>().unwrap(), -1);
    assert_eq!((&lua).read::<i32>().unwrap(), -1);
    assert_eq!((&lua).read::<i16>().unwrap(), -1);
    assert_eq!((&lua).read::<i8 >().unwrap(), -1);
    assert_eq!((&lua).read::<u64>().unwrap(), u64::MAX - 1 + 1);
    assert_eq!((&lua).read::<usize>().unwrap(), usize::MAX - 1 + 1);
    assert_eq!((&lua).read::<u32>().unwrap(), u32::MAX - 1 + 1);
    assert_eq!((&lua).read::<u16>().unwrap(), u16::MAX - 1 + 1);
    assert_eq!((&lua).read::<u8 >().unwrap(), u8 ::MAX - 1 + 1);
    assert_eq!((&lua).read::<f64>().unwrap(), -1.5);
    assert_eq!((&lua).read::<f32>().unwrap(), -1.5);
    assert_eq!((&lua).read::<Strict<i8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i16  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i64  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<isize>>().ok(), None);
    assert_eq!((&lua).read::<Strict<u8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u16  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u64  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<usize>>().ok(), None);
    assert_eq!((&lua).read::<Strict<f32  >>().ok(), Some(Strict(-1.5)));
    assert_eq!((&lua).read::<Strict<f64  >>().ok(), Some(Strict(-1.5)));
    assert_eq!((&lua).read::<CData< i8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< i16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< isize>>().ok(), None);
    assert_eq!((&lua).read::<CData< u8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< u16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< usize>>().ok(), None);
    assert_eq!((&lua).read::<CData< f32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< f64  >>().ok(), None);

    let lua = lua.into_inner().push(0x77bbccddeeff0011i64);
    assert_eq!((&lua).read::<i64>().unwrap(), 0x77bbccddeeff0011i64);
    assert_eq!((&lua).read::<isize>().unwrap(), 0x77bbccddeeff0011isize);
    assert_eq!((&lua).read::<i32>().unwrap(), 0x77bbccddeeff0011i64 as i32);
    assert_eq!((&lua).read::<i16>().unwrap(), 0x77bbccddeeff0011i64 as i16);
    assert_eq!((&lua).read::<i8 >().unwrap(), 0x77bbccddeeff0011i64 as i8);
    assert_eq!((&lua).read::<u64>().unwrap(), 0x77bbccddeeff0011i64 as u64);
    assert_eq!((&lua).read::<usize>().unwrap(), 0x77bbccddeeff0011isize as usize);
    assert_eq!((&lua).read::<u32>().unwrap(), 0x77bbccddeeff0011i64 as u32);
    assert_eq!((&lua).read::<u16>().unwrap(), 0x77bbccddeeff0011i64 as u16);
    assert_eq!((&lua).read::<u8 >().unwrap(), 0x77bbccddeeff0011i64 as u8);
    assert_eq!((&lua).read::<f64>().unwrap(), 0x77bbccddeeff0011i64 as f64);
    assert_eq!((&lua).read::<f32>().unwrap(), 0x77bbccddeeff0011i64 as f32);
    assert_eq!((&lua).read::<Strict<i8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i16  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i64  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<isize>>().ok(), None);
    assert_eq!((&lua).read::<Strict<u8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u16  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u64  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<usize>>().ok(), None);
    assert_eq!((&lua).read::<Strict<f32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<f64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< i16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i64  >>().ok(), Some(CData(0x77bbccddeeff0011_i64)));
    assert_eq!((&lua).read::<CData< isize>>().ok(), Some(CData(0x77bbccddeeff0011_isize)));
    assert_eq!((&lua).read::<CData< u8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< u16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< usize>>().ok(), None);
    assert_eq!((&lua).read::<CData< f32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< f64  >>().ok(), None);

    let lua = lua.into_inner().push(0xaabbccddeeff0011u64);
    assert_eq!((&lua).read::<i64>().unwrap(), 0xaabbccddeeff0011u64 as i64);
    assert_eq!((&lua).read::<isize>().unwrap(), 0xaabbccddeeff0011usize as isize);
    assert_eq!((&lua).read::<i32>().unwrap(), 0xaabbccddeeff0011u64 as i32);
    assert_eq!((&lua).read::<i16>().unwrap(), 0xaabbccddeeff0011u64 as i16);
    assert_eq!((&lua).read::<i8 >().unwrap(), 0xaabbccddeeff0011u64 as i8);
    assert_eq!((&lua).read::<u64>().unwrap(), 0xaabbccddeeff0011u64);
    assert_eq!((&lua).read::<usize>().unwrap(), 0xaabbccddeeff0011usize);
    assert_eq!((&lua).read::<u32>().unwrap(), 0xaabbccddeeff0011u64 as u32);
    assert_eq!((&lua).read::<u16>().unwrap(), 0xaabbccddeeff0011u64 as u16);
    assert_eq!((&lua).read::<u8 >().unwrap(), 0xaabbccddeeff0011u64 as u8);
    assert_eq!((&lua).read::<f64>().unwrap(), 0xaabbccddeeff0011u64 as f64);
    assert_eq!((&lua).read::<f32>().unwrap(), 0xaabbccddeeff0011u64 as f32);
    assert_eq!((&lua).read::<Strict<i8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i16  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i64  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<isize>>().ok(), None);
    assert_eq!((&lua).read::<Strict<u8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u16  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u64  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<usize>>().ok(), None);
    assert_eq!((&lua).read::<Strict<f32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<f64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< i16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< isize>>().ok(), None);
    assert_eq!((&lua).read::<CData< u8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< u16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u64  >>().ok(), Some(CData(0xaabbccddeeff0011_u64)));
    assert_eq!((&lua).read::<CData< usize>>().ok(), Some(CData(0xaabbccddeeff0011_usize)));
    assert_eq!((&lua).read::<CData< f32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< f64  >>().ok(), None);

    let lua = lua.into_inner().push(f64::INFINITY);
    assert_eq!((&lua).read::<i64>().unwrap(), i64::MAX);
    assert_eq!((&lua).read::<isize>().unwrap(), isize::MAX);
    assert_eq!((&lua).read::<i32>().unwrap(), 0);
    assert_eq!((&lua).read::<i16>().unwrap(), 0);
    assert_eq!((&lua).read::<i8 >().unwrap(), 0);
    assert_eq!((&lua).read::<u64>().unwrap(), u64::MAX);
    assert_eq!((&lua).read::<usize>().unwrap(), usize::MAX);
    assert_eq!((&lua).read::<u32>().unwrap(), 0);
    assert_eq!((&lua).read::<u16>().unwrap(), 0);
    assert_eq!((&lua).read::<u8 >().unwrap(), 0);
    assert_eq!((&lua).read::<f64>().unwrap(), f64::INFINITY);
    assert_eq!((&lua).read::<f32>().unwrap(), f32::INFINITY);
    assert_eq!((&lua).read::<Strict<i8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i16  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i64  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<isize>>().ok(), None);
    assert_eq!((&lua).read::<Strict<u8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u16  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u64  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<usize>>().ok(), None);
    assert_eq!((&lua).read::<Strict<f32  >>().ok(), Some(Strict(f32::INFINITY)));
    assert_eq!((&lua).read::<Strict<f64  >>().ok(), Some(Strict(f64::INFINITY)));
    assert_eq!((&lua).read::<CData< i8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< i16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< isize>>().ok(), None);
    assert_eq!((&lua).read::<CData< u8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< u16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< usize>>().ok(), None);
    assert_eq!((&lua).read::<CData< f32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< f64  >>().ok(), None);

    let lua = lua.into_inner().push(f64::NEG_INFINITY);
    assert_eq!((&lua).read::<i64>().unwrap(), i64::MIN);
    assert_eq!((&lua).read::<isize>().unwrap(), isize::MIN);
    assert_eq!((&lua).read::<i32>().unwrap(), 0);
    assert_eq!((&lua).read::<i16>().unwrap(), 0);
    assert_eq!((&lua).read::<i8 >().unwrap(), 0);
    assert_eq!((&lua).read::<u64>().unwrap(), i64::MIN as u64);
    assert_eq!((&lua).read::<usize>().unwrap(), isize::MIN as usize);
    assert_eq!((&lua).read::<u32>().unwrap(), 0);
    assert_eq!((&lua).read::<u16>().unwrap(), 0);
    assert_eq!((&lua).read::<u8 >().unwrap(), 0);
    assert_eq!((&lua).read::<f64>().unwrap(), f64::NEG_INFINITY);
    assert_eq!((&lua).read::<f32>().unwrap(), f32::NEG_INFINITY);
    assert_eq!((&lua).read::<Strict<i8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i16  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i64  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<isize>>().ok(), None);
    assert_eq!((&lua).read::<Strict<u8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u16  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u64  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<usize>>().ok(), None);
    assert_eq!((&lua).read::<Strict<f32  >>().ok(), Some(Strict(f32::NEG_INFINITY)));
    assert_eq!((&lua).read::<Strict<f64  >>().ok(), Some(Strict(f64::NEG_INFINITY)));
    assert_eq!((&lua).read::<CData< i8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< i16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< isize>>().ok(), None);
    assert_eq!((&lua).read::<CData< u8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< u16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< usize>>().ok(), None);
    assert_eq!((&lua).read::<CData< f32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< f64  >>().ok(), None);

    let lua = lua.into_inner().push(f64::NAN);
    assert_eq!((&lua).read::<       isize >().ok(), Some(0));
    assert_eq!((&lua).read::<       i64   >().ok(), Some(0));
    assert_eq!((&lua).read::<       i32   >().ok(), Some(0));
    assert_eq!((&lua).read::<       i16   >().ok(), Some(0));
    assert_eq!((&lua).read::<       i8    >().ok(), Some(0));
    assert_eq!((&lua).read::<       usize >().ok(), Some(0));
    assert_eq!((&lua).read::<       u64   >().ok(), Some(0));
    assert_eq!((&lua).read::<       u32   >().ok(), Some(0));
    assert_eq!((&lua).read::<       u16   >().ok(), Some(0));
    assert_eq!((&lua).read::<       u8    >().ok(), Some(0));
    assert_eq!((&lua).read::<       f64   >().ok().map(f64::is_nan), Some(true));
    assert_eq!((&lua).read::<       f32   >().ok().map(f32::is_nan), Some(true));
    assert_eq!((&lua).read::<Strict<i8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i16  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<i64  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<isize>>().ok(), None);
    assert_eq!((&lua).read::<Strict<u8   >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u16  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u32  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<u64  >>().ok(), None);
    assert_eq!((&lua).read::<Strict<usize>>().ok(), None);
    assert_eq!((&lua).read::<Strict<f32  >>().ok().map(|f| f.0.is_nan()), Some(true));
    assert_eq!((&lua).read::<Strict<f64  >>().ok().map(|f| f.0.is_nan()), Some(true));
    assert_eq!((&lua).read::<CData< i8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< i16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< i64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< isize>>().ok(), None);
    assert_eq!((&lua).read::<CData< u8   >>().ok(), None);
    assert_eq!((&lua).read::<CData< u16  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< u64  >>().ok(), None);
    assert_eq!((&lua).read::<CData< usize>>().ok(), None);
    assert_eq!((&lua).read::<CData< f32  >>().ok(), None);
    assert_eq!((&lua).read::<CData< f64  >>().ok(), None);
}

pub fn cdata_numbers() {
    let lua = tarantool::lua_state();

    lua.exec("tmp = 0ull").unwrap();
    assert_eq!(lua.get::<i64, _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<isize, _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<i32, _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<i16, _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<i8 , _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<u64, _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<usize, _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<u32, _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<u16, _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<u8 , _>("tmp").unwrap(), 0);
    assert_eq!(lua.get::<f64, _>("tmp").unwrap(), 0.);
    assert_eq!(lua.get::<f32, _>("tmp").unwrap(), 0.);
    assert_eq!(lua.get::<Strict<i8   >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i16  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i64  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<isize>, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u8   >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u16  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u64  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<usize>, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<f32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<f64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i8   >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i16  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< isize>, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u8   >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u16  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u64  >, _>("tmp"), Some(CData(0)));
    assert_eq!(lua.get::<CData< usize>, _>("tmp"), Some(CData(0)));
    assert_eq!(lua.get::<CData< f32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< f64  >, _>("tmp"), None);

    lua.exec("tmp = require('ffi').new('double', 3.14)").unwrap();
    assert_eq!(lua.get::<i64, _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<isize, _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<i32, _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<i16, _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<i8 , _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<u64, _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<usize, _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<u32, _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<u16, _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<u8 , _>("tmp").unwrap(), 3);
    assert_eq!(lua.get::<f64, _>("tmp").unwrap(), 3.14);
    assert_eq!(lua.get::<f32, _>("tmp").unwrap(), 3.14);
    assert_eq!(lua.get::<Strict<i8   >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i16  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i64  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<isize>, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u8   >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u16  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u64  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<usize>, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<f32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<f64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i8   >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i16  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< isize>, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u8   >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u16  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< usize>, _>("tmp"), None);
    assert_eq!(lua.get::<CData< f32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< f64  >, _>("tmp"), Some(CData(3.14)));

    lua.exec("tmp = require('ffi').new('int8_t', 69)").unwrap();
    assert_eq!(lua.get::<i64, _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<isize, _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<i32, _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<i16, _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<i8 , _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<u64, _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<usize, _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<u32, _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<u16, _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<u8 , _>("tmp").unwrap(), 69);
    assert_eq!(lua.get::<f64, _>("tmp").unwrap(), 69.);
    assert_eq!(lua.get::<f32, _>("tmp").unwrap(), 69.);
    assert_eq!(lua.get::<Strict<i8   >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i16  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i64  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<isize>, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u8   >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u16  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u64  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<usize>, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<f32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<f64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i8   >, _>("tmp"), Some(CData(69)));
    assert_eq!(lua.get::<CData< i16  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< isize>, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u8   >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u16  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< usize>, _>("tmp"), None);
    assert_eq!(lua.get::<CData< f32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< f64  >, _>("tmp"), None);

    lua.exec("tmp = require('ffi').new('int16_t', 420)").unwrap();
    assert_eq!(lua.get::<i64, _>("tmp").unwrap(), 420);
    assert_eq!(lua.get::<isize, _>("tmp").unwrap(), 420);
    assert_eq!(lua.get::<i32, _>("tmp").unwrap(), 420);
    assert_eq!(lua.get::<i16, _>("tmp").unwrap(), 420);
    assert_eq!(lua.get::<i8 , _>("tmp").unwrap(), 420i16 as i8);
    assert_eq!(lua.get::<u64, _>("tmp").unwrap(), 420);
    assert_eq!(lua.get::<usize, _>("tmp").unwrap(), 420);
    assert_eq!(lua.get::<u32, _>("tmp").unwrap(), 420);
    assert_eq!(lua.get::<u16, _>("tmp").unwrap(), 420);
    assert_eq!(lua.get::<u8 , _>("tmp").unwrap(), 420i16 as u8);
    assert_eq!(lua.get::<f64, _>("tmp").unwrap(), 420.);
    assert_eq!(lua.get::<f32, _>("tmp").unwrap(), 420.);
    assert_eq!(lua.get::<Strict<i8   >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i16  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i64  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<isize>, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u8   >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u16  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u64  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<usize>, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<f32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<f64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i8   >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i16  >, _>("tmp"), Some(CData(420)));
    assert_eq!(lua.get::<CData< i32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< isize>, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u8   >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u16  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< usize>, _>("tmp"), None);
    assert_eq!(lua.get::<CData< f32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< f64  >, _>("tmp"), None);

    lua.exec("tmp = require('ffi').new('uint32_t', -1)").unwrap();
    assert_eq!(lua.get::<i64, _>("tmp").unwrap(), u32::MAX as i64);
    assert_eq!(lua.get::<isize, _>("tmp").unwrap(), u32::MAX as isize);
    assert_eq!(lua.get::<i32, _>("tmp").unwrap(), -1);
    assert_eq!(lua.get::<i16, _>("tmp").unwrap(), -1);
    assert_eq!(lua.get::<i8 , _>("tmp").unwrap(), -1);
    assert_eq!(lua.get::<u64, _>("tmp").unwrap(), u32::MAX as u64);
    assert_eq!(lua.get::<usize, _>("tmp").unwrap(), u32::MAX as usize);
    assert_eq!(lua.get::<u32, _>("tmp").unwrap(), u32::MAX);
    assert_eq!(lua.get::<u16, _>("tmp").unwrap(), u16::MAX);
    assert_eq!(lua.get::<u8 , _>("tmp").unwrap(), u8::MAX);
    assert_eq!(lua.get::<f64, _>("tmp").unwrap(), u32::MAX as f64);
    assert_eq!(lua.get::<f32, _>("tmp").unwrap(), u32::MAX as f32);
    assert_eq!(lua.get::<Strict<i8   >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i16  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i64  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<isize>, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u8   >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u16  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u64  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<usize>, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<f32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<f64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i8   >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i16  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< isize>, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u8   >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u16  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u32  >, _>("tmp"), Some(CData(u32::MAX)));
    assert_eq!(lua.get::<CData< u64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< usize>, _>("tmp"), None);
    assert_eq!(lua.get::<CData< f32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< f64  >, _>("tmp"), None);

    lua.exec("tmp = require('ffi').new('char', 255)").unwrap();
    assert_eq!(lua.get::<Strict<i8   >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i16  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<i64  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<isize>, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u8   >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u16  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<u64  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<usize>, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<f32  >, _>("tmp"), None);
    assert_eq!(lua.get::<Strict<f64  >, _>("tmp"), None);
    match <c_char>::MAX as i16 {
        signed if signed == i8::MAX as i16 => {
            assert_eq!(lua.get::<CData< i8>, _>("tmp"), Some(CData(-1)));
            assert_eq!(lua.get::<CData< u8>, _>("tmp"), None);
        }
        unsigned if unsigned == u8::MAX as i16 => {
            assert_eq!(lua.get::<CData< i8>, _>("tmp"), None);
            assert_eq!(lua.get::<CData< u8>, _>("tmp"), Some(CData(255)));
        }
        _ => unreachable!(),
    }
    assert_eq!(lua.get::<CData< i16  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< i64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< isize>, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u16  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< u64  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< usize>, _>("tmp"), None);
    assert_eq!(lua.get::<CData< f32  >, _>("tmp"), None);
    assert_eq!(lua.get::<CData< f64  >, _>("tmp"), None);
}

pub fn push_cdata() {
    let lua = tarantool::lua_state();
    lua.exec("ffi = require 'ffi'").unwrap();
    let f = LuaFunction::load(lua, "return ffi.typeof(...), ...").unwrap();

    let (ToString(ty), CData(num)): (_, CData<i8>) = f.call_with_args(CData(i8::MAX)).unwrap();
    assert_eq!((ty.as_str(), num), ("ctype<char>", i8::MAX));

    let (ToString(ty), CData(num)): (_, CData<i16>) = f.call_with_args(CData(i16::MAX)).unwrap();
    assert_eq!((ty.as_str(), num), ("ctype<short>", i16::MAX));

    let (ToString(ty), CData(num)): (_, CData<i32>) = f.call_with_args(CData(i32::MAX)).unwrap();
    assert_eq!((ty.as_str(), num), ("ctype<int>", i32::MAX));

    let (ToString(ty), CData(num)): (_, CData<i64>) = f.call_with_args(CData(i64::MAX)).unwrap();
    assert_eq!((ty.as_str(), num), ("ctype<int64_t>", i64::MAX));

    let (ToString(ty), CData(num)): (_, CData<isize>) = f.call_with_args(CData(isize::MAX)).unwrap();
    assert_eq!((ty.as_str(), num), ("ctype<int64_t>", isize::MAX));

    let (ToString(ty), CData(num)): (_, CData<u8>) = f.call_with_args(CData(u8::MAX)).unwrap();
    assert_eq!((ty.as_str(), num), ("ctype<unsigned char>", u8::MAX));

    let (ToString(ty), CData(num)): (_, CData<u16>) = f.call_with_args(CData(u16::MAX)).unwrap();
    assert_eq!((ty.as_str(), num), ("ctype<unsigned short>", u16::MAX));

    let (ToString(ty), CData(num)): (_, CData<u32>) = f.call_with_args(CData(u32::MAX)).unwrap();
    assert_eq!((ty.as_str(), num), ("ctype<unsigned int>", u32::MAX));

    let (ToString(ty), CData(num)): (_, CData<u64>) = f.call_with_args(CData(u64::MAX)).unwrap();
    assert_eq!((ty.as_str(), num), ("ctype<uint64_t>", u64::MAX));

    let (ToString(ty), CData(num)): (_, CData<usize>) = f.call_with_args(CData(usize::MAX)).unwrap();
    assert_eq!((ty.as_str(), num), ("ctype<uint64_t>", usize::MAX));

    let (ToString(ty), CData(num)): (_, CData<f32>) = f.call_with_args(CData(f32::MAX)).unwrap();
    assert_eq!((ty.as_str(), num), ("ctype<float>", f32::MAX));

    let (ToString(ty), CData(num)): (_, CData<f64>) = f.call_with_args(CData(f64::MAX)).unwrap();
    assert_eq!((ty.as_str(), num), ("ctype<double>", f64::MAX));

    let lua = f.into_inner().into_inner();

    use tarantool::tlua;
    #[repr(C)]
    #[derive(Clone, Copy, PartialEq, Eq, Debug, tlua::LuaRead)]
    struct S {
        i: i32,
        c: [u8; 4],
    }

    lua.exec("ffi.cdef[[ struct S { int i; char c[4]; }; ]]").unwrap();
    static mut CTID_STRUCT_S: Option<ffi::CTypeID> = None;
    unsafe {
        let ctid = ffi::luaL_ctypeid(lua.as_lua(), c_ptr!("struct S"));
        CTID_STRUCT_S = Some(ctid)
    }
    unsafe impl AsCData for S {
        fn ctypeid() -> ffi::CTypeID {
            unsafe { CTID_STRUCT_S.unwrap() }
        }
    }
    let s = S { i: 69, c: [110, 105, 99, 101] };
    lua.set("tmp", CData(s));

    // use the value from within lua
    let res: (i32, i8, i8, i8, i8) = lua.eval(
        "return tmp.i, tmp.c[0], tmp.c[1], tmp.c[2], tmp.c[3]",
    ).unwrap();
    assert_eq!(res, (69, 110, 105, 99, 101));

    // read the value directly into a rust value
    let CData(res): CData<S> = lua.get("tmp").unwrap();
    assert_eq!(res, s);

    // use the value as an indexable lua value inside lua stack
    let res: tlua::Indexable<_> = lua.get("tmp").unwrap();
    use tlua::Index;
    assert_eq!(res.get::<_, i32>("i").unwrap(), 69);
    let c: tlua::Indexable<_> = res.get("c").unwrap();
    assert_eq!(c.get::<_, u8>(0).unwrap(), 110);
    assert_eq!(c.get::<_, u8>(1).unwrap(), 105);
    assert_eq!(c.get::<_, u8>(2).unwrap(), 99);
    assert_eq!(c.get::<_, u8>(3).unwrap(), 101);

    // access raw cdata bytes
    let cdata: CDataOnStack<_> = lua.get("tmp").unwrap();
    assert_eq!(cdata.data(), b"\x45\x00\x00\x00nice");
}

pub fn as_cdata_wrong_size() {
    #[derive(Debug)]
    struct WrongSize(u64);
    unsafe impl AsCData for WrongSize {
        fn ctypeid() -> ffi::CTypeID {
            ffi::CTID_UINT32
        }
    }
    let lua = tarantool::lua_state();
    let cdata: CDataOnStack<_> = lua.eval("return require'ffi'.new('uint32_t', 69)").unwrap();
    // This will panic in debug build, because of the size mismatch. Expected
    // size of cdata to be 8 bytes (u64) but actual is 4 bytes.
    let _: Option<&WrongSize> = cdata.try_downcast::<WrongSize>();
}

pub fn cdata_on_stack() {
    let lua = tarantool::lua_state();
    let val = lua.eval("return require('ffi').new('uint32_t', 0x01020304)").unwrap();
    let mut cdata: CDataOnStack<_> = val;
    assert_eq!(cdata.ctypeid(), ffi::CTID_UINT32);
    assert_eq!(cdata.data(), [4, 3, 2, 1]);
    let n: u32 = lua.eval_with("return ... + 1", &cdata).unwrap();
    assert_eq!(n, 0x01020305);
    cdata.data_mut().sort_unstable();
    assert_eq!(cdata.try_downcast::<u32>().unwrap(), &0x04030201_u32);

    let cdata: CDataOnStack<_> = lua.eval(
        "return require'ffi'.new('char[4]', 'abcd')"
    ).unwrap();
    assert_eq!(cdata.data(), b"abcd");
    let cdata: CDataOnStack<_> = lua.eval_with(
        "return require('ffi').cast('void const *', ...)",
        &cdata,
    ).unwrap();
    assert!(cdata.try_downcast::<*mut c_void>().is_none());
    let ptr = cdata.try_downcast::<*const c_void>().unwrap();
    let s = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr.cast(), 4))
    };
    assert_eq!(s, "abcd");
}

pub fn readwrite_floats() {
    let lua = Lua::new();

    lua.set("a", 2.51234_f32);
    lua.set("b", 3.4123456789_f64);

    let x: f32 = lua.get("a").unwrap();
    assert!(x - 2.51234 < 0.000001);

    let y: f64 = lua.get("a").unwrap();
    assert!(y - 2.51234 < 0.000001);

    let z: f32 = lua.get("b").unwrap();
    assert!(z - 3.4123456 < 0.000001);

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
    lua.set("c", c_str!("can you hear me?"));
    lua.set("d", CString::new("HELLO!!!").unwrap());

    let x: String = lua.get("a").unwrap();
    assert_eq!(x, "hello");

    let y: String = lua.get("b").unwrap();
    assert_eq!(y, "hello");

    assert_eq!(lua.get::<String, _>("c").unwrap(), "can you hear me?");
    assert_eq!(lua.get::<String, _>("d").unwrap(), "HELLO!!!");

    assert_eq!(lua.eval::<String>("return 'abc'").unwrap(), "abc");
    assert_eq!(lua.eval::<CString>("return 'abc'").unwrap().as_ref(), c_str!("abc"));
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
    let lua = tarantool::lua_state();
    assert_eq!(lua.eval::<Nil>("return nil").unwrap(), Nil);
    assert_eq!(lua.eval::<Option<i32>>("return nil").unwrap(), None);
    assert_eq!(lua.eval::<Null>("return box.NULL").unwrap(), Null);
    assert_eq!(lua.eval::<Option<i32>>("return box.NULL").unwrap(), None);

    let lua = Lua::new();
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

pub fn tuple_as_table() {
    let lua = Lua::new();

    let v = (&lua).try_push(AsTable((1, 2, 3))).unwrap();
    assert_eq!(v.read::<[i32; 3]>().ok(), Some([1, 2, 3]));

    let v = (&lua).try_push(
        AsTable((
            ("foo", 2),
            (69, "nice"),
            ("bar", AsTable((1, 2, 3))),
        ))
    ).unwrap();
    let table = v.read::<LuaTable<_>>().unwrap();
    assert_eq!(table.get("foo"), Some(2));
    assert_eq!(table.get(69), Some("nice".to_string()));
    assert_eq!(table.get("bar"), Some([1, 2, 3]));

    let v = (&lua).try_push(
        AsTable((
            ("foo", 2),
            (69, "nice"),
            ("bar", AsTable((1, 2, 3))),
        ))
    ).unwrap();
    let table = v.read::<LuaTable<_>>().unwrap();
    assert_eq!(table.get("foo"), Some(2));
    assert_eq!(table.get(69), Some("nice".to_string()));
    assert_eq!(table.get("bar"), Some([1, 2, 3]));

    let table = LuaTable::empty(&lua);
    table.set(1, "one");
    table.set(2, 69);
    table.set(3, [1, 2, 3]);
    lua.set("tuple_as_table::gvar", &table);
    assert_eq!(
        lua.get("tuple_as_table::gvar"),
        Some(AsTable(("one".to_string(), 69, AsTable((1, 2, 3))))),
    );

    let (e, _) = (&lua).try_push(AsTable(((1, 2), (1, 2, 3)))).unwrap_err();
    assert_eq!(
        e.to_string(),
        "Can only push 1 or 2 values as lua table item"
    );
}

