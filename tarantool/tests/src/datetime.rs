use tarantool::{datetime::Datetime, tlua::LuaFunction, tuple::Tuple};
use time_macros::datetime;

pub fn to_tuple() {
    let dt: Datetime = datetime!(2023-11-11 6:10:20.10010 -7).into();
    let t = Tuple::new(&[dt]).unwrap();
    let lua = tarantool::lua_state();
    let f: LuaFunction<_> = lua.eval("return box.tuple.unpack").unwrap();
    let result: Datetime = f.call_with_args(&t).unwrap();
    assert_eq!(dt, result);
}

pub fn from_tuple() {
    let t: Tuple = tarantool::lua_state()
        .eval("return box.tuple.new(require('datetime').parse('2023-11-11T10:11:12.10142+0500'))")
        .unwrap();
    let (d,): (Datetime,) = t.decode().unwrap();
    assert_eq!(d.to_string(), "2023-11-11 10:11:12.10142 +05:00:00");
}

pub fn to_lua() {
    let lua = tarantool::lua_state();
    let tostring: LuaFunction<_> = lua.eval("return tostring").unwrap();
    let dt: Datetime = datetime!(2023-11-11 6:10:20.10010 -7).into();
    let s: String = tostring.call_with_args(dt).unwrap();
    assert_eq!(s, "2023-11-11T06:10:20.100100-0700");
}

pub fn from_lua() {
    let d: Datetime = tarantool::lua_state()
        .eval("return require('datetime').parse('2023-11-11T10:11:12.10142+0500')")
        .unwrap();

    assert_eq!(d.to_string(), "2023-11-11 10:11:12.10142 +05:00:00");
}
