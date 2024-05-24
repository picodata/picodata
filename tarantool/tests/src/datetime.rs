use tarantool::{datetime::Datetime, tuple::Tuple};
use time_macros::datetime;

pub fn to_tuple() {
    let dt: Datetime = datetime!(2023-11-11 6:10:20.10010 -7).into();
    let t = Tuple::new(&[dt]).unwrap();
    let lua = tarantool::lua_state();
    let result: Datetime = lua.eval_with("return box.tuple.unpack(...)", &t).unwrap();
    assert_eq!(dt, result);
}

pub fn from_tuple() {
    let t: Tuple = tarantool::lua_state()
        .eval(
            "local dt = require('datetime').parse('2023-11-11T10:11:12.10142+0500')
            return box.tuple.new(dt)",
        )
        .unwrap();
    let (d,): (Datetime,) = t.decode().unwrap();
    assert_eq!(d.to_string(), "2023-11-11 10:11:12.10142 +05:00:00");
}

pub fn to_lua() {
    let lua = tarantool::lua_state();
    let dt: Datetime = datetime!(2006-8-13 21:45:13.042069 +3).into();
    let s: String = lua.eval_with("return tostring(...)", &dt).unwrap();
    assert_eq!(s, "2006-08-13T21:45:13.042069+0300");
}

pub fn from_lua() {
    let d: Datetime = tarantool::lua_state()
        .eval("return require('datetime').parse('2023-11-11T10:11:12.10142+0500')")
        .unwrap();

    assert_eq!(d.to_string(), "2023-11-11 10:11:12.10142 +05:00:00");
}
