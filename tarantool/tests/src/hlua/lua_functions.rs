
use tarantool::hlua::{
    LuaError,
    LuaFunction,
    LuaTable,
};
use std::io::Read;

pub fn basic() {
    let mut lua = crate::hlua::global();
    let mut f = LuaFunction::load(&mut lua, "return 5;").unwrap();
    let val: i32 = f.call().unwrap();
    assert_eq!(val, 5);
}

pub fn args() {
    let mut lua = crate::hlua::global();
    lua.execute::<()>("function foo(a) return a * 5 end").unwrap();
    let val: i32 = lua.get::<LuaFunction<_>, _>("foo").unwrap().call_with_args(3).unwrap();
    assert_eq!(val, 15);
}

pub fn args_in_order() {
    let mut lua = crate::hlua::global();
    lua.execute::<()>("function foo(a, b) return a - b end").unwrap();
    let val: i32 = lua.get::<LuaFunction<_>, _>("foo").unwrap().call_with_args((5, 3)).unwrap();
    assert_eq!(val, 2);
}

pub fn syntax_error() {
    let mut lua = crate::hlua::global();
    match LuaFunction::load(&mut lua, "azerazer") {
        Err(LuaError::SyntaxError(_)) => (),
        _ => panic!(),
    };
}

pub fn execution_error() {
    let mut lua = crate::hlua::global();
    let mut f = LuaFunction::load(&mut lua, "return a:hello()").unwrap();
    match f.call::<()>() {
        Err(LuaError::ExecutionError(_)) => (),
        _ => panic!(),
    };
}

pub fn check_types() {
    let mut lua = crate::hlua::global();
    let mut f = LuaFunction::load(&mut lua, "return 12").unwrap();
    let err = f.call::<bool>().unwrap_err();
    match err {
        LuaError::WrongType{ref rust_expected, ref lua_actual} => {
            assert_eq!(rust_expected, "bool");
            assert_eq!(lua_actual, "number");
        },
        v => panic!("{}", v),
    };
    assert_eq!(
        err.to_string(),
        "Wrong type returned by Lua: bool expected, got number"
    );

    assert_eq!(f.call::<i32>().unwrap(), 12i32);
    assert_eq!(f.call::<f32>().unwrap(), 12f32);
    assert_eq!(f.call::<f64>().unwrap(), 12f64);
    assert_eq!(f.call::<String>().unwrap(), "12".to_string());
}

pub fn call_and_read_table() {
    let mut lua = crate::hlua::global();
    let mut f = LuaFunction::load(&mut lua, "return {1, 2, 3};").unwrap();
    let mut val: LuaTable<_> = f.call().unwrap();
    assert_eq!(val.get::<u8, _, _>(2).unwrap(), 2);
}

pub fn lua_function_returns_function() {
    let mut lua = crate::hlua::global();
    lua.execute::<()>("function foo() return 5 end").unwrap();
    let mut bar = LuaFunction::load(&mut lua, "return foo;").unwrap();
    let mut foo: LuaFunction<_> = bar.call().unwrap();
    let val: i32 = foo.call().unwrap();
    assert_eq!(val, 5);
}

pub fn execute_from_reader_errors_if_cant_read() {
    struct Reader { }

    impl Read for Reader {
        fn read(&mut self, _: &mut [u8]) -> ::std::io::Result<usize> {
            use std::io::{Error, ErrorKind};
            Err(Error::new(ErrorKind::Other, "oh no!"))
        }
    }

    let mut lua = crate::hlua::global();
    let reader = Reader { };
    let res: Result<(), _> = lua.execute_from_reader(reader);
    match res {
        Ok(_) => panic!("Reading succeded"),
        Err(LuaError::ReadError(e)) => { assert_eq!("oh no!", e.to_string()) },
        Err(_) => panic!("Unexpected error happened"),
    }
}
