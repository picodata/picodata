use tarantool::hlua::{
    LuaError,
    LuaFunction,
    LuaTable,
    MethodCallError,
};
use std::io::Read;
use std::collections::HashMap;

pub fn basic() {
    let lua = crate::hlua::global();
    let f = LuaFunction::load(&lua, "return 5;").unwrap();
    let val: i32 = f.call().unwrap();
    assert_eq!(val, 5);
}

pub fn two_functions_at_the_same_time() {
    let lua = crate::hlua::global();
    let f1 = LuaFunction::load(&lua, "return 69;").unwrap();
    let f2 = LuaFunction::load(&lua, "return 420;").unwrap();
    assert_eq!(f1.call::<i32>().unwrap(), 69);
    assert_eq!(f2.call::<i32>().unwrap(), 420);
}

pub fn args() {
    let lua = crate::hlua::global();
    lua.execute::<()>("function foo(a) return a * 5 end").unwrap();
    let val: i32 = lua.get::<LuaFunction<_>, _>("foo").unwrap().call_with_args(3).unwrap();
    assert_eq!(val, 15);
}

pub fn args_in_order() {
    let lua = crate::hlua::global();
    lua.execute::<()>("function foo(a, b) return a - b end").unwrap();
    let val: i32 = lua.get::<LuaFunction<_>, _>("foo").unwrap().call_with_args((5, 3)).unwrap();
    assert_eq!(val, 2);
}

pub fn syntax_error() {
    let lua = crate::hlua::global();
    match LuaFunction::load(&lua, "azerazer") {
        Err(LuaError::SyntaxError(_)) => (),
        _ => panic!(),
    };
}

pub fn execution_error() {
    let lua = crate::hlua::global();
    let f = LuaFunction::load(&lua, "return a:hello()").unwrap();
    match f.call::<()>() {
        Err(LuaError::ExecutionError(_)) => (),
        _ => panic!(),
    };
}

pub fn check_types() {
    let lua = crate::hlua::global();
    let f = LuaFunction::load(&lua, "return 12").unwrap();
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
    let lua = crate::hlua::global();
    let f = LuaFunction::load(&lua, "return {1, 2, 3};").unwrap();
    let val: LuaTable<_> = f.call().unwrap();
    assert_eq!(val.get::<u8, _>(2).unwrap(), 2);
}

pub fn table_as_args() {
    let lua = crate::hlua::global();
    let f: LuaFunction<_> = lua.execute("return function(a) return a.foo end").unwrap();
    let t: LuaTable<_> = lua.execute("return { foo = 69 };").unwrap();
    let val: i32 = f.call_with_args(&t).unwrap();
    assert_eq!(val, 69);

    let f: LuaFunction<_> = lua.execute("return function(a, b) return a.foo + b.bar end").unwrap();
    let u: LuaTable<_> = lua.execute("return { bar = 420 };").unwrap();
    let val: i32 = f.call_with_args((&t, &u)).unwrap();
    assert_eq!(val, 420 + 69);

    let json_encode: LuaFunction<_> = lua.execute("return require('json').encode").unwrap();
    let res: String = json_encode.call_with_args(vec!("a", "b", "c")).unwrap();
    assert_eq!(res, r#"["a","b","c"]"#);

    let mut t = HashMap::new();
    t.insert("foo", "bar");
    let res: String = json_encode.call_with_args(t).unwrap();
    assert_eq!(res, r#"{"foo":"bar"}"#);
}

#[rustfmt::skip]
pub fn table_method_call() {
    let lua = crate::hlua::global();
    let t: LuaTable<_> = lua.execute("
        return {
            a = 0,
            inc_a = function(self, b, c)
                self.a = self.a + (b or 1) + (c or 0)
            end
        }
    ").unwrap();
    let method: LuaFunction<_> = t.get("inc_a").unwrap();
    let () = method.call_with_args(&t).unwrap();
    assert_eq!(t.get::<i32, _>("a"), Some(1));
    let () = method.call_with_args((&t, 2)).unwrap();
    assert_eq!(t.get::<i32, _>("a"), Some(3));

    let () = t.call_method("inc_a", ()).unwrap();
    assert_eq!(t.get::<i32, _>("a"), Some(4));
    let () = t.call_method("inc_a", (2,)).unwrap();
    assert_eq!(t.get::<i32, _>("a"), Some(6));
    let () = t.call_method("inc_a", (2, 3)).unwrap();
    assert_eq!(t.get::<i32, _>("a"), Some(11));

    let e = t.call_method::<(), _>("inc_b", ()).unwrap_err();
    assert!(matches!(e, MethodCallError::NoSuchMethod));
}

pub fn lua_function_returns_function() {
    let lua = crate::hlua::global();
    lua.execute::<()>("function foo() return 5 end").unwrap();
    let bar = LuaFunction::load(&lua, "return foo;").unwrap();
    let foo: LuaFunction<_> = bar.call().unwrap();
    let val: i32 = foo.call().unwrap();
    assert_eq!(val, 5);
}

pub fn error() {
    let lua = crate::hlua::global();
    lua.execute::<()>("function foo() error('oops'); end").unwrap();
    let foo: LuaFunction<_> = lua.get("foo").unwrap();
    let res: Result<(), _> = foo.call();
    assert!(res.is_err());
    if let Err(LuaError::ExecutionError(msg)) = res {
        assert_eq!(msg, "[string \"chunk\"]:1: oops");
    }
}

pub fn either_or() {
    let lua = crate::hlua::global();
    lua.execute::<()>(r#"
        function foo(a)
            if a > 0 then
                return true, 69, 420
            else
                return false, "hello"
            end
        end
    "#).unwrap();
    let foo: LuaFunction<_> = lua.get("foo").unwrap();
    type Res = Result<(bool, i32, i32), (bool, String)>;
    let res: Res = foo.call_with_args(1).unwrap();
    assert_eq!(res, Ok((true, 69, 420)));
    let res: Res = foo.call_with_args(0).unwrap();
    assert_eq!(res, Err((false, "hello".to_string())));
}

pub fn multiple_return_values() {
    let lua = crate::hlua::global();
    let f = LuaFunction::load(&lua, r#"return 69, "foo", 3.14, true;"#).unwrap();
    let res: (i32, String, f64, bool) = f.call().unwrap();
    assert_eq!(res, (69, "foo".to_string(), 3.14, true));
}

pub fn multiple_return_values_fail() {
    let lua = crate::hlua::global();
    let f = LuaFunction::load(&lua, "return 1, 2, 3;").unwrap();
    assert_eq!(f.call::<i32>().unwrap(), 1);
    assert_eq!(f.call::<(i32,)>().unwrap(), (1,));
    assert_eq!(f.call::<(i32, i32)>().unwrap(), (1, 2));
    assert_eq!(f.call::<(i32, i32, i32)>().unwrap(), (1, 2, 3));
    assert_eq!(
        f.call::<(i32, i32, i32, i32)>()
            .unwrap_err().to_string(),
        "Wrong type returned by Lua: (i32, i32, i32, i32) expected, got (number, number, number)"
            .to_string()
    );
    assert_eq!(
        f.call::<(i32, i32, i32, Option<i32>)>().unwrap(),
        (1, 2, 3, None)
    );
    assert_eq!(
        f.call::<(i32, i32, i32, Option<i32>, Option<i32>)>().unwrap(),
        (1, 2, 3, None, None)
    );

    assert_eq!(
        f.call::<(bool, String, f64)>()
            .unwrap_err().to_string(),
        "Wrong type returned by Lua: (bool, alloc::string::String, f64) expected, got (number, number, number)"
            .to_string()
    );
}

pub fn execute_from_reader_errors_if_cant_read() {
    struct Reader { }

    impl Read for Reader {
        fn read(&mut self, _: &mut [u8]) -> ::std::io::Result<usize> {
            use std::io::{Error, ErrorKind};
            Err(Error::new(ErrorKind::Other, "oh no!"))
        }
    }

    let lua = crate::hlua::global();
    let reader = Reader { };
    let res: Result<(), _> = lua.execute_from_reader(reader);
    match res {
        Ok(_) => panic!("Reading succeded"),
        Err(LuaError::ReadError(e)) => { assert_eq!("oh no!", e.to_string()) },
        Err(_) => panic!("Unexpected error happened"),
    }
}
