use std::collections::HashMap;
use std::io::Read;
use tarantool::tlua::{
    self, AsLua, False, Lua, LuaError, LuaFunction, LuaTable, MethodCallError, True,
};

pub fn basic() {
    let lua = Lua::new();
    let f = LuaFunction::load(&lua, "return 5;").unwrap();
    let val: i32 = f.call().unwrap();
    assert_eq!(val, 5);
}

pub fn two_functions_at_the_same_time() {
    let lua = Lua::new();
    let f1 = LuaFunction::load(&lua, "return 69;").unwrap();
    let f2 = LuaFunction::load(&lua, "return 420;").unwrap();
    assert_eq!(f1.call::<i32>().unwrap(), 69);
    assert_eq!(f2.call::<i32>().unwrap(), 420);
}

pub fn args() {
    let lua = Lua::new();
    lua.exec("function foo(a) return a * 5 end").unwrap();
    let val: i32 = lua
        .get::<LuaFunction<_>, _>("foo")
        .unwrap()
        .call_with_args(3)
        .unwrap();
    assert_eq!(val, 15);
}

pub fn args_in_order() {
    let lua = Lua::new();
    lua.exec("function foo(a, b) return a - b end").unwrap();
    let val: i32 = lua
        .get::<LuaFunction<_>, _>("foo")
        .unwrap()
        .call_with_args((5, 3))
        .unwrap();
    assert_eq!(val, 2);
}

pub fn syntax_error() {
    let lua = Lua::new();
    match LuaFunction::load(&lua, "azerazer") {
        Err(LuaError::SyntaxError(_)) => (),
        _ => panic!(),
    };
}

pub fn execution_error() {
    let lua = Lua::new();
    let f = LuaFunction::load(&lua, "return a:hello()").unwrap();
    match f.call::<()>() {
        Err(LuaError::ExecutionError(_)) => (),
        _ => panic!(),
    };
}

pub fn check_types() {
    let lua = Lua::new();
    let f = LuaFunction::load(&lua, "return 12").unwrap();
    let err = f.call::<bool>().unwrap_err();
    match &err {
        LuaError::WrongType {
            rust_expected,
            lua_actual,
            when,
        } => {
            assert_eq!(rust_expected, "bool");
            assert_eq!(lua_actual, "number");
            assert_eq!(*when, "Wrong type returned by Lua")
        }
        v => panic!("{}", v),
    };
    assert_eq!(
        err.to_string(),
        "Wrong type returned by Lua: bool expected, got number"
    );

    assert_eq!(f.call::<i32>().unwrap(), 12i32);
    assert_eq!(f.call::<f32>().unwrap(), 12f32);
    assert_eq!(f.call::<f64>().unwrap(), 12f64);
    assert_eq!(
        f.call::<String>().unwrap_err().to_string(),
        "Wrong type returned by Lua: alloc::string::String expected, got number"
    );
}

pub fn call_and_read_table() {
    let lua = Lua::new();
    let f = LuaFunction::load(&lua, "return {1, 2, 3};").unwrap();
    let val: LuaTable<_> = f.call().unwrap();
    assert_eq!(val.get::<u8, _>(2).unwrap(), 2);
}

pub fn table_as_args() {
    let lua = Lua::new();
    let f: LuaFunction<_> = lua.eval("return function(a) return a.foo end").unwrap();
    let t: LuaTable<_> = (&lua).push(&Foo { foo: 69 }).read().unwrap();
    let val: i32 = f.call_with_args(&t).unwrap();
    assert_eq!(val, 69);

    let f: LuaFunction<_> = lua
        .eval("return function(a, b) return a.foo + b.bar end")
        .unwrap();
    let u: LuaTable<_> = (&lua).push(&Bar { bar: 420 }).read().unwrap();
    let val: i32 = f.call_with_args((&t, &u)).unwrap();
    assert_eq!(val, 420 + 69);

    let lua = tarantool::lua_state();
    let json_encode: LuaFunction<_> = lua.eval("return require('json').encode").unwrap();
    let res: String = json_encode.call_with_args(vec!["a", "b", "c"]).unwrap();
    assert_eq!(res, r#"["a","b","c"]"#);

    let mut t = HashMap::new();
    t.insert("foo", "bar");
    let res: String = json_encode.call_with_args(t).unwrap();
    assert_eq!(res, r#"{"foo":"bar"}"#);

    #[derive(tlua::Push)]
    struct Foo {
        foo: i32,
    }

    #[derive(tlua::Push)]
    struct Bar {
        bar: i32,
    }
}

#[rustfmt::skip]
pub fn table_method_call() {
    let lua = Lua::new();
    let t: LuaTable<_> = lua.eval("
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
    let lua = Lua::new();
    lua.exec("function foo() return 5 end").unwrap();
    let bar = LuaFunction::load(&lua, "return foo;").unwrap();
    let foo: LuaFunction<_> = bar.call().unwrap();
    let val: i32 = foo.call().unwrap();
    assert_eq!(val, 5);
}

pub fn error_location() {
    let lua = tarantool::lua_state();
    eprintln!();

    let file = file!();

    // Trivial exec
    let code = "error('exec failed')";
    let (line, res) = (line!(), lua.exec(code).unwrap_err());
    assert_eq!(
        res.to_string(),
        format!("Execution error: [{file}:{line}]:1: exec failed")
    );
    eprintln!("  {res}");

    // Trivial eval
    let code = "error('eval failed')";
    let (line, res) = (line!(), lua.eval::<()>(code).unwrap_err());
    assert_eq!(
        res.to_string(),
        format!("Execution error: [{file}:{line}]:1: eval failed")
    );
    eprintln!("  {res}");

    // Trivial exec_with
    let (code, arg) = ("error(...)", "exec_with failed");
    let (line, res) = (line!(), lua.exec_with(code, arg).unwrap_err());
    assert_eq!(
        res.to_string(),
        format!("Lua error: Execution error: [{file}:{line}]:1: exec_with failed")
    );
    eprintln!("  {res}");

    // Trivial eval_with
    let (code, arg) = ("error(...)", "eval_with failed");
    let (line, res) = (line!(), lua.eval_with::<_, ()>(code, arg).unwrap_err());
    assert_eq!(
        res.to_string(),
        format!("Lua error: Execution error: [{file}:{line}]:1: eval_with failed")
    );
    eprintln!("  {res}");

    // We skip testing `exec_from` and `eval_from` as they're already
    // called inside `exec` and `eval` implicitly.

    // One stack frame above
    let res = lua.exec("error('error(2)', 2)").unwrap_err();
    assert_eq!(res.to_string(), "Execution error: error(2)");
    eprintln!("  {res}");

    // One plus one minus
    let code = "
        function foo()
            error('oops', 2)
        end

        foo() -- < location points here
    ";
    let (line, res) = (line!(), lua.exec(code).unwrap_err());
    assert_eq!(
        res.to_string(),
        format!("Execution error: [{}:{}]:6: oops", file!(), line)
    );
    eprintln!("  {res}");

    // Exec doesn't raise, but the call does
    let code = "
        function foo()
            error('oops'); -- < location points here
        end
    ";
    let (line, _) = (line!(), lua.exec(code).unwrap());
    let foo: LuaFunction<_> = lua.get("foo").unwrap();
    let res = foo.call::<()>().unwrap_err();

    assert_eq!(
        res.to_string(),
        format!("Execution error: [{file}:{line}]:3: oops")
    );
    eprintln!("  {res}");
}

pub fn either_or() {
    let lua = Lua::new();
    let foo: LuaFunction<_> = lua
        .eval(
            r#"
        return function(a)
            if a > 0 then
                return true, 69, 420
            else
                return false, "hello"
            end
        end
    "#,
        )
        .unwrap();
    type Res = Result<(True, i32, i32), (False, String)>;
    let res: Res = foo.call_with_args(1).unwrap();
    assert_eq!(res, Ok((True, 69, 420)));
    let res: Res = foo.call_with_args(0).unwrap();
    assert_eq!(res, Err((False, "hello".to_string())));
}

pub fn multiple_return_values() {
    let lua = Lua::new();
    let f = LuaFunction::load(&lua, r#"return 69, "foo", 3.14, true;"#).unwrap();
    let res: (i32, String, f64, bool) = f.call().unwrap();
    assert_eq!(res, (69, "foo".to_string(), 3.14, true));
    let e = f.call::<(i8, i8, i8, i8)>().unwrap_err();
    assert_eq!(
        e.to_string(),
        "Wrong type returned by Lua: (i8, i8, i8, i8) expected, got (number, string, number, boolean)",
    );
}

pub fn multiple_return_values_fail() {
    let lua = Lua::new();
    let f = LuaFunction::load(&lua, "return 1, 2, 3;").unwrap();
    assert_eq!(f.call::<i32>().unwrap(), 1);
    assert_eq!(f.call::<(i32,)>().unwrap(), (1,));
    assert_eq!(f.call::<(i32, i32)>().unwrap(), (1, 2));
    assert_eq!(f.call::<(i32, i32, i32)>().unwrap(), (1, 2, 3));
    assert_eq!(
        f.call::<(i32, i32, i32, i32)>().unwrap_err().to_string(),
        "Wrong type returned by Lua: (i32, i32, i32, i32) expected, got (number, number, number)"
            .to_string()
    );
    assert_eq!(
        f.call::<(i32, i32, i32, Option<i32>)>().unwrap(),
        (1, 2, 3, None)
    );
    assert_eq!(
        f.call::<(i32, i32, i32, Option<i32>, Option<i32>)>()
            .unwrap(),
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
    struct Reader {}

    impl Read for Reader {
        fn read(&mut self, _: &mut [u8]) -> ::std::io::Result<usize> {
            use std::io::{Error, ErrorKind};
            Err(Error::new(ErrorKind::Other, "oh no!"))
        }
    }

    let lua = Lua::new();
    let reader = Reader {};
    match lua.exec_from(reader) {
        Ok(_) => panic!("Reading succeded"),
        Err(LuaError::ReadError(e)) => {
            assert_eq!("oh no!", e.to_string())
        }
        Err(_) => panic!("Unexpected error happened"),
    }
}

pub fn from_function_call_error() {
    fn inner() -> Result<u32, LuaError> {
        let lua = Lua::new();
        let f: LuaFunction<_> = lua.eval("return function(x, y) return x + y end").unwrap();
        let res = f.call_with_args((1, 2))?;
        Ok(res)
    }

    assert_eq!(inner().unwrap(), 3);
}

pub fn non_string_error() {
    let lua = tarantool::lua_state();

    match lua.exec("error()").unwrap_err() {
        LuaError::ExecutionError(msg) => assert_eq!(msg, "nil"),
        _ => unreachable!(),
    }

    match lua
        .exec("error(box.error.new(box.error.UNKNOWN))")
        .unwrap_err()
    {
        LuaError::ExecutionError(msg) => assert_eq!(msg, "Unknown error"),
        _ => unreachable!(),
    }

    match lua
        .exec("error(box.error.new(box.error.SYSTEM, 'oops'))")
        .unwrap_err()
    {
        LuaError::ExecutionError(msg) => assert_eq!(msg, "oops"),
        _ => unreachable!(),
    }
}

pub fn push_function() {
    let lua = Lua::new();
    let call: LuaFunction<_> = lua.eval("return function(f, x) return f(x) end").unwrap();
    let add_one: LuaFunction<_> = lua.eval("return function(x) return x + 1 end").unwrap();
    let res: i32 = call.call_with_args((&add_one, 1)).unwrap();
    assert_eq!(res, 2);

    let lua = tarantool::lua_state();
    let type_f: LuaFunction<_> = lua.get("type").unwrap();
    let lua = (&type_f)
        .push_iter(std::iter::once(("get_type", &type_f)))
        .unwrap();
    let t: LuaTable<_> = (&lua).read().unwrap();
    let res: String = t.call_method("get_type", ()).unwrap();
    assert_eq!(res, "table");
}

#[rustfmt::skip]
pub fn push_iter_no_err() {
    let lua = tarantool::lua_state();
    let (file, line) = (file!(), line!() + 1); // should point `lua.eval` next line
    let call: LuaFunction<_> = lua.eval("return
        function(a, b, c)
            if b[1] == 1 then
                error('oopsie')
            end
        end
    ").unwrap();
    let f = || -> Result<(), tlua::LuaError> {
        Ok(call.call_with_args((69, [1, 2, 3], "foo"))?)
    };
    assert_eq!(
        f().unwrap_err().to_string(),
        format!("Execution error: [{file}:{line}]:4: oopsie")
    );
}

pub fn eval_with() {
    let lua = Lua::new();
    let res: i32 = lua
        .eval_with("a, b = ... ; return a + b", (33, 36))
        .unwrap();
    assert_eq!(res, 69);
    lua.exec_with("n = ...; function eval_with_helper(x) return x + n end", 20)
        .unwrap();
    let f: LuaFunction<_> = lua.get("eval_with_helper").unwrap();
    let res: i32 = f.call_with_args(400).unwrap();
    assert_eq!(res, 420);
}
