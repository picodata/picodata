use tarantool::tlua::{
    self,
    function,
    AsLua,
    Lua,
    LuaError,
    LuaFunction,
    Function,
    function0,
    function1,
    function2,
};
use std::sync::Arc;

pub fn simple_function() {
    let lua = Lua::new();

    fn ret5() -> i32 {
        5
    }
    let f: function![() -> i32] = function0(ret5);
    lua.set("ret5", f);

    let val: i32 = lua.eval("return ret5()").unwrap();
    assert_eq!(val, 5);
}

pub fn one_argument() {
    let lua = Lua::new();

    fn plus_one(val: i32) -> i32 {
        val + 1
    }
    let f: function![(i32) -> i32] = function1(plus_one);
    lua.set("plus_one", f);

    let val: i32 = lua.eval("return plus_one(3)").unwrap();
    assert_eq!(val, 4);
}

pub fn two_arguments() {
    let lua = Lua::new();

    fn add(val1: i32, val2: i32) -> i32 {
        val1 + val2
    }
    let f: function![(i32, i32) -> i32] = function2(add);
    lua.set("add", f);

    let val: i32 = lua.eval("return add(3, 7)").unwrap();
    assert_eq!(val, 10);
}

pub fn wrong_arguments_types() {
    let lua = Lua::new();

    fn add(val1: i32, val2: i32) -> i32 {
        val1 + val2
    }
    lua.set("add", function2(add));

    match lua.eval::<i32>("return add(3, \"hello\")") {
        Err(LuaError::ExecutionError(_)) => (),
        _ => panic!(),
    }
}

pub fn return_result() {
    let lua = Lua::new();
    lua.openlibs();

    fn always_fails() -> Result<i32, &'static str> {
        Err("oops, problem")
    }
    let f: function![() -> Result<i32, &'static str>] = function0(always_fails);
    lua.set("always_fails", &f);

    match lua.exec(r#"
        local res, err = always_fails();
        assert(res == nil);
        assert(err == "oops, problem");
    "#) {
        Ok(()) => {}
        Err(e) => panic!("{:?}", e),
    }
}

pub fn closures() {
    let lua = Lua::new();

    lua.set("add", function2(|a: i32, b: i32| a + b));
    lua.set("sub", function2(|a: i32, b: i32| a - b));

    let val1: i32 = lua.eval("return add(3, 7)").unwrap();
    assert_eq!(val1, 10);

    let val2: i32 = lua.eval("return sub(5, 2)").unwrap();
    assert_eq!(val2, 3);
}

pub fn closures_lifetime() {
    fn t<F: 'static>(f: F)
        where F: Fn(i32, i32) -> i32
    {
        let lua = Lua::new();

        lua.set("add", function2(f));

        let val1: i32 = lua.eval("return add(3, 7)").unwrap();
        assert_eq!(val1, 10);
    }

    t(|a, b| a + b);
}

pub fn closures_extern_access() {
    let a = std::rc::Rc::new(std::cell::Cell::new(5));

    {
        let lua = Lua::new();

        let a = a.clone();
        lua.set("inc", function0(move || {
            let old = a.get();
            a.set(old + 1);
        }));
        for _ in 0..15 {
            lua.exec("inc()").unwrap();
        }
    }

    assert_eq!(a.get(), 20)
}

pub fn closures_drop_env() {
    static mut DID_DESTRUCTOR_RUN: bool = false;

    #[derive(Debug)]
    struct Foo { }
    impl Drop for Foo {
        fn drop(&mut self) {
            unsafe {
                DID_DESTRUCTOR_RUN = true;
            }
        }
    }
    {
        let foo = Arc::new(Foo { });

        {
            let lua = Lua::new();

            lua.set("print_foo", function0(move || println!("{:?}", foo)));
        }
    }
    assert_eq!(unsafe { DID_DESTRUCTOR_RUN }, true);
}

static mut GLOBAL_DATA: i32 = 0;

pub fn global_data() {
    let lua = Lua::new();
    let f: function![()] = Function::new(access_global_state);
    let f: LuaFunction<_> = lua.push(f).read().unwrap();
    let () = f.call().unwrap();
    assert_eq!(unsafe { GLOBAL_DATA }, 1);
    let () = f.call().unwrap();
    assert_eq!(unsafe { GLOBAL_DATA }, 2);

    fn access_global_state() {
        unsafe {
            GLOBAL_DATA += 1
        }
    }
}

pub fn push_callback_by_ref() {
    let lua = Lua::new();

    let f: LuaFunction<_> = lua.push(&function1(|x: i32| x + 1)).read().unwrap();
    assert_eq!(f.call_with_args(2_i32).ok(), Some(3_i32));
    let lua = f.into_inner();

    let data = vec![1, 2, 3];

    // Doesn't compile, because the closure isn't 'static and can capture a
    // dangling reference
    // let f: LuaFunction<_> = lua.push(&function0(|| data[0] + data[1] + data[2])).read().unwrap();

    // Doesn't compile, because the closure isn't Copy and cannot be moved from
    // a reference
    // let f: LuaFunction<_> = lua.push(&function0(move || data[0] + data[1] + data[2])).read().unwrap();
    let f: LuaFunction<_> = lua.push(function0(move || data[0] + data[1] + data[2])).read().unwrap();
    assert_eq!(f.call().ok(), Some(6_i32));
    let lua = f.into_inner();

    #[derive(tlua::Push)]
    struct S {
        callback: function![() -> i32],
    }

    let s = S { callback: Function::new(|| 42) };

    let t: tlua::LuaTable<_> = lua.push(&s).read().unwrap();
    assert_eq!(t.call_method("callback", ()).ok(), Some(42_i32));
}

pub fn closures_must_be_static() {
    let lua = Lua::new();

    static mut GLOBAL: Option<Vec<i32>> = None;
    {
        let v = vec![1, 2, 3];
        let f = move || unsafe { GLOBAL = Some(v.clone()) };
        // lua.set("a", Function::new(&f)); <- doesn't compile because otherwise
        // this test fails
        lua.set("a", Function::new(f));
    }
    let f: LuaFunction<_> = lua.get("a").unwrap();
    let () = f.call().unwrap();
    assert_eq!(unsafe { &GLOBAL }, &Some(vec![1, 2, 3]));
}

