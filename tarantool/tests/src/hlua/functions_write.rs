use tarantool::hlua::{
    Lua,
    LuaError,
    function0,
    function1,
    function2,
};
use std::sync::Arc;

pub fn simple_function() {
    let lua = tarantool::global_lua();

    fn ret5() -> i32 {
        5
    }
    lua.set("ret5", function0(ret5));

    let val: i32 = lua.eval("return ret5()").unwrap();
    assert_eq!(val, 5);
}

pub fn one_argument() {
    let lua = tarantool::global_lua();

    fn plus_one(val: i32) -> i32 {
        val + 1
    }
    lua.set("plus_one", function1(plus_one));

    let val: i32 = lua.eval("return plus_one(3)").unwrap();
    assert_eq!(val, 4);
}

pub fn two_arguments() {
    let lua = tarantool::global_lua();

    fn add(val1: i32, val2: i32) -> i32 {
        val1 + val2
    }
    lua.set("add", function2(add));

    let val: i32 = lua.eval("return add(3, 7)").unwrap();
    assert_eq!(val, 10);
}

pub fn wrong_arguments_types() {
    let lua = tarantool::global_lua();

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
    let lua = tarantool::global_lua();
    lua.openlibs();

    fn always_fails() -> Result<i32, &'static str> {
        Err("oops, problem")
    }
    lua.set("always_fails", function0(always_fails));

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
    let lua = tarantool::global_lua();

    lua.set("add", function2(|a: i32, b: i32| a + b));
    lua.set("sub", function2(|a: i32, b: i32| a - b));

    let val1: i32 = lua.eval("return add(3, 7)").unwrap();
    assert_eq!(val1, 10);

    let val2: i32 = lua.eval("return sub(5, 2)").unwrap();
    assert_eq!(val2, 3);
}

pub fn closures_lifetime() {
    fn t<F>(f: F)
        where F: Fn(i32, i32) -> i32
    {
        let lua = tarantool::global_lua();

        lua.set("add", function2(f));

        let val1: i32 = lua.eval("return add(3, 7)").unwrap();
        assert_eq!(val1, 10);
    }

    t(|a, b| a + b);
}

pub fn closures_extern_access() {
    let mut a = 5;

    {
        let lua = tarantool::global_lua();

        lua.set("inc", function0(|| a += 1));
        for _ in 0..15 {
            lua.exec("inc()").unwrap();
        }
    }

    assert_eq!(a, 20)
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
