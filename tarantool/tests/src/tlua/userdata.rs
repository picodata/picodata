use std::num::NonZeroI32;

use tarantool::tlua;

pub fn readwrite() {
    #[derive(Clone)]
    struct Foo;
    impl<L> tlua::PushInto<L> for Foo
    where
        L: tlua::AsLua,
    {
        type Err = tlua::Void;
        fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (tlua::Void, L)> {
            Ok(tlua::push_userdata(self, lua, |_| {}))
        }
    }
    impl<L> tlua::PushOneInto<L> for Foo where L: tlua::AsLua {}
    impl<L> tlua::LuaRead<L> for Foo
    where
        L: tlua::AsLua,
    {
        fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Foo, L> {
            let val: Result<tlua::UserdataOnStack<Foo, _>, _> =
                tlua::LuaRead::lua_read_at_position(lua, index);
            val.map(|d| d.clone())
        }
    }

    let lua = tlua::Lua::new();

    lua.set("a", Foo);
    let _: Foo = lua.get("a").unwrap();
}

pub fn destructor_called() {
    use std::cell::RefCell;
    use std::rc::Rc;

    let called = Rc::new(RefCell::new(false));

    struct Foo {
        called: Rc<RefCell<bool>>,
    }

    impl Drop for Foo {
        fn drop(&mut self) {
            let mut called = self.called.borrow_mut();
            (*called) = true;
        }
    }

    impl<L> tlua::PushInto<L> for Foo
    where
        L: tlua::AsLua,
    {
        type Err = tlua::Void;
        fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (tlua::Void, L)> {
            Ok(tlua::push_userdata(self, lua, |_| {}))
        }
    }
    impl<L> tlua::PushOneInto<L> for Foo where L: tlua::AsLua {}

    {
        let lua = tlua::Lua::new();
        lua.set("a", Foo { called: called.clone() });
    }

    assert!(*called.borrow());
}

pub fn type_check() {
    #[derive(Clone)]
    struct Foo;
    impl<L> tlua::PushInto<L> for Foo
    where
        L: tlua::AsLua,
    {
        type Err = tlua::Void;
        fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (tlua::Void, L)> {
            Ok(tlua::push_userdata(self, lua, |_| {}))
        }
    }
    impl<L> tlua::PushOneInto<L> for Foo where L: tlua::AsLua {}
    impl<L> tlua::LuaRead<L> for Foo
    where
        L: tlua::AsLua,
    {
        fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Foo, L> {
            let val: Result<tlua::UserdataOnStack<Foo, _>, _> =
                tlua::LuaRead::lua_read_at_position(lua, index);
            val.map(|d| d.clone())
        }
    }

    #[derive(Clone)]
    struct Bar;
    impl<L> tlua::PushInto<L> for Bar
    where
        L: tlua::AsLua,
    {
        type Err = tlua::Void;
        fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (tlua::Void, L)> {
            Ok(tlua::push_userdata(self, lua, |_| {}))
        }
    }
    impl<L> tlua::PushOneInto<L> for Bar where L: tlua::AsLua {}
    impl<L> tlua::LuaRead<L> for Bar
    where
        L: tlua::AsLua,
    {
        fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Bar, L> {
            let val: Result<tlua::UserdataOnStack<Bar, _>, _> =
                tlua::LuaRead::lua_read_at_position(lua, index);
            val.map(|d| d.clone())
        }
    }

    let lua = tlua::Lua::new();

    lua.set("a", Foo);

    let x: Option<Bar> = lua.get("a");
    assert!(x.is_none())
}

pub fn metatables() {
    #[derive(Clone)]
    struct Foo;
    impl<L> tlua::PushInto<L> for Foo
    where
        L: tlua::AsLua,
    {
        type Err = tlua::Void;
        fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (tlua::Void, L)> {
            Ok(tlua::push_userdata(self, lua, |table| {
                table.set(
                    "__index",
                    vec![("test", tlua::function0(|| 5))]
                );
            }))
        }
    }
    impl<L> tlua::PushOneInto<L> for Foo where L: tlua::AsLua {}

    let lua = tlua::Lua::new();

    lua.set("a", Foo);

    let x: i32 = lua.eval("return a.test()").unwrap();
    assert_eq!(x, 5);
}

pub fn multiple_userdata() {
   #[derive(Clone)]
    struct Integer(u32);
    impl<L> tlua::PushInto<L> for Integer
    where
        L: tlua::AsLua,
    {
        type Err = tlua::Void;
        fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (tlua::Void, L)> {
            Ok(tlua::push_userdata(self, lua, |_| { }))
        }
    }
    impl<L> tlua::PushOneInto<L> for Integer where L: tlua::AsLua {}
    impl<L> tlua::LuaRead<L> for Integer
    where
        L: tlua::AsLua,
    {
        fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Integer, L> {
            let val: Result<tlua::UserdataOnStack<Integer, _>, _> =
                tlua::LuaRead::lua_read_at_position(lua, index);
            val.map(|d| d.clone())
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct BigInteger(u32, u32, u32, u32);
    impl<L> tlua::PushInto<L> for BigInteger
    where
        L: tlua::AsLua,
    {
        type Err = tlua::Void;
        fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (tlua::Void, L)> {
            Ok(tlua::push_userdata(self, lua, |_| { }))
        }
    }
    impl<L> tlua::PushOneInto<L> for BigInteger where L: tlua::AsLua {}
    impl<L> tlua::LuaRead<L> for BigInteger
    where
        L: tlua::AsLua,
    {
        fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<BigInteger, L> {
            let val: Result<tlua::UserdataOnStack<BigInteger, _>, _> =
                tlua::LuaRead::lua_read_at_position(lua, index);
            val.map(|d| d.clone())
        }
    }

    let axpy_float = |a: f64, x: Integer, y: Integer| a * x.0 as f64 + y.0 as f64;
    let axpy_float_2 = |a: f64, x: Integer, y: f64| a * x.0 as f64 + y;
    let broadcast_mul = |k: Integer, v: BigInteger|
        BigInteger(k.0 * v.0, k.0 * v.1, k.0 * v.2, k.0 * v.3);
    let collapse = |a: f32, k: Integer, v: BigInteger|
        (k.0 * v.0) as f32 * a + (k.0 * v.1) as f32 * a + (k.0 * v.2) as f32 * a + (k.0 * v.3) as f32 * a;
    let lua = tlua::Lua::new();

    let big_integer = BigInteger(531,246,1,953);
    lua.set("a", Integer(19));
    lua.set("b", Integer(114));
    lua.set("c", Integer(96));
    lua.set("d", Integer(313));
    lua.set("v", big_integer.clone());
    lua.set("add", tlua::function2(|Integer(x), Integer(y)| Integer(x + y)));
    lua.set("axpy", tlua::function3(|a: Integer, x: Integer, y: Integer|
        Integer(a.0 * x.0 + y.0)));
    lua.set("axpy_float", tlua::function3(axpy_float));
    lua.set("axpy_float_2", tlua::function3(axpy_float_2));
    lua.set("broadcast_mul", tlua::function2(broadcast_mul));
    lua.set("collapse", tlua::function3(collapse));

    assert_eq!(lua.eval::<Integer>("return add(a, b)").unwrap().0, 19 + 114);
    assert_eq!(lua.eval::<Integer>("return add(b, c)").unwrap().0, 114 + 96);
    assert_eq!(lua.eval::<Integer>("return add(c, d)").unwrap().0, 96 + 313);
    assert_eq!(lua.eval::<Integer>("return axpy(a, b, c)").unwrap().0, 19 * 114 + 96);
    assert_eq!(lua.eval::<Integer>("return axpy(b, c, d)").unwrap().0, 114 * 96 + 313);
    assert_eq!(lua.eval::<f64>("return axpy_float(2.5, c, d)").unwrap(),
        axpy_float(2.5, Integer(96), Integer(313)));
    assert_eq!(lua.eval::<BigInteger>("return broadcast_mul(a, v)").unwrap(),
        broadcast_mul(Integer(19), big_integer.clone()));
    assert_eq!(lua.eval::<BigInteger>("return broadcast_mul(b, v)").unwrap(),
        broadcast_mul(Integer(114), big_integer.clone()));
    assert_eq!(lua.eval::<f32>("return collapse(19.25, c, v)").unwrap(),
        collapse(19.25, Integer(96), big_integer));
}
