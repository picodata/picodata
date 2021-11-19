use tarantool::hlua::{
    Lua,
    LuaTable,
    PushGuard,
    function0,
};

pub fn iterable() {
    let lua = crate::hlua::global();

    let _: () = lua.execute("a = { 9, 8, 7 }").unwrap();

    let table = lua.get::<LuaTable<_>, _>("a").unwrap();
    let mut counter = 0;

    for (key, value) in table.iter().filter_map(|e| e) {
        let _: u32 = key;
        let _: u32 = value;
        assert_eq!(key + value, 10);
        counter += 1;
    }

    assert_eq!(counter, 3);
}

pub fn iterable_multipletimes() {
    let lua = crate::hlua::global();

    let _: () = lua.execute("a = { 9, 8, 7 }").unwrap();

    let table = lua.get::<LuaTable<_>, _>("a").unwrap();

    for _ in 0..10 {
        let table_content: Vec<Option<(u32, u32)>> = table.iter().collect();
        assert_eq!(table_content,
                vec![Some((1, 9)), Some((2, 8)), Some((3, 7))]);
    }
}

pub fn get_set() {
    let lua = crate::hlua::global();

    let _: () = lua.execute("a = { 9, 8, 7 }").unwrap();
    let table = lua.get::<LuaTable<_>, _>("a").unwrap();

    let x: i32 = table.get(2).unwrap();
    assert_eq!(x, 8);

    table.set(3, "hello");
    let y: String = table.get(3).unwrap();
    assert_eq!(y, "hello");

    let z: i32 = table.get(1).unwrap();
    assert_eq!(z, 9);
}

pub fn table_over_table() {
    let lua = crate::hlua::global();

    lua.execute::<()>("a = { 9, { 8, 7 }, 6 }").unwrap();
    let table = lua.get::<LuaTable<_>, _>("a").unwrap();

    let x: i32 = table.get(1).unwrap();
    assert_eq!(x, 9);

    {
        let subtable = table.get::<LuaTable<_>, _>(2).unwrap();

        let y: i32 = subtable.get(1).unwrap();
        assert_eq!(y, 8);

        let z: i32 = subtable.get(2).unwrap();
        assert_eq!(z, 7);
    }

    let w: i32 = table.get(3).unwrap();
    assert_eq!(w, 6);
}

pub fn metatable() {
    let lua = crate::hlua::global();

    let _: () = lua.execute("a = { 9, 8, 7 }").unwrap();

    {
        let table = lua.get::<LuaTable<_>, _>("a").unwrap();

        let metatable = table.get_or_create_metatable();
        fn handler() -> i32 {
            5
        }
        metatable.set("__add".to_string(), function0(handler));
    }

    let r: i32 = lua.execute("return a + a").unwrap();
    assert_eq!(r, 5);
}

pub fn empty_array() {
    let lua = crate::hlua::global();

    {
        let array = lua.empty_array("a");
        array.set("b", 3)
    }

    let table: LuaTable<_> = lua.get("a").unwrap();
    assert_eq!(3, table.get::<i32, _>("b").unwrap());
}

pub fn by_value() {
    let lua = crate::hlua::global();

    {
        let array = lua.empty_array("a");
        {
            let array2 = array.empty_array("b");
            array2.set("c", 3);
        }
    }

    let table: LuaTable<PushGuard<Lua>> = lua.into_get("a").ok().unwrap();
    let table2: LuaTable<PushGuard<LuaTable<PushGuard<Lua>>>> =
        table.into_get("b").ok().unwrap();
    assert_eq!(3, table2.get::<i32, _>("c").unwrap());
    let table: LuaTable<PushGuard<Lua>> = table2.into_inner().into_inner();
    // do it again to make sure the stack is still sane
    let table2: LuaTable<PushGuard<LuaTable<PushGuard<Lua>>>> =
        table.into_get("b").ok().unwrap();
    assert_eq!(3, table2.get::<i32, _>("c").unwrap());
    let table: LuaTable<PushGuard<Lua>> = table2.into_inner().into_inner();
    let _lua: Lua = table.into_inner().into_inner();
}

pub fn registry() {
    let lua = Lua::new();

    let table = LuaTable::registry(&lua);
    table.set(3, "hello");
    let y: String = table.get(3).unwrap();
    assert_eq!(y, "hello");
}

pub fn registry_metatable() {
    let lua = Lua::new();

    let registry = LuaTable::registry(&lua);
    let metatable = registry.get_or_create_metatable();
    metatable.set(3, "hello");
}
