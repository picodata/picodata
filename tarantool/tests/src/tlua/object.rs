use tarantool::tlua::{
    AnyLuaString, Call, Callable, Index, Indexable, IndexableRW, LuaTable, NewIndex, Nil, Object,
};

use crate::common::LuaStackIntegrityGuard;

use std::convert::TryFrom;

pub fn callable_builtin() {
    let lua = tarantool::lua_state();
    let c: Callable<_> = lua.eval("return function(x) return x + 1 end").unwrap();
    assert_eq!(c.call_with(1).ok(), Some(2));
}

pub fn callable_ffi() {
    let lua = tarantool::lua_state();
    let c: Callable<_> = lua
        .eval(
            "
        require 'ffi'.cdef[[ int atoi(const char *); ]]
        return require 'ffi'.C.atoi
    ",
        )
        .unwrap();
    assert_eq!(c.call_with("420").ok(), Some(420));
}

pub fn callable_meta() {
    let lua = tarantool::lua_state();
    let c: Callable<_> = lua
        .eval(
            "
        return setmetatable(
            { a = 1, b = 'hello' },
            { __call = function(self, key) return self[key] end }
        )
    ",
        )
        .unwrap();
    assert_eq!(c.call_with("a").ok(), Some(1));
    assert_eq!(c.call_with("b").ok(), Some("hello".to_string()));
    assert_eq!(c.call().ok(), Some(Nil));
}

pub fn indexable_builtin() {
    let lua = tarantool::lua_state();
    let i: Indexable<_> = lua.eval("return { [1] = 'one', one = 1 }").unwrap();
    assert_eq!(i.get(1), Some("one".to_string()));
    assert_eq!(i.get("one"), Some(1));
    assert_eq!(i.get(2), Some(Nil));
    assert_eq!(i.get(2), None::<i32>);
    assert_eq!(
        i.try_get::<_, i32>(2).unwrap_err().to_string(),
        "failed reading value from Lua table: i32 expected, got nil"
    );
}

#[rustfmt::skip]
pub fn indexable_ffi() {
    let lua = tarantool::lua_state();
    let _stack_integrity_guard = LuaStackIntegrityGuard::new("indexable stack check", &lua);
    {
        let i: Indexable<_> = lua.eval("
            local ffi = require('ffi')
            ffi.cdef([[
                struct bigfoo_t {
                    int nice;
                    int array[3];
                }
            ]])
            return ffi.new(
                'struct bigfoo_t',
                {
                    nice = 69,
                    array = { 1, 2, 3 }
                }
            )
        ").unwrap();
        assert_eq!(i.get("nice"), Some(69));
        let array = i.get::<_, Indexable<_>>("array").unwrap();
        assert_eq!(array.get(0), Some(1));
        assert_eq!(array.get(1), Some(2));
        assert_eq!(array.get(2), Some(3));
        assert_eq!(i.get("no such member"), None::<()>);
        assert_eq!(
            i.try_get::<_, ()>("no such member").unwrap_err().to_string(),
            "execution error: 'struct bigfoo_t' has no member named 'no such member'"
        );
    }
}

#[rustfmt::skip]
pub fn indexable_meta() {
    let lua = tarantool::lua_state();
    let (file, line) = (file!(), line!() + 1); // should point `lua.eval` next line
    let i: Indexable<_> = lua.eval("
        return setmetatable(
            { 1 },
            {
                __index = function(self, key)
                    local res = {}
                    for i = 1,key do
                        res[i] = self[1] + i
                    end
                    return res
                end
            }
        )
    ").unwrap();
    assert_eq!(i.get(1), Some(1));
    assert_eq!(i.get(2), Some([2, 3]));
    assert_eq!(i.get(3), Some([2, 3, 4]));
    assert_eq!(i.get("hello"), None::<()>);
    assert_eq!(
        i.try_get::<_, u8>("hello").unwrap_err().to_string(),
        format!("execution error: [{file}:{line}]:7: 'for' limit must be a number")
    );

    let t = LuaTable::try_from(Object::from(i)).unwrap();
    assert_eq!(
        t.try_get::<_, u8>("hello").unwrap_err().to_string(),
        format!("execution error: [{file}:{line}]:7: 'for' limit must be a number")
    );
}

pub fn cannot_get_mutltiple_values() {
    let lua = tarantool::lua_state();
    let i: Indexable<_> = lua.eval("return { 'one' };").unwrap();
    assert_eq!(i.get::<_, String>(1), Some("one".to_string()));
    assert_eq!(i.get::<_, (String,)>(1), Some(("one".to_string(),)));
    assert_eq!(
        i.try_get::<_, (String, i32)>(1).unwrap_err().to_string(),
        "failed reading Lua value: i32 expected, got no value
    while reading one of multiple values: i32 at index 2 (1-based) expected, got no value
    while reading value from Lua table: (alloc::string::String, i32) expected, got string"
    );
    assert_eq!(
        i.try_get::<_, (i32, i32)>(2).unwrap_err().to_string(),
        "failed reading Lua value: i32 expected, got nil
    while reading one of multiple values: i32 at index 1 (1-based) expected, got incorrect value
    while reading value from Lua table: (i32, i32) expected, got nil"
    );
}

pub fn indexable_rw_builtin() {
    let lua = tarantool::lua_state();
    let i: IndexableRW<_> = lua.eval("return {}").unwrap();
    i.set(1, "foo");
    assert_eq!(i.get(1), Some("foo".to_string()));
    i.set("nice", 69);
    assert_eq!(i.get("nice"), Some(69));
}

#[rustfmt::skip]
pub fn indexable_rw_meta() {
    let lua = tarantool::lua_state();
    let (file, line) = (file!(), line!() + 1); // should point `lua.eval` next line
    let i: IndexableRW<_> = lua.eval("
        return setmetatable({}, { __newindex =
            function(self, k, v)
                rawset(self, k, 'super_' .. v)
            end
        })
    ").unwrap();
    i.set(1, "foo");
    assert_eq!(i.get(1), Some("super_foo".to_string()));
    i.set(2, 69);
    assert_eq!(i.get(2), Some("super_69".to_string()));
    i.set(1, "foo");
    assert_eq!(i.get(1), Some("foo".to_string()));
    assert_eq!(
        i.try_set(3, [1, 2, 3]).unwrap_err().to_string(),
        format!(
            "execution error: [{file}:{line}]:4: \
            attempt to concatenate local 'v' (a table value)"
        )
    );
    assert_eq!(i.get(3), None::<String>);
}

pub fn anything_to_msgpack() {
    let lua = tarantool::lua_state();
    let o: Object<_> = lua.eval("return {69, foo='bar'}").unwrap();
    let mp: AnyLuaString = lua
        .eval_with("return require'msgpack'.encode(...)", &o)
        .unwrap();
    assert_eq!(mp.as_bytes(), b"\x82\x01\x45\xa3foo\xa3bar");
}
