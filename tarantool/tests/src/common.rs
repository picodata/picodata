use serde::{Deserialize, Serialize};

use tarantool::{
    tlua::{self, AsLua},
    tuple::AsTuple,
};

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S1Record {
    pub id: u32,
    pub text: String,
}

impl AsTuple for S1Record {}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S2Record {
    pub id: u32,
    pub key: String,
    pub value: String,
    pub a: i32,
    pub b: i32,
}

impl AsTuple for S2Record {}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S2Key {
    pub id: u32,
    pub a: i32,
    pub b: i32,
}

impl AsTuple for S2Key {}

#[derive(Serialize)]
pub struct QueryOperation {
    pub op: String,
    pub field_id: u32,
    pub value: serde_json::Value,
}

impl AsTuple for QueryOperation {}

#[derive(Clone, Debug)]
pub(crate) struct DropCounter(pub(crate) std::rc::Rc<std::cell::Cell<usize>>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        let old_count = self.0.get();
        self.0.set(old_count + 1);
    }
}

pub(crate) fn capture_value<T>(_: &T) {}

pub(crate) fn fiber_csw() -> i32 {
    static mut FUNCTION_DEFINED: bool = false;
    let lua = global_lua();

    if unsafe { !FUNCTION_DEFINED } {
        #[rustfmt::skip]
        lua.exec(r#"
            function fiber_csw()
                local fiber = require('fiber')
                return fiber.info()[fiber.id()].csw
            end
        "#).unwrap();
        unsafe { FUNCTION_DEFINED = true; }
    }

    lua.get::<tarantool::tlua::LuaFunction<_>, _>("fiber_csw")
        .unwrap().into_call().unwrap()
}

pub(crate) fn check_yield<F, T>(f: F) -> YieldResult<T>
where
    F: FnOnce() -> T,
{
    let csw_before = fiber_csw();
    let res = f();
    if fiber_csw() == csw_before {
        YieldResult::DoesntYield(res)
    } else {
        YieldResult::Yields(res)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum YieldResult<T> {
    Yields(T),
    DoesntYield(T),
}

pub(crate) struct LuaStackIntegrityGuard {
    name: &'static str,
}

impl LuaStackIntegrityGuard {
    pub fn new(name: &'static str) -> Self {
        let lua = global_lua();
        unsafe { lua.push_one(name).forget() };
        Self { name }
    }
}

impl Drop for LuaStackIntegrityGuard {
    fn drop(&mut self) {
        let lua = global_lua();
        let single_value = unsafe { tlua::PushGuard::new(lua, 1) };
        let msg: tlua::StringInLua<_> = single_value.read()
            .expect("Lua stack integrity violation");
        assert_eq!(msg, self.name);
    }
}

fn global_lua() -> tlua::Lua {
    unsafe {
        tlua::Lua::from_existing_state(
            tarantool::ffi::tarantool::luaT_state(), false)
    }
}

pub trait BoolExt {
    fn so(&self) -> bool;

    #[inline(always)]
    fn as_some<T>(&self, v: T) -> Option<T> {
        if self.so() { Some(v) } else { None }
    }

    #[inline(always)]
    fn as_some_from<T>(&self, f: impl FnOnce() -> T) -> Option<T> {
        if self.so() { Some(f()) } else { None }
    }
}

impl BoolExt for bool {
    #[inline(always)]
    fn so(&self) -> bool {
        *self
    }
}

