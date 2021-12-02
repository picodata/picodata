use serde::{Deserialize, Serialize};

use tarantool::tuple::AsTuple;

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
    let lua = crate::hlua::global();

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

    return lua.get::<tarantool::hlua::LuaFunction<_>, _>("fiber_csw")
        .unwrap().call().unwrap();
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

