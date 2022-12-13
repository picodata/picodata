//! General testing utils, can be reused in dependent crates

pub fn fiber_csw() -> i32 {
    static mut FUNCTION_DEFINED: bool = false;
    let lua = crate::lua_state();

    if unsafe { !FUNCTION_DEFINED } {
        #[rustfmt::skip]
        lua.exec(r#"
            function fiber_csw()
                local fiber = require('fiber')
                return fiber.info()[fiber.id()].csw
            end
        "#).unwrap();
        unsafe {
            FUNCTION_DEFINED = true;
        }
    }

    lua.get::<crate::tlua::LuaFunction<_>, _>("fiber_csw")
        .unwrap()
        .into_call()
        .unwrap()
}

pub fn check_yield<F, T>(f: F) -> YieldResult<T>
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
pub enum YieldResult<T> {
    Yields(T),
    DoesntYield(T),
}
