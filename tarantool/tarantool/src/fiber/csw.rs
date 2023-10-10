//! Context switches tracking utilities.
//!
//! Those are mostly used for testing.

use super::FiberId;

/// Returns the number of context switches of the fiber with given id or of
/// calling fiber if id is `None`. Returns `None` if fiber wasn't found.
pub(crate) fn csw_lua(id: Option<FiberId>) -> Option<u64> {
    static mut FUNCTION_DEFINED: bool = false;
    let lua = crate::lua_state();

    if unsafe { !FUNCTION_DEFINED } {
        #[rustfmt::skip]
        lua.exec(r#"
            local fiber = require('fiber')
            function fiber_csw(id)
                local f
                if id == nil then
                    f = fiber.self()
                    id = f.id()
                else
                    f = fiber.find(id)
                end
                if f == nil then
                    return nil
                end
                if f.csw ~= nil then
                    return f:csw()
                else
                    return fiber.info({bt = false})[id].csw
                end
            end
        "#).unwrap();
        unsafe {
            FUNCTION_DEFINED = true;
        }
    }

    lua.get::<crate::tlua::LuaFunction<_>, _>("fiber_csw")
        .unwrap()
        .into_call_with_args(id)
        .unwrap()
}

/// Calls a function and checks whether it yielded.
///
/// It's mostly useful in tests.
///
/// See also: <https://www.tarantool.io/en/doc/latest/concepts/coop_multitasking/#app-yields>
///
/// # Examle
///
/// ```no_run
/// # use tarantool::fiber;
/// # use tarantool::fiber::check_yield;
/// # use tarantool::fiber::YieldResult::*;
/// # use std::time::Duration;
/// assert_eq!(
///     check_yield(|| fiber::sleep(Duration::ZERO)),
///     Yielded(())
/// );
/// ```
pub fn check_yield<F, T>(f: F) -> YieldResult<T>
where
    F: FnOnce() -> T,
{
    let csw_before = crate::fiber::csw();
    let res = f();
    if crate::fiber::csw() == csw_before {
        YieldResult::DidntYield(res)
    } else {
        YieldResult::Yielded(res)
    }
}

/// Possible [`check_yield`] results.
#[derive(Debug, PartialEq, Eq)]
pub enum YieldResult<T> {
    /// The function didn't yield.
    DidntYield(T),
    /// The function did yield.
    Yielded(T),
}

#[cfg(feature = "internal_test")]
mod tests {
    use super::YieldResult;
    use crate::fiber;
    use std::time::Duration;

    #[crate::test(tarantool = "crate")]
    fn check_yield() {
        assert_eq!(
            super::check_yield(|| ()), //
            YieldResult::DidntYield(())
        );
        assert_eq!(
            super::check_yield(|| fiber::sleep(Duration::ZERO)),
            YieldResult::Yielded(())
        );
    }

    #[crate::test(tarantool = "crate")]
    fn performance() {
        let now = crate::time::Instant::now();
        let _ = crate::fiber::csw();
        let elapsed = now.elapsed();
        print!("{elapsed:?} ");
        assert!(elapsed < Duration::from_millis(1));
    }
}
