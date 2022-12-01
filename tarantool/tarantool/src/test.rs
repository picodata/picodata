use linkme::distributed_slice;
use tester::{ShouldPanic, TestDesc, TestDescAndFn, TestFn, TestName, TestType};

/// The recommended way to describe tests in `tarantool` crate
///
/// # Example
/// ```
/// use tarantool::test::{TESTS, TestCase};
/// use tarantool::test_name;
/// use linkme::distributed_slice;
///
/// #[distributed_slice(TESTS)]
/// static MY_TEST: TestCase = TestCase { name: test_name!("my_test"), f: || { assert!(true) }};
/// ```
#[derive(Clone)]
pub struct TestCase {
    pub name: &'static str,
    // TODO: Support functions returning `Result`
    pub f: fn(),
}

/// Combines a user defined test name with its module path
#[macro_export]
macro_rules! test_name {
    ($name:literal) => {
        concat!(module_path!(), "::", $name)
    };
}

#[distributed_slice]
pub static TESTS: [TestCase] = [..];

pub fn collect() -> Vec<TestDescAndFn> {
    TESTS
        .iter()
        .map(|case| TestDescAndFn {
            desc: TestDesc {
                name: TestName::StaticTestName(case.name),
                ignore: false,
                should_panic: ShouldPanic::No,
                allow_fail: false,
                test_type: TestType::IntegrationTest,
            },
            testfn: TestFn::StaticTestFn(case.f),
        })
        .collect()
}

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
