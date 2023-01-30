//! Internals used by custom test runtime to run tests that require tarantool environment

use linkme::distributed_slice;
use tester::{ShouldPanic, TestDesc, TestDescAndFn, TestFn, TestName, TestType};

/// The recommended way to describe tests in `tlua` crate
///
/// # Example
/// ```
/// use tlua::test::{TLUA_TESTS, TestCase};
/// use tlua::test_name;
/// use linkme::distributed_slice;
///
/// #[distributed_slice(TLUA_TESTS)]
/// static MY_TEST: TestCase = TestCase { name: test_name!("my_test"), f: || { assert!(true) }};
/// ```
#[derive(Clone)]
pub struct TestCase {
    pub name: &'static str,
    pub f: fn(),
}

/// Combines a user defined test name with its module path
#[macro_export]
macro_rules! test_name {
    ($name:literal) => {
        concat!(module_path!(), "::", $name)
    };
}

// It is not possible to name it `TESTS` as in `tarantool` lib
// as there happens a name clash and tests are executed twice.
//
// This seems to be an undocumented side effect of `linkme`.
#[distributed_slice]
pub static TLUA_TESTS: [TestCase] = [..];

pub fn collect() -> Vec<TestDescAndFn> {
    TLUA_TESTS
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
