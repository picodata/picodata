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
