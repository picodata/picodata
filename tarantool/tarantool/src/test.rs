//! Internals used by custom test runtime to run tests that require tarantool environment
use tester::{ShouldPanic, TestDesc, TestDescAndFn, TestFn, TestName, TestType};

#[derive(Clone)]
pub struct TestCase {
    pub name: &'static str,
    // TODO: Support functions returning `Result`
    pub f: fn(),
}

// Linkme distributed_slice exports a symbol with the given name, so we must
// make sure the name is unique, so as not to conflict with distributed slices
// from other crates.
#[::linkme::distributed_slice]
pub static TARANTOOL_MODULE_TESTS: [TestCase] = [..];

pub fn collect() -> Vec<TestDescAndFn> {
    TARANTOOL_MODULE_TESTS
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

/// The default port where tarantool listens in tests
pub const TARANTOOL_LISTEN: u16 = 3301;
