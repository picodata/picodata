//! Internals used by custom test runtime to run tests that require tarantool environment
use tester::{ShouldPanic, TestDesc, TestDescAndFn, TestFn, TestName, TestType};

/// A struct representing a test case definide using the `#[`[`tarantool::test`]`]`
/// macro attribute. Can be used to implement a custom testing harness.
///
/// See also [`collect_tester`].
///
/// [`tarantool::test`]: macro@crate::test
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TestCase {
    name: &'static str,
    // TODO: Support functions returning `Result`
    f: fn(),
    should_panic: bool,
}

impl TestCase {
    /// Creates a new test case.
    ///
    /// This function is called when `#[`[`tarantool::test`]`]` attribute is
    /// used, so users don't usually use it directly.
    ///
    /// [`tarantool::test`]: macro@crate::test
    pub const fn new(name: &'static str, f: fn(), should_panic: bool) -> Self {
        Self {
            name,
            f,
            should_panic,
        }
    }

    /// Get test case name. This is usually a full path to the test function.
    pub const fn name(&self) -> &str {
        self.name
    }

    /// Run the test case.
    ///
    /// # Panicking
    /// This function may or may not panic depending on if test fails or not.
    pub fn run(&self) {
        (self.f)()
    }

    /// Check if the test case should panic.
    pub fn should_panic(&self) -> bool {
        self.should_panic
    }

    /// Convert the test case into a struct that can be used with the [`tester`]
    /// crate.
    pub const fn to_tester(&self) -> TestDescAndFn {
        TestDescAndFn {
            desc: TestDesc {
                name: TestName::StaticTestName(self.name),
                ignore: false,
                should_panic: if self.should_panic {
                    ShouldPanic::Yes
                } else {
                    ShouldPanic::No
                },
                allow_fail: false,
                test_type: TestType::IntegrationTest,
            },
            testfn: TestFn::StaticTestFn(self.f),
        }
    }
}

impl From<&TestCase> for TestDescAndFn {
    #[inline(always)]
    fn from(tc: &TestCase) -> Self {
        tc.to_tester()
    }
}

impl From<TestCase> for TestDescAndFn {
    #[inline(always)]
    fn from(tc: TestCase) -> Self {
        tc.to_tester()
    }
}

// Linkme distributed_slice exports a symbol with the given name, so we must
// make sure the name is unique, so as not to conflict with distributed slices
// from other crates.
#[::linkme::distributed_slice]
pub static TARANTOOL_MODULE_TESTS: [TestCase] = [..];

/// Returns a static slice of test cases defined with `#[`[`tarantool::test`]`]`
/// macro attribute. Can be used to implement a custom testing harness.
///
/// See also [`collect_tester`].
///
/// [`tarantool::test`]: macro@crate::test
pub fn test_cases() -> &'static [TestCase] {
    &TARANTOOL_MODULE_TESTS
}

/// Returns a vec test description structs which can be used with
/// [`tester::run_tests_console`] function.
pub fn collect_tester() -> Vec<TestDescAndFn> {
    TARANTOOL_MODULE_TESTS.iter().map(Into::into).collect()
}

#[cfg(feature = "internal_test")]
pub mod util {
    use std::convert::Infallible;

    /// The default port where tarantool listens in tests
    pub const TARANTOOL_LISTEN: u16 = 3301;

    /// Returns a future, which is never resolved
    pub async fn always_pending() -> Result<Infallible, Infallible> {
        loop {
            futures::pending!()
        }
    }

    /// Wraps the provided value in a `Ok` of an `Infallible` `Result`.
    pub fn ok<T>(v: T) -> std::result::Result<T, Infallible> {
        Ok(v)
    }
}

#[macro_export]
macro_rules! temp_space_name {
    () => {
        ::std::format!(
            "temp_space@{}:{}:{}",
            ::std::file!(),
            ::std::line!(),
            ::std::column!()
        )
    };
}

#[cfg(feature = "internal_test")]
mod tests {
    const NAMING_CONFLICT: () = ();

    #[crate::test(tarantool = "crate")]
    fn naming_conflict() {
        // Before this commit this test couldn't even compile
        let () = NAMING_CONFLICT;
    }
}
