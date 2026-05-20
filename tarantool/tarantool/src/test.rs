//! Internals used by custom test runtime to run tests that require tarantool environment
use tester::{ShouldPanic, TestDesc, TestDescAndFn, TestFn, TestName, TestType};

/// A struct representing a test case definide using the `#[`[`tarantool::test`]`]`
/// macro attribute. Can be used to implement a custom testing harness.
///
/// See also [`collect_tester`].
///
/// [`tarantool::test`]: macro@crate::test
#[derive(Clone, Debug)]
pub struct TestCase {
    name: &'static str,
    // TODO: Support functions returning `Result`
    f: fn(),
    should_panic: bool,
    skip: Option<&'static str>,
}

impl PartialEq for TestCase {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.should_panic == other.should_panic
            && self.skip == other.skip
    }
}

impl TestCase {
    /// Creates a new test case.
    ///
    /// This function is called when `#[`[`tarantool::test`]`]` attribute is
    /// used, so users don't usually use it directly.
    ///
    /// [`tarantool::test`]: macro@crate::test
    pub const fn new(
        name: &'static str,
        f: fn(),
        should_panic: bool,
        skip: Option<&'static str>,
    ) -> Self {
        Self {
            name,
            f,
            should_panic,
            skip,
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

    /// Check if the test case should be skipped and return the reason.
    pub fn skip(&self) -> Option<&'static str> {
        self.skip
    }

    /// Convert the test case into a struct that can be used with the [`tester`]
    /// crate.
    pub const fn to_tester(&self) -> TestDescAndFn {
        TestDescAndFn {
            desc: TestDesc {
                name: TestName::StaticTestName(self.name),
                ignore: self.skip.is_some(),
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
    use crate::network::client::tls;
    use std::convert::Infallible;
    use std::path::PathBuf;
    use tlua::AsLua;
    use tlua::LuaState;

    /// Returns the binary protocol port of the current tarantool instance.
    /// It is the first port in the config.
    pub fn listen_port() -> u16 {
        let lua = crate::lua_state();
        let listen: String = lua
            .eval("return (box.info.listen[1] or box.info.listen)")
            .unwrap();
        let (_address, port) = listen.rsplit_once(':').unwrap();
        port.parse().unwrap()
    }

    /// Returns the TLS binary protocol port of the current tarantool instance.
    /// It is the second port in the config.
    /// Makes sense only with "picodata" feature.
    pub fn tls_listen_port() -> u16 {
        let lua = crate::lua_state();
        let listen: String = lua.eval("return box.info.listen[2]").unwrap();
        let (_address, port) = listen.rsplit_once(':').unwrap();
        port.parse().unwrap()
    }

    /// Returns TLS connector.
    pub fn get_tls_connector() -> tls::TlsConnector {
        let cargo_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let path = cargo_path.parent().unwrap().join("tests/ssl_certs");
        let cert_file = path.join("server.crt");
        let key_file = path.join("server.key");
        let ca_file = path.join("combined-ca.crt");
        let tls_config = tls::TlsConfig {
            cert_file: &cert_file,
            key_file: &key_file,
            ca_file: Some(&ca_file),
        };
        tls::TlsConnector::new(tls_config).unwrap()
    }

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

    ////////////////////////////////////////////////////////////////////////////////
    // LuaStackIntegrityGuard
    ////////////////////////////////////////////////////////////////////////////////

    pub struct LuaStackIntegrityGuard {
        name: &'static str,
        lua: LuaState,
    }

    impl LuaStackIntegrityGuard {
        pub fn global(name: &'static str) -> Self {
            Self::new(name, crate::global_lua())
        }

        pub fn new(name: &'static str, lua: impl AsLua) -> Self {
            let lua = lua.as_lua();
            unsafe { lua.push_one(name).forget() };
            Self { name, lua }
        }
    }

    impl Drop for LuaStackIntegrityGuard {
        #[track_caller]
        fn drop(&mut self) {
            let single_value = unsafe { tlua::PushGuard::new(self.lua, 1) };
            let msg: tlua::StringInLua<_> = crate::unwrap_ok_or!(single_value.read(),
                Err((l, e)) => {
                    eprintln!(
                        "Lua stack integrity violation:
    Error: {e}
    Expected string: \"{}\"
    Stack dump:",
                        self.name,
                    );
                    let mut buf = Vec::with_capacity(64);
                    unsafe { tlua::debug::dump_stack_raw_to(l.as_lua(), &mut buf).unwrap() };
                    for line in String::from_utf8_lossy(&buf).lines() {
                        eprintln!("        {line}");
                    }
                    panic!("Lua stack integrity violation: See error message above");
                }
            );
            assert_eq!(msg, self.name);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////
    // ScopeGuard
    ////////////////////////////////////////////////////////////////////////////////

    #[derive(Debug)]
    #[must_use = "The callback is invoked when the `ScopeGuard` is dropped"]
    pub struct ScopeGuard<F>
    where
        F: FnOnce(),
    {
        cb: Option<F>,
    }

    impl<F> Drop for ScopeGuard<F>
    where
        F: FnOnce(),
    {
        fn drop(&mut self) {
            if let Some(cb) = self.cb.take() {
                cb()
            }
        }
    }

    pub fn on_scope_exit<F>(cb: F) -> ScopeGuard<F>
    where
        F: FnOnce(),
    {
        ScopeGuard { cb: Some(cb) }
    }

    /// Defines the native tarantool stored procedure with name given in `proc_name`.
    /// `proc_pointer` is only used to get the module name using [`crate::proc::module_path`].
    pub fn define_stored_proc(
        proc_pointer: crate::ffi::tarantool::Proc,
        proc_name: &'static str,
    ) -> String {
        let path = crate::proc::module_path(proc_pointer as _).unwrap();
        let module = path.file_stem().unwrap();
        let module = module.to_str().unwrap();
        let proc = format!("{module}.{proc_name}");

        let lua = crate::lua_state();
        lua.exec_with("box.schema.func.create(..., { language = 'C' })", &proc)
            .unwrap();

        proc
    }

    #[macro_export]
    macro_rules! define_stored_proc_for_tests {
        (@stringify_last_token $tail:tt) => { ::std::stringify!($tail) };
        (@stringify_last_token $head:tt $($tail:tt)+) => { define_stored_proc_for_tests!(@stringify_last_token $($tail)+) };

        ( $($proc:tt)+ ) => {{
            $crate::test::util::define_stored_proc($($proc)+, $crate::define_stored_proc_for_tests!(@stringify_last_token $($proc)+))
        }};
    }

    pub use crate::define_stored_proc_for_tests as define_stored_proc;
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
