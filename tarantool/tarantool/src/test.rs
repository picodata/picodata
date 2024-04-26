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
    use tlua::AsLua;
    use tlua::LuaState;

    /// Returns the binary protocol port of the current tarantool instance.
    pub fn listen_port() -> u16 {
        let lua = crate::lua_state();
        let listen: String = lua.eval("return box.info.listen").unwrap();
        let (_address, port) = listen.rsplit_once(':').unwrap();
        port.parse().unwrap()
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

    ////////////////////////////////////////////////////////////////////////////////
    // setup_ldap_auth
    ////////////////////////////////////////////////////////////////////////////////

    /// Starts the `glauth` ldap server and configures tarantool to use the 'ldap'
    /// authentication method. Returns a `guard` object, it should be dropped
    /// at the end of the test which will stop the server and reset the
    /// configuration to the default authentication method.
    ///
    /// If `glauth` is not found, returns an error message. You can download it
    /// from <https://github.com/glauth/glauth/releases>.
    pub fn setup_ldap_auth(username: &str, password: &str) -> Result<impl Drop, String> {
        use crate::fiber;
        use std::io::Write;
        use std::time::Duration;
        let res = std::process::Command::new("glauth").output();

        match res {
            Err(e @ std::io::Error { .. }) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err("`glauth` executable not found".into());
            }
            _ => {}
        }

        //
        // Create ldap configuration file
        //
        let tempdir = tempfile::tempdir().unwrap();
        let ldap_cfg_path = tempdir.path().join("ldap.cfg");
        let mut ldap_cfg_file = std::fs::File::create(&ldap_cfg_path).unwrap();

        const LDAP_SERVER_PORT: u16 = 1389;
        const LDAP_SERVER_HOST: &str = "127.0.0.1";

        let password_sha256 = sha256_hex(password);

        ldap_cfg_file
            .write_all(
                format!(
                    r#"
            [ldap]
                enabled = true
                listen = "{LDAP_SERVER_HOST}:{LDAP_SERVER_PORT}"

            [ldaps]
                enabled = false

            [backend]
                datastore = "config"
                baseDN = "dc=example,dc=org"

            [[users]]
                name = "{username}"
                uidnumber = 5001
                primarygroup = 5501
                passsha256 = "{password_sha256}"
                    [[users.capabilities]]
                        action = "search"
                        object = "*"

            [[groups]]
                name = "deep down in Louisianna"
                gidnumber = 5501
        "#
                )
                .as_bytes(),
            )
            .unwrap();
        // Close the file
        drop(ldap_cfg_file);

        //
        // Start the ldap server
        //
        println!();
        let mut ldap_server_process = std::process::Command::new("glauth")
            .arg("-c")
            .arg(&ldap_cfg_path)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .unwrap();

        // Wait for ldap server to start up
        let deadline = fiber::clock().saturating_add(Duration::from_secs(3));
        while fiber::clock() < deadline {
            let res = std::net::TcpStream::connect((LDAP_SERVER_HOST, LDAP_SERVER_PORT));
            match res {
                Ok(_) => {
                    // Ldap server is ready
                    break;
                }
                Err(_) => {
                    fiber::sleep(Duration::from_millis(100));
                }
            }
        }

        let guard = on_scope_exit(move || {
            crate::say_info!("killing ldap server");
            ldap_server_process.kill().unwrap();

            // Remove the temporary directory with it's contents
            drop(tempdir);
        });

        #[allow(dyn_drop)]
        let mut cleanup: Vec<Box<dyn Drop>> = vec![];
        cleanup.push(Box::new(guard));

        //
        // Configure tarantool
        //
        std::env::set_var(
            "TT_LDAP_URL",
            format!("ldap://{LDAP_SERVER_HOST}:{LDAP_SERVER_PORT}"),
        );
        std::env::set_var("TT_LDAP_DN_FMT", "cn=$USER,dc=example,dc=org");

        crate::lua_state()
            .exec_with(
                "local username = ...
                box.cfg { auth_type = 'ldap' }
                box.schema.user.create(username, { if_not_exists = true })
                box.schema.user.grant(username, 'super', nil, nil, { if_not_exists = true })",
                username,
            )
            .unwrap();

        let username = username.to_owned();
        let guard = on_scope_exit(move || {
            crate::lua_state()
                // This is the default
                .exec_with(
                    "local username = ...
                    box.cfg { auth_type = 'chap-sha1' }
                    box.schema.user.drop(username)",
                    username,
                )
                .unwrap();
        });
        cleanup.push(Box::new(guard));

        Ok(cleanup)
    }

    pub fn sha256_hex(s: &str) -> String {
        use std::io::Write;

        let tlua::AnyLuaString(bytes) = crate::lua_state()
            .eval_with("return require 'digest'.sha256(...)", s)
            .unwrap();

        let mut buffer = Vec::new();
        for b in bytes {
            write!(&mut buffer, "{b:02x}").unwrap();
        }

        String::from_utf8(buffer).unwrap()
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
