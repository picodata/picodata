//! Tarantool tests for auth methods implemented in picodata.
// Those were originally part of `tarantool-module`.
// However, implementation of some auth methods (md5, scram, ldap) was moved to picodata,
// so now these tests have to live in picodata with the auth methods they test.

use crate::tarantool::test_util::{listen_port, tls_listen_port};
use std::{io::Write as _, path::PathBuf, time::Duration};
use tarantool::{
    auth::{AuthData, AuthMethod},
    fiber::{self, r#async::timeout::IntoTimeout as _},
    net_box::{Conn, ConnOptions},
    network::{client::tls, protocol, AsClient as _, Client},
};

#[tarantool::test]
pub fn auth_data() {
    let data = AuthData::new(&AuthMethod::Md5, "user", "password");
    assert_eq!(&data.into_string(), "md54d45974e13472b5a0be3533de4666414");

    let data = AuthData::new(&AuthMethod::Ldap, "", "");
    assert_eq!(&data.into_string(), "");

    let data = AuthData::new(&AuthMethod::ScramSha256, "", "password");
    // Scram generates a random secret every time auth data is constructed,
    //  so we cannot have a fixed value to compare with here.
    assert!(&data.into_string().starts_with("SCRAM-SHA-256$4096:"));
}

/// Returns TLS connector.
pub fn get_tls_connector() -> tls::TlsConnector {
    let cargo_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path = cargo_path.join("tarantool/tests/ssl_certs");
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

pub fn sha256_hex(s: &str) -> String {
    use std::io::Write;

    let tarantool::tlua::AnyLuaString(bytes) = tarantool::lua_state()
        .eval_with("return require 'digest'.sha256(...)", s)
        .unwrap();

    let mut buffer = Vec::new();
    for b in bytes {
        write!(&mut buffer, "{b:02x}").unwrap();
    }

    String::from_utf8(buffer).unwrap()
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
        tarantool::say_info!("killing ldap server");
        ldap_server_process.kill().unwrap();
        ldap_server_process.wait().unwrap();

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

    tarantool::lua_state()
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
        tarantool::lua_state()
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

#[tarantool::test]
async fn md5_net_box() {
    let username = "Worry";
    let password = "B Gone";

    // NOTE: because we test our fork of `tarantool` here (see `picodata` feature flag on a test), we can
    // pass `auth_type` parameter right into `box.schema.user.create`. This won't work in default `tarantool`.
    tarantool::lua_state()
        .exec_with(
            "local username, password = ...
                box.cfg { }
                box.schema.user.create(username, { if_not_exists = true, auth_type = 'md5', password = password })
                box.schema.user.grant(username, 'super', nil, nil, { if_not_exists = true })",
            (username, password),
        )
        .unwrap();

    // Successful connection
    {
        let conn = Conn::new(
            ("localhost", listen_port()),
            ConnOptions {
                user: username.into(),
                password: password.into(),
                auth_method: AuthMethod::Md5,
                ..ConnOptions::default()
            },
            None,
        )
        .unwrap();

        conn.eval(
            "print('\\x1b[32mit works!\\x1b[0m')",
            &(),
            &Default::default(),
        )
        .unwrap();
    }

    // Wrong password
    {
        let conn = Conn::new(
            ("localhost", listen_port()),
            ConnOptions {
                user: username.into(),
                password: "wrong password".into(),
                auth_method: AuthMethod::Md5,
                ..ConnOptions::default()
            },
            None,
        )
        .unwrap();

        let err = conn
            .eval("return", &(), &Default::default())
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "server responded with error: PasswordMismatch: User not found or supplied credentials are invalid"
        );
    }

    // Wrong auth method
    {
        let conn = Conn::new(
            ("localhost", listen_port()),
            ConnOptions {
                user: username.into(),
                password: "wrong password".into(),
                auth_method: AuthMethod::ChapSha1,
                ..ConnOptions::default()
            },
            None,
        )
        .unwrap();

        let err = conn
            .eval("return", &(), &Default::default())
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "server responded with error: PasswordMismatch: User not found or supplied credentials are invalid"
        );
    }

    tarantool::lua_state()
        // This is the default
        .exec_with(
            "local username = ...
                box.cfg { auth_type = 'chap-sha1' }
                box.schema.user.drop(username)",
            username,
        )
        .unwrap();
}

#[tarantool::test]
async fn ldap_net_box() {
    use tarantool::auth::AuthMethod;

    let username = "Worry";
    let password = "B Gone";

    let _guard = tarantool::unwrap_ok_or!(
        setup_ldap_auth(username, password),
        Err(e) => {
            println!("{e}, skipping ldap test");
            return;
        }
    );

    // Successfull connection
    {
        let conn = Conn::new(
            ("localhost", listen_port()),
            ConnOptions {
                user: username.into(),
                password: password.into(),
                auth_method: AuthMethod::Ldap,
                ..ConnOptions::default()
            },
            None,
        )
        .unwrap();

        conn.eval(
            "print('\\x1b[32mit works!\\x1b[0m')",
            &(),
            &Default::default(),
        )
        .unwrap();
    }

    // Wrong password
    {
        let conn = Conn::new(
            ("localhost", listen_port()),
            ConnOptions {
                user: username.into(),
                password: "wrong password".into(),
                auth_method: AuthMethod::Ldap,
                ..ConnOptions::default()
            },
            None,
        )
        .unwrap();

        let err = conn
            .eval("return", &(), &Default::default())
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "server responded with error: System: Invalid credentials"
        );
    }

    // Wrong auth method
    {
        let conn = Conn::new(
            ("localhost", listen_port()),
            ConnOptions {
                user: username.into(),
                password: "wrong password".into(),
                auth_method: AuthMethod::ChapSha1,
                ..ConnOptions::default()
            },
            None,
        )
        .unwrap();

        let err = conn
            .eval("return", &(), &Default::default())
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "server responded with error: PasswordMismatch: User not found or supplied credentials are invalid"
        );
    }
}

#[tarantool::test]
async fn md5_auth_method() {
    let username = "Johnny";
    let password = "B. Goode";

    // NOTE: because we test our fork of `tarantool` here (see `picodata` feature flag on a test), we can
    // pass `auth_type` parameter right into `box.schema.user.create`. This won't work in default `tarantool`.
    tarantool::lua_state()
        .exec_with(
            "local username, password = ...
                box.cfg { }
                box.schema.user.create(username, { if_not_exists = true, auth_type = 'md5', password = password })
                box.schema.user.grant(username, 'super', nil, nil, { if_not_exists = true })",
            (username, password),
        )
        .unwrap();

    // Successful connection
    {
        let client = Client::connect_with_config("localhost", listen_port(), {
            // cannot use struct update syntax here due to https://rust-lang.github.io/rfcs/2008-non-exhaustive.html#functional-record-updates
            let mut config = protocol::Config::default();
            config.creds = Some((username.into(), password.into()));
            config.auth_method = AuthMethod::Md5;
            config
        })
        .timeout(Duration::from_secs(3))
        .await
        .unwrap();

        // network::Client will not try actually connecting until we send the
        // first request
        client
            .eval("print('\\x1b[32mit works!\\x1b[0m')", &())
            .await
            .unwrap();
    }

    // Wrong password
    {
        let client = Client::connect_with_config("localhost", listen_port(), {
            // cannot use struct update syntax here due to https://rust-lang.github.io/rfcs/2008-non-exhaustive.html#functional-record-updates
            let mut config = protocol::Config::default();
            config.creds = Some((username.into(), "wrong password".into()));
            config.auth_method = AuthMethod::Md5;
            config
        })
        .timeout(Duration::from_secs(3))
        .await
        .unwrap();

        // network::Client will not try actually connecting until we send the
        // first request
        let err = client.eval("return", &()).await.unwrap_err().to_string();
        #[rustfmt::skip]
            assert_eq!(err, "server responded with error: PasswordMismatch: User not found or supplied credentials are invalid");
    }

    // Wrong auth method
    {
        let client = Client::connect_with_config("localhost", listen_port(), {
            // cannot use struct update syntax here due to https://rust-lang.github.io/rfcs/2008-non-exhaustive.html#functional-record-updates
            let mut config = protocol::Config::default();
            config.creds = Some((username.into(), password.into()));
            config.auth_method = AuthMethod::ChapSha1;
            config
        })
        .timeout(Duration::from_secs(3))
        .await
        .unwrap();

        // network::Client will not try actually connecting until we send the
        // first request
        let err = client.eval("return", &()).await.unwrap_err().to_string();
        #[rustfmt::skip]
            assert_eq!(err, "server responded with error: PasswordMismatch: User not found or supplied credentials are invalid");
    }

    tarantool::lua_state()
        // This is the default
        .exec_with(
            "local username = ...
                box.cfg { auth_type = 'chap-sha1' }
                box.schema.user.drop(username)",
            username,
        )
        .unwrap();
}

#[tarantool::test]
async fn tls_md5_auth_method() {
    let username = "Johnny";
    let password = "B. Goode";

    // NOTE: because we test our fork of `tarantool` here (see `picodata` feature flag on a test), we can
    // pass `auth_type` parameter right into `box.schema.user.create`. This won't work in default `tarantool`.
    tarantool::lua_state()
        .exec_with(
            "local username, password = ...
                box.cfg { }
                box.schema.user.create(username, { if_not_exists = true, auth_type = 'md5', password = password })
                box.schema.user.grant(username, 'super', nil, nil, { if_not_exists = true })",
            (username, password),
        )
        .unwrap();

    // Successful connection
    {
        let client = Client::connect_with_config_and_tls(
            "127.0.0.1",
            tls_listen_port(),
            {
                // cannot use struct update syntax here due to https://rust-lang.github.io/rfcs/2008-non-exhaustive.html#functional-record-updates
                let mut config = protocol::Config::default();
                config.creds = Some((username.into(), password.into()));
                config.auth_method = AuthMethod::Md5;
                config
            },
            Some(get_tls_connector()),
        )
        .timeout(Duration::from_secs(3))
        .await
        .unwrap();

        // network::Client will not try actually connecting until we send the
        // first request
        client
            .eval("print('\\x1b[32mit works!\\x1b[0m')", &())
            .await
            .unwrap();
    }

    // Wrong password
    {
        let client = Client::connect_with_config_and_tls(
            "127.0.0.1",
            tls_listen_port(),
            {
                // cannot use struct update syntax here due to https://rust-lang.github.io/rfcs/2008-non-exhaustive.html#functional-record-updates
                let mut config = protocol::Config::default();
                config.creds = Some((username.into(), "wrong password".into()));
                config.auth_method = AuthMethod::Md5;
                config
            },
            Some(get_tls_connector()),
        )
        .timeout(Duration::from_secs(3))
        .await
        .unwrap();

        // network::Client will not try actually connecting until we send the
        // first request
        let err = client.eval("return", &()).await.unwrap_err().to_string();
        #[rustfmt::skip]
            assert_eq!(err, "server responded with error: PasswordMismatch: User not found or supplied credentials are invalid");
    }

    // Wrong auth method
    {
        let client = Client::connect_with_config_and_tls(
            "127.0.0.1",
            tls_listen_port(),
            {
                // cannot use struct update syntax here due to https://rust-lang.github.io/rfcs/2008-non-exhaustive.html#functional-record-updates
                let mut config = protocol::Config::default();
                config.creds = Some((username.into(), password.into()));
                config.auth_method = AuthMethod::ChapSha1;
                config
            },
            Some(get_tls_connector()),
        )
        .timeout(Duration::from_secs(3))
        .await
        .unwrap();

        // network::Client will not try actually connecting until we send the
        // first request
        let err = client.eval("return", &()).await.unwrap_err().to_string();
        #[rustfmt::skip]
            assert_eq!(err, "server responded with error: PasswordMismatch: User not found or supplied credentials are invalid");
    }

    tarantool::lua_state()
        // This is the default
        .exec_with(
            "local username = ...
                box.cfg { auth_type = 'chap-sha1' }
                box.schema.user.drop(username)",
            username,
        )
        .unwrap();
}

#[tarantool::test]
async fn ldap_auth_method() {
    let username = "Johnny";
    let password = "B. Goode";

    let _guard = tarantool::unwrap_ok_or!(
        setup_ldap_auth(username, password),
        Err(e) => {
            println!("{e}, skipping ldap test");
            return;
        }
    );

    // Successfull connection
    {
        let client = Client::connect_with_config("localhost", listen_port(), {
            // cannot use struct update syntax here due to https://rust-lang.github.io/rfcs/2008-non-exhaustive.html#functional-record-updates
            let mut config = protocol::Config::default();
            config.creds = Some((username.into(), password.into()));
            config.auth_method = AuthMethod::Ldap;
            config
        })
        .timeout(Duration::from_secs(3))
        .await
        .unwrap();

        // network::Client will not try actually connecting until we send the
        // first request
        client
            .eval("print('\\x1b[32mit works!\\x1b[0m')", &())
            .await
            .unwrap();
    }

    // Wrong password
    {
        let client = Client::connect_with_config("localhost", listen_port(), {
            // cannot use struct update syntax here due to https://rust-lang.github.io/rfcs/2008-non-exhaustive.html#functional-record-updates
            let mut config = protocol::Config::default();
            config.creds = Some((username.into(), "wrong password".into()));
            config.auth_method = AuthMethod::Ldap;
            config
        })
        .timeout(Duration::from_secs(3))
        .await
        .unwrap();

        // network::Client will not try actually connecting until we send the
        // first request
        let err = client.eval("return", &()).await.unwrap_err().to_string();
        #[rustfmt::skip]
            assert_eq!(err, "server responded with error: System: Invalid credentials");
    }

    // Wrong auth method
    {
        let client = Client::connect_with_config("localhost", listen_port(), {
            // cannot use struct update syntax here due to https://rust-lang.github.io/rfcs/2008-non-exhaustive.html#functional-record-updates
            let mut config = protocol::Config::default();
            config.creds = Some((username.into(), password.into()));
            config.auth_method = AuthMethod::ChapSha1;
            config
        })
        .timeout(Duration::from_secs(3))
        .await
        .unwrap();

        // network::Client will not try actually connecting until we send the
        // first request
        let err = client.eval("return", &()).await.unwrap_err().to_string();
        #[rustfmt::skip]
            assert_eq!(err, "server responded with error: PasswordMismatch: User not found or supplied credentials are invalid");
    }
}
