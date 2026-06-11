use crate::auth::tarantool::AuthenticationVerdict;
use crate::auth::typed_method::{AuthMethod, Authenticator};
use crate::auth::SALT_LEN;
use crate::config::TlsClientMethod;
use crate::tlog;
use std::sync::{LazyLock, OnceLock};
use tarantool::error::{BoxError, TarantoolErrorCode};

use crate::auth::methods::ldap::config::LdapRuntimeTlsConfiguration;
pub use config::{LdapDnFormatString, LdapRuntimeConfiguration};

mod bridge;
mod config;

fn get_config() -> Option<&'static LdapRuntimeConfiguration> {
    // NB: This can be called before the picodata config is initialized,
    //  for example when using `picodata tarantool`. Handle this case gracefully.

    // NB: do not cache the uninitialized picodata config,
    //  it may be set at a later point in time.
    let config = crate::config::PicodataConfig::try_get()?;

    static CACHED_CONFIG: OnceLock<Option<LdapRuntimeConfiguration>> = OnceLock::new();
    CACHED_CONFIG
        .get_or_init(|| {
            LdapRuntimeConfiguration::from_config_section(&config.instance.ldap, true)
                .unwrap_or_else(|error| {
                    // This won't happen normally, since picodata will validate the
                    // config on start. It can, however, happen in a faulty unit test.
                    tlog!(Error, "LDAP configuration is invalid: {}", error);
                    None
                })
        })
        .as_ref()
}

/// Use [`LdapRuntimeConfiguration`] to construct an [`ldap3::Ldap`].
async fn get_ldap_client(
    config: &LdapRuntimeConfiguration,
) -> Result<ldap3::Ldap, ldap3::LdapError> {
    let address = &config.connect;

    let mut settings = ldap3::LdapConnSettings::new();
    let scheme;
    if let Some(tls) = &config.tls {
        let LdapRuntimeTlsConfiguration { method, connector } = tls;

        match method {
            TlsClientMethod::Implicit => {
                scheme = "ldaps";
                settings = settings.set_starttls(false);
            }
            TlsClientMethod::StartTls => {
                scheme = "ldap";
                settings = settings.set_starttls(true);
            }
        }

        // Passing a TLS connector explicitly lets us do two things:
        // 1. Have our custom TLS settings applied -- alternative root CAs and
        //    `LDAP_NO_TLS_VERIFICATION` injection
        // 2. Avoid overhead of building the connector for each connection.
        //   `native-tls`'s connector will re-read root CAs every time it's created, and creating
        //   it for every connection, as does `ldap3` by default, will tank the performance.
        settings = settings.set_connector(connector.clone());
    } else {
        scheme = "ldap";
    };

    let url = format!("{scheme}://{address}");
    let host_port = format!("{address}");

    // Instead of letting `ldap3` create the socket, do it ourselves. This lets us set
    //  the `TCP_NODELAY` option on the socket, which prevents serious latency issues
    //  observable in certain configurations.
    let socket = tokio::net::TcpStream::connect(&host_port).await?;
    socket.set_nodelay(true)?;
    let settings = settings.set_std_stream(ldap3::StdStream::Tcp(socket.into_std().unwrap()));

    let (conn, client) = ldap3::LdapConnAsync::with_settings(settings, &url).await?;

    tokio::spawn(async move {
        if let Err(e) = conn.drive().await {
            tlog!(Error, "LDAP connection error: {}", e);
        }
    });

    Ok(client)
}

/// Perform a single user authentication attempt.
///
/// This function will:
/// 1. Open a new connection to the LDAP server
/// 2. Convert the passed username to a DN using the configured format string
/// 3. Perform a simple LDAP bind request using the derived DN and the provided password
/// 4. Gracefully terminate the connection to the LDAP server by sending an unbind request
/// 5. Derive an authentication verdict from the result returned by the simple bind request
async fn ldap_check_password(
    config: &'static LdapRuntimeConfiguration,
    user: String,
    password: String,
) -> Result<AuthenticationVerdict, ldap3::LdapError> {
    let mut client = get_ldap_client(config).await.map_err(|e| {
        tlog!(Warning, "failed to connect to LDAP server: {e}");
        e
    })?;

    let dn = config.dn_format.format(&user);

    tlog!(Debug, "attempting LDAP BIND as '{dn}'");

    let result = client.simple_bind(&dn, &password).await.map_err(|e| {
        tlog!(Warning, "failed to send a BIND request to LDAP server: {e}");
        e
    })?;

    // Don't make errors on unbind fatal. Just log them.
    if let Err(e) = client.unbind().await {
        tlog!(Warning, "failed to unbind LDAP: {e}");
    }

    // Result codes are defined in Section A.1 of RFC 4511.
    // Unfortunately, ldap3 crate doesn't define these constants for us,
    //  so we have to make our own.
    // See: https://tools.ietf.org/html/rfc4511#appendix-A.1
    const LDAP_RESULT_SUCCESS: u32 = 0;
    const LDAP_RESULT_INVALID_CREDENTIALS: u32 = 49;

    match result.rc {
        LDAP_RESULT_SUCCESS => Ok(AuthenticationVerdict::Authenticated),
        LDAP_RESULT_INVALID_CREDENTIALS => Ok(AuthenticationVerdict::Rejected),
        _ => Err(ldap3::LdapError::LdapResult { result }),
    }
}

/// The async runtime we will run the `ldap3` futures in.
static RUNTIME: LazyLock<bridge::TokioBridge> =
    LazyLock::new(|| bridge::TokioBridge::new("picodata-ldap"));

pub struct LdapAuthMethod;

// SAFETY: there are no usages of tarantool box region in any of the methods
unsafe impl AuthMethod for LdapAuthMethod {
    const NAME: &'static std::ffi::CStr = c"ldap";
    const PASSWORDLESS_DATA_PREPARE: bool = true;
    type Authenticator = LdapAuthenticator;

    type AuthDataOut<'a> = &'static str;
    type AuthDataIn<'a> = &'a str;

    type AuthRequestOut<'a> = &'a [u8];
    type AuthRequestIn<'a> = &'a str; // password

    type CheckedAuthRequest<'a> = &'a str;

    fn auth_data_prepare<'a>(&self, _password: &'a [u8], _user: &'a [u8]) -> Self::AuthDataOut<'a> {
        ""
    }

    fn auth_request_prepare<'a>(
        &self,
        password: &'a [u8],
        _user: &'a [u8],
        _salt: &'a [u8; SALT_LEN],
    ) -> Self::AuthRequestOut<'a> {
        password
    }

    fn auth_request_check<'a>(
        &self,
        auth_request: Self::AuthRequestIn<'a>,
    ) -> tarantool::Result<Self::CheckedAuthRequest<'a>> {
        Ok(auth_request)
    }

    fn authenticator_new(
        &self,
        _auth_data: Self::AuthDataIn<'_>,
    ) -> tarantool::Result<Self::Authenticator> {
        Ok(LdapAuthenticator)
    }
}

pub struct LdapAuthenticator;

impl Authenticator for LdapAuthenticator {
    type Method = LdapAuthMethod;

    fn authenticate_request(
        &self,
        user: &[u8],
        _salt: &[u8; SALT_LEN],
        auth_request: <Self::Method as AuthMethod>::CheckedAuthRequest<'_>,
    ) -> tarantool::Result<AuthenticationVerdict> {
        let user = std::str::from_utf8(user).map_err(|_| {
            BoxError::new(
                TarantoolErrorCode::InvalidAuthRequest,
                "username must be a valid UTF-8 string",
            )
        })?;
        let password = auth_request;

        let Some(config) = get_config() else {
            return Err(BoxError::new(
                TarantoolErrorCode::System,
                "LDAP is not configured. Check that `instance.ldap.enabled` is true",
            )
            .into());
        };

        let result = RUNTIME.block_on(ldap_check_password(
            config,
            user.to_string(),
            password.to_string(),
        ));
        let verdict = result.map_err(|e| {
            BoxError::new(
                TarantoolErrorCode::System,
                format!("authentication failed: {e}"),
            )
        })?;

        Ok(verdict)
    }
}
