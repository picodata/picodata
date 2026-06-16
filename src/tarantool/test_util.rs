//! Utilities to use when writing picodata tarantool tests.
//!
//! NOTE: this code makes assumptions about the testing environment. Do not use outside of tests.

use smol_str::SmolStr;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tarantool::network::client::tls;

// Provide some certificates to test iproto with TLS in `picodata test`.

// This is a little bit ugly: on one hand, we want to embed the certificates into the executable
// to make `picodata test` work in release versions because some people like it.
// On the other hand, tarantool's API doesn't let us pass certificate data directly.
// It has to be written to a file first. So we provide a function that will write
// the certificates to a temporary directory, which is called at the start of a test.
// We then store the paths to the certificates in `TEST_TLS_CERT_PATHS` for tests
// to access.
pub struct TlsCertificateData {
    pub cert: &'static [u8],
    pub key: &'static [u8],
    pub ca: &'static [u8],
}

impl TlsCertificateData {
    pub fn write_to(&self, path: &Path) -> std::io::Result<TlsCertificatePaths> {
        std::fs::create_dir_all(path)?;

        let cert_file = path.join("server.crt");
        let key_file = path.join("server.key");
        let ca_file = path.join("combined-ca.crt");

        std::fs::write(&cert_file, self.cert)?;
        std::fs::write(&key_file, self.key)?;
        std::fs::write(&ca_file, self.ca)?;

        Ok(TlsCertificatePaths {
            cert_file,
            key_file,
            ca_file,
        })
    }
}

pub struct TlsCertificatePaths {
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
    pub ca_file: PathBuf,
}

pub const TEST_TLS_CERTS: TlsCertificateData = TlsCertificateData {
    cert: include_bytes!("../../tarantool/tests/ssl_certs/server.crt"),
    key: include_bytes!("../../tarantool/tests/ssl_certs/server.key"),
    ca: include_bytes!("../../tarantool/tests/ssl_certs/combined-ca.crt"),
};
pub static TEST_TLS_CERT_PATHS: OnceLock<TlsCertificatePaths> = OnceLock::new();

/// Returns TLS connector.
pub fn get_tls_connector() -> tls::TlsConnector {
    let paths = TEST_TLS_CERT_PATHS
        .get()
        .expect("TLS certs paths are not initialized");
    let tls_config = tls::TlsConfig {
        cert_file: &paths.cert_file,
        key_file: &paths.key_file,
        ca_file: Some(&paths.ca_file),
    };
    tls::TlsConnector::new(tls_config).unwrap()
}

/// Returns the binary protocol listen address of the current tarantool instance.
/// It is the first listener in the config.
pub fn listen_address() -> SmolStr {
    let lua = tarantool::lua_state();
    lua.eval("return (box.info.listen[1] or box.info.listen)")
        .unwrap()
}

/// Returns the binary protocol port of the current tarantool instance.
/// It is the first port in the config.
pub fn listen_port() -> u16 {
    let listen = listen_address();
    let (_address, port) = listen.rsplit_once(':').unwrap();
    port.parse().unwrap()
}

/// Returns the TLS binary protocol listen address of the current tarantool instance.
/// It is the second listener in the config.
pub fn tls_listen_address() -> SmolStr {
    let lua = tarantool::lua_state();
    lua.eval("return box.info.listen[2]").unwrap()
}

/// Returns the TLS binary protocol port of the current tarantool instance.
/// It is the second port in the config.
pub fn tls_listen_port() -> u16 {
    let listen = tls_listen_address();
    let (_address, port) = listen.rsplit_once(':').unwrap();
    port.parse().unwrap()
}
