use pico_proc_macro::Introspection;
use std::path::PathBuf;

/// Unified TLS listener configuration for all protocols.
///
/// This structure is used by `http`, `iproto`, `pgproto`, and plugin listeners.
#[derive(
    Clone, Debug, Default, Eq, PartialEq, Introspection, serde::Deserialize, serde::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct TlsListenerSettings {
    /// Whether TLS is enabled for this listener.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[introspection(config_default = false)]
    pub enabled: Option<bool>,

    /// Path to the certificate file (PEM format).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_file: Option<PathBuf>,

    /// Path to the private key file (PEM format).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_file: Option<PathBuf>,

    /// Path to the CA certificate file for mTLS (PEM format).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,

    /// Path to the password file for encrypted private keys.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password_file: Option<PathBuf>,
}

impl TlsListenerSettings {
    // NOTE: here we implement option defaults as methods instead of relying on `#[introspection(config_default = ...)]`
    // because the structure maybe be part of `PluginListenerConfig` living inside a `HashMap`, which is not supported well by the introspection library
    /// Returns whether TLS is enabled.
    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(false)
    }

    /// Returns true if any TLS-related field is set.
    pub fn has_any_setting(&self) -> bool {
        self.enabled.is_some()
            || self.cert_file.is_some()
            || self.key_file.is_some()
            || self.ca_file.is_some()
            || self.password_file.is_some()
    }

    /// Returns true if this is the default (empty) TLS configuration.
    /// Used for skip_serializing_if.
    pub fn is_default(&self) -> bool {
        !self.has_any_setting()
    }
}

tarantool::define_str_enum! {
    #[derive(Default)]
    pub enum TlsClientMethod {
        /// Implicit TLS - the connection is initiated as TLS without any protocol-specific signaling.
        #[default]
        Implicit = "implicit",
        /// StartTLS - the connection starts unencrypted and then switches to TLS via a protocol-specific mechanism.
        StartTls = "start_tls",
    }
}

/// TLS Client configuration.
#[derive(
    Clone, Debug, Default, Eq, PartialEq, Introspection, serde::Deserialize, serde::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct TlsClientSettings {
    /// Whether TLS should be used when connecting.
    #[introspection(config_default = false)]
    pub enabled: Option<bool>,

    /// TLS connection method to use.
    #[introspection(config_default = TlsClientMethod::default())]
    pub method: Option<TlsClientMethod>,

    /// Path to custom root CA file to trust. If specified, system CA files are ignored.
    pub ca_file: Option<PathBuf>,
}

impl TlsClientSettings {
    pub fn enabled(&self) -> bool {
        self.enabled
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }
    pub fn method(&self) -> TlsClientMethod {
        self.method
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }
}
