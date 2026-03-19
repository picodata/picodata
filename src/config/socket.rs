//! Unified socket configuration for picodata protocols.
//!
//! This module provides a consistent configuration interface for all protocols
//! (iproto, http, pgproto) and plugin listeners.

use crate::address::{HttpAddress, IprotoAddress, PgprotoAddress, PluginAddress};
use crate::introspection::Introspection;
use std::path::PathBuf;

////////////////////////////////////////////////////////////////////////////////
// TlsSettings
////////////////////////////////////////////////////////////////////////////////

/// Unified TLS configuration for all protocols.
///
/// This structure is used by `http`, `iproto`, `pgproto`, and plugin listeners.
#[derive(
    Clone, Debug, Default, Eq, PartialEq, Introspection, serde::Deserialize, serde::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct TlsSettings {
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

impl TlsSettings {
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

////////////////////////////////////////////////////////////////////////////////
// HttpConfig (new unified http section)
////////////////////////////////////////////////////////////////////////////////

/// Unified HTTP protocol configuration.
///
/// This replaces the old `http_listen` and `https` fields with a single
/// consistent structure.
#[derive(
    Clone, Debug, Default, Eq, PartialEq, Introspection, serde::Deserialize, serde::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    /// Whether HTTP protocol is enabled.
    /// Defaults to `true` if the section is present.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[introspection(config_default = true)]
    pub enabled: Option<bool>,

    /// The address to listen on.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[introspection(config_default = HttpAddress::default())]
    pub listen: Option<HttpAddress>,

    /// The address to advertise to other nodes.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[introspection(config_default = self.listen())]
    pub advertise: Option<HttpAddress>,

    /// TLS configuration for HTTPS.
    #[serde(default, skip_serializing_if = "TlsSettings::is_default")]
    #[introspection(nested)]
    pub tls: TlsSettings,

    /// Whether Kubernetes health probe endpoints are enabled.
    /// When false, /health/live, /health/ready, and /health/startup return 404.
    /// Defaults to `true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[introspection(config_default = true)]
    pub kubernetes_probes: Option<bool>,
}

impl HttpConfig {
    /// Returns whether HTTP protocol is enabled.
    /// Defaults to `true` when the http section is present in config.
    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    #[inline]
    pub fn listen(&self) -> HttpAddress {
        self.listen
            .clone()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    pub fn advertise(&self) -> HttpAddress {
        self.advertise
            .clone()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    /// Returns whether Kubernetes health probe endpoints are enabled.
    /// Defaults to `true`.
    #[inline]
    pub fn kubernetes_probes(&self) -> bool {
        self.kubernetes_probes.unwrap_or(true)
    }

    /// Returns true if this section has any configuration.
    pub fn has_any_setting(&self) -> bool {
        self.enabled.is_some()
            || self.listen.is_some()
            || self.advertise.is_some()
            || self.tls.has_any_setting()
            || self.kubernetes_probes.is_some()
    }

    /// Returns true if this is the default (empty) configuration.
    /// Used for skip_serializing_if.
    pub fn is_default(&self) -> bool {
        !self.has_any_setting()
    }
}

////////////////////////////////////////////////////////////////////////////////
// IprotoConfig (new unified iproto section)
////////////////////////////////////////////////////////////////////////////////

/// Unified iproto protocol configuration.
///
/// This replaces the old `iproto_listen`, `iproto_advertise`, and `iproto_tls`
/// fields with a single consistent structure.
#[derive(
    Clone, Debug, Default, Eq, PartialEq, Introspection, serde::Deserialize, serde::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct IprotoConfig {
    /// Whether iproto protocol is enabled.
    /// Defaults to `true` if not specified.
    /// Note: iproto cannot be disabled in picodata.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[introspection(config_default = true)]
    pub enabled: Option<bool>,

    /// The address to listen on.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[introspection(config_default = IprotoAddress::default())]
    pub listen: Option<IprotoAddress>,

    /// The address to advertise to other nodes.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[introspection(config_default = self.listen())]
    pub advertise: Option<IprotoAddress>,

    /// TLS configuration.
    #[serde(default, skip_serializing_if = "TlsSettings::is_default")]
    #[introspection(nested)]
    pub tls: TlsSettings,
}

impl IprotoConfig {
    /// Returns whether iproto protocol is enabled.
    /// Defaults to `true` if not specified.
    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }

    #[inline]
    pub fn listen(&self) -> IprotoAddress {
        self.listen
            .clone()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    pub fn advertise(&self) -> IprotoAddress {
        self.advertise
            .clone()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    /// Returns true if this section has any configuration.
    pub fn has_any_setting(&self) -> bool {
        self.enabled.is_some()
            || self.listen.is_some()
            || self.advertise.is_some()
            || self.tls.has_any_setting()
    }

    /// Returns true if this is the default (empty) configuration.
    /// Used for skip_serializing_if.
    pub fn is_default(&self) -> bool {
        !self.has_any_setting()
    }
}

////////////////////////////////////////////////////////////////////////////////
// PgprotoConfig (new unified pgproto section)
////////////////////////////////////////////////////////////////////////////////

/// Unified pgproto (PostgreSQL wire protocol) configuration.
///
/// This replaces the old `pg` section with a consistent structure.
#[derive(
    Clone, Debug, Default, Eq, PartialEq, Introspection, serde::Deserialize, serde::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct PgprotoConfig {
    /// Whether pgproto is enabled.
    /// Defaults to `true` if not specified.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[introspection(config_default = true)]
    pub enabled: Option<bool>,

    /// The address to listen on.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[introspection(config_default = PgprotoAddress::default())]
    pub listen: Option<PgprotoAddress>,

    /// The address to advertise to other nodes.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[introspection(config_default = self.listen())]
    pub advertise: Option<PgprotoAddress>,

    /// TLS configuration.
    #[serde(default, skip_serializing_if = "TlsSettings::is_default")]
    #[introspection(nested)]
    pub tls: TlsSettings,
}

impl PgprotoConfig {
    /// Returns whether pgproto is enabled.
    /// Defaults to `true` if not specified.
    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }

    #[inline]
    pub fn listen(&self) -> PgprotoAddress {
        self.listen
            .clone()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    pub fn advertise(&self) -> PgprotoAddress {
        self.advertise
            .clone()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    /// Returns true if this section has any configuration.
    pub fn has_any_setting(&self) -> bool {
        self.enabled.is_some()
            || self.listen.is_some()
            || self.advertise.is_some()
            || self.tls.has_any_setting()
    }

    /// Returns true if this is the default (empty) configuration.
    /// Used for skip_serializing_if.
    pub fn is_default(&self) -> bool {
        !self.has_any_setting()
    }
}

////////////////////////////////////////////////////////////////////////////////
// PluginListenerConfig
////////////////////////////////////////////////////////////////////////////////

/// Configuration for a plugin service listener.
///
/// Each plugin service can have at most one listener configured.
#[derive(
    Clone, Debug, Default, Eq, PartialEq, Introspection, serde::Deserialize, serde::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct PluginListenerConfig {
    /// Whether the listener is enabled.
    /// Defaults to `true` if not specified.
    pub enabled: Option<bool>,

    /// The address to listen on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen: Option<PluginAddress>,

    /// The address to advertise to other nodes.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[introspection(config_default = self.listen)]
    pub advertise: Option<PluginAddress>,

    /// TLS configuration.
    #[serde(default, skip_serializing_if = "TlsSettings::is_default")]
    #[introspection(nested)]
    pub tls: TlsSettings,
}

impl PluginListenerConfig {
    // NOTE: here we implement option defaults as methods instead of relying on `#[introspection(config_default = ...)]`
    // because the structure lives inside a `HashMap`, which is not supported well by the library
    /// Returns whether the listener is enabled.
    /// Defaults to `true` if not specified.
    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }

    /// Returns the advertise address, falling back to listen address.
    #[inline]
    pub fn advertise(&self) -> Option<&PluginAddress> {
        self.advertise.as_ref().or(self.listen.as_ref())
    }

    /// Returns true if any listener-related field is set.
    pub fn has_any_setting(&self) -> bool {
        self.enabled.is_some()
            || self.listen.is_some()
            || self.advertise.is_some()
            || self.tls.has_any_setting()
    }
}

////////////////////////////////////////////////////////////////////////////////
// PluginServiceConfig
////////////////////////////////////////////////////////////////////////////////

/// Configuration for a single plugin service.
#[derive(
    Clone, Debug, Default, Eq, PartialEq, Introspection, serde::Deserialize, serde::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct PluginServiceConfig {
    /// Listener configuration for this service.
    #[serde(default)]
    #[introspection(nested)]
    pub listener: PluginListenerConfig,
}

////////////////////////////////////////////////////////////////////////////////
// PluginConfig
////////////////////////////////////////////////////////////////////////////////

/// Configuration for a single plugin.
#[derive(
    Clone, Debug, Default, Eq, PartialEq, Introspection, serde::Deserialize, serde::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct PluginConfig {
    /// Service configurations indexed by service name.
    #[serde(default)]
    pub service: std::collections::HashMap<String, PluginServiceConfig>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PicodataConfig;

    #[test]
    fn test_http_config_defaults() {
        let http = PicodataConfig::with_defaults().instance.http;
        assert!(http.enabled());
        assert_eq!(http.listen(), HttpAddress::default());
        assert!(!http.tls.enabled());
        assert!(http.kubernetes_probes());
    }

    #[test]
    fn test_iproto_config_defaults() {
        let iproto = PicodataConfig::with_defaults().instance.iproto;
        assert!(iproto.enabled());
        assert_eq!(iproto.listen(), IprotoAddress::default());
        assert!(!iproto.tls.enabled());
    }

    #[test]
    fn test_pgproto_config_defaults() {
        let pgproto = PicodataConfig::with_defaults().instance.pgproto;
        assert!(pgproto.enabled());
        assert_eq!(pgproto.listen(), PgprotoAddress::default());
        assert!(!pgproto.tls.enabled());
    }

    #[test]
    fn test_plugin_listener_config() {
        let addr: PluginAddress = "127.0.0.1:7777".parse().unwrap();
        let listener = PluginListenerConfig {
            enabled: None,
            listen: Some(addr.clone()),
            advertise: None,
            tls: TlsSettings::default(),
        };
        assert!(listener.enabled());
        assert_eq!(listener.listen, Some(addr.clone()));
        assert_eq!(listener.advertise(), Some(&addr)); // falls back to listen
    }

    #[test]
    fn test_yaml_deserialization() {
        let yaml = r#"
cluster:
  name: demo
instance:
  http:
    enabled: true
    listen: "127.0.0.1:8080"
    advertise: "public.example.com:8080"
    kubernetes_probes: false
    tls:
      enabled: true
      cert_file: /path/to/cert.pem
      key_file: /path/to/key.pem
  plugin:
    plugin_name:
      service:
        service_name:
          listener:
            listen: 127.0.0.1:7777
"#;

        let config = PicodataConfig::read_yaml_contents(yaml).unwrap();
        let http = config.instance.http;
        assert!(http.enabled());
        assert!(!http.kubernetes_probes());
        assert_eq!(http.listen.unwrap().to_string(), "127.0.0.1:8080");
        assert_eq!(
            http.advertise.unwrap().to_string(),
            "public.example.com:8080"
        );
        assert!(http.tls.enabled());
    }
}
