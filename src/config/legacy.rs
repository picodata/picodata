//! Contains definitions for deprecated portions of the config file.
//! Those are retained to aid in migration from the older configuration interfaces and should not be used as representations for config options.

use crate::address::PgprotoAddress;
use pico_proc_macro::Introspection;
use std::path::PathBuf;

#[derive(
    Clone, Debug, Default, Eq, Introspection, PartialEq, serde::Deserialize, serde::Serialize,
)]
pub struct LegacyIprotoTlsConfig {
    #[deprecated = "use iproto.tls.enabled instead"]
    pub enabled: Option<bool>,
    #[deprecated = "use iproto.tls.cert_file instead"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_file: Option<PathBuf>,
    #[deprecated = "use iproto.tls.key_file instead"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_file: Option<PathBuf>,
    #[deprecated = "use iproto.tls.ca_file instead"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,
}

impl LegacyIprotoTlsConfig {
    /// Returns true if all fields are None (default state).
    /// Used for skip_serializing_if to hide deprecated section in default config.
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(
    Clone, Debug, Default, Eq, Introspection, PartialEq, serde::Deserialize, serde::Serialize,
)]
pub struct LegacyHttpsConfig {
    #[deprecated = "use http.tls.enabled instead"]
    pub enabled: Option<bool>,
    #[deprecated = "use http.tls.cert_file instead"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_file: Option<PathBuf>,
    #[deprecated = "use http.tls.key_file instead"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_file: Option<PathBuf>,
    #[deprecated = "use http.tls.ca_file instead"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,
    #[deprecated = "use http.tls.password_file instead"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password_file: Option<PathBuf>,
}

impl LegacyHttpsConfig {
    /// Returns true if all fields are None (default state).
    /// Used for skip_serializing_if to hide deprecated section in default config.
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// Main postgres server configuration.
#[derive(PartialEq, Default, Debug, Clone, serde::Deserialize, serde::Serialize, Introspection)]
#[serde(deny_unknown_fields)]
pub struct LegacyPgConfig {
    #[deprecated = "use pgproto.listen instead"]
    pub listen: Option<PgprotoAddress>,

    #[deprecated = "use pgproto.advertise instead"]
    pub advertise: Option<PgprotoAddress>,

    #[deprecated = "use pgproto.tls.enabled instead"]
    pub ssl: Option<bool>,

    #[deprecated = "use pgproto.tls.cert_file instead"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_file: Option<PathBuf>,
    #[deprecated = "use pgproto.tls.key_file instead"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_file: Option<PathBuf>,
    #[deprecated = "use pgproto.tls.ca_file instead"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,
}

impl LegacyPgConfig {
    /// Returns true if all fields are None (default state).
    /// Used for skip_serializing_if to hide deprecated section in default config.
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}
