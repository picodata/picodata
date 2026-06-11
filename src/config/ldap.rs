use crate::address::LdapAddress;
use crate::auth::methods::ldap::LdapDnFormatString;
use crate::config::{TlsClientMethod, TlsClientSettings};
use pico_proc_macro::Introspection;

/// Helpers to access LDAP environment variables in a more concise manner.
mod env {
    pub fn get_str(var: &str) -> Result<String, String> {
        match std::env::var(var) {
            Ok(value) => Ok(value),
            Err(std::env::VarError::NotPresent) => Err(format!(
                "a required LDAP environment variable {var} is not configured, \
                    while some other LDAP environment variable is set."
            )),
            Err(std::env::VarError::NotUnicode(_)) => Err(format!(
                "LDAP environment variable {var} is set, but it's not a valid UTF-8 string."
            )),
        }
    }

    /// Get an environment variables as a boolean. Treats missing values and values
    ///  other than non-case-sensitive `"true"` as false.
    pub fn get_bool(var: &str) -> bool {
        std::env::var(var).is_ok_and(|value| value.eq_ignore_ascii_case("true"))
    }
}

#[derive(
    Clone, Debug, Default, Eq, PartialEq, Introspection, serde::Deserialize, serde::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct LdapSection {
    /// Whether LDAP is enabled. If false, authentications using the LDAP method
    /// will unconditionally fail.
    #[introspection(config_default = false)]
    pub enabled: Option<bool>,

    /// Format string that defines how should a picodata username be converted
    /// to an LDAP Distinguished Name (DN).
    pub dn_format: Option<LdapDnFormatString>,

    /// Address of the LDAP server to connect to.
    pub connect: Option<LdapAddress>,

    /// TLS configuration for LDAP.
    #[serde(default)]
    #[introspection(nested)]
    pub tls: TlsClientSettings,
}

impl LdapSection {
    /// Check the environment variables of the current process for presence of legacy
    /// picodata tarantool LDAP configuration environment variables, and return
    /// the list of such found variables.
    ///
    /// This can be used for deciding whether to use those variables as configuration
    /// source or not.
    ///
    /// The checked environment variables are:
    ///  - `TT_LDAP_URL`
    ///  - `TT_LDAP_DN_FMT`
    ///  - `TT_LDAP_ENABLE_TLS`
    pub fn check_for_legacy_env() -> Option<Vec<String>> {
        let mut result = Vec::new();

        for variable in ["TT_LDAP_URL", "TT_LDAP_DN_FMT", "TT_LDAP_ENABLE_TLS"] {
            if std::env::var_os(variable).is_some() {
                result.push(variable.to_string())
            }
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    /// Read legacy picodata tarantool LDAP configuration environment variables,
    /// and derive an equivalent runtime configuration from them.
    ///
    /// The function will read the following environment variables are:
    ///  - `TT_LDAP_URL`
    ///  - `TT_LDAP_DN_FMT`
    ///  - `TT_LDAP_ENABLE_TLS`
    pub fn migrate_from_legacy_env() -> Result<Self, String> {
        let url = env::get_str("TT_LDAP_URL")?;
        let dn_fmt = env::get_str("TT_LDAP_DN_FMT")?;
        let start_tls_enabled = env::get_bool("TT_LDAP_ENABLE_TLS");

        let url = url::Url::parse(&url).map_err(|e| format!("Could not parse TT_LDAP_URL: {e}"))?;

        enum Scheme {
            Ldap,
            Ldaps,
        }

        let scheme = url.scheme();
        let scheme = match scheme {
            "ldap" => Scheme::Ldap,
            "ldaps" => Scheme::Ldaps,
            _ => return Err("TTL_LDAP_URL scheme must be `ldap` or `ldaps`".to_string()),
        };
        if url.path() != "" && url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err("TTL_LDAP_URL must not have a path, query or a fragment".to_string());
        }
        let Some(host_str) = url.host_str() else {
            return Err("TTL_LDAP_URL must have a host".to_string());
        };
        let port = url.port().unwrap_or(match scheme {
            Scheme::Ldap => 389,
            Scheme::Ldaps => 636,
        });
        let address = LdapAddress {
            host: host_str.to_string(),
            port: port.to_string(),
        };

        let dn_format = LdapDnFormatString::parse(&dn_fmt)
            .map_err(|e| format!("TT_LDAP_DN_FMT is invalid: {}", e))?;

        let tls = match (scheme, start_tls_enabled) {
            (Scheme::Ldap, false) => TlsClientSettings {
                enabled: Some(false),
                method: None,
                ca_file: None,
            },
            (Scheme::Ldaps, false) => TlsClientSettings {
                enabled: Some(true),
                method: Some(TlsClientMethod::Implicit),
                ca_file: None,
            },
            (Scheme::Ldap, true) => TlsClientSettings {
                enabled: Some(true),
                method: Some(TlsClientMethod::StartTls),
                ca_file: None,
            },
            (Scheme::Ldaps, true) => {
                return Err("TT_LDAP_ENABLE_TLS cannot be used with an ldaps:// URL".to_string());
            }
        };

        Ok(Self {
            enabled: Some(true),
            dn_format: Some(dn_format),
            connect: Some(address),
            tls,
        })
    }

    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    pub fn enabled(&self) -> bool {
        self.enabled
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }
}
