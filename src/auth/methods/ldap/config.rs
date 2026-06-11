use crate::address::LdapAddress;
use crate::config::TlsClientMethod;
use crate::tls::{ClientConfigLoadOptions, LoadedClientTlsConfig, TlsClientConfigurationSource};
use crate::{config, error_injection};
use std::fmt;

/// A format string that defines how picodata username should be converted
/// to an LDAP Distinguished Name (DN).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LdapDnFormatString {
    prefix: String,
    suffix: String,
}

impl LdapDnFormatString {
    /// Parse an LDAP DN format string.
    ///
    /// The only restriction is that the provided string has to contain one and only one
    /// occurrence of substring `$USER`.
    ///
    /// When formatted, `$USER` will be replaced with a `picodata` username
    /// to obtain an LDAP Distinguished Name (DN).
    pub fn parse(s: &str) -> Result<Self, &'static str> {
        const MAGIC: &str = "$USER";

        let Some((prefix, suffix)) = s.split_once(MAGIC) else {
            return Err("DN format string doesn't contain `$USER`");
        };

        if suffix.contains(MAGIC) {
            return Err("DN format string contains more than one `$USER`");
        }

        Ok(Self {
            prefix: prefix.to_string(),
            suffix: suffix.to_string(),
        })
    }

    /// Obtain an LDAP Distinguished Name (DN) by replacing `$USER` with
    /// a picodata username.
    pub fn format(&self, user: &str) -> String {
        format!("{}{}{}", self.prefix, user, self.suffix)
    }
}

impl fmt::Display for LdapDnFormatString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}$USER{}", self.prefix, self.suffix)
    }
}

impl serde::Serialize for LdapDnFormatString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for LdapDnFormatString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = LdapDnFormatString;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "LDAP DN format string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                LdapDnFormatString::parse(v).map_err(|e| E::custom(e))
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

#[derive(Debug)]
pub struct LdapRuntimeTlsConfiguration {
    pub method: TlsClientMethod,
    pub connector: native_tls::TlsConnector,
}

#[derive(Debug)]
pub struct LdapRuntimeConfiguration {
    /// Address of the LDAP server to connect to.
    pub connect: LdapAddress,
    /// Format string that defines how should a picodata username be converted
    /// to an LDAP Distinguished Name (DN).
    pub dn_format: LdapDnFormatString,
    /// TLS configuration for LDAP. If `None`, TLS will not be used.
    pub tls: Option<LdapRuntimeTlsConfiguration>,
}

impl LdapRuntimeConfiguration {
    /// Loads the runtime LDAP configuration picodata LDAP config section.
    ///
    /// Returns `None` if LDAP is disabled.
    ///
    /// Returns an error if:
    /// - LDAP is enabled, but `dn_format` or `connect` is not specified
    /// - the TLS config cannot be loaded
    pub fn from_config_section(
        config: &config::ldap::LdapSection,
        should_log: bool,
    ) -> Result<Option<Self>, String> {
        if !config.enabled() {
            return Ok(None);
        }

        let Some(dn_format) = &config.dn_format else {
            return Err("instance.ldap.dn_format is required when ldap is enabled".to_string());
        };
        let Some(connect) = &config.connect else {
            return Err("instance.ldap.connect is required when ldap is enabled".to_string());
        };

        let tls = crate::tls::load_client_tls_config_from_files(
            &TlsClientConfigurationSource::Ldap,
            &config.tls,
            ClientConfigLoadOptions {
                allow_starttls: true,
                should_log,
            },
        )
        .map_err(|e| e.to_string())?;

        let tls = match tls {
            Some(LoadedClientTlsConfig {
                method,
                alternative_trusted_root_cas,
            }) => {
                let mut connector = native_tls::TlsConnector::builder();

                // if requested, replace the root CAs with user-supplied ones
                if let Some(root_cas) = alternative_trusted_root_cas {
                    connector.disable_built_in_roots(true);
                    for ca in root_cas {
                        // Even though `native_tls` will be implemented using  `openssl`
                        // on all platforms that picodata supports, `native_tls` doesn't
                        // let us exploit this fact. Therefore, convert from `openssl::X509`
                        // to `native_tls::Certificate` via PEM serialization.
                        let ca_pem = ca
                            .to_pem()
                            .expect("Converting openssl cert to PEM should not fail");
                        let ca = native_tls::Certificate::from_pem(&ca_pem)
                            .expect("Parsing a valid PEM cert should not fail");
                        connector.add_root_certificate(ca);
                    }
                }

                error_injection!("LDAP_NO_TLS_VERIFICATION" => {
                    connector.danger_accept_invalid_certs(true);
                });

                Some(LdapRuntimeTlsConfiguration {
                    method,
                    connector: connector
                        .build()
                        .map_err(|e| format!("failed to build TLS connector: {e}"))?,
                })
            }
            None => None,
        };

        return Ok(Some(Self {
            connect: connect.clone(),
            dn_format: dn_format.clone(),
            tls,
        }));
    }
}
