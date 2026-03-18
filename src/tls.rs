//! This module provides functions for loading and validating picodata TLS configuration.

use crate::tlog;
use openssl::pkey::PKey;
use openssl::x509::{X509NameEntries, X509};
use picodata_plugin::transport::listener::LoadedListenerTlsConfig;
use std::fmt;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////
// Certificate formatting helpers
////////////////////////////////////////////////////////////////////////////////

fn x509_name_entries_to_string(
    entries: X509NameEntries,
) -> Result<String, openssl::error::ErrorStack> {
    let mut s = vec![];

    for entry in entries {
        s.push(format!(
            "{key} = {val:?}",
            key = entry.object(),
            val = entry.data().as_utf8()?,
        ));
    }

    Ok(s.join(", "))
}

fn cert_to_string(cert: &X509) -> Result<String, openssl::error::ErrorStack> {
    let issuer = x509_name_entries_to_string(cert.issuer_name().entries())?;
    let subject = x509_name_entries_to_string(cert.subject_name().entries())?;
    let serial = cert.serial_number().to_bn()?.to_hex_str()?;

    let not_before = cert.not_before().to_string();
    let not_after = cert.not_after().to_string();

    let s = format!(
        "\
        \n    Serial number: {serial}\
        \n    Issuer:  {issuer}\
        \n    Subject: {subject}\
        \n    Validity:\
        \n        Not before: {not_before}\
        \n        Not after:  {not_after}\
        "
    );

    Ok(s)
}

fn certs_to_string(certs: &[X509]) -> Result<String, openssl::error::ErrorStack> {
    let mut s = String::new();

    for cert in certs {
        use std::fmt::Write;
        writeln!(&mut s, "{}", cert_to_string(cert)?).unwrap();
    }

    Ok(s)
}

#[derive(Copy, Clone)]
pub enum TlsConfigurationSource<'a> {
    Iproto,
    Pgproto,
    Http,
    Plugin { plugin: &'a str, service: &'a str },
}

impl<'a> fmt::Display for TlsConfigurationSource<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TlsConfigurationSource::Iproto => write!(f, "iproto"),
            TlsConfigurationSource::Pgproto => write!(f, "pgproto"),
            TlsConfigurationSource::Http => write!(f, "http"),
            TlsConfigurationSource::Plugin { plugin, service } => {
                write!(f, "plugin {plugin}.{service}")
            }
        }
    }
}

fn log_cert_info(
    source: &TlsConfigurationSource,
    config: &LoadedListenerTlsConfig,
) -> Result<(), openssl::error::ErrorStack> {
    tlog!(
        Info,
        "TLS({source}): server certificate chain: {}",
        certs_to_string(&config.cert_chain)?
    );
    if let Some(mtls_ca_chain) = &config.mtls_ca_chain {
        tlog!(
            Info,
            "TLS({source}): CA certificate chain is configured, mTLS will be enforced"
        );

        tlog!(
            Info,
            "TLS({source}): mTLS CA certificate chain: {}",
            certs_to_string(mtls_ca_chain)?
        );
    } else {
        tlog!(
            Info,
            "TLS({source}): CA certificate chain is not configured, mTLS will be disabled"
        );
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// TLS configuration loading
////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum TlsConfigLoadError {
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("validation failed: {0}")]
    Validation(String),

    #[error("could not read cert file: {0}")]
    ReadCert(std::io::Error),
    #[error("could not load cert file: {0}")]
    LoadCert(openssl::error::ErrorStack),

    #[error("could not read key file: {0}")]
    ReadKey(std::io::Error),
    #[error("could not read key password file: {0}")]
    ReadPassword(std::io::Error),
    #[error("could not load key file: {0}")]
    LoadKey(openssl::error::ErrorStack),

    #[error("could not read CA cert file: {0}")]
    ReadCa(std::io::Error),
    #[error("could not load CA cert file: {0}")]
    LoadCa(openssl::error::ErrorStack),

    #[error("openssl error while validating the server certificate: {0}")]
    ValidatingCert(openssl::error::ErrorStack),
    #[error("openssl error while logging server certificates: {0}")]
    LoggingCerts(openssl::error::ErrorStack),
}

fn validate_config(config: &LoadedListenerTlsConfig) -> Result<(), TlsConfigLoadError> {
    use TlsConfigLoadError::Validation;

    // 1. The server certificate chain is non-empty.
    let [server_cert, ..] = config.cert_chain.as_slice() else {
        return Err(Validation(
            "The provided server certificate chain is empty".to_string(),
        ));
    };
    let server_public_key = server_cert
        .public_key()
        .map_err(TlsConfigLoadError::ValidatingCert)?;

    // 2. The provided private key matches the server certificate.
    if !config.key.public_eq(&server_public_key) {
        return Err(Validation(
            "The provided private key does not match the public key of the server certificate"
                .to_string(),
        ));
    }

    // 3. The CA certificate chain (if provided) is non-empty.
    if let Some(mtls_ca_chain) = &config.mtls_ca_chain {
        if mtls_ca_chain.is_empty() {
            return Err(Validation(
                "The provided mTLS CA certificate chain is empty".to_string(),
            ));
        }
    }

    Ok(())
}

/// Reads certificate files specified by the [`crate::config::TlsSettings`] into memory.
///
/// This function will also attempt to validate the provided files:
/// 1. The server certificate chain is non-empty.
/// 2. The provided private key matches the server certificate.
/// 3. The CA certificate chain (if provided) is non-empty.
///
/// The `source` parameter is used for logging and should specify the picodata subsystem for which
///  the TLS configuration is being loaded.
///
/// The `should_log` parameter specifies whether the function should log the paths it tries to access
///  and the loaded certificate chain. It exists because for some code paths (currently pgproto and
///  plugin listeners) we load the certificate twice: once during config validation and once during
///  actual listener creation. Having it logged twice is confusing, so we only do so when actually
///  creating the listener for those code paths.
///
/// The function will also log information about the loaded certificates for ease of debugging.
pub fn load_listener_tls_config_from_files(
    source: &TlsConfigurationSource,
    config: &crate::config::TlsSettings,
    allow_missing_ca: bool,
    should_log: bool,
) -> Result<Option<LoadedListenerTlsConfig>, TlsConfigLoadError> {
    use TlsConfigLoadError::InvalidConfiguration;

    macro_rules! log {
        ($($args:tt)*) => {
            if should_log {
                tlog!(Info, $($args)*);
            }
        };
    }

    if !config.enabled() {
        log!("TLS({source}): disabled");
        return Ok(None);
    }

    let cert_file = config.cert_file.as_ref().ok_or_else(|| {
        InvalidConfiguration("missing cert_file for enabled TLS in plugin listener".to_string())
    })?;
    log!("TLS({source}): loading certificate {}", cert_file.display());
    let cert_chain_pem = std::fs::read(cert_file).map_err(TlsConfigLoadError::ReadCert)?;
    let cert_chain = X509::stack_from_pem(&cert_chain_pem).map_err(TlsConfigLoadError::LoadCert)?;

    let key_file = config.key_file.as_ref().ok_or_else(|| {
        InvalidConfiguration("missing key_file for enabled TLS in plugin listener".to_string())
    })?;
    log!("TLS({source}): loading key {}", key_file.display());
    let key_pem = std::fs::read(key_file).map_err(TlsConfigLoadError::ReadKey)?;

    // try to decrypt the key PEM if password file is provided
    let key = if let Some(password_file) = &config.password_file {
        log!(
            "TLS({source}): reading key password {}",
            password_file.display()
        );

        // NB: we read the password as a string and then trim it
        // this prevents issues with trailing whitespace (like a newline at the end of the file),
        // but limits the password to use UTF-8 characters only
        let password =
            std::fs::read_to_string(password_file).map_err(TlsConfigLoadError::ReadPassword)?;
        let password = password.trim();

        PKey::private_key_from_pem_passphrase(&key_pem, password.as_bytes())
            .map_err(TlsConfigLoadError::LoadKey)?
    } else {
        PKey::private_key_from_pem(&key_pem).map_err(TlsConfigLoadError::LoadKey)?
    };

    let mtls_ca_chain_pem = if let Some(ca_file) = &config.ca_file {
        log!("TLS({source}): reading mTLS CA cert {}", ca_file.display());

        match std::fs::read(ca_file) {
            Ok(ca_pem) => Some(ca_pem),
            Err(e) if allow_missing_ca && e.kind() == std::io::ErrorKind::NotFound => {
                log!(
                    "TLS({source}): CA file {} is missing, mTLS will be disabled",
                    ca_file.display()
                );

                None
            }
            Err(e) => return Err(TlsConfigLoadError::ReadCa(e)),
        }
    } else {
        None
    };
    let mtls_ca_chain = if let Some(mtls_ca_chain_pem) = mtls_ca_chain_pem {
        Some(X509::stack_from_pem(&mtls_ca_chain_pem).map_err(TlsConfigLoadError::LoadCa)?)
    } else {
        None
    };

    let config = LoadedListenerTlsConfig {
        cert_chain,
        key,
        mtls_ca_chain,
    };

    // Log certificate information and validate
    if should_log {
        log_cert_info(source, &config).map_err(TlsConfigLoadError::LoggingCerts)?;
    }
    validate_config(&config)?;

    Ok(Some(config))
}
