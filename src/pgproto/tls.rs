use crate::config::TlsSettings;
use crate::tlog;
use crate::traft::error::Error;
use openssl::ssl::SslStream;
use std::path::Path;

/// If some TLS paths are not configured explicitly, fall back to fixed filenames in the instance directory. NB: this is only done by pgproto.
fn apply_pgproto_tls_default_paths(instance_dir: &Path, tls_settings: TlsSettings) -> TlsSettings {
    let TlsSettings {
        enabled,
        cert_file,
        key_file,
        ca_file,
        password_file,
    } = tls_settings;

    // We should use the absolute paths here, because SslContextBuilder::set_certificate_chain_file
    // fails for relative paths with an unclear error, represented as an empty error stack.
    let cert_file = match cert_file {
        Some(cert_file) => cert_file.to_path_buf(),
        None => instance_dir.join("server.crt"),
    };

    let key_file = match key_file {
        Some(key_file) => key_file.to_path_buf(),
        None => instance_dir.join("server.key"),
    };

    let ca_file = match ca_file {
        Some(ca_file) => ca_file,
        None => instance_dir.join("ca.crt"),
    };

    TlsSettings {
        enabled,
        cert_file: Some(cert_file),
        key_file: Some(key_file),
        ca_file: Some(ca_file),
        password_file,
    }
}

/// Create TLS acceptor for pgproto from the provided configuration.
///
/// If TLS is disabled will return [`None`]
pub fn configure_tls_acceptor(
    instance_dir: &Path,
    config: &TlsSettings,
) -> Result<Option<TlsAcceptor>, Error> {
    let tls_config = apply_pgproto_tls_default_paths(instance_dir, config.clone());

    let loaded_tls_config = crate::tls::load_listener_tls_config_from_files(
        &crate::tls::TlsConfigurationSource::Pgproto,
        &tls_config,
        true,
    )
    .map_err(Error::invalid_configuration)?;

    let tls_acceptor = loaded_tls_config
        .map(|tls| TlsAcceptor::new(&tls))
        .transpose()
        .map_err(Error::invalid_configuration)?
        .inspect(|tls| tlog!(Info, "configured {} for pgproto", tls.kind()));

    Ok(tls_acceptor)
}

pub type TlsStream<S> = SslStream<S>;
pub type TlsAcceptor = picodata_plugin::transport::listener::TlsAcceptor;
