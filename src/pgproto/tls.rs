use crate::tlog;
use openssl::ssl::{SslAcceptorBuilder, SslVerifyMode};
use openssl::x509::store::X509StoreBuilder;
use openssl::x509::{X509NameEntries, X509};
use openssl::{
    error::ErrorStack,
    ssl::Error as SslError,
    ssl::{self, HandshakeError, SslFiletype, SslMethod, SslStream},
};
use std::{fs, io, path::Path, path::PathBuf, rc::Rc};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TlsHandshakeError {
    #[error("setup failure: {0}")]
    SetupFailure(ErrorStack),

    #[error("handshake error: {0}")]
    Failure(SslError),
}

#[derive(Error, Debug)]
pub enum TlsConfigError {
    #[error("error stack: {0}")]
    ErrorStack(#[from] ErrorStack),

    #[error("cert file error '{0}': {1}")]
    CertFile(PathBuf, std::io::Error),

    #[error("key file error '{0}': {1}")]
    KeyFile(PathBuf, std::io::Error),

    #[error("ca file error '{0}': {1}")]
    CaFile(PathBuf, std::io::Error),
}

#[derive(Debug)]
pub struct TlsConfig {
    cert: PathBuf,
    key: PathBuf,
    // Optional CA certificate for peer certificates verification (mTLS).
    ca_cert: Option<PathBuf>,
}

impl TlsConfig {
    pub fn from_instance_dir(instance_dir: &Path) -> Result<Self, TlsConfigError> {
        // We should use the absolute paths here, because SslContextBuilder::set_certificate_chain_file
        // fails for relative paths with an unclear error, represented as an empty error stack.
        let cert = instance_dir.join("server.crt");
        let cert = fs::canonicalize(&cert).map_err(|e| TlsConfigError::CertFile(cert, e))?;

        let key = instance_dir.join("server.key");
        let key = fs::canonicalize(&key).map_err(|e| TlsConfigError::KeyFile(key, e))?;

        let ca_cert = instance_dir.join("ca.crt");
        let ca_cert = match fs::canonicalize(&ca_cert) {
            Ok(path) => Some(path),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => Err(TlsConfigError::CaFile(ca_cert, e))?,
        };

        // TODO: Make sure that the file permissions are set to 0640 or 0600.
        // See https://www.postgresql.org/docs/current/ssl-tcp.html#SSL-SETUP for details.
        Ok(Self { key, cert, ca_cert })
    }
}

pub type TlsStream<S> = SslStream<S>;

#[derive(Clone)]
pub struct TlsAcceptor(Rc<ssl::SslAcceptor>);

impl TlsAcceptor {
    pub fn new(config: &TlsConfig) -> Result<Self, TlsConfigError> {
        let mut builder = ssl::SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())?;

        Self::load_server_key(&mut builder, &config.key)
            .map_err(|e| TlsConfigError::KeyFile(config.key.clone(), e))?;

        Self::load_server_cert(&mut builder, &config.cert)
            .map_err(|e| TlsConfigError::CertFile(config.cert.clone(), e))?;

        if let Some(ca) = &config.ca_cert {
            Self::load_ca_cert(&mut builder, ca)
                .map_err(|e| TlsConfigError::CaFile(ca.clone(), e))?;
        }

        Ok(Self(builder.build().into()))
    }

    fn load_server_key(
        builder: &mut SslAcceptorBuilder,
        path: &PathBuf,
    ) -> Result<(), std::io::Error> {
        tlog!(Info, "TLS: found key: {}", path.display());
        builder.set_private_key_file(path, SslFiletype::PEM)?;

        Ok(())
    }

    fn load_server_cert(
        builder: &mut SslAcceptorBuilder,
        path: &PathBuf,
    ) -> Result<(), std::io::Error> {
        tlog!(Info, "TLS: found certificate: {}", path.display());
        builder.set_certificate_chain_file(path)?;

        let pem = fs::read(path)?;
        let certs = X509::stack_from_pem(&pem)?;
        tlog!(Info, "TLS: cert stack: {}", certs_to_string(&certs)?);

        Ok(())
    }

    fn load_ca_cert(
        builder: &mut SslAcceptorBuilder,
        path: &PathBuf,
    ) -> Result<(), std::io::Error> {
        tlog!(Info, "TLS: found CA certificate: {}", path.display());
        builder.set_verify_cert_store({
            let pem = fs::read(path)?;
            let certs = X509::stack_from_pem(&pem)?;
            tlog!(Info, "TLS: CA cert stack: {}", certs_to_string(&certs)?);

            let mut store_builder = X509StoreBuilder::new()?;
            for cert in certs {
                store_builder.add_cert(cert)?;
            }
            store_builder.build()
        })?;

        let mode = SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT;
        builder.set_verify(mode);

        Ok(())
    }

    pub fn new_from_dir(instance_dir: &Path) -> Result<Self, TlsConfigError> {
        let tls_config = TlsConfig::from_instance_dir(instance_dir)?;
        Self::new(&tls_config)
    }

    pub fn accept<S>(&self, stream: S) -> Result<TlsStream<S>, TlsHandshakeError>
    where
        S: io::Read + io::Write,
    {
        self.0.accept(stream).map_err(|e| match e {
            HandshakeError::SetupFailure(stack) => TlsHandshakeError::SetupFailure(stack),
            HandshakeError::Failure(e) | HandshakeError::WouldBlock(e) => {
                TlsHandshakeError::Failure(e.into_error())
            }
        })
    }

    pub fn kind(&self) -> &'static str {
        if self.0.context().verify_mode().contains(SslVerifyMode::PEER) {
            "mTLS (mutual TLS)"
        } else {
            "TLS"
        }
    }
}

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
