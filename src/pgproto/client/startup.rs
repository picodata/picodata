use crate::pgproto::error::{PgError, PgResult};
use crate::pgproto::messages;
use crate::pgproto::stream::{FeMessage, PgStream};
use crate::pgproto::tls::TlsAcceptor;
use crate::tlog;
use pgwire::messages::startup::Startup;
use std::collections::BTreeMap;
use std::io::{Read, Write};

pub struct ClientParams {
    pub username: String,
    pub rest: BTreeMap<String, String>,
}

fn parse_startup(startup: Startup) -> PgResult<ClientParams> {
    let mut parameters = startup.parameters;
    tlog!(Debug, "client parameters: {parameters:?}");

    let Some(username) = parameters.remove("user") else {
        return Err(PgError::ProtocolViolation(
            "parameter 'user' is missing".into(),
        ));
    };

    // NB: add more params as needed.
    // Keep in mind that a client is required to send only "user".
    Ok(ClientParams {
        username,
        rest: parameters,
    })
}

fn handle_ssl_request<S: Read + Write>(
    mut stream: PgStream<S>,
    tls_acceptor: Option<&TlsAcceptor>,
) -> PgResult<PgStream<S>> {
    let Some(acceptor) = tls_acceptor else {
        stream.write_message(messages::ssl_refuse())?;
        return Ok(stream);
    };

    stream.write_message(messages::ssl_accept())?;
    stream.into_secure(acceptor)
}

/// Respond to SslRequest if you receive it, read startup message, verify parameters and return them.
pub fn handshake<S: Read + Write>(
    mut stream: PgStream<S>,
    tls_acceptor: Option<&TlsAcceptor>,
) -> PgResult<(PgStream<S>, ClientParams)> {
    let mut expect_startup = false;
    loop {
        let message = stream.read_message()?;
        // At the beginning we can get SslRequest or Startup.
        match message {
            FeMessage::Startup(startup) => return Ok((stream, parse_startup(startup)?)),
            FeMessage::SslRequest(_) => {
                if expect_startup {
                    return Err(PgError::ProtocolViolation(format!(
                        "expected Startup, got {message:?}"
                    )));
                } else {
                    stream = handle_ssl_request(stream, tls_acceptor)?;
                    // After SslRequest, only Startup is expected.
                    expect_startup = true;
                }
            }
            _ => {
                return Err(PgError::ProtocolViolation(format!(
                    "expected Startup or SslRequest, got {message:?}"
                )))
            }
        }
    }
}
