use pgwire::messages::startup::Startup;

use crate::error::{PgError, PgResult};
use crate::messages;
use crate::stream::{FeMessage, PgStream};
use std::{collections::BTreeMap, io};

pub struct ClientParams {
    pub username: String,
    pub rest: BTreeMap<String, String>,
}

fn parse_startup(mut startup: Startup) -> PgResult<ClientParams> {
    let mut parameters = std::mem::take(startup.parameters_mut());
    log::debug!("client parameters: {parameters:?}");

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
/// Respond to SslRequest if you receive it, read startup message, verify parameters and return them.
pub fn handshake(stream: &mut PgStream<impl io::Read + io::Write>) -> PgResult<ClientParams> {
    let mut expect_startup = false;
    loop {
        let message = stream.read_message()?;
        // At the beginning we can get SslRequest or Startup.
        match message {
            FeMessage::Startup(startup) => return parse_startup(startup),
            FeMessage::SslRequest(_) => {
                if expect_startup {
                    return Err(PgError::ProtocolViolation(format!(
                        "expected Startup, got {message:?}"
                    )));
                } else {
                    stream.write_message(messages::ssl_refuse())?;
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
