use crate::error::{PgError, PgResult};
use crate::stream::{FeMessage, PgStream};
use std::{collections::BTreeMap, io};

pub struct ClientParams {
    pub username: String,
    pub rest: BTreeMap<String, String>,
}

/// Read startup message, verify parameters and return them.
pub fn handshake(stream: &mut PgStream<impl io::Read>) -> PgResult<ClientParams> {
    let message = stream.read_message()?;
    let FeMessage::Startup(mut startup) = message else {
        return Err(PgError::ProtocolViolation(format!(
            "expected Startup, got {message:?}"
        )));
    };

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
