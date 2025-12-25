use crate::{
    pgproto::{
        error::{PgError, PgResult},
        messages,
        stream::{FeMessage, PgStream},
        tls::TlsAcceptor,
    },
    tlog,
};
use pgwire::messages::{startup::Startup, SslNegotiationMetaMessage};
use smol_str::format_smolstr;
use sql::ir::options::{PartialOptions, ReadPreference};
use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    str::FromStr,
};

#[derive(Clone, Debug)]
pub struct ClientParams {
    pub username: String,
    pub options: PartialOptions,
    pub _rest: BTreeMap<String, String>,
    // NB: add more params as needed.
    // Keep in mind that a client is required to send only "user".
}

impl ClientParams {
    fn new(mut parameters: BTreeMap<String, String>) -> PgResult<Self> {
        fn validate_value_for_option(raw_value: &str, option_name: &str) -> PgResult<i64> {
            let value = raw_value.parse().map_err(|error| {
                PgError::other(format!(
                    "value for option {option_name} is too large: {error}"
                ))
            })?;

            if value < 0 {
                return Err(PgError::other(format!(
                    "value for option {option_name} should be positive"
                )));
            }

            Ok(value)
        }

        let Some(username) = parameters.remove("user") else {
            return Err(PgError::ProtocolViolation(format_smolstr!(
                "parameter 'user' is missing"
            )));
        };

        let mut options_accumulator = PartialOptions::default();
        if let Some(options) = parameters.get("options") {
            for pair in options.split(',') {
                let mut pair = pair.split('=');
                let name = pair
                    .next()
                    .ok_or_else(|| PgError::other("option without name"))?;
                let val = pair
                    .next()
                    .ok_or_else(|| PgError::other("option without value"))?;
                match name {
                    option_name @ "sql_motion_row_max" => {
                        let value = validate_value_for_option(val, option_name)?;
                        options_accumulator.sql_motion_row_max = Some(value)
                    }
                    option_name @ "sql_vdbe_opcode_max" => {
                        let value = validate_value_for_option(val, option_name)?;
                        options_accumulator.sql_vdbe_opcode_max = Some(value)
                    }
                    "read_preference" => {
                        let value = ReadPreference::from_str(val).map_err(|_| {
                            PgError::other(format!(
                                "unknown read_preference value: '{val}', expected one of leader, replica, any"
                            ))
                        })?;
                        options_accumulator.read_preference = Some(value)
                    }
                    _ => {
                        // We prefer using warnings instead of errors for these reasons:
                        // 1) This is similar to how we handle unknown PostgreSQL parameters:
                        //    we just ignore them without causing errors.
                        // 2) Some clients might send unknown parameters, so throwing errors will
                        //    make it impossible to work with such clients. However, we're not
                        //    sure if any clients do this.
                        tlog!(Warning, "unknown option: '{name}'");
                    }
                }
            }
        }

        Ok(Self {
            username,
            options: options_accumulator,
            _rest: parameters,
        })
    }

    pub fn execution_options(&self) -> &PartialOptions {
        &self.options
    }
}

fn parse_startup(startup: Startup) -> PgResult<ClientParams> {
    tlog!(Debug, "client parameters: {:?}", &startup.parameters);
    ClientParams::new(startup.parameters)
}

fn handle_ssl_request<S: Read + Write>(
    mut stream: PgStream<S>,
    tls_acceptor: Option<&TlsAcceptor>,
) -> io::Result<PgStream<S>> {
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
    let mut waiting_for_ssl = tls_acceptor.is_some();
    let mut client_attempted_ssl = false;

    loop {
        let message = stream.read_message()?;
        // At the beginning we can get SslRequest or Startup.
        match message {
            FeMessage::Startup(startup) => {
                if waiting_for_ssl {
                    // ssl handshake is required (because the server has ssl set up), but wasn't performed
                    stream.write_message(messages::error_response(PgError::SslRequired.info()))?;
                    return Err(PgError::SslRequired);
                }

                return Ok((stream, parse_startup(startup)?));
            }
            FeMessage::SslNegotiation(SslNegotiationMetaMessage::PostgresSsl(_)) => {
                if client_attempted_ssl {
                    // ssl handshake was already attempted
                    return Err(PgError::ProtocolViolation(format_smolstr!(
                        "expected Startup, got {message:?}"
                    )));
                } else {
                    stream = handle_ssl_request(stream, tls_acceptor)?;
                    client_attempted_ssl = true;
                    waiting_for_ssl = false;
                }
            }
            _ => {
                return Err(PgError::ProtocolViolation(format_smolstr!(
                    "expected Startup or SslRequest, got {message:?}"
                )))
            }
        }
    }
}
