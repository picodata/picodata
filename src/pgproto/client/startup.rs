use crate::pgproto::error::{PgError, PgResult};
use crate::pgproto::messages;
use crate::pgproto::stream::{FeMessage, PgStream};
use crate::pgproto::tls::TlsAcceptor;
use crate::tlog;
use pgwire::messages::startup::Startup;
use sbroad::ir::value::Value as SbroadValue;
use sbroad::ir::{OptionKind, OptionParamValue, OptionSpec};
use std::collections::BTreeMap;
use std::io::{Read, Write};

#[derive(Clone, Debug)]
pub struct ClientParams {
    pub username: String,
    pub sql_motion_row_max: Option<u64>,
    pub sql_vdbe_opcode_max: Option<u64>,
    pub _rest: BTreeMap<String, String>,
    // NB: add more params as needed.
    // Keep in mind that a client is required to send only "user".
}

impl ClientParams {
    fn new(mut parameters: BTreeMap<String, String>) -> PgResult<Self> {
        let Some(username) = parameters.remove("user") else {
            return Err(PgError::ProtocolViolation(
                "parameter 'user' is missing".into(),
            ));
        };

        let (mut sql_motion_row_max, mut sql_vdbe_opcode_max) = (None, None);
        if let Some(options) = parameters.get("options") {
            for pair in options.split(',') {
                let mut pair = pair.split('=');
                let name = pair.next().ok_or(PgError::other("option with no name"))?;
                let val = pair.next().ok_or(PgError::other("option with no value"))?;
                match name {
                    "sql_motion_row_max" => {
                        sql_motion_row_max = Some(val.parse().map_err(PgError::other)?)
                    }
                    "sql_vdbe_opcode_max" => {
                        sql_vdbe_opcode_max = Some(val.parse().map_err(PgError::other)?)
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
            sql_motion_row_max,
            sql_vdbe_opcode_max,
            _rest: parameters,
        })
    }

    pub fn execution_options(&self) -> Vec<OptionSpec> {
        let mut opts = vec![];

        if let Some(sql_vdbe_opcode_max) = self.sql_vdbe_opcode_max {
            let sql_vdbe_opcode_max = OptionParamValue::Value {
                val: SbroadValue::Unsigned(sql_vdbe_opcode_max),
            };
            opts.push(OptionSpec {
                kind: OptionKind::VdbeOpcodeMax,
                val: sql_vdbe_opcode_max,
            })
        }

        if let Some(sql_motion_row_max) = self.sql_motion_row_max {
            let sql_motion_row_max = OptionParamValue::Value {
                val: SbroadValue::Unsigned(sql_motion_row_max),
            };
            opts.push(OptionSpec {
                kind: OptionKind::MotionRowMax,
                val: sql_motion_row_max,
            })
        }

        opts
    }
}

fn parse_startup(startup: Startup) -> PgResult<ClientParams> {
    tlog!(Debug, "client parameters: {:?}", &startup.parameters);
    ClientParams::new(startup.parameters)
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
            FeMessage::SslRequest(_) => {
                if client_attempted_ssl {
                    // ssl handshake was already attempted
                    return Err(PgError::ProtocolViolation(format!(
                        "expected Startup, got {message:?}"
                    )));
                } else {
                    stream = handle_ssl_request(stream, tls_acceptor)?;
                    client_attempted_ssl = true;
                    waiting_for_ssl = false;
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
