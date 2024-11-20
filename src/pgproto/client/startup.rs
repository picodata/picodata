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
    pub vtable_max_rows: Option<u64>,
    pub vdbe_max_steps: Option<u64>,
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

        let (mut vtable_max_rows, mut vdbe_max_steps) = (None, None);
        if let Some(options) = parameters.get("options") {
            for pair in options.split(',') {
                let mut pair = pair.split('=');
                let name = pair.next().ok_or(PgError::other("option with no name"))?;
                let val = pair.next().ok_or(PgError::other("option with no value"))?;
                match name {
                    "vtable_max_rows" => {
                        vtable_max_rows = Some(val.parse().map_err(PgError::other)?)
                    }
                    "vdbe_max_steps" => vdbe_max_steps = Some(val.parse().map_err(PgError::other)?),
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
            vtable_max_rows,
            vdbe_max_steps,
            _rest: parameters,
        })
    }

    pub fn execution_options(&self) -> Vec<OptionSpec> {
        let mut opts = vec![];

        if let Some(vdbe_max_steps) = self.vdbe_max_steps {
            let vdbe_max_steps = OptionParamValue::Value {
                val: SbroadValue::Unsigned(vdbe_max_steps),
            };
            opts.push(OptionSpec {
                kind: OptionKind::VdbeMaxSteps,
                val: vdbe_max_steps,
            })
        }

        if let Some(vtable_max_rows) = self.vtable_max_rows {
            let vtable_max_rows = OptionParamValue::Value {
                val: SbroadValue::Unsigned(vtable_max_rows),
            };
            opts.push(OptionSpec {
                kind: OptionKind::VTableMaxRows,
                val: vtable_max_rows,
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
