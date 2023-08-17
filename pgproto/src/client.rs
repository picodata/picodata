use crate::auth::authenticate;
use crate::error::*;
use crate::helpers;
use crate::messages;
use crate::stream::{BeMessage, FeMessage, PgStream};
use pgwire::messages::startup::*;
use std::collections::BTreeMap;
use std::io;
use std::mem;

/// Postgres client states.
#[derive(PartialEq)]
enum PgClientState {
    /// Initial state, waiting for startup packet.
    Startup,
    /// At this state exchange of authentication messages is performed.
    Authentication,
    /// At this state the client is ready to start processing a query.
    ReadyForQuery,
    /// Client disconnected.
    Terminated,
}

/// Postgres client representation
pub struct PgClient<S> {
    /// Stream is used for network communication.
    stream: PgStream<S>,
    /// Client state.
    state: PgClientState,
    /// Username sent in startup message.
    username: Option<String>,
}

fn extract_password(message: PasswordMessageFamily) -> String {
    let mut password = message
        .into_password() // @todo: replace it with something that doesn't cause panic
        .unwrap_or(Password::new("".to_string()));
    mem::take(password.password_mut())
}

impl<S: io::Read + io::Write> PgClient<S> {
    /// Read startup message, verify parameters and return them.
    fn process_startup_message(&mut self) -> PgResult<BTreeMap<String, String>> {
        let message = self.stream.read_message()?;
        if let FeMessage::Startup(mut startup) = message {
            let parameters = startup.parameters_mut();
            if parameters.contains_key("user") {
                Ok(mem::take(parameters))
            } else {
                Err(PgError::ProtocolViolation(
                    "parameter 'user' is missed".into(),
                ))
            }
        } else {
            Err(PgError::ProtocolViolation(format!(
                "expected Startup, got {message:?}"
            )))
        }
    }

    /// Process the startup phase.
    fn process_startup(&mut self) -> PgResult<()> {
        assert!(self.state == PgClientState::Startup);
        let mut parameters = self.process_startup_message()?;
        log::debug!("client parameters: {parameters:?}");
        let username = parameters.get_mut("user").unwrap();
        self.username = Some(mem::take(username));
        self.state = PgClientState::Authentication;
        Ok(())
    }

    /// Request a password, receive the response, extract the password and return it.
    fn auth_exchage(&mut self, salt: &[u8; 20]) -> PgResult<String> {
        // MD5 method requires only 4 bytes for salt.
        let salt = helpers::split_at_const::<4>(salt).unwrap().0;
        self.stream
            .write_message(messages::md5_auth_request(salt))?;

        let message = self.stream.read_message()?;
        if let FeMessage::PasswordMessageFamily(message) = message {
            let password = extract_password(message);
            Ok(password)
        } else {
            Err(PgError::ProtocolViolation(format!(
                "expected Password, got {message:?}"
            )))
        }
    }

    /// Perform exchange of authentication messages and authentication.
    /// Authentication failure is treated as an error.
    fn authenticate(&mut self) -> PgResult<()> {
        assert!(self.state == PgClientState::Authentication);
        // method authenticate expects that the salt len is not less than 20,
        // see box_process_auth from tarantool/src/box.cc.
        let salt = rand::random::<[u8; 20]>();
        let password = self.auth_exchage(&salt)?;

        let user = self.username.as_ref().expect("user must be already set");
        if authenticate(user, &salt, &password) {
            self.stream.write_message_noflush(messages::auth_ok())?;
            self.state = PgClientState::ReadyForQuery;
            Ok(())
        } else {
            Err(PgError::InvalidPassword(user.to_owned()))
        }
    }

    /// Send error to the client.
    /// Returns the passed error if it cannot be handled, otherwise Ok is returned.
    fn process_error(&mut self, error: PgError) -> PgResult<()> {
        log::debug!("processing error: {error}");
        self.stream
            .write_message(messages::error_response(error_info(&error)))?;

        match error {
            // We have nothing to do if authentication failed.
            PgError::InvalidPassword(_) => Err(error),
            // TODO: consider the error type.
            PgError::PgWireError(_) => Err(error),
            _otherwise => Ok(()),
        }
    }

    /// Create a postgres client.
    /// Startup prcessing and authentication is performed, which may lead to errors.
    pub fn new(stream: PgStream<S>) -> PgResult<PgClient<S>> {
        let mut client = PgClient {
            stream,
            state: PgClientState::Startup,
            username: None,
        };

        let mut startup_sequence = || {
            client.process_startup()?;
            log::info!("processed startup");
            client.authenticate()?;
            log::info!("authenticated");
            PgResult::Ok(())
        };

        if let Err(error) = startup_sequence() {
            client.process_error(error)?;
        }

        Ok(client)
    }

    /// Send paraneter to the frontend.
    pub fn send_parameter(&mut self, name: &str, value: &str) -> PgResult<&mut Self> {
        self.stream
            .write_message_noflush(BeMessage::ParameterStatus(ParameterStatus::new(
                String::from(name),
                String::from(value),
            )))?;
        Ok(self)
    }

    /// Ask a query from the frontend, process it and send response with command complete.
    /// In case of error the error is returned, thus, error handling is layed to the caller.
    fn process_query(&mut self) -> PgResult<()> {
        assert!(self.state == PgClientState::ReadyForQuery);
        self.stream.write_message(messages::ready_for_query())?;
        let message = self.stream.read_message()?;
        log::debug!("read {message:?}");
        match message {
            FeMessage::Terminate(_) => {
                log::info!("terminating");
                self.state = PgClientState::Terminated;
                Ok(())
            }
            message => Err(PgError::FeatureNotSupported(format!("message {message:?}"))),
        }
    }

    /// Start processing incoming queries.
    pub fn start_query_cycle(&mut self) -> PgResult<()> {
        log::info!("starting query cycle");

        while self.state == PgClientState::ReadyForQuery {
            if let Err(error) = self.process_query() {
                self.process_error(error)?;
            }
        }
        Ok(())
    }
}
