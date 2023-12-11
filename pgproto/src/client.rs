use crate::client::simple_query::process_query_message;
use crate::error::*;
use crate::messages;
use crate::storage::StorageManager;
use crate::stream::{BeMessage, FeMessage, PgStream};
use pgwire::messages::startup::*;
use std::io;

mod auth;
mod simple_query;
mod startup;

pub type ClientId = u32;

/// Postgres client representation.
pub struct PgClient<S> {
    // The portal and statement storage manager.
    manager: StorageManager,
    /// Stream for network communication.
    stream: PgStream<S>,
}

impl<S: io::Read + io::Write> PgClient<S> {
    /// Create a client context by receiving a startup message and authenticating the client.
    pub fn accept(mut stream: PgStream<S>) -> PgResult<PgClient<S>> {
        let mut startup_sequence = || {
            let client_params = startup::handshake(&mut stream)?;
            log::info!("processed startup message");

            let salt = rand::random::<[u8; 20]>();
            auth::authenticate(&mut stream, &salt, &client_params.username)?;
            log::info!("client authenticated");

            PgResult::Ok(())
        };

        startup_sequence().map_err(|error| {
            log::info!("failed to establish client connection: {error}");
            // At this point we don't care about failed writes (best effort approach).
            let _ = stream.write_message(messages::error_response(error.info()));
            error
        })?;

        Ok(PgClient {
            manager: StorageManager::new(),
            stream,
        })
    }

    /// Send paraneter to the frontend.
    pub fn send_parameter(
        &mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> PgResult<&mut Self> {
        let (name, value) = (name.into(), value.into());
        let msg = BeMessage::ParameterStatus(ParameterStatus::new(name, value));
        self.stream.write_message_noflush(msg)?;
        Ok(self)
    }
}

#[derive(PartialEq)]
enum ConnectionState {
    ReadyForQuery,
    Terminated,
}

impl PgError {
    /// Check wether we should break the message handling loop.
    fn check_fatality(self) -> PgResult<()> {
        match self {
            // TODO: consider the error type.
            PgError::PgWireError(_) => Err(self),
            _otherwise => Ok(()),
        }
    }
}

impl<S: io::Read + io::Write> PgClient<S> {
    /// Receive a single message, process it, then send a proper response.
    fn process_message(&mut self) -> PgResult<ConnectionState> {
        let message = self.stream.read_message()?;
        log::debug!("received {message:?}");
        match message {
            FeMessage::Query(query) => {
                log::info!("executing query");
                process_query_message(&mut self.stream, &self.manager, query)?;
                Ok(ConnectionState::ReadyForQuery)
            }
            FeMessage::Terminate(_) => {
                log::info!("terminating the session");
                Ok(ConnectionState::Terminated)
            }
            message => Err(PgError::FeatureNotSupported(format!("{message:?}"))),
        }
    }

    /// Process incoming client messages until we see an irrecoverable error.
    pub fn process_messages_loop(&mut self) -> PgResult<()> {
        log::info!("entering the message handling loop");
        loop {
            self.stream.write_message(messages::ready_for_query())?;
            match self.process_message() {
                Ok(ConnectionState::ReadyForQuery) => continue,
                Ok(ConnectionState::Terminated) => break Ok(()),
                Err(error) => {
                    self.stream
                        .write_message(messages::error_response(error.info()))?;
                    error.check_fatality()?;
                }
            }
        }
    }
}
