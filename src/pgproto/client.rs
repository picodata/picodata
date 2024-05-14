use super::client::simple_query::process_query_message;
use super::error::*;
use super::messages;
use super::storage::StorageManager;
use super::stream::{BeMessage, FeMessage, PgStream};
use super::tls::TlsAcceptor;
use crate::tlog;
use pgwire::messages::startup::*;
use std::io;

mod auth;
mod extended_query;
mod simple_query;
mod startup;

pub type ClientId = u32;

/// Postgres client representation.
pub struct PgClient<S> {
    // The portal and statement storage manager.
    manager: StorageManager,
    /// Stream for network communication.
    stream: PgStream<S>,

    loop_state: MessageLoopState,
}

impl<S: io::Read + io::Write> PgClient<S> {
    /// Create a client context by receiving a startup message and authenticating the client.
    pub fn accept(stream: PgStream<S>, tls_acceptor: Option<TlsAcceptor>) -> PgResult<PgClient<S>> {
        let (mut stream, params) = startup::handshake(stream, tls_acceptor.as_ref())?;
        tlog!(Info, "processed startup");

        auth::authenticate(&mut stream, &params.username).map_err(|error| {
            tlog!(Info, "failed to establish client connection: {error}");
            // At this point we don't care about failed writes (best effort approach).
            let _ = stream.write_message(messages::error_response(error.info()));
            error
        })?;
        tlog!(Info, "client authenticated");

        Ok(PgClient {
            manager: StorageManager::new(),
            loop_state: MessageLoopState::ReadyForQuery,
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
enum MessageLoopState {
    ReadyForQuery,
    RunningExtendedQuery,
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
    fn process_message(&mut self) -> PgResult<()> {
        let message = self.stream.read_message()?;
        tlog!(Debug, "received {message:?}");

        if self.is_running_extended_query() && !extended_query::is_extended_query_message(&message)
        {
            // According to the protocol, the extended query is expected to be finished by getting
            // a Sync message, but the frontend can send a simple query message before Sync,
            // which will finish the pipeline. In that case Postgres just changes the state without any
            // errors or warnings. We can follow the Postgres way, but I think a warning might be helpful.
            //
            // See the discussion about getting a Query message while running extended query:
            // https://postgrespro.com/list/thread-id/2416958.
            tlog!(
                Warning,
                "got {message:?} message while running extended query"
            );
        }

        match message {
            FeMessage::Query(query) => {
                tlog!(Info, "executing simple query: {}", query.query);
                process_query_message(&mut self.stream, &self.manager, query)?;
                self.loop_state = MessageLoopState::ReadyForQuery;
            }
            FeMessage::Parse(parse) => {
                tlog!(
                    Info,
                    "parsing query \'{}\': {}",
                    parse.name.as_deref().unwrap_or_default(),
                    parse.query,
                );
                self.loop_state = MessageLoopState::RunningExtendedQuery;
                extended_query::process_parse_message(&mut self.stream, &self.manager, parse)?;
            }
            FeMessage::Bind(bind) => {
                tlog!(
                    Info,
                    "binding statement \'{}\' to portal \'{}\'",
                    bind.statement_name.as_deref().unwrap_or_default(),
                    bind.portal_name.as_deref().unwrap_or_default()
                );
                self.loop_state = MessageLoopState::RunningExtendedQuery;
                extended_query::process_bind_message(&mut self.stream, &self.manager, bind)?;
            }
            FeMessage::Execute(execute) => {
                tlog!(
                    Info,
                    "executing portal \'{}\'",
                    execute.name.as_deref().unwrap_or_default()
                );
                self.loop_state = MessageLoopState::RunningExtendedQuery;
                extended_query::process_execute_message(&mut self.stream, &self.manager, execute)?;
            }
            FeMessage::Describe(describe) => {
                tlog!(
                    Info,
                    "describing {} \'{}\'",
                    describe.target_type,
                    describe.name.as_deref().unwrap_or_default()
                );
                self.loop_state = MessageLoopState::RunningExtendedQuery;
                extended_query::process_describe_message(
                    &mut self.stream,
                    &self.manager,
                    describe,
                )?;
            }
            FeMessage::Close(close) => {
                tlog!(
                    Info,
                    "closing {} \'{}\'",
                    close.target_type,
                    close.name.as_deref().unwrap_or_default()
                );
                self.loop_state = MessageLoopState::RunningExtendedQuery;
                extended_query::process_close_message(&mut self.stream, &self.manager, close)?;
            }
            FeMessage::Flush(_) => {
                tlog!(Info, "flushing");
                self.loop_state = MessageLoopState::RunningExtendedQuery;
                self.stream.flush()?;
            }
            FeMessage::Sync(_) => {
                tlog!(Info, "syncing");
                self.loop_state = MessageLoopState::ReadyForQuery;
                extended_query::process_sync_mesage(&self.manager)?;
            }
            FeMessage::Terminate(_) => {
                tlog!(Info, "terminating the session");
                self.loop_state = MessageLoopState::Terminated;
            }
            message => return Err(PgError::FeatureNotSupported(format!("{message:?}"))),
        };
        Ok(())
    }

    fn process_error(&mut self, error: PgError) -> PgResult<()> {
        tlog!(Info, "processing error: {error:?}");
        self.stream
            .write_message(messages::error_response(error.info()))?;
        error.check_fatality()?;
        if let MessageLoopState::RunningExtendedQuery = self.loop_state {
            loop {
                if let FeMessage::Sync(_) = self.stream.read_message()? {
                    self.loop_state = MessageLoopState::ReadyForQuery;
                    extended_query::process_sync_mesage(&self.manager)?;
                    break;
                }
            }
        };
        Ok(())
    }

    fn is_terminated(&self) -> bool {
        matches!(self.loop_state, MessageLoopState::Terminated)
    }

    fn is_running_extended_query(&self) -> bool {
        matches!(self.loop_state, MessageLoopState::RunningExtendedQuery)
    }

    /// Process incoming client messages until we see an irrecoverable error.
    pub fn process_messages_loop(&mut self) -> PgResult<()> {
        tlog!(Info, "entering the message handling loop");
        while !self.is_terminated() {
            if let MessageLoopState::ReadyForQuery = self.loop_state {
                self.stream.write_message(messages::ready_for_query())?;
            }

            match self.process_message() {
                Ok(_) => continue,
                Err(error) => self.process_error(error)?,
            };
        }
        Ok(())
    }
}
