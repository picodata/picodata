use super::{
    backend::Backend,
    error::{PgError, PgResult},
    messages,
    stream::{BeMessage, FeMessage, PgStream},
    tls::TlsAcceptor,
};
use crate::{storage::Catalog, tlog};
use pgwire::messages::startup::*;
use smol_str::format_smolstr;
use std::io;

mod auth;
mod extended_query;
mod simple_query;
mod startup;

pub use startup::ClientParams;

/// We generate those sequentially for every client connection.
pub type ClientId = u64;

/// Postgres client representation.
pub struct PgClient<S> {
    /// Postgres backend that handles queries.
    backend: Backend,
    /// Stream for network communication.
    stream: PgStream<S>,

    loop_state: MessageLoopState,
}

impl<S: io::Read + io::Write> PgClient<S> {
    /// Create a client context by receiving a startup message and authenticating the client.
    pub fn accept(
        stream: PgStream<S>,
        tls_acceptor: Option<TlsAcceptor>,
        storage: &Catalog,
    ) -> PgResult<PgClient<S>> {
        let (mut stream, params) = startup::handshake(stream, tls_acceptor.as_ref())?;
        tlog!(Info, "processed startup");

        auth::authenticate(&mut stream, &params.username, storage).map_err(|error| {
            tlog!(Info, "failed to establish client connection: {error}");
            // At this point we don't care about failed writes (best effort approach).
            let _ = stream.write_message(messages::error_response(error.info()));
            error
        })?;
        tlog!(Info, "client authenticated");

        Ok(PgClient {
            backend: Backend::new(params),
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
    /// Check whether we should break the message handling loop.
    fn check_fatality(self) -> PgResult<()> {
        match self {
            // From a standpoint of pgproto/stream.rs, certain severe protocol
            // violations (e.g. during the initial handshake) are represented as
            // IO errors as well. Previously, we'd propagate PgWriteError in
            // such cases, but that wasn't very useful.
            Self::IoError(_) => Err(self),

            // These are fatal, but cannot occur in the main message handling loop.
            // We list them here just in case somebody uses this function inappropriately.
            Self::SslRequired | Self::AuthError(_) => Err(self),

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
                tlog!(Debug, "executing simple query: {}", query.query);
                simple_query::process_query_message(&mut self.stream, &self.backend, query)?;
                self.loop_state = MessageLoopState::ReadyForQuery;
            }
            FeMessage::Parse(parse) => {
                tlog!(
                    Debug,
                    "parsing query \'{}\': {}",
                    parse.name.as_deref().unwrap_or_default(),
                    parse.query,
                );
                self.loop_state = MessageLoopState::RunningExtendedQuery;
                extended_query::process_parse_message(&mut self.stream, &self.backend, parse)?;
            }
            FeMessage::Bind(bind) => {
                tlog!(
                    Debug,
                    "binding statement \'{}\' to portal \'{}\'",
                    bind.statement_name.as_deref().unwrap_or_default(),
                    bind.portal_name.as_deref().unwrap_or_default()
                );
                self.loop_state = MessageLoopState::RunningExtendedQuery;
                extended_query::process_bind_message(&mut self.stream, &self.backend, bind)?;
            }
            FeMessage::Execute(execute) => {
                tlog!(
                    Debug,
                    "executing portal \'{}\'",
                    execute.name.as_deref().unwrap_or_default()
                );
                self.loop_state = MessageLoopState::RunningExtendedQuery;
                extended_query::process_execute_message(&mut self.stream, &self.backend, execute)?;
            }
            FeMessage::Describe(describe) => {
                tlog!(
                    Debug,
                    "describing {} \'{}\'",
                    describe.target_type,
                    describe.name.as_deref().unwrap_or_default()
                );
                self.loop_state = MessageLoopState::RunningExtendedQuery;
                extended_query::process_describe_message(
                    &mut self.stream,
                    &self.backend,
                    describe,
                )?;
            }
            FeMessage::Close(close) => {
                tlog!(
                    Debug,
                    "closing {} \'{}\'",
                    close.target_type,
                    close.name.as_deref().unwrap_or_default()
                );
                self.loop_state = MessageLoopState::RunningExtendedQuery;
                extended_query::process_close_message(&mut self.stream, &self.backend, close)?;
            }
            FeMessage::Flush(_) => {
                tlog!(Debug, "flushing");
                self.loop_state = MessageLoopState::RunningExtendedQuery;
                self.stream.flush()?;
            }
            FeMessage::Sync(_) => {
                tlog!(Debug, "syncing");
                self.loop_state = MessageLoopState::ReadyForQuery;
                extended_query::process_sync_mesage(&self.backend);
            }
            FeMessage::Terminate(_) => {
                tlog!(Info, "terminating the session");
                self.loop_state = MessageLoopState::Terminated;
            }
            message => return Err(PgError::FeatureNotSupported(format_smolstr!("{message:?}"))),
        };
        Ok(())
    }

    fn process_error(&mut self, error: PgError) -> PgResult<()> {
        tlog!(Debug, "processing error: {error:?}");

        // First and foremost, try sending the error to client.
        // True IO errors and stream-level protocol violations are treated the same;
        // Even so, we should give it a last try before terminating the connection.
        self.stream
            .write_message(messages::error_response(error.info()))?;

        // Now terminate the connection if it's a fatal error.
        error.check_fatality()?;

        // Otherwise, perform a pipeline synchronization.
        if let MessageLoopState::RunningExtendedQuery = self.loop_state {
            loop {
                if let FeMessage::Sync(_) = self.stream.read_message()? {
                    self.loop_state = MessageLoopState::ReadyForQuery;
                    extended_query::process_sync_mesage(&self.backend);
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
                Err(error) => self.process_error(error)?,
                Ok(_) => continue,
            };
        }
        Ok(())
    }
}
