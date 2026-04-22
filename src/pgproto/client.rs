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
mod copy_in;
mod extended_query;
mod simple_query;
mod startup;

pub use startup::ClientParams;

/// We generate those sequentially for every client connection.
pub type ClientId = u64;

enum MessageExecutionOutcome {
    Completed,
    EnterCopyIn(copy_in::ActiveCopyIn),
}

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

enum MessageLoopState {
    ReadyForQuery,
    CopyIn(copy_in::ActiveCopyIn),
    DrainingCopyError,
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
    fn enter_copy_in(&mut self, active_copy: copy_in::ActiveCopyIn) {
        self.loop_state = MessageLoopState::CopyIn(active_copy);
    }

    fn finish_copy_in(&mut self, mode: copy_in::CopyInMode) {
        self.loop_state = match mode {
            copy_in::CopyInMode::ExtendedQuery => MessageLoopState::RunningExtendedQuery,
            copy_in::CopyInMode::SimpleQuery => MessageLoopState::ReadyForQuery,
        };
    }

    fn take_copy_in_mode(&mut self) -> Option<copy_in::CopyInMode> {
        let state = std::mem::replace(&mut self.loop_state, MessageLoopState::ReadyForQuery);
        match state {
            MessageLoopState::CopyIn(active_copy) => Some(active_copy.mode()),
            other => {
                self.loop_state = other;
                None
            }
        }
    }

    fn sync_extended_query(&mut self) -> PgResult<()> {
        loop {
            match self.stream.read_message()? {
                FeMessage::Sync(_) => {
                    self.loop_state = MessageLoopState::ReadyForQuery;
                    extended_query::process_sync_message(&self.backend);
                    return Ok(());
                }
                FeMessage::Terminate(_) => {
                    tlog!(Info, "terminating the session while waiting for Sync");
                    self.loop_state = MessageLoopState::Terminated;
                    return Ok(());
                }
                _ => {}
            }
        }
    }

    fn process_regular_message(&mut self, message: FeMessage) -> PgResult<()> {
        match message {
            FeMessage::Query(query) => {
                tlog!(Debug, "executing simple query: {}", query.query);
                match simple_query::process_query_message(&mut self.stream, &self.backend, query)? {
                    MessageExecutionOutcome::Completed => {
                        self.loop_state = MessageLoopState::ReadyForQuery;
                    }
                    MessageExecutionOutcome::EnterCopyIn(active_copy) => {
                        self.enter_copy_in(active_copy)
                    }
                }
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
                match extended_query::process_execute_message(
                    &mut self.stream,
                    &self.backend,
                    execute,
                )? {
                    MessageExecutionOutcome::Completed => {}
                    MessageExecutionOutcome::EnterCopyIn(active_copy) => {
                        self.enter_copy_in(active_copy)
                    }
                }
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
                extended_query::process_sync_message(&self.backend);
            }
            FeMessage::Terminate(_) => {
                tlog!(Info, "terminating the session");
                self.loop_state = MessageLoopState::Terminated;
            }
            FeMessage::CopyData(_) => {
                return Err(PgError::ProtocolViolation(format_smolstr!(
                    "unexpected frontend CopyData message outside COPY FROM STDIN"
                )));
            }
            FeMessage::CopyDone(_) => {
                return Err(PgError::ProtocolViolation(format_smolstr!(
                    "unexpected frontend CopyDone message outside COPY FROM STDIN"
                )));
            }
            FeMessage::CopyFail(_) => {
                return Err(PgError::ProtocolViolation(format_smolstr!(
                    "unexpected frontend CopyFail message outside COPY FROM STDIN"
                )));
            }
            message => return Err(PgError::FeatureNotSupported(format_smolstr!("{message:?}"))),
        };
        Ok(())
    }

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

        if self.is_running_copy_in()
            && !copy_in::is_copy_in_message(&message)
            && !matches!(message, FeMessage::Terminate(_))
        {
            tlog!(
                Warning,
                "got {message:?} message while running COPY FROM STDIN"
            );
        }

        if matches!(&self.loop_state, MessageLoopState::DrainingCopyError) {
            match message {
                FeMessage::Terminate(_) => {
                    self.loop_state = MessageLoopState::Terminated;
                }
                _ if copy_in::is_copy_in_message(&message) => {
                    tlog!(Debug, "dropping stale {message:?} after COPY error");
                }
                other => {
                    self.loop_state = MessageLoopState::ReadyForQuery;
                    self.process_regular_message(other)?;
                }
            }
            return Ok(());
        }

        if self.is_running_copy_in() {
            let (mode, outcome) = {
                let MessageLoopState::CopyIn(active_copy) = &mut self.loop_state else {
                    unreachable!("checked COPY state above");
                };
                let mode = active_copy.mode();
                let outcome = copy_in::process_copy_in_message(active_copy, message)?;
                (mode, outcome)
            };

            match outcome {
                copy_in::CopyInMessageOutcome::Continue => {}
                copy_in::CopyInMessageOutcome::Done { inserted_rows } => {
                    self.finish_copy_in(mode);
                    self.stream
                        .write_message(messages::copy_command_complete(inserted_rows))?;
                }
                copy_in::CopyInMessageOutcome::ClientFailed { reason } => {
                    return Err(PgError::other(format!("COPY from stdin failed: {reason}")));
                }
                copy_in::CopyInMessageOutcome::Terminate => {
                    self.loop_state = MessageLoopState::Terminated;
                }
            }
            return Ok(());
        }

        self.process_regular_message(message)
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
        if matches!(&self.loop_state, MessageLoopState::RunningExtendedQuery) {
            self.sync_extended_query()?;
        } else if let Some(mode) = self.take_copy_in_mode() {
            match mode {
                copy_in::CopyInMode::SimpleQuery => {
                    self.stream.write_message(messages::ready_for_query())?;
                    self.loop_state = MessageLoopState::DrainingCopyError;
                }
                copy_in::CopyInMode::ExtendedQuery => {
                    self.sync_extended_query()?;
                }
            }
        };

        Ok(())
    }

    fn is_terminated(&self) -> bool {
        matches!(&self.loop_state, MessageLoopState::Terminated)
    }

    fn is_running_extended_query(&self) -> bool {
        matches!(&self.loop_state, MessageLoopState::RunningExtendedQuery)
    }

    fn is_running_copy_in(&self) -> bool {
        matches!(&self.loop_state, MessageLoopState::CopyIn(_))
    }

    /// Process incoming client messages until we see an irrecoverable error.
    pub fn process_messages_loop(&mut self) -> PgResult<()> {
        tlog!(Info, "entering the message handling loop");
        while !self.is_terminated() {
            if matches!(&self.loop_state, MessageLoopState::ReadyForQuery) {
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
