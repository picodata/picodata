use crate::pgproto::backend::copy::{CopySession, CopyStart};
use crate::pgproto::error::{PgError, PgResult};
use crate::pgproto::messages;
use crate::pgproto::stream::{FeMessage, PgStream};
use smol_str::format_smolstr;
use std::io::{Read, Write};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CopyInMode {
    SimpleQuery,
    ExtendedQuery,
}

pub enum CopyInMessageOutcome {
    Continue,
    Done { inserted_rows: usize },
    ClientFailed { reason: String },
    Terminate,
}

pub struct ActiveCopyIn {
    mode: CopyInMode,
    session: Option<Box<CopySession>>,
}

impl ActiveCopyIn {
    pub fn new(mode: CopyInMode, session: Box<CopySession>) -> Self {
        Self {
            mode,
            session: Some(session),
        }
    }

    pub fn mode(&self) -> CopyInMode {
        self.mode
    }

    fn session_mut(&mut self) -> PgResult<&mut CopySession> {
        self.session
            .as_deref_mut()
            .ok_or_else(|| PgError::ProtocolViolation(format_smolstr!("COPY session is missing")))
    }
}

pub fn is_copy_in_message(message: &FeMessage) -> bool {
    matches!(
        message,
        FeMessage::CopyData(_)
            | FeMessage::CopyDone(_)
            | FeMessage::CopyFail(_)
            | FeMessage::Flush(_)
            | FeMessage::Sync(_)
    )
}

pub fn send_copy_in_response(
    stream: &mut PgStream<impl Read + Write>,
    start: &CopyStart,
) -> PgResult<()> {
    stream.write_message(messages::copy_in_response_text(start.column_count)?)?;
    Ok(())
}

pub fn process_copy_in_message(
    active_copy: &mut ActiveCopyIn,
    message: FeMessage,
) -> PgResult<CopyInMessageOutcome> {
    match message {
        FeMessage::CopyData(copy_data) => {
            active_copy.session_mut()?.on_copy_data(copy_data.data)?;
            Ok(CopyInMessageOutcome::Continue)
        }
        FeMessage::CopyDone(_) => {
            let inserted_rows = active_copy
                .session
                .take()
                .ok_or_else(|| {
                    PgError::ProtocolViolation(format_smolstr!("COPY session is missing"))
                })?
                .on_copy_done()?;
            Ok(CopyInMessageOutcome::Done { inserted_rows })
        }
        FeMessage::CopyFail(copy_fail) => Ok(CopyInMessageOutcome::ClientFailed {
            reason: copy_fail.message,
        }),
        FeMessage::Flush(_) | FeMessage::Sync(_) => Ok(CopyInMessageOutcome::Continue),
        FeMessage::Terminate(_) => Ok(CopyInMessageOutcome::Terminate),
        other => Err(PgError::ProtocolViolation(format_smolstr!(
            "unexpected frontend message during COPY FROM STDIN: {other:?}"
        ))),
    }
}
