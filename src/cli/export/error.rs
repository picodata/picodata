use std::panic::Location;

use tokio_postgres as pg;
use tokio_postgres::error::DbError;

const EXIT_USAGE: i32 = 2;
const EXIT_FAILURE: i32 = 1;

#[derive(Debug)]
pub(super) struct ExportError {
    pub(super) kind: ExportErrorKind,
    pub(super) location: &'static Location<'static>,
}

impl std::fmt::Display for ExportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The location is only printed with `--verbose`.
        self.kind.fmt(formatter)
    }
}

impl std::error::Error for ExportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.kind.source()
    }
}

impl From<ExportErrorKind> for ExportError {
    #[track_caller]
    fn from(kind: ExportErrorKind) -> Self {
        Self {
            kind,
            location: Location::caller(),
        }
    }
}

impl From<std::io::Error> for ExportError {
    #[track_caller]
    fn from(source: std::io::Error) -> Self {
        Self {
            kind: ExportErrorKind::Write(source),
            location: Location::caller(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ExportErrorKind {
    #[error("failed parsing the connection string")]
    BadDsn(#[source] pg::Error),

    #[error(
        "no user name given: specify it either in the connection string \
         or with -U/--username"
    )]
    NoUser,

    #[error("`{parameter}` asks for TLS, which `picodata export` does not support yet")]
    TlsRequested { parameter: &'static str },

    #[error("failed connecting to the cluster")]
    Connect(#[source] pg::Error),

    #[error("failed reading the {subject} of the cluster")]
    Query {
        subject: &'static str,
        #[source]
        source: pg::Error,
    },

    #[error("failed decoding the `{column}` column of a system table")]
    Decode {
        column: &'static str,
        #[source]
        source: pg::Error,
    },

    #[error("failed writing the dump to {path}")]
    Output {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// The `Write` failed before its destination was known; `write_dump` turns it
    /// into `Output`, so this never reaches the user.
    #[error("failed writing the dump")]
    Write(#[from] std::io::Error),

    #[error("{0}")]
    Unsupported(String),

    #[error("{details}")]
    Other {
        details: &'static str,
        #[source]
        source: std::io::Error,
    },
}

impl ExportError {
    #[track_caller]
    pub(super) fn other(details: &'static str, source: std::io::Error) -> Self {
        ExportErrorKind::Other { details, source }.into()
    }

    #[track_caller]
    pub(super) fn unsupported(what: impl Into<String>) -> Self {
        ExportErrorKind::Unsupported(what.into()).into()
    }

    /// Like libpq, only a refused connection gets a hint:
    /// every other error is explained well enough by its own message.
    pub(super) fn get_hint(&self) -> Option<&'static str> {
        let ExportErrorKind::Connect(source) = &self.kind else {
            return None;
        };
        let source: &(dyn std::error::Error + 'static) = source;
        let is_refused = std::iter::successors(Some(source), |error| error.source())
            .filter_map(|error| error.downcast_ref::<std::io::Error>())
            .any(|error| error.kind() == std::io::ErrorKind::ConnectionRefused);
        is_refused.then_some(
            "is the cluster running on that host and accepting pgproto connections on that port?",
        )
    }

    pub(super) fn is_broken_pipe(&self) -> bool {
        matches!(
            &self.kind,
            ExportErrorKind::Output { source, .. }
                if source.kind() == std::io::ErrorKind::BrokenPipe
        )
    }

    pub(super) fn exit_code(&self) -> i32 {
        match self.kind {
            ExportErrorKind::BadDsn(_)
            | ExportErrorKind::NoUser
            | ExportErrorKind::TlsRequested { .. } => EXIT_USAGE,
            ExportErrorKind::Other { .. }
            | ExportErrorKind::Connect(_)
            | ExportErrorKind::Query { .. }
            | ExportErrorKind::Decode { .. }
            | ExportErrorKind::Output { .. }
            | ExportErrorKind::Write(_)
            | ExportErrorKind::Unsupported(_) => EXIT_FAILURE,
        }
    }
}

/// Walks the source chain looking for an error the server itself reported.
/// This is both the most informative link in the chain and the only one that contains `SqlState`.
/// `tokio_postgres::Error` hides the detailed text of the original error,
/// displaying, for example, a simple message such as “DB error.”
fn look_for_server_error<'error>(
    error: &'error (dyn std::error::Error + 'static),
) -> Option<&'error DbError> {
    std::iter::successors(Some(error), |error| error.source()).find_map(|error| {
        error
            .downcast_ref::<pg::Error>()
            .and_then(pg::Error::as_db_error)
    })
}

/// Every error here keeps its cause in `#[source]` instead of its own message,
/// since the message can only reference to one level down. The errors of
/// `tokio_postgres`, `std::io` and `tempfile` in the chain do the same, so joining
/// the messages of the whole chain prints each of them exactly once.
pub(super) fn describe_chain(error: &(dyn std::error::Error + 'static)) -> String {
    if let Some(server_error) = look_for_server_error(error) {
        return format!("{error}: {}", server_error.message());
    }

    std::iter::successors(Some(error), |error| error.source())
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}
