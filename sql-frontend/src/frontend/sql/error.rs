use smol_str::SmolStr;
use thiserror::Error;

use crate::errors::{Entity, SbroadError};

/// Result alias for SQL frontend parser helpers.
pub(super) type Result<T> = std::result::Result<T, SqlFrontendError>;

/// Typed errors produced by SQL frontend parsing helpers.
#[derive(Debug, Error)]
pub(super) enum SqlFrontendError {
    #[error("{0}")]
    Frontend(SbroadError),

    #[error("{0}")]
    InsertConflict(#[from] InsertConflictError),
}

#[derive(Debug, Error)]
pub(super) enum InsertConflictError {
    #[error("ON CONFLICT DO UPDATE requires explicit conflict target")]
    TargetRequired,

    #[error("ON CONFLICT target is only supported with DO UPDATE")]
    TargetDoUpdateOnly,

    #[error(
        "unsupported constant type {kind} in ON CONFLICT DO UPDATE \
         (supported: integer, decimal, double, boolean, null)"
    )]
    UnsupportedLiteral { kind: SmolStr },

    #[error(
        "column reference {name:?} is ambiguous: it could refer to either a \
         LET variable or a table column"
    )]
    LetColumnAmbiguous { name: SmolStr },

    #[error(
        "column {name} is of type {column_type}, but update value is of type \
         {value_type}"
    )]
    ValueTypeMismatch {
        name: SmolStr,
        column_type: SmolStr,
        value_type: SmolStr,
    },

    #[error(
        "unsupported ON CONFLICT DO UPDATE expression for column {name}: \
         expected {name} = {name} op value, or value op {name} for \
         commutative ops"
    )]
    InvalidSelfReference { name: SmolStr },

    #[error("{column_kind} column {name} cannot be updated in ON CONFLICT DO UPDATE")]
    ForbiddenColumnUpdate {
        name: SmolStr,
        column_kind: &'static str,
    },

    #[error("the same column is specified twice in {location}: {name}")]
    DuplicateColumn {
        name: SmolStr,
        location: &'static str,
    },

    #[error("ON CONFLICT DO UPDATE SET target column must not be table-qualified")]
    QualifiedUpdateColumn,
}

impl SqlFrontendError {
    pub(super) fn insert_conflict_ast(reason: impl Into<SmolStr>) -> Self {
        Self::Frontend(SbroadError::Invalid(Entity::AST, Some(reason.into())))
    }
}

impl From<SbroadError> for SqlFrontendError {
    fn from(value: SbroadError) -> Self {
        Self::Frontend(value)
    }
}

/// Maps SQL frontend parser errors to compact `SbroadError` messages.
pub(super) fn map_sbroad_error(error: SqlFrontendError) -> SbroadError {
    SbroadError::other(error.to_string())
}
