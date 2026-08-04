//! The error type shared by every stage of the new SQL AST.
//!
//! Both stages fail the same way, so [`AstErr`] and its constructors sit in this
//! crate — below the parser and the analyzer.

use smol_str::{format_smolstr, SmolStr};
use sql_type_system::error::Error as TypeSystemError;

use sql_ir::errors::SbroadError;

/// AST-local error: a boxed [`SbroadError`].
///
/// Every function inside the AST crates returns [`AstResult`]. The frontend's
/// `Ast` trait boundary unwraps it back into a plain [`SbroadError`] via
/// [`From<AstErr>`], so nothing outside the AST ever sees [`AstErr`].
#[derive(Debug, PartialEq)]
pub struct AstErr(SbroadError);

impl AstErr {
    pub fn new(err: SbroadError) -> Self {
        Self(err)
    }

    fn sbroad_err(self) -> SbroadError {
        self.0
    }
}

/// The module-local [`Result`]: [`Result<T, AstErr>`]. Used by every inner function.
pub type AstResult<T> = std::result::Result<T, AstErr>;

impl std::fmt::Display for AstErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

/// Box on the way in: lets `?` turn any [`SbroadError`]-producing call (or one of
/// the `*_error` helpers) into an [`AstErr`] inside an [`AstResult`] function.
impl From<SbroadError> for AstErr {
    fn from(err: SbroadError) -> Self {
        AstErr::new(err)
    }
}

/// Unbox on the way out: lets the frontend's `Ast` trait boundary propagate an
/// inner [`AstErr`] as a plain [`SbroadError`].
impl From<AstErr> for SbroadError {
    fn from(err: AstErr) -> Self {
        err.sbroad_err()
    }
}

/// The one foreign error propagated with `?` inside the module.
/// `?` performs a single [`From`], so it cannot chain
/// [`TypeSystemError`] -> [`SbroadError`] -> [`AstErr`] on its own.
impl From<TypeSystemError> for AstErr {
    fn from(err: TypeSystemError) -> Self {
        AstErr::new(err.into())
    }
}

/// Appended to internal-invariant errors: they indicate grammar/parser or
/// parser/analyzer drift (a bug), not a user mistake.
pub const REPORT_TO_SUFFIX: &str = "Report to git.picodata.io/core/picodata/-/issues.";

pub(super) fn ast_arbitrary_err(msg: SmolStr) -> AstErr {
    SbroadError::Other(msg).into()
}

pub(super) fn ast_invariant_err(msg: SmolStr) -> AstErr {
    SbroadError::Other(format_smolstr!("{msg}. {REPORT_TO_SUFFIX}")).into()
}
