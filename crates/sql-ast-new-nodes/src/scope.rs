//! The stack of statements a reader of the analyzed tree is inside of.
//!
//! An analyzed reference names what it resolved to by *position* in an
//! enclosing statement, never by pointer. Reading through one therefore takes
//! the statements between the root and the reader, one [`FrameView`] each,
//! which every walk over the analyzed tree keeps as it descends: rendering
//! (see [`rendering`](crate::rendering)), and the analyzer while it binds.

use crate::multiset::Ctes;
use crate::table_expression::From;
use crate::Analyzed;

/// What one statement contributes to the scope of everything nested in it:
/// its `WITH` clause, and the `FROM` clause of its `SELECT` body if it has one.
#[derive(Clone, Copy)]
pub struct FrameView<'a, 'q> {
    pub ctes: &'a Ctes<'q, Analyzed>,
    /// [`None`] for a `VALUES` statement and for a set operation, which have
    /// no `FROM` of their own.
    pub from: Option<&'a From<'q, Analyzed>>,
}
