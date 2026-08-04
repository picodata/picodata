//! The pest grammar, and the only place `query.pest` is compiled.
//!
//! [`PairParser`] and the generated [`Rule`] enum are a leaf below every stage
//! of the new SQL AST: `sql-ast-new-parser` drives the whole grammar, and
//! `sql-ast-new-nodes` reaches for exactly one rule — `Rule::RegularIdentifier`,
//! which `Ident`'s rendering probes to decide whether an identifier can be
//! written bare. Probing the grammar rather than restating its character
//! classes is what keeps the two from drifting.

#![deny(rustdoc::broken_intra_doc_links)]

use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "query.pest"]
pub struct PairParser;

/// The grammar source itself.
///
/// The generated [`Rule`] enum names the rules but not the literals they spell,
/// so a test that wants to assert something about *every keyword* has no way to
/// enumerate them. Exposing the text lets such a test read the keyword set off
/// the grammar instead of keeping a second copy of it that silently goes stale
/// — see `sql-ast-new-parser`'s keyword coverage tests.
pub const GRAMMAR_SOURCE: &str = include_str!("query.pest");
