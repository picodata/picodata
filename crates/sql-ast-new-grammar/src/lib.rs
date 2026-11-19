//! The pest grammar, and the only place `query.pest` is compiled.
//!
//! [`PairParser`] and the generated [`Rule`] enum are a leaf below every stage
//! of the new SQL AST: `sql-ast-new-parser` drives the whole grammar, and
//! `sql-ast-new-nodes` reaches for exactly one rule — `Rule::RegularIdentifier`,
//! which `Ident`'s rendering probes to decide whether an identifier can be
//! written bare. Probing the grammar rather than restating its character
//! classes is what keeps the two from drifting.
//!
//! [`parse_limited`] is here too: both SQL parsers parse a query through it.

#![deny(rustdoc::broken_intra_doc_links)]

use std::num::NonZeroUsize;
use std::sync::{Mutex, PoisonError};

use pest::error::ErrorVariant;
use pest::iterators::Pairs;
use pest::{Parser, RuleType};
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "query.pest"]
pub struct PairParser;

/// Parser steps allowed per byte of the query.
// XXX: a generous guess until the grammars stop re-reading nested parts.
const STEPS_PER_BYTE: usize = 1000;

/// A query shorter than this gets the budget of a query this long.
const MIN_LEN: usize = 1000;

/// The error text when a query runs out of the step budget.
pub const TOO_COMPLEX: &str = "query is too complex to parse";

/// Guards the step limit, which pest keeps in one process-wide variable and
/// reads when a parse starts.
///
/// picodata parses queries in the tx thread only, and nothing between setting
/// the limit and the start of the parse yields the fiber, so there the lock
/// never waits. It is for `cargo test`, which runs the tests as threads of one
/// process: without the lock a parse could start with the limit of a parse in
/// another thread and fail with [`TOO_COMPLEX`] at random. The state cannot be
/// thread-local, as elsewhere in picodata, because it belongs to pest.
///
/// Nothing under the lock may yield the fiber: another fiber of the tx thread
/// would block the whole thread on `lock()`.
static CALL_LIMIT_LOCK: Mutex<()> = Mutex::new(());

/// Parse `input` with a step budget proportional to its length.
///
/// pest tries the alternatives of a rule one by one and remembers nothing,
/// so a grammar can re-read a nested part on every level, and the work grows
/// exponentially with the depth. The budget stops such a parse quickly
/// instead. A step is an attempt of a rule or of its part (a sequence,
/// a repetition, an optional or a lookahead).
///
/// Every parse of a query text must go through here. The error is the text
/// for the user: [`TOO_COMPLEX`] when the budget runs out, the pest message
/// otherwise.
pub fn parse_limited<P: Parser<R>, R: RuleType>(
    rule: R,
    input: &str,
) -> Result<Pairs<'_, R>, String> {
    let budget = STEPS_PER_BYTE.saturating_mul(input.len().max(MIN_LEN));
    parse_with_budget::<P, R>(rule, input, budget)
}

fn parse_with_budget<P: Parser<R>, R: RuleType>(
    rule: R,
    input: &str,
    budget: usize,
) -> Result<Pairs<'_, R>, String> {
    // A panic inside pest leaves nothing to clean up: the next parse sets its
    // own limit.
    let _lock = CALL_LIMIT_LOCK
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    pest::set_call_limit(NonZeroUsize::new(budget));
    let res = P::parse(rule, input);
    // Parses outside of this function (tests, one-word checks) run unlimited.
    pest::set_call_limit(None);
    res.map_err(|e| match e.variant {
        // Our grammars raise no custom errors, only the step limit does.
        ErrorVariant::CustomError { .. } => TOO_COMPLEX.to_owned(),
        // `Error` prints the line of the query with the error position.
        ErrorVariant::ParsingError { .. } => e.to_string(),
    })
}

/// The grammar source itself.
///
/// The generated [`Rule`] enum names the rules but not the literals they spell,
/// so a test that wants to assert something about *every keyword* has no way to
/// enumerate them. Exposing the text lets such a test read the keyword set off
/// the grammar instead of keeping a second copy of it that silently goes stale
/// — see `sql-ast-new-parser`'s keyword coverage tests.
pub const GRAMMAR_SOURCE: &str = include_str!("query.pest");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_runs_out() {
        let err = parse_with_budget::<PairParser, Rule>(Rule::Command, "select 1", 1).unwrap_err();
        assert_eq!(err, TOO_COMPLEX);

        // The limit does not outlive the parse.
        PairParser::parse(Rule::Command, "select 1").unwrap();
        parse_limited::<PairParser, Rule>(Rule::Command, "select 1").unwrap();
    }

    #[test]
    fn syntax_error_is_pest_message() {
        let err = parse_limited::<PairParser, Rule>(Rule::Command, "select 1 +").unwrap_err();
        assert!(err.contains("expected"), "{err}");
    }
}
