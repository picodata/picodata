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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

use pest::error::{Error, ErrorVariant, LineColLocation};
use pest::iterators::Pairs;
use pest::{Parser, RuleType};
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "query.pest"]
pub struct PairParser;

/// Parser steps allowed per byte of the query.
///
/// About 8 times the most a query of the tests (sbroad, `test/int/sql`, the
/// corpus of the new parser) takes per byte from 1 KB on, 41, and twice the
/// most of a dense list of one-letter names like `select x, x, ...`, 180. It
/// does not grow with the nesting depth: long queries are shallow, and a
/// budget fit for deep ones would let a megabyte of a malicious query block
/// the thread for seconds. The time of a long query is bounded by the cap of
/// [`set_step_max`].
const STEPS_PER_BYTE: usize = 350;

/// Parser steps allowed on top, for a deeply nested part of the query.
///
/// The lookaheads of the grammars run over the nested parentheses on every
/// level, so a deep part takes steps in proportion to its length times its
/// depth. 4 times the most steps of a correct form 40 levels deep.
const NESTED_STEPS: usize = 900_000;

/// The error text when a query runs out of the step budget.
pub const TOO_COMPLEX: &str = "query is too complex to parse";

/// The default cap of [`set_step_max`].
///
/// 30 million steps keep the thread for 60 to 150 ms, by the form of the
/// query: well within the 1 s of the raft heartbeat and of the replication
/// timeout, by which the other instances see a stuck thread. Correct queries
/// reach the cap from about 170 KB on as a dense list of one-letter names,
/// from about 700 KB on as the queries of the tests.
pub const DEFAULT_STEP_MAX: usize = 30_000_000;

/// The least cap of [`set_step_max`] but 0: the budget of an empty query.
///
/// A lower cap would reject short queries that parse now, among them the
/// `ALTER SYSTEM` that would raise the cap back.
pub const MIN_STEP_MAX: usize = NESTED_STEPS;

/// The cap of [`set_step_max`].
static STEP_MAX: AtomicUsize = AtomicUsize::new(DEFAULT_STEP_MAX);

/// Caps the step budget of every query, whatever its length: picodata sets it
/// from `sql_parse_step_max`. 0 removes the cap, but not the budget per byte,
/// which stops the parses that grow exponentially.
pub fn set_step_max(max: usize) {
    STEP_MAX.store(max, Ordering::Relaxed);
}

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

/// Parse `input` with a step budget linear in its length: `STEPS_PER_BYTE`
/// per byte plus `NESTED_STEPS`, at most the cap of [`set_step_max`].
///
/// pest tries the alternatives of a rule one by one and remembers nothing,
/// so a grammar can re-read a nested part on every level, and the work grows
/// exponentially with the depth. The budget stops such a parse quickly
/// instead. A step is an attempt of a rule or of its part (a sequence,
/// a repetition, an optional or a lookahead).
///
/// Every parse of a query text must go through here. The error is the text
/// for the user: [`TOO_COMPLEX`] with the budget and a hint when the budget
/// runs out, the pest message otherwise.
pub fn parse_limited<P: Parser<R>, R: RuleType>(
    rule: R,
    input: &str,
) -> Result<Pairs<'_, R>, String> {
    parse_capped::<P, R>(rule, input, STEP_MAX.load(Ordering::Relaxed))
}

/// [`parse_limited`] with the cap `max`, 0 for none.
fn parse_capped<P: Parser<R>, R: RuleType>(
    rule: R,
    input: &str,
    max: usize,
) -> Result<Pairs<'_, R>, String> {
    let budget = STEPS_PER_BYTE
        .saturating_mul(input.len())
        .saturating_add(NESTED_STEPS);
    let capped = max != 0 && max < budget;
    let budget = if capped { max } else { budget };
    parse_steps::<P, R>(rule, input, budget).map_err(|e| {
        e.unwrap_or_else(|| {
            if capped {
                format!(
                    "{TOO_COMPLEX}: it takes over {budget} parser steps, the limit of \
                    sql_parse_step_max; split it into smaller queries"
                )
            } else {
                format!(
                    "{TOO_COMPLEX}: it takes over {budget} parser steps for its {} bytes; \
                    reduce its nesting",
                    input.len()
                )
            }
        })
    })
}

/// [`parse_limited`] with a given budget, for the tests of what a rule costs.
pub fn parse_with_budget<P: Parser<R>, R: RuleType>(
    rule: R,
    input: &str,
    budget: usize,
) -> Result<Pairs<'_, R>, String> {
    parse_steps::<P, R>(rule, input, budget)
        .map_err(|e| e.unwrap_or_else(|| TOO_COMPLEX.to_owned()))
}

/// Parse `input` in at most `budget` steps. The error is `None` when the
/// budget runs out, the pest message otherwise.
fn parse_steps<P: Parser<R>, R: RuleType>(
    rule: R,
    input: &str,
    budget: usize,
) -> Result<Pairs<'_, R>, Option<String>> {
    // A panic inside pest leaves the limit set, which does no harm: the next
    // parse here sets its own, and the parses elsewhere are one-word checks
    // far below any budget.
    let _lock = CALL_LIMIT_LOCK
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    pest::set_call_limit(NonZeroUsize::new(budget));
    let res = P::parse(rule, input);
    // Parses outside of this function (tests, one-word checks) run unlimited.
    pest::set_call_limit(None);
    res.map_err(|e| match e.variant {
        // Our grammars raise no custom errors, only the step limit does.
        ErrorVariant::CustomError { .. } => None,
        ErrorVariant::ParsingError { .. } => Some(syntax_error(&e)),
    })
}

/// The most characters of the query line a syntax error shows on each side
/// of the error position.
const ERROR_CONTEXT: usize = 100;

/// The pest message of a syntax error: the line of the query with the error
/// position, cut to `ERROR_CONTEXT` characters on each side. pest shows the
/// whole line, and a long query would get an error of its size.
fn syntax_error<R: RuleType>(e: &Error<R>) -> String {
    // pest reports a syntax error at a position, not at a span.
    let LineColLocation::Pos((row, col)) = e.line_col else {
        return e.to_string();
    };
    let chars = e.line().chars().count();
    if chars <= 2 * ERROR_CONTEXT {
        return e.to_string();
    }
    let skip = col.saturating_sub(1 + ERROR_CONTEXT);
    let end = col + ERROR_CONTEXT;
    let mut line = String::from(if skip > 0 { "..." } else { "" });
    let caret = line.len() + col - 1 - skip;
    line.extend(e.line().chars().skip(skip).take(end - skip));
    if end < chars {
        line.push_str("...");
    }
    // The spaces up to the error position keep the tabs, as in pest.
    let underline: String = line
        .chars()
        .take(caret)
        .map(|c| if c == '\t' { '\t' } else { ' ' })
        .collect();
    // The rest is the format of pest.
    let s = " ".repeat(row.to_string().len());
    format!(
        "{s}--> {row}:{col}\n{s} |\n{row} | {line}\n{s} | {underline}^---\n{s} |\n{s} = {}",
        e.variant.message()
    )
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

    /// The cap bounds the budget of a long query; 0 removes the cap but not
    /// the budget per byte.
    #[test]
    fn budget_capped() {
        let parse = |query: &str, max| {
            parse_capped::<PairParser, Rule>(Rule::Command, query, max).map(drop)
        };
        // About 170 steps per byte, 3.4 million.
        let long = format!("select {}x", "x,".repeat(10_000));
        let err = parse(&long, MIN_STEP_MAX).unwrap_err();
        assert!(err.starts_with(TOO_COMPLEX), "{err}");
        assert!(err.contains("sql_parse_step_max"), "{err}");
        parse(&long, 0).unwrap();
        parse(&long, DEFAULT_STEP_MAX).unwrap();

        let deep = format!("select {}1 +{}", "1 in (".repeat(40), ")".repeat(40));
        let err = parse(&deep, 0).unwrap_err();
        assert!(err.starts_with(TOO_COMPLEX), "{err}");
        assert!(err.contains("reduce its nesting"), "{err}");
    }

    /// A long line of the query is cut around the error position.
    #[test]
    fn long_line_cut() {
        let parse = |query: &str| parse_limited::<PairParser, Rule>(Rule::Command, query).map(drop);
        // A line of 188 characters is kept whole.
        let short = format!("select {})", "x,".repeat(90));
        // Under the lock, or the budget of another test could cut the parse.
        let pest = {
            let _lock = CALL_LIMIT_LOCK
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            PairParser::parse(Rule::Command, &short).unwrap_err()
        };
        assert_eq!(parse(&short).unwrap_err(), pest.to_string());

        // The error at the end: 100 characters before it.
        let err = parse(&format!("select {})", "x,".repeat(200))).unwrap_err();
        let head = format!(
            " --> 1:408\n  |\n1 | ...{})\n  | {}^---\n  |\n  = expected ",
            "x,".repeat(50),
            " ".repeat(103),
        );
        assert!(err.starts_with(&head), "{err}");

        // The error at the start: 100 characters after it.
        let err = parse(&format!("select ){}", " x".repeat(200))).unwrap_err();
        let head = format!(
            " --> 1:8\n  |\n1 | select ){}...\n  |        ^---\n",
            " x".repeat(50),
        );
        assert!(err.starts_with(&head), "{err}");
    }
}
