//! Deep nesting must not multiply the parse work.
//!
//! pest remembers nothing, so a rule that reads a nested part and then fails
//! makes the next alternative read it again, on every level. 40 levels of such
//! a rule take about 2^40 steps: the parse runs out of the step budget and the
//! test fails at once instead of hanging.

use crate::errors::SbroadError;
use crate::frontend::sql::ast::{AbstractSyntaxTree, ParseTree, ParsingPairsMap, Rule};
use sql_ast_new_grammar::{parse_with_budget, TOO_COMPLEX};
use std::collections::HashMap;

/// Expressions as (innermost level, one level with `{}` for the inner one).
const CORRECT: &[(&str, &str)] = &[
    ("1", "case when true then {} end"),
    ("1", "cast({} as int)"),
    ("1", "array[{}]"),
    ("1", "(select {})"),
    ("1", "1 in ({}, 2)"),
    ("a", "coalesce({}, false)"),
    ("a", "sum({}) filter (where true) over ()"),
    ("a", "({}, 1)"),
    ("a", "trim({})"),
    ("'x'", "trim({} from a)"),
    ("a", "trim({}, 'a')"),
    ("a", "substring({} similar 'x' escape '#')"),
    ("a", "substring({} from 1)"),
    ("a", "substring({} from 1 for 2)"),
    ("a", "substring({} for 2)"),
    ("a", "substring({}, 1)"),
    ("a", "substring({}, 1, 2)"),
    ("'x'", "substring('a' similar {} escape '#')"),
    ("1", "substring('a' from {})"),
    ("1", "substring('a' from {} for 2)"),
    ("1", "substring('a' for {})"),
    ("1", "substring('a', {})"),
    ("1", "substring('a', {}, 2)"),
    ("1", "sum(a) over (rows {} following)"),
];

/// Forms with an error deep inside that no rule reads twice: they fail with
/// the syntax error. The budget error here means a lookahead stopped working.
const SYNTAX_ERROR: &[(&str, &str)] = &[
    ("1 +", "({}, 1)"),
    ("1 +", "substring({})"),
    ("1 +", "sum(a) over (rows {} following)"),
    ("1 +", "cast(f() over (partition by {}) as int)"),
];

/// Forms whose error path still reads the nested part again: only the
/// budget stops them.
const TOO_COMPLEX_ERROR: &[(&str, &str)] = &[
    ("1 +", "1 in ({})"),
    ("1 +", "({})"),
    ("a", "sum({}) filter (where true)"),
    ("1 +", "trim({})"),
    // Unclosed: still read as trim, for the error position (see
    // `error_text_kept`), and every level reads the rest of the query again.
    ("a", "trim({}"),
];

/// `select` of 40 levels of the form.
fn query((seed, level): (&str, &str)) -> String {
    let expr = (0..40).fold(seed.to_owned(), |x, _| level.replace("{}", &x));
    format!("select {expr}")
}

/// Only the parse, through the real entry point: no plan is built.
fn parse(query: &str) -> Result<(), SbroadError> {
    let mut ast = AbstractSyntaxTree::empty();
    ast.fill(
        query,
        &mut ParsingPairsMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut Vec::new(),
    )
}

#[test]
fn correct_forms_parse() {
    for &form in CORRECT {
        let query = query(form);
        if let Err(e) = parse(&query) {
            panic!("{form:?}: {e}\n{query}");
        }
    }
}

/// The check for `FROM` in `trim` runs once per run of spaces: at every space
/// over the rest of the run, 10 000 spaces would take 50 million steps.
#[test]
fn trim_spaces_parse() {
    parse(&format!("select trim('x'{})", " ".repeat(10_000))).unwrap();
}

#[test]
fn erroneous_forms_fail() {
    for (forms, too_complex) in [(SYNTAX_ERROR, false), (TOO_COMPLEX_ERROR, true)] {
        for &form in forms {
            let err = parse(&query(form)).unwrap_err().to_string();
            assert_eq!(err.contains(TOO_COMPLEX), too_complex, "{form:?}: {err}");
        }
    }
}

/// The lookaheads must not add their rules to the error message.
#[test]
fn error_text_kept() {
    let err = parse("select count(distinct) from t1")
        .unwrap_err()
        .to_string();
    // pest writes "= unexpected ...; expected ..." when a rule leaks there.
    assert!(err.contains("= expected Expr or CountAsterisk"), "{err}");

    // The tails of substring are tried at their first word only.
    let err = parse("select substring('x';)").unwrap_err().to_string();
    assert!(!err.contains("Tail"), "{err}");

    // An unclosed trim is still read as trim: the error is where `)` is missing.
    let err = parse("select trim(both 'x' from name")
        .unwrap_err()
        .to_string();
    assert!(err.contains("--> 1:31"), "{err}");
    let err = parse("select trim(a from b from t")
        .unwrap_err()
        .to_string();
    assert!(err.contains("--> 1:22"), "{err}");
}

/// A digit is a step of the budget: an error path that reads a long number
/// again on every level must run out of the budget, not take quadratic time.
#[test]
fn digits_take_steps() {
    // pest keeps one limit for the whole process, and a test that parses with
    // pest directly runs under the limit of a parse in another thread: a
    // budget below that of any query could fail it.
    let budget = sql_ast_new_grammar::MIN_STEP_MAX;
    let parse =
        |query: &str| parse_with_budget::<ParseTree, Rule>(Rule::Command, query, budget).map(drop);
    parse("select 1").unwrap();
    let err = parse(&format!("select {}", "1".repeat(2 * budget))).unwrap_err();
    assert_eq!(err, TOO_COMPLEX);
}

/// A dense list of one-letter names takes the most steps per byte of a correct
/// query, up to 180. 100 KB of it must fit the budget, where its constant part
/// no longer helps.
#[test]
fn dense_list_parses() {
    parse(&format!("select {}x", "x,".repeat(50_000))).unwrap();
}
