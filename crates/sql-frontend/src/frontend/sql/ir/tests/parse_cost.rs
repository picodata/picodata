//! Deep nesting must not multiply the parse work.
//!
//! pest remembers nothing, so a rule that reads a nested part and then fails
//! makes the next alternative read it again, on every level. 40 levels of such
//! a rule take about 2^40 steps: the parse runs out of the step budget and the
//! test fails at once instead of hanging.

use crate::errors::SbroadError;
use crate::frontend::sql::ast::{AbstractSyntaxTree, ParsingPairsMap};
use sql_ast_new_grammar::TOO_COMPLEX;
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

const ERRONEOUS: &[(&str, &str)] = &[
    ("1 +", "1 in ({})"),
    ("1 +", "cast(f() over (partition by {}) as int)"),
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

#[test]
fn erroneous_forms_fail() {
    for &form in ERRONEOUS {
        assert!(parse(&query(form)).is_err(), "{form:?} parsed");
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
}

#[test]
fn too_complex_error() {
    let err = parse(&query(ERRONEOUS[0])).unwrap_err().to_string();
    assert!(err.contains(TOO_COMPLEX), "{err}");
}
