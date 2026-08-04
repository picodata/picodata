//! How expression forms combine: precedence, associativity and the tiers that
//! forbid combining at all.
//!
//! # Cases
//! Each case states a grouping twice — once fully parenthesized, once bare — and
//! asserts they parse the same.
//!
//! That pins two things at once.
//! 1. The operator ladder itself.
//! 2. The round trip through rendering, since the bare spelling is also the
//!    snapshot the parenthesized one must print as.
//!
//! The reference for the expected groupings is PostgreSQL.
//!
//!
//! # Non-associative tiers
//! Comparison and matching get the opposite treatment: chaining them must be
//! rejected, not silently grouped.

use super::{expr_err, parse_expr, render_expr};

/// Assert that two spellings parse into structurally identical trees.
#[track_caller]
fn assert_parses_same(left: &'static str, right: &'static str) {
    let left_parsed = parse_expr(left).expect("left expression must parse");
    let right_parsed = parse_expr(right).expect("right expression must parse");
    assert!(
        left_parsed == right_parsed,
        "`{left}` and `{right}` parse into structurally different trees"
    );
}

/// One precedence case: the parenthesized spelling must render as the bare
/// snapshot, and both spellings must parse into structurally identical trees.
/// The [`assert_ne!`] guard keeps a careless snapshot-accept from rewriting the
/// bare spelling into the parenthesized one, which would make the structural
/// check a tautology.
macro_rules! assert_precedence {
    ($parenthesized:literal, @$bare:literal) => {
        assert_ne!($parenthesized, $bare, "snapshot must be the bare spelling");
        insta::assert_snapshot!(render_expr($parenthesized), @$bare);
        assert_parses_same($parenthesized, $bare);
    };
}

//////////////////////////////// . ////////////////////////////////

#[test]
fn qualified_name_binds_tighter_than_cast() {
    assert_precedence!("(t.c)::int", @"t.c::int");
}

//////////////////////////////// :: ////////////////////////////////

#[test]
fn cast_binds_tighter_than_arithmetic() {
    assert_precedence!("(a::int) + 1", @"a::int + 1");
    assert_precedence!("(a::int) * b", @"a::int * b");
}

//////////////////////////////// [ ] ////////////////////////////////

#[test]
fn index_binds_tighter_than_arithmetic() {
    assert_precedence!("(a[1]) + 1", @"a[1] + 1");
}

#[test]
fn index_and_cast_postfixes_apply_left_to_right() {
    assert_precedence!("(a[1])::int", @"a[1]::int");
}

//////////////////////////////// + - (unary) ////////////////////////////////

// The signs render with a trailing space: a glued `-1` would lex as one
// signed-literal token and change the tree shape (see the next test).
#[test]
fn unary_minus_applies_to_any_operand() {
    insta::assert_snapshot!(render_expr("-x"), @"- x");
    insta::assert_snapshot!(render_expr("-(a + b)"), @"- (a + b)");
    insta::assert_snapshot!(render_expr("+x"), @"+ x");
}

// A sign directly adjacent to a number is part of the literal token, not a
// unary operator — that is what lets i64::MIN's spelling fit. With a space
// (or another sign in between) it is the operator applied to a bare literal.
#[test]
fn sign_adjacent_to_number_is_part_of_the_literal() {
    insta::assert_snapshot!(render_expr("-1"), @"-1");
    insta::assert_snapshot!(render_expr("- 1"), @"- 1");
    assert!(
        parse_expr("-1").expect("literal") != parse_expr("- 1").expect("unary over literal"),
        "`-1` (a literal) and `- 1` (unary minus over a literal) must differ structurally"
    );
}

#[test]
fn unary_sign_binds_tighter_than_multiplication() {
    assert_precedence!("(- a) * b", @"- a * b");
    assert_precedence!("(+ a) % b", @"+ a % b");
}

// PG's `UMINUS` sits below the postfixes: `-a::int` negates the cast result.
#[test]
fn cast_binds_tighter_than_unary_sign() {
    assert_precedence!("- (a::int)", @"- a::int");
}

// See `cast_binds_tighter_than_unary_sign`.
#[test]
fn index_binds_tighter_than_unary_sign() {
    assert_precedence!("- (a[1])", @"- a[1]");
}

#[test]
fn unary_signs_stack() {
    assert_precedence!("- (- a)", @"- - a");
    assert_precedence!("+ (- a)", @"+ - a");
}

#[test]
fn unary_sign_as_binary_operand() {
    assert_precedence!("a - (- b)", @"a - - b");
    assert_precedence!("a - (- 1)", @"a - - 1");
}

#[test]
fn not_binds_looser_than_unary_sign() {
    assert_precedence!("NOT (- a)", @"NOT - a");
}

// `IS` binds looser than the signs, as in PG.
#[test]
fn unary_sign_binds_tighter_than_is() {
    assert_precedence!("(- a) IS NULL", @"- a IS NULL");
}

// The signs are legal in BETWEEN's middle operand.
#[test]
fn between_middle_admits_unary_sign() {
    assert_precedence!("a BETWEEN (- b) AND c", @"a BETWEEN - b AND c");
}

// `--` stays a syntax error in prefix position: PostgreSQL lexes it as a
// comment opener, so it must not silently read as a double negation. `++`
// likewise (PG lexes it as one unknown operator).
#[test]
fn doubled_glued_signs_are_rejected() {
    insta::assert_snapshot!(expr_err("--a"), @r"
    expression parsing error:  --> 1:1
      |
    1 | --a
      | ^---
      |
      = expected Expr
    ");
    insta::assert_snapshot!(expr_err("++a"), @r"
    expression parsing error:  --> 1:1
      |
    1 | ++a
      | ^---
      |
      = expected Expr
    ");
}

#[test]
fn negative_literal_binds_tighter_than_multiplication() {
    assert_precedence!("(-2) * a", @"-2 * a");
}

//////////////////////////////// ^ ////////////////////////////////

// `^` (exponentiation, between unary minus and `* / %`) is
// not supported.
// #[test]
// fn exponentiation_is_supported() {
//     insta::assert_snapshot!(render_expr("2 ^ 3"), @"2 ^ 3");
//     assert_precedence!("2 * (3 ^ 2)", @"2 * 3 ^ 2");
// }

//////////////////////////////// * / % ////////////////////////////////

#[test]
fn arithmetic() {
    assert_precedence!("(a * 2) + b", @"a * 2 + b");
    assert_precedence!("(a * b) + c", @"a * b + c");
}

#[test]
fn multiplicative_tier_left_associative() {
    assert_precedence!("(a * b) / c", @"a * b / c");
    assert_precedence!("(a % b) * c", @"a % b * c");
}

//////////////////////////////// + - ////////////////////////////////

#[test]
fn additive_tier_left_associative() {
    assert_precedence!("(a + b) - c", @"a + b - c");
    assert_precedence!("(a - b) + c", @"a - b + c");
}

#[test]
fn additive_binds_tighter_than_comparison() {
    assert_precedence!("(a + b) < c", @"a + b < c");
}

//////////////////////////////// || (any other operator) ////////////////////////////////

#[test]
fn concat_left_associative() {
    assert_precedence!("(a || b) || c", @"a || b || c");
}

#[test]
fn concat_binds_tighter_than_comparison() {
    assert_precedence!("(a || b) = c", @"a || b = c");
}

// `||` is an "any other operator", below `+ -`.
#[test]
fn additive_binds_tighter_than_concat() {
    assert_precedence!("(a + b) || c", @"a + b || c");
}

// Same tier ordering, seen from the other side.
#[test]
fn multiplicative_binds_tighter_than_concat() {
    assert_precedence!("a || (b * c)", @"a || b * c");
}

//////////////////////////////// BETWEEN IN LIKE ILIKE SIMILAR ////////////////////////////////

#[test]
fn matching_tier_binds_looser_than_concat_and_additive() {
    assert_precedence!("(a || b) LIKE c", @"a || b LIKE c");
    assert_precedence!("(a + b) LIKE c", @"a + b LIKE c");
    assert_precedence!("(a + b) BETWEEN c AND d", @"a + b BETWEEN c AND d");
}

#[test]
fn between_middle_admits_comparison() {
    assert_precedence!("a BETWEEN (b = c) AND d", @"a BETWEEN b = c AND d");
}

#[test]
fn in_then_comparison_groups_left() {
    assert_precedence!("(a IN (1, 2)) = b", @"a IN (1, 2) = b");
}

// IN shares the matching tier, below `+ -`.
#[test]
fn in_binds_looser_than_additive() {
    assert_precedence!("(a + b) IN (1, 2)", @"a + b IN (1, 2)");
}

// See `like_is_non_associative` — IN is on the same
// non-associative tier.
#[test]
fn in_is_non_associative() {
    insta::assert_snapshot!(expr_err("a IN (1, 2) IN (3, 4)"), @"invalid expression: BETWEEN/IN/LIKE/ILIKE/SIMILAR operators are non-associative; use parentheses to group operands");
}

// The IN list is a closed construct; a trailing tighter operator
// is an error.
#[test]
fn in_rhs_is_closed() {
    insta::assert_snapshot!(expr_err("a IN (1, 2) + b"), @"In expression must have query or a list of values as right child");
}

// The matching tier (BETWEEN/IN/LIKE/ILIKE/SIMILAR) binds tighter
// than comparisons.
#[test]
fn like_binds_tighter_than_comparison_on_rhs() {
    assert_precedence!("a = (b LIKE c)", @"a = b LIKE c");
}

// See `like_binds_tighter_than_comparison_on_rhs`.
#[test]
fn like_binds_tighter_than_comparison_on_lhs() {
    assert_precedence!("(a LIKE b) = c", @"a LIKE b = c");
}

// See `like_binds_tighter_than_comparison_on_rhs`.
#[test]
fn in_binds_tighter_than_comparison() {
    assert_precedence!("a = (b IN (1, 2))", @"a = b IN (1, 2)");
}

// See `like_binds_tighter_than_comparison_on_rhs`.
#[test]
fn between_binds_tighter_than_comparison_on_rhs() {
    assert_precedence!("a = (b BETWEEN c AND d)", @"a = b BETWEEN c AND d");
}

// See `like_binds_tighter_than_comparison_on_rhs`.
#[test]
fn between_binds_tighter_than_comparison_on_lhs() {
    assert_precedence!("(a BETWEEN b AND c) = d", @"a BETWEEN b AND c = d");
}

// The matching tier is non-associative.
#[test]
fn like_is_non_associative() {
    insta::assert_snapshot!(expr_err("a LIKE b LIKE c"), @"invalid expression: BETWEEN/IN/LIKE/ILIKE/SIMILAR operators are non-associative; use parentheses to group operands");
}

// See `like_is_non_associative`.
#[test]
fn between_is_non_associative() {
    insta::assert_snapshot!(expr_err("a BETWEEN 1 AND 2 BETWEEN 3 AND 4"), @"invalid expression: BETWEEN/IN/LIKE/ILIKE/SIMILAR operators are non-associative; use parentheses to group operands");
}

// A parenthesized operand resets the tier,
// so an explicitly grouped same-tier chain is legal — and must render with
// its parentheses, since the bare spelling is a syntax error.
#[test]
fn matching_nonassoc_renders_parenthesized() {
    insta::assert_snapshot!(render_expr("(a LIKE b) LIKE c"), @"(a LIKE b) LIKE c");
}

// ESCAPE attaches inside the LIKE production; once the
// LIKE is parenthesized it is an ordinary expression and ESCAPE cannot
// follow it.
#[test]
fn escape_requires_adjacent_like() {
    insta::assert_snapshot!(expr_err("(a LIKE b) ESCAPE 'x'"), @"invalid expression: ESCAPE must directly follow a LIKE/ILIKE/SIMILAR pattern; a parenthesized expression cannot take an ESCAPE clause");
}

// An ESCAPE-carrying LIKE is a completed matching-tier
// construct; chaining another LIKE onto it is non-associative.
#[test]
fn like_escape_then_like_is_rejected() {
    insta::assert_snapshot!(expr_err("a LIKE b ESCAPE c LIKE d"), @"invalid expression: BETWEEN/IN/LIKE/ILIKE/SIMILAR operators are non-associative; use parentheses to group operands");
}

// The BETWEEN middle operand (`b_expr`) excludes IN.
#[test]
fn between_middle_rejects_in() {
    insta::assert_snapshot!(expr_err("a BETWEEN b IN (1, 2) AND c"), @"expression parsing error: expression parsing stopped before: `BETWEEN b IN (1, 2) AND c`");
}

// `b_expr` has no IS forms — the bare spelling is a
// syntax error; the parenthesized middle (a `c_expr`) stays legal and must
// keep its parentheses when rendered.
#[test]
fn between_middle_rejects_bare_is() {
    insta::assert_snapshot!(
        render_expr("a BETWEEN (b IS NULL) AND c"),
        @"a BETWEEN (b IS NULL) AND c"
    );
    insta::assert_snapshot!(expr_err("a BETWEEN b IS NULL AND c"), @"expression parsing error: expression parsing stopped before: `BETWEEN b IS NULL AND c`");
}

// `b_expr` has no NOT either.
#[test]
fn between_middle_rejects_not() {
    insta::assert_snapshot!(expr_err("a BETWEEN NOT b AND c"), @"expression parsing error: expression parsing stopped before: `BETWEEN NOT b AND c`");
}

// Negated matching operators, spelled with the NOT inside the
// operator token.
#[test]
fn not_like_is_supported() {
    insta::assert_snapshot!(render_expr("a NOT LIKE b"), @"a NOT LIKE b");
}

// See `not_like_is_supported`.
#[test]
fn not_ilike_is_supported() {
    insta::assert_snapshot!(render_expr("a NOT ILIKE b"), @"a NOT ILIKE b");
}

// See `not_like_is_supported`.
#[test]
fn not_similar_is_supported() {
    insta::assert_snapshot!(render_expr("a NOT SIMILAR b"), @"a NOT SIMILAR b");
}

// See `not_like_is_supported`. The ESCAPE clause composes.
#[test]
fn not_like_with_escape() {
    insta::assert_snapshot!(
        render_expr("a NOT LIKE 'x%' ESCAPE 'x'"),
        @"a NOT LIKE 'x%' ESCAPE 'x'"
    );
}

//////////////////////////////// < > = <= >= <> ////////////////////////////////

#[test]
fn comparison_binds_tighter_than_is() {
    assert_precedence!("(a = b) IS NULL", @"a = b IS NULL");
}

// Comparisons are non-associative.
#[test]
fn comparison_is_non_associative() {
    insta::assert_snapshot!(expr_err("a = b = c"), @"invalid expression: comparison operators are non-associative; use parentheses to group operands");
    insta::assert_snapshot!(expr_err("a < b < c"), @"invalid expression: comparison operators are non-associative; use parentheses to group operands");
}

// See `matching_nonassoc_renders_parenthesized`.
#[test]
fn comparison_nonassoc_renders_parenthesized() {
    insta::assert_snapshot!(render_expr("(a = b) = c"), @"(a = b) = c");
}

// NOT is legal as a bare comparison operand.
// The conservative rendering keeps the parentheses.
#[test]
fn not_as_comparison_operand() {
    assert_parses_same("a = NOT b", "a = (NOT b)");
    insta::assert_snapshot!(render_expr("a = NOT b"), @"a = (NOT b)");
}

//////////////////////////////// IS ////////////////////////////////

#[test]
fn is() {
    assert_precedence!("(a + b) IS NULL", @"a + b IS NULL");
    assert_precedence!("NOT (TRUE IS TRUE)", @"NOT TRUE IS TRUE");
    assert_precedence!("TRUE AND (TRUE IS TRUE)", @"TRUE AND TRUE IS TRUE");
    assert_precedence!("TRUE OR (TRUE IS TRUE)", @"TRUE OR TRUE IS TRUE");
    assert_precedence!("('hello, world' LIKE '%hello%') IS TRUE", @"'hello, world' LIKE '%hello%' IS TRUE");
}

#[test]
fn is_not() {
    assert_precedence!("(a + b) IS NOT NULL", @"a + b IS NOT NULL");
    assert_precedence!("NOT (TRUE IS NOT TRUE)", @"NOT TRUE IS NOT TRUE");
    assert_precedence!("TRUE AND (TRUE IS NOT TRUE)", @"TRUE AND TRUE IS NOT TRUE");
    assert_precedence!("TRUE OR (TRUE IS NOT TRUE)", @"TRUE OR TRUE IS NOT TRUE");
    assert_precedence!("('hello, world' LIKE '%hello%') IS NOT TRUE", @"'hello, world' LIKE '%hello%' IS NOT TRUE");
}

#[test]
fn is_chains() {
    assert_precedence!("(a IS NULL) IS NOT FALSE", @"a IS NULL IS NOT FALSE");
}

#[test]
fn is_in_sequential() {
    assert_precedence!("(1 IN (1, 2)) IS NOT FALSE", @"1 IN (1, 2) IS NOT FALSE");
}

// A completed IS chain may feed a tighter operator on its
// left (yacc reduces the IS production and keeps going); the conservative
// rendering keeps the parentheses.
#[test]
fn is_result_as_comparison_operand() {
    assert_parses_same("a IS NULL = b", "(a IS NULL) = b");
    insta::assert_snapshot!(render_expr("a IS NULL = b"), @"(a IS NULL) = b");
}

//////////////////////////////// NOT ////////////////////////////////

#[test]
fn not_binds_looser_than_infix_operators() {
    assert_precedence!("NOT (a = b)", @"NOT a = b");
    assert_precedence!("NOT (a LIKE b)", @"NOT a LIKE b");
    assert_precedence!("NOT (a BETWEEN 1 AND 2)", @"NOT a BETWEEN 1 AND 2");
    assert_precedence!("NOT (a IS NULL)", @"NOT a IS NULL");
}

#[test]
fn not_binds_tighter_than_and() {
    assert_precedence!("(NOT a) AND b", @"NOT a AND b");
}

#[test]
fn not_is_right_associative() {
    assert_precedence!("NOT (NOT a)", @"NOT NOT a");
}

//////////////////////////////// AND ////////////////////////////////

#[test]
fn and_left_associative() {
    assert_precedence!("(a AND b) AND c", @"a AND b AND c");
}

#[test]
fn and_binds_tighter_than_or() {
    assert_precedence!("(a AND b) OR c", @"a AND b OR c");
    assert_precedence!("a OR (b AND c)", @"a OR b AND c");
}

//////////////////////////////// OR ////////////////////////////////

#[test]
fn or_left_associative() {
    assert_precedence!("(a OR b) OR c", @"a OR b OR c");
}
