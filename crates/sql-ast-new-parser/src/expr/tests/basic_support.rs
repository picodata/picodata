//! Per-form coverage of the expression language.
//!
//! # Cases
//! One section per node kind, walking the grammar from the leaves up.
//!
//! Each case renders a spelling of that form and pins the result. An accepted
//! form that parses into the wrong tree therefore fails as loudly as one that is
//! rejected outright.
//!
//!
//! # Rejections
//! Covered here too. An expression the grammar admits but the language does not
//! must fail with a message aimed at the query author.

use super::{expr_err, parse_expr, render_expr};
use sql_ast_new_nodes::expr::*;

//////////////////////////////// Nil ////////////////////////////////

//////////////////////////////// BinaryOperation ////////////////////////////////

#[test]
fn concat_left_associative() {
    insta::assert_snapshot!(
        render_expr("a || b || 'x'"),
        @"a || b || 'x'"
    );
}

#[test]
fn comparison_lte_and_gte() {
    insta::assert_snapshot!(render_expr("a <= b"), @"a <= b");
    insta::assert_snapshot!(render_expr("a >= b"), @"a >= b");
}

#[test]
fn conjunction() {
    insta::assert_snapshot!(render_expr("(true)and(false)"), @"true AND false");
}

// Keyword token boundaries (`a in(1, 2)`, `trueand`, `1and`, ...) live in
// `crate::keywords`, which sweeps the whole keyword set rather than the few
// tokens this module happens to touch.

//////////////////////////////// UnaryOperation ////////////////////////////////

//////////////////////////////// ColumnRef ////////////////////////////////

#[test]
fn unquoted_identifiers_fold_to_lowercase() {
    insta::assert_snapshot!(render_expr("CoLuMn"), @"column");
    insta::assert_snapshot!(render_expr("TaB.CoL"), @"tab.col");
    // Function names normalize too; the quoted argument keeps its case.
    insta::assert_snapshot!(render_expr(r#"Fn(A, "B")"#), @r#"fn(a, "B")"#);
    // Folding is Unicode-aware, not ASCII-only.
    insta::assert_snapshot!(render_expr("Ф"), @"ф");
}

#[test]
fn quoted_identifiers_keep_case_and_render_requoted() {
    insta::assert_snapshot!(render_expr(r#""Column""#), @r#""Column""#);
    insta::assert_snapshot!(render_expr(r#""TaB"."CoL""#), @r#""TaB"."CoL""#);
}

/// A case-preserving quoted identifier renders re-quoted, so the render
/// re-parses to the same identifier and re-renders identically — the
/// expression-level face of `quoted_identifier_rendering_roundtrips`
/// (`mod.rs` tests).
#[test]
fn quoted_identifier_render_roundtrips() {
    let rendered = render_expr(r#""C2""#);
    insta::assert_snapshot!(&rendered, @r#""C2""#);
    let reparsed = parse_expr(&rendered).expect("render must re-parse");
    insta::assert_snapshot!(&reparsed, @r#""C2""#);
}

/// The `""` escape collapses on parse (`"Ab""Cd"` is the identifier `Ab"Cd`) and is re-escaped on render.
#[test]
fn doubled_quote_escape_roundtrips() {
    let rendered = render_expr(r#""Ab""Cd""#);
    insta::assert_snapshot!(&rendered, @r#""Ab""Cd""#);
    let reparsed = parse_expr(&rendered).expect("render must re-parse");
    assert_eq!(reparsed.to_string(), rendered);
}

/// A zero-length delimited identifier is a syntax error.
#[test]
fn zero_length_delimited_identifier_fails() {
    insta::assert_snapshot!(expr_err(r#""" + 1"#), @r#"
    expression parsing error:  --> 1:1
      |
    1 | "" + 1
      | ^---
      |
      = expected Expr
    "#);
}

//////////////////////////////// Literal ////////////////////////////////

#[test]
fn literal_quotes() {
    insta::assert_snapshot!(
        render_expr("'1'"),
        @"'1'"
    );
}

#[test]
fn literal_string_value_is_stored_without_quotes() {
    let expr = parse_expr("'abc'").expect("expected to parse Expr");
    let ExprInner::Literal(Literal { value, quotes, .. }) = expr.inner else {
        panic!("expected a literal at the root");
    };
    insta::assert_snapshot!(value, @"abc");
    assert!(matches!(quotes, QuotesType::Single));
}

#[test]
fn literal_number_has_no_quotes() {
    let expr = parse_expr("42").expect("expected to parse Expr");
    let ExprInner::Literal(Literal { value, quotes, .. }) = expr.inner else {
        panic!("expected a literal at the root");
    };
    insta::assert_snapshot!(value, @"42");
    assert!(matches!(quotes, QuotesType::None));
}

#[test]
fn literal_quotes_with_inner_escaped_quote() {
    insta::assert_snapshot!(
        render_expr("'don''t'"),
        @"'don''t'"
    );
}

#[test]
fn literal_quotes_inside_expression() {
    insta::assert_snapshot!(
        render_expr("a || 'x y' || ''"),
        @"a || 'x y' || ''"
    );
}

#[test]
fn literal_bool_and_null_unquoted() {
    insta::assert_snapshot!(
        render_expr("true = null"),
        @"true = null"
    );
}

#[test]
fn double_literal_scientific_notation() {
    insta::assert_snapshot!(render_expr("1e3"), @"1e3");
    insta::assert_snapshot!(render_expr("2.5e4"), @"2.5e4");
}

#[test]
fn literal_integer_boundaries_round_trip() {
    // i64::MAX; the sign of i64::MIN is part of the `Integer` token, so the
    // whole spelling fits too (unlike the old pipeline, where unary minus is
    // a separate node and the bare magnitude overflows).
    insta::assert_snapshot!(render_expr("9223372036854775807"), @"9223372036854775807");
    insta::assert_snapshot!(render_expr("-9223372036854775808"), @"-9223372036854775808");
}

#[test]
fn unary_minus_double() {
    insta::assert_snapshot!(render_expr("a--b"), @"a - - b");
}

#[test]
fn unary_plus_double() {
    insta::assert_snapshot!(render_expr("a++b"), @"a + + b");
}

#[test]
fn unary_plus_minus_double() {
    insta::assert_snapshot!(render_expr("a+-b"), @"a + - b");
}

#[test]
fn unary_minus_plus_double() {
    insta::assert_snapshot!(render_expr("a-+b"), @"a - + b");
}

#[test]
fn literal_integer_out_of_range_fails() {
    insta::assert_snapshot!(
        expr_err("9223372036854775808"),
        @"invalid query: value doesn't fit into integer range: 9223372036854775808"
    );
    insta::assert_snapshot!(
        expr_err("-9223372036854775809"),
        @"invalid query: value doesn't fit into integer range: -9223372036854775809"
    );
    // With a space the sign is the unary operator over a bare literal, and
    // the bare magnitude overflows. Documented divergence: PG parses
    // oversized integer literals as numeric, so `- 9223372036854775808`
    // works there via its constant-negation fold.
    insta::assert_snapshot!(
        expr_err("- 9223372036854775808"),
        @"invalid query: value doesn't fit into integer range: 9223372036854775808"
    );
}

//////////////////////////////// SubQuery ////////////////////////////////

//////////////////////////////// Row ////////////////////////////////

//////////////////////////////// Array ////////////////////////////////

#[test]
fn array_literal() {
    insta::assert_snapshot!(render_expr("ARRAY[1, 2, 3]"), @"ARRAY[1, 2, 3]");
    insta::assert_snapshot!(render_expr("array [ 1 ,2 ]"), @"ARRAY[1, 2]");
}

#[test]
fn array_literal_empty() {
    insta::assert_snapshot!(render_expr("ARRAY[]"), @"ARRAY[]");
}

#[test]
fn array_literal_expression_elements() {
    insta::assert_snapshot!(
        render_expr("ARRAY[1 + 2, a, (SELECT 1)]"),
        @"ARRAY[1 + 2, a, (SELECT 1)]"
    );
}

#[test]
fn array_literal_nested_parses() {
    // Nested arrays are rejected later, by type analysis.
    insta::assert_snapshot!(render_expr("ARRAY[ARRAY[1]]"), @"ARRAY[ARRAY[1]]");
}

#[test]
fn array_literal_is_not_an_index() {
    // `array` is a reserved keyword: `ARRAY[1]` must build an array
    // literal, never an index into a column named `array` (which the
    // grammar used to produce, with an identical rendering).
    let expr = parse_expr("ARRAY[1]").expect("expected to parse Expr");
    assert!(matches!(expr.inner, ExprInner::Array(_)));

    // A keyword-prefixed identifier still indexes as before.
    let expr = parse_expr("arrayx[1]").expect("expected to parse Expr");
    assert!(matches!(expr.inner, ExprInner::Index(_)));
}

#[test]
fn array_keyword_quoted_is_an_identifier() {
    // Reservation itself is swept in `crate::keywords`. What is specific to
    // this form: a quoted identifier is not covered by the keyword, and renders
    // re-quoted (bare `array` would parse back as the keyword).
    insta::assert_snapshot!(render_expr(r#""array"[1]"#), @r#""array"[1]"#);
}

#[test]
fn array_literal_unclosed_is_an_error() {
    insta::assert_snapshot!(expr_err("ARRAY[1"), @"
    expression parsing error:  --> 1:8
      |
    1 | ARRAY[1
      |        ^---
      |
      = expected Escape, And, Or, ConcatInfixOp, Add, Subtract, Modulo, Multiply, Divide, Eq, Gt, GtEq, Lt, LtEq, NotEq, IndexPostfix, CastPostfix, IsPostfix, or NotFlag
    ");
}

//////////////////////////////// WindowFunction ////////////////////////////////

#[test]
fn over_inside_arithmetic() {
    insta::assert_snapshot!(
        render_expr("1 + sum(a) OVER ()"),
        @"1 + sum(a) OVER ()"
    );
}

#[test]
fn over_count_with_asterisk() {
    insta::assert_snapshot!(
        render_expr("count(*) OVER ()"),
        @"count(*) OVER ()"
    );
}

#[test]
fn over_sum_with_asterisk_error() {
    insta::assert_snapshot!(
        expr_err("sum(*) OVER ()"),
        @"invalid expression: function sum(*) does not exist"
    );
}

//////////////////////////////// FunctionCall ////////////////////////////////

#[test]
fn count_asterisk() {
    insta::assert_snapshot!(render_expr("count(*)"), @"count(*)");
}

#[test]
fn count_distinct() {
    insta::assert_snapshot!(render_expr("count(DISTINCT a)"), @"count(DISTINCT a)");
}

#[test]
fn string_agg_distinct() {
    insta::assert_snapshot!(render_expr("string_agg(DISTINCT a)"), @"string_agg(DISTINCT a)");
}

#[test]
fn distinct_outside_aggregate_fails() {
    insta::assert_snapshot!(
        expr_err("lower(DISTINCT a)"),
        @"invalid query: DISTINCT modifier is allowed only for aggregate functions"
    );
}

#[test]
fn distinct_without_arguments_fails() {
    insta::assert_snapshot!(
        expr_err("count(DISTINCT )"),
        @"expression parsing error: expression parsing stopped before: `(DISTINCT )`"
    );
}

#[test]
fn function_call_no_arguments() {
    insta::assert_snapshot!(render_expr("foo()"), @"foo()");
}

#[test]
fn function_inside_arithmetic() {
    insta::assert_snapshot!(render_expr("sum(a + 1) / 2"), @"sum(a + 1) / 2");
}

#[test]
fn function_nested_calls() {
    insta::assert_snapshot!(
        render_expr("nested(f(a), g(b, c))"),
        @"nested(f(a), g(b, c))"
    );
}

#[test]
fn asterisk_outside_count_fails() {
    insta::assert_snapshot!(
        expr_err("sum(*)"),
        @r#"invalid query: "*" is allowed only inside "count" aggregate function. Got: sum"#
    );
}

#[test]
fn count_asterisk_uses_normalized_name() {
    insta::assert_snapshot!(render_expr("COUNT(*)"), @"count(*)");
    // The quoted lowercase spelling denotes the same function after normalization
    // (the pre-normalization check compared the raw slice, quotes included, and rejected it).
    insta::assert_snapshot!(render_expr(r#""count"(*)"#), @"count(*)");
    // A quoted uppercase spelling names a different identifier, reported in its SQL spelling.
    insta::assert_snapshot!(
        expr_err(r#""COUNT"(*)"#),
        @r#"invalid query: "*" is allowed only inside "count" aggregate function. Got: "COUNT""#
    );
}

#[test]
fn distinct_aggregate_check_uses_normalized_name() {
    insta::assert_snapshot!(render_expr("SUM(DISTINCT a)"), @"sum(DISTINCT a)");
    // The quoted lowercase spelling denotes the same aggregate
    insta::assert_snapshot!(render_expr(r#""sum"(DISTINCT a)"#), @"sum(DISTINCT a)");
    insta::assert_snapshot!(
        expr_err(r#""MyAgg"(DISTINCT a)"#),
        @"invalid query: DISTINCT modifier is allowed only for aggregate functions"
    );
    // Case-preserved `"SUM"` names a different, non-aggregate identifier.
    insta::assert_snapshot!(
        expr_err(r#""SUM"(DISTINCT a)"#),
        @"invalid query: DISTINCT modifier is allowed only for aggregate functions"
    );
}

#[test]
fn parameters() {
    insta::assert_snapshot!(expr_err("$1 + ?"), @"invalid parameters usage. Got $n and ? parameters in one query!");
}

#[test]
fn parameter_0_index() {
    insta::assert_snapshot!(expr_err("$0"), @"invalid expression: parameter index must be >=1");
}

/// `?` indexes come from the source position, not from the order the parser
/// reaches them: a BETWEEN's middle operand rides inside the operator token
/// and is parsed only once the upper bound is built, so a counter would
/// number whole subtrees out of order.
#[test]
fn parameter_between_indexes() {
    insta::assert_snapshot!(render_expr("? BETWEEN ? AND ?"), @"$1 BETWEEN $2 AND $3");
    insta::assert_snapshot!(render_expr("? NOT BETWEEN ? AND ?"), @"$1 NOT BETWEEN $2 AND $3");
    insta::assert_snapshot!(
        render_expr("? BETWEEN ? + ? AND ? * ?"),
        @"$1 BETWEEN $2 + $3 AND $4 * $5"
    );
    insta::assert_snapshot!(
        render_expr("? BETWEEN (? BETWEEN ? AND ?) AND ?"),
        @"$1 BETWEEN ($2 BETWEEN $3 AND $4) AND $5"
    );
    insta::assert_snapshot!(
        render_expr("? BETWEEN ? AND ? + (SELECT ? FROM t)"),
        @"$1 BETWEEN $2 AND $3 + (SELECT $4 FROM t)"
    );
}

#[test]
fn pg_parameter_index_out_of_u16_range_is_rejected() {
    insta::assert_snapshot!(
        expr_err("$70000"),
        @"invalid expression: parameter index must be between 1 and 65536"
    );
}

//////////////////////////////// Cast ////////////////////////////////

#[test]
fn cast_postfix() {
    insta::assert_snapshot!(render_expr("a::int + 1"), @"a::int + 1");
}

#[test]
fn cast_postfix_parenthesizes_compound_child() {
    insta::assert_snapshot!(render_expr("(a + b)::text"), @"(a + b)::string");
}

#[test]
fn cast_postfix_varchar_with_length() {
    insta::assert_snapshot!(render_expr("a::varchar(10)"), @"a::string");
}

#[test]
fn varchar_len_error() {
    insta::assert_snapshot!(expr_err("a::varchar(99999999999999999999999)"), @"value parsing error: failed to parse varchar length: ParseIntError { kind: PosOverflow }.");
}

#[test]
fn varchar_len_error_in_array() {
    insta::assert_snapshot!(expr_err("a::varchar(99999999999999999999999)[]"), @"value parsing error: failed to parse varchar length: ParseIntError { kind: PosOverflow }.");
}

#[test]
fn cast_postfix_array() {
    insta::assert_snapshot!(render_expr("a::int[]"), @"a::int[]");
    insta::assert_snapshot!(render_expr("a::text[ ]"), @"a::string[]");
}

#[test]
fn cast_postfix_json_array() {
    // `json[]` is a legal cast target even though bare `::json` is not;
    // rendering must not emit `map[]` (see `CastTypeSql`).
    insta::assert_snapshot!(render_expr("a::json[]"), @"a::json[]");
}

#[test]
fn cast_postfix_then_index() {
    // Index and cast postfixes interleave (old-grammar parity): a bound
    // like `[1]` can never be a cast array suffix (`EmptyBracket` only),
    // so it attaches as an index over the cast.
    insta::assert_snapshot!(render_expr("a::int[1]"), @"a::int[1]");
    insta::assert_snapshot!(render_expr("a::int[][1]"), @"a::int[][1]");
    insta::assert_snapshot!(render_expr("a[1]::int[2]"), @"a[1]::int[2]");
}

#[test]
fn cast_postfix_array_bound_belongs_to_keyword_suffix() {
    // In the `ARRAY[N]` keyword spelling the sized bound is part of the
    // cast target (accepted and ignored), not an index postfix.
    insta::assert_snapshot!(render_expr("a::int array[1][2]"), @"a::int[][2]");
}

#[test]
fn cast_postfix_array_rejects_multidim() {
    // Multidimensional bracket typing is a DDL-only feature; `unsigned`
    // is a DDL-only domain type, not a cast target.
    insta::assert_snapshot!(expr_err("a::int[][]"), @"expression parsing error: expression parsing stopped before: `[]`");
    insta::assert_snapshot!(expr_err("a::unsigned[]"), @"expression parsing error: expression parsing stopped before: `::unsigned[]`");
}

#[test]
fn cast_op_varchar() {
    insta::assert_snapshot!(
        render_expr("CAST(a AS varchar(10))"),
        @"CAST(a AS string)"
    );
}

#[test]
fn cast_op_expression() {
    insta::assert_snapshot!(
        render_expr("CAST(1 + 2 AS double)"),
        @"CAST(1 + 2 AS double)"
    );
}

#[test]
fn cast_op_array_keyword() {
    // The `T ARRAY` spelling canonicalizes to `T[]`; a sized bound is
    // accepted and ignored (old-pipeline parity).
    insta::assert_snapshot!(render_expr("CAST(a AS int ARRAY)"), @"CAST(a AS int[])");
    insta::assert_snapshot!(render_expr("CAST(a AS int ARRAY[5])"), @"CAST(a AS int[])");
}

#[test]
fn cast_op_array_brackets() {
    insta::assert_snapshot!(render_expr("CAST(a AS varchar(10)[])"), @"CAST(a AS string[])");
}

#[test]
fn scalar_casts_datetime_decimal_uuid() {
    insta::assert_snapshot!(render_expr("a::datetime"), @"a::datetime");
    insta::assert_snapshot!(render_expr("a::decimal"), @"a::decimal");
    insta::assert_snapshot!(render_expr("a::uuid"), @"a::uuid");
}

#[test]
fn array_casts_element_types() {
    insta::assert_snapshot!(render_expr("a::bool[]"), @"a::bool[]");
    insta::assert_snapshot!(render_expr("a::datetime[]"), @"a::datetime[]");
    insta::assert_snapshot!(render_expr("a::decimal[]"), @"a::decimal[]");
    insta::assert_snapshot!(render_expr("a::double[]"), @"a::double[]");
    insta::assert_snapshot!(render_expr("a::uuid[]"), @"a::uuid[]");
}

//////////////////////////////// Like ////////////////////////////////

#[test]
fn like_simple() {
    insta::assert_snapshot!(render_expr("name LIKE 'a%'"), @"name LIKE 'a%'");
}

#[test]
fn ilike_simple() {
    insta::assert_snapshot!(render_expr("name ILIKE 'a%'"), @"name ILIKE 'a%'");
}

#[test]
fn like_with_escape() {
    insta::assert_snapshot!(
        render_expr(r#"name LIKE 'a\%' ESCAPE '\'"#),
        @r"name LIKE 'a\%' ESCAPE '\'"
    );
}

#[test]
fn escape_without_like_fails() {
    insta::assert_snapshot!(
        expr_err("'a' ESCAPE 'b'"),
        @"invalid expression: ESCAPE can go only after LIKE or SIMILAR expressions, got: Literal"
    );
}

#[test]
fn like_escape_specified_twice_is_rejected() {
    insta::assert_snapshot!(
        expr_err("a LIKE b ESCAPE 'x' ESCAPE 'y'"),
        @"invalid expression: escape specified twice: expr1 LIKE/SIMILAR expr2 ESCAPE expr3 ESCAPE expr4"
    );
}

//////////////////////////////// Similar ////////////////////////////////

#[test]
fn similar_with_escape() {
    insta::assert_snapshot!(
        render_expr("name SIMILAR 'a_' ESCAPE 'x'"),
        @"name SIMILAR 'a_' ESCAPE 'x'"
    );
}

//////////////////////////////// Between ////////////////////////////////

#[test]
fn between_builds_between_node() {
    let expr = parse_expr("a BETWEEN 1 AND 2").expect("expected to parse Expr");
    assert!(matches!(expr.inner, ExprInner::Between(_)));
}

#[test]
fn between_simple() {
    insta::assert_snapshot!(
        render_expr("a BETWEEN 1 AND 10"),
        @"a BETWEEN 1 AND 10"
    );
}

#[test]
fn not_between_under_or() {
    insta::assert_snapshot!(
        render_expr("a NOT BETWEEN 1 AND 10 OR b = 1"),
        @"a NOT BETWEEN 1 AND 10 OR b = 1"
    );
}

#[test]
fn between_with_trailing_and() {
    insta::assert_snapshot!(
        render_expr("x = 1 AND a BETWEEN 1 AND 5 AND y = 2"),
        @"x = 1 AND a BETWEEN 1 AND 5 AND y = 2"
    );
}

#[test]
fn between_under_not() {
    insta::assert_snapshot!(
        render_expr("NOT a BETWEEN 1 AND 2"),
        @"NOT a BETWEEN 1 AND 2"
    );
}

#[test]
fn between_middle_arithmetic() {
    insta::assert_snapshot!(
        render_expr("a BETWEEN 1 + 2 * 3 AND 10"),
        @"a BETWEEN 1 + 2 * 3 AND 10"
    );
}

#[test]
fn between_middle_comparison() {
    insta::assert_snapshot!(
        render_expr("a BETWEEN b = c AND d"),
        @"a BETWEEN b = c AND d"
    );
}

#[test]
fn between_middle_parenthesized_and() {
    insta::assert_snapshot!(
        render_expr("a BETWEEN (b AND c) AND d"),
        @"a BETWEEN (b AND c) AND d"
    );
}

#[test]
fn between_compound_left_operand() {
    insta::assert_snapshot!(
        render_expr("a + b BETWEEN 1 AND 10"),
        @"a + b BETWEEN 1 AND 10"
    );
}

#[test]
fn between_operators_in_argument() {
    insta::assert_snapshot!(render_expr("true BETWEEN (('a' LIKE 'b') = true) AND true"), @"true BETWEEN ('a' LIKE 'b' = true) AND true");
    insta::assert_snapshot!(render_expr("a BETWEEN (b LIKE c) = d AND e"), @"a BETWEEN (b LIKE c = d) AND e");
    insta::assert_snapshot!(render_expr("a BETWEEN (b BETWEEN c AND d) = e AND f"), @"a BETWEEN (b BETWEEN c AND d = e) AND f");
    insta::assert_snapshot!(render_expr("a BETWEEN ((b LIKE c) IS NULL) = d AND e"), @"a BETWEEN (b LIKE c IS NULL) = d AND e");
}

// Complete BETWEEN inside compound primaries whose inner expressions are
// parsed by their own `parse_expr` calls.
#[test]
fn between_inside_index_postfix() {
    insta::assert_snapshot!(
        render_expr("a[b BETWEEN 1 AND 2]"),
        @"a[b BETWEEN 1 AND 2]"
    );
}

#[test]
fn between_inside_case_condition() {
    insta::assert_snapshot!(
        render_expr("CASE WHEN a BETWEEN 1 AND 2 THEN 3 END"),
        @"CASE WHEN a BETWEEN 1 AND 2 THEN 3 END"
    );
}

#[test]
fn between_inside_trim_pattern() {
    insta::assert_snapshot!(
        render_expr("TRIM(a BETWEEN 'x' AND 'y' FROM name)"),
        @"TRIM(a BETWEEN 'x' AND 'y' FROM name)"
    );
}

#[test]
fn between_inside_substring_from() {
    insta::assert_snapshot!(
        render_expr("SUBSTRING(s FROM a BETWEEN 1 AND 2)"),
        @"SUBSTRING(s FROM a BETWEEN 1 AND 2)"
    );
}

#[test]
fn between_inside_cast_arg() {
    insta::assert_snapshot!(
        render_expr("CAST(1 BETWEEN 2 AND 4 AS int)"),
        @"CAST(1 BETWEEN 2 AND 4 AS int)"
    );
}

// A dangling BETWEEN (no upper-bound `AND`) must be rejected in every
// expression context: the grammar cannot match `BETWEEN <middle> AND` as
// an operator, so the leftover input breaks the enclosing rule.

#[test]
fn between_without_and_fails() {
    insta::assert_snapshot!(
        expr_err("a BETWEEN 1"),
        @"expression parsing error: expression parsing stopped before: `BETWEEN 1`"
    );
}

#[test]
fn between_without_and_under_or_fails() {
    insta::assert_snapshot!(
        expr_err("a BETWEEN 1 OR b"),
        @"expression parsing error: expression parsing stopped before: `BETWEEN 1 OR b`"
    );
}

// LIKE binds looser than BETWEEN, so it cannot appear (unparenthesized) in the middle operand.
#[test]
fn between_middle_like_fails() {
    insta::assert_snapshot!(
        expr_err("a BETWEEN b LIKE c AND d"),
        @"expression parsing error: expression parsing stopped before: `BETWEEN b LIKE c AND d`"
    );
}

#[test]
fn between_without_and_in_paren_fails() {
    insta::assert_snapshot!(
        expr_err("(1 BETWEEN 2) AND true"),
        @"
    expression parsing error:  --> 1:13
      |
    1 | (1 BETWEEN 2) AND true
      |             ^---
      |
      = expected EOI, ConcatInfixOp, Add, Subtract, Modulo, Multiply, Divide, Eq, Gt, GtEq, Lt, LtEq, NotEq, IndexPostfix, or CastPostfix
    "
    );
}

#[test]
fn between_without_and_in_func_call_fails() {
    insta::assert_snapshot!(
        expr_err("abs(1 BETWEEN 2)"),
        @"expression parsing error: expression parsing stopped before: `(1 BETWEEN 2)`"
    );
}

#[test]
fn between_without_and_in_cast_arg_fails() {
    insta::assert_snapshot!(
        expr_err("CAST(1 BETWEEN 2 AS int)"),
        @"
    expression parsing error:  --> 1:18
      |
    1 | CAST(1 BETWEEN 2 AS int)
      |                  ^---
      |
      = expected ConcatInfixOp, Add, Subtract, Modulo, Multiply, Divide, Eq, Gt, GtEq, Lt, LtEq, or NotEq
    "
    );
}

#[test]
fn between_without_and_in_index_fails() {
    insta::assert_snapshot!(
        expr_err("a[1 BETWEEN 2]"),
        @"expression parsing error: expression parsing stopped before: `[1 BETWEEN 2]`"
    );
}

#[test]
fn between_without_and_in_case_condition_fails() {
    insta::assert_snapshot!(
        expr_err("CASE WHEN 1 BETWEEN 2 THEN 3 END"),
        @"
    expression parsing error:  --> 1:23
      |
    1 | CASE WHEN 1 BETWEEN 2 THEN 3 END
      |                       ^---
      |
      = expected ConcatInfixOp, Add, Subtract, Modulo, Multiply, Divide, Eq, Gt, GtEq, Lt, LtEq, or NotEq
    "
    );
}

#[test]
fn between_without_and_in_case_search_fails() {
    insta::assert_snapshot!(
        expr_err("CASE 1 BETWEEN 2 WHEN true THEN 3 END"),
        @"
    expression parsing error:  --> 1:18
      |
    1 | CASE 1 BETWEEN 2 WHEN true THEN 3 END
      |                  ^---
      |
      = expected ConcatInfixOp, Add, Subtract, Modulo, Multiply, Divide, Eq, Gt, GtEq, Lt, LtEq, or NotEq
    "
    );
}

#[test]
fn between_without_and_in_trim_fails() {
    insta::assert_snapshot!(
        expr_err("TRIM('x' BETWEEN 'y' FROM name)"),
        @"expression parsing error: expression parsing stopped before: `('x' BETWEEN 'y' FROM name)`"
    );
}

#[test]
fn between_without_and_in_substring_fails() {
    insta::assert_snapshot!(
        expr_err("SUBSTRING(s FROM 1 BETWEEN 2)"),
        @"
    expression parsing error:  --> 1:29
      |
    1 | SUBSTRING(s FROM 1 BETWEEN 2)
      |                             ^---
      |
      = expected EOI, ConcatInfixOp, Add, Subtract, Modulo, Multiply, Divide, Eq, Gt, GtEq, Lt, LtEq, NotEq, IndexPostfix, or CastPostfix
    "
    );
}

//////////////////////////////// In ////////////////////////////////

#[test]
fn in_values_row() {
    insta::assert_snapshot!(render_expr("a IN (1, 2, 3)"), @"a IN (1, 2, 3)");
}

#[test]
fn not_in_subquery() {
    insta::assert_snapshot!(
        render_expr("a NOT IN (SELECT b FROM t)"),
        @"a NOT IN (SELECT b FROM t)"
    );
}

#[test]
fn in_non_row_fails() {
    insta::assert_snapshot!(
        expr_err("a IN b"),
        @"In expression must have query or a list of values as right child"
    );
}

//////////////////////////////// Is ////////////////////////////////

#[test]
fn is_null() {
    insta::assert_snapshot!(render_expr("a IS NULL"), @"a IS NULL");
}

#[test]
fn is_not_true() {
    insta::assert_snapshot!(render_expr("a IS NOT TRUE"), @"a IS NOT TRUE");
}

#[test]
fn is_unknown_renders_as_null() {
    insta::assert_snapshot!(render_expr("a IS UNKNOWN"), @"a IS NULL");
}

#[test]
fn is_chained() {
    insta::assert_snapshot!(
        render_expr("a IS NULL IS NOT FALSE"),
        @"a IS NULL IS NOT FALSE"
    );
}

#[test]
fn postfix_chain() {
    insta::assert_snapshot!(
        render_expr("m['key']::int IS NOT NULL"),
        @"m['key']::int IS NOT NULL"
    );
}

//////////////////////////////// Index ////////////////////////////////

#[test]
fn index_postfix() {
    insta::assert_snapshot!(render_expr("a[1][b + 2]"), @"a[1][b + 2]");
}

//////////////////////////////// Trim ////////////////////////////////

#[test]
fn trim_plain() {
    insta::assert_snapshot!(render_expr("TRIM(name)"), @"TRIM(name)");
}

#[test]
fn trim_kind_only() {
    insta::assert_snapshot!(
        render_expr("TRIM(LEADING FROM name)"),
        @"TRIM(LEADING FROM name)"
    );
}

#[test]
fn trim_kind_and_pattern() {
    insta::assert_snapshot!(
        render_expr("TRIM(BOTH 'x' FROM name)"),
        @"TRIM(BOTH 'x' FROM name)"
    );
}

#[test]
fn trim_trailing_kind() {
    insta::assert_snapshot!(
        render_expr("TRIM(TRAILING 'x' FROM name)"),
        @"TRIM(TRAILING 'x' FROM name)"
    );
}

#[test]
fn trim_pattern_only() {
    insta::assert_snapshot!(
        render_expr("TRIM('x' FROM name)"),
        @"TRIM('x' FROM name)"
    );
}

//////////////////////////////// Substring ////////////////////////////////

#[test]
fn substring_from_for() {
    insta::assert_snapshot!(
        render_expr("SUBSTRING(s FROM 2 FOR 3)"),
        @"SUBSTRING(s FROM 2 FOR 3)"
    );
}

#[test]
fn substring_regular() {
    insta::assert_snapshot!(
        render_expr("SUBSTRING(s, 2, 3)"),
        @"SUBSTRING(s, 2, 3)"
    );
}

#[test]
fn substring_for() {
    insta::assert_snapshot!(render_expr("SUBSTRING(s FOR 3)"), @"SUBSTRING(s FOR 3)");
}

#[test]
fn substring_from_comma_form_normalized() {
    insta::assert_snapshot!(render_expr("SUBSTRING(s, 2)"), @"SUBSTRING(s FROM 2)");
}

#[test]
fn substring_similar_with_escape() {
    insta::assert_snapshot!(
        render_expr(r#"SUBSTRING('abcdef' SIMILAR '%#"c_f#"%' ESCAPE '#')"#),
        @r#"SUBSTRING('abcdef' SIMILAR '%#"c_f#"%' ESCAPE '#')"#
    );
}

#[test]
fn substring_similar_without_escape() {
    // Parses like the old grammar; the old pipeline's "missing escape
    // symbol" rejection is an IR-build-time (analysis) concern.
    insta::assert_snapshot!(
        render_expr("SUBSTRING(a SIMILAR b)"),
        @"SUBSTRING(a SIMILAR b)"
    );
}

#[test]
fn substring_single_expr_is_similar_fallback() {
    // The fallback variant admits any single expression (old-grammar
    // parity); arity rejection is an analysis concern.
    insta::assert_snapshot!(render_expr("SUBSTRING(s)"), @"SUBSTRING(s)");
}

//////////////////////////////// Case ////////////////////////////////

#[test]
fn case_searched() {
    insta::assert_snapshot!(
        render_expr("CASE WHEN a > 1 THEN 1 ELSE 2 END"),
        @"CASE WHEN a > 1 THEN 1 ELSE 2 END"
    );
}

#[test]
fn case_with_search_expr() {
    insta::assert_snapshot!(
        render_expr("CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END"),
        @"CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END"
    );
}

//////////////////////////////// Exists ////////////////////////////////

#[test]
fn exists_simple() {
    insta::assert_snapshot!(
        render_expr("EXISTS (SELECT a FROM t)"),
        @"EXISTS (SELECT a FROM t)"
    );
}

#[test]
fn not_exists() {
    insta::assert_snapshot!(
        render_expr("NOT EXISTS (SELECT a FROM t)"),
        @"NOT EXISTS (SELECT a FROM t)"
    );
}

//////////////////////////////// TimeFunction ////////////////////////////////

#[test]
fn current_date() {
    insta::assert_snapshot!(render_expr("CURRENT_DATE"), @"CURRENT_DATE");
}

#[test]
fn current_time_builtin() {
    insta::assert_snapshot!(render_expr("CURRENT_TIME"), @"CURRENT_TIME");
    insta::assert_snapshot!(render_expr("CURRENT_TIME(2)"), @"CURRENT_TIME(2)");
}

#[test]
fn current_timestamp_with_precision() {
    insta::assert_snapshot!(
        render_expr("current_timestamp(3)"),
        @"CURRENT_TIMESTAMP(3)"
    );
}

#[test]
fn localtimestamp_no_precision() {
    insta::assert_snapshot!(render_expr("LOCALTIMESTAMP"), @"LOCALTIMESTAMP");
}

#[test]
fn localtime_simple() {
    insta::assert_snapshot!(render_expr("localtime"), @"LOCALTIME");
}

// The niladics' token boundaries (`current_date_col`, `localtime_ms`, ...) live
// in `crate::keywords`.
