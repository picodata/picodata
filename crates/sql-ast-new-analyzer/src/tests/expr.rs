//! Expressions: what a column reference resolves to, and what type the
//! expression built on it derives.
//!
//! # Cases
//! The first two sections cover resolution — the name-to-column step every
//! other section depends on — ordered by how far the lookup has to travel:
//! within one query level first, then outward through nested statements.
//!
//! The rest walk the expression forms one at a time. A case renders the
//! analyzed expression and pins it whole, so the `::type` suffixes and the
//! `CAST` calls materialized for implicit coercions are part of the contract:
//! an expression accepted at the wrong type fails as loudly as one rejected
//! outright.

use super::{analyze_error, analyzed};
use sql_ir::ir::types::{DerivedType, UnrestrictedType};

// ------------- Column references -------------

#[test]
fn column_ref() {
    let query = "SELECT t1.a, b FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.a::int, t1.b::int FROM t1");
}

#[test]
fn incosistent_table_qualifier() {
    let query = "SELECT t.a FROM t1";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @"failed to analyze AST: cannot resolve column reference 't.a'"
    );
}

#[test]
fn qualifier_reference_to_nonexisting_table() {
    let query = "SELECT bogus.a FROM (SELECT 1 AS a)";
    insta::assert_snapshot!(analyze_error(query, &[]), @"failed to analyze AST: cannot resolve column reference 'bogus.a'");
}

#[test]
fn qualified_column_resolution_multiple_satisfy() {
    let query = "SELECT (SELECT t2.a FROM t2 AS x), (SELECT a FROM t2) FROM t1 as t2";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (SELECT t2.a::int FROM t2 AS x)::int, (SELECT t2.a::double FROM t2)::double FROM t1 AS t2");
}

#[test]
fn ambigious_column_reference_subquery() {
    let query = "SELECT a FROM (SELECT 1 AS a, 2 AS a)";
    insta::assert_snapshot!(analyze_error(query, &[]), @"failed to analyze AST: column reference 'a' is ambigious");
}

#[test]
fn ambigious_qualified_column_reference_subquery() {
    let query = "SELECT x.a FROM (SELECT 1 AS a, 2 AS a) AS x";
    insta::assert_snapshot!(analyze_error(query, &[]), @"failed to analyze AST: column reference 'x.a' is ambigious");
}

// ------------- Scopes a subquery sees -------------

#[test]
fn resolve_relation_in_scalar_subquery() {
    let query = "SELECT (SELECT e FROM t2) FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (SELECT t2.e::bool FROM t2)::bool FROM t1");
}

#[test]
fn resolve_relation_in_scalar_subquery_before() {
    let query = "SELECT (SELECT e FROM t2), a FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (SELECT t2.e::bool FROM t2)::bool, t1.a::int FROM t1");
}

#[test]
fn resolve_relation_in_scalar_subquery_after() {
    let query = "SELECT a, (SELECT e FROM t2) FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.a::int, (SELECT t2.e::bool FROM t2)::bool FROM t1");
}

#[test]
fn resolve_relation_in_scalar_subquery_among() {
    let query = "SELECT *, (SELECT e FROM t2), * FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.a::int, t1.b::int, t1.c::double, t1.d::decimal, t1.e::string, t1.f::string, t1.g::bool, (SELECT t2.e::bool FROM t2)::bool, t1.a::int, t1.b::int, t1.c::double, t1.d::decimal, t1.e::string, t1.f::string, t1.g::bool FROM t1");
}

#[test]
fn correlated_subquery_simple() {
    let query = "SELECT (SELECT a) FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (SELECT t1.a::int)::int FROM t1");
}

#[test]
fn correlated_subqueries() {
    // Neither enclosing scope projects `c`, so it resolves two scopes outward.
    let query = "SELECT (SELECT (SELECT c FROM (SELECT 1 AS x)) FROM (SELECT 2 AS y)) FROM t1;";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (SELECT (SELECT t1.c::double FROM (SELECT 1::int AS x))::double FROM (SELECT 2::int AS y))::double FROM t1");
}

#[test]
fn correlated_subqueries_mixed() {
    // `c` in the innermost projection correlates all the way out to t1, and the
    // scope above it then reads that projection's own column of the same name.
    let query =
        "SELECT (SELECT (SELECT c FROM (SELECT *, c FROM (SELECT 1 AS x))) FROM (SELECT 2 AS y)) FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (SELECT (SELECT c::double FROM (SELECT x::int, t1.c::double FROM (SELECT 1::int AS x)))::double FROM (SELECT 2::int AS y))::double FROM t1");
}

#[test]
fn correlated_subqueries_mixed_multiple_satisfy() {
    // Both scopes have an `a`; the innermost reference takes the nearest one.
    let query = "SELECT (SELECT (SELECT a) FROM t1), a FROM t2;";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (SELECT (SELECT t1.a::int)::int FROM t1)::int, t2.a::double FROM t2");
}

#[test]
fn correlated_subqueries_mixed_multiple_satisfy_qualified() {
    // The inner `x` projects `a` alone and shadows the outer one, so the
    // qualified reference must not reach t2.c.
    let query = "SELECT (SELECT x.c FROM (SELECT a FROM t1) AS x) FROM t2 AS x;";
    insta::assert_snapshot!(analyze_error(query, &[]), @"failed to analyze AST: cannot resolve column reference 'x.c'");
}

#[test]
fn correlated_subqueries_qualified_resolves_outward() {
    // No relation in the inner scope matches `x`, so the qualified
    // correlated reference resolves against the enclosing scope.
    let query = "SELECT (SELECT x.e FROM t1) FROM t2 AS x;";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (SELECT x.e::bool FROM t1)::bool FROM t2 AS x");
}

#[test]
fn correlated_subqueries_qualified_shadowing() {
    // Both scopes name a relation `x`, and only the outer one (t1) has an
    // `a` column - the inner one projects `b` alone. The inner `x` anchors the
    // qualified reference and shadows the outer one, so resolution must fail
    // rather than reach t1.a.
    let query = "SELECT (SELECT x.a FROM (SELECT b FROM t2) AS x) FROM t1 AS x;";
    insta::assert_snapshot!(analyze_error(query, &[]), @"failed to analyze AST: cannot resolve column reference 'x.a'");
}

// ------------- Type derivation -------------

#[test]
fn literal() {
    let query = "SELECT 1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT 1::int");
}

#[test]
fn incompatible_type() {
    let query = "SELECT e + b FROM t1";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @"could not resolve operator overload for +(text, int)"
    );
}

#[test]
fn binary_operation() {
    let query = "SELECT 1 + 2, 1.0, 2 * 3.0";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (1::int + 2::int)::int, 1.0::decimal, (CAST(2 AS decimal) * 3.0::decimal)::decimal");
}

#[test]
fn coerce_str_literal() {
    let query = "SELECT 1 + '1'";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (1::int + 1::int)::int");
}

#[test]
fn coerce_numerics_literals() {
    let query = "SELECT 1 + 1.5";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (CAST(1 AS decimal) + 1.5::decimal)::decimal");
}

#[test]
fn explicit_cast_child_reduced() {
    let query = "SELECT CAST(1 = 2 AS bool)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (1::int = 2::int)::bool");
}

#[test]
fn explicit_postfix_cast_child_not_annotated() {
    let query = "SELECT (1 = 2)::bool";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (1::int = 2::int)::bool");
}

// ------------- Cast legality -------------

#[test]
fn cast_without_a_conversion_path_error() {
    for (query, expected) in [
        ("SELECT d::bool FROM t1", "decimal to bool"),
        ("SELECT c::bool FROM t1", "double to bool"),
        ("SELECT c::uuid FROM t1", "double to uuid"),
        ("SELECT a::uuid FROM t1", "int to uuid"),
        ("SELECT g::uuid FROM t1", "bool to uuid"),
        ("SELECT true::decimal", "bool to decimal"),
        ("SELECT CAST(true AS double)", "bool to double"),
        ("SELECT CAST(1 AS datetime)", "int to datetime"),
        ("SELECT f::int FROM t2", "datetime to int"),
        ("SELECT f::bool FROM t2", "datetime to bool"),
        ("SELECT f::uuid FROM t2", "datetime to uuid"),
    ] {
        assert_eq!(
            analyze_error(query, &[]),
            format!("failed to analyze AST: cannot cast type {expected}"),
            "for `{query}`"
        );
    }
}

#[test]
fn cast_with_a_conversion_path() {
    for query in [
        "SELECT e::bool FROM t1",
        "SELECT e::uuid FROM t1",
        "SELECT e::datetime FROM t1",
        "SELECT e::decimal FROM t1",
        "SELECT f::string FROM t2",
        "SELECT g::string FROM t1",
        "SELECT a::decimal FROM t1",
        "SELECT c::int FROM t1",
        "SELECT g::int FROM t1",
        "SELECT 1::bool",
        "SELECT true::int",
        "SELECT a::string::bool FROM t1",
    ] {
        analyzed(query, &[]);
    }
}

/// Picodata can cast `bool` into `int`.
#[test]
fn cast_bool_to_int_is_a_declared_int_width_divergence() {
    insta::assert_snapshot!(analyzed("SELECT g::int FROM t1", &[]), @"SELECT t1.g::int FROM t1");
    insta::assert_snapshot!(analyzed("SELECT a::bool FROM t1", &[]), @"SELECT t1.a::bool FROM t1");
}

#[test]
fn cast_of_a_non_scalar_is_not_ruled_on() {
    insta::assert_snapshot!(analyzed("SELECT b::int[] FROM t1", &[]), @"SELECT t1.b::int[] FROM t1");
}

// ------------- Text literal folding -------------

#[test]
fn text_literal_that_is_not_valid_input_error() {
    for (query, expected) in [
        ("SELECT 'x'::int", r#"int: "x""#),
        ("SELECT CAST('x' AS bool)", r#"bool: "x""#),
        ("SELECT '1.5'::int", r#"int: "1.5""#),
        ("SELECT 'x'::uuid", r#"uuid: "x""#),
        ("SELECT '1'::uuid", r#"uuid: "1""#),
        ("SELECT 'x'::datetime", r#"datetime: "x""#),
        ("SELECT 'x' + 1", r#"int: "x""#),
        ("SELECT '' + 1", r#"int: """#),
        ("SELECT 'it''s' + 1", r#"int: "it's""#),
        ("SELECT ' 1 '::int", r#"int: " 1 ""#),
    ] {
        assert_eq!(
            analyze_error(query, &[]),
            format!("failed to analyze AST: invalid input syntax for type {expected}"),
            "for `{query}`"
        );
    }
}

#[test]
fn text_literal_is_folded_in_place() {
    insta::assert_snapshot!(analyzed("SELECT '1'::int", &[]), @"SELECT 1::int");
    insta::assert_snapshot!(analyzed("SELECT '1.5'::decimal", &[]), @"SELECT 1.5::decimal");
    insta::assert_snapshot!(analyzed("SELECT 'true'::bool", &[]), @"SELECT true::bool");
    insta::assert_snapshot!(analyzed("SELECT 'false'::bool", &[]), @"SELECT false::bool");
    insta::assert_snapshot!(
        analyzed("SELECT a = '1' FROM t1", &[]),
        @"SELECT (t1.a::int = 1::int)::bool FROM t1"
    );
}

#[test]
fn text_literal_that_cannot_be_rewritten_keeps_its_cast() {
    insta::assert_snapshot!(analyzed("SELECT '01'::int", &[]), @"SELECT CAST('01' AS int)");
    insta::assert_snapshot!(analyzed("SELECT '1.0'::double", &[]), @"SELECT CAST('1.0' AS double)");
    insta::assert_snapshot!(analyzed("SELECT 't'::bool", &[]), @"SELECT CAST('t' AS bool)");
    insta::assert_snapshot!(
        analyzed("SELECT '2020-01-01'::datetime", &[]),
        @"SELECT CAST('2020-01-01' AS datetime)"
    );
    insta::assert_snapshot!(
        analyzed("SELECT '00000000-0000-0000-0000-000000000000'::uuid", &[]),
        @"SELECT CAST('00000000-0000-0000-0000-000000000000' AS uuid)"
    );
    insta::assert_snapshot!(analyzed("SELECT 'x'::string", &[]), @"SELECT 'x'::string");
}

// ------------- Scalar subquery -------------

#[test]
fn subquery_expr() {
    let query = "SELECT (SELECT 1) AS a";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (SELECT 1::int)::int AS a");
}

#[test]
fn subquery_expr_multiple_columns() {
    let query = "SELECT (SELECT 1, 2, 3) AS a";
    insta::assert_snapshot!(analyze_error(query, &[]), @"failed to analyze AST: subquery must return only one column");
}

#[test]
fn scalar_subquery_arity_error() {
    let query = "SELECT (SELECT 1,2) = (SELECT 1)";
    insta::assert_snapshot!(analyze_error(query, &[]), @"failed to analyze AST: subquery must return only one column");
}

#[test]
fn coerce_with_scalar_subquery() {
    let query = "SELECT (SELECT 1) + 2.5";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (CAST((SELECT 1::int) AS decimal) + 2.5::decimal)::decimal");
}

#[test]
fn arithmetic_ops_with_subquery_expr() {
    let query = "SELECT (SELECT c FROM t2) + 2.5 * b FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (CAST((SELECT t2.c::int FROM t2) AS decimal) + (2.5::decimal * CAST(t1.b AS decimal))::decimal)::decimal FROM t1");
}

#[test]
fn scalar_subquery_equality() {
    let query = "SELECT (SELECT 1) = (SELECT 1)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT ((SELECT 1::int) = (SELECT 1::int))::bool");
}

#[test]
fn scalar_subquery_inequality() {
    let query = "SELECT (SELECT a FROM t1) < (SELECT b FROM t1)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT ((SELECT t1.a::int FROM t1) < (SELECT t1.b::int FROM t1))::bool");
}

// ------------- Parameters -------------

#[test]
fn parameter_types_from_client_are_positional() {
    // `$n` reads the n-th supplied type, whatever order the query mentions
    // parameters in: the SQL spelling counts from one, the type system's
    // parameter vector from zero.
    let query = "SELECT $2, $1";
    let param_types = [
        DerivedType::new(UnrestrictedType::Integer),
        DerivedType::new(UnrestrictedType::String),
    ];
    insta::assert_snapshot!(
        analyzed(query, &param_types),
        @"SELECT $2::string, $1::int"
    );
}

#[test]
fn parameter_type_inferred_from_operand() {
    // No type supplied for `$1`, so it takes the one the context demands —
    // here the column it is compared against.
    let query = "SELECT b = $1, e = $2 FROM t1";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT (t1.b::int = $1::int)::bool, (t1.e::string = $2::string)::bool FROM t1"
    );
}

#[test]
fn parameter_type_unknown_defaults_to_text() {
    // Nothing to infer from and nothing supplied: an untyped parameter
    // defaults to text, the same rule `SELECT NULL` follows. An `unknown`
    // entry in the client's types is no different from an absent one.
    let query = "SELECT $1, $2";
    insta::assert_snapshot!(
        analyzed(query, &[DerivedType::unknown()]),
        @"SELECT $1::string, $2::string"
    );
}

#[test]
fn parameter_of_client_type_is_coerced() {
    // `$1` is fixed to int by the client, so it is the operand that gets
    // coerced to the operator's decimal — materialized as a CAST, exactly
    // like the coercion of an int literal in `1 + 1.5`.
    let query = "SELECT $1 + 1.5";
    insta::assert_snapshot!(
        analyzed(query, &[DerivedType::new(UnrestrictedType::Integer)]),
        @"SELECT (CAST($1 AS decimal) + 1.5::decimal)::decimal"
    );
}

#[test]
fn parameter_inconsistent_types_error() {
    // One occurrence wants text and the other int; a parameter has a single
    // type, so this is rejected rather than resolved per occurrence.
    let query = "SELECT $1 = e AND $1 = b FROM t1";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @"inconsistent types int and text deduced for parameter $1, consider using transitive type casts through a common type, e.g. $1::int::text and $1::int"
    );
}

// ------------- Arrays -------------

#[test]
fn array_literal_type() {
    let query = "SELECT ARRAY[1, 2, 3]";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT ARRAY[1::int, 2::int, 3::int]::int[]");
}

#[test]
fn array_literal_element_widening() {
    // `int` widens to the common `decimal` element type.
    let query = "SELECT ARRAY[1, 1.5]";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT ARRAY[CAST(1 AS decimal), 1.5::decimal]::decimal[]");
}

#[test]
fn array_literal_string_literal_coerces_to_peer() {
    let query = "SELECT ARRAY[1, '3']";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT ARRAY[1::int, 3::int]::int[]");
}

#[test]
fn array_literal_null_element() {
    let query = "SELECT ARRAY[NULL]";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT ARRAY[NULL::string]::string[]");
}

#[test]
fn array_literal_empty_defaults_to_text() {
    let query = "SELECT ARRAY[]";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT ARRAY[]::string[]");
}

#[test]
fn array_literal_empty_takes_element_type_from_cast() {
    let query = "SELECT ARRAY[]::int[]";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT ARRAY[]::int[]");
}

#[test]
fn array_literal_of_column_refs() {
    let query = "SELECT ARRAY[a, b] FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT ARRAY[t1.a::int, t1.b::int]::int[] FROM t1");
}

#[test]
fn array_literal_of_text_column() {
    let query = "SELECT ARRAY[e] FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT ARRAY[t1.e::string]::string[] FROM t1");
}

#[test]
fn array_cast_on_column() {
    let query = "SELECT b::int[] FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.b::int[] FROM t1");
}

#[test]
fn array_literal_unmatchable_elements_error() {
    let query = "SELECT ARRAY[1, true]";
    insta::assert_snapshot!(analyze_error(query, &[]), @"ARRAY types int and bool cannot be matched");
}

#[test]
fn array_literal_nested_error() {
    let query = "SELECT ARRAY[ARRAY[1]]";
    insta::assert_snapshot!(analyze_error(query, &[]), @"nested arrays are not supported");
}

// ------------- Index -------------

#[test]
fn array_index_type() {
    insta::assert_snapshot!(
        analyzed("SELECT ARRAY[1, 2][1]", &[]),
        @"SELECT ARRAY[1::int, 2::int]::int[][1::int]::int"
    );
}

#[test]
fn array_index_chain_type() {
    // The whole bracket run types as one IndexChain; only the outermost
    // node carries a type.
    insta::assert_snapshot!(
        analyzed("SELECT ARRAY[1, 2][1][2]", &[]),
        @"SELECT ARRAY[1::int, 2::int]::int[][1::int][2::int]::int"
    );
}

#[test]
fn index_non_indexable_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT b[1] FROM t1", &[]),
        @"cannot index expression of type int"
    );
}

#[test]
fn index_bad_key_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT ARRAY[1, 2][g] FROM t1", &[]),
        @"could not resolve operator overload for [](int[], bool)"
    );
}

// ------------- Row -------------

#[test]
fn row_comparison() {
    insta::assert_snapshot!(
        analyzed("SELECT (a, b) = (1, 2) FROM t1", &[]),
        @"SELECT ((t1.a::int, t1.b::int) = (1::int, 2::int))::bool FROM t1"
    );
}

#[test]
fn row_standalone_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT (1, 2)", &[]),
        @"row value misused"
    );
}

#[test]
fn row_length_mismatch_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT (a, b) = (1, 2, 3) FROM t1", &[]),
        @"unequal number of entries in row expression: 2 and 3"
    );
}

// ------------- Unary operations -------------

#[test]
fn not_type() {
    insta::assert_snapshot!(
        analyzed("SELECT NOT g FROM t1", &[]),
        @"SELECT (NOT t1.g::bool)::bool FROM t1"
    );
}

#[test]
fn not_not_type() {
    insta::assert_snapshot!(
        analyzed("SELECT NOT NOT g FROM t1", &[]),
        @"SELECT (NOT (NOT t1.g::bool)::bool)::bool FROM t1"
    );
}

#[test]
fn not_non_boolean_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT NOT b FROM t1", &[]),
        @"argument of NOT must be type boolean, not type int"
    );
}

#[test]
fn unary_minus() {
    insta::assert_snapshot!(
        analyzed("SELECT - 1 + 1", &[]),
        @"SELECT ((- 1::int)::int + 1::int)::int"
    );
}

#[test]
fn unary_plus() {
    insta::assert_snapshot!(
        analyzed("SELECT + 1 + 1", &[]),
        @"SELECT ((+ 1::int)::int + 1::int)::int"
    );
}

// A sign adjacent to a number is part of the literal token (that keeps
// i64::MIN spellable); the operator only appears for detached signs.
#[test]
fn adjacent_sign_folds_into_literal() {
    insta::assert_snapshot!(
        analyzed("SELECT -1, - 1", &[]),
        @"SELECT -1::int, (- 1::int)::int"
    );
}

// The arity-1 operator entries: int, double and decimal each negate to
// themselves, as in PG.
#[test]
fn unary_minus_numeric_types() {
    insta::assert_snapshot!(
        analyzed("SELECT -a, -c, -d FROM t1", &[]),
        @"SELECT (- t1.a::int)::int, (- t1.c::double)::double, (- t1.d::decimal)::decimal FROM t1"
    );
}

#[test]
fn unary_minus_non_numeric_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT -e FROM t1", &[]),
        @"could not resolve operator overload for -(text)"
    );
}

#[test]
fn unary_plus_non_numeric_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT +g FROM t1", &[]),
        @"could not resolve operator overload for +(bool)"
    );
}

// PG's `UMINUS` precedence: the sign binds tighter than `*` and looser than
// `::`, so `- a * b` negates only `a` and `- a::double` negates the cast.
#[test]
fn unary_minus_precedence() {
    insta::assert_snapshot!(
        analyzed("SELECT - a * b FROM t1", &[]),
        @"SELECT ((- t1.a::int)::int * t1.b::int)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT - a::double FROM t1", &[]),
        @"SELECT (- t1.a::double)::double FROM t1"
    );
}

#[test]
fn unary_minus_stacks() {
    insta::assert_snapshot!(
        analyzed("SELECT - - a FROM t1", &[]),
        @"SELECT (- (- t1.a::int)::int)::int FROM t1"
    );
}

// The signs are legal in BETWEEN's middle operand.
#[test]
fn unary_minus_in_between_middle() {
    insta::assert_snapshot!(
        analyzed("SELECT b BETWEEN - a AND 10 FROM t1", &[]),
        @"SELECT (t1.b::int BETWEEN (- t1.a::int)::int AND 10::int)::bool FROM t1"
    );
}

// The operand coerces like any operator argument: `- b` widens to decimal
// to match the other addend.
#[test]
fn unary_minus_operand_coercion() {
    insta::assert_snapshot!(
        analyzed("SELECT - b + 2.5 FROM t1", &[]),
        @"SELECT ((- CAST(t1.b AS decimal))::decimal + 2.5::decimal)::decimal FROM t1"
    );
}

// An untyped operand (text literal, un-annotated parameter) resolves to the
// first arity-1 overload, int. Documented divergence: PG prefers `double
// precision` for an unknown operand of `-`.
#[test]
fn unary_minus_untyped_operand_resolves_to_int() {
    insta::assert_snapshot!(analyzed("SELECT - '1'", &[]), @"SELECT (- 1::int)::int");
    insta::assert_snapshot!(analyzed("SELECT - $1", &[]), @"SELECT (- $1::int)::int");
}

// ------------- BETWEEN -------------

#[test]
fn between_type() {
    insta::assert_snapshot!(
        analyzed("SELECT b BETWEEN 1 AND 3 FROM t1", &[]),
        @"SELECT (t1.b::int BETWEEN 1::int AND 3::int)::bool FROM t1"
    );
}

#[test]
fn between_widened_operands() {
    // All three operands unify to decimal; the coercions are materialized.
    insta::assert_snapshot!(
        analyzed("SELECT b BETWEEN 1 AND 2.5 FROM t1", &[]),
        @"SELECT (CAST(t1.b AS decimal) BETWEEN CAST(1 AS decimal) AND 2.5::decimal)::bool FROM t1"
    );
}

#[test]
fn not_between_type() {
    insta::assert_snapshot!(
        analyzed("SELECT b NOT BETWEEN a AND 10 FROM t1", &[]),
        @"SELECT (t1.b::int NOT BETWEEN t1.a::int AND 10::int)::bool FROM t1"
    );
}

#[test]
fn between_unmatched_types_error() {
    // A text *column* pins the failure: a text literal would coerce instead.
    insta::assert_snapshot!(
        analyze_error("SELECT b BETWEEN 1 AND e FROM t1", &[]),
        @"BETWEEN types int, int and text cannot be matched"
    );
}

// ------------- IS -------------

#[test]
fn is_null_type() {
    insta::assert_snapshot!(
        analyzed("SELECT b IS NULL FROM t1", &[]),
        @"SELECT (t1.b::int IS NULL)::bool FROM t1"
    );
}

#[test]
fn is_not_null_type() {
    insta::assert_snapshot!(
        analyzed("SELECT b IS NOT NULL FROM t1", &[]),
        @"SELECT (t1.b::int IS NOT NULL)::bool FROM t1"
    );
}

#[test]
fn is_true_type() {
    insta::assert_snapshot!(
        analyzed("SELECT (b > 1) IS TRUE FROM t1", &[]),
        @"SELECT ((t1.b::int > 1::int)::bool IS TRUE)::bool FROM t1"
    );
}

#[test]
fn is_not_false_type() {
    insta::assert_snapshot!(
        analyzed("SELECT g IS NOT FALSE FROM t1", &[]),
        @"SELECT (t1.g::bool IS NOT FALSE)::bool FROM t1"
    );
}

#[test]
fn is_true_non_boolean_error() {
    // Postgres-strict: IS TRUE requires a boolean argument (PG: "... not type integer").
    insta::assert_snapshot!(
        analyze_error("SELECT b IS TRUE FROM t1", &[]),
        @"argument of IS TRUE must be type boolean, not type int"
    );
}

#[test]
fn null_is_null() {
    // The untyped NULL operand follows the text-defaulting rule.
    insta::assert_snapshot!(
        analyzed("SELECT NULL IS NULL", &[]),
        @"SELECT (NULL::string IS NULL)::bool"
    );
}

#[test]
fn is_unknown_non_bool_arg_error() {
    // The untyped NULL operand follows the text-defaulting rule.
    insta::assert_snapshot!(
        analyze_error("select 1 is unknown;", &[]),
        @"argument of IS UNKNOWN must be type boolean, not type int"
    );
}

// ------------- LIKE / SIMILAR -------------

#[test]
fn like_escape_type() {
    insta::assert_snapshot!(
        analyzed("SELECT e LIKE 'a!%' ESCAPE '!' FROM t1", &[]),
        @"SELECT (t1.e::string LIKE 'a!%'::string ESCAPE '!'::string)::bool FROM t1"
    );
}

#[test]
fn like_or_ilike() {
    // ILIKE must survive analysis: case-folding is the planner's concern.
    insta::assert_snapshot!(
        analyzed("SELECT e LIKE 'x' OR e ILIKE 'y' FROM t1", &[]),
        @"SELECT ((t1.e::string LIKE 'x'::string)::bool OR (t1.e::string ILIKE 'y'::string)::bool)::bool FROM t1"
    );
}

#[test]
fn is_and_like() {
    insta::assert_snapshot!(
        analyzed("SELECT (b > 1) IS TRUE AND e LIKE 'y' ESCAPE 'z' FROM t1", &[]),
        @"SELECT (((t1.b::int > 1::int)::bool IS TRUE)::bool AND (t1.e::string LIKE 'y'::string ESCAPE 'z'::string)::bool)::bool FROM t1"
    );
}

#[test]
fn like_non_text_pattern_error() {
    // The third `text` is the implicit `'\'` escape the mirror always carries.
    insta::assert_snapshot!(
        analyze_error("SELECT e LIKE b FROM t1", &[]),
        @"could not resolve function overload for like(text, int, text)"
    );
}

#[test]
fn like_non_text_escape_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT e LIKE 'x' ESCAPE g FROM t1", &[]),
        @"could not resolve function overload for like(text, text, bool)"
    );
}

#[test]
fn similar_type() {
    insta::assert_snapshot!(
        analyzed("SELECT e SIMILAR 'a_' ESCAPE 'x' FROM t1", &[]),
        @"SELECT (t1.e::string SIMILAR 'a_'::string ESCAPE 'x'::string)::bool FROM t1"
    );
}

#[test]
fn not_similar_default_escape() {
    // The implicit escape exists only in the type-system mirror — the
    // rendered SQL keeps no ESCAPE clause.
    insta::assert_snapshot!(
        analyzed("SELECT e NOT SIMILAR 'a' FROM t1", &[]),
        @"SELECT (t1.e::string NOT SIMILAR 'a'::string)::bool FROM t1"
    );
}

#[test]
fn similar_non_text_error() {
    // Documented divergence: SIMILAR types through the `like` overload,
    // so the message names `like`.
    insta::assert_snapshot!(
        analyze_error("SELECT b SIMILAR 'a' FROM t1", &[]),
        @"could not resolve function overload for like(int, text, text)"
    );
}

// ------------- IN -------------

#[test]
fn in_list_type() {
    insta::assert_snapshot!(
        analyzed("SELECT b IN (1, 2, 3) FROM t1", &[]),
        @"SELECT (t1.b::int IN (1::int, 2::int, 3::int))::bool FROM t1"
    );
}

#[test]
fn not_in_list_type() {
    insta::assert_snapshot!(
        analyzed("SELECT b NOT IN (1, 2) FROM t1", &[]),
        @"SELECT (t1.b::int NOT IN (1::int, 2::int))::bool FROM t1"
    );
}

#[test]
fn in_elements_widened() {
    // The lhs and every list element unify to decimal; coercions materialize.
    insta::assert_snapshot!(
        analyzed("SELECT b IN (1, 2.5) FROM t1", &[]),
        @"SELECT (CAST(t1.b AS decimal) IN (CAST(1 AS decimal), 2.5::decimal))::bool FROM t1"
    );
}

#[test]
fn in_type_mismatch_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT e IN (b, b) FROM t1", &[]),
        @"IN types text, int and int cannot be matched"
    );
}

#[test]
fn in_subquery() {
    insta::assert_snapshot!(
        analyzed("SELECT b IN (SELECT a FROM t3) FROM t1", &[]),
        @"SELECT (t1.b::int IN (SELECT t3.a::int FROM t3)::int)::bool FROM t1"
    );
}

#[test]
fn in_subquery_coerced_pins_no_rhs_cast() {
    insta::assert_snapshot!(
        analyzed("SELECT c IN (SELECT b FROM t1) FROM t1", &[]),
        @"SELECT (t1.c::double IN CAST((SELECT t1.b::int FROM t1) AS double))::bool FROM t1"
    );
}

#[test]
fn in_subquery_multi_column_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT 1 IN (SELECT a, b FROM t3)", &[]),
        @"failed to analyze AST: subquery must return only one column"
    );
}

#[test]
fn row_in_rows_unsupported() {
    insta::assert_snapshot!(
        analyze_error("SELECT (a, b) IN ((1, 2)) FROM t1", &[]),
        @"IN operator for rows is not supported"
    );
}

#[test]
fn in_cast_subquery_arg() {
    insta::assert_snapshot!(
        analyzed("SELECT 1.5 IN (SELECT 1)", &[]),
        @"SELECT (1.5::decimal IN CAST((SELECT 1::int) AS decimal))::bool"
    );
}

#[test]
fn in_no_cast_subquery_arg() {
    insta::assert_snapshot!(
        analyzed("SELECT 1.5 IN (SELECT 1 UNION SELECT 2.0)", &[]),
        @"SELECT (1.5::decimal IN ((SELECT CAST(1 AS decimal)) UNION (SELECT 2.0::decimal))::decimal)::bool"
    );
}

// ------------- EXISTS -------------

#[test]
fn exists_type() {
    insta::assert_snapshot!(
        analyzed("SELECT EXISTS (SELECT a FROM t3) FROM t1", &[]),
        @"SELECT EXISTS (SELECT t3.a::int FROM t3)::bool FROM t1"
    );
}

#[test]
fn not_exists_multi_column() {
    // No single-column restriction, as in Postgres.
    insta::assert_snapshot!(
        analyzed("SELECT NOT EXISTS (SELECT a, b FROM t3)", &[]),
        @"SELECT (NOT EXISTS (SELECT t3.a::int, t3.b::string FROM t3)::bool)::bool"
    );
}

#[test]
fn exists_correlated() {
    insta::assert_snapshot!(
        analyzed("SELECT EXISTS (SELECT 1 FROM t3 WHERE t3.a = t1.a) FROM t1", &[]),
        @"SELECT EXISTS (SELECT 1::int FROM t3 WHERE (t3.a::int = t1.a::int)::bool)::bool FROM t1"
    );
}

#[test]
fn exists_in_where() {
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t1 WHERE EXISTS (SELECT 1 FROM t3)", &[]),
        @"SELECT t1.a::int FROM t1 WHERE EXISTS (SELECT 1::int FROM t3)::bool"
    );
}

#[test]
fn exists_over_set_operation() {
    // EXISTS never asks for the body's result types, so set-operation
    // bodies (whose result types are not derivable yet) still work.
    insta::assert_snapshot!(
        analyzed("SELECT EXISTS (SELECT a FROM t3 UNION SELECT a FROM t4)", &[]),
        @"SELECT EXISTS ((SELECT t3.a::int FROM t3) UNION (SELECT t4.a::int FROM t4))::bool"
    );
}

// ------------- CASE -------------

#[test]
fn case_searched_type() {
    insta::assert_snapshot!(
        analyzed("SELECT CASE WHEN g THEN 1 ELSE 2 END FROM t1", &[]),
        @"SELECT CASE WHEN t1.g::bool THEN 1::int ELSE 2::int END::int FROM t1"
    );
}

#[test]
fn case_simple_type() {
    insta::assert_snapshot!(
        analyzed("SELECT CASE b WHEN 1 THEN 'one' ELSE 'other' END FROM t1", &[]),
        @"SELECT CASE t1.b::int WHEN 1::int THEN 'one'::string ELSE 'other'::string END::string FROM t1"
    );
}

#[test]
fn case_branches_widened() {
    // THEN/ELSE branches unify to decimal; the coercion materializes.
    insta::assert_snapshot!(
        analyzed("SELECT CASE WHEN g THEN 1 ELSE 2.5 END FROM t1", &[]),
        @"SELECT CASE WHEN t1.g::bool THEN CAST(1 AS decimal) ELSE 2.5::decimal END::decimal FROM t1"
    );
}

#[test]
fn case_all_null_branches() {
    // The untyped NULL branch follows the text-defaulting rule.
    insta::assert_snapshot!(
        analyzed("SELECT CASE WHEN g THEN NULL END FROM t1", &[]),
        @"SELECT CASE WHEN t1.g::bool THEN NULL::string END::string FROM t1"
    );
}

#[test]
fn case_branch_type_mismatch_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT CASE WHEN g THEN 1 ELSE e END FROM t1", &[]),
        @"CASE/THEN types int and text cannot be matched"
    );
}

#[test]
fn case_searched_non_boolean_when_error() {
    // Postgres-parity check the type system lacks: its CASE/WHEN unification
    // would happily unify all-int conditions.
    insta::assert_snapshot!(
        analyze_error("SELECT CASE WHEN b THEN 1 END FROM t1", &[]),
        @"argument of CASE/WHEN must be type boolean, not type int"
    );
}

#[test]
fn case_simple_when_type_mismatch_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT CASE b WHEN g THEN 1 END FROM t1", &[]),
        @"CASE/WHEN types int and bool cannot be matched"
    );
}

// ------------- TRIM -------------

#[test]
fn trim_type() {
    insta::assert_snapshot!(
        analyzed("SELECT TRIM(e) FROM t1", &[]),
        @"SELECT TRIM(t1.e::string)::string FROM t1"
    );
}

#[test]
fn trim_pattern_type() {
    insta::assert_snapshot!(
        analyzed("SELECT TRIM(LEADING 'x' FROM e) FROM t1", &[]),
        @"SELECT TRIM(LEADING 'x'::string FROM t1.e::string)::string FROM t1"
    );
}

#[test]
fn trim_kind_without_pattern() {
    insta::assert_snapshot!(
        analyzed("SELECT TRIM(BOTH FROM e) FROM t1", &[]),
        @"SELECT TRIM(BOTH FROM t1.e::string)::string FROM t1"
    );
}

#[test]
fn trim_non_text_type_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT TRIM(b) FROM t1", &[]),
        @"could not resolve function overload for trim(int)"
    );
}

// ------------- SUBSTRING -------------

#[test]
fn substring_regular_type() {
    insta::assert_snapshot!(
        analyzed("SELECT SUBSTRING(e, 1, 2) FROM t1", &[]),
        @"SELECT SUBSTRING(t1.e::string, 1::int, 2::int)::string FROM t1"
    );
}

#[test]
fn substring_from_for_type() {
    insta::assert_snapshot!(
        analyzed("SELECT SUBSTRING(e FROM 1 FOR 2) FROM t1", &[]),
        @"SELECT SUBSTRING(t1.e::string FROM 1::int FOR 2::int)::string FROM t1"
    );
}

#[test]
fn substring_from_type() {
    insta::assert_snapshot!(
        analyzed("SELECT SUBSTRING(e, 2) FROM t1", &[]),
        @"SELECT SUBSTRING(t1.e::string FROM 2::int)::string FROM t1"
    );
}

#[test]
fn substring_for_type() {
    insta::assert_snapshot!(
        analyzed("SELECT SUBSTRING(e FOR 2) FROM t1", &[]),
        @"SELECT SUBSTRING(t1.e::string FOR 2::int)::string FROM t1"
    );
}

#[test]
fn substring_regex_positions() {
    // The (text, text, text) overload is the POSIX-regex form, as in Postgres.
    insta::assert_snapshot!(
        analyzed("SELECT SUBSTRING(e, 'a', 'b') FROM t1", &[]),
        @"SELECT SUBSTRING(t1.e::string, 'a'::string, 'b'::string)::string FROM t1"
    );
}

#[test]
fn substring_similar_type() {
    insta::assert_snapshot!(
        analyzed("SELECT SUBSTRING(e SIMILAR 'a' ESCAPE 'x') FROM t1", &[]),
        @"SELECT SUBSTRING(t1.e::string SIMILAR 'a'::string ESCAPE 'x'::string)::string FROM t1"
    );
}

#[test]
fn substring_similar_missing_escape_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT SUBSTRING(e SIMILAR 'a') FROM t1", &[]),
        @"failed to analyze AST: missing escape symbol for SIMILAR substring operator"
    );
}

#[test]
fn substring_single_arg_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT SUBSTRING(e) FROM t1", &[]),
        @"failed to analyze AST: incorrect SUBSTRING parameters. There is no such overload that takes only 1 argument"
    );
}

#[test]
fn substring_bad_arg_type_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT SUBSTRING(e FROM g) FROM t1", &[]),
        @"could not resolve function overload for substring(text, bool)"
    );
}

// ------------- Time functions -------------

#[test]
fn current_date_type() {
    insta::assert_snapshot!(
        analyzed("SELECT CURRENT_DATE", &[]),
        @"SELECT CURRENT_DATE::datetime"
    );
}

#[test]
fn current_timestamp_precision_type() {
    insta::assert_snapshot!(
        analyzed("SELECT CURRENT_TIMESTAMP(3)", &[]),
        @"SELECT CURRENT_TIMESTAMP(3)::datetime"
    );
}

#[test]
fn localtimestamp_type() {
    insta::assert_snapshot!(
        analyzed("SELECT LOCALTIMESTAMP", &[]),
        @"SELECT LOCALTIMESTAMP::datetime"
    );
}

#[test]
fn current_time_not_implemented() {
    // Old-pipeline parity: picodata has no time-of-day type.
    insta::assert_snapshot!(
        analyze_error("SELECT CURRENT_TIME", &[]),
        @"SQL function `CURRENT_TIME` not implemented"
    );
}

#[test]
fn localtime_not_implemented() {
    insta::assert_snapshot!(
        analyze_error("SELECT LOCALTIME", &[]),
        @"SQL function `LOCALTIME` not implemented"
    );
}

// ------------- Function calls -------------

#[test]
fn aggregate_fn_type() {
    // sum(int) → numeric (rendered `decimal`), as registered.
    insta::assert_snapshot!(
        analyzed("SELECT sum(a) FROM t1", &[]),
        @"SELECT sum(t1.a::int)::decimal FROM t1"
    );
}

#[test]
fn count_asterisk_type() {
    insta::assert_snapshot!(
        analyzed("SELECT count(*) FROM t1", &[]),
        @"SELECT count(*)::int FROM t1"
    );
}

#[test]
fn count_distinct_type() {
    // DISTINCT does not affect typing and must survive analysis.
    insta::assert_snapshot!(
        analyzed("SELECT count(DISTINCT a) FROM t1", &[]),
        @"SELECT count(DISTINCT t1.a::int)::int FROM t1"
    );
}

#[test]
fn scalar_fn_and_coalesce_type() {
    // COALESCE is not a registry function: it types as a homogeneous group.
    insta::assert_snapshot!(
        analyzed("SELECT lower(e), coalesce(a, 1) FROM t1", &[]),
        @"SELECT lower(t1.e::string)::string, coalesce(t1.a::int, 1::int)::int FROM t1"
    );
}

#[test]
fn coalesce_unmatched_types_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT coalesce(a, g) FROM t1", &[]),
        @"COALESCE types int and bool cannot be matched"
    );
}

#[test]
fn unknown_function_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT foo(1)", &[]),
        @"function foo does not exist"
    );
}

#[test]
fn scalar_fn_overload_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT lower(b) FROM t1", &[]),
        @"could not resolve function overload for lower(int)"
    );
}
