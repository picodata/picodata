//! The clauses that filter what FROM produced: WHERE.
//!
//! # Cases
//! These clauses read the FROM frame without being part of it, which is the
//! point of the section: WHERE resolves against the tables in FROM and not
//! against the select list beside it, so an alias defined one clause over stays
//! invisible to it.
//!
//! WHERE carries the bulk of the cases because it is bound *before* the select
//! list. A parameter first met there is therefore typed there, and the type it
//! settles on has to reach the projections that mention it afterwards.
//!
//! # Scoping
//! Where a clause sits relative to the FROM frames also decides what it can
//! correlate to, so the middle of the section walks WHERE through nested FROMs,
//! join conditions, CTEs and both branches of a set operation.

use super::{analyze_error, analyzed};
use sql_ir::ir::types::{DerivedType, UnrestrictedType};

// ------------- WHERE -------------

#[test]
fn where_simple() {
    let query = "SELECT a FROM t1 WHERE a = 1";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT t1.a::int FROM t1 WHERE (t1.a::int = 1::int)::bool"
    );
}

#[test]
fn where_inconsistent_condition_result_type_error() {
    let query = "SELECT a FROM t1 WHERE a";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @"failed to analyze AST: argument of WHERE must be type boolean, not type int"
    );
}

/// The literal is read as the `bool` WHERE asks for right here, so a literal
/// that is not valid `bool` input fails analysis - as it does in Postgres.
#[test]
fn where_inconsistent_condition_result_type_literal_error() {
    let query = "SELECT a FROM t1 WHERE 'a'";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @r#"failed to analyze AST: invalid input syntax for type bool: "a""#
    );
}

/// ... and one that *is* valid `bool` input is folded into the literal itself,
/// so no cast survives into the tree.
#[test]
fn where_condition_result_type_literal_is_folded() {
    let query = "SELECT a FROM t1 WHERE 'true'";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT t1.a::int FROM t1 WHERE true::bool"
    );
}

#[test]
fn where_inconsistent_condition_result_type_mixed() {
    let query = "SELECT a FROM t1 WHERE 'x' OR g;";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @r#"failed to analyze AST: invalid input syntax for type bool: "x""#
    );
}

#[test]
fn where_does_not_see_select_list_alias() {
    // The clause is analyzed before the select list and resolves against the
    // FROM alone, so an output alias is invisible to it - as in Postgres, and
    // unlike GROUP BY / ORDER BY, which will have to see it.
    let query = "SELECT a AS xx FROM t1 WHERE xx > 1";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @"failed to analyze AST: cannot resolve column reference 'xx'"
    );
}

#[test]
fn where_ambigious_column_across_join() {
    let query = "SELECT 1 FROM t3 INNER JOIN t4 ON TRUE WHERE a = 1";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @"failed to analyze AST: column reference 'a' is ambigious"
    );
}

#[test]
fn where_over_join_using_takes_merged_column() {
    // `USING` merges the two `a`s into one column anchored on the left side,
    // so the unqualified reference is not ambigious here.
    let query = "SELECT a FROM t3 INNER JOIN t4 USING (a) WHERE a = 1";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT t3.a::int FROM t3 INNER JOIN t4 USING (a) WHERE (t3.a::int = 1::int)::bool"
    );
}

/// Under an outer join the choice of merged column is observable: `t4.b` is
/// null for a row `t4` did not match, so WHERE must read `t3.b`.
#[test]
fn where_over_left_join_using_takes_the_left_column() {
    let query = "SELECT b FROM t3 LEFT JOIN t4 USING (b) WHERE b = 'x'";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT t3.b::string FROM t3 LEFT OUTER JOIN t4 USING (b) WHERE (t3.b::string = 'x'::string)::bool"
    );
}

#[test]
fn where_over_left_join_with_distinct_and_qualified_asterisk() {
    let query = "SELECT DISTINCT t3.* FROM t3 LEFT JOIN t5 ON t3.a = t5.c WHERE t5.d = t3.b";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT DISTINCT t3.a::int, t3.b::string FROM t3 LEFT OUTER JOIN t5 ON (t3.a::int = t5.c::int)::bool WHERE (t5.d::string = t3.b::string)::bool"
    );
}

#[test]
fn where_subquery_from_shadows_the_enclosing_alias() {
    // Both scopes name a relation `x`; the subquery's own FROM is the innermost
    // frame, so `x.b` is t3's string column, not t1's int one.
    let query = "SELECT a FROM t1 x WHERE (SELECT x.b FROM t3 x) = 'q'";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT x.a::int FROM t1 AS x WHERE ((SELECT x.b::string FROM t3 AS x)::string = 'q'::string)::bool"
    );
}

#[test]
fn where_subquery_binds_inward_and_correlates_outward() {
    // One subquery, two directions: `b` is t3's own column, while `g` exists
    // nowhere in t3 and correlates out to the enclosing t1.
    let query = "SELECT a FROM t1 WHERE (SELECT g FROM t3 WHERE b = 'x')";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT t1.a::int FROM t1 WHERE (SELECT t1.g::bool FROM t3 WHERE (t3.b::string = 'x'::string)::bool)::bool"
    );
}

#[test]
fn where_correlates_through_a_nested_from() {
    let query = "SELECT a FROM t1 WHERE (SELECT ax FROM (SELECT t1.a AS ax) s) = 1";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT t1.a::int FROM t1 WHERE ((SELECT s.ax::int FROM (SELECT t1.a::int AS ax) AS s)::int = 1::int)::bool"
    );
}

#[test]
fn where_inside_a_join_condition_subquery() {
    // The innermost WHERE sits three scopes down: `b` binds to the subquery's
    // own t1, `x.a` reaches the enclosing join's left side.
    let query = "SELECT 1 FROM t1 x INNER JOIN t5 z ON (SELECT g FROM t1 WHERE b = x.a)";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT 1::int FROM t1 AS x INNER JOIN t5 AS z ON (SELECT t1.g::bool FROM t1 WHERE (t1.b::int = x.a::int)::bool)::bool"
    );
}

#[test]
fn where_in_joined_subquery_cannot_reference_the_join() {
    // No LATERAL: a joined table factor is analyzed with the FROM being built
    // blanked out, so its WHERE cannot reach the relation it is joined to.
    let query = "SELECT 1 FROM t1 INNER JOIN (SELECT * FROM t3 WHERE t1.a = 1) s ON TRUE";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @"failed to analyze AST: cannot resolve column reference 't1.a'"
    );
}

#[test]
fn where_at_each_nesting_level() {
    let query = "SELECT a FROM (SELECT a, b FROM t1 WHERE a = 1) s WHERE s.b = 2";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT s.a::int FROM (SELECT t1.a::int, t1.b::int FROM t1 WHERE (t1.a::int = 1::int)::bool) AS s WHERE (s.b::int = 2::int)::bool"
    );
}

#[test]
fn where_over_cte_with_column_list() {
    // The outer condition names the CTE's renamed column, the inner one the
    // underlying table column.
    let query = "WITH c (p, q) AS (SELECT a, b FROM t1 WHERE a = 1) SELECT p FROM c WHERE q = 2";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"WITH c (p, q) AS (SELECT t1.a::int, t1.b::int FROM t1 WHERE (t1.a::int = 1::int)::bool) SELECT c.p::int FROM c WHERE (c.q::int = 2::int)::bool"
    );
}

#[test]
fn where_in_both_set_operation_branches() {
    let query = "SELECT a FROM t1 WHERE b = 1 UNION ALL SELECT a FROM t3 WHERE a = 2";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"(SELECT t1.a::int FROM t1 WHERE (t1.b::int = 1::int)::bool) UNION ALL (SELECT t3.a::int FROM t3 WHERE (t3.a::int = 2::int)::bool)"
    );
}

#[test]
fn where_coerces_operands_of_a_compound_condition() {
    // Each comparison is typed on its own - int widens to double on the left,
    // the int literal widens to decimal in the middle - and the boolean column
    // needs no coercion to be an operand of AND.
    let query = "SELECT a FROM t1 WHERE b = c AND d = 1 AND g";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT t1.a::int FROM t1 WHERE (((CAST(t1.b AS double) = t1.c::double)::bool AND (t1.d::decimal = CAST(1 AS decimal))::bool)::bool AND t1.g::bool)::bool"
    );
}

#[test]
fn where_parameter_type_reaches_the_select_list() {
    // Type derivation filters first
    let query = "SELECT $1, a FROM t1 WHERE b = $1";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT $1::int, t1.a::int FROM t1 WHERE (t1.b::int = $1::int)::bool"
    );
}

#[test]
fn where_parameter_is_typed_before_the_select_list() {
    // WHERE is analyzed first, so `$1` is int by the time the projection asks
    // for text - the operand order in the message is what pins that order.
    // Postgres rejects this too, with the operands the other way round.
    let query = "SELECT e = $1 FROM t1 WHERE b = $1";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @"could not resolve operator overload for =(text, int)"
    );
}

#[test]
fn where_client_typed_parameter_is_coerced() {
    // `$1` is fixed to int by the client, so it is the parameter that gets
    // coerced to the column's decimal rather than the other way round.
    let query = "SELECT a FROM t1 WHERE d = $1";
    insta::assert_snapshot!(
        analyzed(query, &[DerivedType::new(UnrestrictedType::Integer)]),
        @"SELECT t1.a::int FROM t1 WHERE (t1.d::decimal = CAST($1 AS decimal))::bool"
    );
}

#[test]
fn where_client_typed_parameter_as_whole_condition() {
    let query = "SELECT a FROM t1 WHERE $1";
    insta::assert_snapshot!(
        analyzed(query, &[DerivedType::new(UnrestrictedType::Boolean)]),
        @"SELECT t1.a::int FROM t1 WHERE $1::bool"
    );
}

#[test]
fn where_client_typed_parameter_conflicts_with_column() {
    let query = "SELECT a FROM t1 WHERE g = $1 AND b = $2";
    let param_types = [
        DerivedType::unknown(),
        DerivedType::new(UnrestrictedType::String),
    ];
    insta::assert_snapshot!(
        analyze_error(query, &param_types),
        @"could not resolve operator overload for =(int, text)"
    );
}
