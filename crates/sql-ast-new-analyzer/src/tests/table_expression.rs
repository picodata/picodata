//! The FROM clause: table factors and joins.
//!
//! # Cases
//! A FROM entry is what everything else resolves against, so the cases here ask
//! what a given entry contributes to the scope — a base table its catalog
//! columns, a subquery its result columns under its alias — and what happens
//! when two entries contribute the same name.
//!
//! USING has a section of its own. It does not merely compare the two sides: it
//! reduces the pair of columns to one, and that reduction has to be visible to
//! everything reading the join afterwards without suppressing the qualified
//! spelling of either side.
//!
//! # Scoping
//! A join condition is bound while the FROM clause it belongs to is still being
//! built, so the entries to its left are reachable through it while the clause
//! as a whole is not yet a frame. The last section pins that boundary from both
//! directions: what a subquery inside ON can see, and what it must not.

use super::{analyze_error, analyzed};

// ------------- Table factors -------------

#[test]
fn reference_to_nonexisting_relation() {
    let query = "WITH t1 AS (SELECT 1 AS x) SELECT x FROM nonexisting_relation";
    insta::assert_snapshot!(analyze_error(query, &[]), @r#"failed to analyze AST: relation "nonexisting_relation" does not exist"#);
}

#[test]
fn subquery_table_expr() {
    let query = "SELECT a FROM (SELECT 1 AS a) AS t";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t.a::int FROM (SELECT 1::int AS a) AS t");
}

#[test]
fn column_reference_to_unnamed_subquery_result_column() {
    let query = "SELECT a FROM (SELECT a FROM t1) AS s";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT s.a::int FROM (SELECT t1.a::int FROM t1) AS s");
}

#[test]
fn qualified_column_reference_to_unnamed_subquery_result_column() {
    let query = "SELECT s.a FROM (SELECT a FROM t1) AS s";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT s.a::int FROM (SELECT t1.a::int FROM t1) AS s");
}

// ------------- JOIN -------------

#[test]
fn join_simple() {
    let query = "SELECT t1.a, t2.c FROM t1 INNER JOIN t2 ON t1.a = t2.c";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.a::int, t2.c::int FROM t1 INNER JOIN t2 ON (t1.a::int = t2.c::int)::bool");
}

#[test]
fn left_join_simple() {
    let query = "SELECT t1.a, t2.c FROM t1 LEFT JOIN t2 ON t1.a = t2.c";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.a::int, t2.c::int FROM t1 LEFT OUTER JOIN t2 ON (t1.a::int = t2.c::int)::bool");
}

#[test]
fn join_multiple_definition_same_tbl_factor() {
    let query = "select t1.a from t1 INNER JOIN (SELECT c AS a, d AS b FROM t2) t1 USING (a);";
    insta::assert_snapshot!(analyze_error(query, &[]), @r#"failed to analyze AST: table "t1" specified more than once"#);
}

#[test]
fn left_join_multiple_definition_same_tbl_factor() {
    let query = "select t1.a from t1 LEFT JOIN (SELECT c AS a, d AS b FROM t2) t1 USING (a);";
    insta::assert_snapshot!(analyze_error(query, &[]), @r#"failed to analyze AST: table "t1" specified more than once"#);
}

#[test]
fn join_invalid_condition_expr_data_type() {
    let query = "SELECT * FROM t3 INNER JOIN t5 ON t3.a";
    insta::assert_snapshot!(analyze_error(query, &[]), @"failed to analyze AST: argument of JOIN/ON must be type boolean, not type int");
}

// ------------- USING -------------

#[test]
fn join_using_duplicate() {
    let query = "SELECT * FROM (SELECT a, b FROM t1) t1 INNER JOIN (SELECT c a, d b FROM t2) t2 USING (a, a)";
    insta::assert_snapshot!(analyze_error(query, &[]), @r#"failed to analyze AST: column name "a" appears more than once in USING clause"#);
}

#[test]
fn join_multiple_different_using() {
    let query = "WITH cte (ugu) AS (SELECT 1) SELECT ugu, * FROM t1 INNER JOIN (SELECT c a, c ugu FROM t2) t2 USING(a) INNER JOIN cte USING(ugu)";
    insta::assert_snapshot!(analyzed(query, &[]), @"WITH cte (ugu) AS (SELECT 1::int) SELECT t2.ugu::int, t2.ugu::int, t1.a::int, t1.b::int, t1.c::double, t1.d::decimal, t1.e::string, t1.f::string, t1.g::bool FROM t1 INNER JOIN (SELECT t2.c::int AS a, t2.c::int AS ugu FROM t2) AS t2 USING (a) INNER JOIN cte USING (ugu)");
}

/// No common type between the two inputs: rejected with PostgreSQL's wording,
/// by the same machinery that unifies one output column of a set operation.
#[test]
fn join_inconsistent_using_column_types() {
    let query = "SELECT * FROM (SELECT a FROM t1) t1 INNER JOIN (SELECT d a FROM t2) t2 USING(a)";
    insta::assert_snapshot!(analyze_error(query, &[]), @"JOIN/USING types int and text cannot be matched");
}

/// The merged output column takes the *common* type of the two inputs
/// (as PostgreSQL: `pg_typeof(a)` is numeric here), not the left one's.
#[test]
fn join_castable_using_columns() {
    let query = "SELECT * FROM (SELECT 1 a) INNER JOIN (SELECT 1.5 a) USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT CAST(a AS decimal) FROM (SELECT 1::int AS a) INNER JOIN (SELECT 1.5::decimal AS a) USING (a)");
}

#[test]
fn join_tables_column_using_reduction() {
    let query = "SELECT a FROM t3 INNER JOIN t4 USING(a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t3.a::int FROM t3 INNER JOIN t4 USING (a)");
}

#[test]
fn join_tables_column_using_reduction_multiple() {
    let query = "SELECT * FROM t3 t1 INNER JOIN t3 t2 USING (a) INNER JOIN t3 t3 USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.a::int, t1.b::string, t2.b::string, t3.b::string FROM t3 AS t1 INNER JOIN t3 AS t2 USING (a) INNER JOIN t3 AS t3 USING (a)");
}

/// The merged column is the left one, and an outer join is where that matters:
/// `t4.a` is null for a row `t4` did not match.
#[test]
fn left_join_using_reduction_keeps_the_left_column() {
    let query = "SELECT a FROM t3 LEFT JOIN t4 USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t3.a::int FROM t3 LEFT OUTER JOIN t4 USING (a)");
}

/// A chain merging one name resolves it to the leftmost column, not to the last
/// table joined.
#[test]
fn join_chain_using_reduction_resolves_to_the_leftmost() {
    let query = "SELECT a FROM t3 x INNER JOIN t3 y USING (a) INNER JOIN t3 z USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT x.a::int FROM t3 AS x INNER JOIN t3 AS y USING (a) INNER JOIN t3 AS z USING (a)");
}

/// A USING column of a later join is looked up in the join output built so far,
/// so a name two earlier entries still expose is ambiguous there - exactly as it
/// would be in a select list.
#[test]
fn join_using_column_ambigious_in_the_left_input() {
    let query = "SELECT * FROM t6 x INNER JOIN t6 y USING (k1) INNER JOIN t6 z USING (k2)";
    insta::assert_snapshot!(analyze_error(query, &[]), @"failed to analyze AST: column reference 'k2' is ambigious");
}

#[test]
fn join_table_using_reduction_does_not_suppress_qualified_ref() {
    let query = "SELECT t4.a FROM t3 INNER JOIN t4 USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t4.a::int FROM t3 INNER JOIN t4 USING (a)");
}

// ------------- The USING merged column's type -------------

/// Bare `a` is the merge (common type), `x.a`/`y.a` are the original columns.
#[test]
fn join_using_merged_type_bare_vs_qualified() {
    let query = "SELECT a, x.a, y.a FROM (SELECT 1 a) x INNER JOIN (SELECT 1.5 a) y USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT CAST(x.a AS decimal), x.a::int, y.a::decimal FROM (SELECT 1::int AS a) AS x INNER JOIN (SELECT 1.5::decimal AS a) AS y USING (a)");
}

/// The merged type feeds enclosing expressions: `a + 1` is numeric, not int.
#[test]
fn join_using_merged_type_in_expression() {
    let query = "SELECT a + 1 FROM (SELECT 1 a) x INNER JOIN (SELECT 1.5 a) y USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (CAST(x.a AS decimal) + CAST(1 AS decimal))::decimal FROM (SELECT 1::int AS a) AS x INNER JOIN (SELECT 1.5::decimal AS a) AS y USING (a)");
}

/// A chain of merges unifies left to right.
#[test]
fn join_using_merged_type_chain_unifies_left_to_right() {
    let query = "SELECT a FROM (SELECT 1 a) x INNER JOIN (SELECT 1.5 a) y USING (a) INNER JOIN (SELECT CAST(2.5 AS double) a) z USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT CAST(x.a AS decimal) FROM (SELECT 1::int AS a) AS x INNER JOIN (SELECT 1.5::decimal AS a) AS y USING (a) INNER JOIN (SELECT CAST(2.5 AS double) AS a) AS z USING (a)");
}

/// LEFT JOIN merges by the same rule.
#[test]
fn left_join_using_merged_type() {
    let query = "SELECT a FROM (SELECT 1 a) x LEFT JOIN (SELECT 1.5 a) y USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT CAST(x.a AS decimal) FROM (SELECT 1::int AS a) AS x LEFT OUTER JOIN (SELECT 1.5::decimal AS a) AS y USING (a)");
}

/// The merged type is what the join output exposes through a subquery.
#[test]
fn join_using_merged_type_through_subquery() {
    let query =
        "SELECT a FROM (SELECT * FROM (SELECT 1 a) x INNER JOIN (SELECT 1.5 a) y USING (a)) s";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT s.a::decimal FROM (SELECT CAST(x.a AS decimal) FROM (SELECT 1::int AS a) AS x INNER JOIN (SELECT 1.5::decimal AS a) AS y USING (a)) AS s");
}

/// Merging two real-table columns of different types: t1.a is int, t2.a is double, the merge is double.
#[test]
fn join_using_merged_type_base_tables() {
    let query = "SELECT a FROM (SELECT a FROM t1) x INNER JOIN (SELECT a FROM t2) y USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT CAST(x.a AS double) FROM (SELECT t1.a::int FROM t1) AS x INNER JOIN (SELECT t2.a::double FROM t2) AS y USING (a)");
}

/// Datetime USING datetime.
#[test]
fn join_using_datetime_columns() {
    let query = "SELECT f FROM t2 x INNER JOIN t2 y USING (f)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT x.f::datetime FROM t2 AS x INNER JOIN t2 AS y USING (f)");
}

/// A correlated reference reads the merged type of the *outer* level's join.
#[test]
fn join_using_merged_type_correlated_reference() {
    let query = "SELECT (SELECT a) FROM (SELECT 1 a) x INNER JOIN (SELECT 1.5 a) y USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (SELECT CAST(x.a AS decimal))::decimal FROM (SELECT 1::int AS a) AS x INNER JOIN (SELECT 1.5::decimal AS a) AS y USING (a)");
}

/// The merged type is what a set-operation branch contributes: decimal ∪ int is decimal.
#[test]
fn join_using_merged_type_under_set_operation() {
    let query = "SELECT a FROM (SELECT 1 a) x INNER JOIN (SELECT 1.5 a) y USING (a) UNION SELECT 2";
    insta::assert_snapshot!(analyzed(query, &[]), @"(SELECT CAST(x.a AS decimal) FROM (SELECT 1::int AS a) AS x INNER JOIN (SELECT 1.5::decimal AS a) AS y USING (a)) UNION (SELECT CAST(2 AS decimal))");
}

// ------------- Scopes visible from a join condition -------------

#[test]
fn join_subquery_in_join_condition_sees_current_from_entries() {
    let query = "SELECT a FROM (SELECT 1 a) t1 INNER JOIN (SELECT 1) t2 ON (SELECT t1.a)::bool";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.a::int FROM (SELECT 1::int AS a) AS t1 INNER JOIN (SELECT 1::int) AS t2 ON (SELECT t1.a::int)::bool");
}

#[test]
fn join_subquery_in_join_condition_sees_current_from_entry_subquery() {
    let query = "SELECT a FROM (SELECT 1 a) t1 INNER JOIN (SELECT 2) t2 ON (SELECT t1.a)::bool";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.a::int FROM (SELECT 1::int AS a) AS t1 INNER JOIN (SELECT 2::int) AS t2 ON (SELECT t1.a::int)::bool");
}

#[test]
fn join_correlated_subquery_in_join_condition_sees_current_from_entry_base_table() {
    let query = "SELECT (SELECT a FROM t3 t4 INNER JOIN t5 t6 ON (SELECT t4.a::BOOL)) FROM t3 x";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (SELECT t4.a::int FROM t3 AS t4 INNER JOIN t5 AS t6 ON (SELECT t4.a::bool)::bool)::int FROM t3 AS x");
}

#[test]
fn join_correlated_subquery_in_join_condition_sees_outer_scope() {
    let query = "SELECT (SELECT a FROM t3 INNER JOIN t5 ON (SELECT x.a::BOOL)) FROM t3 x";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (SELECT t3.a::int FROM t3 INNER JOIN t5 ON (SELECT x.a::bool)::bool)::int FROM t3 AS x");
}

#[test]
fn join_on_condition_curr_from_has_lower_priority_then_subquery_from_frame() {
    let query = "SELECT 1 FROM t1 x INNER JOIN t5 z ON (SELECT b FROM t3)::bool";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT 1::int FROM t1 AS x INNER JOIN t5 AS z ON (SELECT t3.b::string FROM t3)::bool");
}

#[test]
fn join_on_condition_curr_from_has_lower_priority_then_subquery_from_frame_aliased() {
    let query = "SELECT 1 FROM t1 x INNER JOIN t5 z ON (SELECT x.b FROM t3 x)::bool";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT 1::int FROM t1 AS x INNER JOIN t5 AS z ON (SELECT x.b::string FROM t3 AS x)::bool");
}

#[test]
fn join_on_condition_nested_from_stack() {
    let query =
        "SELECT 1 FROM t1 x INNER JOIN t3 y ON (SELECT ax FROM (SELECT x.a AS ax) s)::bool;";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT 1::int FROM t1 AS x INNER JOIN t3 AS y ON (SELECT s.ax::int FROM (SELECT x.a::int AS ax) AS s)::bool");
}
