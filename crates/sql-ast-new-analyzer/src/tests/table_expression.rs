//! The FROM clause: table factors, joins, and the grouping read against them.
//!
//! GROUP BY and HAVING are one subject here rather than two: a HAVING clause
//! makes its level grouped whether or not a GROUP BY is written, and a grouping
//! key that is an expression covers HAVING as well as the projection, so the two
//! decide the same question and the cases for them sit together. What HAVING
//! *resolves* and what type it has to be is a separate matter, and lives in
//! [`super::select`] beside WHERE.
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
//! as a whole is not yet a frame. The section on the scopes visible from a join
//! condition pins that boundary from both directions: what a subquery inside ON
//! can see, and what it must not.

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

/// Merged column in WHERE and GROUP BY reads the merged type too.
#[test]
fn join_using_merged_type_in_where_and_group_by() {
    let query = "SELECT sum(a) FROM (SELECT 1 a) x INNER JOIN (SELECT 1.5 a) y USING (a) WHERE a > 1 GROUP BY a";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT sum(CAST(x.a AS decimal))::decimal FROM (SELECT 1::int AS a) AS x INNER JOIN (SELECT 1.5::decimal AS a) AS y USING (a) WHERE (CAST(x.a AS decimal) > CAST(1 AS decimal))::bool GROUP BY CAST(x.a AS decimal)");
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

/// Grouping by the merged column does not group the column it merges.
/// The merged `a` is `x.a` widened to decimal, a different expression from the
/// bare int `x.a` - which is why Postgres rejects this one.
#[test]
fn join_using_merged_type_grouped_does_not_group_its_input() {
    let query =
        "SELECT a, x.a FROM (SELECT 1 a) x INNER JOIN (SELECT 1.5 a) y USING (a) GROUP BY a";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @r#"failed to analyze AST: column "x.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// The same rejection under LEFT JOIN, with the merged column read through an
/// aggregate this time: the bare `x.a` beside it is still what is rejected.
#[test]
fn left_join_using_merged_type_grouped_does_not_group_its_input() {
    let query =
        "SELECT sum(a), x.a FROM (SELECT 1 a) x LEFT JOIN (SELECT 1.5 a) y USING (a) GROUP BY a";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @r#"failed to analyze AST: column "x.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// A merge that widened nothing leaves the column exactly as it was: grouping by
/// the merged `a` groups `x.a`, because the two *are* the same expression.
#[test]
fn join_using_unwidened_merge_groups_its_input() {
    let query = "SELECT a, x.a FROM (SELECT 1 a) x INNER JOIN (SELECT 2 a) y USING (a) GROUP BY a";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT x.a::int, x.a::int FROM (SELECT 1::int AS a) AS x INNER JOIN (SELECT 2::int AS a) AS y USING (a) GROUP BY x.a::int"
    );
}

/// The other direction: grouping by the merge's input does not reject the merge.
/// The merged column reads only `x.a`, which is grouped.
#[test]
fn join_using_merged_type_covered_by_grouping_its_input() {
    let query = "SELECT a FROM (SELECT 1 a) x INNER JOIN (SELECT 1.5 a) y USING (a) GROUP BY x.a";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT CAST(x.a AS decimal) FROM (SELECT 1::int AS a) AS x INNER JOIN (SELECT 1.5::decimal AS a) AS y USING (a) GROUP BY x.a::int"
    );
}

/// `*` expands the merged column to the same expression an unqualified
/// reference analyzes to, so `GROUP BY a` covers the one `*` emitted.
#[test]
fn join_using_merged_type_asterisk_matches_grouping_key() {
    let query = "SELECT * FROM (SELECT 1 a) x INNER JOIN (SELECT 1.5 a) y USING (a) GROUP BY a";
    insta::assert_snapshot!(
        analyzed(query, &[]),
        @"SELECT CAST(x.a AS decimal) FROM (SELECT 1::int AS a) AS x INNER JOIN (SELECT 1.5::decimal AS a) AS y USING (a) GROUP BY CAST(x.a AS decimal)"
    );
}

/// The merged column being a grouping key excuses only itself: the other
/// columns `*` expanded still owe the grouping check.
#[test]
fn join_using_merged_type_asterisk_still_checks_other_columns() {
    let query =
        "SELECT * FROM (SELECT 1 a, 2 b) x INNER JOIN (SELECT 1.5 a) y USING (a) GROUP BY a";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @r#"failed to analyze AST: column "x.b" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
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

/// The other direction of the boundary: an entry to the right of the join is
/// not part of the FROM clause yet, so neither the condition nor a subquery
/// inside it can reach it. Postgres rejects both the same way ("missing
/// FROM-clause entry").
#[test]
fn join_subquery_in_join_condition_does_not_see_entries_to_its_right() {
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 INNER JOIN t3 ON (SELECT t5.c) > 0 INNER JOIN t5 ON true", &[]),
        @"failed to analyze AST: cannot resolve column reference 't5.c'"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 INNER JOIN t3 ON t5.c > 0 INNER JOIN t5 ON true", &[]),
        @"failed to analyze AST: cannot resolve column reference 't5.c'"
    );
}

// ------------- GROUP BY -------------
#[test]
fn group_by_simple() {
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t1 GROUP BY a", &[]),
        @"SELECT t1.a::int FROM t1 GROUP BY t1.a::int"
    );
}

#[test]
fn group_by_aggr_under_alias() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(a) a1 FROM t1 GROUP BY a1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

#[test]
fn group_by_aggr_under_ordinal() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(a) a1 FROM t1 GROUP BY 1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

#[test]
fn group_by_aggr_under_ordinal_aterisk() {
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT * FROM (SELECT 1 + sum(t2.a))) FROM t1 t2 GROUP BY 1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

#[test]
fn group_by_alias() {
    insta::assert_snapshot!(
        analyzed("SELECT a a1 FROM t1 GROUP BY a1", &[]),
        @"SELECT t1.a::int AS a1 FROM t1 GROUP BY 1"
    );
}

#[test]
fn group_by_alias_ambigious() {
    insta::assert_snapshot!(
        analyze_error("select a a1, b a1 from t1 GROUP BY a1;", &[]),
        @r#"failed to analyze AST: GROUP BY "a1" is ambiguous"#
    );
}

/// A name is ambiguous only between *different* expressions: two projections
/// of the same expression under one name are one key.
#[test]
fn group_by_alias_shared_by_the_same_expression() {
    insta::assert_snapshot!(
        analyzed("SELECT a AS x, a AS x FROM t1 GROUP BY x", &[]),
        @"SELECT t1.a::int AS x, t1.a::int AS x FROM t1 GROUP BY 1"
    );
}

/// A key named by its alias is the expression behind the alias, exactly as if
/// it were named by its ordinal: the projections after it are matched against
/// that expression, in the select list, HAVING and ORDER BY alike.
#[test]
fn group_by_alias_key_covers_later_projections() {
    insta::assert_snapshot!(
        analyzed("SELECT abs(a) AS x, abs(a) + 1 FROM t1 GROUP BY x", &[]),
        @"SELECT abs(t1.a::int)::int AS x, (abs(t1.a::int)::int + 1::int)::int FROM t1 GROUP BY 1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT abs(a) AS x, abs(a) AS x FROM t1 GROUP BY x", &[]),
        @"SELECT abs(t1.a::int)::int AS x, abs(t1.a::int)::int AS x FROM t1 GROUP BY 1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT abs(a) AS x, (SELECT abs(t1.a)) FROM t1 GROUP BY x", &[]),
        @"SELECT abs(t1.a::int)::int AS x, (SELECT abs(t1.a::int)::int)::int FROM t1 GROUP BY 1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT abs(a) AS x, abs(a) + 1 FROM t1 GROUP BY x HAVING abs(a) > 1 ORDER BY abs(a)", &[]),
        @"SELECT abs(t1.a::int)::int AS x, (abs(t1.a::int)::int + 1::int)::int FROM t1 GROUP BY 1 HAVING (abs(t1.a::int)::int > 1::int)::bool ORDER BY abs(t1.a::int)::int ASC"
    );
    // The key covers only its own expression.
    insta::assert_snapshot!(
        analyze_error("SELECT abs(a) AS x, sum(abs(a)), abs(a) + b FROM t1 GROUP BY x", &[]),
        @r#"failed to analyze AST: column "t1.b" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// The alias is published under the projection's *expanded* position: after an
/// asterisk the raw position would name a different projection.
#[test]
fn group_by_alias_key_after_an_asterisk() {
    insta::assert_snapshot!(
        analyzed("SELECT *, abs(a) AS x, abs(a) + 1 FROM t3 GROUP BY x, a, b", &[]),
        @"SELECT t3.a::int, t3.b::string, abs(t3.a::int)::int AS x, (abs(t3.a::int)::int + 1::int)::int FROM t3 GROUP BY 3, t3.a::int, t3.b::string"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT *, abs(a) AS x, abs(a) + 1 FROM t3 GROUP BY x", &[]),
        @r#"failed to analyze AST: column "t3.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// A bare GROUP BY name also reaches the output name a projection carries
/// without an alias: a call is named by its function, a cast by its operand
/// and a CASE by its ELSE branch.
#[test]
fn group_by_output_name_inherited_from_the_expression() {
    insta::assert_snapshot!(
        analyzed("SELECT lower(e) FROM t1 GROUP BY lower", &[]),
        @"SELECT lower(t1.e::string)::string FROM t1 GROUP BY 1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT lower(e)::text FROM t1 GROUP BY lower", &[]),
        @"SELECT lower(t1.e::string)::string FROM t1 GROUP BY 1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT CASE WHEN a > 1 THEN 'x' ELSE lower(e) END FROM t1 GROUP BY lower", &[]),
        @"SELECT CASE WHEN (t1.a::int > 1::int)::bool THEN 'x'::string ELSE lower(t1.e::string)::string END::string FROM t1 GROUP BY 1"
    );
    // Only the ELSE branch names a CASE.
    insta::assert_snapshot!(
        analyze_error("SELECT CASE WHEN a > 1 THEN lower(e) ELSE 'x' END FROM t1 GROUP BY lower", &[]),
        @"failed to analyze AST: cannot resolve column reference 'lower'"
    );
    // An inherited name is held to the same rules as an alias.
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) FROM t1 GROUP BY count", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT lower(e), upper(e) AS lower FROM t1 GROUP BY lower", &[]),
        @r#"failed to analyze AST: GROUP BY "lower" is ambiguous"#
    );
    // A local FROM column of that name still wins.
    insta::assert_snapshot!(
        analyze_error("SELECT lower(e) AS a FROM t1 GROUP BY a", &[]),
        @r#"failed to analyze AST: column "t1.e" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// An ambiguous FROM name is ambiguous, alias or no alias.
#[test]
fn group_by_ambigious_from_name_does_not_fall_back_to_an_alias() {
    insta::assert_snapshot!(
        analyze_error("SELECT 1 AS b FROM t3 JOIN t4 ON t3.a = t4.a GROUP BY b", &[]),
        @"failed to analyze AST: column reference 'b' is ambigious"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT t3.b AS a FROM t3 JOIN t4 ON t3.a = t4.a GROUP BY a", &[]),
        @"failed to analyze AST: column reference 'a' is ambigious"
    );
    // A name no relation exposes still reaches the aliases.
    insta::assert_snapshot!(
        analyzed("SELECT 1 AS zz FROM t3 JOIN t4 ON t3.a = t4.a GROUP BY zz", &[]),
        @"SELECT 1::int AS zz FROM t3 INNER JOIN t4 ON (t3.a::int = t4.a::int)::bool GROUP BY 1"
    );
    // And USING merges the two columns into one, so the name is unambiguous and
    // resolves against the FROM clause rather than the equally named alias.
    insta::assert_snapshot!(
        analyzed("SELECT 1 AS a FROM t3 JOIN t4 USING (a) GROUP BY a", &[]),
        @"SELECT 1::int AS a FROM t3 INNER JOIN t4 USING (a) GROUP BY t3.a::int"
    );
}

// ---- Which elements are positions ----

#[test]
fn group_by_ordinal_spellings() {
    // Redundant parentheses and leading zeros are gone by the time the
    // element is examined, so these are the same position as a bare `1`.
    insta::assert_snapshot!(
        analyzed("SELECT b FROM t1 GROUP BY 01", &[]),
        @"SELECT t1.b::int FROM t1 GROUP BY 1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT b FROM t1 GROUP BY ((1))", &[]),
        @"SELECT t1.b::int FROM t1 GROUP BY 1"
    );
}

#[test]
fn group_by_signed_position() {
    insta::assert_snapshot!(
        analyzed("SELECT b FROM t1 GROUP BY - -1", &[]),
        @"SELECT t1.b::int FROM t1 GROUP BY 1"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT sum(a) FROM t1 GROUP BY - -1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyzed("SELECT sum(a) FROM t1 GROUP BY +1", &[]),
        @"SELECT sum(t1.a::int)::decimal FROM t1 GROUP BY +1::int"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 GROUP BY +1", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn group_by_drop_no_op_cast() {
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t1 GROUP BY a::int;", &[]),
        @"SELECT t1.a::int FROM t1 GROUP BY t1.a::int"
    );
}

#[test]
fn group_by_exprs_diff_cast() {
    insta::assert_snapshot!(
        analyzed("SELECT a::text FROM t1 GROUP BY a::int;", &[]),
        @"SELECT t1.a::string FROM t1 GROUP BY t1.a::int"
    );
}

#[test]
fn group_by_converting_cast_does_not_group_its_operand() {
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 GROUP BY a::text", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT e FROM t1 GROUP BY e::int", &[]),
        @r#"failed to analyze AST: column "t1.e" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) FROM t1 GROUP BY a::text HAVING a = 1", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn group_by_converting_cast_matches_the_same_cast() {
    insta::assert_snapshot!(
        analyzed("SELECT a::text FROM t1 GROUP BY a::text", &[]),
        @"SELECT t1.a::string FROM t1 GROUP BY t1.a::string"
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(*) FROM t1 GROUP BY a::text HAVING a::text = 'x'", &[]),
        @"SELECT count(*)::int FROM t1 GROUP BY t1.a::string HAVING (t1.a::string = 'x'::string)::bool"
    );
}

#[test]
fn group_by_no_op_cast_keeps_the_functional_dependency() {
    insta::assert_snapshot!(
        analyzed("SELECT v FROM t6 GROUP BY k1::int, k2::int", &[]),
        @"SELECT t6.v::int FROM t6 GROUP BY t6.k1::int, t6.k2::int"
    );
    insta::assert_snapshot!(
        analyzed("SELECT v FROM t6 GROUP BY CAST(k1 AS int), k2", &[]),
        @"SELECT t6.v::int FROM t6 GROUP BY t6.k1::int, t6.k2::int"
    );
    insta::assert_snapshot!(
        analyzed("SELECT a, e FROM t1 GROUP BY b::int", &[]),
        @"SELECT t1.a::int, t1.e::string FROM t1 GROUP BY t1.b::int"
    );
}

#[test]
fn group_by_converting_cast_drops_the_functional_dependency() {
    insta::assert_snapshot!(
        analyze_error("SELECT v FROM t6 GROUP BY k1::text, k2::text", &[]),
        @r#"failed to analyze AST: column "t6.v" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 GROUP BY b::text", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn group_by_no_op_cast_over_partial_key_is_not_a_dependency() {
    insta::assert_snapshot!(
        analyze_error("SELECT v FROM t6 GROUP BY k1::int", &[]),
        @r#"failed to analyze AST: column "t6.v" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn group_by_constant_expression_is_not_a_position() {
    insta::assert_snapshot!(
        analyzed("SELECT sum(a) FROM t1 GROUP BY 1 + 0", &[]),
        @"SELECT sum(t1.a::int)::decimal FROM t1 GROUP BY (1::int + 0::int)::int"
    );
    insta::assert_snapshot!(
        analyzed("SELECT sum(a) FROM t1 GROUP BY CAST(1 AS INT)", &[]),
        @"SELECT sum(t1.a::int)::decimal FROM t1 GROUP BY 1::int"
    );
}

#[test]
fn group_by_ordinal_out_of_range() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(a) FROM t1 GROUP BY 2", &[]),
        @"failed to analyze AST: GROUP BY position 2 is not in select list"
    );
}

#[test]
fn group_by_ordinal_counts_asterisk_expansion() {
    insta::assert_snapshot!(
        analyzed("SELECT * FROM t1 GROUP BY 1, 2, 3, 4, 5, 6, 7", &[]),
        @"SELECT t1.a::int, t1.b::int, t1.c::double, t1.d::decimal, t1.e::string, t1.f::string, t1.g::bool FROM t1 GROUP BY 1, 2, 3, 4, 5, 6, 7"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT * FROM t1 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8", &[]),
        @"failed to analyze AST: GROUP BY position 8 is not in select list"
    );
}

#[test]
fn group_by_ordinal_after_asterisk_reaches_the_aggregate() {
    insta::assert_snapshot!(
        analyzed("SELECT *, sum(a) FROM t1 GROUP BY 1, 2, 3, 4, 5, 6, 7", &[]),
        @"SELECT t1.a::int, t1.b::int, t1.c::double, t1.d::decimal, t1.e::string, t1.f::string, t1.g::bool, sum(t1.a::int)::decimal FROM t1 GROUP BY 1, 2, 3, 4, 5, 6, 7"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT *, sum(a) FROM t1 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT t1.*, sum(a) FROM t1 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

#[test]
fn group_by_ordinal_before_asterisk() {
    // Mirror image: the expansion has to shift what follows it, not what
    // precedes it.
    insta::assert_snapshot!(
        analyzed("SELECT sum(a), * FROM t1 GROUP BY 2, 3, 4, 5, 6, 7, 8", &[]),
        @"SELECT sum(t1.a::int)::decimal, t1.a::int, t1.b::int, t1.c::double, t1.d::decimal, t1.e::string, t1.f::string, t1.g::bool FROM t1 GROUP BY 2, 3, 4, 5, 6, 7, 8"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT sum(a), * FROM t1 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

#[test]
fn group_by_ordinal_counts_join_asterisk() {
    insta::assert_snapshot!(
        analyzed("SELECT *, sum(t3.a) FROM t3 INNER JOIN t5 ON t3.a = t5.c GROUP BY 1, 2, 3, 4", &[]),
        @"SELECT t3.a::int, t3.b::string, t5.c::int, t5.d::string, sum(t3.a::int)::decimal FROM t3 INNER JOIN t5 ON (t3.a::int = t5.c::int)::bool GROUP BY 1, 2, 3, 4"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT *, sum(t3.a) FROM t3 INNER JOIN t5 ON t3.a = t5.c GROUP BY 1, 2, 3, 4, 5", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyzed("SELECT *, sum(t3.a) FROM t3 INNER JOIN t4 USING (a) GROUP BY 1, 2, 3", &[]),
        @"SELECT t3.a::int, t3.b::string, t4.b::string, sum(t3.a::int)::decimal FROM t3 INNER JOIN t4 USING (a) GROUP BY 1, 2, 3"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT *, sum(t3.a) FROM t3 INNER JOIN t4 USING (a) GROUP BY 1, 2, 3, 4", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

// ---- What the named projection is then checked for ----

#[test]
fn group_by_ordinal_reaches_an_aggregate_at_any_depth() {
    insta::assert_snapshot!(
        analyze_error("SELECT 1 + sum(a) FROM t1 GROUP BY 1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT abs(sum(a)) FROM t1 GROUP BY 1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT CASE WHEN b > 0 THEN sum(a) ELSE 0 END AS x FROM t1 GROUP BY x", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

#[test]
fn group_by_gates_only_the_projections_it_names() {
    insta::assert_snapshot!(
        analyzed("SELECT b, sum(a) FROM t1 GROUP BY 1", &[]),
        @"SELECT t1.b::int, sum(t1.a::int)::decimal FROM t1 GROUP BY 1"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT b, sum(a) FROM t1 GROUP BY 2", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyzed("SELECT sum(a), b FROM t1 GROUP BY 2", &[]),
        @"SELECT sum(t1.a::int)::decimal, t1.b::int FROM t1 GROUP BY 2"
    );
    insta::assert_snapshot!(
        analyzed("SELECT b AS x, sum(a) AS y FROM t1 GROUP BY x", &[]),
        @"SELECT t1.b::int AS x, sum(t1.a::int)::decimal AS y FROM t1 GROUP BY 1"
    );
}

#[test]
fn group_by_ordinal_rejects_every_aggregate_spelling() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) FROM t1 GROUP BY 1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT count(DISTINCT a) AS x FROM t1 GROUP BY x", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT DISTINCT sum(a) FROM t1 GROUP BY 1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

// ---- Which projection a bare name names ----

#[test]
fn group_by_alias_yields_to_a_local_from_column() {
    insta::assert_snapshot!(
        analyzed("SELECT sum(a) AS b FROM t1 GROUP BY b", &[]),
        @"SELECT sum(t1.a::int)::decimal AS b FROM t1 GROUP BY t1.b::int"
    );
    insta::assert_snapshot!(
        analyzed("SELECT max(b) AS a, sum(c) AS x FROM t1 GROUP BY a", &[]),
        @"SELECT max(t1.b::int)::int AS a, sum(t1.c::double)::double AS x FROM t1 GROUP BY t1.a::int"
    );
}

#[test]
fn group_by_alias_beats_an_outer_column() {
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT sum(t5.c) AS a FROM t5 GROUP BY a) FROM t1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT sum(t5.c) AS d FROM t5 GROUP BY d) FROM t1", &[]),
        @"SELECT (SELECT sum(t5.c::int)::decimal AS d FROM t5 GROUP BY t5.d::string)::decimal FROM t1"
    );
}

#[test]
fn group_by_alias_yields_to_a_derived_table_column() {
    // The local FROM is whatever the clause exposes, not only base tables.
    insta::assert_snapshot!(
        analyze_error("SELECT sum(s.a) AS z FROM (SELECT a FROM t1) s GROUP BY z", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyzed("SELECT sum(s.a) AS a FROM (SELECT a FROM t1) s GROUP BY a", &[]),
        @"SELECT sum(s.a::int)::decimal AS a FROM (SELECT t1.a::int FROM t1) AS s GROUP BY s.a::int"
    );
}

/// An ordinal counts the *expanded* position, so it has to count the merged
/// column where the merge put it - at the front, ahead of columns the tables
/// list earlier.
#[test]
fn group_by_ordinal_counts_the_hoisted_merged_column() {
    insta::assert_snapshot!(
        analyzed("SELECT * FROM t3 INNER JOIN t4 USING (b) GROUP BY 1, 2, 3", &[]),
        @"SELECT t3.b::string, t3.a::int, t4.a::int FROM t3 INNER JOIN t4 USING (b) GROUP BY 1, 2, 3"
    );
    insta::assert_snapshot!(
        analyzed("SELECT * FROM t3 INNER JOIN t4 USING (b) ORDER BY 1", &[]),
        @"SELECT t3.b::string, t3.a::int, t4.a::int FROM t3 INNER JOIN t4 USING (b) ORDER BY 1 ASC"
    );
    // A chain collapses three `a` columns into one, so the expansion is four
    // wide and a fifth ordinal is out of range.
    insta::assert_snapshot!(
        analyzed("SELECT * FROM t3 t1 INNER JOIN t3 t2 USING (a) INNER JOIN t3 t3 USING (a) GROUP BY 1, 2, 3, 4", &[]),
        @"SELECT t1.a::int, t1.b::string, t2.b::string, t3.b::string FROM t3 AS t1 INNER JOIN t3 AS t2 USING (a) INNER JOIN t3 AS t3 USING (a) GROUP BY 1, 2, 3, 4"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT * FROM t3 t1 INNER JOIN t3 t2 USING (a) INNER JOIN t3 t3 USING (a) GROUP BY 1, 2, 3, 4, 5", &[]),
        @"failed to analyze AST: GROUP BY position 5 is not in select list"
    );
}

/// The merged column and the copy it hides are two different grouping keys:
/// grouping by one does not group the other, and the rejection names whichever
/// of the two the select list actually reached.
#[test]
fn group_by_merged_column_and_hidden_copy_are_distinct_keys() {
    insta::assert_snapshot!(
        analyzed("SELECT count(*) FROM t3 INNER JOIN t4 USING (a) GROUP BY t4.a", &[]),
        @"SELECT count(*)::int FROM t3 INNER JOIN t4 USING (a) GROUP BY t4.a::int"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t3 INNER JOIN t4 USING (a) GROUP BY t4.a", &[]),
        @r#"failed to analyze AST: column "t3.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT t4.a FROM t3 INNER JOIN t4 USING (a) GROUP BY a", &[]),
        @r#"failed to analyze AST: column "t4.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn group_by_alias_yields_to_a_using_reduced_column() {
    insta::assert_snapshot!(
        analyzed("SELECT sum(t3.a) AS a FROM t3 INNER JOIN t4 USING (a) GROUP BY a", &[]),
        @"SELECT sum(t3.a::int)::decimal AS a FROM t3 INNER JOIN t4 USING (a) GROUP BY t3.a::int"
    );
}

#[test]
fn group_by_alias_is_a_projection_alias_not_a_from_alias() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(a) AS x FROM t1 AS x GROUP BY x", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

#[test]
fn group_by_alias_is_looked_up_only_as_a_bare_name() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(a) AS x FROM t1 GROUP BY t1.x", &[]),
        @"failed to analyze AST: cannot resolve column reference 't1.x'"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT sum(a) AS x FROM t1 GROUP BY x + 0", &[]),
        @"failed to analyze AST: cannot resolve column reference 'x'"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT b AS x FROM t1 GROUP BY x, t1.x", &[]),
        @"failed to analyze AST: cannot resolve column reference 't1.x'"
    );
    insta::assert_snapshot!(
        analyzed("SELECT b AS x FROM t1 GROUP BY x, t1.b", &[]),
        @"SELECT t1.b::int AS x FROM t1 GROUP BY 1, t1.b::int"
    );
}

#[test]
fn group_by_alias_does_not_chain_through_the_select_list() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(a) AS x, x AS y FROM t1 GROUP BY y", &[]),
        @"failed to analyze AST: cannot resolve column reference 'x'"
    );
}

#[test]
fn group_by_alias_matched_by_its_normalized_spelling() {
    insta::assert_snapshot!(
        analyze_error(r#"SELECT sum(a) AS x FROM t1 GROUP BY "x""#, &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyze_error(r#"SELECT count(*) AS "X" FROM t1 GROUP BY "X""#, &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

#[test]
fn group_by_alias_gates_only_the_projection_it_names() {
    insta::assert_snapshot!(
        analyzed("SELECT sum(a) AS x, b AS y FROM t1 GROUP BY y", &[]),
        @"SELECT sum(t1.a::int)::decimal AS x, t1.b::int AS y FROM t1 GROUP BY 2"
    );
}

// ---- The query level the check belongs to ----

#[test]
fn group_by_over_a_subquery_aggregate_of_its_own_level() {
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT sum(t2.c) FROM t2) FROM t1 GROUP BY 1", &[]),
        @"SELECT (SELECT sum(t2.c::int)::decimal FROM t2)::decimal FROM t1 GROUP BY 1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT sum(t2.c) FROM t2) AS x FROM t1 GROUP BY x", &[]),
        @"SELECT (SELECT sum(t2.c::int)::decimal FROM t2)::decimal AS x FROM t1 GROUP BY 1"
    );
}

#[test]
fn group_by_over_a_correlated_subquery_aggregate() {
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT sum(t1.a) FROM t2) FROM t1 GROUP BY 1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT sum(t1.a) FROM t2) AS x FROM t1 GROUP BY x", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

#[test]
fn group_by_of_an_inner_query_is_read_against_that_query() {
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT t5.c AS a FROM t5 GROUP BY a) FROM t1 GROUP BY 1", &[]),
        @"SELECT (SELECT t5.c::int AS a FROM t5 GROUP BY 1)::int FROM t1 GROUP BY 1"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT sum(t2.c) AS q FROM t2 GROUP BY q) AS x FROM t1 GROUP BY x", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

#[test]
fn group_by_aggregate_rejected_inside_a_cte_or_derived_table() {
    insta::assert_snapshot!(
        analyze_error("WITH cte AS (SELECT sum(a) x FROM t1 GROUP BY x) SELECT * FROM cte", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT * FROM (SELECT sum(a) x FROM t1 GROUP BY 1) s", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

#[test]
fn group_by_aggregate_rejected_in_a_set_operation_branch() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(a) x FROM t1 GROUP BY x UNION SELECT 1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT 1 UNION SELECT sum(a) x FROM t1 GROUP BY 1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

#[test]
fn group_by_reference_aggr_check_before_resolution() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(a) AS s FROM t1 GROUP BY w, s", &[]),
        @"failed to analyze AST: cannot resolve column reference 'w'"
    );
}

#[test]
fn group_by_elem_under_aggr_under_alias() {
    insta::assert_snapshot!(
        analyzed("select (select sum(t1.a + t2.a) FROM t1 t2) FROM t1 GROUP BY 1;", &[]),
        @"SELECT (SELECT sum((t1.a::int + t2.a::int)::int)::decimal FROM t1 AS t2)::decimal FROM t1 GROUP BY 1"
    );
}

#[test]
fn group_by_different_spelling() {
    insta::assert_snapshot!(
        analyzed("SELECT a + b + 1 FROM t1 GROUP BY t1.a + t1.b", &[]),
        @"SELECT ((t1.a::int + t1.b::int)::int + 1::int)::int FROM t1 GROUP BY (t1.a::int + t1.b::int)::int"
    );
}

#[test]
fn group_by_aggr_same_as_sel_list_elem() {
    insta::assert_snapshot!(
        analyze_error("select sum(b) from t1 group by sum(b);", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

#[test]
fn group_by_aggr_same_as_sel_list_elem_compound() {
    insta::assert_snapshot!(
        analyze_error("select a + sum(b) from t1 group by a + sum(b);", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in GROUP BY"
    );
}

#[test]
fn group_by_same_expr_subtree_inner_sq_level() {
    insta::assert_snapshot!(
        analyzed("select (select sum(t1.a + t1.b + t2.a) from t1 t2) from t1 group by t1.a + t1.b", &[]),
        @"SELECT (SELECT sum(((t1.a::int + t1.b::int)::int + t2.a::int)::int)::decimal FROM t1 AS t2)::decimal FROM t1 GROUP BY (t1.a::int + t1.b::int)::int"
    );
}

/// A whole subquery serves as a grouping key, and matching it excuses the
/// correlated columns the projection reads through it.
#[test]
fn group_by_same_expr_subquery() {
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT max(t2.c) FROM t2 WHERE t2.c = t1.a) FROM t1 GROUP BY (SELECT max(t2.c) FROM t2 WHERE t2.c = t1.a);", &[]),
        @"SELECT (SELECT max(t2.c::int)::int FROM t2 WHERE (t2.c::int = t1.a::int)::bool)::int FROM t1 GROUP BY (SELECT max(t2.c::int)::int FROM t2 WHERE (t2.c::int = t1.a::int)::bool)::int"
    );
}

#[test]
fn group_by_subquery_key_uncovered() {
    // The key excuses the projection matching it, not a column written beside it.
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT max(t2.c) FROM t2 WHERE t2.c = t1.a), t1.a FROM t1 GROUP BY (SELECT max(t2.c) FROM t2 WHERE t2.c = t1.a)", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // The correlated column differs.
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT max(t2.c) FROM t2 WHERE t2.c = t1.a) FROM t1 GROUP BY (SELECT max(t2.c) FROM t2 WHERE t2.c = t1.b)", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // The relation behind the corresponding column differs - `t3` and `t4` are
    // shaped alike, so nothing but the relation itself tells the two keys apart.
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT max(t3.a) FROM t3 WHERE t3.a = t1.a) FROM t1 GROUP BY (SELECT max(t4.a) FROM t4 WHERE t4.a = t1.a)", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // Matching is structural, so a FROM alias is part of the key.
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT max(x.c) FROM t2 x WHERE x.c = t1.a) FROM t1 GROUP BY (SELECT max(y.c) FROM t2 y WHERE y.c = t1.a)", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn group_by_subquery_key_with_nested_relations() {
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT max(t3.a) FROM t3 INNER JOIN t5 ON t3.a = t5.c WHERE t3.a = t1.a) FROM t1 GROUP BY (SELECT max(t3.a) FROM t3 INNER JOIN t5 ON t3.a = t5.c WHERE t3.a = t1.a)", &[]),
        @"SELECT (SELECT max(t3.a::int)::int FROM t3 INNER JOIN t5 ON (t3.a::int = t5.c::int)::bool WHERE (t3.a::int = t1.a::int)::bool)::int FROM t1 GROUP BY (SELECT max(t3.a::int)::int FROM t3 INNER JOIN t5 ON (t3.a::int = t5.c::int)::bool WHERE (t3.a::int = t1.a::int)::bool)::int"
    );
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT max(s.c) FROM (SELECT t2.c FROM t2) s WHERE s.c = t1.a) FROM t1 GROUP BY (SELECT max(s.c) FROM (SELECT t2.c FROM t2) s WHERE s.c = t1.a)", &[]),
        @"SELECT (SELECT max(s.c::int)::int FROM (SELECT t2.c::int FROM t2) AS s WHERE (s.c::int = t1.a::int)::bool)::int FROM t1 GROUP BY (SELECT max(s.c::int)::int FROM (SELECT t2.c::int FROM t2) AS s WHERE (s.c::int = t1.a::int)::bool)::int"
    );
}

/// EXISTS reaches the same matching, and the negation is a different key.
#[test]
fn group_by_same_exists_subquery() {
    insta::assert_snapshot!(
        analyzed("SELECT EXISTS (SELECT 1 FROM t2 WHERE t2.c = t1.a) FROM t1 GROUP BY EXISTS (SELECT 1 FROM t2 WHERE t2.c = t1.a)", &[]),
        @"SELECT EXISTS (SELECT 1::int FROM t2 WHERE (t2.c::int = t1.a::int)::bool)::bool FROM t1 GROUP BY EXISTS (SELECT 1::int FROM t2 WHERE (t2.c::int = t1.a::int)::bool)::bool"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT EXISTS (SELECT 1 FROM t2 WHERE t2.c = t1.a) FROM t1 GROUP BY NOT EXISTS (SELECT 1 FROM t2 WHERE t2.c = t1.a)", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn group_by_whole_multiset_match() {
    insta::assert_snapshot!(
        analyzed("WITH x AS (VALUES (1)) SELECT (SELECT x.column1 ORDER BY x.column1 LIMIT 1) FROM x GROUP BY (SELECT x.column1 ORDER BY x.column1 LIMIT 1)", &[]),
        @"WITH x AS (VALUES (1::int)) SELECT (SELECT x.column1::int ORDER BY x.column1::int ASC LIMIT 1)::int FROM x GROUP BY (SELECT x.column1::int ORDER BY x.column1::int ASC LIMIT 1)::int"
    );
}

/// HAVING reaches a subquery key through the same rewind.
#[test]
fn having_same_expr_subquery() {
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 GROUP BY (SELECT max(t2.c) FROM t2 WHERE t2.c = t1.a) HAVING (SELECT max(t2.c) FROM t2 WHERE t2.c = t1.a) > 0", &[]),
        @"SELECT 1::int FROM t1 GROUP BY (SELECT max(t2.c::int)::int FROM t2 WHERE (t2.c::int = t1.a::int)::bool)::int HAVING ((SELECT max(t2.c::int)::int FROM t2 WHERE (t2.c::int = t1.a::int)::bool)::int > 0::int)::bool"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 GROUP BY (SELECT max(t2.c) FROM t2 WHERE t2.c = t1.a) HAVING (SELECT max(t2.c) FROM t2 WHERE t2.c = t1.b) > 0", &[]),
        @r#"failed to analyze AST: column "t1.b" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn group_by_expr_subtree_inner_sq_level_uncovered() {
    insta::assert_snapshot!(
        analyze_error("select (select sum(t2.a + t1.a + t1.b) from t1 t2) from t1 group by t1.a + t1.b", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // Matching is structural, so operand order is part of the key.
    insta::assert_snapshot!(
        analyze_error("select (select sum(t1.a + t1.b + t2.a) from t1 t2) from t1 group by t1.b + t1.a", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("select (select sum(t1.a + t1.b + t2.a) from t1 t2), t1.a from t1 group by t1.a + t1.b", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("select t1.a, (select sum(t1.a + t1.b + t2.a) from t1 t2) from t1 group by t1.a + t1.b", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// HAVING reaches the same key through the same rewind.
#[test]
fn having_same_expr_subtree_inner_sq_level() {
    insta::assert_snapshot!(
        analyzed("select 1 from t1 group by t1.a + t1.b having (select sum(t1.a + t1.b + t2.a) from t1 t2) > 0", &[]),
        @"SELECT 1::int FROM t1 GROUP BY (t1.a::int + t1.b::int)::int HAVING ((SELECT sum(((t1.a::int + t1.b::int)::int + t2.a::int)::int)::decimal FROM t1 AS t2)::decimal > CAST(0 AS decimal))::bool"
    );
    insta::assert_snapshot!(
        analyze_error("select 1 from t1 group by t1.a + t1.b having (select sum(t1.a + t1.c + t2.a) from t1 t2) > 0", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn group_by_same_expr_subtree() {
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT t1.a + 1 FROM t2) FROM t1 GROUP BY t1.a + 1", &[]),
        @"SELECT (SELECT (t1.a::int + 1::int)::int FROM t2)::int FROM t1 GROUP BY (t1.a::int + 1::int)::int"
    );
}

#[test]
fn group_by_fails_on_unary_plused_integer() {
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 GROUP BY +1;", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn group_by_flatten_row_value() {
    insta::assert_snapshot!(
        analyzed("SELECT a, b FROM t1 GROUP BY (a, b);", &[]),
        @"SELECT t1.a::int, t1.b::int FROM t1 GROUP BY t1.a::int, t1.b::int"
    );
    insta::assert_snapshot!(
        analyzed("SELECT c FROM t1 GROUP BY (a, b);", &[]),
        @"SELECT t1.c::double FROM t1 GROUP BY t1.a::int, t1.b::int"
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(*) FROM t1 GROUP BY (a, b);", &[]),
        @"SELECT count(*)::int FROM t1 GROUP BY t1.a::int, t1.b::int"
    );
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t1 GROUP BY (a, b), c;", &[]),
        @"SELECT t1.a::int FROM t1 GROUP BY t1.a::int, t1.b::int, t1.c::double"
    );
    insta::assert_snapshot!(
        analyzed("SELECT a, b, c FROM t1 GROUP BY ((a, b), c);", &[]),
        @"SELECT t1.a::int, t1.b::int, t1.c::double FROM t1 GROUP BY t1.a::int, t1.b::int, t1.c::double"
    );
    insta::assert_snapshot!(
        analyzed("SELECT a, b FROM t1 GROUP BY (1, 2);", &[]),
        @"SELECT t1.a::int, t1.b::int FROM t1 GROUP BY 1, 2"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 GROUP BY (a, 99);", &[]),
        @"failed to analyze AST: GROUP BY position 99 is not in select list"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT a, b FROM t1 GROUP BY (a, b) + 1;", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT a, b FROM t1 GROUP BY ROW(a, b);", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn group_by_pin_unsupported() {
    // Do not neglate user casts in matching grouping keys.
    insta::assert_snapshot!(
        analyze_error("SELECT a + 1 FROM t1 GROUP BY (a + 1)::int", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // Each expression referenced by ordinal assumed unique expression. SELECT list binding is left-to-right.
    insta::assert_snapshot!(
        analyzed("SELECT abs(a), abs(a) FROM t1 GROUP BY 1", &[]),
        @"SELECT abs(t1.a::int)::int, abs(t1.a::int)::int FROM t1 GROUP BY 1"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT abs(a), abs(a) FROM t1 GROUP BY 2", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

// ------------- HAVING -------------

#[test]
fn having_simple() {
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t1 GROUP BY a HAVING a > 1", &[]),
        @"SELECT t1.a::int FROM t1 GROUP BY t1.a::int HAVING (t1.a::int > 1::int)::bool"
    );
}

#[test]
fn having_with_aggr_simple() {
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t1 GROUP BY a HAVING a > 1 AND sum(b) > 1", &[]),
        @"SELECT t1.a::int FROM t1 GROUP BY t1.a::int HAVING ((t1.a::int > 1::int)::bool AND (sum(t1.b::int)::decimal > CAST(1 AS decimal))::bool)::bool"
    );
}

#[test]
fn having_with_non_aggr_target_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 GROUP BY a HAVING a > 1 AND b > 1", &[]),
        @r#"failed to analyze AST: column "t1.b" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// HAVING on its own makes the level grouped: the whole table becomes a single
/// group, so every column outside an aggregate has to be a grouping key even
/// though the query has neither a GROUP BY nor an aggregate. Postgres does the
/// same, and this is the rule that is easiest to lose — the check reads
/// "GROUP BY *or* an aggregate *or* HAVING", and dropping the third disjunct
/// silently accepts every query below that must be rejected.
#[test]
fn having_alone_groups_the_level() {
    // Nothing outside the aggregate, so nothing to complain about.
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 HAVING sum(t1.a) > 0", &[]),
        @"SELECT 1::int FROM t1 HAVING (sum(t1.a::int)::decimal > CAST(0 AS decimal))::bool"
    );
    // Not even an aggregate is needed: the clause by itself groups.
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 HAVING true", &[]),
        @"SELECT 1::int FROM t1 HAVING true::bool"
    );
    // ... which is why the clause's own column now has nowhere to sit.
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 HAVING t1.a > 0", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // The select list is held to it too.
    insta::assert_snapshot!(
        analyze_error("SELECT t1.a FROM t1 HAVING count(*) > 0", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // An asterisk is no exception - it expands to columns like any other.
    insta::assert_snapshot!(
        analyze_error("SELECT * FROM t1 HAVING count(*) > 0", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// The other polarity of [`having_alone_groups_the_level`], and the reason both
/// exist: with no HAVING, no GROUP BY and no aggregate the level is ungrouped
/// and a bare column is fine. Inverting the HAVING test - reading the clause as
/// present when it is absent - flips every verdict in the two tests at once,
/// which neither test catches alone.
#[test]
fn without_having_the_level_stays_ungrouped() {
    insta::assert_snapshot!(
        analyzed("SELECT t1.a FROM t1", &[]),
        @"SELECT t1.a::int FROM t1"
    );
    // WHERE filters rows; it does not group them.
    insta::assert_snapshot!(
        analyzed("SELECT t1.a FROM t1 WHERE t1.b > 0", &[]),
        @"SELECT t1.a::int FROM t1 WHERE (t1.b::int > 0::int)::bool"
    );
}

/// The middle disjunct of the same check: an aggregate anywhere in the level
/// groups it, with no GROUP BY and no HAVING in sight, so a bare column beside
/// the aggregate has nowhere to sit. Postgres rejects it with the same wording.
#[test]
fn aggregate_alone_groups_the_level() {
    insta::assert_snapshot!(
        analyze_error("SELECT t1.a, count(*) FROM t1", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // A GROUP BY over the primary key rescues the same select list.
    insta::assert_snapshot!(
        analyzed("SELECT t1.a, count(*) FROM t1 GROUP BY t1.b", &[]),
        @"SELECT t1.a::int, count(*)::int FROM t1 GROUP BY t1.b::int"
    );
}

/// Grouping by a table's whole primary key functionally determines its other
/// columns, and the exemption reaches HAVING just as it reaches the select
/// list. `t1`'s key is `b`.
#[test]
fn having_column_covered_by_the_grouped_primary_key() {
    insta::assert_snapshot!(
        analyzed("SELECT t1.a FROM t1 GROUP BY t1.b HAVING t1.a > 0", &[]),
        @"SELECT t1.a::int FROM t1 GROUP BY t1.b::int HAVING (t1.a::int > 0::int)::bool"
    );
    // Grouping by a non-key column determines nothing.
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 GROUP BY t1.a HAVING t1.b > 0", &[]),
        @r#"failed to analyze AST: column "t1.b" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // The exemption is per FROM entry: one alias' key says nothing about the other's.
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 x INNER JOIN t1 y ON TRUE GROUP BY x.b HAVING y.a > 0", &[]),
        @r#"failed to analyze AST: column "y.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 x INNER JOIN t1 y ON TRUE GROUP BY x.b, y.b HAVING y.a > 0", &[]),
        @"SELECT 1::int FROM t1 AS x INNER JOIN t1 AS y ON TRUE::bool GROUP BY x.b::int, y.b::int HAVING (y.a::int > 0::int)::bool"
    );
    // A derived table has no primary key to carry the dependency, so the same
    // grouping over the same columns is rejected once it goes through one.
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM (SELECT a, b FROM t1) s GROUP BY s.b HAVING s.a > 0", &[]),
        @r#"failed to analyze AST: column "s.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// The dependency is on the *whole* key: `t6` is keyed on `(k1, k2)`, and
/// either column alone determines nothing. A single-column key cannot tell
/// "every key column is grouped" from "some key column is grouped", so this is
/// the case that pins which of the two the check asks.
#[test]
fn having_column_covered_only_by_the_whole_primary_key() {
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t6 GROUP BY t6.k1, t6.k2 HAVING t6.v > 0", &[]),
        @"SELECT 1::int FROM t6 GROUP BY t6.k1::int, t6.k2::int HAVING (t6.v::int > 0::int)::bool"
    );
    // Order is irrelevant - the key columns are a set.
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t6 GROUP BY t6.k2, t6.k1 HAVING t6.v > 0", &[]),
        @"SELECT 1::int FROM t6 GROUP BY t6.k2::int, t6.k1::int HAVING (t6.v::int > 0::int)::bool"
    );
    // Half a key is not a key, whichever half.
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t6 GROUP BY t6.k1 HAVING t6.v > 0", &[]),
        @r#"failed to analyze AST: column "t6.v" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t6 GROUP BY t6.k2 HAVING t6.v > 0", &[]),
        @r#"failed to analyze AST: column "t6.v" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // The same, read from the select list, with HAVING only doing the grouping.
    insta::assert_snapshot!(
        analyzed("SELECT t6.v FROM t6 GROUP BY t6.k1, t6.k2 HAVING count(*) > 0", &[]),
        @"SELECT t6.v::int FROM t6 GROUP BY t6.k1::int, t6.k2::int HAVING (count(*)::int > 0::int)::bool"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT t6.v FROM t6 GROUP BY t6.k1 HAVING count(*) > 0", &[]),
        @r#"failed to analyze AST: column "t6.v" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// The dependency also flows through a key stated as a position or an alias:
/// the projection it names is the grouped column. `t3`'s key is `a`, so naming
/// the `a` projection determines `b`, and naming the `b` projection determines
/// nothing. Postgres agrees on all four.
#[test]
fn primary_key_grouped_through_a_position_or_alias() {
    insta::assert_snapshot!(
        analyzed("SELECT t3.a, t3.b FROM t3 GROUP BY 1", &[]),
        @"SELECT t3.a::int, t3.b::string FROM t3 GROUP BY 1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT t3.a AS x, t3.b FROM t3 GROUP BY x", &[]),
        @"SELECT t3.a::int AS x, t3.b::string FROM t3 GROUP BY 1"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT t3.b, t3.a FROM t3 GROUP BY 1", &[]),
        @r#"failed to analyze AST: column "t3.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT t3.b AS x, t3.a FROM t3 GROUP BY x", &[]),
        @r#"failed to analyze AST: column "t3.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// An aggregate in HAVING lifts the columns in its arguments, whatever they
/// are; nesting one aggregate in another is still rejected there.
#[test]
fn having_aggregate_lifts_its_arguments() {
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 GROUP BY t1.a HAVING count(DISTINCT t1.b) > 0", &[]),
        @"SELECT 1::int FROM t1 GROUP BY t1.a::int HAVING (count(DISTINCT t1.b::int)::int > 0::int)::bool"
    );
    // An expression over a grouped and an ungrouped column alike.
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 GROUP BY t1.a HAVING sum(t1.a + t1.b) > 0", &[]),
        @"SELECT 1::int FROM t1 GROUP BY t1.a::int HAVING (sum((t1.a::int + t1.b::int)::int)::decimal > CAST(0 AS decimal))::bool"
    );
    // Only the aggregate's own arguments are lifted, not the rest of the clause.
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 GROUP BY t1.a HAVING t1.a > 0 AND t1.c > 0", &[]),
        @r#"failed to analyze AST: column "t1.c" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 GROUP BY t1.a HAVING sum(sum(t1.b)) > 0", &[]),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
    // A conditional is not a barrier either way.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT 1 FROM t1 GROUP BY t1.a HAVING CASE WHEN t1.c > 0 THEN true ELSE false END",
            &[]
        ),
        @r#"failed to analyze AST: column "t1.c" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// Grouping is decided per query level, so a HAVING one level down groups only
/// that level - and an aggregate one level down over an outer column still
/// belongs to the outer level.
#[test]
fn having_groups_one_level_only() {
    // The derived table's HAVING groups the derived table.
    insta::assert_snapshot!(
        analyzed("SELECT * FROM (SELECT t1.a FROM t1 GROUP BY t1.a HAVING sum(t1.b) > 0) s", &[]),
        @"SELECT s.a::int FROM (SELECT t1.a::int FROM t1 GROUP BY t1.a::int HAVING (sum(t1.b::int)::decimal > CAST(0 AS decimal))::bool) AS s"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT * FROM (SELECT t1.a FROM t1 HAVING sum(t1.b) > 0) s", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // An inner HAVING leaves the enclosing level ungrouped.
    insta::assert_snapshot!(
        analyzed("SELECT t1.a FROM t1 WHERE (SELECT count(*) FROM t2 HAVING count(*) > 0) > 0", &[]),
        @"SELECT t1.a::int FROM t1 WHERE ((SELECT count(*)::int FROM t2 HAVING (count(*)::int > 0::int)::bool)::int > 0::int)::bool"
    );
    // `sum(t1.b)` is written inside the subquery but aggregates the outer
    // level, so it satisfies the outer grouping rather than needing it.
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 GROUP BY t1.a HAVING (SELECT sum(t1.b) FROM t2) > 0", &[]),
        @"SELECT 1::int FROM t1 GROUP BY t1.a::int HAVING ((SELECT sum(t1.b::int)::decimal FROM t2)::decimal > CAST(0 AS decimal))::bool"
    );
    // Same for an aggregate in the select list of a HAVING-grouped level.
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT sum(t1.a) FROM t2) FROM t1 HAVING count(*) > 0", &[]),
        @"SELECT (SELECT sum(t1.a::int)::decimal FROM t2)::decimal FROM t1 HAVING (count(*)::int > 0::int)::bool"
    );
}

/// A correlated reference out of a subquery in HAVING is held to the outer
/// grouping - Postgres calls this "subquery uses ungrouped column ... from
/// outer query" and rejects the same shapes.
#[test]
fn having_subquery_reads_the_outer_grouping() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT t1.a FROM t1 GROUP BY t1.a HAVING (SELECT count(*) FROM t2 WHERE t2.c = t1.a) > 0",
            &[]
        ),
        @"SELECT t1.a::int FROM t1 GROUP BY t1.a::int HAVING ((SELECT count(*)::int FROM t2 WHERE (t2.c::int = t1.a::int)::bool)::int > 0::int)::bool"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT t1.a FROM t1 GROUP BY t1.a HAVING (SELECT count(*) FROM t2 WHERE t2.c = t1.b) > 0",
            &[]
        ),
        @r#"failed to analyze AST: column "t1.b" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // Also through IN and EXISTS, and beside a conjunct that is itself fine.
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 GROUP BY t1.a HAVING t1.a IN (SELECT t2.c FROM t2)", &[]),
        @"SELECT 1::int FROM t1 GROUP BY t1.a::int HAVING (t1.a::int IN (SELECT t2.c::int FROM t2)::int)::bool"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 GROUP BY t1.a HAVING t1.c IN (SELECT t2.c FROM t2)", &[]),
        @r#"failed to analyze AST: column "t1.c" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT 1 FROM t1 GROUP BY t1.a HAVING EXISTS (SELECT 1 FROM t2 WHERE t2.c = t1.a)",
            &[]
        ),
        @"SELECT 1::int FROM t1 GROUP BY t1.a::int HAVING EXISTS (SELECT 1::int FROM t2 WHERE (t2.c::int = t1.a::int)::bool)::bool"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT t1.a FROM t1 GROUP BY t1.a HAVING t1.a > 0 AND (SELECT t2.c FROM t2 WHERE t2.d = t1.e) > 0",
            &[]
        ),
        @r#"failed to analyze AST: column "t1.e" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // A HAVING-grouped level with no GROUP BY at all still binds its
    // correlated references to that grouping.
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 HAVING (SELECT count(*) FROM t2 WHERE t2.c = t1.a) > 0", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// Each branch of a set operation is a level of its own, HAVING included.
#[test]
fn having_in_a_set_operation_branch() {
    insta::assert_snapshot!(
        analyzed("SELECT t1.a FROM t1 GROUP BY t1.a HAVING count(*) > 1 UNION SELECT 1", &[]),
        @"(SELECT t1.a::int FROM t1 GROUP BY t1.a::int HAVING (count(*)::int > 1::int)::bool) UNION (SELECT 1::int)"
    );
    // The grouping HAVING imposes is checked in whichever branch carries it.
    insta::assert_snapshot!(
        analyze_error("SELECT t1.a FROM t1 HAVING count(*) > 1 UNION SELECT 1", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT 1 UNION SELECT t1.a FROM t1 HAVING count(*) > 1", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// Over a join the rule reads per column, not per FROM entry: grouping by one
/// side's column does not make the other side's available.
#[test]
fn having_over_a_join() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT t3.a FROM t3 INNER JOIN t4 ON t3.a = t4.a GROUP BY t3.a HAVING count(t4.b) > 0",
            &[]
        ),
        @"SELECT t3.a::int FROM t3 INNER JOIN t4 ON (t3.a::int = t4.a::int)::bool GROUP BY t3.a::int HAVING (count(t4.b::string)::int > 0::int)::bool"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT t3.a FROM t3 INNER JOIN t4 ON t3.a = t4.a GROUP BY t3.a HAVING t4.b > 'x'",
            &[]
        ),
        @r#"failed to analyze AST: column "t4.b" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // A USING-reduced column is one column, so grouping by it covers the HAVING
    // reference to it.
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t3 INNER JOIN t4 USING (a) GROUP BY a HAVING count(*) > 0", &[]),
        @"SELECT t3.a::int FROM t3 INNER JOIN t4 USING (a) GROUP BY t3.a::int HAVING (count(*)::int > 0::int)::bool"
    );
    // HAVING groups a joined level the same way it groups a single table.
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t3 INNER JOIN t4 ON t3.a = t4.a HAVING count(*) > 0", &[]),
        @"SELECT 1::int FROM t3 INNER JOIN t4 ON (t3.a::int = t4.a::int)::bool HAVING (count(*)::int > 0::int)::bool"
    );
}

/// A grouping key stated as an ordinal is a grouping key like any other, and
/// HAVING reads it as one.
#[test]
fn having_over_a_group_by_ordinal() {
    insta::assert_snapshot!(
        analyzed("SELECT t1.a FROM t1 GROUP BY 1 HAVING t1.a > 0", &[]),
        @"SELECT t1.a::int FROM t1 GROUP BY 1 HAVING (t1.a::int > 0::int)::bool"
    );
    insta::assert_snapshot!(
        analyzed("SELECT t1.a + t1.c FROM t1 GROUP BY 1 HAVING sum(t1.b) > 0", &[]),
        @"SELECT (CAST(t1.a AS double) + t1.c::double)::double FROM t1 GROUP BY 1 HAVING (sum(t1.b::int)::decimal > CAST(0 AS decimal))::bool"
    );
}

/// DISTINCT is applied to what the grouping produced, so it neither adds to nor
/// removes from the grouping requirement.
#[test]
fn having_with_a_distinct_select_list() {
    insta::assert_snapshot!(
        analyzed("SELECT DISTINCT t1.a FROM t1 GROUP BY t1.a HAVING count(*) > 1", &[]),
        @"SELECT DISTINCT t1.a::int FROM t1 GROUP BY t1.a::int HAVING (count(*)::int > 1::int)::bool"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT DISTINCT t1.a FROM t1 HAVING count(*) > 1", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// An expression grouping key covers HAVING, at any depth, however the key was
/// written.
///
/// The key is matched as an expression, so it does not have to be the whole
/// predicate - the fourth case here reads the key through a multiplication - and
/// a grouping key that is not a column leaves nothing behind in `grouping_vars`
/// for HAVING to be checked against, which is why the match is what does the
/// work.
#[test]
fn having_over_an_expression_grouping_key() {
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 GROUP BY t1.a + t1.b HAVING t1.a + t1.b > 0", &[]),
        @"SELECT 1::int FROM t1 GROUP BY (t1.a::int + t1.b::int)::int HAVING ((t1.a::int + t1.b::int)::int > 0::int)::bool"
    );
    // A predicate as the key, matched whole.
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 GROUP BY (t1.a > 0) HAVING (t1.a > 0)", &[]),
        @"SELECT 1::int FROM t1 GROUP BY (t1.a::int > 0::int)::bool HAVING (t1.a::int > 0::int)::bool"
    );
    // A function call as the key.
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 GROUP BY upper(t1.e) HAVING upper(t1.e) = 'X'", &[]),
        @"SELECT 1::int FROM t1 GROUP BY upper(t1.e::string)::string HAVING (upper(t1.e::string)::string = 'X'::string)::bool"
    );
    // Not the root of the predicate: the key is one operand of the comparison.
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 GROUP BY t1.a + t1.c HAVING (t1.a + t1.c) * 2 > 0", &[]),
        @"SELECT 1::int FROM t1 GROUP BY (CAST(t1.a AS double) + t1.c::double)::double HAVING (((CAST(t1.a AS double) + t1.c::double)::double * CAST(2 AS double))::double > CAST(0 AS double))::bool"
    );
    // Nor the root of the clause: buried in a CASE, and twice over in an AND.
    insta::assert_snapshot!(
        analyzed(
            "SELECT 1 FROM t1 GROUP BY t1.a + t1.c HAVING CASE WHEN t1.a + t1.c > 0 THEN true ELSE false END",
            &[],
        ),
        @"SELECT 1::int FROM t1 GROUP BY (CAST(t1.a AS double) + t1.c::double)::double HAVING CASE WHEN ((CAST(t1.a AS double) + t1.c::double)::double > CAST(0 AS double))::bool THEN true::bool ELSE false::bool END::bool"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT 1 FROM t1 GROUP BY t1.a + t1.c HAVING t1.a + t1.c > 0 AND t1.a + t1.c > 1",
            &[],
        ),
        @"SELECT 1::int FROM t1 GROUP BY (CAST(t1.a AS double) + t1.c::double)::double HAVING (((CAST(t1.a AS double) + t1.c::double)::double > CAST(0 AS double))::bool AND ((CAST(t1.a AS double) + t1.c::double)::double > CAST(1 AS double))::bool)::bool"
    );
}

/// The two ways a GROUP BY element ends up naming a select-list position - an
/// ordinal written by hand, and an alias - both reach the same expression, so
/// HAVING matches against the projection in each case. Rendering shows the
/// collapse: every GROUP BY below prints as `1`.
#[test]
fn having_over_an_expression_key_named_by_position() {
    // Written out by hand.
    insta::assert_snapshot!(
        analyzed("SELECT t1.a + t1.c FROM t1 GROUP BY 1 HAVING t1.a + t1.c > 0", &[]),
        @"SELECT (CAST(t1.a AS double) + t1.c::double)::double FROM t1 GROUP BY 1 HAVING ((CAST(t1.a AS double) + t1.c::double)::double > CAST(0 AS double))::bool"
    );
    // Named by the projection's alias. HAVING cannot use the alias itself -
    // see `having_does_not_inherit_the_group_by_alias` - but the expression
    // behind it is still the key.
    insta::assert_snapshot!(
        analyzed("SELECT t1.a + t1.c AS x FROM t1 GROUP BY x HAVING t1.a + t1.c > 0", &[]),
        @"SELECT (CAST(t1.a AS double) + t1.c::double)::double AS x FROM t1 GROUP BY 1 HAVING ((CAST(t1.a AS double) + t1.c::double)::double > CAST(0 AS double))::bool"
    );
}

/// What the match is *not*: nothing is normalized. Only the same tree over the
/// same resolved columns is the same key. Postgres rejects all of these too -
/// it compares its analyzed nodes the same way.
#[test]
fn having_expression_key_is_matched_verbatim() {
    // A part of the key is not the key.
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 GROUP BY t1.a + t1.c HAVING t1.a > 0", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // No commutativity.
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 GROUP BY t1.a + t1.c HAVING t1.c + t1.a > 0", &[]),
        @r#"failed to analyze AST: column "t1.c" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // The same operands under a different operator are a different key.
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 GROUP BY t1.a + t1.c HAVING t1.a - t1.c > 0", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 GROUP BY t1.a > 0 HAVING t1.a < 0", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // A different column, in the shape of the key.
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 GROUP BY abs(t1.a) HAVING abs(t1.b) > 0", &[]),
        @r#"failed to analyze AST: column "t1.b" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // Columns are matched by what they resolve to, not by the name written, so
    // two aliases of one table are two different keys.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT 1 FROM t1 x INNER JOIN t1 y ON TRUE GROUP BY x.a + y.a HAVING y.a + x.a > 0",
            &[],
        ),
        @r#"failed to analyze AST: column "y.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // The key covers only its own occurrences. Here one operand of the
    // multiplication is the key and the other is a bare ungrouped column ...
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 GROUP BY t1.a + t1.c HAVING (t1.a + t1.c) * t1.a > 0", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // ... and here the offender is bound *before* the key that also reads it,
    // which is the order the covered columns are rewound in.
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 GROUP BY t1.a + t1.c HAVING t1.a > 0 AND t1.a + t1.c > 0", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // Grouping by an expression groups nothing in the select list either.
    insta::assert_snapshot!(
        analyze_error("SELECT t1.a FROM t1 GROUP BY t1.a + t1.c HAVING t1.a + t1.c > 0", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// `CAST(x AS t)` and `x::t` are one node that remembers which way it was
/// written, only so it can be rendered back. As grouping keys they are the same
/// expression, and Postgres agrees. Note the two spellings surviving into the
/// rendering on either side of the same query.
#[test]
fn having_expression_key_ignores_cast_spelling() {
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 GROUP BY CAST(t1.a AS double) HAVING t1.a::double > 0", &[]),
        @"SELECT 1::int FROM t1 GROUP BY CAST(t1.a AS double) HAVING (t1.a::double > CAST(0 AS double))::bool"
    );
}

/// Aggregates and expression keys do not interact: an aggregate's arguments
/// need no key of their own, so a key inside one changes nothing. Worth pinning
/// because the matching machinery does run over these nodes and has to come out
/// as a no-op - including the second case, where the aggregate belongs to this
/// level but is written a level down.
#[test]
fn having_expression_key_inside_an_aggregate() {
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 GROUP BY t1.a + t1.c HAVING sum(t1.a + t1.c) > 0", &[]),
        @"SELECT 1::int FROM t1 GROUP BY (CAST(t1.a AS double) + t1.c::double)::double HAVING (sum((CAST(t1.a AS double) + t1.c::double)::double)::double > CAST(0 AS double))::bool"
    );
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 GROUP BY t1.a + t1.c HAVING (SELECT sum(t1.a + t1.c) FROM t2) > 0", &[]),
        @"SELECT 1::int FROM t1 GROUP BY (CAST(t1.a AS double) + t1.c::double)::double HAVING ((SELECT sum((CAST(t1.a AS double) + t1.c::double)::double)::double FROM t2)::double > CAST(0 AS double))::bool"
    );
    // An aggregate still needs no key at all.
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 GROUP BY t1.a + t1.b HAVING sum(t1.c) > 0", &[]),
        @"SELECT 1::int FROM t1 GROUP BY (t1.a::int + t1.b::int)::int HAVING (sum(t1.c::double)::double > CAST(0 AS double))::bool"
    );
}

/// The key as the left-hand side of `IN`: the operand is matched like any other
/// occurrence, and so is the same expression inside the `IN` subquery, a level
/// down - see [`expression_grouping_key_reaches_into_a_subquery_below_it`] for
/// the rule and the Postgres version it follows.
#[test]
fn having_expression_key_as_an_in_operand() {
    insta::assert_snapshot!(
        analyzed("SELECT 1 FROM t1 GROUP BY t1.a + t1.c HAVING (t1.a + t1.c) IN (SELECT c FROM t5)", &[]),
        @"SELECT 1::int FROM t1 GROUP BY (CAST(t1.a AS double) + t1.c::double)::double HAVING ((CAST(t1.a AS double) + t1.c::double)::double IN CAST((SELECT t5.c::int FROM t5) AS double))::bool"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT 1 FROM t1 GROUP BY t1.a + t1.c HAVING t1.a + t1.c IN (SELECT c FROM t5 WHERE t5.c > t1.a + t1.c)",
            &[],
        ),
        @"SELECT 1::int FROM t1 GROUP BY (CAST(t1.a AS double) + t1.c::double)::double HAVING ((CAST(t1.a AS double) + t1.c::double)::double IN CAST((SELECT t5.c::int FROM t5 WHERE (CAST(t5.c AS double) > (CAST(t1.a AS double) + t1.c::double)::double)::bool) AS double))::bool"
    );
}

/// A grouping key belongs to one query level, and covers that level's
/// occurrences of the expression wherever they are written: at the level
/// itself, or inside a subquery below it, exactly as a column key does. A
/// subquery's own keys, in turn, are matched against its own level only.
///
/// This is Postgres 19's rule (commit `415100aa62b`, not back-patched): the
/// analyzer follows it deliberately. Postgres 18 matched expression keys at the
/// key's own level only and rejected the second query below with "subquery
/// uses ungrouped column ... from outer query"; only a grouped *column* could
/// be read from a subquery there. Two more tests lean on the same choice:
/// [`having_expression_key_as_an_in_operand`] and
/// [`grouping_keys_reach_into_a_select_list_subquery`].
#[test]
fn expression_grouping_key_reaches_into_a_subquery_below_it() {
    // The inner level's key covers the inner HAVING; the outer level, with no
    // GROUP BY and no HAVING, is not grouped at all and does not care.
    insta::assert_snapshot!(
        analyzed(
            "SELECT (SELECT 1 FROM t2 GROUP BY t2.c + t2.a HAVING t2.c + t2.a > 0) FROM t1",
            &[],
        ),
        @"SELECT (SELECT 1::int FROM t2 GROUP BY (CAST(t2.c AS double) + t2.a::double)::double HAVING ((CAST(t2.c AS double) + t2.a::double)::double > CAST(0 AS double))::bool)::int FROM t1"
    );
    // The outer key covers its occurrence inside a subquery written in HAVING.
    // Postgres 19 accepts this; 18 rejected it.
    insta::assert_snapshot!(
        analyzed(
            "SELECT 1 FROM t1 GROUP BY t1.a + t1.c HAVING (SELECT count(*) FROM t2 WHERE t1.a + t1.c > 0) > 0",
            &[],
        ),
        @"SELECT 1::int FROM t1 GROUP BY (CAST(t1.a AS double) + t1.c::double)::double HAVING ((SELECT count(*)::int FROM t2 WHERE ((CAST(t1.a AS double) + t1.c::double)::double > CAST(0 AS double))::bool)::int > 0::int)::bool"
    );
}

// ------------- Expression grouping keys in the select list -------------
//
// The same matching HAVING gets, one stage earlier: a projection is covered by
// an expression key wherever the key occurs in it.

/// The mirror of [`having_over_an_expression_grouping_key`]: the key is found
/// at any depth of a projection, and a composite key is found inside a wider
/// expression.
#[test]
fn select_list_matches_an_expression_key_at_depth() {
    insta::assert_snapshot!(
        analyzed("SELECT abs(abs(t1.a + t1.c)) FROM t1 GROUP BY t1.a + t1.c", &[]),
        @"SELECT abs(abs((CAST(t1.a AS double) + t1.c::double)::double)::double)::double FROM t1 GROUP BY (CAST(t1.a AS double) + t1.c::double)::double"
    );
    // Two occurrences inside one CASE.
    insta::assert_snapshot!(
        analyzed(
            "SELECT CASE WHEN t1.a + t1.c > 0 THEN t1.a + t1.c ELSE 0 END FROM t1 GROUP BY t1.a + t1.c",
            &[],
        ),
        @"SELECT CASE WHEN ((CAST(t1.a AS double) + t1.c::double)::double > CAST(0 AS double))::bool THEN (CAST(t1.a AS double) + t1.c::double)::double ELSE CAST(0 AS double) END::double FROM t1 GROUP BY (CAST(t1.a AS double) + t1.c::double)::double"
    );
    // A function call as the key, under another function call.
    insta::assert_snapshot!(
        analyzed("SELECT lower(upper(t1.e)) FROM t1 GROUP BY upper(t1.e)", &[]),
        @"SELECT lower(upper(t1.e::string)::string)::string FROM t1 GROUP BY upper(t1.e::string)::string"
    );
}

/// The mirror of [`having_expression_key_is_matched_verbatim`]: nothing is
/// normalized on the select-list side either.
#[test]
fn select_list_expression_key_is_matched_verbatim() {
    // A part of the key is not the key.
    insta::assert_snapshot!(
        analyze_error("SELECT t1.e FROM t1 GROUP BY lower(t1.e)", &[]),
        @r#"failed to analyze AST: column "t1.e" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // The same operands under a different operator.
    insta::assert_snapshot!(
        analyze_error("SELECT t1.a + t1.c FROM t1 GROUP BY t1.a * t1.c", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT a IS NULL FROM t1 GROUP BY a IS NOT NULL", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// A composite key covers only its whole occurrences: the key's own subtrees
/// grant nothing to the rest of the list. Postgres rejects this too - a bare
/// `t1.a + t1.c` is not the key `abs(t1.a + t1.c)`.
#[test]
fn composite_key_does_not_cover_its_subtrees() {
    insta::assert_snapshot!(
        analyze_error("SELECT abs(t1.a + t1.c), t1.a + t1.c FROM t1 GROUP BY abs(t1.a + t1.c)", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// Columns are matched by what they resolve to, not by the name written - the
/// select-list side of the same rule HAVING is held to. Two aliases of one
/// table are two different keys, in either direction.
#[test]
fn select_list_key_match_reads_resolved_columns_not_names() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT x.a + y.a FROM t3 x INNER JOIN t3 y ON x.a = y.a GROUP BY y.a + x.a",
            &[],
        ),
        @r#"failed to analyze AST: column "x.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // The shape and the table agree; the alias does not.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT y.a + y.a FROM t3 x INNER JOIN t3 y ON x.a = y.a GROUP BY x.a + x.a",
            &[],
        ),
        @r#"failed to analyze AST: column "y.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT x.a + y.a FROM t3 x INNER JOIN t3 y ON x.a = y.a GROUP BY x.a + y.a",
            &[],
        ),
        @"SELECT (x.a::int + y.a::int)::int FROM t3 AS x INNER JOIN t3 AS y ON (x.a::int = y.a::int)::bool GROUP BY (x.a::int + y.a::int)::int"
    );
}

/// A key covers exactly its own occurrences: a column read beside one - before
/// it or after it - is still ungrouped, and becomes fine the moment it is a key
/// of its own.
#[test]
fn select_list_key_covers_only_its_own_occurrences() {
    insta::assert_snapshot!(
        analyze_error("SELECT (t1.a + t1.c) + t1.a FROM t1 GROUP BY t1.a + t1.c", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyzed("SELECT (t1.a + t1.c) + t1.a FROM t1 GROUP BY t1.a + t1.c, t1.a", &[]),
        @"SELECT ((CAST(t1.a AS double) + t1.c::double)::double + CAST(t1.a AS double))::double FROM t1 GROUP BY (CAST(t1.a AS double) + t1.c::double)::double, t1.a::int"
    );
    // The offender bound before the key occurrence that also reads the column -
    // the order the covered columns are rewound in.
    insta::assert_snapshot!(
        analyze_error("SELECT t1.a + (t1.a + t1.c) FROM t1 GROUP BY t1.a + t1.c", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyzed("SELECT t1.a + (t1.a + t1.c) FROM t1 GROUP BY t1.a + t1.c, t1.a", &[]),
        @"SELECT (CAST(t1.a AS double) + (CAST(t1.a AS double) + t1.c::double)::double)::double FROM t1 GROUP BY (CAST(t1.a AS double) + t1.c::double)::double, t1.a::int"
    );
}

/// An occurrence is a subtree of the parse tree, so association decides whether
/// the key occurs at all: `sum(c) + a + b` is `(sum(c) + a) + b`, which
/// contains no `a + b`, and parenthesizing it back is what makes the match.
/// Postgres reads all four the same way.
#[test]
fn select_list_key_occurrences_follow_the_parse_tree() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(c) + a + b FROM t1 GROUP BY a + b", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyzed("SELECT sum(c) + (a + b) FROM t1 GROUP BY a + b", &[]),
        @"SELECT (sum(t1.c::double)::double + (CAST(t1.a AS double) + CAST(t1.b AS double))::double)::double FROM t1 GROUP BY (t1.a::int + t1.b::int)::int"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT sum(a + b) + a + b FROM t1 GROUP BY a + b", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // Key first, aggregate second: no parentheses needed, the key is the
    // comparison's own left subtree.
    insta::assert_snapshot!(
        analyzed("SELECT (a + b) + sum(c) FROM t1 GROUP BY a + b", &[]),
        @"SELECT ((CAST(t1.a AS double) + CAST(t1.b AS double))::double + sum(t1.c::double)::double)::double FROM t1 GROUP BY (t1.a::int + t1.b::int)::int"
    );
}

/// The select-list side of [`having_expression_key_inside_an_aggregate`]: an
/// aggregate's arguments are lifted whole, so they need no key - not when they
/// extend the key, and not when they resemble one that was never declared.
#[test]
fn select_list_aggregate_arguments_need_no_expression_key() {
    insta::assert_snapshot!(
        analyzed("SELECT sum(t1.a + t1.c + 1) FROM t1 GROUP BY t1.a + t1.c", &[]),
        @"SELECT sum(((CAST(t1.a AS double) + t1.c::double)::double + CAST(1 AS double))::double)::double FROM t1 GROUP BY (CAST(t1.a AS double) + t1.c::double)::double"
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(DISTINCT t1.a + t1.c) FROM t1 GROUP BY t1.e", &[]),
        @"SELECT count(DISTINCT (CAST(t1.a AS double) + t1.c::double)::double)::int FROM t1 GROUP BY t1.e::string"
    );
}

/// What crosses a subquery boundary in the select list: a grouped *column*
/// covers its uses inside a subquery, and so does an *expression* key - the
/// third query reads the key inside the subquery and right beside it, and both
/// occurrences are covered. Postgres 19 agrees; Postgres 18 let only the column
/// through, see [`expression_grouping_key_reaches_into_a_subquery_below_it`].
#[test]
fn grouping_keys_reach_into_a_select_list_subquery() {
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT t1.a FROM t2) FROM t1 GROUP BY t1.a", &[]),
        @"SELECT (SELECT t1.a::int FROM t2)::int FROM t1 GROUP BY t1.a::int"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT t1.a FROM t2) FROM t1 GROUP BY t1.e", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT (SELECT c FROM t5 WHERE t5.c > t1.a + t1.c) + (t1.a + t1.c) FROM t1 GROUP BY t1.a + t1.c",
            &[],
        ),
        @"SELECT (CAST((SELECT t5.c::int FROM t5 WHERE (CAST(t5.c AS double) > (CAST(t1.a AS double) + t1.c::double)::double)::bool) AS double) + (CAST(t1.a AS double) + t1.c::double)::double)::double FROM t1 GROUP BY (CAST(t1.a AS double) + t1.c::double)::double"
    );
}

/// A correlated aggregate satisfies the grouping of the level it belongs to -
/// the innermost level its columns come from - not the level it is written in.
/// Reading only outer columns, it aggregates the outer level and lifts them,
/// however many subqueries down it sits; one local column drags it inward and
/// leaves the outer column a bare correlated use.
#[test]
fn correlated_aggregate_satisfies_the_level_it_belongs_to() {
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT sum(t1.a) FROM t2) FROM t1 GROUP BY t1.e", &[]),
        @"SELECT (SELECT sum(t1.a::int)::decimal FROM t2)::decimal FROM t1 GROUP BY t1.e::string"
    );
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT (SELECT sum(t1.a) FROM t5) FROM t2) FROM t1 GROUP BY t1.e", &[]),
        @"SELECT (SELECT (SELECT sum(t1.a::int)::decimal FROM t5)::decimal FROM t2)::decimal FROM t1 GROUP BY t1.e::string"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT sum(t1.a + t2.c) FROM t2) FROM t1 GROUP BY t1.e", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT sum(t1.a + t2.c) FROM t2) FROM t1 GROUP BY t1.a", &[]),
        @"SELECT (SELECT sum((t1.a::int + t2.c::int)::int)::decimal FROM t2)::decimal FROM t1 GROUP BY t1.a::int"
    );
}

/// A subquery as the grouping key grants exactly the projections that match
/// it: the column beside it still needs a key of its own. Note the first case
/// needs no grant at all - an uncorrelated subquery reads nothing of this
/// level.
#[test]
fn subquery_key_grants_only_its_own_projection() {
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT c FROM t5), t1.a FROM t1 GROUP BY (SELECT c FROM t5), t1.a", &[]),
        @"SELECT (SELECT t5.c::int FROM t5)::int, t1.a::int FROM t1 GROUP BY (SELECT t5.c::int FROM t5)::int, t1.a::int"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT c FROM t5), t1.a FROM t1 GROUP BY (SELECT c FROM t5)", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// WHERE and a join's ON run before the grouping, so their columns are never
/// held to it - in either direction between the clause and the keys, and with
/// the ON reading raw columns of an expression-grouped level.
#[test]
fn where_and_join_on_are_not_held_to_the_grouping() {
    insta::assert_snapshot!(
        analyzed("SELECT count(*) FROM t1 WHERE a + b > 0 GROUP BY c", &[]),
        @"SELECT count(*)::int FROM t1 WHERE ((t1.a::int + t1.b::int)::int > 0::int)::bool GROUP BY t1.c::double"
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(*) FROM t1 WHERE c > 0 GROUP BY a + b", &[]),
        @"SELECT count(*)::int FROM t1 WHERE (t1.c::double > CAST(0 AS double))::bool GROUP BY (t1.a::int + t1.b::int)::int"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT t3.a + t5.c FROM t3 INNER JOIN t5 ON t3.a = t5.c GROUP BY t3.a + t5.c",
            &[],
        ),
        @"SELECT (t3.a::int + t5.c::int)::int FROM t3 INNER JOIN t5 ON (t3.a::int = t5.c::int)::bool GROUP BY (t3.a::int + t5.c::int)::int"
    );
}

/// DISTINCT runs after the grouping and changes nothing about it: an
/// expression-keyed list may be DISTINCT, and DISTINCT buys no exemption.
#[test]
fn distinct_select_list_is_held_to_the_expression_keys() {
    insta::assert_snapshot!(
        analyzed("SELECT DISTINCT t1.a + t1.c FROM t1 GROUP BY t1.a + t1.c", &[]),
        @"SELECT DISTINCT (CAST(t1.a AS double) + t1.c::double)::double FROM t1 GROUP BY (CAST(t1.a AS double) + t1.c::double)::double"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT DISTINCT t1.a FROM t1 GROUP BY t1.a + t1.c", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// Known gaps. Keys are compared as bound trees before types are derived, so
/// two spellings Postgres folds to one expression stay two keys here. Postgres
/// accepts every query below.
#[test]
fn select_list_expression_grouping_key_gap() {
    // Two spellings of one constant, in HAVING and in the select list.
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 GROUP BY t1.a + 1 HAVING t1.a + 01 > 0", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT t1.a + 1 FROM t1 GROUP BY t1.a + 01", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // The cast the projection gets implicitly, spelled out in the key.
    insta::assert_snapshot!(
        analyze_error("SELECT t1.a + t1.d FROM t1 GROUP BY t1.a::decimal + t1.d", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}
