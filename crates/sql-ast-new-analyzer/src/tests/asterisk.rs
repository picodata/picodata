//! Expansion of `*` and `t.*`.
//!
//! # Cases
//! An asterisk stands for as many projections as its source has columns, so a
//! case pins the whole expanded list — which columns came out, in what order
//! and with which types — rather than mere acceptance.
//!
//! Sections follow what the asterisk expands *over*, since that is what decides
//! the answer: a base table, a CTE, a subquery, a join. The join is the
//! interesting one, because USING merges a pair of columns into a single one
//! that must then appear exactly once.
//!
//! # Rejections
//! A qualifier naming nothing in the FROM clause is reported as a missing
//! FROM-clause entry, which is a different error from the unresolvable column
//! reference the same qualifier produces on `t.a`. Both spellings are pinned —
//! the second one in [`super::expr`] — so the two messages cannot drift into
//! one another.

use super::{analyze_error, analyzed};
use sql_ast_new_nodes::AbstractSyntaxTree;
use sql_frontend::frontend::sql::Ast;

// ------------- Unqualified -------------

#[test]
fn asterisk_from_table() {
    let query = "SELECT * FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.a::int, t1.b::int, t1.c::double, t1.d::decimal, t1.e::string, t1.f::string, t1.g::bool FROM t1");
}

#[test]
fn asterisk_and_exprs_mixed_query() {
    let query = "SELECT *, b * 2 AS a1, *, t1.a FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.a::int, t1.b::int, t1.c::double, t1.d::decimal, t1.e::string, t1.f::string, t1.g::bool, (t1.b::int * 2::int)::int AS a1, t1.a::int, t1.b::int, t1.c::double, t1.d::decimal, t1.e::string, t1.f::string, t1.g::bool, t1.a::int FROM t1");
}

#[test]
fn asterisk_no_table_expr_query() {
    let query = "SELECT *";
    let err = AbstractSyntaxTree::new(query)
        .err()
        .expect("expected error in AST building");

    let err_str = err.to_string();
    insta::assert_snapshot!(
        err_str,
        @"invalid query: cannot use asterisk '*' in select list without table expression"
    );
}

// ------------- Qualified -------------

#[test]
fn qualified_asterisk_from_table() {
    let query = "SELECT t1.* FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.a::int, t1.b::int, t1.c::double, t1.d::decimal, t1.e::string, t1.f::string, t1.g::bool FROM t1");
}

#[test]
fn qualified_asterisk_by_alias() {
    let query = "SELECT x.* FROM t1 AS x";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT x.a::int, x.b::int, x.c::double, x.d::decimal, x.e::string, x.f::string, x.g::bool FROM t1 AS x");
}

#[test]
fn incosistent_table_qualifier_asterisk() {
    let query = "SELECT t.* FROM t1";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @r#"failed to analyze AST: missing FROM-clause entry for table "t""#
    );
}

#[test]
fn qualified_asterisk_matches_nothing() {
    let query = "SELECT y.* FROM t1";
    insta::assert_snapshot!(analyze_error(query, &[]), @r#"failed to analyze AST: missing FROM-clause entry for table "y""#);
}

#[test]
fn qualified_asterisk_matches_nothing_aliased_target() {
    let query = "SELECT y.* FROM t1 AS x";
    insta::assert_snapshot!(analyze_error(query, &[]), @r#"failed to analyze AST: missing FROM-clause entry for table "y""#);
}

#[test]
fn mixed_qualified_asterisk_matches_nothing() {
    let query = "SELECT a, y.* FROM t1";
    insta::assert_snapshot!(analyze_error(query, &[]), @r#"failed to analyze AST: missing FROM-clause entry for table "y""#);
}

#[test]
fn mixed_qualified_asterisk_matches_nothing_aliased_target() {
    let query = "SELECT a, y.* FROM t1 AS x";
    insta::assert_snapshot!(analyze_error(query, &[]), @r#"failed to analyze AST: missing FROM-clause entry for table "y""#);
}

#[test]
fn mixed_qualified_asterisk_matches_nothing_due_to_alias() {
    let query = "SELECT a, t1.* FROM t1 AS x";
    insta::assert_snapshot!(analyze_error(query, &[]), @r#"failed to analyze AST: missing FROM-clause entry for table "t1""#);
}

#[test]
fn qualified_asterisk_wrong_name_on_subquery() {
    let query = "SELECT y.* FROM (SELECT 1 AS a) AS s";
    insta::assert_snapshot!(analyze_error(query, &[]), @r#"failed to analyze AST: missing FROM-clause entry for table "y""#);
}

#[test]
fn asterisk_qualifier_reference_to_nonexisting_table() {
    let query = "SELECT bogus.* FROM (SELECT 1 AS a)";
    insta::assert_snapshot!(analyze_error(query, &[]), @r#"failed to analyze AST: missing FROM-clause entry for table "bogus""#);
}

// ------------- Over a CTE -------------

#[test]
fn asterisk_from_cte() {
    let query = "WITH cte AS (SELECT 1 AS a) SELECT * FROM cte";
    insta::assert_snapshot!(analyzed(query, &[]), @"WITH cte AS (SELECT 1::int AS a) SELECT cte.a::int FROM cte");
}

#[test]
fn asterisk_from_cte_with_anon_column() {
    let query = "WITH cte (a) AS (SELECT 1) SELECT * FROM cte";
    insta::assert_snapshot!(analyzed(query, &[]), @"WITH cte (a) AS (SELECT 1::int) SELECT cte.a::int FROM cte");
}

#[test]
fn asterisk_from_cte_with_anon_columns() {
    let query = "WITH cte AS (SELECT 1, 2) SELECT * FROM cte";
    insta::assert_snapshot!(analyzed(query, &[]), @"WITH cte AS (SELECT 1::int, 2::int) SELECT ?column?::int, ?column?::int FROM cte");
}

#[test]
fn asterisk_from_cte_with_column_list_over_table_columns() {
    // The CTE body projects real table columns (asterisk over `t1`), and the
    // CTE has an explicit column list: the columns keep their underlying
    // column attribute and take the list's names, the same rename a projection
    // gets.
    let query = "WITH cte (c1, c2, c3, c4, c5, c6, c7) AS (SELECT * FROM t1) SELECT * FROM cte";
    insta::assert_snapshot!(analyzed(query, &[]), @"WITH cte (c1, c2, c3, c4, c5, c6, c7) AS (SELECT t1.a::int, t1.b::int, t1.c::double, t1.d::decimal, t1.e::string, t1.f::string, t1.g::bool FROM t1) SELECT cte.c1::int, cte.c2::int, cte.c3::double, cte.c4::decimal, cte.c5::string, cte.c6::string, cte.c7::bool FROM cte");
}

#[test]
fn ambigious_column_reference_asterisk_cte() {
    let query = "WITH cte (a, a) AS (SELECT 1, 2) SELECT * FROM cte;";
    insta::assert_snapshot!(analyzed(query, &[]), @"WITH cte (a, a) AS (SELECT 1::int, 2::int) SELECT cte.a::int, cte.a::int FROM cte");
}

// ------------- Over a subquery -------------

#[test]
fn correlated_subquery_qualified_asterisk() {
    let query = "SELECT (SELECT t.* FROM t1) FROM (SELECT 1 a) t";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (SELECT t.a::int FROM t1)::int FROM (SELECT 1::int AS a) AS t");
}

/// A qualified asterisk naming an enclosing query's relation expands into
/// references of *that* level, so they answer to its grouping exactly as the
/// written `t.a` does. Booking them on the inner level - which is not grouped -
/// would hide them from the check.
#[test]
fn correlated_subquery_qualified_asterisk_is_checked_against_the_outer_grouping() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(*), (SELECT t.* FROM t2) FROM (SELECT 1 AS a) t", &[]),
        @r#"failed to analyze AST: column "t.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // The same query spelled with the column, for the verdict to line up with.
    insta::assert_snapshot!(
        analyze_error("SELECT count(*), (SELECT t.a FROM t2) FROM (SELECT 1 AS a) t", &[]),
        @r#"failed to analyze AST: column "t.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(*), (SELECT t.* FROM t2) FROM (SELECT 1 AS a) t GROUP BY t.a", &[]),
        @"SELECT count(*)::int, (SELECT t.a::int FROM t2)::int FROM (SELECT 1::int AS a) AS t GROUP BY t.a::int"
    );
}

/// Which GROUP BY the expansion is matched against, and how. An expression
/// key is matched at the level the columns belong to, so the outer `t.a` covers
/// them and the inner GROUP BY has no say over them. An ordinal key, though,
/// names a position in its own select list, and the expansion's positions in
/// the *inner* list mean nothing to the outer ordinals: in the third query the
/// inner `t.*` puts `t.a` at position 1, and the outer `GROUP BY 1` - which
/// groups by `b` - must not be read as covering it. The last query shows what
/// does cover it: the explicit `a` key.
#[test]
fn correlated_subquery_qualified_asterisk_matches_only_the_outer_expression_keys() {
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT t.* FROM t2 GROUP BY t2.d) FROM (SELECT 1 AS a) t GROUP BY t.a", &[]),
        @"SELECT (SELECT t.a::int FROM t2 GROUP BY t2.d::string)::int FROM (SELECT 1::int AS a) AS t GROUP BY t.a::int"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT t.* FROM t2 GROUP BY t2.d) FROM (SELECT 1 AS a) t GROUP BY t.a + 1", &[]),
        @r#"failed to analyze AST: column "t.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT b, EXISTS (SELECT t.* FROM t2) FROM (SELECT 1 AS a, 2 AS b) t GROUP BY 1", &[]),
        @r#"failed to analyze AST: column "t.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyzed("SELECT b, EXISTS (SELECT t.* FROM t2) FROM (SELECT 1 AS a, 2 AS b) t GROUP BY 1, a", &[]),
        @"SELECT t.b::int, EXISTS (SELECT t.a::int, t.b::int FROM t2)::bool FROM (SELECT 1::int AS a, 2::int AS b) AS t GROUP BY 1, t.a::int"
    );
}

#[test]
fn ambigious_column_reference_asterisk_subquery() {
    let query = "SELECT * FROM (SELECT 1 AS a, 2 AS a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT a::int, a::int FROM (SELECT 1::int AS a, 2::int AS a)");
}

// ------------- Over a join -------------

#[test]
fn join_asterisk_unqualified() {
    let query =
        "SELECT * FROM (SELECT a, b FROM t1) t1 INNER JOIN (SELECT c a, d b FROM t2) t2 USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.a::int, t1.b::int, t2.b::string FROM (SELECT t1.a::int, t1.b::int FROM t1) AS t1 INNER JOIN (SELECT t2.c::int AS a, t2.d::string AS b FROM t2) AS t2 USING (a)");
}

#[test]
fn join_asterisk_unqualified_cte_and_table() {
    let query =
        "WITH cte (a, b) AS (SELECT 1, 2) SELECT * FROM t1 INNER JOIN (SELECT c a, d b FROM t2) t2 USING (a) INNER JOIN cte USING(a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"WITH cte (a, b) AS (SELECT 1::int, 2::int) SELECT t1.a::int, t1.b::int, t1.c::double, t1.d::decimal, t1.e::string, t1.f::string, t1.g::bool, t2.b::string, cte.b::int FROM t1 INNER JOIN (SELECT t2.c::int AS a, t2.d::string AS b FROM t2) AS t2 USING (a) INNER JOIN cte USING (a)");
}

#[test]
fn join_asterisk_qualified() {
    let query = "SELECT t1.*, t2.*, t3.* FROM (SELECT a, b FROM t1) t1 INNER JOIN (SELECT c, d FROM t2) t2 ON t1.a = t2.c INNER JOIN (SELECT c a, d b FROM t2) t3 USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.a::int, t1.b::int, t2.c::int, t2.d::string, t3.a::int, t3.b::string FROM (SELECT t1.a::int, t1.b::int FROM t1) AS t1 INNER JOIN (SELECT t2.c::int, t2.d::string FROM t2) AS t2 ON (t1.a::int = t2.c::int)::bool INNER JOIN (SELECT t2.c::int AS a, t2.d::string AS b FROM t2) AS t3 USING (a)");
}

#[test]
fn left_join_asterisk_qualified() {
    let query = "SELECT t1.*, t2.*, t3.* FROM (SELECT a, b FROM t1) t1 LEFT JOIN (SELECT c, d FROM t2) t2 ON t1.a = t2.c LEFT JOIN (SELECT c a, d b FROM t2) t3 USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.a::int, t1.b::int, t2.c::int, t2.d::string, t3.a::int, t3.b::string FROM (SELECT t1.a::int, t1.b::int FROM t1) AS t1 LEFT OUTER JOIN (SELECT t2.c::int, t2.d::string FROM t2) AS t2 ON (t1.a::int = t2.c::int)::bool LEFT OUTER JOIN (SELECT t2.c::int AS a, t2.d::string AS b FROM t2) AS t3 USING (a)");
}

// ------------- JOIN/USING -------------
#[test]
fn join_asterisk_merged_column_leads_the_expansion() {
    let query = "SELECT * FROM t3 INNER JOIN t4 USING (b)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t3.b::string, t3.a::int, t4.a::int FROM t3 INNER JOIN t4 USING (b)");
}

/// Merged columns follow the USING list, not the table's column order.
#[test]
fn join_asterisk_merged_columns_follow_the_using_order() {
    insta::assert_snapshot!(
        analyzed("SELECT * FROM t3 INNER JOIN t4 USING (a, b)", &[]),
        @"SELECT t3.a::int, t3.b::string FROM t3 INNER JOIN t4 USING (a, b)"
    );
    insta::assert_snapshot!(
        analyzed("SELECT * FROM t3 INNER JOIN t4 USING (b, a)", &[]),
        @"SELECT t3.b::string, t3.a::int FROM t3 INNER JOIN t4 USING (b, a)"
    );
}

/// The merge is the *left* column, and an outer join is what makes the choice
/// observable: for a row `t4` did not match, `t4.a` is null where `t3.a` is not.
#[test]
fn join_asterisk_merge_keeps_the_left_column() {
    let query = "SELECT * FROM t3 LEFT JOIN t4 USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t3.a::int, t3.b::string, t4.b::string FROM t3 LEFT OUTER JOIN t4 USING (a)");
}

/// A name merged by every join of a chain is emitted once, and it is the
/// leftmost column - not the last one merged.
#[test]
fn join_asterisk_chain_merging_one_name_keeps_the_leftmost() {
    let query = "SELECT * FROM t3 x LEFT JOIN t3 y USING (a) LEFT JOIN t3 z USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT x.a::int, x.b::string, y.b::string, z.b::string FROM t3 AS x LEFT OUTER JOIN t3 AS y USING (a) LEFT OUTER JOIN t3 AS z USING (a)");
}

/// Two joins merging *different* names: the outer join's merge (`r`) leads the
/// inner one's (`p`), which in turn leads everything left unmerged.
#[test]
fn join_asterisk_outermost_merge_leads() {
    let query = "SELECT * FROM (SELECT 1 p, 2 q) x INNER JOIN (SELECT 1 p, 3 r) y USING (p) INNER JOIN (SELECT 3 r, 4 s) z USING (r)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT y.r::int, x.p::int, x.q::int, z.s::int FROM (SELECT 1::int AS p, 2::int AS q) AS x INNER JOIN (SELECT 1::int AS p, 3::int AS r) AS y USING (p) INNER JOIN (SELECT 3::int AS r, 4::int AS s) AS z USING (r)");
}

/// Merging every column of both inputs leaves the merges as the whole result.
#[test]
fn join_asterisk_all_columns_merged() {
    let query = "SELECT * FROM t6 x INNER JOIN t6 y USING (k1, k2, v)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT x.k1::int, x.k2::int, x.v::int FROM t6 AS x INNER JOIN t6 AS y USING (k1, k2, v)");
}

/// Merged and unmerged columns of one input interleave by the same rule: the
/// merges first, then what each input has left, left input before right.
#[test]
fn join_asterisk_partial_merge_orders_the_remainder() {
    let query = "SELECT * FROM t6 x INNER JOIN t6 y USING (v, k2)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT x.v::int, x.k2::int, x.k1::int, y.k1::int FROM t6 AS x INNER JOIN t6 AS y USING (v, k2)");
}

/// A *qualified* asterisk names one relation, not the join output, so the merge
/// hides nothing from it: `t4.*` still yields the copy the merge withheld.
#[test]
fn join_qualified_asterisk_is_not_reduced() {
    let query = "SELECT t4.*, * FROM t3 INNER JOIN t4 USING (b)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t4.a::int, t4.b::string, t3.b::string, t3.a::int, t4.a::int FROM t3 INNER JOIN t4 USING (b)");
}

/// Two asterisks each expand to the whole join output, merge included.
#[test]
fn join_asterisk_twice_expands_the_merge_twice() {
    let query = "SELECT *, * FROM t3 INNER JOIN t4 USING (b)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t3.b::string, t3.a::int, t4.a::int, t3.b::string, t3.a::int, t4.a::int FROM t3 INNER JOIN t4 USING (b)");
}

/// The merge is found by name, not by position: here `p` sits second in the
/// left input and first in the right one.
#[test]
fn join_asterisk_merge_over_derived_tables_reordering_columns() {
    let query = "SELECT * FROM (SELECT b q, a p FROM t3) x INNER JOIN (SELECT a p, b q FROM t4) y USING (p)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT x.p::int, x.q::string, y.q::string FROM (SELECT t3.b::string AS q, t3.a::int AS p FROM t3) AS x INNER JOIN (SELECT t4.a::int AS p, t4.b::string AS q FROM t4) AS y USING (p)");
}

/// A join wrapped in a derived table exposes an ordinary three-column relation:
/// the merge happened inside, and the two `b` columns keep both their names.
#[test]
fn join_asterisk_over_a_derived_table_holding_the_join() {
    let query = "SELECT * FROM (SELECT * FROM t3 LEFT JOIN t4 USING (a)) z";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT z.a::int, z.b::string, z.b::string FROM (SELECT t3.a::int, t3.b::string, t4.b::string FROM t3 LEFT OUTER JOIN t4 USING (a)) AS z");
}

#[test]
fn join_asterisk_mixed() {
    let query = "SELECT *, t2.* FROM (SELECT a, b FROM t1) t1 INNER JOIN (SELECT c a, d b FROM t2) t2 USING (a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT t1.a::int, t1.b::int, t2.b::string, t2.a::int, t2.b::string FROM (SELECT t1.a::int, t1.b::int FROM t1) AS t1 INNER JOIN (SELECT t2.c::int AS a, t2.d::string AS b FROM t2) AS t2 USING (a)");
}
