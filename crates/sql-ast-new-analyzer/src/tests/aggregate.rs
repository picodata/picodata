//! # Aggregates
//!
//! Which clause may hold an aggregate, which level a call belongs to, and what
//! may not be nested in its arguments: another call of the same level, or a
//! CTE defined below it.

use super::{analyze_error, analyzed};

// ------------- Aggregate placement -------------

#[test]
fn aggregate_in_select_list() {
    insta::assert_snapshot!(
        analyzed("SELECT sum(a) FROM t1 WHERE b > 0", &[]),
        @"SELECT sum(t1.a::int)::decimal FROM t1 WHERE (t1.b::int > 0::int)::bool"
    );
}

#[test]
fn aggregate_in_select_list_subquery() {
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT sum(t1.a) FROM t2) FROM t1 WHERE b > 0", &[]),
        @"SELECT (SELECT sum(t1.a::int)::decimal FROM t2)::decimal FROM t1 WHERE (t1.b::int > 0::int)::bool"
    );
}

#[test]
fn aggregate_in_where_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 WHERE sum(a) > 1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in WHERE"
    );
}

#[test]
fn aggregate_in_where_no_args_defaults_to_curr_level() {
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 WHERE sum(1) > 1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in WHERE"
    );
}

#[test]
fn aggregate_in_where_reference_outer_from_frame_under_analysis() {
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 WHERE (SELECT sum(t1.a) FROM t2) > 1;", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in WHERE"
    );
}

#[test]
fn aggregate_in_where_subquery_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 WHERE (SELECT sum(a) > 1)", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in WHERE"
    );
}

#[test]
fn aggregate_in_where_subquery_with_from() {
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t1 WHERE (SELECT sum(a) > 1 FROM t1)", &[]),
        @"SELECT t1.a::int FROM t1 WHERE (SELECT (sum(t1.a::int)::decimal > CAST(1 AS decimal))::bool FROM t1)::bool"
    );
}

#[test]
fn aggregate_in_where_subquery_with_from_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 WHERE (SELECT sum(t1.a) FROM t2) > 1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in WHERE"
    );
}

#[test]
fn aggregate_in_where_subquery_count_asterisk() {
    insta::assert_snapshot!(
        analyzed("SELECT * FROM t1 WHERE (SELECT count(*) > 0)", &[]),
        @"SELECT t1.a::int, t1.b::int, t1.c::double, t1.d::decimal, t1.e::string, t1.f::string, t1.g::bool FROM t1 WHERE (SELECT (count(*)::int > 0::int)::bool)::bool"
    );
}

#[test]
fn aggregate_in_where_subquery_projection_is_allowed() {
    // The subquery's select list is a Projection clause of its own even
    // though the subquery sits in the enclosing WHERE.
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t1 WHERE b = (SELECT count(*) FROM t3)", &[]),
        @"SELECT t1.a::int FROM t1 WHERE (t1.b::int = (SELECT count(*)::int FROM t3)::int)::bool"
    );
}

#[test]
fn aggregate_in_join_on_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t3 INNER JOIN t4 ON count(*) = 1", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in JOIN conditions"
    );
}

#[test]
fn aggregate_in_join_using_references_outward_relation() {
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT t1.a FROM t1 INNER JOIN t1 t2 ON sum(t3.a) > 1) FROM t1 t3", &[]),
        @"SELECT (SELECT t1.a::int FROM t1 INNER JOIN t1 AS t2 ON (sum(t3.a::int)::decimal > CAST(1 AS decimal))::bool)::int FROM t1 AS t3"
    );
}

#[test]
fn aggregate_over_subquery_aggregate_is_allowed() {
    // A subquery is a statement boundary: its aggregates are its own.
    insta::assert_snapshot!(
        analyzed("SELECT sum((SELECT count(*) FROM t3)) FROM t1", &[]),
        @"SELECT sum((SELECT count(*)::int FROM t3)::int)::decimal FROM t1"
    );
}

// ------------- Nested aggregates -------------
//
// Reject an aggregate whose argument contains another aggregate of the
// *same semantic level*. A call's semantic level is the minimum over the levels
// of the columns it reads and of the aggregates nested in it,
// counted in the frame of the call itself; a call that reads nothing
// belongs to the query level it is written at. So nesting is not a syntactic
// property: the same two calls are legal or illegal depending on what they read.

#[test]
fn nested_aggregate_at_same_query_level_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(max(a)) FROM t1", &[]),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
    // DISTINCT does not open a new level either.
    insta::assert_snapshot!(
        analyze_error("SELECT count(DISTINCT max(a)) FROM t1", &[]),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
}

#[test]
fn nested_aggregate_through_scalar_call_error() {
    // A scalar call is not a level boundary, so the check reaches through it.
    insta::assert_snapshot!(
        analyze_error("SELECT sum(abs(count(a))) FROM t1", &[]),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
}

#[test]
fn nested_aggregate_count_asterisk_error() {
    // `count(*)` reads no column, so it belongs to the level it is written at -
    // here the same level as the enclosing `sum`.
    insta::assert_snapshot!(
        analyze_error("SELECT sum(count(*)) FROM t1", &[]),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
}

#[test]
fn aggregate_over_subquery_local_aggregate_is_allowed() {
    // `max` reads only the subquery's own relation, so it belongs to the subquery
    // and never meets `sum`.
    insta::assert_snapshot!(
        analyzed("SELECT sum((SELECT max(t2.c) FROM t2)) FROM t1", &[]),
        @"SELECT sum((SELECT max(t2.c::int)::int FROM t2)::int)::decimal FROM t1"
    );
}

#[test]
fn aggregate_over_subquery_correlated_aggregate_error() {
    // Same shape as above, but `max` reads the outer relation, which lifts it back
    // to `sum`'s level.
    insta::assert_snapshot!(
        analyze_error("SELECT sum((SELECT max(t1.a) FROM t2)) FROM t1", &[]),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
}

#[test]
fn aggregate_ignores_columns_local_to_inner_subquery() {
    // `t2.c` lives below `sum`, so it does not contribute a level; `a` does.
    insta::assert_snapshot!(
        analyzed("SELECT sum(a + (SELECT t2.c FROM t2)) FROM t1", &[]),
        @"SELECT sum((t1.a::int + (SELECT t2.c::int FROM t2)::int)::int)::decimal FROM t1"
    );
}

#[test]
fn aggregate_semantic_level_is_the_innermost_column_level() {
    // Reading both an outer and a local column puts `sum` at the local level.
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT sum(t1.a + t2.c) FROM t2) FROM t1", &[]),
        @"SELECT (SELECT sum((t1.a::int + t2.c::int)::int)::decimal FROM t2)::decimal FROM t1"
    );
}

#[test]
fn nested_aggregate_sharing_the_subquery_level_error() {
    // `sum` reads an outer column, `max` a local one, yet both end up at the
    // subquery's level: `sum`'s level is the minimum of the two.
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT sum(t1.a + max(t2.c)) FROM t2) FROM t1", &[]),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
    // Mirror image: `max` reads both levels, so its own minimum drags it down to
    // meet `sum` rather than the other way round.
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT sum(t2.c + max(t1.a + t2.c)) FROM t2) FROM t1", &[]),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
}

#[test]
fn nested_aggregate_above_own_semantic_level_is_allowed() {
    // The mirror of the case above: `max` belongs to the outer query, `sum` to the
    // subquery, so they are aggregates of two different queries.
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT sum(t2.c + max(t1.a)) FROM t2) FROM t1", &[]),
        @"SELECT (SELECT sum((t2.c::int + max(t1.a::int)::int)::int)::decimal FROM t2)::decimal FROM t1"
    );
}

#[test]
fn nested_aggregate_verdict_is_argument_order_independent() {
    // The check runs once per aggregate, after all of its arguments are bound;
    // validating per argument would make these two disagree.
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT sum(t1.a + abs(count(t2.c))) FROM t2) FROM t1", &[]),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT sum(abs(count(t2.c)) + t1.a) FROM t2) FROM t1", &[]),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
}

#[test]
fn nested_aggregate_across_from_subquery_frame_error() {
    // A FROM-clause subquery is analyzed before its own query pushes a frame, so
    // it is the one shape where the two level counters can drift apart.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT sum((SELECT s.y FROM (SELECT max(t1.a) AS y FROM t2) s)) FROM t1",
            &[]
        ),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
}

#[test]
fn nested_aggregate_across_cte_frame_error() {
    // Same drift hazard as above, through a CTE body instead of a derived table.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT sum((WITH cte AS (SELECT max(t1.a) AS z FROM t2) SELECT cte.z FROM cte)) FROM t1",
            &[]
        ),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
}

#[test]
fn nested_aggregate_two_subquery_hops() {
    // Two hops down, still correlated all the way up to `sum`'s relation.
    insta::assert_snapshot!(
        analyze_error("SELECT sum((SELECT (SELECT max(t1.a) FROM t3) FROM t2)) FROM t1", &[]),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
    // Two hops down and correlated to only one of them: `max` stops one level
    // short of `sum`.
    insta::assert_snapshot!(
        analyzed("SELECT sum((SELECT (SELECT max(t2.c) FROM t3) FROM t2)) FROM t1", &[]),
        @"SELECT sum((SELECT (SELECT max(t2.c::int)::int FROM t3)::int FROM t2)::int)::decimal FROM t1"
    );
}

#[test]
fn nested_aggregate_checked_against_every_sibling() {
    // `max` sits one level above `sum`, `min` at its level: the first sibling must
    // not stop the scan before the second one is examined.
    insta::assert_snapshot!(
        analyze_error("SELECT (SELECT sum(max(t1.a) + min(t2.c)) FROM t2) FROM t1", &[]),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
    // Both siblings above `sum`, which its own local column pins to the subquery.
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT sum(t2.c + max(t1.a) + min(t1.a)) FROM t2) FROM t1", &[]),
        @"SELECT (SELECT sum(((t2.c::int + max(t1.a::int)::int)::int + min(t1.a::int)::int)::int)::decimal FROM t2)::decimal FROM t1"
    );
}

#[test]
fn nested_aggregate_propagated_through_intermediate_aggregate_error() {
    // `max` belongs to the subquery and so is invisible to `sum` on its own, but
    // the `min` it carries belongs to `sum`'s level and must still be reported.
    insta::assert_snapshot!(
        analyze_error("SELECT sum((SELECT max(t2.c + min(t1.a)) FROM t2)) FROM t1", &[]),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
    // Two hops down: `min` reads `t2.c` and so belongs to the middle subquery,
    // and `max`, that subquery's own call, is dragged to the same level twice
    // over - by the column, which every enclosing call registers, and by `min`
    // when it finalizes. The collision is `max` against `min` at the middle
    // level; `sum` never sees either of them.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT sum((SELECT max((SELECT min(t2.c) FROM t3)) FROM t2)) FROM t1",
            &[]
        ),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
}

#[test]
fn aggregate_reads_columns_inside_a_discarded_nested_aggregate() {
    // `max` belongs to the innermost subquery, so `sum` discards it - but the
    // `t2.c` it reads belongs to `sum`'s own level and still counts, which puts
    // `sum` one level below `count` and makes the pair legal.
    insta::assert_snapshot!(
        analyzed(
            "SELECT (SELECT sum((SELECT max(t3.a + t2.c) FROM t3) + count(t1.a)) FROM t2) FROM t1",
            &[]
        ),
        @"SELECT (SELECT sum(((SELECT max((t3.a::int + t2.c::int)::int)::int FROM t3)::int + count(t1.a::int)::int)::int)::decimal FROM t2)::decimal FROM t1"
    );
    // Drop that one column and nothing is left to read at `sum`'s level, so it
    // falls back onto `count`'s and the same two calls are now nested.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT (SELECT sum((SELECT max(t3.a) FROM t3) + count(t1.a)) FROM t2) FROM t1",
            &[]
        ),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
}

#[test]
fn discarded_nested_aggregate_is_read_through_in_either_operand_order() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT (SELECT sum(min(t1.a) + (SELECT max(t3.a + t2.c) FROM t3)) FROM t2) FROM t1",
            &[]
        ),
        @"SELECT (SELECT sum((min(t1.a::int)::int + (SELECT max((t3.a::int + t2.c::int)::int)::int FROM t3)::int)::int)::decimal FROM t2)::decimal FROM t1"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT (SELECT sum(min(t1.a) + (SELECT max(t3.a) FROM t3)) FROM t2) FROM t1",
            &[]
        ),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
}

/// A column an asterisk expands to is a read of its level even when a grouping
/// key covers it: the key lifts the column's grouping duty, not the level the
/// enclosing calls read. Here `s.*` is `s.a`, a key of the outer level, so the
/// inner `sum` belongs to that level and the outer `sum` nests it - exactly as
/// with `s.a` written out.
#[test]
fn nested_aggregate_through_asterisk_expanding_to_a_grouping_key_error() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT sum((SELECT sum((SELECT s.* FROM t2)) FROM t2)) FROM (SELECT a FROM t1) s GROUP BY a::int",
            &[]
        ),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT sum((SELECT sum((SELECT s.a FROM t2)) FROM t2)) FROM (SELECT a FROM t1) s GROUP BY a::int",
            &[]
        ),
        @"failed to analyze AST: aggregate function calls cannot be nested"
    );
    // Without the outer call there is nothing to nest, key or no key.
    insta::assert_snapshot!(
        analyzed(
            "SELECT (SELECT sum((SELECT s.* FROM t2)) FROM t2) FROM (SELECT a FROM t1) s GROUP BY a::int",
            &[]
        ),
        @"SELECT (SELECT sum((SELECT s.a::int FROM t2)::int)::decimal FROM t2)::decimal FROM (SELECT t1.a::int FROM t1) AS s GROUP BY s.a::int"
    );
}

/// A column a nested call reads at a level *deeper* than the call enclosing it
/// can never be the enclosing call's semantic level, so it is not handed on to
/// that call but registered on its own level right away - where it still
/// answers to the level's grouping. Here `max` belongs to `t3`, which leaves
/// `t2.c` a plain read of the grouped `t2` level, and the `sum` written at
/// `t1` around all of it changes nothing about that.
#[test]
fn nested_aggregate_column_below_the_enclosing_call_is_still_checked() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT sum((SELECT (SELECT max(t2.c + t3.a) FROM t1 t3) FROM t2 HAVING true)) FROM t1",
            &[]
        ),
        @r#"failed to analyze AST: column "t2.c" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT sum((SELECT (SELECT max(t2.c + t3.a) FROM t1 t3) FROM t2 GROUP BY t2.d)) FROM t1",
            &[]
        ),
        @r#"failed to analyze AST: column "t2.c" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    // The same read without the enclosing call, for the verdict to line up with.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT (SELECT max(t2.c + t3.a) FROM t1 t3) FROM t2 GROUP BY t2.d",
            &[]
        ),
        @r#"failed to analyze AST: column "t2.c" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn nested_aggregate_column_below_the_enclosing_call_is_not_a_blanket_rejection() {
    // `max` reads nothing but `t2.c`, so it belongs to `t2` and aggregates the
    // column itself.
    insta::assert_snapshot!(
        analyzed(
            "SELECT sum((SELECT (SELECT max(t2.c) FROM t1 t3) FROM t2 HAVING true)) FROM t1",
            &[]
        ),
        @"SELECT sum((SELECT (SELECT max(t2.c::int)::int FROM t1 AS t3)::int FROM t2 HAVING true::bool)::int)::decimal FROM t1"
    );
    // An ungrouped `t2` has nothing to hold the read to.
    insta::assert_snapshot!(
        analyzed(
            "SELECT sum((SELECT (SELECT max(t2.c + t3.a) FROM t1 t3) FROM t2)) FROM t1",
            &[]
        ),
        @"SELECT sum((SELECT (SELECT max((t2.c::int + t3.a::int)::int)::int FROM t1 AS t3)::int FROM t2)::int)::decimal FROM t1"
    );
    // A read of `max`'s own level is never in question.
    insta::assert_snapshot!(
        analyzed(
            "SELECT sum((SELECT (SELECT max(t3.a) FROM t1 t3) FROM t2 HAVING true)) FROM t1",
            &[]
        ),
        @"SELECT sum((SELECT (SELECT max(t3.a::int)::int FROM t1 AS t3)::int FROM t2 HAVING true::bool)::int)::decimal FROM t1"
    );
}

// ---- A CTE below the call's semantic level ----

/// A call belongs to the deepest level it reads, and a CTE it uses through a
/// subquery must be in scope at that level. Here `max` reads `t1.a` and so
/// belongs to `t1`, but `cte` is defined in the subquery below it. PostgreSQL
/// rejects the same way rather than guess how such a call could be evaluated.
#[test]
fn aggregate_cannot_use_a_cte_below_its_level() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT a, (WITH cte AS (SELECT 1) SELECT max((SELECT t1.a FROM cte))) b FROM t1 GROUP BY a",
            &[]
        ),
        @"failed to analyze AST: outer-level aggregate cannot use a nested CTE"
    );
    // Reading the CTE's own column alongside does not raise the level.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT a, (WITH cte AS (SELECT 1 AS x) SELECT max((SELECT t1.a + cte.x FROM cte))) b FROM t1 GROUP BY a",
            &[]
        ),
        @"failed to analyze AST: outer-level aggregate cannot use a nested CTE"
    );
    // The CTE beside another relation, or one more subquery hop down.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT a, (WITH cte AS (SELECT 1) SELECT max((SELECT t1.a FROM t2 JOIN cte ON true))) b FROM t1 GROUP BY a",
            &[]
        ),
        @"failed to analyze AST: outer-level aggregate cannot use a nested CTE"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT a, (WITH cte AS (SELECT 1) SELECT (SELECT max((SELECT t1.a FROM cte)))) b FROM t1 GROUP BY a",
            &[]
        ),
        @"failed to analyze AST: outer-level aggregate cannot use a nested CTE"
    );
    // The inner call is finished, and rejected, before the outer one is.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT a, (WITH cte AS (SELECT 1) SELECT sum(max((SELECT t1.a FROM cte)))) b FROM t1 GROUP BY a",
            &[]
        ),
        @"failed to analyze AST: outer-level aggregate cannot use a nested CTE"
    );
}

/// A CTE defined at the call's own level or above it is in scope wherever
/// the call belongs, and one local to a subquery of the call is that
/// subquery's business.
#[test]
fn aggregate_may_use_a_cte_at_or_above_its_level() {
    // At the call's level: `max` reads nothing outside and stays at its own level.
    insta::assert_snapshot!(
        analyzed("SELECT (WITH cte AS (SELECT 1 AS x) SELECT max((SELECT cte.x FROM cte))) b FROM t1", &[]),
        @"SELECT (WITH cte AS (SELECT 1::int AS x) SELECT max((SELECT cte.x::int FROM cte)::int)::int)::int AS b FROM t1"
    );
    // The call belongs to the subquery's own `t2`, where the CTE is in scope.
    insta::assert_snapshot!(
        analyzed(
            "SELECT a, (WITH cte AS (SELECT 1) SELECT max((SELECT t2.c FROM t2 JOIN cte ON true)) FROM t2) b FROM t1 GROUP BY a",
            &[]
        ),
        @"SELECT t1.a::int, (WITH cte AS (SELECT 1::int) SELECT max((SELECT t2.c::int FROM t2 INNER JOIN cte ON true::bool)::int)::int FROM t2)::int AS b FROM t1 GROUP BY t1.a::int"
    );
    // Above the call: the CTE of the outermost query.
    insta::assert_snapshot!(
        analyzed(
            "WITH cte AS (SELECT 1 AS x) SELECT a, (SELECT max((SELECT t1.a FROM cte))) b FROM t1 GROUP BY a",
            &[]
        ),
        @"WITH cte AS (SELECT 1::int AS x) SELECT t1.a::int, (SELECT max((SELECT t1.a::int FROM cte)::int)::int)::int AS b FROM t1 GROUP BY t1.a::int"
    );
    // Local to a subquery of the call, even though its body reads `t1.a` for `max`.
    insta::assert_snapshot!(
        analyzed("SELECT max((WITH cte AS (SELECT t1.a AS x FROM t2) SELECT x FROM cte)) FROM t1", &[]),
        @"SELECT max((WITH cte AS (SELECT t1.a::int AS x FROM t2) SELECT cte.x::int FROM cte)::int)::int FROM t1"
    );
}
