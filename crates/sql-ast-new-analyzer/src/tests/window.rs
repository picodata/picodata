//! Window functions and the named `WINDOW` clause.

use sql_ir::ir::types::{DerivedType, UnrestrictedType};

use super::{analyze_error, analyzed};

// ------------- Placement -------------

/// The select list is one of the two places a window function may appear.
#[test]
fn window_in_select_list() {
    insta::assert_snapshot!(
        analyzed("SELECT row_number() OVER () FROM t1", &[]),
        @"SELECT row_number() OVER ()::int FROM t1"
    );
}

/// Query-level ORDER BY is the other.
#[test]
fn window_in_order_by() {
    insta::assert_snapshot!(
        analyzed("SELECT b FROM t1 ORDER BY row_number() OVER (ORDER BY b)", &[]),
        @"SELECT t1.b::int FROM t1 ORDER BY row_number() OVER (ORDER BY t1.b::int ASC)::int ASC"
    );
}

#[test]
fn window_in_where_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT b FROM t1 WHERE row_number() OVER () = 1", &[]),
        @"semantic error: window functions are not allowed in WHERE"
    );
}

#[test]
fn window_in_group_by_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) FROM t1 GROUP BY row_number() OVER ()", &[]),
        @"semantic error: window functions are not allowed in GROUP BY"
    );
}

/// A grouping key that *is* a window expression.
#[test]
fn group_by_alias_or_ordinal_of_window_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT row_number() OVER () r FROM t1 GROUP BY r", &[]),
        @"semantic error: window functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT row_number() OVER () FROM t1 GROUP BY 1", &[]),
        @"semantic error: window functions are not allowed in GROUP BY"
    );
}

#[test]
fn window_in_having_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) FROM t1 HAVING row_number() OVER () = 1", &[]),
        @"semantic error: window functions are not allowed in HAVING"
    );
}

#[test]
fn window_in_join_on_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t3 JOIN t4 ON row_number() OVER () = 1", &[]),
        @"semantic error: window functions are not allowed in JOIN conditions"
    );
}

#[test]
fn window_in_values_error() {
    insta::assert_snapshot!(
        analyze_error("VALUES (row_number() OVER ())", &[]),
        @"semantic error: window functions are not allowed in VALUES"
    );
}

#[test]
fn window_in_subquery_in_where() {
    insta::assert_snapshot!(
        analyzed("SELECT b FROM t1 WHERE b IN (SELECT row_number() OVER () FROM t3)", &[]),
        @"SELECT t1.b::int FROM t1 WHERE (t1.b::int IN (SELECT row_number() OVER ()::int FROM t3)::int)::bool"
    );
}

// ------------- Nesting -------------

#[test]
fn nested_window_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(row_number() OVER ()) OVER () FROM t1", &[]),
        @"semantic error: window function calls cannot be nested"
    );
}

#[test]
fn window_in_aggregate_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(row_number() OVER ()) FROM t1", &[]),
        @"semantic error: aggregate function calls cannot contain window function calls"
    );
}

/// The nearest enclosing call names the error: here the aggregate,
/// even though a window function encloses it.
#[test]
fn window_in_aggregate_in_window_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(max(row_number() OVER ())) OVER () FROM t1", &[]),
        @"semantic error: aggregate function calls cannot contain window function calls"
    );
}

/// The other direction is legal because the aggregate runs first,
/// the window function ranges over the grouped rows.
#[test]
fn aggregate_in_window_args() {
    insta::assert_snapshot!(
        analyzed("SELECT sum(sum(a)) OVER () FROM t1 GROUP BY b", &[]),
        @"SELECT sum(sum(t1.a::int)::decimal) OVER ()::decimal FROM t1 GROUP BY t1.b::int"
    );
}

#[test]
fn subquery_resets_nesting() {
    insta::assert_snapshot!(
        analyzed("SELECT sum((SELECT row_number() OVER () FROM t3)) OVER () FROM t1", &[]),
        @"SELECT sum((SELECT row_number() OVER ()::int FROM t3)::int) OVER ()::decimal FROM t1"
    );
}

#[test]
fn window_fn_without_over_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT row_number() FROM t1", &[]),
        @"semantic error: window function row_number requires an OVER clause"
    );
}

// ------------- Window definitions -------------

#[test]
fn window_in_window_def_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ORDER BY row_number() OVER ()) FROM t1", &[]),
        @"semantic error: window functions are not allowed in window definitions"
    );
}

/// Aggregates are legal inside window definitions.
#[test]
fn aggregate_in_inline_window_def() {
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (PARTITION BY sum(a)) FROM t1 GROUP BY b", &[]),
        @"SELECT count(*) OVER (PARTITION BY sum(t1.a::int)::decimal)::int FROM t1 GROUP BY t1.b::int"
    );
}

#[test]
fn aggregate_in_named_window_def() {
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER w FROM t1 GROUP BY b WINDOW w AS (ORDER BY sum(a))", &[]),
        @"SELECT count(*) OVER w::int FROM t1 GROUP BY t1.b::int WINDOW w AS (ORDER BY sum(t1.a::int)::decimal ASC)"
    );
}

// ------------- Named windows -------------

#[test]
fn named_window_basic() {
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER w FROM t1 WINDOW w AS (PARTITION BY b)", &[]),
        @"SELECT count(*) OVER w::int FROM t1 WINDOW w AS (PARTITION BY t1.b::int)"
    );
}

#[test]
fn unknown_window_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER w FROM t1", &[]),
        @r#"semantic error: window "w" does not exist"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (w) FROM t1", &[]),
        @r#"semantic error: window "w" does not exist"#
    );
}

/// Windows resolve left to right, so a definition may reference only the
/// windows before it.
#[test]
fn forward_reference_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER w2 FROM t1 WINDOW w2 AS (w), w AS (ORDER BY b)", &[]),
        @r#"semantic error: window "w" does not exist"#
    );
    // The backward reference is fine.
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER w2 FROM t1 WINDOW w AS (ORDER BY b), w2 AS (w)", &[]),
        @"SELECT count(*) OVER w2::int FROM t1 WINDOW w AS (ORDER BY t1.b::int ASC), w2 AS (w)"
    );
}

#[test]
fn duplicate_window_error() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER w FROM t1 WINDOW w AS (PARTITION BY a), w AS (PARTITION BY b)",
            &[]
        ),
        @r#"semantic error: window "w" is already defined"#
    );
}

#[test]
fn order_by_override() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (w ORDER BY b) FROM t1 WINDOW w AS (ORDER BY a)",
            &[]
        ),
        @r#"semantic error: cannot override ORDER BY clause of window "w""#
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (w ORDER BY b) FROM t1 WINDOW w AS (PARTITION BY a)",
            &[]
        ),
        @"SELECT count(*) OVER (w ORDER BY t1.b::int ASC)::int FROM t1 WINDOW w AS (PARTITION BY t1.a::int)"
    );
}

/// PARTITION BY belongs to the base window alone, whether or not it has one.
#[test]
fn partition_by_override() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (w PARTITION BY b) FROM t1 WINDOW w AS ()",
            &[]
        ),
        @r#"semantic error: cannot override PARTITION BY clause of window "w""#
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT 1 FROM t1 WINDOW w AS (ORDER BY a), w2 AS (w PARTITION BY b)",
            &[]
        ),
        @r#"semantic error: cannot override PARTITION BY clause of window "w""#
    );
    // A missing base window is reported first.
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (nope PARTITION BY b) FROM t1", &[]),
        @r#"semantic error: window "nope" does not exist"#
    );
}

#[test]
fn frame_copy() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (w) FROM t1 WINDOW w AS (ORDER BY b ROWS UNBOUNDED PRECEDING)",
            &[]
        ),
        @r#"semantic error: cannot copy window "w" because it has a frame clause"#
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT 1 FROM t1 WINDOW w AS (ORDER BY b ROWS UNBOUNDED PRECEDING), w2 AS (w)",
            &[]
        ),
        @r#"semantic error: cannot copy window "w" because it has a frame clause"#
    );
    // The bare name uses the window as written, frame included.
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER w FROM t1 WINDOW w AS (ORDER BY b ROWS UNBOUNDED PRECEDING)",
            &[]
        ),
        @"SELECT count(*) OVER w::int FROM t1 WINDOW w AS (ORDER BY t1.b::int ASC ROWS UNBOUNDED PRECEDING)"
    );
}

#[test]
fn inheritance_chain() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (w2 ORDER BY b) FROM t1 WINDOW w AS (PARTITION BY a), w2 AS (w)",
            &[]
        ),
        @"SELECT count(*) OVER (w2 ORDER BY t1.b::int ASC)::int FROM t1 WINDOW w AS (PARTITION BY t1.a::int), w2 AS (w)"
    );
}

#[test]
fn subquery_cannot_see_outer_window() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT (SELECT count(*) OVER w FROM t3) FROM t1 WINDOW w AS (PARTITION BY b)",
            &[]
        ),
        @r#"semantic error: window "w" does not exist"#
    );
}

#[test]
fn order_by_sees_named_window() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT b FROM t1 WINDOW w AS (ORDER BY b) ORDER BY count(*) OVER w",
            &[]
        ),
        @"SELECT t1.b::int FROM t1 WINDOW w AS (ORDER BY t1.b::int ASC) ORDER BY count(*) OVER w::int ASC"
    );
}

/// Window names are identifiers: an unquoted name folds to lower case
/// wherever it is written, a quoted one keeps its case.
#[test]
fn window_name_case_and_quoting() {
    insta::assert_snapshot!(
        analyzed("SELECT row_number() OVER W FROM t1 WINDOW w AS ()", &[]),
        @"SELECT row_number() OVER w::int FROM t1 WINDOW w AS ()"
    );
    insta::assert_snapshot!(
        analyzed(r#"SELECT row_number() OVER "w" FROM t1 WINDOW W AS ()"#, &[]),
        @"SELECT row_number() OVER w::int FROM t1 WINDOW w AS ()"
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (W ORDER BY b) FROM t1 WINDOW w AS (PARTITION BY a)", &[]),
        @"SELECT count(*) OVER (w ORDER BY t1.b::int ASC)::int FROM t1 WINDOW w AS (PARTITION BY t1.a::int)"
    );
    insta::assert_snapshot!(
        analyzed(r#"SELECT row_number() OVER w FROM t1 WINDOW "w" AS (), "W" AS ()"#, &[]),
        @r#"SELECT row_number() OVER w::int FROM t1 WINDOW w AS (), "W" AS ()"#
    );
    insta::assert_snapshot!(
        analyze_error(r#"SELECT row_number() OVER "W" FROM t1 WINDOW w AS ()"#, &[]),
        @r#"semantic error: window "W" does not exist"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT row_number() OVER w FROM t1 WINDOW w AS (), W AS ()", &[]),
        @r#"semantic error: window "w" is already defined"#
    );
}

/// Every query level has a WINDOW clause of its own, so an inner `w` is
/// another window than the outer `w`, and may read the outer row.
#[test]
fn same_window_name_at_two_levels() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER w, (SELECT count(*) OVER w FROM t3 WINDOW w AS (ORDER BY t3.a) \
            LIMIT 1) FROM t1 WINDOW w AS (PARTITION BY b)",
            &[]
        ),
        @"SELECT count(*) OVER w::int, (SELECT count(*) OVER w::int FROM t3 WINDOW w AS (ORDER BY t3.a::int ASC) LIMIT 1)::int FROM t1 WINDOW w AS (PARTITION BY t1.b::int)"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT (SELECT row_number() OVER w FROM t3 \
            WINDOW w AS (PARTITION BY t1.a ORDER BY t3.a) LIMIT 1) FROM t1 WINDOW w AS (ORDER BY b)",
            &[]
        ),
        @"SELECT (SELECT row_number() OVER w::int FROM t3 WINDOW w AS (PARTITION BY t1.a::int ORDER BY t3.a::int ASC) LIMIT 1)::int FROM t1 WINDOW w AS (ORDER BY t1.b::int ASC)"
    );
}

// ------------- Frames -------------

#[test]
fn frame_bound_sanity_errors() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY b ROWS UNBOUNDED FOLLOWING) FROM t1",
            &[]
        ),
        @"semantic error: frame start cannot be UNBOUNDED FOLLOWING"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY b ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM t1",
            &[]
        ),
        @"semantic error: frame end cannot be UNBOUNDED PRECEDING"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY b ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM t1",
            &[]
        ),
        @"semantic error: frame starting from current row cannot have preceding rows"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY b ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW) FROM t1",
            &[]
        ),
        @"semantic error: frame starting from following row cannot have preceding rows"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY b ROWS 1 FOLLOWING) FROM t1",
            &[]
        ),
        @"semantic error: frame starting from following row cannot end with current row"
    );
}

#[test]
fn range_offset_order_col_count() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t1",
            &[]
        ),
        @"semantic error: RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY a, b RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t1",
            &[]
        ),
        @"semantic error: RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY b RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t1",
            &[]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.b::int ASC RANGE BETWEEN 1::int PRECEDING AND CURRENT ROW)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT b FROM t1 WINDOW w AS (RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)",
            &[]
        ),
        @"semantic error: RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column"
    );
}

#[test]
fn frame_offset_variable_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ORDER BY a ROWS b PRECEDING) FROM t1", &[]),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY a ROWS (SELECT max(t3.a) FROM t3 WHERE t3.a = t1.b) PRECEDING) FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
}

#[test]
fn frame_offset_aggregate_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ORDER BY a ROWS count(*) PRECEDING) FROM t1", &[]),
        @"semantic error: aggregate functions are not allowed in window ROWS"
    );
}

/// A frame offset may not read a column of its window's own level, wherever
/// the column sits — even inside an aggregate, when that aggregate belongs
/// to a subquery: `max(t3.a + t1.b)` aggregates over `t3`, so `t1.b` there is
/// still the current row's column. A grouped column is no exception, and
/// the window need not be at the outermost level.
#[test]
fn frame_offset_variable_inside_aggregate_error() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ROWS (SELECT max(t3.a + t1.b) FROM t3) PRECEDING) FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ROWS (SELECT max(t3.a + t1.b) FROM t3) PRECEDING) FROM t1 \
            GROUP BY b",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ROWS (SELECT (SELECT max(t4.a + t1.b) FROM t4) FROM t3) \
            PRECEDING) FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT (SELECT count(*) OVER (ROWS (SELECT max(t4.a + t3.a + t1.a) FROM t4) \
            PRECEDING) FROM t3 LIMIT 1) FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING \
            AND (SELECT max(t3.a + t1.b) FROM t3) FOLLOWING) FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY a RANGE (SELECT max(t3.a + t1.a) FROM t3) PRECEDING) \
            FROM t1",
            &[]
        ),
        @"semantic error: argument of RANGE must not contain variables"
    );
}

/// The same check for a window nested in an outer aggregate's argument: the
/// open aggregate belongs to the outer level, the offset's columns do not.
#[test]
fn frame_offset_variable_under_outer_aggregate_error() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT sum((SELECT count(*) OVER (ROWS t3.a PRECEDING) FROM t3)) FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT max((SELECT count(*) OVER (ORDER BY t3.a RANGE t3.a PRECEDING) FROM t3)) \
            FROM t1",
            &[]
        ),
        @"semantic error: argument of RANGE must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT sum((SELECT count(*) OVER (ROWS (SELECT max(t4.a + t3.a) FROM t4) PRECEDING) \
            FROM t3 LIMIT 1)) FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    // `s.*` expands to the grouped `s.a`.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT sum((SELECT count(*) OVER (ROWS (SELECT s.* FROM t4 LIMIT 1) PRECEDING) \
            FROM (SELECT a FROM t3) s GROUP BY s.a)) FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
}

/// The check holds wherever a frame is written: in a WINDOW definition, used
/// or not, and in a frame added on top of a named window.
#[test]
fn frame_offset_variable_named_window_error() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER w FROM t1 WINDOW w AS (ROWS (SELECT max(t3.a + t1.b) FROM t3) \
            PRECEDING)",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (w ROWS (SELECT max(t3.a + t1.b) FROM t3) PRECEDING) FROM t1 \
            WINDOW w AS (ORDER BY a)",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT 1 FROM t1 WINDOW w AS (ORDER BY a), w2 \
            AS (w ROWS (SELECT max(t3.a + t1.b) FROM t3) PRECEDING)",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
}

/// Errors come in binding order: a column of the window's own level is
/// reported as soon as it binds. A deliberate divergence from Postgres, which
/// reports the misplaced aggregate in the first two queries and the offset
/// type in the last one; the other three match it.
#[test]
fn frame_offset_error_order() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ROWS a + max(b) PRECEDING) FROM t1", &[]),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ROWS b + max(a) PRECEDING) FROM t1 GROUP BY b", &[]),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ROWS (SELECT max(t3.a + t1.b) FROM t3) PRECEDING) FROM t1 \
            GROUP BY a",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ROWS BETWEEN a PRECEDING AND count(*) FOLLOWING) FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ROWS BETWEEN count(*) PRECEDING AND a FOLLOWING) FROM t1",
            &[]
        ),
        @"semantic error: aggregate functions are not allowed in window ROWS"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ROWS e PRECEDING) FROM t1", &[]),
        @"semantic error: argument of ROWS must not contain variables"
    );
}

/// A window nested in another window's offset: a column is charged to the
/// offset of the level it belongs to — the outer row to the outer offset,
/// which an inner offset may read, the inner row to the inner offset.
#[test]
fn frame_offset_nested_window() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ROWS (SELECT count(*) OVER (ORDER BY t3.a RANGE t1.a PRECEDING) \
            FROM t3 LIMIT 1) PRECEDING) FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY a RANGE (SELECT count(*) OVER (ROWS t1.a PRECEDING) \
            FROM t3 LIMIT 1) PRECEDING) FROM t1",
            &[]
        ),
        @"semantic error: argument of RANGE must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY a RANGE (SELECT count(*) OVER (ROWS t3.a PRECEDING) \
            FROM t3 LIMIT 1) PRECEDING) FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
}

/// Only the window's own level is forbidden: a column or an aggregate of an
/// outer query is a constant for the inner window, and so is a subquery
/// that reads nothing of the window's level.
#[test]
fn frame_offset_outer_references() {
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (ROWS (SELECT max(t3.a) FROM t3) PRECEDING) FROM t1", &[]),
        @"SELECT count(*) OVER (ROWS (SELECT max(t3.a::int)::int FROM t3)::int PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT (SELECT count(*) OVER (ROWS t1.a PRECEDING) FROM t3 LIMIT 1) FROM t1",
            &[]
        ),
        @"SELECT (SELECT count(*) OVER (ROWS t1.a::int PRECEDING)::int FROM t3 LIMIT 1)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT (SELECT count(*) OVER (ORDER BY t3.a RANGE t1.a PRECEDING) FROM t3 LIMIT 1) \
            FROM t1",
            &[]
        ),
        @"SELECT (SELECT count(*) OVER (ORDER BY t3.a::int ASC RANGE t1.a::int PRECEDING)::int FROM t3 LIMIT 1)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT (SELECT count(*) OVER (ROWS t1.a PRECEDING) FROM t3 LIMIT 1) FROM t1 \
            GROUP BY a",
            &[]
        ),
        @"SELECT (SELECT count(*) OVER (ROWS t1.a::int PRECEDING)::int FROM t3 LIMIT 1)::int FROM t1 GROUP BY t1.a::int"
    );
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT count(*) OVER (ROWS sum(t1.a) PRECEDING) FROM t3) FROM t1", &[]),
        @"SELECT (SELECT count(*) OVER (ROWS CAST(sum(t1.a::int) AS int) PRECEDING)::int FROM t3)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT sum((SELECT count(*) OVER (ROWS t1.a PRECEDING) FROM t3)) FROM t1", &[]),
        @"SELECT sum((SELECT count(*) OVER (ROWS t1.a::int PRECEDING)::int FROM t3)::int)::decimal FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT (SELECT count(*) OVER (ROWS (SELECT max(t4.a + t1.a) FROM t4) PRECEDING) \
            FROM t3 LIMIT 1) FROM t1",
            &[]
        ),
        @"SELECT (SELECT count(*) OVER (ROWS (SELECT max((t4.a::int + t1.a::int)::int)::int FROM t4)::int PRECEDING)::int FROM t3 LIMIT 1)::int FROM t1"
    );
}

/// Columns of the window's own level reached without an aggregate in
/// between: grouped, behind `s.*`, in a subquery's clauses, or in a window
/// nested in an outer window function's argument.
#[test]
fn frame_offset_own_level_variable_errors() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ROWS a PRECEDING) FROM t1 GROUP BY a", &[]),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ROWS (SELECT t1.a FROM t3 LIMIT 1) PRECEDING) FROM t1 \
            GROUP BY a",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ROWS (SELECT s.* FROM t3 LIMIT 1) PRECEDING) \
            FROM (SELECT a FROM t1) s",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ROWS (SELECT s.* FROM t3 LIMIT 1) PRECEDING) \
            FROM (SELECT a FROM t1) s GROUP BY s.a",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ROWS (SELECT max(t3.a) FROM t3 GROUP BY t3.b \
            HAVING max(t3.a) > t1.a LIMIT 1) PRECEDING) FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ROWS (SELECT count(*) FROM t3 WHERE t3.a > t1.b) PRECEDING) \
            FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT sum((SELECT count(*) OVER (ROWS t3.a PRECEDING) FROM t3 LIMIT 1)) OVER () \
            FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
}

/// An aggregate over the window's own level is misplaced in the offset even
/// when written inside a subquery. Postgres reports the aggregate; we report
/// the column inside it, which binds first (see `frame_offset_error_order`).
#[test]
fn frame_offset_own_level_aggregate_errors() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ROWS (SELECT max(t1.b) FROM t3) PRECEDING) FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ROWS (SELECT max(t3.a) + count(t1.b) FROM t3) PRECEDING) \
            FROM t1",
            &[]
        ),
        @"semantic error: argument of ROWS must not contain variables"
    );
}

#[test]
fn count_window_func_must_have_args_or_asterisk() {
    insta::assert_snapshot!(
        analyze_error("SELECT count() OVER ()  FROM t1", &[]),
        @"semantic error: count(*) must be used to call a parameterless aggregate function"
    );
}

#[test]
fn frame_offset_subquery_and_param() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY b ROWS (SELECT 1) PRECEDING) FROM t1",
            &[]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.b::int ASC ROWS (SELECT 1::int)::int PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY b ROWS BETWEEN $1 PRECEDING AND $2 FOLLOWING) FROM t1",
            &[]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.b::int ASC ROWS BETWEEN $1::int PRECEDING AND $2::int FOLLOWING)::int FROM t1"
    );
}

/// A ROWS offset takes anything Postgres would assignment-cast to bigint:
/// `numeric` and `double` are accepted with a cast to int, other types are
/// rejected with Postgres' message (with this dialect's type names).
#[test]
fn rows_offset_types() {
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (ORDER BY b ROWS 1.5 PRECEDING) FROM t1", &[]),
        @"SELECT count(*) OVER (ORDER BY t1.b::int ASC ROWS CAST(1.5 AS int) PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (ORDER BY b ROWS 1e0 PRECEDING) FROM t1", &[]),
        @"SELECT count(*) OVER (ORDER BY t1.b::int ASC ROWS CAST(1e0 AS int) PRECEDING)::int FROM t1"
    );
    // The offset literal is read as the `int` the frame asks for right here,
    // the way Postgres constant-folds it during analysis.
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (ORDER BY b ROWS '1' PRECEDING) FROM t1", &[]),
        @"SELECT count(*) OVER (ORDER BY t1.b::int ASC ROWS 1::int PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ORDER BY b ROWS 'x' PRECEDING) FROM t1", &[]),
        @r#"semantic error: invalid input syntax for type int: "x""#
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY b ROWS CAST('x' AS text) PRECEDING) FROM t1",
            &[]
        ),
        @"argument of ROWS must be type int, not type text"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ORDER BY b ROWS true PRECEDING) FROM t1", &[]),
        @"argument of ROWS must be type int, not type bool"
    );
}

/// A RANGE offset is checked against the ORDER BY column type, mirroring
/// Postgres' `in_range` support: an int column takes an int offset, a numeric
/// column also takes int, a double column takes all three, and every other
/// column type supports no RANGE offset frame at all. Every verdict verified
/// against Postgres.
#[test]
fn range_offset_types() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ORDER BY b RANGE 1.5 PRECEDING) FROM t1", &[]),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type int and offset type numeric"
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (ORDER BY d RANGE 1.5 PRECEDING) FROM t1", &[]),
        @"SELECT count(*) OVER (ORDER BY t1.d::decimal ASC RANGE 1.5::decimal PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (ORDER BY d RANGE 1 PRECEDING) FROM t1", &[]),
        @"SELECT count(*) OVER (ORDER BY t1.d::decimal ASC RANGE CAST(1 AS decimal) PRECEDING)::int FROM t1"
    );
    // `1e0` is a double literal here, but as a literal it coerces to the
    // numeric the column calls for — converging with Postgres, whose `1e0`
    // is a numeric literal to begin with.
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (ORDER BY d RANGE 1e0 PRECEDING) FROM t1", &[]),
        @"SELECT count(*) OVER (ORDER BY t1.d::decimal ASC RANGE CAST(1e0 AS decimal) PRECEDING)::int FROM t1"
    );
    // A *typed* double offset over a numeric column is where both engines
    // draw the line: Postgres has no numeric/float8 in_range support.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY d RANGE CAST(1e0 AS double) PRECEDING) FROM t1",
            &[]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (ORDER BY c RANGE 1.5 PRECEDING) FROM t1", &[]),
        @"SELECT count(*) OVER (ORDER BY t1.c::double ASC RANGE CAST(1.5 AS double) PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (ORDER BY c RANGE 1e0 PRECEDING) FROM t1", &[]),
        @"SELECT count(*) OVER (ORDER BY t1.c::double ASC RANGE 1e0::double PRECEDING)::int FROM t1"
    );
    // The untyped literal takes the column's offset type, like in Postgres.
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (ORDER BY d RANGE '1' PRECEDING) FROM t1", &[]),
        @"SELECT count(*) OVER (ORDER BY t1.d::decimal ASC RANGE 1::decimal PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ORDER BY e RANGE 1 PRECEDING) FROM t1", &[]),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type text"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ORDER BY g RANGE 1 PRECEDING) FROM t1", &[]),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type bool"
    );
    // Postgres pairs a datetime column with `interval` offsets, which this
    // dialect does not have, so the offset type never fits.
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ORDER BY f RANGE 1 PRECEDING) FROM t2", &[]),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type datetime and offset type int"
    );
    // Deliberate divergence: Postgres reads this untyped literal as an
    // interval and accepts the query; with no interval type there is nothing
    // for it to become here.
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ORDER BY f RANGE '1' PRECEDING) FROM t2", &[]),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type datetime and offset type int"
    );
}

/// The RANGE offset check follows an inherited ORDER BY across named windows —
/// the frame is typed against a copy of the base's ORDER BY. Postgres agrees
/// on every verdict, unused definitions included.
#[test]
fn range_offset_inherited_order_by() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (w RANGE 1 PRECEDING) FROM t1 WINDOW w AS (ORDER BY e)",
            &[]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type text"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (w2 RANGE 1.5 PRECEDING) FROM t1 \
            WINDOW w AS (ORDER BY d), w2 AS (w)",
            &[]
        ),
        @"SELECT count(*) OVER (w2 RANGE 1.5::decimal PRECEDING)::int FROM t1 WINDOW w AS (ORDER BY t1.d::decimal ASC), w2 AS (w)"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT b FROM t1 WINDOW w AS (ORDER BY b), w2 AS (w RANGE 1.5 PRECEDING)",
            &[]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type int and offset type numeric"
    );
    // Two references may each add their own frame over one base.
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (w RANGE 1.5 PRECEDING), count(*) OVER (w ROWS 1 PRECEDING) \
            FROM t1 WINDOW w AS (ORDER BY d)",
            &[]
        ),
        @"SELECT count(*) OVER (w RANGE 1.5::decimal PRECEDING)::int, count(*) OVER (w ROWS 1::int PRECEDING)::int FROM t1 WINDOW w AS (ORDER BY t1.d::decimal ASC)"
    );
    // Each frame is typed on its own, as with the ORDER BY written inline:
    // the ROWS offset makes `$1` an int, which the RANGE offset then casts.
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (w ROWS $1 PRECEDING), count(*) OVER (w RANGE $1 PRECEDING) \
            FROM t1 WINDOW w AS (ORDER BY d)",
            &[DerivedType::unknown()]
        ),
        @"SELECT count(*) OVER (w ROWS $1::int PRECEDING)::int, count(*) OVER (w RANGE CAST($1 AS decimal) PRECEDING)::int FROM t1 WINDOW w AS (ORDER BY t1.d::decimal ASC)"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY d ROWS $1 PRECEDING), \
            count(*) OVER (ORDER BY d RANGE $1 PRECEDING) FROM t1",
            &[DerivedType::unknown()]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.d::decimal ASC ROWS $1::int PRECEDING)::int, count(*) OVER (ORDER BY t1.d::decimal ASC RANGE CAST($1 AS decimal) PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (w ROWS $1 PRECEDING), count(*) OVER (w RANGE $1 PRECEDING) \
            FROM t1 WINDOW w AS (ORDER BY c)",
            &[DerivedType::unknown()]
        ),
        @"SELECT count(*) OVER (w ROWS $1::int PRECEDING)::int, count(*) OVER (w RANGE CAST($1 AS double) PRECEDING)::int FROM t1 WINDOW w AS (ORDER BY t1.c::double ASC)"
    );
    // The query ORDER BY may add a frame over a named window too.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT b FROM t1 WINDOW w AS (ORDER BY b) \
            ORDER BY count(*) OVER (w RANGE 1.5 PRECEDING)",
            &[]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type int and offset type numeric"
    );
}

/// A frame is typed against the ORDER BY of the window it names, whatever the
/// other definitions and subqueries of the statement.
#[test]
fn range_offset_inherited_order_by_isolation() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (a RANGE 1 PRECEDING) FROM t1 \
            WINDOW a AS (ORDER BY e), b AS (PARTITION BY (SELECT 1) ORDER BY d)",
            &[]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type text"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT (SELECT 1), count(*) OVER (w RANGE 1.5 PRECEDING) FROM t1 \
            WINDOW w AS (ORDER BY d)",
            &[]
        ),
        @"SELECT (SELECT 1::int)::int, count(*) OVER (w RANGE 1.5::decimal PRECEDING)::int FROM t1 WINDOW w AS (ORDER BY t1.d::decimal ASC)"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (w RANGE (SELECT 1.5) PRECEDING) FROM t1 \
            WINDOW w AS (ORDER BY d)",
            &[]
        ),
        @"SELECT count(*) OVER (w RANGE (SELECT 1.5::decimal)::decimal PRECEDING)::int FROM t1 WINDOW w AS (ORDER BY t1.d::decimal ASC)"
    );
}

/// A parameter the client typed keeps its type: a `double` parameter is a
/// `double` offset, which a numeric ORDER BY column does not take.
#[test]
fn range_offset_typed_parameter_error() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY d RANGE $1 PRECEDING) FROM t1",
            &[DerivedType::new(UnrestrictedType::Double)]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY d RANGE BETWEEN 1 PRECEDING AND $1 FOLLOWING) FROM t1",
            &[DerivedType::new(UnrestrictedType::Double)]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY d RANGE BETWEEN $1 PRECEDING AND $2 FOLLOWING) \
            FROM t1",
            &[DerivedType::new(UnrestrictedType::Decimal), DerivedType::new(UnrestrictedType::Double)]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER w FROM t1 WINDOW w0 AS (ORDER BY d), w \
            AS (w0 RANGE $1 PRECEDING)",
            &[DerivedType::new(UnrestrictedType::Double)]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (w RANGE BETWEEN CURRENT ROW AND $1 FOLLOWING) FROM t1 \
            WINDOW w AS (ORDER BY d)",
            &[DerivedType::new(UnrestrictedType::Double)]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT 1 FROM t1 WINDOW w AS (ORDER BY d RANGE $1 PRECEDING)",
            &[DerivedType::new(UnrestrictedType::Double)]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT $1 + c, count(*) OVER (ORDER BY d RANGE $1 PRECEDING) FROM t1",
            &[DerivedType::unknown()]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
    // A frame over an inherited ORDER BY is typed with the select list, too.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT $1 + c, count(*) OVER (w RANGE $1 PRECEDING) FROM t1 WINDOW w AS (ORDER BY d)",
            &[DerivedType::unknown()]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
}

/// The parameter verdicts that already hold, each verified against Postgres:
/// an untyped parameter takes the offset type the column calls for, a typed
/// one is cast where Postgres casts it implicitly.
#[test]
fn range_offset_parameter_types() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY d RANGE $1 PRECEDING) FROM t1",
            &[DerivedType::unknown()]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.d::decimal ASC RANGE $1::decimal PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (ORDER BY d RANGE $1 PRECEDING) FROM t1", &[]),
        @"SELECT count(*) OVER (ORDER BY t1.d::decimal ASC RANGE $1::decimal PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY d RANGE $1 PRECEDING) FROM t1",
            &[DerivedType::new(UnrestrictedType::Decimal)]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.d::decimal ASC RANGE $1::decimal PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY d RANGE $1 PRECEDING) FROM t1",
            &[DerivedType::new(UnrestrictedType::Integer)]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.d::decimal ASC RANGE CAST($1 AS decimal) PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY c RANGE $1 PRECEDING) FROM t1",
            &[DerivedType::new(UnrestrictedType::Decimal)]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.c::double ASC RANGE CAST($1 AS double) PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY c RANGE $1 PRECEDING) FROM t1",
            &[DerivedType::new(UnrestrictedType::Double)]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.c::double ASC RANGE $1::double PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY c RANGE $1 PRECEDING) FROM t1",
            &[DerivedType::new(UnrestrictedType::Integer)]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.c::double ASC RANGE CAST($1 AS double) PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY a RANGE BETWEEN $1 PRECEDING AND $1 FOLLOWING) \
            FROM t1",
            &[DerivedType::new(UnrestrictedType::Integer)]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.a::int ASC RANGE BETWEEN $1::int PRECEDING AND $1::int FOLLOWING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY a RANGE $1 PRECEDING) FROM t1",
            &[DerivedType::new(UnrestrictedType::Double)]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type int and offset type double"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY a RANGE $1 PRECEDING) FROM t1",
            &[DerivedType::new(UnrestrictedType::Decimal)]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type int and offset type numeric"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY d RANGE $1 PRECEDING) FROM t1",
            &[DerivedType::new(UnrestrictedType::String)]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type text"
    );
    // ROWS takes what Postgres would assignment-cast to bigint.
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ROWS $1 PRECEDING) FROM t1",
            &[DerivedType::new(UnrestrictedType::Double)]
        ),
        @"SELECT count(*) OVER (ROWS CAST($1 AS int) PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ROWS $1 PRECEDING) FROM t1",
            &[DerivedType::new(UnrestrictedType::String)]
        ),
        @"argument of ROWS must be type int, not type text"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT b, sum(b) OVER (ORDER BY b ROWS BETWEEN (SELECT 1) PRECEDING AND $1 \
            FOLLOWING) FROM t1",
            &[DerivedType::unknown()]
        ),
        @"SELECT t1.b::int, sum(t1.b::int) OVER (ORDER BY t1.b::int ASC ROWS BETWEEN (SELECT 1::int)::int PRECEDING AND $1::int FOLLOWING)::decimal FROM t1"
    );
}

/// The offset's own type decides, not the one the column's type makes of it:
/// a `double` offset stays `double` inside an expression too, though an
/// operator asked for `decimal` would cast it.
#[test]
fn range_offset_double_in_expression_error() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY d RANGE $1 + 0 PRECEDING) FROM t1",
            &[DerivedType::new(UnrestrictedType::Double)]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY d RANGE COALESCE($1, 1.5) PRECEDING) FROM t1",
            &[DerivedType::new(UnrestrictedType::Double)]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY d RANGE CAST(1 AS double) + 0 PRECEDING) FROM t1",
            &[]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY d RANGE CASE WHEN true THEN CAST(1 AS double) \
            ELSE 1.5 END PRECEDING) FROM t1",
            &[]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY d RANGE (SELECT max(t2.a) FROM t2) + 0 PRECEDING) \
            FROM t1",
            &[]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
    // An outer column is a constant for the window, but a typed one.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT (SELECT count(*) OVER (ORDER BY t1.d RANGE t2.a + 0 PRECEDING) FROM t1) \
            FROM t2",
            &[]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER w FROM t1 WINDOW w0 AS (ORDER BY d), w \
            AS (w0 RANGE $1 + 0 PRECEDING)",
            &[DerivedType::new(UnrestrictedType::Double)]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
}

/// An offset expression is typed on its own, as for any other context, and
/// the result is then cast to the column type: an untyped parameter inside it
/// takes the type the expression gives it, not the column's. Every verdict
/// verified against Postgres.
#[test]
fn range_offset_expression_types() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY c RANGE $1 + 1 PRECEDING) FROM t1",
            &[DerivedType::unknown()]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.c::double ASC RANGE CAST($1::int + 1::int AS double) PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY d RANGE (SELECT 1) PRECEDING) FROM t1",
            &[]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.d::decimal ASC RANGE CAST((SELECT 1::int) AS decimal) PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY d RANGE $1 + 0 PRECEDING) FROM t1",
            &[DerivedType::unknown()]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.d::decimal ASC RANGE CAST($1::int + 0::int AS decimal) PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY d RANGE $1 + 1.5 PRECEDING) FROM t1",
            &[DerivedType::unknown()]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.d::decimal ASC RANGE ($1::decimal + 1.5::decimal)::decimal PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY d RANGE $1 + 0 PRECEDING) FROM t1",
            &[DerivedType::new(UnrestrictedType::Integer)]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.d::decimal ASC RANGE CAST($1::int + 0::int AS decimal) PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT (SELECT count(*) OVER (ORDER BY t1.d RANGE t2.c + 0 PRECEDING) FROM t1) \
            FROM t2",
            &[]
        ),
        @"SELECT (SELECT count(*) OVER (ORDER BY t1.d::decimal ASC RANGE CAST(t2.c::int + 0::int AS decimal) PRECEDING)::int FROM t1)::int FROM t2"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY c RANGE $1 + 0 PRECEDING) FROM t1",
            &[DerivedType::new(UnrestrictedType::Double)]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.c::double ASC RANGE ($1::double + CAST(0 AS double))::double PRECEDING)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY a RANGE $1 + 1.5 PRECEDING) FROM t1",
            &[DerivedType::unknown()]
        ),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type int and offset type numeric"
    );
}

/// Known divergences with Postgres.
///
/// `1e0` is a double literal here and a numeric one in Postgres. Alone it
/// coerces to the column's numeric as any literal does, but inside an
/// expression it is a double operand, so the offset is double: rejected,
/// while Postgres accepts.
///
/// A parameter is typed by its first use here, while Postgres types frame
/// offsets after the select list. Here the offset makes `$1` numeric, and
/// `$1 + c` then casts `c`: accepted. Postgres first types `$1` as double
/// from `$1 + c`, and rejects the offset.
///
/// The offsets of one frame are typed in one go, so a parameter that is bare
/// in one offset takes the column type there and the expression's type in the
/// other: rejected. Postgres types the offsets in order and fixes `$1` at the
/// first one, here `numeric`.
#[test]
fn range_offset_type_divergences() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) OVER (ORDER BY d RANGE BETWEEN $1 PRECEDING AND $1 + 1 FOLLOWING) \
            FROM t1",
            &[DerivedType::unknown()]
        ),
        @"inconsistent types int and numeric deduced for parameter $1, consider using transitive type casts through a common type, e.g. $1::int::numeric and $1::int"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ORDER BY d RANGE 1e0 + 0 PRECEDING) FROM t1", &[]),
        @"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (ORDER BY d RANGE $1 PRECEDING), $1 + c FROM t1",
            &[DerivedType::unknown()]
        ),
        @"SELECT count(*) OVER (ORDER BY t1.d::decimal ASC RANGE $1::decimal PRECEDING)::int, ($1::decimal + CAST(t1.c AS decimal))::decimal FROM t1"
    );
}

// ------------- Grouping -------------

#[test]
fn window_agg_arg_ungrouped_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(b) OVER () FROM t1 GROUP BY a", &[]),
        @r#"semantic error: column "t1.b" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn window_partition_ungrouped_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (PARTITION BY b) FROM t1 GROUP BY a", &[]),
        @r#"semantic error: column "t1.b" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn unused_window_grouping_checked() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(a) FROM t1 GROUP BY a WINDOW w AS (PARTITION BY b)", &[]),
        @r#"semantic error: column "t1.b" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn window_agg_does_not_group_level() {
    insta::assert_snapshot!(
        analyzed("SELECT b, sum(b) OVER () FROM t1", &[]),
        @"SELECT t1.b::int, sum(t1.b::int) OVER ()::decimal FROM t1"
    );
}

#[test]
fn expression_key_in_window_def() {
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (ORDER BY a + b) FROM t1 GROUP BY a + b", &[]),
        @"SELECT count(*) OVER (ORDER BY (t1.a::int + t1.b::int)::int ASC)::int FROM t1 GROUP BY (t1.a::int + t1.b::int)::int"
    );
    insta::assert_snapshot!(
        analyzed("SELECT row_number() OVER (ORDER BY b) FROM t1 GROUP BY b", &[]),
        @"SELECT row_number() OVER (ORDER BY t1.b::int ASC)::int FROM t1 GROUP BY t1.b::int"
    );
}

/// A WINDOW definition sees a grouping key an alias or an ordinal names, as
/// the select list does: Postgres binds window definitions after the select
/// list and GROUP BY. The renderings are composed from the spellings the
/// analyzer accepts today (`GROUP BY a + b`, an inline window).
#[test]
#[ignore = "A WINDOW definition is bound before alias and ordinal keys exist"]
fn named_window_sees_alias_and_ordinal_keys() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT a + b AS k, row_number() OVER w FROM t1 GROUP BY k \
            WINDOW w AS (ORDER BY a + b)",
            &[]
        ),
        @"SELECT (t1.a::int + t1.b::int)::int AS k, row_number() OVER w::int FROM t1 GROUP BY 1 WINDOW w AS (ORDER BY (t1.a::int + t1.b::int)::int ASC)"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT a + b, row_number() OVER w FROM t1 GROUP BY 1 WINDOW w AS (ORDER BY a + b)",
            &[]
        ),
        @"SELECT (t1.a::int + t1.b::int)::int, row_number() OVER w::int FROM t1 GROUP BY 1 WINDOW w AS (ORDER BY (t1.a::int + t1.b::int)::int ASC)"
    );
}

/// The select-list side of the same gap: a window written before the
/// projection whose alias or ordinal makes the key.
#[test]
#[ignore = "A projection is bound before a later alias or ordinal key exists"]
fn window_before_aliased_key() {
    insta::assert_snapshot!(
        analyzed("SELECT a + b AS k, row_number() OVER (ORDER BY a + b) FROM t1 GROUP BY k", &[]),
        @"SELECT row_number() OVER (ORDER BY (t1.a::int + t1.b::int)::int ASC)::int, (t1.a::int + t1.b::int)::int AS k FROM t1 GROUP BY 2"
    );
    insta::assert_snapshot!(
        analyzed("SELECT row_number() OVER (ORDER BY a + b), a + b AS k FROM t1 GROUP BY k", &[]),
        @"SELECT row_number() OVER (ORDER BY (t1.a::int + t1.b::int)::int ASC)::int, (t1.a::int + t1.b::int)::int AS k FROM t1 GROUP BY 2"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT a + b AS k, row_number() OVER (ORDER BY a + b), a + b FROM t1 GROUP BY 3",
            &[]
        ),
        @"SELECT (t1.a::int + t1.b::int)::int AS k, row_number() OVER (ORDER BY (t1.a::int + t1.b::int)::int ASC)::int, (t1.a::int + t1.b::int)::int FROM t1 GROUP BY 3"
    );
}

/// What already holds next to alias and ordinal keys: an expression key, a
/// window after the aliased projection, a window's ORDER BY that cannot name
/// an output column, and an ungrouped column, which stays an error.
#[test]
fn window_next_to_alias_and_ordinal_keys() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT a + b, row_number() OVER w FROM t1 GROUP BY a + b \
            WINDOW w AS (ORDER BY a + b)",
            &[]
        ),
        @"SELECT (t1.a::int + t1.b::int)::int, row_number() OVER w::int FROM t1 GROUP BY (t1.a::int + t1.b::int)::int WINDOW w AS (ORDER BY (t1.a::int + t1.b::int)::int ASC)"
    );
    insta::assert_snapshot!(
        analyzed("SELECT a + b AS k, row_number() OVER (ORDER BY a + b) FROM t1 GROUP BY k", &[]),
        @"SELECT (t1.a::int + t1.b::int)::int AS k, row_number() OVER (ORDER BY (t1.a::int + t1.b::int)::int ASC)::int FROM t1 GROUP BY 1"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT a + b AS k, row_number() OVER w FROM t1 GROUP BY k WINDOW w AS (ORDER BY k)",
            &[]
        ),
        @"semantic error: cannot resolve column reference 'k'"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT a + b AS k, row_number() OVER w FROM t1 GROUP BY k WINDOW w AS (ORDER BY a)",
            &[]
        ),
        @r#"semantic error: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT a + b AS k FROM t1 GROUP BY k WINDOW w AS (ORDER BY a) \
            ORDER BY row_number() OVER w",
            &[]
        ),
        @r#"semantic error: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// A window over a grouped query ranges over the groups: its arguments,
/// PARTITION BY, ORDER BY and frame may read grouping keys and aggregates.
#[test]
fn window_over_grouped_query() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT e, count(*), sum(count(*)) OVER (ORDER BY e) FROM t1 WHERE a > 0 GROUP BY e \
            HAVING count(*) > 1 ORDER BY sum(count(*)) OVER (ORDER BY e) DESC LIMIT 5",
            &[]
        ),
        @"SELECT t1.e::string, count(*)::int, sum(count(*)::int) OVER (ORDER BY t1.e::string ASC)::decimal FROM t1 WHERE (t1.a::int > 0::int)::bool GROUP BY t1.e::string HAVING (count(*)::int > 1::int)::bool ORDER BY sum(count(*)::int) OVER (ORDER BY t1.e::string ASC)::decimal DESC LIMIT 5"
    );
    insta::assert_snapshot!(
        analyzed("SELECT sum(count(*)) OVER () FROM t1", &[]),
        @"SELECT sum(count(*)::int) OVER ()::decimal FROM t1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (ORDER BY count(*)) FROM t1 GROUP BY e", &[]),
        @"SELECT count(*) OVER (ORDER BY count(*)::int ASC)::int FROM t1 GROUP BY t1.e::string"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT max(sum(a)) OVER (PARTITION BY e ORDER BY max(b)) FROM t1 GROUP BY e",
            &[]
        ),
        @"SELECT max(sum(t1.a::int)::decimal) OVER (PARTITION BY t1.e::string ORDER BY max(t1.b::int)::int ASC)::decimal FROM t1 GROUP BY t1.e::string"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER w FROM t1 GROUP BY e WINDOW w \
            AS (PARTITION BY e ORDER BY count(*) DESC)",
            &[]
        ),
        @"SELECT count(*) OVER w::int FROM t1 GROUP BY t1.e::string WINDOW w AS (PARTITION BY t1.e::string ORDER BY count(*)::int DESC)"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT sum(b) OVER (PARTITION BY e ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING \
            AND UNBOUNDED FOLLOWING) FROM t1 GROUP BY e, b",
            &[]
        ),
        @"SELECT sum(t1.b::int) OVER (PARTITION BY t1.e::string ORDER BY t1.b::int ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)::decimal FROM t1 GROUP BY t1.e::string, t1.b::int"
    );
}

/// The grouping check of window definitions knows functional dependency:
/// grouping by the whole primary key makes every column of the table usable.
#[test]
fn window_grouping_functional_dependency() {
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (PARTITION BY a) FROM t1 GROUP BY b", &[]),
        @"SELECT count(*) OVER (PARTITION BY t1.a::int)::int FROM t1 GROUP BY t1.b::int"
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (PARTITION BY v) FROM t6 GROUP BY k1, k2", &[]),
        @"SELECT count(*) OVER (PARTITION BY t6.v::int)::int FROM t6 GROUP BY t6.k1::int, t6.k2::int"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (PARTITION BY v) FROM t6 GROUP BY k1", &[]),
        @r#"semantic error: column "t6.v" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// An ungrouped column stays an error wherever the window is: under a HAVING
/// that groups the whole table, next to a plain aggregate, in a window's
/// ORDER BY or in its arguments.
#[test]
fn window_ungrouped_column_errors() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) OVER (ORDER BY a) FROM t1 HAVING count(*) > 0", &[]),
        @r#"semantic error: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT sum(a) OVER (), count(*) FROM t1", &[]),
        @r#"semantic error: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT max(sum(a)) OVER (PARTITION BY e ORDER BY f) FROM t1 GROUP BY e",
            &[]
        ),
        @r#"semantic error: column "t1.f" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error("SELECT e, sum(a) OVER (PARTITION BY e) FROM t1 GROUP BY e", &[]),
        @r#"semantic error: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

// ------------- FILTER -------------

#[test]
fn filter_on_pure_window_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT row_number() FILTER (WHERE b > 0) OVER () FROM t1", &[]),
        @"semantic error: FILTER is not implemented for non-aggregate window functions"
    );
}

#[test]
fn aggregate_in_filter_error() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) FILTER (WHERE sum(a) > 0) OVER () FROM t1 GROUP BY b",
            &[]
        ),
        @"semantic error: aggregate functions are not allowed in FILTER"
    );
}

#[test]
fn window_in_filter_error() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) FILTER (WHERE row_number() OVER () = 1) OVER () FROM t1",
            &[]
        ),
        @"semantic error: window functions are not allowed in FILTER"
    );
}

#[test]
fn filter_grouping_checked() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT count(*) FILTER (WHERE b > 0) OVER () FROM t1 GROUP BY a",
            &[]
        ),
        @r#"semantic error: column "t1.b" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) FILTER (WHERE a > 0) OVER () FROM t1 GROUP BY a",
            &[]
        ),
        @"SELECT count(*) FILTER (WHERE (t1.a::int > 0::int)::bool) OVER ()::int FROM t1 GROUP BY t1.a::int"
    );
}

#[test]
fn filter_non_boolean_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT count(*) FILTER (WHERE b) OVER () FROM t1", &[]),
        @"argument of FILTER must be type boolean, not type int"
    );
}

// ------------- Types -------------

#[test]
fn window_function_types() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT row_number() OVER (), last_value(e) OVER (), sum(b) OVER (), count(*) OVER () FROM t1",
            &[]
        ),
        @"SELECT row_number() OVER ()::int, last_value(t1.e::string) OVER ()::string, sum(t1.b::int) OVER ()::decimal, count(*) OVER ()::int FROM t1"
    );
}

/// Postgres has `rank`, Picodata's window registry does not — a registry divergence, not an analyzer one.
#[test]
fn unknown_window_function_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT rank() OVER () FROM t1", &[]),
        @"window function rank does not exist"
    );
}

/// Literals in a window's ORDER BY and PARTITION BY are constants,
/// never select-list ordinals, unlike the query-level ORDER BY.
#[test]
fn constant_in_window_order_by() {
    insta::assert_snapshot!(
        analyzed("SELECT b FROM t1 WINDOW w AS (ORDER BY 2)", &[]),
        @"SELECT t1.b::int FROM t1 WINDOW w AS (ORDER BY 2::int ASC)"
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (PARTITION BY 2) FROM t1", &[]),
        @"SELECT count(*) OVER (PARTITION BY 2::int)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT count(*) OVER (ORDER BY 'x') FROM t1", &[]),
        @"SELECT count(*) OVER (ORDER BY 'x'::string ASC)::int FROM t1"
    );
}

#[test]
fn param_type_inference_through_frame() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER w FROM t1 WINDOW w AS (ORDER BY b ROWS BETWEEN $1 PRECEDING AND $1 FOLLOWING)",
            &[DerivedType::unknown()]
        ),
        @"SELECT count(*) OVER w::int FROM t1 WINDOW w AS (ORDER BY t1.b::int ASC ROWS BETWEEN $1::int PRECEDING AND $1::int FOLLOWING)"
    );
}

// ------------- Output names -------------

/// A window call is named after its function, like an aggregate call, so the
/// column can be read back by that name from a derived table, a CTE, or a
/// set operation whose first branch carries it.
#[test]
fn window_output_name() {
    insta::assert_snapshot!(
        analyzed("SELECT x.row_number FROM (SELECT row_number() OVER () FROM t1) AS x", &[]),
        @"SELECT x.row_number::int FROM (SELECT row_number() OVER ()::int FROM t1) AS x"
    );
    insta::assert_snapshot!(
        analyzed("SELECT x.sum FROM (SELECT sum(a) OVER () FROM t1) AS x", &[]),
        @"SELECT x.sum::decimal FROM (SELECT sum(t1.a::int) OVER ()::decimal FROM t1) AS x"
    );
    insta::assert_snapshot!(
        analyzed("SELECT x.count FROM (SELECT count(*) OVER (PARTITION BY e) FROM t1) AS x", &[]),
        @"SELECT x.count::int FROM (SELECT count(*) OVER (PARTITION BY t1.e::string)::int FROM t1) AS x"
    );
    insta::assert_snapshot!(
        analyzed("WITH c AS (SELECT row_number() OVER () FROM t1) SELECT row_number FROM c", &[]),
        @"WITH c AS (SELECT row_number() OVER ()::int FROM t1) SELECT c.row_number::int FROM c"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT row_number FROM (SELECT row_number() OVER () FROM t1 UNION ALL SELECT a \
            FROM t1) AS x",
            &[]
        ),
        @"SELECT x.row_number::int FROM ((SELECT row_number() OVER ()::int FROM t1) UNION ALL (SELECT t1.a::int FROM t1)) AS x"
    );
    insta::assert_snapshot!(
        analyzed("SELECT * FROM (SELECT row_number() OVER () FROM t1) AS x", &[]),
        @"SELECT x.row_number::int FROM (SELECT row_number() OVER ()::int FROM t1) AS x"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT * FROM (SELECT sum(a) OVER (), count(*) OVER (), last_value(e) OVER () \
            FROM t1) AS x",
            &[]
        ),
        @"SELECT x.sum::decimal, x.count::int, x.last_value::string FROM (SELECT sum(t1.a::int) OVER ()::decimal, count(*) OVER ()::int, last_value(t1.e::string) OVER ()::string FROM t1) AS x"
    );
}

/// The name resolves in the query's own ORDER BY and GROUP BY too — the
/// GROUP BY then finds a window function and rejects it, as Postgres does —
/// and two calls of one function make it ambiguous.
#[test]
fn window_output_name_in_order_and_group_by() {
    insta::assert_snapshot!(
        analyzed("SELECT row_number() OVER () FROM t1 ORDER BY row_number", &[]),
        @"SELECT row_number() OVER ()::int FROM t1 ORDER BY 1 ASC"
    );
    insta::assert_snapshot!(
        analyzed("SELECT row_number() OVER (), b FROM t1 ORDER BY row_number DESC, b", &[]),
        @"SELECT row_number() OVER ()::int, t1.b::int FROM t1 ORDER BY 1 DESC, 2 ASC"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT row_number() OVER () FROM t1 GROUP BY row_number", &[]),
        @"semantic error: window functions are not allowed in GROUP BY"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT x.row_number FROM (SELECT row_number() OVER (), row_number() \
            OVER (ORDER BY a) FROM t1) AS x",
            &[]
        ),
        @"semantic error: column reference 'x.row_number' is ambigious"
    );
}

/// An alias replaces the function name.
#[test]
fn window_output_alias() {
    insta::assert_snapshot!(
        analyzed("SELECT x.r FROM (SELECT row_number() OVER () AS r FROM t1) AS x", &[]),
        @"SELECT x.r::int FROM (SELECT row_number() OVER ()::int AS r FROM t1) AS x"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT x.row_number FROM (SELECT row_number() OVER () AS r FROM t1) AS x",
            &[]
        ),
        @"semantic error: cannot resolve column reference 'x.row_number'"
    );
    insta::assert_snapshot!(
        analyzed("SELECT x.sum FROM (SELECT sum(a) FROM t1) AS x", &[]),
        @"SELECT x.sum::decimal FROM (SELECT sum(t1.a::int)::decimal FROM t1) AS x"
    );
}

#[test]
#[ignore = "Coercion to int of floating point number in `ROWS <float_num>` frame diverges with PG"]
fn coerce_to_int_float_rows_frame() {
    insta::assert_snapshot!(
        analyzed("SELECT DISTINCT count(*) OVER (w ROWS 1.5 PRECEDING) FROM t1 WINDOW w AS (ORDER BY a)", &[]),
        // Result may not make sense, anyway keep it to avoid it to be lost
        @"SELECT DISTINCT count(*) OVER (w ROWS 2 PRECEDING) FROM t1 WINDOW w AS (ORDER BY a)"
    );

    insta::assert_snapshot!(
        analyzed("SELECT DISTINCT count(*) OVER (w ROWS 2.7e0 PRECEDING) FROM t1 WINDOW w AS (ORDER BY a)", &[]),
        // Result may not make sense, anyway keep it to avoid it to be lost
        @"SELECT DISTINCT count(*) OVER (w ROWS 3 PRECEDING) FROM t1 WINDOW w AS (ORDER BY a)"
    );
}

// ------------- Equality -------------

/// Two window calls are equal when their functions, arguments, FILTERs and
/// windows are: `SELECT DISTINCT` needs that to find an ORDER BY window call
/// in the select list. Arguments compare by meaning (`t1.a` is `a`).
/// Postgres accepts every query here.
#[test]
#[ignore = "Window function calls never compare equal"]
fn distinct_order_by_same_window_call() {
    insta::assert_snapshot!(
        analyzed("SELECT DISTINCT row_number() OVER () FROM t1 ORDER BY row_number() OVER ()", &[]),
        @"SELECT DISTINCT row_number() OVER ()::int FROM t1 ORDER BY row_number() OVER ()::int ASC"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT DISTINCT sum(a) OVER (PARTITION BY b) FROM t1 \
            ORDER BY sum(a) OVER (PARTITION BY b)",
            &[]
        ),
        @"SELECT DISTINCT sum(t1.a::int) OVER (PARTITION BY t1.b::int)::decimal FROM t1 ORDER BY sum(t1.a::int) OVER (PARTITION BY t1.b::int)::decimal ASC"
    );
    insta::assert_snapshot!(
        analyzed("SELECT DISTINCT sum(a) OVER () FROM t1 ORDER BY sum(t1.a) OVER ()", &[]),
        @"SELECT DISTINCT sum(t1.a::int) OVER ()::decimal FROM t1 ORDER BY sum(t1.a::int) OVER ()::decimal ASC"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT DISTINCT row_number() OVER (ORDER BY a) FROM t1 \
            ORDER BY row_number() OVER (ORDER BY (a))",
            &[]
        ),
        @"SELECT DISTINCT row_number() OVER (ORDER BY t1.a::int ASC)::int FROM t1 ORDER BY row_number() OVER (ORDER BY t1.a::int ASC)::int ASC"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT DISTINCT row_number() OVER w FROM t1 WINDOW w AS (ORDER BY a) \
            ORDER BY row_number() OVER w",
            &[]
        ),
        @"SELECT DISTINCT row_number() OVER w::int FROM t1 WINDOW w AS (ORDER BY t1.a::int ASC) ORDER BY row_number() OVER w::int ASC"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT DISTINCT row_number() OVER (w) FROM t1 WINDOW w AS (ORDER BY a) \
            ORDER BY row_number() OVER (w)",
            &[]
        ),
        @"SELECT DISTINCT row_number() OVER (w)::int FROM t1 WINDOW w AS (ORDER BY t1.a::int ASC) ORDER BY row_number() OVER (w)::int ASC"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT DISTINCT count(*) OVER (w ROWS 1 PRECEDING) FROM t1 WINDOW w AS (ORDER BY a) \
            ORDER BY count(*) OVER (w ROWS 1 PRECEDING)",
            &[]
        ),
        @"SELECT DISTINCT count(*) OVER (w ROWS 1::int PRECEDING)::int FROM t1 WINDOW w AS (ORDER BY t1.a::int ASC) ORDER BY count(*) OVER (w ROWS 1::int PRECEDING)::int ASC"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT DISTINCT count(*) OVER (ORDER BY a ROWS 1 PRECEDING) FROM t1 \
            ORDER BY count(*) OVER (ORDER BY a ROWS 1 PRECEDING)",
            &[]
        ),
        @"SELECT DISTINCT count(*) OVER (ORDER BY t1.a::int ASC ROWS 1::int PRECEDING)::int FROM t1 ORDER BY count(*) OVER (ORDER BY t1.a::int ASC ROWS 1::int PRECEDING)::int ASC"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT DISTINCT count(*) FILTER (WHERE g) OVER () FROM t1 \
            ORDER BY count(*) FILTER (WHERE g) OVER ()",
            &[]
        ),
        @"SELECT DISTINCT count(*) FILTER (WHERE t1.g::bool) OVER ()::int FROM t1 ORDER BY count(*) FILTER (WHERE t1.g::bool) OVER ()::int ASC"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT DISTINCT row_number() OVER (PARTITION BY e ORDER BY b DESC) FROM t1 \
            ORDER BY row_number() OVER (PARTITION BY e ORDER BY b DESC)",
            &[]
        ),
        @"SELECT DISTINCT row_number() OVER (PARTITION BY t1.e::string ORDER BY t1.b::int DESC)::int FROM t1 ORDER BY row_number() OVER (PARTITION BY t1.e::string ORDER BY t1.b::int DESC)::int ASC"
    );
}

/// Calls that differ in any part stay distinct. An explicit NULLS order and a
/// frame spelled with BETWEEN differ from the defaults they spell, as in
/// Postgres, and `OVER w` is not `OVER (w)`.
#[test]
fn distinct_order_by_different_window_call() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT DISTINCT row_number() OVER (ORDER BY a) FROM t1 \
            ORDER BY row_number() OVER (ORDER BY b)",
            &[]
        ),
        @"semantic error: for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT DISTINCT sum(a) OVER (PARTITION BY b) FROM t1 \
            ORDER BY sum(b) OVER (PARTITION BY b)",
            &[]
        ),
        @"semantic error: for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT DISTINCT count(*) OVER (ORDER BY a ROWS 1 PRECEDING) FROM t1 \
            ORDER BY count(*) OVER (ORDER BY a ROWS 2 PRECEDING)",
            &[]
        ),
        @"semantic error: for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT DISTINCT count(*) FILTER (WHERE g) OVER () FROM t1 \
            ORDER BY count(*) FILTER (WHERE NOT g) OVER ()",
            &[]
        ),
        @"semantic error: for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT DISTINCT row_number() OVER w1 FROM t1 \
            WINDOW w1 AS (ORDER BY a), w2 AS (ORDER BY a) ORDER BY row_number() OVER w2",
            &[]
        ),
        @"semantic error: for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT DISTINCT row_number() OVER () FROM t1 \
            ORDER BY row_number() OVER (RANGE UNBOUNDED PRECEDING)",
            &[]
        ),
        @"semantic error: for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT DISTINCT row_number() OVER (ORDER BY a ROWS 1 PRECEDING) FROM t1 \
            ORDER BY row_number() OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
            &[]
        ),
        @"semantic error: for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT DISTINCT row_number() OVER (ORDER BY a) FROM t1 \
            ORDER BY row_number() OVER (ORDER BY a NULLS LAST)",
            &[]
        ),
        @"semantic error: for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT DISTINCT row_number() OVER (ORDER BY a DESC) FROM t1 \
            ORDER BY row_number() OVER (ORDER BY a DESC NULLS FIRST)",
            &[]
        ),
        @"semantic error: for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT DISTINCT row_number() OVER w FROM t1 WINDOW w AS (ORDER BY a) \
            ORDER BY row_number() OVER (w)",
            &[]
        ),
        @"semantic error: for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT row_number() OVER () AS r, row_number() OVER (ORDER BY a) AS r FROM t1 \
            ORDER BY r",
            &[]
        ),
        @r#"semantic error: ORDER BY "r" is ambiguous"#
    );
    // Divergence: Postgres reuses a named window for an inline spec written
    // the same way, so it accepts this.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT DISTINCT row_number() OVER w FROM t1 WINDOW w AS (ORDER BY a) \
            ORDER BY row_number() OVER (ORDER BY a)",
            &[]
        ),
        @"semantic error: for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
}

// ------------- Across query levels -------------

/// Windows inside a CTE, a derived table and set operation branches, each
/// branch with a WINDOW clause of its own.
#[test]
fn window_in_cte_derived_table_and_set_operation() {
    insta::assert_snapshot!(
        analyzed(
            "WITH c AS (SELECT a, row_number() OVER (ORDER BY a) r FROM t1) SELECT r FROM c",
            &[]
        ),
        @"WITH c AS (SELECT t1.a::int, row_number() OVER (ORDER BY t1.a::int ASC)::int AS r FROM t1) SELECT c.r::int FROM c"
    );
    insta::assert_snapshot!(
        analyzed(
            "WITH c AS (SELECT e, row_number() OVER (PARTITION BY e ORDER BY a) AS r FROM t1) \
            SELECT e, max(r) FROM c GROUP BY e HAVING max(r) > 1",
            &[]
        ),
        @"WITH c AS (SELECT t1.e::string, row_number() OVER (PARTITION BY t1.e::string ORDER BY t1.a::int ASC)::int AS r FROM t1) SELECT c.e::string, max(c.r::int)::int FROM c GROUP BY c.e::string HAVING (max(c.r::int)::int > 1::int)::bool"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT max(r) FROM (SELECT row_number() OVER (PARTITION BY e ORDER BY b) AS r \
            FROM t1) AS x",
            &[]
        ),
        @"SELECT max(x.r::int)::int FROM (SELECT row_number() OVER (PARTITION BY t1.e::string ORDER BY t1.b::int ASC)::int AS r FROM t1) AS x"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT x.a, x.r FROM (SELECT a, row_number() OVER (PARTITION BY a) AS r FROM t3) \
            AS x JOIN t4 ON x.a = t4.a WHERE x.r = 1",
            &[]
        ),
        @"SELECT x.a::int, x.r::int FROM (SELECT t3.a::int, row_number() OVER (PARTITION BY t3.a::int)::int AS r FROM t3) AS x INNER JOIN t4 ON (x.a::int = t4.a::int)::bool WHERE (x.r::int = 1::int)::bool"
    );
    insta::assert_snapshot!(
        analyzed("SELECT row_number() OVER () FROM t1 UNION SELECT a FROM t1", &[]),
        @"(SELECT row_number() OVER ()::int FROM t1) UNION (SELECT t1.a::int FROM t1)"
    );
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t1 UNION SELECT row_number() OVER w FROM t1 WINDOW w AS ()", &[]),
        @"(SELECT t1.a::int FROM t1) UNION (SELECT row_number() OVER w::int FROM t1 WINDOW w AS ())"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT a FROM t1 EXCEPT SELECT row_number() OVER w FROM t1 \
            WINDOW w AS (ORDER BY a DESC NULLS FIRST)",
            &[]
        ),
        @"(SELECT t1.a::int FROM t1) EXCEPT (SELECT row_number() OVER w::int FROM t1 WINDOW w AS (ORDER BY t1.a::int DESC NULLS FIRST))"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT row_number() OVER w FROM t1 WINDOW w AS (ORDER BY a) UNION ALL \
            SELECT row_number() OVER w FROM t3 WINDOW w AS (ORDER BY b)",
            &[]
        ),
        @"(SELECT row_number() OVER w::int FROM t1 WINDOW w AS (ORDER BY t1.a::int ASC)) UNION ALL (SELECT row_number() OVER w::int FROM t3 WINDOW w AS (ORDER BY t3.b::string ASC))"
    );
}

/// An inner window may read the outer query's columns and aggregates
/// anywhere in its spec and arguments: they are constants there.
#[test]
fn outer_reference_in_inner_window() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT (SELECT row_number() OVER (PARTITION BY t1.a) FROM t3 LIMIT 1) FROM t1",
            &[]
        ),
        @"SELECT (SELECT row_number() OVER (PARTITION BY t1.a::int)::int FROM t3 LIMIT 1)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT row_number() OVER (PARTITION BY a ORDER BY b) FROM t1 \
            WHERE b IN (SELECT count(*) OVER (PARTITION BY t1.a) FROM t3)",
            &[]
        ),
        @"SELECT row_number() OVER (PARTITION BY t1.a::int ORDER BY t1.b::int ASC)::int FROM t1 WHERE (t1.b::int IN (SELECT count(*) OVER (PARTITION BY t1.a::int)::int FROM t3)::int)::bool"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT a, (SELECT sum(t3.a) OVER (PARTITION BY t1.a) FROM t3 LIMIT 1) FROM t1 \
            GROUP BY a",
            &[]
        ),
        @"SELECT t1.a::int, (SELECT sum(t3.a::int) OVER (PARTITION BY t1.a::int)::decimal FROM t3 LIMIT 1)::decimal FROM t1 GROUP BY t1.a::int"
    );
    insta::assert_snapshot!(
        analyzed("SELECT (SELECT sum(sum(t1.a)) OVER () FROM t3 LIMIT 1) FROM t1", &[]),
        @"SELECT (SELECT sum(sum(t1.a::int)::decimal) OVER ()::decimal FROM t3 LIMIT 1)::decimal FROM t1"
    );
}

/// Divergence in the message only: Postgres says `subquery uses ungrouped
/// column "t1.c" from outer query`.
#[test]
fn outer_ungrouped_column_in_inner_window_error() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT a, (SELECT sum(t3.a) OVER (PARTITION BY t1.c) FROM t3 LIMIT 1) FROM t1 \
            GROUP BY a",
            &[]
        ),
        @r#"semantic error: column "t1.c" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT row_number() OVER (ORDER BY (SELECT count(*) FROM t3 WHERE t3.a = t1.c)) \
            FROM t1 GROUP BY a",
            &[]
        ),
        @r#"semantic error: column "t1.c" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT (SELECT count(*) OVER (ROWS t1.c PRECEDING) FROM t3 LIMIT 1) FROM t1 \
            GROUP BY a",
            &[]
        ),
        @r#"semantic error: column "t1.c" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

/// Subqueries in a window's arguments, PARTITION BY and ORDER BY, correlated
/// or holding windows of their own.
#[test]
fn subqueries_in_window_spec() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT sum((SELECT max(t3.a) FROM t3 WHERE t3.a = t1.a)) \
            OVER (PARTITION BY (SELECT count(*) FROM t4 WHERE t4.a = t1.b)) FROM t1",
            &[]
        ),
        @"SELECT sum((SELECT max(t3.a::int)::int FROM t3 WHERE (t3.a::int = t1.a::int)::bool)::int) OVER (PARTITION BY (SELECT count(*)::int FROM t4 WHERE (t4.a::int = t1.b::int)::bool)::int)::decimal FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT row_number() OVER (ORDER BY (SELECT count(*) FROM t3 WHERE t3.a = t1.a)) \
            FROM t1 GROUP BY a",
            &[]
        ),
        @"SELECT row_number() OVER (ORDER BY (SELECT count(*)::int FROM t3 WHERE (t3.a::int = t1.a::int)::bool)::int ASC)::int FROM t1 GROUP BY t1.a::int"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (PARTITION BY (SELECT row_number() OVER () FROM t3 LIMIT 1)) \
            FROM t1",
            &[]
        ),
        @"SELECT count(*) OVER (PARTITION BY (SELECT row_number() OVER ()::int FROM t3 LIMIT 1)::int)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT row_number() OVER (ORDER BY sum((SELECT count(*) OVER () FROM t3 LIMIT 1))) \
            FROM t1",
            &[]
        ),
        @"SELECT row_number() OVER (ORDER BY sum((SELECT count(*) OVER ()::int FROM t3 LIMIT 1)::int)::decimal ASC)::int FROM t1"
    );
}

// ------------- Composite queries -------------

/// Several windows in one select list: a named one, a reference to it that
/// adds ORDER BY and a frame, and an inline one; then a three-part chain.
#[test]
fn several_windows_in_one_query() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT b, sum(a) OVER w, avg(a) OVER (w ORDER BY c ROWS BETWEEN 1 PRECEDING AND 1 \
            FOLLOWING), row_number() OVER (PARTITION BY e ORDER BY b DESC) FROM t1 \
            WINDOW w AS (PARTITION BY e)",
            &[]
        ),
        @"SELECT t1.b::int, sum(t1.a::int) OVER w::decimal, avg(t1.a::int) OVER (w ORDER BY t1.c::double ASC ROWS BETWEEN 1::int PRECEDING AND 1::int FOLLOWING)::decimal, row_number() OVER (PARTITION BY t1.e::string ORDER BY t1.b::int DESC)::int FROM t1 WINDOW w AS (PARTITION BY t1.e::string)"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER w2 FROM t1 WINDOW w AS (PARTITION BY e), w2 \
            AS (w ORDER BY b DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
            &[]
        ),
        @"SELECT count(*) OVER w2::int FROM t1 WINDOW w AS (PARTITION BY t1.e::string), w2 AS (w ORDER BY t1.b::int DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    );
}

#[test]
fn window_over_join() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT t3.a, count(*) OVER (PARTITION BY t4.b ORDER BY t3.a) FROM t3 \
            JOIN t4 ON t3.a = t4.a",
            &[]
        ),
        @"SELECT t3.a::int, count(*) OVER (PARTITION BY t4.b::string ORDER BY t3.a::int ASC)::int FROM t3 INNER JOIN t4 ON (t3.a::int = t4.a::int)::bool"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT count(*) OVER (PARTITION BY t1.e ORDER BY t2.a RANGE BETWEEN 0.5 PRECEDING \
            AND 0.5 FOLLOWING) FROM t1 JOIN t2 ON t1.a = t2.c",
            &[]
        ),
        @"SELECT count(*) OVER (PARTITION BY t1.e::string ORDER BY t2.a::double ASC RANGE BETWEEN CAST(0.5 AS double) PRECEDING AND CAST(0.5 AS double) FOLLOWING)::int FROM t1 INNER JOIN t2 ON (t1.a::int = t2.c::int)::bool"
    );
    insta::assert_snapshot!(
        analyzed("SELECT row_number() OVER (ORDER BY a), * FROM t3", &[]),
        @"SELECT row_number() OVER (ORDER BY t3.a::int ASC)::int, t3.a::int, t3.b::string FROM t3"
    );
}

#[test]
fn window_with_distinct_filter_and_order_by() {
    insta::assert_snapshot!(
        analyzed("SELECT DISTINCT e, count(*) OVER (PARTITION BY e) FROM t1", &[]),
        @"SELECT DISTINCT t1.e::string, count(*) OVER (PARTITION BY t1.e::string)::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT sum(a) FILTER (WHERE g) OVER (PARTITION BY e) FROM t1", &[]),
        @"SELECT sum(t1.a::int) FILTER (WHERE t1.g::bool) OVER (PARTITION BY t1.e::string)::decimal FROM t1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t1 ORDER BY sum(a) OVER (PARTITION BY e), a LIMIT 3", &[]),
        @"SELECT t1.a::int FROM t1 ORDER BY sum(t1.a::int) OVER (PARTITION BY t1.e::string)::decimal ASC, 1 ASC LIMIT 3"
    );
}

/// Aggregates called as window functions, typed by the window registry.
#[test]
fn aggregate_window_function_types() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT avg(d) OVER (), sum(d) OVER (), min(e) OVER (), max(f) OVER (), count(c) \
            OVER () FROM t1",
            &[]
        ),
        @"SELECT avg(t1.d::decimal) OVER ()::decimal, sum(t1.d::decimal) OVER ()::decimal, min(t1.e::string) OVER ()::string, max(t1.f::string) OVER ()::string, count(t1.c::double) OVER ()::int FROM t1"
    );
    insta::assert_snapshot!(
        analyzed("SELECT string_agg(e, ',') OVER (PARTITION BY g ORDER BY b) FROM t1", &[]),
        @"SELECT string_agg(t1.e::string, ','::string) OVER (PARTITION BY t1.g::bool ORDER BY t1.b::int ASC)::string FROM t1"
    );
}
