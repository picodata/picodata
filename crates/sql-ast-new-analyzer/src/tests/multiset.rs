//! The WITH clause, the `UNION` / `EXCEPT` / `INTERSECT` tree, and the clauses
//! that trail a body: ORDER BY, LIMIT, and VALUES as a body of its own.
//!
//! # Cases
//! A CTE is analyzed once, where it is declared, and referred to afterwards by
//! name; the two halves fail differently and are sectioned apart. Declaring one
//! can go wrong on its own terms — a repeated name, a column list that does not
//! match the body it names — while referring to one is a question of what the
//! name resolves to and which columns it carries out.
//!
//! Set operations get a section of their own because a branch is not typed on
//! its own: derivation is postponed to the root of the tree, so the branches
//! settle on one output type together or fail to.
//!
//! # Rejections
//! The statement bodies and trailing clauses the analyzer does not implement
//! yet are pinned here as errors. They must be *rejected*, not silently
//! dropped: a query that quietly loses its ORDER BY returns wrong rows instead
//! of failing.

use super::{analyze_error, analyzed};

// ------------- Declaring a CTE -------------

#[test]
fn duplicating_cte() {
    let query = "WITH a AS (SELECT 1), a AS (SELECT 2) SELECT * FROM a";
    insta::assert_snapshot!(analyze_error(query, &[]), @r#"failed to analyze AST: WITH query name "a" specified more than once"#);
}

#[test]
fn cte_inconsistent_columns_count() {
    let query = "WITH cte (a1, a2) AS (SELECT 1 AS a) SELECT a1 FROM cte";
    insta::assert_snapshot!(analyze_error(query, &[]), @r#"failed to analyze AST: WITH query "cte" has 1 columns available but 2 columns specified"#);
}

#[test]
fn cte_in_subquery() {
    let query = "WITH c AS (SELECT 1 AS x) SELECT (SELECT c.x FROM c) FROM t1;";
    insta::assert_snapshot!(analyzed(query, &[]), @"WITH c AS (SELECT 1::int AS x) SELECT (SELECT c.x::int FROM c)::int FROM t1");
}

#[test]
fn preceeding_cte_visible() {
    let query = "WITH cte1 AS (SELECT 1 AS a), cte2 AS (SELECT a FROM cte1) SELECT a FROM cte2";
    insta::assert_snapshot!(analyzed(query, &[]), @"WITH cte1 AS (SELECT 1::int AS a), cte2 AS (SELECT cte1.a::int FROM cte1) SELECT cte2.a::int FROM cte2");
}

#[test]
fn preceeding_cte_visible_nested() {
    let query = "WITH cte1 AS (SELECT 1 AS a), cte2 AS (SELECT a FROM cte1) SELECT (WITH cte2 AS (SELECT 'a' AS a) SELECT * FROM cte2), a FROM cte2";
    insta::assert_snapshot!(analyzed(query, &[]), @"WITH cte1 AS (SELECT 1::int AS a), cte2 AS (SELECT cte1.a::int FROM cte1) SELECT (WITH cte2 AS (SELECT 'a'::string AS a) SELECT cte2.a::string FROM cte2)::string, cte2.a::int FROM cte2");
}

#[test]
fn cte_preferrable_over_table() {
    let query = "WITH t1 AS (SELECT 1 AS x) SELECT x FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"WITH t1 AS (SELECT 1::int AS x) SELECT t1.x::int FROM t1");
}

#[test]
fn subquery_with_nested_cte_table_expr() {
    let query =
        "WITH cte1 AS (SELECT 1) SELECT * FROM (WITH cte2 AS (SELECT 2) SELECT * FROM cte2) AS t";
    insta::assert_snapshot!(analyzed(query, &[]), @"WITH cte1 AS (SELECT 1::int) SELECT ?column?::int FROM (WITH cte2 AS (SELECT 2::int) SELECT ?column?::int FROM cte2) AS t");
}

// ------------- Columns a CTE exposes -------------

#[test]
fn resolve_column_from_cte() {
    let query = "WITH cte AS (SELECT 1 AS a, 2 as b) SELECT a FROM cte";
    insta::assert_snapshot!(analyzed(query, &[]), @"WITH cte AS (SELECT 1::int AS a, 2::int AS b) SELECT cte.a::int FROM cte");
}

#[test]
fn resolve_column_from_cte_with_column_list() {
    let query = "WITH cte (a, b) AS (SELECT 1, 2) SELECT a FROM cte";
    insta::assert_snapshot!(analyzed(query, &[]), @"WITH cte (a, b) AS (SELECT 1::int, 2::int) SELECT cte.a::int FROM cte");
}

#[test]
fn resolve_column_from_cte_with_column_list_nonexisting() {
    let query = "WITH cte (a, b) AS (SELECT 1, 2) SELECT c FROM cte";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @"failed to analyze AST: cannot resolve column reference 'c'"
    );
}

#[test]
fn qualified_column_reference_to_unnamed_cte_result_column() {
    let query = "WITH cte AS (SELECT a FROM t1) SELECT a FROM cte";
    insta::assert_snapshot!(analyzed(query, &[]), @"WITH cte AS (SELECT t1.a::int FROM t1) SELECT cte.a::int FROM cte");
}

#[test]
fn ambigious_column_reference_cte() {
    let query = "WITH cte (a, a) AS (SELECT 1, 2) SELECT a FROM cte;";
    insta::assert_snapshot!(analyze_error(query, &[]), @"failed to analyze AST: column reference 'a' is ambigious");
}

#[test]
fn qualified_cte_column_reference_in_correlated_subquery() {
    let query = "SELECT (WITH t1 (a) AS (SELECT 1) SELECT t1.b) FROM t1";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (WITH t1 (a) AS (SELECT 1::int) SELECT t1.b::int)::int FROM t1");
}

#[test]
fn qualified_cte_missing_column_reference_in_correlated_subquery() {
    let query = "SELECT (WITH t1 (a) AS (SELECT 1) SELECT t1.b FROM t1) FROM t1;";
    insta::assert_snapshot!(analyze_error(query, &[]), @"failed to analyze AST: cannot resolve column reference 't1.b'");
}

#[test]
fn correlated_subquery_in_cte() {
    let query = "SELECT (WITH q AS (SELECT t2.a) SELECT * FROM q) FROM t2";
    insta::assert_snapshot!(analyzed(query, &[]), @"SELECT (WITH q AS (SELECT t2.a::double) SELECT q.a::double FROM q)::double FROM t2");
}

// ------------- Set operations -------------

#[test]
fn set_operation_castable_literals() {
    let query = "SELECT 1 UNION SELECT '1'";
    insta::assert_snapshot!(analyzed(query, &[]), @"(SELECT 1::int) UNION (SELECT 1::int)");
}

#[test]
fn set_operation_castable_columns() {
    let query = "SELECT * FROM (SELECT 1 a) UNION (SELECT 1.5 a)";
    insta::assert_snapshot!(analyzed(query, &[]), @"(SELECT CAST(a AS decimal) FROM (SELECT 1::int AS a)) UNION (SELECT 1.5::decimal AS a)");
}

/// The literal is read as the branch's `int` right here, the way Postgres
/// resolves an untyped literal during parse analysis - so a literal that is not
/// valid `int` input fails now rather than at runtime.
#[test]
fn set_operation_uncastable_columns() {
    let query = "SELECT * FROM (SELECT 1 a) UNION (SELECT '1.5' a)";
    insta::assert_snapshot!(
        analyze_error(query, &[]),
        @r#"failed to analyze AST: invalid input syntax for type int: "1.5""#
    );
}

#[test]
fn incorrect_cast_int_to_double_is_ok() {
    insta::assert_snapshot!(
        analyzed("(SELECT 9007199254740992 UNION SELECT 9007199254740993) UNION ALL SELECT
    0::double", &[]),
        @"((SELECT CAST(9007199254740992 AS double)) UNION (SELECT CAST(9007199254740993 AS double))) UNION ALL (SELECT CAST(0 AS double))"
    );
}

#[test]
fn correct_cast_int_to_decimal() {
    insta::assert_snapshot!(
        analyzed("(SELECT 9007199254740992 UNION SELECT 9007199254740993) UNION ALL SELECT
    0::decimal", &[]),
        @"((SELECT CAST(9007199254740992 AS decimal)) UNION (SELECT CAST(9007199254740993 AS decimal))) UNION ALL (SELECT CAST(0 AS decimal))"
    );
}

#[test]
fn get_cast_from_report_for_homogenours_types() {
    insta::assert_snapshot!(
        analyzed("SELECT * FROM (SELECT 1::int AS x UNION SELECT 2::double) q", &[]),
        @"SELECT q.x::double FROM ((SELECT CAST(1::int AS double) AS x) UNION (SELECT CAST(2 AS double))) AS q"
    )
}

// ------------- ORDER BY -------------

/// A bare name names the *output column*, not the input one - so it renders
/// back as the position it resolved to, the same way an explicit alias does.
#[test]
fn order_by_simple() {
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t1 ORDER BY a", &[]),
        @"SELECT t1.a::int FROM t1 ORDER BY 1 ASC"
    );
}

/// ...and only a name no output column carries falls through to the input scope.
#[test]
fn order_by_input_column() {
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t1 ORDER BY b", &[]),
        @"SELECT t1.a::int FROM t1 ORDER BY t1.b::int ASC"
    );
}

#[test]
fn order_by_bind_alias_first() {
    insta::assert_snapshot!(
        analyzed("SELECT a as e FROM t1 ORDER BY e", &[]),
        @"SELECT t1.a::int AS e FROM t1 ORDER BY 1 ASC"
    );
}

#[test]
fn order_by_ungrouped_column_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(b) FROM t1 ORDER BY a;", &[]),
        @r#"failed to analyze AST: column "t1.a" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn subquery_with_order_by_referencing_table_column() {
    insta::assert_snapshot!(
        analyzed("SELECT x FROM (SELECT a AS x FROM t1 ORDER BY a) AS s", &[]),
        @"SELECT s.x::int FROM (SELECT t1.a::int AS x FROM t1 ORDER BY t1.a::int ASC) AS s"
    );
}

#[test]
fn order_by_ungrouped_column_of_grouped_query_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 GROUP BY a ORDER BY b", &[]),
        @r#"failed to analyze AST: column "t1.b" must appear in the GROUP BY clause or be used in an aggregate function"#
    );
}

#[test]
fn order_by_aggregate_alias_of_grouped_query() {
    insta::assert_snapshot!(
        analyzed("SELECT a, count(*) AS n FROM t1 GROUP BY a ORDER BY n DESC", &[]),
        @"SELECT t1.a::int, count(*)::int AS n FROM t1 GROUP BY t1.a::int ORDER BY 2 DESC"
    );
}

#[test]
fn order_by_invalid_coercion() {
    insta::assert_snapshot!(
        analyze_error("SELECT 1 FROM t1 ORDER BY a + 'xyz'", &[]),
        @r#"failed to analyze AST: invalid input syntax for type int: "xyz""#
    );
}

// ------------- ORDER BY: ordinal positions -------------

/// A position is counted over the output columns, so one past the end is an
/// error rather than something IR construction has to defend against.
#[test]
fn order_by_position_out_of_range_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 ORDER BY 2", &[]),
        @"failed to analyze AST: ORDER BY position 2 is not in select list"
    );
}

#[test]
fn order_by_position_out_of_range_in_grouped_query_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT a, count(*) FROM t1 GROUP BY 1 ORDER BY 3", &[]),
        @"failed to analyze AST: ORDER BY position 3 is not in select list"
    );
}

/// Every body counts its own output columns - the check is not the enclosing
/// statement's to make.
#[test]
fn order_by_position_out_of_range_in_from_subquery_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM (SELECT a FROM t1 ORDER BY 2) s", &[]),
        @"failed to analyze AST: ORDER BY position 2 is not in select list"
    );
}

#[test]
fn order_by_position_out_of_range_in_cte_error() {
    insta::assert_snapshot!(
        analyze_error("WITH q AS (SELECT a FROM t1) SELECT a FROM q ORDER BY 2", &[]),
        @"failed to analyze AST: ORDER BY position 2 is not in select list"
    );
}

#[test]
fn order_by_position_out_of_range_in_set_operation_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 UNION SELECT c FROM t5 ORDER BY 2", &[]),
        @"failed to analyze AST: ORDER BY position 2 is not in select list"
    );
}

// ------------- ORDER BY: output column names -------------

/// An unaliased element still exposes a name, and ORDER BY may use it: a call
/// is named by its function, so `abs(a)` answers to `abs`.
#[test]
fn order_by_implicit_output_name_of_function_call() {
    insta::assert_snapshot!(
        analyzed("SELECT abs(a) FROM t1 ORDER BY abs", &[]),
        @"SELECT abs(t1.a::int)::int FROM t1 ORDER BY 1 ASC"
    );
}

/// A cast defers to its operand, so `a::text` is still named `a` - and naming
/// the output column is what keeps this out of the grouping check, which would
/// otherwise see an ungrouped read of `t1.a`.
#[test]
fn order_by_implicit_output_name_through_cast() {
    insta::assert_snapshot!(
        analyzed("SELECT a::text FROM t1 GROUP BY 1 ORDER BY a", &[]),
        @"SELECT t1.a::string FROM t1 GROUP BY 1 ORDER BY 1 ASC"
    );
}

/// An implicit name collides with an alias just as two aliases would.
#[test]
fn order_by_ambiguous_between_alias_and_implicit_name_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT b AS a, a FROM t1 ORDER BY a", &[]),
        @r#"failed to analyze AST: ORDER BY "a" is ambiguous"#
    );
}

/// Two elements may share a name as long as they are the same expression -
/// then there is no question which one was meant.
#[test]
fn order_by_duplicate_name_of_equal_expressions() {
    insta::assert_snapshot!(
        analyzed("SELECT a AS z, a AS z FROM t1 ORDER BY z", &[]),
        @"SELECT t1.a::int AS z, t1.a::int AS z FROM t1 ORDER BY 1 ASC"
    );
}

#[test]
fn order_by_duplicate_name_of_different_expressions_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT a AS z, b AS z FROM t1 ORDER BY z", &[]),
        @r#"failed to analyze AST: ORDER BY "z" is ambiguous"#
    );
}

// ------------- ORDER BY: row expressions -------------

/// `(1, b)` in ORDER BY is a row value, not the list `1, b` that GROUP BY reads
/// the same text as: sorting by it compares rows field by field, the constant
/// `1` first, and folding it into `ORDER BY 1, b` would sort by the first
/// output column instead. So the row reaches the analyzer intact - and is then
/// rejected the way a row is anywhere a scalar is needed, since there is no row
/// type to sort by yet.
#[test]
fn order_by_row_expression_is_not_flattened() {
    insta::assert_snapshot!(
        analyze_error("SELECT a, b FROM t1 ORDER BY (1, b)", &[]),
        @"row value misused"
    );
    insta::assert_snapshot!(
        analyze_error("SELECT a, b FROM t1 ORDER BY (a, b)", &[]),
        @"row value misused"
    );
    // Over a set operation a row is what any expression that is not an output
    // column is.
    insta::assert_snapshot!(
        analyze_error(
            "SELECT a, b FROM t1 UNION SELECT a, b FROM t1 ORDER BY (1, 2)",
            &[]
        ),
        @"failed to analyze AST: invalid UNION/INTERSECT/EXCEPT ORDER BY clause"
    );
}

/// Redundant parentheses around a single element are not a row.
#[test]
fn order_by_parenthesized_element_is_not_a_row() {
    insta::assert_snapshot!(
        analyzed("SELECT a, b FROM t1 ORDER BY ((1)), (b)", &[]),
        @"SELECT t1.a::int, t1.b::int FROM t1 ORDER BY 1 ASC, 2 ASC"
    );
}

// ------------- ORDER BY: SELECT DISTINCT -------------

#[test]
fn order_by_distinct_projected_expression() {
    insta::assert_snapshot!(
        analyzed("SELECT DISTINCT a + 1 FROM t1 ORDER BY a + 1", &[]),
        @"SELECT DISTINCT (t1.a::int + 1::int)::int FROM t1 ORDER BY (t1.a::int + 1::int)::int ASC"
    );
}

/// Sorting by a value that is not unique within a distinct group has no
/// defined result, so the sort key has to be one of the projections.
#[test]
fn order_by_distinct_unprojected_column_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT DISTINCT a FROM t1 ORDER BY b", &[]),
        @"failed to analyze AST: for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
}

#[test]
fn order_by_distinct_unprojected_expression_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT DISTINCT a FROM t1 ORDER BY a + 1", &[]),
        @"failed to analyze AST: for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
}

// ------------- ORDER BY: over a set operation -------------

/// A set operation takes its output column names from the leftmost branch.
#[test]
fn order_by_set_operation_output_name() {
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t1 UNION SELECT c FROM t5 ORDER BY a", &[]),
        @"(SELECT t1.a::int FROM t1) UNION (SELECT t5.c::int FROM t5) ORDER BY 1 ASC"
    );
}

#[test]
fn order_by_set_operation_leftmost_alias() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT a AS x FROM t1 UNION SELECT c AS y FROM t5 ORDER BY x",
            &[]
        ),
        @"(SELECT t1.a::int AS x FROM t1) UNION (SELECT t5.c::int AS y FROM t5) ORDER BY 1 ASC"
    );
}

/// The output columns of a set operation are distinct columns of the
/// operation, however the leftmost branch computed them, so a name shared by
/// two of them is ambiguous even when that branch projected the same
/// expression twice - unlike a plain SELECT, where equal expressions may share
/// a name (see [`order_by_duplicate_name_of_equal_expressions`]).
#[test]
fn order_by_set_operation_duplicate_output_name_error() {
    insta::assert_snapshot!(
        analyze_error(
            "SELECT a AS x, b AS x FROM t1 UNION SELECT a, b FROM t1 ORDER BY x",
            &[]
        ),
        @r#"failed to analyze AST: ORDER BY "x" is ambiguous"#
    );
    insta::assert_snapshot!(
        analyze_error(
            "SELECT a AS x, a AS x FROM t1 UNION SELECT a, b FROM t1 ORDER BY x",
            &[]
        ),
        @r#"failed to analyze AST: ORDER BY "x" is ambiguous"#
    );
}

/// Only the leftmost branch names the output columns, so the right branch
/// repeating a name does not make it ambiguous - and a position never is.
#[test]
fn order_by_set_operation_duplicate_name_across_branches_or_by_position() {
    insta::assert_snapshot!(
        analyzed("SELECT a AS x FROM t1 UNION SELECT b AS x FROM t1 ORDER BY x", &[]),
        @"(SELECT t1.a::int AS x FROM t1) UNION (SELECT t1.b::int AS x FROM t1) ORDER BY 1 ASC"
    );
    insta::assert_snapshot!(
        analyzed(
            "SELECT a AS x, b AS x FROM t1 UNION SELECT a, b FROM t1 ORDER BY 1",
            &[]
        ),
        @"(SELECT t1.a::int AS x, t1.b::int AS x FROM t1) UNION (SELECT t1.a::int, t1.b::int FROM t1) ORDER BY 1 ASC"
    );
}

/// A set operation exposes output columns but no input scope of its own, so
/// nothing but an output column reference can be sorted by.
#[test]
fn order_by_set_operation_expression_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 UNION SELECT c FROM t5 ORDER BY a + 1", &[]),
        @"failed to analyze AST: invalid UNION/INTERSECT/EXCEPT ORDER BY clause"
    );
}

#[test]
fn order_by_set_operation_aggregate_error() {
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 UNION SELECT c FROM t5 ORDER BY count(*)", &[]),
        @"failed to analyze AST: invalid UNION/INTERSECT/EXCEPT ORDER BY clause"
    );
}

/// A set operation is as wide as any one of its branches, however many there
/// are - the width check walks into the left operand rather than giving up on it.
#[test]
fn set_operation_of_three_branches() {
    insta::assert_snapshot!(
        analyzed(
            "SELECT a FROM t1 UNION SELECT c FROM t5 UNION SELECT a FROM t3",
            &[]
        ),
        @"((SELECT t1.a::int FROM t1) UNION (SELECT t5.c::int FROM t5)) UNION (SELECT t3.a::int FROM t3)"
    );
}

// ------------- LIMIT -------------

#[test]
fn limit_unsigned() {
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t1 LIMIT 10", &[]),
        @"SELECT t1.a::int FROM t1 LIMIT 10"
    );
}

// ------------- VALUES -------------

#[test]
fn values_single_row() {
    insta::assert_snapshot!(
        analyzed("VALUES (1, 'a', true)", &[]),
        @"VALUES (1::int, 'a'::string, true::bool)"
    );
}

#[test]
fn values_rows_unify_column_wise() {
    insta::assert_snapshot!(
        analyzed("VALUES (1), (2.5)", &[]),
        @"VALUES (CAST(1 AS decimal)), (2.5::decimal)"
    );
}

#[test]
fn values_type_mismatch() {
    insta::assert_snapshot!(
        analyze_error("VALUES (1), (true)", &[]),
        @"VALUES types int and bool cannot be matched"
    );
}

#[test]
fn values_of_untyped_nulls() {
    insta::assert_snapshot!(
        analyzed("VALUES (NULL), (NULL)", &[]),
        @"VALUES (NULL::string), (NULL::string)"
    );
}

#[test]
fn values_row_length_mismatch() {
    insta::assert_snapshot!(
        analyze_error("VALUES (1), (2, 3)", &[]),
        @"failed to analyze AST: VALUES lists must all be the same length"
    );
}

#[test]
fn values_expression_error_beats_length_mismatch() {
    insta::assert_snapshot!(
        analyze_error("VALUES (1), (x, 2)", &[]),
        @"failed to analyze AST: cannot resolve column reference 'x'"
    );
}

#[test]
fn values_aggregate_is_rejected() {
    insta::assert_snapshot!(
        analyze_error("VALUES (max(1))", &[]),
        @"failed to analyze AST: aggregate functions are not allowed in VALUES"
    );
}

#[test]
fn values_column_reference_is_rejected() {
    insta::assert_snapshot!(
        analyze_error("VALUES (a)", &[]),
        @"failed to analyze AST: cannot resolve column reference 'a'"
    );
}

#[test]
fn values_element_may_be_a_subquery() {
    insta::assert_snapshot!(
        analyzed("VALUES ((SELECT 1)), (2)", &[]),
        @"VALUES ((SELECT 1::int)::int), (2::int)"
    );
}

// ------------- VALUES: trailing clauses -------------

#[test]
fn values_order_by_ordinal() {
    insta::assert_snapshot!(
        analyzed("VALUES (2), (1) ORDER BY 1", &[]),
        @"VALUES (2::int), (1::int) ORDER BY 1 ASC"
    );
}

#[test]
fn values_order_by_output_name() {
    insta::assert_snapshot!(
        analyzed("VALUES (2, 'b'), (1, 'a') ORDER BY column2 DESC, column1", &[]),
        @"VALUES (2::int, 'b'::string), (1::int, 'a'::string) ORDER BY 2 DESC, 1 ASC"
    );
}

#[test]
fn values_order_by_position_out_of_range() {
    insta::assert_snapshot!(
        analyze_error("VALUES (1) ORDER BY 2", &[]),
        @"failed to analyze AST: ORDER BY position 2 is not in select list"
    );
}

#[test]
fn values_order_by_unknown_name() {
    insta::assert_snapshot!(
        analyze_error("VALUES (1) ORDER BY x", &[]),
        @"failed to analyze AST: cannot resolve column reference 'x'"
    );
}

#[test]
fn values_order_by_expression_is_rejected() {
    insta::assert_snapshot!(
        analyze_error("VALUES (1) ORDER BY column1 + 1", &[]),
        @"failed to analyze AST: ORDER BY expressions over VALUES are not supported yet"
    );
}

#[test]
fn values_limit() {
    insta::assert_snapshot!(
        analyzed("VALUES (1), (2), (3) LIMIT 2", &[]),
        @"VALUES (1::int), (2::int), (3::int) LIMIT 2"
    );
}

// ------------- VALUES as a row source -------------

#[test]
fn values_in_from() {
    insta::assert_snapshot!(
        analyzed("SELECT column2, column1 FROM (VALUES (1, 'a'), (2, 'b'))", &[]),
        @"SELECT column2::string, column1::int FROM (VALUES (1::int, 'a'::string), (2::int, 'b'::string))"
    );
}

#[test]
fn values_in_from_asterisk() {
    insta::assert_snapshot!(
        analyzed("SELECT * FROM (VALUES (1, true))", &[]),
        @"SELECT column1::int, column2::bool FROM (VALUES (1::int, true::bool))"
    );
}

#[test]
fn values_in_from_aliased() {
    insta::assert_snapshot!(
        analyzed("SELECT v.column1 FROM (VALUES (1, 2)) v", &[]),
        @"SELECT v.column1::int FROM (VALUES (1::int, 2::int)) AS v"
    );
}

#[test]
fn values_in_from_unknown_column() {
    insta::assert_snapshot!(
        analyze_error("SELECT x FROM (VALUES (1))", &[]),
        @"failed to analyze AST: cannot resolve column reference 'x'"
    );
}

#[test]
fn values_cte() {
    insta::assert_snapshot!(
        analyzed("WITH w AS (VALUES (1), (2)) SELECT column1 FROM w", &[]),
        @"WITH w AS (VALUES (1::int), (2::int)) SELECT w.column1::int FROM w"
    );
}

#[test]
fn values_cte_renamed() {
    insta::assert_snapshot!(
        analyzed("WITH w(x) AS (VALUES (1), (2)) SELECT x FROM w", &[]),
        @"WITH w (x) AS (VALUES (1::int), (2::int)) SELECT w.x::int FROM w"
    );
    insta::assert_snapshot!(
        analyzed("WITH w(x, y) AS (VALUES (1, 2)) SELECT * FROM w", &[]),
        @"WITH w (x, y) AS (VALUES (1::int, 2::int)) SELECT w.x::int, w.y::int FROM w"
    );
}

#[test]
fn values_cte_column_count_mismatch() {
    insta::assert_snapshot!(
        analyze_error("WITH w(x, y) AS (VALUES (1)) SELECT x FROM w", &[]),
        @r#"failed to analyze AST: WITH query "w" has 1 columns available but 2 columns specified"#
    );
}

// ------------- VALUES in set operations -------------

#[test]
fn values_union_select() {
    insta::assert_snapshot!(
        analyzed("VALUES (1) UNION SELECT 2.5", &[]),
        @"(VALUES (CAST(1 AS decimal))) UNION (SELECT 2.5::decimal)"
    );
}

#[test]
fn values_union_order_by_name() {
    insta::assert_snapshot!(
        analyzed("VALUES (1, 2) UNION SELECT 3, 4 ORDER BY column2", &[]),
        @"(VALUES (1::int, 2::int)) UNION (SELECT 3::int, 4::int) ORDER BY 2 ASC"
    );
}

#[test]
fn values_union_column_count_mismatch() {
    insta::assert_snapshot!(
        analyze_error("VALUES (1) UNION SELECT 1, 2", &[]),
        @"failed to analyze AST: operands of UNION operation have different number of columns"
    );
}

// ------------- VALUES as a subquery expression -------------

#[test]
fn values_scalar_subquery() {
    insta::assert_snapshot!(
        analyzed("SELECT (VALUES (1))", &[]),
        @"SELECT (VALUES (1::int))::int"
    );
}

#[test]
fn values_scalar_subquery_two_columns() {
    insta::assert_snapshot!(
        analyze_error("SELECT (VALUES (1, 2))", &[]),
        @"failed to analyze AST: subquery must return only one column"
    );
}

#[test]
fn values_correlated() {
    insta::assert_snapshot!(
        analyzed("SELECT (VALUES (a)) FROM t1", &[]),
        @"SELECT (VALUES (t1.a::int))::int FROM t1"
    );
}

#[test]
fn values_aggregate_of_outer_level() {
    insta::assert_snapshot!(
        analyzed("SELECT (VALUES (max(a))) FROM t1", &[]),
        @"SELECT (VALUES (max(t1.a::int)::int))::int FROM t1"
    );
}

#[test]
fn values_in_predicate() {
    insta::assert_snapshot!(
        analyzed("SELECT a FROM t1 WHERE a IN (VALUES (1), (2))", &[]),
        @"SELECT t1.a::int FROM t1 WHERE (t1.a::int IN (VALUES (1::int), (2::int))::int)::bool"
    );
}

// ------------- Unsupported clauses -------------

#[test]
fn window_clause_is_rejected() {
    insta::assert_snapshot!(
        analyze_error("SELECT a FROM t1 WINDOW w AS ()", &[]),
        @"failed to analyze AST: WINDOW clause is not supported yet"
    );
}

#[test]
fn window_function_is_rejected() {
    insta::assert_snapshot!(
        analyze_error("SELECT sum(a) OVER () FROM t1", &[]),
        @"failed to analyze AST: window functions are not supported yet"
    );
}
