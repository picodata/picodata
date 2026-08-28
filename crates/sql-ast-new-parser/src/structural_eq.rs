//! Tests for the structural [`PartialEq`] over whole raw statements.
//!
//! The impls live in `sql-ast-new-nodes` next to their nodes (`PartialEq` is
//! only implementable in the declaring crate); the tests live here because
//! building a tree requires parsing — the same split as the precedence tests
//! in [`crate::expr`], which cover the expression side of the comparison.

use sql_ast_new_nodes::multiset::MultisetStmt;
use sql_ast_new_nodes::{Node, Raw};

fn parse_stmt(query: &str) -> Box<MultisetStmt<'_, Raw>> {
    let ast = crate::parse(query).expect("query must parse");
    match ast.root {
        Node::DqlStmt(dql) => dql.stmt,
        Node::Empty => panic!("expected a DQL statement, got an empty node"),
    }
}

#[track_caller]
fn assert_same(a: &str, b: &str) {
    assert!(
        parse_stmt(a) == parse_stmt(b),
        "expected the same tree:\n  {a}\n  {b}"
    );
}

#[track_caller]
fn assert_differs(a: &str, b: &str) {
    assert!(
        parse_stmt(a) != parse_stmt(b),
        "expected different trees:\n  {a}\n  {b}"
    );
}

/// Re-parsing the same text must give an equal tree: every node kind the
/// grammar can produce goes through its `PartialEq` here.
#[test]
fn reparse_gives_an_equal_tree() {
    for query in [
        "WITH w (a, b) AS (SELECT 1, 2), v AS (SELECT a FROM w) SELECT a FROM v",
        "VALUES (1, 'x'), (2, 'y')",
        "SELECT a FROM t1 UNION ALL SELECT b FROM t2 EXCEPT SELECT c FROM t3",
        "SELECT a FROM t1 INTERSECT SELECT b FROM t2",
        "SELECT DISTINCT a, b AS x FROM t1 ORDER BY a DESC NULLS LAST, 2 LIMIT 5",
        "SELECT a FROM t1 LEFT JOIN t2 USING (a, b)",
        "SELECT a FROM (SELECT a FROM t) AS s INDEXED BY idx",
        "SELECT a FROM t1 WHERE a > 1 GROUP BY a, 2 HAVING count(*) > 0",
        "SELECT sum(a) FILTER (WHERE a > 0) \
         OVER (PARTITION BY b ORDER BY c ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t1",
        "SELECT row_number() OVER w FROM t1 WINDOW w AS (ORDER BY a)",
        "SELECT a FROM t1 WHERE EXISTS (SELECT b FROM t2) AND a IN (SELECT c FROM t3)",
    ] {
        assert_same(query, query);
    }
}

/// Different spellings that must reach the same tree.
#[test]
fn spelling_differences_the_tree_does_not_keep() {
    // Keyword case and identifier normalization.
    assert_same(
        "select A from T1 where A < 2",
        "SELECT a FROM t1 WHERE a < 2",
    );
    // Parentheses around a GROUP BY position are not a node.
    assert_same(
        "SELECT a FROM t1 GROUP BY ((2))",
        "SELECT a FROM t1 GROUP BY 2",
    );
    // OUTER is noise.
    assert_same(
        "SELECT a FROM t1 LEFT JOIN t2 ON t1.a = t2.a",
        "SELECT a FROM t1 LEFT OUTER JOIN t2 ON t1.a = t2.a",
    );
}

#[test]
fn set_operations_discriminate() {
    let union = "SELECT a FROM t1 UNION SELECT b FROM t2";
    assert_differs(union, "SELECT a FROM t1 EXCEPT SELECT b FROM t2");
    assert_differs(union, "SELECT a FROM t1 UNION ALL SELECT b FROM t2");
    // A set operation against one of its own branches.
    assert_differs(union, "SELECT a FROM t1");
}

#[test]
fn order_by_and_limit_discriminate() {
    let plain = "SELECT a FROM t1 ORDER BY a";
    assert_differs(plain, "SELECT a FROM t1 ORDER BY a DESC");
    assert_differs(plain, "SELECT a FROM t1 ORDER BY a NULLS FIRST");
    assert_differs(plain, "SELECT a FROM t1");
    assert_differs("SELECT a FROM t1 LIMIT 1", "SELECT a FROM t1 LIMIT 2");
    assert_differs("SELECT a FROM t1 LIMIT 1", "SELECT a FROM t1 LIMIT ALL");
    assert_differs("SELECT a FROM t1 LIMIT 1", "SELECT a FROM t1");
}

#[test]
fn ctes_discriminate() {
    let named = "WITH w AS (SELECT 1) SELECT a FROM w";
    assert_differs(named, "WITH v AS (SELECT 1) SELECT a FROM v");
    assert_differs(named, "WITH w (c) AS (SELECT 1) SELECT a FROM w");
    assert_differs(named, "WITH w AS (SELECT 2) SELECT a FROM w");
}

#[test]
fn select_lists_discriminate() {
    let plain = "SELECT a FROM t1";
    assert_differs(plain, "SELECT DISTINCT a FROM t1");
    assert_differs(plain, "SELECT a AS x FROM t1");
    assert_differs("SELECT * FROM t1", "SELECT t1.* FROM t1");
}

#[test]
fn from_clauses_discriminate() {
    let plain = "SELECT a FROM t1";
    assert_differs(plain, "SELECT a FROM t1 AS s");
    assert_differs(plain, "SELECT a FROM t1 INDEXED BY idx");
    let inner = "SELECT a FROM t1 INNER JOIN t2 ON t1.a = t2.a";
    assert_differs(inner, "SELECT a FROM t1 LEFT JOIN t2 ON t1.a = t2.a");
    assert_differs(inner, "SELECT a FROM t1 INNER JOIN t2 USING (a)");
}

#[test]
fn group_by_positions_are_not_literals() {
    assert_differs(
        "SELECT a FROM t1 GROUP BY 1",
        "SELECT a FROM t1 GROUP BY 1 + 0",
    );
    assert_differs("SELECT a FROM t1 GROUP BY 1", "SELECT a FROM t1 GROUP BY 2");
}

#[test]
fn window_functions_discriminate() {
    let counting = "SELECT count(*) OVER () FROM t1";
    assert_differs(counting, "SELECT count(a) OVER () FROM t1");
    assert_differs(
        counting,
        "SELECT count(*) FILTER (WHERE a > 0) OVER () FROM t1",
    );
    // A reference to a named window vs. that window's spec written inline.
    assert_differs(
        "SELECT sum(a) OVER w FROM t1 WINDOW w AS (PARTITION BY b)",
        "SELECT sum(a) OVER (PARTITION BY b) FROM t1 WINDOW w AS (PARTITION BY b)",
    );
    let rows = "SELECT sum(a) OVER (ORDER BY b ROWS 1 PRECEDING) FROM t1";
    assert_differs(
        rows,
        "SELECT sum(a) OVER (ORDER BY b RANGE 1 PRECEDING) FROM t1",
    );
    assert_differs(
        rows,
        "SELECT sum(a) OVER (ORDER BY b ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t1",
    );
}

/// The statement arms inside expressions: these compared by rendering before
/// they compared structurally.
#[test]
fn statements_inside_expressions_discriminate() {
    assert_differs(
        "SELECT (SELECT a FROM t2) FROM t1",
        "SELECT (SELECT b FROM t2) FROM t1",
    );
    assert_differs(
        "SELECT a FROM t1 WHERE EXISTS (SELECT a FROM t2)",
        "SELECT a FROM t1 WHERE EXISTS (SELECT b FROM t2)",
    );
    assert_differs("VALUES (1)", "VALUES (1), (1)");
    assert_differs("VALUES (1)", "VALUES (2)");
}
