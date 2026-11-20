use super::*;
use crate::test_helpers::sql_to_optimized_ir;
use insta::assert_snapshot;
use sql_explain::explain::utils::format_query;

fn pattern(query: &str) -> String {
    check_sql_with_snapshot(query, vec![], Snapshot::Oldest).pattern
}

/// The local SQL of `query` as `EXPLAIN (RAW)` writes it, a clause a line, and
/// its parameters.
fn local_sql(query: &str) -> String {
    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    let mut text = format_query(&output.pattern, &[] as &[&str], true);
    for param in &output.params {
        text += &format!("\n- {param:?}");
    }
    text
}

#[test]
fn subquery_is_written_once() {
    let sql =
        local_sql("SELECT a FROM global_t WHERE (SELECT a + 1 FROM global_t) BETWEEN 1 AND 2");
    assert_snapshot!(sql, @r#"
    SELECT
      "global_t"."a"
    FROM
      "global_t"
    WHERE
      (
        SELECT
          "global_t"."a" + $1::int as "col_1"
        FROM
          "global_t"
      ) BETWEEN $2::int AND $3::int
    - Integer(1)
    - Integer(1)
    - Integer(2)
    "#);
}

/// Also when the condition around `BETWEEN` is rebuilt, as the push-down of
/// `enrich_restrictions` does around `a = 1`.
#[test]
fn nested_between_keeps_the_text_linear() {
    let levels = 40;
    for other in ["", "a = 1 AND "] {
        let mut query = "SELECT a FROM global_t".to_string();
        for _ in 0..levels {
            query = format!("SELECT a FROM global_t WHERE {other}({query}) BETWEEN 0 AND 1");
        }
        let pattern = pattern(&query);
        assert_eq!(pattern.matches("SELECT").count(), levels + 1, "{other}");
        assert_eq!(pattern.matches("BETWEEN").count(), levels, "{other}");
    }
}

#[test]
fn parentheses_follow_the_comparisons() {
    let sql = local_sql(
        "SELECT a FROM global_t \
         WHERE (a = 1) BETWEEN false AND true AND (a + 1 BETWEEN 1 AND 2) = (a = 3)",
    );
    assert_snapshot!(sql, @r#"
    SELECT
      "global_t"."a"
    FROM
      "global_t"
    WHERE
      ("global_t"."a" = $1::int) BETWEEN $2::bool AND $3::bool
      and (
        "global_t"."a" + $4::int BETWEEN $5::int AND $6::int
      ) = ("global_t"."a" = $7::int)
    - Integer(1)
    - Boolean(false)
    - Boolean(true)
    - Integer(1)
    - Integer(1)
    - Integer(2)
    - Integer(3)
    "#);
}

#[test]
fn not_between_is_not_over_between() {
    for (left, sql) in [
        (
            "a",
            r#"not ("global_t"."a" >= $1::int and "global_t"."a" <= $2::int)"#,
        ),
        (
            "a + 1",
            r#"not "global_t"."a" + $1::int BETWEEN $2::int AND $3::int"#,
        ),
    ] {
        let query = format!("SELECT a FROM global_t WHERE {left} NOT BETWEEN 1 AND 2");
        for snapshot in [Snapshot::Oldest, Snapshot::Latest] {
            let pattern = check_sql_with_snapshot(&query, vec![], snapshot).pattern;
            assert!(pattern.ends_with(&format!("WHERE {sql}")), "{pattern}");
        }
    }
}

#[test]
fn nested_between_over_a_boolean_keeps_the_text_linear() {
    for wrapper in [
        "({x}) BETWEEN false AND true",
        "({x}) NOT BETWEEN false AND true",
        "NOT (({x}) BETWEEN false AND true)",
        "({x} AND a = 2) BETWEEN false AND true",
        "({x} OR a = 2) BETWEEN false AND true",
    ] {
        let mut x = "a = 1".to_string();
        for _ in 0..40 {
            x = wrapper.replace("{x}", &x);
        }
        let query = format!("SELECT a FROM global_t WHERE {x}");
        for snapshot in [Snapshot::Oldest, Snapshot::Latest] {
            let pattern = check_sql_with_snapshot(&query, vec![], snapshot).pattern;
            assert_eq!(pattern.matches("BETWEEN").count(), 40, "{wrapper}");
        }
    }
}

/// The pushdown of `LIMIT` compares the expressions of `ORDER BY`.
#[test]
fn equal_order_by_expressions_are_compared_linearly() {
    let mut x = "id = 1".to_string();
    for _ in 0..40 {
        x = format!("({x}) BETWEEN false AND true");
    }
    let plan = sql_to_optimized_ir(
        &format!("SELECT id FROM test_space ORDER BY {x}, {x} LIMIT 1"),
        vec![],
    );
    assert!(plan.get_top().is_ok());
}

/// Matching the output to `GROUP BY` compares the two subqueries, and a join also checks
/// the sides of the references in its condition.
#[test]
fn join_condition_of_equal_subqueries_is_compared_linearly() {
    let mut x = "x.id = y.id".to_string();
    for _ in 0..40 {
        x = format!("({x}) BETWEEN false AND true");
    }
    let sq = format!("(SELECT sum(x.id) FROM test_space x JOIN test_space y ON {x})");
    let plan = sql_to_optimized_ir(
        &format!("SELECT id + {sq} FROM test_space GROUP BY id + {sq}"),
        vec![],
    );
    assert!(plan.get_top().is_ok());
}
