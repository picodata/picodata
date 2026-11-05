use super::*;
use crate::ir::tree::Snapshot;
use insta::assert_yaml_snapshot;

#[test]
fn projection1() {
    let query = r#"SELECT "identification_number", "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1"#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"identification_number\", \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" = $1::int"
    params:
      - Integer: 1
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"identification_number\", \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" = $1::int"
    params:
      - Integer: 1
    "#);
}

#[test]
fn projection2() {
    let query = r#"SELECT *
        FROM "hash_testing"
        WHERE "identification_number" = 1"#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"identification_number\", \"hash_testing\".\"product_code\", \"hash_testing\".\"product_units\", \"hash_testing\".\"sys_op\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" = $1::int"
    params:
      - Integer: 1
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"identification_number\", \"hash_testing\".\"product_code\", \"hash_testing\".\"product_units\", \"hash_testing\".\"sys_op\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" = $1::int"
    params:
      - Integer: 1
    "#);
}

#[test]
fn select_without_scan() {
    let query = r#"SELECT 1 as foo, (values (1)), (select a from global_t)"#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT $1::int as \"foo\", (VALUES ($2::int)) as \"col_1\", (SELECT \"global_t\".\"a\" FROM \"global_t\") as \"col_2\""
    params:
      - Integer: 1
      - Integer: 1
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT $1::int as \"foo\", (VALUES ($2::int)) as \"col_1\", (SELECT \"global_t\".\"a\" FROM \"global_t\") as \"col_2\""
    params:
      - Integer: 1
      - Integer: 1
    "#);
}

/// The walk of the SQL text enters a subquery once per its use in the text.
/// Entering it twice would double the work on each level of nesting.
#[test]
fn nested_subqueries_in_projection() {
    let levels = 40;
    let mut query = "SELECT a FROM global_t".to_string();
    for _ in 0..levels {
        query = format!("SELECT ({query}) AS a FROM global_t");
    }

    for snapshot in [Snapshot::Latest, Snapshot::Oldest] {
        let output = check_sql_with_snapshot(&query, vec![], snapshot);
        assert_eq!(output.pattern.matches("SELECT").count(), levels + 1);
    }
}

#[test]
fn nested_subqueries_in_select_without_scan() {
    let levels = 40;
    let mut query = "SELECT 1 AS a".to_string();
    for _ in 0..levels {
        query = format!("SELECT ({query}) AS a");
    }

    for snapshot in [Snapshot::Latest, Snapshot::Oldest] {
        let output = check_sql_with_snapshot(&query, vec![], snapshot);
        assert_eq!(output.pattern.matches("SELECT").count(), levels + 1);
    }
}

/// The text writes a subquery once for the row of its columns, so the walk must not
/// build its body per column.
#[test]
fn nested_subqueries_with_several_columns_keep_the_text_linear() {
    let levels = 40;
    let mut query = "SELECT 1, 2".to_string();
    for _ in 0..levels {
        query = format!("SELECT EXISTS ({query}), 2");
    }
    let pattern = check_sql_with_snapshot(&query, vec![], Snapshot::Oldest).pattern;
    assert_eq!(pattern.matches("exists").count(), levels);
}
