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
    pattern: "SELECT \"hash_testing\".\"identification_number\", \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" = CAST($1 AS int)"
    params:
      - Integer: 1
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"identification_number\", \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" = CAST($1 AS int)"
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
    pattern: "SELECT \"hash_testing\".\"identification_number\", \"hash_testing\".\"product_code\", \"hash_testing\".\"product_units\", \"hash_testing\".\"sys_op\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" = CAST($1 AS int)"
    params:
      - Integer: 1
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"identification_number\", \"hash_testing\".\"product_code\", \"hash_testing\".\"product_units\", \"hash_testing\".\"sys_op\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" = CAST($1 AS int)"
    params:
      - Integer: 1
    "#);
}

#[test]
fn select_without_scan() {
    let query = r#"SELECT 1 as foo, (values (1)), (select a from global_t)"#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT CAST($1 AS int) as \"foo\", (VALUES (CAST($2 AS int))) as \"col_1\", (SELECT \"global_t\".\"a\" FROM \"global_t\") as \"col_2\""
    params:
      - Integer: 1
      - Integer: 1
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT CAST($1 AS int) as \"foo\", (VALUES (CAST($2 AS int))) as \"col_1\", (SELECT \"global_t\".\"a\" FROM \"global_t\") as \"col_2\""
    params:
      - Integer: 1
      - Integer: 1
    "#);
}
