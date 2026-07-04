use super::*;
use crate::ir::tree::Snapshot;
use insta::assert_yaml_snapshot;

#[test]
fn selection_column_from_values() {
    let query = r#"
        SELECT "COLUMN_1" FROM (VALUES (1))
    "#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"unnamed_subquery\".\"COLUMN_1\" FROM (VALUES (CAST($1 AS int))) as \"unnamed_subquery\""
    params:
      - Integer: 1
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"unnamed_subquery\".\"COLUMN_1\" FROM (VALUES (CAST($1 AS int))) as \"unnamed_subquery\""
    params:
      - Integer: 1
    "#);
}

#[test]
fn selection1() {
    let query = r#"SELECT "product_code" FROM "hash_testing"
        WHERE "identification_number" in
        (SELECT "identification_number" FROM "hash_testing_hist" WHERE "product_code" = 'b') and "product_code" < 'a'"#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"product_code\" < CAST($1 AS string) and \"hash_testing\".\"identification_number\" in (SELECT \"hash_testing_hist\".\"identification_number\" FROM \"hash_testing_hist\" WHERE \"hash_testing_hist\".\"product_code\" = CAST($2 AS string))"
    params:
      - String: a
      - String: b
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" in (SELECT \"hash_testing_hist\".\"identification_number\" FROM \"hash_testing_hist\" WHERE \"hash_testing_hist\".\"product_code\" = CAST($1 AS string)) and \"hash_testing\".\"product_code\" < CAST($2 AS string)"
    params:
      - String: b
      - String: a
    "#);
}

#[test]
fn selection2() {
    let query = r#"SELECT "product_code" FROM "hash_testing"
        WHERE "identification_number" IN (1)
        AND "product_units" = true
        AND ("product_units" OR "product_units" IS NULL)"#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE (\"hash_testing\".\"product_units\", \"hash_testing\".\"identification_number\") = (CAST($1 AS bool), CAST($2 AS int)) and \"hash_testing\".\"product_units\" or (\"hash_testing\".\"product_units\", \"hash_testing\".\"identification_number\") = (CAST($1 AS bool), CAST($2 AS int)) and \"hash_testing\".\"product_units\" is null"
    params:
      - Boolean: true
      - Integer: 1
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" in (CAST($1 AS int)) and \"hash_testing\".\"product_units\" = CAST($2 AS bool) and (\"hash_testing\".\"product_units\" or \"hash_testing\".\"product_units\" is null)"
    params:
      - Integer: 1
      - Boolean: true
    "#);
}
