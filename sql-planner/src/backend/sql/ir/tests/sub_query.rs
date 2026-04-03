use super::*;
use crate::ir::tree::Snapshot;
use insta::assert_yaml_snapshot;

#[test]
fn sub_query1() {
    let query = r#"SELECT "product_code"
        FROM (SELECT "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1) as t1
        WHERE "product_code" = 'a'"#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"t1\".\"product_code\" FROM (SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" = CAST($1 AS int)) as \"t1\" WHERE \"t1\".\"product_code\" = CAST($2 AS string)"
    params:
      - Integer: 1
      - String: a
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"t1\".\"product_code\" FROM (SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" = CAST($1 AS int)) as \"t1\" WHERE \"t1\".\"product_code\" = CAST($2 AS string)"
    params:
      - Integer: 1
      - String: a
    "#);
}

#[test]
fn sub_query2() {
    let query = r#"SELECT "product_code"
        FROM (SELECT "product_code"
            FROM "hash_testing"
            WHERE "identification_number" = 1
            UNION ALL
            SELECT "product_code"
            FROM "hash_testing_hist"
            WHERE "product_code" = 'a') as "t1"
        WHERE "product_code" = 'a'"#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"t1\".\"product_code\" FROM (SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" = CAST($1 AS int) UNION ALL SELECT \"hash_testing_hist\".\"product_code\" FROM \"hash_testing_hist\" WHERE \"hash_testing_hist\".\"product_code\" = CAST($2 AS string)) as \"t1\" WHERE \"t1\".\"product_code\" = CAST($3 AS string)"
    params:
      - Integer: 1
      - String: a
      - String: a
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"t1\".\"product_code\" FROM (SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" = CAST($1 AS int) UNION ALL SELECT \"hash_testing_hist\".\"product_code\" FROM \"hash_testing_hist\" WHERE \"hash_testing_hist\".\"product_code\" = CAST($2 AS string)) as \"t1\" WHERE \"t1\".\"product_code\" = CAST($3 AS string)"
    params:
      - Integer: 1
      - String: a
      - String: a
    "#);
}

#[test]
fn sub_query_exists() {
    let query =
        r#"SELECT "FIRST_NAME" FROM "test_space" WHERE EXISTS (SELECT 0 FROM "hash_testing")"#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"test_space\".\"FIRST_NAME\" FROM \"test_space\" WHERE exists (SELECT CAST($1 AS int) as \"col_1\" FROM \"hash_testing\")"
    params:
      - Integer: 0
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"test_space\".\"FIRST_NAME\" FROM \"test_space\" WHERE exists (SELECT CAST($1 AS int) as \"col_1\" FROM \"hash_testing\")"
    params:
      - Integer: 0
    "#);
}
