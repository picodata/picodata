use super::*;
use crate::ir::tree::Snapshot;
use insta::assert_yaml_snapshot;

#[test]
fn union_all1() {
    let query = r#"SELECT "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1
        UNION ALL
        SELECT "product_code"
        FROM "hash_testing_hist"
        WHERE "product_code" = 'a'"#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" = CAST($1 AS int) UNION ALL SELECT \"hash_testing_hist\".\"product_code\" FROM \"hash_testing_hist\" WHERE \"hash_testing_hist\".\"product_code\" = CAST($2 AS string)"
    params:
      - Integer: 1
      - String: a
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" = CAST($1 AS int) UNION ALL SELECT \"hash_testing_hist\".\"product_code\" FROM \"hash_testing_hist\" WHERE \"hash_testing_hist\".\"product_code\" = CAST($2 AS string)"
    params:
      - Integer: 1
      - String: a
    "#);
}
