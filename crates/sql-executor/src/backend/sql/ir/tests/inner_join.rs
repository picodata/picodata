use super::*;
use crate::ir::tree::Snapshot;
use insta::assert_yaml_snapshot;

#[test]
fn inner_join1() {
    let query = r#"SELECT "product_code" FROM "hash_testing" join "history"
        on "hash_testing"."identification_number" = "history"."id"
        WHERE "product_code" = 'a'"#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" INNER JOIN \"history\" ON \"hash_testing\".\"identification_number\" = \"history\".\"id\" WHERE \"hash_testing\".\"product_code\" = CAST($1 AS string)"
    params:
      - String: a
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" INNER JOIN \"history\" ON \"hash_testing\".\"identification_number\" = \"history\".\"id\" WHERE \"hash_testing\".\"product_code\" = CAST($1 AS string)"
    params:
      - String: a
    "#);
}

#[test]
fn inner_join2() {
    let query = r#"SELECT "product_code" FROM "hash_testing" join
        (SELECT * FROM "history" WHERE "id" = 1) as "t"
        on "hash_testing"."identification_number" = "t"."id"
        WHERE "product_code" = 'a'"#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" INNER JOIN (SELECT \"history\".\"id\" FROM \"history\" WHERE \"history\".\"id\" = CAST($1 AS int)) as \"t\" ON \"hash_testing\".\"identification_number\" = \"t\".\"id\" WHERE \"hash_testing\".\"product_code\" = CAST($2 AS string)"
    params:
      - Integer: 1
      - String: a
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" INNER JOIN (SELECT \"history\".\"id\" FROM \"history\" WHERE \"history\".\"id\" = CAST($1 AS int)) as \"t\" ON \"hash_testing\".\"identification_number\" = \"t\".\"id\" WHERE \"hash_testing\".\"product_code\" = CAST($2 AS string)"
    params:
      - Integer: 1
      - String: a
    "#);
}
