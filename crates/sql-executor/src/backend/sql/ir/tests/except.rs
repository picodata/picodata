use super::*;
use crate::ir::tree::Snapshot;
use insta::assert_yaml_snapshot;

#[test]
fn except1() {
    let query = r#"SELECT "id"
        FROM "test_space"
        WHERE "sysFrom" = 1
        EXCEPT DISTINCT
        SELECT "id"
        FROM "test_space"
        WHERE "FIRST_NAME" = 'a'"#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"test_space\".\"id\" FROM \"test_space\" WHERE \"test_space\".\"sysFrom\" = CAST($1 AS int) EXCEPT SELECT \"test_space\".\"id\" FROM \"test_space\" WHERE \"test_space\".\"FIRST_NAME\" = CAST($2 AS string)"
    params:
      - Integer: 1
      - String: a
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"test_space\".\"id\" FROM \"test_space\" WHERE \"test_space\".\"sysFrom\" = CAST($1 AS int) EXCEPT SELECT \"test_space\".\"id\" FROM \"test_space\" WHERE \"test_space\".\"FIRST_NAME\" = CAST($2 AS string)"
    params:
      - Integer: 1
      - String: a
    "#);
}
