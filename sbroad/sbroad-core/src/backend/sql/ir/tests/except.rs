use super::*;
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;

#[test]
fn except1_latest() {
    let query = r#"SELECT "id"
        FROM "test_space"
        WHERE "sysFrom" = 1
        EXCEPT DISTINCT
        SELECT "id"
        FROM "test_space"
        WHERE "FIRST_NAME" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "test_space"."id" FROM "test_space""#,
            r#"WHERE ("test_space"."sysFrom") = (?)"#,
            r#"EXCEPT"#,
            r#"SELECT "test_space"."id" FROM "test_space""#,
            r#"WHERE ("test_space"."FIRST_NAME") = (?)"#
        ),
        vec![Value::from(1_u64), Value::from("a")],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Latest);
}

#[test]
fn except1_oldest() {
    let query = r#"SELECT "id"
        FROM "test_space"
        WHERE "sysFrom" = 1
        EXCEPT DISTINCT
        SELECT "id"
        FROM "test_space"
        WHERE "FIRST_NAME" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "test_space"."id" FROM "test_space""#,
            r#"WHERE ("test_space"."sysFrom") = (?)"#,
            r#"EXCEPT"#,
            r#"SELECT "test_space"."id" FROM "test_space""#,
            r#"WHERE ("test_space"."FIRST_NAME") = (?)"#
        ),
        vec![Value::from(1_u64), Value::from("a")],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Oldest);
}
