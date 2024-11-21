use super::*;
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;

#[test]
fn inner_join1_latest() {
    let query = r#"SELECT "product_code" FROM "hash_testing" join "history"
        on "hash_testing"."identification_number" = "history"."id"
        WHERE "product_code" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {} {} {} {}",
            r#"SELECT "hash_testing"."product_code""#,
            r#"FROM (SELECT "hash_testing"."identification_number","#,
            r#""hash_testing"."product_code","#,
            r#""hash_testing"."product_units","#,
            r#""hash_testing"."sys_op" FROM "hash_testing") as "hash_testing""#,
            r#"INNER JOIN (SELECT "history"."id" FROM "history") as "history""#,
            r#"ON ("hash_testing"."identification_number") = ("history"."id")"#,
            r#"WHERE ("hash_testing"."product_code") = (?)"#,
        ),
        vec![Value::from("a")],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Latest);
}

#[test]
fn inner_join1_oldest() {
    let query = r#"SELECT "product_code" FROM "hash_testing" join "history"
        on "hash_testing"."identification_number" = "history"."id"
        WHERE "product_code" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {} {} {} {}",
            r#"SELECT "hash_testing"."product_code""#,
            r#"FROM (SELECT "hash_testing"."identification_number","#,
            r#""hash_testing"."product_code","#,
            r#""hash_testing"."product_units","#,
            r#""hash_testing"."sys_op" FROM "hash_testing") as "hash_testing""#,
            r#"INNER JOIN (SELECT "history"."id" FROM "history") as "history""#,
            r#"ON ("hash_testing"."identification_number") = ("history"."id")"#,
            r#"WHERE ("hash_testing"."product_code") = (?)"#,
        ),
        vec![Value::from("a")],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Oldest);
}

#[test]
fn inner_join2_latest() {
    let query = r#"SELECT "product_code" FROM "hash_testing" join
        (SELECT * FROM "history" WHERE "id" = 1) as "t"
        on "hash_testing"."identification_number" = "t"."id"
        WHERE "product_code" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {} {} {} {} {}",
            r#"SELECT "hash_testing"."product_code" FROM (SELECT"#,
            r#""hash_testing"."identification_number","#,
            r#""hash_testing"."product_code","#,
            r#""hash_testing"."product_units","#,
            r#""hash_testing"."sys_op" FROM "hash_testing") as "hash_testing""#,
            r#"INNER JOIN"#,
            r#"(SELECT "history"."id" FROM "history" WHERE ("history"."id") = (?)) as "t""#,
            r#"ON ("hash_testing"."identification_number") = ("t"."id")"#,
            r#"WHERE ("hash_testing"."product_code") = (?)"#,
        ),
        vec![Value::from(1_u64), Value::from("a")],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Latest);
}

#[test]
fn inner_join2_oldest() {
    let query = r#"SELECT "product_code" FROM "hash_testing" join
        (SELECT * FROM "history" WHERE "id" = 1) as "t"
        on "hash_testing"."identification_number" = "t"."id"
        WHERE "product_code" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {} {} {} {} {}",
            r#"SELECT "hash_testing"."product_code" FROM (SELECT"#,
            r#""hash_testing"."identification_number","#,
            r#""hash_testing"."product_code","#,
            r#""hash_testing"."product_units","#,
            r#""hash_testing"."sys_op" FROM "hash_testing") as "hash_testing""#,
            r#"INNER JOIN"#,
            r#"(SELECT "history"."id" FROM "history" WHERE ("history"."id") = (?)) as "t""#,
            r#"ON ("hash_testing"."identification_number") = ("t"."id")"#,
            r#"WHERE ("hash_testing"."product_code") = (?)"#,
        ),
        vec![Value::from(1_u64), Value::from("a")],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Oldest);
}
