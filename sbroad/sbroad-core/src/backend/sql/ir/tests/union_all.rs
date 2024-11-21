use super::*;
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;

#[test]
fn union_all1_latest() {
    let query = r#"SELECT "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1
        UNION ALL
        SELECT "product_code"
        FROM "hash_testing_hist"
        WHERE "product_code" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "hash_testing"."product_code" FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (?)"#,
            r#"UNION ALL"#,
            r#"SELECT "hash_testing_hist"."product_code" FROM "hash_testing_hist""#,
            r#"WHERE ("hash_testing_hist"."product_code") = (?)"#
        ),
        vec![Value::from(1_u64), Value::from("a")],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Latest);
}

#[test]
fn union_all1_oldest() {
    let query = r#"SELECT "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1
        UNION ALL
        SELECT "product_code"
        FROM "hash_testing_hist"
        WHERE "product_code" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "hash_testing"."product_code" FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (?)"#,
            r#"UNION ALL"#,
            r#"SELECT "hash_testing_hist"."product_code" FROM "hash_testing_hist""#,
            r#"WHERE ("hash_testing_hist"."product_code") = (?)"#
        ),
        vec![Value::from(1_u64), Value::from("a")],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Oldest);
}
