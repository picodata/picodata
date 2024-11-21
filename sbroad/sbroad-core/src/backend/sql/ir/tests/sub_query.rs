use super::*;
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;

#[test]
fn sub_query1_latest() {
    let query = r#"SELECT "product_code"
        FROM (SELECT "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1) as t1
        WHERE "product_code" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {}",
            r#"SELECT "t1"."product_code" FROM"#,
            r#"(SELECT "hash_testing"."product_code" FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (?)) as "t1""#,
            r#"WHERE ("t1"."product_code") = (?)"#
        ),
        vec![Value::from(1_u64), Value::from("a")],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Latest);
}

#[test]
fn sub_query1_oldest() {
    let query = r#"SELECT "product_code"
        FROM (SELECT "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1) as "T1"
        WHERE "product_code" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {}",
            r#"SELECT "T1"."product_code" FROM"#,
            r#"(SELECT "hash_testing"."product_code" FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (?)) as "T1""#,
            r#"WHERE ("T1"."product_code") = (?)"#
        ),
        vec![Value::from(1_u64), Value::from("a")],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Oldest);
}

#[test]
fn sub_query2_latest() {
    let query = r#"SELECT "product_code"
        FROM (SELECT "product_code"
            FROM "hash_testing"
            WHERE "identification_number" = 1
            UNION ALL
            SELECT "product_code"
            FROM "hash_testing_hist"
            WHERE "product_code" = 'a') as "t1"
        WHERE "product_code" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {} {} {}",
            r#"SELECT "t1"."product_code" FROM"#,
            r#"(SELECT "hash_testing"."product_code" FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (?)"#,
            r#"UNION ALL"#,
            r#"SELECT "hash_testing_hist"."product_code" FROM "hash_testing_hist""#,
            r#"WHERE ("hash_testing_hist"."product_code") = (?)) as "t1""#,
            r#"WHERE ("t1"."product_code") = (?)"#,
        ),
        vec![Value::from(1_u64), Value::from("a"), Value::from("a")],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Latest);
}

#[test]
fn sub_query2_oldest() {
    let query = r#"SELECT "product_code"
        FROM (SELECT "product_code"
            FROM "hash_testing"
            WHERE "identification_number" = 1
            UNION ALL
            SELECT "product_code"
            FROM "hash_testing_hist"
            WHERE "product_code" = 'a') as "t1"
        WHERE "product_code" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {} {} {}",
            r#"SELECT "t1"."product_code" FROM"#,
            r#"(SELECT "hash_testing"."product_code" FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (?)"#,
            r#"UNION ALL"#,
            r#"SELECT "hash_testing_hist"."product_code" FROM "hash_testing_hist""#,
            r#"WHERE ("hash_testing_hist"."product_code") = (?)) as "t1""#,
            r#"WHERE ("t1"."product_code") = (?)"#,
        ),
        vec![Value::from(1_u64), Value::from("a"), Value::from("a")],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Oldest);
}

#[test]
fn sub_query3_latest() {
    let query = r#"SELECT "a", "b" FROM "t"
    WHERE "a" <= ? and "b" >= ? and "b" <= ?
    AND ("a", "b") NOT IN (
        SELECT "a", "b" FROM "t"
        WHERE "b" >= ?
        UNION ALL
        SELECT "a", "b" FROM "t1"
        WHERE "a" <= ?
    )"#;
    let params = vec![
        Value::from(1_u64),
        Value::from(2_u64),
        Value::from(3_u64),
        Value::from(4_u64),
        Value::from(5_u64),
        Value::from(6_u64),
    ];

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {} {}",
            r#"SELECT "t"."a", "t"."b" FROM "t""#,
            r#"WHERE ("t"."b") <= (?)"#,
            r#"and not ("t"."a", "t"."b") in"#,
            r#"(SELECT "t"."a", "t"."b" FROM "t" WHERE ("t"."b") >= (?)"#,
            r#"UNION ALL SELECT "t1"."a", "t1"."b" FROM "t1" WHERE ("t1"."a") <= (?))"#,
            r#"and ("t"."a") <= (?) and ("t"."b") >= (?)"#
        ),
        vec![
            Value::from(3_u64),
            Value::from(4_u64),
            Value::from(5_u64),
            Value::from(1_u64),
            Value::from(2_u64),
        ],
    );
    check_sql_with_snapshot(query, params, expected, Snapshot::Latest);
}

#[test]
fn sub_query_exists() {
    let query =
        r#"SELECT "FIRST_NAME" FROM "test_space" WHERE EXISTS (SELECT 0 FROM "hash_testing")"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "test_space"."FIRST_NAME" FROM "test_space""#,
            r#"WHERE exists (SELECT ? as "col_1" FROM "hash_testing")"#
        ),
        vec![Value::from(0_u64)],
    );
    check_sql_with_snapshot(query, vec![], expected.clone(), Snapshot::Oldest);
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Oldest);
}
