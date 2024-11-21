use super::*;
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;

#[test]
fn projection1_latest() {
    let query = r#"SELECT "identification_number", "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {}",
            r#"SELECT "hash_testing"."identification_number","#,
            r#""hash_testing"."product_code""#,
            r#"FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (?)"#,
        ),
        vec![Value::from(1_u64)],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Latest);
}

#[test]
fn projection1_oldest() {
    let query = r#"SELECT "identification_number", "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {}",
            r#"SELECT "hash_testing"."identification_number","#,
            r#""hash_testing"."product_code""#,
            r#"FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (?)"#,
        ),
        vec![Value::from(1_u64)],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Oldest);
}

#[test]
fn projection2_latest() {
    let query = r#"SELECT *
        FROM "hash_testing"
        WHERE "identification_number" = 1"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "hash_testing"."identification_number","#,
            r#""hash_testing"."product_code","#,
            r#""hash_testing"."product_units", "hash_testing"."sys_op""#,
            r#"FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (?)"#
        ),
        vec![Value::from(1_u64)],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Latest);
}

#[test]
fn projection2_oldest() {
    let query = r#"SELECT *
        FROM "hash_testing"
        WHERE "identification_number" = 1"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "hash_testing"."identification_number","#,
            r#""hash_testing"."product_code","#,
            r#""hash_testing"."product_units", "hash_testing"."sys_op""#,
            r#"FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (?)"#
        ),
        vec![Value::from(1_u64)],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Oldest);
}

#[test]
fn select_without_scan() {
    let query = r#"SELECT 1 as foo, (values (1)), (select a from global_t)"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {} {}",
            r#"SELECT ? as "foo","#,
            r#"(VALUES (?)) as "col_1","#,
            r#"(SELECT "global_t"."a" FROM "global_t") as "col_2""#,
        ),
        vec![Value::from(1_u64), Value::from(1_u64)],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Oldest);
}
