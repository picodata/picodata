use super::*;
use crate::executor::tests::f_sql;
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;

#[test]
fn selection_column_from_values() {
    let query = r#"
        SELECT "COLUMN_1" FROM (VALUES (1))
    "#;

    let expected = PatternWithParams::new(
        r#"SELECT "COLUMN_1" FROM (VALUES (CAST($1 AS unsigned)))"#.to_string(),
        vec![Value::Unsigned(1)],
    );
    check_sql_with_snapshot(query, vec![], expected.clone(), Snapshot::Oldest);
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Latest);
}

#[test]
fn selection1_latest() {
    let query = r#"SELECT "product_code" FROM "hash_testing"
        WHERE "identification_number" in
        (SELECT "identification_number" FROM "hash_testing_hist" WHERE "product_code" = 'b') and "product_code" < 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "hash_testing"."product_code" FROM "hash_testing""#,
            r#"WHERE (("hash_testing"."product_code") < (CAST($1 AS string)))"#,
            r#"and (("hash_testing"."identification_number") in"#,
            r#"(SELECT "hash_testing_hist"."identification_number" FROM "hash_testing_hist""#,
            r#"WHERE ("hash_testing_hist"."product_code") = (CAST($2 AS string))))"#,
        ),
        vec![Value::from("a"), Value::from("b")],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Latest);
}

#[test]
fn selection1_oldest() {
    let query = r#"SELECT "product_code" FROM "hash_testing"
        WHERE "identification_number" in
        (SELECT "identification_number" FROM "hash_testing_hist" WHERE "product_code" = 'b') and "product_code" < 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "hash_testing"."product_code" FROM "hash_testing""#,
            r#"WHERE (("hash_testing"."identification_number") in"#,
            r#"(SELECT "hash_testing_hist"."identification_number" FROM "hash_testing_hist""#,
            r#"WHERE ("hash_testing_hist"."product_code") = (CAST($1 AS string))))"#,
            r#"and (("hash_testing"."product_code") < (CAST($2 AS string)))"#,
        ),
        vec![Value::from("b"), Value::from("a")],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Oldest);
}

#[test]
#[allow(clippy::too_many_lines)]
fn selection2_latest() {
    let query = r#"SELECT "product_code" FROM "hash_testing"
        WHERE "identification_number" IN (1)
        AND "product_units" = true
        AND ("product_units" OR "product_units" IS NULL)"#;

    let expected = PatternWithParams::new(
        f_sql(
            r#"SELECT "hash_testing"."product_code" FROM "hash_testing"
WHERE ((("hash_testing"."product_units", "hash_testing"."identification_number") = (CAST($1 AS boolean), CAST($2 AS unsigned)))
and ("hash_testing"."product_units"))
or ((("hash_testing"."product_units", "hash_testing"."identification_number") = (CAST($1 AS boolean), CAST($2 AS unsigned)))
and (("hash_testing"."product_units") is null))"#,
        ),
        vec![Value::Boolean(true), Value::Unsigned(1)],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Latest);
}

#[test]
fn selection2_oldest() {
    let query = r#"SELECT "product_code" FROM "hash_testing"
        WHERE "identification_number" IN (1)
        AND "product_units" = true
        AND ("product_units" OR "product_units" IS NULL)"#;

    let expected = PatternWithParams::new(
        [
            r#"SELECT "hash_testing"."product_code" FROM "hash_testing""#,
            r#"WHERE ((("hash_testing"."identification_number") in (CAST($1 AS unsigned))) and (("hash_testing"."product_units") = (CAST($2 AS boolean))))"#,
            r#"and (("hash_testing"."product_units") or (("hash_testing"."product_units") is null))"#,
        ].join(" "),
        vec![Value::Unsigned(1), Value::Boolean(true)],
    );
    check_sql_with_snapshot(query, vec![], expected, Snapshot::Oldest);
}
