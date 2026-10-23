use super::*;
use crate::ir::tree::Snapshot;
use insta::assert_yaml_snapshot;

#[test]
fn selection_column_from_values() {
    let query = r#"
        SELECT "COLUMN_1" FROM (VALUES (1))
    "#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"unnamed_subquery\".\"COLUMN_1\" FROM (VALUES ($1::int)) as \"unnamed_subquery\""
    params:
      - Integer: 1
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"unnamed_subquery\".\"COLUMN_1\" FROM (VALUES ($1::int)) as \"unnamed_subquery\""
    params:
      - Integer: 1
    "#);
}

#[test]
fn selection1() {
    let query = r#"SELECT "product_code" FROM "hash_testing"
        WHERE "identification_number" in
        (SELECT "identification_number" FROM "hash_testing_hist" WHERE "product_code" = 'b') and "product_code" < 'a'"#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"product_code\" < $1::text and \"hash_testing\".\"identification_number\" in (SELECT \"hash_testing_hist\".\"identification_number\" FROM \"hash_testing_hist\" WHERE \"hash_testing_hist\".\"product_code\" = $2::text)"
    params:
      - String: a
      - String: b
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" in (SELECT \"hash_testing_hist\".\"identification_number\" FROM \"hash_testing_hist\" WHERE \"hash_testing_hist\".\"product_code\" = $1::text) and \"hash_testing\".\"product_code\" < $2::text"
    params:
      - String: b
      - String: a
    "#);
}

#[test]
fn selection2() {
    let query = r#"SELECT "product_code" FROM "hash_testing"
        WHERE "identification_number" IN (1)
        AND "product_units" = true
        AND ("product_units" OR "product_units" IS NULL)"#;

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Latest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE (\"hash_testing\".\"product_units\", \"hash_testing\".\"identification_number\") = ($1::bool, $2::int) and \"hash_testing\".\"product_units\" or (\"hash_testing\".\"product_units\", \"hash_testing\".\"identification_number\") = ($1::bool, $2::int) and \"hash_testing\".\"product_units\" is null"
    params:
      - Boolean: true
      - Integer: 1
    "#);

    let output = check_sql_with_snapshot(query, vec![], Snapshot::Oldest);
    assert_yaml_snapshot!(output, @r#"
    pattern: "SELECT \"hash_testing\".\"product_code\" FROM \"hash_testing\" WHERE \"hash_testing\".\"identification_number\" in ($1::int) and \"hash_testing\".\"product_units\" = $2::bool and (\"hash_testing\".\"product_units\" or \"hash_testing\".\"product_units\" is null)"
    params:
      - Integer: 1
      - Boolean: true
    "#);
}

#[cfg(feature = "enrich_restrictions")]
#[test]
fn selection_long_in_list_survives_rebuilt_condition() {
    // A moved or rebuilt condition must still render the `IN`: its 256-term
    // `OR` expansion exceeds the expression tree limit of the local engine.
    let list = (0..256)
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    // Folding cuts `l.b` out of this expansion, so its `IN` is rebuilt.
    let folded = (0..256)
        .map(|i| format!("r.a > {i}"))
        .collect::<Vec<_>>()
        .join(", ");
    // Folding turns `true = (a > i)` into `a > i`: no `IN` can be rebuilt from
    // that chain, only the logged one serves.
    let unrebuildable = (0..256)
        .map(|i| format!("a > {i}"))
        .collect::<Vec<_>>()
        .join(", ");
    for query in [
        format!("SELECT a FROM global_t WHERE b = 1 AND a IN ({list})"),
        format!("SELECT l.a FROM global_t l JOIN global_t r ON l.a = r.a WHERE l.b IN ({list})"),
        format!("SELECT l.a FROM global_t l JOIN global_t r ON true WHERE (l.b > 0 AND false) IN ({folded})"),
        format!("SELECT a FROM global_t WHERE b = 1 AND true IN ({unrebuildable})"),
    ] {
        let output = check_sql_with_snapshot(&query, vec![], Snapshot::Oldest);
        assert!(output.pattern.contains(" in ("), "{}", output.pattern);
        assert!(!output.pattern.contains(" or "), "{}", output.pattern);
    }
}

#[cfg(feature = "enrich_restrictions")]
#[test]
fn selection_tuple_inequality_survives_rebuilt_condition() {
    // Only `=` splits into an AND of columns: `a > 1 and b > 2` would drop
    // the row (2, 0) that `(a, b) > (1, 2)` keeps.
    for op in [">", ">=", "<", "<=", "<>"] {
        for query in [
            format!("SELECT a FROM global_t WHERE a = 2 AND (a, b) {op} (1, 2)"),
            format!("SELECT s.a FROM (SELECT a, b FROM global_t WHERE a > 0) s WHERE (s.a, s.b) {op} (1, 2)"),
        ] {
            let output = check_sql_with_snapshot(&query, vec![], Snapshot::Oldest);
            assert!(
                output.pattern.contains(&format!("\"b\") {op} (")),
                "{}",
                output.pattern
            );
        }
    }
}
