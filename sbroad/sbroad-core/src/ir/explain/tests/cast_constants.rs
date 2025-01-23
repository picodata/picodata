use crate::executor::{engine::mock::RouterRuntimeMock, Query};
use pretty_assertions::assert_eq;

fn assert_expain_matches(sql: &str, expected_explain: &str) {
    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    let actual_explain = query.to_explain().unwrap();
    assert_eq!(actual_explain.as_str(), expected_explain);
}

#[test]
fn select_values_rows() {
    assert_expain_matches(
        "SELECT * FROM (VALUES (1::int, 2::decimal::unsigned, 'txt'::text::text::text))",
        r#"projection ("COLUMN_1"::integer -> "COLUMN_1", "COLUMN_2"::unsigned -> "COLUMN_2", "COLUMN_3"::string -> "COLUMN_3")
    scan
        values
            value row (data=ROW(1::integer, 2::unsigned, 'txt'::string))
execution options:
    sql_vdbe_opcode_max = 45000
    vtable_max_rows = 5000
buckets = any
"#,
    );
}

#[test]
fn insert_values_rows() {
    assert_expain_matches(
        "INSERT INTO t1 VALUES ('txt'::text::text::text, 2::decimal::unsigned::double::integer)",
        r#"insert "t1" on conflict: fail
    motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")])]
        values
            value row (data=ROW('txt'::string, 2::integer))
execution options:
    sql_vdbe_opcode_max = 45000
    vtable_max_rows = 5000
buckets = unknown
"#,
    );
}

#[test]
fn select_selection() {
    assert_expain_matches(
        "SELECT * FROM t3 WHERE a = 'kek'::text::text::text",
        r#"projection ("t3"."a"::string -> "a", "t3"."b"::integer -> "b")
    selection ROW("t3"."a"::string) = ROW('kek'::string)
        scan "t3"
execution options:
    sql_vdbe_opcode_max = 45000
    vtable_max_rows = 5000
buckets = [1610]
"#,
    );
}

#[test]
fn update_selection() {
    assert_expain_matches(
        "UPDATE t SET c = 2 WHERE a = 1::int::int and b = 2::unsigned::decimal",
        r#"update "t"
"c" = "col_0"
    motion [policy: local]
        projection (2::unsigned -> "col_0", "t"."b"::unsigned -> "col_1")
            selection ROW("t"."a"::unsigned) = ROW(1::integer) and ROW("t"."b"::unsigned) = ROW(2::decimal)
                scan "t"
execution options:
    sql_vdbe_opcode_max = 45000
    vtable_max_rows = 5000
buckets = [550]
"#,
    );
}

#[test]
fn delete_selection() {
    assert_expain_matches(
        r#"DELETE FROM "t2" where "e" = 3::unsigned and "f" = 2::decimal"#,
        r#"delete "t2"
    motion [policy: local]
        projection ("t2"."g"::unsigned -> "pk_col_0", "t2"."h"::unsigned -> "pk_col_1")
            selection ROW("t2"."e"::unsigned) = ROW(3::unsigned) and ROW("t2"."f"::unsigned) = ROW(2::decimal)
                scan "t2"
execution options:
    sql_vdbe_opcode_max = 45000
    vtable_max_rows = 5000
buckets = [9374]
"#,
    );
}
