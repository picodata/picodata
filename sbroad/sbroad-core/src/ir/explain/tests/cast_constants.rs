use crate::executor::{engine::mock::RouterRuntimeMock, Query};

#[test]
fn select_values_rows() {
    let sql = r#"SELECT * FROM (VALUES (1::int, 2::decimal::integer, 'txt'::text::text::text))"#;
    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    projection ("unnamed_subquery"."COLUMN_1"::int -> "COLUMN_1", "unnamed_subquery"."COLUMN_2"::int -> "COLUMN_2", "unnamed_subquery"."COLUMN_3"::string -> "COLUMN_3")
        scan "unnamed_subquery"
            values
                value row (data=ROW(1::int, 2::int, 'txt'::string))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = any
    "#);
}

#[test]
fn insert_values_rows() {
    let sql =
        r#"INSERT INTO t1 VALUES ('txt'::text::text::text, 2::decimal::integer::double::integer)"#;
    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    insert "t1" on conflict: fail
        motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")])]
            values
                value row (data=ROW('txt'::string, 2::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = unknown
    "#);
}

#[test]
fn select_selection() {
    let sql = r#"SELECT * FROM t3 WHERE a = 'kek'::text::text::text"#;
    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    projection ("t3"."a"::string -> "a", "t3"."b"::int -> "b")
        selection "t3"."a"::string = 'kek'::string
            scan "t3"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = [1610]
    "#);
}

#[test]
fn update_selection() {
    let sql = r#"UPDATE t SET c = 2 WHERE a = 1::int::int and b = 2::integer::decimal"#;
    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    update "t"
    "c" = "col_0"
        motion [policy: local]
            projection (2::int -> "col_0", "t"."b"::int -> "col_1")
                selection ("t"."a"::int = 1::int) and ("t"."b"::int = 2::decimal)
                    scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = [550]
    "#);
}

#[test]
fn delete_selection() {
    let sql = r#"DELETE FROM "t2" where "e" = 3::integer and "f" = 2::decimal"#;
    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    delete "t2"
        motion [policy: local]
            projection ("t2"."g"::int -> "pk_col_0", "t2"."h"::int -> "pk_col_1")
                selection ("t2"."e"::int = 3::int) and ("t2"."f"::int = 2::decimal)
                    scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = [9374]
    "#);
}
