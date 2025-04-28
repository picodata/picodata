use crate::executor::{engine::mock::RouterRuntimeMock, Query};

#[test]
fn test_query_explain_1() {
    let sql = r#"select 1"#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    projection (1::unsigned -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = any
    "#);
}

#[test]
fn test_query_explain_2() {
    let sql = r#"select e from t2"#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    projection ("t2"."e"::unsigned -> "e")
        scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = [1-10000]
    "#);
}

#[test]
fn test_query_explain_3() {
    let sql = r#"select e from t2 where e = 1 and f = 13"#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    projection ("t2"."e"::unsigned -> "e")
        selection (ROW("t2"."e"::unsigned) = ROW(1::unsigned)) and (ROW("t2"."f"::unsigned) = ROW(13::unsigned))
            scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = [111]
    "#);
}

#[test]
fn test_query_explain_4() {
    let sql = r#"select count(*) from t2"#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    projection (sum(("count_1"::unsigned))::unsigned -> "col_1")
        motion [policy: full]
            projection (count((*::integer))::unsigned -> "count_1")
                scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = [1-10000]
    "#);
}

#[test]
fn test_query_explain_5() {
    let sql = r#"select a from global_t"#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    projection ("global_t"."a"::integer -> "a")
        scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = any
    "#);
}

#[test]
fn test_query_explain_6() {
    let sql = r#"insert into t1 values ('1', 1)"#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    insert "t1" on conflict: fail
        motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")])]
            values
                value row (data=ROW('1'::string, 1::unsigned))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = unknown
    "#);
}

#[test]
fn test_query_explain_7() {
    let sql = r#"insert into t1 select a, b from t1"#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    insert "t1" on conflict: fail
        motion [policy: local segment([ref("a"), ref("b")])]
            projection ("t1"."a"::string -> "a", "t1"."b"::integer -> "b")
                scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = [1-10000]
    "#);
}

#[test]
fn test_query_explain_8() {
    let sql = r#"insert into global_t values (1, 1)"#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    insert "global_t" on conflict: fail
        motion [policy: full]
            values
                value row (data=ROW(1::unsigned, 1::unsigned))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = any
    "#);
}

#[test]
fn test_query_explain_9() {
    let sql = r#"delete from t2"#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    delete "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = [1-10000]
    "#);
}

#[test]
fn test_query_explain_10() {
    let sql = r#"update t2 set e = 20 where (e, f) = (10, 10)"#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    update "t2"
    "f" = "col_1"
    "h" = "col_3"
    "e" = "col_0"
    "g" = "col_2"
        motion [policy: segment([])]
            projection (20::unsigned -> "col_0", "t2"."f"::unsigned -> "col_1", "t2"."g"::unsigned -> "col_2", "t2"."h"::unsigned -> "col_3", "t2"."e"::unsigned -> "col_4", "t2"."f"::unsigned -> "col_5")
                selection ROW("t2"."e"::unsigned, "t2"."f"::unsigned) = ROW(10::unsigned, 10::unsigned)
                    scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = unknown
    "#);
}

#[test]
fn test_query_explain_11() {
    // This query contains Segment motion
    // we can't estimate buckets in this case

    let sql = r#"select a, count(b) from
    (select e, f from t2 where (e, f) = (10, 10))
    join
    (select a, b from t1 where (a, b) = ('20', 20))
    on e = b 
    group by a
"#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    projection ("gr_expr_1"::string -> "a", sum(("count_1"::unsigned))::unsigned -> "col_1")
        group by ("gr_expr_1"::string) output: ("gr_expr_1"::string -> "gr_expr_1", "count_1"::unsigned -> "count_1")
            motion [policy: segment([ref("gr_expr_1")])]
                projection ("a"::string -> "gr_expr_1", count(("b"::integer))::unsigned -> "count_1")
                    group by ("a"::string) output: ("e"::unsigned -> "e", "f"::unsigned -> "f", "a"::string -> "a", "b"::integer -> "b")
                        join on ROW("e"::unsigned) = ROW("b"::integer)
                            scan
                                projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f")
                                    selection ROW("t2"."e"::unsigned, "t2"."f"::unsigned) = ROW(10::unsigned, 10::unsigned)
                                        scan "t2"
                            motion [policy: full]
                                scan
                                    projection ("t1"."a"::string -> "a", "t1"."b"::integer -> "b")
                                        selection ROW("t1"."a"::string, "t1"."b"::integer) = ROW('20'::string, 20::unsigned)
                                            scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = unknown
    "#);
}

#[test]
fn test_query_explain_12() {
    // This query does not contain
    // segment motions and we can estimate it!

    let sql = r#"select a from
    (select e, f from t2 where (e, f) = (10, 10))
    join
    (select a, b from t1 where (a, b) = ('20', 20))
    on e = b
"#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    projection ("a"::string -> "a")
        join on ROW("e"::unsigned) = ROW("b"::integer)
            scan
                projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f")
                    selection ROW("t2"."e"::unsigned, "t2"."f"::unsigned) = ROW(10::unsigned, 10::unsigned)
                        scan "t2"
            motion [policy: full]
                scan
                    projection ("t1"."a"::string -> "a", "t1"."b"::integer -> "b")
                        selection ROW("t1"."a"::string, "t1"."b"::integer) = ROW('20'::string, 20::unsigned)
                            scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets <= [62,2132]
    "#);
}

#[test]
fn test_query_explain_13() {
    let sql = r#"insert into global_t select a, b from t1 where (a, b) = ('1', 1)"#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    insert "global_t" on conflict: fail
        motion [policy: full]
            projection ("t1"."a"::string -> "a", "t1"."b"::integer -> "b")
                selection ROW("t1"."a"::string, "t1"."b"::integer) = ROW('1'::string, 1::unsigned)
                    scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets <= [6691]
    "#);
}

#[test]
fn test_query_explain_14() {
    let sql = r#"select a, b from t1 where (a, b) = ('1', 1) and (a, b) = ('2', 2)"#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();
    insta::assert_snapshot!(query.to_explain().unwrap(), @r#"
    projection ("t1"."a"::string -> "a", "t1"."b"::integer -> "b")
        selection (ROW("t1"."a"::string, "t1"."b"::integer) = ROW('1'::string, 1::unsigned)) and (ROW("t1"."a"::string, "t1"."b"::integer) = ROW('2'::string, 2::unsigned))
            scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    buckets = []
    "#);
}
