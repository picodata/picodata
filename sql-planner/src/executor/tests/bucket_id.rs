use super::*;
use crate::executor::engine::mock::{DispatchInfo, PortMocked, RouterRuntimeMock};
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::value::Value;
use pretty_assertions::assert_eq;

#[test]
fn bucket1_test() {
    let sql = r#"SELECT *, "bucket_id" FROM "t1""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let mut port = PortMocked::new();
    query.dispatch(&mut port).unwrap();

    let info = port.decode();
    assert_eq!(1, info.len());
    let DispatchInfo::All(sql, params) = info.get(0).unwrap() else {
        panic!("Expected dispatch on all buckets");
    };
    assert_eq!(
        sql,
        r#"SELECT "t1"."a", "t1"."b", "t1"."bucket_id" FROM "t1""#,
    );
    assert!(params.is_empty());
}

#[test]
fn bucket2_test() {
    let sql = r#"SELECT "a", "bucket_id", "b" FROM "t1"
        WHERE "a" = '1' AND "b" = 2"#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let mut port = PortMocked::new();
    query.dispatch(&mut port).unwrap();

    let info = port.decode();
    assert_eq!(1, info.len());
    let DispatchInfo::Filtered(filtered) = info.get(0).unwrap() else {
        panic!("Expected a single dispatch on a bucket");
    };
    assert_eq!(filtered.len(), 1);
    let (sql, params, _, buckets) = filtered.get(0).unwrap();

    let param1 = Value::from("1");
    let param2 = Value::from(2);
    let bucket = query
        .coordinator
        .determine_bucket_id(&[&param1, &param2])
        .unwrap();

    assert_eq!(
        sql,
        &format!(
            "{} {}",
            r#"SELECT "t1"."a", "t1"."bucket_id", "t1"."b" FROM "t1""#,
            r#"WHERE ("t1"."a" = CAST($1 AS string)) and ("t1"."b" = CAST($2 AS int))"#,
        ),
    );
    assert_eq!(params, &vec![param1, param2]);
    assert_eq!(buckets, &vec![bucket]);
}

#[test]
fn bucket3_test() {
    let sql = r#"SELECT *, trim('111') FROM "t1""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let mut port = PortMocked::new();
    query.dispatch(&mut port).unwrap();
    let info = port.decode();
    assert_eq!(1, info.len());
    let DispatchInfo::All(sql, params) = info.get(0).unwrap() else {
        panic!("Expected dispatch on all buckets");
    };
    assert_eq!(
        sql,
        r#"SELECT "t1"."a", "t1"."b", TRIM (CAST($1 AS string)) as "col_1" FROM "t1""#
    );
    assert_eq!(params, &vec![Value::from("111")]);
}

#[test]
fn bucket_id_from_join() {
    let input = r#"select t1.bucket_id from t t1 join t on true"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t1"."bucket_id"::int -> "bucket_id")
        join on true::bool
            scan "t" -> "t1"
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                    scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn sharding_key_from_tuple1() {
    let coordinator = RouterRuntimeMock::new();
    let tuple = vec![Value::from("123"), Value::from(1)];
    let sharding_key = coordinator
        .extract_sharding_key_from_tuple("t1".into(), &tuple)
        .unwrap();
    assert_eq!(sharding_key, vec![&Value::from("123"), &Value::from(1)]);
}

#[test]
fn explicit_select_bucket_id_from_subquery_under_limit() {
    let input = r#"select * from (
                            select "test_space"."bucket_id" as "bucket_id",
                                   "test_space"."id" as "id"
                            from "test_space"
                        ) x limit 1;"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    limit 1
        motion [policy: full, program: ReshardIfNeeded]
            limit 1
                projection ("x"."bucket_id"::int -> "bucket_id", "x"."id"::int -> "id")
                    scan "x"
                        projection ("test_space"."bucket_id"::int -> "bucket_id", "test_space"."id"::int -> "id")
                            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn explicit_select_bucket_id_from_cte_under_limit() {
    let input = r#"with x as (
                            select "test_space"."bucket_id" as "bucket_id",
                                   "test_space"."id" as "id"
                            from "test_space"
                        )
                        select * from x limit 1;"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    limit 1
        projection ("x"."bucket_id"::int -> "bucket_id", "x"."id"::int -> "id")
            scan cte x($0)
    subquery $0:
    motion [policy: full, program: ReshardIfNeeded]
                    projection ("test_space"."bucket_id"::int -> "bucket_id", "test_space"."id"::int -> "id")
                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn groupby_bucket_id() {
    let input = r#"SELECT * FROM t GROUP BY a, b, c, d, bucket_id"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
        group by ("t"."a"::int, "t"."b"::int, "t"."c"::int, "t"."d"::int, "t"."bucket_id"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
            scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}
