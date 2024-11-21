use pretty_assertions::assert_eq;
use std::collections::HashSet;

use crate::collection;
use crate::executor::bucket::Buckets;
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::executor::engine::Vshard;
use crate::executor::Query;
use crate::ir::helpers::RepeatableState;
use crate::ir::value::Value;

#[test]
#[allow(clippy::similar_names)]
fn simple_union_query() {
    let query = r#"SELECT * FROM (
    SELECT * FROM "test_space" WHERE "sysFrom" > 0
    UNION ALL
    SELECT * FROM "test_space_hist" WHERE "sysFrom" < 0
    ) as "t3"
    WHERE "id" = 1"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::from(1_u64);

    let bucket1 = query.coordinator.determine_bucket_id(&[&param1]).unwrap();
    let bucket_set: HashSet<u64, RepeatableState> = vec![bucket1].into_iter().collect();
    let expected = Buckets::new_filtered(bucket_set);

    assert_eq!(expected, buckets);
}

#[test]
#[allow(clippy::similar_names)]
fn simple_disjunction_in_union_query() {
    let query = r#"SELECT * FROM (
    SELECT * FROM "test_space" WHERE "sysFrom" > 0
    UNION ALL
    SELECT * FROM "test_space_hist" WHERE "sysFrom" < 0
    ) as "t3"
    WHERE ("id" = 1) OR ("id" = 100)"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::from(1_u64);
    let bucket1 = query.coordinator.determine_bucket_id(&[&param1]).unwrap();

    let param100 = Value::from(100_u64);
    let bucket100 = query.coordinator.determine_bucket_id(&[&param100]).unwrap();

    let bucket_set: HashSet<u64, RepeatableState> = vec![bucket1, bucket100].into_iter().collect();
    let expected = Buckets::new_filtered(bucket_set);

    assert_eq!(expected, buckets);
}

#[test]
fn complex_shard_key_union_query() {
    let query = r#"SELECT *
    FROM
        (SELECT "identification_number", "product_code"
        FROM "hash_testing"
        WHERE "sys_op" = 1
        UNION ALL
        SELECT "identification_number", "product_code"
        FROM "hash_testing_hist"
        WHERE "sys_op" > 1) AS "t3"
    WHERE "identification_number" = 1 AND "product_code" = '222'"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::from(1_u64);
    let param222 = Value::from("222");

    let bucket = query
        .coordinator
        .determine_bucket_id(&[&param1, &param222])
        .unwrap();
    let bucket_set: HashSet<u64, RepeatableState> = vec![bucket].into_iter().collect();
    let expected = Buckets::new_filtered(bucket_set);

    assert_eq!(expected, buckets);
}

#[test]
#[allow(clippy::similar_names)]
fn union_complex_cond_query() {
    let query = r#"SELECT *
    FROM
        (SELECT "identification_number", "product_code"
        FROM "hash_testing"
        WHERE "sys_op" = 1
        UNION ALL
        SELECT "identification_number", "product_code"
        FROM "hash_testing_hist"
        WHERE "sys_op" > 1) AS "t3"
    WHERE ("identification_number" = 1
        OR ("identification_number" = 100
        OR "identification_number" = 1000))
        AND ("product_code" = '222'
        OR "product_code" = '111')"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::from(1_u64);
    let param100 = Value::from(100_u64);
    let param1000 = Value::from(1000_u64);
    let param222 = Value::from("222");
    let param111 = Value::from("111");

    let bucket1222 = query
        .coordinator
        .determine_bucket_id(&[&param1, &param222])
        .unwrap();
    let bucket100222 = query
        .coordinator
        .determine_bucket_id(&[&param100, &param222])
        .unwrap();
    let bucket1000222 = query
        .coordinator
        .determine_bucket_id(&[&param1000, &param222])
        .unwrap();

    let bucket1111 = query
        .coordinator
        .determine_bucket_id(&[&param1, &param111])
        .unwrap();
    let bucket100111 = query
        .coordinator
        .determine_bucket_id(&[&param100, &param111])
        .unwrap();
    let bucket1000111 = query
        .coordinator
        .determine_bucket_id(&[&param1000, &param111])
        .unwrap();

    let bucket_set: HashSet<u64, RepeatableState> = vec![
        bucket1222,
        bucket100222,
        bucket1000222,
        bucket1111,
        bucket100111,
        bucket1000111,
    ]
    .into_iter()
    .collect();

    let expected = Buckets::new_filtered(bucket_set);

    assert_eq!(expected, buckets);
}

#[test]
#[allow(clippy::similar_names)]
fn union_query_conjunction() {
    let query = r#"SELECT * FROM "test_space" WHERE "id" = 1
    UNION ALL
    SELECT * FROM "test_space_hist" WHERE "id" = 2"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::from(1_u64);
    let bucket1 = query.coordinator.determine_bucket_id(&[&param1]).unwrap();

    let param2 = Value::from(2_u64);
    let bucket2 = query.coordinator.determine_bucket_id(&[&param2]).unwrap();
    let bucket_set: HashSet<u64, RepeatableState> = vec![bucket1, bucket2].into_iter().collect();
    let expected = Buckets::new_filtered(bucket_set);

    assert_eq!(expected, buckets);
}

#[test]
#[allow(clippy::similar_names)]
fn simple_except_query() {
    let query = r#"SELECT * FROM (
    SELECT * FROM "test_space" WHERE "sysFrom" > 0
    EXCEPT
    SELECT * FROM "test_space_hist" WHERE "sysFrom" < 0
    ) as "t3"
    WHERE "id" = 1"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::from(1_u64);
    let bucket1 = query.coordinator.determine_bucket_id(&[&param1]).unwrap();
    let bucket_set: HashSet<u64, RepeatableState> = vec![bucket1].into_iter().collect();
    let expected = Buckets::new_filtered(bucket_set);

    assert_eq!(expected, buckets);
}

#[test]
fn global_tbl_selection() {
    let query = r#"
    select * from "global_t"
    where "a" = 1 or "b" = 2"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Any, buckets);
}

#[test]
fn global_tbl_scan() {
    let query = r#"
    select * from "global_t""#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Any, buckets);
}

#[test]
fn global_tbl_sq1() {
    // first sq will have motion(full)
    // second sq will have motion(full)
    // from map aggregation stage.
    let query = r#"
    select * from "global_t"
    where "a" in (select "a" as a1 from "t") or
    "a" in (select sum("a") from "t")
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Any, buckets);
}

#[test]
fn global_tbl_sq2() {
    // first sq will have no motion
    // second sq will have motion(full)
    // from map aggregation stage.
    let query = r#"
    select * from "global_t"
    where ("a", "b") in (select "a" as a1, "b" as b1 from "t") and
    "a" in (select sum("a") from "t")
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn global_tbl_sq3() {
    // sq will have no motion, because
    // it is reading from global table.
    // During bucket discovery it shouldn't
    // affect buckets from inner and outer children
    let query = r#"
    select "product_code" from "t" inner join "hash_testing"
    on "t"."a" = "hash_testing"."identification_number" and "hash_testing"."product_code"
    in (select "a" as a1 from "global_t")
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn global_tbl_sq4() {
    // sq will have no motion, because
    // it has distribution Segment .
    let query = r#"
    select * from "global_t" 
    where ("a", "b") in (select "a" as a, "b" as b from "t")
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn global_tbl_sq5() {
    // sq will have motion(full), because
    // it has distribution Any. So the
    // whole query will be executed on one node.
    let query = r#"
    select * from "global_t" 
    where "a" in (select "a" as a from "t")
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Any, buckets);
}

#[test]
fn global_tbl_join1() {
    let query = r#"
    select * from "global_t" 
    inner join (select "a" as a from "global_t")
    on true
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Any, buckets);
}

#[test]
fn global_tbl_join2() {
    // t2 sharded by (e, f)
    // Note that "a" in join condition relates to the left join scan as soon as under its right scan
    // we have an "A" aliased column.
    let query = r#"
        select * from "global_t"
        inner join (select "a" as q from "global_t")
        on ("a", "b") in (select "e", "f" from "t2")
    "#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn global_tbl_join3() {
    let query = r#"
    select * from "t2" 
    inner join "global_t"
    on ("e", "f") = (1, 1)
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();
    let param = Value::from(1_u64);
    let bucket = query
        .coordinator
        .determine_bucket_id(&[&param, &param])
        .unwrap();
    let bucket_set: HashSet<u64, RepeatableState> = vec![bucket].into_iter().collect();

    assert_eq!(Buckets::new_filtered(bucket_set), buckets);
}

#[test]
fn global_tbl_join4() {
    let query = r#"
    select * from "t2" 
    left join "global_t"
    on ("e", "f") = (1, 1)
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn global_tbl_join5() {
    let query = r#"
    select e from "global_t"
    left join (select sum("e") as e from "t2") as s
    on true
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Any, buckets);
}

#[test]
fn global_tbl_join6() {
    let query = r#"update "t3" set "b" = "b1" from (select "b" as "b1" from "global_t")"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn global_tbl_groupby() {
    let query = r#"select "a", avg("b") from "global_t" group by "a" having sum("b") > 10"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Any, buckets);
}

#[test]
fn update_local() {
    let query = r#"update t set c = 1 where (a, b) = (1, 1)"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Filtered(collection!(6691)), buckets);
}

#[test]
fn delete_local() {
    let query = r#"delete from t where (a, b) = (1, 1)"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Filtered(collection!(6691)), buckets);
}

#[test]
fn same_multicolumn_sk_in_eq() {
    let query = r#"select * from t where a = 1 and b = 1 and b = 2 and a = 2"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Filtered(collection!()), buckets);
}

#[test]
fn same_column_in_eq() {
    let query = r#"select * from test_space where id = 1 and id = 2"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, query, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Filtered(collection!()), buckets);
}
