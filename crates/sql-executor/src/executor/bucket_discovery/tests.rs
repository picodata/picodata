use crate::test_helpers::ExecutingQueryExt;
use pretty_assertions::assert_eq;
use std::collections::HashSet;

use crate::collection;
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::executor::engine::Vshard;
use crate::executor::ExecutingQuery;
use crate::ir::bucket::{BucketSet, Buckets};
use crate::ir::value::Value;

#[test]
fn typed_literal_in_sharding_key_filter() {
    let query = r#"SELECT * FROM "test_space" WHERE "id" = int '1'"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::from(1);

    let bucket1 = query
        .get_coordinator()
        .determine_bucket_id(&[&param1])
        .unwrap();
    let bucket_set: HashSet<_, _> = vec![bucket1].into_iter().collect();
    let expected = Buckets::new_filtered(bucket_set);

    assert_eq!(expected, buckets);
}

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
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::from(1);

    let bucket1 = query
        .get_coordinator()
        .determine_bucket_id(&[&param1])
        .unwrap();
    let bucket_set: HashSet<_, _> = vec![bucket1].into_iter().collect();
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
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::from(1);
    let bucket1 = query
        .get_coordinator()
        .determine_bucket_id(&[&param1])
        .unwrap();

    let param100 = Value::from(100);
    let bucket100 = query
        .get_coordinator()
        .determine_bucket_id(&[&param100])
        .unwrap();

    let bucket_set: HashSet<_, _> = vec![bucket1, bucket100].into_iter().collect();
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
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::from(1);
    let param222 = Value::from("222");

    let bucket = query
        .get_coordinator()
        .determine_bucket_id(&[&param1, &param222])
        .unwrap();
    let bucket_set: HashSet<_, _> = vec![bucket].into_iter().collect();
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
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::from(1);
    let param100 = Value::from(100);
    let param1000 = Value::from(1000);
    let param222 = Value::from("222");
    let param111 = Value::from("111");

    let bucket1222 = query
        .get_coordinator()
        .determine_bucket_id(&[&param1, &param222])
        .unwrap();
    let bucket100222 = query
        .get_coordinator()
        .determine_bucket_id(&[&param100, &param222])
        .unwrap();
    let bucket1000222 = query
        .get_coordinator()
        .determine_bucket_id(&[&param1000, &param222])
        .unwrap();

    let bucket1111 = query
        .get_coordinator()
        .determine_bucket_id(&[&param1, &param111])
        .unwrap();
    let bucket100111 = query
        .get_coordinator()
        .determine_bucket_id(&[&param100, &param111])
        .unwrap();
    let bucket1000111 = query
        .get_coordinator()
        .determine_bucket_id(&[&param1000, &param111])
        .unwrap();

    let bucket_set: HashSet<_, _> = vec![
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
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::from(1);
    let bucket1 = query
        .get_coordinator()
        .determine_bucket_id(&[&param1])
        .unwrap();

    let param2 = Value::from(2);
    let bucket2 = query
        .get_coordinator()
        .determine_bucket_id(&[&param2])
        .unwrap();
    let bucket_set: HashSet<_, _> = vec![bucket1, bucket2].into_iter().collect();
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
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::from(1);
    let bucket1 = query
        .get_coordinator()
        .determine_bucket_id(&[&param1])
        .unwrap();
    let bucket_set: HashSet<_, _> = vec![bucket1].into_iter().collect();
    let expected = Buckets::new_filtered(bucket_set);

    assert_eq!(expected, buckets);
}

#[test]
fn global_tbl_selection() {
    let query = r#"
    select * from "global_t"
    where "a" = 1 or "b" = 2"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Any, buckets);
}

#[test]
fn global_tbl_scan() {
    let query = r#"
    select * from "global_t""#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
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
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
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
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Any, buckets);
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
    in (select "a"::text as a1 from "global_t")
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
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
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Any, buckets);
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
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
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
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
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
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Any, buckets);
}

#[test]
fn global_tbl_join3() {
    let query = r#"
    select * from "t2"
    inner join "global_t"
    on ("e", "f") = (1, 1)
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();
    let param = Value::from(1);
    let bucket = query
        .get_coordinator()
        .determine_bucket_id(&[&param, &param])
        .unwrap();
    let bucket_set: HashSet<_, _> = vec![bucket].into_iter().collect();

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
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
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
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Any, buckets);
}

#[test]
fn global_tbl_join6() {
    let query = r#"update "t3" set "b" = "b1" from (select "b" as "b1" from "global_t")"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn tbl_join_single_constant_condition1() {
    // t5 sharded by (a)
    let query = r#"
    select * from "t5"
    inner join "t5" as "jt5"
    on 1 in ("t5"."a")
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();
    let param = Value::from(1);
    let bucket = query
        .get_coordinator()
        .determine_bucket_id(&[&param])
        .unwrap();
    let bucket_set: HashSet<_, _> = vec![bucket].into_iter().collect();

    assert_eq!(Buckets::new_filtered(bucket_set), buckets);
}

#[test]
fn tbl_join_single_constant_condition2() {
    // t5 sharded by (a)
    let query = r#"
    select * from "t5"
    inner join "t5" as "jt5"
    on 1 in ("t5"."a", "t5"."a")
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();
    let param = Value::from(1);
    let bucket = query
        .get_coordinator()
        .determine_bucket_id(&[&param])
        .unwrap();
    let bucket_set: HashSet<_, _> = vec![bucket].into_iter().collect();

    assert_eq!(Buckets::new_filtered(bucket_set), buckets);
}

#[test]
fn tbl_join_single_constant_condition3() {
    // t5 sharded by (a)
    let query = r#"
    select * from "t5"
    inner join "t5" as "jt5"
    on 1 in ("t5"."a", "t5"."b", "t5"."a")
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();
    // b = 1 can be anywhere
    assert_eq!(Buckets::All, buckets);
}

#[test]
fn tbl_join_single_constant_condition4() {
    // t sharded by (a, b)
    let query = r#"
    select * from "t"
    inner join "t" as "jt"
    on 1 in ("jt"."a", "jt"."b")
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn tbl_join_tuple_constant_condition1() {
    // t5 sharded by (a)

    let query = r#"
    select * from "t5"
    inner join "t5" as "jt5"
    on "t5"."a" in (1,2,3);
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();
    let param1 = Value::from(1);
    let param2 = Value::from(2);
    let param3 = Value::from(3);
    let bucket1 = query
        .get_coordinator()
        .determine_bucket_id(&[&param1])
        .unwrap();
    let bucket2 = query
        .get_coordinator()
        .determine_bucket_id(&[&param2])
        .unwrap();
    let bucket3 = query
        .get_coordinator()
        .determine_bucket_id(&[&param3])
        .unwrap();
    let bucket_set: HashSet<_, _> = vec![bucket1, bucket2, bucket3].into_iter().collect();

    assert_eq!(Buckets::new_filtered(bucket_set), buckets);
}

#[test]
fn tbl_join_cross_table_constant_conditions1() {
    let query = r#"
    select * from "t5"
    inner join "t4"
    on "t5"."a" = 1 and "t4"."c" = 'x'
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();
    let param = Value::from(1);
    let bucket = query
        .get_coordinator()
        .determine_bucket_id(&[&param])
        .unwrap();
    let bucket_set: HashSet<_, _> = vec![bucket].into_iter().collect();

    assert_eq!(Buckets::new_filtered(bucket_set), buckets);
}

#[test]
fn tbl_join_cross_table_constant_conditions2() {
    let query = r#"
    select * from "t5"
    inner join "t4"
    on "t5"."a" in (1, 2) and "t4"."c" = 'x'
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();
    let param1 = Value::from(1);
    let param2 = Value::from(2);
    let bucket1 = query
        .get_coordinator()
        .determine_bucket_id(&[&param1])
        .unwrap();
    let bucket2 = query
        .get_coordinator()
        .determine_bucket_id(&[&param2])
        .unwrap();
    let bucket_set: HashSet<_, _> = vec![bucket1, bucket2].into_iter().collect();

    assert_eq!(Buckets::new_filtered(bucket_set), buckets);
}

#[test]
fn tbl_join_cross_table_constant_conditions3() {
    let query = r#"
    select * from "t5"
    inner join "t3_2"
    on "t5"."a" = 1 and "t3_2"."a" = 2
"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();
    let param = Value::from(1);
    let bucket = query
        .get_coordinator()
        .determine_bucket_id(&[&param])
        .unwrap();
    let bucket_set: HashSet<_, _> = vec![bucket].into_iter().collect();

    assert_eq!(Buckets::new_filtered(bucket_set), buckets);
}

#[test]
fn global_tbl_groupby() {
    let query = r#"select "a", avg("b") from "global_t" group by "a" having sum("b") > 10"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Any, buckets);
}

#[test]
fn update_local() {
    let query = r#"update t set c = 1 where (a, b) = (1, 1)"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(
        Buckets::Filtered(BucketSet::Exact(collection!(6691))),
        buckets
    );
}

#[test]
fn delete_local() {
    let query = r#"delete from t where (a, b) = (1, 1)"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(
        Buckets::Filtered(BucketSet::Exact(collection!(6691))),
        buckets
    );
}

#[test]
fn same_multicolumn_sk_in_eq() {
    let query = r#"select * from t where a = 1 and b = 1 and b = 2 and a = 2"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Filtered(BucketSet::Exact(collection!())), buckets);
}

#[test]
fn same_column_in_eq() {
    let query = r#"select * from test_space where id = 1 and id = 2"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    assert_eq!(Buckets::Filtered(BucketSet::Exact(collection!())), buckets);
}

#[test]
fn bool_eq_true_on_sharding_key() {
    let query = r#"select * from bool_sharded where b = true"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let true_val = Value::Boolean(true);
    let bucket_id = query
        .get_coordinator()
        .determine_bucket_id(&[&true_val])
        .unwrap();
    let expected: HashSet<_, _> = vec![bucket_id].into_iter().collect();

    assert_eq!(Buckets::new_filtered(expected), buckets);
}

/// Buckets for a query text against the mock router.
fn buckets_of(query: &str) -> Buckets {
    buckets_of_with_params(query, vec![])
}

/// Buckets for a query text with bound parameters against the mock router.
fn buckets_of_with_params(query: &str, params: Vec<Value>) -> Buckets {
    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, params).unwrap();
    let top = query.get_exec_plan().get_ir_plan().get_top().unwrap();
    query.bucket_discovery(top).unwrap()
}

#[test]
fn explicit_bucket_id_eq() {
    assert_eq!(
        buckets_of(r#"select * from test_space where bucket_id = 42"#),
        Buckets::Filtered(BucketSet::Exact(collection!(42)))
    );
}

#[test]
fn explicit_bucket_id_eq_reversed_operands() {
    assert_eq!(
        buckets_of(r#"select * from test_space where 42 = bucket_id"#),
        Buckets::Filtered(BucketSet::Exact(collection!(42)))
    );
}

#[test]
fn explicit_bucket_id_disjunction() {
    assert_eq!(
        buckets_of(r#"select * from test_space where bucket_id = 42 or bucket_id = 43"#),
        Buckets::Filtered(BucketSet::Exact(collection!(42, 43)))
    );
}

#[test]
fn explicit_bucket_id_in_list() {
    assert_eq!(
        buckets_of(r#"select * from test_space where bucket_id in (42, 43)"#),
        Buckets::Filtered(BucketSet::Exact(collection!(42, 43)))
    );
}

#[test]
fn explicit_bucket_id_anded_with_unrelated_filter() {
    assert_eq!(
        buckets_of(r#"select * from test_space where bucket_id = 42 and sys_op = 1"#),
        Buckets::Filtered(BucketSet::Exact(collection!(42)))
    );
}

#[test]
fn explicit_bucket_id_in_row_form() {
    assert_eq!(
        buckets_of(
            r#"select t1.id from test_space as t1
               join test_space as t2
               on t1.bucket_id = 42 and t2.bucket_id = 42"#
        ),
        Buckets::Filtered(BucketSet::Exact(collection!(42)))
    );
}

#[test]
fn explicit_bucket_id_conflicting_with_sharding_key() {
    let coordinator = RouterRuntimeMock::new();
    let one = Value::from(1);
    let id_bucket = coordinator.determine_bucket_id(&[&one]).unwrap();
    assert_ne!(
        id_bucket, 42,
        "test relies on a hash collision not happening"
    );

    assert_eq!(
        buckets_of(r#"select * from test_space where bucket_id = 42 and id = 1"#),
        Buckets::Filtered(BucketSet::Exact(collection!()))
    );
}

#[test]
fn explicit_bucket_id_ored_with_unrelated_filter() {
    assert_eq!(
        buckets_of(r#"select * from test_space where bucket_id = 42 or sys_op > 5"#),
        Buckets::All
    );
}

#[test]
fn explicit_bucket_id_out_of_range() {
    assert_eq!(
        buckets_of(r#"select * from test_space where bucket_id = 10001"#),
        Buckets::new_empty()
    );
    assert_eq!(
        buckets_of(r#"select * from test_space where bucket_id = 0"#),
        Buckets::new_empty()
    );
    assert_eq!(
        buckets_of(r#"select * from test_space where bucket_id = -1"#),
        Buckets::new_empty()
    );
}

#[test]
fn explicit_bucket_id_under_cast_is_not_a_bucket_predicate() {
    assert_eq!(
        buckets_of(r#"select * from test_space where cast(bucket_id as string) = '42'"#),
        Buckets::All
    );
}

#[test]
fn explicit_bucket_id_with_bound_parameter() {
    assert_eq!(
        buckets_of_with_params(
            r#"select * from test_space where bucket_id = ?"#,
            vec![Value::from(42)]
        ),
        Buckets::Filtered(BucketSet::Exact(collection!(42)))
    );
}

fn decimal(s: &str) -> Value {
    Value::Decimal(Box::new(s.parse().unwrap()))
}

#[test]
fn explicit_bucket_id_with_non_integer_parameter() {
    for value in [
        decimal("42"),
        decimal("42.5"),
        decimal("42.99"),
        Value::Double(42.0.into()),
        Value::Double(42.5.into()),
    ] {
        assert_eq!(
            buckets_of_with_params(
                r#"select * from test_space where bucket_id = ?"#,
                vec![value.clone()]
            ),
            Buckets::Filtered(BucketSet::Exact(collection!(42))),
            "{value:?} points at bucket 42"
        );
    }

    for value in [
        decimal("-1"),
        decimal("-1.5"),
        decimal("0.5"),
        Value::Double(f64::NAN.into()),
        Value::Double(f64::INFINITY.into()),
        Value::Null,
    ] {
        assert_eq!(
            buckets_of_with_params(
                r#"select * from test_space where bucket_id = ?"#,
                vec![value.clone()]
            ),
            Buckets::new_empty(),
            "{value:?} matches nothing"
        );
    }
}

#[test]
fn explicit_bucket_id_with_non_integer_constant() {
    for query in [
        r#"select * from test_space where bucket_id = 42.0"#,
        r#"select * from test_space where bucket_id = 42.5"#,
        r#"select * from test_space where bucket_id = 42e0"#,
        r#"select * from test_space where bucket_id = 4.25e1"#,
    ] {
        assert_eq!(
            buckets_of(query),
            Buckets::Filtered(BucketSet::Exact(collection!(42))),
            "{query}"
        );
    }
    assert_eq!(
        buckets_of(r#"select * from test_space where bucket_id = -1.5"#),
        Buckets::new_empty()
    );
}

#[test]
fn explicit_bucket_id_with_string_constant() {
    assert_eq!(
        buckets_of(r#"select * from test_space where bucket_id = '42'"#),
        Buckets::Filtered(BucketSet::Exact(collection!(42)))
    );
}

#[test]
fn explicit_bucket_id_in_delete() {
    assert_eq!(
        buckets_of(r#"delete from test_space where bucket_id = 42"#),
        Buckets::Filtered(BucketSet::Exact(collection!(42)))
    );
}

fn discover(query: &str, params: Vec<Value>) -> Buckets {
    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, params).unwrap();
    let top = query.get_exec_plan().get_ir_plan().get_top().unwrap();
    query.bucket_discovery(top).unwrap()
}

/// The only relational node of the tree under `top` that `is_wanted` accepts.
fn single_rel(
    plan: &crate::ir::Plan,
    top: crate::ir::node::NodeId,
    is_wanted: impl Fn(&crate::ir::node::relational::Relational) -> bool,
) -> crate::ir::node::NodeId {
    use crate::ir::tree::traversal::{PostOrderWithFilter, REL_CAPACITY};

    let tree = PostOrderWithFilter::new(
        |node| plan.nodes.rel_iter(node),
        |node| {
            plan.get_relation_node(node)
                .is_ok_and(|rel| is_wanted(&rel))
        },
        REL_CAPACITY,
    );
    let found = tree.traverse_into_vec(top);
    assert_eq!(1, found.len(), "expected exactly one matching node");
    found[0]
}

/// A parameter compared with a constant is merged into a row equality
/// (`(b, $1) = (1, 1)`), which constant folding does not look into. A bound
/// value that fails the comparison makes the query provably empty.
#[test]
fn unequal_constants_in_row_after_bind() {
    use crate::ir::node::expression::Expression;
    use crate::ir::node::relational::Relational;
    use crate::ir::node::{BoolExpr, Selection};
    use crate::ir::operator::Bool;

    let sql = r#"SELECT "id" FROM "test_space" WHERE "sysFrom" = 1 AND $1 = 1"#;
    let coordinator = RouterRuntimeMock::new();
    let mut query =
        ExecutingQuery::from_text_and_params(&coordinator, sql, vec![Value::from(2)]).unwrap();

    // The bound pair must still sit in a row equality, or the test would pass
    // without the check once folding learns to look into rows.
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let selection = single_rel(plan, top, |rel| matches!(rel, Relational::Selection(_)));
    let Relational::Selection(Selection { filter, .. }) =
        plan.get_relation_node(selection).unwrap()
    else {
        unreachable!()
    };
    let Expression::Bool(BoolExpr {
        op: Bool::Eq,
        left,
        right,
    }) = plan.get_expression_node(*filter).unwrap()
    else {
        panic!("expected the filter to be an equality");
    };
    let left = plan
        .get_row_list(*left)
        .expect("expected a row on the left");
    let right = plan
        .get_row_list(*right)
        .expect("expected a row on the right");
    assert_eq!(left.len(), right.len(), "row widths differ");
    let constant = |id| match plan.get_expression_node(id).unwrap() {
        Expression::Constant(c) => Some(c.value.clone()),
        _ => None,
    };
    let pairs: Vec<_> = left
        .iter()
        .zip(right)
        .filter_map(|(&l, &r)| Some((constant(l)?, constant(r)?)))
        .collect();
    let (two, one) = (Value::from(2), Value::from(1));
    assert!(
        pairs.contains(&(two.clone(), one.clone())) || pairs.contains(&(one, two)),
        "expected the bound pair 2 = 1 in the row equality, got {pairs:?}"
    );

    assert_eq!(Buckets::new_empty(), query.bucket_discovery(top).unwrap());
    assert_eq!(Buckets::All, discover(sql, vec![Value::from(1)]));
}

#[test]
fn null_constant_in_row_after_bind() {
    let query = r#"SELECT "id" FROM "test_space" WHERE "sysFrom" = 1 AND $1 = 1"#;
    assert_eq!(Buckets::new_empty(), discover(query, vec![Value::Null]));
}

/// Different numeric kinds are left undecided: `Value::eq` is not symmetric
/// across them.
#[test]
fn mixed_numeric_constants_in_row_after_bind() {
    let query = r#"SELECT "id" FROM "test_space" WHERE "sysFrom" = 1 AND $1 = 1"#;
    assert_eq!(Buckets::All, discover(query, vec![Value::from(2.5)]));
}

#[test]
fn unequal_constants_in_one_or_branch() {
    let query = r#"SELECT "id" FROM "test_space" WHERE ("sysFrom" = 1 AND $1 = 1) OR "id" = 5"#;
    let coordinator = RouterRuntimeMock::new();
    let bucket = coordinator.determine_bucket_id(&[&Value::from(5)]).unwrap();
    let expected = Buckets::new_filtered(vec![bucket].into_iter().collect());
    assert_eq!(expected, discover(query, vec![Value::from(2)]));
}

/// Check that the query's join condition is folded to `folded`, then discover
/// its buckets on the same query.
fn join_buckets(sql: &str, params: Vec<Value>, folded: &Value) -> Buckets {
    use crate::ir::node::expression::Expression;
    use crate::ir::node::relational::Relational;
    use crate::ir::node::{Constant, Join};

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, params).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let join = single_rel(plan, top, |rel| matches!(rel, Relational::Join(_)));
    let Relational::Join(Join { condition, .. }) = plan.get_relation_node(join).unwrap() else {
        unreachable!()
    };
    let Expression::Constant(Constant { value }) = plan.get_expression_node(*condition).unwrap()
    else {
        panic!("expected the join condition to be folded to a constant");
    };
    assert_eq!(folded, value);
    query.bucket_discovery(top).unwrap()
}

#[test]
fn folded_join_condition() {
    let inner = r#"SELECT a."id" FROM "test_space" AS a JOIN "test_space_hist" AS b ON "#;
    let left = r#"SELECT a."id" FROM "test_space" AS a LEFT JOIN "test_space_hist" AS b ON "#;
    let cases = [
        ("false", vec![], Value::Boolean(false)),
        ("null", vec![], Value::Null),
        (
            r#"a."id" = b."id" AND $1 = 1"#,
            vec![Value::from(2)],
            Value::Boolean(false),
        ),
    ];
    for (condition, params, folded) in cases {
        let sql = format!("{inner}{condition}");
        let buckets = join_buckets(&sql, params.clone(), &folded);
        assert_eq!(Buckets::new_empty(), buckets, "{sql}");
        // A LEFT JOIN keeps its left side whatever the condition.
        let sql = format!("{left}{condition}");
        let buckets = join_buckets(&sql, params, &folded);
        assert_eq!(Buckets::All, buckets, "{sql}");
    }
}

/// A column compared with a `NULL` parameter is never `TRUE`, so the query is
/// provably empty even though only one side of the comparison is a constant.
#[test]
fn column_eq_null_param() {
    let query = r#"SELECT "id" FROM "test_space" WHERE "sysFrom" = $1"#;
    assert_eq!(Buckets::new_empty(), discover(query, vec![Value::Null]));
}

#[test]
fn column_eq_null_param_in_one_or_branch() {
    let query = r#"SELECT "id" FROM "test_space" WHERE ("sysFrom" = $1 AND "id" = 1) OR "id" = 5"#;
    let coordinator = RouterRuntimeMock::new();
    let bucket = coordinator.determine_bucket_id(&[&Value::from(5)]).unwrap();
    let expected = Buckets::new_filtered(vec![bucket].into_iter().collect());
    assert_eq!(expected, discover(query, vec![Value::Null]));
}

/// The same inside a row equality: `("sysFrom", "sys_op") = (NULL, 1)`.
#[test]
fn column_eq_null_param_in_row() {
    let query = r#"SELECT "id" FROM "test_space" WHERE "sysFrom" = $1 AND "sys_op" = 1"#;
    assert_eq!(Buckets::new_empty(), discover(query, vec![Value::Null]));
}
