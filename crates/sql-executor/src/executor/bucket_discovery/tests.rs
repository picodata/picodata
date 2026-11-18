use crate::test_helpers::ExecutingQueryExt;
use pretty_assertions::assert_eq;
use std::collections::HashSet;
use std::rc::Rc;

use crate::collection;
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::executor::engine::Vshard;
use crate::executor::ExecutingQuery;
use crate::ir::bucket::{BucketSet, Buckets};
use crate::ir::value::Value;

/// A global INNER JOIN preserves empty routing, so the following LEFT JOIN
/// and scalar aggregate can run in one stage for every parameter binding.
#[test]
fn prepared_left_join_over_global_join_preserves_empty_routing() {
    use crate::ir::node::relational::Relational;
    use crate::ir::operator::JoinKind;

    let sql = r#"SELECT count(*) FROM (
        SELECT s.a FROM (SELECT a FROM t5 WHERE a = $1) AS s
        JOIN global_t ON true
    ) AS l LEFT JOIN t5 AS r ON l.a = r.a"#;
    let coordinator = RouterRuntimeMock::new();
    let bucket = coordinator.determine_bucket_id(&[&Value::from(7)]).unwrap();
    for (param, expected) in [
        (Value::Null, Buckets::new_empty()),
        (
            Value::from(7),
            Buckets::new_filtered([bucket].into_iter().collect()),
        ),
    ] {
        let mut query =
            ExecutingQuery::from_text_and_params(&coordinator, sql, vec![param]).unwrap();
        let plan = query.get_exec_plan().get_ir_plan();
        assert!(plan.slices().slices().is_empty());
        let join = single_rel(
            plan,
            plan.get_top().unwrap(),
            |rel| matches!(rel, Relational::Join(join) if join.kind == JoinKind::LeftOuter),
        );
        assert_eq!(expected, query.bucket_discovery(join).unwrap());
    }
}

#[test]
fn global_inner_join_preserves_empty_input_but_not_scalar_aggregate() {
    for from in [
        "(SELECT a FROM t5 WHERE a = $1) AS s JOIN global_t ON true",
        "global_t JOIN (SELECT a FROM t5 WHERE a = $1) AS s ON true",
    ] {
        assert_eq!(
            Buckets::new_empty(),
            discover(&format!("SELECT * FROM {from}"), vec![Value::Null]),
        );
    }
    // Empty at planning time: no facts route on the join hides the global
    // input's Any, so only the choice of the input keeps it empty.
    for sql in [
        "SELECT * FROM global_t JOIN (SELECT a FROM t5 WHERE false) AS s ON true",
        "SELECT * FROM global_t JOIN (SELECT a FROM t5 WHERE a = 1 AND a = 2) AS s ON true",
        "SELECT s.a FROM global_t JOIN (SELECT a FROM t5 WHERE a = 1 AND a = 2) AS s ON true \
         LEFT JOIN t5 ON s.a = t5.a",
    ] {
        assert_eq!(Buckets::new_empty(), discover(sql, vec![]), "{sql}");
    }
    let sql = "SELECT * FROM global_t JOIN (SELECT count(*) AS c FROM t5 WHERE false) AS s ON true";
    assert_eq!(Buckets::Any, discover(sql, vec![]));
    for from in [
        "(SELECT count(*) AS c FROM t5 WHERE a = $1) AS s JOIN global_t ON true",
        "global_t JOIN (SELECT count(*) AS c FROM t5 WHERE a = $1) AS s ON true",
    ] {
        assert_eq!(
            Buckets::Any,
            discover(&format!("SELECT * FROM {from}"), vec![Value::Null]),
        );
    }
}

/// A chain of global INNER JOINs keeps the empty input, unless a scalar
/// aggregate under it yields a row on any node.
#[test]
fn global_join_chain_preserves_empty_input_but_not_scalar_aggregate() {
    let chain = |input: &str| {
        format!(
            "SELECT * FROM {input} AS s JOIN global_t AS g1 ON true \
             JOIN global_t AS g2 ON true JOIN global_t AS g3 ON true"
        )
    };
    let empty = chain("(SELECT a FROM t5 WHERE a = $1)");
    assert_eq!(Buckets::new_empty(), discover(&empty, vec![Value::Null]));
    let aggregate = chain("(SELECT count(*) AS c FROM t5 WHERE a = $1)");
    assert_eq!(Buckets::Any, discover(&aggregate, vec![Value::Null]));
}

/// Empty routing does not require a second aggregate stage: the scalar
/// aggregate runs on any node, still marked emits_row_from_empty_input.
#[test]
fn single_stage_count_with_empty_buckets_keeps_dispatch_flag() {
    let coordinator = RouterRuntimeMock::new();
    for (sql, params) in [
        ("SELECT count(*) FROM t5 WHERE a = $1", vec![Value::Null]),
        (
            "SELECT count(*) FROM test_space WHERE bucket_id = 0 AND id = 1",
            vec![],
        ),
    ] {
        let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, params).unwrap();
        let plan = query.get_exec_plan().get_ir_plan();
        assert!(plan.slices().slices().is_empty());
        let top = plan.get_top().unwrap();
        assert_eq!(Buckets::Any, query.bucket_discovery(top).unwrap());
        assert!(
            query
                .get_exec_plan()
                .subtree_dispatch_flags_at(top)
                .unwrap()
                .emits_row_from_empty_input
        );
    }
}

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

#[test]
fn bool_is_true_false_on_sharding_key() {
    let coordinator = RouterRuntimeMock::new();
    for (query, value) in [
        (r#"select * from bool_sharded where b is true"#, true),
        (r#"select * from bool_sharded where b is false"#, false),
        (
            r#"select * from bool_sharded where b is true is true and payload = 1"#,
            true,
        ),
        (
            r#"select * from bool_sharded where (b = false) is true"#,
            false,
        ),
    ] {
        let bucket_id = coordinator
            .determine_bucket_id(&[&Value::Boolean(value)])
            .unwrap();
        let expected: HashSet<_, _> = vec![bucket_id].into_iter().collect();
        assert_eq!(
            Buckets::new_filtered(expected),
            buckets_of(query),
            "{query}"
        );
    }

    // A NULL key passes these filters as well, so they cannot be routed.
    for query in [
        r#"select * from bool_sharded where b is not true"#,
        r#"select * from bool_sharded where b is not false"#,
        r#"select * from bool_sharded where (b = true) is false"#,
    ] {
        assert_eq!(Buckets::All, buckets_of(query), "{query}");
    }
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
    use crate::ir::tree::traversal::{NodeIter, PostOrder};

    let found: Vec<_> = PostOrder::new(&plan.nodes, |node| plan.nodes.rel_iter(node), top)
        .relational()
        .filter(|(_, rel)| is_wanted(rel))
        .map(|(id, _)| id)
        .collect();
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

/// Without enrichment, the condition folds in the join's `ON`; with it, an
/// INNER join's parameter check moves above the join (see `enrich_markers`).
#[cfg(not(feature = "enrich_restrictions"))]
#[test]
fn folded_join_condition() {
    let inner = r#"SELECT a."id" FROM "test_space" AS a JOIN "test_space_hist" AS b ON "#;
    let left = r#"SELECT a."id" FROM "test_space" AS a LEFT JOIN "test_space_hist" AS b ON "#;
    let cases = [
        ("false", vec![], Value::Boolean(false)),
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

/// Without enrichment, ON NULL remains NULL for both join kinds.
#[cfg(not(feature = "enrich_restrictions"))]
#[test]
fn null_join_condition() {
    let sql = r#"SELECT a."id" FROM "test_space" AS a JOIN "test_space_hist" AS b ON null"#;
    assert_eq!(
        Buckets::new_empty(),
        join_buckets(sql, vec![], &Value::Null)
    );
    let sql = r#"SELECT a."id" FROM "test_space" AS a LEFT JOIN "test_space_hist" AS b ON null"#;
    assert_eq!(Buckets::All, join_buckets(sql, vec![], &Value::Null));
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

/// Pruning on the `false` markers and parameter gates `enrich_restrictions`
/// derives.
#[cfg(feature = "enrich_restrictions")]
mod enrich_markers {
    use super::{discover, join_buckets, motion_input_buckets};
    use crate::ir::bucket::Buckets;
    use crate::ir::value::Value;
    use pretty_assertions::assert_eq;

    /// Enrichment moves the INNER ON's `$1 = 1` to a gate above the join; the
    /// bound value folds it to false and prunes every bucket.
    #[test]
    fn bound_parameter_gate_above_inner_join_prunes_buckets() {
        let sql = r#"SELECT a."id" FROM "test_space" AS a JOIN "test_space_hist" AS b
            ON a."id" = b."id" AND $1 = 1"#;
        assert_eq!(Buckets::new_empty(), discover(sql, vec![Value::from(2)]));
    }

    /// The outer check prunes a Motion input that had no parameter filter.
    #[test]
    fn outer_param_check_prunes_a_motion_input() {
        let sql = r#"SELECT t."id" FROM "test_space" AS t
            JOIN (SELECT "id", "sysFrom" FROM "test_space_hist") AS s
            ON t."id" = s."sysFrom" WHERE $1 = 1"#;
        assert_eq!(
            vec![Buckets::new_empty()],
            motion_input_buckets(sql, vec![Value::from(2)])
        );
        assert_eq!(
            vec![Buckets::All],
            motion_input_buckets(sql, vec![Value::from(1)])
        );
    }

    /// Both nested Motion inputs receive the domain's check.
    #[test]
    fn outer_param_check_prunes_nested_motion_inputs() {
        let sql = r#"SELECT t."id" FROM "test_space" AS t
            JOIN (SELECT x."id", x."sysFrom" FROM "test_space_hist" AS x
                  JOIN "test_space_hist" AS y ON x."sysFrom" = y."sys_op") AS s
            ON t."id" = s."sysFrom" WHERE $1 = 1"#;
        let buckets = motion_input_buckets(sql, vec![Value::from(2)]);
        assert!(buckets.len() >= 2, "{buckets:?}");
        assert!(
            buckets.iter().all(|b| *b == Buckets::new_empty()),
            "{buckets:?}"
        );
        let buckets = motion_input_buckets(sql, vec![Value::from(1)]);
        assert!(buckets.iter().all(|b| *b == Buckets::All), "{buckets:?}");
    }

    /// A LEFT join's `ON` is no restriction: its parameter check stays there,
    /// folds to false, and the left side is kept.
    #[test]
    fn bound_parameter_folds_left_join_on() {
        let sql = r#"SELECT a."id" FROM "test_space" AS a LEFT JOIN "test_space_hist" AS b
            ON a."id" = b."id" AND $1 = 1"#;
        assert_eq!(
            Buckets::All,
            join_buckets(sql, vec![Value::from(2)], &Value::Boolean(false))
        );
    }

    /// Enrichment folds INNER JOIN ON NULL to false. LEFT JOIN retains NULL
    /// and preserves its left rows.
    #[test]
    fn null_inner_join_condition_gets_a_false_marker() {
        let sql = r#"SELECT a."id" FROM "test_space" AS a JOIN "test_space_hist" AS b ON null"#;
        assert_eq!(
            Buckets::new_empty(),
            join_buckets(sql, vec![], &Value::Boolean(false))
        );
        let sql =
            r#"SELECT a."id" FROM "test_space" AS a LEFT JOIN "test_space_hist" AS b ON null"#;
        assert_eq!(Buckets::All, join_buckets(sql, vec![], &Value::Null));
    }

    /// Check pruning from a contradiction outside the sharding key.
    fn empty_by_contradiction(query: &str) {
        assert_eq!(Buckets::new_empty(), discover(query, vec![]), "{query}");
    }

    #[test]
    fn contradictory_where_on_a_non_sharding_column() {
        empty_by_contradiction(
            r#"SELECT "id" FROM "test_space" WHERE "sysFrom" = 1 AND "sysFrom" = 2"#,
        );
    }

    /// Binding folds the marked ON to false, enabling bucket pruning.
    #[test]
    fn contradictory_inner_join_condition_on_a_non_sharding_column() {
        empty_by_contradiction(
            r#"SELECT "t"."id" FROM "test_space" AS "t"
            JOIN "test_space_hist" AS "h" ON "t"."sysFrom" = 1 AND "t"."sysFrom" = 2"#,
        );
    }

    /// The conflict appears only after combining ON and WHERE facts.
    #[test]
    fn contradictory_class_across_join_condition_and_where() {
        empty_by_contradiction(
            r#"SELECT "t"."id" FROM "test_space" AS "t"
            JOIN "test_space_hist" AS "h" ON "t"."sysFrom" = "h"."sysFrom"
            WHERE "t"."sysFrom" = 1 AND "h"."sysFrom" = 2"#,
        );
    }

    #[test]
    fn every_or_arm_contradictory() {
        empty_by_contradiction(
            r#"SELECT "id" FROM "test_space"
            WHERE ("sysFrom" = 1 AND "sysFrom" = 2) OR ("sys_op" = 1 AND "sys_op" = 2)"#,
        );
    }

    /// A contradiction in the right subquery of a LEFT JOIN must not prune
    /// the left side: its rows survive with NULLs.
    #[test]
    fn contradiction_in_left_join_right_subquery_keeps_left_side() {
        let query = r#"SELECT "t"."id" FROM "test_space" AS "t"
            LEFT JOIN (
                SELECT "id" FROM "test_space_hist" WHERE "sysFrom" = 1 AND "sysFrom" = 2
            ) AS "s" ON "t"."id" = "s"."id""#;
        assert_eq!(Buckets::All, discover(query, vec![]));
    }

    /// A contradiction in one UNION ALL arm must not prune the other arm.
    #[test]
    fn contradiction_in_one_union_all_arm_keeps_the_other() {
        let query = r#"SELECT "id" FROM "test_space" WHERE "sysFrom" = 1 AND "sysFrom" = 2
            UNION ALL
            SELECT "id" FROM "test_space_hist""#;
        assert_eq!(Buckets::All, discover(query, vec![]));
    }
}

/// Discover buckets for each Motion input on the bound plan, in postorder.
/// This helper does not materialize virtual tables between discoveries.
fn motion_input_buckets(sql: &str, params: Vec<Value>) -> Vec<Buckets> {
    let coordinator = RouterRuntimeMock::new();
    let query = ExecutingQuery::from_text_and_params(&coordinator, sql, params).unwrap();
    motion_input_buckets_of(query)
}

/// [`motion_input_buckets`] of a query without parameters, optimized but
/// never bound: nothing folds the plan after motions are added.
#[cfg(feature = "enrich_restrictions")]
fn unbound_motion_input_buckets(sql: &str) -> Vec<Buckets> {
    use crate::executor::engine::Router;
    use crate::utils::MutexLike;
    use sql_frontend::frontend::sql::transform_into_plan;

    let coordinator = RouterRuntimeMock::new();
    let mut plan = {
        let metadata = coordinator.metadata().lock();
        transform_into_plan(sql, &[], &*metadata).unwrap()
    };
    plan.optimize_statement().unwrap();
    motion_input_buckets_of(ExecutingQuery::from_plan(&coordinator, plan).unwrap())
}

fn motion_input_buckets_of(mut query: ExecutingQuery<'_, RouterRuntimeMock>) -> Vec<Buckets> {
    use crate::ir::node::relational::Relational;
    use crate::ir::tree::traversal::{NodeIter, PostOrder};

    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let roots: Vec<_> = PostOrder::new(&plan.nodes, |node| plan.nodes.rel_iter(node), top)
        .relational()
        .filter(|(_, rel)| matches!(rel, Relational::Motion(_)))
        .map(|(motion, _)| plan.get_motion_subtree_root(motion).unwrap())
        .collect();
    roots
        .into_iter()
        .map(|root| query.bucket_discovery(root).unwrap())
        .collect()
}

/// Parameter checks must prune Motion inputs with and without enrichment.
/// A check moved above a join must be repeated below its Motion.
mod param_checks_on_motion_inputs {
    use super::{discover, motion_input_buckets};
    use crate::ir::bucket::Buckets;
    use crate::ir::value::Value;
    use pretty_assertions::assert_eq;

    /// `t` and `s` are sharded by `id`, the join is on `s."sysFrom"`, so `s`
    /// is resharded through a Motion.
    const JOIN: &str = r#"SELECT t."id" FROM "test_space" AS t
        JOIN (SELECT "id", "sysFrom" FROM "test_space_hist" WHERE $1 = 1) AS s
        ON t."id" = s."sysFrom""#;

    #[test]
    fn param_check_prunes_the_motion_input() {
        assert_eq!(
            vec![Buckets::new_empty()],
            motion_input_buckets(JOIN, vec![Value::from(2)])
        );
        assert_eq!(
            vec![Buckets::All],
            motion_input_buckets(JOIN, vec![Value::from(1)])
        );
        // `NULL = 1` is UNKNOWN, which a filter drops like FALSE.
        assert_eq!(
            vec![Buckets::new_empty()],
            motion_input_buckets(JOIN, vec![Value::Null])
        );
    }

    #[test]
    fn param_to_param_check_prunes_the_motion_input() {
        let sql = r#"SELECT t."id" FROM "test_space" AS t
            JOIN (SELECT "id", "sysFrom" FROM "test_space_hist" WHERE $1 = $2) AS s
            ON t."id" = s."sysFrom""#;
        let buckets = |l: Value, r: Value| motion_input_buckets(sql, vec![l, r]);
        assert_eq!(
            vec![Buckets::new_empty()],
            buckets(Value::from(1), Value::from(2))
        );
        assert_eq!(vec![Buckets::All], buckets(Value::from(1), Value::from(1)));
        assert_eq!(
            vec![Buckets::new_empty()],
            buckets(Value::Null, Value::from(1))
        );
        assert_eq!(
            vec![Buckets::new_empty()],
            buckets(Value::Null, Value::Null)
        );
    }

    /// `$1 = 1` shares its class with `"sysFrom"`.
    #[test]
    fn param_check_of_a_class_with_columns_prunes_the_motion_input() {
        let sql = r#"SELECT t."id" FROM "test_space" AS t
            JOIN (SELECT "id", "sysFrom" FROM "test_space_hist"
                  WHERE "sysFrom" = $1 AND $1 = 1) AS s
            ON t."id" = s."sysFrom""#;
        assert_eq!(
            vec![Buckets::new_empty()],
            motion_input_buckets(sql, vec![Value::from(2)])
        );
        assert_eq!(
            vec![Buckets::All],
            motion_input_buckets(sql, vec![Value::from(1)])
        );
    }

    /// A false NOT IN subquery check prunes its Motion input without pruning
    /// the outer query's buckets.
    #[test]
    fn not_in_subquery_check_keeps_the_outer_rows() {
        let sql = r#"SELECT "id" FROM "test_space"
            WHERE "id" NOT IN (SELECT "sysFrom" FROM "test_space_hist" WHERE $1 = 1)"#;
        assert_eq!(
            vec![Buckets::new_empty()],
            motion_input_buckets(sql, vec![Value::from(2)])
        );
        assert_eq!(Buckets::All, discover(sql, vec![Value::from(2)]));
    }

    /// A false check in the nullable subquery prunes its Motion input.
    #[test]
    fn left_join_nullable_side_check_prunes_its_motion_input_only() {
        let sql = r#"SELECT t."id" FROM "test_space" AS t
            LEFT JOIN (SELECT "id", "sysFrom" FROM "test_space_hist" WHERE $1 = 1) AS s
            ON t."id" = s."sysFrom""#;
        assert_eq!(
            vec![Buckets::new_empty()],
            motion_input_buckets(sql, vec![Value::from(2)])
        );
    }

    /// The input check prunes the buckets of the local count stage, which
    /// then runs once on any node, but not those of the final aggregate's
    /// Motion.
    #[test]
    fn check_below_count_does_not_prune_the_aggregate_result() {
        let sql = r#"SELECT t."id" FROM "test_space" AS t
            JOIN (SELECT count(*) AS c FROM "test_space_hist" WHERE $1 = 1) AS s
            ON t."id" = s.c"#;
        let buckets = motion_input_buckets(sql, vec![Value::from(2)]);
        // The final stage consumes the local aggregate's result.
        assert_eq!(2, buckets.len(), "{buckets:?}");
        assert_eq!(Buckets::Any, buckets[0]);
        assert_ne!(Buckets::new_empty(), buckets[1]);
    }

    /// A consumer's check does not prune the shared CTE's buckets.
    #[test]
    fn consumer_check_does_not_prune_a_shared_cte() {
        let sql = r#"WITH c AS (SELECT "id", "sysFrom" FROM "test_space_hist")
            SELECT t."id" FROM "test_space" AS t JOIN c ON t."id" = c."sysFrom" WHERE $1 = 1
            UNION ALL SELECT "sysFrom" FROM c"#;
        let buckets = motion_input_buckets(sql, vec![Value::from(2)]);
        assert!(!buckets.is_empty());
        assert!(
            buckets.iter().all(|b| *b != Buckets::new_empty()),
            "{buckets:?}"
        );
    }
}

/// Proven false checks prune eligible Motion inputs without parameter binding.
/// A check is added only when the input query lacks a false host from its domain.
#[cfg(feature = "enrich_restrictions")]
mod false_on_motion_inputs {
    use super::{discover, motion_input_buckets, unbound_motion_input_buckets};
    use crate::ir::bucket::Buckets;
    use crate::ir::value::Value;
    use pretty_assertions::assert_eq;

    /// Both bound and unbound plans prune the single Motion input.
    fn assert_motion_input_pruned(sql: &str) {
        assert_eq!(
            vec![Buckets::new_empty()],
            unbound_motion_input_buckets(sql),
            "{sql}"
        );
        assert_eq!(
            vec![Buckets::new_empty()],
            motion_input_buckets(sql, vec![]),
            "{sql}"
        );
    }

    #[test]
    fn contradiction_in_an_inner_on_prunes_the_motion_input() {
        assert_motion_input_pruned(
            r#"SELECT t."id" FROM "test_space" AS t
            JOIN (SELECT "id", "sysFrom" FROM "test_space_hist") AS s
            ON t."id" = s."sysFrom" AND t."sys_op" = 1 AND t."sys_op" = 2"#,
        );
    }

    /// The class's `false` lands above the join, out of the Motion's query.
    #[test]
    fn contradictory_class_across_filters_prunes_the_motion_input() {
        assert_motion_input_pruned(
            r#"SELECT t."id" FROM "test_space" AS t
            JOIN (SELECT "id", "sysFrom" FROM "test_space_hist" WHERE "sysFrom" = 1) AS s
            ON t."id" = s."id" + 1 WHERE s."sysFrom" = 2"#,
        );
    }

    #[test]
    fn explicit_false_where_prunes_the_motion_input() {
        assert_motion_input_pruned(
            r#"SELECT t."id" FROM "test_space" AS t
            JOIN (SELECT "id", "sysFrom" FROM "test_space_hist") AS s
            ON t."id" = s."sysFrom" WHERE false"#,
        );
    }

    /// The inner Motion runs first and is pruned too.
    #[test]
    fn false_prunes_nested_motion_inputs() {
        let sql = r#"SELECT t."id" FROM "test_space" AS t
            JOIN (SELECT x."id", x."sysFrom" FROM "test_space_hist" AS x
                  JOIN "test_space_hist" AS y ON x."sysFrom" = y."sys_op") AS s
            ON t."id" = s."sysFrom" WHERE false"#;
        for buckets in [
            unbound_motion_input_buckets(sql),
            motion_input_buckets(sql, vec![]),
        ] {
            assert!(buckets.len() >= 2, "{buckets:?}");
            assert!(
                buckets.iter().all(|b| *b == Buckets::new_empty()),
                "{buckets:?}"
            );
        }
    }

    /// `false` makes a passing parameter check irrelevant.
    #[test]
    fn false_with_a_passing_param_check_prunes_the_motion_input() {
        let sql = r#"SELECT t."id" FROM "test_space" AS t
            JOIN (SELECT "id", "sysFrom" FROM "test_space_hist") AS s
            ON t."id" = s."sysFrom" WHERE t."sys_op" = 1 AND t."sys_op" = 2 AND $1 = 1"#;
        assert_eq!(
            vec![Buckets::new_empty()],
            motion_input_buckets(sql, vec![Value::from(1)])
        );
    }

    /// An empty NOT IN subquery leaves the outer query whole.
    #[test]
    fn false_not_in_subquery_keeps_the_outer_rows() {
        let sql = r#"SELECT "id" FROM "test_space"
            WHERE "id" NOT IN (SELECT "sysFrom" FROM "test_space_hist" WHERE false)"#;
        assert_eq!(
            vec![Buckets::new_empty()],
            motion_input_buckets(sql, vec![])
        );
        assert_eq!(Buckets::All, discover(sql, vec![]));
    }

    /// An empty nullable side prunes its own Motion input only.
    #[test]
    fn false_left_join_nullable_side_prunes_its_motion_input_only() {
        let sql = r#"SELECT t."id" FROM "test_space" AS t
            LEFT JOIN (SELECT "id", "sysFrom" FROM "test_space_hist" WHERE false) AS s
            ON t."id" = s."sysFrom""#;
        assert_eq!(
            vec![Buckets::new_empty()],
            motion_input_buckets(sql, vec![])
        );
    }

    /// A false LEFT JOIN `ON` proves no emptiness.
    #[test]
    fn false_left_join_on_prunes_nothing() {
        let sql = r#"SELECT t."id" FROM "test_space" AS t
            LEFT JOIN (SELECT "id", "sysFrom" FROM "test_space_hist") AS s
            ON t."id" = s."sysFrom" AND false"#;
        assert_eq!(vec![Buckets::All], motion_input_buckets(sql, vec![]));
    }

    /// `count(*)` of an empty input is a row: the bare `false` keeps the
    /// aggregate one-stage, and it runs on any node.
    #[test]
    fn false_below_count_keeps_the_aggregate_result() {
        let sql = r#"SELECT t."id" FROM "test_space" AS t
            JOIN (SELECT count(*) AS c FROM "test_space_hist" WHERE false) AS s
            ON t."id" = s.c"#;
        assert_eq!(vec![Buckets::Any], motion_input_buckets(sql, vec![]));
    }

    /// One consumer's contradiction never empties a CTE another one reads.
    #[test]
    fn false_of_one_consumer_keeps_a_shared_cte() {
        let sql = r#"WITH c AS (SELECT "id", "sysFrom" FROM "test_space_hist")
            SELECT t."id" FROM "test_space" AS t JOIN c ON t."id" = c."sysFrom"
            WHERE t."sys_op" = 1 AND t."sys_op" = 2
            UNION ALL SELECT "sysFrom" FROM c"#;
        let buckets = motion_input_buckets(sql, vec![]);
        assert!(!buckets.is_empty());
        assert!(
            buckets.iter().all(|b| *b != Buckets::new_empty()),
            "{buckets:?}"
        );
    }
}

/// A left join yields rows only where its left side has them, whatever the right
/// side's buckets: matching rows of a co-located join share a bucket, and a
/// non-co-located side is moved next to the left one by a motion first.
/// An inner join runs on the union of its sides' buckets, narrowed by ON.
#[test]
fn join_buckets_left_follow_driving_side_inner_take_union() {
    let s1 = r#"(select "a", "b" from "t5" where "a" = 1) as s"#;
    let r2 = r#"(select "a" from "t5" where "a" = 2) as r"#;
    let coordinator = RouterRuntimeMock::new();
    let buckets = |vs: &[i64]| {
        let set: HashSet<_, _> = vs
            .iter()
            .map(|v| {
                coordinator
                    .determine_bucket_id(&[&Value::from(*v)])
                    .unwrap()
            })
            .collect();
        Buckets::new_filtered(set)
    };
    let cases = [
        (
            format!(r#"{s1} left join "t5" on s."a" = "t5"."a""#),
            buckets(&[1]),
        ),
        (
            format!(r#"{s1} left join {r2} on s."a" = r."a""#),
            buckets(&[1]),
        ),
        (
            format!(r#"{s1} left join "t5" on s."b" = "t5"."b""#),
            buckets(&[1]),
        ),
        // The facts pin `"t5"."a" = 2`, the unrestricted side too.
        (
            format!(r#""t5" join {r2} on "t5"."a" = r."a""#),
            buckets(&[2]),
        ),
        (
            format!(r#"{s1} join {r2} on s."a" = r."a""#),
            // Enrichment sees `a = 1 and a = 2` and empties both sides; the
            // facts alone leave the contradictory class unpinned.
            if cfg!(feature = "enrich_restrictions") {
                Buckets::new_empty()
            } else {
                buckets(&[1, 2])
            },
        ),
        // Not co-located: the other side is broadcast (Any), so s's buckets remain.
        (
            format!(r#"{s1} join "t5" on s."b" = "t5"."b""#),
            buckets(&[1]),
        ),
    ];
    for (from, expected) in cases {
        let sql = format!("select * from {from}");
        assert_eq!(expected, discover(&sql, vec![]), "{sql}");
    }
}

mod pinned_select_without_scan {
    use super::{discover, single_rel};
    use crate::executor::engine::mock::RouterRuntimeMock;
    use crate::executor::engine::Vshard;
    use crate::executor::ExecutingQuery;
    use crate::ir::bucket::Buckets;
    use crate::ir::node::expression::{Expression, MutExpression};
    use crate::ir::node::relational::Relational;
    use crate::ir::node::{BoolExpr, Constant, ScanRelation, Selection};
    use crate::ir::value::Value;
    use crate::test_helpers::sql_to_optimized_ir;
    use pretty_assertions::assert_eq;

    fn bucket_of(values: &[i64]) -> Buckets {
        let coordinator = RouterRuntimeMock::new();
        let set = values
            .iter()
            .map(|v| {
                coordinator
                    .determine_bucket_id(&[&Value::from(*v)])
                    .unwrap()
            })
            .collect();
        Buckets::new_filtered(set)
    }

    #[test]
    fn literal_key() {
        let sql = "SELECT (SELECT count(*) FROM t5 WHERE a = 1) > 0";
        assert_eq!(bucket_of(&[1]), discover(sql, vec![]));
    }

    #[test]
    fn bound_parameter() {
        let sql = "SELECT (SELECT b FROM t5 WHERE a = $1)";
        assert_eq!(bucket_of(&[7]), discover(sql, vec![Value::from(7)]));
    }

    #[test]
    fn two_subqueries_on_one_parameter() {
        let sql = "SELECT EXISTS (SELECT 1 FROM t5 WHERE a = $1) \
                   AND (SELECT b FROM t3_2 WHERE a = $1) > 0";
        assert_eq!(bucket_of(&[7]), discover(sql, vec![Value::from(7)]));
    }

    #[test]
    fn no_bucket_means_any_node() {
        assert_ne!(bucket_of(&[1]), bucket_of(&[2]));
        let sql = "SELECT (SELECT b FROM t5 WHERE a = 1 AND a = 2)";
        assert_eq!(Buckets::Any, discover(sql, vec![]));
    }

    #[test]
    fn empty_subquery_next_to_one_bucket() {
        assert_ne!(bucket_of(&[1]), bucket_of(&[2]));
        let sql = "SELECT (SELECT count(*) FROM t5 WHERE a = $1 AND a = $2), \
                   (SELECT b FROM t3_2 WHERE a = $1)";
        assert_eq!(
            bucket_of(&[1]),
            discover(sql, vec![Value::from(1), Value::from(2)])
        );
    }

    #[test]
    fn unpinned_runs_on_any_node() {
        for sql in ["SELECT (SELECT b FROM global_t WHERE a = 1)", "SELECT 1"] {
            assert_eq!(Buckets::Any, discover(sql, vec![]), "{sql}");
        }
    }

    /// Subqueries on different buckets in the query itself are not pinned:
    /// the planner keeps their motions and the stage runs.
    #[test]
    fn subqueries_on_different_buckets_are_not_pinned() {
        let sql = "SELECT (SELECT b FROM t5 WHERE a = 1), (SELECT b FROM t3_2 WHERE a = 2)";
        let plan = sql_to_optimized_ir(sql, vec![]);
        let top = plan.get_top().unwrap();
        assert!(!plan.is_pinned_select_without_scan(top).unwrap());
        let coordinator = RouterRuntimeMock::new();
        let mut query = ExecutingQuery::from_plan(&coordinator, plan).unwrap();
        query.bucket_discovery(top).unwrap();
    }

    /// Only a wrong proof pins subqueries to different buckets: fail rather
    /// than read one of them. The edit after optimize is out of the witness
    /// contract, so this checks the tree walk alone.
    #[test]
    fn several_buckets_fail() {
        assert_ne!(bucket_of(&[1]), bucket_of(&[2]));
        let mut plan = sql_to_optimized_ir(
            "SELECT (SELECT b FROM t5 WHERE a = 1), (SELECT b FROM t3_2 WHERE a = 1)",
            vec![],
        );
        let top = plan.get_top().unwrap();
        let selection = single_rel(&plan, top, |rel| match rel {
            Relational::Selection(Selection { child, .. }) => matches!(
                plan.get_relation_node(*child),
                Ok(Relational::ScanRelation(ScanRelation { relation, .. })) if relation == "t3_2"
            ),
            _ => false,
        });
        let Relational::Selection(Selection { filter, .. }) =
            plan.get_relation_node(selection).unwrap()
        else {
            unreachable!()
        };
        let Expression::Bool(BoolExpr { right, .. }) = plan.get_expression_node(*filter).unwrap()
        else {
            panic!("expected a = 1");
        };
        let right = *right;
        let MutExpression::Constant(Constant { value }) =
            plan.get_mut_expression_node(right).unwrap()
        else {
            panic!("expected a constant");
        };
        *value = Value::from(2);
        // The edit changes the query after optimize, out of the contract of
        // the facts witness: check the tree walk without it.
        std::rc::Rc::make_mut(&mut plan.facts_proof)
            .routes
            .remove(&selection);

        let coordinator = RouterRuntimeMock::new();
        let mut query = ExecutingQuery::from_plan(&coordinator, plan).unwrap();
        let error = query.bucket_discovery(top).unwrap_err();
        assert!(
            error.to_string().contains("pinned to one bucket reads"),
            "{error}"
        );
    }

    #[test]
    fn colocated_subquery_consumer() {
        let sql = "SELECT * FROM t5 WHERE a = $1 AND b IN (SELECT b FROM t3_2 WHERE a = $1)";
        assert_eq!(bucket_of(&[7]), discover(sql, vec![Value::from(7)]));
    }
}

/// The buckets of the rows whose sharding key is `key`, as a filtered set.
fn buckets_of_key(key: &[Value]) -> Buckets {
    let coordinator = RouterRuntimeMock::new();
    let key: Vec<&Value> = key.iter().collect();
    let bucket = coordinator.determine_bucket_id(&key).unwrap();
    Buckets::new_filtered([bucket].into_iter().collect())
}

/// A statement whose top has `Distribution::Single` runs on any node.
#[test]
fn single_top() {
    use crate::ir::distribution::Distribution;

    let coordinator = RouterRuntimeMock::new();
    let mut query =
        ExecutingQuery::from_text_and_params(&coordinator, r#"select count(*) from "t""#, vec![])
            .unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    assert_eq!(&Distribution::Single, plan.rel_distr_ref(top).unwrap());
    assert_eq!(Buckets::Any, query.bucket_discovery(top).unwrap());
}

/// A local segment motion without a virtual table takes the buckets of its
/// child.
#[test]
fn insert_local_segment() {
    let query = r#"insert into "t" ("a", "b", "c", "d")
        select "a", "b", "c", "d" from "t" where ("a", "b") = (1, 1)"#;
    assert_eq!(
        Buckets::Filtered(BucketSet::Exact(collection!(6691))),
        buckets_of(query)
    );
}

/// A local segment motion with a virtual table takes the buckets of the
/// virtual table, not of its child.
#[test]
fn insert_local_segment_with_vtable() {
    use crate::executor::vtable::VirtualTable;
    use crate::ir::node::relational::Relational;
    use crate::ir::node::Motion;
    use crate::ir::transformation::redistribution::MotionPolicy;
    use crate::test_helpers::vcolumn_integer_user_non_null;

    let query = r#"insert into "t" ("a", "b", "c", "d")
        select "a", "b", "c", "d" from "t" where ("a", "b") = (1, 1)"#;
    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let motion_id = plan.dml_child_id(top).unwrap();
    let Relational::Motion(Motion {
        policy: MotionPolicy::LocalSegment(_),
        ..
    }) = plan.get_relation_node(motion_id).unwrap()
    else {
        panic!("expected a local segment motion under INSERT");
    };

    let key = [Value::from(2), Value::from(2)];
    let mut vtable = VirtualTable::new();
    for _ in 0..4 {
        vtable.add_column(vcolumn_integer_user_non_null());
    }
    vtable.add_tuple(vec![
        key[0].clone(),
        key[1].clone(),
        Value::from(3),
        Value::from(4),
    ]);
    query
        .get_mut_exec_plan()
        .set_motion_vtable(&motion_id, vtable, &coordinator)
        .unwrap();

    assert_eq!(buckets_of_key(&key), query.bucket_discovery(top).unwrap());
}

#[test]
fn subquery_shared_by_group_by_and_having() {
    // The local projection and its `GROUP BY` list one subquery, which the
    // walk enters once.
    let query = r#"SELECT sum("a") FROM "t" GROUP BY (SELECT true) HAVING (SELECT true)"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let motion_id = query.get_motion_id(0, 0);
    let plan = query.get_exec_plan().get_ir_plan();
    let local_id = plan.get_first_rel_child(motion_id).unwrap();

    assert_eq!(Buckets::All, query.bucket_discovery(local_id).unwrap());
}

#[test]
fn cte_read_twice() {
    let query = "WITH c AS (SELECT a FROM t5 WHERE a = 1) \
        SELECT a FROM c UNION ALL SELECT a FROM t5 WHERE a = 2 UNION ALL SELECT a FROM c";
    let expected = buckets_of_key(&[Value::from(1)]).disjunct(&buckets_of_key(&[Value::from(2)]));
    assert_eq!(expected, buckets_of(query));
}

/// A node AddMotions proved single-node that routes wider after a lost
/// witness fails the stage instead of running without its motion.
#[test]
fn lost_witness_of_a_single_node_proof_fails() {
    use crate::ir::node::expression::{Expression, MutExpression};
    use crate::ir::node::relational::Relational;
    use crate::ir::node::{BoolExpr, Selection};
    use crate::ir::operator::Bool;
    use crate::test_helpers::sql_to_optimized_ir;

    let mut plan = sql_to_optimized_ir("SELECT count(*) FROM t5 WHERE a = 1", vec![]);
    let top = plan.get_top().unwrap();
    assert!(!plan.facts_proof.single_node.is_empty());
    let selection = single_rel(&plan, top, |rel| matches!(rel, Relational::Selection(_)));
    let Relational::Selection(Selection { filter, .. }) =
        plan.get_relation_node(selection).unwrap()
    else {
        unreachable!()
    };
    let filter = *filter;
    assert!(matches!(
        plan.get_expression_node(filter).unwrap(),
        Expression::Bool(BoolExpr { op: Bool::Eq, .. })
    ));
    // `a <> 1`: the tree walk reads every bucket, and nothing narrows it.
    let MutExpression::Bool(BoolExpr { op, .. }) = plan.get_mut_expression_node(filter).unwrap()
    else {
        unreachable!()
    };
    *op = Bool::NotEq;
    Rc::make_mut(&mut plan.facts_proof).routes.clear();

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_plan(&coordinator, plan).unwrap();
    let error = query.bucket_discovery(top).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("proven single-node by the planner"),
        "{error}"
    );
}

/// A node with a facts witness is routed by the walk and the witness
/// together, the witness distorted to the bucket of 2 here. A walk of the
/// bucket of 1 meets it too: nothing is left for the input of `count(*)`,
/// which itself runs on any node. So does a walk wider than one bucket:
/// the join inputs read the buckets of 1 and 3, and nothing is left.
#[test]
fn witness_narrows_the_walk() {
    use crate::ir::bucket::facts_route::{FactsRoute, FactsRoutePart};
    use crate::ir::Plan;
    use crate::test_helpers::sql_to_optimized_ir;

    let distort = |plan: &mut Plan| {
        let mut distorted = 0;
        for routes in Rc::make_mut(&mut plan.facts_proof).routes.values_mut() {
            for route in routes {
                if let FactsRoute::Key(parts) = route {
                    *parts = vec![FactsRoutePart::Value(Value::from(2))];
                    distorted += 1;
                }
            }
        }
        distorted
    };
    let coordinator = RouterRuntimeMock::new();
    let bucket = |v: i64| coordinator.determine_bucket_id(&[&Value::from(v)]).unwrap();
    assert_ne!(bucket(2), bucket(1));
    assert_ne!(bucket(2), bucket(3));

    let mut plan = sql_to_optimized_ir("SELECT count(*) FROM t5 WHERE a = 1", vec![]);
    let top = plan.get_top().unwrap();
    let input = plan.get_first_rel_child(top).unwrap();
    assert_eq!(1, distort(&mut plan));
    let mut query = ExecutingQuery::from_plan(&coordinator, plan).unwrap();
    assert_eq!(Buckets::new_empty(), query.bucket_discovery(input).unwrap());
    assert_eq!(Buckets::Any, query.bucket_discovery(top).unwrap());

    let mut plan = sql_to_optimized_ir(
        "SELECT x.a FROM (SELECT a FROM t5 WHERE a = 1) x \
         JOIN (SELECT a FROM t5 WHERE a IN (1, 3)) y ON x.a = y.a",
        vec![],
    );
    let top = plan.get_top().unwrap();
    // The Selection of `x` and the key of each join input.
    assert_eq!(3, distort(&mut plan));
    let mut query = ExecutingQuery::from_plan(&coordinator, plan).unwrap();
    assert_eq!(Buckets::new_empty(), query.bucket_discovery(top).unwrap());
}

/// A walk of one bucket is not the bucket of the key the facts proof rests
/// on: `bucket_id = $2` names another bucket than the key `id = $1`. The
/// witness of that key meets the walk and leaves the first branch no
/// bucket, so the union reads the bucket of the key alone, without motions,
/// instead of failing its single-node check with both buckets. The first
/// `count(*)`, of no input, runs there too.
#[test]
fn witness_meets_a_walk_of_another_bucket() {
    use crate::executor::engine::Router;
    use crate::ir::options::Options;
    use crate::ir::types::{DerivedType, UnrestrictedType};
    use crate::utils::MutexLike;
    use sql_frontend::frontend::sql::transform_into_plan;

    let coordinator = RouterRuntimeMock::new();
    let one = Value::from(1);
    let key = coordinator.determine_bucket_id(&[&one]).unwrap();
    let other = if key == 42 { 43 } else { 42 };
    let int = DerivedType::new(UnrestrictedType::Integer);
    for sql in [
        r#"SELECT count(*) FROM "test_space"
            WHERE "id" = "sys_op" AND "sys_op" = $1 AND "bucket_id" = $2
            UNION ALL SELECT count(*) FROM "test_space" WHERE "id" = $1"#,
        r#"SELECT "id" FROM "test_space"
            WHERE "id" = "sys_op" AND "sys_op" = $1 AND "bucket_id" = $2
            UNION SELECT "id" FROM "test_space" WHERE "id" = $1"#,
    ] {
        let mut plan = {
            let metadata = coordinator.metadata().lock();
            transform_into_plan(sql, &[int, int], &*metadata).unwrap()
        };
        plan.optimize_statement().unwrap();
        assert!(plan.slices().slices().is_empty(), "{sql}");
        plan.bind_statement(vec![one.clone(), Value::from(other)], Options::default())
            .unwrap();
        let top = plan.get_top().unwrap();
        let mut query = ExecutingQuery::from_plan(&coordinator, plan).unwrap();
        assert_eq!(
            Buckets::new_filtered(collection![key]),
            query.bucket_discovery(top).unwrap(),
            "{sql}"
        );
    }
}

/// Key hashes of shared values, see `key_buckets`.
mod shared_key_cache {
    use crate::collection;
    use crate::executor::bucket_discovery::walk_reads_no_bucket;
    use crate::executor::engine::mock::RouterRuntimeMock;
    use crate::executor::engine::{Router, Vshard};
    use crate::executor::ExecutingQuery;
    use crate::ir::bucket::facts_route::SameChecked;
    use crate::ir::bucket::Buckets;
    use crate::ir::bucket::{BucketsResolver, HashedKeys};
    use crate::ir::node::relational::Relational;
    use crate::ir::node::{Join, Selection};
    use crate::ir::operator::JoinKind;
    use crate::ir::options::Options;
    use crate::ir::tree::traversal::{NodeIter, PostOrder};
    use crate::ir::types::{DerivedType, UnrestrictedType};
    use crate::ir::value::Value;
    use crate::ir::Plan;
    use crate::utils::MutexLike;
    use pretty_assertions::assert_eq;
    use smol_str::SmolStr;
    use sql_frontend::frontend::sql::transform_into_plan;
    use std::cell::{Cell, RefCell};

    struct Counting(Cell<usize>);
    impl BucketsResolver for Counting {
        type Buckets = Buckets;
        fn buckets_from_motion(
            &self,
            _: crate::ir::node::NodeId,
        ) -> Result<Option<Buckets>, crate::errors::SbroadError> {
            Ok(None)
        }
        fn buckets_from_values(
            &self,
            _: &[&Value],
            _: Option<&SmolStr>,
        ) -> Result<Buckets, crate::errors::SbroadError> {
            self.0.set(self.0.get() + 1);
            Ok(Buckets::new_filtered(collection![1]))
        }
        fn buckets_from_bucket_id(
            &self,
            _: &Value,
            _: Option<&SmolStr>,
        ) -> Result<Buckets, crate::errors::SbroadError> {
            Ok(Buckets::new_filtered(collection![1]))
        }
    }

    /// The plan of `sql` prepared with integer parameters and bound to
    /// `params`.
    fn bound(sql: &str, params: Vec<Value>) -> Plan {
        let coordinator = RouterRuntimeMock::new();
        let types = vec![DerivedType::new(UnrestrictedType::Integer); params.len()];
        let mut plan = {
            let metadata = coordinator.metadata().lock();
            transform_into_plan(sql, &types, &*metadata).unwrap()
        };
        plan.optimize_statement().unwrap();
        plan.bind_statement(params, Options::default()).unwrap();
        plan
    }

    /// The key hashes of one discovery of the whole `plan`: the condition
    /// walk and the facts routes of each node, as `bucket_discovery` calls
    /// them, with one cache.
    fn hashes(plan: &Plan) -> usize {
        let resolver = Counting(Cell::new(0));
        let hashed = RefCell::new(HashedKeys::new());
        let same = RefCell::new(SameChecked::default());
        let top = plan.get_top().unwrap();
        for (id, rel) in
            PostOrder::new(&plan.nodes, |node| plan.nodes.rel_iter(node), top).relational()
        {
            let walk = match rel {
                Relational::Selection(Selection {
                    child,
                    subqueries,
                    filter,
                    ..
                }) => plan.get_expression_tree_buckets_cached(
                    *filter,
                    &[*child],
                    subqueries,
                    &resolver,
                    Some(&hashed),
                ),
                Relational::Join(Join {
                    left,
                    right,
                    subqueries,
                    condition,
                    kind: JoinKind::Inner,
                    ..
                }) => plan.get_expression_tree_buckets_cached(
                    *condition,
                    &[*left, *right],
                    subqueries,
                    &resolver,
                    Some(&hashed),
                ),
                _ => continue,
            }
            .unwrap();
            let hash_keys = !walk_reads_no_bucket(&walk);
            plan.facts_route_buckets(id, &resolver, hash_keys, &hashed, &same, walk)
                .unwrap();
        }
        resolver.0.get()
    }

    fn check(cases: &[(&str, Vec<Value>, usize)]) {
        for (sql, params, expected) in cases {
            let plan = bound(sql, params.clone());
            assert_eq!(*expected, hashes(&plan), "{sql} {params:?}");
        }
    }

    fn ints(values: &[i64]) -> Vec<Value> {
        values.iter().map(|v| Value::from(*v)).collect()
    }

    fn chain(key: &str) -> String {
        format!(
            "SELECT * FROM (SELECT a FROM t5 WHERE a = {key}) x \
             JOIN t5 y ON x.a = y.a JOIN t5 z ON y.a = z.a"
        )
    }

    /// The walk and the facts of `a = $1` hash its key once, and so do the
    /// facts and the carriers of enrichment that repeat it on a chain.
    #[test]
    fn walk_and_facts_hash_a_key_once() {
        check(&[
            ("SELECT a FROM t5 WHERE a = $1", ints(&[1]), 1),
            ("SELECT count(*) FROM t5 WHERE a = $1", ints(&[1]), 1),
            ("SELECT a FROM t5 WHERE a = b AND b = $1", ints(&[1]), 1),
            (&chain("$1"), ints(&[1]), 1),
            (
                "SELECT * FROM t5 WHERE a = $1 AND (b = 1 OR b = 2 OR b = 3)",
                ints(&[1]),
                1,
            ),
        ]);
    }

    /// Nodes of one parameter, and parameters of one value, share the hash;
    /// other values do not.
    #[test]
    fn equal_values_share_the_hash() {
        let twice = "SELECT a FROM t5 WHERE a = $1 UNION ALL SELECT a FROM t5 WHERE a = $1";
        let two = "SELECT a FROM t5 WHERE a = $1 UNION ALL SELECT a FROM t5 WHERE a = $2";
        let via_facts = "SELECT a FROM t5 WHERE a = b AND b = $1 \
                         UNION ALL SELECT a FROM t5 WHERE a = $2";
        check(&[
            (twice, ints(&[1]), 1),
            (two, ints(&[1, 1]), 1),
            (two, ints(&[1, 2]), 2),
            (via_facts, ints(&[7, 7]), 1),
            (via_facts, ints(&[7, 8]), 2),
        ]);
    }

    /// A composite key is one value of all its parts in hash order: another
    /// last part, or the parts swapped, is another key.
    #[test]
    fn composite_keys_compare_every_part() {
        let pair = |l: &str, r: &str| {
            format!(
                "SELECT a FROM t WHERE (a, b) = ({l}) UNION ALL SELECT a FROM t WHERE (a, b) = ({r})"
            )
        };
        check(&[
            (&pair("$1, $2", "$1, $3"), ints(&[1, 2, 2]), 1),
            (&pair("$1, $2", "$1, $3"), ints(&[1, 2, 3]), 2),
            (&pair("$1, $2", "$2, $1"), ints(&[1, 2]), 2),
            (&pair("$1, $2", "$2, $1"), ints(&[1, 1]), 1),
        ]);
    }

    /// Literals of a type that hashes by value share the hash with each
    /// other and with parameters of their value.
    #[test]
    fn literal_keys_share_the_hash() {
        check(&[
            (&chain("1"), vec![], 1),
            (
                "SELECT a FROM t5 WHERE a = 1 UNION ALL SELECT a FROM t5 WHERE a = $1",
                ints(&[1]),
                1,
            ),
            (
                "SELECT a FROM t5 WHERE a = 1 UNION ALL SELECT a FROM t5 WHERE a = $1",
                ints(&[2]),
                2,
            ),
        ]);
    }

    /// NULL and a bound value of another type than the route expects keep
    /// their verdicts with the key already in the cache.
    #[test]
    fn null_and_wrong_type_after_a_hashed_key() {
        let coordinator = RouterRuntimeMock::new();
        let one = coordinator.determine_bucket_id(&[&Value::from(1)]).unwrap();
        let sql = "SELECT a FROM t5 WHERE a = $1 UNION ALL SELECT a FROM t5 WHERE a = $2";
        let route = |params: Vec<Value>| {
            let plan = bound(sql, params);
            let top = plan.get_top().unwrap();
            ExecutingQuery::from_plan(&coordinator, plan)
                .unwrap()
                .bucket_discovery(top)
        };
        assert_eq!(
            Buckets::new_filtered(collection![one]),
            route(vec![Value::from(1), Value::Null]).unwrap()
        );
        let error = route(vec![Value::from(1), Value::from("1")]).unwrap_err();
        assert!(
            error.to_string().contains("facts route expects int"),
            "{error}"
        );
    }

    /// Each execution hashes its own values: the cache lives one discovery.
    #[test]
    fn executions_do_not_share_the_cache() {
        let coordinator = RouterRuntimeMock::new();
        let sql = "SELECT a FROM t5 WHERE a = $1 UNION ALL SELECT a FROM t5 WHERE a = b AND b = $1";
        for v in [1, 2, 1, 7] {
            let plan = bound(sql, ints(&[v]));
            let top = plan.get_top().unwrap();
            let bucket = coordinator.determine_bucket_id(&[&Value::from(v)]).unwrap();
            let mut query = ExecutingQuery::from_plan(&coordinator, plan).unwrap();
            assert_eq!(
                Buckets::new_filtered(collection![bucket]),
                query.bucket_discovery(top).unwrap(),
                "{v}"
            );
        }
    }
}

/// The pin of a class is one of its values: the others, unequal or NULL
/// after bind, leave no row, and the witness routes nowhere.
#[test]
fn unequal_values_of_one_class_route_nowhere() {
    let coordinator = RouterRuntimeMock::new();
    let one = coordinator.determine_bucket_id(&[&Value::from(1)]).unwrap();
    for (sql, params, expected) in [
        (
            "SELECT a FROM t5 WHERE a = $1 AND a = $2",
            vec![Value::from(1), Value::from(1)],
            Buckets::new_filtered([one].into_iter().collect()),
        ),
        (
            "SELECT a FROM t5 WHERE a = $1 AND a = $2",
            vec![Value::from(1), Value::from(2)],
            Buckets::new_empty(),
        ),
        (
            "SELECT a FROM t5 WHERE a = $1 AND a = $2",
            vec![Value::from(1), Value::Null],
            Buckets::new_empty(),
        ),
        (
            "SELECT a FROM t5 WHERE a = 1 AND a = $1",
            vec![Value::from(2)],
            Buckets::new_empty(),
        ),
        (
            "SELECT a FROM t5 WHERE bucket_id = $1 AND bucket_id = $2",
            vec![Value::from(1), Value::from(2)],
            Buckets::new_empty(),
        ),
    ] {
        assert_eq!(expected, discover(sql, params.clone()), "{sql} {params:?}");
    }
}

/// The hosts of one class share its list of values, so that a chain of joins
/// on one key keeps one list and bucket discovery checks it once; unequal
/// values still route nowhere.
#[test]
fn hosts_of_a_class_share_its_values() {
    use crate::executor::engine::Router;
    use crate::ir::bucket::facts_route::FactsRoute;
    use crate::ir::types::{DerivedType, UnrestrictedType};
    use crate::utils::MutexLike;
    use sql_frontend::frontend::sql::transform_into_plan;

    let sql = "SELECT x.a FROM t5 x JOIN t5 y ON x.a = y.a JOIN t5 z ON y.a = z.a \
               WHERE x.a = $1 AND x.a = $2";
    let params = vec![Value::from(1), Value::from(2)];
    let coordinator = RouterRuntimeMock::new();
    let types = vec![DerivedType::new(UnrestrictedType::Integer); params.len()];
    let mut plan = {
        let metadata = coordinator.metadata().lock();
        transform_into_plan(sql, &types, &*metadata).unwrap()
    };
    plan.optimize_statement().unwrap();
    let lists: Vec<_> = plan
        .facts_proof
        .routes
        .values()
        .flatten()
        .filter_map(|route| match route {
            FactsRoute::Same(values) => Some(values),
            _ => None,
        })
        .collect();
    assert!(lists.len() > 1, "{:?}", plan.facts_proof.routes);
    assert!(lists.iter().all(|values| Rc::ptr_eq(values, lists[0])));
    assert_eq!(Buckets::new_empty(), discover(sql, params));
}

/// Like `PreparedStatement::bind`, each execution binds a clone of the
/// prepared plan, which shares its arenas: the witness routes every binding
/// by its own value and leaves the prepared plan unbound.
#[test]
fn facts_witness_of_a_prepared_plan_routes_every_binding() {
    use crate::executor::engine::Router;
    use crate::ir::node::expression::Expression;
    use crate::ir::options::Options;
    use crate::ir::types::{DerivedType, UnrestrictedType};
    use crate::utils::MutexLike;
    use sql_frontend::frontend::sql::transform_into_plan;

    let coordinator = RouterRuntimeMock::new();
    let prepare = |sql: &str, types: &[DerivedType]| {
        let metadata = coordinator.metadata().lock();
        let mut plan = transform_into_plan(sql, types, &*metadata).unwrap();
        plan.optimize_statement().unwrap();
        plan
    };
    let route = |prepared: &crate::ir::Plan, params: Vec<Value>| {
        let mut plan = prepared.clone();
        plan.bind_statement(params, Options::default()).unwrap();
        let top = plan.get_top().unwrap();
        let mut query = ExecutingQuery::from_plan(&coordinator, plan).unwrap();
        query.bucket_discovery(top).unwrap()
    };
    let bucket_of = |key: &[&Value]| {
        Buckets::new_filtered(
            [coordinator.determine_bucket_id(key).unwrap()]
                .into_iter()
                .collect(),
        )
    };

    let prepared = prepare(
        "SELECT count(*) FROM t5 WHERE a = $1",
        &[DerivedType::new(UnrestrictedType::Integer)],
    );
    assert!(!prepared.facts_proof.routes.is_empty());
    let (one, two) = (Value::from(1), Value::from(2));
    for (param, expected) in [
        (one.clone(), bucket_of(&[&one])),
        (two.clone(), bucket_of(&[&two])),
        // `count(*)` of no row runs on any node.
        (Value::Null, Buckets::Any),
        (one.clone(), bucket_of(&[&one])),
    ] {
        assert_eq!(expected, route(&prepared, vec![param.clone()]), "{param:?}");
    }
    for routes in prepared.facts_proof.routes.values() {
        for route in routes {
            let crate::ir::bucket::facts_route::FactsRoute::Key(parts) = route else {
                continue;
            };
            for part in parts {
                if let crate::ir::bucket::facts_route::FactsRoutePart::Bound(id, _) = part {
                    assert!(matches!(
                        prepared.get_expression_node(*id),
                        Ok(Expression::Parameter(_))
                    ));
                }
            }
        }
    }

    // `CURRENT_USER` is replaced in each execution's copy, by the mock's "admin".
    let prepared = prepare(
        "SELECT count(*) FROM t1 WHERE a = current_user AND b = 1",
        &[],
    );
    assert!(!prepared.facts_proof.routes.is_empty());
    let admin = Value::from("admin");
    for _ in 0..2 {
        assert_eq!(bucket_of(&[&admin, &one]), route(&prepared, vec![]));
    }
}
