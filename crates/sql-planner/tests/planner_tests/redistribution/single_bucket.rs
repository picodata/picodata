//! `t5` and `t3_2` are sharded by `a`, `t` by `(a, b)`.

use sql::explain::explain_logical;
use sql::helpers::sql_to_ir_without_bind;
use sql::ir::types::DerivedType;

fn explain(query: &str) -> String {
    let params = query.matches('$').count();
    let query = format!("EXPLAIN (LOGICAL) {query}");
    let mut plan = sql_to_ir_without_bind(&query, &vec![DerivedType::unknown(); params]);
    plan.optimize().unwrap();
    explain_logical(&plan).unwrap()
}

#[test]
fn union_all_of_scalar_aggregates_on_one_parameter() {
    insta::assert_snapshot!(explain(
        "SELECT count(*) FROM t5 WHERE a = $1 UNION ALL SELECT count(*) FROM t3_2 WHERE a = $1"
    ), @r"
    union all
      projection (count(*)::int -> col_1)
        selection (t5.a::int = $1::int)
          scan t5
      projection (count(*)::int -> col_1)
        selection (t3_2.a::int = $1::int)
          scan t3_2
    ");
}

#[test]
fn union_all_of_scalar_aggregates_on_different_keys_keeps_motions() {
    insta::assert_snapshot!(explain(
        "SELECT count(*) FROM t5 WHERE a = $1 UNION ALL SELECT count(*) FROM t3_2 WHERE a = $2"
    ), @r"
    union all
      motion [policy: segment([ref(col_1)]), program: ReshardIfNeeded]
        projection (count(*)::int -> col_1)
          selection (t5.a::int = $1::int)
            scan t5
      motion [policy: segment([ref(col_1)]), program: ReshardIfNeeded]
        projection (count(*)::int -> col_1)
          selection (t3_2.a::int = $2::int)
            scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT count(*) FROM t5 WHERE a = $1 UNION ALL SELECT count(*) FROM t3_2 WHERE a = 1"
    ), @r"
    union all
      motion [policy: segment([ref(col_1)]), program: ReshardIfNeeded]
        projection (count(*)::int -> col_1)
          selection (t5.a::int = $1::int)
            scan t5
      motion [policy: segment([ref(col_1)]), program: ReshardIfNeeded]
        projection (count(*)::int -> col_1)
          selection (t3_2.a::int = 1::int)
            scan t3_2
    ");
}

#[test]
fn aggregate_over_join_on_one_parameter() {
    insta::assert_snapshot!(explain(
        "SELECT count(*) FROM (SELECT * FROM t5 WHERE a = $1) x \
         JOIN (SELECT * FROM t3_2 WHERE a = $1) y ON true"
    ), @r"
    projection (count(*)::int -> col_1)
      join on (true::bool)
        scan x
          projection (t5.a::int -> a, t5.b::int -> b)
            selection (t5.a::int = $1::int)
              scan t5
        scan y
          projection (t3_2.a::int -> a, t3_2.b::int -> b)
            selection (t3_2.a::int = $1::int)
              scan t3_2
    ");
}

#[test]
fn limit_over_union_all_on_one_parameter() {
    insta::assert_snapshot!(explain(
        "SELECT * FROM (SELECT b FROM t5 WHERE a = $1 UNION ALL SELECT b FROM t3_2 WHERE a = $1) u \
         ORDER BY b LIMIT 1"
    ), @r"
    limit 1
      projection (b::int)
        order by (b::int)
          scan
            projection (u.b::int -> b)
              scan u
                union all
                  projection (t5.b::int -> b)
                    selection (t5.a::int = $1::int)
                      scan t5
                  projection (t3_2.b::int -> b)
                    selection (t3_2.a::int = $1::int)
                      scan t3_2
    ");
}

#[test]
fn parameter_keys_that_differ_keep_plans() {
    insta::assert_snapshot!(explain("SELECT count(*) FROM t5 WHERE a = $1 OR a = $2"), @r"
    projection (sum(count_1::int::int)::int::int -> col_1)
      motion [policy: full, program: ReshardIfNeeded]
        projection (count(*)::int -> count_1)
          selection (t5.a::int = $1::int or t5.a::int = $2::int)
            scan t5
    ");
    insta::assert_snapshot!(explain("SELECT count(*) FROM t5 WHERE a IN ($1, $2)"), @r"
    projection (sum(count_1::int::int)::int::int -> col_1)
      motion [policy: full, program: ReshardIfNeeded]
        projection (count(*)::int -> count_1)
          selection (t5.a::int in ROW($1::int, $2::int))
            scan t5
    ");
    // `(a, b) = ($1, 2)` is a key of `t`, not of `t5`.
    insta::assert_snapshot!(explain(
        "SELECT count(*) FROM t WHERE (a, b) = ($1, 2) UNION ALL SELECT count(*) FROM t5 WHERE a = $1"
    ), @r"
    union all
      motion [policy: segment([ref(col_1)]), program: ReshardIfNeeded]
        projection (count(*)::int -> col_1)
          selection (ROW(t.a::int, t.b::int) = ROW($1::int, 2::int))
            scan t
      motion [policy: segment([ref(col_1)]), program: ReshardIfNeeded]
        projection (count(*)::int -> col_1)
          selection (t5.a::int = $1::int)
            scan t5
    ");
}

#[test]
fn colocated_subquery_in_where() {
    insta::assert_snapshot!(explain(
        "SELECT * FROM t5 WHERE a = 1 AND b IN (SELECT b FROM t3_2 WHERE a = 1)"
    ), @r"
    projection (t5.a::int -> a, t5.b::int -> b)
      selection ((t5.a::int = 1::int and t5.b::int in ROW($0)))
        scan t5
    subquery $0:
      scan
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = 1::int)
            scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM t5 WHERE a = $1 AND b IN (SELECT b FROM t3_2 WHERE a = $1)"
    ), @r"
    projection (t5.a::int -> a, t5.b::int -> b)
      selection ((t5.a::int = $1::int and t5.b::int in ROW($0)))
        scan t5
    subquery $0:
      scan
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = $1::int)
            scan t3_2
    ");
}

#[test]
fn colocated_exists_with_order_by() {
    insta::assert_snapshot!(explain(
        "SELECT * FROM t5 WHERE a = $1 AND EXISTS (SELECT 1 FROM t3_2 WHERE a = $1) ORDER BY b"
    ), @r"
    projection (a::int, b::int)
      order by (b::int)
        scan
          projection (t5.a::int -> a, t5.b::int -> b)
            selection ((t5.a::int = $1::int and exists ROW($0)))
              scan t5
    subquery $0:
      scan
        projection (1::int -> col_1)
          selection (t3_2.a::int = $1::int)
            scan t3_2
    ");
}

#[test]
fn colocated_not_exists_that_may_read_nothing() {
    insta::assert_snapshot!(explain(
        "SELECT * FROM t5 WHERE a = $1 AND NOT EXISTS (SELECT 1 FROM t3_2 WHERE a = $1 AND a = $2)"
    ), @r"
    projection (t5.a::int -> a, t5.b::int -> b)
      selection ((t5.a::int = $1::int and not exists ROW($0)))
        scan t5
    subquery $0:
      scan
        projection (1::int -> col_1)
          selection ((t3_2.a::int = $1::int and t3_2.a::int = $2::int))
            scan t3_2
    ");
}

#[test]
fn colocated_subquery_in_select_list() {
    insta::assert_snapshot!(explain(
        "SELECT b, (SELECT b FROM t3_2 WHERE a = $1) FROM t5 WHERE a = $1"
    ), @r"
    projection (t5.b::int -> b, ROW($0) -> col_1)
      selection (t5.a::int = $1::int)
        scan t5
    subquery $0:
      scan
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = $1::int)
            scan t3_2
    ");
}

#[test]
fn colocated_subquery_in_update_and_delete() {
    insta::assert_snapshot!(explain(
        "UPDATE t5 SET b = (SELECT b FROM t3_2 WHERE a = $1) WHERE a = $1"
    ), @r"
    update t5 (b = col_0)
      motion [policy: local, program: ReshardIfNeeded]
        projection (ROW($0) -> col_0, t5.a::int -> col_1)
          selection (t5.a::int = $1::int)
            scan t5
    subquery $0:
      scan
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = $1::int)
            scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "DELETE FROM t5 WHERE a = $1 AND b IN (SELECT b FROM t3_2 WHERE a = $1)"
    ), @r"
    delete from t5
      motion [policy: local, program: [PrimaryKey(0), ReshardIfNeeded]]
        projection (t5.a::int -> pk_col_0)
          selection ((t5.a::int = $1::int and t5.b::int in ROW($0)))
            scan t5
    subquery $0:
      scan
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = $1::int)
            scan t3_2
    ");
}

#[test]
fn colocated_subquery_in_group_by_and_order_by() {
    insta::assert_snapshot!(explain(
        "SELECT b FROM t5 WHERE a = $1 GROUP BY b, (SELECT b FROM t3_2 WHERE a = $1)"
    ), @r"
    projection (t5.b::int -> b)
      group by (t5.b::int, ROW($0))
        selection (t5.a::int = $1::int)
          scan t5
    subquery $0:
      scan
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = $1::int)
            scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT b FROM t5 WHERE a = $1 ORDER BY (SELECT b FROM t3_2 WHERE a = $1)"
    ), @r"
    projection (b::int)
      order by (ROW($0))
        scan
          projection (t5.b::int -> b)
            selection (t5.a::int = $1::int)
              scan t5
    subquery $0:
      scan
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = $1::int)
            scan t3_2
    ");
}

#[test]
fn subquery_of_a_consumer_on_many_nodes_keeps_motion() {
    insta::assert_snapshot!(explain(
        "SELECT * FROM t5 WHERE b IN (SELECT b FROM t3_2 WHERE a = $1)"
    ), @r"
    projection (t5.a::int -> a, t5.b::int -> b)
      selection (t5.b::int in ROW($0))
        scan t5
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = $1::int)
              scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM global_t WHERE a IN (SELECT b FROM t3_2 WHERE a = $1)"
    ), @r"
    projection (global_t.a::int -> a, global_t.b::int -> b)
      selection (global_t.a::int in ROW($0))
        scan global_t
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = $1::int)
              scan t3_2
    ");
}

#[test]
fn subquery_on_another_key_keeps_motion() {
    insta::assert_snapshot!(explain(
        "SELECT * FROM t5 WHERE a = $1 AND b IN (SELECT b FROM t3_2 WHERE a = $2)"
    ), @r"
    projection (t5.a::int -> a, t5.b::int -> b)
      selection ((t5.a::int = $1::int and t5.b::int in ROW($0)))
        scan t5
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = $2::int)
              scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM t5 WHERE a = $1 AND b IN (SELECT b FROM t3_2 WHERE a = 1)"
    ), @r"
    projection (t5.a::int -> a, t5.b::int -> b)
      selection ((t5.a::int = $1::int and t5.b::int in ROW($0)))
        scan t5
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = 1::int)
              scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM t5 WHERE a = 1 AND b IN (SELECT b FROM t3_2 WHERE a = 2)"
    ), @r"
    projection (t5.a::int -> a, t5.b::int -> b)
      selection ((t5.a::int = 1::int and t5.b::int in ROW($0)))
        scan t5
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = 2::int)
              scan t3_2
    ");
}

#[test]
fn colocated_subquery_used_twice() {
    insta::assert_snapshot!(explain(
        "SELECT * FROM t5 WHERE a = $1 AND (SELECT b FROM t3_2 WHERE a = $1) BETWEEN 1 AND 2"
    ), @r"
    projection (t5.a::int -> a, t5.b::int -> b)
      selection ((t5.a::int = $1::int and ROW($0) >= 1::int and ROW($0) <= 2::int))
        scan t5
    subquery $0:
      scan
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = $1::int)
            scan t3_2
    ");
}

#[test]
fn subquery_of_a_scalar_aggregate_keeps_motion() {
    insta::assert_snapshot!(explain(
        "SELECT count(*), (SELECT b FROM t3_2 WHERE a = $1) FROM t5 WHERE a = $1"
    ), @r"
    projection (count(*)::int -> col_1, ROW($0) -> col_2)
      selection (t5.a::int = $1::int)
        scan t5
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = $1::int)
              scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT (SELECT b FROM t3_2 WHERE a = $1) FROM t5 WHERE a = $1 HAVING count(*) > 0"
    ), @r"
    projection (ROW($0) -> col_1)
      having (count(*)::int > 0::int)
        selection (t5.a::int = $1::int)
          scan t5
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = $1::int)
              scan t3_2
    ");
}

#[test]
fn subquery_over_a_scalar_aggregate_keeps_motion() {
    insta::assert_snapshot!(explain(
        "SELECT c, (SELECT b FROM t3_2 WHERE a = $1) \
         FROM (SELECT count(*) AS c FROM t5 WHERE a = $1 AND a = $2) s"
    ), @r"
    projection (s.c::int -> c, ROW($0) -> col_1)
      scan s
        projection (count(*)::int -> c)
          selection ((t5.a::int = $1::int and t5.a::int = $2::int))
            scan t5
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = $1::int)
              scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM (SELECT count(*) AS c FROM t5 WHERE a = 1 AND bucket_id = 1) s \
         WHERE c < (SELECT b FROM t3_2 WHERE a = 1)"
    ), @r"
    projection (s.c::int -> c)
      selection (s.c::int < ROW($0))
        scan s
          projection (count(*)::int -> c)
            selection ((t5.a::int = 1::int and t5.bucket_id::int = 1::int))
              scan t5
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = 1::int)
              scan t3_2
    ");
    // The motion of the IN subquery may leave no bucket at runtime.
    insta::assert_snapshot!(explain(
        "SELECT c, (SELECT b FROM t3_2 WHERE a = $1) \
         FROM (SELECT count(*) AS c FROM t5 WHERE a = $1 AND a IN (SELECT b FROM t3_2)) s"
    ), @r"
    projection (s.c::int -> c, ROW($1) -> col_1)
      scan s
        projection (count(*)::int -> c)
          selection ((t5.a::int = $1::int and t5.a::int in ROW($0)))
            scan t5
    subquery $0:
      motion [policy: segment([ref(b)]), program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            scan t3_2
    subquery $1:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = $1::int)
              scan t3_2
    ");
}

#[test]
fn colocated_subquery_over_a_grouped_aggregate() {
    insta::assert_snapshot!(explain(
        "SELECT c, (SELECT b FROM t3_2 WHERE a = $1) \
         FROM (SELECT b, count(*) AS c FROM t5 WHERE a = $1 GROUP BY b) s"
    ), @r"
    projection (s.c::int -> c, ROW($0) -> col_1)
      scan s
        projection (t5.b::int -> b, count(*)::int -> c)
          group by (t5.b::int)
            selection (t5.a::int = $1::int)
              scan t5
    subquery $0:
      scan
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = $1::int)
            scan t3_2
    ");
}

#[test]
fn subquery_under_windows_gathered_on_the_router_keeps_motion() {
    // A volatile function keeps the windows on the router.
    insta::assert_snapshot!(explain(
        "SELECT pico_instance_uuid(), row_number() OVER (ORDER BY b), \
         (SELECT b FROM t3_2 WHERE a = 1) FROM t5 WHERE a = 1"
    ), @r#"
    projection (".proc_instance_uuid"()::text -> col_1, row_number() over (order by (b::int asc) ) -> col_2, ROW($0) -> col_3)
      motion [policy: full, program: ReshardIfNeeded]
        projection (t5.bucket_id::int -> bucket_id, t5.b::int -> b)
          selection (t5.a::int = 1::int)
            scan t5
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = 1::int)
              scan t3_2
    "#);
    insta::assert_snapshot!(explain(
        "SELECT * FROM (SELECT b, row_number() OVER () AS rn, \
         (SELECT b FROM t3_2 WHERE a = 1) AS s FROM t5 WHERE a = 1) w JOIN t ON w.b = t.c"
    ), @r"
    projection (w.b::int -> b, w.rn::int -> rn, w.s::int -> s, t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      join on (w.b::int = t.c::int)
        scan w
          projection (b::int, row_number() over () -> rn, ROW($0) -> s)
            motion [policy: full, program: ReshardIfNeeded]
              projection (t5.bucket_id::int -> bucket_id, t5.b::int -> b)
                selection (t5.a::int = 1::int)
                  scan t5
        scan t
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = 1::int)
              scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT b, (SELECT b FROM t3_2 WHERE a = 1) AS c, row_number() OVER (ORDER BY b) AS rn \
         FROM t5 WHERE a = 1 UNION ALL SELECT b, b, 0 FROM t5 WHERE a = 2"
    ), @r"
    union all
      motion [policy: local, program: SerializeAsEmptyTable(true)]
        projection (b::int, ROW($0) -> c, row_number() over (order by (b::int asc) ) -> rn)
          motion [policy: full, program: ReshardIfNeeded]
            projection (t5.bucket_id::int -> bucket_id, t5.b::int -> b)
              selection (t5.a::int = 1::int)
                scan t5
      projection (t5.b::int -> b, t5.b::int -> b, 0::int -> col_1)
        selection (t5.a::int = 2::int)
          scan t5
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = 1::int)
              scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM (SELECT b, row_number() OVER () AS rn FROM t5 WHERE a = 1 \
         GROUP BY b, (SELECT b FROM t3_2 WHERE a = 1)) w JOIN t ON w.b = t.c"
    ), @r"
    projection (w.b::int -> b, w.rn::int -> rn, t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      join on (w.b::int = t.c::int)
        motion [policy: full, program: ReshardIfNeeded]
          scan w
            projection (b::int, row_number() over () -> rn)
              group by (b::int, ROW($0))
                motion [policy: full, program: ReshardIfNeeded]
                  projection (t5.bucket_id::int -> bucket_id, t5.a::int -> a, t5.b::int -> b)
                    selection (t5.a::int = 1::int)
                      scan t5
        scan t
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = 1::int)
              scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM (SELECT b, (SELECT b FROM t3_2 WHERE a = 1) AS s, \
         row_number() OVER () AS rn FROM t5 WHERE a = 1 GROUP BY b) w JOIN t ON w.b = t.c"
    ), @r"
    projection (w.b::int -> b, w.s::int -> s, w.rn::int -> rn, t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      join on (w.b::int = t.c::int)
        motion [policy: full, program: ReshardIfNeeded]
          scan w
            projection (b::int, ROW($0) -> s, row_number() over () -> rn)
              group by (b::int)
                motion [policy: full, program: ReshardIfNeeded]
                  projection (t5.bucket_id::int -> bucket_id, t5.a::int -> a, t5.b::int -> b)
                    selection (t5.a::int = 1::int)
                      scan t5
        scan t
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = 1::int)
              scan t3_2
    ");
}

#[test]
fn colocated_subquery_next_to_windows_in_place() {
    insta::assert_snapshot!(explain(
        "SELECT row_number() OVER (ORDER BY b), (SELECT b FROM t3_2 WHERE a = $1) \
         FROM t5 WHERE a = $1"
    ), @r"
    projection (row_number() over (order by (t5.b::int asc) ) -> col_1, ROW($0) -> col_2)
      selection (t5.a::int = $1::int)
        scan t5
    subquery $0:
      scan
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = $1::int)
            scan t3_2
    ");
}

/// `a = NULL` reads no bucket, but a filter over a global table runs on any node.
#[test]
fn subquery_of_a_filter_over_a_global_table_keeps_motion() {
    insta::assert_snapshot!(explain(
        "SELECT * FROM global_t WHERE a = NULL AND b IN (SELECT b FROM t5 WHERE a = 1)"
    ), @r"
    projection (global_t.a::int -> a, global_t.b::int -> b)
      selection ((global_t.a::int = NULL::unknown and global_t.b::int in ROW($0)))
        scan global_t
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t5.b::int -> b)
            selection (t5.a::int = 1::int)
              scan t5
    ");
}

#[test]
fn pinned_select_without_scan() {
    insta::assert_snapshot!(explain("SELECT (SELECT count(*) FROM t5 WHERE a = 1) > 0"), @r"
    projection (ROW($0) > 0::int -> col_1)
    subquery $0:
      scan
        projection (count(*)::int -> col_1)
          selection (t5.a::int = 1::int)
            scan t5
    ");
    insta::assert_snapshot!(explain("SELECT EXISTS (SELECT * FROM t5 WHERE a = 1)"), @r"
    projection (exists ROW($0, $0) -> col_1)
    subquery $0:
      scan
        projection (t5.a::int -> a, t5.b::int -> b)
          selection (t5.a::int = 1::int)
            scan t5
    ");
    insta::assert_snapshot!(explain("SELECT (1, 2) IN (SELECT a, b FROM t5 WHERE a = 1)"), @r"
    projection (ROW(1::int, 2::int) in ROW($0, $0) -> col_1)
    subquery $0:
      scan
        projection (t5.a::int -> a, t5.b::int -> b)
          selection (t5.a::int = 1::int)
            scan t5
    ");
}

#[test]
fn pinned_select_without_scan_with_several_subqueries() {
    insta::assert_snapshot!(explain(
        "SELECT EXISTS (SELECT 1 FROM t5 WHERE a = $1) AND (SELECT b FROM t3_2 WHERE a = $1) > 0"
    ), @r"
    projection ((exists ROW($1) and ROW($0) > 0::int) -> col_1)
    subquery $0:
      scan
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = $1::int)
            scan t3_2
    subquery $1:
      scan
        projection (1::int -> col_1)
          selection (t5.a::int = $1::int)
            scan t5
    ");
    insta::assert_snapshot!(explain(
        "SELECT (SELECT b FROM t5 WHERE a = 1), (SELECT b FROM t3_2 WHERE a = 1)"
    ), @r"
    projection (ROW($1) -> col_1, ROW($0) -> col_2)
    subquery $0:
      scan
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = 1::int)
            scan t3_2
    subquery $1:
      scan
        projection (t5.b::int -> b)
          selection (t5.a::int = 1::int)
            scan t5
    ");
    insta::assert_snapshot!(explain(
        "SELECT (SELECT b FROM global_t WHERE a = 1), (SELECT count(*) FROM t5 WHERE a = $1)"
    ), @r"
    projection (ROW($1) -> col_1, ROW($0) -> col_2)
    subquery $0:
      scan
        projection (count(*)::int -> col_1)
          selection (t5.a::int = $1::int)
            scan t5
    subquery $1:
      scan
        projection (global_t.b::int -> b)
          selection (global_t.a::int = 1::int)
            scan global_t
    ");
    // The first subquery reads no bucket if 1 and 2 route apart.
    insta::assert_snapshot!(explain(
        "SELECT (SELECT b FROM t5 WHERE a = 1 AND a = 2), (SELECT b FROM t3_2 WHERE a = 1)"
    ), @r"
    projection (ROW($1) -> col_1, ROW($0) -> col_2)
    subquery $0:
      scan
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = 1::int)
            scan t3_2
    subquery $1:
      scan
        projection (t5.b::int -> b)
          selection ((t5.a::int = 1::int and t5.a::int = 2::int))
            scan t5
    ");
}

#[test]
fn pinned_select_without_scan_under_one_row_operators() {
    insta::assert_snapshot!(explain(
        "SELECT (SELECT count(*) FROM t5 WHERE a = $1) ORDER BY 1"
    ), @r"
    projection (col_1::int)
      order by (1)
        scan
          projection (ROW($0) -> col_1)
    subquery $0:
      scan
        projection (count(*)::int -> col_1)
          selection (t5.a::int = $1::int)
            scan t5
    ");
    insta::assert_snapshot!(explain(
        "SELECT (SELECT count(*) FROM t5 WHERE a = $1) LIMIT 1"
    ), @r"
    limit 1
      projection (ROW($0) -> col_1)
    subquery $0:
      scan
        projection (count(*)::int -> col_1)
          selection (t5.a::int = $1::int)
            scan t5
    ");
    insta::assert_snapshot!(explain(
        "SELECT c + 1 FROM (SELECT (SELECT b FROM t5 WHERE a = $1) AS c) WHERE c > 0 ORDER BY 1"
    ), @r"
    projection (col_1::int)
      order by (1)
        scan
          projection (unnamed_subquery.c::int + 1::int -> col_1)
            selection (unnamed_subquery.c::int > 0::int)
              scan unnamed_subquery
                projection (ROW($0) -> c)
    subquery $0:
      scan
        projection (t5.b::int -> b)
          selection (t5.a::int = $1::int)
            scan t5
    ");
    insta::assert_snapshot!(explain(
        "SELECT count(*) FROM (SELECT (SELECT b FROM t5 WHERE a = $1) AS c) WHERE c > 0"
    ), @r"
    projection (count(*)::int -> col_1)
      selection (unnamed_subquery.c::int > 0::int)
        scan unnamed_subquery
          projection (ROW($0) -> c)
    subquery $0:
      scan
        projection (t5.b::int -> b)
          selection (t5.a::int = $1::int)
            scan t5
    ");
    insta::assert_snapshot!(explain(
        "SELECT DISTINCT c FROM (SELECT (SELECT b FROM t5 WHERE a = $1) AS c)"
    ), @r"
    projection (unnamed_subquery.c::int -> c)
      scan unnamed_subquery
        projection (ROW($0) -> c)
    subquery $0:
      scan
        projection (t5.b::int -> b)
          selection (t5.a::int = $1::int)
            scan t5
    ");
    insta::assert_snapshot!(explain(
        "SELECT c, count(*) FROM (SELECT (SELECT b FROM t5 WHERE a = $1) AS c) GROUP BY c"
    ), @r"
    projection (unnamed_subquery.c::int -> c, count(*)::int -> col_1)
      group by (unnamed_subquery.c::int)
        scan unnamed_subquery
          projection (ROW($0) -> c)
    subquery $0:
      scan
        projection (t5.b::int -> b)
          selection (t5.a::int = $1::int)
            scan t5
    ");
    insta::assert_snapshot!(explain(
        "WITH x AS (SELECT (SELECT b FROM t5 WHERE a = $1) AS c) SELECT c FROM x"
    ), @r"
    projection (x.c::int -> c)
      scan cte x($1)
    subquery $0:
      scan
        projection (t5.b::int -> b)
          selection (t5.a::int = $1::int)
            scan t5
    subquery $1:
      projection (ROW($0) -> c)
    ");
}

#[test]
fn pinned_select_without_scan_as_subquery() {
    insta::assert_snapshot!(explain(
        "SELECT * FROM t5 WHERE a = $1 AND b = (SELECT (SELECT b FROM t3_2 WHERE a = $1) + 1)"
    ), @r"
    projection (t5.a::int -> a, t5.b::int -> b)
      selection ((t5.a::int = $1::int and t5.b::int = ROW($1)))
        scan t5
    subquery $0:
      scan
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = $1::int)
            scan t3_2
    subquery $1:
      scan
        projection (ROW($0) + 1::int -> col_1)
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM global_t WHERE a = (SELECT (SELECT b FROM t5 WHERE a = $1))"
    ), @r"
    projection (global_t.a::int -> a, global_t.b::int -> b)
      selection (global_t.a::int = ROW($1))
        scan global_t
    subquery $0:
      scan
        projection (t5.b::int -> b)
          selection (t5.a::int = $1::int)
            scan t5
    subquery $1:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (ROW($0) -> col_1)
    ");
}

#[test]
fn select_without_scan_keeps_motions_for_gathered_or_global_subqueries() {
    insta::assert_snapshot!(explain(
        "SELECT (SELECT b FROM t5 ORDER BY b LIMIT 1), (SELECT b FROM t3_2 WHERE a = 1)"
    ), @r"
    projection (ROW($1) -> col_1, ROW($0) -> col_2)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = 1::int)
              scan t3_2
    subquery $1:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          limit 1
            projection (b::int)
              order by (b::int)
                motion [policy: full, program: ReshardIfNeeded]
                  limit 1
                    projection (b::int)
                      order by (b::int)
                        scan
                          projection (t5.b::int -> b)
                            scan t5
    ");
    insta::assert_snapshot!(explain("SELECT (SELECT b FROM global_t WHERE a = 1)"), @r"
    projection (ROW($0) -> col_1)
    subquery $0:
      scan
        projection (global_t.b::int -> b)
          selection (global_t.a::int = 1::int)
            scan global_t
    ");
}

#[test]
fn select_without_scan_keeps_motions_for_subquery_used_twice() {
    insta::assert_snapshot!(explain(
        "SELECT (SELECT b FROM t5 WHERE a = 1) BETWEEN 1 AND 2"
    ), @r"
    projection ((ROW($0) >= 1::int and ROW($0) <= 2::int) -> col_1)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t5.b::int -> b)
            selection (t5.a::int = 1::int)
              scan t5
    ");
}

#[test]
fn select_without_scan_keeps_motions_for_different_buckets() {
    insta::assert_snapshot!(explain(
        "SELECT (SELECT count(*) FROM t5 WHERE a = $1), (SELECT b FROM t3_2 WHERE a = $2)"
    ), @r"
    projection (ROW($1) -> col_1, ROW($0) -> col_2)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = $2::int)
              scan t3_2
    subquery $1:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (count(*)::int -> col_1)
            selection (t5.a::int = $1::int)
              scan t5
    ");
    insta::assert_snapshot!(explain(
        "SELECT (SELECT count(*) FROM t5 WHERE a = 1), (SELECT b FROM t3_2 WHERE bucket_id = 5)"
    ), @r"
    projection (ROW($1) -> col_1, ROW($0) -> col_2)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.bucket_id::int = 5::int)
              scan t3_2
    subquery $1:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (count(*)::int -> col_1)
            selection (t5.a::int = 1::int)
              scan t5
    ");
    insta::assert_snapshot!(explain(
        "SELECT (SELECT b FROM t5 WHERE a = 1), (SELECT c FROM t WHERE a = 1 AND b = 1)"
    ), @r"
    projection (ROW($1) -> col_1, ROW($0) -> col_2)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t.c::int -> c)
            selection ((t.a::int = 1::int and t.b::int = 1::int))
              scan t
    subquery $1:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t5.b::int -> b)
            selection (t5.a::int = 1::int)
              scan t5
    ");
}

#[test]
fn select_without_scan_keeps_motions_for_volatile_functions() {
    insta::assert_snapshot!(explain(
        "SELECT pico_instance_uuid(), (SELECT b FROM t5 WHERE a = 1)"
    ), @r#"
    projection (".proc_instance_uuid"()::text -> col_1, ROW($0) -> col_2)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t5.b::int -> b)
            selection (t5.a::int = 1::int)
              scan t5
    "#);
    insta::assert_snapshot!(explain(
        "SELECT (SELECT pico_instance_uuid() FROM global_t LIMIT 1), (SELECT b FROM t5 WHERE a = 1)"
    ), @r#"
    projection (ROW($1) -> col_1, ROW($0) -> col_2)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t5.b::int -> b)
            selection (t5.a::int = 1::int)
              scan t5
    subquery $1:
      scan
        limit 1
          projection (".proc_instance_uuid"()::text -> col_1)
            scan global_t
    "#);
    insta::assert_snapshot!(explain(
        "SELECT pico_instance_uuid(), c FROM (SELECT (SELECT b FROM t5 WHERE a = 1) AS c)"
    ), @r#"
    projection (".proc_instance_uuid"()::text -> col_1, unnamed_subquery.c::int -> c)
      scan unnamed_subquery
        projection (ROW($0) -> c)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t5.b::int -> b)
            selection (t5.a::int = 1::int)
              scan t5
    "#);
}

#[test]
fn select_without_scan_keeps_motions_for_global_subquery_with_motion() {
    insta::assert_snapshot!(explain(
        "SELECT (SELECT b FROM global_t WHERE a IN (SELECT a FROM t5)), \
         (SELECT count(*) FROM t5 WHERE a = $1)"
    ), @r"
    projection (ROW($2) -> col_1, ROW($1) -> col_2)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t5.a::int -> a)
            scan t5
    subquery $1:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (count(*)::int -> col_1)
            selection (t5.a::int = $1::int)
              scan t5
    subquery $2:
      scan
        projection (global_t.b::int -> b)
          selection (global_t.a::int in ROW($0))
            scan global_t
    ");
}

#[test]
fn select_without_scan_keeps_motions_outside_one_row_chains() {
    insta::assert_snapshot!(explain(
        "SELECT * FROM global_t JOIN (SELECT (SELECT count(*) FROM t5 WHERE a = $1) AS c) d ON true"
    ), @r"
    projection (global_t.a::int -> a, global_t.b::int -> b, d.c::int -> c)
      join on (true::bool)
        scan global_t
        scan d
          projection (ROW($0) -> c)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (count(*)::int -> col_1)
            selection (t5.a::int = $1::int)
              scan t5
    ");
    insta::assert_snapshot!(explain(
        "SELECT (SELECT count(*) FROM t5 WHERE a = $1) \
         UNION ALL SELECT (SELECT count(*) FROM t3_2 WHERE a = $1)"
    ), @r"
    union all
      projection (ROW($0) -> col_1)
      projection (ROW($1) -> col_1)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (count(*)::int -> col_1)
            selection (t5.a::int = $1::int)
              scan t5
    subquery $1:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (count(*)::int -> col_1)
            selection (t3_2.a::int = $1::int)
              scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT row_number() OVER () FROM (SELECT (SELECT b FROM t5 WHERE a = $1) AS c)"
    ), @r"
    projection (row_number() over () -> col_1)
      scan unnamed_subquery
        projection (ROW($0) -> c)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t5.b::int -> b)
            selection (t5.a::int = $1::int)
              scan t5
    ");
    insta::assert_snapshot!(explain(
        "SELECT c FROM (SELECT (SELECT b FROM t5 WHERE a = $1) AS c) \
         WHERE c IN (SELECT b FROM t3_2 WHERE a = $1)"
    ), @r"
    projection (unnamed_subquery.c::int -> c)
      selection (unnamed_subquery.c::int in ROW($1))
        scan unnamed_subquery
          projection (ROW($0) -> c)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t5.b::int -> b)
            selection (t5.a::int = $1::int)
              scan t5
    subquery $1:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = $1::int)
              scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "WITH x AS (SELECT (SELECT b FROM t5 WHERE a = $1) AS c) SELECT * FROM x JOIN x AS y ON true"
    ), @r"
    projection (x.c::int -> c, y.c::int -> c)
      join on (true::bool)
        scan cte x($1)
        scan cte y($1)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t5.b::int -> b)
            selection (t5.a::int = $1::int)
              scan t5
    subquery $1:
      motion [policy: full, program: ReshardIfNeeded]
        projection (ROW($0) -> c)
    ");
    insta::assert_snapshot!(explain(
        "INSERT INTO t3_2 SELECT 1, (SELECT count(*) FROM t5 WHERE a = 1)"
    ), @r"
    insert into t3_2 on conflict: fail
      motion [policy: segment([ref(col_1)]), program: ReshardIfNeeded]
        projection (1::int -> col_1, ROW($0) -> col_2)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (count(*)::int -> col_1)
            selection (t5.a::int = 1::int)
              scan t5
    ");
}

#[test]
fn window_over_single_bucket() {
    insta::assert_snapshot!(explain(
        "SELECT row_number() OVER (ORDER BY b) FROM t5 WHERE a = 1"
    ), @r"
    projection (row_number() over (order by (t5.b::int asc) ) -> col_1)
      selection (t5.a::int = 1::int)
        scan t5
    ");
    insta::assert_snapshot!(explain(
        "SELECT b, count(*), row_number() OVER () FROM t5 WHERE a = $1 GROUP BY b"
    ), @r"
    projection (t5.b::int -> b, count(*)::int -> col_1, row_number() over () -> col_2)
      group by (t5.b::int)
        selection (t5.a::int = $1::int)
          scan t5
    ");
}

#[test]
fn union_and_except_over_single_bucket() {
    insta::assert_snapshot!(explain(
        "SELECT b FROM t5 WHERE a = 1 UNION SELECT b FROM t3_2 WHERE a = 1"
    ), @r"
    union
      projection (t5.b::int -> b)
        selection (t5.a::int = 1::int)
          scan t5
      projection (t3_2.b::int -> b)
        selection (t3_2.a::int = 1::int)
          scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT b FROM t5 WHERE a = $1 UNION SELECT b FROM t3_2 WHERE a = $1"
    ), @r"
    union
      projection (t5.b::int -> b)
        selection (t5.a::int = $1::int)
          scan t5
      projection (t3_2.b::int -> b)
        selection (t3_2.a::int = $1::int)
          scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT b FROM t5 WHERE a = 1 EXCEPT SELECT b FROM t3_2 WHERE a = 1"
    ), @r"
    except
      projection (t5.b::int -> b)
        selection (t5.a::int = 1::int)
          scan t5
      projection (t3_2.b::int -> b)
        selection (t3_2.a::int = 1::int)
          scan t3_2
    ");
}

#[test]
fn window_union_and_except_over_different_buckets_keep_motions() {
    insta::assert_snapshot!(explain(
        "SELECT row_number() OVER (ORDER BY b) FROM t5 WHERE a = 1 OR a = 2"
    ), @r"
    projection (row_number() over (order by (b::int asc) ) -> col_1)
      motion [policy: full, program: ReshardIfNeeded]
        projection (t5.b::int -> b)
          selection (t5.a::int = 1::int or t5.a::int = 2::int)
            scan t5
    ");
    insta::assert_snapshot!(explain(
        "SELECT b FROM t5 WHERE a = 1 UNION SELECT b FROM t3_2 WHERE a = 2"
    ), @r"
    motion [policy: full, program: RemoveDuplicates]
      union
        projection (t5.b::int -> b)
          selection (t5.a::int = 1::int)
            scan t5
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = 2::int)
            scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT b FROM t5 WHERE a = $1 EXCEPT SELECT b FROM t3_2 WHERE a = $2"
    ), @r"
    except
      projection (t5.b::int -> b)
        selection (t5.a::int = $1::int)
          scan t5
      motion [policy: full, program: ReshardIfNeeded]
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = $2::int)
            scan t3_2
    ");
}

#[test]
fn window_union_and_except_keep_motions_outside_in_place_positions() {
    insta::assert_snapshot!(explain(
        "SELECT * FROM (SELECT b FROM t5 WHERE a = 1 UNION SELECT b FROM t3_2 WHERE a = 1) u \
         JOIN t ON u.b = t.c"
    ), @r"
    projection (u.b::int -> b, t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      join on (u.b::int = t.c::int)
        scan u
          motion [policy: full, program: RemoveDuplicates]
            union
              projection (t5.b::int -> b)
                selection (t5.a::int = 1::int)
                  scan t5
              projection (t3_2.b::int -> b)
                selection (t3_2.a::int = 1::int)
                  scan t3_2
        scan t
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM (SELECT b, row_number() OVER () AS rn FROM t5 WHERE a = 1) w \
         JOIN t ON w.b = t.c"
    ), @r"
    projection (w.b::int -> b, w.rn::int -> rn, t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      join on (w.b::int = t.c::int)
        scan w
          projection (b::int, row_number() over () -> rn)
            motion [policy: full, program: ReshardIfNeeded]
              projection (t5.b::int -> b)
                selection (t5.a::int = 1::int)
                  scan t5
        scan t
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM (SELECT b FROM t5 WHERE a = 1 EXCEPT SELECT b FROM t3_2 WHERE a = 1) u \
         JOIN t ON u.b = t.c"
    ), @r"
    projection (u.b::int -> b, t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      join on (u.b::int = t.c::int)
        scan u
          except
            projection (t5.b::int -> b)
              selection (t5.a::int = 1::int)
                scan t5
            motion [policy: full, program: ReshardIfNeeded]
              projection (t3_2.b::int -> b)
                selection (t3_2.a::int = 1::int)
                  scan t3_2
        motion [policy: full, program: ReshardIfNeeded]
          projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
            scan t
    ");
    insta::assert_snapshot!(explain(
        "SELECT b FROM t5 WHERE a = 1 UNION SELECT b FROM t3_2 WHERE a = 1 EXCEPT SELECT c FROM t"
    ), @r"
    except
      motion [policy: full, program: RemoveDuplicates]
        union
          projection (t5.b::int -> b)
            selection (t5.a::int = 1::int)
              scan t5
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = 1::int)
              scan t3_2
      motion [policy: full, program: ReshardIfNeeded]
        intersect
          motion [policy: full, program: RemoveDuplicates]
            union
              projection (t5.b::int -> b)
                selection (t5.a::int = 1::int)
                  scan t5
              projection (t3_2.b::int -> b)
                selection (t3_2.a::int = 1::int)
                  scan t3_2
          projection (t.c::int -> c)
            scan t
    ");
    insta::assert_snapshot!(explain(
        "SELECT b FROM t5 WHERE a = 1 UNION SELECT b FROM t3_2 WHERE a = 1 \
         UNION ALL SELECT a FROM global_t"
    ), @r"
    union all
      motion [policy: full, program: RemoveDuplicates]
        union
          projection (t5.b::int -> b)
            selection (t5.a::int = 1::int)
              scan t5
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = 1::int)
              scan t3_2
      projection (global_t.a::int -> a)
        scan global_t
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM (SELECT b FROM t5 WHERE a = 1 EXCEPT SELECT b FROM t3_2) u ORDER BY 1 LIMIT 1"
    ), @r"
    limit 1
      projection (b::int)
        order by (1)
          motion [policy: full, program: ReshardIfNeeded]
            limit 1
              projection (b::int)
                order by (1)
                  scan
                    projection (u.b::int -> b)
                      scan u
                        except
                          projection (t5.b::int -> b)
                            selection (t5.a::int = 1::int)
                              scan t5
                          motion [policy: full, program: ReshardIfNeeded]
                            projection (t3_2.b::int -> b)
                              scan t3_2
    ");
    // A volatile function must keep running on the router.
    insta::assert_snapshot!(explain(
        "SELECT pico_instance_uuid(), row_number() OVER (ORDER BY b) FROM t5 WHERE a = 1"
    ), @r#"
    projection (".proc_instance_uuid"()::text -> col_1, row_number() over (order by (b::int asc) ) -> col_2)
      motion [policy: full, program: ReshardIfNeeded]
        projection (t5.b::int -> b)
          selection (t5.a::int = 1::int)
            scan t5
    "#);
}

#[test]
fn window_union_and_select_without_scan_keep_motions_under_having() {
    insta::assert_snapshot!(explain(
        "SELECT b FROM t5 WHERE a = 1 GROUP BY b \
         HAVING b IN (SELECT b FROM t5 WHERE a = 2 UNION SELECT b FROM t3_2 WHERE a = 2)"
    ), @r"
    projection (t5.b::int -> b)
      having (t5.b::int in ROW($0))
        group by (t5.b::int)
          selection (t5.a::int = 1::int)
            scan t5
    subquery $0:
      scan
        motion [policy: full, program: RemoveDuplicates]
          union
            projection (t5.b::int -> b)
              selection (t5.a::int = 2::int)
                scan t5
            projection (t3_2.b::int -> b)
              selection (t3_2.a::int = 2::int)
                scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT b FROM t5 WHERE a = 1 GROUP BY b HAVING b < (SELECT (SELECT b FROM t3_2 WHERE a = 2))"
    ), @r"
    projection (t5.b::int -> b)
      having (t5.b::int < ROW($1))
        group by (t5.b::int)
          selection (t5.a::int = 1::int)
            scan t5
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = 2::int)
              scan t3_2
    subquery $1:
      scan
        projection (ROW($0) -> col_1)
    ");
    insta::assert_snapshot!(explain(
        "SELECT a FROM global_t GROUP BY a \
         HAVING a IN (SELECT row_number() OVER () FROM t5 WHERE a = 1)"
    ), @r"
    projection (global_t.a::int -> a)
      having (global_t.a::int in ROW($0))
        group by (global_t.a::int)
          scan global_t
    subquery $0:
      scan
        projection (row_number() over () -> col_1)
          motion [policy: full, program: ReshardIfNeeded]
            projection (t5.bucket_id::int -> bucket_id)
              selection (t5.a::int = 1::int)
                scan t5
    ");
}

#[test]
fn window_union_and_select_without_scan_keep_motions_in_join_condition() {
    insta::assert_snapshot!(explain(
        "SELECT * FROM t5 JOIN t3_2 ON t5.a = t3_2.a \
         AND t5.b = (SELECT (SELECT b FROM t3_2 WHERE a = 1)) + 0"
    ), @r"
    projection (t5.a::int -> a, t5.b::int -> b, t3_2.a::int -> a, t3_2.b::int -> b)
      join on ((t5.a::int = t3_2.a::int and t5.b::int = ROW($1) + 0::int))
        scan t5
        scan t3_2
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = 1::int)
              scan t3_2
    subquery $1:
      scan
        projection (ROW($0) -> col_1)
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM t5 JOIN t3_2 ON t5.a = t3_2.a \
         AND t5.b = (SELECT b FROM t3_2 WHERE a = 1 UNION SELECT b FROM t3_2 WHERE a = 1) + 0"
    ), @r"
    projection (t5.a::int -> a, t5.b::int -> b, t3_2.a::int -> a, t3_2.b::int -> b)
      join on ((t5.a::int = t3_2.a::int and t5.b::int = ROW($0) + 0::int))
        scan t5
        scan t3_2
    subquery $0:
      scan
        motion [policy: full, program: RemoveDuplicates]
          union
            projection (t3_2.b::int -> b)
              selection (t3_2.a::int = 1::int)
                scan t3_2
            projection (t3_2.b::int -> b)
              selection (t3_2.a::int = 1::int)
                scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM t5 JOIN t3_2 ON t5.a = t3_2.a \
         AND t5.b = (SELECT row_number() OVER () FROM t3_2 WHERE a = 1) + 0"
    ), @r"
    projection (t5.a::int -> a, t5.b::int -> b, t3_2.a::int -> a, t3_2.b::int -> b)
      join on ((t5.a::int = t3_2.a::int and t5.b::int = ROW($0) + 0::int))
        scan t5
        scan t3_2
    subquery $0:
      scan
        projection (row_number() over () -> col_1)
          motion [policy: full, program: ReshardIfNeeded]
            projection (t3_2.bucket_id::int -> bucket_id)
              selection (t3_2.a::int = 1::int)
                scan t3_2
    ");
}

#[test]
fn join_condition_subqueries_keep_motions_with_global_tables() {
    insta::assert_snapshot!(explain(
        "SELECT * FROM global_t g1 JOIN global_t g2 ON g1.a = g2.a \
         AND g1.b = (SELECT (SELECT b FROM t5 WHERE a = 1)) + 0"
    ), @r"
    projection (g1.a::int -> a, g1.b::int -> b, g2.a::int -> a, g2.b::int -> b)
      join on ((g1.a::int = g2.a::int and g1.b::int = ROW($1) + 0::int))
        scan global_t -> g1
        scan global_t -> g2
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t5.b::int -> b)
            selection (t5.a::int = 1::int)
              scan t5
    subquery $1:
      scan
        projection (ROW($0) -> col_1)
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM global_t g LEFT JOIN t5 ON g.a = t5.a \
         AND t5.b IN (SELECT b FROM t3_2 WHERE a = 1 UNION SELECT b FROM t3_2 WHERE a = 1)"
    ), @r"
    projection (unnamed_join.a::int -> a, unnamed_join.b::int -> b, unnamed_join.a::int -> a, unnamed_join.b::int -> b)
      motion [policy: full, program: AddMissingRowsForLeftJoin]
        projection (g.a::int -> a, g.b::int -> b, t5.bucket_id::int -> bucket_id, t5.a::int -> a, t5.b::int -> b)
          join on ((g.a::int = t5.a::int and t5.b::int in ROW($0)))
            motion [policy: full, program: ReshardIfNeeded]
              projection (g.a::int -> a, g.b::int -> b)
                scan global_t -> g
            scan t5
    subquery $0:
      scan
        motion [policy: full, program: RemoveDuplicates]
          union
            projection (t3_2.b::int -> b)
              selection (t3_2.a::int = 1::int)
                scan t3_2
            projection (t3_2.b::int -> b)
              selection (t3_2.a::int = 1::int)
                scan t3_2
    ");
}

#[test]
fn except_over_a_scalar_aggregate_keeps_motion() {
    insta::assert_snapshot!(explain(
        "SELECT count(*) FROM t5 WHERE a = $1 AND a = $2 \
         EXCEPT SELECT b FROM t3_2 WHERE a = $1"
    ), @r"
    except
      projection (count(*)::int -> col_1)
        selection ((t5.a::int = $1::int and t5.a::int = $2::int))
          scan t5
      motion [policy: full, program: ReshardIfNeeded]
        projection (t3_2.b::int -> b)
          selection (t3_2.a::int = $1::int)
            scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT count(*) FROM t5 WHERE false EXCEPT SELECT count(*) FROM t3_2 WHERE a = 1"
    ), @r"
    except
      projection (count(*)::int -> col_1)
        selection (false::bool)
          scan t5
      motion [policy: full, program: ReshardIfNeeded]
        projection (count(*)::int -> col_1)
          selection (t3_2.a::int = 1::int)
            scan t3_2
    ");
}

#[test]
fn window_union_and_except_under_one_row_operators() {
    insta::assert_snapshot!(explain(
        "SELECT count(*) FROM (SELECT b FROM t5 WHERE a = $1 UNION SELECT b FROM t3_2 WHERE a = $1) u"
    ), @r"
    projection (count(*)::int -> col_1)
      scan u
        union
          projection (t5.b::int -> b)
            selection (t5.a::int = $1::int)
              scan t5
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = $1::int)
              scan t3_2
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM (SELECT b, row_number() OVER (ORDER BY b) AS rn FROM t5 WHERE a = 1) w \
         WHERE rn > 1 ORDER BY rn LIMIT 1"
    ), @r"
    limit 1
      projection (b::int, rn::int)
        order by (rn::int)
          scan
            projection (w.b::int -> b, w.rn::int -> rn)
              selection (w.rn::int > 1::int)
                scan w
                  projection (t5.b::int -> b, row_number() over (order by (t5.b::int asc) ) -> rn)
                    selection (t5.a::int = 1::int)
                      scan t5
    ");
    insta::assert_snapshot!(explain(
        "SELECT * FROM t5 WHERE a = 1 AND b IN \
         (SELECT b FROM t5 WHERE a = 1 EXCEPT SELECT b FROM t3_2 WHERE a = 1)"
    ), @r"
    projection (t5.a::int -> a, t5.b::int -> b)
      selection ((t5.a::int = 1::int and t5.b::int in ROW($0)))
        scan t5
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          except
            projection (t5.b::int -> b)
              selection (t5.a::int = 1::int)
                scan t5
            projection (t3_2.b::int -> b)
              selection (t3_2.a::int = 1::int)
                scan t3_2
    ");
}

#[test]
fn cte_over_union_used_twice_shares_one_motion() {
    // The second reference must read the motion of the first, not run the
    // UNION again.
    insta::assert_snapshot!(explain(
        "WITH c AS (SELECT b FROM t5 WHERE a = 1 UNION SELECT b FROM t3_2 WHERE a = 1) \
         SELECT * FROM c UNION ALL SELECT * FROM c"
    ), @r"
    union all
      projection (c.b::int -> b)
        scan cte c($0)
      projection (c.b::int -> b)
        scan cte c($0)
    subquery $0:
      motion [policy: full, program: RemoveDuplicates]
        union
          projection (t5.b::int -> b)
            selection (t5.a::int = 1::int)
              scan t5
          projection (t3_2.b::int -> b)
            selection (t3_2.a::int = 1::int)
              scan t3_2
    ");
}
