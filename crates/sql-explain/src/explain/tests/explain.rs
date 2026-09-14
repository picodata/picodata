use super::{explain, explain_with_params};
use sql_ir::ir::value::Value;

// Facet headers.

#[test]
fn logical_and_buckets_headers() {
    let sql = r#"EXPLAIN (LOGICAL, BUCKETS) SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t""#;
    insta::assert_snapshot!(explain(sql), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    projection (t.identification_number::int -> c1, t.product_code::string -> product_code)
      scan hash_testing -> t

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets <= [1-10000]
    ");
}

#[test]
fn verbose_alone_selects_default_facets() {
    let sql = r#"explain (verbose) select e from t2
        where e = 1 and f in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)"#;
    insta::assert_snapshot!(explain(sql), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    projection (t2.e::int -> e)
      selection ((t2.e::int = 1::int and t2.f::int in ROW(1::int, 2::int, 3::int, 4::int, 5::int, 6::int, 7::int, 8::int, 9::int, 10::int)))
        scan t2

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets = [100, 550, 1077, 1098, 3485, 5930, 6691, 7479, 7602, 8577]
    ");
}

// Select.

#[test]
fn select_from_global_table() {
    let sql = r#"explain (logical, buckets) select a from global_t"#;
    insta::assert_snapshot!(explain(sql), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    projection (global_t.a::int -> a)
      scan global_t

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets = any
    ");
}

#[test]
fn select_with_composite_key_filter() {
    let sql = r#"explain (logical, buckets) select e from t2 where e = 1 and f = 13"#;
    insta::assert_snapshot!(explain(sql), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    projection (t2.e::int -> e)
      selection ((t2.e::int = 1::int and t2.f::int = 13::int))
        scan t2

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets = [111]
    ");
}

#[test]
fn select_with_contradicting_row_filters() {
    let sql = r#"explain (logical, buckets) select a, b from t1 where (a, b) = ('1', 1) and (a, b) = ('2', 2)"#;
    insta::assert_snapshot!(explain(sql), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    projection (t1.a::string -> a, t1.b::int -> b)
      selection ((ROW(t1.a::string, t1.b::int) = ROW('1'::string, 1::int) and ROW(t1.a::string, t1.b::int) = ROW('2'::string, 2::int)))
        scan t1

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets = []
    ");
}

// Set operations.

#[test]
fn union_all() {
    let sql = r#"explain (logical, buckets) SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t"
        UNION ALL
        SELECT "t2"."identification_number", "product_code" FROM "hash_testing_hist" as "t2""#;
    insta::assert_snapshot!(explain(sql), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    union all
      projection (t.identification_number::int -> c1, t.product_code::string -> product_code)
        scan hash_testing -> t
      projection (t2.identification_number::int -> identification_number, t2.product_code::string -> product_code)
        scan hash_testing_hist -> t2

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets <= [1-10000]
    ");
}

// Joins.

/// Table t2 is sharded by (e, f), t1 by (a, b). The join has Segment {e, f}
/// vs Segment {a, b}: they don't equal each other, so motion is needed. We
/// know e = f = 10 and e = b, hence by transitivity e = f = b, so the motion
/// key is derived as segment([b, b]).
///
/// Buckets can't be estimated after a segment motion reshards the rows,
/// so only the upper bound is shown.
#[test]
fn segment_motion_key_derived_by_transitivity() {
    let sql = r#"explain (logical, buckets) select a, count(b) from
    (select e, f from t2 where (e, f) = (10, 10))
    join
    (select a, b from t1 where (a, b) = ('20', 20))
    on e = b
    group by a
"#;
    insta::assert_snapshot!(explain(sql), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    projection (gr_expr_1::string -> a, sum(count_1::int)::int -> col_1)
      group by (gr_expr_1::string)
        motion [policy: full, program: ReshardIfNeeded]
          projection (unnamed_subquery_1.a::string -> gr_expr_1, count(unnamed_subquery_1.b::int::int)::int -> count_1)
            group by (unnamed_subquery_1.a::string)
              join on (unnamed_subquery.e::int = unnamed_subquery_1.b::int)
                scan unnamed_subquery
                  projection (t2.e::int -> e, t2.f::int -> f)
                    selection (ROW(t2.e::int, t2.f::int) = ROW(10::int, 10::int))
                      scan t2
                motion [policy: segment([ref(b), ref(b)]), program: ReshardIfNeeded]
                  scan unnamed_subquery_1
                    projection (t1.a::string -> a, t1.b::int -> b)
                      selection (ROW(t1.a::string, t1.b::int) = ROW('20'::string, 20::int))
                        scan t1

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets <= [1-10000]
    ");
}

/// Table t2 is sharded by (e, f), t1 by (a, b). The join has Segment {e, f}
/// vs Segment {a, b}: they don't equal each other, so motion is needed. We
/// only know e = b, which doesn't cover t2's sharding key, so no new segment
/// key can be derived and the motion is full.
///
/// The plan has no segment motion, so the exact bucket set is estimated.
#[test]
fn join_with_full_motion() {
    let sql = r#"explain (logical, buckets) select a from
    (select e, f from t2 where (e, f) = (10, 12))
    join
    (select a, b from t1 where (a, b) = ('20', 20))
    on e = b
"#;
    insta::assert_snapshot!(explain(sql), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    projection (unnamed_subquery_1.a::string -> a)
      join on (unnamed_subquery.e::int = unnamed_subquery_1.b::int)
        scan unnamed_subquery
          projection (t2.e::int -> e, t2.f::int -> f)
            selection (ROW(t2.e::int, t2.f::int) = ROW(10::int, 12::int))
              scan t2
        motion [policy: full, program: ReshardIfNeeded]
          scan unnamed_subquery_1
            projection (t1.a::string -> a, t1.b::int -> b)
              selection (ROW(t1.a::string, t1.b::int) = ROW('20'::string, 20::int))
                scan t1

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets = [62, 6266]
    ");
}

// DML.

/// Source and target are sharded by the same key, so rows stay on their
/// storages: local segment motion only recalculates `bucket_id`.
#[test]
fn insert_select_uses_local_segment_motion() {
    let sql = r#"explain (logical, buckets) insert into t1 select a, b from t1"#;
    insta::assert_snapshot!(explain(sql), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    insert into t1 on conflict: fail
      motion [policy: local segment([ref(a), ref(b)]), program: ReshardIfNeeded]
        projection (t1.a::string -> a, t1.b::int -> b)
          scan t1

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets <= [1-10000]
    ");
}

#[test]
fn insert_into_global_table_uses_full_motion() {
    let sql = r#"explain (logical, buckets) insert into global_t values (1, 1)"#;
    insta::assert_snapshot!(explain(sql), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    insert into global_t on conflict: fail
      motion [policy: full, program: ReshardIfNeeded]
        values
          value ROW(1::int, 1::int)

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets = any
    ");
}

#[test]
fn insert_select_into_global_table_uses_full_motion() {
    let sql = r#"explain (logical, buckets) insert into global_t select a, b from t1 where (a, b) = ('1', 1)"#;
    insta::assert_snapshot!(explain(sql), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    insert into global_t on conflict: fail
      motion [policy: full, program: ReshardIfNeeded]
        projection (t1.a::string -> a, t1.b::int -> b)
          selection (ROW(t1.a::string, t1.b::int) = ROW('1'::string, 1::int))
            scan t1

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets = [6691]
    ");
}

/// Updating a sharding-key column is executed as delete + insert. The
/// projection holds the new row followed by the old sharding key (col_5,
/// col_6), and the motion program splits every row into a delete tuple
/// routed by the old key and an insert tuple routed by the new key at
/// positions 0 and 1.
#[test]
fn update_of_sharding_key_rearranges_rows() {
    let sql = r#"explain (logical, buckets) update t2 set e = 20 where (e, f) = (10, 10)"#;
    insta::assert_snapshot!(explain(sql), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    update t2 (f = col_1, h = col_3, bucket_id = col_4, e = col_0, g = col_2)
      motion [policy: segment([]), program: [PrimaryKey(2, 3), RearrangeForShardedUpdate(0, 1)]]
        projection (20::int -> col_0, t2.f::int -> col_1, t2.g::int -> col_2, t2.h::int -> col_3, t2.bucket_id::int -> col_4, t2.e::int -> col_5, t2.f::int -> col_6)
          selection (ROW(t2.e::int, t2.f::int) = ROW(10::int, 10::int))
            scan t2

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets <= [1-10000]
    ");
}

// Casts.

#[test]
fn constant_casts_folded_in_insert() {
    let sql = r#"explain (logical, buckets) INSERT INTO t1 VALUES ('txt'::text::text::text, 2::decimal::integer::double::integer)"#;
    insta::assert_snapshot!(explain(sql), @r#"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    insert into t1 on conflict: fail
      motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")]), program: ReshardIfNeeded]
        values
          value ROW('txt'::string, 2::int)

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets = [369]
    "#);
}

#[test]
fn constant_casts_folded_in_select() {
    let sql = r#"explain (logical, buckets) SELECT * FROM t3 WHERE a = 'kek'::text::text::text"#;
    insta::assert_snapshot!(explain(sql), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    projection (t3.a::string -> a, t3.b::int -> b)
      selection (t3.a::string = 'kek'::string)
        scan t3

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets = [1610]
    ");
}

#[test]
fn constant_casts_folded_in_update() {
    let sql = r#"explain (logical, buckets) UPDATE t SET c = 2 WHERE a = 1::int::int and b = 2::integer::decimal"#;
    insta::assert_snapshot!(explain(sql), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    update t (c = col_0)
      motion [policy: local, program: ReshardIfNeeded]
        projection (2::int -> col_0, t.b::int -> col_1)
          selection ((t.a::int = 1::int and t.b::int = 2::decimal))
            scan t

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets = [550]
    ");
}

#[test]
fn constant_casts_folded_in_delete() {
    let sql = r#"explain (logical, buckets) DELETE FROM "t2" where "e" = 3::integer and "f" = 2::decimal"#;
    insta::assert_snapshot!(explain(sql), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    delete from t2
      motion [policy: local, program: [PrimaryKey(0, 1), ReshardIfNeeded]]
        projection (t2.g::int -> pk_col_0, t2.h::int -> pk_col_1)
          selection ((t2.e::int = 3::int and t2.f::int = 2::decimal))
            scan t2

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets = [9374]
    ");
}

// Prepared statements.

#[test]
fn prepared_single_key_aggregate_stays_single_node() {
    let sql = r#"explain (logical, buckets) select count(*)
        from t5
        where a = $1"#;
    let params = vec![Value::Integer(1)];
    insta::assert_snapshot!(explain_with_params(sql, params), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    projection (count(*)::int -> col_1)
      selection (t5.a::int = 1::int)
        scan t5

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets = [3940]
    ");
}

#[test]
fn prepared_single_key_with_constant_drops_reduce_stage() {
    let sql = r#"explain (logical, buckets) select count(*)
        from t5
        where a = $1 and a = 1"#;
    let params = vec![Value::Integer(1)];
    insta::assert_snapshot!(explain_with_params(sql, params), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    projection (count(*)::int -> col_1)
      selection ((t5.a::int = 1::int and t5.a::int = 1::int))
        scan t5

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets = [3940]
    ");
}

#[test]
fn prepared_reused_parameters_drops_reduce_stage() {
    let sql = r#"explain (logical, buckets) select count(*)
        from t5
        where a = $1 and a = $1 and a = $2"#;
    let params = vec![Value::Integer(1), Value::Integer(1)];
    insta::assert_snapshot!(explain_with_params(sql, params), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    projection (count(*)::int -> col_1)
      selection ((t5.a::int = 1::int and t5.a::int = 1::int and t5.a::int = 1::int))
        scan t5

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets = [3940]
    ");
}

#[test]
fn prepared_partial_composite_key_keeps_reduce_stage() {
    let sql = r#"explain (logical, buckets) select count(*)
        from "hash_testing"
        where ("identification_number", "product_code") = ($1, trim("product_code"))"#;
    let params = vec![Value::Integer(1)];
    insta::assert_snapshot!(explain_with_params(sql, params), @r"
    ──────────────────────────────────────────────────────────────────────
     # Logical plan                                                       
    ──────────────────────────────────────────────────────────────────────

    projection (sum(count_1::int)::int -> col_1)
      motion [policy: full, program: ReshardIfNeeded]
        projection (count(*)::int -> count_1)
          selection (ROW(hash_testing.identification_number::int, hash_testing.product_code::string) = ROW(1::int, TRIM(hash_testing.product_code::string::string)))
            scan hash_testing

    ──────────────────────────────────────────────────────────────────────
     # Buckets                                                            
    ──────────────────────────────────────────────────────────────────────

    buckets <= [1-10000]
    ");
}
