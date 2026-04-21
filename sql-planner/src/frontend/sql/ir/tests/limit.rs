use crate::frontend::sql::ir::tests::RouterConfigurationMock;
use crate::frontend::sql::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::transformation::helpers::sql_to_optimized_ir;

#[test]
fn select() {
    let sql = r#"SELECT "id" FROM "test_space" LIMIT 100"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 100
      motion [policy: full, program: ReshardIfNeeded]
        limit 100
          projection (test_space.id::int -> id)
            scan test_space

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn union_all() {
    let sql = r#"
    select "product_code" from "hash_testing"
    union all
    select "e"::text from "t2"
    limit 100
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 100
      motion [policy: full, program: ReshardIfNeeded]
        limit 100
          union all
            projection (hash_testing.product_code::string -> product_code)
              scan hash_testing
            projection (t2.e::int::string -> col_1)
              scan t2

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn aggregate() {
    let input = r#"SELECT min("b"), min(distinct "b") FROM "t" LIMIT 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 1
      projection (min(min_1::int)::int -> col_1, min(distinct gr_expr_1::int)::int -> col_2)
        motion [policy: full, program: ReshardIfNeeded]
          projection (t.b::int::int -> gr_expr_1, min(t.b::int::int)::int -> min_1)
            group by (t.b::int::int) output (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              scan t

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn group_by() {
    let input = r#"SELECT count(*), "b" FROM "t" group by "b" limit 555"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 555
      projection (sum(count_1::int)::int -> col_1, gr_expr_1::int -> b)
        group by (gr_expr_1::int) output (gr_expr_1::int, count_1::int)
          motion [policy: full, program: ReshardIfNeeded]
            projection (t.b::int -> gr_expr_1, count(*)::int -> count_1)
              group by (t.b::int) output (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
                scan t

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn single_limit() {
    let sql = r#"SELECT * FROM (SELECT "id" FROM "test_space" LIMIT 1) LIMIT 1"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 1
      projection (unnamed_subquery.id::int -> id)
        scan unnamed_subquery
          limit 1
            motion [policy: full, program: ReshardIfNeeded]
              limit 1
                projection (test_space.id::int -> id)
                  scan test_space

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn join() {
    let input = r#"SELECT * FROM "t1" LEFT JOIN "t2" ON "t1"."b" = "t2"."e"
    JOIN "t3" ON "t1"."a" = "t3"."a" JOIN "t4" ON "t2"."f" = "t4"."d"
    LIMIT 128
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 128
      motion [policy: full, program: ReshardIfNeeded]
        limit 128
          projection (t1.a::string -> a, t1.b::int -> b, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t3.a::string -> a, t3.b::int -> b, t4.c::string -> c, t4.d::int -> d)
            join on (t2.f::int = t4.d::int)
              join on (t1.a::string = t3.a::string)
                left join on (t1.b::int = t2.e::int)
                  scan t1
                  motion [policy: full, program: ReshardIfNeeded]
                    projection (t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t2.bucket_id::int -> bucket_id)
                      scan t2
                motion [policy: full, program: ReshardIfNeeded]
                  projection (t3.bucket_id::int -> bucket_id, t3.a::string -> a, t3.b::int -> b)
                    scan t3
              motion [policy: full, program: ReshardIfNeeded]
                projection (t4.bucket_id::int -> bucket_id, t4.c::string -> c, t4.d::int -> d)
                  scan t4

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn limit_all() {
    let sql = r#"SELECT "id" FROM "test_space" LIMIT ALL"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    projection (test_space.id::int -> id)
      scan test_space

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn limit_null() {
    let sql = r#"SELECT "id" FROM "test_space" LIMIT NULL"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    projection (test_space.id::int -> id)
      scan test_space

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn explicit_select_bucket_id_from_subquery_under_limit() {
    let input = r#"select * from (
                            select "test_space"."bucket_id" as "bucket_id",
                                   "test_space"."id" as "id"
                            from "test_space"
                        ) x limit 1;"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 1
      motion [policy: full, program: ReshardIfNeeded]
        limit 1
          projection (x.bucket_id::int -> bucket_id, x.id::int -> id)
            scan x
              projection (test_space.bucket_id::int -> bucket_id, test_space.id::int -> id)
                scan test_space

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
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

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 1
      projection (x.bucket_id::int -> bucket_id, x.id::int -> id)
        scan cte x($0)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        projection (test_space.bucket_id::int -> bucket_id, test_space.id::int -> id)
          scan test_space

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

fn has_direct_child_limit_under_motion(explain: &str, limit: u64) -> bool {
    let expected = format!("limit {limit}");
    let lines: Vec<_> = explain.lines().collect();

    lines.windows(2).any(|pair| {
        pair[0].trim_start().starts_with("motion [policy: full") && pair[1].trim() == expected
    })
}

#[test]
fn limit_pushdown_having() {
    let input = r#"SELECT count(*), "b" FROM "t" GROUP BY "b" HAVING count(*) > 0 LIMIT 3"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let explain = plan.as_explain().unwrap();

    assert!(
        !has_direct_child_limit_under_motion(&explain, 3),
        "unexpected pushdown for GROUP BY + HAVING:\n{explain}"
    );
}

#[test]
fn limit_pushdown_window() {
    let input = r#"SELECT count(*) OVER () AS "c" FROM "t" LIMIT 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 1
      projection (count(*) over () -> c)
        motion [policy: full, program: ReshardIfNeeded]
          projection (t.a::int -> a)
            scan t

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn limit_pushdown_with_aggregate_in_order_by_alias() {
    let sql = r#"
        select sum(b) as s, b from t group by b order by s limit 5;
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    let explain = plan.as_explain().unwrap();

    assert!(
        !has_direct_child_limit_under_motion(&explain, 5),
        "unexpected pushdown for aggregate in ORDER BY alias:\n{explain}"
    );
}

#[test]
fn limit_pushdown_with_aggregate_in_order_by_position() {
    let sql = r#"
        select sum(b) as s, b from t group by b order by 1 limit 5;
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    let explain = plan.as_explain().unwrap();

    assert!(
        !has_direct_child_limit_under_motion(&explain, 5),
        "unexpected pushdown for aggregate in ORDER BY position:\n{explain}"
    );
}

#[test]
fn limit_pushdown_order_by_position() {
    let sql = r#"SELECT "a", "b" FROM "t" ORDER BY 2 LIMIT 5"#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    let explain = plan.as_explain().unwrap();

    assert!(
        has_direct_child_limit_under_motion(&explain, 5),
        "expected pushdown for ORDER BY position:\n{explain}"
    );
}

#[test]
fn limit_pushdown_order_by_alias() {
    let sql = r#"SELECT "b" AS "x" FROM "t" ORDER BY "x" LIMIT 5"#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    let explain = plan.as_explain().unwrap();

    assert!(
        has_direct_child_limit_under_motion(&explain, 5),
        "expected pushdown for ORDER BY alias:\n{explain}"
    );
}

#[test]
fn limit_pushdown_distinct_order_by_alias() {
    let sql = r#"
        SELECT DISTINCT "a" AS "x" FROM "t" ORDER BY "x" LIMIT 5;
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 5
      projection (x::int)
        order by (x::int)
          scan
            projection (gr_expr_1::int -> x)
              group by (gr_expr_1::int) output (gr_expr_1::int)
                motion [policy: full, program: ReshardIfNeeded]
                  limit 5
                    projection (gr_expr_1::int)
                      order by (gr_expr_1::int)
                        scan
                          projection (t.a::int -> gr_expr_1)
                            group by (t.a::int) output (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
                              scan t

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn limit_pushdown_distinct_order_by_expr_over_duplicated_aliases() {
    let sql = r#"
        SELECT DISTINCT "a" AS "c0", "b" AS "c1", "a" AS "c2"
        FROM "t"
        ORDER BY "c0" + "c2" LIMIT 5;
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 5
      projection (c0::int, c1::int, c2::int)
        order by (c0::int + c2::int)
          scan
            projection (gr_expr_1::int -> c0, gr_expr_2::int -> c1, gr_expr_1::int -> c2)
              group by (gr_expr_1::int, gr_expr_2::int) output (gr_expr_1::int, gr_expr_2::int)
                motion [policy: full, program: ReshardIfNeeded]
                  limit 5
                    projection (gr_expr_1::int, gr_expr_2::int)
                      order by (gr_expr_1::int + gr_expr_1::int)
                        scan
                          projection (t.a::int -> gr_expr_1, t.b::int -> gr_expr_2)
                            group by (t.a::int, t.b::int) output (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
                              scan t

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn limit_pushdown_distinct_order_by_ordinal_position() {
    let sql = r#"
        SELECT DISTINCT "a" AS "c0", "b" AS "c1", "a" AS "c2"
        FROM "t"
        ORDER BY 3 DESC LIMIT 5;
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 5
      projection (c0::int, c1::int, c2::int)
        order by (3 desc)
          scan
            projection (gr_expr_1::int -> c0, gr_expr_2::int -> c1, gr_expr_1::int -> c2)
              group by (gr_expr_1::int, gr_expr_2::int) output (gr_expr_1::int, gr_expr_2::int)
                motion [policy: full, program: ReshardIfNeeded]
                  limit 5
                    projection (gr_expr_1::int, gr_expr_2::int)
                      order by (1 desc)
                        scan
                          projection (t.a::int -> gr_expr_1, t.b::int -> gr_expr_2)
                            group by (t.a::int, t.b::int) output (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
                              scan t

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn limit_pushdown_order_by_subquery_no_pushdown() {
    let sql = r#"
        SELECT "a" FROM "t" ORDER BY (SELECT 1), "a" LIMIT 5;
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 5
      projection (a::int)
        order by (ROW($0), a::int)
          motion [policy: full, program: ReshardIfNeeded]
            scan
              projection (t.a::int -> a)
                scan t
    subquery $0:
      scan
        projection (1::int -> col_1)

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn limit_pushdown_except() {
    let sql = r#"
        select a from t except select a from t where a = 1 order by a limit 1;
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 1
      projection (a::int)
        order by (a::int)
          motion [policy: full, program: ReshardIfNeeded]
            limit 1
              projection (a::int)
                order by (a::int)
                  scan
                    except
                      projection (t.a::int -> a)
                        scan t
                      motion [policy: full, program: ReshardIfNeeded]
                        projection (t.a::int -> a)
                          selection (t.a::int = 1::int)
                            scan t

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn limit_pushdown_aggregate_in_order_by() {
    let sql = r#"
        select b from t group by b order by sum(b) limit 5;
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 5
      projection (b::int)
        order by (sum(b::int::int)::decimal)
          scan
            projection (gr_expr_1::int -> b)
              group by (gr_expr_1::int) output (gr_expr_1::int)
                motion [policy: full, program: ReshardIfNeeded]
                  projection (t.b::int -> gr_expr_1)
                    group by (t.b::int) output (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
                      scan t

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn limit_pushdown_distinct() {
    let sql = r#"
        SELECT DISTINCT a, b FROM t LIMIT 5;
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 5
      projection (gr_expr_1::int -> a, gr_expr_2::int -> b)
        group by (gr_expr_1::int, gr_expr_2::int) output (gr_expr_1::int, gr_expr_2::int)
          motion [policy: full, program: ReshardIfNeeded]
            limit 5
              projection (t.a::int -> gr_expr_1, t.b::int -> gr_expr_2)
                group by (t.a::int, t.b::int) output (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
                  scan t

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn limit_pushdown_having_filter_aggregate() {
    let sql = r#"
        SELECT b FROM t GROUP BY b HAVING count(*) > 1 LIMIT 5;
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 5
      projection (gr_expr_1::int -> b)
        having (sum(count_1::int)::int > 1::int)
          group by (gr_expr_1::int) output (gr_expr_1::int, count_1::int)
            motion [policy: full, program: ReshardIfNeeded]
              projection (t.b::int -> gr_expr_1, count(*)::int -> count_1)
                group by (t.b::int) output (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
                  scan t

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn limit_pushdown_orderby_and_having() {
    let sql = r#"
        SELECT b FROM t GROUP BY b HAVING b > 1 ORDER BY b LIMIT 5;
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    limit 5
      projection (b::int)
        order by (b::int)
          scan
            projection (gr_expr_1::int -> b)
              having (gr_expr_1::int > 1::int)
                group by (gr_expr_1::int) output (gr_expr_1::int)
                  motion [policy: full, program: ReshardIfNeeded]
                    projection (t.b::int -> gr_expr_1)
                      group by (t.b::int) output (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
                        scan t

    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn no_limit_pushdown_with_volatile_funcs() {
    // Verify skipping limit pushdown for volatile functions
    // in OrderBy exprs (when it will be allowed)
    let sql = r#"
        SELECT b FROM t GROUP BY b ORDER BY pico_instance_uuid() LIMIT 5;
    "#;
    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(sql, &vec![], metadata);

    assert!(matches!(plan, Err(_)));

    // insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"..."#);
}
