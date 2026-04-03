use crate::ir::transformation::helpers::sql_to_optimized_ir;

#[test]
fn front_select_chaning_1() {
    let input = r#"
    select "product_code" from "hash_testing"
    union all
    select "e"::text from "t2"
    union all
    select "a" from "t3"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @"
    union all
      union all
        projection (hash_testing.product_code::string -> product_code)
          scan hash_testing
        projection (t2.e::int::string -> col_1)
          scan t2
      projection (t3.a::string -> a)
        scan t3
    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn front_select_chaining_2() {
    let input = r#"
    select "product_code" from "hash_testing"
    union all
    select "e"::text from "t2"
    union
    select "a" from "t3"
    except
    select "b"::text from "t3"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @"
    except
      motion [policy: full, program: RemoveDuplicates]
        union
          union all
            projection (hash_testing.product_code::string -> product_code)
              scan hash_testing
            projection (t2.e::int::string -> col_1)
              scan t2
          projection (t3.a::string -> a)
            scan t3
      motion [policy: full, program: ReshardIfNeeded]
        intersect
          projection (t3.b::int::string -> col_1)
            scan t3
          motion [policy: full, program: RemoveDuplicates]
            union
              union all
                projection (hash_testing.product_code::string -> product_code)
                  scan hash_testing
                projection (t2.e::int::string -> col_1)
                  scan t2
              projection (t3.a::string -> a)
                scan t3
    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn front_select_chaining_3() {
    let input = r#"
    select "product_code" from "hash_testing"
    union all
    select "e"::text from "t2"
    order by 1
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @"
    projection (product_code::string)
      order by (1)
        motion [policy: full, program: ReshardIfNeeded]
          scan
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
fn union_under_insert() {
    let input = r#"
    insert into t2
    select e, f, 1, 1 from t2
    union
    select f, e, 2, 2 from t2
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @"
    insert into t2 on conflict: fail
      motion [policy: segment([ref(e), ref(f)]), program: [RemoveDuplicates, ReshardIfNeeded]]
        union
          projection (t2.e::int -> e, t2.f::int -> f, 1::int -> col_1, 1::int -> col_2)
            scan t2
          projection (t2.f::int -> f, t2.e::int -> e, 2::int -> col_1, 2::int -> col_2)
            scan t2
    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn union_under_insert1() {
    let input = r#"
    insert into "TBL"
    select * from (values (1, 1))
    union
    select * from (values (2, 2))
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert into "TBL" on conflict: fail
      motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")]), program: [RemoveDuplicates, ReshardIfNeeded]]
        union
          projection (unnamed_subquery."COLUMN_1"::int -> "COLUMN_1", unnamed_subquery."COLUMN_2"::int -> "COLUMN_2")
            scan unnamed_subquery
              motion [policy: full, program: ReshardIfNeeded]
                values
                  value ROW(1::int, 1::int)
          projection (unnamed_subquery_1."COLUMN_1"::int -> "COLUMN_1", unnamed_subquery_1."COLUMN_2"::int -> "COLUMN_2")
            scan unnamed_subquery_1
              motion [policy: full, program: ReshardIfNeeded]
                values
                  value ROW(2::int, 2::int)
    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    "#);
}

#[test]
fn limit_pushdown_with_union() {
    let sql = r#"
        select * from t union select * from t order by a limit 1;
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @"
    limit 1
      projection (a::int, b::int, c::int, d::int)
        order by (a::int)
          scan
            motion [policy: full, program: RemoveDuplicates]
              limit 1
                projection (a::int, b::int, c::int, d::int)
                  order by (a::int)
                    scan
                      union
                        projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
                          scan t
                        projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
                          scan t
    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");

    let sql = r#"
        select * from t union all select * from t order by a limit 1;
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @"
    limit 1
      projection (a::int, b::int, c::int, d::int)
        order by (a::int)
          motion [policy: full, program: ReshardIfNeeded]
            limit 1
              projection (a::int, b::int, c::int, d::int)
                order by (a::int)
                  scan
                    union all
                      projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
                        scan t
                      projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
                        scan t
    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}

#[test]
fn limit_pushdown_with_union_and_group_by() {
    let sql = r#"
        select a as a1, a as a2 from t union all select a as a1, a as a2 from t group by a1, a2 order by a1 limit 1;
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @"
    limit 1
      projection (a1::int, a2::int)
        order by (a1::int)
          motion [policy: full, program: ReshardIfNeeded]
            limit 1
              projection (a1::int, a2::int)
                order by (a1::int)
                  scan
                    union all
                      projection (t.a::int -> a1, t.a::int -> a2)
                        scan t
                      motion [policy: segment([ref(a1)]), program: ReshardIfNeeded]
                        projection (gr_expr_1::int -> a1, gr_expr_1::int -> a2)
                          group by (gr_expr_1::int) output (gr_expr_1::int)
                            motion [policy: full, program: ReshardIfNeeded]
                              projection (t.a::int -> gr_expr_1)
                                group by (t.a::int) output (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
                                  scan t
    execution options:
      sql_vdbe_opcode_max = 45000
      sql_motion_row_max = 5000
    ");
}
