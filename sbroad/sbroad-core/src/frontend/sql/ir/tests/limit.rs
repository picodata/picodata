use crate::ir::transformation::helpers::sql_to_optimized_ir;

#[test]
fn select() {
    let sql = r#"SELECT "id" FROM "test_space" LIMIT 100"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    limit 100
        motion [policy: full]
            limit 100
                projection ("test_space"."id"::int -> "id")
                    scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn union_all() {
    let sql = r#"
    select "product_code" from "hash_testing"
    union all
    select "e" from "t2"
    limit 100
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    limit 100
        motion [policy: full]
            limit 100
                union all
                    projection ("hash_testing"."product_code"::string -> "product_code")
                        scan "hash_testing"
                    projection ("t2"."e"::int -> "e")
                        scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn aggregate() {
    let input = r#"SELECT min("b"), min(distinct "b") FROM "t" LIMIT 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    limit 1
        projection (min(("min_1"::int))::int -> "col_1", min(distinct ("gr_expr_1"::int))::int -> "col_2")
            motion [policy: full]
                projection ("t"."b"::int -> "gr_expr_1", min(("t"."b"::int))::int -> "min_1")
                    group by ("t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn group_by() {
    let input = r#"SELECT cOuNt(*), "b" FROM "t" group by "b" limit 555"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    limit 555
        projection (sum(("count_1"::int))::int -> "col_1", "gr_expr_1"::int -> "b")
            group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "count_1"::int -> "count_1")
                motion [policy: full]
                    projection ("t"."b"::int -> "gr_expr_1", count((*::int))::int -> "count_1")
                        group by ("t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                            scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn single_limit() {
    let sql = r#"SELECT * FROM (SELECT "id" FROM "test_space" LIMIT 1) LIMIT 1"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    limit 1
        projection ("unnamed_subquery"."id"::int -> "id")
            scan "unnamed_subquery"
                limit 1
                    motion [policy: full]
                        limit 1
                            projection ("test_space"."id"::int -> "id")
                                scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn join() {
    let input = r#"SELECT * FROM "t1" LEFT JOIN "t2" ON "t1"."b" = "t2"."e"
    JOIN "t3" ON "t1"."a" = "t3"."a" JOIN "t4" ON "t2"."f" = "t4"."d"
    LIMIT 128
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    limit 128
        motion [policy: full]
            limit 128
                projection ("t1"."a"::string -> "a", "t1"."b"::int -> "b", "t2"."e"::int -> "e", "t2"."f"::int -> "f", "t2"."g"::int -> "g", "t2"."h"::int -> "h", "t3"."a"::string -> "a", "t3"."b"::int -> "b", "t4"."c"::string -> "c", "t4"."d"::int -> "d")
                    join on "t2"."f"::int = "t4"."d"::int
                        join on "t1"."a"::string = "t3"."a"::string
                            left join on "t1"."b"::int = "t2"."e"::int
                                scan "t1"
                                motion [policy: full]
                                    projection ("t2"."e"::int -> "e", "t2"."f"::int -> "f", "t2"."g"::int -> "g", "t2"."h"::int -> "h", "t2"."bucket_id"::int -> "bucket_id")
                                        scan "t2"
                            motion [policy: full]
                                projection ("t3"."bucket_id"::int -> "bucket_id", "t3"."a"::string -> "a", "t3"."b"::int -> "b")
                                    scan "t3"
                        motion [policy: full]
                            projection ("t4"."bucket_id"::int -> "bucket_id", "t4"."c"::string -> "c", "t4"."d"::int -> "d")
                                scan "t4"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn limit_all() {
    let sql = r#"SELECT "id" FROM "test_space" LIMIT ALL"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::int -> "id")
        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn limit_null() {
    let sql = r#"SELECT "id" FROM "test_space" LIMIT NULL"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::int -> "id")
        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
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
        motion [policy: full]
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
    motion [policy: full]
                    projection ("test_space"."bucket_id"::int -> "bucket_id", "test_space"."id"::int -> "id")
                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}
