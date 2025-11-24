use pretty_assertions::assert_eq;

use super::*;
use crate::{collection, ir::transformation::helpers::sql_to_optimized_ir};

#[test]
fn simple_query_without_cond_plan() {
    let query =
        r#"SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection ("t"."identification_number"::int -> "c1", "t"."product_code"::string -> "product_code")
        scan "hash_testing" -> "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn simple_query_with_cond_plan() {
    let query = r#"SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t" WHERE "t"."identification_number" = 1 AND "t"."product_code" = '222'"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection ("t"."identification_number"::int -> "c1", "t"."product_code"::string -> "product_code")
        selection ("t"."identification_number"::int = 1::int) and ("t"."product_code"::string = '222'::string)
            scan "hash_testing" -> "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn union_query_plan() {
    let query = r#"SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t"
        UNION ALL
        SELECT "t2"."identification_number", "product_code" FROM "hash_testing_hist" as "t2""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let expected = format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
        r#"union all"#,
        r#"    projection ("t"."identification_number"::int -> "c1", "t"."product_code"::string -> "product_code")"#,
        r#"        scan "hash_testing" -> "t""#,
        r#"    projection ("t2"."identification_number"::int -> "identification_number", "t2"."product_code"::string -> "product_code")"#,
        r#"        scan "hash_testing_hist" -> "t2""#,
        r#"execution options:"#,
        r#"    sql_vdbe_opcode_max = 45000"#,
        r#"    sql_motion_row_max = 5000"#,
    );
    assert_eq!(expected, explain_tree.to_string());
}

#[test]
fn union_subquery_plan() {
    let query = r#"SELECT * FROM (
SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0 and "sysFrom" < 0
UNION ALL
SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
) as "t"
WHERE "id" = 1"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection ("t"."id"::int -> "id", "t"."FIRST_NAME"::string -> "FIRST_NAME")
        selection "t"."id"::int = 1::int
            scan "t"
                union all
                    projection ("test_space"."id"::int -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                        selection ("test_space"."sys_op"::int > 0::int) and ("test_space"."sysFrom"::int < 0::int)
                            scan "test_space"
                    projection ("test_space_hist"."id"::int -> "id", "test_space_hist"."FIRST_NAME"::string -> "FIRST_NAME")
                        selection "test_space_hist"."sys_op"::int < 0::int
                            scan "test_space_hist"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn union_cond_subquery_plan() {
    let query = r#"SELECT * FROM (
SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0 and "sysFrom" < 0
UNION ALL
SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
) as "t"
WHERE "id" IN (SELECT "id"
   FROM (
      SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0
      UNION ALL
      SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
  ) as "t2"
  WHERE "t2"."id" = 4)
"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection ("t"."id"::int -> "id", "t"."FIRST_NAME"::string -> "FIRST_NAME")
        selection "t"."id"::int in ROW($0)
            scan "t"
                union all
                    projection ("test_space"."id"::int -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                        selection ("test_space"."sys_op"::int > 0::int) and ("test_space"."sysFrom"::int < 0::int)
                            scan "test_space"
                    projection ("test_space_hist"."id"::int -> "id", "test_space_hist"."FIRST_NAME"::string -> "FIRST_NAME")
                        selection "test_space_hist"."sys_op"::int < 0::int
                            scan "test_space_hist"
    subquery $0:
    scan
                projection ("t2"."id"::int -> "id")
                    selection "t2"."id"::int = 4::int
                        scan "t2"
                            union all
                                projection ("test_space"."id"::int -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                                    selection "test_space"."sys_op"::int > 0::int
                                        scan "test_space"
                                projection ("test_space_hist"."id"::int -> "id", "test_space_hist"."FIRST_NAME"::string -> "FIRST_NAME")
                                    selection "test_space_hist"."sys_op"::int < 0::int
                                        scan "test_space_hist"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn explain_except1() {
    let query = r#"SELECT "product_code" as "pc" FROM "hash_testing" AS "t"
        EXCEPT DISTINCT
        SELECT "identification_number"::text FROM "hash_testing_hist""#;

    let plan = sql_to_optimized_ir(query, vec![]);
    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let expected = format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
        r#"except"#,
        r#"    projection ("t"."product_code"::string -> "pc")"#,
        r#"        scan "hash_testing" -> "t""#,
        r#"    motion [policy: full]"#,
        r#"        projection ("hash_testing_hist"."identification_number"::int::string -> "col_1")"#,
        r#"            scan "hash_testing_hist""#,
        r#"execution options:"#,
        r#"    sql_vdbe_opcode_max = 45000"#,
        r#"    sql_motion_row_max = 5000"#,
    );
    assert_eq!(expected, explain_tree.to_string());
}

#[test]
fn motion_subquery_plan() {
    let query = r#"
    SELECT * FROM (
        SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0 and "sysFrom" < 0
        UNION ALL
        SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
    ) as "t"
    WHERE
    "id" IN (SELECT "id"
        FROM (
            SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0
            UNION ALL
            SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
        ) as "t2"
        WHERE "t2"."id" = 4)
    OR "id" IN (SELECT "identification_number"
        FROM "hash_testing"
        WHERE "identification_number" = 5 AND "product_code" = '123'
        )
"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection ("t"."id"::int -> "id", "t"."FIRST_NAME"::string -> "FIRST_NAME")
        selection ("t"."id"::int in ROW($1)) or ("t"."id"::int in ROW($0))
            scan "t"
                union all
                    projection ("test_space"."id"::int -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                        selection ("test_space"."sys_op"::int > 0::int) and ("test_space"."sysFrom"::int < 0::int)
                            scan "test_space"
                    projection ("test_space_hist"."id"::int -> "id", "test_space_hist"."FIRST_NAME"::string -> "FIRST_NAME")
                        selection "test_space_hist"."sys_op"::int < 0::int
                            scan "test_space_hist"
    subquery $0:
    motion [policy: segment([ref("identification_number")])]
                scan
                    projection ("hash_testing"."identification_number"::int -> "identification_number")
                        selection ("hash_testing"."identification_number"::int = 5::int) and ("hash_testing"."product_code"::string = '123'::string)
                            scan "hash_testing"
    subquery $1:
    scan
                projection ("t2"."id"::int -> "id")
                    selection "t2"."id"::int = 4::int
                        scan "t2"
                            union all
                                projection ("test_space"."id"::int -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                                    selection "test_space"."sys_op"::int > 0::int
                                        scan "test_space"
                                projection ("test_space_hist"."id"::int -> "id", "test_space_hist"."FIRST_NAME"::string -> "FIRST_NAME")
                                    selection "test_space_hist"."sys_op"::int < 0::int
                                        scan "test_space_hist"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn motion_join_plan() {
    let query = r#"SELECT "t1"."FIRST_NAME"
FROM (SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "id" = 3) as "t1"
    JOIN (SELECT "identification_number", "product_code" FROM "hash_testing") as "t2" ON "t1"."id"="t2"."identification_number"
WHERE "t2"."product_code" = '123'"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection ("t1"."FIRST_NAME"::string -> "FIRST_NAME")
        selection "t2"."product_code"::string = '123'::string
            join on "t1"."id"::int = "t2"."identification_number"::int
                scan "t1"
                    projection ("test_space"."id"::int -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                        selection "test_space"."id"::int = 3::int
                            scan "test_space"
                motion [policy: segment([ref("identification_number")])]
                    scan "t2"
                        projection ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
                            scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn sq_join_plan() {
    let query = r#"SELECT "t1"."FIRST_NAME"
FROM (SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "id" = 3) as "t1"
    JOIN "hash_testing" ON "t1"."id"=(SELECT "identification_number" FROM "hash_testing")"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection ("t1"."FIRST_NAME"::string -> "FIRST_NAME")
        join on "t1"."id"::int = ROW($0)
            scan "t1"
                projection ("test_space"."id"::int -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                    selection "test_space"."id"::int = 3::int
                        scan "test_space"
            motion [policy: full]
                projection ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::bool -> "product_units", "hash_testing"."sys_op"::int -> "sys_op", "hash_testing"."bucket_id"::int -> "bucket_id")
                    scan "hash_testing"
    subquery $0:
    motion [policy: segment([ref("identification_number")])]
                scan
                    projection ("hash_testing"."identification_number"::int -> "identification_number")
                        scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn unary_condition_plan() {
    let query = r#"SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "id" IS NULL and "FIRST_NAME" IS NOT NULL"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection ("test_space"."id"::int -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
        selection ("test_space"."id"::int is null) and (not ("test_space"."FIRST_NAME"::string is null))
            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn insert_plan() {
    let query = r#"INSERT INTO "test_space" ("id", "FIRST_NAME") VALUES (1, '123')"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"insert "test_space" on conflict: fail
    motion [policy: segment([ref("COLUMN_1")])]
        values
            value row (data=ROW(1::int, '123'::string))
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn multiply_insert_plan() {
    let query = r#"INSERT INTO "test_space" ("id", "FIRST_NAME") VALUES (1, '123'), (2, '456'), (3, '789')"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"insert "test_space" on conflict: fail
    motion [policy: segment([ref("COLUMN_5")])]
        values
            value row (data=ROW(1::int, '123'::string))
            value row (data=ROW(2::int, '456'::string))
            value row (data=ROW(3::int, '789'::string))
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn insert_select_plan() {
    let query = r#"INSERT INTO "test_space" ("id", "FIRST_NAME")
SELECT "identification_number", "product_code" FROM "hash_testing""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"insert "test_space" on conflict: fail
    motion [policy: segment([ref("identification_number")])]
        projection ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
            scan "hash_testing"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn select_value_plan() {
    let query = r#"select * from (values (1))"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection ("unnamed_subquery"."COLUMN_1"::int -> "COLUMN_1")
        scan "unnamed_subquery"
            motion [policy: full]
                values
                    value row (data=ROW(1::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn select_cast_plan1() {
    let query = r#"SELECT CAST("id" as int) as "b" FROM "test_space""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection ("test_space"."id"::int::int -> "b")
        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn select_cast_plan2() {
    let query = r#"SELECT "id", "FIRST_NAME" FROM "test_space" WHERE CAST("id" as int) = 1"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection ("test_space"."id"::int -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
        selection "test_space"."id"::int::int = 1::int
            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn select_cast_plan_nested() {
    let query = r#"SELECT cast(trim("id"::text) as string) FROM "test_space""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection (TRIM("test_space"."id"::int::string)::string -> "col_1")
        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn select_cast_plan_nested_where() {
    let query = r#"SELECT "id" FROM "test_space" WHERE cast(trim("id"::text) as string) = '1'"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("test_space"."id"::int -> "id")
    selection TRIM("test_space"."id"::int::string)::string = '1'::string
        scan "test_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn select_cast_plan_nested_where2() {
    let query = r#"SELECT "id" FROM "test_space" WHERE trim(cast(42 as string)) = '1'"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("test_space"."id"::int -> "id")
    selection TRIM(42::int::string) = '1'::string
        scan "test_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn check_buckets_repr() {
    let bc = 3000;
    assert_eq!("[1-3000]", buckets_repr(&Buckets::All, bc));
    assert_eq!("any", buckets_repr(&Buckets::Any, bc));
    assert_eq!(
        "[1-3]",
        buckets_repr(&Buckets::Filtered(collection!(1, 2, 3)), bc)
    );
    assert_eq!(
        "[1-3]",
        buckets_repr(&Buckets::Filtered(collection!(3, 2, 1)), bc)
    );
    assert_eq!(
        "[1,10-11,21-23]",
        buckets_repr(&Buckets::Filtered(collection!(1, 10, 11, 23, 22, 21)), bc)
    );
    assert_eq!("[]", buckets_repr(&Buckets::Filtered(collection!()), bc));
}

mod cast_constants;
mod concat;
mod delete;
mod query_explain;
