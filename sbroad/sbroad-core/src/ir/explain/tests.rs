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

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("t"."identification_number"::integer -> "c1", "t"."product_code"::string -> "product_code")
    scan "hash_testing" -> "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn simple_query_with_cond_plan() {
    let query = r#"SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t" WHERE "t"."identification_number" = 1 AND "t"."product_code" = '222'"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("t"."identification_number"::integer -> "c1", "t"."product_code"::string -> "product_code")
    selection ROW("t"."identification_number"::integer) = ROW(1::unsigned) and ROW("t"."product_code"::string) = ROW('222'::string)
        scan "hash_testing" -> "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
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
        r#"    projection ("t"."identification_number"::integer -> "c1", "t"."product_code"::string -> "product_code")"#,
        r#"        scan "hash_testing" -> "t""#,
        r#"    projection ("t2"."identification_number"::integer -> "identification_number", "t2"."product_code"::string -> "product_code")"#,
        r#"        scan "hash_testing_hist" -> "t2""#,
        r#"execution options:"#,
        r#"    vdbe_max_steps = 45000"#,
        r#"    vtable_max_rows = 5000"#,
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

    let mut actual_explain = String::new();
    actual_explain.push_str(r#"projection ("t"."id"::unsigned -> "id", "t"."FIRST_NAME"::string -> "FIRST_NAME")
    selection ROW("t"."id"::unsigned) = ROW(1::unsigned)
        scan "t"
            union all
                projection ("test_space"."id"::unsigned -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                    selection ROW("test_space"."sys_op"::unsigned) > ROW(0::unsigned) and ROW("test_space"."sysFrom"::unsigned) < ROW(0::unsigned)
                        scan "test_space"
                projection ("test_space_hist"."id"::unsigned -> "id", "test_space_hist"."FIRST_NAME"::string -> "FIRST_NAME")
                    selection ROW("test_space_hist"."sys_op"::unsigned) < ROW(0::unsigned)
                        scan "test_space_hist"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#);

    assert_eq!(actual_explain, explain_tree.to_string());
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

    let mut actual_explain = String::new();
    actual_explain.push_str(r#"projection ("t"."id"::unsigned -> "id", "t"."FIRST_NAME"::string -> "FIRST_NAME")
    selection ROW("t"."id"::unsigned) in ROW($0)
        scan "t"
            union all
                projection ("test_space"."id"::unsigned -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                    selection ROW("test_space"."sys_op"::unsigned) > ROW(0::unsigned) and ROW("test_space"."sysFrom"::unsigned) < ROW(0::unsigned)
                        scan "test_space"
                projection ("test_space_hist"."id"::unsigned -> "id", "test_space_hist"."FIRST_NAME"::string -> "FIRST_NAME")
                    selection ROW("test_space_hist"."sys_op"::unsigned) < ROW(0::unsigned)
                        scan "test_space_hist"
subquery $0:
scan
            projection ("t2"."id"::unsigned -> "id")
                selection ROW("t2"."id"::unsigned) = ROW(4::unsigned)
                    scan "t2"
                        union all
                            projection ("test_space"."id"::unsigned -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                                selection ROW("test_space"."sys_op"::unsigned) > ROW(0::unsigned)
                                    scan "test_space"
                            projection ("test_space_hist"."id"::unsigned -> "id", "test_space_hist"."FIRST_NAME"::string -> "FIRST_NAME")
                                selection ROW("test_space_hist"."sys_op"::unsigned) < ROW(0::unsigned)
                                    scan "test_space_hist"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#);

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn explain_except1() {
    let query = r#"SELECT "product_code" as "pc" FROM "hash_testing" AS "t"
        EXCEPT DISTINCT
        SELECT "identification_number" FROM "hash_testing_hist""#;

    let plan = sql_to_optimized_ir(query, vec![]);
    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let expected = format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
        r#"except"#,
        r#"    projection ("t"."product_code"::string -> "pc")"#,
        r#"        scan "hash_testing" -> "t""#,
        r#"    motion [policy: full]"#,
        r#"        projection ("hash_testing_hist"."identification_number"::integer -> "identification_number")"#,
        r#"            scan "hash_testing_hist""#,
        r#"execution options:"#,
        r#"    vdbe_max_steps = 45000"#,
        r#"    vtable_max_rows = 5000"#,
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

    let mut actual_explain = String::new();
    actual_explain.push_str(r#"projection ("t"."id"::unsigned -> "id", "t"."FIRST_NAME"::string -> "FIRST_NAME")
    selection ROW("t"."id"::unsigned) in ROW($1) or ROW("t"."id"::unsigned) in ROW($0)
        scan "t"
            union all
                projection ("test_space"."id"::unsigned -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                    selection ROW("test_space"."sys_op"::unsigned) > ROW(0::unsigned) and ROW("test_space"."sysFrom"::unsigned) < ROW(0::unsigned)
                        scan "test_space"
                projection ("test_space_hist"."id"::unsigned -> "id", "test_space_hist"."FIRST_NAME"::string -> "FIRST_NAME")
                    selection ROW("test_space_hist"."sys_op"::unsigned) < ROW(0::unsigned)
                        scan "test_space_hist"
subquery $0:
motion [policy: segment([ref("identification_number")])]
            scan
                projection ("hash_testing"."identification_number"::integer -> "identification_number")
                    selection ROW("hash_testing"."identification_number"::integer) = ROW(5::unsigned) and ROW("hash_testing"."product_code"::string) = ROW('123'::string)
                        scan "hash_testing"
subquery $1:
scan
            projection ("t2"."id"::unsigned -> "id")
                selection ROW("t2"."id"::unsigned) = ROW(4::unsigned)
                    scan "t2"
                        union all
                            projection ("test_space"."id"::unsigned -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                                selection ROW("test_space"."sys_op"::unsigned) > ROW(0::unsigned)
                                    scan "test_space"
                            projection ("test_space_hist"."id"::unsigned -> "id", "test_space_hist"."FIRST_NAME"::string -> "FIRST_NAME")
                                selection ROW("test_space_hist"."sys_op"::unsigned) < ROW(0::unsigned)
                                    scan "test_space_hist"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#);

    assert_eq!(actual_explain, explain_tree.to_string());
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

    let mut actual_explain = String::new();
    actual_explain.push_str(r#"projection ("t1"."FIRST_NAME"::string -> "FIRST_NAME")
    selection ROW("t2"."product_code"::string) = ROW('123'::string)
        join on ROW("t1"."id"::unsigned) = ROW("t2"."identification_number"::integer)
            scan "t1"
                projection ("test_space"."id"::unsigned -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                    selection ROW("test_space"."id"::unsigned) = ROW(3::unsigned)
                        scan "test_space"
            motion [policy: segment([ref("identification_number")])]
                scan "t2"
                    projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
                        scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#);

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn sq_join_plan() {
    let query = r#"SELECT "t1"."FIRST_NAME"
FROM (SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "id" = 3) as "t1"
    JOIN "hash_testing" ON "t1"."id"=(SELECT "identification_number" FROM "hash_testing")"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(r#"projection ("t1"."FIRST_NAME"::string -> "FIRST_NAME")
    join on ROW("t1"."id"::unsigned) = ROW($0)
        scan "t1"
            projection ("test_space"."id"::unsigned -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                selection ROW("test_space"."id"::unsigned) = ROW(3::unsigned)
                    scan "test_space"
        motion [policy: full]
            scan "hash_testing"
                projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op")
                    scan "hash_testing"
subquery $0:
motion [policy: segment([ref("identification_number")])]
            scan
                projection ("hash_testing"."identification_number"::integer -> "identification_number")
                    scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#);

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn unary_condition_plan() {
    let query = r#"SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "id" IS NULL and "FIRST_NAME" IS NOT NULL"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("test_space"."id"::unsigned -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
    selection ROW("test_space"."id"::unsigned) is null and not ROW("test_space"."FIRST_NAME"::string) is null
        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
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
            value row (data=ROW(1::unsigned, '123'::string))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
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
            value row (data=ROW(1::unsigned, '123'::string))
            value row (data=ROW(2::unsigned, '456'::string))
            value row (data=ROW(3::unsigned, '789'::string))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
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
        projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
            scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
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

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("COLUMN_1"::unsigned -> "COLUMN_1")
    scan
        values
            value row (data=ROW(1::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn select_cast_plan1() {
    let query = r#"SELECT CAST("id" as unsigned) as "b" FROM "test_space""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("test_space"."id"::unsigned::unsigned -> "b")
    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn select_cast_plan2() {
    let query = r#"SELECT "id", "FIRST_NAME" FROM "test_space" WHERE CAST("id" as integer) = 1"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("test_space"."id"::unsigned -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
    selection ROW("test_space"."id"::unsigned::int) = ROW(1::unsigned)
        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn select_cast_plan_nested() {
    let query = r#"SELECT cast(func("id") as string) FROM "test_space""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("func"(("test_space"."id"::unsigned))::integer::string -> "col_1")
    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn select_cast_plan_nested_where() {
    let query = r#"SELECT "id" FROM "test_space" WHERE cast(func("id") as string) = 1"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("test_space"."id"::unsigned -> "id")
    selection ROW("func"(("test_space"."id"::unsigned))::integer::string) = ROW(1::unsigned)
        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn select_cast_plan_nested_where2() {
    let query = r#"SELECT "id" FROM "test_space" WHERE func(cast(42 as string)) = 1"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("test_space"."id"::unsigned -> "id")
    selection ROW("func"((42::unsigned::string))::integer) = ROW(1::unsigned)
        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

pub(crate) fn explain_check(sql: &str, explain: &str) {
    let plan = sql_to_optimized_ir(sql, vec![]);
    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();
    assert_eq!(explain, explain_tree.to_string());
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

#[cfg(test)]
mod concat;

#[cfg(test)]
mod delete;

#[cfg(test)]
mod query_explain;
