use crate::errors::{Entity, SbroadError};
use crate::executor::engine::mock::RouterConfigurationMock;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use pretty_assertions::assert_eq;

#[test]
fn cte() {
    let sql = r#"WITH cte (a) AS (SELECT "FIRST_NAME" FROM "test_space") SELECT * FROM cte"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("cte"."a"::string -> "a")
    scan cte cte($0)
subquery $0:
motion [policy: full]
            projection ("test_space"."FIRST_NAME"::string -> "a")
                scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn global_cte() {
    let sql = r#"WITH cte (a) AS (SELECT "a" FROM "global_t") SELECT * FROM cte"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("cte"."a"::integer -> "a")
    scan cte cte($0)
subquery $0:
projection ("global_t"."a"::integer -> "a")
            scan "global_t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn nested_cte() {
    let sql = r#"
        WITH cte1 (a) AS (SELECT "FIRST_NAME" FROM "test_space"),
        cte2 AS (SELECT * FROM cte1)
        SELECT * FROM cte2
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("cte2"."a"::string -> "a")
    scan cte cte2($1)
subquery $0:
motion [policy: full]
                    projection ("test_space"."FIRST_NAME"::string -> "a")
                        scan "test_space"
subquery $1:
projection ("cte1"."a"::string -> "a")
            scan cte cte1($0)
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn reuse_cte_union_all() {
    let sql = r#"
        WITH cte (a) AS (SELECT "FIRST_NAME" FROM "test_space")
        SELECT * FROM cte
        UNION ALL
        SELECT * FROM cte
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"union all
    projection ("cte"."a"::string -> "a")
        scan cte cte($0)
    projection ("cte"."a"::string -> "a")
        scan cte cte($0)
subquery $0:
motion [policy: full]
                projection ("test_space"."FIRST_NAME"::string -> "a")
                    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn reuse_union_in_cte() {
    let sql = r#"
        WITH cte (a) AS (
            SELECT "FIRST_NAME" FROM "test_space"
            UNION
            SELECT "FIRST_NAME" FROM "test_space"
        )
        SELECT * FROM cte
        UNION
        SELECT * FROM cte
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"motion [policy: full]
    union
        projection ("cte"."a"::string -> "a")
            scan cte cte($0)
        projection ("cte"."a"::string -> "a")
            scan cte cte($0)
subquery $0:
projection ("cte"."FIRST_NAME"::string -> "a")
                    scan "cte"
                        motion [policy: full]
                            union
                                projection ("test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                                    scan "test_space"
                                projection ("test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                                    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn reuse_cte_values() {
    let sql = r#"
        WITH cte (b) AS (VALUES(1))
        SELECT t.c FROM (SELECT count(*) as c FROM cte c1 JOIN cte c2 ON true) t
        JOIN cte ON true
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."c"::integer -> "c")
    join on true::boolean
        scan "t"
            projection (count((*::integer))::integer -> "c")
                join on true::boolean
                    scan cte c1($0)
                    scan cte c2($0)
        scan cte cte($0)
subquery $0:
motion [policy: full]
                            projection ("cte"."COLUMN_1"::unsigned -> "b")
                                scan "cte"
                                    values
                                        value row (data=ROW(1::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn join_cte() {
    let sql = r#"
        WITH cte (a) AS (SELECT "FIRST_NAME" FROM "test_space")
        SELECT t."FIRST_NAME" FROM "test_space" t
        JOIN cte ON t."FIRST_NAME" = cte.a
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."FIRST_NAME"::string -> "FIRST_NAME")
    join on ROW("t"."FIRST_NAME"::string) = ROW("cte"."a"::string)
        scan "t"
            projection ("t"."id"::unsigned -> "id", "t"."sysFrom"::unsigned -> "sysFrom", "t"."FIRST_NAME"::string -> "FIRST_NAME", "t"."sys_op"::unsigned -> "sys_op")
                scan "test_space" -> "t"
        scan cte cte($0)
subquery $0:
motion [policy: full]
                projection ("test_space"."FIRST_NAME"::string -> "a")
                    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn agg_cte() {
    let sql = r#"
        WITH cte (a) AS (SELECT "FIRST_NAME" FROM "test_space")
        SELECT count(a) FROM cte
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection (count(("cte"."a"::string))::integer -> "col_1")
    scan cte cte($0)
subquery $0:
motion [policy: full]
            projection ("test_space"."FIRST_NAME"::string -> "a")
                scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn sq_cte() {
    let sql = r#"
        WITH cte (a) AS (SELECT "FIRST_NAME" FROM "test_space" WHERE "FIRST_NAME" = 'hi')
        SELECT "FIRST_NAME" FROM "test_space" WHERE "FIRST_NAME" IN (SELECT a FROM cte)
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."FIRST_NAME"::string -> "FIRST_NAME")
    selection ROW("test_space"."FIRST_NAME"::string) in ROW($1)
        scan "test_space"
subquery $0:
motion [policy: full]
                        projection ("test_space"."FIRST_NAME"::string -> "a")
                            selection ROW("test_space"."FIRST_NAME"::string) = ROW('hi'::string)
                                scan "test_space"
subquery $1:
scan
            projection ("cte"."a"::string -> "a")
                scan cte cte($0)
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn values_in_cte() {
    let sql = r#"
        WITH cte (a) AS (VALUES ('a'))
        SELECT * FROM cte
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("cte"."a"::string -> "a")
    scan cte cte($0)
subquery $0:
motion [policy: full]
            projection ("cte"."COLUMN_1"::string -> "a")
                scan "cte"
                    values
                        value row (data=ROW('a'::string))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn union_all_in_cte() {
    let sql = r#"
        WITH cte1 (a) AS (VALUES ('a')),
        cte2 as (SELECT * FROM cte1 UNION ALL SELECT * FROM cte1)
        SELECT * FROM cte2
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("cte2"."a"::string -> "a")
    scan cte cte2($1)
subquery $0:
motion [policy: full]
                            projection ("cte1"."COLUMN_1"::string -> "a")
                                scan "cte1"
                                    values
                                        value row (data=ROW('a'::string))
subquery $1:
motion [policy: full]
            union all
                projection ("cte1"."a"::string -> "a")
                    scan cte cte1($0)
                projection ("cte1"."a"::string -> "a")
                    scan cte cte1($0)
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn join_in_cte() {
    let sql = r#"
        WITH cte AS (
            SELECT t1."FIRST_NAME" FROM "test_space" t1
            JOIN "test_space" t2 ON t1."FIRST_NAME" = t2."id"
        )
        SELECT * FROM cte
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("cte"."FIRST_NAME"::string -> "FIRST_NAME")
    scan cte cte($0)
subquery $0:
motion [policy: full]
            projection ("t1"."FIRST_NAME"::string -> "FIRST_NAME")
                join on ROW("t1"."FIRST_NAME"::string) = ROW("t2"."id"::unsigned)
                    scan "t1"
                        projection ("t1"."id"::unsigned -> "id", "t1"."sysFrom"::unsigned -> "sysFrom", "t1"."FIRST_NAME"::string -> "FIRST_NAME", "t1"."sys_op"::unsigned -> "sys_op")
                            scan "test_space" -> "t1"
                    motion [policy: full]
                        scan "t2"
                            projection ("t2"."id"::unsigned -> "id", "t2"."sysFrom"::unsigned -> "sysFrom", "t2"."FIRST_NAME"::string -> "FIRST_NAME", "t2"."sys_op"::unsigned -> "sys_op")
                                scan "test_space" -> "t2"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn order_by_in_cte() {
    let sql = r#"
        WITH cte AS (
            SELECT "FIRST_NAME" FROM "test_space"
            ORDER BY 1
        )
        SELECT * FROM cte
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("cte"."FIRST_NAME"::string -> "FIRST_NAME")
    scan cte cte($0)
subquery $0:
projection ("FIRST_NAME"::string -> "FIRST_NAME")
            order by (1)
                motion [policy: full]
                    projection ("test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn table_name_conflict() {
    let sql = r#"
        WITH "test_space" AS (SELECT "FIRST_NAME" FROM "test_space")
        SELECT * FROM "test_space"
    "#;
    let metadata = &RouterConfigurationMock::new();
    let plan_error = AbstractSyntaxTree::transform_into_plan(sql, metadata);
    assert_eq!(
        plan_error,
        Err(SbroadError::Invalid(
            Entity::Table,
            Some(r#"table with name "test_space" is already defined as a CTE"#.into())
        ))
    );
}

#[test]
fn cte_name_conflict() {
    let sql = r#"
        WITH cte AS (SELECT "FIRST_NAME" FROM "test_space"),
        cte as (SELECT 'a' as a from "test_space")
        SELECT * FROM cte
    "#;
    let metadata = &RouterConfigurationMock::new();
    let plan_error = AbstractSyntaxTree::transform_into_plan(sql, metadata);
    assert_eq!(
        plan_error,
        Err(SbroadError::Invalid(
            Entity::Cte,
            Some(r#"CTE with name "cte" is already defined"#.into())
        ))
    );
}

#[test]
fn cte_column_mismatch() {
    let sql = r#"
        WITH cte(a) AS (SELECT "FIRST_NAME", "FIRST_NAME" FROM "test_space")
        SELECT * FROM cte
    "#;
    let metadata = &RouterConfigurationMock::new();
    let plan_error = AbstractSyntaxTree::transform_into_plan(sql, metadata);
    assert_eq!(
        plan_error,
        Err(SbroadError::UnexpectedNumberOfValues(
            "expected 2 columns in CTE, got 1".into()
        ))
    );
}

#[test]
fn cte_with_left_join() {
    let sql = r#"
        with cte as (select "e" as "E" from "t2")
        select "E" from cte left join "t2"
        on true
    "#;

    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("E"::unsigned -> "E")
    motion [policy: full]
        projection ("cte"."E"::unsigned -> "E", "t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
            join on true::boolean
                scan cte cte($0)
                scan "t2"
                    projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                        scan "t2"
subquery $0:
motion [policy: full]
                        projection ("t2"."e"::unsigned -> "E")
                            scan "t2"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}
