use crate::errors::SbroadError;
use crate::executor::engine::mock::RouterConfigurationMock;
use crate::frontend::sql::ast::{AbstractSyntaxTree, ParseTree, Rule};
use crate::frontend::Ast;
use crate::ir::node::relational::Relational;
use crate::ir::node::NodeId;
use crate::ir::transformation::helpers::{sql_to_ir, sql_to_optimized_ir};
use crate::ir::tree::traversal::PostOrder;
use crate::ir::value::Value;
use crate::ir::{Plan, Positions};
use pest::Parser;
use pretty_assertions::assert_eq;
use time::{format_description, OffsetDateTime, Time};

fn sql_to_optimized_ir_add_motions_err(query: &str) -> SbroadError {
    let metadata = &RouterConfigurationMock::new();
    let mut plan = AbstractSyntaxTree::transform_into_plan(query, metadata).unwrap();
    plan.replace_in_operator().unwrap();
    plan.push_down_not().unwrap();
    plan.split_columns().unwrap();
    plan.set_dnf().unwrap();
    plan.derive_equalities().unwrap();
    plan.merge_tuples().unwrap();
    plan.add_motions().unwrap_err()
}

fn check_output(input: &str, params: Vec<Value>, expected_explain: &str) {
    let plan = sql_to_optimized_ir(input, params);
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql1() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        WHERE "identification_number" = 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
    selection ROW("hash_testing"."identification_number"::integer) = ROW(1::unsigned)
        scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql2() {
    let input = r#"SELECT "identification_number", "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1 AND "product_code" = '1'
        OR "identification_number" = 2 AND "product_code" = '2'"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
    selection ROW("hash_testing"."identification_number"::integer) = ROW(1::unsigned) and ROW("hash_testing"."product_code"::string) = ROW('1'::string) or ROW("hash_testing"."identification_number"::integer) = ROW(2::unsigned) and ROW("hash_testing"."product_code"::string) = ROW('2'::string)
        scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql3() {
    let input = r#"SELECT *
        FROM
            (SELECT "identification_number", "product_code"
            FROM "hash_testing"
            WHERE "sys_op" = 1
            UNION ALL
            SELECT "identification_number", "product_code"
            FROM "hash_testing_hist"
            WHERE "sys_op" > 1) AS "t3"
        WHERE "identification_number" = 1"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t3"."identification_number"::integer -> "identification_number", "t3"."product_code"::string -> "product_code")
    selection ROW("t3"."identification_number"::integer) = ROW(1::unsigned)
        scan "t3"
            union all
                projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
                    selection ROW("hash_testing"."sys_op"::unsigned) = ROW(1::unsigned)
                        scan "hash_testing"
                projection ("hash_testing_hist"."identification_number"::integer -> "identification_number", "hash_testing_hist"."product_code"::string -> "product_code")
                    selection ROW("hash_testing_hist"."sys_op"::unsigned) > ROW(1::unsigned)
                        scan "hash_testing_hist"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql4() {
    let input = r#"SELECT *
        FROM
            (SELECT "identification_number", "product_code"
            FROM "hash_testing"
            WHERE "sys_op" = 1
            UNION ALL
            SELECT "identification_number", "product_code"
            FROM "hash_testing_hist"
            WHERE "sys_op" > 1) AS "t3"
        WHERE ("identification_number" = 1
            OR ("identification_number" = 2
            OR "identification_number" = 3))
            AND ("product_code" = '1'
            OR "product_code" = '2')"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t3"."identification_number"::integer -> "identification_number", "t3"."product_code"::string -> "product_code")
    selection (ROW("t3"."identification_number"::integer) = ROW(1::unsigned) or (ROW("t3"."identification_number"::integer) = ROW(2::unsigned) or ROW("t3"."identification_number"::integer) = ROW(3::unsigned))) and (ROW("t3"."product_code"::string) = ROW('1'::string) or ROW("t3"."product_code"::string) = ROW('2'::string))
        scan "t3"
            union all
                projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
                    selection ROW("hash_testing"."sys_op"::unsigned) = ROW(1::unsigned)
                        scan "hash_testing"
                projection ("hash_testing_hist"."identification_number"::integer -> "identification_number", "hash_testing_hist"."product_code"::string -> "product_code")
                    selection ROW("hash_testing_hist"."sys_op"::unsigned) > ROW(1::unsigned)
                        scan "hash_testing_hist"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql5() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        WHERE "identification_number" in (
        SELECT "identification_number" FROM "hash_testing_hist" WHERE "product_code" = 'a')"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
    selection ROW("hash_testing"."identification_number"::integer) in ROW($0)
        scan "hash_testing"
subquery $0:
motion [policy: full]
            scan
                projection ("hash_testing_hist"."identification_number"::integer -> "identification_number")
                    selection ROW("hash_testing_hist"."product_code"::string) = ROW('a'::string)
                        scan "hash_testing_hist"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql6() {
    let input = r#"SELECT "id", "product_units" FROM "hash_testing"
        INNER JOIN (SELECT "id" FROM "test_space") as t
        ON "hash_testing"."identification_number" = t."id"
        WHERE "hash_testing"."identification_number" = 5 and "hash_testing"."product_code" = '123'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."id"::unsigned -> "id", "hash_testing"."product_units"::boolean -> "product_units")
    selection ROW("hash_testing"."identification_number"::integer) = ROW(5::unsigned) and ROW("hash_testing"."product_code"::string) = ROW('123'::string)
        join on ROW("hash_testing"."identification_number"::integer) = ROW("t"."id"::unsigned)
            scan "hash_testing"
                projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op")
                    scan "hash_testing"
            motion [policy: full]
                scan "t"
                    projection ("test_space"."id"::unsigned -> "id")
                        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql8() {
    let input = r#"SELECT t."identification_number", "product_code" FROM "hash_testing" as t
        WHERE t."identification_number" = 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."identification_number"::integer -> "identification_number", "t"."product_code"::string -> "product_code")
    selection ROW("t"."identification_number"::integer) = ROW(1::unsigned)
        scan "hash_testing" -> "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql9() {
    let input = r#"SELECT *
        FROM
            (SELECT "id", "FIRST_NAME"
            FROM "test_space"
            WHERE "sys_op" < 0
                    AND "sysFrom" >= 0
            UNION ALL
            SELECT "id", "FIRST_NAME"
            FROM "test_space_hist"
            WHERE "sysFrom" <= 0) AS "t3"
        INNER JOIN
            (SELECT "identification_number", "product_code"
            FROM "hash_testing_hist"
            WHERE "sys_op" > 0
            UNION ALL
            SELECT "identification_number", "product_code"
            FROM "hash_single_testing_hist"
            WHERE "sys_op" <= 0) AS "t8"
            ON "t3"."id" = "t8"."identification_number"
        WHERE "id" = 1 AND "t8"."identification_number" = 1 AND "product_code" = '123'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t3"."id"::unsigned -> "id", "t3"."FIRST_NAME"::string -> "FIRST_NAME", "t8"."identification_number"::integer -> "identification_number", "t8"."product_code"::string -> "product_code")
    selection ROW("t3"."id"::unsigned) = ROW(1::unsigned) and ROW("t8"."identification_number"::integer) = ROW(1::unsigned) and ROW("t8"."product_code"::string) = ROW('123'::string)
        join on ROW("t3"."id"::unsigned) = ROW("t8"."identification_number"::integer)
            scan "t3"
                union all
                    projection ("test_space"."id"::unsigned -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                        selection ROW("test_space"."sys_op"::unsigned) < ROW(0::unsigned) and ROW("test_space"."sysFrom"::unsigned) >= ROW(0::unsigned)
                            scan "test_space"
                    projection ("test_space_hist"."id"::unsigned -> "id", "test_space_hist"."FIRST_NAME"::string -> "FIRST_NAME")
                        selection ROW("test_space_hist"."sysFrom"::unsigned) <= ROW(0::unsigned)
                            scan "test_space_hist"
            motion [policy: segment([ref("identification_number")])]
                scan "t8"
                    union all
                        projection ("hash_testing_hist"."identification_number"::integer -> "identification_number", "hash_testing_hist"."product_code"::string -> "product_code")
                            selection ROW("hash_testing_hist"."sys_op"::unsigned) > ROW(0::unsigned)
                                scan "hash_testing_hist"
                        projection ("hash_single_testing_hist"."identification_number"::integer -> "identification_number", "hash_single_testing_hist"."product_code"::string -> "product_code")
                            selection ROW("hash_single_testing_hist"."sys_op"::unsigned) <= ROW(0::unsigned)
                                scan "hash_single_testing_hist"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql10() {
    let input = r#"INSERT INTO "t" VALUES(1, 2, 3, 4)"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")])]
        values
            value row (data=ROW(1::unsigned, 2::unsigned, 3::unsigned, 4::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql11() {
    let input = r#"INSERT INTO "t" ("b", "d") VALUES(1, 2)"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([value(NULL), ref("COLUMN_1")])]
        values
            value row (data=ROW(1::unsigned, 2::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql14() {
    let input = r#"INSERT INTO "t" ("b", "c") SELECT "b", "d" FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([value(NULL), ref("b")])]
        projection ("t"."b"::unsigned -> "b", "t"."d"::unsigned -> "d")
            scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

// check cyrillic strings support
#[test]
fn front_sql16() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        WHERE "product_code" = 'кириллица'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
    selection ROW("hash_testing"."product_code"::string) = ROW('кириллица'::string)
        scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql17() {
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        WHERE "product_code" IS NULL"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number")
    selection ROW("hash_testing"."product_code"::string) is null
        scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql18() {
    let input = r#"SELECT "product_code" FROM "hash_testing"
        WHERE "product_code" BETWEEN 1 AND 2"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."product_code"::string -> "product_code")
    selection ROW("hash_testing"."product_code"::string) >= ROW(1::unsigned) and ROW("hash_testing"."product_code"::string) <= ROW(2::unsigned)
        scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql19() {
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        WHERE "product_code" IS NOT NULL"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number")
    selection not ROW("hash_testing"."product_code"::string) is null
        scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_is_true() {
    check_output(
        "select true is true",
        vec![],
        r#"projection (ROW(true::boolean) = true::boolean -> "col_1")
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    check_output(
        "select true is not true",
        vec![],
        r#"projection (not ROW(true::boolean) = true::boolean -> "col_1")
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
}

#[test]
fn front_sql_is_false() {
    check_output(
        "select true is false",
        vec![],
        r#"projection (ROW(true::boolean) = false::boolean -> "col_1")
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    check_output(
        "select true is not false",
        vec![],
        r#"projection (not ROW(true::boolean) = false::boolean -> "col_1")
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
}

#[test]
fn front_sql_is_null_unknown() {
    check_output(
        "select true is null",
        vec![],
        r#"projection (ROW(true::boolean) is null -> "col_1")
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    check_output(
        "select true is unknown",
        vec![],
        r#"projection (ROW(true::boolean) is null -> "col_1")
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    check_output(
        "select true is not null",
        vec![],
        r#"projection (not ROW(true::boolean) is null -> "col_1")
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    check_output(
        "select true is not unknown",
        vec![],
        r#"projection (not ROW(true::boolean) is null -> "col_1")
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
}

#[test]
fn front_sql_between_with_additional_non_bool_value_from_left() {
    let input = r#"SELECT * FROM "test_space" WHERE 42 and 1 between 2 and 3"#;
    let mut plan = sql_to_ir(input, vec![]);
    let err = plan.optimize().unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("Left expression is not a boolean expression or NULL")
    );
}

#[test]
fn front_sql_between_with_additional_and_from_left() {
    // Was previously misinterpreted as
    //                  SELECT "id" FROM "test_space" as "t" WHERE ("t"."id" > 1 AND "t"."id") BETWEEN "t"."id" AND "t"."id" + 10
    let input = r#"SELECT "id" FROM "test_space" as "t" WHERE "t"."id" > 1 AND "t"."id" BETWEEN "t"."id" AND "t"."id" + 10"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."id"::unsigned -> "id")
    selection ROW("t"."id"::unsigned) > ROW(1::unsigned) and ROW("t"."id"::unsigned) >= ROW("t"."id"::unsigned) and ROW("t"."id"::unsigned) <= ROW("t"."id"::unsigned) + ROW(10::unsigned)
        scan "test_space" -> "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_between_with_additional_not_from_left() {
    // Was previously misinterpreted as
    //                  SELECT "id" FROM "test_space" as "t" WHERE (not "t"."id") BETWEEN "t"."id" AND "t"."id" + 10 and true
    let input = r#"SELECT "id" FROM "test_space" as "t" WHERE not "t"."id" BETWEEN "t"."id" AND "t"."id" + 10 and true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."id"::unsigned -> "id")
    selection not (ROW("t"."id"::unsigned) >= ROW("t"."id"::unsigned) and ROW("t"."id"::unsigned) <= ROW("t"."id"::unsigned) + ROW(10::unsigned)) and ROW(true::boolean)
        scan "test_space" -> "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_between_with_additional_and_from_left_and_right() {
    // Was previously misinterpreted as
    //                  SELECT "id" FROM "test_space" as "t" WHERE ("t"."id" > 1 AND "t"."id") BETWEEN "t"."id" AND "t"."id" + 10 AND true
    let input = r#"SELECT "id" FROM "test_space" as "t" WHERE "t"."id" > 1 AND "t"."id" BETWEEN "t"."id" AND "t"."id" + 10 AND true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."id"::unsigned -> "id")
    selection ROW("t"."id"::unsigned) > ROW(1::unsigned) and ROW("t"."id"::unsigned) >= ROW("t"."id"::unsigned) and ROW("t"."id"::unsigned) <= ROW("t"."id"::unsigned) + ROW(10::unsigned) and ROW(true::boolean)
        scan "test_space" -> "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_between_with_nested_not_from_the_left() {
    // `not not false between false and true` should be interpreted as
    // `not (not (false between false and true))`
    let input =
        r#"SELECT "id" FROM "test_space" as "t" WHERE not not false between false and true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."id"::unsigned -> "id")
    selection not not (ROW(false::boolean) >= ROW(false::boolean) and ROW(false::boolean) <= ROW(true::boolean))
        scan "test_space" -> "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_between_with_nested_and_from_the_left() {
    // `false and true and false between false and true` should be interpreted as
    // `(false and true) and (false between false and true)`
    let input = r#"SELECT "id" FROM "test_space" as "t" WHERE false and true and false between false and true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."id"::unsigned -> "id")
    selection ROW(false::boolean) and ROW(true::boolean) and ROW(false::boolean) >= ROW(false::boolean) and ROW(false::boolean) <= ROW(true::boolean)
        scan "test_space" -> "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_between_invalid() {
    let invalid_between_expressions = vec![
        r#"SELECT * FROM "test_space" WHERE 1 BETWEEN 2"#,
        r#"SELECT * FROM "test_space" WHERE 1 BETWEEN 2 OR 3"#,
    ];

    for invalid_expr in invalid_between_expressions {
        let metadata = &RouterConfigurationMock::new();
        let plan = AbstractSyntaxTree::transform_into_plan(invalid_expr, metadata);
        let err = plan.unwrap_err();

        assert_eq!(
            true,
            err.to_string().contains(
                "BETWEEN operator should have a view of `expr_1 BETWEEN expr_2 AND expr_3`."
            )
        );
    }
}

#[test]
fn front_sql_parse_inner_join() {
    let input = r#"SELECT * FROM "hash_testing"
        left join "hash_testing" on true inner join "hash_testing" on true"#;

    // Check there are no panics
    let _ = sql_to_optimized_ir(input, vec![]);
    assert_eq!(true, true)
}

#[test]
fn front_sql_check_arbitrary_utf_in_single_quote_strings() {
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        WHERE "product_code" = '«123»§#*&%@/// / // \\ ƵǖḘỺʥ ͑ ͑  ͕ΆΨѮښ ۞ܤ'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number")
    selection ROW("hash_testing"."product_code"::string) = ROW('«123»§#*&%@/// / // \\ ƵǖḘỺʥ ͑ ͑  ͕ΆΨѮښ ۞ܤ'::string)
        scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_check_arbitraty_utf_in_identifiers() {
    let input = r#"SELECT "id" "from", "id" as "select", "id"
                               "123»&%ښ۞@Ƶǖselect.""''\\"
                                , "id" aц1&@$Ƶǖ%^&«»§&%ښ۞@Ƶǖ FROM "test_space" &%ښ۞@Ƶǖ"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("&%ښ۞@ƶǖ"."id"::unsigned -> "from", "&%ښ۞@ƶǖ"."id"::unsigned -> "select", "&%ښ۞@ƶǖ"."id"::unsigned -> "123»&%ښ۞@Ƶǖselect.""''\\", "&%ښ۞@ƶǖ"."id"::unsigned -> "aц1&@$ƶǖ%^&«»§&%ښ۞@ƶǖ")
    scan "test_space" -> "&%ښ۞@ƶǖ"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_check_inapplicatable_symbols() {
    let input = r#"
    SELECT "A"*"A", "B"+"B", "A"-"A"
    FROM "TBL"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (ROW("TBL"."A"::unsigned) * ROW("TBL"."A"::unsigned) -> "col_1", ROW("TBL"."B"::unsigned) + ROW("TBL"."B"::unsigned) -> "col_2", ROW("TBL"."A"::unsigned) - ROW("TBL"."A"::unsigned) -> "col_3")
    scan "TBL"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_projection_with_scan_specification_under_scan() {
    let input = r#"SELECT "hash_testing".* FROM "hash_testing""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op")
    scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_projection_with_scan_specification_under_join() {
    let input = r#"SELECT "hash_testing".* FROM "hash_testing" join "test_space" on true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op")
    join on true::boolean
        scan "hash_testing"
            projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op")
                scan "hash_testing"
        motion [policy: full]
            scan "test_space"
                projection ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op")
                    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_projection_with_scan_specification_under_join_of_subqueries() {
    let input = r#"SELECT "ts_sq".*, "hs".* FROM "hash_testing" as "hs"
                                join (select "ts".* from "test_space" as "ts") as "ts_sq" on true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("ts_sq"."id"::unsigned -> "id", "ts_sq"."sysFrom"::unsigned -> "sysFrom", "ts_sq"."FIRST_NAME"::string -> "FIRST_NAME", "ts_sq"."sys_op"::unsigned -> "sys_op", "hs"."identification_number"::integer -> "identification_number", "hs"."product_code"::string -> "product_code", "hs"."product_units"::boolean -> "product_units", "hs"."sys_op"::unsigned -> "sys_op")
    join on true::boolean
        scan "hs"
            projection ("hs"."identification_number"::integer -> "identification_number", "hs"."product_code"::string -> "product_code", "hs"."product_units"::boolean -> "product_units", "hs"."sys_op"::unsigned -> "sys_op")
                scan "hash_testing" -> "hs"
        motion [policy: full]
            scan "ts_sq"
                projection ("ts"."id"::unsigned -> "id", "ts"."sysFrom"::unsigned -> "sysFrom", "ts"."FIRST_NAME"::string -> "FIRST_NAME", "ts"."sys_op"::unsigned -> "sys_op")
                    scan "test_space" -> "ts"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_order_by_with_simple_select() {
    let input = r#"select * from "test_space" order by "id""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("id"::unsigned -> "id", "sysFrom"::unsigned -> "sysFrom", "FIRST_NAME"::string -> "FIRST_NAME", "sys_op"::unsigned -> "sys_op")
    order by ("id"::unsigned)
        motion [policy: full]
            projection ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op")
                scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_order_by_with_param() {
    let input = r#"select * from "test_space" order by ?"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string().contains(
            "Using parameter as a standalone ORDER BY expression doesn't influence sorting"
        )
    );
}

#[test]
fn front_order_by_without_position_and_reference() {
    let input = r#"select * from "test_space" order by 1 + 8 asc, true and 'value'"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("ORDER BY element that is not position and doesn't contain reference doesn't influence ordering")
    );
}

#[test]
fn front_order_by_with_order_type_specification() {
    let input = r#"select * from "test_space" order by "id" desc, "sysFrom" asc"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("id"::unsigned -> "id", "sysFrom"::unsigned -> "sysFrom", "FIRST_NAME"::string -> "FIRST_NAME", "sys_op"::unsigned -> "sys_op")
    order by ("id"::unsigned desc, "sysFrom"::unsigned asc)
        motion [policy: full]
            projection ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op")
                scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_order_by_with_indices() {
    let input = r#"select * from "test_space" order by 2, 1 desc"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("id"::unsigned -> "id", "sysFrom"::unsigned -> "sysFrom", "FIRST_NAME"::string -> "FIRST_NAME", "sys_op"::unsigned -> "sys_op")
    order by (2, 1 desc)
        motion [policy: full]
            projection ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op")
                scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_order_by_ordering_by_expressions_from_projection() {
    let input =
        r#"select "id" as "my_col", "id" from "test_space" order by "my_col", "id", 1 desc, 2 asc"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("my_col"::unsigned -> "my_col", "id"::unsigned -> "id")
    order by ("my_col"::unsigned, "id"::unsigned, 1 desc, 2 asc)
        motion [policy: full]
            projection ("test_space"."id"::unsigned -> "my_col", "test_space"."id"::unsigned -> "id")
                scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_order_by_with_indices_bigger_than_projection_output_length() {
    let input = r#"select "id" from "test_space" order by 1 asc, 2 desc, 3"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("Ordering index (2) is bigger than child projection output length (1).")
    );
}

#[test]
fn front_order_by_over_single_distribution_must_not_add_motion() {
    let input = r#"select "id_count" from
                        (select count("id") as "id_count" from "test_space")
                        order by "id_count""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("id_count"::integer -> "id_count")
    order by ("id_count"::integer)
        projection ("id_count"::integer -> "id_count")
            scan
                projection (sum(("count_696"::integer))::decimal -> "id_count")
                    motion [policy: full]
                        projection (count(("test_space"."id"::unsigned))::integer -> "count_696")
                            scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_join_with_identical_columns() {
    let input = r#"select * from (select "sysFrom" from "test_space") join (select "sysFrom" from "test_space") on true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("sysFrom"::unsigned -> "sysFrom", "sysFrom"::unsigned -> "sysFrom")
    join on true::boolean
        scan
            projection ("test_space"."sysFrom"::unsigned -> "sysFrom")
                scan "test_space"
        motion [policy: full]
            scan
                projection ("test_space"."sysFrom"::unsigned -> "sysFrom")
                    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_join_with_vtable_ambiguous_column_name() {
    let input = r#"select * from "test_space"
                        join (
                            select * from (select "id" from "test_space") t1
                            join (select "id" from "test_space") t2
                            on true
                        )
                        on true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op", "id"::unsigned -> "id", "id"::unsigned -> "id")
    join on true::boolean
        scan "test_space"
            projection ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op")
                scan "test_space"
        motion [policy: full]
            scan
                projection ("t1"."id"::unsigned -> "id", "t2"."id"::unsigned -> "id")
                    join on true::boolean
                        scan "t1"
                            projection ("test_space"."id"::unsigned -> "id")
                                scan "test_space"
                        motion [policy: full]
                            scan "t2"
                                projection ("test_space"."id"::unsigned -> "id")
                                    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_case_search() {
    let input = r#"select
                            case "id" when 1 then true end
                        from
                        "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (case "test_space"."id"::unsigned when 1::unsigned then true::boolean end -> "col_1")
    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_case_simple() {
    let input = r#"select
                            case
                                when true = true then 'Moscow'
                                when 1 != 2 and 4 < 5 then 42
                                else false
                            end as "case_result"
                        from
                        "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (case when ROW(true::boolean) = ROW(true::boolean) then 'Moscow'::string when ROW(1::unsigned) <> ROW(2::unsigned) and ROW(4::unsigned) < ROW(5::unsigned) then 42::unsigned else false::boolean end -> "case_result")
    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_case_nested() {
    let input = r#"select
                            case "id"
                                when 1 then
                                    case "sysFrom"
                                        when 69 then true
                                        when 42 then false
                                    end
                                when 2 then 42
                                else false
                            end as "case_result"
                        from
                        "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (case "test_space"."id"::unsigned when 1::unsigned then case "test_space"."sysFrom"::unsigned when 69::unsigned then true::boolean when 42::unsigned then false::boolean end when 2::unsigned then 42::unsigned else false::boolean end -> "case_result")
    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_subquery_column_duplicates() {
    let input = r#"SELECT "id" FROM "test_space" WHERE ("id", "id")
        IN (SELECT "id", "id" from "test_space")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id")
    selection ROW("test_space"."id"::unsigned, "test_space"."id"::unsigned) in ROW($0, $0)
        scan "test_space"
subquery $0:
scan
            projection ("test_space"."id"::unsigned -> "id", "test_space"."id"::unsigned -> "id")
                scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

impl Plan {
    fn get_positions(&self, node_id: NodeId) -> Option<Positions> {
        let mut context = self.context_mut();
        context
            .get_shard_columns_positions(node_id, self)
            .unwrap()
            .copied()
    }
}

#[test]
fn track_shard_col_pos() {
    let input = r#"
    select "e", "bucket_id", "f"
    from "t2"
    where "e" + "f" = 3
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    let mut dfs = PostOrder::with_capacity(|x| plan.nodes.rel_iter(x), 10);
    for level_node in dfs.iter(top) {
        let node_id = level_node.1;
        let node = plan.get_relation_node(node_id).unwrap();
        match node {
            Relational::ScanRelation(_) | Relational::Selection(_) => {
                assert_eq!([Some(4_usize), None], plan.get_positions(node_id).unwrap())
            }
            Relational::Projection(_) => {
                assert_eq!([Some(1_usize), None], plan.get_positions(node_id).unwrap())
            }
            _ => {}
        }
    }

    let input = r#"select t_mv."bucket_id", "t2"."bucket_id" from "t2" join (
        select "bucket_id" from "test_space" where "id" = 1
    ) as t_mv
    on t_mv."bucket_id" = "t2"."bucket_id";
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    let mut dfs = PostOrder::with_capacity(|x| plan.nodes.rel_iter(x), 10);
    for level_node in dfs.iter(top) {
        let node_id = level_node.1;
        let node = plan.get_relation_node(node_id).unwrap();
        if let Relational::Join(_) = node {
            assert_eq!(
                [Some(4_usize), Some(5_usize)],
                plan.get_positions(node_id).unwrap()
            );
        }
    }
    assert_eq!(
        [Some(0_usize), Some(1_usize)],
        plan.get_positions(top).unwrap()
    );

    let input = r#"select t_mv."bucket_id", "t2"."bucket_id" from "t2" join (
        select "bucket_id" from "test_space" where "id" = 1
    ) as t_mv
    on t_mv."bucket_id" < "t2"."bucket_id";
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    let mut dfs = PostOrder::with_capacity(|x| plan.nodes.rel_iter(x), 10);
    for level_node in dfs.iter(top) {
        let node_id = level_node.1;
        let node = plan.get_relation_node(node_id).unwrap();
        if let Relational::Join(_) = node {
            assert_eq!([Some(4_usize), None], plan.get_positions(node_id).unwrap());
        }
    }
    assert_eq!([Some(1_usize), None], plan.get_positions(top).unwrap());

    let input = r#"
    select "bucket_id", "e" from "t2"
    union all
    select "id", "bucket_id" from "test_space"
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    assert_eq!(None, plan.get_positions(top));

    let input = r#"
    select "bucket_id", "e" from "t2"
    union all
    select "bucket_id", "id" from "test_space"
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    assert_eq!([Some(0_usize), None], plan.get_positions(top).unwrap());

    let input = r#"
    select "e" from (select "bucket_id" as "e" from "t2")
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    assert_eq!([Some(0_usize), None], plan.get_positions(top).unwrap());

    let input = r#"
    select "e" as "bucket_id" from "t2"
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    assert_eq!(None, plan.get_positions(top));
}

#[test]
fn front_sql_join_on_bucket_id1() {
    let input = r#"select * from "t2" join (
        select "bucket_id" from "test_space" where "id" = 1
    ) as t_mv
    on t_mv."bucket_id" = "t2"."bucket_id";
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
    join on ROW("t_mv"."bucket_id"::unsigned) = ROW("t2"."bucket_id"::unsigned)
        scan "t2"
            projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h", "t2"."bucket_id"::unsigned -> "bucket_id")
                scan "t2"
        scan "t_mv"
            projection ("test_space"."bucket_id"::unsigned -> "bucket_id")
                selection ROW("test_space"."id"::unsigned) = ROW(1::unsigned)
                    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_join_on_bucket_id2() {
    let input = r#"select * from "t2" join (
        select "bucket_id" from "test_space" where "id" = 1
    ) as t_mv
    on t_mv."bucket_id" = "t2"."bucket_id" or "t2"."e" = "t2"."f";
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
    join on ROW("t_mv"."bucket_id"::unsigned) = ROW("t2"."bucket_id"::unsigned) or ROW("t2"."e"::unsigned) = ROW("t2"."f"::unsigned)
        scan "t2"
            projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h", "t2"."bucket_id"::unsigned -> "bucket_id")
                scan "t2"
        motion [policy: full]
            scan "t_mv"
                projection ("test_space"."bucket_id"::unsigned -> "bucket_id")
                    selection ROW("test_space"."id"::unsigned) = ROW(1::unsigned)
                        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_on_bucket_id() {
    let input = r#"
    select b, count(*) from (select "bucket_id" as b from "t2") as t
    group by b
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."b"::unsigned -> "b", count((*::integer))::integer -> "col_1")
    group by ("t"."b"::unsigned) output: ("t"."b"::unsigned -> "b")
        scan "t"
            projection ("t2"."bucket_id"::unsigned -> "b")
                scan "t2"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_sq_on_bucket_id() {
    let input = r#"
    select b, e from (select "bucket_id" as b, "e" as e from "t2") as t
    where (b, e) in (select "bucket_id", "id" from "test_space")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."b"::unsigned -> "b", "t"."e"::unsigned -> "e")
    selection ROW("t"."b"::unsigned, "t"."e"::unsigned) in ROW($0, $0)
        scan "t"
            projection ("t2"."bucket_id"::unsigned -> "b", "t2"."e"::unsigned -> "e")
                scan "t2"
subquery $0:
scan
            projection ("test_space"."bucket_id"::unsigned -> "bucket_id", "test_space"."id"::unsigned -> "id")
                scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_except_on_bucket_id() {
    let input = r#"
    select "e", "bucket_id" from "t2"
    except
    select "id", "bucket_id" from "test_space"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"except
    projection ("t2"."e"::unsigned -> "e", "t2"."bucket_id"::unsigned -> "bucket_id")
        scan "t2"
    projection ("test_space"."id"::unsigned -> "id", "test_space"."bucket_id"::unsigned -> "bucket_id")
        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_exists_subquery_select_from_table() {
    let input = r#"SELECT "id" FROM "test_space" WHERE EXISTS (SELECT 0 FROM "hash_testing")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id")
    selection exists ROW($0)
        scan "test_space"
subquery $0:
motion [policy: full]
            scan
                projection (0::unsigned -> "col_1")
                    scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_not_exists_subquery_select_from_table() {
    let input = r#"SELECT "id" FROM "test_space" WHERE NOT EXISTS (SELECT 0 FROM "hash_testing")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id")
    selection not exists ROW($0)
        scan "test_space"
subquery $0:
motion [policy: full]
            scan
                projection (0::unsigned -> "col_1")
                    scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_exists_subquery_select_from_table_with_condition() {
    let input = r#"SELECT "id" FROM "test_space" WHERE EXISTS (SELECT 0 FROM "hash_testing" WHERE "identification_number" != 42)"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id")
    selection exists ROW($0)
        scan "test_space"
subquery $0:
motion [policy: full]
            scan
                projection (0::unsigned -> "col_1")
                    selection ROW("hash_testing"."identification_number"::integer) <> ROW(42::unsigned)
                        scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing" group by "identification_number", "product_code""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("column_596"::integer -> "identification_number", "column_696"::string -> "product_code")
    group by ("column_596"::integer, "column_696"::string) output: ("column_596"::integer -> "column_596", "column_696"::string -> "column_696")
        motion [policy: segment([ref("column_596"), ref("column_696")])]
            projection ("hash_testing"."identification_number"::integer -> "column_596", "hash_testing"."product_code"::string -> "column_696")
                group by ("hash_testing"."identification_number"::integer, "hash_testing"."product_code"::string) output: ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op", "hash_testing"."bucket_id"::unsigned -> "bucket_id")
                    scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_less_cols_in_proj() {
    // check case when we specify less columns than in groupby clause
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        GROUP BY "identification_number", "product_units"
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_596"::integer -> "identification_number")
    group by ("column_596"::integer, "column_696"::boolean) output: ("column_596"::integer -> "column_596", "column_696"::boolean -> "column_696")
        motion [policy: segment([ref("column_596"), ref("column_696")])]
            projection ("hash_testing"."identification_number"::integer -> "column_596", "hash_testing"."product_units"::boolean -> "column_696")
                group by ("hash_testing"."identification_number"::integer, "hash_testing"."product_units"::boolean) output: ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op", "hash_testing"."bucket_id"::unsigned -> "bucket_id")
                    scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_union_1() {
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        GROUP BY "identification_number"
        UNION ALL
        SELECT "identification_number" FROM "hash_testing""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    projection ("column_596"::integer -> "identification_number")
        group by ("column_596"::integer) output: ("column_596"::integer -> "column_596")
            motion [policy: segment([ref("column_596")])]
                projection ("hash_testing"."identification_number"::integer -> "column_596")
                    group by ("hash_testing"."identification_number"::integer) output: ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op", "hash_testing"."bucket_id"::unsigned -> "bucket_id")
                        scan "hash_testing"
    projection ("hash_testing"."identification_number"::integer -> "identification_number")
        scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_union_2() {
    let input = r#"SELECT "identification_number" FROM "hash_testing" UNION ALL
        SELECT * FROM (SELECT "identification_number" FROM "hash_testing"
        GROUP BY "identification_number"
        UNION ALL
        SELECT "identification_number" FROM "hash_testing")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    projection ("hash_testing"."identification_number"::integer -> "identification_number")
        scan "hash_testing"
    projection ("identification_number"::integer -> "identification_number")
        scan
            union all
                projection ("column_1196"::integer -> "identification_number")
                    group by ("column_1196"::integer) output: ("column_1196"::integer -> "column_1196")
                        motion [policy: segment([ref("column_1196")])]
                            projection ("hash_testing"."identification_number"::integer -> "column_1196")
                                group by ("hash_testing"."identification_number"::integer) output: ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op", "hash_testing"."bucket_id"::unsigned -> "bucket_id")
                                    scan "hash_testing"
                projection ("hash_testing"."identification_number"::integer -> "identification_number")
                    scan "hash_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_join_1() {
    // inner select is a kostyl because tables have the col sys_op
    let input = r#"SELECT "product_code", "product_units" FROM (SELECT "product_units", "product_code", "identification_number" FROM "hash_testing") as t2
        INNER JOIN (SELECT "id" from "test_space") as t
        ON t2."identification_number" = t."id"
        group by t2."product_code", t2."product_units"
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("column_2496"::string -> "product_code", "column_2596"::boolean -> "product_units")
    group by ("column_2496"::string, "column_2596"::boolean) output: ("column_2496"::string -> "column_2496", "column_2596"::boolean -> "column_2596")
        motion [policy: segment([ref("column_2496"), ref("column_2596")])]
            projection ("t2"."product_code"::string -> "column_2496", "t2"."product_units"::boolean -> "column_2596")
                group by ("t2"."product_code"::string, "t2"."product_units"::boolean) output: ("t2"."product_units"::boolean -> "product_units", "t2"."product_code"::string -> "product_code", "t2"."identification_number"::integer -> "identification_number", "t"."id"::unsigned -> "id")
                    join on ROW("t2"."identification_number"::integer) = ROW("t"."id"::unsigned)
                        scan "t2"
                            projection ("hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."identification_number"::integer -> "identification_number")
                                scan "hash_testing"
                        motion [policy: full]
                            scan "t"
                                projection ("test_space"."id"::unsigned -> "id")
                                    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
    println!("{}", plan.as_explain().unwrap());
}

#[test]
fn front_sql_join() {
    // test we can have not null and bool kind of condition in join
    let input = r#"SELECT "product_code", "product_units" FROM (SELECT "product_units", "product_code", "identification_number" FROM "hash_testing") as t2
        INNER JOIN (SELECT "id" from "test_space") as t
        ON t2."identification_number" = t."id" and t."id" is not null
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t2"."product_code"::string -> "product_code", "t2"."product_units"::boolean -> "product_units")
    join on ROW("t2"."identification_number"::integer) = ROW("t"."id"::unsigned) and not ROW("t"."id"::unsigned) is null
        scan "t2"
            projection ("hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."identification_number"::integer -> "identification_number")
                scan "hash_testing"
        motion [policy: full]
            scan "t"
                projection ("test_space"."id"::unsigned -> "id")
                    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());

    // here hash_single_testing is sharded by "identification_number", so it is a local join
    let input = r#"SELECT "product_code", "product_units" FROM (SELECT "product_units", "product_code", "identification_number" FROM "hash_single_testing") as t1
        INNER JOIN (SELECT "id" from "test_space") as t2
        ON t1."identification_number" = t2."id" and not t2."id" is null
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t1"."product_code"::string -> "product_code", "t1"."product_units"::boolean -> "product_units")
    join on ROW("t1"."identification_number"::integer) = ROW("t2"."id"::unsigned) and not ROW("t2"."id"::unsigned) is null
        scan "t1"
            projection ("hash_single_testing"."product_units"::boolean -> "product_units", "hash_single_testing"."product_code"::string -> "product_code", "hash_single_testing"."identification_number"::integer -> "identification_number")
                scan "hash_single_testing"
        scan "t2"
            projection ("test_space"."id"::unsigned -> "id")
                scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());

    // check we have no error, in case one of the join children has Distribution::Single
    let input = r#"SELECT "product_code", "product_units" FROM (SELECT "product_units", "product_code", "identification_number" FROM "hash_single_testing") as t1
        INNER JOIN (SELECT sum("id") as "id" from "test_space") as t2
        ON t1."identification_number" = t2."id" and t2."id" is not null
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // TODO: For the  hash function in the cartrisge runtime we can apply
    //       `motion [policy: segment([ref("id")])]` instead of the `motion [policy: full]`.
    let expected_explain = String::from(
        r#"projection ("t1"."product_code"::string -> "product_code", "t1"."product_units"::boolean -> "product_units")
    join on ROW("t1"."identification_number"::integer) = ROW("t2"."id"::decimal) and not ROW("t2"."id"::decimal) is null
        scan "t1"
            projection ("hash_single_testing"."product_units"::boolean -> "product_units", "hash_single_testing"."product_code"::string -> "product_code", "hash_single_testing"."identification_number"::integer -> "identification_number")
                scan "hash_single_testing"
        motion [policy: full]
            scan "t2"
                projection (sum(("sum_1796"::decimal))::decimal -> "id")
                    motion [policy: full]
                        projection (sum(("test_space"."id"::unsigned))::decimal -> "sum_1796")
                            scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
    println!("{}", plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_insert() {
    let input = r#"INSERT INTO "t" ("c", "b")
    SELECT "b", "d" FROM "t" group by "b", "d" ON CONFLICT DO FAIL"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([value(NULL), ref("d")])]
        projection ("column_596"::unsigned -> "b", "column_696"::unsigned -> "d")
            group by ("column_596"::unsigned, "column_696"::unsigned) output: ("column_596"::unsigned -> "column_596", "column_696"::unsigned -> "column_696")
                motion [policy: segment([ref("column_596"), ref("column_696")])]
                    projection ("t"."b"::unsigned -> "column_596", "t"."d"::unsigned -> "column_696")
                        group by ("t"."b"::unsigned, "t"."d"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                            scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
    println!("{}", plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_invalid() {
    let input = r#"select "b", "a" from "t" group by "b""#;

    let metadata = &RouterConfigurationMock::new();
    let mut plan = AbstractSyntaxTree::transform_into_plan(input, metadata).unwrap();
    let res = plan.optimize();

    assert_eq!(true, res.is_err());
}

#[test]
fn front_sql_distinct_invalid() {
    let input = r#"select "b", bucket_id(distinct cast("a" as string)) from "t" group by "b", bucket_id(distinct cast("a" as string))"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("DISTINCT modifier is allowed only for aggregate functions")
    );
}

#[test]
fn front_sql_aggregates() {
    let input = r#"SELECT "b", count("a") + count("b") FROM "t"
        group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_596"::unsigned -> "b", ROW(sum(("count_1496"::integer))::decimal) + ROW(sum(("count_1596"::integer))::decimal) -> "col_1")
    group by ("column_596"::unsigned) output: ("column_596"::unsigned -> "column_596", "count_1496"::integer -> "count_1496", "count_1596"::integer -> "count_1596")
        motion [policy: segment([ref("column_596")])]
            projection ("t"."b"::unsigned -> "column_596", count(("t"."a"::unsigned))::integer -> "count_1496", count(("t"."b"::unsigned))::integer -> "count_1596")
                group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_distinct_asterisk() {
    check_output(
        r#"select distinct * from (select "id" from "test_space_hist")
                 join (select "id" from "test_space") on true"#,
        vec![],
        r#"projection ("column_1996"::unsigned -> "id", "column_2096"::unsigned -> "id")
    group by ("column_1996"::unsigned, "column_2096"::unsigned) output: ("column_1996"::unsigned -> "column_1996", "column_2096"::unsigned -> "column_2096")
        motion [policy: segment([ref("column_1996"), ref("column_2096")])]
            projection ("id"::unsigned -> "column_1996", "id"::unsigned -> "column_2096")
                group by ("id"::unsigned, "id"::unsigned) output: ("id"::unsigned -> "id", "id"::unsigned -> "id")
                    join on true::boolean
                        scan
                            projection ("test_space_hist"."id"::unsigned -> "id")
                                scan "test_space_hist"
                        motion [policy: full]
                            scan
                                projection ("test_space"."id"::unsigned -> "id")
                                    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_avg_aggregate() {
    let input = r#"SELECT avg("b"), avg(distinct "b"), avg("b") * avg("b") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (sum(("sum_696"::decimal::double))::decimal / sum(("count_696"::decimal::double))::decimal -> "col_1", avg(distinct ("column_796"::decimal::double))::decimal -> "col_2", ROW(sum(("sum_696"::decimal::double))::decimal / sum(("count_696"::decimal::double))::decimal) * ROW(sum(("sum_696"::decimal::double))::decimal / sum(("count_696"::decimal::double))::decimal) -> "col_3")
    motion [policy: full]
        projection ("t"."b"::unsigned -> "column_796", sum(("t"."b"::unsigned))::decimal -> "sum_696", count(("t"."b"::unsigned))::integer -> "count_696")
            group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_total_aggregate() {
    let input = r#"SELECT total("b"), total(distinct "b") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (total(("total_696"::double))::double -> "col_1", total(distinct ("column_796"::double))::double -> "col_2")
    motion [policy: full]
        projection ("t"."b"::unsigned -> "column_796", total(("t"."b"::unsigned))::double -> "total_696")
            group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_min_aggregate() {
    let input = r#"SELECT min("b"), min(distinct "b") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (min(("min_696"::unsigned))::scalar -> "col_1", min(distinct ("column_796"::unsigned))::scalar -> "col_2")
    motion [policy: full]
        projection ("t"."b"::unsigned -> "column_796", min(("t"."b"::unsigned))::scalar -> "min_696")
            group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_max_aggregate() {
    let input = r#"SELECT max("b"), max(distinct "b") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (max(("max_696"::unsigned))::scalar -> "col_1", max(distinct ("column_796"::unsigned))::scalar -> "col_2")
    motion [policy: full]
        projection ("t"."b"::unsigned -> "column_796", max(("t"."b"::unsigned))::scalar -> "max_696")
            group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_group_concat_aggregate() {
    let input = r#"SELECT group_concat("FIRST_NAME"), group_concat(distinct "FIRST_NAME") FROM "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (group_concat(("group_concat_696"::string))::string -> "col_1", group_concat(distinct ("column_796"::string))::string -> "col_2")
    motion [policy: full]
        projection ("test_space"."FIRST_NAME"::string -> "column_796", group_concat(("test_space"."FIRST_NAME"::string))::string -> "group_concat_696")
            group by ("test_space"."FIRST_NAME"::string) output: ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op", "test_space"."bucket_id"::unsigned -> "bucket_id")
                scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_group_concat_aggregate2() {
    let input = r#"SELECT group_concat("FIRST_NAME", ' '), group_concat(distinct "FIRST_NAME") FROM "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (group_concat(("group_concat_696"::string, ' '::string))::string -> "col_1", group_concat(distinct ("column_796"::string))::string -> "col_2")
    motion [policy: full]
        projection ("test_space"."FIRST_NAME"::string -> "column_796", group_concat(("test_space"."FIRST_NAME"::string, ' '::string))::string -> "group_concat_696")
            group by ("test_space"."FIRST_NAME"::string) output: ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op", "test_space"."bucket_id"::unsigned -> "bucket_id")
                scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_count_asterisk1() {
    let input = r#"SELECT count(*), count(*) FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (sum(("count_596"::integer))::decimal -> "col_1", sum(("count_596"::integer))::decimal -> "col_2")
    motion [policy: full]
        projection (count((*::integer))::integer -> "count_596")
            scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_count_asterisk2() {
    let input = r#"SELECT cOuNt(*), "b" FROM "t" group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (sum(("count_1196"::integer))::decimal -> "col_1", "column_596"::unsigned -> "b")
    group by ("column_596"::unsigned) output: ("column_596"::unsigned -> "column_596", "count_1196"::integer -> "count_1196")
        motion [policy: segment([ref("column_596")])]
            projection ("t"."b"::unsigned -> "column_596", count((*::integer))::integer -> "count_1196")
                group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_invalid_count_asterisk1() {
    let input = r#"SELECT sum(*) FROM "t" group by "b""#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("\"*\" is allowed only inside \"count\" aggregate function.")
    );
}

#[test]
fn front_sql_aggregates_with_subexpressions() {
    let input = r#"SELECT "b", count("a" * "b" + 1), count(func("a")) FROM "t"
        group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_596"::unsigned -> "b", sum(("count_1496"::integer))::decimal -> "col_1", sum(("count_1796"::integer))::decimal -> "col_2")
    group by ("column_596"::unsigned) output: ("column_596"::unsigned -> "column_596", "count_1796"::integer -> "count_1796", "count_1496"::integer -> "count_1496")
        motion [policy: segment([ref("column_596")])]
            projection ("t"."b"::unsigned -> "column_596", count(("func"(("t"."a"::unsigned))::integer))::integer -> "count_1796", count((ROW("t"."a"::unsigned) * ROW("t"."b"::unsigned) + ROW(1::unsigned)))::integer -> "count_1496")
                group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_aggregates_with_distinct1() {
    let input = r#"SELECT "b", count(distinct "a"), count(distinct "b") FROM "t"
        group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("column_596"::unsigned -> "b", count(distinct ("column_1296"::integer))::integer -> "col_1", count(distinct ("column_596"::integer))::integer -> "col_2")
    group by ("column_596"::unsigned) output: ("column_596"::unsigned -> "column_596", "column_1296"::unsigned -> "column_1296")
        motion [policy: segment([ref("column_596")])]
            projection ("t"."b"::unsigned -> "column_596", "t"."a"::unsigned -> "column_1296")
                group by ("t"."b"::unsigned, "t"."a"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_aggregates_with_distinct2() {
    let input = r#"SELECT "b", sum(distinct "a" + "b" + 3) FROM "t"
        group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_596"::unsigned -> "b", sum(distinct ("column_1232"::decimal))::decimal -> "col_1")
    group by ("column_596"::unsigned) output: ("column_596"::unsigned -> "column_596", "column_1232"::unsigned -> "column_1232")
        motion [policy: segment([ref("column_596")])]
            projection ("t"."b"::unsigned -> "column_596", ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) + ROW(3::unsigned) -> "column_1232")
                group by ("t"."b"::unsigned, ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) + ROW(3::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_aggregates_with_distinct3() {
    let input = r#"SELECT sum(distinct "a" + "b" + 3) FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (sum(distinct ("column_632"::decimal))::decimal -> "col_1")
    motion [policy: full]
        projection (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) + ROW(3::unsigned) -> "column_632")
            group by (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) + ROW(3::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_aggregate_inside_aggregate() {
    let input = r#"select "b", count(sum("a")) from "t" group by "b""#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, metadata)
        .unwrap()
        .optimize()
        .unwrap_err();

    assert_eq!(
        "invalid query: aggregate function inside aggregate function is not allowed.",
        err.to_string()
    );
}

#[test]
fn front_sql_column_outside_aggregate_no_groupby() {
    let input = r#"select "b", count("a") from "t""#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, metadata)
        .unwrap()
        .optimize()
        .unwrap_err();

    assert_eq!(
        "invalid query: found column reference (\"b\") outside aggregate function",
        err.to_string()
    );
}

#[test]
fn front_sql_option_basic() {
    let input = r#"select * from "t" option(vdbe_max_steps = 1000, vtable_max_rows = 10)"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
    scan "t"
execution options:
    vdbe_max_steps = 1000
    vtable_max_rows = 10
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_option_with_param() {
    let input = r#"select * from "t" option(vdbe_max_steps = ?, vtable_max_rows = ?)"#;

    let plan = sql_to_optimized_ir(input, vec![Value::Unsigned(1000), Value::Unsigned(10)]);
    let expected_explain = String::from(
        r#"projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
    scan "t"
execution options:
    vdbe_max_steps = 1000
    vtable_max_rows = 10
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_pg_style_params1() {
    let input = r#"select $1, $2, $1 from "t""#;

    let plan = sql_to_optimized_ir(
        input,
        vec![Value::Unsigned(1000), Value::String("hi".into())],
    );
    let expected_explain = String::from(
        r#"projection (1000::unsigned -> "col_1", 'hi'::string -> "col_2", 1000::unsigned -> "col_3")
    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_pg_style_params2() {
    let input = r#"select $1, $2, $1 from "t" option(vdbe_max_steps = $1, vtable_max_rows = $1)"#;

    let plan = sql_to_optimized_ir(
        input,
        vec![Value::Unsigned(1000), Value::String("hi".into())],
    );
    let expected_explain = String::from(
        r#"projection (1000::unsigned -> "col_1", 'hi'::string -> "col_2", 1000::unsigned -> "col_3")
    scan "t"
execution options:
    vdbe_max_steps = 1000
    vtable_max_rows = 1000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_pg_style_params3() {
    let input = r#"select "a" + $1 from "t"
        where "a" = $1
        group by "a" + $1
        having count("b") > $1
        option(vdbe_max_steps = $1, vtable_max_rows = $1)"#;

    let plan = sql_to_optimized_ir(input, vec![Value::Unsigned(42)]);
    let expected_explain = String::from(
        r#"projection ("column_1132"::unsigned -> "col_1")
    having ROW(sum(("count_1896"::integer))::decimal) > ROW(42::unsigned)
        group by ("column_1132"::unsigned) output: ("column_1132"::unsigned -> "column_1132", "count_1896"::integer -> "count_1896")
            motion [policy: segment([ref("column_1132")])]
                projection (ROW("t"."a"::unsigned) + ROW(42::unsigned) -> "column_1132", count(("t"."b"::unsigned))::integer -> "count_1896")
                    group by (ROW("t"."a"::unsigned) + ROW(42::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        selection ROW("t"."a"::unsigned) = ROW(42::unsigned)
                            scan "t"
execution options:
    vdbe_max_steps = 42
    vtable_max_rows = 42
"#,
    );
    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_pg_style_params4() {
    let input = r#"select $1, ? from "t""#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, metadata).unwrap_err();

    assert_eq!(
        "invalid parameters usage. Got $n and ? parameters in one query!",
        err.to_string()
    );
}

#[test]
fn front_sql_pg_style_params5() {
    let input = r#"select $0 from "t""#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, metadata).unwrap_err();

    assert_eq!(
        "invalid query: $n parameters are indexed from 1!",
        err.to_string()
    );
}

#[test]
fn front_sql_option_defaults() {
    let input = r#"select * from "t" where "a" = ? and "b" = ?"#;

    let plan = sql_to_optimized_ir(input, vec![Value::Unsigned(1000), Value::Unsigned(10)]);
    let expected_explain = String::from(
        r#"projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
    selection ROW("t"."a"::unsigned) = ROW(1000::unsigned) and ROW("t"."b"::unsigned) = ROW(10::unsigned)
        scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_column_outside_aggregate() {
    let input = r#"select "b", "a", count("a") from "t" group by "b""#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, metadata)
        .unwrap()
        .optimize()
        .unwrap_err();

    assert_eq!(
        "invalid query: column \"a\" is not found in grouping expressions!",
        err.to_string()
    );
}

#[test]
fn front_sql_aggregate_without_groupby() {
    let input = r#"SELECT sum("a" * "b" + 1) FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (sum(("sum_796"::decimal))::decimal -> "col_1")
    motion [policy: full]
        projection (sum((ROW("t"."a"::unsigned) * ROW("t"."b"::unsigned) + ROW(1::unsigned)))::decimal -> "sum_796")
            scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_aggregate_without_groupby2() {
    let input = r#"SELECT * FROM (SELECT count("id") FROM "test_space") as "t1""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t1"."col_1"::integer -> "col_1")
    scan "t1"
        projection (sum(("count_696"::integer))::decimal -> "col_1")
            motion [policy: full]
                projection (count(("test_space"."id"::unsigned))::integer -> "count_696")
                    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_aggregate_on_aggregate() {
    let input = r#"SELECT max(c) FROM (SELECT count("id") as c FROM "test_space") as "t1""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (max(("t1"."c"::integer))::scalar -> "col_1")
    scan "t1"
        projection (sum(("count_696"::integer))::decimal -> "c")
            motion [policy: full]
                projection (count(("test_space"."id"::unsigned))::integer -> "count_696")
                    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_union_single_left() {
    let input = r#"
        SELECT "a" FROM "t"
        UNION ALL
        SELECT sum("a") FROM "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    projection ("t"."a"::unsigned -> "a")
        scan "t"
    motion [policy: segment([ref("col_1")])]
        projection (sum(("sum_1296"::decimal))::decimal -> "col_1")
            motion [policy: full]
                projection (sum(("t"."a"::unsigned))::decimal -> "sum_1296")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_union_single_right() {
    let input = r#"
        SELECT sum("a") FROM "t"
        UNION ALL
        SELECT "a" FROM "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    motion [policy: segment([ref("col_1")])]
        projection (sum(("sum_696"::decimal))::decimal -> "col_1")
            motion [policy: full]
                projection (sum(("t"."a"::unsigned))::decimal -> "sum_696")
                    scan "t"
    projection ("t"."a"::unsigned -> "a")
        scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_union_single_both() {
    let input = r#"
        SELECT sum("a") FROM "t"
        UNION ALL
        SELECT sum("a") FROM "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    motion [policy: segment([ref("col_1")])]
        projection (sum(("sum_696"::decimal))::decimal -> "col_1")
            motion [policy: full]
                projection (sum(("t"."a"::unsigned))::decimal -> "sum_696")
                    scan "t"
    motion [policy: segment([ref("col_1")])]
        projection (sum(("sum_1396"::decimal))::decimal -> "col_1")
            motion [policy: full]
                projection (sum(("t"."a"::unsigned))::decimal -> "sum_1396")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_single() {
    let input = r#"INSERT INTO "t" ("c", "b") SELECT sum("b"), count("d") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([value(NULL), ref("col_2")])]
        projection (sum(("sum_696"::decimal))::decimal -> "col_1", sum(("count_896"::integer))::decimal -> "col_2")
            motion [policy: full]
                projection (count(("t"."d"::unsigned))::integer -> "count_896", sum(("t"."b"::unsigned))::decimal -> "sum_696")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_except_single_right() {
    let input = r#"SELECT "a", "b" from "t"
        EXCEPT
        SELECT sum("a"), count("b") from "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"except
    projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b")
        scan "t"
    motion [policy: segment([ref("col_1"), ref("col_2")])]
        projection (sum(("sum_1396"::decimal))::decimal -> "col_1", sum(("count_1596"::integer))::decimal -> "col_2")
            motion [policy: full]
                projection (sum(("t"."a"::unsigned))::decimal -> "sum_1396", count(("t"."b"::unsigned))::integer -> "count_1596")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());

    let input = r#"SELECT "b", "a" from "t"
        EXCEPT
        SELECT sum("a"), count("b") from "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"except
    projection ("t"."b"::unsigned -> "b", "t"."a"::unsigned -> "a")
        scan "t"
    motion [policy: segment([ref("col_2"), ref("col_1")])]
        projection (sum(("sum_1396"::decimal))::decimal -> "col_1", sum(("count_1596"::integer))::decimal -> "col_2")
            motion [policy: full]
                projection (sum(("t"."a"::unsigned))::decimal -> "sum_1396", count(("t"."b"::unsigned))::integer -> "count_1596")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_except_single_left() {
    let input = r#"SELECT sum("a"), count("b") from "t"
        EXCEPT
        SELECT "a", "b" from "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"except
    motion [policy: segment([ref("col_1"), ref("col_2")])]
        projection (sum(("sum_696"::decimal))::decimal -> "col_1", sum(("count_896"::integer))::decimal -> "col_2")
            motion [policy: full]
                projection (sum(("t"."a"::unsigned))::decimal -> "sum_696", count(("t"."b"::unsigned))::integer -> "count_896")
                    scan "t"
    projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b")
        scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_except_single_both() {
    let input = r#"SELECT sum("a"), count("b") from "t"
        EXCEPT
        SELECT sum("a"), sum("b") from "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"except
    motion [policy: segment([ref("col_1")])]
        projection (sum(("sum_696"::decimal))::decimal -> "col_1", sum(("count_896"::integer))::decimal -> "col_2")
            motion [policy: full]
                projection (sum(("t"."a"::unsigned))::decimal -> "sum_696", count(("t"."b"::unsigned))::integer -> "count_896")
                    scan "t"
    motion [policy: segment([ref("col_1")])]
        projection (sum(("sum_1596"::decimal))::decimal -> "col_1", sum(("sum_1796"::decimal))::decimal -> "col_2")
            motion [policy: full]
                projection (sum(("t"."a"::unsigned))::decimal -> "sum_1596", sum(("t"."b"::unsigned))::decimal -> "sum_1796")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_expression() {
    let input = r#"SELECT "a"+"b" FROM "t"
        group by "a"+"b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_532"::unsigned -> "col_1")
    group by ("column_532"::unsigned) output: ("column_532"::unsigned -> "column_532")
        motion [policy: segment([ref("column_532")])]
            projection (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) -> "column_532")
                group by (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_expression2() {
    let input = r#"SELECT ("a"+"b") + count("a") FROM "t"
        group by ("a"+"b")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_632"::unsigned + ROW(sum(("count_1596"::integer))::decimal) -> "col_1")
    group by ("column_632"::unsigned) output: ("column_632"::unsigned -> "column_632", "count_1596"::integer -> "count_1596")
        motion [policy: segment([ref("column_632")])]
            projection ((ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned)) -> "column_632", count(("t"."a"::unsigned))::integer -> "count_1596")
                group by ((ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned))) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_expression3() {
    let input = r#"SELECT "a"+"b", ("c"*"d")*sum("c"*"d")/count("a"*"b") FROM "t"
        group by "a"+"b", "a"+"b", ("c"*"d")"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("column_532"::unsigned -> "col_1", "column_832"::unsigned * ROW(sum(("sum_2496"::decimal))::decimal) / ROW(sum(("count_2596"::integer))::decimal) -> "col_2")
    group by ("column_532"::unsigned, "column_832"::unsigned) output: ("column_532"::unsigned -> "column_532", "column_832"::unsigned -> "column_832", "sum_2496"::decimal -> "sum_2496", "count_2596"::integer -> "count_2596")
        motion [policy: segment([ref("column_532"), ref("column_832")])]
            projection (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) -> "column_532", (ROW("t"."c"::unsigned) * ROW("t"."d"::unsigned)) -> "column_832", sum((ROW("t"."c"::unsigned) * ROW("t"."d"::unsigned)))::decimal -> "sum_2496", count((ROW("t"."a"::unsigned) * ROW("t"."b"::unsigned)))::integer -> "count_2596")
                group by (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned), (ROW("t"."c"::unsigned) * ROW("t"."d"::unsigned))) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
    println!("{}", plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_expression4() {
    let input = r#"SELECT "a"+"b", "a" FROM "t"
        group by "a"+"b", "a""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("column_532"::unsigned -> "col_1", "column_796"::unsigned -> "a")
    group by ("column_532"::unsigned, "column_796"::unsigned) output: ("column_532"::unsigned -> "column_532", "column_796"::unsigned -> "column_796")
        motion [policy: segment([ref("column_532"), ref("column_796")])]
            projection (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) -> "column_532", "t"."a"::unsigned -> "column_796")
                group by (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned), "t"."a"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
    println!("{}", plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_with_aggregates() {
    let input = r#"
        select * from (select "a", "b", sum("c") as "c" from "t" group by "a", "b") as t1
        join (select "g", "e", sum("f") as "f" from "t2" group by "g", "e") as t2
        on (t1."a", t2."g") = (t2."e", t1."b")"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("t1"."a"::unsigned -> "a", "t1"."b"::unsigned -> "b", "t1"."c"::decimal -> "c", "t2"."g"::unsigned -> "g", "t2"."e"::unsigned -> "e", "t2"."f"::decimal -> "f")
    join on ROW("t1"."a"::unsigned, "t1"."b"::unsigned) = ROW("t2"."e"::unsigned, "t2"."g"::unsigned)
        scan "t1"
            projection ("column_596"::unsigned -> "a", "column_696"::unsigned -> "b", sum(("sum_1596"::decimal))::decimal -> "c")
                group by ("column_596"::unsigned, "column_696"::unsigned) output: ("column_596"::unsigned -> "column_596", "column_696"::unsigned -> "column_696", "sum_1596"::decimal -> "sum_1596")
                    motion [policy: segment([ref("column_596"), ref("column_696")])]
                        projection ("t"."a"::unsigned -> "column_596", "t"."b"::unsigned -> "column_696", sum(("t"."c"::unsigned))::decimal -> "sum_1596")
                            group by ("t"."a"::unsigned, "t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                                scan "t"
        motion [policy: segment([ref("e"), ref("g")])]
            scan "t2"
                projection ("column_2496"::unsigned -> "g", "column_2596"::unsigned -> "e", sum(("sum_3496"::decimal))::decimal -> "f")
                    group by ("column_2496"::unsigned, "column_2596"::unsigned) output: ("column_2496"::unsigned -> "column_2496", "column_2596"::unsigned -> "column_2596", "sum_3496"::decimal -> "sum_3496")
                        motion [policy: segment([ref("column_2496"), ref("column_2596")])]
                            projection ("t2"."g"::unsigned -> "column_2496", "t2"."e"::unsigned -> "column_2596", sum(("t2"."f"::unsigned))::decimal -> "sum_3496")
                                group by ("t2"."g"::unsigned, "t2"."e"::unsigned) output: ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h", "t2"."bucket_id"::unsigned -> "bucket_id")
                                    scan "t2"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_left_join() {
    let input = r#"SELECT * from (select "a" as a from "t") as o
        left outer join (select "b" as c, "d" as d from "t") as i
        on o.a = i.c
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("o"."a"::unsigned -> "a", "i"."c"::unsigned -> "c", "i"."d"::unsigned -> "d")
    left join on ROW("o"."a"::unsigned) = ROW("i"."c"::unsigned)
        scan "o"
            projection ("t"."a"::unsigned -> "a")
                scan "t"
        motion [policy: full]
            scan "i"
                projection ("t"."b"::unsigned -> "c", "t"."d"::unsigned -> "d")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_left_join_single_left() {
    let input = r#"
        select * from (select sum("id") / 3 as a from "test_space") as t1
        left outer join (select "id" as b from "test_space") as t2
        on t1.a = t2.b
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t1"."a"::decimal -> "a", "t2"."b"::unsigned -> "b")
    left join on ROW("t1"."a"::decimal) = ROW("t2"."b"::unsigned)
        motion [policy: segment([ref("a")])]
            scan "t1"
                projection (ROW(sum(("sum_696"::decimal))::decimal) / ROW(3::unsigned) -> "a")
                    motion [policy: full]
                        projection (sum(("test_space"."id"::unsigned))::decimal -> "sum_696")
                            scan "test_space"
        motion [policy: full]
            scan "t2"
                projection ("test_space"."id"::unsigned -> "b")
                    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
    println!("{}", plan.as_explain().unwrap());
}

#[test]
fn front_sql_left_join_single_left2() {
    let input = r#"
        select * from (select sum("id") / 3 as a from "test_space") as t1
        left join (select "id" as b from "test_space") as t2
        on t1.a + 3 != t2.b
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // full motion should be under outer child
    let expected_explain = String::from(
        r#"projection ("t1"."a"::decimal -> "a", "t2"."b"::unsigned -> "b")
    left join on ROW("t1"."a"::decimal) + ROW(3::unsigned) <> ROW("t2"."b"::unsigned)
        motion [policy: segment([ref("a")])]
            scan "t1"
                projection (ROW(sum(("sum_696"::decimal))::decimal) / ROW(3::unsigned) -> "a")
                    motion [policy: full]
                        projection (sum(("test_space"."id"::unsigned))::decimal -> "sum_696")
                            scan "test_space"
        motion [policy: full]
            scan "t2"
                projection ("test_space"."id"::unsigned -> "b")
                    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
    println!("{}", plan.as_explain().unwrap());
}

#[test]
fn front_sql_left_join_single_both() {
    let input = r#"
        select * from (select sum("id") / 3 as a from "test_space") as t1
        left join (select count("id") as b from "test_space") as t2
        on t1.a != t2.b
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // full motion should be under outer child
    let expected_explain = String::from(
        r#"projection ("t1"."a"::decimal -> "a", "t2"."b"::integer -> "b")
    left join on ROW("t1"."a"::decimal) <> ROW("t2"."b"::integer)
        scan "t1"
            projection (ROW(sum(("sum_696"::decimal))::decimal) / ROW(3::unsigned) -> "a")
                motion [policy: full]
                    projection (sum(("test_space"."id"::unsigned))::decimal -> "sum_696")
                        scan "test_space"
        scan "t2"
            projection (sum(("count_1496"::integer))::decimal -> "b")
                motion [policy: full]
                    projection (count(("test_space"."id"::unsigned))::integer -> "count_1496")
                        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
    println!("{}", plan.as_explain().unwrap());
}

#[test]
fn front_sql_nested_subqueries() {
    let input = r#"SELECT "a" FROM "t"
        WHERE "a" in (SELECT "a" FROM "t1" WHERE "a" in (SELECT "b" FROM "t1"))"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."a"::unsigned -> "a")
    selection ROW("t"."a"::unsigned) in ROW($1)
        scan "t"
subquery $0:
motion [policy: full]
                            scan
                                projection ("t1"."b"::integer -> "b")
                                    scan "t1"
subquery $1:
motion [policy: full]
            scan
                projection ("t1"."a"::string -> "a")
                    selection ROW("t1"."a"::string) in ROW($0)
                        scan "t1"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_having1() {
    let input = r#"SELECT "a", sum("b") FROM "t"
        group by "a"
        having "a" > 1 and sum(distinct "b") > 1
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("column_596"::unsigned -> "a", sum(("sum_2196"::decimal))::decimal -> "col_1")
    having ROW("column_596"::unsigned) > ROW(1::unsigned) and ROW(sum(distinct ("column_1296"::decimal))::decimal) > ROW(1::unsigned)
        group by ("column_596"::unsigned) output: ("column_596"::unsigned -> "column_596", "column_1296"::unsigned -> "column_1296", "sum_2196"::decimal -> "sum_2196")
            motion [policy: segment([ref("column_596")])]
                projection ("t"."a"::unsigned -> "column_596", "t"."b"::unsigned -> "column_1296", sum(("t"."b"::unsigned))::decimal -> "sum_2196")
                    group by ("t"."a"::unsigned, "t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_having2() {
    let input = r#"SELECT sum("a") * count(distinct "b"), sum("a") FROM "t"
        having sum(distinct "b") > 1 and sum("a") > 1
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection (ROW(sum(("sum_1696"::decimal))::decimal) * ROW(count(distinct ("column_1596"::integer))::integer) -> "col_1", sum(("sum_1696"::decimal))::decimal -> "col_2")
    having ROW(sum(distinct ("column_1596"::decimal))::decimal) > ROW(1::unsigned) and ROW(sum(("sum_1696"::decimal))::decimal) > ROW(1::unsigned)
        motion [policy: full]
            projection ("t"."b"::unsigned -> "column_1596", sum(("t"."a"::unsigned))::decimal -> "sum_1696")
                group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_having3() {
    let input = r#"SELECT sum("a") FROM "t"
        having sum("a") > 1
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection (sum(("sum_1396"::decimal))::decimal -> "col_1")
    having ROW(sum(("sum_1396"::decimal))::decimal) > ROW(1::unsigned)
        motion [policy: full]
            projection (sum(("t"."a"::unsigned))::decimal -> "sum_1396")
                scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_having4() {
    let input = r#"SELECT sum("a") FROM "t"
        having sum("a") > 1 and "b" > 1
    "#;

    let err = sql_to_optimized_ir_add_motions_err(input);

    assert_eq!(
        true,
        err.to_string()
            .contains("HAVING argument must appear in the GROUP BY clause or be used in an aggregate function")
    );
}

#[test]
fn front_sql_having_with_sq() {
    let input = r#"
        SELECT "sysFrom", sum(distinct "id") as "sum", count(distinct "id") as "count" from "test_space"
        group by "sysFrom"
        having (select "sysFrom" from "test_space" where "sysFrom" = 2) > count(distinct "id")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_596"::unsigned -> "sysFrom", sum(distinct ("column_3396"::decimal))::decimal -> "sum", count(distinct ("column_3396"::integer))::integer -> "count")
    having ROW($0) > ROW(count(distinct ("column_3396"::integer))::integer)
        group by ("column_596"::unsigned) output: ("column_596"::unsigned -> "column_596", "column_3396"::unsigned -> "column_3396")
            motion [policy: segment([ref("column_596")])]
                projection ("test_space"."sysFrom"::unsigned -> "column_596", "test_space"."id"::unsigned -> "column_3396")
                    group by ("test_space"."sysFrom"::unsigned, "test_space"."id"::unsigned) output: ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op", "test_space"."bucket_id"::unsigned -> "bucket_id")
                        scan "test_space"
subquery $0:
motion [policy: full]
            scan
                projection ("test_space"."sysFrom"::unsigned -> "sysFrom")
                    selection ROW("test_space"."sysFrom"::unsigned) = ROW(2::unsigned)
                        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_unmatched_column_in_having() {
    let input = r#"SELECT sum("a"), "a" FROM "t"
        group by "a"
        having sum("a") > 1 and "a" > 1 or "c" = 1
    "#;

    let err = sql_to_optimized_ir_add_motions_err(input);

    assert_eq!(
        true,
        err.to_string()
            .contains("column \"c\" is not found in grouping expressions!")
    );
}

#[test]
fn front_sql_having_with_sq_segment_motion() {
    // check subquery has Segment Motion on groupby columns
    let input = r#"
        SELECT "sysFrom", "sys_op", sum(distinct "id") as "sum", count(distinct "id") as "count" from "test_space"
        group by "sysFrom", "sys_op"
        having ("sysFrom", "sys_op") in (select "a", "d" from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_596"::unsigned -> "sysFrom", "column_696"::unsigned -> "sys_op", sum(distinct ("column_3296"::decimal))::decimal -> "sum", count(distinct ("column_3296"::integer))::integer -> "count")
    having ROW("column_596"::unsigned, "column_696"::unsigned) in ROW($0, $0)
        group by ("column_596"::unsigned, "column_696"::unsigned) output: ("column_596"::unsigned -> "column_596", "column_696"::unsigned -> "column_696", "column_3296"::unsigned -> "column_3296")
            motion [policy: segment([ref("column_596"), ref("column_696")])]
                projection ("test_space"."sysFrom"::unsigned -> "column_596", "test_space"."sys_op"::unsigned -> "column_696", "test_space"."id"::unsigned -> "column_3296")
                    group by ("test_space"."sysFrom"::unsigned, "test_space"."sys_op"::unsigned, "test_space"."id"::unsigned) output: ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op", "test_space"."bucket_id"::unsigned -> "bucket_id")
                        scan "test_space"
subquery $0:
motion [policy: segment([ref("a"), ref("d")])]
            scan
                projection ("t"."a"::unsigned -> "a", "t"."d"::unsigned -> "d")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
    println!("{}", plan.as_explain().unwrap());
}

#[test]
fn front_sql_having_with_sq_segment_local_motion() {
    // Check subquery has no Motion, as it has the same distribution
    // as columns used in GroupBy.
    let input = r#"
        SELECT "sysFrom", "sys_op", sum(distinct "id") as "sum", count(distinct "id") as "count" from "test_space"
        group by "sysFrom", "sys_op"
        having ("sysFrom", "sys_op") in (select "a", "b" from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_596"::unsigned -> "sysFrom", "column_696"::unsigned -> "sys_op", sum(distinct ("column_3296"::decimal))::decimal -> "sum", count(distinct ("column_3296"::integer))::integer -> "count")
    having ROW("column_596"::unsigned, "column_696"::unsigned) in ROW($0, $0)
        group by ("column_596"::unsigned, "column_696"::unsigned) output: ("column_596"::unsigned -> "column_596", "column_696"::unsigned -> "column_696", "column_3296"::unsigned -> "column_3296")
            motion [policy: segment([ref("column_596"), ref("column_696")])]
                projection ("test_space"."sysFrom"::unsigned -> "column_596", "test_space"."sys_op"::unsigned -> "column_696", "test_space"."id"::unsigned -> "column_3296")
                    group by ("test_space"."sysFrom"::unsigned, "test_space"."sys_op"::unsigned, "test_space"."id"::unsigned) output: ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op", "test_space"."bucket_id"::unsigned -> "bucket_id")
                        scan "test_space"
subquery $0:
scan
            projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b")
                scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_unique_local_aggregates() {
    // make sure we don't compute extra aggregates at local stage
    let input = r#"SELECT sum("a"), count("a"), sum("a") + count("a") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    // here we must compute only two aggregates at local stage: sum(a), count(a)
    let expected_explain = String::from(
        r#"projection (sum(("sum_696"::decimal))::decimal -> "col_1", sum(("count_896"::integer))::decimal -> "col_2", ROW(sum(("sum_696"::decimal))::decimal) + ROW(sum(("count_896"::integer))::decimal) -> "col_3")
    motion [policy: full]
        projection (count(("t"."a"::unsigned))::integer -> "count_896", sum(("t"."a"::unsigned))::decimal -> "sum_696")
            scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
    println!("{}", plan.as_explain().unwrap());
}

#[test]
fn front_sql_unique_local_groupings() {
    // make sure we don't compute extra group by columns at local stage
    let input = r#"SELECT sum(distinct "a"), count(distinct "a"), count(distinct "b") FROM "t"
        group by "b"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    // here we must compute only two groupby columns at local stage: a, b
    let expected_explain = String::from(
        r#"projection (sum(distinct ("column_1196"::decimal))::decimal -> "col_1", count(distinct ("column_1196"::integer))::integer -> "col_2", count(distinct ("column_596"::integer))::integer -> "col_3")
    group by ("column_596"::unsigned) output: ("column_596"::unsigned -> "column_596", "column_1196"::unsigned -> "column_1196")
        motion [policy: segment([ref("column_596")])]
            projection ("t"."b"::unsigned -> "column_596", "t"."a"::unsigned -> "column_1196")
                group by ("t"."b"::unsigned, "t"."a"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_join_table_with_bucket_id_as_first_col() {
    // here we are joining t3 who has bucket_id as its first column,
    // check that we correctly handle references in join condition,
    // after inserting SQ with Projection under outer child
    let input = r#"
SELECT * FROM
    "t3"
INNER JOIN
    (SELECT * FROM "hash_single_testing" INNER JOIN (SELECT "id" FROM "test_space") as "ts"
     ON "hash_single_testing"."identification_number" = "ts"."id") as "ij"
ON "t3"."a" = "ij"."id"
"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t3"."a"::string -> "a", "t3"."b"::integer -> "b", "ij"."identification_number"::integer -> "identification_number", "ij"."product_code"::string -> "product_code", "ij"."product_units"::boolean -> "product_units", "ij"."sys_op"::unsigned -> "sys_op", "ij"."id"::unsigned -> "id")
    join on ROW("t3"."a"::string) = ROW("ij"."id"::unsigned)
        scan "t3"
            projection ("t3"."a"::string -> "a", "t3"."b"::integer -> "b")
                scan "t3"
        scan "ij"
            projection ("hash_single_testing"."identification_number"::integer -> "identification_number", "hash_single_testing"."product_code"::string -> "product_code", "hash_single_testing"."product_units"::boolean -> "product_units", "hash_single_testing"."sys_op"::unsigned -> "sys_op", "ts"."id"::unsigned -> "id")
                join on ROW("hash_single_testing"."identification_number"::integer) = ROW("ts"."id"::unsigned)
                    scan "hash_single_testing"
                        projection ("hash_single_testing"."identification_number"::integer -> "identification_number", "hash_single_testing"."product_code"::string -> "product_code", "hash_single_testing"."product_units"::boolean -> "product_units", "hash_single_testing"."sys_op"::unsigned -> "sys_op")
                            scan "hash_single_testing"
                    scan "ts"
                        projection ("test_space"."id"::unsigned -> "id")
                            scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_select_distinct() {
    // make sure we don't compute extra group by columns at local stage
    let input = r#"SELECT distinct "a", "a" + "b" FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    // here we must compute only two groupby columns at local stage: a, b
    let expected_explain = String::from(
        r#"projection ("column_896"::unsigned -> "a", "column_832"::unsigned -> "col_1")
    group by ("column_896"::unsigned, "column_832"::unsigned) output: ("column_896"::unsigned -> "column_896", "column_832"::unsigned -> "column_832")
        motion [policy: segment([ref("column_896"), ref("column_832")])]
            projection ("t"."a"::unsigned -> "column_896", ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) -> "column_832")
                group by ("t"."a"::unsigned, ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
    println!("{}", plan.as_explain().unwrap());
}

#[test]
fn front_sql_select_distinct_asterisk() {
    let input = r#"SELECT distinct * FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("column_996"::unsigned -> "a", "column_1096"::unsigned -> "b", "column_1196"::unsigned -> "c", "column_1296"::unsigned -> "d")
    group by ("column_996"::unsigned, "column_1096"::unsigned, "column_1196"::unsigned, "column_1296"::unsigned) output: ("column_996"::unsigned -> "column_996", "column_1096"::unsigned -> "column_1096", "column_1196"::unsigned -> "column_1196", "column_1296"::unsigned -> "column_1296")
        motion [policy: segment([ref("column_996"), ref("column_1096"), ref("column_1196"), ref("column_1296")])]
            projection ("t"."a"::unsigned -> "column_996", "t"."b"::unsigned -> "column_1096", "t"."c"::unsigned -> "column_1196", "t"."d"::unsigned -> "column_1296")
                group by ("t"."a"::unsigned, "t"."b"::unsigned, "t"."c"::unsigned, "t"."d"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
    println!("{}", plan.as_explain().unwrap());
}

#[test]
fn front_sql_invalid_select_distinct() {
    let input = r#"SELECT distinct "a" FROM "t"
        group by "b"
    "#;

    let err = sql_to_optimized_ir_add_motions_err(input);

    assert_eq!(
        true,
        err.to_string()
            .contains("column \"a\" is not found in grouping expressions!")
    );
}

#[test]
fn front_sql_select_distinct_with_aggr() {
    let input = r#"SELECT distinct sum("a"), "b" FROM "t"
    group by "b"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection (sum(("sum_1296"::decimal))::decimal -> "col_1", "column_596"::unsigned -> "b")
    group by ("column_596"::unsigned) output: ("column_596"::unsigned -> "column_596", "sum_1296"::decimal -> "sum_1296")
        motion [policy: segment([ref("column_596")])]
            projection ("t"."b"::unsigned -> "column_596", sum(("t"."a"::unsigned))::decimal -> "sum_1296")
                group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_select_distinct_with_aggr2() {
    let input = r#"SELECT distinct sum("a") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection (sum(("sum_696"::decimal))::decimal -> "col_1")
    motion [policy: full]
        projection (sum(("t"."a"::unsigned))::decimal -> "sum_696")
            scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_on_conflict() {
    let mut input = r#"insert into "t" values (1, 1, 1, 1) on conflict do nothing"#;

    let mut plan = sql_to_optimized_ir(input, vec![]);
    let mut expected_explain = String::from(
        r#"insert "t" on conflict: nothing
    motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")])]
        values
            value row (data=ROW(1::unsigned, 1::unsigned, 1::unsigned, 1::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());

    input = r#"insert into "t" values (1, 1, 1, 1) on conflict do replace"#;
    plan = sql_to_optimized_ir(input, vec![]);
    expected_explain = String::from(
        r#"insert "t" on conflict: replace
    motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")])]
        values
            value row (data=ROW(1::unsigned, 1::unsigned, 1::unsigned, 1::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_1() {
    let input = r#"insert into "t" ("b") select "a" from "t"
        where "a" = 1 and "b" = 2 or "a" = 2 and "b" = 3"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([value(NULL), ref("a")])]
        projection ("t"."a"::unsigned -> "a")
            selection ROW("t"."a"::unsigned) = ROW(1::unsigned) and ROW("t"."b"::unsigned) = ROW(2::unsigned) or ROW("t"."a"::unsigned) = ROW(2::unsigned) and ROW("t"."b"::unsigned) = ROW(3::unsigned)
                scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_2() {
    let input = r#"insert into "t" ("a", "b") select "a", "b" from "t"
        where "a" = 1 and "b" = 2"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: local segment([ref("a"), ref("b")])]
        projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b")
            selection ROW("t"."a"::unsigned) = ROW(1::unsigned) and ROW("t"."b"::unsigned) = ROW(2::unsigned)
                scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_3() {
    // check different column order leads to Segment motion
    let input = r#"insert into "t" ("b", "a") select "a", "b" from "t"
        where "a" = 1 and "b" = 2 or "a" = 3 and "b" = 4"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([ref("b"), ref("a")])]
        projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b")
            selection ROW("t"."a"::unsigned) = ROW(1::unsigned) and ROW("t"."b"::unsigned) = ROW(2::unsigned) or ROW("t"."a"::unsigned) = ROW(3::unsigned) and ROW("t"."b"::unsigned) = ROW(4::unsigned)
                scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_4() {
    let input = r#"insert into "t" ("b", "a") select "b", "a" from "t"
        where "a" = 1 and "b" = 2"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: local segment([ref("a"), ref("b")])]
        projection ("t"."b"::unsigned -> "b", "t"."a"::unsigned -> "a")
            selection ROW("t"."a"::unsigned) = ROW(1::unsigned) and ROW("t"."b"::unsigned) = ROW(2::unsigned)
                scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_5() {
    let input = r#"insert into "t" ("b", "a") select 5, 6 from "t"
        where "a" = 1 and "b" = 2"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([ref("col_2"), ref("col_1")])]
        projection (5::unsigned -> "col_1", 6::unsigned -> "col_2")
            selection ROW("t"."a"::unsigned) = ROW(1::unsigned) and ROW("t"."b"::unsigned) = ROW(2::unsigned)
                scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_6() {
    // The values should be materialized on the router, and
    // then dispatched to storages.
    let input = r#"insert into "t" ("a", "b") values (1, 2), (1, 2), (3, 4)"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([ref("COLUMN_5"), ref("COLUMN_6")])]
        values
            value row (data=ROW(1::unsigned, 2::unsigned))
            value row (data=ROW(1::unsigned, 2::unsigned))
            value row (data=ROW(3::unsigned, 4::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_7() {
    // Check system column can't be inserted
    let input = r#"insert into "hash_testing" ("identification_number", "product_code", "bucket_id") values (1, 2, 3)"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
    let err = plan.unwrap_err();
    assert_eq!(
        true,
        err.to_string()
            .contains("system column \"bucket_id\" cannot be inserted")
    );
}

#[test]
fn front_sql_insert_8() {
    // Both table have the same columns, but hash_single_testing has different shard key
    let input = r#"insert into "hash_testing" select * from "hash_single_testing""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "hash_testing" on conflict: fail
    motion [policy: segment([ref("identification_number"), ref("product_code")])]
        projection ("hash_single_testing"."identification_number"::integer -> "identification_number", "hash_single_testing"."product_code"::string -> "product_code", "hash_single_testing"."product_units"::boolean -> "product_units", "hash_single_testing"."sys_op"::unsigned -> "sys_op")
            scan "hash_single_testing"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_9() {
    let input = r#"insert into "t" ("a", "b") values (?, ?)"#;

    let plan = sql_to_optimized_ir(input, vec![Value::from(1_u64), Value::from(2_u64)]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")])]
        values
            value row (data=ROW(1::unsigned, 2::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_update1() {
    let input = r#"update "t" set "a" = 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"update "t"
"b" = "col_1"
"d" = "col_3"
"a" = "col_0"
"c" = "col_2"
    motion [policy: segment([])]
        projection (1::unsigned -> "col_0", "t"."b"::unsigned -> "col_1", "t"."c"::unsigned -> "col_2", "t"."d"::unsigned -> "col_3", "t"."a"::unsigned -> "col_4", "t"."b"::unsigned -> "col_5")
            scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_update2() {
    let input = r#"update "t" set "c" = "a" + "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"update "t"
"c" = "col_0"
    motion [policy: local]
        projection (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) -> "col_0", "t"."b"::unsigned -> "col_1")
            scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_update3() {
    let input = r#"update "t" set "c" = "a" + "b" where "c" = 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"update "t"
"c" = "col_0"
    motion [policy: local]
        projection (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) -> "col_0", "t"."b"::unsigned -> "col_1")
            selection ROW("t"."c"::unsigned) = ROW(1::unsigned)
                scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_update4() {
    let input = r#"update "t" set
    "d" = "b1"*2,
    "c" = "b1"*2
    from (select "a" as "a1", "b" as "b1" from "t1")
    where "c" = "b1""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"update "t"
"d" = "col_0"
"c" = "col_0"
    motion [policy: local]
        projection (ROW("b1"::integer) * ROW(2::unsigned) -> "col_0", "t"."b"::unsigned -> "col_1")
            join on ROW("t"."c"::unsigned) = ROW("b1"::integer)
                scan "t"
                    projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
                        scan "t"
                motion [policy: full]
                    scan
                        projection ("t1"."a"::string -> "a1", "t1"."b"::integer -> "b1")
                            scan "t1"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_update5() {
    let input = r#"update "t3" set
    "b" = "id"
    from "test_space"
    where "a" = "id""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"update "t3"
"b" = "col_0"
    motion [policy: local]
        projection ("test_space"."id"::unsigned -> "col_0", "t3"."a"::string -> "col_1")
            join on ROW("t3"."a"::string) = ROW("test_space"."id"::unsigned)
                scan "t3"
                    projection ("t3"."a"::string -> "a", "t3"."b"::integer -> "b")
                        scan "t3"
                scan "test_space"
                    projection ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op")
                        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_update6() {
    let input = r#"update "t3" set
    "b" = 2
    where "b" in (select sum("b") as s from "t3")"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"update "t3"
"b" = "col_0"
    motion [policy: local]
        projection (2::unsigned -> "col_0", "t3"."a"::string -> "col_1")
            selection ROW("t3"."b"::integer) in ROW($0)
                scan "t3"
subquery $0:
motion [policy: full]
                    scan
                        projection (sum(("sum_796"::decimal))::decimal -> "s")
                            motion [policy: full]
                                projection (sum(("t3"."b"::integer))::decimal -> "sum_796")
                                    scan "t3"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_not_true() {
    assert_explain_eq(
        r#"
            SELECT "a" FROM "t" WHERE not true
        "#,
        vec![],
        r#"projection ("t"."a"::unsigned -> "a")
    selection not ROW(true::boolean)
        scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_equal() {
    assert_explain_eq(
        r#"
            SELECT * FROM (VALUES (1)) where not true = true
        "#,
        vec![],
        r#"projection ("COLUMN_1"::unsigned -> "COLUMN_1")
    selection not ROW(true::boolean) = ROW(true::boolean)
        scan
            values
                value row (data=ROW(1::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_cast() {
    assert_explain_eq(
        r#"
            SELECT * FROM (values (1)) where not cast('true' as boolean)
        "#,
        vec![],
        r#"projection ("COLUMN_1"::unsigned -> "COLUMN_1")
    selection not 'true'::string::bool
        scan
            values
                value row (data=ROW(1::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    )
}

#[test]
fn from_sql_not_column() {
    assert_explain_eq(
        r#"
            SELECT * FROM (values (true)) where not "COLUMN_1"
        "#,
        vec![],
        r#"projection ("COLUMN_1"::boolean -> "COLUMN_1")
    selection not "COLUMN_1"::boolean
        scan
            values
                value row (data=ROW(true::boolean))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_or() {
    assert_explain_eq(
        r#"
            SELECT * FROM (values (1)) where not true or true
        "#,
        vec![],
        r#"projection ("COLUMN_1"::unsigned -> "COLUMN_1")
    selection not ROW(true::boolean) or ROW(true::boolean)
        scan
            values
                value row (data=ROW(1::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_and_with_parentheses() {
    assert_explain_eq(
        r#"
            SELECT not (true and false) FROM (values (1))
        "#,
        vec![],
        r#"projection (not (ROW(true::boolean) and ROW(false::boolean)) -> "col_1")
    scan
        values
            value row (data=ROW(1::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_or_with_parentheses() {
    assert_explain_eq(
        r#"
            SELECT * FROM (values (1)) where not (true or true)
        "#,
        vec![],
        r#"projection ("COLUMN_1"::unsigned -> "COLUMN_1")
    selection not (ROW(true::boolean) or ROW(true::boolean))
        scan
            values
                value row (data=ROW(1::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_exists() {
    assert_explain_eq(
        r#"
            select * from (values (1)) where not exists (select * from (values (1)))
        "#,
        vec![],
        r#"projection ("COLUMN_1"::unsigned -> "COLUMN_1")
    selection not exists ROW($0)
        scan
            values
                value row (data=ROW(1::unsigned))
subquery $0:
scan
            projection ("COLUMN_2"::unsigned -> "COLUMN_2")
                scan
                    values
                        value row (data=ROW(1::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_in() {
    assert_explain_eq(
        r#"
            select * from (values (1)) where 1 not in (select * from (values (1)))
        "#,
        vec![],
        r#"projection ("COLUMN_1"::unsigned -> "COLUMN_1")
    selection not ROW(1::unsigned) in ROW($0)
        scan
            values
                value row (data=ROW(1::unsigned))
subquery $0:
motion [policy: full]
            scan
                projection ("COLUMN_2"::unsigned -> "COLUMN_2")
                    scan
                        values
                            value row (data=ROW(1::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_complex_query() {
    assert_explain_eq(
        r#"
            select not (not (cast('true' as boolean)) and 1 + (?) != false)
            from
                (select not "id" as "nid" from "test_space") as "ts"
                inner join
                (select not not "id" as "nnid" from "test_space") as "nts"
                on not "nid" = "nnid" * (?) or not false = cast((not not true) as bool)
            where not exists (select * from (values (1)) where not true = (?))
        "#,
        vec![Value::from(1), Value::from(1), Value::from(true)],
        r#"projection (not (not ('true'::string::bool) and ROW(1::unsigned) + (1::integer) <> ROW(false::boolean)) -> "col_1")
    selection not exists ROW($0)
        join on ROW("ts"."nid"::boolean) <> ROW("nts"."nnid"::boolean) * (1::integer) or ROW(false::boolean) <> ROW((not not ROW(true::boolean))::bool)
            scan "ts"
                projection (not ROW("test_space"."id"::unsigned) -> "nid")
                    scan "test_space"
            motion [policy: full]
                scan "nts"
                    projection (not not ROW("test_space"."id"::unsigned) -> "nnid")
                        scan "test_space"
subquery $0:
scan
            projection ("COLUMN_1"::unsigned -> "COLUMN_1")
                selection not ROW(true::boolean) = (true::boolean)
                    scan
                        values
                            value row (data=ROW(1::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_arithmetic_with_parentheses() {
    assert_explain_eq(
        r#"
            SELECT (1 + 2) * 3 FROM (values (1))
        "#,
        vec![],
        r#"projection ((ROW(1::unsigned) + ROW(2::unsigned)) * ROW(3::unsigned) -> "col_1")
    scan
        values
            value row (data=ROW(1::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_to_date() {
    assert_explain_eq(
        r#"
            SELECT to_date("COLUMN_1", '%Y/%d/%m') FROM (values ('2010/10/10'))
        "#,
        vec![],
        r#"projection ("to_date"(("COLUMN_1"::string, '%Y/%d/%m'::string))::datetime -> "col_1")
    scan
        values
            value row (data=ROW('2010/10/10'::string))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_current_date() {
    let input = r#"
    SELECT current_date FROM (values ('2010/10/10'))
    where to_date('2010/10/10', '%Y/%d/%m') < current_Date"#;

    let today = OffsetDateTime::now_utc().replace_time(Time::MIDNIGHT);
    let format = format_description::parse("[year]-[month]-[day]").unwrap();
    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = format!(
        r#"projection ({today} 0:00:00.0 +00:00:00::datetime -> "col_1")
    selection ROW("to_date"(('2010/10/10'::string, '%Y/%d/%m'::string))::datetime) < ROW({today} 0:00:00.0 +00:00:00::datetime)
        scan
            values
                value row (data=ROW('2010/10/10'::string))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
        today = today.format(&format).unwrap()
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_check_non_null_columns_specified() {
    let input = r#"insert into "test_space" ("sys_op") values (1)"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
    let err = plan.unwrap_err();
    assert_eq!(
        true,
        err.to_string()
            .contains("NonNull column \"id\" must be specified")
    );
}

fn assert_explain_eq(query: &str, params: Vec<Value>, expected: &str) {
    let plan = sql_to_optimized_ir(query, params);
    let actual = plan.as_explain().unwrap();
    assert_eq!(expected, actual);
}

#[test]
fn non_existent_references_in_values_do_not_panic() {
    // scenario: somebody mixed up " with '
    let input = r#"insert into "test_space" values(1, "nonexistent_reference")"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
    let err = plan.unwrap_err();

    assert!(err
        .to_string()
        .contains("Reference \"nonexistent_reference\" met under Values that is unsupported. For string literals use single quotes."));
}

#[test]
fn front_count_no_params() {
    let input = r#"select count() from "test_space""#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("invalid query: Expected one argument for aggregate: \"count\"")
    );
}

#[test]
fn front_mock_set_param_transaction() {
    let queries_to_check = vec![
        r#"set session default_param = default"#,
        r#"set session stringparam = 'value'"#,
        r#"set session identparam to ident"#,
        r#"set local intparam to -3"#,
        r#"set local doubleparam = -42.5"#,
        r#"set doubleparam = -42.5"#,
        r#"set local time zone local"#,
        r#"set time zone -3"#,
        r#"set time zone 'value'"#,
        r#"SET search_path TO my_schema, public;"#,
        r#"SET datestyle TO postgres, dmy;"#,
        r#"SET TIME ZONE 'PST8PDT';"#,
        r#"SET TIME ZONE 'Europe/Rome';"#,
        r#"SET param To list, 'of', 4, valuez;"#,
        r#"set transaction snapshot 'snapshot-string'"#,
        r#"set transaction read write"#,
        r#"set transaction read only"#,
        r#"set transaction deferrable"#,
        r#"set transaction not deferrable"#,
        r#"set transaction isolation level serializable"#,
        r#"set transaction isolation level repeatable read"#,
        r#"set transaction isolation level read commited"#,
        r#"set transaction isolation level read uncommited"#,
        r#"set session characteristics as transaction read only"#,
        r#"set session characteristics as transaction isolation level read commited"#,
    ];

    let metadata = &RouterConfigurationMock::new();
    for query in queries_to_check {
        let plan = AbstractSyntaxTree::transform_into_plan(query, metadata);
        assert!(plan.is_ok())
    }
}

#[test]
fn front_mock_partition_by() {
    let metadata = &RouterConfigurationMock::new();

    let queries_to_check = vec![
        r#"create table t(a int primary key) partition by list (a)"#,
        r#"create table t(a int primary key) partition by hash (a, b)"#,
        r#"create table t(a int primary key) partition by range (a, b, c)"#,
    ];
    for query in queries_to_check {
        let plan = AbstractSyntaxTree::transform_into_plan(query, metadata);
        assert!(plan.is_ok())
    }

    let queries_to_check = vec![
        r#"create table tp partition of t default"#,
        r#"create table tp partition of t default partition by range (a)"#,
        r#"create table tp partition of t for values in (1)"#,
        r#"create table tp partition of t for values in (1, 2)"#,
        r#"create table tp partition of t for values from (1) to (2)"#,
        r#"create table tp partition of t for values from (1, 3) to (2, 4)"#,
        r#"create table tp partition of t for values from (1, MINVALUE) to (2, MAXVALUE)"#,
        r#"create table tp partition of t for values with (modulus 1, remainder 2)"#,
        r#"create table tp partition of t for values with (modulus 1, remainder 2) partition by range (a, b, c)"#,
    ];
    for query in queries_to_check {
        let err = AbstractSyntaxTree::transform_into_plan(query, metadata).unwrap_err();
        assert!(err
            .to_string()
            .contains("PARTITION OF logic is not supported yet"))
    }
}

#[test]
fn front_create_table_with_tier_syntax() {
    let query = r#"CREATE TABLE warehouse (
        id INTEGER PRIMARY KEY,
        type TEXT NOT NULL)
        USING memtx
        DISTRIBUTED BY (id)
        IN TIER "default";"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(query, metadata);
    assert!(plan.is_ok());

    let query = r#"CREATE TABLE warehouse (
        id INTEGER PRIMARY KEY,
        type TEXT NOT NULL)
        USING memtx
        DISTRIBUTED BY (id)
        IN TIER;"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(query, metadata);
    assert!(plan.is_err());
}

#[test]
fn front_alter_system_check_parses_ok() {
    let queries_to_check_ok = vec![
        r#"alter system set param_name = 1"#,
        r#"alter system set "param_name" = 1"#,
        r#"alter system set param_name = 'value'"#,
        r#"alter system set param_name = true"#,
        r#"alter system set param_name to 1"#,
        r#"alter system set param_name to 2.3"#,
        r#"alter system set param_name to default"#,
        r#"alter system set param_name to 'value'"#,
        r#"alter system set param_name to null"#,
        r#"alter system reset all"#,
        r#"alter system reset param_name"#,
        r#"alter system reset "param_name""#,
        r#"alter system reset "param_name" for all tiers"#,
        r#"alter system reset "param_name" for tier tier_name"#,
        r#"alter system reset "param_name" for tier "tier_name""#,
    ];
    let metadata = &RouterConfigurationMock::new();
    for query in queries_to_check_ok {
        let plan = AbstractSyntaxTree::transform_into_plan(query, metadata);
        assert!(plan.is_ok())
    }

    let queries_to_check_all_expressions_not_supported = vec![
        r#"alter system set param_name = ?"#,
        r#"alter system set param_name = 1 + 1"#,
    ];
    let metadata = &RouterConfigurationMock::new();
    for query in queries_to_check_all_expressions_not_supported {
        let err = AbstractSyntaxTree::transform_into_plan(query, metadata).unwrap_err();
        assert!(err
            .to_string()
            .contains("ALTER SYSTEM currently supports only literals as values."))
    }
}

#[test]
fn front_subqueries_interpreted_as_expression() {
    let input = r#"select (values (2)) from "test_space""#;
    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (ROW($0) -> "col_1")
    scan "test_space"
subquery $0:
scan
        values
            value row (data=ROW(2::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_subqueries_interpreted_as_expression_as_required_child() {
    let input = r#"select * from (select (values (1)) from "test_space")"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("col_1"::unsigned -> "col_1")
    scan
        projection (ROW($0) -> "col_1")
            scan "test_space"
subquery $0:
scan
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
fn front_subqueries_interpreted_as_expression_nested() {
    let input = r#"select (values ((values (2)))) from "test_space""#;
    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (ROW($1) -> "col_1")
    scan "test_space"
subquery $0:
scan
                    values
                        value row (data=ROW(2::unsigned))
subquery $1:
scan
        values
            value row (data=ROW(ROW($0)))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_subqueries_interpreted_as_expression_under_group_by() {
    let input = r#"SELECT COUNT(*) FROM "test_space" GROUP BY "id" + (VALUES (1))"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (sum(("count_1496"::integer))::decimal -> "col_1")
    group by ("column_932"::unsigned) output: ("column_932"::unsigned -> "column_932", "count_1496"::integer -> "count_1496")
        motion [policy: segment([ref("column_932")])]
            projection (ROW("test_space"."id"::unsigned) + ROW($1) -> "column_932", count((*::integer))::integer -> "count_1496")
                group by (ROW("test_space"."id"::unsigned) + ROW($0)) output: ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op", "test_space"."bucket_id"::unsigned -> "bucket_id")
                    scan "test_space"
subquery $0:
scan
                        values
                            value row (data=ROW(1::unsigned))
subquery $1:
scan
                    values
                        value row (data=ROW(1::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    println!("{}", plan.as_explain().unwrap());
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_select_without_scan() {
    let input = r#"select 1"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (1::unsigned -> "col_1")
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_select_without_scan_2() {
    let input = r#"select (values (1)), (select count(*) from t2)"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (ROW($1) -> "col_1", ROW($0) -> "col_2")
subquery $0:
motion [policy: full]
        scan
            projection (sum(("count_796"::integer))::decimal -> "col_1")
                motion [policy: full]
                    projection (count((*::integer))::integer -> "count_796")
                        scan "t2"
subquery $1:
scan
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
fn front_select_without_scan_3() {
    let input = r#"select *"#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, metadata).unwrap_err();

    assert_eq!(
        "invalid type: expected a Column in SelectWithoutScan, got Asterisk.",
        err.to_string()
    );
}

#[test]
fn front_select_without_scan_4() {
    let input = r#"select distinct 1"#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, metadata).unwrap_err();

    assert_eq!(
        "invalid type: expected a Column in SelectWithoutScan, got Distinct.",
        err.to_string()
    );
}

#[test]
fn front_select_without_scan_5() {
    let input = r#"select (?, ?) in (select e, f from t2) as foo"#;
    let plan = sql_to_optimized_ir(input, vec![Value::Unsigned(1), Value::Unsigned(1)]);

    let expected_explain = String::from(
        r#"projection (ROW(1::unsigned, 1::unsigned) in ROW($0, $0) -> "foo")
subquery $0:
motion [policy: full]
        scan
            projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f")
                scan "t2"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_select_without_scan_6() {
    let input = r#"select (select 1) from t2 where f in (select 3 as foo)"#;
    let plan = sql_to_optimized_ir(input, vec![Value::Unsigned(1), Value::Unsigned(1)]);

    let expected_explain = String::from(
        r#"projection (ROW($1) -> "col_1")
    selection ROW("t2"."f"::unsigned) in ROW($0)
        scan "t2"
subquery $0:
scan
            projection (3::unsigned -> "foo")
subquery $1:
scan
        projection (1::unsigned -> "col_1")
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_check_concat_with_parameters() {
    let input = r#"values (? || ?)"#;

    let plan = sql_to_optimized_ir(input, vec![Value::from("a"), Value::from("b")]);

    let expected_explain = String::from(
        r#"values
    value row (data=ROW(ROW('a'::string) || ROW('b'::string)))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_different_values_row_len() {
    let input = r#"values (1), (1,2)"#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, metadata).unwrap_err();

    assert_eq!(
        "invalid query: all values rows must have the same length",
        err.to_string()
    );
}

#[test]
fn front_sql_whitespaces_are_not_ignored() {
    // Deletion of any WHITESPACE in those query will transform
    // them into invalid.
    let correct_queries = [
        r#"create user "emir" with password 'vildanov' using md5"#,
        r#"set value to key"#,
        r#"set transaction isolation level read commited"#,
        r#"grant create on user vasya to emir option(timeout=1)"#,
        r#"alter plugin "abc" 0.1.0 remove service "svc1" from tier "tier1" option(timeout=11)"#,
        r#"create table if not exists t(a int primary key,b int) using memtx distributed by(a,b) wait applied locally option(timeout=1)"#,
        r#"create procedure if not exists name(int,int,varchar(1)) language sql as $$insert into t values(1,2)$$ wait applied globally"#,
        r#"with cte1(a,b) as(select * from t),cte2 as(select * from t) select * from t join t on true group by a having b order by a union all select * from t"#,
        r#"select cast(1 as int) or not exists (values(true)) and 1+1 and true or (a in (select * from t)) and i is not null"#,
    ];

    fn whitespace_positions(s: &str) -> Vec<usize> {
        s.char_indices()
            .filter_map(|(pos, c)| if c.is_whitespace() { Some(pos) } else { None })
            .collect()
    }

    for query in correct_queries {
        let res = ParseTree::parse(Rule::Command, query);
        if res.is_err() {
            println!("Query [{query}] is invalid.")
        }
        assert!(res.is_ok());
    }

    for query in correct_queries {
        let whitespaces = whitespace_positions(query);
        for wp_idx in whitespaces {
            let mut fixed = String::new();
            fixed.push_str(&query[..wp_idx]);
            fixed.push_str(&query[wp_idx + 1..]);
            let res = ParseTree::parse(Rule::Command, &fixed);
            if res.is_ok() {
                println!("Query [{fixed}] is valid.")
            }
            assert!(res.is_err())
        }
    }
}

#[cfg(test)]
mod coalesce;
#[cfg(test)]
mod cte;
#[cfg(test)]
mod ddl;
#[cfg(test)]
mod funcs;
#[cfg(test)]
mod global;
#[cfg(test)]
mod insert;
#[cfg(test)]
mod join;
#[cfg(test)]
mod like;
#[cfg(test)]
mod limit;
#[cfg(test)]
mod params;
#[cfg(test)]
mod single;
#[cfg(test)]
mod subtree_cloner;
#[cfg(test)]
mod trim;
#[cfg(test)]
mod union;
#[cfg(test)]
mod update;
