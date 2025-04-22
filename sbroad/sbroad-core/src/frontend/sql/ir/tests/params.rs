use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::value::Value;

#[test]
fn front_param_in_cast() {
    let pattern = r#"SELECT CAST(? AS INTEGER) FROM "test_space""#;
    let plan = sql_to_optimized_ir(pattern, vec![Value::from(1_i64)]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (1::integer::int -> "col_1")
        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_params1() {
    let pattern = r#"SELECT "id", "FIRST_NAME" FROM "test_space"
        WHERE "sys_op" = ? AND "sysFrom" > ?"#;
    let plan = sql_to_optimized_ir(pattern, vec![Value::from(0_i64), Value::from(1_i64)]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::unsigned -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
        selection (ROW("test_space"."sys_op"::unsigned) = ROW(0::integer)) and (ROW("test_space"."sysFrom"::unsigned) > ROW(1::integer))
            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_params2() {
    let pattern = r#"SELECT "id" FROM "test_space"
        WHERE "sys_op" = ? AND "FIRST_NAME" = ?"#;

    let plan = sql_to_optimized_ir(pattern, vec![Value::Null, Value::from("hello")]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::unsigned -> "id")
        selection (ROW("test_space"."sys_op"::unsigned) = ROW(NULL::unknown)) and (ROW("test_space"."FIRST_NAME"::string) = ROW('hello'::string))
            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

// check cyrillic params support
#[test]
fn front_params3() {
    let pattern = r#"SELECT "id" FROM "test_space"
        WHERE "sys_op" = ? AND "FIRST_NAME" = ?"#;

    let plan = sql_to_optimized_ir(pattern, vec![Value::Null, Value::from("кириллица")]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::unsigned -> "id")
        selection (ROW("test_space"."sys_op"::unsigned) = ROW(NULL::unknown)) and (ROW("test_space"."FIRST_NAME"::string) = ROW('кириллица'::string))
            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

// check symbols in values (grammar)
#[test]
fn front_params4() {
    let pattern = r#"SELECT "id" FROM "test_space"
        WHERE "FIRST_NAME" = ?"#;

    let plan = sql_to_optimized_ir(
        pattern,
        vec![Value::from(r#"''± !@#$%^&*()_+=-\/><";:,.`~"#)],
    );

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::unsigned -> "id")
        selection ROW("test_space"."FIRST_NAME"::string) = ROW('''± !@#$%^&*()_+=-\/><";:,.`~'::string)
            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

// check parameter binding order, when selection has sub-queries
#[test]
fn front_params5() {
    let pattern = r#"
        SELECT "id" FROM "test_space"
        WHERE "sys_op" = ? OR "id" IN (
            SELECT "sysFrom" FROM "test_space_hist"
            WHERE "sys_op" = ?
        )
    "#;

    let plan = sql_to_optimized_ir(pattern, vec![Value::from(0_i64), Value::from(1_i64)]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::unsigned -> "id")
        selection (ROW("test_space"."sys_op"::unsigned) = ROW(0::integer)) or (ROW("test_space"."id"::unsigned) in ROW($0))
            scan "test_space"
    subquery $0:
    motion [policy: segment([ref("sysFrom")])]
                scan
                    projection ("test_space_hist"."sysFrom"::unsigned -> "sysFrom")
                        selection ROW("test_space_hist"."sys_op"::unsigned) = ROW(1::integer)
                            scan "test_space_hist"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_params6() {
    let pattern = r#"
        SELECT "id" FROM "test_space"
        WHERE "sys_op" = ? OR "id" NOT IN (
            SELECT "id" FROM "test_space"
            WHERE "sys_op" = ?
            UNION ALL
            SELECT "id" FROM "test_space"
            WHERE "sys_op" = ?
        )
    "#;

    let plan = sql_to_optimized_ir(
        pattern,
        vec![Value::from(0_i64), Value::from(1_i64), Value::from(2_i64)],
    );

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::unsigned -> "id")
        selection (ROW("test_space"."sys_op"::unsigned) = ROW(0::integer)) or (not (ROW("test_space"."id"::unsigned) in ROW($0)))
            scan "test_space"
    subquery $0:
    motion [policy: full]
                scan
                    union all
                        projection ("test_space"."id"::unsigned -> "id")
                            selection ROW("test_space"."sys_op"::unsigned) = ROW(1::integer)
                                scan "test_space"
                        projection ("test_space"."id"::unsigned -> "id")
                            selection ROW("test_space"."sys_op"::unsigned) = ROW(2::integer)
                                scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}
