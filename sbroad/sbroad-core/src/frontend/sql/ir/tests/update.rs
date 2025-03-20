use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::value::Value;

#[test]
fn update1() {
    let pattern = r#"UPDATE "test_space" SET "FIRST_NAME" = 'test'"#;
    let plan = sql_to_optimized_ir(pattern, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    update "test_space"
    "FIRST_NAME" = "col_0"
        motion [policy: local]
            projection ('test'::string -> "col_0", "test_space"."id"::unsigned -> "col_1")
                scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn update2() {
    let pattern = r#"UPDATE "test_space" SET "FIRST_NAME" = ?"#;
    let plan = sql_to_optimized_ir(pattern, vec![Value::from("test")]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    update "test_space"
    "FIRST_NAME" = "col_0"
        motion [policy: local]
            projection ('test'::string -> "col_0", "test_space"."id"::unsigned -> "col_1")
                scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}
