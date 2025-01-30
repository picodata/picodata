use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::value::Value;
use pretty_assertions::assert_eq;

#[test]
fn insert1() {
    let pattern = r#"INSERT INTO "test_space"("id", "FIRST_NAME") VALUES(?, ?)"#;
    let plan = sql_to_optimized_ir(pattern, vec![Value::from(1_i64), Value::from("test")]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "test_space" on conflict: fail
        motion [policy: segment([ref("COLUMN_1")])]
            values
                value row (data=ROW(1::integer, 'test'::string))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn insert2() {
    let pattern = r#"INSERT INTO "test_space"("id", "FIRST_NAME") VALUES(1, 'test')"#;
    let plan = sql_to_optimized_ir(pattern, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "test_space" on conflict: fail
        motion [policy: segment([ref("COLUMN_1")])]
            values
                value row (data=ROW(1::unsigned, 'test'::string))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn insert3() {
    let pattern = r#"INSERT INTO "test_space"("id", "sys_op")
        SELECT "id", "id" FROM "test_space""#;
    let plan = sql_to_optimized_ir(pattern, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "test_space" on conflict: fail
        motion [policy: local segment([ref("id")])]
            projection ("test_space"."id"::unsigned -> "id", "test_space"."id"::unsigned -> "id")
                scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}
