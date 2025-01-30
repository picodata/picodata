use crate::ir::transformation::helpers::sql_to_optimized_ir;
use pretty_assertions::assert_eq;

#[test]
fn trim() {
    let sql = r#"SELECT TRIM("FIRST_NAME") FROM "test_space""#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (TRIM("test_space"."FIRST_NAME"::string) -> "col_1")
        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn trim_leading_from() {
    let sql = r#"SELECT TRIM(LEADING FROM "FIRST_NAME") FROM "test_space""#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (TRIM(leading from "test_space"."FIRST_NAME"::string) -> "col_1")
        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn trim_both_space_from() {
    let sql = r#"SELECT TRIM(BOTH ' ' FROM "FIRST_NAME") FROM "test_space""#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (TRIM(both ' '::string from "test_space"."FIRST_NAME"::string) -> "col_1")
        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
#[should_panic]
fn trim_trailing_without_from_should_fail() {
    let sql = r#"SELECT TRIM(TRAILING "FIRST_NAME") FROM "test_space""#;
    let _plan = sql_to_optimized_ir(sql, vec![]);
}
