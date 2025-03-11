use super::*;

#[test]
fn concat1_test() {
    let sql = r#"SELECT CAST('1' as string) || 'hello' FROM "t1""#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (ROW('1'::string) || ROW('hello'::string) -> "col_1")
        scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn concat2_test() {
    let sql = r#"SELECT "a" FROM "t1" WHERE CAST('1' as string) || "a" || '2' = '42'"#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t1"."a"::string -> "a")
        selection ROW(ROW(ROW('1'::string) || ROW("t1"."a"::string)) || ROW('2'::string)) = ROW('42'::string)
            scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}
