use super::*;

#[ignore = "(1806) while const cast is not reworked"]
#[test]
fn concat1_test() {
    let sql = r#"SELECT CAST('1' as string) || 'hello' FROM "t1""#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ('1'::string || 'hello'::string -> "col_1")
        scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[ignore = "(1806) while const cast is not reworked"]
#[test]
fn concat2_test() {
    let sql = r#"SELECT "a" FROM "t1" WHERE CAST('1' as string) || "a" || '2' = '42'"#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t1"."a"::string -> "a")
        selection (('1'::string || "t1"."a"::string) || '2'::string) = '42'::string
            scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}
