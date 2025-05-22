use crate::ir::transformation::helpers::sql_to_optimized_ir;

#[test]
fn coalesce_in_projection() {
    let sql = r#"SELECT COALESCE(NULL, "FIRST_NAME") FROM "test_space""#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (coalesce((NULL::unknown, "test_space"."FIRST_NAME"::string))::any -> "col_1")
        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn coalesce_in_selection() {
    let sql = r#"SELECT "FIRST_NAME" FROM "test_space" WHERE COALESCE("FIRST_NAME", '(none)') = '(none)'"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."FIRST_NAME"::string -> "FIRST_NAME")
        selection coalesce(("test_space"."FIRST_NAME"::string, '(none)'::string))::any = '(none)'::string
            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}
