use sql_executor::test_helpers::sql_to_optimized_ir;
use sql_explain::explain::explain_logical;

#[test]
fn coalesce_in_projection() {
    let sql = r#"explain (logical) SELECT COALESCE(NULL, "FIRST_NAME") FROM "test_space""#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (coalesce(NULL::unknown, test_space."FIRST_NAME"::string::string)::any -> col_1)
      scan test_space
    "#);
}

#[test]
fn coalesce_in_selection() {
    let sql = r#"explain (logical) SELECT "FIRST_NAME" FROM "test_space" WHERE COALESCE("FIRST_NAME", '(none)') = '(none)'"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (test_space."FIRST_NAME"::string -> "FIRST_NAME")
      selection (coalesce(test_space."FIRST_NAME"::string::string, '(none)'::string)::any = '(none)'::string)
        scan test_space
    "#);
}
