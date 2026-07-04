use sql_executor::test_helpers::sql_to_optimized_ir;

#[test]
fn trim() {
    let sql = r#"explain (logical) SELECT TRIM("FIRST_NAME") FROM "test_space""#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (TRIM(test_space."FIRST_NAME"::string) -> col_1)
      scan test_space
    "#);
}

#[test]
fn trim_leading_from() {
    let sql = r#"explain (logical) SELECT TRIM(LEADING FROM "FIRST_NAME") FROM "test_space""#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (TRIM(leading from test_space."FIRST_NAME"::string) -> col_1)
      scan test_space
    "#);
}

#[test]
fn trim_both_space_from() {
    let sql = r#"explain (logical) SELECT TRIM(BOTH ' ' FROM "FIRST_NAME") FROM "test_space""#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (TRIM(both ' '::string from test_space."FIRST_NAME"::string) -> col_1)
      scan test_space
    "#);
}

#[test]
#[should_panic]
fn trim_trailing_without_from_should_fail() {
    let sql = r#"SELECT TRIM(TRAILING "FIRST_NAME") FROM "test_space""#;
    let _plan = sql_to_optimized_ir(sql, vec![]);
}
