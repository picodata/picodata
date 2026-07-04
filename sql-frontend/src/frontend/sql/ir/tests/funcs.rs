use sql_executor::test_helpers::sql_to_optimized_ir;

#[test]
fn lower_upper() {
    let input = r#"explain (logical) select upper(lower('a' || 'B')), upper(a) from t1"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r"
    projection (upper(lower(('a'::string || 'B'::string)::string)::string::string)::string -> col_1, upper(t1.a::string::string)::string -> col_2)
      scan t1
    ");
}
