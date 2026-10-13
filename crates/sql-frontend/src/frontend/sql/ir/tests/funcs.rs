use crate::frontend::sql::transform_into_plan;
use sql_executor::executor::engine::mock::RouterConfigurationMock;
use sql_executor::test_helpers::sql_to_optimized_ir;
use sql_explain::explain::explain_logical;

#[test]
fn lower_upper() {
    let input = r#"explain (logical) select upper(lower('a' || 'B')), upper(a) from t1"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    projection (upper(lower(('a'::string || 'B'::string)::string)::string::string)::string -> col_1, upper(t1.a::string::string)::string -> col_2)
      scan t1
    ");
}

/// A quoted aggregate name is matched as written.
#[test]
fn quoted_aggregate_name_is_case_sensitive() {
    let metadata = &RouterConfigurationMock::new();
    assert!(transform_into_plan(r#"select "count"(a) from t1"#, &[], metadata).is_ok());

    let err = transform_into_plan(r#"select "COUNT"(a) from t1"#, &[], metadata).unwrap_err();
    assert_eq!("SQL function COUNT not found", err.to_string());
}
