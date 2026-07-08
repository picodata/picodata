use sql_executor::test_helpers::sql_to_ir;
use sql_explain::explain::explain_logical;

#[test]
fn text_literal_is_parsed_to_bool() {
    // Test for `coerce_scalar_expr` & `collect_strings_to_be_coerced`
    //
    // Text literal is coerced to the type desired by the context with `false`.
    let pattern = "explain (logical) select coalesce('f', false);";
    let plan = sql_to_ir(pattern, vec![]);

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @"projection (coalesce(false::bool::bool, false::bool::bool)::any -> col_1)");
}

#[test]
fn text_literal_is_left_as_text_due_to_exlicit_cast() {
    // Test for `coerce_scalar_expr` & `collect_strings_to_be_coerced`
    //
    // In contrast to `text_literal_is_parsed_to_bool`, the text literal is not coerced to the
    // type desired by the type cast, because explicit type casts prevent coercion.
    let pattern = "explain (logical) select coalesce('f'::bool, false);";
    let plan = sql_to_ir(pattern, vec![]);

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @"projection (coalesce('f'::string::bool, false::bool::bool)::any -> col_1)");
}
