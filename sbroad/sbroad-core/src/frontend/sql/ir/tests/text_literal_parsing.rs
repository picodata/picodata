use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::value::Value;

#[test]
fn text_literal_is_parsed_to_bool() {
    // Test for `coerce_scalar_expr` & `collect_strings_to_be_coerced`
    //
    // Text literal is coerced to the type desired by the context with `false`.
    let pattern = "explain select coalesce('f', false);";
    let plan = sql_to_optimized_ir(pattern, vec![Value::from(0_i64), Value::from(1_i64)]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (coalesce((false::boolean, false::boolean))::any -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn text_literal_is_left_as_text_due_to_exlicit_cast() {
    // Test for `coerce_scalar_expr` & `collect_strings_to_be_coerced`
    //
    // In contrast to `text_literal_is_parsed_to_bool`, the text literal is not coerced to the
    // type desired by the type cast, because explicit type casts prevent coercion.
    let pattern = "explain select coalesce('f'::bool, false);";
    let plan = sql_to_optimized_ir(pattern, vec![Value::from(0_i64), Value::from(1_i64)]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (coalesce(('f'::string::bool, false::boolean))::any -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}
