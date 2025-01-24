use crate::ir::transformation::helpers::sql_to_optimized_ir;
use pretty_assertions::assert_eq;

#[test]
fn lower_upper() {
    let input = r#"select upper(lower('a' || 'B')), upper(a) from t1"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    println!("{}", plan.as_explain().unwrap());

    let expected_explain = String::from(
        r#"projection (upper((lower((ROW('a'::string) || ROW('B'::string)))::string))::string -> "col_1", upper(("t1"."a"::string))::string -> "col_2")
    scan "t1"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}
