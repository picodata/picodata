use crate::ir::transformation::helpers::sql_to_optimized_ir;
use pretty_assertions::assert_eq;

#[test]
fn coalesce_in_projection() {
    let sql = r#"SELECT COALESCE(NULL, "FIRST_NAME") FROM "test_space""#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection (coalesce((NULL::scalar, "test_space"."FIRST_NAME"::string))::scalar -> "col_1")
    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn coalesce_in_selection() {
    let sql = r#"SELECT "FIRST_NAME" FROM "test_space" WHERE COALESCE("FIRST_NAME", '(none)') = '(none)'"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."FIRST_NAME"::string -> "FIRST_NAME")
    selection ROW(coalesce(("test_space"."FIRST_NAME"::string, '(none)'::string))::scalar) = ROW('(none)'::string)
        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}
