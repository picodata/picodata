use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::value::Value;
use pretty_assertions::assert_eq;

#[test]
fn update1() {
    let pattern = r#"UPDATE "test_space" SET "FIRST_NAME" = 'test'"#;
    let plan = sql_to_optimized_ir(pattern, vec![]);

    let expected_explain = String::from(
        r#"update "test_space"
"FIRST_NAME" = "col_0"
    motion [policy: local]
        projection ('test'::string -> "col_0", "test_space"."id"::unsigned -> "col_1")
            scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn update2() {
    let pattern = r#"UPDATE "test_space" SET "FIRST_NAME" = ?"#;
    let plan = sql_to_optimized_ir(pattern, vec![Value::from("test")]);

    let expected_explain = String::from(
        r#"update "test_space"
"FIRST_NAME" = "col_0"
    motion [policy: local]
        projection ('test'::string -> "col_0", "test_space"."id"::unsigned -> "col_1")
            scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}
