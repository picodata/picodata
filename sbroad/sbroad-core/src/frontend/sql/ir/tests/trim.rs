use crate::ir::transformation::helpers::sql_to_optimized_ir;
use pretty_assertions::assert_eq;

#[test]
fn trim() {
    let sql = r#"SELECT TRIM("FIRST_NAME") FROM "test_space""#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection (TRIM("test_space"."FIRST_NAME"::string) -> "col_1")
    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn trim_leading_from() {
    let sql = r#"SELECT TRIM(LEADING FROM "FIRST_NAME") FROM "test_space""#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection (TRIM(leading from "test_space"."FIRST_NAME"::string) -> "col_1")
    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn trim_both_space_from() {
    let sql = r#"SELECT TRIM(BOTH ' ' FROM "FIRST_NAME") FROM "test_space""#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection (TRIM(both ' '::string from "test_space"."FIRST_NAME"::string) -> "col_1")
    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
#[should_panic]
fn trim_trailing_without_from_should_fail() {
    let sql = r#"SELECT TRIM(TRAILING "FIRST_NAME") FROM "test_space""#;
    let _plan = sql_to_optimized_ir(sql, vec![]);
}
