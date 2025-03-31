use crate::backend::sql::ir::PatternWithParams;
use crate::ir::transformation::helpers::check_transformation;
use crate::ir::value::Value;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

pub fn set_dnf(plan: &mut Plan) {
    plan.set_dnf().unwrap();
}

#[test]
fn dnf1() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 AND "b" = 2 OR "a" = 3) AND "c" = 4"#;
    let actual_pattern_params = check_transformation(input, vec![], &set_dnf);

    assert_eq!(
        actual_pattern_params.params,
        vec![
            Value::from(1_u64),
            Value::from(2_u64),
            Value::from(4_u64),
            Value::from(3_u64),
            Value::from(4_u64),
        ]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE (((("t"."a") = (?)) and (("t"."b") = (?))) and (("t"."c") = (?))) or ((("t"."a") = (?)) and (("t"."c") = (?)))"#
    );
}

#[test]
fn dnf2() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 OR "b" = 2) AND ("a" = 3 OR "c" = 4)"#;
    let actual_pattern_params = check_transformation(input, vec![], &set_dnf);

    assert_eq!(
        actual_pattern_params.params,
        vec![
            Value::from(3_u64),
            Value::from(1_u64),
            Value::from(4_u64),
            Value::from(1_u64),
            Value::from(3_u64),
            Value::from(2_u64),
            Value::from(4_u64),
            Value::from(2_u64),
        ]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ((((("t"."a") = (?)) and (("t"."a") = (?))) or ((("t"."c") = (?)) and (("t"."a") = (?)))) or ((("t"."a") = (?)) and (("t"."b") = (?)))) or ((("t"."c") = (?)) and (("t"."b") = (?)))"#
    );
}

#[test]
fn dnf3() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 OR "b" = 2) AND NULL"#;
    let actual_pattern_params = check_transformation(input, vec![], &set_dnf);

    assert_eq!(
        actual_pattern_params.params,
        vec![
            Value::from(1_u64),
            Value::Null,
            Value::from(2_u64),
            Value::Null,
        ]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ((("t"."a") = (?)) and (?)) or ((("t"."b") = (?)) and (?))"#
    );
}

#[test]
fn dnf4() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 OR "b" = 2) AND true"#;
    let actual_pattern_params = check_transformation(input, vec![], &set_dnf);

    assert_eq!(
        actual_pattern_params.params,
        vec![
            Value::from(1_u64),
            Value::Boolean(true),
            Value::from(2_u64),
            Value::Boolean(true),
        ]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ((("t"."a") = (?)) and (?)) or ((("t"."b") = (?)) and (?))"#
    );
}

#[test]
fn dnf5() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 OR "b" = 2) AND ((false))"#;
    let actual_pattern_params = check_transformation(input, vec![], &set_dnf);

    assert_eq!(
        actual_pattern_params.params,
        vec![
            Value::from(1_u64),
            Value::Boolean(false),
            Value::from(2_u64),
            Value::Boolean(false),
        ]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ((("t"."a") = (?)) and (?)) or ((("t"."b") = (?)) and (?))"#
    );
}

#[test]
fn dnf6() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 and "c" = 1 OR "b" = 2"#;
    let actual_pattern_params = check_transformation(input, vec![], &set_dnf);

    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(1_u64), Value::from(1_u64), Value::from(2_u64)]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ((("t"."a") = (?)) and (("t"."c") = (?))) or (("t"."b") = (?))"#
    );
}
