use crate::backend::sql::ir::PatternWithParams;
use crate::ir::transformation::dnf::tests::set_dnf;
use crate::ir::transformation::helpers::check_transformation;
use crate::ir::value::Value;
use crate::ir::Plan;

fn push_down_not(plan: &mut Plan) {
    plan.push_down_not().unwrap();
}

#[test]
fn not_true() {
    let input = r#"SELECT * FROM (values (1)) where not true"#;
    let expected = PatternWithParams::new(
        r#"SELECT * FROM (VALUES (?)) WHERE (?)"#.to_string(),
        vec![Value::Unsigned(1), Value::from(false)],
    );
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(actual, expected);
}

#[test]
fn not_double() {
    let input = r#"SELECT * FROM (values (1)) where not not true"#;
    let expected = PatternWithParams::new(
        r#"SELECT * FROM (VALUES (?)) WHERE (?)"#.to_string(),
        vec![Value::Unsigned(1), Value::from(true)],
    );
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(actual, expected);
}

#[test]
fn not_null() {
    let input = r#"SELECT * FROM (values (1)) where not null"#;
    let expected = PatternWithParams::new(
        r#"SELECT * FROM (VALUES (?)) WHERE (?)"#.to_string(),
        vec![Value::Unsigned(1), Value::Null],
    );
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(actual, expected);
}

#[test]
fn not_and() {
    let input = r#"SELECT * FROM (values (1)) where not (true and false)"#;
    let expected = PatternWithParams::new(
        r#"SELECT * FROM (VALUES (?)) WHERE ((?) or (?))"#.to_string(),
        vec![Value::Unsigned(1), Value::from(false), Value::from(true)],
    );
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(actual, expected);
}

#[test]
fn not_or() {
    let input = r#"SELECT * FROM (values (1)) where not (false or true)"#;
    let expected = PatternWithParams::new(
        r#"SELECT * FROM (VALUES (?)) WHERE ((?) and (?))"#.to_string(),
        vec![Value::Unsigned(1), Value::from(true), Value::from(false)],
    );
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(actual, expected);
}

#[test]
fn not_dnf() {
    let initial_input = r#"SELECT "a" FROM "t"
    WHERE NOT ((("a" != 1 AND "b" != 2) OR "a" != 3) AND "c" != 4)"#;
    let actual = check_transformation(initial_input, vec![], &push_down_not);
    let actual_after_dnf = check_transformation(actual.pattern.as_str(), actual.params, &set_dnf);

    let expected = r#"SELECT "a" FROM "t"
    WHERE (("a" = 1 or "b" = 2) and "a" = 3) or "c" = 4"#;
    let expected_after_dnf = check_transformation(expected, vec![], &set_dnf);

    assert_eq!(actual_after_dnf, expected_after_dnf);
}

#[test]
fn not_nothing_to_push_down() {
    let input = r#"SELECT "a" FROM "t"
    WHERE (("a" != 1 AND "b" != 2 OR "a" != 3) AND "c" != 4)"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ((("t"."a") <> (?) and ("t"."b") <> (?) or ("t"."a") <> (?)) and ("t"."c") <> (?))"#,
        ),
        vec![
            Value::from(1_u64),
            Value::from(2_u64),
            Value::from(3_u64),
            Value::from(4_u64),
        ],
    );
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(actual, expected);
}
