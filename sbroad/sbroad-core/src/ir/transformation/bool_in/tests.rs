use crate::backend::sql::ir::PatternWithParams;
use crate::ir::transformation::helpers::check_transformation;
use crate::ir::value::Value;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

fn replace_in_operator(plan: &mut Plan) {
    plan.replace_in_operator().unwrap();
}

#[test]
fn bool_in1() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" IN (1, 2, 3)"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ("t"."a") = (?) or ("t"."a") = (?) or ("t"."a") = (?)"#,
        ),
        vec![Value::from(1_u64), Value::from(2_u64), Value::from(3_u64)],
    );

    assert_eq!(
        check_transformation(input, vec![], &replace_in_operator),
        expected
    );
}

#[test]
fn bool_in2() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a", "b") IN ((1, 10), (2, 20), (3, 30))"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ("t"."a", "t"."b") = (?, ?) or ("t"."a", "t"."b") = (?, ?) or ("t"."a", "t"."b") = (?, ?)"#,
        ),
        vec![
            Value::from(1_u64),
            Value::from(10_u64),
            Value::from(2_u64),
            Value::from(20_u64),
            Value::from(3_u64),
            Value::from(30_u64),
        ],
    );

    assert_eq!(
        check_transformation(input, vec![], &replace_in_operator),
        expected
    );
}

#[test]
fn bool_in3() {
    // Note: as soon as DNF transformation is not applied, there are not parentheses around OR
    //       operator (that is an invalid SQL transformation).
    //       In case we apply the whole optimization transformation, we'll get a correct output
    //       query.
    let input = r#"SELECT "a" FROM "t" WHERE "a" IN (1, 2) AND "b" IN (3)"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ("t"."a") = (?) or ("t"."a") = (?) and ("t"."b") = (?)"#,
        ),
        vec![Value::from(1_u64), Value::from(2_u64), Value::from(3_u64)],
    );

    assert_eq!(
        check_transformation(input, vec![], &replace_in_operator),
        expected
    );
}

#[test]
fn bool_in4() {
    // check bool expression in cast expression will be replaced.
    let input = r#"SELECT "a" FROM "t" WHERE cast(("a" IN (1, 2)) as integer) - 1 = 0"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE (CAST ((("t"."a") = (?) or ("t"."a") = (?)) as int)) - (?) = (?)"#,
        ),
        vec![
            Value::from(1_u64),
            Value::from(2_u64),
            Value::from(1_u64),
            Value::from(0_u64),
        ],
    );

    assert_eq!(
        check_transformation(input, vec![], &replace_in_operator),
        expected
    );
}

#[test]
fn bool_in5() {
    // check bool expression inside function expression will be replaced.
    let input = r#"SELECT "a" FROM "t" WHERE func(("a" IN (1, 2))) < 1"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ("func" ((("t"."a") = (?) or ("t"."a") = (?)))) < (?)"#,
        ),
        vec![Value::from(1_u64), Value::from(2_u64), Value::from(1_u64)],
    );

    assert_eq!(
        check_transformation(input, vec![], &replace_in_operator),
        expected
    );
}
