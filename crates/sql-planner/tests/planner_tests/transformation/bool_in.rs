use pretty_assertions::assert_eq;
use sql::helpers::check_transformation;
use sql::ir::value::Value;
use sql::ir::Plan;

fn replace_in_operator(plan: Plan) -> Plan {
    plan.replace_in_operator().unwrap()
}

#[test]
fn bool_in1() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" IN (1, 2, 3)"#;
    let actual_pattern_params = check_transformation(input, vec![], &replace_in_operator);

    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(1), Value::from(2), Value::from(3)]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE "t"."a" = $1::int or "t"."a" = $2::int or "t"."a" = $3::int"#
    );
}

#[test]
fn bool_in3() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" IN (1, 2) AND "b" IN (3)"#;
    let actual_pattern_params = check_transformation(input, vec![], &replace_in_operator);

    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(1), Value::from(2), Value::from(3)]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ("t"."a" = $1::int or "t"."a" = $2::int) and "t"."b" = $3::int"#
    );
}

#[test]
fn bool_in4() {
    // check bool expression in cast expression will be replaced.
    let input = r#"SELECT "a" FROM "t" WHERE cast(("a" IN (1, 2)) as integer) - 1 = 0"#;
    let actual_pattern_params = check_transformation(input, vec![], &replace_in_operator);

    assert_eq!(
        actual_pattern_params.params,
        vec![
            Value::from(1),
            Value::from(2),
            Value::from(1),
            Value::from(0),
        ]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ("t"."a" = $1::int or "t"."a" = $2::int)::int - $3::int = $4::int"#
    );
}

#[test]
fn bool_in5() {
    // check bool expression inside function expression will be replaced.
    let input = r#"SELECT "a" FROM "t" WHERE trim(("a" IN (1, 2))::text) < '1'"#;
    let actual_pattern_params = check_transformation(input, vec![], &replace_in_operator);

    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(1), Value::from(2), Value::from("1")]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE TRIM (("t"."a" = $1::int or "t"."a" = $2::int)::text) < $3::text"#
    );
}
