use crate::ir::transformation::helpers::check_transformation;
use crate::ir::value::Value;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

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
        @r#"SELECT "t"."a" FROM "t" WHERE (("t"."a" = CAST($1 AS int)) or ("t"."a" = CAST($2 AS int))) or ("t"."a" = CAST($3 AS int))"#
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
        @r#"SELECT "t"."a" FROM "t" WHERE (("t"."a" = CAST($1 AS int)) or ("t"."a" = CAST($2 AS int))) and ("t"."b" = CAST($3 AS int))"#
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
        @r#"SELECT "t"."a" FROM "t" WHERE (CAST ((("t"."a" = CAST($1 AS int)) or ("t"."a" = CAST($2 AS int))) as int) - CAST($3 AS int)) = CAST($4 AS int)"#
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
        @r#"SELECT "t"."a" FROM "t" WHERE TRIM (CAST ((("t"."a" = CAST($1 AS int)) or ("t"."a" = CAST($2 AS int))) as string)) < CAST($3 AS string)"#
    );
}
