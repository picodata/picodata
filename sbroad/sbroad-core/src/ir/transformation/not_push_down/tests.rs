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
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(actual.params, vec![Value::Unsigned(1), Value::from(false)],);
    insta::assert_snapshot!(
        actual.pattern,
        @"SELECT * FROM (VALUES (CAST($1 AS unsigned))) WHERE CAST($2 AS boolean)"
    );
}

#[test]
fn not_double() {
    let input = r#"SELECT * FROM (values (1)) where not not true"#;
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(actual.params, vec![Value::Unsigned(1), Value::from(true)],);
    insta::assert_snapshot!(
        actual.pattern,
        @"SELECT * FROM (VALUES (CAST($1 AS unsigned))) WHERE CAST($2 AS boolean)"
    );
}

#[test]
fn not_null() {
    let input = r#"SELECT * FROM (values (1)) where not null"#;
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(actual.params, vec![Value::Unsigned(1), Value::Null]);
    insta::assert_snapshot!(
        actual.pattern,
        @"SELECT * FROM (VALUES (CAST($1 AS unsigned))) WHERE not $2",
    );
}

#[test]
fn not_and() {
    let input = r#"SELECT * FROM (values (1)) where not (true and false)"#;
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(
        actual.params,
        vec![Value::Unsigned(1), Value::from(false), Value::from(true)],
    );
    insta::assert_snapshot!(
        actual.pattern,
        @"SELECT * FROM (VALUES (CAST($1 AS unsigned))) WHERE CAST($2 AS boolean) or CAST($3 AS boolean)"
    );
}

#[test]
fn not_or() {
    let input = r#"SELECT * FROM (values (1)) where not (false or true)"#;
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(
        actual.params,
        vec![Value::Unsigned(1), Value::from(true), Value::from(false)],
    );
    insta::assert_snapshot!(
        actual.pattern,
        @"SELECT * FROM (VALUES (CAST($1 AS unsigned))) WHERE CAST($2 AS boolean) and CAST($3 AS boolean)",
    );
}

#[test]
fn not_dnf() {
    let initial_input = r#"SELECT "a" FROM "t"
    WHERE NOT ((("a" != 1 AND "b" != 2) OR "a" != 3) AND "c" != 4)"#;
    let actual = check_transformation(initial_input, vec![], &push_down_not);
    let actual_after_dnf = check_transformation(actual.pattern.as_str(), actual.params, &set_dnf);

    assert_eq!(
        actual_after_dnf.params,
        vec![
            Value::Unsigned(1),
            Value::Unsigned(3),
            Value::Unsigned(2),
            Value::Unsigned(4),
        ],
    );

    // As we call check_transformation twice, to_sql is called twice,
    // so parameters are parameterized twice.
    insta::assert_snapshot!(
        actual_after_dnf.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ((("t"."a" = CAST (CAST($1 AS unsigned) as unsigned)) and ("t"."a" = CAST (CAST($2 AS unsigned) as unsigned))) or (("t"."b" = CAST (CAST($3 AS unsigned) as unsigned)) and ("t"."a" = CAST (CAST($2 AS unsigned) as unsigned)))) or ("t"."c" = CAST (CAST($4 AS unsigned) as unsigned))"#
    );
}

#[test]
fn not_nothing_to_push_down() {
    let input = r#"SELECT "a" FROM "t"
    WHERE (("a" != 1 AND "b" != 2 OR "a" != 3) AND "c" != 4)"#;
    let actual_pattern_params = check_transformation(input, vec![], &push_down_not);

    assert_eq!(
        actual_pattern_params.params,
        vec![
            Value::from(1_u64),
            Value::from(2_u64),
            Value::from(3_u64),
            Value::from(4_u64),
        ]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ((("t"."a" <> CAST($1 AS unsigned)) and ("t"."b" <> CAST($2 AS unsigned))) or ("t"."a" <> CAST($3 AS unsigned))) and ("t"."c" <> CAST($4 AS unsigned))"#
    );
}
