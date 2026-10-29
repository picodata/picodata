use super::dnf::set_dnf;
use sql::helpers::check_transformation;
use sql::ir::value::Value;
use sql::ir::Plan;

fn push_down_not(plan: &mut Plan) {
    plan.push_down_not().unwrap();
}

#[test]
fn not_true() {
    let input = r#"SELECT * FROM (values (1)) where not true"#;
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(actual.params, vec![Value::Integer(1), Value::from(false)],);
    insta::assert_snapshot!(
        actual.pattern,
        @r#"SELECT * FROM (VALUES ($1::int)) as "unnamed_subquery" WHERE $2::bool"#
    );
}

#[test]
fn not_double() {
    let input = r#"SELECT * FROM (values (1)) where not not true"#;
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(actual.params, vec![Value::Integer(1), Value::from(true)],);
    insta::assert_snapshot!(
        actual.pattern,
        @r#"SELECT * FROM (VALUES ($1::int)) as "unnamed_subquery" WHERE $2::bool"#
    );
}

#[test]
fn not_null() {
    let input = r#"SELECT * FROM (values (1)) where not null"#;
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(actual.params, vec![Value::Integer(1), Value::Null]);
    insta::assert_snapshot!(
        actual.pattern,
        @r#"SELECT * FROM (VALUES ($1::int)) as "unnamed_subquery" WHERE not $2"#,
    );
}

#[test]
fn not_and() {
    let input = r#"SELECT * FROM (values (1)) where not (true and false)"#;
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(
        actual.params,
        vec![Value::Integer(1), Value::from(false), Value::from(true)],
    );
    insta::assert_snapshot!(
        actual.pattern,
        @r#"SELECT * FROM (VALUES ($1::int)) as "unnamed_subquery" WHERE $2::bool or $3::bool"#
    );
}

#[test]
fn not_or() {
    let input = r#"SELECT * FROM (values (1)) where not (false or true)"#;
    let actual = check_transformation(input, vec![], &push_down_not);

    assert_eq!(
        actual.params,
        vec![Value::Integer(1), Value::from(true), Value::from(false)],
    );
    insta::assert_snapshot!(
        actual.pattern,
        @r#"SELECT * FROM (VALUES ($1::int)) as "unnamed_subquery" WHERE $2::bool and $3::bool"#,
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
            Value::Integer(1),
            Value::Integer(3),
            Value::Integer(2),
            Value::Integer(4),
        ],
    );

    // As we call check_transformation twice, to_sql is called twice,
    // so parameters are parameterized twice.
    insta::assert_snapshot!(
        actual_after_dnf.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE "t"."a" = $1::int::int and "t"."a" = $2::int::int or "t"."b" = $3::int::int and "t"."a" = $2::int::int or "t"."c" = $4::int::int"#
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
            Value::from(1),
            Value::from(2),
            Value::from(3),
            Value::from(4),
        ]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ("t"."a" <> $1::int and "t"."b" <> $2::int or "t"."a" <> $3::int) and "t"."c" <> $4::int"#
    );
}

#[test]
fn not_subquery() {
    // The row wrapping the subquery reference has to survive the push down: the SQL backend
    // reaches a subquery through its row.
    let input = r#"SELECT * FROM (values (1)) where not (select true from (values (1)))"#;
    let actual = check_transformation(input, vec![], &push_down_not);

    insta::assert_snapshot!(actual.pattern, @r#"SELECT * FROM (VALUES ($1::int)) as "unnamed_subquery" WHERE not (SELECT $2::bool as "col_1" FROM (VALUES ($3::int)) as "unnamed_subquery_1")"#);
}
