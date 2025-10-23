use crate::executor::engine::mock::RouterConfigurationMock;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::transformation::helpers::check_transformation;
use crate::ir::value::Value;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

fn split_columns(plan: Plan) -> Plan {
    plan.split_columns().unwrap()
}

#[test]
fn split_columns1() {
    let input = r#"SELECT "a" FROM "t" WHERE ("a", 2) = (1, "b")"#;
    let actual_pattern_params = check_transformation(input, vec![], &split_columns);

    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(1), Value::from(2)]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ("t"."a" = CAST($1 AS int)) and (CAST($2 AS int) = "t"."b")"#
    );
}

#[test]
fn split_columns2() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" = 1"#;
    let actual_pattern_params = check_transformation(input, vec![], &split_columns);

    assert_eq!(actual_pattern_params.params, vec![Value::from(1)]);
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE "t"."a" = CAST($1 AS int)"#
    );
}

#[test]
fn split_columns3() {
    let query = r#"SELECT "a" FROM "t" WHERE ("a", 2, "b") = (1, "b")"#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(query, &[], metadata).unwrap_err();
    assert_eq!(
        "unequal number of entries in row expression: 3 and 2",
        err.to_string()
    );
}

#[test]
fn split_columns4() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" in (1, 2)"#;
    let actual_pattern_params = check_transformation(input, vec![], &split_columns);

    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(1), Value::from(2)]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE "t"."a" in (CAST($1 AS int), CAST($2 AS int))"#
    );
}

#[test]
fn split_columns5() {
    let input = r#"SELECT "a" FROM "t" WHERE ("a", 2) < (1, "b") and "a" > 2"#;

    let actual_pattern_params = check_transformation(input, vec![], &split_columns);
    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(1), Value::from(2), Value::from(2)]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE (("t"."a" < CAST($1 AS int)) and (CAST($2 AS int) < "t"."b")) and ("t"."a" > CAST($3 AS int))"#
    );
}

#[test]
fn split_columns6() {
    let input = r#"SELECT "a" FROM "t" WHERE (SELECT a, b FROM t) = (1, 2)"#;

    let actual_pattern_params = check_transformation(input, vec![], &split_columns);
    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(1), Value::from(2)]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE (SELECT "t"."a", "t"."b" FROM "t") = (CAST($1 AS int), CAST($2 AS int))"#
    );

    let input = r#"SELECT "a" FROM "t" WHERE (1, 2) = (SELECT a, b FROM t)"#;

    let actual_pattern_params = check_transformation(input, vec![], &split_columns);
    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(1), Value::from(2)]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE (CAST($1 AS int), CAST($2 AS int)) = (SELECT "t"."a", "t"."b" FROM "t")"#
    );
}
