use crate::ir::node::NodeId;
use crate::ir::operator::Bool;
use crate::ir::transformation::helpers::check_transformation;
use crate::ir::value::Value;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

pub fn set_dnf(plan: Plan) -> Plan {
    plan.set_dnf().unwrap()
}

/// Build a single two-way OR group `(true OR true)`.
fn or_group(plan: &mut Plan) -> NodeId {
    let left = plan.add_const(Value::Boolean(true));
    let right = plan.add_const(Value::Boolean(true));
    plan.add_bool(left, Bool::Or, right).unwrap()
}

/// Build `(true OR true) AND (true OR true) AND ...` with `groups`.
fn and_of_or_groups(plan: &mut Plan, groups: usize) -> NodeId {
    let mut top = or_group(plan);
    for _ in 1..groups {
        let next = or_group(plan);
        top = plan.add_bool(top, Bool::And, next).unwrap();
    }
    top
}

#[test]
fn dnf_chains_at_limit() {
    let mut plan = Plan::default();
    let top = and_of_or_groups(&mut plan, 9);
    let chains = plan.get_dnf_chains(top).unwrap();
    assert_eq!(chains.map(|c| c.len()), Some(512));
}

#[test]
fn dnf_chains_over_limit() {
    let mut plan = Plan::default();
    let top = and_of_or_groups(&mut plan, 10);
    assert!(plan.get_dnf_chains(top).unwrap().is_none());
}

#[test]
fn dnf_chains_much_over_limit() {
    let mut plan = Plan::default();
    let top = and_of_or_groups(&mut plan, 30);
    assert!(plan.get_dnf_chains(top).unwrap().is_none());
}

#[test]
fn dnf1() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 AND "b" = 2 OR "a" = 3) AND "c" = 4"#;
    let actual_pattern_params = check_transformation(input, vec![], &set_dnf);

    assert_eq!(
        actual_pattern_params.params,
        vec![
            Value::from(1),
            Value::from(2),
            Value::from(4),
            Value::from(3),
        ]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE "t"."a" = CAST($1 AS int) and "t"."b" = CAST($2 AS int) and "t"."c" = CAST($3 AS int) or "t"."a" = CAST($4 AS int) and "t"."c" = CAST($3 AS int)"#
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
            Value::from(3),
            Value::from(1),
            Value::from(4),
            Value::from(2),
        ]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE "t"."a" = CAST($1 AS int) and "t"."a" = CAST($2 AS int) or "t"."c" = CAST($3 AS int) and "t"."a" = CAST($2 AS int) or "t"."a" = CAST($1 AS int) and "t"."b" = CAST($4 AS int) or "t"."c" = CAST($3 AS int) and "t"."b" = CAST($4 AS int)"#
    );
}

#[test]
fn dnf3() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 OR "b" = 2) AND NULL"#;
    let actual_pattern_params = check_transformation(input, vec![], &set_dnf);

    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(1), Value::Null, Value::from(2),]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE "t"."a" = CAST($1 AS int) and $2 or "t"."b" = CAST($3 AS int) and $2"#
    );
}

#[test]
fn dnf4() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 OR "b" = 2) AND true"#;
    let actual_pattern_params = check_transformation(input, vec![], &set_dnf);

    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(1), Value::Boolean(true), Value::from(2),]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE "t"."a" = CAST($1 AS int) and CAST($2 AS bool) or "t"."b" = CAST($3 AS int) and CAST($2 AS bool)"#
    );
}

#[test]
fn dnf5() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 OR "b" = 2) AND ((false))"#;
    let actual_pattern_params = check_transformation(input, vec![], &set_dnf);

    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(1), Value::Boolean(false), Value::from(2),]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE "t"."a" = CAST($1 AS int) and CAST($2 AS bool) or "t"."b" = CAST($3 AS int) and CAST($2 AS bool)"#
    );
}

#[test]
fn dnf6() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 and "c" = 1 OR "b" = 2"#;
    let actual_pattern_params = check_transformation(input, vec![], &set_dnf);

    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(1), Value::from(1), Value::from(2)]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE "t"."a" = CAST($1 AS int) and "t"."c" = CAST($2 AS int) or "t"."b" = CAST($3 AS int)"#
    );
}
