use super::constant_pair_rejects_eq;
use crate::ir::node::NodeId;
use crate::ir::types::DerivedType;
use crate::ir::value::Value;
use crate::ir::Plan;

#[test]
fn same_kind_constants() {
    assert!(!constant_pair_rejects_eq(&Value::from(1), &Value::from(1)));
    assert!(constant_pair_rejects_eq(&Value::from(1), &Value::from(2)));
    assert!(!constant_pair_rejects_eq(
        &Value::from("a"),
        &Value::from("a")
    ));
    assert!(constant_pair_rejects_eq(
        &Value::from("a"),
        &Value::from("b")
    ));
}

#[test]
fn null_constants() {
    assert!(constant_pair_rejects_eq(&Value::Null, &Value::from(1)));
    assert!(constant_pair_rejects_eq(&Value::from(1), &Value::Null));
    assert!(constant_pair_rejects_eq(&Value::Null, &Value::Null));
}

/// `Value::eq` disagrees with itself here: integer-to-double drops the low bit,
/// while double-to-integer compares exactly. Neither orientation may reject.
#[test]
fn mixed_numeric_constants_are_undecided() {
    let int = Value::from(9_007_199_254_740_993_i64);
    let double = Value::from(9_007_199_254_740_992.0);
    assert!(!constant_pair_rejects_eq(&int, &double));
    assert!(!constant_pair_rejects_eq(&double, &int));
    assert!(!constant_pair_rejects_eq(
        &Value::from(1),
        &Value::from(2.5)
    ));
}

/// `Value::eq` never reports tuples equal, so it proves nothing about them.
#[test]
fn tuple_constants_are_undecided() {
    let a = Value::Tuple(vec![Value::from(1)]);
    let b = Value::Tuple(vec![Value::from(2)]);
    assert!(!constant_pair_rejects_eq(&a, &a.clone()));
    assert!(!constant_pair_rejects_eq(&a, &b));
}

/// Whether constants make `left = right` provably not `TRUE`.
fn rejects(plan: &Plan, left: NodeId, right: NodeId) -> bool {
    let left = plan.get_expression_node(left).unwrap();
    let right = plan.get_expression_node(right).unwrap();
    plan.constants_reject_eq(&left, &right).unwrap()
}

/// Build `ROW(left) = ROW(right)` operands; `None` stands for a parameter.
fn rows(plan: &mut Plan, pairs: &[(Option<i64>, Option<i64>)]) -> (NodeId, NodeId) {
    let mut operand = |value: Option<i64>| match value {
        Some(v) => plan.nodes.add_const(Value::from(v)),
        None => plan.add_param(1, DerivedType::unknown()),
    };
    let (left, right): (Vec<_>, Vec<_>) =
        pairs.iter().map(|&(l, r)| (operand(l), operand(r))).unzip();
    (plan.nodes.add_row(left), plan.nodes.add_row(right))
}

#[test]
fn row_contradiction_after_equal_pair() {
    let mut plan = Plan::default();
    let (left, right) = rows(
        &mut plan,
        &[(Some(1), Some(1)), (Some(2), Some(1)), (None, None)],
    );
    assert!(rejects(&plan, left, right));
}

#[test]
fn row_contradiction_at_last_position() {
    let mut plan = Plan::default();
    let (left, right) = rows(
        &mut plan,
        &[(None, Some(1)), (Some(1), Some(1)), (Some(3), Some(4))],
    );
    assert!(rejects(&plan, left, right));
}

#[test]
fn row_without_contradiction() {
    let mut plan = Plan::default();
    let (left, right) = rows(
        &mut plan,
        &[(None, Some(1)), (Some(1), Some(1)), (Some(2), None)],
    );
    assert!(!rejects(&plan, left, right));
}

#[test]
fn scalar_constants() {
    let mut plan = Plan::default();
    let one = plan.nodes.add_const(Value::from(1));
    let two = plan.nodes.add_const(Value::from(2));
    assert!(rejects(&plan, one, two));
    assert!(!rejects(&plan, one, one));
}

/// `NULL` rejects equality against anything, including a non-constant operand.
#[test]
fn null_against_non_constant() {
    let mut plan = Plan::default();
    let null = plan.nodes.add_const(Value::Null);
    let param = plan.add_param(1, DerivedType::unknown());
    assert!(rejects(&plan, null, param));
    assert!(rejects(&plan, param, null));

    let one = plan.nodes.add_const(Value::from(1));
    let param2 = plan.add_param(2, DerivedType::unknown());
    let null2 = plan.nodes.add_const(Value::Null);
    let left = plan.nodes.add_row(vec![param2, one]);
    let one2 = plan.nodes.add_const(Value::from(1));
    let right = plan.nodes.add_row(vec![null2, one2]);
    assert!(rejects(&plan, left, right));
}
