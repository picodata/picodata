use pretty_assertions::assert_eq;

use crate::collection;
use crate::errors::{Entity, SbroadError};
use crate::ir::distribution::{Distribution, Key};
use crate::ir::expression::ColumnWithScan;
use crate::ir::relation::{Column, ColumnRole, SpaceEngine, Table, Type};
use crate::ir::tests::{column_user_non_null, sharding_column};
use crate::ir::value::Value;
use crate::ir::Plan;

use super::*;

#[test]
fn scan_rel() {
    let mut plan = Plan::default();

    let t = Table::new_sharded(
        "t",
        vec![
            column_user_non_null(SmolStr::from("a"), Type::Boolean),
            column_user_non_null(SmolStr::from("b"), Type::Unsigned),
            column_user_non_null(SmolStr::from("c"), Type::String),
            column_user_non_null(SmolStr::from("d"), Type::String),
        ],
        &["b", "a"],
        &["b", "a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t);

    let scan_output = NodeId {
        offset: 0,
        arena_type: ArenaType::Arena64,
    };
    let scan_node = NodeId {
        offset: 1,
        arena_type: ArenaType::Arena64,
    };

    let scan_id = plan.add_scan("t", None).unwrap();
    assert_eq!(scan_node, scan_id);
    plan.top = Some(scan_node);

    plan.set_distribution(scan_output).unwrap();
    let row = plan.get_node(scan_output).unwrap();
    if let Node::Expression(expr) = row {
        let keys: HashSet<_, RepeatableState> = collection! { Key::new(vec![1, 0]) };
        assert_eq!(
            expr.distribution().unwrap(),
            &Distribution::Segment { keys: keys.into() }
        );
    } else {
        panic!("Wrong output node type!");
    }
}

#[test]
fn projection() {
    let mut plan = Plan::default();

    let t = Table::new_sharded(
        "t",
        vec![
            column_user_non_null(SmolStr::from("a"), Type::Boolean),
            column_user_non_null(SmolStr::from("b"), Type::Unsigned),
            column_user_non_null(SmolStr::from("c"), Type::String),
            column_user_non_null(SmolStr::from("d"), Type::String),
        ],
        &["b", "a"],
        &["b", "a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t", None).unwrap();

    // Invalid alias names in the output
    assert_eq!(
        SbroadError::NotFound(Entity::Column, r#"with name "e""#.into()),
        plan.add_proj(scan_id, &["a", "e"], false, false)
            .unwrap_err()
    );

    let mut test_node = NodeId {
        offset: 0,
        arena_type: ArenaType::Arena32,
    };

    // Expression node instead of relational one
    assert_eq!(
        SbroadError::Invalid(
            Entity::Node,
            Some(
                "node is not Relational type: Expression(Alias(Alias { name: \"a\", child: NodeId { offset: 0, arena_type: Arena96 } }))".into()
            )
        ),
        plan.add_proj(test_node, &["a"], false, false).unwrap_err()
    );

    test_node.offset = 42;

    // Try to build projection from the non-existing node
    assert_eq!(
        SbroadError::NotFound(Entity::Node, "from Arena32 with index 42".to_smolstr()),
        plan.add_proj(test_node, &["a"], false, false).unwrap_err()
    );
}

#[test]
fn selection() {
    // select * from t where (a, b) = (1, 10)

    let mut plan = Plan::default();

    let t = Table::new_sharded(
        "t",
        vec![
            column_user_non_null(SmolStr::from("a"), Type::Boolean),
            column_user_non_null(SmolStr::from("b"), Type::Unsigned),
            column_user_non_null(SmolStr::from("c"), Type::String),
            column_user_non_null(SmolStr::from("d"), Type::String),
        ],
        &["b", "a"],
        &["b", "a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t", None).unwrap();

    let ref_row = plan.add_row_from_child(scan_id, &["a", "b"]).unwrap();
    let const_1 = plan.nodes.add_const(Value::from(1_u64));
    let const_10 = plan.nodes.add_const(Value::from(10_u64));
    let const_row = plan.nodes.add_row(vec![const_1, const_10], None);
    let gt_id = plan.nodes.add_bool(ref_row, Bool::Gt, const_row).unwrap();

    // Correct Selection operator
    plan.add_select(&[scan_id], gt_id).unwrap();

    // Non-trivalent filter
    assert_eq!(
        SbroadError::Invalid(
            Entity::Expression,
            Some("filter expression is not a trivalent expression.".into())
        ),
        plan.add_select(&[scan_id], const_row).unwrap_err()
    );
}

#[test]
fn except() {
    let mut valid_plan = Plan::default();

    let t1 = Table::new_sharded(
        "t1",
        vec![column_user_non_null(SmolStr::from("a"), Type::Unsigned)],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    let t1_copy = t1.clone();
    valid_plan.add_rel(t1);
    let scan_t1_id = valid_plan.add_scan("t1", None).unwrap();

    let t2 = Table::new_sharded(
        "t2",
        vec![column_user_non_null(SmolStr::from("a"), Type::Unsigned)],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    valid_plan.add_rel(t2);
    let scan_t2_id = valid_plan.add_scan("t2", None).unwrap();

    // Correct Except operator
    valid_plan.add_except(scan_t1_id, scan_t2_id).unwrap();

    let mut invalid_plan = Plan::default();

    invalid_plan.add_rel(t1_copy);
    let scan_t1_id = invalid_plan.add_scan("t1", None).unwrap();

    let t3 = Table::new_sharded(
        "t3",
        vec![
            column_user_non_null(SmolStr::from("a"), Type::Unsigned),
            column_user_non_null(SmolStr::from("b"), Type::Unsigned),
        ],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    invalid_plan.add_rel(t3);
    let scan_t3_id = invalid_plan.add_scan("t3", None).unwrap();

    assert_eq!(
        SbroadError::UnexpectedNumberOfValues(
            "children tuples have mismatching amount of columns in except node: left 1, right 2"
                .into()
        ),
        invalid_plan.add_except(scan_t1_id, scan_t3_id).unwrap_err()
    );
}

#[test]
fn insert() {
    let mut plan = Plan::default();

    let t1 = Table::new_sharded(
        "t1",
        vec![column_user_non_null(SmolStr::from("a"), Type::Unsigned)],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();

    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();

    let t2 = Table::new_sharded(
        "t2",
        vec![
            column_user_non_null(SmolStr::from("a"), Type::Unsigned),
            column_user_non_null(SmolStr::from("b"), Type::Unsigned),
            Column::new("c", Type::Unsigned, ColumnRole::Sharding, true),
        ],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t2);

    assert_eq!(
        SbroadError::NotFound(Entity::Table, "t4 among plan relations".into()),
        plan.add_insert("t4", scan_t1_id, &["a".into()], ConflictStrategy::default())
            .unwrap_err()
    );

    assert_eq!(
        SbroadError::FailedTo(
            Action::Insert,
            Some(Entity::Column),
            "system column \"c\" cannot be inserted".into(),
        ),
        plan.add_insert(
            "t2",
            scan_t1_id,
            &["a".into(), "b".into(), "c".into()],
            ConflictStrategy::default()
        )
        .unwrap_err()
    );

    assert_eq!(
        SbroadError::UnexpectedNumberOfValues(
            "invalid number of values: 1. Table t2 expects 2 column(s).".into()
        ),
        plan.add_insert(
            "t2",
            scan_t1_id,
            &["a".into(), "b".into()],
            ConflictStrategy::default()
        )
        .unwrap_err()
    );

    plan.add_insert("t1", scan_t1_id, &["a".into()], ConflictStrategy::default())
        .unwrap();
}

#[test]
fn union_all() {
    let mut plan = Plan::default();

    let t1 = Table::new_sharded(
        "t1",
        vec![column_user_non_null(SmolStr::from("a"), Type::Unsigned)],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();

    let t2 = Table::new_sharded(
        "t2",
        vec![column_user_non_null(SmolStr::from("a"), Type::Unsigned)],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2", None).unwrap();

    plan.add_union(scan_t2_id, scan_t1_id, false).unwrap();
}

#[test]
fn union_all_col_amount_mismatch() {
    let mut plan = Plan::default();

    let t1 = Table::new_sharded(
        "t1",
        vec![
            column_user_non_null(SmolStr::from("a"), Type::Boolean),
            column_user_non_null(SmolStr::from("b"), Type::Unsigned),
        ],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t1);

    let scan_t1_id = plan.add_scan("t1", None).unwrap();

    // Check errors for children with different amount of column
    let t2 = Table::new_sharded(
        "t2",
        vec![column_user_non_null(SmolStr::from("b"), Type::Unsigned)],
        &["b"],
        &["b"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t2);

    let scan_t2_id = plan.add_scan("t2", None).unwrap();
    assert_eq!(
        SbroadError::UnexpectedNumberOfValues(
            "children tuples have mismatching amount of columns in union all node: left 1, right 2"
                .into()
        ),
        plan.add_union(scan_t2_id, scan_t1_id, false).unwrap_err()
    );
}

#[test]
fn sub_query() {
    let mut plan = Plan::default();

    let t = Table::new_sharded(
        "t",
        vec![
            column_user_non_null(SmolStr::from("a"), Type::Boolean),
            column_user_non_null(SmolStr::from("b"), Type::Unsigned),
        ],
        &["a"],
        &["b"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t", None).unwrap();
    plan.add_sub_query(scan_id, Some("sq")).unwrap();

    // Non-relational child node
    let a = NodeId {
        offset: 0,
        arena_type: ArenaType::Arena32,
    };
    assert_eq!(
        SbroadError::Invalid(
            Entity::Node,
            Some("node is not Relational type: Expression(Alias(Alias { name: \"a\", child: NodeId { offset: 0, arena_type: Arena96 } }))".into())
        ),
        plan.add_sub_query(a, Some("sq")).unwrap_err()
    );
}

#[test]
fn join() {
    // t1(a, b), t2(c, d)
    // select * from t1 join t2 on a = d;
    //
    // Treat a = d as (a) = (d),
    // i.e. (a), (d) - tuples containing a single column.
    let mut plan = Plan::default();

    let t1 = Table::new_sharded(
        "t1",
        vec![
            column_user_non_null(SmolStr::from("a"), Type::Boolean),
            column_user_non_null(SmolStr::from("b"), Type::Unsigned),
            sharding_column(),
        ],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1 = plan.add_scan("t1", None).unwrap();

    let t2 = Table::new_sharded(
        "t2",
        vec![
            column_user_non_null(SmolStr::from("c"), Type::Boolean),
            column_user_non_null(SmolStr::from("d"), Type::Unsigned),
            sharding_column(),
        ],
        &["d"],
        &["d"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2 = plan.add_scan("t2", None).unwrap();

    let a_row = plan
        .add_row_from_left_branch(scan_t1, scan_t2, &[ColumnWithScan::new("a", None)])
        .unwrap();
    let d_row = plan
        .add_row_from_right_branch(scan_t1, scan_t2, &[ColumnWithScan::new("d", None)])
        .unwrap();
    let condition = plan.nodes.add_bool(a_row, Bool::Eq, d_row).unwrap();
    let join = plan
        .add_join(scan_t1, scan_t2, condition, JoinKind::Inner)
        .unwrap();
    plan.top = Some(join);
}

#[test]
fn join_duplicate_columns() {
    // t1(a, b), t2(a, d)
    // select * from t1 join t2 on t1.a = t2.d
    let mut plan = Plan::default();

    let t1 = Table::new_sharded(
        "t1",
        vec![
            column_user_non_null(SmolStr::from("a"), Type::Boolean),
            column_user_non_null(SmolStr::from("b"), Type::Unsigned),
            sharding_column(),
        ],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1 = plan.add_scan("t1", None).unwrap();

    let t2 = Table::new_sharded(
        "t2",
        vec![
            column_user_non_null(SmolStr::from("a"), Type::Boolean),
            column_user_non_null(SmolStr::from("d"), Type::Unsigned),
            sharding_column(),
        ],
        &["d"],
        &["d"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2 = plan.add_scan("t2", None).unwrap();

    let a_row = plan
        .add_row_from_left_branch(scan_t1, scan_t2, &[ColumnWithScan::new("a", None)])
        .unwrap();
    let d_row = plan
        .add_row_from_right_branch(scan_t1, scan_t2, &[ColumnWithScan::new("d", None)])
        .unwrap();
    let condition = plan.nodes.add_bool(a_row, Bool::Eq, d_row).unwrap();
    let join = plan
        .add_join(scan_t1, scan_t2, condition, JoinKind::Inner)
        .unwrap();
    plan.top = Some(join);
}
