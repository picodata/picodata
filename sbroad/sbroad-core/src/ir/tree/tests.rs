use crate::ir::node::expression::Expression;
use crate::ir::node::{Alias, ArenaType};
use crate::ir::operator::Bool;
use crate::ir::relation::{SpaceEngine, Table, Type};
use crate::ir::tests::column_user_non_null;
use crate::ir::tree::traversal::{BreadthFirst, LevelNode, PostOrder, EXPR_CAPACITY, REL_CAPACITY};
use crate::ir::value::Value;
use crate::ir::Plan;
use pretty_assertions::assert_eq;
use smol_str::SmolStr;

#[test]
fn expression_bft() {
    // ((c1 = c2) and (c2 = c3)) or (c4 = c5)

    let mut plan = Plan::default();
    let c1 = plan.nodes.add_const(Value::from(1_u64));
    let c2 = plan.nodes.add_const(Value::from(1_u64));
    let c3 = plan.nodes.add_const(Value::from(1_u64));
    let c4 = plan.nodes.add_const(Value::from(2_u64));
    let c5 = plan.nodes.add_const(Value::from(3_u64));

    let c1_eq_c2 = plan.nodes.add_bool(c1, Bool::Eq, c2).unwrap();
    let c2_eq_c3 = plan.nodes.add_bool(c2, Bool::Eq, c3).unwrap();
    let c1c2_and_c2c3 = plan.nodes.add_bool(c1_eq_c2, Bool::And, c2_eq_c3).unwrap();
    let c4_eq_c5 = plan.nodes.add_bool(c4, Bool::Eq, c5).unwrap();
    let top = plan
        .nodes
        .add_bool(c1c2_and_c2c3, Bool::Or, c4_eq_c5)
        .unwrap();

    let mut bft_tree = BreadthFirst::with_capacity(
        |node| plan.nodes.expr_iter(node, true),
        EXPR_CAPACITY,
        EXPR_CAPACITY,
    );
    let mut iter = bft_tree.iter(top);
    assert_eq!(iter.next(), Some(LevelNode(0, top)));
    assert_eq!(iter.next(), Some(LevelNode(1, c1c2_and_c2c3)));
    assert_eq!(iter.next(), Some(LevelNode(1, c4_eq_c5)));
    assert_eq!(iter.next(), Some(LevelNode(2, c1_eq_c2)));
    assert_eq!(iter.next(), Some(LevelNode(2, c2_eq_c3)));
    assert_eq!(iter.next(), Some(LevelNode(2, c4)));
    assert_eq!(iter.next(), Some(LevelNode(2, c5)));
    assert_eq!(iter.next(), Some(LevelNode(3, c1)));
    assert_eq!(iter.next(), Some(LevelNode(3, c2)));
    assert_eq!(iter.next(), Some(LevelNode(3, c2)));
    assert_eq!(iter.next(), Some(LevelNode(3, c3)));
    assert_eq!(iter.next(), None);
}

#[test]
fn relational_post() {
    // select * from t1 union all (select * from t2 where a = 1)
    // output: scan t1, scan t2, selection, union all

    // Initialize plan
    let mut plan = Plan::default();

    let t1 = Table::new_sharded(
        "t1",
        vec![column_user_non_null(SmolStr::from("a"), Type::Boolean)],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();

    let t2 = Table::new_sharded(
        "t2",
        vec![column_user_non_null(SmolStr::from("a"), Type::Boolean)],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2", None).unwrap();

    let a = plan.add_row_from_child(scan_t2_id, &["a"]).unwrap();
    let const1 = plan.add_const(Value::from(1_i64));
    let eq = plan.nodes.add_bool(a, Bool::Eq, const1).unwrap();
    let selection_id = plan.add_select(&[scan_t2_id], eq).unwrap();

    let union_id = plan.add_union(scan_t1_id, selection_id, false).unwrap();
    plan.set_top(union_id).unwrap();
    let top = plan.get_top().unwrap();

    // Traverse the tree
    let mut dft_post = PostOrder::with_capacity(|node| plan.nodes.rel_iter(node), REL_CAPACITY);
    let mut iter = dft_post.iter(top);
    assert_eq!(iter.next(), Some(LevelNode(1, scan_t1_id)));
    assert_eq!(iter.next(), Some(LevelNode(2, scan_t2_id)));
    assert_eq!(iter.next(), Some(LevelNode(1, selection_id)));
    assert_eq!(iter.next(), Some(LevelNode(0, union_id)));
    assert_eq!(iter.next(), None);
}

#[test]
fn selection_subquery_dfs_post() {
    // select * from t1 where a in (select c from t2 where b = 1)
    //
    // ir tree:
    // selection
    // - scan t1
    // - subquery
    //     - projection: (c)
    //         - selection
    //             - scan t2
    //             - filter
    //                 - eq
    //                     - (b)
    //                     - (1)
    // - filter
    //     - in
    //         - (a)
    //         - (c)

    // Initialize plan
    let mut plan = Plan::default();

    let t1 = Table::new_sharded(
        "t1",
        vec![column_user_non_null(SmolStr::from("a"), Type::Boolean)],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();
    let a = plan.add_row_from_child(scan_t1_id, &["a"]).unwrap();

    let t2 = Table::new_sharded(
        "t2",
        vec![
            column_user_non_null(SmolStr::from("b"), Type::Boolean),
            column_user_non_null(SmolStr::from("c"), Type::Boolean),
        ],
        &["b"],
        &["b"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2", None).unwrap();

    let b = plan.add_row_from_child(scan_t2_id, &["b"]).unwrap();
    let const1 = plan.add_const(Value::from(1_u64));
    let eq_op = plan.nodes.add_bool(b, Bool::Eq, const1).unwrap();
    let selection_t2_id = plan.add_select(&[scan_t2_id], eq_op).unwrap();
    let proj_id = plan
        .add_proj(selection_t2_id, &["c"], false, false)
        .unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    let c = plan.get_row_from_rel_node(sq_id).unwrap();

    let in_op = plan.nodes.add_bool(a, Bool::In, c).unwrap();
    let selection_t1_id = plan.add_select(&[scan_t1_id, sq_id], in_op).unwrap();

    plan.set_top(selection_t1_id).unwrap();
    let top = plan.get_top().unwrap();

    // Traverse relational nodes in the plan tree
    let mut dft_post = PostOrder::with_capacity(|node| plan.nodes.rel_iter(node), REL_CAPACITY);
    let mut iter = dft_post.iter(top);
    assert_eq!(iter.next(), Some(LevelNode(1, scan_t1_id)));
    assert_eq!(iter.next(), Some(LevelNode(4, scan_t2_id)));
    assert_eq!(iter.next(), Some(LevelNode(3, selection_t2_id)));
    assert_eq!(iter.next(), Some(LevelNode(2, proj_id)));
    assert_eq!(iter.next(), Some(LevelNode(1, sq_id)));
    assert_eq!(iter.next(), Some(LevelNode(0, selection_t1_id)));
    assert_eq!(iter.next(), None);

    // Traverse expression nodes in the selection t2 filter
    let mut dft_post =
        PostOrder::with_capacity(|node| plan.nodes.expr_iter(node, true), EXPR_CAPACITY);
    let mut iter = dft_post.iter(eq_op);
    assert_eq!(iter.next(), Some(LevelNode(1, b)));
    assert_eq!(iter.next(), Some(LevelNode(1, const1)));
    assert_eq!(iter.next(), Some(LevelNode(0, eq_op)));
    assert_eq!(iter.next(), None);

    // Traverse expression nodes in the selection t1 filter
    let mut dft_post =
        PostOrder::with_capacity(|node| plan.nodes.expr_iter(node, true), EXPR_CAPACITY);
    let mut iter = dft_post.iter(in_op);
    assert_eq!(iter.next(), Some(LevelNode(1, a)));
    assert_eq!(iter.next(), Some(LevelNode(1, c)));
    assert_eq!(iter.next(), Some(LevelNode(0, in_op)));
    assert_eq!(iter.next(), None);
}

#[test]
fn subtree_dfs_post() {
    // select c from t1 where a = 1
    //
    // ir tree:
    // - projection
    //    - (c)
    //    - selection
    //      - scan t1
    //      - filter
    //          - eq
    //              - (a)
    //              - (1)

    // Initialize plan
    let mut plan = Plan::default();

    let t1 = Table::new_sharded(
        "t1",
        vec![
            column_user_non_null(SmolStr::from("a"), Type::Boolean),
            column_user_non_null(SmolStr::from("c"), Type::Boolean),
        ],
        &["a", "c"],
        &["a", "c"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();
    let a_ref = plan.nodes.next_id(ArenaType::Arena96);
    let a = plan.add_row_from_child(scan_t1_id, &["a"]).unwrap();
    let const1 = plan.add_const(Value::from(1_i64));
    let eq_op = plan.nodes.add_bool(a, Bool::Eq, const1).unwrap();
    let selection_t1_id = plan.add_select(&[scan_t1_id], eq_op).unwrap();
    let proj_id = plan
        .add_proj(selection_t1_id, &["c"], false, false)
        .unwrap();

    plan.set_top(proj_id).unwrap();
    let top = plan.get_top().unwrap();

    let proj_row_id = plan.get_relation_node(proj_id).unwrap().output();
    let row_children = plan
        .get_expression_node(proj_row_id)
        .unwrap()
        .clone_row_list()
        .unwrap();
    let alias_id = row_children.first().unwrap();
    let Expression::Alias(Alias {
        child: c_ref_id, ..
    }) = plan.get_expression_node(*alias_id).unwrap()
    else {
        panic!("invalid child in the row");
    };

    // Traverse relational nodes in the plan tree
    let mut dft_post =
        PostOrder::with_capacity(|node| plan.subtree_iter(node, false), plan.nodes.len());
    let mut iter = dft_post.iter(top);
    assert_eq!(iter.next(), Some(LevelNode(3, *c_ref_id)));
    assert_eq!(iter.next(), Some(LevelNode(2, *alias_id)));
    assert_eq!(iter.next(), Some(LevelNode(1, proj_row_id)));
    assert_eq!(iter.next(), Some(LevelNode(2, scan_t1_id)));
    assert_eq!(iter.next(), Some(LevelNode(4, a_ref)));
    assert_eq!(iter.next(), Some(LevelNode(3, a)));
    assert_eq!(iter.next(), Some(LevelNode(3, const1)));
    assert_eq!(iter.next(), Some(LevelNode(2, eq_op)));
    assert_eq!(iter.next(), Some(LevelNode(1, selection_t1_id)));
    assert_eq!(iter.next(), Some(LevelNode(0, proj_id)));
    assert_eq!(iter.next(), None);
}
