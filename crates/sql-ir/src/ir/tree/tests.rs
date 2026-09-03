use crate::ir::node::expression::Expression;
use crate::ir::node::{Alias, ArenaType, NodeId};
use crate::ir::operator::Bool;
use crate::ir::relation::{SpaceEngine, Table};
use crate::ir::test_fixtures::column_user_non_null;
use crate::ir::tree::traversal::{BreadthFirst, EveryPath, NodeIter, PostOrder, EXPR_CAPACITY};
use crate::ir::types::UnrestrictedType;
use crate::ir::value::Value;
use crate::ir::Node;
use crate::ir::Plan;
use pretty_assertions::assert_eq;
use rand::random;
use smol_str::SmolStr;
use std::collections::HashSet;

#[test]
fn expression_bft() {
    // ((c1 = c2) and (c2 = c3)) or (c4 = c5)

    let mut plan = Plan::default();
    let c1 = plan.nodes.add_const(Value::from(1));
    let c2 = plan.nodes.add_const(Value::from(1));
    let c3 = plan.nodes.add_const(Value::from(1));
    let c4 = plan.nodes.add_const(Value::from(2));
    let c5 = plan.nodes.add_const(Value::from(3));

    let c1_eq_c2 = plan.nodes.add_bool(c1, Bool::Eq, c2).unwrap();
    let c2_eq_c3 = plan.nodes.add_bool(c2, Bool::Eq, c3).unwrap();
    let c1c2_and_c2c3 = plan.nodes.add_bool(c1_eq_c2, Bool::And, c2_eq_c3).unwrap();
    let c4_eq_c5 = plan.nodes.add_bool(c4, Bool::Eq, c5).unwrap();
    let top = plan
        .nodes
        .add_bool(c1c2_and_c2c3, Bool::Or, c4_eq_c5)
        .unwrap();

    let bft_tree = BreadthFirst::new(
        |node| plan.nodes.expr_iter(node, true),
        EXPR_CAPACITY,
        EXPR_CAPACITY,
    );
    let mut iter = bft_tree.traverse_into_iter(top);
    assert_eq!(iter.next(), Some(top));
    assert_eq!(iter.next(), Some(c1c2_and_c2c3));
    assert_eq!(iter.next(), Some(c4_eq_c5));
    assert_eq!(iter.next(), Some(c1_eq_c2));
    assert_eq!(iter.next(), Some(c2_eq_c3));
    assert_eq!(iter.next(), Some(c4));
    assert_eq!(iter.next(), Some(c5));
    assert_eq!(iter.next(), Some(c1));
    assert_eq!(iter.next(), Some(c2));
    assert_eq!(iter.next(), Some(c2));
    assert_eq!(iter.next(), Some(c3));
    assert_eq!(iter.next(), None);
}

#[test]
fn relational_post() {
    // select * from t1 union all (select * from t2 where a = 1)
    // output: scan t1, scan t2, selection, union all

    // Initialize plan
    let mut plan = Plan::default();

    let t1 = Table::new_sharded(
        random(),
        "t1",
        vec![column_user_non_null(
            SmolStr::from("a"),
            UnrestrictedType::Boolean,
        )],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1".into(), None, None).unwrap();

    let t2 = Table::new_sharded(
        random(),
        "t2",
        vec![column_user_non_null(
            SmolStr::from("a"),
            UnrestrictedType::Boolean,
        )],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2".into(), None, None).unwrap();

    let a = plan.add_row_from_child(scan_t2_id, &["a"]).unwrap();
    let const1 = plan.add_const(Value::from(1_i64));
    let eq = plan.nodes.add_bool(a, Bool::Eq, const1).unwrap();
    let selection_id = plan.add_select(scan_t2_id, eq).unwrap();

    let union_id = plan.add_union(scan_t1_id, selection_id, false).unwrap();
    plan.set_top(union_id).unwrap();
    let top = plan.get_top().unwrap();

    // Traverse the tree
    let mut iter =
        PostOrder::new(&plan.nodes, |node| plan.nodes.rel_iter(node), top).map(|(id, _)| id);
    assert_eq!(iter.next(), Some(scan_t1_id));
    assert_eq!(iter.next(), Some(scan_t2_id));
    assert_eq!(iter.next(), Some(selection_id));
    assert_eq!(iter.next(), Some(union_id));
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
        random(),
        "t1",
        vec![column_user_non_null(
            SmolStr::from("a"),
            UnrestrictedType::Boolean,
        )],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1".into(), None, None).unwrap();
    let a = plan.add_row_from_child(scan_t1_id, &["a"]).unwrap();

    let t2 = Table::new_sharded(
        random(),
        "t2",
        vec![
            column_user_non_null(SmolStr::from("b"), UnrestrictedType::Boolean),
            column_user_non_null(SmolStr::from("c"), UnrestrictedType::Boolean),
        ],
        &["b"],
        &["b"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2".into(), None, None).unwrap();

    let b = plan.add_row_from_child(scan_t2_id, &["b"]).unwrap();
    let const1 = plan.add_const(Value::from(1));
    let eq_op = plan.nodes.add_bool(b, Bool::Eq, const1).unwrap();
    let selection_t2_id = plan.add_select(scan_t2_id, eq_op).unwrap();
    let proj_id = plan
        .add_proj(selection_t2_id, vec![], &["c"], false, false)
        .unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    let c = plan.get_row_from_rel_node(sq_id).unwrap();

    let in_op = plan.nodes.add_bool(a, Bool::In, c).unwrap();
    let selection_t1_id = plan.add_select(scan_t1_id, in_op).unwrap();
    {
        let mut selection = plan.get_mut_relation_node(selection_t1_id).unwrap();
        selection.add_sq_child(sq_id);
    }

    plan.set_top(selection_t1_id).unwrap();
    let top = plan.get_top().unwrap();

    // Traverse relational nodes in the plan tree
    let mut iter =
        PostOrder::new(&plan.nodes, |node| plan.nodes.rel_iter(node), top).map(|(id, _)| id);
    assert_eq!(iter.next(), Some(scan_t1_id));
    assert_eq!(iter.next(), Some(scan_t2_id));
    assert_eq!(iter.next(), Some(selection_t2_id));
    assert_eq!(iter.next(), Some(proj_id));
    assert_eq!(iter.next(), Some(sq_id));
    assert_eq!(iter.next(), Some(selection_t1_id));
    assert_eq!(iter.next(), None);
    check_node_post_order(&plan, |node| plan.nodes.rel_iter(node), top);

    // Traverse expression nodes in the selection t2 filter
    let mut iter = PostOrder::new(&plan.nodes, |node| plan.nodes.expr_iter(node, true), eq_op)
        .map(|(id, _)| id);
    assert_eq!(iter.next(), Some(b));
    assert_eq!(iter.next(), Some(const1));
    assert_eq!(iter.next(), Some(eq_op));
    assert_eq!(iter.next(), None);

    // Traverse expression nodes in the selection t1 filter
    let mut iter = PostOrder::new(&plan.nodes, |node| plan.nodes.expr_iter(node, true), in_op)
        .map(|(id, _)| id);
    assert_eq!(iter.next(), Some(a));
    assert_eq!(iter.next(), Some(c));
    assert_eq!(iter.next(), Some(in_op));
    assert_eq!(iter.next(), None);
    check_node_post_order(&plan, |node| plan.nodes.expr_iter(node, true), in_op);
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
        random(),
        "t1",
        vec![
            column_user_non_null(SmolStr::from("a"), UnrestrictedType::Boolean),
            column_user_non_null(SmolStr::from("c"), UnrestrictedType::Boolean),
        ],
        &["a", "c"],
        &["a", "c"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1".into(), None, None).unwrap();
    let a_ref = plan.nodes.next_id(ArenaType::Arena96);
    let a = plan.add_row_from_child(scan_t1_id, &["a"]).unwrap();
    let const1 = plan.add_const(Value::from(1_i64));
    let eq_op = plan.nodes.add_bool(a, Bool::Eq, const1).unwrap();
    let selection_t1_id = plan.add_select(scan_t1_id, eq_op).unwrap();
    let proj_id = plan
        .add_proj(selection_t1_id, vec![], &["c"], false, false)
        .unwrap();

    plan.set_top(proj_id).unwrap();
    let top = plan.get_top().unwrap();

    let proj_row_id = plan.get_relational_output(proj_id).unwrap();
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
    let mut iter =
        PostOrder::new(&plan.nodes, |node| plan.subtree_iter(node), top).map(|(id, _)| id);
    assert_eq!(iter.next(), Some(*c_ref_id));
    assert_eq!(iter.next(), Some(*alias_id));
    assert_eq!(iter.next(), Some(proj_row_id));
    assert_eq!(iter.next(), Some(scan_t1_id));
    assert_eq!(iter.next(), Some(a_ref));
    assert_eq!(iter.next(), Some(a));
    assert_eq!(iter.next(), Some(const1));
    assert_eq!(iter.next(), Some(eq_op));
    assert_eq!(iter.next(), Some(selection_t1_id));
    assert_eq!(iter.next(), Some(proj_id));
    assert_eq!(iter.next(), None);
    check_node_post_order(&plan, |node| plan.subtree_iter(node), top);

    // Children from the node give the same traversal as children by id.
    let by_id: Vec<_> = PostOrder::new(&plan.nodes, |node| plan.subtree_iter(node), top)
        .map(|(id, _)| id)
        .collect();
    let by_node: Vec<_> =
        PostOrder::new_by_node(&plan.nodes, |_, node| plan.subtree_iter_of(node), top)
            .map(|(id, _)| id)
            .collect();
    assert_eq!(by_node, by_id);

    let missing = plan.nodes.next_id(ArenaType::Arena64);
    let mut iter = PostOrder::new(&plan.nodes, |node| plan.subtree_iter(node), missing);
    assert!(iter.next().is_none());
}

fn add_table_t(plan: &mut Plan) {
    let t = Table::new_sharded(
        random(),
        "t",
        vec![column_user_non_null(
            SmolStr::from("a"),
            UnrestrictedType::Boolean,
        )],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t);
}

/// A scan under `levels` of `c(i) = c(i-1) union all c(i-1)`. Each level has
/// a union and two references to one body, so the plan has `1 + 3 * levels`
/// relational nodes and `4 * 2^levels - 3` paths to them.
fn reused_cte_chain(plan: &mut Plan, levels: usize) -> NodeId {
    add_table_t(plan);
    let mut body = plan.add_scan("t".into(), None, None).unwrap();
    for _ in 0..levels {
        let left = plan.add_cte_scan(body, SmolStr::from("c")).unwrap();
        let right = plan.add_cte_scan(body, SmolStr::from("c")).unwrap();
        body = plan.add_union(left, right, false).unwrap();
    }
    body
}

#[test]
fn post_order_enters_shared_nodes_once() {
    let mut plan = Plan::default();
    // Every path would take 2^66 visits.
    let levels = 64;
    let top = reused_cte_chain(&mut plan, levels);

    let visited: Vec<NodeId> = PostOrder::new(&plan.nodes, |node| plan.nodes.rel_iter(node), top)
        .map(|(id, _)| id)
        .collect();
    assert_eq!(visited.len(), 1 + 3 * levels);
    assert_eq!(visited.last(), Some(&top));

    // Children go before their parents, however many parents they have.
    let mut seen = HashSet::new();
    for id in &visited {
        assert!(plan.nodes.rel_iter(*id).all(|child| seen.contains(&child)));
        assert!(seen.insert(*id));
    }

    // The traversal with expressions enters shared nodes once too.
    let subtree = PostOrder::new(&plan.nodes, |node| plan.subtree_iter(node), top);
    assert_eq!(subtree.relational().count(), 1 + 3 * levels);
}

#[test]
fn post_order_every_path_visits_each_occurrence() {
    let mut plan = Plan::default();
    let top = reused_cte_chain(&mut plan, 4);

    let visits = PostOrder::new(
        EveryPath(&plan.nodes),
        |node| plan.nodes.rel_iter(node),
        top,
    )
    .count();
    assert_eq!(visits, 4 * 2_usize.pow(4) - 3);

    let by_node =
        PostOrder::new_by_node(EveryPath(&plan.nodes), |id, _| plan.nodes.rel_iter(id), top)
            .count();
    assert_eq!(by_node, visits);
}

#[test]
fn post_order_of_a_tree_is_not_changed() {
    let mut plan = Plan::default();
    // Without a reused CTE and subqueries each node has one path.
    add_table_t(&mut plan);
    let left = plan.add_scan("t".into(), None, None).unwrap();
    let right = plan.add_scan("t".into(), None, None).unwrap();
    let top = plan.add_union(left, right, false).unwrap();

    let new: Vec<_> = PostOrder::new(&plan.nodes, |node| plan.subtree_iter(node), top)
        .map(|(id, _)| id)
        .collect();
    let every_path: Vec<_> =
        PostOrder::new(EveryPath(&plan.nodes), |node| plan.subtree_iter(node), top)
            .map(|(id, _)| id)
            .collect();
    assert_eq!(new, every_path);
}

/// A new kind of shared relational node must not slip past `shares_children`.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "`Arena::shares_children` misses an edge")]
fn post_order_checks_shares_children() {
    let mut plan = Plan::default();
    add_table_t(&mut plan);
    let scan = plan.add_scan("t".into(), None, None).unwrap();
    let top = plan.add_union(scan, scan, false).unwrap();

    let _ = PostOrder::new(&plan.nodes, |node| plan.nodes.rel_iter(node), top).count();
}

/// Check that `PostOrder` gives the node of each id, and that the typed
/// views select only their kind.
fn check_node_post_order<F, I>(plan: &Plan, children_fn: F, root: NodeId)
where
    F: FnMut(NodeId) -> I + Clone,
    I: Iterator<Item = NodeId>,
{
    let nodes: Vec<_> = PostOrder::new(&plan.nodes, children_fn.clone(), root).collect();
    for (id, node) in &nodes {
        assert_eq!(node, &plan.get_node(*id).unwrap());
    }

    let rel_ids: Vec<_> = PostOrder::new(&plan.nodes, children_fn.clone(), root)
        .relational()
        .map(|(id, _)| id)
        .collect();
    let expected_rel_ids: Vec<_> = nodes
        .iter()
        .filter(|(_, node)| matches!(node, Node::Relational(_)))
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(rel_ids, expected_rel_ids);

    let expr_ids: Vec<_> = PostOrder::new(&plan.nodes, children_fn, root)
        .expressions()
        .map(|(id, _)| id)
        .collect();
    let expected_expr_ids: Vec<_> = nodes
        .iter()
        .filter(|(_, node)| matches!(node, Node::Expression(_)))
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(expr_ids, expected_expr_ids);
}
