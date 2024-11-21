use pretty_assertions::{assert_eq, assert_ne};

use crate::{
    frontend::sql::ir::SubtreeCloner,
    ir::{
        node::{expression::Expression, Alias, Constant, NodeId, Row},
        transformation::helpers::sql_to_optimized_ir,
        tree::traversal::PostOrder,
        value::Value,
        Plan,
    },
};

#[test]
fn test_clone_dag() {
    let mut plan = Plan::new();

    // In plan we may have multiple nodes reffering to the same node,
    // let's check we can clone such plan correctly:
    //
    //      row
    //     /   \
    //    foo  bar
    //      \ /
    //     const(1)

    let value = Value::Integer(1);
    let const_id = plan.add_const(value.clone());
    let foo_id = plan.nodes.add_alias("foo", const_id).unwrap();
    let bar_id = plan.nodes.add_alias("bar", const_id).unwrap();
    let row_id = plan.add_row(vec![foo_id, bar_id], None);

    let mut cloner = SubtreeCloner::new(0);
    let new_row_id = cloner.clone(&mut plan, row_id, 0).unwrap();

    let new_ids: Vec<NodeId> = {
        let mut dfs = PostOrder::with_capacity(|x| plan.subtree_iter(x, true), 0);
        dfs.populate_nodes(new_row_id);
        dfs.take_nodes().into_iter().map(|n| n.1).collect()
    };

    // Check we cloned subtree correctly

    // Subtree iter will visit const node twice because
    // there are two incoming references
    assert_eq!(new_ids.len(), 5);

    let get_node = |idx: usize, old_id: NodeId| -> Expression<'_> {
        let new_id = new_ids[idx];
        assert_ne!(old_id, new_id);
        plan.get_expression_node(new_id).unwrap()
    };

    assert_eq!(
        Expression::Constant(&Constant {
            value: value.clone()
        }),
        get_node(0, const_id)
    );

    assert_eq!(
        Expression::Alias(&Alias {
            name: "foo".into(),
            child: new_ids[0]
        }),
        get_node(1, const_id)
    );

    assert_eq!(
        Expression::Constant(&Constant {
            value: value.clone()
        }),
        get_node(2, const_id)
    );

    assert_eq!(
        Expression::Alias(&Alias {
            name: "bar".into(),
            child: new_ids[2]
        }),
        get_node(3, const_id)
    );

    assert_eq!(
        Expression::Row(&Row {
            list: vec![new_ids[1], new_ids[3]],
            distribution: None
        }),
        get_node(4, const_id)
    );
}

#[test]
fn except_transform_with_dag_plan() {
    // In this plan we have Const node referred twice:
    // both `data` and `output` of `ValuesRow` fields
    // are refferring to it. Let's check except transformation
    // with global table works in this case.

    let input = r#"select 1 from (values (1)) except select e from t2 where e = 1"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"except
    projection (1::unsigned -> "col_1")
        scan
            values
                value row (data=ROW(1::unsigned))
    motion [policy: full]
        intersect
            projection ("t2"."e"::unsigned -> "e")
                selection ROW("t2"."e"::unsigned) = ROW(1::unsigned)
                    scan "t2"
            projection (1::unsigned -> "col_1")
                scan
                    values
                        value row (data=ROW(1::unsigned))
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}
