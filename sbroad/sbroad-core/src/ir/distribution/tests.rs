use super::*;
use crate::ir::relation::{SpaceEngine, Table, Type};
use crate::ir::tests::column_user_non_null;
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::tree::traversal::{PostOrder, REL_CAPACITY};
use crate::ir::{Node, Plan};
use pretty_assertions::assert_eq;
use smol_str::SmolStr;

#[test]
fn proj_preserve_dist_key() {
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
    let proj_id = plan.add_proj(scan_id, &["a", "b"], false, false).unwrap();

    plan.top = Some(proj_id);

    let rel_node = plan.get_relation_node(scan_id).unwrap();
    let scan_output = rel_node.output();

    plan.set_distribution(scan_output).unwrap();
    let expr_node = plan.get_expression_node(scan_output).unwrap();
    let keys: HashSet<_, RepeatableState> = collection! { Key::new(vec![1, 0]) };
    assert_eq!(
        &Distribution::Segment { keys: keys.into() },
        expr_node.distribution().unwrap()
    );

    let rel_node = plan.get_relation_node(proj_id).unwrap();
    let proj_output: NodeId = rel_node.output();

    plan.set_distribution(proj_output).unwrap();
    let expr_node = plan.get_node(proj_output).unwrap();
    if let Node::Expression(expr) = expr_node {
        let keys: HashSet<_, RepeatableState> = collection! { Key::new(vec![1, 0]) };
        assert_eq!(
            &Distribution::Segment { keys: keys.into() },
            expr.distribution().unwrap()
        );
    }
}

#[test]
fn projection_any_dist_for_expr() {
    let input = r#"select count("id") FROM "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // check explain first
    let expected_explain = SmolStr::from(
        r#"projection (sum(("count_696"::integer))::decimal -> "col_1")
    motion [policy: full]
        projection (count(("test_space"."id"::unsigned))::integer -> "count_696")
            scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());

    // check that local Projection has Distribution::Any
    let local_proj_id = {
        let mut dfs = PostOrder::with_capacity(|x| plan.nodes.rel_iter(x), REL_CAPACITY);
        dfs.iter(plan.top.unwrap())
            .find(|level_node| {
                matches!(
                    plan.get_relation_node(level_node.1).unwrap(),
                    Relational::Projection(_)
                )
            })
            .unwrap()
            .1
    };
    assert_eq!(
        &Distribution::Any,
        plan.get_distribution(plan.get_relational_output(local_proj_id).unwrap())
            .unwrap()
    );
}
//TODO: add other distribution variants to the test cases.
