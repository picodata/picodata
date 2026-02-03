use super::*;
use crate::ir::relation::{SpaceEngine, Table};
use crate::ir::tests::column_user_non_null;
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::tree::traversal::{PostOrder, REL_CAPACITY};
use crate::ir::types::UnrestrictedType as Type;
use crate::ir::Plan;
use pretty_assertions::assert_eq;
use rand::random;
use smol_str::SmolStr;

#[test]
fn proj_preserve_dist_key() {
    let mut plan = Plan::default();

    let t = Table::new_sharded(
        random(),
        "t",
        vec![
            column_user_non_null(SmolStr::from("a"), Type::Boolean),
            column_user_non_null(SmolStr::from("b"), Type::Integer),
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
    let proj_id = plan
        .add_proj(scan_id, vec![], &["a", "b"], false, false)
        .unwrap();

    plan.top = Some(proj_id);

    let rel_node = plan.get_relation_node(scan_id).unwrap();
    let scan_output = rel_node.output();

    plan.set_rel_output_distribution(scan_id).unwrap();

    let keys: HashSet<_, RepeatableState> = collection! { Key::new(vec![1, 0]) };
    assert_eq!(
        Distribution::Segment { keys: keys.into() },
        plan.get_distribution(scan_output).unwrap()
    );

    let rel_node = plan.get_relation_node(proj_id).unwrap();
    let proj_output: NodeId = rel_node.output();

    plan.set_rel_output_distribution(proj_id).unwrap();

    let keys: HashSet<_, RepeatableState> = collection! { Key::new(vec![1, 0]) };
    assert_eq!(
        Distribution::Segment { keys: keys.into() },
        plan.get_distribution(proj_output).unwrap()
    );
}

#[test]
fn projection_any_dist_for_expr() {
    let input = r#"select count("id") FROM "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // check explain first
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (sum(("count_1"::int))::int -> "col_1")
        motion [policy: full, program: ReshardIfNeeded]
            projection (count(("test_space"."id"::int::int))::int -> "count_1")
                scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    // check that local Projection has Distribution::Any
    let local_proj_id = {
        let dfs = PostOrder::with_capacity(|x| plan.nodes.rel_iter(x), REL_CAPACITY);
        dfs.into_iter(plan.top.unwrap())
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
        Distribution::Any,
        plan.get_distribution(plan.get_relational_output(local_proj_id).unwrap())
            .unwrap()
    );
}
//TODO: add other distribution variants to the test cases.
