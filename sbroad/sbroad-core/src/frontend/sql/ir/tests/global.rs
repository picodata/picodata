use crate::ir::distribution::Distribution;
use crate::ir::node::relational::Relational;
use crate::ir::node::{Node, NodeId};
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::tree::traversal::{FilterFn, LevelNode, PostOrderWithFilter, REL_CAPACITY};
use crate::ir::value::Value;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

#[derive(PartialEq, Eq, Debug)]
enum DistMock {
    Segment,
    Global,
    Single,
    Any,
}

impl From<&Distribution> for DistMock {
    fn from(dist: &Distribution) -> Self {
        match dist {
            Distribution::Any => DistMock::Any,
            Distribution::Global => DistMock::Global,
            Distribution::Segment { .. } => DistMock::Segment,
            Distribution::Single => DistMock::Single,
        }
    }
}

fn collect_relational(plan: &Plan, predicate: FilterFn<'_, NodeId>) -> Vec<LevelNode<NodeId>> {
    let mut rel_tree = PostOrderWithFilter::with_capacity(
        |node| plan.nodes.rel_iter(node),
        REL_CAPACITY,
        predicate,
    );
    rel_tree.populate_nodes(plan.get_top().unwrap());
    let nodes = rel_tree.take_nodes();
    drop(rel_tree);
    nodes
}

fn check_distributions(
    plan: &Plan,
    nodes: &[LevelNode<NodeId>],
    expected_distributions: &[DistMock],
) {
    assert_eq!(
        expected_distributions.len(),
        nodes.len(),
        "different number of nodes"
    );
    for (LevelNode(level, id), expected) in nodes.iter().zip(expected_distributions.iter()) {
        let actual: DistMock = plan.get_rel_distribution(*id).unwrap().into();
        assert_eq!(
            expected, &actual,
            "wrong distribution for node ({id:?}) at level {level}"
        );
    }
}

fn check_selection_dist(plan: &Plan, expected_dist: DistMock) {
    let filter = |id: NodeId| -> bool {
        matches!(
            plan.get_node(id),
            Ok(Node::Relational(Relational::Selection(_)))
        )
    };
    let nodes = collect_relational(plan, Box::new(filter));
    check_distributions(plan, &nodes, &[expected_dist]);
}

#[test]
fn front_sql_global_tbl_sq1() {
    // sq has distribution Any, motion(full) must be inserted,
    // for sq with single distribution motion is not needed,
    // it will calculated on the router
    let input = r#"
    select * from "global_t"
    where "a" in (select "a" as a1 from "t") or
    "a" in (select sum("a") from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
        selection ("global_t"."a"::integer in ROW($1)) or ("global_t"."a"::integer in ROW($0))
            scan "global_t"
    subquery $0:
    scan
                projection (sum(("sum_1"::decimal))::decimal -> "col_1")
                    motion [policy: full]
                        projection (sum(("t"."a"::unsigned))::decimal -> "sum_1")
                            scan "t"
    subquery $1:
    motion [policy: full]
                scan
                    projection ("t"."a"::unsigned -> "a1")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
    check_selection_dist(&plan, DistMock::Global);
}

#[test]
fn front_sql_global_tbl_multiple_sqs1() {
    // For sq with single distribution we never need
    // a motion. But now the first subquery has
    // Segment distribution, and thus does need a motion.
    let input = r#"
    select * from "global_t"
    where ("a", "b") in (select "a" as a1, "b" as b1 from "t") and
    "a" in (select sum("a") from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
        selection (ROW("global_t"."a"::integer, "global_t"."b"::integer) in ROW($1, $1)) and ("global_t"."a"::integer in ROW($0))
            scan "global_t"
    subquery $0:
    scan
                projection (sum(("sum_1"::decimal))::decimal -> "col_1")
                    motion [policy: full]
                        projection (sum(("t"."a"::unsigned))::decimal -> "sum_1")
                            scan "t"
    subquery $1:
    motion [policy: full]
                scan
                    projection ("t"."a"::unsigned -> "a1", "t"."b"::unsigned -> "b1")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
    check_selection_dist(&plan, DistMock::Global);
}

#[test]
fn front_sql_global_tbl_multiple_sqs2() {
    // For sq with single distribution we never need
    // a motion. But now the first subquery has
    // Segment distribution, and thus does need a motion,
    // but it is connected with the second sq via `OR`,
    // so the motion is needed for the first subquery.
    let input = r#"
    select * from "global_t"
    where ("a", "b") in (select "a" as a1, "b" as b1 from "t") or
    "a" in (select sum("a") from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
        selection (ROW("global_t"."a"::integer, "global_t"."b"::integer) in ROW($1, $1)) or ("global_t"."a"::integer in ROW($0))
            scan "global_t"
    subquery $0:
    scan
                projection (sum(("sum_1"::decimal))::decimal -> "col_1")
                    motion [policy: full]
                        projection (sum(("t"."a"::unsigned))::decimal -> "sum_1")
                            scan "t"
    subquery $1:
    motion [policy: full]
                scan
                    projection ("t"."a"::unsigned -> "a1", "t"."b"::unsigned -> "b1")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
    check_selection_dist(&plan, DistMock::Global);
}

#[test]
fn front_sql_global_tbl_sq2() {
    // sq has distribution Segment, no motion must be inserted
    let input = r#"
    select * from "global_t"
    where ("a", "b") in (select "a" as a1, "b" as b1 from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
        selection ROW("global_t"."a"::integer, "global_t"."b"::integer) in ROW($0, $0)
            scan "global_t"
    subquery $0:
    motion [policy: full]
                scan
                    projection ("t"."a"::unsigned -> "a1", "t"."b"::unsigned -> "b1")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
    check_selection_dist(&plan, DistMock::Global);
}

#[test]
fn front_sql_global_tbl_sq3() {
    // sq has distribution Segment, but due to `not in` motion(full) must be inserted
    let input = r#"
    select * from "global_t"
    where ("a", "b") not in (select "a" as a1, "b" as b1 from "t") or
    ("a", "b") < (select "a" as a1, "b" as b1 from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
        selection (not (ROW("global_t"."a"::integer, "global_t"."b"::integer) in ROW($1, $1))) or (ROW("global_t"."a"::integer, "global_t"."b"::integer) < ROW($0, $0))
            scan "global_t"
    subquery $0:
    motion [policy: full]
                scan
                    projection ("t"."a"::unsigned -> "a1", "t"."b"::unsigned -> "b1")
                        scan "t"
    subquery $1:
    motion [policy: full]
                scan
                    projection ("t"."a"::unsigned -> "a1", "t"."b"::unsigned -> "b1")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
    check_selection_dist(&plan, DistMock::Global);
}

#[test]
fn front_sql_global_tbl_sq4() {
    // Reading from global subquery should not produce motion
    let input = r#"
    select "product_code" from "t" inner join "hash_testing"
    on "t"."a" = "hash_testing"."identification_number" and "hash_testing"."product_code"
    in (select "a"::text as a1 from "global_t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("hash_testing"."product_code"::string -> "product_code")
        join on ("t"."a"::unsigned = "hash_testing"."identification_number"::integer) and ("hash_testing"."product_code"::string in ROW($0))
            scan "t"
                projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
                    scan "t"
            motion [policy: full]
                scan "hash_testing"
                    projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op")
                        scan "hash_testing"
    subquery $0:
    scan
                projection ("global_t"."a"::integer::text -> "a1")
                    scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_tbl_sq5() {
    // Reading from global subquery should not produce motion
    let input = r#"
    select "a", "f" from "t" inner join "t2"
    on ("t"."a", "t"."b") = ("t2"."e", "t2"."f") AND
    "t"."c" in (select "a" as a1 from "global_t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."a"::unsigned -> "a", "t2"."f"::unsigned -> "f")
        join on (ROW("t"."a"::unsigned, "t"."b"::unsigned) = ROW("t2"."e"::unsigned, "t2"."f"::unsigned)) and ("t"."c"::unsigned in ROW($0))
            scan "t"
                projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
                    scan "t"
            scan "t2"
                projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                    scan "t2"
    subquery $0:
    scan
                projection ("global_t"."a"::integer -> "a1")
                    scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_tbl_sq6() {
    // Reading from global subquery should not produce motion
    let input = r#"
    select "a", "f"
    from "t" inner join "t2"
        on ("t"."a", "t"."b") = ("t2"."e", "t2"."f") OR
           "t"."c" in (select "a" as a1 from "global_t") OR
           "t"."d" not in (select "a" as a1 from "global_t") AND
           exists (select "a" * 20 as a1 from "global_t" where "a" = 1)
        where "e" in (select "a" * 10 from "global_t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."a"::unsigned -> "a", "t2"."f"::unsigned -> "f")
        selection "t2"."e"::unsigned in ROW($3)
            join on ((ROW("t"."a"::unsigned, "t"."b"::unsigned) = ROW("t2"."e"::unsigned, "t2"."f"::unsigned)) or ("t"."c"::unsigned in ROW($2))) or (exists ROW($0) and (not ("t"."d"::unsigned in ROW($1))))
                scan "t"
                    projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
                        scan "t"
                motion [policy: full]
                    scan "t2"
                        projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                            scan "t2"
    subquery $0:
    scan
                    projection ("global_t"."a"::integer * 20::unsigned -> "a1")
                        selection "global_t"."a"::integer = 1::unsigned
                            scan "global_t"
    subquery $1:
    scan
                    projection ("global_t"."a"::integer -> "a1")
                        scan "global_t"
    subquery $2:
    scan
                    projection ("global_t"."a"::integer -> "a1")
                        scan "global_t"
    subquery $3:
    scan
                projection ("global_t"."a"::integer * 10::unsigned -> "col_1")
                    scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_tbl_sq7() {
    // Reading from global subquery should not produce motion
    let input = r#"
    select "a", "f" from "t" inner join "t2"
    on ("t"."a", "t"."b") = ("t2"."e", "t2"."f") OR
    "t"."c" in (select "a" as a1 from "global_t") OR
    "t"."d" not in (select "a" as a1 from "global_t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."a"::unsigned -> "a", "t2"."f"::unsigned -> "f")
        join on ((ROW("t"."a"::unsigned, "t"."b"::unsigned) = ROW("t2"."e"::unsigned, "t2"."f"::unsigned)) or ("t"."c"::unsigned in ROW($1))) or (not ("t"."d"::unsigned in ROW($0)))
            scan "t"
                projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
                    scan "t"
            motion [policy: full]
                scan "t2"
                    projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                        scan "t2"
    subquery $0:
    scan
                projection ("global_t"."a"::integer -> "a1")
                    scan "global_t"
    subquery $1:
    scan
                projection ("global_t"."a"::integer -> "a1")
                    scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

fn check_join_dist(plan: &Plan, expected_distributions: &[DistMock]) {
    let filter = |id: NodeId| -> bool {
        matches!(plan.get_node(id), Ok(Node::Relational(Relational::Join(_))))
    };
    let nodes = collect_relational(plan, Box::new(filter));
    check_distributions(plan, &nodes, expected_distributions);
}

#[test]
fn front_sql_global_join1() {
    let input = r#"
    select "e", "a" from "global_t"
    inner join "t2"
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t2"."e"::unsigned -> "e", "global_t"."a"::integer -> "a")
        join on true::boolean
            scan "global_t"
                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                    scan "global_t"
            scan "t2"
                projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                    scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
    check_join_dist(&plan, &[DistMock::Segment]);
}

#[test]
fn front_sql_global_join2() {
    let input = r#"
    select "e", "a" from "t2"
    inner join "global_t"
    on "e" = "a" or "b" = "f"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Segment]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t2"."e"::unsigned -> "e", "global_t"."a"::integer -> "a")
        join on ("t2"."e"::unsigned = "global_t"."a"::integer) or ("global_t"."b"::integer = "t2"."f"::unsigned)
            scan "t2"
                projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                    scan "t2"
            scan "global_t"
                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                    scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_join3() {
    let input = r#"
    select "e", "a" from "t2"
    left join "global_t"
    on "e" = "a" or "b" = "f"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Segment]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t2"."e"::unsigned -> "e", "global_t"."a"::integer -> "a")
        left join on ("t2"."e"::unsigned = "global_t"."a"::integer) or ("global_t"."b"::integer = "t2"."f"::unsigned)
            scan "t2"
                projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                    scan "t2"
            scan "global_t"
                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                    scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_join4() {
    let input = r#"
    select e from (select sum("e") as e from "t2") as s
    left join "global_t"
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Single]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("s"."e"::decimal -> "e")
        left join on true::boolean
            scan "s"
                projection (sum(("sum_1"::decimal))::decimal -> "e")
                    motion [policy: full]
                        projection (sum(("t2"."e"::unsigned))::decimal -> "sum_1")
                            scan "t2"
            scan "global_t"
                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                    scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_join5() {
    let input = r#"
    select e from "global_t"
    left join (select sum("e") as e from "t2") as s
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Single]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("s"."e"::decimal -> "e")
        left join on true::boolean
            scan "global_t"
                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                    scan "global_t"
            scan "s"
                projection (sum(("sum_1"::decimal))::decimal -> "e")
                    motion [policy: full]
                        projection (sum(("t2"."e"::unsigned))::decimal -> "sum_1")
                            scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_join6() {
    let input = r#"
    select e from "global_t"
    inner join (select "e"*"e" as e from "t2") as s
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Any]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("s"."e"::unsigned -> "e")
        join on true::boolean
            scan "global_t"
                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                    scan "global_t"
            scan "s"
                projection ("t2"."e"::unsigned * "t2"."e"::unsigned -> "e")
                    scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_join7() {
    let input = r#"
    select e from (select "e"*"e" as e from "t2") as s
    inner join "global_t"
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Any]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("s"."e"::unsigned -> "e")
        join on true::boolean
            scan "s"
                projection ("t2"."e"::unsigned * "t2"."e"::unsigned -> "e")
                    scan "t2"
            scan "global_t"
                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                    scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_join8() {
    let input = r#"
    select e from (select "a"*"a" as e from "global_t")
    inner join "global_t"
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Global]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("e"::integer -> "e")
        join on true::boolean
            scan
                projection ("global_t"."a"::integer * "global_t"."a"::integer -> "e")
                    scan "global_t"
            scan "global_t"
                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                    scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_join9() {
    let input = r#"
    select e from (select "e"*"e" as e from "t2")
    left join "global_t"
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Any]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("e"::unsigned -> "e")
        left join on true::boolean
            scan
                projection ("t2"."e"::unsigned * "t2"."e"::unsigned -> "e")
                    scan "t2"
            scan "global_t"
                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                    scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_join10() {
    let input = r#"
    select e from (select "a"*"a" as e from "global_t")
    inner join "global_t"
    on e in (select "e" from "t2")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Global]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("e"::integer -> "e")
        join on "e"::integer in ROW($0)
            scan
                projection ("global_t"."a"::integer * "global_t"."a"::integer -> "e")
                    scan "global_t"
            scan "global_t"
                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                    scan "global_t"
    subquery $0:
    motion [policy: full]
                scan
                    projection ("t2"."e"::unsigned -> "e")
                        scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_join11() {
    let input = r#"
    select e from (select "a"*"a" as e from "global_t")
    inner join "global_t"
    on (e, e) in (select "e", "f" from "t2")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Global]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("e"::integer -> "e")
        join on ROW("e"::integer, "e"::integer) in ROW($0, $0)
            scan
                projection ("global_t"."a"::integer * "global_t"."a"::integer -> "e")
                    scan "global_t"
            scan "global_t"
                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                    scan "global_t"
    subquery $0:
    motion [policy: full]
                scan
                    projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f")
                        scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_aggregate1() {
    let input = r#"
    select sum("a") + avg("b" + "b") from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (sum(("global_t"."a"::integer))::decimal + avg(("global_t"."b"::integer + "global_t"."b"::integer))::decimal -> "col_1")
        scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_aggregate2() {
    let input = r#"
    select sum("a") + avg("b" + "b") from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (sum(("global_t"."a"::integer))::decimal + avg(("global_t"."b"::integer + "global_t"."b"::integer))::decimal -> "col_1")
        scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_aggregate3() {
    let input = r#"
    select "b"+"a", sum("a") from "global_t"
    group by "b"+"a"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("global_t"."b"::integer + "global_t"."a"::integer -> "col_1", sum(("global_t"."a"::integer))::decimal -> "col_2")
        group by ("global_t"."b"::integer + "global_t"."a"::integer) output: ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
            scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_aggregate4() {
    let input = r#"
    select "b"+"a", sum("a") from "global_t"
    group by "b"+"a"
    having avg("b") > 3
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("global_t"."b"::integer + "global_t"."a"::integer -> "col_1", sum(("global_t"."a"::integer))::decimal -> "col_2")
        having avg(("global_t"."b"::integer))::decimal > 3::unsigned
            group by ("global_t"."b"::integer + "global_t"."a"::integer) output: ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_aggregate5() {
    let input = r#"
    select "b"+"a", sum("a") from "global_t"
    where ("a", "b") in (select "e", "f" from "t2")
    group by "b"+"a"
    having avg("b") > 3
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("global_t"."b"::integer + "global_t"."a"::integer -> "col_1", sum(("global_t"."a"::integer))::decimal -> "col_2")
        having avg(("global_t"."b"::integer))::decimal > 3::unsigned
            group by ("global_t"."b"::integer + "global_t"."a"::integer) output: ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                selection ROW("global_t"."a"::integer, "global_t"."b"::integer) in ROW($0, $0)
                    scan "global_t"
    subquery $0:
    motion [policy: full]
                        scan
                            projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f")
                                scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_left_join1() {
    let input = r#"
    select "e", "b" from "global_t"
    left join "t2" on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("e"::unsigned -> "e", "b"::integer -> "b")
        motion [policy: full]
            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b", "t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                join on true::boolean
                    motion [policy: full]
                        scan "global_t"
                            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                                scan "global_t"
                    scan "t2"
                        projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                            scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_left_join2() {
    let input = r#"
    select "e", sum("b") from "global_t"
    left join "t2" on true
    group by "e"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("e"::unsigned -> "e", sum(("b"::integer))::decimal -> "col_1")
        group by ("e"::unsigned) output: ("a"::integer -> "a", "b"::integer -> "b", "e"::unsigned -> "e", "f"::unsigned -> "f", "g"::unsigned -> "g", "h"::unsigned -> "h")
            motion [policy: full]
                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b", "t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                    join on true::boolean
                        motion [policy: full]
                            scan "global_t"
                                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                                    scan "global_t"
                        scan "t2"
                            projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                                scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_left_join3() {
    let input = r#"
    select "e", "b" from
    (select "b" * "b" as "b" from "global_t")
    left join "t2" on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("e"::unsigned -> "e", "b"::integer -> "b")
        motion [policy: full]
            projection ("b"::integer -> "b", "t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                join on true::boolean
                    motion [policy: full]
                        scan
                            projection ("global_t"."b"::integer * "global_t"."b"::integer -> "b")
                                scan "global_t"
                    scan "t2"
                        projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                            scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_left_join4() {
    let input = r#"
    select "e", "b" from
    (select "b" * "b" as "b" from "global_t")
    left join
    (select "e" + 1 as "e" from "t2")
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("e"::unsigned -> "e", "b"::integer -> "b")
        motion [policy: full]
            projection ("b"::integer -> "b", "e"::unsigned -> "e")
                join on true::boolean
                    motion [policy: full]
                        scan
                            projection ("global_t"."b"::integer * "global_t"."b"::integer -> "b")
                                scan "global_t"
                    scan
                        projection ("t2"."e"::unsigned + 1::unsigned -> "e")
                            scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_order_by_from_global_node_must_not_add_motion() {
    let input = r#"select "b", "a" as "my_col" from "global_t" order by "my_col""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("b"::integer -> "b", "my_col"::integer -> "my_col")
        order by ("my_col"::integer)
            scan
                projection ("global_t"."b"::integer -> "b", "global_t"."a"::integer -> "my_col")
                    scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

fn check_union_dist(plan: &Plan, expected_distributions: &[DistMock]) {
    let filter = |id: NodeId| -> bool {
        matches!(
            plan.get_node(id),
            Ok(Node::Relational(Relational::UnionAll { .. }))
        )
    };
    let nodes = collect_relational(plan, Box::new(filter));
    check_distributions(plan, &nodes, expected_distributions);
}

#[test]
fn front_sql_global_union_all1() {
    let input = r#"
    select "a", "b" from "global_t"
    union all
    select "e", "f" from "t2"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    union all
        motion [policy: local]
            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                scan "global_t"
        projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f")
            scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    check_union_dist(&plan, &[DistMock::Any]);
}

#[test]
fn front_sql_global_union_all2() {
    let input = r#"
    select "a" from "global_t"
    union all
    select "e" from "t2"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    union all
        motion [policy: local]
            projection ("global_t"."a"::integer -> "a")
                scan "global_t"
        projection ("t2"."e"::unsigned -> "e")
            scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    check_union_dist(&plan, &[DistMock::Any]);
}

#[test]
fn front_sql_global_union_all3() {
    let input = r#"
    select * from (select "a" from "global_t"
    union all
    select sum("e") from "t2")
    union all
    select "b" from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    union all
        projection ("a"::integer -> "a")
            scan
                union all
                    projection ("global_t"."a"::integer -> "a")
                        scan "global_t"
                    motion [policy: local]
                        projection (sum(("sum_1"::decimal))::decimal -> "col_1")
                            motion [policy: full]
                                projection (sum(("t2"."e"::unsigned))::decimal -> "sum_1")
                                    scan "t2"
        motion [policy: local]
            projection ("global_t"."b"::integer -> "b")
                scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    check_union_dist(&plan, &[DistMock::Any, DistMock::Any]);
}

#[test]
fn front_sql_global_union_all5() {
    let input = r#"
    select "a" from "global_t"
    union all
    select "b" from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    union all
        projection ("global_t"."a"::integer -> "a")
            scan "global_t"
        projection ("global_t"."b"::integer -> "b")
            scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    check_union_dist(&plan, &[DistMock::Global]);
}

#[test]
fn front_sql_global_union() {
    let input = r#"
    select "a" from "global_t"
    union
    select "b" from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    motion [policy: full]
        union
            projection ("global_t"."a"::integer -> "a")
                scan "global_t"
            projection ("global_t"."b"::integer -> "b")
                scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_union1() {
    let input = r#"
    select "a" from "global_t"
    union
    select "e" from "t2"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    motion [policy: full]
        union
            motion [policy: local]
                projection ("global_t"."a"::integer -> "a")
                    scan "global_t"
            projection ("t2"."e"::unsigned -> "e")
                scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_global_union2() {
    let input = r#"
    select "a" from "global_t"
    union
    select sum("e") from "t2"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    motion [policy: full]
        union
            projection ("global_t"."a"::integer -> "a")
                scan "global_t"
            motion [policy: local]
                projection (sum(("sum_1"::decimal))::decimal -> "col_1")
                    motion [policy: full]
                        projection (sum(("t2"."e"::unsigned))::decimal -> "sum_1")
                            scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_union() {
    let input = r#"
    select * from (
        select "a" from "global_t"
        union
        select "e" from "t2"
    ) union
    select "f" from "t2"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    motion [policy: full]
        union
            motion [policy: local]
                projection ("a"::integer -> "a")
                    scan
                        motion [policy: full]
                            union
                                motion [policy: local]
                                    projection ("global_t"."a"::integer -> "a")
                                        scan "global_t"
                                projection ("t2"."e"::unsigned -> "e")
                                    scan "t2"
            projection ("t2"."f"::unsigned -> "f")
                scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn check_plan_except_global_vs_segment() {
    let input = r#"
    select "a", "b" from "global_t"
    where "a" = ?
    except
    select "e", "f" from "t2"
    "#;

    let plan = sql_to_optimized_ir(input, vec![Value::Unsigned(1)]);

    // TODO: the subtree for left except child is reused
    // from another motion, show this in explain
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    except
        projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
            selection "global_t"."a"::integer = 1::unsigned
                scan "global_t"
        motion [policy: full]
            intersect
                projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f")
                    scan "t2"
                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                    selection "global_t"."a"::integer = 1::unsigned
                        scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn check_plan_except_global_vs_any() {
    let input = r#"
    select "a" from "global_t"
    except
    select "e" from "t2"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // TODO: the subtree for left except child is reused
    // from another motion, show this in explain
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    except
        projection ("global_t"."a"::integer -> "a")
            scan "global_t"
        motion [policy: full]
            intersect
                projection ("t2"."e"::unsigned -> "e")
                    scan "t2"
                projection ("global_t"."a"::integer -> "a")
                    scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn check_plan_except_global_vs_global() {
    let input = r#"
    select "a" from "global_t"
    except
    select "b" from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    except
        projection ("global_t"."a"::integer -> "a")
            scan "global_t"
        projection ("global_t"."b"::integer -> "b")
            scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn check_plan_except_global_vs_single() {
    let input = r#"
    select "a" from "global_t"
    except
    select sum("e") from "t2"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    except
        projection ("global_t"."a"::integer -> "a")
            scan "global_t"
        projection (sum(("sum_1"::decimal))::decimal -> "col_1")
            motion [policy: full]
                projection (sum(("t2"."e"::unsigned))::decimal -> "sum_1")
                    scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn check_plan_except_single_vs_global() {
    let input = r#"
    select sum("e") from "t2"
    except
    select "a" from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    except
        projection (sum(("sum_1"::decimal))::decimal -> "col_1")
            motion [policy: full]
                projection (sum(("t2"."e"::unsigned))::decimal -> "sum_1")
                    scan "t2"
        projection ("global_t"."a"::integer -> "a")
            scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn check_plan_except_segment_vs_global() {
    let input = r#"
    select "e", "f" from "t2"
    except
    select "a", "b" from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    except
        projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f")
            scan "t2"
        projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
            scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn check_plan_except_any_vs_global() {
    let input = r#"
    select "e" from "t2"
    except
    select "b" from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    except
        projection ("t2"."e"::unsigned -> "e")
            scan "t2"
        projection ("global_t"."b"::integer -> "b")
            scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn check_plan_except_non_trivial_global_subtree_vs_any() {
    // check that plan is correctly built when left global
    // subtree is something more difficult than a scan of
    // a global table
    let input = r#"
    select "b" from "global_t"
    left join (select "b" as "B" from "global_t")
    on "a" = "B"
    where "a" = 1
    except
    select "e" from "t2"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    except
        projection ("global_t"."b"::integer -> "b")
            selection "global_t"."a"::integer = 1::unsigned
                left join on "global_t"."a"::integer = "B"::integer
                    scan "global_t"
                        projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                            scan "global_t"
                    scan
                        projection ("global_t"."b"::integer -> "B")
                            scan "global_t"
        motion [policy: full]
            intersect
                projection ("t2"."e"::unsigned -> "e")
                    scan "t2"
                projection ("global_t"."b"::integer -> "b")
                    selection "global_t"."a"::integer = 1::unsigned
                        left join on "global_t"."a"::integer = "B"::integer
                            scan "global_t"
                                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                                    scan "global_t"
                            scan
                                projection ("global_t"."b"::integer -> "B")
                                    scan "global_t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}
