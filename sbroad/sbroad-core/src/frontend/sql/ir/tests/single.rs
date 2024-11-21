use smol_str::{SmolStr, ToSmolStr};

use crate::ir::node::relational::Relational;
use crate::ir::node::{Motion, NodeId};
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::transformation::redistribution::{MotionKey, MotionPolicy, Target};
use crate::ir::tree::traversal::{PostOrder, REL_CAPACITY};
use crate::ir::Plan;

/// Helper struct equivalent to `MotionPolicy`
/// that allows to be created more easily with more readability.
///
/// It is intended only for testing purposes.
#[derive(PartialEq, Eq, Debug)]
enum Policy {
    None,
    Local,
    Full,
    Segment(Vec<SmolStr>),
}

impl Policy {
    fn new_seg(targets: &[&str]) -> Policy {
        Policy::Segment(
            targets
                .iter()
                .map(|x| (*x).to_smolstr())
                .collect::<Vec<SmolStr>>(),
        )
    }
}

impl Plan {
    fn to_test_motion(&self, node_id: NodeId) -> Policy {
        let node = self.get_relation_node(node_id).unwrap();

        match node {
            Relational::Motion(Motion { policy, .. }) => match policy {
                MotionPolicy::None => Policy::None,
                MotionPolicy::Full => Policy::Full,
                MotionPolicy::Local => Policy::Local,
                MotionPolicy::Segment(MotionKey { targets })
                | MotionPolicy::LocalSegment(MotionKey { targets }) => {
                    let mut new_targets: Vec<SmolStr> = Vec::with_capacity(targets.len());
                    let aliases = self.get_relational_aliases(node_id).unwrap();
                    for t in targets {
                        match t {
                            Target::Reference(position) => {
                                let a = aliases.get(*position).unwrap();
                                let striped_alias = if let Some(s) = a.strip_prefix('\"') {
                                    if let Some(ss) = s.strip_suffix('\"') {
                                        ss.to_smolstr()
                                    } else {
                                        a.to_smolstr()
                                    }
                                } else {
                                    a.to_smolstr()
                                };
                                new_targets.push(striped_alias);
                            }
                            Target::Value(value) => new_targets.push(value.to_smolstr()),
                        }
                    }
                    Policy::Segment(new_targets)
                }
            },
            _ => Policy::None,
        }
    }
}

fn check_join_motions(
    sql: &str,
    left_expected: Policy,
    right_expected: Policy,
    sq_policies: Option<Vec<Policy>>,
) {
    let plan = sql_to_optimized_ir(sql, vec![]);
    let mut dfs = PostOrder::with_capacity(|x| plan.nodes.rel_iter(x), REL_CAPACITY);
    let level_node = dfs
        .iter(plan.get_top().unwrap())
        .find(|level_node| -> bool {
            let rel = plan.get_relation_node(level_node.1).unwrap();
            matches!(rel, Relational::Join(_))
        })
        .unwrap();
    let join_id = level_node.1;
    let children = plan.get_relational_children(join_id).unwrap();
    let (left_id, right_id, sq_nodes_ids) = (children[0], children[1], &children[2..]);
    let (left_actual, right_actual) = (plan.to_test_motion(left_id), plan.to_test_motion(right_id));
    assert_eq!(left_expected, left_actual);
    assert_eq!(right_expected, right_actual);
    if let Some(expected_sq_policies) = sq_policies {
        let actual_sq_policies = sq_nodes_ids
            .iter()
            .map(|x| plan.to_test_motion(*x))
            .collect::<Vec<Policy>>();
        assert_eq!(expected_sq_policies, actual_sq_policies);
    }
}

#[test]
fn front_sql_join_single_both_1() {
    let input = r#"SELECT * from (select sum("a") as a from "t") as o 
        inner join (select cast(count("b") as decimal) as b from "t") as i
        on o.a = i.b
    "#;

    check_join_motions(input, Policy::None, Policy::None, None);
}

#[test]
fn front_sql_join_single_both_2() {
    let input = r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select cast(count("b") as decimal) as c, count("d") as d from "t") as i
        on o.a = i.c and o.b = i.c
    "#;

    check_join_motions(input, Policy::None, Policy::None, None);
}

#[test]
fn front_sql_join_single_both_3() {
    let inputs = [
        r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select cast(count("b") as decimal) as c, cast(count("d") as decimal) as d from "t") as i
        on o.a = i.c and o.b = i.d"#,
        r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select cast(count("b") as decimal) as c, cast(count("d") as decimal) as d from "t") as i
        on (o.a, o.b) = (i.c, i.d)"#,
        r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select cast(count("b") as decimal) as c, cast(count("d") as decimal) as d from "t") as i
        on (o.a, i.d) = (i.c, o.b)"#,
    ];

    for input in inputs {
        check_join_motions(input, Policy::None, Policy::None, None);
    }
}

#[test]
fn front_sql_join_single_both_4() {
    let input = r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select count("b") as c, count("d") as d from "t") as i
        on o.a = i.c and o.b < i.d
    "#;

    check_join_motions(input, Policy::None, Policy::None, None);
}

#[test]
fn front_sql_join_single_both_5() {
    let input = r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select cast(count("b") as decimal) as c, count("d") as d from "t") as i
        on cast(o.a as number) = i.c
    "#;

    check_join_motions(input, Policy::None, Policy::None, None);
}

#[test]
fn front_sql_join_single_both_6() {
    let input = r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select cast(count("b") as decimal) as c, count("d") as d from "t") as i
        on o.a = i.c and i.c = 1 
    "#;

    check_join_motions(input, Policy::None, Policy::None, None);
}

#[test]
fn front_sql_join_single_both_7() {
    let input = r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select cast(count("b") as decimal) as c, count("d") as d from "t") as i
        on o.a = i.c and i.c + i.d = 2
    "#;

    check_join_motions(input, Policy::None, Policy::None, None);
}

#[test]
fn front_sql_join_single_both_8() {
    let input = r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select cast(count("b") as decimal) as c, count("d") as d from "t") as i
        on o.a = i.c and i.c < 2
    "#;

    check_join_motions(input, Policy::None, Policy::None, None);
}

#[test]
fn front_sql_join_single_both_9() {
    let input = r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select cast(count("b") as decimal) as c, count("d") as d from "t") as i
        on o.a = i.c or i.c < 2
    "#;

    check_join_motions(input, Policy::None, Policy::None, None);
}

#[test]
fn front_sql_join_single_both_10() {
    let input = r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select cast(count("b") as decimal) as c, cast(count("d") as decimal) as d from "t") as i
        on (o.a, o.a) = (i.c, i.d)
    "#;

    check_join_motions(input, Policy::None, Policy::None, None);
}

#[test]
fn front_sql_join_single_both_11() {
    let input = r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select cast(count("b") as decimal) as c, cast(count("d") as decimal) as d from "t") as i
        on (o.a, o.a) = (i.c, i.d)
    "#;

    check_join_motions(input, Policy::None, Policy::None, None);
}

#[test]
fn front_sql_join_single_both_12() {
    let input = r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select cast(count("b") as decimal) as c, cast(count("d") as decimal) as d from "t") as i
        on (o.a, o.a) = (i.c, i.d) or i.c in (select "a" as q from "t")
    "#;

    check_join_motions(input, Policy::None, Policy::None, Some(vec![Policy::Full]));
}

#[test]
fn front_sql_join_single_left_1() {
    let input = r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select cast("b" as decimal) as c, cast("d" as decimal) as d from "t") as i
        on (o.a, o.a) = (i.c, i.d)
    "#;

    check_join_motions(input, Policy::Full, Policy::None, None);
}

#[test]
fn front_sql_join_single_left_2() {
    let input = r#"SELECT * from (select cast(sum("a") as unsigned) as a, cast(sum("b") as unsigned) as b from "t") as o 
        inner join (select "a" as c, "b" as d from "t") as i
        on (o.a, o.b) = (i.c, i.d)
    "#;

    check_join_motions(input, Policy::new_seg(&["a", "b"]), Policy::None, None);
}

#[test]
fn front_sql_join_single_left_3() {
    let input = r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select "a" as c, cast("b" as decimal) as d from "t") as i
        on (o.a, o.b) = (cast(i.c as number), i.d)
    "#;

    check_join_motions(input, Policy::Full, Policy::None, None);
}

#[test]
fn front_sql_join_single_left_4() {
    // Inner child here will have distribution Segment, while outer child will have
    // Distribution::Single
    let input = r#"SELECT * from (select cast(sum("a") as unsigned) as a, cast(sum("b") as unsigned) as b from "t") as o 
        inner join (select "a" as c, "b" as d from "t" group by "a", "b") as i
        on (o.a, o.b) = (i.c, i.d) and o.a in (select "a" from "t")
    "#;

    check_join_motions(
        input,
        Policy::new_seg(&["a", "b"]),
        Policy::None,
        Some(vec![Policy::Full]),
    );
}

#[test]
fn front_sql_join_single_left_5() {
    let input = r#"SELECT * from (select cast(sum("a") as unsigned) as a, sum("b") as b from "t") as o 
        inner join "test_space" as i
        on o.a = i."id" and (i."sysFrom" = i."sys_op" and o.a = cast(o.a as number) + 1)
    "#;

    check_join_motions(input, Policy::new_seg(&["a"]), Policy::None, None);
}

#[test]
fn front_sql_join_single_left_6() {
    let input = r#"SELECT * from (select cast(sum("a") as unsigned) as a, sum("b") as b from "t") as o 
        inner join "test_space" as i
        on o.a = i."id" and o.a in (select cast(sum("a") as unsigned) from "t")
    "#;

    check_join_motions(
        input,
        Policy::new_seg(&["a"]),
        Policy::None,
        Some(vec![Policy::Full]),
    );
}

#[test]
fn front_sql_join_single_left_7() {
    let input = r#"
            select o.a, o.b, i.c, i.d from  (select "c" + 3 as c, cast("d" + 4 as decimal) as d from "t") as i
            inner join (select sum("a") as a, count("b") as b from "t") as o
            on o.a = i.d or o.b = i.c and i.c in (select "a" from "t")
            where o.a > 5
    "#;

    check_join_motions(input, Policy::None, Policy::Full, Some(vec![Policy::Full]));
}

#[test]
fn front_sql_join_single_left_8() {
    let input = r#"
            select o.a, o.b, i.c, i.d from  (select cast("c" + 3 as decimal) as c, cast("d" + 4 as decimal) as d from "t") as i
            inner join (select sum("a") as a, count("b") as b from "t") as o
            on o.a = i.d or o.b = i.c and i.c in (select sum("a") from "t")
    "#;

    check_join_motions(input, Policy::None, Policy::Full, Some(vec![Policy::Full]));
}
