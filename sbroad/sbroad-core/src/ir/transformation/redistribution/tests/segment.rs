use crate::collection;
use crate::ir::distribution::{Distribution, Key};
use crate::ir::helpers::RepeatableState;
use crate::ir::node::{Node64, NodeId};
use crate::ir::relation::Column;
use crate::ir::transformation::helpers::sql_to_ir;
use crate::ir::transformation::redistribution::{MotionKey, MotionPolicy, Target};
use pretty_assertions::assert_eq;
use std::collections::HashSet;

use super::{Motion, Relational};

#[test]
fn inner_join1() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."pc", "t2"."id")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(
                (Key {
                    positions: vec![1, 0]
                })
                .into()
            )
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn inner_join2() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")
        AND ("t"."a", "t"."b") =
        (SELECT "hash_testing"."identification_number", "hash_testing"."product_code" FROM "hash_testing"
        UNION ALL
        SELECT "hash_testing_hist"."product_code", "hash_testing_hist"."identification_number" FROM "hash_testing_hist")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(
                (Key {
                    positions: vec![0, 1]
                })
                .into()
            )
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn inner_join3() {
    /*
       test_space is sharded by ("id") pos: (0)
       hash_testing & hash_testing_hist have the same columns and sharded
       by (indentification_number, product_code) pos: (0, 1)

       outer join child will have distribution Segment(0)
       inner join child will have distribution Any
       Motion for inner child will have policy Segment(0)
       and motion distribution will be Segment(0)
       then for join distribution we will have 2 keys:
       outer child gives Key(0)
       inner child gives Key(2) (shifted first column of inner child:
       outer child have two columns)
    */
    let query = r#"SELECT *
        FROM
            (SELECT "id", "FIRST_NAME"
            FROM "test_space"
            WHERE "sys_op" < 0
                    AND "sysFrom" >= 0
            UNION ALL
            SELECT "id", "FIRST_NAME"
            FROM "test_space_hist"
            WHERE "sysFrom" <= 0) AS "t3"
        INNER JOIN
            (SELECT "identification_number"
            FROM "hash_testing_hist"
            WHERE "sys_op" > 0
            UNION ALL
            SELECT "identification_number"
            FROM "hash_single_testing_hist"
            WHERE "sys_op" <= 0) AS "t8"
            ON "t3"."id" = "t8"."identification_number"
        WHERE "t3"."id" = 1 AND "t8"."identification_number" = 1"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment((Key { positions: vec![0] }).into())
        );
    } else {
        panic!("Expected a motion node");
    }

    // Check distribution of the join output tuple.
    let mut join_node = None;
    for node in plan.nodes.arena64.iter().enumerate() {
        if let Node64::Join(_) = node.1 {
            join_node = Some(
                plan.get_relation_node(NodeId {
                    offset: u32::try_from(node.0).unwrap(),
                    arena_type: crate::ir::node::ArenaType::Arena64,
                })
                .unwrap(),
            );
            break;
        }
    }
    let join = join_node.unwrap();
    let dist = plan.get_distribution(join.output()).unwrap();
    let keys: HashSet<_, RepeatableState> = collection! { Key::new(vec![0]), Key::new(vec![2]) };
    assert_eq!(&Distribution::Segment { keys: keys.into() }, dist,);
}

#[test]
fn inner_join4() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN "t" as "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."d", "t2"."a")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(
                (Key {
                    positions: vec![3, 0]
                })
                .into()
            )
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn insert1() {
    let query = r#"INSERT INTO "t" SELECT "d", "c", "b", "a" FROM "t""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(
                (Key {
                    positions: vec![0, 1]
                })
                .into()
            )
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn insert2() {
    let query = r#"INSERT INTO "t" SELECT * FROM "t""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(
            *policy,
            MotionPolicy::LocalSegment(
                (Key {
                    positions: vec![0, 1]
                })
                .into()
            )
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn insert3() {
    let query = r#"INSERT INTO "t" ("a", "b") SELECT "a", "b" FROM "t""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(
            *policy,
            MotionPolicy::LocalSegment(
                (Key {
                    positions: vec![0, 1]
                })
                .into()
            )
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn insert4() {
    let query = r#"INSERT INTO "t" ("b", "a") SELECT "a", "b" FROM "t""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(
                (Key {
                    positions: vec![1, 0]
                })
                .into()
            )
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn insert5() {
    let query = r#"INSERT INTO "t" ("c", "d", "b") SELECT "a", "c", "b" FROM "t""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(MotionKey {
                targets: vec![Target::Value(Column::default_value()), Target::Reference(2),]
            })
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn insert6() {
    let query = r#"INSERT INTO "t" ("c", "b") SELECT "b", "c" FROM "t""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(MotionKey {
                targets: vec![Target::Value(Column::default_value()), Target::Reference(1)]
            })
        );
    } else {
        panic!("Expected a motion node");
    }
}
