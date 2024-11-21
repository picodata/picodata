use crate::ir::transformation::helpers::sql_to_ir;
use crate::ir::transformation::redistribution::tests::get_motion_id;
use crate::ir::transformation::redistribution::{Key, MotionPolicy};
use crate::ir::Slices;
use pretty_assertions::assert_eq;

use super::{Motion, Relational};

#[test]
fn except1() {
    let query = r#"SELECT 1, 2 FROM "hash_testing" AS "t"
        EXCEPT DISTINCT
        SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *get_motion_id(&plan, 0, 0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn except2() {
    let query = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        EXCEPT DISTINCT
        SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist" AS "t""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn except3() {
    let query = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        EXCEPT
        SELECT 1, 2 FROM "hash_testing_hist""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *get_motion_id(&plan, 0, 0).unwrap();
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
fn except4() {
    let query = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        EXCEPT
        SELECT * FROM (
            SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist"
            UNION ALL
            SELECT 1, 2 FROM "hash_testing_hist"
        ) as t"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *get_motion_id(&plan, 0, 0).unwrap();
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
fn except5() {
    let query = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        EXCEPT
        SELECT * FROM (
            SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist"
            EXCEPT
            SELECT 1, 2 FROM "hash_testing_hist"
        ) as t"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *get_motion_id(&plan, 0, 0).unwrap();
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
    let no_other_motions = get_motion_id(&plan, 0, 1).is_none();
    assert_eq!(no_other_motions, true);
}
