use crate::ir::node::relational::Relational;
use crate::ir::node::Motion;
use crate::ir::transformation::helpers::sql_to_ir;
use crate::ir::transformation::redistribution::tests::get_motion_id;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::Slices;
use pretty_assertions::assert_eq;

#[test]
#[allow(clippy::similar_names)]
fn between1() {
    let query = r#"SELECT "identification_number" FROM "hash_testing" AS "t"
        WHERE (SELECT "identification_number" FROM "hash_testing_hist" AS "h"
            WHERE "identification_number" = 2) BETWEEN 1 AND 2"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *get_motion_id(&plan, 0, 0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
    let no_other_motions = get_motion_id(&plan, 0, 1).is_none();
    assert_eq!(no_other_motions, true);
}

#[test]
fn between2() {
    let query = r#"SELECT "identification_number" FROM "hash_testing" AS "t"
        WHERE "identification_number" BETWEEN 1 AND 2"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn between3() {
    let query = r#"SELECT "identification_number" FROM "hash_testing" AS "t"
        WHERE "identification_number" BETWEEN 1 AND (
            SELECT "identification_number" FROM "hash_testing_hist"
            WHERE "identification_number" = 3)"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *get_motion_id(&plan, 0, 0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
    let no_other_motions = get_motion_id(&plan, 0, 2).is_none();
    assert_eq!(no_other_motions, true);
}

#[test]
fn between4() {
    let query = r#"SELECT "identification_number" FROM "hash_testing" AS "t"
        WHERE 1 BETWEEN "identification_number" AND 2"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}
