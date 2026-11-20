use pretty_assertions::assert_eq;
use sql::helpers::get_motion_id;
use sql::helpers::sql_to_ir;
use sql::ir::node::relational::Relational;
use sql::ir::node::Motion;
use sql::ir::transformation::redistribution::MotionPolicy;
use sql::ir::transformation::PlanningState;
use sql::ir::Slices;

#[test]
#[allow(clippy::similar_names)]
fn between1() {
    let query = r#"SELECT "identification_number" FROM "hash_testing" AS "t"
        WHERE (SELECT "identification_number" FROM "hash_testing_hist" AS "h"
            WHERE "identification_number" = 2) BETWEEN 1 AND 2"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions(&mut PlanningState::default()).unwrap();
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
    plan.add_motions(&mut PlanningState::default()).unwrap();
    assert_eq!(Slices::empty(), *plan.slices());
}

#[test]
fn between3() {
    let query = r#"SELECT "identification_number" FROM "hash_testing" AS "t"
        WHERE "identification_number" BETWEEN 1 AND (
            SELECT "identification_number" FROM "hash_testing_hist"
            WHERE "identification_number" = 3)"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions(&mut PlanningState::default()).unwrap();
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
    plan.add_motions(&mut PlanningState::default()).unwrap();
    assert_eq!(Slices::empty(), *plan.slices());
}

/// A subquery of a join condition that is not an operand of a comparison gets a
/// full motion, as in the other nodes. `t5` and `t3_2` are sharded by `a`.
#[test]
fn subquery_outside_a_comparison_of_a_join_condition_gets_a_full_motion() {
    for join in [
        "JOIN t3_2 ON t5.a = t3_2.a AND t5.b BETWEEN (SELECT b FROM t3_2) AND 5",
        "LEFT JOIN t3_2 ON t5.a = t3_2.a AND t5.b BETWEEN (SELECT b FROM t3_2) AND 5",
        "JOIN t3_2 ON t5.a = t3_2.a AND t5.b NOT BETWEEN (SELECT b FROM t3_2) AND 5",
        "JOIN t3_2 ON t5.a = t3_2.a AND (SELECT b FROM t3_2) BETWEEN t5.b AND 5",
        "JOIN t3_2 ON t5.a = t3_2.a AND t5.b + (SELECT b FROM t3_2) > 5",
    ] {
        let query = format!("SELECT t5.a FROM t5 {join}");
        assert_full_motion(&query);
    }
}

/// NOT reaches `IN` in an operand of BETWEEN, as under a comparison, and `NOT IN`
/// needs all rows of its subquery for their NULLs.
#[test]
fn not_in_under_between_gets_a_full_motion() {
    assert_full_motion(
        "SELECT a FROM t5 WHERE (NOT (a IN (SELECT a FROM t3_2) OR b = 1)) \
         BETWEEN true AND true",
    );
}

fn assert_full_motion(query: &str) {
    let mut plan = sql::helpers::sql_to_ir_without_bind(&format!("EXPLAIN (LOGICAL) {query}"), &[]);
    plan.optimize().unwrap();
    let explain = sql::explain::explain_logical(&plan).unwrap();
    let subquery = explain.split("subquery $0:").nth(1).unwrap_or_default();
    assert!(
        subquery.trim_start().starts_with("motion [policy: full"),
        "{query}\n{explain}"
    );
}
