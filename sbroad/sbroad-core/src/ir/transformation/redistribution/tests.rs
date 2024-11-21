use super::*;
use crate::ir::node::Motion;
use crate::ir::relation::Column;
use crate::ir::relation::Table;
use crate::ir::transformation::helpers::sql_to_ir;
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::Plan;
use crate::ir::Slices;
use pretty_assertions::assert_eq;

#[test]
fn union_all_in_sq() {
    let query = r#"SELECT *
        FROM
            (SELECT "identification_number", "product_code"
            FROM "hash_testing"
            WHERE "sys_op" = 1
            UNION ALL
            SELECT "identification_number", "product_code"
            FROM "hash_testing_hist"
            WHERE "sys_op" > 1) AS "t3"
        WHERE "identification_number" = 1"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn inner_join_eq_for_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_sq_eq_for_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."id", "t2"."pc")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_sq_eq_for_keys_with_const() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", 1, "t1"."product_code") = ("t2"."id", 1, "t2"."pc")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_sq_less_for_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") < ("t2"."id", "t2"."pc")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn join_inner_sq_eq_no_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", 1) = (1, "t2"."pc")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn join_inner_sq_eq_no_outer_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", 1) = ("t2"."id", "t2"."pc")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn inner_join_full_policy_sq_in_filter() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")
        AND ("t"."a", "t"."b") >=
        (SELECT "hash_testing"."sys_op", "hash_testing"."bucket_id" FROM "hash_testing")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn inner_join_local_policy_sq_in_filter() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")
        AND ("t"."a", "t"."b") =
        (SELECT "hash_testing"."identification_number", "hash_testing"."product_code" FROM "hash_testing")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn inner_join_local_policy_sq_with_union_all_in_filter() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")
        AND ("t"."a", "t"."b") =
        (SELECT "hash_testing"."identification_number", "hash_testing"."product_code" FROM "hash_testing"
        UNION ALL
        SELECT "hash_testing_hist"."identification_number", "hash_testing_hist"."product_code" FROM "hash_testing_hist")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_and_local_full_policies() {
    // "hash_testing"      sharding_key = ("identification_number", "product_code")
    // "hash_testing_hist" sharding_key = the same
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."id", "t2"."pc")
        AND "t1"."identification_number" = "t2"."pc""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_or_local_full_policies() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."id", "t2"."pc")
        OR "t1"."identification_number" = "t2"."pc""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
}

/// Helper function to extract a motion id from a plan.
///
/// # Panics
///   Motion node does not found
#[must_use]
pub fn get_motion_id(plan: &Plan, slice_id: usize, motion_idx: usize) -> Option<&NodeId> {
    plan.slices.slice(slice_id).unwrap().position(motion_idx)
}

#[test]
fn test_slices_1() {
    let query = r#"select e from (select t2.f from t2 join t2 t3 on true) join t2 on true"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let expected_explain = String::from(
        r#"projection ("t2"."e"::unsigned -> "e")
    join on true::boolean
        scan
            projection ("t2"."f"::unsigned -> "f")
                join on true::boolean
                    scan "t2"
                        projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                            scan "t2"
                    motion [policy: full]
                        scan "t3"
                            projection ("t3"."e"::unsigned -> "e", "t3"."f"::unsigned -> "f", "t3"."g"::unsigned -> "g", "t3"."h"::unsigned -> "h")
                                scan "t2" -> "t3"
        motion [policy: full]
            scan "t2"
                projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                    scan "t2"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());

    // check both motions are in the same slice
    let t3_mid = *get_motion_id(&plan, 0, 0).unwrap();
    let t2_mid = *get_motion_id(&plan, 0, 1).unwrap();

    let slices = plan.calculate_slices(plan.get_top().unwrap()).unwrap();

    assert_eq!(slices, vec![vec![t3_mid, t2_mid]]);
}

#[test]
fn test_slices_2() {
    let query = r#"select count(e) from (select t2.f from t2 join t2 t3 on true) join t2 on true"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let expected_explain = String::from(
        r#"projection (sum(("count_5596"::integer))::decimal -> "col_1")
    motion [policy: full]
        projection (count(("t2"."e"::unsigned))::integer -> "count_5596")
            join on true::boolean
                scan
                    projection ("t2"."f"::unsigned -> "f")
                        join on true::boolean
                            scan "t2"
                                projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                                    scan "t2"
                            motion [policy: full]
                                scan "t3"
                                    projection ("t3"."e"::unsigned -> "e", "t3"."f"::unsigned -> "f", "t3"."g"::unsigned -> "g", "t3"."h"::unsigned -> "h")
                                        scan "t2" -> "t3"
                motion [policy: full]
                    scan "t2"
                        projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                            scan "t2"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());

    // check both motions are in the same slice
    let t3_mid = *get_motion_id(&plan, 0, 0).unwrap();
    let t2_mid = *get_motion_id(&plan, 0, 1).unwrap();

    // check last motion in the last slice
    let count_mid = *get_motion_id(&plan, 1, 0).unwrap();
    let slices = plan.calculate_slices(plan.get_top().unwrap()).unwrap();

    assert_eq!(slices, vec![vec![t3_mid, t2_mid], vec![count_mid]]);
}

#[test]
fn test_slices_3() {
    let mut plan = Plan::new();

    // Our plan is a DAG, let's test we compute slices correctly in this case.

    //         m6
    //         |
    //         u2
    //        /  \
    //       m4  m5
    //       |    |
    //       m3   u1
    //        \  / \
    //         m1   m2
    //         |    |
    //         s1   s2

    // Correct slices:
    // 1. [m1, m2]
    // 2. [m3, m5]
    // 3. [m4]
    // 4. [m6]

    let add_motion = |plan: &mut Plan, child_id: NodeId| -> NodeId {
        plan.add_motion(child_id, &MotionPolicy::Full, Program::default())
            .unwrap()
    };

    // We can't add multiple motions in a row because they will be squeezed
    // into one, so insert ScanSQ between them.
    let add_sq_and_motion = |plan: &mut Plan, child_id: NodeId| -> NodeId {
        let sq_id = plan.add_sub_query(child_id, None).unwrap();
        add_motion(plan, sq_id)
    };

    plan.add_rel(Table::new_global("t", vec![Column::default()], &[""]).unwrap());

    let s1_id = plan.add_scan("t", None).unwrap();
    let m1_id = add_motion(&mut plan, s1_id);

    let s2_id = plan.add_scan("t", None).unwrap();
    let m2_id = add_motion(&mut plan, s2_id);

    let m3_id = add_sq_and_motion(&mut plan, m1_id);
    let m4_id = add_sq_and_motion(&mut plan, m3_id);

    let u1_id = plan.add_union(m1_id, m2_id, false).unwrap();
    let m5_id = add_motion(&mut plan, u1_id);

    let u2_id = plan.add_union(m4_id, m5_id, false).unwrap();
    let m6_id = add_motion(&mut plan, u2_id);

    plan.set_top(m6_id).unwrap();

    let slices = plan.calculate_slices(plan.get_top().unwrap()).unwrap();

    let expected = vec![
        vec![m1_id, m2_id],
        vec![m3_id, m5_id],
        vec![m4_id],
        vec![m6_id],
    ];

    assert_eq!(slices, expected);
}

#[cfg(test)]
mod between;

#[cfg(test)]
mod except;

#[cfg(test)]
mod not_in;

#[cfg(test)]
mod segment;
