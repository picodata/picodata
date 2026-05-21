use super::*;
use crate::ir::node::Motion;
use crate::ir::relation::Column;
use crate::ir::relation::Table;
use crate::ir::transformation::helpers::sql_to_ir;
use crate::ir::transformation::helpers::sql_to_ir_without_bind;
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::types::{DerivedType, UnrestrictedType};
use crate::ir::Plan;
use crate::ir::Slices;
use pretty_assertions::assert_eq;
use rand::random;

fn prepared_optimized_ir(query: &str, params: &[DerivedType]) -> Plan {
    sql_to_ir_without_bind(query, params).optimize().unwrap()
}

fn has_full_motion(plan: &Plan) -> bool {
    plan.slices
        .slices()
        .iter()
        .flat_map(|slice| slice.positions().iter())
        .any(|motion_id| {
            matches!(
                plan.get_relation_node(*motion_id).unwrap(),
                Relational::Motion(Motion {
                    policy: MotionPolicy::Full,
                    ..
                })
            )
        })
}

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

    let plan = sql_to_ir(query, vec![]).add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn inner_join_eq_for_keys() {
    let query = r#"SELECT * FROM "hash_testing2" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")"#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn inner_join_eq_via_shared_const_in_on() {
    let query = r#"SELECT * FROM t5 AS t1
        INNER JOIN t5 AS t2
        ON t1.a = 1 AND t2.a = 1"#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn inner_join_eq_chain() {
    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2 ON true = true = true = (t1.a = t2.a)"#;
    let plan = sql_to_ir(query, vec![])
        .cast_constants()
        .unwrap()
        .fold_boolean_tree()
        .unwrap()
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);

    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2 ON (t1.a = t2.a) = true = true = true"#;
    let plan = sql_to_ir(query, vec![])
        .fold_boolean_tree()
        .unwrap()
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);

    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2 ON (t1.a = t2.a) in (true)"#;
    let plan = sql_to_ir(query, vec![])
        .replace_in_operator()
        .unwrap()
        .fold_boolean_tree()
        .unwrap()
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);

    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2 ON true = (t1.a = t2.a)"#;
    let plan = sql_to_ir(query, vec![])
        .fold_boolean_tree()
        .unwrap()
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);

    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2 ON (t1.a = t2.a) is true is true"#;
    let plan = sql_to_ir(query, vec![])
        .fold_boolean_tree()
        .unwrap()
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);

    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2 ON (t1.a = t2.a) = (1 = 1)"#;
    let plan = sql_to_ir(query, vec![])
        .fold_boolean_tree()
        .unwrap()
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);

    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2 ON true = (t1.a != t2.a)"#;
    let plan = sql_to_ir(query, vec![])
        .fold_boolean_tree()
        .unwrap()
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn inner_join_eq_true_on_bool_sharding_key() {
    let query = r#"SELECT * FROM bool_sharded AS t1 JOIN bool_sharded AS t2
        ON t1.b = true AND t2.b = true"#;
    let plan = sql_to_optimized_ir(query, vec![]);
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn inner_join_use_fact_eq_cols() {
    let query = r#"explain (logical) SELECT * FROM t5 AS t1
        INNER JOIN t5 AS t2
        ON t1.a < t2.a
        WHERE t1.a = t2.a"#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();

    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn inner_join_condition() {
    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2
    ON t1.a::text::datetime = to_date('2000-01-01', '%F')"#;
    sql_to_ir(query, vec![]).fold_boolean_tree().unwrap();

    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2
    ON t1.a::text = trim('hello')"#;
    sql_to_ir(query, vec![]).fold_boolean_tree().unwrap();

    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2
    ON t1.a = case when t2.a = 1 then 42 else 666 end"#;
    sql_to_ir(query, vec![]).fold_boolean_tree().unwrap();

    let query = r#"SELECT * FROM (SELECT 1) AS t1
    JOIN (SELECT 1) AS t2 ON (98 <> 1)"#;
    sql_to_ir(query, vec![]).fold_boolean_tree().unwrap();
}

#[test]
fn join_inner_sq_eq_for_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."id", "t2"."pc")"#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_sq_eq_for_keys_with_const() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", 1, "t1"."product_code") = ("t2"."id", 1, "t2"."pc")"#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_sq_less_for_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") < ("t2"."id", "t2"."pc")"#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
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
        ON ("t1"."product_code", '1') = ('1', "t2"."pc")"#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
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
        ON ("t1"."identification_number", '1') = ("t2"."id", "t2"."pc")"#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
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
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b"::text)
        AND ("t"."a", "t"."b") >=
        (SELECT "hash_testing"."sys_op", "hash_testing"."bucket_id" FROM "hash_testing")"#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
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
    let query = r#"SELECT * FROM "hash_testing2" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")
        AND ("t"."a", "t"."b") =
        (SELECT "hash_testing2"."identification_number", "hash_testing2"."product_code" FROM "hash_testing2")"#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn inner_join_local_policy_sq_with_union_all_in_filter() {
    let query = r#"SELECT * FROM "hash_testing2" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")
        AND ("t"."a", "t"."b") =
        (SELECT "hash_testing2"."identification_number", "hash_testing2"."product_code" FROM "hash_testing2"
        UNION ALL
        SELECT "hash_testing_hist2"."identification_number", "hash_testing_hist2"."product_code" FROM "hash_testing_hist2")"#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_and_local_full_policies() {
    // "hash_testing"      sharding_key = ("identification_number", "product_code")
    // "hash_testing_hist" sharding_key = the same
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."id", "t2"."pc")"#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_safe_subquery_filters_remove_motion_via_equality_facts() {
    let query = r#"SELECT *
        FROM (
            SELECT
                "t1"."identification_number" as "left_id",
                "t1"."product_code" as "left_pc",
                "t2"."identification_number" as "right_id",
                "t2"."product_code" as "right_pc"
            FROM "hash_testing" as "t1"
            JOIN "hash_testing_hist" as "t2" ON true
        ) as "sq"
        WHERE "sq"."left_id" = 1
          AND "sq"."left_pc" = '111'
          AND "sq"."right_id" = 1
          AND "sq"."right_pc" = '111'"#;

    let plan = sql_to_ir(query, vec![]).optimize().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_safe_subquery_multi_slot_fact_class_covers_composite_key() {
    let query = r#"SELECT * FROM
        (SELECT "identification_number" AS "id", "product_code" AS "pc"
         FROM "hash_testing2"
         WHERE "identification_number" = "product_code") AS "o"
        INNER JOIN
        (SELECT "identification_number" AS "id", "product_code" AS "pc"
         FROM "hash_testing_hist2"
         WHERE "identification_number" = "product_code") AS "i"
        ON "o"."id" = "i"."id""#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_safe_subquery_prefers_key_slot_over_output_order() {
    let query_x_first = r#"SELECT * FROM
        (SELECT "b" AS "x", "a" AS "k" FROM "t5" WHERE "a" = "b") AS "o"
        INNER JOIN "t5" AS "i"
        ON "o"."x" = "i"."a""#;
    let query_k_first = r#"SELECT * FROM
        (SELECT "a" AS "k", "b" AS "x" FROM "t5" WHERE "a" = "b") AS "o"
        INNER JOIN "t5" AS "i"
        ON "o"."x" = "i"."a""#;

    let x_first_plan = sql_to_ir(query_x_first, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    let k_first_plan = sql_to_ir(query_k_first, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();

    assert_eq!(Slices::empty(), x_first_plan.slices);
    assert_eq!(Slices::empty(), k_first_plan.slices);
}

#[test]
fn join_inner_chain_with_constants_in_different_on_clauses() {
    let query = r#"SELECT * FROM "hash_single_testing" AS "a"
        JOIN "hash_single_testing_hist" AS "b"
          ON "a"."product_code" = "b"."product_code"
         AND "a"."identification_number" = 1
         AND "b"."identification_number" = 1
        JOIN "hash_single_testing" AS "c"
          ON "b"."product_code" = "c"."product_code"
         AND "c"."identification_number" = 1"#;

    let plan = sql_to_optimized_ir(query, vec![]);
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_or_local_full_policies() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."id", "t2"."pc")
        OR "t1"."identification_number"::text = "t2"."pc""#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
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
    let query = r#"explain (logical) select e from (select t2.f from t2 join t2 t3 on true) join t2 on true"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r"
    projection (t2.e::int -> e)
      join on (true::bool)
        scan unnamed_subquery
          projection (t2.f::int -> f)
            join on (true::bool)
              scan t2
              motion [policy: full, program: ReshardIfNeeded]
                projection (t3.e::int -> e, t3.f::int -> f, t3.g::int -> g, t3.h::int -> h, t3.bucket_id::int -> bucket_id)
                  scan t2 -> t3
        motion [policy: full, program: ReshardIfNeeded]
          projection (t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t2.bucket_id::int -> bucket_id)
            scan t2
    ");

    // check both motions are in the same slice
    let t3_mid = *get_motion_id(&plan, 0, 0).unwrap();
    let t2_mid = *get_motion_id(&plan, 0, 1).unwrap();

    let slices = plan.calculate_slices(plan.get_top().unwrap()).unwrap();

    assert_eq!(slices, vec![vec![t3_mid, t2_mid]]);
}

#[test]
fn test_slices_2() {
    let query = r#"explain (logical) select count(e) from (select t2.f from t2 join t2 t3 on true) join t2 on true"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r"
    projection (sum(count_1::int)::int -> col_1)
      motion [policy: full, program: ReshardIfNeeded]
        projection (count(t2.e::int::int)::int -> count_1)
          join on (true::bool)
            scan unnamed_subquery
              projection (t2.f::int -> f)
                join on (true::bool)
                  scan t2
                  motion [policy: full, program: ReshardIfNeeded]
                    projection (t3.e::int -> e, t3.f::int -> f, t3.g::int -> g, t3.h::int -> h, t3.bucket_id::int -> bucket_id)
                      scan t2 -> t3
            motion [policy: full, program: ReshardIfNeeded]
              projection (t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t2.bucket_id::int -> bucket_id)
                scan t2
    ");

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

    plan.add_rel(Table::new_global(random(), "t", vec![Column::default()], &[""]).unwrap());

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

#[test]
fn prepared_partial_composite_key_keeps_full_motion_for_aggregate() {
    let query = r#"SELECT count(*)
        FROM "hash_testing"
        WHERE ("identification_number", "product_code") = ($1, trim("product_code"))"#;

    let plan = prepared_optimized_ir(query, &[DerivedType::new(UnrestrictedType::Integer)]);

    assert!(
        has_full_motion(&plan),
        "expected a full motion for aggregate prepared-path, got slices: {:?}",
        plan.slices
    );
}

#[test]
fn prepared_partial_composite_key_keeps_full_motion_for_order_limit() {
    let query = r#"SELECT "product_code"
        FROM "hash_testing"
        WHERE ("identification_number", "product_code") = ($1, trim("product_code"))
        ORDER BY "product_code"
        LIMIT 1"#;

    let plan = prepared_optimized_ir(query, &[DerivedType::new(UnrestrictedType::Integer)]);

    assert!(
        has_full_motion(&plan),
        "expected a full motion for ORDER BY / LIMIT prepared-path, got slices: {:?}",
        plan.slices
    );
}

#[test]
fn prepared_all_composite_sharding_key_columns_fixed_stays_single_node() {
    let query = r#"SELECT count(*)
        FROM "hash_testing"
        WHERE ("identification_number", "product_code") = ($1, '111')"#;

    let plan = prepared_optimized_ir(query, &[DerivedType::new(UnrestrictedType::Integer)]);

    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn prepared_single_sharding_key_aggregate_stays_single_node() {
    let query = r#"SELECT count(*)
        FROM "t5"
        WHERE "a" = $1"#;

    let plan = prepared_optimized_ir(query, &[DerivedType::new(UnrestrictedType::Integer)]);

    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn prepared_single_sharding_key_with_constant_keeps_full_motion_for_aggregate() {
    let query = r#"SELECT count(*)
        FROM "t5"
        WHERE "a" = $1 AND "a" = 1"#;

    let plan = prepared_optimized_ir(query, &[DerivedType::new(UnrestrictedType::Integer)]);

    assert!(
        has_full_motion(&plan),
        "expected a full motion for aggregate prepared-path, got slices: {:?}",
        plan.slices
    );
}

#[test]
fn prepared_reused_single_sharding_key_parameters_keep_full_motion_for_aggregate() {
    let query = r#"SELECT count(*)
        FROM "t5"
        WHERE "a" = $1 AND "a" = $1 AND "a" = $2"#;

    let plan = prepared_optimized_ir(
        query,
        &[
            DerivedType::new(UnrestrictedType::Integer),
            DerivedType::new(UnrestrictedType::Integer),
        ],
    );

    assert!(
        has_full_motion(&plan),
        "expected a full motion for aggregate prepared-path, got slices: {:?}",
        plan.slices
    );
}

#[test]
fn prepared_reused_placeholder_in_join_condition_localizes_composite_join() {
    let query = r#"SELECT *
        FROM "t" AS "l"
        INNER JOIN "t" AS "r"
        ON "l"."a" = $1
       AND "r"."a" = $1
       AND "l"."b" = "r"."b""#;

    let plan = prepared_optimized_ir(query, &[DerivedType::new(UnrestrictedType::Integer)]);

    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_chain_1() {
    let input = r#"explain (logical) SELECT * FROM "test_space" as "t1" JOIN "test_space" as "t2" ON "t1"."id" = 1 AND "t2"."id" = 1
    JOIN "test_space" as "t3" ON "t3"."id" = 1
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::string -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::string -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::string -> "FIRST_NAME", t3.sys_op::int -> sys_op)
      join on (t3.id::int = 1::int)
        join on ((t1.id::int = t2.id::int and ROW(t1.id::int, t2.id::int) = ROW(1::int, 1::int)))
          scan test_space -> t1
          scan test_space -> t2
        scan test_space -> t3
    "#);
}

#[test]
fn join_chain_2() {
    let input = r#"explain (logical) SELECT * FROM "test_space" as "t1" JOIN "test_space" as "t2" ON "t1"."sysFrom" = "t2"."sysFrom" AND "t1"."id" = 1 AND "t2"."id" = 1
    JOIN "test_space" as "t3" ON "t3"."sysFrom" = "t2"."sysFrom" AND "t3"."id" = 1
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::string -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::string -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::string -> "FIRST_NAME", t3.sys_op::int -> sys_op)
      join on ((t2."sysFrom"::int = t3."sysFrom"::int and t3.id::int = 1::int))
        join on ((ROW(t1.id::int, t1."sysFrom"::int) = ROW(t2.id::int, t2."sysFrom"::int) and ROW(t1.id::int, t2.id::int) = ROW(1::int, 1::int)))
          scan test_space -> t1
          scan test_space -> t2
        scan test_space -> t3
    "#);
}

#[test]
fn join_chain_3() {
    let input = r#"explain (logical) SELECT * FROM "test_space" as "t1" JOIN "test_space" as "t2" ON "t1"."sysFrom" = "t2"."sysFrom" AND "t1"."id" = 1 AND "t2"."id" = 1
    JOIN "test_space" as "t3" ON "t3"."sysFrom" = "t2"."sysFrom" AND "t3"."id" = 2
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::string -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::string -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::string -> "FIRST_NAME", t3.sys_op::int -> sys_op)
      join on ((t2."sysFrom"::int = t3."sysFrom"::int and t3.id::int = 2::int))
        join on ((ROW(t1.id::int, t1."sysFrom"::int) = ROW(t2.id::int, t2."sysFrom"::int) and ROW(t1.id::int, t2.id::int) = ROW(1::int, 1::int)))
          scan test_space -> t1
          scan test_space -> t2
        motion [policy: full, program: ReshardIfNeeded]
          projection (t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::string -> "FIRST_NAME", t3.sys_op::int -> sys_op, t3.bucket_id::int -> bucket_id)
            scan test_space -> t3
    "#);
}

#[test]
fn join_chain_4() {
    let input = r#"explain (logical) SELECT * FROM "test_space" as "t1" LEFT JOIN "test_space" as "t2" ON "t1"."id" = "t2"."id" AND "t2"."sysFrom" = 1
    JOIN "test_space" as "t3" ON "t3"."id" = "t2"."id" AND "t3"."sysFrom" = 1
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::string -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::string -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::string -> "FIRST_NAME", t3.sys_op::int -> sys_op)
      join on ((t2.id::int = t3.id::int and t3."sysFrom"::int = 1::int))
        left join on ((t1.id::int = t2.id::int and t2."sysFrom"::int = 1::int))
          scan test_space -> t1
          scan test_space -> t2
        scan test_space -> t3
    "#);
}

#[test]
fn join_chain_5() {
    let input = r#"explain (logical) SELECT * FROM "test_space" as "t1" JOIN "test_space" as "t2" ON "t1"."id" = "t2"."id" AND "t2"."sysFrom" = 1
    LEFT JOIN "test_space" as "t3" ON "t3"."id" = "t2"."id" AND "t3"."sysFrom" = 1
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::string -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::string -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::string -> "FIRST_NAME", t3.sys_op::int -> sys_op)
      left join on ((t2.id::int = t3.id::int and t3."sysFrom"::int = 1::int))
        join on ((t1.id::int = t2.id::int and t2."sysFrom"::int = 1::int))
          scan test_space -> t1
          scan test_space -> t2
        scan test_space -> t3
    "#);
}

#[test]
fn join_chain_6() {
    let input = r#"explain (logical) SELECT * FROM "test_space" as "t1" JOIN "test_space" as "t2" ON "t1"."id" = "t2"."id" AND "t2"."sysFrom" = 1
    LEFT JOIN "test_space" as "t3" ON "t3"."id" = "t2"."id" AND "t3"."sysFrom" = 1
    JOIN "test_space" as "t4" ON "t4"."id" = "t3"."id" AND "t4"."sysFrom" = 1
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::string -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::string -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::string -> "FIRST_NAME", t3.sys_op::int -> sys_op, t4.id::int -> id, t4."sysFrom"::int -> "sysFrom", t4."FIRST_NAME"::string -> "FIRST_NAME", t4.sys_op::int -> sys_op)
      join on ((t3.id::int = t4.id::int and t4."sysFrom"::int = 1::int))
        left join on ((t2.id::int = t3.id::int and t3."sysFrom"::int = 1::int))
          join on ((t1.id::int = t2.id::int and t2."sysFrom"::int = 1::int))
            scan test_space -> t1
            scan test_space -> t2
          scan test_space -> t3
        scan test_space -> t4
    "#);
}

#[test]
fn join_chain_7() {
    let input = r#"explain (logical) SELECT * FROM "test_space" as "t1" LEFT JOIN "test_space" as "t2" ON "t1"."id" = "t2"."id" AND "t2"."sysFrom" = 1
    JOIN "test_space" as "t3" ON "t3"."id" = "t2"."id" AND "t3"."sysFrom" = 1
    JOIN "test_space" as "t4" ON "t4"."id" = "t3"."id" AND "t4"."sysFrom" = 1
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::string -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::string -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::string -> "FIRST_NAME", t3.sys_op::int -> sys_op, t4.id::int -> id, t4."sysFrom"::int -> "sysFrom", t4."FIRST_NAME"::string -> "FIRST_NAME", t4.sys_op::int -> sys_op)
      join on ((t3.id::int = t4.id::int and t4."sysFrom"::int = 1::int))
        join on ((t2.id::int = t3.id::int and t3."sysFrom"::int = 1::int))
          left join on ((t1.id::int = t2.id::int and t2."sysFrom"::int = 1::int))
            scan test_space -> t1
            scan test_space -> t2
          scan test_space -> t3
        scan test_space -> t4
    "#);
}

#[test]
fn join_chain_8() {
    let input = r#"explain (logical) SELECT * FROM "test_space" as "t1" JOIN "test_space" as "t2" ON "t1"."id" = "t2"."id" AND "t2"."sysFrom" = 1
    JOIN "test_space" as "t3" ON "t3"."id" = "t2"."id" AND "t3"."sysFrom" = 1
    LEFT JOIN "test_space" as "t4" ON "t4"."id" = "t3"."id" AND "t4"."sysFrom" = 1
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::string -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::string -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::string -> "FIRST_NAME", t3.sys_op::int -> sys_op, t4.id::int -> id, t4."sysFrom"::int -> "sysFrom", t4."FIRST_NAME"::string -> "FIRST_NAME", t4.sys_op::int -> sys_op)
      left join on ((t3.id::int = t4.id::int and t4."sysFrom"::int = 1::int))
        join on ((t2.id::int = t3.id::int and t3."sysFrom"::int = 1::int))
          join on ((t1.id::int = t2.id::int and t2."sysFrom"::int = 1::int))
            scan test_space -> t1
            scan test_space -> t2
          scan test_space -> t3
        scan test_space -> t4
    "#);
}

#[test]
fn join_chain_9() {
    let input = r#"explain (logical) SELECT * FROM "test_space" as "t1" LEFT JOIN "test_space" as "t2" ON "t1"."id" = "t2"."id" AND "t2"."sysFrom" = 1
    LEFT JOIN "test_space" as "t3" ON "t3"."id" = "t2"."id" AND "t3"."sysFrom" = 1
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::string -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::string -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::string -> "FIRST_NAME", t3.sys_op::int -> sys_op)
      left join on ((t2.id::int = t3.id::int and t3."sysFrom"::int = 1::int))
        left join on ((t1.id::int = t2.id::int and t2."sysFrom"::int = 1::int))
          scan test_space -> t1
          scan test_space -> t2
        scan test_space -> t3
    "#);
}

/// `LEFT JOIN` sandwiched between one `INNER JOIN` below and two
/// `INNER JOIN`s above.  All five tables share the same sharding column,
/// and every join condition aligns it across sides, so the whole chain
/// must stay co-located — no `Motion` on any subtree.
#[test]
fn join_chain_10() {
    let input = r#"explain (logical) SELECT * FROM "test_space" as "t1" JOIN "test_space" as "t2" ON "t1"."id" = "t2"."id" AND "t2"."sysFrom" = 1
    LEFT JOIN "test_space" as "t3" ON "t3"."id" = "t2"."id" AND "t3"."sysFrom" = 1
    JOIN "test_space" as "t4" ON "t4"."id" = "t3"."id" AND "t4"."sysFrom" = 1
    JOIN "test_space" as "t5" ON "t5"."id" = "t4"."id" AND "t5"."sysFrom" = 1
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::string -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::string -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::string -> "FIRST_NAME", t3.sys_op::int -> sys_op, t4.id::int -> id, t4."sysFrom"::int -> "sysFrom", t4."FIRST_NAME"::string -> "FIRST_NAME", t4.sys_op::int -> sys_op, t5.id::int -> id, t5."sysFrom"::int -> "sysFrom", t5."FIRST_NAME"::string -> "FIRST_NAME", t5.sys_op::int -> sys_op)
      join on ((t4.id::int = t5.id::int and t5."sysFrom"::int = 1::int))
        join on ((t3.id::int = t4.id::int and t4."sysFrom"::int = 1::int))
          left join on ((t2.id::int = t3.id::int and t3."sysFrom"::int = 1::int))
            join on ((t1.id::int = t2.id::int and t2."sysFrom"::int = 1::int))
              scan test_space -> t1
              scan test_space -> t2
            scan test_space -> t3
          scan test_space -> t4
        scan test_space -> t5
    "#);
}

/// `JOIN ON true` between two sharded tables, with explicit `bucket_id`
/// equality in WHERE.  This exercises the
/// `bucket_id_co_located_via_facts` path in motion planning: with the
/// sharding-column class established via the fact, no Motion should be
/// inserted on the inner side.
#[test]
fn join_bucket_id_eq_in_where() {
    let input = r#"explain (logical) SELECT * FROM "test_space" AS "t1"
    JOIN "test_space" AS "t2" ON true
    WHERE "t1"."bucket_id" = "t2"."bucket_id"
"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::string -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::string -> "FIRST_NAME", t2.sys_op::int -> sys_op)
      selection (t1.bucket_id::int = t2.bucket_id::int)
        join on (true::bool)
          scan test_space -> t1
          scan test_space -> t2
    "#);
}

/// Same as above but the `bucket_id = bucket_id` equality is in the
/// INNER JOIN's ON instead of WHERE.  For Inner joins ON-facts flow
/// into the global UF just like WHERE, so the result should match.
#[test]
fn join_bucket_id_eq_in_inner_on() {
    let input = r#"explain (logical) SELECT * FROM "test_space" AS "t1"
    JOIN "test_space" AS "t2" ON "t1"."bucket_id" = "t2"."bucket_id"
"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::string -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::string -> "FIRST_NAME", t2.sys_op::int -> sys_op)
      join on (t1.bucket_id::int = t2.bucket_id::int)
        scan test_space -> t1
        scan test_space -> t2
    "#);
}

/// LEFT JOIN with `bucket_id` equality in ON.  The cross-side equality
/// is scope-only (LEFT JOIN), but motion planning queries the scope
/// when deciding the inner-child policy — so co-location is still
/// detected at the LJ level.
#[test]
fn left_join_bucket_id_eq_in_on() {
    let input = r#"explain (logical) SELECT * FROM "test_space" AS "t1"
    LEFT JOIN "test_space" AS "t2" ON "t1"."bucket_id" = "t2"."bucket_id"
"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::string -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::string -> "FIRST_NAME", t2.sys_op::int -> sys_op)
      left join on (t1.bucket_id::int = t2.bucket_id::int)
        scan test_space -> t1
        scan test_space -> t2
    "#);
}

/// LEFT JOIN counterpart of `inner_join_eq_via_shared_const_in_on`.
/// Both sides' sharding column pinned to the same constant inside the
/// LJ's ON.  The equality stays scope-only (LEFT JOIN), but motion
/// planning at the LJ NodeId consults `ResolvedScope`'s constant
/// merging — both `t1.a` and `t2.a` resolve to `1` in the scope, so
/// Motion is dropped.
#[test]
fn left_join_eq_via_shared_const_in_on() {
    let query = r#"SELECT * FROM t5 AS t1
        LEFT JOIN t5 AS t2
        ON t1.a = 1 AND t2.a = 1"#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

/// LEFT JOIN counterpart of `inner_join_eq_for_keys`.  Composite
/// row-equality `(t1.id, t1.code) = (t.a, t.b)` in ON.  Each pair
/// becomes a separate scope-class merge; `find_equal_position` walks
/// these per-position and the resulting Key([0, 1]) matches the outer
/// sharding key — Motion is dropped.
#[test]
fn left_join_eq_for_keys() {
    let query = r#"SELECT * FROM "hash_testing2" AS "t1"
        LEFT JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")"#;

    let plan = sql_to_ir(query, vec![])
        .analyze_equality_facts()
        .unwrap()
        .add_motions()
        .unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

/// INNER JOIN above a LEFT JOIN where the upper ON
/// references the LEFT JOIN's nullable-side column.
#[test]
fn inner_above_left_join_diagnostic() {
    let input = r#"explain (logical) SELECT * FROM "test_space" AS "t1"
    LEFT JOIN "test_space" AS "t2" ON "t1"."id" = "t2"."id"
    INNER JOIN "test_space" AS "t3" ON "t2"."id" = "t3"."id"
"#;

    // TODO: ideally get rid of left join here. because nullable part of t2.id will be filtered by
    // next inner join
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::string -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::string -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::string -> "FIRST_NAME", t3.sys_op::int -> sys_op)
      join on (t2.id::int = t3.id::int)
        left join on (t1.id::int = t2.id::int)
          scan test_space -> t1
          scan test_space -> t2
        scan test_space -> t3
    "#);
}

mod between;
mod except;
mod not_in;
mod segment;
mod window;
