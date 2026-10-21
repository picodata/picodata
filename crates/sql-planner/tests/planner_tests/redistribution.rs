use pretty_assertions::assert_eq;
use rand::random;
use sql::explain::explain_logical;
use sql::helpers::sql_to_ir;
use sql::helpers::sql_to_ir_without_bind;
use sql::helpers::sql_to_optimized_ir;
use sql::ir::node::relational::Relational;
use sql::ir::node::Motion;
use sql::ir::node::NodeId;
use sql::ir::relation::Column;
use sql::ir::relation::Table;
use sql::ir::transformation::redistribution::*;
use sql::ir::types::{DerivedType, UnrestrictedType};
use sql::ir::Plan;
use sql::ir::Slices;

fn prepared_optimized_ir(query: &str, params: &[DerivedType]) -> Plan {
    sql_to_ir_without_bind(query, params).optimize().unwrap()
}

/// Build the input `add_motions` sees in production (via `optimize_before`),
/// then run `add_motions` itself and return the plan for motion inspection.
/// Replaces the ad-hoc per-test pass chains that each hand-picked a subset of
/// passes.
fn optimized_to_motions(query: &str) -> Plan {
    let plan = sql_to_ir(query, vec![]);
    let top = plan.get_top().unwrap();
    plan.optimize_before(top, Stage::AddMotions)
        .unwrap()
        .add_motions()
        .unwrap()
}

fn has_full_motion(plan: &Plan) -> bool {
    plan.slices()
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

    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());
}

#[test]
fn inner_join_eq_for_keys() {
    let query = r#"SELECT * FROM "hash_testing2" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")"#;

    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());
}

#[test]
fn inner_join_eq_via_shared_const_in_on() {
    let query = r#"SELECT * FROM t5 AS t1
        INNER JOIN t5 AS t2
        ON t1.a = 1 AND t2.a = 1"#;

    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());
}

#[test]
fn inner_join_eq_chain() {
    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2 ON true = true = true = (t1.a = t2.a)"#;
    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());

    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2 ON (t1.a = t2.a) = true = true = true"#;
    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());

    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2 ON (t1.a = t2.a) in (true)"#;
    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());

    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2 ON true = (t1.a = t2.a)"#;
    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());

    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2 ON (t1.a = t2.a) is true is true"#;
    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());

    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2 ON (t1.a = t2.a) = (1 = 1)"#;
    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());

    let query = r#"SELECT * FROM t5 AS t1 JOIN t5 AS t2 ON true = (t1.a != t2.a)"#;
    let plan = optimized_to_motions(query);
    let motion_id = *plan.slices().slice(0).unwrap().position(0).unwrap();
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
    assert_eq!(Slices::empty(), *plan.slices());
}

#[test]
fn inner_join_use_fact_eq_cols() {
    let query = r#"explain (logical) SELECT * FROM t5 AS t1
        INNER JOIN t5 AS t2
        ON t1.a < t2.a
        WHERE t1.a = t2.a"#;

    let plan = optimized_to_motions(query);

    assert_eq!(Slices::empty(), *plan.slices());
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

    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());
}

#[test]
fn join_inner_sq_eq_for_keys_with_const() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", 1, "t1"."product_code") = ("t2"."id", 1, "t2"."pc")"#;

    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());
}

#[test]
fn join_inner_sq_less_for_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") < ("t2"."id", "t2"."pc")"#;

    let plan = optimized_to_motions(query);
    let motion_id = *plan.slices().slice(0).unwrap().position(0).unwrap();
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

    let plan = optimized_to_motions(query);
    let motion_id = *plan.slices().slice(0).unwrap().position(0).unwrap();
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

    let plan = optimized_to_motions(query);
    let motion_id = *plan.slices().slice(0).unwrap().position(0).unwrap();
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

    let plan = optimized_to_motions(query);
    let motion_id = *plan.slices().slice(0).unwrap().position(0).unwrap();
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

    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());
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

    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());
}

#[test]
fn join_inner_and_local_full_policies() {
    // "hash_testing"      sharding_key = ("identification_number", "product_code")
    // "hash_testing_hist" sharding_key = the same
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."id", "t2"."pc")"#;

    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());
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
    assert_eq!(Slices::empty(), *plan.slices());
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

    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());
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

    let x_first_plan = optimized_to_motions(query_x_first);
    let k_first_plan = optimized_to_motions(query_k_first);

    assert_eq!(Slices::empty(), *x_first_plan.slices());
    assert_eq!(Slices::empty(), *k_first_plan.slices());
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
    assert_eq!(Slices::empty(), *plan.slices());
}

#[test]
fn join_inner_or_local_full_policies() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."id", "t2"."pc")
        OR "t1"."identification_number"::text = "t2"."pc""#;

    let plan = optimized_to_motions(query);
    let motion_id = *plan.slices().slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion(Motion { policy, .. }) = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
}

pub use sql::ir::test_fixtures::get_motion_id;

#[test]
fn test_slices_1() {
    let query = r#"explain (logical) select e from (select t2.f from t2 join t2 t3 on true) join t2 on true"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
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

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @"
    projection (sum(count_1::int::int)::int::int -> col_1)
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
        plan.slices()
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
        plan.slices()
    );
}

#[test]
fn prepared_all_composite_sharding_key_columns_fixed_stays_single_node() {
    let query = r#"SELECT count(*)
        FROM "hash_testing"
        WHERE ("identification_number", "product_code") = ($1, '111')"#;

    let plan = prepared_optimized_ir(query, &[DerivedType::new(UnrestrictedType::Integer)]);

    assert_eq!(Slices::empty(), *plan.slices());
}

#[test]
fn prepared_single_sharding_key_aggregate_stays_single_node() {
    let query = r#"SELECT count(*)
        FROM "t5"
        WHERE "a" = $1"#;

    let plan = prepared_optimized_ir(query, &[DerivedType::new(UnrestrictedType::Integer)]);

    assert_eq!(Slices::empty(), *plan.slices());
}

#[test]
fn explicit_bucket_id_constant_stays_single_node_for_order_by() {
    let query = r#"SELECT "id"
        FROM "test_space"
        WHERE "bucket_id" = 42
        ORDER BY "id""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    assert_eq!(*plan.slices(), Slices::empty());
}

#[test]
fn explicit_bucket_id_constant_stays_single_node_for_limit() {
    let query = r#"SELECT "id"
        FROM "test_space"
        WHERE "bucket_id" = 42
        LIMIT 1"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    assert_eq!(*plan.slices(), Slices::empty());
}

#[test]
fn explicit_bucket_id_anded_with_unrelated_filter_stays_single_node() {
    let query = r#"SELECT "id"
        FROM "test_space"
        WHERE "bucket_id" = 42 AND "sys_op" = 1
        ORDER BY "id""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    assert_eq!(*plan.slices(), Slices::empty());
}

#[test]
fn explicit_bucket_id_anded_with_sharding_key_stays_single_node() {
    let query = r#"SELECT "id"
        FROM "test_space"
        WHERE "bucket_id" = 42 AND "id" = 1
        ORDER BY "id""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    assert_eq!(*plan.slices(), Slices::empty());
}

#[test]
fn explicit_bucket_id_zero_stays_single_node_for_order_by() {
    let query = r#"SELECT "id"
        FROM "test_space"
        WHERE "bucket_id" = 0
        ORDER BY "id""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    assert_eq!(*plan.slices(), Slices::empty());
}

#[test]
fn explicit_bucket_id_above_motion_keeps_full_motion() {
    let query = r#"SELECT "t2"."e"
        FROM (SELECT "id" FROM "test_space") AS "o"
        JOIN "t2" ON "o"."id" = "t2"."f"
        WHERE "t2"."bucket_id" = 42
        ORDER BY "t2"."e""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    assert!(
        has_full_motion(&plan),
        "expected a full motion: the inner rows were resharded, so their \
         bucket_id no longer locates them, got slices: {:?}",
        plan.slices()
    );
}

#[test]
fn explicit_bucket_id_stays_single_node_for_aggregate() {
    let query = r#"SELECT count(*)
        FROM "test_space"
        WHERE "bucket_id" = 42"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    assert_eq!(*plan.slices(), Slices::empty());
}

#[test]
fn sharding_key_without_bucket_id_stays_single_node_for_aggregate() {
    let query = r#"SELECT count(*)
        FROM "test_space"
        WHERE "id" = 1"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    assert_eq!(*plan.slices(), Slices::empty());
}

#[test]
fn explicit_bucket_id_ored_with_sharding_key_keeps_full_motion() {
    let query = r#"SELECT "id"
        FROM "test_space"
        WHERE ("bucket_id" = 42 AND "id" = 1) OR "bucket_id" = 43
        ORDER BY "id""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    assert!(
        has_full_motion(&plan),
        "expected a full motion: the two OR arms may resolve to different \
         buckets, got slices: {:?}",
        plan.slices()
    );
}

#[test]
fn explicit_bucket_id_parameter_stays_single_node() {
    let query = r#"SELECT "id"
        FROM "test_space"
        WHERE "bucket_id" = $1
        ORDER BY "id""#;

    let plan = prepared_optimized_ir(query, &[DerivedType::new(UnrestrictedType::Integer)]);

    assert_eq!(*plan.slices(), Slices::empty());
}

#[test]
fn explicit_bucket_id_parameter_stays_single_node_for_aggregate() {
    let query = r#"SELECT count(*)
        FROM "test_space"
        WHERE "bucket_id" = $1"#;

    let plan = prepared_optimized_ir(query, &[DerivedType::new(UnrestrictedType::Integer)]);

    assert_eq!(*plan.slices(), Slices::empty());
}

#[test]
fn explicit_bucket_id_decimal_parameter_stays_single_node_for_aggregate() {
    let query = r#"SELECT count(*)
        FROM "test_space"
        WHERE "bucket_id" = $1"#;

    let plan = prepared_optimized_ir(query, &[DerivedType::new(UnrestrictedType::Decimal)]);

    assert_eq!(*plan.slices(), Slices::empty());
}

#[test]
fn explicit_bucket_id_non_integer_constant_stays_single_node_for_aggregate() {
    let query = r#"SELECT count(*)
        FROM "test_space"
        WHERE "bucket_id" = 42.5"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    assert_eq!(*plan.slices(), Slices::empty());
}

#[test]
fn explicit_bucket_id_ored_with_unrelated_filter_keeps_full_motion() {
    let query = r#"SELECT "id"
        FROM "test_space"
        WHERE "bucket_id" = 42 OR "sys_op" > 5
        ORDER BY "id""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    assert!(
        has_full_motion(&plan),
        "expected a full motion when the OR arm is unrestricted, got slices: {:?}",
        plan.slices()
    );
}

#[test]
fn prepared_single_sharding_key_with_constant_stays_single_node_for_aggregate() {
    let query = r#"SELECT count(*)
        FROM "t5"
        WHERE "a" = $1 AND "a" = 1"#;

    let plan = prepared_optimized_ir(query, &[DerivedType::new(UnrestrictedType::Integer)]);

    assert_eq!(*plan.slices(), Slices::empty());
}

#[test]
fn prepared_reused_single_sharding_key_parameters_stay_single_node_for_aggregate() {
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

    assert_eq!(*plan.slices(), Slices::empty());
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

    assert_eq!(Slices::empty(), *plan.slices());
}

#[test]
fn join_chain_1() {
    let input = r#"explain (logical) SELECT * FROM "test_space" as "t1" JOIN "test_space" as "t2" ON "t1"."id" = 1 AND "t2"."id" = 1
    JOIN "test_space" as "t3" ON "t3"."id" = 1
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::text -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::text -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::text -> "FIRST_NAME", t3.sys_op::int -> sys_op)
      join on (t3.id::int = 1::int)
        join on (ROW(t1.id::int, t2.id::int) = ROW(1::int, 1::int))
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

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::text -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::text -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::text -> "FIRST_NAME", t3.sys_op::int -> sys_op)
      join on ((t2."sysFrom"::int = t3."sysFrom"::int and t3.id::int = 1::int))
        join on ((t1."sysFrom"::int = t2."sysFrom"::int and ROW(t2.id::int, t1.id::int) = ROW(1::int, 1::int)))
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

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::text -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::text -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::text -> "FIRST_NAME", t3.sys_op::int -> sys_op)
      join on ((t2."sysFrom"::int = t3."sysFrom"::int and t3.id::int = 2::int))
        join on ((t1."sysFrom"::int = t2."sysFrom"::int and ROW(t2.id::int, t1.id::int) = ROW(1::int, 1::int)))
          scan test_space -> t1
          scan test_space -> t2
        motion [policy: full, program: ReshardIfNeeded]
          projection (t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::text -> "FIRST_NAME", t3.sys_op::int -> sys_op, t3.bucket_id::int -> bucket_id)
            scan test_space -> t3
    "#);
}

#[test]
fn join_chain_4() {
    let input = r#"explain (logical) SELECT * FROM "test_space" as "t1" LEFT JOIN "test_space" as "t2" ON "t1"."id" = "t2"."id" AND "t2"."sysFrom" = 1
    JOIN "test_space" as "t3" ON "t3"."id" = "t2"."id" AND "t3"."sysFrom" = 1
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::text -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::text -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::text -> "FIRST_NAME", t3.sys_op::int -> sys_op)
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

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::text -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::text -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::text -> "FIRST_NAME", t3.sys_op::int -> sys_op)
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

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::text -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::text -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::text -> "FIRST_NAME", t3.sys_op::int -> sys_op, t4.id::int -> id, t4."sysFrom"::int -> "sysFrom", t4."FIRST_NAME"::text -> "FIRST_NAME", t4.sys_op::int -> sys_op)
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

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::text -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::text -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::text -> "FIRST_NAME", t3.sys_op::int -> sys_op, t4.id::int -> id, t4."sysFrom"::int -> "sysFrom", t4."FIRST_NAME"::text -> "FIRST_NAME", t4.sys_op::int -> sys_op)
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

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::text -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::text -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::text -> "FIRST_NAME", t3.sys_op::int -> sys_op, t4.id::int -> id, t4."sysFrom"::int -> "sysFrom", t4."FIRST_NAME"::text -> "FIRST_NAME", t4.sys_op::int -> sys_op)
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

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::text -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::text -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::text -> "FIRST_NAME", t3.sys_op::int -> sys_op)
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

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::text -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::text -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::text -> "FIRST_NAME", t3.sys_op::int -> sys_op, t4.id::int -> id, t4."sysFrom"::int -> "sysFrom", t4."FIRST_NAME"::text -> "FIRST_NAME", t4.sys_op::int -> sys_op, t5.id::int -> id, t5."sysFrom"::int -> "sysFrom", t5."FIRST_NAME"::text -> "FIRST_NAME", t5.sys_op::int -> sys_op)
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
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::text -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::text -> "FIRST_NAME", t2.sys_op::int -> sys_op)
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
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::text -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::text -> "FIRST_NAME", t2.sys_op::int -> sys_op)
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
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::text -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::text -> "FIRST_NAME", t2.sys_op::int -> sys_op)
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

    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());
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

    let plan = optimized_to_motions(query);
    assert_eq!(Slices::empty(), *plan.slices());
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
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    projection (t1.id::int -> id, t1."sysFrom"::int -> "sysFrom", t1."FIRST_NAME"::text -> "FIRST_NAME", t1.sys_op::int -> sys_op, t2.id::int -> id, t2."sysFrom"::int -> "sysFrom", t2."FIRST_NAME"::text -> "FIRST_NAME", t2.sys_op::int -> sys_op, t3.id::int -> id, t3."sysFrom"::int -> "sysFrom", t3."FIRST_NAME"::text -> "FIRST_NAME", t3.sys_op::int -> sys_op)
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

/// Enrichment may add a routing predicate that safely narrows these queries.
/// Their results with either setting are covered by integration tests.
#[cfg(not(feature = "enrich_restrictions"))]
#[test]
fn static_bucket_proofs_keep_reduce_for_colliding_contradictions() {
    let inputs = [
        "SELECT a FROM t5 WHERE (a = 1 AND a = 10208) OR a = 2",
        "SELECT a FROM t5 WHERE a = 1 AND a = 10208 \
         UNION ALL SELECT a FROM t5 WHERE a = 2",
        "SELECT l.a FROM (SELECT a FROM t5 WHERE a = 1 AND a = 10208) l \
         JOIN (SELECT a FROM t5 WHERE a = 2) r ON l.a = r.a",
    ];
    for input in inputs {
        let query = format!("EXPLAIN (LOGICAL) SELECT count(*) FROM ({input}) u");
        let plan = sql_to_optimized_ir(&query, vec![]);
        let explain = explain_logical(&plan).unwrap();
        assert!(explain.contains("sum(count_"), "{query}\n{explain}");
    }
}

#[test]
fn static_bucket_proofs_preserve_shared_keys() {
    let inputs = [
        "SELECT a FROM t5 WHERE (a = 1 AND a = 10208) OR a = 1",
        "SELECT a FROM t5 WHERE a = 1 UNION ALL SELECT a FROM t5 WHERE a = 1",
        "SELECT a FROM t WHERE (a, b) = (1, 2) \
         UNION ALL SELECT a FROM t WHERE (a, b) = (1, 2)",
        "SELECT l.a FROM (SELECT a, b FROM t WHERE (a, b) = (1, 2)) l \
         JOIN (SELECT a, b FROM t WHERE (a, b) = (1, 2)) r \
         ON (l.a, l.b) = (r.a, r.b)",
    ];
    for input in inputs {
        let query = format!("EXPLAIN (LOGICAL) SELECT count(*) FROM ({input}) u");
        let plan = sql_to_optimized_ir(&query, vec![]);
        assert_eq!(*plan.slices(), Slices::empty(), "{query}");
    }
}

/// The full motion under the join is Global: it must not hide the CTE's key.
#[test]
fn static_bucket_proofs_keep_key_across_full_motion_join() {
    let query = "EXPLAIN (LOGICAL) WITH cte AS (SELECT a, b FROM t5 WHERE a = 1) \
                 SELECT c1.a FROM cte c1 JOIN cte c2 ON c1.b = c2.b \
                 ORDER BY c1.a LIMIT 1";
    let plan = sql_to_optimized_ir(query, vec![]);
    let explain = explain_logical(&plan).unwrap();
    assert_eq!(explain.matches("motion [").count(), 1, "{explain}");
}

#[test]
fn static_bucket_proofs_isolate_empty_global_join_aggregates() {
    for filter in ["false", "a = 1 AND a = 10208"] {
        let query = format!(
            "EXPLAIN (LOGICAL) SELECT count(*) FROM (SELECT a FROM t5 WHERE {filter}) l \
             JOIN global_t g ON l.a = g.b \
             UNION ALL SELECT a FROM t5 WHERE a = 2"
        );
        let plan = sql_to_optimized_ir(&query, vec![]);
        let explain = explain_logical(&plan).unwrap();
        assert!(explain.contains("motion [policy: segment"), "{explain}");
    }
}

#[test]
fn static_bucket_proofs_do_not_assume_parameters_stay_equal() {
    use sql::ir::options::Options;
    use sql::ir::value::Value;

    let prepared = prepared_optimized_ir(
        "EXPLAIN (LOGICAL) SELECT count(*) FROM t5 WHERE a = $1 OR a = $2",
        &[DerivedType::unknown(); 2],
    );
    for values in [vec![1, 1], vec![1, 2]] {
        let bound = prepared
            .clone()
            .bind_statement(
                values.into_iter().map(Value::from).collect(),
                Options::default(),
            )
            .unwrap();
        let explain = explain_logical(&bound).unwrap();
        assert!(explain.contains("sum(count_"), "{explain}");
    }
}

/// With GROUP BY an empty input yields no groups, so a single-shard input
/// keeps skipping the reduce stage, also behind an inner join whose both sides
/// are on that shard.
#[test]
fn grouped_aggregate_on_single_shard_behind_join_stays_single_node() {
    let query = r#"SELECT l."b", count(*) FROM (SELECT "a", "b" FROM "t5" WHERE "a" = 1) AS l
        JOIN (SELECT "a" FROM "t5" WHERE "a" = 1) AS r ON l."a" = r."a"
        GROUP BY l."b""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    assert_eq!(*plan.slices(), Slices::empty());
}

/// Both sides of the inner join are on the same single shard.
#[test]
fn inner_join_on_same_shard_stays_single_node() {
    let query = r#"SELECT count(*) FROM (SELECT "a" FROM "t5" WHERE "a" = 1) AS l
        JOIN (SELECT "a" FROM "t5" WHERE "a" = 1) AS r ON l."a" = r."a""#;
    let plan = sql_to_optimized_ir(query, vec![]);
    assert_eq!(*plan.slices(), Slices::empty());
}

/// An inner join runs on the union of its inputs' buckets: restricting one side
/// does not pin it.
/// Enrichment derives `l.a = 1`, pinning both sides: then no reduce is right.
#[cfg(not(feature = "enrich_restrictions"))]
#[test]
fn inner_join_with_one_restricted_side_keeps_reduce() {
    let query = r#"explain (logical) SELECT count(*) FROM "t5" AS l
        JOIN (SELECT "a" FROM "t5" WHERE "a" = 1) AS r ON l."a" = r."a""#;
    let plan = explain_logical(&sql_to_optimized_ir(query, vec![])).unwrap();
    assert!(plan.contains("sum(count_"), "{plan}");
}

/// A global table is on every node, so an inner join with it stays on the other
/// side's single shard; with that side unrestricted the reduce stays.
#[test]
fn inner_join_with_global_side_follows_other_side() {
    let single = [
        r#"SELECT count(*) FROM (SELECT "a" FROM "t5" WHERE "a" = 1) AS l
        JOIN "global_t" AS g ON l."a" = g."b""#,
        r#"SELECT count(*) FROM "global_t" AS g
        JOIN (SELECT "a" FROM "t5" WHERE "a" = 1) AS l ON l."a" = g."b""#,
    ];
    for query in single {
        let plan = sql_to_optimized_ir(query, vec![]);
        assert_eq!(*plan.slices(), Slices::empty(), "{query}");
    }
    let query = r#"explain (logical) SELECT count(*) FROM "t5" AS l
        JOIN "global_t" AS g ON l."a" = g."b""#;
    let plan = explain_logical(&sql_to_optimized_ir(query, vec![])).unwrap();
    assert!(plan.contains("sum(count_"), "{plan}");
}

/// Different sharding keys do not prove that both inputs use the same shard.
/// Keep the reduce stage: if they use different shards, a local `count(*)`
/// would emit a separate zero on each. Enrichment is disabled here because
/// it turns `a = 1 and a = 2` into `false`.
#[cfg(not(feature = "enrich_restrictions"))]
#[test]
fn inner_join_with_differently_restricted_sides_keeps_reduce() {
    let query = r#"explain (logical) SELECT count(*) FROM (SELECT "a" FROM "t5" WHERE "a" = 1) AS l
        JOIN (SELECT "a" FROM "t5" WHERE "a" = 2) AS r ON l."a" = r."a""#;
    let plan = explain_logical(&sql_to_optimized_ir(query, vec![])).unwrap();
    assert!(plan.contains("sum(count_"), "{plan}");
}

/// A one-stage scalar aggregate must run on exactly one node, even when its
/// input is empty. UNION ALL dispatches the statement to the shards of both
/// sides, so the branch with the one-stage aggregate gets a stage of its own:
/// a motion over it, whose buckets are discovered from `a = 1`. Without it the
/// shard with `a = 4` returns an extra `count = 0` row.
#[test]
fn scalar_aggregate_on_single_shard_under_union_all() {
    let query = r#"explain (logical) SELECT count(*) AS n FROM "t5" WHERE "a" = 1
        UNION ALL SELECT "a" FROM "t5" WHERE "a" = 4"#;
    let plan = sql_to_optimized_ir(query, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    union all
      motion [policy: segment([ref(n)]), program: ReshardIfNeeded]
        projection (count(*)::int -> n)
          selection (t5.a::int = 1::int)
            scan t5
      projection (t5.a::int -> a)
        selection (t5.a::int = 4::int)
          scan t5
    ");
}

/// Same as `scalar_aggregate_on_single_shard_under_union_all`, with the
/// single-shard proof coming from an inner join of two sides on that shard.
#[test]
fn scalar_aggregate_on_single_shard_join_under_union_all() {
    let query = r#"explain (logical) SELECT count(*) AS n
        FROM (SELECT "a" FROM "t5" WHERE "a" = 1) AS l
        JOIN (SELECT "a" FROM "t5" WHERE "a" = 1) AS r ON l."a" = r."a"
        UNION ALL SELECT "a" FROM "t5" WHERE "a" = 4"#;
    let plan = sql_to_optimized_ir(query, vec![]);
    #[cfg(not(feature = "enrich_restrictions"))]
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    union all
      motion [policy: segment([ref(n)]), program: ReshardIfNeeded]
        projection (count(*)::int -> n)
          join on (l.a::int = r.a::int)
            scan l
              projection (t5.a::int -> a)
                selection (t5.a::int = 1::int)
                  scan t5
            scan r
              projection (t5.a::int -> a)
                selection (t5.a::int = 1::int)
                  scan t5
      projection (t5.a::int -> a)
        selection (t5.a::int = 4::int)
          scan t5
    ");
    // Enrichment folds the join condition implied by both filters.
    #[cfg(feature = "enrich_restrictions")]
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    union all
      motion [policy: segment([ref(n)]), program: ReshardIfNeeded]
        projection (count(*)::int -> n)
          join on (true::bool)
            scan l
              projection (t5.a::int -> a)
                selection (t5.a::int = 1::int)
                  scan t5
            scan r
              projection (t5.a::int -> a)
                selection (t5.a::int = 1::int)
                  scan t5
      projection (t5.a::int -> a)
        selection (t5.a::int = 4::int)
          scan t5
    ");
}

/// The inner UNION ALL keeps both branches on `a = 1` and needs no motion.
/// The outer one widens the shards, so it isolates the whole inner branch.
#[test]
fn scalar_aggregate_on_single_shard_under_nested_union_all() {
    let query = r#"explain (logical) SELECT count(*) AS n FROM "t5" WHERE "a" = 1
        UNION ALL SELECT "a" FROM "t5" WHERE "a" = 1
        UNION ALL SELECT "a" FROM "t5" WHERE "a" <> 1"#;
    let plan = sql_to_optimized_ir(query, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    union all
      motion [policy: segment([ref(n)]), program: ReshardIfNeeded]
        union all
          projection (count(*)::int -> n)
            selection (t5.a::int = 1::int)
              scan t5
          projection (t5.a::int -> a)
            selection (t5.a::int = 1::int)
              scan t5
      projection (t5.a::int -> a)
        selection (t5.a::int <> 1::int)
          scan t5
    ");
}

/// The motion protects the consumer edge; the CTE body stays one-stage.
#[test]
fn scalar_aggregate_on_single_shard_in_reused_cte() {
    let query = r#"explain (logical) WITH c(n) AS (SELECT count(*) FROM "t5" WHERE "a" = 1)
        SELECT n FROM c UNION ALL SELECT n FROM c
        UNION ALL SELECT "a" FROM "t5" WHERE "a" = 4"#;
    let plan = sql_to_optimized_ir(query, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    union all
      motion [policy: segment([ref(n)]), program: ReshardIfNeeded]
        union all
          projection (c.n::int -> n)
            scan cte c($0)
          projection (c.n::int -> n)
            scan cte c($0)
      projection (t5.a::int -> a)
        selection (t5.a::int = 4::int)
          scan t5
    subquery $0:
      projection (count(*)::int -> n)
        selection (t5.a::int = 1::int)
          scan t5
    ");
}

/// Single-input nodes above the aggregate keep the statement on its shard.
#[test]
fn scalar_aggregate_on_single_shard_under_order_by_limit() {
    let query = r#"explain (logical) SELECT count(*) AS n FROM "t5" WHERE "a" = 1
        ORDER BY 1 LIMIT 1"#;
    let plan = sql_to_optimized_ir(query, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    limit 1
      projection (n::int)
        order by (1)
          scan
            projection (count(*)::int -> n)
              selection (t5.a::int = 1::int)
                scan t5
    ");
}

/// An empty input does not empty a scalar aggregate: no `false` domain check
/// is repeated above it, so the union still gets the `count = 0` row.
#[test]
fn scalar_aggregate_over_false_under_union_all() {
    let query = r#"explain (logical) SELECT count(*) AS n FROM "t5" WHERE false
        UNION ALL SELECT "a" FROM "t5" WHERE "a" <> 1"#;
    let plan = sql_to_optimized_ir(query, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    union all
      motion [policy: segment([ref(n)]), program: ReshardIfNeeded]
        projection (count(*)::int -> n)
          selection (false::bool)
            scan t5
      projection (t5.a::int -> a)
        selection (t5.a::int <> 1::int)
          scan t5
    ");
}

/// Both branches are proven to run on the bucket of `a = 1`, so the union
/// does not widen the shards of the aggregate.
#[test]
fn scalar_aggregate_on_single_shard_union_all_same_bucket() {
    let query = r#"explain (logical) SELECT count(*) AS n FROM "t5" WHERE "a" = 1
        UNION ALL SELECT "a" FROM "t5" WHERE "a" = 1"#;
    let plan = sql_to_optimized_ir(query, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    union all
      projection (count(*)::int -> n)
        selection (t5.a::int = 1::int)
          scan t5
      projection (t5.a::int -> a)
        selection (t5.a::int = 1::int)
          scan t5
    ");
}

#[test]
fn scalar_aggregates_on_single_shards_in_both_union_all_branches() {
    let query = r#"explain (logical) SELECT count(*) AS n FROM "t5" WHERE "a" = 1
        UNION ALL SELECT sum("a") FROM "t5" WHERE "a" = 4"#;
    let plan = sql_to_optimized_ir(query, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    union all
      motion [policy: segment([ref(n)]), program: ReshardIfNeeded]
        projection (count(*)::int -> n)
          selection (t5.a::int = 1::int)
            scan t5
      motion [policy: segment([ref(col_1)]), program: ReshardIfNeeded]
        projection (sum(t5.a::int::int)::decimal -> col_1)
          selection (t5.a::int = 4::int)
            scan t5
    ");
}

/// A GROUP BY yields no rows on empty input, so extra shards are harmless.
#[test]
fn group_by_aggregate_on_single_shard_under_union_all() {
    let query = r#"explain (logical) SELECT count(*) AS n FROM "t5" WHERE "a" = 1 GROUP BY "a"
        UNION ALL SELECT "a" FROM "t5" WHERE "a" = 4"#;
    let plan = sql_to_optimized_ir(query, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    union all
      projection (count(*)::int -> n)
        group by (t5.a::int)
          selection (t5.a::int = 1::int)
            scan t5
      projection (t5.a::int -> a)
        selection (t5.a::int = 4::int)
          scan t5
    ");
}

#[test]
fn scalar_aggregate_on_single_shard_under_union() {
    let query = r#"explain (logical) SELECT count(*) AS n FROM "t5" WHERE "a" = 1
        UNION SELECT "a" FROM "t5" WHERE "a" = 4"#;
    let plan = sql_to_optimized_ir(query, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    motion [policy: full, program: RemoveDuplicates]
      union
        motion [policy: segment([ref(n)]), program: ReshardIfNeeded]
          projection (count(*)::int -> n)
            selection (t5.a::int = 1::int)
              scan t5
        projection (t5.a::int -> a)
          selection (t5.a::int = 4::int)
            scan t5
    ");
}

/// The inner UNION stays materialized under UNION ALL, and only one shard
/// reads it, so `count()` needs two stages.
#[test]
fn scalar_aggregate_over_union_under_union_all() {
    let query = r#"explain (logical) SELECT count(*) FROM (SELECT "a" FROM "t5"
        UNION SELECT "a" FROM "t5"
        UNION ALL SELECT "a" FROM "t5" WHERE "a" = 1) u"#;
    let plan = sql_to_optimized_ir(query, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    projection (sum(count_1::int::int)::int::int -> col_1)
      motion [policy: full, program: ReshardIfNeeded]
        projection (count(*)::int -> count_1)
          scan u
            union all
              motion [policy: local, program: SerializeAsEmptyTable(true)]
                projection (a::int)
                  motion [policy: full, program: RemoveDuplicates]
                    union
                      projection (t5.a::int -> a)
                        scan t5
                      projection (t5.a::int -> a)
                        scan t5
              projection (t5.a::int -> a)
                selection (t5.a::int = 1::int)
                  scan t5
    ");
}

/// A scalar aggregate under UNION stays behind the full motion when outer
/// UNION ALLs widen the shards.
#[test]
fn scalar_aggregate_under_union_under_union_all() {
    let query = r#"explain (logical) SELECT count(*) AS a FROM "t5" WHERE "a" = 1
        UNION SELECT "a" FROM "t5" WHERE "a" = 1
        UNION ALL SELECT "a" FROM "t5"
        UNION ALL SELECT "a" FROM "t5""#;
    let plan = sql_to_optimized_ir(query, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    union all
      union all
        motion [policy: local, program: SerializeAsEmptyTable(true)]
          projection (a::int)
            motion [policy: full, program: RemoveDuplicates]
              union
                projection (count(*)::int -> a)
                  selection (t5.a::int = 1::int)
                    scan t5
                projection (t5.a::int -> a)
                  selection (t5.a::int = 1::int)
                    scan t5
        projection (t5.a::int -> a)
          scan t5
      projection (t5.a::int -> a)
        scan t5
    ");
}

/// A UNION read on one shard together with its sibling stays in place under
/// UNION: the outer stage runs on that shard only.
#[test]
fn union_on_one_shard_under_union() {
    let query = r#"explain (logical) SELECT "a" FROM "t5" WHERE "a" = 1
        UNION SELECT "a" FROM "t5" WHERE "a" = 1
        UNION SELECT "a" FROM "t5" WHERE "a" = 1"#;
    let plan = sql_to_optimized_ir(query, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    motion [policy: full, program: RemoveDuplicates]
      union
        motion [policy: local, program: SerializeAsEmptyTable(true)]
          union
            projection (t5.a::int -> a)
              selection (t5.a::int = 1::int)
                scan t5
            projection (t5.a::int -> a)
              selection (t5.a::int = 1::int)
                scan t5
        projection (t5.a::int -> a)
          selection (t5.a::int = 1::int)
            scan t5
    ");
}

/// `add_motions` memoizes bucket proofs and union verdicts; in debug builds
/// every cache hit is checked against a recomputation from the current plan.
/// Run shapes where processed subtrees are rewritten or read again.
#[test]
fn motions_cache_matches_recomputation() {
    for query in [
        // Nested UNION ALL with the same and with different buckets.
        r#"SELECT count(*) AS n FROM "t5" WHERE "a" = 1
            UNION ALL SELECT "a" FROM "t5" WHERE "a" = 1
            UNION ALL SELECT "a" FROM "t5" WHERE "a" = 1
            UNION ALL SELECT "a" FROM "t5" WHERE "a" <> 1"#,
        r#"SELECT count(*) AS n FROM "t5" WHERE "a" = 1
            UNION ALL SELECT "a" FROM "t5" WHERE "a" = 4
            UNION ALL SELECT "a" FROM "t5" WHERE "a" <> 1"#,
        // UNION DISTINCT and the Full motion above it.
        r#"SELECT count(*) AS n FROM "t5" WHERE "a" = 1
            UNION SELECT "a" FROM "t5" WHERE "a" = 1
            UNION ALL SELECT "a" FROM "t5" WHERE "a" <> 1"#,
        // A CTE read by a UNION ALL and by an IN subquery.
        r#"WITH c(n) AS (SELECT count(*) FROM "t5" WHERE "a" = 1)
            SELECT n FROM c UNION ALL SELECT n FROM c
            UNION ALL SELECT "a" FROM "t5" WHERE "a" IN (SELECT n FROM c)"#,
        // Two-stage aggregation with GROUP BY and HAVING under a union.
        r#"SELECT count(*) AS n FROM "t5" GROUP BY "b" HAVING count(*) > 1
            UNION ALL SELECT count(*) FROM "t5" WHERE "a" = 1 HAVING count(*) > 0
            UNION ALL SELECT "a" FROM "t5" WHERE "a" = 1"#,
        // Expression subqueries around unions.
        r#"SELECT "a" FROM "t5" WHERE "a" IN (
            SELECT count(*) FROM "t5" WHERE "a" = 1
            UNION ALL SELECT "a" FROM "t5" WHERE "a" = 1)
            UNION ALL SELECT (SELECT count(*) FROM "t5" WHERE "a" = 1) FROM "t5""#,
        // ORDER BY / LIMIT over and inside unions.
        r#"SELECT * FROM (SELECT count(*) AS n FROM "t5" WHERE "a" = 1
            UNION ALL SELECT "a" FROM "t5" WHERE "a" = 1
            UNION ALL SELECT "a" FROM "t5" WHERE "a" <> 1) ORDER BY 1 LIMIT 3"#,
        r#"SELECT "a" FROM "t5" WHERE "a" = (
            SELECT count(*) FROM "t5" WHERE "a" = 1 ORDER BY 1 LIMIT 1)
            UNION ALL SELECT "a" FROM "t5" WHERE "a" = 4"#,
        // Joins over single-shard inputs under a union.
        r#"SELECT count(*) AS n FROM (SELECT "a" FROM "t5" WHERE "a" = 1) AS l
            JOIN (SELECT "a" FROM "t5" WHERE "a" = 1) AS r ON l."a" = r."a"
            UNION ALL SELECT "a" FROM "t5" WHERE "a" = 1
            UNION ALL SELECT "a" FROM "t5" WHERE "a" = 4"#,
    ] {
        let _ = sql_to_optimized_ir(query, vec![]);
    }
}
