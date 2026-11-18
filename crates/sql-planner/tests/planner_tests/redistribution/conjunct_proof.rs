//! With enrichment, AddMotions proves a node single-node from the pins of the
//! equality facts, without reading its condition, and stores them as the
//! witness the executor narrows by. A plan proven single-node must be routed
//! to one node by the executor.

use sql::explain::explain_logical;
use sql::helpers::sql_to_ir_without_bind;
use sql::ir::options::Options;
use sql::ir::types::{DerivedType, UnrestrictedType};
use sql::ir::value::Value;
use sql::ir::Plan;
use sql_executor::executor::engine::mock::RouterRuntimeMock;
use sql_executor::executor::ExecutingQuery;

/// `query` optimized with integer parameters `params`.
fn optimized(query: &str, params: usize) -> Plan {
    let mut plan = sql_to_ir_without_bind(
        &format!("EXPLAIN (LOGICAL) {query}"),
        &vec![DerivedType::new(UnrestrictedType::Integer); params],
    );
    plan.optimize().unwrap();
    plan
}

/// Whether the optimized `query` needs no motion; if so, the mock executor
/// routes it, bound to `params`, to one node.
fn single_node(query: &str, params: &[Value]) -> bool {
    let plan = optimized(query, params.len());
    if !plan.slices().slices().is_empty() {
        return false;
    }
    let mut bound = plan.clone();
    bound
        .bind_statement(params.to_vec(), Options::default())
        .unwrap();
    let top = bound.get_top().unwrap();
    let runtime = RouterRuntimeMock::new();
    let mut executing = ExecutingQuery::from_plan(&runtime, bound).unwrap();
    let buckets = executing.bucket_discovery(top).unwrap();
    assert!(
        buckets.is_single_node(),
        "{query}: proven single-node, routed to {buckets:?}\n{}",
        explain_logical(&plan).unwrap()
    );
    true
}

/// An equality enrichment absorbs is replaced by the derived conjuncts,
/// which still fix the key.
#[test]
fn absorbed_equality_leaves_the_key_fixed() {
    let query = "SELECT count(*) FROM t5 WHERE a = b AND b = 1";
    insta::assert_snapshot!(explain_logical(&optimized(query, 0)).unwrap(), @r"
    projection (count(*)::int -> col_1)
      selection ((t5.a::int = 1::int and t5.b::int = 1::int))
        scan t5
    ");
    assert!(single_node(query, &[]));
}

/// A row equality on a composite key with a parameter fixes it.
#[test]
fn row_equality_with_parameter_fixes_composite_key() {
    let query = "SELECT count(*) FROM t WHERE (a, b) = (1, $1)";
    assert!(single_node(query, &[Value::from(2)]));
}

/// A key fixed only in one branch of an OR is not fixed by the filter.
#[test]
fn key_inside_one_or_branch_is_not_fixed() {
    for query in [
        "SELECT count(*) FROM t WHERE (a = 1 AND c = 1) OR c = 2",
        "SELECT count(*) FROM t5 WHERE a = 1 OR b > 0",
    ] {
        assert!(!single_node(query, &[]), "{query}");
    }
}

/// Enrichment states the pin all branches of an OR share as a conjunct.
#[test]
fn common_pin_of_or_branches_fixes_the_key() {
    let query = "SELECT count(*) FROM t5 WHERE (a = 1 AND b = 2) OR (a = 1 AND b = 3)";
    insta::assert_snapshot!(explain_logical(&optimized(query, 0)).unwrap(), @r"
    projection (count(*)::int -> col_1)
      selection ((((t5.a::int = 1::int and t5.b::int = 2::int) or (t5.a::int = 1::int and t5.b::int = 3::int)) and t5.a::int = 1::int))
        scan t5
    ");
    assert!(single_node(query, &[]));
}

/// A conjunct PushDownRestrictions moves fixes the key where it lands, the
/// filter of the operand here, and no longer where it was stated.
#[test]
fn pushed_down_conjunct_fixes_the_key_where_it_lands() {
    let query = "SELECT count(*) FROM t5 AS l JOIN global_t AS g ON true WHERE l.a = 1";
    insta::assert_snapshot!(explain_logical(&optimized(query, 0)).unwrap(), @r"
    projection (count(*)::int -> col_1)
      selection (true::bool)
        join on (true::bool)
          scan l
            projection (l.bucket_id::int -> bucket_id, l.a::int -> a, l.b::int -> b)
              selection (l.a::int = 1::int)
                scan t5 -> l
          scan global_t -> g
    ");
    assert!(single_node(query, &[]));
}

/// `t` is sharded by `(a, b)`: `a = 1` below a motion and `b = 2` above it
/// fix the key on neither filter.
#[test]
fn key_parts_on_different_filters_are_not_fixed() {
    let query = "SELECT count(*) FROM (SELECT a, b FROM t WHERE a = 1 GROUP BY a, b) s WHERE b = 2";
    assert!(!single_node(query, &[]));
}

/// Contradicting filters below a join: each keeps its own bucket, and only
/// the `ON` enrichment empties with `false` proves nothing to scan.
#[test]
fn contradiction_is_empty_only_where_false_stands() {
    let query = "SELECT count(*) FROM (SELECT a FROM t5 WHERE a = 1) l \
        JOIN (SELECT a FROM t5 WHERE a = 2) r ON l.a = r.a";
    insta::assert_snapshot!(explain_logical(&optimized(query, 0)).unwrap(), @r"
    projection (count(*)::int -> col_1)
      join on (false::bool)
        scan l
          projection (t5.a::int -> a)
            selection (t5.a::int = 1::int)
              scan t5
        scan r
          projection (t5.a::int -> a)
            selection (t5.a::int = 2::int)
              scan t5
    ");
    assert!(single_node(query, &[]));
}

/// With enrichment, the counterpart of `inner_join_with_one_restricted_side_keeps_reduce`:
/// `l.a = 1` is derived into the `ON`, so the join is single-node, and the
/// executor routes it to that node.
#[test]
fn inner_join_with_one_restricted_side_is_routed_to_one_node() {
    let query = r#"SELECT count(*) FROM "t5" AS l
        JOIN (SELECT "a" FROM "t5" WHERE "a" = 1) AS r ON l."a" = r."a""#;
    assert!(single_node(query, &[]));
}

/// With enrichment, the counterpart of
/// `static_bucket_proofs_keep_reduce_for_colliding_contradictions`: `1` and
/// `10208` share a bucket. Whatever enrichment derives, a plan proven
/// single-node is routed to one node.
#[test]
fn colliding_contradictions_are_routed_to_one_node() {
    for input in [
        "SELECT a FROM t5 WHERE (a = 1 AND a = 10208) OR a = 2",
        "SELECT a FROM t5 WHERE a = 1 AND a = 10208 UNION ALL SELECT a FROM t5 WHERE a = 2",
        "SELECT l.a FROM (SELECT a FROM t5 WHERE a = 1 AND a = 10208) l \
         JOIN (SELECT a FROM t5 WHERE a = 2) r ON l.a = r.a",
    ] {
        single_node(&format!("SELECT count(*) FROM ({input}) u"), &[]);
    }
}

/// A key equality among the conjuncts makes `count(*)` single-node.
#[test]
fn conjunct_on_sharding_key_makes_aggregate_single_node() {
    assert!(single_node("SELECT count(*) FROM t5 WHERE a = 1", &[]));
}

/// `t1` sharded by `a`, `t2` by `b` in the spec: enrichment states the
/// derived `r.a = 1` below the join, where the proof reads it.
#[test]
fn derived_conjunct_on_sharding_key_makes_join_single_node() {
    let query = "SELECT count(*) FROM t5 AS l JOIN t5 AS r ON l.a = r.a WHERE l.a = 1";
    assert!(single_node(query, &[]));
}

/// Keys only under an OR are read by the tree walk alone: the conjunct proof
/// is less strict, which the debug check allows, and the motion stays.
#[test]
fn keys_under_or_keep_aggregate_motion() {
    assert!(!single_node(
        "SELECT count(*) FROM t5 WHERE a = 1 OR a = 2",
        &[]
    ));
}

/// The `false` of a contradictory class is repeated on the motion input,
/// whose whole filter it is: that input proves empty on its own.
#[test]
fn false_copied_to_motion_input_is_empty_there() {
    let query = "SELECT count(*) FROM t JOIN (SELECT a, b FROM t3 WHERE a = 'x') s \
        ON t.a = s.b WHERE s.a = 'y'";
    let plan = optimized(query, 0);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    projection (count(*)::int -> col_1)
      selection (false::bool)
        join on (t.a::int = s.b::int)
          scan t
          motion [policy: full, program: ReshardIfNeeded]
            projection (s.a::text -> a, s.b::int -> b)
              selection (false::bool)
                scan s
                  projection (t3.a::text -> a, t3.b::int -> b)
                    selection (t3.a::text = 'x'::text)
                      scan t3
    ");
    single_node(query, &[]);
}

/// What the tree walk finds only under an OR the conjunct proof does not:
/// such a query may keep a motion, but whatever it proves single-node is
/// routed to one node.
#[test]
fn keys_only_under_or_are_routed_safely() {
    for query in [
        // OR over columns of different tables.
        "SELECT count(*) FROM t5 AS l JOIN t5 AS r ON l.b = r.b WHERE l.a = 1 OR r.a = 1",
        // A part of the composite key `(a, b)` of `t` outside the OR.
        "SELECT count(*) FROM t WHERE a = 1 AND (b = 2 OR b = 3)",
        // Values of different types.
        "SELECT count(*) FROM t5 WHERE a = 1 OR a = 1.0",
        // An IN list is a chain of ORs by now.
        "SELECT count(*) FROM t5 WHERE a IN (1, 2)",
        // A cast, bucket_id, IS TRUE or CURRENT_USER under an OR.
        "SELECT count(*) FROM t5 WHERE (CAST(a AS int) = 1 AND b = 1) OR (CAST(a AS int) = 1 AND b = 2)",
        "SELECT count(*) FROM t5 WHERE bucket_id = 42 OR a = 1",
        "SELECT count(*) FROM bool_sharded WHERE b IS TRUE OR payload = 1",
        "SELECT count(*) FROM t3 WHERE a = CURRENT_USER OR b = 1",
    ] {
        single_node(query, &[]);
    }
}

/// `dec_t.a` is a decimal key: equal decimals may be stored in several
/// forms, so the facts never pin it, whatever it is compared with, and
/// constants of two types are no error. The node keeps its motion and is
/// routed by its condition, as on master.
#[test]
fn decimal_key_is_never_pinned_by_the_facts() {
    for query in [
        "SELECT count(*) FROM dec_t AS l JOIN dec_t AS r ON l.a = r.a \
         WHERE (l.a = 1 AND r.b = 10) OR (l.a = 1 AND r.b = 100)",
        "SELECT count(*) FROM dec_t WHERE (a = 1 AND b = 10) OR (a = 1 AND b = 100)",
        "SELECT count(*) FROM dec_t WHERE a = 1.0",
        "SELECT count(*) FROM dec_t WHERE a = 1.0 AND a = 1",
        "SELECT count(*) FROM dec_t WHERE a = 1.0 AND a = 2",
    ] {
        assert!(!single_node(query, &[]), "{query}");
    }
}

/// The buckets the mock executor routes the optimized `query` to.
fn routed_buckets(query: &str) -> sql::ir::bucket::Buckets {
    let plan = optimized(query, 0);
    let top = plan.get_top().unwrap();
    let runtime = RouterRuntimeMock::new();
    let mut executing = ExecutingQuery::from_plan(&runtime, plan).unwrap();
    executing.bucket_discovery(top).unwrap()
}

/// An integer literal pins an integer key (`t5.a`): directly, as the
/// common pin of OR branches, and through a join to the other column. A
/// decimal key (`dec_t.a`) is not pinned: the node keeps its motion.
#[test]
fn integer_literal_pins_an_integer_key_but_not_a_decimal_one() {
    use sql::ir::bucket::Buckets;
    use sql_executor::executor::engine::Vshard;
    let bucket = RouterRuntimeMock::new()
        .determine_bucket_id(&[&Value::from(1)])
        .unwrap();
    let expected = Buckets::new_filtered([bucket].into_iter().collect());
    for table in ["t5", "dec_t"] {
        for filter in ["a = 1", "(a = 1 AND b = 10) OR (a = 1 AND b = 100)"] {
            let query = format!("SELECT count(*) FROM {table} WHERE {filter}");
            assert_eq!(table == "t5", single_node(&query, &[]), "{query}");
            if table == "t5" {
                assert_eq!(expected, routed_buckets(&query), "{query}");
            }
        }
        let query = format!(
            "SELECT count(*) FROM {table} AS l JOIN {table} AS r ON l.a = r.a WHERE l.a = 1"
        );
        assert_eq!(table == "t5", single_node(&query, &[]), "{query}");
    }
    let query = "SELECT count(*) FROM t5 AS l JOIN t5 AS r ON l.a = r.a WHERE l.a = 1";
    assert_eq!(expected, routed_buckets(query), "{query}");
}

/// The composite key `(a, b)` of `t` (both integer) pinned by literals of its
/// type, in key order. Literals of another type pin no part of it.
#[test]
fn composite_key_is_pinned_by_literals_of_its_type() {
    use sql::ir::bucket::Buckets;
    use sql_executor::executor::engine::Vshard;
    let query = "SELECT count(*) FROM t WHERE b = 2::decimal AND a = 1::decimal";
    assert!(!single_node(query, &[]), "{query}");
    let query = "SELECT count(*) FROM t WHERE b = 2 AND a = 1";
    let bucket = RouterRuntimeMock::new()
        .determine_bucket_id(&[&Value::from(1), &Value::from(2)])
        .unwrap();
    assert!(single_node(query, &[]), "{query}");
    assert_eq!(
        Buckets::new_filtered([bucket].into_iter().collect()),
        routed_buckets(query),
        "{query}"
    );
}

/// `CURRENT_USER` is one value per query and `bucket_id = 42` one bucket: in
/// every branch of an OR they still fix the key, as a literal does.
#[test]
fn current_user_and_bucket_id_shared_by_or_branches_fix_the_key() {
    for query in [
        "SELECT count(*) FROM t3 WHERE (a = CURRENT_USER AND b = 1) OR (a = CURRENT_USER AND b = 2)",
        "SELECT count(*) FROM t5 WHERE (bucket_id = 42 AND b = 1) OR (bucket_id = 42 AND b = 2)",
    ] {
        assert!(single_node(query, &[]), "{query}");
    }
    assert!(!single_node(
        "SELECT count(*) FROM t5 WHERE bucket_id = 42 OR a = 1",
        &[]
    ));
}

/// `CURRENT_USER`, a known non-NULL value, pins the class over a parameter.
#[test]
fn current_user_outranks_a_parameter_as_the_pin() {
    let query = "SELECT count(*) FROM t3 WHERE a = $1 AND a = CURRENT_USER";
    let mut plan = sql_to_ir_without_bind(
        &format!("EXPLAIN (LOGICAL) {query}"),
        &[DerivedType::new(UnrestrictedType::String)],
    );
    plan.optimize().unwrap();
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    projection (count(*)::int -> col_1)
      selection (($1::text = CurrentUser::text and t3.a::text = CurrentUser::text))
        scan t3
    ");
}

/// A composite key `(a, b)` fixed as `($1, 2)` by separate equalities, with
/// no row equality: the facts pin each part.
#[test]
fn composite_key_fixed_by_separate_equalities() {
    let query = "SELECT count(*) FROM t WHERE a = $1 AND b = 2";
    assert!(single_node(query, &[Value::from(1)]));
}

/// The parts of a key pinned by different filters of one domain fix it
/// together; a motion between them splits the domain (see
/// `key_parts_on_different_filters_are_not_fixed`).
#[test]
fn key_parts_in_one_domain_fix_the_key_together() {
    let query = "SELECT count(*) FROM (SELECT a, b FROM t WHERE a = $1) s WHERE b = 2";
    assert!(single_node(query, &[Value::from(1)]));
}

/// A pin on the nullable side of a LEFT JOIN, in its input or in `ON`,
/// does not bound the left rows it keeps.
#[test]
fn nullable_side_of_left_join_does_not_fix_the_key() {
    for query in [
        "SELECT count(*) FROM t5 AS l LEFT JOIN (SELECT a FROM t5 WHERE a = 1) AS r ON l.a = r.a",
        "SELECT count(*) FROM t5 AS l LEFT JOIN t5 AS r ON l.a = r.a AND r.a = 1",
    ] {
        assert!(!single_node(query, &[]), "{query}");
    }
}

/// A CTE used with different pins keeps its shared body unpinned.
#[test]
fn pins_of_cte_uses_do_not_fix_the_shared_body() {
    let query = "WITH s AS (SELECT a FROM t5) \
        SELECT count(*) FROM (SELECT a FROM s WHERE a = 1 UNION ALL SELECT a FROM s WHERE a = 2) u";
    assert!(!single_node(query, &[]));
}

/// A pin above a Segment motion fixes the consumer, not the producer that
/// reads every bucket: the motion and its reduce stay.
#[test]
fn pin_above_segment_motion_keeps_the_motion() {
    let query = "SELECT count(*) FROM (SELECT b AS a FROM t5 GROUP BY b) AS s WHERE a = 1";
    assert!(!single_node(query, &[]));
}

/// AddMotions does not read conditions: right before it, `a = 1` turned into
/// `a <> 1` leaves the decision taken from the facts, which still pin `a`.
#[test]
fn add_motions_decides_from_facts_not_from_the_condition() {
    use sql::ir::node::expression::{Expression, MutExpression};
    use sql::ir::node::relational::Relational;
    use sql::ir::node::{BoolExpr, Selection};
    use sql::ir::operator::Bool;
    use sql::ir::transformation::Stage;
    use sql::ir::tree::traversal::{NodeIter, PostOrder};

    let decide = |edit: bool| {
        let mut plan =
            sql_to_ir_without_bind("SELECT count(*) FROM t5 WHERE a = 1", &[] as &[DerivedType]);
        let top = plan.get_top().unwrap();
        let mut state = plan.optimize_before(top, Stage::AddMotions).unwrap();
        if edit {
            let mut edited = 0;
            let filters: Vec<_> =
                PostOrder::new(&plan.nodes, |node| plan.nodes.rel_iter(node), top)
                    .relational()
                    .filter_map(|(_, rel)| match rel {
                        Relational::Selection(Selection { filter, .. }) => Some(*filter),
                        _ => None,
                    })
                    .collect();
            for filter in filters {
                if let Ok(Expression::Bool(BoolExpr { op: Bool::Eq, .. })) =
                    plan.get_expression_node(filter)
                {
                    if let Ok(MutExpression::Bool(BoolExpr { op, .. })) =
                        plan.get_mut_expression_node(filter)
                    {
                        *op = Bool::NotEq;
                        edited += 1;
                    }
                }
            }
            assert_eq!(1, edited);
        }
        plan.add_motions_to_subtree(top, &mut state).unwrap();
        plan.slices().slices().is_empty()
    };
    assert!(decide(false));
    assert!(decide(true));
}
