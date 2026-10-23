use std::collections::HashSet;

use insta::assert_snapshot;
use sql::explain::ir::LogicalExplain;
use sql::helpers::sql_to_ir_without_bind;
use sql::ir::expression::VolatilityType;
use sql::ir::node::expression::{Expression, MutExpression};
use sql::ir::node::relational::Relational;
use sql::ir::node::{
    Join, Node, NodeId, Projection, ScalarFunction, ScanRelation, Selection, SubQueryReference,
};
use sql::ir::transformation::Stage;
use sql::ir::tree::traversal::{PostOrderWithFilter, EXPR_CAPACITY};
use sql::ir::types::{DerivedType, UnrestrictedType};
use sql::ir::Plan;

/// Run every pass before the push-down over `plan`, then the push-down
/// itself, stopping after push-down. Checks the result is
/// consistent and a second run changes nothing.
fn push_down(plan: Plan) -> Plan {
    let top = plan.get_top().unwrap();
    let plan = plan
        .optimize_before(top, Stage::PushDownRestrictions)
        .unwrap()
        .push_down_restrictions(top)
        .unwrap();
    assert_restrictions_are_fresh(&plan, top);
    assert_selections_keep_input_classes(&plan, top);
    assert_subquery_lists_match_conditions(&plan, top);
    assert_rerun_changes_nothing(&plan, top);
    plan
}

/// [`push_down`] over the plan of `query`.
fn run_push_down(query: &str) -> Plan {
    push_down(sql_to_ir_without_bind(query, &[]))
}

/// The stored restrictions equal a full rebuild over `top`.
#[track_caller]
fn assert_restrictions_are_fresh(plan: &Plan, top: NodeId) {
    let mut fresh = plan.clone();
    fresh.restrictions = None;
    let fresh = fresh
        .analyze_restrictions_in_subtree(top)
        .unwrap()
        .restrictions;
    assert_eq!(plan.restrictions, fresh, "stale restrictions");
}

/// Every `Selection` passes its input's equality classes through unchanged,
/// the new ones included.
#[track_caller]
fn assert_selections_keep_input_classes(plan: &Plan, top: NodeId) {
    let facts = plan.facts.as_ref().expect("facts are analyzed");
    for id in rel_nodes(plan, top) {
        let Relational::Selection(Selection { child, .. }) = plan.get_relation_node(id).unwrap()
        else {
            continue;
        };
        for pos in 0..plan.columns_len(id).unwrap() {
            assert_eq!(
                facts.class_of_slot(id, pos),
                facts.class_of_slot(*child, pos),
                "classes of {id:?} and its input differ at {pos}"
            );
        }
    }
}

/// Every `Selection` and `Join` lists exactly the subqueries its condition
/// references, each once.
#[track_caller]
fn assert_subquery_lists_match_conditions(plan: &Plan, top: NodeId) {
    let key = |id: &NodeId| (id.arena_type, id.offset);
    for id in rel_nodes(plan, top) {
        let rel = plan.get_relation_node(id).unwrap();
        let condition = match rel {
            Relational::Selection(Selection { filter, .. }) => *filter,
            Relational::Join(Join { condition, .. }) => *condition,
            _ => continue,
        };
        let mut referenced: Vec<NodeId> = Vec::new();
        for ref_id in plan.get_sq_refs_from_subtree(condition).unwrap() {
            let Expression::SubQueryReference(SubQueryReference { rel_id, .. }) =
                plan.get_expression_node(ref_id).unwrap()
            else {
                unreachable!();
            };
            if !referenced.contains(rel_id) {
                referenced.push(*rel_id);
            }
        }
        let mut listed: Vec<NodeId> = rel.subqueries().to_vec();
        let len = listed.len();
        listed.sort_unstable_by_key(key);
        listed.dedup();
        assert_eq!(listed.len(), len, "{id:?} lists a subquery twice");
        referenced.sort_unstable_by_key(key);
        assert_eq!(listed, referenced, "stale subqueries of {id:?}");
    }
}

/// A second push-down moves no clause and leaves the restrictions as they are.
#[track_caller]
fn assert_rerun_changes_nothing(plan: &Plan, top: NodeId) {
    let again = plan.clone().push_down_restrictions(top).unwrap();
    assert_eq!(logical(&again), logical(plan), "rerun moved a clause");
    assert_eq!(
        again.restrictions, plan.restrictions,
        "rerun changed restrictions"
    );
}

fn logical(plan: &Plan) -> String {
    let top = plan.get_top().unwrap();
    LogicalExplain::new(plan, top).unwrap().to_string()
}

/// The relational nodes under `root`, subquery bodies included, each once
/// however many parents it has.
fn rel_nodes(plan: &Plan, root: NodeId) -> Vec<NodeId> {
    let mut seen = HashSet::new();
    let mut nodes = Vec::new();
    let mut stack = vec![root];
    while let Some(id) = stack.pop() {
        if seen.insert(id) {
            nodes.push(id);
            stack.extend(plan.nodes.rel_iter(id));
        }
    }
    nodes
}

fn scans_alias(plan: &Plan, body: NodeId, alias: &str) -> bool {
    rel_nodes(plan, body).into_iter().any(|id| {
        matches!(
            plan.get_relation_node(id).unwrap(),
            Relational::ScanRelation(ScanRelation { alias: Some(scan), .. }) if scan == alias
        )
    })
}

/// The only node listing a subquery that scans `alias`, and that subquery.
/// Fails if no node or more than one (owner, subquery) pair matches.
#[track_caller]
fn listed_body(plan: &Plan, alias: &str) -> (NodeId, NodeId) {
    let top = plan.get_top().unwrap();
    let mut found = Vec::new();
    for owner in rel_nodes(plan, top) {
        for subquery in plan.get_relation_node(owner).unwrap().subqueries().iter() {
            if scans_alias(plan, *subquery, alias) {
                found.push((owner, *subquery));
            }
        }
    }
    match found[..] {
        [one] => one,
        [] => panic!("no node lists a subquery over {alias}"),
        _ => panic!("several (owner, subquery) pairs over {alias}: {found:?}"),
    }
}

fn subqueries_of(plan: &Plan, owner: NodeId) -> Vec<NodeId> {
    plan.get_relation_node(owner).unwrap().subqueries().to_vec()
}

/// The `WHERE` of the outermost query: the Selection under the top projection.
fn top_where(plan: &Plan) -> NodeId {
    let top = plan.get_top().unwrap();
    let Relational::Projection(Projection {
        child: Some(child), ..
    }) = plan.get_relation_node(top).unwrap()
    else {
        panic!("the top is a projection");
    };
    assert!(matches!(
        plan.get_relation_node(*child).unwrap(),
        Relational::Selection(_)
    ));
    *child
}

#[test]
fn an_or_over_one_join_operand_moves_to_that_operand() {
    // The OR reads `t` only, so it filters `t` below the join; the emptied
    // WHERE becomes `true`.
    let plan = run_push_down(
        r#"SELECT * FROM "t" JOIN "t2" ON "t"."a" = "t2"."e" WHERE "t"."b" = 1 OR "t"."c" = 2"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on (t.a::int = t2.e::int)
          scan t
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              selection (t.b::int = 1::int or t.c::int = 2::int)
                scan t
          scan t2
    ");
}

#[test]
fn an_or_over_both_join_operands_moves_into_the_on() {
    let plan = run_push_down(
        r#"SELECT * FROM "t" JOIN "t2" ON "t"."a" = "t2"."e" WHERE "t"."b" = 1 OR "t2"."f" = 2"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on ((t.a::int = t2.e::int and (t.b::int = 1::int or t2.f::int = 2::int)))
          scan t
          scan t2
    ");
}

#[test]
fn a_one_sided_or_leaves_the_on_for_its_operand() {
    let plan = run_push_down(
        r#"SELECT * FROM "t" JOIN "t2" ON "t"."a" = "t2"."e" AND ("t2"."f" = 1 OR "t2"."g" = 2)"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      join on (t.a::int = t2.e::int)
        scan t
        scan t2
          projection (t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t2.bucket_id::int -> bucket_id)
            selection (t2.f::int = 1::int or t2.g::int = 2::int)
              scan t2
    ");
}

#[test]
fn conjuncts_on_one_edge_share_one_selection() {
    // Both ORs and the plain comparison land on the same operand: one new filter.
    let plan = run_push_down(
        r#"SELECT * FROM "t" JOIN "t2" ON true
           WHERE ("t"."b" = 1 OR "t"."c" = 2) AND ("t"."d" = 3 OR "t"."b" = 4) AND "t"."c" > 5"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on (true::bool)
          scan t
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              selection (((t.b::int = 1::int or t.c::int = 2::int) and (t.d::int = 3::int or t.b::int = 4::int) and t.c::int > 5::int))
                scan t
          scan t2
    ");
}

#[test]
fn an_existing_selection_is_reused() {
    // The subquery already filters `t`: the OR joins that filter.
    let plan = run_push_down(
        r#"SELECT * FROM (SELECT * FROM "t" WHERE "d" > 0) AS "s"
           WHERE "s"."b" = 1 OR "s"."c" = 2"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (s.a::int -> a, s.b::int -> b, s.c::int -> c, s.d::int -> d)
      selection (true::bool)
        scan s
          projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
            selection ((t.d::int > 0::int and (t.b::int = 1::int or t.c::int = 2::int)))
              scan t
    ");
}

#[test]
fn a_bare_projection_remaps_the_column_positions() {
    // `x` and `y` are `c` and `a` below the projection: the moved OR reads
    // them at their positions in `t`.
    let plan = run_push_down(
        r#"SELECT * FROM (SELECT "c" AS "x", "a" AS "y" FROM "t") AS "s" JOIN "t2" ON true
           WHERE "s"."x" = 1 OR "s"."y" = 2"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (s.x::int -> x, s.y::int -> y, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on (true::bool)
          scan s
            projection (t.c::int -> x, t.a::int -> y)
              selection (t.c::int = 1::int or t.a::int = 2::int)
                scan t
          scan t2
    ");
}

#[test]
fn a_computed_column_stops_the_descent_at_a_higher_place() {
    // `x` is `a + 1`: the OR cannot enter the subquery, but still filters the
    // subquery operand below the join.
    let plan = run_push_down(
        r#"SELECT * FROM (SELECT "a" + 1 AS "x", "b" FROM "t") AS "s" JOIN "t2" ON true
           WHERE "s"."x" = 1 OR "s"."b" = 2"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (s.x::int -> x, s.b::int -> b, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on (true::bool)
          scan s
            projection (s.x::int -> x, s.b::int -> b)
              selection (s.x::int = 1::int or s.b::int = 2::int)
                scan s
                  projection (t.a::int + 1::int -> x, t.b::int -> b)
                    scan t
          scan t2
    ");
}

#[test]
fn an_or_over_the_preserved_side_descends_below_a_left_join() {
    let plan = run_push_down(
        r#"SELECT * FROM "t" LEFT JOIN "t2" ON "t"."a" = "t2"."e"
           WHERE "t"."b" = 1 OR "t"."c" IS NULL"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        left join on (t.a::int = t2.e::int)
          scan t
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              selection (t.b::int = 1::int or t.c::int is null)
                scan t
          scan t2
    ");
}

#[test]
fn an_or_over_the_nullable_side_stays_above_a_left_join() {
    // Pushing this OR into the right input could remove all matches for a
    // left row. The LEFT JOIN would then emit a null-extended row, whereas
    // the original WHERE rejects the matching rows with f neither 1 nor NULL.
    let plan = run_push_down(
        r#"SELECT * FROM "t" LEFT JOIN "t2" ON "t"."a" = "t2"."e"
           WHERE "t2"."f" = 1 OR "t2"."f" IS NULL"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (t2.f::int = 1::int or t2.f::int is null)
        left join on (t.a::int = t2.e::int)
          scan t
          scan t2
    ");
}

#[test]
fn an_or_over_both_sides_of_a_left_join_stays_above_it() {
    let plan = run_push_down(
        r#"SELECT * FROM "t" LEFT JOIN "t2" ON "t"."a" = "t2"."e"
           WHERE "t"."b" = 1 OR "t2"."f" IS NULL"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (t.b::int = 1::int or t2.f::int is null)
        left join on (t.a::int = t2.e::int)
          scan t
          scan t2
    ");
}

#[test]
fn an_aggregate_is_not_crossed() {
    let plan = run_push_down(
        r#"SELECT * FROM (SELECT "b", count(*) AS "n" FROM "t" GROUP BY "b") AS "s"
           WHERE "s"."b" = 1 OR "s"."b" = 2"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (s.b::int -> b, s.n::int -> n)
      selection (s.b::int = 1::int or s.b::int = 2::int)
        scan s
          projection (t.b::int -> b, count(*)::int -> n)
            group by (t.b::int)
              scan t
    ");
}

#[test]
fn a_limit_is_not_crossed() {
    let plan = run_push_down(
        r#"SELECT * FROM (SELECT "a", "b" FROM "t" LIMIT 10) AS "s"
           WHERE "s"."a" = 1 OR "s"."b" = 2"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (s.a::int -> a, s.b::int -> b)
      selection (s.a::int = 1::int or s.b::int = 2::int)
        scan s
          limit 10
            projection (t.a::int -> a, t.b::int -> b)
              scan t
    ");
}

#[test]
fn a_union_is_not_crossed() {
    let plan = run_push_down(
        r#"SELECT * FROM (SELECT "a", "b" FROM "t" UNION ALL SELECT "e", "f" FROM "t2") AS "s"
           WHERE "s"."a" = 1 OR "s"."b" = 2"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (s.a::int -> a, s.b::int -> b)
      selection (s.a::int = 1::int or s.b::int = 2::int)
        scan s
          union all
            projection (t.a::int -> a, t.b::int -> b)
              scan t
            projection (t2.e::int -> e, t2.f::int -> f)
              scan t2
    ");
}

#[test]
fn an_or_with_an_in_subquery_moves_with_its_subquery() {
    // The subquery is uncorrelated: the OR's columns pick `t`, and the `IN`
    // body moves with it. The subquery's own filter is already the lowest.
    let plan = run_push_down(
        r#"SELECT * FROM "t" JOIN "t2" ON true
           WHERE ("t"."b" = 1 OR "t"."a" IN (SELECT "e" FROM "t2" AS "x" WHERE "x"."f" = 1 OR "x"."g" = 2))
             AND ("t"."c" = 1 OR "t"."d" = 2)"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on (true::bool)
          scan t
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              selection (((t.b::int = 1::int or t.a::int in ROW($0)) and (t.c::int = 1::int or t.d::int = 2::int)))
                scan t
          scan t2
    subquery $0:
      scan
        projection (x.e::int -> e)
          selection (x.f::int = 1::int or x.g::int = 2::int)
            scan t2 -> x
    ");
}

#[test]
fn a_shared_cte_body_is_not_changed_for_one_consumer() {
    let plan = run_push_down(
        r#"WITH "c" AS (SELECT "a", "b" FROM "t")
           SELECT * FROM "c" AS "c1" JOIN "c" AS "c2" ON true
           WHERE "c1"."a" = 1 OR "c1"."b" = 2"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (c1.a::int -> a, c1.b::int -> b, c2.a::int -> a, c2.b::int -> b)
      selection (true::bool)
        join on (true::bool)
          scan c1
            projection (c1.a::int -> a, c1.b::int -> b)
              selection (c1.a::int = 1::int or c1.b::int = 2::int)
                scan cte c1($0)
          scan cte c2($0)
    subquery $0:
      projection (t.a::int -> a, t.b::int -> b)
        scan t
    ");
}

#[test]
fn an_inner_join_operand_takes_the_or_in_its_own_on() {
    // An operand that is itself a join cannot be wrapped, so an OR over both
    // operands of the lower join lands in that join's ON.
    let plan = run_push_down(
        r#"SELECT * FROM "t" JOIN "t2" ON "t"."a" = "t2"."e" JOIN "t2" AS "x"
           ON "t"."b" = 1 OR "t2"."f" = 2"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, x.e::int -> e, x.f::int -> f, x.g::int -> g, x.h::int -> h)
      join on (true::bool)
        join on ((t.a::int = t2.e::int and (t.b::int = 1::int or t2.f::int = 2::int)))
          scan t
          scan t2
        scan t2 -> x
    ");
}

#[test]
fn an_unsupported_wrap_over_a_left_join_keeps_the_clause_in_place() {
    // The OR needs both operands of the LEFT JOIN, whose ON never hosts it,
    // and the LEFT JOIN cannot be wrapped: it stays in the upper ON.
    let plan = run_push_down(
        r#"SELECT * FROM "t" LEFT JOIN "t2" ON "t"."a" = "t2"."e" JOIN "t2" AS "x"
           ON "t"."b" = 1 OR "t2"."f" = 2"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, x.e::int -> e, x.f::int -> f, x.g::int -> g, x.h::int -> h)
      join on (t.b::int = 1::int or t2.f::int = 2::int)
        left join on (t.a::int = t2.e::int)
          scan t
          scan t2
        scan t2 -> x
    ");
}

#[test]
fn a_clause_without_columns_stays() {
    let plan = push_down(sql_to_ir_without_bind(
        r#"SELECT * FROM "t" JOIN "t2" ON true WHERE $1 = 1 OR $2 = 2"#,
        &[DerivedType::new(UnrestrictedType::Integer); 2],
    ));
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection ($1::int = 1::int or $2::int = 2::int)
        join on (true::bool)
          scan t
          scan t2
    ");
}

#[test]
fn an_or_descends_through_a_passthrough_subquery_stack() {
    let plan = run_push_down(
        r#"SELECT * FROM (SELECT * FROM (SELECT "a", "b" FROM "t") AS "u") AS "s"
           JOIN "t2" ON "s"."a" = "t2"."e"
           WHERE "s"."a" = 1 OR "s"."b" = 2"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (s.a::int -> a, s.b::int -> b, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on (s.a::int = t2.e::int)
          scan s
            projection (u.a::int -> a, u.b::int -> b)
              scan u
                projection (t.a::int -> a, t.b::int -> b)
                  selection (t.a::int = 1::int or t.b::int = 2::int)
                    scan t
          scan t2
    ");
}

#[test]
fn a_scalar_subquery_with_an_aggregate_moves_with_its_clause() {
    // The aggregate is inside the body, not in the moved clause.
    let plan = run_push_down(
        r#"SELECT * FROM "t" JOIN "t2" ON true
           WHERE "t"."b" = (SELECT max("e") FROM "t2" AS "x") OR "t"."c" = 1"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on (true::bool)
          scan t
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              selection (t.b::int = ROW($0) or t.c::int = 1::int)
                scan t
          scan t2
    subquery $0:
      scan
        projection (max(x.e::int::int)::int -> col_1)
          scan t2 -> x
    ");
}

#[test]
fn two_subqueries_of_one_or_move_together() {
    let plan = run_push_down(
        r#"SELECT * FROM "t" JOIN "t2" ON true
           WHERE "t"."a" IN (SELECT "e" FROM "t2" AS "x") OR "t"."b" IN (SELECT "f" FROM "t2" AS "y")"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on (true::bool)
          scan t
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              selection (t.a::int in ROW($1) or t.b::int in ROW($0))
                scan t
          scan t2
    subquery $0:
      scan
        projection (y.f::int -> f)
          scan t2 -> y
    subquery $1:
      scan
        projection (x.e::int -> e)
          scan t2 -> x
    ");
}

#[test]
fn one_body_of_two_conjuncts_is_listed_once_at_their_new_owner() {
    // BETWEEN states its subquery twice: both conjuncts read `t` only, so
    // they land together and the new filter lists the body once.
    let plan = run_push_down(
        r#"SELECT * FROM "t" JOIN "t2" ON true
           WHERE (SELECT max("e") FROM "t2" AS "x") BETWEEN "t"."a" AND "t"."b""#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on (true::bool)
          scan t
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              selection ((ROW($0) >= t.a::int and ROW($0) <= t.b::int))
                scan t
          scan t2
    subquery $0:
      scan
        projection (max(x.e::int::int)::int -> col_1)
          scan t2 -> x
    ");
}

#[test]
fn a_subquery_clause_joins_an_existing_selection() {
    let plan = run_push_down(
        r#"SELECT * FROM (SELECT * FROM "t" WHERE "d" > 0) AS "s"
           WHERE "s"."b" IN (SELECT "e" FROM "t2") OR "s"."c" = 1"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (s.a::int -> a, s.b::int -> b, s.c::int -> c, s.d::int -> d)
      selection (true::bool)
        scan s
          projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
            selection ((t.d::int > 0::int and (t.b::int in ROW($0) or t.c::int = 1::int)))
              scan t
    subquery $0:
      scan
        projection (t2.e::int -> e)
          scan t2
    ");
}

#[test]
fn a_subquery_clause_over_both_operands_moves_into_the_on() {
    let plan = run_push_down(
        r#"SELECT * FROM "t" JOIN "t2" ON "t"."a" = "t2"."e"
           WHERE "t"."b" IN (SELECT "e" FROM "t2" AS "x") OR "t2"."f" = 1"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on ((t.a::int = t2.e::int and (t.b::int in ROW($0) or t2.f::int = 1::int)))
          scan t
          scan t2
    subquery $0:
      scan
        projection (x.e::int -> e)
          scan t2 -> x
    ");
}

#[test]
fn a_body_shared_across_join_operands_moves_into_the_on() {
    // The two conjuncts of BETWEEN share one body and read the two operands:
    // together they move into the ON, which lists the body once; the WHERE
    // loses it.
    let plan = run_push_down(
        r#"SELECT * FROM "t" JOIN "t2" ON true
           WHERE (SELECT max("e") FROM "t2" AS "x") BETWEEN "t"."a" AND "t2"."f""#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on ((ROW($0) >= t.a::int and ROW($0) <= t2.f::int))
          scan t
          scan t2
    subquery $0:
      scan
        projection (max(x.e::int::int)::int -> col_1)
          scan t2 -> x
    ");
}

#[test]
fn a_body_kept_by_a_remaining_conjunct_does_not_move() {
    // `$0 <= t2.f` reads the nullable side and stays above the LEFT JOIN, so
    // `$0 >= t.a`, which could descend alone, stays with it.
    let plan = run_push_down(
        r#"SELECT * FROM "t" LEFT JOIN "t2" ON "t"."a" = "t2"."e"
           WHERE (SELECT max("e") FROM "t2" AS "x") BETWEEN "t"."a" AND "t2"."f""#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection ((ROW($0) >= t.a::int and ROW($0) <= t2.f::int))
        left join on (t.a::int = t2.e::int)
          scan t
          scan t2
    subquery $0:
      scan
        projection (max(x.e::int::int)::int -> col_1)
          scan t2 -> x
    ");
}

#[test]
fn the_old_owner_keeps_only_the_subqueries_it_still_references() {
    // The first conjunct leaves with its body; the second stays above the
    // LEFT JOIN with its own.
    let plan = run_push_down(
        r#"SELECT * FROM "t" LEFT JOIN "t2" ON "t"."a" = "t2"."e"
           WHERE "t"."b" IN (SELECT "e" FROM "t2" AS "x")
             AND ("t2"."f" IN (SELECT "f" FROM "t2" AS "y") OR "t2"."f" IS NULL)"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (t2.f::int in ROW($1) or t2.f::int is null)
        left join on (t.a::int = t2.e::int)
          scan t
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              selection (t.b::int in ROW($0))
                scan t
          scan t2
    subquery $0:
      scan
        projection (x.e::int -> e)
          scan t2 -> x
    subquery $1:
      scan
        projection (y.f::int -> f)
          scan t2 -> y
    ");
}

#[test]
fn a_moved_subquery_gets_its_motion_at_the_new_owner() {
    // The full pipeline: the body is covered by a motion under the filter
    // it moved to.
    let plan = sql_to_ir_without_bind(
        r#"SELECT * FROM "t" JOIN "t2" ON "t"."a" = "t2"."e"
           WHERE "t"."b" IN (SELECT "e" FROM "t2" AS "x") OR "t"."c" = 1"#,
        &[],
    )
    .optimize()
    .unwrap();
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on (t.a::int = t2.e::int)
          scan t
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              selection (t.b::int in ROW($0) or t.c::int = 1::int)
                scan t
          motion [policy: full, program: ReshardIfNeeded]
            projection (t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t2.bucket_id::int -> bucket_id)
              scan t2
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (x.e::int -> e)
            scan t2 -> x
    ");
}

#[test]
fn a_component_descends_to_the_lower_join_that_first_needs_both_sides() {
    // Through the upper INNER JOIN, the subquery and its projection to the lower
    // join, whose ON is the first place that sees both `t` and `t2`.
    let plan = run_push_down(
        r#"SELECT * FROM (SELECT * FROM "t" JOIN "t2" ON "t"."a" = "t2"."e") AS "s"
           JOIN "t2" AS "z" ON true
           WHERE (SELECT max("e") FROM "t2" AS "x") BETWEEN "s"."b" AND "s"."f""#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (s.a::int -> a, s.b::int -> b, s.c::int -> c, s.d::int -> d, s.e::int -> e, s.f::int -> f, s.g::int -> g, s.h::int -> h, z.e::int -> e, z.f::int -> f, z.g::int -> g, z.h::int -> h)
      selection (true::bool)
        join on (true::bool)
          scan s
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
              join on ((t.a::int = t2.e::int and ROW($0) >= t.b::int and ROW($0) <= t2.f::int))
                scan t
                scan t2
          scan t2 -> z
    subquery $0:
      scan
        projection (max(x.e::int::int)::int -> col_1)
          scan t2 -> x
    ");
}

#[test]
fn a_component_over_one_join_operand_descends_below_the_join() {
    let plan = run_push_down(
        r#"SELECT * FROM (SELECT * FROM "t") AS "s" JOIN "t2" ON true
           WHERE (SELECT max("e") FROM "t2" AS "x") BETWEEN "s"."a" AND "s"."b""#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (s.a::int -> a, s.b::int -> b, s.c::int -> c, s.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on (true::bool)
          scan s
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
              selection ((ROW($0) >= t.a::int and ROW($0) <= t.b::int))
                scan t
          scan t2
    subquery $0:
      scan
        projection (max(x.e::int::int)::int -> col_1)
          scan t2 -> x
    ");
}

#[test]
fn a_component_stops_at_the_last_place_common_to_its_conjuncts() {
    // Alone, `$0 >= s.b` would reach `t`; with `$0 <= z.f` it stops at the
    // upper join's ON, the deepest place that sees both.
    let plan = run_push_down(
        r#"SELECT * FROM (SELECT * FROM "t" JOIN "t2" ON "t"."a" = "t2"."e") AS "s"
           JOIN "t2" AS "z" ON true
           WHERE (SELECT max("e") FROM "t2" AS "x") BETWEEN "s"."b" AND "z"."f""#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (s.a::int -> a, s.b::int -> b, s.c::int -> c, s.d::int -> d, s.e::int -> e, s.f::int -> f, s.g::int -> g, s.h::int -> h, z.e::int -> e, z.f::int -> f, z.g::int -> g, z.h::int -> h)
      selection (true::bool)
        join on ((ROW($0) >= s.b::int and ROW($0) <= z.f::int))
          scan s
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
              join on (t.a::int = t2.e::int)
                scan t
                scan t2
          scan t2 -> z
    subquery $0:
      scan
        projection (max(x.e::int::int)::int -> col_1)
          scan t2 -> x
    ");
}

#[test]
fn a_column_less_conjunct_moves_with_its_component() {
    // `$0 <= 10` reads no column but shares the body with `$0 >= t.a`.
    let plan = run_push_down(
        r#"SELECT * FROM "t" JOIN "t2" ON true
           WHERE (SELECT max("e") FROM "t2" AS "x") BETWEEN "t"."a" AND 10"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on (true::bool)
          scan t
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              selection ((ROW($0) >= t.a::int and ROW($0) <= 10::int))
                scan t
          scan t2
    subquery $0:
      scan
        projection (max(x.e::int::int)::int -> col_1)
          scan t2 -> x
    ");
}

#[test]
fn a_column_less_component_stays() {
    let plan = run_push_down(
        r#"SELECT * FROM "t" JOIN "t2" ON true
           WHERE (SELECT max("e") FROM "t2" AS "x") BETWEEN 1 AND 10"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection ((ROW($0) >= 1::int and ROW($0) <= 10::int))
        join on (true::bool)
          scan t
          scan t2
    subquery $0:
      scan
        projection (max(x.e::int::int)::int -> col_1)
          scan t2 -> x
    ");
}

#[test]
fn a_not_in_list_component_moves_whole_into_the_on() {
    // `NOT (x IN (b, c, f))` becomes three conjuncts over one body `x`; one
    // reads `t2`, so all three move into the ON.
    let plan = run_push_down(
        r#"SELECT * FROM "t" JOIN "t2" ON "t"."a" = "t2"."e"
           WHERE NOT ((SELECT max("e") FROM "t2" AS "x") IN ("t"."b", "t"."c", "t2"."f"))"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on ((t.a::int = t2.e::int and ROW($0) <> t.b::int and ROW($0) <> t.c::int and ROW($0) <> t2.f::int))
          scan t
          scan t2
    subquery $0:
      scan
        projection (max(x.e::int::int)::int -> col_1)
          scan t2 -> x
    ");
}

#[test]
fn a_not_in_list_component_stays_whole_above_a_left_join() {
    // `x <> t2.f` reads the nullable side and cannot leave the WHERE. Alone,
    // `x <> t.b` and `x <> t.c` would descend to `t`; sharing `x`, they stay.
    // Nothing moves, so EXPLAIN shows the WHERE as written, before the NOT is
    // pushed into the three conjuncts.
    let plan = run_push_down(
        r#"SELECT * FROM "t" LEFT JOIN "t2" ON "t"."a" = "t2"."e"
           WHERE NOT ((SELECT max("e") FROM "t2" AS "x") IN ("t"."b", "t"."c", "t2"."f"))"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (not (ROW($0) = t.b::int or ROW($0) = t.c::int or ROW($0) = t2.f::int))
        left join on (t.a::int = t2.e::int)
          scan t
          scan t2
    subquery $0:
      scan
        projection (max(x.e::int::int)::int -> col_1)
          scan t2 -> x
    ");
}

#[test]
fn a_large_component_relocates_each_conjunct_by_its_own_columns() {
    // Five conjuncts share the body `x`; each reads its own part of `v`, one
    // reads none. The projection reverses the columns, so every reference
    // must land on its own column of `t`.
    let plan = run_push_down(
        r#"SELECT * FROM (SELECT "d" AS "p", "c" AS "q", "b" AS "r", "a" AS "s" FROM "t") AS "v"
           JOIN "t2" ON true
           WHERE NOT ((SELECT max("e") FROM "t2" AS "x") IN ("v"."s", "v"."r", "v"."q", 10, "v"."p" + "v"."s"))"#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (v.p::int -> p, v.q::int -> q, v.r::int -> r, v.s::int -> s, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on (true::bool)
          scan v
            projection (t.d::int -> p, t.c::int -> q, t.b::int -> r, t.a::int -> s)
              selection ((ROW($0) <> t.a::int and ROW($0) <> t.b::int and ROW($0) <> t.c::int and ROW($0) <> 10::int and ROW($0) <> t.d::int + t.a::int))
                scan t
          scan t2
    subquery $0:
      scan
        projection (max(x.e::int::int)::int -> col_1)
          scan t2 -> x
    ");
}

#[test]
fn components_and_independent_conjuncts_keep_their_order_at_a_common_place() {
    // Two BETWEEN components with an independent OR between them all land on
    // `t`: there they keep the WHERE order.
    let plan = run_push_down(
        r#"SELECT * FROM "t" JOIN "t2" ON true
           WHERE (SELECT max("e") FROM "t2" AS "x") BETWEEN "t"."a" AND "t"."b"
             AND ("t"."c" = 1 OR "t"."d" = 2)
             AND (SELECT max("f") FROM "t2" AS "y") BETWEEN "t"."c" AND "t"."d""#,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on (true::bool)
          scan t
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              selection ((ROW($1) >= t.a::int and ROW($1) <= t.b::int and (t.c::int = 1::int or t.d::int = 2::int) and ROW($0) >= t.c::int and ROW($0) <= t.d::int))
                scan t
          scan t2
    subquery $0:
      scan
        projection (max(y.f::int::int)::int -> col_1)
          scan t2 -> y
    subquery $1:
      scan
        projection (max(x.e::int::int)::int -> col_1)
          scan t2 -> x
    ");
}

#[test]
fn a_shared_body_moved_into_the_on_gets_its_motion_there() {
    let plan = sql_to_ir_without_bind(
        r#"SELECT * FROM "t" JOIN "t2" ON true
           WHERE (SELECT max("e") FROM "t2" AS "x") BETWEEN "t"."a" AND "t2"."f""#,
        &[],
    )
    .optimize()
    .unwrap();
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection (true::bool)
        join on ((ROW($0) <= t2.f::int and ROW($0) >= t.a::int))
          scan t
          motion [policy: full, program: ReshardIfNeeded]
            projection (t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t2.bucket_id::int -> bucket_id)
              scan t2
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (max(max_1::int)::int -> col_1)
            motion [policy: full, program: ReshardIfNeeded]
              projection (max(x.e::int::int)::int -> max_1)
                scan t2 -> x
    ");
}

#[test]
fn an_unmovable_conjunct_pins_its_component_and_nothing_else() {
    // `$0 >= lower(t.a)` and `$0 <= 'z'` share the body; the OR is independent.
    let mut plan = sql_to_ir_without_bind(
        r#"SELECT * FROM "t" JOIN "t2" ON true
           WHERE CAST((SELECT max("e") FROM "t2" AS "x") AS text)
                 BETWEEN lower(CAST("t"."a" AS text)) AND 'z'
             AND ("t"."c" = 1 OR "t"."d" = 2)"#,
        &[],
    );

    // Mark `lower` volatile in the plan to exercise the movement guard:
    // `$0 >= lower(t.a)` must then stay in place.
    let Relational::Selection(Selection { filter, .. }) =
        plan.get_relation_node(top_where(&plan)).unwrap()
    else {
        unreachable!();
    };
    let calls = PostOrderWithFilter::new(
        |node| plan.nodes.expr_iter(node, false),
        |node| {
            matches!(
                plan.get_node(node),
                Ok(Node::Expression(Expression::ScalarFunction(_)))
            )
        },
        EXPR_CAPACITY,
    )
    .traverse_into_vec(*filter);
    let mut marked = 0;
    for call in calls {
        if let MutExpression::ScalarFunction(ScalarFunction {
            name,
            volatility_type,
            ..
        }) = plan.get_mut_expression_node(call).unwrap()
        {
            if name == "lower" {
                *volatility_type = VolatilityType::Volatile;
                marked += 1;
            }
        }
    }
    assert_eq!(marked, 1);

    let plan = push_down(plan);

    // Neither can `$0 <= 'z'`, which shares its body; the OR still descends.
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      selection ((ROW($0)::text >= lower(t.a::int::text)::text and ROW($0)::text <= 'z'::text))
        join on (true::bool)
          scan t
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              selection (t.c::int = 1::int or t.d::int = 2::int)
                scan t
          scan t2
    subquery $0:
      scan
        projection (max(x.e::int::int)::int -> col_1)
          scan t2 -> x
    ");
}

/// How the owners' subquery lists change when a clause moves. EXPLAIN shows
/// where each clause and body ends up but not the order of an owner's list,
/// so each test also checks that order directly.
mod subquery_lists {
    use super::*;

    #[test]
    fn a_new_filter_lists_a_copied_body_once() {
        // BETWEEN copies `x`: the moved conjuncts name x, y, x.
        let plan = run_push_down(
            r#"SELECT * FROM "t" JOIN "t2" ON true
               WHERE (SELECT max("e") FROM "t2" AS "x")
                     BETWEEN (SELECT max("f") FROM "t2" AS "y") + "t"."a" AND "t"."b""#,
        );
        assert_snapshot!(logical(&plan), @r"
        projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
          selection (true::bool)
            join on (true::bool)
              scan t
                projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
                  selection ((ROW($1) >= ROW($0) + t.a::int and ROW($1) <= t.b::int))
                    scan t
              scan t2
        subquery $0:
          scan
            projection (max(y.f::int::int)::int -> col_1)
              scan t2 -> y
        subquery $1:
          scan
            projection (max(x.e::int::int)::int -> col_1)
              scan t2 -> x
        ");
        let (owner, x) = listed_body(&plan, "x");
        let (y_owner, y) = listed_body(&plan, "y");
        assert_eq!(y_owner, owner);
        assert_eq!(subqueries_of(&plan, owner), vec![x, y]);
    }

    #[test]
    fn the_old_owner_drops_only_the_body_that_left() {
        // `a` and `b` read the nullable side and stay; `c`, listed between
        // them, leaves for `t`.
        let plan = run_push_down(
            r#"SELECT * FROM "t" LEFT JOIN "t2" ON "t"."a" = "t2"."e"
               WHERE "t2"."f" IN (SELECT "e" FROM "t2" AS "a")
                 AND "t"."b" IN (SELECT "e" FROM "t2" AS "c")
                 AND "t2"."g" IN (SELECT "e" FROM "t2" AS "b")"#,
        );
        assert_snapshot!(logical(&plan), @r"
        projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
          selection ((t2.f::int in ROW($2) and t2.g::int in ROW($1)))
            left join on (t.a::int = t2.e::int)
              scan t
                projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
                  selection (t.b::int in ROW($0))
                    scan t
              scan t2
        subquery $0:
          scan
            projection (c.e::int -> e)
              scan t2 -> c
        subquery $1:
          scan
            projection (b.e::int -> e)
              scan t2 -> b
        subquery $2:
          scan
            projection (a.e::int -> e)
              scan t2 -> a
        ");
        let (owner, a) = listed_body(&plan, "a");
        let (_, b) = listed_body(&plan, "b");
        assert_eq!(subqueries_of(&plan, owner), vec![a, b]);
        let (c_owner, c) = listed_body(&plan, "c");
        assert_eq!(subqueries_of(&plan, c_owner), vec![c]);
    }

    #[test]
    fn a_receiving_filter_keeps_its_list_and_appends_new_bodies() {
        // The OR joins the inner WHERE, which lists p, q: r and u follow
        // them in the order the OR names them.
        let plan = run_push_down(
            r#"SELECT * FROM (SELECT * FROM "t"
                              WHERE "t"."c" IN (SELECT "e" FROM "t2" AS "p")
                                AND "t"."d" IN (SELECT "e" FROM "t2" AS "q")) AS "s"
               WHERE "s"."a" IN (SELECT "e" FROM "t2" AS "r") OR "s"."b" IN (SELECT "e" FROM "t2" AS "u")"#,
        );
        assert_snapshot!(logical(&plan), @r"
        projection (s.a::int -> a, s.b::int -> b, s.c::int -> c, s.d::int -> d)
          selection (true::bool)
            scan s
              projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
                selection ((t.c::int in ROW($3) and t.d::int in ROW($2) and (t.a::int in ROW($1) or t.b::int in ROW($0))))
                  scan t
        subquery $0:
          scan
            projection (u.e::int -> e)
              scan t2 -> u
        subquery $1:
          scan
            projection (r.e::int -> e)
              scan t2 -> r
        subquery $2:
          scan
            projection (q.e::int -> e)
              scan t2 -> q
        subquery $3:
          scan
            projection (p.e::int -> e)
              scan t2 -> p
        ");
        let (inner, p) = listed_body(&plan, "p");
        let bodies = ["q", "r", "u"].map(|alias| listed_body(&plan, alias).1);
        assert_eq!(subqueries_of(&plan, inner), [&[p][..], &bodies].concat());
    }

    #[test]
    fn a_receiving_filter_without_new_bodies_keeps_its_list() {
        let plan = run_push_down(
            r#"SELECT * FROM (SELECT * FROM "t"
                              WHERE "t"."c" IN (SELECT "e" FROM "t2" AS "p")
                                AND "t"."d" IN (SELECT "e" FROM "t2" AS "q")) AS "s"
               WHERE "s"."a" = 1 OR "s"."b" = 2"#,
        );
        assert_snapshot!(logical(&plan), @r"
        projection (s.a::int -> a, s.b::int -> b, s.c::int -> c, s.d::int -> d)
          selection (true::bool)
            scan s
              projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
                selection ((t.c::int in ROW($1) and t.d::int in ROW($0) and (t.a::int = 1::int or t.b::int = 2::int)))
                  scan t
        subquery $0:
          scan
            projection (q.e::int -> e)
              scan t2 -> q
        subquery $1:
          scan
            projection (p.e::int -> e)
              scan t2 -> p
        ");
        let (inner, p) = listed_body(&plan, "p");
        let (_, q) = listed_body(&plan, "q");
        assert_eq!(subqueries_of(&plan, inner), vec![p, q]);
    }
}

#[test]
fn a_clause_sharing_one_reference_node_moves_down() {
    // The expanded IN list repeats its needle `t1.c` through one shared
    // reference node; the moved copy rewrites it once, not once per parent.
    let plan = push_down(sql_to_ir_without_bind(
        r#"SELECT t1.c AS c0, t1.b AS c1 FROM t2 AS t0 JOIN t AS t1 ON t1.b = t0.e
           WHERE t1.c IN ($1, t1.c, t1.a, $2, t1.c) OR t1.b >= t1.a
           ORDER BY c0 ASC, c1 DESC NULLS FIRST"#,
        &[DerivedType::new(UnrestrictedType::Integer); 2],
    ));
    assert_snapshot!(logical(&plan), @r"
    projection (c0::int, c1::int)
      order by (c0::int asc, not c1::int is null, c1::int desc)
        scan
          projection (t1.c::int -> c0, t1.b::int -> c1)
            selection (true::bool)
              join on (t0.e::int = t1.b::int)
                scan t2 -> t0
                scan t1
                  projection (t1.a::int -> a, t1.b::int -> b, t1.c::int -> c, t1.d::int -> d, t1.bucket_id::int -> bucket_id)
                    selection (t1.c::int = $1::int or t1.c::int = t1.c::int or t1.c::int = t1.a::int or t1.c::int = $2::int or t1.c::int = t1.c::int or t1.b::int >= t1.a::int)
                      scan t -> t1
    ");
}

#[test]
fn a_conjunct_repeated_through_one_node_is_dropped_once() {
    // `x NOT IN (false, false)` becomes `x <> false and x <> false`, and both
    // `x <> false` fold to the one node `x`: the join condition lists it twice.
    let plan = run_push_down(
        r#"SELECT * FROM (VALUES (1)) AS v JOIN t ON (v."COLUMN_1" IS NOT NULL) NOT IN (FALSE, FALSE)"#,
    );
    assert_snapshot!(logical(&plan), @r#"
    projection (v."COLUMN_1"::int -> "COLUMN_1", t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      join on (true::bool)
        scan v
          projection (v."COLUMN_1"::int -> "COLUMN_1")
            selection ((not v."COLUMN_1"::int is null and not v."COLUMN_1"::int is null))
              scan v
                values
                  value ROW(1::int)
        scan t
    "#);
}

// A filter on a table function whose arguments reference the other join
// operand stays above the join: a carrier subquery could not see that
// operand.
#[test]
fn lateral_function_keeps_filter_above_join() {
    let plan = run_push_down(
        r#"SELECT g.value FROM "t" JOIN generate_series(1, "t"."c") g ON "t"."a" = g.value WHERE g.value > 1"#,
    );
    assert_snapshot!(logical(&plan), @"
    projection (g.value::int -> value)
      selection (g.value::int > 1::int)
        join on (t.a::int = g.value::int)
          scan t
          scan generate_series(1::int, t.c::int::int) -> g
    ");
}

// A table function without such references gets the carrier like a table.
#[test]
fn function_takes_filter_in_carrier() {
    let plan = run_push_down(
        r#"SELECT g.value FROM "t" JOIN generate_series(1, 10) g ON "t"."a" = g.value WHERE g.value > 1"#,
    );
    assert_snapshot!(logical(&plan), @"
    projection (g.value::int -> value)
      selection (true::bool)
        join on (t.a::int = g.value::int)
          scan t
          scan g
            projection (g.value::int -> value)
              selection (g.value::int > 1::int)
                scan generate_series(1::int, 10::int) -> g
    ");
}
