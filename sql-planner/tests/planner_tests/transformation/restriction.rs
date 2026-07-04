use sql::helpers::sql_to_ir_without_bind;
use sql::ir::node::expression::Expression;
use sql::ir::node::{BoolExpr, NodeId};
use sql::ir::operator::Bool;
use sql::ir::transformation::restriction::{Restriction, Restrictions};
use sql::ir::transformation::Stage;
use sql::ir::Plan;

/// Build the input the restriction pass sees in production (`IN` already
/// `OR`-expanded, `(a, b) = (1, 2)` already split into per-column equalities)
/// via [`Plan::optimize_before`], then run the restriction pass itself and
/// return the restrictions it stored on the plan.
fn restrictions(query: &str) -> (Plan, Restrictions) {
    let plan = sql_to_ir_without_bind(query, &[]);
    let top = plan.get_top().unwrap();
    let plan = plan
        .optimize_before(top, Stage::Restrictions)
        .unwrap()
        .analyze_restrictions_in_subtree(top)
        .unwrap();
    let restrictions = plan.restrictions.clone().unwrap();
    (plan, restrictions)
}

/// The single restriction for queries that have exactly one filter-bearing node.
fn only(r: &Restrictions) -> &Restriction {
    assert_eq!(r.by_rel.len(), 1, "expected exactly one restriction");
    r.by_rel.values().next().unwrap()
}

/// The boolean operator of a clause node, if it is a boolean expression.
fn clause_op(plan: &Plan, clause: NodeId) -> Option<Bool> {
    match plan.get_expression_node(clause).unwrap() {
        Expression::Bool(BoolExpr { op, .. }) => Some(*op),
        _ => None,
    }
}

#[test]
fn single_equality_is_one_clause() {
    //  A bare equality is one clause referencing one column source `(node, pos)`.
    let (plan, r) = restrictions(r#"SELECT "a" FROM "t" WHERE "b" = 1"#);
    let restr = only(&r);
    assert_eq!(restr.clauses().len(), 1);
    let c = &restr.clauses()[0];
    assert_eq!(clause_op(&plan, c.clause()), Some(Bool::Eq));
    assert_eq!(c.slots().len(), 1);
}

#[test]
fn same_column_or_is_one_clause() {
    //  A same-column OR is ONE clause (PG keeps `b = 1 OR b = 2` as one
    //    RestrictInfo). No value-set is extracted here.
    let (plan, r) = restrictions(r#"SELECT "a" FROM "t" WHERE "b" = 1 OR "b" = 2"#);
    let restr = only(&r);
    assert_eq!(restr.clauses().len(), 1);
    assert_eq!(
        clause_op(&plan, restr.clauses()[0].clause()),
        Some(Bool::Or)
    );
}

#[test]
fn in_is_one_clause() {
    //  `IN` is OR-expanded by the prefix pass, so it is still ONE clause.
    let (plan, r) = restrictions(r#"SELECT "a" FROM "t" WHERE "b" IN (1, 2, 3)"#);
    let restr = only(&r);
    assert_eq!(restr.clauses().len(), 1);
    assert_eq!(
        clause_op(&plan, restr.clauses()[0].clause()),
        Some(Bool::Or)
    );
}

#[test]
fn cross_column_or_is_one_clause() {
    //  A cross-column OR is one clause referencing two column sources (b and c).
    let (_, r) = restrictions(r#"SELECT "a" FROM "t" WHERE "b" = 1 OR "c" = 2"#);
    let restr = only(&r);
    assert_eq!(restr.clauses().len(), 1);
    let rels = restr.clauses()[0].slots();
    assert_eq!(rels.len(), 2, "two columns -> two (node, pos)");
    assert_eq!(rels[0].rel_id, rels[1].rel_id, "both on the same node");
}

#[test]
fn and_splits_into_clauses() {
    //  The top-level `AND` is split into one clause per conjunct.
    let (_, r) = restrictions(r#"SELECT "a" FROM "t" WHERE "b" = 1 AND "c" = 2"#);
    assert_eq!(only(&r).clauses().len(), 2);
}

#[test]
fn same_column_and_is_two_clauses() {
    //  `a = 1 AND a = 2` is TWO clauses -- the list never merges same-column
    //    conjuncts or detects contradiction (that is the EC layer's job).
    let (_, r) = restrictions(r#"SELECT "a" FROM "t" WHERE "b" = 1 AND "b" = 2"#);
    assert_eq!(only(&r).clauses().len(), 2);
}

#[test]
fn nested_and_flattens() {
    //  A nested `AND` chain flattens to one clause per leaf conjunct.
    let (_, r) = restrictions(r#"SELECT "a" FROM "t" WHERE "b" = 1 AND "c" = 2 AND "d" = 3"#);
    assert_eq!(only(&r).clauses().len(), 3);
}

#[test]
fn composite_key_splits_into_two_clauses() {
    //  A composite key `(b, c) = (1, 2)` is split into `b = 1 AND c = 2`, so it
    //    emerges as two clauses with no special code.
    let (_, r) = restrictions(r#"SELECT "a" FROM "t" WHERE ("b", "c") = (1, 2)"#);
    assert_eq!(only(&r).clauses().len(), 2);
}

#[test]
fn non_equality_is_a_clause() {
    //  A non-equality conjunct is just another clause: `b = 1 AND c > 5` -> two.
    let (plan, r) = restrictions(r#"SELECT "a" FROM "t" WHERE "b" = 1 AND "c" > 5"#);
    let restr = only(&r);
    assert_eq!(restr.clauses().len(), 2);
    let ops: Vec<_> = restr
        .clauses()
        .iter()
        .map(|c| clause_op(&plan, c.clause()))
        .collect();
    assert!(ops.contains(&Some(Bool::Eq)));
    assert!(ops.contains(&Some(Bool::Gt)));
}

#[test]
fn null_equality_is_a_clause() {
    //  `col = NULL` is just a clause -- the list does not special-case NULL.
    let (plan, r) = restrictions(r#"SELECT "a" FROM "t" WHERE "b" = NULL"#);
    let restr = only(&r);
    assert_eq!(restr.clauses().len(), 1);
    assert_eq!(
        clause_op(&plan, restr.clauses()[0].clause()),
        Some(Bool::Eq)
    );
}

#[test]
fn negated_equality_is_one_clause() {
    //  `NOT (b = 1)` is rewritten to `b <> 1` by `push_down_not`.
    let (plan, r) = restrictions(r#"SELECT "a" FROM "t" WHERE NOT ("b" = 1)"#);
    let restr = only(&r);
    assert_eq!(restr.clauses().len(), 1);
    assert_eq!(
        clause_op(&plan, restr.clauses()[0].clause()),
        Some(Bool::NotEq)
    );
}

#[test]
fn surviving_not_in_with_sibling() {
    //  A surviving `NOT (b IN (...))` plus a sibling equality is two clauses.
    let (_, r) =
        restrictions(r#"SELECT "a" FROM "t" WHERE "c" = 5 AND NOT ("b" IN (SELECT "b" FROM "t"))"#);
    assert_eq!(only(&r).clauses().len(), 2);
}

#[test]
fn in_subquery_visits_both_nodes() {
    //  `b IN (subquery)` is one clause; the subquery's own WHERE produces its own
    //     restriction node (the pass visits every Selection in the subtree).
    let (_, r) =
        restrictions(r#"SELECT "a" FROM "t" WHERE "b" IN (SELECT "b" FROM "t" WHERE "c" = 2)"#);
    assert_eq!(r.by_rel.len(), 2);
    for restr in r.by_rel.values() {
        assert_eq!(restr.clauses().len(), 1);
    }
}

#[test]
fn pin_and_in_subquery_are_two_clauses() {
    //  A pin alongside an `IN (subquery)` is two clauses on the outer node.
    let (_, r) =
        restrictions(r#"SELECT "a" FROM "t" WHERE "b" = 1 AND "b" IN (SELECT "b" FROM "t")"#);
    assert_eq!(only(&r).clauses().len(), 2);
}

#[test]
fn in_subquery_clause_collects_subquery() {
    //  `b IN (subquery)` records the subquery node on the clause so it can be
    //     pushed down together with the clause.
    let (_, r) = restrictions(r#"SELECT "a" FROM "t" WHERE "b" IN (SELECT "b" FROM "t")"#);
    let restr = only(&r);
    let c = &restr.clauses()[0];
    assert_eq!(
        c.subqueries().len(),
        1,
        "the IN clause references one subquery"
    );
}

#[test]
fn plain_clause_has_no_subqueries() {
    //  A clause with no subquery references collects an empty subquery list.
    let (_, r) = restrictions(r#"SELECT "a" FROM "t" WHERE "b" = 1"#);
    assert!(only(&r).clauses()[0].subqueries().is_empty());
}

#[test]
fn group_by_having_only_where_is_analyzed() {
    //  GROUP BY + HAVING: only the WHERE (a Selection) is analyzed; HAVING is a
    //     separate node and is not visited.
    let (_, r) = restrictions(
        r#"SELECT "b", count("a") FROM "t" WHERE "b" = 1 GROUP BY "b" HAVING count("a") > 5"#,
    );
    assert_eq!(r.by_rel.len(), 1);
    assert_eq!(only(&r).clauses().len(), 1);
}

#[test]
fn inner_join_condition_relations() {
    //  An INNER join condition splits into clauses; `t.a = t1.b` references two
    //     column sources (the two sides), `t1.b = 2` references one. No join/base
    //     classification yet -- that comes after the relations are pushed down.
    let (_, r) = restrictions(
        r#"SELECT "t"."a"
           FROM "t"
           JOIN "t1_2" AS "t1" ON "t"."a" = "t1"."b" AND "t1"."b" = 2"#,
    );
    let restr = only(&r);
    assert_eq!(restr.clauses().len(), 2);
    let two = restr
        .clauses()
        .iter()
        .filter(|c| c.slots().len() == 2)
        .count();
    let one = restr
        .clauses()
        .iter()
        .filter(|c| c.slots().len() == 1)
        .count();
    assert_eq!(two, 1, "`t.a = t1.b` references two columns");
    assert_eq!(one, 1, "`t1.b = 2` references one column");
}

#[test]
fn nested_joins_and_left_join() {
    //  Nested INNER JOIN + LEFT JOIN + WHERE: the INNER join condition and the
    //     WHERE each get a restriction; the LEFT JOIN condition is skipped.
    let (_, r) = restrictions(
        r#"SELECT "t"."a"
           FROM "t"
           JOIN "t1_2" AS "t1" ON "t"."a" = "t1"."b"
           LEFT JOIN "t1_2" AS "t2" ON "t"."b" = "t2"."b"
           WHERE "t"."b" = 1"#,
    );
    assert_eq!(
        r.by_rel.len(),
        2,
        "INNER join + WHERE; LEFT join is skipped"
    );
}

#[test]
fn left_join_condition_is_skipped() {
    //  A LEFT-JOIN-only query produces no restriction (its ON is skipped).
    let (_, r) = restrictions(
        r#"SELECT "t"."a"
           FROM "t"
           LEFT JOIN "t1_2" AS "t1" ON "t"."a" = "t1"."b" AND "t1"."b" = 2"#,
    );
    assert!(r.by_rel.is_empty());
}

#[test]
fn cleared_after_optimize() {
    //  Parity guard: after a full optimize the field is cleared, proving step 1
    //     changes nothing downstream.
    use sql::helpers::sql_to_ir;
    let plan = sql_to_ir(r#"SELECT "a" FROM "t" WHERE "b" = 1"#, vec![])
        .optimize()
        .unwrap();
    assert!(plan.restrictions.is_none());
}
