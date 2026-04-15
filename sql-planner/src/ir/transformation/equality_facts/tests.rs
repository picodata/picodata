use crate::ir::transformation::equality_facts::{EqualityAnalysis, EqualityFacts};
use crate::ir::transformation::helpers::sql_to_ir_without_bind;
use crate::ir::value::Value;
use crate::ir::Plan;

fn equalities_facts(query: &str, params: Vec<Value>) -> (Plan, EqualityFacts) {
    let params_types: Vec<_> = params.iter().map(|v| v.get_type()).collect();
    let plan = sql_to_ir_without_bind(query, &params_types);
    let facts = EqualityAnalysis::get_equality_facts(&plan, plan.top.unwrap()).unwrap();
    (plan, facts)
}

#[test]
fn equality_facts_1() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = 2 AND "c" = 1 OR "d" = 1"#;

    let (_, equalities_facts) = equalities_facts(input, vec![]);

    // we have 2 DNF chains:
    // 1) "a" = 1 AND "b" = 2 AND "c" = 1
    // 2) "d" = 1"
    // They don't have intersection, so we have no facts about constant values.
    assert!(equalities_facts.class_const.iter().all(|v| v.is_none()));
}

#[test]
fn equality_facts_2() {
    let input = r#"SELECT "a", "b" FROM "t"
    WHERE "a" = NULL AND NULL = "b""#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    // Both predicates compare against NULL.  Since `x = NULL` evaluates to
    // UNKNOWN (never TRUE), the WHERE clause is unsatisfiable — no row can
    // match.  No equalities can be derived: a and b stay in separate
    // equivalence classes and no column has a known constant.
    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    assert_ne!(a, b);

    assert!(equalities_facts.class_const.iter().all(|v| v.is_none()));
}

#[test]
fn equality_facts_3() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = null AND "a" = null"#;
    let (_, equalities_facts) = equalities_facts(input, vec![]);

    // Any `= NULL` conjunct makes the entire AND-chain unsatisfiable (that
    // conjunct is UNKNOWN, so the whole AND is UNKNOWN and no row matches).
    // Even though `"a" = 1` alone would bind a to 1, the surrounding clause
    // is unsatisfiable, so no equality facts are derived.
    assert!(equalities_facts.class_const.iter().all(|v| v.is_none()));
}

#[test]
fn equality_facts_4() {
    let input = r#"SELECT "a", "b" FROM "t"
    WHERE "a" = 1 AND "b" = 1"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    // Both "a" and "b" equal the same literal 1, so they are transitively
    // equal to each other and land in the same equivalence class with const = 1.
    let top_id = plan.top.unwrap();
    let a = equalities_facts.const_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.const_of_slot(top_id, 1).unwrap();
    assert_eq!(a, b);
    assert_eq!(a, &Value::from(1));
}

#[test]
fn equality_facts_5() {
    let input = r#"SELECT "a", "b", "c", "d" FROM "t"
    WHERE "a" = 1 AND "b" = 1 AND "c" = 2 AND "d" = 2"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    // Two independent constant groups: {a, b} = 1 and {c, d} = 2.
    // Columns within each group share the same equivalence class; columns
    // across groups do not.
    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    let c = equalities_facts.class_of_slot(top_id, 2).unwrap();
    let d = equalities_facts.class_of_slot(top_id, 3).unwrap();
    assert_eq!(a, b);
    assert_eq!(c, d);
    assert_ne!(a, c);

    let a = equalities_facts.const_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.const_of_slot(top_id, 1).unwrap();
    let c = equalities_facts.const_of_slot(top_id, 2).unwrap();
    let d = equalities_facts.const_of_slot(top_id, 3).unwrap();
    assert_eq!(a, b);
    assert_eq!(a, &Value::from(1));
    assert_eq!(c, d);
    assert_eq!(c, &Value::from(2));
}

#[test]
fn equality_facts_6() {
    let input = r#"SELECT "t"."a", "t"."b", "t1"."a", "t1"."b"
    FROM "t" join "t1_2" as "t1"
        ON "t1"."a" = 1 AND "t"."a" = 1 AND "t1"."b" = 2 AND "t"."b" = 2"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    // An INNER JOIN's ON condition behaves like a WHERE for fact derivation.
    // All four constants (t.a = 1, t.b = 2, t1.a = 1, t1.b = 2) flow to the
    // top SELECT's output; t.a and t1.a land in the same class, as do t.b and t1.b.
    let top_id = plan.top.unwrap();
    let a_0 = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b_0 = equalities_facts.class_of_slot(top_id, 1).unwrap();
    let a_1 = equalities_facts.class_of_slot(top_id, 2).unwrap();
    let b_1 = equalities_facts.class_of_slot(top_id, 3).unwrap();
    assert_eq!(a_0, a_1);
    assert_eq!(b_0, b_1);
    assert_ne!(a_0, b_0);

    let a_0 = equalities_facts.const_of_slot(top_id, 0).unwrap();
    let b_0 = equalities_facts.const_of_slot(top_id, 1).unwrap();
    let a_1 = equalities_facts.const_of_slot(top_id, 2).unwrap();
    let b_1 = equalities_facts.const_of_slot(top_id, 3).unwrap();
    assert_eq!(a_0, a_1);
    assert_eq!(a_1, &Value::from(1));
    assert_eq!(b_0, b_1);
    assert_eq!(b_1, &Value::from(2));
}

#[test]
fn equality_facts_7() {
    let input = r#"SELECT "t"."a", "t"."b", "t1"."a", "t1"."b"
    FROM "t" left join "t1_2" as "t1"
        ON "t1"."a" = 1 AND "t"."a" = 1 AND "t1"."b" = 2 AND "t"."b" = 2"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    // Condition in outer join doesn't affect equality facts
    let top_id = plan.top.unwrap();
    let a_0 = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b_0 = equalities_facts.class_of_slot(top_id, 1).unwrap();
    let a_1 = equalities_facts.class_of_slot(top_id, 2).unwrap();
    let b_1 = equalities_facts.class_of_slot(top_id, 3).unwrap();
    assert_ne!(a_0, a_1);
    assert_ne!(b_0, b_1);

    assert!(equalities_facts.class_const.iter().all(|v| v.is_none()));
}

#[test]
fn equality_facts_8() {
    let input = r#"SELECT "t"."a", "t1"."a", "t"."b", "t1"."b"
    FROM "t" join "t1_2" as "t1"
        ON "t1"."a" = $1 AND "t"."a" = $1 AND "t1"."b" = $2 AND "t"."b" = $2"#;
    let (plan, equalities_facts) =
        equalities_facts(input, vec![Value::Integer(1), Value::Integer(2)]);

    // $1 appears on both sides of the join condition for column "a", and $2
    // for column "b".  The same parameter on both sides bridges the two
    // columns into one equivalence class each (t.a ≡ t1.a, t.b ≡ t1.b).
    // No constant is attached to the class — a parameter is not a literal,
    // so the class cannot claim a compile-time known value.
    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let a_1 = equalities_facts.class_of_slot(top_id, 1).unwrap();
    assert_eq!(a, a_1);

    let b = equalities_facts.class_of_slot(top_id, 2).unwrap();
    let b_1 = equalities_facts.class_of_slot(top_id, 3).unwrap();
    assert_eq!(b, b_1);

    assert_ne!(a, b);
}

#[test]
fn equality_facts_9() {
    let input = r#"SELECT "a", "b" FROM "t"
    WHERE "a" = $1 AND "b" = $2"#;
    let (plan, equalities_facts) =
        equalities_facts(input, vec![Value::Integer(1), Value::Integer(1)]);

    // $1 and $2 are different parameter indices, so "a" = $1 and "b" = $2
    // place a and b into separate equivalence classes even when the runtime
    // values happen to be equal.  Equality classes are derived from the query
    // structure, not the bound values.
    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    assert_ne!(a, b);
}

// --- Column-to-column equality ---

#[test]
fn equality_facts_col_eq_col_basic() {
    // "a" = "b" → a and b are in the same class, no const binding
    let input = r#"SELECT "a", "b" FROM "t" WHERE "a" = "b""#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    assert_eq!(a, b);
    // No constant is known for this class
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

#[test]
fn equality_facts_col_eq_col_transitivity() {
    // a=b AND b=c → all three in the same class
    let input = r#"SELECT "a", "b", "c" FROM "t" WHERE "a" = "b" AND "b" = "c""#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    let c = equalities_facts.class_of_slot(top_id, 2).unwrap();
    assert_eq!(a, b);
    assert_eq!(b, c);
}

#[test]
fn equality_facts_col_eq_col_with_const() {
    // a=b AND a=1 → a and b in same class with const=1
    let input = r#"SELECT "a", "b" FROM "t" WHERE "a" = "b" AND "a" = 1"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    assert_eq!(a, b);
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 0).unwrap(),
        &Value::from(1)
    );
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 1).unwrap(),
        &Value::from(1)
    );
}

// --- OR intersection ---

#[test]
fn equality_facts_or_col_eq_col_survives() {
    // (a=b AND c=1) OR (a=b AND d=2) → a=b survives, c=1 and d=2 do not
    let input = r#"SELECT "a", "b", "c", "d" FROM "t"
    WHERE "a" = "b" AND "c" = 1 OR "a" = "b" AND "d" = 2"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    assert_eq!(a, b);
    assert!(equalities_facts.const_of_slot(top_id, 2).is_none());
    assert!(equalities_facts.const_of_slot(top_id, 3).is_none());
}

// --- Dead OR branch (conflict → branch is skipped, not intersected) ---

#[test]
fn equality_facts_dead_or_branch_skipped() {
    // (a=1 AND a=2) is a contradiction → that branch is None and skipped.
    // The second branch (b=3) is the only live one → b=3 survives.
    // If intersection were used instead of skip, the result would be empty.
    let input = r#"SELECT "a", "b" FROM "t"
    WHERE ("a" = 1 AND "a" = 2) OR "b" = 3"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 1).unwrap(),
        &Value::from(3)
    );
}

#[test]
fn equality_facts_all_or_branches_dead() {
    // All branches are contradictions → no facts at all
    let input = r#"SELECT "a", "b" FROM "t"
    WHERE ("a" = 1 AND "a" = 2) OR ("b" = 1 AND "b" = 2)"#;
    let (_, equalities_facts) = equalities_facts(input, vec![]);

    assert!(equalities_facts.class_const.iter().all(|v| v.is_none()));
}

#[test]
fn equality_facts_dead_null_branch() {
    // First DNF branch is dead (a = NULL makes it unsatisfiable);
    // the surviving branch b = 5 is the only live one → b = 5 is derived
    let input = r#"SELECT "a", "b" FROM "t"
    WHERE a = NULL and b = 1 or b = 5"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.const_of_slot(top_id, 0);
    let b = equalities_facts.const_of_slot(top_id, 1);
    assert!(a.is_none());
    assert_eq!(b, Some(&Value::from(5)));
}

#[test]
fn equality_facts_dead_branch_with_null() {
    // First DNF branch is dead (NULL makes it unsatisfiable);
    // the surviving branch b = 5 is the only live one → b = 5 is derived
    let input = r#"SELECT "a", "b" FROM "t"
    WHERE NULL and b = 1 or b = 5"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.const_of_slot(top_id, 0);
    let b = equalities_facts.const_of_slot(top_id, 1);
    assert!(a.is_none());
    assert_eq!(b, Some(&Value::from(5)));
}

#[test]
fn equality_facts_dead_branch_with_false() {
    // First DNF branch is dead (false makes it unsatisfiable);
    // the surviving branch b = 5 is the only live one → b = 5 is derived
    let input = r#"SELECT "a", "b" FROM "t"
    WHERE false and b = 1 or b = 5"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.const_of_slot(top_id, 0);
    let b = equalities_facts.const_of_slot(top_id, 1);
    assert!(a.is_none());
    assert_eq!(b, Some(&Value::from(5)));
}

// --- Row equality ---

#[test]
fn equality_facts_row_eq_row() {
    // (a, b) = (c, d) → two independent classes: {a,c} and {b,d}
    let input = r#"SELECT "a", "b", "c", "d" FROM "t"
    WHERE ("a", "b") = ("c", "d")"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    let c = equalities_facts.class_of_slot(top_id, 2).unwrap();
    let d = equalities_facts.class_of_slot(top_id, 3).unwrap();
    assert_eq!(a, c);
    assert_eq!(b, d);
    assert_ne!(a, b);
}

#[test]
fn equality_facts_row_eq_const_row() {
    // (a, b) = (1, 2) → a=1, b=2 in separate classes
    let input = r#"SELECT "a", "b" FROM "t" WHERE ("a", "b") = (1, 2)"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 0).unwrap(),
        &Value::from(1)
    );
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 1).unwrap(),
        &Value::from(2)
    );
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    assert_ne!(a, b);
}

// --- Parameters ---

#[test]
fn equality_facts_same_param_bridges_columns() {
    // a=$1 AND b=$1 → a and b are in the same class (transitive through the
    // shared parameter), but the class has no known constant because $1 is
    // a parameter, not a literal.
    let input = r#"SELECT "a", "b" FROM "t" WHERE "a" = $1 AND "b" = $1"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![Value::Integer(42)]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    assert_eq!(a, b);
    // Parameter is not a literal, so the class carries no known constant.
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
    assert!(equalities_facts.const_of_slot(top_id, 1).is_none());
}

#[test]
fn equality_facts_param_blocks_const_propagation() {
    // a = $1 AND a = 1: the equivalence class for "a" contains both a
    // parameter and a literal.  Since $1 is not a compile-time value, the
    // class is not bound to the literal — no constant is attached to "a".
    let input = r#"SELECT "a" FROM "t" WHERE "a" = $1 AND "a" = 1"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![Value::Integer(1)]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

// --- Simple vs. computed projection ---

#[test]
fn equality_facts_safe_projection_passthrough() {
    // A subquery that simply re-selects its columns preserves equality
    // facts: the inner WHERE's `a = 1` is visible on the outer SELECT's output.
    let input = r#"SELECT "a", "b" FROM (SELECT "a", "b" FROM "t" WHERE "a" = 1)"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 0).unwrap(),
        &Value::from(1)
    );
}

#[test]
fn equality_facts_computed_projection_breaks_chain() {
    // The projected expression "a" + 1 is a computed value, not a plain
    // column reference, so the outer "x" is a new column unrelated to "a".
    // The inner fact `a = 1` cannot be restated as `x = anything`.
    let input = r#"SELECT "x" FROM (SELECT "a" + 1 as "x" FROM "t" WHERE "a" = 1)"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

// --- Inner JOIN column-to-column ---

#[test]
fn equality_facts_inner_join_col_eq_col() {
    // INNER JOIN ON t.a = t1.b → the two output slots are in the same class
    let input = r#"SELECT "t"."a", "t1"."b"
    FROM "t" JOIN "t1_2" AS "t1" ON "t"."a" = "t1"."b""#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    assert_eq!(a, b);
}

#[test]
fn equality_facts_inner_join_transitivity_through_join() {
    // t.a = t1.b AND t1.b = t2.c → all three in the same class
    let input = r#"SELECT "t"."a", "t1"."b", "t2"."c"
    FROM "t"
    JOIN "t" AS "t1" ON "t"."a" = "t1"."b"
    JOIN "t" AS "t2" ON "t1"."b" = "t2"."c""#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    let c = equalities_facts.class_of_slot(top_id, 2).unwrap();
    assert_eq!(a, b);
    assert_eq!(b, c);
}

// --- Subquery isolation ---

#[test]
fn equality_facts_subquery_is_isolated() {
    // Facts derived inside a subquery must not leak into the outer plan.
    // The outer column "a" should not get const=1 just because "b"=1 inside.
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = (SELECT "b" FROM "t1_2" WHERE "b" = 1)"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    // Outer "a" has no const fact (the subquery result is opaque)
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

#[test]
fn equality_facts_same_subqueries_are_isolated() {
    // Same subquery twice: the first have constant for 'a', the second doesn't.
    // Test checks that the first subquery is not leaked into the second.
    let input = r#"SELECT l.a, r.a
        FROM (SELECT a FROM t) AS l
        JOIN (SELECT a FROM t) AS r ON true
        WHERE l.a = 5;"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 0),
        Some(&Value::from(5))
    ); // l.a
    assert_eq!(equalities_facts.const_of_slot(top_id, 1), None); // r.a
}

// --- Self-equality ---

#[test]
fn equality_facts_self_equality_is_noop() {
    // "a" = "a" — both sides are the same slot, union-find is idempotent, no crash
    let input = r#"SELECT "a" FROM "t" WHERE "a" = "a""#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    // No const fact, but also no panic
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

// --- Non-equality predicates ---

#[test]
fn equality_facts_non_eq_predicate_generates_no_fact() {
    // "a" > 1 is not an equality → no facts at all
    let input = r#"SELECT "a" FROM "t" WHERE "a" > 1"#;
    let (_, equalities_facts) = equalities_facts(input, vec![]);

    assert!(equalities_facts.class_const.iter().all(|v| v.is_none()));
}

// --- Projection column remapping ---

#[test]
fn equality_facts_projection_reorders_columns() {
    // The SELECT list reorders the columns relative to the table.  The fact
    // `a = 1` must follow column "a" to its new output position (slot 1),
    // not stay at slot 0 (now "b").
    let input = r#"SELECT "b", "a" FROM "t" WHERE "a" = 1"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none()); // b
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 1).unwrap(),
        &Value::from(1)
    ); // a
}

// --- Stacked selections ---

#[test]
fn equality_facts_nested_selections_merge_facts() {
    // Two Selection nodes stacked: inner adds b=2, outer adds a=1.
    // Both facts must appear on the outermost output.
    let input = r#"SELECT "a", "b" FROM (SELECT "a", "b" FROM "t" WHERE "b" = 2) WHERE "a" = 1"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 0).unwrap(),
        &Value::from(1)
    ); // a
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 1).unwrap(),
        &Value::from(2)
    ); // b
}

// --- Long transitivity chain ---

#[test]
fn equality_facts_long_transitivity_chain() {
    // a=b AND b=c AND c=1 — const must propagate through the full chain to a and b.
    let input = r#"SELECT "a", "b", "c" FROM "t" WHERE "a" = "b" AND "b" = "c" AND "c" = 1"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    let c = equalities_facts.class_of_slot(top_id, 2).unwrap();
    assert_eq!(a, b);
    assert_eq!(b, c);
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 0).unwrap(),
        &Value::from(1)
    );
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 1).unwrap(),
        &Value::from(1)
    );
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 2).unwrap(),
        &Value::from(1)
    );
}

// --- DISTINCT breaks propagation ---

#[test]
fn equality_facts_distinct_breaks_propagation() {
    // SELECT DISTINCT reshapes the result set (duplicates collapse), so a
    // fact about an input row's value does not translate to a fact about
    // the DISTINCT output.  The inner `a = 1` must not reach the outer output.
    let input = r#"SELECT DISTINCT "a" FROM "t" WHERE "a" = 1"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

// --- LEFT JOIN: left-side WHERE facts still flow, right-side join condition does not ---

#[test]
fn equality_facts_left_join_left_where_flows_right_join_does_not() {
    // WHERE t.a = 1 sits above the LEFT JOIN, so t.a = 1 holds for every
    // row of the final result and must surface on the outer output.
    // The ON condition on the right side (t2.b = 1) does NOT: under a LEFT
    // JOIN, unmatched left rows produce NULL for t2.b, so t2.b need not
    // equal 1 in the output.
    let input = r#"SELECT "t"."a", "t2"."b"
    FROM "t" LEFT JOIN "t1_2" AS "t2" ON "t2"."b" = 1
    WHERE "t"."a" = 1"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 0).unwrap(),
        &Value::from(1)
    ); // t.a
    assert!(equalities_facts.const_of_slot(top_id, 1).is_none()); // t2.b
}

// --- OR: const survives only when it appears in every live branch ---

#[test]
fn equality_facts_or_const_survives_when_in_all_branches() {
    // Both branches assert a=1, so const=1 for a must survive the intersection.
    // b=2 and c=3 are branch-local and must not survive.
    let input = r#"SELECT "a", "b", "c" FROM "t"
    WHERE "a" = 1 AND "b" = 2 OR "a" = 1 AND "c" = 3"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 0).unwrap(),
        &Value::from(1)
    ); // a
    assert!(equalities_facts.const_of_slot(top_id, 1).is_none()); // b
    assert!(equalities_facts.const_of_slot(top_id, 2).is_none()); // c
}

// --- UNION ALL / UNION: каждая ветка изолирована, факты не текут на выход ---

#[test]
fn equality_facts_union_all_no_facts_on_output() {
    // UNION ALL produces rows from two independent sources; the output
    // column is not the same column as either branch's "a".  Even when both
    // branches filter on `a = 1`, no fact about the combined output column
    // is derived.
    let input = r#"SELECT "a" FROM "t" WHERE "a" = 1
    UNION ALL
    SELECT "a" FROM "t1_2" WHERE "a" = 1"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

#[test]
fn equality_facts_union_branches_with_different_consts_no_facts() {
    // UNION branches bind columns to different constants; the combined
    // output satisfies neither constant on its own, so no equality fact
    // should appear on the UNION's output.
    let input = r#"SELECT "a", "b" FROM "t" WHERE "a" = 1 AND "b" = 2
    UNION
    SELECT "a", "b" FROM "t1_2" WHERE "a" = 3 AND "b" = 4"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
    assert!(equalities_facts.const_of_slot(top_id, 1).is_none());
}

// --- LIMIT / ORDER BY inside a subquery break fact propagation ---

#[test]
fn equality_facts_limit_breaks_propagation() {
    // LIMIT keeps a subset of rows but produces new result tuples; facts
    // from inside the LIMIT do not describe the outer result.  The inner
    // `a = 1` must not reach the outer output.
    let input = r#"SELECT "a" FROM (SELECT "a" FROM "t" WHERE "a" = 1 LIMIT 10)"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

#[test]
fn equality_facts_order_by_breaks_propagation() {
    // ORDER BY reorders rows but does not change which rows appear, so the
    // inner `a = 1` fact is still logically valid on the output.  The analyzer
    // treats Sort nodes as opaque barriers anyway (conservative choice): facts
    // are not propagated through them.
    let input = r#"SELECT "a" FROM (SELECT "a" FROM "t" WHERE "a" = 1 ORDER BY "a")"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

// --- Set operations inside a scalar subquery isolate inner facts ---

#[test]
fn equality_facts_subquery_with_union_inside_is_unsafe() {
    // A scalar subquery that itself contains a UNION ALL is opaque: its
    // result comes from a set-combining operation, so facts from inside the
    // branches do not describe the outer column being compared.
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = (SELECT "a" FROM "t1_2" WHERE "a" = 1
                 UNION ALL
                 SELECT "a" FROM "t1_2" WHERE "a" = 1)"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

// --- CTE: body is always isolated from the outer query ---

#[test]
fn equality_facts_cte_isolation() {
    // A CTE body is analysed in isolation — its output columns are a
    // separate relation referenced from the outer query.  Facts derived
    // inside the CTE body (a = 1) must not be claimed about the outer
    // SELECT's output, even when the CTE is the only source.
    let input = r#"WITH cte (a) AS (SELECT "a" FROM "t" WHERE "a" = 1) SELECT * FROM cte"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

// --- CTE referenced twice: each call-site is its own scope ---

#[test]
fn equality_facts_cte_referenced_twice_in_join() {
    // The same CTE appears on both sides of a self-join.  The `"a" = 1`
    // filter lives inside the CTE body and does not propagate onto the
    // outer `SELECT` — a CTE reference exposes only the columns of its
    // declared output.  The `ON "l"."a" = "r"."a"` condition still
    // unifies the two outer references at the join.
    let input = r#"
        WITH cte (a) AS (SELECT "a" FROM "t" WHERE "a" = 1)
        SELECT "l"."a", "r"."a"
        FROM cte AS "l" JOIN cte AS "r" ON "l"."a" = "r"."a"
    "#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
    assert!(equalities_facts.const_of_slot(top_id, 1).is_none());
    let l = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let r = equalities_facts.class_of_slot(top_id, 1).unwrap();
    assert_eq!(l, r);
}

#[test]
fn equality_facts_cte_referenced_twice_outer_facts_are_per_site() {
    // Two references to the same CTE are independent relations in the
    // outer query: `"l"."a" = 5` constrains only the left reference and
    // `"r"."a" = 7` only the right.  The two references must therefore
    // carry DIFFERENT constants on the outer output, and they must NOT
    // be unified into the same class even though they share a CTE.
    let input = r#"
        WITH cte (a) AS (SELECT "a" FROM "t")
        SELECT "l"."a", "r"."a"
        FROM cte AS "l" JOIN cte AS "r" ON true
        WHERE "l"."a" = 5 AND "r"."a" = 7
    "#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let l_const = equalities_facts.const_of_slot(top_id, 0).unwrap();
    let r_const = equalities_facts.const_of_slot(top_id, 1).unwrap();
    assert_eq!(l_const, &Value::from(5));
    assert_eq!(r_const, &Value::from(7));
    let l = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let r = equalities_facts.class_of_slot(top_id, 1).unwrap();
    assert_ne!(l, r);
}

#[test]
fn equality_facts_cte_referenced_twice_body_fact_does_not_leak_across_sites() {
    // The CTE body constrains `"a" = 1`.  Only the left reference carries
    // an extra outer filter `"l"."a" = 2`.  The body's filter does not
    // reach the outer query, so the outer output sees only `"l"."a" = 2`
    // (no const conflict with 1) and `"r"."a"` remains unconstrained on
    // the outer output.
    let input = r#"
        WITH cte (a) AS (SELECT "a" FROM "t" WHERE "a" = 1)
        SELECT "l"."a", "r"."a"
        FROM cte AS "l" JOIN cte AS "r" ON true
        WHERE "l"."a" = 2
    "#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 0).unwrap(),
        &Value::from(2)
    );
    assert!(equalities_facts.const_of_slot(top_id, 1).is_none());
}

// --- Multi-level safe subquery: facts propagate through two nesting layers ---

#[test]
fn equality_facts_multi_level_safe_subquery() {
    // Two nested subqueries each just re-select the column "a", so the
    // outer column "a" is the same column as the innermost one.  The
    // WHERE fact `a = 1` must reach the outermost output through both layers.
    let input = r#"SELECT "a" FROM (SELECT "a" FROM (SELECT "a" FROM "t" WHERE "a" = 1))"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 0).unwrap(),
        &Value::from(1)
    );
}

// --- IS NULL invalidates equalities involving the null-constrained slot ---

#[test]
fn equality_facts_is_null_with_eq_same_slot_makes_chain_dead() {
    // "a" IS NULL AND "a" = "b": since "a" IS NULL, "a" = "b" evaluates to
    // NULL (not TRUE), so the chain is unsatisfiable → no facts at all.
    let input = r#"SELECT "a", "b" FROM "t" WHERE "a" IS NULL AND "a" = "b""#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    // The chain is dead: a and b must NOT be unified.
    assert_ne!(a, b);
}

#[test]
fn equality_facts_is_null_with_eq_own_slot_makes_chain_dead() {
    // "a" IS NULL AND "a" = "a": self-equality generates no DerivedFact
    // (single-slot group → no SlotEq, no SlotConst), so a facts-only null
    // check would miss it.  The chain must still be marked dead because
    // NULL = NULL evaluates to UNKNOWN, not TRUE.  The surviving branch
    // "b" = 5 must surface as the only live fact.
    let input = r#"SELECT "a", "b" FROM "t" WHERE "a" IS NULL AND "a" = "a" OR "b" = 5"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    assert_ne!(a, b);

    let b = equalities_facts.const_of_slot(top_id, 1).unwrap();
    assert_eq!(b, &Value::from(5));
}

#[test]
fn equality_facts_is_null_with_eq_wrapped_self_makes_chain_dead() {
    // "a" IS NULL AND cast("a" as int) = "a": when "a" is NULL, cast("a" as int)
    // is also NULL, and NULL = NULL evaluates to UNKNOWN (not TRUE), so the
    // whole AND-chain is unsatisfiable.  Only the surviving branch `b = 5`
    // must produce a fact.
    let input =
        r#"SELECT "a", "b" FROM "t" WHERE "a" IS NULL AND cast("a" as int) = "a" OR "b" = 5"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let b = equalities_facts.const_of_slot(top_id, 1).unwrap();
    assert_eq!(b, &Value::from(5));
}

#[test]
fn equality_facts_is_null_does_not_invalidate_unrelated_eq() {
    // "a" IS NULL AND "b" = "c": "a" IS NULL does not affect "b" = "c",
    // so the fact b ≡ c must still be derived.
    let input = r#"SELECT "a", "b", "c" FROM "t" WHERE "a" IS NULL AND "b" = "c""#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    let c = equalities_facts.class_of_slot(top_id, 2).unwrap();
    assert_eq!(b, c);
}

// --- Inner JOIN inside a safe subquery: WHERE fact propagates out ---

#[test]
fn equality_facts_inner_join_inside_safe_subquery() {
    // The subquery is an INNER JOIN with a WHERE filter on t.a = 5; the
    // outer SELECT just re-exposes t.a.  Since neither the join nor the
    // trivial projection changes the referenced column, the fact `t.a = 5`
    // remains valid on the outermost output.
    let input = r#"SELECT "t"."a" FROM (
        SELECT "t"."a" FROM "t" JOIN "t1_2" AS "t1" ON "t"."a" = "t1"."a" WHERE "t"."a" = 5
    ) AS "t""#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 0).unwrap(),
        &Value::from(5)
    );
}

// --- Row equality with parameters: separate classes, no const propagation ---

#[test]
fn equality_facts_row_eq_params_no_const() {
    // (a, b) = ($1, $2) is equivalent to a = $1 AND b = $2.
    // $1 and $2 are distinct parameters, so a and b are in different
    // equivalence classes, and neither class has a known constant since
    // the right-hand sides are parameters, not literals.
    let input = r#"SELECT "a", "b" FROM "t" WHERE ("a", "b") = ($1, $2)"#;
    let (plan, equalities_facts) =
        equalities_facts(input, vec![Value::Integer(1), Value::Integer(2)]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    // Different parameters → different equivalence classes
    assert_ne!(a, b);
    // Parameters are not literals → no constant bindings
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
    assert!(equalities_facts.const_of_slot(top_id, 1).is_none());
}

// --- Conflicting constants across nested WHERE layers ---

#[test]
fn equality_facts_conflicting_consts_across_selections() {
    // The inner subquery says a = 1; the outer WHERE says a = 2.  Both
    // facts apply to the same column, so the equivalence class for "a"
    // sees two contradictory constants.  The class cannot carry either
    // value — no constant is reported.  (Note: this query never returns
    // rows, but equality-facts analysis only reasons about class membership.)
    let input = r#"SELECT "a" FROM (SELECT "a" FROM "t" WHERE "a" = 1) WHERE "a" = 2"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

// --- Window function: breaks fact propagation ---

#[test]
fn equality_facts_window_breaks_propagation() {
    // A SELECT that computes a window function (count(*) OVER ()) is not
    // a plain pass-through projection — its output rows depend on a
    // partition computation, not just per-row column values.  Facts from
    // the inner WHERE must not surface on the outer SELECT.
    let input = r#"SELECT "a" FROM (
        SELECT "a", count(*) OVER () AS "cnt" FROM "t" WHERE "a" = 1
    )"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

// --- GROUP BY / HAVING: break fact propagation ---

#[test]
fn equality_facts_group_by_breaks_propagation() {
    // GROUP BY aggregates rows, so the output is not the same row-for-row
    // relation as the input.  A fact about an input column does not
    // translate to the grouped output.  The inner `a = 1` must not
    // surface on the outer SELECT.
    let input = r#"SELECT "a" FROM (
        SELECT "a" FROM "t" WHERE "a" = 1 GROUP BY "a"
    )"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

#[test]
fn equality_facts_having_breaks_propagation() {
    // HAVING filters aggregated groups; like GROUP BY, it reshapes rows,
    // so inner per-row facts do not carry to the outer output.
    let input = r#"SELECT "a" FROM (
        SELECT "a" FROM "t" WHERE "a" = 1 GROUP BY "a" HAVING count(*) > 0
    )"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

// --- IS NOT NULL: must NOT be treated like IS NULL ---

#[test]
fn equality_facts_is_not_null_does_not_poison_eq() {
    // "a" IS NOT NULL is a different predicate from "a" IS NULL — it does
    // not restrict "a" to be NULL, so it must not prevent `a = b` from
    // unifying a and b into one equivalence class.
    let input = r#"SELECT "a", "b" FROM "t" WHERE "a" IS NOT NULL AND "a" = "b""#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    assert_eq!(a, b);
}

// --- IS NULL also blocks constant bindings on the same column ---

#[test]
fn equality_facts_is_null_invalidates_slot_const() {
    // "a" IS NULL AND "a" = 1 is unsatisfiable: a cannot be both NULL and
    // equal to 1.  No row matches, so no equality facts can be derived —
    // in particular, "a" must not be reported as bound to 1.
    let input = r#"SELECT "a" FROM "t" WHERE "a" IS NULL AND "a" = 1"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

// --- Computed expression on one side: no column equality is implied ---

#[test]
fn equality_facts_computed_expr_side_yields_no_fact() {
    // `"a" + 1 = "b"` equates a computed expression with column "b"; it
    // does NOT mean `a = b` (e.g., a=1, b=2 satisfies it).  Columns a and
    // b must remain in separate equivalence classes.
    let input = r#"SELECT "a", "b" FROM "t" WHERE "a" + 1 = "b""#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    assert_ne!(a, b);
}

// --- EXCEPT: no facts flow to the set-difference output ---

#[test]
fn equality_facts_except_no_facts_on_output() {
    // EXCEPT is a set operation: the result column is neither branch's
    // "a" directly, so facts about each branch's rows do not describe the
    // EXCEPT output column.
    let input = r#"SELECT "a" FROM "t" WHERE "a" = 1
    EXCEPT
    SELECT "a" FROM "t1_2" WHERE "a" = 2"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

// --- UPDATE ... WHERE: analysis runs cleanly on DML plans ---

#[test]
fn equality_facts_update_with_where_does_not_panic() {
    // The analyzer must handle a top-level UPDATE without panicking.
    // UPDATE's own output columns are not a query result consumers read,
    // so facts from the WHERE clause must not be claimed about UPDATE's
    // output columns.
    let input = r#"UPDATE "t" SET "c" = 0 WHERE "a" = 1"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    // UPDATE's own output column 0 must not inherit the WHERE constant.
    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

// --- SELECT from VALUES: no equality facts derived ---

#[test]
fn equality_facts_select_from_values_no_facts() {
    // `SELECT * FROM (VALUES (1, 2))` has no WHERE clause and no join
    // conditions — there are no equality predicates to derive facts from.
    // The analyzer must walk this plan without emitting any facts.
    let input = r#"SELECT * FROM (VALUES (1, 2))"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
    assert!(equalities_facts.const_of_slot(top_id, 1).is_none());
}

// --- Parameter in one class must not leak into another ---

#[test]
fn equality_facts_param_blocks_const_in_multiple_classes() {
    // Two independent classes, each combining a parameter and a literal:
    //   {a, $1, 1} — "a" class also involves $1
    //   {b, $2, 2} — "b" class also involves $2
    // Neither class can claim a known constant (each contains a
    // parameter), but the rule must apply per class, not poison unrelated
    // classes.
    let input = r#"SELECT "a", "b" FROM "t"
    WHERE "a" = $1 AND "a" = 1 AND "b" = $2 AND "b" = 2"#;
    let (plan, equalities_facts) =
        equalities_facts(input, vec![Value::Integer(1), Value::Integer(2)]);

    let top_id = plan.top.unwrap();
    let a = equalities_facts.class_of_slot(top_id, 0).unwrap();
    let b = equalities_facts.class_of_slot(top_id, 1).unwrap();
    // Different parameters → distinct equivalence classes
    assert_ne!(a, b);
    // Each class contains a parameter, so neither has a known constant
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
    assert!(equalities_facts.const_of_slot(top_id, 1).is_none());
}

// --- Three-way constant conflict across nested WHERE clauses ---

#[test]
fn equality_facts_three_way_const_conflict() {
    // Three nested subqueries each constrain the same column "a" to a
    // different literal (1, 2, 3).  All three refer to the same output
    // column, so the equivalence class sees three contradictory constants.
    // The class must not be reported as bound to any of them.
    let input = r#"SELECT "a" FROM (
        SELECT "a" FROM (
            SELECT "a" FROM "t" WHERE "a" = 1
        ) WHERE "a" = 2
    ) WHERE "a" = 3"#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

// --- BETWEEN with a subquery as the scrutinee ---

#[test]
fn equality_facts_subquery_between_literals() {
    // `(SELECT ...) BETWEEN 1 AND 2` is a range predicate, not an
    // equality, so the outer `WHERE` yields no constant for `"a"`.  The
    // subquery on the left of BETWEEN is a separate relation; its inner
    // filter `"a" = 5` applies only inside it and does not constrain the
    // outer `"a"`.
    let input = r#"
        SELECT "a" FROM "t"
        WHERE (SELECT "a" FROM "t" WHERE "a" = 5) BETWEEN 1 AND 2
    "#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
}

#[test]
fn equality_facts_subquery_between_literals_with_sibling_eq() {
    // Same `WHERE` as above plus a sibling `"b" = 7`.  The BETWEEN on the
    // scalar subquery adds no facts, but the sibling equality still
    // binds `"b"` to 7 on the outer output, and the subquery's inner
    // filter stays isolated.
    let input = r#"
        SELECT "a", "b" FROM "t"
        WHERE (SELECT "a" FROM "t" WHERE "a" = 5) BETWEEN 1 AND 2
          AND "b" = 7
    "#;
    let (plan, equalities_facts) = equalities_facts(input, vec![]);

    let top_id = plan.top.unwrap();
    assert!(equalities_facts.const_of_slot(top_id, 0).is_none());
    assert_eq!(
        equalities_facts.const_of_slot(top_id, 1).unwrap(),
        &Value::from(7)
    );
}
