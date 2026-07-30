use insta::assert_snapshot;
use sql::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use sql::executor::engine::helpers::table_name;
use sql::executor::ir::ExecutionPlan;
use sql::explain::ir::LogicalExplain;
use sql::helpers::sql_to_ir_without_bind;
use sql::ir::node::NodeId;
use sql::ir::transformation::Stage;
use sql::ir::tree::Snapshot;
use sql::ir::types::{DerivedType, UnrestrictedType};
use sql::ir::Plan;

/// Enrich the restrictions of `query`, then materialize them into their filters,
/// stopping right before motions are added.
///
/// Parameters are left unbound so a clause over one still prints as `$n`.
fn run_enrich(query: &str, params: &[DerivedType]) -> Plan {
    let plan = sql_to_ir_without_bind(query, params);
    let top_id = plan.get_top().unwrap();
    plan.optimize_before(top_id, Stage::EnrichRestrictions)
        .unwrap()
        .enrich_restrictions_from_facts(top_id)
        .unwrap()
}

/// The plain logical-explain rendering of `plan`.
fn logical(plan: &Plan) -> String {
    let top = plan.get_top().unwrap();
    LogicalExplain::new(plan, top).unwrap().to_string()
}

#[test]
fn const_pins_every_member_at_the_base_scan() {
    // a = b AND b = 1 => the class {a, b, 1} pins the constant, so both members
    // get `col = 1` at the base scan, including `a = 1`, which the WHERE never
    // spells out.
    let plan = run_enrich(r#"SELECT "a" FROM "t" WHERE "a" = "b" AND "b" = 1"#, &[]);
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection ((t.a::int = t.b::int and t.b::int = 1::int and t.a::int = 1::int and t.b::int = 1::int))
        scan t
    ");
}

#[test]
fn const_crosses_an_inner_join_to_the_other_scan() {
    // t.a = t2.e (inner join ON) AND t.a = 1 => t2.e is in the same class, so the
    // constant reaches t2's own base scan, on the far side of the motion. This is
    // the payoff of the pass: a bucket-pruning fact present in no clause of the
    // query.
    let plan = run_enrich(
        r#"SELECT "t"."a" FROM "t" JOIN "t2" ON "t"."a" = "t2"."e" WHERE "t"."a" = 1"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection (t.a::int = 1::int)
        join on (t.a::int = t2.e::int)
          scan t
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              selection (t.a::int = 1::int)
                scan t
          scan t2
            projection (t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t2.bucket_id::int -> bucket_id)
              selection (t2.e::int = 1::int)
                scan t2
    ");
}

#[test]
fn a_passthrough_stack_collapses_to_one_clause_at_the_base_scan() {
    // The class member lives on the outer Selection, and holds the same column at
    // every passthrough level below it (Projection, ScanSubQuery, scan). They all
    // resolve to one target, so the whole stack yields exactly one clause,
    // placed at the base scan, and nowhere between.
    let plan = run_enrich(
        r#"SELECT "a" FROM (SELECT "a" FROM "t") WHERE "a" = 1"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (unnamed_subquery.a::int -> a)
      selection (unnamed_subquery.a::int = 1::int)
        scan unnamed_subquery
          projection (t.a::int -> a)
            selection (t.a::int = 1::int)
              scan t
    ");
}

#[test]
fn param_pins_every_member_when_no_const() {
    // a = $1 AND a = b => the class {a, b, $1} has no literal, so the parameter is
    // the pin and each member gets `col = $1`, transitive b included.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE "a" = $1 AND "a" = "b""#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection ((t.a::int = $1::int and t.a::int = t.b::int and t.a::int = $1::int and t.b::int = $1::int))
        scan t
    ");
}

#[test]
fn const_wins_over_param_and_leaves_a_one_time_filter() {
    // a = $1 AND a = 5 => the literal is the pin (usable at planning time), so the
    // column gets `a = 5`, and the parameter member gets `$1 = 5`.
    // The `$1::int` is the declared type, not one inferred
    // from the constant it was pinned to.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE "a" = $1 AND "a" = 5"#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection ((t.a::int = $1::int and t.a::int = 5::int and $1::int = 5::int and t.a::int = 5::int))
        scan t
    ");
}

#[test]
fn extra_params_get_a_one_time_filter_against_the_pin_param() {
    // a = $1 AND a = $2 => the class pins the lowest param, so the column gets
    // `a = $1` and the other param gets `$2 = $1`. This produces one parameter
    // check whose operands keep their own declared types.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE "a" = $1 AND "a" = $2"#,
        &[DerivedType::new(UnrestrictedType::Integer); 2],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection ((t.a::int = $1::int and t.a::int = $2::int and $2::int = $1::int and t.a::int = $1::int))
        scan t
    ");
}

#[test]
fn a_one_time_filter_carries_on_a_selection_above_a_passthrough_projection() {
    // SELECT * FROM (... WHERE a = 1 AND a = b AND b = $1): the derived table is a
    // transparent passthrough, so its facts live in the outer query's domain and
    // the class anchor is the outer projection. That projection has no filter, so
    // the `$1 = 1` one-time filter carries on a real `selection` spliced above the
    // derived table. The column pins (`a = 1`, `b = 1`) still land at the base
    // scan inside.
    let plan = run_enrich(
        r#"SELECT * FROM (SELECT "a" FROM "t" WHERE "a" = 1 AND "a" = "b" AND "b" = $1)"#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (unnamed_subquery.a::int -> a)
      selection ($1::int = 1::int)
        scan unnamed_subquery
          projection (t.a::int -> a)
            selection ((t.a::int = 1::int and t.a::int = t.b::int and t.b::int = $1::int and t.a::int = 1::int and t.b::int = 1::int))
              scan t
    ");
}

#[test]
fn implied_clauses_are_base_keyed_deduped() {
    // a = 1 AND a = b AND b = 1: several class members and several source clauses
    // collapse onto the same base column, but each (slot, pin) is emitted once,
    // `a = 1` appears once under the scan, not three times.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE "a" = 1 AND "a" = "b" AND "b" = 1"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection ((t.a::int = 1::int and t.a::int = t.b::int and t.b::int = 1::int and t.a::int = 1::int and t.b::int = 1::int))
        scan t
    ");
}

#[test]
fn unpinned_class_derives_nothing() {
    // The pass derives one shape, the star around the pin. `a = b` pins no
    // value, so the class yields nothing and the filter is untouched.
    let plan = run_enrich(r#"SELECT "a" FROM "t" WHERE "a" = "b""#, &[]);
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection (t.a::int = t.b::int)
        scan t
    ");
}

#[test]
fn null_const_derives_nothing() {
    // `a = NULL` is never TRUE, so NULL pins nothing that could restrict a scan.
    let plan = run_enrich(r#"SELECT "a" FROM "t" WHERE "a" = NULL"#, &[]);
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection (t.a::int = NULL::unknown)
        scan t
    ");
}

#[test]
fn contradictory_class_derives_nothing() {
    // a = 1 AND a = b AND b = 2 merges two non-equal constants into one class.
    // It is unsatisfiable, so nothing may be derived from it. The filter keeps
    // only the clauses the query spelled out.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE "a" = 1 AND "a" = "b" AND "b" = 2"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection ((t.a::int = 1::int and t.a::int = t.b::int and t.b::int = 2::int))
        scan t
    ");
}

#[test]
fn no_pin_across_an_outer_join_nullable_side() {
    // A LEFT JOIN's ON equality holds only for matched rows, so t2.e is not
    // globally in t.a's class: the constant reaches t's scan and stops there,
    // leaving t2's scan bare.
    let plan = run_enrich(
        r#"SELECT "t"."a" FROM "t" LEFT JOIN "t2" ON "t"."a" = "t2"."e" WHERE "t"."a" = 1"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection (t.a::int = 1::int)
        left join on (t.a::int = t2.e::int)
          scan t
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              selection (t.a::int = 1::int)
                scan t
          scan t2
    ");
}

#[test]
fn a_pin_about_the_nullable_side_settles_above_the_outer_join() {
    // `t2.f = 1` comes from the WHERE, above the join, so it holds on the join's
    // output. It may not reach t2's scan, and it may not sit on the join either,
    // whose ON does not filter unmatched rows. It settles directly above the
    // join. Hence the doubled clause: the filter spells `t2.f = 1`, and the pass
    // settled the same fact here.
    let plan = run_enrich(
        r#"SELECT "t"."a" FROM "t" LEFT JOIN "t2" ON "t"."a" = "t2"."e" WHERE "t2"."f" = 1"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection ((t2.f::int = 1::int and t2.f::int = 1::int))
        left join on (t.a::int = t2.e::int)
          scan t
          scan t2
    ");
}

#[test]
fn a_pin_about_the_preserved_side_still_descends() {
    // The veto covers only the nullable side. A clause about the *preserved*
    // side descends into the left child and reaches t's scan, allowing bucket pruning.
    let plan = run_enrich(
        r#"SELECT "t"."a" FROM "t" LEFT JOIN "t2" ON "t"."a" = "t2"."e" WHERE "t"."b" = 1"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection (t.b::int = 1::int)
        left join on (t.a::int = t2.e::int)
          scan t
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              selection (t.b::int = 1::int)
                scan t
          scan t2
    ");
}

#[test]
fn a_pinned_bucket_id_is_recorded() {
    // A pinned bucket_id identifies the query's bucket and gets a derived equality
    // like any other column.
    let plan = run_enrich(r#"SELECT "a" FROM "t" WHERE "bucket_id" = 42"#, &[]);
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection ((t.bucket_id::int = 42::int and t.bucket_id::int = 42::int))
        scan t
    ");
}

#[test]
fn a_transitively_pinned_bucket_id_is_recorded() {
    // The payoff case: nothing in the query says `bucket_id = 42`, the class does.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE "bucket_id" = "a" AND "a" = 42"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection ((t.bucket_id::int = t.a::int and t.a::int = 42::int and t.a::int = 42::int and t.bucket_id::int = 42::int))
        scan t
    ");
}

#[test]
fn a_barrier_settles_the_clause_instead_of_dropping_it() {
    // a = 1 AND a = b derives b = 1, but the base column sits behind a GROUP BY.
    // The clause is still valid on the aggregation's own output, so it settles on
    // the projection above the grouping rather than being thrown away.
    let plan = run_enrich(
        r#"SELECT "a", "b" FROM (SELECT "a", "b" FROM "t" GROUP BY "a", "b") WHERE "a" = 1 AND "a" = "b""#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (unnamed_subquery.a::int -> a, unnamed_subquery.b::int -> b)
      selection ((unnamed_subquery.a::int = 1::int and unnamed_subquery.a::int = unnamed_subquery.b::int and unnamed_subquery.a::int = 1::int and unnamed_subquery.b::int = 1::int))
        scan unnamed_subquery
          projection (t.a::int -> a, t.b::int -> b)
            group by (t.a::int, t.b::int)
              scan t
    ");
}

#[test]
fn group_by_without_where() {
    // Two nested subqueries: the outer one is a clean passthrough, the inner one
    // wraps a GROUP BY. `a = 1 AND a = b` derives `b = 1`, which must not be
    // pushed into the opaque GROUP BY body, but the passthrough in between is
    // crossable, so the clause settles one boundary down, on the inner
    // `ScanSubQuery`.
    let plan = run_enrich(
        r#"SELECT "a", "b" FROM (SELECT "a", "b" FROM (SELECT "a", "b" FROM "t" GROUP BY "a", "b") ) WHERE "a" = 1 AND "a" = "b""#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (unnamed_subquery_1.a::int -> a, unnamed_subquery_1.b::int -> b)
      selection ((unnamed_subquery_1.a::int = 1::int and unnamed_subquery_1.a::int = unnamed_subquery_1.b::int))
        scan unnamed_subquery_1
          projection (unnamed_subquery.a::int -> a, unnamed_subquery.b::int -> b)
            selection ((unnamed_subquery.a::int = 1::int and unnamed_subquery.b::int = 1::int))
              scan unnamed_subquery
                projection (t.a::int -> a, t.b::int -> b)
                  group by (t.a::int, t.b::int)
                    scan t
    ");
}

#[test]
fn a_barrier_is_not_crossed() {
    // The flip side: the clause settles *on* the barrier and must not leak below
    // it. `max(a) = 1` says nothing about the rows feeding the aggregate, so the
    // base scan stays bare.
    let plan = run_enrich(
        r#"SELECT "a" FROM (SELECT max("a") as "a" FROM "t") WHERE "a" = 1"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (unnamed_subquery.a::int -> a)
      selection ((unnamed_subquery.a::int = 1::int and unnamed_subquery.a::int = 1::int))
        scan unnamed_subquery
          projection (max(t.a::int::int)::int -> a)
            scan t
    ");
}

#[test]
fn a_one_time_filter_per_param_pin_and_anchor() {
    // `$1` is domain-tagged, so the outer query and the isolated subquery put it
    // in two different classes. Both pin 5, so both parameter checks read
    // `$1 = 5`. They are still two one-time filters, keyed at two anchors.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE "a" = $1 AND "a" = 5 AND "b" IN (SELECT "e" FROM "t2" WHERE "e" = $1 AND "e" = 5)"#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection ((t.a::int = $1::int and t.a::int = 5::int and t.b::int in ROW($0) and $1::int = 5::int and t.a::int = 5::int))
        scan t
    subquery $0:
      scan
        projection (t2.e::int -> e)
          selection ((t2.e::int = $1::int and t2.e::int = 5::int and $1::int = 5::int and t2.e::int = 5::int))
            scan t2
    ");
}

#[test]
fn conflicting_one_time_filters_for_one_param_stay_in_their_own_domains() {
    // The flip side: two domains pinning $1 to different values. Together they
    // prove the query empty at bind time, but `$1 = 7` is a fact about the
    // subquery, and this pass does not lift it out of one.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE "a" = $1 AND "a" = 5 AND "b" IN (SELECT "e" FROM "t2" WHERE "e" = $1 AND "e" = 7)"#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection ((t.a::int = $1::int and t.a::int = 5::int and t.b::int in ROW($0) and $1::int = 5::int and t.a::int = 5::int))
        scan t
    subquery $0:
      scan
        projection (t2.e::int -> e)
          selection ((t2.e::int = $1::int and t2.e::int = 7::int and $1::int = 7::int and t2.e::int = 7::int))
            scan t2
    ");
}

// Parameter checks stay within the domain where their class was derived.
//
// A column clause is placed by walking down from its own member, so it cannot
// stray. A one-time filter has no column and has to be keyed on a node outright,
// which is where it can go wrong: keyed too high, it claims a fact for rows the
// class never constrained. Each test below is a shape where the region can be
// empty while the query above it still returns rows.

#[test]
fn a_one_time_filter_stays_inside_a_not_in_subquery() {
    // Bound `$1 = 3` the subquery is empty, `NOT IN` is true for every row, and
    // the answer is all of `t`. A one-time filter at the outer selection would
    // delete it.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE "b" NOT IN (SELECT "e" FROM "t2" WHERE "e" = $1 AND "e" = 7)"#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection (not t.b::int in ROW($0))
        scan t
    subquery $0:
      scan
        projection (t2.e::int -> e)
          selection ((t2.e::int = $1::int and t2.e::int = 7::int and $1::int = 7::int and t2.e::int = 7::int))
            scan t2
    ");
}

#[test]
fn a_one_time_filter_stays_inside_a_union_arm() {
    // An empty arm still lets its sibling through, so a one-time filter keyed on
    // the `union all` would drop `t2`'s rows too.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE "a" = $1 AND "a" = 7 UNION ALL SELECT "e" FROM "t2""#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    union all
      projection (t.a::int -> a)
        selection ((t.a::int = $1::int and t.a::int = 7::int and $1::int = 7::int and t.a::int = 7::int))
          scan t
      projection (t2.e::int -> e)
        scan t2
    ");
}

#[test]
fn a_one_time_filter_stays_inside_an_except_arm() {
    // The subtrahend: empty right arm means the answer is the whole left one.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" EXCEPT SELECT "e" FROM "t2" WHERE "e" = $1 AND "e" = 7"#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    except
      projection (t.a::int -> a)
        scan t
      projection (t2.e::int -> e)
        selection ((t2.e::int = $1::int and t2.e::int = 7::int and $1::int = 7::int and t2.e::int = 7::int))
          scan t2
    ");
}

#[test]
fn a_one_time_filter_stays_on_the_nullable_side_of_an_outer_join() {
    // The fact is inside the derived table, which the LEFT JOIN preserves `t`
    // against: an empty right side null-extends instead of filtering.
    let plan = run_enrich(
        r#"SELECT "t"."a" FROM "t" LEFT JOIN (SELECT "e" FROM "t2" WHERE "e" = $1 AND "e" = 7) s ON "t"."a" = s."e""#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      left join on (t.a::int = s.e::int)
        scan t
        scan s
          projection (t2.e::int -> e)
            selection ((t2.e::int = $1::int and t2.e::int = 7::int and $1::int = 7::int and t2.e::int = 7::int))
              scan t2
    ");
}

#[test]
fn a_one_time_filter_stays_inside_a_subquery_under_or() {
    // A plain `IN` propagates emptiness to the whole query, but under `OR` it
    // does not: with `$1 = 3` the `IN` is false and the rows with `a = 3` are
    // still the answer.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE "b" IN (SELECT "e" FROM "t2" WHERE "e" = $1 AND "e" = 7) OR "a" = 3"#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection (t.b::int in ROW($0) or t.a::int = 3::int)
        scan t
    subquery $0:
      scan
        projection (t2.e::int -> e)
          selection ((t2.e::int = $1::int and t2.e::int = 7::int and $1::int = 7::int and t2.e::int = 7::int))
            scan t2
    ");
}

#[test]
fn a_flat_or_leaks_no_one_time_filter() {
    // A disjunction is not a domain boundary. `a = $1` lives in one `OR` arm and
    // not the other, so the arms' facts intersect to nothing and no `$1 = 7`
    // one-time filter is ever derived: the filter is left exactly as written.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE ("a" = $1 AND "a" = 7) OR "a" = 3"#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection ((t.a::int = $1::int and t.a::int = 7::int) or t.a::int = 3::int)
        scan t
    ");
}

#[test]
fn a_flat_or_across_two_params_leaks_no_one_time_filter() {
    // Same intersection, both arms parameterized: neither `$1 = 7` nor `$2 = 9`
    // holds for every row the `OR` admits, so both are dropped.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE ("a" = $1 AND "a" = 7) OR ("a" = $2 AND "a" = 9)"#,
        &[DerivedType::new(UnrestrictedType::Integer); 2],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection ((t.a::int = $1::int and t.a::int = 7::int) or (t.a::int = $2::int and t.a::int = 9::int))
        scan t
    ");
}

#[test]
fn a_one_time_filter_settles_in_the_innermost_of_nested_domains() {
    // Two opaque subquery boundaries stacked: the one-time filter is derived in
    // the innermost body (`t3`), and its anchor is that body's root, not the
    // middle subquery over `t2`, and not the outer query.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE "b" IN (SELECT "e" FROM "t2" WHERE "e" IN (SELECT "b" FROM "t3" WHERE "b" = $1 AND "b" = 7))"#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection (t.b::int in ROW($1))
        scan t
    subquery $0:
      scan
        projection (t3.b::int -> b)
          selection ((t3.b::int = $1::int and t3.b::int = 7::int and $1::int = 7::int and t3.b::int = 7::int))
            scan t3
    subquery $1:
      scan
        projection (t2.e::int -> e)
          selection (t2.e::int in ROW($0))
            scan t2
    ");
}

#[test]
fn a_one_time_filter_stays_below_an_aggregate() {
    // An aggregate over no rows still returns a row, so `$1 = 7` keyed above the
    // `count` would delete the `count(*) = 0` answer the query is entitled to.
    let plan = run_enrich(
        r#"SELECT count("a") FROM "t" WHERE "a" = $1 AND "a" = 7"#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (count(t.a::int::int)::int -> col_1)
      selection ((t.a::int = $1::int and t.a::int = 7::int and $1::int = 7::int and t.a::int = 7::int))
        scan t
    ");
}

#[test]
fn a_param_check_stays_below_an_aggregate_inside_a_not_in() {
    // The anchor boundary and an empty-input aggregate together. `$1 = 7` is
    // derived in the subquery's WHERE, and the aggregating projection re-opens the
    // domain at that WHERE, so the WHERE is the class anchor. When $1 != 7 the
    // WHERE is empty, yet `count(e)` still returns one row (0), so `NOT IN` can be
    // false and `t`'s rows remain the answer. The check must fold into the inner
    // WHERE. It must never climb to the outer selection (which would delete those rows),
    // and never rise onto the aggregate above its own anchor.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE "b" NOT IN (SELECT count("e") FROM "t2" WHERE "e" = $1 AND "e" = 7)"#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection (not t.b::int in ROW($0))
        scan t
    subquery $0:
      scan
        projection (count(t2.e::int::int)::int -> col_1)
          selection ((t2.e::int = $1::int and t2.e::int = 7::int and $1::int = 7::int and t2.e::int = 7::int))
            scan t2
    ");
}

#[test]
fn a_one_time_filter_stays_below_an_explicit_group_by() {
    // An explicit `GROUP BY` leaves a real `group by (...)` node between the
    // projection and the `WHERE`. `$1 = 5` says nothing about the grouped rows the
    // projection returns, so it stays keyed on the pre-aggregation `WHERE`.
    let plan = run_enrich(
        r#"SELECT "a", count("b") FROM "t" WHERE "a" = $1 AND "a" = 5 GROUP BY "a""#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a, count(t.b::int::int)::int -> col_1)
      group by (t.a::int)
        selection ((t.a::int = $1::int and t.a::int = 5::int and $1::int = 5::int and t.a::int = 5::int))
          scan t
    ");
}

#[test]
fn a_one_time_filter_stays_below_an_order_by() {
    // `ORDER BY` changes nothing about which rows exist, but it opens a fresh
    // domain, and the one-time filter follows it down rather than sitting on the
    // query root out of habit.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE "a" = $1 AND "a" = 7 ORDER BY "a""#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (a::int)
      order by (a::int)
        scan
          projection (t.a::int -> a)
            selection ((t.a::int = $1::int and t.a::int = 7::int and $1::int = 7::int and t.a::int = 7::int))
              scan t
    ");
}

#[test]
fn two_classes_may_share_a_target() {
    // `OrderBy` opens a fresh domain, but the walk crosses it. The two sides of
    // that asymmetry are two different classes, and their members land on the
    // *same* base slot: the inner class pins the literal, the outer the parameter.
    // Both clauses are real and neither may be dropped.
    let plan = run_enrich(
        r#"SELECT "a" FROM (SELECT "a" FROM "t" WHERE "a" = 5 ORDER BY "a") WHERE "a" = $1"#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (unnamed_subquery.a::int -> a)
      selection ((unnamed_subquery.a::int = $1::int and unnamed_subquery.a::int = $1::int))
        scan unnamed_subquery
          projection (a::int)
            order by (a::int)
              scan
                projection (t.a::int -> a)
                  selection ((t.a::int = 5::int and t.a::int = 5::int))
                    scan t
    ");
}

#[test]
fn converging_aliases_of_one_column_collapse() {
    // `a AS x, a AS y` gives two projection positions feeding the *same* scan
    // column. Both are upper copies of that column, so `has_deeper_member` drops
    // them before the search and only the shared base column is placed, producing
    // one clause, without any per-slot dedup.
    let plan = run_enrich(
        r#"SELECT "x" FROM (SELECT "a" as "x", "a" as "y" FROM "t") WHERE "x" = 1"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (unnamed_subquery.x::int -> x)
      selection (unnamed_subquery.x::int = 1::int)
        scan unnamed_subquery
          projection (t.a::int -> x, t.a::int -> y)
            selection (t.a::int = 1::int)
              scan t
    ");
}

#[test]
fn const_reaches_both_call_sites_of_a_shared_cte() {
    // One CTE, two call sites joined on their shared column, plus `l.a = 5`.
    // The class {l.a, r.a, 5} spans both `ScanCte` nodes, so the constant lands on
    // both call sites as `l.a = 5` and `r.a = 5`. The shared body is a separate
    // domain, so no clause lands inside it.
    let plan = run_enrich(
        r#"WITH cte (a) AS (SELECT "a" FROM "t")
           SELECT "l"."a" FROM cte AS "l" JOIN cte AS "r" ON "l"."a" = "r"."a"
           WHERE "l"."a" = 5"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (l.a::int -> a)
      selection (l.a::int = 5::int)
        join on (l.a::int = r.a::int)
          scan l
            projection (l.a::int -> a)
              selection (l.a::int = 5::int)
                scan cte l($0)
          scan r
            projection (r.a::int -> a)
              selection (r.a::int = 5::int)
                scan cte r($0)
    subquery $0:
      projection (t.a::int -> a)
        scan t
    ");
}

#[test]
fn pin_into_a_projection_only_subquery_operand_inserts_a_filter() {
    // The join's left operand is a subquery `(SELECT a FROM t)`: a Projection
    // over a scan, with no filter of its own. The pin `sub.a = 1` descends to
    // `t`'s base scan, whose parent is that Projection. A real `selection` is
    // spliced onto the `Projection -> scan` edge to carry `t.a = 1`.
    // The bare t2 operand also gets a new selection.
    let plan = run_enrich(
        r#"SELECT "sub"."a" FROM (SELECT "a" FROM "t") AS "sub"
           JOIN "t2" ON "sub"."a" = "t2"."e"
           WHERE "sub"."a" = 1"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (sub.a::int -> a)
      selection (sub.a::int = 1::int)
        join on (sub.a::int = t2.e::int)
          scan sub
            projection (t.a::int -> a)
              selection (t.a::int = 1::int)
                scan t
          scan t2
            projection (t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t2.bucket_id::int -> bucket_id)
              selection (t2.e::int = 1::int)
                scan t2
    ");
}

#[test]
fn pin_into_a_filtered_subquery_operand_uses_the_existing_filter() {
    // Same shape, but the left operand's subquery already has a filter
    // `(SELECT a FROM t WHERE a > 0)`. The pin descends to `t`'s base scan, whose
    // parent is that existing Selection, so `t.a = 1` is AND-ed onto it with no
    // tree change (enrich REUSES the filter, does not insert one).
    let plan = run_enrich(
        r#"SELECT "sub"."a" FROM (SELECT "a" FROM "t" WHERE "a" > 0) AS "sub"
           JOIN "t2" ON "sub"."a" = "t2"."e"
           WHERE "sub"."a" = 1"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (sub.a::int -> a)
      selection (sub.a::int = 1::int)
        join on (sub.a::int = t2.e::int)
          scan sub
            projection (t.a::int -> a)
              selection ((t.a::int > 0::int and t.a::int = 1::int))
                scan t
          scan t2
            projection (t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t2.bucket_id::int -> bucket_id)
              selection (t2.e::int = 1::int)
                scan t2
    ");
}

#[test]
fn a_carrier_survives_a_motion_spliced_above_its_landing_scan() {
    // The same shape as `pin_into_a_projection_only_subquery_operand_inserts_a_filter`,
    // but planned through the WHOLE pipeline (`Stage::End`) so motions are in the
    // tree. `AddMotions` splices a `motion` between the join and the `t2` operand.
    // Because the carrier is a real materialized `selection` on the `join -> t2`
    // edge, the motion lands above it and the carrier rides down with its scan.
    let query = r#"SELECT * FROM t t1
           JOIN t t2 ON t1.a = t2.b
           WHERE t1.a = 1"#;

    let plan = sql_to_ir_without_bind(query, &[]);
    let top_id = plan.get_top().unwrap();
    let plan = plan.optimize_before(top_id, Stage::End).unwrap();

    assert_snapshot!(logical(&plan), @r"
    projection (t1.a::int -> a, t1.b::int -> b, t1.c::int -> c, t1.d::int -> d, t2.a::int -> a, t2.b::int -> b, t2.c::int -> c, t2.d::int -> d)
      selection (t1.a::int = 1::int)
        join on (t1.a::int = t2.b::int)
          scan t1
            projection (t1.a::int -> a, t1.b::int -> b, t1.c::int -> c, t1.d::int -> d, t1.bucket_id::int -> bucket_id)
              selection (t1.a::int = 1::int)
                scan t -> t1
          motion [policy: full, program: ReshardIfNeeded]
            scan t2
              projection (t2.a::int -> a, t2.b::int -> b, t2.c::int -> c, t2.d::int -> d, t2.bucket_id::int -> bucket_id)
                selection (t2.b::int = 1::int)
                  scan t -> t2
    ");
}

#[test]
fn wide_carriers_allocate_only_one_output_per_operand() {
    use sql::ir::node::{Node32, Node64, Node96};
    use sql_ast_new_corpus::MockCatalog;
    use sql_frontend::frontend::sql::transform_into_plan;

    // A restriction on each join operand needs a subquery, a projection and a
    // selection. Only the projection needs new output expressions, regardless
    // of the table width. Count arena entries so even detached copies are caught.
    fn output_counts(plan: &Plan) -> [usize; 3] {
        let nodes = plan.get_nodes();
        [
            nodes
                .iter32()
                .filter(|n| matches!(n, Node32::Alias(_)))
                .count(),
            nodes
                .iter64()
                .filter(|n| matches!(n, Node64::Row(_)))
                .count(),
            nodes
                .iter96()
                .filter(|n| matches!(n, Node96::Reference(_)))
                .count(),
        ]
    }

    for width in [8, 64, 256] {
        let names: Vec<_> = (0..width).map(|i| format!("c{i}")).collect();
        let columns: Vec<_> = names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let ty = if i % 2 == 0 {
                    UnrestrictedType::Integer
                } else {
                    UnrestrictedType::String
                };
                (name.as_str(), ty, false)
            })
            .collect();
        let mut catalog = MockCatalog::new();
        catalog.add_sharded("wide", &columns, &["c0"], &["c0"]);
        let plan = transform_into_plan(
            "SELECT l.c0 FROM wide l JOIN wide r ON l.c0 = r.c0 WHERE l.c0 = 1",
            &[],
            &catalog,
        )
        .unwrap();
        let top = plan.get_top().unwrap();
        let plan = plan
            .optimize_before(top, Stage::EnrichRestrictions)
            .unwrap();
        let before = output_counts(&plan);
        let old_nodes64 = plan.get_nodes().iter64().len();
        let plan = plan.enrich_restrictions_from_facts(top).unwrap();
        let after = output_counts(&plan);

        // Each projection includes the hidden bucket_id; the two new predicates
        // add one reference each, independently of table width.
        assert_eq!(after[0] - before[0], 2 * (width + 1), "width {width}");
        assert_eq!(after[1] - before[1], 2, "width {width}");
        assert_eq!(after[2] - before[2], 2 * (width + 1) + 2, "width {width}");

        let mut carriers = 0;
        for (id, node) in plan.get_nodes().iter64_with_ids().skip(old_nodes64) {
            let child = match node {
                Node64::Selection(selection) => selection.child,
                Node64::ScanSubQuery(scan) => scan.child,
                _ => continue,
            };
            carriers += 1;
            let produced = plan
                .columns_of(id)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let input = plan
                .columns_of(child)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(produced, input);
            assert_eq!(produced.len(), width + 1);
            for (i, column) in produced[..width].iter().enumerate() {
                assert_eq!(column.name, names[i]);
                assert_eq!(column.r#type, DerivedType::new(columns[i].1));
                assert!(!column.is_system);
            }
            assert_eq!(produced[width].name, "bucket_id");
            assert!(produced[width].is_system);
        }
        assert_eq!(carriers, 4);
    }
}

#[test]
fn name_clash_folds_into_the_inner_on_others_still_descend() {
    // r.b = t3.b AND t3.b = 1 pins the class {l.a, r.b, t3.b, 1}. r is the
    // null-extendable side of a LEFT JOIN of `t` with itself, so `r.b = 1` cannot
    // reach r's base scan; its deepest spot is the left-join output, an input of
    // the inner join. A carrier there would expose l.* and r.* (both from `t`)
    // under one qualifier and collapse their names, so the clause folds into the
    // inner join's own ON instead. `t3.b = 1` has a unique-name scan, so it keeps
    // the deep carrier push-down onto t3's own scan.
    let plan = run_enrich(
        r#"SELECT * FROM "t" l LEFT JOIN "t" r ON l."a" = r."b" JOIN "t3" ON r."b" = "t3"."b" AND "t3"."b" = 1"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (l.a::int -> a, l.b::int -> b, l.c::int -> c, l.d::int -> d, r.a::int -> a, r.b::int -> b, r.c::int -> c, r.d::int -> d, t3.a::string -> a, t3.b::int -> b)
      join on ((r.b::int = t3.b::int and t3.b::int = 1::int and r.b::int = 1::int))
        left join on (l.a::int = r.b::int)
          scan t -> l
          scan t -> r
        scan t3
          projection (t3.bucket_id::int -> bucket_id, t3.a::string -> a, t3.b::int -> b)
            selection (t3.b::int = 1::int)
              scan t3
    ");
}

#[test]
fn a_derived_clause_lifts_through_nested_left_joins_into_the_inner_on() {
    // Exercises place_column's lift loop. `r.b = t3.b AND t3.b = 1` pins the class
    // {r.b, t3.b, 1}. `r` is a nullable LEFT JOIN side, so `r.b = 1` cannot
    // reach scan r; a carrier over a LEFT JOIN output is refused, and the enclosing
    // second LEFT JOIN's ON is no host. The loop lifts the same `r.b` to the second
    // LEFT JOIN's output, where it remains a class member because the first join
    // is on the preserved side. It then lands in the top INNER JOIN's ON. Neither
    // LEFT JOIN's ON gets the equality and no carrier wraps either join. `t3.b = 1`
    // descends independently to scan t3 through a carrier.
    let plan = run_enrich(
        r#"SELECT "l"."a", "r"."b" FROM "t" l
           LEFT JOIN "t" r ON l."a" = r."a"
           LEFT JOIN "t" v ON l."a" = v."a"
           JOIN "t3" ON r."b" = "t3"."b" AND "t3"."b" = 1"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (l.a::int -> a, r.b::int -> b)
      join on ((r.b::int = t3.b::int and t3.b::int = 1::int and r.b::int = 1::int))
        left join on (l.a::int = v.a::int)
          left join on (l.a::int = r.a::int)
            scan t -> l
            scan t -> r
          scan t -> v
        scan t3
          projection (t3.bucket_id::int -> bucket_id, t3.a::string -> a, t3.b::int -> b)
            selection (t3.b::int = 1::int)
              scan t3
    ");

    // On global tables the whole query stays one statement; `r.b` keeps its `r`
    // qualifier in the top ON (it is not rewritten into a carrier's single alias).
    let sql = optimized_join_sql(
        "SELECT l.a, r.b FROM t l \
         LEFT JOIN t r ON l.a = r.a \
         LEFT JOIN t v ON l.a = v.a \
         JOIN t3 ON r.b = t3.b AND t3.b = 1",
    );
    assert_snapshot!(sql, @r#"SELECT "l"."a", "r"."b" FROM "t" as "l" LEFT JOIN "t" as "r" ON "l"."a" = "r"."a" LEFT JOIN "t" as "v" ON "l"."a" = "v"."a" INNER JOIN (SELECT * FROM "t3" WHERE "t3"."b" = CAST($1 AS int)) as "t3" ON "r"."b" = "t3"."b" and ("r"."b", "t3"."b") = (CAST($2 AS int), CAST($3 AS int))"#);
}

/// Output column names of the plan's top relational node, in order.
fn top_column_names(plan: &Plan) -> Vec<String> {
    plan.columns_of(plan.get_top().unwrap())
        .unwrap()
        .map(|column| column.unwrap().name.into_owned())
        .collect()
}

/// Global tables keep the entire optimized query in one SQL statement, including
/// the projections and joins whose column identities we want to check.
fn optimized_join_sql(query: &str) -> String {
    let ex_plan = ExecutionPlan::new(optimized_join_plan(query, false));
    let top = ex_plan.get_ir_plan().get_top().unwrap();
    join_sql(&ex_plan, top, Snapshot::Latest)
}

fn optimized_join_plan(query: &str, sharded: bool) -> Plan {
    join_plan(query, sharded).optimize().unwrap()
}

fn join_plan(query: &str, sharded: bool) -> Plan {
    use sql_ast_new_corpus::MockCatalog;
    use sql_frontend::frontend::sql::transform_into_plan;

    let mut catalog = MockCatalog::new();
    let columns = [
        ("a", UnrestrictedType::Integer, false),
        ("b", UnrestrictedType::Integer, false),
        ("col_0", UnrestrictedType::Integer, false),
    ];
    for table in ["t", "t3"] {
        if sharded {
            catalog.add_sharded(table, &columns, &["a"], &["a"]);
        } else {
            catalog.add_global(table, &columns, &["a"]);
        }
    }
    for (table, column) in [("u", "x"), ("v", "y")] {
        let columns = [(column, UnrestrictedType::Integer, false)];
        if sharded {
            catalog.add_sharded(table, &columns, &[column], &[column]);
        } else {
            catalog.add_global(table, &columns, &[column]);
        }
    }
    transform_into_plan(query, &[], &catalog).unwrap()
}

fn join_sql(ex_plan: &ExecutionPlan, top: NodeId, snapshot: Snapshot) -> String {
    let subtree = ex_plan.freeze().execution_view().dql_subtree(top).unwrap();
    let params = ex_plan.local_sql_params(top, snapshot).unwrap();
    let syntax = SyntaxPlan::new_for_dql_subtree(&subtree, snapshot).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(syntax).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    subtree
        .generate_sql(&nodes, 0, table_name, Some(params.constant_ids().to_vec()))
        .unwrap()
}

#[test]
fn preserved_side_filter_stays_below_left_join_in_sql() {
    // The derived l.a = 1 filters the left scan; the original WHERE remains above
    // the LEFT JOIN, so the equality appears at both levels.
    let sql =
        optimized_join_sql("SELECT l.a, r.a FROM t l LEFT JOIN t r ON l.a = r.b WHERE l.a = 1");
    assert_snapshot!(sql, @r#"SELECT "l"."a", "r"."a" FROM (SELECT * FROM "t" as "l" WHERE "l"."a" = CAST($1 AS int)) as "l" LEFT JOIN "t" as "r" ON "l"."a" = "r"."b" WHERE "l"."a" = CAST($2 AS int)"#);
}

#[test]
fn nullable_side_filter_stays_above_left_join_in_sql() {
    // The derived r.b = 1 stays above the LEFT JOIN alongside the original
    // equality; `(r.b, r.b) = ($1, $2)` contains both conditions.
    let sql =
        optimized_join_sql("SELECT l.a, r.a FROM t l LEFT JOIN t r ON l.a = r.b WHERE r.b = 1");
    assert_snapshot!(sql, @r#"SELECT "l"."a", "r"."a" FROM "t" as "l" LEFT JOIN "t" as "r" ON "l"."a" = "r"."b" WHERE ("r"."b", "r"."b") = (CAST($1 AS int), CAST($2 AS int))"#);
}

#[test]
fn unique_subquery_output_still_descends_through_a_carrier() {
    // No name clash and a single qualifier: a carrier over the unique-name
    // subquery is sound, so the derived s.c = 1 keeps its deep push-down onto the
    // operand rather than lifting. (A carrier over a JOIN would collapse
    // qualifiers, so those cases use ON or lift to a parent.)
    let plan = optimized_join_plan(
        "SELECT * FROM (SELECT DISTINCT a AS x, b AS y, col_0 AS c FROM t) s \
         JOIN t3 ON s.c = t3.col_0 AND t3.col_0 = 1",
        false,
    );
    let top = plan.get_top().unwrap();
    let ex_plan = ExecutionPlan::new(plan);
    // The carrier over the unique-name subquery renders as `SELECT *`, keeping the
    // deep push-down onto the operand (its own `WHERE "s"."c"`).
    let mut rendered = String::new();
    for snapshot in [Snapshot::Latest, Snapshot::Oldest] {
        rendered.push_str(&format!("{}\n", join_sql(&ex_plan, top, snapshot)));
    }
    assert_snapshot!(rendered, @r#"
    SELECT * FROM (SELECT * FROM (SELECT DISTINCT "t"."a" as "x", "t"."b" as "y", "t"."col_0" as "c" FROM "t") as "s" WHERE "s"."c" = CAST($1 AS int)) as "s" INNER JOIN (SELECT * FROM "t3" WHERE "t3"."col_0" = CAST($2 AS int)) as "t3" ON "s"."c" = "t3"."col_0" and "t3"."col_0" = CAST($3 AS int)
    SELECT * FROM (SELECT * FROM (SELECT DISTINCT "t"."a" as "x", "t"."b" as "y", "t"."col_0" as "c" FROM "t") as "s" WHERE "s"."c" = CAST($1 AS int)) as "s" INNER JOIN (SELECT * FROM "t3" WHERE "t3"."col_0" = CAST($2 AS int)) as "t3" ON "s"."c" = "t3"."col_0" and "t3"."col_0" = CAST($3 AS int)
    "#);
}

#[test]
fn duplicate_names_carry_only_when_the_filter_column_is_unique() {
    // A `SELECT *` carrier passes duplicate names through positionally, so it is
    // sound whenever the *filter* column is uniquely named. `s.c` (unique `c`) thus
    // descends into a `SELECT *` carrier over its INNER-join operand; the self-join
    // `r.b` (ambiguous `b`) and any `s.c` on a LEFT JOIN's nullable side instead
    // fold into the inner join's ON. Either way the wildcard top schema is
    // unchanged and no `_1` rename appears.
    let mut inputs = vec![("t l LEFT JOIN t r ON l.a = r.b".to_owned(), "r.b")];
    for body in [
        "SELECT DISTINCT a AS x, b AS x, col_0 AS c FROM t",
        "SELECT a AS x, b AS x, col_0 AS c FROM t LIMIT 10",
    ] {
        inputs.extend([
            (format!("({body}) s"), "s.c"),
            (format!("({body}) s LEFT JOIN t r ON s.c = r.b"), "r.b"),
            (format!("t l LEFT JOIN ({body}) s ON l.col_0 = s.c"), "s.c"),
        ]);
    }
    let mut rendered = String::new();
    for (input, key) in inputs {
        let query = format!("SELECT * FROM {input} JOIN t3 ON {key} = t3.b AND t3.b = 1");
        let original = join_plan(&query, false);
        let expected = top_column_names(&original);
        let plan = original.optimize().unwrap();
        // Neither a carrier nor an ON fold renames, so the wildcard schema is kept.
        assert_eq!(top_column_names(&plan), expected, "{query}");
        assert!(!top_column_names(&plan).iter().any(|n| n.ends_with("_1")));
        let top = plan.get_top().unwrap();
        let ex_plan = ExecutionPlan::new(plan);
        let sql = join_sql(&ex_plan, top, Snapshot::Latest);
        rendered.push_str(&format!("{query}\n{sql}\n\n"));
    }

    assert_snapshot!(rendered, @r#"
    SELECT * FROM t l LEFT JOIN t r ON l.a = r.b JOIN t3 ON r.b = t3.b AND t3.b = 1
    SELECT * FROM "t" as "l" LEFT JOIN "t" as "r" ON "l"."a" = "r"."b" INNER JOIN (SELECT * FROM "t3" WHERE "t3"."b" = CAST($1 AS int)) as "t3" ON "r"."b" = "t3"."b" and ("r"."b", "t3"."b") = (CAST($2 AS int), CAST($3 AS int))

    SELECT * FROM (SELECT DISTINCT a AS x, b AS x, col_0 AS c FROM t) s JOIN t3 ON s.c = t3.b AND t3.b = 1
    SELECT * FROM (SELECT * FROM (SELECT DISTINCT "t"."a" as "x", "t"."b" as "x", "t"."col_0" as "c" FROM "t") as "s" WHERE "s"."c" = CAST($1 AS int)) as "s" INNER JOIN (SELECT * FROM "t3" WHERE "t3"."b" = CAST($2 AS int)) as "t3" ON "s"."c" = "t3"."b" and "t3"."b" = CAST($3 AS int)

    SELECT * FROM (SELECT DISTINCT a AS x, b AS x, col_0 AS c FROM t) s LEFT JOIN t r ON s.c = r.b JOIN t3 ON r.b = t3.b AND t3.b = 1
    SELECT * FROM (SELECT DISTINCT "t"."a" as "x", "t"."b" as "x", "t"."col_0" as "c" FROM "t") as "s" LEFT JOIN "t" as "r" ON "s"."c" = "r"."b" INNER JOIN (SELECT * FROM "t3" WHERE "t3"."b" = CAST($1 AS int)) as "t3" ON "r"."b" = "t3"."b" and ("r"."b", "t3"."b") = (CAST($2 AS int), CAST($3 AS int))

    SELECT * FROM t l LEFT JOIN (SELECT DISTINCT a AS x, b AS x, col_0 AS c FROM t) s ON l.col_0 = s.c JOIN t3 ON s.c = t3.b AND t3.b = 1
    SELECT * FROM "t" as "l" LEFT JOIN (SELECT DISTINCT "t"."a" as "x", "t"."b" as "x", "t"."col_0" as "c" FROM "t") as "s" ON "l"."col_0" = "s"."c" INNER JOIN (SELECT * FROM "t3" WHERE "t3"."b" = CAST($1 AS int)) as "t3" ON "s"."c" = "t3"."b" and ("s"."c", "t3"."b") = (CAST($2 AS int), CAST($3 AS int))

    SELECT * FROM (SELECT a AS x, b AS x, col_0 AS c FROM t LIMIT 10) s JOIN t3 ON s.c = t3.b AND t3.b = 1
    SELECT * FROM (SELECT * FROM (SELECT "t"."a" as "x", "t"."b" as "x", "t"."col_0" as "c" FROM "t" LIMIT 10) as "s" WHERE "s"."c" = CAST($1 AS int)) as "s" INNER JOIN (SELECT * FROM "t3" WHERE "t3"."b" = CAST($2 AS int)) as "t3" ON "s"."c" = "t3"."b" and "t3"."b" = CAST($3 AS int)

    SELECT * FROM (SELECT a AS x, b AS x, col_0 AS c FROM t LIMIT 10) s LEFT JOIN t r ON s.c = r.b JOIN t3 ON r.b = t3.b AND t3.b = 1
    SELECT * FROM (SELECT "t"."a" as "x", "t"."b" as "x", "t"."col_0" as "c" FROM "t" LIMIT 10) as "s" LEFT JOIN "t" as "r" ON "s"."c" = "r"."b" INNER JOIN (SELECT * FROM "t3" WHERE "t3"."b" = CAST($1 AS int)) as "t3" ON "r"."b" = "t3"."b" and ("r"."b", "t3"."b") = (CAST($2 AS int), CAST($3 AS int))

    SELECT * FROM t l LEFT JOIN (SELECT a AS x, b AS x, col_0 AS c FROM t LIMIT 10) s ON l.col_0 = s.c JOIN t3 ON s.c = t3.b AND t3.b = 1
    SELECT * FROM "t" as "l" LEFT JOIN (SELECT "t"."a" as "x", "t"."b" as "x", "t"."col_0" as "c" FROM "t" LIMIT 10) as "s" ON "l"."col_0" = "s"."c" INNER JOIN (SELECT * FROM "t3" WHERE "t3"."b" = CAST($1 AS int)) as "t3" ON "s"."c" = "t3"."b" and ("s"."c", "t3"."b") = (CAST($2 AS int), CAST($3 AS int))
    "#);
}

#[test]
fn duplicate_names_with_unique_filter_column_render_a_star_carrier() {
    // Positive case for the relaxed carrier: the operand exposes two ambiguous `x`
    // columns but a unique `c`. The carrier is a real `SELECT *`, so both `x`s pass
    // through positionally and `s.c = 1` descends into the carrier's own WHERE, a
    // push-down the old all-names-unique check forbade (it folded into the ON). The
    // rendered SQL never re-emits the ambiguous `x` pair as an explicit list.
    let sql = optimized_join_sql(
        "SELECT * FROM (SELECT DISTINCT a AS x, b AS x, col_0 AS c FROM t) s \
         JOIN t3 ON s.c = t3.b AND t3.b = 1",
    );
    assert_snapshot!(sql, @r#"SELECT * FROM (SELECT * FROM (SELECT DISTINCT "t"."a" as "x", "t"."b" as "x", "t"."col_0" as "c" FROM "t") as "s" WHERE "s"."c" = CAST($1 AS int)) as "s" INNER JOIN (SELECT * FROM "t3" WHERE "t3"."b" = CAST($2 AS int)) as "t3" ON "s"."c" = "t3"."b" and "t3"."b" = CAST($3 AS int)"#);
}

#[test]
fn a_sharded_join_bucket_id_clash_folds_into_the_inner_on() {
    // u and v are sharded, so the LEFT JOIN output carries two `bucket_id`
    // columns: a carrier would collapse them. `r.y = 1` therefore folds into the
    // inner join's ON. x and y are unique, so nothing else is disturbed.
    let query = "SELECT l.*, r.* FROM u l LEFT JOIN v r ON l.x = r.y \
                 JOIN t3 ON r.y = t3.a AND t3.a = 1";
    let plan = join_plan(query, true);
    let top = plan.get_top().unwrap();
    let plan = plan
        .optimize_before(top, Stage::EnrichRestrictions)
        .unwrap()
        .enrich_restrictions_from_facts(top)
        .unwrap();
    assert_snapshot!(logical(&plan), @r"
    projection (l.x::int -> x, r.y::int -> y)
      join on ((r.y::int = t3.a::int and t3.a::int = 1::int and r.y::int = 1::int))
        left join on (l.x::int = r.y::int)
          scan u -> l
          scan v -> r
        scan t3
          projection (t3.a::int -> a, t3.b::int -> b, t3.col_0::int -> col_0, t3.bucket_id::int -> bucket_id)
            selection (t3.a::int = 1::int)
              scan t3
    ");

    // Full optimization (motions included) succeeds; the schema is only x, y with
    // no system column, and routing survives (shard column tracked by position).
    let plan = optimized_join_plan(query, true);
    let columns: Vec<_> = plan
        .columns_of(plan.get_top().unwrap())
        .unwrap()
        .map(|column| {
            let column = column.unwrap();
            assert!(!column.is_system);
            column.name.into_owned()
        })
        .collect();
    assert_eq!(columns, ["x", "y"]);
}

#[test]
fn shared_cte_duplicate_names_descend_through_a_star_carrier() {
    // A CTE with duplicate output names is scanned twice and self-joined. `c` is
    // unique, so the derived l.c = 1 and r.c = 1 each descend into a `SELECT *`
    // carrier over their OWN CTE scan; the shared body is untouched (each call site
    // is wrapped independently).
    let mut rendered = String::new();
    for repeated in ["b", "a"] {
        let query = format!(
            "WITH s AS (SELECT DISTINCT a AS x, {repeated} AS x, col_0 AS c FROM t) \
             SELECT * FROM s l JOIN s r ON l.c = r.c AND l.c = 1"
        );
        let plan = optimized_join_plan(&query, false);
        rendered.push_str(&format!("{query}\n{}\n\n", logical(&plan)));
    }
    assert_snapshot!(rendered, @r"
    WITH s AS (SELECT DISTINCT a AS x, b AS x, col_0 AS c FROM t) SELECT * FROM s l JOIN s r ON l.c = r.c AND l.c = 1
    projection (l.x::int -> x, l.x::int -> x, l.c::int -> c, r.x::int -> x, r.x::int -> x, r.c::int -> c)
      join on ((l.c::int = r.c::int and l.c::int = 1::int))
        scan l
          projection (l.x::int -> x, l.x::int -> x, l.c::int -> c)
            selection (l.c::int = 1::int)
              scan cte l($0)
        scan r
          projection (r.x::int -> x, r.x::int -> x, r.c::int -> c)
            selection (r.c::int = 1::int)
              scan cte r($0)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        projection (t.a::int -> x, t.b::int -> x, t.col_0::int -> c)
          scan t

    WITH s AS (SELECT DISTINCT a AS x, a AS x, col_0 AS c FROM t) SELECT * FROM s l JOIN s r ON l.c = r.c AND l.c = 1
    projection (l.x::int -> x, l.x::int -> x, l.c::int -> c, r.x::int -> x, r.x::int -> x, r.c::int -> c)
      join on ((l.c::int = r.c::int and l.c::int = 1::int))
        scan l
          projection (l.x::int -> x, l.x::int -> x, l.c::int -> c)
            selection (l.c::int = 1::int)
              scan cte l($0)
        scan r
          projection (r.x::int -> x, r.x::int -> x, r.c::int -> c)
            selection (r.c::int = 1::int)
              scan cte r($0)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        projection (t.a::int -> x, t.a::int -> x, t.col_0::int -> c)
          scan t
    ");
}

#[test]
fn setop_operand_duplicate_names_descend_through_a_star_carrier() {
    // A set-op operand has duplicate output names but a unique `c`, so the derived
    // u.c = 1 descends into a `SELECT *` carrier over the operand for every set-op
    // kind (a set-op is not a join, so the qualifier-collapse ban does not apply).
    let mut rendered = String::new();
    for op in ["UNION ALL", "EXCEPT", "UNION"] {
        let query = format!(
            "SELECT * FROM (SELECT a AS x, b AS x, col_0 AS c FROM t \
             {op} SELECT a AS x, b AS x, col_0 AS c FROM t3) u \
             JOIN t3 ON u.c = t3.b AND t3.b = 1"
        );
        let plan = optimized_join_plan(&query, false);
        rendered.push_str(&format!("{query}\n{}\n\n", logical(&plan)));
    }
    assert_snapshot!(rendered, @r"
    SELECT * FROM (SELECT a AS x, b AS x, col_0 AS c FROM t UNION ALL SELECT a AS x, b AS x, col_0 AS c FROM t3) u JOIN t3 ON u.c = t3.b AND t3.b = 1
    projection (u.x::int -> x, u.x::int -> x, u.c::int -> c, t3.a::int -> a, t3.b::int -> b, t3.col_0::int -> col_0)
      join on ((u.c::int = t3.b::int and t3.b::int = 1::int))
        scan u
          projection (u.x::int -> x, u.x::int -> x, u.c::int -> c)
            selection (u.c::int = 1::int)
              scan u
                union all
                  projection (t.a::int -> x, t.b::int -> x, t.col_0::int -> c)
                    scan t
                  projection (t3.a::int -> x, t3.b::int -> x, t3.col_0::int -> c)
                    scan t3
        scan t3
          projection (t3.a::int -> a, t3.b::int -> b, t3.col_0::int -> col_0)
            selection (t3.b::int = 1::int)
              scan t3

    SELECT * FROM (SELECT a AS x, b AS x, col_0 AS c FROM t EXCEPT SELECT a AS x, b AS x, col_0 AS c FROM t3) u JOIN t3 ON u.c = t3.b AND t3.b = 1
    projection (u.x::int -> x, u.x::int -> x, u.c::int -> c, t3.a::int -> a, t3.b::int -> b, t3.col_0::int -> col_0)
      join on ((u.c::int = t3.b::int and t3.b::int = 1::int))
        scan u
          projection (u.x::int -> x, u.x::int -> x, u.c::int -> c)
            selection (u.c::int = 1::int)
              scan u
                except
                  projection (t.a::int -> x, t.b::int -> x, t.col_0::int -> c)
                    scan t
                  projection (t3.a::int -> x, t3.b::int -> x, t3.col_0::int -> c)
                    scan t3
        scan t3
          projection (t3.a::int -> a, t3.b::int -> b, t3.col_0::int -> col_0)
            selection (t3.b::int = 1::int)
              scan t3

    SELECT * FROM (SELECT a AS x, b AS x, col_0 AS c FROM t UNION SELECT a AS x, b AS x, col_0 AS c FROM t3) u JOIN t3 ON u.c = t3.b AND t3.b = 1
    projection (u.x::int -> x, u.x::int -> x, u.c::int -> c, t3.a::int -> a, t3.b::int -> b, t3.col_0::int -> col_0)
      join on ((u.c::int = t3.b::int and t3.b::int = 1::int))
        scan u
          projection (u.x::int -> x, u.x::int -> x, u.c::int -> c)
            selection (u.c::int = 1::int)
              scan u
                motion [policy: full, program: RemoveDuplicates]
                  union
                    projection (t.a::int -> x, t.b::int -> x, t.col_0::int -> c)
                      scan t
                    projection (t3.a::int -> x, t3.b::int -> x, t3.col_0::int -> c)
                      scan t3
        scan t3
          projection (t3.a::int -> a, t3.b::int -> b, t3.col_0::int -> col_0)
            selection (t3.b::int = 1::int)
              scan t3
    ");
}

#[test]
fn separate_carriers_keep_each_operands_column_identity() {
    // Both operands have duplicate names but a unique c. Derive both l.c = 1 and
    // r.c = 1, each in a SELECT * carrier referencing its own operand.
    let plan = optimized_join_plan(
        "SELECT * FROM (SELECT DISTINCT a AS x, b AS x, col_0 AS c FROM t) l \
         JOIN (SELECT DISTINCT a AS x, b AS x, col_0 AS c FROM t3) r \
         ON l.c = r.c AND l.c = 1",
        false,
    );
    assert_snapshot!(logical(&plan), @r"
    projection (l.x::int -> x, l.x::int -> x, l.c::int -> c, r.x::int -> x, r.x::int -> x, r.c::int -> c)
      join on ((l.c::int = r.c::int and l.c::int = 1::int))
        scan l
          projection (l.x::int -> x, l.x::int -> x, l.c::int -> c)
            selection (l.c::int = 1::int)
              scan l
                projection (t.a::int -> x, t.b::int -> x, t.col_0::int -> c)
                  scan t
        scan r
          projection (r.x::int -> x, r.x::int -> x, r.c::int -> c)
            selection (r.c::int = 1::int)
              scan r
                projection (t3.a::int -> x, t3.b::int -> x, t3.col_0::int -> c)
                  scan t3
    ");
}

#[test]
fn a_carrier_is_not_built_over_a_join_with_unique_names() {
    // Unique output names do not make a carrier over the LEFT JOIN safe: its
    // single alias would hide l and r. Place r.y = 1 in the INNER JOIN's ON,
    // keeping l.* bound to u's single column x.
    let sql = optimized_join_sql(
        "SELECT l.* FROM u l LEFT JOIN v r ON l.x = r.y JOIN t3 ON r.y = t3.b AND t3.b = 1",
    );
    assert_snapshot!(sql, @r#"SELECT "l".* FROM "u" as "l" LEFT JOIN "v" as "r" ON "l"."x" = "r"."y" INNER JOIN (SELECT * FROM "t3" WHERE "t3"."b" = CAST($1 AS int)) as "t3" ON "r"."y" = "t3"."b" and ("r"."y", "t3"."b") = (CAST($2 AS int), CAST($3 AS int))"#);
}

#[test]
fn a_param_gate_never_builds_a_carrier_over_a_join() {
    // The class {l.a, r.b, $1, 1} is anchored at the INNER JOIN below GROUP BY.
    // Place $1 = 1 in its ON, preserving the l/r qualifiers and the outer r.a.
    // The column predicates l.a = 1 and r.b = 1 reach their respective scans.
    let query =
        "SELECT r.a, count(*) FROM t l JOIN t r ON l.a = r.b AND r.b = $1 AND r.b = 1 GROUP BY r.a";
    let plan = join_plan(query, false);
    let top = plan.get_top().unwrap();
    let plan = plan
        .optimize_before(top, Stage::EnrichRestrictions)
        .unwrap()
        .enrich_restrictions_from_facts(top)
        .unwrap();
    // Assert the IR: SQL generation for an unbound parameter in JOIN ON is not
    // supported by this test's backend path.
    assert_snapshot!(logical(&plan), @r"
    projection (r.a::int -> a, count(*)::int -> col_1)
      group by (r.a::int)
        join on ((l.a::int = r.b::int and r.b::int = $1::int and r.b::int = 1::int and $1::int = 1::int))
          scan l
            projection (l.a::int -> a, l.b::int -> b, l.col_0::int -> col_0)
              selection (l.a::int = 1::int)
                scan t -> l
          scan r
            projection (r.a::int -> a, r.b::int -> b, r.col_0::int -> col_0)
              selection (r.b::int = 1::int)
                scan t -> r
    ");
}

#[test]
fn a_param_gate_stays_inside_its_domain() {
    // The check $1 = 1 belongs to the subquery's INNER JOIN. Keep it in that ON:
    // when $1 != 1, the subquery is empty but LEFT JOIN must preserve o's rows.
    // A check above the outer LEFT JOIN would incorrectly remove those rows.
    let query = "SELECT o.a FROM t o LEFT JOIN (\
        SELECT r.a AS k, count(*) AS x, count(*) AS x \
        FROM t l JOIN t r ON l.a = r.b AND r.b = $1 AND r.b = 1 GROUP BY r.a\
    ) s ON o.a = s.k";
    let plan = join_plan(query, false);
    let top = plan.get_top().unwrap();
    let plan = plan
        .optimize_before(top, Stage::EnrichRestrictions)
        .unwrap()
        .enrich_restrictions_from_facts(top)
        .unwrap();
    assert_snapshot!(logical(&plan), @r"
    projection (o.a::int -> a)
      left join on (o.a::int = s.k::int)
        scan t -> o
        scan s
          projection (r.a::int -> k, count(*)::int -> x, count(*)::int -> x)
            group by (r.a::int)
              join on ((l.a::int = r.b::int and r.b::int = $1::int and r.b::int = 1::int and $1::int = 1::int))
                scan l
                  projection (l.a::int -> a, l.b::int -> b, l.col_0::int -> col_0)
                    selection (l.a::int = 1::int)
                      scan t -> l
                scan r
                  projection (r.a::int -> a, r.b::int -> b, r.col_0::int -> col_0)
                    selection (r.b::int = 1::int)
                      scan t -> r
    ");
}

#[test]
fn a_param_gate_over_a_join_subquery_prefers_a_new_selection_to_the_on() {
    // The IN subquery body is a bare Projection over an INNER JOIN with no WHERE,
    // so the class {t2.e, t3.b, $1, 7} anchors at the subquery. The `$1 = 7` check
    // must splice a new Selection above the join rather than fold into its ON: a
    // check that folds to `false` prunes at the router only inside a Selection.
    let plan = run_enrich(
        r#"SELECT "a" FROM "t" WHERE "b" IN (SELECT "t2"."e" FROM "t2" JOIN "t3" ON "t2"."e" = "t3"."b" AND "t2"."e" = $1 AND "t2"."e" = 7)"#,
        &[DerivedType::new(UnrestrictedType::Integer); 1],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection (t.b::int in ROW($0))
        scan t
    subquery $0:
      scan
        projection (t2.e::int -> e)
          selection ($1::int = 7::int)
            join on ((t2.e::int = t3.b::int and t2.e::int = $1::int and t2.e::int = 7::int))
              scan t2
                projection (t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t2.bucket_id::int -> bucket_id)
                  selection (t2.e::int = 7::int)
                    scan t2
              scan t3
                projection (t3.bucket_id::int -> bucket_id, t3.a::string -> a, t3.b::int -> b)
                  selection (t3.b::int = 7::int)
                    scan t3
    ");
}

#[test]
fn different_columns_of_a_class_on_one_node_do_not_shadow_each_other() {
    // The class contains both l.a and r.b on the LEFT JOIN output. Only l.a can
    // reach its scan; r.b = 1 must stay in the WHERE above null-extension.
    // Choose the lowest representation per column, not per relational node.
    // The WHERE rejects unmatched rows whose r.b is NULL.
    let plan = run_enrich(
        r#"SELECT "l"."a", "r"."b" FROM "t" "l" LEFT JOIN "t" "r" ON "l"."a" = "r"."b" WHERE "l"."a" = "r"."b" AND "l"."a" = 1"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (l.a::int -> a, r.b::int -> b)
      selection ((l.a::int = r.b::int and l.a::int = 1::int and r.b::int = 1::int))
        left join on (l.a::int = r.b::int)
          scan l
            projection (l.a::int -> a, l.b::int -> b, l.c::int -> c, l.d::int -> d, l.bucket_id::int -> bucket_id)
              selection (l.a::int = 1::int)
                scan t -> l
          scan t -> r
    ");
}

#[test]
fn a_column_predicate_folds_into_an_inner_join_on_under_group_by() {
    // Keep r.b = 1 above the LEFT JOIN's null-extension and below GROUP BY by
    // placing it in the INNER JOIN's ON. The other member, t3.b, reaches its scan.
    let plan = run_enrich(
        r#"SELECT "r"."b", count(*) FROM "t" "l" LEFT JOIN "t" "r" ON "l"."a" = "r"."b" JOIN "t3" ON "r"."b" = "t3"."b" AND "t3"."b" = 1 GROUP BY "r"."b""#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (r.b::int -> b, count(*)::int -> col_1)
      group by (r.b::int)
        join on ((r.b::int = t3.b::int and t3.b::int = 1::int and r.b::int = 1::int))
          left join on (l.a::int = r.b::int)
            scan t -> l
            scan t -> r
          scan t3
            projection (t3.bucket_id::int -> bucket_id, t3.a::string -> a, t3.b::int -> b)
              selection (t3.b::int = 1::int)
                scan t3
    ");
}

#[test]
fn a_const_crosses_three_inner_joins_to_every_scan() {
    // The class {t.a, t2.e, t3.b, 1} spans a chain of three INNER JOINs with no
    // outer join in the way, so `= 1` reaches all three base scans.
    let plan = run_enrich(
        r#"SELECT "t"."a" FROM "t" JOIN "t2" ON "t"."a" = "t2"."e" JOIN "t3" ON "t2"."e" = "t3"."b" WHERE "t"."a" = 1"#,
        &[],
    );
    assert_snapshot!(logical(&plan), @r"
    projection (t.a::int -> a)
      selection (t.a::int = 1::int)
        join on (t2.e::int = t3.b::int)
          join on (t.a::int = t2.e::int)
            scan t
              projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
                selection (t.a::int = 1::int)
                  scan t
            scan t2
              projection (t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t2.bucket_id::int -> bucket_id)
                selection (t2.e::int = 1::int)
                  scan t2
          scan t3
            projection (t3.bucket_id::int -> bucket_id, t3.a::string -> a, t3.b::int -> b)
              selection (t3.b::int = 1::int)
                scan t3
    ");
}

#[test]
fn a_delete_where_pins_the_base_scan() {
    // DELETE ... WHERE a = b AND b = 1 pins the class {a, b, 1}; the derived a = 1
    // must reach the scan feeding the delete.
    let plan = run_enrich(r#"DELETE FROM "t" WHERE "a" = "b" AND "b" = 1"#, &[]);
    assert_snapshot!(logical(&plan), @r"
    delete from t
      projection (t.b::int -> pk_col_0)
        selection ((t.a::int = t.b::int and t.b::int = 1::int and t.a::int = 1::int and t.b::int = 1::int))
          scan t
    ");
}

#[test]
fn an_update_where_pins_the_base_scan() {
    // UPDATE ... WHERE a = b AND b = 1 pins the class {a, b, 1}; the derived a = 1
    // must reach the scan feeding the update.
    let plan = run_enrich(r#"UPDATE "t" SET "c" = 5 WHERE "a" = "b" AND "b" = 1"#, &[]);
    assert_snapshot!(logical(&plan), @r"
    update t (c = col_0)
      projection (5::int -> col_0, t.b::int -> col_1)
        selection ((t.a::int = t.b::int and t.b::int = 1::int and t.a::int = 1::int and t.b::int = 1::int))
          scan t
    ");
}
