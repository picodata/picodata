//! Benchmark for the `push_down_restrictions` pass alone.
//!
//! Parsing, binding and every pass before it (restrictions, equality facts,
//! enrichment) run in the setup; DNF and later passes never run. Only
//! [`Plan::push_down_restrictions`] is timed, over a clone of the prepared plan.
//!
//! Shapes, each over a growing number of conjuncts `n`:
//! - `independent/n`: `n` ORs over one join operand, `n` components of one;
//! - `component/n`: `n` conjuncts sharing one subquery body, each reading one
//!   of eight columns of either operand, so one component moves into the
//!   join's ON; its column map stays at most eight wide;
//! - `wide_component/n`: `n` conjuncts sharing one body, each reading its own
//!   column of an `n`-column table, so the one component that moves below the
//!   join maps `n` distinct columns;
//! - `many_bodies/n`: `n` conjuncts with a body each, all landing on one new
//!   filter, which then lists `n` subqueries.
//!
//! The setup of the last two checks the conjuncts, distinct columns and
//! bodies it built, and that one run moves them all.
//!
//! # Running
//!
//! ```sh
//! taskset -c 2 cargo bench -p sql-planner --features enrich_restrictions --bench push_down
//! ```

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};

use sql_ast_new_corpus::MockCatalog;
use sql_executor::executor::engine::mock::RouterConfigurationMock;
use sql_frontend::frontend::sql::transform_into_plan;
use sql_frontend::ir::metadata::Metadata;
use sql_ir::ir::node::expression::MutExpression;
use sql_ir::ir::node::relational::Relational;
use sql_ir::ir::node::{NodeId, Projection, Selection, SubQueryReference};
use sql_ir::ir::transformation::equality_facts::Slot;
use sql_ir::ir::transformation::Stage;
use sql_ir::ir::types::UnrestrictedType;
use sql_ir::ir::Plan;

const SIZES: [usize; 4] = [16, 64, 256, 1024];

fn independent_sql(n: usize) -> String {
    let conjuncts: Vec<String> = (0..n)
        .map(|i| format!(r#"("t"."a" = {i} OR "t"."b" = {i})"#))
        .collect();
    format!(
        r#"SELECT * FROM "t" JOIN "t2" ON true WHERE {}"#,
        conjuncts.join(" AND ")
    )
}

/// `n` conjuncts, each with its own scalar subquery; [`share_one_body`]
/// makes them all use the first one.
fn component_sql(n: usize) -> String {
    let columns = [
        r#""t"."a""#,
        r#""t2"."e""#,
        r#""t"."b""#,
        r#""t2"."f""#,
        r#""t"."c""#,
        r#""t2"."g""#,
        r#""t"."d""#,
        r#""t2"."h""#,
    ];
    let conjuncts: Vec<String> = (0..n)
        .map(|i| {
            format!(
                r#"(SELECT max("e") FROM "t2" AS "x{i}") >= {}"#,
                columns[i % columns.len()]
            )
        })
        .collect();
    format!(
        r#"SELECT * FROM "t" JOIN "t2" ON true WHERE {}"#,
        conjuncts.join(" AND ")
    )
}

/// The `WHERE` of the outermost query.
fn top_where(plan: &Plan) -> NodeId {
    let top = plan.get_top().unwrap();
    let Relational::Projection(Projection {
        child: Some(child), ..
    }) = plan.get_relation_node(top).unwrap()
    else {
        panic!("the top is a projection");
    };
    *child
}

/// Point every subquery reference of the top `WHERE` at its first subquery
/// and list only that one, as BETWEEN shares a body between two conjuncts.
fn share_one_body(plan: &mut Plan) {
    let owner = top_where(plan);
    let Relational::Selection(Selection {
        filter, subqueries, ..
    }) = plan.get_relation_node(owner).unwrap()
    else {
        panic!("the top WHERE is a Selection");
    };
    let (filter, body) = (*filter, subqueries[0]);
    for ref_id in plan.get_sq_refs_from_subtree(filter).unwrap() {
        if let MutExpression::SubQueryReference(SubQueryReference { rel_id, .. }) =
            plan.get_mut_expression_node(ref_id).unwrap()
        {
            *rel_id = body;
        }
    }
    plan.set_relational_subqueries(owner, vec![body]);
}

/// The plan right before the push-down, ready to time it.
fn prepared(sql: &str, edit: impl FnOnce(&mut Plan)) -> (Plan, NodeId) {
    prepared_with(sql, &RouterConfigurationMock::new(), edit)
}

fn prepared_with(
    sql: &str,
    metadata: &impl Metadata,
    edit: impl FnOnce(&mut Plan),
) -> (Plan, NodeId) {
    let mut plan = transform_into_plan(sql, &[], metadata).unwrap();
    edit(&mut plan);
    let top = plan.get_top().unwrap();
    let plan = plan
        .optimize_before(top, Stage::PushDownRestrictions)
        .unwrap();
    (plan, top)
}

fn bench_shape(
    c: &mut Criterion,
    group_name: &'static str,
    sql: fn(usize) -> String,
    edit: fn(&mut Plan),
) {
    let mut group = c.benchmark_group(group_name);
    for n in SIZES {
        let (plan, top) = prepared(&sql(n), edit);
        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, plan| {
            b.iter_batched(
                || plan.clone(),
                |plan| black_box(plan.push_down_restrictions(top).unwrap()),
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

/// A sharded `w` of `width` integer columns `c0..` and a one-column `u`.
fn wide_catalog(width: usize) -> MockCatalog {
    let names: Vec<String> = (0..width).map(|i| format!("c{i}")).collect();
    let columns: Vec<(&str, UnrestrictedType, bool)> = names
        .iter()
        .map(|name| (name.as_str(), UnrestrictedType::Integer, true))
        .collect();
    let mut catalog = MockCatalog::new();
    catalog.add_sharded("w", &columns, &["c0"], &["c0"]);
    catalog.add_sharded(
        "u",
        &[("x", UnrestrictedType::Integer, true)],
        &["x"],
        &["x"],
    );
    catalog
}

/// `n` conjuncts, the `i`-th comparing its own scalar subquery with `w.c{i}`
/// (`w.c{i % width}` for a narrower `w`); `IN` compares a body of `u` instead.
fn wide_sql(n: usize, width: usize, in_subquery: bool) -> String {
    let conjuncts: Vec<String> = (0..n)
        .map(|i| {
            let column = format!(r#""w"."c{}""#, i % width);
            if in_subquery {
                format!(r#"{column} IN (SELECT "x" FROM "u" AS "x{i}")"#)
            } else {
                format!(r#"(SELECT max("x") FROM "u" AS "x{i}") >= {column}"#)
            }
        })
        .collect();
    format!(
        r#"SELECT * FROM "w" JOIN "u" ON true WHERE {}"#,
        conjuncts.join(" AND ")
    )
}

/// Check the prepared top `WHERE` holds `conjuncts` conjuncts over `slots`
/// distinct columns and `bodies` subqueries, and that one run moves them all
/// to one new owner listing every body.
fn check_shape(plan: &Plan, top: NodeId, conjuncts: usize, slots: usize, bodies: usize) {
    let owner = top_where(plan);
    let restriction = plan.restrictions.as_ref().unwrap().for_rel(owner).unwrap();
    assert_eq!(restriction.clauses().len(), conjuncts);
    let mut distinct: Vec<Slot> = restriction
        .clauses()
        .iter()
        .flat_map(|info| info.slots().iter().copied())
        .collect();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(distinct.len(), slots);
    let listed = plan.get_relation_node(owner).unwrap().subqueries().len();
    assert_eq!(listed, bodies);

    let moved = plan.clone().push_down_restrictions(top).unwrap();
    assert!(moved
        .get_relation_node(owner)
        .unwrap()
        .subqueries()
        .is_empty());
    let left = moved
        .restrictions
        .as_ref()
        .unwrap()
        .for_rel(owner)
        .unwrap()
        .clauses()
        .len();
    assert_eq!(left, 1, "only the `true` is left");

    // One node now lists the subqueries: the new owner, with every body
    // listed once and every conjunct in its restriction.
    let mut owners = Vec::new();
    let mut seen = Vec::new();
    let mut stack = vec![moved.get_top().unwrap()];
    while let Some(id) = stack.pop() {
        if seen.contains(&id) {
            continue;
        }
        seen.push(id);
        stack.extend(moved.nodes.rel_iter(id));
        if !moved.get_relation_node(id).unwrap().subqueries().is_empty() {
            owners.push(id);
        }
    }
    let [new_owner] = owners[..] else {
        panic!("the bodies have {} owners", owners.len());
    };
    let mut listed: Vec<NodeId> = moved
        .get_relation_node(new_owner)
        .unwrap()
        .subqueries()
        .iter()
        .copied()
        .collect();
    assert_eq!(listed.len(), bodies);
    listed.sort_unstable_by_key(|id| (id.arena_type, id.offset));
    listed.dedup();
    assert_eq!(listed.len(), bodies, "a body is listed twice");
    let gained = moved
        .restrictions
        .as_ref()
        .unwrap()
        .for_rel(new_owner)
        .unwrap()
        .clauses()
        .len();
    assert_eq!(gained, conjuncts);
}

fn bench_wide(c: &mut Criterion, group_name: &'static str, many_bodies: bool) {
    let mut group = c.benchmark_group(group_name);
    for n in SIZES {
        // A body per conjunct over a fixed-width table, or one shared body
        // and a column per conjunct.
        let width = if many_bodies { 8 } else { n };
        let catalog = wide_catalog(width);
        let edit: fn(&mut Plan) = if many_bodies { |_| {} } else { share_one_body };
        let (plan, top) = prepared_with(&wide_sql(n, width, many_bodies), &catalog, edit);
        let bodies = if many_bodies { n } else { 1 };
        check_shape(&plan, top, n, width.min(n), bodies);
        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, plan| {
            b.iter_batched(
                || plan.clone(),
                |plan| black_box(plan.push_down_restrictions(top).unwrap()),
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

fn bench_push_down(c: &mut Criterion) {
    bench_shape(c, "push_down/independent", independent_sql, |_| {});
    bench_shape(c, "push_down/component", component_sql, share_one_body);
    bench_wide(c, "push_down/wide_component", false);
    bench_wide(c, "push_down/many_bodies", true);
}

criterion_group!(benches, bench_push_down);
criterion_main!(benches);
