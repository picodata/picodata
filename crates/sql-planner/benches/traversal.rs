//! Plan traversals and node lookups over the real-world DQL corpus.
//!
//! The plans are built and optimized outside the timed region and are only
//! read. One iteration walks every plan of the corpus once:
//! - `rel_post`: post-order over the relational nodes (`rel_iter`);
//! - `subtree_post`: post-order over the whole plan (`subtree_iter`);
//! - `exec_subtree_post`: post-order over the oldest snapshot
//!   (`exec_plan_subtree_iter`), the walks of the executor over one slice.
//!   The iterator stops at a motion that is not local, so the walk starts at
//!   the top and at the subtree root of each such motion;
//! - `expr_post`: `expr_iter` from each expression child that `subtree_iter`
//!   gives for the relational nodes of `subtree_post` (filters, conditions,
//!   projection rows), many short walks as the transformations make;
//! - `*_by_node`: the same walks with the child iterators built from the
//!   node that the traversal already has (`PostOrder::new_by_node`);
//! - `sql_gen_post_by_node`: the walk of SQL generation (`SyntaxPlan::new`):
//!   `flashback_subtree_iter_of` behind the test for a motion leaf. No motion
//!   is a leaf here, so the walk covers the whole plan;
//! - `lookup_*`: the node getters on the ids that `subtree_post` visits; a
//!   typed getter gets only the ids of its kind, so no error is built.
//!
//! Run: taskset -c 2 cargo bench -p sql-planner --bench traversal
//! Count events of a fixed amount of work: see `main`.
//! Compare both revisions with --save-baseline/--baseline, or with
//! --baseline-lenient when a revision adds cases. Compare the base with
//! itself first: the time of a case in one run of all the cases has moved by
//! 17% between two runs of the same binary.

use std::panic::{catch_unwind, AssertUnwindSafe};

use criterion::{black_box, criterion_group, Criterion};
use sql_ast_new_corpus::{corpus_catalog, corpus_queries};
use sql_frontend::frontend::sql::transform_into_plan;
use sql_ir::ir::node::expression::Expression;
use sql_ir::ir::node::relational::Relational;
use sql_ir::ir::node::{Motion, Node, NodeId};
use sql_ir::ir::tree::traversal::PostOrder;
use sql_ir::ir::tree::Snapshot;
use sql_ir::ir::Plan;

struct Prepared {
    plan: Plan,
    top: NodeId,
    /// The ids of the whole plan in post-order.
    ids: Vec<NodeId>,
    rel_ids: Vec<NodeId>,
    expr_ids: Vec<NodeId>,
    /// The top and the subtree root of each motion that is not local.
    exec_roots: Vec<NodeId>,
    /// The expression children that `subtree_iter` gives for `rel_ids`.
    expr_roots: Vec<NodeId>,
}

fn prepare() -> Vec<Prepared> {
    let catalog = corpus_catalog();
    corpus_queries()
        .into_iter()
        .filter_map(|query| {
            let (name, sql) = query.into_parts();
            let plan = catch_unwind(AssertUnwindSafe(|| {
                let mut plan = transform_into_plan(&sql, &[], &catalog).ok()?;
                plan.optimize().ok()?;
                Some(plan)
            }));
            let Ok(Some(plan)) = plan else {
                eprintln!("skipping `{name}`: does not optimize");
                return None;
            };
            let top = plan.get_top().ok()?;
            let mut ids = Vec::new();
            let mut rel_ids = Vec::new();
            let mut expr_ids = Vec::new();
            for (id, node) in PostOrder::new(&plan.nodes, |id| plan.subtree_iter(id), top) {
                ids.push(id);
                match node {
                    Node::Relational(_) => rel_ids.push(id),
                    Node::Expression(_) => expr_ids.push(id),
                    _ => {}
                }
            }
            // The iterator goes through a local motion, so only the other
            // motions start walks of their own.
            let mut exec_roots = vec![top];
            for &motion in plan.slices().slices().iter().flat_map(|s| s.positions()) {
                let Relational::Motion(Motion { policy, .. }) =
                    plan.get_relation_node(motion).ok()?
                else {
                    unreachable!("slices contain motions");
                };
                if !policy.is_local() {
                    exec_roots.push(plan.get_motion_subtree_root(motion).ok()?);
                }
            }
            let expr_roots = rel_ids
                .iter()
                .flat_map(|&rel| plan.subtree_iter(rel))
                .filter(|&child| matches!(plan.get_node(child), Ok(Node::Expression(_))))
                .collect();
            Some(Prepared {
                plan,
                top,
                ids,
                rel_ids,
                expr_ids,
                exec_roots,
                expr_roots,
            })
        })
        .collect()
}

fn rel_post(cases: &[Prepared]) -> usize {
    cases
        .iter()
        .map(|p| PostOrder::new(&p.plan.nodes, |id| p.plan.nodes.rel_iter(id), p.top).count())
        .sum()
}

fn subtree_post(cases: &[Prepared]) -> usize {
    cases
        .iter()
        .map(|p| PostOrder::new(&p.plan.nodes, |id| p.plan.subtree_iter(id), p.top).count())
        .sum()
}

fn exec_subtree_post(cases: &[Prepared]) -> usize {
    let mut visited = 0;
    for p in cases {
        for &root in &p.exec_roots {
            visited += PostOrder::new(
                &p.plan.nodes,
                |id| p.plan.exec_plan_subtree_iter(id, Snapshot::Oldest),
                root,
            )
            .count();
        }
    }
    visited
}

fn expr_post(cases: &[Prepared]) -> usize {
    let mut visited = 0;
    for p in cases {
        for &root in &p.expr_roots {
            visited +=
                PostOrder::new(&p.plan.nodes, |id| p.plan.nodes.expr_iter(id, false), root).count();
        }
    }
    visited
}

fn subtree_post_by_node(cases: &[Prepared]) -> usize {
    cases
        .iter()
        .map(|p| {
            PostOrder::new_by_node(&p.plan.nodes, |_, node| p.plan.subtree_iter_of(node), p.top)
                .count()
        })
        .sum()
}

fn exec_subtree_post_by_node(cases: &[Prepared]) -> usize {
    let mut visited = 0;
    for p in cases {
        for &root in &p.exec_roots {
            visited += PostOrder::new_by_node(
                &p.plan.nodes,
                |_, node| {
                    p.plan
                        .exec_plan_subtree_iter_of(node, Snapshot::Oldest, false)
                },
                root,
            )
            .count();
        }
    }
    visited
}

/// The child iterator of `SyntaxPlan::new` in sql-executor's
/// `backend/sql/tree.rs`: its shape, an `Option` flattened into the
/// iterator, is part of the cost. `cut` keeps the leaf test without cutting;
/// `black_box` keeps LLVM from folding the test away.
fn sql_gen_post_by_node(cases: &[Prepared]) -> usize {
    let cut = black_box(false);
    cases
        .iter()
        .map(|p| {
            PostOrder::new_by_node(
                &p.plan.nodes,
                |_, node| {
                    let leaf = matches!(&node, Node::Relational(Relational::Motion(_))) && cut;
                    (!leaf)
                        .then(|| p.plan.flashback_subtree_iter_of(node))
                        .into_iter()
                        .flatten()
                },
                p.top,
            )
            .count()
        })
        .sum()
}

// Each getter result is matched at once, as the callers do.

fn lookup_node(cases: &[Prepared]) -> usize {
    let mut found = 0usize;
    for p in cases {
        for &id in &p.ids {
            found += matches!(p.plan.get_node(black_box(id)), Ok(Node::Relational(_))) as usize;
        }
    }
    found
}

fn lookup_relation(cases: &[Prepared]) -> usize {
    let mut found = 0usize;
    for p in cases {
        for &id in &p.rel_ids {
            found += matches!(
                p.plan.get_relation_node(black_box(id)),
                Ok(Relational::Projection(_))
            ) as usize;
        }
    }
    found
}

fn lookup_expression(cases: &[Prepared]) -> usize {
    let mut found = 0usize;
    for p in cases {
        for &id in &p.expr_ids {
            found += matches!(
                p.plan.get_expression_node(black_box(id)),
                Ok(Expression::Reference(_))
            ) as usize;
        }
    }
    found
}

type Bench = (&'static str, fn(&[Prepared]) -> usize);

const BENCHES: [Bench; 10] = [
    ("rel_post", rel_post),
    ("subtree_post", subtree_post),
    ("exec_subtree_post", exec_subtree_post),
    ("expr_post", expr_post),
    ("subtree_post_by_node", subtree_post_by_node),
    ("exec_subtree_post_by_node", exec_subtree_post_by_node),
    ("sql_gen_post_by_node", sql_gen_post_by_node),
    ("lookup_node", lookup_node),
    ("lookup_relation", lookup_relation),
    ("lookup_expression", lookup_expression),
];

fn traversals(c: &mut Criterion) {
    let cases = prepare();
    let mut group = c.benchmark_group("traversal");
    for (name, bench) in BENCHES {
        group.bench_function(name, |b| b.iter(|| bench(&cases)));
    }
    group.finish();
}

criterion_group!(benches, traversals);

/// `TRAVERSAL_COUNT=<n> traversal <name>` runs one benchmark `n` times
/// without criterion, so that `perf stat` counts the events of a fixed
/// amount of work. A run with `n = 0` gives the cost of the preparation and
/// of the one call that prints the count of a walk.
fn main() {
    let Ok(count) = std::env::var("TRAVERSAL_COUNT") else {
        benches();
        Criterion::default().configure_from_args().final_summary();
        return;
    };
    let count: usize = count.parse().expect("TRAVERSAL_COUNT is a number");
    let name = std::env::args().nth(1).expect("a benchmark name");
    let (_, bench) = BENCHES
        .into_iter()
        .find(|(n, _)| *n == name)
        .unwrap_or_else(|| panic!("unknown benchmark `{name}`"));
    let cases = prepare();
    eprintln!("{name}: {} per walk", bench(&cases));
    for _ in 0..count {
        black_box(bench(black_box(&cases)));
    }
}
