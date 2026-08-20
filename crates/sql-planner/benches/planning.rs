//! Benchmark for the planning (optimizer) stage.
//!
//! Measures only `Plan::optimize()` — the transformation pipeline in
//! `sql_ir::ir::transformation`. Each query in the real-world DQL corpus
//! (`sql-ast-new-corpus`, `q1`..`q91`) is parsed and bound against `CorpusMock` once,
//! up front and outside the timed region, so the number is the optimizer alone.
//!
//! # Running
//!
//! To A/B a compile-time-gated change, build the bench twice — with and without the
//! feature that gates it. Below, `any_feature_flag_to_compare` is a placeholder;
//! substitute the actual feature name.
//!
//! Measure the baseline (`any_feature_flag_to_compare` OFF):
//!
//! ```sh
//! cargo bench -p sql-planner --features mock --bench planning -- --save-baseline off
//! ```
//!
//! Measure with the feature ON, compared against that baseline:
//!
//! ```sh
//! cargo bench -p sql-planner --features "mock,any_feature_flag_to_compare" --bench planning -- --baseline off
//! ```
//!
//! The feature is compile-time gated, hence the two separate builds; criterion prints a
//! per-query `change: [-x% +x%]` line for the second run.
//!
//! Run one query: append its name, e.g. `-- q11`. Set `PLANNING_BENCH=full` for
//! criterion's full sample count and measurement window instead of the short budget
//! below. A query that fails to bind is reported and skipped, not fatal.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkGroup, BenchmarkId, Criterion,
};
use sql_frontend::frontend::sql::transform_into_plan;

use sql_ast_new_corpus::{corpus_queries, CorpusMock};

pub fn full_synthetic_requested() -> bool {
    std::env::var_os("PLANNING_BENCH").is_some_and(|mode| mode == "full")
}

/// Short timing budget so a full sweep finishes in minutes rather than criterion's
/// defaults; `PLANNING_BENCH=full` hands the group back to those defaults (more
/// samples, full measurement window) for a publishable run.
fn configure_precision(group: &mut BenchmarkGroup<WallTime>) {
    if full_synthetic_requested() {
        return;
    }
    group
        .sample_size(20)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3));
}

fn bench_planning_corpus(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimize_corpus");
    configure_precision(&mut group);

    let metadata = CorpusMock::new();
    for case in corpus_queries() {
        let (name, sql) = case.into_parts();
        let parsed = catch_unwind(AssertUnwindSafe(|| {
            transform_into_plan(&sql, &[], &metadata)
        }));
        let plan = match parsed {
            Ok(Ok(plan)) => plan,
            val => {
                eprintln!(
                    "skipping `{}`: does not bind against CorpusMock {:?}",
                    name, val
                );
                continue;
            }
        };

        // Clone in the (untimed) setup closure so only `optimize()` itself is
        // measured — `optimize(self)` consumes the plan, so each iteration needs a
        // fresh one, and timing the clone would blur the transformation numbers.
        group.bench_with_input(BenchmarkId::from_parameter(name), &plan, |b, plan| {
            b.iter_batched(
                || plan.clone(),
                |plan| black_box(plan.optimize().unwrap()),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// Rendering the HTML report costs a meaningful chunk of a quick run's wall time (it
/// plots every benchmark), a poor trade when the point is a terminal-readable answer in
/// a couple of minutes. Full mode keeps the report; `--noplot` / `--plotting-backend`
/// still work there since this only ever *disables* plotting, never re-enables it.
fn bench_criterion() -> Criterion {
    let criterion = Criterion::default();
    if full_synthetic_requested() {
        criterion
    } else {
        criterion.without_plots()
    }
}

criterion_group! {
    name = benches;
    config = bench_criterion();
    targets = bench_planning_corpus,
}
criterion_main!(benches);
