//! Benchmark for the optimizer stage.
//!
//! Measures only [`Plan::optimize`](`sql_ir::ir::Plan::optimize`)
//! over the generated, pathologically large queries and the real-world DQL corpus.
//!
//! # What is timed
//! The input is the plan [`transform_into_plan`] returns -
//! arenas carrying the slack capacity the build left in them.
//! A clone is *not* that input: `Vec::clone` sizes every arena to its length,
//! so the first push into each one reallocates and copies it.
//!
//! # Running
//!
//! ```sh
//! taskset -c 2 cargo bench -p sql-planner --bench optimize                       # corpus only
//! SQL_BENCH_SYNTHETIC=1 taskset -c 2 cargo bench -p sql-planner --bench optimize # + synthetic
//! taskset -c 2 cargo bench -p sql-planner --bench optimize -- q1                 # ids containing q1
//! taskset -c 2 cargo bench -p sql-planner --bench optimize -- --exact bench_optimize_corpus/case/q1
//! ```
//!
//! Pin the run (`taskset`, same core for every run you compare). The filter is a regex
//! over the benchmark id, so `q1` also matches `q10`..`q19`, `--exact` takes the whole id.
//!
//! Before trusting any before/after delta, run the same binary twice against itself
//! (`--save-baseline aa`, then `--baseline aa`) and take that spread as the floor.

use std::panic::{catch_unwind, AssertUnwindSafe};

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};

use sql_ast_new_corpus::corpus_catalog;
use sql_executor::executor::engine::mock::RouterConfigurationMock;
use sql_frontend::frontend::sql::transform_into_plan;
use sql_frontend::ir::metadata::Metadata;

mod common;

use common::bench_cases::{corpus, synthetic, BenchmarkCase};
use common::bench_env::synthetic_enabled;

fn run(
    c: &mut Criterion,
    cases: Vec<BenchmarkCase>,
    metadata: &impl Metadata,
    group_name: &'static str,
) {
    let mut group = c.benchmark_group(group_name);

    for case in &cases {
        let name = case.name;
        let ir = catch_unwind(AssertUnwindSafe(|| {
            transform_into_plan(&case.sql, &[], metadata)
        }));

        if !matches!(ir, Ok(Ok(_))) {
            eprintln!("skipping `{name}`: does not bind against the catalog {ir:?}");
            continue;
        }

        group.bench_with_input(BenchmarkId::from_parameter(name), &case.sql, |b, sql| {
            b.iter_batched(
                || {
                    transform_into_plan(sql, &[], metadata)
                        .unwrap_or_else(|err| panic!("case `{name}` should bind: {err}"))
                },
                |ir| black_box(ir.optimize().unwrap()),
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn optimize_synthetic(c: &mut Criterion) {
    if !synthetic_enabled() {
        return;
    }
    let cases = synthetic();
    run(
        c,
        cases,
        &RouterConfigurationMock::new(),
        "optimize_synthetic",
    );
}

fn optimize_corpus(c: &mut Criterion) {
    let cases = corpus();
    run(c, cases, &corpus_catalog(), "optimize_corpus");
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = optimize_synthetic, optimize_corpus,
}
criterion_main!(benches);
