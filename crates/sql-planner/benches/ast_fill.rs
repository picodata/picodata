//! AST-fill benchmark.
//!
//! Measures the cost of building the AST (SQL string → raw tree) over two case sets
//! from `bench_cases` — generated pathologically large queries, and the real-world
//! DQL corpus — for whichever frontend the `__bench_ast_new` feature selects.
//!
//! AST fill is the one stage the old and the new frontend share a boundary at,
//! which is what makes a like-for-like comparison possible: run once per feature
//! setting and diff the reports.
//!
//! # Running
//! ```bash
//! taskset -c 2 cargo bench -p sql-planner --bench ast_fill                       # corpus only
//! SQL_BENCH_SYNTHETIC=1 taskset -c 2 cargo bench -p sql-planner --bench ast_fill # + synthetic cases
//! taskset -c 2 cargo bench -p sql-planner --bench ast_fill -- q1                 # ids containing q1
//! taskset -c 2 cargo bench -p sql-planner --bench ast_fill -- --exact bench_fill_corpus/case/q1
//! ```
//!
//! Pin the run (`taskset`, same core for every run you compare). The filter is a regex
//! over the benchmark id, so `q1` also matches `q10`..`q19`, `--exact` takes the whole id.
//!
//! Before trusting any before/after delta, run the same binary twice against itself
//! (`--save-baseline aa`, then `--baseline aa`) and take that spread as the floor.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

#[cfg(not(feature = "__bench_ast_new"))]
use sql_frontend::frontend::sql::ast::AbstractSyntaxTree;

#[cfg(feature = "__bench_ast_new")]
use sql_frontend::frontend::sql::ast_new::RawAst as AbstractSyntaxTree;

use sql_frontend::frontend::sql::Ast;

mod common;
use common::bench_cases::{corpus, synthetic, BenchmarkCase};
use common::bench_env::synthetic_enabled;

fn fill_cases(c: &mut Criterion, cases: Vec<BenchmarkCase>, group_name: &'static str) {
    let mut group = c.benchmark_group(group_name);

    for case in &cases {
        group.bench_with_input(BenchmarkId::new("case", case.name), case, |b, case| {
            b.iter_with_large_drop(|| {
                let ast = AbstractSyntaxTree::new(black_box(case.sql.as_str())).unwrap();
                black_box(ast)
            });
        });
    }

    group.finish();
}

fn bench_fill_synthetic(c: &mut Criterion) {
    if !synthetic_enabled() {
        return;
    }
    let cases = synthetic();
    fill_cases(c, cases, "bench_fill_synthetic");
}

fn bench_fill_corpus(c: &mut Criterion) {
    let cases = corpus();
    fill_cases(c, cases, "bench_fill_corpus");
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_fill_synthetic, bench_fill_corpus,
}
criterion_main!(benches);
