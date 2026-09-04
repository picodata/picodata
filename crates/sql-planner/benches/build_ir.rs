//! IR-building benchmark.
//!
//! Measures [`build_ir`] alone — the analyzed AST in, a bound IR
//! [`Plan`](sql_frontend::ir::Plan) out - over generated pathologically large queries
//! and the real-world DQL corpus.
//!
//! This needs a catalog to bind against, and the two case sets do not share one.
//! The synthetic queries are generated against the default
//! [router mock](`sql_executor::executor::engine::mock::RouterConfigurationMock`).
//! The corpus binds against its own [`mocked catalog`](`sql_ast_new_corpus::corpus_mock::corpus_catalog`).
//!
//! # What is timed
//! One iteration is one `build_ir` call. The tree is consumed and freed inside it.
//! The plan it returns is dropped by criterion outside it.
//!
//! Every iteration gets a tree parsed just before it ([`BatchSize::PerIteration`]) —
//! the shape production has, and no batch of trees cooling in cache before their turn.
//!
//! # Running
//! ```text
//! taskset -c 2 cargo bench -p sql-planner --bench build_ir                       # corpus only
//! SQL_BENCH_SYNTHETIC=1 taskset -c 2 cargo bench -p sql-planner --bench build_ir # + synthetic
//! taskset -c 2 cargo bench -p sql-planner --bench build_ir -- q1                 # ids containing q1
//! taskset -c 2 cargo bench -p sql-planner --bench build_ir -- --exact bench_build_ir_corpus/case/q1
//! ```
//!
//! Pin the run (`taskset`, same core for every run you compare). The filter is a regex
//! over the benchmark id, so `q1` also matches `q10`..`q19`, `--exact` takes the whole id.
//!
//! Before trusting any before/after delta, run the same binary twice against itself
//! (`--save-baseline aa`, then `--baseline aa`) and take that spread as the floor.

use std::sync::Once;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};

use sql_ast_new_corpus::corpus_catalog;
use sql_executor::executor::engine::mock::RouterConfigurationMock;
use sql_frontend::frontend::sql::ast::AbstractSyntaxTree;
use sql_frontend::frontend::sql::{build_ir, Ast};
use sql_frontend::ir::metadata::Metadata;

mod common;

use common::bench_cases::{corpus, synthetic, BenchmarkCase};
use common::bench_env::synthetic_enabled;

/// Parse `sql` and run the analysis stage, leaving a tree ready for `build_ir`.
fn analyzed<'q>(sql: &'q str, metadata: &'q impl Metadata) -> AbstractSyntaxTree<'q> {
    let ast = AbstractSyntaxTree::new(sql).expect("case should parse");
    ast.analyze(metadata, &[]).expect("case should analyze")
}

fn build_ir_cases(
    c: &mut Criterion,
    cases: Vec<BenchmarkCase>,
    metadata: &impl Metadata,
    group_name: &'static str,
) {
    let mut group = c.benchmark_group(group_name);

    for case in &cases {
        // Build once before timing. A case that stops planning is a broken fixture
        // rather than a result, and failing here names it.
        let checked = Once::new();
        group.bench_with_input(BenchmarkId::new("case", case.name), case, |b, case| {
            checked.call_once(|| {
                if let Err(err) = build_ir(analyzed(&case.sql, metadata), &[], metadata) {
                    panic!(
                        "case `{}` does not build an IR against the catalog: {err}",
                        case.name
                    );
                }
            });

            // `build_ir` consumes the tree, so every iteration needs its own.
            b.iter_batched(
                || analyzed(&case.sql, metadata),
                |ast| black_box(build_ir(ast, &[], metadata).unwrap()),
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

fn bench_build_ir_synthetic(c: &mut Criterion) {
    if !synthetic_enabled() {
        return;
    }
    let cases = synthetic();
    build_ir_cases(
        c,
        cases,
        &RouterConfigurationMock::new(),
        "bench_build_ir_synthetic",
    );
}

fn bench_build_ir_corpus(c: &mut Criterion) {
    let cases = corpus();
    build_ir_cases(c, cases, &corpus_catalog(), "bench_build_ir_corpus");
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_build_ir_synthetic, bench_build_ir_corpus,
}
criterion_main!(benches);
