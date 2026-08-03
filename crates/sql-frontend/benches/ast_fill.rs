//! AST-fill benchmark.
//!
//! Measures the cost of building the AST (SQL string → raw tree) over two case sets
//! from `bench_cases` — generated pathologically large queries, and the real-world
//! DQL corpus — for whichever frontend the `__bench_ast_new` feature selects. The
//! same cases feed the dhat allocation profiler in `examples/ast_fill_alloc.rs`.
//!
//! AST fill is the one stage the old and the new frontend share a boundary at
//! (a query string in, a self-contained tree out, no catalog involved), which
//! is what makes a like-for-like comparison possible: run once per feature
//! setting and diff the reports.
//!
//! # Running
//! ```text
//! cargo bench -p sql-frontend                                # old frontend
//! cargo bench -p sql-frontend --features __bench_ast_new     # new frontend
//! cargo bench -p sql-frontend --bench ast_fill -- q11        # one case by name
//! cargo bench -p sql-frontend --bench ast_fill -- --test     # smoke-run, no timing
//! AST_FILL_BENCH=full cargo bench -p sql-frontend            # criterion defaults
//! ```
//! Timings are short by default (see the constants below); `AST_FILL_BENCH=full`
//! restores criterion's defaults for publishable numbers.

use std::sync::Once;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

#[cfg(not(feature = "__bench_ast_new"))]
use sql_frontend::frontend::sql::ast::AbstractSyntaxTree;

#[cfg(feature = "__bench_ast_new")]
use sql_frontend::frontend::sql::ast_new::RawAst as AbstractSyntaxTree;

use sql_frontend::frontend::sql::Ast;

mod bench_cases;
use bench_cases::corpus_benchmark_cases;
use bench_cases::synthetic_benchmark_cases;

use crate::bench_cases::BenchmarkCase;

/// Timing profile for a routine run, sized so `cargo bench -p sql-frontend` finishes
/// in minutes instead of the ~1 hour criterion's defaults cost over these ~100 cases.
///
/// This only trades away precision on the *fast* cases. Criterion never truncates a
/// sample, so the synthetic cases (0.2-1s per parse) are bounded by `SAMPLE_SIZE`
/// iterations and ignore `MEASUREMENT_TIME` — it is the µs-scale corpus parses that
/// would otherwise spend 8s each restating a number they reach in milliseconds.
const SAMPLE_SIZE: usize = 10;
const WARM_UP_TIME: Duration = Duration::from_millis(200);
const MEASUREMENT_TIME: Duration = Duration::from_millis(500);

/// Whether to leave the groups unconfigured and let criterion's defaults apply.
///
/// This has to be an env var rather than the usual `--sample-size` / `--warm-up-time`
/// / `--measurement-time` flags: criterion merges a group's own settings *over* the
/// config it built from argv (`PartialBenchmarkConfig::to_complete`), so the constants
/// above would silently win over anything passed on the command line. Configuring
/// nothing in full mode is what hands those flags back.
fn full_precision() -> bool {
    std::env::var_os("AST_FILL_BENCH").is_some_and(|mode| mode == "full")
}

fn fill_cases(c: &mut Criterion, cases: Vec<BenchmarkCase>, group_name: &'static str) {
    let mut group = c.benchmark_group(group_name);

    if !full_precision() {
        group
            .sample_size(SAMPLE_SIZE)
            .warm_up_time(WARM_UP_TIME)
            .measurement_time(MEASUREMENT_TIME);

        static NOTE: Once = Once::new();
        NOTE.call_once(|| {
            eprintln!(
                "note: quick timings ({SAMPLE_SIZE} samples, {}ms warm-up, {}ms measurement). \
                 Set AST_FILL_BENCH=full for criterion's defaults.",
                WARM_UP_TIME.as_millis(),
                MEASUREMENT_TIME.as_millis(),
            );
        });
    }

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
    let cases = synthetic_benchmark_cases();
    fill_cases(c, cases, "bench_fill_synthetic");
}

fn bench_fill_corpus(c: &mut Criterion) {
    let cases = corpus_benchmark_cases();
    fill_cases(c, cases, "bench_fill_corpus");
}

/// Rendering the HTML report costs ~40% of a quick run's wall time (it plots all ~100
/// benchmarks), which is a poor trade when the point is a terminal-readable answer in
/// a couple of minutes. Full mode keeps the report, and `--noplot` /
/// `--plotting-backend` still work there — `configure_from_args` only ever *disables*
/// plotting, so it will not undo this.
fn bench_criterion() -> Criterion {
    let criterion = Criterion::default();
    if full_precision() {
        criterion
    } else {
        criterion.without_plots()
    }
}

criterion_group! {
    name = benches;
    config = bench_criterion();
    targets = bench_fill_synthetic, bench_fill_corpus,
}
criterion_main!(benches);
