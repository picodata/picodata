//! Deterministic heap-allocation profile of the planning (optimizer) stage.
//!
//! Only [`Plan::optimize`] is profiled, over the plan [`transform_into_plan`] returns.
//!
//! Table mode (default):
//!   cargo run --release --example optimize_alloc                       # corpus only
//!   cargo run --release --example optimize_alloc -- -d                 # + one line per corpus query
//!   SQL_BENCH_SYNTHETIC=1 cargo run --release --example optimize_alloc # corpus + synthetic
//!
//! Synthetic (11 generated cases) always prints per case.
//! The corpus (91 real statements) prints only its aggregate total
//! unless `--detailed` (`-d`) is given.
//!
//! Call-site mode — writes `dhat-optimize_alloc-<case>.json` (in the current
//! directory) for the DHAT viewer
//! Use (<https://nnethercote.github.io/dh_view/dh_view.html>) to see *where*
//! the optimizer allocates for one case:
//!   cargo run --release --example optimize_alloc -- --dump <case_name>

use std::panic::{catch_unwind, AssertUnwindSafe};

use sql_ast_new_corpus::corpus_catalog;
use sql_executor::executor::engine::mock::RouterConfigurationMock;
use sql_frontend::frontend::sql::transform_into_plan;
use sql_frontend::ir::metadata::Metadata;
use sql_frontend::ir::Plan;

mod common;

use common::alloc_report::{run, Profile, Suite};
use common::heap_profile::{measure, CountingAlloc, Recorder, Stats};

#[global_allocator]
static ALLOC: CountingAlloc<dhat::Alloc> = CountingAlloc(dhat::Alloc);

/// Parse and bind one case, or `None` if it does not reach the optimizer at all.
fn bind(name: &str, sql: &str, metadata: &impl Metadata) -> Option<Plan> {
    let bound = catch_unwind(AssertUnwindSafe(|| transform_into_plan(sql, &[], metadata)));
    match bound {
        Ok(Ok(plan)) => Some(plan),
        val => {
            eprintln!("skipping `{name}`: does not bind against the catalog {val:?}");
            None
        }
    }
}

/// Bind `sql` outside the window, then profile `optimize` over the bound plan.
fn profile_optimize(
    name: &str,
    sql: &str,
    metadata: &impl Metadata,
    recorder: Recorder,
) -> Option<Stats> {
    let plan = bind(name, sql, metadata)?;
    Some(measure(
        recorder,
        || plan,
        |plan| {
            plan.optimize()
                .unwrap_or_else(|err| panic!("case `{name}` does not optimize: {err}"))
        },
    ))
}

fn main() {
    let profile = Profile {
        example: "optimize_alloc",
        about: "deterministic heap-allocation profile of Plan::optimize",
        unit: "Plan::optimize",
        retained: "the optimized plan, minus the bound plan it consumed",
    };

    let vehicle = RouterConfigurationMock::new();
    let corpus = corpus_catalog();

    run(&profile, |suite, case, recorder| match suite {
        Suite::Synthetic => profile_optimize(case.name, &case.sql, &vehicle, recorder),
        Suite::Corpus => profile_optimize(case.name, &case.sql, &corpus, recorder),
    });
}
