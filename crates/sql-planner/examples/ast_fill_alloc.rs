//! Deterministic heap-allocation profile of AST fill, over the exact synthetic +
//! corpus cases from `benches/`.
//!
//! Profile [`AbstractSyntaxTree::new`] (use `__bench_ast_new` for new AST):
//!   cargo run --release --example ast_fill_alloc                       # corpus only
//!   cargo run --release --example ast_fill_alloc -- -d                 # + one line per corpus query
//!   SQL_BENCH_SYNTHETIC=1 cargo run --release --example ast_fill_alloc # corpus + synthetic
//!
//! Synthetic (11 generated cases) always prints per case.
//! The corpus (91 real statements) prints only its aggregate total
//! unless `--detailed` (`-d`) is given.
//!
//! Call-site mode — writes `dhat-ast_fill_alloc-<case>.json` (in the current
//! directory) for the DHAT viewer
//! Use (<https://nnethercote.github.io/dh_view/dh_view.html>) to see *where*
//! frontend allocates for one case:
//!   cargo run --release --example ast_fill_alloc -- --dump <case_name>
//!
//! Use `__bench_ast_new` feature for measuring new frontend.

#[cfg(not(feature = "__bench_ast_new"))]
use sql_frontend::frontend::sql::ast::AbstractSyntaxTree;

#[cfg(feature = "__bench_ast_new")]
use sql_frontend::frontend::sql::ast_new::RawAst as AbstractSyntaxTree;

use sql_frontend::frontend::sql::Ast;

mod common;

use common::alloc_report::run;
use common::alloc_report::Profile;
use common::heap_profile::measure;
use common::heap_profile::CountingAlloc;

#[global_allocator]
static ALLOC: CountingAlloc<dhat::Alloc> = CountingAlloc(dhat::Alloc);

fn main() {
    let profile = Profile {
        example: "ast_fill_alloc",
        about: "deterministic heap-allocation profile of AST fill",
        unit: "AbstractSyntaxTree::new",
        retained: "the filled raw AST",
    };

    run(&profile, |_suite, case, recorder| {
        Some(measure(
            recorder,
            || (),
            |()| AbstractSyntaxTree::new(&case.sql).expect("corpus/synthetic SQL should parse"),
        ))
    });
}
