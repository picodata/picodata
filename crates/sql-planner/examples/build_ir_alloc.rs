//! Deterministic heap-allocation profile of IR building, over the exact synthetic +
//! corpus cases from `benches/`.
//!
//! Profile translation of AST into IR ([`build_ir`]):
//!   cargo run --release --example build_ir_alloc                       # corpus only
//!   cargo run --release --example build_ir_alloc -- -d                 # + one line per corpus query
//!   SQL_BENCH_SYNTHETIC=1 cargo run --release --example build_ir_alloc # corpus + synthetic
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

use sql_ast_new_corpus::corpus_catalog;
use sql_executor::executor::engine::mock::RouterConfigurationMock;
use sql_frontend::frontend::sql::ast::AbstractSyntaxTree;
use sql_frontend::frontend::sql::{build_ir, Ast};
use sql_frontend::ir::metadata::Metadata;

mod common;

use common::alloc_report::{run, Profile, Suite};
use common::heap_profile::{measure, CountingAlloc, Recorder, Stats};

#[global_allocator]
static ALLOC: CountingAlloc<dhat::Alloc> = CountingAlloc(dhat::Alloc);

/// Parse and analyze `sql` as the window's setup, then profile the IR build.
fn profile_build<'q>(sql: &'q str, metadata: &'q impl Metadata, recorder: Recorder) -> Stats {
    measure(
        recorder,
        || {
            let ast = AbstractSyntaxTree::new(sql).expect("corpus/synthetic SQL should parse");
            ast.analyze(metadata, &[])
                .expect("corpus/synthetic SQL should analyze")
        },
        |ast| build_ir(ast, &[], metadata).expect("corpus/synthetic SQL should build an IR"),
    )
}

fn main() {
    let profile = Profile {
        example: "build_ir_alloc",
        about: "deterministic heap-allocation profile of IR building",
        unit: "build_ir",
        retained: "the built IR plan",
    };

    let vehicle = RouterConfigurationMock::new();
    let corpus = corpus_catalog();

    run(&profile, |suite, case, recorder| match suite {
        Suite::Synthetic => Some(profile_build(&case.sql, &vehicle, recorder)),
        Suite::Corpus => Some(profile_build(&case.sql, &corpus, recorder)),
    });
}
