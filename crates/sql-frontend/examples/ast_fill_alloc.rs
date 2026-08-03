//! Deterministic heap-allocation profile of AST fill, over the exact synthetic +
//! corpus cases from `benches/` — reused via `bench_cases`.
//!
//! The complement to the timing benchmark: allocation counts don't fluctuate
//! run to run the way timings do, so two dhat runs — old frontend vs. new
//! (`__bench_ast_new`) — diff down to the exact number of allocations a
//! change adds or removes, with no statistics in between.
//!
//! Table mode (default) — profiles AST (use __bench_ast_new for new AST):
//!   cargo run --release --example ast_fill_alloc                    # both suites
//!   cargo run --release --example ast_fill_alloc -- synthetic       # synthetic only
//!   cargo run --release --example ast_fill_alloc -- corpus          # corpus only
//!   cargo run --release --example ast_fill_alloc -- corpus -d       # + one line per corpus query
//!
//! Synthetic (11 generated cases) always prints per case; the corpus (91 real
//! statements) prints only its aggregate total unless `--detailed` (`-d`) is given.
//!
//! Call-site mode — writes `dhat-<case>.json` for the DHAT viewer
//! (<https://nnethercote.github.io/dh_view/dh_view.html>) to see *where*
//! frontend allocates for one case:
//!   cargo run --release --example ast_fill_alloc -- --dump <case_name>
//!
//! Use `__bench_ast_new` feature for measuring new frontend.

use std::hint::black_box;

#[cfg(not(feature = "__bench_ast_new"))]
use sql_frontend::frontend::sql::ast::AbstractSyntaxTree;

#[cfg(feature = "__bench_ast_new")]
use sql_frontend::frontend::sql::ast_new::RawAst as AbstractSyntaxTree;

use sql_frontend::frontend::sql::Ast;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[path = "../benches/bench_cases.rs"]
mod bench_cases;
use bench_cases::{corpus_benchmark_cases, synthetic_benchmark_cases, BenchmarkCase};

/// Parsed command line.
struct Options {
    run_synthetic: bool,
    run_corpus: bool,
    /// Print one line per corpus query instead of just the aggregate total.
    corpus_detailed: bool,
    /// `--dump <case>`: call-site JSON dump mode for a single case.
    dump: Option<String>,
}

fn parse_args() -> Options {
    let mut only_synth = false;
    let mut only_corpus = false;
    let mut corpus_detailed = false;
    let mut dump = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "synthetic" => only_synth = true,
            "corpus" => only_corpus = true,
            "-d" | "--detailed" => corpus_detailed = true,
            "--dump" => {
                dump = Some(args.next().unwrap_or_else(|| {
                    eprintln!("--dump requires a <case_name> argument");
                    std::process::exit(2);
                }));
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument {other:?}; try --help");
                std::process::exit(2);
            }
        }
    }

    // No suite named ⇒ both.
    let both = !only_synth && !only_corpus;
    Options {
        run_synthetic: only_synth || both,
        run_corpus: only_corpus || both,
        corpus_detailed,
        dump,
    }
}

fn print_usage() {
    print!(
        "\
ast_fill_alloc — deterministic heap-allocation profile of AST fill

USAGE:
  cargo run --release --example ast_fill_alloc -- [OPTIONS]

SUITE (default: both):
  synthetic        profile only the generated, pathologically large cases
  corpus           profile only the real-world DQL corpus statements

OPTIONS:
  -d, --detailed   print one line per corpus query (default: corpus total only)
  --dump <case>    write dhat-<case>.json for the DHAT viewer
  -h, --help       show this help

Synthetic is always per-case (only a handful of cases); --detailed affects the corpus.
"
    );
}

/// One heap-profiled AST fill. The profiler is live while `HeapStats::get()` runs and
/// the AST is still held, so `total_*` counts every allocation the parse made and
/// `curr_*` is the retained footprint of the resulting tree.
fn profile<T>(build: impl FnOnce() -> T) -> dhat::HeapStats {
    let _profiler = dhat::Profiler::builder().testing().build();
    let ast = build();
    let stats = dhat::HeapStats::get();
    drop(black_box(ast));
    stats
}

fn main() {
    let opts = parse_args();

    if let Some(case_name) = &opts.dump {
        let synthetic = synthetic_benchmark_cases();
        let corpus = corpus_benchmark_cases();
        dump_call_sites(case_name, &synthetic, &corpus);
        return;
    }

    println!(
        "Per case, AbstractSyntaxTree::new. \
         `allocs`=total allocations, `alloc'd`=total bytes, `peak`=high-water live \
         bytes, `retained`=live bytes of the filled raw AST.\n"
    );

    if opts.run_synthetic {
        let synthetic = synthetic_benchmark_cases();
        print_suite("synthetic", &synthetic, true);
    }
    if opts.run_corpus {
        let corpus = corpus_benchmark_cases();
        print_suite("corpus", &corpus, opts.corpus_detailed);
    }

    println!(
        "Tip: `-d`/`--detailed` shows one line per corpus query; \
         `--dump <case_name>` writes dhat-<case>.json for the DHAT viewer."
    );
}

/// Profile every case in `cases`. With `detailed`, print one row per
/// case. Otherwise print only the aggregate TOTAL row (cases are still profiled).
fn print_suite(suite: &str, cases: &[BenchmarkCase], detailed: bool) {
    let mode = if detailed { "per-case" } else { "summary" };
    println!("=== suite: {suite} ({} cases, {mode}) ===", cases.len());
    println!(
        "  {:<42} {:>12} {:>12} {:>12} {:>12}",
        "case", "allocs", "alloc'd", "peak", "retained"
    );
    let mut tot_blocks = 0u64;
    let mut tot_bytes = 0u64;
    let mut max_peak = 0u64;
    let mut tot_retained = 0u64;
    for case in cases {
        let s = profile(|| {
            AbstractSyntaxTree::new(&case.sql).expect("corpus/synthetic SQL should parse")
        });
        tot_blocks += s.total_blocks;
        tot_bytes += s.total_bytes;
        max_peak = max_peak.max(s.max_bytes as u64);
        tot_retained += s.curr_bytes as u64;
        if detailed {
            println!(
                "  {:<42} {:>12} {:>12} {:>12} {:>12}",
                truncate(case.name, 42),
                commas(s.total_blocks),
                human_bytes(s.total_bytes),
                human_bytes(s.max_bytes as u64),
                human_bytes(s.curr_bytes as u64),
            );
        }
    }
    println!(
        "  {:<42} {:>12} {:>12} {:>12} {:>12}",
        "TOTAL / peak-max",
        commas(tot_blocks),
        human_bytes(tot_bytes),
        human_bytes(max_peak),
        human_bytes(tot_retained),
    );
    println!();
}

/// `--dump` mode: profile one case with a real heap profiler that writes
/// `dhat-<case>.json` on drop, for inspection in the DHAT viewer.
fn dump_call_sites(case_name: &str, synthetic: &[BenchmarkCase], corpus: &[BenchmarkCase]) {
    let case = synthetic.iter().chain(corpus).find(|c| c.name == case_name);
    let Some(case) = case else {
        eprintln!("no case named {case_name:?}. Available cases:");
        for c in synthetic.iter().chain(corpus) {
            eprintln!("  {}", c.name);
        }
        std::process::exit(1);
    };

    let file = format!("dhat-{case_name}.json");
    {
        let _profiler = dhat::Profiler::builder().file_name(file.clone()).build();
        let ast = AbstractSyntaxTree::new(&case.sql).expect("case SQL should parse");
        black_box(&ast);
    }
    println!("wrote {file} — open it in https://nnethercote.github.io/dh_view/dh_view.html");
}

/// Truncate to `width` characters. Char-based on both sides: `{:<width}` pads by
/// char count, and a byte slice could split a multi-byte character and panic.
fn truncate(s: &str, width: usize) -> String {
    if s.chars().count() <= width {
        s.to_string()
    } else {
        let head: String = s.chars().take(width - 1).collect();
        format!("{head}…")
    }
}

/// Thousands-separated allocation count, e.g. `1,234,567`.
fn commas(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    let len = digits.len();
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

/// Human-readable byte size, e.g. `12.34 MiB`.
fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if n < 1024 {
        return format!("{n} B");
    }
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1023.995 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.2} {}", UNITS[unit])
}
