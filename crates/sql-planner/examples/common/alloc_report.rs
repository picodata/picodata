//! Shared harness for the allocation-profiling examples.
//!
//! # Every case runs 2 times
//! The first run of a case is discarded, the second reported.
//! The first call into the frontend of a process pays its one-time initialization -
//! the Pratt parser tables, the type system, the builtin-function registry.

use crate::common::bench_cases::{corpus, synthetic, BenchmarkCase};
use crate::common::bench_env::synthetic_enabled;

use crate::common::heap_profile::{Recorder, Stats};
use crate::common::human_format::{commas, human_bytes, human_signed};

/// What the including example profiles, for the report header and `--help`.
pub struct Profile {
    /// The example's name, as `cargo run --example` takes it.
    pub example: &'static str,
    /// One line, what the profile measures — the `--help` subtitle.
    pub about: &'static str,
    /// The call each row is one invocation of, e.g. `AbstractSyntaxTree::new`.
    pub unit: &'static str,
    /// What the `retained` column holds afterwards, e.g. `the filled raw AST`.
    pub retained: &'static str,
}

/// Which case set a case being profiled came from.
#[derive(Clone, Copy)]
pub enum Suite {
    Synthetic,
    Corpus,
}

impl Suite {
    fn name(self) -> &'static str {
        match self {
            Suite::Synthetic => "synthetic",
            Suite::Corpus => "corpus",
        }
    }
}

/// Parsed command line.
struct Options {
    run_synthetic: bool,
    /// Print one line per corpus query instead of just the aggregate total.
    corpus_detailed: bool,
    /// `--dump <case>`: call-site JSON dump mode for a single case.
    dump: Option<String>,
}

fn parse_args(profile: &Profile) -> Options {
    let mut corpus_detailed = false;
    let mut dump = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-d" | "--detailed" => corpus_detailed = true,
            "--dump" => {
                dump = Some(args.next().unwrap_or_else(|| {
                    eprintln!("--dump requires a <case_name> argument");
                    std::process::exit(2);
                }));
            }
            "-h" | "--help" => {
                print_usage(profile);
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument {other:?}; try --help");
                std::process::exit(2);
            }
        }
    }

    // No suite named, so run the corpus alone.
    Options {
        run_synthetic: synthetic_enabled(),
        corpus_detailed,
        dump,
    }
}

fn print_usage(profile: &Profile) {
    let Profile { example, about, .. } = profile;
    print!(
        "\
{example} — {about}

USAGE:
  cargo run --release --example {example} -- [OPTIONS]

SUITE (default: corpus only; SQL_BENCH_SYNTHETIC=1 adds the generated cases):

OPTIONS:
  -d, --detailed   print one line per corpus query (default: corpus total only)
  --dump <case>    write dhat-{example}-<case>.json for the DHAT viewer
  -h, --help       show this help

Synthetic is always per-case (only a handful of cases). --detailed affects the corpus.
"
    );
}

/// Run the profile and print the report.
pub fn run(
    profile: &Profile,
    measure_fn: impl Fn(Suite, &BenchmarkCase, Recorder) -> Option<Stats>,
) {
    let opts = parse_args(profile);

    if let Some(case_name) = &opts.dump {
        dump_call_sites(profile, case_name, &measure_fn);
        return;
    }

    println!(
        "Per case, {unit}. \
         `allocs`=allocations, `alloc'd`=bytes allocated, `peak`=max live bytes \
         above the level at the start of the call, `retained`=live bytes of {retained}.\n",
        unit = profile.unit,
        retained = profile.retained,
    );

    if opts.run_synthetic {
        let synthetic = synthetic();
        run_suite(Suite::Synthetic, &synthetic, true, &measure_fn);
    }

    let corpus = corpus();
    run_suite(Suite::Corpus, &corpus, opts.corpus_detailed, &measure_fn);

    println!(
        "Tip: `-d`/`--detailed` shows one line per corpus query; \
         `--dump <case_name>` writes dhat-{}-<case>.json for the DHAT viewer.",
        profile.example
    );
}

/// Profile every case in `cases`. With `detailed`, print one row per case.
/// Otherwise print only the aggregate TOTAL row (cases are still profiled).
fn run_suite(
    suite: Suite,
    cases: &[BenchmarkCase],
    detailed: bool,
    measure_fn: impl Fn(Suite, &BenchmarkCase, Recorder) -> Option<Stats>,
) {
    let mode = if detailed { "per-case" } else { "summary" };
    println!(
        "=== suite: {} ({} cases, {mode}) ===",
        suite.name(),
        cases.len()
    );
    println!(
        "  {:<42} {:>12} {:>12} {:>12} {:>12}",
        "case", "allocs", "alloc'd", "peak", "retained"
    );
    let mut tot_allocs = 0u64;
    let mut tot_bytes = 0u64;
    let mut max_peak = 0u64;
    let mut tot_retained = 0i64;
    let mut profiled = 0usize;
    for case in cases {
        // 2 runs: warm-up and reported.
        let Some(_warm) = measure_fn(suite, case, Recorder::Stats) else {
            continue;
        };
        let Some(reported) = measure_fn(suite, case, Recorder::Stats) else {
            continue;
        };

        profiled += 1;
        tot_allocs += reported.allocs;
        tot_bytes += reported.bytes;
        max_peak = max_peak.max(reported.peak);
        tot_retained += reported.retained;
        if detailed {
            println!(
                "  {:<42} {:>12} {:>12} {:>12} {:>12}",
                truncate(case.name, 42),
                commas(reported.allocs),
                human_bytes(reported.bytes),
                human_bytes(reported.peak),
                human_signed(reported.retained),
            );
        }
    }
    println!(
        "  {:<42} {:>12} {:>12} {:>12} {:>12}",
        format!("TOTAL / peak-max ({profiled} profiled)"),
        commas(tot_allocs),
        human_bytes(tot_bytes),
        human_bytes(max_peak),
        human_signed(tot_retained),
    );
    println!();
}

/// `--dump` mode. Profile one case with a real heap profiler that writes
/// `dhat-<example>-<case>.json` on drop, for inspection in the DHAT viewer.
fn dump_call_sites(
    profile: &Profile,
    case_name: &str,
    measure_fn: impl Fn(Suite, &BenchmarkCase, Recorder) -> Option<Stats>,
) {
    let synthetic = synthetic();
    let corpus = corpus();
    let found = synthetic
        .iter()
        .map(|case| (Suite::Synthetic, case))
        .chain(corpus.iter().map(|case| (Suite::Corpus, case)))
        .find(|(_, case)| case.name == case_name);
    let Some((suite, case)) = found else {
        eprintln!("no case named {case_name:?}. Available cases:");
        for c in synthetic.iter().chain(&corpus) {
            eprintln!("  {}", c.name);
        }
        std::process::exit(1);
    };

    if measure_fn(suite, case, Recorder::Stats).is_none() {
        std::process::exit(1);
    }

    // Named after the profile as well as the case.
    let file = format!("dhat-{}-{case_name}.json", profile.example);
    measure_fn(suite, case, Recorder::Dump(file.clone()));
    println!("wrote {file} — open it in https://nnethercote.github.io/dh_view/dh_view.html");
}

/// Truncate to `width` characters.
fn truncate(s: &str, width: usize) -> String {
    if s.chars().count() <= width {
        s.to_string()
    } else {
        let head: String = s.chars().take(width - 1).collect();
        format!("{head}…")
    }
}
