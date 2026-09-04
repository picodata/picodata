//! Environment knobs shared by the benches and the profiling examples.

/// Whether the generated cases run alongside the corpus.
pub fn synthetic_enabled() -> bool {
    std::env::var_os("SQL_BENCH_SYNTHETIC").is_some_and(|value| !value.is_empty() && value != "0")
}
