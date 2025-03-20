use prometheus::{CounterVec, Histogram, HistogramOpts, IntCounter, Opts, Registry, TextEncoder};
use std::sync::OnceLock;

static GOVERNOR_CHANGE_COUNTER: OnceLock<IntCounter> = OnceLock::new();
static SQL_QUERY_TOTAL: OnceLock<IntCounter> = OnceLock::new();
static SQL_QUERY_ERRORS_TOTAL: OnceLock<IntCounter> = OnceLock::new();
static SQL_QUERY_DURATION_SECONDS: OnceLock<Histogram> = OnceLock::new();
static RPC_REQUEST_TOTAL: OnceLock<CounterVec> = OnceLock::new();
static RPC_REQUEST_ERRORS_TOTAL: OnceLock<CounterVec> = OnceLock::new();
static RPC_REQUEST_DURATION_SECONDS: OnceLock<Histogram> = OnceLock::new();

pub fn governor_change_counter() -> &'static IntCounter {
    GOVERNOR_CHANGE_COUNTER.get_or_init(|| {
        IntCounter::with_opts(Opts::new(
            "governor_changes_total",
            "Total number of times the governor status has changed",
        ))
        .expect("Failed to create governor_changes_total counter")
    })
}

pub fn sql_query_total() -> &'static IntCounter {
    SQL_QUERY_TOTAL.get_or_init(|| {
        IntCounter::with_opts(Opts::new(
            "sql_query_total",
            "Total number of SQL queries executed",
        ))
        .expect("Failed to create sql_query_total counter")
    })
}

pub fn sql_query_errors_total() -> &'static IntCounter {
    SQL_QUERY_ERRORS_TOTAL.get_or_init(|| {
        IntCounter::with_opts(Opts::new(
            "sql_query_errors_total",
            "Total number of SQL queries that resulted in errors",
        ))
        .expect("Failed to create sql_query_errors_total counter")
    })
}

pub fn sql_query_duration_seconds() -> &'static Histogram {
    SQL_QUERY_DURATION_SECONDS.get_or_init(|| {
        Histogram::with_opts(HistogramOpts::new(
            "sql_query_duration_seconds",
            "Histogram of SQL query execution durations (in seconds)",
        ))
        .expect("Failed to create sql_query_duration_seconds histogram")
    })
}

pub fn rpc_request_total() -> &'static CounterVec {
    RPC_REQUEST_TOTAL.get_or_init(|| {
        CounterVec::new(
            Opts::new("rpc_request_total", "Total number of RPC requests executed"),
            &["service"],
        )
        .expect("Failed to create rpc_request_total")
    })
}

pub fn rpc_request_errors_total() -> &'static CounterVec {
    RPC_REQUEST_ERRORS_TOTAL.get_or_init(|| {
        CounterVec::new(
            Opts::new(
                "rpc_request_errors_total",
                "Total number of RPC requests that resulted in errors",
            ),
            &["service"],
        )
        .expect("Failed to create rpc_request_errors_total")
    })
}

pub fn rpc_request_duration_seconds() -> &'static Histogram {
    RPC_REQUEST_DURATION_SECONDS.get_or_init(|| {
        Histogram::with_opts(HistogramOpts::new(
            "rpc_request_duration_seconds",
            "Histogram of RPC request execution durations (in seconds)",
        ))
        .expect("Failed to create rpc_request_duration_seconds histogram")
    })
}

pub fn register_metrics(registry: &Registry) {
    registry
        .register(Box::new(governor_change_counter().clone()))
        .expect("Failed to register governor_changes_total counter");
    registry
        .register(Box::new(sql_query_total().clone()))
        .expect("Failed to register sql_query_total counter");
    registry
        .register(Box::new(sql_query_errors_total().clone()))
        .expect("Failed to register sql_query_errors_total counter");
    registry
        .register(Box::new(sql_query_duration_seconds().clone()))
        .expect("Failed to register sql_query_duration_seconds histogram");
    registry
        .register(Box::new(rpc_request_total().clone()))
        .expect("Failed to register rpc_request_total");
    registry
        .register(Box::new(rpc_request_errors_total().clone()))
        .expect("Failed to register rpc_request_errors_total");
    registry
        .register(Box::new(rpc_request_duration_seconds().clone()))
        .expect("Failed to register rpc_request_duration_seconds histogram");
}

pub fn collect_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder.encode_to_string(&metric_families).unwrap()
}
