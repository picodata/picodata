use prometheus::{
    GaugeVec, Histogram, HistogramOpts, IntCounter, IntCounterVec, Opts, Registry, TextEncoder,
};
use std::sync::LazyLock;

static GOVERNOR_CHANGE_COUNTER: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pico_governor_changes_total",
        "Total number of times the governor status has changed",
    ))
    .expect("Failed to create pico_governor_changes_total counter")
});

static SQL_QUERY_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pico_sql_query_total",
        "Total number of SQL queries executed",
    ))
    .expect("Failed to create pico_sql_query_total counter")
});

static SQL_QUERY_ERRORS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pico_sql_query_errors_total",
        "Total number of SQL queries that resulted in errors",
    ))
    .expect("Failed to create pico_sql_query_errors_total counter")
});

static SQL_QUERY_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::with_opts(HistogramOpts::new(
        "pico_sql_query_duration",
        "Histogram of SQL query execution durations (in milliseconds)",
    ))
    .expect("Failed to create pico_sql_query_duration histogram")
});

static RPC_REQUEST_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_rpc_request_total",
            "Total number of RPC requests executed",
        ),
        &["service"],
    )
    .expect("Failed to create pico_rpc_request_total")
});

static RPC_REQUEST_ERRORS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_rpc_request_errors_total",
            "Total number of RPC requests that resulted in errors",
        ),
        &["service"],
    )
    .expect("Failed to create pico_rpc_request_errors_total")
});

static RPC_REQUEST_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::with_opts(HistogramOpts::new(
        "pico_rpc_request_duration",
        "Histogram of RPC request execution durations (in milliseconds)",
    ))
    .expect("Failed to create pico_rpc_request_duration histogram")
});

static GLOBAL_TABLES_RECORDS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_global_tables_records_total",
            "Total number of records written via CAS operations on global tables",
        ),
        &["op_type", "table"],
    )
    .expect("Failed to create pico_global_tables_records_total")
});

static GLOBAL_TABLES_ERRORS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_global_tables_errors_total",
            "Total number of CAS operations on global tables that resulted in an error",
        ),
        &["op_type", "table"],
    )
    .expect("Failed to create pico_global_tables_errors_total")
});

static GLOBAL_TABLES_WRITE_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::with_opts(HistogramOpts::new(
        "pico_global_tables_write_duration",
        "Histogram of CAS operation durations on global tables (in milliseconds)",
    ))
    .expect("Failed to create pico_global_tables_write_duration")
});

static INSTANCE_STATE_GAUGE: LazyLock<GaugeVec> = LazyLock::new(|| {
    GaugeVec::new(
        Opts::new(
            "pico_instance_state",
            "Gauge indicating the current state of an instance (0.0 - Online, 1.0 - Offline, 2.0 - Expelled)"
        ),
        &["tier", "instance", "state"]
    ).expect("Failed to create pico_instance_state gauge")
});

pub fn record_governor_change() {
    GOVERNOR_CHANGE_COUNTER.inc();
}

pub fn record_sql_query_total() {
    SQL_QUERY_TOTAL.inc();
}

pub fn record_sql_query_error() {
    SQL_QUERY_ERRORS_TOTAL.inc();
}

pub fn observe_sql_query_duration(duration_ms: f64) {
    SQL_QUERY_DURATION.observe(duration_ms);
}

pub fn record_rpc_request(service: &str) {
    RPC_REQUEST_TOTAL.with_label_values(&[service]).inc();
}

pub fn record_rpc_request_error(service: &str) {
    RPC_REQUEST_ERRORS_TOTAL.with_label_values(&[service]).inc();
}

pub fn observe_rpc_request_duration(duration_ms: f64) {
    RPC_REQUEST_DURATION.observe(duration_ms);
}

pub fn record_global_table_ops_total(operations: Vec<(&str, String)>) {
    for (op_type, table) in operations.iter() {
        GLOBAL_TABLES_RECORDS_TOTAL
            .with_label_values(&[op_type, table])
            .inc();
    }
}

pub fn record_global_table_errors(operations: Vec<(&str, String)>) {
    for (op_type, table) in operations.iter() {
        GLOBAL_TABLES_ERRORS_TOTAL
            .with_label_values(&[op_type, table])
            .inc();
    }
}

pub fn observe_global_table_write_latency(duration_ms: f64) {
    GLOBAL_TABLES_WRITE_DURATION.observe(duration_ms);
}

/// Sets the gauge for the instance state to a specific value
/// Since prometheus metrics only work with numeric values
/// we transform state into numerical value
/// Online - 0.0
/// Offline - 1.0
/// Expelled - 2.0
pub fn record_instance_state(tier: &str, instance_name: &str, state: &str) {
    let value: f64;
    match state {
        "Online" => {
            value = 0.0;
        }
        "Offline" => {
            value = 1.0;
        }
        "Expelled" => {
            value = 2.0;
        }
        _ => {
            panic!("Invalid state: {}", state);
        }
    }

    INSTANCE_STATE_GAUGE
        .with_label_values(&[tier, instance_name, state])
        .set(value);
}

pub fn register_metrics(registry: &Registry) {
    registry
        .register(Box::new(GOVERNOR_CHANGE_COUNTER.clone()))
        .expect("Failed to register pico_governor_changes_total counter");
    registry
        .register(Box::new(SQL_QUERY_TOTAL.clone()))
        .expect("Failed to register pico_sql_query_total counter");
    registry
        .register(Box::new(SQL_QUERY_ERRORS_TOTAL.clone()))
        .expect("Failed to register pico_sql_query_errors_total counter");
    registry
        .register(Box::new(SQL_QUERY_DURATION.clone()))
        .expect("Failed to register pico_sql_query_duration histogram");
    registry
        .register(Box::new(RPC_REQUEST_TOTAL.clone()))
        .expect("Failed to register pico_rpc_request_total");
    registry
        .register(Box::new(RPC_REQUEST_ERRORS_TOTAL.clone()))
        .expect("Failed to register pico_rpc_request_errors_total");
    registry
        .register(Box::new(RPC_REQUEST_DURATION.clone()))
        .expect("Failed to register pico_rpc_request_duration histogram");
    registry
        .register(Box::new(GLOBAL_TABLES_RECORDS_TOTAL.clone()))
        .expect("Failed to register pico_global_tables_records_total");
    registry
        .register(Box::new(GLOBAL_TABLES_ERRORS_TOTAL.clone()))
        .expect("Failed to register pico_global_tables_errors_total");
    registry
        .register(Box::new(GLOBAL_TABLES_WRITE_DURATION.clone()))
        .expect("Failed to register pico_global_tables_write_duration");
    registry
        .register(Box::new(INSTANCE_STATE_GAUGE.clone()))
        .expect("Failed to register pico_instance_state gauge");
}

pub fn collect_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder.encode_to_string(&metric_families).unwrap()
}
