use prometheus::{
    GaugeVec, Histogram, HistogramOpts, IntCounter, IntCounterVec, Opts, Registry, TextEncoder,
};
use std::sync::OnceLock;

static GOVERNOR_CHANGE_COUNTER: OnceLock<IntCounter> = OnceLock::new();

static SQL_QUERY_TOTAL: OnceLock<IntCounter> = OnceLock::new();
static SQL_QUERY_ERRORS_TOTAL: OnceLock<IntCounter> = OnceLock::new();
static SQL_QUERY_DURATION_SECONDS: OnceLock<Histogram> = OnceLock::new();

static RPC_REQUEST_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static RPC_REQUEST_ERRORS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static RPC_REQUEST_DURATION_SECONDS: OnceLock<Histogram> = OnceLock::new();

static GLOBAL_TABLES_OPS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static GLOBAL_TABLES_OPS_ERRORS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
static GLOBAL_TABLES_WRITE_LATENCY_SECONDS: OnceLock<Histogram> = OnceLock::new();
static GLOBAL_TABLES_RECORDS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();

static INSTANCE_STATE_GAUGE: OnceLock<GaugeVec> = OnceLock::new();

fn governor_change_counter() -> &'static IntCounter {
    GOVERNOR_CHANGE_COUNTER.get_or_init(|| {
        IntCounter::with_opts(Opts::new(
            "governor_changes_total",
            "Total number of times the governor status has changed",
        ))
        .expect("Failed to create governor_changes_total counter")
    })
}

fn sql_query_total() -> &'static IntCounter {
    SQL_QUERY_TOTAL.get_or_init(|| {
        IntCounter::with_opts(Opts::new(
            "sql_query_total",
            "Total number of SQL queries executed",
        ))
        .expect("Failed to create sql_query_total counter")
    })
}

fn sql_query_errors_total() -> &'static IntCounter {
    SQL_QUERY_ERRORS_TOTAL.get_or_init(|| {
        IntCounter::with_opts(Opts::new(
            "sql_query_errors_total",
            "Total number of SQL queries that resulted in errors",
        ))
        .expect("Failed to create sql_query_errors_total counter")
    })
}

fn sql_query_duration_seconds() -> &'static Histogram {
    SQL_QUERY_DURATION_SECONDS.get_or_init(|| {
        Histogram::with_opts(HistogramOpts::new(
            "sql_query_duration_seconds",
            "Histogram of SQL query execution durations (in seconds)",
        ))
        .expect("Failed to create sql_query_duration_seconds histogram")
    })
}

fn rpc_request_total() -> &'static IntCounterVec {
    RPC_REQUEST_TOTAL.get_or_init(|| {
        IntCounterVec::new(
            Opts::new("rpc_request_total", "Total number of RPC requests executed"),
            &["service"],
        )
        .expect("Failed to create rpc_request_total")
    })
}

fn rpc_request_errors_total() -> &'static IntCounterVec {
    RPC_REQUEST_ERRORS_TOTAL.get_or_init(|| {
        IntCounterVec::new(
            Opts::new(
                "rpc_request_errors_total",
                "Total number of RPC requests that resulted in errors",
            ),
            &["service"],
        )
        .expect("Failed to create rpc_request_errors_total")
    })
}

fn rpc_request_duration_seconds() -> &'static Histogram {
    RPC_REQUEST_DURATION_SECONDS.get_or_init(|| {
        Histogram::with_opts(HistogramOpts::new(
            "rpc_request_duration_seconds",
            "Histogram of RPC request execution durations (in seconds)",
        ))
        .expect("Failed to create rpc_request_duration_seconds histogram")
    })
}

fn global_tables_ops_total() -> &'static IntCounterVec {
    GLOBAL_TABLES_OPS_TOTAL.get_or_init(|| {
        IntCounterVec::new(
            Opts::new(
                "global_tables_ops_total",
                "Total number of CAS operations on global tables",
            ),
            &["op_type", "table"],
        )
        .expect("Failed to create global_tables_ops_total")
    })
}

fn global_tables_ops_errors_total() -> &'static IntCounterVec {
    GLOBAL_TABLES_OPS_ERRORS_TOTAL.get_or_init(|| {
        IntCounterVec::new(
            Opts::new(
                "global_tables_ops_errors_total",
                "Total number of CAS operations on global tables that resulted in an error",
            ),
            &["op_type", "table"],
        )
        .expect("Failed to create global_tables_ops_errors_total")
    })
}

fn global_tables_write_latency_seconds() -> &'static Histogram {
    GLOBAL_TABLES_WRITE_LATENCY_SECONDS.get_or_init(|| {
        Histogram::with_opts(HistogramOpts::new(
            "global_tables_write_latency_seconds",
            "Histogram of CAS operation latencies on global tables (in seconds)",
        ))
        .expect("Failed to create global_tables_write_latency_seconds")
    })
}

fn global_tables_records_total() -> &'static IntCounterVec {
    GLOBAL_TABLES_RECORDS_TOTAL.get_or_init(|| {
        IntCounterVec::new(
            Opts::new(
                "global_tables_records_total",
                "Total number of records written via CAS operations on global tables",
            ),
            &["op_type", "table"],
        )
        .expect("Failed to create global_tables_records_total")
    })
}

fn instance_state_gauge() -> &'static GaugeVec {
    INSTANCE_STATE_GAUGE.get_or_init(|| {
        GaugeVec::new(
            Opts::new(
                "instance_state",
                "Gauge indicating the current state of an instance (1 for the current state, 0 otherwise)"
            ),
            &["tier", "instance", "state"]
        ).expect("Failed to create instance_state gauge")
    })
}

pub fn report_governor_change() {
    governor_change_counter().inc();
}

pub fn record_sql_query_total() {
    sql_query_total().inc();
}

pub fn record_sql_query_error() {
    sql_query_errors_total().inc();
}

pub fn observe_sql_query_duration(duration_secs: f64) {
    sql_query_duration_seconds().observe(duration_secs);
}

pub fn record_rpc_request(service: &str) {
    rpc_request_total().with_label_values(&[service]).inc();
}

pub fn record_rpc_request_error(service: &str) {
    rpc_request_errors_total()
        .with_label_values(&[service])
        .inc();
}

pub fn observe_rpc_request_duration(duration_secs: f64) {
    rpc_request_duration_seconds().observe(duration_secs);
}

pub fn record_global_table_ops_total(op_type: &str, table: &str) {
    global_tables_ops_total()
        .with_label_values(&[op_type, table])
        .inc();
}

pub fn record_global_table_errors(op_type: &str, table: &str) {
    global_tables_ops_errors_total()
        .with_label_values(&[op_type, table])
        .inc();
}

pub fn observe_global_table_write_latency(duration_secs: f64) {
    global_tables_write_latency_seconds().observe(duration_secs);
}

pub fn record_global_table_records(op_type: &str, table: &str, amount: usize) {
    global_tables_records_total()
        .with_label_values(&[op_type, table])
        .inc_by(amount as u64);
}

/// Sets the gauge for the instance state to a specific value
/// Online - 0.0
/// Offline - 1.0
/// Expelled - 2.0
///
/// ```rust
/// report_instance_state("default", "default_1_1", "Online", 0.0);
/// report_instance_state("default", "default_1_1", "Offline", 1.0);
/// report_instance_state("default", "default_1_1", "Expelled", 2.0);
/// ```
pub fn report_instance_state(tier: &str, instance_name: &str, state: &str) {
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

    instance_state_gauge()
        .with_label_values(&[tier, instance_name, state])
        .set(value);
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
    registry
        .register(Box::new(global_tables_ops_total().clone()))
        .expect("Failed to register global_tables_ops_total");
    registry
        .register(Box::new(global_tables_ops_errors_total().clone()))
        .expect("Failed to register global_tables_ops_errors_total");
    registry
        .register(Box::new(global_tables_write_latency_seconds().clone()))
        .expect("Failed to register global_tables_write_latency_seconds");
    registry
        .register(Box::new(global_tables_records_total().clone()))
        .expect("Failed to register global_tables_records_total");
    registry
        .register(Box::new(instance_state_gauge().clone()))
        .expect("Failed to register instance_state gauge");
}

pub fn collect_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder.encode_to_string(&metric_families).unwrap()
}
