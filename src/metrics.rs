use crate::config::PicodataConfig;
use crate::instance::StateVariant;
use crate::tlog;
use crate::traft::node;
use crate::traft::op::{Acl, Ddl, Op};
use prometheus::{
    Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts,
    TextEncoder,
};
use smol_str::format_smolstr;
use smol_str::SmolStr;
use smol_str::ToSmolStr;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

extern "C" {
    fn tarantool_uptime() -> f64;
}

const TABLE_METRIC_LABEL_NAMES: [&str; 3] = ["table_name", "table_kind", "engine"];

static TABLE_SIZE_BYTES: LazyLock<GaugeVec> = LazyLock::new(|| {
    GaugeVec::new(
        Opts::new(
            "pico_table_size_bytes",
            "Total number of bytes occupied by memtx table tuples and indexes",
        ),
        &TABLE_METRIC_LABEL_NAMES,
    )
    .expect("Failed to create pico_table_size_bytes gauge")
});

static TABLE_LEN: LazyLock<GaugeVec> = LazyLock::new(|| {
    GaugeVec::new(
        Opts::new("pico_table_len", "Number of tuples in a table"),
        &TABLE_METRIC_LABEL_NAMES,
    )
    .expect("Failed to create pico_table_len gauge")
});

#[derive(Clone, PartialEq, Eq)]
struct TableMetricLabels {
    table_name: String,
    table_kind: &'static str,
    engine: &'static str,
}

impl TableMetricLabels {
    fn values(&self) -> [&str; 3] {
        [&self.table_name, self.table_kind, self.engine]
    }
}

#[derive(Default)]
struct TableMetricsState {
    entries_by_space_id: HashMap<u32, TableMetricLabels>,
    next_entries_by_space_id: HashMap<u32, TableMetricLabels>,
    index_ids_schema_version: u64,
    index_ids_by_space_id: HashMap<u32, Vec<u32>>,
}

static TABLE_METRICS_STATE: LazyLock<Mutex<TableMetricsState>> =
    LazyLock::new(|| Mutex::new(TableMetricsState::default()));

static GOVERNOR_CHANGE_COUNTER: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pico_governor_changes_total",
        "Total number of times the governor status has changed",
    ))
    .expect("Failed to create pico_governor_changes_total counter")
});

static SQL_TX_SPLITS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_sql_tx_splits_total",
            "Total number of SQL transaction splits caused by execution yields",
        ),
        &["tier", "replicaset"],
    )
    .expect("Failed to create pico_sql_tx_splits_total counter")
});

static SQL_YIELD_SLEEP_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "pico_sql_yield_sleep_duration",
            "Histogram of the time spent sleeping during SQL execution yields (in seconds)",
        ),
        &["tier", "replicaset"],
    )
    .expect("Failed to create pico_sql_yield_sleep_duration counter")
});

static SQL_YIELDS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_sql_yields_total",
            "Total number of SQL execution yield events",
        ),
        &["tier", "replicaset"],
    )
    .expect("Failed to create pico_sql_yields_total counter")
});

static SQL_QUERY_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_sql_query_total",
            "Total number of SQL queries executed",
        ),
        &["tier", "replicaset"],
    )
    .expect("Failed to create pico_sql_query_total counter")
});

static SQL_REPLICAS_READ_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_sql_replicas_read_total",
            "Total number of DQL executions on read-only replicas",
        ),
        &["tier", "replicaset"],
    )
    .expect("Failed to create pico_sql_replicas_read_total counter")
});

static SQL_QUERY_ERRORS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_sql_query_errors_total",
            "Total number of SQL queries that resulted in errors",
        ),
        &["tier", "replicaset"],
    )
    .expect("Failed to create pico_sql_query_errors_total counter")
});

static SQL_QUERY_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "pico_sql_query_duration",
            "Histogram of SQL query execution durations (in seconds)",
        ),
        &["tier", "replicaset"],
    )
    .expect("Failed to create pico_sql_query_duration histogram")
});

static SQL_TEMP_TABLE_LEASES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pico_sql_temp_table_leases_total",
        "Total number of temp table leases acquired",
    ))
    .expect("Failed to create pico_sql_temp_table_leases_total counter")
});

static SQL_TEMP_TABLE_LOCK_WAITS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pico_sql_temp_table_lock_waits_total",
        "Total number of waits for temp table locks",
    ))
    .expect("Failed to create pico_sql_temp_table_lock_waits_total counter")
});

static SQL_GLOBAL_DML_QUERY_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pico_sql_global_dml_query_total",
        "Total number of SQL DML queries on global tables executed",
    ))
    .expect("Failed to create pico_sql_query_total counter")
});

static SQL_GLOBAL_DML_QUERY_RETRIES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(
        Opts::new(
            "pico_sql_global_dml_query_retries_total",
            "Total number of SQL DML queries on global tables which failed due to CAS errors and were automatically retried",
        ),
    )
    .expect("Failed to create pico_sql_query_total counter")
});

static RPC_REQUEST_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_rpc_request_total",
            "Total number of RPC requests executed",
        ),
        &["proc_name"],
    )
    .expect("Failed to create pico_rpc_request_total")
});

static RPC_REQUEST_ERRORS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_rpc_request_errors_total",
            "Total number of RPC requests that resulted in errors",
        ),
        &["proc_name"],
    )
    .expect("Failed to create pico_rpc_request_errors_total")
});

static RPC_REQUEST_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "pico_rpc_request_duration",
            "Histogram of RPC request execution durations (in seconds)",
        ),
        &["proc_name"],
    )
    .expect("Failed to create pico_rpc_request_duration histogram")
});

static CAS_RECORDS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_cas_records_total",
            "Total number of records written via CAS operations on global tables",
        ),
        &["op_type", "table"],
    )
    .expect("Failed to create pico_cas_records_total")
});

static CAS_ERRORS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_cas_errors_total",
            "Total number of CAS operations on global tables that resulted in error",
        ),
        &["op_type", "table"],
    )
    .expect("Failed to create pico_cas_errors_total")
});

static CAS_OPS_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::with_opts(HistogramOpts::new(
        "pico_cas_ops_duration",
        "Histogram of CAS operation durations on global tables (in seconds)",
    ))
    .expect("Failed to create pico_cas_ops_duration")
});

static INSTANCE_STATE: LazyLock<GaugeVec> = LazyLock::new(|| {
    GaugeVec::new(
        Opts::new(
            "pico_instance_state",
            "Current state of the instance (Online, Offline, Expelled)",
        ),
        &["tier", "instance", "state"],
    )
    .expect("Failed to create pico_instance_state gauge")
});

static RAFT_APPLIED_INDEX: LazyLock<Gauge> = LazyLock::new(|| {
    Gauge::with_opts(Opts::new(
        "pico_raft_applied_index",
        "Current Raft applied index",
    ))
    .expect("Failed to create pico_raft_applied_index gauge")
});

static RAFT_COMMIT_INDEX: LazyLock<Gauge> = LazyLock::new(|| {
    Gauge::with_opts(Opts::new(
        "pico_raft_commit_index",
        "Current Raft commit index",
    ))
    .expect("Failed to create pico_raft_commit_index gauge")
});

static RAFT_TERM: LazyLock<Gauge> = LazyLock::new(|| {
    Gauge::with_opts(Opts::new("pico_raft_term", "Current Raft term"))
        .expect("Failed to create pico_raft_term gauge")
});

static RAFT_APPLIED_TERM: LazyLock<Gauge> = LazyLock::new(|| {
    Gauge::with_opts(Opts::new(
        "pico_raft_applied_term",
        "Raft term of the last applied entry",
    ))
    .expect("Initializing metrics handlers shouldn't fail")
});

static RAFT_STATE: LazyLock<GaugeVec> = LazyLock::new(|| {
    GaugeVec::new(
        Opts::new(
            "pico_raft_state",
            "Current Raft role (Follower, Candidate, Leader, PreCandidate)",
        ),
        &["state"],
    )
    .expect("Failed to create pico_raft_state gauge")
});

static RAFT_LEADER_ID: LazyLock<Gauge> = LazyLock::new(|| {
    Gauge::with_opts(Opts::new(
        "pico_raft_leader_id",
        "Current Raft leader ID (0 if no leader)",
    ))
    .expect("Failed to create pico_raft_leader_id gauge")
});

static INFO_UPTIME: LazyLock<GaugeVec> = LazyLock::new(|| {
    GaugeVec::new(
        Opts::new("pico_info_uptime", "Picodata uptime"),
        &["instance_dir_name", "replicaset", "tier", "cluster_name"],
    )
    .expect("Failed to create pico_info_uptime gauge")
});

pub static ROUTER_CACHE_STATEMENTS_ADDED_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        prometheus::Opts::new(
            "pico_router_cache_statements_added_total",
            "Total number of statements added to the router cache since startup",
        ),
        &["tier", "replicaset"],
    )
    .unwrap()
});

pub static ROUTER_CACHE_STATEMENTS_EVICTED_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        prometheus::Opts::new(
            "pico_router_cache_statements_evicted_total",
            "Total number of statements evicted from the router cache since startup",
        ),
        &["tier", "replicaset"],
    )
    .unwrap()
});

pub static ROUTER_CACHE_HITS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        prometheus::Opts::new(
            "pico_router_cache_hits_total",
            "Total number of requests to the router cache resulted in cache hit since startup",
        ),
        &["tier", "replicaset"],
    )
    .unwrap()
});

pub static ROUTER_CACHE_MISSES_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        prometheus::Opts::new(
            "pico_router_cache_misses_total",
            "Total number of requests to the router cache resulted in cache miss since startup",
        ),
        &["tier", "replicaset"],
    )
    .unwrap()
});

pub static ROUTER_BLOCK_PATTERN_CACHE_STATEMENTS_ADDED_TOTAL: LazyLock<IntCounterVec> =
    LazyLock::new(|| {
        IntCounterVec::new(
            prometheus::Opts::new(
                "pico_router_block_pattern_cache_statements_added_total",
                "Total number of statements added to the router transactional block pattern cache since startup",
            ),
            &["tier", "replicaset"],
        )
        .unwrap()
    });

pub static ROUTER_BLOCK_PATTERN_CACHE_STATEMENTS_EVICTED_TOTAL: LazyLock<IntCounterVec> =
    LazyLock::new(|| {
        IntCounterVec::new(
            prometheus::Opts::new(
                "pico_router_block_pattern_cache_statements_evicted_total",
                "Total number of statements evicted from the router transactional block pattern cache since startup",
            ),
            &["tier", "replicaset"],
        )
        .unwrap()
    });

pub static ROUTER_BLOCK_PATTERN_CACHE_HITS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        prometheus::Opts::new(
            "pico_router_block_pattern_cache_hits_total",
            "Total number of router transactional block pattern cache hits since startup",
        ),
        &["tier", "replicaset"],
    )
    .unwrap()
});

pub static ROUTER_BLOCK_PATTERN_CACHE_MISSES_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        prometheus::Opts::new(
            "pico_router_block_pattern_cache_misses_total",
            "Total number of router transactional block pattern cache misses since startup",
        ),
        &["tier", "replicaset"],
    )
    .unwrap()
});

pub static STORAGE_CACHE_STATEMENTS_ADDED_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        prometheus::Opts::new(
            "pico_storage_cache_statements_added_total",
            "Total number of statements added to the storage cache since startup",
        ),
        &["tier", "replicaset"],
    )
    .unwrap()
});

pub static STORAGE_CACHE_STATEMENTS_EVICTED_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        prometheus::Opts::new(
            "pico_storage_cache_statements_evicted_total",
            "Total number of statements evicted from the storage cache since startup",
        ),
        &["tier", "replicaset"],
    )
    .unwrap()
});

pub static STORAGE_1ST_REQUESTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_storage_1st_requests_total",
            "Total number of 1st requests to the storage cache since startup (aka total number of requests to the cache)",
        ),
        &["tier", "replicaset", "query_type", "result"],
    )
    .unwrap()
});

pub static STORAGE_2ND_REQUESTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_storage_2nd_requests_total",
            "Total number of 2nd reqests to the storage cache since startup (aka total number of cache misses)",
        ),
        &["tier", "replicaset", "query_type", "result"]
    )
    .unwrap()
});

pub static SQL_LOCAL_QUERY_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_sql_local_query_total",
            "Total number of local SQL query executions that bypass iproto since startup",
        ),
        &["tier", "replicaset", "query_type", "result"],
    )
    .unwrap()
});

pub static SQL_LOCAL_QUERY_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "pico_sql_local_query_duration",
            "Histogram of local SQL query execution durations that bypass iproto (in seconds)",
        ),
        &["tier", "replicaset", "query_type", "result"],
    )
    .unwrap()
});

pub static SQL_STORAGE_QUERY_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "pico_sql_storage_query_duration",
            "Histogram of storage-side SQL query execution durations served via .proc_sql_execute (in seconds)",
        )
        .buckets(vec![
            0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0,
        ]),
        &["tier", "replicaset", "query_type", "result"],
    )
    .unwrap()
});

pub static STORAGE_CACHE_HITS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_storage_cache_hits_total",
            "The total number cache hits on the storage",
        ),
        &["tier", "replicaset", "query_type", "rpc_type"],
    )
    .unwrap()
});

pub static STORAGE_CACHE_MISSES_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "pico_storage_cache_misses_total",
            "The total number of cache misses on the storage",
        ),
        &["tier", "replicaset", "query_type", "rpc_type", "miss_type"],
    )
    .unwrap()
});

pub fn report_storage_cache_hit(query_type: &str, rpc_type: &str) {
    STORAGE_CACHE_HITS_TOTAL
        .with_label_values(&[my_tier(), my_replicaset(), query_type, rpc_type])
        .inc()
}

pub fn record_sql_local_query_total(query_type: &str, result: &str) {
    SQL_LOCAL_QUERY_TOTAL
        .with_label_values(&[my_tier(), my_replicaset(), query_type, result])
        .inc()
}

pub fn observe_sql_local_query_duration(query_type: &str, result: &str, duration: &Duration) {
    SQL_LOCAL_QUERY_DURATION
        .with_label_values(&[my_tier(), my_replicaset(), query_type, result])
        .observe(duration.as_secs_f64())
}

pub fn observe_sql_storage_query_duration(query_type: &str, result: &str, duration: &Duration) {
    SQL_STORAGE_QUERY_DURATION
        .with_label_values(&[my_tier(), my_replicaset(), query_type, result])
        .observe(duration.as_secs_f64())
}

pub fn report_storage_cache_miss(query_type: &str, rpc_type: &str, miss_type: &str) {
    STORAGE_CACHE_MISSES_TOTAL
        .with_label_values(&[my_tier(), my_replicaset(), query_type, rpc_type, miss_type])
        .inc()
}

pub fn record_router_cache_hit() {
    ROUTER_CACHE_HITS_TOTAL
        .with_label_values(&[my_tier(), my_replicaset()])
        .inc();
}

pub fn record_router_cache_miss() {
    ROUTER_CACHE_MISSES_TOTAL
        .with_label_values(&[my_tier(), my_replicaset()])
        .inc();
}

pub fn record_router_cache_statement_added() {
    let tier = my_tier();
    let replicaset = my_replicaset();
    ROUTER_CACHE_STATEMENTS_ADDED_TOTAL
        .with_label_values(&[tier, replicaset])
        .inc();
    // Prime the evicted counter at 0 for the same labelset so dashboards can
    // freely write `added - evicted` without an `OR vector(0)` fallback.
    ROUTER_CACHE_STATEMENTS_EVICTED_TOTAL
        .with_label_values(&[tier, replicaset])
        .inc_by(0);
}

pub fn record_router_cache_statement_evicted() {
    ROUTER_CACHE_STATEMENTS_EVICTED_TOTAL
        .with_label_values(&[my_tier(), my_replicaset()])
        .inc();
}

pub fn record_router_block_pattern_cache_hit() {
    ROUTER_BLOCK_PATTERN_CACHE_HITS_TOTAL
        .with_label_values(&[my_tier(), my_replicaset()])
        .inc();
}

pub fn record_router_block_pattern_cache_miss() {
    ROUTER_BLOCK_PATTERN_CACHE_MISSES_TOTAL
        .with_label_values(&[my_tier(), my_replicaset()])
        .inc();
}

pub fn record_router_block_pattern_cache_statement_added() {
    let tier = my_tier();
    let replicaset = my_replicaset();
    ROUTER_BLOCK_PATTERN_CACHE_STATEMENTS_ADDED_TOTAL
        .with_label_values(&[tier, replicaset])
        .inc();
    ROUTER_BLOCK_PATTERN_CACHE_STATEMENTS_EVICTED_TOTAL
        .with_label_values(&[tier, replicaset])
        .inc_by(0);
}

pub fn record_router_block_pattern_cache_statement_evicted() {
    ROUTER_BLOCK_PATTERN_CACHE_STATEMENTS_EVICTED_TOTAL
        .with_label_values(&[my_tier(), my_replicaset()])
        .inc();
}

pub fn record_storage_cache_statement_added() {
    let tier = my_tier();
    let replicaset = my_replicaset();
    STORAGE_CACHE_STATEMENTS_ADDED_TOTAL
        .with_label_values(&[tier, replicaset])
        .inc();
    STORAGE_CACHE_STATEMENTS_EVICTED_TOTAL
        .with_label_values(&[tier, replicaset])
        .inc_by(0);
}

pub fn record_storage_cache_statement_evicted() {
    STORAGE_CACHE_STATEMENTS_EVICTED_TOTAL
        .with_label_values(&[my_tier(), my_replicaset()])
        .inc();
}

pub fn record_storage_1st_request(query_type: &str, result: &str) {
    STORAGE_1ST_REQUESTS_TOTAL
        .with_label_values(&[my_tier(), my_replicaset(), query_type, result])
        .inc();
}

pub fn record_storage_2nd_request(query_type: &str, result: &str) {
    STORAGE_2ND_REQUESTS_TOTAL
        .with_label_values(&[my_tier(), my_replicaset(), query_type, result])
        .inc();
}

pub fn record_governor_change() {
    GOVERNOR_CHANGE_COUNTER.inc();
}

pub fn record_sql_tx_splits_total() {
    SQL_TX_SPLITS_TOTAL
        .with_label_values(&[my_tier(), my_replicaset()])
        .inc();
}

pub fn record_sql_yields_total() {
    SQL_YIELDS_TOTAL
        .with_label_values(&[my_tier(), my_replicaset()])
        .inc();
}

pub fn record_sql_yield_sleep_duration(duration: &Duration) {
    let seconds = duration.as_secs_f64();
    SQL_YIELD_SLEEP_DURATION
        .with_label_values(&[my_tier(), my_replicaset()])
        .observe(seconds);
}

pub fn record_sql_query_total(tier: &str, replicaset: &str) {
    SQL_QUERY_TOTAL.with_label_values(&[tier, replicaset]).inc();
}

pub fn record_sql_replicas_read_total() {
    SQL_REPLICAS_READ_TOTAL
        .with_label_values(&[my_tier(), my_replicaset()])
        .inc();
}

pub fn record_sql_global_dml_query_total() {
    SQL_GLOBAL_DML_QUERY_TOTAL.inc();
}

pub fn record_sql_global_dml_query_retries_total() {
    SQL_GLOBAL_DML_QUERY_RETRIES_TOTAL.inc();
}

pub fn record_sql_query_errors_total(tier: &str, replicaset: &str) {
    SQL_QUERY_ERRORS_TOTAL
        .with_label_values(&[tier, replicaset])
        .inc();
}

pub fn observe_sql_query_duration(tier: &str, replicaset: &str, duration: &Duration) {
    let seconds = duration.as_secs_f64();
    SQL_QUERY_DURATION
        .with_label_values(&[tier, replicaset])
        .observe(seconds);
}

pub fn record_sql_temp_table_leases_total() {
    SQL_TEMP_TABLE_LEASES_TOTAL.inc();
}

pub fn record_sql_temp_table_lock_waits_total() {
    SQL_TEMP_TABLE_LOCK_WAITS_TOTAL.inc();
}

pub fn record_rpc_request_total(proc_name: &str) {
    RPC_REQUEST_TOTAL.with_label_values(&[proc_name]).inc();
}

pub fn record_rpc_request_errors_total(proc_name: &str) {
    RPC_REQUEST_ERRORS_TOTAL
        .with_label_values(&[proc_name])
        .inc();
}

pub fn observe_rpc_request_duration(proc_name: &str, duration: &Duration) {
    let seconds = duration.as_secs_f64();
    RPC_REQUEST_DURATION
        .with_label_values(&[proc_name])
        .observe(seconds);
}

pub fn record_cas_ops_total(cas_ops: &Op) {
    let operations = get_op_type_and_table(cas_ops);

    for (op_type, table) in operations {
        CAS_RECORDS_TOTAL
            .with_label_values(&[op_type, &table])
            .inc();
    }
}

pub fn record_cas_errors_total(cas_ops: &Op) {
    let operations = get_op_type_and_table(cas_ops);

    for (op_type, table) in operations {
        CAS_ERRORS_TOTAL.with_label_values(&[op_type, &table]).inc();
    }
}

pub fn observe_cas_ops_duration(duration: &Duration) {
    let seconds = duration.as_secs_f64();
    CAS_OPS_DURATION.observe(seconds);
}

pub fn record_instance_state(tier: &str, instance_name: &str, state: &StateVariant) {
    // clean up previous metric, we dont need to keep it around
    for s in StateVariant::values() {
        let _ = INSTANCE_STATE.remove_label_values(&[tier, instance_name, s]);
    }

    INSTANCE_STATE
        .with_label_values(&[tier, instance_name, state.as_str()])
        .set(1.0); // Always set to 1.0 to avoid resetting for each state
}

pub fn record_raft_applied_index(index: u64) {
    RAFT_APPLIED_INDEX.set(index as f64);
}

pub fn record_raft_commit_index(index: u64) {
    RAFT_COMMIT_INDEX.set(index as f64);
}

pub fn record_raft_term(term: u64) {
    RAFT_TERM.set(term as f64);
}

pub fn record_raft_applied_term(term: u64) {
    RAFT_APPLIED_TERM.set(term as f64);
}

pub fn record_raft_state(state: raft::StateRole) {
    let state_value = match state {
        ::raft::StateRole::Follower => "Follower",
        ::raft::StateRole::PreCandidate => "PreCandidate",
        ::raft::StateRole::Candidate => "Candidate",
        ::raft::StateRole::Leader => "Leader",
    };
    RAFT_STATE.with_label_values(&[state_value]).set(1.0);
}

pub fn record_raft_leader_id(leader_id: Option<u64>) {
    RAFT_LEADER_ID.set(leader_id.unwrap_or(0) as f64);
}

/// Refresh table metrics from Tarantool system spaces.
pub fn update_table_metrics() {
    let mut state = TABLE_METRICS_STATE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Err(error) = update_table_index_ids_cache(&mut state) {
        tlog!(Error, "failed collecting table metrics: {error}");
        return;
    }
    state.next_entries_by_space_id.clear();

    let spaces = tarantool::space::SystemSpace::Space.as_space();
    let Ok(spaces) = spaces.select(tarantool::index::IteratorType::All, &()) else {
        tlog!(Error, "failed collecting table metrics: cannot scan _space");
        return;
    };

    for tuple in spaces {
        let (Ok(Some(space_id)), Ok(Some(table_name)), Ok(Some(engine))) = (
            tuple.field::<u32>(0),
            tuple.field::<&str>(2),
            tuple.field::<&str>(3),
        ) else {
            tlog!(
                Error,
                "failed collecting table metrics: invalid _space tuple"
            );
            continue;
        };
        let engine = match engine {
            "memtx" => "memtx",
            "vinyl" => "vinyl",
            _ => continue,
        };

        let table_kind = if space_id <= crate::storage::SPACE_ID_INTERNAL_MAX {
            "system"
        } else {
            let Ok(Some(flags)) =
                tuple.field::<BTreeMap<Cow<'_, str>, tarantool::util::Value<'_>>>(5)
            else {
                tlog!(
                    Error,
                    "failed collecting table metrics: invalid _space flags"
                );
                continue;
            };
            if matches!(
                flags.get("type"),
                Some(tarantool::util::Value::Str(kind)) if kind == "temporary"
            ) {
                "temporary"
            } else {
                "user"
            }
        };
        let labels = TableMetricLabels {
            table_name: table_name.into(),
            table_kind,
            engine,
        };
        let metric = if table_kind == "temporary" {
            table_index_ids_for_space(space_id).and_then(|index_ids| {
                collect_one_table_metric(space_id, &index_ids, engine == "memtx")
            })
        } else {
            let index_ids = state
                .index_ids_by_space_id
                .get(&space_id)
                .map(Vec::as_slice)
                .unwrap_or_default();
            collect_one_table_metric(space_id, index_ids, engine == "memtx")
        };

        match metric {
            Ok((size_bytes, tuple_count)) => {
                if let Some(size_bytes) = size_bytes {
                    TABLE_SIZE_BYTES
                        .with_label_values(&labels.values())
                        .set(size_bytes as f64);
                }
                TABLE_LEN
                    .with_label_values(&labels.values())
                    .set(tuple_count as f64);
                state.next_entries_by_space_id.insert(space_id, labels);
            }
            Err(error) => {
                tlog!(
                    Error,
                    "failed collecting table metrics for space {space_id}: {error}"
                );
                if let Some(labels) = state.entries_by_space_id.get(&space_id).cloned() {
                    state.next_entries_by_space_id.insert(space_id, labels);
                }
            }
        }
    }

    remove_stale_table_metrics(&state.entries_by_space_id, &state.next_entries_by_space_id);
    let TableMetricsState {
        entries_by_space_id,
        next_entries_by_space_id,
        ..
    } = &mut *state;
    std::mem::swap(entries_by_space_id, next_entries_by_space_id);
}

fn update_table_index_ids_cache(state: &mut TableMetricsState) -> tarantool::Result<()> {
    let schema_version = crate::tarantool::box_schema_version();
    if state.index_ids_schema_version == schema_version {
        return Ok(());
    }

    let indexes = tarantool::space::SystemSpace::Index.as_space();
    state.index_ids_by_space_id.clear();
    for tuple in indexes.select(tarantool::index::IteratorType::All, &())? {
        let metadata = tuple.decode::<tarantool::index::Metadata<'_>>()?;
        state
            .index_ids_by_space_id
            .entry(metadata.space_id)
            .or_default()
            .push(metadata.index_id);
    }
    state.index_ids_schema_version = schema_version;
    Ok(())
}

fn table_index_ids_for_space(space_id: u32) -> tarantool::Result<Vec<u32>> {
    let indexes = tarantool::space::SystemSpace::Index.as_space();
    indexes
        .select(tarantool::index::IteratorType::Eq, &(space_id,))?
        .map(|tuple| Ok(tuple.field(1)?.expect("database constraint violation")))
        .collect()
}

fn collect_one_table_metric(
    space_id: u32,
    index_ids: &[u32],
    collect_size: bool,
) -> tarantool::Result<(Option<usize>, usize)> {
    let space = crate::storage::space_by_id_unchecked(space_id);
    let size_bytes = if collect_size {
        let mut size_bytes = space.bsize()?;
        for &index_id in index_ids {
            // SAFETY: IDs came from the current contents of Tarantool's _index.
            let index = unsafe { tarantool::index::Index::from_ids_unchecked(space_id, index_id) };
            size_bytes += index.bsize()?;
        }
        Some(size_bytes)
    } else {
        None
    };
    let tuple_count = if index_ids.is_empty() {
        0
    } else {
        space.len()?
    };
    Ok((size_bytes, tuple_count))
}

fn remove_stale_table_metrics(
    previous: &HashMap<u32, TableMetricLabels>,
    current: &HashMap<u32, TableMetricLabels>,
) {
    for (space_id, labels) in previous {
        if current.get(space_id) != Some(labels) {
            let _ = TABLE_SIZE_BYTES.remove_label_values(&labels.values());
            let _ = TABLE_LEN.remove_label_values(&labels.values());
        }
    }
}

pub fn register_metrics(registry: &prometheus::Registry) -> prometheus::Result<()> {
    registry.register(Box::new(CAS_ERRORS_TOTAL.clone()))?;
    registry.register(Box::new(TABLE_SIZE_BYTES.clone()))?;
    registry.register(Box::new(TABLE_LEN.clone()))?;
    registry.register(Box::new(CAS_OPS_DURATION.clone()))?;
    registry.register(Box::new(CAS_RECORDS_TOTAL.clone()))?;
    registry.register(Box::new(GOVERNOR_CHANGE_COUNTER.clone()))?;
    registry.register(Box::new(INSTANCE_STATE.clone()))?;
    registry.register(Box::new(RAFT_APPLIED_INDEX.clone()))?;
    registry.register(Box::new(RAFT_COMMIT_INDEX.clone()))?;
    registry.register(Box::new(RAFT_LEADER_ID.clone()))?;
    registry.register(Box::new(RAFT_STATE.clone()))?;
    registry.register(Box::new(RAFT_TERM.clone()))?;
    registry.register(Box::new(RPC_REQUEST_DURATION.clone()))?;
    registry.register(Box::new(RPC_REQUEST_ERRORS_TOTAL.clone()))?;
    registry.register(Box::new(RPC_REQUEST_TOTAL.clone()))?;
    registry.register(Box::new(SQL_TX_SPLITS_TOTAL.clone()))?;
    registry.register(Box::new(SQL_YIELD_SLEEP_DURATION.clone()))?;
    registry.register(Box::new(SQL_YIELDS_TOTAL.clone()))?;
    registry.register(Box::new(SQL_QUERY_DURATION.clone()))?;
    registry.register(Box::new(SQL_QUERY_ERRORS_TOTAL.clone()))?;
    registry.register(Box::new(SQL_QUERY_TOTAL.clone()))?;
    registry.register(Box::new(SQL_REPLICAS_READ_TOTAL.clone()))?;
    registry.register(Box::new(SQL_GLOBAL_DML_QUERY_TOTAL.clone()))?;
    registry.register(Box::new(SQL_GLOBAL_DML_QUERY_RETRIES_TOTAL.clone()))?;
    registry.register(Box::new(INFO_UPTIME.clone()))?;
    registry.register(Box::new(STORAGE_CACHE_STATEMENTS_ADDED_TOTAL.clone()))?;
    registry.register(Box::new(STORAGE_CACHE_STATEMENTS_EVICTED_TOTAL.clone()))?;
    registry.register(Box::new(STORAGE_1ST_REQUESTS_TOTAL.clone()))?;
    registry.register(Box::new(STORAGE_2ND_REQUESTS_TOTAL.clone()))?;
    registry.register(Box::new(SQL_LOCAL_QUERY_DURATION.clone()))?;
    registry.register(Box::new(SQL_LOCAL_QUERY_TOTAL.clone()))?;
    registry.register(Box::new(SQL_STORAGE_QUERY_DURATION.clone()))?;
    registry.register(Box::new(ROUTER_CACHE_STATEMENTS_ADDED_TOTAL.clone()))?;
    registry.register(Box::new(ROUTER_CACHE_STATEMENTS_EVICTED_TOTAL.clone()))?;
    registry.register(Box::new(ROUTER_CACHE_HITS_TOTAL.clone()))?;
    registry.register(Box::new(ROUTER_CACHE_MISSES_TOTAL.clone()))?;
    registry.register(Box::new(
        ROUTER_BLOCK_PATTERN_CACHE_STATEMENTS_ADDED_TOTAL.clone(),
    ))?;
    registry.register(Box::new(
        ROUTER_BLOCK_PATTERN_CACHE_STATEMENTS_EVICTED_TOTAL.clone(),
    ))?;
    registry.register(Box::new(ROUTER_BLOCK_PATTERN_CACHE_HITS_TOTAL.clone()))?;
    registry.register(Box::new(ROUTER_BLOCK_PATTERN_CACHE_MISSES_TOTAL.clone()))?;
    registry.register(Box::new(STORAGE_CACHE_HITS_TOTAL.clone()))?;
    registry.register(Box::new(STORAGE_CACHE_MISSES_TOTAL.clone()))?;
    registry.register(Box::new(SQL_TEMP_TABLE_LEASES_TOTAL.clone()))?;
    registry.register(Box::new(SQL_TEMP_TABLE_LOCK_WAITS_TOTAL.clone()))?;

    Ok(())
}

pub fn collect_from_registry(registry: &prometheus::Registry) -> String {
    update_pico_info_uptime();
    update_table_metrics();
    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    encoder.encode_to_string(&metric_families).unwrap()
}

pub fn get_op_type_and_table(op: &Op) -> Vec<(&str, SmolStr)> {
    let mut operations = vec![];

    match op {
        Op::Dml(dml) => {
            let op_type = dml.kind().as_str();
            operations.push((op_type, dml.table_id().to_smolstr()));
        }
        Op::BatchDml { ops } => {
            for dml in ops {
                let op_type = dml.kind().as_str();
                operations.push((op_type, dml.table_id().to_smolstr()));
            }
        }
        Op::DdlPrepare { ddl, .. } => match ddl {
            Ddl::Backup { .. } => {
                operations.push(("ddl_backup", Default::default()));
            }
            Ddl::CreateTable { name, .. } => {
                operations.push(("ddl_create_table", name.clone()));
            }
            Ddl::DropTable { id, .. } => {
                operations.push(("ddl_drop_table", id.to_smolstr()));
            }
            Ddl::TruncateTable { id, .. } => {
                operations.push(("ddl_truncate_table", id.to_smolstr()));
            }
            Ddl::ChangeFormat { table_id, .. } => {
                operations.push(("ddl_change_format", table_id.to_smolstr()));
            }
            Ddl::RenameTable {
                old_name, new_name, ..
            } => {
                operations.push(("ddl_rename_table", format_smolstr!("{old_name}→{new_name}")));
            }
            Ddl::CreateIndex {
                space_id, index_id, ..
            } => {
                operations.push(("ddl_create_index", format_smolstr!("{space_id}:{index_id}")));
            }
            Ddl::DropIndex {
                space_id, index_id, ..
            } => {
                operations.push(("ddl_drop_index", format_smolstr!("{space_id}:{index_id}")));
            }
            Ddl::RenameIndex {
                old_name, new_name, ..
            } => operations.push(("ddl_rename_index", format_smolstr!("{old_name}→{new_name}"))),
            Ddl::CreateProcedure { name, .. } => {
                operations.push(("ddl_create_procedure", name.clone()));
            }
            Ddl::DropProcedure { id, .. } => {
                operations.push(("ddl_drop_procedure", id.to_smolstr()));
            }
            Ddl::RenameProcedure {
                old_name, new_name, ..
            } => {
                operations.push((
                    "ddl_rename_procedure",
                    format_smolstr!("{old_name} -> {new_name}"),
                ));
            }
        },
        Op::Acl(acl) => match acl {
            Acl::CreateUser { user_def } => {
                operations.push(("acl_create_user", user_def.name.clone()));
            }
            Acl::RenameUser { name, .. } => {
                operations.push(("acl_rename_user", name.clone()));
            }
            Acl::ChangeAuth { user_id, .. } => {
                operations.push(("acl_change_auth", user_id.to_smolstr()));
            }
            Acl::DropUser { user_id, .. } => {
                operations.push(("acl_drop_user", user_id.to_smolstr()));
            }
            Acl::CreateRole { role_def } => {
                operations.push(("acl_create_role", role_def.name.clone()));
            }
            Acl::DropRole { role_id, .. } => {
                operations.push(("acl_drop_role", role_id.to_smolstr()));
            }
            Acl::GrantPrivilege { priv_def } => {
                operations.push(("acl_grant_privilege", priv_def.object_type().to_smolstr()));
            }
            Acl::RevokePrivilege { priv_def, .. } => {
                operations.push(("acl_revoke_privilege", priv_def.object_type().to_smolstr()));
            }
            Acl::AuditPolicy { user_id, .. } => {
                operations.push(("acl_audit_policy", user_id.to_smolstr()));
            }
        },
        _ => {
            operations.push(("other", SmolStr::new_static("global")));
        }
    }

    operations
}

fn update_pico_info_uptime() {
    let uptime: f64 = unsafe { tarantool_uptime() };

    let instance_dir_name = PicodataConfig::get()
        .instance
        .instance_dir()
        .file_name()
        .map(OsStr::to_string_lossy)
        .unwrap_or_else(|| Cow::Owned(String::from("unknown")));

    INFO_UPTIME
        .with_label_values(&[
            instance_dir_name.as_ref(),
            my_replicaset(),
            my_tier(),
            my_cluster(),
        ])
        .set(uptime);
}

#[inline(always)]
fn my_replicaset() -> &'static str {
    let mut replicaset = "unknown";
    if let Ok(node) = node::global() {
        replicaset = node.topology_cache.my_replicaset_name();
    }
    replicaset
}

#[inline(always)]
fn my_tier() -> &'static str {
    let mut tier = "unknown";
    if let Ok(node) = node::global() {
        tier = node.topology_cache.my_tier_name();
    }
    tier
}

#[inline(always)]
fn my_cluster() -> &'static str {
    let mut cluster = "unknown";
    if let Ok(node) = node::global() {
        cluster = node.topology_cache.cluster_name;
    }
    cluster
}
