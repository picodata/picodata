use crate::config::PicodataConfig;
use crate::info as pd_info;
use crate::traft::node;
use crate::traft::op::{Acl, Ddl, Op};
use prometheus::{
    Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts,
    TextEncoder,
};
use std::borrow::Cow;
use std::ffi::OsStr;
use std::sync::LazyLock;

extern "C" {
    fn tarantool_uptime() -> f64;
}

static GOVERNOR_CHANGE_COUNTER: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(Opts::new(
        "pico_governor_changes_total",
        "Total number of times the governor status has changed",
    ))
    .expect("Failed to create pico_governor_changes_total counter")
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
            "Histogram of SQL query execution durations (in milliseconds)",
        ),
        &["tier", "replicaset"],
    )
    .expect("Failed to create pico_sql_query_duration histogram")
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
            "Histogram of RPC request execution durations (in milliseconds)",
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
        "Histogram of CAS operation durations on global tables (in milliseconds)",
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

pub fn record_governor_change() {
    GOVERNOR_CHANGE_COUNTER.inc();
}

pub fn record_sql_query_total(tier: &str, replicaset: &str) {
    SQL_QUERY_TOTAL.with_label_values(&[tier, replicaset]).inc();
}

pub fn record_sql_query_errors_total(tier: &str, replicaset: &str) {
    SQL_QUERY_ERRORS_TOTAL
        .with_label_values(&[tier, replicaset])
        .inc();
}

pub fn observe_sql_query_duration(tier: &str, replicaset: &str, duration_ms: f64) {
    SQL_QUERY_DURATION
        .with_label_values(&[tier, replicaset])
        .observe(duration_ms);
}

pub fn record_rpc_request_total(proc_name: &str) {
    RPC_REQUEST_TOTAL.with_label_values(&[proc_name]).inc();
}

pub fn record_rpc_request_errors_total(proc_name: &str) {
    RPC_REQUEST_ERRORS_TOTAL
        .with_label_values(&[proc_name])
        .inc();
}

pub fn observe_rpc_request_duration(proc_name: &str, duration_ms: f64) {
    RPC_REQUEST_DURATION
        .with_label_values(&[proc_name])
        .observe(duration_ms);
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

pub fn observe_cas_ops_duration(duration_ms: f64) {
    CAS_OPS_DURATION.observe(duration_ms);
}

pub fn record_instance_state(tier: &str, instance_name: &str, state: &str) {
    INSTANCE_STATE
        .with_label_values(&[tier, instance_name, state])
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

pub fn register_metrics(registry: &prometheus::Registry) -> prometheus::Result<()> {
    registry.register(Box::new(CAS_ERRORS_TOTAL.clone()))?;
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
    registry.register(Box::new(SQL_QUERY_DURATION.clone()))?;
    registry.register(Box::new(SQL_QUERY_ERRORS_TOTAL.clone()))?;
    registry.register(Box::new(SQL_QUERY_TOTAL.clone()))?;
    registry.register(Box::new(INFO_UPTIME.clone()))?;

    Ok(())
}

pub fn collect_from_registry(registry: &prometheus::Registry) -> String {
    update_pico_info_uptime();
    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    encoder.encode_to_string(&metric_families).unwrap()
}

pub fn get_op_type_and_table(op: &Op) -> Vec<(&str, String)> {
    let mut operations = vec![];

    match op {
        Op::Dml(dml) => {
            let op_type = dml.kind().as_str();
            operations.push((op_type, dml.table_id().to_string()));
        }
        Op::BatchDml { ops } => {
            for dml in ops {
                let op_type = dml.kind().as_str();
                operations.push((op_type, dml.table_id().to_string()));
            }
        }
        Op::DdlPrepare { ddl, .. } => match ddl {
            Ddl::Backup { .. } => {
                operations.push(("ddl_backup", String::new()));
            }
            Ddl::CreateTable { name, .. } => {
                operations.push(("ddl_create_table", name.clone()));
            }
            Ddl::DropTable { id, .. } => {
                operations.push(("ddl_drop_table", id.to_string()));
            }
            Ddl::TruncateTable { id, .. } => {
                operations.push(("ddl_truncate_table", id.to_string()));
            }
            Ddl::ChangeFormat { table_id, .. } => {
                operations.push(("ddl_change_format", table_id.to_string()));
            }
            Ddl::RenameTable {
                old_name, new_name, ..
            } => {
                operations.push(("ddl_rename_table", format!("{old_name}→{new_name}")));
            }
            Ddl::CreateIndex {
                space_id, index_id, ..
            } => {
                operations.push(("ddl_create_index", format!("{space_id}:{index_id}")));
            }
            Ddl::DropIndex {
                space_id, index_id, ..
            } => {
                operations.push(("ddl_drop_index", format!("{space_id}:{index_id}")));
            }
            Ddl::CreateProcedure { name, .. } => {
                operations.push(("ddl_create_procedure", name.clone()));
            }
            Ddl::DropProcedure { id, .. } => {
                operations.push(("ddl_drop_procedure", id.to_string()));
            }
            Ddl::RenameProcedure {
                old_name, new_name, ..
            } => {
                operations.push(("ddl_rename_procedure", format!("{old_name} -> {new_name}")));
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
                operations.push(("acl_change_auth", user_id.to_string()));
            }
            Acl::DropUser { user_id, .. } => {
                operations.push(("acl_drop_user", user_id.to_string()));
            }
            Acl::CreateRole { role_def } => {
                operations.push(("acl_create_role", role_def.name.clone()));
            }
            Acl::DropRole { role_id, .. } => {
                operations.push(("acl_drop_role", role_id.to_string()));
            }
            Acl::GrantPrivilege { priv_def } => {
                operations.push(("acl_grant_privilege", priv_def.object_type().to_string()));
            }
            Acl::RevokePrivilege { priv_def, .. } => {
                operations.push(("acl_revoke_privilege", priv_def.object_type().to_string()));
            }
            Acl::AuditPolicy { user_id, .. } => {
                operations.push(("acl_audit_policy", user_id.to_string()));
            }
        },
        _ => {
            operations.push(("other", "global".into()));
        }
    }

    operations
}

fn update_pico_info_uptime() {
    let uptime: f64 = unsafe { tarantool_uptime() };

    let (replicaset, tier, cluster_name) = node::global()
        .ok()
        .and_then(|node| pd_info::InstanceInfo::try_get(node, None).ok())
        .map(|info| (info.replicaset_name.0, info.tier, info.cluster_name))
        .unwrap_or_else(|| {
            (
                String::from("unknown"),
                String::from("unknown"),
                String::from("unknown"),
            )
        });

    let instance_dir_name = PicodataConfig::get()
        .instance
        .instance_dir()
        .file_name()
        .map(OsStr::to_string_lossy)
        .unwrap_or_else(|| Cow::Owned(String::from("unknown")));

    INFO_UPTIME
        .with_label_values(&[
            instance_dir_name.as_ref(),
            replicaset.as_str(),
            tier.as_str(),
            cluster_name.as_str(),
        ])
        .set(uptime);
}
