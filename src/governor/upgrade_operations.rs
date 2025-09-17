/// Maps system catalog versions to their corresponding upgrade governor operations.
///
/// This is an append-only array. The current system catalog version is defined by the last element.
///
/// # Governor Operations
/// - `sql`: Executes an SQL statement.
/// - `proc_name`: Creates a Tarantool procedure on all master replicas.
pub const CATALOG_UPGRADE_LIST: &'static [(
    &'static str,
    &'static [(&'static str, &'static str)],
)] = &[
    (
        "25.3.1",
        &[
            ("proc_name", "proc_before_online"),
            ("proc_name", "proc_cas_v2"),
            ("proc_name", "proc_instance_uuid"),
            ("proc_name", "proc_raft_leader_uuid"),
            ("proc_name", "proc_raft_leader_id"),
            ("proc_name", "proc_picodata_version"),
        ],
    ),
    ("25.3.3", &[("proc_name", "proc_runtime_info_v2")]),
    (
        "25.4.1",
        &[
            ("proc_name", "proc_backup_abort_clear"),
            ("proc_name", "proc_apply_backup"),
        ],
    ),
];
