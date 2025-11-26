use crate::catalog::pico_bucket::PicoBucket;
use crate::catalog::pico_resharding_state::PicoReshardingState;
use crate::storage::schema::ddl_change_format_on_master;
use crate::storage::{Replicasets, SystemTable, Tiers};
use crate::tier::DEFAULT_TIER;
use crate::tlog;
use crate::traft;
use std::rc::Rc;
use tarantool::{fiber, transaction};

/// Maps system catalog versions to their corresponding upgrade governor operations.
///
/// This is an append-only array. The current system catalog version is defined by the last element.
///
/// # Governor Operations
/// - `sql`: Executes an SQL statement.
/// - `proc_name`: Creates a Tarantool procedure on all master replicas.
/// - `exec_script` - Executes internal script on all instances.
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
            ("sql", "CREATE TABLE _pico_user_audit_policy (user_id UNSIGNED NOT NULL, policy_id UNSIGNED NOT NULL, PRIMARY KEY (user_id, policy_id)) DISTRIBUTED GLOBALLY"),
            ("proc_name", "proc_internal_script"),
            ("exec_script", "alter_pico_tier_add_is_default"),
            ("sql", "UPDATE _pico_tier SET is_default = true WHERE 1 in (SELECT count(*) FROM _pico_tier)"),
            ("sql", "UPDATE _pico_tier SET is_default = CASE WHEN name = 'default' THEN true ELSE false END"),
        ],
    ),
    (
        "25.5.1",
        &[
            ("proc_name", "proc_config_file"),
            ("proc_name", "proc_instance_dir"),
            ("proc_name", "proc_instance_name"),
            ("proc_name", "proc_replicaset_name"),
            ("proc_name", "proc_tier_name"),
            ("exec_script", "alter_pico_replicaset_add_master_change_counter"),
            ("exec_script", "alter_pico_replicaset_add_bucket_state_fields"),
            ("exec_script", "alter_pico_tier_add_bucket_state_fields"),
            ("sql", PicoBucket::SQL_CREATE),
            ("sql", PicoReshardingState::SQL_CREATE),
        ],
    ),
];

tarantool::define_str_enum! {
    /// List of internal scripts.
    pub enum InternalScript {
        /// Schema upgrade operation `ALTER TABLE _pico_tier ADD is_default bool`.
        ///
        /// NOTE: this must be done as a special script step instead of simple
        /// SQL, because alterring any of _pico_instance, _pico_replicaset,
        /// _pico_tier via SQL will block any CAS requests into those tables
        /// which may lead to a deadlock if any instance in the cluster goes
        /// offline during cluster upgrade.
        AlterPicoTierAddIsDefault = "alter_pico_tier_add_is_default",

        /// Schema upgrade operation equivalent to:
        /// ```ignore
        /// ALTER TABLE _pico_replicaset ADD COLUMN master_change_counter UNSIGNED NULL
        /// ```
        AlterPicoReplicasetAddMasterChangeCounter = "alter_pico_replicaset_add_master_change_counter",

        /// Schema upgrade operation equivalent to:
        /// ```ignore
        /// ALTER TABLE _pico_replicaset ADD COLUMN
        ///     current_bucket_state_version UNSIGNED NULL,
        ///     target_bucket_state_version  UNSIGNED NULL
        /// ```
        AlterPicoReplicasetAddBucketStateFields = "alter_pico_replicaset_add_bucket_state_fields",

        /// Schema upgrade operation equivalent to:
        /// ```ignore
        /// ALTER TABLE _pico_tier ADD COLUMN
        ///     current_bucket_state_version UNSIGNED NULL,
        ///     target_bucket_state_version  UNSIGNED NULL
        /// ```
        AlterPicoTierAddBucketStateFields = "alter_pico_tier_add_bucket_state_fields",
    }
}

thread_local! {
    static LOCK: Rc<fiber::Mutex<()>> = Rc::new(fiber::Mutex::new(()));
}

crate::define_rpc_request! {
    fn proc_internal_script(req: Request) -> traft::Result<Response> {
        let script_name = req
            .script_name
            .parse::<InternalScript>()
            .map_err(|e| traft::error::Error::invalid_configuration(format!("bad script name: {e}")))?;
        let lock = LOCK.with(Rc::clone);
        let _guard = lock.lock();
        match script_name {
            InternalScript::AlterPicoTierAddIsDefault => execute_alter_pico_tier_add_is_default(),
            InternalScript::AlterPicoReplicasetAddMasterChangeCounter => execute_alter_pico_replicaset_add_master_change_counter(),
            InternalScript::AlterPicoReplicasetAddBucketStateFields => execute_alter_pico_replicaset_add_bucket_state_fields(),
            InternalScript::AlterPicoTierAddBucketStateFields => execute_alter_pico_tier_add_bucket_state_fields(),
        }
    }

    pub struct Request {
        pub script_name: String,
    }

    pub struct Response {}
}

fn execute_alter_pico_tier_add_is_default() -> traft::Result<Response> {
    let node = traft::node::global()?;

    // Preliminary check before schema change performing.
    let topology_cache_ref = node.topology_cache.get();
    let mut default_tier_name = topology_cache_ref
        .tier_by_name(DEFAULT_TIER)
        .ok()
        .map(|t| &t.name);
    if default_tier_name.is_none() && topology_cache_ref.all_tiers().count() == 1 {
        default_tier_name = Some(&topology_cache_ref.this_tier().name);
    }
    if let Some(name) = default_tier_name {
        tlog!(Info, "successfully determined the default tier: `{name}`");
    } else {
        return Err(traft::error::Error::other(
            "cannot determine the default tier while altering _pico_tier",
        ));
    };
    drop(topology_cache_ref);

    actualize_system_table_format::<Tiers>()?;

    Ok(Response {})
}

fn execute_alter_pico_replicaset_add_master_change_counter() -> traft::Result<Response> {
    actualize_system_table_format::<Replicasets>()?;
    Ok(Response {})
}

fn execute_alter_pico_replicaset_add_bucket_state_fields() -> traft::Result<Response> {
    actualize_system_table_format::<Replicasets>()?;
    Ok(Response {})
}

fn execute_alter_pico_tier_add_bucket_state_fields() -> traft::Result<Response> {
    actualize_system_table_format::<Tiers>()?;
    Ok(Response {})
}

fn actualize_system_table_format<T: SystemTable>() -> traft::Result<()> {
    let node = traft::node::global()?;
    let is_master = !node.is_readonly();

    let table_id = T::TABLE_ID;
    let table_name = T::TABLE_NAME;
    let expected_format = T::format();

    // Idempotency check.
    let res = node
        .storage
        .pico_table
        .get(table_id)
        .expect("storage shouldn't fail");
    let Some(table_def) = res else {
        return Err(traft::error::Error::other(format!(
            "failed looking up info about table {table_name}",
        )));
    };

    if table_def.format == expected_format {
        tlog!(Info, "no need to alter {table_name}; this was already done",);
        return Ok(());
    }

    // We cannot perform all operations in a single transaction
    // because Tarantool prohibits space format check within multi-statement transaction.
    if is_master {
        transaction::transaction(|| ddl_change_format_on_master(table_id, &expected_format))?;
    }

    transaction::transaction(|| -> traft::Result<()> {
        let rename_mapping = traft::op::RenameMappingBuilder::new().build();
        node.storage
            .pico_table
            .update_format(table_id, &expected_format, &rename_mapping)
            .expect("storage shouldn't fail");

        Ok(())
    })?;

    Ok(())
}
