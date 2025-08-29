use crate::storage::schema::ddl_change_format_on_master;
use crate::storage::{SystemTable, Tiers};
use crate::tier::{Tier, DEFAULT_TIER};
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
            ("proc_name", "proc_internal_script"),
            ("exec_script", "alter_pico_tier_add_is_default"),
            ("sql", "UPDATE _pico_tier SET is_default = true WHERE 1 in (SELECT count(*) FROM _pico_tier)"),
            ("sql", "UPDATE _pico_tier SET is_default = CASE WHEN name = 'default' THEN true ELSE false END"),
        ],
    ),
];

tarantool::define_str_enum! {
    /// List of internal scripts.
    pub enum InternalScript {
        /// Schema upgrade operation `ALTER TABLE _pico_tier ADD is_default bool`.
        AlterPicoTierAddIsDefault = "alter_pico_tier_add_is_default",
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
        }
    }

    pub struct Request {
        pub script_name: String,
    }

    pub struct Response {}
}

fn execute_alter_pico_tier_add_is_default() -> traft::Result<Response> {
    let node = traft::node::global()?;
    let is_master = !node.is_readonly();

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

    // Idempotency check.
    let table_def = node
        .storage
        .pico_table
        .get(Tiers::TABLE_ID)
        .expect("storage shouldn't fail")
        .expect("_pico_tier should exist");
    if table_def.format == Tier::format() {
        tlog!(Info, "no need to alter _pico_tier; this was already done");
        return Ok(Response {});
    }

    // We cannot perform all operations in a single transaction
    // because Tarantool prohibits space format check within multi-statement transaction.
    if is_master {
        transaction::transaction(|| ddl_change_format_on_master(Tiers::TABLE_ID, &Tier::format()))?;
    }

    transaction::transaction(|| {
        let rename_mapping = traft::op::RenameMappingBuilder::new().build();
        node.storage
            .pico_table
            .update_format(Tiers::TABLE_ID, &Tier::format(), &rename_mapping)
            .expect("storage shouldn't fail");

        Ok::<(), traft::error::Error>(())
    })?;

    Ok(Response {})
}
