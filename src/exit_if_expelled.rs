use crate::has_states;
use crate::instance::Instance;
use crate::instance::InstanceName;
use crate::storage;
use crate::storage::SystemTable as _;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::raft_storage::RaftSpaceAccess;
use crate::Result;
use ::tarantool::space::Space;
use tarantool::tlua;

/// Sets a on_recovery_state tarantool callback which makes an expelled instance
/// fail to start with a clear message.
///
/// After an instance is expelled its row in `_cluster` is deleted on master and
/// that change races to be replicated to the instance itself. If that delete
/// reaches the instance before it shuts down then during a subsequent restart
/// the `box.cfg()` call fails with `ER_UNKNOWN_REPLICA` and panics.
///
/// So we add the `Expelled` state check in the `wal_recovered` trigger, which
/// runs after local space data is restored but before the `_cluster` check.
pub fn set_on_recovery_state_expelled_check() -> Result<()> {
    let lua = ::tarantool::lua_state();
    lua.exec_with(
        "box.ctl.on_recovery_state(...)",
        tlua::Function::new(|state: String| {
            if state != "wal_recovered" {
                return;
            }
            if let Err(e) = exit_if_expelled_on_recovery() {
                tlog!(
                    Warning,
                    "failed checking if instance is expelled on recovery: {e}"
                );
            }
        }),
    )
    .map_err(|e| Error::other(format!("failed setting on_recovery_state trigger: {e}")))?;

    Ok(())
}

/// See [`set_on_recovery_state_expelled_check`].
fn exit_if_expelled_on_recovery() -> Result<()> {
    let Some(raft_state) = Space::find(RaftSpaceAccess::SPACE_RAFT_STATE) else {
        // not initialized yet, cannot be expelled
        return Ok(());
    };

    let Some(tuple) = raft_state.get(&["instance_name"])? else {
        // not initialized yet, cannot be expelled
        return Ok(());
    };

    if Space::find(storage::Instances::TABLE_NAME).is_none() {
        // not initialized yet, cannot be expelled
        return Ok(());
    }

    let name: InstanceName = tuple
        .field(RaftSpaceAccess::FIELD_STATE_VALUE)
        .expect("value column is text")
        .expect("value column is not nullable");

    let Some(instance) = storage::Instances::new().try_get(&name)? else {
        // not initialized yet, cannot be expelled
        return Ok(());
    };

    exit_if_expelled(&instance);

    Ok(())
}

/// Shuts the instance down if `me` (the current instance) is expelled.
pub fn exit_if_expelled(me: &Instance) {
    if has_states!(me, Expelled -> *) {
        tlog!(Critical, "current instance is expelled from the cluster");
        tlog!(Info, "expelled, shutting down");
        crate::tarantool::exit(1);
    }
}

#[cfg(feature = "error_injection")]
pub fn am_expelled_and_not_in_cluster(me: &Instance) -> bool {
    const SPACE_CLUSTER_INDEX_UUID: ::tarantool::index::IndexId = 1;

    if !has_states!(me, Expelled -> *) {
        return false;
    }

    let sys_cluster_uuid = storage::index_by_ids_unchecked(
        ::tarantool::space::SystemSpace::Cluster as _,
        SPACE_CLUSTER_INDEX_UUID,
    );

    let not_in_cluster = sys_cluster_uuid
        .get(&[&me.uuid])
        .expect("reading from `_cluster` shouldn't fail")
        .is_none();

    not_in_cluster
}
