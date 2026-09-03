use crate::config::{PicodataConfig, ReplicationMode, DEFAULT_REPLICATION_MODE};
use crate::traft::{error::Error, Result};
use tarantool::error::Error as TarantoolError;
use tarantool::transaction::transaction_force_async;

pub mod synchro_election_watcher;

/// Make the tarantool system spaces synchronous. Masters apply alters
/// directly to the storage engine and replicas receive it over tarantool
/// replication.
///
/// `_cluster` is deliberately not in the list: there is a bug,
/// see <https://git.picodata.io/core/picodata/-/work_items/3009>.
pub fn make_system_spaces_sync() -> Result<()> {
    let lua = ::tarantool::lua_state();
    // The alters must not become synchronous transactions themselves: as soon
    // as `_space` is flipped to synchronous, every following alter writes into
    // synchronous `_space`, and needs a quorum.
    transaction_force_async(|| -> Result<(), TarantoolError> {
        lua.exec(
            r#"
              local sys = {'_space','_index','_user','_priv','_func',
                           '_schema','_collation','_sequence','_sequence_data',
                           '_truncate','_trigger'}
              for _, name in ipairs(sys) do
                  local s = box.space[name]
                  if s and not s.is_sync then s:alter({is_sync = true}) end
              end
          "#,
        )?;
        Ok(())
    })?;

    crate::tlog!(Info, "tarantool system spaces are now synchronous");
    Ok(())
}

/// Make the tarantool system spaces synchronous if needed
/// (current tier has `replication_mode = sync` param).
pub fn maybe_make_system_spaces_sync() -> Result<()> {
    let (replication_mode, _) = get_this_tier_replication_mode_and_factor()?;
    if replication_mode.is_sync() {
        return make_system_spaces_sync();
    }

    Ok(())
}

/// Get replication mode and factor for current instance's tier.
///
/// Get values from config. Use when cannot get values from
/// topology_cache reliably (no synchronization via `wait_index`).
pub fn get_this_tier_replication_mode_and_factor() -> Result<(ReplicationMode, u8)> {
    get_tier_replication_mode_and_factor(PicodataConfig::get())
}

/// Same as [`get_this_tier_replication_mode_and_factor`], but reads from the provided
/// config instead of the global one.
pub fn get_tier_replication_mode_and_factor(
    config: &PicodataConfig,
) -> Result<(ReplicationMode, u8)> {
    let my_tier_name = config.effective_instance_tier();
    let Some(tiers) = &config.cluster.tier else {
        return Ok((
            DEFAULT_REPLICATION_MODE,
            config.cluster.default_replication_factor(),
        ));
    };
    let (_, tier) = tiers
        .iter()
        .find(|(tier_name, _)| my_tier_name == tier_name)
        .ok_or_else(|| {
            Error::other(format!(
                "failed to get tier info from config: tier name = {my_tier_name}"
            ))
        })?;

    Ok((
        tier.replication_mode,
        tier.replication_factor
            .unwrap_or_else(|| config.cluster.default_replication_factor()),
    ))
}
