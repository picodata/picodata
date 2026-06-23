use crate::traft;
use tarantool::error::Error as TarantoolError;
use tarantool::transaction::transaction_force_async;

/// Make the tarantool system spaces synchronous. Masters apply alters
/// directly to the storage engine and replicas receive it over tarantool
/// replication.
///
/// `_cluster` is deliberately not in the list: there is a bug,
/// see <https://git.picodata.io/core/picodata/-/work_items/3009>.
pub fn make_system_spaces_sync() -> traft::Result<()> {
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
pub fn maybe_make_system_spaces_sync() -> traft::Result<()> {
    let (replication_mode, _) = crate::rpc::replication::get_tier_replication_mode_and_factor(
        crate::config::PicodataConfig::get(),
    )?;
    if replication_mode.is_sync() {
        return make_system_spaces_sync();
    }

    Ok(())
}
