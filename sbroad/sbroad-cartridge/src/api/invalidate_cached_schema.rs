use anyhow::Context;

use crate::{
    api::{COORDINATOR_ENGINE, SEGMENT_ENGINE},
    cartridge::ConfigurationProvider,
    utils::{wrap_proc_result, ProcResult},
};
use sbroad::executor::engine::QueryCache;

/// Flush cached configuration in the Rust memory of the coordinator runtime.
/// This function should be invoked in the Lua cartridge application with `apply_config()`.
#[tarantool::proc]
fn invalidate_coordinator_cache() -> ProcResult<()> {
    wrap_proc_result(
        "invalidate_coordinator_cache".into(),
        COORDINATOR_ENGINE.with(|runtime| {
            let runtime = runtime.lock();
            runtime.clear_config().context("clear config")?;
            runtime.clear_cache().context("clear IR cache on router")
        }),
    )
}

/// Flush cached configuration in the Rust memory of the segment runtime.
/// This function should be invoked in the Lua cartridge application with `apply_config()`.
#[tarantool::proc]
fn invalidate_segment_cache() -> ProcResult<()> {
    wrap_proc_result(
        "invalidate_segment_cache".into(),
        SEGMENT_ENGINE.with(|runtime| {
            let runtime = runtime.lock();
            runtime.clear_config().context("clear config")
        }),
    )
}
