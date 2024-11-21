//! Tarantool cartridge engine module.

pub mod config;
pub mod router;
pub mod storage;

use sbroad::errors::SbroadError;
use sbroad::{error, utils::MutexLike};
use smol_str::{format_smolstr, ToSmolStr};
use tarantool::tlua::LuaFunction;

/// Cartridge cluster configuration provider.
pub trait ConfigurationProvider: Sized {
    type Configuration;

    /// Return a cached cluster configuration from the Rust memory.
    fn cached_config(&self) -> &impl MutexLike<Self::Configuration>;

    /// Clear the cached cluster configuration in the Rust memory.
    ///
    /// # Errors
    /// - Failed to clear the cached configuration.
    fn clear_config(&self) -> Result<(), SbroadError>;

    /// Check if the cached cluster configuration is empty.
    ///
    /// # Errors
    /// - Failed to get the cached configuration.
    fn is_config_empty(&self) -> Result<bool, SbroadError>;

    /// Retrieve cluster configuration from the Lua memory.
    ///
    /// If the configuration is already cached, return None,
    /// otherwise return Some(config).
    ///
    /// # Errors
    /// - Internal error.
    fn retrieve_config(&self) -> Result<Option<Self::Configuration>, SbroadError>;

    /// Update cached cluster configuration.
    ///
    /// # Errors
    /// - Failed to update the configuration.
    fn update_config(&self, metadata: Self::Configuration) -> Result<(), SbroadError>;
}

fn bucket_count() -> Result<u64, SbroadError> {
    let lua = tarantool::lua_state();

    let bucket_count_fn: LuaFunction<_> =
        match lua.eval("return require('vshard').router.bucket_count") {
            Ok(v) => v,
            Err(e) => {
                error!(Option::from("set_bucket_count"), &format!("{e:?}"));
                return Err(SbroadError::LuaError(format_smolstr!(
                    "Failed lua function load: {e}"
                )));
            }
        };

    let bucket_count: u64 = match bucket_count_fn.call() {
        Ok(r) => r,
        Err(e) => {
            error!(Option::from("set_bucket_count"), &format!("{e:?}"));
            return Err(SbroadError::LuaError(e.to_smolstr()));
        }
    };

    Ok(bucket_count)
}
