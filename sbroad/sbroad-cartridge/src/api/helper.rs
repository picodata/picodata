use anyhow::Context;

use tarantool::fiber::Mutex;

use std::thread::LocalKey;

use crate::cartridge::ConfigurationProvider;

pub fn load_config<Runtime>(engine: &'static LocalKey<Mutex<Runtime>>) -> anyhow::Result<()>
where
    Runtime: ConfigurationProvider,
{
    // Tarantool can yield in the middle of a current closure,
    // so we can hold only an immutable reference to the engine.
    let config = (*engine).with(|engine| {
        let runtime = engine.lock();
        runtime.retrieve_config().context("retrieve config")
    })?;

    // Tarantool never yields here, so it is possible to hold
    // a mutable reference to the engine.
    if let Some(config) = config {
        (*engine).with(|runtime| {
            let runtime = runtime.lock();
            runtime.update_config(config).context("update config")
        })?;
    }
    Ok(())
}
