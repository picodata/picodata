use tarantool::fiber::Mutex;

use crate::cartridge::router::RouterRuntime;
use crate::cartridge::storage::StorageRuntime;

#[derive(Default)]
pub struct AsyncCommands {
    pub invalidate_router_cache: bool,
    pub invalidate_segment_cache: bool,
}

thread_local!(pub static COORDINATOR_ENGINE: Mutex<RouterRuntime> = Mutex::new(RouterRuntime::new().unwrap()));
thread_local!(pub static SEGMENT_ENGINE: Mutex<StorageRuntime> = Mutex::new(StorageRuntime::new().unwrap()));
thread_local!(pub static ASYNC_COMMANDS: Mutex<AsyncCommands> = Mutex::new(AsyncCommands::default()));

pub mod calculate_bucket_id;
pub mod exec_query;
mod helper;
pub mod invalidate_cached_schema;
