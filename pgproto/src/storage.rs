use self::result::ExecuteResult;
use crate::client::ClientId;
use crate::entrypoints::PG_ENTRYPOINTS;
use crate::error::PgResult;
use log::warn;
use std::sync::atomic::{AtomicU32, Ordering};

pub mod describe;
pub mod result;
pub mod value;

fn unique_id() -> ClientId {
    static ID_COUNTER: AtomicU32 = AtomicU32::new(0);
    ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// It allows to interact with the real storage in sbroad
/// and cleanups all the created portals and statements when it's dropped.
pub struct StorageManager {
    /// Unique id of a PG client.
    /// Every portal and statement is tagged by this id in the sbroad storage,
    /// so they all can be found and deleted using this id.
    /// Since the id is unique the statements and portals are isolated between users.
    client_id: ClientId,
}

impl StorageManager {
    pub fn new() -> Self {
        Self {
            client_id: unique_id(),
        }
    }

    /// Handler for a Query message. See Entrypoints::simple_query for the details.
    pub fn simple_query(&self, sql: &str) -> PgResult<ExecuteResult> {
        PG_ENTRYPOINTS.with(|entrypoints| entrypoints.borrow().simple_query(self.client_id, sql))
    }

    fn on_disconnect(&self) -> PgResult<()> {
        // Close all the statements with its portals.
        PG_ENTRYPOINTS
            .with(|entrypoints| entrypoints.borrow().close_client_statements(self.client_id))
    }
}

impl Drop for StorageManager {
    fn drop(&mut self) {
        match self.on_disconnect() {
            Ok(_) => {}
            Err(err) => warn!(
                "failed to close user {} statements and portals: {:?}",
                self.client_id, err
            ),
        }
    }
}
