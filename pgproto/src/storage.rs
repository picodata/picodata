use self::describe::{PortalDescribe, StatementDescribe};
use self::result::ExecuteResult;
use self::value::PgValue;
use crate::client::ClientId;
use crate::entrypoints::PG_ENTRYPOINTS;
use crate::error::PgResult;
use log::warn;
use postgres_types::Oid;
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

    pub fn describe_statement(&self, name: Option<&str>) -> PgResult<StatementDescribe> {
        PG_ENTRYPOINTS.with(|entrypoints| {
            entrypoints
                .borrow()
                .describe_statement(self.client_id, name.unwrap_or(""))
        })
    }

    pub fn describe_portal(&self, name: Option<&str>) -> PgResult<PortalDescribe> {
        PG_ENTRYPOINTS.with(|entrypoints| {
            entrypoints
                .borrow()
                .describe_portal(self.client_id, name.unwrap_or(""))
        })
    }

    pub fn parse(&self, name: Option<&str>, sql: &str, param_oids: &[Oid]) -> PgResult<()> {
        PG_ENTRYPOINTS.with(|entrypoints| {
            entrypoints
                .borrow()
                .parse(self.client_id, name.unwrap_or(""), sql, param_oids)
        })
    }

    pub fn bind(
        &self,
        statement: Option<&str>,
        portal: Option<&str>,
        params: Vec<PgValue>,
    ) -> PgResult<()> {
        PG_ENTRYPOINTS.with(|entrypoints| {
            entrypoints.borrow().bind(
                self.client_id,
                statement.unwrap_or(""),
                portal.unwrap_or(""),
                params,
            )
        })
    }

    pub fn execute(&self, portal: Option<&str>) -> PgResult<ExecuteResult> {
        PG_ENTRYPOINTS.with(|entrypoints| {
            entrypoints
                .borrow()
                .execute(self.client_id, portal.unwrap_or(""))
        })
    }

    pub fn close_portal(&self, portal: Option<&str>) -> PgResult<()> {
        PG_ENTRYPOINTS.with(|entrypoints| {
            entrypoints
                .borrow()
                .close_portal(self.client_id, portal.unwrap_or(""))
        })
    }

    pub fn close_statement(&self, statement: Option<&str>) -> PgResult<()> {
        PG_ENTRYPOINTS.with(|entrypoints| {
            entrypoints
                .borrow()
                .close_statement(self.client_id, statement.unwrap_or(""))
        })
    }

    pub fn close_all_portals(&self) -> PgResult<()> {
        PG_ENTRYPOINTS.with(|entrypoints| entrypoints.borrow().close_client_portals(self.client_id))
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
