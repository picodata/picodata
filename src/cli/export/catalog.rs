mod fetch;
pub(super) mod model;

use tokio_postgres::Client;

use crate::cli::export::ExportError;
use model::{RawIndex, RawTable, RawTier};

#[derive(Debug, Clone)]
pub(super) struct Catalog {
    /// More than one means an upgrade is in progress.
    pub(super) instance_versions: Vec<String>,
    pub(super) catalog_version: Option<String>,
    pub(super) tiers: Vec<RawTier>,
    pub(super) tables: Vec<RawTable>,
    pub(super) indexes: Vec<RawIndex>,
}

impl Catalog {
    pub(super) async fn fetch(client: &Client) -> Result<Self, ExportError> {
        // TODO: the queries below do not share a consistent snapshot of the server catalog.
        // A DDL that commits between them makes the catalog inconsistent: e g a table
        // without its indexes, or index parts naming a column the table format does not have yet.
        // One way to fix this may be to read `global_schema_version` before and after the queries
        // and fail (or retry) if it changed. But that introduces a liveness problem: for a cluster
        // whose schema changes continuously this check needs to be skippable. "operable" column in
        // tables and indexes doesn't resolve the issue because other operations like truncate or
        // sql backup command change it potentially reintroducing the inconsistency.
        let (instance_versions, catalog_version, tiers, tables, indexes) = tokio::try_join!(
            fetch::fetch_instance_versions(client),
            fetch::fetch_catalog_version(client),
            fetch::fetch_tiers(client),
            fetch::fetch_tables(client),
            fetch::fetch_indexes(client),
        )?;

        Ok(Self {
            instance_versions,
            catalog_version,
            tiers,
            tables,
            indexes,
        })
    }
}
