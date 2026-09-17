use std::collections::HashMap;
use std::future::Future;
use std::panic::Location;
use std::str::FromStr as _;

use serde::de::DeserializeOwned;
use tokio_postgres as pg;

use tokio_postgres::Client;

use crate::cli::export::catalog::model::{RawIndex, RawTable, RawTier};
use crate::cli::export::{ExportError, ExportErrorKind};
use crate::column_name;
use crate::config::{AlterSystemParameters, ReplicationMode, DEFAULT_REPLICATION_MODE};
use crate::instance::{Instance, StateVariant};
use crate::schema::{IndexDef, TableDef};
use crate::storage::{
    DbConfig, Indexes, Instances, Properties, PropertyName, SystemTable as _, Tiers,
    SPACE_ID_INTERNAL_MAX,
};
use crate::tier::Tier;

use crate::catalog::pico_table::PicoTable;

/// Not an `async fn` because `#[track_caller]` works differently for async fn. It doesn't identify the true caller,
/// but a function creating the future so the location of the caller is taken in synchronous body,
/// before the future is created. For details see nightly feature async_fn_track_caller.
#[track_caller]
fn query<'a>(
    client: &'a Client,
    sql: &'a str,
    subject: &'static str,
) -> impl Future<Output = Result<Vec<pg::Row>, ExportError>> + 'a {
    let location = Location::caller();
    async move {
        client.query(sql, &[]).await.map_err(|source| ExportError {
            kind: ExportErrorKind::Query { subject, source },
            location,
        })
    }
}

/// Expelled instances may be still in the `_pico_instance`, so they are skipped:
/// otherwise an instance that is expelled would report an upgrade that is not happening.
pub(super) async fn fetch_instance_versions(client: &Client) -> Result<Vec<String>, ExportError> {
    const VERSION: &str = column_name!(Instance, picodata_version);
    // `current_state` is a `[variant, incarnation]` array.
    let query_text = format!(
        "SELECT DISTINCT {VERSION} FROM {table} \
         WHERE {state}[1]::text <> '{expelled}' ORDER BY {VERSION}",
        table = Instances::TABLE_NAME,
        state = column_name!(Instance, current_state),
        expelled = StateVariant::Expelled.as_str(),
    );

    let rows = query(client, &query_text, "instance versions").await?;
    rows.into_iter().map(|row| read(&row, VERSION)).collect()
}

/// The structure of the system tables read below is primarily decided by the catalog.
/// This is because the binary can support multiple versions of the catalog and upgrades between them.
pub(super) async fn fetch_catalog_version(client: &Client) -> Result<Option<String>, ExportError> {
    // TODO: `_pico_property` creates its own format based on string literals.
    // There is no structure that would allow the use of `column_name!` here.
    const KEY: &str = "key";
    const VALUE: &str = "value";

    let query_text = format!(
        "SELECT \"{VALUE}\" FROM {table} WHERE \"{KEY}\" = '{property}'",
        table = Properties::TABLE_NAME,
        property = PropertyName::SystemCatalogVersion.as_str(),
    );

    let rows = query(client, &query_text, "catalog version").await?;
    // `_pico_property.value` is typed `ANY`, so pgproto sends it as JSON.
    rows.first().map(|row| read_json(row, VALUE)).transpose()
}

pub(super) async fn fetch_tiers(client: &Client) -> Result<Vec<RawTier>, ExportError> {
    let query_text = format!(
        "SELECT {name}, {factor}, {buckets} FROM {table} ORDER BY {name}",
        name = column_name!(Tier, name),
        factor = column_name!(Tier, replication_factor),
        buckets = column_name!(Tier, bucket_count),
        table = Tiers::TABLE_NAME,
    );

    let (rows, replication_modes) = tokio::try_join!(
        query(client, &query_text, "tiers"),
        fetch_replication_modes(client),
    )?;

    rows.into_iter()
        .map(|row| {
            let name: String = read(&row, column_name!(Tier, name))?;
            Ok(RawTier {
                replication_factor: read(&row, column_name!(Tier, replication_factor))?,
                bucket_count: read(&row, column_name!(Tier, bucket_count))?,
                replication_mode: replication_modes
                    .get(&name)
                    .copied()
                    .unwrap_or(DEFAULT_REPLICATION_MODE),
                name,
            })
        })
        .collect()
}

async fn fetch_replication_modes(
    client: &Client,
) -> Result<HashMap<String, ReplicationMode>, ExportError> {
    // TODO: `_pico_db_config` creates its own format based on string literals.
    // There is no structure that would allow the use of `column_name!` here.
    const SCOPE: &str = "scope";
    const VALUE: &str = "value";
    let query_text = format!(
        "SELECT \"{SCOPE}\", \"{VALUE}\" FROM {table} WHERE \"key\" = '{parameter}'",
        table = DbConfig::TABLE_NAME,
        parameter = column_name!(AlterSystemParameters, replication_mode),
    );

    let rows = query(client, &query_text, "replication modes").await?;
    rows.into_iter()
        .map(|row| {
            let mode: String = read_json(&row, VALUE)?;
            let mode = ReplicationMode::from_str(&mode).unwrap_or(DEFAULT_REPLICATION_MODE);
            Ok((read(&row, SCOPE)?, mode))
        })
        .collect()
}

/// `SELECT *` is because the exporter and the cluster may be of different catalogs:
/// `opts` only arrived in 26.1.1, and naming it explicitly makes the query fail
/// on a cluster that predates it, where it degrades into an empty list instead.
/// We access columns by their name without using positions, so extra fields are accepted.
pub(super) async fn fetch_tables(client: &Client) -> Result<Vec<RawTable>, ExportError> {
    let query_text = format!(
        "SELECT * FROM {table} WHERE {id} > {SPACE_ID_INTERNAL_MAX} ORDER BY {id}",
        table = PicoTable::TABLE_NAME,
        id = column_name!(TableDef, id),
    );

    let rows = query(client, &query_text, "tables").await?;

    // `opts` arrived with the 26.1.1 catalog.
    let has_options = rows.first().is_some_and(|row| {
        row.columns()
            .iter()
            .any(|it| it.name() == column_name!(TableDef, opts))
    });

    rows.into_iter()
        .map(|row| {
            Ok(RawTable {
                id: read(&row, column_name!(TableDef, id))?,
                name: read(&row, column_name!(TableDef, name))?,
                distribution: read_json(&row, column_name!(TableDef, distribution))?,
                format: read_json(&row, column_name!(TableDef, format))?,
                engine: read(&row, column_name!(TableDef, engine))?,
                description: read(&row, column_name!(TableDef, description))?,
                options: if has_options {
                    read_json(&row, column_name!(TableDef, opts))?
                } else {
                    Vec::default()
                },
            })
        })
        .collect()
}

// The implicit `bucket_id` index of a sharded table is never written to
// `_pico_index`, so there is nothing to filter out.
pub(super) async fn fetch_indexes(client: &Client) -> Result<Vec<RawIndex>, ExportError> {
    const INDEX_TYPE_COLUMN: &str = "type";
    let query_text = format!(
        "SELECT {table_id}, {id}, {name}, {ty}, {opts}, {parts} \
         FROM {table} WHERE {table_id} > {SPACE_ID_INTERNAL_MAX} ORDER BY {table_id}, {id}",
        table = Indexes::TABLE_NAME,
        table_id = column_name!(IndexDef, table_id),
        id = column_name!(IndexDef, id),
        name = column_name!(IndexDef, name),
        ty = INDEX_TYPE_COLUMN,
        opts = column_name!(IndexDef, opts),
        parts = column_name!(IndexDef, parts),
    );

    let rows = query(client, &query_text, "indexes").await?;
    rows.into_iter()
        .map(|row| {
            Ok(RawIndex {
                table_id: read(&row, column_name!(IndexDef, table_id))?,
                id: read(&row, column_name!(IndexDef, id))?,
                name: read(&row, column_name!(IndexDef, name))?,
                ty: read(&row, INDEX_TYPE_COLUMN)?,
                options: read_json(&row, column_name!(IndexDef, opts))?,
                parts: read_json(&row, column_name!(IndexDef, parts))?,
            })
        })
        .collect()
}

/// `#[track_caller]` does not reach into the `map_err` closure,
/// so the location of the caller is taken before it.
#[track_caller]
fn read<'row, T: pg::types::FromSql<'row>>(
    row: &'row pg::Row,
    column: &'static str,
) -> Result<T, ExportError> {
    let location = Location::caller();
    row.try_get(column).map_err(|source| ExportError {
        kind: ExportErrorKind::Decode { column, source },
        location,
    })
}

/// Arrays and maps arrive as JSON: sbroad can't cast them to text yet.
#[track_caller]
fn read_json<T: DeserializeOwned>(row: &pg::Row, column: &'static str) -> Result<T, ExportError> {
    read(row, column).map(|pg::types::Json(value)| value)
}
