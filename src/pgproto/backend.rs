use self::{
    describe::{PortalDescribe, StatementDescribe},
    result::ExecuteResult,
    storage::{Portal, Statement, PG_PORTALS, PG_STATEMENTS},
};
use super::{
    client::{ClientId, ClientParams},
    error::{PgError, PgResult},
    value::PgValue,
};
use crate::config::DYNAMIC_CONFIG;
use crate::sql::router::RouterRuntime;
use crate::tlog;
use crate::{
    pgproto::value::{FieldFormat, RawFormat},
    schema::ADMIN_ID,
};
use bytes::Bytes;
use postgres_types::Oid;
use smol_str::format_smolstr;
use sql::ir::value::Value as SbroadValue;
use sql::PreparedStatement;
use std::sync::atomic::{AtomicU64, Ordering};
use storage::param_oid_to_derived_type;
use tarantool::session::with_su;

mod pgproc;
mod well_known_queries;

pub mod describe;
pub mod result;
pub mod storage;

fn decode_parameter(bytes: Option<&[u8]>, oid: Oid, format: FieldFormat) -> PgResult<SbroadValue> {
    let value = PgValue::decode(bytes, oid, format)?.try_into()?;
    Ok(value)
}

fn decode_parameters(
    params: Vec<Option<Bytes>>,
    param_oids: &[Oid],
    formats: &[FieldFormat],
    statement: &str,
) -> PgResult<Vec<SbroadValue>> {
    if params.len() != param_oids.len() {
        return Err(PgError::ProtocolViolation(format_smolstr!(
            "bind message supplies {} parameters, but prepared statement \"{}\" requires {}",
            params.len(),
            statement,
            param_oids.len(),
        )));
    }

    let iter = params.into_iter().enumerate().zip(param_oids).zip(formats);
    let res: PgResult<Vec<_>> = iter
        .map(|(((param_idx, bytes), oid), format)| {
            decode_parameter(bytes.as_deref(), *oid, *format)
                .map_err(|e| e.cannot_bind_param(param_idx + 1))
        })
        .collect();

    res
}

/// Map any encoding format to per-column or per-parameter format just like pg does it in
/// [exec_bind_message](https://github.com/postgres/postgres/blob/5c7038d70bb9c4d28a80b0a2051f73fafab5af3f/src/backend/tcop/postgres.c#L1840-L1845)
/// or [PortalSetResultFormat](https://github.com/postgres/postgres/blob/5c7038d70bb9c4d28a80b0a2051f73fafab5af3f/src/backend/tcop/pquery.c#L623).
fn prepare_encoding_format(formats: &[RawFormat], n: usize) -> PgResult<Vec<FieldFormat>> {
    if formats.len() == n {
        // format specified for each column
        Ok(formats.iter().copied().map(FieldFormat::from).collect())
    } else if formats.len() == 1 {
        // single format specified, use it for each column
        Ok(vec![FieldFormat::from(formats[0]); n])
    } else if formats.is_empty() {
        // no format specified, use the default for each column
        Ok(vec![FieldFormat::Text; n])
    } else {
        Err(PgError::ProtocolViolation(format_smolstr!(
            "got {} format codes for {} items",
            formats.len(),
            n
        )))
    }
}

pub fn bind(
    id: ClientId,
    stmt_name: String,
    portal_name: String,
    params: Vec<SbroadValue>,
    result_format: Vec<FieldFormat>,
    client_params: &ClientParams,
) -> PgResult<()> {
    let statement_key = storage::Key(id, stmt_name.into());

    let statement: Statement = PG_STATEMENTS
        .with(|storage| {
            storage
                .borrow()
                .get(&statement_key)
                .map(|holder| holder.statement())
        })
        .ok_or_else(|| PgError::other(format!("Couldn't find statement '{}'.", statement_key.1)))?;

    if client_params.statement_invalidation_enabled() {
        statement.ensure_valid()?;
    }

    let Some(sql_options) = DYNAMIC_CONFIG.current_sql_options() else {
        return Err(PgError::other("Not initialized yet"));
    };
    let effective_options = client_params.execution_options().unwrap_or(sql_options);

    let bound_statement = statement
        .prepared_statement()
        .bind(params, effective_options)?;

    let portal_key = storage::Key(id, portal_name.into());
    let portal = Portal::new(
        portal_key.clone(),
        statement.clone(),
        result_format,
        bound_statement,
    )?;
    PG_PORTALS.with(|storage| storage.borrow_mut().put(portal_key, portal))?;

    Ok(())
}

pub fn execute(id: ClientId, name: String, max_rows: i64) -> PgResult<ExecuteResult> {
    let key = storage::Key(id, name.into());

    let router = RouterRuntime::new();

    let portal: Portal = PG_PORTALS
        .with(|storage| storage.borrow_mut().get(&key).cloned())
        .ok_or_else(|| PgError::other(format!("Couldn't find portal '{}'.", key.1)))?;

    let max_rows = if max_rows <= 0 { i64::MAX } else { max_rows };
    portal.execute(&router, max_rows as usize)
}

pub fn parse(id: ClientId, name: String, query: &str, param_oids: Vec<Oid>) -> PgResult<()> {
    let key = storage::Key(id, name.into());

    let router = RouterRuntime::new();

    let param_types: Vec<_> = param_oids
        .iter()
        .map(|oid| param_oid_to_derived_type(*oid))
        .collect::<Result<_, _>>()?;

    let prepared_statement = PreparedStatement::parse(&router, query, &param_types)?;

    let statement = Statement::new(key.clone(), prepared_statement, param_oids)?;
    PG_STATEMENTS.with(|storage| storage.borrow_mut().put(key, statement.into()))?;

    Ok(())
}

pub fn describe_statement(id: ClientId, name: &str) -> PgResult<StatementDescribe> {
    let key = storage::Key(id, name.into());
    let statement: Statement = PG_STATEMENTS
        .with(|storage| storage.borrow().get(&key).map(|holder| holder.statement()))
        .ok_or_else(|| PgError::other(format!("Couldn't find statement '{}'.", key.1)))?;

    Ok(statement.describe().clone())
}

pub fn describe_portal(id: ClientId, name: &str) -> PgResult<PortalDescribe> {
    let key = storage::Key(id, name.into());
    let portal: Portal = PG_PORTALS
        .with(|storage| storage.borrow_mut().get(&key).cloned())
        .ok_or_else(|| PgError::other(format!("Couldn't find portal '{}'.", key.1)))?;

    Ok(portal.describe().clone())
}

pub fn close_statement(id: ClientId, name: &str) {
    // Close can't cause an error in PG.
    let key = storage::Key(id, name.into());
    PG_STATEMENTS.with(|storage| storage.borrow_mut().remove(&key));
}

pub fn close_portal(id: ClientId, name: &str) {
    // Close can't cause an error in PG.
    let key = storage::Key(id, name.into());
    PG_PORTALS.with(|storage| storage.borrow_mut().remove(&key));
}

pub fn close_client_statements(id: ClientId) {
    PG_STATEMENTS.with(|storage| storage.borrow_mut().remove_by_client_id(id))
}

pub fn close_client_portals(id: ClientId) {
    PG_PORTALS.with(|storage| storage.borrow_mut().remove_by_client_id(id))
}

pub fn deallocate_statement(id: ClientId, name: &str) -> PgResult<()> {
    // In contrast to closing, deallocation can cause an error in PG.
    let key = storage::Key(id, name.into());
    PG_STATEMENTS
        .with(|storage| storage.borrow_mut().remove(&key))
        .ok_or_else(|| PgError::other(format!("prepared statement {name} does not exist.")))?;

    Ok(())
}

/// Each postgres client uses its own backend to handle incoming messages.
pub struct Backend {
    /// A unique identificator of a postgres client. It is used as a part of a key in the portal
    /// storage, allowing to store in a single storage portals from many clients.
    client_id: ClientId,

    params: ClientParams,
}

impl Backend {
    pub fn new(params: ClientParams) -> Self {
        /// Generate a unique client id.
        fn unique_id() -> ClientId {
            static ID_COUNTER: AtomicU64 = AtomicU64::new(0);
            ID_COUNTER.fetch_add(1, Ordering::Relaxed)
        }

        Self {
            client_id: unique_id(),
            params,
        }
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    /// Execute a simple query. Handler for a Query message.
    ///
    /// First, it closes an unnamed portal and statement, just like PG does when gets a Query
    /// message. After that the extended pipeline is executed on unnamed portal and statement:
    /// parse + bind + describe + execute and result is returned.
    ///
    /// Note that it closes the unnamed portal and statement even in case of a failure.
    pub fn simple_query(&self, sql: &str) -> PgResult<ExecuteResult> {
        let do_simple_query = || {
            let close_unnamed = || {
                self.close_statement(None);
                self.close_portal(None);
            };

            let simple_query = || {
                close_unnamed();
                self.parse(None, sql, vec![])?;
                self.bind(None, None, vec![], &[], &[FieldFormat::Text as RawFormat])?;
                self.execute(None, -1)
            };

            simple_query().inspect_err(|_| close_unnamed())
        };

        let result = do_simple_query();

        let parsing_failed = match &result {
            Err(e) => e.is_sbroad_parsing_error(),
            Ok(_) => false,
        };

        if parsing_failed {
            // In case of parsing error, we can try to parse and adjust some well known queries
            // from PostgreSQL that our SQL doesn't support. Recognizing these common queries
            // allows us to offer useful features like tab-completion.
            if let Some(query) = well_known_queries::parse(sql) {
                tlog!(Debug, "recognized well known query: {query:?}");
                // sudo helps to avoid read access to system tables is denied error.
                if let Ok(Ok(result)) = with_su(ADMIN_ID, || {
                    self.execute_query(&query.sql(), query.parameters())
                        .inspect_err(|e| {
                            tlog!(Info, "execution of a well know query failed: {e:?}")
                        })
                }) {
                    return Ok(result);
                }
            }
        }

        result
    }

    /// This function is similar to `simple_query`, but the query can be parameterized.
    fn execute_query(&self, sql: &str, params: Vec<SbroadValue>) -> PgResult<ExecuteResult> {
        let close_unnamed = || {
            self.close_statement(None);
            self.close_portal(None);
        };

        let do_execute_query = || {
            close_unnamed();
            self.parse(None, sql, vec![])?;
            let ncolumns = self.describe_statement(None)?.ncolumns();
            // TODO: This looks pretty ugly, so refactoring is needed.
            bind(
                self.client_id,
                "".into(),
                "".into(),
                params,
                vec![FieldFormat::Text; ncolumns],
                &self.params,
            )?;
            self.execute(None, -1)
        };

        do_execute_query().inspect_err(|_| close_unnamed())
    }

    /// Handler for a Describe message.
    ///
    /// Describe a statement.
    pub fn describe_statement(&self, name: Option<&str>) -> PgResult<StatementDescribe> {
        let name = name.unwrap_or_default();
        describe_statement(self.client_id, name)
    }

    /// Handler for a Describe message.
    ///
    /// Describe a portal.
    pub fn describe_portal(&self, name: Option<&str>) -> PgResult<PortalDescribe> {
        let name = name.unwrap_or_default();
        describe_portal(self.client_id, name)
    }

    /// Handler for a Parse message.
    ///
    /// Create a statement from a query and store it in the statement storage with the
    /// given name. In case of a conflict the strategy is the same with PG.
    /// The statement lasts until it is explicitly closed.
    pub fn parse(&self, name: Option<String>, sql: &str, param_oids: Vec<Oid>) -> PgResult<()> {
        let name = name.unwrap_or_default();
        parse(self.client_id, name, sql, param_oids)
    }

    /// Handler for a Bind message.
    ///
    /// Copy the sources statement, create a portal by binding the given parameters and store it
    /// in the portal storage. In case of a conflict the strategy is the same with PG.
    /// The portal lasts until it is explicitly closed.
    pub fn bind(
        &self,
        statement: Option<String>,
        portal: Option<String>,
        params: Vec<Option<Bytes>>,
        params_format: &[RawFormat],
        result_format: &[RawFormat],
    ) -> PgResult<()> {
        let statement = statement.unwrap_or_default();
        let portal = portal.unwrap_or_default();

        let describe = describe_statement(self.client_id, &statement)?;
        let params_format = prepare_encoding_format(params_format, params.len())?;
        let result_format = prepare_encoding_format(result_format, describe.ncolumns())?;
        let params = decode_parameters(params, &describe.param_oids, &params_format, &statement)?;

        bind(
            self.client_id,
            statement,
            portal,
            params,
            result_format,
            &self.params,
        )
    }

    /// Handler for an Execute message.
    ///
    /// Take a portal from the storage and retrieve at most max_rows rows from it. In case of
    /// non-dql queries max_rows is ignored and result with no rows is returned.
    pub fn execute(&self, portal: Option<String>, max_rows: i64) -> PgResult<ExecuteResult> {
        let name = portal.unwrap_or_default();
        execute(self.client_id, name, max_rows)
    }

    /// Handler for a Close message.
    ///
    /// Close a portal. It's not an error to close a non-existent portal.
    pub fn close_portal(&self, name: Option<&str>) {
        let name = name.unwrap_or_default();
        close_portal(self.client_id, name)
    }

    /// Handler for a Close message.
    ///
    /// Close a statement. It's not an error to close a non-existent statement.
    pub fn close_statement(&self, name: Option<&str>) {
        let name = name.unwrap_or_default();
        close_statement(self.client_id, name)
    }

    /// Close all the client's portals. It should be called at the end of the transaction.
    pub fn close_all_portals(&self) {
        close_client_portals(self.client_id)
    }

    fn on_disconnect(&self) {
        close_client_statements(self.client_id);
        close_client_portals(self.client_id);
    }

    pub fn params(&self) -> &ClientParams {
        &self.params
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.on_disconnect()
    }
}
