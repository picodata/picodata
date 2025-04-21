use self::{
    describe::{PortalDescribe, QueryType, StatementDescribe},
    result::ExecuteResult,
    storage::{Portal, Statement, PG_PORTALS, PG_STATEMENTS},
};
use super::{
    client::{ClientId, ClientParams},
    error::{PgError, PgResult},
    value::PgValue,
};
use crate::{
    pgproto::value::{FieldFormat, RawFormat},
    schema::ADMIN_ID,
    sql::router::RouterRuntime,
};
use crate::{tlog, traft::error::Error};
use bytes::Bytes;
use postgres_types::Oid;
use sbroad::ir::{value::Value as SbroadValue, OptionKind};
use sbroad::{errors::SbroadError, ir::OptionSpec};
use sbroad::{
    executor::{
        engine::{QueryCache, Router, TableVersionMap},
        lru::Cache,
    },
    frontend::Ast,
    ir::{value::Value, Plan as IrPlan},
    utils::MutexLike,
};
use smol_str::ToSmolStr;
use std::{
    iter::zip,
    sync::atomic::{AtomicU64, Ordering},
};
use tarantool::session::with_su;

mod pgproc;
mod well_known_queries;

pub mod describe;
pub mod result;
pub mod storage;

fn decode_parameter_values(
    params: Vec<Option<Bytes>>,
    param_oids: &[Oid],
    formats: &[FieldFormat],
) -> PgResult<Vec<Value>> {
    if params.len() != param_oids.len() {
        return Err(PgError::ProtocolViolation(format!(
            "got {} parameters, {} oids and {} formats",
            params.len(),
            param_oids.len(),
            formats.len()
        )));
    }

    zip(zip(params, param_oids), formats)
        .map(|((bytes, oid), format)| {
            let value = PgValue::decode(bytes.as_ref(), *oid, *format)?;
            value.try_into()
        })
        .collect()
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
        Err(PgError::ProtocolViolation(format!(
            "got {} format codes for {} items",
            formats.len(),
            n
        )))
    }
}

/// Set values from default_options for unspecified options in query_options.
fn apply_default_options(
    query_options: &[OptionSpec],
    default_options: &[OptionSpec],
) -> Vec<OptionSpec> {
    // First, set query options, as they have higher priority.
    let (mut max_steps, mut max_rows) = (None, None);
    for opt in query_options {
        match opt.kind {
            OptionKind::VdbeOpcodeMax => max_steps = Some(opt),
            OptionKind::MotionRowMax => max_rows = Some(opt),
        }
    }

    // Then, apply defaults for unspecified options.
    for opt in default_options {
        match opt.kind {
            OptionKind::VdbeOpcodeMax if max_steps.is_none() => max_steps = Some(opt),
            OptionKind::MotionRowMax if max_rows.is_none() => max_rows = Some(opt),
            _ => {}
        }
    }

    // Keep only Some variants.
    [max_steps, max_rows]
        .into_iter()
        .filter_map(|x| x.cloned())
        .collect()
}

pub fn bind(
    id: ClientId,
    stmt_name: String,
    portal_name: String,
    params: Vec<Value>,
    result_format: Vec<FieldFormat>,
    default_options: Vec<OptionSpec>,
) -> PgResult<()> {
    let key = storage::Key(id, stmt_name.into());
    let statement: Statement = PG_STATEMENTS
        .with(|storage| storage.borrow().get(&key).map(|holder| holder.statement()))
        .ok_or_else(|| PgError::other(format!("Couldn't find statement '{}'.", key.1)))?;

    let mut plan = statement.plan().clone();
    let is_dql = matches!(statement.describe().query_type(), QueryType::Dql);
    if is_dql && !default_options.is_empty() {
        plan.raw_options = apply_default_options(&plan.raw_options, &default_options);
    }

    if !plan.is_empty()
        && !plan.is_ddl()?
        && !plan.is_acl()?
        && !plan.is_plugin()?
        && !plan.is_deallocate()?
        && !plan.is_tcl()?
        && !plan.is_plugin()?
    {
        plan.bind_params(params)?;
        plan.apply_options()?;
        plan.optimize()?;
    }

    let key = storage::Key(id, portal_name.into());
    let portal = Portal::new(key.clone(), statement.clone(), result_format, plan)?;
    PG_PORTALS.with(|storage| storage.borrow_mut().put(key, portal))?;

    Ok(())
}

pub fn execute(id: ClientId, name: String, max_rows: i64) -> PgResult<ExecuteResult> {
    let key = storage::Key(id, name.into());
    let portal: Portal = PG_PORTALS
        .with(|storage| storage.borrow_mut().get(&key).cloned())
        .ok_or_else(|| PgError::other(format!("Couldn't find portal '{}'.", key.1)))?;

    let max_rows = if max_rows <= 0 { i64::MAX } else { max_rows };
    portal.execute(max_rows as usize)
}

pub fn parse(id: ClientId, name: String, query: &str, param_oids: Vec<Oid>) -> PgResult<()> {
    let runtime = RouterRuntime::new().map_err(Error::from)?;
    let mut cache = runtime.cache().lock();

    let key = storage::Key(id, name.into());

    let cache_entry = with_su(ADMIN_ID, || cache.get(&query.to_smolstr()))??;
    if let Some(plan) = cache_entry {
        let statement = Statement::new(key.clone(), plan.clone(), param_oids)?;
        PG_STATEMENTS.with(|storage| storage.borrow_mut().put(key, statement.into()))?;
        return Ok(());
    }

    let plan = with_su(ADMIN_ID, || -> PgResult<IrPlan> {
        let metadata = runtime.metadata().lock();
        let mut plan =
            <RouterRuntime as Router>::ParseTree::transform_into_plan(query, &*metadata)?;
        if runtime.provides_versions() {
            let mut table_version_map = TableVersionMap::with_capacity(plan.relations.tables.len());
            for table in plan.relations.tables.keys() {
                let version = runtime.get_table_version(table.as_str())?;
                table_version_map.insert(table.clone(), version);
            }
            plan.version_map = table_version_map;
        }
        Ok(plan)
    })
    .map_err(PgError::other)??;

    if !plan.is_empty() && !plan.is_tcl()? && !plan.is_ddl()? && !plan.is_acl()? {
        cache.put(query.into(), plan.clone())?;
    }

    let statement = Statement::new(key.clone(), plan, param_oids)?;
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

    /// Execute a simple query. Handler for a Query message.
    ///
    /// First, it closes an unnamed portal and statement, just like PG does when gets a Query
    /// messsage. After that the extended pipeline is executed on unnamed portal and statement:
    /// parse + bind + describe + execute and result is returned.
    ///
    /// Note that it closes the uunamed portal and statement even in case of a failure.
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

        if let Err(PgError::PicodataError(Error::Sbroad(SbroadError::ParsingError(..)))) = result {
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
                vec![],
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
        let params = decode_parameter_values(params, &describe.param_oids, &params_format)?;
        let default_options = self.params.execution_options();

        bind(
            self.client_id,
            statement,
            portal,
            params,
            result_format,
            default_options,
        )
    }

    /// Handler for an Execute message.
    ///
    /// Take a portal from the storage and retrive at most max_rows rows from it. In case of
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
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.on_disconnect()
    }
}
