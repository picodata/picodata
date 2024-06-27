use self::describe::{PortalDescribe, StatementDescribe};
use self::result::ExecuteResult;
use self::storage::{with_portals_mut, Portal, Statement, PG_PORTALS, PG_STATEMENTS};
use super::client::ClientId;
use super::error::{PgError, PgResult};
use crate::pgproto::value::{Format, PgValue, RawFormat};
use crate::schema::ADMIN_ID;
use crate::sql::otm::TracerKind;
use crate::sql::router::RouterRuntime;
use crate::sql::with_tracer;
use crate::traft::error::Error;
use bytes::Bytes;
use opentelemetry::sdk::trace::Tracer;
use opentelemetry::Context;
use postgres_types::Oid;
use sbroad::executor::engine::helpers::normalize_name_for_space_api;
use sbroad::executor::engine::{QueryCache, Router, TableVersionMap};
use sbroad::executor::lru::Cache;
use sbroad::frontend::Ast;
use sbroad::ir::value::Value;
use sbroad::ir::Plan as IrPlan;
use sbroad::otm::{query_id, query_span, OTM_CHAR_LIMIT};
use sbroad::utils::MutexLike;
use smol_str::ToSmolStr;
use std::iter::zip;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};
use tarantool::session::with_su;

mod pgproc;
mod result;
mod storage;

pub mod describe;

fn decode_parameter_values(
    params: Vec<Option<Bytes>>,
    param_oids: &[Oid],
    formats: &[Format],
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
            Ok(value.into())
        })
        .collect()
}

/// Map any encoding format to per-column or per-parameter format just like pg does it in
/// [exec_bind_message](https://github.com/postgres/postgres/blob/5c7038d70bb9c4d28a80b0a2051f73fafab5af3f/src/backend/tcop/postgres.c#L1840-L1845)
/// or [PortalSetResultFormat](https://github.com/postgres/postgres/blob/5c7038d70bb9c4d28a80b0a2051f73fafab5af3f/src/backend/tcop/pquery.c#L623).
fn prepare_encoding_format(formats: &[RawFormat], n: usize) -> PgResult<Vec<Format>> {
    if formats.len() == n {
        // format specified for each column
        formats.iter().map(|i| Format::try_from(*i)).collect()
    } else if formats.len() == 1 {
        // single format specified, use it for each column
        Ok(vec![Format::try_from(formats[0])?; n])
    } else if formats.is_empty() {
        // no format specified, use the default for each column
        Ok(vec![Format::Text; n])
    } else {
        Err(PgError::ProtocolViolation(format!(
            "got {} format codes for {} items",
            formats.len(),
            n
        )))
    }
}

// helper function to get `TracerRef`
fn get_tracer_param(traceable: bool) -> &'static Tracer {
    let kind = TracerKind::from_traceable(traceable);
    kind.get_tracer()
}

pub fn bind(
    client_id: ClientId,
    stmt_name: String,
    portal_name: String,
    params: Vec<Value>,
    result_format: Vec<Format>,
    traceable: bool,
) -> PgResult<()> {
    let key = (client_id, stmt_name.into());
    let Some(statement) = PG_STATEMENTS.with(|storage| storage.borrow().get(&key)) else {
        return Err(PgError::Other(
            format!("Couldn't find statement \'{}\'.", key.1).into(),
        ));
    };
    let mut plan = statement.plan().clone();
    let ctx = with_tracer(Context::new(), TracerKind::from_traceable(traceable));
    let portal = query_span::<PgResult<_>, _>(
        "\"api.router.bind\"",
        statement.id(),
        get_tracer_param(traceable),
        &ctx,
        statement.query_pattern(),
        || {
            if !plan.is_ddl()? && !plan.is_acl()? {
                plan.bind_params(params)?;
                plan.apply_options()?;
                plan.optimize()?;
            }
            Portal::new(plan, statement.clone(), result_format)
        },
    )?;

    PG_PORTALS.with(|storage| {
        storage
            .borrow_mut()
            .put((client_id, portal_name.into()), portal)
    })?;
    Ok(())
}

pub fn execute(
    id: ClientId,
    name: String,
    max_rows: i64,
    traceable: bool,
) -> PgResult<ExecuteResult> {
    let max_rows = if max_rows <= 0 { i64::MAX } else { max_rows };
    let name = Rc::from(name);

    let statement = with_portals_mut((id, Rc::clone(&name)), |portal| {
        // We are cloning Rc here.
        Ok(portal.statement().clone())
    })?;
    with_portals_mut((id, name), |portal| {
        let ctx = with_tracer(Context::new(), TracerKind::from_traceable(traceable));
        query_span::<PgResult<_>, _>(
            "\"api.router.execute\"",
            statement.id(),
            get_tracer_param(traceable),
            &ctx,
            statement.query_pattern(),
            || portal.execute(max_rows as usize),
        )
    })
}

pub fn parse(
    cid: ClientId,
    name: String,
    query: String,
    param_oids: Vec<Oid>,
    traceable: bool,
) -> PgResult<()> {
    let id = query_id(&query);
    // Keep the query patterns for opentelemetry spans short enough.
    let sql = query
        .char_indices()
        .filter_map(|(i, c)| if i <= OTM_CHAR_LIMIT { Some(c) } else { None })
        .collect::<String>();
    let ctx = with_tracer(Context::new(), TracerKind::from_traceable(traceable));
    query_span::<PgResult<()>, _>(
        "\"api.router.parse\"",
        &id.clone(),
        get_tracer_param(traceable),
        &ctx,
        &sql.clone(),
        || {
            let runtime = RouterRuntime::new().map_err(Error::from)?;
            let mut cache = runtime.cache().lock();
            let cache_entry = with_su(ADMIN_ID, || cache.get(&query.to_smolstr()))??;
            if let Some(plan) = cache_entry {
                let statement =
                    Statement::new(id.to_string(), sql.clone(), plan.clone(), param_oids)?;
                PG_STATEMENTS
                    .with(|cache| cache.borrow_mut().put((cid, name.into()), statement))?;
                return Ok(());
            }
            let metadata = &*runtime.metadata().lock();
            let plan = with_su(ADMIN_ID, || -> PgResult<IrPlan> {
                let mut plan =
                    <RouterRuntime as Router>::ParseTree::transform_into_plan(&query, metadata)?;
                if runtime.provides_versions() {
                    let mut table_version_map =
                        TableVersionMap::with_capacity(plan.relations.tables.len());
                    for table in plan.relations.tables.keys() {
                        let normalized = normalize_name_for_space_api(table);
                        let version = runtime.get_table_version(normalized.as_str())?;
                        table_version_map.insert(normalized, version);
                    }
                    plan.version_map = table_version_map;
                }
                Ok(plan)
            })
            .map_err(|e| PgError::Other(e.into()))??;
            if !plan.is_ddl()? && !plan.is_acl()? {
                cache.put(query.into(), plan.clone())?;
            }
            let statement = Statement::new(id.to_string(), sql, plan, param_oids)?;
            PG_STATEMENTS
                .with(|storage| storage.borrow_mut().put((cid, name.into()), statement))?;
            Ok(())
        },
    )
}

pub fn describe_statement(id: ClientId, name: &str) -> PgResult<StatementDescribe> {
    let key = (id, name.into());
    let Some(statement) = PG_STATEMENTS.with(|storage| storage.borrow().get(&key)) else {
        return Err(PgError::Other(
            format!("Couldn't find statement \'{}\'.", key.1).into(),
        ));
    };
    Ok(statement.describe().clone())
}

pub fn describe_portal(id: ClientId, name: &str) -> PgResult<PortalDescribe> {
    with_portals_mut((id, name.into()), |portal| Ok(portal.describe().clone()))
}

pub fn close_statement(id: ClientId, name: &str) {
    // Close can't cause an error in PG.
    PG_STATEMENTS.with(|storage| storage.borrow_mut().remove(&(id, name.into())));
}

pub fn close_portal(id: ClientId, name: &str) {
    // Close can't cause an error in PG.
    PG_PORTALS.with(|storage| storage.borrow_mut().remove(&(id, name.into())));
}

pub fn close_client_statements(id: ClientId) {
    PG_STATEMENTS.with(|storage| storage.borrow_mut().remove_by_client_id(id))
}

pub fn close_client_portals(id: ClientId) {
    PG_PORTALS.with(|storage| storage.borrow_mut().remove_by_client_id(id))
}

/// Each postgres client uses its own backend to handle incoming messages.
pub struct Backend {
    /// A unique identificator of a postgres client. It is used as a part of a key in the portal
    /// storage, allowing to store in a single storage portals from many clients.
    client_id: ClientId,
}

impl Backend {
    pub fn new() -> Self {
        /// Generate a unique client id.
        fn unique_id() -> ClientId {
            static ID_COUNTER: AtomicU32 = AtomicU32::new(0);
            ID_COUNTER.fetch_add(1, Ordering::Relaxed)
        }

        Self {
            client_id: unique_id(),
        }
    }

    /// Execute a simple query. Handler for a Query message.
    ///
    /// First, it closes an unnamed portal and statement, just like PG does when gets a Query
    /// messsage. After that the extended pipeline is executed on unnamed portal and statement:
    /// parse + bind + describe + execute and result is returned.
    ///
    /// Note that it closes the uunamed portal and statement even in case of a failure.
    pub fn simple_query(&self, sql: String) -> PgResult<ExecuteResult> {
        let close_unnamed = || {
            self.close_statement(None);
            self.close_portal(None);
        };

        let simple_query = || {
            close_unnamed();
            self.parse(None, sql, vec![])?;
            self.bind(None, None, vec![], &[], &[Format::Text as RawFormat])?;
            self.execute(None, -1)
        };

        simple_query().map_err(|err| {
            close_unnamed();
            err
        })
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
    pub fn parse(&self, name: Option<String>, sql: String, param_oids: Vec<Oid>) -> PgResult<()> {
        let name = name.unwrap_or_default();
        parse(self.client_id, name, sql, param_oids, false)
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

        bind(
            self.client_id,
            statement,
            portal,
            params,
            result_format,
            true,
        )
    }

    /// Handler for an Execute message.
    ///
    /// Take a portal from the storage and retrive at most max_rows rows from it. In case of
    /// non-dql queries max_rows is ignored and result with no rows is returned.
    pub fn execute(&self, portal: Option<String>, max_rows: i64) -> PgResult<ExecuteResult> {
        let name = portal.unwrap_or_default();
        execute(self.client_id, name, max_rows, true)
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
