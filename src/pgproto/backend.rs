use self::{
    describe::{PortalDescribe, QueryType, StatementDescribe},
    result::ExecuteResult,
    storage::{ClientStorage, Key, Portal, Statement},
};
use super::{
    client::ClientParams,
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
use storage::param_oid_to_derived_type;
use tarantool::session::with_su;

mod well_known_queries;

pub mod describe;
pub mod port_rows;
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

/// Return true if the query has only semicolons and whitespace, so that
/// the SQL parser makes an empty query from it. The whitespace is the same
/// as in the SQL grammar: " ", "\t", "\n" and "\r\n". Other queries, for
/// example with "\r" alone, go to the parser, which rejects them.
fn is_empty_query(sql: &str) -> bool {
    sql.split("\r\n").all(|part| {
        part.bytes()
            .all(|b| matches!(b, b';' | b' ' | b'\t' | b'\n'))
    })
}

/// Each postgres client uses its own backend to handle incoming messages.
pub struct Backend {
    storage: ClientStorage,
    params: ClientParams,
}

impl Backend {
    pub fn new(params: ClientParams) -> Self {
        Self {
            storage: ClientStorage::new(),
            params,
        }
    }

    /// Execute a simple query. Handler for a Query message.
    ///
    /// First, it closes an unnamed portal and statement, just like PG does when gets a Query
    /// message. After that the extended pipeline is executed on unnamed portal and statement:
    /// parse + bind + describe + execute and result is returned.
    ///
    /// Note that it closes the unnamed portal and statement even in case of a failure.
    pub fn simple_query(&mut self, sql: &str) -> PgResult<ExecuteResult> {
        self.close_unnamed();

        // An empty query gets EmptyQueryResponse. Send it without the parse,
        // bind and dispatch of the full pipeline.
        if is_empty_query(sql) {
            return Ok(ExecuteResult::Empty);
        }

        let result = self
            .simple_query_unnamed(sql)
            .inspect_err(|_| self.close_unnamed());

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

    fn simple_query_unnamed(&mut self, sql: &str) -> PgResult<ExecuteResult> {
        let statement = self.parse(None, sql, vec![])?;
        let portal = self.bind_encoded(
            statement,
            "",
            String::new(),
            vec![],
            &[],
            &[FieldFormat::Text as RawFormat],
        )?;
        self.execute_portal(portal, -1)
    }

    /// This function is similar to `simple_query`, but the query can be parameterized.
    fn execute_query(&mut self, sql: &str, params: Vec<SbroadValue>) -> PgResult<ExecuteResult> {
        self.close_unnamed();
        self.execute_query_unnamed(sql, params)
            .inspect_err(|_| self.close_unnamed())
    }

    fn execute_query_unnamed(
        &mut self,
        sql: &str,
        params: Vec<SbroadValue>,
    ) -> PgResult<ExecuteResult> {
        let statement = self.parse(None, sql, vec![])?;
        let ncolumns = statement.describe().ncolumns();
        let portal = self.bind_statement(
            statement,
            String::new(),
            params,
            vec![FieldFormat::Text; ncolumns],
        )?;
        self.execute_portal(portal, -1)
    }

    fn close_unnamed(&mut self) {
        self.close_statement(None);
        self.close_portal(None);
    }

    /// Find a statement by name.
    pub fn statement(&self, name: Option<&str>) -> PgResult<Statement> {
        self.storage.statement(name.unwrap_or_default())
    }

    /// Handler for a Describe message.
    ///
    /// Describe a statement.
    pub fn describe_statement(&self, name: Option<&str>) -> PgResult<StatementDescribe> {
        Ok(self.statement(name)?.describe().clone())
    }

    /// Handler for a Describe message.
    ///
    /// Describe a portal.
    pub fn describe_portal(&self, name: Option<&str>) -> PgResult<PortalDescribe> {
        let portal = self.storage.portal(name.unwrap_or_default())?;
        Ok(portal.describe().clone())
    }

    /// Handler for a Parse message.
    ///
    /// Create a statement from a query and store it in the statement storage with the
    /// given name. In case of a conflict the strategy is the same with PG.
    /// The statement lasts until it is explicitly closed.
    pub fn parse(
        &mut self,
        name: Option<String>,
        sql: &str,
        param_oids: Vec<Oid>,
    ) -> PgResult<Statement> {
        let router = RouterRuntime::new();

        let param_types: Vec<_> = param_oids
            .iter()
            .map(|oid| param_oid_to_derived_type(*oid))
            .collect::<Result<_, _>>()?;

        let prepared_statement = PreparedStatement::parse(&router, sql, &param_types)?;

        let name = name.unwrap_or_default().into();
        let statement = Statement::new(Key::clone(&name), prepared_statement, param_oids)?;
        self.storage.put_statement(name, statement.clone())?;
        Ok(statement)
    }

    /// Handler for a Bind message.
    ///
    /// Copy the sources statement, create a portal by binding the given parameters and store it
    /// in the portal storage. In case of a conflict the strategy is the same with PG.
    /// The portal lasts until it is explicitly closed.
    pub fn bind(
        &mut self,
        statement: Option<String>,
        portal: Option<String>,
        params: Vec<Option<Bytes>>,
        params_format: &[RawFormat],
        result_format: &[RawFormat],
    ) -> PgResult<Portal> {
        let statement_name = statement.unwrap_or_default();
        let statement = self.storage.statement(&statement_name)?;
        self.bind_encoded(
            statement,
            &statement_name,
            portal.unwrap_or_default(),
            params,
            params_format,
            result_format,
        )
    }

    fn bind_encoded(
        &mut self,
        statement: Statement,
        statement_name: &str,
        portal: String,
        params: Vec<Option<Bytes>>,
        params_format: &[RawFormat],
        result_format: &[RawFormat],
    ) -> PgResult<Portal> {
        let describe = statement.describe();
        let params_format = prepare_encoding_format(params_format, params.len())?;
        let result_format = prepare_encoding_format(result_format, describe.ncolumns())?;
        let params =
            decode_parameters(params, &describe.param_oids, &params_format, statement_name)?;

        self.bind_statement(statement, portal, params, result_format)
    }

    fn bind_statement(
        &mut self,
        statement: Statement,
        portal: String,
        params: Vec<SbroadValue>,
        result_format: Vec<FieldFormat>,
    ) -> PgResult<Portal> {
        if self.params.statement_invalidation_enabled() {
            statement.ensure_valid()?;
        }

        let Some(sql_options) = DYNAMIC_CONFIG.current_sql_options() else {
            return Err(PgError::other("Not initialized yet"));
        };
        let effective_options = self.params.execution_options().unwrap_or(sql_options);

        let bound_statement = statement
            .prepared_statement()
            .bind(params, effective_options)?;

        let name = portal.into();
        let portal = Portal::new(Key::clone(&name), statement, result_format, bound_statement)?;
        self.storage.put_portal(name, portal.clone())?;
        Ok(portal)
    }

    /// Handler for an Execute message.
    ///
    /// Take a portal from the storage and retrieve at most max_rows rows from it. In case of
    /// non-dql queries max_rows is ignored and result with no rows is returned.
    pub fn execute(&mut self, portal: Option<String>, max_rows: i64) -> PgResult<ExecuteResult> {
        let portal = self.storage.portal(portal.as_deref().unwrap_or_default())?;
        self.execute_portal(portal, max_rows)
    }

    fn execute_portal(&mut self, portal: Portal, max_rows: i64) -> PgResult<ExecuteResult> {
        let router = RouterRuntime::new();
        let max_rows = if max_rows <= 0 { i64::MAX } else { max_rows };
        let result = portal.execute(&router, max_rows as usize)?;
        // Not inside the portal: DEALLOCATE can close this portal, and the
        // portal must not get a mutable borrow of the storage that holds it.
        if let QueryType::Deallocate = portal.describe().query_type() {
            self.deallocate(portal.statement())?;
        }
        Ok(result)
    }

    fn deallocate(&mut self, statement: &Statement) -> PgResult<()> {
        let plan = statement.prepared_statement().as_plan();
        let deallocate = plan.get_deallocate_node(plan.get_top()?)?;
        match &deallocate.name {
            Some(name) => self.storage.deallocate(name),
            None => {
                self.storage.deallocate_all();
                Ok(())
            }
        }
    }

    /// Handler for a Close message.
    ///
    /// Close a portal. It's not an error to close a non-existent portal.
    pub fn close_portal(&mut self, name: Option<&str>) {
        self.storage.close_portal(name.unwrap_or_default())
    }

    /// Handler for a Close message.
    ///
    /// Close a statement. It's not an error to close a non-existent statement.
    pub fn close_statement(&mut self, name: Option<&str>) {
        self.storage.close_statement(name.unwrap_or_default());
    }

    /// Close all the client's portals. It should be called at the end of the transaction.
    pub fn close_all_portals(&mut self) {
        self.storage.close_all_portals()
    }

    pub fn params(&self) -> &ClientParams {
        &self.params
    }
}

#[cfg(test)]
mod tests {
    use super::is_empty_query;

    // Check that only the whitespace of the SQL grammar and semicolons make
    // an empty query.
    #[test]
    fn empty_query() {
        for sql in ["", "  ", ";", " ; ", ";;", "\n;\t", "\r\n;\r\n"] {
            assert!(is_empty_query(sql), "{sql:?}");
        }
        for sql in [
            " ;\r", "\r;", "\x0b;", "\x0c", "-- c", "/* c */;", "x", "; x",
        ] {
            assert!(!is_empty_query(sql), "{sql:?}");
        }
    }
}
