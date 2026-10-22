use super::{
    describe::{Describe, PortalDescribe, QueryType, StatementDescribe},
    port_rows::{PinnedPort, RowSource},
    result::{ExecuteResult, Rows},
};
use crate::config::observer::AtomicObserver;
use crate::sql::router::{get_table_version, RouterRuntime};
use crate::{audit, schema::ADMIN_ID};
use crate::{
    pgproto::{
        error::{PedanticError, PgError, PgErrorCode, PgResult},
        value::FieldFormat,
    },
    tlog,
    traft::node,
};
use pgwire::api::results::FieldInfo;
use postgres_types::{Oid, Type as PgType};
use prometheus::IntCounter;
use smol_str::{format_smolstr, SmolStr};
use sql::ir::types::{DerivedType, NestedType, UnrestrictedType as SbroadType};
use sql_protocol::iterators::ExplainIter;
use std::{
    cell::{Cell, RefCell},
    collections::{btree_map::Entry, BTreeMap},
    io::Cursor,
    rc::Rc,
    sync::{Arc, LazyLock},
    thread::LocalKey,
};
use tarantool::session::with_su;

// Object that stores information specific to the portal or statement storage.
struct StorageContext {
    value_kind: &'static str,
    capacity_parameter: &'static str,
    capacity: AtomicObserver<usize>,
    dublicate_key_error_code: PgErrorCode,
    // The capacity limits all clients together, so their storages share the count.
    total: &'static LocalKey<Cell<usize>>,
}

thread_local! {
    static STATEMENTS_TOTAL: Cell<usize> = const { Cell::new(0) };
    static PORTALS_TOTAL: Cell<usize> = const { Cell::new(0) };
}

impl StorageContext {
    fn portals() -> StorageContext {
        Self {
            value_kind: "Portal",
            capacity_parameter: crate::system_parameter_name!(pg_portal_max),
            capacity: node::global()
                .unwrap()
                .storage
                .db_config
                .observe_pg_portal_max(),
            dublicate_key_error_code: PgErrorCode::DuplicateCursor,
            total: &PORTALS_TOTAL,
        }
    }

    fn statements() -> StorageContext {
        Self {
            value_kind: "Statement",
            capacity_parameter: crate::system_parameter_name!(pg_statement_max),
            capacity: node::global()
                .unwrap()
                .storage
                .db_config
                .observe_pg_statement_max(),
            dublicate_key_error_code: PgErrorCode::DuplicatePreparedStatement,
            total: &STATEMENTS_TOTAL,
        }
    }

    fn get_capacity(&self) -> usize {
        self.capacity.current_value()
    }
}

/// Name of a statement or portal.
pub type Key = Rc<str>;

/// Storage for Statements or Portals of one client.
/// Essentially a map with custom logic for updates and unnamed elements.
struct PgStorage<S> {
    map: BTreeMap<Key, S>,

    // Context specific to the portal or statement storage.
    context: StorageContext,
}

impl<S> PgStorage<S> {
    fn with_context(context: StorageContext) -> Self {
        Self {
            map: BTreeMap::new(),
            context,
        }
    }

    /// Change the map and add the change of its size to the shared count.
    fn update<R>(&mut self, f: impl FnOnce(&mut BTreeMap<Key, S>) -> R) -> R {
        let before = self.map.len();
        let result = f(&mut self.map);
        let after = self.map.len();
        self.context
            .total
            .with(|total| total.set(total.get() - before + after));
        result
    }

    /// Put a value and return the unnamed value that it replaces.
    fn put(&mut self, name: Key, value: S) -> PgResult<Option<S>> {
        let capacity = self.context.get_capacity();
        let total = self.context.total.get();
        let kind = self.context.value_kind;
        let parameter = self.context.capacity_parameter;
        let code = self.context.dublicate_key_error_code;
        self.update(|map| match map.entry(name) {
            // Updates are only allowed for unnamed statements and portals.
            // An update does not change the total, so the limit does not apply.
            Entry::Occupied(mut entry) if entry.key().is_empty() => Ok(Some(entry.insert(value))),
            Entry::Occupied(entry) => {
                let name = entry.key();
                let err = PedanticError::new(code, format!("{kind} '{name}' already exists"));
                Err(err.into())
            }
            Entry::Vacant(_) if capacity > 0 && total >= capacity => {
                // TODO: it should be configuration_limit_exceeded error
                Err(PgError::other(format!(
                    "{kind} storage is full. Current size limit: {capacity}. \
                    Please, increase storage limit using: \
                    ALTER SYSTEM SET \"{parameter}\" TO <new-limit>",
                )))
            }
            Entry::Vacant(entry) => {
                entry.insert(value);
                Ok(None)
            }
        })
    }

    #[inline(always)]
    fn get(&self, name: &str) -> Option<&S> {
        self.map.get(name)
    }

    fn remove(&mut self, name: &str) -> Option<S> {
        self.update(|map| map.remove(name))
    }

    fn retain(&mut self, f: impl FnMut(&Key, &mut S) -> bool) {
        self.update(|map| map.retain(f))
    }

    fn clear(&mut self) {
        self.update(|map| map.clear())
    }
}

impl<S> Drop for PgStorage<S> {
    fn drop(&mut self) {
        let len = self.map.len();
        self.context
            .total
            .with(|total| total.set(total.get() - len));
    }
}

/// Statements and portals of one client.
pub struct ClientStorage {
    statements: PgStorage<Statement>,
    portals: PgStorage<Portal>,
}

impl Default for ClientStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientStorage {
    pub fn new() -> Self {
        Self {
            statements: PgStorage::with_context(StorageContext::statements()),
            portals: PgStorage::with_context(StorageContext::portals()),
        }
    }

    pub fn statement(&self, name: &str) -> PgResult<Statement> {
        self.statements
            .get(name)
            .cloned()
            .ok_or_else(|| PgError::other(format!("Couldn't find statement '{name}'.")))
    }

    pub fn portal(&self, name: &str) -> PgResult<Portal> {
        self.portals
            .get(name)
            .cloned()
            .ok_or_else(|| PgError::other(format!("Couldn't find portal '{name}'.")))
    }

    pub fn put_statement(&mut self, name: Key, statement: Statement) -> PgResult<()> {
        if let Some(replaced) = self.statements.put(name, statement)? {
            self.close_portals_of(&replaced);
        }
        Ok(())
    }

    pub fn put_portal(&mut self, name: Key, portal: Portal) -> PgResult<()> {
        self.portals.put(name, portal)?;
        Ok(())
    }

    /// Close a statement and its portals. It's not an error to close a non-existent statement.
    /// Return false if there is no such statement.
    pub fn close_statement(&mut self, name: &str) -> bool {
        let Some(statement) = self.statements.remove(name) else {
            return false;
        };
        self.close_portals_of(&statement);
        true
    }

    /// Close a portal. It's not an error to close a non-existent portal.
    pub fn close_portal(&mut self, name: &str) {
        self.portals.remove(name);
    }

    pub fn close_all_portals(&mut self) {
        self.portals.clear();
    }

    /// Same as [`ClientStorage::close_statement`], but in contrast to closing,
    /// deallocation can cause an error in PG.
    pub fn deallocate(&mut self, name: &str) -> PgResult<()> {
        if !self.close_statement(name) {
            return Err(PgError::other(format!(
                "prepared statement {name} does not exist."
            )));
        }
        Ok(())
    }

    /// Close all statements and, as a consequence, all portals.
    pub fn deallocate_all(&mut self) {
        self.statements.clear();
        self.portals.clear();
    }

    fn close_portals_of(&mut self, statement: &Statement) {
        self.portals
            .retain(|_, portal| !portal.contains_statement(statement));
    }
}

pub static PGPROTO_STATEMENTS_OPENED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(prometheus::Opts::new(
        "pico_pgproto_statements_opened_total",
        "Total number of opened prepared statements since startup",
    ))
    .unwrap()
});

pub static PGPROTO_STATEMENTS_CLOSED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(prometheus::Opts::new(
        "pico_pgproto_statements_closed_total",
        "Total number of closed prepared statements since startup",
    ))
    .unwrap()
});

#[derive(Debug)]
pub struct StatementInner {
    name: Key,
    statement: sql::PreparedStatement,
    describe: StatementDescribe,
}

impl Drop for StatementInner {
    fn drop(&mut self) {
        PGPROTO_STATEMENTS_CLOSED_TOTAL.inc();
        tlog!(
            Debug,
            "dropped statement {:?} of type {:?}",
            self.name,
            self.describe.query_type()
        );
    }
}

#[derive(Debug, Clone)]
pub struct Statement(Rc<StatementInner>);

impl Statement {
    pub fn new(
        name: Key,
        statement: sql::PreparedStatement,
        specified_param_oids: Vec<u32>,
    ) -> PgResult<Self> {
        // generate pgproto metadata
        let inferred_types = statement.collect_parameter_types();
        let param_oids = collect_param_oids(&inferred_types, &specified_param_oids);
        let describe = Describe::new(statement.as_plan())?;

        let describe = StatementDescribe::new(describe, param_oids);
        let inner = StatementInner {
            name,
            statement,
            describe,
        };

        PGPROTO_STATEMENTS_OPENED_TOTAL.inc();
        tlog!(
            Debug,
            "created new statement {:?} of type {:?}",
            inner.name,
            inner.describe.query_type()
        );

        Ok(Self(inner.into()))
    }

    #[inline(always)]
    pub fn prepared_statement(&self) -> &sql::PreparedStatement {
        &self.0.statement
    }

    #[inline(always)]
    pub fn describe(&self) -> &StatementDescribe {
        &self.0.describe
    }

    #[inline(always)]
    fn ptr_eq(&self, other: &Statement) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }

    pub fn ensure_valid(&self) -> PgResult<()> {
        let stmt_plan = self.prepared_statement().as_plan();
        for table in stmt_plan.relations.tables.values() {
            let actual_version = with_su(ADMIN_ID, || get_table_version(&table.name))??;
            let cached_version = stmt_plan
                .table_version_map
                .get(&table.id)
                .expect("table version must be present in Plan");

            if actual_version > *cached_version {
                return Err(PgError::WithExplicitCode(PedanticError::new(
                    PgErrorCode::InvalidatedPreparedStatement,
                    std::io::Error::other(format!(
                        "prepared statement {} has been invalidated",
                        self.0.name
                    )),
                )));
            }
        }

        Ok(())
    }
}

pub(super) fn sbroad_type_to_pg(ty: &SbroadType) -> postgres_types::Type {
    match ty {
        SbroadType::Boolean => PgType::BOOL,
        SbroadType::Decimal => PgType::NUMERIC,
        SbroadType::Double => PgType::FLOAT8,
        SbroadType::Integer => PgType::INT8,
        SbroadType::String => PgType::TEXT,
        SbroadType::Uuid => PgType::UUID,
        SbroadType::Map | SbroadType::Any => PgType::JSON,
        SbroadType::Array(elem) => nested_type_to_pg_array(*elem),
        SbroadType::Datetime => PgType::TIMESTAMPTZ,
    }
}

fn nested_type_to_pg_array(elem: NestedType) -> postgres_types::Type {
    match elem {
        NestedType::Boolean => PgType::BOOL_ARRAY,
        NestedType::Datetime => PgType::TIMESTAMPTZ_ARRAY,
        NestedType::Numeric => PgType::NUMERIC_ARRAY,
        NestedType::Double => PgType::FLOAT8_ARRAY,
        NestedType::Integer => PgType::INT8_ARRAY,
        NestedType::Text => PgType::TEXT_ARRAY,
        NestedType::Uuid => PgType::UUID_ARRAY,
        NestedType::Map => PgType::JSON,
        // Untyped array: keep the historical JSON OID.
        NestedType::Any => PgType::JSON,
    }
}

pub(super) fn pg_type_to_sbroad(ty: &PgType) -> Option<SbroadType> {
    match ty {
        &PgType::BOOL => Some(SbroadType::Boolean),
        &PgType::NUMERIC => Some(SbroadType::Decimal),
        &PgType::FLOAT8 => Some(SbroadType::Double),
        &PgType::INT8 | &PgType::INT4 | &PgType::INT2 => Some(SbroadType::Integer),
        &PgType::TEXT | &PgType::VARCHAR => Some(SbroadType::String),
        &PgType::UUID => Some(SbroadType::Uuid),
        &PgType::TIMESTAMPTZ => Some(SbroadType::Datetime),
        &PgType::BOOL_ARRAY => Some(SbroadType::Array(NestedType::Boolean)),
        &PgType::INT8_ARRAY | &PgType::INT4_ARRAY | &PgType::INT2_ARRAY => {
            Some(SbroadType::Array(NestedType::Integer))
        }
        &PgType::FLOAT8_ARRAY | &PgType::FLOAT4_ARRAY => {
            Some(SbroadType::Array(NestedType::Double))
        }
        &PgType::NUMERIC_ARRAY => Some(SbroadType::Array(NestedType::Numeric)),
        &PgType::TEXT_ARRAY | &PgType::VARCHAR_ARRAY => Some(SbroadType::Array(NestedType::Text)),
        &PgType::UUID_ARRAY => Some(SbroadType::Array(NestedType::Uuid)),
        &PgType::TIMESTAMPTZ_ARRAY => Some(SbroadType::Array(NestedType::Datetime)),
        &PgType::JSON_ARRAY | &PgType::JSONB_ARRAY => Some(SbroadType::Array(NestedType::Map)),
        _unsupported_type => None,
    }
}

pub(super) fn param_oid_to_derived_type(oid: Oid) -> PgResult<DerivedType> {
    // 0 oid does not match any type, but it can be used to leave parameter type unspecified
    // (https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
    if oid == 0 || oid == PgType::UNKNOWN.oid() {
        return Ok(DerivedType::unknown());
    }

    let pg_type = PgType::from_oid(oid)
        .ok_or_else(|| PgError::FeatureNotSupported(format_smolstr!("parameter oid {oid}")))?;

    let sbroad_type = pg_type_to_sbroad(&pg_type)
        .ok_or_else(|| PgError::FeatureNotSupported(format_smolstr!("{pg_type} parameters")))?;

    Ok(DerivedType::new(sbroad_type))
}

/// Note that `client_types` may be incomplete or even empty,
/// as postgres protocol forces the backend to implement type inference.
/// Take the original client params and extended them with the inferred ones.
pub fn collect_param_oids(inferred_types: &[SbroadType], client_types: &[Oid]) -> Vec<Oid> {
    #[allow(non_snake_case)]
    let UNKNOWN_OID = PgType::UNKNOWN.oid();
    const BAD_OID: u32 = 0;
    debug_assert_ne!(UNKNOWN_OID, BAD_OID);

    let inferred_types = inferred_types.iter().map(|ty| sbroad_type_to_pg(ty).oid());

    let client_types = client_types
        .iter()
        .copied()
        // NOTE: client_types might be shorter!
        .chain(std::iter::repeat(UNKNOWN_OID));

    inferred_types
        .zip(client_types)
        .map(|(inferred_oid, client_oid)| {
            if [BAD_OID, UNKNOWN_OID].contains(&client_oid) {
                inferred_oid
            } else {
                client_oid
            }
        })
        .collect()
}

#[derive(serde::Serialize)]
#[serde(tag = "type")]
pub struct PreparedStatementMetadata {
    pub query: String,
    pub tier: Option<SmolStr>,
    // Vector which contains pairs of
    // parameter index and pg data type oid.
    pub dk_meta: Vec<(u16, Oid)>,
}

pub fn build_prepared_statement_metadata(
    prepared_statement: &sql::PreparedStatement,
    query: &str,
) -> PgResult<PreparedStatementMetadata> {
    let tier = prepared_statement.as_plan().tier.clone();
    let dk_meta = prepared_statement.as_plan().discover_sharding_key_info()?;

    let dk_meta: Vec<_> = dk_meta
        .into_iter()
        .map(|(idx, ty)| (idx, sbroad_type_to_pg(&ty).oid()))
        .collect();

    Ok(PreparedStatementMetadata {
        query: query.to_string(),
        tier,
        dk_meta,
    })
}

/// Get the number of changed rows from the port with DML result.
fn port_read_changed<'bytes>(mut port: impl Iterator<Item = &'bytes [u8]>) -> PgResult<usize> {
    let first_mp = port.next().unwrap_or(b"\xcc\x00");
    let mut cur = Cursor::new(first_mp);
    let changed: usize = rmp::decode::read_int(&mut cur).map_err(PgError::other)?;
    Ok(changed)
}

enum PortalState {
    /// Portal has just been created.
    /// Ideally, it should've been `Box<Plan>`, but we need to move it
    /// from a mutable reference and we don't want to allocate a substitute.
    NotStarted(sql::BoundStatement),
    /// Portal has been executed and contains rows to be sent in batches.
    StreamingRows(RowSource),
    /// Portal has been executed and contains a result ready to be sent.
    ResultReady(ExecuteResult),
    /// Portal has been executed, and a result has been sent.
    Done,
    /// An error encountered when executing the portal. Further execution is not possible.
    Errored,
}

// We use this instance in tests. Furthermore, stock debug print is too verbose.
// See test/pgproto/cornerstone_test.py::test_interactive_portals
impl std::fmt::Display for PortalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use PortalState::*;
        match self {
            NotStarted(_) => f.debug_tuple("NotStarted").finish_non_exhaustive(),
            StreamingRows(_) => f.debug_tuple("StreamingRows").finish_non_exhaustive(),
            ResultReady(_) => f.debug_struct("ResultReady").finish_non_exhaustive(),
            Done => f.debug_struct("Done").finish(),
            Errored => f.debug_struct("Errored").finish(),
        }
    }
}

pub static PGPROTO_PORTALS_OPENED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(prometheus::Opts::new(
        "pico_pgproto_portals_opened_total",
        "Total number of opened query execution portals since startup",
    ))
    .unwrap()
});

pub static PGPROTO_PORTALS_CLOSED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(prometheus::Opts::new(
        "pico_pgproto_portals_closed_total",
        "Total number of closed query execution portals since startup",
    ))
    .unwrap()
});

struct PortalInner {
    name: Key,
    statement: Statement,
    describe: PortalDescribe,
    row_info: Arc<Vec<FieldInfo>>,
    state: RefCell<PortalState>,
}

impl Drop for PortalInner {
    fn drop(&mut self) {
        PGPROTO_PORTALS_CLOSED_TOTAL.inc();
        tlog!(
            Debug,
            "dropped portal {:?} in state {}",
            self.name,
            self.state.borrow()
        );
    }
}

impl PortalInner {
    fn start(
        &self,
        router: &RouterRuntime,
        statement: sql::BoundStatement,
    ) -> PgResult<PortalState> {
        // An empty query has nothing to execute: do not dispatch it.
        if let QueryType::Empty = self.describe.query_type() {
            return Ok(PortalState::ResultReady(ExecuteResult::Empty));
        }

        if let QueryType::Dml = self.describe.query_type() {
            if let Some(query) = self.statement.prepared_statement().query_for_audit() {
                audit::policy::log_dml_for_user(query, statement.params_for_audit());
            }
        }
        if let Some(query) = self.statement.prepared_statement().query_for_logging() {
            // Logs only the SQL text, not its parameters.
            tlog!(Info, "sql-log: {query}");
        }

        let port = PinnedPort::fill(|port| {
            crate::sql::dispatch_bound_statement(router, statement, None, None, port)?;
            Ok(())
        })?;

        let state = match self.describe.query_type() {
            QueryType::Acl | QueryType::Ddl => {
                let tag = self.describe.command_tag();
                PortalState::ResultReady(ExecuteResult::AclOrDdl { tag })
            }
            QueryType::Tcl => {
                let tag = self.describe.command_tag();
                PortalState::ResultReady(ExecuteResult::Tcl { tag })
            }
            QueryType::Dml => {
                let row_count = port_read_changed(port.port_c().iter())?;
                let tag = self.describe.command_tag();
                PortalState::ResultReady(ExecuteResult::Dml { row_count, tag })
            }
            // The port of BACKUP has the same layout as the port of DQL.
            QueryType::Dql | QueryType::Backup => PortalState::StreamingRows(RowSource::dql(port)),
            QueryType::Explain => PortalState::StreamingRows(RowSource::explain(ExplainIter::new(
                port.port_c().iter(),
            ))),
            // Backend::execute_portal deallocates the statements.
            QueryType::Deallocate => {
                let tag = self.describe.command_tag();
                PortalState::ResultReady(ExecuteResult::AclOrDdl { tag })
            }
            QueryType::Empty => PortalState::ResultReady(ExecuteResult::Empty),
        };

        Ok(state)
    }

    fn execute(&self, runtime: &RouterRuntime, max_rows: usize) -> PgResult<ExecuteResult> {
        let mut state = self.state.borrow_mut();

        /// A helper that allows to run the [`PortalState`] state machine, with a step taking the ownership of the current state.
        ///
        /// The function `f` defines the step function of the state machine.
        /// It receives the current state by value and returns a tuple of a return value and a new state.
        ///
        /// If `f` returns [`Err`] or panics, `dest` will be replaced with [`PortalState::Errored`]
        pub fn step_portal<R, F: FnOnce(PortalState) -> PgResult<(R, PortalState)>>(
            dest: &mut PortalState,
            f: F,
        ) -> PgResult<R> {
            replace_with::replace_with_and_return(
                dest,
                || PortalState::Errored,
                |state| match f(state) {
                    Ok((result, new_state)) => (Ok(result), new_state),
                    Err(err) => (Err(err), PortalState::Errored),
                },
            )
        }

        loop {
            // run the state machine until a return value is produced as a `Some`
            let result = step_portal(&mut state, |state| match state {
                PortalState::NotStarted(bound_statement) => {
                    Ok((None, self.start(runtime, bound_statement)?))
                }
                PortalState::ResultReady(result) => Ok((Some(result), PortalState::Done)),
                PortalState::StreamingRows(mut stored_rows) => {
                    let batch = stored_rows.split_off_front(max_rows);
                    let row_count = batch.len();
                    let rows = Rows::new(batch, Arc::clone(&self.row_info));

                    Ok(if stored_rows.is_empty() {
                        (
                            Some(ExecuteResult::FinishedDql {
                                rows,
                                row_count,
                                tag: self.describe.command_tag(),
                            }),
                            PortalState::Done,
                        )
                    } else {
                        (
                            Some(ExecuteResult::SuspendedDql { rows }),
                            PortalState::StreamingRows(stored_rows),
                        )
                    })
                }
                _ => Err(PgError::other(format!(
                    "Can't execute portal in state {state}",
                ))),
            })?;

            if let Some(result) = result {
                break Ok(result);
            }
        }
    }
}

#[derive(Clone)]
pub struct Portal(Rc<PortalInner>);

impl Portal {
    pub fn new(
        name: Key,
        statement: Statement,
        output_format: Vec<FieldFormat>,
        bound_statement: sql::BoundStatement,
    ) -> PgResult<Self> {
        let stmt_describe = statement.describe();
        let describe = PortalDescribe::new(stmt_describe.describe.clone(), output_format);
        let row_info = Arc::new(describe.row_info());
        let state = PortalState::NotStarted(bound_statement).into();
        let inner = PortalInner {
            name,
            statement,
            describe,
            row_info,
            state,
        };

        PGPROTO_PORTALS_OPENED_TOTAL.inc();
        tlog!(Debug, "created new portal {:?}", inner.name);

        Ok(Self(inner.into()))
    }

    #[inline(always)]
    pub fn describe(&self) -> &PortalDescribe {
        &self.0.describe
    }

    #[inline(always)]
    pub fn contains_statement(&self, statement: &Statement) -> bool {
        Statement::ptr_eq(&self.0.statement, statement)
    }

    #[inline(always)]
    pub fn statement(&self) -> &Statement {
        &self.0.statement
    }

    #[inline(always)]
    pub fn execute(&self, runtime: &RouterRuntime, max_rows: usize) -> PgResult<ExecuteResult> {
        self.0.execute(runtime, max_rows)
    }
}

#[cfg(test)]
mod test {
    use super::{pg_type_to_sbroad, sbroad_type_to_pg};
    use postgres_types::Type as PgType;
    use sql::ir::types::NestedType;
    use sql::ir::types::UnrestrictedType as SbroadType;

    #[test]
    fn test_sbroad_type_to_pg() {
        for (sbroad, expected_pg) in [
            (SbroadType::Boolean, PgType::BOOL),
            (SbroadType::Datetime, PgType::TIMESTAMPTZ),
            (SbroadType::Decimal, PgType::NUMERIC),
            (SbroadType::Double, PgType::FLOAT8),
            (SbroadType::Integer, PgType::INT8),
            (SbroadType::String, PgType::TEXT),
            (SbroadType::Uuid, PgType::UUID),
            (SbroadType::Any, PgType::JSON),
            (SbroadType::Map, PgType::JSON),
            (SbroadType::Array(NestedType::Any), PgType::JSON),
        ] {
            assert_eq!(sbroad_type_to_pg(&sbroad), expected_pg)
        }
        for (nested, expected_pg) in [
            (NestedType::Boolean, PgType::BOOL_ARRAY),
            (NestedType::Datetime, PgType::TIMESTAMPTZ_ARRAY),
            (NestedType::Numeric, PgType::NUMERIC_ARRAY),
            (NestedType::Double, PgType::FLOAT8_ARRAY),
            (NestedType::Integer, PgType::INT8_ARRAY),
            (NestedType::Text, PgType::TEXT_ARRAY),
            (NestedType::Uuid, PgType::UUID_ARRAY),
            (NestedType::Map, PgType::JSON),
        ] {
            assert_eq!(sbroad_type_to_pg(&SbroadType::Array(nested)), expected_pg)
        }
    }

    #[test]
    fn test_pg_type_to_sbroad() {
        for (pg, expected_sbroad) in [
            (PgType::BOOL, SbroadType::Boolean),
            (PgType::NUMERIC, SbroadType::Decimal),
            (PgType::FLOAT8, SbroadType::Double),
            (PgType::INT8, SbroadType::Integer),
            (PgType::INT4, SbroadType::Integer),
            (PgType::INT2, SbroadType::Integer),
            (PgType::TEXT, SbroadType::String),
            (PgType::UUID, SbroadType::Uuid),
            (PgType::BOOL_ARRAY, SbroadType::Array(NestedType::Boolean)),
            (PgType::INT8_ARRAY, SbroadType::Array(NestedType::Integer)),
            (PgType::INT4_ARRAY, SbroadType::Array(NestedType::Integer)),
            (PgType::FLOAT8_ARRAY, SbroadType::Array(NestedType::Double)),
            (
                PgType::NUMERIC_ARRAY,
                SbroadType::Array(NestedType::Numeric),
            ),
            (PgType::TEXT_ARRAY, SbroadType::Array(NestedType::Text)),
            (PgType::UUID_ARRAY, SbroadType::Array(NestedType::Uuid)),
            (
                PgType::TIMESTAMPTZ_ARRAY,
                SbroadType::Array(NestedType::Datetime),
            ),
            (PgType::JSON_ARRAY, SbroadType::Array(NestedType::Map)),
        ] {
            assert!(pg_type_to_sbroad(&pg).unwrap() == expected_sbroad)
        }
    }
}
