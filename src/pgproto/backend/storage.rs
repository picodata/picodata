use super::{
    close_client_statements, deallocate_statement,
    describe::{Describe, MetadataColumn, PortalDescribe, QueryType, StatementDescribe},
    result::{ExecuteResult, Rows},
};
use crate::config::observer::AtomicObserver;
use crate::sql::port::PicoPortOwned;
use crate::sql::router::{get_table_version, RouterRuntime};
use crate::{audit, schema::ADMIN_ID};
use crate::{
    pgproto::{
        client::ClientId,
        error::{PedanticError, PgError, PgErrorCode, PgResult},
        value::{FieldFormat, PgValue},
    },
    tlog,
    traft::node,
};
use postgres_types::{Oid, Type as PgType};
use prometheus::IntCounter;
use serde::Serialize;
use smol_str::format_smolstr;
use sql::executor::Port;
use sql::ir::types::{DerivedType, UnrestrictedType as SbroadType};
use sql_protocol::query_plan::PlanBlockIter;
use std::{
    cell::RefCell,
    collections::{btree_map::Entry, BTreeMap},
    io::Cursor,
    ops::Bound,
    os::raw::c_int,
    rc::{Rc, Weak},
    sync::LazyLock,
    vec::IntoIter,
};
use tarantool::{
    proc::{Return, ReturnMsgpack},
    session::with_su,
    tuple::FunctionCtx,
};

/// Used to store portals in [`PG_PORTALS`] and statements in [`PG_STATEMENTS`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key(pub ClientId, pub Rc<str>);

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (id, name) = (self.0, &self.1);
        f.debug_tuple("")
            .field(&format_args!("{id}"))
            .field(&format_args!("{name:?}"))
            .finish()
    }
}

// Object that stores information specific to the portal or statement storage.
struct StorageContext {
    value_kind: &'static str,
    capacity_parameter: &'static str,
    capacity: AtomicObserver<usize>,
    dublicate_key_error_code: PgErrorCode,
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
        }
    }

    fn get_capacity(&self) -> usize {
        self.capacity.current_value()
    }
}

/// Storage for Statements and Portals.
/// Essentially a map with custom logic for updates and unnamed elements.
pub struct PgStorage<S> {
    map: BTreeMap<Key, S>,
    // We reuse this empty name in range scans in order to avoid Rc allocations.
    // Note: see names_by_client_id.
    empty_name: Rc<str>,

    // Context specific to the portal or statement storage.
    context: StorageContext,
}

impl<S> PgStorage<S> {
    fn with_context(context: StorageContext) -> Self {
        Self {
            map: BTreeMap::new(),
            empty_name: Rc::from(String::new()),
            context,
        }
    }

    pub fn put(&mut self, key: Key, value: S) -> PgResult<()> {
        let capacity = self.context.get_capacity();
        let kind = self.context.value_kind;
        if capacity > 0 && self.len() >= capacity {
            let parameter = self.context.capacity_parameter;
            // TODO: it should be configuration_limit_exceeded error
            return Err(PgError::other(format!(
                "{kind} storage is full. Current size limit: {capacity}. \
                Please, increase storage limit using: \
                ALTER SYSTEM SET \"{parameter}\" TO <new-limit>",
            )));
        }

        if key.1.is_empty() {
            // Updates are only allowed for unnamed statements and portals.
            self.map.insert(key, value);
            return Ok(());
        }

        match self.map.entry(key) {
            Entry::Occupied(entry) => {
                let Key(id, name) = entry.key();
                let err = PedanticError::new(
                    self.context.dublicate_key_error_code,
                    format!("{kind} '{name}' for client {id} already exists"),
                );
                Err(err.into())
            }
            Entry::Vacant(entry) => {
                entry.insert(value);
                Ok(())
            }
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &Key) -> Option<&S> {
        self.map.get(key)
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &Key) -> Option<S> {
        self.map.remove(key)
    }

    #[inline(always)]
    pub fn remove_by_client_id(&mut self, id: ClientId) {
        self.map.retain(|k, _| k.0 != id)
    }

    pub fn names_by_client_id(&self, id: ClientId) -> Vec<Rc<str>> {
        let range = (
            Bound::Included(Key(id, Rc::clone(&self.empty_name))),
            Bound::Excluded(Key(id + 1, Rc::clone(&self.empty_name))),
        );
        self.map
            .range(range)
            .map(|(Key(_, name), _)| Rc::clone(name))
            .collect()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type StatementStorage = PgStorage<StatementHolder>;

impl StatementStorage {
    fn new() -> Self {
        let context = StorageContext::statements();
        PgStorage::with_context(context)
    }
}

impl Default for PortalStorage {
    fn default() -> Self {
        Self::new()
    }
}

type PortalStorage = PgStorage<Portal>;

impl PortalStorage {
    pub fn new() -> Self {
        let context = StorageContext::portals();
        PgStorage::with_context(context)
    }

    pub fn remove_portals_by_statement(&mut self, statement: &Statement) {
        self.map
            .retain(|_, portal| !portal.contains_statement(statement));
    }
}

// TODO: those shouldn't be global variables; move them to some context (backend?).
thread_local! {
    pub static PG_STATEMENTS: RefCell<StatementStorage> = RefCell::new(StatementStorage::new());
    pub static PG_PORTALS: Rc<RefCell<PortalStorage>> = Rc::new(RefCell::new(PortalStorage::new()));
}

/// Eagerly initialize storages for prepared statements and portals.
pub fn force_init_portals_and_statements() {
    PG_STATEMENTS.with(|_| {});
    PG_PORTALS.with(|_| {});
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
    key: Key,
    statement: sql::PreparedStatement,
    describe: StatementDescribe,
}

impl Drop for StatementInner {
    fn drop(&mut self) {
        PGPROTO_STATEMENTS_CLOSED_TOTAL.inc();
        tlog!(
            Debug,
            "dropped statement {} of type {:?}",
            self.key,
            self.describe.query_type()
        );
    }
}

#[derive(Debug, Clone)]
pub struct Statement(Rc<StatementInner>);

impl Statement {
    pub fn new(
        key: Key,
        statement: sql::PreparedStatement,
        specified_param_oids: Vec<u32>,
    ) -> PgResult<Self> {
        // generate pgproto metadata
        let inferred_types = statement.collect_parameter_types();
        let param_oids = collect_param_oids(&inferred_types, &specified_param_oids);
        let describe = Describe::new(statement.as_plan())?;

        let describe = StatementDescribe::new(describe, param_oids);
        let inner = StatementInner {
            key,
            statement,
            describe,
        };

        PGPROTO_STATEMENTS_OPENED_TOTAL.inc();
        tlog!(
            Debug,
            "created new statement {} of type {:?}",
            inner.key,
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
                        self.0.key.1
                    )),
                )));
            }
        }

        Ok(())
    }
}

// A wrapper over Statement that closes portals created from it on `Drop`.
pub struct StatementHolder {
    statement: Statement,
    // When the holder drops it also drops some portals from the portal storage, but the
    // storage may have already been dropped, so must not be accessed.
    // To handle this, we use this Weak that won't allow us to access a dropped storage.
    portals: Weak<RefCell<PortalStorage>>,
}

impl StatementHolder {
    pub fn new(statement: Statement) -> StatementHolder {
        let portals = PG_PORTALS.with(Rc::downgrade);
        Self { statement, portals }
    }

    pub fn statement(&self) -> Statement {
        self.statement.clone()
    }
}

impl From<Statement> for StatementHolder {
    fn from(value: Statement) -> Self {
        Self::new(value)
    }
}

impl Drop for StatementHolder {
    fn drop(&mut self) {
        // The storage may have already been dropped, so we cannot access PG_PORTALS.
        if let Some(portals) = self.portals.upgrade() {
            portals
                .borrow_mut()
                .remove_portals_by_statement(&self.statement)
        }
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
        SbroadType::Map | SbroadType::Array | SbroadType::Any => PgType::JSON,
        SbroadType::Datetime => PgType::TIMESTAMPTZ,
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

pub fn port_read_tuples<'bytes>(
    port: impl Iterator<Item = &'bytes [u8]>,
    tuples: usize,
    metadata: &[MetadataColumn],
) -> PgResult<Vec<Vec<PgValue>>> {
    let mut rows = Vec::with_capacity(tuples);
    for mp in port {
        let mut cur = Cursor::new(mp);

        // First check that we have an array value.
        let len = rmp::decode::read_array_len(&mut cur).map_err(PgError::other)? as usize;
        if len != metadata.len() {
            return Err(PgError::other(format!(
                "Expected {} columns, got {}",
                metadata.len(),
                len
            )));
        }

        // Decode each column and convert it to postgres value.
        let mut tuple = Vec::with_capacity(len);
        for col in metadata.iter().take(len) {
            let v = rmpv::decode::read_value(&mut cur).map_err(PgError::other)?;
            let pg_value = PgValue::try_from_rmpv(v, &col.ty)?;
            tuple.push(pg_value);
        }
        rows.push(tuple);
    }
    Ok(rows)
}

fn port_read_explain<'bytes>(
    port: impl Iterator<Item = &'bytes [u8]>,
    tuples: usize,
    metadata: &[MetadataColumn],
) -> PgResult<Vec<Vec<PgValue>>> {
    if metadata.len() != 1 {
        return Err(PgError::other(format!(
            "Expected 1 column in EXPLAIN metadata, got {}",
            metadata.len()
        )));
    }
    let mut rows = Vec::with_capacity(tuples);
    for mp in port {
        let mut cur = Cursor::new(mp);
        let val = rmpv::decode::read_value(&mut cur).map_err(PgError::other)?;
        let pg_value = PgValue::try_from_rmpv(val, &metadata[0].ty)?;
        rows.push(vec![pg_value]);
    }
    Ok(rows)
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
    StreamingRows(IntoIter<Vec<PgValue>>),
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
    key: Key,
    statement: Statement,
    describe: PortalDescribe,
    state: RefCell<PortalState>,
}

impl Drop for PortalInner {
    fn drop(&mut self) {
        PGPROTO_PORTALS_CLOSED_TOTAL.inc();
        tlog!(
            Debug,
            "dropped portal {} in state {}",
            self.key,
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
        if let QueryType::Dml = self.describe.query_type() {
            if let Some(query) = self.statement.prepared_statement().query_for_audit() {
                audit::policy::log_dml_for_user(query, statement.params_for_audit());
            }
        }

        let mut port = PicoPortOwned::new();
        crate::sql::dispatch_bound_statement(router, statement, None, None, &mut port)?;

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
                let row_count = port_read_changed(port.iter())?;
                let tag = self.describe.command_tag();
                PortalState::ResultReady(ExecuteResult::Dml { row_count, tag })
            }
            QueryType::Dql => {
                let rows = port_read_tuples(
                    port.iter().skip(1),
                    port.size() as usize,
                    self.describe.metadata(),
                )?;
                PortalState::StreamingRows(rows.into_iter())
            }
            QueryType::Explain => {
                let ir_plan = self.statement.prepared_statement().as_plan();
                let rows = if ir_plan.is_plain_explain() {
                    port_read_explain(port.iter(), port.size() as usize, self.describe.metadata())?
                } else {
                    let mut rows: Vec<Vec<PgValue>> = Vec::new();
                    let plan_steps = PlanBlockIter::new(port.iter());
                    for block in plan_steps.into_iter().map(|b| b.to_string()) {
                        for line in block.split('\n') {
                            rows.push(vec![PgValue::Text(line.to_string())]);
                        }
                    }
                    rows
                };
                PortalState::StreamingRows(rows.into_iter())
            }
            QueryType::Deallocate => {
                let ir_plan = self.statement.prepared_statement().as_plan();
                let top_id = ir_plan.get_top()?;
                let deallocate = ir_plan.get_deallocate_node(top_id)?;
                let name = deallocate.name.as_ref().map(|name| name.as_str());
                match name {
                    Some(name) => deallocate_statement(self.key.0, name)?,
                    None => close_client_statements(self.key.0),
                };

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
                    let taken: Vec<_> = (&mut stored_rows).take(max_rows).collect();
                    let row_count = taken.len();
                    let rows = Rows::new(taken, self.describe.row_info());

                    Ok(match stored_rows.len() {
                        0 => (
                            Some(ExecuteResult::FinishedDql {
                                rows,
                                row_count,
                                tag: self.describe.command_tag(),
                            }),
                            PortalState::Done,
                        ),
                        _ => (
                            Some(ExecuteResult::SuspendedDql { rows }),
                            PortalState::StreamingRows(stored_rows),
                        ),
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
        key: Key,
        statement: Statement,
        output_format: Vec<FieldFormat>,
        bound_statement: sql::BoundStatement,
    ) -> PgResult<Self> {
        let stmt_describe = statement.describe();
        let describe = PortalDescribe::new(stmt_describe.describe.clone(), output_format);
        let state = PortalState::NotStarted(bound_statement).into();
        let inner = PortalInner {
            key,
            statement,
            describe,
            state,
        };

        PGPROTO_PORTALS_OPENED_TOTAL.inc();
        tlog!(Debug, "created new portal {}", inner.key);

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
    pub fn execute(&self, runtime: &RouterRuntime, max_rows: usize) -> PgResult<ExecuteResult> {
        self.0.execute(runtime, max_rows)
    }
}

#[derive(Debug, Serialize)]
pub struct UserStatementNames {
    // Known statements for the client with the given client id.
    available: Vec<Rc<str>>,
    // Total number of statements in the storage (including other clients).
    total: usize,
}

impl UserStatementNames {
    pub fn new(id: ClientId) -> Self {
        PG_STATEMENTS.with(|storage| {
            let storage = storage.borrow();
            Self {
                available: storage.names_by_client_id(id),
                total: storage.len(),
            }
        })
    }
}

impl Return for UserStatementNames {
    fn ret(self, ctx: FunctionCtx) -> c_int {
        ReturnMsgpack(self).ret(ctx)
    }
}

#[derive(Debug, Serialize)]
pub struct UserPortalNames {
    // Known portals for the client with the given client id.
    available: Vec<Rc<str>>,
    // Total number of portals in the storage (including other clients).
    total: usize,
}

impl UserPortalNames {
    pub fn new(id: ClientId) -> Self {
        PG_PORTALS.with(|storage| {
            let storage = storage.borrow();
            Self {
                available: storage.names_by_client_id(id),
                total: storage.len(),
            }
        })
    }
}

impl Return for UserPortalNames {
    fn ret(self, ctx: FunctionCtx) -> c_int {
        ReturnMsgpack(self).ret(ctx)
    }
}

#[cfg(test)]
mod test {
    use super::{pg_type_to_sbroad, sbroad_type_to_pg};
    use postgres_types::Type as PgType;
    use sql::ir::types::UnrestrictedType as SbroadType;

    #[test]
    fn test_sbroad_type_to_pg() {
        for (sbroad, expected_pg) in vec![
            (SbroadType::Array, PgType::JSON),
            (SbroadType::Decimal, PgType::NUMERIC),
            (SbroadType::Double, PgType::FLOAT8),
            (SbroadType::Integer, PgType::INT8),
            (SbroadType::String, PgType::TEXT),
            (SbroadType::Uuid, PgType::UUID),
            (SbroadType::Any, PgType::JSON),
            (SbroadType::Array, PgType::JSON),
            (SbroadType::Map, PgType::JSON),
        ] {
            assert!(sbroad_type_to_pg(&sbroad) == expected_pg)
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
        ] {
            assert!(pg_type_to_sbroad(&pg).unwrap() == expected_sbroad)
        }
    }
}
