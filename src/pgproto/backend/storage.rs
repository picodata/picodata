use super::{
    close_client_statements, deallocate_statement,
    describe::{Describe, MetadataColumn, PortalDescribe, QueryType, StatementDescribe},
    result::{ExecuteResult, Rows},
};
use crate::config::observer::AtomicObserver;
use crate::{
    pgproto::{
        client::ClientId,
        error::{PedanticError, PgError, PgErrorCode, PgResult},
        value::{FieldFormat, PgValue},
    },
    sql::{dispatch, router::RouterRuntime},
    tlog,
    traft::node,
};
use postgres_types::{Oid, Type as PgType};
use prometheus::IntCounter;
use rmpv::Value;
use sbroad::{
    executor::{ir::ExecutionPlan, Query},
    ir::{
        relation::{DerivedType, Type as SbroadType},
        Plan,
    },
};
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;
use std::{
    cell::RefCell,
    collections::{btree_map::Entry, BTreeMap, HashMap},
    iter::zip,
    ops::Bound,
    os::raw::c_int,
    rc::{Rc, Weak},
    sync::LazyLock,
    vec::IntoIter,
};
use tarantool::{
    proc::{Return, ReturnMsgpack},
    tuple::{FunctionCtx, Tuple},
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
}

type StatementStorage = PgStorage<StatementHolder>;

impl StatementStorage {
    fn new() -> Self {
        let context = StorageContext::statements();
        PgStorage::with_context(context)
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
    plan: Plan,
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
    pub fn new(key: Key, plan: Plan, specified_param_oids: Vec<u32>) -> PgResult<Self> {
        let param_oids = collect_param_oids(&plan, &specified_param_oids);
        let describe = StatementDescribe::new(Describe::new(&plan)?, param_oids);
        let inner = StatementInner {
            key,
            plan,
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
    pub fn plan(&self) -> &Plan {
        &self.0.plan
    }

    #[inline(always)]
    pub fn describe(&self) -> &StatementDescribe {
        &self.0.describe
    }

    #[inline(always)]
    fn ptr_eq(&self, other: &Statement) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
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
        SbroadType::Integer | SbroadType::Unsigned => PgType::INT8,
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
pub fn collect_param_oids(plan: &Plan, client_types: &[Oid]) -> Vec<Oid> {
    #[allow(non_snake_case)]
    let UNKNOWN_OID = PgType::UNKNOWN.oid();
    const BAD_OID: u32 = 0;
    debug_assert_ne!(UNKNOWN_OID, BAD_OID);

    let inferred_types = plan
        .collect_parameter_types()
        .into_iter()
        .map(|ty| sbroad_type_to_pg(&ty).oid());

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

/// Get rows from DQL or EXPLAIN query execution result tuple.
/// **Panics** if the tuple does not match the expected format.
fn get_rows_from_tuple(tuple: &Tuple) -> Vec<Vec<Value>> {
    #[derive(Deserialize)]
    struct DqlResult {
        rows: Vec<Vec<Value>>,
    }

    // Try parsing a regular DQL result.
    if let Ok(Some(res)) = tuple.field::<DqlResult>(0) {
        return res.rows;
    }

    // Try parsing an EXPLAIN result.
    if let Ok(Some(res)) = tuple.field::<Vec<Value>>(0) {
        let rows = res.into_iter().map(|row| vec![row]).collect();
        return rows;
    }

    panic!("query result: invalid representation of rows");
}

/// Get row count from query execution result tuple.
/// **Panics** if the tuple does not match the expected format.
fn get_row_count_from_tuple(tuple: &Tuple) -> usize {
    #[derive(Deserialize)]
    struct RowCount {
        row_count: usize,
    }

    if let Ok(Some(res)) = tuple.field::<RowCount>(0) {
        return res.row_count;
    }

    panic!("query result: no row count found");
}

fn mp_row_into_pg_row(mp: Vec<Value>, metadata: &[MetadataColumn]) -> PgResult<Vec<PgValue>> {
    zip(mp, metadata)
        .map(|(v, col)| PgValue::try_from_rmpv(v, &col.ty))
        .collect()
}

#[derive(Debug)]
enum PortalState {
    /// Portal has just been created.
    /// Ideally, it should've been `Box<Plan>`, but we need to move it
    /// from a mutable reference and we don't want to allocate a substitute.
    NotStarted(Option<Box<Plan>>),
    /// Portal has been executed and contains rows to be sent in batches.
    StreamingRows(IntoIter<Vec<PgValue>>),
    /// Portal has been executed and contains a result ready to be sent.
    ResultReady(ExecuteResult),
    /// Portal has been executed, and a result has been sent.
    Done,
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

#[derive(Debug)]
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
    fn start(&self, plan: Box<Plan>) -> PgResult<PortalState> {
        let runtime = RouterRuntime::new()?;
        let query = Query::from_parts(
            plan.is_explain(),
            ExecutionPlan::from(*plan),
            &runtime,
            HashMap::new(),
        );
        let tuple = dispatch(query, None)?;

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
                let row_count = get_row_count_from_tuple(&tuple);
                let tag = self.describe.command_tag();
                PortalState::ResultReady(ExecuteResult::Dml { row_count, tag })
            }
            QueryType::Dql | QueryType::Explain => {
                let mp_rows = get_rows_from_tuple(&tuple);
                let metadata = self.describe.metadata();
                let pg_rows = mp_rows
                    .into_iter()
                    .map(|row| mp_row_into_pg_row(row, metadata))
                    .collect::<PgResult<Vec<Vec<_>>>>()?;

                PortalState::StreamingRows(pg_rows.into_iter())
            }
            QueryType::Deallocate => {
                let ir_plan = self.statement.plan();
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

    fn execute(&self, max_rows: usize) -> PgResult<ExecuteResult> {
        let mut state = self.state.borrow_mut();
        loop {
            match &mut *state {
                PortalState::NotStarted(plan) => {
                    // We always provide the plan, see Portal::new.
                    let plan = plan.take().expect("plan not found");
                    *state = self.start(plan)?;
                }
                PortalState::ResultReady(result) => {
                    let result = std::mem::replace(result, ExecuteResult::Empty);
                    *state = PortalState::Done;

                    return Ok(result);
                }
                PortalState::StreamingRows(ref mut stored_rows) => {
                    let taken: Vec<_> = stored_rows.take(max_rows).collect();
                    let row_count = taken.len();
                    let rows = Rows::new(taken, self.describe.row_info());

                    return Ok(match stored_rows.len() {
                        0 => {
                            *state = PortalState::Done;

                            ExecuteResult::FinishedDql {
                                rows,
                                row_count,
                                tag: self.describe.command_tag(),
                            }
                        }
                        _ => ExecuteResult::SuspendedDql { rows },
                    });
                }
                _ => {
                    return Err(PgError::other(format!(
                        "Can't execute portal in state {state}",
                    )));
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Portal(Rc<PortalInner>);

impl Portal {
    pub fn new(
        key: Key,
        statement: Statement,
        output_format: Vec<FieldFormat>,
        plan: Plan,
    ) -> PgResult<Self> {
        let stmt_describe = statement.describe();
        let describe = PortalDescribe::new(stmt_describe.describe.clone(), output_format);
        let state = PortalState::NotStarted(Some(plan.into())).into();
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
    pub fn execute(&self, max_rows: usize) -> PgResult<ExecuteResult> {
        self.0.execute(max_rows)
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
    use sbroad::ir::relation::Type as SbroadType;

    #[test]
    fn test_sbroad_type_to_pg() {
        for (sbroad, expected_pg) in vec![
            (SbroadType::Array, PgType::JSON),
            (SbroadType::Decimal, PgType::NUMERIC),
            (SbroadType::Double, PgType::FLOAT8),
            (SbroadType::Integer, PgType::INT8),
            (SbroadType::Unsigned, PgType::INT8),
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
        for (pg, expected_sbroad) in vec![
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
