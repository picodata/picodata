use super::describe::{Describe, PortalDescribe, StatementDescribe};
use super::describe::{MetadataColumn, QueryType};
use super::result::{ExecuteResult, Rows};
use crate::pgproto::error::{PgError, PgErrorCode, PgResult};
use crate::pgproto::value::{FieldFormat, PgValue};
use crate::storage::PropertyName;
use crate::traft::node;
use ::tarantool::tuple::Tuple;
use rmpv::Value;
use sbroad::executor::ir::ExecutionPlan;
use sbroad::executor::Query;
use sbroad::ir::Plan;
use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::iter::zip;
use std::ops::Bound::{Excluded, Included};
use std::os::raw::c_int;
use std::rc::{Rc, Weak};
use std::vec::IntoIter;
use tarantool::proc::{Return, ReturnMsgpack};
use tarantool::tuple::FunctionCtx;

use crate::sql::dispatch;
use crate::sql::router::RouterRuntime;

pub type ClientId = u32;

// Object that stores information specific to the portal or statement storage.
struct StorageContext {
    capacity_property: PropertyName,
    get_capacity: fn() -> PgResult<usize>,
    dublicate_key_error_code: PgErrorCode,
}

impl StorageContext {
    fn portals() -> StorageContext {
        fn get_capacity() -> PgResult<usize> {
            Ok(node::global()?.storage.properties.max_pg_portals()?)
        }

        Self {
            capacity_property: PropertyName::MaxPgPortals,
            get_capacity,
            dublicate_key_error_code: PgErrorCode::DuplicateCursor,
        }
    }

    fn statements() -> StorageContext {
        fn get_capacity() -> PgResult<usize> {
            Ok(node::global()?.storage.properties.max_pg_statements()?)
        }

        Self {
            capacity_property: PropertyName::MaxPgStatements,
            get_capacity,
            dublicate_key_error_code: PgErrorCode::DuplicatePreparedStatement,
        }
    }

    fn get_capacity(&self) -> PgResult<usize> {
        (self.get_capacity)()
    }
}

/// Storage for Statements and Portals.
/// Essentially a map with custom logic for updates and unnamed elements.
pub struct PgStorage<S> {
    map: BTreeMap<(ClientId, Rc<str>), S>,
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

    pub fn put(&mut self, key: (ClientId, Rc<str>), value: S) -> PgResult<()> {
        let capacity = self.context.get_capacity()?;
        if self.len() >= capacity {
            // TODO: it should be configuration_limit_exceeded error
            return Err(PgError::Other(
                format!(
                    "{} storage is full. Current size limit: {}. \
                    Please, increase storage limit using: \
                    UPDATE \"_pico_property\" SET \"value\" = <new-limit> WHERE \"key\" = '{}';",
                    self.value_kind(),
                    capacity,
                    self.context.capacity_property
                )
                .into(),
            ));
        }

        if key.1.is_empty() {
            // Updates are only allowed for unnamed statements and portals.
            self.map.insert(key, value);
            return Ok(());
        }

        let value_name = self.value_kind();
        match self.map.entry(key) {
            Entry::Occupied(entry) => {
                let (id, name) = entry.key();
                Err(PgError::WithExplicitCode(
                    self.context.dublicate_key_error_code,
                    format!("{} \'{name}\' for client {id} already exists", value_name),
                ))
            }
            Entry::Vacant(entry) => {
                entry.insert(value);
                Ok(())
            }
        }
    }

    pub fn remove(&mut self, key: &(ClientId, Rc<str>)) -> Option<S> {
        self.map.remove(key)
    }

    pub fn remove_by_client_id(&mut self, id: ClientId) {
        self.map.retain(|k, _| k.0 != id)
    }

    pub fn names_by_client_id(&self, id: ClientId) -> Vec<Rc<str>> {
        let range = (
            Included((id, Rc::clone(&self.empty_name))),
            Excluded((id + 1, Rc::clone(&self.empty_name))),
        );
        self.map
            .range(range)
            .map(|((_, name), _)| Rc::clone(name))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    fn value_kind(&self) -> &'static str {
        match self.context.capacity_property {
            PropertyName::MaxPgStatements => "Statement",
            PropertyName::MaxPgPortals => "Portal",
            prop => panic!("unexpected property for pg storage: {prop}"),
        }
    }
}

type StatementStorage = PgStorage<StatementHolder>;

impl StatementStorage {
    fn new() -> Self {
        PgStorage::with_context(StorageContext::statements())
    }

    pub fn get(&self, key: &(ClientId, Rc<str>)) -> Option<Statement> {
        self.map.get(key).map(|h| h.statement.clone())
    }
}

impl Default for StatementStorage {
    fn default() -> Self {
        Self::new()
    }
}

type PortalStorage = PgStorage<Portal>;

impl PortalStorage {
    pub fn new() -> Self {
        PgStorage::with_context(StorageContext::portals())
    }

    pub fn remove_portals_by_statement(&mut self, statement: &Statement) {
        self.map
            .retain(|_, p| !Statement::ptr_eq(p.statement(), statement));
    }
}

impl Default for PortalStorage {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    pub static PG_STATEMENTS: Rc<RefCell<StatementStorage>> = Rc::new(RefCell::new(StatementStorage::new()));
    pub static PG_PORTALS: Rc<RefCell<PortalStorage>> = Rc::new(RefCell::new(PortalStorage::new()));
}

pub fn with_portals_mut<T, F>(key: (ClientId, Rc<str>), f: F) -> PgResult<T>
where
    F: FnOnce(&mut Portal) -> PgResult<T>,
{
    let mut portal: Portal = PG_PORTALS.with(|storage| {
        storage.borrow_mut().remove(&key).ok_or(PgError::Other(
            format!("Couldn't find portal \'{}\'.", key.1).into(),
        ))
    })?;
    let result = f(&mut portal);
    // Statement could be closed while the processing portal was running,
    // so the portal wasn't in the storage and wasn't removed.
    // In that case there is no need to put it back.
    if !portal.statement().is_closed() {
        PG_PORTALS.with(|storage| storage.borrow_mut().put(key, portal))?;
    }
    result
}

pub type Oid = u32;

#[derive(Debug, Default)]
pub struct StatementInner {
    id: String,
    // Query pattern used for opentelemetry.
    query_pattern: String,
    plan: Plan,
    describe: StatementDescribe,
    // true when the statement is deleted from the storage
    is_closed: Cell<bool>,
}

impl StatementInner {
    fn id(&self) -> &str {
        &self.id
    }

    fn new(
        id: String,
        query_pattern: String,
        plan: Plan,
        specified_param_oids: Vec<u32>,
    ) -> PgResult<Self> {
        let param_oids = derive_param_oids(&plan, specified_param_oids)?;
        let describe = StatementDescribe::new(Describe::new(&plan)?, param_oids);
        Ok(Self {
            id,
            query_pattern,
            plan,
            describe,
            is_closed: Cell::new(false),
        })
    }

    fn plan(&self) -> &Plan {
        &self.plan
    }

    fn query_pattern(&self) -> &str {
        &self.query_pattern
    }

    fn describe(&self) -> &StatementDescribe {
        &self.describe
    }

    fn is_closed(&self) -> bool {
        self.is_closed.get()
    }
}

#[derive(Debug, Clone, Default)]
pub struct Statement(Rc<StatementInner>);

impl Statement {
    pub fn id(&self) -> &str {
        self.0.id()
    }

    pub fn new(
        id: String,
        sql: String,
        plan: Plan,
        specified_param_oids: Vec<u32>,
    ) -> PgResult<Self> {
        Ok(Self(Rc::new(StatementInner::new(
            id,
            sql,
            plan,
            specified_param_oids,
        )?)))
    }

    pub fn plan(&self) -> &Plan {
        self.0.plan()
    }

    pub fn query_pattern(&self) -> &str {
        self.0.query_pattern()
    }

    pub fn describe(&self) -> &StatementDescribe {
        self.0.describe()
    }

    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    pub fn set_is_closed(&self, is_closed: bool) {
        self.0.is_closed.replace(is_closed);
    }

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
}

impl From<Statement> for StatementHolder {
    fn from(value: Statement) -> Self {
        Self::new(value)
    }
}

impl Drop for StatementHolder {
    fn drop(&mut self) {
        self.statement.set_is_closed(true);
        // The storage may have already been dropped, so we cannot access PG_PORTALS.
        if let Some(portals) = self.portals.upgrade() {
            portals
                .borrow_mut()
                .remove_portals_by_statement(&self.statement)
        }
    }
}

// TODO: use const from pgwire once pgproto is merged to picodata
const TEXT_OID: u32 = 25;

fn derive_param_oids(plan: &Plan, mut param_oids: Vec<Oid>) -> PgResult<Vec<Oid>> {
    let params_count = plan.get_param_set().len();
    if params_count < param_oids.len() {
        return Err(PgError::ProtocolViolation(format!(
            "query has {} parameters, but {} were given",
            params_count,
            param_oids.len()
        )));
    }

    // Postgres derives oids of unspecified parameters depending on the context.
    // If oid cannot be derived, it is treated as text.
    let text_oid = TEXT_OID;
    param_oids.iter_mut().for_each(|oid| {
        // 0 means unspecified type
        if *oid == 0 {
            *oid = text_oid;
        }
    });
    param_oids.resize(params_count, text_oid);
    Ok(param_oids)
}

#[derive(Debug, Default)]
pub struct Portal {
    plan: Plan,
    statement: Statement,
    describe: PortalDescribe,
    state: PortalState,
}

#[derive(Debug, Default)]
enum PortalState {
    #[default]
    /// Portal has just been created.
    NotStarted,
    /// Portal has been executed and contains rows to be sent in batches.
    StreamingRows(IntoIter<Vec<PgValue>>),
    /// Portal has been executed and contains a result ready to be sent.
    ResultReady(ExecuteResult),
    /// Portal has been executed, and a result has been sent.
    Done,
}

/// Get rows from dql-like(dql or explain) query execution result.
fn get_rows_from_tuple(tuple: &Tuple) -> PgResult<Vec<Vec<Value>>> {
    #[derive(Deserialize, Default, Debug)]
    struct DqlResult {
        rows: Vec<Vec<Value>>,
    }
    if let Ok(Some(res)) = tuple.field::<DqlResult>(0) {
        return Ok(res.rows);
    }

    // Try to parse explain result.
    if let Ok(Some(res)) = tuple.field::<Vec<Value>>(0) {
        return Ok(res.into_iter().map(|row| vec![row]).collect());
    }

    Err(PgError::InternalError(
        "couldn't get rows from the result tuple".into(),
    ))
}

/// Get row_count from result tuple.
fn get_row_count_from_tuple(tuple: &Tuple) -> PgResult<usize> {
    #[derive(Deserialize)]
    struct RowCount {
        row_count: usize,
    }
    let res: RowCount = tuple.field(0)?.ok_or(PgError::InternalError(
        "couldn't get row count from the result tuple".into(),
    ))?;

    Ok(res.row_count)
}

fn mp_row_into_pg_row(mp: Vec<Value>, metadata: &[MetadataColumn]) -> PgResult<Vec<PgValue>> {
    zip(mp, metadata)
        .map(|(v, col)| PgValue::try_from_rmpv(v, &col.ty))
        .collect()
}

impl Portal {
    pub fn new(
        plan: Plan,
        statement: Statement,
        output_format: Vec<FieldFormat>,
    ) -> PgResult<Self> {
        let stmt_describe = statement.describe();
        let describe = PortalDescribe::new(stmt_describe.describe.clone(), output_format);
        Ok(Self {
            plan,
            statement,
            describe,
            state: PortalState::NotStarted,
        })
    }

    pub fn execute(&mut self, max_rows: usize) -> PgResult<ExecuteResult> {
        loop {
            match &mut self.state {
                PortalState::NotStarted => self.start()?,
                PortalState::ResultReady(_) => {
                    let state = std::mem::replace(&mut self.state, PortalState::Done);
                    match state {
                        PortalState::ResultReady(result) => return Ok(result),
                        _ => unreachable!(),
                    }
                }
                PortalState::StreamingRows(ref mut stored_rows) => {
                    let taken: Vec<_> = stored_rows.take(max_rows).collect();
                    let row_count = taken.len();
                    let rows = Rows::new(taken, self.describe.row_info());
                    if stored_rows.len() == 0 {
                        self.state = PortalState::Done;
                        return Ok(ExecuteResult::FinishedDql {
                            rows,
                            tag: self.describe.command_tag(),
                            row_count,
                        });
                    }
                    return Ok(ExecuteResult::SuspendedDql { rows });
                }
                _ => {
                    return Err(PgError::Other(
                        format!("Can't execute portal in state {:?}", self.state).into(),
                    ));
                }
            }
        }
    }

    fn start(&mut self) -> PgResult<()> {
        let runtime = RouterRuntime::new()?;
        let query = Query::from_parts(
            self.plan.is_explain(),
            // XXX: the router runtime cache contains only unbinded IR plans to
            // speed up SQL parsing and metadata resolution. We need to clone the
            // plan here as its IR would be mutated during query execution (bind,
            // optimization, dispatch steps). Otherwise we'll polute the parsing
            // cache entry.
            ExecutionPlan::from(self.plan.clone()),
            &runtime,
            HashMap::new(),
        );
        let tuple = dispatch(query)?;
        self.state = match self.describe().query_type() {
            QueryType::Acl | QueryType::Ddl => {
                let tag = self.describe().command_tag();
                PortalState::ResultReady(ExecuteResult::AclOrDdl { tag })
            }
            QueryType::Dml => {
                let row_count = get_row_count_from_tuple(&tuple)?;
                let tag = self.describe().command_tag();
                PortalState::ResultReady(ExecuteResult::Dml { row_count, tag })
            }
            QueryType::Dql | QueryType::Explain => {
                let mp_rows = get_rows_from_tuple(&tuple)?;
                let metadata = self.describe.metadata();
                let pg_rows = mp_rows
                    .into_iter()
                    .map(|row| mp_row_into_pg_row(row, metadata))
                    .collect::<PgResult<Vec<Vec<_>>>>()?;
                PortalState::StreamingRows(pg_rows.into_iter())
            }
        };
        Ok(())
    }

    pub fn describe(&self) -> &PortalDescribe {
        &self.describe
    }

    pub fn statement(&self) -> &Statement {
        &self.statement
    }
}

#[derive(Debug, Serialize)]
pub struct UserStatementNames {
    // Available statements for the client with the given client id.
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
    // Available portals for the client with the given client id.
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
