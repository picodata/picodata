use super::{
    describe::{Describe, MetadataColumn, PortalDescribe, QueryType, StatementDescribe},
    result::{ExecuteResult, Rows},
};
use crate::{
    pgproto::{
        error::{PgError, PgErrorCode, PgResult},
        value::{FieldFormat, PgValue},
    },
    sql::{dispatch, router::RouterRuntime},
    storage::PropertyName,
    tlog,
    traft::node,
};
use postgres_types::{Oid, Type as PgType};
use rmpv::Value;
use sbroad::{
    executor::{ir::ExecutionPlan, Query},
    ir::{relation::Type as SbroadType, Plan},
};
use serde::{Deserialize, Serialize};
use std::{
    cell::{Cell, RefCell},
    collections::{btree_map::Entry, BTreeMap, HashMap},
    iter::zip,
    ops::Bound,
    os::raw::c_int,
    rc::{Rc, Weak},
    vec::IntoIter,
};
use tarantool::{
    proc::{Return, ReturnMsgpack},
    tuple::{FunctionCtx, Tuple},
};

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
            Bound::Included((id, Rc::clone(&self.empty_name))),
            Bound::Excluded((id + 1, Rc::clone(&self.empty_name))),
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
        let context = StorageContext::statements();
        let capacity = context.get_capacity().expect("storage capacity");
        tlog!(Info, "creating statement storage with capacity {capacity}");
        PgStorage::with_context(context)
    }

    pub fn get(&self, key: &(ClientId, Rc<str>)) -> Option<Statement> {
        self.map.get(key).map(|h| h.statement.clone())
    }
}

type PortalStorage = PgStorage<Portal>;

impl PortalStorage {
    pub fn new() -> Self {
        let context = StorageContext::portals();
        let capacity = context.get_capacity().expect("storage capacity");
        tlog!(Info, "creating portal storage with capacity {capacity}");
        PgStorage::with_context(context)
    }

    pub fn remove_portals_by_statement(&mut self, statement: &Statement) {
        self.map
            .retain(|_, p| !Statement::ptr_eq(p.statement(), statement));
    }
}

// TODO: those shoudn't be global variables; move them to some context (backend?).
thread_local! {
    pub static PG_STATEMENTS: Rc<RefCell<StatementStorage>> = Rc::new(RefCell::new(StatementStorage::new()));
    pub static PG_PORTALS: Rc<RefCell<PortalStorage>> = Rc::new(RefCell::new(PortalStorage::new()));
}

/// Eagerly initialize storages for prepared statements and portals.
pub fn force_init_portals_and_statements() {
    PG_STATEMENTS.with(|_| {});
    PG_PORTALS.with(|_| {});
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
        mut plan: Plan,
        specified_param_oids: Vec<u32>,
    ) -> PgResult<Self> {
        let param_oids = derive_param_oids(&mut plan, specified_param_oids)?;
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

pub(super) fn sbroad_type_to_pg(ty: &SbroadType) -> PgResult<postgres_types::Type> {
    match ty {
        SbroadType::Boolean => Ok(PgType::BOOL),
        SbroadType::Decimal => Ok(PgType::NUMERIC),
        SbroadType::Double | SbroadType::Number => Ok(PgType::FLOAT8),
        SbroadType::Integer | SbroadType::Unsigned => Ok(PgType::INT8),
        SbroadType::String => Ok(PgType::TEXT),
        SbroadType::Uuid => Ok(PgType::UUID),
        SbroadType::Map | SbroadType::Array | SbroadType::Any => Ok(PgType::JSON),
        SbroadType::Datetime => Ok(PgType::TIMESTAMPTZ),
        unsupported_type => Err(PgError::FeatureNotSupported(unsupported_type.to_string())),
    }
}

pub(super) fn pg_type_to_sbroad(ty: &PgType) -> PgResult<SbroadType> {
    match ty {
        &PgType::BOOL => Ok(SbroadType::Boolean),
        &PgType::NUMERIC => Ok(SbroadType::Decimal),
        &PgType::FLOAT8 => Ok(SbroadType::Double),
        &PgType::INT8 | &PgType::INT4 | &PgType::INT2 => Ok(SbroadType::Integer),
        &PgType::TEXT | &PgType::VARCHAR => Ok(SbroadType::String),
        &PgType::UUID => Ok(SbroadType::Uuid),
        &PgType::TIMESTAMPTZ => Ok(SbroadType::Datetime),
        unsupported_type => Err(PgError::FeatureNotSupported(unsupported_type.to_string())),
    }
}

pub fn derive_param_oids(plan: &mut Plan, param_oids: Vec<Oid>) -> PgResult<Vec<Oid>> {
    let client_types = param_oids
        .iter()
        .map(|oid| {
            PgType::from_oid(*oid)
                .map(|ty| {
                    pg_type_to_sbroad(&ty)
                        .map_err(|_| PgError::FeatureNotSupported(format!("{ty} parameters")))
                })
                .transpose()
        })
        .collect::<PgResult<Vec<_>>>()?;
    let inferred_types = plan.infer_pg_parameters_types(&client_types)?;
    let mut oids = inferred_types
        .iter()
        .map(|ty| sbroad_type_to_pg(ty).map(|pg| pg.oid()))
        .collect::<PgResult<Vec<_>>>()?;
    // Sbroad does not support PgType::INT2 and PgType::INT4 types, so we map them to
    // Sborad::Integer (8-byte integer), which is then mapped back to PgType::INT8, potentially
    // losing the original type information. Therefore, we need to restore the original types.
    for (n, oid) in param_oids.iter().enumerate() {
        if *oid == PgType::INT8.oid() || *oid == PgType::INT4.oid() || *oid == PgType::INT2.oid() {
            assert!(inferred_types[n] == SbroadType::Integer);
            oids[n] = *oid;
        }
    }

    Ok(oids)
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
            assert!(sbroad_type_to_pg(&sbroad).unwrap() == expected_pg)
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
