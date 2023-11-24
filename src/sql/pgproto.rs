//! PostgreSQL protocol.

use crate::traft::error::Error;
use crate::traft::{self, node};
use ::tarantool::tuple::Tuple;
use rmpv::Value;
use sbroad::errors::{Entity, SbroadError};
use sbroad::executor::ir::ExecutionPlan;
use sbroad::executor::result::MetadataColumn;
use sbroad::executor::Query;
use sbroad::ir::acl::{Acl, GrantRevokeType};
use sbroad::ir::block::Block;
use sbroad::ir::ddl::Ddl;
use sbroad::ir::expression::Expression;
use sbroad::ir::operator::Relational;
use sbroad::ir::{Node, Plan};
use serde::{Deserialize, Serialize};
use serde_repr::Serialize_repr;
use std::cell::{Cell, RefCell};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Included};
use std::os::raw::c_int;
use std::rc::Rc;
use std::vec::IntoIter;
use tarantool::proc::{Return, ReturnMsgpack};
use tarantool::tuple::FunctionCtx;

use super::dispatch;
use super::router::RouterRuntime;

pub const DEFAULT_MAX_PG_STATEMENTS: usize = 50;
pub const DEFAULT_MAX_PG_PORTALS: usize = 50;

pub type ClientId = u32;

/// Storage for Statements and Portals.
/// Essentially a map with custom logic for updates and unnamed elements.
struct PgStorage<S> {
    map: BTreeMap<(ClientId, Rc<str>), S>,
    capacity: usize,
    // We reuse this empty name in range scans in order to avoid Rc allocations.
    // Note: see names_by_client_id.
    empty_name: Rc<str>,
}

impl<S> PgStorage<S> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: BTreeMap::new(),
            capacity,
            empty_name: Rc::from(String::new()),
        }
    }

    pub fn put(&mut self, key: (ClientId, Rc<str>), value: S) -> traft::Result<()> {
        if self.len() >= self.capacity() {
            return Err(Error::Other("Statement storage is full".into()));
        }

        if key.1.is_empty() {
            // Updates are only allowed for unnamed statements and portals.
            self.map.insert(key, value);
            return Ok(());
        }

        match self.map.entry(key) {
            Entry::Occupied(entry) => {
                let (id, name) = entry.key();
                Err(Error::Other(
                    format!("Duplicated name \'{name}\' for client {id}").into(),
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

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

pub struct StatementStorage(PgStorage<StatementHolder>);

impl StatementStorage {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(PgStorage::with_capacity(capacity))
    }

    pub fn new() -> Self {
        let storage = match node::global() {
            Ok(node) => &node.storage,
            Err(_) => return Self::with_capacity(DEFAULT_MAX_PG_STATEMENTS),
        };
        match storage.properties.max_pg_portals() {
            Ok(size) => Self::with_capacity(size),
            Err(_) => Self::with_capacity(DEFAULT_MAX_PG_STATEMENTS),
        }
    }

    pub fn put(&mut self, key: (ClientId, Rc<str>), statement: Statement) -> traft::Result<()> {
        self.0.put(key, StatementHolder(statement))
    }

    pub fn get(&self, key: &(ClientId, Rc<str>)) -> Option<Statement> {
        self.0.map.get(key).map(|s| s.0.clone())
    }

    pub fn remove(&mut self, key: &(ClientId, Rc<str>)) {
        self.0.remove(key);
    }

    pub fn remove_by_client_id(&mut self, id: ClientId) {
        self.0.remove_by_client_id(id);
    }

    pub fn names_by_client_id(&self, id: ClientId) -> Vec<Rc<str>> {
        self.0.names_by_client_id(id)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }
}

impl Default for StatementStorage {
    fn default() -> Self {
        Self::new()
    }
}

pub struct PortalStorage(PgStorage<Portal>);

impl PortalStorage {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(PgStorage::with_capacity(capacity))
    }

    pub fn new() -> Self {
        let storage = match node::global() {
            Ok(node) => &node.storage,
            Err(_) => return Self::with_capacity(DEFAULT_MAX_PG_PORTALS),
        };
        match storage.properties.max_pg_portals() {
            Ok(size) => Self::with_capacity(size),
            Err(_) => Self::with_capacity(DEFAULT_MAX_PG_PORTALS),
        }
    }

    pub fn put(&mut self, key: (ClientId, Rc<str>), portal: Portal) -> traft::Result<()> {
        self.0.put(key, portal)?;
        Ok(())
    }

    pub fn remove(&mut self, key: &(ClientId, Rc<str>)) -> Option<Portal> {
        self.0.remove(key)
    }

    pub fn remove_by_client_id(&mut self, id: ClientId) {
        self.0.remove_by_client_id(id);
    }

    pub fn remove_portals_by_statement(&mut self, statement: &Statement) {
        self.0
            .map
            .retain(|_, p| !Statement::ptr_eq(p.statement(), statement));
    }

    pub fn names_by_client_id(&self, id: ClientId) -> Vec<Rc<str>> {
        self.0.names_by_client_id(id)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.0.capacity()
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

pub fn with_portals_mut<T, F>(key: (ClientId, Rc<str>), f: F) -> traft::Result<T>
where
    F: FnOnce(&mut Portal) -> traft::Result<T>,
{
    let mut portal: Portal = PG_PORTALS.with(|storage| {
        storage.borrow_mut().remove(&key).ok_or(Error::Other(
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
    ) -> Result<Self, Error> {
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
    ) -> Result<Self, Error> {
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
pub struct StatementHolder(Statement);

impl Drop for StatementHolder {
    fn drop(&mut self) {
        self.0.set_is_closed(true);
        PG_PORTALS.with(|storage| storage.borrow_mut().remove_portals_by_statement(&self.0));
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct StatementDescribe {
    #[serde(flatten)]
    describe: Describe,
    param_oids: Vec<Oid>,
}

impl StatementDescribe {
    fn new(describe: Describe, param_oids: Vec<Oid>) -> Self {
        Self {
            describe,
            param_oids,
        }
    }
}

// TODO: use const from pgwire once pgproto is merged to picodata
const TEXT_OID: u32 = 25;

fn derive_param_oids(plan: &Plan, mut param_oids: Vec<Oid>) -> Result<Vec<Oid>, Error> {
    let params_count = plan.get_param_set().len();
    if params_count < param_oids.len() {
        return Err(Error::Other(
            format!(
                "query has {} parameters, but {} were given",
                params_count,
                param_oids.len()
            )
            .into(),
        ));
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
    NotStarted,
    Running(IntoIter<Value>),
    Finished(Option<Tuple>),
}

/// Try to get rows from the query execution result.
/// Some is returned for dql-like queires (dql or explain), otherwise None is returned.
fn tuple_as_rows(tuple: &Tuple) -> Option<Vec<Value>> {
    #[derive(Deserialize, Default, Debug)]
    struct DqlResult {
        rows: Vec<Value>,
    }
    if let Ok(Some(res)) = tuple.field::<DqlResult>(0) {
        return Some(res.rows);
    }

    // Try to parse explain result.
    if let Ok(Some(res)) = tuple.field::<Vec<Value>>(0) {
        return Some(res);
    }

    None
}

fn take_rows(rows: &mut IntoIter<Value>, max_rows: usize) -> traft::Result<Tuple> {
    let is_finished = rows.len() <= max_rows;
    let rows = rows.take(max_rows).collect();
    #[derive(Serialize)]
    struct RunningResult {
        rows: Vec<Value>,
        is_finished: bool,
    }
    let result = RunningResult { rows, is_finished };
    let mp = rmp_serde::to_vec_named(&vec![result]).map_err(|e| Error::Other(e.into()))?;
    let ret = Tuple::try_from_slice(&mp).map_err(|e| Error::Other(e.into()))?;
    Ok(ret)
}

impl Portal {
    pub fn new(plan: Plan, statement: Statement, output_format: Vec<u8>) -> Result<Self, Error> {
        let stmt_describe = statement.describe();
        let describe = PortalDescribe::new(stmt_describe.describe.clone(), output_format);
        Ok(Self {
            plan,
            statement,
            describe,
            state: PortalState::NotStarted,
        })
    }

    pub fn execute(&mut self, max_rows: usize) -> traft::Result<Tuple> {
        loop {
            match &mut self.state {
                PortalState::NotStarted => self.start()?,
                PortalState::Finished(Some(res)) => {
                    // clone only increments tuple's refcounter
                    let res = res.clone();
                    self.state = PortalState::Finished(None);
                    return Ok(res);
                }
                PortalState::Running(ref mut rows) => {
                    let res = take_rows(rows, max_rows)?;
                    if rows.len() == 0 {
                        self.state = PortalState::Finished(None);
                    }
                    return Ok(res);
                }
                _ => {
                    return Err(Error::Other(
                        format!("Can't execute portal in state {:?}", self.state).into(),
                    ))
                }
            }
        }
    }

    fn start(&mut self) -> traft::Result<()> {
        let runtime = RouterRuntime::new().map_err(Error::from)?;
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
        let res = dispatch(query)?;
        if let Some(rows) = tuple_as_rows(&res) {
            self.state = PortalState::Running(rows.into_iter());
        } else {
            self.state = PortalState::Finished(Some(res));
        }
        Ok(())
    }

    pub fn describe(&self) -> &PortalDescribe {
        &self.describe
    }

    pub fn statement(&self) -> &Statement {
        &self.statement
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PortalDescribe {
    #[serde(flatten)]
    describe: Describe,
    output_format: Vec<u8>,
}

impl PortalDescribe {
    fn new(describe: Describe, output_format: Vec<u8>) -> Self {
        Self {
            describe,
            output_format,
        }
    }
}

/// Get an output format from the dql query plan.
fn dql_output_format(ir: &Plan) -> Result<Vec<MetadataColumn>, SbroadError> {
    // Get metadata (column types) from the top node's output tuple.
    let top_id = ir.get_top()?;
    let top_output_id = ir.get_relation_node(top_id)?.output();
    let columns = ir.get_row_list(top_output_id)?;
    let mut metadata = Vec::with_capacity(columns.len());
    for col_id in columns {
        let column = ir.get_expression_node(*col_id)?;
        let column_type = column.calculate_type(ir)?.to_string();
        let column_name = if let Expression::Alias { name, .. } = column {
            name.clone()
        } else {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format!("expected alias, got {column:?}")),
            ));
        };
        metadata.push(MetadataColumn::new(column_name, column_type));
    }
    Ok(metadata)
}

/// Get the output format of explain message.
fn explain_output_format() -> Vec<MetadataColumn> {
    vec![MetadataColumn::new("QUERY PLAN".into(), "string".into())]
}

#[derive(Debug, Clone, Default, Serialize_repr)]
#[repr(u8)]
pub enum QueryType {
    Acl = 0,
    Ddl = 1,
    Dml = 2,
    #[default]
    Dql = 3,
    Explain = 4,
}

#[derive(Clone, Debug, Default, Serialize_repr)]
#[repr(u8)]
pub enum CommandTag {
    AlterRole = 0,
    CallProcedure = 16,
    CreateProcedure = 14,
    CreateRole = 1,
    CreateTable = 2,
    DropProcedure = 15,
    DropRole = 3,
    DropTable = 4,
    Delete = 5,
    Explain = 6,
    Grant = 7,
    GrantRole = 8,
    Insert = 9,
    RenameRoutine = 17,
    Revoke = 10,
    RevokeRole = 11,
    #[default]
    Select = 12,
    Update = 13,
}

impl From<CommandTag> for QueryType {
    fn from(command_tag: CommandTag) -> Self {
        match command_tag {
            CommandTag::AlterRole
            | CommandTag::DropRole
            | CommandTag::CreateRole
            | CommandTag::Grant
            | CommandTag::GrantRole
            | CommandTag::Revoke
            | CommandTag::RevokeRole => QueryType::Acl,
            CommandTag::DropTable
            | CommandTag::CreateTable
            | CommandTag::CreateProcedure
            | CommandTag::RenameRoutine
            | CommandTag::DropProcedure => QueryType::Ddl,
            CommandTag::Delete
            | CommandTag::Insert
            | CommandTag::Update
            | CommandTag::CallProcedure => QueryType::Dml,
            CommandTag::Explain => QueryType::Explain,
            CommandTag::Select => QueryType::Dql,
        }
    }
}

impl TryFrom<&Node> for CommandTag {
    type Error = SbroadError;

    fn try_from(node: &Node) -> Result<Self, Self::Error> {
        match node {
            Node::Acl(acl) => match acl {
                Acl::DropRole { .. } | Acl::DropUser { .. } => Ok(CommandTag::DropRole),
                Acl::CreateRole { .. } | Acl::CreateUser { .. } => Ok(CommandTag::CreateRole),
                Acl::AlterUser { .. } => Ok(CommandTag::AlterRole),
                Acl::GrantPrivilege { grant_type, .. } => match grant_type {
                    GrantRevokeType::RolePass { .. } => Ok(CommandTag::GrantRole),
                    _ => Ok(CommandTag::Grant),
                },
                Acl::RevokePrivilege { revoke_type, .. } => match revoke_type {
                    GrantRevokeType::RolePass { .. } => Ok(CommandTag::RevokeRole),
                    _ => Ok(CommandTag::Revoke),
                },
            },
            Node::Block(block) => match block {
                Block::Procedure { .. } => Ok(CommandTag::CallProcedure),
            },
            Node::Ddl(ddl) => match ddl {
                Ddl::DropTable { .. } => Ok(CommandTag::DropTable),
                Ddl::CreateTable { .. } => Ok(CommandTag::CreateTable),
                Ddl::CreateProc { .. } => Ok(CommandTag::CreateProcedure),
                Ddl::DropProc { .. } => Ok(CommandTag::DropProcedure),
                Ddl::RenameRoutine { .. } => Ok(CommandTag::RenameRoutine),
            },
            Node::Relational(rel) => match rel {
                Relational::Delete { .. } => Ok(CommandTag::Delete),
                Relational::Insert { .. } => Ok(CommandTag::Insert),
                Relational::Update { .. } => Ok(CommandTag::Update),
                Relational::Except { .. }
                | Relational::Join { .. }
                | Relational::Motion { .. }
                | Relational::Projection { .. }
                | Relational::Intersect { .. }
                | Relational::ScanRelation { .. }
                | Relational::ScanSubQuery { .. }
                | Relational::Selection { .. }
                | Relational::GroupBy { .. }
                | Relational::Having { .. }
                | Relational::UnionAll { .. }
                | Relational::Values { .. }
                | Relational::ValuesRow { .. } => Ok(CommandTag::Select),
            },
            Node::Expression(_) | Node::Parameter => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("{node:?} can't be converted to CommandTag")),
            )),
        }
    }
}

/// Contains a query description used by pgproto.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Describe {
    pub command_tag: CommandTag,
    pub query_type: QueryType,
    /// Output columns format.
    pub metadata: Vec<MetadataColumn>,
}

impl Describe {
    #[inline]
    pub fn with_command_tag(mut self, command_tag: CommandTag) -> Self {
        self.command_tag = command_tag.clone();
        self.query_type = command_tag.into();
        self
    }

    #[inline]
    pub fn with_metadata(mut self, metadata: Vec<MetadataColumn>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn new(plan: &Plan) -> Result<Self, SbroadError> {
        let command_tag = if plan.is_explain() {
            CommandTag::Explain
        } else {
            let top = plan.get_top()?;
            let node = plan.get_node(top)?;
            CommandTag::try_from(node)?
        };
        let query_type = command_tag.clone().into();
        match query_type {
            QueryType::Acl | QueryType::Ddl | QueryType::Dml => {
                Ok(Describe::default().with_command_tag(command_tag))
            }
            QueryType::Dql => Ok(Describe::default()
                .with_command_tag(command_tag)
                .with_metadata(dql_output_format(plan)?)),
            QueryType::Explain => Ok(Describe::default()
                .with_command_tag(command_tag)
                .with_metadata(explain_output_format())),
        }
    }
}

impl Return for Describe {
    fn ret(self, ctx: FunctionCtx) -> c_int {
        ReturnMsgpack(self).ret(ctx)
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
