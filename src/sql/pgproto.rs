//! PostgreSQL protocol.

use crate::traft::error::Error;
use crate::traft::{self, node};
use crate::util::effective_user_id;
use sbroad::errors::{Entity, SbroadError};
use sbroad::executor::result::MetadataColumn;
use sbroad::ir::acl::{Acl, GrantRevokeType};
use sbroad::ir::ddl::Ddl;
use sbroad::ir::expression::Expression;
use sbroad::ir::operator::Relational;
use sbroad::ir::{Node, Plan};
use serde::Serialize;
use serde_repr::Serialize_repr;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::os::raw::c_int;
use std::rc::Rc;
use tarantool::proc::{Return, ReturnMsgpack};
use tarantool::session::UserId;
use tarantool::tuple::FunctionCtx;

pub const DEFAULT_MAX_PG_PORTALS: usize = 50;

pub type Descriptor = usize;

pub struct PortalStorage {
    map: BTreeMap<(UserId, Descriptor), BoxedPortal>,
    len: usize,
    capacity: usize,
}

impl PortalStorage {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: BTreeMap::new(),
            len: 0,
            capacity,
        }
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

    pub fn put(&mut self, key: Descriptor, boxed: BoxedPortal) -> traft::Result<()> {
        if self.len() >= self.capacity() {
            return Err(Error::Other("Portal storage is full".into()));
        }
        let user_id = boxed.portal().user_id();
        self.map.insert((user_id, key), boxed);
        self.len += 1;
        Ok(())
    }

    pub fn remove(&mut self, key: Descriptor) -> traft::Result<BoxedPortal> {
        let user_id = effective_user_id();
        let portal = self.map.remove(&(user_id, key)).map_or_else(
            || {
                Err(Error::Other(
                    format!("No such descriptor {key} for user {user_id}").into(),
                ))
            },
            Ok,
        )?;
        self.len -= 1;
        Ok(portal)
    }

    pub fn keys(&self) -> Vec<Descriptor> {
        let user_id = effective_user_id();
        self.map
            .range((
                Included((user_id, Descriptor::MIN)),
                Included((user_id, Descriptor::MAX)),
            ))
            .map(|((_, key), _)| *key)
            .collect()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Default for PortalStorage {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    pub static PG_PORTALS: Rc<RefCell<PortalStorage>> = Rc::new(RefCell::new(PortalStorage::new()));
}

pub fn with_portals<T, F>(key: Descriptor, f: F) -> traft::Result<T>
where
    F: FnOnce(&mut BoxedPortal) -> traft::Result<T>,
{
    // The f() closure may yield and requires a mutable access to the portal from the portal storage.
    // So we have to remove the portal from the storage, pass it to the closure and put it back
    // after the closure is done. Otherwise, the portal may be borrowed twice and produce a panic.
    let mut portal: BoxedPortal = PG_PORTALS.with(|storage| storage.borrow_mut().remove(key))?;
    let result = f(&mut portal);
    PG_PORTALS.with(|storage| storage.borrow_mut().put(key, portal))?;
    result
}

#[derive(Debug, Default)]
struct Portal {
    id: String,
    user_id: UserId,
    sql: String,
    plan: Plan,
}

impl Portal {
    fn id(&self) -> &str {
        &self.id
    }

    fn new(id: String, sql: String, plan: Plan) -> Self {
        let user_id = effective_user_id();
        Self {
            id,
            sql,
            plan,
            user_id,
        }
    }

    fn plan(&self) -> &Plan {
        &self.plan
    }

    fn plan_mut(&mut self) -> &mut Plan {
        &mut self.plan
    }

    fn sql(&self) -> &str {
        &self.sql
    }

    fn user_id(&self) -> UserId {
        self.user_id
    }
}

#[derive(Debug, Default)]
pub struct BoxedPortal(Box<Portal>);

impl BoxedPortal {
    pub fn new(id: String, sql: String, plan: Plan) -> Self {
        Self(Box::new(Portal::new(id, sql, plan)))
    }

    pub fn id(&self) -> &str {
        self.portal().id()
    }

    pub fn plan(&self) -> &Plan {
        self.portal().plan()
    }

    pub fn plan_mut(&mut self) -> &mut Plan {
        self.portal_mut().plan_mut()
    }

    pub fn sql(&self) -> &str {
        self.portal().sql()
    }

    pub fn descriptor(&self) -> Descriptor {
        &*(self.0) as *const Portal as Descriptor
    }

    fn portal(&self) -> &Portal {
        &self.0
    }

    fn portal_mut(&mut self) -> &mut Portal {
        &mut self.0
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

#[derive(Debug, Default, Serialize_repr)]
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
            | CommandTag::DropProcedure => QueryType::Ddl,
            CommandTag::Delete | CommandTag::Insert | CommandTag::Update => QueryType::Dml,
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
            Node::Ddl(ddl) => match ddl {
                Ddl::DropTable { .. } => Ok(CommandTag::DropTable),
                Ddl::CreateTable { .. } => Ok(CommandTag::CreateTable),
                Ddl::CreateProc { .. } => Ok(CommandTag::CreateProcedure),
                Ddl::DropProc { .. } => Ok(CommandTag::DropProcedure),
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
#[derive(Debug, Default, Serialize)]
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
pub struct UserDescriptors {
    available: Vec<Descriptor>,
    total: usize,
}

impl UserDescriptors {
    pub fn new() -> Self {
        PG_PORTALS.with(|storage| {
            let storage = storage.borrow();
            UserDescriptors {
                available: storage.keys(),
                total: storage.len(),
            }
        })
    }
}

impl Default for UserDescriptors {
    fn default() -> Self {
        Self::new()
    }
}

impl Return for UserDescriptors {
    fn ret(self, ctx: FunctionCtx) -> c_int {
        ReturnMsgpack(self).ret(ctx)
    }
}
