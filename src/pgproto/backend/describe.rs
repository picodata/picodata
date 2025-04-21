use crate::pgproto::{
    error::PgResult,
    value::{FieldFormat, RawFormat},
};
use pgwire::{
    api::results::FieldInfo,
    messages::data::{FieldDescription, RowDescription},
};
use postgres_types::{Oid, Type};
use sbroad::{
    errors::{Entity, SbroadError},
    ir::{
        acl::GrantRevokeType,
        node::{
            acl::Acl, block::Block, ddl::Ddl, expression::Expression, plugin::Plugin,
            relational::Relational, tcl::Tcl, Alias, GrantPrivilege, Node, RevokePrivilege,
        },
        relation::{DerivedType, Type as SbroadType},
        Plan,
    },
};
use serde::Serialize;
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::{collections::HashMap, iter::zip, os::raw::c_int};
use tarantool::{
    proc::{Return, ReturnMsgpack},
    tuple::FunctionCtx,
};

#[derive(Debug, Clone, Copy, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum QueryType {
    Acl = 0,
    Ddl = 1,
    Dml = 2,
    Dql = 3,
    Explain = 4,
    Empty = 5,
    Tcl = 6,
    Deallocate = 7,
}

#[derive(Debug, Clone, Copy, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum CommandTag {
    AddTrier = 37,
    AlterRole = 0,
    AlterSystem = 22,
    AlterTable = 41,
    Begin = 52,
    CallProcedure = 16,
    CreateProcedure = 14,
    CreateRole = 1,
    CreateTable = 2,
    CreateIndex = 18,
    CreatePlugin = 31,
    CreateSchema = 50,
    ChangeConfig = 39,
    Commit = 53,
    DropProcedure = 15,
    DropRole = 3,
    DropTable = 4,
    Deallocate = 48,
    DeallocateAll = 49,
    Delete = 5,
    DisablePlugin = 33,
    DropIndex = 19,
    DropPlugin = 34,
    DropSchema = 51,
    EnablePlugin = 32,
    EmptyQuery = 55,
    Explain = 6,
    Grant = 7,
    GrantRole = 8,
    Insert = 9,
    MigrateUp = 35,
    RemoveTier = 38,
    RenameRoutine = 17,
    Revoke = 10,
    Rollback = 54,
    RevokeRole = 11,
    Select = 12,
    SetParam = 20,
    SetTransaction = 21,
    TruncateTable = 40,
    Update = 13,
}

impl CommandTag {
    /// Note: Should be in accordance with
    ///       `<https://github.com/postgres/postgres/blob/master/src/include/tcop/cmdtaglist.h>`
    pub fn as_str(&self) -> &str {
        match *self {
            Self::AlterRole => "ALTER ROLE",
            Self::AlterSystem => "ALTER SYSTEM",
            Self::CreateRole => "CREATE ROLE",
            Self::CreateSchema => "CREATE SCHEMA",
            Self::CreateTable => "CREATE TABLE",
            Self::CreateIndex => "CREATE INDEX",
            Self::Deallocate => "DEALLOCATE",
            Self::DeallocateAll => "DEALLOCATE ALL",
            Self::DropRole => "DROP ROLE",
            Self::DropSchema => "DROP SCHEMA",
            Self::DropTable => "DROP TABLE",
            Self::TruncateTable => "TRUNCATE TABLE",
            Self::AlterTable => "ALTER TABLE",
            Self::DropIndex => "DROP INDEX",
            Self::Delete => "DELETE",
            Self::Explain => "EXPLAIN",
            Self::Grant => "GRANT",
            Self::GrantRole => "GRANT ROLE",
            // ** from postgres sources **
            // In PostgreSQL versions 11 and earlier, it was possible to create a
            // table WITH OIDS.  When inserting into such a table, INSERT used to
            // include the Oid of the inserted record in the completion tag.  To
            // maintain compatibility in the wire protocol, we now write a "0" (for
            // InvalidOid) in the location where we once wrote the new record's Oid.
            Self::Insert => "INSERT 0",
            Self::Revoke => "REVOKE",
            Self::RevokeRole => "REVOKE ROLE",
            Self::Select => "SELECT",
            Self::Update => "UPDATE",
            Self::CreateProcedure => "CREATE PROCEDURE",
            Self::DropProcedure => "DROP PROCEDURE",
            Self::CallProcedure => "CALL",
            Self::RenameRoutine => "RENAME ROUTINE",
            Self::SetParam | Self::SetTransaction => "SET",
            Self::CreatePlugin => "CREATE PLUGIN",
            Self::EnablePlugin => "ALTER PLUGIN ENABLE",
            Self::DisablePlugin => "ALTER PLUGIN DISABLE",
            Self::DropPlugin => "DROP PLUGIN",
            Self::MigrateUp => "ALTER PLUGIN MIGRATE TO",
            Self::AddTrier => "ALTER PLUGIN ADD SERVICE TO TIER",
            Self::RemoveTier => "ALTER PLUGIN REMOVE SERVICE FROM TIER",
            Self::ChangeConfig => "ALTER PLUGIN SET",
            Self::Begin => "BEGIN",
            Self::Commit => "COMMIT",
            Self::Rollback => "ROLLBACK",
            // Response on an empty query is EmptyQueryResponse with no tag.
            // https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-EMPTYQUERYRESPONSE
            Self::EmptyQuery => "",
        }
    }
}

impl From<CommandTag> for QueryType {
    fn from(command_tag: CommandTag) -> Self {
        match command_tag {
            CommandTag::AlterRole
            | CommandTag::AlterSystem
            | CommandTag::DropRole
            | CommandTag::CreateRole
            | CommandTag::Grant
            | CommandTag::GrantRole
            | CommandTag::Revoke
            | CommandTag::RevokeRole => QueryType::Acl,
            CommandTag::DropTable
            | CommandTag::TruncateTable
            | CommandTag::AlterTable
            | CommandTag::CreateTable
            | CommandTag::CreateProcedure
            | CommandTag::CreateIndex
            | CommandTag::CreateSchema
            | CommandTag::RenameRoutine
            | CommandTag::DropIndex
            | CommandTag::DropSchema
            | CommandTag::SetParam
            | CommandTag::SetTransaction
            | CommandTag::CreatePlugin
            | CommandTag::DropPlugin
            | CommandTag::EnablePlugin
            | CommandTag::DisablePlugin
            | CommandTag::MigrateUp
            | CommandTag::AddTrier
            | CommandTag::RemoveTier
            | CommandTag::ChangeConfig
            | CommandTag::DropProcedure => QueryType::Ddl,
            CommandTag::Delete
            | CommandTag::Insert
            | CommandTag::Update
            | CommandTag::CallProcedure => QueryType::Dml,
            CommandTag::Explain => QueryType::Explain,
            CommandTag::Select => QueryType::Dql,
            CommandTag::Deallocate | CommandTag::DeallocateAll => QueryType::Deallocate,
            CommandTag::Begin | CommandTag::Commit | CommandTag::Rollback => QueryType::Tcl,
            CommandTag::EmptyQuery => QueryType::Empty,
        }
    }
}

impl TryFrom<&Node<'_>> for CommandTag {
    type Error = SbroadError;

    fn try_from(node: &Node) -> Result<Self, Self::Error> {
        match node {
            Node::Acl(acl) => match acl {
                Acl::DropRole { .. } | Acl::DropUser { .. } => Ok(CommandTag::DropRole),
                Acl::CreateRole { .. } | Acl::CreateUser { .. } => Ok(CommandTag::CreateRole),
                Acl::AlterUser { .. } => Ok(CommandTag::AlterRole),
                Acl::GrantPrivilege(GrantPrivilege { grant_type, .. }) => match grant_type {
                    GrantRevokeType::RolePass { .. } => Ok(CommandTag::GrantRole),
                    _ => Ok(CommandTag::Grant),
                },
                Acl::RevokePrivilege(RevokePrivilege { revoke_type, .. }) => match revoke_type {
                    GrantRevokeType::RolePass { .. } => Ok(CommandTag::RevokeRole),
                    _ => Ok(CommandTag::Revoke),
                },
            },
            Node::Block(block) => match block {
                Block::Procedure { .. } => Ok(CommandTag::CallProcedure),
            },
            Node::Ddl(ddl) => match ddl {
                Ddl::AlterSystem { .. } => Ok(CommandTag::AlterSystem),
                Ddl::DropTable { .. } => Ok(CommandTag::DropTable),
                Ddl::TruncateTable { .. } => Ok(CommandTag::TruncateTable),
                Ddl::CreateTable { .. } => Ok(CommandTag::CreateTable),
                Ddl::CreateProc { .. } => Ok(CommandTag::CreateProcedure),
                Ddl::CreateIndex { .. } => Ok(CommandTag::CreateIndex),
                Ddl::CreateSchema => Ok(CommandTag::CreateSchema),
                Ddl::DropSchema => Ok(CommandTag::DropSchema),
                Ddl::DropProc { .. } => Ok(CommandTag::DropProcedure),
                Ddl::DropIndex { .. } => Ok(CommandTag::DropIndex),
                Ddl::RenameRoutine { .. } => Ok(CommandTag::RenameRoutine),
                Ddl::SetParam { .. } => Ok(CommandTag::SetParam),
                Ddl::SetTransaction { .. } => Ok(CommandTag::SetTransaction),
                Ddl::AlterTable(..) => Ok(CommandTag::AlterTable),
            },
            Node::Tcl(tcl) => match tcl {
                Tcl::Begin => Ok(CommandTag::Begin),
                Tcl::Commit => Ok(CommandTag::Commit),
                Tcl::Rollback => Ok(CommandTag::Rollback),
            },
            Node::Plugin(plugin) => match plugin {
                Plugin::Create { .. } => Ok(CommandTag::CreatePlugin),
                Plugin::Drop { .. } => Ok(CommandTag::DropPlugin),
                Plugin::Enable { .. } => Ok(CommandTag::EnablePlugin),
                Plugin::Disable { .. } => Ok(CommandTag::DisablePlugin),
                Plugin::MigrateTo { .. } => Ok(CommandTag::MigrateUp),
                Plugin::AppendServiceToTier { .. } => Ok(CommandTag::AddTrier),
                Plugin::RemoveServiceFromTier { .. } => Ok(CommandTag::RemoveTier),
                Plugin::ChangeConfig { .. } => Ok(CommandTag::ChangeConfig),
            },

            Node::Deallocate(deallocate) => match deallocate.name {
                Some(_) => Ok(CommandTag::Deallocate),
                None => Ok(CommandTag::DeallocateAll),
            },

            Node::Relational(rel) => match rel {
                Relational::Delete { .. } => Ok(CommandTag::Delete),
                Relational::Insert { .. } => Ok(CommandTag::Insert),
                Relational::Update { .. } => Ok(CommandTag::Update),
                Relational::Except { .. }
                | Relational::Join { .. }
                | Relational::Motion { .. }
                | Relational::Projection { .. }
                | Relational::NamedWindows { .. }
                | Relational::Intersect { .. }
                | Relational::ScanCte { .. }
                | Relational::ScanRelation { .. }
                | Relational::ScanSubQuery { .. }
                | Relational::Selection { .. }
                | Relational::GroupBy { .. }
                | Relational::OrderBy { .. }
                | Relational::Having { .. }
                | Relational::Union { .. }
                | Relational::UnionAll { .. }
                | Relational::Values { .. }
                | Relational::ValuesRow { .. }
                | Relational::SelectWithoutScan { .. }
                | Relational::Limit { .. } => Ok(CommandTag::Select),
            },
            Node::Invalid(_) | Node::Expression(_) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(smol_str::format_smolstr!(
                    "{node:?} can't be converted to CommandTag"
                )),
            )),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct MetadataColumn {
    pub name: String,
    pub ty: Type,
}

impl Serialize for MetadataColumn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let map = HashMap::from([(self.name.clone(), self.ty.to_string())]);
        map.serialize(serializer)
    }
}

impl MetadataColumn {
    fn new(name: String, ty: Type) -> Self {
        Self { name, ty }
    }
}

fn pg_type_from_sbroad(sbroad: &DerivedType) -> Type {
    if let Some(sbroad) = sbroad.get() {
        match sbroad {
            SbroadType::Integer | SbroadType::Unsigned => Type::INT8,
            SbroadType::Map | SbroadType::Array | SbroadType::Any => Type::JSON,
            SbroadType::String => Type::TEXT,
            SbroadType::Boolean => Type::BOOL,
            SbroadType::Double => Type::FLOAT8,
            SbroadType::Decimal => Type::NUMERIC,
            SbroadType::Uuid => Type::UUID,
            SbroadType::Datetime => Type::TIMESTAMPTZ,
        }
    } else {
        Type::UNKNOWN
    }
}

/// Get an output format from the dql query plan.
fn dql_output_format(ir: &Plan) -> PgResult<Vec<MetadataColumn>> {
    // Get metadata (column types) from the top node's output tuple.
    let top_id = ir.get_top()?;
    let top_output_id = ir.get_relation_node(top_id)?.output();
    let columns = ir.get_row_list(top_output_id)?;
    let mut metadata = Vec::with_capacity(columns.len());
    for col_id in columns {
        let column = ir.get_expression_node(*col_id)?;
        let column_type = column.calculate_type(ir)?;
        let column_name = if let Expression::Alias(Alias { name, .. }) = column {
            name.to_string()
        } else {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(smol_str::format_smolstr!("expected alias, got {column:?}")),
            )
            .into());
        };
        let ty = pg_type_from_sbroad(&column_type);
        metadata.push(MetadataColumn::new(column_name, ty));
    }
    Ok(metadata)
}

/// Get the output format of explain message.
fn explain_output_format() -> Vec<MetadataColumn> {
    vec![MetadataColumn::new("QUERY PLAN".into(), Type::TEXT)]
}

fn field_description(name: String, ty: Type, format: FieldFormat) -> FieldDescription {
    // ** From postgres sources **
    // resorigtbl/resorigcol identify the source of the column, if it is a
    // simple reference to a column of a base table (or view).  If it is not
    // a simple reference, these fields are zeroes.
    let resorigtbl = 0;
    let resorigcol = 0;

    // typmod records type-specific data supplied at table creation time
    // (for example, the max length of a varchar field).  The
    // value will generally be -1 for types that do not need typmod.
    let typemod = -1;

    let id = ty.oid();
    // TODO: add Type::len()
    let len = 0;

    FieldDescription::new(
        name,
        resorigtbl,
        resorigcol,
        id,
        len,
        typemod,
        format as RawFormat,
    )
}

/// Contains a query description used by pgproto.
#[derive(Debug, Clone, Serialize)]
pub struct Describe {
    pub command_tag: CommandTag,
    pub query_type: QueryType,
    /// Output columns format.
    pub metadata: Vec<MetadataColumn>,
}

impl Describe {
    pub fn new(plan: &Plan) -> PgResult<Self> {
        let command_tag = if plan.is_empty() {
            CommandTag::EmptyQuery
        } else if plan.is_explain() {
            CommandTag::Explain
        } else {
            let top = plan.get_top()?;
            let node = plan.get_node(top)?;
            CommandTag::try_from(&node)?
        };
        let query_type = command_tag.into();

        let metadata = match query_type {
            QueryType::Acl
            | QueryType::Ddl
            | QueryType::Dml
            | QueryType::Tcl
            | QueryType::Deallocate
            | QueryType::Empty => vec![],
            QueryType::Dql => dql_output_format(plan)?,
            QueryType::Explain => explain_output_format(),
        };

        Ok(Describe {
            command_tag,
            query_type,
            metadata,
        })
    }
}

impl Describe {
    pub fn query_type(&self) -> QueryType {
        self.query_type
    }

    pub fn command_tag(&self) -> CommandTag {
        self.command_tag
    }

    pub fn row_description(&self) -> Option<RowDescription> {
        match self.query_type() {
            QueryType::Acl
            | QueryType::Ddl
            | QueryType::Dml
            | QueryType::Deallocate
            | QueryType::Tcl
            | QueryType::Empty => None,

            QueryType::Dql | QueryType::Explain => {
                let row_description = self
                    .metadata
                    .iter()
                    .map(|col| {
                        field_description(col.name.clone(), col.ty.clone(), FieldFormat::Text)
                    })
                    .collect();

                Some(RowDescription::new(row_description))
            }
        }
    }
}

impl Return for Describe {
    fn ret(self, ctx: FunctionCtx) -> c_int {
        ReturnMsgpack(self).ret(ctx)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StatementDescribe {
    #[serde(flatten)]
    pub describe: Describe,
    pub param_oids: Vec<Oid>,
}

impl StatementDescribe {
    pub fn new(describe: Describe, param_oids: Vec<Oid>) -> Self {
        Self {
            describe,
            param_oids,
        }
    }

    pub fn query_type(&self) -> QueryType {
        self.describe.query_type()
    }
}

impl StatementDescribe {
    pub fn ncolumns(&self) -> usize {
        self.describe.metadata.len()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PortalDescribe {
    #[serde(flatten)]
    pub describe: Describe,
    #[serde(serialize_with = "PortalDescribe::serialize_output_format")]
    pub output_format: Vec<FieldFormat>,
}

impl PortalDescribe {
    pub fn new(describe: Describe, output_format: Vec<FieldFormat>) -> Self {
        Self {
            describe,
            output_format,
        }
    }

    fn serialize_output_format<S: serde::Serializer>(
        output_format: &[FieldFormat],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(output_format.len()))?;
        for format in output_format.iter().copied() {
            seq.serialize_element(&(format as RawFormat))?;
        }
        seq.end()
    }

    pub fn metadata(&self) -> &[MetadataColumn] {
        &self.describe.metadata
    }
}

impl PortalDescribe {
    pub fn row_description(&self) -> Option<RowDescription> {
        match self.query_type() {
            QueryType::Acl
            | QueryType::Ddl
            | QueryType::Dml
            | QueryType::Deallocate
            | QueryType::Tcl
            | QueryType::Empty => None,
            QueryType::Dql | QueryType::Explain => {
                let metadata = &self.describe.metadata;
                let output_format = &self.output_format;
                let row_description = zip(metadata, output_format)
                    .map(|(col, format)| {
                        field_description(col.name.clone(), col.ty.clone(), *format)
                    })
                    .collect();
                Some(RowDescription::new(row_description))
            }
        }
    }

    pub fn row_info(&self) -> Vec<FieldInfo> {
        let metadata = &self.describe.metadata;
        let output_format = &self.output_format;
        zip(metadata, output_format)
            .map(|(col, format)| {
                FieldInfo::new(col.name.clone(), None, None, col.ty.clone(), *format)
            })
            .collect()
    }

    pub fn query_type(&self) -> QueryType {
        self.describe.query_type()
    }

    pub fn command_tag(&self) -> CommandTag {
        self.describe.command_tag()
    }
}
