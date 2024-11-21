use std::{collections::HashMap, fmt::Display};

use acl::{Acl, AclOwned, MutAcl};
use block::{Block, BlockOwned, MutBlock};
use ddl::{Ddl, DdlOwned, MutDdl};
use expression::{ExprOwned, Expression, MutExpression};
use relational::{MutRelational, RelOwned, Relational};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tarantool::{
    decimal::Decimal,
    index::{IndexType, RtreeIndexDistanceType},
    space::SpaceEngineType,
};

use super::{
    ddl::AlterSystemType,
    expression::{cast, FunctionFeature, TrimKind},
    operator::{self, ConflictStrategy, JoinKind, OrderByElement, UpdateStrategy},
};
use crate::ir::{
    acl::{AlterOption, GrantRevokeType},
    ddl::{ColumnDef, Language, ParamDef, SetParamScopeType, SetParamValue},
    distribution::Distribution,
    helpers::RepeatableState,
    relation::Type,
    transformation::redistribution::{ColumnPosition, MotionPolicy, Program},
    value::Value,
};
use plugin::{
    AppendServiceToTier, ChangeConfig, CreatePlugin, DisablePlugin, DropPlugin, EnablePlugin,
    MigrateTo, MutPlugin, Plugin, PluginOwned, RemoveServiceFromTier,
};

pub mod acl;
pub mod block;
pub mod ddl;
pub mod expression;
pub mod plugin;
pub mod relational;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Hash, Copy)]
pub enum ArenaType {
    Arena32,
    Arena64,
    Arena96,
    Arena136,
    Arena232,
}

impl Display for ArenaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArenaType::Arena32 => {
                write!(f, "32")
            }
            ArenaType::Arena64 => {
                write!(f, "64")
            }
            ArenaType::Arena96 => {
                write!(f, "96")
            }
            ArenaType::Arena136 => {
                write!(f, "136")
            }
            ArenaType::Arena232 => {
                write!(f, "224")
            }
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize, Hash, Copy)]
pub struct NodeId {
    pub offset: u32,
    pub arena_type: ArenaType,
}

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.offset, self.arena_type)
    }
}

impl Default for NodeId {
    fn default() -> Self {
        NodeId {
            offset: 0,
            arena_type: ArenaType::Arena32,
        }
    }
}

/// Expression name.
///
/// Example: `42 as a`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Alias {
    /// Alias name.
    pub name: SmolStr,
    /// Child expression node index in the plan node arena.
    pub child: NodeId,
}

impl From<Alias> for NodeAligned {
    fn from(value: Alias) -> Self {
        Self::Node32(Node32::Alias(value))
    }
}

/// Binary expression returning boolean result.
///
/// Example: `a > 42`, `b in (select c from ...)`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct BoolExpr {
    /// Left branch expression node index in the plan node arena.
    pub left: NodeId,
    /// Boolean operator.
    pub op: operator::Bool,
    /// Right branch expression node index in the plan node arena.
    pub right: NodeId,
}

impl From<BoolExpr> for NodeAligned {
    fn from(value: BoolExpr) -> Self {
        Self::Node32(Node32::Bool(value))
    }
}

/// Binary expression returning row result.
///
/// Example: `a + b > 42`, `a + b < c + 1`, `1 + 2 != 2 * 2`.
///
/// TODO: always cover children with parentheses (in `to_sql`).
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ArithmeticExpr {
    /// Left branch expression node index in the plan node arena.
    pub left: NodeId,
    /// Arithmetic operator.
    pub op: operator::Arithmetic,
    /// Right branch expression node index in the plan node arena.
    pub right: NodeId,
}

impl From<ArithmeticExpr> for NodeAligned {
    fn from(value: ArithmeticExpr) -> Self {
        Self::Node32(Node32::Arithmetic(value))
    }
}

/// Type cast expression.
///
/// Example: `cast(a as text)`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Cast {
    /// Target expression that must be casted to another type.
    pub child: NodeId,
    /// Cast type.
    pub to: cast::Type,
}

impl From<Cast> for NodeAligned {
    fn from(value: Cast) -> Self {
        Self::Node32(Node32::Cast(value))
    }
}

/// String concatenation expression.
///
/// Example: `a || 'hello'`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Concat {
    /// Left expression node id.
    pub left: NodeId,
    /// Right expression node id.
    pub right: NodeId,
}

impl From<Concat> for NodeAligned {
    fn from(value: Concat) -> Self {
        Self::Node32(Node32::Concat(value))
    }
}

/// Constant expressions.
///
/// Example: `42`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Constant {
    /// Contained value (boolean, number, string or null)
    pub value: Value,
}

impl From<Constant> for NodeAligned {
    fn from(value: Constant) -> Self {
        Self::Node64(Node64::Constant(value))
    }
}

/// Helper structure for cases of references generated from asterisk.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize, Hash)]
pub struct ReferenceAsteriskSource {
    /// None                -> generated from simple asterisk: `select * from t`
    /// Some(relation_name) -> generated from table asterisk: `select t.* from t`
    pub relation_name: Option<SmolStr>,
    /// Unique asterisk id local for single Projection
    pub asterisk_id: usize,
}

impl ReferenceAsteriskSource {
    pub fn new(relation_name: Option<SmolStr>, asterisk_id: usize) -> Self {
        Self {
            relation_name,
            asterisk_id,
        }
    }
}

/// Like expressions.
///
/// Example: `a like b escape '\'`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Like {
    /// Left child id
    pub left: NodeId,
    /// Right child id
    pub right: NodeId,
    /// Escape child id
    pub escape: NodeId,
}

impl From<Like> for NodeAligned {
    fn from(value: Like) -> Self {
        Self::Node32(Node32::Like(value))
    }
}

/// Reference to the position in the incoming tuple(s).
/// Uses a relative pointer as a coordinate system:
/// - relational node (containing this reference)
/// - target(s) in the relational nodes list of children
/// - column position in the child(ren) output tuple
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Reference {
    /// Relational node ID that contains current reference.
    pub parent: Option<NodeId>,
    /// Targets in the relational node children list.
    /// - Leaf nodes (relation scans): None.
    /// - Union nodes: two elements (left and right).
    /// - Other: single element.
    pub targets: Option<Vec<usize>>,
    /// Expression position in the input tuple (i.e. `Alias` column).
    pub position: usize,
    /// Referred column type in the input tuple.
    pub col_type: Type,
    /// Field indicating whether this reference resulted
    /// from an asterisk "*" under projection.
    pub asterisk_source: Option<ReferenceAsteriskSource>,
}

impl From<Reference> for NodeAligned {
    fn from(value: Reference) -> Self {
        Self::Node96(Node96::Reference(value))
    }
}

/// Top of the tuple tree.
///
/// If the current tuple is the output for some relational operator, it should
/// consist of the list of aliases. Otherwise (rows in selection filter
/// or in join condition) we don't require aliases in the list.
///
///
///  Example: (a, b, 1).
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Row {
    /// A list of the alias expression node indexes in the plan node arena.
    pub list: Vec<NodeId>,
    /// Resulting data distribution of the tuple. Should be filled as a part
    /// of the last "add Motion" transformation.
    pub distribution: Option<Distribution>,
}

impl From<Row> for NodeAligned {
    fn from(value: Row) -> Self {
        Self::Node64(Node64::Row(value))
    }
}

/// Stable function cannot modify the database and
/// is guaranteed to return the same results given
/// the same arguments for all rows within a single
/// statement.
///
/// Example: `bucket_id("1")` (the number of buckets can be
/// changed only after restarting the cluster).
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct StableFunction {
    /// Function name.
    pub name: SmolStr,
    /// Function arguments.
    pub children: Vec<NodeId>,
    /// Optional function feature.
    pub feature: Option<FunctionFeature>,
    /// Function return type.
    pub func_type: Type,
    /// Whether function is provided by tarantool,
    /// when referencing these funcs from local
    /// sql we must not use quotes.
    /// Examples: aggregates, substr
    pub is_system: bool,
}

impl From<StableFunction> for NodeAligned {
    fn from(value: StableFunction) -> Self {
        Self::Node96(Node96::StableFunction(value))
    }
}

/// Trim expression.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Trim {
    /// Trim kind.
    pub kind: Option<TrimKind>,
    /// Trim string pattern to remove (it can be an expression).
    pub pattern: Option<NodeId>,
    /// Target expression to trim.
    pub target: NodeId,
}

impl From<Trim> for NodeAligned {
    fn from(value: Trim) -> Self {
        Self::Node32(Node32::Trim(value))
    }
}

/// Unary expression returning boolean result.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct UnaryExpr {
    /// Unary operator.
    pub op: operator::Unary,
    /// Child expression node index in the plan node arena.
    pub child: NodeId,
}

impl From<UnaryExpr> for NodeAligned {
    fn from(value: UnaryExpr) -> Self {
        Self::Node32(Node32::Unary(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ExprInParentheses {
    pub child: NodeId,
}

impl From<ExprInParentheses> for NodeAligned {
    fn from(value: ExprInParentheses) -> Self {
        Self::Node32(Node32::ExprInParentheses(value))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Case {
    pub search_expr: Option<NodeId>,
    pub when_blocks: Vec<(NodeId, NodeId)>,
    pub else_expr: Option<NodeId>,
}

impl From<Case> for NodeAligned {
    fn from(value: Case) -> Self {
        Self::Node64(Node64::Case(value))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Parameter {
    pub param_type: Option<Type>,
}

impl From<Parameter> for NodeAligned {
    fn from(value: Parameter) -> Self {
        Self::Node64(Node64::Parameter(value))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CountAsterisk {}

impl From<CountAsterisk> for NodeAligned {
    fn from(value: CountAsterisk) -> Self {
        Self::Node32(Node32::CountAsterisk(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ScanCte {
    /// CTE's name.
    pub alias: SmolStr,
    /// Contains exactly one single element (projection node index).
    pub child: NodeId,
    /// An output tuple with aliases.
    pub output: NodeId,
}

impl From<ScanCte> for NodeAligned {
    fn from(value: ScanCte) -> Self {
        Self::Node64(Node64::ScanCte(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Except {
    /// Left child id
    pub left: NodeId,
    /// Right child id
    pub right: NodeId,
    /// Outputs tuple node index in the plan node arena.
    pub output: NodeId,
}

impl From<Except> for NodeAligned {
    fn from(value: Except) -> Self {
        Self::Node32(Node32::Except(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Delete {
    /// Relation name.
    pub relation: SmolStr,
    /// Contains exactly one single element.
    pub children: Vec<NodeId>,
    /// The output tuple (reserved for `delete returning`).
    pub output: Option<NodeId>,
}

impl From<Delete> for NodeAligned {
    fn from(value: Delete) -> Self {
        Self::Node64(Node64::Delete(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Insert {
    /// Relation name.
    pub relation: SmolStr,
    /// Target column positions for data insertion from
    /// the child's tuple.
    pub columns: Vec<usize>,
    /// Contains exactly one single element.
    pub children: Vec<NodeId>,
    /// The output tuple (reserved for `insert returning`).
    pub output: NodeId,
    /// What to do in case there is a conflict during insert on storage
    pub conflict_strategy: ConflictStrategy,
}

impl From<Insert> for NodeAligned {
    fn from(value: Insert) -> Self {
        Self::Node96(Node96::Insert(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Intersect {
    pub left: NodeId,
    pub right: NodeId,
    // id of the output tuple
    pub output: NodeId,
}

impl From<Intersect> for NodeAligned {
    fn from(value: Intersect) -> Self {
        Self::Node32(Node32::Intersect(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Update {
    /// Relation name.
    pub relation: SmolStr,
    /// Children ids. Update has exactly one child.
    pub children: Vec<NodeId>,
    /// Maps position of column being updated in table to corresponding position
    /// in `Projection` below `Update`.
    ///
    /// For sharded `Update`, it will contain every table column except `bucket_id`
    /// column. For local `Update` it will contain only update table columns.
    pub update_columns_map: HashMap<ColumnPosition, ColumnPosition, RepeatableState>,
    /// How this update must be executed.
    pub strategy: UpdateStrategy,
    /// Positions of primary columns in `Projection`
    /// below `Update`.
    pub pk_positions: Vec<ColumnPosition>,
    /// Output id.
    pub output: NodeId,
}

impl From<Update> for NodeAligned {
    fn from(value: Update) -> Self {
        Self::Node136(Node136::Update(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Join {
    /// Contains at least two elements: left and right node indexes
    /// from the plan node arena. Every element other than those
    /// two should be treated as a `SubQuery` node.
    pub children: Vec<NodeId>,
    /// Left and right tuple comparison condition.
    /// In fact it is an expression tree top index from the plan node arena.
    pub condition: NodeId,
    /// Outputs tuple node index from the plan node arena.
    pub output: NodeId,
    /// inner or left
    pub kind: JoinKind,
}

impl From<Join> for NodeAligned {
    fn from(value: Join) -> Self {
        Self::Node64(Node64::Join(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Limit {
    /// Output tuple.
    pub output: NodeId,
    // The limit value constant that comes after LIMIT keyword.
    pub limit: u64,
    /// Select statement that is being limited.
    /// Note that it can be a complex statement, like SELECT .. UNION ALL SELECT .. LIMIT 100,
    /// in that case limit is applied to the result of union.
    pub child: NodeId,
}

impl From<Limit> for NodeAligned {
    fn from(value: Limit) -> Self {
        Self::Node32(Node32::Limit(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Motion {
    // Scan name.
    pub alias: Option<SmolStr>,
    /// Contains exactly one single element: child node index
    /// from the plan node arena.
    pub children: Vec<NodeId>,
    /// Motion policy - the amount of data to be moved.
    pub policy: MotionPolicy,
    /// A sequence of opcodes that transform the data.
    pub program: Program,
    /// Outputs tuple node index in the plan node arena.
    pub output: NodeId,
}

impl From<Motion> for NodeAligned {
    fn from(value: Motion) -> Self {
        Self::Node136(Node136::Motion(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Projection {
    /// Contains at least one single element: child node index
    /// from the plan node arena. Every element other than the
    /// first one should be treated as a `SubQuery` node from
    /// the output tree.
    pub children: Vec<NodeId>,
    /// Outputs tuple node index in the plan node arena.
    pub output: NodeId,
    /// Wheter the select was marked with `distinct` keyword
    pub is_distinct: bool,
}

impl From<Projection> for NodeAligned {
    fn from(value: Projection) -> Self {
        Self::Node64(Node64::Projection(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct SelectWithoutScan {
    /// Additional subquery children
    pub children: Vec<NodeId>,
    /// Outputs tuple node index in the plan node arena.
    pub output: NodeId,
}

impl From<SelectWithoutScan> for NodeAligned {
    fn from(value: SelectWithoutScan) -> Self {
        Self::Node32(Node32::SelectWithoutScan(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ScanRelation {
    // Scan name.
    pub alias: Option<SmolStr>,
    /// Outputs tuple node index in the plan node arena.
    pub output: NodeId,
    /// Relation name.
    pub relation: SmolStr,
}

impl From<ScanRelation> for NodeAligned {
    fn from(value: ScanRelation) -> Self {
        Self::Node64(Node64::ScanRelation(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ScanSubQuery {
    /// SubQuery name.
    pub alias: Option<SmolStr>,
    /// Contains exactly one single element: child node index
    /// from the plan node arena.
    pub children: Vec<NodeId>,
    /// Outputs tuple node index in the plan node arena.
    pub output: NodeId,
}

impl From<ScanSubQuery> for NodeAligned {
    fn from(value: ScanSubQuery) -> Self {
        Self::Node64(Node64::ScanSubQuery(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Selection {
    /// Contains at least one single element: child node index
    /// from the plan node arena. Every element other than the
    /// first one should be treated as a `SubQuery` node from
    /// the filter tree.
    pub children: Vec<NodeId>,
    /// Filters expression node index in the plan node arena.
    pub filter: NodeId,
    /// Outputs tuple node index in the plan node arena.
    pub output: NodeId,
}

impl From<Selection> for NodeAligned {
    fn from(value: Selection) -> Self {
        Self::Node64(Node64::Selection(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct GroupBy {
    /// The first child is a relational operator before group by
    pub children: Vec<NodeId>,
    pub gr_cols: Vec<NodeId>,
    pub output: NodeId,
    pub is_final: bool,
}

impl From<GroupBy> for NodeAligned {
    fn from(value: GroupBy) -> Self {
        Self::Node64(Node64::GroupBy(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Having {
    pub children: Vec<NodeId>,
    pub output: NodeId,
    pub filter: NodeId,
}

impl From<Having> for NodeAligned {
    fn from(value: Having) -> Self {
        Self::Node64(Node64::Having(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct OrderBy {
    pub children: Vec<NodeId>,
    pub output: NodeId,
    pub order_by_elements: Vec<OrderByElement>,
}

impl From<OrderBy> for NodeAligned {
    fn from(value: OrderBy) -> Self {
        Self::Node64(Node64::OrderBy(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct UnionAll {
    /// Left child id
    pub left: NodeId,
    /// Right child id
    pub right: NodeId,
    /// Outputs tuple node index in the plan node arena.
    pub output: NodeId,
}

impl From<UnionAll> for NodeAligned {
    fn from(value: UnionAll) -> Self {
        Self::Node32(Node32::UnionAll(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Values {
    /// Output tuple.
    pub output: NodeId,
    /// Non-empty list of value rows.
    pub children: Vec<NodeId>,
}

impl From<Values> for NodeAligned {
    fn from(value: Values) -> Self {
        Self::Node32(Node32::Values(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ValuesRow {
    /// Output tuple of aliases.
    pub output: NodeId,
    /// The data tuple.
    pub data: NodeId,
    /// A list of children is required for the rows containing
    /// sub-queries. For example, the row `(1, (select a from t))`
    /// requires `children` to keep projection node. If the row
    /// contains only constants (i.e. `(1, 2)`), then `children`
    /// should be empty.
    pub children: Vec<NodeId>,
}

impl From<ValuesRow> for NodeAligned {
    fn from(value: ValuesRow) -> Self {
        Self::Node64(Node64::ValuesRow(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct DropRole {
    pub name: SmolStr,
    pub if_exists: bool,
    pub timeout: Decimal,
}

impl From<DropRole> for NodeAligned {
    fn from(value: DropRole) -> Self {
        Self::Node64(Node64::DropRole(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct DropUser {
    pub name: SmolStr,
    pub if_exists: bool,
    pub timeout: Decimal,
}

impl From<DropUser> for NodeAligned {
    fn from(value: DropUser) -> Self {
        Self::Node64(Node64::DropUser(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CreateRole {
    pub name: SmolStr,
    pub if_not_exists: bool,
    pub timeout: Decimal,
}

impl From<CreateRole> for NodeAligned {
    fn from(value: CreateRole) -> Self {
        Self::Node64(Node64::CreateRole(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CreateUser {
    pub name: SmolStr,
    pub password: SmolStr,
    pub if_not_exists: bool,
    pub auth_method: SmolStr,
    pub timeout: Decimal,
}

impl From<CreateUser> for NodeAligned {
    fn from(value: CreateUser) -> Self {
        Self::Node136(Node136::CreateUser(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct AlterUser {
    pub name: SmolStr,
    pub alter_option: AlterOption,
    pub timeout: Decimal,
}

impl From<AlterUser> for NodeAligned {
    fn from(value: AlterUser) -> Self {
        Self::Node136(Node136::AlterUser(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct GrantPrivilege {
    pub grant_type: GrantRevokeType,
    pub grantee_name: SmolStr,
    pub timeout: Decimal,
}

impl From<GrantPrivilege> for NodeAligned {
    fn from(value: GrantPrivilege) -> Self {
        Self::Node136(Node136::GrantPrivilege(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct RevokePrivilege {
    pub revoke_type: GrantRevokeType,
    pub grantee_name: SmolStr,
    pub timeout: Decimal,
}

impl From<RevokePrivilege> for NodeAligned {
    fn from(value: RevokePrivilege) -> Self {
        Self::Node136(Node136::RevokePrivilege(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CreateTable {
    pub name: SmolStr,
    pub format: Vec<ColumnDef>,
    pub primary_key: Vec<SmolStr>,
    /// If `None`, create global table.
    pub sharding_key: Option<Vec<SmolStr>>,
    /// Vinyl is supported only for sharded tables.
    pub engine_type: SpaceEngineType,
    pub if_not_exists: bool,
    pub wait_applied_globally: bool,
    pub timeout: Decimal,
    /// Shows which tier the sharded table belongs to.
    /// Field has value, only if it was specified in [ON TIER] part of CREATE TABLE statement.
    /// Field is None, if:
    /// 1) Global table.
    /// 2) Sharded table without [ON TIER] part. In this case picodata will use default tier.
    pub tier: Option<SmolStr>,
}

impl From<CreateTable> for NodeAligned {
    fn from(value: CreateTable) -> Self {
        Self::Node232(Node232::CreateTable(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct DropTable {
    pub name: SmolStr,
    pub if_exists: bool,
    pub wait_applied_globally: bool,
    pub timeout: Decimal,
}

impl From<DropTable> for NodeAligned {
    fn from(value: DropTable) -> Self {
        Self::Node64(Node64::DropTable(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CreateProc {
    pub name: SmolStr,
    pub params: Vec<ParamDef>,
    pub body: SmolStr,
    pub if_not_exists: bool,
    pub language: Language,
    pub wait_applied_globally: bool,
    pub timeout: Decimal,
}

impl From<CreateProc> for NodeAligned {
    fn from(value: CreateProc) -> Self {
        Self::Node136(Node136::CreateProc(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct DropProc {
    pub name: SmolStr,
    pub params: Option<Vec<ParamDef>>,
    pub if_exists: bool,
    pub wait_applied_globally: bool,
    pub timeout: Decimal,
}

impl From<DropProc> for NodeAligned {
    fn from(value: DropProc) -> Self {
        Self::Node96(Node96::DropProc(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct RenameRoutine {
    pub old_name: SmolStr,
    pub new_name: SmolStr,
    pub params: Option<Vec<ParamDef>>,
    pub wait_applied_globally: bool,
    pub timeout: Decimal,
}

impl From<RenameRoutine> for NodeAligned {
    fn from(value: RenameRoutine) -> Self {
        Self::Node136(Node136::RenameRoutine(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CreateIndex {
    pub name: SmolStr,
    pub table_name: SmolStr,
    pub columns: Vec<SmolStr>,
    pub unique: bool,
    pub if_not_exists: bool,
    pub index_type: IndexType,
    pub bloom_fpr: Option<Decimal>,
    pub page_size: Option<u32>,
    pub range_size: Option<u32>,
    pub run_count_per_level: Option<u32>,
    pub run_size_ratio: Option<Decimal>,
    pub dimension: Option<u32>,
    pub distance: Option<RtreeIndexDistanceType>,
    pub hint: Option<bool>,
    pub wait_applied_globally: bool,
    pub timeout: Decimal,
}

impl From<CreateIndex> for NodeAligned {
    fn from(value: CreateIndex) -> Self {
        Self::Node232(Node232::CreateIndex(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct DropIndex {
    pub name: SmolStr,
    pub if_exists: bool,
    pub wait_applied_globally: bool,
    pub timeout: Decimal,
}

impl From<DropIndex> for NodeAligned {
    fn from(value: DropIndex) -> Self {
        Self::Node64(Node64::DropIndex(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct SetParam {
    pub scope_type: SetParamScopeType,
    pub param_value: SetParamValue,
    pub timeout: Decimal,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct AlterSystem {
    pub ty: AlterSystemType,
    /// In case of None, ALTER is supposed
    /// to be executed on all tiers.
    pub tier_name: Option<SmolStr>,
    pub timeout: Decimal,
}

impl From<AlterSystem> for NodeAligned {
    fn from(value: AlterSystem) -> Self {
        Self::Node136(Node136::AlterSystem(value))
    }
}

impl From<SetParam> for NodeAligned {
    fn from(value: SetParam) -> Self {
        Self::Node64(Node64::SetParam(value))
    }
}

// TODO: Fill with actual values.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct SetTransaction {
    pub timeout: Decimal,
}

impl From<SetTransaction> for NodeAligned {
    fn from(value: SetTransaction) -> Self {
        Self::Node64(Node64::SetTransaction(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Invalid;

impl From<Invalid> for NodeAligned {
    fn from(value: Invalid) -> Self {
        Self::Node32(Node32::Invalid(value))
    }
}

/// Procedure body.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Procedure {
    /// The name of the procedure.
    pub name: SmolStr,
    /// Passed values to the procedure.
    pub values: Vec<NodeId>,
}

impl From<Procedure> for NodeAligned {
    fn from(value: Procedure) -> Self {
        Self::Node64(Node64::Procedure(value))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Union {
    /// Left child id
    pub left: NodeId,
    /// Right child id
    pub right: NodeId,
    /// Outputs tuple node index in the plan node arena.
    pub output: NodeId,
}

impl From<Union> for NodeAligned {
    fn from(value: Union) -> Self {
        Self::Node32(Node32::Union(value))
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Node32 {
    Invalid(Invalid),
    Union(Union),
    CountAsterisk(CountAsterisk),
    ExprInParentheses(ExprInParentheses),
    Unary(UnaryExpr),
    Concat(Concat),
    Like(Like),
    Bool(BoolExpr),
    Limit(Limit),
    Arithmetic(ArithmeticExpr),
    Trim(Trim),
    Cast(Cast),
    Alias(Alias),
    Except(Except),
    Intersect(Intersect),
    SelectWithoutScan(SelectWithoutScan),
    UnionAll(UnionAll),
    Values(Values),
}

impl Node32 {
    #[must_use]
    pub fn into_owned(self) -> NodeOwned {
        match self {
            Node32::Alias(alias) => NodeOwned::Expression(ExprOwned::Alias(alias)),
            Node32::Arithmetic(arithm) => NodeOwned::Expression(ExprOwned::Arithmetic(arithm)),
            Node32::Bool(bool) => NodeOwned::Expression(ExprOwned::Bool(bool)),
            Node32::Limit(limit) => NodeOwned::Relational(RelOwned::Limit(limit)),
            Node32::Cast(cast) => NodeOwned::Expression(ExprOwned::Cast(cast)),
            Node32::Concat(concat) => NodeOwned::Expression(ExprOwned::Concat(concat)),
            Node32::CountAsterisk(count) => NodeOwned::Expression(ExprOwned::CountAsterisk(count)),
            Node32::Like(like) => NodeOwned::Expression(ExprOwned::Like(like)),
            Node32::Except(except) => NodeOwned::Relational(RelOwned::Except(except)),
            Node32::ExprInParentheses(expr_in_par) => {
                NodeOwned::Expression(ExprOwned::ExprInParentheses(expr_in_par))
            }
            Node32::Intersect(intersect) => NodeOwned::Relational(RelOwned::Intersect(intersect)),
            Node32::Invalid(inv) => NodeOwned::Invalid(inv),
            Node32::SelectWithoutScan(select) => {
                NodeOwned::Relational(RelOwned::SelectWithoutScan(select))
            }
            Node32::Trim(trim) => NodeOwned::Expression(ExprOwned::Trim(trim)),
            Node32::Unary(unary) => NodeOwned::Expression(ExprOwned::Unary(unary)),
            Node32::Union(un) => NodeOwned::Relational(RelOwned::Union(un)),
            Node32::UnionAll(union_all) => NodeOwned::Relational(RelOwned::UnionAll(union_all)),
            Node32::Values(values) => NodeOwned::Relational(RelOwned::Values(values)),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Node64 {
    ScanCte(ScanCte),
    Case(Case),
    Parameter(Parameter),
    Constant(Constant),
    Projection(Projection),
    Selection(Selection),
    Having(Having),
    ValuesRow(ValuesRow),
    OrderBy(OrderBy),
    Procedure(Procedure),
    Join(Join),
    Row(Row),
    Delete(Delete),
    ScanRelation(ScanRelation),
    ScanSubQuery(ScanSubQuery),
    DropRole(DropRole),
    DropUser(DropUser),
    CreateRole(CreateRole),
    DropTable(DropTable),
    DropIndex(DropIndex),
    GroupBy(GroupBy),
    SetParam(SetParam),
    SetTransaction(SetTransaction),
    Invalid(Invalid),
}

impl Node64 {
    #[must_use]
    pub fn into_owned(self) -> NodeOwned {
        match self {
            Node64::Case(case) => NodeOwned::Expression(ExprOwned::Case(case)),
            Node64::Invalid(invalid) => NodeOwned::Invalid(invalid),
            Node64::Constant(constant) => NodeOwned::Expression(ExprOwned::Constant(constant)),
            Node64::CreateRole(create_role) => NodeOwned::Acl(AclOwned::CreateRole(create_role)),
            Node64::Delete(delete) => NodeOwned::Relational(RelOwned::Delete(delete)),
            Node64::DropIndex(drop_index) => NodeOwned::Ddl(DdlOwned::DropIndex(drop_index)),
            Node64::DropRole(drop_role) => NodeOwned::Acl(AclOwned::DropRole(drop_role)),
            Node64::DropTable(drop_table) => NodeOwned::Ddl(DdlOwned::DropTable(drop_table)),
            Node64::DropUser(drop_user) => NodeOwned::Acl(AclOwned::DropUser(drop_user)),
            Node64::GroupBy(group_by) => NodeOwned::Relational(RelOwned::GroupBy(group_by)),
            Node64::Having(having) => NodeOwned::Relational(RelOwned::Having(having)),
            Node64::Join(join) => NodeOwned::Relational(RelOwned::Join(join)),
            Node64::OrderBy(order_by) => NodeOwned::Relational(RelOwned::OrderBy(order_by)),
            Node64::Parameter(param) => NodeOwned::Parameter(param),
            Node64::Row(row) => NodeOwned::Expression(ExprOwned::Row(row)),
            Node64::Procedure(proc) => NodeOwned::Block(BlockOwned::Procedure(proc)),
            Node64::Projection(proj) => NodeOwned::Relational(RelOwned::Projection(proj)),
            Node64::ScanCte(scan_cte) => NodeOwned::Relational(RelOwned::ScanCte(scan_cte)),
            Node64::ScanRelation(scan_rel) => {
                NodeOwned::Relational(RelOwned::ScanRelation(scan_rel))
            }
            Node64::ScanSubQuery(scan_squery) => {
                NodeOwned::Relational(RelOwned::ScanSubQuery(scan_squery))
            }
            Node64::Selection(sel) => NodeOwned::Relational(RelOwned::Selection(sel)),
            Node64::SetParam(set_param) => NodeOwned::Ddl(DdlOwned::SetParam(set_param)),
            Node64::SetTransaction(set_trans) => {
                NodeOwned::Ddl(DdlOwned::SetTransaction(set_trans))
            }
            Node64::ValuesRow(values_row) => NodeOwned::Relational(RelOwned::ValuesRow(values_row)),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Node96 {
    Reference(Reference),
    Invalid(Invalid),
    StableFunction(StableFunction),
    DropProc(DropProc),
    Insert(Insert),
    CreatePlugin(CreatePlugin),
    EnablePlugin(EnablePlugin),
    DisablePlugin(DisablePlugin),
    DropPlugin(DropPlugin),
}

impl Node96 {
    #[must_use]
    pub fn into_owned(self) -> NodeOwned {
        match self {
            Node96::Reference(reference) => NodeOwned::Expression(ExprOwned::Reference(reference)),
            Node96::DropProc(drop_proc) => NodeOwned::Ddl(DdlOwned::DropProc(drop_proc)),
            Node96::Insert(insert) => NodeOwned::Relational(RelOwned::Insert(insert)),
            Node96::Invalid(inv) => NodeOwned::Invalid(inv),
            Node96::StableFunction(stable_func) => {
                NodeOwned::Expression(ExprOwned::StableFunction(stable_func))
            }
            Node96::DropPlugin(drop_plugin) => NodeOwned::Plugin(PluginOwned::Drop(drop_plugin)),
            Node96::CreatePlugin(create) => NodeOwned::Plugin(PluginOwned::Create(create)),
            Node96::EnablePlugin(enable) => NodeOwned::Plugin(PluginOwned::Enable(enable)),
            Node96::DisablePlugin(disable) => NodeOwned::Plugin(PluginOwned::Disable(disable)),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Node136 {
    Invalid(Invalid),
    CreateUser(CreateUser),
    AlterUser(AlterUser),
    AlterSystem(AlterSystem),
    CreateProc(CreateProc),
    RenameRoutine(RenameRoutine),
    Motion(Motion),
    GrantPrivilege(GrantPrivilege),
    RevokePrivilege(RevokePrivilege),
    Update(Update),
    MigrateTo(MigrateTo),
    ChangeConfig(ChangeConfig),
}

impl Node136 {
    #[must_use]
    pub fn into_owned(self) -> NodeOwned {
        match self {
            Node136::AlterUser(alter_user) => NodeOwned::Acl(AclOwned::AlterUser(alter_user)),
            Node136::AlterSystem(alter_system) => {
                NodeOwned::Ddl(DdlOwned::AlterSystem(alter_system))
            }
            Node136::CreateProc(create_proc) => NodeOwned::Ddl(DdlOwned::CreateProc(create_proc)),
            Node136::GrantPrivilege(grant_privelege) => {
                NodeOwned::Acl(AclOwned::GrantPrivilege(grant_privelege))
            }
            Node136::CreateUser(create_user) => NodeOwned::Acl(AclOwned::CreateUser(create_user)),
            Node136::RevokePrivilege(revoke_privelege) => {
                NodeOwned::Acl(AclOwned::RevokePrivilege(revoke_privelege))
            }
            Node136::Invalid(inv) => NodeOwned::Invalid(inv),
            Node136::Motion(motion) => NodeOwned::Relational(RelOwned::Motion(motion)),
            Node136::Update(update) => NodeOwned::Relational(RelOwned::Update(update)),
            Node136::RenameRoutine(rename_routine) => {
                NodeOwned::Ddl(DdlOwned::RenameRoutine(rename_routine))
            }
            Node136::MigrateTo(migrate) => NodeOwned::Plugin(PluginOwned::MigrateTo(migrate)),
            Node136::ChangeConfig(change_config) => {
                NodeOwned::Plugin(PluginOwned::ChangeConfig(change_config))
            }
        }
    }
}

#[allow(clippy::module_name_repetitions, clippy::large_enum_variant)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Node232 {
    Invalid(Invalid),
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    AppendServiceToTier(AppendServiceToTier),
    RemoveServiceFromTier(RemoveServiceFromTier),
}

impl Node232 {
    #[must_use]
    pub fn into_owned(self) -> NodeOwned {
        match self {
            Node232::CreateTable(create_table) => {
                NodeOwned::Ddl(DdlOwned::CreateTable(create_table))
            }
            Node232::CreateIndex(create_index) => {
                NodeOwned::Ddl(DdlOwned::CreateIndex(create_index))
            }
            Node232::Invalid(inv) => NodeOwned::Invalid(inv),
            Node232::AppendServiceToTier(append) => {
                NodeOwned::Plugin(PluginOwned::AppendServiceToTier(append))
            }
            Node232::RemoveServiceFromTier(remove) => {
                NodeOwned::Plugin(PluginOwned::RemoveServiceFromTier(remove))
            }
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub enum NodeAligned {
    Node32(Node32),
    Node64(Node64),
    Node96(Node96),
    Node136(Node136),
    Node232(Node232),
}

impl From<Node32> for NodeAligned {
    fn from(value: Node32) -> Self {
        Self::Node32(value)
    }
}

impl From<Node64> for NodeAligned {
    fn from(value: Node64) -> Self {
        Self::Node64(value)
    }
}

impl From<Node96> for NodeAligned {
    fn from(value: Node96) -> Self {
        Self::Node96(value)
    }
}

impl From<Node136> for NodeAligned {
    fn from(value: Node136) -> Self {
        Self::Node136(value)
    }
}

impl From<Node232> for NodeAligned {
    fn from(value: Node232) -> Self {
        Self::Node232(value)
    }
}
// parameter to avoid multiple enums
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum Node<'nodes> {
    Expression(Expression<'nodes>),
    Relational(Relational<'nodes>),
    Acl(Acl<'nodes>),
    Ddl(Ddl<'nodes>),
    Block(Block<'nodes>),
    Parameter(&'nodes Parameter),
    Invalid(&'nodes Invalid),
    Plugin(Plugin<'nodes>),
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, PartialEq, Eq)]
pub enum MutNode<'nodes> {
    Expression(MutExpression<'nodes>),
    Relational(MutRelational<'nodes>),
    Acl(MutAcl<'nodes>),
    Ddl(MutDdl<'nodes>),
    Block(MutBlock<'nodes>),
    Parameter(&'nodes mut Parameter),
    Invalid(&'nodes mut Invalid),
    Plugin(MutPlugin<'nodes>),
}

impl Node<'_> {
    #[must_use]
    pub fn into_owned(self) -> NodeOwned {
        match self {
            Node::Expression(expr) => NodeOwned::Expression(expr.get_expr_owned()),
            Node::Relational(rel) => NodeOwned::Relational(rel.get_rel_owned()),
            Node::Ddl(ddl) => NodeOwned::Ddl(ddl.get_ddl_owned()),
            Node::Acl(acl) => NodeOwned::Acl(acl.get_acl_owned()),
            Node::Block(block) => NodeOwned::Block(block.get_block_owned()),
            Node::Parameter(param) => NodeOwned::Parameter((*param).clone()),
            Node::Invalid(inv) => NodeOwned::Invalid((*inv).clone()),
            Node::Plugin(plugin) => NodeOwned::Plugin(plugin.get_plugin_owned()),
        }
    }
}

// rename to NodeOwned
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum NodeOwned {
    Expression(ExprOwned),
    Relational(RelOwned),
    Ddl(DdlOwned),
    Acl(AclOwned),
    Block(BlockOwned),
    Parameter(Parameter),
    Invalid(Invalid),
    Plugin(PluginOwned),
}

impl From<NodeOwned> for NodeAligned {
    fn from(value: NodeOwned) -> Self {
        match value {
            NodeOwned::Acl(acl) => acl.into(),
            NodeOwned::Block(block) => block.into(),
            NodeOwned::Ddl(ddl) => ddl.into(),
            NodeOwned::Expression(expr) => expr.into(),
            NodeOwned::Invalid(inv) => inv.into(),
            NodeOwned::Parameter(param) => param.into(),
            NodeOwned::Relational(rel) => rel.into(),
            NodeOwned::Plugin(p) => p.into(),
        }
    }
}

#[cfg(test)]
mod tests;
