use std::{collections::HashMap, fmt::Display};

use acl::{Acl, AclOwned, MutAcl};
use block::{Block, BlockOwned, MutBlock};
use ddl::{Ddl, DdlOwned, MutDdl};
use deallocate::Deallocate;
use expression::{ExprOwned, Expression, MutExpression};
use relational::{MutRelational, RelOwned, Relational};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tarantool::{
    decimal::Decimal,
    index::{IndexType, RtreeIndexDistanceType},
    space::SpaceEngineType,
};
use tcl::Tcl;

use super::{
    ddl::AlterSystemType,
    expression::{FunctionFeature, TrimKind, VolatilityType},
    operator::{self, ConflictStrategy, JoinKind, OrderByElement, UpdateStrategy},
    types::{CastType, DerivedType},
};
use crate::ir::{
    acl::{AlterOption, AuditPolicyOption, GrantRevokeType},
    ddl::{ColumnDef, Language, ParamDef, SetParamScopeType, SetParamValue},
    distribution::Distribution,
    helpers::RepeatableState,
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
pub mod deallocate;
pub mod expression;
pub mod plugin;
pub mod relational;
pub mod tcl;

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
    pub to: CastType,
}

impl From<Cast> for NodeAligned {
    fn from(value: Cast) -> Self {
        Self::Node32(Node32::Cast(value))
    }
}

/// Index expression.
///
/// Example: `x[10]`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct IndexExpr {
    /// Target expression we're going to take an element of.
    pub child: NodeId,
    /// Expression pointing to an element of the target expression.
    pub which: NodeId,
}

impl From<IndexExpr> for NodeAligned {
    fn from(value: IndexExpr) -> Self {
        Self::Node32(Node32::Index(value))
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
        Self::Node32(Node32::Constant(value))
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

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
pub enum ReferenceTarget {
    Leaf,
    Single(NodeId),
    Union(NodeId, NodeId),
    Values(Vec<NodeId>),
}

pub struct ReferenceIterator<'a> {
    counter: usize,
    source: &'a ReferenceTarget,
}

impl<'a> Iterator for ReferenceIterator<'a> {
    type Item = &'a NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        let node = match self.source {
            ReferenceTarget::Leaf => None,
            ReferenceTarget::Single(id) => {
                if self.counter == 0 {
                    Some(id)
                } else {
                    None
                }
            }
            ReferenceTarget::Union(left, right) => {
                if self.counter == 0 {
                    Some(left)
                } else if self.counter == 1 {
                    Some(right)
                } else {
                    None
                }
            }
            ReferenceTarget::Values(nodes) => nodes.get(self.counter),
        };

        if node.is_some() {
            self.counter += 1;
        }
        node
    }
}

impl ReferenceTarget {
    pub fn first(&self) -> Option<&NodeId> {
        match self {
            ReferenceTarget::Leaf => None,
            ReferenceTarget::Single(id) => Some(id),
            ReferenceTarget::Union(id, _) => Some(id),
            ReferenceTarget::Values(nodes) => nodes.first(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ReferenceTarget::Leaf => 0,
            ReferenceTarget::Single(_) => 1,
            ReferenceTarget::Union(_, _) => 2,
            ReferenceTarget::Values(nodes) => nodes.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self, ReferenceTarget::Leaf)
    }

    pub fn iter(&self) -> ReferenceIterator<'_> {
        ReferenceIterator {
            counter: 0,
            source: self,
        }
    }
}

/// Reference to the position in the incoming tuple(s).
/// Uses a relative pointer as a coordinate system:
/// - relational node (containing this reference)
/// - target(s) in the relational nodes list of children
/// - column position in the child(ren) output tuple
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
pub struct Reference {
    /// Target node
    pub target: ReferenceTarget,
    /// Expression position in the input tuple (i.e. `Alias` column).
    pub position: usize,
    /// Referred column type in the input tuple.
    pub col_type: DerivedType,
    /// Field indicating whether this reference resulted
    /// from an asterisk "*" under projection.
    pub asterisk_source: Option<ReferenceAsteriskSource>,
    /// Used to distinguish explicit references to system columns (like bucket_id)
    pub is_system: bool,
}

impl From<Reference> for NodeAligned {
    fn from(value: Reference) -> Self {
        Self::Node96(Node96::Reference(value))
    }
}

/// Reference to the position in the incoming tuple(s) for subquery.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
pub struct SubQueryReference {
    /// Relational node
    pub rel_id: NodeId,
    /// Expression position in the input tuple (i.e. `Alias` column).
    pub position: usize,
    /// Referred column type in the input tuple.
    pub col_type: DerivedType,
}

impl From<SubQueryReference> for NodeAligned {
    fn from(value: SubQueryReference) -> Self {
        Self::Node32(Node32::SubQueryReference(value))
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

/// Example: `bucket_id("1")` (the number of buckets can be
/// changed only after restarting the cluster).
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
pub struct ScalarFunction {
    /// Function name.
    pub name: SmolStr,
    /// Function arguments.
    pub children: Vec<NodeId>,
    /// Optional function feature.
    pub feature: Option<FunctionFeature>,
    pub volatility_type: VolatilityType,
    /// Function return type.
    pub func_type: DerivedType,
    /// Whether function is provided by tarantool,
    /// when referencing these funcs from local
    /// sql we must not use quotes.
    /// Examples: aggregates, substr
    pub is_system: bool,
    /// Whether function is used as window function
    pub is_window: bool,
}

impl From<ScalarFunction> for NodeAligned {
    fn from(value: ScalarFunction) -> Self {
        Self::Node96(Node96::ScalarFunction(value))
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
    pub param_type: DerivedType,
    // index of parameter (starting with 1)
    pub index: u16,
}

impl From<Parameter> for NodeAligned {
    fn from(value: Parameter) -> Self {
        Self::Node32(Node32::Parameter(value))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CountAsterisk {}

impl From<CountAsterisk> for NodeAligned {
    fn from(value: CountAsterisk) -> Self {
        Self::Node32(Node32::CountAsterisk(value))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TimeParameters {
    /// Number of digits to round the seconds to
    pub precision: usize,
    /// Whether to produce a timestamp with timezone (`CURRENT_*` family of SQL functions) or without it (`LOCAL*` family of SQL functions)
    pub include_timezone: bool,
}

/// Node representing SQL functions for retrieving time
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Timestamp {
    /// `CURRENT_DATE`
    Date,
    /// `CURRENT_TIMESTAMP` or `LOCALTIMESTAMP`
    DateTime(TimeParameters),
}

impl From<Timestamp> for NodeAligned {
    fn from(value: Timestamp) -> Self {
        Self::Node32(Node32::Timestamp(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ScanCte {
    /// CTE's name.
    pub alias: SmolStr,
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
    /// Contains child id in plan node arena or
    /// None if child was substituted with virtual table.
    pub child: Option<NodeId>,
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
    pub child: NodeId,
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
    pub child: NodeId,
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
    /// Contains child id in plan node arena or
    /// None if child was substituted with virtual table.
    pub child: Option<NodeId>,
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
    pub windows: Vec<NodeId>,
    /// Outputs tuple node index in the plan node arena.
    pub output: NodeId,
    /// Whether the select was marked with `distinct` keyword
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
    pub child: NodeId,
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
    /// The first child is a
    /// * Scan in case it's local GroupBy
    /// * Motion with policy Segment in case two stage aggregation was applied
    ///
    /// Other children are subqueries used under grouping expressions.
    pub children: Vec<NodeId>,
    pub gr_exprs: Vec<NodeId>,
    pub output: NodeId,
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
    pub auth_method: tarantool::auth::AuthMethod,
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
pub struct AuditPolicy {
    pub policy_name: SmolStr,
    pub audit_option: AuditPolicyOption,
    pub timeout: Decimal,
}

impl From<AuditPolicy> for NodeAligned {
    fn from(value: AuditPolicy) -> Self {
        Self::Node96(Node96::AuditPolicy(value))
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

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct AlterTable {
    pub name: SmolStr,
    pub wait_applied_globally: bool,
    pub timeout: Decimal,
    pub op: AlterTableOp,
}

impl From<AlterTable> for NodeAligned {
    fn from(value: AlterTable) -> Self {
        Self::Node136(Node136::AlterTable(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum AlterTableOp {
    AlterColumn(Vec<AlterColumn>),
    RenameTable { new_table_name: SmolStr },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum AlterColumn {
    Add {
        column: ColumnDef,
        if_not_exists: bool,
    },
    Rename {
        from: SmolStr,
        to: SmolStr,
    },
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
pub struct Backup {
    pub wait_applied_globally: bool,
    pub timeout: Decimal,
}

impl From<Backup> for NodeAligned {
    fn from(value: Backup) -> Self {
        Self::Node32(Node32::Backup(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct TruncateTable {
    pub name: SmolStr,
    pub wait_applied_globally: bool,
    pub timeout: Decimal,
}

impl From<TruncateTable> for NodeAligned {
    fn from(value: TruncateTable) -> Self {
        Self::Node64(Node64::TruncateTable(value))
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

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
pub enum FrameType {
    Range,
    Rows,
}

impl Display for FrameType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameType::Range => write!(f, "range"),
            FrameType::Rows => write!(f, "rows"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
pub enum BoundType {
    PrecedingUnbounded,
    PrecedingOffset(NodeId),
    CurrentRow,
    FollowingOffset(NodeId),
    FollowingUnbounded,
}

impl BoundType {
    pub(crate) fn index(&self) -> usize {
        match self {
            BoundType::PrecedingUnbounded => 0,
            BoundType::PrecedingOffset(_) => 1,
            BoundType::CurrentRow => 2,
            BoundType::FollowingOffset(_) => 3,
            BoundType::FollowingUnbounded => 4,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
pub enum Bound {
    Single(BoundType),
    Between(BoundType, BoundType),
}

impl Bound {
    pub(crate) fn index(&self) -> usize {
        match self {
            Bound::Single(_) => 0,
            Bound::Between(_, _) => 1,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
pub struct Frame {
    pub ty: FrameType,
    pub bound: Bound,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Window {
    pub partition: Option<Vec<NodeId>>,
    pub ordering: Option<Vec<OrderByElement>>,
    pub frame: Option<Frame>,
}

impl From<Window> for NodeAligned {
    fn from(value: Window) -> Self {
        Self::Node136(Node136::Window(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]

pub struct Over {
    pub stable_func: NodeId,
    pub filter: Option<NodeId>,
    pub window: NodeId,
}

impl From<Over> for NodeAligned {
    fn from(value: Over) -> Self {
        Self::Node64(Node64::Over(value))
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
    Unary(UnaryExpr),
    Concat(Concat),
    Like(Like),
    Bool(BoolExpr),
    Limit(Limit),
    Arithmetic(ArithmeticExpr),
    Trim(Trim),
    Index(IndexExpr),
    Cast(Cast),
    Alias(Alias),
    Except(Except),
    Intersect(Intersect),
    SelectWithoutScan(SelectWithoutScan),
    UnionAll(UnionAll),
    Values(Values),
    Deallocate(Deallocate),
    Tcl(Tcl),
    CreateSchema,
    DropSchema,
    SubQueryReference(SubQueryReference),
    Backup(Backup),
    // begin the section to allow in-place swapping with Constant using the replace32()
    Parameter(Parameter),
    Constant(Constant),
    Timestamp(Timestamp),
    // end the section to allow in-place swapping with Constant using the replace32()
}

impl Node32 {
    #[must_use]
    pub fn into_owned(self) -> NodeOwned {
        match self {
            Node32::Alias(alias) => NodeOwned::Expression(ExprOwned::Alias(alias)),
            Node32::Arithmetic(arithm) => NodeOwned::Expression(ExprOwned::Arithmetic(arithm)),
            Node32::Bool(bool) => NodeOwned::Expression(ExprOwned::Bool(bool)),
            Node32::Limit(limit) => NodeOwned::Relational(RelOwned::Limit(limit)),
            Node32::Index(index) => NodeOwned::Expression(ExprOwned::Index(index)),
            Node32::Cast(cast) => NodeOwned::Expression(ExprOwned::Cast(cast)),
            Node32::Concat(concat) => NodeOwned::Expression(ExprOwned::Concat(concat)),
            Node32::CountAsterisk(count) => NodeOwned::Expression(ExprOwned::CountAsterisk(count)),
            Node32::Like(like) => NodeOwned::Expression(ExprOwned::Like(like)),
            Node32::Except(except) => NodeOwned::Relational(RelOwned::Except(except)),
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
            Node32::Deallocate(deallocate) => NodeOwned::Deallocate(deallocate),
            Node32::Tcl(tcl) => match tcl {
                Tcl::Begin => NodeOwned::Tcl(Tcl::Begin),
                Tcl::Commit => NodeOwned::Tcl(Tcl::Commit),
                Tcl::Rollback => NodeOwned::Tcl(Tcl::Rollback),
            },
            Node32::CreateSchema => NodeOwned::Ddl(DdlOwned::CreateSchema),
            Node32::DropSchema => NodeOwned::Ddl(DdlOwned::DropSchema),
            Node32::SubQueryReference(sub_query_reference) => {
                NodeOwned::Expression(ExprOwned::SubQueryReference(sub_query_reference))
            }
            Node32::Backup(backup) => NodeOwned::Ddl(DdlOwned::Backup(backup)),
            Node32::Constant(constant) => NodeOwned::Expression(ExprOwned::Constant(constant)),
            Node32::Parameter(param) => NodeOwned::Expression(ExprOwned::Parameter(param)),
            Node32::Timestamp(lt) => NodeOwned::Expression(ExprOwned::Timestamp(lt)),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Node64 {
    ScanCte(ScanCte),
    Case(Case),
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
    Over(Over),
    TruncateTable(TruncateTable),
}

impl Node64 {
    #[must_use]
    pub fn into_owned(self) -> NodeOwned {
        match self {
            Node64::Over(over) => NodeOwned::Expression(ExprOwned::Over(over)),
            Node64::Case(case) => NodeOwned::Expression(ExprOwned::Case(case)),
            Node64::Invalid(invalid) => NodeOwned::Invalid(invalid),
            Node64::CreateRole(create_role) => NodeOwned::Acl(AclOwned::CreateRole(create_role)),
            Node64::Delete(delete) => NodeOwned::Relational(RelOwned::Delete(delete)),
            Node64::DropIndex(drop_index) => NodeOwned::Ddl(DdlOwned::DropIndex(drop_index)),
            Node64::DropRole(drop_role) => NodeOwned::Acl(AclOwned::DropRole(drop_role)),
            Node64::DropTable(drop_table) => NodeOwned::Ddl(DdlOwned::DropTable(drop_table)),
            Node64::TruncateTable(truncate_table) => {
                NodeOwned::Ddl(DdlOwned::TruncateTable(truncate_table))
            }
            Node64::DropUser(drop_user) => NodeOwned::Acl(AclOwned::DropUser(drop_user)),
            Node64::GroupBy(group_by) => NodeOwned::Relational(RelOwned::GroupBy(group_by)),
            Node64::Having(having) => NodeOwned::Relational(RelOwned::Having(having)),
            Node64::Join(join) => NodeOwned::Relational(RelOwned::Join(join)),
            Node64::OrderBy(order_by) => NodeOwned::Relational(RelOwned::OrderBy(order_by)),
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
    ScalarFunction(ScalarFunction),
    DropProc(DropProc),
    Insert(Insert),
    CreatePlugin(CreatePlugin),
    EnablePlugin(EnablePlugin),
    DisablePlugin(DisablePlugin),
    DropPlugin(DropPlugin),
    AuditPolicy(AuditPolicy),
}

impl Node96 {
    #[must_use]
    pub fn into_owned(self) -> NodeOwned {
        match self {
            Node96::Reference(reference) => NodeOwned::Expression(ExprOwned::Reference(reference)),
            Node96::DropProc(drop_proc) => NodeOwned::Ddl(DdlOwned::DropProc(drop_proc)),
            Node96::Insert(insert) => NodeOwned::Relational(RelOwned::Insert(insert)),
            Node96::Invalid(inv) => NodeOwned::Invalid(inv),
            Node96::ScalarFunction(scalar_func) => {
                NodeOwned::Expression(ExprOwned::ScalarFunction(scalar_func))
            }
            Node96::DropPlugin(drop_plugin) => NodeOwned::Plugin(PluginOwned::Drop(drop_plugin)),
            Node96::CreatePlugin(create) => NodeOwned::Plugin(PluginOwned::Create(create)),
            Node96::EnablePlugin(enable) => NodeOwned::Plugin(PluginOwned::Enable(enable)),
            Node96::DisablePlugin(disable) => NodeOwned::Plugin(PluginOwned::Disable(disable)),
            Node96::AuditPolicy(audit_policy) => {
                NodeOwned::Acl(AclOwned::AuditPolicy(audit_policy))
            }
        }
    }
}

const _: () = assert!(std::mem::size_of::<Node136>() < 136);

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Node136 {
    Invalid(Invalid),
    CreateUser(CreateUser),
    AlterUser(AlterUser),
    AlterSystem(AlterSystem),
    AlterTable(AlterTable),
    CreateProc(CreateProc),
    RenameRoutine(RenameRoutine),
    Motion(Motion),
    GrantPrivilege(GrantPrivilege),
    RevokePrivilege(RevokePrivilege),
    Update(Update),
    MigrateTo(MigrateTo),
    ChangeConfig(ChangeConfig),
    Window(Window),
}

impl Node136 {
    #[must_use]
    pub fn into_owned(self) -> NodeOwned {
        match self {
            Node136::AlterUser(alter_user) => NodeOwned::Acl(AclOwned::AlterUser(alter_user)),
            Node136::AlterSystem(alter_system) => {
                NodeOwned::Ddl(DdlOwned::AlterSystem(alter_system))
            }
            Node136::AlterTable(alter_table) => NodeOwned::Ddl(DdlOwned::AlterTable(alter_table)),
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
            Node136::Window(window) => NodeOwned::Expression(ExprOwned::Window(window)),
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
    Invalid(&'nodes Invalid),
    Plugin(Plugin<'nodes>),
    Deallocate(&'nodes Deallocate),
    Tcl(Tcl),
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, PartialEq, Eq)]
pub enum MutNode<'nodes> {
    Expression(MutExpression<'nodes>),
    Relational(MutRelational<'nodes>),
    Acl(MutAcl<'nodes>),
    Ddl(MutDdl<'nodes>),
    Block(MutBlock<'nodes>),
    Invalid(&'nodes mut Invalid),
    Plugin(MutPlugin<'nodes>),
    Deallocate(&'nodes mut Deallocate),
    Tcl(Tcl),
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
            Node::Invalid(inv) => NodeOwned::Invalid((*inv).clone()),
            Node::Plugin(plugin) => NodeOwned::Plugin(plugin.get_plugin_owned()),
            Node::Deallocate(deallocate) => NodeOwned::Deallocate((*deallocate).clone()),
            Node::Tcl(tcl) => NodeOwned::Tcl(tcl),
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
    Invalid(Invalid),
    Plugin(PluginOwned),
    Deallocate(Deallocate),
    Tcl(Tcl),
}

impl From<NodeOwned> for NodeAligned {
    fn from(value: NodeOwned) -> Self {
        match value {
            NodeOwned::Acl(acl) => acl.into(),
            NodeOwned::Block(block) => block.into(),
            NodeOwned::Ddl(ddl) => ddl.into(),
            NodeOwned::Expression(expr) => expr.into(),
            NodeOwned::Invalid(inv) => inv.into(),
            NodeOwned::Relational(rel) => rel.into(),
            NodeOwned::Plugin(p) => p.into(),
            NodeOwned::Deallocate(d) => d.into(),
            NodeOwned::Tcl(tcl) => tcl.into(),
        }
    }
}

#[cfg(test)]
mod tests;
