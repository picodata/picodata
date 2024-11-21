//! Contains the logical plan tree and helpers.

use base64ct::{Base64, Encoding};
use expression::Position;
use node::acl::{Acl, MutAcl};
use node::block::{Block, MutBlock};
use node::ddl::{Ddl, MutDdl};
use node::expression::{Expression, MutExpression};
use node::relational::{MutRelational, Relational};
use node::{Invalid, NodeAligned};
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::cell::{RefCell, RefMut};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::slice::Iter;
use tree::traversal::LevelNode;

use operator::Arithmetic;
use relation::{Table, Type};

use self::parameters::Parameters;
use self::relation::Relations;
use self::transformation::redistribution::MotionPolicy;
use crate::errors::Entity::Query;
use crate::errors::{Action, Entity, SbroadError, TypeError};
use crate::executor::engine::helpers::to_user;
use crate::executor::engine::TableVersionMap;
use crate::ir::node::plugin::{MutPlugin, Plugin};
use crate::ir::node::{
    Alias, ArenaType, ArithmeticExpr, BoolExpr, Case, Cast, Concat, Constant, ExprInParentheses,
    GroupBy, Having, Insert, Limit, Motion, MutNode, Node, Node136, Node232, Node32, Node64,
    Node96, NodeId, NodeOwned, OrderBy, Projection, Reference, Row, ScanRelation, Selection,
    StableFunction, Trim, UnaryExpr, Values,
};
use crate::ir::operator::Bool;
use crate::ir::relation::Column;
use crate::ir::tree::traversal::{
    BreadthFirst, PostOrder, PostOrderWithFilter, EXPR_CAPACITY, REL_CAPACITY,
};
use crate::ir::undo::TransformationLog;
use crate::ir::value::Value;
use crate::warn;

use self::node::Like;

// TODO: remove when rust version in bumped in module
#[allow(elided_lifetimes_in_associated_constant)]
pub mod acl;
pub mod aggregates;
pub mod block;
pub mod ddl;
pub mod distribution;
pub mod expression;
pub mod function;
pub mod helpers;
pub mod node;
pub mod operator;
pub mod parameters;
pub mod relation;
pub mod transformation;
pub mod tree;
pub mod undo;
pub mod value;

pub const DEFAULT_VTABLE_MAX_ROWS: u64 = 5000;
pub const DEFAULT_VDBE_MAX_STEPS: u64 = 45000;

/// Plan nodes storage.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Nodes {
    /// The positions in the arrays act like pointers, so it is possible
    /// only to add nodes to the plan, but never remove them.
    arena32: Vec<Node32>,
    arena64: Vec<Node64>,
    arena96: Vec<Node96>,
    arena136: Vec<Node136>,
    arena224: Vec<Node232>,
}

impl Nodes {
    pub(crate) fn get(&self, id: NodeId) -> Option<Node> {
        match id.arena_type {
            ArenaType::Arena32 => self.arena32.get(id.offset as usize).map(|node| match node {
                Node32::Alias(alias) => Node::Expression(Expression::Alias(alias)),
                Node32::Arithmetic(arithm) => Node::Expression(Expression::Arithmetic(arithm)),
                Node32::Bool(bool) => Node::Expression(Expression::Bool(bool)),
                Node32::Concat(concat) => Node::Expression(Expression::Concat(concat)),
                Node32::Cast(cast) => Node::Expression(Expression::Cast(cast)),
                Node32::CountAsterisk(count) => Node::Expression(Expression::CountAsterisk(count)),
                Node32::Like(like) => Node::Expression(Expression::Like(like)),
                Node32::Except(except) => Node::Relational(Relational::Except(except)),
                Node32::ExprInParentheses(expr) => {
                    Node::Expression(Expression::ExprInParentheses(expr))
                }
                Node32::Intersect(intersect) => Node::Relational(Relational::Intersect(intersect)),
                Node32::Invalid(inv) => Node::Invalid(inv),
                Node32::Limit(limit) => Node::Relational(Relational::Limit(limit)),
                Node32::SelectWithoutScan(select) => {
                    Node::Relational(Relational::SelectWithoutScan(select))
                }
                Node32::Trim(trim) => Node::Expression(Expression::Trim(trim)),
                Node32::Unary(unary) => Node::Expression(Expression::Unary(unary)),
                Node32::Union(un) => Node::Relational(Relational::Union(un)),
                Node32::UnionAll(union_all) => Node::Relational(Relational::UnionAll(union_all)),
                Node32::Values(values) => Node::Relational(Relational::Values(values)),
            }),
            ArenaType::Arena64 => self.arena64.get(id.offset as usize).map(|node| match node {
                Node64::Case(case) => Node::Expression(Expression::Case(case)),
                Node64::Invalid(invalid) => Node::Invalid(invalid),
                Node64::Constant(constant) => Node::Expression(Expression::Constant(constant)),
                Node64::CreateRole(create_role) => Node::Acl(Acl::CreateRole(create_role)),
                Node64::Delete(delete) => Node::Relational(Relational::Delete(delete)),
                Node64::DropIndex(drop_index) => Node::Ddl(Ddl::DropIndex(drop_index)),
                Node64::DropRole(drop_role) => Node::Acl(Acl::DropRole(drop_role)),
                Node64::DropTable(drop_table) => Node::Ddl(Ddl::DropTable(drop_table)),
                Node64::Row(row) => Node::Expression(Expression::Row(row)),
                Node64::DropUser(drop_user) => Node::Acl(Acl::DropUser(drop_user)),
                Node64::GroupBy(group_by) => Node::Relational(Relational::GroupBy(group_by)),
                Node64::Having(having) => Node::Relational(Relational::Having(having)),
                Node64::Join(join) => Node::Relational(Relational::Join(join)),
                Node64::OrderBy(order_by) => Node::Relational(Relational::OrderBy(order_by)),
                Node64::Parameter(param) => Node::Parameter(param),
                Node64::Procedure(proc) => Node::Block(Block::Procedure(proc)),
                Node64::Projection(proj) => Node::Relational(Relational::Projection(proj)),
                Node64::ScanCte(scan_cte) => Node::Relational(Relational::ScanCte(scan_cte)),
                Node64::ScanRelation(scan_rel) => {
                    Node::Relational(Relational::ScanRelation(scan_rel))
                }
                Node64::ScanSubQuery(scan_squery) => {
                    Node::Relational(Relational::ScanSubQuery(scan_squery))
                }
                Node64::Selection(sel) => Node::Relational(Relational::Selection(sel)),
                Node64::SetParam(set_param) => Node::Ddl(Ddl::SetParam(set_param)),
                Node64::SetTransaction(set_trans) => Node::Ddl(Ddl::SetTransaction(set_trans)),
                Node64::ValuesRow(values_row) => {
                    Node::Relational(Relational::ValuesRow(values_row))
                }
            }),
            ArenaType::Arena96 => self.arena96.get(id.offset as usize).map(|node| match node {
                Node96::Reference(reference) => Node::Expression(Expression::Reference(reference)),
                Node96::DropProc(drop_proc) => Node::Ddl(Ddl::DropProc(drop_proc)),
                Node96::Insert(insert) => Node::Relational(Relational::Insert(insert)),
                Node96::Invalid(inv) => Node::Invalid(inv),
                Node96::StableFunction(stable_func) => {
                    Node::Expression(Expression::StableFunction(stable_func))
                }
                Node96::CreatePlugin(create) => Node::Plugin(Plugin::Create(create)),
                Node96::EnablePlugin(enable) => Node::Plugin(Plugin::Enable(enable)),
                Node96::DisablePlugin(disable) => Node::Plugin(Plugin::Disable(disable)),
                Node96::DropPlugin(drop) => Node::Plugin(Plugin::Drop(drop)),
            }),
            ArenaType::Arena136 => self
                .arena136
                .get(id.offset as usize)
                .map(|node| match node {
                    Node136::AlterUser(alter_user) => Node::Acl(Acl::AlterUser(alter_user)),
                    Node136::CreateProc(create_proc) => Node::Ddl(Ddl::CreateProc(create_proc)),
                    Node136::RevokePrivilege(revoke_priv) => {
                        Node::Acl(Acl::RevokePrivilege(revoke_priv))
                    }
                    Node136::GrantPrivilege(grant_priv) => {
                        Node::Acl(Acl::GrantPrivilege(grant_priv))
                    }
                    Node136::Update(update) => Node::Relational(Relational::Update(update)),
                    Node136::AlterSystem(alter_system) => Node::Ddl(Ddl::AlterSystem(alter_system)),
                    Node136::CreateUser(create_user) => Node::Acl(Acl::CreateUser(create_user)),
                    Node136::Invalid(inv) => Node::Invalid(inv),
                    Node136::Motion(motion) => Node::Relational(Relational::Motion(motion)),
                    Node136::RenameRoutine(rename_routine) => {
                        Node::Ddl(Ddl::RenameRoutine(rename_routine))
                    }
                    Node136::MigrateTo(migrate) => Node::Plugin(Plugin::MigrateTo(migrate)),
                    Node136::ChangeConfig(change_config) => {
                        Node::Plugin(Plugin::ChangeConfig(change_config))
                    }
                }),
            ArenaType::Arena232 => self
                .arena224
                .get(id.offset as usize)
                .map(|node| match node {
                    Node232::CreateIndex(create_index) => Node::Ddl(Ddl::CreateIndex(create_index)),
                    Node232::CreateTable(create_table) => Node::Ddl(Ddl::CreateTable(create_table)),
                    Node232::Invalid(inv) => Node::Invalid(inv),
                    Node232::AppendServiceToTier(append) => {
                        Node::Plugin(Plugin::AppendServiceToTier(append))
                    }
                    Node232::RemoveServiceFromTier(remove) => {
                        Node::Plugin(Plugin::RemoveServiceFromTier(remove))
                    }
                }),
        }
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn get_mut(&mut self, id: NodeId) -> Option<MutNode> {
        match id.arena_type {
            ArenaType::Arena32 => self
                .arena32
                .get_mut(id.offset as usize)
                .map(|node| match node {
                    Node32::Alias(alias) => MutNode::Expression(MutExpression::Alias(alias)),
                    Node32::Arithmetic(arithm) => {
                        MutNode::Expression(MutExpression::Arithmetic(arithm))
                    }
                    Node32::Bool(bool) => MutNode::Expression(MutExpression::Bool(bool)),
                    Node32::Limit(limit) => MutNode::Relational(MutRelational::Limit(limit)),
                    Node32::Concat(concat) => MutNode::Expression(MutExpression::Concat(concat)),
                    Node32::Cast(cast) => MutNode::Expression(MutExpression::Cast(cast)),
                    Node32::CountAsterisk(count) => {
                        MutNode::Expression(MutExpression::CountAsterisk(count))
                    }
                    Node32::Like(like) => MutNode::Expression(MutExpression::Like(like)),
                    Node32::Except(except) => MutNode::Relational(MutRelational::Except(except)),
                    Node32::ExprInParentheses(expr) => {
                        MutNode::Expression(MutExpression::ExprInParentheses(expr))
                    }
                    Node32::Intersect(intersect) => {
                        MutNode::Relational(MutRelational::Intersect(intersect))
                    }
                    Node32::Invalid(inv) => MutNode::Invalid(inv),
                    Node32::Trim(trim) => MutNode::Expression(MutExpression::Trim(trim)),
                    Node32::Unary(unary) => MutNode::Expression(MutExpression::Unary(unary)),
                    Node32::Union(un) => MutNode::Relational(MutRelational::Union(un)),
                    Node32::UnionAll(union_all) => {
                        MutNode::Relational(MutRelational::UnionAll(union_all))
                    }
                    Node32::SelectWithoutScan(select) => {
                        MutNode::Relational(MutRelational::SelectWithoutScan(select))
                    }
                    Node32::Values(values) => MutNode::Relational(MutRelational::Values(values)),
                }),
            ArenaType::Arena64 => self
                .arena64
                .get_mut(id.offset as usize)
                .map(|node| match node {
                    Node64::Case(case) => MutNode::Expression(MutExpression::Case(case)),
                    Node64::Invalid(invalid) => MutNode::Invalid(invalid),
                    Node64::Constant(constant) => {
                        MutNode::Expression(MutExpression::Constant(constant))
                    }
                    Node64::CreateRole(create_role) => {
                        MutNode::Acl(MutAcl::CreateRole(create_role))
                    }
                    Node64::Delete(delete) => MutNode::Relational(MutRelational::Delete(delete)),
                    Node64::DropIndex(drop_index) => MutNode::Ddl(MutDdl::DropIndex(drop_index)),
                    Node64::Row(row) => MutNode::Expression(MutExpression::Row(row)),
                    Node64::DropRole(drop_role) => MutNode::Acl(MutAcl::DropRole(drop_role)),
                    Node64::DropTable(drop_table) => MutNode::Ddl(MutDdl::DropTable(drop_table)),
                    Node64::DropUser(drop_user) => MutNode::Acl(MutAcl::DropUser(drop_user)),
                    Node64::GroupBy(group_by) => {
                        MutNode::Relational(MutRelational::GroupBy(group_by))
                    }
                    Node64::Having(having) => MutNode::Relational(MutRelational::Having(having)),
                    Node64::Join(join) => MutNode::Relational(MutRelational::Join(join)),
                    Node64::OrderBy(order_by) => {
                        MutNode::Relational(MutRelational::OrderBy(order_by))
                    }
                    Node64::Parameter(param) => MutNode::Parameter(param),
                    Node64::Procedure(proc) => MutNode::Block(MutBlock::Procedure(proc)),
                    Node64::Projection(proj) => {
                        MutNode::Relational(MutRelational::Projection(proj))
                    }
                    Node64::ScanCte(scan_cte) => {
                        MutNode::Relational(MutRelational::ScanCte(scan_cte))
                    }
                    Node64::ScanRelation(scan_rel) => {
                        MutNode::Relational(MutRelational::ScanRelation(scan_rel))
                    }
                    Node64::ScanSubQuery(scan_squery) => {
                        MutNode::Relational(MutRelational::ScanSubQuery(scan_squery))
                    }
                    Node64::Selection(sel) => MutNode::Relational(MutRelational::Selection(sel)),
                    Node64::SetParam(set_param) => MutNode::Ddl(MutDdl::SetParam(set_param)),
                    Node64::SetTransaction(set_trans) => {
                        MutNode::Ddl(MutDdl::SetTransaction(set_trans))
                    }
                    Node64::ValuesRow(values_row) => {
                        MutNode::Relational(MutRelational::ValuesRow(values_row))
                    }
                }),
            ArenaType::Arena96 => self
                .arena96
                .get_mut(id.offset as usize)
                .map(|node| match node {
                    Node96::Reference(reference) => {
                        MutNode::Expression(MutExpression::Reference(reference))
                    }
                    Node96::DropProc(drop_proc) => MutNode::Ddl(MutDdl::DropProc(drop_proc)),
                    Node96::Insert(insert) => MutNode::Relational(MutRelational::Insert(insert)),
                    Node96::Invalid(inv) => MutNode::Invalid(inv),
                    Node96::StableFunction(stable_func) => {
                        MutNode::Expression(MutExpression::StableFunction(stable_func))
                    }
                    Node96::CreatePlugin(create) => MutNode::Plugin(MutPlugin::Create(create)),
                    Node96::EnablePlugin(enable) => MutNode::Plugin(MutPlugin::Enable(enable)),
                    Node96::DisablePlugin(disable) => MutNode::Plugin(MutPlugin::Disable(disable)),
                    Node96::DropPlugin(drop) => MutNode::Plugin(MutPlugin::Drop(drop)),
                }),
            ArenaType::Arena136 => {
                self.arena136
                    .get_mut(id.offset as usize)
                    .map(|node| match node {
                        Node136::AlterUser(alter_user) => {
                            MutNode::Acl(MutAcl::AlterUser(alter_user))
                        }
                        Node136::CreateProc(create_proc) => {
                            MutNode::Ddl(MutDdl::CreateProc(create_proc))
                        }
                        Node136::GrantPrivilege(grant_priv) => {
                            MutNode::Acl(MutAcl::GrantPrivilege(grant_priv))
                        }
                        Node136::CreateUser(create_user) => {
                            MutNode::Acl(MutAcl::CreateUser(create_user))
                        }
                        Node136::RevokePrivilege(revoke_priv) => {
                            MutNode::Acl(MutAcl::RevokePrivilege(revoke_priv))
                        }
                        Node136::Invalid(inv) => MutNode::Invalid(inv),
                        Node136::AlterSystem(alter_system) => {
                            MutNode::Ddl(MutDdl::AlterSystem(alter_system))
                        }
                        Node136::Update(update) => {
                            MutNode::Relational(MutRelational::Update(update))
                        }
                        Node136::Motion(motion) => {
                            MutNode::Relational(MutRelational::Motion(motion))
                        }
                        Node136::RenameRoutine(rename_routine) => {
                            MutNode::Ddl(MutDdl::RenameRoutine(rename_routine))
                        }
                        Node136::MigrateTo(migrate) => {
                            MutNode::Plugin(MutPlugin::MigrateTo(migrate))
                        }
                        Node136::ChangeConfig(change_config) => {
                            MutNode::Plugin(MutPlugin::ChangeConfig(change_config))
                        }
                    })
            }
            ArenaType::Arena232 => {
                self.arena224
                    .get_mut(id.offset as usize)
                    .map(|node| match node {
                        Node232::CreateIndex(create_index) => {
                            MutNode::Ddl(MutDdl::CreateIndex(create_index))
                        }
                        Node232::Invalid(inv) => MutNode::Invalid(inv),
                        Node232::CreateTable(create_table) => {
                            MutNode::Ddl(MutDdl::CreateTable(create_table))
                        }
                        Node232::AppendServiceToTier(append) => {
                            MutNode::Plugin(MutPlugin::AppendServiceToTier(append))
                        }
                        Node232::RemoveServiceFromTier(remove) => {
                            MutNode::Plugin(MutPlugin::RemoveServiceFromTier(remove))
                        }
                    })
            }
        }
    }

    /// Returns the next node position
    /// # Panics
    #[must_use]
    pub fn next_id(&self, arena_type: ArenaType) -> NodeId {
        match arena_type {
            ArenaType::Arena32 => NodeId {
                offset: u32::try_from(self.arena32.len()).unwrap(),
                arena_type: ArenaType::Arena32,
            },
            ArenaType::Arena64 => NodeId {
                offset: u32::try_from(self.arena64.len()).unwrap(),
                arena_type: ArenaType::Arena64,
            },
            ArenaType::Arena96 => NodeId {
                offset: u32::try_from(self.arena96.len()).unwrap(),
                arena_type: ArenaType::Arena96,
            },
            ArenaType::Arena136 => NodeId {
                offset: u32::try_from(self.arena136.len()).unwrap(),
                arena_type: ArenaType::Arena136,
            },
            ArenaType::Arena232 => NodeId {
                offset: u32::try_from(self.arena224.len()).unwrap(),
                arena_type: ArenaType::Arena232,
            },
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.arena32.len()
            + self.arena64.len()
            + self.arena96.len()
            + self.arena136.len()
            + self.arena224.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.arena32.is_empty()
            && self.arena64.is_empty()
            && self.arena96.is_empty()
            && self.arena136.is_empty()
            && self.arena224.is_empty()
    }

    pub fn iter32(&self) -> Iter<'_, Node32> {
        self.arena32.iter()
    }

    pub fn iter64(&self) -> Iter<'_, Node64> {
        self.arena64.iter()
    }

    pub fn iter96(&self) -> Iter<'_, Node96> {
        self.arena96.iter()
    }

    pub fn iter136(&self) -> Iter<'_, Node136> {
        self.arena136.iter()
    }

    pub fn iter224(&self) -> Iter<'_, Node232> {
        self.arena224.iter()
    }

    /// Add new node to arena.
    /// # Panics
    ///
    /// # Panics
    /// Inserts a new node to the arena and returns its position,
    /// that is treated as a pointer.
    pub fn push(&mut self, node: NodeAligned) -> NodeId {
        match node {
            NodeAligned::Node32(node32) => {
                let new_node_id = NodeId {
                    offset: u32::try_from(self.arena32.len()).unwrap(),
                    arena_type: ArenaType::Arena32,
                };

                self.arena32.push(node32);

                new_node_id
            }
            NodeAligned::Node64(node64) => {
                let new_node_id = NodeId {
                    offset: u32::try_from(self.arena64.len()).unwrap(),
                    arena_type: ArenaType::Arena64,
                };

                self.arena64.push(node64);

                new_node_id
            }
            NodeAligned::Node96(node96) => {
                let new_node_id = NodeId {
                    offset: u32::try_from(self.arena96.len()).unwrap(),
                    arena_type: ArenaType::Arena96,
                };

                self.arena96.push(node96);

                new_node_id
            }
            NodeAligned::Node136(node136) => {
                let new_node_id = NodeId {
                    offset: u32::try_from(self.arena136.len()).unwrap(),
                    arena_type: ArenaType::Arena136,
                };

                self.arena136.push(node136);

                new_node_id
            }
            NodeAligned::Node232(node224) => {
                let new_node_id = NodeId {
                    offset: u32::try_from(self.arena224.len()).unwrap(),
                    arena_type: ArenaType::Arena232,
                };

                self.arena224.push(node224);

                new_node_id
            }
        }
    }

    /// Replace a node in arena with another one.
    ///
    /// # Errors
    /// - The node with the given position doesn't exist.
    pub fn replace(&mut self, id: NodeId, node: Node64) -> Result<Node64, SbroadError> {
        let offset = id.offset as usize;

        match id.arena_type {
            ArenaType::Arena64 => {
                if offset >= self.arena64.len() {
                    return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "can't replace node with id {id:?} as it is out of arena bounds"
                    )));
                }
            }
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format_smolstr!("node {:?} is invalid", node)),
                ));
            }
        };

        let old_node = std::mem::replace(&mut self.arena64[offset], node);
        Ok(old_node)
    }
}

/// One level of `Slices`.
/// Element of `slice` vec is a `motion_id` to execute.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Slice {
    slice: Vec<NodeId>,
}

impl From<Vec<NodeId>> for Slice {
    fn from(vec: Vec<NodeId>) -> Self {
        Self { slice: vec }
    }
}

impl Slice {
    #[must_use]
    pub fn position(&self, index: usize) -> Option<&NodeId> {
        self.slice.get(index)
    }

    #[must_use]
    pub fn positions(&self) -> &[NodeId] {
        &self.slice
    }
}

/// Vec of `motion_id` levels (see `slices` field of `Plan` structure for more information).
/// Element of `slices` vec is one level containing several `motion_id`s to execute.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Slices {
    pub slices: Vec<Slice>,
}

impl From<Vec<Slice>> for Slices {
    fn from(vec: Vec<Slice>) -> Self {
        Self { slices: vec }
    }
}

impl From<Vec<Vec<NodeId>>> for Slices {
    fn from(vec: Vec<Vec<NodeId>>) -> Self {
        Self {
            slices: vec.into_iter().map(Slice::from).collect(),
        }
    }
}

impl Slices {
    #[must_use]
    pub fn slice(&self, index: usize) -> Option<&Slice> {
        self.slices.get(index)
    }

    #[must_use]
    pub fn slices(&self) -> &[Slice] {
        self.slices.as_ref()
    }

    #[must_use]
    pub fn empty() -> Self {
        Self { slices: vec![] }
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Deserialize, Serialize)]
pub enum OptionParamValue {
    Value { val: Value },
    Parameter { plan_id: NodeId },
}

#[derive(PartialEq, Eq, Debug, Clone, Deserialize, Serialize)]
pub struct OptionSpec {
    pub kind: OptionKind,
    pub val: OptionParamValue,
}

#[derive(Clone, PartialEq, Eq, Debug, Hash, Deserialize, Serialize)]
pub enum OptionKind {
    VdbeMaxSteps,
    VTableMaxRows,
}

impl Display for OptionKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            OptionKind::VdbeMaxSteps => "vdbe_max_steps",
            OptionKind::VTableMaxRows => "vtable_max_rows",
        };
        write!(f, "{s}")
    }
}

/// SQL options specified by user in `option(..)` clause.
///
/// Note: ddl options are handled separately.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Options {
    /// Maximum size of the virtual table that this query can produce or use during
    /// query execution. This limit is checked on storage before sending a result table,
    /// and on router before appending the result from one storage to results from other
    /// storages. Value of `0` indicates that this limit is disabled.
    ///
    /// Note: this limit allows the out of memory error for query execution in the following
    /// scenario: if already received vtable has `X` rows and `X + a` causes the OOM, then
    /// if one of the storages returns `a` or more rows, the OOM will occur.
    pub vtable_max_rows: u64,
    /// Options passed to `box.execute` function on storages. Currently there is only one option
    /// `vdbe_max_steps`.
    pub vdbe_max_steps: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options::new(DEFAULT_VTABLE_MAX_ROWS, DEFAULT_VDBE_MAX_STEPS)
    }
}

impl Options {
    #[must_use]
    pub fn new(vtable_max_rows: u64, vdbe_max_steps: u64) -> Self {
        Options {
            vtable_max_rows,
            vdbe_max_steps,
        }
    }
}

pub type ValueIdx = usize;

/// Logical plan tree structure.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Plan {
    /// Append only arena for the plan nodes.
    pub(crate) nodes: Nodes,
    /// Relations are stored in a hash-map, with a table name acting as a
    /// key to guarantee its uniqueness across the plan.
    pub relations: Relations,
    /// Slice is a plan subtree under Motion node, that can be executed
    /// on a single db instance without data distribution problems (we add
    /// Motions to resolve them). Them we traverse the plan tree and collect
    /// Motions level by level in a bottom-up manner to the "slices" array
    /// of arrays. All the slices on the same level can be executed in parallel.
    /// In fact, "slices" is a prepared set of commands for the executor.
    pub(crate) slices: Slices,
    /// The plan top is marked as optional for tree creation convenience.
    /// We build the plan tree in a bottom-up manner, so the top would
    /// be added last. The plan without a top should be treated as invalid.
    top: Option<NodeId>,
    /// The flag is enabled if user wants to get a query plan only.
    /// In this case we don't need to execute query.
    is_explain: bool,
    /// The undo log keeps the history of the plan transformations. It can
    /// be used to revert the plan subtree to some previous snapshot if needed.
    pub(crate) undo: TransformationLog,
    /// Maps parameter to the corresponding constant node.
    pub(crate) constants: Parameters,
    /// Options that were passed by user in `Option` clause. Does not include
    /// options for DDL as those are handled separately. This field is used only
    /// for storing the order of options in `Option` clause, after `bind_params` is
    /// called this field is not used and becomes empty.
    pub raw_options: Vec<OptionSpec>,
    /// Mapping between parameter plan id and corresponding value position in
    /// in values list. This is needed only for handling PG-like parameters
    /// in `bind_params` after which it becomes `None`.
    /// If query uses tnt-like params, then the map is empty.
    pub pg_params_map: HashMap<NodeId, ValueIdx>,
    /// SQL options. Initiliazed to defaults upon IR creation. Then bound to actual
    /// values after `bind_params` these options are set to their actual values.
    /// See `apply_options`.
    pub options: Options,
    pub version_map: TableVersionMap,
    /// Exists only on the router during plan build.
    /// RefCell is used because context can be mutated
    /// independently of the plan. It is just stored
    /// in the plan for convenience: otherwise we'd
    /// have to explictly pass context to every method
    /// of the pipeline.
    #[serde(skip)]
    pub context: Option<RefCell<BuildContext>>,
    /// Any sharded table must belongs to a single tier.
    /// Option::None is for: global tables or when the concept of tiers
    /// is not applicable - for example cartridge case(vshard.router.static used).
    #[serde(skip)]
    pub tier: Option<SmolStr>,
}

/// Helper structures used to build the plan
/// on the router.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct BuildContext {
    shard_col_info: ShardColumnsMap,
}

impl BuildContext {
    /// Returns positions in node's output
    /// referring to the shard column.
    ///
    /// # Errors
    /// - Invalid plan
    pub fn get_shard_columns_positions(
        &mut self,
        node_id: NodeId,
        plan: &Plan,
    ) -> Result<Option<&Positions>, SbroadError> {
        self.shard_col_info.get(node_id, plan)
    }
}

impl Default for Plan {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl Plan {
    /// Get mut reference to build context
    ///
    /// # Panics
    /// - There are other mut refs
    pub fn context_mut(&self) -> RefMut<'_, BuildContext> {
        self.context
            .as_ref()
            .expect("context always exists during plan build")
            .borrow_mut()
    }

    /// Add relation to the plan.
    ///
    /// If relation already exists, do nothing.
    pub fn add_rel(&mut self, table: Table) {
        self.relations.insert(table);
    }

    /// # Panics
    #[must_use]
    pub fn replace_with_stub(&mut self, dst_id: NodeId) -> NodeOwned {
        match dst_id.arena_type {
            ArenaType::Arena32 => {
                let node32 = self
                    .nodes
                    .arena32
                    .get_mut(usize::try_from(dst_id.offset).unwrap())
                    .unwrap();
                let stub = Node32::Invalid(Invalid {});
                let node32 = std::mem::replace(node32, stub);
                node32.into_owned()
            }
            ArenaType::Arena64 => {
                let node64 = self
                    .nodes
                    .arena64
                    .get_mut(usize::try_from(dst_id.offset).unwrap())
                    .unwrap();
                let stub = Node64::Invalid(Invalid {});
                let node64 = std::mem::replace(node64, stub);
                node64.into_owned()
            }
            ArenaType::Arena96 => {
                let node96 = self
                    .nodes
                    .arena96
                    .get_mut(usize::try_from(dst_id.offset).unwrap())
                    .unwrap();
                let stub = Node96::Invalid(Invalid {});
                let node96 = std::mem::replace(node96, stub);
                node96.into_owned()
            }
            ArenaType::Arena136 => {
                let node136 = self
                    .nodes
                    .arena136
                    .get_mut(usize::try_from(dst_id.offset).unwrap())
                    .unwrap();
                let stub = Node136::Invalid(Invalid {});
                let node136 = std::mem::replace(node136, stub);
                node136.into_owned()
            }
            ArenaType::Arena232 => {
                let node224 = self
                    .nodes
                    .arena224
                    .get_mut(usize::try_from(dst_id.offset).unwrap())
                    .unwrap();
                let stub = Node232::Invalid(Invalid {});
                let node224 = std::mem::replace(node224, stub);
                node224.into_owned()
            }
        }
    }

    /// Constructor for an empty plan structure.
    #[must_use]
    pub fn new() -> Self {
        Self::empty()
    }

    /// Construct an empty plan.
    pub fn empty() -> Self {
        Self {
            nodes: Nodes {
                arena32: Vec::new(),
                arena64: Vec::new(),
                arena96: Vec::new(),
                arena136: Vec::new(),
                arena224: Vec::new(),
            },
            relations: Relations::new(),
            slices: Slices { slices: vec![] },
            top: None,
            is_explain: false,
            undo: TransformationLog::new(),
            constants: Parameters::new(),
            raw_options: vec![],
            options: Options::default(),
            version_map: TableVersionMap::new(),
            pg_params_map: HashMap::new(),
            context: Some(RefCell::new(BuildContext::default())),
            tier: None,
        }
    }

    /// Validate options stored in `Plan.raw_options` and initialize
    /// `Plan`'s fields for corresponding options
    ///
    /// # Errors
    /// - Invalid parameter value for given option
    /// - The same option used more than once in `Plan.raw_options`
    /// - Option value already violated in current `Plan`
    /// - The given option does not work for this specific query
    #[allow(clippy::uninlined_format_args)]
    pub fn apply_options(&mut self) -> Result<(), SbroadError> {
        let mut used_options: HashSet<OptionKind> = HashSet::new();
        let options = std::mem::take(&mut self.raw_options);
        let values_count = {
            let mut values_count: Option<usize> = None;
            let mut bfs =
                BreadthFirst::with_capacity(|x| self.nodes.rel_iter(x), REL_CAPACITY, REL_CAPACITY);
            bfs.populate_nodes(self.get_top()?);
            let nodes = bfs.take_nodes();
            for level_node in nodes {
                let id = level_node.1;
                if let Relational::Insert(_) = self.get_relation_node(id)? {
                    let child_id = self.get_relational_child(id, 0)?;
                    if let Relational::Values(Values { children, .. }) =
                        self.get_relation_node(child_id)?
                    {
                        values_count = Some(children.len());
                    }
                }
            }
            values_count
        };
        for opt in options {
            if !used_options.insert(opt.kind.clone()) {
                return Err(SbroadError::Invalid(
                    Query,
                    Some(format_smolstr!(
                        "option {} specified more than once!",
                        opt.kind
                    )),
                ));
            }
            let OptionParamValue::Value { val } = opt.val else {
                return Err(SbroadError::Invalid(Entity::OptionSpec, None));
            };
            match opt.kind {
                OptionKind::VdbeMaxSteps => {
                    if values_count.is_some() {
                        warn!(
                            Option::from("apply_options"),
                            &format!("Option {} does not apply for insert with values", opt.kind)
                        );
                    }
                    if let Value::Unsigned(num) = val {
                        self.options.vdbe_max_steps = num;
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::OptionSpec,
                            Some(format_smolstr!(
                                "expected option {} to be unsigned got: {val:?}",
                                opt.kind
                            )),
                        ));
                    }
                }
                OptionKind::VTableMaxRows => {
                    if let Value::Unsigned(limit) = val {
                        if let Some(vtable_rows_count) = values_count {
                            if limit < vtable_rows_count as u64 {
                                return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                                    "Exceeded maximum number of rows ({limit}) in virtual table: {}",
                                    vtable_rows_count
                                )));
                            }
                        }
                        self.options.vtable_max_rows = limit;
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::OptionSpec,
                            Some(format_smolstr!(
                                "expected option {} to be unsigned got: {val:?}",
                                opt.kind
                            )),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Check if the plan arena is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Get a node by its pointer (position in the node arena).
    ///
    /// # Errors
    /// Returns `SbroadError` when the node with requested index
    /// doesn't exist.
    pub fn get_node(&self, id: NodeId) -> Result<Node, SbroadError> {
        match self.nodes.get(id) {
            None => Err(SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("from {:?} with index {}", id.arena_type, id.offset),
            )),
            Some(node) => Ok(node),
        }
    }

    /// Get a mutable node by its pointer (position in the node arena).
    ///
    /// # Errors
    /// Returns `SbroadError` when the node with requested index
    /// doesn't exist.
    pub fn get_mut_node(&mut self, id: NodeId) -> Result<MutNode, SbroadError> {
        match self.nodes.get_mut(id) {
            None => Err(SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("from {:?} with index {}", id.arena_type, id.offset),
            )),
            Some(node) => Ok(node),
        }
    }

    /// Get a top node of the plan tree.
    ///
    /// # Errors
    /// - top node is None (i.e. invalid plan)
    pub fn get_top(&self) -> Result<NodeId, SbroadError> {
        self.top
            .ok_or_else(|| SbroadError::Invalid(Entity::Plan, Some("plan tree top is None".into())))
    }

    /// Clone plan slices.
    #[must_use]
    pub fn clone_slices(&self) -> Slices {
        self.slices.clone()
    }

    /// Find source relational node from which given reference came.
    /// E.g. in case we're working with a reference under Projection with the following
    /// children struct:
    /// Projection <- given reference in the output
    ///     Selection
    ///         Scan
    /// the source would be the Scan. It could also be a Join or Union nodes.
    pub fn get_reference_source_relation(&self, ref_id: NodeId) -> Result<NodeId, SbroadError> {
        let mut ref_id = ref_id;
        let mut ref_node = self.get_expression_node(ref_id)?;
        if let Expression::Alias(Alias { child, .. }) = ref_node {
            ref_node = self.get_expression_node(*child)?;
            ref_id = *child;
        }
        let Expression::Reference(Reference { position, .. }) = ref_node else {
            panic!("Expected reference")
        };
        let ref_parent_node_id = self.get_relational_from_reference_node(ref_id)?;
        let ref_source_node = self.get_relation_node(ref_parent_node_id)?;
        match ref_source_node {
            Relational::Delete { .. } | Relational::Insert { .. } | Relational::Update { .. } => {
                panic!("Reference source search shouldn't reach DML node.")
            }
            Relational::Selection(Selection { output, .. })
            | Relational::Having(Having { output, .. })
            | Relational::OrderBy(OrderBy { output, .. })
            | Relational::Limit(Limit { output, .. }) => {
                let source_output_list = self.get_row_list(*output)?;
                let source_ref_id = source_output_list[*position];
                self.get_reference_source_relation(source_ref_id)
            }
            Relational::ScanRelation { .. }
            | Relational::Projection { .. }
            | Relational::SelectWithoutScan { .. }
            | Relational::ScanCte { .. }
            | Relational::GroupBy { .. }
            | Relational::Motion { .. }
            | Relational::ScanSubQuery { .. }
            | Relational::Join { .. }
            | Relational::Except { .. }
            | Relational::Intersect { .. }
            | Relational::UnionAll { .. }
            | Relational::Union { .. }
            | Relational::Values { .. } => Ok(ref_parent_node_id),
            Relational::ValuesRow { .. } => {
                panic!(
                    "Reference source search shouldn't reach unsupported node {ref_source_node:?}."
                )
            }
        }
    }

    /// Get relation in the plan by its name or returns error.
    ///
    /// # Errors
    /// - no relation with given name
    pub fn get_relation_or_error(&self, name: &str) -> Result<&Table, SbroadError> {
        self.relations.get(name).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Table,
                format_smolstr!("with name {}", to_user(name)),
            )
        })
    }

    /// Get relation of a scan node
    ///
    /// # Errors
    /// - Given node is not a scan
    pub fn get_scan_relation(&self, scan_id: NodeId) -> Result<&str, SbroadError> {
        let node = self.get_relation_node(scan_id)?;
        if let Relational::ScanRelation(ScanRelation { relation, .. }) = node {
            return Ok(relation.as_str());
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!("expected scan node, got: {node:?}")),
        ))
    }

    /// Get relation in the plan by its name or returns error.
    ///
    /// # Errors
    /// - invalid table name
    /// - invalid column index
    pub fn get_relation_column(
        &self,
        table_name: &str,
        col_idx: usize,
    ) -> Result<&Column, SbroadError> {
        self.get_relation_or_error(table_name)?
            .columns
            .get(col_idx)
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Column,
                    Some(format_smolstr!(
                        "invalid column position {col_idx} for table {table_name}"
                    )),
                )
            })
    }

    /// Check whether given expression contains aggregates.
    /// If `check_top` is false, the root expression node is not
    /// checked.
    ///
    /// # Errors
    /// - node is not an expression
    /// - invalid expression tree
    pub fn contains_aggregates(
        &self,
        expr_id: NodeId,
        check_top: bool,
    ) -> Result<bool, SbroadError> {
        let filter = |id: NodeId| -> bool {
            matches!(
                self.get_node(id),
                Ok(Node::Expression(Expression::StableFunction(_)))
            )
        };
        let mut dfs = PostOrderWithFilter::with_capacity(
            |x| self.nodes.expr_iter(x, false),
            EXPR_CAPACITY,
            Box::new(filter),
        );
        for level_node in dfs.iter(expr_id) {
            let id = level_node.1;
            if !check_top && id == expr_id {
                continue;
            }
            if let Node::Expression(Expression::StableFunction(StableFunction { name, .. })) =
                self.get_node(id)?
            {
                if Expression::is_aggregate_name(name) {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Get relational node and produce a new row without aliases from its output (row with aliases).
    ///
    /// # Panics
    /// # Errors
    /// - node is not relational
    /// - node's output is not a row of aliases
    pub fn get_row_from_rel_node(&mut self, node: NodeId) -> Result<NodeId, SbroadError> {
        let n = self.get_node(node)?;
        if let Node::Relational(ref rel) = n {
            if let Node::Expression(Expression::Row(Row { list, .. })) =
                self.get_node(rel.output())?
            {
                let mut cols: Vec<NodeId> = Vec::with_capacity(list.len());
                for alias in list {
                    if let Node::Expression(Expression::Alias(Alias { child, .. })) =
                        self.get_node(*alias)?
                    {
                        cols.push(*child);
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Node,
                            Some("node's output is not a row of aliases".into()),
                        ));
                    }
                }
                return Ok(self.nodes.add_row(cols, None));
            }
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!("node is not Relational type: {n:?}")),
        ))
    }

    /// Add condition node to the plan.
    ///
    /// # Errors
    /// Returns `SbroadError` when the condition node can't append'.
    pub fn add_cond(
        &mut self,
        left: NodeId,
        op: operator::Bool,
        right: NodeId,
    ) -> Result<NodeId, SbroadError> {
        self.nodes.add_bool(left, op, right)
    }

    /// Add Like operator to the plan.
    ///
    /// # Errors
    /// Returns `SbroadError` when the escape pattern is more than 1 char.
    pub fn add_like(
        &mut self,
        left: NodeId,
        right: NodeId,
        escape_id: Option<NodeId>,
    ) -> Result<NodeId, SbroadError> {
        let escape_id = if let Some(id) = escape_id {
            id
        } else {
            let s_id = self.add_const(Value::String('\\'.into()));
            self.nodes.add_row(vec![s_id], None)
        };
        let node = Like {
            left,
            right,
            escape: escape_id,
        };
        Ok(self.nodes.push(node.into()))
    }

    /// Add node covered with parentheses to the plan.
    ///
    /// # Errors
    /// Returns `SbroadError` when the condition node can't append'.
    pub fn add_covered_with_parentheses(&mut self, child: NodeId) -> NodeId {
        self.nodes.add_covered_with_parentheses(child)
    }

    /// Add arithmetic node to the plan.
    ///
    /// # Errors
    /// Returns `SbroadError` when the condition node can't append'.
    pub fn add_arithmetic_to_plan(
        &mut self,
        left: NodeId,
        op: Arithmetic,
        right: NodeId,
    ) -> Result<NodeId, SbroadError> {
        self.nodes.add_arithmetic_node(left, op, right)
    }

    /// Add unary operator node to the plan.
    ///
    /// # Errors
    /// - Child node is invalid
    pub fn add_unary(&mut self, op: operator::Unary, child: NodeId) -> Result<NodeId, SbroadError> {
        self.nodes.add_unary_bool(op, child)
    }

    /// Add CASE ... END operator to the plan.
    pub fn add_case(
        &mut self,
        search_expr: Option<NodeId>,
        when_blocks: Vec<(NodeId, NodeId)>,
        else_expr: Option<NodeId>,
    ) -> NodeId {
        self.nodes.push(
            Case {
                search_expr,
                when_blocks,
                else_expr,
            }
            .into(),
        )
    }

    /// Add bool operator node to the plan.
    ///
    /// # Errors
    /// - Children node are invalid
    pub fn add_bool(
        &mut self,
        left: NodeId,
        op: Bool,
        right: NodeId,
    ) -> Result<NodeId, SbroadError> {
        self.nodes.add_bool(left, op, right)
    }

    /// Marks plan as query explain
    pub fn mark_as_explain(&mut self) {
        self.is_explain = true;
    }

    /// Checks that plan is explain query
    #[must_use]
    pub fn is_explain(&self) -> bool {
        self.is_explain
    }

    /// Checks that plan is a block of queries.
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_block(&self) -> Result<bool, SbroadError> {
        let top_id = self.get_top()?;
        let top = self.get_node(top_id)?;
        Ok(matches!(top, Node::Block(_)))
    }

    /// Checks that plan is a dml query on global table.
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_dml_on_global_table(&self) -> Result<bool, SbroadError> {
        let top_id = self.get_top()?;
        if !self.get_relation_node(top_id)?.is_dml() {
            return Ok(false);
        }
        Ok(self.dml_node_table(top_id)?.is_global())
    }

    /// Checks that plan is DDL query
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_ddl(&self) -> Result<bool, SbroadError> {
        let top_id = self.get_top()?;
        let top = self.get_node(top_id)?;
        Ok(matches!(top, Node::Ddl(_)))
    }

    /// Checks that plan is ACL query.
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_acl(&self) -> Result<bool, SbroadError> {
        let top_id = self.get_top()?;
        let top = self.get_node(top_id)?;
        Ok(matches!(top, Node::Acl(_)))
    }

    /// Checks that plan is a plugin query.
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_plugin(&self) -> Result<bool, SbroadError> {
        let top_id = self.get_top()?;
        Ok(matches!(self.get_node(top_id)?, Node::Plugin(_)))
    }

    /// Set top node of plan
    /// # Errors
    /// - top node doesn't exist in the plan.
    pub fn set_top(&mut self, top: NodeId) -> Result<(), SbroadError> {
        self.get_node(top)?;
        self.top = Some(top);
        Ok(())
    }

    /// Get relation type node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not a relational type
    pub fn get_relation_node(&self, node_id: NodeId) -> Result<Relational, SbroadError> {
        let node = self.get_node(node_id)?;
        match node {
            Node::Relational(rel) => Ok(rel),
            Node::Expression(_)
            | Node::Parameter(..)
            | Node::Ddl(..)
            | Node::Invalid(..)
            | Node::Acl(..)
            | Node::Block(..)
            | Node::Plugin(..) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("node is not Relational type: {node:?}")),
            )),
        }
    }

    /// Get mutable relation type node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not a relational type
    pub fn get_mut_relation_node(&mut self, node_id: NodeId) -> Result<MutRelational, SbroadError> {
        match self.get_mut_node(node_id)? {
            MutNode::Relational(rel) => Ok(rel),
            MutNode::Expression(_)
            | MutNode::Parameter(..)
            | MutNode::Ddl(..)
            | MutNode::Invalid(..)
            | MutNode::Acl(..)
            | MutNode::Plugin(..)
            | MutNode::Block(..) => Err(SbroadError::Invalid(
                Entity::Node,
                Some("Node is not relational".into()),
            )),
        }
    }

    /// Get expression type node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not expression type
    pub fn get_expression_node(&self, node_id: NodeId) -> Result<Expression, SbroadError> {
        match self.get_node(node_id)? {
            Node::Expression(exp) => Ok(exp),
            Node::Parameter(..) => {
                if let Some(Node64::Constant(constant)) = self.constants.get(node_id) {
                    return Ok(Expression::Constant(constant));
                }

                Err(SbroadError::Invalid(
                    Entity::Node,
                    Some("parameter node does not refer to an expression".into()),
                ))
            }
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some("node is not Expression type".into()),
            )),
        }
    }

    /// Return Reference if this `node_id` refers to it,
    /// otherwise return `None`.
    pub fn get_reference(&self, node_id: NodeId) -> Option<&Reference> {
        if let Expression::Reference(r) = self.get_expression_node(node_id).ok()? {
            return Some(r);
        }
        None
    }

    /// Get mutable expression type node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not expression type
    pub fn get_mut_expression_node(
        &mut self,
        node_id: NodeId,
    ) -> Result<MutExpression, SbroadError> {
        let node = self.get_mut_node(node_id)?;
        match node {
            MutNode::Expression(exp) => Ok(exp),
            MutNode::Relational(_)
            | MutNode::Parameter(..)
            | MutNode::Ddl(..)
            | MutNode::Invalid(..)
            | MutNode::Acl(..)
            | MutNode::Plugin(..)
            | MutNode::Block(..) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!(
                    "node ({node_id}) is not expression type: {node:?}"
                )),
            )),
        }
    }

    /// Gets list of `Row` children ids
    ///
    /// # Errors
    /// - supplied id does not correspond to `Row` node
    pub fn get_row_list(&self, row_id: NodeId) -> Result<&Vec<NodeId>, SbroadError> {
        if let Expression::Row(Row { list, .. }) = self.get_expression_node(row_id)? {
            return Ok(list);
        }

        Err(SbroadError::Invalid(
            Entity::Expression,
            Some("node is not Row".into()),
        ))
    }

    /// Helper function to get id of node under alias node,
    /// or return the given id if node is not an alias.
    ///
    /// # Errors
    /// - node is not an expression node
    pub fn get_child_under_alias(&self, child_id: NodeId) -> Result<NodeId, SbroadError> {
        if let Expression::Alias(Alias { child, .. }) = self.get_expression_node(child_id)? {
            return Ok(*child);
        }

        Ok(child_id)
    }

    /// Gets mut list of `Row` children ids
    ///
    /// # Errors
    /// - supplied id does not correspond to `Row` node
    pub fn get_mut_row_list(&mut self, row_id: NodeId) -> Result<&mut Vec<NodeId>, SbroadError> {
        if let MutExpression::Row(Row { list, .. }) = self.get_mut_expression_node(row_id)? {
            return Ok(list);
        }

        Err(SbroadError::Invalid(
            Entity::Expression,
            Some("node is not Row".into()),
        ))
    }

    /// Replace expression that is not root of the tree (== has parent)
    ///
    /// # Arguments
    /// * `parent_id` - id of the expression that is parent to expression being replaced
    /// * `old_id` - child of `parent_id` expression that is being replaced
    /// * `new_id` - id of expression that replaces `old_id` expression
    ///
    /// # Errors
    /// - invalid parent id
    /// - parent expression does not have specified child expression
    ///
    /// # Note
    /// This function assumes that parent expression does NOT have two or more
    /// children with the same id. So, if this happens, only one child will be replaced.
    #[allow(clippy::too_many_lines)]
    pub fn replace_expression(
        &mut self,
        parent_id: NodeId,
        old_id: NodeId,
        new_id: NodeId,
    ) -> Result<(), SbroadError> {
        match self.get_mut_expression_node(parent_id)? {
            MutExpression::Unary(UnaryExpr { child, .. })
            | MutExpression::ExprInParentheses(ExprInParentheses { child })
            | MutExpression::Alias(Alias { child, .. })
            | MutExpression::Cast(Cast { child, .. }) => {
                if *child == old_id {
                    *child = new_id;
                    return Ok(());
                }
            }
            MutExpression::Case(Case {
                search_expr,
                when_blocks,
                else_expr,
            }) => {
                if let Some(search_expr) = search_expr {
                    if *search_expr == old_id {
                        *search_expr = new_id;
                        return Ok(());
                    }
                }
                for (cond_expr, res_expr) in when_blocks {
                    if *cond_expr == old_id {
                        *cond_expr = new_id;
                        return Ok(());
                    }
                    if *res_expr == old_id {
                        *res_expr = new_id;
                        return Ok(());
                    }
                }
                if let Some(else_expr) = else_expr {
                    if *else_expr == old_id {
                        *else_expr = new_id;
                        return Ok(());
                    }
                }
            }
            MutExpression::Bool(BoolExpr { left, right, .. })
            | MutExpression::Arithmetic(ArithmeticExpr { left, right, .. })
            | MutExpression::Concat(Concat { left, right, .. }) => {
                if *left == old_id {
                    *left = new_id;
                    return Ok(());
                }
                if *right == old_id {
                    *right = new_id;
                    return Ok(());
                }
            }
            MutExpression::Like(Like {
                escape,
                left,
                right,
            }) => {
                if *left == old_id {
                    *left = new_id;
                    return Ok(());
                }
                if *right == old_id {
                    *right = new_id;
                    return Ok(());
                }
                if *escape == old_id {
                    *escape = new_id;
                    return Ok(());
                }
            }
            MutExpression::Trim(Trim {
                pattern, target, ..
            }) => {
                if let Some(pattern_id) = pattern {
                    if *pattern_id == old_id {
                        *pattern_id = new_id;
                        return Ok(());
                    }
                }
                if *target == old_id {
                    *target = new_id;
                    return Ok(());
                }
            }
            MutExpression::Row(Row { list: arr, .. })
            | MutExpression::StableFunction(StableFunction { children: arr, .. }) => {
                for child in arr.iter_mut() {
                    if *child == old_id {
                        *child = new_id;
                        return Ok(());
                    }
                }
            }
            MutExpression::Constant { .. }
            | MutExpression::Reference { .. }
            | MutExpression::CountAsterisk { .. } => {}
        }
        Err(SbroadError::FailedTo(
            Action::Replace,
            Some(Entity::Expression),
            format_smolstr!("parent expression ({parent_id}) has no child with id {old_id}"),
        ))
    }

    /// Gets `GroupBy` column by idx
    ///
    /// # Errors
    /// - supplied index is out of range
    /// - node is not `GroupBy`
    pub fn get_groupby_col(
        &self,
        groupby_id: NodeId,
        col_idx: usize,
    ) -> Result<NodeId, SbroadError> {
        let node = self.get_relation_node(groupby_id)?;
        if let Relational::GroupBy(GroupBy { gr_cols, .. }) = node {
            let col_id = gr_cols.get(col_idx).ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                    "groupby column index out of range. Node: {node:?}"
                ))
            })?;
            return Ok(*col_id);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!("Expected GroupBy node. Got: {node:?}")),
        ))
    }

    /// Gets `Projection` column by idx
    ///
    /// # Errors
    /// - supplied index is out of range
    /// - node is not `Projection`
    pub fn get_proj_col(&self, proj_id: NodeId, col_idx: usize) -> Result<NodeId, SbroadError> {
        let node = self.get_relation_node(proj_id)?;
        if let Relational::Projection(Projection { output, .. }) = node {
            let col_id = self.get_row_list(*output)?.get(col_idx).ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                    "projection column index out of range. Node: {node:?}"
                ))
            })?;
            return Ok(*col_id);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!("Expected Projection node. Got: {node:?}")),
        ))
    }

    /// Gets `GroupBy` columns
    ///
    /// # Errors
    /// - node is not `GroupBy`
    pub fn get_grouping_cols(&self, groupby_id: NodeId) -> Result<&[NodeId], SbroadError> {
        let node = self.get_relation_node(groupby_id)?;
        if let Relational::GroupBy(GroupBy { gr_cols, .. }) = node {
            return Ok(gr_cols);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!("Expected GroupBy node. Got: {node:?}")),
        ))
    }

    /// Gets `GroupBy` columns to specified columns
    ///
    /// # Errors
    /// - node is not `GroupBy`
    pub fn set_grouping_cols(
        &mut self,
        groupby_id: NodeId,
        new_cols: Vec<NodeId>,
    ) -> Result<(), SbroadError> {
        let node = self.get_mut_relation_node(groupby_id)?;
        if let MutRelational::GroupBy(GroupBy { gr_cols, .. }) = node {
            *gr_cols = new_cols;
            return Ok(());
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!("Expected GroupBy node. Got: {node:?}")),
        ))
    }

    /// Get alias string for `Reference` node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not `Reference`
    /// - invalid references between nodes
    ///
    /// # Panics
    /// - Plan is in invalid state
    pub fn get_alias_from_reference_node(&self, node: &Expression) -> Result<&str, SbroadError> {
        let Expression::Reference(Reference {
            targets,
            position,
            parent,
            ..
        }) = node
        else {
            unreachable!("get_alias_from_reference_node: Node is not of a reference type");
        };

        let ref_node = if let Some(parent) = parent {
            self.get_relation_node(*parent)?
        } else {
            unreachable!("get_alias_from_reference_node: Reference node has no parent");
        };

        // In a case of insert we don't inspect children output tuple
        // but rather use target relation columns.
        if let Relational::Insert(Insert { ref relation, .. }) = ref_node {
            let rel = self
                .relations
                .get(relation)
                .unwrap_or_else(|| panic!("Relation {relation} is not found."));
            let col_name = rel
                .columns
                .get(*position)
                .unwrap_or_else(|| {
                    panic!("Not found column at position {position} at relation {rel:?}.")
                })
                .name
                .as_str();
            return Ok(col_name);
        }

        let ref_node_children = ref_node.children();

        let Some(targets) = targets else {
            unreachable!("get_alias_from_reference_node: No targets in reference");
        };
        let first_target = targets.first().expect("Reference targets list is empty");
        let ref_node_target_child =
            ref_node_children
                .get(*first_target)
                .unwrap_or_else(|| panic!("Failed to get target index {first_target:?} for reference {node:?} and ref_node [id = {parent:?}] {ref_node:?}"));

        let column_rel_node = self.get_relation_node(*ref_node_target_child)?;
        let column_expr_node = self.get_expression_node(column_rel_node.output())?;

        let col_alias_id = column_expr_node
            .get_row_list()?
            .get(*position)
            .unwrap_or_else(|| panic!("Column not found at position {position} in row list"));

        self.get_alias_name(*col_alias_id)
    }

    /// Gets alias node name.
    ///
    /// # Errors
    /// - node isn't `Alias`
    pub fn get_alias_name(&self, alias_id: NodeId) -> Result<&str, SbroadError> {
        if let Expression::Alias(Alias { name, .. }) = self.get_expression_node(alias_id)? {
            return Ok(name);
        }

        Err(SbroadError::Invalid(
            Entity::Expression,
            Some("node is not Alias".into()),
        ))
    }

    /// Set slices of the plan.
    pub fn set_slices(&mut self, slices: Vec<Vec<NodeId>>) {
        self.slices = slices.into();
    }

    /// # Errors
    /// - serialization error (to binary)
    pub fn pattern_id(&self, top_id: NodeId) -> Result<SmolStr, SbroadError> {
        let mut dfs = PostOrder::with_capacity(|x| self.subtree_iter(x, true), self.nodes.len());
        dfs.populate_nodes(top_id);
        let nodes = dfs.take_nodes();
        let mut plan_nodes: Vec<Node> = Vec::with_capacity(nodes.len());
        for level_node in nodes {
            let node = self.get_node(level_node.1)?;
            plan_nodes.push(node);
        }

        let bytes: Vec<u8> = bincode::serialize(&plan_nodes).map_err(|e| {
            SbroadError::FailedTo(
                Action::Serialize,
                None,
                format_smolstr!("plan nodes to binary: {e:?}"),
            )
        })?;

        let hash = Base64::encode_string(blake3::hash(&bytes).to_hex().as_bytes()).to_smolstr();
        Ok(hash)
    }
}

impl Plan {
    fn get_param_type(&self, param_id: NodeId) -> Result<Option<Type>, SbroadError> {
        let node = self.get_node(param_id)?;
        if let Node::Parameter(ty) = node {
            return Ok(ty.param_type);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!("node is not Parameter type: {node:?}")),
        ))
    }

    fn set_param_type(&mut self, param_id: NodeId, ty: Type) -> Result<(), SbroadError> {
        let node = self.get_mut_node(param_id)?;
        if let MutNode::Parameter(param) = node {
            param.param_type = Some(ty);
            Ok(())
        } else {
            Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("node is not Parameter type: {node:?}")),
            ))
        }
    }

    fn count_pg_parameters(&self) -> usize {
        self.pg_params_map
            .values()
            .fold(0, |p1, p2| std::cmp::max(p1, *p2 + 1)) // idx 0 stands for $1
    }

    /// Infer parameter types specified via cast.
    ///
    /// # Errors
    /// - Parameter type is ambiguous.
    ///
    /// # Panics
    /// - `self.pg_params_map` missed some parameters.
    pub fn infer_pg_parameters_types(
        &mut self,
        client_types: &[Option<Type>],
    ) -> Result<Vec<Type>, SbroadError> {
        let params_count = self.count_pg_parameters();
        if params_count < client_types.len() {
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "client provided {} types for {} parameters",
                client_types.len(),
                params_count
            )));
        }
        let mut inferred_types = vec![None; params_count];

        for (node_id, param_idx) in &self.pg_params_map {
            let param_type = self.get_param_type(*node_id)?;
            let inferred_type = inferred_types.get(*param_idx).unwrap_or_else(|| {
                panic!("param idx {param_idx} exceeds params count {params_count}")
            });
            let client_type = client_types.get(*param_idx).copied().flatten();
            match (param_type, inferred_type, client_type) {
                (_, _, Some(client_type)) => {
                    // Client provided an explicit type, no additional checks are required.
                    inferred_types[*param_idx] = Some(client_type);
                }
                (Some(param_type), Some(inferred_type), None) => {
                    if &param_type != inferred_type {
                        // We've inferred 2 different types for the same parameter.
                        return Err(TypeError::AmbiguousParameterType(
                            *param_idx,
                            param_type,
                            *inferred_type,
                        )
                        .into());
                    }
                }
                (Some(param_type), None, None) => {
                    // We've inferred a more specific type from the context.
                    inferred_types[*param_idx] = Some(param_type);
                }
                _ => {}
            }
        }

        let types = inferred_types
            .into_iter()
            .enumerate()
            .map(|(idx, ty)| ty.ok_or(TypeError::CouldNotDetermineParameterType(idx).into()))
            .collect::<Result<Vec<_>, SbroadError>>()?;

        // Specify inferred types in all parameters nodes, allowing to calculate the result type
        // for queries like `SELECT $1::int + $1`. Without this correction there will be an error
        // like int and scalar are not supported for arithmetic expression, despite of the fact
        // that the type of parameter was specified.
        //
        // TODO: Avoid cloning of self.pg_params_map.
        for (node_id, param_idx) in &self.pg_params_map.clone() {
            self.set_param_type(*node_id, types[*param_idx])?;
        }

        Ok(types)
    }
}

/// Target positions in the reference.
pub type Positions = [Option<Position>; 2];

/// Relational node id -> positions of columns in output that refer to sharding column.
pub type ShardColInfo = ahash::AHashMap<NodeId, Positions>;

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ShardColumnsMap {
    /// Maps node id to positions of bucket_id column in
    /// the node output. Currently we track only two
    /// bucket_id columns appearences for perf reasons.
    pub memo: ahash::AHashMap<NodeId, Positions>,
    /// ids of nodes which were inserted into the middle
    /// of the plan and changed the bucket_id columns
    /// positions and thus invalidated all the nodes
    /// in the memo which are located above this node.
    pub invalid_ids: ahash::AHashSet<NodeId>,
}

impl ShardColumnsMap {
    /// Update information about node's sharding column positions
    /// assuming that node's children positions were already computed.
    ///
    /// # Errors
    /// - invalid plan
    ///
    /// # Panics
    /// - invalid plan
    pub fn update_node(&mut self, node_id: NodeId, plan: &Plan) -> Result<(), SbroadError> {
        let node = plan.get_relation_node(node_id)?;
        match node {
            Relational::ScanRelation(ScanRelation { relation, .. }) => {
                let table = plan.get_relation_or_error(relation)?;
                if let Ok(Some(pos)) = table.get_bucket_id_position() {
                    self.memo.insert(node_id, [Some(pos), None]);
                }
                return Ok(());
            }
            Relational::Motion(Motion { policy, .. }) => {
                // Any motion node that moves data invalidates
                // bucket_id column selected from that space.
                // Even Segment policy is no help, because it only
                // creates index on virtual table but does not actually
                // add or update bucket_id column.
                if !matches!(policy, MotionPolicy::Local | MotionPolicy::LocalSegment(_)) {
                    return Ok(());
                }
            }
            _ => {}
        }

        let children = node.children();
        if children.is_empty() {
            return Ok(());
        };
        let children_contain_shard_positions = children.iter().any(|c| self.memo.contains_key(c));
        if !children_contain_shard_positions {
            // The children do not contain any shard columns, no need to check
            // the output.
            return Ok(());
        }

        let output_id = node.output();
        let output_len = plan.get_row_list(output_id)?.len();
        let mut new_positions = [None, None];
        for pos in 0..output_len {
            let output = plan.get_row_list(output_id)?;
            let alias_id = output.get(pos).expect("can't fail");
            let ref_id = plan.get_child_under_alias(*alias_id)?;
            // If there is a parameter under alias
            // and we haven't bound parameters yet,
            // we will get an error.
            let Ok(Expression::Reference(Reference {
                targets, position, ..
            })) = plan.get_expression_node(ref_id)
            else {
                continue;
            };
            let Some(targets) = targets else {
                continue;
            };

            let children = plan.get_relational_children(node_id)?;
            // For node with multiple targets (Union, Except, Intersect)
            // we need that ALL targets would refer to the shard column.
            let mut refers_to_shard_col = true;
            for target in targets {
                let child_id = children.get(*target).expect("invalid reference");
                let Some(positions) = self.memo.get(child_id) else {
                    refers_to_shard_col = false;
                    break;
                };
                if positions[0] != Some(*position) && positions[1] != Some(*position) {
                    refers_to_shard_col = false;
                    break;
                }
            }

            if refers_to_shard_col {
                if new_positions[0].is_none() {
                    new_positions[0] = Some(pos);
                } else if new_positions[0] == Some(pos) {
                    // Do nothing, we already have this position.
                } else {
                    new_positions[1] = Some(pos);

                    // We already tracked two positions,
                    // the node may have more, but we assume
                    // that's really rare case and just don't
                    // want to allocate more memory to track them.
                    break;
                }
            }
        }
        if new_positions[0].is_some() {
            self.memo.insert(node_id, new_positions);
        }
        Ok(())
    }

    /// Handle node insertion into the middle of the plan.
    /// Node insertion may invalidate already computed positions
    /// for all the nodes located above it (on the path from root to
    /// the inserted node). Currently only node that invalidates already
    /// computed positions is Motion (non-local).
    ///
    /// # Errors
    /// - Invalid plan
    ///
    /// # Panics
    /// - invalid plan
    pub fn handle_node_insertion(
        &mut self,
        node_id: NodeId,
        plan: &Plan,
    ) -> Result<(), SbroadError> {
        let node = plan.get_relation_node(node_id)?;
        if let Relational::Motion(Motion {
            policy, children, ..
        }) = node
        {
            if matches!(policy, MotionPolicy::Local | MotionPolicy::LocalSegment(_)) {
                return Ok(());
            }
            let child_id = children.first().expect("invalid plan");
            if let Some(positions) = self.memo.get(child_id) {
                if positions[0].is_some() || positions[1].is_some() {
                    self.invalid_ids.insert(node_id);
                }
            }
        }
        Ok(())
    }

    /// Get positions in the node's output which refer
    /// to the sharding columns.
    ///
    /// # Errors
    /// - Invalid plan
    pub fn get(&mut self, id: NodeId, plan: &Plan) -> Result<Option<&Positions>, SbroadError> {
        if !self.invalid_ids.is_empty() {
            self.update_subtree(id, plan)?;
        }
        Ok(self.memo.get(&id))
    }

    fn update_subtree(&mut self, node_id: NodeId, plan: &Plan) -> Result<(), SbroadError> {
        let mut dfs = PostOrder::with_capacity(|x| plan.nodes.rel_iter(x), REL_CAPACITY);
        for LevelNode(_, id) in dfs.iter(node_id) {
            self.update_node(id, plan)?;
            self.invalid_ids.remove(&id);
        }
        if plan.get_top()? != node_id {
            self.invalid_ids.insert(node_id);
        }
        Ok(())
    }
}

pub mod api;
mod explain;
#[cfg(test)]
pub mod tests;
