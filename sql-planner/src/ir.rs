//! Contains the logical plan tree and helpers.
use ahash::AHashMap;
use bitflags::bitflags;
use expression::Position;
use node::acl::{Acl, MutAcl};
use node::block::{Block, MutBlock};
use node::ddl::{Ddl, MutDdl};
use node::expression::{Expression, MutExpression};
use node::relational::{MutRelational, Relational};
use node::{Invalid, NodeAligned, Parameter};
use operator::{Arithmetic, Unary};
use options::{OptionParamValue, OptionSpec, Options};
use relation::Table;
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::cell::{RefCell, RefMut};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::slice::{Iter, IterMut};
use tree::traversal::LevelNode;
use types::UnrestrictedType;

use self::relation::Relations;
use self::transformation::redistribution::MotionPolicy;
use crate::errors::{Action, Entity, SbroadError};
use crate::executor::engine::helpers::to_user;
use crate::executor::engine::VersionMap;
use crate::frontend::sql::ir::SubtreeCloner;
use crate::ir::expression::{Comparator, VolatilityType};
use crate::ir::helpers::RepeatableState;
use crate::ir::index::Indexes;
use crate::ir::node::plugin::{MutPlugin, Plugin};
use crate::ir::node::tcl::Tcl;
use crate::ir::node::{
    Alias, ArenaType, ArithmeticExpr, BoolExpr, Case, Cast, Concat, Constant, GroupBy, Having,
    IndexExpr, Insert, Limit, Motion, MutNode, Node, Node136, Node232, Node32, Node64, Node96,
    NodeId, NodeOwned, OrderBy, Projection, Reference, ReferenceTarget, Row, ScalarFunction,
    ScanRelation, Selection, SubQueryReference, Trim, UnaryExpr,
};
use crate::ir::operator::{Bool, OrderByElement, OrderByEntity};
use crate::ir::relation::Column;
use crate::ir::tree::traversal::{PostOrder, PostOrderWithFilter, EXPR_CAPACITY, REL_CAPACITY};
use crate::ir::undo::TransformationLog;
use crate::ir::value::Value;

use self::node::{Bound, BoundType, Like, Over, Window};

// TODO: remove when rust version in bumped in module
#[allow(elided_lifetimes_in_associated_constant)]
pub mod acl;
pub mod aggregates;
pub mod block;
pub mod bucket;
pub mod ddl;
pub mod distribution;
pub mod expression;
pub mod function;
pub mod helpers;
pub mod index;
pub mod node;
pub mod operator;
pub mod options;
pub mod relation;
pub mod sharding_key;
pub mod transformation;
pub mod tree;
pub mod types;
pub mod undo;
pub mod value;

pub const DEFAULT_MAX_NUMBER_OF_CASTS: usize = 10;

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
    pub(crate) fn get(&self, id: NodeId) -> Option<Node<'_>> {
        match id.arena_type {
            ArenaType::Arena32 => self.arena32.get(id.offset as usize).map(|node| match node {
                Node32::Alias(alias) => Node::Expression(Expression::Alias(alias)),
                Node32::Arithmetic(arithm) => Node::Expression(Expression::Arithmetic(arithm)),
                Node32::Bool(bool) => Node::Expression(Expression::Bool(bool)),
                Node32::Concat(concat) => Node::Expression(Expression::Concat(concat)),
                Node32::Index(index) => Node::Expression(Expression::Index(index)),
                Node32::Cast(cast) => Node::Expression(Expression::Cast(cast)),
                Node32::CountAsterisk(count) => Node::Expression(Expression::CountAsterisk(count)),
                Node32::Like(like) => Node::Expression(Expression::Like(like)),
                Node32::Except(except) => Node::Relational(Relational::Except(except)),
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
                Node32::Deallocate(deallocate) => Node::Deallocate(deallocate),
                Node32::Tcl(tcl) => match *tcl {
                    Tcl::Begin => Node::Tcl(Tcl::Begin),
                    Tcl::Commit => Node::Tcl(Tcl::Commit),
                    Tcl::Rollback => Node::Tcl(Tcl::Rollback),
                },
                Node32::CreateSchema => Node::Ddl(Ddl::CreateSchema),
                Node32::DropSchema => Node::Ddl(Ddl::DropSchema),
                Node32::SubQueryReference(sq_reference) => {
                    Node::Expression(Expression::SubQueryReference(sq_reference))
                }
                Node32::Constant(constant) => Node::Expression(Expression::Constant(constant)),
                Node32::Parameter(param) => Node::Expression(Expression::Parameter(param)),
                Node32::Timestamp(lt) => Node::Expression(Expression::Timestamp(lt)),
                Node32::Backup(backup) => Node::Ddl(Ddl::Backup(backup)),
            }),
            ArenaType::Arena64 => self.arena64.get(id.offset as usize).map(|node| match node {
                Node64::AnonymousBlock(block) => Node::Block(Block::Anonymous(block)),
                Node64::Over(over) => Node::Expression(Expression::Over(over)),
                Node64::Case(case) => Node::Expression(Expression::Case(case)),
                Node64::Invalid(invalid) => Node::Invalid(invalid),
                Node64::CreateRole(create_role) => Node::Acl(Acl::CreateRole(create_role)),
                Node64::Delete(delete) => Node::Relational(Relational::Delete(delete)),
                Node64::DropIndex(drop_index) => Node::Ddl(Ddl::DropIndex(drop_index)),
                Node64::DropRole(drop_role) => Node::Acl(Acl::DropRole(drop_role)),
                Node64::DropTable(drop_table) => Node::Ddl(Ddl::DropTable(drop_table)),
                Node64::TruncateTable(truncate_table) => {
                    Node::Ddl(Ddl::TruncateTable(truncate_table))
                }
                Node64::Row(row) => Node::Expression(Expression::Row(row)),
                Node64::DropUser(drop_user) => Node::Acl(Acl::DropUser(drop_user)),
                Node64::GroupBy(group_by) => Node::Relational(Relational::GroupBy(group_by)),
                Node64::Having(having) => Node::Relational(Relational::Having(having)),
                Node64::Join(join) => Node::Relational(Relational::Join(join)),
                Node64::OrderBy(order_by) => Node::Relational(Relational::OrderBy(order_by)),
                Node64::CallProcedure(proc) => Node::Block(Block::CallProcedure(proc)),
                Node64::ScanCte(scan_cte) => Node::Relational(Relational::ScanCte(scan_cte)),
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
                Node96::Projection(proj) => Node::Relational(Relational::Projection(proj)),
                Node96::Reference(reference) => Node::Expression(Expression::Reference(reference)),
                Node96::DropProc(drop_proc) => Node::Ddl(Ddl::DropProc(drop_proc)),
                Node96::Insert(insert) => Node::Relational(Relational::Insert(insert)),
                Node96::Invalid(inv) => Node::Invalid(inv),
                Node96::ScalarFunction(stable_func) => {
                    Node::Expression(Expression::ScalarFunction(stable_func))
                }
                Node96::CreatePlugin(create) => Node::Plugin(Plugin::Create(create)),
                Node96::EnablePlugin(enable) => Node::Plugin(Plugin::Enable(enable)),
                Node96::DisablePlugin(disable) => Node::Plugin(Plugin::Disable(disable)),
                Node96::DropPlugin(drop) => Node::Plugin(Plugin::Drop(drop)),
                Node96::ScanRelation(scan_rel) => {
                    Node::Relational(Relational::ScanRelation(scan_rel))
                }
                Node96::AuditPolicy(audit_policy) => Node::Acl(Acl::AuditPolicy(audit_policy)),
                Node96::RenameIndex(rename) => Node::Ddl(Ddl::RenameIndex(rename)),
            }),
            ArenaType::Arena136 => self
                .arena136
                .get(id.offset as usize)
                .map(|node| match node {
                    Node136::AlterUser(alter_user) => Node::Acl(Acl::AlterUser(alter_user)),
                    Node136::AlterTable(alter_table) => Node::Ddl(Ddl::AlterTable(alter_table)),
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
                    Node136::Window(window) => Node::Expression(Expression::Window(window)),
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
    pub(crate) fn get_mut(&mut self, id: NodeId) -> Option<MutNode<'_>> {
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
                    Node32::Index(index) => MutNode::Expression(MutExpression::Index(index)),
                    Node32::Cast(cast) => MutNode::Expression(MutExpression::Cast(cast)),
                    Node32::CountAsterisk(count) => {
                        MutNode::Expression(MutExpression::CountAsterisk(count))
                    }
                    Node32::Like(like) => MutNode::Expression(MutExpression::Like(like)),
                    Node32::Except(except) => MutNode::Relational(MutRelational::Except(except)),
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
                    Node32::Deallocate(deallocate) => MutNode::Deallocate(deallocate),
                    Node32::Tcl(tcl) => match *tcl {
                        Tcl::Begin => MutNode::Tcl(node::tcl::Tcl::Begin),
                        Tcl::Commit => MutNode::Tcl(node::tcl::Tcl::Commit),
                        Tcl::Rollback => MutNode::Tcl(node::tcl::Tcl::Rollback),
                    },
                    Node32::CreateSchema => MutNode::Ddl(MutDdl::CreateSchema),
                    Node32::DropSchema => MutNode::Ddl(MutDdl::DropSchema),
                    Node32::SubQueryReference(sq_reference) => {
                        MutNode::Expression(MutExpression::SubQueryReference(sq_reference))
                    }
                    Node32::Constant(constant) => {
                        MutNode::Expression(MutExpression::Constant(constant))
                    }
                    Node32::Parameter(param) => {
                        MutNode::Expression(MutExpression::Parameter(param))
                    }
                    Node32::Timestamp(lt) => MutNode::Expression(MutExpression::Timestamp(lt)),
                    Node32::Backup(backup) => MutNode::Ddl(MutDdl::Backup(backup)),
                }),
            ArenaType::Arena64 => self
                .arena64
                .get_mut(id.offset as usize)
                .map(|node| match node {
                    Node64::AnonymousBlock(block) => MutNode::Block(MutBlock::Anonymous(block)),
                    Node64::Over(over) => MutNode::Expression(MutExpression::Over(over)),
                    Node64::Case(case) => MutNode::Expression(MutExpression::Case(case)),
                    Node64::Invalid(invalid) => MutNode::Invalid(invalid),
                    Node64::CreateRole(create_role) => {
                        MutNode::Acl(MutAcl::CreateRole(create_role))
                    }
                    Node64::Delete(delete) => MutNode::Relational(MutRelational::Delete(delete)),
                    Node64::DropIndex(drop_index) => MutNode::Ddl(MutDdl::DropIndex(drop_index)),
                    Node64::Row(row) => MutNode::Expression(MutExpression::Row(row)),
                    Node64::DropRole(drop_role) => MutNode::Acl(MutAcl::DropRole(drop_role)),
                    Node64::DropTable(drop_table) => MutNode::Ddl(MutDdl::DropTable(drop_table)),
                    Node64::TruncateTable(truncate_table) => {
                        MutNode::Ddl(MutDdl::TruncateTable(truncate_table))
                    }
                    Node64::DropUser(drop_user) => MutNode::Acl(MutAcl::DropUser(drop_user)),
                    Node64::GroupBy(group_by) => {
                        MutNode::Relational(MutRelational::GroupBy(group_by))
                    }
                    Node64::Having(having) => MutNode::Relational(MutRelational::Having(having)),
                    Node64::Join(join) => MutNode::Relational(MutRelational::Join(join)),
                    Node64::OrderBy(order_by) => {
                        MutNode::Relational(MutRelational::OrderBy(order_by))
                    }
                    Node64::CallProcedure(proc) => MutNode::Block(MutBlock::CallProcedure(proc)),
                    Node64::ScanCte(scan_cte) => {
                        MutNode::Relational(MutRelational::ScanCte(scan_cte))
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
                    Node96::Projection(proj) => {
                        MutNode::Relational(MutRelational::Projection(proj))
                    }
                    Node96::Reference(reference) => {
                        MutNode::Expression(MutExpression::Reference(reference))
                    }
                    Node96::DropProc(drop_proc) => MutNode::Ddl(MutDdl::DropProc(drop_proc)),
                    Node96::Insert(insert) => MutNode::Relational(MutRelational::Insert(insert)),
                    Node96::Invalid(inv) => MutNode::Invalid(inv),
                    Node96::ScalarFunction(scalar_func) => {
                        MutNode::Expression(MutExpression::ScalarFunction(scalar_func))
                    }
                    Node96::CreatePlugin(create) => MutNode::Plugin(MutPlugin::Create(create)),
                    Node96::EnablePlugin(enable) => MutNode::Plugin(MutPlugin::Enable(enable)),
                    Node96::DisablePlugin(disable) => MutNode::Plugin(MutPlugin::Disable(disable)),
                    Node96::DropPlugin(drop) => MutNode::Plugin(MutPlugin::Drop(drop)),
                    Node96::AuditPolicy(audit_policy) => {
                        MutNode::Acl(MutAcl::AuditPolicy(audit_policy))
                    }
                    Node96::RenameIndex(rename) => MutNode::Ddl(MutDdl::RenameIndex(rename)),
                    Node96::ScanRelation(scan_rel) => {
                        MutNode::Relational(MutRelational::ScanRelation(scan_rel))
                    }
                }),
            ArenaType::Arena136 => {
                self.arena136
                    .get_mut(id.offset as usize)
                    .map(|node| match node {
                        Node136::AlterUser(alter_user) => {
                            MutNode::Acl(MutAcl::AlterUser(alter_user))
                        }
                        Node136::AlterTable(alter_table) => {
                            MutNode::Ddl(MutDdl::AlterTable(alter_table))
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
                        Node136::Window(window) => {
                            MutNode::Expression(MutExpression::Window(window))
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
    pub fn iter32_mut(&mut self) -> IterMut<'_, Node32> {
        self.arena32.iter_mut()
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

    /// Replace a node in arena 32 with another one.
    ///
    /// # Errors
    /// - The node with the given position doesn't exist.
    pub fn replace32(&mut self, id: NodeId, node: Node32) -> Result<Node32, SbroadError> {
        let offset = id.offset as usize;

        match id.arena_type {
            ArenaType::Arena32 => {
                if offset >= self.arena32.len() {
                    return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "can't replace node with id {id:?} as it is out of arena bounds"
                    )));
                }
            }
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format_smolstr!(
                        "can't replace node: node {:?} is invalid",
                        node
                    )),
                ));
            }
        };

        let old_node = std::mem::replace(&mut self.arena32[offset], node);
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

bitflags! {
    #[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
    pub struct ExplainOptions: u8 {
        const Logical = 1;
        const Raw = 1 << 1;
        const Fmt = 1 << 2;
    }
}

impl ExplainOptions {
    #[inline(always)]
    pub fn facettes() -> Self {
        Self::Logical | Self::Raw
    }

    pub fn has_facette(&self) -> bool {
        self.intersects(Self::facettes())
    }
}

/// Logical plan tree structure.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Plan {
    /// Append only arena for the plan nodes.
    pub(crate) nodes: Nodes,
    /// Relations are stored in a hash-map, with a table name acting as a
    /// key to guarantee its uniqueness across the plan.
    pub relations: Relations,
    /// Indexes are stored in a hash-map, with an index name acting as a
    /// key to guarantee its uniqueness across the plan.
    pub indexes: Indexes,
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
    pub explain_options: ExplainOptions,
    /// The undo log keeps the history of the plan transformations. It can
    /// be used to revert the plan subtree to some previous snapshot if needed.
    pub(crate) undo: TransformationLog,
    /// Constants that were stashed during execution preparation. They will be passed as parameters.
    pub(crate) constants: Vec<Value>,
    /// Options that were passed by user in `Option` clause. Does not include
    /// options for DDL as those are handled separately. This field is used only
    /// for storing the order of options in `Option` clause. This is needed because
    /// the user can use query parameters in options, delaying the resolution of options
    /// until after the parameters are bound.
    pub raw_options: Vec<OptionSpec<OptionParamValue>>,
    /// SQL options. Initialized to defaults upon IR creation.
    /// Then bound to their effective values resolved from `raw_options` when calling `bind_params`.
    pub effective_options: Options,
    pub table_version_map: VersionMap,
    pub index_version_map: HashMap<[u32; 2], u64, RepeatableState>,
    /// Exists only on the router during plan build.
    /// RefCell is used because context can be mutated
    /// independently of the plan. It is just stored
    /// in the plan for convenience: otherwise we'd
    /// have to explictly pass context to every method
    /// of the pipeline.
    #[serde(skip)]
    pub context: Option<RefCell<BuildContext>>,
    /// Any sharded table must belongs to a single tier,
    /// global tables use `None`.
    #[serde(skip)]
    pub tier: Option<SmolStr>,
    /// Plan id stored for each motion subtree.
    /// Valid only for the original plan.
    /// Check out `materialize_motion` for more.
    #[serde(skip)]
    pub plan_id_cache: Rc<RefCell<AHashMap<NodeId, u64>>>,
}

/// Helper structures used to build the plan
/// on the router.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct BuildContext {
    shard_col_info: ShardColumnsMap,
    /// Any aliases that were used in the query. Used for resolving unnamed subqueries/joins
    used_aliases: HashSet<SmolStr>,
    unnamed_join_idx: u64,
    unnamed_subquery_idx: u64,
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

    pub fn set_used_aliases(&mut self, aliases: HashSet<SmolStr>) {
        debug_assert!(self.used_aliases.is_empty());
        self.used_aliases = aliases;
    }

    fn get_unique_name(&self, pattern: &str, mut counter: u64) -> (SmolStr, u64) {
        loop {
            let candidate_name = if counter == 0 {
                pattern.to_smolstr()
            } else {
                format_smolstr!("{}_{}", pattern, counter)
            };
            counter += 1;

            if self.used_aliases.contains(&candidate_name) {
                continue;
            }

            break (candidate_name, counter);
        }
    }
    pub fn get_unnamed_subquery_name(&mut self) -> SmolStr {
        let (name, counter) = self.get_unique_name("unnamed_subquery", self.unnamed_subquery_idx);
        self.unnamed_subquery_idx = counter;
        name
    }

    pub fn get_unnamed_join_name(&mut self) -> SmolStr {
        let (name, counter) = self.get_unique_name("unnamed_join", self.unnamed_join_idx);
        self.unnamed_join_idx = counter;
        name
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

    pub fn get_params(&self) -> &Vec<Value> {
        self.constants.as_ref()
    }

    pub fn get_nodes(&self) -> &Nodes {
        &self.nodes
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
            indexes: Indexes::new(),
            slices: Slices { slices: vec![] },
            top: None,
            explain_options: ExplainOptions::empty(),
            undo: TransformationLog::new(),
            constants: Vec::new(),
            raw_options: vec![],
            effective_options: Options::default(),
            table_version_map: VersionMap::with_hasher(RepeatableState),
            index_version_map: HashMap::with_hasher(RepeatableState),
            context: Some(RefCell::new(BuildContext::default())),
            tier: None,
            plan_id_cache: Rc::new(RefCell::new(AHashMap::new())),
        }
    }

    /// Validate options stored in `raw_options` and their usage.
    ///
    /// This will catch as many errors as possible without having access to query parameters,
    /// but in some cases a call to `bind_params` might still fail due to options usage.
    ///
    /// # Errors
    /// - Invalid parameter value for given option
    /// - The same option used more than once in `Plan.raw_options`
    /// - Option value already violated in current `Plan`
    #[allow(clippy::uninlined_format_args)]
    pub fn check_raw_options(&self) -> Result<(), SbroadError> {
        let resolved = self.resolve_raw_options(&self.raw_options, None);
        let lowered = options::lower_options(&resolved)?;
        self.validate_options_usage(&lowered)?;
        Ok(())
    }

    /// Resolve and validate options, writing the results to `effective_options`.
    ///
    /// # Errors
    /// - Invalid parameter value for given option
    /// - The same option used more than once in `Plan.raw_options`
    /// - Option value already violated in current `Plan`
    #[allow(clippy::uninlined_format_args)]
    fn apply_raw_options(
        &mut self,
        params: &[Value],
        defaults: Options,
    ) -> Result<(), SbroadError> {
        let resolved = self.resolve_raw_options(&self.raw_options, Some(params));
        let lowered = options::lower_options(&resolved)?;
        self.validate_options_usage(&lowered)?;
        self.effective_options = lowered.unwrap(defaults);
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
    pub fn get_node(&self, id: NodeId) -> Result<Node<'_>, SbroadError> {
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
    pub fn get_mut_node(&mut self, id: NodeId) -> Result<MutNode<'_>, SbroadError> {
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
            | Relational::GroupBy(GroupBy { output, .. })
            | Relational::Limit(Limit { output, .. }) => {
                let source_output_list = self.get_row_list(*output)?;
                let source_ref_id = source_output_list[*position];
                self.get_reference_source_relation(source_ref_id)
            }
            Relational::ScanRelation { .. }
            | Relational::Projection { .. }
            | Relational::SelectWithoutScan { .. }
            | Relational::ScanCte { .. }
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

    /// Check whether the expression subtree contains a scalar function
    /// accepted by `check_function`.
    ///
    /// If `check_top` is false, the root expression node itself is skipped.
    ///
    /// # Errors
    /// - node is not an expression
    /// - invalid expression tree
    fn contains_function<F>(
        &self,
        expr_id: NodeId,
        check_top: bool,
        check_function: F,
    ) -> Result<bool, SbroadError>
    where
        F: Fn(&ScalarFunction) -> bool,
    {
        let dfs = PostOrderWithFilter::new(
            |node| self.nodes.expr_iter(node, false),
            |node| {
                matches!(
                    self.get_node(node),
                    Ok(Node::Expression(Expression::ScalarFunction(_)))
                )
            },
            EXPR_CAPACITY,
        );
        for level_node in dfs.traverse_into_iter(expr_id) {
            let id = level_node.1;
            if !check_top && id == expr_id {
                continue;
            }
            if let Node::Expression(Expression::ScalarFunction(scalar_function)) =
                self.get_node(id)?
            {
                if check_function(scalar_function) {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Check whether given expression contains aggregates.
    /// If `check_top` is false, the root expression node is not
    /// checked.
    pub fn contains_aggregates(
        &self,
        expr_id: NodeId,
        check_top: bool,
    ) -> Result<bool, SbroadError> {
        self.contains_function(expr_id, check_top, |scalar_function| {
            Expression::is_aggregate_name(&scalar_function.name)
        })
    }

    /// Check whether given expression contains volatile functions.
    /// If `check_top` is false, the root expression node is not
    /// checked.
    ///
    /// # Errors
    /// - node is not an expression
    /// - invalid expression tree
    pub fn contains_volatile(&self, expr_id: NodeId, check_top: bool) -> Result<bool, SbroadError> {
        self.contains_function(expr_id, check_top, |scalar_function| {
            matches!(scalar_function.volatility_type, VolatilityType::Volatile)
        })
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
            self.add_const(Value::String('\\'.into()))
        };
        let node = Like {
            left,
            right,
            escape: escape_id,
        };
        Ok(self.nodes.push(node.into()))
    }

    pub fn add_index(&mut self, child: NodeId, which: NodeId) -> Result<NodeId, SbroadError> {
        self.nodes.get(child).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("(left child of Index node) from arena with index {child:?}"),
            )
        })?;
        self.nodes.get(which).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("(right child of Index node) from arena with index {which:?}"),
            )
        })?;
        Ok(self.nodes.push(IndexExpr { child, which }.into()))
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

    #[must_use]
    pub fn is_logical_explain(&self) -> bool {
        self.explain_options.contains(ExplainOptions::Logical)
    }

    #[must_use]
    pub fn is_explain(&self) -> bool {
        !self.explain_options.is_empty()
    }

    #[must_use]
    pub fn is_raw_explain(&self) -> bool {
        self.explain_options.contains(ExplainOptions::Raw)
    }

    /// Checks that plan is a block of queries.
    pub fn is_block(&self) -> Result<bool, SbroadError> {
        let maybe_top_id = self.get_top();
        let maybe_top = maybe_top_id.and_then(|top_id| self.get_node(top_id));
        Ok(matches!(maybe_top, Ok(Node::Block(_))))
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

    /// Checks that plan is a dml query.
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_dml(&self) -> Result<bool, SbroadError> {
        if !self.is_dql_or_dml()? {
            return Ok(false);
        }
        let top_id = self.get_top()?;
        Ok(self.get_relation_node(top_id)?.is_dml())
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

    /// Checks that plan is TCL query.
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_tcl(&self) -> Result<bool, SbroadError> {
        let top_id = self.get_top()?;
        let top = self.get_node(top_id)?;
        Ok(matches!(top, Node::Tcl(_)))
    }

    /// Checks that plan is a plugin query.
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_plugin(&self) -> Result<bool, SbroadError> {
        let top_id = self.get_top()?;
        Ok(matches!(self.get_node(top_id)?, Node::Plugin(_)))
    }

    /// Replace the parser default timeout with the system default
    /// from `Options`, based on the IR node category.
    /// Only replaces if `source == TimeoutSource::Default`.
    pub fn resolve_default_timeout(&mut self, opts: &Options) {
        use node::MutNode;
        use options::TimeoutSource;

        let Ok(top_id) = self.get_top() else { return };
        let Ok(mut mut_node) = self.get_mut_node(top_id) else {
            return;
        };
        let timeout = match mut_node {
            MutNode::Ddl(ref mut ddl) => ddl.timeout_mut(),
            MutNode::Acl(ref mut acl) => Some(acl.timeout_mut()),
            MutNode::Plugin(ref mut plugin) => plugin.timeout_mut(),
            _ => None,
        };
        if let Some(t) = timeout {
            if t.source == TimeoutSource::Default {
                t.us = opts.sql_ddl_timeout_us;
            }
        }
    }

    pub fn is_backup(&self) -> Result<bool, SbroadError> {
        let top_id = self.get_top()?;
        Ok(matches!(self.get_node(top_id)?, Node::Ddl(Ddl::Backup(_))))
    }

    /// Checks that top node of the plan is insert.
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_insert(&self) -> Result<bool, SbroadError> {
        let top_id = self.get_top()?;

        Ok(matches!(
            self.get_node(top_id)?,
            Node::Relational(Relational::Insert(_))
        ))
    }

    /// Checks that top node of the plan is sharded insert.
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_sharded_insert(&self) -> Result<bool, SbroadError> {
        let top_id = self.get_top()?;
        if let Node::Relational(Relational::Insert(Insert { relation, .. })) =
            self.get_node(top_id)?
        {
            let table = self
                .relations
                .get(relation)
                .expect("relation must be present in Plan");
            return Ok(!table.is_global());
        }

        Ok(false)
    }

    /// Checks that plan is a deallocate query.
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_deallocate(&self) -> Result<bool, SbroadError> {
        let top_id = self.get_top()?;
        Ok(matches!(self.get_node(top_id)?, Node::Deallocate(_)))
    }

    /// Checks that plan is a DQL query.
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_dql(&self) -> Result<bool, SbroadError> {
        Ok(!self.is_empty()
            && !self.is_ddl()?
            && !self.is_acl()?
            && !self.is_plugin()?
            && !self.is_deallocate()?
            && !self.is_tcl()?
            && !self.is_block()?
            && !self.is_dml()?)
    }

    /// Checks that plan is a DQL or DML query.
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_dql_or_dml(&self) -> Result<bool, SbroadError> {
        Ok(!self.is_empty()
            && !self.is_ddl()?
            && !self.is_acl()?
            && !self.is_plugin()?
            && !self.is_deallocate()?
            && !self.is_tcl()?
            && !self.is_block()?)
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
    pub fn get_relation_node(&self, node_id: NodeId) -> Result<Relational<'_>, SbroadError> {
        let node = self.get_node(node_id)?;
        match node {
            Node::Relational(rel) => Ok(rel),
            Node::Expression(_)
            | Node::Ddl(..)
            | Node::Invalid(..)
            | Node::Acl(..)
            | Node::Tcl(..)
            | Node::Block(..)
            | Node::Plugin(..)
            | Node::Deallocate(..) => Err(SbroadError::Invalid(
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
    pub fn get_mut_relation_node(
        &mut self,
        node_id: NodeId,
    ) -> Result<MutRelational<'_>, SbroadError> {
        match self.get_mut_node(node_id)? {
            MutNode::Relational(rel) => Ok(rel),
            MutNode::Expression(_)
            | MutNode::Ddl(..)
            | MutNode::Invalid(..)
            | MutNode::Acl(..)
            | MutNode::Tcl(..)
            | MutNode::Plugin(..)
            | MutNode::Deallocate(..)
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
    pub fn get_expression_node(&self, node_id: NodeId) -> Result<Expression<'_>, SbroadError> {
        match self.get_node(node_id)? {
            Node::Expression(exp) => Ok(exp),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some("node is not Expression type".into()),
            )),
        }
    }

    /// Calculate type of expression
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not expression type
    pub fn calculate_expression_type(
        &self,
        node_id: NodeId,
    ) -> Result<Option<UnrestrictedType>, SbroadError> {
        Ok(*self
            .get_expression_node(node_id)?
            .calculate_type(self)?
            .get())
    }

    /// Return Reference if this `node_id` refers to it,
    /// otherwise return `None`.
    pub fn get_reference(&self, node_id: NodeId) -> Option<&Reference> {
        if let Expression::Reference(r) = self.get_expression_node(node_id).ok()? {
            return Some(r);
        }
        None
    }

    /// Return Reference if this `node_id` refers to it,
    /// otherwise return `None`.
    pub fn get_sq_reference(&self, node_id: NodeId) -> Option<&SubQueryReference> {
        if let Expression::SubQueryReference(r) = self.get_expression_node(node_id).ok()? {
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
    ) -> Result<MutExpression<'_>, SbroadError> {
        let node = self.get_mut_node(node_id)?;
        match node {
            MutNode::Expression(exp) => Ok(exp),
            MutNode::Relational(_)
            | MutNode::Ddl(..)
            | MutNode::Invalid(..)
            | MutNode::Acl(..)
            | MutNode::Tcl(..)
            | MutNode::Plugin(..)
            | MutNode::Deallocate(..)
            | MutNode::Block(..) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!(
                    "node ({node_id}) is not expression type: {node:?}"
                )),
            )),
        }
    }

    /// Get vec of references from the subtree of the given expression.
    pub fn get_refs_from_subtree(&self, expr_id: NodeId) -> Result<Vec<NodeId>, SbroadError> {
        let dfs = PostOrderWithFilter::new(
            |node| self.nodes.expr_iter(node, false),
            |node| {
                matches!(
                    self.get_node(node),
                    Ok(Node::Expression(Expression::Reference(_)))
                )
            },
            EXPR_CAPACITY,
        );
        let ref_ids = dfs.traverse_into_iter(expr_id).map(|n| n.1).collect();
        Ok(ref_ids)
    }

    /// Get vec of subquery references from the subtree of the given expression.
    pub fn get_sq_refs_from_subtree(&self, expr_id: NodeId) -> Result<Vec<NodeId>, SbroadError> {
        let dfs = PostOrderWithFilter::new(
            |node| self.nodes.expr_iter(node, false),
            |node| {
                matches!(
                    self.get_node(node),
                    Ok(Node::Expression(Expression::SubQueryReference(_)))
                )
            },
            EXPR_CAPACITY,
        );
        let ref_ids = dfs.traverse_into_iter(expr_id).map(|n| n.1).collect();
        Ok(ref_ids)
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

    /// Return the child of an `Alias` node.
    ///
    /// # Errors
    /// - node is not an `Alias`
    fn get_alias_child(&self, alias_id: NodeId) -> Result<NodeId, SbroadError> {
        match self.get_expression_node(alias_id)? {
            Expression::Alias(Alias { child, .. }) => Ok(*child),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some("node is not Alias type".into()),
            )),
        }
    }

    pub fn get_child_under_cast(&self, child_id: NodeId) -> Result<NodeId, SbroadError> {
        let mut id = child_id;
        let mut node = self.get_expression_node(child_id)?;
        let mut attempt = 0;
        while let Expression::Cast(Cast { child, .. }) = node {
            if attempt > DEFAULT_MAX_NUMBER_OF_CASTS {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some(format_smolstr!(
                        "Too many cast, cannot uncover node({})",
                        child_id
                    )),
                ));
            }
            id = *child;
            node = self.get_expression_node(*child)?;
            attempt += 1;
        }

        Ok(id)
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
    pub fn replace_expression(
        &mut self,
        parent_id: NodeId,
        old_id: NodeId,
        new_id: NodeId,
    ) -> Result<(), SbroadError> {
        let mut expr = self.get_mut_expression_node(parent_id)?;
        for child in expr.expr_children_mut() {
            if *child == old_id {
                *child = new_id;
                return Ok(());
            }
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
        if let Relational::GroupBy(GroupBy { gr_exprs, .. }) = node {
            let col_id = gr_exprs.get(col_idx).ok_or_else(|| {
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
            Ok(*col_id)
        } else {
            Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("Expected Projection node. Got: {node:?}")),
            ))
        }
    }

    /// Gets `GroupBy` columns
    ///
    /// # Errors
    /// - node is not `GroupBy`
    pub fn get_grouping_exprs(&self, groupby_id: NodeId) -> Result<&[NodeId], SbroadError> {
        let node = self.get_relation_node(groupby_id)?;
        if let Relational::GroupBy(GroupBy { gr_exprs, .. }) = node {
            return Ok(gr_exprs);
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
    pub fn set_grouping_exprs(
        &mut self,
        groupby_id: NodeId,
        new_cols: Vec<NodeId>,
    ) -> Result<(), SbroadError> {
        let node = self.get_mut_relation_node(groupby_id)?;
        if let MutRelational::GroupBy(GroupBy { gr_exprs, .. }) = node {
            *gr_exprs = new_cols;
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
    /// - node is neither `Reference` nor `SubQueryReference`
    /// - invalid references between nodes
    ///
    /// # Panics
    /// - Plan is in invalid state
    pub fn get_alias_from_reference_node(&self, node: &Expression) -> Result<&str, SbroadError> {
        let (ref_node_target_child, position) = match node {
            Expression::Reference(Reference {
                target, position, ..
            }) => (
                target
                    .first()
                    .expect("Expected at least one node in reference"),
                position,
            ),
            Expression::SubQueryReference(SubQueryReference {
                rel_id, position, ..
            }) => (rel_id, position),
            _ => unreachable!("get_alias_from_reference_node: Node is not of a reference type"),
        };

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

    /// Find whether we should cover the child expression with parentheses.
    /// For a pair of parent and child expression by default we cover
    /// all children with parentheses in order to save info about precedence
    /// and associativity. But for some cases we'd like not to cover expressions
    /// with redundant parentheses in case we are 100% sure it won't break anything.
    pub fn should_cover_with_parentheses(
        &self,
        top_expr_id: NodeId,
        child_expr_id: NodeId,
    ) -> Result<bool, SbroadError> {
        let top = self.get_expression_node(top_expr_id)?;
        let child = self.get_expression_node(child_expr_id)?;

        let should_not_cover = matches!(
            (top, child),
            (
                Expression::ScalarFunction(_)
                    | Expression::Timestamp(_)
                    | Expression::Row(_)
                    | Expression::Alias(_)
                    | Expression::Trim(_)
                    | Expression::Case(_)
                    | Expression::Over(_)
                    | Expression::Window(_),
                _
            ) | (
                _,
                Expression::ScalarFunction(_)
                    | Expression::Trim(_)
                    | Expression::Timestamp(_)
                    | Expression::Index(_)
                    | Expression::CountAsterisk(_)
                    | Expression::Row(_)
                    | Expression::Reference(_)
                    | Expression::SubQueryReference(_)
                    | Expression::Constant(_)
                    | Expression::Cast(_)
                    | Expression::Parameter(_)
                    | Expression::Case(_)
                    | Expression::Over(_)
                    | Expression::Window(_)
                    | Expression::Unary(UnaryExpr {
                        op: Unary::Exists,
                        ..
                    })
            )
        );

        Ok(!should_not_cover)
    }

    /// Replace the plan slices with `slices`.
    ///
    /// This is used when rebuilding the execution layout after planner
    /// transformations that add or remove stage boundaries.
    pub fn set_slices(&mut self, slices: Vec<Vec<NodeId>>) {
        self.slices = slices.into();
    }

    /// Return the root of the executable subtree located under a `Motion` node.
    ///
    /// When the direct child is `ScanSubQuery` or `ScanCte`, the helper unwraps
    /// that wrapper and returns its relational child instead.
    ///
    /// # Errors
    /// - `node_id` is not a valid relational node
    /// - `node_id` is not a `Motion`
    /// - the `Motion` child is not supported as a local-stage root
    pub fn get_motion_subtree_root(&self, node_id: NodeId) -> Result<NodeId, SbroadError> {
        let top_id = &self.get_motion_child(node_id)?;
        let rel = self.get_relation_node(*top_id)?;
        match rel {
            Relational::ScanSubQuery { .. } | Relational::ScanCte { .. } => {
                self.get_first_rel_child(*top_id)
            }
            Relational::Except { .. }
            | Relational::GroupBy { .. }
            | Relational::OrderBy { .. }
            | Relational::Intersect { .. }
            | Relational::Join { .. }
            | Relational::Projection { .. }
            | Relational::ScanRelation { .. }
            | Relational::Selection { .. }
            | Relational::SelectWithoutScan { .. }
            | Relational::Union { .. }
            | Relational::UnionAll { .. }
            | Relational::Update { .. }
            | Relational::Values { .. }
            | Relational::Having { .. }
            | Relational::ValuesRow { .. }
            | Relational::Limit { .. } => Ok(*top_id),
            Relational::Motion { .. } | Relational::Insert { .. } | Relational::Delete { .. } => {
                Err(SbroadError::Invalid(
                    Entity::Relational,
                    Some(format_smolstr!("invalid motion child node: {rel:?}.")),
                ))
            }
        }
    }

    /// Extract the child from the Motion node.
    ///
    /// # Errors
    /// - `motion_id` is not a valid relational node
    /// - `motion_id` is not a `Motion`
    /// - `motion_id` does not contain a child
    pub(crate) fn get_motion_child(&self, motion_id: NodeId) -> Result<NodeId, SbroadError> {
        match self.get_relation_node(motion_id)? {
            Relational::Motion(_) => self.get_first_rel_child(motion_id),
            rel => Err(SbroadError::Invalid(
                Entity::Relational,
                Some(format_smolstr!("node ({motion_id}) is not motion: {rel:?}")),
            )),
        }
    }

    /// Translate a zero-based output position from the final stage to the
    /// corresponding position in the local pushed-down stage.
    ///
    /// # Errors
    /// - `position` is outside `position_mapping`
    fn get_order_by_output_position(
        &self,
        position_mapping: &[usize],
        position: usize,
        context: &str,
    ) -> Result<usize, SbroadError> {
        position_mapping.get(position).copied().ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Expression,
                Some(format_smolstr!(
                    "{context} position ({position}) is out of bounds"
                )),
            )
        })
    }

    /// Translate a one-based `ORDER BY` index from the final stage to the
    /// corresponding one-based index in the local pushed-down stage.
    ///
    /// # Errors
    /// - the index is zero
    /// - the referenced final output column is out of bounds
    fn get_order_by_index_value(
        &self,
        position_mapping: &[usize],
        value: usize,
    ) -> Result<usize, SbroadError> {
        let output_idx = value.checked_sub(1).ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Expression,
                Some("invalid ORDER BY index (0) for final projection output".into()),
            )
        })?;

        Ok(self.get_order_by_output_position(position_mapping, output_idx, "ORDER BY index")? + 1)
    }

    /// Compare two `ORDER BY` expressions after accounting for final-to-local
    /// output position remapping.
    ///
    /// This is used to deduplicate equivalent `ORDER BY` expressions before
    /// cloning them into the local stage.
    ///
    /// # Errors
    /// - either expression subtree is invalid
    /// - an expression contains a reference shape unsupported by limit pushdown
    /// - a remapped output position is out of bounds
    fn are_order_by_exprs_equal(
        &self,
        lhs: NodeId,
        rhs: NodeId,
        position_mapping: &[usize],
    ) -> Result<bool, SbroadError> {
        let lhs_expr = self.get_expression_node(lhs)?;
        let rhs_expr = self.get_expression_node(rhs)?;

        let cmp_expr_vec = |left: &[NodeId], right: &[NodeId]| -> Result<bool, SbroadError> {
            if left.len() != right.len() {
                return Ok(false);
            }

            for (left_id, right_id) in left.iter().zip(right.iter()) {
                if !self.are_order_by_exprs_equal(*left_id, *right_id, position_mapping)? {
                    return Ok(false);
                }
            }

            Ok(true)
        };

        match (lhs_expr, rhs_expr) {
            (
                Expression::Alias(Alias {
                    name: lhs_name,
                    child: lhs_child,
                }),
                Expression::Alias(Alias {
                    name: rhs_name,
                    child: rhs_child,
                }),
            ) => Ok(lhs_name == rhs_name
                && self.are_order_by_exprs_equal(*lhs_child, *rhs_child, position_mapping)?),
            (
                Expression::Bool(BoolExpr {
                    left: lhs_left,
                    op: lhs_op,
                    right: lhs_right,
                }),
                Expression::Bool(BoolExpr {
                    left: rhs_left,
                    op: rhs_op,
                    right: rhs_right,
                }),
            ) => Ok(lhs_op == rhs_op
                && self.are_order_by_exprs_equal(*lhs_left, *rhs_left, position_mapping)?
                && self.are_order_by_exprs_equal(*lhs_right, *rhs_right, position_mapping)?),
            (
                Expression::Arithmetic(ArithmeticExpr {
                    left: lhs_left,
                    op: lhs_op,
                    right: lhs_right,
                }),
                Expression::Arithmetic(ArithmeticExpr {
                    left: rhs_left,
                    op: rhs_op,
                    right: rhs_right,
                }),
            ) => Ok(lhs_op == rhs_op
                && self.are_order_by_exprs_equal(*lhs_left, *rhs_left, position_mapping)?
                && self.are_order_by_exprs_equal(*lhs_right, *rhs_right, position_mapping)?),
            (
                Expression::Index(IndexExpr {
                    child: lhs_child,
                    which: lhs_which,
                }),
                Expression::Index(IndexExpr {
                    child: rhs_child,
                    which: rhs_which,
                }),
            ) => Ok(
                self.are_order_by_exprs_equal(*lhs_child, *rhs_child, position_mapping)?
                    && self.are_order_by_exprs_equal(*lhs_which, *rhs_which, position_mapping)?,
            ),
            (
                Expression::Cast(Cast {
                    child: lhs_child,
                    to: lhs_to,
                }),
                Expression::Cast(Cast {
                    child: rhs_child,
                    to: rhs_to,
                }),
            ) => Ok(lhs_to == rhs_to
                && self.are_order_by_exprs_equal(*lhs_child, *rhs_child, position_mapping)?),
            (
                Expression::Concat(Concat {
                    left: lhs_left,
                    right: lhs_right,
                }),
                Expression::Concat(Concat {
                    left: rhs_left,
                    right: rhs_right,
                }),
            ) => Ok(
                self.are_order_by_exprs_equal(*lhs_left, *rhs_left, position_mapping)?
                    && self.are_order_by_exprs_equal(*lhs_right, *rhs_right, position_mapping)?,
            ),
            (Expression::Constant(lhs_constant), Expression::Constant(rhs_constant)) => {
                Ok(lhs_constant == rhs_constant)
            }
            (
                Expression::Like(Like {
                    left: lhs_left,
                    right: lhs_right,
                    escape: lhs_escape,
                }),
                Expression::Like(Like {
                    left: rhs_left,
                    right: rhs_right,
                    escape: rhs_escape,
                }),
            ) => Ok(
                self.are_order_by_exprs_equal(*lhs_left, *rhs_left, position_mapping)?
                    && self.are_order_by_exprs_equal(*lhs_right, *rhs_right, position_mapping)?
                    && self.are_order_by_exprs_equal(*lhs_escape, *rhs_escape, position_mapping)?,
            ),
            (Expression::Reference(lhs_ref), Expression::Reference(rhs_ref)) => {
                if !matches!(lhs_ref.target, ReferenceTarget::Single(_))
                    || !matches!(rhs_ref.target, ReferenceTarget::Single(_))
                {
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some("ORDER BY pushdown supports only single-target references".into()),
                    ));
                }

                Ok(self.get_order_by_output_position(
                    position_mapping,
                    lhs_ref.position,
                    "ORDER BY reference",
                )? == self.get_order_by_output_position(
                    position_mapping,
                    rhs_ref.position,
                    "ORDER BY reference",
                )? && lhs_ref.col_type == rhs_ref.col_type
                    && lhs_ref.asterisk_source == rhs_ref.asterisk_source
                    && lhs_ref.is_system == rhs_ref.is_system)
            }
            (
                Expression::SubQueryReference(SubQueryReference {
                    rel_id: lhs_rel_id,
                    position: lhs_position,
                    col_type: lhs_col_type,
                }),
                Expression::SubQueryReference(SubQueryReference {
                    rel_id: rhs_rel_id,
                    position: rhs_position,
                    col_type: rhs_col_type,
                }),
            ) => Ok(lhs_rel_id == rhs_rel_id
                && lhs_position == rhs_position
                && lhs_col_type == rhs_col_type),
            (
                Expression::Row(Row { list: lhs_list, .. }),
                Expression::Row(Row { list: rhs_list, .. }),
            ) => cmp_expr_vec(lhs_list, rhs_list),
            (
                Expression::ScalarFunction(ScalarFunction {
                    name: lhs_name,
                    children: lhs_children,
                    feature: lhs_feature,
                    func_type: lhs_func_type,
                    is_system: lhs_is_system,
                    volatility_type: lhs_volatility_type,
                    is_window: lhs_is_window,
                }),
                Expression::ScalarFunction(ScalarFunction {
                    name: rhs_name,
                    children: rhs_children,
                    feature: rhs_feature,
                    func_type: rhs_func_type,
                    is_system: rhs_is_system,
                    volatility_type: rhs_volatility_type,
                    is_window: rhs_is_window,
                }),
            ) => Ok(lhs_name == rhs_name
                && lhs_feature == rhs_feature
                && lhs_func_type == rhs_func_type
                && lhs_is_system == rhs_is_system
                && lhs_volatility_type == rhs_volatility_type
                && lhs_is_window == rhs_is_window
                && cmp_expr_vec(lhs_children, rhs_children)?),
            (
                Expression::Trim(Trim {
                    kind: lhs_kind,
                    pattern: lhs_pattern,
                    target: lhs_target,
                }),
                Expression::Trim(Trim {
                    kind: rhs_kind,
                    pattern: rhs_pattern,
                    target: rhs_target,
                }),
            ) => {
                let patterns_equal = match (lhs_pattern, rhs_pattern) {
                    (Some(lhs_pattern), Some(rhs_pattern)) => {
                        self.are_order_by_exprs_equal(*lhs_pattern, *rhs_pattern, position_mapping)?
                    }
                    (None, None) => true,
                    _ => false,
                };

                Ok(lhs_kind == rhs_kind
                    && patterns_equal
                    && self.are_order_by_exprs_equal(*lhs_target, *rhs_target, position_mapping)?)
            }
            (
                Expression::Unary(UnaryExpr {
                    op: lhs_op,
                    child: lhs_child,
                }),
                Expression::Unary(UnaryExpr {
                    op: rhs_op,
                    child: rhs_child,
                }),
            ) => Ok(lhs_op == rhs_op
                && self.are_order_by_exprs_equal(*lhs_child, *rhs_child, position_mapping)?),
            (Expression::CountAsterisk(lhs_count), Expression::CountAsterisk(rhs_count)) => {
                Ok(lhs_count == rhs_count)
            }
            (
                Expression::Case(Case {
                    search_expr: lhs_search_expr,
                    when_blocks: lhs_when_blocks,
                    else_expr: lhs_else_expr,
                }),
                Expression::Case(Case {
                    search_expr: rhs_search_expr,
                    when_blocks: rhs_when_blocks,
                    else_expr: rhs_else_expr,
                }),
            ) => {
                if lhs_when_blocks.len() != rhs_when_blocks.len() {
                    return Ok(false);
                }

                let search_expr_equal = match (lhs_search_expr, rhs_search_expr) {
                    (Some(lhs_search_expr), Some(rhs_search_expr)) => self
                        .are_order_by_exprs_equal(
                            *lhs_search_expr,
                            *rhs_search_expr,
                            position_mapping,
                        )?,
                    (None, None) => true,
                    _ => false,
                };

                let else_expr_equal = match (lhs_else_expr, rhs_else_expr) {
                    (Some(lhs_else_expr), Some(rhs_else_expr)) => self.are_order_by_exprs_equal(
                        *lhs_else_expr,
                        *rhs_else_expr,
                        position_mapping,
                    )?,
                    (None, None) => true,
                    _ => false,
                };

                if !search_expr_equal || !else_expr_equal {
                    return Ok(false);
                }

                for ((lhs_cond, lhs_res), (rhs_cond, rhs_res)) in
                    lhs_when_blocks.iter().zip(rhs_when_blocks.iter())
                {
                    if !self.are_order_by_exprs_equal(*lhs_cond, *rhs_cond, position_mapping)?
                        || !self.are_order_by_exprs_equal(*lhs_res, *rhs_res, position_mapping)?
                    {
                        return Ok(false);
                    }
                }

                Ok(true)
            }
            (Expression::Timestamp(lhs_timestamp), Expression::Timestamp(rhs_timestamp)) => {
                Ok(lhs_timestamp == rhs_timestamp)
            }
            (
                Expression::Over(Over {
                    stable_func: lhs_stable_func,
                    filter: lhs_filter,
                    window: lhs_window,
                }),
                Expression::Over(Over {
                    stable_func: rhs_stable_func,
                    filter: rhs_filter,
                    window: rhs_window,
                }),
            ) => {
                let filters_equal = match (lhs_filter, rhs_filter) {
                    (Some(lhs_filter), Some(rhs_filter)) => {
                        self.are_order_by_exprs_equal(*lhs_filter, *rhs_filter, position_mapping)?
                    }
                    (None, None) => true,
                    _ => false,
                };

                Ok(filters_equal
                    && self.are_order_by_exprs_equal(
                        *lhs_stable_func,
                        *rhs_stable_func,
                        position_mapping,
                    )?
                    && self.are_order_by_exprs_equal(*lhs_window, *rhs_window, position_mapping)?)
            }
            (
                Expression::Window(Window {
                    partition: lhs_partition,
                    ordering: lhs_ordering,
                    frame: lhs_frame,
                }),
                Expression::Window(Window {
                    partition: rhs_partition,
                    ordering: rhs_ordering,
                    frame: rhs_frame,
                }),
            ) => {
                let partitions_equal = match (lhs_partition, rhs_partition) {
                    (Some(lhs_partition), Some(rhs_partition)) => {
                        cmp_expr_vec(lhs_partition, rhs_partition)?
                    }
                    (None, None) => true,
                    _ => false,
                };

                let ordering_equal = match (lhs_ordering, rhs_ordering) {
                    (Some(lhs_ordering), Some(rhs_ordering)) => {
                        if lhs_ordering.len() != rhs_ordering.len() {
                            return Ok(false);
                        }

                        let mut ordering_equal = true;
                        for (lhs_elem, rhs_elem) in lhs_ordering.iter().zip(rhs_ordering.iter()) {
                            if lhs_elem.order_type != rhs_elem.order_type {
                                return Ok(false);
                            }

                            ordering_equal &= match (&lhs_elem.entity, &rhs_elem.entity) {
                                (
                                    OrderByEntity::Expression {
                                        expr_id: lhs_expr_id,
                                    },
                                    OrderByEntity::Expression {
                                        expr_id: rhs_expr_id,
                                    },
                                ) => self.are_order_by_exprs_equal(
                                    *lhs_expr_id,
                                    *rhs_expr_id,
                                    position_mapping,
                                )?,
                                _ => lhs_elem.entity == rhs_elem.entity,
                            };
                        }

                        ordering_equal
                    }
                    (None, None) => true,
                    _ => false,
                };

                let frames_equal = match (lhs_frame, rhs_frame) {
                    (Some(lhs_frame), Some(rhs_frame)) => {
                        if lhs_frame.ty != rhs_frame.ty {
                            return Ok(false);
                        }

                        match (&lhs_frame.bound, &rhs_frame.bound) {
                            (Bound::Single(lhs_bound), Bound::Single(rhs_bound)) => {
                                match (lhs_bound, rhs_bound) {
                                    (
                                        BoundType::PrecedingOffset(lhs_offset),
                                        BoundType::PrecedingOffset(rhs_offset),
                                    )
                                    | (
                                        BoundType::FollowingOffset(lhs_offset),
                                        BoundType::FollowingOffset(rhs_offset),
                                    ) => self.are_order_by_exprs_equal(
                                        *lhs_offset,
                                        *rhs_offset,
                                        position_mapping,
                                    )?,
                                    _ => lhs_bound == rhs_bound,
                                }
                            }
                            (
                                Bound::Between(lhs_lower, lhs_upper),
                                Bound::Between(rhs_lower, rhs_upper),
                            ) => {
                                let lowers_equal = match (lhs_lower, rhs_lower) {
                                    (
                                        BoundType::PrecedingOffset(lhs_offset),
                                        BoundType::PrecedingOffset(rhs_offset),
                                    )
                                    | (
                                        BoundType::FollowingOffset(lhs_offset),
                                        BoundType::FollowingOffset(rhs_offset),
                                    ) => self.are_order_by_exprs_equal(
                                        *lhs_offset,
                                        *rhs_offset,
                                        position_mapping,
                                    )?,
                                    _ => lhs_lower == rhs_lower,
                                };
                                let uppers_equal = match (lhs_upper, rhs_upper) {
                                    (
                                        BoundType::PrecedingOffset(lhs_offset),
                                        BoundType::PrecedingOffset(rhs_offset),
                                    )
                                    | (
                                        BoundType::FollowingOffset(lhs_offset),
                                        BoundType::FollowingOffset(rhs_offset),
                                    ) => self.are_order_by_exprs_equal(
                                        *lhs_offset,
                                        *rhs_offset,
                                        position_mapping,
                                    )?,
                                    _ => lhs_upper == rhs_upper,
                                };

                                lowers_equal && uppers_equal
                            }
                            _ => false,
                        }
                    }
                    (None, None) => true,
                    _ => false,
                };

                Ok(partitions_equal && ordering_equal && frames_equal)
            }
            (Expression::Parameter(lhs_parameter), Expression::Parameter(rhs_parameter)) => {
                Ok(lhs_parameter == rhs_parameter)
            }
            _ => Ok(false),
        }
    }

    /// Clone a final-stage `ORDER BY` expression for the local stage and retarget
    /// its references to `target_sq_id`.
    ///
    /// Reference positions are remapped using `position_mapping`, which maps final
    /// projection output positions to local projection output positions.
    ///
    /// # Errors
    /// - the expression subtree cannot be cloned
    /// - the cloned subtree contains a reference shape unsupported by limit pushdown
    /// - a remapped output position is out of bounds
    fn translate_order_by_expr(
        &mut self,
        expr_id: NodeId,
        target_sq_id: NodeId,
        position_mapping: &[usize],
    ) -> Result<NodeId, SbroadError> {
        let expr_id = SubtreeCloner::clone_subtree(self, expr_id)?;
        let references = {
            let dfs = PostOrderWithFilter::new(
                |x| self.nodes.expr_iter(x, false),
                |node_id| {
                    matches!(
                        self.get_node(node_id),
                        Ok(Node::Expression(Expression::Reference(_)))
                    )
                },
                EXPR_CAPACITY,
            );
            dfs.traverse_into_vec(expr_id)
        };

        for LevelNode(_, ref_id) in references {
            let current_position = match self.get_expression_node(ref_id)? {
                Expression::Reference(Reference {
                    target, position, ..
                }) => {
                    if !matches!(target, ReferenceTarget::Single(_)) {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some("ORDER BY pushdown supports only single-target references".into()),
                        ));
                    }
                    *position
                }
                _ => unreachable!("expected Reference IR node"),
            };
            let new_position = self.get_order_by_output_position(
                position_mapping,
                current_position,
                "ORDER BY reference",
            )?;

            let MutExpression::Reference(Reference {
                target, position, ..
            }) = self.get_mut_expression_node(ref_id)?
            else {
                unreachable!("expected Reference IR node");
            };

            if !matches!(target, ReferenceTarget::Single(_)) {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some("ORDER BY pushdown supports only single-target references".into()),
                ));
            }

            *position = new_position;
            *target = ReferenceTarget::Single(target_sq_id);
        }

        Ok(expr_id)
    }

    /// Translate `OrderBy` elements from the final stage to the local stage.
    ///
    /// # Arguments
    /// * `order_by_id` - original `OrderBy` node from the final stage
    /// * `final_proj_id` - `Projection` node in the final stage (above `Motion`)
    /// * `target_sq_id` - `ScanSubQuery` target in the local stage
    ///
    /// # Errors
    /// - `order_by_id` is not an `OrderBy`
    /// - `final_proj_id` is invalid
    /// - expression cloning or reference remapping fails
    /// - an `ORDER BY` index or translated reference points outside the final output
    fn translate_order_by_elements(
        &mut self,
        order_by_id: NodeId,
        final_proj_id: NodeId,
        target_sq_id: NodeId,
    ) -> Result<Vec<OrderByElement>, SbroadError> {
        let final_proj_output_list = {
            let final_proj_output = self.get_relation_node(final_proj_id)?.output();
            self.get_row_list(final_proj_output)?
        };

        let position_mapping = {
            let mut position_mapping = vec![0; final_proj_output_list.len()];
            let mut unique_output_exprs = Vec::with_capacity(final_proj_output_list.len());
            for (final_proj_output_idx, alias_id) in final_proj_output_list.iter().enumerate() {
                let expr_id = self.get_alias_child(*alias_id)?;
                let mapped_position = {
                    let comparator = Comparator::new(self);
                    let mut mapped_position = None;
                    for (local_proj_output_idx, seen_expr_id) in
                        unique_output_exprs.iter().enumerate()
                    {
                        if comparator.are_subtrees_equal(*seen_expr_id, expr_id)? {
                            mapped_position = Some(local_proj_output_idx);
                            break;
                        }
                    }
                    mapped_position
                };

                if let Some(mapped_position) = mapped_position {
                    position_mapping[final_proj_output_idx] = mapped_position;
                } else {
                    position_mapping[final_proj_output_idx] = unique_output_exprs.len();
                    unique_output_exprs.push(expr_id);
                }
            }
            position_mapping
        };

        let order_by_len = match self.get_relation_node(order_by_id)? {
            Relational::OrderBy(OrderBy {
                order_by_elements, ..
            }) => order_by_elements.len(),
            rel => {
                return Err(SbroadError::Invalid(
                    Entity::Relational,
                    Some(format_smolstr!("expected OrderBy node, got: {rel:?}")),
                ))
            }
        };

        let mut translated_order_by_elems = Vec::with_capacity(order_by_len);
        let mut exprs = Vec::with_capacity(order_by_len);
        let mut indexes = HashSet::with_capacity(order_by_len);

        for idx in 0..order_by_len {
            let order_by_elem = match self.get_relation_node(order_by_id)? {
                Relational::OrderBy(OrderBy {
                    order_by_elements, ..
                }) => order_by_elements[idx].clone(),
                rel => {
                    return Err(SbroadError::Invalid(
                        Entity::Relational,
                        Some(format_smolstr!("expected OrderBy node, got: {rel:?}")),
                    ))
                }
            };

            match order_by_elem.entity {
                OrderByEntity::Expression { expr_id } => {
                    let mut seen = false;
                    for seen_expr_id in &exprs {
                        if self.are_order_by_exprs_equal(
                            *seen_expr_id,
                            expr_id,
                            &position_mapping,
                        )? {
                            seen = true;
                            break;
                        }
                    }
                    if seen {
                        continue;
                    }

                    exprs.push(expr_id);
                    translated_order_by_elems.push(OrderByElement {
                        entity: OrderByEntity::Expression {
                            expr_id: self.translate_order_by_expr(
                                expr_id,
                                target_sq_id,
                                &position_mapping,
                            )?,
                        },
                        order_type: order_by_elem.order_type,
                    });
                }
                OrderByEntity::Index { value } => {
                    let value = self.get_order_by_index_value(&position_mapping, value)?;
                    if indexes.insert(value) {
                        translated_order_by_elems.push(OrderByElement {
                            entity: OrderByEntity::Index { value },
                            order_type: order_by_elem.order_type,
                        });
                    }
                }
            }
        }

        Ok(translated_order_by_elems)
    }

    /// Clone an `OrderBy` node for the local stage.
    ///
    /// # Arguments
    /// * `old_order_by_id` - source `OrderBy` node id
    /// * `target_rel_id` - child of the new local `OrderBy` node
    /// * `final_proj_id` - final-stage `Projection` used for position remapping
    ///
    /// # Errors
    /// - `old_order_by_id` is invalid or not an `OrderBy`
    /// - the target local stage cannot be wrapped into a subquery
    /// - `ORDER BY` translation fails
    fn create_local_order_by(
        &mut self,
        old_order_by_id: NodeId,
        target_rel_id: NodeId,
        final_proj_id: NodeId,
    ) -> Result<(NodeId, NodeId), SbroadError> {
        let target_sq_id =
            if let Relational::ScanSubQuery(_) = self.get_relation_node(target_rel_id)? {
                target_rel_id
            } else {
                self.add_sub_query(target_rel_id, None)?
            };
        let new_order_by_elements =
            self.translate_order_by_elements(old_order_by_id, final_proj_id, target_sq_id)?;

        self.add_order_by(target_sq_id, new_order_by_elements)
    }

    /// Check whether `OrderBy` elements contain aggregates.
    ///
    /// This includes direct aggregate expressions and index-based ordering that
    /// points to an aggregate expression in the final projection.
    ///
    /// # Errors
    /// - `order_by_id` is invalid or not an `OrderBy`
    /// - `final_proj_id` is invalid
    /// - an `ORDER BY` index points outside the final projection output
    fn order_by_contains_aggregates_or_volatile(
        &self,
        order_by_id: NodeId,
        final_proj_id: NodeId,
    ) -> Result<bool, SbroadError> {
        let order_by_elements = match self.get_relation_node(order_by_id)? {
            Relational::OrderBy(OrderBy {
                order_by_elements, ..
            }) => order_by_elements,
            rel => {
                return Err(SbroadError::Invalid(
                    Entity::Relational,
                    Some(format_smolstr!("expected OrderBy node, got: {rel:?}")),
                ))
            }
        };

        let final_proj_output = self.get_relation_node(final_proj_id)?.output();
        let final_proj_output_list = self.get_row_list(final_proj_output)?;

        for order_by_elem in order_by_elements {
            match order_by_elem.entity {
                OrderByEntity::Expression { expr_id } => {
                    if self.contains_aggregates(expr_id, true)?
                        || self.contains_volatile(expr_id, true)?
                    {
                        return Ok(true);
                    }
                }
                OrderByEntity::Index { value } => {
                    let output_idx = value.checked_sub(1).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::Expression,
                            Some("invalid ORDER BY index (0) for final projection output".into()),
                        )
                    })?;
                    let Some(alias_id) = final_proj_output_list.get(output_idx) else {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some(format_smolstr!(
                                "invalid ORDER BY index ({value}) for final projection output"
                            )),
                        ));
                    };
                    let expr_id = self.get_alias_child(*alias_id)?;
                    if self.contains_aggregates(expr_id, true)?
                        || self.contains_volatile(expr_id, true)?
                    {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    /// Push down a `Limit` node to the nearest local stage.
    ///
    /// # Arguments
    /// * `limit_id` - source `Limit` node id
    ///
    /// # Errors
    /// - `limit_id` or one of the traversed relational nodes is invalid
    /// - local `ORDER BY` cloning or reference remapping fails
    /// - distribution metadata needed for pushdown is inconsistent
    ///
    /// # Panics
    /// - `limit_id` is not a `Limit` node
    pub(crate) fn pushdown_limit(&mut self, limit_id: NodeId) -> Result<(), SbroadError> {
        let Relational::Limit(Limit { child, limit, .. }) = self.get_relation_node(limit_id)?
        else {
            unreachable!("expected LIMIT IR Plan node");
        };

        let limit = *limit;
        let mut curr_node = *child;
        let mut order_by_id: Option<NodeId> = None;
        let mut final_proj_id: Option<NodeId> = None;
        let mut met_aggregates = false;
        // Suggested that all paths from Limit IR node to the nearest downward Motion IR node are the following:
        // Same query block:
        // - Limit -> Motion
        //     - Plain distributed query where the gather is the top relational step under LIMIT.
        // - Limit -> Projection -> Motion
        //     - Final reduce projection for aggregate queries without final HAVING/final GROUP BY.
        // - Limit -> Projection -> Having -> Motion
        //     - Final reduce stage with HAVING, no final GROUP BY.
        // - Limit -> Projection -> GroupBy -> Motion
        //     - Final grouped reduce stage, no HAVING.
        // - Limit -> Projection -> Having -> GroupBy -> Motion
        //     - Final grouped reduce stage with HAVING.
        // - Limit -> Projection -> OrderBy -> Motion
        //     - ORDER BY itself forces a gather.
        // - Limit -> Projection -> OrderBy -> ScanSubQuery -> Projection -> Motion
        //     - ORDER BY wrapper above a subtree whose nearest gather belongs to a lower final stage.
        // - Limit -> Projection -> OrderBy -> ScanSubQuery -> Projection -> Having -> Motion
        // - Limit -> Projection -> OrderBy -> ScanSubQuery -> Projection -> GroupBy -> Motion
        // - Limit -> Projection -> OrderBy -> ScanSubQuery -> Projection -> Having -> GroupBy -> Motion
        loop {
            match self.get_relation_node(curr_node)? {
                Relational::Projection(Projection {
                    windows,
                    group_by,
                    having,
                    output,
                    ..
                }) => {
                    if !windows.is_empty() || (order_by_id.is_some() && having.is_some()) {
                        break;
                    }
                    met_aggregates |= self.contains_aggregates(*output, false)?;
                    if let Some(group_by) = group_by {
                        met_aggregates |= self
                            .contains_aggregates(self.get_relational_output(*group_by)?, false)?;
                    }
                    if let Some(having) = having {
                        if let Relational::Having(Having { filter, output, .. }) =
                            self.get_relation_node(*having)?
                        {
                            met_aggregates |= self.contains_aggregates(*filter, true)?;
                            met_aggregates |= self.contains_aggregates(*output, false)?;
                        }
                    }
                    final_proj_id = Some(curr_node);
                }
                Relational::ScanCte(_) => {
                    break;
                }
                Relational::Motion(_) => {
                    let upper_local_node_id = self.get_motion_subtree_root(curr_node)?;
                    match self.get_relation_node(upper_local_node_id)? {
                        Relational::Projection(_)
                        | Relational::Except { .. }
                        | Relational::Union { .. }
                        | Relational::UnionAll { .. } => {}
                        _ => {
                            // Limit pushdown is not supported for other relational node types as the local stage root.
                            break;
                        }
                    }
                    if met_aggregates {
                        break;
                    }

                    let Some(final_proj_id) = final_proj_id else {
                        break;
                    };

                    // If there is no distribution under `Motion`, this `Limit`
                    // cannot be pushed down.
                    // E.g.
                    // SELECT emp, region, CAST((
                    // SELECT sum(total) OVER (
                    //     ORDER BY total RANGE BETWEEN UNBOUNDED PRECEDING
                    //     AND UNBOUNDED FOLLOWING
                    //     ) FROM sales LIMIT 1
                    // ) AS TEXT) || emp FROM sales
                    // ORDER BY emp;
                    let child_dist =
                        if let Ok(dist) = self.get_rel_distribution(upper_local_node_id) {
                            dist.clone()
                        } else {
                            break;
                        };

                    let new_limit_node_child = if let Some(order_by_id) = order_by_id {
                        // If `OrderBy::order_by_elements` contains no subqueries,
                        // clone it and adjust the subtree under `Motion`:
                        // Projection -> OrderBy -> ScanSubQuery -> <existing nodes>
                        if !self.get_relation_subqueries(order_by_id)?.is_empty() {
                            break;
                        }

                        let target_rel_id = self.get_motion_child(curr_node)?;

                        // The subtree under `Motion` is copied while preserving
                        // target positions, so we remap `Reference` positions in
                        // `OrderBy::order_by_elements` accordingly.
                        // This is consistent with the two-stage aggregation algorithm.
                        let (new_order_by, new_proj) =
                            self.create_local_order_by(order_by_id, target_rel_id, final_proj_id)?;
                        self.set_dist(
                            self.get_relational_output(new_order_by)?,
                            child_dist.clone(),
                        )?;
                        self.set_dist(self.get_relational_output(new_proj)?, child_dist.clone())?;

                        new_proj
                    } else {
                        upper_local_node_id
                    };

                    let local_limit_id = self.add_limit(new_limit_node_child, limit)?;
                    self.set_dist(
                        self.get_relational_output(local_limit_id)?,
                        child_dist.clone(),
                    )?;

                    let motion_output = self.get_relational_output(curr_node)?;
                    self.set_target_in_subtree(motion_output, local_limit_id)?;

                    if let MutRelational::Motion(Motion { child, .. }) =
                        self.get_mut_relation_node(curr_node)?
                    {
                        *child = Some(local_limit_id);
                    };
                    // Push the limit down only to the nearest local stage.
                    break;
                }
                Relational::OrderBy(_) => {
                    if let Some(final_proj_id) = final_proj_id {
                        met_aggregates |= self
                            .order_by_contains_aggregates_or_volatile(curr_node, final_proj_id)?;
                    }
                    order_by_id = Some(curr_node);
                }
                Relational::Having(_) | Relational::GroupBy(_) | Relational::ScanSubQuery(_) => {}
                Relational::Join(_)
                | Relational::Union(_)
                | Relational::UnionAll(_)
                | Relational::Except(_)
                | Relational::ScanRelation(_)
                | Relational::Values(_)
                | Relational::ValuesRow(_)
                | Relational::Intersect(_)
                | Relational::Selection(_)
                | Relational::SelectWithoutScan(_)
                | Relational::Update(_)
                | Relational::Insert(_)
                | Relational::Limit(_)
                | Relational::Delete(_) => {
                    // Do not push down if nodes above met before motion.
                    break;
                }
            }
            curr_node = self.get_first_rel_child(curr_node)?;
        }
        Ok(())
    }
}

impl Plan {
    /// Collect parameter types for DQL, DML or DO queries.
    ///
    /// Note that procedures can have parameters too, but they have special semantics so this
    /// function ignores them and returns an empty result.
    ///
    /// # Panics
    /// - If there are parameters with unknown types.
    pub fn collect_parameter_types(&self) -> Vec<UnrestrictedType> {
        if self.is_empty() {
            return Vec::new();
        }

        if !self
            .is_dql_or_dml()
            .expect("top must be valid when collecting parameter types")
            && !self
                .is_block()
                .expect("top must be valid when collecting parameter types")
        {
            return Vec::new();
        }

        let params_count = self
            .nodes
            .iter32()
            .map(|node| match node {
                Node32::Parameter(Parameter { index, .. }) => *index,
                _ => 0,
            })
            .max()
            .unwrap_or(0) as usize;

        let mut parameter_types = vec![UnrestrictedType::Any; params_count];

        for node in self.nodes.iter32() {
            if let Node32::Parameter(Parameter {
                param_type, index, ..
            }) = node
            {
                let index = (*index - 1) as usize;
                // TODO: We need to introduce ParameterType that cannot be unknown and store it in
                // Parameter nodes making parameters with unknown type impossible.
                parameter_types[index] = param_type
                    .get()
                    .expect("types must be known at this moment");
            }
        }

        parameter_types
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
        let subqueries = node.subqueries();
        if children.is_empty() && subqueries.is_empty() {
            return Ok(());
        };
        let children_contain_shard_positions = children
            .iter()
            .chain(subqueries.iter())
            .any(|c| self.memo.contains_key(c));
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
                target, position, ..
            })) = plan.get_expression_node(ref_id)
            else {
                continue;
            };

            if target.is_leaf() {
                continue;
            }

            // For node with multiple targets (Union, Except, Intersect)
            // we need that ALL targets would refer to the shard column.
            let mut refers_to_shard_col = true;
            for target in target.iter() {
                let Some(positions) = self.memo.get(target) else {
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
        if let Relational::Motion(Motion { policy, child, .. }) = node {
            if matches!(policy, MotionPolicy::Local | MotionPolicy::LocalSegment(_)) {
                return Ok(());
            }
            let child_id = child.expect("invalid plan");
            if let Some(positions) = self.memo.get(&child_id) {
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
        let dfs = PostOrder::new(|x| plan.nodes.rel_iter(x), REL_CAPACITY);
        for LevelNode(_, id) in dfs.traverse_into_iter(node_id) {
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
