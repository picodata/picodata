use std::collections::HashMap;

use ahash::AHashMap;
use pest::iterators::Pair;
use smol_str::format_smolstr;
use tarantool::decimal::Decimal;

use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::ast::Rule;
use crate::frontend::sql::parse_trimmed_unsigned_from_str;
use crate::ir::node::expression::{ExprOwned, Expression};
use crate::ir::node::relational::{MutRelational, RelOwned, Relational};
use crate::ir::node::{
    Delete, Except, GroupBy, Having, Insert, Intersect, Join, Limit, Motion, Node, NodeAligned,
    NodeId, OrderBy, Projection, Reference, ReferenceTarget, ScanCte, ScanRelation, ScanSubQuery,
    SelectWithoutScan, Selection, Union, UnionAll, Update, Values, ValuesRow,
};
use crate::ir::operator::{OrderByElement, OrderByEntity};
use crate::ir::transformation::redistribution::MotionOpcode;
use crate::ir::tree::traversal::{LevelNode, PostOrder};
use crate::ir::value::double::Double;
use crate::ir::value::Value;
use crate::ir::Plan;

use super::escape_single_quotes;

impl Value {
    /// Creates `Value` from pest pair.
    ///
    /// # Errors
    /// Returns `SbroadError` when the operator is invalid.
    #[allow(dead_code)]
    pub(super) fn from_node(pair: &Pair<Rule>) -> Result<Self, SbroadError> {
        let pair_string = pair.as_str();

        match pair.as_rule() {
            Rule::False => Ok(false.into()),
            Rule::True => Ok(true.into()),
            Rule::Null => Ok(Value::Null),
            Rule::Integer => Ok(pair_string
                .parse::<i64>()
                .map_err(|e| {
                    SbroadError::ParsingError(
                        Entity::Value,
                        format_smolstr!("i64 parsing error {e}"),
                    )
                })?
                .into()),
            Rule::Decimal => Ok(pair_string
                .parse::<Decimal>()
                .map_err(|e| {
                    SbroadError::ParsingError(
                        Entity::Value,
                        format_smolstr!("decimal parsing error {e:?}"),
                    )
                })?
                .into()),
            Rule::Double => Ok(pair_string
                .parse::<Double>()
                .map_err(|e| {
                    SbroadError::ParsingError(
                        Entity::Value,
                        format_smolstr!("double parsing error {e}"),
                    )
                })?
                .into()),
            Rule::Unsigned => {
                let unsigned = parse_trimmed_unsigned_from_str(pair_string)?;
                let value = unsigned.into();
                Ok(value)
            }
            Rule::SingleQuotedString => {
                let pair_str = pair.as_str();
                let inner = &pair_str[1..pair_str.len() - 1];
                Ok(escape_single_quotes(inner).into())
            }
            _ => Err(SbroadError::Unsupported(
                Entity::Type,
                Some("can not create Value from ParseNode".into()),
            )),
        }
    }
}

#[derive(Debug)]
/// Helper struct representing map of { `ParseNode` id -> `Node` id }
pub(super) struct Translation {
    map: HashMap<usize, NodeId>,
}

impl Translation {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Translation {
            map: HashMap::with_capacity(capacity),
        }
    }

    pub(super) fn add(&mut self, parse_id: usize, plan_id: NodeId) {
        self.map.insert(parse_id, plan_id);
    }

    pub(super) fn get(&self, old: usize) -> Result<NodeId, SbroadError> {
        self.map.get(&old).copied().ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("(parse node) [{old}] in translation map"),
            )
        })
    }
}

/// Helper struct to clone plan's subtree.
/// Assumes that all parameters are bound.
pub struct SubtreeCloner {
    old_new_map: AHashMap<NodeId, NodeId>,
    nodes_with_backward_references: Vec<NodeId>,
}

impl SubtreeCloner {
    fn new(capacity: usize) -> Self {
        SubtreeCloner {
            old_new_map: AHashMap::with_capacity(capacity),
            nodes_with_backward_references: Vec::new(),
        }
    }

    fn get_new_id(&self, old_id: NodeId) -> Result<NodeId, SbroadError> {
        self.old_new_map
            .get(&old_id)
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!("new node not found for old id: {old_id:?}")),
                )
            })
            .copied()
    }

    fn copy_list(&self, list: &[NodeId]) -> Result<Vec<NodeId>, SbroadError> {
        let mut new_list = Vec::with_capacity(list.len());
        for id in list {
            new_list.push(self.get_new_id(*id)?);
        }
        Ok(new_list)
    }

    fn clone_expression(&mut self, expr: &Expression) -> Result<ExprOwned, SbroadError> {
        let mut copied = expr.get_expr_owned();
        copied.try_map_children(|id| -> Result<(), SbroadError> {
            *id = self.get_new_id(*id)?;
            Ok(())
        })?;
        Ok(copied)
    }

    #[allow(clippy::too_many_lines)]
    fn clone_relational(&mut self, plan: &mut Plan, id: NodeId) -> Result<RelOwned, SbroadError> {
        let old_relational = plan.get_relation_node(id)?;
        let mut copied: RelOwned = old_relational.get_rel_owned();

        // All relational nodes have output and children lists, which must be copied.
        // We don't need to copy subqueries because we reuse them
        let children = old_relational.children().to_vec();
        let new_children = self.copy_list(&children)?;
        copied.set_children(new_children);
        let new_output_id = self.get_new_id(old_relational.output())?;
        *copied.mut_output() = new_output_id;

        // copy node specific fields, that reference other plan nodes

        // note: all struct fields are listed explicitly (instead of `..`), so that
        // when a new field is added to a struct, this match must
        // be updated, or compilation will fail.
        match &mut copied {
            RelOwned::Values(Values {
                output: _,
                children: _,
            })
            | RelOwned::SelectWithoutScan(SelectWithoutScan {
                subqueries: _,
                output: _,
            })
            | RelOwned::Projection(Projection {
                child: _,
                subqueries: _,
                windows: _,
                output: _,
                is_distinct: _,
                group_by: _,
                having: _,
            })
            | RelOwned::Insert(Insert {
                relation: _,
                columns: _,
                child: _,
                output: _,
                conflict_strategy: _,
            })
            | RelOwned::Update(Update {
                relation: _,
                child: _,
                update_columns_map: _,
                strategy: _,
                pk_positions: _,
                output: _,
            })
            | RelOwned::Delete(Delete {
                relation: _,
                child: _,
                output: _,
            })
            | RelOwned::ScanRelation(ScanRelation {
                alias: _,
                output: _,
                relation: _,
                ..
            })
            | RelOwned::ScanCte(ScanCte {
                alias: _,
                output: _,
                child: _,
            })
            | RelOwned::ScanSubQuery(ScanSubQuery {
                alias: _,
                child: _,
                output: _,
            })
            | RelOwned::Except(Except {
                left: _,
                right: _,
                output: _,
            })
            | RelOwned::Intersect(Intersect {
                left: _,
                right: _,
                output: _,
            })
            | RelOwned::Union(Union {
                left: _,
                right: _,
                output: _,
            })
            | RelOwned::UnionAll(UnionAll {
                left: _,
                right: _,
                output: _,
            })
            | RelOwned::Limit(Limit {
                limit: _,
                child: _,
                output: _,
            }) => {}
            RelOwned::Selection(Selection {
                child: _,
                subqueries: _,
                filter,
                output: _,
            })
            | RelOwned::Having(Having {
                child: _,
                subqueries: _,
                output: _,
                filter,
            })
            | RelOwned::Join(Join {
                left: _,
                right: _,
                subqueries: _,
                condition: filter,
                output: _,
                kind: _,
            }) => {
                *filter = self.get_new_id(*filter)?;
            }
            RelOwned::Motion(Motion {
                alias: _,
                child: _,
                policy: _,
                program,
                output: _,
            }) => {
                for op in &mut program.0 {
                    match op {
                        MotionOpcode::RearrangeForShardedUpdate {
                            update_id: _,
                            old_shard_columns_len: _,
                            new_shard_columns_positions: _,
                        } => {
                            // Update -> Motion -> ...
                            // Update is not copied yet.
                            self.nodes_with_backward_references.push(id);
                        }
                        MotionOpcode::AddMissingRowsForLeftJoin { motion_id } => {
                            // Projection -> THIS Motion -> Projection -> InnerJoin -> Motion (== motion_id)
                            // so it is safe to look up motion_id in map
                            *motion_id = self.get_new_id(*motion_id)?;
                        }
                        MotionOpcode::PrimaryKey(_)
                        | MotionOpcode::RemoveDuplicates
                        | MotionOpcode::ReshardIfNeeded
                        | MotionOpcode::SerializeAsEmptyTable(_) => {}
                    }
                }
            }
            RelOwned::GroupBy(GroupBy {
                child: _,
                subqueries: _,
                gr_exprs,
                output: _,
            }) => {
                *gr_exprs = self.copy_list(gr_exprs)?;
            }
            RelOwned::OrderBy(OrderBy {
                child: _,
                subqueries: _,
                order_by_elements,
                output: _,
            }) => {
                let mut new_order_by_elements = Vec::with_capacity(order_by_elements.len());
                for element in &mut *order_by_elements {
                    let new_entity = match element.entity {
                        OrderByEntity::Expression { expr_id } => {
                            let new_expr_id = self.get_new_id(expr_id)?;
                            OrderByEntity::Expression {
                                expr_id: new_expr_id,
                            }
                        }
                        OrderByEntity::Index { value } => OrderByEntity::Index { value },
                    };
                    new_order_by_elements.push(OrderByElement {
                        entity: new_entity,
                        order_type: element.order_type.clone(),
                    });
                }
                *order_by_elements = new_order_by_elements;
            }
            RelOwned::ValuesRow(ValuesRow {
                output: _,
                data,
                subqueries: _,
            }) => {
                *data = self.get_new_id(*data)?;
            }
        }

        Ok(copied)
    }

    // Some nodes contain references to nodes above in the tree
    // This function replaces those references to new nodes.
    fn replace_backward_refs(&self, plan: &mut Plan) -> Result<(), SbroadError> {
        for old_id in &self.nodes_with_backward_references {
            if let Node::Relational(Relational::Motion(Motion { program, .. })) =
                plan.get_node(*old_id)?
            {
                let op_cnt = program.0.len();
                for idx in 0..op_cnt {
                    let op = plan.get_motion_opcode(*old_id, idx)?;
                    if let MotionOpcode::RearrangeForShardedUpdate { update_id, .. } = op {
                        let new_motion_id = self.get_new_id(*old_id)?;
                        let new_update_id = self.get_new_id(*update_id)?;

                        if let MutRelational::Motion(Motion {
                            program: new_program,
                            ..
                        }) = plan.get_mut_relation_node(new_motion_id)?
                        {
                            if let Some(MotionOpcode::RearrangeForShardedUpdate {
                                update_id: new_node_update_id,
                                ..
                            }) = new_program.0.get_mut(idx)
                            {
                                *new_node_update_id = new_update_id;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn clone(
        &mut self,
        plan: &mut Plan,
        top_id: NodeId,
        capacity: usize,
    ) -> Result<NodeId, SbroadError> {
        // We don't copy the subquery's children because otherwise it would create a new subquery.
        // All references would then point to the same subquery.
        let dfs = PostOrder::new(|x| plan.subtree_iter_except_subquery(x, true), capacity);
        let nodes = dfs.traverse_into_vec(top_id);
        let mut invalid_refs = Vec::new();
        for LevelNode(_, id) in nodes {
            if self.old_new_map.contains_key(&id) {
                // IR is a DAG and our DFS traversal does not
                // track already visited nodes, so we may
                // visit the same node multiple times.
                // If we already cloned the node, no need to clone it
                // again.
                continue;
            }

            let node = plan.get_node(id)?;
            let new_node: NodeAligned = match node {
                Node::Relational(_) => self.clone_relational(plan, id)?.into(),
                Node::Expression(expr) => {
                    let mut node = self.clone_expression(&expr)?;
                    if let ExprOwned::Reference(Reference { target, .. }) = &mut node {
                        match target {
                            ReferenceTarget::Leaf => {}
                            ReferenceTarget::Single(node_id) => {
                                match self.old_new_map.get(node_id) {
                                    Some(node) => {
                                        *target = ReferenceTarget::Single(*node);
                                    }
                                    None => invalid_refs.push(*node_id),
                                }
                            }
                            ReferenceTarget::Union(left, right) => {
                                let new_left = self.old_new_map.get(left).unwrap_or_else(|| {
                                    invalid_refs.push(*left);
                                    left
                                });
                                let new_right = self.old_new_map.get(right).unwrap_or_else(|| {
                                    invalid_refs.push(*right);
                                    right
                                });
                                *target = ReferenceTarget::Union(*new_left, *new_right);
                            }
                            ReferenceTarget::Values(nodes) => {
                                let new_targets = nodes
                                    .iter()
                                    .map(|node_id| {
                                        *self.old_new_map.get(node_id).unwrap_or_else(|| {
                                            invalid_refs.push(*node_id);
                                            node_id
                                        })
                                    })
                                    .collect();
                                *target = ReferenceTarget::Values(new_targets);
                            }
                        }
                    }
                    node.into()
                }
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Node,
                        Some(format_smolstr!(
                            "clone: expected relational or expression on id: {id}"
                        )),
                    ))
                }
            };
            let new_id = plan.nodes.push(new_node);
            self.old_new_map.insert(id, new_id);
        }

        // Here we check for invalid references that should not appear later.
        // There are two scenarios we need to handle:
        // 1) We traverse in the wrong order - the referenced node will be copied before
        //    the reference is traversed, so the reference points to the old version
        // 2) A reference points to a node that will not be copied, meaning the reference
        //    remains valid and we don't need to change it
        for id in invalid_refs.iter() {
            if self.old_new_map.contains_key(id) {
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!(
                        "invalid subtree traversal with ref to: {id}"
                    )),
                ));
            }
        }

        self.replace_backward_refs(plan)?;

        let new_top_id = self
            .old_new_map
            .get(&top_id)
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!(
                        "invalid subtree traversal with top: {top_id}"
                    )),
                )
            })
            .copied()?;
        Ok(new_top_id)
    }

    /// Clones the given subtree to the plan arena and returns new `top_id`.
    /// Assumes that all parameters are bound and there are no parameters
    /// in the subtree.
    ///
    /// TODO: Should we return translation map as well? Seems like
    ///       it can simply life sometimes.
    pub fn clone_subtree(plan: &mut Plan, top_id: NodeId) -> Result<NodeId, SbroadError> {
        let subtree_capacity = top_id.offset as usize;
        let mut helper = Self::new(subtree_capacity);
        helper.clone(plan, top_id, subtree_capacity)
    }
}

#[cfg(feature = "mock")]
#[cfg(test)]
mod tests;
