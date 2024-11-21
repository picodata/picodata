use std::collections::HashMap;

use ahash::AHashMap;
use pest::iterators::Pair;
use smol_str::format_smolstr;
use tarantool::decimal::Decimal;

use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::ast::Rule;
use crate::ir::node::expression::{ExprOwned, Expression};
use crate::ir::node::relational::{MutRelational, RelOwned, Relational};
use crate::ir::node::{
    Alias, ArithmeticExpr, BoolExpr, Case, Cast, Concat, Constant, Delete, Except,
    ExprInParentheses, GroupBy, Having, Insert, Intersect, Join, Like, Limit, Motion, Node,
    NodeAligned, NodeId, OrderBy, Projection, Reference, Row, ScanCte, ScanRelation, ScanSubQuery,
    SelectWithoutScan, Selection, StableFunction, Trim, UnaryExpr, Union, UnionAll, Update, Values,
    ValuesRow,
};
use crate::ir::operator::{OrderByElement, OrderByEntity};
use crate::ir::transformation::redistribution::MotionOpcode;
use crate::ir::tree::traversal::{LevelNode, PostOrder};
use crate::ir::value::double::Double;
use crate::ir::value::Value;
use crate::ir::Plan;

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
            Rule::Unsigned => Ok(pair_string
                .parse::<u64>()
                .map_err(|e| {
                    SbroadError::ParsingError(
                        Entity::Value,
                        format_smolstr!("u64 parsing error {e}"),
                    )
                })?
                .into()),
            Rule::SingleQuotedString => {
                let pair_str = pair.as_str();
                Ok(pair_str[1..pair_str.len() - 1].into())
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
    old_new: AHashMap<NodeId, NodeId>,
    nodes_with_backward_references: Vec<NodeId>,
}

impl SubtreeCloner {
    fn new(capacity: usize) -> Self {
        SubtreeCloner {
            old_new: AHashMap::with_capacity(capacity),
            nodes_with_backward_references: Vec::new(),
        }
    }

    fn get_new_id(&self, old_id: NodeId) -> Result<NodeId, SbroadError> {
        self.old_new
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

        // note: all struct fields are listed explicitly (instead of `..`), so that
        // when a new field is added to a struct, this match must
        // be updated, or compilation will fail.
        match &mut copied {
            ExprOwned::Constant(Constant { value: _ })
            | ExprOwned::Reference(Reference {
                parent: _,
                targets: _,
                position: _,
                col_type: _,
                asterisk_source: _,
            })
            | ExprOwned::CountAsterisk { .. } => {}
            ExprOwned::Alias(Alias {
                ref mut child,
                name: _,
            })
            | ExprOwned::ExprInParentheses(ExprInParentheses { ref mut child })
            | ExprOwned::Cast(Cast {
                ref mut child,
                to: _,
            })
            | ExprOwned::Unary(UnaryExpr {
                ref mut child,
                op: _,
            }) => {
                *child = self.get_new_id(*child)?;
            }
            ExprOwned::Case(Case {
                ref mut search_expr,
                ref mut when_blocks,
                ref mut else_expr,
            }) => {
                if let Some(search_expr) = search_expr {
                    *search_expr = self.get_new_id(*search_expr)?;
                }
                for (cond_expr, res_expr) in when_blocks {
                    *cond_expr = self.get_new_id(*cond_expr)?;
                    *res_expr = self.get_new_id(*res_expr)?;
                }
                if let Some(else_expr) = else_expr {
                    *else_expr = self.get_new_id(*else_expr)?;
                }
            }
            ExprOwned::Bool(BoolExpr {
                ref mut left,
                ref mut right,
                op: _,
            })
            | ExprOwned::Arithmetic(ArithmeticExpr {
                ref mut left,
                ref mut right,
                op: _,
            })
            | ExprOwned::Concat(Concat {
                ref mut left,
                ref mut right,
            }) => {
                *left = self.get_new_id(*left)?;
                *right = self.get_new_id(*right)?;
            }
            ExprOwned::Like(Like {
                ref mut left,
                ref mut right,
                ref mut escape,
            }) => {
                *left = self.get_new_id(*left)?;
                *right = self.get_new_id(*right)?;
                *escape = self.get_new_id(*escape)?;
            }
            ExprOwned::Trim(Trim {
                ref mut pattern,
                ref mut target,
                ..
            }) => {
                if let Some(pattern) = pattern {
                    *pattern = self.get_new_id(*pattern)?;
                }
                *target = self.get_new_id(*target)?;
            }
            ExprOwned::Row(Row {
                list: ref mut children,
                distribution: _,
            })
            | ExprOwned::StableFunction(StableFunction {
                ref mut children, ..
            }) => {
                *children = self.copy_list(&*children)?;
            }
        }

        Ok(copied)
    }

    #[allow(clippy::too_many_lines)]
    fn clone_relational(
        &mut self,
        old_relational: &Relational,
        id: NodeId,
    ) -> Result<RelOwned, SbroadError> {
        let mut copied: RelOwned = old_relational.get_rel_owned();

        // all relational nodes have output and children list,
        // which must be copied.
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
                children: _,
                output: _,
            })
            | RelOwned::Projection(Projection {
                children: _,
                output: _,
                is_distinct: _,
            })
            | RelOwned::Insert(Insert {
                relation: _,
                columns: _,
                children: _,
                output: _,
                conflict_strategy: _,
            })
            | RelOwned::Update(Update {
                relation: _,
                children: _,
                update_columns_map: _,
                strategy: _,
                pk_positions: _,
                output: _,
            })
            | RelOwned::Delete(Delete {
                relation: _,
                children: _,
                output: _,
            })
            | RelOwned::ScanRelation(ScanRelation {
                alias: _,
                output: _,
                relation: _,
            })
            | RelOwned::ScanCte(ScanCte {
                alias: _,
                output: _,
                child: _,
            })
            | RelOwned::ScanSubQuery(ScanSubQuery {
                alias: _,
                children: _,
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
            RelOwned::Having(Having {
                children: _,
                output: _,
                filter,
            })
            | RelOwned::Selection(Selection {
                children: _,
                filter,
                output: _,
            })
            | RelOwned::Join(Join {
                children: _,
                condition: filter,
                output: _,
                kind: _,
            }) => {
                *filter = self.get_new_id(*filter)?;
            }
            RelOwned::Motion(Motion {
                alias: _,
                children: _,
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
                children: _,
                gr_cols,
                output: _,
                is_final: _,
            }) => {
                *gr_cols = self.copy_list(gr_cols)?;
            }
            RelOwned::OrderBy(OrderBy {
                children: _,
                order_by_elements,
                output: _,
            }) => {
                let mut new_order_by_elements = Vec::with_capacity(order_by_elements.len());
                for element in &mut *order_by_elements {
                    let new_entity = match element.entity {
                        OrderByEntity::Expression { expr_id } => OrderByEntity::Expression {
                            expr_id: self.get_new_id(expr_id)?,
                        },
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
                children: _,
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
        let mut dfs = PostOrder::with_capacity(|x| plan.subtree_iter(x, true), capacity);
        dfs.populate_nodes(top_id);
        let nodes = dfs.take_nodes();
        drop(dfs);
        for LevelNode(_, id) in nodes {
            if self.old_new.contains_key(&id) {
                // IR is a DAG and our DFS traversal does not
                // track already visited nodes, so we may
                // visit the same node multiple times.
                // If we already cloned the node, no need to clone it
                // again.
                continue;
            }

            let node = plan.get_node(id)?;
            let new_node: NodeAligned = match node {
                Node::Relational(rel) => self.clone_relational(&rel, id)?.into(),
                Node::Expression(expr) => self.clone_expression(&expr)?.into(),
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
            self.old_new.insert(id, new_id);
        }

        self.replace_backward_refs(plan)?;

        let new_top_id = self
            .old_new
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
    /// # Errors
    /// - invalid plan subtree, e.g some node is met twice in the plan
    /// - parameters/ddl/acl nodes are found in subtree
    pub fn clone_subtree(plan: &mut Plan, top_id: NodeId) -> Result<NodeId, SbroadError> {
        let subtree_capacity = top_id.offset as usize;
        let mut helper = Self::new(subtree_capacity);
        helper.clone(plan, top_id, subtree_capacity)
    }
}

#[cfg(test)]
mod tests;
