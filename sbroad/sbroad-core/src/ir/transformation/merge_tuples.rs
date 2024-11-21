//! Merge tuples in a disjunction of equality expressions
//! into a single tuple.
//!
//! Example:
//! ```sql
//! select * from t where (a = 1) and (b = 2) and (c = 3)
//! ```
//! is converted to:
//! ```sql
//! select * from t where (a, b, c) = (1, 2, 3)
//! ```

use crate::errors::{Entity, SbroadError};
use crate::ir::helpers::RepeatableState;
use crate::ir::node::expression::{Expression, MutExpression};
use crate::ir::node::relational::Relational;
use crate::ir::node::{Alias, ArithmeticExpr, BoolExpr, NodeId, Reference, Row};
use crate::ir::operator::Bool;
use crate::ir::transformation::OldNewTopIdPair;
use crate::ir::tree::traversal::BreadthFirst;
use crate::ir::tree::traversal::EXPR_CAPACITY;
use crate::ir::Plan;
use smol_str::{format_smolstr, ToSmolStr};
use std::collections::{hash_map::Entry, HashMap, HashSet};

fn call_expr_tree_merge_tuples(
    plan: &mut Plan,
    top_id: NodeId,
) -> Result<OldNewTopIdPair, SbroadError> {
    plan.expr_tree_modify_and_chains(top_id, &call_build_and_chains, &call_as_plan)
}

fn call_build_and_chains(
    plan: &mut Plan,
    nodes: &[NodeId],
) -> Result<HashMap<NodeId, Chain, RepeatableState>, SbroadError> {
    plan.populate_and_chains(nodes)
}

fn call_as_plan(chain: &Chain, plan: &mut Plan) -> Result<NodeId, SbroadError> {
    chain.as_plan(plan)
}

/// "AND" chain grouped by the operator type.
#[derive(Debug)]
pub struct Chain {
    // Left and right sides of the equality (and non-equality) expressions
    // grouped by the operator.
    grouped: HashMap<Bool, (Vec<NodeId>, Vec<NodeId>)>,
    // Other boolean expressions in the AND chain (true, false, null, Lt, GtEq, etc).
    other: Vec<NodeId>,
}

impl Chain {
    /// Create a new chain.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            grouped: HashMap::with_capacity(capacity),
            other: Vec::new(),
        }
    }

    /// Add a new expression to the chain.
    ///
    /// # Errors
    /// - Failed if the node is not an expression.
    /// - Failed if expression is not an "AND" or "OR".
    /// - There is something wrong with our sub-queries.
    pub fn insert(&mut self, plan: &mut Plan, expr_id: NodeId) -> Result<(), SbroadError> {
        let bool_expr = plan.get_expression_node(expr_id)?;
        if let Expression::Bool(BoolExpr { left, op, right }) = bool_expr {
            if let Bool::And | Bool::Or = op {
                // We don't expect nested AND/OR expressions in DNF.
                return Err(SbroadError::Unsupported(
                    Entity::Operator,
                    Some(format_smolstr!(
                        "AND/OR expressions are not supported: {bool_expr:?}"
                    )),
                ));
            }

            // Merge expression into tuples only for equality operators.
            if let Bool::Eq = op {
                // Try to put expressions with references to the left side.
                let (left_id, right_id, group_op) =
                    match (plan.is_ref(*left)?, plan.is_ref(*right)?) {
                        (false, true) => (*right, *left, op.clone()),
                        _ => (*left, *right, op.clone()),
                    };

                if let Ok(Expression::Arithmetic(_)) = plan.get_expression_node(left_id) {
                    self.other.push(expr_id);
                    return Ok(());
                }

                if let Ok(Expression::Arithmetic(_)) = plan.get_expression_node(right_id) {
                    self.other.push(expr_id);
                    return Ok(());
                }

                // If boolean expression contains a reference to an additional
                //  sub-query, it should be added to the "other" list.
                let left_sq = plan.get_sub_query_from_row_node(left_id)?;
                let right_sq = plan.get_sub_query_from_row_node(right_id)?;
                for sq_id in [left_sq, right_sq].iter().flatten() {
                    if plan.is_additional_child(*sq_id)? {
                        self.other.push(expr_id);
                        return Ok(());
                    }
                }

                match self.grouped.entry(group_op) {
                    Entry::Occupied(mut entry) => {
                        let (left, right) = entry.get_mut();
                        let new_left_id = left_id;
                        let new_right_id = right_id;
                        plan.get_columns_or_self(new_left_id)?
                            .iter()
                            .for_each(|id| {
                                left.push(*id);
                            });
                        plan.get_columns_or_self(new_right_id)?
                            .iter()
                            .for_each(|id| {
                                right.push(*id);
                            });
                    }
                    Entry::Vacant(entry) => {
                        let new_left_id = left_id;
                        let new_right_id = right_id;
                        entry.insert((
                            plan.get_columns_or_self(new_left_id)?,
                            plan.get_columns_or_self(new_right_id)?,
                        ));
                    }
                }
                return Ok(());
            }
        }

        self.other.push(expr_id);
        Ok(())
    }

    fn as_plan(&self, plan: &mut Plan) -> Result<NodeId, SbroadError> {
        let other_top_id = match self.other.split_first() {
            Some((first, other)) => {
                let mut top_id = *first;
                for id in other {
                    top_id = plan.add_cond(*id, Bool::And, top_id)?;
                }
                Some(top_id)
            }
            None => None,
        };

        // Chain is grouped by the operators in the hash map.
        // To make serialization non-flaky, we extract operators
        // in a deterministic order.
        let mut grouped_top_id: Option<NodeId> = None;
        let ordered_ops = &[Bool::Eq, Bool::NotEq];
        for op in ordered_ops {
            let Some((left, right)) = self.grouped.get(op) else {
                continue;
            };
            let cond_id = if *op == Bool::Eq {
                if let Some(grouped) = plan.split_join_references(left, right) {
                    grouped.add_rows_to_plan(plan)?
                } else {
                    add_rows_and_cond(plan, left.clone(), right.clone(), op)?
                }
            } else {
                add_rows_and_cond(plan, left.clone(), right.clone(), op)?
            };
            match grouped_top_id {
                None => {
                    grouped_top_id = Some(cond_id);
                }
                Some(top_id) => {
                    grouped_top_id = Some(plan.add_cond(top_id, Bool::And, cond_id)?);
                }
            }
        }
        match (grouped_top_id, other_top_id) {
            (Some(grouped_top_id), Some(other_top_id)) => {
                Ok(plan.add_cond(grouped_top_id, Bool::And, other_top_id)?)
            }
            (Some(grouped_top_id), None) => Ok(grouped_top_id),
            (None, Some(other_top_id)) => Ok(other_top_id),
            (None, None) => Err(SbroadError::UnexpectedNumberOfValues(
                "no expressions to merge, expected one or twoe".to_smolstr(),
            )),
        }
    }

    fn is_empty(&self) -> bool {
        self.grouped.is_empty() && self.other.is_empty()
    }

    /// Return boolean expression nodes grouped by the operator.
    #[must_use]
    pub fn get_grouped(&self) -> &HashMap<Bool, (Vec<NodeId>, Vec<NodeId>)> {
        &self.grouped
    }

    /// Return "other" boolean expression nodes.
    #[must_use]
    pub fn get_other(&self) -> &Vec<NodeId> {
        &self.other
    }
}

fn add_rows_and_cond(
    plan: &mut Plan,
    left: impl Into<Vec<NodeId>>,
    right: impl Into<Vec<NodeId>>,
    op: &Bool,
) -> Result<NodeId, SbroadError> {
    let left_row_id = plan.nodes.add_row(left.into(), None);
    let right_row_id = plan.nodes.add_row(right.into(), None);
    plan.add_cond(left_row_id, op.clone(), right_row_id)
}

struct GroupedRows {
    // Left row that contains references to first join child
    join_refs_left: Vec<NodeId>,
    // Right row that contains references to second join child
    join_refs_right: Vec<NodeId>,
    other_left: Vec<NodeId>,
    other_right: Vec<NodeId>,
}

impl GroupedRows {
    fn add_rows_to_plan(self, plan: &mut Plan) -> Result<NodeId, SbroadError> {
        fn add_eq(
            left: Vec<NodeId>,
            right: Vec<NodeId>,
            plan: &mut Plan,
        ) -> Result<Option<NodeId>, SbroadError> {
            debug_assert!(left.len() == right.len());
            let res = if !left.is_empty() {
                add_rows_and_cond(plan, left, right, &Bool::Eq)?.into()
            } else {
                None
            };
            Ok(res)
        }

        let eq1: Option<NodeId> = add_eq(self.join_refs_left, self.join_refs_right, plan)?;
        let eq2: Option<NodeId> = add_eq(self.other_left, self.other_right, plan)?;

        let res = match (eq1, eq2) {
            (Some(id1), Some(id2)) => plan.add_bool(id1, Bool::And, id2)?,
            (None, Some(id)) | (Some(id), None) => id,
            (None, None) => panic!("at least some row must be non-empty"),
        };
        Ok(res)
    }
}

impl Plan {
    fn split_join_references(&self, left: &[NodeId], right: &[NodeId]) -> Option<GroupedRows> {
        // First check that we are in join
        let contains_join_refs = |row: &[NodeId]| -> bool {
            row.iter().any(|id| {
                self.get_expression_node(*id).is_ok_and(|expr| {
                    if let Expression::Reference(Reference {
                        parent: Some(p), ..
                    }) = expr
                    {
                        if self
                            .get_relation_node(*p)
                            .is_ok_and(|rel| matches!(rel, Relational::Join(_)))
                        {
                            return true;
                        }
                    }
                    false
                })
            })
        };
        if !contains_join_refs(left) && !contains_join_refs(right) {
            return None;
        }

        // Split (left) = (right) into
        // (a1) = (b1) and (a2) = (b2)
        // a1 - contains references from one child
        // a2 - contains references from some other child
        // a2, b2 - contain all other expressions
        // This is done for join conflict resolution:
        // we calculate the distribution of rows in the `on`
        // condition to find equality on sharding keys.
        // In case of `(t1.a, t2.d) = (t2.c, t1.b)` where
        // sk(t1) = (a, b), sk(t2) = (c, d) we will fail
        // to find matching keys and insert Motion(Full).
        // So we need to group references by their table (child).

        // a1 - will store references of the first child
        // b1 - of the second child
        let mut join_refs_left = Vec::new();
        let mut join_refs_right = Vec::new();
        let mut other_left = Vec::new();
        let mut other_right = Vec::new();

        let first_child_target = Some(vec![0]);
        let second_child_target = Some(vec![1]);

        left.iter()
            .zip(right.iter())
            .map(|(left_id, right_id)| {
                // Map each pair of equal expressions into
                // (left, right, flag), where flag=true indicates
                // that this pair is of form Reference1 = Reference2
                // where Reference1 refers to first join child
                // and Reference2 refers to second join child

                let other_pair = (left_id, right_id, false);
                let Some(Reference {
                    targets: target_l,
                    parent: parent_l,
                    ..
                }) = self.get_reference(*left_id)
                else {
                    return other_pair;
                };
                let Some(Reference {
                    targets: target_r,
                    parent: parent_r,
                    ..
                }) = self.get_reference(*right_id)
                else {
                    return other_pair;
                };
                debug_assert!(parent_r == parent_l);

                if target_l == &first_child_target && target_r == &second_child_target {
                    return (left_id, right_id, true);
                } else if target_l == &second_child_target && target_r == &first_child_target {
                    return (right_id, left_id, true);
                }
                other_pair
            })
            .for_each(|(left_id, right_id, is_join_refs)| {
                if is_join_refs {
                    join_refs_left.push(*left_id);
                    join_refs_right.push(*right_id);
                } else {
                    other_left.push(*left_id);
                    other_right.push(*right_id);
                }
            });

        Some(GroupedRows {
            join_refs_left,
            join_refs_right,
            other_left,
            other_right,
        })
    }

    fn get_columns_or_self(&self, expr_id: NodeId) -> Result<Vec<NodeId>, SbroadError> {
        let expr = self.get_expression_node(expr_id)?;
        match expr {
            Expression::Row(Row { list, .. }) => Ok(list.clone()),
            _ => Ok(vec![expr_id]),
        }
    }

    /// Returns all the DNF "And" chains with their tops from list of nodes.
    ///
    /// # Errors
    /// - Failed to get an expression node where expected.
    /// - Failed to insert the node to the "And" chain.
    pub fn populate_and_chains(
        &mut self,
        nodes: &[NodeId],
    ) -> Result<HashMap<NodeId, Chain, RepeatableState>, SbroadError> {
        let mut visited: HashSet<NodeId> = HashSet::with_capacity(self.nodes.len());
        let mut chains: HashMap<NodeId, Chain, RepeatableState> =
            HashMap::with_capacity_and_hasher(nodes.len(), RepeatableState);

        for id in nodes {
            if visited.contains(id) {
                continue;
            }
            visited.insert(*id);

            let mut tree_and = BreadthFirst::with_capacity(
                |node| self.nodes.and_iter(node),
                EXPR_CAPACITY,
                EXPR_CAPACITY,
            );
            let nodes_and: Vec<NodeId> =
                tree_and.iter(*id).map(|level_node| level_node.1).collect();
            let mut nodes_for_chain: Vec<NodeId> = Vec::with_capacity(nodes_and.len());
            for and_id in nodes_and {
                let expr = self.get_expression_node(and_id)?;
                if let Expression::Bool(BoolExpr {
                    left,
                    op: Bool::And,
                    right,
                    ..
                }) = expr
                {
                    let children = vec![*left, *right];
                    for child_id in children {
                        visited.insert(child_id);
                        let child_expr = self.get_expression_node(child_id)?;
                        if let Expression::Bool(BoolExpr {
                            op: Bool::And | Bool::Or,
                            ..
                        }) = child_expr
                        {
                            continue;
                        }
                        nodes_for_chain.push(child_id);
                    }
                }
            }
            let mut chain = Chain::with_capacity(nodes_for_chain.len());
            for node_id in nodes_for_chain {
                chain.insert(self, node_id)?;
            }

            if !chain.is_empty() {
                chains.insert(*id, chain);
            }
        }
        Ok(chains)
    }

    /// Build all the "AND" chains in subtree with `f_build_chains` function,
    /// transform back every chain to a plan expression with `f_to_plan` function
    /// and return a new expression subtree.
    ///
    /// # Errors
    /// - Failed to build an expression subtree for some chain.
    /// - The plan is invalid (some bugs).
    #[allow(clippy::type_complexity, clippy::too_many_lines)]
    pub fn expr_tree_modify_and_chains(
        &mut self,
        expr_id: NodeId,
        f_build_chains: &dyn Fn(
            &mut Plan,
            &[NodeId],
        )
            -> Result<HashMap<NodeId, Chain, RepeatableState>, SbroadError>,
        f_to_plan: &dyn Fn(&Chain, &mut Plan) -> Result<NodeId, SbroadError>,
    ) -> Result<OldNewTopIdPair, SbroadError> {
        let mut tree = BreadthFirst::with_capacity(
            |node| self.nodes.expr_iter(node, false),
            EXPR_CAPACITY,
            EXPR_CAPACITY,
        );
        let nodes: Vec<NodeId> = tree.iter(expr_id).map(|level_node| level_node.1).collect();
        let chains = f_build_chains(self, &nodes)?;

        // Replace nodes' children with the merged tuples.
        for id in nodes {
            let expr = self.get_expression_node(id)?;
            match expr {
                Expression::Alias(Alias { child, .. }) => {
                    let chain = chains.get(child);
                    if let Some(chain) = chain {
                        let new_child_id = f_to_plan(chain, self)?;
                        let expr_mut = self.get_mut_expression_node(id)?;
                        if let MutExpression::Alias(Alias {
                            child: ref mut child_id,
                            ..
                        }) = expr_mut
                        {
                            *child_id = new_child_id;
                        } else {
                            return Err(SbroadError::Invalid(
                                Entity::Expression,
                                Some(format_smolstr!("expected alias expression: {expr_mut:?}")),
                            ));
                        }
                    }
                }
                Expression::Bool(BoolExpr { left, right, .. }) => {
                    let children = [*left, *right];
                    for (pos, child) in children.iter().enumerate() {
                        let chain = chains.get(child);
                        if let Some(chain) = chain {
                            let new_child_id = f_to_plan(chain, self)?;
                            let expr_mut = self.get_mut_expression_node(id)?;
                            if let MutExpression::Bool(BoolExpr {
                                left: ref mut left_id,
                                right: ref mut right_id,
                                ..
                            }) = expr_mut
                            {
                                if pos == 0 {
                                    *left_id = new_child_id;
                                } else {
                                    *right_id = new_child_id;
                                }
                            } else {
                                return Err(SbroadError::Invalid(
                                    Entity::Expression,
                                    Some(format_smolstr!(
                                        "expected boolean expression: {expr_mut:?}"
                                    )),
                                ));
                            }
                        }
                    }
                }
                Expression::Arithmetic(ArithmeticExpr { left, right, .. }) => {
                    let children = [*left, *right];
                    for (pos, child) in children.iter().enumerate() {
                        let chain = chains.get(child);
                        if let Some(chain) = chain {
                            let new_child_id = f_to_plan(chain, self)?;
                            let expr_mut = self.get_mut_expression_node(id)?;
                            if let MutExpression::Arithmetic(ArithmeticExpr {
                                left: ref mut left_id,
                                right: ref mut right_id,
                                ..
                            }) = expr_mut
                            {
                                if pos == 0 {
                                    *left_id = new_child_id;
                                } else {
                                    *right_id = new_child_id;
                                }
                            } else {
                                return Err(SbroadError::Invalid(
                                    Entity::Expression,
                                    Some(format_smolstr!(
                                        "expected Arithmetic expression: {expr_mut:?}"
                                    )),
                                ));
                            }
                        }
                    }
                }
                Expression::Row(Row { list, .. }) => {
                    let children = list.clone();
                    for (pos, child) in children.iter().enumerate() {
                        let chain = chains.get(child);
                        if let Some(chain) = chain {
                            let new_child_id = f_to_plan(chain, self)?;
                            let expr_mut = self.get_mut_expression_node(id)?;
                            if let MutExpression::Row(Row { ref mut list, .. }) = expr_mut {
                                if let Some(child_id) = list.get_mut(pos) {
                                    *child_id = new_child_id;
                                } else {
                                    return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                                        "expected a column at position {pos} in the row {expr_mut:?}"
                                    )));
                                }
                            } else {
                                return Err(SbroadError::Invalid(
                                    Entity::Expression,
                                    Some(format_smolstr!("expected row expression: {expr_mut:?}")),
                                ));
                            }
                        }
                    }
                }
                _ => continue,
            }
        }

        // Try to replace the subtree top node (if it is also AND).
        if let Some(top_chain) = chains.get(&expr_id) {
            let new_expr_id = f_to_plan(top_chain, self)?;
            return Ok((expr_id, new_expr_id));
        }

        Ok((expr_id, expr_id))
    }

    /// Group boolean operators in the AND-ed chain by operator type and merge
    /// them into a single boolean operator.
    ///
    /// # Errors
    /// - If the plan tree is invalid (doesn't contain correct nodes where we expect it to).
    pub fn merge_tuples(&mut self) -> Result<(), SbroadError> {
        self.transform_expr_trees(&call_expr_tree_merge_tuples)
    }
}

#[cfg(test)]
mod tests;
