//! Disjunctive normal form (DNF) transformation.
//!
//! DNF transformation is a subtask of the derivation of the distribution keys in filter trees.
//! The idea is to generate wide "AND" expression chains that can be later converted to the
//! tuples. So we increase the chance that the current tuple contains a distribution key that
//! can be later used to optimize motion policy and filter the amount of segments to evaluate on
//! the local query.
//!
//! The idea behind generating DNF is to use an IR expression tree, build "AND" chains from its top
//! to bottom and then unite the chains into one "OR" expression. An important aspect: we clone the
//! chain in every "OR" node, so "OR" is treated as a "fork" node.
//!
//! For example:
//! ```text
//! ((a = 1) and (b = 2) or (a = 3)) and (c = 4)
//! ```
//! can be converted to a tree:
//! ```text
//!                         ┌─────┐
//!                         │ AND │
//!                         └┬───┬┘
//!                          │   │
//!                          │   │
//!               ┌──────┐   │   │   ┌──────┐
//!               │  OR  │◄──┘   └───┤c = 4 │
//!               └──┬─┬─┘           └──────┘
//!                  │ │
//!                  │ │
//!         ┌─────┐  │ │  ┌─────┐
//!         │ AND │◄─┘ └─►│a = 3│
//!         └─┬──┬┘       └─────┘
//!           │  │
//!           │  │
//! ┌─────┐   │  │   ┌─────┐
//! │a = 1│◄──┘  └──►│b = 2│
//! └─────┘          └─────┘
//! ```
//!
//! Here is the list of all the chains ("paths") from the top to the bottom:
//! ```text
//! 1. (c = 4) and (a = 1) and (b = 2)
//! 2. (c = 4) and (a = 3)
//! ```
//!
//! To build the DNF we unite the chains with the "OR" node:
//! ```text
//! ((c = 4) and (a = 1) and (b = 2)) or ((c = 4) and (a = 3))
//! ```
//!
//! The corresponding tree:
//! ```text
//!
//!                         ┌────┐
//!                         │ OR │
//!                         └┬──┬┘
//!                          │  │
//!                          │  │
//!                ┌─────┐   │  │           ┌─────┐
//!                │ AND │◄──┘  └──────────►│ AND │
//!                └─┬─┬─┘                  └┬───┬┘
//!                  │ │                     │   │
//!                  │ │                     │   │
//!         ┌─────┐  │ │  ┌─────┐   ┌─────┐  │   │  ┌─────┐
//!         │ AND │◄─┘ └─►│c = 4│   │a = 3│◄─┘   └─►│c = 4│
//!         └─┬──┬┘       └─────┘   └─────┘         └─────┘
//!           │  │
//!           │  │
//! ┌─────┐   │  │   ┌─────┐
//! │a = 1│◄──┘  └──►│b = 2│
//! └─────┘          └─────┘
//! ```

use crate::errors::{Entity, SbroadError};
use crate::ir::node::expression::Expression;
use crate::ir::node::{BoolExpr, ExprInParentheses, Node32, NodeId};
use crate::ir::operator::Bool;
use crate::ir::transformation::OldNewTopIdPair;
use crate::ir::Plan;
use std::collections::VecDeque;

/// A chain of the trivalents (boolean or NULL expressions) concatenated by AND.
#[derive(Clone, Debug)]
pub struct Chain {
    nodes: VecDeque<NodeId>,
}

/// Helper function to identify whether we are dealing with AND/OR operator that
/// may be covered with parentheses.
fn optionally_covered_and_or(expr_id: NodeId, plan: &Plan) -> Result<Option<NodeId>, SbroadError> {
    let expr = plan.get_expression_node(expr_id)?;
    let and_or = match expr {
        Expression::Bool(BoolExpr { op, .. }) => {
            if matches!(op, Bool::And) || matches!(op, Bool::Or) {
                Some(expr_id)
            } else {
                None
            }
        }
        Expression::ExprInParentheses(ExprInParentheses { child }) => {
            optionally_covered_and_or(*child, plan)?
        }
        _ => None,
    };
    Ok(and_or)
}

impl Chain {
    fn with_capacity(capacity: usize) -> Self {
        Chain {
            nodes: VecDeque::with_capacity(capacity),
        }
    }

    fn reserve(&mut self, additional: usize) {
        self.nodes.reserve(additional);
    }

    fn length(&self) -> usize {
        self.nodes.len()
    }

    /// Append a new node to the chain. Keep AND and OR nodes in the back,
    /// while other nodes in the front of the chain queue.
    fn push(&mut self, expr_id: NodeId, plan: &Plan) -> Result<(), SbroadError> {
        let and_or = optionally_covered_and_or(expr_id, plan)?;
        if let Some(and_or_id) = and_or {
            self.nodes.push_back(and_or_id);
        } else {
            self.nodes.push_front(expr_id);
        }
        Ok(())
    }

    /// Pop AND and OR nodes (we append them to the back).
    fn pop_back(&mut self, plan: &Plan) -> Result<Option<NodeId>, SbroadError> {
        if let Some(expr_id) = self.nodes.back() {
            let expr = plan.get_expression_node(*expr_id)?;
            if let Expression::Bool(BoolExpr {
                op: Bool::And | Bool::Or,
                ..
            }) = expr
            {
                return Ok(self.nodes.pop_back());
            }
        }
        Ok(None)
    }

    /// Pop trivalent nodes other than AND and OR (we keep then in the front).
    fn pop_front(&mut self) -> Option<NodeId> {
        self.nodes.pop_front()
    }

    /// Convert a chain to a new expression tree (reuse trivalent expressions).
    fn as_plan(&mut self, plan: &mut Plan) -> Result<NodeId, SbroadError> {
        let mut top_id: Option<NodeId> = None;
        while let Some(expr_id) = self.pop_front() {
            match top_id {
                None => {
                    top_id = Some(expr_id);
                }
                Some(left_id) => {
                    let right_id = expr_id;
                    top_id = Some(plan.concat_and(left_id, right_id)?);
                }
            }
        }
        let new_top_id = top_id
            .ok_or_else(|| SbroadError::Invalid(Entity::Chain, Some("Empty chain".into())))?;
        Ok(new_top_id)
    }

    /// Return a mutable reference to the chain nodes.
    pub fn get_mut_nodes(&mut self) -> &mut VecDeque<NodeId> {
        &mut self.nodes
    }
}

fn call_expr_tree_to_dnf(plan: &mut Plan, top_id: NodeId) -> Result<OldNewTopIdPair, SbroadError> {
    plan.expr_tree_to_dnf(top_id)
}

impl Plan {
    /// Get the DNF "AND" chains from the expression tree.
    ///
    /// # Errors
    /// - If the expression tree is not a trivalent expression.
    /// - Failed to append node to the AND chain.
    pub fn get_dnf_chains(&self, top_id: NodeId) -> Result<VecDeque<Chain>, SbroadError> {
        let capacity: usize = self.nodes.arena32.iter().fold(0_usize, |acc, node| {
            acc + match node {
                Node32::Bool(BoolExpr {
                    op: Bool::And | Bool::Or,
                    ..
                }) => 1,
                _ => 0,
            }
        });
        let mut result: VecDeque<Chain> = VecDeque::with_capacity(capacity);
        let mut stack: Vec<Chain> = Vec::with_capacity(capacity);

        let mut top_chain = Chain::with_capacity(capacity);
        top_chain.push(top_id, self)?;
        stack.push(top_chain);

        while let Some(mut chain) = stack.pop() {
            let Some(expr_id) = chain.pop_back(self)? else {
                result.push_back(chain);
                continue;
            };
            let expr = self.get_expression_node(expr_id)?;
            if let Expression::Bool(BoolExpr {
                op, left, right, ..
            }) = expr
            {
                match *op {
                    Bool::And => {
                        chain.push(*right, self)?;
                        chain.push(*left, self)?;
                    }
                    Bool::Or => {
                        let mut new_chain = chain.clone();
                        new_chain.reserve(capacity - new_chain.length());
                        new_chain.push(*right, self)?;
                        stack.push(new_chain);
                        chain.push(*left, self)?;
                    }
                    _ => {
                        return Err(SbroadError::Invalid(
                            Entity::Chain,
                            Some("Chain returned unexpected boolean operator".into()),
                        ))
                    }
                }
            }
            stack.push(chain);
        }

        Ok(result)
    }

    /// Unwrap expression from ExprInParentheses.
    ///
    /// # Errors
    /// - Failed to find node with given id in plan.
    pub fn unwrap_expr(&self, id: NodeId) -> Result<Expression<'_>, SbroadError> {
        let mut bool_id = id;

        loop {
            let node = self.get_expression_node(bool_id)?;
            if let Expression::ExprInParentheses(ExprInParentheses { child }) = node {
                bool_id = *child;
                continue;
            }

            break Ok(node);
        }
    }

    /// Convert an expression tree of trivalent nodes to a disjunctive normal form (DNF).
    ///
    /// # Errors
    /// - Failed to retrieve DNF chains.
    /// - Failed to convert the AND chain to a new expression tree.
    /// - Failed to concatenate the AND expression trees to the OR tree.
    pub fn expr_tree_to_dnf(&mut self, top_id: NodeId) -> Result<OldNewTopIdPair, SbroadError> {
        let mut result = self.get_dnf_chains(top_id)?;

        let mut new_top_id: Option<NodeId> = None;
        while let Some(mut chain) = result.pop_front() {
            let ir_chain_top = chain.as_plan(self)?;
            new_top_id = match new_top_id {
                Some(top_id) => Some(self.concat_or(top_id, ir_chain_top)?),
                None => Some(ir_chain_top),
            }
        }

        let new_top_id = new_top_id.ok_or_else(|| {
            SbroadError::Invalid(Entity::Chain, Some("Chain returned no expressions".into()))
        })?;
        Ok((top_id, new_top_id))
    }

    /// Convert an expression tree of trivalent nodes to a conjunctive
    /// normal form (CNF) for the whole plan.
    ///
    /// # Errors
    /// - If the plan does not contain the top.
    /// - If the plan doesn't contain relational operators where expected.
    /// - If failed to convert an expression tree to a CNF.
    pub fn set_dnf(&mut self) -> Result<(), SbroadError> {
        self.transform_expr_trees(&call_expr_tree_to_dnf)
    }
}

#[cfg(test)]
pub mod tests;
