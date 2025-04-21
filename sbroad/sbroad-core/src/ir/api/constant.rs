use std::collections::HashSet;

use crate::errors::{Entity, SbroadError};
use crate::ir::node::expression::Expression;
use crate::ir::node::{Constant, Node, Node64, NodeId, Parameter};
use crate::ir::tree::traversal::{LevelNode, PostOrderWithFilter, REL_CAPACITY};
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;
use crate::ir::DerivedType;
use crate::ir::{Nodes, Plan};
use smol_str::format_smolstr;

impl Expression<'_> {
    /// Gets value from const node
    ///
    /// # Errors
    /// - node isn't constant type
    pub fn as_const_value(&self) -> Result<Value, SbroadError> {
        if let Expression::Constant(Constant { value }) = self {
            return Ok(value.clone());
        }

        Err(SbroadError::Invalid(
            Entity::Node,
            Some("node is not Const type".into()),
        ))
    }

    /// Check whether the node is a constant expression.
    #[must_use]
    pub fn is_const(&self) -> bool {
        matches!(self, Expression::Constant(_))
    }
}

impl Nodes {
    /// Adds constant node.
    pub fn add_const(&mut self, value: Value) -> NodeId {
        self.push(Constant { value }.into())
    }
}

impl Plan {
    /// Gets reference to value from const node
    ///
    /// # Errors
    /// - node isn't constant type
    pub fn as_const_value_ref(&self, const_id: NodeId) -> Result<&Value, SbroadError> {
        if let Expression::Constant(Constant { value }) = self.get_expression_node(const_id)? {
            return Ok(value);
        }

        Err(SbroadError::Invalid(
            Entity::Node,
            Some("node is not Const type".into()),
        ))
    }

    /// Add constant value to the plan.
    pub fn add_const(&mut self, v: Value) -> NodeId {
        self.nodes.add_const(v)
    }

    /// # Panics
    #[must_use]
    /// # Panics
    pub fn get_const_list(&self, snapshot: Snapshot) -> Vec<NodeId> {
        let filter = |node_id: NodeId| -> bool {
            if let Ok(Node::Expression(Expression::Constant(..))) = self.get_node(node_id) {
                return true;
            }
            false
        };
        let mut tree = PostOrderWithFilter::with_capacity(
            |node| self.exec_plan_subtree_iter(node, snapshot),
            REL_CAPACITY,
            Box::new(filter),
        );
        let top_id = self.get_top().expect("Top node should be specified!");

        let mut set = HashSet::new();
        let mut vec = Vec::new();
        for LevelNode(_, node_id) in tree.iter(top_id) {
            if !set.contains(&node_id) {
                vec.push(node_id);
                set.insert(node_id);
            }
        }

        vec
    }

    /// Replace parameters with constants from the parameters map.
    ///
    /// # Errors
    /// - The parameters map is corrupted (parameters map points to invalid nodes).
    pub fn restore_constants(&mut self) -> Result<(), SbroadError> {
        for (id, constant) in self.constants.drain() {
            self.nodes.replace(id, Node64::Constant(constant))?;
        }
        Ok(())
    }

    /// Replace constant nodes with parameters (and hide them in the parameters map).
    ///
    /// # Errors
    /// - The plan is corrupted (collected constants point to invalid arena positions).
    pub fn stash_constants(&mut self, snapshot: Snapshot) -> Result<(), SbroadError> {
        // TODO: ensure that constants.len() does not exceed the limit on the number of parameters
        // in tarantool
        let index = |num: usize| -> Result<u16, _> {
            (num + 1).try_into().map_err(|_| {
                SbroadError::Other(format_smolstr!("too many parameters in local sql: {num}"))
            })
        };
        let constants = self.get_const_list(snapshot);
        for (num, const_id) in constants.iter().enumerate() {
            let param_type = self.calculate_expression_type(*const_id)?;
            let param_type = param_type
                .map(DerivedType::new)
                // NULL literal has an unknown type
                .unwrap_or(DerivedType::unknown());
            let const_node = self.nodes.replace(
                *const_id,
                Node64::Parameter(Parameter {
                    param_type,
                    index: index(num)?,
                }),
            )?;
            if let Node64::Constant(constant) = const_node {
                self.constants.insert(*const_id, constant);
            } else {
                panic!("{const_node:?} is not a constant");
            }
        }
        Ok(())
    }
}
