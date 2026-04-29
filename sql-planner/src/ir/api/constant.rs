use crate::errors::{Entity, SbroadError};
use crate::ir::node::expression::Expression;
use crate::ir::node::{Constant, Node, NodeId};
use crate::ir::tree::traversal::{LevelNode, PostOrderWithFilter, REL_CAPACITY};
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;
use crate::ir::{Nodes, Plan};
use smol_str::format_smolstr;
use std::collections::HashSet;

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
    pub fn get_const_list(&self, top_id: NodeId, snapshot: Snapshot) -> Vec<NodeId> {
        // Here we need to output first so that constants have the right order.
        // Otherwise they will be in reverse order (e.g., $1 $2 $3 becomes $3 $2 $1).
        let tree = PostOrderWithFilter::new(
            |node| self.exec_plan_subtree_output_first_iter(node, snapshot),
            |node| {
                matches!(
                    self.get_node(node),
                    Ok(Node::Expression(Expression::Constant(_)))
                )
            },
            REL_CAPACITY,
        );

        let mut set = HashSet::new();
        let mut vec = Vec::new();
        for LevelNode(_, node_id) in tree.traverse_into_iter(top_id) {
            if !set.contains(&node_id) {
                vec.push(node_id);
                set.insert(node_id);
            }
        }

        vec
    }

    /// Collect constant nodes as SQL parameters without mutating the plan.
    ///
    /// # Errors
    /// - The plan is corrupted (collected constants point to invalid arena positions).
    pub fn collect_const_params(
        &self,
        top_id: NodeId,
        snapshot: Snapshot,
    ) -> Result<(Vec<NodeId>, Vec<Value>), SbroadError> {
        // TODO: ensure that constants.len() does not exceed the limit on the number of parameters
        // in tarantool
        let check_index = |num: usize| -> Result<(), SbroadError> {
            let _: u16 = (num + 1).try_into().map_err(|_| {
                SbroadError::Other(format_smolstr!("too many parameters in local sql: {num}"))
            })?;
            Ok(())
        };
        let constant_ids = self.get_const_list(top_id, snapshot);
        let mut params = Vec::with_capacity(constant_ids.len());
        for (num, const_id) in constant_ids.iter().enumerate() {
            check_index(num)?;
            params.push(self.as_const_value_ref(*const_id)?.clone());
        }
        Ok((constant_ids, params))
    }
}
