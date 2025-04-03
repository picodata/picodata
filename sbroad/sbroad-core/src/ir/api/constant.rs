use smol_str::format_smolstr;

use crate::errors::{Entity, SbroadError};
use crate::ir::node::expression::Expression;
use crate::ir::node::{Constant, Node64, NodeId, Parameter};
use crate::ir::value::Value;
use crate::ir::DerivedType;
use crate::ir::{ArenaType, Nodes, Plan};

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
    pub fn get_const_list(&self) -> Vec<NodeId> {
        self.nodes
            .arena64
            .iter()
            .enumerate()
            .filter_map(|(id, node)| {
                if let Node64::Constant(_) = node {
                    Some(NodeId {
                        offset: u32::try_from(id).unwrap(),
                        arena_type: ArenaType::Arena64,
                    })
                } else {
                    None
                }
            })
            .collect()
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
    pub fn stash_constants(&mut self) -> Result<(), SbroadError> {
        let constants = self.get_const_list();
        for const_id in constants {
            let const_node = self.nodes.replace(
                const_id,
                Node64::Parameter(Parameter {
                    param_type: DerivedType::unknown(),
                }),
            )?;
            if let Node64::Constant(constant) = const_node {
                self.constants.insert(const_id, constant);
            } else {
                panic!("{const_node:?} is not a constant");
            }
        }
        Ok(())
    }
}
