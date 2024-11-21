//! IR nodes representing blocks of commands.

use crate::errors::{Entity, SbroadError};
use crate::ir::node::{MutNode, NodeId};
use crate::ir::{Node, Plan};
use smol_str::format_smolstr;

use super::node::block::{Block, MutBlock};

impl Plan {
    /// Get a reference to a block node.
    ///
    /// # Errors
    /// - the node is not a block node.
    pub fn get_block_node(&self, node_id: NodeId) -> Result<Block, SbroadError> {
        let node = self.get_node(node_id)?;
        match node {
            Node::Block(block) => Ok(block),
            Node::Expression(_)
            | Node::Relational(_)
            | Node::Ddl(..)
            | Node::Acl(..)
            | Node::Invalid(..)
            | Node::Plugin(_)
            | Node::Parameter(..) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!(
                    "node {node:?} (id {node_id}) is not Block type"
                )),
            )),
        }
    }

    /// Get a mutable reference to a block node.
    ///
    /// # Errors
    /// - the node is not a block node.
    pub fn get_mut_block_node(&mut self, node_id: NodeId) -> Result<MutBlock, SbroadError> {
        let node = self.get_mut_node(node_id)?;
        match node {
            MutNode::Block(block) => Ok(block),
            MutNode::Expression(_)
            | MutNode::Relational(_)
            | MutNode::Ddl(..)
            | MutNode::Acl(..)
            | MutNode::Invalid(..)
            | MutNode::Plugin(_)
            | MutNode::Parameter(..) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!(
                    "node {node:?} (id {node_id}) is not Block type"
                )),
            )),
        }
    }
}
