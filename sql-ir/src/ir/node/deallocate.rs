use crate::errors::{Entity, SbroadError};
use crate::ir::node::NodeAligned;
use crate::ir::{Node, NodeId, Plan};
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr};

use super::{Node32, NodeOwned};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deallocate {
    pub name: Option<SmolStr>,
}

impl From<Deallocate> for NodeAligned {
    fn from(deallocate: Deallocate) -> Self {
        Self::Node32(Node32::Deallocate(deallocate))
    }
}

impl From<Deallocate> for NodeOwned {
    fn from(deallocate: Deallocate) -> Self {
        NodeOwned::Deallocate(deallocate)
    }
}

impl Plan {
    /// Get Deallocate node from the plan arena.
    ///
    /// # Errors
    /// - the node index is absent in arena
    /// - current node is not of Deallocate type
    pub fn get_deallocate_node(&self, node_id: NodeId) -> Result<&Deallocate, SbroadError> {
        let node = self.get_node(node_id)?;
        match node {
            Node::Deallocate(deallocate) => Ok(deallocate),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("node is not Deallocate type: {node:?}")),
            )),
        }
    }
}
