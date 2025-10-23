use super::{NodeAligned, NodeOwned};
use crate::ir::{Entity, Node, Node32, NodeId, Plan, SbroadError};
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Tcl {
    Begin,
    Commit,
    Rollback,
}

impl Tcl {
    /// Returns a string slice representing the `Tcl` variant.
    pub fn as_str(&self) -> &str {
        match self {
            Tcl::Begin => "Begin",
            Tcl::Commit => "Commit",
            Tcl::Rollback => "Rollback",
        }
    }
}

impl From<Tcl> for NodeAligned {
    fn from(tcl: Tcl) -> Self {
        match tcl {
            Tcl::Begin => Self::Node32(Node32::Tcl(Tcl::Begin)),
            Tcl::Commit => Self::Node32(Node32::Tcl(Tcl::Commit)),
            Tcl::Rollback => Self::Node32(Node32::Tcl(Tcl::Rollback)),
        }
    }
}

impl From<Tcl> for NodeOwned {
    fn from(tcl: Tcl) -> Self {
        match tcl {
            Tcl::Begin => NodeOwned::Tcl(Tcl::Begin),
            Tcl::Commit => NodeOwned::Tcl(Tcl::Commit),
            Tcl::Rollback => NodeOwned::Tcl(Tcl::Rollback),
        }
    }
}

impl Plan {
    /// Get TCL node from the plan arena.
    ///
    /// # Errors
    /// - the node index is absent in arena
    /// - current node is not of TCL type
    pub fn get_tcl_node(&self, node_id: NodeId) -> Result<Tcl, SbroadError> {
        let node = self.get_node(node_id)?;
        match node {
            Node::Tcl(tcl) => Ok(tcl),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("node is not TCL type: {node:?}")),
            )),
        }
    }
}
