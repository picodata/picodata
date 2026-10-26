use super::{Node32, NodeAligned, NodeOwned};
use crate::ir::{Entity, Node, NodeId, Plan, SbroadError};
use crate::utils::to_user;
use serde::Serialize;
use smol_str::format_smolstr;
use std::str::FromStr;

/// Node-local target reset by `DISCARD`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Discard {
    /// `DISCARD CACHE <cache>`: flush the given node-local cache.
    Cache(DiscardCache),
    /// `DISCARD <domain>`: reset everything in the given domain.
    All(DiscardDomain),
}

/// Node-local domain reset by `DISCARD`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum DiscardDomain {
    /// Every cache listed in [`DiscardCache`].
    Cache,
}

/// Node-local cache flushed by `DISCARD CACHE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum DiscardCache {
    /// Storage statement cache.
    SqlStmt,
    /// Router logical plan cache.
    SqlRouter,
    /// Anonymous block pattern cache.
    SqlBlock,
}

impl DiscardCache {
    pub const ALL: [DiscardCache; 3] = [
        DiscardCache::SqlStmt,
        DiscardCache::SqlRouter,
        DiscardCache::SqlBlock,
    ];

    /// Cache name as a normalized SQL identifier.
    pub fn as_str(&self) -> &'static str {
        match self {
            DiscardCache::SqlStmt => "sql_stmt",
            DiscardCache::SqlRouter => "sql_router",
            DiscardCache::SqlBlock => "sql_block",
        }
    }
}

impl FromStr for DiscardCache {
    type Err = SbroadError;

    /// Look up a cache by its normalized SQL identifier.
    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|cache| cache.as_str() == name)
            .ok_or_else(|| {
                let supported = Self::ALL.map(|cache| cache.as_str()).join(", ");
                SbroadError::Unsupported(
                    Entity::Cache,
                    Some(format_smolstr!(
                        "{} (supported: {supported})",
                        to_user(name)
                    )),
                )
            })
    }
}

impl From<Discard> for NodeAligned {
    fn from(discard: Discard) -> Self {
        Self::Node32(Node32::Discard(discard))
    }
}

impl From<Discard> for NodeOwned {
    fn from(discard: Discard) -> Self {
        NodeOwned::Discard(discard)
    }
}

impl Plan {
    /// Get Discard node from the plan arena.
    ///
    /// # Errors
    /// - the node index is absent in arena
    /// - current node is not of Discard type
    pub fn get_discard_node(&self, node_id: NodeId) -> Result<Discard, SbroadError> {
        let node = self.get_node(node_id)?;
        match node {
            Node::Discard(discard) => Ok(discard),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("node is not Discard type: {node:?}")),
            )),
        }
    }
}
