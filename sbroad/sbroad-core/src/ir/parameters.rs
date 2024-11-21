//! IR parameters module.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::ir::node::{Node64, NodeId};

// TODO: why this wrapper is needed? remove it
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Parameters(HashMap<NodeId, Node64>);

impl Default for Parameters {
    fn default() -> Self {
        Self::new()
    }
}

impl Parameters {
    #[must_use]
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert(&mut self, index: NodeId, node: Node64) {
        self.0.insert(index, node);
    }

    #[must_use]
    pub fn get(&self, index: NodeId) -> Option<&Node64> {
        self.0.get(&index)
    }

    pub fn drain(&mut self) -> HashMap<NodeId, Node64> {
        std::mem::take(&mut self.0)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
