//! Undo transformation module.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::ir::node::NodeId;

/// Transformation log keep the history of the plan subtree modifications.
/// When we modify the plan subtree, we add a new entry to the log, where
/// the key is a new subtree top node and the value is the previous version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransformationLog {
    log: HashMap<NodeId, NodeId>,
}

impl Default for TransformationLog {
    fn default() -> Self {
        Self::new()
    }
}

impl TransformationLog {
    #[must_use]
    pub fn new() -> Self {
        Self {
            log: HashMap::new(),
        }
    }

    pub fn add(&mut self, new_id: NodeId, old_id: NodeId) {
        match self.log.get_key_value(&new_id) {
            None => {
                self.log.insert(new_id, old_id);
            }
            Some((_, prev_old_id)) => {
                self.log.insert(old_id, *prev_old_id);
                self.log.insert(new_id, old_id);
            }
        }
    }

    #[must_use]
    pub fn get(&self, new_id: &NodeId) -> Option<&NodeId> {
        self.log.get(new_id)
    }

    #[must_use]
    pub fn get_oldest(&self, new_id: &NodeId) -> Option<&NodeId> {
        match self.log.get_key_value(new_id) {
            None => None,
            Some((id, _)) => {
                let mut current = id;
                while let Some(parent) = self.get(current) {
                    current = parent;
                }
                Some(current)
            }
        }
    }
}
