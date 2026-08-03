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

    pub fn add(&mut self, old_id_new: NodeId, new_id: NodeId) {
        // We are inserting a `new_id` as a key, because later
        // we'd like to retrieve old_id as value during the process of restoration.
        self.log.entry(new_id).or_insert(old_id_new);
    }

    #[must_use]
    pub fn get_oldest<'log, 'new: 'log>(&'log self, new_id: &'new NodeId) -> &'log NodeId {
        if self.log.contains_key(new_id) {
            let mut current_id = new_id;
            while let Some(prev_id) = self.log.get(current_id) {
                current_id = prev_id;
            }
            current_id
        } else {
            new_id
        }
    }
}
