use crate::ir::node::{Concat, Node};
use crate::ir::Plan;

use super::NodeId;

impl Plan {
    /// Add concatenation expression to the IR plan.
    ///
    /// # Errors
    /// - Left or right child nodes are not of the expression type.
    pub fn add_concat(&mut self, left_id: NodeId, right_id: NodeId) -> NodeId {
        debug_assert!(matches!(
            self.get_node(left_id),
            Ok(Node::Expression(_) | Node::Parameter(_))
        ));
        debug_assert!(matches!(
            self.get_node(right_id),
            Ok(Node::Expression(_) | Node::Parameter(_))
        ));

        self.nodes.push(
            Concat {
                left: left_id,
                right: right_id,
            }
            .into(),
        )
    }
}
