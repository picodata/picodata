use crate::ir::node::Concat;
use crate::ir::Plan;

use super::NodeId;

impl Plan {
    /// Add concatenation expression to the IR plan.
    ///
    /// # Errors
    /// - Left or right child nodes are not of the expression type.
    pub fn add_concat(&mut self, left_id: NodeId, right_id: NodeId) -> NodeId {
        debug_assert!(self.get_expression_node(left_id).is_ok());
        debug_assert!(self.get_expression_node(right_id).is_ok());

        self.nodes.push(
            Concat {
                left: left_id,
                right: right_id,
            }
            .into(),
        )
    }
}
