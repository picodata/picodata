use crate::errors::SbroadError;
use crate::ir::node::Cast;
use crate::ir::types::CastType;
use crate::ir::Plan;

use super::NodeId;

impl Plan {
    /// Adds a cast expression to the plan.
    ///
    /// # Errors
    /// - Child node is not of the expression type.
    pub fn add_cast(&mut self, expr_id: NodeId, to_type: CastType) -> Result<NodeId, SbroadError> {
        let cast_expr = Cast {
            child: expr_id,
            to: to_type,
        };
        let cast_id = self.nodes.push(cast_expr.into());

        Ok(cast_id)
    }
}
