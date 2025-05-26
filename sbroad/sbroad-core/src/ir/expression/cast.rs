use crate::ir::node::expression::MutExpression;
use crate::ir::node::Cast;
use crate::ir::Plan;
use crate::{errors::SbroadError, ir::types::CastType};

use super::{MutNode, NodeId};

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

        let child_plan_node = self.get_mut_node(expr_id)?;
        if let MutNode::Expression(MutExpression::Parameter(ty)) = child_plan_node {
            ty.param_type.set(to_type.into());
        }

        Ok(cast_id)
    }
}
