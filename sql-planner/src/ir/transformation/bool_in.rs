//! Replace all boolean "IN": operators with a chian of equalities,
//! combined by "OR" operator.
//!
//! For example, the following query:
//! ```sql
//! SELECT * FROM t WHERE a IN (1, 2, 3)
//! ```
//! would be converted to:
//! ```sql
//! SELECT * FROM t WHERE (a = 1) or (a = 2) or (a = 3)
//! ```

use crate::errors::{Entity, SbroadError};
use crate::ir::node::expression::Expression;
use crate::ir::node::{BoolExpr, NodeId};
use crate::ir::operator::Bool;
use crate::ir::Plan;
use smol_str::format_smolstr;

use super::{ExprId, TransformationOldNewPair};

/// Replace IN operator with the chain of the OR-ed equalities in the expression tree.
fn call_expr_tree_replace_in(
    plan: &mut Plan,
    _parent_id: NodeId,
    top_id: NodeId,
) -> Result<TransformationOldNewPair, SbroadError> {
    plan.expr_tree_replace_bool(top_id, &call_from_in, &[Bool::In])
}

fn call_from_in(plan: &mut Plan, top_id: NodeId) -> Result<ExprId, SbroadError> {
    plan.in_to_or(top_id)
}

impl Plan {
    /// Convert the IN operator to the chain of the OR-ed equalities.
    fn in_to_or(&mut self, top_id: NodeId) -> Result<ExprId, SbroadError> {
        let top_expr = self.get_expression_node(top_id)?;
        let (left_id, right_id) = match top_expr {
            Expression::Bool(BoolExpr {
                left,
                op: Bool::In,
                right,
                ..
            }) => (*left, *right),
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some(format_smolstr!(
                        "Node is not a boolean IN expression: {top_expr:?}"
                    )),
                ));
            }
        };

        // Do not apply current transformation to motion and sub-query nodes.
        if self.get_motion_from_row(right_id)?.is_some()
            || self.get_sub_query_from_row_node(right_id)?.is_some()
        {
            return Ok(top_id);
        }

        let right_columns = self.get_expression_node(right_id)?.clone_row_list()?;
        if let Some((first_id, other)) = right_columns.split_first() {
            let new_left_id = left_id;

            let mut new_top_id = self.add_cond(new_left_id, Bool::Eq, *first_id)?;

            for right_id in other {
                let new_right_id = self.add_cond(new_left_id, Bool::Eq, *right_id)?;
                new_top_id = self.concat_or(new_top_id, new_right_id)?;
            }

            return Ok(new_top_id);
        }
        Ok(top_id)
    }

    /// Replace all IN operators with the OR-ed chain of equalities.
    pub fn replace_in_operator(mut self) -> Result<Self, SbroadError> {
        self.transform_expr_trees(&call_expr_tree_replace_in)?;
        Ok(self)
    }
}

#[cfg(feature = "mock")]
#[cfg(test)]
mod tests;
