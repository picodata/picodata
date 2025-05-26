use crate::ir::node::Node32;
use crate::{
    errors::SbroadError,
    ir::{
        node::{expression::Expression, Cast, Constant, Node, NodeId},
        tree::traversal::{LevelNode, PostOrderWithFilter},
        types::CastType,
        value::Value,
        Plan,
    },
};

fn apply_cast(plan: &Plan, child_id: NodeId, target_type: CastType) -> Option<Value> {
    match plan.get_expression_node(child_id).ok()? {
        Expression::Constant(Constant { value }) => {
            let value = value.clone();
            value.cast(target_type.into()).ok()
        }
        Expression::Cast(Cast {
            child: cast_child,
            to: cast_type,
        }) => {
            let value = apply_cast(plan, *cast_child, *cast_type);
            // Note: We don't throw errors if casting fails.
            // It's possible that some type and value combinations are missing,
            // but in such cases, we simply skip this evaluation and continue with other casts.
            // An optimization failure should not prevent the execution of the plan.
            value.and_then(|x| x.cast(target_type.into()).ok())
        }
        _ => None,
    }
}

impl Plan {
    /// Evaluates cast constant expressions and replaces them with actual values in the plan.
    ///
    /// This function focuses on simplifying the plan by eliminating unnecessary casts in selection
    /// expressions, enabling bucket filtering and in value rows, enabling local materialization.
    pub fn cast_constants(&mut self) -> Result<(), SbroadError> {
        let cast_filter = |node_id| {
            matches!(
                self.get_node(node_id),
                Ok(Node::Expression(Expression::Cast(_)))
            )
        };
        let mut subtree = PostOrderWithFilter::with_capacity(
            |node| self.subtree_iter(node, false),
            self.nodes.len(),
            Box::new(cast_filter),
        );

        let top_id = self.get_top()?;
        subtree.populate_nodes(top_id);
        let row_ids = subtree.take_nodes();
        drop(subtree);

        for LevelNode(_, cast_id) in row_ids {
            let replace_node = if let Expression::Cast(Cast {
                child: cast_child,
                to,
            }) = self.get_expression_node(cast_id)?
            {
                apply_cast(self, *cast_child, *to).map(|value| Node32::Constant(Constant { value }))
            } else {
                None
            };

            if let Some(node) = replace_node {
                self.nodes.replace32(cast_id, node)?;
            }
        }

        Ok(())
    }
}
