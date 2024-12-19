use crate::{
    errors::SbroadError,
    ir::{
        node::{
            expression::{Expression, MutExpression},
            Cast, Constant, Node, NodeId, Row,
        },
        relation::Type,
        tree::traversal::{LevelNode, PostOrderWithFilter},
        value::Value,
        Plan,
    },
};

fn apply_cast(plan: &Plan, child_id: NodeId, target_type: Type) -> Option<Value> {
    match plan.get_expression_node(child_id).ok()? {
        Expression::Constant(Constant { value }) => {
            let value = value.clone();
            value.cast(target_type).ok()
        }
        Expression::Cast(Cast {
            child: cast_child,
            to: cast_type,
        }) => {
            let cast_type = cast_type.as_relation_type();
            let value = apply_cast(plan, *cast_child, cast_type);
            // Note: We don't throw errors if casting fails.
            // It's possible that some type and value combinations are missing,
            // but in such cases, we simply skip this evaluation and continue with other casts.
            // An optimization failure should not prevent the execution of the plan.
            value.and_then(|x| x.cast(target_type).ok())
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
        // For simplicity, we only evaluate constants wrapped in Row,
        // e.g., Row(Cast(Constant), Cast(Cast(Constant))).
        // This approach includes the target cases from the function comment
        // (selection expressions and values rows).
        let rows_filter = |node_id| {
            matches!(
                self.get_node(node_id),
                Ok(Node::Expression(Expression::Row(_)))
            )
        };
        let mut subtree = PostOrderWithFilter::with_capacity(
            |node| self.subtree_iter(node, false),
            self.nodes.len(),
            Box::new(rows_filter),
        );

        let top_id = self.get_top()?;
        subtree.populate_nodes(top_id);
        let row_ids = subtree.take_nodes();
        drop(subtree);

        let mut new_list = Vec::new();
        for LevelNode(_, row_id) in row_ids {
            // Clone row children list to overcome borrow checker.
            new_list.clear();
            if let Expression::Row(Row { list, .. }) = self.get_expression_node(row_id)? {
                new_list.clone_from(list);
            }

            // Try to apply cast to constants, push new values in the plan and remember ids in a
            // copy if row children list.
            for row_child in new_list.iter_mut() {
                if let Expression::Cast(Cast {
                    child: cast_child,
                    to,
                }) = self.get_expression_node(*row_child)?
                {
                    let to = to.as_relation_type();
                    if let Some(value) = apply_cast(self, *cast_child, to) {
                        *row_child = self.add_const(value);
                    }
                }
            }

            // Change row children to the new ones with casts applied.
            if let MutExpression::Row(Row { ref mut list, .. }) =
                self.get_mut_expression_node(row_id)?
            {
                new_list.clone_into(list);
            }
        }

        Ok(())
    }
}
