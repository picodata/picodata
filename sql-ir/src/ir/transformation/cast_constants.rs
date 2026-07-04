use crate::ir::node::{ArenaType, Node32};
use crate::{
    errors::SbroadError,
    ir::{
        node::{expression::Expression, Cast, Constant, NodeId},
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

// Collect cast node ids and return them in case there are some constant nodes.
fn find_cast_node_ids_for_cast_constants_opt(plan: &Plan) -> Vec<NodeId> {
    let mut found_const = false;
    let mut found_casts = vec![];

    for (offset, node) in plan.nodes.iter32().enumerate() {
        match node {
            Node32::Cast(_) => {
                let offset = offset.try_into().unwrap();
                let arena_type = ArenaType::Arena32;
                let cast_id = NodeId { offset, arena_type };
                found_casts.push(cast_id);
            }
            Node32::Constant(_) => found_const = true,
            _ => (),
        }
    }

    // If there is no constants there is nothing we can cast.
    if !found_const {
        return vec![];
    }

    found_casts
}

impl Plan {
    /// Evaluates cast constant expressions and replaces them with actual values in the plan.
    ///
    /// This function focuses on simplifying the plan by eliminating unnecessary casts in selection
    /// expressions, enabling bucket filtering and in value rows, enabling local materialization.
    pub fn cast_constants(mut self) -> Result<Self, SbroadError> {
        let cast_ids = find_cast_node_ids_for_cast_constants_opt(&self);
        for cast_id in cast_ids {
            let replace_node = if let Expression::Cast(Cast {
                child: cast_child,
                to,
            }) = self.get_expression_node(cast_id)?
            {
                apply_cast(&self, *cast_child, *to)
                    .map(|value| Node32::Constant(Constant { value }))
            } else {
                None
            };

            if let Some(node) = replace_node {
                self.nodes.replace32(cast_id, node)?;
            }
        }

        Ok(self)
    }
}
