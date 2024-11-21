//! Left Join trasformation logic when outer child has Global distribution
//! and inner child has Segment or Any distribution.

use smol_str::format_smolstr;

use crate::{
    errors::{Entity, SbroadError},
    ir::{
        distribution::Distribution,
        node::{relational::MutRelational, Join, NodeId},
        operator::JoinKind,
        Plan,
    },
};

use super::{MotionOpcode, MotionPolicy, Program, Strategy};

impl Plan {
    pub(super) fn calculate_strategy_for_left_join_with_global_tbl(
        &mut self,
        join_id: NodeId,
        join_kind: &JoinKind,
    ) -> Result<Option<Strategy>, SbroadError> {
        let is_left_join = matches!(join_kind, JoinKind::LeftOuter);
        let is_outer_global = matches!(
            self.get_rel_distribution(self.get_relational_child(join_id, 0)?)?,
            Distribution::Global
        );
        let is_inner_non_local = matches!(
            self.get_rel_distribution(self.get_relational_child(join_id, 1)?)?,
            Distribution::Segment { .. } | Distribution::Any
        );
        if !(is_left_join && is_outer_global && is_inner_non_local) {
            return Ok(None);
        }

        if let MutRelational::Join(Join { kind, .. }) = self.get_mut_relation_node(join_id)? {
            *kind = JoinKind::Inner;
        }
        self.set_distribution(self.get_relational_output(join_id)?)?;

        let Some(parent_id) = self.find_parent_rel(join_id)? else {
            return Err(SbroadError::Invalid(
                Entity::Plan,
                Some(format_smolstr!("join ({join_id:?}) has no parent!")),
            ));
        };
        let projection_id = create_projection(self, join_id)?;

        self.set_distribution(self.get_relational_output(projection_id)?)?;
        self.change_child(parent_id, join_id, projection_id)?;

        let outer_id = self.get_relational_child(join_id, 0)?;

        // In case there are no motions under outer child,
        // we need to add one, because we need to materialize
        // the subtree from which missing rows will be added.
        let outer_child_motion_id = {
            let child = self.get_relation_node(outer_id)?;
            let mut motion_child_id = None;
            // Check if there is already motion under outer child
            if child.is_subquery_or_cte() {
                let sq_child = self.get_relational_child(outer_id, 0)?;
                if self.get_relation_node(sq_child)?.is_motion() {
                    motion_child_id = Some(sq_child);
                }
            } else if child.is_motion() {
                motion_child_id = Some(outer_id);
            }

            if motion_child_id.is_none() {
                let motion_id =
                    self.add_motion(outer_id, &MotionPolicy::Full, Program::default())?;
                self.change_child(join_id, outer_id, motion_id)?;
                motion_child_id = Some(motion_id);
            }
            motion_child_id.unwrap()
        };

        // Add motion which will do the reduce stage of joining:
        // adding missing rows.
        let motion_op = MotionOpcode::AddMissingRowsForLeftJoin {
            motion_id: outer_child_motion_id,
        };
        let mut strategy = Strategy::new(parent_id);

        strategy.add_child(projection_id, MotionPolicy::Full, Program(vec![motion_op]));

        Ok(Some(strategy))
    }
}

fn create_projection(plan: &mut Plan, join_id: NodeId) -> Result<NodeId, SbroadError> {
    let proj_id = plan.add_proj(join_id, &[], false, false)?;
    let output_id = plan.get_relational_output(proj_id)?;
    plan.replace_parent_in_subtree(output_id, Some(join_id), Some(proj_id))?;
    plan.set_distribution(output_id)?;
    Ok(proj_id)
}
