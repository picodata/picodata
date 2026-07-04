use super::filter_vtable;
use crate::errors::{Entity, SbroadError};
use crate::executor::engine::Vshard;
use crate::executor::ir::ExecutionPlan;
use crate::ir::bucket::{BucketSet, Buckets};
use crate::ir::node::relational::Relational;
use crate::ir::node::{Motion, Node, NodeId};
use crate::ir::transformation::redistribution::{MotionOpcode, MotionPolicy};
use crate::ir::tree::relation::RelationalIterator;
use crate::ir::tree::traversal::{LevelNode, PostOrderWithFilter, REL_CAPACITY};
use crate::ir::Plan;
use ahash::AHashMap;
use smol_str::format_smolstr;
use std::collections::{HashMap, HashSet};

tarantool::define_str_enum! {
    #[derive(Default)]
    pub enum CacheInfo {
        #[default]
        CacheableFirstRequest = "cacheable-1",
        CacheableSecondRequest = "cacheable-2",
    }
}

// Helper struct to hold information
// needed to apply SerializeAsEmpty opcode
// to subtree.
pub struct SerializeAsEmptyInfo {
    // ids of motions which have this opcode
    target_motion_ids: Vec<NodeId>,
    unused_motions: Vec<NodeId>,
    // ids of motions that are located below
    // top_motion_id, vtables corresponding
    // to those motions must be deleted from
    // replicaset message.
    motions_ref_count: AHashMap<NodeId, usize>,
}

pub trait PlanSerializeAsEmptyExt {
    fn is_serialize_as_empty_motion(&self, node_id: NodeId, check_enabled: bool) -> bool;

    fn collect_top_ids(&self, top_id: NodeId) -> Result<Vec<NodeId>, SbroadError>;

    fn serialize_as_empty_info(
        &self,
        top_id: NodeId,
    ) -> Result<Option<SerializeAsEmptyInfo>, SbroadError>;
}

impl PlanSerializeAsEmptyExt for Plan {
    // return true if given node is Motion containing seriliaze as empty
    // opcode. If `check_enabled` is true checks that the opcode is enabled.
    fn is_serialize_as_empty_motion(&self, node_id: NodeId, check_enabled: bool) -> bool {
        if let Ok(Node::Relational(Relational::Motion(Motion { program, .. }))) =
            self.get_node(node_id)
        {
            if let Some(op) = program
                .0
                .iter()
                .find(|op| matches!(op, MotionOpcode::SerializeAsEmptyTable(_)))
            {
                return !check_enabled || matches!(op, MotionOpcode::SerializeAsEmptyTable(true));
            };
        }
        false
    }

    fn collect_top_ids(&self, top_id: NodeId) -> Result<Vec<NodeId>, SbroadError> {
        let mut stop_nodes: HashSet<NodeId> = HashSet::new();
        let iter_children = |node_id| -> RelationalIterator<'_> {
            if self.is_serialize_as_empty_motion(node_id, true) {
                stop_nodes.insert(node_id);
            }
            // do not traverse subtree with this child
            if stop_nodes.contains(&node_id) {
                return self.nodes.empty_rel_iter();
            }
            self.nodes.rel_iter(node_id)
        };
        let filter_empty_motion = |node| self.is_serialize_as_empty_motion(node, true);
        let dfs = PostOrderWithFilter::new(iter_children, filter_empty_motion, 4);

        Ok(dfs.traverse_into_iter(top_id).map(|id| id.1).collect())
    }

    fn serialize_as_empty_info(
        &self,
        top_id: NodeId,
    ) -> Result<Option<SerializeAsEmptyInfo>, SbroadError> {
        let top_ids = self.collect_top_ids(top_id)?;

        let mut motions_ref_count: AHashMap<NodeId, usize> = AHashMap::new();
        let dfs = PostOrderWithFilter::new(
            |node| self.nodes.rel_iter(node),
            |node| {
                matches!(
                    self.get_node(node),
                    Ok(Node::Relational(Relational::Motion(_)))
                )
            },
            0,
        );
        for LevelNode(_, motion_id) in dfs.traverse_into_iter(top_id) {
            motions_ref_count
                .entry(motion_id)
                .and_modify(|cnt| *cnt += 1)
                .or_insert(1);
        }

        if top_ids.is_empty() {
            return Ok(None);
        }

        // all motion nodes that are inside the subtrees
        // defined by `top_ids`
        let all_motion_nodes = {
            let mut all_motions = Vec::new();
            for top_id in &top_ids {
                let dfs = PostOrderWithFilter::new(
                    |node| self.nodes.rel_iter(node),
                    |node| {
                        matches!(
                            self.get_node(node),
                            Ok(Node::Relational(Relational::Motion(_)))
                        )
                    },
                    REL_CAPACITY,
                );
                all_motions.extend(dfs.traverse_into_iter(*top_id).map(|id| id.1));
            }
            all_motions
        };
        let mut target_motions = Vec::new();
        let mut unused_motions = Vec::new();
        for id in all_motion_nodes {
            if self.is_serialize_as_empty_motion(id, false) {
                target_motions.push(id);
            } else {
                unused_motions.push(id);
            }
        }

        Ok(Some(SerializeAsEmptyInfo {
            target_motion_ids: target_motions,
            unused_motions,
            motions_ref_count,
        }))
    }
}

/// Prepares execution plan for each replicaset.
///
/// # Errors
/// - Failed to apply customization opcodes
/// - Failed to filter vtable
pub fn prepare_rs_to_ir_map(
    rs_bucket_vec: &[(String, Vec<u64>)],
    sub_plan: ExecutionPlan,
    top_id: NodeId,
) -> Result<(HashMap<String, ExecutionPlan>, Option<u64>), SbroadError> {
    let mut rs_ir = HashMap::new();
    rs_ir.reserve(rs_bucket_vec.len());
    let mut extra_plan_id = None;
    if let Some((last, other)) = rs_bucket_vec.split_last() {
        let mut sae_info = sub_plan.get_ir_plan().serialize_as_empty_info(top_id)?;

        if !other.is_empty() {
            let mut other_plan = sub_plan.clone();
            if let Some(info) = sae_info.as_mut() {
                trim_serialize_as_empty_vtables(&mut other_plan, info)?;
                other_plan.salt_plan_id()?;
                // It's important to use `get_plan_id` because of the plan's metadata.
                extra_plan_id = Some(other_plan.get_plan_id()?);
            }

            let (other_last, other_other) = other
                .split_last()
                .expect("other replicasets must be non-empty");
            for (rs, bucket_ids) in other_other {
                let mut rs_plan = other_plan.clone();
                filter_vtable(&mut rs_plan, bucket_ids)?;
                rs_ir.insert(rs.clone(), rs_plan);
            }
            let (rs, bucket_ids) = other_last;
            filter_vtable(&mut other_plan, bucket_ids)?;
            rs_ir.insert(rs.clone(), other_plan);
        }

        let (rs, bucket_ids) = last;
        let sub_plan = prepare_single_rs_ir_plan_with_serialize_as_empty_info(
            sub_plan, bucket_ids, top_id, sae_info,
        )?;
        rs_ir.insert(rs.clone(), sub_plan);
    }

    Ok((rs_ir, extra_plan_id))
}

/// Prepares an execution plan for a single replicaset.
///
/// # Errors
/// - Failed to apply customization opcodes.
/// - Failed to filter vtable.
pub fn prepare_single_rs_ir_plan(
    sub_plan: ExecutionPlan,
    bucket_ids: &[u64],
    top_id: NodeId,
) -> Result<ExecutionPlan, SbroadError> {
    let sae_info = sub_plan.get_ir_plan().serialize_as_empty_info(top_id)?;
    prepare_single_rs_ir_plan_with_serialize_as_empty_info(sub_plan, bucket_ids, top_id, sae_info)
}

fn prepare_single_rs_ir_plan_with_serialize_as_empty_info(
    mut sub_plan: ExecutionPlan,
    bucket_ids: &[u64],
    top_id: NodeId,
    sae_info: Option<SerializeAsEmptyInfo>,
) -> Result<ExecutionPlan, SbroadError> {
    if let Some(ref info) = sae_info {
        let disabled_motions = serialize_as_empty_motions_to_disable(sub_plan.get_ir_plan(), info)?;
        sub_plan.disable_serialize_as_empty_for_motions(disabled_motions);
    }
    filter_vtable(&mut sub_plan, bucket_ids)?;
    if sae_info.is_some() {
        sub_plan.set_plan_id(top_id)?;
    }
    Ok(sub_plan)
}

fn trim_serialize_as_empty_vtables(
    sub_plan: &mut ExecutionPlan,
    info: &mut SerializeAsEmptyInfo,
) -> Result<(), SbroadError> {
    let vtables_map = sub_plan.get_mut_vtables();
    let unused_motions = std::mem::take(&mut info.unused_motions);
    for motion_id in &unused_motions {
        let Some(use_count) = info.motions_ref_count.get_mut(motion_id) else {
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "no ref count for motion={motion_id:?}"
            )));
        };
        if *use_count > 1 {
            *use_count -= 1;
        } else {
            vtables_map.remove(motion_id);
        }
    }

    Ok(())
}

pub fn serialize_as_empty_motions_to_disable(
    plan: &Plan,
    info: &SerializeAsEmptyInfo,
) -> Result<Vec<NodeId>, SbroadError> {
    let mut disabled_motions = Vec::with_capacity(info.target_motion_ids.len());
    for motion_id in &info.target_motion_ids {
        let Relational::Motion(Motion { policy, .. }) = plan.get_relation_node(*motion_id)? else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("expected motion node on id {motion_id:?}")),
            ));
        };
        if matches!(policy, MotionPolicy::Local) {
            disabled_motions.push(*motion_id);
        }
    }
    Ok(disabled_motions)
}

pub fn get_random_bucket(runtime: &impl Vshard) -> Buckets {
    let bucket_id: u64 = rand::random_range(1..=runtime.bucket_count());
    let bucket_set = [bucket_id].into_iter().collect();
    Buckets::Filtered(BucketSet::Exact(bucket_set))
}

#[cfg(test)]
mod tests {

    use super::{prepare_rs_to_ir_map, PlanSerializeAsEmptyExt};
    use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
    use crate::executor::engine::helpers::table_name;
    use crate::executor::ir::ExecutionPlan;
    use crate::executor::ir::SerializeAsEmptyState;
    use crate::executor::vtable::VirtualTable;
    use crate::ir::node::relational::Relational;
    use crate::ir::node::{ArenaType, Node136};
    use crate::ir::node::{Motion, NodeId};
    use crate::ir::transformation::redistribution::MotionOpcode;
    use crate::ir::tree::Snapshot;
    use crate::ir::Plan;
    use crate::test_helpers::sql_to_optimized_ir;
    use crate::test_helpers::vcolumn_integer_user_non_null;
    use std::rc::Rc;

    fn motion_ids(plan: &Plan) -> Vec<NodeId> {
        plan.nodes
            .iter136()
            .enumerate()
            .filter_map(|(offset, node)| {
                matches!(node, Node136::Motion(_)).then_some(NodeId {
                    offset: offset.try_into().expect("node offset must fit into u32"),
                    arena_type: ArenaType::Arena136,
                })
            })
            .collect()
    }

    fn motion_state(plan: &Plan, motion_id: NodeId) -> (Option<NodeId>, Option<bool>) {
        let Relational::Motion(Motion { child, program, .. }) =
            plan.get_relation_node(motion_id).expect("motion node")
        else {
            panic!("expected motion node on id {motion_id:?}");
        };
        let opcode = program.0.iter().find_map(|op| {
            if let MotionOpcode::SerializeAsEmptyTable(enabled) = op {
                Some(*enabled)
            } else {
                None
            }
        });
        (*child, opcode)
    }

    fn effective_serialize_as_empty(plan: &ExecutionPlan, motion_id: NodeId) -> Option<bool> {
        let Relational::Motion(Motion { program, .. }) = plan
            .get_ir_plan()
            .get_relation_node(motion_id)
            .expect("motion node")
        else {
            panic!("expected motion node on id {motion_id:?}");
        };
        match plan.effective_serialize_as_empty_state(motion_id, program) {
            SerializeAsEmptyState::Absent => None,
            SerializeAsEmptyState::Enabled => Some(true),
            SerializeAsEmptyState::Disabled => Some(false),
        }
    }

    fn sql(plan: &ExecutionPlan, top_id: NodeId) -> String {
        let sp = SyntaxPlan::new(plan, top_id, Snapshot::Oldest, false).expect("syntax plan");
        let ordered = OrderedSyntaxNodes::try_from(sp).expect("ordered syntax nodes");
        let nodes = ordered.to_syntax_data().expect("syntax data");
        let params = plan
            .local_sql_params(top_id, Snapshot::Oldest)
            .expect("local sql params");
        plan.generate_sql(&nodes, 0, table_name, Some(params.constant_ids().to_vec()))
            .expect("local sql")
    }

    #[test]
    fn prepare_rs_to_ir_map_keeps_serialize_as_empty_ir_intact() {
        let plan = sql_to_optimized_ir(
            r#"
            select * from (
                select "a" from "global_t"
                union
                select "e" from "t2"
            ) union
            select "f" from "t2"
            "#,
            vec![],
        );
        let top_id = plan.get_top().expect("top node");
        let mut exec_plan = ExecutionPlan::new(plan);
        let motion_ids = motion_ids(exec_plan.get_ir_plan());
        let sae_info = exec_plan
            .get_ir_plan()
            .serialize_as_empty_info(top_id)
            .expect("serialize_as_empty info")
            .expect("serialize_as_empty info must exist");
        assert!(!sae_info.target_motion_ids.is_empty());
        assert!(!sae_info.unused_motions.is_empty());

        let states_before = motion_ids
            .iter()
            .map(|id| (*id, motion_state(exec_plan.get_ir_plan(), *id)))
            .collect::<Vec<_>>();

        for motion_id in &motion_ids {
            let mut vtable = VirtualTable::new();
            vtable.add_column(vcolumn_integer_user_non_null());
            exec_plan
                .get_mut_vtables()
                .insert(*motion_id, Rc::new(vtable));
        }
        exec_plan.set_plan_id(top_id).expect("plan id");

        let rs_bucket_vec = vec![
            ("empty".to_string(), vec![1_u64]),
            ("real".to_string(), vec![2_u64]),
        ];
        let (rs_plans, extra_plan_id) =
            prepare_rs_to_ir_map(&rs_bucket_vec, exec_plan, top_id).expect("custom plans");
        assert!(extra_plan_id.is_some());

        for plan in rs_plans.values() {
            for (motion_id, state) in &states_before {
                assert_eq!(*state, motion_state(plan.get_ir_plan(), *motion_id));
            }
        }

        let empty_plan = rs_plans
            .values()
            .find(|plan| {
                sae_info
                    .target_motion_ids
                    .iter()
                    .all(|motion_id| effective_serialize_as_empty(plan, *motion_id) == Some(true))
            })
            .expect("empty plan");
        for motion_id in &sae_info.unused_motions {
            assert!(!empty_plan.get_vtables().contains_key(motion_id));
        }
        let empty_motion_id = sae_info.target_motion_ids[0];
        assert!(sql(empty_plan, empty_motion_id).contains("where false"));

        let real_plan = rs_plans
            .values()
            .find(|plan| {
                sae_info
                    .target_motion_ids
                    .iter()
                    .all(|motion_id| effective_serialize_as_empty(plan, *motion_id) == Some(false))
            })
            .expect("real plan");
        for motion_id in &sae_info.unused_motions {
            assert!(real_plan.get_vtables().contains_key(motion_id));
        }
        assert!(!sql(real_plan, empty_motion_id).contains("where false"));
    }

    #[test]
    fn prepare_rs_to_ir_map_ignores_serialize_as_empty_outside_top() {
        let plan = sql_to_optimized_ir(
            r#"
            select * from (
                select "a" from "global_t"
                union
                select "e" from "t2"
            ) union
            select "f" from "t2"
            "#,
            vec![],
        );
        let full_top_id = plan.get_top().expect("top node");
        let mut exec_plan = ExecutionPlan::new(plan);
        let sae_info = exec_plan
            .get_ir_plan()
            .serialize_as_empty_info(full_top_id)
            .expect("serialize_as_empty info")
            .expect("serialize_as_empty info must exist");

        let scoped_top_id = sae_info
            .target_motion_ids
            .iter()
            .find_map(|motion_id| {
                let child_top_id = exec_plan
                    .get_ir_plan()
                    .get_motion_subtree_root(*motion_id)
                    .ok()?;
                matches!(
                    exec_plan
                        .get_ir_plan()
                        .serialize_as_empty_info(child_top_id),
                    Ok(None)
                )
                .then_some(child_top_id)
            })
            .expect("serialize-as-empty motion child without nested serialize-as-empty");

        let motion_ids = motion_ids(exec_plan.get_ir_plan());
        for motion_id in &motion_ids {
            let mut vtable = VirtualTable::new();
            vtable.add_column(vcolumn_integer_user_non_null());
            exec_plan
                .get_mut_vtables()
                .insert(*motion_id, Rc::new(vtable));
        }
        exec_plan.set_plan_id(scoped_top_id).expect("plan id");

        let rs_bucket_vec = vec![
            ("empty".to_string(), vec![1_u64]),
            ("real".to_string(), vec![2_u64]),
        ];
        let (rs_plans, extra_plan_id) =
            prepare_rs_to_ir_map(&rs_bucket_vec, exec_plan, scoped_top_id).expect("custom plans");
        assert!(extra_plan_id.is_none());

        for plan in rs_plans.values() {
            for motion_id in &sae_info.unused_motions {
                assert!(plan.get_vtables().contains_key(motion_id));
            }
            for motion_id in &sae_info.target_motion_ids {
                assert_eq!(Some(true), effective_serialize_as_empty(plan, *motion_id));
            }
        }
    }
}
