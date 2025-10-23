use super::filter_vtable;
use crate::errors::{Entity, SbroadError};
use crate::executor::bucket::Buckets;
use crate::executor::engine::Vshard;
use crate::executor::ir::ExecutionPlan;
use crate::ir::helpers::RepeatableState;
use crate::ir::node::relational::{MutRelational, Relational};
use crate::ir::node::{Motion, Node, NodeId};
use crate::ir::transformation::redistribution::{MotionOpcode, MotionPolicy};
use crate::ir::tree::relation::RelationalIterator;
use crate::ir::tree::traversal::{LevelNode, PostOrderWithFilter, REL_CAPACITY};
use crate::ir::Plan;
use ahash::AHashMap;
use rand::{thread_rng, Rng};
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
struct SerializeAsEmptyInfo {
    // ids of topmost motion nodes which have this opcode
    // with `true` value
    top_motion_ids: Vec<NodeId>,
    // ids of motions which have this opcode
    target_motion_ids: Vec<NodeId>,
    unused_motions: Vec<NodeId>,
    // ids of motions that are located below
    // top_motion_id, vtables corresponding
    // to those motions must be deleted from
    // replicaset message.
    motions_ref_count: AHashMap<NodeId, usize>,
}

impl Plan {
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

    fn collect_top_ids(&self) -> Result<Vec<NodeId>, SbroadError> {
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
        let filter = |node_id: NodeId| -> bool { self.is_serialize_as_empty_motion(node_id, true) };
        let dfs = PostOrderWithFilter::with_capacity(iter_children, 4, Box::new(filter));

        Ok(dfs.into_iter(self.get_top()?).map(|id| id.1).collect())
    }

    fn serialize_as_empty_info(&self) -> Result<Option<SerializeAsEmptyInfo>, SbroadError> {
        let top_ids = self.collect_top_ids()?;

        let mut motions_ref_count: AHashMap<NodeId, usize> = AHashMap::new();
        let filter = |node_id: NodeId| -> bool {
            matches!(
                self.get_node(node_id),
                Ok(Node::Relational(Relational::Motion(_)))
            )
        };
        let dfs =
            PostOrderWithFilter::with_capacity(|x| self.nodes.rel_iter(x), 0, Box::new(filter));
        for LevelNode(_, motion_id) in dfs.into_iter(self.get_top()?) {
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
            let is_motion = |node_id: NodeId| -> bool {
                matches!(
                    self.get_node(node_id),
                    Ok(Node::Relational(Relational::Motion(_)))
                )
            };
            let mut all_motions = Vec::new();
            for top_id in &top_ids {
                let dfs = PostOrderWithFilter::with_capacity(
                    |x| self.nodes.rel_iter(x),
                    REL_CAPACITY,
                    Box::new(is_motion),
                );
                all_motions.extend(dfs.into_iter(*top_id).map(|id| id.1));
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
            top_motion_ids: top_ids,
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
    mut sub_plan: ExecutionPlan,
) -> Result<HashMap<String, ExecutionPlan>, SbroadError> {
    let mut rs_ir = HashMap::new();
    rs_ir.reserve(rs_bucket_vec.len());
    if let Some((last, other)) = rs_bucket_vec.split_last() {
        let mut sae_info = sub_plan.get_ir_plan().serialize_as_empty_info()?;
        let mut other_plan = sub_plan.clone();

        if let Some(info) = sae_info.as_mut() {
            apply_serialize_as_empty_opcode(&mut other_plan, info)?;
        }
        if let Some((other_last, other_other)) = other.split_last() {
            for (rs, bucket_ids) in other_other {
                let mut rs_plan = other_plan.clone();
                filter_vtable(&mut rs_plan, bucket_ids)?;
                rs_ir.insert(rs.clone(), rs_plan);
            }
            let (rs, bucket_ids) = other_last;
            filter_vtable(&mut other_plan, bucket_ids)?;
            rs_ir.insert(rs.clone(), other_plan);
        }

        if let Some(ref info) = sae_info {
            disable_serialize_as_empty_opcode(&mut sub_plan, info)?;
        }
        let (rs, bucket_ids) = last;
        filter_vtable(&mut sub_plan, bucket_ids)?;
        rs_ir.insert(rs.clone(), sub_plan);
    }

    Ok(rs_ir)
}

fn apply_serialize_as_empty_opcode(
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

    for top_id in &info.top_motion_ids {
        sub_plan.unlink_motion_subtree(*top_id)?;
    }
    Ok(())
}

fn disable_serialize_as_empty_opcode(
    sub_plan: &mut ExecutionPlan,
    info: &SerializeAsEmptyInfo,
) -> Result<(), SbroadError> {
    for motion_id in &info.target_motion_ids {
        let program = if let MutRelational::Motion(Motion {
            policy, program, ..
        }) = sub_plan
            .get_mut_ir_plan()
            .get_mut_relation_node(*motion_id)?
        {
            if !matches!(policy, MotionPolicy::Local) {
                continue;
            }
            program
        } else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("expected motion node on id {motion_id:?}")),
            ));
        };
        for op in &mut program.0 {
            if let MotionOpcode::SerializeAsEmptyTable(enabled) = op {
                *enabled = false;
            }
        }
    }

    Ok(())
}

pub fn get_random_bucket(runtime: &impl Vshard) -> Buckets {
    let mut rng = thread_rng();
    let bucket_id: u64 = rng.gen_range(1..=runtime.bucket_count());
    let bucket_set: HashSet<u64, RepeatableState> = HashSet::from_iter(vec![bucket_id]);
    Buckets::Filtered(bucket_set)
}
