use ahash::AHashSet;

use crate::{
    errors::SbroadError,
    executor::{
        bucket::Buckets,
        engine::{Router, Vshard},
        Query,
    },
    ir::{
        node::{relational::Relational, Motion, Node, NodeId},
        transformation::redistribution::MotionPolicy,
        tree::traversal::{LevelNode, PostOrder, PostOrderWithFilter, REL_CAPACITY},
        Plan,
    },
};

#[derive(Debug)]
pub struct CalculatedBuckets {
    /// Estimated buckets on which whole plan will be executed.
    pub buckets: Buckets,
    /// True if estimation is correct, otherwise
    /// it means this is an upper bound.
    pub is_exact: bool,
    /// Total number of buckets in cluster
    pub bucket_count: u64,
}

#[derive(Debug)]
pub enum BucketsInfo {
    /// We can't calculate buckets for this query,
    /// see `can_estimate_buckets`
    Unknown,
    Calculated(CalculatedBuckets),
}

impl BucketsInfo {
    pub fn new_calculated(buckets: Buckets, is_exact: bool, bucket_count: u64) -> Self {
        BucketsInfo::Calculated(CalculatedBuckets {
            buckets,
            is_exact,
            bucket_count,
        })
    }

    /// Estimate on which buckets query will be executed.
    /// If query consists only of single subtree we
    /// can predict buckets precisely. If there are multiple
    /// subtrees we calculate the upper bound:
    ///
    /// We gather all subtrees from plan that don't have
    /// non-local motions and call `bucket_discovery` for
    /// each such node, then we merge (disjunct) all buckets
    /// for upper bound estimate.
    ///
    /// In case we can't compute buckets for this query, we
    /// `BucketsInfo::Unknown` variant.
    pub fn new_from_query<R: Router>(query: &mut Query<'_, R>) -> Result<Self, SbroadError> {
        let ir = query.get_exec_plan().get_ir_plan();
        if !Self::can_estimate_buckets(ir)? {
            return Ok(BucketsInfo::Unknown);
        }

        let top_id = ir.get_top()?;

        let mut dfs_tree = PostOrder::with_capacity(|node| ir.nodes.rel_iter(node), REL_CAPACITY);
        // Stores previously computed results for each
        // child of the current node: weather the child
        // has non-local motion in its subtree.
        let mut stack: Vec<(NodeId, bool)> = Vec::new();
        // Ids of nodes that don't have non-local motions in their subtrees.
        // We can safely call `bucket_discovery` on such nodes. For buckets
        // estimation we take union of all buckets produced by those nodes:
        //
        // m - non-local motion, n - any other kind of node
        //
        //               n1
        //              /  \
        //             m1  n2
        //             |
        //             n3
        //            /  \
        //           m4  m5
        //           |   |
        //           n4  n5
        //
        // For such subtree, we would have: {n4, n5, n2}
        // For single subtree without motions, we would have only root node.
        let mut without_motions_ids: AHashSet<NodeId> = AHashSet::new();
        // Ids of children of current node, that don't have non-local motions
        // in their subtree. If current node is a non-local motion or
        // some children have such motions in their subtrees, then
        // such children are to be used for buckets estimation.
        let mut cur_children_without_motions: Vec<NodeId> = Vec::new();
        for LevelNode(_, id) in dfs_tree.iter(top_id) {
            let rel = ir.get_relation_node(id)?;

            // true if this subtree has non-local motion
            let mut has_non_local_motion = false;
            for _ in 0..rel.children().len() {
                let (child_id, child_value) = stack.pop().expect("rel iter visits all children");

                if !child_value {
                    cur_children_without_motions.push(child_id);
                }

                has_non_local_motion = has_non_local_motion || child_value;
            }

            if rel.is_non_local_motion() {
                has_non_local_motion = true;
            }
            if has_non_local_motion {
                without_motions_ids.extend(cur_children_without_motions.iter());
            }
            cur_children_without_motions.clear();

            if !has_non_local_motion && top_id == id {
                without_motions_ids.insert(id);
            }

            stack.push((id, has_non_local_motion));
        }

        let mut estimated_buckets: Option<Buckets> = None;
        for child_id in &without_motions_ids {
            let buckets = query.bucket_discovery(*child_id)?;
            if let Some(ebuckets) = estimated_buckets.as_mut() {
                *ebuckets = ebuckets.disjunct(&buckets)?;
            } else {
                estimated_buckets = Some(buckets);
            }
        }

        let buckets = estimated_buckets.expect("there's at least one subtree");

        // Estimation is exact if we only have single
        // executable subtree == whole plan
        let is_exact = without_motions_ids.len() == 1 && without_motions_ids.contains(&top_id);
        let bucket_count = query
            .get_coordinator()
            .get_current_vshard_object()?
            .bucket_count();

        let buckets_info = BucketsInfo::new_calculated(buckets, is_exact, bucket_count);

        Ok(buckets_info)
    }

    /// Currently we can't estimate buckets for DML queries with
    /// non-local motions:
    /// insert
    ///    Motion(Segment)
    ///        Values (...)
    ///
    /// If we estimate whole query buckets by buckets of its leaf subtree,
    /// we get that the whole query will be executed on no more than one
    /// node (buckets `Any` corresponds to 1 node execution), which is
    /// wrong.
    ///
    /// Also we can't estimate buckets in plans with `Motion(Segment)`
    /// because after we resharding, we can get any set of buckets.
    fn can_estimate_buckets(plan: &Plan) -> Result<bool, SbroadError> {
        let top_id = plan.get_top()?;

        let mut contains_segment_motion = false;
        let filter = Box::new(|id: NodeId| -> bool {
            if !contains_segment_motion {
                contains_segment_motion = matches!(
                    plan.get_node(id),
                    Ok(Node::Relational(Relational::Motion(Motion {
                        policy: MotionPolicy::Segment(_),
                        ..
                    })))
                );
            }
            false
        });
        let mut dfs = PostOrderWithFilter::with_capacity(|x| plan.nodes.rel_iter(x), 0, filter);
        dfs.populate_nodes(top_id);
        drop(dfs);

        if contains_segment_motion {
            return Ok(false);
        }

        let node = plan.get_relation_node(top_id)?;
        if !node.is_dml() {
            return Ok(true);
        }
        if plan.dml_node_table(top_id)?.is_global() {
            return Ok(true);
        }

        let children = plan.children(top_id);
        if children.is_empty() {
            // Case of DELETE without WHERE.
            return Ok(true);
        }
        let child_id = children[0];
        let child_node = plan.get_relation_node(child_id)?;

        // In case of DELETE without WHERE clause it doesn't contain Motion child.
        let can_estimate = !child_node.is_motion() || child_node.is_local_motion();

        Ok(can_estimate)
    }
}
