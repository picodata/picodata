use ahash::AHashSet;
use serde::{Deserialize, Serialize};
use sql_executor::executor::{
    engine::{Router, Vshard},
    ExecutingQuery,
};
use sql_ir::errors::SbroadError;
use sql_ir::ir::bucket::BucketSet;
use sql_ir::ir::{
    bucket::Buckets,
    node::{block::BlockOwned, relational::Relational, Motion, Node, NodeId},
    transformation::redistribution::MotionPolicy,
    tree::traversal::{PostOrder, REL_CAPACITY},
    Plan,
};
use std::fmt::Display;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BoundedBuckets {
    /// Estimated buckets on which whole plan will be executed.
    pub buckets: Buckets,
    /// Total number of buckets in cluster
    pub bucket_count: u64,
    /// Whether `buckets` is only an upper bound estimate rather than the exact
    /// execution set.
    pub is_upper_bound: bool,
}

impl Display for BoundedBuckets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let repr = buckets_repr(&self.buckets, self.bucket_count);
        match self.buckets {
            Buckets::All => write!(f, "buckets <= {repr}"),
            Buckets::Any => write!(f, "buckets = {repr}"),
            // Possible only in EXPLAIN (RAW): the query is not really
            // executed there, so the Segment motion's temporary table is
            // empty and bucket discovery yields an empty set. An empty
            // set with the upper-bound flag means buckets are calculated
            // during actual execution and are unknown yet, so show the
            // maximal possible range instead of `buckets <= []`.
            Buckets::Filtered(BucketSet::Exact(ref set))
                if set.is_empty() && self.is_upper_bound =>
            {
                write!(f, "buckets <= [1-{}]", self.bucket_count)
            }
            Buckets::Filtered(_) => {
                let sym = if self.is_upper_bound { "<=" } else { "=" };
                write!(f, "buckets {sym} {repr}")
            }
        }
    }
}

impl BoundedBuckets {
    pub fn new(buckets: Buckets, bucket_count: u64) -> Self {
        BoundedBuckets {
            buckets,
            bucket_count,
            is_upper_bound: false,
        }
    }
}

/// Estimate on which buckets query will be executed.
///
/// We gather all subtrees from plan that don't have
/// non-local motions and call `bucket_discovery` for
/// each such node, then we merge (disjunct) all buckets.
pub fn bounded_buckets_from_query<R: Router>(
    query: &mut ExecutingQuery<'_, R>,
) -> Result<BoundedBuckets, SbroadError> {
    let ir = query.get_exec_plan().get_ir_plan();
    let coord = query.get_coordinator();
    let vshard = coord.get_current_vshard_object().unwrap();
    let bucket_count = vshard.bucket_count();

    if ir.is_block()? {
        let top_id = ir.get_top()?;
        let block = ir.get_owned_block_node(top_id)?;
        let BlockOwned::Anonymous(block) = block else {
            unreachable!("plan.is_block() returned true, but top is {block:?}")
        };
        let buckets = query.calculate_block_buckets(&block)?;
        return Ok(BoundedBuckets::new(buckets, bucket_count));
    }

    if ir.is_sharded_insert()? {
        let buckets = query.try_calculate_sharded_insert_buckets()?;

        let actual_buckets = buckets.unwrap_or(Buckets::All);
        return Ok(BoundedBuckets::new(actual_buckets, bucket_count));
    }

    if !can_estimate_buckets(ir)? {
        return Ok(BoundedBuckets::new(Buckets::All, bucket_count));
    }

    let top_id = ir.get_top()?;

    let dfs_tree = PostOrder::new(|node| ir.nodes.rel_iter(node), REL_CAPACITY);
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
    for id in dfs_tree.traverse_into_iter(top_id) {
        let rel = ir.get_relation_node(id)?;
        let rel_deps_len = rel.children_len() + rel.subqueries().len();

        // true if this subtree has non-local motion
        let mut has_non_local_motion = false;
        for _ in 0..rel_deps_len {
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
        if let Some(estimated) = estimated_buckets.as_mut() {
            *estimated = estimated.disjunct(&buckets);
        } else {
            estimated_buckets = Some(buckets);
        }
    }

    let buckets = estimated_buckets.expect("there's at least one subtree");
    let buckets_info = BoundedBuckets::new(buckets, bucket_count);

    Ok(buckets_info)
}

/// Currently we don't estimate buckets for DML queries with
/// non-local motions:
/// insert
///    Motion(Segment)
///        Values (...)
/// Use `try_calculate_sharded_insert_buckets()` instead.
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

    let dfs = PostOrder::new(|node| plan.nodes.rel_iter(node), 0);
    for node in dfs.traverse_into_iter(top_id) {
        let has_segment_motion = matches!(
            plan.get_node(node),
            Ok(Node::Relational(Relational::Motion(Motion {
                policy: MotionPolicy::Segment(_),
                ..
            })))
        );

        if has_segment_motion {
            return Ok(false);
        }
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

pub fn buckets_repr(buckets: &Buckets, bucket_count: u64) -> String {
    match buckets {
        Buckets::All => format!("[1-{bucket_count}]"),
        Buckets::Filtered(BucketSet::Exact(buckets_set)) => 'f: {
            if buckets_set.is_empty() {
                break 'f "[]".into();
            }

            let mut nums: Vec<u64> = buckets_set.iter().copied().collect();
            nums.sort_unstable();

            let mut ranges = Vec::new();
            let mut l = 0;
            for r in 1..nums.len() {
                if nums[r - 1] + 1 == nums[r] {
                    continue;
                }
                if r - l == 1 {
                    ranges.push(format!("{}", nums[l]));
                } else {
                    ranges.push(format!("{}-{}", nums[l], nums[r - 1]))
                }
                l = r;
            }

            let r = nums.len();
            if r - l == 1 {
                ranges.push(format!("{}", nums[r - 1]));
            } else {
                ranges.push(format!("{}-{}", nums[l], nums[r - 1]))
            }

            format!("[{}]", ranges.join(","))
        }
        Buckets::Filtered(BucketSet::EstimatedCount { lower, upper }) => {
            if lower != upper {
                format!("estimated count ({lower}..={upper})")
            } else {
                format!("estimated count ({lower})")
            }
        }
        Buckets::Any => "any".into(),
    }
}
