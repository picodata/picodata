use crate::explain::utils::{TinyFmtBuffer, FMT_WIDTH, INDENT};
use ahash::AHashSet;
use itertools::Itertools;
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
    ExplainOptions, Plan,
};
use std::fmt::{Display, Write as _};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default)]
pub struct BucketFormatOptions {
    /// `FMT`: break a list wider than [`FMT_WIDTH`] up across lines.
    pub fmt: bool,
    /// `VERBOSE`: print every bucket instead of the first few.
    pub verbose: bool,
}

impl From<ExplainOptions> for BucketFormatOptions {
    fn from(options: ExplainOptions) -> Self {
        BucketFormatOptions {
            fmt: options.contains(ExplainOptions::Fmt),
            verbose: options.contains(ExplainOptions::Verbose),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BoundedBuckets {
    /// Estimated buckets on which whole plan will be executed.
    pub buckets: Buckets,
    /// Total number of buckets in cluster
    pub bucket_count: u64,
    /// Whether `buckets` is only an upper bound estimate rather than the exact
    /// execution set.
    pub is_upper_bound: bool,
    pub format_options: BucketFormatOptions,
}

impl Display for BoundedBuckets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let repr = buckets_repr(&self.buckets, self.bucket_count, self.format_options);
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
    pub fn new(buckets: Buckets, bucket_count: u64, format_options: BucketFormatOptions) -> Self {
        BoundedBuckets {
            buckets,
            bucket_count,
            is_upper_bound: false,
            format_options,
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
    let format_options = ir.explain_options.into();

    if ir.is_block()? {
        let top_id = ir.get_top()?;
        let block = ir.get_owned_block_node(top_id)?;
        let BlockOwned::Anonymous(block) = block else {
            unreachable!("plan.is_block() returned true, but top is {block:?}")
        };
        let buckets = query.calculate_block_buckets(&block)?;
        return Ok(BoundedBuckets::new(buckets, bucket_count, format_options));
    }

    if ir.is_sharded_insert()? {
        let buckets = query.try_calculate_sharded_insert_buckets()?;

        let actual_buckets = buckets.unwrap_or(Buckets::All);
        return Ok(BoundedBuckets::new(
            actual_buckets,
            bucket_count,
            format_options,
        ));
    }

    if !can_estimate_buckets(ir)? {
        return Ok(BoundedBuckets::new(
            Buckets::All,
            bucket_count,
            format_options,
        ));
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
    let buckets_info = BoundedBuckets::new(buckets, bucket_count, format_options);

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

/// Separator between the printed elements of a bucket list.
const SEP: &str = ", ";

/// Ids and ranges of the set, in printing order. A contiguous run of buckets
/// is passed in as a `(first, last)` pair: a run of a single bucket is printed
/// as a plain id, a run of two adjacent buckets as two ids (`1, 2`), and
/// anything wider as a range (`1-3`).
fn items(ranges: &[(u64, u64)]) -> impl Iterator<Item = (u64, u64)> + '_ {
    ranges.iter().flat_map(|&(l, r)| match r - l {
        1 => vec![(l, l), (r, r)],
        _ => vec![(l, r)],
    })
}

fn render(&(l, r): &(u64, u64)) -> String {
    match r - l {
        0 => l.to_string(),
        _ => format!("{l}-{r}"),
    }
}

/// Do the rendered elements, with the `... (N more)` marker for the `hidden`
/// buckets, fit into a single line of [`FMT_WIDTH`]?
fn fits_single_line(shown: &[(String, u64)], hidden: u64) -> bool {
    let mut line = TinyFmtBuffer::default();
    let items = shown.iter().map(|(text, _)| text).format(SEP);
    if hidden > 0 {
        write!(line, "[{items}{SEP}... ({hidden} more)]").is_ok()
    } else {
        write!(line, "[{items}]").is_ok()
    }
}

/// The elements a bucket list is printed as: ids, ranges and, when the list
/// is shortened, the trailing `... (N more)`.
struct BucketList(Vec<String>);

impl BucketList {
    /// Print the rendered elements, telling how many buckets are left out.
    fn new(mut items: Vec<String>, hidden: u64) -> Self {
        if hidden > 0 {
            items.push(format!("... ({hidden} more)"));
        }

        Self(items)
    }

    /// Every id and range of the set.
    fn full(ranges: &[(u64, u64)]) -> Self {
        Self::new(items(ranges).map(|item| render(&item)).collect(), 0)
    }

    /// The head of the set: as many ids and ranges as fit into
    /// [`FMT_WIDTH`], since a shortened list is always printed on a
    /// single line.
    fn shortened(ranges: &[(u64, u64)]) -> Self {
        /// How many buckets an id or a range covers.
        fn bucket_number(&(l, r): &(u64, u64)) -> u64 {
            r - l + 1
        }

        let mut shown: Vec<(String, u64)> = Vec::new();
        let mut hidden = ranges.iter().map(bucket_number).sum();

        for item in items(ranges) {
            let count = bucket_number(&item);
            shown.push((render(&item), count));
            hidden -= count;

            // The first element is printed however wide it is.
            if shown.len() > 1 && !fits_single_line(&shown, hidden) {
                let (_, count) = shown.pop().expect("more than one element");
                hidden += count;
                break;
            }
        }

        Self::new(shown.into_iter().map(|(text, _)| text).collect(), hidden)
    }

    /// Print the whole list on a single line.
    fn line(&self) -> String {
        format!("[{}]", self.0.join(SEP))
    }

    /// Print the list broken up across lines, every one of them holding as
    /// many elements as fit into [`FMT_WIDTH`]. A list that fits into a
    /// single line is printed as is.
    fn lines(&self) -> String {
        let mut buffer = TinyFmtBuffer::default();
        if write!(buffer, "[{}]", self.0.iter().format(SEP)).is_ok() {
            return buffer.to_string();
        }

        // Besides the elements and separators, a wrapped line holds the indent
        // and (unless it is the last one) a trailing comma.
        let budget = FMT_WIDTH - 1;

        let lines = self
            .0
            .iter()
            .fold(Vec::new(), |mut lines: Vec<String>, item| {
                match lines.last_mut() {
                    Some(line) if line.len() + SEP.len() + item.len() <= budget => {
                        line.push_str(SEP);
                        line.push_str(item);
                    }
                    _ => lines.push(format!("{INDENT}{item}")),
                }
                lines
            });

        format!("[\n{}\n]", lines.join(",\n"))
    }
}

/// Render bucket ranges as a comma separated list in brackets.
///
/// Without the `VERBOSE` option only the head of the list is printed,
/// followed by `... (N more)` with the number of buckets left out. Only
/// a full list is ever broken up across lines, and only with the `FMT`
/// option.
fn format_bucket_ranges(ranges: &[(u64, u64)], format_options: BucketFormatOptions) -> String {
    if !format_options.verbose {
        return BucketList::shortened(ranges).line();
    }

    let list = BucketList::full(ranges);
    if format_options.fmt {
        return list.lines();
    }

    list.line()
}

pub fn buckets_repr(
    buckets: &Buckets,
    bucket_count: u64,
    format_options: BucketFormatOptions,
) -> String {
    match buckets {
        Buckets::All => format!("[1-{bucket_count}]"),
        Buckets::Filtered(BucketSet::Exact(buckets_set)) => 'f: {
            if buckets_set.is_empty() {
                break 'f "[]".into();
            }

            let mut nums: Vec<u64> = buckets_set.iter().copied().collect();
            nums.sort_unstable();

            // Contiguous runs of buckets, collected as (first, last) pairs.
            let mut ranges: Vec<(u64, u64)> = Vec::new();
            let mut l = 0;
            for r in 1..nums.len() {
                if nums[r - 1] + 1 == nums[r] {
                    continue;
                }
                ranges.push((nums[l], nums[r - 1]));
                l = r;
            }
            ranges.push((nums[l], nums[nums.len() - 1]));

            format_bucket_ranges(&ranges, format_options)
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
