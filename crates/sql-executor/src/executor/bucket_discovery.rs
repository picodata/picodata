use rustc_hash::FxHashMap;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::cell::RefCell;
use std::collections::HashSet;
use std::ops::Not;

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::engine::{Router, Vshard};
use crate::executor::ir::ExecutionPlan;
use crate::executor::ExecutingQuery;
use crate::ir::bucket::{value_to_bucket_id, Buckets, BucketsResolver};
use crate::ir::distribution::Distribution;
use crate::ir::helpers::RepeatableState;
use crate::ir::node::expression::{Expression, MutExpression};
use crate::ir::node::relational::Relational;
use crate::ir::node::{
    Constant, Delete, Insert, Join, Motion, Node, NodeId, ScanCte, ScanRelation, SelectWithoutScan,
    Selection, Values,
};
use crate::ir::operator::JoinKind;
use crate::ir::transformation::redistribution::{MotionKey, MotionPolicy, Target};
use crate::ir::transformation::BlockExecutionRestriction;
use crate::ir::tree::traversal::{NodeIter, PostOrder};
use crate::ir::value::Value;
use crate::ir::PlanKind;

struct ExecutorBucketsResolver<'p, C: Router> {
    coordinator: &'p C,
    exec_plan: &'p ExecutionPlan,
}

impl<C: Router> BucketsResolver for ExecutorBucketsResolver<'_, C> {
    type Buckets = Buckets;
    fn buckets_from_motion(&self, motion_id: NodeId) -> Result<Option<Buckets>, SbroadError> {
        let bucket_ids = motion_vtable_buckets(self.exec_plan, motion_id)?;
        let buckets = bucket_ids
            .is_empty()
            .not()
            .then(|| Buckets::new_filtered(bucket_ids));

        Ok(buckets)
    }

    fn buckets_from_values(
        &self,
        values: &[&Value],
        tier: Option<&SmolStr>,
    ) -> Result<Buckets, SbroadError> {
        let bucket = self
            .coordinator
            .get_vshard_object_by_tier(tier)?
            .determine_bucket_id(values)?;
        let bucket_set = [bucket].into_iter().collect();

        Ok(Buckets::new_filtered(bucket_set))
    }

    fn buckets_from_bucket_id(
        &self,
        value: &Value,
        tier: Option<&SmolStr>,
    ) -> Result<Buckets, SbroadError> {
        let bucket_count = self
            .coordinator
            .get_vshard_object_by_tier(tier)?
            .bucket_count();

        let buckets = match value_to_bucket_id(value) {
            Some(id) if (1..=bucket_count).contains(&id) => {
                let bucket_set = [id].into_iter().collect();
                Buckets::new_filtered(bucket_set)
            }
            _ => Buckets::new_empty(),
        };

        Ok(buckets)
    }
}

impl<T> ExecutingQuery<'_, T>
where
    T: Router,
{
    fn try_calculate_values_row_bucket_id(
        &self,
        row_list: &[NodeId],
        motion_key: &MotionKey,
    ) -> Result<Option<u64>, SbroadError> {
        let mut values = Vec::with_capacity(motion_key.targets.len());
        for target in motion_key.targets.iter() {
            match target {
                Target::Reference(pos) => {
                    let column_id = row_list
                        .get(*pos)
                        .expect("Reference position should point to column");
                    match self
                        .exec_plan
                        .get_ir_plan()
                        .get_expression_node(*column_id)?
                    {
                        Expression::Constant(const_node) => {
                            values.push(&const_node.value);
                        }
                        _ => {
                            return Ok(None);
                        }
                    };
                }
                Target::Value(val) => {
                    values.push(val);
                }
            }
        }

        let tier = self.exec_plan.get_ir_plan().tier.as_ref();
        let vshard_object = self.get_coordinator().get_vshard_object_by_tier(tier)?;
        let bucket_id = vshard_object.determine_bucket_id(&values)?;

        Ok(Some(bucket_id))
    }

    /// Hash sharding-key cells of every row in `Values` and return the bucket set.
    /// Returns `None` if any row's sharding-key cell isn't a Constant after
    /// parameter binding.
    fn try_buckets_from_values_rows(
        &self,
        rows: &[NodeId],
        motion_key: &MotionKey,
    ) -> Result<Option<Buckets>, SbroadError> {
        let plan = self.exec_plan.get_ir_plan();
        let mut bucket_set = HashSet::with_hasher(RepeatableState);
        for row_id in rows {
            let row_list = plan.get_row_list(*row_id)?;
            let Some(bucket_id) = self.try_calculate_values_row_bucket_id(row_list, motion_key)?
            else {
                return Ok(None);
            };
            bucket_set.insert(bucket_id);
        }
        Ok(Some(Buckets::new_filtered(bucket_set)))
    }

    // Calculates the buckets of the sharded INSERT `insert_id`, for example
    // `INSERT INTO t VALUES (1, 2)`. If this is not possible, the function
    // returns `None`.
    pub fn try_calculate_sharded_insert_buckets(
        &self,
        insert_id: NodeId,
    ) -> Result<Option<Buckets>, SbroadError> {
        let plan = self.get_exec_plan().get_ir_plan();
        let child_id = plan.dml_child_id(insert_id)?;
        let Relational::Motion(Motion { child, policy, .. }) = plan.get_relation_node(child_id)?
        else {
            return Ok(None);
        };

        let motion_key = match policy {
            MotionPolicy::Segment(motion_key) => motion_key,
            MotionPolicy::LocalSegment(_) => return Ok(Some(Buckets::All)),
            _ => return Ok(None),
        };

        let child = child.expect("MOTION must contain child");
        let Relational::Values(Values { rows, .. }) = plan.get_relation_node(child)? else {
            return Ok(None);
        };
        // We have to ensure that there are no motions below in IR.
        if plan.subtree_has_motions(child)? {
            return Ok(None);
        }

        self.try_buckets_from_values_rows(rows, motion_key)
    }

    /// Write the bucket of each block INSERT row to its `bucket_id` cell,
    /// the last cell of the row (see `Plan::optimize_block`).
    ///
    /// Returns `None` if the plan is not a block, or if `query_id` is not
    /// an INSERT of VALUES.
    pub(crate) fn bind_block_insert_buckets(
        &mut self,
        query_id: NodeId,
    ) -> Result<Option<Buckets>, SbroadError> {
        let plan = self.exec_plan.get_ir_plan();
        if !plan.top_is(PlanKind::BLOCK)? {
            return Ok(None);
        }
        let Relational::Insert(Insert { child, .. }) = plan.get_relation_node(query_id)? else {
            return Ok(None);
        };
        let Relational::Values(Values { rows, .. }) = plan.get_relation_node(*child)? else {
            return Ok(None);
        };
        let motion_key = plan.insert_motion_key(query_id)?;
        let mut bucket_cells = Vec::with_capacity(rows.len());
        for row_id in rows {
            let row_list = plan.get_row_list(*row_id)?;
            let bucket_id = self
                .try_calculate_values_row_bucket_id(row_list, &motion_key)?
                .ok_or_else(|| BlockExecutionRestriction::InsertShardingKey.into_error())?;
            let cell_id = *row_list
                .last()
                .expect("block INSERT row must have a bucket_id cell");
            bucket_cells.push((cell_id, bucket_id));
        }

        let plan = self.exec_plan.get_mut_ir_plan();
        let mut bucket_set = HashSet::with_hasher(RepeatableState);
        for (cell_id, bucket_id) in bucket_cells {
            let MutExpression::Constant(Constant { value }) =
                plan.get_mut_expression_node(cell_id)?
            else {
                panic!("expected bucket_id Constant, got {cell_id:?}");
            };
            *value = Value::Integer(bucket_id as i64);
            bucket_set.insert(bucket_id);
        }
        Ok(Some(Buckets::new_filtered(bucket_set)))
    }

    /// Whether the subtree of `top` reads or writes data placed by buckets: a
    /// sharded table or a motion other than a full (replicated) one.
    fn references_buckets(&self, top_id: NodeId, top: &Relational) -> Result<bool, SbroadError> {
        let ir_plan = self.exec_plan.get_ir_plan();
        // A DML top is checked before the walk is built: a sharded target
        // table is enough, whatever its child reads.
        if top.is_dml() && !ir_plan.dml_node_table(top_id)?.is_global() {
            return Ok(true);
        }
        let tree = PostOrder::new_by_node(
            &ir_plan.nodes,
            |id, node| match node {
                Node::Relational(rel) => self.exec_plan.exec_rel_children(id, &rel),
                _ => ir_plan.nodes.empty_rel_iter(),
            },
            top_id,
        );
        for (node_id, rel) in tree.relational() {
            let references_buckets = match rel {
                Relational::ScanRelation(ScanRelation { relation, .. }) => !ir_plan
                    .get_relation_or_error(relation.as_str())?
                    .is_global(),
                Relational::Insert(_) | Relational::Update(_) | Relational::Delete(_) => {
                    !ir_plan.dml_node_table(node_id)?.is_global()
                }
                Relational::Motion(Motion { policy, .. }) => !matches!(policy, MotionPolicy::Full),
                _ => false,
            };
            if references_buckets {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Discover required buckets to execute the query subtree.
    ///
    /// For a block INSERT of VALUES, the function writes the bucket of each
    /// row to its `bucket_id` cell. It gives these buckets.
    ///
    /// # Errors
    /// - Relational iterator returns non-relational nodes.
    /// - Failed to find a virtual table.
    /// - Relational nodes contain invalid children.
    #[allow(clippy::too_many_lines)]
    pub fn bucket_discovery(&mut self, top_id: NodeId) -> Result<Buckets, SbroadError> {
        if let Some(buckets) = self.bind_block_insert_buckets(top_id)? {
            return Ok(buckets);
        }

        let ir_plan = self.exec_plan.get_ir_plan();
        let top = ir_plan.get_relation_node(top_id)?;
        // A top with `Distribution::Single` runs on one node. It is checked
        // before `references_buckets`, as it needs no walk and both give
        // `Any`. `Insert` and `DELETE` without `WHERE` have no distribution:
        // `distribution()` is checked, as `rel_distr_ref` would build an
        // error for them.
        if matches!(top.distribution(), Some(Distribution::Single)) {
            return Ok(Buckets::Any);
        }
        // Global tables have no buckets, so a statement that touches only
        // them runs on any node whatever its filter (e.g. `WHERE false`).
        if !self.references_buckets(top_id, &top)? {
            return Ok(Buckets::Any);
        }

        // The buckets of the CTE bodies, so that a CTE referenced again is not
        // walked again.
        let memo: RefCell<FxHashMap<NodeId, Buckets>> = RefCell::default();
        // The relational children of a node are visited right before it, so
        // their buckets are the last `arity` entries of the stack.
        let tree = PostOrder::new_by_node(
            &ir_plan.nodes,
            |id, node| match node {
                Node::Relational(Relational::ScanCte(ScanCte { child, .. }))
                    if memo.borrow().contains_key(child) =>
                {
                    ir_plan.nodes.empty_rel_iter().take(0)
                }
                Node::Relational(rel) => self
                    .exec_plan
                    .exec_rel_children(id, &rel)
                    .take(walked_children(&rel)),
                _ => ir_plan.nodes.empty_rel_iter().take(0),
            },
            top_id,
        );
        let resolver = ExecutorBucketsResolver {
            coordinator: self.coordinator,
            exec_plan: &self.exec_plan,
        };
        // Few nodes wait for their parent at once, so the stack seldom grows.
        let mut stack: Vec<Buckets> = Vec::with_capacity(8);

        for (node_id, rel) in tree.relational() {
            if let Relational::ScanCte(ScanCte { child, .. }) = rel {
                if let Some(buckets) = memo.borrow().get(child) {
                    stack.push(buckets.clone());
                    continue;
                }
            }
            let arity = self
                .exec_plan
                .exec_rel_children(node_id, &rel)
                .take(walked_children(&rel))
                .count();
            let base = stack.len().checked_sub(arity).ok_or_else(|| {
                SbroadError::FailedTo(
                    Action::Retrieve,
                    Some(Entity::Buckets),
                    format_smolstr!("of the children of {node_id:?} from the stack."),
                )
            })?;
            let mut children = stack.drain(base..);

            let buckets = match rel {
                Relational::ScanRelation(ScanRelation { relation, .. }) => {
                    if ir_plan
                        .get_relation_or_error(relation.as_str())?
                        .is_global()
                    {
                        Buckets::Any
                    } else {
                        Buckets::All
                    }
                }
                Relational::Motion(Motion { policy, .. }) => {
                    match policy {
                        MotionPolicy::Full => Buckets::Any,
                        MotionPolicy::Segment(_) => {
                            Buckets::new_filtered(motion_vtable_buckets(&self.exec_plan, node_id)?)
                        }
                        // In a case of `insert .. values ..` it is possible to build a local
                        // segmented virtual table right on the router (see `dispatch` in
                        // `src/executor.rs`).
                        MotionPolicy::LocalSegment(_)
                            if self.exec_plan.contains_vtable_for_motion(node_id) =>
                        {
                            Buckets::new_filtered(motion_vtable_buckets(&self.exec_plan, node_id)?)
                        }
                        // A local segmented virtual table is built on the storage later, so
                        // the motion has the buckets of its child.
                        MotionPolicy::Local | MotionPolicy::LocalSegment(_) => {
                            next_child(&mut children)?
                        }
                        MotionPolicy::None => {
                            return Err(SbroadError::Invalid(
                                Entity::Motion,
                                Some(
                                    "local motion policy should never appear in the plan"
                                        .to_smolstr(),
                                ),
                            ));
                        }
                    }
                }
                Relational::Delete(_) | Relational::Insert(_) | Relational::Update(_) => {
                    // By default Motion(Full) produces
                    // Buckets::Any, but insert, update and
                    // delete require to be executed on all nodes
                    // unless global table is modified.
                    // DELETE without WHERE has no child.
                    let mut my_buckets = match rel {
                        Relational::Delete(Delete { child: None, .. }) => Buckets::Any,
                        _ => next_child(&mut children)?,
                    };
                    if !ir_plan.dml_node_table(node_id)?.is_global() {
                        if let Buckets::Any = my_buckets {
                            my_buckets = Buckets::All;
                        }
                    }
                    my_buckets
                }
                Relational::ScanCte(ScanCte { child, .. }) => {
                    let buckets = next_child(&mut children)?;
                    memo.borrow_mut().insert(*child, buckets.clone());
                    buckets
                }
                // `Except` takes the left child only: the motion moves the rows
                // of the right child to the buckets of the left one, or they
                // are there already.
                Relational::Projection(_)
                | Relational::GroupBy(_)
                | Relational::Having(_)
                | Relational::OrderBy(_)
                | Relational::ScanSubQuery(_)
                | Relational::Limit(_)
                | Relational::Except(_) => next_child(&mut children)?,
                Relational::Union(_) | Relational::UnionAll(_) | Relational::Intersect(_) => {
                    let first_buckets = next_child(&mut children)?;
                    let second_buckets = next_child(&mut children)?;
                    first_buckets.disjunct(&second_buckets)
                }
                Relational::Selection(Selection {
                    child,
                    subqueries,
                    filter,
                    ..
                }) => {
                    // We need to get the buckets of the child node for the case
                    // when the filter returns no buckets to reduce.
                    let child_buckets = next_child(&mut children)?;
                    let filter_buckets = ir_plan.get_expression_tree_buckets(
                        *filter,
                        &[*child],
                        subqueries,
                        &resolver,
                    )?;
                    child_buckets.conjunct(&filter_buckets)
                }
                Relational::Join(Join {
                    left,
                    right,
                    subqueries,
                    condition,
                    kind,
                    ..
                }) => {
                    let inner_buckets = next_child(&mut children)?;
                    let outer_buckets = next_child(&mut children)?;
                    match kind {
                        JoinKind::Inner => {
                            let filter_buckets = ir_plan.get_expression_tree_buckets(
                                *condition,
                                &[*left, *right],
                                subqueries,
                                &resolver,
                            )?;
                            inner_buckets
                                .disjunct(&outer_buckets)
                                .conjunct(&filter_buckets)
                        }
                        // Preserve the left input's buckets, including rows that do
                        // not match ON: neither the right input nor ON may prune them.
                        // If the left input is Any, use the right input's routing
                        // requirements, falling back to Any when the right is empty.
                        JoinKind::LeftOuter => match inner_buckets {
                            Buckets::Any => inner_buckets.disjunct(&outer_buckets),
                            _ => inner_buckets,
                        },
                    }
                }
                // We can materialize buckets on any node.
                Relational::Values(_) | Relational::ScanFunction(_) => Buckets::Any,
                Relational::SelectWithoutScan(SelectWithoutScan { subqueries, .. }) => {
                    let mut buckets = Buckets::Any;
                    // The planner pinned it to the bucket of its subqueries.
                    // If they read no bucket, it runs on any node.
                    if ir_plan.is_pinned_select_without_scan(node_id)? {
                        debug_assert!(
                            !subqueries.is_empty(),
                            "pinned SELECT without FROM {node_id} has no subqueries"
                        );
                        // It has no other relational children.
                        for _ in subqueries {
                            buckets = buckets.disjunct(&next_child(&mut children)?);
                        }
                        if !buckets.is_single_node() {
                            return Err(SbroadError::Invalid(
                                Entity::Buckets,
                                Some(format_smolstr!(
                                    "SELECT without FROM pinned to one bucket reads {buckets}"
                                )),
                            ));
                        }
                    }
                    buckets
                }
            };
            // The children that no arm reads go with the drain.
            drop(children);
            stack.push(buckets);
        }

        let top_buckets = stack.pop().ok_or_else(|| {
            SbroadError::FailedTo(
                Action::Retrieve,
                Some(Entity::Buckets),
                "of the top relation.".to_smolstr(),
            )
        })?;
        if !stack.is_empty() {
            return Err(SbroadError::FailedTo(
                Action::Retrieve,
                Some(Entity::Buckets),
                format_smolstr!("of {top_id:?}: buckets of nodes outside the subtree."),
            ));
        }
        Ok(top_buckets)
    }
}

/// The buckets of the rows in the virtual table of a motion.
fn motion_vtable_buckets(
    exec_plan: &ExecutionPlan,
    motion_id: NodeId,
) -> Result<HashSet<u64, RepeatableState>, SbroadError> {
    let virtual_table = exec_plan.get_motion_vtable(motion_id)?;
    Ok(virtual_table.get_bucket_index().keys().copied().collect())
}

/// How many relational children of a node bucket discovery walks. Subqueries
/// come after the other children, and only a `SELECT` without `FROM` reads
/// their buckets. The others are not walked: a subquery that a projection
/// shares with its `GROUP BY` is entered once, so both cannot take it from the
/// stack.
fn walked_children(rel: &Relational) -> usize {
    match rel {
        Relational::SelectWithoutScan(SelectWithoutScan { subqueries, .. }) => subqueries.len(),
        // Its child is the `HAVING`, the `GROUP BY` or the source, which
        // `children` does not give for the first two.
        Relational::Projection(_) => 1,
        _ => rel.children().len(),
    }
}

/// Takes the buckets of the next relational child of a node.
fn next_child(children: &mut impl Iterator<Item = Buckets>) -> Result<Buckets, SbroadError> {
    children.next().ok_or_else(|| {
        SbroadError::FailedTo(
            Action::Retrieve,
            Some(Entity::Buckets),
            "of a child from the stack.".to_smolstr(),
        )
    })
}

#[cfg(test)]
mod tests;
