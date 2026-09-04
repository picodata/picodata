use smol_str::{SmolStr, ToSmolStr};
use std::collections::HashSet;
use std::ops::Not;

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::engine::{Router, Vshard};
use crate::executor::ir::ExecutionPlan;
use crate::executor::ExecutingQuery;
use crate::ir::bucket::{value_to_bucket_id, Buckets, BucketsResolver};
use crate::ir::distribution::Distribution;
use crate::ir::helpers::RepeatableState;
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::Relational;
use crate::ir::node::{
    Constant, Delete, Except, Intersect, Join, Motion, Node, NodeId, ScanRelation, Selection,
    Union, UnionAll, Values,
};
use crate::ir::operator::JoinKind;
use crate::ir::transformation::redistribution::{MotionKey, MotionPolicy, Target};
use crate::ir::tree::traversal::{PostOrderWithFilter, REL_CAPACITY};
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;

struct ExecutorBucketsResolver<'p, C: Router> {
    coordinator: &'p C,
    exec_plan: &'p ExecutionPlan,
}

impl<C: Router> BucketsResolver for ExecutorBucketsResolver<'_, C> {
    fn buckets_from_motion(&self, motion_id: NodeId) -> Result<Option<Buckets>, SbroadError> {
        let virtual_table = self.exec_plan.get_motion_vtable(motion_id)?;
        let bucket_ids: HashSet<_, _> = virtual_table.get_bucket_index().keys().copied().collect();
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

        let vshard_object = self.get_coordinator().get_current_vshard_object()?;
        let bucket_id = vshard_object.determine_bucket_id(&values)?;

        Ok(Some(bucket_id))
    }

    /// Hash sharding-key cells of every row in `Values` and return the bucket set.
    /// Returns `None` if any row's sharding-key cell isn't a Constant after
    /// parameter binding.
    fn try_buckets_from_values_rows(
        &self,
        values_id: NodeId,
        motion_key: &MotionKey,
    ) -> Result<Option<Buckets>, SbroadError> {
        let plan = self.exec_plan.get_ir_plan();
        let Relational::Values(Values { rows, .. }) = plan.get_relation_node(values_id)? else {
            panic!("expected Values, got {values_id:?}");
        };

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

    // Calculates buckets for sharded insert queries(e.g.
    // `INSERT INTO t VALUES (1, 2)`). In case when it's not
    // possible to calculate buckets it returns None.
    pub fn try_calculate_sharded_insert_buckets(&self) -> Result<Option<Buckets>, SbroadError> {
        let plan = self.get_exec_plan().get_ir_plan();
        let insert_id = plan.get_top()?;
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

        // We have to ensure that there are no motions below in IR.
        let dfs = PostOrderWithFilter::new(
            |x| plan.nodes.rel_iter(x),
            |id| {
                matches!(
                    plan.get_node(id),
                    Ok(Node::Relational(Relational::Motion(_)))
                )
            },
            REL_CAPACITY,
        );
        let child = child.expect("MOTION must contain child");
        if dfs.traverse_into_iter(child).next().is_some() {
            return Ok(None);
        }

        if !matches!(plan.get_relation_node(child)?, Relational::Values(_)) {
            return Ok(None);
        }

        self.try_buckets_from_values_rows(child, motion_key)
    }

    /// Compute buckets for a block-optimized INSERT whose Motion node has been
    /// eliminated, leaving Values directly under Insert. Sharding-key cells in
    /// each value row are guaranteed (by the block optimizer) to be Constants
    /// after parameter binding.
    fn insert_buckets_from_values(
        &self,
        insert_id: NodeId,
        values_id: NodeId,
    ) -> Result<Buckets, SbroadError> {
        let motion_key = self.exec_plan.get_ir_plan().insert_motion_key(insert_id)?;
        self.try_buckets_from_values_rows(values_id, &motion_key)?
            .ok_or_else(|| {
                SbroadError::other(
                    "INSERT in transaction requires constant or parameter values for sharding-key columns",
                )
            })
    }

    /// Discover required buckets to execute the query subtree.
    ///
    /// # Errors
    /// - Relational iterator returns non-relational nodes.
    /// - Failed to find a virtual table.
    /// - Relational nodes contain invalid children.
    #[allow(clippy::too_many_lines)]
    pub fn bucket_discovery(&mut self, top_id: NodeId) -> Result<Buckets, SbroadError> {
        let ir_plan = self.exec_plan.get_ir_plan();
        let top_node = ir_plan.get_relation_node(top_id)?;
        if matches!(top_node, Relational::Delete(Delete { child: None, .. })) {
            // DML without output (e.g. DELETE without WHERE clause) should be executed on all buckets.
            return Ok(Buckets::All);
        }

        // If the top node has Distribution::Single then the whole subtree must be executed only
        // on a single node, no need to traverse the subtree. Some nodes (e.g. DELETE with a
        // filter) never get a distribution, so an unset one is simply skipped.
        if let Ok(Distribution::Single) = ir_plan.rel_distr_ref(top_id) {
            return Ok(Buckets::Any);
        }

        // We use a `exec_plan_subtree_iter()` because we need DNF version of the
        // filter/condition expressions to determine buckets.
        let tree = PostOrderWithFilter::new(
            |node| {
                self.exec_plan
                    .effective_exec_plan_subtree_iter(node, Snapshot::Latest)
            },
            |node| matches!(ir_plan.get_node(node), Ok(Node::Relational(..))),
            REL_CAPACITY,
        );
        let resolver = ExecutorBucketsResolver {
            coordinator: self.coordinator,
            exec_plan: &self.exec_plan,
        };

        for node_id in tree.traverse_into_iter(top_id) {
            if self.bucket_map.contains_key(&node_id) {
                continue;
            }

            let rel = ir_plan.get_relation_node(node_id)?;
            match rel {
                Relational::ScanRelation(ScanRelation { relation, .. }) => {
                    if ir_plan
                        .get_relation_or_error(relation.as_str())?
                        .is_global()
                    {
                        self.bucket_map.insert(node_id, Buckets::Any);
                    } else {
                        self.bucket_map.insert(node_id, Buckets::All);
                    }
                }
                Relational::Motion(Motion { child, policy, .. }) => match policy {
                    MotionPolicy::Full => {
                        self.bucket_map.insert(node_id, Buckets::Any);
                    }
                    MotionPolicy::Segment(_) => {
                        let virtual_table = self.exec_plan.get_motion_vtable(node_id)?;
                        let buckets = virtual_table
                            .get_bucket_index()
                            .keys()
                            .copied()
                            .collect::<HashSet<_, _>>();
                        self.bucket_map
                            .insert(node_id, Buckets::new_filtered(buckets));
                    }
                    MotionPolicy::Local => {
                        let child_id = ir_plan.get_first_rel_child(node_id)?;
                        let child_buckets = self
                            .bucket_map
                            .get(&child_id)
                            .ok_or_else(|| {
                                SbroadError::FailedTo(
                                    Action::Retrieve,
                                    Some(Entity::Buckets),
                                    "of the child from the bucket map.".to_smolstr(),
                                )
                            })?
                            .clone();
                        self.bucket_map.insert(node_id, child_buckets);
                    }
                    MotionPolicy::LocalSegment(_) => {
                        // See `dispatch` method in `src/executor.rs` in order to understand when
                        // `virtual_table` materialized and when not for LocalSegment.
                        if self.exec_plan.contains_vtable_for_motion(node_id) {
                            let virtual_table = self.exec_plan.get_motion_vtable(node_id)?;
                            // In a case of `insert .. values ..` it is possible built a local
                            // segmented virtual table right on the router. So, we can use its
                            // buckets from the index to determine the buckets for the insert.
                            let buckets = virtual_table
                                .get_bucket_index()
                                .keys()
                                .copied()
                                .collect::<HashSet<_, _>>();
                            self.bucket_map
                                .insert(node_id, Buckets::new_filtered(buckets));
                        } else {
                            // We'll create and populate a local segmented virtual table on the
                            // storage later. At the moment the best thing we can do is to copy
                            // child's buckets.
                            let child_id = child.ok_or_else(|| {
                                SbroadError::UnexpectedNumberOfValues(
                                    "Motion node should have exactly one child".to_smolstr(),
                                )
                            })?;

                            let child_buckets = self
                                .bucket_map
                                .get(&child_id)
                                .ok_or_else(|| {
                                    SbroadError::FailedTo(
                                        Action::Retrieve,
                                        Some(Entity::Buckets),
                                        "of the child from the bucket map.".to_smolstr(),
                                    )
                                })?
                                .clone();
                            self.bucket_map.insert(node_id, child_buckets);
                        }
                    }
                    MotionPolicy::None => {
                        return Err(SbroadError::Invalid(
                            Entity::Motion,
                            Some(
                                "local motion policy should never appear in the plan".to_smolstr(),
                            ),
                        ));
                    }
                },
                Relational::Delete(Delete { child: None, .. }) => {
                    unreachable!("DELETE without WHERE clause should have been handled previously.")
                }
                Relational::Delete(Delete { child: Some(_), .. })
                | Relational::Insert(_)
                | Relational::Update(_) => {
                    let child_id = ir_plan.get_first_rel_child(node_id)?;
                    // Block-optimized INSERTs sit directly on top of a Values node (the
                    // motion got eliminated). Compute concrete buckets from the rows so
                    // the block dispatcher can route the transaction to a single bucket.
                    if matches!(rel, Relational::Insert(_))
                        && matches!(ir_plan.get_relation_node(child_id)?, Relational::Values(_))
                    {
                        let buckets = self.insert_buckets_from_values(node_id, child_id)?;
                        self.bucket_map.insert(node_id, buckets);
                        continue;
                    }
                    let child_buckets = self
                        .bucket_map
                        .get(&child_id)
                        .ok_or_else(|| {
                            SbroadError::FailedTo(
                                Action::Retrieve,
                                Some(Entity::Buckets),
                                "of the child from the bucket map.".to_smolstr(),
                            )
                        })?
                        .clone();
                    // By default Motion(Full) produces
                    // Buckets::Any, but insert, update and
                    // delete require to be executed on all nodes
                    // unless global table is modified
                    let mut my_buckets = child_buckets;
                    if !ir_plan.dml_node_table(node_id)?.is_global() {
                        if let Buckets::Any = my_buckets {
                            my_buckets = Buckets::All;
                        }
                    }
                    self.bucket_map.insert(node_id, my_buckets);
                }
                Relational::Projection(_)
                | Relational::GroupBy(_)
                | Relational::Having(_)
                | Relational::OrderBy(_)
                | Relational::ScanCte(_)
                | Relational::ScanSubQuery(_)
                | Relational::Limit(_) => {
                    let child_id = ir_plan.get_first_rel_child(node_id)?;
                    let child_buckets = self
                        .bucket_map
                        .get(&child_id)
                        .ok_or_else(|| {
                            SbroadError::FailedTo(
                                Action::Retrieve,
                                Some(Entity::Buckets),
                                "of the child from the bucket map.".to_smolstr(),
                            )
                        })?
                        .clone();
                    self.bucket_map.insert(node_id, child_buckets);
                }
                Relational::Except(Except { left, .. }) => {
                    // We are only interested in the first child (the left one).
                    // The rows from the second child would be transferred to the
                    // first child by the motion or already located in the first
                    // child's bucket. So we don't need to worry about the second
                    // child's buckets here.
                    let first_buckets = self
                        .bucket_map
                        .get(left)
                        .expect("of the first except child from the bucket map.")
                        .clone();
                    self.bucket_map.insert(node_id, first_buckets);
                }
                Relational::Union(Union { left, right, .. })
                | Relational::UnionAll(UnionAll { left, right, .. }) => {
                    let first_buckets = self
                        .bucket_map
                        .get(left)
                        .expect("of the first union all child from the bucket map.");
                    let second_buckets = self
                        .bucket_map
                        .get(right)
                        .expect("of the second union all child from the bucket map.");
                    let buckets = first_buckets.disjunct(second_buckets);
                    self.bucket_map.insert(node_id, buckets);
                }
                Relational::Intersect(Intersect { left, right, .. }) => {
                    let first_buckets = self
                        .bucket_map
                        .get(left)
                        .expect("of the first intersect child from the bucket map.");
                    let second_buckets = self
                        .bucket_map
                        .get(right)
                        .expect("of the second intersect child from the bucket map.");
                    let buckets = first_buckets.disjunct(second_buckets);
                    self.bucket_map.insert(node_id, buckets);
                }
                Relational::Selection(Selection {
                    child,
                    subqueries,
                    filter,
                    ..
                }) => {
                    // We need to get the buckets of the child node for the case
                    // when the filter returns no buckets to reduce.
                    let child_buckets = self
                        .bucket_map
                        .get(child)
                        .ok_or_else(|| {
                            SbroadError::FailedTo(
                                Action::Retrieve,
                                Some(Entity::Buckets),
                                "of the selection child from the bucket map.".to_smolstr(),
                            )
                        })?
                        .clone();
                    let filter_id = *filter;

                    let filter_buckets = if let Expression::Constant(Constant {
                        value: Value::Boolean(false) | Value::Null,
                    }) = ir_plan.get_expression_node(filter_id)?
                    {
                        Buckets::new_empty()
                    } else {
                        ir_plan.get_expression_tree_buckets(
                            filter_id,
                            &[*child],
                            subqueries,
                            &resolver,
                        )?
                    };

                    self.bucket_map
                        .insert(node_id, child_buckets.conjunct(&filter_buckets));
                }
                Relational::Join(Join {
                    left,
                    right,
                    subqueries,
                    condition,
                    kind,
                    ..
                }) => {
                    let inner_buckets = self
                        .bucket_map
                        .get(left)
                        .ok_or_else(|| {
                            SbroadError::FailedTo(
                                Action::Retrieve,
                                Some(Entity::Buckets),
                                "of the inner child from the bucket map.".to_smolstr(),
                            )
                        })?
                        .clone();
                    let outer_buckets = self
                        .bucket_map
                        .get(right)
                        .ok_or_else(|| {
                            SbroadError::FailedTo(
                                Action::Retrieve,
                                Some(Entity::Buckets),
                                "of the outer child from the bucket map.".to_smolstr(),
                            )
                        })?
                        .clone();
                    let condition_id = *condition;
                    let join_buckets = match kind {
                        JoinKind::Inner => {
                            let filter_buckets = ir_plan.get_expression_tree_buckets(
                                condition_id,
                                &[*left, *right],
                                subqueries,
                                &resolver,
                            )?;
                            inner_buckets
                                .disjunct(&outer_buckets)
                                .conjunct(&filter_buckets)
                        }
                        JoinKind::LeftOuter => inner_buckets.disjunct(&outer_buckets),
                    };
                    self.bucket_map.insert(node_id, join_buckets);
                }
                Relational::Values(_) => {
                    // We can materialize buckets on any node.
                    self.bucket_map.insert(node_id, Buckets::Any);
                }
                Relational::SelectWithoutScan(_) => {
                    self.bucket_map.insert(node_id, Buckets::Any);
                }
            }
        }

        let top_buckets = self
            .bucket_map
            .get(&top_id)
            .ok_or_else(|| {
                SbroadError::FailedTo(
                    Action::Retrieve,
                    Some(Entity::Buckets),
                    "of the top relation from the bucket map.".to_smolstr(),
                )
            })?
            .clone();

        Ok(top_buckets)
    }
}

#[cfg(test)]
mod tests;
