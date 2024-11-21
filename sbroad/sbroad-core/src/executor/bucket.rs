use itertools::FoldWhile::{Continue, Done};
use itertools::Itertools;
use smol_str::{format_smolstr, ToSmolStr};
use std::collections::HashSet;

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::engine::{Router, Vshard};
use crate::executor::Query;
use crate::ir::distribution::Distribution;
use crate::ir::helpers::RepeatableState;
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::Relational;
use crate::ir::node::{
    BoolExpr, Delete, Except, GroupBy, Having, Insert, Intersect, Join, Limit, Motion, Node,
    NodeId, OrderBy, Projection, Row, ScanCte, ScanRelation, ScanSubQuery, SelectWithoutScan,
    Selection, Union, UnionAll, Update, Values, ValuesRow,
};
use crate::ir::operator::{Bool, JoinKind};
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::tree::traversal::{LevelNode, PostOrderWithFilter, REL_CAPACITY};
use crate::ir::value::Value;

/// Buckets are used to determine which nodes to send the query to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Buckets {
    // We don't want to keep thousands of buckets in memory
    // so we use a special enum to represent all the buckets
    // in a cluster.
    All,
    // A filtered set of buckets.
    Filtered(HashSet<u64, RepeatableState>),
    // Execute query on any single node, maybe locally.
    Any,
}

impl Buckets {
    /// Get no buckets in the cluster (coordinator).
    #[must_use]
    pub fn new_empty() -> Self {
        Buckets::Filtered(HashSet::with_hasher(RepeatableState))
    }

    /// Get a filtered set of buckets.
    #[must_use]
    pub fn new_filtered(buckets: HashSet<u64, RepeatableState>) -> Self {
        Buckets::Filtered(buckets)
    }

    /// Conjuction of two sets of buckets.
    ///
    /// # Errors
    /// - Buckets that can't be conjucted
    pub fn conjuct(&self, buckets: &Buckets) -> Result<Buckets, SbroadError> {
        let buckets = match (self, buckets) {
            (Buckets::All, Buckets::All) => Buckets::All,
            (Buckets::Filtered(b), Buckets::All) | (Buckets::All, Buckets::Filtered(b)) => {
                Buckets::Filtered(b.clone())
            }
            (Buckets::Filtered(a), Buckets::Filtered(b)) => {
                Buckets::Filtered(a.intersection(b).copied().collect())
            }
            (Buckets::Any, _) => buckets.clone(),
            (_, Buckets::Any) => self.clone(),
        };
        Ok(buckets)
    }

    /// Disjunction of two sets of buckets.
    ///
    /// # Errors
    /// - Buckets that can't be disjuncted
    pub fn disjunct(&self, buckets: &Buckets) -> Result<Buckets, SbroadError> {
        let buckets = match (self, buckets) {
            (Buckets::All, _) | (_, Buckets::All) => Buckets::All,
            (Buckets::Filtered(a), Buckets::Filtered(b)) => {
                Buckets::Filtered(a.union(b).copied().collect())
            }
            (Buckets::Any, _) => buckets.clone(),
            (_, Buckets::Any) => self.clone(),
        };
        Ok(buckets)
    }
}

impl<'a, T> Query<'a, T>
where
    T: Router,
{
    /// Inner logic of `get_expression_tree_buckets` for simple expressions (without OR and AND
    /// operators).
    /// In general it returns `Buckets::All`, but in some cases (e.g. `Eq` and `In` operators) it
    /// will return `Buckets::Filtered` (if such a result is met in SELECT or JOIN filter, it means
    /// that we can execute the query only on some of the replicasets).
    fn get_buckets_from_expr(&self, expr_id: NodeId) -> Result<Option<Buckets>, SbroadError> {
        // The only possible case there will be several `Buckets` in the vec is when we have `Eq`.
        // See the logic of its handling below.
        let mut buckets: Vec<Buckets> = vec![];
        let ir_plan = self.exec_plan.get_ir_plan();
        let tier = self.exec_plan.get_ir_plan().tier.as_ref();
        let expr = ir_plan.unwrap_expr(expr_id)?;

        // Try to collect buckets from expression of type `sharding_key = value`
        if let Expression::Bool(BoolExpr {
            op: Bool::Eq | Bool::In,
            left,
            right,
            ..
        }) = expr
        {
            let pairs = vec![(*left, *right), (*right, *left)];
            for (left_id, right_id) in pairs {
                let left_expr = ir_plan.unwrap_expr(left_id)?;
                if !matches!(left_expr, Expression::Row(_)) {
                    continue;
                }

                let right_expr = ir_plan.unwrap_expr(right_id)?;
                let right_columns = if let Expression::Row(Row { list, .. }) = right_expr {
                    list.clone()
                } else {
                    continue;
                };

                // Get the distribution of the left row.
                let left_dist = ir_plan.get_distribution(left_id)?;

                // Gather buckets from the right row.
                if let Distribution::Segment { keys } = left_dist {
                    // If the right side is a row referencing the motion
                    // it means that the corresponding virtual table contains
                    // tuple with the same distribution as the left side (because this motion
                    // was specially added in order to fulfill `Eq` of `In` conditions).
                    if let Some(motion_id) = ir_plan.get_motion_from_row(right_id)? {
                        let virtual_table = self.exec_plan.get_motion_vtable(motion_id)?;
                        let bucket_ids: HashSet<u64, RepeatableState> =
                            virtual_table.get_bucket_index().keys().copied().collect();
                        if !bucket_ids.is_empty() {
                            return Ok(Some(Buckets::new_filtered(bucket_ids)));
                        }
                    }

                    // The right side is a regular row or subquery which distribution
                    // didn't result in Motion creation (e.g. `"a" in (select "a" from t)`).
                    // So we have a case of `Eq` operator.
                    // If we have a case of constants on the positions of the left keys,
                    // we can return `Buckets::Filtered`.
                    // E.g. we have query
                    // `SELECT * FROM (SELECT A.a, B.b FROM A JOIN B ON A.a = B.b)
                    //  WHERE (a, b) = (0, 1)`.
                    // Here (a, b) row will have Distribution::Segment(keys = {[a], [b]}).
                    // After handling key "a" we will leave buckets which satisfy `a = 0`.
                    // After handling key "b" we will leave buckets which satisfy `b = 1`.
                    // In the end (when `conjuct` function is called) we will leave buckets
                    // which satisfy `(a, b) = (0, 1)`.
                    for key in keys.iter() {
                        let mut values: Vec<&Value> = Vec::new();
                        for position in &key.positions {
                            let right_column_id =
                                *right_columns.get(*position).ok_or_else(|| {
                                    SbroadError::NotFound(
                                        Entity::Column,
                                        format_smolstr!("at position {position} for right row"),
                                    )
                                })?;
                            let right_column_expr = ir_plan.get_expression_node(right_column_id)?;
                            if let Expression::Constant(_) = right_column_expr {
                                values.push(ir_plan.as_const_value_ref(right_column_id)?);
                            } else {
                                // One of the columns is not a constant. Skip this key.
                                values = Vec::new();
                                break;
                            }
                        }
                        if !values.is_empty() {
                            let bucket = self
                                .coordinator
                                .get_vshard_object_by_tier(tier)?
                                .determine_bucket_id(&values)?;
                            let bucket_set: HashSet<u64, RepeatableState> =
                                vec![bucket].into_iter().collect();
                            buckets.push(Buckets::new_filtered(bucket_set));
                        }
                    }
                }
            }
        }

        if buckets.is_empty() {
            return Ok(None);
        }

        let merged = buckets
            .into_iter()
            .fold_while(Ok(Buckets::Any), |acc, b| match acc {
                Ok(a) => Continue(a.conjuct(&b)),
                Err(_) => Done(acc),
            })
            .into_inner()?;

        Ok(Some(merged))
    }

    /// Inner logic of `bucket_discovery` for expressions (currently it's called only on
    /// `Selection` or `Join` filters).
    /// `rel_children` is a set of relational children of `Selection` or `Join` node.
    /// It splits given expression into DNF chains (`ORed` expressions) and calls `get_buckets_from_expr` on each simple
    /// chain subexpression, later disjuncting results.
    ///
    /// In case there is just one expression (not `ANDed`) `chains` will remain empty.
    fn get_expression_tree_buckets(
        &self,
        expr_id: NodeId,
        rel_children: &[NodeId],
    ) -> Result<Buckets, SbroadError> {
        let ir_plan = self.exec_plan.get_ir_plan();
        let chains = ir_plan.get_dnf_chains(expr_id)?;
        // When we can't extract buckets from expression,
        // we use default buckets for that expression with
        // idea that expression does not influence the result
        // bucket set for given subtree. But we must choose
        // between Buckets::Any and Buckets::All, we can't use
        // one of them in all cases:
        // `select a from global where true`
        // If expression `true` gives Buckets::All,
        // then we will get that query must be executed on all
        // nodes, which is wrong.
        // On the other hand, if Buckets::Any is the default:
        // `select a from segment_a where a = 1 or true`
        // `true` -> Buckets::Any || `a=1` -> Buckets::Filtered = Buckets::Filtered
        // which is wrong, because query must be executed on all nodes.
        // So, we should choose default buckets depending on
        // children's buckets: if some child subtree must be
        // executed on several nodes, we must use Buckets::All,
        // otherwise we should use Buckets::Any.
        let default_buckets = {
            let mut default_buckets = Buckets::Any;
            for child_id in rel_children {
                let child_dist = ir_plan.get_rel_distribution(*child_id)?;
                if matches!(child_dist, Distribution::Any | Distribution::Segment { .. }) {
                    default_buckets = Buckets::All;
                    break;
                }
            }
            default_buckets
        };
        let mut result: Vec<Buckets> = Vec::new();
        for mut chain in chains {
            let mut chain_buckets = default_buckets.clone();
            let nodes = chain.get_mut_nodes();
            // Nodes in the chain are in the top-down order (from left to right).
            // We need to pop back the chain to get nodes in the bottom-up order.
            while let Some(node_id) = nodes.pop_back() {
                let node_buckets = self
                    .get_buckets_from_expr(node_id)?
                    .unwrap_or(default_buckets.clone());
                chain_buckets = chain_buckets.conjuct(&node_buckets)?;
            }
            result.push(chain_buckets);
        }

        if let Some((first, other)) = result.split_first_mut() {
            for buckets in other {
                *first = first.disjunct(buckets)?;
            }
            return Ok(first.clone());
        }

        Ok(Buckets::All)
    }

    /// Discover required buckets to execute the query subtree.
    ///
    /// # Errors
    /// - Relational iterator returns non-relational nodes.
    /// - Failed to find a virtual table.
    /// - Relational nodes contain invalid children.
    #[allow(clippy::too_many_lines)]
    pub fn bucket_discovery(&mut self, top_id: NodeId) -> Result<Buckets, SbroadError> {
        let top_node = self.exec_plan.get_ir_plan().get_relation_node(top_id)?;
        if top_node.is_dml() && !top_node.has_output() {
            // DML without output (e.g. DELETE without WHERE clause) should be executed on all buckets.
            return Ok(Buckets::All);
        }

        // if top's output has Distribution::Single then the whole subtree must executed only on
        // a single node, no need to traverse the subtree
        let top_output_id = self.exec_plan.get_ir_plan().get_relational_output(top_id)?;
        if let Expression::Row(Row {
            distribution: Some(dist),
            ..
        }) = self
            .exec_plan
            .get_ir_plan()
            .get_expression_node(top_output_id)?
        {
            if *dist == Distribution::Single {
                return Ok(Buckets::Any);
            }
        }

        let ir_plan = self.exec_plan.get_ir_plan();
        let filter = |node_id: NodeId| -> bool {
            if let Ok(Node::Relational(..)) = ir_plan.get_node(node_id) {
                return true;
            }
            false
        };
        // We use a `exec_plan_subtree_iter()` because we need DNF version of the
        // filter/condition expressions to determine buckets.
        let mut tree = PostOrderWithFilter::with_capacity(
            |node| ir_plan.exec_plan_subtree_iter(node),
            REL_CAPACITY,
            Box::new(filter),
        );

        for LevelNode(_, node_id) in tree.iter(top_id) {
            if self.bucket_map.contains_key(&node_id) {
                continue;
            }

            let rel = self.exec_plan.get_ir_plan().get_relation_node(node_id)?;
            match rel {
                Relational::ScanRelation(ScanRelation {
                    output, relation, ..
                }) => {
                    if ir_plan
                        .get_relation_or_error(relation.as_str())?
                        .is_global()
                    {
                        self.bucket_map.insert(*output, Buckets::Any);
                    } else {
                        self.bucket_map.insert(*output, Buckets::All);
                    }
                }
                Relational::Motion(Motion {
                    children,
                    policy,
                    output,
                    ..
                }) => match policy {
                    MotionPolicy::Full => {
                        self.bucket_map.insert(*output, Buckets::Any);
                    }
                    MotionPolicy::Segment(_) => {
                        let virtual_table = self.exec_plan.get_motion_vtable(node_id)?;
                        let buckets = virtual_table
                            .get_bucket_index()
                            .keys()
                            .copied()
                            .collect::<HashSet<u64, RepeatableState>>();
                        self.bucket_map
                            .insert(*output, Buckets::new_filtered(buckets));
                    }
                    MotionPolicy::Local => {
                        let child_id = ir_plan.get_relational_child(node_id, 0)?;
                        let child_buckets = self
                            .bucket_map
                            .get(&ir_plan.get_relational_output(child_id)?)
                            .ok_or_else(|| {
                                SbroadError::FailedTo(
                                    Action::Retrieve,
                                    Some(Entity::Buckets),
                                    "of the child from the bucket map.".to_smolstr(),
                                )
                            })?
                            .clone();
                        self.bucket_map.insert(*output, child_buckets);
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
                                .collect::<HashSet<u64, RepeatableState>>();
                            self.bucket_map
                                .insert(*output, Buckets::new_filtered(buckets));
                        } else {
                            // We'll create and populate a local segmented virtual table on the
                            // storage later. At the moment the best thing we can do is to copy
                            // child's buckets.
                            let child_id = children.first().ok_or_else(|| {
                                SbroadError::UnexpectedNumberOfValues(
                                    "Motion node should have exactly one child".to_smolstr(),
                                )
                            })?;
                            let child_rel =
                                self.exec_plan.get_ir_plan().get_relation_node(*child_id)?;
                            let child_buckets = self
                                .bucket_map
                                .get(&child_rel.output())
                                .ok_or_else(|| {
                                    SbroadError::FailedTo(
                                        Action::Retrieve,
                                        Some(Entity::Buckets),
                                        "of the child from the bucket map.".to_smolstr(),
                                    )
                                })?
                                .clone();
                            self.bucket_map.insert(*output, child_buckets);
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
                Relational::Delete(Delete { output: None, .. }) => {
                    unreachable!("DELETE without WHERE clause should have been handled previously.")
                }
                Relational::Delete(Delete {
                    output: Some(output),
                    ..
                })
                | Relational::Insert(Insert { output, .. })
                | Relational::Update(Update { output, .. }) => {
                    let child_id = ir_plan.get_relational_child(node_id, 0)?;
                    let child_buckets = self
                        .bucket_map
                        .get(&ir_plan.get_relational_output(child_id)?)
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
                    self.bucket_map.insert(*output, my_buckets);
                }
                Relational::Projection(Projection { output, .. })
                | Relational::GroupBy(GroupBy { output, .. })
                | Relational::Having(Having { output, .. })
                | Relational::OrderBy(OrderBy { output, .. })
                | Relational::ScanCte(ScanCte { output, .. })
                | Relational::ScanSubQuery(ScanSubQuery { output, .. })
                | Relational::Limit(Limit { output, .. }) => {
                    let child_id = ir_plan.get_relational_child(node_id, 0)?;
                    let child_rel = ir_plan.get_relation_node(child_id)?;
                    let child_buckets = self
                        .bucket_map
                        .get(&child_rel.output())
                        .ok_or_else(|| {
                            SbroadError::FailedTo(
                                Action::Retrieve,
                                Some(Entity::Buckets),
                                "of the child from the bucket map.".to_smolstr(),
                            )
                        })?
                        .clone();
                    self.bucket_map.insert(*output, child_buckets);
                }
                Relational::Except(Except { left, output, .. }) => {
                    // We are only interested in the first child (the left one).
                    // The rows from the second child would be transferred to the
                    // first child by the motion or already located in the first
                    // child's bucket. So we don't need to worry about the second
                    // child's buckets here.
                    let first_rel = self.exec_plan.get_ir_plan().get_relation_node(*left)?;
                    let first_buckets = self
                        .bucket_map
                        .get(&first_rel.output())
                        .expect("of the first except child from the bucket map.")
                        .clone();
                    self.bucket_map.insert(*output, first_buckets);
                }
                Relational::Union(Union {
                    left,
                    right,
                    output,
                    ..
                })
                | Relational::UnionAll(UnionAll {
                    left,
                    right,
                    output,
                    ..
                }) => {
                    let first_rel = self.exec_plan.get_ir_plan().get_relation_node(*left)?;
                    let second_rel = self.exec_plan.get_ir_plan().get_relation_node(*right)?;
                    let first_buckets = self
                        .bucket_map
                        .get(&first_rel.output())
                        .expect("of the first union all child from the bucket map.");
                    let second_buckets = self
                        .bucket_map
                        .get(&second_rel.output())
                        .expect("of the second union all child from the bucket map.");
                    let buckets = first_buckets.disjunct(second_buckets)?;
                    self.bucket_map.insert(*output, buckets);
                }
                Relational::Intersect(Intersect {
                    left,
                    right,
                    output,
                    ..
                }) => {
                    let first_rel = self.exec_plan.get_ir_plan().get_relation_node(*left)?;
                    let second_rel = self.exec_plan.get_ir_plan().get_relation_node(*right)?;
                    let first_buckets = self
                        .bucket_map
                        .get(&first_rel.output())
                        .expect("of the first intersect child from the bucket map.");
                    let second_buckets = self
                        .bucket_map
                        .get(&second_rel.output())
                        .expect("of the second intersect child from the bucket map.");
                    let buckets = first_buckets.disjunct(second_buckets)?;
                    self.bucket_map.insert(*output, buckets);
                }
                Relational::Selection(Selection {
                    children,
                    filter,
                    output,
                    ..
                }) => {
                    // We need to get the buckets of the child node for the case
                    // when the filter returns no buckets to reduce.
                    let child_id = children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "current node should have exactly one child".to_smolstr(),
                        )
                    })?;

                    let child_rel = self.exec_plan.get_ir_plan().get_relation_node(*child_id)?;
                    let child_buckets = self
                        .bucket_map
                        .get(&child_rel.output())
                        .ok_or_else(|| {
                            SbroadError::FailedTo(
                                Action::Retrieve,
                                Some(Entity::Buckets),
                                "of the selection child from the bucket map.".to_smolstr(),
                            )
                        })?
                        .clone();
                    let output_id = *output;
                    let filter_id = *filter;

                    let filter_buckets = self.get_expression_tree_buckets(filter_id, children)?;
                    self.bucket_map
                        .insert(output_id, child_buckets.conjuct(&filter_buckets)?);
                }
                Relational::Join(Join {
                    children,
                    condition,
                    output,
                    kind,
                }) => {
                    if let (Some(inner_id), Some(outer_id)) = (children.first(), children.get(1)) {
                        let inner_rel =
                            self.exec_plan.get_ir_plan().get_relation_node(*inner_id)?;
                        let outer_rel =
                            self.exec_plan.get_ir_plan().get_relation_node(*outer_id)?;
                        let inner_buckets = self
                            .bucket_map
                            .get(&inner_rel.output())
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
                            .get(&outer_rel.output())
                            .ok_or_else(|| {
                                SbroadError::FailedTo(
                                    Action::Retrieve,
                                    Some(Entity::Buckets),
                                    "of the outer child from the bucket map.".to_smolstr(),
                                )
                            })?
                            .clone();
                        let output_id = *output;
                        let condition_id = *condition;
                        let join_buckets = match kind {
                            JoinKind::Inner => {
                                let filter_buckets =
                                    self.get_expression_tree_buckets(condition_id, children)?;
                                inner_buckets
                                    .disjunct(&outer_buckets)?
                                    .conjuct(&filter_buckets)?
                            }
                            JoinKind::LeftOuter => inner_buckets.disjunct(&outer_buckets)?,
                        };
                        self.bucket_map.insert(output_id, join_buckets);
                    } else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "current node should have at least two children".to_smolstr(),
                        ));
                    }
                }
                Relational::Values(Values { output, .. })
                | Relational::ValuesRow(ValuesRow { output, .. }) => {
                    // We can materialize buckets on any node.
                    self.bucket_map.insert(*output, Buckets::Any);
                }
                Relational::SelectWithoutScan(SelectWithoutScan { output, .. }) => {
                    self.bucket_map.insert(*output, Buckets::Any);
                }
            }
        }

        let top_rel = self.exec_plan.get_ir_plan().get_relation_node(top_id)?;
        let top_buckets = self
            .bucket_map
            .get(&top_rel.output())
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
