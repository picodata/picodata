//! Resolve distribution conflicts and insert motion nodes to IR.

use ahash::{AHashMap, AHashSet, RandomState};
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::ir::SubtreeCloner;
use crate::ir::api::children::Children;
use crate::ir::distribution::{Distribution, Key, KeySet};
use crate::ir::expression::ColumnPositionMap;
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::{RelOwned, Relational};
use crate::ir::operator::{Bool, JoinKind, OrderByEntity, Unary, UpdateStrategy};

use crate::ir::node::{
    BoolExpr, Except, GroupBy, Having, Intersect, Join, Limit, NodeId, OrderBy, Projection,
    Reference, ScanCte, ScanRelation, ScanSubQuery, SelectWithoutScan, Selection, UnaryExpr, Union,
    UnionAll, Update, Values, ValuesRow,
};
use crate::ir::transformation::redistribution::eq_cols::EqualityCols;
use crate::ir::tree::traversal::{
    LevelNode, PostOrder, PostOrderWithFilter, EXPR_CAPACITY, REL_CAPACITY,
};
use crate::ir::value::Value;
use crate::ir::{Node, Plan};

pub(crate) mod dml;
pub(crate) mod eq_cols;
pub(crate) mod groupby;
pub(crate) mod left_join;

const CTE_CAPACITY: usize = 8;

#[derive(Debug)]
pub(crate) enum JoinChild {
    Inner,
    Outer,
}

/// Redistribution key targets (columns or values of the key).
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
pub enum Target {
    /// A position of the existing column in the tuple.
    Reference(usize),
    /// A value that should be used as a part of the
    /// redistribution key. We need it in a case of
    /// `insert into t1 (b) ...` when
    /// table t1 consists of two columns `a` and `b`.
    /// Column `a` is absent in insertion and should be generated
    /// from the default value.
    Value(Value),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
pub struct MotionKey {
    pub targets: Vec<Target>,
}

impl MotionKey {
    fn new() -> Self {
        MotionKey { targets: vec![] }
    }
}

impl From<Key> for MotionKey {
    fn from(key: Key) -> Self {
        MotionKey {
            targets: key.positions.into_iter().map(Target::Reference).collect(),
        }
    }
}

impl From<&Key> for MotionKey {
    fn from(key: &Key) -> Self {
        let positions: &[usize] = &key.positions;
        MotionKey {
            targets: positions
                .iter()
                .map(|pos| Target::Reference(*pos))
                .collect(),
        }
    }
}

/// Determinate what portion of data to move between data nodes in cluster.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum MotionPolicy {
    /// Nothing to move.
    None,
    /// Move all data.
    Full,
    /// Move only a segment of data according to the motion key.
    Segment(MotionKey),
    /// Materialize a virtual table on the storage without `bucket_id`
    /// calculation (see `LocalSegment` for opposite).
    ///
    /// E.g. we can see this policy when executing local update.
    /// We can't update system `bucket_id` column and that's why its taken from the tuple we're
    /// updating with no change.
    Local,
    /// Materialize a virtual table on the storage and calculate `bucket_id` for each tuple.
    ///
    /// E.g. we can see this policy when executing local insert (`insert into T1 select * from T2`).
    /// When inserting new tuple into `T1`, we need it to contain `bucket_id` field. That's why
    /// we store motion key for its calculation.
    /// The reason we don't get `bucket_id` from `T2` tuples is that such tuples may contain
    /// corrupted `bucket_id` value (e.g. resulted from user's wrong actions). That's why we make a
    /// decision to recalculate the value just in case.
    LocalSegment(MotionKey),
}

impl MotionPolicy {
    #[must_use]
    pub fn is_local(&self) -> bool {
        matches!(
            self,
            MotionPolicy::Local | MotionPolicy::LocalSegment(_) | MotionPolicy::None
        )
    }
}

pub type ColumnPosition = usize;

/// Single instruction to execute during virtual table tinkering.
/// See `Program` structure description.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum MotionOpcode {
    /// Set `primary_key` field of virtual table to the given argument.
    PrimaryKey(Vec<ColumnPosition>),
    /// Call `reshard` method on a vtable, if we met `Segment` or `LocalSegment` motion policy.
    ReshardIfNeeded,
    /// Call `rearrange_for_update` method on vtable and then call `set_update_delete_tuple_len`
    /// on `Update` relational node.
    RearrangeForShardedUpdate {
        update_id: NodeId,
        old_shard_columns_len: usize,
        new_shard_columns_positions: Vec<ColumnPosition>,
    },
    AddMissingRowsForLeftJoin {
        motion_id: NodeId,
    },
    /// When set to `true` this opcode serializes motion subtree to sql that produces
    /// empty table.
    ///
    /// Relevant only for Local motion policy. Must be initialized to `true` by planner,
    /// executor guarantees to mark only one replicaset which will have `false` value
    /// in this opcode. For all replicasets that will have `true` value, executor unlinks
    /// the sub-trees below the given motion nodes.
    ///
    /// Note: currently this opcode is only used for execution of union all having global
    /// child and sharded child.
    ///
    /// Note: look at `disable_serialize_as_empty_opcode` to see where we set the flag value
    /// to `false`.
    SerializeAsEmptyTable(bool),
    RemoveDuplicates,
}

/// Helper struct that unwraps `Expression::Bool` fields.
struct BoolOp {
    left: NodeId,
    op: Bool,
    right: NodeId,
}

impl BoolOp {
    fn from_expr(plan: &Plan, expr_id: NodeId) -> Result<Self, SbroadError> {
        if let Expression::Bool(BoolExpr {
            left, op, right, ..
        }) = plan.get_expression_node(expr_id)?
        {
            Ok(BoolOp {
                left: *left,
                op: op.clone(),
                right: *right,
            })
        } else {
            Err(SbroadError::Invalid(Entity::Operator, None))
        }
    }
}

/// Vec of opcodes that are executed in a `set_motion_vtable` function call.
/// We encapsulate the logic of virtual table tinkering into those opcodes.
/// The idea is that such logic is met among several cases and that we can operate those opcodes
/// instead of rewriting this logic everytime.
/// See the description of each command in `MotionOpcode` enum.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Program(pub Vec<MotionOpcode>);

impl Default for Program {
    fn default() -> Self {
        Program(vec![MotionOpcode::ReshardIfNeeded])
    }
}

impl Program {
    #[must_use]
    pub fn new(program: Vec<MotionOpcode>) -> Self {
        Program(program)
    }
}

type ChildId = NodeId;
type DataTransformation = (MotionPolicy, Program);

/// Helper struct to store motion policy for every child of
/// relational node with `parent_id`.
#[derive(Debug)]
struct Strategy {
    parent_id: NodeId,
    children_policy: HashMap<ChildId, DataTransformation>,
}

impl Strategy {
    fn new(parent_id: NodeId) -> Self {
        Self {
            parent_id,
            children_policy: HashMap::new(),
        }
    }

    /// Add motion policy for child node.
    /// Update policy in case `child_id` key is already in the `children_policy` map.
    fn add_child(&mut self, child_id: NodeId, policy: MotionPolicy, program: Program) {
        self.children_policy.insert(child_id, (policy, program));
    }

    fn get_rel_ids(&self) -> AHashSet<NodeId> {
        let mut vec_ids = Vec::new();
        for id in self.children_policy.keys() {
            vec_ids.push(*id);
        }
        let mut set_ids = AHashSet::new();
        for id in vec_ids {
            set_ids.insert(id);
        }
        set_ids
    }
}

/// Choose a policy for the inner join child (a policy with the largest motion of data wins).
fn join_policy_for_or(
    left_policy: &MotionPolicy,
    right_policy: &MotionPolicy,
) -> Result<MotionPolicy, SbroadError> {
    match (left_policy, right_policy) {
        (MotionPolicy::LocalSegment(_) | MotionPolicy::Local, _)
        | (_, MotionPolicy::LocalSegment(_) | MotionPolicy::Local) => Err(SbroadError::Invalid(
            Entity::Motion,
            Some("LocalSegment motion is not supported for joins".to_smolstr()),
        )),
        (MotionPolicy::Full, _) | (_, MotionPolicy::Full) => Ok(MotionPolicy::Full),
        (MotionPolicy::None, _) => Ok(right_policy.clone()),
        (_, MotionPolicy::None) => Ok(left_policy.clone()),
        (MotionPolicy::Segment(key_left), MotionPolicy::Segment(key_right)) => {
            if key_left == key_right {
                Ok(left_policy.clone())
            } else {
                Ok(MotionPolicy::Full)
            }
        }
    }
}

/// Choose a policy for the inner join child (a policy with the smallest motion of data wins).
fn join_policy_for_and(
    left_policy: &MotionPolicy,
    right_policy: &MotionPolicy,
) -> Result<MotionPolicy, SbroadError> {
    match (left_policy, right_policy) {
        (MotionPolicy::LocalSegment(_) | MotionPolicy::Local, _)
        | (_, MotionPolicy::LocalSegment(_) | MotionPolicy::Local) => Err(SbroadError::Invalid(
            Entity::Motion,
            Some("LocalSegment motion is not supported for joins".to_smolstr()),
        )),
        (MotionPolicy::Full, _) => Ok(right_policy.clone()),
        (_, MotionPolicy::Full) => Ok(left_policy.clone()),
        (MotionPolicy::None, _) | (_, MotionPolicy::None) => Ok(MotionPolicy::None),
        (MotionPolicy::Segment(key_left), MotionPolicy::Segment(key_right)) => {
            if key_left == key_right {
                Ok(left_policy.clone())
            } else {
                Ok(MotionPolicy::Full)
            }
        }
    }
}

impl Plan {
    /// Get unary NOT expression nodes.
    pub(crate) fn get_not_unary_nodes(&self, top: NodeId) -> Vec<LevelNode<NodeId>> {
        let filter = |node_id: NodeId| -> bool {
            matches!(
                self.get_node(node_id),
                Ok(Node::Expression(Expression::Unary(UnaryExpr {
                    op: Unary::Not,
                    ..
                })))
            )
        };
        let mut post_tree = PostOrderWithFilter::with_capacity(
            |node| self.nodes.expr_iter(node, false),
            EXPR_CAPACITY,
            Box::new(filter),
        );
        post_tree.populate_nodes(top);
        post_tree.take_nodes()
    }

    /// Get boolean expressions with both row children in the sub-tree.
    /// It's a helper function for resolving subquery conflicts.
    /// E.g. boolean `In` operator will have both row children where
    /// right `Row` is a transformed subquery.
    ///
    /// # Errors
    /// - some of the expression nodes are invalid
    pub(crate) fn get_bool_nodes_with_row_children(&self, top: NodeId) -> Vec<LevelNode<NodeId>> {
        let filter = |node_id: NodeId| -> bool {
            // Append only booleans with row children.
            if let Ok(Node::Expression(Expression::Bool(BoolExpr { left, right, .. }))) =
                self.get_node(node_id)
            {
                let left_is_row = matches!(
                    self.get_node(*left),
                    Ok(Node::Expression(Expression::Row(_)))
                );
                let right_is_row = matches!(
                    self.get_node(*right),
                    Ok(Node::Expression(Expression::Row(_)))
                );
                if left_is_row && right_is_row {
                    return true;
                }
            }
            false
        };
        let mut post_tree = PostOrderWithFilter::with_capacity(
            |node| self.nodes.expr_iter(node, false),
            EXPR_CAPACITY,
            Box::new(filter),
        );
        post_tree.populate_nodes(top);
        post_tree.take_nodes()
    }

    /// Get unary expressions with both row children in the sub-tree.
    /// It's a helper function for resolving subquery conflicts.
    /// E.g. unary `Exists` operator will have `Row` child that
    /// is a transformed subquery.
    ///
    /// # Errors
    /// - some of the expression nodes are invalid
    pub(crate) fn get_unary_nodes_with_row_children(&self, top: NodeId) -> Vec<LevelNode<NodeId>> {
        let filter = |node_id: NodeId| -> bool {
            // Append only unaries with row children.
            if let Ok(Node::Expression(Expression::Unary(UnaryExpr { child, .. }))) =
                self.get_node(node_id)
            {
                let child_is_row = matches!(
                    self.get_node(*child),
                    Ok(Node::Expression(Expression::Row(_)))
                );
                if child_is_row {
                    return true;
                }
            }
            false
        };
        let mut post_tree = PostOrderWithFilter::with_capacity(
            |node| self.nodes.expr_iter(node, false),
            EXPR_CAPACITY,
            Box::new(filter),
        );
        post_tree.populate_nodes(top);
        post_tree.take_nodes()
    }

    /// Get a single sub-query from the row node.
    ///
    /// # Errors
    /// - Row node is not of a row type
    ///
    /// There are more than one sub-queries in the row node.
    pub fn get_sub_query_from_row_node(
        &self,
        row_id: NodeId,
    ) -> Result<Option<NodeId>, SbroadError> {
        let rel_ids = self.get_relational_nodes_from_row(row_id)?;
        self.get_sub_query_among_rel_nodes(&rel_ids)
    }

    /// Get a single sub-query from among the list of relational nodes.
    ///
    /// # Errors
    /// - Some of the nodes are not relational
    /// - There are more than one sub-query
    pub fn get_sub_query_among_rel_nodes(
        &self,
        rel_nodes: &HashSet<NodeId, RandomState>,
    ) -> Result<Option<NodeId>, SbroadError> {
        let mut sq_set: HashSet<NodeId, RandomState> = HashSet::with_hasher(RandomState::new());
        for rel_id in rel_nodes {
            if let Node::Relational(Relational::ScanSubQuery(_)) = self.get_node(*rel_id)? {
                sq_set.insert(*rel_id);
            }
        }
        match sq_set.len().cmp(&1) {
            Ordering::Equal | Ordering::Greater => sq_set.iter().next().map_or_else(
                || {
                    Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "Failed to get the first sub-query node from the list of relational nodes: {rel_nodes:?}."
                    )))
                },
                |sq_id| Ok(Some(*sq_id)),
            ),
            Ordering::Less => Ok(None),
        }
    }

    /// Extract a motion node id from the row node
    ///
    /// # Errors
    /// - Row node is not of a row type
    /// - There are more than one motion nodes in the row node
    pub fn get_motion_from_row(&self, node_id: NodeId) -> Result<Option<NodeId>, SbroadError> {
        let rel_nodes = self.get_relational_nodes_from_row(node_id)?;
        self.get_motion_among_rel_nodes(&rel_nodes)
    }

    /// Extract motion node id from row node
    ///
    /// # Errors
    /// - Some of the nodes are not relational
    pub fn get_motion_among_rel_nodes(
        &self,
        rel_nodes: &HashSet<NodeId, RandomState>,
    ) -> Result<Option<NodeId>, SbroadError> {
        let mut motion_set: HashSet<NodeId> = HashSet::new();

        for child in rel_nodes {
            if self.get_relation_node(*child)?.is_motion() {
                motion_set.insert(*child);
            }
        }

        match motion_set.len().cmp(&1) {
            Ordering::Equal | Ordering::Greater => {
                let motion_id = motion_set.iter().next().ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(
                        "failed to get the first Motion node from the set.".into(),
                    )
                })?;
                Ok(Some(*motion_id))
            }
            Ordering::Less => Ok(None),
        }
    }

    /// Check for join/sq equality on `bucket_id` column:
    /// ```text
    /// .. on (t1.a, t1.bucket_id) = (t2.b, t2.bucket_id)
    ///
    /// select * from t1 where bucket_id in (select bucket_id from t2)
    /// ```
    ///
    /// In such case join/selection can be done locally.
    fn has_eq_on_bucket_id(
        &self,
        left_row_id: NodeId,
        right_row_id: NodeId,
        rel_id: NodeId,
        op: &Bool,
    ) -> Result<bool, SbroadError> {
        if !(Bool::Eq == *op || Bool::In == *op) {
            return Ok(false);
        }
        // It is possible that multiple columns in row refer to the shard column
        // we need to find if there is a pair of such columns from different
        // children for local join:
        //
        // select * from (select bucket_id as a from t1) as t1
        // join (select bucket_id as b from t2) as t2
        // on (1, a, b) = (2, b, 3)
        //
        // Equality pair `a = b` allows us to do local join.
        //
        // position in row that refers to shard column -> child id
        let mut memo: AHashMap<usize, NodeId> = AHashMap::new();
        let mut search_row = |row_id: NodeId| -> Result<bool, SbroadError> {
            let refs = self.get_row_list(row_id)?;
            for (pos_in_row, ref_id) in refs.iter().enumerate() {
                let ref node @ Expression::Reference(Reference {
                    targets,
                    position: ref_pos,
                    ..
                }) = self.get_expression_node(*ref_id)?
                else {
                    continue;
                };
                let targets = targets.as_ref().ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Node,
                        Some(format_smolstr!(
                            "ref ({ref_id:?}) in join condition with no targets: {node:?}"
                        )),
                    )
                })?;
                let child_idx = targets.first().ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Node,
                        Some(format_smolstr!(
                            "ref ({ref_id:?}) in join condition with empty targets: {node:?}"
                        )),
                    )
                })?;
                let child_id = self.get_relational_child(rel_id, *child_idx)?;
                let mut context = self.context_mut();
                if let Some(positions) = context.get_shard_columns_positions(child_id, self)? {
                    if positions[0] != Some(*ref_pos) && positions[1] != Some(*ref_pos) {
                        continue;
                    }
                    if let Some(other_child_id) = memo.get(&pos_in_row) {
                        if *other_child_id != child_id {
                            return Ok(true);
                        }
                    } else {
                        memo.insert(pos_in_row, child_id);
                    }
                }
            }

            Ok(false)
        };
        Ok(search_row(left_row_id)? || search_row(right_row_id)?)
    }

    /// Choose a `MotionPolicy` strategy for the inner row.
    ///
    /// # Errors
    /// - nodes are not rows
    /// - uninitialized distribution for some row
    fn choose_strategy_for_bool_op_inner_sq(
        &self,
        outer_id: NodeId,
        inner_id: NodeId,
        op: &Bool,
    ) -> Result<MotionPolicy, SbroadError> {
        let outer_dist = self.get_distribution(outer_id)?;
        let inner_dist = self.get_distribution(inner_id)?;
        if let Distribution::Global = inner_dist {
            return Ok(MotionPolicy::None);
        }
        if Bool::Eq == *op || Bool::In == *op {
            match outer_dist {
                Distribution::Segment {
                    keys: ref keys_outer,
                } => {
                    // If the inner and outer tuples are segmented by the same key we don't need motions.
                    if let Distribution::Segment {
                        keys: ref keys_inner,
                    } = inner_dist
                    {
                        if keys_outer.intersection(keys_inner).iter().next().is_some() {
                            return Ok(MotionPolicy::None);
                        }
                    }
                    // Redistribute the inner tuples using the first key from the outer tuple.
                    return keys_outer.iter().next().map_or_else(
                        || {
                            Err(SbroadError::UnexpectedNumberOfValues(SmolStr::from(
                                "Failed to get the first distribution key from the outer row.",
                            )))
                        },
                        |key| Ok(MotionPolicy::Segment(key.into())),
                    );
                }
                Distribution::Global => {
                    if let Distribution::Segment { .. } | Distribution::Single = inner_dist {
                        return Ok(MotionPolicy::None);
                    }
                }
                _ => {}
            }
        }
        Ok(MotionPolicy::Full)
    }

    /// Create Motions from the strategy map.
    /// Note: If we call this function, every child of strategy `parent_id` must be presented in the map.
    ///       For such a case we have `MotionPolicy::None` that won't create additional Motion nodes.
    fn create_motion_nodes(&mut self, mut strategy: Strategy) -> Result<(), SbroadError> {
        let parent_id = strategy.parent_id;
        let children = self.get_relational_children(parent_id)?;
        if children.is_empty() {
            // E.g., case of ValuesRow without SubQueries children.
            return Ok(());
        }

        // Check that all children we need to add motions exist in the current relational node.
        let children_set: HashSet<NodeId> = children.iter().copied().collect();
        if !strategy
            .children_policy
            .iter()
            .all(|(node, _)| children_set.contains(node))
        {
            panic!("Trying to add motions for non-existing children in relational node");
        }

        // Add motions.

        // Children nodes covered with Motions (if needed from `strategy`).
        let mut children_with_motions: Vec<NodeId> = Vec::new();
        let children_owned = children.to_vec();
        for child in children_owned {
            if let Some((policy, ref mut program)) = strategy.children_policy.get_mut(&child) {
                let program = std::mem::take(program);
                if let MotionPolicy::None = policy {
                    children_with_motions.push(child);
                } else {
                    children_with_motions.push(self.add_motion(child, policy, program)?);
                }
            } else {
                children_with_motions.push(child);
            }
        }
        self.set_relational_children(parent_id, children_with_motions);
        Ok(())
    }

    /// Get `Relational::SubQuery` node that is referenced by passed `row_id`.
    /// Only returns `SubQuery` that is an additional child of passed `rel_id` node.
    fn get_additional_sq(
        &self,
        rel_id: NodeId,
        row_id: NodeId,
    ) -> Result<Option<NodeId>, SbroadError> {
        if self.get_expression_node(row_id)?.is_row() {
            if let Some(sq_id) = self.get_sub_query_from_row_node(row_id)? {
                if self.is_additional_child_of_rel(rel_id, sq_id)? {
                    return Ok(Some(sq_id));
                }
            }
        }
        Ok(None)
    }

    /// Get `SubQuery`s from passed boolean `op_id` node (e.g. `In`).
    fn get_sq_node_strategies_for_bool_op(
        &self,
        rel_id: NodeId,
        op_id: NodeId,
    ) -> Result<Vec<(NodeId, MotionPolicy)>, SbroadError> {
        let mut strategies: Vec<(NodeId, MotionPolicy)> = Vec::new();
        let bool_op = BoolOp::from_expr(self, op_id)?;
        let left = self.get_additional_sq(rel_id, bool_op.left)?;
        let right = self.get_additional_sq(rel_id, bool_op.right)?;

        // If we eq/in where both rows contain bucket_id in same position
        // we don't need Motion nodes.
        if (left.is_some() || right.is_some())
            && self.has_eq_on_bucket_id(bool_op.left, bool_op.right, rel_id, &bool_op.op)?
        {
            if let Some(left_sq) = left {
                strategies.push((left_sq, MotionPolicy::None));
            }
            if let Some(right_sq) = right {
                strategies.push((right_sq, MotionPolicy::None));
            }
            return Ok(strategies);
        }

        match left {
            Some(left_sq) => {
                match right {
                    Some(right_sq) => {
                        // Both sides are sub-queries and require a full copy.
                        if !matches!(self.get_distribution(bool_op.left)?, Distribution::Global) {
                            strategies.push((left_sq, MotionPolicy::Full));
                        }
                        if !matches!(self.get_distribution(bool_op.right)?, Distribution::Global) {
                            strategies.push((right_sq, MotionPolicy::Full));
                        }
                    }
                    None => {
                        // Left side is sub-query, right is an outer tuple.
                        strategies.push((
                            left_sq,
                            self.choose_strategy_for_bool_op_inner_sq(
                                bool_op.right,
                                self.get_relational_output(left_sq)?,
                                &bool_op.op,
                            )?,
                        ));
                    }
                }
            }
            None => {
                if let Some(right_sq) = right {
                    // Left side is an outer tuple, right is sub-query.
                    strategies.push((
                        right_sq,
                        self.choose_strategy_for_bool_op_inner_sq(
                            bool_op.left,
                            self.get_relational_output(right_sq)?,
                            &bool_op.op,
                        )?,
                    ));
                }
            }
        }
        Ok(strategies)
    }

    /// Get `SubQuery`s from passed unary `op_id` node (e.g. `Exists`).
    fn get_sq_node_strategy_for_unary_op(
        &self,
        rel_id: NodeId,
        op_id: NodeId,
    ) -> Result<Option<(NodeId, MotionPolicy)>, SbroadError> {
        let unary_op_expr = self.get_expression_node(op_id)?;
        let Expression::Unary(UnaryExpr { child, op }) = unary_op_expr else {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format_smolstr!(
                    "Expected Unary expression, got {unary_op_expr:?}"
                )),
            ));
        };

        if let Unary::Exists = op {
            let child_sq = self.get_additional_sq(rel_id, *child)?;
            if let Some(child_sq) = child_sq {
                if let Distribution::Global = self.get_rel_distribution(child_sq)? {
                    return Ok(Some((child_sq, MotionPolicy::None)));
                }
                return Ok(Some((child_sq, MotionPolicy::Full)));
            }
        }

        Ok(None)
    }

    /// Calculate Motion strategies for `SubQueries`.
    /// For the special cases of subqueries under IN/EXISTS operators, apply special logic.
    /// For other cases cover subqueries with Full motion.
    fn resolve_sub_query_conflicts(
        &mut self,
        rel_parent_id: NodeId,
        expr_id: NodeId,
    ) -> Result<Strategy, SbroadError> {
        let mut strategy = Strategy::new(rel_parent_id);

        let not_nodes = self.get_not_unary_nodes(expr_id);
        let mut not_nodes_children = HashSet::with_capacity(not_nodes.len());
        for level_node in &not_nodes {
            let not_node_id = level_node.1;
            let not_node = self.get_expression_node(not_node_id)?;
            if let Expression::Unary(UnaryExpr { child, .. }) = not_node {
                not_nodes_children.insert(*child);
            } else {
                panic!("Expected Not operator, got {not_node:?}");
            }
        }

        let bool_nodes = self.get_bool_nodes_with_row_children(expr_id);
        for LevelNode(_, bool_node) in &bool_nodes {
            let bool_op = BoolOp::from_expr(self, *bool_node)?;
            self.set_distribution(bool_op.left)?;
            self.set_distribution(bool_op.right)?;
        }

        for LevelNode(_, bool_node) in &bool_nodes {
            let strategies = self.get_sq_node_strategies_for_bool_op(rel_parent_id, *bool_node)?;
            for (id, policy) in strategies {
                // In case NOT operator is covering expression with subquery like in
                // `not (... in ...)` expression, we have to change motion policy to Full.
                if not_nodes_children.contains(bool_node) {
                    strategy.add_child(id, MotionPolicy::Full, Program::default());
                } else {
                    strategy.add_child(id, policy, Program::default());
                }
            }
        }

        let unary_nodes = self.get_unary_nodes_with_row_children(expr_id);
        for LevelNode(_, unary_node) in &unary_nodes {
            let unary_strategy =
                self.get_sq_node_strategy_for_unary_op(rel_parent_id, *unary_node)?;
            if let Some((id, policy)) = unary_strategy {
                strategy.add_child(id, policy, Program::default());
            }
        }

        let rel = self.get_relation_node(rel_parent_id)?;
        if !rel.children().is_empty() {
            if let Distribution::Global =
                self.get_rel_distribution(self.get_relational_child(rel_parent_id, 0)?)?
            {
                self.fix_sq_strategy_for_global_tbl(rel_parent_id, expr_id, &mut strategy)?;
            }
        }

        Ok(strategy)
    }

    /// Get the children of a join node (outer, inner and sub-queries).
    ///
    /// # Errors
    /// - If the node is not a join node.
    /// - Join node has no children.
    fn get_join_children(&self, join_id: NodeId) -> Result<Children<'_>, SbroadError> {
        if let Ok(children) = self.get_relational_children(join_id) {
            Ok(children)
        } else {
            Err(SbroadError::Invalid(
                Entity::Relational,
                Some("Join node is not an inner join.".into()),
            ))
        }
    }

    /// Detect join child from the position map corresponding to the distribution key.
    fn get_join_child_by_key(
        &self,
        key: &Key,
        row_map: &HashMap<usize, NodeId>,
        join_children: &Children<'_>,
    ) -> Result<NodeId, SbroadError> {
        let mut children_set: HashSet<NodeId> = HashSet::new();
        for pos in &key.positions {
            let column_id = *row_map.get(pos).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Column,
                    format_smolstr!("{pos} in row map {row_map:?}"),
                )
            })?;
            if let Expression::Reference(Reference { targets, .. }) =
                self.get_expression_node(column_id)?
            {
                if let Some(targets) = targets {
                    for target in targets {
                        let child_id = *join_children.get(*target).ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::Target,
                                format_smolstr!("{target} in join children {join_children:?}"),
                            )
                        })?;
                        children_set.insert(child_id);
                    }
                }
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some("Row column is not a reference.".into()),
                ));
            }
        }
        if children_set.len() > 1 {
            return Err(SbroadError::UnexpectedNumberOfValues(
                "Distribution key in the join condition has more than one child.".into(),
            ));
        }
        children_set.iter().next().copied().ok_or_else(|| {
            SbroadError::UnexpectedNumberOfValues(
                "Distribution key in the join condition has no children.".into(),
            )
        })
    }

    /// Builds a row column map, where every column in the row is mapped by its position.
    ///
    /// # Errors
    /// - If the node is not a row node.
    fn build_row_map(&self, row_id: NodeId) -> Result<HashMap<usize, NodeId>, SbroadError> {
        let columns = self.get_row_list(row_id)?;
        let mut map: HashMap<usize, NodeId> = HashMap::new();
        for (pos, col) in columns.iter().enumerate() {
            map.insert(pos, *col);
        }
        Ok(map)
    }

    /// Split the distribution keys from the row of the join
    /// condition into inner and outer keys.
    /// Returns a tuple of (outer keys, inner keys).
    ///
    /// # Errors
    /// - Join node does not have any children.
    /// - Distribution keys do not refer to inner or outer children of the join.
    fn split_join_keys_to_inner_and_outer(
        &self,
        join_id: NodeId,
        keys: &[Key],
        row_map: &HashMap<usize, NodeId>,
    ) -> Result<(Vec<Key>, Vec<Key>), SbroadError> {
        let mut outer_keys: Vec<Key> = Vec::new();
        let mut inner_keys: Vec<Key> = Vec::new();

        let children = self.get_join_children(join_id)?;
        let outer_child = *children.get(0).ok_or_else(|| {
            SbroadError::UnexpectedNumberOfValues("Join node has no children.".into())
        })?;
        let inner_child = *children.get(1).ok_or_else(|| {
            SbroadError::NotFound(Entity::Node, "that is Join node inner child".into())
        })?;

        for key in keys {
            let child = self.get_join_child_by_key(key, row_map, &children)?;
            if child == outer_child {
                outer_keys.push(key.clone());
            } else if child == inner_child {
                inner_keys.push(key.clone());
            } else {
                // It can be only a sub-query, but we have already processed it.
                return Err(SbroadError::Invalid(
                    Entity::DistributionKey,
                    Some(
                        "distribution key doesn't correspond to inner or outer join children."
                            .into(),
                    ),
                ));
            }
        }
        Ok((outer_keys, inner_keys))
    }

    /// The join condition row can contain arbitrary expressions inside (constants,
    /// arithmetic operations, references to inner and outer children, etc.).
    /// This function extracts only the positions of the references to the inner child.
    fn get_inner_positions_from_condition_row(
        &self,
        row_map: &HashMap<usize, NodeId>,
    ) -> Result<AHashSet<usize>, SbroadError> {
        let mut inner_positions: AHashSet<usize> = AHashSet::with_capacity(row_map.len());
        for (pos, col) in row_map {
            let expression = self.get_expression_node(*col)?;
            if let Expression::Reference(Reference {
                targets: Some(targets),
                ..
            }) = expression
            {
                // Inner child of the join node is always the second one.
                if targets == &[1; 1] {
                    inner_positions.insert(*pos);
                }
            }
        }
        Ok(inner_positions)
    }

    /// Take the positions of the columns in the join condition row
    /// and return the positions of the columns in the inner child row.
    fn get_referred_inner_child_column_positions(
        &self,
        column_positions: &[usize],
        condition_row_map: &HashMap<usize, NodeId>,
    ) -> Result<Vec<usize>, SbroadError> {
        let mut referred_column_positions: Vec<usize> = Vec::with_capacity(column_positions.len());
        for pos in column_positions {
            let column_id = *condition_row_map.get(pos).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Column,
                    format_smolstr!("{pos} in row map {condition_row_map:?}"),
                )
            })?;
            if let Expression::Reference(Reference {
                targets, position, ..
            }) = self.get_expression_node(column_id)?
            {
                if let Some(targets) = targets {
                    // Inner child of the join node is always the second one.
                    if targets == &[1; 1] {
                        referred_column_positions.push(*position);
                    }
                }
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some("Row column is not a reference.".into()),
                ));
            }
        }
        Ok(referred_column_positions)
    }

    /// This is a helper function used to determine the policy of the inner child motion node in the join.
    /// For this purpose we inspect the join equality condition expression. It has the left and the right
    /// sides, both of which can either point to the inner and outer children of the join.
    /// If one of the sides has a `Segment` distribution and its keys point to the outer join child, then
    /// we try to calculate the possible motion policy of the inner child (it can be `Segment` or `Full`).
    /// To deal with it we also need the inner row map that maps the positions of the columns in the condition
    /// row to the `Reference` ids in that row. These references point to the inner child row.
    fn get_inner_policy_by_outer_segment(
        &self,
        outer_keys: &[Key],
        inner_row_map: &HashMap<usize, NodeId>,
    ) -> Result<MotionPolicy, SbroadError> {
        let inner_position_map = self.get_inner_positions_from_condition_row(inner_row_map)?;
        for outer_key in outer_keys {
            if outer_key
                .positions
                .iter()
                .all(|pos| inner_position_map.contains(pos))
            {
                let mut matched_inner_positions: Vec<usize> =
                    Vec::with_capacity(outer_key.positions.len());
                for pos in &outer_key.positions {
                    let pos = match inner_position_map.get(pos) {
                        Some(pos) => *pos,
                        None => continue,
                    };
                    matched_inner_positions.push(pos);
                }
                let inner_child_positions = self.get_referred_inner_child_column_positions(
                    &matched_inner_positions,
                    inner_row_map,
                )?;
                let inner_key = Key::new(inner_child_positions);
                return Ok(MotionPolicy::Segment(inner_key.into()));
            }
        }
        Ok(MotionPolicy::Full)
    }

    /// Derive the motion policy in the boolean node with equality operator.
    ///
    /// # Errors
    /// - Left or right row IDs are invalid.
    /// - Failed to get row distribution.
    /// - Failed to split distribution keys in the row to inner and outer keys.
    fn join_policy_for_eq(
        &self,
        join_id: NodeId,
        left_row_id: NodeId,
        right_row_id: NodeId,
    ) -> Result<MotionPolicy, SbroadError> {
        if self.has_eq_on_bucket_id(left_row_id, right_row_id, join_id, &Bool::Eq)? {
            return Ok(MotionPolicy::None);
        }

        let left_dist = self.get_distribution(left_row_id)?;
        let right_dist = self.get_distribution(right_row_id)?;
        let row_map_left = self.build_row_map(left_row_id)?;
        let row_map_right = self.build_row_map(right_row_id)?;

        match (left_dist, right_dist) {
            (
                Distribution::Segment {
                    keys: keys_left_set,
                },
                Distribution::Segment {
                    keys: keys_right_set,
                },
            ) => {
                let keys_left = keys_left_set.iter().map(Clone::clone).collect::<Vec<_>>();
                let keys_right = keys_right_set.iter().map(Clone::clone).collect::<Vec<_>>();
                let (left_outer_keys, left_inner_keys) =
                    self.split_join_keys_to_inner_and_outer(join_id, &keys_left, &row_map_left)?;
                let (right_outer_keys, right_inner_keys) =
                    self.split_join_keys_to_inner_and_outer(join_id, &keys_right, &row_map_right)?;

                // Can we join locally?
                let pairs = &[
                    (&left_outer_keys, &right_inner_keys),
                    (&right_outer_keys, &left_inner_keys),
                ];
                // We don't care about O(n^2) here, because the number of keys is usually small.
                for (outer_keys, inner_keys) in pairs {
                    for outer_key in *outer_keys {
                        for inner_key in *inner_keys {
                            if outer_key == inner_key {
                                return Ok(MotionPolicy::None);
                            }
                        }
                    }
                }
                // Example:
                //
                // t1 (a, b, c): key(c, b)
                // t2 (d, e): key(d)
                // select * from t1 join t2 on (t1.b, t1.c) = (t2.d, t2.e);
                // (t1.b, t1.c): segment[1, 0]
                // (t2.d, t2.e): segment[0]
                //
                // We take the first left outer key - (t1.b, t1.c) with positions 1 and 0 - and
                // check if on the right side of equality operator we have references to the inner
                // child at these positions. In our case, we have t2.e at positions 1 and t2.d at
                // 0 that refers to the inner child t2. So, we should reshard the inner child t2 by
                // (t2.e, t2.d). The motion node would have a distribution segment[0, 1] as t2.e is
                // at position 0 and t2.d is at position 1 in the inner child t2.

                if let MotionPolicy::Segment(inner_key) =
                    self.get_inner_policy_by_outer_segment(&left_outer_keys, &row_map_right)?
                {
                    return Ok(MotionPolicy::Segment(inner_key));
                }
                self.get_inner_policy_by_outer_segment(&right_outer_keys, &row_map_left)
            }
            (Distribution::Segment { keys: keys_set }, _) => {
                let keys_left = keys_set.iter().map(Clone::clone).collect::<Vec<_>>();
                let (left_outer_keys, _) =
                    self.split_join_keys_to_inner_and_outer(join_id, &keys_left, &row_map_left)?;
                self.get_inner_policy_by_outer_segment(&left_outer_keys, &row_map_right)
            }
            (_, Distribution::Segment { keys: keys_set }) => {
                let keys_right = keys_set.iter().map(Clone::clone).collect::<Vec<_>>();
                let (right_outer_keys, _) =
                    self.split_join_keys_to_inner_and_outer(join_id, &keys_right, &row_map_right)?;
                self.get_inner_policy_by_outer_segment(&right_outer_keys, &row_map_left)
            }
            _ => Ok(MotionPolicy::Full),
        }
    }

    fn set_rows_distributions_in_expr(&mut self, expr_id: NodeId) -> Result<(), SbroadError> {
        let nodes = self.get_bool_nodes_with_row_children(expr_id);
        for level_node in &nodes {
            let node = level_node.1;
            let bool_op = BoolOp::from_expr(self, node)?;
            self.set_distribution(bool_op.left)?;
            self.set_distribution(bool_op.right)?;
        }
        Ok(())
    }

    /// Derive the motion policy for the inner child and sub-queries in the join node.
    ///
    /// # Errors
    /// - Failed to set row distribution in the join condition tree.
    #[allow(clippy::too_many_lines)]
    fn resolve_join_conflicts(
        &mut self,
        rel_id: NodeId,
        cond_id: NodeId,
        join_kind: &JoinKind,
    ) -> Result<(), SbroadError> {
        // If one of the children has Distribution::Single, then we can't compute Distribution of
        // Rows in condition, because in case of Single it depends on join condition, and computing
        // distribution of Row in condition makes no sense, so we handle the single distribution separately
        if let Some(strategy) =
            self.calculate_strategy_for_single_distribution(rel_id, cond_id, join_kind)?
        {
            self.create_motion_nodes(strategy)?;
            self.set_rows_distributions_in_expr(cond_id)?;
            return Ok(());
        }

        // First, we need to set the motion policy for each boolean expression in the join condition.
        self.set_rows_distributions_in_expr(cond_id)?;

        if let Some(strategy) =
            self.calculate_strategy_for_left_join_with_global_tbl(rel_id, join_kind)?
        {
            self.create_motion_nodes(strategy)?;
            return Ok(());
        }

        // Init the strategy (motion policy map) for all the join children except the outer child.
        let join_children = self.get_join_children(rel_id)?;
        let mut strategy = Strategy::new(rel_id);
        for child_id in &join_children[1..] {
            if !matches!(self.get_rel_distribution(*child_id)?, Distribution::Global) {
                strategy.add_child(*child_id, MotionPolicy::Full, Program::default());
            }
        }

        // Let's improve the full motion policy for the join children (sub-queries and the inner child).
        let (inner_child, outer_child) = {
            let outer = *join_children.get(0).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Node,
                    "that is Join node inner child with index 0.".into(),
                )
            })?;
            let inner = *join_children.get(1).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Node,
                    "that is Join node inner child with index 1.".into(),
                )
            })?;
            (inner, outer)
        };

        let mut inner_map: HashMap<NodeId, MotionPolicy> = HashMap::new();
        let mut new_inner_policy = MotionPolicy::Full;
        let filter = |node_id: NodeId| -> bool {
            matches!(
                self.get_node(node_id),
                Ok(Node::Expression(
                    Expression::Bool(_) | Expression::Unary(UnaryExpr { op: Unary::Not, .. })
                ))
            )
        };
        let mut expr_tree = PostOrderWithFilter::with_capacity(
            |node| self.nodes.expr_iter(node, true),
            EXPR_CAPACITY,
            Box::new(filter),
        );
        expr_tree.populate_nodes(cond_id);
        let nodes = expr_tree.take_nodes();
        drop(expr_tree);
        for level_node in nodes {
            let node_id = level_node.1;
            let expr = self.get_expression_node(node_id)?;

            // Under `not ... in ...` we should change the policy to `Full`
            if let Expression::Unary(UnaryExpr {
                op: Unary::Not,
                child,
            }) = expr
            {
                let child_expr = self.get_expression_node(*child)?;
                if let Expression::Bool(BoolExpr { op: Bool::In, .. }) = child_expr {
                    new_inner_policy = MotionPolicy::Full;
                    inner_map.insert(node_id, new_inner_policy.clone());
                    continue;
                }
            }

            let Expression::Bool(BoolExpr { left, right, .. }) = expr else {
                continue;
            };
            let bool_op = BoolOp::from_expr(self, node_id)?;

            // Try to improve full motion policy in the sub-queries.
            // We don't influence the inner child here, so the inner map is empty
            // for the current node id.
            // `get_sq_node_strategies_for_bool_op` will be triggered only in case `node_id` is a
            // boolean operator with both `Row` children.
            // Note, that we don't have to call `get_sq_node_strategy_for_unary_op` here, because
            // the only strategy it can return is `Motion::Full` for its child and all subqueries
            // are covered with `Motion::Full` by default.

            let left_expr = self.get_expression_node(*left)?;
            let right_expr = self.get_expression_node(*right)?;
            if left_expr.is_row() && right_expr.is_row() {
                let sq_strategies = self.get_sq_node_strategies_for_bool_op(rel_id, node_id)?;
                let sq_strategies_len = sq_strategies.len();
                for (id, policy) in sq_strategies {
                    strategy.add_child(id, policy, Program::default());
                }
                if sq_strategies_len > 0 {
                    continue;
                }
            }

            // Ok, we don't have any sub-queries.
            // Lets try to improve the motion policy for the inner join child.
            let left_expr = self.get_expression_node(bool_op.left)?;
            let right_expr = self.get_expression_node(bool_op.right)?;
            new_inner_policy = match (&left_expr, &right_expr) {
                (Expression::Arithmetic(_), _) | (_, Expression::Arithmetic(_)) => {
                    MotionPolicy::Full
                }
                (Expression::Row { .. }, Expression::Row { .. }) => {
                    match bool_op.op {
                        Bool::Between => {
                            unreachable!("Between in redistribution")
                        }
                        Bool::Eq | Bool::In => {
                            self.join_policy_for_eq(rel_id, bool_op.left, bool_op.right)?
                        }
                        Bool::Gt | Bool::GtEq | Bool::Lt | Bool::LtEq | Bool::NotEq => {
                            MotionPolicy::Full
                        }
                        Bool::And | Bool::Or => {
                            // "a and 1" or "a or 1" expressions make no sense.
                            return Err(SbroadError::Unsupported(
                                Entity::Operator,
                                Some("unsupported boolean operation And or Or".into()),
                            ));
                        }
                    }
                }
                (
                    Expression::Bool(_) | Expression::Unary(_) | Expression::Row(_),
                    Expression::Bool(_) | Expression::Unary(_) | Expression::Row(_),
                ) => {
                    let left_policy = inner_map
                        .get(&bool_op.left)
                        .cloned()
                        .unwrap_or(MotionPolicy::Full);
                    let right_policy = inner_map
                        .get(&bool_op.right)
                        .cloned()
                        .unwrap_or(MotionPolicy::Full);
                    match bool_op.op {
                        Bool::And => join_policy_for_and(&left_policy, &right_policy)?,
                        Bool::Or => join_policy_for_or(&left_policy, &right_policy)?,
                        _ => {
                            return Err(SbroadError::Unsupported(
                                Entity::Operator,
                                Some("unsupported boolean operation, expected And or Or".into()),
                            ));
                        }
                    }
                }
                (Expression::Constant(_), Expression::Bool(_) | Expression::Unary(_)) => inner_map
                    .get(&bool_op.right)
                    .cloned()
                    .unwrap_or(MotionPolicy::Full),
                (Expression::Bool(_) | Expression::Unary(_), Expression::Constant(_)) => inner_map
                    .get(&bool_op.left)
                    .cloned()
                    .unwrap_or(MotionPolicy::Full),
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some(format_smolstr!("Unable to resolve join conflict for left ({left_expr:?}) and right ({right_expr:?}) expressions.")),
                    ));
                }
            };
            inner_map.insert(node_id, new_inner_policy.clone());
        }
        strategy.add_child(inner_child, new_inner_policy, Program::default());

        {
            let (outer_dist, inner_dist) = (
                self.get_rel_distribution(outer_child)?,
                self.get_rel_distribution(inner_child)?,
            );
            if matches!(
                (outer_dist, inner_dist),
                (Distribution::Global, _) | (_, Distribution::Global)
            ) {
                // If at least one child is global, the join can be done without any motions.
                strategy.add_child(inner_child, MotionPolicy::None, Program::default());
                strategy.add_child(outer_child, MotionPolicy::None, Program::default());
                self.fix_sq_strategy_for_global_tbl(rel_id, cond_id, &mut strategy)?;
            }
        }
        self.create_motion_nodes(strategy)?;
        Ok(())
    }

    /// In case there are more than one and-chains which
    /// (both) contain subquery with `Segment` or `Single`
    /// distribution (at least one subquery must be `Segment`),
    /// then default motions assigned in `choose_strategy_for_bool_op_inner_sq`
    /// function are wrong.
    ///
    /// By default, if subquery has `Segment` distribution, we assign
    /// `Motion(None)` to it in `choose_strategy_for_bool_op_inner_sq`.
    /// But in the case described below it can lead to wrong results:
    ///
    /// ```sql
    /// select a, b from global
    /// where a in (select b from segment_b) or b in (select c from segment_c)
    /// ```
    ///
    /// The data distribution is as follows:
    /// ```text
    /// global (a int, b decimal): [1, 100]
    /// segment_b (b int): [1]
    /// segment_c (c decimal): [100]
    ///
    /// node1:
    /// segment_b: [1]
    /// segment_c: []
    ///
    /// node2:
    /// segment_b: []
    /// segment_c: [100]
    /// ```
    ///
    /// So, if use `Motion(None)` for both subqueries, we will get `\[1, 100\]` twice in result
    /// table. So we need `Motion(Full)` for both subqueries.
    ///
    /// The same can be said when one subquery has distribution `Any` and the other one has
    /// distribution `Single`. Then we need to add `Motion(Full)` to `Any` subquery, and
    /// for `Single` no motion is needed.
    fn fix_sq_strategy_for_global_tbl(
        &self,
        rel_id: NodeId,
        expr_id: NodeId,
        strategy: &mut Strategy,
    ) -> Result<(), SbroadError> {
        let chains = self.get_dnf_chains(expr_id)?;
        let mut subqueries: Vec<NodeId> = vec![];
        let mut chain_count: usize = 0;
        for mut chain in chains {
            let nodes = chain.get_mut_nodes();
            let mut contains_sq = false;
            while let Some(node_id) = nodes.pop_back() {
                // Subqueries with `exists` are not handled here, because by default
                // if they read a global table, then they never need a motion.
                // Otherwise, if they read some sharded table (Segment or Any),
                // then they always have Motion, which was set before this function
                // was called in `get_sq_node_strategy_for_unary_op`.
                //
                // If some other operator is used (not Eq or In), then the corresponding subquery
                // already must have a Motion (in case it is reading non-global table),
                // see `choose_strategy_for_bool_op_inner_sq`.
                if let Expression::Bool(BoolExpr {
                    op: Bool::Eq | Bool::In,
                    left,
                    right,
                    ..
                }) = self.get_expression_node(node_id)?
                {
                    for child_id in [left, right] {
                        if let Some(sq_id) = self.get_additional_sq(rel_id, *child_id)? {
                            if let Distribution::Segment { .. } | Distribution::Single =
                                self.get_rel_distribution(sq_id)?
                            {
                                subqueries.push(sq_id);
                                contains_sq = true;
                            }
                        }
                    }
                }
            }
            chain_count += usize::from(contains_sq);
        }

        if chain_count > 1 {
            for sq_id in subqueries {
                if let Distribution::Segment { .. } = self.get_rel_distribution(sq_id)? {
                    strategy.add_child(sq_id, MotionPolicy::Full, Program::default());
                }
            }
        }
        Ok(())
    }

    /// Helper function to compute motion policies for outer and inner children of join
    /// when one child has Segment distribution, and the other one has Single distribution
    fn compute_policies_for_segment_vs_single(
        condition_eq_cols: Option<&EqualityCols>,
        keys: &KeySet,
        segmented_child: &JoinChild,
        join_kind: &JoinKind,
    ) -> (MotionPolicy, MotionPolicy) {
        let (mut outer_policy, mut inner_policy) = match segmented_child {
            JoinChild::Outer => (MotionPolicy::None, MotionPolicy::Full),
            JoinChild::Inner => (MotionPolicy::Full, MotionPolicy::None),
        };
        if let Some(eq_cols) = condition_eq_cols {
            for key in keys.iter() {
                if key.positions.len() == eq_cols.len()
                    && key.positions.iter().all(|pos| -> bool {
                        eq_cols.iter().any(|(i, o)| {
                            *pos == match segmented_child {
                                JoinChild::Outer => *o,
                                JoinChild::Inner => *i,
                            }
                        })
                    })
                {
                    let policy_to_update = match segmented_child {
                        JoinChild::Outer => &mut inner_policy,
                        JoinChild::Inner => &mut outer_policy,
                    };
                    *policy_to_update = MotionPolicy::Segment(MotionKey {
                        targets: eq_cols
                            .iter()
                            .map(|(i_col, o_col)| -> Target {
                                let pos = match segmented_child {
                                    JoinChild::Outer => *i_col,
                                    JoinChild::Inner => *o_col,
                                };
                                Target::Reference(pos)
                            })
                            .collect::<Vec<Target>>(),
                    });
                    break;
                }
            }
        } else if let JoinKind::LeftOuter = join_kind {
            // if we can't perform repartition join (no equality columns),
            // and left join is performed, we can't broadcast left (outer) table.
            // in this case we broadcast the inner table and rehash outer table
            outer_policy = MotionPolicy::Segment(MotionKey {
                // we can choose any distribution columns here
                targets: vec![Target::Reference(0)],
            });
            inner_policy = MotionPolicy::Full;
        }
        (outer_policy, inner_policy)
    }

    /// Create strategy if at least one of the join children
    /// has `Distribution::Single`.
    ///
    /// Distributed join can be done either by broadcasting
    /// or repartitioning. Broadcasting is joining two tables
    /// when one table is sharded across several nodes and the
    /// copy of the second table is sent (broadcasted) to each node.
    /// Repartitioning is when the second table is sent by parts (not
    /// whole table is transferred).
    ///
    /// ```text
    /// select * from o join i on o.a = i.b
    /// ```
    /// In this example repartition join can be done:
    /// we can segment one of the tables by equality column and send
    /// it to nodes where other table resides.
    ///
    /// Because repartitioning involves less data motion, we should
    /// use it over broadcasting. But repartition join is possible only
    /// if join condition is good enough. For most of the join conditions
    /// we (sbroad) can do only broadcast join:
    ///```text
    /// select * from o join i on o.a < i.b
    ///```
    ///
    /// This function attempts to find by which columns inner and outer
    /// join children should be distributed in order to do repartition
    /// join.
    ///
    /// Then it looks on distributions of inner and outer. And depending
    /// on distribution of both children creates a strategy map.
    #[allow(clippy::too_many_lines)]
    fn calculate_strategy_for_single_distribution(
        &mut self,
        join_id: NodeId,
        condition_id: NodeId,
        join_kind: &JoinKind,
    ) -> Result<Option<Strategy>, SbroadError> {
        let (outer_id, inner_id) = {
            let children = self.get_relational_children(join_id)?;
            (
                *children.get(0).ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "join {join_id:?} has no children!"
                    ))
                })?,
                *children.get(1).ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "join {join_id:?} has one child!"
                    ))
                })?,
            )
        };
        let outer_dist = self.get_distribution(self.get_relational_output(outer_id)?)?;
        let inner_dist = self.get_distribution(self.get_relational_output(inner_id)?)?;

        if !matches!(
            (outer_dist, inner_dist),
            (Distribution::Single, _) | (_, Distribution::Single)
        ) {
            return Ok(None);
        }

        let mut strategy = Strategy::new(join_id);

        for sq in &self.get_relational_children(join_id)?[2..] {
            // todo: improve subqueries motions
            strategy.add_child(*sq, MotionPolicy::Full, Program::default());
        }

        // If one child has Distribution::Global and the other child
        // Distribution::Single, then motion is not needed.
        // The fastest way is to execute the subtree on single node,
        // since we have a child that requires one node execution.
        if matches!(
            (outer_dist, inner_dist),
            (Distribution::Global, _) | (_, Distribution::Global)
        ) {
            strategy.add_child(outer_id, MotionPolicy::None, Program::default());
            strategy.add_child(inner_id, MotionPolicy::None, Program::default());
            return Ok(Some(strategy));
        }

        let eq_cols = EqualityCols::from_join_condition(self, join_id, inner_id, condition_id)?;
        let (outer_policy, inner_policy) = match (outer_dist, inner_dist) {
            (Distribution::Single, Distribution::Single) => {
                (MotionPolicy::None, MotionPolicy::None)
            }
            (Distribution::Single, Distribution::Segment { keys }) => {
                Self::compute_policies_for_segment_vs_single(
                    eq_cols.as_ref(),
                    keys,
                    &JoinChild::Inner,
                    join_kind,
                )
            }
            (Distribution::Segment { keys }, Distribution::Single) => {
                Self::compute_policies_for_segment_vs_single(
                    eq_cols.as_ref(),
                    keys,
                    &JoinChild::Outer,
                    join_kind,
                )
            }
            (Distribution::Any, Distribution::Single) => (MotionPolicy::None, MotionPolicy::Full),
            (Distribution::Single, Distribution::Any) => {
                if let JoinKind::LeftOuter = join_kind {
                    // outer table can't be safely broadcasted in case of LeftJoin see
                    // https://git.picodata.io/picodata/picodata/sbroad/-/issues/248
                    (
                        MotionPolicy::Segment(MotionKey {
                            // we can choose any distribution columns here
                            targets: vec![Target::Reference(0)],
                        }),
                        MotionPolicy::Full,
                    )
                } else {
                    (MotionPolicy::Full, MotionPolicy::None)
                }
            }
            // above we checked that at least one child has Distribution::Single
            (_, _) => return Err(SbroadError::Invalid(Entity::Distribution, None)),
        };
        strategy.add_child(outer_id, outer_policy, Program::default());
        strategy.add_child(inner_id, inner_policy, Program::default());

        Ok(Some(strategy))
    }

    #[allow(clippy::too_many_lines)]
    fn resolve_update_conflicts(&mut self, update_id: NodeId) -> Result<Strategy, SbroadError> {
        if self.dml_node_table(update_id)?.is_global() {
            return self.resolve_dml_node_conflict_for_global_table(update_id);
        }

        if let Relational::Update(Update { strategy: kind, .. }) =
            self.get_relation_node(update_id)?
        {
            let mut map = Strategy::new(update_id);
            let table = self.dml_node_table(update_id)?;
            let child_id = self.get_relational_child(update_id, 0)?;
            if !matches!(self.get_relation_node(child_id)?, Relational::Projection(_)) {
                return Err(SbroadError::Invalid(
                    Entity::Update,
                    Some(format_smolstr!(
                        "expected Projection under Update ({update_id:?})"
                    )),
                ));
            }
            match kind {
                UpdateStrategy::ShardedUpdate { .. } => {
                    let new_shard_cols_positions = {
                        let mut positions = table.get_sk()?.to_vec();
                        let bucket_id_pos = table
                            .get_bucket_id_position()?
                            .expect("wrong update strategy");
                        for pos in &mut positions {
                            if *pos > bucket_id_pos {
                                *pos -= 1;
                            }
                        }
                        positions
                    };
                    let op = MotionOpcode::RearrangeForShardedUpdate {
                        update_id,
                        old_shard_columns_len: table.get_sk()?.len(),
                        new_shard_columns_positions: new_shard_cols_positions,
                    };

                    // Check child distribution.

                    // Projection for sharded update always looks like this:
                    // `select new_tuple, old_shard_key from t`
                    // So the child must always have distribution of `old_shard_key`.

                    let child_output_id = self.get_relation_node(child_id)?.output();
                    let child_dist = self.get_distribution(child_output_id)?;
                    // Len of the new tuple, 1 is subtracted because new tuple does not
                    // contain bucket_id.
                    let projection_len = self.get_row_list(child_output_id)?.len();
                    let new_tuple_len = table.columns.len() - 1;
                    let old_shard_key_positions =
                        (new_tuple_len..projection_len).collect::<Vec<usize>>();
                    let expected_key = Key::new(old_shard_key_positions);
                    if let Distribution::Segment { keys } = child_dist {
                        if !keys.iter().any(|key| *key == expected_key) {
                            return Err(SbroadError::Invalid(
                                Entity::Update,
                                Some(format_smolstr!(
                                    "expected sharded update child to be \
                                 always distributed on old sharding key. Child dist: {child_dist:?}"
                                )),
                            ));
                        }
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Update,
                            Some("expected update child to have segment distribution".into()),
                        ));
                    }
                    let pk_op = MotionOpcode::PrimaryKey(table.primary_key.positions.clone());

                    map.add_child(
                        child_id,
                        MotionPolicy::Segment(MotionKey::new()),
                        Program(vec![pk_op, op]),
                    );
                }
                UpdateStrategy::LocalUpdate { .. } => {
                    // NB: currently when sharding column is not updated,
                    // the children below projection will always have the same distribution
                    // as update table, but this may change in the future: e.g. if join children
                    // will be reordered. The projection itself may have different distribution:
                    // Any/Segment, because it only contains needed update expressions and
                    // pk key columns.

                    // Check child below projection has update table distribution.
                    // projection child
                    let pr_child = self.get_relational_child(child_id, 0)?;
                    let pr_child_output_id = self.get_relational_output(pr_child)?;
                    let pr_child_dist = self.get_distribution(pr_child_output_id)?;

                    match pr_child_dist {
                        Distribution::Segment { keys, .. } => {
                            // Some nodes below projection (projection for Join) may
                            // remove bucket_id position, and shard_key.positions
                            // can't be used directly.
                            let expected_positions = {
                                let child_alias_map = ColumnPositionMap::new(self, pr_child)?;
                                let mut expected_positions =
                                    Vec::with_capacity(table.get_sk()?.len());
                                for pos in table.get_sk()? {
                                    let col_name = &table
                                        .columns
                                        .get(*pos)
                                        .ok_or_else(|| {
                                            SbroadError::Invalid(
                                                Entity::Table,
                                                Some(format_smolstr!(
                                                    "invalid shar key position: {pos}"
                                                )),
                                            )
                                        })?
                                        .name;
                                    let col_pos = child_alias_map.get(col_name.as_str())?;
                                    expected_positions.push(col_pos);
                                }
                                expected_positions
                            };
                            if !keys.iter().any(|key| key.positions == expected_positions) {
                                return Err(SbroadError::Invalid(
                                    Entity::Update,
                                    Some(format_smolstr!(
                                        "for local update expected children below \
                                  Projection to have update table dist. Got: {pr_child_dist:?}"
                                    )),
                                ));
                            }
                            map.add_child(child_id, MotionPolicy::Local, Program::default());
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::Update,
                                Some(format_smolstr!(
                                    "expected child below projection to have Segment dist,\
                             got: {pr_child_dist:?}"
                                )),
                            ))
                        }
                    }
                }
            }
            Ok(map)
        } else {
            Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("expected Update node on id {update_id:?}")),
            ))
        }
    }

    fn resolve_delete_conflicts(&mut self, rel_id: NodeId) -> Result<Strategy, SbroadError> {
        if self.dml_node_table(rel_id)?.is_global() {
            return self.resolve_dml_node_conflict_for_global_table(rel_id);
        }

        let mut map = Strategy::new(rel_id);

        let children = self.children(rel_id);
        if children.is_empty() {
            // We have a deal with DELETE without WHERE filter.
            // In such a case we don't materialize DELETE subtree
            // and we don't need a Motion node.
            // We will execute such a query via local `DELETE FROM T` sql
            // instead of Tarantool space api.
            return Ok(map);
        }
        let child_id = children[0];

        let table = self.dml_node_table(rel_id)?;
        let pk_len = table.primary_key.positions.len();
        if pk_len == 0 {
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "empty primary key for space {}",
                table.name()
            )));
        }
        // We expect that the columns in the child projection of the DELETE operator
        let pk_pos: Vec<usize> = (0..pk_len).collect();

        // Mark primary keys in the motion's virtual table.
        let program = vec![
            MotionOpcode::PrimaryKey(pk_pos),
            MotionOpcode::ReshardIfNeeded,
        ];

        // Delete node always produce a local segment policy
        // (i.e. materialization without bucket calculation).
        map.add_child(child_id, MotionPolicy::Local, Program(program));

        Ok(map)
    }

    fn resolve_dml_node_conflict_for_global_table(
        &mut self,
        rel_id: NodeId,
    ) -> Result<Strategy, SbroadError> {
        let mut map = Strategy::new(rel_id);
        let child_id = self.dml_child_id(rel_id)?;
        let child_node = self.get_relation_node(child_id)?;
        if !matches!(child_node, Relational::Motion(_)) {
            map.add_child(child_id, MotionPolicy::Full, Program::default());
        }
        Ok(map)
    }

    fn resolve_insert_conflicts(&mut self, rel_id: NodeId) -> Result<Strategy, SbroadError> {
        if self.dml_node_table(rel_id)?.is_global() {
            return self.resolve_dml_node_conflict_for_global_table(rel_id);
        }
        let mut map = Strategy::new(rel_id);
        let child_id = self.dml_child_id(rel_id)?;

        let motion_key = self.insert_motion_key(rel_id)?;
        let child_output_id = self.get_relation_node(child_id)?.output();
        let child_dist = self.get_distribution(child_output_id)?;

        // Check that we can make a local segment motion.
        if let Distribution::Segment { keys } = child_dist {
            for key in keys.iter() {
                let insert_mkey = MotionKey::from(key);
                if insert_mkey == motion_key {
                    map.add_child(
                        child_id,
                        MotionPolicy::LocalSegment(motion_key),
                        Program::default(),
                    );
                    return Ok(map);
                }
            }
        }

        map.add_child(
            child_id,
            MotionPolicy::Segment(motion_key),
            Program::default(),
        );

        Ok(map)
    }

    fn resolve_cte_conflicts(&mut self, cte_id: NodeId) -> Result<Strategy, SbroadError> {
        // We always gather CTE data on the router node.
        let mut map = Strategy::new(cte_id);
        let child_id = self.get_relational_child(cte_id, 0)?;

        // We need to check whether CTE subtree contains VALUES nodes. If so, we have to
        // materialize them on the router node, though they are globally distributed. The
        // reason is that we wrap VALUES with projection and generate anonymous column names
        // for them. And these names can be incorrect when we inline the CTE subtree into
        // the plan (as Tarantool has a very weird behavior with anonymous column names).
        let contains_values = self.subtree_contains_values(cte_id)?;

        let child_output_id = self.get_relation_node(child_id)?.output();
        let child_dist = self.get_distribution(child_output_id)?;
        match child_dist {
            Distribution::Global | Distribution::Single if !contains_values => {
                // The data is already on the router node, no need to build a virtual table.
                map.add_child(child_id, MotionPolicy::None, Program::default());
            }
            _ => {
                // Build a virtual table on the router node.
                // If the child node is already some motion (UNION DISTINCT, etc.),
                // no need to add another motion.
                if !self.get_relation_node(child_id)?.is_motion() {
                    map.add_child(child_id, MotionPolicy::Full, Program::default());
                }
            }
        }
        Ok(map)
    }

    /// Helper function to check whether except is done between
    /// sharded tables that both contain the `bucket_id` column
    /// at the same position in their outputs. In such case
    /// except can be done locally.
    ///
    /// Example:
    /// select `bucket_id` as a from t1
    /// except
    /// select `bucket_id` as b from t1
    fn is_except_on_bucket_id(
        &self,
        left_id: NodeId,
        right_id: NodeId,
    ) -> Result<bool, SbroadError> {
        let mut context = self.context_mut();
        let Some(left_shard_positions) =
            context.get_shard_columns_positions(left_id, self)?.copied()
        else {
            return Ok(false);
        };
        let Some(right_shard_positions) = context
            .get_shard_columns_positions(right_id, self)?
            .copied()
        else {
            return Ok(false);
        };
        for l in &left_shard_positions {
            if l.is_some() && right_shard_positions.contains(l) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[allow(clippy::too_many_lines)]
    fn resolve_except_conflicts(&mut self, rel_id: NodeId) -> Result<Strategy, SbroadError> {
        if !matches!(self.get_relation_node(rel_id)?, Relational::Except(_)) {
            return Err(SbroadError::Invalid(
                Entity::Relational,
                Some("expected Except node".into()),
            ));
        }

        let mut map = Strategy::new(rel_id);

        if self.resolve_except_global_vs_sharded(rel_id)? {
            return Ok(map);
        }

        let left_id = self.get_relational_child(rel_id, 0)?;
        let right_id = self.get_relational_child(rel_id, 1)?;
        let left_dist = self.get_rel_distribution(left_id)?;
        let right_dist = self.get_rel_distribution(right_id)?;

        if self.is_except_on_bucket_id(left_id, right_id)? {
            return Ok(map);
        }

        let (left_motion, right_motion) = match (left_dist, right_dist) {
            (
                Distribution::Segment { keys: left_keys },
                Distribution::Segment { keys: right_keys },
            ) => {
                if right_keys.intersection(left_keys).iter().next().is_some() {
                    // Distribution key sets have common keys, no need for the data motion.
                    (MotionPolicy::None, MotionPolicy::None)
                } else {
                    let key = left_keys.iter().next().ok_or_else(|| SbroadError::Invalid(
                        Entity::Distribution,
                        Some("left child's segment distribution is invalid: no keys found in the set".into()),
                    ))?;
                    (MotionPolicy::None, MotionPolicy::Segment(key.into()))
                }
            }
            (
                Distribution::Segment { .. }
                | Distribution::Any
                | Distribution::Global
                | Distribution::Single,
                Distribution::Global,
            ) => (MotionPolicy::None, MotionPolicy::None),
            (Distribution::Segment { keys }, _) => {
                let key = keys.iter().next().ok_or_else(|| SbroadError::Invalid(
                                Entity::Distribution,
                                Some("left child's segment distribution is invalid: no keys found in the set".into()),
                            ))?;
                (MotionPolicy::None, MotionPolicy::Segment(key.into()))
            }
            (Distribution::Single, Distribution::Single) => {
                // we could redistribute both children by any combination of columns,
                // first column is used for simplicity
                let policy = MotionPolicy::Segment(MotionKey {
                    targets: vec![Target::Reference(0)],
                });
                (policy.clone(), policy)
            }
            (Distribution::Single, Distribution::Segment { keys }) => {
                let key = keys.iter().next().ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Distribution,
                        Some(format_smolstr!(
                            "{} {} {right_id:?}",
                            "Segment distribution with no keys.",
                            "Except right child:"
                        )),
                    )
                })?;
                (MotionPolicy::Segment(key.into()), MotionPolicy::None)
            }
            (Distribution::Global, Distribution::Single) => {
                (MotionPolicy::None, MotionPolicy::None)
            }
            (_, _) => (MotionPolicy::None, MotionPolicy::Full),
        };

        map.add_child(left_id, left_motion, Program::default());
        map.add_child(right_id, right_motion, Program::default());

        Ok(map)
    }

    /// Resolves the case when left child has distribution Global
    /// and right child has Any or Segment distribution.
    /// If distributions are different returns `false`, otherwise modifies the
    /// plan inserting motions (and other nodes) and returns `true`.
    ///
    /// Currently, the except is executed in two stages:
    /// 1. Map stage: do intersect of the global child and sharded child
    /// 2. Reduce stage: do except with global child and results from Map stage
    ///
    /// For example:
    /// ```sql
    /// select a from g
    /// except
    /// select b from segment_a
    /// ```
    ///
    /// Before transformation:
    /// ```text
    /// Except
    ///     Projection a
    ///         scan g
    ///     Projection b
    ///         scan segment_a
    /// ```
    ///
    /// Transforms into:
    ///
    /// ```text
    /// Except
    ///     Projection a
    ///         scan g
    ///     Motion(Full)
    ///         Intersect
    ///             Projection b
    ///                 scan segment_a
    ///             Projection a
    ///                 scan g
    /// ```
    fn resolve_except_global_vs_sharded(&mut self, except_id: NodeId) -> Result<bool, SbroadError> {
        let left_id = self.get_relational_child(except_id, 0)?;
        let right_id = self.get_relational_child(except_id, 1)?;
        let left_dist = self.get_rel_distribution(left_id)?;
        let right_dist = self.get_rel_distribution(right_id)?;
        if !matches!(
            (left_dist, right_dist),
            (
                Distribution::Global,
                Distribution::Any | Distribution::Segment { .. }
            )
        ) {
            return Ok(false);
        }

        let cloned_left_id = SubtreeCloner::clone_subtree(self, left_id)?;
        let right_output_id = self.get_relational_output(right_id)?;
        let intersect_output_id = SubtreeCloner::clone_subtree(self, right_output_id)?;
        let intersect = Intersect {
            left: right_id,
            right: cloned_left_id,
            output: intersect_output_id,
        };
        let intersect_id = self.add_relational(intersect.into())?;

        self.change_child(except_id, right_id, intersect_id)?;

        let mut map = Strategy::new(except_id);
        map.add_child(intersect_id, MotionPolicy::Full, Program::default());
        self.create_motion_nodes(map)?;

        Ok(true)
    }

    fn resolve_union_conflicts(&mut self, rel_id: NodeId) -> Result<Strategy, SbroadError> {
        if !matches!(
            self.get_relation_node(rel_id)?,
            Relational::UnionAll(_) | Relational::Union(_)
        ) {
            return Err(SbroadError::Invalid(
                Entity::Relational,
                Some("expected union node".into()),
            ));
        }
        let mut map = Strategy::new(rel_id);
        let left_id = self.get_relational_child(rel_id, 0)?;
        let right_id = self.get_relational_child(rel_id, 1)?;
        let left_output_id = self.get_relation_node(left_id)?.output();
        let right_output_id = self.get_relation_node(right_id)?.output();

        {
            let left_output_row = self.get_row_list(left_output_id)?;
            let right_output_row = self.get_row_list(right_output_id)?;
            if left_output_row.len() != right_output_row.len() {
                return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                    "Except node children have different row lengths: left {}, right {}",
                    left_output_row.len(),
                    right_output_row.len()
                )));
            }
        }

        let left_dist = self.get_distribution(left_output_id)?;
        let right_dist = self.get_distribution(right_output_id)?;
        match (left_dist, right_dist) {
            (Distribution::Single, Distribution::Single) => {
                // todo: do not use a motion here
                map.add_child(
                    left_id,
                    MotionPolicy::Segment(MotionKey {
                        targets: vec![Target::Reference(0)],
                    }),
                    Program::default(),
                );
                map.add_child(
                    right_id,
                    MotionPolicy::Segment(MotionKey {
                        targets: vec![Target::Reference(0)],
                    }),
                    Program::default(),
                );
            }
            (Distribution::Single, _) => {
                map.add_child(
                    left_id,
                    MotionPolicy::Segment(MotionKey {
                        targets: vec![Target::Reference(0)],
                    }),
                    Program::default(),
                );
            }
            (_, Distribution::Single) => {
                map.add_child(
                    right_id,
                    MotionPolicy::Segment(MotionKey {
                        targets: vec![Target::Reference(0)],
                    }),
                    Program::default(),
                );
            }
            (Distribution::Global, Distribution::Segment { .. } | Distribution::Any) => {
                map.add_child(
                    left_id,
                    MotionPolicy::Local,
                    Program(vec![MotionOpcode::SerializeAsEmptyTable(true)]),
                );
            }
            (Distribution::Segment { .. } | Distribution::Any, Distribution::Global) => {
                map.add_child(
                    right_id,
                    MotionPolicy::Local,
                    Program(vec![MotionOpcode::SerializeAsEmptyTable(true)]),
                );
            }
            (_, _) => {}
        }
        Ok(map)
    }

    /// Set dist from subqueries or clone it from output.
    fn try_dist_from_subqueries(&mut self, id: NodeId, output: NodeId) -> Result<(), SbroadError> {
        if let Some(dist) = self.dist_from_subqueries(id)? {
            self.set_dist(output, dist)?;
        } else {
            self.set_distribution(output)?;
        }
        Ok(())
    }

    /// Cover `SubQueries` that wasn't matched as part of IN/EXISTS operators
    /// with motion nodes.
    ///
    /// `already_fixed` is a set of `SubQuery` ids which lies under IN/EXISTS operators and which
    /// weren't covered with Motion nodes. We should skip those nodes.
    /// Some of `SubQueries` under IN/EXISTS were covered with Motion nodes. Their ids are still in
    /// the `already_fixed`, but we identify them checking whether the node is covered with Motion.
    fn fix_additional_subqueries(
        &mut self,
        rel_id: NodeId,
        already_fixed: &AHashSet<NodeId>,
    ) -> Result<(), SbroadError> {
        let mut strategy = Strategy::new(rel_id);
        let rel_required_children_len = self
            .get_required_children_len(rel_id)?
            .unwrap_or_else(|| panic!("Unexpected node to get required children number."));
        let rel_children_len = self.get_relational_children(rel_id)?.len();
        for sq_index in rel_required_children_len..rel_children_len {
            let sq_id = self.get_relational_child(rel_id, sq_index)?;
            if already_fixed.contains(&sq_id) {
                continue;
            }
            let sq = self.get_relation_node(sq_id)?;
            if matches!(sq, Relational::Motion { .. }) {
                continue;
            }

            let sq_dist = self.get_rel_distribution(sq_id)?;

            // We can apply the same logic out of this function:
            // * We have a relational operator that has a required and possibly Distribution::Segment(some_key)
            // * Our sq may have the same required children and the same Distribution::Segment(some_key)
            // in such a case we won't have to apply any motions
            //
            // Otherwise the only thing we can do is to add Motion::Full to support subqueries.
            if !matches!(sq_dist, Distribution::Global) {
                // Query example showing that we always need to apply Motion::Full for subqueries:
                // Given: table t(a) with 2 rows:
                // * shard_1: { 1 }
                // * shard_2: { 2 }
                // `select (select a from t) from t`
                // It seems like instead of Motion::Full we could set Motion::Segment on column a for
                // subquery so we have to send less data. But in such a case we'd get the following scenario:
                // * shard_1: select a from t = 1
                // * shard_2: select a from t = 2
                // * total result = {1, 2} that seems right
                // but semantically we subquery execution should fail on each of the shards because
                // subquery returns more than 1 row.
                strategy.add_child(sq_id, MotionPolicy::Full, Program::default());
            }
        }
        self.create_motion_nodes(strategy)?;
        Ok(())
    }

    /// Add motion nodes to the plan tree.
    ///
    /// # Errors
    /// - failed to get relational nodes (plan is invalid?)
    /// - failed to resolve distribution conflicts
    /// - failed to set distribution
    pub fn add_motions(&mut self) -> Result<(), SbroadError> {
        type CteChildId = ChildId;
        type MotionId = ChildId;
        let mut cte_motions: AHashMap<CteChildId, MotionId> = AHashMap::with_capacity(CTE_CAPACITY);
        let top = self.get_top()?;
        let mut post_tree =
            PostOrder::with_capacity(|node| self.nodes.rel_iter(node), REL_CAPACITY);
        post_tree.populate_nodes(top);
        let nodes = post_tree.take_nodes();
        // Set of already visited nodes. Used for the case of BETWEEN where two expressions may
        // refer to the same relational node.
        let mut visited = AHashSet::with_capacity(nodes.len());
        // Map of { old relational child -> new relational child }
        // used to fix Union nodes.
        let mut old_new: AHashMap<NodeId, NodeId> = AHashMap::new();

        for LevelNode(_, id) in nodes {
            if visited.contains(&id) {
                continue;
            }

            // We clone it because immutable reference won't allow us to change plan later.
            let node = self.get_relation_node(id)?.get_rel_owned();

            // Some transformations (Union) need to add new nodes above
            // themselves, because we don't store parent references,
            // we update child reference when DFS reaches parent node.
            for child_id in &node.children() {
                if let Some(new_id) = old_new.get(child_id) {
                    self.change_child(id, *child_id, *new_id)?;
                    old_new.remove(child_id);
                }
            }

            match node {
                RelOwned::Motion { .. } => {
                    // We can apply this transformation only once,
                    // i.e. to the plan without any motion nodes.
                    panic!("IR mustn't contain Motion nodes at the stage of redistribution.")
                }
                RelOwned::Limit(Limit { output, limit, .. }) => {
                    let rel_child_id = self.get_relational_child(id, 0)?;
                    let child_dist =
                        self.get_distribution(self.get_relational_output(rel_child_id)?)?;

                    match child_dist {
                        Distribution::Single | Distribution::Global => {
                            // All rows on a single node, no motion needed.
                            self.set_dist(output, child_dist.clone())?;
                        }
                        Distribution::Any | Distribution::Segment { .. } => {
                            // Rows are distributed, so motion needed with full policy to
                            // bring them on a single node.
                            let child_dist = child_dist.clone();
                            // We don't need more than limit rows, so we can add a limit for the
                            // queries sent during the map stage.
                            let limit_id = self.add_limit(id, limit)?;
                            self.set_dist(
                                self.get_relational_output(limit_id)?,
                                Distribution::Single,
                            )?;
                            old_new.insert(id, limit_id);
                            let mut strategy = Strategy::new(limit_id);
                            strategy.add_child(id, MotionPolicy::Full, Program::default());
                            self.create_motion_nodes(strategy)?;
                            self.set_dist(output, child_dist)?;
                        }
                    }
                }
                RelOwned::ScanRelation(ScanRelation { output, .. })
                | RelOwned::ScanSubQuery(ScanSubQuery { output, .. })
                | RelOwned::Intersect(Intersect { output, .. })
                | RelOwned::Having(Having { output, .. }) => {
                    // Note: For `Having` true distribution is calculated
                    //       at the end of `add_two_stage_aggregation` function
                    //       after Map-Reduce transformation is applied.

                    // Leaf nodes that calculate distribution only out of their output
                    // (native or resulted from children).
                    self.set_distribution(output)?;
                }
                RelOwned::Values(Values { output, .. }) => {
                    self.set_dist(output, Distribution::Global)?;
                }
                RelOwned::GroupBy(GroupBy {
                    output, gr_cols, ..
                }) => {
                    // Previously there was no additional logic for creating Motions for local GroupBy?
                    let mut fixed_subquery_ids = AHashSet::new();
                    for gr_col in gr_cols {
                        let gr_col_strategy = self.resolve_sub_query_conflicts(id, gr_col)?;
                        fixed_subquery_ids.extend(gr_col_strategy.get_rel_ids());
                        self.create_motion_nodes(gr_col_strategy)?;
                    }
                    self.fix_additional_subqueries(id, &fixed_subquery_ids)?;

                    self.try_dist_from_subqueries(id, output)?;
                }
                RelOwned::Selection(Selection {
                    output,
                    filter: data,
                    ..
                })
                | RelOwned::ValuesRow(ValuesRow { output, data, .. }) => {
                    let strategy = self.resolve_sub_query_conflicts(id, data)?;
                    let fixed_subquery_ids = strategy.get_rel_ids();
                    self.create_motion_nodes(strategy)?;
                    self.fix_additional_subqueries(id, &fixed_subquery_ids)?;

                    self.try_dist_from_subqueries(id, output)?;
                }
                RelOwned::OrderBy(OrderBy {
                    output,
                    order_by_elements,
                    ..
                }) => {
                    let rel_child_id = self.get_relational_child(id, 0)?;

                    let mut fixed_subquery_ids = AHashSet::new();
                    for order_by_el in order_by_elements {
                        if let OrderByEntity::Expression { expr_id } = order_by_el.entity {
                            let strategy = self.resolve_sub_query_conflicts(id, expr_id)?;
                            fixed_subquery_ids.extend(strategy.get_rel_ids());
                            self.create_motion_nodes(strategy)?;
                        }
                    }
                    self.fix_additional_subqueries(id, &fixed_subquery_ids)?;

                    let child_dist =
                        self.get_distribution(self.get_relational_output(rel_child_id)?)?;
                    if !matches!(child_dist, Distribution::Single | Distribution::Global) {
                        // We must execute OrderBy on a single node containing all the rows
                        // that child relational node outputs.
                        // In case child node has distribution `Single` or `Global` we already have
                        // all the needed rows on one of the instances.
                        let mut strategy = Strategy::new(id);
                        strategy.add_child(rel_child_id, MotionPolicy::Full, Program::default());
                        self.create_motion_nodes(strategy)?;
                    }
                    self.set_dist(output, Distribution::Single)?;
                }
                RelOwned::Projection(Projection { output, .. }) => {
                    let strategy = self.resolve_sub_query_conflicts(id, output)?;
                    let fixed_subquery_ids = strategy.get_rel_ids();
                    self.create_motion_nodes(strategy)?;
                    self.fix_additional_subqueries(id, &fixed_subquery_ids)?;

                    let child_dist = self.get_distribution(
                        self.get_relational_output(self.get_relational_child(id, 0)?)?,
                    )?;
                    if matches!(child_dist, Distribution::Single | Distribution::Global) {
                        // Note on why we skip `add_two_stage_aggregation` call below in case of
                        // Single or Global distribution:
                        // If child has Single or Global distribution and this Projection
                        // contains aggregates or there is GroupBy,
                        // then we don't need two stage transformation,
                        // we can calculate aggregates / GroupBy in one
                        // stage, because all data will reside on a single node.
                        if let Some(dist) = self.dist_from_subqueries(id)? {
                            self.set_dist(output, dist)?;
                        } else {
                            self.set_dist(output, child_dist.clone())?;
                        }
                    } else if !self.add_two_stage_aggregation(id)? {
                        // if there are no aggregates or GroupBy, just take distribution
                        // from child.
                        self.set_projection_distribution(id)?;
                    }
                }
                RelOwned::SelectWithoutScan(SelectWithoutScan { output, .. }) => {
                    let strategy = self.resolve_sub_query_conflicts(id, output)?;
                    let fixed_subquery_ids = strategy.get_rel_ids();
                    self.create_motion_nodes(strategy)?;
                    self.fix_additional_subqueries(id, &fixed_subquery_ids)?;
                    self.set_dist(output, Distribution::Global)?;
                }
                RelOwned::Join(Join {
                    output,
                    condition,
                    kind,
                    ..
                }) => {
                    self.resolve_join_conflicts(id, condition, &kind)?;
                    if let Some(dist) = self.dist_from_subqueries(id)? {
                        self.set_dist(output, dist)?;
                    } else {
                        self.set_distribution(output)?;
                    }
                }
                RelOwned::Delete { .. } => {
                    let strategy = self.resolve_delete_conflicts(id)?;
                    self.create_motion_nodes(strategy)?;
                }
                RelOwned::Insert { .. } => {
                    // Insert output tuple already has relation's distribution.
                    let strategy = self.resolve_insert_conflicts(id)?;
                    self.create_motion_nodes(strategy)?;
                }
                RelOwned::Update(Update { output, .. }) => {
                    let strategy = self.resolve_update_conflicts(id)?;
                    self.create_motion_nodes(strategy)?;
                    self.set_distribution(output)?;
                }
                RelOwned::Except(Except { output, .. }) => {
                    let strategy = self.resolve_except_conflicts(id)?;
                    self.create_motion_nodes(strategy)?;
                    self.set_distribution(output)?;
                }
                RelOwned::Union(Union { output, .. }) => {
                    let strategy = self.resolve_union_conflicts(id)?;
                    self.create_motion_nodes(strategy)?;
                    self.set_distribution(output)?;
                    let new_top_id = self.add_motion(
                        id,
                        &MotionPolicy::Full,
                        Program::new(vec![MotionOpcode::RemoveDuplicates]),
                    )?;
                    old_new.insert(id, new_top_id);
                }
                RelOwned::UnionAll(UnionAll { output, .. }) => {
                    let strategy = self.resolve_union_conflicts(id)?;
                    self.create_motion_nodes(strategy)?;
                    self.set_distribution(output)?;
                }
                RelOwned::ScanCte(ScanCte { output, child, .. }) => {
                    // Possible, current CTE subtree has already been resolved and we
                    // can just copy the corresponding motion node.
                    if let Some(motion_id) = cte_motions.get(&child) {
                        self.set_relational_children(id, vec![*motion_id]);
                    } else {
                        let strategy = self.resolve_cte_conflicts(id)?;
                        self.create_motion_nodes(strategy)?;
                        let new_child_id = self.get_relational_child(id, 0)?;
                        let new_child_node = self.get_relation_node(new_child_id)?;
                        if let Relational::Motion { .. } = new_child_node {
                            cte_motions.insert(child, new_child_id);
                        }
                    }
                    // We don't materialize CTEs with global and single distribution.
                    // So, for global child let's preserve global distribution for CTE.
                    // Otherwise force a single distribution.
                    let child_id = self.get_relational_child(id, 0)?;
                    let child_dist =
                        self.get_distribution(self.get_relational_output(child_id)?)?;
                    if matches!(child_dist, Distribution::Global) {
                        self.set_dist(output, Distribution::Global)?;
                    } else {
                        self.set_dist(output, Distribution::Single)?;
                    }
                }
            }

            visited.insert(id);
        }

        if !old_new.is_empty() {
            assert_eq!(
                old_new.len(),
                1,
                "add_motions: old_new map has too many entries"
            );
            self.set_top(*old_new.values().next().unwrap())?;
        }

        let top_id = self.get_top()?;
        let slices = self.calculate_slices(top_id)?;
        self.set_slices(slices);
        Ok(())
    }

    /// Calculate slices for given subtree
    ///
    /// # Errors
    /// - failed to traverse plan
    pub fn calculate_slices(&self, top_id: NodeId) -> Result<Vec<Vec<NodeId>>, SbroadError> {
        // Let's compute for each node the maximum number
        // of motion nodes on the path from this node
        // to some leaf node. For motion node this number
        // is the slice index.
        let mut slices: Vec<Vec<NodeId>> = Vec::new();
        let mut dfs_tree = PostOrder::with_capacity(|node| self.nodes.rel_iter(node), REL_CAPACITY);
        // When visiting new node this stack stores
        // all the children of the current node.
        let mut stack = Vec::new();
        // Our plan is a DAG, we must not add the same motion twice
        // to slices.
        let mut visited_motions_ids = AHashSet::with_capacity(REL_CAPACITY);
        for LevelNode(_, id) in dfs_tree.iter(top_id) {
            let rel = self.get_relation_node(id)?;

            let mut max_motions_in_path = 0;
            for _ in 0..rel.children().len() {
                // We can use unwrap here, because:
                // 1. We traverse nodes in DFS, bottom up manner.
                // So we will first iterate over node's children
                // and add them to stack.
                // 2. DFS uses rel iter to iterate over node's children
                // and we know that rel iter iterates over ALL children.
                let child_mm = stack.pop().expect("rel iter visits all children");
                max_motions_in_path = max_motions_in_path.max(child_mm);
            }

            if rel.is_motion() {
                debug_assert!(max_motions_in_path <= slices.len());

                if visited_motions_ids.insert(id) {
                    if max_motions_in_path == slices.len() {
                        slices.push(Vec::new())
                    }
                    // We can unwrap here, because max_motions_in_path
                    // is always incremented by one and each time we
                    // add new slice if needed.
                    slices.get_mut(max_motions_in_path).unwrap().push(id);
                }

                max_motions_in_path += 1;
            }

            stack.push(max_motions_in_path);
        }
        Ok(slices)
    }
}

#[cfg(test)]
pub mod tests;
