//! Tuple distribution module.

use ahash::AHashMap;
use itertools::Itertools;
use smol_str::{format_smolstr, ToSmolStr};
use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::collection;
use crate::errors::{Action, Entity, SbroadError};
use crate::ir::helpers::RepeatableState;
use crate::ir::node::{
    Delete, Except, GroupBy, Having, Intersect, Join, Limit, Motion, NodeId, OrderBy, Projection,
    Reference, ReferenceTarget, Row, ScanCte, ScanRelation, ScanSubQuery, SelectWithoutScan,
    Selection, SubQueryReference, Union, UnionAll, Update,
};
use crate::ir::transformation::redistribution::{MotionKey, Target};

use super::node::expression::Expression;
use super::node::relational::Relational;
use super::relation::{Column, ColumnPositions};
use super::Plan;

/// Tuple columns that determinate its segment distribution.
///
/// Given:
/// * f -- distribution function.
/// * Table T1 contains columns (a, b, c) and distributed by columns (a, b).
///
/// Let's look at tuple (column row) with index i: (`a_i`, `b_i`, `c_i`).
/// Calling function f on (`a_i`, `b_i`) gives us segment `S_i`. Its a segment on which
/// this tuple will be located.
/// (a, b) is called a "segmentation key".
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
pub struct Key {
    /// A list of column positions in the tuple that form a
    /// segmentation key.
    pub positions: Vec<usize>,
}

impl Key {
    #[must_use]
    pub fn new(positions: Vec<usize>) -> Self {
        Key { positions }
    }

    pub(crate) fn with_columns(
        columns: &[Column],
        pos_map: &ColumnPositions,
        sharding_key: &[&str],
    ) -> Result<Self, SbroadError> {
        let shard_positions = sharding_key
            .iter()
            .map(|name| match pos_map.get(name) {
                Some(pos) => {
                    // Check that the column type is scalar.
                    // Compound types are not supported as sharding keys.
                    let column = &columns.get(pos).ok_or_else(|| {
                        SbroadError::FailedTo(
                            Action::Create,
                            Some(Entity::Column),
                            format_smolstr!("column {name} not found at position {pos}"),
                        )
                    })?;
                    if let Some(ty) = column.r#type.get() {
                        if !ty.is_scalar() {
                            return Err(SbroadError::Invalid(
                                Entity::Column,
                                Some(format_smolstr!(
                                    "column {name} at position {pos} is not scalar"
                                )),
                            ));
                        }
                    }
                    Ok(pos)
                }
                None => Err(SbroadError::Invalid(Entity::ShardingKey, None)),
            })
            .collect::<Result<Vec<usize>, _>>()?;
        Ok(Key::new(shard_positions))
    }
}

/// Set of `Key`s each of which represents the same segmentation.
/// After a join of several tables on the given key we may get several columns' sets that represent
/// the same distribution.
/// E.g. given 2 tables:
/// * t(a, b) distributed by a
/// * q(p, r) distributed by p
///   After their join (`t join q on a = p`) we'll get table tq(a, b, p, r) where
///   both Key((a)) and Key((p)) will represent the same segmentation.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct KeySet(HashSet<Key, RepeatableState>);

impl TryFrom<&MotionKey> for KeySet {
    type Error = SbroadError;

    fn try_from(value: &MotionKey) -> Result<Self, Self::Error> {
        let mut positions: Vec<usize> = Vec::with_capacity(value.targets.len());
        for t in &value.targets {
            match t {
                Target::Reference(pos) => positions.push(*pos),
                Target::Value(v) => {
                    return Err(SbroadError::FailedTo(
                        Action::Create,
                        Some(Entity::DistributionKey),
                        format_smolstr!("found value target in motion key: {v}"),
                    ));
                }
            }
        }
        let keys: HashSet<_, RepeatableState> = collection! { Key::new(positions) };
        Ok(keys.into())
    }
}

impl KeySet {
    pub(crate) fn empty() -> Self {
        KeySet(HashSet::with_hasher(RepeatableState))
    }

    pub(crate) fn insert(&mut self, key: Key) {
        self.0.insert(key);
    }

    pub fn iter(&self) -> impl Iterator<Item = &Key> {
        self.0.iter()
    }

    #[must_use]
    pub fn intersection(&self, other: &Self) -> Self {
        KeySet(self.0.intersection(&other.0).cloned().collect())
    }

    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        KeySet(self.0.union(&other.0).cloned().collect())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<HashSet<Key, RepeatableState>> for KeySet {
    fn from(keys: HashSet<Key, RepeatableState>) -> Self {
        Self(keys)
    }
}

/// Tuple distribution (location in cluster) in the cluster.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Distribution {
    /// The output of relational operator with this distribution
    /// can be located on several storages (maybe zero or one).
    /// Example: projection removes the segment key columns.
    Any,
    /// The output of relational operator with this distribution
    /// can be located on several storages (maybe zero or one).
    /// But if the data is present on the node, it is located
    /// as if it is a sharded by any of the keys in the keyset.
    ///
    /// Example: tuples from the segmented table.
    Segment {
        /// A set of distribution keys (we can have multiple keys after join).
        keys: KeySet,
    },
    /// A subtree with relational operator that has this distribution is guaranteed
    /// to be executed on a single node.
    Single,
    /// If subtree which top has `Global` distribution is executed on several nodes,
    /// then on each node output table will be exactly the same table.
    ///
    /// Example: scan of global tables, motion with policy full.
    Global,
}

impl Distribution {
    /// Calculate a new distribution for the `Union` or `UnionAll` output tuple.
    fn union(left: &Distribution, right: &Distribution) -> Distribution {
        match (left, right) {
            (
                Distribution::Global | Distribution::Any,
                Distribution::Any | Distribution::Segment { .. },
            )
            | (
                Distribution::Any | Distribution::Segment { .. },
                Distribution::Global | Distribution::Any,
            ) => Distribution::Any,
            (Distribution::Global, Distribution::Global) => Distribution::Global,
            (Distribution::Single, Distribution::Single)
            | (Distribution::Single, Distribution::Global)
            | (Distribution::Global, Distribution::Single) => Distribution::Single,
            (Distribution::Single, _) | (_, Distribution::Single) => {
                panic!("Union (all) child has unexpected distribution Single. Left: {left:?}, right: {right:?}.");
            }
            (
                Distribution::Segment {
                    keys: keys_left, ..
                },
                Distribution::Segment {
                    keys: keys_right, ..
                },
            ) => {
                let mut keys: HashSet<Key, RepeatableState> = HashSet::with_hasher(RepeatableState);
                for key in keys_left.intersection(keys_right).iter() {
                    keys.insert(Key::new(key.positions.clone()));
                }
                if keys.is_empty() {
                    Distribution::Any
                } else {
                    Distribution::Segment { keys: keys.into() }
                }
            }
        }
    }

    /// Calculate a new distribution for the `Except` output tuple.
    fn except(left: &Distribution, right: &Distribution) -> Distribution {
        match (left, right) {
            (Distribution::Global, _) => right.clone(),
            (_, Distribution::Global) => left.clone(),
            (Distribution::Single, _) | (_, Distribution::Single) => {
                panic!("Except child has unexpected distribution Single. Left: {left:?}, right: {right:?}");
            }
            (Distribution::Any, _) | (_, Distribution::Any) => Distribution::Any,
            (
                Distribution::Segment {
                    keys: keys_left, ..
                },
                Distribution::Segment {
                    keys: keys_right, ..
                },
            ) => {
                let mut keys: HashSet<Key, RepeatableState> = HashSet::with_hasher(RepeatableState);
                for key in keys_left.intersection(keys_right).iter() {
                    keys.insert(Key::new(key.positions.clone()));
                }
                if keys.is_empty() {
                    Distribution::Any
                } else {
                    Distribution::Segment { keys: keys.into() }
                }
            }
        }
    }

    /// Calculate a new distribution for the tuple combined from two different tuples.
    fn join(left: &Distribution, right: &Distribution) -> Distribution {
        match (left, right) {
            (Distribution::Any, Distribution::Any) => Distribution::Any,
            (Distribution::Single, Distribution::Global | Distribution::Single)
            | (Distribution::Global, Distribution::Single) => Distribution::Single,
            (Distribution::Single, _) | (_, Distribution::Single) => {
                panic!("Join child has unexpected distribution Single. Left: {left:?}, right: {right:?}");
            }
            (Distribution::Global, Distribution::Global) => {
                // This case is handled by `dist_from_subqueries`.
                Distribution::Global
            }
            (Distribution::Global, _) | (Distribution::Any, Distribution::Segment { .. }) => {
                right.clone()
            }
            (_, Distribution::Global) | (Distribution::Segment { .. }, Distribution::Any) => {
                left.clone()
            }
            (
                Distribution::Segment {
                    keys: ref keys_left,
                    ..
                },
                Distribution::Segment {
                    keys: ref keys_right,
                    ..
                },
            ) => {
                let mut keys: HashSet<Key, RepeatableState> = HashSet::with_hasher(RepeatableState);
                for key in keys_left.union(keys_right).iter() {
                    keys.insert(Key::new(key.positions.clone()));
                }
                if keys.is_empty() {
                    Distribution::Any
                } else {
                    Distribution::Segment { keys: keys.into() }
                }
            }
        }
    }
}

/// Nodes referred by relational operator output (ids of its children).
enum ReferredNodes {
    None,
    Single(NodeId),
    Pair(NodeId, NodeId),
    Multiple(Vec<NodeId>),
}

impl ReferredNodes {
    fn new() -> Self {
        ReferredNodes::None
    }

    fn append(&mut self, node: NodeId) {
        match self {
            ReferredNodes::None => *self = ReferredNodes::Single(node),
            ReferredNodes::Single(n) => {
                if *n != node {
                    *self = ReferredNodes::Pair(*n, node);
                }
            }
            ReferredNodes::Pair(n1, n2) => {
                if *n1 != node && *n2 != node {
                    *self = ReferredNodes::Multiple(vec![*n1, *n2, node]);
                }
            }
            ReferredNodes::Multiple(ref mut nodes) => {
                if !nodes.contains(&node) {
                    nodes.push(node);
                }
            }
        }
    }

    fn reserve(&mut self, capacity: usize) {
        if let ReferredNodes::Multiple(ref mut nodes) = self {
            nodes.reserve(capacity);
        }
    }
}

/// Helper structure to get the column position
/// in the child node.
#[derive(Debug, Eq, Hash, PartialEq)]
struct ChildColumnReference {
    /// Child node id.
    node_id: NodeId,
    /// Column position in the child node.
    column_position: usize,
}

type ParentColumnPosition = usize;

/// Set of the relational nodes referred by references under the row.
struct ReferenceInfo {
    referred_children: ReferredNodes,
    child_column_to_parent_col: AHashMap<ChildColumnReference, Vec<ParentColumnPosition>>,
}

impl ReferenceInfo {
    fn new(row_id: NodeId, ir: &Plan) -> Result<Self, SbroadError> {
        let mut ref_nodes = ReferredNodes::new();
        let mut ref_map: AHashMap<ChildColumnReference, Vec<ParentColumnPosition>> =
            AHashMap::new();
        let child: &[NodeId] = match ir.get_expression_node(row_id) {
            Ok(Expression::Row(Row { list, .. })) => list,
            Ok(Expression::Reference(..)) => std::array::from_ref(&row_id),
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some("node is not Row or Refence type".into()),
                ))
            }
        };
        for (parent_column_pos, id) in child.iter().enumerate() {
            let child_id = ir.get_child_under_alias(*id)?;
            let child_id = ir.get_child_under_cast(child_id)?;
            if let Expression::Reference(Reference {
                target, position, ..
            }) = ir.get_expression_node(child_id)?
            {
                // As the row is located in the branch relational node, the targets should be non-empty.
                let targets_len = target.len();
                if targets_len == 0 {
                    return Err(SbroadError::UnexpectedNumberOfValues(
                        "Reference targets are empty".to_smolstr(),
                    ));
                }

                ref_map.reserve(targets_len);
                ref_nodes.reserve(targets_len);
                for target_id in target.iter() {
                    ref_map
                        .entry((*target_id, *position).into())
                        .or_default()
                        .push(parent_column_pos);
                    ref_nodes.append(*target_id);
                }
            } else if let Expression::SubQueryReference(SubQueryReference {
                rel_id,
                position,
                ..
            }) = ir.get_expression_node(child_id)?
            {
                ref_map
                    .entry((*rel_id, *position).into())
                    .or_default()
                    .push(parent_column_pos);
                ref_nodes.append(*rel_id);
            }
        }

        Ok(ReferenceInfo {
            referred_children: ref_nodes,
            child_column_to_parent_col: ref_map,
        })
    }
}

impl Iterator for ReferredNodes {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ReferredNodes::None => None,
            ReferredNodes::Single(n) => {
                let node = *n;
                *self = ReferredNodes::None;
                Some(node)
            }
            ReferredNodes::Pair(n1, n2) => {
                let node = *n1;
                *self = ReferredNodes::Single(*n2);
                Some(node)
            }
            ReferredNodes::Multiple(ref mut nodes) => {
                let node = nodes.pop();
                if nodes.is_empty() {
                    *self = ReferredNodes::None;
                }
                node
            }
        }
    }
}

impl From<(NodeId, usize)> for ChildColumnReference {
    fn from((node_id, column_position): (NodeId, usize)) -> Self {
        ChildColumnReference {
            node_id,
            column_position,
        }
    }
}

impl Plan {
    /// Sets distribution for output tuple of projection.
    /// Applied in case two stage aggregation is not present.
    pub(crate) fn set_projection_distribution(
        &mut self,
        proj_id: NodeId,
    ) -> Result<(), SbroadError> {
        if !matches!(self.get_relation_node(proj_id)?, Relational::Projection(_)) {
            panic!("Expected projection on id: {proj_id}.")
        };

        let output_id = self.get_relational_output(proj_id)?;
        let child_id = self.get_first_rel_child(proj_id)?;
        let ref_info = ReferenceInfo::new(output_id, self)?;
        let child_dist = self.dist_from_child(child_id, &ref_info.child_column_to_parent_col)?;

        if let Distribution::Segment { .. } = child_dist {
            let mut only_compound_exprs = true;
            for id in self.get_row_list(output_id)? {
                let child_id = self.get_child_under_alias(*id)?;
                if let Expression::Reference(_) = self.get_expression_node(child_id)? {
                    only_compound_exprs = false;
                    break;
                }
            }
            if only_compound_exprs {
                // The projection looks like this: `select 1, a + b, 10 * b`
                // i.e no bare references like in `select a, b, c`
                self.set_rel_distr(proj_id, Distribution::Any)?;
                return Ok(());
            }
        }

        self.set_rel_distr(proj_id, child_dist)?;
        Ok(())
    }

    fn get_dist_from_scan_relation(&self, scan_id: NodeId) -> Result<Distribution, SbroadError> {
        // Working with a leaf node (ScanRelation).
        let tbl_name = self.get_scan_relation(scan_id)?;
        let tbl = self.get_relation_or_error(tbl_name)?;
        if tbl.is_global() {
            return Ok(Distribution::Global);
        }
        // The scan produces the table columns in the table order, so the
        // sharding key positions in the table are the output positions.
        let key = Key::new(tbl.get_sk()?.to_vec());
        let keys: HashSet<Key, RepeatableState> = collection! { key };
        Ok(Distribution::Segment { keys: keys.into() })
    }

    /// Calculate the distribution of the relational node from its output tuple
    /// (and from the node kind for scans, joins and set operations) and set it.
    ///
    /// # Errors
    /// - node is not relational
    /// - output contains broken references
    /// - distribution of some child is not calculated yet
    ///
    /// # Panics
    /// - reference has invalid targets
    pub fn set_rel_output_distribution(&mut self, rel_id: NodeId) -> Result<(), SbroadError> {
        let rel_node = self.get_relation_node(rel_id)?;

        let dist = match rel_node {
            Relational::ScanRelation(ScanRelation { .. }) => {
                self.get_dist_from_scan_relation(rel_id)?
            }
            Relational::Projection(Projection { output, .. })
            | Relational::SelectWithoutScan(SelectWithoutScan { output, .. }) => {
                self.distr_from_node(*output)?
            }
            Relational::Join(Join { left, right, .. }) => {
                // The columns of the left child are followed by the columns of the right one.
                let (left, right) = (*left, *right);
                let mut child_pos_map = AHashMap::new();
                self.map_child_columns(left, 0, &mut child_pos_map)?;
                let left_len = self.columns_len(left)?;
                self.map_child_columns(right, left_len, &mut child_pos_map)?;
                self.get_two_children_node_dist(&child_pos_map, left, right, rel_id)?
            }
            Relational::Union(Union { left, right, .. })
            | Relational::UnionAll(UnionAll { left, right, .. })
            | Relational::Except(Except { left, right, .. }) => {
                // Every column is taken from both children at the same position.
                let (left, right) = (*left, *right);
                let mut child_pos_map = AHashMap::new();
                self.map_child_columns(left, 0, &mut child_pos_map)?;
                self.map_child_columns(right, 0, &mut child_pos_map)?;
                self.get_two_children_node_dist(&child_pos_map, left, right, rel_id)?
            }
            Relational::Intersect(Intersect { right: child, .. })
            | Relational::Selection(Selection { child, .. })
            | Relational::Having(Having { child, .. })
            | Relational::GroupBy(GroupBy { child, .. })
            | Relational::OrderBy(OrderBy { child, .. })
            | Relational::Limit(Limit { child, .. })
            | Relational::ScanSubQuery(ScanSubQuery { child, .. })
            | Relational::ScanCte(ScanCte { child, .. })
            | Relational::Update(Update { child, .. })
            | Relational::Motion(Motion {
                child: Some(child), ..
            })
            | Relational::Delete(Delete {
                child: Some(child), ..
            }) => {
                // The node passes the columns of its child through.
                let child = *child;
                let mut child_pos_map = AHashMap::new();
                self.map_child_columns(child, 0, &mut child_pos_map)?;
                self.dist_from_child(child, &child_pos_map)?
            }
            Relational::Insert(_)
            | Relational::Values(_)
            | Relational::Motion(Motion { child: None, .. })
            | Relational::Delete(Delete { child: None, .. }) => {
                return Err(SbroadError::Invalid(
                    Entity::Relational,
                    Some(format_smolstr!(
                        "distribution of {} ({rel_id}) is not derived from its output",
                        rel_node.name()
                    )),
                ))
            }
        };

        self.set_rel_distr(rel_id, dist)
    }

    /// Records that every column of `child` appears in the parent's output at
    /// the same position shifted by `offset`.
    fn map_child_columns(
        &self,
        child: NodeId,
        offset: usize,
        child_pos_map: &mut AHashMap<ChildColumnReference, Vec<ParentColumnPosition>>,
    ) -> Result<(), SbroadError> {
        for pos in 0..self.columns_len(child)? {
            child_pos_map
                .entry((child, pos).into())
                .or_default()
                .push(pos + offset);
        }
        Ok(())
    }

    /// Each relational node have non-sq (required) and sq (additional) children.
    /// In case required children have `Distribution::Global` we can copy sq distribution
    /// as far as required children data is stored on each replicaset.
    ///
    /// In case all required children have Global distribution it improves
    /// Global distribution based on subqueries in case there are any (note that `Values` has
    /// not required children).
    /// Otherwise, it returns `None`.
    ///
    /// # Errors
    /// - node is not relational
    /// - incorrect number of children for node
    /// - missing Motion(Full) for sq with Any distribution
    pub(crate) fn dist_from_subqueries(
        &self,
        node_id: NodeId,
    ) -> Result<Option<Distribution>, SbroadError> {
        let node = self.get_relation_node(node_id)?;

        // Check all required children have Global distribution.
        for child_id in node.children().iter() {
            let child_dist = self.rel_distr_ref(*child_id)?;
            if !matches!(child_dist, Distribution::Global) {
                return Ok(None);
            }
        }

        let subqueries = self.get_relation_subqueries(node_id)?;

        let mut suggested_dist = Some(Distribution::Global);
        for sq_id in subqueries.iter() {
            let sq_dist = self.rel_distr_ref(*sq_id)?;
            match sq_dist {
                Distribution::Segment { .. } => {
                    suggested_dist = Some(Distribution::Any);
                }
                Distribution::Any => {
                    // Earlier when resolving conflicts for subqueries we must have
                    // inserted Motion(Full) for subquery with Any distribution.
                    panic!("Expected Motion(Full) for subquery child ({sq_id}).")
                }
                Distribution::Single | Distribution::Global => {
                    // TODO: In case we have a single sq can we improve Global to Single?
                }
            }
        }

        Ok(suggested_dist)
    }

    // Private methods

    fn distr_from_node(&self, node_id: NodeId) -> Result<Distribution, SbroadError> {
        let children_list: &[NodeId] = match self.get_expression_node(node_id) {
            Ok(Expression::Row(Row { list, .. })) => list,
            _ => std::array::from_ref(&node_id),
        };

        let mut reference_met = false;
        for id in children_list {
            let child_id = self.get_child_under_alias(*id)?;
            let child_id = self.get_child_under_cast(child_id)?;
            match self.get_expression_node(child_id)? {
                Expression::Reference(Reference { target, .. }) => {
                    if matches!(target, ReferenceTarget::Leaf) {
                        unreachable!(
                            "distribution with leaf targets should be handled in parent function"
                        );
                    }
                    reference_met = true;
                    break;
                }
                Expression::SubQueryReference(_) => {
                    reference_met = true;
                    break;
                }
                _ => continue,
            }
        }

        if !reference_met {
            // We haven't met any Reference in the output.
            return Ok(Distribution::Any);
        };

        // Working with all other nodes.
        let ref_info = ReferenceInfo::new(node_id, self)?;

        let dist = match ref_info.referred_children {
            ReferredNodes::None => {
                // Row contains reference that doesn't point to any relational node.
                panic!("Row reference doesn't point to relational node.");
            }
            ReferredNodes::Single(child_id) => {
                self.dist_from_child(child_id, &ref_info.child_column_to_parent_col)?
            }
            ReferredNodes::Pair(left_id, right_id) => {
                // Output tuples of Join, Union and Except are handled by
                // `set_rel_output_distribution`. Any other row referring to two
                // relational nodes is a tuple in a join condition (e.g.
                // `(t1.a, t2.b) in (select ...)`) built from both join children.
                let left_dist =
                    self.dist_from_child(left_id, &ref_info.child_column_to_parent_col)?;
                let right_dist =
                    self.dist_from_child(right_id, &ref_info.child_column_to_parent_col)?;
                Distribution::join(&left_dist, &right_dist)
            }
            ReferredNodes::Multiple(_) => {
                // Reference points to more than two relational children nodes,
                // that is impossible.
                panic!("Row contains multiple references to the same node (and it is not VALUES)");
            }
        };

        Ok(dist)
    }

    fn dist_from_child(
        &self,
        child_rel_node: NodeId,
        child_pos_map: &AHashMap<ChildColumnReference, Vec<ParentColumnPosition>>,
    ) -> Result<Distribution, SbroadError> {
        let child_dist = self.rel_distr_ref(child_rel_node)?;
        match child_dist {
            Distribution::Single => Ok(Distribution::Single),
            Distribution::Any => Ok(Distribution::Any),
            Distribution::Global => Ok(Distribution::Global),
            Distribution::Segment { keys } => {
                let mut new_keys: HashSet<Key, RepeatableState> =
                    HashSet::with_hasher(RepeatableState);
                for key in keys.iter() {
                    let all_found = key
                        .positions
                        .iter()
                        .all(|pos| child_pos_map.contains_key(&(child_rel_node, *pos).into()));

                    if all_found {
                        let product = key
                            .positions
                            .iter()
                            .map(|pos| {
                                child_pos_map
                                    .get(&(child_rel_node, *pos).into())
                                    .unwrap()
                                    .iter()
                                    .copied()
                            })
                            .multi_cartesian_product();

                        for positions in product {
                            new_keys.insert(Key::new(positions));
                        }
                    }
                }

                // Parent's operator output does not contain some
                // sharding columns. For example:
                // ```sql
                // select b from t
                // ```
                //
                // Where `t` is sharded by `a`.
                if new_keys.is_empty() {
                    return Ok(Distribution::Any);
                }
                Ok(Distribution::Segment {
                    keys: new_keys.into(),
                })
            }
        }
    }

    fn get_two_children_node_dist(
        &self,
        child_pos_map: &AHashMap<ChildColumnReference, Vec<ParentColumnPosition>>,
        left_id: NodeId,
        right_id: NodeId,
        parent_id: NodeId,
    ) -> Result<Distribution, SbroadError> {
        let left_dist = self.dist_from_child(left_id, child_pos_map)?;
        let right_dist = self.dist_from_child(right_id, child_pos_map)?;

        let parent = self.get_relation_node(parent_id)?;
        let new_dist = match parent {
            Relational::Except { .. } => Distribution::except(&left_dist, &right_dist),
            Relational::Union { .. } | Relational::UnionAll { .. } => {
                Distribution::union(&left_dist, &right_dist)
            }
            Relational::Join { .. } => Distribution::join(&left_dist, &right_dist),
            _ => {
                panic!("Expected Except, Union(All) or Join node");
            }
        };

        Ok(new_dist)
    }

    /// Calculates the distribution of an expression (a row or a reference in a
    /// filter or a join condition) from the relational nodes it refers to.
    ///
    /// The distribution of a relational node itself is stored in the node,
    /// see [`Plan::rel_distr_ref`].
    ///
    /// # Errors
    /// - expression doesn't exist or contains broken references
    /// - distribution of a referred relational node is not calculated yet
    pub fn get_distribution(&self, node_id: NodeId) -> Result<Distribution, SbroadError> {
        self.distr_from_node(node_id)
    }

    /// Gets the distribution of the relational node.
    ///
    /// # Errors
    /// - node is not relational
    /// - distribution is not calculated yet
    pub fn rel_distr_ref(&self, rel_id: NodeId) -> Result<&Distribution, SbroadError> {
        self.get_relation_node(rel_id)?
            .distribution()
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Distribution,
                    Some(format_smolstr!(
                        "distribution of the node {rel_id} is uninitialized"
                    )),
                )
            })
    }

    /// Sets the distribution of the relational node.
    ///
    /// # Errors
    /// - node is not relational
    pub fn set_rel_distr(&mut self, rel_id: NodeId, dist: Distribution) -> Result<(), SbroadError> {
        *self.get_mut_relation_node(rel_id)?.distribution_mut() = Some(Box::new(dist));
        Ok(())
    }
}
