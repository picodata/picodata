//! Tuple distribution module.

use ahash::{AHashMap, RandomState};
use itertools::Itertools;
use smol_str::{format_smolstr, ToSmolStr};
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::collection;
use crate::errors::{Action, Entity, SbroadError};
use crate::ir::helpers::RepeatableState;
use crate::ir::node::{NodeId, Reference, Row};
use crate::ir::transformation::redistribution::{MotionKey, Target};

use super::api::children::Children;
use super::node::expression::{Expression, MutExpression};
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
                    if !column.r#type.is_scalar() {
                        return Err(SbroadError::Invalid(
                            Entity::Column,
                            Some(format_smolstr!(
                                "column {name} at position {pos} is not scalar"
                            )),
                        ));
                    }
                    Ok(pos)
                }
                None => Err(SbroadError::Invalid(Entity::ShardingKey, None)),
            })
            .collect::<Result<Vec<usize>, _>>()?;
        Ok(Key::new(shard_positions))
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
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
    pub fn new(
        row_id: NodeId,
        ir: &Plan,
        parent_children: &Children<'_>,
    ) -> Result<Self, SbroadError> {
        let mut ref_nodes = ReferredNodes::new();
        let mut ref_map: AHashMap<ChildColumnReference, Vec<ParentColumnPosition>> =
            AHashMap::new();
        for (parent_column_pos, id) in ir.get_row_list(row_id)?.iter().enumerate() {
            let child_id = ir.get_child_under_alias(*id)?;
            if let Expression::Reference(Reference {
                targets, position, ..
            }) = ir.get_expression_node(child_id)?
            {
                // As the row is located in the branch relational node, the targets should be non-empty.
                let targets = targets.as_ref().ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(
                        "Reference targets are empty".to_smolstr(),
                    )
                })?;
                ref_map.reserve(targets.len());
                ref_nodes.reserve(targets.len());
                for target in targets {
                    let referred_id = parent_children.get(*target).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Expression,
                            "reference points to invalid column".to_smolstr(),
                        )
                    })?;
                    ref_nodes.append(*referred_id);
                    ref_map
                        .entry((*referred_id, *position).into())
                        .or_default()
                        .push(parent_column_pos);
                }
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
    pub fn set_projection_distribution(&mut self, proj_id: NodeId) -> Result<(), SbroadError> {
        if !matches!(
            self.get_relation_node(proj_id)?,
            Relational::Projection { .. }
        ) {
            panic!("Expected projection on id: {proj_id}.")
        };

        let output_id = self.get_relational_output(proj_id)?;
        let child_id = self.get_relational_child(proj_id, 0)?;

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
            self.set_dist(output_id, Distribution::Any)?;
            return Ok(());
        }

        // Projection has some bare references: `select a + b, b, c ..`
        // It may have Segment distribution, to check that we need a mapping
        // between referenced columns in child and column position in projection's
        // output.
        let children = self.get_relational_children(proj_id)?;
        let ref_info = ReferenceInfo::new(output_id, self, &children)?;
        let child_dist = self.dist_from_child(child_id, &ref_info.child_column_to_parent_col)?;
        self.set_dist(output_id, child_dist)?;

        Ok(())
    }

    /// Calculate and set tuple distribution.
    /// In comparison with `set_dist` it automatically
    /// derives distribution from children nodes.
    ///
    /// # Errors
    /// Returns `SbroadError` when current expression is not a `Row` or contains broken references.
    ///
    /// # Panics
    /// - reference has invalid targets
    #[allow(clippy::too_many_lines)]
    pub fn set_distribution(&mut self, row_id: NodeId) -> Result<(), SbroadError> {
        let row_children = self.get_row_list(row_id)?;

        let mut parent_node = None;
        for id in row_children {
            let child_id = self.get_child_under_alias(*id)?;
            if let Expression::Reference(Reference { parent, .. }) =
                self.get_expression_node(child_id)?
            {
                parent_node = *parent;
                break;
            }
        }

        let Some(parent_id) = parent_node else {
            // We haven't met any Reference in the output.
            self.set_dist(row_id, Distribution::Any)?;
            return Ok(());
        };
        let parent = self.get_relation_node(parent_id)?;

        let children = parent.children();
        if children.is_empty() {
            // Working with a leaf node (ScanRelation).
            let tbl_name = self.get_scan_relation(parent_id)?;
            let tbl = self.get_relation_or_error(tbl_name)?;
            if tbl.is_global() {
                self.set_dist(row_id, Distribution::Global)?;
                return Ok(());
            }
            let mut table_map: HashMap<usize, usize, RandomState> =
                HashMap::with_capacity_and_hasher(row_children.len(), RandomState::new());
            for (pos, id) in row_children.iter().enumerate() {
                let child_id = self.get_child_under_alias(*id)?;
                if let Expression::Reference(Reference {
                    targets, position, ..
                }) = self.get_expression_node(child_id)?
                {
                    if targets.is_some() {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some("References to the children targets in the leaf (relation scan) node are not supported".to_smolstr()),
                        ));
                    }
                    table_map.insert(*position, pos);
                }
            }
            let sk = tbl.get_sk()?;
            let mut new_key: Key = Key::new(Vec::with_capacity(sk.len()));
            let all_found = sk.iter().all(|pos| {
                table_map.get(pos).map_or(false, |v| {
                    new_key.positions.push(*v);
                    true
                })
            });
            if all_found {
                let keys: HashSet<Key, RepeatableState> = collection! { new_key };
                self.set_dist(row_id, Distribution::Segment { keys: keys.into() })?;
            }
        } else {
            // Working with all other nodes.
            let ref_info = ReferenceInfo::new(row_id, self, &children)?;

            match ref_info.referred_children {
                ReferredNodes::None => {
                    // Row contains reference that doesn't point to any relational node.
                    panic!("Row reference doesn't point to relational node.");
                }
                ReferredNodes::Single(child_id) => {
                    let suggested_dist =
                        self.dist_from_child(child_id, &ref_info.child_column_to_parent_col)?;
                    self.set_dist(row_id, suggested_dist)?;
                }
                ReferredNodes::Pair(n1, n2) => {
                    // Union, join
                    self.set_two_children_node_dist(
                        &ref_info.child_column_to_parent_col,
                        n1,
                        n2,
                        parent_id,
                        row_id,
                    )?;
                }
                ReferredNodes::Multiple(_) => {
                    // Reference points to more than two relational children nodes,
                    // that is impossible.
                    panic!(
                        "Row contains multiple references to the same node (and it is not VALUES)"
                    );
                }
            }
        }
        Ok(())
    }

    /// Each relational node have non-sq (required) and sq (additional) children.
    /// In case required children have `Distribution::Global` we can copy sq distribution
    /// as far as required children data is stored on each replicaset.
    ///
    /// In case all required children have Global distribution it improves
    /// Global distribution based on subqueries in case there are any (note that `ValuesRow` has
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

        let required_children_len = self
            .get_required_children_len(node_id)?
            .unwrap_or_else(|| panic!("Unexpected node to get required children number: {node:?}"));
        // Check all required children have Global distribution.
        for child_idx in 0..required_children_len {
            let child_id = self.get_relational_child(node_id, child_idx)?;
            let child_dist = self.get_rel_distribution(child_id)?;
            if !matches!(child_dist, Distribution::Global) {
                return Ok(None);
            }
        }

        let children_len = node.children().len();
        let mut suggested_dist = Some(Distribution::Global);
        for sq_idx in required_children_len..children_len {
            let sq_id = self.get_relational_child(node_id, sq_idx)?;
            let sq_dist = self.get_rel_distribution(sq_id)?;
            match sq_dist {
                Distribution::Segment { .. } => {
                    suggested_dist = Some(Distribution::Any);
                }
                Distribution::Any { .. } => {
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

    fn dist_from_child(
        &self,
        child_rel_node: NodeId,
        child_pos_map: &AHashMap<ChildColumnReference, Vec<ParentColumnPosition>>,
    ) -> Result<Distribution, SbroadError> {
        let rel_node = self.get_relation_node(child_rel_node)?;
        let output_expr = self.get_expression_node(rel_node.output())?;
        if let Expression::Row(Row {
            distribution: child_dist,
            ..
        }) = output_expr
        {
            match child_dist {
                None => panic!("Unable to calculate distribution from child: it's uninitialized."),
                Some(Distribution::Single) => Ok(Distribution::Single),
                Some(Distribution::Any) => Ok(Distribution::Any),
                Some(Distribution::Global) => Ok(Distribution::Global),
                Some(Distribution::Segment { keys }) => {
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
        } else {
            panic!("Expected Row node.")
        }
    }

    /// Sets the `Distribution` of row to given one
    ///
    /// # Errors
    /// - Unable to get node.
    ///
    /// # Panics
    /// - Supplied node is `Row`.
    pub fn set_dist(&mut self, row_id: NodeId, dist: Distribution) -> Result<(), SbroadError> {
        if let MutExpression::Row(Row {
            ref mut distribution,
            ..
        }) = self.get_mut_expression_node(row_id)?
        {
            *distribution = Some(dist);
            return Ok(());
        }
        panic!("The node is not a Row.");
    }

    fn set_two_children_node_dist(
        &mut self,
        child_pos_map: &AHashMap<ChildColumnReference, Vec<ParentColumnPosition>>,
        left_id: NodeId,
        right_id: NodeId,
        parent_id: NodeId,
        row_id: NodeId,
    ) -> Result<(), SbroadError> {
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
        self.set_dist(row_id, new_dist)?;

        Ok(())
    }

    /// Gets current row distribution.
    ///
    /// # Errors
    /// Returns `SbroadError` when the function is called on expression
    /// other than `Row` or a node doesn't know its distribution yet.
    pub fn distribution(&self, id: NodeId) -> Result<&Distribution, SbroadError> {
        if let Expression::Row(Row { distribution, .. }) = self.get_expression_node(id)? {
            let Some(dist) = distribution else {
                return Err(SbroadError::Invalid(
                    Entity::Distribution,
                    Some("distribution is uninitialized".into()),
                ));
            };
            return Ok(dist);
        }
        Err(SbroadError::Invalid(Entity::Expression, None))
    }

    /// Gets distribution of the output row.
    ///
    /// # Errors
    /// - Node is not of a row type.
    pub fn get_distribution(&self, row_id: NodeId) -> Result<&Distribution, SbroadError> {
        self.distribution(row_id)
    }

    /// Gets distribution of the relational node.
    ///
    /// # Errors
    /// - Node is not realtional
    /// - Node is not of a row type.
    pub fn get_rel_distribution(&self, rel_id: NodeId) -> Result<&Distribution, SbroadError> {
        self.get_distribution(self.get_relation_node(rel_id)?.output())
    }
}

#[cfg(test)]
mod tests;
