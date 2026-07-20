//! Per-relation restriction clauses, the analog of PostgreSQL's `baserestrictinfo`.
//!
//! For every `Selection` filter and INNER `Join` condition this pass splits the
//! top-level `AND` into a flat list of [`RestrictInfo`] clauses, each annotated
//! with the relations it references. It only splits and annotates; extracting
//! value-sets, intersecting them and detecting contradictions are left to the
//! consumers (`equality_facts` and the bucket/pruning step).
//!
//! OUTER (`LEFT`) join conditions are skipped: an outer `ON` does not filter the
//! preserved side, so its clauses are not restrictions.

use std::collections::HashMap;

use smallvec::SmallVec;

use crate::errors::SbroadError;
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::Relational;
use crate::ir::node::{Join, Node, NodeId, Reference, Selection, SubQueryReference};
use crate::ir::operator::JoinKind;
use crate::ir::transformation::equality_facts::Slot;
use crate::ir::tree::traversal::{PostOrderWithFilter, EXPR_CAPACITY};
use crate::ir::Plan;

/// One restriction clause: the source conjunct node, the column sources
/// (`rel_id`, `position`) it references, and the subquery nodes it references.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestrictInfo {
    clause: NodeId,
    slots: SmallVec<[Slot; 2]>,
    /// The relational nodes of the subqueries this clause references (via
    /// `SubQueryReference`). They are owned by node and must be pushed
    /// down together with the clause.
    subqueries: SmallVec<[NodeId; 1]>,
}

impl RestrictInfo {
    /// The clause's source expression node.
    #[must_use]
    pub fn clause(&self) -> NodeId {
        self.clause
    }

    /// The column sources this clause references (deduplicated).
    #[must_use]
    pub fn slots(&self) -> &[Slot] {
        &self.slots
    }

    /// The subquery nodes this clause references (deduplicated).
    #[must_use]
    pub fn subqueries(&self) -> &[NodeId] {
        &self.subqueries
    }
}

/// The restriction clauses for one relational node's filter or condition.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Restriction {
    clauses: Vec<RestrictInfo>,
}

impl Restriction {
    #[must_use]
    pub fn clauses(&self) -> &[RestrictInfo] {
        &self.clauses
    }
}

/// Restrictions for a subtree, keyed by the relational node that owns the
/// filter or condition.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Restrictions {
    pub by_rel: HashMap<NodeId, Restriction>,
}

impl Restrictions {
    #[must_use]
    pub fn for_rel(&self, rel_id: NodeId) -> Option<&Restriction> {
        self.by_rel.get(&rel_id)
    }
}

/// Walks the raw boolean tree and builds [`Restrictions`].
struct RestrictionBuilder<'p> {
    plan: &'p Plan,
}

impl<'p> RestrictionBuilder<'p> {
    fn build(plan: &'p Plan, top_id: NodeId) -> Result<Restrictions, SbroadError> {
        let builder = RestrictionBuilder { plan };
        let ir_tree = PostOrderWithFilter::new(
            |node| plan.nodes.rel_iter(node),
            |node| {
                matches!(
                    plan.get_node(node),
                    Ok(Node::Relational(Relational::Join(_)))
                        | Ok(Node::Relational(Relational::Selection(_)))
                )
            },
            EXPR_CAPACITY,
        );
        let nodes = ir_tree.traverse_into_vec(top_id);

        let mut by_rel = HashMap::new();
        for rel_id in nodes {
            let expr_id = match plan.get_relation_node(rel_id)? {
                Relational::Selection(Selection { filter, .. }) => *filter,
                // Only INNER conditions are restrictions; LEFT is skipped.
                Relational::Join(Join {
                    condition,
                    kind: JoinKind::Inner,
                    ..
                }) => *condition,
                _ => continue,
            };
            let restriction = builder.build_for_filter(expr_id)?;
            by_rel.insert(rel_id, restriction);
        }
        Ok(Restrictions { by_rel })
    }

    fn build_for_filter(&self, expr_id: NodeId) -> Result<Restriction, SbroadError> {
        let clauses = self
            .plan
            .nodes
            .and_conjuncts(expr_id)
            .into_iter()
            .map(|clause| {
                let (slots, subqueries) = self.clause_sources(clause)?;
                Ok(RestrictInfo {
                    clause,
                    slots,
                    subqueries,
                })
            })
            .collect::<Result<Vec<_>, SbroadError>>()?;
        Ok(Restriction { clauses })
    }

    /// Collect what a clause references: the `(rel_id, position)` column sources
    /// (from every `Reference`) and the subquery nodes (from every
    /// `SubQueryReference`) in its expression subtree. Both must travel with the
    /// clause when it is pushed down the tree. A reference through a
    /// `UNION`/`VALUES` contributes all of its target nodes at that position. No
    /// resolution down the tree happens here.
    #[allow(clippy::type_complexity)]
    fn clause_sources(
        &self,
        clause: NodeId,
    ) -> Result<(SmallVec<[Slot; 2]>, SmallVec<[NodeId; 1]>), SbroadError> {
        let mut slots: SmallVec<[Slot; 2]> = SmallVec::new();
        for ref_id in self.plan.get_refs_from_subtree(clause)? {
            let Expression::Reference(Reference {
                target, position, ..
            }) = self.plan.get_expression_node(ref_id)?
            else {
                continue;
            };
            for node in target.iter() {
                let slot = Slot::new(*node, *position);
                if !slots.contains(&slot) {
                    slots.push(slot);
                }
            }
        }

        let mut subqueries: SmallVec<[NodeId; 1]> = SmallVec::new();
        for sq_id in self.plan.get_sq_refs_from_subtree(clause)? {
            let Expression::SubQueryReference(SubQueryReference { rel_id, .. }) =
                self.plan.get_expression_node(sq_id)?
            else {
                continue;
            };
            if !subqueries.contains(rel_id) {
                subqueries.push(*rel_id);
            }
        }

        Ok((slots, subqueries))
    }
}

impl Plan {
    /// Build the [`Restrictions`] for a subtree and store them on the plan.
    ///
    /// # Errors
    /// - Malformed plan node encountered while walking the expression trees.
    pub fn analyze_restrictions_in_subtree(mut self, top_id: NodeId) -> Result<Self, SbroadError> {
        self.restrictions = RestrictionBuilder::build(&self, top_id).map(Some)?;
        Ok(self)
    }
}
