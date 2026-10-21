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
use crate::ir::node::{Join, NodeId, Reference, Selection, SubQueryReference};
use crate::ir::operator::JoinKind;
use crate::ir::transformation::equality_facts::Slot;
use crate::ir::tree::traversal::{NodeIter, NodePostOrder};
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
    /// A clause derived from the equality facts It is built from class
    /// members only, so it never references a subquery.
    #[must_use]
    pub fn implied(clause: NodeId, slots: SmallVec<[Slot; 2]>) -> Self {
        Self {
            clause,
            slots,
            subqueries: SmallVec::new(),
        }
    }

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

    /// Update cached column and subquery sources to match
    /// [`Plan::replace_target_in_subtree`].
    fn retarget(&mut self, from: NodeId, to: NodeId) {
        for slot in &mut self.slots {
            if slot.rel_id == from {
                slot.rel_id = to;
            }
        }
        for subquery in &mut self.subqueries {
            if *subquery == from {
                *subquery = to;
            }
        }
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
    pub fn add(&mut self, rel_id: NodeId, info: RestrictInfo) {
        self.by_rel.entry(rel_id).or_default().clauses.push(info);
    }

    /// Append `infos` to the restriction of `rel_id`.
    #[cfg_attr(not(feature = "enrich_restrictions"), allow(dead_code))]
    pub(super) fn extend(&mut self, rel_id: NodeId, infos: Vec<RestrictInfo>) {
        self.by_rel.entry(rel_id).or_default().clauses.extend(infos);
    }

    /// Make `infos` the whole restriction of `rel_id`.
    #[cfg_attr(not(feature = "enrich_restrictions"), allow(dead_code))]
    pub(super) fn set(&mut self, rel_id: NodeId, infos: Vec<RestrictInfo>) {
        self.by_rel.insert(rel_id, Restriction { clauses: infos });
    }

    /// Keep only the clauses of `rel_id` whose node `keep` accepts.
    #[cfg_attr(not(feature = "enrich_restrictions"), allow(dead_code))]
    pub(super) fn retain(&mut self, rel_id: NodeId, mut keep: impl FnMut(NodeId) -> bool) {
        if let Some(restriction) = self.by_rel.get_mut(&rel_id) {
            restriction.clauses.retain(|info| keep(info.clause));
        }
    }

    /// Follow [`Plan::replace_target_in_relational`]`(rel_id, from, to)`: the
    /// clauses of `rel_id` reference `to` instead of `from`.
    #[cfg_attr(not(feature = "enrich_restrictions"), allow(dead_code))]
    pub(super) fn retarget(&mut self, rel_id: NodeId, from: NodeId, to: NodeId) {
        if let Some(restriction) = self.by_rel.get_mut(&rel_id) {
            for info in &mut restriction.clauses {
                info.retarget(from, to);
            }
        }
    }
}

/// Walks the raw boolean tree and builds [`Restrictions`].
struct RestrictionBuilder<'p> {
    plan: &'p Plan,
}

impl<'p> RestrictionBuilder<'p> {
    fn build(plan: &'p Plan, top_id: NodeId) -> Result<Restrictions, SbroadError> {
        let builder = RestrictionBuilder { plan };
        let ir_tree = NodePostOrder::new(&plan.nodes, |node| plan.nodes.rel_iter(node), top_id);

        let mut by_rel = HashMap::new();
        for (rel_id, rel) in ir_tree.relational() {
            let expr_id = match rel {
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
        let mut subqueries: SmallVec<[NodeId; 1]> = SmallVec::new();
        // One walk for both kinds. Like `get_refs_from_subtree`, it stays in
        // the expression tree and never enters a subquery's relations.
        let dfs = NodePostOrder::new(
            &self.plan.nodes,
            |node| self.plan.nodes.expr_iter(node, false),
            clause,
        );
        for (_, expr) in dfs.expressions() {
            match expr {
                Expression::Reference(Reference {
                    target, position, ..
                }) => {
                    for node in target.iter() {
                        let slot = Slot::new(*node, *position);
                        if !slots.contains(&slot) {
                            slots.push(slot);
                        }
                    }
                }
                Expression::SubQueryReference(SubQueryReference { rel_id, .. })
                    if !subqueries.contains(rel_id) =>
                {
                    subqueries.push(*rel_id);
                }
                _ => {}
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

    /// The [`RestrictInfo`] of `clause`, as [`Plan::analyze_restrictions_in_subtree`]
    /// builds it for a conjunct.
    ///
    /// # Errors
    /// - Malformed expression node encountered while walking the clause.
    #[cfg_attr(not(feature = "enrich_restrictions"), allow(dead_code))]
    pub(super) fn restrict_info(&self, clause: NodeId) -> Result<RestrictInfo, SbroadError> {
        let (slots, subqueries) = RestrictionBuilder { plan: self }.clause_sources(clause)?;
        Ok(RestrictInfo {
            clause,
            slots,
            subqueries,
        })
    }

    /// Debug check that the stored restrictions describe the current IR of the
    /// subtree at `top_id`: each of its owners has exactly the restriction a
    /// full, independent rebuild gives. Entries outside the subtree are not
    /// checked, so a pass over part of the plan keeps them as they are.
    #[cfg(debug_assertions)]
    #[cfg_attr(not(feature = "enrich_restrictions"), allow(dead_code))]
    pub(super) fn debug_assert_restrictions_fresh(&self, top_id: NodeId) {
        let Some(stored) = self.restrictions.as_ref() else {
            return;
        };
        let fresh = RestrictionBuilder::build(self, top_id).expect("restrictions rebuild");
        for (rel_id, restriction) in &fresh.by_rel {
            debug_assert_eq!(
                stored.for_rel(*rel_id),
                Some(restriction),
                "stale restriction of {rel_id:?}"
            );
        }
    }
}
