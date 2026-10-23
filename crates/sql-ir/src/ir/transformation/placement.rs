//! Attaching restriction clauses to the plan tree, shared by the passes that
//! move or derive filters (`enrich_restrictions`, `push_down_restrictions`).
//!
//! A pass chooses a [`Placement`] for each clause on the unchanged tree, groups
//! the clauses by it and hands the groups to [`Plan::apply_placements`]. That
//! step edits the owners' conditions, splices the new filter nodes, keeps
//! [`Restrictions`](super::restriction::Restrictions) in step with the tree and
//! registers the inserted nodes in the equality facts. How a pass picks the
//! placement of its clauses stays its own.

use crate::errors::{Entity, SbroadError};
use crate::ir::columns::RelColumn;
use crate::ir::helpers::RepeatableState;
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::{MutRelational, Relational};
use crate::ir::node::{Constant, Join, NodeId, Selection, SubQueryReference};
use crate::ir::operator::JoinKind;
use crate::ir::transformation::restriction::{RestrictInfo, Restriction};
use crate::ir::tree::traversal::REL_CAPACITY;
use crate::ir::value::Value;
use crate::ir::Plan;
use ahash::{AHashMap, AHashSet};
use smallvec::{smallvec, SmallVec};

/// Where clauses attach to the tree. Chosen during planning and consumed by
/// [`Plan::apply_placements`] without repeating the placement search.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Placement {
    /// Fold into this existing `Selection`'s filter.
    ExistingSelection(NodeId),
    /// Fold into this INNER `Join`'s `ON`.
    InnerJoinOn(NodeId),
    /// Splice a plain `Selection` on the edge `parent -> child`.
    NewSelection { parent: NodeId, child: NodeId },
    /// Wrap a new `Selection` over `child` in a `SELECT *` subquery.
    NewSubquery { parent: NodeId, child: NodeId },
}

/// Clauses to attach, grouped by their placement, in a repeatable order.
pub(super) type Grouped = AHashMap<Placement, Vec<RestrictInfo>, RepeatableState>;

/// The conjuncts each existing filter or INNER `ON` loses, keyed by the fold
/// placement of their owner.
pub(super) type Dropped = AHashMap<Placement, AHashSet<NodeId>>;

/// Inserted filter chains as `(child, inserted_nodes)`, to register in the
/// equality facts as passthroughs of `child`.
pub(super) type Passthroughs = Vec<(NodeId, SmallVec<[NodeId; 3]>)>;

/// The node each placement materialized as.
pub(super) type Hosts = AHashMap<Placement, NodeId>;

impl Plan {
    /// Attach every group of `grouped` at its placement, dropping the `dropped`
    /// conjuncts of the existing owners on the way; an owner that only loses
    /// conjuncts is rebuilt too. Restrictions follow every step and the
    /// inserted nodes are registered in the equality facts at the end.
    ///
    /// Return `(owner, clause)` of every attached clause and the node each
    /// placement materialized as.
    ///
    /// # Errors
    /// - Building the `AND` filter or splicing a node fails.
    pub(super) fn apply_placements(
        &mut self,
        mut grouped: Grouped,
        mut dropped: Dropped,
    ) -> Result<(Vec<(NodeId, NodeId)>, Hosts), SbroadError> {
        for &placement in dropped.keys() {
            grouped.entry(placement).or_default();
        }
        // Splicing does not read equality facts. Register the inserted nodes
        // afterwards to rebuild each affected class's members only once.
        let mut passthroughs = Passthroughs::new();
        // Folds before splices: splicing over a join operand rewrites the
        // references already in that join's `ON`, so a clause folded into the
        // `ON` afterwards would keep pointing at the old operand.
        // Partition preserves the map's iteration order within each kind.
        let (folds, splices): (Vec<_>, Vec<_>) = grouped.into_iter().partition(|(placement, _)| {
            matches!(
                placement,
                Placement::ExistingSelection(_) | Placement::InnerJoinOn(_)
            )
        });
        // Splices retarget references in place, so the clause ids stay valid.
        let mut attached: Vec<(NodeId, NodeId)> = Vec::new();
        let mut hosts = Hosts::new();
        // Each step keeps the restrictions in step with the tree: later
        // readers see no dropped or missing clause, nor a slot of a replaced
        // child.
        for (placement, clauses) in folds.into_iter().chain(splices) {
            let lost = dropped.remove(&placement);
            let ids: SmallVec<[NodeId; 4]> = clauses.iter().map(RestrictInfo::clause).collect();
            let rel = self.materialize_restrictions(
                placement,
                clauses,
                lost.as_ref(),
                &mut passthroughs,
            )?;
            hosts.insert(placement, rel);
            attached.extend(ids.into_iter().map(|clause| (rel, clause)));
        }
        if let Some(facts) = self.facts.as_mut() {
            facts.register_passthroughs(&passthroughs);
        }
        Ok((attached, hosts))
    }

    /// The placement that folds into `owner`'s own condition: its filter for a
    /// `Selection`, its `ON` for an INNER join, `None` otherwise.
    ///
    /// # Errors
    /// - Reading a relational node fails.
    pub(super) fn fold_placement(&self, owner: NodeId) -> Result<Option<Placement>, SbroadError> {
        Ok(match self.get_relation_node(owner)? {
            Relational::Selection(_) => Some(Placement::ExistingSelection(owner)),
            Relational::Join(Join {
                kind: JoinKind::Inner,
                ..
            }) => Some(Placement::InnerJoinOn(owner)),
            _ => None,
        })
    }

    /// Materialize `derived` at the planned `placement`: extend an existing
    /// filter or INNER JOIN condition without its `dropped` conjuncts, or create
    /// the planned filter nodes. Only a fold may drop conjuncts or have no
    /// clauses. Record each inserted chain in `passthroughs` as
    /// `(child, inserted_nodes)`. The owner's restriction follows: `derived`
    /// is appended, `dropped` removed. Return the node whose filter /
    /// condition changed.
    ///
    /// # Errors
    /// - Building the `AND` filter or splicing a node fails.
    fn materialize_restrictions(
        &mut self,
        placement: Placement,
        derived: Vec<RestrictInfo>,
        dropped: Option<&AHashSet<NodeId>>,
        passthroughs: &mut Passthroughs,
    ) -> Result<NodeId, SbroadError> {
        debug_assert!(
            matches!(
                placement,
                Placement::ExistingSelection(_) | Placement::InnerJoinOn(_)
            ) || (dropped.is_none() && !derived.is_empty()),
            "placement: a new filter drops nothing and is never empty"
        );
        Ok(match placement {
            // Keep the remaining conjuncts and append the new ones.
            Placement::ExistingSelection(selection) => {
                let Relational::Selection(Selection { filter, .. }) =
                    self.get_relation_node(selection)?
                else {
                    unreachable!("caller checked Selection");
                };
                let filter = self.rebuild_condition(selection, *filter, dropped, derived)?;
                let MutRelational::Selection(Selection {
                    filter: filter_id, ..
                }) = self.get_mut_relation_node(selection)?
                else {
                    unreachable!("caller checked Selection");
                };
                *filter_id = filter;
                self.sync_condition_subqueries(selection, filter)?;
                selection
            }
            // An INNER join emits only pairs that satisfy ON. An outer join
            // also emits unmatched rows, so ON cannot enforce an output filter.
            Placement::InnerJoinOn(join) => {
                let Relational::Join(Join { condition, .. }) = self.get_relation_node(join)? else {
                    unreachable!("caller checked Join");
                };
                let condition = self.rebuild_condition(join, *condition, dropped, derived)?;
                let MutRelational::Join(Join {
                    condition: condition_id,
                    ..
                }) = self.get_mut_relation_node(join)?
                else {
                    unreachable!("caller checked Join");
                };
                *condition_id = condition;
                self.sync_condition_subqueries(join, condition)?;
                join
            }
            Placement::NewSelection { parent, child } => {
                let new_sel = self.add_filter_over(child, derived)?;
                self.splice_over(parent, child, new_sel)?;
                passthroughs.push((child, smallvec![new_sel]));
                new_sel
            }
            // A join / set-op operand must be a named relation: wrap `child` in a
            // single-alias subquery that preserves its output columns.
            Placement::NewSubquery { parent, child } => {
                let new_sel = self.add_filter_over(child, derived)?;
                let alias = self
                    .scan_name(RelColumn::new(child, 0))?
                    .map(smol_str::ToSmolStr::to_smolstr);
                // SELECT * preserves duplicate output names without ambiguous
                // column references. Keep the shard column for routing.
                let new_proj = self.add_proj_star(new_sel, true)?;
                let sub_query = self.add_sub_query(new_proj, alias.as_deref())?;
                self.splice_over(parent, child, sub_query)?;
                passthroughs.push((child, smallvec![new_sel, new_proj, sub_query]));
                new_sel
            }
        })
    }

    /// A new `Selection` over `child` filtering by `derived`, which become its
    /// restriction: the owner is this Selection, not a carrier above it.
    ///
    /// # Errors
    /// - Building the `AND` filter or the Selection fails.
    fn add_filter_over(
        &mut self,
        child: NodeId,
        derived: Vec<RestrictInfo>,
    ) -> Result<NodeId, SbroadError> {
        let filter = self.and_of(derived.iter().map(RestrictInfo::clause).collect())?;
        let new_sel = self.add_select(child, filter)?;
        // `add_select` starts with no subqueries: the filter's own come next.
        self.sync_condition_subqueries(new_sel, filter)?;
        if let Some(restrictions) = self.restrictions.as_mut() {
            restrictions.extend(new_sel, derived);
        }
        Ok(new_sel)
    }

    /// Make the `subqueries` of `owner`, a `Selection` or `Join`, list exactly
    /// the subqueries its `condition` references, each once: the ones it
    /// keeps stay in their order, new ones follow in the order the condition
    /// first names them. Rebuilt from the final condition rather than by
    /// removing a leaving clause's subqueries, since a kept clause may
    /// reference the same body. An unchanged list is left as is.
    ///
    /// Expected `O(E + Q)` for `E` expression visits in the condition traversal
    /// and `Q` entries in the old list. Hash sets only answer membership; order
    /// comes from the condition traversal and the old list.
    ///
    /// # Errors
    /// - Reading the condition fails.
    fn sync_condition_subqueries(
        &mut self,
        owner: NodeId,
        condition: NodeId,
    ) -> Result<(), SbroadError> {
        let refs = self.get_sq_refs_from_subtree(condition)?;
        let current = self.get_relation_subqueries(owner)?;
        // Most conditions reference no subquery: nothing to hash.
        if refs.is_empty() {
            if !current.is_empty() {
                self.set_relational_subqueries(owner, Vec::new());
            }
            return Ok(());
        }
        let mut referenced: Vec<NodeId> = Vec::new();
        let mut referenced_set: AHashSet<NodeId> = AHashSet::with_capacity(refs.len());
        for ref_id in refs {
            let Expression::SubQueryReference(SubQueryReference { rel_id, .. }) =
                self.get_expression_node(ref_id)?
            else {
                unreachable!("filtered to subquery references");
            };
            if referenced_set.insert(*rel_id) {
                referenced.push(*rel_id);
            }
        }
        let mut subqueries: Vec<NodeId> = Vec::with_capacity(referenced.len());
        let mut added: AHashSet<NodeId> = AHashSet::with_capacity(referenced.len());
        for &subquery in current.iter() {
            if referenced_set.contains(&subquery) && added.insert(subquery) {
                subqueries.push(subquery);
            }
        }
        for subquery in referenced {
            if added.insert(subquery) {
                subqueries.push(subquery);
            }
        }
        if current.iter().eq(subqueries.iter()) {
            return Ok(());
        }
        self.set_relational_subqueries(owner, subqueries);
        Ok(())
    }

    /// The existing filter / condition `base` of `owner` without its `dropped`
    /// conjuncts and with `derived` folded on; `true` when nothing remains.
    /// The owner's restriction gets the same edit: the dropped sources leave
    /// it and `derived` is appended. When no old conjunct survives (all were
    /// dropped, or only a bare `true` that the fold replaces), the restriction
    /// is `derived` (or the new `true`) alone.
    ///
    /// # Errors
    /// - Reading an expression or building the `AND` fails.
    fn rebuild_condition(
        &mut self,
        owner: NodeId,
        base: NodeId,
        dropped: Option<&AHashSet<NodeId>>,
        mut derived: Vec<RestrictInfo>,
    ) -> Result<NodeId, SbroadError> {
        let clauses: Vec<NodeId> = derived.iter().map(RestrictInfo::clause).collect();
        let (condition, kept_none) = match self.without_conjuncts(owner, base, dropped)? {
            // Fold onto the remaining condition; a bare `true` may be replaced.
            Some(kept) => self.fold_onto(kept, clauses)?,
            None if clauses.is_empty() => {
                let true_id = self.add_const(Value::Boolean(true));
                // TODO: drop the bare `true` from restrictions? It restricts
                // nothing and is kept only to match `RestrictionBuilder`, which
                // records it too; skip it there as well if removed.
                derived.push(RestrictInfo::implied(true_id, SmallVec::new()));
                (true_id, true)
            }
            None => (self.and_of(clauses)?, true),
        };
        if let Some(restrictions) = self.restrictions.as_mut() {
            if kept_none {
                restrictions.set(owner, derived);
            } else {
                if let Some(dropped) = dropped {
                    restrictions.retain(owner, |clause| !dropped.contains(&clause));
                }
                restrictions.extend(owner, derived);
            }
        }
        Ok(condition)
    }

    /// `owner`'s condition `base` without its `dropped` top-level conjuncts,
    /// the rest `AND`ed in their order; `None` when none remain. The conjuncts
    /// come from `owner`'s restriction, still unchanged here. Builds new `AND`
    /// nodes only and never changes a kept conjunct, which another node may
    /// share.
    ///
    /// # Errors
    /// - `owner` has no restriction.
    /// - Building the `AND` fails.
    fn without_conjuncts(
        &mut self,
        owner: NodeId,
        base: NodeId,
        dropped: Option<&AHashSet<NodeId>>,
    ) -> Result<Option<NodeId>, SbroadError> {
        let Some(dropped) = dropped.filter(|dropped| !dropped.is_empty()) else {
            return Ok(Some(base));
        };
        let clauses = self.owner_restriction(owner)?.clauses();
        let kept: Vec<NodeId> = clauses
            .iter()
            .map(RestrictInfo::clause)
            .filter(|clause| !dropped.contains(clause))
            .collect();
        // Sources are conjuncts of this very condition, read at analysis. A
        // conjunct may repeat through one shared node (`x <> false and x <>
        // false` folded to `x and x`), so count nodes, not occurrences.
        debug_assert!(
            dropped
                .iter()
                .all(|id| clauses.iter().any(|clause| clause.clause() == *id)),
            "placement: a dropped conjunct is no conjunct of {base:?}"
        );
        if kept.is_empty() {
            return Ok(None);
        }
        self.and_of(kept).map(Some)
    }

    /// Relink `parent` from `child` to `spliced` (the top of the inserted chain),
    /// retargeting the parent's restriction too, even when it gains no clause.
    /// The caller must register the inserted nodes as passthroughs of `child`
    /// in the equality facts so the motion pass can track join keys through them.
    fn splice_over(
        &mut self,
        parent: NodeId,
        child: NodeId,
        spliced: NodeId,
    ) -> Result<(), SbroadError> {
        // References first (as motion insertion does), then the child pointer.
        self.replace_target_in_relational(parent, child, spliced)?;
        self.change_child(parent, child, spliced)?;
        if let Some(restrictions) = self.restrictions.as_mut() {
            restrictions.retarget(parent, child, spliced);
        }
        Ok(())
    }

    /// Fold `clauses` onto an existing filter / condition `base`. A bare `true`
    /// base (a join on `true`) is replaced rather than kept as `true AND ...`;
    /// the flag tells whether it was.
    fn fold_onto(
        &mut self,
        base: NodeId,
        clauses: Vec<NodeId>,
    ) -> Result<(NodeId, bool), SbroadError> {
        if clauses.is_empty() {
            return Ok((base, false));
        }
        if matches!(
            self.get_expression_node(base)?,
            Expression::Constant(Constant {
                value: Value::Boolean(true)
            })
        ) {
            return Ok((self.and_of(clauses)?, true));
        }
        Ok((self.concat_and_fold(base, clauses)?, false))
    }

    /// Fold `clauses` together with `AND`, left to right. `clauses` is never empty.
    fn and_of(&mut self, clauses: Vec<NodeId>) -> Result<NodeId, SbroadError> {
        let mut iter = clauses.into_iter();
        let seed = iter.next().expect("groups are non-empty");
        self.concat_and_fold(seed, iter)
    }

    /// Fold `clauses` onto `base` with `AND`, left to right.
    fn concat_and_fold(
        &mut self,
        base: NodeId,
        clauses: impl IntoIterator<Item = NodeId>,
    ) -> Result<NodeId, SbroadError> {
        let mut filter = base;
        for clause in clauses {
            filter = self.concat_and(filter, clause)?;
        }
        Ok(filter)
    }

    /// The restriction of `owner`, a `Selection` or INNER join of the
    /// analyzed subtree; missing one breaks the pass's precondition.
    ///
    /// # Errors
    /// - `owner` has no restriction.
    pub(super) fn owner_restriction(&self, owner: NodeId) -> Result<&Restriction, SbroadError> {
        self.restrictions
            .as_ref()
            .and_then(|restrictions| restrictions.for_rel(owner))
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Plan,
                    Some(smol_str::ToSmolStr::to_smolstr(&format!(
                        "placement: no restriction for {owner:?}"
                    ))),
                )
            })
    }

    /// Choose a placement for a new filter on the edge `parent -> child`:
    /// a plain `Selection` on a `Projection`'s direct input, or a `SELECT *`
    /// carrier under a join or set operation when `child` is not a join.
    /// Return `None` for unsupported edges.
    pub(super) fn new_filter_placement(
        &self,
        parent: NodeId,
        child: NodeId,
    ) -> Result<Option<Placement>, SbroadError> {
        Ok(match self.get_relation_node(parent)? {
            // A new Selection is supported only on the projection's direct input.
            Relational::Projection(projection) => (projection.child == Some(child))
                .then_some(Placement::NewSelection { parent, child }),
            // A lateral table function references the other join operand, which
            // a subquery cannot see: it stays a direct operand of the join.
            Relational::Join(Join { lateral, .. }) if *lateral == Some(child) => None,
            // A join / set-op operand must be a named relation: wrap it in a
            // `SELECT *` carrier. Refused over an immediate join, whose two input
            // qualifiers (`l`, `r`) a single carrier alias cannot preserve; the `*`
            // carries every other operand's columns through, duplicate names and all.
            Relational::Join(_)
            | Relational::UnionAll(_)
            | Relational::Union(_)
            | Relational::Except(_)
            | Relational::Intersect(_) => {
                (!matches!(self.get_relation_node(child)?, Relational::Join(_)))
                    .then_some(Placement::NewSubquery { parent, child })
            }
            _ => None,
        })
    }

    /// Child -> parent map over the subtree at `top_id`. `rel_iter` visits
    /// referenced subquery bodies too, so a scan inside an `IN` body resolves to
    /// its own enclosing filter.
    pub(super) fn rel_parents(&self, top_id: NodeId) -> AHashMap<NodeId, NodeId> {
        let mut parents: AHashMap<NodeId, NodeId> = AHashMap::with_capacity(REL_CAPACITY);
        let mut seen: AHashSet<NodeId> = AHashSet::with_capacity(REL_CAPACITY);
        let mut stack = Vec::with_capacity(REL_CAPACITY);
        stack.push(top_id);
        while let Some(node) = stack.pop() {
            if !seen.insert(node) {
                // A shared CTE body has several call sites; keep the first parent.
                continue;
            }
            for child in self.nodes.rel_iter(node) {
                parents.entry(child).or_insert(node);
                stack.push(child);
            }
        }
        parents
    }
}
