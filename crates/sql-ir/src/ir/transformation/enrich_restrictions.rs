//! Deriving the equality conditions a query implies but never spells out.
//!
//! When a query proves `a = b AND b = 5`, it also proves `a = 5`. For each such
//! group tied to a constant (or, failing that, a bind parameter), this pass states
//! `member = that value` for every other member.
//!
//! A column member's condition attaches at the deepest point the column still
//! exists ([`Placement`]), so it can restrict a table scan even across a join. A
//! bind parameter has no column, so it becomes an ordinary `parameter = value`
//! conjunct placed within the class's domain — never above an outer join or
//! aggregate the fact does not constrain. Each column must have a valid placement;
//! a parameter check with no valid placement is skipped. The query's original
//! equalities are always kept.
//!
//! An unpinned class derives equalities between columns with the same relation
//! and placement. Each column is paired with the first, producing `n - 1`
//! predicates for `n` columns. Cross-relation pairs are not derived.
//!
//! An unsatisfiable filter or INNER JOIN condition gets a `false` conjunct.
//! A class with conflicting constants from separate filters gets a `false`
//! predicate within its domain, using the placement rules for parameter checks.
//!
//! Only equalities that hold for every row are used; an outer join's `ON` is left
//! out, as it holds only for matched rows.

use crate::errors::{Entity, SbroadError};
use crate::ir::columns::RelColumn;
use crate::ir::helpers::RepeatableState;
use crate::ir::node::relational::{MutRelational, Relational};
use crate::ir::node::{Join, NodeId, Projection, ReferenceTarget, ScanSubQuery, Selection};
use crate::ir::operator::{Bool, JoinKind};
use crate::ir::transformation::equality_facts::{ClassPin, EqualityFacts, EquivalenceClass, Slot};
use crate::ir::tree::traversal::REL_CAPACITY;
use crate::ir::types::DerivedType;
use crate::ir::value::Value;
use crate::ir::Plan;
use ahash::{AHashMap, AHashSet};
use smallvec::{smallvec, SmallVec};
use std::collections::hash_map::Entry;

enum Pin {
    Const(Value),
    Param(u16, DerivedType),
}

/// Where derived predicates attach to the tree. Chosen during planning and
/// consumed by `materialize_restrictions` without repeating the placement search.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Placement {
    /// Fold into this existing `Selection`'s filter.
    ExistingSelection(NodeId),
    /// Fold into this INNER `Join`'s `ON`.
    InnerJoinOn(NodeId),
    /// Splice a plain `Selection` on the edge `parent -> child`.
    NewSelection { parent: NodeId, child: NodeId },
    /// Wrap a new `Selection` over `child` in a `SELECT *` subquery.
    NewSubquery { parent: NodeId, child: NodeId },
}

/// One `left = right` equality between two columns of the same relational
/// node, derived from an unpinned class and attached through `placement`.
struct ColPair {
    placement: Placement,
    rel_id: NodeId,
    left: usize,
    right: usize,
}

/// A star of equalities connecting columns and parameters to one shared pin.
/// Stores the operands and placements; `build_predicates` creates the expressions.
/// Every lowest column member must have a placement. Parameter checks without
/// a valid placement are omitted.
struct Star {
    pin: Pin,
    cols: Vec<(Placement, Slot)>,
    /// Parameters to compare with the pin, with a shared placement for the checks.
    params: Option<(Placement, Vec<(u16, DerivedType)>)>,
}

/// What enrichment derives from one class, see [`classify`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ClassAction<'a> {
    /// A `false` marker: the class is unsatisfiable.
    Contradiction,
    /// A star of `member = pin` (and `$param = pin`) equalities.
    Pinned(ClassPin<'a>),
    /// `col = col` pairs between members on one relational node.
    Pairs,
}

/// Choose one action: mark a contradiction, derive equalities to a pin, or
/// derive column pairs. Pins hold throughout the class's domain; facts scoped
/// to a LEFT JOIN's ON are excluded from global classes.
///
/// Pairs require an unpinned class with two members on the same relation.
/// A NULL constant is not a pin and does not permit pair derivation.
/// Members are sorted by relation, so adjacent entries suffice for this check.
fn classify(class: &EquivalenceClass) -> Option<ClassAction<'_>> {
    if class.is_contradictory() {
        return Some(ClassAction::Contradiction);
    }
    if let Some(pin) = class.pin() {
        return (!class.members.is_empty() || !class.params.is_empty())
            .then_some(ClassAction::Pinned(pin));
    }
    (class.constant.is_none()
        && class.params.is_empty()
        && class
            .members
            .windows(2)
            .any(|pair| pair[0].rel_id == pair[1].rel_id))
    .then_some(ClassAction::Pairs)
}

/// Predicates in build order: pinned equalities, column pairs, then false
/// markers for unsatisfiable conditions and contradictory classes.
#[derive(Default)]
struct Enrichment {
    stars: Vec<Star>,
    pairs: Vec<ColPair>,
    empty: Vec<Placement>,
}

impl Plan {
    /// Add implied equalities and false markers from analyzed facts.
    /// Requires equality facts and restrictions.
    ///
    /// # Errors
    /// - Building a predicate, reading a column, or splicing a filter fails.
    pub fn enrich_restrictions_from_facts(mut self, top_id: NodeId) -> Result<Self, SbroadError> {
        // Anchors name nodes of this subtree; facts from another would misplace.
        debug_assert!(
            self.facts
                .as_ref()
                .is_none_or(|facts| facts.analyzed_top() == top_id),
            "facts analyzed for another subtree"
        );
        if self.restrictions.is_none() {
            return Ok(self);
        }
        // Three phases, kept apart so the search never depends on the order of
        // already-inserted nodes: plan where each class emits (reads the tree),
        // build the predicate expressions, then apply the relational changes.
        let Some(planned) = self.plan_enrichment(top_id)? else {
            return Ok(self);
        };
        let grouped = self.build_predicates(planned)?;
        // Splicing does not read equality facts. Register the inserted nodes
        // afterwards to rebuild each affected class's members only once.
        let mut passthroughs = Vec::new();
        for (placement, clauses) in grouped {
            self.materialize_restrictions(placement, clauses, &mut passthroughs)?;
        }
        if let Some(facts) = self.facts.as_mut() {
            facts.register_passthroughs(&passthroughs);
        }
        Ok(self)
    }

    /// Plan predicates in a stable order without modifying the tree.
    /// Return `None` when no class or relation needs enrichment.
    ///
    /// # Errors
    /// - Reading a relational node or an output column fails.
    fn plan_enrichment(&self, top_id: NodeId) -> Result<Option<Enrichment>, SbroadError> {
        let Some(facts) = self.facts.as_ref() else {
            return Ok(None);
        };
        let mut candidates: Vec<(&EquivalenceClass, ClassAction<'_>)> = facts
            .classes
            .iter()
            .filter_map(|class| Some((class, classify(class)?)))
            .collect();
        if candidates.is_empty() && facts.dead_rels().is_empty() {
            return Ok(None);
        }
        // Sort by stable sources because ClassId order depends on hashing.
        candidates.sort_unstable_by_key(|(class, _)| {
            (
                class.anchor.arena_type,
                class.anchor.offset,
                class.members.as_ref(),
                class.params.as_ref(),
            )
        });

        // Only column placement needs a parent map.
        let parents = if candidates.iter().any(|(class, action)| {
            !class.members.is_empty() && !matches!(action, ClassAction::Contradiction)
        }) {
            self.rel_parents(top_id)
        } else {
            AHashMap::new()
        };
        // Cache each source column's first output position per parent.
        // Shared across classes to avoid quadratic scans of wide outputs.
        // Valid only during planning, before the tree changes.
        let mut lift_index: AHashMap<NodeId, AHashMap<RelColumn, usize>> = AHashMap::new();
        // Two domains pinning the same param to the same value are two distinct
        // checks, so key seen checks by anchor too.
        let mut param_check_seen: AHashSet<(u16, ClassPin<'_>, NodeId)> = AHashSet::new();
        let mut planned = Enrichment {
            empty: self.dead_rel_markers(facts)?,
            ..Enrichment::default()
        };
        for (class, action) in candidates {
            match action {
                ClassAction::Contradiction => {
                    if let Some(placement) = self.place_param_check(class.anchor)? {
                        planned.empty.push(placement);
                    }
                }
                ClassAction::Pinned(pin) => {
                    let star = self.plan_class_star(
                        facts,
                        class,
                        pin,
                        &parents,
                        &mut lift_index,
                        &mut param_check_seen,
                    )?;
                    planned.stars.extend(star);
                }
                ClassAction::Pairs => {
                    self.plan_class_pairs(class, &parents, &mut lift_index, &mut planned.pairs)?;
                }
            }
        }
        Ok(Some(planned))
    }

    /// Build expressions grouped by [`Placement`] without changing relations.
    ///
    /// # Errors
    /// - Reading an output column or building a predicate fails.
    fn build_predicates(
        &mut self,
        Enrichment {
            stars,
            pairs,
            empty,
        }: Enrichment,
    ) -> Result<AHashMap<Placement, Vec<NodeId>, RepeatableState>, SbroadError> {
        let mut grouped: AHashMap<Placement, Vec<NodeId>, RepeatableState> =
            AHashMap::with_hasher(RepeatableState::default());
        for class in stars {
            // Param checks before column pins, for a stable rendered conjunct order.
            if let Some((placement, params)) = class.params {
                for (param, param_type) in params {
                    let check_id = self.make_param_check(param, param_type, &class.pin)?;
                    grouped.entry(placement).or_default().push(check_id);
                }
            }
            for &(placement, slot) in &class.cols {
                let eq_id = self.make_pin_eq(slot.rel_id, slot.pos, &class.pin)?;
                grouped.entry(placement).or_default().push(eq_id);
            }

            // TODO: here we enrich only filters, but motions can be executed with
            // one-time filter violation. One way to prevent it is to store one-time
            // filters in the plan and execute them before the rest.
        }
        for pair in pairs {
            let eq_id = self.make_col_eq(pair.rel_id, pair.left, pair.right)?;
            grouped.entry(pair.placement).or_default().push(eq_id);
        }
        for placement in empty {
            let false_id = self.add_const(Value::Boolean(false));
            grouped.entry(placement).or_default().push(false_id);
        }
        Ok(grouped)
    }

    /// Materialize nonempty `clauses` at the planned `placement`: extend an
    /// existing filter or INNER JOIN condition, or create the planned filter nodes.
    /// Record each inserted chain in `passthroughs` as `(child, inserted_nodes)`.
    ///
    /// # Errors
    /// - Building the `AND` filter or splicing a node fails.
    fn materialize_restrictions(
        &mut self,
        placement: Placement,
        clauses: Vec<NodeId>,
        passthroughs: &mut Vec<(NodeId, SmallVec<[NodeId; 3]>)>,
    ) -> Result<(), SbroadError> {
        match placement {
            // Fold onto the existing filter / condition, which stays the AND's base.
            Placement::ExistingSelection(selection) => {
                let Relational::Selection(Selection { filter, .. }) =
                    self.get_relation_node(selection)?
                else {
                    unreachable!("caller checked Selection");
                };
                let filter = self.concat_and_fold(*filter, clauses)?;
                let MutRelational::Selection(Selection {
                    filter: filter_id, ..
                }) = self.get_mut_relation_node(selection)?
                else {
                    unreachable!("caller checked Selection");
                };
                *filter_id = filter;
            }
            // Sound only for an INNER join: its condition filters the output, so a
            // failed check empties the result. An outer join keeps its preserved
            // side regardless, so its condition is never a restriction.
            Placement::InnerJoinOn(join) => {
                let Relational::Join(Join { condition, .. }) = self.get_relation_node(join)? else {
                    unreachable!("caller checked Join");
                };
                let condition = self.concat_and_fold(*condition, clauses)?;
                let MutRelational::Join(Join {
                    condition: condition_id,
                    ..
                }) = self.get_mut_relation_node(join)?
                else {
                    unreachable!("caller checked Join");
                };
                *condition_id = condition;
            }
            Placement::NewSelection { parent, child } => {
                let filter = self.and_of(clauses)?;
                let new_sel = self.add_select(child, filter)?;
                self.splice_over(parent, child, new_sel)?;
                passthroughs.push((child, smallvec![new_sel]));
            }
            // A join / set-op operand must be a named relation: wrap `child` in a
            // single-alias subquery that preserves its output columns.
            Placement::NewSubquery { parent, child } => {
                let filter = self.and_of(clauses)?;
                let new_sel = self.add_select(child, filter)?;
                let alias = self
                    .scan_name(RelColumn::new(child, 0))?
                    .map(smol_str::ToSmolStr::to_smolstr);
                // SELECT * preserves duplicate output names without ambiguous
                // column references. Keep the shard column for routing.
                let new_proj = self.add_proj_star(new_sel, true)?;
                let sub_query = self.add_sub_query(new_proj, alias.as_deref())?;
                self.splice_over(parent, child, sub_query)?;
                passthroughs.push((child, smallvec![new_sel, new_proj, sub_query]));
            }
        }
        Ok(())
    }

    /// Relink `parent` from `child` to `spliced` (the top of the inserted chain).
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
        Ok(())
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

    /// Plan equalities to a class's pin. Every lowest column member requires
    /// a placement; parameter checks without one are skipped.
    /// Return `None` when no predicates are needed.
    ///
    /// # Errors
    /// - Reading an output column fails.
    /// - A lowest member has no placement.
    fn plan_class_star<'f>(
        &self,
        facts: &'f EqualityFacts,
        class: &'f EquivalenceClass,
        pin: ClassPin<'f>,
        parents: &AHashMap<NodeId, NodeId>,
        lift_index: &mut AHashMap<NodeId, AHashMap<RelColumn, usize>>,
        param_check_seen: &mut AHashSet<(u16, ClassPin<'f>, NodeId)>,
    ) -> Result<Option<Star>, SbroadError> {
        let member_set: AHashSet<Slot> = class.members.iter().copied().collect();
        // Distinct lowest members cannot converge: each lifted slot has one source.
        let mut cols: Vec<(Placement, Slot)> = Vec::new();
        for &member in class.members.iter() {
            if !self.is_lowest_member(member, &member_set)? {
                continue;
            }
            let Some((placement, slot)) =
                self.place_column(member, &member_set, parents, lift_index)?
            else {
                // Each lowest member must reach a restricting WHERE or INNER
                // JOIN ON through same-column members, unless an earlier
                // insertion is possible.
                debug_assert!(
                    false,
                    "enrich_restrictions: no placement for class member {member:?}"
                );
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some(smol_str::ToSmolStr::to_smolstr(&format!(
                        "enrich_restrictions: no placement for class member {member:?}"
                    ))),
                ));
            };
            cols.push((placement, slot));
        }
        // One check per (param, pin, anchor); the pin param itself needs none.
        let mut params: Vec<(u16, DerivedType)> = Vec::new();
        for &param in &class.params {
            if pin != ClassPin::Param(param) && param_check_seen.insert((param, pin, class.anchor))
            {
                params.push((param, facts.param_type(param)));
            }
        }
        let checks = if params.is_empty() {
            None
        } else {
            self.place_param_check(class.anchor)?
                .map(|placement| (placement, params))
        };
        if cols.is_empty() && checks.is_none() {
            return Ok(None);
        }
        let pin = match pin {
            ClassPin::Const(value) => Pin::Const(value.clone()),
            ClassPin::Param(idx) => Pin::Param(idx, facts.param_type(idx)),
        };
        Ok(Some(Star {
            pin,
            cols,
            params: checks,
        }))
    }

    /// Pair lowest members that share a relation and placement with the first
    /// member of their group. Skip members without a valid placement.
    ///
    /// # Errors
    /// - Reading an output column fails.
    fn plan_class_pairs(
        &self,
        class: &EquivalenceClass,
        parents: &AHashMap<NodeId, NodeId>,
        lift_index: &mut AHashMap<NodeId, AHashMap<RelColumn, usize>>,
        pairs: &mut Vec<ColPair>,
    ) -> Result<(), SbroadError> {
        let member_set: AHashSet<Slot> = class.members.iter().copied().collect();
        // Use the first member as each group's centre. Sorted members and
        // lookup-only map access keep the predicate order deterministic.
        let mut centres: AHashMap<(Placement, NodeId), usize> = AHashMap::new();
        for &member in class.members.iter() {
            if !self.is_lowest_member(member, &member_set)? {
                continue;
            }
            let Some((placement, slot)) =
                self.place_column(member, &member_set, parents, lift_index)?
            else {
                continue;
            };
            match centres.entry((placement, slot.rel_id)) {
                Entry::Occupied(centre) => pairs.push(ColPair {
                    placement,
                    rel_id: slot.rel_id,
                    left: *centre.get(),
                    right: slot.pos,
                }),
                Entry::Vacant(centre) => {
                    centre.insert(slot.pos);
                }
            }
        }
        Ok(())
    }

    /// Place `false` in each unsatisfiable Selection filter or INNER JOIN
    /// condition. Class-level contradictions are handled by `plan_enrichment`.
    ///
    /// # Errors
    /// - Reading a relational node fails.
    fn dead_rel_markers(&self, facts: &EqualityFacts) -> Result<Vec<Placement>, SbroadError> {
        let dead = facts.dead_rels();
        let mut empty: Vec<Placement> = Vec::with_capacity(dead.len());
        for &rel in dead {
            match self.get_relation_node(rel)? {
                Relational::Selection(_) => empty.push(Placement::ExistingSelection(rel)),
                Relational::Join(Join {
                    kind: JoinKind::Inner,
                    ..
                }) => empty.push(Placement::InnerJoinOn(rel)),
                // `apply_expr_facts` runs only for these two.
                _ => debug_assert!(false, "dead rel {rel:?} is no Selection or INNER Join"),
            }
        }
        Ok(empty)
    }

    /// Check that the column's source is not already a class member.
    /// Compare both relation and position: columns of a LEFT JOIN can have
    /// different lower bounds depending on which side they belong to.
    ///
    /// # Errors
    /// - Reading an output column fails.
    fn is_lowest_member(
        &self,
        member: Slot,
        member_set: &AHashSet<Slot>,
    ) -> Result<bool, SbroadError> {
        let source = self.column_source(RelColumn::new(member.rel_id, member.pos))?;
        Ok(!source.is_some_and(|s| member_set.contains(&Slot::new(s.rel_id, s.position))))
    }

    /// Where a `col = pin` clause for class member `slot` attaches, and the slot it
    /// references. Walks up from the deepest copy: at each level prefer an existing
    /// `WHERE`, then a new filter (plain `Selection` or carrier), then an
    /// INNER join's own `ON`; failing all three, lift the same column to the
    /// parent's output and retry, staying on class members so a pushdown barrier
    /// (no member above) stops the lift. `lift_index` caches each parent's first
    /// output position per source column across calls. `None` when nothing hosts it.
    fn place_column(
        &self,
        mut slot: Slot,
        members: &AHashSet<Slot>,
        parents: &AHashMap<NodeId, NodeId>,
        lift_index: &mut AHashMap<NodeId, AHashMap<RelColumn, usize>>,
    ) -> Result<Option<(Placement, Slot)>, SbroadError> {
        loop {
            let Some(&parent) = parents.get(&slot.rel_id) else {
                return Ok(None);
            };
            if matches!(self.get_relation_node(parent)?, Relational::Selection(_)) {
                return Ok(Some((Placement::ExistingSelection(parent), slot)));
            }
            if let Some(placement) = self.new_filter_placement(parent, slot.rel_id)? {
                return Ok(Some((placement, slot)));
            }
            if matches!(
                self.get_relation_node(parent)?,
                Relational::Join(Join {
                    kind: JoinKind::Inner,
                    ..
                })
            ) {
                return Ok(Some((Placement::InnerJoinOn(parent), slot)));
            }
            // Find this same column on the parent's output and retry there; stop if
            // it is not carried through, or the lifted copy leaves the class.
            if !lift_index.contains_key(&parent) {
                let width = self.columns_len(parent)?;
                let mut index: AHashMap<RelColumn, usize> = AHashMap::with_capacity(width);
                // Choose the first output copy of each source independently of
                // class membership, which is checked after the lookup.
                for pos in 0..width {
                    if let Some(source) = self.column_source(RelColumn::new(parent, pos))? {
                        index.entry(source).or_insert(pos);
                    }
                }
                lift_index.insert(parent, index);
            }
            let source = RelColumn::new(slot.rel_id, slot.pos);
            let Some(&up_pos) = lift_index[&parent].get(&source) else {
                return Ok(None);
            };
            let up = Slot::new(parent, up_pos);
            if !members.contains(&up) {
                return Ok(None);
            }
            slot = up;
        }
    }

    /// Choose a placement for a new filter on the edge `parent -> child`:
    /// a plain `Selection` on a `Projection`'s direct input, or a `SELECT *`
    /// carrier under a join or set operation when `child` is not a join.
    /// Return `None` for unsupported edges.
    fn new_filter_placement(
        &self,
        parent: NodeId,
        child: NodeId,
    ) -> Result<Option<Placement>, SbroadError> {
        Ok(match self.get_relation_node(parent)? {
            // A new Selection is supported only on the projection's direct input.
            Relational::Projection(projection) => (projection.child == Some(child))
                .then_some(Placement::NewSelection { parent, child }),
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

    /// Where a column-less `$p = pin` check attaches within the class's domain
    /// rooted at `anchor`. Prefers an existing `WHERE`, then a new `Selection`
    /// under a `Projection`, and only failing both an immediate INNER join's `ON`.
    /// `None` when there is no such spot. The class's original equalities still
    /// enforce the omitted check.
    ///
    /// Binding folds constant checks. A false Selection filter or INNER JOIN
    /// condition allows bucket pruning.
    fn place_param_check(&self, anchor: NodeId) -> Result<Option<Placement>, SbroadError> {
        // Step through an optional subquery body, then an optional Projection.
        let root = match self.get_relation_node(anchor)? {
            Relational::ScanSubQuery(ScanSubQuery { child, .. }) => *child,
            _ => anchor,
        };
        let (projection, inner) = match self.get_relation_node(root)? {
            Relational::Projection(Projection {
                child: Some(child), ..
            }) => (Some(root), *child),
            _ => (None, root),
        };
        let inner_node = self.get_relation_node(inner)?;
        // Prefer an existing WHERE to any other spot.
        if matches!(inner_node, Relational::Selection(Selection { .. })) {
            return Ok(Some(Placement::ExistingSelection(inner)));
        }
        // Then a plain Selection spliced on the Projection's input edge.
        if let Some(projection) = projection {
            if let Some(placement) = self.new_filter_placement(projection, inner)? {
                return Ok(Some(placement));
            }
        }
        // Only failing both, an immediate INNER JOIN's ON. Reached when no
        // Projection hosts a new Selection above the join.
        if matches!(
            inner_node,
            Relational::Join(Join {
                kind: JoinKind::Inner,
                ..
            })
        ) {
            return Ok(Some(Placement::InnerJoinOn(inner)));
        }
        Ok(None)
    }

    /// Child -> parent map over the subtree at `top_id`. `rel_iter` visits
    /// referenced subquery bodies too, so a scan inside an `IN` body resolves to
    /// its own enclosing filter.
    fn rel_parents(&self, top_id: NodeId) -> AHashMap<NodeId, NodeId> {
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

    /// Derived type of the output column at `(rel, pos)`, from the column
    /// expression, so it holds for any relational node, not just a scan.
    fn output_col_type(&self, rel: NodeId, pos: usize) -> Result<DerivedType, SbroadError> {
        Ok(self.column_at(RelColumn::new(rel, pos))?.r#type)
    }

    fn make_pin(&mut self, pin: &Pin) -> NodeId {
        match pin {
            Pin::Const(value) => self.add_const(value.clone()),
            Pin::Param(index, ty) => self.add_param(*index, *ty),
        }
    }

    fn make_pin_eq(
        &mut self,
        rel_id: NodeId,
        pos: usize,
        pin: &Pin,
    ) -> Result<NodeId, SbroadError> {
        let col_type = self.output_col_type(rel_id, pos)?;
        // TODO: should we save is_system flag
        let ref_id =
            self.nodes
                .add_ref(ReferenceTarget::Single(rel_id), pos, col_type, None, false);
        let pin_id = self.make_pin(pin);
        self.add_bool(ref_id, Bool::Eq, pin_id)
    }

    /// Build a detached `Reference(rel, left) = Reference(rel, right)`. Both
    /// columns sit on the same node; each keeps its own column type.
    fn make_col_eq(
        &mut self,
        rel_id: NodeId,
        left: usize,
        right: usize,
    ) -> Result<NodeId, SbroadError> {
        let left_type = self.output_col_type(rel_id, left)?;
        let right_type = self.output_col_type(rel_id, right)?;
        let left_id = self.nodes.add_ref(
            ReferenceTarget::Single(rel_id),
            left,
            left_type,
            None,
            false,
        );
        let right_id = self.nodes.add_ref(
            ReferenceTarget::Single(rel_id),
            right,
            right_type,
            None,
            false,
        );
        self.add_bool(left_id, Bool::Eq, right_id)
    }

    /// Build `Parameter($param) = <pin>` as a detached, column-less SQL conjunct.
    fn make_param_check(
        &mut self,
        param: u16,
        param_type: DerivedType,
        pin: &Pin,
    ) -> Result<NodeId, SbroadError> {
        let pin_id = self.make_pin(pin);
        let param_id = self.add_param(param, param_type);
        self.add_bool(param_id, Bool::Eq, pin_id)
    }
}
