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
//! Only equalities that hold for every row are used; an outer join's `ON` is left
//! out, as it holds only for matched rows.

use crate::errors::{Entity, SbroadError};
use crate::ir::columns::RelColumn;
use crate::ir::helpers::RepeatableState;
use crate::ir::node::relational::{MutRelational, Relational};
use crate::ir::node::{Join, NodeId, Projection, ReferenceTarget, ScanSubQuery, Selection};
use crate::ir::operator::{Bool, JoinKind};
use crate::ir::transformation::equality_facts::{ClassPin, Slot};
use crate::ir::tree::traversal::REL_CAPACITY;
use crate::ir::types::DerivedType;
use crate::ir::value::Value;
use crate::ir::Plan;
use ahash::{AHashMap, AHashSet};
use smallvec::{smallvec, SmallVec};

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

impl Plan {
    /// Enrich the query with the equality conditions its pinned classes imply.
    /// No-op unless facts, restrictions, and at least one pin all exist.
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
        let has_pins = self
            .facts
            .as_ref()
            .is_some_and(|facts| facts.pinned_classes().next().is_some());
        if !(has_pins && self.restrictions.is_some()) {
            return Ok(self);
        }

        // Three phases, kept apart so the search never depends on the order of
        // already-inserted nodes: plan where each class emits (reads the tree),
        // build the predicate expressions, then apply the relational changes.
        let parents = self.rel_parents(top_id);
        let planned = self.collect_class_predicates(&parents)?;
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

    /// Build the derived predicate nodes for every planned [`Star`], grouped by the
    /// [`Placement`] each attaches through. Only expression nodes are built here, so
    /// the reads that resolved placements stay valid.
    ///
    /// # Errors
    /// - Reading an output column or building a predicate fails.
    fn build_predicates(
        &mut self,
        planned: Vec<Star>,
    ) -> Result<AHashMap<Placement, Vec<NodeId>, RepeatableState>, SbroadError> {
        let mut grouped: AHashMap<Placement, Vec<NodeId>, RepeatableState> =
            AHashMap::with_hasher(RepeatableState::default());
        for class in planned {
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

    /// Plan a [`Star`] for each pinned class that needs predicates: select the
    /// columns, parameters, and their placements without modifying the tree.
    ///
    /// # Errors
    /// - Reading an output column fails.
    fn collect_class_predicates(
        &self,
        parents: &AHashMap<NodeId, NodeId>,
    ) -> Result<Vec<Star>, SbroadError> {
        let Some(facts) = self.facts.as_ref() else {
            return Ok(Vec::new());
        };
        // Two domains pinning the same param to the same value are two distinct
        // checks, so key seen checks by anchor too.
        let mut param_check_seen: AHashSet<(u16, ClassPin<'_>, NodeId)> = AHashSet::new();
        let mut planned: Vec<Star> = Vec::new();
        // Reverse output index per parent, built lazily on the first lift through
        // that parent and reused for every class: source column -> first output
        // position carrying it. Without it, lifting `W` columns through one wide
        // node rescans its output each time, an O(W^2) walk. Lives only for this
        // planning phase, before the tree is mutated.
        let mut lift_index: AHashMap<NodeId, AHashMap<RelColumn, usize>> = AHashMap::new();
        // Class ids follow hash-table iteration order, which must not decide the
        // predicate order or which class owns a duplicate param check: order the
        // classes by their stable sources first.
        let mut pinned: Vec<_> = facts
            .pinned_classes()
            .filter(|(_, class)| !class.members.is_empty() || !class.params.is_empty())
            .collect();
        pinned.sort_unstable_by_key(|(_, class)| {
            (
                class.anchor.arena_type,
                class.anchor.offset,
                class.members.as_ref(),
                class.params.as_ref(),
            )
        });
        for (pin, class) in pinned {
            let member_set: AHashSet<Slot> = class.members.iter().copied().collect();
            // Distinct lowest members cannot converge: each lifted slot has one source.
            let mut cols: Vec<(Placement, Slot)> = Vec::new();
            for &member in class.members.iter() {
                // Start from the lowest copy of this column in the class: skip a
                // copy whose own source is already a member. Check the full
                // (rel, position) pair: two columns of one LEFT
                // JOIN can have different lower bounds (a nullable right column
                // stays on the join output while a left one reaches its scan).
                let source = self.column_source(RelColumn::new(member.rel_id, member.pos))?;
                let has_deeper_member =
                    source.is_some_and(|s| member_set.contains(&Slot::new(s.rel_id, s.position)));
                if has_deeper_member {
                    continue;
                }
                let Some((placement, slot)) =
                    self.place_column(member, &member_set, parents, &mut lift_index)?
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
                if pin != ClassPin::Param(param)
                    && param_check_seen.insert((param, pin, class.anchor))
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
                continue;
            }
            let pin = match pin {
                ClassPin::Const(value) => Pin::Const(value.clone()),
                ClassPin::Param(idx) => Pin::Param(idx, facts.param_type(idx)),
            };
            planned.push(Star {
                pin,
                cols,
                params: checks,
            });
        }
        Ok(planned)
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
    /// A `WHERE` is preferred to a join `ON` because a check that folds to a
    /// constant `false` is pruned at the router only inside a `Selection` (the
    /// same `false` folded into a join condition is not).
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
        // Then a plain Selection spliced on the Projection's input edge, so a
        // `false`-folding check can still prune at the router.
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
