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
//! aggregate the fact does not constrain. Each column and each parameter check
//! must have a valid placement; a missing one is an error, never a silently
//! dropped predicate.
//!
//! Parameter checks and the `false` markers below are also recorded as
//! [`DomainCheck`]s of their domain for redistribution: each is necessary for
//! any row of the domain's result, so a FALSE one means the domain is empty.
//! That pass repeats checks on eligible Motion inputs in the same domain
//! when their hosts are outside the input query. The original hosts keep
//! the checks; the records are discarded with the equality facts.
//!
//! An unpinned class derives equalities between columns with the same relation
//! and placement, each paired with the first. Columns of different relations
//! are linked where their branches meet: an INNER join gets a pair between
//! its operands in `ON`; an outer join's nullable output group is linked to
//! a carried member above that join. Each source group is connected internally,
//! and `k` groups get `k - 1` cross-group pairs.
//!
//! An unsatisfiable filter or INNER JOIN condition gets a `false` conjunct.
//! A class with conflicting constants from separate filters gets a `false`
//! predicate within its domain, using the placement rules for parameter checks.
//!
//! Only equalities that hold for every row are used; an outer join's `ON` is left
//! out, as it holds only for matched rows.
//!
//! A class whose clauses are all planned replaces the query's own equalities
//! within it: each [`SourceEquality`] of such a class is dropped from its
//! `WHERE` or INNER `ON`, which keeps its other conjuncts and gains the derived
//! clauses placed there. The derived clauses cover the class within its domain:
//! column conditions follow their members, while column-less checks use a host
//! where failure empties the domain. Everything else stays: an outer join's
//! `ON`, `OR` / `NOT` subtrees, row equalities, casts and other opaque terms,
//! self-comparisons such as `a = a`, the conjuncts of an unsatisfiable condition
//! and every source of a contradictory class. A condition left with no conjunct
//! becomes `true`; the node itself stays.

use crate::errors::{Entity, SbroadError};
use crate::ir::columns::RelColumn;
use crate::ir::helpers::RepeatableState;
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::Relational;
use crate::ir::node::{
    BoolExpr, Join, NodeId, Projection, Reference, ReferenceTarget, ScanSubQuery,
};
use crate::ir::operator::{Bool, JoinKind};
#[cfg(debug_assertions)]
use crate::ir::transformation::equality_facts::UnionFind;
use crate::ir::transformation::equality_facts::{
    ClassId, ClassPin, DomainCheck, DomainCheckKind, EqualityFacts, EquivalenceClass, Pin, Slot,
    SourceEquality,
};
use crate::ir::transformation::placement::{Dropped, Grouped, Placement};
use crate::ir::transformation::restriction::RestrictInfo;
use crate::ir::types::DerivedType;
use crate::ir::value::Value;
use crate::ir::Plan;
use ahash::{AHashMap, AHashSet};
use smallvec::{smallvec, SmallVec};
use std::collections::hash_map::Entry;

/// One `left = right` equality between two columns, derived from an unpinned
/// class and attached through `placement`. Both columns are visible there: they
/// sit on one node, or on the two operands of the INNER join whose `ON` it is.
struct ColPair {
    placement: Placement,
    left: Slot,
    right: Slot,
}

/// One source group of an unpinned class: lowest members on one relation. They
/// share the placement and node they land through; `positions` are theirs there.
struct PairGroup {
    /// The relation every lowest member of the group is on.
    source: NodeId,
    placement: Placement,
    placed: NodeId,
    positions: SmallVec<[usize; 4]>,
}

/// The first stated `ON` pair of each class at each join, see
/// [`Plan::stated_pairs_by_class`]. Indexed lazily, once per join, by the
/// classes of `facts`.
struct StatedPairs<'f> {
    facts: &'f EqualityFacts,
    by_join: AHashMap<NodeId, AHashMap<ClassId, (Slot, Slot)>>,
}

/// The class members one join brings together, see [`Plan::plan_class_pairs`].
#[derive(Default)]
struct JoinSides {
    /// Members on the left operand's output.
    left: SmallVec<[Slot; 2]>,
    /// Members on the right operand's output.
    right: SmallVec<[Slot; 2]>,
    /// Members on the join's own output whose lowest copy is the join itself:
    /// an outer join's nullable-side columns, not linked below it. They form
    /// one source group.
    own: SmallVec<[Slot; 2]>,
    /// Members on the join's own output carried up from an operand.
    carried: SmallVec<[Slot; 2]>,
}

/// A star of equalities connecting columns and parameters to one shared pin.
/// Stores the operands and placements; `build_predicates` creates the expressions.
/// Every lowest column member and every needed parameter check has a placement.
struct Star {
    /// The root of the class's domain.
    anchor: NodeId,
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
    /// `col = col` pairs between members, on one node or where they meet.
    Pairs,
}

/// Choose one action: mark a contradiction, derive equalities to a pin, or
/// derive column pairs. Pins hold throughout the class's domain; facts scoped
/// to a LEFT JOIN's ON are excluded from global classes.
///
/// Pairs require an unpinned class with two members on the same relation:
/// this includes columns from different join operands when the join output
/// carries both. A NULL constant is not a pin and does not permit pair derivation.
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
    /// Each `false` marker's placement and a node of the domain it empties.
    empty: Vec<(Placement, NodeId)>,
    /// Source conjuncts to drop, keyed by the fold placement of their owner.
    replaced: Replaced,
}

/// The [`SourceEquality`] conjuncts each existing filter or INNER `ON` loses.
type Replaced = Dropped;

/// A check of the domain `domain_node` belongs to, planned at `placement`;
/// it becomes a [`DomainCheck`] once that placement has a node.
struct PlacedCheck {
    domain_node: NodeId,
    placement: Placement,
    kind: DomainCheckKind,
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
        let Some(mut planned) = self.plan_enrichment(top_id)? else {
            return Ok(self);
        };
        let replaced = std::mem::take(&mut planned.replaced);
        let (grouped, checks) = self.build_predicates(planned)?;
        // Facts stay valid: the classes are unchanged and the inserted nodes
        // are registered by `apply_placements`.
        // `(owner, clause)` of every derived clause, emitted exactly once.
        // `hosts`: the node each placement materialized as, the host of its checks.
        #[cfg_attr(not(debug_assertions), allow(unused_variables))]
        let (emitted, hosts) = self.apply_placements(grouped, replaced)?;
        #[cfg(debug_assertions)]
        {
            self.debug_assert_restrictions_fresh(top_id);
            self.debug_assert_emitted_once(&emitted);
        }
        if let Some(facts) = self.facts.as_mut() {
            // The conditions they name are rewritten now.
            facts.clear_source_equalities();
            // Save checks and their hosts for redistribution.
            facts.set_domain_checks(
                checks
                    .into_iter()
                    .map(|check| {
                        let host = hosts[&check.placement];
                        (
                            check.domain_node,
                            DomainCheck {
                                kind: check.kind,
                                host,
                            },
                        )
                    })
                    .collect(),
            );
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
        let mut candidates: Vec<(ClassId, &EquivalenceClass, ClassAction<'_>)> = facts
            .classes
            .iter()
            .zip(facts.class_ids())
            .filter_map(|(class, id)| Some((id, class, classify(class)?)))
            .collect();
        if candidates.is_empty() && facts.dead_rels().is_empty() {
            return Ok(None);
        }
        // Sort by stable sources because ClassId order depends on hashing.
        candidates.sort_unstable_by_key(|(_, class, _)| {
            (
                class.anchor.arena_type,
                class.anchor.offset,
                class.members.as_ref(),
                class.params.as_ref(),
            )
        });

        // Only column placement needs a parent map.
        let parents = if candidates.iter().any(|(_, class, action)| {
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
        // Each INNER join's stated `ON` pairs, parsed and indexed by class
        // once per join and shared across classes.
        // Valid only during planning, before the tree changes.
        let mut stated_pairs = StatedPairs {
            facts,
            by_join: AHashMap::new(),
        };
        // A param is interned per domain, so it joins one class there, and
        // each domain has its own root: one check per class never repeats.
        #[cfg(debug_assertions)]
        {
            let mut seen: AHashSet<(u16, NodeId)> = AHashSet::new();
            for (_, class, _) in &candidates {
                for &param in &class.params {
                    debug_assert!(
                        seen.insert((param, class.anchor)),
                        "enrich_restrictions: param {param} in two classes at {:?}",
                        class.anchor
                    );
                }
            }
        }
        let mut planned = Enrichment {
            empty: self.dead_rel_markers(facts)?,
            ..Enrichment::default()
        };
        // Classes whose every clause is planned. Only these replace sources;
        // a class `classify` skips is not complete.
        let mut complete: AHashSet<ClassId> = AHashSet::new();
        for (class_id, class, action) in candidates {
            match action {
                // Its sources stay: the `false` alone would also do, but its
                // placement is not proven to cover every source yet.
                ClassAction::Contradiction => {
                    let placement = self.required_domain_check(facts, class, None)?;
                    planned.empty.push((placement, class.anchor));
                    continue;
                }
                ClassAction::Pinned(pin) => {
                    let star =
                        self.plan_class_star(facts, class, pin, &parents, &mut lift_index)?;
                    planned.stars.extend(star);
                }
                ClassAction::Pairs => {
                    self.plan_class_pairs(
                        class_id,
                        class,
                        &parents,
                        &mut lift_index,
                        &mut stated_pairs,
                        &mut planned.pairs,
                    )?;
                }
            }
            // Every failure above is an error, so reaching here means the
            // class is fully planned.
            complete.insert(class_id);
        }
        planned.replaced = self.replaced_sources(facts.source_equalities(), &complete)?;
        Ok(Some(planned))
    }

    /// Check that emitted clause IDs are unique and occur in restrictions at
    /// their expected owners. This checks materialization by identity, not
    /// semantic deduplication; the provenance is local to this debug check.
    #[cfg(debug_assertions)]
    fn debug_assert_emitted_once(&self, emitted: &[(NodeId, NodeId)]) {
        let Some(restrictions) = self.restrictions.as_ref() else {
            return;
        };
        let owners: AHashMap<NodeId, NodeId> = emitted
            .iter()
            .map(|&(owner, clause)| (clause, owner))
            .collect();
        debug_assert_eq!(owners.len(), emitted.len(), "a clause emitted twice");
        let mut seen = 0;
        for (rel_id, restriction) in &restrictions.by_rel {
            for info in restriction.clauses() {
                if let Some(owner) = owners.get(&info.clause()) {
                    debug_assert_eq!(owner, rel_id, "a derived clause left its owner");
                    seen += 1;
                }
            }
        }
        debug_assert_eq!(seen, emitted.len(), "a derived clause is no restriction");
    }

    /// Group the sources of `complete` classes by the fold placement of their
    /// owner.
    ///
    /// # Errors
    /// - Reading a relational node fails.
    fn replaced_sources(
        &self,
        sources: &[SourceEquality],
        complete: &AHashSet<ClassId>,
    ) -> Result<Replaced, SbroadError> {
        let mut replaced = Replaced::new();
        for source in sources {
            if !complete.contains(&source.class_id) {
                continue;
            }
            // The analysis records sources of Selections and INNER joins only.
            let placement = self.fold_placement(source.owner_rel)?.unwrap_or_else(|| {
                unreachable!(
                    "source owner {:?} is no Selection or INNER Join",
                    source.owner_rel
                )
            });
            replaced
                .entry(placement)
                .or_default()
                .insert(source.expr_id);
        }
        Ok(replaced)
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
            replaced: _,
        }: Enrichment,
    ) -> Result<(Grouped, Vec<PlacedCheck>), SbroadError> {
        let mut grouped = Grouped::with_hasher(RepeatableState);
        let mut checks: Vec<PlacedCheck> = Vec::new();
        for class in stars {
            // Param checks before column pins, for a stable rendered conjunct order.
            if let Some((placement, params)) = class.params {
                for (param, param_type) in params {
                    let check_id = self.make_param_check(param, param_type, &class.pin)?;
                    grouped
                        .entry(placement)
                        .or_default()
                        .push(RestrictInfo::implied(check_id, SmallVec::new()));
                    checks.push(PlacedCheck {
                        domain_node: class.anchor,
                        placement,
                        kind: DomainCheckKind::ParamEq {
                            param,
                            param_type,
                            pin: class.pin.clone(),
                        },
                    });
                }
            }
            for &(placement, slot) in &class.cols {
                let eq_id = self.make_pin_eq(slot.rel_id, slot.pos, &class.pin)?;
                grouped
                    .entry(placement)
                    .or_default()
                    .push(RestrictInfo::implied(eq_id, smallvec![slot]));
            }
        }
        for pair in pairs {
            let eq_id = self.make_col_eq(pair.left, pair.right)?;
            grouped
                .entry(pair.placement)
                .or_default()
                .push(RestrictInfo::implied(
                    eq_id,
                    smallvec![pair.left, pair.right],
                ));
        }
        for (placement, domain_node) in empty {
            let false_id = self.add_const(Value::Boolean(false));
            grouped
                .entry(placement)
                .or_default()
                .push(RestrictInfo::implied(false_id, SmallVec::new()));
            checks.push(PlacedCheck {
                domain_node,
                placement,
                kind: DomainCheckKind::AlwaysFalse,
            });
        }
        Ok((grouped, checks))
    }

    /// Plan equalities to a class's pin. Every lowest column member and every
    /// parameter check requires a placement.
    /// Return `None` when no predicates are needed.
    ///
    /// # Errors
    /// - Reading an output column fails.
    /// - A lowest member or a parameter check has no placement.
    fn plan_class_star<'f>(
        &self,
        facts: &'f EqualityFacts,
        class: &'f EquivalenceClass,
        pin: ClassPin<'f>,
        parents: &AHashMap<NodeId, NodeId>,
        lift_index: &mut AHashMap<NodeId, AHashMap<RelColumn, usize>>,
    ) -> Result<Option<Star>, SbroadError> {
        let member_set: AHashSet<Slot> = class.members.iter().copied().collect();
        // Distinct lowest members cannot converge: each lifted slot has one source.
        let mut cols: Vec<(Placement, Slot)> = Vec::new();
        for &member in class.members.iter() {
            if !self.is_lowest_member(member, &member_set)? {
                continue;
            }
            let (placement, slot) =
                self.place_lowest_member(member, &member_set, parents, lift_index)?;
            cols.push((placement, slot));
        }
        // One check per param; the pin param itself needs none.
        let params: Vec<(u16, DerivedType)> = class
            .params
            .iter()
            .filter(|&&param| pin != ClassPin::Param(param))
            .map(|&param| (param, facts.param_type(param)))
            .collect();
        let checks = if params.is_empty() {
            None
        } else {
            let placement = self.required_domain_check(facts, class, Some(pin))?;
            Some((placement, params))
        };
        if cols.is_empty() && checks.is_none() {
            return Ok(None);
        }
        let pin = match pin {
            ClassPin::Const(value) => Pin::Const(value.clone()),
            ClassPin::Param(idx) => Pin::Param(idx, facts.param_type(idx)),
        };
        Ok(Some(Star {
            anchor: class.anchor,
            pin,
            cols,
            params: checks,
        }))
    }

    /// Plan the [`ColPair`]s of an unpinned class: stars within source groups
    /// and links between groups at joins.
    ///
    /// Each lowest member is placed exactly as a pinned class' column. Lowest
    /// members on one relation form a source group, starred around its first
    /// column; they must share a placement. Groups follow where columns come
    /// from, not where their filters land: two groups placed in one `WHERE`
    /// stay apart, so a shared placement never links them ahead of their join.
    ///
    /// Groups merge only at joins, each with one pair per extra side:
    /// - an INNER join with members on both operands takes `l = r` in its `ON`,
    ///   preferring an edge the `ON` already states;
    /// - a join whose own output holds nullable-side copies (the join's own
    ///   source group) ties them to a carried member where a filter over that
    ///   output goes ([`Plan::place_column`]), so a pair never enters the
    ///   nullable side.
    ///
    /// Each side is linked inside already (by its group's star or lower joins)
    /// and the sides hold disjoint groups, so every cross pair links two
    /// components and `k` groups get exactly `k - 1` cross pairs. The choice
    /// reads no other join's result, so the join order only orders the output.
    ///
    /// Edges an `ON` already states are emitted too: the class must yield a
    /// full spanning set on its own, independent of the query's conjuncts.
    ///
    /// # Errors
    /// - Reading an output column or a relational node fails.
    /// - A lowest member has no placement, or two sides do not share one.
    /// - Lowest members on one relation land through different placements.
    fn plan_class_pairs(
        &self,
        class_id: ClassId,
        class: &EquivalenceClass,
        parents: &AHashMap<NodeId, NodeId>,
        lift_index: &mut AHashMap<NodeId, AHashMap<RelColumn, usize>>,
        stated_pairs: &mut StatedPairs<'_>,
        pairs: &mut Vec<ColPair>,
    ) -> Result<(), SbroadError> {
        let member_set: AHashSet<Slot> = class.members.iter().copied().collect();
        let mut lowest_of: AHashMap<Slot, Slot> = AHashMap::with_capacity(member_set.len());
        // Build groups in member order so each group's first column, used as
        // its star centre, is stable.
        let mut groups: Vec<PairGroup> = Vec::new();
        debug_assert!(
            class.members.windows(2).all(|w| w[0] <= w[1]),
            "enrich_restrictions: members of a class are not sorted"
        );
        for &member in class.members.iter() {
            if self.lowest_copy(member, &member_set, &mut lowest_of)? != member {
                continue;
            }
            let (placement, slot) =
                self.place_lowest_member(member, &member_set, parents, lift_index)?;
            // Members are sorted by relation (checked above) and the lowest
            // keep that order, so one relation's members are adjacent: only
            // the last group can match.
            match groups.last_mut().filter(|g| g.source == member.rel_id) {
                Some(group) if group.placement == placement && group.placed == slot.rel_id => {
                    group.positions.push(slot.pos);
                }
                // Same-relation columns climb the same parents, so this only
                // means placement depends on more than the relation.
                Some(_) => {
                    debug_assert!(
                        false,
                        "enrich_restrictions: lowest members on {:?} part ways",
                        member.rel_id
                    );
                    return Err(SbroadError::Invalid(
                        Entity::Plan,
                        Some(smol_str::ToSmolStr::to_smolstr(&format!(
                            "enrich_restrictions: lowest members on {:?} part ways",
                            member.rel_id
                        ))),
                    ));
                }
                None => groups.push(PairGroup {
                    source: member.rel_id,
                    placement,
                    placed: slot.rel_id,
                    positions: smallvec![slot.pos],
                }),
            }
        }
        for group in &groups {
            let Some((&left, others)) = group.positions.split_first() else {
                continue;
            };
            for &right in others {
                pairs.push(ColPair {
                    placement: group.placement,
                    left: Slot::new(group.placed, left),
                    right: Slot::new(group.placed, right),
                });
            }
        }
        if groups.len() < 2 {
            return Ok(());
        }

        // What each join brings together. Members are sorted, so every side
        // lists them in a stable order.
        let mut joins: AHashMap<NodeId, JoinSides> = AHashMap::new();
        for &member in class.members.iter() {
            if let Relational::Join(_) = self.get_relation_node(member.rel_id)? {
                let sides = joins.entry(member.rel_id).or_default();
                if lowest_of[&member] == member {
                    sides.own.push(member);
                } else {
                    sides.carried.push(member);
                }
            }
            let Some(&parent) = parents.get(&member.rel_id) else {
                continue;
            };
            if let Relational::Join(Join { left, right, .. }) = self.get_relation_node(parent)? {
                let sides = joins.entry(parent).or_default();
                if member.rel_id == *left {
                    sides.left.push(member);
                } else if member.rel_id == *right {
                    sides.right.push(member);
                }
            }
        }
        // Any order works, each merge is one join; the ids keep it repeatable.
        let mut order: Vec<NodeId> = joins.keys().copied().collect();
        order.sort_unstable_by_key(|join| (join.arena_type, join.offset));

        #[cfg(debug_assertions)]
        let cross_before = pairs.len();
        for join in order {
            let sides = &joins[&join];
            let Relational::Join(Join { kind, .. }) = self.get_relation_node(join)? else {
                unreachable!("keyed by joins");
            };
            if let (Some(&left_rep), Some(&right_rep)) = (sides.left.first(), sides.right.first()) {
                // An outer join's nullable operand is never linked to the
                // other side, so its members meet only through `own`.
                let inner = matches!(kind, JoinKind::Inner);
                debug_assert!(
                    inner,
                    "enrich_restrictions: class spans both operands of outer join {join:?}"
                );
                if inner {
                    // Prefer an edge the `ON` already states: any one will do.
                    let stated = match stated_pairs.by_join.entry(join) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => {
                            entry.insert(self.stated_pairs_by_class(stated_pairs.facts, join)?)
                        }
                    };
                    let (left, right) = stated
                        .get(&class_id)
                        .copied()
                        .unwrap_or((left_rep, right_rep));
                    debug_assert!(
                        member_set.contains(&left) && member_set.contains(&right),
                        "enrich_restrictions: stated pair of {join:?} leaves the class"
                    );
                    pairs.push(ColPair {
                        placement: Placement::InnerJoinOn(join),
                        left,
                        right,
                    });
                }
            }
            if let (Some(&carried), Some(&own)) = (sides.carried.first(), sides.own.first()) {
                pairs.push(self.output_pair(carried, own, &member_set, parents, lift_index)?);
            }
        }
        #[cfg(debug_assertions)]
        {
            // Independently validate the structural emission: cross pairs must
            // connect every source group without cycles. This catches missing
            // or redundant edges even while the original query predicates
            // still mask such mistakes. The union-find only checks the chosen
            // pairs; it never selects them.
            let group_index: AHashMap<NodeId, usize> = groups
                .iter()
                .enumerate()
                .map(|(index, group)| (group.source, index))
                .collect();
            let group = |slot: Slot| group_index[&lowest_of[&slot].rel_id];
            let mut linked = UnionFind::with_capacity(groups.len());
            for index in 0..groups.len() {
                linked.add(index);
            }
            // Both sides of every pair are members: placement lifts through
            // members only.
            for pair in &pairs[cross_before..] {
                let (left, right) = (group(pair.left), group(pair.right));
                debug_assert_ne!(
                    linked.find(&left),
                    linked.find(&right),
                    "enrich_restrictions: cross pairs of {:?} close a cycle",
                    class.members
                );
                linked.union(&left, &right);
            }
            debug_assert_eq!(
                linked.groups_number(),
                1,
                "enrich_restrictions: joins do not span the groups of {:?}",
                class.members
            );
        }
        Ok(())
    }

    /// A pair of two members on one node's output, placed where a filter over
    /// that output goes. Both climb the same parents, so they share a placement.
    ///
    /// # Errors
    /// - Reading an output column fails.
    /// - A member has no placement, or the two do not share one.
    fn output_pair(
        &self,
        left: Slot,
        right: Slot,
        member_set: &AHashSet<Slot>,
        parents: &AHashMap<NodeId, NodeId>,
        lift_index: &mut AHashMap<NodeId, AHashMap<RelColumn, usize>>,
    ) -> Result<ColPair, SbroadError> {
        let (placement, left) = self.place_lowest_member(left, member_set, parents, lift_index)?;
        let (right_placement, right) =
            self.place_lowest_member(right, member_set, parents, lift_index)?;
        if right_placement != placement || right.rel_id != left.rel_id {
            debug_assert!(
                false,
                "enrich_restrictions: {left:?} and {right:?} part ways"
            );
            return Err(SbroadError::Invalid(
                Entity::Plan,
                Some(smol_str::ToSmolStr::to_smolstr(&format!(
                    "enrich_restrictions: {left:?} and {right:?} part ways"
                ))),
            ));
        }
        Ok(ColPair {
            placement,
            left,
            right,
        })
    }

    /// For each class, the first column equality `join`'s `ON` states between
    /// its two operands with both ends in that class, as `(left operand column,
    /// right operand column)`. Read in conjunct order from the join's
    /// restriction, so a class finds its stated edge without a scan. Empty when
    /// no pair fits, which still marks the join as parsed.
    ///
    /// # Errors
    /// - The INNER join has no restriction.
    /// - Reading a node fails.
    fn stated_pairs_by_class(
        &self,
        facts: &EqualityFacts,
        join: NodeId,
    ) -> Result<AHashMap<ClassId, (Slot, Slot)>, SbroadError> {
        let mut first_by_class = AHashMap::new();
        let Relational::Join(Join { left, right, .. }) = self.get_relation_node(join)? else {
            return Ok(first_by_class);
        };
        let column = |id: NodeId| -> Result<Option<Slot>, SbroadError> {
            Ok(match self.get_expression_node(id)? {
                Expression::Reference(Reference {
                    target: ReferenceTarget::Single(rel),
                    position,
                    ..
                }) => Some(Slot::new(*rel, *position)),
                _ => None,
            })
        };
        // Slots alone do not tell an equality from an `OR`, a cast or another
        // term over the same columns: read each clause itself.
        for info in self.owner_restriction(join)?.clauses() {
            let Expression::Bool(BoolExpr {
                op: Bool::Eq,
                left: l,
                right: r,
            }) = self.get_expression_node(info.clause())?
            else {
                continue;
            };
            let (Some(l), Some(r)) = (column(*l)?, column(*r)?) else {
                continue;
            };
            let (l, r) = if l.rel_id == *left && r.rel_id == *right {
                (l, r)
            } else if l.rel_id == *right && r.rel_id == *left {
                (r, l)
            } else {
                continue;
            };
            let Some(class_id) = facts.class_of_slot(l.rel_id, l.pos) else {
                continue;
            };
            if facts.class_of_slot(r.rel_id, r.pos) == Some(class_id) {
                // The first in conjunct order wins.
                first_by_class.entry(class_id).or_insert((l, r));
            }
        }
        Ok(first_by_class)
    }

    /// The lowest copy of `member`'s column in the class: follow each copy's
    /// source while it is still a member. Memoized in `lowest_of` for every
    /// copy on the way.
    ///
    /// # Errors
    /// - Reading an output column fails.
    fn lowest_copy(
        &self,
        member: Slot,
        member_set: &AHashSet<Slot>,
        lowest_of: &mut AHashMap<Slot, Slot>,
    ) -> Result<Slot, SbroadError> {
        let mut chain: SmallVec<[Slot; 4]> = SmallVec::new();
        let mut current = member;
        let lowest = loop {
            if let Some(&lowest) = lowest_of.get(&current) {
                break lowest;
            }
            chain.push(current);
            match self.column_source(RelColumn::new(current.rel_id, current.pos))? {
                Some(source) if member_set.contains(&Slot::new(source.rel_id, source.position)) => {
                    current = Slot::new(source.rel_id, source.position);
                }
                _ => break current,
            }
        };
        for slot in chain {
            lowest_of.insert(slot, lowest);
        }
        Ok(lowest)
    }

    /// Place `false` in each unsatisfiable Selection filter or INNER JOIN
    /// condition, with the relation itself as the node of the domain it
    /// empties: the relation's output is empty, and an empty node of a domain
    /// empties its root (see `EqualityFacts::same_domain`); never a parent
    /// domain. Class-level contradictions are handled by `plan_enrichment`.
    ///
    /// # Errors
    /// - Reading a relational node fails.
    fn dead_rel_markers(
        &self,
        facts: &EqualityFacts,
    ) -> Result<Vec<(Placement, NodeId)>, SbroadError> {
        let dead = facts.dead_rels();
        let mut empty: Vec<(Placement, NodeId)> = Vec::with_capacity(dead.len());
        for &rel in dead {
            match self.get_relation_node(rel)? {
                Relational::Selection(_) => empty.push((Placement::ExistingSelection(rel), rel)),
                Relational::Join(Join {
                    kind: JoinKind::Inner,
                    ..
                }) => empty.push((Placement::InnerJoinOn(rel), rel)),
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

    /// [`Plan::place_column`] for a lowest class member, which always has a
    /// placement: it is a member only because a restricting WHERE or INNER JOIN
    /// ON mentions its column, and same-column members lead up to it.
    ///
    /// # Errors
    /// - Reading an output column fails.
    /// - The member has no placement (a broken invariant).
    fn place_lowest_member(
        &self,
        member: Slot,
        member_set: &AHashSet<Slot>,
        parents: &AHashMap<NodeId, NodeId>,
        lift_index: &mut AHashMap<NodeId, AHashMap<RelColumn, usize>>,
    ) -> Result<(Placement, Slot), SbroadError> {
        let placed = self.place_column(member, member_set, parents, lift_index)?;
        debug_assert!(
            placed.is_some(),
            "enrich_restrictions: no placement for class member {member:?}"
        );
        placed.ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Plan,
                Some(smol_str::ToSmolStr::to_smolstr(&format!(
                    "enrich_restrictions: no placement for class member {member:?}"
                ))),
            )
        })
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
            // An `IN` / scalar subquery body hangs under the filter using it, but
            // is its own domain: its output is no column of that filter.
            if self
                .get_relation_node(parent)?
                .subqueries()
                .iter()
                .any(|&subquery| subquery == slot.rel_id)
            {
                return Ok(None);
            }
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

    /// [`Plan::place_domain_check`] for a check the class needs: a `$p = pin`
    /// check of a pinned class (`pin` is set) or the `false` of a contradictory
    /// one. Such a class must have a host reachable by the domain-check walk;
    /// failing to find one is an invariant violation, not a reason to omit it.
    ///
    /// # Errors
    /// - Reading a relational node fails.
    /// - The check has no placement (a broken invariant).
    fn required_domain_check(
        &self,
        facts: &EqualityFacts,
        class: &EquivalenceClass,
        pin: Option<ClassPin<'_>>,
    ) -> Result<Placement, SbroadError> {
        if let Some(placement) = self.place_domain_check(facts, class.anchor)? {
            return Ok(placement);
        }
        Err(SbroadError::Invalid(
            Entity::Plan,
            Some(smol_str::ToSmolStr::to_smolstr(&format!(
                "enrich_restrictions: no placement for a check of class at anchor {:?} \
                 (pin {pin:?}, params {:?}, members {:?})",
                class.anchor, class.params, class.members
            ))),
        ))
    }

    /// Where a column-less check (`$p = pin`, or the `false` of a
    /// contradictory class) attaches within the domain rooted at `anchor`.
    ///
    /// The contract: a failing check at the chosen node empties the whole
    /// domain, and a passing one changes nothing. So the walk goes down from
    /// `anchor` only through edges that carry emptiness up, listed here
    /// explicitly rather than inferred from the analysis' domains (which only
    /// back the walk up as a guard):
    /// - a Selection hosts the check in its filter;
    /// - an INNER join hosts it in its `ON`;
    /// - a LEFT JOIN continues to its preserved (left) operand only. Its own
    ///   `ON` never hosts: a failing check there null-extends the preserved
    ///   rows instead of removing them. The nullable operand is another domain;
    /// - a plain Projection (no aggregate, window, `DISTINCT`) hosts in an
    ///   existing `WHERE` below it, else in a new Selection on its input edge,
    ///   before any `ON` deeper down;
    /// - a pass-through subquery continues to its body.
    ///
    /// Anything else (an aggregate, a set operation, an expression subquery,
    /// a CTE body) ends the walk with `None`.
    ///
    /// Binding folds constant checks. A false Selection filter or INNER JOIN
    /// condition allows bucket pruning.
    ///
    /// # Errors
    /// - Reading a relational node fails.
    fn place_domain_check(
        &self,
        facts: &EqualityFacts,
        anchor: NodeId,
    ) -> Result<Option<Placement>, SbroadError> {
        let mut node = anchor;
        loop {
            if !facts.same_domain(anchor, node) {
                debug_assert!(
                    false,
                    "enrich_restrictions: {node:?} left the domain of {anchor:?}"
                );
                return Ok(None);
            }
            node = match self.get_relation_node(node)? {
                Relational::Selection(_) => return Ok(Some(Placement::ExistingSelection(node))),
                Relational::Join(Join {
                    kind: JoinKind::Inner,
                    ..
                }) => return Ok(Some(Placement::InnerJoinOn(node))),
                Relational::Join(Join {
                    kind: JoinKind::LeftOuter,
                    left,
                    ..
                }) => *left,
                Relational::Projection(Projection {
                    child: Some(child),
                    output,
                    windows,
                    is_distinct,
                    group_by: None,
                    having: None,
                    ..
                }) if windows.is_empty()
                    && !is_distinct
                    && !self.contains_aggregates(*output, true)? =>
                {
                    if matches!(self.get_relation_node(*child)?, Relational::Selection(_)) {
                        return Ok(Some(Placement::ExistingSelection(*child)));
                    }
                    return self.new_filter_placement(node, *child);
                }
                Relational::ScanSubQuery(ScanSubQuery { child, .. })
                    if facts.same_domain(node, *child) =>
                {
                    *child
                }
                _ => return Ok(None),
            };
        }
    }

    /// Derived type of the output column at `(rel, pos)`, from the column
    /// expression, so it holds for any relational node, not just a scan.
    fn output_col_type(&self, rel: NodeId, pos: usize) -> Result<DerivedType, SbroadError> {
        Ok(self.column_at(RelColumn::new(rel, pos))?.r#type)
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

    /// Build a detached `Reference(left) = Reference(right)`: two columns on one
    /// node, or on the two operands of an INNER join. Each keeps its own type.
    fn make_col_eq(&mut self, left: Slot, right: Slot) -> Result<NodeId, SbroadError> {
        let left_type = self.output_col_type(left.rel_id, left.pos)?;
        let right_type = self.output_col_type(right.rel_id, right.pos)?;
        let left_id = self.nodes.add_ref(
            ReferenceTarget::Single(left.rel_id),
            left.pos,
            left_type,
            None,
            false,
        );
        let right_id = self.nodes.add_ref(
            ReferenceTarget::Single(right.rel_id),
            right.pos,
            right_type,
            None,
            false,
        );
        self.add_bool(left_id, Bool::Eq, right_id)
    }
}
