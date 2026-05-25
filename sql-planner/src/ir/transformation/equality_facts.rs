//! Equality-class analysis.
//!
//! # Mental model
//!
//! The pass does not try to rewrite the boolean tree directly. Instead it
//! builds a compact table of facts of the form:
//!
//! - "this output slot is equal to that output slot";
//! - "this output slot is equal to this constant".
//!
//! The key point is that facts are stored per relational output slot, not per
//! expression node. A class member is therefore identified by:
//!
//! - the relational node that owns the output row;
//! - the zero-based output position inside that row;
//! - the join domain in which the equality is known to hold.
//!
//! Thinking in slots gives the rest of the planner a stable lookup key even
//! after predicates are normalized or projected through aliases.
//!
//! A concrete example:
//!
//! ```text
//! a.id = b.id AND b.id = 1
//! ```
//!
//! produces a single class containing:
//!
//! - slot `(a, id)`
//! - slot `(b, id)`
//! - constant `1`
//!
//! so later consumers can cheaply answer "is `a.id` fixed to a constant?" or
//! "do `a.id` and `b.id` belong to the same class?" without reparsing the
//! original predicate.
//!
//! # Why join domains exist
//!
//! Equality does not hold uniformly across the whole plan tree. The pass keeps
//! separate [`DomainId`] values across semantic boundaries:
//!
//! - `INNER JOIN` keeps both sides in the same domain;
//! - the nullable side of `LEFT JOIN` gets a fresh domain;
//! - safe `ScanSubQuery` nodes may pass facts through unchanged;
//! - `ScanCte`, `Motion`, `Limit`, `OrderBy`, `GroupBy`, `Having`, and set
//!   operations open fresh domains and therefore stop propagation.
//!
//! This prevents the analyzer from unifying slots merely because they look
//! similar syntactically when SQL semantics say the equality is not guaranteed
//! to hold in the parent query block.
//!
//! # One domain per NodeId
//!
//! The IR is a DAG: `ScanSubQuery` and `ScanCte` bodies may be shared
//! between several call-sites. The analyzer commits to a single rule:
//! **every `NodeId` is visited in exactly one domain**, and the frozen
//! [`EqualityFacts`] therefore map each `rel_id` to one [`DomainId`]. This is
//! what makes random-access lookups (`class_of_slot`, `const_of_slot`)
//! unambiguous for consumers such as motion planning.
//!
//! Shared subquery bodies are analyzed once and kept opaque — the analyzer
//! never unions an outer slot with a subquery output slot, so one domain
//! per body is enough to describe every call-site.
//!
//! If a future pass needs per-call-site facts (e.g. to propagate equalities
//! across the subquery boundary), the intended extension point is **IR
//! cloning**: fork the shared subtree so each call-site gets its own
//! `NodeId`s, then run analysis on the resulting tree. The fact model does
//! not need to grow a `(Domain, NodeId)` key.
//!
//! # Two-phase analysis
//!
//! The implementation has two union-find layers with different roles:
//!
//! - `LocalFacts` is a temporary per-DNF-chain union-find. It derives facts
//!   from one conjunction of `=` predicates and emits only the slot-to-slot and
//!   slot-to-constant facts that are safe to keep from that chain.
//! - `EqualityFactsBuilder` is the global union-find that interns slots and
//!   constants for the whole subtree, merges facts that survive DNF
//!   intersection, and finally freezes them into [`EqualityFacts`].
//!
//! `OR` predicates are handled conservatively: each DNF chain is analyzed
//! independently, then only the intersection of facts that hold in every
//! satisfiable branch is added to the global builder. This keeps the final
//! classes small and prevents execution SQL from inheriting a blown-up DNF
//! representation.
//!
//! # Conservative term extraction
//!
//! Only direct `Reference`, `Constant`, and `Parameter` terms participate in
//! equality reasoning. Casts, functions, arithmetic, and other derived
//! expressions stay opaque on purpose:
//!
//! - classes remain type-sensitive because every extracted term carries its
//!   [`DerivedType`];
//! - classes remain cast-sensitive because `cast(ref)` is not silently reduced
//!   to `ref`;
//! - repeated occurrences of the same SQL placeholder are canonicalized by
//!   parameter index rather than expression-node identity;
//! - parameter-driven classes are marked as "tainted" inside `LocalFacts`, so
//!   they can still support join reasoning but do not produce derived
//!   slot-to-constant facts for execution-time filter materialization.
//!
//! # What the frozen result is optimized for
//!
//! [`EqualityFacts`] intentionally stores only the hot lookup data:
//!
//! - `slot -> class`
//! - `class -> optional constant`
//! - `rel -> domain` (one domain per rel by construction — see above)
//!
//! It does not keep provenance, derivation trees, or an explicit list of class
//! members. The intended consumers only need fast answers to planner questions
//! such as:
//!
//! - "is this sharding-key slot fixed to a constant?"
//! - "do these two join slots belong to the same class?"
//! - "which domain was assigned to this relational node?"
//!
//! The pass is run after predicate normalization and before motion planning in
//! `optimize_subtree`, and the resulting facts are then consumed by
//! redistribution and by execution-time key-filter materialization.

use crate::errors::SbroadError;
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::Relational;
use crate::ir::node::{
    Alias, BoolExpr, Constant, Delete, Except, GroupBy, Having, Insert, Intersect, Join, Limit,
    NodeId, OrderBy, Projection, Reference, ReferenceTarget, Row, ScanCte, ScanSubQuery, Selection,
    UnaryExpr, Union, UnionAll, Update,
};
use crate::ir::operator::{Bool, JoinKind, Unary};
use crate::ir::transformation::dnf::Chain;
use crate::ir::types::DerivedType;
use crate::ir::value::{Trivalent, Value};
use crate::ir::Plan;
use itertools::Itertools;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

#[derive(Eq, PartialEq, Copy, Clone, Debug, Hash)]
struct UnionFindGroup(usize);

impl UnionFindGroup {
    fn index(&self) -> usize {
        self.0
    }
}

struct UnionFind<T> {
    elems: HashMap<T, usize>,
    parents: Vec<usize>,
    sizes: Vec<usize>,
}

impl<T> UnionFind<T>
where
    T: Eq + Hash,
{
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            elems: HashMap::with_capacity(cap),
            parents: Vec::with_capacity(cap),
            sizes: Vec::with_capacity(cap),
        }
    }

    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn add(&mut self, elem: T) -> UnionFindGroup {
        match self.elems.entry(elem) {
            Entry::Occupied(e) => UnionFindGroup(*e.get()),
            Entry::Vacant(e) => {
                let group = self.parents.len();
                e.insert(group);
                self.parents.push(group);
                self.sizes.push(1);
                UnionFindGroup(group)
            }
        }
    }

    fn inner_find(&mut self, start: usize) -> usize {
        let mut current = start;
        while self.parents[current] != current {
            current = self.parents[current];
        }
        let root = current;
        let mut current = start;
        while self.parents[current] != current {
            let next = self.parents[current];
            self.parents[current] = root;
            current = next;
        }
        root
    }

    #[allow(dead_code)]
    pub fn find(&mut self, elem: &T) -> Option<UnionFindGroup> {
        let start = *self.elems.get(elem)?;
        Some(UnionFindGroup(self.inner_find(start)))
    }

    #[allow(dead_code)]
    pub fn union(&mut self, l: &T, r: &T) {
        if let (Some(UnionFindGroup(l)), Some(UnionFindGroup(r))) = (self.find(l), self.find(r)) {
            self.inner_union(l, r);
        }
    }

    pub fn union_groups(&mut self, l: UnionFindGroup, r: UnionFindGroup) {
        let UnionFindGroup(l) = l;
        if l >= self.parents.len() {
            return;
        }
        let UnionFindGroup(r) = r;
        if r >= self.parents.len() {
            return;
        }

        let l = self.inner_find(l);
        let r = self.inner_find(r);
        self.inner_union(l, r);
    }

    fn inner_union(&mut self, l: usize, r: usize) {
        if l == r {
            return;
        }

        let (small, large) = if self.sizes[l] < self.sizes[r] {
            (l, r)
        } else {
            (r, l)
        };
        self.parents[small] = large;
        self.sizes[large] += self.sizes[small];
    }

    fn inner_flatten(&mut self) {
        for i in 0..self.parents.len() {
            self.inner_find(i);
        }
    }

    pub fn len(&self) -> usize {
        self.parents.len()
    }

    pub fn groups_number(&mut self) -> usize {
        self.inner_flatten();
        self.parents.iter().unique().count()
    }

    pub fn into_groups(mut self) -> impl Iterator<Item = (T, UnionFindGroup)> {
        self.inner_flatten();
        let parents = self.parents;
        self.elems
            .into_iter()
            .map(move |(elem, idx)| (elem, UnionFindGroup(parents[idx])))
    }
}

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
struct DomainId(usize);

impl DomainId {
    fn inc(&mut self) -> DomainId {
        self.0 += 1;
        DomainId(self.0)
    }
}

#[derive(Eq, PartialEq, Hash, Clone)]
struct SlotKey {
    // domain_id is required here even though (rel_id, output_idx) looks
    // sufficient at first glance.  CTEs and subqueries can reuse the same
    // physical node (same rel_id) in multiple semantic scopes: e.g. a CTE
    // referenced twice shares one body NodeId, but analyze() visits it twice
    // under two different fresh domains.  Without domain_id those two
    // instantiations would collapse into one UnionFind group and share facts
    // that should be independent.
    domain_id: DomainId,
    rel_id: NodeId,
    output_idx: usize,
}

impl PartialOrd<Self> for SlotKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SlotKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.domain_id
            .0
            .cmp(&other.domain_id.0)
            .then_with(|| self.rel_id.arena_type.cmp(&other.rel_id.arena_type))
            .then_with(|| self.rel_id.offset.cmp(&other.rel_id.offset))
            .then_with(|| self.output_idx.cmp(&other.output_idx))
    }
}

#[derive(Eq, PartialEq, Hash)]
enum EqualityFactAtom {
    Slot(SlotKey),
    // domain_id is part of the atom identity for constants (and params in
    // FactAtom below).  Without it, Constant(1) from an isolated subquery
    // (dom1) and Constant(1) from the outer query (dom0) would be the same
    // UnionFind entry, bridging slots across domain boundaries: any two slots
    // that happen to equal the same literal would end up in one equivalence
    // class regardless of their semantic scope.  domain_id prevents that.
    Constant(DomainId, Value),
}

#[derive(Eq, PartialEq, Clone, Copy, Debug, Hash)]
struct ClassId(u32);

// Callers do not need to track which domain a node belongs to.  Domain
// separation is enforced internally: domain_id is baked into every atom
// (SlotKey, Constant, Param), so classes from different scopes never merge
// accidentally.  A direct child of an inner join is always analyzed in the
// same domain as the join itself (see analyze()), so looking up
// class_of_slot / const_of_slot for join children is always safe without an
// explicit domain guard.
#[allow(dead_code)]
struct EqualityFacts {
    slot_classes: HashMap<NodeId, Box<[Option<ClassId>]>>,
    class_const: Box<[Option<Value>]>,
    domains: HashMap<NodeId, DomainId>,
}

impl EqualityFacts {
    /// Returns the equivalence class of a relational output slot.
    #[must_use]
    #[allow(dead_code)]
    pub fn class_of_slot(&self, rel_id: NodeId, pos: usize) -> Option<ClassId> {
        self.slot_classes
            .get(&rel_id)
            .and_then(|classes| classes.get(pos))
            .and_then(|class_id| *class_id)
    }

    /// Returns the constant unified with a class, if the class is fixed.
    #[must_use]
    #[allow(dead_code)]
    pub fn const_of_class(&self, class_id: ClassId) -> Option<&Value> {
        self.class_const
            .get(usize::try_from(class_id.0).expect("class id should fit usize"))
            .and_then(Option::as_ref)
    }

    /// Convenience lookup for a slot fixed to a constant in its class.
    #[must_use]
    #[allow(dead_code)]
    pub fn const_of_slot(&self, rel_id: NodeId, pos: usize) -> Option<&Value> {
        let class_id = self.class_of_slot(rel_id, pos)?;
        self.const_of_class(class_id)
    }

    /// Returns the join domain assigned to a relational node.
    #[must_use]
    #[allow(dead_code)]
    pub fn domain_of_rel(&self, rel_id: NodeId) -> Option<DomainId> {
        self.domains.get(&rel_id).copied()
    }
}

struct EqualityFactsBuilder {
    members: UnionFind<EqualityFactAtom>,
    domains: HashMap<NodeId, DomainId>,
}

impl EqualityFactsBuilder {
    fn new() -> Self {
        Self {
            members: UnionFind::with_capacity(0),
            domains: HashMap::new(),
        }
    }

    fn add_slot(&mut self, key: SlotKey) -> UnionFindGroup {
        // Contract: one NodeId -> one domain. Re-analyzing under another
        // domain means a shared subtree was not cloned first.
        debug_assert!(
            self.domains
                .get(&key.rel_id)
                .is_none_or(|d| *d == key.domain_id),
            "rel_id visited under two different domains"
        );
        self.domains.insert(key.rel_id, key.domain_id);
        self.members.add(EqualityFactAtom::Slot(key))
    }

    fn union_slot_and_slot(&mut self, l: SlotKey, r: SlotKey) {
        let left = self.add_slot(l);
        let right = self.add_slot(r);

        self.members.union_groups(left, right);
    }

    fn union_slot_and_const(&mut self, slot: SlotKey, value: Value) {
        let constant = self
            .members
            .add(EqualityFactAtom::Constant(slot.domain_id, value));
        let slot = self.add_slot(slot);

        self.members.union_groups(slot, constant);
    }

    fn freeze(mut self) -> EqualityFacts {
        let groups = self.members.groups_number();

        let mut root_to_class: HashMap<UnionFindGroup, ClassId> = HashMap::with_capacity(groups);
        let mut class_const: Vec<Option<Value>> = Vec::with_capacity(groups);
        let mut const_conflict: Vec<bool> = vec![false; groups];
        let mut slot_classes: HashMap<NodeId, Vec<Option<ClassId>>> =
            HashMap::with_capacity(self.domains.len());

        for (atom, group) in self.members.into_groups() {
            let class_id = *root_to_class.entry(group).or_insert_with(|| {
                let id = ClassId(class_const.len() as u32);
                class_const.push(None);
                id
            });

            match atom {
                EqualityFactAtom::Constant(_, value) => {
                    // If multiple constants land in the same group (e.g. a = 1 AND a = 2),
                    // we should merge them. If they are trivalent, we keep the first one, otherwise
                    // we set constant to None, because we can have more than 2 constants and this
                    // would indicate previous conflict.
                    // TODO: when conflicting constants bridge here from different
                    // chains (as opposed to within one chain, where `LocalFacts`
                    // already kills the chain), the class is semantically
                    // unsatisfiable — the whole plan slice is infeasible.
                    // PostgreSQL marks such classes as contradictory and uses
                    // that to prove the query returns no rows.  We only drop
                    // the constant and keep the (now unreachable) slot
                    // membership, which is conservative but loses a pruning
                    // opportunity.
                    let i = class_id.0 as usize;
                    if const_conflict[i] {
                        // we already meet conflict, ignore this constant
                        continue;
                    }

                    if let Some(v) = class_const[i].as_ref() {
                        if v.eq(&value) != Trivalent::True {
                            class_const[i] = None;
                            const_conflict[i] = true;
                        }
                        continue;
                    }

                    class_const[i] = Some(value);
                }
                EqualityFactAtom::Slot(SlotKey {
                    rel_id, output_idx, ..
                }) => {
                    let classes = slot_classes.entry(rel_id).or_default();
                    if classes.len() <= output_idx {
                        classes.resize(output_idx + 1, None);
                    }
                    classes[output_idx] = Some(class_id);
                }
            }
        }

        EqualityFacts {
            slot_classes: slot_classes
                .into_iter()
                .map(|(rel_id, v)| (rel_id, v.into_boxed_slice()))
                .collect(),
            class_const: class_const.into_boxed_slice(),
            domains: self.domains,
        }
    }
}

struct EqualityAnalysis<'p> {
    plan: &'p Plan,
    builder: EqualityFactsBuilder,
    next_domain_id: DomainId,
    // IR is a DAG: shared bodies (subquery or CTE) must be analyzed only
    // once to uphold the one-NodeId-one-domain contract. If cross-boundary
    // fact propagation is ever added, clone the body per call-site instead
    // of weakening the contract here.
    visited_shared_bodies: HashSet<NodeId>,
}

impl<'p> EqualityAnalysis<'p> {
    pub fn get_equality_facts(
        plan: &'p Plan,
        top_id: NodeId,
    ) -> Result<EqualityFacts, SbroadError> {
        let mut analyzer = Self {
            plan,
            builder: EqualityFactsBuilder::new(),
            next_domain_id: DomainId(0),
            visited_shared_bodies: HashSet::new(),
        };

        let top_domain = analyzer.fresh_domain();
        analyzer.analyze(top_id, top_domain)?;

        Ok(analyzer.builder.freeze())
    }

    fn union_passthrough_output(
        &mut self,
        rel_id: NodeId,
        child_id: NodeId,
        domain_id: DomainId,
    ) -> Result<(), SbroadError> {
        let output_len = self
            .plan
            .get_row_list(self.plan.get_relational_output(rel_id)?)?
            .len();
        for output_idx in 0..output_len {
            self.builder.union_slot_and_slot(
                SlotKey {
                    domain_id,
                    rel_id,
                    output_idx,
                },
                SlotKey {
                    domain_id,
                    rel_id: child_id,
                    output_idx,
                },
            );
        }
        Ok(())
    }

    fn union_projection_output(
        &mut self,
        rel_id: NodeId,
        child_id: NodeId,
        domain_id: DomainId,
    ) -> Result<(), SbroadError> {
        let output_id = self.plan.get_relational_output(rel_id)?;
        for (pos, alias_id) in self
            .plan
            .get_row_list(output_id)?
            .iter()
            .copied()
            .enumerate()
        {
            let alias = self.plan.get_expression_node(alias_id)?;
            let Expression::Alias(Alias { child, .. }) = alias else {
                continue;
            };
            let child_expr = self.plan.get_expression_node(*child)?;
            let Expression::Reference(Reference {
                target: ReferenceTarget::Single(target_rel),
                position,
                ..
            }) = child_expr
            else {
                continue;
            };
            if *target_rel != child_id {
                continue;
            }
            self.builder.union_slot_and_slot(
                SlotKey {
                    domain_id,
                    rel_id,
                    output_idx: pos,
                },
                SlotKey {
                    domain_id,
                    rel_id: child_id,
                    output_idx: *position,
                },
            );
        }
        Ok(())
    }

    fn union_join_output(
        &mut self,
        rel_id: NodeId,
        left_id: NodeId,
        right_id: NodeId,
        domain_id: DomainId,
        include_right: bool,
    ) -> Result<(), SbroadError> {
        let left_len = self
            .plan
            .get_row_list(self.plan.get_relational_output(left_id)?)?
            .len();
        let right_len = self
            .plan
            .get_row_list(self.plan.get_relational_output(right_id)?)?
            .len();
        for pos in 0..left_len {
            self.builder.union_slot_and_slot(
                SlotKey {
                    domain_id,
                    rel_id,
                    output_idx: pos,
                },
                SlotKey {
                    domain_id,
                    rel_id: left_id,
                    output_idx: pos,
                },
            );
        }
        if include_right {
            for pos in 0..right_len {
                self.builder.union_slot_and_slot(
                    SlotKey {
                        domain_id,
                        rel_id,
                        output_idx: left_len + pos,
                    },
                    SlotKey {
                        domain_id,
                        rel_id: right_id,
                        output_idx: pos,
                    },
                );
            }
        }
        Ok(())
    }

    fn analyze(&mut self, rel_id: NodeId, domain_id: DomainId) -> Result<(), SbroadError> {
        let rel = self.plan.get_relation_node(rel_id)?;

        for subquery in rel.subqueries().iter() {
            if !self.visited_shared_bodies.insert(*subquery) {
                continue;
            }
            let sub_domain = self.fresh_domain();
            self.analyze(*subquery, sub_domain)?;
        }

        if rel.has_output() {
            let output = self.plan.get_row_list(rel.output())?;
            for (output_idx, _) in output.iter().enumerate() {
                self.builder.add_slot(SlotKey {
                    domain_id,
                    rel_id,
                    output_idx,
                });
            }
        }

        match rel {
            Relational::Projection(Projection {
                child,
                windows,
                is_distinct,
                group_by,
                having,
                ..
            }) => {
                if windows.is_empty() && !is_distinct && group_by.is_none() && having.is_none() {
                    let Some(child_id) = child else {
                        unreachable!("Projection must have a child");
                    };
                    self.analyze(*child_id, domain_id)?;
                    self.union_projection_output(rel_id, *child_id, domain_id)?;
                } else {
                    let inner_domain = self.fresh_domain();
                    if let Some(having_id) = having {
                        self.analyze(*having_id, inner_domain)?;
                    } else if let Some(group_by_id) = group_by {
                        self.analyze(*group_by_id, inner_domain)?;
                    } else if let Some(child_id) = child {
                        self.analyze(*child_id, inner_domain)?;
                    }
                }
            }
            Relational::Selection(Selection { child, filter, .. }) => {
                self.analyze(*child, domain_id)?;
                self.union_passthrough_output(rel_id, *child, domain_id)?;
                self.apply_expr_facts(*filter, domain_id)?;
            }

            Relational::Limit(Limit { child, .. })
            | Relational::GroupBy(GroupBy { child, .. })
            | Relational::Having(Having { child, .. })
            | Relational::OrderBy(OrderBy { child, .. }) => {
                let child_domain = self.fresh_domain();
                self.analyze(*child, child_domain)?;
            }

            Relational::ScanCte(ScanCte { child, .. }) => {
                // CTE bodies are shared across every `FROM cte` in the plan
                // (DAG), so only the first call-site analyzes the body; the
                // rest rely on the body being opaque — ScanCte's own output
                // slots carry outer-scope facts independently.
                if self.visited_shared_bodies.insert(*child) {
                    let child_domain = self.fresh_domain();
                    self.analyze(*child, child_domain)?;
                }
            }

            Relational::Join(Join {
                left,
                right,
                kind,
                condition,
                ..
            }) => match kind {
                JoinKind::Inner => {
                    self.analyze(*left, domain_id)?;
                    self.analyze(*right, domain_id)?;
                    self.union_join_output(rel_id, *left, *right, domain_id, true)?;
                    self.apply_expr_facts(*condition, domain_id)?;
                }
                JoinKind::LeftOuter => {
                    self.analyze(*left, domain_id)?;
                    let right_domain = self.fresh_domain();
                    self.analyze(*right, right_domain)?;
                    self.union_join_output(rel_id, *left, *right, domain_id, false)?;
                    // TODO: we drop the ON condition entirely, but PostgreSQL
                    // uses it to infer facts about the non-nullable (left)
                    // side: `LEFT JOIN r ON l.a = 1 AND ...` implies l.a = 1
                    // for every surviving row.  Splitting the condition into
                    // left-only / right-only / cross predicates and applying
                    // the left-only part in `domain_id` would recover those
                    // facts without breaking outer-join semantics.
                }
            },

            Relational::Motion(_) => {
                unreachable!("Unexpected motion node, we don't support analyze of them");
            }

            Relational::ScanSubQuery(ScanSubQuery { child, .. }) => {
                if self.is_safe_subtree(*child)? {
                    self.analyze(*child, domain_id)?;
                    self.union_passthrough_output(rel_id, *child, domain_id)?;
                } else {
                    let child_domain = self.fresh_domain();
                    self.analyze(*child, child_domain)?;
                }
            }

            Relational::UnionAll(UnionAll { left, right, .. })
            | Relational::Union(Union { left, right, .. })
            | Relational::Except(Except { left, right, .. })
            | Relational::Intersect(Intersect { left, right, .. }) => {
                let left_domain = self.fresh_domain();
                self.analyze(*left, left_domain)?;
                let right_domain = self.fresh_domain();
                self.analyze(*right, right_domain)?;
            }

            // DML
            Relational::Insert(Insert { child, .. })
            | Relational::Update(Update { child, .. })
            | Relational::Delete(Delete {
                child: Some(child), ..
            }) => {
                let child_domain = self.fresh_domain();
                self.analyze(*child, child_domain)?;
            }

            // don't need to analyze these
            Relational::ScanRelation(_)
            | Relational::SelectWithoutScan(_)
            | Relational::Values(_)
            | Relational::ValuesRow(_)
            | Relational::Delete(Delete { child: None, .. }) => {}
        }

        Ok(())
    }

    fn fresh_domain(&mut self) -> DomainId {
        let domain_id = self.next_domain_id;
        self.next_domain_id.inc();
        domain_id
    }

    fn is_safe_subtree(&self, rel_id: NodeId) -> Result<bool, SbroadError> {
        let rel = self.plan.get_relation_node(rel_id)?;
        let is_safe = match rel {
            // Leaf nodes: rows come directly from storage or literals, no transformations.
            Relational::ScanRelation(_)
            | Relational::SelectWithoutScan(_)
            | Relational::Values(_)
            | Relational::ValuesRow(_) => true,
            // Selection only filters rows without changing their values.
            Relational::Selection(Selection { child, .. }) => self.is_safe_subtree(*child)?,
            // Projection is safe only when every output column is a direct reference to a
            // child column (no expressions like a + 1). Computed expressions break the
            // equality: knowing output.b = 5 says nothing useful about input.a when b = a + 1.
            // Windows, DISTINCT, GROUP BY and HAVING all aggregate or deduplicate rows,
            // destroying per-row identity.
            Relational::Projection(Projection {
                child,
                windows,
                is_distinct,
                group_by,
                having,
                ..
            }) => {
                windows.is_empty()
                    && !is_distinct
                    && group_by.is_none()
                    && having.is_none()
                    && child.is_some()
                    && self.projection_is_direct_ref(rel_id, child.expect("checked above"))?
                    && self.is_safe_subtree(child.expect("checked above"))?
            }
            // Transparent wrapper around a subquery: safe if the subquery itself is safe.
            Relational::ScanSubQuery(ScanSubQuery { child, .. }) => self.is_safe_subtree(*child)?,
            // Inner join preserves row identity on both sides and lets facts flow through.
            // Outer joins pad non-matching rows with NULLs, so an equality
            // fact from the nullable side may not hold for every output row.
            Relational::Join(Join {
                left, right, kind, ..
            }) => {
                matches!(kind, JoinKind::Inner)
                    && self.is_safe_subtree(*left)?
                    && self.is_safe_subtree(*right)?
            }
            // LIMIT and ORDER BY sit above Projection/Selection in the plan and are
            // conservative false here: they do not distort row values, but we never
            // expect to start equality analysis at or below them.
            Relational::Limit(_) | Relational::OrderBy(_) => false,
            // Aggregation collapses many input rows into one output row, so individual
            // column equalities from the input no longer hold on the output.
            Relational::GroupBy(_) | Relational::Having(_) => false,
            // Set operations mix rows from sources with potentially different equality
            // facts, so no single fact can be attributed to all output rows.
            Relational::Union(_)
            | Relational::UnionAll(_)
            | Relational::Except(_)
            | Relational::Intersect(_) => false,
            // CTE scan re-reads a materialized result that could be any query shape.
            Relational::ScanCte(_) => false,
            // Motion moves data between nodes but does not change values; marked false
            // conservatively since cross-node equality propagation is not needed here.
            Relational::Motion(_) => false,
            // DML nodes are not SELECT queries.
            Relational::Insert(_) | Relational::Update(_) | Relational::Delete(_) => false,
        };
        Ok(is_safe)
    }

    /// Returns `true` when every output column of `rel_id` is a plain
    /// `Alias → Reference(child_id, pos)` with no intervening expression.
    ///
    /// This is the precondition for treating a Projection as safe in
    /// `is_safe_subtree`.  `union_projection_output` builds a slot-to-slot
    /// mapping by walking exactly this `Alias → Reference` structure: if any
    /// output column is a computed expression (`a + 1`, `COALESCE(...)`, etc.)
    /// that method silently skips it, leaving the corresponding output slot
    /// disconnected.  Declaring such a projection "safe" would be wrong —
    /// callers would trust that all output slots carry facts from the child,
    /// but some slots would have no connection at all.
    ///
    /// By checking up front that every column is a direct reference, we
    /// guarantee that `union_projection_output` will cover the full output
    /// before we allow facts to flow through the projection.
    fn projection_is_direct_ref(
        &self,
        rel_id: NodeId,
        child_id: NodeId,
    ) -> Result<bool, SbroadError> {
        let output_id = self.plan.get_relational_output(rel_id)?;
        for alias_id in self.plan.get_row_list(output_id)? {
            let alias = self.plan.get_expression_node(*alias_id)?;
            let Expression::Alias(Alias { child, .. }) = alias else {
                return Ok(false);
            };
            let child_expr = self.plan.get_expression_node(*child)?;
            let Expression::Reference(Reference {
                target: ReferenceTarget::Single(target_rel),
                ..
            }) = child_expr
            else {
                return Ok(false);
            };
            if *target_rel != child_id {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Applies the always-true equality facts derived from a boolean
    /// expression.
    ///
    /// The expression is first analyzed per DNF chain; then only the
    /// intersection of facts that hold in every satisfiable chain is merged into
    /// the global builder.
    // TODO: DNF can blow up exponentially on deeply nested OR expressions.
    // PostgreSQL avoids DNF explicitly for eclass derivation.  We should either
    // bound the number of chains returned by get_dnf_chains() or fall back to
    // analyzing only the top-level AND chain when the DNF expansion is too large.
    fn apply_expr_facts(&mut self, expr_id: NodeId, domain: DomainId) -> Result<(), SbroadError> {
        let chains = self.plan.get_dnf_chains(expr_id)?;
        let mut intersection: Option<HashSet<DerivedFact>> = None;
        for chain in chains {
            let ChainOutcome::Facts(facts) = self.collect_chain_facts(chain, domain)? else {
                continue;
            };
            intersection = Some(match intersection.take() {
                Some(existing) => existing
                    .intersection(&facts)
                    .cloned()
                    .collect::<HashSet<DerivedFact>>(),
                None => facts,
            });
        }
        let Some(facts) = intersection else {
            return Ok(());
        };
        for fact in facts {
            match fact {
                DerivedFact::SlotEq(left, right) => self.builder.union_slot_and_slot(left, right),
                DerivedFact::SlotConst(slot, value) => {
                    self.builder.union_slot_and_const(slot, value)
                }
            }
        }
        Ok(())
    }

    fn collect_chain_facts(
        &self,
        mut chain: Chain,
        domain: DomainId,
    ) -> Result<ChainOutcome, SbroadError> {
        let mut local = LocalFacts::default();
        // Two passes over the chain. Pass 1 collects slots that are known NULL
        // via `IS NULL`; pass 2 then kills the whole chain if any `=` predicate
        // touches such a slot, because `NULL = <anything>` is UNKNOWN — never
        // TRUE — so the conjunction can never be satisfied. Splitting the scans
        // lets `IS NULL` and `=` appear in any order without a post-factum
        // reconciliation step.
        let mut null_slots: HashSet<SlotKey> = HashSet::new();
        for node_id in chain.get_mut_nodes().iter() {
            let expr = self.plan.get_expression_node(*node_id)?;
            let child = match expr {
                Expression::Unary(UnaryExpr {
                    op: Unary::IsNull,
                    child,
                }) => child,
                // The chain is already dead if one of conditions is false or null
                Expression::Constant(Constant {
                    value: Value::Boolean(false),
                })
                | Expression::Constant(Constant { value: Value::Null }) => {
                    return Ok(ChainOutcome::Dead)
                }
                _ => continue,
            };
            let child_expr = self.plan.get_expression_node(*child)?;
            if let Expression::Reference(Reference {
                target: ReferenceTarget::Single(rel_id),
                position,
                ..
            }) = child_expr
            {
                null_slots.insert(SlotKey {
                    domain_id: domain,
                    rel_id: *rel_id,
                    output_idx: *position,
                });
            }
        }

        while let Some(node_id) = chain.get_mut_nodes().pop_back() {
            let expr = self.plan.get_expression_node(node_id)?;
            let Expression::Bool(BoolExpr {
                op: Bool::Eq,
                left,
                right,
                ..
            }) = expr
            else {
                continue;
            };
            let left_terms = self.extract_equality_terms(*left, domain)?;
            if left_terms.is_none() {
                return Ok(ChainOutcome::Dead);
            }
            let right_terms = self.extract_equality_terms(*right, domain)?;
            if right_terms.is_none() {
                return Ok(ChainOutcome::Dead);
            }

            let left_terms = left_terms.expect("checked above");
            let right_terms = right_terms.expect("checked above");

            if left_terms.len() != right_terms.len() {
                continue;
            }
            for (left_term, right_term) in left_terms.into_iter().zip(right_terms) {
                if let FactAtom::Slot(ref slot) = left_term.atom {
                    if null_slots.contains(slot) {
                        return Ok(ChainOutcome::Dead);
                    }
                }
                if let FactAtom::Slot(ref slot) = right_term.atom {
                    if null_slots.contains(slot) {
                        return Ok(ChainOutcome::Dead);
                    }
                }
                if left_term.atom == FactAtom::Other || right_term.atom == FactAtom::Other {
                    continue;
                }
                if left_term.ty != right_term.ty {
                    continue;
                }

                local.union(left_term.atom, right_term.atom);
            }
        }

        Ok(local.into_facts())
    }

    fn extract_equality_terms(
        &self,
        expr_id: NodeId,
        domain_id: DomainId,
    ) -> Result<Option<Vec<FactTerm>>, SbroadError> {
        let expr = self.plan.get_expression_node(expr_id)?;
        // TODO: nested rows are not flattened — `((a, b), (c, d)) = ((1, 2), (3, 4))`
        // ends up with one top-level Row on each side whose children are
        // themselves Rows, which fall through to `extract_scalar_equality_term`
        // as `FactAtom::Other` and contribute no facts.  PostgreSQL flattens
        // row equality recursively before eclass derivation; we should do the
        // same, or at least handle one extra level explicitly.
        match expr {
            Expression::Row(Row { list, .. }) => {
                let mut terms = Vec::with_capacity(list.len());
                for child_id in list {
                    let term = self.extract_scalar_equality_term(*child_id, domain_id)?;
                    if term.is_none() {
                        return Ok(None);
                    }
                    terms.push(term.expect("checked above"));
                }
                Ok(Some(terms))
            }
            _ => {
                let term = self.extract_scalar_equality_term(expr_id, domain_id)?;
                if term.is_none() {
                    return Ok(None);
                }
                Ok(Some(vec![term.expect("checked above")]))
            }
        }
    }

    fn extract_scalar_equality_term(
        &self,
        expr_id: NodeId,
        domain_id: DomainId,
    ) -> Result<Option<FactTerm>, SbroadError> {
        let expr = self.plan.get_expression_node(expr_id)?;
        let term = match expr {
            Expression::Reference(Reference {
                target: ReferenceTarget::Single(rel_id),
                position,
                col_type,
                ..
            }) => FactTerm {
                atom: FactAtom::Slot(SlotKey {
                    domain_id,
                    rel_id: *rel_id,
                    output_idx: *position,
                }),
                ty: *col_type,
            },
            Expression::Constant(Constant { value }) => {
                // `value.eq(value) != True` catches both NaN (floats where
                // NaN != NaN) and NULL (Null.eq(Null) = Unknown).  In both
                // cases the equality predicate can never evaluate to TRUE,
                // so the whole chain is unsatisfiable.
                if value.eq(value) != Trivalent::True {
                    return Ok(None);
                }

                FactTerm {
                    atom: FactAtom::Const {
                        domain_id,
                        value: value.clone(),
                    },
                    ty: value.get_type(),
                }
            }
            Expression::Parameter(parameter) => FactTerm {
                atom: FactAtom::Param {
                    domain_id,
                    param_index: parameter.index,
                },
                ty: parameter.param_type,
            },
            // TODO: casts and computed expressions fall here and are treated
            // as opaque.  PostgreSQL recognises cast-preserving equivalences
            // through `opfamilies`, so `CAST(a AS INT) = CAST(b AS INT)` can
            // still unify a and b when the cast is injective on the involved
            // types.  Worth revisiting once we have a type lattice rich
            // enough to express which casts preserve equality.
            _ => FactTerm {
                atom: FactAtom::Other,
                ty: DerivedType::unknown(),
            },
        };
        Ok(Some(term))
    }
}

/// Internal atom used while deriving facts from a boolean expression.
///
/// A fact atom is the expression-level counterpart of a frozen slot class
/// member. Slots and constants are domain-qualified so two identical values in
/// different semantic scopes do not get unified accidentally. Parameters are
/// also kept as separate atoms so the analyzer can reason about transitivity
/// without materializing unsafe constant predicates from prepared statements.
#[derive(PartialEq, Eq, Hash)]
enum FactAtom {
    Slot(SlotKey),
    // domain_id keeps Const and Param atoms domain-scoped for the same reason
    // as EqualityFactAtom::Constant: identical literals or parameter indices
    // from different scopes must not bridge unrelated slots.
    Const {
        domain_id: DomainId,
        value: Value,
    },
    Param {
        domain_id: DomainId,
        param_index: u16,
    },
    Other,
}

/// Typed wrapper around [`FactAtom`].
///
/// The analyzer only unifies terms when their [`DerivedType`] matches exactly,
/// which keeps equality classes conservative around implicit coercions.
#[derive(PartialEq, Eq)]
struct FactTerm {
    atom: FactAtom,
    ty: DerivedType,
}

/// Normalized fact emitted from one satisfiable DNF chain.
///
/// `LocalFacts` reduces a conjunction of `=` predicates to these canonical
/// statements before the global builder merges them into final classes.
#[derive(PartialEq, Eq, Hash, Clone)]
enum DerivedFact {
    SlotEq(SlotKey, SlotKey),
    SlotConst(SlotKey, Value),
}

/// Result of analyzing one DNF chain.
///
/// The two variants carry different meaning for `apply_expr_facts`, which
/// intersects the live chains' facts and skips dead ones:
///
/// - [`ChainOutcome::Dead`] — the chain is unsatisfiable (e.g. `a = 1 AND
///   a = 2`, `a = NULL`, `a IS NULL AND a = b`).  Callers MUST skip it
///   entirely and treat it as if the branch did not exist, otherwise an
///   intersection with the live chains would wrongly wipe out real facts.
/// - [`ChainOutcome::Facts`] — the chain is live.  The carried `HashSet`
///   may be empty, which is distinct from `Dead`: an empty fact set means
///   "satisfiable but no equality facts can be derived", and intersecting
///   it with other chains' facts correctly collapses the result to empty.
enum ChainOutcome {
    Dead,
    Facts(HashSet<DerivedFact>),
}

struct LocalFacts {
    members: UnionFind<FactAtom>,
}

impl Default for LocalFacts {
    fn default() -> Self {
        Self {
            members: UnionFind::new(),
        }
    }
}

impl LocalFacts {
    fn union(&mut self, left: FactAtom, right: FactAtom) {
        let left = self.members.add(left);
        let right = self.members.add(right);
        self.members.union_groups(left, right);
    }

    fn into_facts(self) -> ChainOutcome {
        // TODO: into_groups() returns raw root indices (not dense 0..n), so we
        // must size vecs by members.len(), not groups_number(). Consider making
        // into_groups() renumber groups densely to save memory.
        let n = self.members.len();
        let mut slots_by_root: Vec<Vec<SlotKey>> = (0..n).map(|_| Vec::new()).collect();
        let mut const_by_root: Vec<Option<Value>> = vec![None; n];
        let mut param_roots: Vec<bool> = vec![false; n];

        for (atom, root) in self.members.into_groups() {
            let i = root.index();
            match atom {
                FactAtom::Slot(slot) => slots_by_root[i].push(slot),
                FactAtom::Const { value, .. } => match &const_by_root[i] {
                    None => const_by_root[i] = Some(value),
                    Some(v) => {
                        if v.eq(&value) != Trivalent::True {
                            // it means we have a conflict like (a = 1 AND a = 2). It is inside one
                            // DNF chain, so a whole chain is false. We can break the loop here.
                            return ChainOutcome::Dead;
                        }
                    }
                },
                FactAtom::Param { .. } => param_roots[i] = true,
                FactAtom::Other => {
                    unreachable!("FactAtom::Other should not be in LocalFacts");
                }
            }
        }

        let mut facts = HashSet::new();
        for i in 0..n {
            let slots = &mut slots_by_root[i];
            if slots.is_empty() {
                continue;
            }
            slots.sort();
            if !param_roots[i] {
                if let Some(value) = &const_by_root[i] {
                    for slot in slots.iter() {
                        facts.insert(DerivedFact::SlotConst(slot.clone(), value.clone()));
                    }
                }
            }
            for l in 0..slots.len() {
                for r in (l + 1)..slots.len() {
                    facts.insert(DerivedFact::SlotEq(slots[l].clone(), slots[r].clone()));
                }
            }
        }
        ChainOutcome::Facts(facts)
    }
}

impl Plan {
    pub fn analyze_equality_facts_in_subtree(self, top_id: NodeId) -> Result<Self, SbroadError> {
        // TODO: wire this up in a follow-up MR. For now the result is
        // dropped on purpose: how the facts are stored on the plan and
        // how consumers access them is still to be decided.
        let _ = EqualityAnalysis::get_equality_facts(&self, top_id)?;
        Ok(self)
    }

    pub fn analyze_equality_facts(self) -> Result<Self, SbroadError> {
        let top_id = self.get_top()?;
        self.analyze_equality_facts_in_subtree(top_id)
    }
}

#[cfg(feature = "mock")]
#[cfg(test)]
mod tests;
