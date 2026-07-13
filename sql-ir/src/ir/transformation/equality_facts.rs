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
//! - `LocalFacts` is a temporary per-`AND`-region union-find. It derives facts
//!   from one conjunction of `=` predicates and emits only the slot-to-slot and
//!   slot-to-constant facts that are safe to keep from that region.
//! - `EqualityFactsBuilder` is the global union-find that interns slots and
//!   constants for the whole subtree, merges the derived facts, and finally
//!   freezes them into [`EqualityFacts`].
//!
//! `OR` predicates are handled conservatively and polynomially — no DNF: the
//! boolean tree is walked directly with `AND` = merge / `OR` = intersect, so
//! only facts that hold in every satisfiable arm reach the global builder. This
//! keeps the final classes small without ever materializing a blown-up DNF
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

use crate::errors::{Entity, SbroadError};
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::Relational;
use crate::ir::node::{
    Alias, BoolExpr, Constant, Delete, Except, GroupBy, Having, Insert, Intersect, Join, Limit,
    NodeId, OrderBy, Projection, Reference, ReferenceTarget, Row, ScanCte, ScanSubQuery, Selection,
    UnaryExpr, Union, UnionAll, Update,
};
use crate::ir::operator::{Bool, JoinKind, Unary};
use crate::ir::transformation::equality_facts::DeriveOutcome::Facts;
use crate::ir::types::DerivedType;
use crate::ir::value::{Trivalent, Value};
use crate::ir::Plan;
use itertools::Itertools;
use smol_str::ToSmolStr;
use std::borrow::Cow;
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

    /// Star-union a whole group: intern each element with `add` and union it
    /// into the first. Groups of 0 or 1 elements are no-ops.
    pub fn union_atoms(&mut self, elems: impl IntoIterator<Item = T>) {
        let mut iter = elems.into_iter();
        let Some(rep) = iter.next() else {
            return;
        };
        let rep = self.add(rep);
        for elem in iter {
            let g = self.add(elem);
            self.union_groups(rep, g);
        }
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

#[derive(Eq, PartialEq, Hash, Clone, Copy, Debug)]
struct DomainId(usize);

impl DomainId {
    fn inc(&mut self) -> DomainId {
        self.0 += 1;
        DomainId(self.0)
    }
}

#[derive(Eq, PartialEq, Hash, Clone)]
struct SlotKey {
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
        self.rel_id
            .arena_type
            .cmp(&other.rel_id.arena_type)
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
    // Same domain-tagging rationale as `Constant`: a parameter `$1` in an
    // isolated subquery must not bridge slots with `$1` in the outer query.
    Param(DomainId, u16),
}

/// Public (with its fields below) for the whitebox tests in
/// `sql-planner/tests/planner_tests`; not part of the planner API.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy, Debug, Hash)]
pub struct ClassId(u32);

/// Materialized equivalence class.
///
/// First-class object built at [`EqualityFactsBuilder::freeze`] time, one
/// per UF root.  Stores everything the planner needs to reason about the
/// class without walking the union-find:
///
/// - `members` — every slot that ended up in this class, sorted by
///   `(arena_type, offset, pos)` for deterministic iteration;
/// - `constant` — value pinned to the class, if any.  `None` either when
///   no constant atom was ever unioned in, or when `contradictory == true`
///   (the class lost its single-value invariant);
/// - `contradictory` — PostgreSQL's `ec_broken`: two non-equal constants
///   were merged into the same class (typically via slot-eq facts from
///   different expressions).  Semantically the class is unsatisfiable, so
///   any row matching it cannot exist — consumers may short-circuit to
///   an empty result.
///
/// The type and its fields are public for the whitebox tests in
/// `sql-planner/tests/planner_tests`; not part of the planner API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EquivalenceClass {
    pub members: Box<[Slot]>,
    // The params that are members of the class: sorted, deduped 1-based indices
    // (`a = $1 AND a = $2` keeps both). They are ordinary members, equal to the
    // pin like any slot; the pin is the `constant` if the class has one, else the
    // first (lowest) param.
    pub params: Box<[u16]>,
    pub constant: Option<Value>,
    contradictory: bool,
}

/// Identifier of an output slot used by the public API.
///
/// A slot is a column at a fixed position in the output row of a relational
/// node.  The public motion-planning queries (`are_equal`, `pinned_constant`,
/// `find_equal_position`) operate on slots; class identity and union-find
/// internals stay inside the module.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Slot {
    pub rel_id: NodeId,
    pub pos: usize,
}

impl Slot {
    #[must_use]
    pub fn new(rel_id: NodeId, pos: usize) -> Self {
        Self { rel_id, pos }
    }
}

impl PartialOrd for Slot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Slot {
    /// Canonical ordering: `(arena_type, offset, pos)`.  Members of an
    /// [`EquivalenceClass`] are sorted by this for deterministic iteration
    /// and so that slots from the same relation form a contiguous run
    /// (`find_equal_position` relies on this).  Mirrors [`SlotKey::cmp`]
    /// minus the domain tag, which is internal-only.
    fn cmp(&self, other: &Self) -> Ordering {
        self.rel_id
            .arena_type
            .cmp(&other.rel_id.arena_type)
            .then_with(|| self.rel_id.offset.cmp(&other.rel_id.offset))
            .then_with(|| self.pos.cmp(&other.pos))
    }
}

// Callers do not need to track which domain a node belongs to.  Domain
// separation is enforced internally: domain_id is baked into every atom
// (SlotKey, Constant, Param), so classes from different scopes never merge
// accidentally.  A direct child of an inner join is always analyzed in the
// same domain as the join itself (see analyze()), so looking up
// classes for join children is always safe without an
// explicit domain guard.
//
// The `classes` / `scopes` fields are public for the whitebox tests in
// `sql-planner/tests/planner_tests`; not part of the planner API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EqualityFacts {
    slot_classes: HashMap<NodeId, Box<[Option<ClassId>]>>,
    /// Materialized equivalence classes indexed by [`ClassId`].
    pub classes: Box<[EquivalenceClass]>,
    domains: HashMap<NodeId, DomainId>,
    /// Per-`LEFT JOIN` scope, resolved once at freeze time from the raw
    /// cross-side ON equalities.
    ///
    /// Keyed by the LEFT JOIN's `rel_id`.  Equalities here are NOT merged
    /// into the global classes because they don't hold for null-extended
    /// rows, but they're correct *inside* the join's local scope (matched
    /// rows) and let motion planning detect co-located outer joins.
    pub scopes: HashMap<NodeId, ResolvedScope>,
}

/// Raw cross-side equalities collected for a LEFT JOIN during `analyze`.
/// Intermediate state only — consumed at freeze time to produce
/// [`ResolvedScope`].  Slots are identified by `(rel_id, output_idx)`
/// pairs; the analyzer's domain tagging is intentionally dropped here —
/// scope reasoning looks up classes via [`EqualityFacts::class_of_slot`],
/// which is domain-agnostic for external callers.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ScopedFacts {
    extra_slot_eq: Vec<((NodeId, usize), (NodeId, usize))>,
    extra_slot_const: Vec<((NodeId, usize), Value)>,
}

/// Frozen per-LEFT-JOIN scope.
///
/// The type and its fields are public for the whitebox tests in
/// `sql-planner/tests/planner_tests`; not part of the planner API.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResolvedScope {
    /// `class -> representative class of its scope-group`.  Classes that
    /// the scope's cross-side equalities never touched are absent
    /// (implicit singleton groups — those collapse to "not scope-merged").
    pub class_alias: HashMap<ClassId, usize>,
    /// `representative -> info about the group it represents`.  The
    /// representative is the smallest [`ClassId`] in the group, picked for
    /// deterministic iteration.
    pub repr_info: Vec<ReprInfo>,
}

/// Per-group payload inside a [`ResolvedScope`].
///
/// The type and its fields are public for the whitebox tests in
/// `sql-planner/tests/planner_tests`; not part of the planner API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReprInfo {
    /// Constant pinned for the whole group in this scope, if any.
    /// Resolved at freeze time with priority:
    /// 1. scope-only `Const` value unioned with the group;
    /// 2. global `EquivalenceClass::constant` of any group member;
    /// 3. `None`.
    pub constant: Option<Value>,
    /// Every class in this scope-group, including the representative,
    /// sorted by [`ClassId`].  Used by `find_equal_position` to enumerate
    /// scope-equivalent inner-side positions.
    pub members: Box<[ClassId]>,
}

impl ResolvedScope {
    /// Build a [`ResolvedScope`] from one LEFT JOIN's raw cross-side
    /// equalities.  Collapses them through a tiny union-find, then groups
    /// by root to produce the flat `class_alias` / `repr_info` lookup
    /// tables.  Singleton groups without a scope-only constant are
    /// dropped — they don't merge anything.
    fn from_raw(
        scoped: ScopedFacts,
        slot_classes: &HashMap<NodeId, Vec<Option<ClassId>>>,
        classes: &[EquivalenceClass],
    ) -> Self {
        let class_of_slot = |rel_id: NodeId, pos: usize| -> Option<ClassId> {
            slot_classes
                .get(&rel_id)
                .and_then(|cs| cs.get(pos))
                .and_then(|c| *c)
        };

        let mut uf: UnionFind<ScopedNode> = UnionFind::new();
        for ((r1, p1), (r2, p2)) in &scoped.extra_slot_eq {
            let (Some(c1), Some(c2)) = (class_of_slot(*r1, *p1), class_of_slot(*r2, *p2)) else {
                continue;
            };
            let n1 = uf.add(ScopedNode::Class(c1));
            let n2 = uf.add(ScopedNode::Class(c2));
            uf.union_groups(n1, n2);
        }
        for ((r, p), v) in &scoped.extra_slot_const {
            let Some(c) = class_of_slot(*r, *p) else {
                continue;
            };
            let nc = uf.add(ScopedNode::Class(c));
            let nv = uf.add(ScopedNode::Const(v.clone()));
            uf.union_groups(nc, nv);
        }

        let mut root_to_classes: HashMap<usize, Vec<ClassId>> = HashMap::new();
        let mut root_to_const: HashMap<usize, Value> = HashMap::new();
        for (node, root) in uf.into_groups() {
            let root_idx = root.index();
            match node {
                ScopedNode::Class(c) => root_to_classes.entry(root_idx).or_default().push(c),
                ScopedNode::Const(v) => {
                    // Multiple constants in one scope-group is a scope-only
                    // contradiction; keep the first to stay deterministic.
                    // Real conflict pruning is the global-class job
                    // (`contradictory` flag on [`EquivalenceClass`]).
                    // TODO: surface this scope-only contradiction too, so the
                    // LEFT JOIN's matched-row side can be pruned to empty.
                    root_to_const.entry(root_idx).or_insert(v);
                }
            }
        }

        let total_members: usize = root_to_classes.values().map(Vec::len).sum();
        let mut class_alias: HashMap<ClassId, usize> = HashMap::with_capacity(total_members);
        let mut repr_info: Vec<ReprInfo> = Vec::with_capacity(root_to_classes.len());

        for (root, mut group_classes) in root_to_classes {
            // Singletons (group of one) are pointless — they don't merge
            // anything across the scope and only inflate the lookup maps.
            // Drop them unless they carry a scope-only constant.
            let has_scope_const = root_to_const.contains_key(&root);
            if group_classes.len() < 2 && !has_scope_const {
                continue;
            }
            group_classes.sort();
            let repr = repr_info.len();
            for &c in &group_classes {
                class_alias.insert(c, repr);
            }
            // Pinned-constant resolution: prefer the scope-only `Const`
            // value; fall back to any group member's global constant.
            let constant = root_to_const.remove(&root).or_else(|| {
                group_classes
                    .iter()
                    .find_map(|c| classes[c.0 as usize].constant.clone())
            });
            repr_info.push(ReprInfo {
                constant,
                members: group_classes.into_boxed_slice(),
            });
        }

        ResolvedScope {
            class_alias,
            repr_info,
        }
    }

    /// True when the scope produced no useful merges — empty `ScopedFacts`
    /// (e.g. a LEFT JOIN whose ON has no `=` operands) and scopes whose
    /// groups were all singletons-without-const collapse to this.  Such
    /// scopes are dropped from [`EqualityFacts::scopes`] to avoid forcing
    /// callers into defensive `is_empty()` checks.
    fn is_empty(&self) -> bool {
        self.class_alias.is_empty() && self.repr_info.is_empty()
    }
}

impl EqualityFacts {
    /// Returns the equivalence class of a relational output slot.
    #[must_use]
    pub fn class_of_slot(&self, rel_id: NodeId, pos: usize) -> Option<ClassId> {
        self.slot_classes
            .get(&rel_id)
            .and_then(|classes| classes.get(pos))
            .and_then(|class_id| *class_id)
    }

    /// Returns the constant unified with a class, if the class is fixed.
    #[must_use]
    fn const_of_class(&self, class_id: ClassId) -> Option<&Value> {
        self.class(class_id)?.constant.as_ref()
    }

    fn class(&self, class_id: ClassId) -> Option<&EquivalenceClass> {
        self.classes
            .get(usize::try_from(class_id.0).expect("class id should fit usize"))
    }

    /// Look up the [`ReprInfo`] payload for `class` in the scope active at
    /// `at_rel`.  Returns `None` when `at_rel` has no scope entry, or when
    /// `class` isn't part of any scope-group there (i.e. it survives the
    /// scope as a singleton).
    fn scope_info_for_class(&self, at_rel: NodeId, class: ClassId) -> Option<&ReprInfo> {
        let scope = self.scopes.get(&at_rel)?;
        let repr = *scope.class_alias.get(&class)?;
        scope.repr_info.get(repr)
    }

    /// Is the class for this slot marked contradictory (PostgreSQL's
    /// `ec_broken`)?  A contradictory class merged two non-equal constants,
    /// so no row can satisfy it; callers may prune the surrounding plan
    /// slice to an empty result.  Returns `false` if the slot has no class
    /// (unanalyzed node or out-of-range position).
    #[must_use]
    pub fn is_contradictory(&self, slot: Slot) -> bool {
        let Some(class_id) = self.class_of_slot(slot.rel_id, slot.pos) else {
            return false;
        };
        self.class(class_id)
            .map(|c| c.contradictory)
            .unwrap_or(false)
    }

    /// Convenience lookup for a slot fixed to a constant in its class.
    #[must_use]
    #[allow(dead_code)]
    pub fn const_of_slot(&self, rel_id: NodeId, pos: usize) -> Option<&Value> {
        let class_id = self.class_of_slot(rel_id, pos)?;
        self.const_of_class(class_id)
    }

    /// Number of output slots tracked for a relational node, if the analyzer
    /// visited it. Returns `0` for nodes outside the analyzed subtree.
    #[must_use]
    pub fn slot_count(&self, rel_id: NodeId) -> usize {
        self.slot_classes
            .get(&rel_id)
            .map(|slots| slots.len())
            .unwrap_or(0)
    }

    /// Are two output slots provably equal when reasoning is performed from
    /// the standpoint of `at_rel`?
    ///
    /// `at_rel` selects which [`ResolvedScope`] (per-`LEFT JOIN` cross-side
    /// equalities) is visible.  For inner joins and other relational nodes
    /// without a scope entry the answer collapses to "same global class".
    /// Returns `false` if either slot has no class assigned at all.
    #[must_use]
    pub fn are_equal(&self, at_rel: NodeId, a: Slot, b: Slot) -> bool {
        let (Some(ca), Some(cb)) = (
            self.class_of_slot(a.rel_id, a.pos),
            self.class_of_slot(b.rel_id, b.pos),
        ) else {
            return false;
        };
        if ca == cb {
            return true;
        }
        let Some(scope) = self.scopes.get(&at_rel) else {
            return false;
        };
        let ra = scope.class_alias.get(&ca);
        let rb = scope.class_alias.get(&cb);
        ra.is_some() && rb.is_some() && ra == rb
    }

    /// Is `slot` pinned to a known constant when reasoning is performed from
    /// the standpoint of `at_rel`?  Honors the same scope as `are_equal`.
    ///
    /// Returns an owned `Value` because a [`ResolvedScope`] may surface a
    /// constant that has no entry in the global class' `constant` field.
    #[must_use]
    pub fn pinned_constant(&self, at_rel: NodeId, slot: Slot) -> Option<Value> {
        let class = self.class_of_slot(slot.rel_id, slot.pos)?;
        // Scope-aware path: if the class is part of a scope-group at
        // `at_rel`, the group's pre-resolved constant overrides the
        // global one (scope-only constants live only here).
        if let Some(info) = self.scope_info_for_class(at_rel, class) {
            if let Some(v) = info.constant.as_ref() {
                return Some(v.clone());
            }
        }
        self.const_of_class(class).cloned()
    }

    /// Find an output position on `inner_rel` that is provably equal to
    /// `outer_slot` from the standpoint of `at_rel`.
    ///
    /// Returns the lowest matching position for deterministic test output.
    /// When several outer positions of a composite key share an
    /// equivalence class (so they're all class-equal to the same inner
    /// position), each call returns that single inner position — the
    /// resulting repeated key (e.g. `Key([q, q])`) is semantically
    /// correct, because hash equality follows from class equality.
    ///
    /// `EquivalenceClass::members` is sorted by `(arena_type, offset,
    /// pos)`, so all members with the same `rel_id` form a contiguous
    /// run and the linear scan is tight.  Loop bounds:
    /// `candidate_classes` is 1 plus (scope-group-size - 1) — typically
    /// 1-3 entries.  Inner loops over class members are also bounded by
    /// the number of base columns that ever appear in `=` facts together;
    /// small constants in practice.
    #[must_use]
    pub fn find_equal_position(
        &self,
        at_rel: NodeId,
        outer_slot: Slot,
        inner_rel: NodeId,
    ) -> Option<usize> {
        let outer_class = self.class_of_slot(outer_slot.rel_id, outer_slot.pos)?;

        // Collect every global class that is scope-equivalent to
        // outer_class.  Pre-resolved in [`ResolvedScope::repr_info`].
        let mut candidate_classes: Vec<ClassId> = vec![outer_class];
        if let Some(info) = self.scope_info_for_class(at_rel, outer_class) {
            for &c in info.members.iter() {
                if c != outer_class {
                    candidate_classes.push(c);
                }
            }
        }

        let mut best: Option<usize> = None;
        for class in candidate_classes {
            let Some(eclass) = self.class(class) else {
                continue;
            };
            for member in eclass.members.iter() {
                if member.rel_id != inner_rel {
                    continue;
                }
                best = Some(match best {
                    Some(cur) if cur <= member.pos => cur,
                    _ => member.pos,
                });
            }
        }
        best
    }
}

/// Internal UF node used during scope resolution at freeze time.
/// Carries either an already-built global [`ClassId`] or a literal value
/// from a scope-only constant fact.
///
/// Constants are deliberately *not* domain-tagged: the whole point of
/// scope resolution is to bridge domains (left side vs. right side of an
/// outer join) via the ON condition.  Two scope facts `(slot_a, 1)` and
/// `(slot_b, 1)` must unify slot_a's class with slot_b's class even when
/// the slots live in different global domains.
#[derive(Clone, Eq, PartialEq, Hash)]
enum ScopedNode {
    Class(ClassId),
    Const(Value),
}

struct EqualityFactsBuilder {
    members: UnionFind<EqualityFactAtom>,
    domains: HashMap<NodeId, DomainId>,
    scoped: HashMap<NodeId, ScopedFacts>,
}

impl EqualityFactsBuilder {
    fn new() -> Self {
        Self {
            members: UnionFind::with_capacity(0),
            domains: HashMap::new(),
            scoped: HashMap::new(),
        }
    }

    fn scoped_entry(&mut self, join_id: NodeId) -> &mut ScopedFacts {
        self.scoped.entry(join_id).or_default()
    }

    /// Pin `rel_id` to `domain`, once per node in `analyze` before its slots.
    /// The assert catches ANY re-entry (not just a different domain): with
    /// `SlotKey` untagged, a node analyzed twice would silently merge its slots.
    /// A trip means a shared subtree slipped past `visited_shared_bodies`.
    fn record_domain(&mut self, rel_id: NodeId, domain: DomainId) {
        let prev = self.domains.insert(rel_id, domain);
        debug_assert!(prev.is_none(), "rel_id analyzed twice (shared subtree?)");
    }

    fn add_slot(&mut self, key: SlotKey) -> UnionFindGroup {
        // The rel's domain is recorded by `record_domain` in `analyze` before
        // any of its slots are added, so the pin already exists here.
        debug_assert!(
            self.domains.contains_key(&key.rel_id),
            "slot added before its rel's domain was recorded"
        );
        self.members.add(EqualityFactAtom::Slot(key))
    }

    fn union_slot_and_slot(&mut self, l: SlotKey, r: SlotKey) {
        let left = self.add_slot(l);
        let right = self.add_slot(r);

        self.members.union_groups(left, right);
    }

    /// Convert a partition atom to its interned form. Pure: slots keep their
    /// `(rel_id, output_idx)` key — the rel's domain is pinned by `record_domain`
    /// in `analyze` — while constants and params carry their domain tag.
    fn intern_atom(atom: FactAtom) -> EqualityFactAtom {
        match atom {
            FactAtom::Slot(key) => EqualityFactAtom::Slot(key),
            FactAtom::Const { domain_id, value } => EqualityFactAtom::Constant(domain_id, value),
            FactAtom::Param {
                domain_id,
                param_index,
            } => EqualityFactAtom::Param(domain_id, param_index),
            FactAtom::Other => unreachable!("FactAtom::Other should not reach the global builder"),
        }
    }

    /// Union every atom of one equivalence class into the global union-find as
    /// a star around the first atom. Handles `slot = slot`, `slot = const`,
    /// `slot = param` and the slotless `const = param` gate (`x = 1 AND x = $1`
    /// surviving an `OR`) uniformly — the class emerges from the union-find, no
    /// pairwise facts are materialized.
    fn union_atoms(&mut self, group: FactGroup) {
        self.members
            .union_atoms(group.into_iter().map(Self::intern_atom));
    }

    fn freeze(mut self) -> EqualityFacts {
        let groups = self.members.groups_number();

        let mut root_to_class: HashMap<UnionFindGroup, ClassId> = HashMap::with_capacity(groups);
        // Parallel arrays indexed by ClassId.0 — grown lazily as new groups
        // are encountered.  Final EquivalenceClass objects are built once
        // the walk is complete.
        let mut class_const: Vec<Option<Value>> = Vec::with_capacity(groups);
        let mut const_conflict: Vec<bool> = Vec::with_capacity(groups);
        let mut class_params: Vec<Vec<u16>> = Vec::with_capacity(groups);
        let mut class_members: Vec<Vec<Slot>> = Vec::with_capacity(groups);
        let mut slot_classes: HashMap<NodeId, Vec<Option<ClassId>>> =
            HashMap::with_capacity(self.domains.len());

        for (atom, group) in self.members.into_groups() {
            let class_id = *root_to_class.entry(group).or_insert_with(|| {
                let id = ClassId(class_const.len() as u32);
                class_const.push(None);
                const_conflict.push(false);
                class_params.push(Vec::new());
                class_members.push(Vec::new());
                id
            });

            match atom {
                EqualityFactAtom::Constant(_, value) => {
                    // Multiple constants in the same UF group can happen
                    // either inside one AND-region (already killed by
                    // `LocalFacts::into_facts` as `DeriveOutcome::Dead`) or
                    // across different expressions: e.g. `WHERE a = 1`
                    // upstream and `WHERE a = 2` downstream both emit
                    // `SlotConst(a, …)`, both get globally unioned, and
                    // both constants land in `a`'s class.
                    //
                    // The class is semantically unsatisfiable in that
                    // case — analogous to PostgreSQL's `ec_broken`.  We
                    // drop the constant (so consumers don't pin slots to
                    // a value that contradicts the other constraint) and
                    // mark the class contradictory so callers that care
                    // can prune the plan slice to empty.
                    let i = class_id.0 as usize;
                    if const_conflict[i] {
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
                EqualityFactAtom::Param(_, param_index) => {
                    // Keep every distinct param as a class member.
                    let params = &mut class_params[class_id.0 as usize];
                    if !params.contains(&param_index) {
                        params.push(param_index);
                    }
                }
                EqualityFactAtom::Slot(SlotKey { rel_id, output_idx }) => {
                    let classes = slot_classes.entry(rel_id).or_default();
                    if classes.len() <= output_idx {
                        classes.resize(output_idx + 1, None);
                    }
                    classes[output_idx] = Some(class_id);
                    class_members[class_id.0 as usize].push(Slot::new(rel_id, output_idx));
                }
            }
        }

        // Materialize the class objects.  Members are sorted by
        // `(arena_type, offset, pos)` for deterministic iteration
        let classes: Box<[EquivalenceClass]> = class_const
            .into_iter()
            .zip(const_conflict)
            .zip(class_params)
            .zip(class_members)
            .map(|(((constant, contradictory), mut params), mut members)| {
                members.sort();
                params.sort_unstable();
                EquivalenceClass {
                    members: members.into_boxed_slice(),
                    params: params.into_boxed_slice(),
                    constant,
                    contradictory,
                }
            })
            .collect();

        // Resolve every per-LEFT-JOIN scope once, using the just-built
        // class index.  After this point `ScopedFacts` is discarded —
        // adapter queries only see [`ResolvedScope`].  Empty scopes are
        // filtered out: see [`ResolvedScope::is_empty`].
        let scopes: HashMap<NodeId, ResolvedScope> = self
            .scoped
            .into_iter()
            .filter_map(|(join_id, raw)| {
                let resolved = ResolvedScope::from_raw(raw, &slot_classes, &classes);
                (!resolved.is_empty()).then_some((join_id, resolved))
            })
            .collect();

        EqualityFacts {
            slot_classes: slot_classes
                .into_iter()
                .map(|(rel_id, v)| (rel_id, v.into_boxed_slice()))
                .collect(),
            classes,
            domains: self.domains,
            scopes,
        }
    }
}

/// Public for the whitebox tests in `sql-planner/tests/planner_tests`;
/// production code runs the analysis through
/// [`Plan::analyze_equality_facts`].
pub struct EqualityAnalysis<'p> {
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
    ) -> Result<(), SbroadError> {
        let output_len = self
            .plan
            .get_row_list(self.plan.get_relational_output(rel_id)?)?
            .len();
        for output_idx in 0..output_len {
            self.builder.union_slot_and_slot(
                SlotKey { rel_id, output_idx },
                SlotKey {
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
                    rel_id,
                    output_idx: pos,
                },
                SlotKey {
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
                    rel_id,
                    output_idx: pos,
                },
                SlotKey {
                    rel_id: left_id,
                    output_idx: pos,
                },
            );
        }
        if include_right {
            for pos in 0..right_len {
                self.builder.union_slot_and_slot(
                    SlotKey {
                        rel_id,
                        output_idx: left_len + pos,
                    },
                    SlotKey {
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
            self.builder.record_domain(rel_id, domain_id);
            let output = self.plan.get_row_list(rel.output())?;
            for (output_idx, _) in output.iter().enumerate() {
                self.builder.add_slot(SlotKey { rel_id, output_idx });
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
                    self.union_projection_output(rel_id, *child_id)?;
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
            Relational::Selection(Selection { child, .. }) => {
                self.analyze(*child, domain_id)?;
                self.union_passthrough_output(rel_id, *child)?;
                self.apply_expr_facts(rel_id, domain_id)?;
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
                    self.union_join_output(rel_id, *left, *right, true)?;
                    self.apply_expr_facts(rel_id, domain_id)?;
                }
                JoinKind::LeftOuter => {
                    self.analyze(*left, domain_id)?;
                    let right_domain = self.fresh_domain();
                    self.analyze(*right, right_domain)?;
                    self.union_join_output(rel_id, *left, *right, false)?;
                    // The ON condition is unsafe to apply globally because
                    // unmatched rows null-extend the right side.  Inside the
                    // join's local scope it is safe, however, and motion
                    // planning needs cross-side equalities (`outer.dk =
                    // inner.dk`) to detect co-located outer joins.  Stash the
                    // derived facts in a per-join scope.
                    //
                    // A fresh domain is used for the derivation so that
                    // any future change to `LocalFacts` (e.g. seeding it from
                    // the global UF) cannot accidentally bridge scoped
                    // collection back into global state — the domain choice
                    // is internal to one derivation pass and stripped before
                    // storage.
                    //
                    // TODO: as an IR-level optimization (not part of the
                    // eclass module), split ON into left-only / right-only
                    // / cross predicates and push the right-only part as
                    // a filter into the right subtree before the LJ.
                    // Safe rewrite: a b-row can only match if it satisfies
                    // the right-only predicate, so pre-filtering b doesn't
                    // change which rows null-extend.  After the push, the
                    // right subtree's eclass sees those facts globally in
                    // its own (fresh) domain.
                    //
                    // Cross-side and left-only predicates stay scoped to
                    // the LJ — neither holds globally above the join:
                    // - left-only: null-extended rows can have any value,
                    //   so the predicate isn't true for all output rows;
                    // - cross-side: null-extension on the right breaks
                    //   the equality on unmatched rows.
                    // Both are correctly captured by `ScopedFacts` already.
                    let scope_domain = self.fresh_domain();
                    self.collect_scoped_facts(*condition, scope_domain, rel_id)?;
                }
            },

            Relational::Motion(_) => {
                unreachable!("Unexpected motion node, we don't support analyze of them");
            }

            Relational::ScanSubQuery(ScanSubQuery { child, .. }) => {
                if self.is_safe_subtree(*child)? {
                    self.analyze(*child, domain_id)?;
                    self.union_passthrough_output(rel_id, *child)?;
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

    /// Derives the global equality facts for one relational node's filter or INNER
    /// join condition and merges them into the global builder.
    fn apply_expr_facts(&mut self, rel_id: NodeId, domain: DomainId) -> Result<(), SbroadError> {
        let Some(restr) = self
            .plan
            .restrictions
            .as_ref()
            .and_then(|r| r.for_rel(rel_id))
        else {
            return Err(SbroadError::Invalid(
                Entity::Plan,
                Some("Restrictions should be set".to_smolstr()),
            ));
        };

        // The whole per-node filter is one AND-region: every clause is a
        // conjunct.
        let clauses: Vec<NodeId> = restr.clauses().iter().map(|c| c.clause()).collect();
        let Facts(partition) = self.derive_conjuncts(&clauses, &HashSet::new(), domain)? else {
            return Ok(());
        };
        for group in partition {
            self.builder.union_atoms(group);
        }
        Ok(())
    }

    /// Collects facts derived from a `LEFT JOIN` ON condition into a scope
    /// keyed by the join node.  These facts are NOT propagated globally
    /// because the nullable side breaks them for unmatched rows.  Motion
    /// planning consults the scoped facts by passing the join's `NodeId` as
    /// `at_rel` to the adapter API (e.g. [`EqualityFacts::are_equal`]) to detect
    /// co-location inside the join scope only.
    fn collect_scoped_facts(
        &mut self,
        expr_id: NodeId,
        domain: DomainId,
        join_id: NodeId,
    ) -> Result<(), SbroadError> {
        let Facts(partition) = self.derive(expr_id, &HashSet::new(), domain)? else {
            return Ok(());
        };
        let scoped = self.builder.scoped_entry(join_id);
        for mut group in partition {
            // Star over the class's slots plus its pinned constant. Params (and
            // the slotless const=param gate) are not surfaced in `LEFT JOIN`
            // scoped facts yet; they only feed the global pins for enrichment.
            let mut slots = group.iter().filter_map(|atom| match atom {
                FactAtom::Slot(slot) => Some(slot),
                _ => None,
            });
            let Some(rep) = slots.next() else {
                continue;
            };
            let rep = rep.clone();
            for slot in slots {
                scoped
                    .extra_slot_eq
                    .push(((rep.rel_id, rep.output_idx), (slot.rel_id, slot.output_idx)));
            }
            if let Some(value) = group.drain(..).find_map(|atom| match atom {
                FactAtom::Const { value, .. } => Some(value),
                _ => None,
            }) {
                scoped
                    .extra_slot_const
                    .push(((rep.rel_id, rep.output_idx), value));
            }
        }
        Ok(())
    }

    /// Recursively derives the equality facts that hold for `node`, given the
    /// slots already known NULL in the enclosing conjunction (`nulls_in`).
    ///
    /// `AND` = merge (union-find over all conjuncts); `OR` = intersect the facts
    /// of the satisfiable arms. No DNF expansion — polynomial in the tree size.
    /// Replaces the old per-DNF-chain analysis: an OR arm here plays the role a
    /// DNF chain used to, and the intersection is computed directly on the tree.
    fn derive(
        &self,
        node: NodeId,
        nulls_in: &HashSet<SlotKey>,
        domain: DomainId,
    ) -> Result<DeriveOutcome, SbroadError> {
        if let Expression::Bool(BoolExpr {
            op: Bool::Or,
            left,
            right,
            ..
        }) = self.plan.get_expression_node(node)?
        {
            // Enclosing NULLs apply to both arms; dead arms are skipped, the
            // live arms' facts are intersected.
            let left = self.derive(*left, nulls_in, domain)?;
            let right = self.derive(*right, nulls_in, domain)?;
            return Ok(DeriveOutcome::or(left, right));
        }
        let conjuncts = self.plan.nodes.and_conjuncts(node);
        self.derive_conjuncts(&conjuncts, nulls_in, domain)
    }

    /// Processes one maximal `AND`-region: a flat list of conjuncts, each of which
    /// may itself be an `OR`. This is the old `collect_chain_facts`, generalized
    /// so an `OR` conjunct contributes the intersection of its arms' facts.
    fn derive_conjuncts(
        &self,
        conjuncts: &[NodeId],
        nulls_in: &HashSet<SlotKey>,
        domain: DomainId,
    ) -> Result<DeriveOutcome, SbroadError> {
        // Pass 1 collects slots that are known NULL via `IS NULL` (plus any
        // inherited from the enclosing conjunction); pass 2 then kills the whole
        // region if any `=` predicate touches such a slot, because
        // `NULL = <anything>` is UNKNOWN — never TRUE. Splitting the scans lets
        // `IS NULL` and `=` appear in any order.
        let mut null_slots: Cow<HashSet<SlotKey>> = Cow::Borrowed(nulls_in);
        for node_id in conjuncts {
            let child = match self.plan.get_expression_node(*node_id)? {
                Expression::Unary(UnaryExpr {
                    op: Unary::IsNull,
                    child,
                }) => child,
                // The region is already dead if one conjunct is false or null.
                Expression::Constant(Constant {
                    value: Value::Boolean(false),
                })
                | Expression::Constant(Constant { value: Value::Null }) => {
                    return Ok(DeriveOutcome::Dead)
                }
                _ => continue,
            };
            if let Expression::Reference(Reference {
                target: ReferenceTarget::Single(rel_id),
                position,
                ..
            }) = self.plan.get_expression_node(*child)?
            {
                null_slots.to_mut().insert(SlotKey {
                    rel_id: *rel_id,
                    output_idx: *position,
                });
            }
        }

        let mut local = LocalFacts::default();
        for node_id in conjuncts {
            match self.plan.get_expression_node(*node_id)? {
                Expression::Bool(BoolExpr {
                    op: Bool::Eq,
                    left,
                    right,
                    ..
                }) => {
                    let Some(left_terms) = self.extract_equality_terms(*left, domain)? else {
                        return Ok(DeriveOutcome::Dead);
                    };
                    let Some(right_terms) = self.extract_equality_terms(*right, domain)? else {
                        return Ok(DeriveOutcome::Dead);
                    };
                    if left_terms.len() != right_terms.len() {
                        continue;
                    }
                    for (left_term, right_term) in left_terms.into_iter().zip(right_terms) {
                        if let FactAtom::Slot(ref slot) = left_term.atom {
                            if null_slots.contains(slot) {
                                return Ok(DeriveOutcome::Dead);
                            }
                        }
                        if let FactAtom::Slot(ref slot) = right_term.atom {
                            if null_slots.contains(slot) {
                                return Ok(DeriveOutcome::Dead);
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
                // A nested OR conjunct: derive its facts (with the region's NULLs
                // in scope) and fold them back into the region's union-find. A
                // dead OR makes the whole region unsatisfiable.
                Expression::Bool(BoolExpr { op: Bool::Or, .. }) => {
                    let Facts(partition) = self.derive(*node_id, null_slots.as_ref(), domain)?
                    else {
                        return Ok(DeriveOutcome::Dead);
                    };
                    // Fold each class the OR implies into the region's union-find
                    // as a star around its first atom.
                    for group in partition {
                        local.union_atoms(group);
                    }
                }
                _ => {}
            }
        }

        Ok(local.into_partition())
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
                    rel_id: *rel_id,
                    output_idx: *position,
                }),
                ty: *col_type,
            },
            Expression::Constant(Constant { value }) => {
                // `value.eq(value) != True` catches both NaN (floats where
                // NaN != NaN) and NULL (Null.eq(Null) = Unknown).  In both
                // cases the equality predicate can never evaluate to TRUE,
                // so the whole AND-region is unsatisfiable.
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
#[derive(PartialEq, Eq, Hash, Clone)]
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

/// One equivalence class derived from a satisfiable region: the group of
/// [`FactAtom`]s (slots, the pinned constant, params) that must all be equal.
/// A [`Partition`] is a set of these; each carries at least two atoms (alone
/// atom implies nothing).
type FactGroup = Vec<FactAtom>;

/// The equivalence relation a region implies, as a list of its non-trivial
/// classes. Carried through [`DeriveOutcome`] so an `OR` intersects the arms'
/// *partitions* directly and consumers fold each class into a union-find as a
/// star (no pairwise `SlotEq` product is ever materialized).
type Partition = Vec<FactGroup>;

/// Result of deriving facts from one `AND`-region (conjunction).
///
/// The two variants carry different meaning for [`DeriveOutcome::or`], which
/// intersects the live arms' partitions and skips dead ones:
///
/// - [`DeriveOutcome::Dead`] — the region is unsatisfiable (e.g. `a = 1 AND
///   a = 2`, `a = NULL`, `a IS NULL AND a = b`).  An `OR` MUST skip it
///   entirely and treat it as if the arm did not exist, otherwise an
///   intersection with the live arms would wrongly wipe out real facts.
/// - [`DeriveOutcome::Facts`] — the region is live.  The carried [`Partition`]
///   may be empty, which is distinct from `Dead`: an empty partition means
///   "satisfiable but no equality facts can be derived", and intersecting it
///   with other arms correctly collapses the result to empty.
enum DeriveOutcome {
    Dead,
    Facts(Partition),
}

impl DeriveOutcome {
    /// Combines two `OR` arms: a dead (unsatisfiable) arm contributes no
    /// constraint and is skipped; the live arms' partitions are intersected so
    /// only equalities holding in *every* arm survive.
    fn or(left: DeriveOutcome, right: DeriveOutcome) -> DeriveOutcome {
        match (left, right) {
            (DeriveOutcome::Dead, live) | (live, DeriveOutcome::Dead) => live,
            (DeriveOutcome::Facts(a), DeriveOutcome::Facts(b)) => {
                DeriveOutcome::Facts(intersect_partitions(a, b))
            }
        }
    }
}

/// Map every atom of a partition to the index of the class it belongs to.
fn atom_to_class(mut partition: Partition) -> HashMap<FactAtom, usize> {
    let mut map = HashMap::new();
    for (class, group) in partition.drain(..).enumerate() {
        for atom in group {
            map.insert(atom, class);
        }
    }
    map
}

/// Intersect two arms' partitions: two atoms end up in one class iff they are
/// grouped in *both* arms. Computed by labelling each atom with its `(class in
/// a, class in b)` pair and grouping by label (no pairwise product). An atom
/// absent from either arm can only be a singleton here, so only atoms present
/// in both partitions are considered.
fn intersect_partitions(a: Partition, b: Partition) -> Partition {
    let a_class = atom_to_class(a);
    let mut b_class = atom_to_class(b);
    let mut by_label: HashMap<(usize, usize), FactGroup> = HashMap::new();
    for (atom, a_id) in a_class {
        if let Some(b_id) = b_class.remove(&atom) {
            by_label.entry((a_id, b_id)).or_default().push(atom);
        }
    }
    by_label
        .into_values()
        .filter(|group| group.len() >= 2)
        .collect()
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

    fn union_atoms(&mut self, group: FactGroup) {
        self.members.union_atoms(group);
    }

    /// Freeze the region's union-find into its [`Partition`]: one group of
    /// atoms per equivalence class, keeping only non-trivial classes (>= 2
    /// atoms). A class holding two distinct constants (`a = 1 AND a = 2`) makes
    /// the whole `AND`-region unsatisfiable, so we return [`DeriveOutcome::Dead`].
    fn into_partition(mut self) -> DeriveOutcome {
        let n = self.members.groups_number();
        let mut root_to_group: HashMap<UnionFindGroup, usize> = HashMap::with_capacity(n);
        let mut groups: Vec<FactGroup> = Vec::with_capacity(n);
        let mut const_by_root: Vec<Option<Value>> = Vec::with_capacity(n);

        for (atom, root) in self.members.into_groups() {
            let i = *root_to_group.entry(root).or_insert_with(|| {
                let id = groups.len();
                groups.push(Vec::new());
                const_by_root.push(None);
                id
            });
            if let FactAtom::Const { value, .. } = &atom {
                match &const_by_root[i] {
                    None => const_by_root[i] = Some(value.clone()),
                    // Two distinct constants (`a = 1 AND a = 2`) make the class
                    // unsatisfiable, so the whole AND-region is false.
                    Some(seen) => {
                        if seen.eq(value) != Trivalent::True {
                            return DeriveOutcome::Dead;
                        }
                    }
                }
            }
            groups[i].push(atom);
        }

        DeriveOutcome::Facts(groups.into_iter().filter(|g| g.len() >= 2).collect())
    }
}

impl Plan {
    pub fn analyze_equality_facts_in_subtree(
        mut self,
        top_id: NodeId,
    ) -> Result<Self, SbroadError> {
        self.facts = EqualityAnalysis::get_equality_facts(&self, top_id).map(Some)?;
        Ok(self)
    }

    pub fn analyze_equality_facts(self) -> Result<Self, SbroadError> {
        let top_id = self.get_top()?;
        self.analyze_equality_facts_in_subtree(top_id)
    }
}
