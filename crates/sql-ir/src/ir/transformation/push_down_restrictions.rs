//! Moving restriction clauses down the tree, the way PostgreSQL distributes
//! `WHERE` quals to the lowest relation that can evaluate them.
//!
//! Enrichment rewrites what the equality classes cover; every other conjunct,
//! an `OR` above all, stays at its `WHERE` or INNER `ON`. This pass moves each
//! such conjunct as a whole [`RestrictInfo`] (an `OR` is never split into its
//! arms) to the deepest [`Placement`] it can reach:
//!
//! - the walk starts at the conjunct's owner and never leaves its domain
//!   ([`EqualityFacts::same_domain`]), so it stops at an aggregate, `LIMIT`,
//!   set operation, CTE, outer join's nullable side and every other boundary
//!   the analysis draws;
//! - every column the conjunct reads must map exactly onto the new place:
//!   through a `Selection` or a `ScanSubQuery` by position, through a
//!   `Projection` only along a bare column reference, through a join to the
//!   operand the column comes from;
//! - an INNER join whose two operands both feed the conjunct hosts it in `ON`;
//!   a LEFT join is descended into its preserved operand only, and its `ON`
//!   never hosts;
//! - a node with several parents (a shared body) is neither changed nor
//!   descended into, an expression subquery body is never entered, and a new
//!   filter goes only on an edge [`Plan::new_filter_placement`] supports.
//!
//! The last valid place on the way wins; with none, the conjunct stays.
//!
//! A conjunct may reference subqueries: they are uncorrelated, so the
//! conjunct's columns alone choose the place, and the bodies move with it
//! (their owner's `subqueries` list follows the condition, see
//! `Plan::apply_placements`). A body keeps one owner: the conjuncts of an
//! owner that share bodies, even transitively, move as one component to the
//! deepest place that sees all their columns, which may be above where some
//! of them would go alone, see [`Plan::place_component`]. The walk never
//! enters a body.
//!
//! Left in place for now: a component that reads no column, holds a conjunct
//! calling a volatile function or an aggregate outside a subquery body, or
//! uses a body another node lists too.

use crate::errors::{Entity, SbroadError};
use crate::ir::columns::RelColumn;
use crate::ir::helpers::RepeatableState;
use crate::ir::node::expression::{Expression, MutExpression};
use crate::ir::node::relational::Relational;
use crate::ir::node::{
    Join, NodeId, Projection, Reference, ReferenceTarget, ScanSubQuery, Selection,
};
use crate::ir::operator::JoinKind;
use crate::ir::subtree_cloner::SubtreeCloner;
use crate::ir::transformation::equality_facts::{EqualityFacts, Slot};
use crate::ir::transformation::placement::{Dropped, Grouped, Placement};
use crate::ir::transformation::restriction::RestrictInfo;
use crate::ir::tree::traversal::{PostOrderWithFilter, EXPR_CAPACITY, REL_CAPACITY};
use crate::ir::{Node, Plan};
use ahash::{AHashMap, AHashSet};
use smallvec::SmallVec;

/// Each column a clause reads, as `(at its owner, at the current place)`.
type Remap = SmallVec<[(Slot, Slot); 2]>;

/// Where one [`linked_conjuncts`] component moves, shared by its conjuncts.
struct ComponentPlan {
    placement: Placement,
    /// Each column the component reads: at its owner -> at `placement`.
    columns: AHashMap<Slot, Slot>,
}

/// One planned move of a conjunct from its owner's condition.
struct Move {
    owner: NodeId,
    clause: NodeId,
    /// Index of its component's plan in [`PushDown::plans`].
    plan: usize,
}

/// Every planned move, in owner and then conjunct order.
#[derive(Default)]
struct PushDown {
    plans: Vec<ComponentPlan>,
    moves: Vec<Move>,
}

/// Group the conjuncts of one owner by the subquery bodies they share,
/// transitively: `A(S1)`, `B(S1, S2)` and `C(S2)` form one component. Return
/// each conjunct's component as the index of its first conjunct; a conjunct
/// without subqueries is its own component.
fn linked_conjuncts(clauses: &[RestrictInfo]) -> Vec<usize> {
    fn find(parent: &mut [usize], mut index: usize) -> usize {
        while parent[index] != index {
            parent[index] = parent[parent[index]];
            index = parent[index];
        }
        index
    }
    let mut parent: Vec<usize> = (0..clauses.len()).collect();
    let mut first_user: AHashMap<NodeId, usize> = AHashMap::new();
    for (index, info) in clauses.iter().enumerate() {
        for &subquery in info.subqueries() {
            let other = *first_user.entry(subquery).or_insert(index);
            let (a, b) = (find(&mut parent, index), find(&mut parent, other));
            // The lower index is the root, so a component is named by its
            // first conjunct.
            let (low, high) = if a < b { (a, b) } else { (b, a) };
            parent[high] = low;
        }
    }
    (0..clauses.len())
        .map(|index| find(&mut parent, index))
        .collect()
}

impl Plan {
    /// Move restriction conjuncts down to the deepest place that can evaluate
    /// them. Requires restrictions and equality facts; without them, nothing
    /// changes.
    ///
    /// # Errors
    /// - Reading a node, copying a clause or splicing a filter fails.
    pub fn push_down_restrictions(mut self, top_id: NodeId) -> Result<Self, SbroadError> {
        let PushDown { plans, moves } = self.plan_push_down(top_id)?;
        if moves.is_empty() {
            return Ok(self);
        }
        // The tree is unchanged so far: every move's owner, placement and
        // columns name nodes as they were planned.
        let mut grouped = Grouped::with_hasher(RepeatableState);
        let mut dropped = Dropped::new();
        // Conjunct order, not component order: the conjuncts of components
        // landing in one place keep their order there.
        for Move {
            owner,
            clause,
            plan,
        } in moves
        {
            let ComponentPlan { placement, columns } = &plans[plan];
            let Some(owner_placement) = self.fold_placement(owner)? else {
                unreachable!("restriction owner {owner:?} is no Selection or INNER Join");
            };
            dropped.entry(owner_placement).or_default().insert(clause);
            // A copy: the original may be shared, e.g. by the undo journal.
            let moved = self.relocate_clause(clause, columns)?;
            let info = self.restrict_info(moved)?;
            grouped.entry(*placement).or_default().push(info);
        }
        let _ = self.apply_placements(grouped, dropped)?;
        #[cfg(debug_assertions)]
        self.debug_assert_restrictions_fresh(top_id);
        Ok(self)
    }

    /// Plan the moves of every movable conjunct, owners and their conjuncts in
    /// a stable order, without changing the tree. The unit of a move is a
    /// [`linked_conjuncts`] component: its conjuncts share one placement.
    ///
    /// # Errors
    /// - Reading a node fails.
    fn plan_push_down(&self, top_id: NodeId) -> Result<PushDown, SbroadError> {
        let mut planned = PushDown::default();
        let (Some(restrictions), Some(facts)) = (self.restrictions.as_ref(), self.facts.as_ref())
        else {
            return Ok(planned);
        };
        let mut owners: Vec<NodeId> = restrictions.by_rel.keys().copied().collect();
        owners.sort_unstable_by_key(|owner| (owner.arena_type, owner.offset));
        let mut fan_in: Option<AHashMap<NodeId, usize>> = None;
        for owner in owners {
            let clauses = restrictions.by_rel[&owner].clauses();
            if clauses.iter().all(|info| info.slots().is_empty()) {
                continue;
            }
            let fan_in = fan_in.get_or_insert_with(|| self.rel_fan_in(top_id));
            let components = linked_conjuncts(clauses);
            // The members of each component in conjunct order, indexed by its
            // root (its first conjunct), gathered in one pass.
            let mut members_by_root: Vec<SmallVec<[usize; 2]>> =
                vec![SmallVec::new(); clauses.len()];
            for (index, &root) in components.iter().enumerate() {
                members_by_root[root].push(index);
            }
            // Per root, the index of its component's plan, if it moves.
            let mut plan_of: Vec<Option<usize>> = vec![None; clauses.len()];
            for (root, members) in members_by_root.iter().enumerate() {
                if members.is_empty() {
                    continue;
                }
                let members: SmallVec<[&RestrictInfo; 2]> =
                    members.iter().map(|&index| &clauses[index]).collect();
                if let Some((placement, remap)) =
                    self.place_component(facts, fan_in, owner, &members)?
                {
                    plan_of[root] = Some(planned.plans.len());
                    planned.plans.push(ComponentPlan {
                        placement,
                        columns: remap.into_iter().collect(),
                    });
                }
            }
            // Each conjunct in its own order, so the new owner keeps it.
            for (info, &root) in clauses.iter().zip(&components) {
                if let Some(plan) = plan_of[root] {
                    planned.moves.push(Move {
                        owner,
                        clause: info.clause(),
                        plan,
                    });
                }
            }
        }
        Ok(planned)
    }

    /// The deepest common place for the conjuncts `members` of `owner`, one
    /// [`linked_conjuncts`] component, see the module docs. `None` when the
    /// component has to stay.
    ///
    /// A subquery body keeps one owner: the motion pass covers a body once
    /// per node listing it, so two owners would run it twice over a body
    /// shared in the DAG. The component holds every conjunct of `owner` using
    /// its bodies, so they all move together; a body some other node lists
    /// as well pins the component.
    ///
    /// # Errors
    /// - Reading a node or an output column fails.
    fn place_component(
        &self,
        facts: &EqualityFacts,
        fan_in: &AHashMap<NodeId, usize>,
        owner: NodeId,
        members: &[&RestrictInfo],
    ) -> Result<Option<(Placement, Remap)>, SbroadError> {
        // A conjunct's own slots are distinct already; a component's are
        // gathered once each, in a stable order for the walk.
        let mut slots: SmallVec<[Slot; 4]> = SmallVec::new();
        if let [info] = members {
            slots.extend_from_slice(info.slots());
        } else {
            let mut seen: AHashSet<Slot> = AHashSet::new();
            for info in members {
                for &slot in info.slots() {
                    if seen.insert(slot) {
                        slots.push(slot);
                    }
                }
            }
        }
        // A column-less component needs its own choice of branch.
        if slots.is_empty() {
            return Ok(None);
        }
        for info in members {
            if info
                .subqueries()
                .iter()
                .any(|subquery| fan_in.get(subquery).is_some_and(|&parents| parents > 1))
            {
                return Ok(None);
            }
            if !self.is_movable_clause(info.clause())? {
                return Ok(None);
            }
        }
        self.place_columns(facts, fan_in, owner, &slots)
    }

    /// The deepest place below `owner` that sees every column of `slots`, and
    /// those columns there. `None` when no place below `owner` does.
    ///
    /// # Errors
    /// - Reading a node or an output column fails.
    fn place_columns(
        &self,
        facts: &EqualityFacts,
        fan_in: &AHashMap<NodeId, usize>,
        owner: NodeId,
        slots: &[Slot],
    ) -> Result<Option<(Placement, Remap)>, SbroadError> {
        // Every column must come from one input of the owner; a clause that
        // needs both operands of its join is where it belongs already.
        let source = slots[0].rel_id;
        if slots.iter().any(|slot| slot.rel_id != source) {
            return Ok(None);
        }
        let is_input = match self.get_relation_node(owner)? {
            Relational::Selection(Selection { child, .. }) => *child == source,
            Relational::Join(Join {
                left,
                right,
                kind: JoinKind::Inner,
                ..
            }) => *left == source || *right == source,
            _ => false,
        };
        if !is_input {
            return Ok(None);
        }

        let mut current: Remap = slots.iter().map(|&slot| (slot, slot)).collect();
        let mut parent = owner;
        let mut node = source;
        let mut best: Option<(Placement, Remap)> = None;
        loop {
            if !facts.same_domain(owner, node) {
                break;
            }
            let shared = fan_in.get(&node).is_some_and(|&parents| parents > 1);
            let rel = self.get_relation_node(node)?;
            if let Relational::Selection(Selection { child, .. }) = rel {
                if !shared {
                    let Some(lowered) = self.lower_columns(&current, *child)? else {
                        break;
                    };
                    best = Some((Placement::ExistingSelection(node), lowered.clone()));
                    (parent, node, current) = (node, *child, lowered);
                    continue;
                }
            }
            if let Some(placement) = self.new_filter_placement(parent, node)? {
                best = Some((placement, current.clone()));
            }
            if shared {
                break;
            }
            match rel {
                Relational::Projection(Projection {
                    child: Some(child), ..
                })
                | Relational::ScanSubQuery(ScanSubQuery { child, .. }) => {
                    let Some(lowered) = self.lower_columns(&current, *child)? else {
                        break;
                    };
                    (parent, node, current) = (node, *child, lowered);
                }
                Relational::Join(Join {
                    left, right, kind, ..
                }) => {
                    let (left, right, inner) = (*left, *right, matches!(kind, JoinKind::Inner));
                    let mut lowered = Remap::with_capacity(current.len());
                    for &(original, slot) in &current {
                        let Some(column) =
                            self.column_source(RelColumn::new(slot.rel_id, slot.pos))?
                        else {
                            unreachable!("a join output column has an operand source");
                        };
                        lowered.push((original, Slot::new(column.rel_id, column.position)));
                    }
                    let on = |side: NodeId| lowered.iter().all(|(_, slot)| slot.rel_id == side);
                    let next = if on(left) {
                        left
                    } else if inner && on(right) {
                        right
                    } else {
                        // An INNER `ON` sees both operands; an outer `ON`
                        // never filters the preserved side.
                        if inner {
                            best = Some((Placement::InnerJoinOn(node), lowered));
                        }
                        break;
                    };
                    (parent, node, current) = (node, next, lowered);
                }
                _ => break,
            }
        }
        Ok(best)
    }

    /// `current` lowered from a node's output to its input `child`: each
    /// column must be a plain copy of a `child` column. `None` otherwise.
    ///
    /// # Errors
    /// - Reading an output column fails.
    fn lower_columns(&self, current: &Remap, child: NodeId) -> Result<Option<Remap>, SbroadError> {
        let mut lowered = Remap::with_capacity(current.len());
        for &(original, slot) in current {
            match self.column_source(RelColumn::new(slot.rel_id, slot.pos))? {
                Some(column) if column.rel_id == child => {
                    lowered.push((original, Slot::new(child, column.position)));
                }
                _ => return Ok(None),
            }
        }
        Ok(Some(lowered))
    }

    /// Reject volatile calls, aggregates and leaf references outside subquery
    /// bodies. Uncorrelated subquery references are allowed; their ownership
    /// is checked separately by `place_component`.
    ///
    /// # Errors
    /// - Reading an expression fails.
    fn is_movable_clause(&self, clause: NodeId) -> Result<bool, SbroadError> {
        if self.contains_volatile(clause, true)? || self.contains_aggregates(clause, true)? {
            return Ok(false);
        }
        for id in self.clause_references(clause) {
            if let Expression::Reference(Reference {
                target: ReferenceTarget::Leaf,
                ..
            }) = self.get_expression_node(id)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// A copy of `clause` whose column references read the columns `columns`
    /// assigns them. Nothing in the original changes.
    ///
    /// # Errors
    /// - Copying the clause fails, or it reads a column `columns` lacks.
    fn relocate_clause(
        &mut self,
        clause: NodeId,
        columns: &AHashMap<Slot, Slot>,
    ) -> Result<NodeId, SbroadError> {
        let copy = SubtreeCloner::clone_subtree(self, clause)?;
        // A clause may share one reference node between several parents
        // (e.g. the needle of an expanded `IN` list), and the copy keeps that
        // sharing: rewrite each node once, or its second visit reads the
        // already relocated target.
        let mut relocated: AHashSet<NodeId> = AHashSet::new();
        for id in self.clause_references(copy) {
            if !relocated.insert(id) {
                continue;
            }
            let MutExpression::Reference(Reference {
                target: ReferenceTarget::Single(target),
                position,
                ..
            }) = self.get_mut_expression_node(id)?
            else {
                continue;
            };
            let from = Slot::new(*target, *position);
            let Some(&to) = columns.get(&from) else {
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some(smol_str::format_smolstr!(
                        "push_down_restrictions: no new place for column {from:?}"
                    )),
                ));
            };
            *target = to.rel_id;
            *position = to.pos;
        }
        Ok(copy)
    }

    /// The `Reference` nodes of `clause`, not entering subqueries.
    fn clause_references(&self, clause: NodeId) -> Vec<NodeId> {
        PostOrderWithFilter::new(
            |node| self.nodes.expr_iter(node, false),
            |node| {
                matches!(
                    self.get_node(node),
                    Ok(Node::Expression(Expression::Reference(_)))
                )
            },
            EXPR_CAPACITY,
        )
        .traverse_into_vec(clause)
    }

    /// Number of distinct parents of each relational node under `top_id`,
    /// counting referenced subquery bodies too. More than one marks a shared
    /// body that a change for one consumer would leak into the others.
    fn rel_fan_in(&self, top_id: NodeId) -> AHashMap<NodeId, usize> {
        let mut fan_in: AHashMap<NodeId, usize> = AHashMap::with_capacity(REL_CAPACITY);
        let mut seen: AHashSet<NodeId> = AHashSet::with_capacity(REL_CAPACITY);
        let mut stack = Vec::with_capacity(REL_CAPACITY);
        stack.push(top_id);
        while let Some(node) = stack.pop() {
            if !seen.insert(node) {
                continue;
            }
            let mut children: SmallVec<[NodeId; 4]> = self.nodes.rel_iter(node).collect();
            // A node listing one child twice (a self-join) is still one parent.
            children.sort_unstable_by_key(|child| (child.arena_type, child.offset));
            children.dedup();
            for child in children {
                *fan_in.entry(child).or_default() += 1;
                stack.push(child);
            }
        }
        fan_in
    }
}
