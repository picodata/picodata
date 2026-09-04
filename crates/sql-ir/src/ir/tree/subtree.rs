use std::cell::RefCell;

use super::{PlanTreeIterator, Snapshot, TreeIterator};
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::Relational;
use crate::ir::node::{
    ArrayLiteral, GroupBy, Having, Join, Motion, NodeId, OrderBy, Projection, Row, ScalarFunction,
    SelectWithoutScan, Selection, SubQueryReference, Values,
};
use crate::ir::operator::{OrderByElement, OrderByEntity};
use crate::ir::{Node, Nodes, Plan};

trait SubtreePlanIterator<'plan>: PlanTreeIterator<'plan> {
    /// Whether to yield the output tuple of a `Motion`. The rows of
    /// `Projection` and `SelectWithoutScan` are always yielded; a motion row
    /// only copies the child's columns, so most traversals skip it.
    fn need_motion_output(&self) -> bool;
    fn need_motion_subtree(&self) -> bool;
    fn output_first(&self) -> bool;
    fn need_subquery(&self) -> bool;
}

/// Expression and relational nodes iterator.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct SubtreeIterator<'plan> {
    current: NodeId,
    child: RefCell<usize>,
    plan: &'plan Plan,
    motion_output: bool,
    output_first: bool,
    traverse_subquery: bool,
}

impl<'nodes> TreeIterator<'nodes> for SubtreeIterator<'nodes> {
    fn get_current(&self) -> NodeId {
        self.current
    }

    fn get_child(&self) -> &RefCell<usize> {
        &self.child
    }

    fn get_nodes(&self) -> &'nodes Nodes {
        &self.plan.nodes
    }
}

impl<'plan> PlanTreeIterator<'plan> for SubtreeIterator<'plan> {
    fn get_plan(&self) -> &'plan Plan {
        self.plan
    }
}

impl<'plan> SubtreePlanIterator<'plan> for SubtreeIterator<'plan> {
    fn need_motion_output(&self) -> bool {
        self.motion_output
    }
    fn need_motion_subtree(&self) -> bool {
        true
    }
    fn output_first(&self) -> bool {
        self.output_first
    }
    fn need_subquery(&self) -> bool {
        self.traverse_subquery
    }
}

impl Iterator for SubtreeIterator<'_> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        subtree_next(self, &Snapshot::Latest)
    }
}

impl<'plan> Plan {
    /// Whole subtree, motion output tuples excluded (the syntax planner
    /// must not see them: a motion renders as its virtual table).
    #[must_use]
    pub fn subtree_iter(&'plan self, current: NodeId) -> SubtreeIterator<'plan> {
        SubtreeIterator {
            current,
            child: RefCell::new(0),
            plan: self,
            motion_output: false,
            output_first: true,
            traverse_subquery: true,
        }
    }

    /// Whole subtree, motion output tuples included (their references are
    /// part of the plan).
    pub fn parameter_iter(&'plan self, current: NodeId) -> SubtreeIterator<'plan> {
        SubtreeIterator {
            current,
            child: RefCell::new(0),
            plan: self,
            motion_output: true,
            output_first: false,
            traverse_subquery: true,
        }
    }

    /// Everything a subtree owns, motion output tuples included, but not the
    /// shared subqueries: the traversal of the subtree cloner.
    pub fn subtree_iter_except_subquery(&'plan self, current: NodeId) -> SubtreeIterator<'plan> {
        SubtreeIterator {
            current,
            child: RefCell::new(0),
            plan: self,
            motion_output: true,
            output_first: false,
            traverse_subquery: false,
        }
    }
}

/// Expression and relational nodes flashback iterator.
/// It uses the UNDO transformation log to go back to the
/// original state of some subtrees in the plan (selections
/// at the moment).
#[derive(Debug)]
pub struct FlashbackSubtreeIterator<'plan> {
    current: NodeId,
    child: RefCell<usize>,
    plan: &'plan Plan,
}

impl<'nodes> TreeIterator<'nodes> for FlashbackSubtreeIterator<'nodes> {
    fn get_current(&self) -> NodeId {
        self.current
    }

    fn get_child(&self) -> &RefCell<usize> {
        &self.child
    }

    fn get_nodes(&self) -> &'nodes Nodes {
        &self.plan.nodes
    }
}

impl<'plan> PlanTreeIterator<'plan> for FlashbackSubtreeIterator<'plan> {
    fn get_plan(&self) -> &'plan Plan {
        self.plan
    }
}

impl<'plan> SubtreePlanIterator<'plan> for FlashbackSubtreeIterator<'plan> {
    fn need_motion_output(&self) -> bool {
        false
    }

    fn need_motion_subtree(&self) -> bool {
        true
    }

    fn output_first(&self) -> bool {
        true
    }

    fn need_subquery(&self) -> bool {
        true
    }
}

impl Iterator for FlashbackSubtreeIterator<'_> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        subtree_next(self, &Snapshot::Oldest)
    }
}

impl<'plan> Plan {
    #[must_use]
    pub fn flashback_subtree_iter(&'plan self, current: NodeId) -> FlashbackSubtreeIterator<'plan> {
        FlashbackSubtreeIterator {
            current,
            child: RefCell::new(0),
            plan: self,
        }
    }
}

/// An iterator used while copying and execution plan subtree.
#[derive(Debug)]
pub struct ExecPlanSubtreeIterator<'plan> {
    current: NodeId,
    child: RefCell<usize>,
    plan: &'plan Plan,
    snapshot_type: Snapshot,
    output_first: bool,
}

impl<'nodes> TreeIterator<'nodes> for ExecPlanSubtreeIterator<'nodes> {
    fn get_current(&self) -> NodeId {
        self.current
    }

    fn get_child(&self) -> &RefCell<usize> {
        &self.child
    }

    fn get_nodes(&self) -> &'nodes Nodes {
        &self.plan.nodes
    }
}

impl<'plan> PlanTreeIterator<'plan> for ExecPlanSubtreeIterator<'plan> {
    fn get_plan(&self) -> &'plan Plan {
        self.plan
    }
}

impl<'plan> SubtreePlanIterator<'plan> for ExecPlanSubtreeIterator<'plan> {
    fn need_motion_output(&self) -> bool {
        true
    }

    fn need_motion_subtree(&self) -> bool {
        false
    }
    fn output_first(&self) -> bool {
        self.output_first
    }
    fn need_subquery(&self) -> bool {
        true
    }
}

impl Iterator for ExecPlanSubtreeIterator<'_> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        let snapshot = self.snapshot_type;
        subtree_next(self, &snapshot)
    }
}

impl<'plan> Plan {
    #[must_use]
    pub fn exec_plan_subtree_iter(
        &'plan self,
        current: NodeId,
        snapshot: Snapshot,
    ) -> ExecPlanSubtreeIterator<'plan> {
        ExecPlanSubtreeIterator {
            current,
            child: RefCell::new(0),
            plan: self,
            snapshot_type: snapshot,
            output_first: false,
        }
    }

    pub fn exec_plan_subtree_output_first_iter(
        &'plan self,
        current: NodeId,
        snapshot: Snapshot,
    ) -> ExecPlanSubtreeIterator<'plan> {
        ExecPlanSubtreeIterator {
            current,
            child: RefCell::new(0),
            plan: self,
            snapshot_type: snapshot,
            output_first: true,
        }
    }
}

#[allow(clippy::too_many_lines)]
fn subtree_next<'plan>(
    iter: &mut impl SubtreePlanIterator<'plan>,
    snapshot: &Snapshot,
) -> Option<NodeId> {
    if let Some(child) = iter.get_nodes().get(iter.get_current()) {
        return match child {
            Node::Invalid(..)
            | Node::Ddl(..)
            | Node::Acl(..)
            | Node::Tcl(..)
            | Node::Block(..)
            | Node::Plugin(..)
            | Node::Deallocate(..) => None,
            Node::Expression(expr) => match expr {
                Expression::Window { .. } => iter.handle_window_iter(expr),
                Expression::Over { .. } => iter.handle_over_iter(expr),
                Expression::Alias { .. } | Expression::Cast { .. } | Expression::Unary { .. } => {
                    iter.handle_single_child(expr)
                }
                Expression::Case { .. } => iter.handle_case_iter(expr),
                Expression::Bool { .. }
                | Expression::Arithmetic { .. }
                | Expression::Concat { .. } => iter.handle_left_right_children(expr),
                Expression::Index { .. } => iter.handle_index_iter(expr),
                Expression::Trim { .. } => iter.handle_trim(expr),
                Expression::Like { .. } => iter.handle_like(expr),
                Expression::Row(Row { list, .. })
                | Expression::ArrayLiteral(ArrayLiteral { list, .. })
                | Expression::ScalarFunction(ScalarFunction { children: list, .. }) => {
                    let child_step = *iter.get_child().borrow();
                    return match list.get(child_step) {
                        None => None,
                        Some(child) => {
                            *iter.get_child().borrow_mut() += 1;
                            Some(*child)
                        }
                    };
                }
                Expression::Constant { .. }
                | Expression::CountAsterisk { .. }
                | Expression::Timestamp { .. }
                | Expression::Reference { .. }
                | Expression::Parameter { .. }
                | Expression::LetVarRef { .. } => None,
                Expression::SubQueryReference(SubQueryReference { rel_id, .. }) => {
                    if !iter.need_subquery() {
                        return None;
                    }
                    let step = *iter.get_child().borrow();
                    if step == 0 {
                        *iter.get_child().borrow_mut() += 1;
                        Some(*rel_id)
                    } else {
                        None
                    }
                }
            },
            Node::Relational(r) => match r {
                Relational::Join(Join {
                    left,
                    right,
                    condition,
                    ..
                }) => {
                    let step = *iter.get_child().borrow();

                    *iter.get_child().borrow_mut() += 1;
                    match step {
                        0 => Some(*left),
                        1 => Some(*right),
                        2 => match snapshot {
                            Snapshot::Latest => Some(*condition),
                            Snapshot::Oldest => {
                                return Some(*iter.get_plan().undo.get_oldest(condition));
                            }
                        },
                        _ => None,
                    }
                }
                Relational::Except(_)
                | Relational::Insert(_)
                | Relational::Intersect(_)
                | Relational::ScanSubQuery(_)
                | Relational::Union(_)
                | Relational::UnionAll(_)
                | Relational::Delete(_)
                | Relational::ScanCte(_)
                | Relational::Limit(_)
                | Relational::Update(_) => {
                    let step = *iter.get_child().borrow();
                    *iter.get_child().borrow_mut() += 1;
                    let children = r.children();
                    if step < children.len() {
                        return children.get(step).copied();
                    }
                    let step = step - children.len();
                    let subqueries = r.subqueries();
                    subqueries.get(step).copied()
                }
                Relational::GroupBy(GroupBy {
                    child, gr_exprs, ..
                }) => {
                    let step = *iter.get_child().borrow();
                    if step == 0 {
                        *iter.get_child().borrow_mut() += 1;
                        return Some(*child);
                    }
                    let col_idx = step - 1;
                    if col_idx < gr_exprs.len() {
                        *iter.get_child().borrow_mut() += 1;
                        return gr_exprs.get(col_idx).copied();
                    }
                    None
                }
                Relational::OrderBy(OrderBy {
                    child,
                    order_by_elements,
                    ..
                }) => {
                    let step = *iter.get_child().borrow();
                    if step == 0 {
                        *iter.get_child().borrow_mut() += 1;
                        return Some(*child);
                    }
                    let mut col_idx = step - 1;
                    while col_idx < order_by_elements.len() {
                        let current_element = order_by_elements
                            .get(col_idx)
                            .expect("Wrong index passed for OrderBy element retrieval.");
                        *iter.get_child().borrow_mut() += 1;
                        if let OrderByElement {
                            entity: OrderByEntity::Expression { expr_id },
                            ..
                        } = current_element
                        {
                            return Some(*expr_id);
                        }
                        col_idx += 1;
                    }
                    None
                }
                Relational::Motion(Motion {
                    child,
                    output,
                    policy,
                    ..
                }) => {
                    if policy.is_local() || iter.need_motion_subtree() {
                        let step = *iter.get_child().borrow();
                        let len = child.iter().len();
                        if step < len {
                            *iter.get_child().borrow_mut() += 1;
                            return *child;
                        }
                        if iter.need_motion_output() && step == len {
                            *iter.get_child().borrow_mut() += 1;
                            return Some(*output);
                        }
                    } else {
                        // A non-local motion whose subtree is not wanted is
                        // represented by its output tuple alone.
                        let step = *iter.get_child().borrow();
                        if iter.need_motion_output() && step == 0 {
                            *iter.get_child().borrow_mut() += 1;
                            return Some(*output);
                        }
                    }
                    None
                }
                Relational::Values(Values { rows, .. }) => {
                    let step = *iter.get_child().borrow();
                    *iter.get_child().borrow_mut() += 1;
                    rows.get(step).copied()
                }
                Relational::SelectWithoutScan(SelectWithoutScan {
                    output, subqueries, ..
                }) => {
                    let step = *iter.get_child().borrow();
                    *iter.get_child().borrow_mut() += 1;

                    let once_output = std::iter::once(*output);
                    let sub_iter = subqueries.iter().copied();

                    if iter.output_first() {
                        once_output.chain(sub_iter).nth(step)
                    } else {
                        sub_iter.chain(once_output).nth(step)
                    }
                }
                Relational::Projection(Projection {
                    output,
                    child,
                    subqueries,
                    group_by,
                    having,
                    ..
                }) => {
                    let step = *iter.get_child().borrow();
                    *iter.get_child().borrow_mut() += 1;
                    let mut step_shift: usize = 0;
                    if iter.output_first() {
                        if step == 0 {
                            return Some(*output);
                        }
                        step_shift += 1;
                    }
                    if having.is_some() {
                        if step - step_shift == 0 {
                            return *having;
                        }
                        step_shift += 1;
                    } else if group_by.is_some() {
                        if step - step_shift == 0 {
                            return *group_by;
                        }
                        step_shift += 1;
                    } else if child.is_some() {
                        if step - step_shift == 0 {
                            return *child;
                        }
                        step_shift += 1;
                    } else {
                        unreachable!("nothing was set");
                    }

                    let subquery_idx = step - step_shift;
                    if subquery_idx < subqueries.len() {
                        return subqueries.get(subquery_idx).copied();
                    } else if !iter.output_first() && subquery_idx == subqueries.len() {
                        return Some(*output);
                    }

                    None
                }
                Relational::Selection(Selection { child, filter, .. })
                | Relational::Having(Having { child, filter, .. }) => {
                    let step = *iter.get_child().borrow();

                    *iter.get_child().borrow_mut() += 1;
                    match step {
                        0 => Some(*child),
                        1 => match snapshot {
                            Snapshot::Latest => Some(*filter),
                            Snapshot::Oldest => {
                                return Some(*iter.get_plan().undo.get_oldest(filter));
                            }
                        },
                        _ => None,
                    }
                }
                Relational::ScanRelation(_) => None,
            },
        };
    }
    None
}
