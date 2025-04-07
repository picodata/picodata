use std::cell::RefCell;
use std::cmp::Ordering;

use super::{PlanTreeIterator, Snapshot, TreeIterator};
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::Relational;
use crate::ir::node::{
    Delete, Except, GroupBy, Having, Insert, Intersect, Join, Limit, Motion, NamedWindows, NodeId,
    OrderBy, Projection, Row, ScalarFunction, ScanCte, ScanRelation, ScanSubQuery,
    SelectWithoutScan, Selection, Union, UnionAll, Update, Values, ValuesRow,
};
use crate::ir::operator::{OrderByElement, OrderByEntity};
use crate::ir::{Node, Nodes, Plan};

trait SubtreePlanIterator<'plan>: PlanTreeIterator<'plan> {
    fn need_output(&self) -> bool;
    fn need_motion_subtree(&self) -> bool;
    fn output_first(&self) -> bool;
}

/// Expression and relational nodes iterator.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct SubtreeIterator<'plan> {
    current: NodeId,
    child: RefCell<usize>,
    plan: &'plan Plan,
    need_output: bool,
    output_first: bool,
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
    fn need_output(&self) -> bool {
        self.need_output
    }

    fn need_motion_subtree(&self) -> bool {
        true
    }
    fn output_first(&self) -> bool {
        self.output_first
    }
}

impl Iterator for SubtreeIterator<'_> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        subtree_next(self, &Snapshot::Latest)
    }
}

impl<'plan> Plan {
    #[must_use]
    pub fn subtree_iter(&'plan self, current: NodeId, need_output: bool) -> SubtreeIterator<'plan> {
        SubtreeIterator {
            current,
            child: RefCell::new(0),
            plan: self,
            need_output,
            output_first: true,
        }
    }

    pub fn parameter_iter(
        &'plan self,
        current: NodeId,
        need_output: bool,
    ) -> SubtreeIterator<'plan> {
        SubtreeIterator {
            current,
            child: RefCell::new(0),
            plan: self,
            need_output,
            output_first: false,
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
    fn need_output(&self) -> bool {
        false
    }

    fn need_motion_subtree(&self) -> bool {
        true
    }

    fn output_first(&self) -> bool {
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
    fn need_output(&self) -> bool {
        true
    }

    fn need_motion_subtree(&self) -> bool {
        false
    }
    fn output_first(&self) -> bool {
        true
    }
}

impl Iterator for ExecPlanSubtreeIterator<'_> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        subtree_next(self, &Snapshot::Oldest)
    }
}

impl<'plan> Plan {
    #[must_use]
    pub fn exec_plan_subtree_iter(&'plan self, current: NodeId) -> ExecPlanSubtreeIterator<'plan> {
        ExecPlanSubtreeIterator {
            current,
            child: RefCell::new(0),
            plan: self,
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
                Expression::Trim { .. } => iter.handle_trim(expr),
                Expression::Like { .. } => iter.handle_like(expr),
                Expression::Row(Row { list, .. })
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
                | Expression::LocalTimestamp { .. }
                | Expression::Parameter { .. } => None,
                Expression::Reference { .. } => {
                    let step = *iter.get_child().borrow();
                    if step == 0 {
                        *iter.get_child().borrow_mut() += 1;

                        // At first we need to detect the place where the reference is used:
                        // for selection filter or a join condition, we need to check whether
                        // the reference points to an **additional** sub-query and then traverse
                        // into it. Otherwise, stop traversal.
                        let Ok(parent_id) = expr.get_parent() else {
                            return None;
                        };
                        if let Ok(rel_id) = iter
                            .get_plan()
                            .get_relational_from_reference_node(iter.get_current())
                        {
                            match iter.get_plan().get_relation_node(rel_id) {
                                Ok(rel_node)
                                    if rel_node.is_subquery_or_cte() || rel_node.is_motion() =>
                                {
                                    let is_additional_child = iter
                                        .get_plan()
                                        .is_additional_child_of_rel(parent_id, rel_id)
                                        .expect(
                                            "Relational node failed to check additional child.",
                                        );
                                    if is_additional_child {
                                        return Some(rel_id);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    None
                }
            },
            Node::Relational(r) => match r {
                Relational::Join(Join {
                    children,
                    condition,
                    output,
                    ..
                }) => {
                    let step = *iter.get_child().borrow();

                    *iter.get_child().borrow_mut() += 1;
                    match step.cmp(&2) {
                        Ordering::Less => {
                            return children.get(step).copied();
                        }
                        Ordering::Equal => match snapshot {
                            Snapshot::Latest => Some(*condition),
                            Snapshot::Oldest => {
                                return Some(*iter.get_plan().undo.get_oldest(condition));
                            }
                        },
                        Ordering::Greater => {
                            if step == 3 && iter.need_output() {
                                return Some(*output);
                            }
                            None
                        }
                    }
                }
                Relational::Except(Except { output, .. })
                | Relational::Insert(Insert { output, .. })
                | Relational::Intersect(Intersect { output, .. })
                | Relational::ScanSubQuery(ScanSubQuery { output, .. })
                | Relational::Union(Union { output, .. })
                | Relational::UnionAll(UnionAll { output, .. }) => {
                    let step = *iter.get_child().borrow();
                    let children = r.children();
                    *iter.get_child().borrow_mut() += 1;
                    if step < children.len() {
                        return children.get(step).copied();
                    }
                    if iter.need_output() && step == children.len() {
                        return Some(*output);
                    }
                    None
                }
                Relational::Delete(Delete { output, .. }) => {
                    let step = *iter.get_child().borrow();
                    let children = r.children();
                    if step < children.len() {
                        *iter.get_child().borrow_mut() += 1;
                        return children.get(step).copied();
                    }
                    if let Some(output) = output {
                        if iter.need_output() && step == children.len() {
                            *iter.get_child().borrow_mut() += 1;
                            return Some(*output);
                        }
                    }
                    None
                }
                Relational::NamedWindows(NamedWindows {
                    child,
                    output,
                    windows,
                    ..
                }) => {
                    let step = *iter.get_child().borrow();
                    if step == 0 {
                        *iter.get_child().borrow_mut() += 1;
                        return Some(*child);
                    }
                    if step <= windows.len() {
                        *iter.get_child().borrow_mut() += 1;
                        return windows.get(step - 1).copied();
                    }
                    if iter.need_output() && step == windows.len() + 1 {
                        *iter.get_child().borrow_mut() += 1;
                        return Some(*output);
                    }
                    None
                }
                Relational::ScanCte(ScanCte { child, output, .. })
                | Relational::Limit(Limit { child, output, .. }) => {
                    let step = *iter.get_child().borrow();
                    if step == 0 {
                        *iter.get_child().borrow_mut() += 1;
                        return Some(*child);
                    }
                    if iter.need_output() && step == 1 {
                        *iter.get_child().borrow_mut() += 1;
                        return Some(*output);
                    }
                    None
                }
                Relational::GroupBy(GroupBy {
                    children,
                    output,
                    gr_exprs,
                    ..
                }) => {
                    let step = *iter.get_child().borrow();
                    if step == 0 {
                        *iter.get_child().borrow_mut() += 1;
                        return children.get(step).copied();
                    }
                    let col_idx = step - 1;
                    if col_idx < gr_exprs.len() {
                        *iter.get_child().borrow_mut() += 1;
                        return gr_exprs.get(col_idx).copied();
                    }
                    if iter.need_output() && col_idx == gr_exprs.len() {
                        *iter.get_child().borrow_mut() += 1;
                        return Some(*output);
                    }
                    None
                }
                Relational::OrderBy(OrderBy {
                    children,
                    output,
                    order_by_elements,
                    ..
                }) => {
                    let step = *iter.get_child().borrow();
                    if step == 0 {
                        *iter.get_child().borrow_mut() += 1;
                        return children.get(step).copied();
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
                    if iter.need_output() && col_idx == order_by_elements.len() {
                        *iter.get_child().borrow_mut() += 1;
                        return Some(*output);
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
                        if iter.need_output() && step == len {
                            *iter.get_child().borrow_mut() += 1;
                            return Some(*output);
                        }
                    } else {
                        let step = *iter.get_child().borrow();
                        if iter.need_output() && step == 0 {
                            *iter.get_child().borrow_mut() += 1;
                            return Some(*output);
                        }
                    }
                    None
                }
                Relational::Values(Values {
                    output, children, ..
                })
                | Relational::SelectWithoutScan(SelectWithoutScan {
                    output, children, ..
                }) => {
                    let step = *iter.get_child().borrow();
                    *iter.get_child().borrow_mut() += 1;
                    if step == 0 {
                        return Some(*output);
                    }
                    if step <= children.len() {
                        return children.get(step - 1).copied();
                    }
                    None
                }
                Relational::Projection(Projection {
                    output, children, ..
                }) => {
                    let step = *iter.get_child().borrow();
                    *iter.get_child().borrow_mut() += 1;
                    if iter.output_first() {
                        if step == 0 {
                            return Some(*output);
                        }
                        if step <= children.len() {
                            return children.get(step - 1).copied();
                        }
                    } else {
                        if step < children.len() {
                            return children.get(step).copied();
                        }
                        if step == children.len() {
                            return Some(*output);
                        }
                    }
                    None
                }
                Relational::Update(Update { output, child, .. }) => {
                    let step = *iter.get_child().borrow();
                    *iter.get_child().borrow_mut() += 1;
                    if step == 0 {
                        return Some(*output);
                    }

                    if step <= 1 {
                        return Some(*child);
                    }

                    None
                }
                Relational::Selection(Selection {
                    children,
                    filter,
                    output,
                    ..
                })
                | Relational::Having(Having {
                    children,
                    filter,
                    output,
                }) => {
                    let step = *iter.get_child().borrow();

                    *iter.get_child().borrow_mut() += 1;
                    match step.cmp(&1) {
                        Ordering::Less => {
                            return children.get(step).copied();
                        }
                        Ordering::Equal => match snapshot {
                            Snapshot::Latest => Some(*filter),
                            Snapshot::Oldest => {
                                return Some(*iter.get_plan().undo.get_oldest(filter));
                            }
                        },
                        Ordering::Greater => {
                            if step == 2 && iter.need_output() {
                                return Some(*output);
                            }
                            None
                        }
                    }
                }
                Relational::ValuesRow(ValuesRow { data, output, .. }) => {
                    let step = *iter.get_child().borrow();

                    *iter.get_child().borrow_mut() += 1;
                    if step == 0 {
                        return Some(*data);
                    }
                    if iter.need_output() && step == 1 {
                        return Some(*output);
                    }
                    None
                }
                Relational::ScanRelation(ScanRelation { output, .. }) => {
                    if iter.need_output() {
                        let step = *iter.get_child().borrow();

                        *iter.get_child().borrow_mut() += 1;
                        if step == 0 {
                            return Some(*output);
                        }
                    }
                    None
                }
            },
        };
    }
    None
}
