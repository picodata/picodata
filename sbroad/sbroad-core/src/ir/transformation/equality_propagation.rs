//! Equality propagation deduces new equality expressions in join conditions
//! and selection filters.
//! For example:
//! ```text
//! (a = 1) and (b = 1) => (a = b)
//! ```
//!
//! It spawns new equalities that can be helpful for the final motion
//! transformation (compares left and right row distribution in the
//! joins and sub-queries, in a case of a distribution conflict inserts
//! a motion node).
//!
//! That is why equality propagation deducts only new equalities with rows
//! containing references (at the moment only rows with a single column of
//! the reference type are supported). Constants are replicated and do not
//! produce distribution conflicts.
//! ```text
//! select * from (select t1.a, t2.b from t1 join t2 on row(t1.c) = row(t2.d))
//! where row(a) = 1 and row(b) = 1
//! =>
//! select * from (select t1.a, t2.b from t1 join t2 on row(t1.c) = row(t2.d))
//! where row(a) = 1 and row(b) = 1 and row(a) = row(b)
//! =>
//! ..
//! =>
//! select * from (select t1.a, t2.b from t1 join t2 on row(t1.c) = row(t2.d)
//! and row(t1.a) = row(t2.b))
//! where row(a) = 1 and row(b) = 1
//! ```
//! We don't produce `(1) = (1)` equality as it don't give any new information,
//! but `row(a) = row(b)` can be pushed down to the join condition.
//!
//! Currently implementation produces new equalities only for constants, references
//! and rows that contain a single reference column.
//!
//! # Implementation
//! Let's look on an example:
//! ```sql
//! select * from t where
//! (a) = 1 and (c) = (e) and (b) = 1 and (d) = 1 and (e) = 4 and (f) = 1 and (a) = (f)
//! or (e) = 3
//! or (a) = 1 and (c) = (b) and (b) = 1
//! ```
//!
//! 1. We collect all the "AND"-ed chains in the plan tree with the
//!    infrastructure from the merge tuples transformation. As a result
//!    we only need to extend these "AND"-ed chains with the new equalities.
//!
//!    In a current example we'll get two "AND"-ed chains:
//!    - `(a) = 1 and (c) = (e) and (b) = 1 and (d) = 1 and (e) = 4 and (f) = 1 and (a) = (f)`
//!    - `(a) = 1 and (c) = (b) and (b) = 1`
//!      as `(e) = 3` doesn't contain "AND"s.
//!
//! 2. Each chain may produce multiple equality classes (where all the
//!    element are equal to each other). For example, the first chain
//!    ```text
//!    (a) = 1 and (c) = (e) and (b) = 1 and (d) = 1 and (e) = 4 and (f) = 1
//!    and (a) = (f)
//!    ```
//!    produces two eq classes: `{a, 1, b, d, f}` and `{c, e, 4}`.
//!
//! 3. When we populate equality classes with the new equalities, we always put
//!    NULLs and other constants that are not self equivalent into a separate
//!    class that should not be merged with other classes.
//!
//! 4. Some classes in the chain can be splitted (because of the order of equalities
//!    in the chain) and they should be merged later.
//!    `(a) = 1 and (c) = (b) and (b) = 1` produces two eq classes: `{a, 1, b}`
//!    and `{c, b, 1}`. They can be merged into a single eq class `{a, 1, b, c}`
//!
//!    We also track already existing references pairs in equalities (there is no need
//!    to generate already existing ones). So, in the current example pairs set contains
//!    `{c, b}`. We subtract these pairs from every equality class in the list as well
//!    as constants. So, as a result we get a single reference eq class
//!    `{a, 1, b, c} -> {a, b, c} -> {a, b, c} - {c, b} -> {a}`. We don't have enough
//!    elements to produce a new equivalence (need two items at least), so nothing to
//!    add in this example.
//!
//!    But an example from the step 1 produces eq class chain `[{a, 1, b, d, f}, {c, e, 4}]`
//!    with pairs `{c, e, a, f}`. After removing constants and subtracting pairs we get a
//!    new pair to produce:
//!    `[{a, b, d, f} - {c, e, a, f}, {c, e} - {c, e, a, f}] -> [{b, d}, {}] -> [{b, d}]`
//!
//! 5. After the new eq classes were produced, their elements should be added to the
//!    "AND"-ed chain. We don't want to produce all possible combinations, so we use
//!    a `tuple_windows()` function instead. For example, from the eq class of `{a, b, c}`
//!    it produces `(a) = (b) and (b) = (c)` combinations.
//!
//! 6. Finally, we transform the "AND"-ed chains into a plan subtree and attach them back
//!    to the plan tree.

use crate::errors::{Entity, SbroadError};
use crate::ir::helpers::RepeatableState;
use crate::ir::node::expression::Expression;
use crate::ir::node::{Constant, NodeId, Reference, ReferenceAsteriskSource, Row};
use crate::ir::operator::Bool;
use crate::ir::relation::Type;
use crate::ir::transformation::merge_tuples::Chain;
use crate::ir::transformation::OldNewTopIdPair;
use crate::ir::value::{Trivalent, Value};
use crate::ir::Plan;
use itertools::Itertools;
use smol_str::ToSmolStr;
use std::collections::{HashMap, HashSet};

/// A copy of the `Expression::Reference` with traits for the `HashSet`.
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
struct EqClassRef {
    targets: Option<Vec<usize>>,
    position: usize,
    parent: Option<NodeId>,
    col_type: Type,
    asterisk_source: Option<ReferenceAsteriskSource>,
}

impl EqClassRef {
    fn from_ref(expr: &Expression) -> Result<Self, SbroadError> {
        if let Expression::Reference(Reference {
            targets: expr_tgt,
            position: expr_pos,
            parent: expr_prt,
            col_type: expr_type,
            asterisk_source: expr_asterisk_source,
        }) = expr
        {
            return Ok(EqClassRef {
                targets: expr_tgt.clone(),
                position: *expr_pos,
                parent: *expr_prt,
                col_type: *expr_type,
                asterisk_source: expr_asterisk_source.clone(),
            });
        }
        Err(SbroadError::Invalid(Entity::Expression, None))
    }

    fn to_single_col_row(&self, plan: &mut Plan) -> NodeId {
        let id = plan.nodes.add_ref(
            self.parent,
            self.targets.clone(),
            self.position,
            self.col_type,
            self.asterisk_source.clone(),
        );
        plan.nodes.add_row(vec![id], None)
    }
}

#[derive(Clone, Hash, PartialEq, Debug)]
struct EqClassConst {
    value: Value,
}

/// Original `Expression::Constant` doesn't support Eq trait because
/// NaN doesn't satisfy equality reflexive property (NaN != NaN).
///
/// We always filter NaN in our equality class sets, so implement
/// Eq trait for `EqClassConst`.
impl Eq for EqClassConst {}

impl EqClassConst {
    fn from_const(expr: &Expression) -> Result<Self, SbroadError> {
        if let Expression::Constant(Constant { value: expr_value }) = expr {
            return Ok(EqClassConst {
                value: expr_value.clone(),
            });
        }
        Err(SbroadError::Invalid(
            Entity::Expression,
            Some("invaid Constant".into()),
        ))
    }

    fn to_const(&self, plan: &mut Plan) -> NodeId {
        let const_id = plan.add_const(self.value.clone());
        plan.nodes.add_row(vec![const_id], None)
    }
}

/// A enum for supported elements in equality class.
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
enum EqClassExpr {
    EqClassConst(EqClassConst),
    EqClassRef(EqClassRef),
}

impl EqClassExpr {
    fn to_plan(&self, plan: &mut Plan) -> NodeId {
        match self {
            EqClassExpr::EqClassConst(ec_const) => ec_const.to_const(plan),
            EqClassExpr::EqClassRef(ec_ref) => ec_ref.to_single_col_row(plan),
        }
    }

    fn is_self_equivalent(&self) -> bool {
        match self {
            EqClassExpr::EqClassConst(ec_const) => {
                ec_const.value.eq(&ec_const.value) == Trivalent::True
            }
            EqClassExpr::EqClassRef(_) => true,
        }
    }
}

/// A set of expressions that are equal to each other.
#[derive(Clone, PartialEq, Debug, Default)]
struct EqClass {
    set: HashSet<EqClassExpr, RepeatableState>,
}

impl EqClass {
    fn new() -> Self {
        EqClass {
            set: HashSet::with_hasher(RepeatableState),
        }
    }

    /// Build a copy of the current equality class containing only references.
    fn ref_copy(&self) -> Self {
        let mut result = EqClass::new();

        for item in &self.set {
            if let EqClassExpr::EqClassRef(expr) = item {
                result.set.insert(EqClassExpr::EqClassRef(expr.clone()));
            }
        }

        result
    }
}

/// A list of equivalence classes from a single "AND"-ed chain of expressions.
#[derive(Clone, PartialEq, Debug)]
struct EqClassChain {
    /// Groups of equivalence classes of the "AND"-e chain (all element are equal to each other).
    /// INVARIANT: this is REAL equivalence classes, meaning that they don't intersect.
    list: Vec<EqClass>,
    /// A set of equalities where both sides are references.
    pairs: HashSet<EqClassExpr, RepeatableState>,
}

impl EqClassChain {
    fn new() -> Self {
        EqClassChain {
            list: Vec::new(),
            pairs: HashSet::with_hasher(RepeatableState),
        }
    }

    /// Insert a new pair to the equality classes chain.
    fn insert(&mut self, left: &EqClassExpr, right: &EqClassExpr) {
        // a = null and b = null, such pairs can't be used to derive
        // equality relation: `a = null and a = c`.
        // TODO: `a = null` is always false, so is the whole and-chain,
        // in the future we should land an optimization
        // that simplifies boolean expressions and does NOT apply optimizations
        // like `equality propagation` to such and-chains.
        // https://git.picodata.io/picodata/picodata/sbroad/-/issues/855
        if !left.is_self_equivalent() || !right.is_self_equivalent() {
            return;
        }

        // Find indexes of classes in which these expressions appear,
        // as we maintain an invariant that classes do not intersect
        // there may be at most 2 of such classes.
        let mut classes = [0, 0];
        let mut found_classes = 0;
        for (pos, class) in self.list.iter().enumerate() {
            if class.set.contains(left) || class.set.contains(right) {
                classes[found_classes] = pos;
                found_classes += 1;

                if found_classes == 2 {
                    break;
                }
            }
        }

        match found_classes {
            0 => {
                // No matches, so add a new equality class.
                let mut class = EqClass::new();
                class.set.insert(left.clone());
                class.set.insert(right.clone());
                self.list.push(class);
            }
            1 => {
                // one expression is already bound to class
                // another one is not.
                self.list[classes[0]].set.insert(left.clone());
                self.list[classes[0]].set.insert(right.clone());
            }
            2 => {
                // two expressions appear in the different classes
                // merge two classes into one
                // TODO: this works in linear time, we could do
                // better using Disjoint-set-union. Doing merges
                // in O(1).
                let mut i = classes[0];
                let mut j = classes[1];
                if self.list[i].set.len() < self.list[j].set.len() {
                    (i, j) = (j, i);
                }
                // Merge the smaller set (j) into bigger set (i)
                let smaller = std::mem::take(self.list.get_mut(j).unwrap());
                self.list.get_mut(i).unwrap().set.extend(smaller.set);
                self.list.swap_remove(j);
            }
            _ => unreachable!("see break condition in loop above"),
        }

        // If both sides are references, add them to the pairs set.
        if let (EqClassExpr::EqClassRef(_), EqClassExpr::EqClassRef(_)) = (left, right) {
            self.pairs.insert(left.clone());
            self.pairs.insert(right.clone());
        }
    }

    fn subtract_pairs(&self) -> Self {
        let mut result = EqClassChain::new();
        result.pairs.clone_from(&self.pairs);

        for class in &self.list {
            let ec_ref = class.ref_copy();
            let mut buf = EqClass::new();

            for expr in ec_ref.set.difference(&self.pairs) {
                buf.set.insert(expr.clone());
            }

            result.list.push(buf);
        }

        result
    }
}

/// Replace IN operator with the chain of the OR-ed equalities in the expression tree.
fn call_expr_tree_derive_equalities(
    plan: &mut Plan,
    top_id: NodeId,
) -> Result<OldNewTopIdPair, SbroadError> {
    plan.expr_tree_modify_and_chains(top_id, &call_build_and_chains, &call_as_plan)
}

fn call_build_and_chains(
    plan: &mut Plan,
    nodes: &[NodeId],
) -> Result<HashMap<NodeId, Chain, RepeatableState>, SbroadError> {
    let mut chains = plan.populate_and_chains(nodes)?;
    for chain in chains.values_mut() {
        chain.extend_equality_operator(plan)?;
    }
    Ok(chains)
}

fn call_as_plan(chain: &Chain, plan: &mut Plan) -> Result<NodeId, SbroadError> {
    chain.as_plan_ecs(plan)
}

impl Chain {
    fn extend_equality_operator(&mut self, plan: &mut Plan) -> Result<(), SbroadError> {
        if let Some((left_vec, right_vec)) = self.get_grouped().get(&Bool::Eq) {
            let mut eq_classes = EqClassChain::new();

            for (left_id, right_id) in left_vec.iter().zip(right_vec.iter()) {
                let left_eqe = plan.try_to_eq_class_expr(*left_id);
                let right_eqe = plan.try_to_eq_class_expr(*right_id);
                if let (Err(SbroadError::DoSkip), _) | (_, Err(SbroadError::DoSkip)) =
                    (&left_eqe, &right_eqe)
                {
                    continue;
                }
                eq_classes.insert(&left_eqe?, &right_eqe?);
            }

            let ecs = eq_classes.subtract_pairs();

            for ec in &ecs.list {
                // Do not generate new equalities from a empty or single element lists.
                if ec.set.len() <= 1 {
                    continue;
                }

                for (a, b) in (ec.set).iter().tuple_windows() {
                    let left_id = a.to_plan(plan);
                    let right_id = b.to_plan(plan);
                    let eq_id = plan.add_cond(left_id, Bool::Eq, right_id)?;
                    self.insert(plan, eq_id)?;
                }
            }
        }

        Ok(())
    }

    fn as_plan_ecs(&self, plan: &mut Plan) -> Result<NodeId, SbroadError> {
        let other_top_id = match self.get_other().split_first() {
            Some((first, other)) => {
                let mut top_id = *first;
                for id in other {
                    top_id = plan.add_cond(top_id, Bool::And, *id)?;
                }
                Some(top_id)
            }
            None => None,
        };

        // Chain is grouped by the operators in the hash map.
        // To make serialization non-flaky, we extract operators
        // in a deterministic order.
        let mut grouped_top_id: Option<NodeId> = None;
        // No need for "And" and "Or" operators.
        let ordered_ops = &[
            Bool::Eq,
            Bool::Gt,
            Bool::GtEq,
            Bool::In,
            Bool::Lt,
            Bool::LtEq,
            Bool::NotEq,
        ];
        for op in ordered_ops {
            if let Some((left_vec, right_vec)) = self.get_grouped().get(op) {
                if let Some((first, other_pairs)) = left_vec
                    .iter()
                    .zip(right_vec.iter())
                    .map(|(l, r)| (*l, *r))
                    .collect::<Vec<(NodeId, NodeId)>>()
                    .split_first()
                {
                    let left_row_id = plan.nodes.add_row(vec![first.0], None);
                    let right_row_id = plan.nodes.add_row(vec![first.1], None);
                    let mut op_top_id = plan.add_cond(left_row_id, op.clone(), right_row_id)?;

                    for (l_id, r_id) in other_pairs {
                        let left_row_id = plan.nodes.add_row(vec![*l_id], None);
                        let right_row_id = plan.nodes.add_row(vec![*r_id], None);
                        let cond_id = plan.add_cond(left_row_id, op.clone(), right_row_id)?;
                        op_top_id = plan.add_cond(op_top_id, Bool::And, cond_id)?;
                    }
                    match grouped_top_id {
                        Some(id) => {
                            grouped_top_id = Some(plan.add_cond(id, Bool::And, op_top_id)?);
                        }
                        None => {
                            grouped_top_id = Some(op_top_id);
                        }
                    }
                }
            }
        }
        match (grouped_top_id, other_top_id) {
            (Some(grouped_top_id), Some(other_top_id)) => {
                Ok(plan.add_cond(grouped_top_id, Bool::And, other_top_id)?)
            }
            (Some(grouped_top_id), None) => Ok(grouped_top_id),
            (None, Some(other_top_id)) => Ok(other_top_id),
            (None, None) => Err(SbroadError::UnexpectedNumberOfValues(
                "no expressions to merge, expected one or two".to_smolstr(),
            )),
        }
    }
}

impl Plan {
    // DoSkip is a special case of an error - nothing bad had happened, the target node doesn't contain
    // anything interesting for us, skip it without any serious error.
    fn try_to_eq_class_expr(&self, expr_id: NodeId) -> Result<EqClassExpr, SbroadError> {
        let expr = self.get_expression_node(expr_id)?;
        match expr {
            Expression::Constant(_) => {
                Ok(EqClassExpr::EqClassConst(EqClassConst::from_const(&expr)?))
            }
            Expression::Reference(_) => Ok(EqClassExpr::EqClassRef(EqClassRef::from_ref(&expr)?)),
            Expression::Row(Row { list, .. }) => {
                if let (Some(col_id), None) = (list.first(), list.get(1)) {
                    self.try_to_eq_class_expr(*col_id)
                } else {
                    // We don't support more than a single column in a row.
                    Err(SbroadError::DoSkip)
                }
            }
            _ => Err(SbroadError::DoSkip),
        }
    }

    /// Derive new equalities in the expression tree.
    ///
    /// # Errors
    /// - If the plan tree is invalid (doesn't contain correct nodes where we expect it to).
    pub fn derive_equalities(&mut self) -> Result<(), SbroadError> {
        self.transform_expr_trees(&call_expr_tree_derive_equalities)
    }
}

#[cfg(test)]
mod tests;
