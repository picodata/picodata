//! Expression module.
//!
//! Expressions are the building blocks of the tuple.
//! They provide information about:
//! - what input tuple's columns where used to build our tuple
//! - the order of the columns (and we can get their types as well)
//! - distribution of the data in the tuple

use ahash::RandomState;
use distribution::Distribution;
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr};
use std::collections::{BTreeMap, HashSet};
use std::hash::{Hash, Hasher};
use std::ops::Bound::Included;

use super::node::{
    Bound, BoundType, GroupBy, Having, Join, Like, NamedWindows, OrderBy, Over, ReferenceTarget,
    Selection, Window,
};
use super::operator::OrderByEntity;
use super::types::DerivedType;
use super::{
    distribution, operator, Alias, ArithmeticExpr, BoolExpr, Case, Cast, Concat, Constant,
    Expression, LevelNode, MutExpression, MutNode, Node, NodeId, Reference, Row, ScalarFunction,
    Trim, UnaryExpr, Value,
};
use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::to_user;
use crate::ir::node::relational::Relational;
use crate::ir::node::{Parameter, ReferenceAsteriskSource};
use crate::ir::operator::Bool;
use crate::ir::tree::traversal::{PostOrderWithFilter, EXPR_CAPACITY};
use crate::ir::types::UnrestrictedType;
use crate::ir::{Nodes, Plan, Positions as Targets};

pub mod cast;
pub mod concat;
pub mod types;

pub(crate) type ExpressionId = NodeId;

#[derive(Clone, Debug, Hash, Deserialize, PartialEq, Eq, Serialize)]
pub enum FunctionFeature {
    /// Current function is an aggregate function and is marked as DISTINCT.
    Distinct,
    /// Current function is a substring function and has one of 5 substring variants.
    Substring(Substring),
}

#[derive(Clone, Debug, Hash, Deserialize, PartialEq, Eq, Serialize, Copy)]
pub enum VolatilityType {
    /// Stable function cannot modify the database and
    /// is guaranteed to return the same results given
    /// the same arguments for all rows within a single
    /// statement.
    Stable,
    /// Volatile function can produce different results
    /// on different instances in case of distributed plan.
    Volatile,
}

#[derive(Clone, Debug, Hash, Deserialize, PartialEq, Eq, Serialize)]
pub enum Substring {
    FromFor,
    Regular,
    For,
    From,
    Similar,
}
/// This is the kind of `trim` function that can be set
/// by using keywords LEADING, TRAILING or BOTH.
#[derive(Default, Clone, Debug, Hash, Deserialize, PartialEq, Eq, Serialize)]
pub enum TrimKind {
    #[default]
    Both,
    Leading,
    Trailing,
}

impl TrimKind {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            TrimKind::Leading => "leading",
            TrimKind::Trailing => "trailing",
            TrimKind::Both => "both",
        }
    }
}

impl Nodes {
    /// Adds alias node.
    ///
    /// # Errors
    /// - child node is invalid
    /// - name is empty
    pub fn add_alias(&mut self, name: &str, child: NodeId) -> Result<NodeId, SbroadError> {
        let alias = Alias {
            name: SmolStr::from(name),
            child,
        };
        Ok(self.push(alias.into()))
    }

    /// Adds boolean node.
    ///
    /// # Errors
    /// - when left or right nodes are invalid
    pub fn add_bool(
        &mut self,
        left: NodeId,
        op: operator::Bool,
        right: NodeId,
    ) -> Result<NodeId, SbroadError> {
        self.get(left).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("(left child of boolean node) from arena with index {left}"),
            )
        })?;
        self.get(right).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("(right child of boolean node) from arena with index {right}"),
            )
        })?;
        Ok(self.push(BoolExpr { left, op, right }.into()))
    }

    /// Adds arithmetic node.
    ///
    /// # Errors
    /// - when left or right nodes are invalid
    pub fn add_arithmetic_node(
        &mut self,
        left: NodeId,
        op: operator::Arithmetic,
        right: NodeId,
    ) -> Result<NodeId, SbroadError> {
        self.get(left).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("(left child of Arithmetic node) from arena with index {left:?}"),
            )
        })?;
        self.get(right).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("(right child of Arithmetic node) from arena with index {right:?}"),
            )
        })?;
        Ok(self.push(ArithmeticExpr { left, op, right }.into()))
    }

    /// Adds reference node.
    pub fn add_ref(
        &mut self,
        target: ReferenceTarget,
        position: usize,
        col_type: DerivedType,
        asterisk_source: Option<ReferenceAsteriskSource>,
    ) -> NodeId {
        let r = Reference {
            target,
            position,
            col_type,
            asterisk_source,
        };
        self.push(r.into())
    }

    /// Adds row node.
    pub fn add_row(&mut self, list: Vec<NodeId>, distribution: Option<Distribution>) -> NodeId {
        self.push(Row { list, distribution }.into())
    }

    /// Adds unary boolean node.
    ///
    /// # Errors
    /// - child node is invalid
    pub fn add_unary_bool(
        &mut self,
        op: operator::Unary,
        child: NodeId,
    ) -> Result<NodeId, SbroadError> {
        self.get(child).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("from arena with index {child}"),
            )
        })?;
        Ok(self.push(UnaryExpr { op, child }.into()))
    }
}

// todo(ars): think how to refactor, ideally we must not store
// plan for PlanExpression, try to put it into hasher? but what do
// with equality?
pub struct PlanExpr<'plan> {
    pub id: NodeId,
    pub plan: &'plan Plan,
}

impl<'plan> PlanExpr<'plan> {
    #[must_use]
    pub fn new(id: NodeId, plan: &'plan Plan) -> Self {
        PlanExpr { id, plan }
    }
}

impl Hash for PlanExpr<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut comp = Comparator::new(self.plan);
        comp.set_hasher(state);
        comp.hash_for_expr(self.id, EXPR_HASH_DEPTH);
    }
}

impl PartialEq for PlanExpr<'_> {
    fn eq(&self, other: &Self) -> bool {
        let comp = Comparator::new(self.plan);
        comp.are_subtrees_equal(self.id, other.id).unwrap_or(false)
    }
}

impl Eq for PlanExpr<'_> {}

/// Helper struct for comparing plan expression subtrees.
pub struct Comparator<'plan> {
    plan: &'plan Plan,
    state: Option<&'plan mut dyn Hasher>,
}

pub const EXPR_HASH_DEPTH: usize = 5;

impl<'plan> Comparator<'plan> {
    #[must_use]
    pub fn new(plan: &'plan Plan) -> Self {
        Comparator { plan, state: None }
    }

    pub fn set_hasher<H: Hasher>(&mut self, state: &'plan mut H) {
        self.state = Some(state);
    }

    /// Checks whether subtrees `lhs` and `rhs` are equal.
    /// This function traverses both trees comparing their nodes.
    ///
    /// # References Equality
    /// References are considered equal if their `targets` and `position`
    /// fields are equal.
    /// This function is used to find common expressions
    /// between `GroupBy` and nodes in Reduce stage of
    /// 2-stage aggregation (`Projection`, `Having`, `OrderBy`). It's assumed
    /// that those nodes have the same output so that it's safe to compare only
    /// those two fields.
    ///
    /// # Different tables
    /// It would be wrong to use this function for comparing expressions that
    /// come from different tables:
    /// ```text
    /// select a + b from t1
    /// where c in (select a + b from t2)
    /// ```
    /// Here this function would say that expressions `a+b` in projection and
    /// selection are the same, which is wrong.
    ///
    /// # Errors
    /// - invalid [`Expression::Reference`]s in either of subtrees
    /// - invalid children in some expression
    ///
    /// # Panics
    /// - never
    #[allow(clippy::too_many_lines)]
    pub fn are_subtrees_equal(&self, lhs: NodeId, rhs: NodeId) -> Result<bool, SbroadError> {
        let l = self.plan.get_node(lhs)?;
        let r = self.plan.get_node(rhs)?;
        let cmp_expr_vec = |l: &[NodeId], r: &[NodeId]| -> Result<bool, SbroadError> {
            if l.len() != r.len() {
                return Ok(false);
            }
            for (l, r) in l.iter().zip(r.iter()) {
                if !self.are_subtrees_equal(*l, *r)? {
                    return Ok(false);
                }
            }
            Ok(true)
        };
        if let Node::Expression(left) = l {
            if let Node::Expression(right) = r {
                match left {
                    Expression::Alias(_) | Expression::Timestamp(_) => {}
                    Expression::Parameter(Parameter {
                        param_type: _,
                        index: l_index,
                    }) => {
                        if let Expression::Parameter(Parameter {
                            param_type: _,
                            index: r_index,
                        }) = right
                        {
                            return Ok(l_index == r_index);
                        }
                    }
                    Expression::Window(Window {
                        name: l_name,
                        partition: l_partition,
                        ordering: l_ordering,
                        frame: l_frame,
                    }) => {
                        if let Expression::Window(Window {
                            name: r_name,
                            partition: r_partition,
                            ordering: r_ordering,
                            frame: r_frame,
                        }) = right
                        {
                            let mut parts_equal = true;
                            match (l_partition, r_partition) {
                                (Some(l_partition), Some(r_partition)) => {
                                    parts_equal = cmp_expr_vec(l_partition, r_partition)?;
                                }
                                (None, None) => {}
                                _ => return Ok(false),
                            }
                            let mut ordering_equal = true;
                            match (l_ordering, r_ordering) {
                                (Some(l_ordering), Some(r_ordering)) => {
                                    if l_ordering.len() != r_ordering.len() {
                                        return Ok(false);
                                    }
                                    for (l_elem, r_elem) in l_ordering.iter().zip(r_ordering.iter())
                                    {
                                        if l_elem.order_type != r_elem.order_type {
                                            return Ok(false);
                                        }
                                        match (&l_elem.entity, &r_elem.entity) {
                                            (
                                                &OrderByEntity::Expression { expr_id: l_expr },
                                                &OrderByEntity::Expression { expr_id: r_expr },
                                            ) => {
                                                if !self.are_subtrees_equal(l_expr, r_expr)? {
                                                    return Ok(false);
                                                }
                                            }
                                            _ => ordering_equal &= l_elem.entity == r_elem.entity,
                                        }
                                    }
                                }
                                (None, None) => {}
                                _ => return Ok(false),
                            }
                            let mut frame_equal = true;
                            match (l_frame, r_frame) {
                                (Some(l_frame), Some(r_frame)) => {
                                    if l_frame.ty != r_frame.ty {
                                        return Ok(false);
                                    }
                                    let mut bound_types = [None, None];
                                    match (&l_frame.bound, &r_frame.bound) {
                                        (Bound::Single(l_bound), Bound::Single(r_bound)) => {
                                            bound_types[0] = Some((l_bound, r_bound));
                                        }
                                        (
                                            Bound::Between(l_lower, l_upper),
                                            Bound::Between(r_lower, r_upper),
                                        ) => {
                                            bound_types[0] = Some((l_lower, r_lower));
                                            bound_types[1] = Some((l_upper, r_upper));
                                        }
                                        _ => return Ok(false),
                                    }
                                    for b_type in bound_types {
                                        let Some((l_bound, r_bound)) = b_type else {
                                            continue;
                                        };
                                        match (l_bound, r_bound) {
                                            (
                                                BoundType::PrecedingOffset(l_offset),
                                                BoundType::PrecedingOffset(r_offset),
                                            ) => {
                                                frame_equal &=
                                                    self.are_subtrees_equal(*l_offset, *r_offset)?;
                                            }
                                            (
                                                BoundType::FollowingOffset(l_offset),
                                                BoundType::FollowingOffset(r_offset),
                                            ) => {
                                                frame_equal &=
                                                    self.are_subtrees_equal(*l_offset, *r_offset)?;
                                            }
                                            _ => frame_equal &= l_bound == r_bound,
                                        }
                                    }
                                }
                                (None, None) => {}
                                _ => return Ok(false),
                            }
                            return Ok(l_name == r_name
                                && parts_equal
                                && ordering_equal
                                && frame_equal);
                        }
                    }
                    Expression::Over(Over {
                        stable_func: l_stable_func,
                        filter: l_filter,
                        window: l_window,
                        ..
                    }) => {
                        if let Expression::Over(Over {
                            stable_func: r_stable_func,
                            filter: r_filter,
                            window: r_window,
                            ..
                        }) = right
                        {
                            let mut filter_equal = true;
                            match (l_filter, r_filter) {
                                (Some(l_filter), Some(r_filter)) => {
                                    filter_equal = self.are_subtrees_equal(*l_filter, *r_filter)?;
                                }
                                (None, None) => {}
                                _ => return Ok(false),
                            }
                            return Ok(l_stable_func == r_stable_func
                                && filter_equal
                                && l_window == r_window);
                        }
                    }
                    Expression::CountAsterisk(_) => {
                        return Ok(matches!(right, Expression::CountAsterisk(_)))
                    }
                    Expression::Bool(BoolExpr {
                        left: left_left,
                        op: op_left,
                        right: right_left,
                    }) => {
                        if let Expression::Bool(BoolExpr {
                            left: left_right,
                            op: op_right,
                            right: right_right,
                        }) = right
                        {
                            return Ok(*op_left == *op_right
                                && self.are_subtrees_equal(*left_left, *left_right)?
                                && self.are_subtrees_equal(*right_left, *right_right)?);
                        }
                    }
                    Expression::Case(Case {
                        search_expr: search_expr_left,
                        when_blocks: when_blocks_left,
                        else_expr: else_expr_left,
                    }) => {
                        if let Expression::Case(Case {
                            search_expr: search_expr_right,
                            when_blocks: when_blocks_right,
                            else_expr: else_expr_right,
                        }) = right
                        {
                            let mut search_expr_equal =
                                search_expr_left.is_none() && search_expr_right.is_none();
                            if let (Some(search_expr_left), Some(search_expr_right)) =
                                (search_expr_left, search_expr_right)
                            {
                                search_expr_equal =
                                    self.are_subtrees_equal(*search_expr_left, *search_expr_right)?;
                            }

                            let when_blocks_equal = when_blocks_left
                                .iter()
                                .zip(when_blocks_right.iter())
                                .all(|((cond_l, res_l), (cond_r, res_r))| {
                                    self.are_subtrees_equal(*cond_l, *cond_r).unwrap_or(false)
                                        && self.are_subtrees_equal(*res_l, *res_r).unwrap_or(false)
                                });

                            let mut else_expr_equal =
                                else_expr_left.is_none() && else_expr_right.is_none();
                            if let (Some(else_expr_left), Some(else_expr_right)) =
                                (else_expr_left, else_expr_right)
                            {
                                else_expr_equal =
                                    self.are_subtrees_equal(*else_expr_left, *else_expr_right)?;
                            }
                            return Ok(search_expr_equal && when_blocks_equal && else_expr_equal);
                        }
                    }
                    Expression::Arithmetic(ArithmeticExpr {
                        op: op_left,
                        left: l_left,
                        right: r_left,
                    }) => {
                        if let Expression::Arithmetic(ArithmeticExpr {
                            op: op_right,
                            left: l_right,
                            right: r_right,
                        }) = right
                        {
                            return Ok(*op_left == *op_right
                                && self.are_subtrees_equal(*l_left, *l_right)?
                                && self.are_subtrees_equal(*r_left, *r_right)?);
                        }
                    }
                    Expression::Cast(Cast {
                        child: child_left,
                        to: to_left,
                    }) => {
                        if let Expression::Cast(Cast {
                            child: child_right,
                            to: to_right,
                        }) = right
                        {
                            return Ok(*to_left == *to_right
                                && self.are_subtrees_equal(*child_left, *child_right)?);
                        }
                    }
                    Expression::Like(Like {
                        left: left_left,
                        right: right_left,
                        escape: escape_left,
                    }) => {
                        if let Expression::Like(Like {
                            left: left_right,
                            right: right_right,
                            escape: escape_right,
                        }) = right
                        {
                            return Ok(self.are_subtrees_equal(*escape_left, *escape_right)?
                                && self.are_subtrees_equal(*left_left, *left_right)?
                                && self.are_subtrees_equal(*right_left, *right_right)?);
                        }
                    }
                    Expression::Concat(Concat {
                        left: left_left,
                        right: right_left,
                    }) => {
                        if let Expression::Concat(Concat {
                            left: left_right,
                            right: right_right,
                        }) = right
                        {
                            return Ok(self.are_subtrees_equal(*left_left, *left_right)?
                                && self.are_subtrees_equal(*right_left, *right_right)?);
                        }
                    }
                    Expression::Trim(Trim {
                        kind: kind_left,
                        pattern: pattern_left,
                        target: target_left,
                    }) => {
                        if let Expression::Trim(Trim {
                            kind: kind_right,
                            pattern: pattern_right,
                            target: target_right,
                        }) = right
                        {
                            match (pattern_left, pattern_right) {
                                (Some(p_left), Some(p_right)) => {
                                    return Ok(*kind_left == *kind_right
                                        && self.are_subtrees_equal(*p_left, *p_right)?
                                        && self
                                            .are_subtrees_equal(*target_left, *target_right)?);
                                }
                                (None, None) => {
                                    return Ok(*kind_left == *kind_right
                                        && self
                                            .are_subtrees_equal(*target_left, *target_right)?);
                                }
                                _ => return Ok(false),
                            }
                        }
                    }
                    Expression::Constant(Constant { value: value_left }) => {
                        if let Expression::Constant(Constant { value: value_right }) = right {
                            return Ok(*value_left == *value_right);
                        }
                    }
                    Expression::Reference(Reference {
                        position: p_left, ..
                    }) => {
                        if let Expression::Reference(Reference {
                            position: p_right, ..
                        }) = right
                        {
                            // TODO: It's assumed that comparison is taking place for relational
                            //       operators which have the same output order (just cloned). E.g.
                            //       this function shouldn't work in case references are compared from
                            //       the output of two subqueries reading from different
                            //       tables (but it will return true).

                            // Note: Logic below won't work for query like
                            //       `select distinct 1 (select 2), a + (select 1) from t group by a + (select 1)`
                            //       because of the following reasons:
                            //       1. For Projection and GroupBy scalar subquery `(select 1)` will have different targets
                            //       2. They are referencing different relations.

                            let base_equal = p_left == p_right;
                            // When we call `add_update` we have to compare references
                            // without parent relation being added yet. That's why we don't unwrap
                            // results here.
                            let rel_left = self.plan.get_relational_from_reference_node(lhs);
                            let rel_right = self.plan.get_relational_from_reference_node(rhs);
                            let (Ok(rel_left), Ok(rel_right)) = (rel_left, rel_right) else {
                                return Ok(base_equal);
                            };
                            let res = if self.plan.is_additional_child(rel_left)?
                                && self.plan.is_additional_child(rel_right)?
                            {
                                // E.g. additional subqueries may be equal in case we've copied one under DISTINCT
                                // qualifier of Projection into local GroupBy (`select distinct (select 1) from t`).
                                base_equal && rel_left == rel_right
                            } else {
                                base_equal
                            };

                            return Ok(res);
                        }
                    }
                    Expression::Row(Row {
                        list: list_left, ..
                    }) => {
                        if let Expression::Row(Row {
                            list: list_right, ..
                        }) = right
                        {
                            return Ok(list_left
                                .iter()
                                .zip(list_right.iter())
                                .all(|(l, r)| self.are_subtrees_equal(*l, *r).unwrap_or(false)));
                        }
                    }
                    Expression::ScalarFunction(ScalarFunction {
                        name: name_left,
                        children: children_left,
                        feature: feature_left,
                        func_type: func_type_left,
                        is_system: is_aggr_left,
                        volatility_type: volatility_type_left,
                        is_window: is_window_left,
                    }) => {
                        if let Expression::ScalarFunction(ScalarFunction {
                            name: name_right,
                            children: children_right,
                            feature: feature_right,
                            func_type: func_type_right,
                            is_system: is_aggr_right,
                            volatility_type: volatility_type_right,
                            is_window: is_window_right,
                        }) = right
                        {
                            return Ok(name_left == name_right
                                && feature_left == feature_right
                                && func_type_left == func_type_right
                                && is_aggr_left == is_aggr_right
                                && volatility_type_left == volatility_type_right
                                && is_window_left == is_window_right
                                && children_left.iter().zip(children_right.iter()).all(
                                    |(l, r)| self.are_subtrees_equal(*l, *r).unwrap_or(false),
                                ));
                        }
                    }
                    Expression::Unary(UnaryExpr {
                        op: op_left,
                        child: child_left,
                    }) => {
                        if let Expression::Unary(UnaryExpr {
                            op: op_right,
                            child: child_right,
                        }) = right
                        {
                            return Ok(*op_left == *op_right
                                && self.are_subtrees_equal(*child_left, *child_right)?);
                        }
                    }
                }
            }
        }
        Ok(false)
    }

    pub fn hash_for_child_expr(&mut self, child: NodeId, depth: usize) {
        self.hash_for_expr(child, depth - 1);
    }

    /// TODO: See strange [behaviour](https://users.rust-lang.org/t/unintuitive-behaviour-with-passing-a-reference-to-trait-object-to-function/35937)
    ///       about `&mut dyn Hasher` and why we use `ref mut state`.
    ///
    /// # Panics
    /// - Comparator hasher wasn't set.
    #[allow(clippy::too_many_lines)]
    pub fn hash_for_expr(&mut self, top: NodeId, depth: usize) {
        if depth == 0 {
            return;
        }
        let Ok(node) = self.plan.get_expression_node(top) else {
            return;
        };
        let Some(ref mut state) = self.state else {
            panic!("Hasher should have been set previously");
        };
        match node {
            Expression::Window(Window {
                name,
                partition,
                ordering,
                frame,
            }) => {
                name.hash(state);
                if let Some(ordering) = ordering {
                    for elem in ordering {
                        elem.order_type.hash(state);
                    }
                }
                if let Some(ordering) = ordering {
                    for elem in ordering {
                        if let OrderByEntity::Index { value } = &elem.entity {
                            value.hash(state);
                        }
                    }
                }
                if let Some(frame) = frame {
                    frame.ty.hash(state);
                    frame.bound.index().hash(state);
                    match &frame.bound {
                        Bound::Single(bound) => {
                            bound.index().hash(state);
                        }
                        Bound::Between(lower, upper) => {
                            lower.index().hash(state);
                            upper.index().hash(state);
                        }
                    }
                }

                if let Some(ordering) = ordering {
                    for elem in ordering {
                        if let OrderByEntity::Expression { expr_id } = &elem.entity {
                            self.hash_for_child_expr(*expr_id, depth);
                        }
                    }
                }
                if let Some(partition) = partition {
                    for child in partition {
                        self.hash_for_child_expr(*child, depth);
                    }
                }
                if let Some(frame) = frame {
                    let mut bound_types = [None, None];
                    match &frame.bound {
                        Bound::Single(bound) => {
                            bound_types[0] = Some(bound);
                        }
                        Bound::Between(lower, upper) => {
                            bound_types[0] = Some(lower);
                            bound_types[1] = Some(upper);
                        }
                    }
                    bound_types.into_iter().for_each(|b_type| {
                        if let Some(b_type) = b_type {
                            match b_type {
                                BoundType::PrecedingOffset(offset) => {
                                    self.hash_for_child_expr(*offset, depth);
                                }
                                BoundType::FollowingOffset(offset) => {
                                    self.hash_for_child_expr(*offset, depth);
                                }
                                _ => {}
                            }
                        }
                    });
                }
            }
            Expression::Over(Over {
                stable_func,
                filter,
                window,
                ref_by_name,
            }) => {
                let Expression::ScalarFunction(ScalarFunction { name, children, .. }) =
                    self.plan.get_expression_node(*stable_func).unwrap()
                else {
                    panic!("Over should have stable func");
                };
                name.to_string().hash(state);
                ref_by_name.hash(state);
                for arg in children {
                    self.hash_for_child_expr(*arg, depth);
                }
                if let Some(filter) = filter {
                    self.hash_for_child_expr(*filter, depth);
                }
                self.hash_for_child_expr(*window, depth);
            }
            Expression::Alias(Alias { child, name }) => {
                name.hash(state);
                self.hash_for_child_expr(*child, depth);
            }
            Expression::Case(Case {
                search_expr,
                when_blocks,
                else_expr,
            }) => {
                if let Some(search_expr) = search_expr {
                    self.hash_for_child_expr(*search_expr, depth);
                }
                for (cond_expr, res_expr) in when_blocks {
                    self.hash_for_child_expr(*cond_expr, depth);
                    self.hash_for_child_expr(*res_expr, depth);
                }
                if let Some(else_expr) = else_expr {
                    self.hash_for_child_expr(*else_expr, depth);
                }
            }
            Expression::Bool(BoolExpr { op, left, right }) => {
                op.hash(state);
                self.hash_for_child_expr(*left, depth);
                self.hash_for_child_expr(*right, depth);
            }
            Expression::Arithmetic(ArithmeticExpr { op, left, right }) => {
                op.hash(state);
                self.hash_for_child_expr(*left, depth);
                self.hash_for_child_expr(*right, depth);
            }
            Expression::Cast(Cast { child, to }) => {
                to.hash(state);
                self.hash_for_child_expr(*child, depth);
            }
            Expression::Concat(Concat { left, right }) => {
                self.hash_for_child_expr(*left, depth);
                self.hash_for_child_expr(*right, depth);
            }
            Expression::Like(Like {
                left,
                right,
                escape: escape_id,
            }) => {
                self.hash_for_child_expr(*left, depth);
                self.hash_for_child_expr(*right, depth);
                self.hash_for_child_expr(*escape_id, depth);
            }
            Expression::Trim(Trim {
                kind,
                pattern,
                target,
            }) => {
                kind.hash(state);
                if let Some(pattern) = pattern {
                    self.hash_for_child_expr(*pattern, depth);
                }
                self.hash_for_child_expr(*target, depth);
            }
            Expression::Constant(Constant { value }) => {
                value.hash(state);
            }
            Expression::Reference(Reference {
                position,
                target: _,
                col_type,
                asterisk_source: is_asterisk,
            }) => {
                position.hash(state);
                col_type.hash(state);
                is_asterisk.hash(state);
            }
            Expression::Row(Row { list, .. }) => {
                for child in list {
                    self.hash_for_child_expr(*child, depth);
                }
            }
            Expression::ScalarFunction(ScalarFunction {
                name,
                children,
                func_type,
                feature,
                is_system: is_aggr,
                ..
            }) => {
                feature.hash(state);
                func_type.hash(state);
                name.hash(state);
                is_aggr.hash(state);
                for child in children {
                    self.hash_for_child_expr(*child, depth);
                }
            }
            Expression::Unary(UnaryExpr { child, op }) => {
                op.hash(state);
                self.hash_for_child_expr(*child, depth);
            }
            Expression::CountAsterisk(_) => {
                "CountAsterisk".hash(state);
            }
            Expression::Timestamp(_) => {
                "Timestamp".hash(state);
            }
            Expression::Parameter(_) => {
                "Parameter".hash(state);
            }
        }
    }
}

pub(crate) type Position = usize;

/// Identifier of how many times column (with specific name) was met in relational output.
#[derive(Debug, PartialEq)]
pub(crate) enum Positions {
    /// Init state.
    Empty,
    /// Column with such name was met in the output only once on a given `Position`.
    Single(Position),
    /// Several columns were met with the same name in the output.
    Multiple,
}

impl Positions {
    pub(crate) fn new() -> Self {
        Positions::Empty
    }

    pub(crate) fn push(&mut self, pos: Position) {
        if Positions::Empty == *self {
            *self = Positions::Single(pos);
        } else {
            *self = Positions::Multiple;
        }
    }
}

/// Pair of (Column name, Option(Scan name)).
pub(crate) type ColumnScanName = (SmolStr, Option<SmolStr>);

/// Map of { column name (with optional scan name) -> on which positions of relational node it's met }.
/// Built for concrete relational node. Every column from its (relational node) output is
/// presented as a key in `map`.
#[derive(Debug)]
pub(crate) struct ColumnPositionMap {
    /// Binary tree map.
    map: BTreeMap<ColumnScanName, Positions>,
    /// Max Scan name (in alphabetical order) that some of the columns in output can reference to.
    /// E.g. we have Join node that references to Scan nodes "aa" and "ab". The `max_scan_name` will
    /// be "ab".
    ///
    /// Used for querying binary tree `map` by ranges (see `ColumnPositionMap` `get` method below).
    max_scan_name: Option<SmolStr>,
}

impl ColumnPositionMap {
    pub(crate) fn new(plan: &Plan, rel_id: NodeId) -> Result<Self, SbroadError> {
        let rel_node = plan.get_relation_node(rel_id)?;
        let output = plan.get_expression_node(rel_node.output())?;
        let alias_ids = output.get_row_list()?;

        let mut map = BTreeMap::new();
        let mut max_name = None;
        for (pos, alias_id) in alias_ids.iter().enumerate() {
            let alias = plan.get_expression_node(*alias_id)?;
            let alias_name = SmolStr::from(alias.get_alias_name()?);
            let scan_name = plan.scan_name(rel_id, pos)?.map(SmolStr::from);
            // For query `select "a", "b" as "a" from (select "a", "b" from t)`
            // column entry "a" will have `Position::Multiple` so that if parent operator will
            // reference "a" we won't be able to identify which of these two columns
            // will it reference.
            map.entry((alias_name, scan_name.clone()))
                .or_insert_with(Positions::new)
                .push(pos);
            if max_name < scan_name {
                max_name = scan_name;
            }
        }
        Ok(Self {
            map,
            max_scan_name: max_name,
        })
    }

    /// Get position of relational node output that corresponds to given `column`.
    /// Note that we don't specify a Scan name here (see `get_with_scan` below for that logic).
    pub(crate) fn get(&self, column: &str) -> Result<Position, SbroadError> {
        let from_key = (SmolStr::from(column), None);
        let to_key = (SmolStr::from(column), self.max_scan_name.clone());
        let mut iter = self.map.range((Included(from_key), Included(to_key)));
        match (iter.next(), iter.next()) {
            // Map contains several values for the same `column`.
            // e.g. in the query
            // `select "t2"."a", "t1"."a" from (select "a" from "t1") join (select "a" from "t2")
            // for the column "a" there will be two results: {
            // * Some(("a", "t2"), _),
            // * Some(("a", "t1"), _)
            // }
            //
            // So that given just a column name we can't say what column to refer to.
            (Some(..), Some(..)) => Err(SbroadError::DuplicatedValue(format_smolstr!(
                "column name {column} is ambiguous"
            ))),
            // Map contains single value for the given `column`.
            (Some((_, position)), None) => {
                if let Positions::Single(pos) = position {
                    return Ok(*pos);
                }
                // In case we have query like
                // `select "a", "a" from (select "a" from t)`
                // where single column is met on several positions.
                Err(SbroadError::DuplicatedValue(format_smolstr!(
                    "column name {column} is ambiguous"
                )))
            }
            _ => Err(SbroadError::NotFound(
                Entity::Column,
                format_smolstr!("with name {}", to_user(column)),
            )),
        }
    }

    /// Get position of relational node output that corresponds to given `scan.column`.
    pub(crate) fn get_with_scan(
        &self,
        column: &str,
        scan: Option<&str>,
    ) -> Result<Position, SbroadError> {
        let key = &(SmolStr::from(column), scan.map(SmolStr::from));
        if let Some(position) = self.map.get(key) {
            if let Positions::Single(pos) = position {
                return Ok(*pos);
            }
            // In case we have query like
            // `select "a", "a" from (select "a" from t)`
            // where single column is met on several positions.
            //
            // Even given `scan` we can't identify which of these two columns do we need to
            // refer to.
            return Err(SbroadError::DuplicatedValue(format_smolstr!(
                "column name {} is ambiguous",
                to_user(column)
            )));
        }
        Err(SbroadError::NotFound(
            Entity::Column,
            format_smolstr!("with name {} and scan {scan:?}", to_user(column)),
        ))
    }

    /// Get positions of all columns in relational node output
    /// that corresponds to given `target_scan_name`.
    pub(crate) fn get_by_scan_name(
        &self,
        target_scan_name: &str,
    ) -> Result<Vec<Position>, SbroadError> {
        let mut res = Vec::new();
        for (_, positions) in self.map.iter().filter(|((_, scan_name), _)| {
            if let Some(scan_name) = scan_name {
                scan_name == target_scan_name
            } else {
                false
            }
        }) {
            if let Positions::Single(pos) = positions {
                res.push(*pos);
            } else {
                return Err(SbroadError::DuplicatedValue(format_smolstr!(
                    "column name for {target_scan_name} scan name is ambiguous"
                )));
            }
        }

        if res.is_empty() {
            Err(SbroadError::NotFound(
                Entity::Table,
                format_smolstr!("'{target_scan_name}'"),
            ))
        } else {
            // Note: sorting of usizes doesn't take much time.
            res.sort_unstable();
            Ok(res)
        }
    }
}

#[derive(Clone, Debug)]
pub struct ColumnWithScan<'column> {
    pub column: &'column str,
    pub scan: Option<&'column str>,
}

impl<'column> ColumnWithScan<'column> {
    #[must_use]
    pub fn new(column: &'column str, scan: Option<&'column str>) -> Self {
        ColumnWithScan { column, scan }
    }
}

/// Specification of column names/indices that we want to retrieve in `new_columns` call.
#[derive(Clone, Debug)]
pub enum ColumnsRetrievalSpec<'spec> {
    Names(Vec<ColumnWithScan<'spec>>),
    Indices(Vec<usize>),
}

/// Specification of targets to retrieve from join within `new_columns` call.
#[derive(Debug)]
pub enum JoinTargets<'targets> {
    Left {
        columns_spec: Option<ColumnsRetrievalSpec<'targets>>,
    },
    Right {
        columns_spec: Option<ColumnsRetrievalSpec<'targets>>,
    },
    Both,
}

/// Indicator of relational nodes source for `new_columns` call.
///
/// If `columns_spec` is met, it means we'd like to retrieve only specific columns.
/// Otherwise, we retrieve all the columns from children.
#[derive(Debug)]
pub enum NewColumnsSource<'targets> {
    Join {
        outer_child: NodeId,
        inner_child: NodeId,
        targets: JoinTargets<'targets>,
    },
    /// Enum variant used both for Except and UnionAll operators.
    ExceptUnion {
        left_child: NodeId,
        right_child: NodeId,
    },
    /// Other relational nodes.
    Other {
        child: NodeId,
        columns_spec: Option<ColumnsRetrievalSpec<'targets>>,
        /// Indicates whether requested output is coming from asterisk.
        asterisk_source: Option<ReferenceAsteriskSource>,
    },
}

/// Iterator needed for unified way of source nodes traversal during `new_columns` call.
pub struct NewColumnSourceIterator<'iter> {
    source: &'iter NewColumnsSource<'iter>,
    index: usize,
}

impl Iterator for NewColumnSourceIterator<'_> {
    // Pair of (relational node id, target id)
    type Item = (NodeId, usize);

    fn next(&mut self) -> Option<(NodeId, usize)> {
        let result = match &self.source {
            NewColumnsSource::Join {
                outer_child,
                inner_child,
                targets,
            } => match targets {
                JoinTargets::Left { .. } => match self.index {
                    0 => outer_child,
                    _ => return None,
                },
                JoinTargets::Right { .. } => match self.index {
                    0 => inner_child,
                    _ => return None,
                },
                JoinTargets::Both => match self.index {
                    0 => outer_child,
                    1 => inner_child,
                    _ => return None,
                },
            },
            NewColumnsSource::ExceptUnion { left_child, .. } => match self.index {
                // For the `UnionAll` and `Except` operators we need only the first
                // child to get correct column names for a new tuple
                // (the second child aliases would be shadowed). But each reference should point
                // to both children to give us additional information
                // during transformations.
                0 => left_child,
                _ => return None,
            },
            NewColumnsSource::Other { child, .. } => match self.index {
                0 => child,
                _ => return None,
            },
        };
        let res = Some((*result, self.index));
        self.index += 1;
        res
    }
}

impl<'iter, 'source: 'iter> IntoIterator for &'source NewColumnsSource<'iter> {
    type Item = (NodeId, usize);
    type IntoIter = NewColumnSourceIterator<'iter>;

    fn into_iter(self) -> Self::IntoIter {
        NewColumnSourceIterator {
            source: self,
            index: 0,
        }
    }
}

impl<'source> NewColumnsSource<'source> {
    fn get_columns_spec(&self) -> Option<ColumnsRetrievalSpec> {
        match self {
            NewColumnsSource::Join { targets, .. } => match targets {
                JoinTargets::Left { columns_spec } | JoinTargets::Right { columns_spec } => {
                    columns_spec.clone()
                }
                JoinTargets::Both => None,
            },
            NewColumnsSource::ExceptUnion { .. } => None,
            NewColumnsSource::Other { columns_spec, .. } => columns_spec.clone(),
        }
    }

    fn get_asterisk_source(&self) -> Option<ReferenceAsteriskSource> {
        match self {
            NewColumnsSource::Other {
                asterisk_source, ..
            } => asterisk_source.clone(),
            _ => None,
        }
    }

    fn iter(&'source self) -> NewColumnSourceIterator<'source> {
        <&Self as IntoIterator>::into_iter(self)
    }
}

impl Plan {
    /// Returns a list of columns from the children relational nodes outputs.
    ///
    /// `need_aliases` indicates whether we'd like to copy aliases (their names) from the child
    ///  node or whether we'd like to build raw References list.
    ///
    /// # Errors
    /// Returns `SbroadError`:
    /// - relation node contains invalid `Row` in the output
    /// - column names don't exist
    ///
    /// # Panics
    /// - Plan is in inconsistent state.
    #[allow(clippy::too_many_lines)]
    pub fn new_columns(
        &mut self,
        source: &NewColumnsSource,
        need_aliases: bool,
        need_sharding_column: bool,
    ) -> Result<Vec<NodeId>, SbroadError> {
        // Vec of (column position in child output, column plan id, new_targets).
        let mut filtered_children_row_list: Vec<(usize, NodeId, ReferenceTarget)> = Vec::new();

        // Helper lambda to retrieve column positions we need to exclude from child `rel_id`.
        let column_positions_to_exclude = |rel_id| -> Result<Targets, SbroadError> {
            let positions = if need_sharding_column {
                [None, None]
            } else {
                let mut context = self.context_mut();
                context
                    .get_shard_columns_positions(rel_id, self)?
                    .copied()
                    .unwrap_or_default()
            };
            Ok(positions)
        };

        if let Some(columns_spec) = source.get_columns_spec() {
            let (rel_child, _) = source
                .iter()
                .next()
                .expect("Source must have a single target");

            let relational_op = self.get_relation_node(rel_child)?;
            let output_id = relational_op.output();
            let child_node_row_list = self.get_row_list(output_id)?.clone();

            let mut indices: Vec<usize> = Vec::new();
            match columns_spec {
                ColumnsRetrievalSpec::Names(names) => {
                    let col_name_pos_map = ColumnPositionMap::new(self, rel_child)?;
                    indices.reserve(names.len());
                    for ColumnWithScan { column, scan } in names {
                        let index = if scan.is_some() {
                            col_name_pos_map.get_with_scan(column, scan)?
                        } else {
                            col_name_pos_map.get(column)?
                        };
                        indices.push(index);
                    }
                }
                ColumnsRetrievalSpec::Indices(idx) => indices.clone_from(&idx),
            };

            let exclude_positions = column_positions_to_exclude(rel_child)?;

            for index in indices {
                let col_id = *child_node_row_list
                    .get(index)
                    .expect("Column id not found under relational child output");
                if exclude_positions[0] == Some(index) || exclude_positions[1] == Some(index) {
                    continue;
                }
                filtered_children_row_list.push((
                    index,
                    col_id,
                    ReferenceTarget::Single(rel_child),
                ));
            }
        } else {
            for (child_node_id, _) in source {
                let new_targets: ReferenceTarget = match source {
                    NewColumnsSource::ExceptUnion {
                        left_child,
                        right_child,
                    } => ReferenceTarget::Union(*left_child, *right_child),
                    NewColumnsSource::Join {
                        targets: JoinTargets::Both,
                        ..
                    }
                    | NewColumnsSource::Other { .. } => ReferenceTarget::Single(child_node_id),
                    _ => {
                        return Err(SbroadError::Invalid(
                            Entity::Node,
                            Some("WE DIDN'T expect here somehting".into()),
                        ))
                    }
                };

                let rel_node = self.get_relation_node(child_node_id)?;
                let child_row_list = self.get_row_list(rel_node.output())?;
                if need_sharding_column {
                    child_row_list.iter().enumerate().for_each(|(pos, id)| {
                        filtered_children_row_list.push((pos, *id, new_targets.clone()));
                    });
                } else {
                    let exclude_positions = column_positions_to_exclude(child_node_id)?;

                    for (pos, expr_id) in child_row_list.iter().enumerate() {
                        if exclude_positions[0] == Some(pos) || exclude_positions[1] == Some(pos) {
                            continue;
                        }
                        filtered_children_row_list.push((pos, *expr_id, new_targets.clone()));
                    }
                }
            }
        };

        // List of columns to be passed into `Expression::Row`.
        let mut result_row_list: Vec<NodeId> = Vec::with_capacity(filtered_children_row_list.len());
        for (pos, alias_node_id, new_targets) in filtered_children_row_list {
            let alias_expr = self.get_expression_node(alias_node_id)?;
            let asterisk_source = source.get_asterisk_source();
            let alias_name = SmolStr::from(alias_expr.get_alias_name()?);
            let col_type = alias_expr.calculate_type(self)?;

            let r_id = self
                .nodes
                .add_ref(new_targets, pos, col_type, asterisk_source);
            if need_aliases {
                let a_id = self.nodes.add_alias(&alias_name, r_id)?;
                result_row_list.push(a_id);
            } else {
                result_row_list.push(r_id);
            }
        }

        Ok(result_row_list)
    }

    /// New output for a single child node (with aliases)
    /// specified by indices we should retrieve from given `rel_node` output.
    ///
    /// # Errors
    /// Returns `SbroadError`:
    /// - child is an inconsistent relational node
    pub fn add_row_by_indices(
        &mut self,
        rel_node: NodeId,
        indices: Vec<usize>,
        need_sharding_column: bool,
        asterisk_source: Option<ReferenceAsteriskSource>,
    ) -> Result<NodeId, SbroadError> {
        let list = self.new_columns(
            &NewColumnsSource::Other {
                child: rel_node,
                columns_spec: Some(ColumnsRetrievalSpec::Indices(indices)),
                asterisk_source,
            },
            true,
            need_sharding_column,
        )?;
        Ok(self.nodes.add_row(list, None))
    }

    /// New output for a single child node (with aliases).
    ///
    /// If column names are empty, copy all the columns from the child.
    /// # Errors
    /// Returns `SbroadError`:
    /// - child is an inconsistent relational node
    /// - column names don't exist
    pub fn add_row_for_output(
        &mut self,
        rel_node: NodeId,
        col_names: &[&str],
        need_sharding_column: bool,
        asterisk_source: Option<ReferenceAsteriskSource>,
    ) -> Result<NodeId, SbroadError> {
        let specific_columns = if col_names.is_empty() {
            None
        } else {
            let col_names: Vec<ColumnWithScan> = col_names
                .iter()
                .map(|name| ColumnWithScan::new(name, None))
                .collect();
            Some(ColumnsRetrievalSpec::Names(col_names))
        };

        let list = self.new_columns(
            &NewColumnsSource::Other {
                child: rel_node,
                columns_spec: specific_columns,
                asterisk_source,
            },
            true,
            need_sharding_column,
        )?;
        Ok(self.nodes.add_row(list, None))
    }

    /// New output row for union node.
    ///
    /// # Errors
    /// Returns `SbroadError`:
    /// - children are inconsistent relational nodes
    pub fn add_row_for_union_except(
        &mut self,
        left: NodeId,
        right: NodeId,
    ) -> Result<NodeId, SbroadError> {
        let list = self.new_columns(
            &NewColumnsSource::ExceptUnion {
                left_child: left,
                right_child: right,
            },
            true,
            true,
        )?;
        Ok(self.nodes.add_row(list, None))
    }

    /// New output row for join node.
    ///
    /// Contains all the columns from left and right children.
    ///
    /// # Errors
    /// Returns `SbroadError`:
    /// - children are inconsistent relational nodes
    pub fn add_row_for_join(&mut self, left: NodeId, right: NodeId) -> Result<NodeId, SbroadError> {
        let list = self.new_columns(
            &NewColumnsSource::Join {
                outer_child: left,
                inner_child: right,
                targets: JoinTargets::Both,
            },
            true,
            true,
        )?;
        Ok(self.nodes.add_row(list, None))
    }

    /// Project columns from the child node.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the child.
    /// # Errors
    /// Returns `SbroadError`:
    /// - child is an inconsistent relational node
    /// - column names don't exist
    pub fn add_row_from_child(
        &mut self,
        child: NodeId,
        col_names: &[&str],
    ) -> Result<NodeId, SbroadError> {
        let specific_columns = if col_names.is_empty() {
            None
        } else {
            let col_names: Vec<ColumnWithScan> = col_names
                .iter()
                .map(|name| ColumnWithScan::new(name, None))
                .collect();
            Some(ColumnsRetrievalSpec::Names(col_names))
        };

        let list = self.new_columns(
            &NewColumnsSource::Other {
                child,
                columns_spec: specific_columns,
                asterisk_source: None,
            },
            false,
            true,
        )?;
        Ok(self.nodes.add_row(list, None))
    }

    /// Project all the columns from the child's subquery node.
    /// New columns don't have aliases.
    ///
    /// Returns сreated row id
    pub(crate) fn add_row_from_subquery(&mut self, sq_id: NodeId) -> Result<NodeId, SbroadError> {
        let sq_rel = self.get_relation_node(sq_id)?;
        let sq_output_id = sq_rel.output();
        let sq_alias_ids_len = self.get_row_list(sq_output_id)?.len();

        let mut new_refs = Vec::with_capacity(sq_alias_ids_len);
        for pos in 0..sq_alias_ids_len {
            let alias_id = *self
                .get_row_list(sq_output_id)?
                .get(pos)
                .expect("subquery output row already checked");
            let alias_type = self.get_expression_node(alias_id)?.calculate_type(self)?;
            let ref_id = self
                .nodes
                .add_ref(ReferenceTarget::Single(sq_id), pos, alias_type, None);
            new_refs.push(ref_id);
        }
        let row_id = self.nodes.add_row(new_refs.clone(), None);
        Ok(row_id)
    }

    /// Project column from the join's left branch.
    ///
    /// The new column doesn't have an alias.
    /// # Errors
    /// Returns `SbroadError`:
    /// - children are inconsistent relational nodes
    /// - column names don't exist
    pub fn add_ref_from_left_branch(
        &mut self,
        left: NodeId,
        right: NodeId,
        col_name: ColumnWithScan,
    ) -> Result<NodeId, SbroadError> {
        let list = self.new_columns(
            &NewColumnsSource::Join {
                outer_child: left,
                inner_child: right,
                targets: JoinTargets::Left {
                    columns_spec: Some(ColumnsRetrievalSpec::Names(vec![col_name])),
                },
            },
            false,
            true,
        )?;

        assert_eq!(
            list.len(),
            1,
            "join left side, more columns than 1 ({})",
            list.len()
        );

        Ok(list[0])
    }

    /// Project column from the join's right branch.
    ///
    /// The new column doesn't have an alias.
    /// # Errors
    /// Returns `SbroadError`:
    /// - children are inconsistent relational nodes
    /// - column names don't exist
    pub fn add_ref_from_right_branch(
        &mut self,
        left: NodeId,
        right: NodeId,
        col_name: ColumnWithScan,
    ) -> Result<NodeId, SbroadError> {
        let list = self.new_columns(
            &NewColumnsSource::Join {
                outer_child: left,
                inner_child: right,
                targets: JoinTargets::Right {
                    columns_spec: Some(ColumnsRetrievalSpec::Names(vec![col_name])),
                },
            },
            false,
            true,
        )?;

        assert_eq!(
            list.len(),
            1,
            "join right side, more columns than 1 ({})",
            list.len()
        );

        Ok(list[0])
    }

    /// A relational node pointed by the reference.
    /// In a case of a reference in the Motion node
    /// within a dispatched IR to the storage, returns
    /// the Motion node itself.
    pub fn get_relational_from_reference_node(
        &self,
        ref_id: NodeId,
    ) -> Result<NodeId, SbroadError> {
        if let Node::Expression(Expression::Reference(Reference { target, .. })) =
            self.get_node(ref_id)?
        {
            return match target {
                ReferenceTarget::Leaf => Err(SbroadError::UnexpectedNumberOfValues(
                    "Reference node has no targets".into(),
                )),
                ReferenceTarget::Single(child_id) => Ok(*child_id),
                ReferenceTarget::Union(_, _) | ReferenceTarget::Values(_) => {
                    Err(SbroadError::UnexpectedNumberOfValues(
                        "Reference expected to point exactly a single relational node".into(),
                    ))
                }
            };
        }
        Err(SbroadError::Invalid(Entity::Expression, None))
    }

    /// Get relational nodes referenced in the row.
    pub fn get_relational_nodes_from_row(
        &self,
        row_id: NodeId,
    ) -> Result<HashSet<NodeId, RandomState>, SbroadError> {
        let row = self.get_expression_node(row_id)?;
        let capacity = if let Expression::Row(Row { list, .. }) = row {
            list.len()
        } else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some("Node is not a row".into()),
            ));
        };
        let filter = |node_id: NodeId| -> bool {
            if let Ok(Node::Expression(Expression::Reference { .. })) = self.get_node(node_id) {
                return true;
            }
            false
        };
        let mut post_tree = PostOrderWithFilter::with_capacity(
            |node| self.nodes.expr_iter(node, false),
            capacity,
            Box::new(filter),
        );
        post_tree.populate_nodes(row_id);
        let nodes = post_tree.take_nodes();
        // We don't expect much relational references in a row (5 is a reasonable number).
        let mut rel_nodes: HashSet<NodeId, RandomState> =
            HashSet::with_capacity_and_hasher(5, RandomState::new());
        for LevelNode(_, id) in nodes {
            self.get_relational_nodes_from_references_into(id, &mut rel_nodes)?;
        }
        Ok(rel_nodes)
    }

    pub fn get_relational_nodes_from_reference(
        &self,
        ref_id: NodeId,
    ) -> Result<HashSet<NodeId, RandomState>, SbroadError> {
        if let Expression::Reference(..) = self.get_expression_node(ref_id)? {
        } else {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some("Node is not a reference".into()),
            ));
        }

        // We don't expect much relational references in a row (5 is a reasonable number).
        let mut rel_nodes: HashSet<NodeId, RandomState> =
            HashSet::with_capacity_and_hasher(5, RandomState::new());

        self.get_relational_nodes_from_references_into(ref_id, &mut rel_nodes)?;

        Ok(rel_nodes)
    }

    fn get_relational_nodes_from_references_into(
        &self,
        ref_id: NodeId,
        rel_nodes: &mut HashSet<NodeId, RandomState>,
    ) -> Result<(), SbroadError> {
        let reference = self.get_expression_node(ref_id)?;
        if let Expression::Reference(Reference { target, .. }) = reference {
            rel_nodes.reserve(target.len());
            for target_id in target.iter() {
                rel_nodes.insert(*target_id);
            }
        }
        Ok(())
    }

    /// Check that the node is a boolean equality and its children are both rows.
    #[must_use]
    pub fn is_bool_eq_with_rows(&self, node_id: NodeId) -> bool {
        let Ok(node) = self.get_expression_node(node_id) else {
            return false;
        };
        if let Expression::Bool(BoolExpr { left, op, right }) = node {
            if *op != Bool::Eq {
                return false;
            }

            let Ok(left_node) = self.get_expression_node(*left) else {
                return false;
            };

            let Ok(right_node) = self.get_expression_node(*right) else {
                return false;
            };

            if left_node.is_row() && right_node.is_row() {
                return true;
            }
        }

        false
    }

    /// The node is a trivalent (boolean or NULL).
    pub fn is_trivalent(&self, expr_id: NodeId) -> Result<bool, SbroadError> {
        let expr = self.get_expression_node(expr_id).map_err(|_| {
            SbroadError::Invalid(
                Entity::Node,
                Some("Unsupported node to check `is_trivalent`".into()),
            )
        })?;
        match expr {
            Expression::Bool(_)
            | Expression::Like { .. }
            | Expression::Arithmetic(_)
            | Expression::Unary(_)
            | Expression::Constant(Constant {
                value: Value::Boolean(_) | Value::Null,
                ..
            }) => return Ok(true),
            Expression::Row(Row { list, .. }) => {
                if let (Some(inner_id), None) = (list.first(), list.get(1)) {
                    return self.is_trivalent(*inner_id);
                }
            }
            Expression::Reference(Reference { col_type, .. }) => {
                let col_type_inner = col_type.get();
                return Ok(col_type_inner.is_none_or(|t| matches!(t, UnrestrictedType::Boolean)));
            }
            Expression::Parameter(_) => return Ok(true),
            _ => {}
        }
        Ok(false)
    }

    /// The node is a reference (or a row of a single reference column).
    ///
    /// # Errors
    /// - If node is not an expression.
    pub fn is_ref(&self, expr_id: NodeId) -> Result<bool, SbroadError> {
        let expr = self.get_expression_node(expr_id)?;
        match expr {
            Expression::Reference { .. } => return Ok(true),
            Expression::Row(Row { list, .. }) => {
                if let (Some(inner_id), None) = (list.first(), list.get(1)) {
                    return self.is_ref(*inner_id);
                }
            }
            _ => {}
        }
        Ok(false)
    }

    /// The node is a row
    ///
    /// # Errors
    /// - If node is not an expression.
    pub fn is_row(&self, expr_id: NodeId) -> Result<bool, SbroadError> {
        let expr = self.get_expression_node(expr_id)?;
        match expr {
            Expression::Row(..) => Ok(true),
            _ => Ok(false),
        }
    }

    /// Changes reference targets in relational node fields (output and other fields except children)
    /// rather than making changes in multiple places. This avoids having several
    /// sources that need to be changed and ensures the undo journal is handled properly.
    pub fn replace_target_in_relational(
        &mut self,
        parent_id: NodeId,
        from: NodeId,
        to: NodeId,
    ) -> Result<(), SbroadError> {
        self.replace_target_in_subtree(self.get_relational_output(parent_id)?, from, to)?;

        // We maintain the undo journal because sometimes we revert changes with undo, and their
        // references point to an old target. This is relevant for motion, for example, because
        // motion can appear after detaching the first version of a node.
        let parent = self.get_relation_node(parent_id)?;
        match parent {
            Relational::Join(Join { condition, .. }) => {
                let condition = *condition;
                self.replace_target_in_subtree(condition, from, to)?;
                // TODO(#2009): rethink support with undo
                let oldest = self.undo.get_oldest(&condition);
                if *oldest != condition {
                    self.replace_target_in_subtree(*oldest, from, to)?;
                }
            }
            Relational::OrderBy(OrderBy {
                order_by_elements, ..
            }) => {
                let mut refs = Vec::with_capacity(order_by_elements.len());
                for order in order_by_elements.iter() {
                    match order.entity {
                        OrderByEntity::Expression { expr_id } => {
                            refs.push(expr_id);
                        }
                        OrderByEntity::Index { .. } => {}
                    }
                }

                for ref_node in refs {
                    self.replace_target_in_subtree(ref_node, from, to)?;
                }
            }
            Relational::GroupBy(GroupBy { gr_exprs, .. }) => {
                let refs = gr_exprs.clone();
                for ref_node in refs {
                    self.replace_target_in_subtree(ref_node, from, to)?;
                }
            }
            Relational::Selection(Selection { filter, .. }) => {
                let filter = *filter;
                self.replace_target_in_subtree(filter, from, to)?;
                // TODO(#2009): rethink support with undo
                let oldest = self.undo.get_oldest(&filter);
                if *oldest != filter {
                    self.replace_target_in_subtree(*oldest, from, to)?;
                }
            }
            Relational::Having(Having { filter, .. }) => {
                let filter = *filter;
                self.replace_target_in_subtree(filter, from, to)?;
                // TODO(#2009): rethink support with undo
                let oldest = self.undo.get_oldest(&filter);
                if *oldest != filter {
                    self.replace_target_in_subtree(*oldest, from, to)?;
                }
            }
            Relational::NamedWindows(NamedWindows { windows, .. }) => {
                for window in windows.clone() {
                    self.replace_target_in_subtree(window, from, to)?;
                }
            }
            Relational::ScanCte(_) => {}
            Relational::Except(_) => {}
            Relational::Delete(_) => {}
            Relational::Insert(_) => {}
            Relational::Intersect(_) => {}
            Relational::Update(_) => {}
            Relational::Limit(_) => {}
            Relational::Motion(_) => {}
            Relational::Projection(_) => {}
            Relational::ScanRelation(_) => {}
            Relational::ScanSubQuery(_) => {}
            Relational::SelectWithoutScan(_) => {}
            Relational::UnionAll(_) => {}
            Relational::Union(_) => {}
            Relational::Values(_) => {}
            Relational::ValuesRow(_) => {}
        }

        Ok(())
    }

    /// Replace target from one to another for all references in the expression subtree of the provided node.
    pub fn replace_target_in_subtree(
        &mut self,
        node_id: NodeId,
        from_id: NodeId,
        to_id: NodeId,
    ) -> Result<(), SbroadError> {
        let filter = |node_id: NodeId| -> bool {
            if let Ok(Node::Expression(Expression::Reference { .. })) = self.get_node(node_id) {
                return true;
            }
            false
        };
        let mut subtree = PostOrderWithFilter::with_capacity(
            |node| self.nodes.expr_iter(node, false),
            EXPR_CAPACITY,
            Box::new(filter),
        );
        subtree.populate_nodes(node_id);
        let references = subtree.take_nodes();
        drop(subtree);
        for LevelNode(_, id) in references {
            if let MutExpression::Reference(Reference { target, .. }) =
                self.get_mut_expression_node(id)?
            {
                match target {
                    ReferenceTarget::Leaf => {}
                    ReferenceTarget::Single(node_id) => {
                        if node_id == &from_id {
                            *node_id = to_id;
                        }
                    }
                    ReferenceTarget::Union(left, right) => {
                        if left == &from_id {
                            *left = to_id;
                        }

                        if right == &from_id {
                            *right = to_id;
                        }
                    }
                    ReferenceTarget::Values(nodes) => {
                        nodes.iter_mut().for_each(|node| {
                            if node == &from_id {
                                *node = to_id;
                            }
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Replaces the target node ID for all references in the expression subtree of the provided
    /// node, except SubQeury and Motion.
    /// Unlike conditional replacement, this updates any reference.
    pub fn set_target_in_subtree(
        &mut self,
        expr_id: NodeId,
        rel_id: NodeId,
    ) -> Result<(), SbroadError> {
        let filter = |node_id: NodeId| -> bool {
            if let Ok(Node::Expression(Expression::Reference { .. })) = self.get_node(node_id) {
                return true;
            }
            false
        };
        let mut subtree = PostOrderWithFilter::with_capacity(
            |node| self.nodes.expr_iter(node, false),
            EXPR_CAPACITY,
            Box::new(filter),
        );
        subtree.populate_nodes(expr_id);
        let references = subtree.take_nodes();
        drop(subtree);
        for LevelNode(_, id) in references {
            // We don't change references to additional children because we could lose
            // connection with them.
            let node_id = self.get_relational_from_reference_node(id)?;
            let node = self.get_relation_node(node_id)?;
            if matches!(
                node,
                Relational::ScanSubQuery { .. } | Relational::Motion { .. }
            ) && self.is_additional_child(node_id)?
            {
                continue;
            }

            let node = self.get_mut_expression_node(id)?;
            if let MutExpression::Reference(Reference { target, .. }) = node {
                if !matches!(target, ReferenceTarget::Single(_)) {
                    panic!("try to change to single")
                }
                *target = ReferenceTarget::Single(rel_id);
            }
        }
        Ok(())
    }
}

impl Expression<'_> {
    /// Get a reference to the row children list.
    ///
    /// # Errors
    /// - node isn't `Row`
    pub fn get_row_list(&self) -> Result<&Vec<NodeId>, SbroadError> {
        match self {
            Expression::Row(Row { ref list, .. }) => Ok(list),
            _ => Err(SbroadError::Invalid(
                Entity::Expression,
                Some("node isn't Row type".into()),
            )),
        }
    }

    /// Gets alias node name.
    ///
    /// # Errors
    /// - node isn't `Alias`
    pub fn get_alias_name(&self) -> Result<&str, SbroadError> {
        match self {
            Expression::Alias(Alias { name, .. }) => Ok(name.as_str()),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some("node is not Alias type".into()),
            )),
        }
    }
}

#[cfg(test)]
mod tests;
