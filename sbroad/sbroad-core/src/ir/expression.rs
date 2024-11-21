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

use super::node::Like;
use super::{
    distribution, operator, Alias, ArithmeticExpr, BoolExpr, Case, Cast, Concat, Constant,
    ExprInParentheses, Expression, LevelNode, MutExpression, MutNode, Node, NodeId, Reference,
    Relational, Row, StableFunction, Trim, UnaryExpr, Value,
};
use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::to_user;
use crate::ir::node::ReferenceAsteriskSource;
use crate::ir::operator::Bool;
use crate::ir::relation::Type;
use crate::ir::tree::traversal::{PostOrderWithFilter, EXPR_CAPACITY};
use crate::ir::{Nodes, Plan, Positions as Targets};

pub mod cast;
pub mod concat;
pub mod types;

pub(crate) type ExpressionId = NodeId;

#[derive(Clone, Debug, Hash, Deserialize, PartialEq, Eq, Serialize)]
pub enum FunctionFeature {
    /// Current function is an aggregate function and is marked as DISTINCT.
    Distinct,
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
    /// Adds exression covered with parentheses node.
    ///
    /// # Errors
    /// - child node is invalid
    pub(crate) fn add_covered_with_parentheses(&mut self, child: NodeId) -> NodeId {
        let covered_with_parentheses = ExprInParentheses { child };
        self.push(covered_with_parentheses.into())
    }

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
        parent: Option<NodeId>,
        targets: Option<Vec<usize>>,
        position: usize,
        col_type: Type,
        asterisk_source: Option<ReferenceAsteriskSource>,
    ) -> NodeId {
        let r = Reference {
            parent,
            targets,
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

impl<'plan> Hash for PlanExpr<'plan> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut comp = Comparator::new(self.plan);
        comp.set_hasher(state);
        comp.hash_for_expr(self.id, EXPR_HASH_DEPTH);
    }
}

impl<'plan> PartialEq for PlanExpr<'plan> {
    fn eq(&self, other: &Self) -> bool {
        let comp = Comparator::new(self.plan);
        comp.are_subtrees_equal(self.id, other.id).unwrap_or(false)
    }
}

impl<'plan> Eq for PlanExpr<'plan> {}

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
        if let Node::Expression(left) = l {
            if let Node::Expression(right) = r {
                match left {
                    Expression::Alias(_) => {}
                    Expression::CountAsterisk(_) => {
                        return Ok(matches!(right, Expression::CountAsterisk(_)))
                    }
                    Expression::ExprInParentheses(ExprInParentheses { child: l_child }) => {
                        if let Expression::ExprInParentheses(ExprInParentheses { child: r_child }) =
                            right
                        {
                            return self.are_subtrees_equal(*l_child, *r_child);
                        }
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
                            let mut search_expr_equal = false;
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

                            let mut else_expr_equal = false;
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
                        targets: t_left,
                        position: p_left,
                        ..
                    }) => {
                        if let Expression::Reference(Reference {
                            targets: t_right,
                            position: p_right,
                            ..
                        }) = right
                        {
                            return Ok(t_left == t_right && p_left == p_right);
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
                    Expression::StableFunction(StableFunction {
                        name: name_left,
                        children: children_left,
                        feature: feature_left,
                        func_type: func_type_left,
                        is_system: is_aggr_left,
                    }) => {
                        if let Expression::StableFunction(StableFunction {
                            name: name_right,
                            children: children_right,
                            feature: feature_right,
                            func_type: func_type_right,
                            is_system: is_aggr_right,
                        }) = right
                        {
                            return Ok(name_left == name_right
                                && feature_left == feature_right
                                && func_type_left == func_type_right
                                && is_aggr_left == is_aggr_right
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
            Expression::ExprInParentheses(ExprInParentheses { child }) => {
                self.hash_for_child_expr(*child, depth);
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
                parent: _,
                position,
                targets,
                col_type,
                asterisk_source: is_asterisk,
            }) => {
                position.hash(state);
                targets.hash(state);
                col_type.hash(state);
                is_asterisk.hash(state);
            }
            Expression::Row(Row { list, .. }) => {
                for child in list {
                    self.hash_for_child_expr(*child, depth);
                }
            }
            Expression::StableFunction(StableFunction {
                name,
                children,
                func_type,
                feature,
                is_system: is_aggr,
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

        // Note: sorting of usizes doesn't take much time.
        res.sort_unstable();
        Ok(res)
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

impl<'targets> Iterator for NewColumnSourceIterator<'targets> {
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
    fn is_join(&self) -> bool {
        matches!(self, NewColumnsSource::Join { .. })
    }

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

    fn targets(&self) -> Vec<usize> {
        match self {
            NewColumnsSource::Join { targets, .. } => match targets {
                JoinTargets::Left { .. } => vec![0],
                JoinTargets::Right { .. } => vec![1],
                JoinTargets::Both => vec![0, 1],
            },
            NewColumnsSource::ExceptUnion { .. } => vec![0, 1],
            NewColumnsSource::Other { .. } => vec![0],
        }
    }

    fn iter(&'source self) -> NewColumnSourceIterator {
        <&Self as IntoIterator>::into_iter(self)
    }
}

impl Plan {
    /// Add `Row` to plan.
    pub fn add_row(&mut self, list: Vec<NodeId>, distribution: Option<Distribution>) -> NodeId {
        self.nodes.add_row(list, distribution)
    }

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
        let mut filtered_children_row_list: Vec<(usize, NodeId, Vec<usize>)> = Vec::new();

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
                filtered_children_row_list.push((index, col_id, source.targets()));
            }
        } else {
            for (child_node_id, target_idx) in source {
                let new_targets: Vec<usize> = if source.is_join() {
                    vec![target_idx]
                } else {
                    source.targets()
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
                .add_ref(None, Some(new_targets), pos, col_type, asterisk_source);
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
    /// `expected_output_size` is:
    /// * 1 in case of scalar `SubQuery`. `(values (1)) = 1`
    /// * Output size of left child of IN operator. `(1, 2) in (select a, b from t)`
    /// * None in case of EXISTS operator. `exists (select * from t)`
    ///
    /// Returns a pair of:
    /// * Created row id
    /// * Vec of created references ids whose `parent` and `target` should be changed.
    ///
    /// # Errors
    /// - children nodes are inconsistent with the target position
    pub(crate) fn add_row_from_subquery(
        &mut self,
        sq_id: NodeId,
        expected_output_size: Option<usize>,
    ) -> Result<(NodeId, Vec<NodeId>), SbroadError> {
        let sq_rel = self.get_relation_node(sq_id)?;
        let sq_output_id = sq_rel.output();
        let sq_alias_ids_len = self.get_row_list(sq_output_id)?.len();
        if let Some(expected_output_size) = expected_output_size {
            if expected_output_size != sq_alias_ids_len {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some(format_smolstr!("SubQuery expected to have {expected_output_size} rows output, but got {sq_alias_ids_len}."))
                ));
            }
        }

        let mut new_refs = Vec::with_capacity(sq_alias_ids_len);
        for pos in 0..sq_alias_ids_len {
            let alias_id = *self
                .get_row_list(sq_output_id)?
                .get(pos)
                .expect("subquery output row already checked");
            let alias_type = self.get_expression_node(alias_id)?.calculate_type(self)?;
            let ref_id = self.nodes.add_ref(None, None, pos, alias_type, None);
            new_refs.push(ref_id);
        }
        let row_id = self.nodes.add_row(new_refs.clone(), None);
        Ok((row_id, new_refs))
    }

    /// Project columns from the join's left branch.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the left child.
    /// # Errors
    /// Returns `SbroadError`:
    /// - children are inconsistent relational nodes
    /// - column names don't exist
    pub fn add_row_from_left_branch(
        &mut self,
        left: NodeId,
        right: NodeId,
        col_names: &[ColumnWithScan],
    ) -> Result<NodeId, SbroadError> {
        let list = self.new_columns(
            &NewColumnsSource::Join {
                outer_child: left,
                inner_child: right,
                targets: JoinTargets::Left {
                    columns_spec: Some(ColumnsRetrievalSpec::Names(col_names.to_vec())),
                },
            },
            false,
            true,
        )?;
        Ok(self.nodes.add_row(list, None))
    }

    /// Project columns from the join's right branch.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the right child.
    /// # Errors
    /// Returns `SbroadError`:
    /// - children are inconsistent relational nodes
    /// - column names don't exist
    pub fn add_row_from_right_branch(
        &mut self,
        left: NodeId,
        right: NodeId,
        col_names: &[ColumnWithScan],
    ) -> Result<NodeId, SbroadError> {
        let list = self.new_columns(
            &NewColumnsSource::Join {
                outer_child: left,
                inner_child: right,
                targets: JoinTargets::Right {
                    columns_spec: Some(ColumnsRetrievalSpec::Names(col_names.to_vec())),
                },
            },
            false,
            true,
        )?;
        Ok(self.nodes.add_row(list, None))
    }

    /// A relational node pointed by the reference.
    /// In a case of a reference in the Motion node
    /// within a dispatched IR to the storage, returns
    /// the Motion node itself.
    pub fn get_relational_from_reference_node(
        &self,
        ref_id: NodeId,
    ) -> Result<NodeId, SbroadError> {
        if let Node::Expression(Expression::Reference(Reference {
            targets, parent, ..
        })) = self.get_node(ref_id)?
        {
            let Some(referred_rel_id) = parent else {
                panic!("Reference ({ref_id}) parent node not found.");
            };
            let rel = self.get_relation_node(*referred_rel_id)?;
            if let Relational::Insert { .. } = rel {
                return Ok(*referred_rel_id);
            }
            let children = self.children(*referred_rel_id);
            match targets {
                None => {
                    return Err(SbroadError::UnexpectedNumberOfValues(
                        "Reference node has no targets".into(),
                    ))
                }
                Some(positions) => match (positions.first(), positions.get(1)) {
                    (Some(first), None) => {
                        if let Some(child_id) = children.get(*first) {
                            return Ok(*child_id);
                        }
                        // When we dispatch IR to the storage, we truncate the
                        // subtree below the Motion node. So, the references in
                        // the Motion's output row are broken. We treat them in
                        // a special way: we return the Motion node itself. Be
                        // aware of the circular references in the tree!
                        if let Relational::Motion { .. } = rel {
                            return Ok(*referred_rel_id);
                        }
                        return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                            "Relational node {rel:?} has no children"
                        )));
                    }
                    _ => {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "Reference expected to point exactly a single relational node".into(),
                        ))
                    }
                },
            }
        }
        Err(SbroadError::Invalid(Entity::Expression, None))
    }

    /// Get relational nodes referenced in the row.
    ///
    /// # Errors
    /// - node is not a row
    /// - row is invalid
    /// - `relational_map` is not initialized
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
            let reference = self.get_expression_node(id)?;
            if let Expression::Reference(Reference {
                targets, parent, ..
            }) = reference
            {
                let referred_rel_id = parent.ok_or_else(|| {
                    SbroadError::NotFound(
                        Entity::Node,
                        format_smolstr!("that is Reference ({id}) parent"),
                    )
                })?;
                let rel = self.get_relation_node(referred_rel_id)?;
                let children = rel.children();
                if let Some(positions) = targets {
                    for pos in positions {
                        if let Some(child) = children.get(*pos) {
                            rel_nodes.insert(*child);
                        }
                    }
                }
            }
        }
        Ok(rel_nodes)
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
        let expr_node = self.get_node(expr_id)?;
        let expr = match expr_node {
            Node::Parameter(_) => return Ok(true),
            Node::Expression(expr) => expr,
            _ => panic!("Unsupported node to check `is_trivalent`: {expr_node:?}."),
        };
        match expr {
            Expression::Bool(_)
            | Expression::Like { .. }
            | Expression::Arithmetic(_)
            | Expression::Unary(_)
            | Expression::Constant(Constant {
                value: Value::Boolean(_) | Value::Null,
                ..
            }) => return Ok(true),
            Expression::ExprInParentheses(ExprInParentheses { child }) => {
                return self.is_trivalent(*child)
            }
            Expression::Row(Row { list, .. }) => {
                if let (Some(inner_id), None) = (list.first(), list.get(1)) {
                    return self.is_trivalent(*inner_id);
                }
            }
            Expression::Reference(Reference { col_type, .. }) => {
                return Ok(matches!(col_type, Type::Boolean))
            }
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

    /// Replace parent for all references in the expression subtree of the current node.
    ///
    /// # Errors
    /// - node is invalid
    /// - node is not an expression
    pub fn replace_parent_in_subtree(
        &mut self,
        node_id: NodeId,
        from_id: Option<NodeId>,
        to_id: Option<NodeId>,
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
            let mut node = self.get_mut_expression_node(id)?;
            node.replace_parent_in_reference(from_id, to_id);
        }
        Ok(())
    }

    /// Flush parent to `None` for all references in the expression subtree of the current node.
    ///
    /// # Errors
    /// - node is invalid
    /// - node is not an expression
    pub fn flush_parent_in_subtree(&mut self, node_id: NodeId) -> Result<(), SbroadError> {
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
            let mut node = self.get_mut_expression_node(id)?;
            node.flush_parent_in_reference();
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
