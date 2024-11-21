use crate::errors::SbroadError;
use crate::ir::expression::ExpressionId;
use crate::ir::node::expression::Expression;
use crate::ir::node::{
    Alias, ArithmeticExpr, BoolExpr, Case, Cast, Concat, ExprInParentheses, Like, NodeId,
    Reference, Row, StableFunction, Trim, UnaryExpr,
};
use crate::ir::operator::Bool;
use crate::ir::transformation::redistribution::BoolOp;
use crate::ir::tree::traversal::{LevelNode, PostOrder, PostOrderWithFilter, EXPR_CAPACITY};
use crate::ir::{Node, Plan};
use std::collections::{HashMap, HashSet};
use std::slice::Iter;

/// Helper enum for specifying to which join children
/// expression in join condition refers
#[derive(Clone, PartialEq, Debug)]
pub enum Referred {
    /// Refers to both children:
    /// E.g ... on i.a = f(o.b)
    Both,
    /// Refers only to inner child:
    /// E.g ... on i.a = i.b
    Inner,
    /// Refers only to outer child:
    /// E.g ... on o.a = 10
    Outer,
    /// Does not refer to any of join children:
    /// E.g on (select a from t) = 3
    None,
}

impl Referred {
    pub fn add(&self, other: &Self) -> Self {
        match (self, other) {
            (_, Referred::Both) | (Referred::Both, _) => Referred::Both,
            (Referred::None, _) => other.clone(),
            (_, Referred::None) => self.clone(),
            (Referred::Inner, Referred::Inner) => Referred::Inner,
            (Referred::Outer, Referred::Outer) => Referred::Outer,
            (_, _) => Referred::Both,
        }
    }
}

/// Maps expression in join condition to `Referred`
struct ReferredMap(HashMap<ExpressionId, Referred>);

impl ReferredMap {
    #[allow(dead_code)]
    pub fn new() -> Self {
        ReferredMap(HashMap::new())
    }

    pub fn with_capacity(capacity: usize) -> Self {
        ReferredMap(HashMap::with_capacity(capacity))
    }

    pub fn get(&self, id: ExpressionId) -> Option<&Referred> {
        self.0.get(&id)
    }

    pub fn insert(&mut self, id: ExpressionId, referred: Referred) -> Option<Referred> {
        self.0.insert(id, referred)
    }

    pub fn get_or_none(&self, id: ExpressionId) -> &Referred {
        self.get(id).unwrap_or(&Referred::None)
    }

    pub fn new_from_join_condition(
        plan: &Plan,
        condition_id: NodeId,
        join_id: NodeId,
    ) -> Result<Self, SbroadError> {
        let mut referred = ReferredMap::with_capacity(EXPR_CAPACITY);
        let mut expr_tree =
            PostOrder::with_capacity(|node| plan.nodes.expr_iter(node, false), EXPR_CAPACITY);
        for LevelNode(_, node_id) in expr_tree.iter(condition_id) {
            let expr = plan.get_expression_node(node_id)?;
            let res = match expr {
                Expression::Bool(BoolExpr { left, right, .. })
                | Expression::Arithmetic(ArithmeticExpr { left, right, .. })
                | Expression::Concat(Concat { left, right, .. }) => referred
                    .get_or_none(*left)
                    .add(referred.get_or_none(*right)),
                Expression::Like(Like {
                    escape,
                    left,
                    right,
                }) => referred
                    .get_or_none(*left)
                    .add(referred.get_or_none(*right))
                    .add(referred.get_or_none(*escape)),
                Expression::Case(Case {
                    search_expr,
                    when_blocks,
                    else_expr,
                }) => {
                    let mut res = Referred::None;
                    if let Some(search_expr) = search_expr {
                        res = res.add(referred.get_or_none(*search_expr));
                    }
                    for (cond_expr, res_expr) in when_blocks {
                        res = res.add(referred.get_or_none(*cond_expr));
                        res = res.add(referred.get_or_none(*res_expr));
                    }
                    if let Some(else_expr) = else_expr {
                        res = res.add(referred.get_or_none(*else_expr));
                    }
                    res
                }
                Expression::Trim(Trim {
                    pattern, target, ..
                }) => match pattern {
                    Some(pattern) => referred
                        .get_or_none(*pattern)
                        .add(referred.get_or_none(*target)),
                    None => referred.get_or_none(*target).clone(),
                },
                Expression::Constant { .. } | Expression::CountAsterisk { .. } => Referred::None,
                Expression::Reference(Reference {
                    targets, parent, ..
                }) => {
                    if *parent == Some(join_id) && *targets == Some(vec![1]) {
                        Referred::Inner
                    } else if *parent == Some(join_id) && *targets == Some(vec![0]) {
                        Referred::Outer
                    } else {
                        Referred::None
                    }
                }
                Expression::Row(Row { list: children, .. })
                | Expression::StableFunction(StableFunction { children, .. }) => {
                    children.iter().fold(Referred::None, |acc, x| {
                        acc.add(referred.get(*x).unwrap_or(&Referred::None))
                    })
                }
                Expression::Alias(Alias { child, .. })
                | Expression::ExprInParentheses(ExprInParentheses { child })
                | Expression::Cast(Cast { child, .. })
                | Expression::Unary(UnaryExpr { child, .. }) => {
                    referred.get(*child).unwrap_or(&Referred::None).clone()
                }
            };
            referred.insert(node_id, res);
        }
        Ok(referred)
    }
}

type OutputColInner = usize;
type OutputColOuter = usize;
/// Helper struct to calculate pairs of columns by which
/// inner join is computed. More precisely, it stores
/// pairs of positions by which inner/outer child should
/// be segmented to perform repartition join.
///
/// The first column in pair refers to inner child,
/// The second column in pair refers to outer child.
///
/// e.g:
/// select * from o join i on i.a = o.b
/// eq cols: [(a, b)]
///
/// If equality columns are empty, it means that current expression (for which equality columns
/// were computed) does not contain equality operator where both inner and outer tables are used. And it
/// is not clear whether repartition join can be done for current expression. If this expression
/// is a subexpression of a bigger expression, then there's a chance for it:
/// ... on i.a = i.b (repartition join is not possible) eq cols: []
/// ... on i.a = i.b and i.a = o.b (repartition join is possible) eq cols: [(a, b)]
#[derive(Default, Debug)]
pub struct EqualityCols {
    pub pairs: Vec<(OutputColInner, OutputColOuter)>,
    distinct_inner: HashSet<OutputColInner>,
    distinct_outer: HashSet<OutputColOuter>,
}

/// Maps `expr_id` -> `EqualityCols` to perform repartition join as if
/// join condition's root was `expr_id`.
///
/// If the map does not contain given key (`expr_id`),
/// then this expression does not completely forbid
/// repartition join: if expression is a part of a bigger expression, there is
/// still a chance that Repartition join is possible.
///
/// For example:
/// `... on i.a = 3 and i.a = o.b`
/// here for left side of `and` there would be no `EqualityCols` in the map,
/// but repartition join is still possible.
/// On the other hand, if the join condition consists only of left side of `and`:
/// `... on i.a = 3`
/// Then repartition join is not possible for such condition.
#[derive(Debug)]
pub struct EqualityColsMap(HashMap<ExpressionId, EqualityCols>);

impl EqualityColsMap {
    pub fn new() -> EqualityColsMap {
        EqualityColsMap(HashMap::new())
    }

    pub fn remove_or_default(&mut self, expr_id: ExpressionId) -> EqualityCols {
        self.0.remove(&expr_id).unwrap_or_default()
    }

    pub fn insert(&mut self, expr_id: ExpressionId, eq_cols: EqualityCols) -> Option<EqualityCols> {
        self.0.insert(expr_id, eq_cols)
    }

    pub fn remove_expr(&mut self, expr_id: ExpressionId) -> Option<EqualityCols> {
        self.0.remove(&expr_id)
    }

    pub fn remove(&mut self, expr_id: ExpressionId) -> Option<EqualityCols> {
        self.0.remove(&expr_id)
    }
}

impl EqualityCols {
    pub fn with_capacity(capacity: usize) -> EqualityCols {
        EqualityCols {
            pairs: Vec::with_capacity(capacity),
            distinct_inner: HashSet::with_capacity(capacity),
            distinct_outer: HashSet::with_capacity(capacity),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// Derive `EqualityCols` for boolean node with bool children
    ///
    /// # Returns
    /// - `None` in case the subtree with this eq operator does not allow repartition join
    /// - empty `EqualityCols` in case the subtree does not forbid repartition join,
    ///   repartition join still can be done if the parent expression is "good".
    /// - non-empty `EqualityCols` in case the subtree is "good" and supports repartition join
    ///   by returned `EqualityCols`.
    fn eq_cols_for_bool(
        op: &BoolOp,
        node_id: NodeId,
        refers_to: &ReferredMap,
        node_eq_cols: &mut EqualityColsMap,
    ) -> Option<EqualityCols> {
        let result = match op.op {
            Bool::And => EqualityCols::eq_cols_for_and(op.left, op.right, refers_to, node_eq_cols),
            Bool::Between => unreachable!("Between in eq_cols"),
            Bool::Or
            | Bool::Eq
            | Bool::In
            | Bool::Gt
            | Bool::GtEq
            | Bool::Lt
            | Bool::LtEq
            | Bool::NotEq => {
                if let Some(Referred::Both) = refers_to.get(node_id) {
                    None
                } else {
                    Some(EqualityCols::default())
                }
            }
        };

        result
    }

    /// Derive `EqualityCols` for node `in`/`=` with row children.
    ///
    /// # Examples
    /// `... on i.a = o.b`
    /// gives the following equality cols: `[(i.a, o.b)]`
    ///
    /// `... on (i.a, i.b) = (o.c, o.d)` -> `[(i.a, o.c), (i.b, o.d)]`
    ///
    /// `... on i.a = i.b` -> `[]`
    ///
    /// `... on (i.a, o.b) = (i.c, i.b)` -> `[(i.b, o.b)]`
    ///
    /// `... on (i.a, i.a) = (o.b, o.d)` -> `[(i.a, o.b)]`
    ///
    /// `... on i.a + i.b = o.b` -> None
    ///
    /// `... on f(i.a) = o.b` -> None
    ///
    /// # Returns
    /// - `None` in case the subtree with this eq operator does not allow repartition join
    /// - empty `EqualityCols` in case the subtree does not forbid repartition join,
    ///   repartition join still can be done if the parent expression is "good".
    /// - non-empty `EqualityCols` in case the subtree is "good" and supports repartition join
    ///   by returned `EqualityCols`.
    fn eq_cols_for_eq(
        list_left: &[NodeId],
        list_right: &[NodeId],
        node_id: NodeId,
        inner_id: NodeId,
        outer_id: NodeId,
        plan: &Plan,
        refers_to: &ReferredMap,
    ) -> Result<Option<EqualityCols>, SbroadError> {
        let mut new_eq_cols: EqualityCols = EqualityCols::with_capacity(list_right.len());
        let mut is_repartition_join_possible = true;
        for (left_id, right_id) in list_left.iter().zip(list_right.iter()) {
            let left_node = plan.get_expression_node(*left_id)?;
            let right_node = plan.get_expression_node(*right_id)?;
            match (left_node, right_node) {
                (
                    Expression::Reference(Reference {
                        position: pos_left,
                        parent: parent_left,
                        col_type: col_type_left,
                        asterisk_source: asterisk_source_left,
                        ..
                    }),
                    Expression::Reference(Reference {
                        position: pos_right,
                        parent: parent_right,
                        col_type: col_type_right,
                        asterisk_source: asterisk_source_right,
                        ..
                    }),
                ) => {
                    // TODO: compare types only if the runtime requires it.

                    // if one Reference refers to one child, and the second one refers to another one,
                    // then we have an equality pair
                    if parent_left == parent_right
                        && col_type_left == col_type_right
                        && asterisk_source_left == asterisk_source_right
                    {
                        let left_referred_child_id =
                            plan.get_relational_from_reference_node(*left_id)?;
                        let right_referred_child_id =
                            plan.get_relational_from_reference_node(*right_id)?;
                        if left_referred_child_id == inner_id && right_referred_child_id == outer_id
                        {
                            new_eq_cols.add_equality_pair(*pos_left, *pos_right);
                        } else if left_referred_child_id == outer_id
                            && right_referred_child_id == inner_id
                        {
                            new_eq_cols.add_equality_pair(*pos_right, *pos_left);
                        }
                    };
                }
                (Expression::Constant(_), _) | (_, Expression::Constant(_)) => {}
                (_, _) => {
                    // if some kind of transformation is applied to another side of equality
                    // operator, we can't do repartition join, for example:
                    // ... on i.a = f(o.b), we don't know anything about `f`
                    if let Some(Referred::Both) = refers_to.get(node_id) {
                        is_repartition_join_possible = false;
                        break;
                    }
                }
            }
        }
        if is_repartition_join_possible {
            Ok(Some(new_eq_cols))
        } else {
            Ok(None)
        }
    }

    /// Derive `EqualityCols` for boolean node with row children.
    ///
    /// # Returns
    /// - `None` in case the subtree with this eq operator does not allow repartition join
    /// - empty `EqualityCols` in case the subtree does not forbid repartition join,
    ///   repartition join still can be done if the parent expression is "good".
    /// - non-empty `EqualityCols` in case the subtree is "good" and supports repartition join
    ///   by returned `EqualityCols`.
    fn eq_cols_for_rows(
        op: &BoolOp,
        node_id: NodeId,
        refers_to: &ReferredMap,
        inner_id: NodeId,
        outer_id: NodeId,
        plan: &Plan,
    ) -> Result<Option<EqualityCols>, SbroadError> {
        let left_expr = plan.get_expression_node(op.left)?;
        let right_expr = plan.get_expression_node(op.right)?;
        let res = match (left_expr, right_expr) {
            (
                Expression::Row(Row {
                    list: list_left, ..
                }),
                Expression::Row(Row {
                    list: list_right, ..
                }),
            ) => match op.op {
                Bool::Eq | Bool::In => EqualityCols::eq_cols_for_eq(
                    list_left, list_right, node_id, inner_id, outer_id, plan, refers_to,
                )?,
                _ => {
                    if let Some(Referred::Both) = refers_to.get(node_id) {
                        None
                    } else {
                        Some(EqualityCols::default())
                    }
                }
            },
            _ => None,
        };
        Ok(res)
    }

    /// Derive `EqualityCols` for boolean `and` node if possible
    ///
    /// # Examples
    /// `... on i.a = o.b and i.c = o.d`
    /// left side has equality cols: `[(i.a, o.b)]`
    /// right side has: `[(i.c, o.d)]`
    /// `and` produces: `[(i.a, o.b), (i.c, o.d)]`
    ///
    /// `... on i.a = o.b and o.b = o.d`
    /// left side has: `[(i.a, o.b)]`
    /// right side has: `[]`
    /// `and` produces: `[(i.a, o.b)]`
    ///
    /// `... on i.a = o.b and i.a = o.d`
    /// left side has: `[(i.a, o.b)]`
    /// right side has: `[(i.a, o.d)]`
    /// `and` produces: `[(i.a, o.b)]`
    /// Explanation: we can rewrite join condition as `i.a = o.b and o.b = o.d`
    ///
    /// `... on i.a = o.b and cast(i.a as bool)`
    /// left side has: `[(i.a, o.b)]`
    /// right side has: `[]` (it depends only on one table)
    /// `and` produces: `[(i.a, o.b)]`
    ///
    /// # Returns
    /// - `None` in case the subtree with this `and` does not allow repartition join
    /// - empty `EqualityCols` in case the subtree does not forbid repartition join,
    ///   repartition join still can be done if the parent expression is "good".
    /// - non-empty `EqualityCols` in case the subtree is "good" and supports repartition join
    ///   by returned `EqualityCols`.
    fn eq_cols_for_and(
        left: NodeId,
        right: NodeId,
        refers_to: &ReferredMap,
        map: &mut EqualityColsMap,
    ) -> Option<EqualityCols> {
        let left_referred = refers_to.get(left).unwrap_or(&Referred::None).clone();
        let right_referred = refers_to.get(right).unwrap_or(&Referred::None).clone();

        let eq_cols = if left_referred != Referred::Both
            && right_referred != Referred::Both
            && left_referred == right_referred
        {
            // whole `and` expression refers to single child or None
            // No equalities that allow repartition join
            // E.g: `i.a = 3 and o.b in (SubQuery)`, `i.a = i.b and i.a = i.c`
            EqualityCols::default()
        } else if left_referred != Referred::Both && right_referred == Referred::Both {
            // left expression refers to one child or None
            // right expression refers to both children
            // E.g: `i.a = i.b and i.a = o.c`, `i.a = 3 and f(i.a) = o.c`
            map.remove_or_default(right)
        } else if left_referred == Referred::Both && right_referred != Referred::Both {
            // left expression refers to both children
            // right expression refers to one child or None
            map.remove_or_default(left)
        } else if let (Some(left_eq_cols), Some(right_eq_cols)) = (
            // both left and right expressions refer to expressions that provide EqualityCols
            // E.g: `i.a = i.b and i.c = o.d`,
            // `(i.a = i.b and i.c = o.d`) and i.a = o.d`
            map.remove_expr(left),
            map.remove_expr(right),
        ) {
            let mut new_eq_cols = left_eq_cols;
            new_eq_cols.merge_and(right_eq_cols);
            new_eq_cols
        } else {
            return None;
        };
        Some(eq_cols)
    }

    /// Find `EqualityCols` for the given join condition
    ///
    /// `EqualityCols` specify by what columns repartition join can be done.
    /// But for many join conditions, repartition join is not possible:
    /// `... on i.a < i.b`
    /// In general, this function is able to calculate `EqualityCols` for condition that
    /// consists of equality operators `in`/`=` connected by `and`.
    ///
    /// For example:
    /// `... on (i.a, i.b) = (i.c, o.d) and i.a = o.k and o.k = i.b`
    /// gives the following equality columns: `[(i.b, o.d), (i.a, o.k)]`,
    /// Which means we can do repartition join by resharding `i` by `(b, a)` and `o` by `(d, k)`
    ///
    /// Note, that a more broad class of expressions is recognised by this function:
    /// if some boolean expression depending only on one join child is added to `and`-chain
    /// the equality cols still can be calculated.
    ///
    /// For example:
    /// `... on (i.a, i.b) = (i.c, o.d) and i.a < i.b and o.b < 3 and i.a in SQ`
    /// produces the following equality cols: `[(i.b, o.d)]`
    ///
    /// # Returns
    /// - `None` in case this join condition does
    ///   not allow Repartition join.
    /// - Otherwise, returns non-empty `EqualityCols` wrapped in `Option`
    pub fn from_join_condition(
        plan: &Plan,
        join_id: NodeId,
        inner_id: NodeId,
        condition_id: NodeId,
    ) -> Result<Option<EqualityCols>, SbroadError> {
        let mut node_eq_cols: EqualityColsMap = EqualityColsMap::new();
        let outer_id = plan.get_relational_child(join_id, 0)?;
        let refers_to = ReferredMap::new_from_join_condition(plan, condition_id, join_id)?;
        let filter = |node_id: NodeId| -> bool {
            if let Ok(Node::Expression(Expression::Bool(_))) = plan.get_node(node_id) {
                return true;
            }
            false
        };
        let mut expr_tree = PostOrderWithFilter::with_capacity(
            |node| plan.nodes.expr_iter(node, true),
            EXPR_CAPACITY,
            Box::new(filter),
        );
        for level_node in expr_tree.iter(condition_id) {
            let node_id = level_node.1;
            let bool_op = BoolOp::from_expr(plan, node_id)?;
            let left_expr = plan.get_expression_node(bool_op.left)?;
            let right_expr = plan.get_expression_node(bool_op.right)?;
            let new_eq_cols = match (left_expr, right_expr) {
                (Expression::Row(_), Expression::Row(_)) => EqualityCols::eq_cols_for_rows(
                    &bool_op, node_id, &refers_to, inner_id, outer_id, plan,
                )?,
                (_, _) => {
                    EqualityCols::eq_cols_for_bool(&bool_op, node_id, &refers_to, &mut node_eq_cols)
                }
            };
            if let Some(cols) = new_eq_cols {
                if !cols.is_empty() {
                    node_eq_cols.insert(node_id, cols);
                }
            } else {
                break;
            }
        }

        let res = if let Some(eq_cols) = node_eq_cols.remove(condition_id) {
            if eq_cols.pairs.is_empty() {
                None
            } else {
                Some(eq_cols)
            }
        } else {
            None
        };
        Ok(res)
    }

    /// Adds a new equality pair to equality columns
    ///
    /// This function is used to compute equality columns for `=`, `in`
    /// operators:
    /// ... on (i.a, o.c, i.a) = (o.b, i.m, o.f) eq cols: \[(a, b), (m, c)\]
    /// Here we didn't add pair (a, f) because we already have pair (a, b):
    /// (i.a, i.a) = (o.b, o.f) <=> o.b = o.f and i.a = o.b for which eq cols: \[(a, b)\]
    pub fn add_equality_pair(&mut self, inner: usize, outer: usize) {
        if self.distinct_outer.insert(outer) && self.distinct_inner.insert(inner) {
            self.pairs.push((inner, outer));
        }
    }

    /// Merge equality columns from left and right sides of `and` operator
    pub fn merge_and(&mut self, other: EqualityCols) {
        for (i, o) in other.pairs {
            self.add_equality_pair(i, o);
        }
    }

    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    pub fn iter(&self) -> Iter<'_, (OutputColInner, OutputColOuter)> {
        self.pairs.iter()
    }
}
