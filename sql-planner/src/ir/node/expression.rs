use serde::Serialize;
use smallvec::{smallvec, SmallVec};

use crate::{
    errors::{Entity, SbroadError},
    ir::{
        aggregates::AggregateKind,
        node::{Bound, BoundType, IndexExpr},
        operator::{Arithmetic, Bool, OrderByEntity, Unary},
    },
};

use super::{
    Alias, ArithmeticExpr, BoolExpr, Case, Cast, Concat, Constant, CountAsterisk, Like,
    NodeAligned, NodeId, Over, Parameter, Reference, Row, ScalarFunction, SubQueryReference,
    Timestamp, Trim, UnaryExpr, Window,
};

pub const EXPECTED_CHILDREN_CNT: usize = 4;

/// Trait for accessing child `NodeId`s of an expression node.
///
/// Implemented for each inner expression struct (e.g. `Alias`, `BoolExpr`, `Row`).
/// The three expression enums (`Expression`, `MutExpression`, `ExprOwned`) delegate
/// to these impls via one-liner match dispatch.
pub(crate) trait ExprChildren {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]>;
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]>;
}

// ── Leaf nodes (no children) ────────────────────────────────────────

impl ExprChildren for Constant {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        SmallVec::new()
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        SmallVec::new()
    }
}

impl ExprChildren for Reference {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        SmallVec::new()
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        SmallVec::new()
    }
}

impl ExprChildren for SubQueryReference {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        SmallVec::new()
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        SmallVec::new()
    }
}

impl ExprChildren for CountAsterisk {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        SmallVec::new()
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        SmallVec::new()
    }
}

impl ExprChildren for Timestamp {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        SmallVec::new()
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        SmallVec::new()
    }
}

impl ExprChildren for Parameter {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        SmallVec::new()
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        SmallVec::new()
    }
}

// ── Single child ────────────────────────────────────────────────────

impl ExprChildren for Alias {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![self.child]
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![&mut self.child]
    }
}

impl ExprChildren for Cast {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![self.child]
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![&mut self.child]
    }
}

impl ExprChildren for UnaryExpr {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![self.child]
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![&mut self.child]
    }
}

// ── Two children ────────────────────────────────────────────────────

impl ExprChildren for BoolExpr {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![self.left, self.right]
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![&mut self.left, &mut self.right]
    }
}

impl ExprChildren for ArithmeticExpr {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![self.left, self.right]
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![&mut self.left, &mut self.right]
    }
}

impl ExprChildren for Concat {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![self.left, self.right]
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![&mut self.left, &mut self.right]
    }
}

impl ExprChildren for IndexExpr {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![self.child, self.which]
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![&mut self.child, &mut self.which]
    }
}

// ── Three children ──────────────────────────────────────────────────

impl ExprChildren for Like {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![self.left, self.right, self.escape]
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        smallvec![&mut self.left, &mut self.right, &mut self.escape]
    }
}

// ── Optional + required ─────────────────────────────────────────────

impl ExprChildren for Trim {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        let mut children = SmallVec::new();
        if let Some(p) = self.pattern {
            children.push(p);
        }
        children.push(self.target);
        children
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        let mut children = SmallVec::new();
        if let Some(p) = &mut self.pattern {
            children.push(p);
        }
        children.push(&mut self.target);
        children
    }
}

impl ExprChildren for Over {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        let mut children = smallvec![self.stable_func];
        if let Some(flt) = self.filter {
            children.push(flt);
        }
        children.push(self.window);
        children
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        let mut children: SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> =
            smallvec![&mut self.stable_func];
        if let Some(flt) = &mut self.filter {
            children.push(flt);
        }
        children.push(&mut self.window);
        children
    }
}

// ── Variadic ────────────────────────────────────────────────────────

impl ExprChildren for Row {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        self.list.iter().copied().collect()
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        self.list.iter_mut().collect()
    }
}

impl ExprChildren for ScalarFunction {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        self.children.iter().copied().collect()
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        self.children.iter_mut().collect()
    }
}

// ── Case ────────────────────────────────────────────────────────────

impl ExprChildren for Case {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        let mut children = SmallVec::new();
        if let Some(e) = self.search_expr {
            children.push(e);
        }
        for &(cond, res) in &self.when_blocks {
            children.push(cond);
            children.push(res);
        }
        if let Some(e) = self.else_expr {
            children.push(e);
        }
        children
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        let mut children = SmallVec::new();
        if let Some(e) = &mut self.search_expr {
            children.push(e);
        }
        for (cond, res) in &mut self.when_blocks {
            children.push(cond);
            children.push(res);
        }
        if let Some(e) = &mut self.else_expr {
            children.push(e);
        }
        children
    }
}

// ── Window ──────────────────────────────────────────────────────────

fn push_bound_children(bound: &Bound, out: &mut SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]>) {
    let types = match bound {
        Bound::Single(bt) => [Some(bt), None],
        Bound::Between(a, b) => [Some(a), Some(b)],
    };
    for bt in types.into_iter().flatten() {
        if let BoundType::PrecedingOffset(id) | BoundType::FollowingOffset(id) = bt {
            out.push(*id);
        }
    }
}

fn push_bound_children_mut<'a>(
    bound: &'a mut Bound,
    out: &mut SmallVec<[&'a mut NodeId; EXPECTED_CHILDREN_CNT]>,
) {
    match bound {
        Bound::Single(bt) => {
            if let BoundType::PrecedingOffset(id) | BoundType::FollowingOffset(id) = bt {
                out.push(id);
            }
        }
        Bound::Between(a, b) => {
            if let BoundType::PrecedingOffset(id) | BoundType::FollowingOffset(id) = a {
                out.push(id);
            }
            if let BoundType::PrecedingOffset(id) | BoundType::FollowingOffset(id) = b {
                out.push(id);
            }
        }
    }
}

impl ExprChildren for Window {
    fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        let mut children = SmallVec::new();
        if let Some(p) = &self.partition {
            children.extend_from_slice(p);
        }
        if let Some(ord) = &self.ordering {
            for elem in ord {
                if let OrderByEntity::Expression { expr_id } = &elem.entity {
                    children.push(*expr_id);
                }
            }
        }
        if let Some(frame) = &self.frame {
            push_bound_children(&frame.bound, &mut children);
        }
        children
    }
    fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        let mut children = SmallVec::new();
        if let Some(p) = &mut self.partition {
            for id in p {
                children.push(id);
            }
        }
        if let Some(ord) = &mut self.ordering {
            for elem in ord {
                if let OrderByEntity::Expression { expr_id } = &mut elem.entity {
                    children.push(expr_id);
                }
            }
        }
        if let Some(frame) = &mut self.frame {
            push_bound_children_mut(&mut frame.bound, &mut children);
        }
        children
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum ExprOwned {
    Alias(Alias),
    Bool(BoolExpr),
    Arithmetic(ArithmeticExpr),
    Index(IndexExpr),
    Cast(Cast),
    Concat(Concat),
    Constant(Constant),
    Like(Like),
    Reference(Reference),
    SubQueryReference(SubQueryReference),
    Row(Row),
    ScalarFunction(ScalarFunction),
    Trim(Trim),
    Unary(UnaryExpr),
    CountAsterisk(CountAsterisk),
    Case(Case),
    Timestamp(Timestamp),
    Over(Over),
    Window(Window),
    Parameter(Parameter),
}

impl From<ExprOwned> for NodeAligned {
    fn from(value: ExprOwned) -> Self {
        match value {
            ExprOwned::Window(window) => window.into(),
            ExprOwned::Over(over) => over.into(),
            ExprOwned::Alias(alias) => alias.into(),
            ExprOwned::Arithmetic(arithm) => arithm.into(),
            ExprOwned::Bool(bool) => bool.into(),
            ExprOwned::Case(case) => case.into(),
            ExprOwned::Index(index) => index.into(),
            ExprOwned::Cast(cast) => cast.into(),
            ExprOwned::Concat(concat) => concat.into(),
            ExprOwned::Constant(constant) => constant.into(),
            ExprOwned::CountAsterisk(count) => count.into(),
            ExprOwned::Like(like) => like.into(),
            ExprOwned::Reference(reference) => reference.into(),
            ExprOwned::SubQueryReference(sq_reference) => sq_reference.into(),
            ExprOwned::Row(row) => row.into(),
            ExprOwned::ScalarFunction(stable_func) => stable_func.into(),
            ExprOwned::Trim(trim) => trim.into(),
            ExprOwned::Unary(unary) => unary.into(),
            ExprOwned::Timestamp(lt) => lt.into(),
            ExprOwned::Parameter(param) => param.into(),
        }
    }
}

/// Tuple tree build blocks.
///
/// A tuple describes a single portion of data moved among cluster nodes.
/// It consists of the ordered, strictly typed expressions with names
/// (columns) and additional information about data distribution policy.
///
/// Tuple is a tree with a `Row` top (level 0) and a list of the named
/// `Alias` columns (level 1). This convention is used across the code
/// and should not be changed. It ensures that we always know the
/// name of any column in the tuple and therefore simplifies AST
/// deserialization.
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum Expression<'a> {
    Alias(&'a Alias),
    Bool(&'a BoolExpr),
    Arithmetic(&'a ArithmeticExpr),
    Index(&'a IndexExpr),
    Cast(&'a Cast),
    Concat(&'a Concat),
    Constant(&'a Constant),
    Like(&'a Like),
    Reference(&'a Reference),
    SubQueryReference(&'a SubQueryReference),
    Row(&'a Row),
    ScalarFunction(&'a ScalarFunction),
    Trim(&'a Trim),
    Unary(&'a UnaryExpr),
    CountAsterisk(&'a CountAsterisk),
    Case(&'a Case),
    Timestamp(&'a Timestamp),
    Over(&'a Over),
    Window(&'a Window),
    Parameter(&'a Parameter),
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, PartialEq, Eq)]
pub enum MutExpression<'a> {
    Alias(&'a mut Alias),
    Bool(&'a mut BoolExpr),
    Arithmetic(&'a mut ArithmeticExpr),
    Index(&'a mut IndexExpr),
    Cast(&'a mut Cast),
    Concat(&'a mut Concat),
    Constant(&'a mut Constant),
    Like(&'a mut Like),
    Reference(&'a mut Reference),
    SubQueryReference(&'a mut SubQueryReference),
    Row(&'a mut Row),
    ScalarFunction(&'a mut ScalarFunction),
    Trim(&'a mut Trim),
    Unary(&'a mut UnaryExpr),
    CountAsterisk(&'a mut CountAsterisk),
    Case(&'a mut Case),
    Timestamp(&'a mut Timestamp),
    Over(&'a mut Over),
    Window(&'a mut Window),
    Parameter(&'a mut Parameter),
}

#[allow(dead_code)]
impl Expression<'_> {
    /// Clone the row children list.
    ///
    /// # Errors
    /// - node isn't `Row`
    pub fn clone_row_list(&self) -> Result<Vec<NodeId>, SbroadError> {
        match self {
            Expression::Row(Row { list, .. }) => Ok(list.clone()),
            _ => Err(SbroadError::Invalid(
                Entity::Expression,
                Some("node isn't Row type".into()),
            )),
        }
    }

    pub fn is_aggregate_name(name: &str) -> bool {
        // currently we support only simple aggregates
        AggregateKind::from_name(name).is_some()
    }

    pub fn is_aggregate_fun(&self) -> bool {
        match self {
            Expression::ScalarFunction(ScalarFunction { name, .. }) => {
                Expression::is_aggregate_name(name)
            }
            _ => false,
        }
    }

    /// The node is a row expression.
    pub fn is_row(&self) -> bool {
        matches!(self, Expression::Row(_))
    }

    pub fn is_arithmetic(&self) -> bool {
        matches!(self, Expression::Arithmetic(_))
    }

    /// If applicable, check if the operator in the node is associative.
    /// The main use of this method is to put less paretheses in expressions.
    /// Associativity is `(a * b) * c = a * (b * c)`.
    pub fn is_associative(&self) -> bool {
        match self {
            Expression::Arithmetic(ArithmeticExpr {
                op: Arithmetic::Add | Arithmetic::Multiply,
                ..
            }) => true,

            Expression::Bool(BoolExpr {
                op: Bool::And | Bool::Or,
                ..
            }) => true,

            _otherwise => false,
        }
    }

    pub fn may_need_parentheses(&self) -> bool {
        // Atomic expressions don't need parentheses.
        //
        // Note the difference between putting the whole expr
        // in parentheses vs protecting its parts. E.g.
        // A: `(xs[1])` vs B: `(xs)[1]`.
        //
        // This method only cares for A (the whole expr),
        // since every expression should take care of its
        // subexpressions on its own.
        //
        // XXX: please, do not use globs (_) here; write exhaustive matches instead.
        match self {
            // `expr AS name`
            | Expression::Alias(_)
            // `CASE ... WHEN ... THEN ... ELSE`
            | Expression::Case(_)
            // `10 :: int`
            | Expression::Cast(_)
            // `42` or any other literal
            | Expression::Constant(_)
            // `count(*)`
            | Expression::CountAsterisk(_)
            // `expr[index]`
            | Expression::Index(_)
            // `OVER (...)`
            | Expression::Over(_)
            // `$1`
            | Expression::Parameter(_)
            // -- doesn't have its own repesentation
            | Expression::Reference(_)
            // `ROW(...)`
            | Expression::Row(_)
            // `function_name(arg1, arg2, ...)`
            | Expression::ScalarFunction(_)
            // -- doesn't have its own repesentation
            | Expression::SubQueryReference(_)
            // `'2026-04-12'::timestamp`
            | Expression::Timestamp(_)
            // `TRIM('hello')`
            | Expression::Trim(_)
            // `PARTITION BY (...) ...`
            | Expression::Window(_)
            // `EXISTS (...)`
            | Expression::Unary(UnaryExpr {
                op: Unary::Exists, ..
            }) => false,

            // `a IS NULL`
            | Expression::Unary(UnaryExpr {
                op: Unary::IsNull, ..
            })
            // `NOT a`
            | Expression::Unary(UnaryExpr {
                op: Unary::Not, ..
            })
            // `a OR b`
            | Expression::Bool(_)
            // `a + b`
            | Expression::Arithmetic(_)
            // `a || b`
            | Expression::Concat(_)
            // `a LIKE '...'`
            | Expression::Like(_) => true,
        }
    }

    /// If applicable, get the precedence of an expression node operator.
    /// <https://www.postgresql.org/docs/18/sql-syntax-lexical.html#SQL-PRECEDENCE>
    pub fn precedence(&self) -> usize {
        // XXX: please, do not use globs (_) here; write exhaustive matches instead.
        match self {
            // These expressions cannot be torn apart by the operators
            // with higher precendence, so we can safely give them
            // **the lowest precedence**.
            //
            // This is beneficial in e.g.
            // ```
            // function(FOO, x AND y, BAR)
            // ```
            //
            // Since AND has higher precedence than a function call,
            // we don't need to enclose it in parentheses.
            Expression::Alias(_)
            | Expression::Case(_)
            | Expression::Constant(_)
            | Expression::CountAsterisk(_)
            | Expression::Over(_)
            | Expression::Parameter(_)
            | Expression::Reference(_)
            | Expression::Row(_)
            | Expression::ScalarFunction(_)
            | Expression::SubQueryReference(_)
            | Expression::Timestamp(_)
            | Expression::Trim(_)
            | Expression::Window(_)
            | Expression::Unary(UnaryExpr {
                op: Unary::Exists, ..
            }) => 0,

            Expression::Bool(BoolExpr { op: Bool::Or, .. }) => 1,
            Expression::Bool(BoolExpr { op: Bool::And, .. }) => 2,
            Expression::Unary(UnaryExpr { op: Unary::Not, .. }) => 3,

            Expression::Unary(UnaryExpr {
                op: Unary::IsNull, ..
            }) => 4,

            Expression::Bool(BoolExpr {
                op: Bool::Eq | Bool::NotEq | Bool::Lt | Bool::LtEq | Bool::Gt | Bool::GtEq,
                ..
            }) => 5,

            Expression::Like(_)
            | Expression::Bool(BoolExpr {
                op: Bool::In | Bool::Between,
                ..
            }) => 6,

            // "Any other operator" per the link above, meaning that
            // anything not explicitly listed in PG's operator
            // precedence table should go here.
            Expression::Concat(_) => 7,

            Expression::Arithmetic(ArithmeticExpr {
                op: Arithmetic::Add | Arithmetic::Subtract,
                ..
            }) => 8,

            Expression::Arithmetic(ArithmeticExpr {
                op: Arithmetic::Multiply | Arithmetic::Divide | Arithmetic::Modulo,
                ..
            }) => 9,

            Expression::Index(_) => 10,
            Expression::Cast(_) => 11,
        }
    }

    /// Returns all child `NodeId`s of this expression.
    #[must_use]
    pub fn expr_children(&self) -> SmallVec<[NodeId; EXPECTED_CHILDREN_CNT]> {
        match self {
            Expression::Alias(n) => n.expr_children(),
            Expression::Bool(n) => n.expr_children(),
            Expression::Arithmetic(n) => n.expr_children(),
            Expression::Index(n) => n.expr_children(),
            Expression::Cast(n) => n.expr_children(),
            Expression::Concat(n) => n.expr_children(),
            Expression::Constant(n) => n.expr_children(),
            Expression::Like(n) => n.expr_children(),
            Expression::Reference(n) => n.expr_children(),
            Expression::SubQueryReference(n) => n.expr_children(),
            Expression::Row(n) => n.expr_children(),
            Expression::ScalarFunction(n) => n.expr_children(),
            Expression::Trim(n) => n.expr_children(),
            Expression::Unary(n) => n.expr_children(),
            Expression::CountAsterisk(n) => n.expr_children(),
            Expression::Case(n) => n.expr_children(),
            Expression::Timestamp(n) => n.expr_children(),
            Expression::Over(n) => n.expr_children(),
            Expression::Window(n) => n.expr_children(),
            Expression::Parameter(n) => n.expr_children(),
        }
    }

    #[must_use]
    pub fn is_ref(&self) -> bool {
        matches!(self, Expression::Reference(_))
    }

    #[must_use]
    pub fn is_subquery_ref(&self) -> bool {
        matches!(self, Expression::SubQueryReference(_))
    }

    #[must_use]
    pub fn is_unary(&self) -> bool {
        matches!(self, Expression::Unary(_))
    }

    #[must_use]
    pub fn get_expr_owned(&self) -> ExprOwned {
        match self {
            Expression::Window(window) => ExprOwned::Window((*window).clone()),
            Expression::Over(over) => ExprOwned::Over((*over).clone()),
            Expression::Alias(alias) => ExprOwned::Alias((*alias).clone()),
            Expression::Arithmetic(arithm) => ExprOwned::Arithmetic((*arithm).clone()),
            Expression::Bool(bool) => ExprOwned::Bool((*bool).clone()),
            Expression::Case(case) => ExprOwned::Case((*case).clone()),
            Expression::Index(index) => ExprOwned::Index((*index).clone()),
            Expression::Cast(cast) => ExprOwned::Cast((*cast).clone()),
            Expression::Concat(con) => ExprOwned::Concat((*con).clone()),
            Expression::Constant(constant) => ExprOwned::Constant((*constant).clone()),
            Expression::Like(like) => ExprOwned::Like((*like).clone()),
            Expression::CountAsterisk(count) => ExprOwned::CountAsterisk((*count).clone()),
            Expression::Reference(reference) => ExprOwned::Reference((*reference).clone()),
            Expression::SubQueryReference(sq_reference) => {
                ExprOwned::SubQueryReference((*sq_reference).clone())
            }
            Expression::Row(row) => ExprOwned::Row((*row).clone()),
            Expression::ScalarFunction(sfunc) => ExprOwned::ScalarFunction((*sfunc).clone()),
            Expression::Trim(trim) => ExprOwned::Trim((*trim).clone()),
            Expression::Unary(unary) => ExprOwned::Unary((*unary).clone()),
            Expression::Timestamp(lt) => ExprOwned::Timestamp((*lt).clone()),
            Expression::Parameter(param) => ExprOwned::Parameter((*param).clone()),
        }
    }
}

impl MutExpression<'_> {
    /// Returns mutable references to all child `NodeId`s of this expression.
    pub fn expr_children_mut(&mut self) -> SmallVec<[&mut NodeId; EXPECTED_CHILDREN_CNT]> {
        match self {
            MutExpression::Alias(n) => n.expr_children_mut(),
            MutExpression::Bool(n) => n.expr_children_mut(),
            MutExpression::Arithmetic(n) => n.expr_children_mut(),
            MutExpression::Index(n) => n.expr_children_mut(),
            MutExpression::Cast(n) => n.expr_children_mut(),
            MutExpression::Concat(n) => n.expr_children_mut(),
            MutExpression::Constant(n) => n.expr_children_mut(),
            MutExpression::Like(n) => n.expr_children_mut(),
            MutExpression::Reference(n) => n.expr_children_mut(),
            MutExpression::SubQueryReference(n) => n.expr_children_mut(),
            MutExpression::Row(n) => n.expr_children_mut(),
            MutExpression::ScalarFunction(n) => n.expr_children_mut(),
            MutExpression::Trim(n) => n.expr_children_mut(),
            MutExpression::Unary(n) => n.expr_children_mut(),
            MutExpression::CountAsterisk(n) => n.expr_children_mut(),
            MutExpression::Case(n) => n.expr_children_mut(),
            MutExpression::Timestamp(n) => n.expr_children_mut(),
            MutExpression::Over(n) => n.expr_children_mut(),
            MutExpression::Window(n) => n.expr_children_mut(),
            MutExpression::Parameter(n) => n.expr_children_mut(),
        }
    }

    /// Get a mutable reference to the row children list.
    ///
    /// # Errors
    /// - node isn't `Row`
    pub fn get_row_list_mut(&mut self) -> Result<&mut Vec<NodeId>, SbroadError> {
        match self {
            MutExpression::Row(Row { ref mut list, .. }) => Ok(list),
            _ => Err(SbroadError::Invalid(
                Entity::Expression,
                Some("node isn't Row type".into()),
            )),
        }
    }
}

impl ExprOwned {
    /// Apply a fallible function to every child `NodeId` in this expression.
    pub fn try_map_children<E>(
        &mut self,
        mut f: impl FnMut(&mut NodeId) -> Result<(), E>,
    ) -> Result<(), E> {
        let children = match self {
            ExprOwned::Alias(n) => n.expr_children_mut(),
            ExprOwned::Bool(n) => n.expr_children_mut(),
            ExprOwned::Arithmetic(n) => n.expr_children_mut(),
            ExprOwned::Index(n) => n.expr_children_mut(),
            ExprOwned::Cast(n) => n.expr_children_mut(),
            ExprOwned::Concat(n) => n.expr_children_mut(),
            ExprOwned::Constant(n) => n.expr_children_mut(),
            ExprOwned::Like(n) => n.expr_children_mut(),
            ExprOwned::Reference(n) => n.expr_children_mut(),
            ExprOwned::SubQueryReference(n) => n.expr_children_mut(),
            ExprOwned::Row(n) => n.expr_children_mut(),
            ExprOwned::ScalarFunction(n) => n.expr_children_mut(),
            ExprOwned::Trim(n) => n.expr_children_mut(),
            ExprOwned::Unary(n) => n.expr_children_mut(),
            ExprOwned::CountAsterisk(n) => n.expr_children_mut(),
            ExprOwned::Case(n) => n.expr_children_mut(),
            ExprOwned::Timestamp(n) => n.expr_children_mut(),
            ExprOwned::Over(n) => n.expr_children_mut(),
            ExprOwned::Window(n) => n.expr_children_mut(),
            ExprOwned::Parameter(n) => n.expr_children_mut(),
        };
        for child in children {
            f(child)?;
        }
        Ok(())
    }
}
