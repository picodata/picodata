//! Structural comparison for expressions - one walk for both states.
//!
//! The walk is generic over the state; the arms where [`Raw`] and [`Analyzed`]
//! genuinely diverge go through the hooks on [`AstState`] (the comparison
//! subject, casts, window functions) and through the per-state impls of the
//! state-swapped payloads (column references). Everything else is the plain
//! syntactic payload, identical in both states: [`Raw`] metadata is empty, and
//! [`crate::AnalyzedExprMeta`] takes no part either - the `id` is minted fresh
//! per node, so equal ids would mean "the same node" rather than "the same
//! shape", and `data_type` is still unset at the only point this runs (binding,
//! before type derivation writes types back).
//!
//! The [`PartialEq`] impls are entry-point sugar over raw nodes for the
//! parser's precedence and re-parse tests. They live in this crate rather than
//! next to those tests because `PartialEq` is a foreign trait: implementing it
//! for [`Expr`] is only legal in the crate that declares [`Expr`].
//!
//! Neither state normalizes anything. Two expressions that mean the same
//! thing but are written differently - `1` and `01`, `SUBSTRING(s FROM 1)` and
//! `SUBSTRING(s, 1)`, `t1.a + t1.c` and `t1.c + t1.a` - are not equal here.
//! That is the intended reading of "structural": what the query said, not what
//! it denotes. The one exception is what analysis itself adds: [`Analyzed`]
//! ignores cast spelling and the identity casts wrapped around resolved
//! columns, and compares a column reference as the column it resolved to
//! rather than the name it was written with. Two spellings that resolve to one
//! column are equal; one spelling that resolves to two different columns is
//! not.

use super::*;
use crate::structural_eq::StructuralEq;

impl<'q, State: AstState<'q>> Expr<'q, State> {
    /// Structural comparison, from an empty scope. The entry point for a
    /// single pair of expressions; `==` is the same comparison for raw nodes.
    ///
    /// Deliberately partial in [`Analyzed`]: window functions never compare
    /// equal, so this is not the reflexive relation `Eq` would promise.
    pub fn structural_eq(&self, other: &Self) -> bool {
        self.eq_with(other, &mut State::EqScope::default())
    }
}

/// Structural comparison for raw expressions.
impl PartialEq for Expr<'_, Raw> {
    fn eq(&self, other: &Self) -> bool {
        self.structural_eq(other)
    }
}

/// Rows compare elementwise.
impl PartialEq for ValuesRow<'_, Raw> {
    fn eq(&self, other: &Self) -> bool {
        self.eq_with(other, &mut ())
    }
}

/// The walk itself. Each arm pairs a variant with itself.
impl<'q, State: AstState<'q>> StructuralEq<State::EqScope> for Expr<'q, State> {
    fn eq_with(&self, other: &Self, scope: &mut State::EqScope) -> bool {
        let (a, b) = (State::eq_subject(self), State::eq_subject(other));
        match (a.inner_ref(), b.inner_ref()) {
            (ExprInner::Nil, ExprInner::Nil) => true,
            (ExprInner::BinaryOperation(x), ExprInner::BinaryOperation(y)) => {
                x.op == y.op && x.left.eq_with(&y.left, scope) && x.right.eq_with(&y.right, scope)
            }
            (ExprInner::UnaryOperation(x), ExprInner::UnaryOperation(y)) => {
                x.operator == y.operator && x.operand.eq_with(&y.operand, scope)
            }
            (ExprInner::Var(x), ExprInner::Var(y)) => x.eq_with(y, scope),
            (ExprInner::Literal(x), ExprInner::Literal(y)) => {
                x.value == y.value && x.quotes == y.quotes && x.kind == y.kind
            }
            (ExprInner::SubQuery(x), ExprInner::SubQuery(y)) => x.eq_with(y, scope),
            (ExprInner::Row(x), ExprInner::Row(y)) => x.eq_with(y, scope),
            (ExprInner::Array(x), ExprInner::Array(y)) => x.elems.eq_with(&y.elems, scope),
            (ExprInner::WindowFunction(x), ExprInner::WindowFunction(y)) => {
                State::window_fn_eq(x, y, scope)
            }
            (ExprInner::FunctionCall(x), ExprInner::FunctionCall(y)) => {
                x.name == y.name && x.args.eq_with(&y.args, scope)
            }
            (ExprInner::Parameter(x), ExprInner::Parameter(y)) => x == y,
            (ExprInner::Cast(x), ExprInner::Cast(y)) => {
                x.ty == y.ty
                    && (!State::ACCOUNT_CAST_SYNTAX || x.syntax == y.syntax)
                    && x.child.eq_with(&y.child, scope)
            }
            (ExprInner::Like(x), ExprInner::Like(y)) => {
                x.is_not == y.is_not
                    && x.is_ilike == y.is_ilike
                    && x.left.eq_with(&y.left, scope)
                    && x.right.eq_with(&y.right, scope)
                    && x.escape.eq_with(&y.escape, scope)
            }
            (ExprInner::Similar(x), ExprInner::Similar(y)) => {
                x.is_not == y.is_not
                    && x.left.eq_with(&y.left, scope)
                    && x.right.eq_with(&y.right, scope)
                    && x.escape.eq_with(&y.escape, scope)
            }
            (ExprInner::Between(x), ExprInner::Between(y)) => {
                x.is_not == y.is_not
                    && x.left.eq_with(&y.left, scope)
                    && x.center.eq_with(&y.center, scope)
                    && x.right.eq_with(&y.right, scope)
            }
            (ExprInner::In(x), ExprInner::In(y)) => {
                x.is_not == y.is_not
                    && x.left.eq_with(&y.left, scope)
                    && x.rhs.eq_with(&y.rhs, scope)
            }
            (ExprInner::Is(x), ExprInner::Is(y)) => {
                x.is_not == y.is_not && x.value == y.value && x.child.eq_with(&y.child, scope)
            }
            (ExprInner::Index(x), ExprInner::Index(y)) => {
                x.child.eq_with(&y.child, scope) && x.which.eq_with(&y.which, scope)
            }
            (ExprInner::Trim(x), ExprInner::Trim(y)) => {
                x.kind == y.kind
                    && x.pattern.eq_with(&y.pattern, scope)
                    && x.target.eq_with(&y.target, scope)
            }
            (ExprInner::Substring(x), ExprInner::Substring(y)) => x.eq_with(y, scope),
            (ExprInner::Case(x), ExprInner::Case(y)) => {
                x.search.eq_with(&y.search, scope)
                    && x.when_blocks.eq_with(&y.when_blocks, scope)
                    && x.else_expr.eq_with(&y.else_expr, scope)
            }
            (ExprInner::Exists(x), ExprInner::Exists(y)) => {
                x.is_not == y.is_not && x.subquery.eq_with(&y.subquery, scope)
            }
            (ExprInner::TimeFunction(x), ExprInner::TimeFunction(y)) => x == y,
            _mismatched_shapes => false,
        }
    }
}

/// A raw column reference is its written name; there is nothing to resolve.
impl StructuralEq<()> for RawVar {
    fn eq_with(&self, other: &Self, _scope: &mut ()) -> bool {
        self == other
    }
}

impl<'q, State: AstState<'q>> StructuralEq<State::EqScope> for ValuesRow<'q, State> {
    fn eq_with(&self, other: &Self, scope: &mut State::EqScope) -> bool {
        self.values.eq_with(&other.values, scope)
    }
}

impl<'q, State: AstState<'q>> StructuralEq<State::EqScope> for FunctionCallArgs<'q, State> {
    fn eq_with(&self, other: &Self, scope: &mut State::EqScope) -> bool {
        match (self, other) {
            (Self::CountAsterisk, Self::CountAsterisk) => true,
            (
                Self::Exprs {
                    distinct: x_distinct,
                    exprs: x_exprs,
                },
                Self::Exprs {
                    distinct: y_distinct,
                    exprs: y_exprs,
                },
            ) => x_distinct == y_distinct && x_exprs.eq_with(y_exprs, scope),
            _mismatched_shapes => false,
        }
    }
}

impl<'q, State: AstState<'q>> StructuralEq<State::EqScope> for Substring<'q, State> {
    fn eq_with(&self, other: &Self, scope: &mut State::EqScope) -> bool {
        match (self, other) {
            (Self::FromFor(x_target, x_start, x_len), Self::FromFor(y_target, y_start, y_len))
            | (Self::Regular(x_target, x_start, x_len), Self::Regular(y_target, y_start, y_len)) => {
                x_target.eq_with(y_target, scope)
                    && x_start.eq_with(y_start, scope)
                    && x_len.eq_with(y_len, scope)
            }
            (Self::For(x_target, x_len), Self::For(y_target, y_len))
            | (Self::From(x_target, x_len), Self::From(y_target, y_len)) => {
                x_target.eq_with(y_target, scope) && x_len.eq_with(y_len, scope)
            }
            (Self::Similar(x_expr), Self::Similar(y_expr)) => x_expr.eq_with(y_expr, scope),
            _mismatched_shapes => false,
        }
    }
}
