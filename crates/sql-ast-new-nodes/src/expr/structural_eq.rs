//! Structural comparison for raw expressions.
//!
//! It lives in this crate rather than next to the precedence tests that use it
//! because `PartialEq` is a foreign trait: implementing it for [`Expr`] is only
//! legal in the crate that declares [`Expr`].
//!
//! The module itself is private: a trait impl is visible wherever the crate is
//! linked, so callers need no import to write `a == b`.

use super::*;

/// Structural comparison for raw expressions.
/// Two deliberate choices:
///
/// * [`Cast::syntax`] participates: `CAST(a AS int)` and `a::int` are *different* trees.
///   [`Display`](std::fmt::Display) preserves the spelling, so render/re-parse comparisons may rely on it.
/// * Statement sub-trees (subquery bodies, window functions) are compared
///   through their [`Display`](std::fmt::Display) rendering:
///   statements have no structural comparison yet — this covers expressions only.
impl PartialEq for Expr<'_, Raw> {
    fn eq(&self, other: &Self) -> bool {
        structural_eq(self, other)
    }
}

/// The comparison behind [`Expr`]'s [`PartialEq`].
fn structural_eq(a: &Expr<'_, Raw>, b: &Expr<'_, Raw>) -> bool {
    // `Raw` per-node metadata is empty, so the syntactic payload is the whole comparison.
    // Each arm pairs a variant with itself; anything left over is a shape mismatch.
    match (a.inner_ref(), b.inner_ref()) {
        (ExprInner::Nil, ExprInner::Nil) => true,
        (ExprInner::BinaryOperation(x), ExprInner::BinaryOperation(y)) => {
            x.op == y.op && structural_eq(&x.left, &y.left) && structural_eq(&x.right, &y.right)
        }
        (ExprInner::UnaryOperation(x), ExprInner::UnaryOperation(y)) => {
            x.operator == y.operator && structural_eq(&x.operand, &y.operand)
        }
        (ExprInner::ColumnRef(x), ExprInner::ColumnRef(y)) => x == y,
        (ExprInner::Literal(x), ExprInner::Literal(y)) => {
            x.value == y.value && x.quotes == y.quotes && x.kind == y.kind
        }
        (ExprInner::SubQuery(x), ExprInner::SubQuery(y)) => x.to_string() == y.to_string(),
        (ExprInner::Row(x), ExprInner::Row(y)) => all_structural_eq(&x.values, &y.values),
        (ExprInner::Array(x), ExprInner::Array(y)) => all_structural_eq(&x.elems, &y.elems),
        (ExprInner::WindowFunction(x), ExprInner::WindowFunction(y)) => {
            x.to_string() == y.to_string()
        }
        (
            ExprInner::FunctionCall(FunctionCall {
                name: x_name,
                args: FunctionCallArgs::CountAsterisk,
            }),
            ExprInner::FunctionCall(FunctionCall {
                name: y_name,
                args: FunctionCallArgs::CountAsterisk,
            }),
        ) => x_name == y_name,
        (
            ExprInner::FunctionCall(FunctionCall {
                name: x_name,
                args:
                    FunctionCallArgs::Exprs {
                        distinct: x_distinct,
                        exprs: x_exprs,
                    },
            }),
            ExprInner::FunctionCall(FunctionCall {
                name: y_name,
                args:
                    FunctionCallArgs::Exprs {
                        distinct: y_distinct,
                        exprs: y_exprs,
                    },
            }),
        ) => x_name == y_name && x_distinct == y_distinct && all_structural_eq(x_exprs, y_exprs),
        (ExprInner::Parameter(x), ExprInner::Parameter(y)) => x == y,
        (ExprInner::Cast(x), ExprInner::Cast(y)) => {
            x.ty == y.ty && x.syntax == y.syntax && structural_eq(&x.child, &y.child)
        }
        (ExprInner::Like(x), ExprInner::Like(y)) => {
            x.is_not == y.is_not
                && x.is_ilike == y.is_ilike
                && structural_eq(&x.left, &y.left)
                && structural_eq(&x.right, &y.right)
                && opt_structural_eq(x.escape.as_deref(), y.escape.as_deref())
        }
        (ExprInner::Similar(x), ExprInner::Similar(y)) => {
            x.is_not == y.is_not
                && structural_eq(&x.left, &y.left)
                && structural_eq(&x.right, &y.right)
                && opt_structural_eq(x.escape.as_deref(), y.escape.as_deref())
        }
        (ExprInner::Between(x), ExprInner::Between(y)) => {
            x.is_not == y.is_not
                && structural_eq(&x.left, &y.left)
                && structural_eq(&x.center, &y.center)
                && structural_eq(&x.right, &y.right)
        }
        (ExprInner::In(x), ExprInner::In(y)) => {
            x.is_not == y.is_not && structural_eq(&x.left, &y.left) && structural_eq(&x.rhs, &y.rhs)
        }
        (ExprInner::Is(x), ExprInner::Is(y)) => {
            x.is_not == y.is_not && x.value == y.value && structural_eq(&x.child, &y.child)
        }
        (ExprInner::Index(x), ExprInner::Index(y)) => {
            structural_eq(&x.child, &y.child) && structural_eq(&x.which, &y.which)
        }
        (ExprInner::Trim(x), ExprInner::Trim(y)) => {
            x.kind == y.kind
                && opt_structural_eq(x.pattern.as_deref(), y.pattern.as_deref())
                && structural_eq(&x.target, &y.target)
        }
        (
            ExprInner::Substring(Substring::FromFor(x_target, x_start, x_len)),
            ExprInner::Substring(Substring::FromFor(y_target, y_start, y_len)),
        )
        | (
            ExprInner::Substring(Substring::Regular(x_target, x_start, x_len)),
            ExprInner::Substring(Substring::Regular(y_target, y_start, y_len)),
        ) => {
            structural_eq(x_target, y_target)
                && structural_eq(x_start, y_start)
                && structural_eq(x_len, y_len)
        }
        (
            ExprInner::Substring(Substring::For(x_target, x_len)),
            ExprInner::Substring(Substring::For(y_target, y_len)),
        )
        | (
            ExprInner::Substring(Substring::From(x_target, x_len)),
            ExprInner::Substring(Substring::From(y_target, y_len)),
        ) => structural_eq(x_target, y_target) && structural_eq(x_len, y_len),
        (
            ExprInner::Substring(Substring::Similar(x_expr)),
            ExprInner::Substring(Substring::Similar(y_expr)),
        ) => structural_eq(x_expr, y_expr),
        (ExprInner::Case(x), ExprInner::Case(y)) => {
            opt_structural_eq(x.search.as_deref(), y.search.as_deref())
                && x.when_blocks.len() == y.when_blocks.len()
                && x.when_blocks.iter().zip(&y.when_blocks).all(
                    |((x_condition, x_result), (y_condition, y_result))| {
                        structural_eq(x_condition, y_condition) && structural_eq(x_result, y_result)
                    },
                )
                && opt_structural_eq(x.else_expr.as_deref(), y.else_expr.as_deref())
        }
        (ExprInner::Exists(x), ExprInner::Exists(y)) => {
            x.is_not == y.is_not && x.subquery.to_string() == y.subquery.to_string()
        }
        (ExprInner::TimeFunction(x), ExprInner::TimeFunction(y)) => x == y,
        _mismatched_shapes => false,
    }
}

fn opt_structural_eq(a: Option<&Expr<'_, Raw>>, b: Option<&Expr<'_, Raw>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => structural_eq(a, b),
        _ => false,
    }
}

fn all_structural_eq(a: &[Expr<'_, Raw>], b: &[Expr<'_, Raw>]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(a, b)| structural_eq(a, b))
}
