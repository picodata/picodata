//! The trait structural comparison runs on, shared by both states.
//!
//! [`StructuralEq`] is implemented once per node, generically over the state.
//! The handful of places where [`Raw`](crate::Raw) and [`Analyzed`](crate::Analyzed)
//! genuinely disagree are hooks on [`AstState`](crate::AstState), so the walk itself -
//! `expr/structural_eq.rs` for expressions, a `structural_eq` module beside each
//! statement node - is written a single time.
//!
//! The `Scope` parameter is what a state threads through the comparison
//! ([`AstState::EqScope`](crate::AstState::EqScope)). [`Raw`](crate::Raw) needs
//! none. In [`Analyzed`](crate::Analyzed) a resolved column reference is a
//! position in an enclosing statement
//! ([`BoundVar`](crate::table_expression::BoundVar)), so the same subquery
//! written twice binds its references to equal values, and a correlated
//! reference to one query level differs from a reference to another by depth.
//! Written at two different depths, its references into its own subqueries
//! differ by that distance; the scope is the pair of depths
//! ([`EqDepths`](crate::scope::EqDepths)) that relates them.
//!
//! Entry points - [`Expr::structural_eq`](crate::expr::Expr::structural_eq) and the
//! [`PartialEq`] sugar over raw nodes - start from the default scope: both sides
//! written at the same depth. A caller comparing across depths calls
//! [`eq_with`](StructuralEq::eq_with) with its own
//! [`EqDepths`](crate::scope::EqDepths). Inside the walk every recursion must
//! go through [`eq_with`](StructuralEq::eq_with), never through `==`, so the
//! scope reaches the column references.

use crate::Ident;

/// Structural comparison of two nodes in the same state. See the module doc.
pub trait StructuralEq<Scope> {
    fn eq_with(&self, other: &Self, scope: &mut Scope) -> bool;
}

impl<Scope, T: StructuralEq<Scope>> StructuralEq<Scope> for Box<T> {
    fn eq_with(&self, other: &Self, scope: &mut Scope) -> bool {
        T::eq_with(self, other, scope)
    }
}

impl<Scope, T: StructuralEq<Scope>> StructuralEq<Scope> for Option<T> {
    fn eq_with(&self, other: &Self, scope: &mut Scope) -> bool {
        match (self, other) {
            (None, None) => true,
            (Some(left), Some(right)) => left.eq_with(right, scope),
            _mismatched_shapes => false,
        }
    }
}

impl<Scope, T: StructuralEq<Scope>> StructuralEq<Scope> for Vec<T> {
    fn eq_with(&self, other: &Self, scope: &mut Scope) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .zip(other)
                .all(|(left, right)| left.eq_with(right, scope))
    }
}

impl<Scope, A: StructuralEq<Scope>, B: StructuralEq<Scope>> StructuralEq<Scope> for (A, B) {
    fn eq_with(&self, other: &Self, scope: &mut Scope) -> bool {
        self.0.eq_with(&other.0, scope) && self.1.eq_with(&other.1, scope)
    }
}

/// A raw table (or CTE) name in a FROM clause.
impl StructuralEq<()> for Ident {
    fn eq_with(&self, other: &Self, _scope: &mut ()) -> bool {
        self == other
    }
}
