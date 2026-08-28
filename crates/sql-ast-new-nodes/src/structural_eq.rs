//! The trait structural comparison runs on, shared by both states.
//!
//! [`StructuralEq`] is implemented once per node, generically over the state.
//! The handful of places where [`Raw`](crate::Raw) and [`Analyzed`](crate::Analyzed)
//! genuinely disagree are hooks on [`AstState`](crate::AstState), so the walk itself -
//! `expr/structural_eq.rs` for expressions, a `structural_eq` module beside each
//! statement node - is written a single time.
//!
//! The `Scope` parameter is the correspondence between the relations the two
//! compared subtrees introduce. Binding mints a fresh
//! [`TableFactor`](crate::table_expression::TableFactor) per relation, so the same
//! subquery written twice owns two of them, and its own column references would
//! compare unequal by identity. Comparing such a pair therefore records which
//! relations correspond ([`AstState::pair_relations`](crate::AstState::pair_relations));
//! references to anything outside them stay identity-compared, which is what tells
//! a correlated reference to one query level from a reference to another.
//! [`Raw`](crate::Raw) resolves nothing and its scope is `()`.
//!
//! Entry points - [`Expr::structural_eq`](crate::expr::Expr::structural_eq) and the
//! [`PartialEq`] sugar over raw nodes - start from an empty scope. Inside the walk
//! every recursion must go through [`eq_with`](StructuralEq::eq_with), never through
//! `==`, so the scope reaches the column references.

use std::rc::Rc;

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

impl<Scope, T: StructuralEq<Scope>> StructuralEq<Scope> for Rc<T> {
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
