//! The stack of statements a reader of the analyzed tree is inside of.
//!
//! An analyzed reference names what it resolved to by *position* in an
//! enclosing statement, never by pointer: a column reference ([`BoundVar`]) is
//! `(depth, entry, column)` into the `FROM` clause of the statement at `depth`,
//! a CTE reference ([`CteRef`]) is `(depth, offset)` into the `WITH` clause of
//! the statement at `depth`. Reading through one therefore takes the
//! statements between the root and the reader, one [`FrameView`] each, which
//! every walk over the analyzed tree keeps as it descends: rendering (see
//! [`rendering`](crate::rendering)), and the analyzer while it binds. That
//! stack is a [`Scope`].
//!
//! Depth is absolute — the root statement is 0, every nested
//! [`MultisetStmt`](crate::multiset::MultisetStmt) one more — rather than
//! PostgreSQL's "levels up". An expression bound inside a subquery is compared
//! against the grouping keys of *every* enclosing level, and with absolute
//! depths a reference to an outer column is the same value wherever it is
//! written. A reference into a subquery of the compared expression is not:
//! it moves with the expression, which is what [`EqDepths`] accounts for.

use crate::multiset::{Cte, Ctes};
use crate::table_expression::{AttributeView, BoundVar, From, FromEntry};
use crate::{Analyzed, NamedEntity};

/// Index of a statement in the stack of enclosing statements; the root is 0.
pub type Depth = usize;

/// A CTE, by position: the `offset`-th entry of the `WITH` clause of the
/// statement at `depth`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct CteRef {
    pub depth: Depth,
    pub offset: usize,
}

/// The depths the two sides of an [`Analyzed`] structural comparison are
/// written at - its [`EqScope`](crate::AstState::EqScope).
///
/// A reference to a statement at or above its side's depth is an outer
/// reference and compares by absolute depth. A deeper one points into a
/// subquery of the compared expression and compares by its distance below
/// that depth, so `GROUP BY (SELECT … FROM t3)` matches the same subquery
/// written one level deeper (PostgreSQL 19 does the same with
/// `IncrementVarSublevelsUp`). Equal depths make this plain equality.
#[derive(Clone, Copy, Default, Debug)]
pub struct EqDepths {
    pub left: Depth,
    pub right: Depth,
}

impl EqDepths {
    /// Whether depth `left` on the left side and `right` on the right side
    /// name the same statement.
    pub fn same(&self, left: Depth, right: Depth) -> bool {
        let rebase = |depth: Depth, base: Depth| match depth.checked_sub(base) {
            Some(below) if below > 0 => (true, below),
            _outer => (false, depth),
        };
        rebase(left, self.left) == rebase(right, self.right)
    }
}

/// What one statement contributes to the scope of everything nested in it:
/// its `WITH` clause, and the `FROM` clause of its `SELECT` body if it has one.
#[derive(Clone, Copy)]
pub struct FrameView<'a, 'q> {
    pub ctes: &'a Ctes<'q, Analyzed>,
    /// [`None`] for a `VALUES` statement and for a set operation, which have
    /// no `FROM` of their own.
    pub from: Option<&'a From<'q, Analyzed>>,
}

/// The statements a reader is inside of, by depth. Everything a positional
/// reference stands for is read through here.
pub trait Scope<'q> {
    /// The statement at `depth`, if the reader is inside one that deep.
    fn frame<'a>(&'a self, depth: Depth) -> Option<FrameView<'a, 'q>>
    where
        'q: 'a;

    fn cte<'a>(&'a self, cte: CteRef) -> Option<&'a Cte<'q, Analyzed>>
    where
        'q: 'a,
    {
        self.frame(cte.depth)?.ctes.ctes_ref().get(cte.offset)
    }

    /// The `FROM` entry a column reference resolved to.
    fn entry<'a>(&'a self, var: BoundVar) -> Option<&'a FromEntry<'q, Analyzed>>
    where
        'q: 'a,
    {
        self.frame(var.depth)?.from?.entries().get(var.entry)
    }

    /// The column a reference resolved to: its name and type.
    fn attribute<'a>(&'a self, var: BoundVar) -> Option<AttributeView<'q, 'a>>
    where
        'q: 'a,
    {
        self.entry(var)?.attribute(var.column_pos, self)
    }

    /// The name of the relation a column reference resolved to, as it is
    /// referred to in the query: the alias when there is one, else the table
    /// or CTE name; [`None`] for an unaliased subquery.
    fn source_name<'a>(&'a self, var: BoundVar) -> Option<&'a str>
    where
        'q: 'a,
    {
        self.entry(var)?.name()
    }

    /// The whole stack as the rendering scope of [`Analyzed`]: what
    /// [`Rendered`](crate::rendering::Rendered) takes to print a node
    /// below the root.
    fn render_scope<'a>(&'a self) -> Vec<FrameView<'a, 'q>>
    where
        'q: 'a,
    {
        (0..).map_while(|depth| self.frame(depth)).collect()
    }
}

impl<'q> Scope<'q> for Vec<FrameView<'_, 'q>> {
    fn frame<'a>(&'a self, depth: Depth) -> Option<FrameView<'a, 'q>>
    where
        'q: 'a,
    {
        self.get(depth).copied()
    }
}
