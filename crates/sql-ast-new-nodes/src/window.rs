//! Window functions and the window specifications they run over.
//!
//! # `WindowFunction<'q, State: AstState<'q>>`
//! A function call with an `OVER` clause.
//!
//! What follows `OVER` is a [`WindowRef`], one of these forms
//! * `OVER w` names a [`NamedWindow`] from the `WINDOW` clause of the same
//!   `SELECT` and uses it as written, frame included,
//! * `OVER (...)` is a [`WindowSpec`] written inline.
//!
//!
//! # Tree structure
//! A [`WindowSpec`] is an optional base window, partitioning, ordering and an
//! optional [`WindowFrame`].
//!
//! The frame narrows each partition to a moving slice around the current row,
//! either by row count or by value range ([`FrameType`]).
//!
//! Its [`FrameBound`]s can be
//! * unbounded in either direction,
//! * an offset from the current row,
//! * the current row itself.
//!
//!
//! # Base windows
//! A specification may start with the name of a base window: `w` in
//! `OVER (w ORDER BY a)`, or in `WINDOW v AS (w ORDER BY a)` where `w` must be
//! defined before `v`. The specification copies the base and may extend it:
//! * PARTITION BY is the base's; the copy cannot add one, even if the base has none,
//! * ORDER BY is the base's if it has one; otherwise the copy may add its own,
//! * the frame is the copy's own; a base with a frame cannot be copied.
//!
//! A base may itself be a copy; what it inherited counts as its own.
//!
//! So `OVER w` and `OVER (w)` differ: the first uses `w`, the second copies it
//! and fails if `w` has a frame. Neither is flattened, even in the
//! [`Analyzed`] tree: the copied clauses are found by name in the `WINDOW`
//! clause.

use std::fmt::{Display, Error, Formatter};

use crate::expr::Expr;
use crate::multiset::OrderBy;
use crate::{Analyzed, AstState, Ident, Raw};

/// Window function invocation: `name(args) [FILTER (WHERE expr)] OVER window`.
pub struct WindowFunction<'q, State: AstState<'q>> {
    pub name: Ident,
    pub args: WindowFunctionArgs<'q, State>,
    pub filter: Option<Expr<'q, State>>,
    pub window: WindowRef<'q, State>,
}

impl<'q, State: AstState<'q>> Display for WindowFunction<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}({})", self.name, self.args)?;
        if let Some(filter) = &self.filter {
            write!(f, " FILTER (WHERE {filter})")?;
        }
        write!(f, " OVER {}", self.window)
    }
}

impl<'q, State: AstState<'q>> WindowFunction<'q, State> {
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

pub enum WindowFunctionArgs<'q, State: AstState<'q>> {
    /// `count(*)`
    Asterisk,
    /// Regular argument list (empty for `row_number()`)
    List(Vec<Expr<'q, State>>),
}

impl<'q, State: AstState<'q>> Display for WindowFunctionArgs<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            WindowFunctionArgs::Asterisk => write!(f, "*"),
            WindowFunctionArgs::List(exprs) => {
                let Some((first, rest)) = exprs.split_first() else {
                    return Ok(());
                };
                write!(f, "{first}")?;
                for expr in rest {
                    write!(f, ", {expr}")?;
                }
                Ok(())
            }
        }
    }
}

pub enum WindowRef<'q, State: AstState<'q>> {
    /// Reference to a window from the `WINDOW` clause: `OVER w`
    Name(Ident),
    /// Inline window definition: `OVER (...)`
    Spec(WindowSpec<'q, State>),
}

impl<'q, State: AstState<'q>> Display for WindowRef<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            WindowRef::Name(name) => write!(f, "{name}"),
            WindowRef::Spec(spec) => write!(f, "({spec})"),
        }
    }
}

/// Window definition body:
/// `[base_window_name] [PARTITION BY ...] [ORDER BY ...] [frame]`.
#[derive(Default)]
pub struct WindowSpec<'q, State: AstState<'q>> {
    /// The window this one copies, see [base windows](crate::window#base-windows).
    pub base: Option<Ident>,
    pub partition_by: Vec<Expr<'q, State>>,
    pub order_by: Option<OrderBy<'q, State>>,
    pub frame: Option<WindowFrame<'q, State>>,
}

impl<'q, State: AstState<'q>> Display for WindowSpec<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let mut sep = "";
        if let Some(base) = self.base.as_ref() {
            write!(f, "{base}")?;
            sep = " ";
        }
        if let Some((first, rest)) = self.partition_by.split_first() {
            write!(f, "{sep}PARTITION BY {first}")?;
            for expr in rest {
                write!(f, ", {expr}")?;
            }
            sep = " ";
        }
        if let Some(order_by) = self.order_by.as_ref() {
            write!(f, "{sep}{order_by}")?;
            sep = " ";
        }
        if let Some(frame) = &self.frame {
            write!(f, "{sep}{frame}")?;
        }
        Ok(())
    }
}

pub struct WindowFrame<'q, State: AstState<'q>> {
    pub ty: FrameType,
    pub bounds: FrameBounds<'q, State>,
}

impl<'q, State: AstState<'q>> Display for WindowFrame<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{} {}", self.ty, self.bounds)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    Rows,
    Range,
}

impl Display for FrameType {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            FrameType::Rows => write!(f, "ROWS"),
            FrameType::Range => write!(f, "RANGE"),
        }
    }
}

pub enum FrameBounds<'q, State: AstState<'q>> {
    Single(FrameBound<'q, State>),
    Between(FrameBound<'q, State>, FrameBound<'q, State>),
}

impl<'q, State: AstState<'q>> Display for FrameBounds<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            FrameBounds::Single(bound) => write!(f, "{bound}"),
            FrameBounds::Between(from, to) => write!(f, "BETWEEN {from} AND {to}"),
        }
    }
}

pub enum FrameBound<'q, State: AstState<'q>> {
    UnboundedPreceding,
    Preceding(Expr<'q, State>),
    CurrentRow,
    Following(Expr<'q, State>),
    UnboundedFollowing,
}

impl<'q, State: AstState<'q>> Display for FrameBound<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            FrameBound::UnboundedPreceding => write!(f, "UNBOUNDED PRECEDING"),
            FrameBound::Preceding(offset) => write!(f, "{offset} PRECEDING"),
            FrameBound::CurrentRow => write!(f, "CURRENT ROW"),
            FrameBound::Following(offset) => write!(f, "{offset} FOLLOWING"),
            FrameBound::UnboundedFollowing => write!(f, "UNBOUNDED FOLLOWING"),
        }
    }
}

#[derive(Default)]
pub struct NamedWindows<'q, State: AstState<'q>>(pub Vec<NamedWindow<'q, State>>);

impl NamedWindows<'_, Raw> {
    pub fn empty() -> Self {
        Self(vec![])
    }
}

impl<'q, State: AstState<'q>> Display for NamedWindows<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        if let Some((first_window, other_windows)) = self.0.split_first() {
            write!(f, " WINDOW {first_window}")?;
            for window in other_windows {
                write!(f, ", {window}")?;
            }
        }
        Ok(())
    }
}

/// Single entry of the `WINDOW` clause: `name AS (window_definition)`.
pub struct NamedWindow<'q, State: AstState<'q>> {
    name: Ident,
    spec: WindowSpec<'q, State>,
}

impl<'q, State: AstState<'q>> NamedWindow<'q, State> {
    pub fn from_parts(name: Ident, spec: WindowSpec<'q, State>) -> Self {
        Self { name, spec }
    }
}

impl<'q> NamedWindow<'q, Raw> {
    pub fn into_parts(self) -> (Ident, WindowSpec<'q, Raw>) {
        (self.name, self.spec)
    }
}

impl<'q> NamedWindow<'q, Analyzed> {
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn spec_mut(&mut self) -> &mut WindowSpec<'q, Analyzed> {
        &mut self.spec
    }

    pub fn has_frame(&self) -> bool {
        self.spec.frame.is_some()
    }
}

impl<'q, State: AstState<'q>> Display for NamedWindow<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{} AS ({})", self.name, self.spec)
    }
}

/// Structural comparison, one impl per node for both states.
///
/// The two entries from the rest of the walk go through
/// [`AstState::window_fn_eq`] and [`AstState::named_window_eq`], which is
/// where [`Analyzed`]'s windows-never-equal rule lives; the
/// impls below are the [`Raw`] side of those hooks and the shared
/// recursion under them.
mod structural_eq {
    use std::iter::zip;

    use super::*;
    use crate::structural_eq::StructuralEq;

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for WindowFunction<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            self.name == other.name
                && self.args.eq_with(&other.args, scope)
                && self.filter.eq_with(&other.filter, scope)
                && self.window.eq_with(&other.window, scope)
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for WindowFunctionArgs<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            match (self, other) {
                (Self::Asterisk, Self::Asterisk) => true,
                (Self::List(x), Self::List(y)) => x.eq_with(y, scope),
                _mismatched_shapes => false,
            }
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for WindowRef<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            match (self, other) {
                (Self::Name(x), Self::Name(y)) => x == y,
                (Self::Spec(x), Self::Spec(y)) => x.eq_with(y, scope),
                _mismatched_shapes => false,
            }
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for WindowSpec<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            self.base == other.base
                && self.partition_by.eq_with(&other.partition_by, scope)
                && self.order_by.eq_with(&other.order_by, scope)
                && self.frame.eq_with(&other.frame, scope)
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for WindowFrame<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            self.ty == other.ty && self.bounds.eq_with(&other.bounds, scope)
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for FrameBounds<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            match (self, other) {
                (Self::Single(x), Self::Single(y)) => x.eq_with(y, scope),
                (Self::Between(x_from, x_to), Self::Between(y_from, y_to)) => {
                    x_from.eq_with(y_from, scope) && x_to.eq_with(y_to, scope)
                }
                _mismatched_shapes => false,
            }
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for FrameBound<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            match (self, other) {
                (Self::UnboundedPreceding, Self::UnboundedPreceding)
                | (Self::CurrentRow, Self::CurrentRow)
                | (Self::UnboundedFollowing, Self::UnboundedFollowing) => true,
                (Self::Preceding(x), Self::Preceding(y))
                | (Self::Following(x), Self::Following(y)) => x.eq_with(y, scope),
                _mismatched_shapes => false,
            }
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for NamedWindow<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            self.name == other.name && self.spec.eq_with(&other.spec, scope)
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for NamedWindows<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            self.0.len() == other.0.len()
                && zip(&self.0, &other.0).all(|(x, y)| S::named_window_eq(x, y, scope))
        }
    }
}
