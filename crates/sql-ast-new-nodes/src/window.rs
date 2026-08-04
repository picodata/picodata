//! Window functions and the window specifications they run over.
//!
//! # `WindowFunction<'q, State: AstState<'q>>`
//! A function call with an `OVER` clause.
//!
//! What follows `OVER` is a [`WindowRef`], one of these forms
//! * a specification written inline,
//! * the name of a [`NamedWindow`] declared in the enclosing `WINDOW` clause.
//!
//! Both end up as the same [`WindowSpec`], so the two spellings differ only in
//! where the specification is written down.
//!
//!
//! # Tree structure
//! A [`WindowSpec`] is partitioning, ordering and an optional [`WindowFrame`].
//!
//! The frame narrows each partition to a moving slice around the current row,
//! either by row count or by value range ([`FrameType`]).
//!
//! Its [`FrameBound`]s can be
//! * unbounded in either direction,
//! * an offset from the current row,
//! * the current row itself.

use std::fmt::{Display, Error, Formatter};

use crate::expr::Expr;
use crate::multiset::OrderByElement;
use crate::{AstState, Ident};

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
    pub base: Option<Ident>,
    pub partition_by: Vec<Expr<'q, State>>,
    pub order_by: Vec<OrderByElement<'q, State>>,
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
        if let Some((first, rest)) = self.order_by.split_first() {
            write!(f, "{sep}ORDER BY {first}")?;
            for elem in rest {
                write!(f, ", {elem}")?;
            }
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

/// Single entry of the `WINDOW` clause: `name AS (window_definition)`.
pub struct NamedWindow<'q, State: AstState<'q>> {
    pub name: Ident,
    pub spec: WindowSpec<'q, State>,
}

impl<'q, State: AstState<'q>> Display for NamedWindow<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{} AS ({})", self.name, self.spec)
    }
}
