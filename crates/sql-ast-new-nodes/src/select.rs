//! A single `SELECT`: what to project, and over what.
//!
//! # `SelectStmt<'q, State: AstState<'q>>`
//! The pairing of those two halves: a [`SelectList`] and an optional [`TableExpression`].
//! The table expression is optional because queries with no source of rows at all are valid.
//!
//!
//! # Tree structure
//! [`SelectList`] carries the `DISTINCT`/`ALL` duplicate treatment and the projected elements.
//!
//! An element ([`SelectListElem`]) is one of these forms
//! * the bare `*`, or a qualified `t.*`,
//! * [`ProjectionExpr`], an expression with an optional alias.

use std::fmt::{Display, Error, Formatter};

use crate::expr::{Expr, ExprInner};
use crate::table_expression::{AttributeView, SingleEntryColumnRoute, TableExpression};
use crate::{Analyzed, AstState, Ident, Raw};
use sql_ir::ir::types::DerivedType;

pub struct SelectStmt<'q, State: AstState<'q>> {
    select_list: SelectList<'q, State>,
    table_expression: Option<TableExpression<'q, State>>,
}

impl<'q, State: AstState<'q>> SelectStmt<'q, State> {
    pub fn into_parts(self) -> (SelectList<'q, State>, Option<TableExpression<'q, State>>) {
        (self.select_list, self.table_expression)
    }

    pub fn from_parts(
        select_list: SelectList<'q, State>,
        table_expression: Option<TableExpression<'q, State>>,
    ) -> Self {
        Self {
            select_list,
            table_expression,
        }
    }
}

impl SelectStmt<'_, Raw> {
    pub fn has_asterisk(&self) -> bool {
        self.select_list
            .elements
            .0
            .iter()
            .any(|elem| matches!(elem, SelectListElem::Asterisk(_)))
    }

    pub fn has_table_expression(&self) -> bool {
        self.table_expression.is_some()
    }
}

impl<'q> SelectStmt<'q, Analyzed> {
    /// Projected column types in select-list order.
    pub(crate) fn result_types(&self) -> Vec<DerivedType> {
        self.select_list
            .elements
            .0
            .iter()
            .map(ProjectionExpr::<'q, Analyzed>::expr_ref)
            .map(Expr::<'q, Analyzed>::data_type)
            .collect::<Vec<_>>()
    }

    pub(crate) fn result_columns_cnt(&self) -> usize {
        self.select_list.elements.0.len()
    }

    /// The `pos`-th output column viewed as a result attribute.
    /// This is the shape in which CTEs and FROM-subqueries expose
    /// their projection to column resolution and typing.
    pub(crate) fn attribute(&self, pos: usize) -> Option<AttributeView<'q, '_>> {
        self.select_list
            .elements
            .0
            .get(pos)
            .map(|proj_expr| AttributeView::Expr(proj_expr, proj_expr.output_name()))
    }

    /// Position of the output column visible as `column_name`.
    /// `column_name` if an explicit alias or inherited name,
    /// see [`ProjectionExpr::output_name`].
    pub(crate) fn column_route(
        &self,
        column_name: &str,
        exclude_positions: Option<Vec<usize>>,
    ) -> SingleEntryColumnRoute<usize> {
        let routes = self
            .select_list
            .elements
            .0
            .iter()
            .enumerate()
            .filter(|(pos, _)| {
                !exclude_positions
                    .as_ref()
                    .is_some_and(|positions| positions.contains(pos))
            })
            .filter_map(|(pos, proj_expr)| {
                proj_expr
                    .output_name()
                    .filter(|name| *name == column_name)
                    .map(|_| SingleEntryColumnRoute::Resolved(pos))
            })
            .collect::<Vec<_>>();

        if routes.len() > 1 {
            // More than 1 entity match.
            // For example, `SELECT a FROM (SELECT 1 a, 2 b)`.
            // Column reference a is ambigious.
            SingleEntryColumnRoute::Ambigious
        } else {
            routes
                .into_iter()
                .next()
                .unwrap_or(SingleEntryColumnRoute::ColumnMissing)
        }
    }
}

impl<'q, State: AstState<'q>> Display for SelectStmt<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.select_list)?;
        self.table_expression
            .as_ref()
            .map_or(Ok(()), |table_expression| write!(f, " {table_expression}"))
    }
}

pub struct SelectList<'q, State: AstState<'q>> {
    elements: SelectListExprs<'q, State>,
    is_distinct: bool,
}

impl<'q, State: AstState<'q>> SelectList<'q, State> {
    pub fn into_parts(self) -> (SelectListExprs<'q, State>, bool) {
        (self.elements, self.is_distinct)
    }

    pub fn from_parts(elements: SelectListExprs<'q, State>, is_distinct: bool) -> Self {
        Self {
            elements,
            is_distinct,
        }
    }
}

impl<'q, State: AstState<'q>> Display for SelectList<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        if self.is_distinct {
            write!(f, "SELECT DISTINCT ")?;
        } else {
            write!(f, "SELECT ")?;
        }
        write!(f, "{}", self.elements)?;
        Ok(())
    }
}

#[derive(Default)]
pub struct SelectListExprs<'q, State: AstState<'q>>(pub Vec<State::SelectListElemT>);

impl<'q> SelectListExprs<'q, Raw> {
    pub fn into(self) -> Vec<SelectListElem<'q>> {
        self.0
    }
}

impl<'q> SelectListExprs<'q, Analyzed> {
    pub fn from(elems: Vec<ProjectionExpr<'q, Analyzed>>) -> Self {
        Self(elems)
    }
}

impl<'q, State: AstState<'q>> Display for SelectListExprs<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        // Parsing guarantees a non-empty select list.
        let Some((first, rest)) = self.0.split_first() else {
            return Ok(());
        };
        write!(f, "{first}")?;
        for elem in rest {
            write!(f, ", {elem}")?;
        }
        Ok(())
    }
}

pub enum SelectListElem<'q> {
    Asterisk(Option<Ident>),
    Expr(ProjectionExpr<'q, Raw>),
}

impl Display for SelectListElem<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            SelectListElem::Asterisk(Some(specifier)) => write!(f, "{specifier}.*"),
            SelectListElem::Asterisk(None) => write!(f, "*"),
            SelectListElem::Expr(expr) => write!(f, "{}", expr),
        }
    }
}

pub struct ProjectionExpr<'q, State: AstState<'q>> {
    expr: Expr<'q, State>,
    /// Set by the parser after the element is built: the trailing alias is a
    /// separate grammar rule, attached to the projection parsed just before it.
    pub alias: Option<Ident>,
}

impl<'q, State: AstState<'q>> Display for ProjectionExpr<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.expr)?;
        self.alias
            .as_ref()
            .map_or(Ok(()), |alias| write!(f, " AS {alias}"))
    }
}

impl<'q, State: AstState<'q>> ProjectionExpr<'q, State> {
    pub fn new(expr: Expr<'q, State>, alias: Option<Ident>) -> Self {
        Self { expr, alias }
    }
}

impl<'q> ProjectionExpr<'q, Raw> {
    pub fn into_parts(self) -> (Expr<'q, Raw>, Option<Ident>) {
        (self.expr, self.alias)
    }
}

impl<'q> ProjectionExpr<'q, Analyzed> {
    pub fn from_parts(expr: Expr<'q, Analyzed>, alias: Option<Ident>) -> Self {
        Self { expr, alias }
    }

    /// Output column name: the explicit alias or, for a bare column
    /// reference, the name of the referenced column (as PostgreSQL derives it).
    pub(crate) fn output_name(&self) -> Option<&str> {
        self.alias.as_ref().map_or_else(
            || match self.expr.inner_ref() {
                ExprInner::ColumnRef(col_ref) => col_ref.column_name(),
                _ => None,
            },
            |alias| Some(alias.as_str()),
        )
    }

    pub(crate) fn data_type(&self) -> DerivedType {
        self.expr.data_type()
    }
}

impl<'q> ProjectionExpr<'q, Analyzed> {
    pub(crate) fn expr_ref(&self) -> &Expr<'q, Analyzed> {
        &self.expr
    }
}
