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

use std::collections::HashSet;
use std::fmt::{Error, Formatter};

use crate::expr::{Expr, ExprInner};
use crate::rendering::{raw_display, Render};
use crate::table_expression::{
    AttributeView, BoundVar, From, GroupBy, TableExpression, UniqueColumnRoute,
};
use crate::window::NamedWindows;
use crate::{Analyzed, AnalyzedExprMeta, AstNodeId, AstState, Ident, NamedEntity, Raw};
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

    pub fn set_sel_lst(&mut self, lst: SelectList<'q, Analyzed>) {
        self.select_list = lst;
    }

    pub fn set_from(&mut self, from: From<'q, Analyzed>) {
        if let Some(tbl_expr) = self.table_expression.as_mut() {
            tbl_expr.set_from(from);
        }
    }

    pub fn set_grby(&mut self, grby: GroupBy<'q, Analyzed>) {
        if let Some(tbl_expr) = self.table_expression.as_mut() {
            tbl_expr.set_grby(grby);
        }
    }

    pub fn set_windows(&mut self, windows: NamedWindows<'q, Analyzed>) {
        if let Some(tbl_expr) = self.table_expression.as_mut() {
            tbl_expr.set_windows(windows);
        }
    }

    /// Position of the output column visible as `column_name`.
    /// `column_name` if an explicit alias or inherited name,
    /// see [`ProjectionExpr::output_name`].
    pub(crate) fn column_route(
        &self,
        column_name: &str,
        exclude_positions: Option<&[usize]>,
    ) -> UniqueColumnRoute<usize> {
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
                    .map(|_| UniqueColumnRoute::Resolved(pos))
            })
            .collect::<Vec<_>>();

        if routes.len() > 1 {
            // More than 1 entity match.
            // For example, `SELECT a FROM (SELECT 1 a, 2 a)`.
            // Column reference a is ambiguous.
            UniqueColumnRoute::Ambiguous
        } else {
            routes
                .into_iter()
                .next()
                .unwrap_or(UniqueColumnRoute::ColumnMissing)
        }
    }

    pub fn parts_mut(
        &mut self,
    ) -> (
        &mut SelectList<'q, Analyzed>,
        Option<&mut TableExpression<'q, Analyzed>>,
    ) {
        (&mut self.select_list, self.table_expression.as_mut())
    }

    pub fn has_group_by(&self) -> bool {
        self.table_expression
            .as_ref()
            .is_some_and(|tbl_expr| tbl_expr.has_group_by())
    }

    pub fn has_having(&self) -> bool {
        self.table_expression
            .as_ref()
            .is_some_and(|tbl_expr| tbl_expr.has_having())
    }

    pub fn grouping_vars(&self, select_list: &SelectListExprs<'q, Analyzed>) -> HashSet<BoundVar> {
        self.table_expression
            .as_ref()
            .map_or_else(HashSet::new, |tbl_expr| tbl_expr.grouping_vars(select_list))
    }

    pub fn select_list(&self) -> &SelectListExprs<'q, Analyzed> {
        &self.select_list.elements
    }

    /// The `FROM` clause, if the statement has a table expression.
    pub fn from_ref(&self) -> Option<&From<'q, Analyzed>> {
        self.table_expression
            .as_ref()
            .map(TableExpression::from_ref)
    }
}

impl<'q, State: AstState<'q>> Render<'q, State> for SelectStmt<'q, State> {
    fn render<'a>(
        &'a self,
        f: &mut Formatter<'_>,
        scope: &mut State::RenderScope<'a>,
    ) -> Result<(), Error>
    where
        'q: 'a,
    {
        self.select_list.render(f, scope)?;
        if let Some(table_expression) = &self.table_expression {
            f.write_str(" ")?;
            table_expression.render(f, scope)?;
        }
        Ok(())
    }
}

raw_display!(SelectStmt);

#[derive(Default)]
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

impl SelectList<'_, Raw> {
    /// Position of the output column named `name` in the list as written -
    /// before any asterisk is expanded, so an asterisk counts as one element
    /// and never matches. [`None`] when no element carries the name and
    /// `Some(None)` when it is ambiguous.
    ///
    /// The raw twin of [`SelectList::<Analyzed>::output_name_pos`], for the
    /// one lookup that runs before the list is bound: a bare GROUP BY name
    /// that no local relation exposes. It matches the same output names -
    /// the explicit alias or, without one, the name the expression suggests
    /// on its own (see [`ProjectionExpr::output_name`]) - and calls several
    /// matches ambiguous only when they are not the same expression, so
    /// `SELECT a AS x, a AS x ... GROUP BY x` resolves while
    /// `SELECT a AS x, b AS x ... GROUP BY x` is ambiguous.
    pub fn output_name_pos(&self, name: &str) -> Option<Option<usize>> {
        let mut matches = self
            .elements
            .0
            .iter()
            .enumerate()
            .filter_map(|(idx, elem)| match elem {
                SelectListElem::Expr(proj_expr) => Some((idx, proj_expr)),
                SelectListElem::Asterisk(_) => None,
            })
            .filter(|(_, proj_expr)| proj_expr.output_name().is_some_and(|out| out == name));

        let (first_pos, first) = matches.next()?;
        for (_, other) in matches {
            if first.expr_ref() != other.expr_ref() {
                return Some(None);
            }
        }
        Some(Some(first_pos))
    }
}

impl<'q> SelectList<'q, Analyzed> {
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.elements.0.len()
    }

    pub fn exprs_mut(&mut self) -> &mut SelectListExprs<'q, Analyzed> {
        &mut self.elements
    }

    /// The expression projected at `pos`, counting from 0 - the numbering
    /// `GroupByElem::Ordinal` uses, which is over the *expanded* list, after any asterisk.
    pub fn expr_at(&self, pos: usize) -> Option<&Expr<'q, Analyzed>> {
        self.elements.0.get(pos).map(ProjectionExpr::expr_ref)
    }

    /// Position of the output column named `name`, [`None`] when no output
    /// column carries it and `Some(None)` when the name is ambiguous.
    ///
    /// The name is matched against every element's
    /// *output name* - its explicit alias when it has one, otherwise the name
    /// the expression exposes on its own - and several matches are an error
    /// only when they are not the same expression, so
    /// `SELECT a AS x, a AS x ... ORDER BY x` resolves while
    /// `SELECT a AS x, b AS x ... ORDER BY x` is ambiguous.
    pub fn output_name_pos(&self, name: &str) -> Option<Option<usize>> {
        let mut matches = self
            .elements
            .0
            .iter()
            .enumerate()
            .filter(|(_, proj_expr)| proj_expr.output_name().is_some_and(|out| out == name));

        let (first_pos, first) = matches.next()?;
        for (_, other) in matches {
            if !first.expr_ref().structural_eq(other.expr_ref()) {
                return Some(None);
            }
        }
        Some(Some(first_pos))
    }

    pub fn is_distinct(&self) -> bool {
        self.is_distinct
    }

    /// Whether `expr` is one of the projected expressions, by the same
    /// comparison a grouping key uses.
    pub fn projects(&self, expr: &Expr<'q, Analyzed>) -> bool {
        self.elements.projects(expr)
    }
}

impl<'q, State: AstState<'q>> Render<'q, State> for SelectList<'q, State> {
    fn render<'a>(
        &'a self,
        f: &mut Formatter<'_>,
        scope: &mut State::RenderScope<'a>,
    ) -> Result<(), Error>
    where
        'q: 'a,
    {
        if self.is_distinct {
            write!(f, "SELECT DISTINCT ")?;
        } else {
            write!(f, "SELECT ")?;
        }
        self.elements.render(f, scope)
    }
}

pub struct SelectListExprs<'q, State: AstState<'q>>(pub Vec<State::SelectListElemT>);

impl<'q> SelectListExprs<'q, Analyzed> {
    pub fn from(elems: Vec<ProjectionExpr<'q, Analyzed>>) -> Self {
        Self(elems)
    }

    /// Var behind the `pos`-th select list projection, counting from 1 -
    /// the numbering GROUP BY positions use.
    pub(crate) fn var_ref(&self, pos: usize) -> Option<&BoundVar> {
        self.0.get(pos).and_then(ProjectionExpr::var_ref)
    }

    /// Whether `expr` is one of the projected expressions, by the same comparison a grouping key uses.
    pub fn projects(&self, expr: &Expr<'q, Analyzed>) -> bool {
        self.0
            .iter()
            .any(|proj_expr| proj_expr.expr_ref().structural_eq(expr))
    }

    /// The name every projection exposes, in order. See [`ProjectionExpr::output_name`].
    pub fn output_names(&self) -> Vec<Option<&str>> {
        self.0
            .iter()
            .map(ProjectionExpr::<Analyzed>::output_name)
            .collect()
    }
}

impl<'q, State: AstState<'q>> Default for SelectListExprs<'q, State> {
    fn default() -> Self {
        Self(vec![])
    }
}

impl<'q, State: AstState<'q>> Render<'q, State> for SelectListExprs<'q, State> {
    fn render<'a>(
        &'a self,
        f: &mut Formatter<'_>,
        scope: &mut State::RenderScope<'a>,
    ) -> Result<(), Error>
    where
        'q: 'a,
    {
        // Parsing guarantees a non-empty select list.
        let Some((first, rest)) = self.0.split_first() else {
            return Ok(());
        };
        first.render(f, scope)?;
        for elem in rest {
            f.write_str(", ")?;
            elem.render(f, scope)?;
        }
        Ok(())
    }
}

pub enum SelectListElem<'q> {
    Asterisk(Option<Ident>),
    Expr(ProjectionExpr<'q, Raw>),
}

impl<'q> Render<'q, Raw> for SelectListElem<'q> {
    fn render<'a>(&'a self, f: &mut Formatter<'_>, scope: &mut ()) -> Result<(), Error>
    where
        'q: 'a,
    {
        match self {
            SelectListElem::Asterisk(Some(specifier)) => write!(f, "{specifier}.*"),
            SelectListElem::Asterisk(None) => write!(f, "*"),
            SelectListElem::Expr(expr) => expr.render(f, scope),
        }
    }
}

pub struct ProjectionExpr<'q, State: AstState<'q>> {
    expr: Expr<'q, State>,
    /// Set by the parser after the element is built: the trailing alias is a
    /// separate grammar rule, attached to the projection parsed just before it.
    pub alias: Option<Ident>,
    /// The name the expression suggests on its own, derived by the analyzer;
    /// see [`AstState::ProjNameT`].
    inferred_name: State::ProjNameT,
}

impl<'q, State: AstState<'q>> Render<'q, State> for ProjectionExpr<'q, State> {
    fn render<'a>(
        &'a self,
        f: &mut Formatter<'_>,
        scope: &mut State::RenderScope<'a>,
    ) -> Result<(), Error>
    where
        'q: 'a,
    {
        self.expr.render(f, scope)?;
        self.alias
            .as_ref()
            .map_or(Ok(()), |alias| write!(f, " AS {alias}"))
    }
}

impl<'q, State: AstState<'q>> ProjectionExpr<'q, State> {
    pub fn new(expr: Expr<'q, State>, alias: Option<Ident>) -> Self {
        Self {
            expr,
            alias,
            inferred_name: State::ProjNameT::default(),
        }
    }

    pub fn expr_ref(&self) -> &Expr<'q, State> {
        &self.expr
    }
}

impl<'q> ProjectionExpr<'q, Raw> {
    pub fn into_parts(self) -> (Expr<'q, Raw>, Option<Ident>) {
        (self.expr, self.alias)
    }

    /// The name this projection exposes: its explicit alias, or the name the
    /// expression suggests on its own (see [`NamedEntity`] for [`Expr`]).
    pub fn output_name(&self) -> Option<&str> {
        match self.alias.as_ref() {
            Some(alias) => Some(alias.as_str()),
            None => self.expr.name(),
        }
    }
}

impl<'q> ProjectionExpr<'q, Analyzed> {
    /// `inferred_name` is what the expression suggests on its own, consulted
    /// when there is no alias; the analyzer derives it while the scope the
    /// expression was bound in is still at hand.
    pub fn from_parts(
        expr: Expr<'q, Analyzed>,
        alias: Option<Ident>,
        inferred_name: Option<Ident>,
    ) -> Self {
        Self {
            expr,
            alias,
            inferred_name,
        }
    }

    pub fn from_bound_var(var: BoundVar, expr_id: AstNodeId, name: Option<Ident>) -> Self {
        ProjectionExpr::from_parts(
            Expr::from_parts(
                ExprInner::<'q, Analyzed>::Var(var),
                AnalyzedExprMeta::new(DerivedType::unknown(), expr_id),
            ),
            None,
            name,
        )
    }

    /// The name this projection exposes: its explicit alias, or the name the
    /// expression itself suggests (for a bare column reference, the name of
    /// the referenced column).
    pub fn output_name(&self) -> Option<&str> {
        self.alias
            .as_ref()
            .or(self.inferred_name.as_ref())
            .map(Ident::as_str)
    }

    pub(crate) fn data_type(&self) -> DerivedType {
        self.expr.data_type()
    }

    pub fn expr_mut(&mut self) -> &mut Expr<'q, Analyzed> {
        &mut self.expr
    }

    pub fn var_ref(&self) -> Option<&BoundVar> {
        self.expr_ref().var_ref()
    }

    /// See [`Expr::transient_var_ref`].
    pub fn transient_var_ref(&self) -> Option<&BoundVar> {
        self.expr_ref().transient_var_ref()
    }
}

impl NamedEntity for ProjectionExpr<'_, Analyzed> {
    fn name(&self) -> Option<&str> {
        self.alias.as_ref().map(|ident| ident.as_str())
    }
}

/// Structural comparison, one impl per node for both states.
mod structural_eq {
    use super::*;
    use crate::structural_eq::StructuralEq;

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for SelectStmt<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            self.table_expression
                .eq_with(&other.table_expression, scope)
                && self.select_list.eq_with(&other.select_list, scope)
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for SelectList<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            self.is_distinct == other.is_distinct && self.elements.eq_with(&other.elements, scope)
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for SelectListExprs<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            self.0.eq_with(&other.0, scope)
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for ProjectionExpr<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            self.alias == other.alias && self.expr.eq_with(&other.expr, scope)
        }
    }

    /// The raw select-list element.
    impl StructuralEq<()> for SelectListElem<'_> {
        fn eq_with(&self, other: &Self, scope: &mut ()) -> bool {
            match (self, other) {
                (Self::Asterisk(x), Self::Asterisk(y)) => x == y,
                (Self::Expr(x), Self::Expr(y)) => x.eq_with(y, scope),
                _mismatched_shapes => false,
            }
        }
    }
}
