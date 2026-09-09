#![cfg_attr(
    not(test),
    deny(
        clippy::todo,
        clippy::unimplemented,
        clippy::panic,
        clippy::unreachable,
        clippy::expect_used,
        clippy::unwrap_used
    )
)]
#![deny(
    rustdoc::broken_intra_doc_links,
    unreachable_pub,
    unused_lifetimes,
    single_use_lifetimes
)]
#![allow(rustdoc::private_intra_doc_links)]

use std::collections::HashSet;

use smol_str::format_smolstr;

use sql_ast_new_nodes::error::AstResult;
use sql_ast_new_nodes::expr::Expr;
use sql_ast_new_nodes::multiset::Ctes;
use sql_ast_new_nodes::rendering::Rendered;
use sql_ast_new_nodes::scope::{Depth, EqDepths, Scope};
use sql_ast_new_nodes::select::{SelectList, SelectStmt};
use sql_ast_new_nodes::table_expression::{BoundVar, From, GroupBy};
use sql_ast_new_nodes::window::{FrameType, NamedWindow};
use sql_ast_new_nodes::{Analyzed, Raw, StructuralEq};

use crate::{analyze_err, semantic_err, AstTypeExpr};
use sql_ir::ir::types::DerivedType;

pub(crate) struct Stmt<'q> {
    /// WITH clause of current frame. Empty in case of `VALUES`.
    pub(crate) ctes: Ctes<'q, Analyzed>,
    /// FROM clause of current frame. Empty in case of `VALUES`.
    pub(crate) from: From<'q, Analyzed>,
    /// What is the stage of statement binding. For instance `WHERE`, `GROUP BY` etc.
    pub(crate) stage: Stage,
    /// One of
    /// - Raw select list for consulting while GROUP BY discovery
    /// - Bound select list for grouping expressions validation (GROUP BY ordinals utilze it)
    sel_lst: Option<RawBoundSelLst<'q>>,
    /// Bound GROUP BY needed for matching expression keys (expression subtrees) in HAVING binding.
    bound_grby: Option<GroupBy<'q, Analyzed>>,
    /// GROUP BY clause context needed for validating
    /// aggregate functions under select list expressions.
    pub(crate) grby_ctx: GroupByCtx,
    /// Used vars. These ones are neither grouped nor aggregated.
    pub(crate) target_vars: Vec<BoundVar>,
    /// Whether this frame is aggregated.
    pub(crate) is_aggr: bool,
    /// Named windows bound by the WINDOW clause, in definition order. Parked on
    /// the frame for the `OVER` references that follow: the select list, and the
    /// query ORDER BY, which binds after the SELECT statement itself.
    windows: Vec<BoundWindow<'q>>,
}

pub(crate) const MISSING_RAW_SELECT_LIST_ERR: &str = "missing raw SELECT in current frame";
pub(crate) const MISSING_BOUND_SELECT_LIST_ERR: &str = "missing bound SELECT in current frame";
const MISSING_GROUP_BY_ERR: &str = "missing GROUP BY in current frame";

impl<'q> Stmt<'q> {
    pub(crate) fn new(from: From<'q, Analyzed>, stage: Stage) -> Self {
        Self {
            ctes: Ctes::default(),
            from,
            stage,
            sel_lst: None,
            bound_grby: None,
            grby_ctx: GroupByCtx::default(),
            target_vars: Vec::new(),
            is_aggr: false,
            windows: Vec::new(),
        }
    }

    pub(crate) fn set_stage(&mut self, stage: Stage) -> Stage {
        std::mem::replace(&mut self.stage, stage)
    }

    /// If current frame is grouped validate target list referencing only grouped columns
    /// or columns functionally dependent on any grouped expressions subset -
    /// i.e. grouping expression is unique (e.g. PK, unique index etc.).
    ///
    /// `scope` reads the target vars' sources; the frame itself is already
    /// popped by now, so it is the binder's stack plus this frame on top.
    pub(crate) fn check_grouping<S: Scope<'q> + ?Sized>(
        &self,
        stmt: &SelectStmt<'q, Analyzed>,
        scope: &S,
    ) -> AstResult<()> {
        if stmt.has_group_by() || self.is_aggr || stmt.has_having() {
            // Get grouping expressions from GROUP BY
            let group_by = stmt.grouping_vars(stmt.select_list());

            // TODO: wire functional dependencies through subquery and CTE boundaries

            // Fill set of relations whose PK is GROUP BY expressions subset.
            let relation = |var: &BoundVar| (var.depth, var.entry);
            let mut tblpk_grby_subset = HashSet::<(Depth, usize)>::new();
            self.target_vars
                .iter()
                .filter_map(|var| {
                    let tbl = scope.entry(*var)?.tbl_factor().table()?;
                    Some((var, tbl))
                })
                .for_each(|(var, tbl)| {
                    let id = relation(var);
                    if !tblpk_grby_subset.contains(&id)
                        && !tbl.primary_key.positions.is_empty()
                        && tbl.primary_key.positions.iter().all(|&pos| {
                            group_by.contains(&BoundVar::new(
                                var.depth,
                                var.entry,
                                pos,
                                DerivedType::unknown(),
                            ))
                        })
                    {
                        tblpk_grby_subset.insert(id);
                    }
                });

            if let Some(target_var) = self.target_vars.iter().find(|target_var| {
                !tblpk_grby_subset.contains(&relation(target_var)) && !group_by.contains(target_var)
            }) {
                let target_var =
                    Rendered::<'_, 'q, Analyzed, _>::new(target_var, scope.render_scope());
                return Err(semantic_err(format_smolstr!(
                    r#"column "{target_var}" must appear in the GROUP BY clause or be used in an aggregate function"#
                )));
            }
        }
        Ok(())
    }

    /// Whether `expr` is one of this level's expression grouping keys.
    /// `depths.left` is this level, `depths.right` the depth `expr` is written at.
    pub(crate) fn grouping_covers(&self, expr: &Expr<'q, Analyzed>, mut depths: EqDepths) -> bool {
        self.bound_grby_ref().is_some_and(|group_by| {
            group_by
                .0
                .iter()
                .filter_map(|elem| elem.grouping_key_expr(self.bound_sel_lst_ref().ok()))
                .any(|key| key.eq_with(expr, &mut depths))
        })
    }

    pub(crate) fn bound_grby_ref(&self) -> Option<&GroupBy<'q, Analyzed>> {
        self.bound_grby.as_ref()
    }

    pub(crate) fn bound_grby_mut(&mut self) -> AstResult<&mut GroupBy<'q, Analyzed>> {
        self.bound_grby
            .as_mut()
            .ok_or_else(|| analyze_err(format_smolstr!("{MISSING_GROUP_BY_ERR}")))
    }

    /// If this `var` does not come from grouping expression add it to targets.
    pub(crate) fn add_target_var(&mut self, var: BoundVar) {
        if self.stage.forbid_ungrouped() && !self.target_vars.contains(&var) {
            self.target_vars.push(var);
        }
    }

    /// The named window `name` resolves to, if the WINDOW clause defined one so far.
    pub(crate) fn find_window(&self, name: &str) -> Option<&BoundWindow<'q>> {
        self.windows.iter().find(|window| window.def.name() == name)
    }

    pub(crate) fn push_window(&mut self, window: BoundWindow<'q>) {
        self.windows.push(window);
    }

    pub(crate) fn take_windows(&mut self) -> Vec<BoundWindow<'q>> {
        std::mem::take(&mut self.windows)
    }

    pub(crate) fn raw_sel_lst_ref(&self) -> AstResult<&SelectList<'q, Raw>> {
        match self.sel_lst.as_ref() {
            Some(lst) => lst.raw_ref(),
            _ => Err(analyze_err(format_smolstr!(
                "{MISSING_RAW_SELECT_LIST_ERR}"
            ))),
        }
    }

    pub(crate) fn set_raw_sel_lst(&mut self, sel_lst: SelectList<'q, Raw>) {
        self.sel_lst = Some(RawBoundSelLst::Raw(sel_lst));
    }

    pub(crate) fn set_bound_sel_lst(&mut self, sel_lst: SelectList<'q, Analyzed>) {
        self.sel_lst = Some(RawBoundSelLst::Bound(sel_lst));
    }

    pub(crate) fn bound_sel_lst_ref(&self) -> AstResult<&SelectList<'q, Analyzed>> {
        match self.sel_lst.as_ref() {
            Some(lst) => lst.bound_ref(),
            _ => Err(analyze_err(format_smolstr!(
                "{MISSING_BOUND_SELECT_LIST_ERR}"
            ))),
        }
    }

    pub(crate) fn bound_sel_lst_mut(&mut self) -> AstResult<&mut SelectList<'q, Analyzed>> {
        match self.sel_lst.as_mut() {
            Some(lst) => lst.bound_mut(),
            _ => Err(analyze_err(format_smolstr!(
                "{MISSING_BOUND_SELECT_LIST_ERR}"
            ))),
        }
    }

    pub(crate) fn take_raw_sel_lst(&mut self) -> AstResult<SelectList<'q, Raw>> {
        match std::mem::take(&mut self.sel_lst) {
            Some(lst) => lst.take_raw(),
            _ => Err(analyze_err(format_smolstr!(
                "{MISSING_RAW_SELECT_LIST_ERR}"
            ))),
        }
    }

    pub(crate) fn take_bound_sel_lst(&mut self) -> AstResult<SelectList<'q, Analyzed>> {
        match std::mem::take(&mut self.sel_lst) {
            Some(lst) => lst.take_bound(),
            _ => Err(analyze_err(format_smolstr!(
                "{MISSING_RAW_SELECT_LIST_ERR}"
            ))),
        }
    }

    /// Park bound GROUP BY on the frame for HAVING binding.
    pub(crate) fn set_bound_grby(&mut self, group_by: GroupBy<'q, Analyzed>) {
        self.bound_grby = Some(group_by);
    }

    pub(crate) fn take_bound_grby(&mut self) -> AstResult<GroupBy<'q, Analyzed>> {
        std::mem::take(&mut self.bound_grby)
            .ok_or_else(|| analyze_err(format_smolstr!("{MISSING_GROUP_BY_ERR}")))
    }
}

/// What a frame's query level is analyzing right now.
///
/// An aggregate call is rejected by the clause of the level it *belongs to*, not
/// the one it is written in, so the clause has to be recorded on the frame
/// rather than tracked alongside the walk.
#[derive(Clone, Copy)]
pub(crate) enum Stage {
    Projection,
    JoinOn,
    JoinUsing,
    Where,
    GroupBy,
    Having,
    // Window function call
    WindowFn,
    /// PARTITION BY / ORDER BY of a window specification, named or inline.
    WindowDef,
    /// FILTER predicate of a window function call.
    WindowFilter,
    /// Frame offset expression: `ROWS <expr> PRECEDING` and kin.
    WindowFrameOffset(FrameType),
    OrderBy,
    /// `VALUES` statement.
    Values,
    /// The arguments of a table function in FROM.
    FunctionInFrom,
}

impl Stage {
    /// How the clause is named in the aggregate rejection,
    /// or [`None`] when the clause accepts aggregates.
    pub(crate) fn forbid_aggr(&self) -> Option<&'static str> {
        match self {
            Self::Projection | Self::Having | Self::OrderBy | Self::WindowDef | Self::WindowFn => {
                None
            }
            Self::JoinOn | Self::JoinUsing => Some("JOIN conditions"),
            Self::Where => Some("WHERE"),
            Self::GroupBy => Some("GROUP BY"),
            Self::WindowFilter => Some("FILTER"),
            Self::WindowFrameOffset(FrameType::Rows) => Some("window ROWS"),
            Self::WindowFrameOffset(FrameType::Range) => Some("window RANGE"),
            Self::Values => Some("VALUES"),
            Self::FunctionInFrom => Some("functions in FROM"),
        }
    }

    /// The window-function counterpart of [`Self::forbid_aggr`].
    /// Only the select list and the query ORDER BY accept window functions.
    pub(crate) fn forbid_win_fn(&self) -> Option<&'static str> {
        match self {
            Self::Projection | Self::WindowFn | Self::OrderBy => None,
            Self::JoinOn | Self::JoinUsing => Some("JOIN conditions"),
            Self::Where => Some("WHERE"),
            Self::GroupBy => Some("GROUP BY"),
            Self::Having => Some("HAVING"),
            Self::WindowDef | Self::WindowFrameOffset(_) => Some("window definitions"),
            Self::WindowFilter => Some("FILTER"),
            Self::Values => Some("VALUES"),
            Self::FunctionInFrom => Some("functions in FROM"),
        }
    }

    pub(crate) fn forbid_ungrouped(&self) -> bool {
        matches!(
            self,
            Self::Projection
                | Self::Having
                | Self::OrderBy
                | Self::WindowFn
                | Self::WindowDef
                | Self::WindowFilter
        )
    }
}

/// A named window bound by the WINDOW clause.
pub(crate) struct BoundWindow<'q> {
    /// The analyzed definition. References to base windows are preserved:
    /// inherited ORDER BY expressions are not copied into this definition.
    pub(crate) def: NamedWindow<'q, Analyzed>,

    /// Number of ORDER BY expressions this window uses, whether declared
    /// here or inherited from a base window. Zero if neither provides one.
    ///
    /// Used to require exactly one ORDER BY expression for RANGE with an
    /// offset, and to reject overriding an inherited ORDER BY.
    pub(crate) effective_order_by_len: usize,

    /// A copy of the type mirror of the single ORDER BY expression this
    /// window uses, whether declared here or inherited from a base window.
    ///
    /// A RANGE frame with offsets, here or in a window that inherits this
    /// ORDER BY, gets the copy in its type mirror. The type checker needs
    /// the ORDER BY expression and the RANGE offsets in the same frame to
    /// check that their types are compatible.
    ///
    /// For example, for `w1 AS (ORDER BY price), w2 AS (w1)`, both windows
    /// store a copy of the mirror of w1's `price`.
    ///
    /// None unless the ORDER BY has exactly one expression: only a RANGE
    /// offset reads the copy, and such a frame requires exactly one.
    pub(crate) order_t_expr: Option<AstTypeExpr>,
}

impl BoundWindow<'_> {
    pub(crate) fn has_frame(&self) -> bool {
        self.def.has_frame()
    }

    pub(crate) fn has_effective_order_by(&self) -> bool {
        self.effective_order_by_len > 0
    }
}

#[derive(Default)]
pub(crate) struct GroupByCtx {
    /// This is mapping of GROUP BY element index (0-based) into raw (unexpanded)
    /// SELECT list element position. This is needed for marking potential
    /// aliases as ordinal positions.
    pub(crate) raw_pos: Vec<(usize, usize)>,
    /// Mapping of raw ordinal position into position in SELECT list after asterisk expansion.
    pub(crate) raw_to_expanded: Vec<usize>,
}

enum RawBoundSelLst<'q> {
    Raw(SelectList<'q, Raw>),
    /// Bound select list. Needed for substituting ordinal position instead of alias in GROUP BY,
    /// and then held through HAVING binding for resolving expressions matching a select list
    /// element which is under ordinal.
    Bound(SelectList<'q, Analyzed>),
}

impl<'q> RawBoundSelLst<'q> {
    fn raw_ref(&self) -> AstResult<&SelectList<'q, Raw>> {
        match self {
            RawBoundSelLst::Raw(raw) => Ok(raw),
            _ => Err(analyze_err(format_smolstr!(
                "{MISSING_RAW_SELECT_LIST_ERR}"
            ))),
        }
    }

    fn bound_ref(&self) -> AstResult<&SelectList<'q, Analyzed>> {
        match self {
            RawBoundSelLst::Bound(bound) => Ok(bound),
            _ => Err(analyze_err(format_smolstr!(
                "{MISSING_BOUND_SELECT_LIST_ERR}"
            ))),
        }
    }

    fn bound_mut(&mut self) -> AstResult<&mut SelectList<'q, Analyzed>> {
        match self {
            RawBoundSelLst::Bound(bound) => Ok(bound),
            _ => Err(analyze_err(format_smolstr!(
                "{MISSING_BOUND_SELECT_LIST_ERR}"
            ))),
        }
    }

    fn take_raw(self) -> AstResult<SelectList<'q, Raw>> {
        match self {
            RawBoundSelLst::Raw(raw) => Ok(raw),
            _ => Err(analyze_err(format_smolstr!(
                "{MISSING_RAW_SELECT_LIST_ERR}"
            ))),
        }
    }

    fn take_bound(self) -> AstResult<SelectList<'q, Analyzed>> {
        match self {
            RawBoundSelLst::Bound(bound) => Ok(bound),
            _ => Err(analyze_err(format_smolstr!(
                "{MISSING_RAW_SELECT_LIST_ERR}"
            ))),
        }
    }
}

/// Where one level's column references stood before an expression that may turn out
/// to be a grouping key was bound.
/// See [`grouping_key_marks`](`crate::BinderCtx::grouping_key_marks`).
pub(crate) struct GroupingKeyMark {
    /// Rewind point in the level's own [`Stmt::target_vars`].
    pub(crate) frame: usize,
    /// Rewind point in the innermost open aggregate call's
    /// `columns parked for this level, when a call is open at all.
    pub(crate) aggr: Option<usize>,
}
