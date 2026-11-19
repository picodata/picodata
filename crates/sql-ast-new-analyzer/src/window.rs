//! Analysis of window functions and the named `WINDOW` clause.
//!
//! # Scope and lifecycle
//! Named windows resolve left to right and live on the statement frame (see
//! [`BoundWindow`]): a definition joins the registry as soon as it is bound, so
//! later definitions and every `OVER` reference after it — the select list, and
//! the query ORDER BY, which binds after the SELECT statement itself — can find
//! it, while a subquery, pushing a frame of its own, cannot. The multiset
//! statement puts the parked definitions back once ORDER BY has bound.
//!
//! # Inheritance
//! A specification referencing a base window keeps the reference rather than
//! flattening it: the base's expressions are bound once, at its definition, and
//! [`BoundWindow::effective_order_by_len`] carries what validation needs from
//! the chain — ORDER BY presence travels, a frame never does (copying a framed
//! base is an error), and PARTITION BY is the base's alone (overriding it is
//! an error too).
//! One mirror goes against that grain: a RANGE frame with offsets gets a copy
//! of its effective ORDER BY's type mirror (see [`BoundWindow::order_t_expr`]),
//! as Postgres copies the base's ORDER BY clause, because the type system
//! checks a RANGE offset against the ORDER BY column's type.
//!
//! # The grouping stance
//! A window function is evaluated after grouping, per output row. That single
//! fact shapes every rule here: its arguments, FILTER and specification
//! expressions are grouping-checked like projection expressions (with no
//! aggregate exemption for the call itself — `sum(a) OVER ()` does not
//! aggregate `a`), aggregates *are* legal inside window definitions, and a
//! window aggregate does not group its query level.

use smol_str::format_smolstr;

use sql_ast_new_nodes::error::{AstErr, AstResult};
use sql_ast_new_nodes::expr::{Expr, ExprInner};
use sql_ast_new_nodes::multiset::{OrderBy, OrderByElement};
use sql_ast_new_nodes::table_expression::OrdrByGrpByElem;
use sql_ast_new_nodes::window::{
    FrameBound, FrameBounds, FrameType, NamedWindow, NamedWindows, ResolvedWindowFn, WindowFrame,
    WindowFunction, WindowFunctionArgs, WindowRef, WindowSpec,
};
use sql_ast_new_nodes::{Analyzed, AstNodeId, Ident, Raw};
use sql_ir::ir::aggregates::AggregateKind;
use sql_ir::ir::metadata::Metadata;
use sql_type_system::expr::WindowFrame as TypeWindowFrame;

use crate::frame::{BoundWindow, Stage};
use crate::{
    analyze_err, semantic_err, AnalyzerCtx, AstTypeExpr, AstTypeExprKind, AstTypeReport, Binder,
    ExprTypeDeriver,
};

fn window_does_not_exist(name: &str) -> AstErr {
    semantic_err(format_smolstr!(r#"window "{name}" does not exist"#))
}

/// The mirror of a window carrying no expressions of its own: what a
/// [`WindowRef::Name`] reference contributes. The named definition's
/// expressions were typed at the WINDOW clause, and a `Window` mirror never
/// reports a type for its own id, so nothing is lost.
fn empty_window_t_expr<M: Metadata>(meta: &mut AnalyzerCtx<'_, M>) -> AstTypeExpr {
    let id = meta.type_system.inc_expr_id();
    AstTypeExpr::new(
        id,
        AstTypeExprKind::Window {
            order_by: Vec::new(),
            partition_by: Vec::new(),
            frame: None,
        },
    )
}

/// Bind the WINDOW clause. Each definition is typed and grouping-checked even
/// when nothing references it, like in Postgres.
impl<'q> Binder<'q> for NamedWindows<'q, Raw> {
    type BoundNode = ();

    fn bind<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode> {
        let named_windows = self.0;
        for named_window in named_windows {
            let (name, spec) = named_window.into_parts();

            if meta
                .binder
                .top_frame_ref()?
                .find_window(name.as_str())
                .is_some()
            {
                return Err(semantic_err(format_smolstr!(
                    r#"window "{}" is already defined"#,
                    name.as_str()
                )));
            }

            let BoundSpec {
                spec,
                over_t_expr,
                effective_order_by_len,
                order_t_expr,
            } = *bind_window_spec(spec, meta)?;

            // The root that puts the definition's expressions through type
            // analysis with the rest of the statement.
            meta.type_system.add_rest(over_t_expr);

            meta.binder.top_frame_mut()?.push_window(BoundWindow {
                def: NamedWindow::from_parts(name, spec),
                effective_order_by_len,
                order_t_expr,
            });
        }
        Ok(())
    }
}

/// Bind a window function call: `name(args) [FILTER (WHERE ...)] OVER window`.
///
/// The checks run in Postgres' order: placement first — a call in WHERE is
/// rejected before anything in it binds — then nesting, then the call's own
/// shape, then its pieces left to right, the window reference last.
// Out of line: inlined into recursive `Expr::bind`, its slots grow every level's frame.
#[inline(never)]
pub(crate) fn bind_win_fn<'q, M: Metadata>(
    win_fn: WindowFunction<'q, Raw>,
    curr_id: AstNodeId,
    meta: &mut AnalyzerCtx<'q, M>,
) -> AstResult<(ExprInner<'q, Analyzed>, AstTypeExpr)> {
    let (name, args, filter, window) = win_fn.into_parts();

    let aggr = check_win_fn_call(name.as_str(), &args, filter.is_some(), meta)?;
    let args = bind_win_args(args, meta)?;
    let filter = bind_win_filter(filter, meta)?;
    bind_win_over(name, aggr, args, filter, window, curr_id, meta)
}

/// The rest of [`bind_win_fn`], once its arguments and FILTER are bound: the
/// window reference, the call's mirror and the function it resolves to.
#[inline(never)]
fn bind_win_over<'q, M: Metadata>(
    name: Ident,
    aggr: Option<AggregateKind>,
    (args, arg_t_exprs): (WindowFunctionArgs<'q, Analyzed>, Vec<AstTypeExpr>),
    (filter, filter_t_expr): (Option<Expr<'q, Analyzed>>, Option<AstTypeExpr>),
    winref: WindowRef<'q, Raw>,
    curr_id: AstNodeId,
    meta: &mut AnalyzerCtx<'q, M>,
) -> AstResult<(ExprInner<'q, Analyzed>, AstTypeExpr)> {
    let (over, over_t_expr) = *bind_win_ref(winref, meta)?;

    let t_expr = AstTypeExpr::new(
        curr_id,
        AstTypeExprKind::WindowFunction {
            name: name.as_str().to_string(),
            args: arg_t_exprs,
            filter: filter_t_expr.map(Box::new),
            over: Box::new(over_t_expr),
        },
    );
    let resolved_fn = resolve_win_fn(aggr, name.as_str(), &t_expr, meta)?;

    Ok((
        ExprInner::WindowFunction(Box::new(WindowFunction::from_parts(
            name,
            resolved_fn,
            args,
            filter,
            over,
        ))),
        t_expr,
    ))
}

// The helpers below are out of line for the dev build: each one's slots would
// otherwise all live in `bind_win_fn`'s frame.

/// The placement, nesting and shape checks of a window function call, run
/// before anything in it binds. Returns the aggregate the name calls, if any.
#[inline(never)]
fn check_win_fn_call<M: Metadata>(
    name: &str,
    args: &WindowFunctionArgs<'_, Raw>,
    has_filter: bool,
    meta: &AnalyzerCtx<'_, M>,
) -> AstResult<Option<AggregateKind>> {
    if let Some(clause) = meta.binder.top_frame_ref()?.stage.forbid_win_fn() {
        return Err(semantic_err(format_smolstr!(
            "window functions are not allowed in {clause}"
        )));
    }

    let depth = meta.binder.depth()?;
    if meta
        .binder
        .aggrs
        .last()
        .is_some_and(|aggr| aggr.depth == depth)
    {
        return Err(semantic_err(format_smolstr!(
            "aggregate function calls cannot contain window function calls"
        )));
    }

    if matches!(meta.binder.top_frame_ref()?.stage, Stage::WindowFn) {
        return Err(semantic_err(format_smolstr!(
            "window function calls cannot be nested"
        )));
    }

    let aggr = AggregateKind::from_name(name);
    if has_filter && !aggr.is_some() {
        return Err(semantic_err(format_smolstr!(
            "FILTER is not implemented for non-aggregate window functions"
        )));
    }

    if matches!(aggr, Some(AggregateKind::COUNT)) {
        if let WindowFunctionArgs::List(lst) = args {
            if lst.is_empty() {
                return Err(semantic_err(format_smolstr!(
                    "count(*) must be used to call a parameterless aggregate function"
                )));
            }
        }
    }
    Ok(aggr)
}

#[inline(never)]
fn bind_win_args<'q, M: Metadata>(
    args: WindowFunctionArgs<'q, Raw>,
    meta: &mut AnalyzerCtx<'q, M>,
) -> AstResult<(WindowFunctionArgs<'q, Analyzed>, Vec<AstTypeExpr>)> {
    let prev_stage = meta.binder.top_frame_mut()?.set_stage(Stage::WindowFn);

    let bound = match args {
        WindowFunctionArgs::Asterisk => (WindowFunctionArgs::Asterisk, Vec::new()),
        WindowFunctionArgs::List(exprs) => {
            let mut bound_exprs = Vec::with_capacity(exprs.len());
            let mut t_exprs = Vec::with_capacity(exprs.len());
            for expr in exprs {
                let (bound_expr, t_expr) = expr.bind(meta)?;
                bound_exprs.push(bound_expr);
                t_exprs.push(t_expr);
            }
            (WindowFunctionArgs::List(bound_exprs), t_exprs)
        }
    };

    meta.binder.top_frame_mut()?.set_stage(prev_stage);
    Ok(bound)
}

#[inline(never)]
fn bind_win_filter<'q, M: Metadata>(
    filter: Option<Expr<'q, Raw>>,
    meta: &mut AnalyzerCtx<'q, M>,
) -> AstResult<(Option<Expr<'q, Analyzed>>, Option<AstTypeExpr>)> {
    let Some(filter) = filter else {
        return Ok((None, None));
    };
    let prev_stage = meta.binder.top_frame_mut()?.set_stage(Stage::WindowFilter);
    let (bound, t_expr) = filter.bind_with_volatility(meta, false)?;
    meta.binder.top_frame_mut()?.set_stage(prev_stage);
    Ok((Some(bound), Some(t_expr)))
}

/// Bind the `OVER` part: a named window reference or an inline specification.
#[inline(never)]
fn bind_win_ref<'q, M: Metadata>(
    window: WindowRef<'q, Raw>,
    meta: &mut AnalyzerCtx<'q, M>,
) -> AstResult<Box<(WindowRef<'q, Analyzed>, AstTypeExpr)>> {
    Ok(Box::new(match window {
        WindowRef::Name(window_name) => {
            meta.binder
                .top_frame_ref()?
                .find_window(window_name.as_str())
                .ok_or_else(|| window_does_not_exist(window_name.as_str()))?;
            let over_t_expr = empty_window_t_expr(meta);
            (WindowRef::Name(window_name), over_t_expr)
        }
        WindowRef::Spec(spec) => {
            let BoundSpec {
                spec, over_t_expr, ..
            } = *bind_window_spec(spec, meta)?;
            (WindowRef::Spec(spec), over_t_expr)
        }
    }))
}

#[inline(never)]
fn resolve_win_fn<M: Metadata>(
    aggr: Option<AggregateKind>,
    name: &str,
    t_expr: &AstTypeExpr,
    meta: &mut AnalyzerCtx<'_, M>,
) -> AstResult<ResolvedWindowFn> {
    match aggr {
        Some(kind) => Ok(ResolvedWindowFn::Aggregate(kind)),
        None => match name {
            "row_number" => Ok(ResolvedWindowFn::RowNumber),
            "last_value" => Ok(ResolvedWindowFn::LastValue),
            _ => {
                // Keep the type system's unknown-name and overload diagnostics.
                meta.type_system.analyzer.analyze(t_expr, None)?;
                Err(analyze_err(format_smolstr!(
                    "window function {name} has no resolved AST kind"
                )))
            }
        },
    }
}

/// A bound window specification and what its callers read off it.
struct BoundSpec<'q> {
    spec: WindowSpec<'q, Analyzed>,
    /// `Window` mirror of the specification's *own* expressions, on a
    /// synthetic id. Inherited parts are absent by design: the base bound its
    /// expressions at its definition. One exception — a RANGE frame with
    /// offsets gets a copy of its effective ORDER BY mirror (see
    /// [`BoundWindow::order_t_expr`]), so that the offsets are typed against it.
    over_t_expr: AstTypeExpr,
    effective_order_by_len: usize,
    /// [`BoundWindow::order_t_expr`] of a named definition with this body.
    order_t_expr: Option<AstTypeExpr>,
}

/// Bind one window specification — a named definition's body or an inline
/// `OVER (...)` — resolving its base reference and validating its frame.
fn bind_window_spec<'q, M: Metadata>(
    spec: WindowSpec<'q, Raw>,
    meta: &mut AnalyzerCtx<'q, M>,
) -> AstResult<Box<BoundSpec<'q>>> {
    let (effective_order_by_len, base_order_t_expr) = check_window_spec(
        spec.base.as_ref().map(Ident::as_str),
        !spec.partition_by.is_empty(),
        spec.order_by.as_ref().map(OrderBy::len),
        spec.frame.as_deref(),
        meta,
    )?;

    let prev_stage = meta.binder.top_frame_mut()?.set_stage(Stage::WindowDef);
    let bound = bind_window_spec_exprs(spec, effective_order_by_len, base_order_t_expr, meta)?;
    meta.binder.top_frame_mut()?.set_stage(prev_stage);
    Ok(bound)
}

// The helpers below are out of line for the dev build, like those of `bind_win_fn`.

/// Bind the expressions of a checked specification, in the `WindowDef` stage.
#[inline(never)]
fn bind_window_spec_exprs<'q, M: Metadata>(
    spec: WindowSpec<'q, Raw>,
    effective_order_by_len: usize,
    base_order_t_expr: Option<AstTypeExpr>,
    meta: &mut AnalyzerCtx<'q, M>,
) -> AstResult<Box<BoundSpec<'q>>> {
    let WindowSpec {
        base,
        partition_by,
        order_by,
        frame,
    } = spec;

    let (bound_partition_by, partition_by_t_exprs) = bind_window_partition_by(partition_by, meta)?;
    let (bound_order_by, order_by_t_exprs) = bind_window_order_by(order_by, meta)?;

    let order_t_expr = match (&bound_order_by, order_by_t_exprs.as_slice()) {
        (Some(_), [own]) => Some(own.clone()),
        (Some(_), _) => None,
        (None, _) => base_order_t_expr,
    };

    let (bound_frame, frame_t) = bind_window_frame(frame, order_t_expr.as_ref(), meta)?;

    let over_id = meta.type_system.inc_expr_id();
    Ok(Box::new(BoundSpec {
        spec: WindowSpec {
            base,
            partition_by: bound_partition_by,
            order_by: bound_order_by,
            frame: bound_frame,
        },
        over_t_expr: AstTypeExpr::new(
            over_id,
            AstTypeExprKind::Window {
                order_by: order_by_t_exprs,
                partition_by: partition_by_t_exprs,
                frame: frame_t,
            },
        ),
        effective_order_by_len,
        order_t_expr,
    }))
}

/// Check a specification before anything in it binds: the base reference in
/// Postgres' order (the PARTITION BY override, the ORDER BY override, then the
/// frame copy), then the frame's structure. Returns the effective ORDER BY
/// length and the ORDER BY mirror inherited from the base.
#[inline(never)]
fn check_window_spec<M: Metadata>(
    base: Option<&str>,
    has_partition_by: bool,
    order_by_len: Option<usize>,
    frame: Option<&WindowFrame<'_, Raw>>,
    meta: &AnalyzerCtx<'_, M>,
) -> AstResult<(usize, Option<AstTypeExpr>)> {
    let (base_effective_order_by_len, base_order_t_expr) = match base {
        None => (0, None),
        Some(base_name) => {
            check_window_base(base_name, has_partition_by, order_by_len.is_some(), meta)?
        }
    };
    let effective_order_by_len = order_by_len.unwrap_or(base_effective_order_by_len);

    // Frame sanity is structural: validated before anything in it binds.
    if let Some(frame) = frame {
        validate_frame_bounds(frame)?;
        validate_range_frame(frame, effective_order_by_len)?;
    }
    Ok((effective_order_by_len, base_order_t_expr))
}

fn check_window_base<M: Metadata>(
    base_name: &str,
    has_partition_by: bool,
    has_order_by: bool,
    meta: &AnalyzerCtx<'_, M>,
) -> AstResult<(usize, Option<AstTypeExpr>)> {
    let base_window = meta
        .binder
        .top_frame_ref()?
        .find_window(base_name)
        .ok_or_else(|| window_does_not_exist(base_name))?;

    if has_partition_by {
        return Err(semantic_err(format_smolstr!(
            r#"cannot override PARTITION BY clause of window "{base_name}""#
        )));
    }
    if has_order_by && base_window.has_effective_order_by() {
        return Err(semantic_err(format_smolstr!(
            r#"cannot override ORDER BY clause of window "{base_name}""#
        )));
    }
    if base_window.has_frame() {
        return Err(semantic_err(format_smolstr!(
            r#"cannot copy window "{base_name}" because it has a frame clause"#
        )));
    }
    Ok((
        base_window.effective_order_by_len,
        base_window.order_t_expr.clone(),
    ))
}

#[inline(never)]
fn bind_window_partition_by<'q, M: Metadata>(
    partition_by: Vec<Expr<'q, Raw>>,
    meta: &mut AnalyzerCtx<'q, M>,
) -> AstResult<(Vec<Expr<'q, Analyzed>>, Vec<AstTypeExpr>)> {
    let mut bound = Vec::with_capacity(partition_by.len());
    let mut t_exprs = Vec::with_capacity(partition_by.len());
    for expr in partition_by {
        let (bound_expr, t_expr) = expr.bind_with_volatility(meta, false)?;
        bound.push(bound_expr);
        t_exprs.push(t_expr);
    }
    Ok((bound, t_exprs))
}

#[inline(never)]
fn bind_window_order_by<'q, M: Metadata>(
    order_by: Option<OrderBy<'q, Raw>>,
    meta: &mut AnalyzerCtx<'q, M>,
) -> AstResult<(Option<OrderBy<'q, Analyzed>>, Vec<AstTypeExpr>)> {
    let Some(order_by) = order_by else {
        return Ok((None, Vec::new()));
    };
    let mut bound_elems = Vec::with_capacity(order_by.len());
    let mut t_exprs = Vec::new();
    for elem in order_by.into_elements() {
        let (elem_expr, direction, nulls) = elem.into_parts();
        let expr = match elem_expr {
            // The parser keeps window ORDER BY literals as constants.
            OrdrByGrpByElem::Ordinal(_) => {
                return Err(analyze_err(format_smolstr!(
                    "select-list ordinal in a window ORDER BY"
                )))
            }
            OrdrByGrpByElem::Expr(expr) => expr,
        };
        let (bound_expr, t_expr) = expr.bind_with_volatility(meta, false)?;
        bound_elems.push(OrderByElement::from_parts(
            OrdrByGrpByElem::Expr(bound_expr),
            direction,
            nulls,
        ));
        t_exprs.push(t_expr);
    }
    Ok((Some(OrderBy::from(bound_elems)), t_exprs))
}

/// The frame-extent sanity Postgres enforces in its grammar; this grammar
/// accepts every combination, so the analyzer holds the line instead.
/// Structural only — nothing in the frame is bound yet. The single-bound form
/// makes the given bound the start and CURRENT ROW the end.
fn validate_frame_bounds(frame: &WindowFrame<'_, Raw>) -> AstResult<()> {
    let (start, end) = match &frame.bounds {
        FrameBounds::Single(start) => (start, None),
        FrameBounds::Between(start, end) => (start, Some(end)),
    };

    if matches!(start, FrameBound::UnboundedFollowing) {
        return Err(semantic_err(format_smolstr!(
            "frame start cannot be UNBOUNDED FOLLOWING"
        )));
    }
    match end {
        None => {
            if matches!(start, FrameBound::Following(_)) {
                return Err(semantic_err(format_smolstr!(
                    "frame starting from following row cannot end with current row"
                )));
            }
        }
        Some(end) => {
            if matches!(end, FrameBound::UnboundedPreceding) {
                return Err(semantic_err(format_smolstr!(
                    "frame end cannot be UNBOUNDED PRECEDING"
                )));
            }
            if matches!(start, FrameBound::CurrentRow) && matches!(end, FrameBound::Preceding(_)) {
                return Err(semantic_err(format_smolstr!(
                    "frame starting from current row cannot have preceding rows"
                )));
            }
            if matches!(start, FrameBound::Following(_))
                && matches!(end, FrameBound::CurrentRow | FrameBound::Preceding(_))
            {
                return Err(semantic_err(format_smolstr!(
                    "frame starting from following row cannot have preceding rows"
                )));
            }
        }
    }
    Ok(())
}

/// A RANGE frame with an offset bound needs exactly one ORDER BY column — of
/// the *effective* specification, base-inherited ORDER BY included.
fn validate_range_frame(
    frame: &WindowFrame<'_, Raw>,
    effective_order_by_len: usize,
) -> AstResult<()> {
    if !matches!(frame.ty, FrameType::Range) {
        return Ok(());
    }
    let has_offset = match &frame.bounds {
        FrameBounds::Single(bound) => is_offset_bound(bound),
        FrameBounds::Between(start, end) => is_offset_bound(start) || is_offset_bound(end),
    };
    if has_offset && effective_order_by_len != 1 {
        return Err(semantic_err(format_smolstr!(
            "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column"
        )));
    }
    Ok(())
}

fn is_offset_bound(bound: &FrameBound<'_, Raw>) -> bool {
    matches!(bound, FrameBound::Preceding(_) | FrameBound::Following(_))
}

/// The type mirror is `None` for a frame without offsets: it has nothing to type.
#[allow(clippy::type_complexity)]
fn bind_window_frame<'q, M: Metadata>(
    frame: Option<Box<WindowFrame<'q, Raw>>>,
    order_t_expr: Option<&AstTypeExpr>,
    meta: &mut AnalyzerCtx<'q, M>,
) -> AstResult<(
    Option<Box<WindowFrame<'q, Analyzed>>>,
    Option<Box<TypeWindowFrame<AstNodeId>>>,
)> {
    let Some(frame) = frame else {
        return Ok((None, None));
    };
    let WindowFrame { ty, bounds } = *frame;
    let mut bound_offsets = Vec::new();

    let bounds = match bounds {
        FrameBounds::Single(start) => {
            FrameBounds::Single(bind_frame_bound(start, ty, meta, &mut bound_offsets)?)
        }
        FrameBounds::Between(start, end) => FrameBounds::Between(
            bind_frame_bound(start, ty, meta, &mut bound_offsets)?,
            bind_frame_bound(end, ty, meta, &mut bound_offsets)?,
        ),
    };

    if bound_offsets.is_empty() {
        return Ok((Some(Box::new(WindowFrame { ty, bounds })), None));
    }
    let frame_t = match ty {
        FrameType::Rows => TypeWindowFrame::Rows(bound_offsets),
        FrameType::Range => {
            // `validate_range_frame` has checked that the effective ORDER BY
            // has exactly one column.
            let order_col = order_t_expr.ok_or_else(|| {
                analyze_err(format_smolstr!(
                    "RANGE offset frame without a single ORDER BY column"
                ))
            })?;
            TypeWindowFrame::Range {
                order_col: Box::new(order_col.clone()),
                offsets: bound_offsets,
            }
        }
    };

    Ok((
        Some(Box::new(WindowFrame { ty, bounds })),
        Some(Box::new(frame_t)),
    ))
}

fn bind_frame_bound<'q, M: Metadata>(
    bound: FrameBound<'q, Raw>,
    ty: FrameType,
    meta: &mut AnalyzerCtx<'q, M>,
    bound_offsets: &mut Vec<AstTypeExpr>,
) -> AstResult<FrameBound<'q, Analyzed>> {
    Ok(match bound {
        FrameBound::UnboundedPreceding => FrameBound::UnboundedPreceding,
        FrameBound::CurrentRow => FrameBound::CurrentRow,
        FrameBound::UnboundedFollowing => FrameBound::UnboundedFollowing,
        FrameBound::Preceding(offset) => {
            let (bound_expr, t_expr) = bind_frame_offset(offset, ty, meta)?;
            bound_offsets.push(t_expr);
            FrameBound::Preceding(bound_expr)
        }
        FrameBound::Following(offset) => {
            let (bound_expr, t_expr) = bind_frame_offset(offset, ty, meta)?;
            bound_offsets.push(t_expr);
            FrameBound::Following(bound_expr)
        }
    })
}

/// A frame offset is evaluated once per window, not per row: no column of the
/// window's own query level may appear in it — not even through a correlated
/// subquery or inside an aggregate — while an uncorrelated subquery or a
/// parameter is fine. Under the offset's stage
/// [`reg_var`](crate::BinderCtx::reg_var) rejects such a column, grouped or
/// not, as soon as it binds.
///
/// That order deliberately differs from Postgres, which reports these first:
/// a misplaced aggregate (`ROWS a + max(b) PRECEDING`), a window function,
/// an unknown column bound after the offending one, an offset of the wrong
/// type (`ROWS e PRECEDING` for a string `e`).
fn bind_frame_offset<'q, M: Metadata>(
    offset: Expr<'q, Raw>,
    ty: FrameType,
    meta: &mut AnalyzerCtx<'q, M>,
) -> AstResult<(Expr<'q, Analyzed>, AstTypeExpr)> {
    let prev_stage = meta
        .binder
        .top_frame_mut()?
        .set_stage(Stage::WindowFrameOffset(ty));

    let (bound_expr, t_expr) = offset.bind_with_volatility(meta, false)?;

    meta.binder.top_frame_mut()?.set_stage(prev_stage);

    Ok((bound_expr, t_expr))
}

// ------------------------- Type read-back -------------------------

impl ExprTypeDeriver for NamedWindow<'_, Analyzed> {
    fn derive_types(&mut self, type_report: &AstTypeReport) -> AstResult<()> {
        derive_window_spec_types(self.spec_mut(), type_report)
    }
}

pub(crate) fn derive_window_function_types(
    w_func: &mut WindowFunction<'_, Analyzed>,
    type_report: &AstTypeReport,
) -> AstResult<()> {
    match w_func.args_mut() {
        WindowFunctionArgs::Asterisk => { /* no expression children */ }
        WindowFunctionArgs::List(exprs) => {
            for expr in exprs {
                expr.derive_types(type_report)?;
            }
        }
    }
    if let Some(filter) = w_func.filter_mut() {
        // The type system checks the predicate is boolean.
        filter.derive_types(type_report)?;
    }
    match w_func.winref_mut() {
        WindowRef::Name(_) => { /* typed at its definition */ }
        WindowRef::Spec(spec) => derive_window_spec_types(spec, type_report)?,
    }
    Ok(())
}

fn derive_window_spec_types(
    spec: &mut WindowSpec<'_, Analyzed>,
    type_report: &AstTypeReport,
) -> AstResult<()> {
    for expr in &mut spec.partition_by {
        expr.derive_types(type_report)?;
    }
    if let Some(order_by) = &mut spec.order_by {
        for elem in order_by.elements_mut() {
            if let OrdrByGrpByElem::Expr(expr) = &mut elem.expr {
                expr.derive_types(type_report)?;
            }
        }
    }
    if let Some(frame) = &mut spec.frame {
        match &mut frame.bounds {
            FrameBounds::Single(start) => derive_frame_bound_types(start, type_report)?,
            FrameBounds::Between(start, end) => {
                derive_frame_bound_types(start, type_report)?;
                derive_frame_bound_types(end, type_report)?;
            }
        }
    }
    Ok(())
}

fn derive_frame_bound_types(
    bound: &mut FrameBound<'_, Analyzed>,
    type_report: &AstTypeReport,
) -> AstResult<()> {
    match bound {
        FrameBound::Preceding(offset) | FrameBound::Following(offset) => {
            offset.derive_types(type_report)
        }
        FrameBound::UnboundedPreceding
        | FrameBound::CurrentRow
        | FrameBound::UnboundedFollowing => Ok(()),
    }
}
