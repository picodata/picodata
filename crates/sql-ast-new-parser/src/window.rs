//! Parsing of window functions and `WINDOW` clause definitions.

use pest::iterators::Pair;
use smol_str::format_smolstr;
use sql_ir::ir::aggregates::AggregateKind;

use crate::expr::parse_expr;
use crate::invalid_expression_error;
use crate::multiset::parse_order_by_element;
use crate::parse_invariant_error;
use crate::ParseCtx;
use sql_ast_new_grammar::Rule;
use sql_ast_new_nodes::error::AstResult;
use sql_ast_new_nodes::window::{
    FrameBound, FrameBounds, FrameType, NamedWindow, WindowFrame, WindowFunction,
    WindowFunctionArgs, WindowRef, WindowSpec,
};
use sql_ast_new_nodes::{Ident, Raw};
use sql_ir::errors::{Entity, SbroadError};

pub(super) fn parse_named_window<'q>(
    pair: Pair<'q, Rule>,
    ctx: &ParseCtx,
) -> AstResult<NamedWindow<'q, Raw>> {
    debug_assert_eq!(Rule::WindowDef, pair.as_rule());

    let mut pairs = pair.into_inner();

    let name_pair = pairs
        .next()
        .ok_or_else(|| parse_invariant_error(format_smolstr!("`WindowDef` must contain a name")))?;
    debug_assert_eq!(name_pair.as_rule(), Rule::Identifier);

    let body_pair = pairs.next().ok_or_else(|| {
        parse_invariant_error(format_smolstr!("`WindowDef` must contain `WindowBody`"))
    })?;
    let spec = parse_window_spec(body_pair, ctx)?;

    Ok(NamedWindow {
        name: Ident::from_sql(name_pair.as_str()),
        spec,
    })
}

pub(super) fn parse_window_function<'q>(
    pair: Pair<'q, Rule>,
    ctx: &ParseCtx,
) -> AstResult<WindowFunction<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::Over);

    let mut pairs = pair.into_inner();

    let name = {
        let pair = pairs.next().ok_or_else(|| {
            parse_invariant_error(format_smolstr!("`Over` must contain a function name"))
        })?;
        debug_assert_eq!(pair.as_rule(), Rule::Identifier);
        Ident::from_sql(pair.as_str())
    };

    let args = {
        let args_pair = pairs.next().ok_or_else(|| {
            parse_invariant_error(format_smolstr!("`Over` must contain `WindowFunctionArgs`"))
        })?;
        parse_window_function_args(args_pair, ctx)?
    };

    if matches!(args, WindowFunctionArgs::Asterisk)
        && name.as_str() != AggregateKind::COUNT.to_string()
    {
        return Err(invalid_expression_error(format_smolstr!(
            "function {}(*) does not exist",
            name.as_str()
        )));
    }

    let (filter, window_pair) = {
        let pair = pairs.next().ok_or_else(|| {
            parse_invariant_error(format_smolstr!("`Over` must contain a window"))
        })?;
        if matches!(pair.as_rule(), Rule::WindowFunctionFilter) {
            let filter_expr_pair = pair.into_inner().next().ok_or_else(|| {
                parse_invariant_error(format_smolstr!(
                    "`WindowFunctionFilter` must contain `Expr`"
                ))
            })?;
            (
                Some(parse_expr(filter_expr_pair, ctx)?),
                pairs.next().ok_or_else(|| {
                    parse_invariant_error(format_smolstr!("`Over` must contain a window"))
                })?,
            )
        } else {
            (None, pair)
        }
    };

    let window = match window_pair.as_rule() {
        Rule::Identifier => WindowRef::Name(Ident::from_sql(window_pair.as_str())),
        Rule::WindowBody => WindowRef::Spec(parse_window_spec(window_pair, ctx)?),
        rule => {
            return Err(parse_invariant_error(format_smolstr!(
                "expected `Identifier` or `WindowBody`, got {rule:?}"
            )))
        }
    };

    Ok(WindowFunction {
        name,
        args,
        filter,
        window,
    })
}

fn parse_window_function_args<'q>(
    pair: Pair<'q, Rule>,
    ctx: &ParseCtx,
) -> AstResult<WindowFunctionArgs<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::WindowFunctionArgs);

    let Some(pair) = pair.into_inner().next() else {
        return Ok(WindowFunctionArgs::List(Vec::new()));
    };

    match pair.as_rule() {
        Rule::CountAsterisk => Ok(WindowFunctionArgs::Asterisk),
        Rule::WindowFunctionArgsInner => {
            let mut exprs = Vec::new();
            for expr_pair in pair.into_inner() {
                exprs.push(parse_expr(expr_pair, ctx)?);
            }
            Ok(WindowFunctionArgs::List(exprs))
        }
        rule => Err(parse_invariant_error(format_smolstr!(
            "expected `Asterisk` or `WindowFunctionArgsInner`, got {rule:?}"
        ))),
    }
}

fn parse_window_spec<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<WindowSpec<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::WindowBody);

    let mut spec = WindowSpec::default();

    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::BaseWindowName => spec.base = Some(Ident::from_sql(pair.as_str())),
            Rule::WindowPartition => {
                for expr_pair in pair.into_inner() {
                    spec.partition_by.push(parse_expr(expr_pair, ctx)?);
                }
            }
            Rule::WindowOrderBy => {
                for elem_pair in pair.into_inner() {
                    spec.order_by.push(parse_order_by_element(elem_pair, ctx)?);
                }
            }
            Rule::WindowFrame => spec.frame = Some(parse_window_frame(pair, ctx)?),
            rule => {
                return Err(parse_invariant_error(format_smolstr!(
                    "expected `WindowBody` components, got {rule:?}"
                )))
            }
        }
    }

    // PostgreSQL semantics: a window that inherits from a base window copies the
    // partitioning of the base and cannot define its own.
    if let Some(base) = spec.base.as_ref().map(Ident::as_str) {
        if !spec.partition_by.is_empty() {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format_smolstr!(
                    "cannot override PARTITION BY clause of base window {base}"
                )),
            )
            .into());
        }
    }

    Ok(spec)
}

fn parse_window_frame<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<WindowFrame<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::WindowFrame);

    let mut pairs = pair.into_inner();

    let ty_pair = pairs.next().ok_or_else(|| {
        parse_invariant_error(format_smolstr!("`WindowFrame` must contain frame type"))
    })?;
    let ty = match ty_pair.as_rule() {
        Rule::WindowFrameTypeRows => FrameType::Rows,
        Rule::WindowFrameTypeRange => FrameType::Range,
        rule => {
            return Err(parse_invariant_error(format_smolstr!(
                "expected frame type, got {rule:?}"
            )))
        }
    };

    let bounds_pair = pairs.next().ok_or_else(|| {
        parse_invariant_error(format_smolstr!("`WindowFrame` must contain bounds"))
    })?;
    let bounds = match bounds_pair.as_rule() {
        Rule::WindowFrameSingle => {
            let bound_pair = bounds_pair.into_inner().next().ok_or_else(|| {
                parse_invariant_error(format_smolstr!("`WindowFrameSingle` must contain a bound"))
            })?;
            FrameBounds::Single(parse_frame_bound(bound_pair, ctx)?)
        }
        Rule::WindowFrameBetween => {
            let mut bound_pairs = bounds_pair.into_inner();
            let from_pair = bound_pairs.next().ok_or_else(|| {
                parse_invariant_error(format_smolstr!(
                    "`WindowFrameBetween` must contain an opening bound"
                ))
            })?;
            let to_pair = bound_pairs.next().ok_or_else(|| {
                parse_invariant_error(format_smolstr!(
                    "`WindowFrameBetween` must contain a closing bound"
                ))
            })?;
            FrameBounds::Between(
                parse_frame_bound(from_pair, ctx)?,
                parse_frame_bound(to_pair, ctx)?,
            )
        }
        rule => {
            return Err(parse_invariant_error(format_smolstr!(
                "expected `WindowFrameSingle` or `WindowFrameBetween`, got {rule:?}"
            )))
        }
    };

    debug_assert!(
        pairs.next().is_none(),
        "`WindowFrame` must contain only frame type and bounds"
    );

    Ok(WindowFrame { ty, bounds })
}

fn parse_frame_bound<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<FrameBound<'q, Raw>> {
    let offset_expr = |pair: Pair<'q, Rule>| {
        let expr_pair = pair.into_inner().next().ok_or_else(|| {
            parse_invariant_error(format_smolstr!("offset bound must contain `Expr`"))
        })?;
        parse_expr(expr_pair, ctx)
    };

    match pair.as_rule() {
        Rule::PrecedingUnbounded => Ok(FrameBound::UnboundedPreceding),
        Rule::PrecedingOffset => Ok(FrameBound::Preceding(offset_expr(pair)?)),
        Rule::CurrentRow => Ok(FrameBound::CurrentRow),
        Rule::FollowingOffset => Ok(FrameBound::Following(offset_expr(pair)?)),
        Rule::FollowingUnbounded => Ok(FrameBound::UnboundedFollowing),
        rule => Err(parse_invariant_error(format_smolstr!(
            "expected frame bound, got {rule:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pest::Parser;
    use sql_ast_new_grammar::PairParser;

    use crate::test_support::round_trip;

    /// Neither `Over` nor `WindowDef` is anchored by `SOI`/`EOF` (only `Command`
    /// is), so pest is happy to match a prefix of the query and drop the rest.
    /// In a real query the enclosing rule fails on the unconsumed tail; emulate
    /// that here, the way `table_expression`'s helper does. Without it the round
    /// trip is toothless: a rendering with an unparseable tail would re-parse
    /// from its prefix and compare equal.
    #[track_caller]
    fn assert_fully_consumed(query: &str, pair: &Pair<'_, Rule>) {
        assert_eq!(
            pair.as_span().end(),
            query.trim_end().len(),
            "grammar consumed only `{}`",
            pair.as_str()
        );
    }

    fn parse_window_func(query: &str) -> AstResult<WindowFunction<'_, Raw>> {
        let pair = PairParser::parse(Rule::Over, query)
            .expect("expected expression to parse")
            .next()
            .expect("expected expression pair");
        assert_fully_consumed(query, &pair);
        let ctx = ParseCtx::new(&pair)?;
        parse_window_function(pair, &ctx)
    }

    fn parse_window_def(query: &str) -> AstResult<NamedWindow<'_, Raw>> {
        let pair = PairParser::parse(Rule::WindowDef, query)
            .expect("expected window def to parse")
            .next()
            .expect("expected window def pair");
        assert_fully_consumed(query, &pair);
        let ctx = ParseCtx::new(&pair)?;
        parse_named_window(pair, &ctx)
    }

    /// Render `query`, and hold the rendering to the round trip in
    /// [`crate::test_support::round_trip`].
    #[track_caller]
    fn render_window_func(query: &str) -> String {
        round_trip(query, |q| parse_window_func(q).map(|f| f.to_string()))
    }

    /// Render `query`, and hold the rendering to the round trip in
    /// [`crate::test_support::round_trip`].
    #[track_caller]
    fn render_named_window(query: &str) -> String {
        round_trip(query, |q| {
            parse_window_def(q).map(|named| named.to_string())
        })
    }

    #[test]
    fn over_empty_window() {
        insta::assert_snapshot!(render_window_func("row_number() OVER ()"), @"row_number() OVER ()");
    }

    #[test]
    fn over_named_window() {
        insta::assert_snapshot!(render_window_func("rank() OVER w"), @"rank() OVER w");
    }

    #[test]
    fn over_partition_by() {
        insta::assert_snapshot!(
            render_window_func("sum(a) OVER (PARTITION BY b, c)"),
            @"sum(a) OVER (PARTITION BY b, c)"
        );
    }

    #[test]
    fn over_filter_and_order_by() {
        insta::assert_snapshot!(
            render_window_func("count(*) FILTER (WHERE a > 0) OVER (ORDER BY b DESC NULLS LAST)"),
            @"count(*) FILTER (WHERE a > 0) OVER (ORDER BY b DESC NULLS LAST)"
        );
    }

    #[test]
    fn over_rows_between_frame() {
        insta::assert_snapshot!(
            render_window_func("avg(a) OVER (PARTITION BY b ORDER BY c ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"),
            @"avg(a) OVER (PARTITION BY b ORDER BY c ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"
        );
    }

    #[test]
    fn over_rows_following_frame() {
        insta::assert_snapshot!(
            render_window_func("lag(a, 1) OVER (ORDER BY b ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)"),
            @"lag(a, 1) OVER (ORDER BY b ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)"
        );
    }

    #[test]
    fn over_base_window_inheritance() {
        insta::assert_snapshot!(
            render_window_func("sum(a) OVER (w ORDER BY b RANGE UNBOUNDED PRECEDING)"),
            @"sum(a) OVER (w ORDER BY b ASC RANGE UNBOUNDED PRECEDING)"
        );
    }

    #[test]
    fn over_single_bound_frame() {
        insta::assert_snapshot!(
            render_window_func("sum(a) OVER (ORDER BY b ROWS 2 PRECEDING)"),
            @"sum(a) OVER (ORDER BY b ASC ROWS 2 PRECEDING)"
        );
    }

    #[test]
    fn over_order_by_multiple_elements() {
        insta::assert_snapshot!(
            render_window_func("sum(a) OVER (ORDER BY b, c DESC)"),
            @"sum(a) OVER (ORDER BY b ASC, c DESC)"
        );
    }

    #[test]
    fn over_following_offset_bound() {
        insta::assert_snapshot!(
            render_window_func("sum(a) OVER (ORDER BY b ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)"),
            @"sum(a) OVER (ORDER BY b ASC ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)"
        );
    }

    #[test]
    fn over_range_between_unbounded() {
        insta::assert_snapshot!(
            render_window_func("sum(a) OVER (ORDER BY b RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"),
            @"sum(a) OVER (ORDER BY b ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
        );
    }

    #[test]
    fn named_window_definition() {
        insta::assert_snapshot!(
            render_named_window("w AS (PARTITION BY a ORDER BY b DESC)"),
            @"w AS (PARTITION BY a ORDER BY b DESC)"
        );
    }

    #[test]
    fn base_window_cannot_override_partition() {
        let err = parse_window_func("sum(a) OVER (w PARTITION BY b)")
            .err()
            .expect("expected failed parsing");
        insta::assert_snapshot!(
            err.to_string(),
            @"invalid expression: cannot override PARTITION BY clause of base window w"
        );
    }
}
