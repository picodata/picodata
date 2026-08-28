//! Parsing of everything a `SELECT` says after its select list.

use pest::iterators::Pair;
use smol_str::format_smolstr;

use crate::expr::parse_expr;
use crate::multiset::parse_multiset;
use crate::window::parse_named_window;
use crate::{failed_parsing_error, invalid_expression_error, parse_invariant_error, AstResult};
use crate::{unexpected_rule_error, ExpectedRules, ParseCtx};
use sql_ast_new_grammar::Rule;
use sql_ast_new_nodes::expr::{Expr, ExprInner, Literal, LiteralKind, RawVar, UnaryOp};
use sql_ast_new_nodes::table_expression::{
    From, FromEntry, GroupBy, JoinKind, JoinUsingColumn, JoinedTable, OrdrByGrpByElem,
    TableExpression, TableFactor, TableFactorInner,
};
use sql_ast_new_nodes::window::NamedWindow;
use sql_ast_new_nodes::{Ident, Raw};
use sql_ir::errors::{Entity, SbroadError};

pub(super) fn parse_table_expression<'q>(
    pair: Pair<'q, Rule>,
    ctx: &ParseCtx,
) -> AstResult<TableExpression<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::TableExpression);

    let mut from = None;
    let mut selection = None;
    let mut group_by = GroupBy::empty();
    let mut having = None;
    let mut windows = Vec::<NamedWindow<'q, Raw>>::new();

    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::From => from = Some(parse_from(pair, ctx)?),
            Rule::Selection => selection = Some(parse_selection(pair, ctx)?),
            Rule::GroupBy => group_by = parse_group_by(pair, ctx)?,
            Rule::Having => having = Some(parse_having(pair, ctx)?),
            Rule::NamedWindows => {
                windows = pair
                    .into_inner()
                    .map(|pair| parse_named_window(pair, ctx))
                    .collect::<Result<Vec<_>, _>>()?;
            }
            _ => {
                return Err(unexpected_rule_error(
                    ExpectedRules(&[
                        Rule::From,
                        Rule::Selection,
                        Rule::GroupBy,
                        Rule::Having,
                        Rule::NamedWindows,
                    ]),
                    &pair,
                ));
            }
        }
    }

    Ok(TableExpression::<'q, Raw>::from_parts(
        from.ok_or_else(|| {
            parse_invariant_error(format_smolstr!(
                "grammar guarantees a FROM clause in a table expression"
            ))
        })?,
        selection,
        group_by,
        having,
        windows,
    ))
}

fn parse_from<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<From<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::From);

    let mut tbl_factors = Vec::new();

    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::TableFactor => {
                tbl_factors.push(FromEntry::TableFactor(parse_table_factor(pair, ctx)?))
            }
            Rule::JoinedTable => {
                tbl_factors.push(FromEntry::JoinedTable(parse_joined_table(pair, ctx)?))
            }
            _ => {
                return Err(unexpected_rule_error(
                    ExpectedRules(&[Rule::TableFactor, Rule::JoinedTable]),
                    &pair,
                ))
            }
        }
    }

    Ok(From::from_tbl_factors(tbl_factors))
}

fn parse_table_factor<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<TableFactor<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::TableFactor);

    let mut inner = Option::<TableFactorInner<'q, Raw>>::default();
    let mut alias = Option::<Ident>::default();
    let mut indexed_by = Option::<Ident>::default();

    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::IndexedByExpr => {
                if indexed_by.is_some() {
                    return Err(failed_parsing_error(format_smolstr!(
                        "cannot have multiple indexed by exprs"
                    )));
                }
                let inner_pairs = pair.into_inner();
                let idx_name_pair = inner_pairs.peek().ok_or_else(|| {
                    parse_invariant_error(format_smolstr!("expected to find index name"))
                })?;
                debug_assert_eq!(idx_name_pair.as_rule(), Rule::Identifier);

                indexed_by = Some(Ident::from_sql(idx_name_pair.as_str()));

                debug_assert!(inner_pairs.count() == 1)
            }
            Rule::CteOrTable => {
                inner = Some(TableFactorInner::CteOrTable(Ident::from_sql(pair.as_str())));
            }
            Rule::MultisetStmt => {
                inner = Some(TableFactorInner::SubQuery(Box::new(parse_multiset(
                    pair, ctx,
                )?)));
            }
            Rule::Identifier => {
                if indexed_by.is_some() {
                    return Err(failed_parsing_error(format_smolstr!(
                        "cannot have indexed by clause before alias"
                    )));
                }
                alias = Some(Ident::from_sql(pair.as_str()));
            }
            _ => {
                return Err(unexpected_rule_error(
                    ExpectedRules(&[
                        Rule::IndexedByExpr,
                        Rule::CteOrTable,
                        Rule::MultisetStmt,
                        Rule::Identifier,
                    ]),
                    &pair,
                ));
            }
        }
    }

    Ok(TableFactor {
        inner: inner.ok_or_else(|| {
            parse_invariant_error(format_smolstr!("grammar guarantees a table factor source"))
        })?,
        alias,
        indexed_by,
    })
}

/// The grammar accepts any join shape. The semantic rules — CROSS JOIN takes
/// no condition, INNER/LEFT require ON or USING — are enforced here to give
/// targeted errors instead of opaque grammar failures.
fn parse_joined_table<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<JoinedTable<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::JoinedTable);

    let mut kind = JoinKind::Inner;
    let mut table = None;
    let mut condition = None;
    let mut using_cols = Vec::<JoinUsingColumn>::new();

    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::InnerJoin => kind = JoinKind::Inner,
            Rule::LeftJoin => kind = JoinKind::Left,
            Rule::CrossJoin => kind = JoinKind::Cross,
            Rule::TableFactor => table = Some(parse_table_factor(pair, ctx)?),
            Rule::Expr => {
                if matches!(kind, JoinKind::Cross) {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format_smolstr!(
                            "cannot use join condition with `CROSS JOIN`"
                        )),
                    )
                    .into());
                }
                condition = Some(parse_expr(pair, ctx)?);
            }
            Rule::Identifier => {
                if matches!(kind, JoinKind::Cross) {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format_smolstr!(
                            "cannot use join using condition with `CROSS JOIN`"
                        )),
                    )
                    .into());
                }
                let var = RawVar::new(None, Ident::from_sql(pair.as_str()));
                using_cols.push(JoinUsingColumn(var));
            }
            _ => {
                return Err(unexpected_rule_error(
                    ExpectedRules(&[
                        Rule::InnerJoin,
                        Rule::LeftJoin,
                        Rule::CrossJoin,
                        Rule::TableFactor,
                        Rule::Expr,
                        Rule::Identifier,
                    ]),
                    &pair,
                ));
            }
        }
    }

    let table = table.ok_or_else(|| {
        parse_invariant_error(format_smolstr!("grammar guarantees a joined table factor"))
    })?;

    if matches!(kind, JoinKind::Cross) {
        return Err(SbroadError::Invalid(
            Entity::Query,
            Some(format_smolstr!("CROSS JOIN is not supported yet")),
        )
        .into());
    }

    if condition.is_none() && using_cols.is_empty() {
        return Err(SbroadError::Invalid(
            Entity::Query,
            Some(format_smolstr!(
                "cannot use `INNER/LEFT OUTER JOIN` without `ON <condition>` and `USING (<column_list>)`"
            )),
        )
        .into());
    }

    Ok(JoinedTable {
        kind,
        table,
        condition,
        using_cols,
    })
}

fn parse_selection<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<Expr<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::Selection);

    let pair = pair.into_inner().next().ok_or_else(|| {
        parse_invariant_error(format_smolstr!("single `Expr` expected inside `Selection`"))
    })?;
    match pair.as_rule() {
        Rule::Expr => parse_expr(pair, ctx),
        _ => Err(unexpected_rule_error(ExpectedRules(&[Rule::Expr]), &pair)),
    }
}

/// Which clause an element with select-list ordinals is parsed for. It names
/// the clause in the rejections. The two clauses also differ in what a
/// parenthesized list is, but that is settled before an element gets here -
/// see [`parse_group_by_elem`].
#[derive(Clone, Copy)]
pub(crate) enum OrdinalClause {
    GroupBy,
    OrderBy,
}

impl OrdinalClause {
    fn name(self) -> &'static str {
        match self {
            Self::GroupBy => "GROUP BY",
            Self::OrderBy => "ORDER BY",
        }
    }
}

/// One GROUP BY element. `(a, b)` is a grouping *set* here. It groups by `a`
/// and by `b`, so the row is flattened into its fields and each one is read as
/// its own element, ordinals included. Only the implicit-row form is.
/// An explicit `ROW(a, b)` parses as a call and stays a single expression.
///
/// ORDER BY does not flatten. There `(1, b)` is a row value the query sorts
/// by - rows are compared field by field, the constant `1` first - and folding
/// its fields into `ORDER BY 1, b` would sort by the first output column
/// instead. So an ORDER BY element goes straight to [`parse_ordrby_grpby_elem`].
fn parse_group_by_elem<'q>(
    expr: Expr<'q, Raw>,
    elems: &mut Vec<OrdrByGrpByElem<'q, Raw>>,
) -> AstResult<()> {
    if matches!(expr.inner_ref(), ExprInner::Row(_)) {
        let ExprInner::Row(row) = expr.into() else {
            return Err(parse_invariant_error(format_smolstr!(
                "`Row` expression expected"
            )));
        };
        for value in row.into_parts() {
            parse_group_by_elem(value, elems)?;
        }
        return Ok(());
    }
    elems.push(parse_ordrby_grpby_elem(expr, OrdinalClause::GroupBy)?);
    Ok(())
}

/// One GROUP BY / ORDER BY element. A bare integer constant is a 1-based
/// select-list position, any other bare constant is rejected, and everything
/// else is an ordinary expression.
pub(crate) fn parse_ordrby_grpby_elem<'q>(
    expr: Expr<'q, Raw>,
    clause: OrdinalClause,
) -> AstResult<OrdrByGrpByElem<'q, Raw>> {
    // Non-int constant in GROUP BY / ORDER BY element error.
    let non_integer_constant =
        || invalid_expression_error(format_smolstr!("non-integer constant in {}", clause.name()));

    // Whether an element is a position.
    let mut negations = 0usize;
    let mut operand = expr.inner_ref();
    while let ExprInner::UnaryOperation(unary_operation) = operand {
        let (inner, operator) = unary_operation.parts_ref();
        if !matches!(operator, UnaryOp::Minus) {
            break;
        }
        negations += 1;
        operand = inner.inner_ref();
    }
    // Only a numeric constant is folded: `- 'a'` and `- true` stay operators, and
    // so are ordinary expressions rather than a rejected non-integer position.
    let folded = match operand {
        ExprInner::Literal(literal @ Literal { kind, value, .. })
            if !value.starts_with('+')
                && (negations == 0
                    || matches!(
                        kind,
                        LiteralKind::Integer | LiteralKind::Numeric | LiteralKind::Double
                    )) =>
        {
            Some(literal)
        }
        _ => None,
    };

    let elem = if let Some(Literal { value, kind, .. }) = folded {
        // Deny non-integer constant.
        if !matches!(kind, LiteralKind::Integer) {
            return Err(non_integer_constant());
        }

        // Verify it is valid integer number.
        let pos = value.parse::<i32>().map_err(|_| non_integer_constant())?;
        let pos = if negations % 2 == 1 {
            pos.checked_neg().ok_or_else(non_integer_constant)?
        } else {
            pos
        };
        if pos <= 0 {
            return Err(invalid_expression_error(format_smolstr!(
                "{} position {pos} is not in select list",
                clause.name()
            )));
        }
        OrdrByGrpByElem::Ordinal((pos - 1) as usize)
    } else {
        // Everything the fold did not reduce to a bare numeric constant: a real
        // expression, a plus-signed literal, or a minus over a non-numeric one.
        OrdrByGrpByElem::Expr(expr)
    };

    Ok(elem)
}

fn parse_group_by<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<GroupBy<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::GroupBy);

    let mut elems = Vec::<OrdrByGrpByElem<'q, Raw>>::new();
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::Expr => parse_group_by_elem(parse_expr(pair, ctx)?, &mut elems)?,
            _ => return Err(unexpected_rule_error(ExpectedRules(&[Rule::Expr]), &pair)),
        }
    }

    Ok(GroupBy(elems))
}

fn parse_having<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<Expr<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::Having);

    let pair = pair.into_inner().next().ok_or_else(|| {
        parse_invariant_error(format_smolstr!("single `Expr` expected inside `Having`"))
    })?;
    match pair.as_rule() {
        Rule::Expr => parse_expr(pair, ctx),
        _ => Err(unexpected_rule_error(ExpectedRules(&[Rule::Expr]), &pair)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pest::Parser;
    use sql_ast_new_grammar::PairParser;

    use crate::test_support::round_trip;

    fn parse_table_expr(query: &str) -> AstResult<TableExpression<'_, Raw>> {
        let pair = PairParser::parse(Rule::TableExpression, query)
            .expect("expected TableExpression to parse")
            .next()
            .expect("expected TableExpression pair");
        // `TableExpression` is not anchored by `SOI`/`EOF` (only `Command` is), so
        // pest is happy to match a prefix of the query and drop the rest. Catch that
        // here: an unconsumed tail means the grammar rejected the query, and asserting
        // on the AST built from the prefix would silently test something else.
        assert_eq!(
            pair.as_span().end(),
            query.trim_end().len(),
            "grammar consumed only `{}`",
            pair.as_str()
        );
        let ctx = ParseCtx::new(&pair)?;
        parse_table_expression(pair, &ctx)
    }

    /// Render `query`, and hold the rendering to the round trip in
    /// [`crate::test_support::round_trip`].
    #[track_caller]
    fn render_table_expr(query: &str) -> String {
        round_trip(query, |q| parse_table_expr(q).map(|stmt| stmt.to_string()))
    }

    #[test]
    fn where_simple() {
        let query = r#"FROM t1 WHERE t1.a > 5 AND t1.a < 10"#;
        insta::assert_snapshot!(render_table_expr(query), @"FROM t1 WHERE t1.a > 5 AND t1.a < 10");
    }

    #[test]
    fn where_with_scalar_subquery() {
        let query =
            r#"FROM t1 WHERE ((SELECT a FROM t2 WHERE t2.a % 1 = 10 ORDER BY a DESC LIMIT 1) > 5)"#;
        insta::assert_snapshot!(
            render_table_expr(query),
            @"FROM t1 WHERE (SELECT a FROM t2 WHERE t2.a % 1 = 10 ORDER BY a DESC LIMIT 1) > 5"
        );
    }

    #[test]
    fn join_inner() {
        let query = r#"FROM t1 INNER JOIN t2 ON t1.a = t2.a"#;
        insta::assert_snapshot!(render_table_expr(query), @"FROM t1 INNER JOIN t2 ON t1.a = t2.a");
    }

    #[test]
    fn join_left_with_using() {
        let query = r#"FROM t1 LEFT JOIN t2 USING (a, b, c)"#;
        insta::assert_snapshot!(
            render_table_expr(query),
            @"FROM t1 LEFT OUTER JOIN t2 USING (a, b, c)"
        );
    }

    #[test]
    fn join_cross_with_cond_fails() {
        let query = r#"FROM t1 CROSS JOIN t2 ON t1.a = t2.a"#;
        let err = parse_table_expr(query)
            .err()
            .expect("expected failed parsing");
        insta::assert_snapshot!(
            err.to_string(),
            @"invalid query: cannot use join condition with `CROSS JOIN`"
        );
    }

    #[test]
    fn join_cross_with_using_fails() {
        let query = r#"FROM t1 CROSS JOIN t2 USING (a, b)"#;
        let err = parse_table_expr(query)
            .err()
            .expect("expected failed parsing");
        insta::assert_snapshot!(
            err.to_string(),
            @"invalid query: cannot use join using condition with `CROSS JOIN`"
        );
    }

    #[test]
    fn join_without_condition_fails() {
        let query = r#"FROM t1 JOIN t2"#;
        let err = parse_table_expr(query)
            .err()
            .expect("expected failed parsing");
        insta::assert_snapshot!(
            err.to_string(),
            @"invalid query: cannot use `INNER/LEFT OUTER JOIN` without `ON <condition>` and `USING (<column_list>)`"
        );
    }

    // TODO: support such queries.
    #[test]
    fn join_with_parens_is_rejected() {
        let query =
            r#"SELECT * FROM t1 LEFT JOIN (t2 INNER JOIN t1 AS t3 ON t2.c = t3.a) ON t1.b = t2.d"#;
        let err_str = PairParser::parse(Rule::Command, query)
            .expect_err("expected a parenthesized join group to be rejected")
            .to_string();
        insta::assert_snapshot!(err_str, @"
         --> 1:29
          |
        1 | SELECT * FROM t1 LEFT JOIN (t2 INNER JOIN t1 AS t3 ON t2.c = t3.a) ON t1.b = t2.d
          |                             ^---
          |
          = expected MultisetInner
        ");
    }

    #[test]
    fn join_inner_multiple() {
        let query = r#"FROM t1 LEFT JOIN t2 t2_1 USING (a, b) JOIN t2 t2_2 USING (a)"#;
        insta::assert_snapshot!(
            render_table_expr(query),
            @"FROM t1 LEFT OUTER JOIN t2 AS t2_1 USING (a, b) INNER JOIN t2 AS t2_2 USING (a)"
        );
    }

    #[test]
    fn group_by_simple() {
        let query = r#"FROM t1 GROUP BY t1.a / 10, a % 10"#;
        insta::assert_snapshot!(render_table_expr(query), @"FROM t1 GROUP BY t1.a / 10, a % 10");
    }

    #[test]
    fn group_by_ordinal() {
        // An integer literal is a select-list position rather than an
        // expression that happens to be constant, and the two are different
        // nodes from here on. Redundant parentheses and leading zeros do not
        // change which one it is.
        let query = r#"FROM t1 GROUP BY 1, ((2)), 03"#;
        insta::assert_snapshot!(render_table_expr(query), @"FROM t1 GROUP BY 1, 2, 3");
    }

    #[test]
    fn group_by_row_is_flattened_into_its_fields() {
        // `(a, b)` is a grouping set: it groups by `a` and by `b`, so the row
        // is read field by field - and a field that is an integer constant is
        // a position like any other element. Only the implicit-row form is
        // flattened; an explicit `ROW(...)` is a call and stays one element.
        let query = r#"FROM t1 GROUP BY (a, b), (1, c % 2), ((d, 2)), ROW(e, f)"#;
        insta::assert_snapshot!(
            render_table_expr(query),
            @"FROM t1 GROUP BY a, b, 1, c % 2, d, 2, row(e, f)"
        );
        // And a field is held to the same rule as a bare element.
        let err = parse_table_expr(r#"FROM t1 GROUP BY (a, 'x')"#)
            .err()
            .expect("expected failed parsing");
        insta::assert_snapshot!(
            err.to_string(),
            @"invalid expression: non-integer constant in GROUP BY"
        );
    }

    #[test]
    fn group_by_position_below_one_is_rejected() {
        // The upper bound needs a select list and is left to the analyzer; the
        // lower one is decided here, where a position is recognised.
        let err = parse_table_expr(r#"FROM t1 GROUP BY 0"#)
            .err()
            .expect("expected failed parsing");
        insta::assert_snapshot!(
            err.to_string(),
            @"invalid expression: GROUP BY position 0 is not in select list"
        );
        let err = parse_table_expr(r#"FROM t1 GROUP BY -1"#)
            .err()
            .expect("expected failed parsing");
        insta::assert_snapshot!(
            err.to_string(),
            @"invalid expression: GROUP BY position -1 is not in select list"
        );
    }

    #[test]
    fn group_by_non_integer_constant_is_rejected() {
        // Only an integer constant means something in GROUP BY - a position.
        // Every other bare constant is rejected here rather than grouped by,
        // because it is almost always a mistyped ordinal or a column name in
        // the wrong quotes. Redundant parentheses do not make it an expression.
        for query in [
            r#"FROM t1 GROUP BY 'x'"#,
            r#"FROM t1 GROUP BY true"#,
            r#"FROM t1 GROUP BY false"#,
            r#"FROM t1 GROUP BY NULL"#,
            r#"FROM t1 GROUP BY 1.0"#,
            r#"FROM t1 GROUP BY 1e1"#,
            r#"FROM t1 GROUP BY -1.0"#,
            r#"FROM t1 GROUP BY (('x'))"#,
            // Rejected wherever it stands in the list, not just first.
            r#"FROM t1 GROUP BY a, 'x'"#,
        ] {
            let err = parse_table_expr(query)
                .err()
                .unwrap_or_else(|| panic!("`{query}` must be rejected"));
            assert_eq!(
                err.to_string(),
                "invalid expression: non-integer constant in GROUP BY",
                "`{query}`"
            );
        }
    }

    #[test]
    fn group_by_folds_a_leading_minus_into_the_position() {
        // A minus in front of a numeric constant belongs to the constant, so
        // the element is still a position. PostgreSQL folds it in the grammar
        // rather than the lexer, which is why nothing between the two matters -
        // whitespace, parentheses, or a second minus. `-1` alone is already one
        // token to this grammar; the rest reach here as unary operations.
        for query in [
            r#"FROM t1 GROUP BY - 1"#,
            r#"FROM t1 GROUP BY -  1"#,
            r#"FROM t1 GROUP BY -(1)"#,
            r#"FROM t1 GROUP BY - (1)"#,
            r#"FROM t1 GROUP BY (- 1)"#,
            r#"FROM t1 GROUP BY ((- 1))"#,
        ] {
            let err = parse_table_expr(query)
                .err()
                .unwrap_or_else(|| panic!("`{query}` must be rejected"));
            assert_eq!(
                err.to_string(),
                "invalid expression: GROUP BY position -1 is not in select list",
                "`{query}`"
            );
        }
        // The fold repeats, so a doubled minus lands back on a valid position.
        let query = r#"FROM t1 GROUP BY - -1, - - 1"#;
        insta::assert_snapshot!(render_table_expr(query), @"FROM t1 GROUP BY 1, 1");
        // And it reaches the non-integer rejection as readily as an integer one.
        let err = parse_table_expr(r#"FROM t1 GROUP BY - 1.0"#)
            .err()
            .expect("expected failed parsing");
        insta::assert_snapshot!(
            err.to_string(),
            @"invalid expression: non-integer constant in GROUP BY"
        );
    }

    #[test]
    fn group_by_never_folds_a_leading_plus() {
        // Unary plus is not folded - it stays an operator, so the element it
        // heads is an ordinary expression. That is what makes `+1` differ from
        // `-1`, and it is also why `+1.0` is not a *rejected* non-integer
        // position: it is not a position at all. This grammar's `Integer` token
        // absorbs an adjacent sign, so `+1` arrives here as a signed literal
        // rather than an operation - the leading `+` in its text stands in for
        // the operator, and a minus over such a literal inherits it.
        let query = r#"FROM t1 GROUP BY +1, + 1, +1.0, +1e0, - +1, + -1"#;
        insta::assert_snapshot!(
            render_table_expr(query),
            @"FROM t1 GROUP BY +1, + 1, +1.0, +1e0, - +1, + -1"
        );
    }

    #[test]
    fn group_by_folds_only_numeric_constants() {
        // A minus over a non-numeric constant is an ordinary operator: it never
        // makes its operand a position, so it never reaches the rejection above
        // either. Type checking of the operation itself is the analyzer's.
        let query = r#"FROM t1 GROUP BY - 'x', - true, - NULL"#;
        insta::assert_snapshot!(
            render_table_expr(query),
            @"FROM t1 GROUP BY - 'x', - true, - NULL"
        );
    }

    #[test]
    fn group_by_constant_operands_stay_expressions() {
        // The rejection above is about a constant that *is* the whole element.
        // A constant inside one is an ordinary operand, and the element is an
        // ordinary expression - including when the expression is itself
        // constant.
        let query = r#"FROM t1 GROUP BY 1 + 1, 'x' || 'y', CAST('x' AS text), $1"#;
        insta::assert_snapshot!(
            render_table_expr(query),
            @"FROM t1 GROUP BY 1 + 1, 'x' || 'y', CAST('x' AS string), $1"
        );
    }

    #[test]
    fn group_by_position_wider_than_a_position_is_rejected() {
        // A literal too wide to be a position is not silently demoted to an
        // ordinary expression - it is the same rejection PostgreSQL gives a
        // constant it cannot read as one.
        let err = parse_table_expr(r#"FROM t1 GROUP BY 99999999999999"#)
            .err()
            .expect("expected failed parsing");
        insta::assert_snapshot!(
            err.to_string(),
            @"invalid expression: non-integer constant in GROUP BY"
        );
    }

    #[test]
    fn having_simple() {
        let query = r#"FROM t1 HAVING a > 5 AND a < 10 OR a % 10 = 5"#;
        insta::assert_snapshot!(
            render_table_expr(query),
            @"FROM t1 HAVING a > 5 AND a < 10 OR a % 10 = 5"
        );
    }

    #[test]
    fn named_windows_with_inheritance() {
        let query = r#"FROM t WINDOW w AS (PARTITION BY a), v AS (w ORDER BY b)"#;
        insta::assert_snapshot!(
            render_table_expr(query),
            @"FROM t WINDOW w AS (PARTITION BY a), v AS (w ORDER BY b ASC)"
        );
    }

    #[test]
    fn named_window_with_frame() {
        let query = r#"FROM t WINDOW w AS (PARTITION BY a ORDER BY b DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"#;
        insta::assert_snapshot!(
            render_table_expr(query),
            @"FROM t WINDOW w AS (PARTITION BY a ORDER BY b DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        );
    }

    #[test]
    fn indexed_by_simple() {
        let query = r#"FROM t INDEXED BY idx"#;
        insta::assert_snapshot!(
            render_table_expr(query),
            @"FROM t INDEXED BY idx"
        );
    }

    #[test]
    fn indexed_by_with_alias() {
        let query = r#"FROM t t1 INDEXED BY idx"#;
        insta::assert_snapshot!(
            render_table_expr(query),
            @"FROM t AS t1 INDEXED BY idx"
        );
    }

    #[test]
    fn indexed_by_with_as_alias() {
        let query = r#"FROM t AS t1 INDEXED BY idx"#;
        insta::assert_snapshot!(
            render_table_expr(query),
            @"FROM t AS t1 INDEXED BY idx"
        );
    }
}
