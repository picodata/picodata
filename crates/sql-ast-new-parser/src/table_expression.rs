//! Parsing of everything a `SELECT` says after its select list.

use pest::iterators::Pair;
use smol_str::format_smolstr;

use crate::expr::parse_expr;
use crate::multiset::parse_multiset;
use crate::window::parse_named_window;
use crate::{failed_parsing_error, parse_invariant_error, AstResult};
use crate::{unexpected_rule_error, ExpectedRules, ParseCtx};
use sql_ast_new_grammar::Rule;
use sql_ast_new_nodes::expr::{Expr, RawColumnRef};
use sql_ast_new_nodes::table_expression::{
    From, FromEntry, JoinKind, JoinUsingColumn, JoinedTable, TableExpression, TableFactor,
    TableFactorInner,
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
    let mut group_by = Vec::new();
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
                let col_ref = RawColumnRef::new(None, Ident::from_sql(pair.as_str()));
                using_cols.push(JoinUsingColumn(col_ref));
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

    if !matches!(kind, JoinKind::Cross) && condition.is_none() && using_cols.is_empty() {
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

fn parse_group_by<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<Vec<Expr<'q, Raw>>> {
    debug_assert_eq!(pair.as_rule(), Rule::GroupBy);

    let mut group_by = Vec::<Expr<'q, Raw>>::new();
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::Expr => {
                group_by.push(parse_expr(pair, ctx)?);
            }
            _ => return Err(unexpected_rule_error(ExpectedRules(&[Rule::Expr]), &pair)),
        }
    }

    Ok(group_by)
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
    fn join_cross() {
        let query = r#"FROM t1 CROSS JOIN t2"#;
        insta::assert_snapshot!(render_table_expr(query), @"FROM t1 CROSS JOIN t2");
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
