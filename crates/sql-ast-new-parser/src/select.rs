//! Parsing of `SELECT`: pest pairs -> `SelectStmt<Raw>`.

use pest::iterators::Pair;
use smol_str::format_smolstr;

use crate::expr::parse_expr;
use crate::pairs_traversal::Tree;
use crate::table_expression::parse_table_expression;
use crate::{failed_parsing_error, parse_invariant_error};
use crate::{unexpected_rule_error, ExpectedRules, ParseCtx};
use sql_ast_new_grammar::Rule;
use sql_ast_new_nodes::error::AstResult;
use sql_ast_new_nodes::select::{
    ProjectionExpr, SelectList, SelectListElem, SelectListExprs, SelectStmt,
};
use sql_ast_new_nodes::table_expression::TableExpression;
use sql_ast_new_nodes::{Ident, Raw};
use sql_ir::errors::{Entity, SbroadError};

pub(super) fn parse_select_stmt<'q>(
    pair: Pair<'q, Rule>,
    ctx: &ParseCtx,
) -> AstResult<SelectStmt<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::SelectStmt);
    let mut select_list: Option<SelectList<'q, Raw>> = None;
    let mut table_expression: Option<TableExpression<'q, Raw>> = None;

    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::SelectStmt => {}
            Rule::SelectList => {
                select_list = Some(parse_select_list(pair, ctx)?);
            }
            Rule::TableExpression => {
                table_expression = Some(parse_table_expression(pair, ctx)?);
            }
            _ => {
                return Err(unexpected_rule_error(
                    ExpectedRules(&[Rule::SelectList, Rule::TableExpression]),
                    &pair,
                ))
            }
        }
    }

    let select = SelectStmt::from_parts(
        select_list.ok_or_else(|| {
            parse_invariant_error(format_smolstr!("expected non empty select list"))
        })?,
        table_expression,
    );

    if select.has_asterisk() && !select.has_table_expression() {
        return Err(SbroadError::Invalid(
            Entity::Query,
            Some(format_smolstr!(
                "cannot use asterisk '*' in select list without table expression"
            )),
        )
        .into());
    }

    Ok(select)
}

/// The flat scan leans on grammar order: an [`Identifier`](Rule::Identifier) can only be the
/// alias trailing the expression it names, so it is attached to the element
/// parsed just before it. Expressions, asterisks and nested statements are
/// excluded from descent — they are consumed whole by their own parsers.
fn parse_select_list<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<SelectList<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::SelectList);

    let parse_tree = Tree::from_pair_with_gothru_filter(pair, |pair| {
        !matches!(
            pair.as_rule(),
            Rule::MultisetStmt | Rule::Expr | Rule::Asterisk
        )
    });

    let mut is_distinct = false;
    let mut elements = SelectListExprs::<'q, Raw>(vec![]);

    for pair in parse_tree {
        match pair.as_rule() {
            Rule::SelectList | Rule::Column => {}
            Rule::Distinct => is_distinct = true,
            Rule::Asterisk => {
                if let Some(ident_pair) = pair.into_inner().next() {
                    elements
                        .0
                        .push(SelectListElem::Asterisk(Some(Ident::from_sql(
                            ident_pair.as_str(),
                        ))));
                } else {
                    elements.0.push(SelectListElem::Asterisk(None));
                }
            }
            Rule::Expr => {
                let expr = parse_expr(pair, ctx)?;
                elements
                    .0
                    .push(SelectListElem::Expr(ProjectionExpr::new(expr, None)));
            }
            Rule::Identifier => {
                let Some(SelectListElem::Expr(last_expr)) = elements.0.last_mut() else {
                    return Err(failed_parsing_error(format_smolstr!(
                        "expected `SelectListElem::Expr` for setting alias"
                    )));
                };
                last_expr.alias = Some(Ident::from_sql(pair.as_str()));
            }
            _ => {
                return Err(failed_parsing_error(
                    format_smolstr!(
                        "expected `Rule`s: `SelectList`/`Distinct`/`Asterisk`/`Expr`/`Identifier`, got: {:?}",
                        pair.as_rule()
                    )
                ));
            }
        }
    }

    Ok(SelectList::from_parts(elements, is_distinct))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pest::Parser;
    use sql_ast_new_grammar::PairParser;

    use crate::test_support::round_trip;

    fn parse_select(query: &str) -> AstResult<SelectStmt<'_, Raw>> {
        let pair = PairParser::parse(Rule::SelectStmt, query)
            .expect("expected SelectStmt to parse")
            .next()
            .expect("expected SelectStmt pair");
        let ctx = ParseCtx::new(&pair)?;
        parse_select_stmt(pair, &ctx)
    }

    /// Render `query`, and hold the rendering to the round trip in
    /// [`crate::test_support::round_trip`].
    #[track_caller]
    fn render_select(query: &str) -> String {
        round_trip(query, |q| parse_select(q).map(|stmt| stmt.to_string()))
    }

    #[test]
    fn over_without_space_empty() {
        let query = r#"SELECT sum(a) OVER() FROM t"#;
        insta::assert_snapshot!(render_select(query), @"SELECT sum(a) OVER () FROM t");
    }

    #[test]
    fn over_without_space() {
        let query = r#"SELECT sum(a) OVER(PARTITION BY b) FROM t"#;
        insta::assert_snapshot!(render_select(query), @"SELECT sum(a) OVER (PARTITION BY b) FROM t");
    }

    // Keyword-boundary cases (`SELECT distinct_x`, `count(*) overw`,
    // `a::int arrayx`, ...) live in `crate::keywords`, which sweeps the whole
    // keyword set rather than the few forms this module happens to touch.
}
