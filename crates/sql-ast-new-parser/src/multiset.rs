//! Parsing of query expressions: pest pairs -> `MultisetStmt<Raw>`.
//!
//! `UNION`/`EXCEPT` and `INTERSECT` sit on two precedence tiers, so the body is
//! built by a small Pratt parser rather than by grammar nesting — the same
//! technique [`expr`](super::expr) uses for operators.

use pest::iterators::Pair;
use pest::pratt_parser::PrattParser;
use smol_str::format_smolstr;

use crate::expr::{parse_expr, parse_values_row};
use crate::pairs_traversal::Tree;
use crate::select::parse_select_stmt;
use crate::{failed_parsing_error, parse_invariant_error, AstResult};
use crate::{unexpected_rule_error, ExpectedRules, ParseCtx};
use sql_ast_new_grammar::Rule;
use sql_ast_new_nodes::multiset::{
    Cte, Ctes, Limit, MultisetStmt, OpDupElimination, OperationKind, OrderBy, OrderByDirection,
    OrderByElement, OrderByNulls, ValuesStmt,
};
#[cfg(test)]
use sql_ast_new_nodes::multiset::{MultisetInner, Operation};
use sql_ast_new_nodes::{Ident, Raw};
use sql_ir::errors::{Entity, SbroadError};

lazy_static::lazy_static! {
    static ref MULTISET_PRATT_PARSER: PrattParser<Rule> = {
        use pest::pratt_parser::{Assoc::Left, Op};
        use Rule::{ExceptOp, IntersectOp, UnionOp};

        PrattParser::new()
            .op(Op::infix(UnionOp, Left) | Op::infix(ExceptOp, Left))
            .op(Op::infix(IntersectOp, Left))
    };
}

/// Statement-level assembly: leading CTEs, the body, trailing `ORDER BY` / `LIMIT`.
/// A parenthesized statement arrives as a nested [`MultisetStmt`] and
/// is collapsed into this one, so `(SELECT a FROM t LIMIT 3) ORDER BY a`
/// merges into a single statement. Therefore, clause present on
/// both levels (WITH, ORDER BY, LIMIT) is a duplicate and a user error.
pub(super) fn parse_multiset<'q>(
    pair: Pair<'q, Rule>,
    ctx: &ParseCtx,
) -> AstResult<MultisetStmt<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::MultisetStmt);

    let mut pairs = pair.into_inner().peekable();
    let mut ctes = Ctes::default();

    while let Some(pair) = pairs.next_if(|pair| matches!(pair.as_rule(), Rule::Cte)) {
        ctes.add(parse_cte(pair, ctx)?);
    }

    let pair = pairs.next().ok_or_else(|| {
        parse_invariant_error(format_smolstr!(
            "expected `MultisetInner` in `MultisetStmt`"
        ))
    })?;

    let inner = match pair.as_rule() {
        Rule::MultisetInner => {
            let mut parsed = parse_multiset_pratt(pair, ctx)?;
            if !parsed.ctes.is_empty() && !ctes.is_empty() {
                return Err(SbroadError::Invalid(
                    Entity::Cte,
                    Some(format_smolstr!("cannot use multiple WITH")),
                )
                .into());
            } else if ctes.is_empty() {
                std::mem::swap(&mut ctes, &mut parsed.ctes);
            }
            parsed
        }
        _ => {
            return Err(unexpected_rule_error(
                ExpectedRules(&[Rule::MultisetInner]),
                &pair,
            ))
        }
    };

    let mut order_by = None;
    let mut limit = None;
    for pair in pairs {
        match pair.as_rule() {
            Rule::OrderBy => {
                if inner.order_by.is_some() {
                    return Err(failed_parsing_error(format_smolstr!(
                        "multiple ORDER BY clauses not allowed"
                    )));
                }
                order_by = Some(parse_order_by(pair, ctx)?);
            }
            Rule::Limit => {
                if inner.limit.is_some() {
                    return Err(failed_parsing_error(format_smolstr!(
                        "multiple LIMIT clauses not allowed"
                    )));
                }
                limit = Some(parse_limit(pair)?);
            }
            _ => {
                return Err(unexpected_rule_error(
                    ExpectedRules(&[Rule::OrderBy, Rule::Limit]),
                    &pair,
                ));
            }
        }
    }

    Ok(MultisetStmt::from_parts(
        ctes,
        inner.inner,
        order_by.or(inner.order_by),
        limit.or(inner.limit),
    ))
}

fn parse_limit(pair: Pair<'_, Rule>) -> AstResult<Limit> {
    debug_assert_eq!(pair.as_rule(), Rule::Limit);

    let pair = pair.into_inner().next().ok_or_else(|| {
        parse_invariant_error(format_smolstr!("expected limit <value> or limit all"))
    })?;
    match pair.as_rule() {
        Rule::Unsigned => Ok(Limit::Value(pair.as_str().parse::<usize>().map_err(
            |_| {
                SbroadError::Invalid(
                    Entity::Query,
                    Some(format_smolstr!("limit must be unsigned integer")),
                )
            },
        )?)),
        Rule::LimitAll => Ok(Limit::All),
        _ => Err(unexpected_rule_error(
            ExpectedRules(&[Rule::Unsigned, Rule::LimitAll]),
            &pair,
        )),
    }
}

/// Set operations via a Pratt parser: UNION and EXCEPT share one
/// left-associative tier and INTERSECT binds tighter — the SQL-standard precedence.
fn parse_multiset_pratt<'q>(
    pair: Pair<'q, Rule>,
    ctx: &ParseCtx,
) -> AstResult<MultisetStmt<'q, Raw>> {
    MULTISET_PRATT_PARSER
        .map_primary(|pair| parse_multiset_primary(pair, ctx))
        .map_infix(|left, op, right| {
            let (op, dup_elimination) = parse_multiset_op(op)?;
            Ok(MultisetStmt::new_operation(
                left?,
                op,
                dup_elimination,
                right?,
            ))
        })
        .parse(pair.into_inner())
}

fn parse_multiset_primary<'q>(
    pair: Pair<'q, Rule>,
    ctx: &ParseCtx,
) -> AstResult<MultisetStmt<'q, Raw>> {
    match pair.as_rule() {
        Rule::MultisetStmt => parse_multiset(pair, ctx),
        Rule::SelectStmt => parse_select_stmt(pair, ctx).map(MultisetStmt::new_select),
        Rule::ValuesStmt => parse_values(pair, ctx).map(MultisetStmt::new_values),
        _ => Err(unexpected_rule_error(
            ExpectedRules(&[Rule::MultisetStmt, Rule::SelectStmt, Rule::ValuesStmt]),
            &pair,
        )),
    }
}

fn parse_multiset_op(pair: Pair<'_, Rule>) -> AstResult<(OperationKind, OpDupElimination)> {
    let op = match pair.as_rule() {
        Rule::UnionOp => OperationKind::Union,
        Rule::ExceptOp => OperationKind::Except,
        Rule::IntersectOp => OperationKind::Intersect,
        _ => {
            return Err(unexpected_rule_error(
                ExpectedRules(&[Rule::UnionOp, Rule::ExceptOp, Rule::IntersectOp]),
                &pair,
            ));
        }
    };

    let dup_elimination = pair
        .into_inner()
        .find(|pair| matches!(pair.as_rule(), Rule::MultisetOpDupElimination))
        .map(|pair| {
            if pair.as_str().eq_ignore_ascii_case("all") {
                OpDupElimination::All
            } else {
                OpDupElimination::Distinct
            }
        })
        .unwrap_or(OpDupElimination::Distinct);

    Ok((op, dup_elimination))
}

fn parse_cte<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<Cte<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::Cte);

    let parse_tree = Tree::from_pair_with_gothru_filter(pair, |pair| {
        !matches!(pair.as_rule(), Rule::Identifier | Rule::MultisetStmt)
    });

    let mut cte_name: Option<Ident> = None;
    let mut cte_columns = Vec::<Ident>::new();
    let mut cte_body: Option<MultisetStmt<'q, Raw>> = None;

    for pair in parse_tree {
        match pair.as_rule() {
            Rule::Cte => {}
            Rule::Identifier => match cte_name {
                None => cte_name = Some(Ident::from_sql(pair.as_str())),
                Some(_) => cte_columns.push(Ident::from_sql(pair.as_str())),
            },
            Rule::MultisetStmt => cte_body = Some(parse_multiset(pair, ctx)?),
            _ => {
                return Err(unexpected_rule_error(
                    ExpectedRules(&[Rule::Cte, Rule::Identifier, Rule::MultisetStmt]),
                    &pair,
                ));
            }
        }
    }

    let cte_name = cte_name
        .ok_or_else(|| parse_invariant_error(format_smolstr!("grammar guarantees a CTE name")))?;
    let cte_body = cte_body
        .ok_or_else(|| parse_invariant_error(format_smolstr!("grammar guarantees a CTE body")))?;
    Ok(Cte::new(cte_name, cte_columns, cte_body))
}

fn parse_values<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<ValuesStmt<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::ValuesStmt);

    let mut values_stmt = ValuesStmt::default();

    for pair in pair.into_inner() {
        values_stmt.add_row(parse_values_row(pair, ctx)?);
    }

    debug_assert!(!values_stmt.is_empty());
    Ok(values_stmt)
}

fn parse_order_by<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<OrderBy<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::OrderBy);

    let mut order_by = OrderBy::default();

    for pair in pair.into_inner() {
        order_by.add_elem(parse_order_by_element(pair, ctx)?);
    }

    debug_assert!(!order_by.is_empty());
    Ok(order_by)
}

pub(super) fn parse_order_by_element<'q>(
    pair: Pair<'q, Rule>,
    ctx: &ParseCtx,
) -> AstResult<OrderByElement<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::OrderByElement);

    let mut expr = None;
    let mut direction = OrderByDirection::default();
    let mut nulls = OrderByNulls::default();

    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::Expr => expr = Some(parse_expr(pair, ctx)?),
            Rule::Asc => direction = OrderByDirection::Asc,
            Rule::Desc => direction = OrderByDirection::Desc,
            Rule::NullsFirst => nulls = OrderByNulls::First,
            Rule::NullsLast => nulls = OrderByNulls::Last,
            _ => {
                return Err(unexpected_rule_error(
                    ExpectedRules(&[
                        Rule::Expr,
                        Rule::Asc,
                        Rule::Desc,
                        Rule::NullsFirst,
                        Rule::NullsLast,
                    ]),
                    &pair,
                ));
            }
        }
    }

    Ok(OrderByElement {
        expr: expr.ok_or_else(|| {
            parse_invariant_error(format_smolstr!("grammar guarantees an ORDER BY expression"))
        })?,
        direction,
        nulls,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pest::Parser;
    use sql_ast_new_grammar::PairParser;

    use crate::test_support::round_trip;

    fn parse_multiset(query: &str) -> AstResult<MultisetStmt<'_, Raw>> {
        let pair = PairParser::parse(Rule::MultisetStmt, query)
            .expect("expected multiset statement to parse")
            .next()
            .expect("expected multiset statement pair");
        let ctx = ParseCtx::new(&pair)?;
        super::parse_multiset(pair, &ctx)
    }

    /// Render `query`, and hold the rendering to the round trip in
    /// [`crate::test_support::round_trip`].
    #[track_caller]
    fn render_multiset(query: &str) -> String {
        round_trip(query, |q| parse_multiset(q).map(|stmt| stmt.to_string()))
    }

    fn operation<'a, 'q>(stmt: &'a MultisetStmt<'q, Raw>) -> &'a Operation<'q, Raw> {
        match &stmt.inner {
            MultisetInner::Operation(operation) => operation,
            _ => panic!("expected multiset operation"),
        }
    }

    #[test]
    fn repeated_union_is_left_associative() {
        let stmt = parse_multiset("SELECT 1 UNION SELECT 2 UNION SELECT 3")
            .expect("expected multiset AST");

        let root = operation(&stmt);
        assert_eq!(root.op, OperationKind::Union);
        assert!(matches!(&root.right.inner, MultisetInner::Select(_)));

        let left = operation(&root.left);
        assert_eq!(left.op, OperationKind::Union);
    }

    #[test]
    fn union_and_except_share_left_associative_precedence() {
        let stmt = parse_multiset("SELECT 1 EXCEPT SELECT 2 UNION SELECT 3")
            .expect("expected multiset AST");

        let root = operation(&stmt);
        assert_eq!(root.op, OperationKind::Union);
        assert!(matches!(&root.right.inner, MultisetInner::Select(_)));

        let left = operation(&root.left);
        assert_eq!(left.op, OperationKind::Except);
    }

    #[test]
    fn intersect_binds_tighter_than_union() {
        let stmt = parse_multiset("SELECT 1 UNION SELECT 2 INTERSECT SELECT 3")
            .expect("expected multiset AST");

        let root = operation(&stmt);
        assert_eq!(root.op, OperationKind::Union);
        assert!(matches!(&root.left.inner, MultisetInner::Select(_)));

        let right = operation(&root.right);
        assert_eq!(right.op, OperationKind::Intersect);
    }

    #[test]
    fn parentheses_override_multiset_precedence() {
        let stmt = parse_multiset("(SELECT 1 UNION SELECT 2) INTERSECT SELECT 3")
            .expect("expected multiset AST");

        let root = operation(&stmt);
        assert_eq!(root.op, OperationKind::Intersect);

        let left = operation(&root.left);
        assert_eq!(left.op, OperationKind::Union);
    }

    #[test]
    fn parentheses_around_multiset_operation_operands() {
        let stmt =
            parse_multiset("(SELECT 1 LIMIT 1) UNION SELECT 2").expect("expected multiset AST");

        insta::assert_snapshot!(stmt.to_string(), @"(SELECT 1 LIMIT 1) UNION (SELECT 2)");
    }

    #[test]
    fn duplicate_elimination_modifier_is_parsed() {
        let union_all =
            parse_multiset("SELECT 1 UNION ALL SELECT 2").expect("expected multiset AST");
        assert_eq!(operation(&union_all).op, OperationKind::Union);
        assert_eq!(operation(&union_all).dup_elimination, OpDupElimination::All);
        insta::assert_snapshot!(union_all.to_string(), @"(SELECT 1) UNION ALL (SELECT 2)");

        let except_distinct =
            parse_multiset("SELECT 1 EXCEPT DISTINCT SELECT 2").expect("expected multiset AST");
        assert_eq!(operation(&except_distinct).op, OperationKind::Except);
        assert_eq!(
            operation(&except_distinct).dup_elimination,
            OpDupElimination::Distinct
        );
        insta::assert_snapshot!(except_distinct.to_string(), @"(SELECT 1) EXCEPT (SELECT 2)");

        let intersect_default =
            parse_multiset("SELECT 1 INTERSECT SELECT 2").expect("expected multiset AST");
        assert_eq!(operation(&intersect_default).op, OperationKind::Intersect);
        assert_eq!(
            operation(&intersect_default).dup_elimination,
            OpDupElimination::Distinct
        );
    }

    #[test]
    fn intersect_operation_renders() {
        insta::assert_snapshot!(
            render_multiset("SELECT 1 INTERSECT SELECT 2"),
            @"(SELECT 1) INTERSECT (SELECT 2)"
        );
        insta::assert_snapshot!(
            render_multiset("SELECT 1 INTERSECT ALL SELECT 2"),
            @"(SELECT 1) INTERSECT ALL (SELECT 2)"
        );
    }

    #[test]
    fn root_cte_scopes_over_operation_multiset() {
        let query = r#"
            WITH
            cte1 AS (SELECT * FROM t1),
            cte2 AS (SELECT * FROM t2)
            SELECT * FROM t1
            UNION ALL
            SELECT * FROM cte1
            INTERSECT
            SELECT * FROM cte2
        "#;

        let stmt = parse_multiset(query).expect("expected multiset AST");

        assert!(!stmt.ctes.is_empty());
        let oper = operation(&stmt);
        assert!(matches!(oper.op, OperationKind::Union));

        let right_stmt = oper.right.as_ref();
        assert!(right_stmt.ctes.is_empty());
        operation(right_stmt);
    }

    #[test]
    fn cte_scopes_inside_parenthesized_multiset() {
        let query = r#"
            (
                WITH cte1 AS (SELECT * FROM t1)
                SELECT * FROM t1 UNION ALL SELECT * FROM cte1
            )
            INTERSECT
            (
                WITH cte2 AS (SELECT * FROM t2) SELECT * FROM cte2
            )
        "#;

        let stmt = parse_multiset(query).expect("expected multiset AST");

        assert!(stmt.ctes.is_empty());
        let oper = operation(&stmt);
        assert!(matches!(oper.op, OperationKind::Intersect));

        let left_stmt = oper.left.as_ref();
        assert!(!left_stmt.ctes.is_empty());
        operation(left_stmt);
    }

    #[test]
    fn cte_nested_in_operation() {
        let query = r#"
            WITH cte1 AS (SELECT * FROM t1)
            (
                WITH cte2 AS (SELECT * from t2)
                SELECT * FROM cte2
            ) UNION SELECT * FROM t3;
        "#;

        let stmt = parse_multiset(query).expect("expected multiset AST");
        assert!(!stmt.ctes.is_empty());
        let oper = operation(&stmt);
        assert!(matches!(oper.op, OperationKind::Union));
    }

    #[test]
    fn multiple_cte() {
        let query = r#"
            WITH cte1 AS (SELECT 1)
            (
                WITH cte2 AS (SELECT 2)
                SELECT 3
                UNION
                SELECT 4
            );
        "#;

        let stmt = parse_multiset(query);
        let parse_err = stmt.err().expect("expected parsing to fail");
        insta::assert_snapshot!(
            parse_err.to_string(),
            @"invalid CTE: cannot use multiple WITH"
        );
    }

    #[test]
    fn nested_cte_in_subquery() {
        let query = r#"
            WITH
            cte AS (
                SELECT * FROM t1
            )
            SELECT * FROM (
                WITH
                cte AS (
                    SELECT * FROM t1
                )
                (
                    SELECT * FROM cte
                )
            );
        "#;

        assert!(parse_multiset(query).is_ok());
    }

    #[test]
    fn multiple_cte_nested_in_subquery() {
        let query = r#"
            WITH
            cte AS (
                SELECT * FROM t1
            )
            SELECT * FROM (
                WITH
                cte AS (
                    SELECT * FROM t1
                )
                (
                    WITH
                    cte AS (
                        SELECT * FROM t1
                    )
                    SELECT * FROM cte
                )
            );
        "#;

        let stmt = parse_multiset(query);
        let parse_err = stmt.err().expect("expected parsing to fail");
        insta::assert_snapshot!(
            parse_err.to_string(),
            @"invalid CTE: cannot use multiple WITH"
        );
    }

    #[test]
    fn cte_body_can_contain_multiset_op() {
        let query = r#"WITH cte AS (SELECT 1 UNION ALL SELECT 2) SELECT * FROM cte"#;
        insta::assert_snapshot!(
            render_multiset(query),
            @"WITH cte AS ((SELECT 1) UNION ALL (SELECT 2)) SELECT * FROM cte"
        );
    }

    #[test]
    fn single_multiset_stmt_in_parentheses() {
        let query = r#"(WITH q AS (SELECT 1) SELECT * FROM q)"#;
        insta::assert_snapshot!(
            render_multiset(query),
            @"WITH q AS (SELECT 1) SELECT * FROM q"
        );
    }

    #[test]
    fn values_simple() {
        let query = r#"VALUES (0, 8), (0, 7), (0, 3)"#;
        insta::assert_snapshot!(render_multiset(query), @"VALUES (0, 8), (0, 7), (0, 3)");
    }

    #[test]
    fn values_nested_expr() {
        let query = r#"VALUES (1 + (5 - a) * 2, b * (1 / (r + 1))), (2, 3)"#;
        insta::assert_snapshot!(
            render_multiset(query),
            @"VALUES (1 + (5 - a) * 2, b * (1 / (r + 1))), (2, 3)"
        );
    }

    #[test]
    fn order_by_dir_and_nulls() {
        let query =
            r#"SELECT a, b, c FROM t ORDER BY a DESC NULLS LAST, b * 2 + 1 ASC NULLS FIRST"#;
        insta::assert_snapshot!(
            render_multiset(query),
            @"SELECT a, b, c FROM t ORDER BY a DESC NULLS LAST, b * 2 + 1 ASC NULLS FIRST"
        );
    }

    #[test]
    fn duplicating_order_by_clause_error() {
        let query = r#"(SELECT a FROM t ORDER BY a LIMIT 3) ORDER BY a DESC"#;
        let err_str = parse_multiset(query)
            .err()
            .expect("expected failed AST building")
            .to_string();
        insta::assert_snapshot!(err_str, @"failed to parse query: multiple ORDER BY clauses not allowed");
    }

    #[test]
    fn limit_simple() {
        let query = r#"SELECT a, b, c FROM t ORDER BY a DESC LIMIT 10"#;
        insta::assert_snapshot!(
            render_multiset(query),
            @"SELECT a, b, c FROM t ORDER BY a DESC LIMIT 10"
        );
    }

    #[test]
    fn duplicating_limit_clause_error() {
        let query = r#"(SELECT a FROM t LIMIT 3) LIMIT 5"#;
        let err_str = parse_multiset(query)
            .err()
            .expect("expected failed AST building")
            .to_string();
        insta::assert_snapshot!(err_str, @"failed to parse query: multiple LIMIT clauses not allowed");
    }

    #[test]
    fn limit_all() {
        let query = r#"SELECT a FROM t LIMIT ALL"#;
        insta::assert_snapshot!(
            render_multiset(query),
            @"SELECT a FROM t LIMIT ALL"
        );
    }

    #[test]
    fn limit_value_overflow_error() {
        let query = r#"SELECT a FROM t LIMIT 99999999999999999999999999"#;
        let err_str = parse_multiset(query)
            .err()
            .expect("expected failed AST building")
            .to_string();
        insta::assert_snapshot!(err_str, @"invalid query: limit must be unsigned integer");
    }

    #[test]
    fn limit_with_order_by_simple() {
        let query = r#"(SELECT a FROM t LIMIT 3) ORDER BY a"#;
        insta::assert_snapshot!(
            render_multiset(query),
            @"SELECT a FROM t ORDER BY a ASC LIMIT 3"
        );
    }

    #[test]
    fn limit_with_order_by_values() {
        let query = r#"(SELECT a FROM (VALUES (3),(1),(2)) LIMIT 2) ORDER BY a"#;
        insta::assert_snapshot!(
            render_multiset(query),
            @"SELECT a FROM (VALUES (3), (1), (2)) ORDER BY a ASC LIMIT 2"
        );
    }
}
