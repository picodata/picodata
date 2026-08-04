//! Parsing of SQL expressions: pest [`Pair`]s -> [`Expr<Raw>`].
//!
//! # `parse_expr`
//! The entry point, re-exported from [`expr`](super) and used by every statement
//! parser.
//!
//! Around it sits the `parse_*` family — one function per primary form, from
//! literals and identifiers to `CAST`, `CASE`, `TRIM`, `SUBSTRING` and `EXISTS`
//! — together with the smart constructors that assemble nodes.
//!
//!
//! # Precedence
//! Expressions are the one place where structure cannot be read off the grammar.
//! The pest rule matches an operator stream flat. Precedence and associativity
//! are reconstructed here by a Pratt parser, which folds prefix, infix and
//! postfix operators into [`Expr<Raw>`] nodes.
//!
//! Keeping the grammar flat is deliberate.
//! 1. A cascade of nested rules would encode the same ladder less readably.
//! 2. The non-associative tiers (comparison, `BETWEEN`, `LIKE`) are easier to
//!    reject with a precise message here than to express in the grammar at all.

#[cfg(test)]
mod tests;

use pest::iterators::Pair;
use pest::pratt_parser::PrattParser;
use smol_str::format_smolstr;

use crate::multiset::parse_multiset;
use crate::window::parse_window_function;
use crate::ParseCtx;
use crate::{invalid_expression_error, parse_invariant_error, AstResult};
use sql_ast_new_grammar::Rule;
use sql_ast_new_nodes::expr::{
    ArithmeticOp, BinaryOp, BooleanOp, Case, CastSyntax, CmpOp, Expr, ExprInner, FunctionCallArgs,
    Like, LiteralKind, Parameter, QuotesType, RawExpr, Similar, Substring, TimeFunction, TrimKind,
    UnaryOp, ValuesRow,
};
use sql_ast_new_nodes::{Ident, Raw};
use sql_ir::errors::{Entity, SbroadError};
use sql_ir::ir::aggregates::AggregateKind;
use sql_ir::ir::types::{CastType, NestedType};

lazy_static::lazy_static! {
    /// Operator precedence lives here, not in the grammar: pest matches
    /// operator chains flat and this parser groups them.
    /// The tiers must stay in sync with `Precedence` in
    /// `nodes/expr/render.rs`, which drives re-parseable rendering.
    ///
    /// The comparison and matching tiers are non-associative.
    /// [`PrattParser`] cannot express that,
    /// so [`parse_expr`]'s `map_infix` rejects same-tier chains.
    static ref EXPR_PRATT_PARSER: PrattParser<Rule> = {
        use pest::pratt_parser::{Assoc::Left, Op};
        use Rule::{
            Add, And, Between, CastPostfix, ConcatInfixOp, Divide, Eq, Escape, Gt, GtEq, In,
            IndexPostfix, IsPostfix, Like, Lt, LtEq, Modulo, Multiply, NotEq, Or, Similar,
            Subtract, UnaryMinus, UnaryNot, UnaryPlus,
        };

        PrattParser::new()
            .op(Op::infix(Or, Left))
            .op(Op::infix(And, Left))
            .op(Op::prefix(UnaryNot))
            .op(Op::postfix(IsPostfix))
            .op(
                Op::infix(Eq, Left)
                    | Op::infix(NotEq, Left)
                    | Op::infix(Gt, Left)
                    | Op::infix(GtEq, Left)
                    | Op::infix(Lt, Left)
                    | Op::infix(LtEq, Left)
            )
            .op(
                Op::infix(Between, Left)
                    | Op::infix(In, Left)
                    | Op::infix(Like, Left)
                    | Op::infix(Similar, Left)
                    // ESCAPE must be just above LIKE/ILIKE/SIMILAR
                    | Op::infix(Escape, Left)
            )
            .op(Op::infix(ConcatInfixOp, Left))
            .op(Op::infix(Add, Left) | Op::infix(Subtract, Left))
            .op(Op::infix(Multiply, Left) | Op::infix(Divide, Left) | Op::infix(Modulo, Left))
            // Unary signs are tighter than every infix operator, looser than the postfixes (`-a::int` is `-(a::int)`).
            .op(Op::prefix(UnaryMinus) | Op::prefix(UnaryPlus))
            .op(Op::postfix(IndexPostfix))
            .op(Op::postfix(CastPostfix))
    };
}

/// Transient per-operand state threaded through the Pratt fold. The built
/// expression plus whether it came directly from
/// [`ExpressionInParentheses`](Rule::ExpressionInParentheses).
/// Parentheses leave no AST node, but the non-associativity checks must tell
/// `(a = b) = c` (legal — the parentheses reset the tier) apart from `a = b = c`.
/// Never escapes [`parse_expr`].
struct Operand<'q> {
    expr: RawExpr<'q>,
    parenthesized: bool,
}

impl<'q> Operand<'q> {
    fn new(expr: RawExpr<'q>) -> Self {
        Self {
            expr,
            parenthesized: false,
        }
    }
}

/// The two non-associative operator tiers.
#[derive(Clone, Copy, PartialEq)]
enum NonAssocTier {
    /// `= <> < > <= >=`.
    Comparison,
    /// `[NOT] BETWEEN` / `[NOT] IN` / `[NOT] LIKE` / `[NOT] ILIKE` / `[NOT] SIMILAR`.
    /// `ESCAPE` is deliberately absent.
    /// It extends the preceding LIKE/SIMILAR node instead of chaining (see [`connect_escape`]).
    Matching,
}

/// The non-associative tier of an operator token, if any.
fn op_nonassoc_tier(rule: Rule) -> Option<NonAssocTier> {
    match rule {
        Rule::Eq | Rule::NotEq | Rule::Gt | Rule::GtEq | Rule::Lt | Rule::LtEq => {
            Some(NonAssocTier::Comparison)
        }
        Rule::Like | Rule::Similar | Rule::Between | Rule::In => Some(NonAssocTier::Matching),
        _ => None,
    }
}

/// The non-associative tier an expression's top node renders on, if any.
fn node_nonassoc_tier(expr: &RawExpr<'_>) -> Option<NonAssocTier> {
    match expr.inner_ref() {
        ExprInner::BinaryOperation(operation)
            if matches!(operation.op, BinaryOp::Comparison(_)) =>
        {
            Some(NonAssocTier::Comparison)
        }
        ExprInner::Like(_) | ExprInner::Similar(_) | ExprInner::Between(_) | ExprInner::In(_) => {
            Some(NonAssocTier::Matching)
        }
        _ => None,
    }
}

fn parse_binary_op(pair: Pair<'_, Rule>) -> AstResult<BinaryOp> {
    Ok(match pair.as_rule() {
        Rule::Add => BinaryOp::Arithmetic(ArithmeticOp::Add),
        Rule::Subtract => BinaryOp::Arithmetic(ArithmeticOp::Subtract),
        Rule::Multiply => BinaryOp::Arithmetic(ArithmeticOp::Multiply),
        Rule::Divide => BinaryOp::Arithmetic(ArithmeticOp::Divide),
        Rule::Modulo => BinaryOp::Arithmetic(ArithmeticOp::Modulo),
        Rule::Eq => BinaryOp::Comparison(CmpOp::Eq),
        Rule::NotEq => BinaryOp::Comparison(CmpOp::Neq),
        Rule::Lt => BinaryOp::Comparison(CmpOp::Lt),
        Rule::Gt => BinaryOp::Comparison(CmpOp::Gt),
        Rule::LtEq => BinaryOp::Comparison(CmpOp::Lte),
        Rule::GtEq => BinaryOp::Comparison(CmpOp::Gte),
        Rule::And => BinaryOp::Boolean(BooleanOp::And),
        Rule::Or => BinaryOp::Boolean(BooleanOp::Or),
        Rule::ConcatInfixOp => BinaryOp::Concat,
        Rule::Like | Rule::Similar | Rule::Escape | Rule::Between | Rule::In => {
            return Err(parse_invariant_error(format_smolstr!(
                "{:?} must be handled in `map_infix` directly",
                pair.as_rule()
            )))
        }
        rule => {
            return Err(parse_invariant_error(format_smolstr!(
                "expected a `Rule` for `BinaryOp`, got {rule:?}"
            )))
        }
    })
}

pub(super) fn parse_values_row<'q>(
    pair: Pair<'q, Rule>,
    ctx: &ParseCtx,
) -> AstResult<ValuesRow<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::ValuesRow);

    let mut values_row = ValuesRow::default();

    for pair in pair.into_inner() {
        values_row.add_value(parse_expr(pair, ctx)?);
    }

    debug_assert!(!values_row.is_empty());
    Ok(values_row)
}
/// Expression entry point. One Pratt run over the pair's children.
/// Operators carrying more structure than two operands —
/// LIKE/SIMILAR `ESCAPE` chain,
/// fat BETWEEN token with its embedded middle operand,
/// IN with its NOT flag
/// — all they are unpacked in `map_infix` instead of [`BinaryOp`].
pub(super) fn parse_expr<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<RawExpr<'q>> {
    // `BExpr` (BETWEEN's middle operand) has the same inner shape as `Expr`,
    // just with fewer infix operators allowed by the grammar,
    // so one pratt parser serves both.
    debug_assert!(matches!(pair.as_rule(), Rule::Expr | Rule::BExpr));
    EXPR_PRATT_PARSER
        .map_primary(|primary| {
            let parenthesized = matches!(primary.as_rule(), Rule::ExpressionInParentheses);
            let expr = match primary.as_rule() {
                Rule::Expr => parse_expr(primary, ctx),
                Rule::ExpressionInParentheses => {
                    let expr_pair = primary.into_inner().next().ok_or_else(|| {
                        parse_invariant_error(format_smolstr!(
                            "`ExpressionInParentheses` must contain `Expr`"
                        ))
                    })?;
                    parse_expr(expr_pair, ctx)
                }
                Rule::Literal
                | Rule::True
                | Rule::False
                | Rule::Null
                | Rule::Double
                | Rule::Decimal
                | Rule::Unsigned
                | Rule::Integer
                | Rule::SingleQuotedString => parse_literal(primary),
                Rule::IdentifierWithOptionalContinuation => parse_identifier(primary, ctx),
                Rule::MultisetStmt => {
                    let subquery = parse_multiset(primary, ctx)?;
                    Ok(Expr::subquery(subquery))
                }
                Rule::ValuesRow => {
                    let row = parse_values_row(primary, ctx)?;
                    Ok(Expr::row(row))
                }
                Rule::ArrayLiteral => parse_array_literal(primary, ctx),
                Rule::Over => {
                    let window_function = parse_window_function(primary, ctx)?;
                    Ok(Expr::window_function(window_function))
                }
                Rule::Parameter => parse_parameter_expr(primary, ctx),
                Rule::CastOp => parse_cast_op(primary, ctx),
                Rule::Trim => parse_trim(primary, ctx),
                Rule::Substring => parse_substring(primary, ctx),
                Rule::Case => parse_case(primary, ctx),
                Rule::Exists => parse_exists(primary, ctx),
                Rule::CurrentDate
                | Rule::CurrentTime
                | Rule::CurrentTimestamp
                | Rule::LocalTime
                | Rule::LocalTimestamp => parse_time_function(primary),
                _ => Err(parse_invariant_error(format_smolstr!(
                    "expected expression primary, got {:?}",
                    primary.as_rule()
                ))),
            }?;
            Ok(Operand {
                expr,
                parenthesized,
            })
        })
        .map_prefix(|op, operand| match op.as_rule() {
            Rule::UnaryNot => Ok(Operand::new(Expr::unary(UnaryOp::Not, operand?.expr))),
            Rule::UnaryMinus => Ok(Operand::new(Expr::unary(UnaryOp::Minus, operand?.expr))),
            Rule::UnaryPlus => Ok(Operand::new(Expr::unary(UnaryOp::Plus, operand?.expr))),
            rule => Err(parse_invariant_error(format_smolstr!(
                "expected a `Rule` for expression prefix, got {rule:?}"
            ))),
        })
        .map_postfix(|operand, op| {
            let operand = operand?.expr;
            let expr = match op.as_rule() {
                Rule::IsPostfix => parse_is_postfix(operand, op),
                Rule::CastPostfix => {
                    let target_pair = op.into_inner().next().ok_or_else(|| {
                        parse_invariant_error(format_smolstr!(
                            "`CastPostfix` must contain `CastTarget`"
                        ))
                    })?;
                    let ty = parse_cast_target(target_pair)?;
                    Ok(Expr::cast(operand, ty, CastSyntax::Postfix))
                }
                Rule::IndexPostfix => {
                    let which_pair = op.into_inner().next().ok_or_else(|| {
                        parse_invariant_error(format_smolstr!("`IndexPostfix` must contain `Expr`"))
                    })?;
                    let which = parse_expr(which_pair, ctx)?;
                    Ok(Expr::index(operand, which))
                }
                rule => Err(parse_invariant_error(format_smolstr!(
                    "expected `Rule` for expression postfix, got {rule:?}"
                ))),
            }?;
            Ok(Operand::new(expr))
        })
        .map_infix(|left, op, right| {
            let left = left?;
            let right = right?.expr;
            // The comparison and matching tiers are non-associative.
            // A same-tier operator cannot take a same-tier expression
            // as its left operand unless parentheses reset the tier.
            // The check is one-sided: with left-associative folding and the tier ordering,
            // a violation always surfaces as the left operand of the second operator.
            if let Some(tier) = op_nonassoc_tier(op.as_rule()) {
                if !left.parenthesized && node_nonassoc_tier(&left.expr) == Some(tier) {
                    let tier_ops = match tier {
                        NonAssocTier::Comparison => "comparison operators",
                        NonAssocTier::Matching => "BETWEEN/IN/LIKE/ILIKE/SIMILAR operators",
                    };
                    return Err(invalid_expression_error(format_smolstr!(
                        "{tier_ops} are non-associative; use parentheses to group operands"
                    )));
                }
            }
            let expr = match op.as_rule() {
                Rule::Like => {
                    // Both `[NOT] LIKE` and `[NOT] ILIKE` spellings end with
                    // the keyword, and only ILIKE ends with "ilike".
                    let is_ilike = op.as_str().to_ascii_lowercase().ends_with("ilike");
                    let is_not = op
                        .into_inner()
                        .next()
                        .is_some_and(|pair| matches!(pair.as_rule(), Rule::NotFlag));
                    Ok(Expr::like(is_not, left.expr, right, is_ilike))
                }
                Rule::Similar => {
                    let is_not = op
                        .into_inner()
                        .next()
                        .is_some_and(|pair| matches!(pair.as_rule(), Rule::NotFlag));
                    Ok(Expr::similar(is_not, left.expr, right))
                }
                Rule::Escape => connect_escape(left, right),
                Rule::Between => {
                    // The op token is the whole `[NOT] BETWEEN <middle> AND`
                    // sequence; `left`/`right` are the lower expression and
                    // the upper bound, the middle operand is parsed here from
                    // the token's `BExpr`.
                    let mut op_pairs = op.into_inner();
                    let first_pair = op_pairs.next().ok_or_else(|| {
                        parse_invariant_error(format_smolstr!(
                            "`Between` must contain a middle operand"
                        ))
                    })?;
                    let (is_not, middle_pair) = if matches!(first_pair.as_rule(), Rule::NotFlag) {
                        let middle_pair = op_pairs.next().ok_or_else(|| {
                            parse_invariant_error(format_smolstr!(
                                "`Between` must contain a middle operand after NOT"
                            ))
                        })?;
                        (true, middle_pair)
                    } else {
                        (false, first_pair)
                    };
                    let center = parse_expr(middle_pair, ctx)?;
                    Ok(Expr::between(is_not, left.expr, center, right))
                }
                Rule::In => {
                    let is_not = op
                        .into_inner()
                        .next()
                        .is_some_and(|pair| matches!(pair.as_rule(), Rule::NotFlag));
                    Expr::in_expr(is_not, left.expr, right)
                }
                _ => Ok(Expr::binary(left.expr, parse_binary_op(op)?, right)),
            }?;
            Ok(Operand::new(expr))
        })
        .parse(pair.into_inner())
        .map(|operand| operand.expr)
}

/// `ESCAPE` parses as an ordinary left-associative infix, so its left
/// operand is the whole preceding LIKE/ILIKE/SIMILAR chain.
/// The escape is folded into that node here.
/// Any other left operand — including an already-escaped or a parenthesized one
/// is a user error, not a parser invariant, e.g. `(a LIKE b) ESCAPE 'x'` is a syntax error.
fn connect_escape<'q>(left: Operand<'q>, right: RawExpr<'q>) -> AstResult<RawExpr<'q>> {
    let mut left_expr = left.expr;
    match &mut left_expr.inner {
        ExprInner::Like(Like { escape, .. }) | ExprInner::Similar(Similar { escape, .. })
            if !left.parenthesized =>
        {
            if escape.is_some() {
                return Err(invalid_expression_error(format_smolstr!(
                    "escape specified twice: expr1 LIKE/SIMILAR expr2 ESCAPE expr3 ESCAPE expr4"
                )));
            }
            *escape = Some(Box::new(right));
            Ok(left_expr)
        }
        ExprInner::Like(_) | ExprInner::Similar(_) => {
            Err(invalid_expression_error(format_smolstr!(
                "ESCAPE must directly follow a LIKE/ILIKE/SIMILAR pattern; \
                 a parenthesized expression cannot take an ESCAPE clause"
            )))
        }
        _ => Err(invalid_expression_error(format_smolstr!(
            "ESCAPE can go only after LIKE or SIMILAR expressions, got: {}",
            left_expr.inner_ref().kind_name()
        ))),
    }
}

fn parse_literal<'q>(pair: Pair<'q, Rule>) -> AstResult<RawExpr<'q>> {
    let pair = if matches!(pair.as_rule(), Rule::Literal) {
        pair.into_inner().next().ok_or_else(|| {
            parse_invariant_error(format_smolstr!("`Literal` must contain a concrete literal"))
        })?
    } else {
        pair
    };

    let literal = match pair.as_rule() {
        Rule::SingleQuotedString => {
            let quoted = pair.as_str();
            debug_assert!(
                quoted.len() >= 2 && quoted.starts_with('\'') && quoted.ends_with('\''),
                "`SingleQuotedString` must be wrapped in single quotes"
            );
            let value = quoted
                .strip_prefix('\'')
                .and_then(|s| s.strip_suffix('\''))
                .ok_or_else(|| {
                    parse_invariant_error(format_smolstr!(
                        "`SingleQuotedString` must be wrapped in single quotes"
                    ))
                })?;
            Expr::literal(value, QuotesType::Single, LiteralKind::Text)
        }
        rule => {
            let kind = match rule {
                Rule::True | Rule::False => LiteralKind::Boolean,
                Rule::Null => LiteralKind::Null,
                Rule::Double => LiteralKind::Double,
                Rule::Decimal => LiteralKind::Numeric,
                Rule::Unsigned | Rule::Integer => {
                    if pair.as_str().parse::<i64>().is_err() {
                        return Err(SbroadError::Invalid(
                            Entity::Query,
                            Some(format_smolstr!(
                                "value doesn't fit into integer range: {}",
                                pair.as_str()
                            )),
                        )
                        .into());
                    }
                    LiteralKind::Integer
                }
                _ => {
                    return Err(parse_invariant_error(format_smolstr!(
                        "expected a concrete literal, got {rule:?}"
                    )))
                }
            };
            Expr::literal(pair.as_str(), QuotesType::None, kind)
        }
    };
    Ok(literal)
}

/// An identifier is classified by its continuation, which only the parse
/// tree can tell apart: `.column` makes it a qualified column reference,
/// `(args)` a function call, nothing — an unqualified column reference.
fn parse_identifier<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<RawExpr<'q>> {
    let mut inner_pairs = pair.into_inner();

    let ident_pair = inner_pairs.next().ok_or_else(|| {
        parse_invariant_error(format_smolstr!(
            "`IdentifierWithOptionalContinuation` must be non-empty"
        ))
    })?;

    match inner_pairs.next() {
        Some(pair) => {
            match pair.as_rule() {
                Rule::ReferenceContinuation => {
                    let table_name = ident_pair.as_str();
                    // In order to skip "." in `ReferenceContinuation` (".<column_name>")
                    let column_name = pair.as_str().strip_prefix('.').ok_or_else(|| {
                        parse_invariant_error(format_smolstr!(
                            "`ReferenceContinuation` must start with '.'"
                        ))
                    })?;
                    Ok(Expr::column_ref(
                        Ident::from_sql(column_name),
                        Some(Ident::from_sql(table_name)),
                    ))
                }
                Rule::FunctionInvocationContinuation => {
                    parse_function_invocation(ident_pair.as_str(), pair, ctx)
                }
                rule => Err(parse_invariant_error(format_smolstr!(
                    "unexpected continuation in `IdentifierWithOptionalContinuation`: {rule:?}"
                ))),
            }
        }
        None => Ok(Expr::column_ref(Ident::from_sql(ident_pair.as_str()), None)),
    }
}

fn parse_function_invocation<'q>(
    name: &'q str,
    pair: Pair<'q, Rule>,
    ctx: &ParseCtx,
) -> AstResult<RawExpr<'q>> {
    debug_assert_eq!(pair.as_rule(), Rule::FunctionInvocationContinuation);

    let name = Ident::from_sql(name);

    let args = match pair.into_inner().next() {
        None => FunctionCallArgs::Exprs {
            distinct: false,
            exprs: Vec::new(),
        },
        Some(args_pair) => match args_pair.as_rule() {
            Rule::CountAsterisk => {
                if name.as_str() != AggregateKind::COUNT.to_string() {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format_smolstr!(
                            "\"*\" is allowed only inside \"{}\" aggregate function. Got: {name}",
                            AggregateKind::COUNT
                        )),
                    )
                    .into());
                }
                FunctionCallArgs::CountAsterisk
            }
            Rule::FunctionArgs => {
                let mut distinct = false;
                let mut exprs = Vec::new();
                for arg_pair in args_pair.into_inner() {
                    match arg_pair.as_rule() {
                        Rule::Distinct => distinct = true,
                        Rule::Expr => exprs.push(parse_expr(arg_pair, ctx)?),
                        rule => {
                            return Err(parse_invariant_error(format_smolstr!(
                                "expected `Distinct` or `Expr` under `FunctionArgs`, got {rule:?}"
                            )))
                        }
                    }
                }
                if distinct && !is_aggregate_function(&name) {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some("DISTINCT modifier is allowed only for aggregate functions".into()),
                    )
                    .into());
                }
                FunctionCallArgs::Exprs { distinct, exprs }
            }
            rule => {
                return Err(parse_invariant_error(format_smolstr!(
                    "expected `CountAsterisk` or `FunctionArgs`, got {rule:?}"
                )))
            }
        },
    };

    Ok(Expr::function_call(name, args))
}

/// The (already normalized) name denotes a built-in aggregate.
/// `AggregateKind::from_name` matches case-insensitively, which would re-admit
/// case-preserved quoted spellings like `"SUM"`; a normalized aggregate name
/// is always lowercase, so require fold-invariance first.
fn is_aggregate_function(name: &Ident) -> bool {
    let name = name.as_str();
    name == name.to_lowercase() && AggregateKind::from_name(name).is_some()
}

/// Build the parameter node, assigning the index: `?` takes the position of
/// its source offset among the query's placeholders (see [`ParseCtx`]), `$n`
/// states its own. That the two styles are not mixed was already settled when
/// the context was built.
fn parameter_from_pair(pair: Pair<'_, Rule>, ctx: &ParseCtx) -> AstResult<Parameter> {
    match pair.as_rule() {
        Rule::TntParameter => {
            let offset = pair.as_span().start();
            let index = ctx.tnt_param_index(offset).ok_or_else(|| {
                parse_invariant_error(format_smolstr!(
                    "`?` at offset {offset} was not seen while building the parse context"
                ))
            })?;
            Ok(Parameter(index))
        }
        Rule::PgParameter => {
            let index_pair = pair.into_inner().next().ok_or_else(|| {
                parse_invariant_error(format_smolstr!("`PgParameter` must contain `Unsigned`"))
            })?;
            let index = index_pair.as_str().parse::<u16>().map_err(|_| {
                invalid_expression_error(format_smolstr!(
                    "parameter index must be between 1 and 65536"
                ))
            })?;
            if index == 0 {
                return Err(invalid_expression_error(format_smolstr!(
                    "parameter index must be >=1"
                )));
            }
            Ok(Parameter(index))
        }
        rule => Err(parse_invariant_error(format_smolstr!(
            "expected `TntParameter` or `PgParameter`, got {rule:?}"
        ))),
    }
}

fn parse_parameter_expr<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<RawExpr<'q>> {
    Ok(Expr::parameter(parse_parameter(pair, ctx)?))
}

pub(super) fn parse_parameter(pair: Pair<'_, Rule>, ctx: &ParseCtx) -> AstResult<Parameter> {
    debug_assert_eq!(pair.as_rule(), Rule::Parameter);

    let inner_pair = pair
        .into_inner()
        .next()
        .ok_or_else(|| parse_invariant_error(format_smolstr!("`Parameter` must be non-empty")))?;

    parameter_from_pair(inner_pair, ctx)
}

fn parse_array_literal<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<RawExpr<'q>> {
    debug_assert_eq!(pair.as_rule(), Rule::ArrayLiteral);

    let elems = pair
        .into_inner()
        .map(|pair| parse_expr(pair, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Expr::array(elems))
}

fn parse_cast_target(pair: Pair<'_, Rule>) -> AstResult<CastType> {
    debug_assert_eq!(pair.as_rule(), Rule::CastTarget);

    let mut inner_pairs = pair.into_inner();
    let type_pair = inner_pairs.next().ok_or_else(|| {
        parse_invariant_error(format_smolstr!("`CastTarget` must contain `Type`"))
    })?;
    if inner_pairs.next().is_none() {
        return parse_cast_type(type_pair);
    }
    // An array suffix follows (`[]` / `ARRAY` / `ARRAY[N]`; a sized bound is
    // accepted and ignored, like the old pipeline does).
    let concrete_type_pair = type_pair.into_inner().next().ok_or_else(|| {
        parse_invariant_error(format_smolstr!("concrete type expected under `Type`"))
    })?;
    let elem = match concrete_type_pair.as_rule() {
        Rule::TypeBool => NestedType::Boolean,
        Rule::TypeDatetime => NestedType::Datetime,
        Rule::TypeDecimal => NestedType::Numeric,
        Rule::TypeDouble => NestedType::Double,
        Rule::TypeInt => NestedType::Integer,
        Rule::TypeJSON => NestedType::Map,
        Rule::TypeString | Rule::TypeText => NestedType::Text,
        Rule::TypeVarchar => {
            if let Some(length_pair) = concrete_type_pair.into_inner().next() {
                length_pair.as_str().parse::<usize>().map_err(|e| {
                    SbroadError::ParsingError(
                        Entity::Value,
                        format_smolstr!("failed to parse varchar length: {e:?}."),
                    )
                })?;
            }
            NestedType::Text
        }
        Rule::TypeUuid => NestedType::Uuid,
        rule => {
            return Err(SbroadError::Unsupported(
                Entity::Type,
                Some(format_smolstr!("{rule:?} as an array element type")),
            )
            .into())
        }
    };
    Ok(CastType::Array(elem))
}

fn parse_cast_type(pair: Pair<'_, Rule>) -> AstResult<CastType> {
    debug_assert_eq!(pair.as_rule(), Rule::Type);

    let concrete_type_pair = pair.into_inner().next().ok_or_else(|| {
        parse_invariant_error(format_smolstr!("concrete type expected under `Type`"))
    })?;
    // Note: `CastType: TryFrom<&Rule>` exists only for the old grammar's
    // `Rule` type, so the new grammar rules are mapped manually here.
    let ty = match concrete_type_pair.as_rule() {
        Rule::TypeBool => CastType::Boolean,
        Rule::TypeDatetime => CastType::Datetime,
        Rule::TypeDecimal => CastType::Decimal,
        Rule::TypeDouble => CastType::Double,
        Rule::TypeInt => CastType::Integer,
        Rule::TypeString | Rule::TypeText => CastType::String,
        Rule::TypeUuid => CastType::Uuid,
        Rule::TypeVarchar => {
            // Varchar may have an optional length: `varchar(N)`. The length
            // does not affect the cast type, but must be a valid unsigned
            // integer.
            if let Some(length_pair) = concrete_type_pair.into_inner().next() {
                length_pair.as_str().parse::<usize>().map_err(|e| {
                    SbroadError::ParsingError(
                        Entity::Value,
                        format_smolstr!("failed to parse varchar length: {e:?}."),
                    )
                })?;
            }
            CastType::String
        }
        rule => {
            return Err(
                SbroadError::Unsupported(Entity::Type, Some(format_smolstr!("{rule:?}"))).into(),
            )
        }
    };
    Ok(ty)
}

fn parse_cast_op<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<RawExpr<'q>> {
    debug_assert_eq!(pair.as_rule(), Rule::CastOp);

    let mut inner_pairs = pair.into_inner();
    let expr_pair = inner_pairs
        .next()
        .ok_or_else(|| parse_invariant_error(format_smolstr!("`CastOp` must contain `Expr`")))?;
    let child = parse_expr(expr_pair, ctx)?;
    let target_pair = inner_pairs.next().ok_or_else(|| {
        parse_invariant_error(format_smolstr!("`CastOp` must contain `CastTarget`"))
    })?;
    let ty = parse_cast_target(target_pair)?;

    Ok(Expr::cast(child, ty, CastSyntax::Call))
}

fn parse_is_postfix<'q>(operand: RawExpr<'q>, op: Pair<'q, Rule>) -> AstResult<RawExpr<'q>> {
    debug_assert_eq!(op.as_rule(), Rule::IsPostfix);

    let mut inner_pairs = op.into_inner();
    let first_pair = inner_pairs
        .next()
        .ok_or_else(|| parse_invariant_error(format_smolstr!("`IsPostfix` must be non-empty")))?;
    let (is_not, value_pair) = if matches!(first_pair.as_rule(), Rule::NotFlag) {
        let value_pair = inner_pairs.next().ok_or_else(|| {
            parse_invariant_error(format_smolstr!(
                "`IsPostfix` must contain a value after NOT"
            ))
        })?;
        (true, value_pair)
    } else {
        (false, first_pair)
    };

    let value = match value_pair.as_rule() {
        Rule::True => Some(true),
        Rule::False => Some(false),
        Rule::Null | Rule::Unknown => None,
        rule => {
            return Err(parse_invariant_error(format_smolstr!(
                "`IsPostfix` value must be TRUE, FALSE, NULL or UNKNOWN, got {rule:?}"
            )))
        }
    };

    Ok(Expr::is(operand, is_not, value))
}

fn parse_trim<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<RawExpr<'q>> {
    debug_assert_eq!(pair.as_rule(), Rule::Trim);

    let mut kind: Option<TrimKind> = None;
    let mut pattern: Option<Box<RawExpr<'q>>> = None;
    let mut target: Option<RawExpr<'q>> = None;

    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::TrimKind => {
                let kind_pair = pair.into_inner().next().ok_or_else(|| {
                    parse_invariant_error(format_smolstr!(
                        "`TrimKind` must contain a concrete kind"
                    ))
                })?;
                kind = Some(match kind_pair.as_rule() {
                    Rule::TrimKindLeading => TrimKind::Leading,
                    Rule::TrimKindTrailing => TrimKind::Trailing,
                    Rule::TrimKindBoth => TrimKind::Both,
                    rule => {
                        return Err(parse_invariant_error(format_smolstr!(
                            "expected LEADING, TRAILING or BOTH, got {rule:?}"
                        )))
                    }
                });
            }
            Rule::TrimPattern => {
                let expr_pair = pair.into_inner().next().ok_or_else(|| {
                    parse_invariant_error(format_smolstr!("`TrimPattern` must contain `Expr`"))
                })?;
                pattern = Some(Box::new(parse_expr(expr_pair, ctx)?));
            }
            Rule::TrimTarget => {
                let expr_pair = pair.into_inner().next().ok_or_else(|| {
                    parse_invariant_error(format_smolstr!("`TrimTarget` must contain `Expr`"))
                })?;
                target = Some(parse_expr(expr_pair, ctx)?);
            }
            rule => {
                return Err(parse_invariant_error(format_smolstr!(
                    "expected `Trim` components, got {rule:?}"
                )))
            }
        }
    }

    let target = target.ok_or_else(|| {
        parse_invariant_error(format_smolstr!("`Trim` must contain `TrimTarget`"))
    })?;
    Ok(Expr::trim(kind, pattern, target))
}

fn parse_substring<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<RawExpr<'q>> {
    debug_assert_eq!(pair.as_rule(), Rule::Substring);

    let variant_pair = pair.into_inner().next().ok_or_else(|| {
        parse_invariant_error(format_smolstr!("`Substring` must contain a variant"))
    })?;
    let variant_rule = variant_pair.as_rule();

    let exprs = variant_pair
        .into_inner()
        .map(|expr| parse_expr(expr, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let mut exprs = exprs.into_iter();
    let mut take_expr = || -> AstResult<Box<RawExpr<'q>>> {
        Ok(Box::new(exprs.next().ok_or_else(|| {
            parse_invariant_error(format_smolstr!(
                "substring variant must contain enough expressions"
            ))
        })?))
    };

    let substring = match variant_rule {
        Rule::SubstringFromFor => Substring::FromFor(take_expr()?, take_expr()?, take_expr()?),
        Rule::SubstringRegular => Substring::Regular(take_expr()?, take_expr()?, take_expr()?),
        Rule::SubstringFor => Substring::For(take_expr()?, take_expr()?),
        Rule::SubstringFrom => Substring::From(take_expr()?, take_expr()?),
        Rule::SubstringSimilar => Substring::Similar(take_expr()?),
        _ => {
            return Err(parse_invariant_error(format_smolstr!(
                "expected `Substring` variant, got {variant_rule:?}"
            )))
        }
    };

    Ok(Expr::substring(substring))
}

fn parse_case<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<RawExpr<'q>> {
    debug_assert_eq!(pair.as_rule(), Rule::Case);

    let mut search: Option<Box<RawExpr<'q>>> = None;
    let mut when_blocks = Vec::new();
    let mut else_expr: Option<Box<RawExpr<'q>>> = None;

    for pair in pair.into_inner() {
        match pair.as_rule() {
            // The search expression can only be the first child (grammar-enforced).
            Rule::Expr => search = Some(Box::new(parse_expr(pair, ctx)?)),
            Rule::CaseWhenBlock => {
                let mut inner_pairs = pair.into_inner();
                let condition_pair = inner_pairs.next().ok_or_else(|| {
                    parse_invariant_error(format_smolstr!(
                        "`CaseWhenBlock` must contain a condition"
                    ))
                })?;
                let result_pair = inner_pairs.next().ok_or_else(|| {
                    parse_invariant_error(format_smolstr!("`CaseWhenBlock` must contain a result"))
                })?;
                when_blocks.push((
                    parse_expr(condition_pair, ctx)?,
                    parse_expr(result_pair, ctx)?,
                ));
            }
            Rule::CaseElseBlock => {
                let expr_pair = pair.into_inner().next().ok_or_else(|| {
                    parse_invariant_error(format_smolstr!("`CaseElseBlock` must contain `Expr`"))
                })?;
                else_expr = Some(Box::new(parse_expr(expr_pair, ctx)?));
            }
            rule => {
                return Err(parse_invariant_error(format_smolstr!(
                    "expected `Case` components, got {rule:?}"
                )))
            }
        }
    }

    debug_assert!(!when_blocks.is_empty());
    Ok(Expr::case(Case {
        search,
        when_blocks,
        else_expr,
    }))
}

fn parse_exists<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<RawExpr<'q>> {
    debug_assert_eq!(pair.as_rule(), Rule::Exists);

    let mut inner_pairs = pair.into_inner();
    let first_pair = inner_pairs
        .next()
        .ok_or_else(|| parse_invariant_error(format_smolstr!("`Exists` must be non-empty")))?;
    let (is_not, subquery_pair) = if matches!(first_pair.as_rule(), Rule::NotFlag) {
        let subquery_pair = inner_pairs.next().ok_or_else(|| {
            parse_invariant_error(format_smolstr!(
                "`Exists` must contain a subquery after NOT"
            ))
        })?;
        (true, subquery_pair)
    } else {
        (false, first_pair)
    };

    debug_assert_eq!(subquery_pair.as_rule(), Rule::MultisetStmt);
    let subquery = parse_multiset(subquery_pair, ctx)?;

    Ok(Expr::exists(is_not, subquery))
}

fn parse_time_function<'q>(pair: Pair<'q, Rule>) -> AstResult<RawExpr<'q>> {
    let rule = pair.as_rule();

    let precision = pair
        .into_inner()
        .next()
        .map(|precision_pair| {
            precision_pair.as_str().parse::<u32>().map_err(|_| {
                invalid_expression_error(format_smolstr!(
                    "time function precision must be an unsigned integer"
                ))
            })
        })
        .transpose()?;

    let time_function = match rule {
        Rule::CurrentDate => TimeFunction::CurrentDate,
        Rule::CurrentTime => TimeFunction::CurrentTime(precision),
        Rule::CurrentTimestamp => TimeFunction::CurrentTimestamp(precision),
        Rule::LocalTime => TimeFunction::LocalTime(precision),
        Rule::LocalTimestamp => TimeFunction::LocalTimestamp(precision),
        _ => {
            return Err(parse_invariant_error(format_smolstr!(
                "expected time function, got {rule:?}"
            )))
        }
    };

    Ok(Expr::time_function(time_function))
}
