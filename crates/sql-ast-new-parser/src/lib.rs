//! Parsing: query text -> [`AbstractSyntaxTree<Raw>`].
//!
//! Parsing is driven by pest. The parser produces a [`Pair`] tree, and each
//! submodule here turns the pairs of its own grammar rules into the nodes
//! declared in `sql-ast-new-nodes`.
//!
//! [`parse`] is the entry point; everything else is one `parse_*` function per
//! grammar rule. Expression precedence is the one thing not read off the
//! grammar — see [`expr`].
//!
//! This crate and `sql-ast-new-analyzer` never reference each other; they are
//! two independent consumers of the node declarations. The analyzer's *tests*
//! parse, which is a dev-dependency in that direction and not a layering
//! violation.
//!
//!
//! # Panics
//! This crate must never panic: the frontend falls back to the old pipeline on
//! [`Err`], but a panic aborts the whole query. Internal invariants return
//! [`parse_invariant_error`].

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

mod expr;
#[cfg(test)]
mod keywords;
mod multiset;
mod pairs_traversal;
mod select;
mod table_expression;
#[cfg(test)]
mod test_support;
mod window;

use std::fmt::{self, Display, Formatter};

use pest::iterators::Pair;
use pest::Parser;
use smol_str::{format_smolstr, SmolStr};

use sql_ast_new_grammar::{PairParser, Rule};
use sql_ast_new_nodes::error::REPORT_TO_SUFFIX;
use sql_ast_new_nodes::error::{AstErr, AstResult};
use sql_ast_new_nodes::{
    AbstractSyntaxTree, DqlStmt, Forward, Node, Raw, RawAst, ReadPreference, SqlOption,
    SqlOptionParameter, SqlOptionValue,
};
use sql_ir::errors::{Action, Entity, SbroadError};

use self::expr::parse_parameter;
use self::multiset::parse_multiset;
use self::pairs_traversal::Tree;

fn failed_parsing_error(msg: SmolStr) -> AstErr {
    SbroadError::FailedTo(Action::Parse, Some(Entity::Query), msg).into()
}

/// Parse-stage internal-invariant violation (a broken "grammar guarantees ..." contract).
fn parse_invariant_error(msg: SmolStr) -> AstErr {
    failed_parsing_error(format_smolstr!("{msg}. {REPORT_TO_SUFFIX}"))
}

/// User-facing rejection of an expression the grammar accepts but the
/// language does not (a mistake in the query, not a parser invariant).
fn invalid_expression_error(msg: SmolStr) -> AstErr {
    SbroadError::Invalid(Entity::Expression, Some(msg)).into()
}

/// Parse `query` into a raw AST.
pub fn parse(query: &str) -> AstResult<RawAst<'_>> {
    // There is no protection from too deep recursion in queries.
    // Be aware in dev binary on some queries with long chains of nested statements
    // stack overflow is possible.
    let pair = match PairParser::parse(Rule::Command, query) {
        Ok(mut p) => p.next().ok_or_else(|| {
            parse_invariant_error(format_smolstr!(
                "query expected as a first parsing tree child"
            ))
        })?,
        Err(e) => {
            return Err(SbroadError::ParsingError(Entity::Rule, format_smolstr!("{e}")).into());
        }
    };

    let ctx = ParseCtx::new(&pair)?;

    let root = match pair.as_rule() {
        Rule::EmptyQuery => Node::Empty,
        Rule::DqlStmt => parse_dql(pair, &ctx)?,
        _ => {
            return Err(failed_parsing_error(format_smolstr!(
                "cannot yet parse queries other than DQL"
            )));
        }
    };

    Ok(AbstractSyntaxTree { root })
}

struct ExpectedRules<'a>(&'a [Rule]);

impl Display for ExpectedRules<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for (i, rule) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("/")?;
            }
            write!(f, "{rule:?}")?;
        }
        Ok(())
    }
}

fn unexpected_rule_error(expected: ExpectedRules, pair: &Pair<'_, Rule>) -> AstErr {
    parse_invariant_error(format_smolstr!(
        "expected {} `Rule`s, got: {:?}",
        expected,
        pair.as_rule()
    ))
}

/// `$1..$65535`: the index range a [`Parameter`](sql_ast_new_nodes::expr::Parameter)
/// can hold.
const MAX_PARAMETER_INDEX: usize = u16::MAX as usize;

/// Whole-query facts every `parse_*` function is threaded, decided by
/// [`new`](ParseCtx::new) before parsing starts. Nothing here changes as the
/// parse proceeds, which is why it travels as a plain `&`.
struct ParseCtx {
    /// Byte offsets of the query's `?` placeholders, in source order; a
    /// placeholder's `$n` index is its position here plus one.
    ///
    /// Numbering is read off the query text rather than counted as the parser
    /// goes, because parse order is not source order: a BETWEEN's middle
    /// operand rides inside the operator token and is parsed only once the
    /// upper bound is built (see [`expr`]).
    tnt_params: Vec<usize>,
}

impl ParseCtx {
    /// One scan settles both parameter questions. `?` (Tarantool) and `$n`
    /// (PostgreSQL) styles must not be mixed within one query -- they assign
    /// indexes differently (positional vs. explicit), so a mixed query has no
    /// well-defined numbering -- and every `?` takes the index of its source
    /// position.
    fn new(root: &Pair<'_, Rule>) -> AstResult<Self> {
        let (mut tnt_params, mut has_pg) = (Vec::new(), false);
        for pair in Tree::from(root.clone()) {
            match pair.as_rule() {
                Rule::TntParameter => tnt_params.push(pair.as_span().start()),
                Rule::PgParameter => has_pg = true,
                _ => continue,
            }
            if !tnt_params.is_empty() && has_pg {
                return Err(SbroadError::UseOfBothParamsStyles.into());
            }
        }
        if tnt_params.len() > MAX_PARAMETER_INDEX {
            return Err(invalid_expression_error(format_smolstr!(
                "parameter index {} is too big (max: {MAX_PARAMETER_INDEX})",
                tnt_params.len()
            )));
        }
        // A pre-order walk of a pest tree yields pairs in source order.
        debug_assert!(tnt_params.is_sorted());
        Ok(Self { tnt_params })
    }

    /// The `$n` index of the `?` placeholder starting at `offset`.
    fn tnt_param_index(&self, offset: usize) -> Option<u16> {
        let position = self.tnt_params.binary_search(&offset).ok()?;
        u16::try_from(position + 1).ok()
    }
}

fn parse_dql<'q>(pair: Pair<'q, Rule>, ctx: &ParseCtx) -> AstResult<Node<'q, Raw>> {
    debug_assert_eq!(pair.as_rule(), Rule::DqlStmt);

    let mut stmt = None;
    let mut option = None;

    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::MultisetStmt => stmt = Some(parse_multiset(pair, ctx)?),
            Rule::SqlOption => option = Some(parse_sql_option(pair, ctx)?),
            _ => {
                return Err(unexpected_rule_error(
                    ExpectedRules(&[Rule::MultisetStmt, Rule::SqlOption]),
                    &pair,
                ));
            }
        }
    }

    let dql_stmt = DqlStmt {
        stmt: Box::new(stmt.ok_or_else(|| {
            parse_invariant_error(format_smolstr!(
                "grammar guarantees a MultisetStmt in a DQL statement"
            ))
        })?),
        option,
    };

    Ok(Node::DqlStmt(dql_stmt))
}

fn parse_sql_option(pair: Pair<'_, Rule>, ctx: &ParseCtx) -> AstResult<SqlOption> {
    debug_assert_eq!(pair.as_rule(), Rule::SqlOption);

    let mut params = pair.into_inner();
    let first = params.next().ok_or_else(|| {
        parse_invariant_error(format_smolstr!(
            "grammar guarantees at least one parameter in an OPTION clause"
        ))
    })?;

    let mut option = SqlOption::new(parse_sql_option_param(first, ctx)?);
    for pair in params {
        option.add_param(parse_sql_option_param(pair, ctx)?);
    }

    Ok(option)
}

fn parse_sql_option_param(pair: Pair<'_, Rule>, ctx: &ParseCtx) -> AstResult<SqlOptionParameter> {
    match pair.as_rule() {
        Rule::VdbeOpcodeMax => Ok(SqlOptionParameter::VdbeOpcodeMax(
            parse_unsigned_sql_option_value(pair, ctx)?,
        )),
        Rule::MotionRowMax => Ok(SqlOptionParameter::MotionRowMax(
            parse_unsigned_sql_option_value(pair, ctx)?,
        )),
        Rule::ReadPreference => Ok(SqlOptionParameter::ReadPreference(
            parse_read_preference_sql_option_value(pair, ctx)?,
        )),
        Rule::Forward => Ok(SqlOptionParameter::Forward(parse_forward_sql_option_value(
            pair, ctx,
        )?)),
        _ => Err(unexpected_rule_error(
            ExpectedRules(&[
                Rule::VdbeOpcodeMax,
                Rule::MotionRowMax,
                Rule::ReadPreference,
                Rule::Forward,
            ]),
            &pair,
        )),
    }
}

fn parse_unsigned_sql_option_value(
    pair: Pair<'_, Rule>,
    ctx: &ParseCtx,
) -> AstResult<SqlOptionValue<u64>> {
    let option_rule = pair.as_rule();
    debug_assert!(matches!(
        option_rule,
        Rule::VdbeOpcodeMax | Rule::MotionRowMax
    ));

    let value_pair = get_sql_option_value_pair(pair)?;
    match value_pair.as_rule() {
        Rule::Unsigned => Ok(SqlOptionValue::Literal(
            value_pair.as_str().parse::<u64>().map_err(|_| {
                AstErr::from(SbroadError::Invalid(
                    Entity::Option,
                    Some(format_smolstr!(
                        "{} value doesn't fit into integer range: {}",
                        sql_option_name(option_rule),
                        value_pair.as_str()
                    )),
                ))
            })?,
        )),
        Rule::Parameter => Ok(SqlOptionValue::Parameter(parse_parameter(value_pair, ctx)?)),
        _ => Err(unexpected_rule_error(
            ExpectedRules(&[Rule::Unsigned, Rule::Parameter]),
            &value_pair,
        )),
    }
}

fn parse_read_preference_sql_option_value(
    pair: Pair<'_, Rule>,
    ctx: &ParseCtx,
) -> AstResult<SqlOptionValue<ReadPreference>> {
    debug_assert_eq!(pair.as_rule(), Rule::ReadPreference);

    let value_pair = get_sql_option_value_pair(pair)?;
    match value_pair.as_rule() {
        Rule::Leader => Ok(SqlOptionValue::Literal(ReadPreference::Leader)),
        Rule::Replica => Ok(SqlOptionValue::Literal(ReadPreference::Replica)),
        Rule::Any => Ok(SqlOptionValue::Literal(ReadPreference::Any)),
        Rule::Parameter => Ok(SqlOptionValue::Parameter(parse_parameter(value_pair, ctx)?)),
        _ => Err(unexpected_rule_error(
            ExpectedRules(&[Rule::Leader, Rule::Replica, Rule::Any, Rule::Parameter]),
            &value_pair,
        )),
    }
}

fn parse_forward_sql_option_value(
    pair: Pair<'_, Rule>,
    ctx: &ParseCtx,
) -> AstResult<SqlOptionValue<Forward>> {
    debug_assert_eq!(pair.as_rule(), Rule::Forward);

    let value_pair = get_sql_option_value_pair(pair)?;
    match value_pair.as_rule() {
        Rule::ForwardOn => Ok(SqlOptionValue::Literal(Forward::On)),
        Rule::ForwardOff => Ok(SqlOptionValue::Literal(Forward::Off)),
        Rule::ForwardROtoRW => Ok(SqlOptionValue::Literal(Forward::RoToRw)),
        Rule::Parameter => Ok(SqlOptionValue::Parameter(parse_parameter(value_pair, ctx)?)),
        _ => Err(unexpected_rule_error(
            ExpectedRules(&[
                Rule::ForwardOn,
                Rule::ForwardOff,
                Rule::ForwardROtoRW,
                Rule::Parameter,
            ]),
            &value_pair,
        )),
    }
}

fn get_sql_option_value_pair<'q>(pair: Pair<'q, Rule>) -> AstResult<Pair<'q, Rule>> {
    pair.into_inner().next().ok_or_else(|| {
        parse_invariant_error(format_smolstr!(
            "`SqlOption` parameter must contain a value"
        ))
    })
}

/// Only used to name options inside error messages, so an unknown rule
/// degrades the message instead of failing the query.
fn sql_option_name(rule: Rule) -> &'static str {
    match rule {
        Rule::VdbeOpcodeMax => "sql_vdbe_opcode_max",
        Rule::MotionRowMax => "sql_motion_row_max",
        Rule::ReadPreference => "read_preference",
        Rule::Forward => "forward",
        _ => "unknown SQL option",
    }
}

#[cfg(test)]
mod tests {

    use sql_ast_new_nodes::rendering::normalize_whitespace;
    use sql_ast_new_nodes::{Node, RawAst};
    use sql_ir::errors::SbroadError;

    use super::parse;

    /// Parse, surfacing the plain [`SbroadError`] the frontend's `Ast` boundary
    /// would hand back, so the assertions below read the same as they did while
    /// the parser was a module inside `sql-frontend`.
    fn build(query: &str) -> Result<RawAst<'_>, SbroadError> {
        parse(query).map_err(SbroadError::from)
    }

    #[test]
    fn empty_query() {
        let query = "";
        let ast = build(query).expect("expected to build AST");
        assert!(matches!(ast.root, Node::Empty));
    }
    #[test]
    fn dql_with_cte_and_multiset_ops() {
        let query = r#"
            WITH
            cte1 (c1, c2) AS (SELECT * FROM t1),
            cte2 (c3, c4) AS (SELECT * FROM t2)
                (SELECT *, a, a + b * 2 AS a1, (SELECT * FROM t2) AS q1, t1.c AS c1
                FROM (SELECT * FROM t) AS t1 INDEXED BY idx
                WHERE a <> 2 AND b < 4)
            UNION ALL
                SELECT * FROM cte2
        "#;
        let ast = build(query).expect("expected to build AST");
        let ast_str = normalize_whitespace(&ast.to_string());

        insta::assert_snapshot!(ast_str, @"WITH cte1 (c1, c2) AS (SELECT * FROM t1), cte2 (c3, c4) AS (SELECT * FROM t2) (SELECT *, a, a + b * 2 AS a1, (SELECT * FROM t2) AS q1, t1.c AS c1 FROM (SELECT * FROM t) AS t1 INDEXED BY idx WHERE a <> 2 AND b < 4) UNION ALL (SELECT * FROM cte2)");
    }

    #[test]
    fn dql_with_window_functions() {
        let query = r#"
            SELECT a, row_number() OVER w, sum(b) OVER (PARTITION BY a)
            FROM t
            WHERE a > 1
            GROUP BY a
            HAVING a > 2
            WINDOW w AS (ORDER BY a)
            ORDER BY a
            LIMIT 5
        "#;
        let ast = build(query).expect("expected to build AST");
        let ast_str = normalize_whitespace(&ast.to_string());

        insta::assert_snapshot!(ast_str, @"SELECT a, row_number() OVER w, sum(b) OVER (PARTITION BY a) FROM t WHERE a > 1 GROUP BY a HAVING a > 2 WINDOW w AS (ORDER BY a ASC) ORDER BY a ASC LIMIT 5");
    }

    #[test]
    fn dql_with_all_expression_kinds() {
        let query = r#"
            SELECT count(*), CASE WHEN a IS NOT NULL THEN abs(b)::int ELSE $1 END AS c
            FROM t
            WHERE a LIKE 'x%' ESCAPE '\'
                AND b NOT BETWEEN 1 AND 10
                AND c IN (1, 2)
                AND EXISTS (SELECT 1 FROM u)
                AND TRIM(LEADING 'x' FROM name) || SUBSTRING(name FROM 1 FOR 3) <> ''
                AND ts < CURRENT_TIMESTAMP(3)
        "#;
        let ast = build(query).expect("expected to build AST");
        let ast_str = normalize_whitespace(&ast.to_string());

        insta::assert_snapshot!(ast_str, @r"SELECT count(*), CASE WHEN a IS NOT NULL THEN abs(b)::int ELSE $1 END AS c FROM t WHERE a LIKE 'x%' ESCAPE '\' AND b NOT BETWEEN 1 AND 10 AND c IN (1, 2) AND EXISTS (SELECT 1 FROM u) AND TRIM(LEADING 'x' FROM name) || SUBSTRING(name FROM 1 FOR 3) <> '' AND ts < CURRENT_TIMESTAMP(3)");
    }

    /// Operand parenthesization at statement level: bare, the query would
    /// render as `SELECT true AND false = false`, which parses back as
    /// `SELECT true AND (false = false)`.
    #[test]
    fn dql_parenthesized_boolean_under_comparison() {
        let query = "SELECT (true AND false) = false";
        let ast = build(query).expect("expected to build AST");
        insta::assert_snapshot!(
            normalize_whitespace(&ast.to_string()),
            @"SELECT (true AND false) = false"
        );
    }

    fn parse_error(query: &'static str) -> String {
        match build(query) {
            Ok(_) => panic!("expected parsing to fail"),
            Err(error) => error.to_string(),
        }
    }

    /// A dangling BETWEEN (no upper-bound `AND`) is a grammar-level syntax
    /// error: the statement rule fails on the input the expression could not
    /// consume.
    #[test]
    fn dql_with_dangling_between_fails() {
        insta::assert_snapshot!(
            parse_error("SELECT a FROM t WHERE a BETWEEN 1"),
            @"
        rule parsing error:  --> 1:34
          |
        1 | SELECT a FROM t WHERE a BETWEEN 1
          |                                  ^---
          |
          = expected ConcatInfixOp, Add, Subtract, Modulo, Multiply, Divide, Eq, Gt, GtEq, Lt, LtEq, NotEq, IndexPostfix, or CastPostfix
        "
        );
    }

    #[test]
    fn dql_with_dangling_between_in_projection_fails() {
        insta::assert_snapshot!(
            parse_error("SELECT 1 BETWEEN 2 FROM t"),
            @"
        rule parsing error:  --> 1:20
          |
        1 | SELECT 1 BETWEEN 2 FROM t
          |                    ^---
          |
          = expected ConcatInfixOp, Add, Subtract, Modulo, Multiply, Divide, Eq, Gt, GtEq, Lt, LtEq, or NotEq
        "
        );
    }

    #[test]
    fn dql_with_sql_options() {
        let query = r#"
            SELECT * FROM t
            OPTION(
                sql_vdbe_opcode_max = 42,
                sql_motion_row_max = $1,
                read_preference = replica,
                forward = ro_to_rw
            )
        "#;
        let ast = build(query).expect("expected to build AST");
        let ast_str = normalize_whitespace(&ast.to_string());

        insta::assert_snapshot!(ast_str, @"SELECT * FROM t OPTION(sql_vdbe_opcode_max = 42, sql_motion_row_max = $1, read_preference = replica, forward = ro_to_rw)");
    }

    #[test]
    fn dql_with_tnt_parameterized_sql_options() {
        let query = r#"
            VALUES (1)
            OPTION(
                sql_vdbe_opcode_max = ?,
                sql_motion_row_max = ?,
                read_preference = ?,
                forward = ?
            )
        "#;
        let ast = build(query).expect("expected to build AST");
        let ast_str = normalize_whitespace(&ast.to_string());

        insta::assert_snapshot!(ast_str, @"VALUES (1) OPTION(sql_vdbe_opcode_max = $1, sql_motion_row_max = $2, read_preference = $3, forward = $4)");
    }

    /// The whole statement is numbered in source order, across clauses: the
    /// BETWEEN bounds in the projection keep their spelled order, and the
    /// OPTION placeholder follows them.
    #[test]
    fn dql_tnt_parameters_numbered_in_source_order() {
        let query = "SELECT ? BETWEEN ? AND ? FROM t OPTION(sql_vdbe_opcode_max = ?)";
        let ast = build(query).expect("expected to build AST");
        insta::assert_snapshot!(
            normalize_whitespace(&ast.to_string()),
            @"SELECT $1 BETWEEN $2 AND $3 FROM t OPTION(sql_vdbe_opcode_max = $4)"
        );
    }

    #[test]
    fn dql_with_pg_parameterized_sql_options() {
        let query = r#"
            VALUES (1)
            OPTION(
                sql_vdbe_opcode_max = $1,
                sql_motion_row_max = $2,
                read_preference = $3,
                forward = $4
            )
        "#;
        let ast = build(query).expect("expected to build AST");
        let ast_str = normalize_whitespace(&ast.to_string());

        insta::assert_snapshot!(ast_str, @"VALUES (1) OPTION(sql_vdbe_opcode_max = $1, sql_motion_row_max = $2, read_preference = $3, forward = $4)");
    }

    #[test]
    fn mixed_param_styles_are_rejected() {
        let err = build("SELECT ?, $1 FROM t")
            .err()
            .expect("expected mixed parameter styles to be rejected");
        assert!(matches!(err, SbroadError::UseOfBothParamsStyles));

        // A single style anywhere in the query is accepted.
        assert!(build("SELECT ?, ? FROM t").is_ok());
        assert!(build("SELECT $1, $2 FROM t").is_ok());
    }

    #[test]
    fn dql_window_clause_order_is_enforced_by_grammar() {
        let query = r#"SELECT sum(a) OVER (ORDER BY a PARTITION BY b) FROM t"#;
        assert!(build(query).is_err());

        let query = r#"SELECT sum(a) OVER (PARTITION BY a PARTITION BY b) FROM t"#;
        assert!(build(query).is_err());
    }

    // ---- Identifier normalization ----

    #[test]
    fn ident_normalization_rules() {
        use sql_ast_new_nodes::Ident;
        // Unquoted identifiers fold to lowercase — Unicode-aware, not ASCII-only.
        insta::assert_snapshot!(Ident::from_sql("AbC").as_str(), @"abc");
        insta::assert_snapshot!(Ident::from_sql("Ф").as_str(), @"ф");
        // Quoted identifiers lose the delimiters, keep their case, and
        // collapse the doubled-quote escape (read `"a""b"` as `a"b`).
        insta::assert_snapshot!(Ident::from_sql("\"AbC\"").as_str(), @"AbC");
        insta::assert_snapshot!(Ident::from_sql("\"a\"\"b\"").as_str(), @"a\"b");
        // `from_sql` stays total on the empty spelling even though the grammar
        // rejects `""` (see `zero_length_delimited_identifier_is_rejected`).
        insta::assert_snapshot!(Ident::from_sql("\"\"").as_str(), @"");
    }

    /// `Display` must emit SQL that parses back to the same identifier: bare
    /// only for case-fold-invariant regular identifiers, delimited (with the
    /// `""` escape) for everything else — including reserved keywords, which
    /// only the quoted spelling can denote.
    #[test]
    fn ident_display_renders_reparseable_sql() {
        use sql_ast_new_nodes::Ident;
        insta::assert_snapshot!(Ident::from_sql("AbC").to_string(), @"abc");
        insta::assert_snapshot!(Ident::from_sql("ф").to_string(), @"ф");
        insta::assert_snapshot!(Ident::from_sql("\"AbC\"").to_string(), @"\"AbC\"");
        insta::assert_snapshot!(Ident::from_sql("\"array\"").to_string(), @"\"array\"");
        insta::assert_snapshot!(Ident::from_sql("\"a b\"").to_string(), @"\"a b\"");
        insta::assert_snapshot!(Ident::from_sql("\"a\"\"b\"").to_string(), @"\"a\"\"b\"");
    }

    /// Every identifier position not reachable from `Rule::Expr`: CTE name and
    /// column list, table names, table alias, asterisk qualifier, projection
    /// alias, subquery columns. Unquoted spellings fold to lowercase; quoted
    /// ones keep case and lose the delimiters.
    #[test]
    fn dql_identifiers_normalized_in_all_positions() {
        let query = r#"
            WITH "Cte" (C1, "C2") AS (SELECT * FROM T1)
            SELECT Tt.*, A + B AS "Sum"
            FROM T AS "Alias"
            WHERE X IN (SELECT "y" FROM Z)
        "#;
        let ast = build(query).expect("expected to build AST");
        insta::assert_snapshot!(
            normalize_whitespace(&ast.to_string()),
            @r#"WITH "Cte" (c1, "C2") AS (SELECT * FROM t1) SELECT tt.*, a + b AS "Sum" FROM t AS "Alias" WHERE x IN (SELECT y FROM z)"#
        );
    }

    /// Window names in every position: `OVER w`, a base window inside an
    /// inline spec, and the `WINDOW` clause (unquoted and quoted).
    #[test]
    fn dql_window_identifiers_normalized() {
        let query = r#"
            SELECT Row_Number() OVER W, SUM(B) OVER (W1 ORDER BY C)
            FROM t
            WINDOW W AS (ORDER BY a), "W2" AS (ORDER BY b)
        "#;
        let ast = build(query).expect("expected to build AST");
        insta::assert_snapshot!(
            normalize_whitespace(&ast.to_string()),
            @r#"SELECT row_number() OVER w, sum(b) OVER (w1 ORDER BY c ASC) FROM t WINDOW w AS (ORDER BY a ASC), "W2" AS (ORDER BY b ASC)"#
        );
    }

    #[test]
    fn dql_using_and_indexed_by_identifiers_normalized() {
        let ast =
            build(r#"SELECT * FROM t JOIN t2 USING (A, "B")"#).expect("expected to build AST");
        insta::assert_snapshot!(
            normalize_whitespace(&ast.to_string()),
            @r#"SELECT * FROM t INNER JOIN t2 USING (a, "B")"#
        );

        let ast = build("SELECT a FROM (SELECT * FROM t) AS T1 INDEXED BY IdX")
            .expect("expected to build AST");
        insta::assert_snapshot!(
            normalize_whitespace(&ast.to_string()),
            @"SELECT a FROM (SELECT * FROM t) AS t1 INDEXED BY idx"
        );
    }

    /// Identifiers that cannot be spelled bare (case-preserved, embedded
    /// space) render re-quoted, so the render → re-parse → identical re-render
    /// contract holds for quoted identifiers too.
    #[test]
    fn quoted_identifier_rendering_roundtrips() {
        let ast = build(r#"SELECT 1 AS "Weird Alias""#).expect("expected to build AST");
        let rendered = normalize_whitespace(&ast.to_string());
        insta::assert_snapshot!(&rendered, @r#"SELECT 1 AS "Weird Alias""#);

        let reparsed = build(&rendered).expect("render must re-parse");
        assert_eq!(normalize_whitespace(&reparsed.to_string()), rendered);
    }

    /// A zero-length delimited identifier is a syntax error.
    #[test]
    fn zero_length_delimited_identifier_is_rejected() {
        assert!(build(r#"SELECT "" FROM t1"#).is_err());
        assert!(build(r#"SELECT a FROM """#).is_err());
        // The escaped-quote spelling `""""` (the identifier `"`) stays legal.
        assert!(build(r#"SELECT a FROM """""#).is_ok());
    }
}
