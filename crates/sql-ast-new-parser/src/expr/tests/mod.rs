//! Tests for the expression parser.
//!
//! # Harness
//! The parser is exercised through rendering. A query is parsed and the
//! resulting tree is printed back, so one snapshot checks both that the input
//! was accepted and that it produced the tree it should have.
//!
//! This module holds the parse/render/error helpers, plus the structural
//! comparison the precedence tests need: rendering alone cannot tell two trees
//! apart once parentheses are normalized away.
//!
//!
//! # Cases
//! Split by intent between two submodules
//! * [`basic_support`] covers the expression forms one by one,
//! * [`precedence`] covers how they combine.

mod basic_support;
mod precedence;

use pest::Parser;
use smol_str::format_smolstr;

use crate::test_support::round_trip;
use crate::ParseCtx;
use sql_ast_new_grammar::{PairParser, Rule};
use sql_ast_new_nodes::expr::*;
use sql_ast_new_nodes::Raw;
use sql_ir::errors::{Entity, SbroadError};
fn parse_expr(query: &str) -> Result<Expr<'_, Raw>, SbroadError> {
    let pair = match PairParser::parse(Rule::Expr, query) {
        Ok(mut pairs) => pairs.next().expect("expected expression pair"),
        Err(error) => {
            return Err(SbroadError::ParsingError(
                Entity::Expression,
                format_smolstr!("{error}"),
            ))
        }
    };
    // `Rule::Expr` is not anchored to EOI, so it may match only a prefix
    // of the input. In a real query the enclosing statement rule fails on
    // the unconsumed tail; emulate that here by treating a partial match
    // as a parsing error.
    let tail = query[pair.as_str().len()..].trim();
    if !tail.is_empty() {
        return Err(SbroadError::ParsingError(
            Entity::Expression,
            format_smolstr!("expression parsing stopped before: `{tail}`"),
        ));
    }
    let ctx = ParseCtx::new(&pair).map_err(SbroadError::from)?;
    super::parse_expr(pair, &ctx).map_err(SbroadError::from)
}

/// Render `query`, and hold the rendering to the round trip in
/// [`crate::test_support::round_trip`]: it must re-parse and re-render
/// identically, so a rendering that drops a needed parenthesis fails here
/// rather than being pinned by a snapshot.
#[track_caller]
fn render_expr(query: &str) -> String {
    round_trip(query, |q| parse_expr(q).map(|expr| expr.to_string()))
}

fn expr_err(query: &'static str) -> String {
    parse_expr(query)
        .err()
        .expect("expected failed parsing")
        .to_string()
}
