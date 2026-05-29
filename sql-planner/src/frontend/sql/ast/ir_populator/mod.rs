//! Old-AST SQL IR population helpers.
//!
//! This module contains the machinery used by the old pest AST to populate
//! the IR. The parent `frontend::sql` module remains the public SQL facade.

mod expression_ir;
mod expression_parser;
mod expression_walker;
mod helpers;
mod params;
mod plan_ext;
mod select_set;
mod values;

pub(in crate::frontend::sql) use expression_ir::{
    connect_escape_to_like_node, find_interim_between, try_deconstruct_between_expr,
    ParseExpression, ParseExpressionInfixOperator,
};
pub(in crate::frontend::sql) use expression_parser::{parse_expr_no_type_check, parse_scalar_expr};
pub(in crate::frontend::sql) use expression_walker::ExpressionWalker;
pub(in crate::frontend::sql) use helpers::{
    can_assign, dql_return_columns, parse_trimmed_unsigned_from_str, parse_unsigned, OrderNulls,
};
pub(in crate::frontend::sql) use params::{parse_param, parse_parameter_for_option};
pub(in crate::frontend::sql) use select_set::parse_select;
pub(in crate::frontend::sql) use values::parse_values_rows;
