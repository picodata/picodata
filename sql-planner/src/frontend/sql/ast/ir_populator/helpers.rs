use smol_str::format_smolstr;

use crate::errors::Entity::AST;
use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::ast::{ParseNode, Rule};
use crate::ir::node::expression::Expression;
use crate::ir::node::{Alias, NodeId};
use crate::ir::types::{DerivedType, NestedType, UnrestrictedType};
use crate::ir::Plan;

/// Check if an expression of type `src` can be assigned to a column of type `dst`.
pub(in crate::frontend::sql) fn can_assign(src: DerivedType, dst: UnrestrictedType) -> bool {
    if dst == UnrestrictedType::Any {
        // Any type can be assigned to a column of type any.
        return true;
    }

    let Some(src) = *src.get() else {
        // TODO: We should probably check nullability here.
        return true;
    };

    match (src, dst) {
        (UnrestrictedType::Array(a), UnrestrictedType::Array(b)) => b == NestedType::Any || a == b,
        _ => src == dst,
    }
}

/// Get column types that dql query returns.
pub(in crate::frontend::sql) fn dql_return_columns(
    ir: &Plan,
    query_id: NodeId,
) -> Result<Vec<(String, DerivedType)>, SbroadError> {
    let output_id = ir.get_relation_node(query_id)?.output();
    let output_row = ir.get_row_list(output_id)?;
    let mut columns = Vec::with_capacity(output_row.len());
    for col_id in output_row {
        let column = ir.get_expression_node(*col_id)?;
        let column_name = if let Expression::Alias(Alias { name, .. }) = column {
            name.to_string()
        } else {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(smol_str::format_smolstr!("expected alias, got {column:?}")),
            ));
        };
        let column = ir.get_expression_node(*col_id)?;
        let column_type = column.calculate_type(ir)?;
        columns.push((column_name, column_type));
    }
    Ok(columns)
}

pub(in crate::frontend::sql) fn parse_trimmed_unsigned_from_str(
    value: &str,
) -> Result<i64, SbroadError> {
    let result = value.parse::<i64>().map_err(|_| {
        SbroadError::Invalid(
            Entity::Query,
            Some(format_smolstr!(
                "value doesn't fit into integer range: {value}"
            )),
        )
    })?;

    if result < 0 {
        return Err(SbroadError::Invalid(
            Entity::Value,
            Some("value is negative while type of value is unsigned".into()),
        ));
    }

    Ok(result)
}

/// Parses an unsigned integer value from a parsed AST node.
///
/// This function expects a node matching `Rule::Unsigned` and will attempt to parse
/// its string value into a 64-bit integer (`i64`) with guarantee that value is positive.
///
/// # Guarantees
/// - On success, the returned value is guaranteed to fit in `i64` (64-bit signed integer)
pub(in crate::frontend::sql) fn parse_unsigned(ast_node: &ParseNode) -> Result<i64, SbroadError> {
    assert!(matches!(ast_node.rule, Rule::Unsigned));
    if let Some(str_value) = ast_node.value.as_ref() {
        let result = parse_trimmed_unsigned_from_str(str_value)?;
        Ok(result)
    } else {
        Err(SbroadError::Invalid(
            AST,
            Some("Unsigned node has value".into()),
        ))
    }
}

#[derive(Clone, Debug)]
pub(in crate::frontend::sql) enum OrderNulls {
    First,
    Last,
}
