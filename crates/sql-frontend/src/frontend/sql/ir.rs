use std::collections::HashMap;

use pest::iterators::Pair;
use smol_str::format_smolstr;
use tarantool::decimal::Decimal;

use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::ast::ir_populator::parse_trimmed_unsigned_from_str;
use crate::frontend::sql::ast::Rule;
use crate::ir::node::NodeId;
use crate::ir::value::double::Double;
use crate::ir::value::Value;

use crate::frontend::sql::ast::escape_single_quotes;

/// Creates `Value` from pest pair.
///
/// # Errors
/// Returns `SbroadError` when the operator is invalid.
pub(super) fn value_from_node(pair: &Pair<Rule>) -> Result<Value, SbroadError> {
    let pair_string = pair.as_str();

    match pair.as_rule() {
        Rule::False => Ok(false.into()),
        Rule::True => Ok(true.into()),
        Rule::Null => Ok(Value::Null),
        Rule::Integer => Ok(pair_string
            .parse::<i64>()
            .map_err(|e| {
                SbroadError::ParsingError(Entity::Value, format_smolstr!("i64 parsing error {e}"))
            })?
            .into()),
        Rule::Decimal => Ok(pair_string
            .parse::<Decimal>()
            .map_err(|e| {
                SbroadError::ParsingError(
                    Entity::Value,
                    format_smolstr!("decimal parsing error {e:?}"),
                )
            })?
            .into()),
        Rule::Double => Ok(pair_string
            .parse::<Double>()
            .map_err(|e| {
                SbroadError::ParsingError(
                    Entity::Value,
                    format_smolstr!("double parsing error {e}"),
                )
            })?
            .into()),
        Rule::Unsigned => {
            let unsigned = parse_trimmed_unsigned_from_str(pair_string)?;
            let value = unsigned.into();
            Ok(value)
        }
        Rule::SingleQuotedString => {
            let pair_str = pair.as_str();
            let inner = &pair_str[1..pair_str.len() - 1];
            Ok(escape_single_quotes(inner).into())
        }
        _ => Err(SbroadError::Unsupported(
            Entity::Type,
            Some("can not create Value from ParseNode".into()),
        )),
    }
}

#[derive(Debug)]
/// Helper struct representing map of { `ParseNode` id -> `Node` id }
pub(super) struct Translation {
    map: HashMap<usize, NodeId>,
}

impl Translation {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Translation {
            map: HashMap::with_capacity(capacity),
        }
    }

    pub(super) fn add(&mut self, parse_id: usize, plan_id: NodeId) {
        self.map.insert(parse_id, plan_id);
    }

    pub(super) fn get(&self, old: usize) -> Result<NodeId, SbroadError> {
        self.map.get(&old).copied().ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("(parse node) [{old}] in translation map"),
            )
        })
    }
}

pub use crate::ir::subtree_cloner::SubtreeCloner;

#[cfg(test)]
mod tests;
