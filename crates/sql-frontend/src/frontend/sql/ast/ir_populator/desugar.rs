//! Functions that are pure syntactic sugar over native expressions.
//!
//! These are recognized at plan-build time and rewritten into plan nodes, so
//! they never reach function resolution and need no storage-side `_func`
//! object. Their typing and rendering come from the operators they expand to.

use crate::errors::{Entity, SbroadError};
use crate::ir::node::NodeId;
use crate::ir::Plan;

pub(crate) enum SugaredFunction {
    JsonExtractPath,
}

impl SugaredFunction {
    /// Returns `None` if the name is not sugar and should be resolved as an
    /// ordinary function.
    ///
    /// The parser gives the name normalized, and `Metadata::function` matches
    /// it as given, so the spellings intercepted here are the same ones that
    /// would otherwise resolve from the builtin catalog.
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        match name {
            "json_extract_path" => Some(Self::JsonExtractPath),
            _ => None,
        }
    }

    pub(crate) fn desugar(self, plan: &mut Plan, args: &[NodeId]) -> Result<NodeId, SbroadError> {
        match self {
            Self::JsonExtractPath => desugar_json_extract_path(plan, args),
        }
    }
}

/// Expand `json_extract_path(src, k1, k2, ...)` into the subscript chain
/// `src[k1][k2]...`.
fn desugar_json_extract_path(plan: &mut Plan, args: &[NodeId]) -> Result<NodeId, SbroadError> {
    let Some((source, keys)) = args.split_first() else {
        return Err(json_extract_path_arity_error());
    };
    if keys.is_empty() {
        return Err(json_extract_path_arity_error());
    }

    plan.add_index(*source, keys.to_vec())
}

fn json_extract_path_arity_error() -> SbroadError {
    SbroadError::Invalid(
        Entity::Query,
        Some("json_extract_path expects at least two arguments".into()),
    )
}
