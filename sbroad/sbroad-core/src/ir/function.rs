use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::to_user;
use crate::ir::aggregates::AggregateKind;
use crate::ir::node::{NodeId, StableFunction};
use crate::ir::relation::Type;
use crate::ir::Plan;
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};

use super::expression::FunctionFeature;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum Behavior {
    /// The function is a stable function, it does not have any side effects.
    /// It cannot modify the database, and that within a single table scan it
    /// will consistently return the same result for the same argument values,
    /// but that its result could change across SQL statements.
    /// This type of functions can be executed on any node.
    Stable,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Function {
    pub name: SmolStr,
    pub behavior: Behavior,
    pub func_type: Type,
    /// True if this function is provided by tarantool,
    /// when referencing this func in local sql, we must
    /// not use quotes
    pub is_system: bool,
}

impl Function {
    #[must_use]
    pub fn new(name: SmolStr, behavior: Behavior, func_type: Type, is_system: bool) -> Self {
        Self {
            name,
            behavior,
            func_type,
            is_system,
        }
    }

    #[must_use]
    pub fn new_stable(name: SmolStr, func_type: Type, is_system: bool) -> Self {
        Self::new(name, Behavior::Stable, func_type, is_system)
    }

    #[must_use]
    pub fn is_stable(&self) -> bool {
        matches!(self.behavior, Behavior::Stable)
    }
}

impl Plan {
    /// Adds a stable function to the plan.
    ///
    /// # Errors
    /// - Function is not stable.
    /// - Function is not found in the plan.
    pub fn add_stable_function(
        &mut self,
        function: &Function,
        children: Vec<NodeId>,
        feature: Option<FunctionFeature>,
    ) -> Result<NodeId, SbroadError> {
        if !function.is_stable() {
            return Err(SbroadError::Invalid(
                Entity::SQLFunction,
                Some(format_smolstr!("function {} is not stable", function.name)),
            ));
        }
        let func_expr = StableFunction {
            name: function.name.to_smolstr(),
            children,
            feature,
            func_type: function.func_type,
            is_system: function.is_system,
        };
        let func_id = self.nodes.push(func_expr.into());
        Ok(func_id)
    }

    /// Add aggregate function to plan
    ///
    /// # Errors
    /// - Invalid arguments for given aggregate function
    ///
    /// # Panics
    /// - never
    pub fn add_aggregate_function(
        &mut self,
        function: &str,
        kind: AggregateKind,
        children: Vec<NodeId>,
        is_distinct: bool,
    ) -> Result<NodeId, SbroadError> {
        match kind {
            AggregateKind::GRCONCAT => {
                if children.len() > 2 || children.is_empty() {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format_smolstr!(
                            "GROUP_CONCAT aggregate function can have one or two arguments at most. Got: {} arguments", children.len()
                        )),
                    ));
                }
                if is_distinct && children.len() == 2 {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format_smolstr!(
                            "distinct GROUP_CONCAT aggregate function has only one argument. Got: {} arguments", children.len()
                        )),
                    ));
                }
            }
            _ => {
                if children.len() != 1 {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format_smolstr!(
                            "Expected one argument for aggregate: {}.",
                            to_user(function)
                        )),
                    ));
                }
            }
        }
        kind.check_args_types(self, &children)?;
        let feature = if is_distinct {
            Some(FunctionFeature::Distinct)
        } else {
            None
        };
        let func_expr = StableFunction {
            name: function.to_lowercase().to_smolstr(),
            children,
            feature,
            func_type: Type::from(kind),
            is_system: true,
        };
        let id = self.nodes.push(func_expr.into());
        Ok(id)
    }
}
