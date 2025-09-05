use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::to_user;
use crate::ir::aggregates::AggregateKind;
use crate::ir::node::{NodeId, ScalarFunction};
use crate::ir::Plan;
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};

use super::expression::{FunctionFeature, VolatilityType};
use super::types::{DerivedType, UnrestrictedType};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Function {
    pub name: SmolStr,
    pub volatility: VolatilityType,
    pub func_type: DerivedType,
    /// True if this function is provided by tarantool,
    /// when referencing this func in local sql, we must
    /// not use quotes
    pub is_system: bool,
}

impl Function {
    #[must_use]
    pub fn new(
        name: SmolStr,
        volatility: VolatilityType,
        func_type: DerivedType,
        is_system: bool,
    ) -> Self {
        Self {
            name,
            volatility,
            func_type,
            is_system,
        }
    }

    #[must_use]
    pub fn new_stable(name: SmolStr, func_type: DerivedType, is_system: bool) -> Self {
        Self::new(name, VolatilityType::Stable, func_type, is_system)
    }

    #[must_use]
    pub fn new_volatile(name: SmolStr, func_type: DerivedType, is_system: bool) -> Self {
        Self::new(name, VolatilityType::Volatile, func_type, is_system)
    }

    #[must_use]
    pub fn is_stable(&self) -> bool {
        matches!(self.volatility, VolatilityType::Stable)
    }

    #[must_use]
    pub fn is_volatile(&self) -> bool {
        matches!(self.volatility, VolatilityType::Volatile)
    }
}

impl Plan {
    /// Adds a stable function to the plan.
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
        let func_expr = ScalarFunction {
            name: function.name.to_smolstr(),
            children,
            feature,
            func_type: function.func_type,
            is_system: function.is_system,
            volatility_type: function.volatility,
            is_window: false,
        };
        let func_id = self.nodes.push(func_expr.into());
        Ok(func_id)
    }

    /// Adds a volatile function to the plan.
    ///
    /// # Errors
    /// - Function is not volatile.
    /// - Function is not found in the plan.
    pub fn add_volatile_function(
        &mut self,
        function: &Function,
        children: Vec<NodeId>,
        feature: Option<FunctionFeature>,
    ) -> Result<NodeId, SbroadError> {
        if !function.is_volatile() {
            return Err(SbroadError::Invalid(
                Entity::VolatileFunction,
                Some(format_smolstr!(
                    "function {} is not volatile",
                    function.name
                )),
            ));
        }

        let func_expr = ScalarFunction {
            name: function.name.to_smolstr(),
            children,
            feature,
            func_type: function.func_type,
            is_system: function.is_system,
            volatility_type: function.volatility,
            is_window: false,
        };
        let func_id = self.nodes.push(func_expr.into());
        Ok(func_id)
    }

    /// Add aggregate function to plan
    pub fn add_aggregate_function(
        &mut self,
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
                            to_user(kind.to_string())
                        )),
                    ));
                }
            }
        }
        let feature = if is_distinct {
            Some(FunctionFeature::Distinct)
        } else {
            None
        };
        let func_expr = ScalarFunction {
            name: kind.to_smolstr(),
            func_type: kind.get_type(self, &children)?,
            children,
            feature,
            is_system: true,
            volatility_type: super::expression::VolatilityType::Stable,
            is_window: false,
        };
        let id = self.nodes.push(func_expr.into());
        Ok(id)
    }

    /// Add builtin window function to plan
    pub fn add_builtin_window_function(
        &mut self,
        func_name: SmolStr,
        children: Vec<NodeId>,
    ) -> Result<NodeId, SbroadError> {
        let kind = AggregateKind::from_name(&func_name);
        let (func_name, func_type) = match kind {
            Some(kind) => (kind.to_smolstr(), kind.get_type(self, &children)?),
            None => {
                let derived_type = match func_name.as_str() {
                    "row_number" => DerivedType::new(UnrestrictedType::Integer),
                    "last_value" => {
                        if children.len() != 1 {
                            return Err(SbroadError::Invalid(
                                Entity::Query,
                                Some(format_smolstr!(
                                    "window function {} expects 1 argument, got {}",
                                    func_name,
                                    children.len()
                                )),
                            ));
                        }
                        let param = self.get_expression_node(children[0])?;
                        param.calculate_type(self)?
                    }
                    _ => {
                        return Err(SbroadError::Invalid(
                            Entity::Query,
                            Some(format_smolstr!(
                                "window function {} does not exist",
                                func_name
                            )),
                        ))
                    }
                };
                (func_name, derived_type)
            }
        };

        let builtin_func = ScalarFunction {
            name: func_name,
            children,
            feature: None,
            func_type,
            is_system: true,
            is_window: true,
            volatility_type: VolatilityType::Stable,
        };
        let id = self.nodes.push(builtin_func.into());
        Ok(id)
    }
}
