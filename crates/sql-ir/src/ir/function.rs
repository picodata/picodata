use crate::errors::{Entity, SbroadError};
use crate::ir::aggregates::AggregateKind;
use crate::ir::node::expression::{ExprChildren, Expression};
use crate::ir::node::{ArenaType, Node32, Node96};
use crate::ir::node::{Cast, Concat, NodeId, ScalarFunction};
use crate::ir::types::CastType;
use crate::ir::Plan;
use crate::utils::normalize_name_from_sql;
use crate::utils::to_user;
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use sql_type_system::type_system::TypeAnalyzer;
use sql_type_system::TypeReport;

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
                match children.get(1) {
                    Some(_) if is_distinct => {
                        return Err(SbroadError::Invalid(
                                Entity::Query,
                                Some(format_smolstr!(
                                    "distinct GROUP_CONCAT aggregate function has only one argument. Got: {} arguments", children.len()
                                )),
                            ));
                    }
                    Some(child)
                        if !matches!(
                            self.get_expression_node(*child)?,
                            Expression::Constant(_)
                        ) =>
                    {
                        return Err(SbroadError::Invalid(
                                Entity::Query,
                                Some(format_smolstr!(
                                    "GROUP_CONCAT aggregate function second argument must be a string literal.")),
                            ));
                    }
                    _ => {}
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

    /// Add explicit casts for some Expressions in IR plan.
    /// Exact expressions:
    ///   - ScalarFunction
    ///   - Concat
    pub fn explicit_cast_func_args(
        &mut self,
        type_analyzer: &TypeAnalyzer<NodeId>,
    ) -> Result<(), SbroadError> {
        self.cast_scalar_fn_args(type_analyzer.get_report())?;
        self.cast_concat_operator_args()
    }

    /// We add these casts in order to avoid SQL errors on local execution stage
    fn cast_scalar_fn_args(&mut self, type_report: &TypeReport<NodeId>) -> Result<(), SbroadError> {
        let scalar_fns = self
            .nodes
            .iter96()
            .enumerate()
            .filter_map(|(offset, node)| {
                if let Node96::ScalarFunction(scalar_fn) = node {
                    Some((
                        NodeId {
                            offset: offset.try_into().unwrap(),
                            arena_type: ArenaType::Arena96,
                        },
                        scalar_fn.children.clone(),
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<(NodeId, Vec<NodeId>)>>();

        for (node_id, args) in scalar_fns.into_iter() {
            for (idx, arg_id) in args.iter().enumerate() {
                let arg_id = *arg_id;
                let arg_expr = self.get_expression_node(arg_id)?;

                let get_cast_type = || {
                    /*
                        `GROUP BY` aliases can still appear here as placeholders. Type analysis
                        records the type for the aliased child expression, not for the alias node.
                    */
                    DerivedType::from(type_report.get_type(&self.get_child_under_alias(arg_id)?))
                        .get()
                        .filter(|ty| ty.is_scalar())
                        .map_or(Ok(None), |ty| CastType::try_from(&ty).map(Some))
                };

                let cast_type = match arg_expr {
                    Expression::CountAsterisk(_) => continue,
                    Expression::Cast(Cast { to, .. }) => get_cast_type()?.filter(|ty| *to != *ty),
                    _ => get_cast_type()?,
                };
                let Some(cast_type) = cast_type else {
                    continue;
                };

                let cast_id = self.add_cast(arg_id, cast_type)?;
                let mut scalar_fn_node_mut = self.get_mut_expression_node(node_id)?;
                let child_mut = scalar_fn_node_mut
                    .expr_children_mut()
                    .into_iter()
                    .nth(idx)
                    .expect("`idx` is in bounds of children array");

                *child_mut = cast_id;
            }
        }

        Ok(())
    }

    /// Cast non-text argument of concat (`||`) operator into text.
    /// Each overload of the `||` operator must satisfy the following requirements:
    ///   - Both arguments must be scalar.
    ///   - At least one argument must be `Text`.
    ///     The invariant above must be consistent with overloads definition of `||`
    ///     in Picodata SQL type system (see `frontend::sql::type_system` module
    ///     in the `sql-frontend` crate).
    fn cast_concat_operator_args(&mut self) -> Result<(), SbroadError> {
        let concat_nodes = self
            .nodes
            .iter32()
            .enumerate()
            .filter_map(|(offset, node)| {
                if let Node32::Concat(concat) = node {
                    Some((
                        NodeId {
                            offset: offset.try_into().unwrap(),
                            arena_type: ArenaType::Arena32,
                        },
                        concat.clone(),
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<(NodeId, Concat)>>();

        for (node_id, concat) in concat_nodes.into_iter() {
            let args = concat.expr_children();

            for (idx, arg_id) in args.into_iter().enumerate() {
                /*
                    `GROUP BY` aliases can still appear here as placeholders. Type analysis
                    records the type for the aliased child expression.
                    Type system report does not have alias nodes.
                    Hence, we unconditionally cast all arguments to String

                    We expect e.g. such expressions
                    `1 || '1'`, `1.5 || '1'`, `'x' || 1.5 + 1`
                    `SELECT a::int AS a1 FROM t GROUP BY a1, a1 || 'x';`
                    to be valid
                */

                let cast_id = self.add_cast(arg_id, CastType::String)?;

                let mut concat_node_mut = self.get_mut_expression_node(node_id)?;
                let child_mut = concat_node_mut
                    .expr_children_mut()
                    .into_iter()
                    .nth(idx)
                    .expect("`idx` is in bounds of children array");

                *child_mut = cast_id;
            }
        }
        Ok(())
    }
}

/// Holds naming metadata for SQL function.
/// Used mostly for correct mapping between identifiers across different subsystems.
#[derive(Default)]
pub struct FunctionNameMapping {
    /// Function name in SQL as exposed to the users (e.g., in queries).
    pub sql: &'static str,
    /// Rust function in the source code, exposed using `#[tarantool::proc]`.
    pub rust_procedure: &'static str,
    /// Used when calling it via Tarantool, composed as '.' + name in sources.
    ///
    /// # Background
    /// - Tarantool looks for `lib<name>` when using plain names (exported by Picodata).
    /// - With `.<name>`, Tarantool searches the current executable instead.
    /// - This is needed for `box.func['proc_name']:call()` and `box.execute("select proc_name()")`.
    ///
    /// Using `.proc_name` makes Tarantool look for `proc_name` in the current executable,
    /// while `proc_name` would make it search for `libproc_name.so` containing `proc_name`.
    pub tarantool_symbol: &'static str,
    /// Initially, we made these functions volatile, but they lack in usability.
    /// These parameters might serve as modifiers to change the volatility to
    /// stable, allowing to a wider usage.
    /// See <https://git.picodata.io/core/picodata/-/issues/2064> for more information.
    /// **NOTE**: uses Tarantool type names.
    pub parameter_list: &'static [&'static str],
}

/// Stores all identifiers mappings for the functions.
pub const FUNCTION_NAME_MAPPINGS: &[FunctionNameMapping] = &[
    // TODO:
    // Deprecated, remove in the future version.
    // Consider using `pico_instance_uuid` instead.
    FunctionNameMapping {
        sql: "instance_uuid",
        rust_procedure: "proc_instance_uuid",
        tarantool_symbol: ".proc_instance_uuid",
        parameter_list: &[],
    },
    FunctionNameMapping {
        sql: "pico_config_file_path",
        rust_procedure: "proc_config_file",
        tarantool_symbol: ".proc_config_file",
        parameter_list: &["string"],
    },
    FunctionNameMapping {
        sql: "pico_instance_dir",
        rust_procedure: "proc_instance_dir",
        tarantool_symbol: ".proc_instance_dir",
        parameter_list: &["string"],
    },
    FunctionNameMapping {
        sql: "pico_instance_name",
        rust_procedure: "proc_instance_name",
        tarantool_symbol: ".proc_instance_name",
        parameter_list: &["string"],
    },
    FunctionNameMapping {
        sql: "pico_instance_uuid",
        rust_procedure: "proc_instance_uuid",
        tarantool_symbol: ".proc_instance_uuid",
        parameter_list: &[],
    },
    FunctionNameMapping {
        sql: "pico_raft_leader_id",
        rust_procedure: "proc_raft_leader_id",
        tarantool_symbol: ".proc_raft_leader_id",
        parameter_list: &[],
    },
    FunctionNameMapping {
        sql: "pico_raft_leader_uuid",
        rust_procedure: "proc_raft_leader_uuid",
        tarantool_symbol: ".proc_raft_leader_uuid",
        parameter_list: &[],
    },
    FunctionNameMapping {
        sql: "pico_replicaset_name",
        rust_procedure: "proc_replicaset_name",
        tarantool_symbol: ".proc_replicaset_name",
        parameter_list: &["string"],
    },
    FunctionNameMapping {
        sql: "pico_tier_name",
        rust_procedure: "proc_tier_name",
        tarantool_symbol: ".proc_tier_name",
        parameter_list: &["string"],
    },
    FunctionNameMapping {
        sql: "version",
        rust_procedure: "proc_picodata_version",
        tarantool_symbol: ".proc_picodata_version",
        parameter_list: &[],
    },
];

/// Maps (maybe quoted or uppercased) name from user to real procedure name in tarantool.
/// Real name stands for name in _func space.
pub fn get_real_function_name(name_from_sql: &str) -> Option<&'static str> {
    let normalized_name = normalize_name_from_sql(name_from_sql);
    FUNCTION_NAME_MAPPINGS
        .iter()
        .find(|&mapping| mapping.sql == normalized_name)
        .map(|mapping| mapping.tarantool_symbol)
}
