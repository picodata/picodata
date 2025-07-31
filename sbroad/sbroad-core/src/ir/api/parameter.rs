use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::is_negative_number;
use crate::ir::expression::{FunctionFeature, Substring};
use crate::ir::node::expression::{Expression, MutExpression};
use crate::ir::node::relational::Relational;
use crate::ir::node::{
    Alias, Constant, MutNode, Node96, NodeId, Parameter, ScalarFunction, Timestamp, ValuesRow,
};
use crate::ir::node::{Node32, TimeParameters};
use crate::ir::tree::traversal::{LevelNode, PostOrder, PostOrderWithFilter, EXPR_CAPACITY};
use crate::ir::types::{DerivedType, UnrestrictedType};
use crate::ir::value::Value;
use crate::ir::{ArenaType, Node, OptionParamValue, Plan};
use smol_str::{format_smolstr, SmolStr};
use tarantool::datetime::Datetime;
use time::{OffsetDateTime, Time};

use ahash::AHashSet;

// Calculate the maximum parameter index value.
// For example, the result for a query `SELECT $1, $1, $2` will be 2.
fn count_max_parameter_index(
    plan: &Plan,
    param_node_ids: &AHashSet<NodeId>,
) -> Result<usize, SbroadError> {
    let mut params_count = 0;
    for param_id in param_node_ids {
        let param = plan.get_expression_node(*param_id)?;
        if let Expression::Parameter(Parameter { index, .. }) = param {
            params_count = std::cmp::max(*index as usize, params_count);
        }
    }
    Ok(params_count)
}

/// Replace parameters in the plan.
fn bind_params(
    plan: &mut Plan,
    param_node_ids: &AHashSet<NodeId>,
    values: &[Value],
) -> Result<(), SbroadError> {
    for param_id in param_node_ids {
        let node = plan.get_expression_node(*param_id)?;
        if let Expression::Parameter(Parameter { index, .. }) = node {
            let value = values[(index - 1) as usize].clone();
            let constant = Constant { value };
            plan.nodes
                .replace32(*param_id, Node32::Constant(constant))?;
        }
    }

    Ok(())
}

impl Plan {
    pub fn add_param(&mut self, index: u16, param_type: DerivedType) -> NodeId {
        self.nodes.push(Parameter { index, param_type }.into())
    }

    /// Bind params related to `Option` clause.
    pub fn bind_option_params(&mut self, values: &[Value]) {
        let mut params = Vec::new();
        for opt in self.raw_options.iter() {
            if let OptionParamValue::Parameter { plan_id: param_id } = opt.val {
                if let Expression::Parameter(Parameter { index, .. }) =
                    self.get_expression_node(param_id).unwrap()
                {
                    params.push((param_id, *index));
                } else {
                    panic!("OptionParamValue::Parameter does not reffer to parameter node");
                }
            }
        }

        for opt in self.raw_options.iter_mut() {
            if let OptionParamValue::Parameter { plan_id: param_id } = opt.val {
                let index = params.iter().find(|x| x.0 == param_id).unwrap().1 as usize - 1;
                let val = values[index].clone();
                opt.val = OptionParamValue::Value { val };
            }
        }
    }

    // Gather all parameter nodes from the tree to a hash set.
    #[must_use]
    pub fn get_param_set(&self) -> AHashSet<NodeId> {
        self.nodes
            .arena32
            .iter()
            .enumerate()
            .filter_map(|(id, node)| {
                if let Node32::Parameter(_) = node {
                    Some(NodeId {
                        offset: u32::try_from(id).unwrap(),
                        arena_type: ArenaType::Arena32,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    /// Synchronize values row output with the data tuple after parameter binding.
    ///
    /// ValuesRow fields `data` and `output` are referencing the same nodes. We exclude
    /// their output from nodes traversing. And in case parameters are met under ValuesRow, we don't update
    /// references in its output. That why we have to traverse the tree one more time fixing output.
    fn update_values_row(&mut self, id: NodeId) -> Result<(), SbroadError> {
        let values_row = self.get_node(id)?;
        let (output_id, data_id) =
            if let Node::Relational(Relational::ValuesRow(ValuesRow { output, data, .. })) =
                values_row
            {
                (*output, *data)
            } else {
                panic!("Expected a values row: {values_row:?}")
            };
        let data = self.get_expression_node(data_id)?;
        let data_list = data.clone_row_list()?;
        let output = self.get_expression_node(output_id)?;
        let output_list = output.clone_row_list()?;
        for (pos, alias_id) in output_list.iter().enumerate() {
            let new_child_id = *data_list
                .get(pos)
                .unwrap_or_else(|| panic!("Node not found at position {pos}"));
            let alias = self.get_mut_expression_node(*alias_id)?;
            if let MutExpression::Alias(Alias { ref mut child, .. }) = alias {
                *child = new_child_id;
            } else {
                panic!("Expected an alias: {alias:?}")
            }
        }
        Ok(())
    }

    fn update_value_rows(&mut self, nodes: &[LevelNode<NodeId>]) -> Result<(), SbroadError> {
        for LevelNode(_, id) in nodes {
            if let Ok(Node::Relational(Relational::ValuesRow(_))) = self.get_node(*id) {
                self.update_values_row(*id)?;
            }
        }
        Ok(())
    }

    fn recalculate_ref_types(&mut self) -> Result<(), SbroadError> {
        let ref_nodes = {
            let filter = |node_id| {
                matches!(
                    self.get_node(node_id),
                    Ok(Node::Expression(Expression::Reference(_)))
                )
            };
            let mut tree = PostOrderWithFilter::with_capacity(
                |node| self.parameter_iter(node, true),
                EXPR_CAPACITY,
                Box::new(filter),
            );
            let top_id = self.get_top()?;
            tree.populate_nodes(top_id);
            tree.take_nodes()
        };

        for LevelNode(_, id) in &ref_nodes {
            // Before binding, references that referred to
            // parameters had an unknown types,
            // but in fact they should have the types of given parameters.
            let new_type = if let Node::Expression(ref mut expr @ Expression::Reference(_)) =
                self.get_node(*id)?
            {
                Some(expr.recalculate_ref_type(self)?)
            } else {
                None
            };

            if let Some(new_type) = new_type {
                let MutNode::Expression(ref mut expr @ MutExpression::Reference { .. }) =
                    self.get_mut_node(*id)?
                else {
                    panic!("Reference expected to set recalculated type")
                };
                expr.set_ref_type(new_type);
            }
        }
        Ok(())
    }

    /// Substitute parameters to the plan.
    /// The purpose of this function is to find every `Expression::Parameter` node and replace it
    /// with `Expression::Constant` (under the row).
    #[allow(clippy::too_many_lines)]
    pub fn bind_params(&mut self, values: &[Value]) -> Result<(), SbroadError> {
        let param_node_ids = self.get_param_set();
        // As parameter indexes are used as indexes in parameters array,
        // we expect that the number of parameters is not less than the max index.
        let params_count = count_max_parameter_index(self, &param_node_ids)?;

        if params_count == 0 {
            return Ok(());
        }

        // Extra values are ignored.
        if params_count > values.len() {
            return Err(SbroadError::Invalid(
                Entity::Query,
                Some(format_smolstr!(
                    "expected {} values for parameters, got {}",
                    params_count,
                    values.len(),
                )),
            ));
        }

        // Note: `need_output` is set to false for `subtree_iter` specially to avoid traversing
        //       the same nodes twice. See `update_values_row` for more info.
        let mut tree =
            PostOrder::with_capacity(|node| self.subtree_iter(node, false), self.nodes.len());
        let top_id = self.get_top()?;
        tree.populate_nodes(top_id);
        let nodes = tree.take_nodes();

        if !self.raw_options.is_empty() {
            self.bind_option_params(values);
        }

        bind_params(self, &param_node_ids, values)?;

        self.update_value_rows(&nodes)?;
        self.recalculate_ref_types()?;
        Ok(())
    }

    /// Replaces the timestamp functions with corresponding constants
    pub fn update_timestamps(mut self) -> Result<Self, SbroadError> {
        // we use `time` crate to represent time, but it has trouble determining UTC offset on linux
        // because it relies on libc and libc API is unsound in presence of more than one thread (fun™ :/)
        // so we utilize `chrono` to get the actual local time and then convert it to `time::OffsetDateTime`
        // (chrono reimplements the part of libc that parses the timezone database, so it doesn't have the issue)

        let time = chrono::offset::Local::now().fixed_offset();
        let local_datetime =
            OffsetDateTime::from_unix_timestamp_nanos(time.timestamp_nanos_opt().unwrap() as i128)
                .unwrap()
                .to_offset(
                    time::UtcOffset::from_whole_seconds(time.offset().local_minus_utc()).unwrap(),
                );

        for node in self.nodes.arena32.iter_mut() {
            if let Node32::Timestamp(timestamp) = node {
                *node = Node32::Constant(Constant {
                    value: Self::create_datetime_value(local_datetime, timestamp)?,
                });
            }
        }

        Ok(self)
    }

    pub fn update_substring(self) -> Result<Self, SbroadError> {
        self.try_transform_to_substr()?.check_parameter_types()
    }

    fn try_transform_to_substr(mut self) -> Result<Self, SbroadError> {
        // Change new_names to store an owned SmolStr instead of a reference.
        let mut new_names: Vec<(NodeId, SmolStr)> = Vec::new();

        for (id, node) in self.nodes.arena96.iter().enumerate() {
            let Node96::ScalarFunction(_) = node else {
                continue;
            };
            let node_id = NodeId {
                offset: u32::try_from(id).unwrap(),
                arena_type: ArenaType::Arena96,
            };

            if let Node::Expression(Expression::ScalarFunction(ScalarFunction {
                children,
                feature,
                ..
            })) = self.get_node(node_id)?
            {
                if let Some(FunctionFeature::Substring(Substring::From)) = feature {
                    let is_second_parameter_number = matches!(
                        self.calculate_expression_type(children[1])?
                            .unwrap_or(UnrestrictedType::Any),
                        UnrestrictedType::Integer
                    );
                    if is_second_parameter_number {
                        // Create a new owned SmolStr.
                        let new_name = SmolStr::from("substr");
                        new_names.push((node_id, new_name));
                    }
                }
                if let Some(FunctionFeature::Substring(Substring::FromFor | Substring::Regular)) =
                    feature
                {
                    let is_second_parameter_number = matches!(
                        self.calculate_expression_type(children[1])?
                            .unwrap_or(UnrestrictedType::Any),
                        UnrestrictedType::Integer
                    );
                    let is_third_parameter_number = matches!(
                        self.calculate_expression_type(children[2])?
                            .unwrap_or(UnrestrictedType::Any),
                        UnrestrictedType::Integer
                    );
                    if is_second_parameter_number && is_third_parameter_number {
                        // Create a new owned SmolStr.
                        let new_name = SmolStr::from("substr");
                        new_names.push((node_id, new_name));
                    }
                }
            }
        }

        for (node_id, new_name) in new_names {
            if let MutNode::Expression(MutExpression::ScalarFunction(ScalarFunction {
                name,
                is_system,
                ..
            })) = self.get_mut_node(node_id)?
            {
                *name = new_name;
                *is_system = true;
            }
        }
        Ok(self)
    }

    fn check_parameter_types(mut self) -> Result<Self, SbroadError> {
        let mut new_names: Vec<(NodeId, SmolStr)> = Vec::new();

        for (id, node) in self.nodes.arena96.iter().enumerate() {
            let Node96::ScalarFunction(_) = node else {
                continue;
            };
            let node_id = NodeId {
                offset: u32::try_from(id).unwrap(),
                arena_type: ArenaType::Arena96,
            };

            if let Node::Expression(Expression::ScalarFunction(ScalarFunction {
                name,
                children,
                feature: Some(FunctionFeature::Substring(substr)),
                ..
            })) = self.get_node(node_id)?
            {
                let is_first_parameter_string = matches!(
                    self.calculate_expression_type(children[0])?
                        .unwrap_or(UnrestrictedType::String),
                    UnrestrictedType::String,
                );
                let is_second_parameter_number = matches!(
                    self.calculate_expression_type(children[1])?
                        .unwrap_or(UnrestrictedType::Integer),
                    UnrestrictedType::Integer
                );
                let is_second_parameter_string = matches!(
                    self.calculate_expression_type(children[1])?
                        .unwrap_or(UnrestrictedType::String),
                    UnrestrictedType::String,
                );

                match substr {
                    Substring::FromFor | Substring::Regular => {
                        let is_third_parameter_number = matches!(
                            self.calculate_expression_type(children[2])?
                                .unwrap_or(UnrestrictedType::Integer),
                            UnrestrictedType::Integer
                        );
                        let is_third_parameter_string = matches!(
                            self.calculate_expression_type(children[2])?
                                .unwrap_or(UnrestrictedType::String),
                            UnrestrictedType::String,
                        );

                        if !is_first_parameter_string
                            || (!is_second_parameter_number && !is_second_parameter_string)
                            || (!is_third_parameter_number && !is_third_parameter_string)
                        {
                            return Err(SbroadError::Invalid(
                                Entity::Expression,
                                Some(
                                    "explicit types are required for parameters of substring."
                                        .into(),
                                ),
                            ));
                        }

                        if (is_second_parameter_number && is_third_parameter_string)
                            || (is_second_parameter_string && is_third_parameter_number)
                        {
                            return Err(SbroadError::Invalid(
                                    Entity::Expression,
                                    Some("incorrect SUBSTRING parameters type. Second and third parameters should have the same type".into()),

                                ));
                        }

                        // if parameters with numbers
                        if name == "substr" {
                            // Check if length is negative
                            if is_negative_number(&self, children[2])? {
                                return Err(SbroadError::Invalid(
                                    Entity::Expression,
                                    Some(
                                        "Length parameter in substring cannot be negative.".into(),
                                    ),
                                ));
                            }
                        }

                        if name == "substring" {
                            let new_name = SmolStr::from("substring_to_regexp");
                            new_names.push((node_id, new_name));
                        }
                    }
                    Substring::For => {
                        let is_third_parameter_number = matches!(
                            self.calculate_expression_type(children[2])?
                                .unwrap_or(UnrestrictedType::Integer),
                            UnrestrictedType::Integer
                        );

                        if !is_first_parameter_string || !is_third_parameter_number {
                            return Err(SbroadError::Invalid(
                                        Entity::Expression,
                                        Some("explicit types are required. Expected a string, and a numeric length.".into()),
                                    ));
                        }

                        // Check if length is negative
                        if is_negative_number(&self, children[2])? {
                            return Err(SbroadError::Invalid(
                                Entity::Expression,
                                Some("Length parameter in substring cannot be negative.".into()),
                            ));
                        }
                    }
                    Substring::From => {
                        if !is_first_parameter_string
                            || (!is_second_parameter_number && !is_second_parameter_string)
                        {
                            return Err(SbroadError::Invalid(
                                Entity::Expression,
                                Some(
                                    "explicit types are required for parameters of substring."
                                        .into(),
                                ),
                            ));
                        }
                    }
                    Substring::Similar => {
                        let is_third_parameter_string = matches!(
                            self.calculate_expression_type(children[2])?
                                .unwrap_or(UnrestrictedType::String),
                            UnrestrictedType::String,
                        );

                        if !is_first_parameter_string
                            || !is_second_parameter_string
                            || !is_third_parameter_string
                        {
                            return Err(SbroadError::Invalid(
                                Entity::Expression,
                                Some(r#"explicit types are required. Expected three string arguments."#.into()),
                            ));
                        }

                        if name == "substring" {
                            let new_name = SmolStr::from("substring_to_regexp");
                            new_names.push((node_id, new_name));
                        }
                    }
                }
            }
        }

        for (node_id, new_name) in new_names {
            if let MutNode::Expression(MutExpression::ScalarFunction(ScalarFunction {
                name, ..
            })) = self.get_mut_node(node_id)?
            {
                *name = new_name;
            }
        }
        Ok(self)
    }

    fn create_datetime_value(
        local_datetime: OffsetDateTime,
        spec: &Timestamp,
    ) -> Result<Value, SbroadError> {
        match *spec {
            // for the lack of a better type, return datetime with the time component set to midnight
            Timestamp::Date => Ok(Value::Datetime(Datetime::from_inner(
                local_datetime.replace_time(Time::MIDNIGHT),
            ))),
            Timestamp::DateTime(TimeParameters {
                precision,
                // currently picodata lacks a timestamp type that doesn't include a timezone, so this is ignored
                // https://git.picodata.io/core/picodata/-/issues/1797
                include_timezone: _,
            }) => {
                let time = local_datetime.time();

                let time = if precision == 0 {
                    // truncate the nanoseconds
                    Time::from_hms(time.hour(), time.minute(), time.second()).unwrap()
                } else {
                    // Calculate scaling
                    // Convert nanoseconds to the desired precision
                    // 9 - numbers in nanoseconds
                    let scale = 10_u64.pow((9 - precision) as u32) as i64;
                    let nanos = time.nanosecond() as i64;
                    let rounded_nanos = (nanos / scale) * scale;

                    Time::from_hms_nano(
                        time.hour(),
                        time.minute(),
                        time.second(),
                        rounded_nanos as u32,
                    )
                    .unwrap()
                };

                Ok(Value::Datetime(Datetime::from_inner(
                    local_datetime.replace_time(time),
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ir::node::{TimeParameters, Timestamp};
    use crate::ir::value::Value;
    use time::macros::datetime;

    #[test]
    fn datetime_rounding() {
        let original_ts = datetime!(2024-04-03 15:26:14.998133 -3);

        let make_datetime = |precision| {
            super::Plan::create_datetime_value(
                original_ts,
                &Timestamp::DateTime(TimeParameters {
                    precision,
                    include_timezone: true,
                }),
            )
            .unwrap()
        };

        assert_eq!(
            make_datetime(6),
            Value::Datetime(datetime!(2024-04-03 15:26:14.998133 -3).into())
        );
        assert_eq!(
            make_datetime(5),
            Value::Datetime(datetime!(2024-04-03 15:26:14.99813 -3).into())
        );
        assert_eq!(
            make_datetime(4),
            Value::Datetime(datetime!(2024-04-03 15:26:14.9981 -3).into())
        );
        assert_eq!(
            make_datetime(3),
            Value::Datetime(datetime!(2024-04-03 15:26:14.998 -3).into())
        );
        assert_eq!(
            make_datetime(2),
            Value::Datetime(datetime!(2024-04-03 15:26:14.99 -3).into())
        );
        assert_eq!(
            make_datetime(1),
            Value::Datetime(datetime!(2024-04-03 15:26:14.9 -3).into())
        );
        assert_eq!(
            make_datetime(0),
            Value::Datetime(datetime!(2024-04-03 15:26:14 -3).into())
        );
    }
}
