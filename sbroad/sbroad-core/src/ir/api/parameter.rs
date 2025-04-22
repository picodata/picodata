use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::is_negative_number;
use crate::ir::expression::{FunctionFeature, Substring};
use crate::ir::node::expression::{Expression, MutExpression};
use crate::ir::node::relational::Relational;
use crate::ir::node::ArithmeticExpr;
use crate::ir::node::{
    Alias, BoolExpr, Concat, Constant, Having, Join, Like, LocalTimestamp, MutNode, Node64, Node96,
    NodeId, Parameter, Reference, Row, ScalarFunction, Selection, ValuesRow,
};
use crate::ir::relation::{DerivedType, Type};
use crate::ir::tree::traversal::{LevelNode, PostOrder, PostOrderWithFilter, EXPR_CAPACITY};
use crate::ir::value::Value;
use crate::ir::{ArenaType, Node, OptionParamValue, Plan};
use chrono::Local;
use smol_str::{format_smolstr, SmolStr};
use tarantool::datetime::Datetime;
use time::{OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

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

/// Build a map of parameters that should be covered with a row.
#[allow(clippy::too_many_lines)]
fn build_should_cover_with_row_map(
    plan: &Plan,
    nodes: &[LevelNode<NodeId>],
    param_node_ids: &AHashSet<NodeId>,
) -> Result<AHashSet<NodeId>, SbroadError> {
    let mut row_ids = AHashSet::new();
    for LevelNode(_, id) in nodes {
        let node = plan.get_node(*id)?;
        match node {
            // Note: Parameter may not be met at the top of relational operators' expression
            //       trees such as OrderBy and GroupBy, because it won't influence ordering and
            //       grouping correspondingly. These cases are handled during parsing stage.
            Node::Relational(rel) => match rel {
                Relational::Having(Having {
                    filter: ref param_id,
                    ..
                })
                | Relational::Selection(Selection {
                    filter: ref param_id,
                    ..
                })
                | Relational::Join(Join {
                    condition: ref param_id,
                    ..
                }) => {
                    if param_node_ids.contains(param_id) {
                        row_ids.insert(*param_id);
                    }
                }
                _ => {}
            },
            Node::Expression(expr) => match expr {
                Expression::Bool(BoolExpr {
                    ref left,
                    ref right,
                    ..
                })
                | Expression::Arithmetic(ArithmeticExpr {
                    ref left,
                    ref right,
                    ..
                })
                | Expression::Concat(Concat {
                    ref left,
                    ref right,
                }) => {
                    for param_id in &[*left, *right] {
                        if param_node_ids.contains(param_id) {
                            row_ids.insert(*param_id);
                        }
                    }
                }
                Expression::Like(Like {
                    escape,
                    left,
                    right,
                }) => {
                    for param_id in &[*left, *right] {
                        if param_node_ids.contains(param_id) {
                            row_ids.insert(*param_id);
                        }
                    }
                    if param_node_ids.contains(escape) {
                        row_ids.insert(*escape);
                    }
                }
                Expression::Trim(_)
                | Expression::Row(_)
                | Expression::ScalarFunction(_)
                | Expression::Case(_)
                | Expression::Window(_)
                | Expression::Over(_)
                | Expression::Alias(_)
                | Expression::Cast(_)
                | Expression::Unary(_)
                | Expression::Reference(_)
                | Expression::Constant(_)
                | Expression::CountAsterisk(_)
                | Expression::LocalTimestamp(_)
                | Expression::Parameter(_) => {}
            },
            Node::Block(_) => {}
            Node::Invalid(..)
            | Node::Ddl(..)
            | Node::Acl(..)
            | Node::Tcl(..)
            | Node::Plugin(_)
            | Node::Deallocate(..) => {}
        }
    }
    Ok(row_ids)
}

/// Replace parameters in the plan.
fn bind_params(
    plan: &mut Plan,
    param_node_ids: &AHashSet<NodeId>,
    values: &[Value],
    shoud_cover_with_row: &AHashSet<NodeId>,
) -> Result<(), SbroadError> {
    for param_id in param_node_ids {
        let node = plan.get_expression_node(*param_id)?;
        if shoud_cover_with_row.contains(param_id) {
            if let Expression::Parameter(Parameter { index, .. }) = node {
                let value = values[(index - 1) as usize].clone();
                let const_id = plan.add_const(value);
                let list = vec![const_id];
                let distribution = None;
                let row_node = Node64::Row(Row { list, distribution });
                plan.nodes.replace(*param_id, row_node)?;
            }
        } else {
            let node = plan.get_expression_node(*param_id)?;
            if let Expression::Parameter(Parameter { index, .. }) = node {
                let value = values[(index - 1) as usize].clone();
                let constant = Constant { value };
                plan.nodes.replace(*param_id, Node64::Constant(constant))?;
            }
        }
    }

    Ok(())
}

impl Plan {
    pub fn add_param(&mut self, index: u16, param_type: DerivedType) -> NodeId {
        self.nodes.push(Parameter { index, param_type }.into())
    }

    /// Bind params related to `Option` clause.
    fn bind_option_params(&mut self, values: &[Value]) {
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
            .arena64
            .iter()
            .enumerate()
            .filter_map(|(id, node)| {
                if let Node64::Parameter(_) = node {
                    Some(NodeId {
                        offset: u32::try_from(id).unwrap(),
                        arena_type: ArenaType::Arena64,
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
            let new_type = if let Node::Expression(
                ref mut expr @ Expression::Reference(Reference {
                    parent: Some(_), ..
                }),
            ) = self.get_node(*id)?
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
    pub fn bind_params(&mut self, values: Vec<Value>) -> Result<(), SbroadError> {
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
            self.bind_option_params(&values);
        }

        let should_cover_with_row = build_should_cover_with_row_map(self, &nodes, &param_node_ids)?;
        bind_params(self, &param_node_ids, &values, &should_cover_with_row)?;

        self.update_value_rows(&nodes)?;
        self.recalculate_ref_types()?;
        Ok(())
    }

    pub fn update_local_timestamps(&mut self) -> Result<(), SbroadError> {
        let offset_in_sec = Local::now().offset().local_minus_utc();
        let utc_offset_result =
            UtcOffset::from_whole_seconds(offset_in_sec).unwrap_or(UtcOffset::UTC);

        let datetime = OffsetDateTime::now_utc().to_offset(utc_offset_result);
        let local_datetime = PrimitiveDateTime::new(datetime.date(), datetime.time()).assume_utc();

        let mut local_timestamps = Vec::new();

        for (id, node) in self.nodes.arena64.iter().enumerate() {
            if let Node64::LocalTimestamp(_) = node {
                let node_id = NodeId {
                    offset: u32::try_from(id).unwrap(),
                    arena_type: ArenaType::Arena64,
                };

                if let Node::Expression(Expression::LocalTimestamp(LocalTimestamp { precision })) =
                    self.get_node(node_id)?
                {
                    local_timestamps.push((node_id, *precision));
                }
            }
        }

        for (node_id, precision) in local_timestamps {
            let value = Self::create_datetime_value(local_datetime, precision);
            self.nodes
                .replace(node_id, Node64::Constant(Constant { value }))?;
        }

        Ok(())
    }

    pub fn update_substring(&mut self) -> Result<(), SbroadError> {
        self.try_transform_to_substr()?;
        self.check_parameter_types()?;
        Ok(())
    }

    fn try_transform_to_substr(&mut self) -> Result<(), SbroadError> {
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
                            .unwrap_or(Type::Any),
                        Type::Integer | Type::Unsigned
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
                            .unwrap_or(Type::Any),
                        Type::Integer | Type::Unsigned
                    );
                    let is_third_parameter_number = matches!(
                        self.calculate_expression_type(children[2])?
                            .unwrap_or(Type::Any),
                        Type::Integer | Type::Unsigned
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
        Ok(())
    }

    fn check_parameter_types(&mut self) -> Result<(), SbroadError> {
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
                        .unwrap_or(Type::String),
                    Type::String,
                );
                let is_second_parameter_number = matches!(
                    self.calculate_expression_type(children[1])?
                        .unwrap_or(Type::Integer),
                    Type::Integer | Type::Unsigned
                );
                let is_second_parameter_string = matches!(
                    self.calculate_expression_type(children[1])?
                        .unwrap_or(Type::String),
                    Type::String,
                );

                match substr {
                    Substring::FromFor | Substring::Regular => {
                        let is_third_parameter_number = matches!(
                            self.calculate_expression_type(children[2])?
                                .unwrap_or(Type::Integer),
                            Type::Integer | Type::Unsigned
                        );
                        let is_third_parameter_string = matches!(
                            self.calculate_expression_type(children[2])?
                                .unwrap_or(Type::String),
                            Type::String,
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
                            if is_negative_number(self, children[2])? {
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
                                .unwrap_or(Type::Integer),
                            Type::Integer | Type::Unsigned
                        );

                        if !is_first_parameter_string || !is_third_parameter_number {
                            return Err(SbroadError::Invalid(
                                        Entity::Expression,
                                        Some("explicit types are required. Expected a string, and a numeric length.".into()),
                                    ));
                        }

                        // Check if length is negative
                        if is_negative_number(self, children[2])? {
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
                                .unwrap_or(Type::String),
                            Type::String,
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
        Ok(())
    }

    fn create_datetime_value(local_datetime: OffsetDateTime, precision: usize) -> Value {
        // Format according to precision
        if precision == 0 {
            // For precision 0, create new time with 0 nanoseconds
            let truncated_time = Time::from_hms(
                local_datetime.hour(),
                local_datetime.minute(),
                local_datetime.second(),
            )
            .unwrap();
            Value::Datetime(Datetime::from_inner(
                local_datetime.replace_time(truncated_time),
            ))
        } else {
            // Calculate scaling
            // Convert nanoseconds to the desired precision
            // 9 - numbers in nanoseconds
            let scale = 10_u64.pow((9 - precision) as u32) as i64;
            let nanos = local_datetime.nanosecond() as i64;
            let rounded_nanos = (nanos / scale) * scale;

            let adjusted_time = Time::from_hms_nano(
                local_datetime.hour(),
                local_datetime.minute(),
                local_datetime.second(),
                rounded_nanos as u32,
            )
            .unwrap();

            Value::Datetime(Datetime::from_inner(
                local_datetime.replace_time(adjusted_time),
            ))
        }
    }
}
