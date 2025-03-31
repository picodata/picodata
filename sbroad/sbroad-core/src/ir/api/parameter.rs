use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::is_negative_number;
use crate::ir::expression::{FunctionFeature, Substring};
use crate::ir::node::block::{Block, MutBlock};
use crate::ir::node::expression::{Expression, MutExpression};
use crate::ir::node::relational::{MutRelational, Relational};
use crate::ir::node::{
    Alias, ArithmeticExpr, BoolExpr, Bound, BoundType, Case, Cast, Concat, Constant, GroupBy,
    Having, Join, Like, LocalTimestamp, MutNode, Node64, Node96, NodeId, Over, Parameter,
    Procedure, Reference, Row, Selection, StableFunction, Trim, UnaryExpr, ValuesRow, Window,
};
use crate::ir::operator::OrderByEntity;
use crate::ir::relation::{DerivedType, Type};
use crate::ir::tree::traversal::{LevelNode, PostOrder, PostOrderWithFilter, EXPR_CAPACITY};
use crate::ir::value::Value;
use crate::ir::{ArenaType, Node, OptionParamValue, Plan};
use chrono::Local;
use smol_str::{format_smolstr, SmolStr};
use tarantool::datetime::Datetime;
use time::{OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

use ahash::{AHashMap, AHashSet, RandomState};
use std::collections::HashMap;

struct ParamsBinder<'binder> {
    plan: &'binder mut Plan,
    /// Plan nodes to traverse during binding.
    nodes: Vec<LevelNode<NodeId>>,
    /// Number of parameters met in the OPTIONs.
    binded_options_counter: usize,
    /// Flag indicating whether we use Tarantool parameters notation.
    tnt_params_style: bool,
    /// Map of { plan param_id -> corresponding value }.
    pg_params_map: AHashMap<NodeId, usize>,
    /// Plan nodes that correspond to Parameters.
    param_node_ids: AHashSet<NodeId>,
    /// Params transformed into constant Values.
    value_ids: Vec<NodeId>,
    /// Values that should be bind.
    values: Vec<Value>,
    /// We need to use rows instead of values in some cases (AST can solve
    /// this problem for non-parameterized queries, but for parameterized
    /// queries it is IR responsibility).
    ///
    /// Map of { param_id -> corresponding row }.
    row_map: AHashMap<NodeId, NodeId, RandomState>,
}

fn get_param_value(
    tnt_params_style: bool,
    param_id: NodeId,
    param_index: usize,
    value_ids: &[NodeId],
    pg_params_map: &AHashMap<NodeId, usize>,
) -> NodeId {
    let value_index = if tnt_params_style {
        // In case non-pg params are used, index is the correct position
        param_index
    } else {
        value_ids.len()
            - 1
            - *pg_params_map.get(&param_id).unwrap_or_else(|| {
                panic!("Value index not found for parameter with id: {param_id:?}.")
            })
    };
    let val_id = value_ids
        .get(value_index)
        .unwrap_or_else(|| panic!("Parameter not found in position {value_index}."));
    *val_id
}

impl<'binder> ParamsBinder<'binder> {
    fn new(plan: &'binder mut Plan, mut values: Vec<Value>) -> Result<Self, SbroadError> {
        let capacity = plan.nodes.len();

        // Note: `need_output` is set to false for `subtree_iter` specially to avoid traversing
        //       the same nodes twice. See `update_values_row` for more info.
        let mut tree = PostOrder::with_capacity(|node| plan.subtree_iter(node, false), capacity);
        let top_id = plan.get_top()?;
        tree.populate_nodes(top_id);
        let nodes = tree.take_nodes();

        let pg_params_map = plan.build_pg_param_map();
        let tnt_params_style = pg_params_map.is_empty();

        let mut binded_options_counter = 0;
        if !plan.raw_options.is_empty() {
            binded_options_counter = plan.bind_option_params(&mut values, &pg_params_map);
        }

        let param_node_ids = plan.get_param_set();

        let binder = ParamsBinder {
            plan,
            nodes,
            binded_options_counter,
            tnt_params_style,
            pg_params_map,
            param_node_ids,
            value_ids: Vec::new(),
            values,
            row_map: AHashMap::new(),
        };
        Ok(binder)
    }

    /// Copy values to bind for Postgres-style parameters.
    fn handle_pg_parameters(&mut self) -> Result<(), SbroadError> {
        if !self.tnt_params_style {
            // Due to how we calculate hash for plan subtree and the
            // fact that pg parameters can refer to same value multiple
            // times we currently copy params that are referred more
            // than once in order to get the same hash.
            // See https://git.picodata.io/picodata/picodata/sbroad/-/issues/583
            let mut used_values = vec![false; self.values.len()];
            let initial_len = self.values.len();
            let invalid_idx = |value_idx: usize| {
                Err(SbroadError::Invalid(
                    Entity::Query,
                    Some(
                        format!(
                            "Parameter binding error: Index {} out of bounds. Valid range: 1..{}.",
                            value_idx + 1,
                            initial_len,
                        )
                        .into(),
                    ),
                ))
            };
            // NB: we can't use `param_node_ids`, we need to traverse
            // parameters in the same order they will be bound,
            // otherwise we may get different hashes for plans
            // with tnt and pg parameters. See `subtree_hash*` tests,
            for LevelNode(_, param_id) in &self.nodes {
                if !matches!(
                    self.plan.get_node(*param_id)?,
                    Node::Expression(Expression::Parameter(..))
                ) {
                    continue;
                }
                let value_idx = *self.pg_params_map.get(param_id).unwrap_or_else(|| {
                    panic!("Value index not found for parameter with id: {param_id:?}.");
                });
                if used_values.get(value_idx).copied().unwrap_or(true) {
                    let Some(value) = self.values.get(value_idx) else {
                        invalid_idx(value_idx - (self.values.len() - initial_len))?
                    };
                    self.values.push(value.clone());
                    self.pg_params_map
                        .values_mut()
                        .filter(|value| **value >= self.values.len() - 1)
                        .for_each(|value| *value += 1);
                    self.pg_params_map
                        .entry(*param_id)
                        .and_modify(|value_idx| *value_idx = self.values.len() - 1);
                } else if let Some(used) = used_values.get_mut(value_idx) {
                    *used = true;
                } else {
                    invalid_idx(value_idx)?
                }
            }
        }
        Ok(())
    }

    /// Transform parameters (passed by user) to values (plan constants).
    /// The result values are stored in the opposite to parameters order.
    ///
    /// In case some redundant params were passed, they'll
    /// be ignored (just not popped from the `value_ids` stack later).
    fn create_parameter_constants(&mut self) {
        self.value_ids = Vec::with_capacity(self.values.len());
        while let Some(param) = self.values.pop() {
            self.value_ids.push(self.plan.add_const(param));
        }
    }

    /// Check that number of user passed params equal to the params nodes we have to bind.
    fn check_params_count(&self) -> Result<(), SbroadError> {
        let non_binded_params_len = self.param_node_ids.len() - self.binded_options_counter;
        if self.tnt_params_style && non_binded_params_len > self.value_ids.len() {
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "Expected at least {} values for parameters. Got {}.",
                non_binded_params_len,
                self.value_ids.len()
            )));
        }
        Ok(())
    }

    /// Retrieve a corresponding value (plan constant node) for a parameter node.
    fn get_param_value(&self, param_id: NodeId, param_index: usize) -> NodeId {
        get_param_value(
            self.tnt_params_style,
            param_id,
            param_index,
            &self.value_ids,
            &self.pg_params_map,
        )
    }

    /// 1.) Increase binding param index.
    /// 2.) In case `cover_with_row` is set to true, cover the param node with a row.
    fn cover_param_with_row(
        &self,
        param_id: NodeId,
        cover_with_row: bool,
        param_index: &mut usize,
        row_ids: &mut HashMap<NodeId, NodeId, RandomState>,
    ) {
        if self.param_node_ids.contains(&param_id) {
            if row_ids.contains_key(&param_id) {
                return;
            }
            *param_index = param_index.saturating_sub(1);
            if cover_with_row {
                let val_id = self.get_param_value(param_id, *param_index);
                row_ids.insert(param_id, val_id);
            }
        }
    }

    /// Traverse the plan nodes tree and cover parameter nodes with rows if needed.
    #[allow(clippy::too_many_lines)]
    fn cover_params_with_rows(&mut self) -> Result<(), SbroadError> {
        // Len of `value_ids` - `param_index` = param index we are currently binding.
        let mut param_index = self.value_ids.len();
        let mut row_ids = HashMap::with_hasher(RandomState::new());

        for LevelNode(_, id) in &self.nodes {
            let node = self.plan.get_node(*id)?;
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
                        self.cover_param_with_row(*param_id, true, &mut param_index, &mut row_ids);
                    }
                    _ => {}
                },
                Node::Expression(expr) => match expr {
                    Expression::Window(Window {
                        partition,
                        ordering,
                        frame,
                        ..
                    }) => {
                        if let Some(param_id) = partition {
                            for param_id in param_id {
                                self.cover_param_with_row(
                                    *param_id,
                                    false,
                                    &mut param_index,
                                    &mut row_ids,
                                );
                            }
                        }
                        if let Some(ordering) = ordering {
                            for o_elem in ordering {
                                if let OrderByEntity::Expression { expr_id } = o_elem.entity {
                                    self.cover_param_with_row(
                                        expr_id,
                                        false,
                                        &mut param_index,
                                        &mut row_ids,
                                    );
                                }
                            }
                        }
                        if let Some(frame) = frame {
                            let mut bound_types = [None, None];
                            match &frame.bound {
                                Bound::Single(start) => {
                                    bound_types[0] = Some(start);
                                }
                                Bound::Between(start, end) => {
                                    bound_types[0] = Some(start);
                                    bound_types[1] = Some(end);
                                }
                            }
                            for b_type in bound_types.iter().flatten() {
                                match b_type {
                                    BoundType::PrecedingOffset(id)
                                    | BoundType::FollowingOffset(id) => {
                                        self.cover_param_with_row(
                                            *id,
                                            false,
                                            &mut param_index,
                                            &mut row_ids,
                                        );
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    Expression::Over(Over {
                        func_args, filter, ..
                    }) => {
                        for param_id in func_args {
                            self.cover_param_with_row(
                                *param_id,
                                false,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                        if let Some(param_id) = filter {
                            self.cover_param_with_row(
                                *param_id,
                                false,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                    }
                    Expression::Alias(Alias {
                        child: ref param_id,
                        ..
                    })
                    | Expression::Cast(Cast {
                        child: ref param_id,
                        ..
                    })
                    | Expression::Unary(UnaryExpr {
                        child: ref param_id,
                        ..
                    }) => {
                        self.cover_param_with_row(*param_id, false, &mut param_index, &mut row_ids);
                    }
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
                            self.cover_param_with_row(
                                *param_id,
                                true,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                    }
                    Expression::Like(Like {
                        escape,
                        left,
                        right,
                    }) => {
                        for param_id in &[*left, *right] {
                            self.cover_param_with_row(
                                *param_id,
                                true,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                        self.cover_param_with_row(*escape, true, &mut param_index, &mut row_ids);
                    }
                    Expression::Trim(Trim {
                        ref pattern,
                        ref target,
                        ..
                    }) => {
                        let params = match pattern {
                            Some(p) => [Some(*p), Some(*target)],
                            None => [None, Some(*target)],
                        };
                        for param_id in params.into_iter().flatten() {
                            self.cover_param_with_row(
                                param_id,
                                true,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                    }
                    Expression::Row(Row { ref list, .. })
                    | Expression::StableFunction(StableFunction {
                        children: ref list, ..
                    }) => {
                        for param_id in list {
                            // Parameter is already under row/function so that we don't
                            // have to cover it with `add_row` call.
                            self.cover_param_with_row(
                                *param_id,
                                false,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                    }
                    Expression::Case(Case {
                        ref search_expr,
                        ref when_blocks,
                        ref else_expr,
                    }) => {
                        if let Some(search_expr) = search_expr {
                            self.cover_param_with_row(
                                *search_expr,
                                false,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                        for (cond_expr, res_expr) in when_blocks {
                            self.cover_param_with_row(
                                *cond_expr,
                                false,
                                &mut param_index,
                                &mut row_ids,
                            );
                            self.cover_param_with_row(
                                *res_expr,
                                false,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                        if let Some(else_expr) = else_expr {
                            self.cover_param_with_row(
                                *else_expr,
                                false,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                    }
                    Expression::Reference { .. }
                    | Expression::Constant { .. }
                    | Expression::CountAsterisk { .. }
                    | Expression::LocalTimestamp { .. }
                    | Expression::Parameter { .. } => {}
                },
                Node::Block(block) => match block {
                    Block::Procedure(Procedure { ref values, .. }) => {
                        for param_id in values {
                            // We don't need to wrap arguments, passed into the
                            // procedure call, into the rows.
                            self.cover_param_with_row(
                                *param_id,
                                false,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                    }
                },
                Node::Invalid(..)
                | Node::Ddl(..)
                | Node::Acl(..)
                | Node::Tcl(..)
                | Node::Plugin(_)
                | Node::Deallocate(..) => {}
            }
        }

        let fixed_row_ids: AHashMap<NodeId, NodeId, RandomState> = row_ids
            .iter()
            .map(|(param_id, val_id)| {
                let row_cover = self.plan.nodes.add_row(vec![*val_id], None);
                (*param_id, row_cover)
            })
            .collect();
        self.row_map = fixed_row_ids;

        Ok(())
    }

    /// Replace parameters in the plan.
    #[allow(clippy::too_many_lines)]
    fn bind_params(&mut self) -> Result<(), SbroadError> {
        // Len of `value_ids` - `param_index` = param index we are currently binding.
        let mut param_index = self.value_ids.len();

        let tnt_params_style = self.tnt_params_style;
        let row_ids = std::mem::take(&mut self.row_map);
        let value_ids = std::mem::take(&mut self.value_ids);
        let pg_params_map = std::mem::take(&mut self.pg_params_map);

        let bind_param = |param_id: &mut NodeId, is_row: bool, param_index: &mut usize| {
            *param_id = if self.param_node_ids.contains(param_id) {
                *param_index = param_index.saturating_sub(1);
                let binding_node_id = if is_row {
                    *row_ids
                        .get(param_id)
                        .unwrap_or_else(|| panic!("Row not found at position {param_id}"))
                } else {
                    get_param_value(
                        tnt_params_style,
                        *param_id,
                        *param_index,
                        &value_ids,
                        &pg_params_map,
                    )
                };
                binding_node_id
            } else {
                *param_id
            }
        };

        for LevelNode(_, id) in &self.nodes {
            let node = self.plan.get_mut_node(*id)?;
            match node {
                MutNode::Relational(rel) => match rel {
                    MutRelational::Having(Having {
                        filter: ref mut param_id,
                        ..
                    })
                    | MutRelational::Selection(Selection {
                        filter: ref mut param_id,
                        ..
                    })
                    | MutRelational::Join(Join {
                        condition: ref mut param_id,
                        ..
                    }) => {
                        bind_param(param_id, true, &mut param_index);
                    }
                    MutRelational::GroupBy(GroupBy { gr_exprs, .. }) => {
                        for expr_id in gr_exprs {
                            bind_param(expr_id, false, &mut param_index);
                        }
                    }
                    _ => {}
                },
                MutNode::Expression(expr) => match expr {
                    MutExpression::Alias(Alias {
                        child: ref mut param_id,
                        ..
                    })
                    | MutExpression::Cast(Cast {
                        child: ref mut param_id,
                        ..
                    })
                    | MutExpression::Unary(UnaryExpr {
                        child: ref mut param_id,
                        ..
                    }) => {
                        bind_param(param_id, false, &mut param_index);
                    }
                    MutExpression::Bool(BoolExpr {
                        ref mut left,
                        ref mut right,
                        ..
                    })
                    | MutExpression::Arithmetic(ArithmeticExpr {
                        ref mut left,
                        ref mut right,
                        ..
                    })
                    | MutExpression::Concat(Concat {
                        ref mut left,
                        ref mut right,
                    }) => {
                        for param_id in [left, right] {
                            bind_param(param_id, true, &mut param_index);
                        }
                    }
                    MutExpression::Like(Like {
                        ref mut escape,
                        ref mut left,
                        ref mut right,
                    }) => {
                        bind_param(escape, true, &mut param_index);
                        for param_id in [left, right] {
                            bind_param(param_id, true, &mut param_index);
                        }
                    }
                    MutExpression::Trim(Trim {
                        ref mut pattern,
                        ref mut target,
                        ..
                    }) => {
                        let params = match pattern {
                            Some(p) => [Some(p), Some(target)],
                            None => [None, Some(target)],
                        };
                        for param_id in params.into_iter().flatten() {
                            bind_param(param_id, true, &mut param_index);
                        }
                    }
                    MutExpression::Row(Row { ref mut list, .. })
                    | MutExpression::StableFunction(StableFunction {
                        children: ref mut list,
                        ..
                    }) => {
                        for param_id in list {
                            bind_param(param_id, false, &mut param_index);
                        }
                    }

                    MutExpression::Window(Window {
                        partition,
                        ordering,
                        frame,
                        ..
                    }) => {
                        if let Some(frame) = frame {
                            match frame.bound {
                                Bound::Single(
                                    BoundType::PrecedingOffset(mut id)
                                    | BoundType::FollowingOffset(mut id),
                                ) => {
                                    bind_param(&mut id, false, &mut param_index);
                                }
                                Bound::Between(ref mut start, ref mut end) => {
                                    for b_type in [start, end].iter_mut() {
                                        match b_type {
                                            BoundType::PrecedingOffset(ref mut id)
                                            | BoundType::FollowingOffset(ref mut id) => {
                                                bind_param(id, false, &mut param_index);
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        if let Some(ordering) = ordering {
                            for o_elem in ordering {
                                if let OrderByEntity::Expression { mut expr_id } = o_elem.entity {
                                    bind_param(&mut expr_id, false, &mut param_index);
                                }
                            }
                        }
                        if let Some(param_id) = partition {
                            for param_id in param_id {
                                bind_param(param_id, false, &mut param_index);
                            }
                        }
                    }
                    MutExpression::Over(Over {
                        func_args, filter, ..
                    }) => {
                        // TODO: we need to refactor Over and Window nodes to make them
                        // more traversal friendly.
                        if let Some(param_id) = filter {
                            if self.param_node_ids.contains(param_id) {
                                return Err(SbroadError::Invalid(
                                    Entity::Query,
                                    Some(format_smolstr!(
                                        "Parameter binding error: windo filter parameter is not allowed."
                                    )),
                                ));
                            }
                        }
                        for param_id in func_args {
                            if self.param_node_ids.contains(param_id) {
                                return Err(SbroadError::Invalid(
                                    Entity::Query,
                                    Some(format_smolstr!(
                                        "Parameter binding error: window function argument parameters are not allowed."
                                    )),
                                ));
                            }
                        }
                    }
                    MutExpression::Case(Case {
                        ref mut search_expr,
                        ref mut when_blocks,
                        ref mut else_expr,
                    }) => {
                        if let Some(param_id) = search_expr {
                            bind_param(param_id, false, &mut param_index);
                        }
                        for (param_id_1, param_id_2) in when_blocks {
                            bind_param(param_id_1, false, &mut param_index);
                            bind_param(param_id_2, false, &mut param_index);
                        }
                        if let Some(param_id) = else_expr {
                            bind_param(param_id, false, &mut param_index);
                        }
                    }
                    MutExpression::Reference { .. }
                    | MutExpression::Constant { .. }
                    | MutExpression::CountAsterisk { .. }
                    | MutExpression::Parameter { .. }
                    | MutExpression::LocalTimestamp { .. } => {}
                },
                MutNode::Block(block) => match block {
                    MutBlock::Procedure(Procedure { ref mut values, .. }) => {
                        for param_id in values {
                            bind_param(param_id, false, &mut param_index);
                        }
                    }
                },
                MutNode::Invalid(..)
                | MutNode::Ddl(..)
                | MutNode::Tcl(..)
                | MutNode::Plugin(_)
                | MutNode::Acl(..)
                | MutNode::Deallocate(..) => {}
            }
        }

        Ok(())
    }

    fn update_value_rows(&mut self) -> Result<(), SbroadError> {
        for LevelNode(_, id) in &self.nodes {
            if let Ok(Node::Relational(Relational::ValuesRow(_))) = self.plan.get_node(*id) {
                self.plan.update_values_row(*id)?;
            }
        }
        Ok(())
    }

    fn recalculate_ref_types(&mut self) -> Result<(), SbroadError> {
        let ref_nodes = {
            let filter = |node_id| {
                matches!(
                    self.plan.get_node(node_id),
                    Ok(Node::Expression(Expression::Reference(_)))
                )
            };
            let mut tree = PostOrderWithFilter::with_capacity(
                |node| self.plan.parameter_iter(node, true),
                EXPR_CAPACITY,
                Box::new(filter),
            );
            let top_id = self.plan.get_top()?;
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
            ) = self.plan.get_node(*id)?
            {
                Some(expr.recalculate_ref_type(self.plan)?)
            } else {
                None
            };

            if let Some(new_type) = new_type {
                let MutNode::Expression(ref mut expr @ MutExpression::Reference { .. }) =
                    self.plan.get_mut_node(*id)?
                else {
                    panic!("Reference expected to set recalculated type")
                };
                expr.set_ref_type(new_type);
            }
        }
        Ok(())
    }
}

impl Plan {
    pub fn add_param(&mut self, index: Option<usize>) -> NodeId {
        self.nodes.push(
            Parameter {
                index,
                param_type: DerivedType::unknown(),
            }
            .into(),
        )
    }

    /// Bind params related to `Option` clause.
    /// Returns the number of params binded to options.
    pub fn bind_option_params(
        &mut self,
        values: &mut Vec<Value>,
        pg_params_map: &AHashMap<NodeId, usize>,
    ) -> usize {
        // Bind parameters in options to values.
        // Because the Option clause is the last clause in the
        // query the parameters are located in the end of params list.
        let mut binded_params_counter = 0usize;
        for opt in self.raw_options.iter_mut().rev() {
            if let OptionParamValue::Parameter { plan_id: param_id } = opt.val {
                if !pg_params_map.is_empty() {
                    // PG-like params syntax
                    let value_idx = *pg_params_map.get(&param_id).unwrap_or_else(|| {
                        panic!("No value idx in map for option parameter: {opt:?}.");
                    });
                    let value = values.get(value_idx).unwrap_or_else(|| {
                        panic!("Invalid value idx {value_idx}, for option: {opt:?}.");
                    });
                    opt.val = OptionParamValue::Value { val: value.clone() };
                } else if let Some(v) = values.pop() {
                    binded_params_counter += 1;
                    opt.val = OptionParamValue::Value { val: v };
                } else {
                    panic!("No parameter value specified for option: {}", opt.kind);
                }
            }
        }
        binded_params_counter
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

    /// Build a map { pg_parameter_node_id: param_idx }, where param_idx starts with 0.
    #[must_use]
    pub fn build_pg_param_map(&self) -> AHashMap<NodeId, usize> {
        self.nodes
            .arena64
            .iter()
            .enumerate()
            .filter_map(|(id, node)| {
                if let Node64::Parameter(Parameter {
                    index: Some(index), ..
                }) = node
                {
                    Some((
                        NodeId {
                            offset: u32::try_from(id).unwrap(),
                            arena_type: ArenaType::Arena64,
                        },
                        index - 1,
                    ))
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
    pub fn update_values_row(&mut self, id: NodeId) -> Result<(), SbroadError> {
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

    /// Substitute parameters to the plan.
    /// The purpose of this function is to find every `Expression::Parameter` node and replace it
    /// with `Expression::Constant` (under the row).
    #[allow(clippy::too_many_lines)]
    pub fn bind_params(&mut self, values: Vec<Value>) -> Result<(), SbroadError> {
        if values.is_empty() {
            // Check there are no parameters in the plan,
            // we must fail early here, rather than on
            // later pipeline stages.
            let mut param_count: usize = 0;
            let filter = Box::new(|x: NodeId| -> bool {
                if let Ok(Node::Expression(Expression::Parameter(_))) = self.get_node(x) {
                    param_count += 1;
                }
                false
            });
            let mut dfs =
                PostOrderWithFilter::with_capacity(|x| self.subtree_iter(x, false), 0, filter);
            dfs.populate_nodes(self.get_top()?);
            drop(dfs);

            if param_count > 0 {
                return Err(SbroadError::Invalid(
                    Entity::Query,
                    Some(format_smolstr!(
                        "Expected at least {param_count} values for parameters. Got 0"
                    )),
                ));
            }

            // Nothing to do here.
            return Ok(());
        }

        let mut binder = ParamsBinder::new(self, values)?;
        binder.handle_pg_parameters()?;
        binder.create_parameter_constants();
        binder.check_params_count()?;
        binder.cover_params_with_rows()?;
        binder.bind_params()?;
        binder.update_value_rows()?;
        binder.recalculate_ref_types()?;

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
            let Node96::StableFunction(_) = node else {
                continue;
            };
            let node_id = NodeId {
                offset: u32::try_from(id).unwrap(),
                arena_type: ArenaType::Arena96,
            };

            if let Node::Expression(Expression::StableFunction(StableFunction {
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
            if let MutNode::Expression(MutExpression::StableFunction(StableFunction {
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
            let Node96::StableFunction(_) = node else {
                continue;
            };
            let node_id = NodeId {
                offset: u32::try_from(id).unwrap(),
                arena_type: ArenaType::Arena96,
            };

            if let Node::Expression(Expression::StableFunction(StableFunction {
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
            if let MutNode::Expression(MutExpression::StableFunction(StableFunction {
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
