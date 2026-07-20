use smol_str::format_smolstr;
use sql_type_system::error::Error as TypeSystemError;

use crate::errors::{Entity, SbroadError};
use crate::ir::metadata::Metadata;
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::{MutRelational, Relational};
use crate::ir::node::{
    Alias, Constant, GroupBy, Node, Node32, NodeId, Parameter, Reference, ReferenceTarget,
    ScalarFunction,
};
use crate::ir::tree::traversal::{PostOrder, PostOrderWithFilter, EXPR_CAPACITY, REL_CAPACITY};
use crate::ir::types::DerivedType;
use crate::ir::value::Value;
use crate::ir::Plan;

use super::ExpressionWalker;

pub(in crate::frontend::sql) trait PlanSubqueryRowsExt {
    /// Helper function to populate plan with `SubQuery` represented as a Row of its output.
    fn add_replaced_subquery<M: Metadata>(
        &mut self,
        sq_id: NodeId,
        worker: &mut ExpressionWalker<M>,
    ) -> Result<NodeId, SbroadError>;

    /// Fix subqueries (already represented as rows) for newly created relational node
    fn fix_subquery_rows<M: Metadata>(
        &mut self,
        worker: &mut ExpressionWalker<M>,
        rel_id: NodeId,
    ) -> Result<(), SbroadError>;
}

impl PlanSubqueryRowsExt for Plan {
    /// Helper function to populate plan with `SubQuery` represented as a Row of its output.
    fn add_replaced_subquery<M: Metadata>(
        &mut self,
        sq_id: NodeId,
        worker: &mut ExpressionWalker<M>,
    ) -> Result<NodeId, SbroadError> {
        if worker.inside_window_body {
            worker.curr_window_sqs.push(sq_id);
        }
        let sq_row_id = self.add_row_from_subquery(sq_id)?;
        worker.subquery_replaces.insert(sq_id, sq_row_id);
        worker.sub_queries_to_fix_queue.push_back(sq_id);
        Ok(sq_row_id)
    }

    /// Fix subqueries (already represented as rows) for newly created relational node
    fn fix_subquery_rows<M: Metadata>(
        &mut self,
        worker: &mut ExpressionWalker<M>,
        rel_id: NodeId,
    ) -> Result<(), SbroadError> {
        let mut rel_node = self.get_mut_relation_node(rel_id)?;

        while let Some(sq_id) = worker.sub_queries_to_fix_queue.pop_front() {
            let window_id = worker.named_windows_sqs.get(&sq_id);
            if window_id.is_none() || worker.curr_windows.contains(window_id.unwrap()) {
                rel_node.add_sq_child(sq_id);
            }
        }
        Ok(())
    }
}

pub(in crate::frontend::sql) trait PlanGroupByOrdinalsExt {
    fn replace_group_by_ordinals_with_references(&mut self) -> Result<(), SbroadError>;

    fn adjust_grouping_exprs(&mut self, final_proj_id: NodeId) -> Result<(), SbroadError>;

    fn check_grouping_expr_subtree(&self, group_expr: NodeId) -> Result<(), SbroadError>;

    fn replace_const_with_reference(
        &mut self,
        final_proj_cols: &[NodeId],
        groupby_id: NodeId,
        expr: &mut NodeId,
        pos: usize,
    ) -> Result<(), SbroadError>;
}

impl PlanGroupByOrdinalsExt for Plan {
    fn replace_group_by_ordinals_with_references(&mut self) -> Result<(), SbroadError> {
        let top = self.get_top()?;
        if !matches!(self.get_node(top)?, Node::Relational(_)) {
            return Ok(());
        }
        let post_tree = PostOrder::new(|node| self.nodes.rel_iter(node), REL_CAPACITY);
        let nodes = post_tree.traverse_into_vec(top);
        for id in nodes {
            if matches!(self.get_relation_node(id)?, Relational::Projection(_)) {
                self.adjust_grouping_exprs(id)?;
            }
        }
        Ok(())
    }

    fn adjust_grouping_exprs(&mut self, final_proj_id: NodeId) -> Result<(), SbroadError> {
        let group_by = self.get_group_by(final_proj_id)?;
        let Some(group_by) = group_by else {
            return Ok(());
        };
        let final_proj_output = self.get_relational_output(final_proj_id)?;
        let final_proj_cols = self.get_row_list(final_proj_output)?.clone();
        let mut grouping_exprs = match self.get_relation_node(group_by)? {
            Relational::GroupBy(GroupBy { gr_exprs, .. }) => gr_exprs.clone(),
            _ => unreachable!("expected GroupBy node"),
        };

        for expr in grouping_exprs.iter_mut() {
            match self.get_expression_node(*expr)? {
                Expression::Constant(Constant { value, .. }) => {
                    let pos = match value {
                        Value::Integer(val) => {
                            if *val < 0 {
                                return Err(SbroadError::Invalid(
                                    Entity::Query,
                                    Some(format_smolstr!(
                                        "GROUP BY position {value} should be positive"
                                    )),
                                ));
                            }
                            *val as usize
                        }
                        _ => continue,
                    };

                    self.replace_const_with_reference(&final_proj_cols, group_by, expr, pos)?;
                }
                _ => {
                    self.check_grouping_expr_subtree(*expr)?;
                }
            }
        }

        if let MutRelational::GroupBy(GroupBy { gr_exprs, .. }) =
            self.get_mut_relation_node(group_by)?
        {
            *gr_exprs = grouping_exprs;
        }

        Ok(())
    }

    fn check_grouping_expr_subtree(&self, group_expr: NodeId) -> Result<(), SbroadError> {
        let dfs = PostOrderWithFilter::new(
            |node| self.nodes.expr_iter(node, false),
            |node| {
                matches!(
                    self.get_node(node),
                    Ok(Node::Expression(Expression::ScalarFunction(_)))
                )
            },
            EXPR_CAPACITY,
        );

        for node_id in dfs.traverse_into_iter(group_expr) {
            let node = self.get_expression_node(node_id)?;
            if let Expression::ScalarFunction(ScalarFunction {
                name, is_window, ..
            }) = node
            {
                if Expression::is_aggregate_name(name) {
                    let kind = match is_window {
                        true => "window",
                        false => "aggregate",
                    };
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format_smolstr!(
                            "{kind} function \"{name}\" is not allowed in GROUP BY"
                        )),
                    ));
                }
            }
        }

        Ok(())
    }

    fn replace_const_with_reference(
        &mut self,
        final_proj_cols: &[NodeId],
        groupby_id: NodeId,
        expr: &mut NodeId,
        pos: usize,
    ) -> Result<(), SbroadError> {
        let idx = if let Some(idx) = pos.checked_sub(1) {
            idx
        } else {
            return Err(SbroadError::Invalid(
                Entity::Query,
                Some(format_smolstr!("GROUP BY position 0 is not in select list")),
            ));
        };

        let alias_id = if let Some(id) = final_proj_cols.get(idx) {
            *id
        } else {
            return Err(SbroadError::Invalid(
                Entity::Query,
                Some(format_smolstr!(
                    "GROUP BY position {pos} is not in select list"
                )),
            ));
        };
        let alias_node = self.get_expression_node(alias_id)?;
        if let Expression::Alias(Alias { child, .. }) = alias_node {
            let child = *child;
            let expr_node = self.get_expression_node(child)?;
            if let Expression::Reference(Reference {
                position, col_type, ..
            }) = expr_node
            {
                let node_id = self.get_first_rel_child(groupby_id)?;
                // Reuse the type from the existing projection-output reference. This
                // rewrite creates a new reference node, but its `col_type` is already
                // known here.
                let ref_id = self.nodes.add_ref(
                    ReferenceTarget::Single(node_id),
                    *position,
                    *col_type,
                    None,
                    false,
                );

                *expr = ref_id;
            }

            self.check_grouping_expr_subtree(child)?;
        }

        Ok(())
    }
}

pub(in crate::frontend::sql) trait PlanParameterTypesExt {
    /// Set inferred parameter types in all parameters nodes.
    fn set_types_in_parameter_nodes(&mut self, params: &[DerivedType]) -> Result<(), SbroadError>;
}

impl PlanParameterTypesExt for Plan {
    /// Set inferred parameter types in all parameters nodes.
    fn set_types_in_parameter_nodes(&mut self, params: &[DerivedType]) -> Result<(), SbroadError> {
        for node in self.nodes.iter32_mut() {
            if let Node32::Parameter(Parameter {
                ref mut param_type,
                ref index,
                ..
            }) = node
            {
                let could_not_determine_parameter_type =
                    || TypeSystemError::CouldNotDetermineParameterType(*index - 1);

                let index = (*index - 1) as usize;
                let derived = params
                    .get(index)
                    .ok_or_else(could_not_determine_parameter_type)?;

                if derived.get().is_none() {
                    return Err(could_not_determine_parameter_type().into());
                }

                *param_type = *derived;
            }
        }

        Ok(())
    }
}
