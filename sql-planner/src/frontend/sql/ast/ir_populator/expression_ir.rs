use smol_str::{format_smolstr, SmolStr};

use crate::errors::{Entity, SbroadError};
use crate::executor::engine::Metadata;
use crate::ir::aggregates::AggregateKind;
use crate::ir::expression::{FunctionFeature, TrimKind, VolatilityType};
use crate::ir::node::expression::{Expression, MutExpression};
use crate::ir::node::{BoolExpr, Node, NodeId, Trim};
use crate::ir::operator::{Arithmetic, Bool, Unary};
use crate::ir::types::CastType;
use crate::ir::value::Value;
use crate::ir::Plan;

use super::ExpressionWalker;

/// Check if `expr` is `A >= B between A <= C`, which is a special variant
/// of `A >= B and A <= C` we got after rewriting a `BETWEEN` expression.
///
/// Panic if the expression is a malformed `BETWEEN` (it's a bug).
#[allow(clippy::type_complexity)]
pub(in crate::frontend::sql) fn try_deconstruct_between_expr<'a>(
    plan: &'a Plan,
    expr: &Expression<'a>,
) -> Option<((NodeId, &'a BoolExpr), (NodeId, &'a BoolExpr))> {
    let Expression::Bool(BoolExpr {
        op: Bool::Between,
        left: lhs_id,
        right: rhs_id,
    }) = expr
    else {
        return None;
    };

    let lhs = match plan
        .get_expression_node(*lhs_id)
        .expect("malformed BETWEEN (missing lhs)")
    {
        Expression::Bool(expr @ BoolExpr { op: Bool::GtEq, .. }) => expr,
        _ => panic!("malformed BETWEEN (invalid lhs)"),
    };

    let rhs = match plan
        .get_expression_node(*rhs_id)
        .expect("malformed BETWEEN (missing rhs)")
    {
        Expression::Bool(expr @ BoolExpr { op: Bool::LtEq, .. }) => expr,
        _ => panic!("malformed BETWEEN (invalid rhs)"),
    };

    Some(((*lhs_id, lhs), (*rhs_id, rhs)))
}

#[derive(Clone, Debug)]
pub(in crate::frontend::sql) enum ParseExpressionInfixOperator {
    InfixBool(Bool),
    InfixArithmetic(Arithmetic),
    Concat,
    Escape,
}

#[derive(Clone, Debug)]
pub(in crate::frontend::sql) enum ParseExpression {
    PlanId {
        plan_id: NodeId,
    },
    SubQueryPlanId {
        plan_id: NodeId,
    },
    Infix {
        is_not: bool,
        op: ParseExpressionInfixOperator,
        left: Box<ParseExpression>,
        right: Box<ParseExpression>,
    },
    Function {
        name: String,
        args: Vec<ParseExpression>,
        feature: Option<FunctionFeature>,
    },
    Like {
        left: Box<ParseExpression>,
        right: Box<ParseExpression>,
        escape: Option<Box<ParseExpression>>,
        is_ilike: bool,
    },
    Similar {
        left: Box<ParseExpression>,
        right: Box<ParseExpression>,
        escape: Option<Box<ParseExpression>>,
    },
    Row {
        children: Vec<ParseExpression>,
    },
    Prefix {
        op: Unary,
        child: Box<ParseExpression>,
    },
    Exists {
        is_not: bool,
        child: Box<ParseExpression>,
    },
    Is {
        is_not: bool,
        child: Box<ParseExpression>,
        value: Option<bool>,
    },
    Index {
        child: Box<ParseExpression>,
        which: Box<ParseExpression>,
    },
    Cast {
        cast_type: CastType,
        child: Box<ParseExpression>,
    },
    Case {
        search_expr: Option<Box<ParseExpression>>,
        when_blocks: Vec<(Box<ParseExpression>, Box<ParseExpression>)>,
        else_expr: Option<Box<ParseExpression>>,
    },
    Trim {
        kind: Option<TrimKind>,
        pattern: Option<Box<ParseExpression>>,
        target: Box<ParseExpression>,
    },
    /// Workaround for the mixfix BETWEEN operator breaking the logic of
    /// pratt parsing for infix operators.
    /// For expression `expr_1 BETWEEN expr_2 AND expr_3` we would create ParseExpression tree of
    ///
    /// And
    ///   - left  = InterimBetween
    ///     - left  = expr_1
    ///     - right = expr_2
    ///   - right = expr_3
    ///
    /// because priority of BETWEEN is higher than of AND.
    ///
    /// When we face such a tree, we transform it into Expression::Between. So during parsing AND
    /// operator we have to check whether we have to transform it into BETWEEN.
    InterimBetween {
        is_not: bool,
        left: Box<ParseExpression>,
        right: Box<ParseExpression>,
    },
    /// Fixed version of `InterimBetween`.
    FinalBetween {
        is_not: bool,
        left: Box<ParseExpression>,
        center: Box<ParseExpression>,
        right: Box<ParseExpression>,
    },
}

impl ParseExpression {
    #[allow(clippy::too_many_lines)]
    pub(in crate::frontend::sql) fn populate_plan<M>(
        &self,
        plan: &mut Plan,
        worker: &mut ExpressionWalker<M>,
    ) -> Result<NodeId, SbroadError>
    where
        M: Metadata,
    {
        let plan_id = match self {
            ParseExpression::PlanId { plan_id } => *plan_id,
            ParseExpression::SubQueryPlanId { plan_id } => {
                plan.add_replaced_subquery(*plan_id, worker)?
            }
            ParseExpression::Index { child, which } => {
                let child_plan_id = child.populate_plan(plan, worker)?;
                let which_plan_id = which.populate_plan(plan, worker)?;
                plan.add_index(child_plan_id, which_plan_id)?
            }
            ParseExpression::Cast { cast_type, child } => {
                let child_plan_id = child.populate_plan(plan, worker)?;
                plan.add_cast(child_plan_id, *cast_type)?
            }
            ParseExpression::Case {
                search_expr,
                when_blocks,
                else_expr,
            } => {
                let search_expr_id = if let Some(search_expr) = search_expr {
                    Some(search_expr.populate_plan(plan, worker)?)
                } else {
                    None
                };
                let when_block_ids = when_blocks
                    .iter()
                    .map(|(cond, res)| {
                        Ok((
                            cond.populate_plan(plan, worker)?,
                            (res.populate_plan(plan, worker)?),
                        ))
                    })
                    .collect::<Result<Vec<(NodeId, NodeId)>, SbroadError>>()?;
                let else_expr_id = if let Some(else_expr) = else_expr {
                    Some(else_expr.populate_plan(plan, worker)?)
                } else {
                    None
                };
                plan.add_case(search_expr_id, when_block_ids, else_expr_id)
            }
            ParseExpression::Trim {
                kind,
                pattern,
                target,
            } => {
                let pattern = match pattern {
                    Some(p) => Some(p.populate_plan(plan, worker)?),
                    None => None,
                };
                let trim_expr = Trim {
                    kind: kind.clone(),
                    pattern,
                    target: target.populate_plan(plan, worker)?,
                };
                plan.nodes.push(trim_expr.into())
            }
            ParseExpression::Like {
                left,
                right,
                escape,
                is_ilike,
            } => {
                let mut plan_left_id = left.populate_plan(plan, worker)?;

                let mut plan_right_id = right.populate_plan(plan, worker)?;

                let plan_escape_id = if let Some(escape) = escape {
                    let plan_escape_id = escape.populate_plan(plan, worker)?;
                    Some(plan_escape_id)
                } else {
                    None
                };
                if *is_ilike {
                    let lower_func = worker.metadata.function("lower")?;
                    plan_left_id =
                        plan.add_stable_function(lower_func, vec![plan_left_id], None)?;
                    plan_right_id =
                        plan.add_stable_function(lower_func, vec![plan_right_id], None)?;
                }
                plan.add_like(plan_left_id, plan_right_id, plan_escape_id)?
            }
            ParseExpression::Similar {
                left,
                right,
                escape,
            } => {
                let plan_left_id = left.populate_plan(plan, worker)?;

                let plan_right_id = right.populate_plan(plan, worker)?;

                let plan_escape_id = if let Some(escape) = escape {
                    let plan_escape_id = escape.populate_plan(plan, worker)?;
                    Some(plan_escape_id)
                } else {
                    None
                };
                plan.add_like(plan_left_id, plan_right_id, plan_escape_id)?
            }
            ParseExpression::FinalBetween {
                is_not,
                left,
                center,
                right,
            } => {
                let plan_left_id = left.populate_plan(plan, worker)?;
                let plan_center_id = center.populate_plan(plan, worker)?;
                let plan_right_id = right.populate_plan(plan, worker)?;

                // XXX: We're going replace `Bool::Between` with `Bool::And` after typecheck!
                // This is required for better type inference, e.g.
                //   SELECT '2026-01-13' BETWEEN '2026-01-01' AND '2026-01-20'::datetime;
                let greater_eq_id = plan.add_cond(plan_left_id, Bool::GtEq, plan_center_id)?;
                let less_eq_id = plan.add_cond(plan_left_id, Bool::LtEq, plan_right_id)?;
                let and_id = plan.add_cond(greater_eq_id, Bool::Between, less_eq_id)?;
                let between_id = if *is_not {
                    plan.add_unary(Unary::Not, and_id)?
                } else {
                    and_id
                };

                worker.betweens.push(and_id);

                between_id
            }
            ParseExpression::Infix {
                op,
                is_not,
                left,
                right,
            } => {
                let left_plan_id = left.populate_plan(plan, worker)?;

                let right_plan_id = match op {
                    ParseExpressionInfixOperator::InfixBool(op) => match op {
                        Bool::In => {
                            if let ParseExpression::SubQueryPlanId { plan_id } = &**right {
                                plan.add_replaced_subquery(*plan_id, worker)?
                            } else {
                                right.populate_plan(plan, worker)?
                            }
                        }
                        Bool::Eq | Bool::Gt | Bool::GtEq | Bool::Lt | Bool::LtEq | Bool::NotEq => {
                            if let ParseExpression::SubQueryPlanId { plan_id } = &**right {
                                plan.add_replaced_subquery(*plan_id, worker)?
                            } else {
                                right.populate_plan(plan, worker)?
                            }
                        }
                        _ => right.populate_plan(plan, worker)?,
                    },
                    _ => right.populate_plan(plan, worker)?,
                };

                // In case:
                // * `op` = AND (left = `left_plan_id`, right = `right_plan_id`)
                // * `right_plan_id` = AND (left = expr_1, right = expr_2) (resulted from BETWEEN
                //   transformation)
                //                And
                //  left_plan_id        And
                //                expr_1    expr_2
                //
                // we'll end up in a situation when AND is a right child of another AND (We don't
                // expect it later as soon as AND is a left-associative operator).
                // We have to fix it the following way:
                //                      And
                //               And        expr_2
                //  left_plan_id   expr_1
                let right_plan_is_and = {
                    let right_expr = plan.get_node(right_plan_id)?;
                    matches!(
                        right_expr,
                        Node::Expression(Expression::Bool(BoolExpr { op: Bool::And, .. }))
                    )
                };
                if matches!(op, ParseExpressionInfixOperator::InfixBool(Bool::And))
                    && right_plan_is_and
                {
                    let right_expr = plan.get_expression_node(right_plan_id)?;
                    let fixed_left_and_id = if let Expression::Bool(BoolExpr {
                        op: Bool::And,
                        left,
                        ..
                    }) = right_expr
                    {
                        plan.add_cond(left_plan_id, Bool::And, *left)?
                    } else {
                        panic!("Expected to see AND operator as right child.");
                    };

                    let right_expr_mut = plan.get_mut_expression_node(right_plan_id)?;
                    if let MutExpression::Bool(BoolExpr {
                        op: Bool::And,
                        left,
                        ..
                    }) = right_expr_mut
                    {
                        *left = fixed_left_and_id;
                        return Ok(right_plan_id);
                    }
                }

                let op_plan_id = match op {
                    ParseExpressionInfixOperator::Concat => {
                        plan.add_concat(left_plan_id, right_plan_id)
                    }
                    ParseExpressionInfixOperator::InfixArithmetic(arith) => {
                        plan.add_arithmetic_to_plan(left_plan_id, arith.clone(), right_plan_id)?
                    }
                    ParseExpressionInfixOperator::InfixBool(bool) => {
                        plan.add_cond(left_plan_id, *bool, right_plan_id)?
                    }
                    ParseExpressionInfixOperator::Escape => {
                        unreachable!("escape op is not added to AST")
                    }
                };
                if *is_not {
                    plan.add_unary(Unary::Not, op_plan_id)?
                } else {
                    op_plan_id
                }
            }
            ParseExpression::Prefix { op, child } => {
                let child_plan_id = child.populate_plan(plan, worker)?;
                plan.add_unary(*op, child_plan_id)?
            }
            ParseExpression::Function {
                name,
                args,
                feature,
            } => {
                let is_distinct = matches!(feature, Some(FunctionFeature::Distinct));
                let mut plan_arg_ids = Vec::new();
                for arg in args {
                    let arg_plan_id = arg.populate_plan(plan, worker)?;
                    plan_arg_ids.push(arg_plan_id);
                }
                if let Some(kind) = AggregateKind::from_name(name) {
                    plan.add_aggregate_function(kind, plan_arg_ids, is_distinct)?
                } else if is_distinct {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some("DISTINCT modifier is allowed only for aggregate functions".into()),
                    ));
                } else {
                    let func = worker.metadata.function(name)?;
                    match func.volatility {
                        VolatilityType::Stable => {
                            plan.add_stable_function(func, plan_arg_ids, feature.clone())?
                        }
                        VolatilityType::Volatile => {
                            plan.add_volatile_function(func, plan_arg_ids, feature.clone())?
                        }
                    }
                }
            }
            ParseExpression::Row { children } => {
                let mut plan_children_ids = Vec::new();
                for child in children {
                    let plan_child_id = child.populate_plan(plan, worker)?;

                    plan_children_ids.push(plan_child_id);
                }
                plan.nodes.add_row(plan_children_ids, None)
            }
            ParseExpression::Exists { is_not, child } => {
                let ParseExpression::SubQueryPlanId { plan_id } = &**child else {
                    panic!("Expected SubQuery under EXISTS.")
                };
                let child_plan_id = plan.add_replaced_subquery(*plan_id, worker)?;
                let op_id = plan.add_unary(Unary::Exists, child_plan_id)?;
                if *is_not {
                    plan.add_unary(Unary::Not, op_id)?
                } else {
                    op_id
                }
            }
            ParseExpression::Is {
                is_not,
                child,
                value,
            } => {
                let child_plan_id = child.populate_plan(plan, worker)?;
                let op_id = match value {
                    None => plan.add_unary(Unary::IsNull, child_plan_id)?,
                    Some(b) => {
                        let right_operand = plan.add_const(Value::Boolean(*b));
                        plan.add_bool(child_plan_id, Bool::Eq, right_operand)?
                    }
                };
                if *is_not {
                    plan.add_unary(Unary::Not, op_id)?
                } else {
                    op_id
                }
            }
            ParseExpression::InterimBetween { .. } => return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(SmolStr::from(
                    "BETWEEN operator should have a view of `expr_1 BETWEEN expr_2 AND expr_3`.",
                )),
            )),
        };
        Ok(plan_id)
    }
}

/// Workaround for the following expressions:
///     * `expr_1 AND expr_2 BETWEEN expr_3 AND expr_4`
///     * `NOT expr_1 BETWEEN expr_2 AND expr_3`
/// which are parsed as:
///     * `(expr_1 AND (expr_2 BETWEEN expr_3)) AND expr_4`
///     * `(NOT (expr_1 BETWEEN expr_2)) AND expr_3`
/// but we should transform them into:
///     * `expr_1 AND (expr_2 BETWEEN expr_3 AND expr_4)`
///     * `NOT (expr_1 BETWEEN expr_2 AND expr_3)`
///
/// Returns:
/// * None in case `expr` doesn't have `InterimBetween` as a child
/// * Expression whose right child is an `InterimBetween` (or `InterimBetween` itself)
pub(in crate::frontend::sql) fn find_interim_between(
    mut expr: &mut ParseExpression,
) -> Option<(&mut ParseExpression, bool)> {
    let mut is_exact_match = true;
    loop {
        match expr {
            ParseExpression::Infix {
                // TODO: why we handle AND, but don't handle OR?
                op: ParseExpressionInfixOperator::InfixBool(Bool::And),
                right,
                ..
            } => {
                expr = right;
                is_exact_match = false;
            }
            ParseExpression::Prefix {
                op: Unary::Not,
                child,
            } => {
                expr = child;
                is_exact_match = false;
            }
            ParseExpression::InterimBetween { .. } => break Some((expr, is_exact_match)),
            _ => break None,
        }
    }
}

pub(in crate::frontend::sql) fn connect_escape_to_like_node(
    mut lhs: ParseExpression,
    rhs: ParseExpression,
) -> Result<ParseExpression, SbroadError> {
    let (ParseExpression::Like { escape, .. } | ParseExpression::Similar { escape, .. }) = &mut lhs
    else {
        return Err(SbroadError::Invalid(
            Entity::Expression,
            Some(format_smolstr!(
                "ESCAPE can go only after LIKE or SIMILAR expressions, got: {:?}",
                lhs
            )),
        ));
    };
    if escape.is_some() {
        return Err(SbroadError::Invalid(
            Entity::Expression,
            Some(
                "escape specified twice: expr1 LIKE/SIMILAR expr2 ESCAPE expr 3 ESCAPE expr4"
                    .into(),
            ),
        ));
    }
    *escape = Some(Box::new(rhs));
    Ok(lhs)
}
