use core::panic;
use pest::iterators::{Pair, Pairs};
use pest::pratt_parser::PrattParser;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::cell::RefCell;

use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::normalize_name_from_sql;
use crate::executor::engine::Metadata;
use crate::frontend::sql::ast::Rule;
use crate::frontend::sql::get_real_function_name;
use crate::frontend::sql::ir::SubtreeCloner;
use crate::frontend::sql::type_system::{self, get_parameter_derived_types, TypeAnalyzer};
use crate::ir::expression::{ColumnWithScan, FunctionFeature, Substring, TrimKind};
use crate::ir::node::{
    Bound, BoundType, CountAsterisk, Frame, FrameType, LetVarRef, NodeId, Over, ReferenceTarget,
    TimeParameters, Timestamp, Window,
};
use crate::ir::operator::{Arithmetic, Bool, OrderByElement, OrderByEntity, OrderByType, Unary};
use crate::ir::types::{CastType, DerivedType, NestedType};
use crate::ir::value::Value;
use crate::ir::Plan;

use super::{
    connect_escape_to_like_node, find_interim_between, parse_param, ExpressionWalker, OrderNulls,
    ParseExpression, ParseExpressionInfixOperator,
};

fn parse_trim<M: Metadata>(
    pair: Pair<Rule>,
    param_types: &[DerivedType],
    referred_relation_ids: &[NodeId],
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<ParseExpression, SbroadError> {
    assert_eq!(pair.as_rule(), Rule::Trim);
    let mut kind = None;
    let mut pattern = None;
    let mut target = None;

    let inner_pairs = pair.into_inner();
    for child_pair in &mut inner_pairs.into_iter() {
        match child_pair.as_rule() {
            Rule::TrimKind => {
                let kind_pair = child_pair
                    .into_inner()
                    .next()
                    .expect("Expected child of TrimKind");
                match kind_pair.as_rule() {
                    Rule::TrimKindBoth => kind = Some(TrimKind::Both),
                    Rule::TrimKindLeading => kind = Some(TrimKind::Leading),
                    Rule::TrimKindTrailing => kind = Some(TrimKind::Trailing),
                    _ => {
                        panic!("Unexpected node: {kind_pair:?}");
                    }
                }
            }
            Rule::TrimPattern => {
                let inner_pattern = child_pair.into_inner();
                pattern = Some(Box::new(parse_expr_pratt(
                    inner_pattern,
                    param_types,
                    referred_relation_ids,
                    worker,
                    plan,
                    true,
                )?));
            }
            Rule::TrimTarget => {
                let inner_target = child_pair.into_inner();
                target = Some(Box::new(parse_expr_pratt(
                    inner_target,
                    param_types,
                    referred_relation_ids,
                    worker,
                    plan,
                    true,
                )?));
            }
            _ => {
                panic!("Unexpected node: {child_pair:?}");
            }
        }
    }
    let trim = ParseExpression::Trim {
        kind,
        pattern,
        target: target.expect("Trim target must be specified"),
    };
    Ok(trim)
}

// Helper structure used to resolve expression operators priority.
lazy_static::lazy_static! {
    static ref PRATT_PARSER: PrattParser<Rule> = {
        use pest::pratt_parser::{Assoc::Left, Op};
        use Rule::{Add, And, Between, ConcatInfixOp, Divide, Eq, Escape, Gt, GtEq,
            In, IndexPostfix, IsPostfix, CastPostfix, Like, Similar, Lt, LtEq, Modulo, Multiply,
            NotEq, Or, Subtract, UnaryNot
        };

        // Precedence is defined lowest to highest.
        PrattParser::new()
            .op(Op::infix(Or, Left))
            .op(Op::infix(And, Left))
            .op(Op::prefix(UnaryNot))
            // ESCAPE must be followed by LIKE
            .op(Op::infix(Escape, Left))
            .op(Op::infix(Like, Left))
            .op(Op::infix(Similar, Left))
            .op(Op::infix(Between, Left))
            .op(
                Op::infix(Eq, Left) | Op::infix(NotEq, Left)
                | Op::infix(Gt, Left) | Op::infix(GtEq, Left) | Op::infix(Lt, Left)
                | Op::infix(LtEq, Left) | Op::infix(In, Left)
            )
            .op(Op::infix(Add, Left) | Op::infix(Subtract, Left))
            .op(Op::infix(Multiply, Left) | Op::infix(Divide, Left) | Op::infix(ConcatInfixOp, Left) | Op::infix(Modulo, Left))
            .op(Op::postfix(IsPostfix))
            .op(Op::postfix(IndexPostfix))
            .op(Op::postfix(CastPostfix))
    };
}

fn cast_type_from_pair(cast_target_pair: Pair<Rule>) -> Result<CastType, SbroadError> {
    debug_assert_eq!(cast_target_pair.as_rule(), Rule::CastTarget);
    let mut cast_target_pairs = cast_target_pair.into_inner();
    let type_pair = cast_target_pairs
        .next()
        .expect("Type expected under CastTarget");
    let has_array_suffix = cast_target_pairs.next().is_some();

    let mut column_def_type_pairs = type_pair.into_inner();
    let column_def_type = column_def_type_pairs
        .next()
        .expect("concrete type expected under Type");
    if has_array_suffix {
        let elem = NestedType::try_from(column_def_type.as_rule())?;
        return Ok(CastType::Array(elem));
    }
    if column_def_type.as_rule() != Rule::TypeVarchar {
        return CastType::try_from(&column_def_type.as_rule());
    }

    let mut type_pairs_inner = column_def_type.into_inner();
    let type_cast = type_pairs_inner.next().map_or_else(
        || Ok(CastType::String),
        |varchar_length| {
            varchar_length
                .as_str()
                .parse::<usize>()
                .map(|_| CastType::String)
                .map_err(|e| {
                    SbroadError::ParsingError(
                        Entity::Value,
                        format_smolstr!("Failed to parse varchar length: {e:?}."),
                    )
                })
        },
    )?;
    Ok(type_cast)
}

fn parse_window_func<M: Metadata>(
    pair: Pair<Rule>,
    param_types: &[DerivedType],
    referred_relation_ids: &[NodeId],
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<NodeId, SbroadError> {
    let mut inner = pair.into_inner();

    // Parse function name
    let func_name_pair = inner.next().expect("Function name expected under Over");
    let func_name = func_name_pair.as_str().to_lowercase().to_smolstr();

    // Parse function arguments
    let args_pair = inner.next().expect("Function args expected under Over");
    let mut func_args = if let Some(args_inner) = args_pair.clone().into_inner().next() {
        match args_inner.as_rule() {
            Rule::CountAsterisk => Vec::with_capacity(1),
            Rule::WindowFunctionArgsInner => {
                let args_count = args_inner.into_inner().count();
                Vec::with_capacity(args_count)
            }
            _ => Vec::new(),
        }
    } else {
        Vec::new()
    };

    if let Some(args_inner) = args_pair.into_inner().next() {
        match args_inner.as_rule() {
            Rule::CountAsterisk => {
                if "count" != func_name.as_str() {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format_smolstr!(
                            "\"*\" is allowed only inside \"count\" aggregate function. Got: {func_name}",
                        ))
                    ));
                }
                let count_asterisk_plan_id = plan.nodes.push(CountAsterisk {}.into());
                func_args.push(count_asterisk_plan_id);
            }
            Rule::WindowFunctionArgsInner => {
                for arg_pair in args_inner.into_inner() {
                    let expr_plan_node_id = parse_expr_no_type_check(
                        Pairs::single(arg_pair),
                        param_types,
                        referred_relation_ids,
                        worker,
                        plan,
                        true,
                    )?;
                    func_args.push(expr_plan_node_id);
                }
            }
            _ => panic!(
                "Unexpected rule met under WindowFunction: {:?}",
                args_inner.as_rule()
            ),
        }
    }

    // Create ScalarFunction node
    let stable_func_id = plan.add_builtin_window_function(func_name, func_args)?;
    // Parse filter
    let filter_pair = inner.next().expect("Filter expected under Over");
    let filter = if let Some(filter_inner) = filter_pair.into_inner().next() {
        let expr = parse_expr_no_type_check(
            Pairs::single(filter_inner),
            param_types,
            referred_relation_ids,
            worker,
            plan,
            false,
        )?;
        Some(expr)
    } else {
        None
    };

    // Parse window
    let window_pair = inner.next().expect("Window expected under Over");
    let window = match window_pair.as_rule() {
        Rule::Identifier => {
            let window_name = window_pair.as_str().to_smolstr();

            let err = Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format_smolstr!("Window with name {window_name} not found")),
            ));

            worker
                .curr_named_windows
                .get(&window_name)
                .map_or(err, |id| Ok(*id))?
        }
        Rule::WindowBody => {
            let mut partition: Option<Vec<NodeId>> = None;
            let mut ordering = None;
            let mut frame = None;

            let err = Err(SbroadError::Invalid(
                Entity::Expression,
                Some(SmolStr::from("Invalid WINDOW body definition. Expected view of [BASE_WINDOW] [PARTITION BY] [ORDER BY] [FRAME]"))
            ));

            for body_part in window_pair.into_inner() {
                match body_part.as_rule() {
                    Rule::WindowPartition => {
                        if partition.is_some() || ordering.is_some() || frame.is_some() {
                            return err;
                        }

                        let mut partition_exprs = Vec::new();
                        for part_expr in body_part.into_inner() {
                            let part_expr_plan_node_id = parse_expr_no_type_check(
                                Pairs::single(part_expr),
                                param_types,
                                referred_relation_ids,
                                worker,
                                plan,
                                false,
                            )?;
                            partition_exprs.push(part_expr_plan_node_id);
                        }
                        partition = Some(partition_exprs);
                    }
                    Rule::WindowOrderBy => {
                        let mut order_by_elements = Vec::new();
                        for order_item in body_part.into_inner() {
                            let mut order_item_inner = order_item.into_inner();
                            let expr_pair = order_item_inner
                                .next()
                                .expect("Expected expression in ORDER BY");
                            let expr_id = parse_expr_no_type_check(
                                Pairs::single(expr_pair.clone()),
                                param_types,
                                referred_relation_ids,
                                worker,
                                plan,
                                false,
                            )?;

                            let (order_type, order_nulls) = {
                                let mut order_type = Some(OrderByType::Asc);
                                let mut order_nulls = None;

                                for rule in order_item_inner.map(|p| p.as_rule()) {
                                    match rule {
                                        Rule::Asc => {}
                                        Rule::Desc => order_type = Some(OrderByType::Desc),
                                        Rule::NullsFirst => order_nulls = Some(OrderNulls::First),
                                        Rule::NullsLast => order_nulls = Some(OrderNulls::Last),
                                        rule => unreachable!(
                                            "{}",
                                            format!(
                                                "Unexpected rule met under OrderByElement: {rule:?}"
                                            )
                                        ),
                                    }
                                }
                                (order_type, order_nulls)
                            };

                            if let Some(order_nulls) = order_nulls {
                                let nulls_expr_id = SubtreeCloner::clone_subtree(plan, expr_id)?;
                                let is_null_expr_id =
                                    plan.add_unary(Unary::IsNull, nulls_expr_id)?;
                                let top_expr_id = match order_nulls {
                                    OrderNulls::Last => is_null_expr_id,
                                    OrderNulls::First => {
                                        plan.add_unary(Unary::Not, is_null_expr_id)?
                                    }
                                };
                                let new_entity_first = OrderByEntity::Expression {
                                    expr_id: top_expr_id,
                                };
                                order_by_elements.push(OrderByElement {
                                    entity: new_entity_first,
                                    order_type: None,
                                });
                            }

                            order_by_elements.push(OrderByElement {
                                entity: OrderByEntity::Expression { expr_id },
                                order_type,
                            });
                        }
                        ordering = Some(order_by_elements)
                    }
                    Rule::WindowFrame => {
                        if frame.is_some() {
                            return err;
                        }

                        let mut frame_inner = body_part.into_inner();
                        let frame_type = frame_inner
                            .next()
                            .expect("WindowFrame should have WindowFrameType child");
                        let ty = match frame_type.as_rule() {
                            Rule::WindowFrameTypeRange => FrameType::Range,
                            Rule::WindowFrameTypeRows => FrameType::Rows,
                            _ => panic!(
                                "Expected FrameTypeRange or FrameTypeRows, got: {frame_type:?}"
                            ),
                        };

                        let frame_bound = frame_inner
                            .next()
                            .expect("WindowFrame should have WindowFrameBound children");
                        let bound = match frame_bound.as_rule() {
                            Rule::WindowFrameSingle => {
                                let bound_inner = frame_bound.into_inner().next().expect(
                                    "WindowFrameSingle should contain WindowFrameBound as a child",
                                );
                                let bound_type = parse_frame_bound(
                                    bound_inner,
                                    param_types,
                                    referred_relation_ids,
                                    worker,
                                    plan,
                                )?;
                                Bound::Single(bound_type)
                            }
                            Rule::WindowFrameBetween => {
                                let mut between_bounds = frame_bound.into_inner();
                                let first_bound = between_bounds.next().expect(
                                    "WindowFrameBetween should contain an opening bound as a child",
                                );
                                let second_bound = between_bounds.next().expect(
                                    "WindowFrameBetween should contain a closing bound as a child",
                                );
                                let first_type = parse_frame_bound(
                                    first_bound,
                                    param_types,
                                    referred_relation_ids,
                                    worker,
                                    plan,
                                )?;
                                let second_type = parse_frame_bound(
                                    second_bound,
                                    param_types,
                                    referred_relation_ids,
                                    worker,
                                    plan,
                                )?;
                                Bound::Between(first_type, second_type)
                            }
                            _ => panic!("Unexpected rule met under WindowFrame: {frame_bound:?}"),
                        };

                        frame = Some(Frame { ty, bound });
                    }
                    _ => panic!("Unexpected rule met under WindowBody: {body_part:?}"),
                }
            }

            let window = Window {
                partition,
                ordering,
                frame,
            };
            plan.nodes.push(window.into())
        }
        _ => panic!("Unexpected rule met under Window: {window_pair:?}"),
    };
    worker.curr_windows.push(window);
    let over = Over {
        stable_func: stable_func_id,
        filter,
        window,
    };

    Ok(plan.nodes.push(over.into()))
}

fn parse_frame_bound<M: Metadata>(
    bound: Pair<Rule>,
    param_types: &[DerivedType],
    referred_relation_ids: &[NodeId],
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<BoundType, SbroadError> {
    let rule = bound.as_rule();
    match rule {
        Rule::PrecedingUnbounded => Ok(BoundType::PrecedingUnbounded),
        Rule::CurrentRow => Ok(BoundType::CurrentRow),
        Rule::FollowingUnbounded => Ok(BoundType::FollowingUnbounded),
        Rule::PrecedingOffset | Rule::FollowingOffset => {
            let offset_expr = bound
                .into_inner()
                .next()
                .expect("Expr node expected under Offset window bound");
            let offset_expr_id = parse_expr_no_type_check(
                Pairs::single(offset_expr),
                param_types,
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;

            Ok(match rule {
                Rule::PrecedingOffset => BoundType::PrecedingOffset(offset_expr_id),
                Rule::FollowingOffset => BoundType::FollowingOffset(offset_expr_id),
                _ => unreachable!("Offset bound expected"),
            })
        }
        _ => panic!("Unexpected rule met under WindowFrameBound: {rule:?}"),
    }
}

fn parse_substring<M: Metadata>(
    pair: Pair<Rule>,
    param_types: &[DerivedType],
    referred_relation_ids: &[NodeId],
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<ParseExpression, SbroadError> {
    assert_eq!(pair.as_rule(), Rule::Substring);

    let mut inner = pair.into_inner();
    let variant = inner.next().ok_or_else(|| {
        SbroadError::ParsingError(Entity::Expression, "no substring variant".into())
    })?;

    match variant.as_rule() {
        Rule::SubstringFromFor => {
            // Handle: substring(expr FROM expr FOR expr)
            let mut pieces = variant.into_inner();
            let string_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                param_types,
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;
            let from_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                param_types,
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;
            let for_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                param_types,
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;

            Ok(ParseExpression::Function {
                name: "substring".to_string(),
                args: vec![string_expr, from_expr, for_expr],
                feature: Some(FunctionFeature::Substring(Substring::FromFor)),
            })
        }
        Rule::SubstringRegular => {
            // Handle: substring(expr, expr, expr)
            let mut pieces = variant.into_inner();
            let string_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                param_types,
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;
            let from_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                param_types,
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;
            let for_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                param_types,
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;

            Ok(ParseExpression::Function {
                name: "substring".to_string(),
                args: vec![string_expr, from_expr, for_expr],
                feature: Some(FunctionFeature::Substring(Substring::Regular)),
            })
        }
        Rule::SubstringFor => {
            // Handle: substring(expr FOR expr)
            let mut pieces = variant.into_inner();
            let string_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                param_types,
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;
            let for_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                param_types,
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;

            let string_id = string_expr.populate_plan(plan, worker)?;
            let one_literal = plan.add_const(Value::Integer(1));
            let for_id = for_expr.populate_plan(plan, worker)?;

            Ok(ParseExpression::Function {
                name: "substr".to_string(),
                args: vec![
                    ParseExpression::PlanId { plan_id: string_id },
                    ParseExpression::PlanId {
                        plan_id: one_literal,
                    },
                    ParseExpression::PlanId { plan_id: for_id },
                ],
                feature: Some(FunctionFeature::Substring(Substring::For)),
            })
        }
        Rule::SubstringFrom => {
            // Handle: substring(expr FROM expr) - both numeric and regexp variants
            let mut pieces = variant.into_inner();
            let string_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                param_types,
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;
            let from_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                param_types,
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;

            Ok(ParseExpression::Function {
                name: "substring".to_string(),
                args: vec![string_expr, from_expr],
                feature: Some(FunctionFeature::Substring(Substring::From)),
            })
        }
        Rule::SubstringSimilar => {
            // Handle: substring(expr SIMILAR expr ESCAPE expr)
            let mut pieces = variant.into_inner();
            let similar_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                param_types,
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;

            let mut args = vec![];

            if let ParseExpression::Similar {
                left,
                right,
                escape,
            } = similar_expr
            {
                // Provide a better error message when the escape symbol is missing.
                let escape_box = match escape {
                    Some(esc) => esc,
                    None => {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some("missing escape symbol for SIMILAR substring operator".into()),
                        ))
                    }
                };

                // Bind the expressions from the Similar variant.
                let string_expr = *left;
                let pattern_expr = *right;
                let escape_expr = *escape_box;

                args.push(string_expr);
                args.push(pattern_expr);
                args.push(escape_expr);
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some("incorrect SUBSTRING parameters. There is no such overload that takes only 1 argument".into()),
                ));
            }

            Ok(ParseExpression::Function {
                name: "substring".to_string(),
                args,
                feature: Some(FunctionFeature::Substring(Substring::Similar)),
            })
        }
        _ => Err(SbroadError::ParsingError(
            Entity::Expression,
            "Unrecognized SubstringVariant".into(),
        )),
    }
}

/// Function responsible for parsing expressions using Pratt parser.
///
/// Parameters:
/// * Raw `expression_pair`, resulted from pest parsing. General idea is that we always have to
///   pass `Expr` pair with `into_inner` call.
/// * `ast` resulted from some parsing node transformations
/// * `plan` currently being built
///
/// Returns:
/// * Id of root `Expression` node added into the plan
#[allow(clippy::too_many_lines)]
fn parse_expr_pratt<M>(
    expression_pairs: Pairs<Rule>,
    param_types: &[DerivedType],
    referred_relation_ids: &[NodeId],
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
    safe_for_volatile_function: bool,
) -> Result<ParseExpression, SbroadError>
where
    M: Metadata,
{
    type WhenBlocks = Vec<(Box<ParseExpression>, Box<ParseExpression>)>;

    let worker = RefCell::new(worker);
    let plan = RefCell::new(plan);

    let res = PRATT_PARSER
        .map_primary(|primary| {
            let mut worker = worker.borrow_mut();
            let worker = &mut **worker;
            let mut plan = plan.borrow_mut();
            let plan = &mut **plan;

            let parse_expr = match primary.as_rule() {
                Rule::Expr | Rule::Literal => {
                    parse_expr_pratt(primary.into_inner(), param_types, referred_relation_ids, worker, plan, safe_for_volatile_function)?
                }
                Rule::ExpressionInParentheses => {
                    let mut inner_pairs = primary.into_inner();
                    let child_expr_pair = inner_pairs
                        .next()
                        .expect("Expected to see inner expression under parentheses");
                    parse_expr_pratt(
                        Pairs::single(child_expr_pair),
                        param_types,
                        referred_relation_ids,
                        worker,
                        plan,
                        safe_for_volatile_function,
                    )?
                }
                Rule::Parameter => {
                    let plan_id = parse_param(primary, param_types, worker, plan)?;
                    ParseExpression::PlanId { plan_id }
                }
                Rule::Over => {
                    let plan_id = parse_window_func(primary, param_types, referred_relation_ids, worker, plan)?;
                    ParseExpression::PlanId { plan_id }
                }
                Rule::IdentifierWithOptionalContinuation => {
                    let mut inner_pairs = primary.into_inner();
                    let first_identifier = inner_pairs.next().expect(
                        "Identifier expected under IdentifierWithOptionalContinuation"
                    ).as_str();

                    let mut scan_name = None;
                    let mut col_name = normalize_name_from_sql(first_identifier);

					// simple id, without reference continuation or function invocation continuation
					let is_simple_id = inner_pairs.is_empty();

                    if !is_simple_id {
                        let continuation = inner_pairs.next().expect("Continuation expected after Identifier");
                        match continuation.as_rule() {
                            Rule::ReferenceContinuation => {
                                let col_name_pair = continuation.into_inner()
                                    .next().expect("Reference continuation must contain an Identifier");
                                let second_identifier = normalize_name_from_sql(col_name_pair.as_str());
                                scan_name = Some(col_name);
                                col_name = second_identifier;
                            }
                            Rule::FunctionInvocationContinuation => {
                                // Handle function invocation case.
                                let mut function_name = String::from(first_identifier);
                                let mut args_pairs = continuation.into_inner();
                                let mut feature = None;
                                let mut parse_exprs_args = Vec::new();
                                let function_args = args_pairs.next();
                                if let Some(function_args) = function_args {
                                    match function_args.as_rule() {
                                        Rule::CountAsterisk => {
                                            let normalized_name = function_name.to_lowercase();
                                            if "count" != normalized_name.as_str() {
                                                return Err(SbroadError::Invalid(
                                                    Entity::Query,
                                                    Some(format_smolstr!(
                                                        "\"*\" is allowed only inside \"count\" aggregate function. Got: {normalized_name}",
                                                    ))
                                                ));
                                            }
                                            let count_asterisk_plan_id = plan.nodes.push(CountAsterisk{}.into());
                                            parse_exprs_args.push(ParseExpression::PlanId { plan_id: count_asterisk_plan_id });
                                        }
                                        Rule::FunctionArgs => {
                                            let mut args_inner = function_args.into_inner();
                                            let mut arg_pairs_to_parse = Vec::new();
                                            let mut volatile = false;

                                            // Exposed by picodata scalar function name should be
                                            // transformed to real name of representing it stored procedure.
                                            if let Some(name) = get_real_function_name(&function_name) {
                                                function_name = name.to_string();
                                            }

                                            // We should not through an error here as while performing
                                            // Pratt parsing, we inspect function volatility and may
                                            // disallow a function based on the higher-level context
                                            // (for example, volatile functions in filters). But if we
                                            // error out early because the function is missing from
                                            // metadata, we lose meaningful aggregate error diagnostics.
                                            // Metadata will be checked again during full parsing,
                                            // where a correct "function not found" error can be produced.
                                            if let Ok(f) = worker.metadata.function(&function_name) {
                                                volatile = f.is_volatile();
                                            }

                                            if volatile && !safe_for_volatile_function {
                                                return Err(SbroadError::NotImplemented(
                                                    Entity::VolatileFunction, "is not allowed in filter clause".to_smolstr(),
                                                    )
                                                );
                                            }

                                            if let Some(first_arg_pair) = args_inner.next() {
                                                if let Rule::Distinct = first_arg_pair.as_rule() {
                                                    if volatile {
                                                        return Err(SbroadError::Invalid(
                                                            Entity::Query,
                                                            Some(format_smolstr!(
                                                                "\"distinct\" is not allowed inside VOLATILE function call",
                                                            ))
                                                        ));
                                                    }

                                                    feature = Some(FunctionFeature::Distinct);
                                                } else {
                                                    arg_pairs_to_parse.push(first_arg_pair);
                                                }
                                            }

                                            for arg_pair in args_inner {
                                                arg_pairs_to_parse.push(arg_pair);
                                            }

                                            for arg in arg_pairs_to_parse {
                                                let arg_expr = parse_expr_pratt(
                                                    arg.into_inner(),
                                                    param_types,
                                                    referred_relation_ids,
                                                    worker,
                                                    plan,
                                                   safe_for_volatile_function,
                                                )?;
                                                parse_exprs_args.push(arg_expr);
                                            }
                                        }
                                        rule => unreachable!("{}", format!("Unexpected rule under FunctionInvocation: {rule:?}"))
                                    }
                                }
                                return Ok(ParseExpression::Function {
                                    name: function_name,
                                    args: parse_exprs_args,
                                    feature,
                                })
                            }
                            rule => unreachable!("Expr::parse expected identifier continuation, found {:?}", rule)
                        }
                    };

                    // LET-variable resolution. Only applies to bare identifiers
                    // inside an anonymous block — qualified `t.x` always
                    // refers to a column. If the LET scope has the name we
                    // also probe the relation columns to detect ambiguity:
                    // a column with the same name shadowing a LET would be
                    // surprising, so we report the conflict instead of
                    // silently picking one.
                    let is_bare_ident = is_simple_id && scan_name.is_none();
                    let let_decl = worker.let_scope.lookup(&col_name);
                    // FIXME: use let chain after bumping rust to 2024
                    if let (Some(let_decl), true) = (let_decl, is_bare_ident) {
                        let let_ty = let_decl.ty;
                        for rel_id in referred_relation_ids {
                            if worker.build_columns_map(plan, *rel_id).is_ok()
                                && worker
                                    .columns_map_get_positions(*rel_id, &col_name, None)
                                    .is_ok()
                            {
                                return Err(SbroadError::Other(format_smolstr!(
                                    "column reference \"{col_name}\" is ambiguous: \
                                     it could refer to either a LET variable or a table column"
                                )));
                            }
                        }
                        worker.let_scope.mark_used(&col_name);
                        let plan_id = plan.nodes.push(
                            LetVarRef {
                                name: col_name.clone(),
                                var_type: let_ty,
                            }
                            .into(),
                        );
                        worker.reference_to_name_map.insert(plan_id, col_name);
                        return Ok(ParseExpression::PlanId { plan_id });
                    }

                    if referred_relation_ids.is_empty() {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some(format_smolstr!("Reference {first_identifier} met under Values that is unsupported. For string literals use single quotes."))
                        ))
                    }

                    let plan_left_id = referred_relation_ids
                        .first()
                        .ok_or(SbroadError::Invalid(
                            Entity::Query,
                            Some("Reference must point to some relational node".into())
                        ))?;

                    worker.build_columns_map(plan, *plan_left_id)?;

                    let left_child_col_position = worker.columns_map_get_positions(*plan_left_id, &col_name, scan_name.as_deref());

                    let plan_right_id = referred_relation_ids.get(1);
                    if let Some(plan_right_id) = plan_right_id {
                        // Referencing Join node.
                        worker.build_columns_map(plan, *plan_right_id)?;
                        let right_child_col_position = worker.columns_map_get_positions(*plan_right_id, &col_name, scan_name.as_deref());

                        let present_in_left = left_child_col_position.is_ok();
                        let present_in_right = right_child_col_position.is_ok();

                        let ref_id = if present_in_left && present_in_right {
                            return Err(SbroadError::Invalid(
                                Entity::Column,
                                Some(format_smolstr!(
                                    "column name '{col_name}' is present in both join children",
                                )),
                            ));
                        } else if present_in_left {
                            let col_with_scan = ColumnWithScan::new(&col_name, scan_name.as_deref());
                            plan.add_ref_from_left_branch(
                                *plan_left_id,
                                *plan_right_id,
                                col_with_scan,
                            )?
                        } else if present_in_right {
                            let col_with_scan = ColumnWithScan::new(&col_name, scan_name.as_deref());
                            plan.add_ref_from_right_branch(
                                *plan_left_id,
                                *plan_right_id,
                                col_with_scan,
                            )?
                        } else {
                            return Err(SbroadError::NotFound(
                                Entity::Column,
                                format_smolstr!("'{col_name}' in the join children",),
                            ));
                        };
                        worker.reference_to_name_map.insert(ref_id, col_name);
						ParseExpression::PlanId { plan_id: ref_id }
                    } else {
                        // Referencing single node.
                        match left_child_col_position {
                            Ok(col_position) => {
								let child = plan.get_relation_node(*plan_left_id)?;
								let child_alias_ids = plan.get_row_list(
									child.output()
								)?;
								let child_alias_id = child_alias_ids
									.get(col_position)
									.expect("column position is invalid");
								let col_type = plan
									.get_expression_node(*child_alias_id)?
									.calculate_type(plan)?;
								let ref_id = plan.nodes.add_ref(
									ReferenceTarget::Single(*plan_left_id),
									col_position,
									col_type,
									None,
									false);
								worker.reference_to_name_map.insert(ref_id, col_name);
								ParseExpression::PlanId { plan_id: ref_id }
							}
                            Err(e) => {
								if is_simple_id && worker.inside_grouping_expression {
									let ref_id = plan.nodes.add_ref(ReferenceTarget::Leaf, 0, DerivedType::unknown(), None, false);
									let alias_id = plan.nodes.add_alias(&col_name, ref_id)?;
									ParseExpression::PlanId { plan_id: alias_id }
								} else {
									return Err(e);
								}
							}
                        }
                    }
                }
                Rule::SubQuery => {
                    let sq_ast_id: &usize = worker
                        .sq_pair_to_ast_ids
                        .get(&primary).expect("SQ ast_id must exist for pest pair");
                    let plan_id = worker
                        .sq_ast_to_plan_id
                        .get(*sq_ast_id)?;
                    ParseExpression::SubQueryPlanId { plan_id }
                }
                Rule::Row => {
                    let mut children = Vec::new();

                    for expr_pair in primary.into_inner() {
                        let child_parse_expr = parse_expr_pratt(
                            expr_pair.into_inner(),
                            param_types,
                            referred_relation_ids,
                            worker,
                            plan,
                            safe_for_volatile_function,
                        )?;
                        children.push(child_parse_expr);
                    }
                    ParseExpression::Row { children }
                }
                Rule::ArrayLiteral => {
                    let mut children = Vec::new();
                    for expr_pair in primary.into_inner() {
                        let child_parse_expr = parse_expr_pratt(
                            expr_pair.into_inner(),
                            param_types,
                            referred_relation_ids,
                            worker,
                            plan,
                            safe_for_volatile_function,
                        )?;
                        children.push(child_parse_expr);
                    }
                    ParseExpression::ArrayLiteral { children }
                }
                Rule::Decimal
                | Rule::Double
                | Rule::Unsigned
                | Rule::Null
                | Rule::True
                | Rule::SingleQuotedString
                | Rule::Integer
                | Rule::False => {
                    let val = Value::from_node(&primary)?;
                    let plan_id = plan.add_const(val);
                    ParseExpression::PlanId { plan_id }
                }
                Rule::Exists => {
                    let mut inner_pairs = primary.into_inner();
                    let first_pair = inner_pairs.next()
                        .expect("No child found under Exists node");
                    let first_is_not = matches!(first_pair.as_rule(), Rule::NotFlag);
                    let expr_pair = if first_is_not {
                        inner_pairs.next()
                            .expect("Expr expected next to NotFlag under Exists")
                    } else {
                        first_pair
                    };

                    let child_parse_expr = parse_expr_pratt(
                        Pairs::single(expr_pair),
                        param_types,
                        referred_relation_ids,
                        worker,
                        plan,
                      safe_for_volatile_function
                    )?;
                    ParseExpression::Exists { is_not: first_is_not, child: Box::new(child_parse_expr)}
                }
                Rule::Trim => parse_trim(primary, param_types, referred_relation_ids, worker, plan)?,
                Rule::Substring => parse_substring(primary, param_types, referred_relation_ids, worker, plan)?,
                Rule::CastOp => {
                    let mut inner_pairs = primary.into_inner();
                    let expr_pair = inner_pairs.next().expect("Cast has no expr child.");
                    let child_parse_expr = parse_expr_pratt(
                        expr_pair.into_inner(),
                        param_types,
                        referred_relation_ids,
                        worker,
                        plan,
                       safe_for_volatile_function,
                    )?;
                    let type_pair = inner_pairs.next().expect("CastOp has no type child");
                    let cast_type = cast_type_from_pair(type_pair)?;

                    ParseExpression::Cast { cast_type, child: Box::new(child_parse_expr) }
                }
                Rule::Case => {
                    let mut inner_pairs = primary.into_inner();

                    let first_pair = inner_pairs.next().expect("Case must have at least one child");
                    let mut when_block_pairs = Vec::new();
                    let search_expr = if let Rule::Expr = first_pair.as_rule() {
                        let expr = parse_expr_pratt(
                            first_pair.into_inner(),
                            param_types,
                            referred_relation_ids,
                            worker,
                            plan,
                            safe_for_volatile_function,
                        )?;
                        Some(Box::new(expr))
                    } else {
                        when_block_pairs.push(first_pair);
                        None
                    };

                    let mut else_expr = None;
                    for pair in inner_pairs {
                        if Rule::CaseElseBlock == pair.as_rule() {
                            let expr = parse_expr_pratt(
                                pair.into_inner(),
                                param_types,
                                referred_relation_ids,
                                worker,
                                plan,
                                safe_for_volatile_function,
                            )?;
                            else_expr = Some(Box::new(expr));
                        } else {
                            when_block_pairs.push(pair);
                        }
                    }

                    let when_blocks: Result<WhenBlocks, SbroadError> = when_block_pairs
                        .into_iter()
                        .map(|when_block_pair| {
                            let mut inner_pairs = when_block_pair.into_inner();
                            let condition_expr_pair = inner_pairs.next().expect("When block must contain condition expression.");
                            let condition_expr = parse_expr_pratt(
                                condition_expr_pair.into_inner(),
                                param_types,
                                referred_relation_ids,
                                worker,
                                plan,
                               false,
                            )?;

                            let result_expr_pair = inner_pairs.next().expect("When block must contain result expression.");
                            let result_expr = parse_expr_pratt(
                                result_expr_pair.into_inner(),
                                param_types,
                                referred_relation_ids,
                                worker,
                                plan,
                               safe_for_volatile_function,
                            )?;

                            Ok::<(Box<ParseExpression>, Box<ParseExpression>), SbroadError>((
                                Box::new(condition_expr),
                                Box::new(result_expr)
                            ))

                        })
                        .collect();

                    ParseExpression::Case {
                        search_expr,
                        when_blocks: when_blocks?,
                        else_expr,
                    }
                }
                Rule::CurrentDate => {
                    let plan_id = plan.nodes.push(Timestamp::Date.into());
                    ParseExpression::PlanId { plan_id }
                }
                rule @ (Rule::CurrentTime |
                Rule::CurrentTimestamp |
                Rule::LocalTime |
                Rule::LocalTimestamp) => {
                    let precision = primary.into_inner().next()
                        .map(|p| p.as_str().parse::<usize>().unwrap_or(usize::MAX).min(6))
                        .unwrap_or(6); // Default for Postgres is 6

                    let timestamp = match rule {
                        Rule::CurrentTime => {
                            return Err(SbroadError::NotImplemented(
                                Entity::SQLFunction,
                                "`CURRENT_TIME`".to_smolstr())
                            );
                        }
                        Rule::CurrentTimestamp => {
                            Timestamp::DateTime(TimeParameters {
                                precision,
                                include_timezone: true,
                            })
                        }
                        Rule::LocalTime => {
                            return Err(SbroadError::NotImplemented(
                                Entity::SQLFunction,
                                "`LOCALTIME`".to_smolstr())
                            );
                        }
                        Rule::LocalTimestamp => {
                            Timestamp::DateTime(TimeParameters {
                                precision,
                                include_timezone: false,
                            })
                        }
                        _ => unreachable!()
                    };

                    let plan_id = plan.nodes.push(timestamp.into());
                    ParseExpression::PlanId { plan_id }
                }
                Rule::CountAsterisk => {
                    let plan_id = plan.nodes.push(CountAsterisk{}.into());
                    ParseExpression::PlanId { plan_id }
                }
                rule      => unreachable!("Expr::parse expected atomic rule, found {:?}", rule),
            };
            Ok(parse_expr)
        })
        .map_infix(|lhs, op, rhs| {
            let mut lhs = lhs?;
            let rhs = rhs?;
            let mut is_not = false;
            let op = match op.as_rule() {
                Rule::And => ParseExpressionInfixOperator::InfixBool(Bool::And),
                Rule::Or => ParseExpressionInfixOperator::InfixBool(Bool::Or),
                Rule::Like => {
                    let is_ilike = op.as_str().to_lowercase().contains("ilike");
                    return Ok(ParseExpression::Like {
                        left: Box::new(lhs),
                        right: Box::new(rhs),
                        escape: None,
                        is_ilike
                    })
                },
                Rule::Similar => {
                    return Ok(ParseExpression::Similar {
                        left: Box::new(lhs),
                        right: Box::new(rhs),
                        escape: None,
                    })
                },
                Rule::Between => {
                    let mut op_inner = op.into_inner();
                    is_not = op_inner.next().is_some();
                    return Ok(ParseExpression::InterimBetween {
                        is_not,
                        left: Box::new(lhs),
                        right: Box::new(rhs),
                    })
                },
                Rule::Escape => ParseExpressionInfixOperator::Escape,
                Rule::Eq => ParseExpressionInfixOperator::InfixBool(Bool::Eq),
                Rule::NotEq => ParseExpressionInfixOperator::InfixBool(Bool::NotEq),
                Rule::Lt => ParseExpressionInfixOperator::InfixBool(Bool::Lt),
                Rule::LtEq => ParseExpressionInfixOperator::InfixBool(Bool::LtEq),
                Rule::Gt => ParseExpressionInfixOperator::InfixBool(Bool::Gt),
                Rule::GtEq => ParseExpressionInfixOperator::InfixBool(Bool::GtEq),
                Rule::In => {
                    if !matches!(rhs, ParseExpression::Row{..}|ParseExpression::SubQueryPlanId{..}) {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some(format_smolstr!("In expression must have query or a list of values as right child"))
                        ));
                    }
                    let mut op_inner = op.into_inner();
                    is_not = op_inner.next().is_some_and(|i| matches!(i.as_rule(), Rule::NotFlag));
                    ParseExpressionInfixOperator::InfixBool(Bool::In)
                }
                Rule::Subtract      => ParseExpressionInfixOperator::InfixArithmetic(Arithmetic::Subtract),
                Rule::Divide        => ParseExpressionInfixOperator::InfixArithmetic(Arithmetic::Divide),
                Rule::Modulo        => ParseExpressionInfixOperator::InfixArithmetic(Arithmetic::Modulo),
                Rule::Multiply      => ParseExpressionInfixOperator::InfixArithmetic(Arithmetic::Multiply),
                Rule::Add        => ParseExpressionInfixOperator::InfixArithmetic(Arithmetic::Add),
                Rule::ConcatInfixOp => ParseExpressionInfixOperator::Concat,
                rule           => unreachable!("Expr::parse expected infix operation, found {:?}", rule),
            };

            // HACK: InterimBetween(e1, e2) AND e3 => FinalBetween(e1, e2, e3).
            if matches!(op, ParseExpressionInfixOperator::InfixBool(Bool::And)) {
                if let Some((expr, is_exact_match)) = find_interim_between(&mut lhs) {
                    let ParseExpression::InterimBetween { is_not, left, right } = expr else {
                        panic!("expected ParseExpression::InterimBetween");
                    };

                    let fb = ParseExpression::FinalBetween {
                        is_not: *is_not,
                        left: left.clone(),
                        center: right.clone(),
                        right: Box::new(rhs),
                    };

                    if is_exact_match {
                        return Ok(fb);
                    }
                    *expr = fb;
                    return Ok(lhs);
                }
            }
            if matches!(op, ParseExpressionInfixOperator::Escape) {
                return connect_escape_to_like_node(lhs, rhs)
            }

            Ok(ParseExpression::Infix {
                op,
                is_not,
                left: Box::new(lhs),
                right: Box::new(rhs),
            })
        })
        .map_prefix(|op, child| {
            let op = match op.as_rule() {
                Rule::UnaryNot => Unary::Not,
                rule => unreachable!("Expr::parse expected prefix operator, found {:?}", rule),
            };
            Ok(ParseExpression::Prefix { op, child: Box::new(child?)})
        })
        .map_postfix(|child, op| {
            let mut worker = worker.borrow_mut();
            let worker = &mut **worker;
            let mut plan = plan.borrow_mut();
            let plan = &mut **plan;

            let child = child?;
            match op.as_rule() {
                Rule::IndexPostfix => {
                    let expr_pair = op.into_inner().next()
                        .expect("Expected Expr under IndexPostfix.");
                    let which = parse_expr_pratt(
                        expr_pair.into_inner(),
                        param_types,
                        referred_relation_ids,
                        worker,
                        plan,
                        true
                    )?;
                    // Fold consecutive subscripts into a single index-chain node.
                    match child {
                        ParseExpression::Index { child, mut indexes } => {
                            indexes.push(which);
                            Ok(ParseExpression::Index { child, indexes })
                        }
                        other => Ok(ParseExpression::Index {
                            child: Box::new(other),
                            indexes: vec![which],
                        }),
                    }
                }
                Rule::CastPostfix => {
                    let ty_pair = op.into_inner().next()
                        .expect("Expected CastTarget under CastPostfix.");
                    let cast_type = cast_type_from_pair(ty_pair)?;
                    Ok(ParseExpression::Cast { child: Box::new(child), cast_type })
                }
                Rule::IsPostfix => {
                    let mut inner = op.into_inner();
                    let (is_not, value_index) = match inner.len() {
                        2 => (true, 1),
                        1 => (false, 0),
                        _ => unreachable!("Is must have 1 or 2 children")
                    };
                    let value_rule = inner
                        .nth(value_index)
                        .expect("Value must be present under Is")
                        .as_rule();
                    let value = match value_rule {
                        Rule::True => Some(true),
                        Rule::False => Some(false),
                        Rule::Unknown | Rule::Null => None,
                        _ => unreachable!("Is value must be TRUE, FALSE, NULL or UNKNOWN")
                    };
                    Ok(ParseExpression::Is { is_not, child: Box::new(child), value })
                }
                rule => unreachable!("Expr::parse expected postfix operator, found {:?}", rule),
            }
        })
        .parse(expression_pairs);

    res
}

pub(in crate::frontend::sql) fn parse_expr_no_type_check<M>(
    expression_pairs: Pairs<Rule>,
    param_types: &[DerivedType],
    referred_relation_ids: &[NodeId],
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
    safe_for_volatile_function: bool,
) -> Result<NodeId, SbroadError>
where
    M: Metadata,
{
    let parse_expr = parse_expr_pratt(
        expression_pairs,
        param_types,
        referred_relation_ids,
        worker,
        plan,
        safe_for_volatile_function,
    )?;
    parse_expr.populate_plan(plan, worker)
}

/// Parse expression pair and get plan id.
/// * Retrieve expressions tree-like structure using `parse_expr_pratt`
/// * Traverse tree to populate plan with new nodes
/// * Return `plan_id` of root Expression node
pub(in crate::frontend::sql) fn parse_scalar_expr<M>(
    expression_pairs: Pairs<Rule>,
    type_analyzer: &mut TypeAnalyzer,
    desired_type: DerivedType,
    referred_relation_ids: &[NodeId],
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
    safe_for_volatile_function: bool,
) -> Result<NodeId, SbroadError>
where
    M: Metadata,
{
    let param_types = get_parameter_derived_types(type_analyzer);
    let expr_id = parse_expr_no_type_check(
        expression_pairs,
        &param_types,
        referred_relation_ids,
        worker,
        plan,
        safe_for_volatile_function,
    )?;

    type_system::analyze_and_coerce_scalar_expr(
        type_analyzer,
        expr_id,
        desired_type,
        plan,
        &worker.subquery_replaces,
    )?;

    Ok(expr_id)
}
