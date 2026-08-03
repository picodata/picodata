use ahash::{AHashMap, AHashSet};
use smol_str::{format_smolstr, SmolStr};

use crate::errors::{Entity, SbroadError};
use crate::ir::node::expression::Expression;
use crate::ir::node::{Constant, LetVarRef, NodeId, Parameter};
use crate::ir::operator::{
    ConflictDoUpdate, ConflictStrategy, ConflictTarget, ConflictUpdateItem, ConflictUpdateOp,
    ConflictUpdateRhs,
};
use crate::ir::relation::{Column, ColumnRole, Table};
use crate::ir::types::{CastType, DerivedType, UnrestrictedType};
use crate::ir::value::Value;
use crate::ir::Plan;
use crate::utils::to_user;
use sql_type_system::expr::Type as SqlType;

use super::ast::core::{parse_normalized_identifier, AstCore};
use super::ast::ir_populator::{parse_param_with_positions, LetVarScope};
use super::ast::{ParseNode, ParsingPairsMap, Rule};
use super::error::{InsertConflictError as ConflictError, Result, SqlFrontendError};
use super::ir::value_from_node;
use super::type_system;
use pest::iterators::Pair;

fn find_column<'a>(relation: &'a Table, col_name: &SmolStr) -> Result<(&'a Column, usize)> {
    relation
        .columns
        .iter()
        .enumerate()
        .find(|(_, c)| c.name == *col_name)
        .map(|(pos, c)| (c, pos))
        .ok_or_else(|| {
            SqlFrontendError::Frontend(SbroadError::NotFound(Entity::Column, to_user(col_name)))
        })
}

fn parse_update_literal(
    ast: &AstCore,
    node_id: usize,
    ctx: &mut DoUpdateParseContext<'_, '_>,
) -> Result<Value> {
    let node = ast.nodes.get_node(node_id)?;
    if node.rule != Rule::Literal {
        return Err(ConflictError::UnsupportedLiteral {
            kind: format_smolstr!("{:?}", node.rule),
        }
        .into());
    }
    let literal_pair = ctx.pairs_map.remove_pair(node_id);
    let mut inner = literal_pair.into_inner();
    let literal = inner.next().ok_or_else(|| {
        SqlFrontendError::insert_conflict_ast("expected Literal to have 1 child, got 0")
    })?;
    if inner.next().is_some() {
        return Err(SqlFrontendError::insert_conflict_ast(format_smolstr!(
            "expected Literal to have 1 child, got more than 1"
        )));
    }
    value_from_node(&literal).map_err(SqlFrontendError::Frontend)
}

fn parse_unqualified_update_identifier(ast: &AstCore, node_id: usize) -> Result<Option<SmolStr>> {
    let node = ast.nodes.get_node(node_id)?;
    match node.rule {
        Rule::Identifier => Ok(Some(parse_normalized_identifier(ast, node_id)?)),
        Rule::IdentifierWithOptionalContinuation => match node.children.as_slice() {
            [identifier_id] => Ok(Some(parse_normalized_identifier(ast, *identifier_id)?)),
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

pub(super) struct DoUpdateParseContext<'a, 'q> {
    pub(super) plan: &'a mut Plan,
    pub(super) let_scope: &'a mut LetVarScope,
    pub(super) type_analyzer: &'a mut type_system::TypeAnalyzer,
    pub(super) subquery_map: &'a AHashMap<NodeId, NodeId>,
    pub(super) pairs_map: &'a mut ParsingPairsMap<'q>,
    pub(super) tnt_parameters_positions: &'a [Pair<'q, Rule>],
}

struct ParsedUpdateOperand {
    node: NodeId,
    param_index: Option<u16>,
}

fn parse_update_operand(
    ast: &AstCore,
    node_id: usize,
    relation: &Table,
    ctx: &mut DoUpdateParseContext<'_, '_>,
) -> Result<ParsedUpdateOperand> {
    let node = ast.nodes.get_node(node_id)?;
    if node.rule == Rule::Parameter {
        let param_types = type_system::get_parameter_derived_types(ctx.type_analyzer);
        let node = parse_param_with_positions(
            ctx.pairs_map.remove_pair(node_id),
            &param_types,
            ctx.tnt_parameters_positions,
            ctx.plan,
        )?;
        let param_index = match ctx.plan.get_expression_node(node)? {
            Expression::Parameter(Parameter { index, .. }) => *index,
            other => {
                return Err(SqlFrontendError::Frontend(SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!("expected parameter node, got {other:?}")),
                )));
            }
        };
        return Ok(ParsedUpdateOperand {
            node,
            param_index: Some(param_index),
        });
    }
    if let Some(name) = parse_unqualified_update_identifier(ast, node_id)? {
        let Some(let_decl) = ctx.let_scope.lookup(&name) else {
            return Err(ConflictError::UnsupportedLiteral {
                kind: format_smolstr!("{:?}", node.rule),
            }
            .into());
        };
        let var_type = let_decl.ty;
        if find_column(relation, &name).is_ok() {
            return Err(ConflictError::LetColumnAmbiguous { name }.into());
        }
        ctx.let_scope.mark_used(&name);
        let node = ctx.plan.nodes.push(LetVarRef { name, var_type }.into());
        return Ok(ParsedUpdateOperand {
            node,
            param_index: None,
        });
    }
    if matches!(
        node.rule,
        Rule::Identifier | Rule::DoUpdateColumnRef | Rule::IdentifierWithOptionalContinuation
    ) {
        return Err(ConflictError::UnsupportedLiteral {
            kind: format_smolstr!("{:?}", node.rule),
        }
        .into());
    }

    let value = parse_update_literal(ast, node_id, ctx)?;
    Ok(ParsedUpdateOperand {
        node: ctx.plan.nodes.push(Constant { value }.into()),
        param_index: None,
    })
}

fn type_name(ty: UnrestrictedType) -> SmolStr {
    format_smolstr!("{}", SqlType::from(ty))
}

fn validate_update_rhs(
    column_name: &SmolStr,
    column_type: UnrestrictedType,
    op: ConflictUpdateOp,
    rhs: NodeId,
    ctx: &mut DoUpdateParseContext<'_, '_>,
) -> Result<NodeId> {
    // TypeAnalyzer identifies expressions by NodeId and caches reports for them.
    // The IOCDU check below builds a temporary type-only expression, so allocate
    // fresh unreachable nodes only to give that expression stable unique ids.
    let update_expr_id = ctx.plan.nodes.push(Constant { value: Value::Null }.into());
    let column_expr_id = ctx.plan.nodes.push(Constant { value: Value::Null }.into());

    // Model the update as `column op rhs`: the column side is represented only
    // by its type, while rhs comes from the real plan.
    let column_expr = type_system::TypeExpr::new(
        column_expr_id,
        type_system::TypeExprKind::Reference(SqlType::from(column_type)),
    );
    let rhs_expr = type_system::to_type_expr(rhs, ctx.plan, ctx.subquery_map)?;
    let expr = type_system::TypeExpr::new(
        update_expr_id,
        type_system::TypeExprKind::Operator(
            op.as_binary_operator().into(),
            vec![column_expr, rhs_expr],
        ),
    );
    ctx.type_analyzer
        .analyze(&expr, Some(SqlType::from(column_type)))
        .map_err(SbroadError::from)?;

    // The desired type is only a hint for TypeAnalyzer, so verify that the whole
    // update expression really produces the target column type.
    let report = ctx.type_analyzer.get_report();
    let update_type = DerivedType::from(report.get_type(&update_expr_id))
        .get()
        .expect("type analyzer must report known IOCDU expression type");
    if update_type != column_type {
        return Err(ConflictError::ValueTypeMismatch {
            name: to_user(column_name),
            column_type: type_name(column_type),
            value_type: type_name(update_type),
        }
        .into());
    }

    type_system::analyze_and_coerce_scalar_expr(
        ctx.type_analyzer,
        rhs,
        DerivedType::new(column_type),
        ctx.plan,
        ctx.subquery_map,
    )?;

    let report = ctx.type_analyzer.get_report();
    let Some(cast_type) = report.get_cast(&rhs) else {
        return Ok(rhs);
    };
    let cast_type = DerivedType::from(cast_type)
        .get()
        .expect("type analyzer must report known IOC DU RHS cast type");

    let target_type = CastType::try_from(&cast_type)?;
    Ok(ctx.plan.add_cast(rhs, target_type)?)
}

/// Returns whether `node_id` is an identifier naming the conflict target column.
fn is_target_column(
    ast: &AstCore,
    node_id: usize,
    relation: &Table,
    target_column_name: &SmolStr,
    ctx: &DoUpdateParseContext<'_, '_>,
) -> Result<bool> {
    if let Some(name) = parse_unqualified_update_identifier(ast, node_id)? {
        if &name != target_column_name {
            return Ok(false);
        }
        if ctx.let_scope.lookup(&name).is_some() {
            return Err(ConflictError::LetColumnAmbiguous { name }.into());
        }
        return Ok(true);
    }

    let node = ast.nodes.get_node(node_id)?;
    match node.rule {
        Rule::DoUpdateColumnRef => {
            let table_name = parse_normalized_identifier(ast, node.child_n(0))?;
            let column_name = parse_normalized_identifier(ast, node.child_n(1))?;
            Ok(table_name == relation.name && column_name == *target_column_name)
        }
        Rule::IdentifierWithOptionalContinuation => match node.children.as_slice() {
            [table_id, continuation_id] => {
                let continuation = ast.nodes.get_node(*continuation_id)?;
                if continuation.rule != Rule::ReferenceContinuation {
                    return Ok(false);
                }
                let [column_id] = continuation.children.as_slice() else {
                    return Err(SqlFrontendError::insert_conflict_ast(format_smolstr!(
                        "expected ReferenceContinuation to have 1 child, got {}",
                        continuation.children.len()
                    )));
                };
                let table_name = parse_normalized_identifier(ast, *table_id)?;
                let column_name = parse_normalized_identifier(ast, *column_id)?;
                Ok(table_name == relation.name && column_name == *target_column_name)
            }
            _ => Ok(false),
        },
        _ => Ok(false),
    }
}

/// Parses a restricted binary expression `col op value` or `value op col`.
///
/// Exactly one operand must be the conflict target column; the other is the
/// update value. Subtraction is only allowed as `col - value`.
fn parse_conflict_update_expr(
    ast: &AstCore,
    node_id: usize,
    target_column_name: &SmolStr,
    relation: &Table,
    ctx: &mut DoUpdateParseContext<'_, '_>,
) -> Result<(ConflictUpdateOp, ParsedUpdateOperand)> {
    let node = ast.nodes.get_node(node_id)?;
    if node.rule != Rule::DoUpdateExpr {
        return Err(SqlFrontendError::insert_conflict_ast(format_smolstr!(
            "expected DoUpdateExpr, got {:?}",
            node.rule
        )));
    }
    let invalid_self_reference = || {
        ConflictError::InvalidSelfReference {
            name: to_user(target_column_name),
        }
        .into()
    };

    let [expr_id] = node.children.as_slice() else {
        return Err(invalid_self_reference());
    };
    let expr = ast.nodes.get_node(*expr_id)?;
    if expr.rule != Rule::Expr {
        return Err(SqlFrontendError::insert_conflict_ast(format_smolstr!(
            "expected Expr under DoUpdateExpr, got {:?}",
            expr.rule
        )));
    }
    let [lhs_id, op_id, rhs_id] = expr.children.as_slice() else {
        return Err(invalid_self_reference());
    };
    let (lhs_id, op_id, rhs_id) = (*lhs_id, *op_id, *rhs_id);

    let (value_id, col_on_left) =
        if is_target_column(ast, lhs_id, relation, target_column_name, ctx)? {
            (rhs_id, true)
        } else if is_target_column(ast, rhs_id, relation, target_column_name, ctx)? {
            (lhs_id, false)
        } else {
            return Err(invalid_self_reference());
        };

    let op = match ast.nodes.get_node(op_id)?.rule {
        Rule::Add => ConflictUpdateOp::AddAssign,
        Rule::And => ConflictUpdateOp::AndAssign,
        Rule::Or => ConflictUpdateOp::OrAssign,
        Rule::Subtract if col_on_left => ConflictUpdateOp::SubAssign,
        // `value - col` is not commutative and therefore unsupported.
        Rule::Subtract => return Err(invalid_self_reference()),
        _ => return Err(invalid_self_reference()),
    };

    Ok((op, parse_update_operand(ast, value_id, relation, ctx)?))
}

fn parse_do_update_items(
    ast: &AstCore,
    action_child_id: usize,
    target: ConflictTarget,
    relation: &Table,
    ctx: &mut DoUpdateParseContext<'_, '_>,
) -> Result<ConflictStrategy> {
    let child = ast.nodes.get_node(action_child_id)?;
    let update_list_id = child.children.first().ok_or_else(|| {
        SqlFrontendError::insert_conflict_ast("expected update list under DoUpdate")
    })?;
    let update_list = ast.nodes.get_node(*update_list_id)?;
    if update_list.rule != Rule::DoUpdateList {
        return Err(SqlFrontendError::insert_conflict_ast(format_smolstr!(
            "expected DoUpdateList under DoUpdate, got {:?}",
            update_list.rule
        )));
    }

    let mut updates: Vec<ConflictUpdateItem> = Vec::with_capacity(update_list.children.len());
    let mut updated_columns: AHashSet<usize> = AHashSet::with_capacity(update_list.children.len());
    let distribution_columns: AHashSet<usize> = if relation.is_global() {
        AHashSet::new()
    } else {
        relation.get_sk()?.iter().copied().collect()
    };

    for update_item_id in &update_list.children {
        let update_item = ast.nodes.get_node(*update_item_id)?;
        if update_item.rule != Rule::DoUpdateItem {
            return Err(SqlFrontendError::insert_conflict_ast(format_smolstr!(
                "expected DoUpdateItem under DoUpdateList, got {:?}",
                update_item.rule
            )));
        }

        let ast_column_id = update_item.children.first().ok_or_else(|| {
            SqlFrontendError::insert_conflict_ast("column expected as first child of DoUpdateItem")
        })?;
        let ast_expr_id = update_item.children.get(1).ok_or_else(|| {
            SqlFrontendError::insert_conflict_ast(
                "restricted expression expected as second child of DoUpdateItem",
            )
        })?;

        let ast_column = ast.nodes.get_node(*ast_column_id)?;
        if ast_column.rule == Rule::DoUpdateColumnRef {
            return Err(ConflictError::QualifiedUpdateColumn.into());
        }
        if ast_column.rule != Rule::Identifier {
            return Err(SqlFrontendError::insert_conflict_ast(format_smolstr!(
                "expected update column identifier, got {:?}",
                ast_column.rule
            )));
        }
        let col_name = parse_normalized_identifier(ast, *ast_column_id)?;
        let (col_def, col_pos) = find_column(relation, &col_name)?;
        if distribution_columns.contains(&col_pos) {
            return Err(ConflictError::ForbiddenColumnUpdate {
                name: to_user(&col_name),
                column_kind: "distribution",
            }
            .into());
        }
        if relation.primary_key.positions.contains(&col_pos) {
            return Err(ConflictError::ForbiddenColumnUpdate {
                name: to_user(&col_name),
                column_kind: "primary key",
            }
            .into());
        }
        if col_def.get_role() != &ColumnRole::User {
            return Err(ConflictError::ForbiddenColumnUpdate {
                name: to_user(&col_name),
                column_kind: "system",
            }
            .into());
        }
        if !updated_columns.insert(col_pos) {
            return Err(ConflictError::DuplicateColumn {
                name: to_user(&col_name),
                location: "update list",
            }
            .into());
        }
        let column_type = (*col_def.r#type.get()).ok_or_else(|| {
            SqlFrontendError::Frontend(SbroadError::Invalid(
                Entity::Plan,
                Some(format_smolstr!(
                    "column {} type must be known",
                    to_user(&col_name)
                )),
            ))
        })?;
        let (op, rhs) = parse_conflict_update_expr(ast, *ast_expr_id, &col_name, relation, ctx)?;
        let rhs_node = validate_update_rhs(&col_name, column_type, op, rhs.node, ctx)?;

        updates.push(ConflictUpdateItem {
            column: col_pos,
            op,
            rhs: match rhs.param_index {
                Some(source_index) => ConflictUpdateRhs::Param {
                    expr: rhs_node,
                    target_type: column_type,
                    source_index,
                },
                None => ConflictUpdateRhs::Expr(rhs_node),
            },
        });
    }

    Ok(ConflictStrategy::DoUpdate {
        payload: Box::new(ConflictDoUpdate {
            target,
            items: updates,
        }),
    })
}

pub(super) fn parse_insert_conflict_clause(
    node: &ParseNode,
    ast: &AstCore,
    child_idx: usize,
    relation_name: &SmolStr,
    ctx: &mut DoUpdateParseContext<'_, '_>,
) -> Result<ConflictStrategy> {
    let Some(child_id) = node.children.get(child_idx).copied() else {
        return Ok(ConflictStrategy::DoFail);
    };
    let child = ast.nodes.get_node(child_id)?;
    if child.rule != Rule::OnConflict {
        return Err(SqlFrontendError::insert_conflict_ast(format_smolstr!(
            "expected OnConflict, got {:?}",
            child.rule
        )));
    }
    // The clause parser needs the table alongside `&mut Plan`, so it clones it
    // out of the plan — only now that an ON CONFLICT clause actually exists.
    let relation = ctx
        .plan
        .relations
        .get(relation_name)
        .cloned()
        .ok_or_else(|| {
            SqlFrontendError::Frontend(SbroadError::NotFound(
                Entity::Table,
                format_smolstr!("{relation_name} among plan relations"),
            ))
        })?;
    let relation = &relation;

    let (target, action_child_id) = match child.children.as_slice() {
        [action_child_id] => (None, *action_child_id),
        [target_id, action_child_id] => (
            Some(ConflictTarget {
                columns: parse_insert_conflict_target(*target_id, ast, relation)?,
            }),
            *action_child_id,
        ),
        _ => {
            return Err(SqlFrontendError::insert_conflict_ast(format_smolstr!(
                "expected OnConflict to have one or two children, got {}",
                child.children.len()
            )));
        }
    };

    let child = ast.nodes.get_node(action_child_id)?;
    if child.rule == Rule::DoUpdate {
        let Some(target) = target else {
            return Err(ConflictError::TargetRequired.into());
        };
        return parse_do_update_items(ast, action_child_id, target, relation, ctx);
    }
    if target.is_some() {
        return Err(ConflictError::TargetDoUpdateOnly.into());
    }
    match child.rule {
        Rule::DoNothing => Ok(ConflictStrategy::DoNothing),
        Rule::DoReplace => Ok(ConflictStrategy::DoReplace),
        Rule::DoFail => Ok(ConflictStrategy::DoFail),
        rule => Err(SqlFrontendError::insert_conflict_ast(format_smolstr!(
            "expected conflict strategy, got {rule:?}"
        ))),
    }
}

fn parse_insert_conflict_target(
    node_id: usize,
    ast: &AstCore,
    relation: &Table,
) -> Result<Vec<usize>> {
    let node = ast.nodes.get_node(node_id)?;
    if node.rule != Rule::ConflictTarget {
        return Err(SqlFrontendError::insert_conflict_ast(format_smolstr!(
            "expected ConflictTarget, got {:?}",
            node.rule
        )));
    }

    let mut positions = Vec::with_capacity(node.children.len());
    let mut seen = AHashSet::with_capacity(node.children.len());
    for child_id in &node.children {
        let col_name = parse_normalized_identifier(ast, *child_id)?;
        let (_, position) = find_column(relation, &col_name)?;
        if !seen.insert(position) {
            return Err(ConflictError::DuplicateColumn {
                name: to_user(&col_name),
                location: "conflict target",
            }
            .into());
        }
        positions.push(position);
    }

    Ok(positions)
}
