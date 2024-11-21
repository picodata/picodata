use smol_str::{format_smolstr, ToSmolStr};

use crate::{
    errors::{Entity, SbroadError, TypeError},
    ir::{relation::Type, Plan},
};

use super::{
    Alias, ArithmeticExpr, Case, Cast, Constant, ExprInParentheses, Expression, MutExpression,
    Node, NodeId, Reference, Row, StableFunction,
};

impl Plan {
    fn get_node_type(&self, node_id: NodeId) -> Result<Type, SbroadError> {
        match self.get_node(node_id)? {
            Node::Expression(expr) => expr.calculate_type(self),
            Node::Relational(relational) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!(
                    "relational node {relational:?} has no type"
                )),
            )),
            // Parameter nodes must recalculate their type during
            // binding (see `bind_params` function).
            Node::Parameter(ty) => Ok(ty.param_type.unwrap_or(Type::Scalar)),
            Node::Ddl(_) => Err(SbroadError::Invalid(
                Entity::Node,
                Some("DDL node has no type".to_smolstr()),
            )),
            Node::Acl(_) => Err(SbroadError::Invalid(
                Entity::Node,
                Some("ACL node has no type".to_smolstr()),
            )),
            Node::Invalid(_) => Err(SbroadError::Invalid(
                Entity::Node,
                Some("Invalid node has no type".to_smolstr()),
            )),
            Node::Block(_) => Err(SbroadError::Invalid(
                Entity::Node,
                Some("code block node has no type".to_smolstr()),
            )),
            Node::Plugin(_) => Err(SbroadError::Invalid(
                Entity::Node,
                Some("Plugin node has no type".to_smolstr()),
            )),
        }
    }
}

impl Expression<'_> {
    /// Calculate the type of the expression.
    ///
    /// # Errors
    /// - the row list contains non-expression nodes;
    ///
    /// # Panics
    /// - Plan is in inconsistent state
    pub fn calculate_type(&self, plan: &Plan) -> Result<Type, SbroadError> {
        match self {
            Expression::Case(Case {
                when_blocks,
                else_expr,
                ..
            }) => {
                let mut case_type = None;
                let check_types_corresponds = |case_type: &Type, ret_expr_type: &Type| {
                    if case_type != ret_expr_type {
                        return if matches!(ret_expr_type, Type::Array)
                            || matches!(ret_expr_type, Type::Map)
                        {
                            Some(Type::Any)
                        } else {
                            Some(Type::Scalar)
                        };
                    }
                    None
                };

                for (_, ret_expr) in when_blocks {
                    let ret_expr_type = plan.get_node_type(*ret_expr)?;
                    if let Some(case_type) = &case_type {
                        if let Some(ret_type) = check_types_corresponds(case_type, &ret_expr_type) {
                            return Ok(ret_type);
                        }
                    } else {
                        case_type = Some(ret_expr_type);
                    }
                }
                let case_type_unwrapped = case_type.expect("Case WHEN type must be known");
                if let Some(else_expr) = else_expr {
                    let else_expr_type = plan.get_node_type(*else_expr)?;
                    if let Some(ret_type) =
                        check_types_corresponds(&case_type_unwrapped, &else_expr_type)
                    {
                        return Ok(ret_type);
                    }
                }
                Ok(case_type_unwrapped)
            }
            Expression::Alias(Alias { child, .. })
            | Expression::ExprInParentheses(ExprInParentheses { child }) => {
                plan.get_node_type(*child)
            }
            Expression::Bool(_) | Expression::Unary(_) | Expression::Like { .. } => {
                Ok(Type::Boolean)
            }
            Expression::Arithmetic(ArithmeticExpr {
                left, right, op, ..
            }) => {
                let left_type = plan.get_node_type(*left)?;
                let right_type = plan.get_node_type(*right)?;
                match (&left_type, &right_type) {
                    (Type::Double, Type::Double | Type::Unsigned | Type::Integer | Type::Decimal)
                    | (Type::Unsigned | Type::Integer | Type::Decimal, Type::Double) => {
                        Ok(Type::Double)
                    }
                    (Type::Decimal, Type::Decimal | Type::Unsigned | Type::Integer)
                    | (Type::Unsigned | Type::Integer, Type::Decimal) => Ok(Type::Decimal),
                    (Type::Integer, Type::Unsigned | Type::Integer)
                    | (Type::Unsigned, Type::Integer) => Ok(Type::Integer),
                    (Type::Unsigned, Type::Unsigned) => Ok(Type::Unsigned),
                    _ => Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some(format_smolstr!("types {left_type} and {right_type} are not supported for arithmetic expression ({:?} {op:?} {:?})",
                        plan.get_node(*left)?, plan.get_node(*right)?)),
                    )),
                }
            }
            Expression::Cast(Cast { to, .. }) => Ok(to.as_relation_type()),
            Expression::Trim(_) | Expression::Concat(_) => Ok(Type::String),
            Expression::Constant(Constant { value, .. }) => Ok(value.get_type()),
            Expression::Reference(Reference { col_type, .. }) => Ok(*col_type),
            Expression::Row(Row { list, .. }) => {
                if let (Some(expr_id), None) = (list.first(), list.get(1)) {
                    let expr = plan.get_expression_node(*expr_id)?;
                    expr.calculate_type(plan)
                } else {
                    Ok(Type::Array)
                }
            }
            Expression::StableFunction(StableFunction {
                name,
                func_type,
                children,
                ..
            }) => {
                match name.as_str() {
                    "max" | "min" => {
                        // min/max functions have a scalar type, which means that their actual type can be
                        // inferred from the arguments.
                        let expr_id = children
                            .first()
                            .expect("min/max functions must have an argument");
                        let expr = plan.get_expression_node(*expr_id)?;
                        expr.calculate_type(plan)
                    }
                    "coalesce" => {
                        let first_id = children.first().expect("coalesce must have children");
                        let child = plan.get_expression_node(*first_id)?;
                        let ty = child.calculate_type(plan)?;
                        // ensure all the types are the same
                        for child_id in children {
                            let child = plan.get_expression_node(*child_id)?;
                            let child_ty = child.calculate_type(plan)?;
                            if child_ty != ty {
                                return Err(TypeError::TypeMismatch(ty, child_ty).into());
                            }
                        }
                        Ok(ty)
                    }
                    _ => Ok(*func_type),
                }
            }
            Expression::CountAsterisk(_) => Ok(Type::Integer),
        }
    }

    /// Returns the recalculated type of the expression.
    /// At the moment we recalculate only references, because they can change their
    /// type during binding.
    /// E.g. in case of query like
    /// `SELECT "col_1" FROM (
    ///     SELECT * FROM (
    ///         VALUES ((?))
    ///     ))`,
    /// where we can't calculate type of
    /// upper reference, because we don't know what value will be
    /// passed as an argument.
    /// When `resolve_metadata` is called references are typed with `Scalar`.
    /// When `bind_params` is called references types are refined.
    ///
    /// # Errors
    /// - if the reference is invalid;
    pub fn recalculate_type(&self, plan: &Plan) -> Result<Type, SbroadError> {
        if let Expression::Reference(Reference {
            parent,
            targets,
            position,
            ..
        }) = self
        {
            let parent_id = parent.ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Expression,
                    Some("reference expression has no parent".to_smolstr()),
                )
            })?;
            let parent_rel = plan.get_relation_node(parent_id)?;
            // We are interested only in the first target, because:
            // - union all relies on the first child type;
            // - scan has no children (and the space column type can't change anyway);
            if let Some(Some(target)) = targets.as_ref().map(|targets| targets.first()) {
                let target_children = parent_rel.children();
                let target_rel_id = *target_children.get(*target).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Expression,
                        Some(format_smolstr!(
                            "reference expression has no target relation at position {target}"
                        )),
                    )
                })?;
                let target_rel = plan.get_relation_node(target_rel_id)?;
                let columns = plan.get_row_list(target_rel.output())?;
                let column_id = *columns.get(*position).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Expression,
                        Some(format_smolstr!(
                            "reference expression has no target column at position {position}"
                        )),
                    )
                })?;
                let col_expr = plan.get_expression_node(column_id)?;
                return col_expr.calculate_type(plan);
            }
        }
        self.calculate_type(plan)
    }
}

impl MutExpression<'_> {
    pub fn set_ref_type(&mut self, new_type: Type) {
        if let MutExpression::Reference(Reference { col_type, .. }) = self {
            *col_type = new_type;
        }
    }
}
