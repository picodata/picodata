use smol_str::{format_smolstr, ToSmolStr};

use crate::{
    errors::{Entity, SbroadError, TypeError},
    executor::vtable::calculate_unified_types,
    ir::{
        node::Parameter,
        relation::{DerivedType, Type},
        Plan,
    },
};

use super::{
    Alias, ArithmeticExpr, Case, Cast, Constant, ExprInParentheses, Expression, MutExpression,
    Node, NodeId, Reference, Row, StableFunction,
};

impl Plan {
    fn get_node_type(&self, node_id: NodeId) -> Result<DerivedType, SbroadError> {
        match self.get_node(node_id)? {
            Node::Expression(expr) => expr.calculate_type(self),
            Node::Relational(relational) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!(
                    "relational node {relational:?} has no type"
                )),
            )),
            Node::Ddl(ddl) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("DDL node {ddl:?} has no type")),
            )),
            Node::Acl(acl) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("ACL node {acl:?} has no type")),
            )),
            Node::Tcl(tcl) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("TCL node {tcl:?} has no type")),
            )),
            Node::Invalid(invalid) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("Invalid node {invalid:?} has no type")),
            )),
            Node::Plugin(plugin) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("Plugin node {plugin:?} has no type")),
            )),
            Node::Block(block) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("Block node {block:?} has no type")),
            )),
            Node::Deallocate(_) => Err(SbroadError::Invalid(
                Entity::Node,
                Some("Deallocate node has no type".to_smolstr()),
            )),
        }
    }
}

impl Expression<'_> {
    /// Calculate the type of the expression.
    pub fn calculate_type(&self, plan: &Plan) -> Result<DerivedType, SbroadError> {
        let ty = match self {
            Expression::Window(_) | Expression::Over(_) => {
                // TODO: Fix this later.
                DerivedType::new(Type::Any)
            }
            Expression::Case(Case {
                when_blocks,
                else_expr,
                ..
            }) => {
                // Outer option -- for uninitialized type.
                // Inner option -- for the case of Null met.
                let mut case_ty_general: Option<DerivedType> = None;
                let mut check_types_corresponds = |another_ty: &DerivedType| {
                    if let Some(case_ty) = case_ty_general {
                        match (case_ty.get(), another_ty.get()) {
                            (Some(case_ty), Some(another_ty)) => {
                                if *case_ty != *another_ty {
                                    return Err(SbroadError::TypeError(TypeError::TypeMismatch(
                                        *case_ty,
                                        *another_ty,
                                    )));
                                }
                            }
                            (None, Some(_)) => case_ty_general = Some(*another_ty),
                            (_, _) => {}
                        }
                    } else {
                        case_ty_general = Some(*another_ty)
                    }
                    Ok(())
                };

                for (_, ret_expr) in when_blocks {
                    let ret_expr_type = plan.get_node_type(*ret_expr)?;
                    check_types_corresponds(&ret_expr_type)?
                }
                if let Some(else_expr) = else_expr {
                    let else_expr_type = plan.get_node_type(*else_expr)?;
                    check_types_corresponds(&else_expr_type)?
                }
                case_ty_general.expect("Case type must be known")
            }
            Expression::Alias(Alias { child, .. })
            | Expression::ExprInParentheses(ExprInParentheses { child }) => {
                plan.get_node_type(*child)?
            }
            Expression::Bool(_) | Expression::Unary(_) | Expression::Like { .. } => {
                DerivedType::new(Type::Boolean)
            }
            Expression::Arithmetic(ArithmeticExpr {
                left, right, op, ..
            }) => {
                let left_type = plan.get_node_type(*left)?;
                let right_type = plan.get_node_type(*right)?;

                let (left_type, right_type) = match (left_type.get(), right_type.get()) {
                    (Some(l_t), Some(r_t)) => (l_t, r_t),
                    _ => return Ok(DerivedType::unknown()),
                };

                let res = match (left_type, right_type) {
                    (Type::Double, Type::Double | Type::Unsigned | Type::Integer | Type::Decimal)
                    | (Type::Unsigned | Type::Integer | Type::Decimal, Type::Double) => {
                        Type::Double
                    }
                    (Type::Decimal, Type::Decimal | Type::Unsigned | Type::Integer)
                    | (Type::Unsigned | Type::Integer, Type::Decimal) => Type::Decimal,
                    (Type::Integer, Type::Unsigned | Type::Integer)
                    | (Type::Unsigned, Type::Integer) => Type::Integer,
                    (Type::Unsigned, Type::Unsigned) => Type::Unsigned,
                    _ => return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some(format_smolstr!("types {left_type} and {right_type} are not supported for arithmetic expression ({:?} {op:?} {:?})",
                        plan.get_node(*left)?, plan.get_node(*right)?)),
                    )),
                };
                DerivedType::new(res)
            }
            Expression::Cast(Cast { to, .. }) => DerivedType::new(to.as_relation_type()),
            Expression::Trim(_) | Expression::Concat(_) => DerivedType::new(Type::String),
            Expression::Constant(Constant { value, .. }) => value.get_type(),
            Expression::Reference(Reference { col_type, .. }) => *col_type,
            Expression::Row(Row { list, .. }) => {
                if let (Some(expr_id), None) = (list.first(), list.get(1)) {
                    let expr = plan.get_expression_node(*expr_id)?;
                    expr.calculate_type(plan)?
                } else {
                    DerivedType::new(Type::Array)
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
                        expr.calculate_type(plan)?
                    }
                    "coalesce" => {
                        let mut last_ty = DerivedType::unknown();
                        for child_id in children {
                            let child = plan.get_expression_node(*child_id)?;
                            let ty = child.calculate_type(plan)?;
                            if let Some(ty) = ty.get() {
                                if let Some(last_ty) = last_ty.get() {
                                    if ty != last_ty {
                                        return Err(TypeError::TypeMismatch(*last_ty, *ty).into());
                                    }
                                } else {
                                    last_ty.set(*ty)
                                }
                            }
                        }
                        last_ty
                    }
                    _ => *func_type,
                }
            }
            Expression::CountAsterisk(_) => DerivedType::new(Type::Integer),
            Expression::LocalTimestamp(_) => DerivedType::new(Type::Datetime),
            Expression::Parameter(Parameter { param_type }) => *param_type,
        };
        Ok(ty)
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
    /// When `resolve_metadata` is called references have unknown type.
    /// When `bind_params` is called references types are refined.
    pub fn recalculate_ref_type(&self, plan: &Plan) -> Result<DerivedType, SbroadError> {
        let prev_type = self.calculate_type(plan)?;
        let Expression::Reference(Reference {
            parent,
            targets,
            position,
            ..
        }) = self
        else {
            return Ok(prev_type);
        };

        let Some(targets) = targets else {
            // No need to recalculate types for Scan node references.
            return Ok(prev_type);
        };

        let parent_id = parent.ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Expression,
                Some("reference expression has no parent".to_smolstr()),
            )
        })?;
        let parent_rel = plan.get_relation_node(parent_id)?;
        let rel_children: crate::ir::api::children::Children<'_> = parent_rel.children();

        let mut types = Vec::new();
        for target_index in targets {
            let target_rel_id = *rel_children.get(*target_index).unwrap_or_else(|| {
                panic!("reference expression has no target relation at position {target_index}")
            });

            let target_rel = plan.get_relation_node(target_rel_id)?;
            let columns = plan.get_row_list(target_rel.output())?;
            let column_id = *columns.get(*position).unwrap_or_else(|| {
                panic!("reference expression has no target column at position {position}")
            });
            let col_expr = plan.get_expression_node(column_id)?;
            let ty: DerivedType = col_expr.calculate_type(plan)?;
            types.push(vec![ty])
        }

        let unified_res = calculate_unified_types(&types)?;
        let (_, unified_type) = unified_res.first().expect(
            "Vec for reference unified type recalculation should consists of a single column.",
        );
        Ok(*unified_type)
    }
}

impl MutExpression<'_> {
    pub fn set_ref_type(&mut self, new_type: DerivedType) {
        if let MutExpression::Reference(Reference { col_type, .. }) = self {
            *col_type = new_type;
        }
    }
}
