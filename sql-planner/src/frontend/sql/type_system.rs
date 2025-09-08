use crate::errors::SbroadError;
use crate::frontend::sql::get_real_function_name;
use crate::ir::node::expression::{Expression, MutExpression};
use crate::ir::node::relational::Relational;
use crate::ir::node::{
    Alias, ArithmeticExpr, BoolExpr, Bound, BoundType, Case, Cast, Concat, Constant, Frame,
    FrameType, IndexExpr, Like, NodeId, Over, Parameter, Reference, Row, ScalarFunction,
    SubQueryReference, Trim, UnaryExpr, ValuesRow, Window,
};
use crate::ir::operator::{Bool, OrderByElement, OrderByEntity, Unary};
use crate::ir::tree::traversal::{LevelNode, PostOrderWithFilter};
use crate::ir::types::{CastType, DerivedType, UnrestrictedType as SbroadType};
use crate::ir::value::Value;
use crate::ir::Plan;
use ahash::AHashMap;
use smol_str::format_smolstr;
use sql_type_system::error::Error as TypeSystemError;
use sql_type_system::expr::{
    ComparisonOperator, Expr as GenericExpr, ExprKind as GenericExprKind, FrameKind, Type,
    UnaryOperator, WindowFrame as GenericWindowFrame,
};
use sql_type_system::type_system::{Function, TypeAnalyzer as GenericTypeAnalyzer};
use sql_type_system::{TypeReport as GenericTypeReport, TypeSystem};
use std::sync::LazyLock;

#[cfg(test)]
#[cfg(feature = "mock")]
mod tests;

pub type TypeExpr = GenericExpr<NodeId>;
pub type TypeExprKind = GenericExprKind<NodeId>;
pub type WindowFrame = GenericWindowFrame<NodeId>;
pub type TypeAnalyzer = GenericTypeAnalyzer<'static, NodeId>;
pub type TypeReport = GenericTypeReport<NodeId>;

impl From<SbroadType> for Type {
    fn from(value: SbroadType) -> Self {
        match value {
            SbroadType::Integer => Type::Integer,
            SbroadType::Decimal => Type::Numeric,
            SbroadType::Double => Type::Double,
            SbroadType::String => Type::Text,
            SbroadType::Boolean => Type::Boolean,
            SbroadType::Datetime => Type::Datetime,
            SbroadType::Any => Type::Any,
            SbroadType::Uuid => Type::Uuid,
            SbroadType::Array => Type::Array,
            SbroadType::Map => Type::Map,
        }
    }
}

impl From<Type> for DerivedType {
    fn from(value: Type) -> Self {
        match value {
            Type::Integer => DerivedType::new(SbroadType::Integer),
            Type::Double => DerivedType::new(SbroadType::Double),
            Type::Numeric => DerivedType::new(SbroadType::Decimal),
            Type::Text => DerivedType::new(SbroadType::String),
            Type::Boolean => DerivedType::new(SbroadType::Boolean),
            Type::Datetime => DerivedType::new(SbroadType::Datetime),
            Type::Uuid => DerivedType::new(SbroadType::Uuid),
            Type::Array => DerivedType::new(SbroadType::Array),
            Type::Map => DerivedType::new(SbroadType::Map),
            Type::Any => DerivedType::new(SbroadType::Any),
        }
    }
}

pub fn get_parameter_derived_types(analyzer: &TypeAnalyzer) -> Vec<DerivedType> {
    analyzer
        .get_parameter_types()
        .iter()
        .map(|t| match t {
            Some(t) => (*t).into(),
            None => DerivedType::unknown(),
        })
        .collect()
}

impl From<CastType> for Type {
    fn from(value: CastType) -> Self {
        match value {
            CastType::Integer => Type::Integer,
            CastType::Decimal => Type::Numeric,
            CastType::Double => Type::Double,
            CastType::String => Type::Text,
            CastType::Boolean => Type::Boolean,
            CastType::Datetime => Type::Datetime,
            CastType::Uuid => Type::Uuid,
            CastType::Json => Type::Map,
        }
    }
}

impl From<Bool> for ComparisonOperator {
    fn from(op: Bool) -> Self {
        match op {
            Bool::Eq => ComparisonOperator::Eq,
            Bool::In => ComparisonOperator::In,
            Bool::Gt => ComparisonOperator::Gt,
            Bool::GtEq => ComparisonOperator::GtEq,
            Bool::Lt => ComparisonOperator::Lt,
            Bool::LtEq => ComparisonOperator::LtEq,
            Bool::NotEq => ComparisonOperator::NotEq,
            Bool::Or | Bool::Between | Bool::And => panic!("{op} is not a comparison operator"),
        }
    }
}

fn order_by_to_type_expr(
    order_by: &[OrderByElement],
    plan: &Plan,
    subquery_map: &AHashMap<NodeId, NodeId>,
) -> Result<Vec<TypeExpr>, SbroadError> {
    let mut translated = Vec::new();
    for o in order_by {
        if let OrderByEntity::Expression { expr_id } = o.entity {
            translated.push(to_type_expr(expr_id, plan, subquery_map)?);
        } else {
            // There is no need in type checking ORDER BY index
        }
    }
    Ok(translated)
}

fn new_window_frame(
    frame: &Frame,
    plan: &Plan,
    subquery_map: &AHashMap<NodeId, NodeId>,
) -> Result<WindowFrame, SbroadError> {
    let kind = match frame.ty {
        FrameType::Rows => FrameKind::Rows,
        FrameType::Range => FrameKind::Range,
    };

    let push_bound_offset = |ty: &BoundType, res: &mut Vec<TypeExpr>| -> Result<_, _> {
        if let BoundType::PrecedingOffset(id) | BoundType::FollowingOffset(id) = ty {
            res.push(to_type_expr(*id, plan, subquery_map)?);
        }
        Ok::<(), SbroadError>(())
    };

    let mut bound_offsets = Vec::new();
    match &frame.bound {
        Bound::Single(bound_type) => push_bound_offset(bound_type, &mut bound_offsets)?,
        Bound::Between(bound_type1, bound_type2) => {
            push_bound_offset(bound_type1, &mut bound_offsets)?;
            push_bound_offset(bound_type2, &mut bound_offsets)?;
        }
    }

    Ok(WindowFrame {
        kind,
        bound_offsets,
    })
}

fn to_type_expr_many(
    nodes: &[NodeId],
    plan: &Plan,
    subquery_map: &AHashMap<NodeId, NodeId>,
) -> Result<Vec<TypeExpr>, SbroadError> {
    nodes
        .iter()
        .map(|id| to_type_expr(*id, plan, subquery_map))
        .collect()
}

pub fn to_type_expr(
    node_id: NodeId,
    plan: &Plan,
    subquery_map: &AHashMap<NodeId, NodeId>,
) -> Result<TypeExpr, SbroadError> {
    match plan.get_expression_node(node_id)? {
        Expression::Parameter(Parameter { index, .. }) => {
            Ok(TypeExpr::new(node_id, TypeExprKind::Parameter(index - 1)))
        }
        Expression::Constant(value) => {
            let Some(sbroad_type) = *value.value.get_type().get() else {
                return Ok(TypeExpr::new(node_id, TypeExprKind::Null));
            };
            let ty = Type::from(sbroad_type);
            let kind = TypeExprKind::Literal(ty);
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::Reference(Reference { col_type, .. })
        | Expression::SubQueryReference(SubQueryReference { col_type, .. }) => {
            let Some(sbroad_type) = col_type.get() else {
                // Parameterized queries can create references of unknown type. This will be
                // fixed once we support parameter types inference. But until then, we'll treat
                // them as nulls, to avoid annoying errors.
                // Eventually, this should be an error or even a panic.
                return Ok(TypeExpr::new(node_id, TypeExprKind::Null));
            };
            let ty = Type::from(*sbroad_type);
            let kind = TypeExprKind::Reference(ty);
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::Index(IndexExpr { child, which }) => {
            let mut index_ids = vec![which];
            let mut source_id = *child;
            while let Expression::Index(IndexExpr { child, which }) =
                plan.get_expression_node(source_id)?
            {
                source_id = *child;
                index_ids.push(which)
            }

            index_ids.reverse();
            let source = Box::new(to_type_expr(source_id, plan, subquery_map)?);
            let indexes = index_ids
                .into_iter()
                .map(|idx| to_type_expr(*idx, plan, subquery_map))
                .collect::<Result<Vec<_>, SbroadError>>()?;
            let kind = TypeExprKind::IndexChain { source, indexes };
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::Cast(Cast { child, to }) => {
            let to = Type::from(to);
            let child = to_type_expr(*child, plan, subquery_map)?;
            let kind = TypeExprKind::Cast(Box::new(child), to);
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::Concat(Concat { left, right }) => {
            let left = to_type_expr(*left, plan, subquery_map)?;
            let right = to_type_expr(*right, plan, subquery_map)?;
            let kind = TypeExprKind::Operator(String::from("||"), vec![left, right]);
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::Arithmetic(ArithmeticExpr { op, left, right }) => {
            let left = to_type_expr(*left, plan, subquery_map)?;
            let right = to_type_expr(*right, plan, subquery_map)?;
            let kind = TypeExprKind::Operator(op.as_str().into(), vec![left, right]);
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::Bool(BoolExpr { op, left, right }) => {
            // TODO: `a BETWEEN b AND c` is translated into two separate expressions:
            // `a >= b` and `a <= c`. This can lead to inconsistent type coercion for `a` in each
            // expression, which should be avoided to ensure type consistency.
            //
            // In addition, this leads to confusing errors:
            // ```sql
            // picodata> explain select 1 between 0 and 'kek'
            // ---
            // - null
            // - 'sbroad: could not resolve overload for <=(int, text)'
            // ...
            // ```
            let left = to_type_expr(*left, plan, subquery_map)?;
            let right = to_type_expr(*right, plan, subquery_map)?;

            use crate::ir::operator::Bool::*;
            let kind = match op {
                And | Or => TypeExprKind::Operator(op.as_str().into(), vec![left, right]),
                Eq | Gt | GtEq | Lt | LtEq | NotEq | In => {
                    let op = ComparisonOperator::from(*op);
                    TypeExprKind::Comparison(op, left.into(), right.into())
                }
                Between => panic!("there is no between expressions in the plan"),
            };
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::Row(Row { list, .. }) => {
            for (subquery_id, row_id) in subquery_map {
                if *row_id == node_id {
                    let mut types = Vec::with_capacity(list.len());
                    let output_id = plan.get_relation_node(*subquery_id)?.output();
                    let columns = plan.get_row_list(output_id)?;
                    for col_id in columns {
                        let column = plan.get_expression_node(*col_id)?;
                        if let Some(ty) = column.calculate_type(plan)?.get() {
                            types.push(Type::from(*ty));
                        } else {
                            // Strictly speaking, NULL should have unknown type, but the type
                            // system defaults it to text, so we map it to text.
                            types.push(Type::Text);
                        }
                    }
                    let kind = TypeExprKind::Subquery(types);
                    return Ok(TypeExpr::new(node_id, kind));
                }
            }

            let exprs = to_type_expr_many(list, plan, subquery_map)?;
            let kind = TypeExprKind::Row(exprs);
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::ScalarFunction(ScalarFunction {
            name,
            children,
            feature: _,
            ..
        }) => match name.as_str() {
            "coalesce" => {
                let args = to_type_expr_many(children, plan, subquery_map)?;
                let kind = TypeExprKind::Coalesce(args);
                Ok(TypeExpr::new(node_id, kind))
            }
            name => {
                let args = to_type_expr_many(children, plan, subquery_map)?;
                let kind = TypeExprKind::Function(name.to_string(), args);
                Ok(TypeExpr::new(node_id, kind))
            }
        },
        Expression::Over(Over {
            stable_func,
            filter,
            window,
        }) => {
            let Expression::ScalarFunction(ScalarFunction { name, children, .. }) =
                plan.get_expression_node(*stable_func)?
            else {
                panic!("Over should have stable func");
            };
            let args = to_type_expr_many(children, plan, subquery_map)?;
            let filter = filter
                .map(|f| to_type_expr(f, plan, subquery_map))
                .transpose()?;
            let over = to_type_expr(*window, plan, subquery_map)?;
            let kind = TypeExprKind::WindowFunction {
                name: name.to_string(),
                args,
                filter: filter.map(Box::new),
                over: Box::new(over),
            };
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::Window(Window {
            ref partition,
            ref ordering,
            ref frame,
            ..
        }) => {
            let partition = partition.as_deref().unwrap_or(&[]);
            let partition_by = to_type_expr_many(partition, plan, subquery_map)?;
            let ordering = ordering.as_deref().unwrap_or(&[]);
            let order_by = order_by_to_type_expr(ordering, plan, subquery_map)?;
            let frame = frame
                .as_ref()
                .map(|f| new_window_frame(f, plan, subquery_map))
                .transpose()?;
            let kind = TypeExprKind::Window {
                order_by,
                partition_by,
                frame,
            };
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::Case(Case {
            search_expr,
            when_blocks,
            else_expr,
        }) => {
            let mut when_exprs = Vec::with_capacity(when_blocks.len());
            let mut result_exprs = Vec::with_capacity(when_blocks.len());

            if let Some(search) = search_expr {
                let search = to_type_expr(*search, plan, subquery_map)?;
                when_exprs.push(search);
            }

            for (when, result) in when_blocks {
                let when = to_type_expr(*when, plan, subquery_map)?;
                let result = to_type_expr(*result, plan, subquery_map)?;
                when_exprs.push(when);
                result_exprs.push(result);
            }

            if let Some(els) = else_expr {
                let els = to_type_expr(*els, plan, subquery_map)?;
                result_exprs.push(els);
            }

            let kind = TypeExprKind::Case {
                when_exprs,
                result_exprs,
            };
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::Unary(UnaryExpr { op, child }) => {
            let op = match op {
                Unary::Not => UnaryOperator::Not,
                Unary::IsNull => UnaryOperator::IsNull,
                Unary::Exists => UnaryOperator::Exists,
            };
            let child = to_type_expr(*child, plan, subquery_map)?;
            let kind = TypeExprKind::Unary(op, child.into());
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::Like(Like {
            left,
            right,
            escape,
        }) => {
            let args = to_type_expr_many(&[*left, *right, *escape], plan, subquery_map)?;
            let kind = TypeExprKind::Function("like".into(), args);
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::Trim(Trim {
            pattern, target, ..
        }) => {
            let args = if let Some(pattern) = pattern {
                to_type_expr_many(&[*pattern, *target], plan, subquery_map)?
            } else {
                to_type_expr_many(&[*target], plan, subquery_map)?
            };

            let kind = TypeExprKind::Function("trim".into(), args);
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::Timestamp(_) => {
            let kind = TypeExprKind::Literal(Type::Datetime);
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::CountAsterisk(_) => {
            // Handle it as `COUNT(1)`
            let kind = TypeExprKind::Literal(Type::Integer);
            let one = TypeExpr::new(node_id, kind);
            let kind = TypeExprKind::Function(String::from("count"), vec![one]);
            Ok(TypeExpr::new(node_id, kind))
        }
        Expression::Alias(Alias { child, .. }) => to_type_expr(*child, plan, subquery_map),
    }
}

static TYPE_SYSTEM: LazyLock<TypeSystem> = LazyLock::new(default_type_system);

fn default_type_system() -> TypeSystem {
    use sql_type_system::expr::Type::*;

    let functions = vec![
        // Arithmetic operations.
        // - int
        Function::new_operator("+", [Integer, Integer], Integer),
        Function::new_operator("-", [Integer, Integer], Integer),
        Function::new_operator("/", [Integer, Integer], Integer),
        Function::new_operator("*", [Integer, Integer], Integer),
        Function::new_operator("%", [Integer, Integer], Integer),
        // - double
        Function::new_operator("+", [Double, Double], Double),
        Function::new_operator("-", [Double, Double], Double),
        Function::new_operator("/", [Double, Double], Double),
        Function::new_operator("*", [Double, Double], Double),
        // - numeric
        Function::new_operator("+", [Numeric, Numeric], Numeric),
        Function::new_operator("-", [Numeric, Numeric], Numeric),
        Function::new_operator("/", [Numeric, Numeric], Numeric),
        Function::new_operator("*", [Numeric, Numeric], Numeric),
        // Logical operations.
        Function::new_operator("or", [Boolean, Boolean], Boolean),
        Function::new_operator("and", [Boolean, Boolean], Boolean),
        // String operations.
        Function::new_operator("||", [Text, Text], Text),
        // Functions.
        Function::new_scalar(
            // TODO:
            // Deprecated, remove in the future version.
            // Consider using `pico_instance_uuid` instead.
            get_real_function_name("instance_uuid").expect("shouldn't fail"),
            [],
            Text,
        ),
        Function::new_scalar(
            get_real_function_name("pico_config_file_path").expect("shouldn't fail"),
            [Text],
            Text,
        ),
        Function::new_scalar(
            get_real_function_name("pico_instance_dir").expect("shouldn't fail"),
            [Text],
            Text,
        ),
        Function::new_scalar(
            get_real_function_name("pico_instance_name").expect("shouldn't fail"),
            [Text],
            Text,
        ),
        Function::new_scalar(
            get_real_function_name("pico_instance_uuid").expect("shouldn't fail"),
            [],
            Text,
        ),
        Function::new_scalar(
            get_real_function_name("pico_raft_leader_id").expect("shouldn't fail"),
            [],
            Integer,
        ),
        Function::new_scalar(
            get_real_function_name("pico_raft_leader_uuid").expect("shouldn't fail"),
            [],
            Uuid,
        ),
        Function::new_scalar(
            get_real_function_name("pico_replicaset_name").expect("shouldn't fail"),
            [Text],
            Text,
        ),
        Function::new_scalar(
            get_real_function_name("pico_tier_name").expect("shouldn't fail"),
            [Text],
            Text,
        ),
        Function::new_scalar(
            get_real_function_name("version").expect("shouldn't fail"),
            [],
            Text,
        ),
        Function::new_scalar("like", [Text, Text, Text], Boolean),
        Function::new_scalar("trim", [Text], Text),
        Function::new_scalar("trim", [Text, Text], Text),
        Function::new_scalar("to_date", [Text, Text], Datetime),
        Function::new_scalar("to_char", [Datetime, Text], Text),
        Function::new_scalar("substr", [Text, Integer], Text),
        Function::new_scalar("substr", [Text, Integer, Integer], Text),
        Function::new_scalar("lower", [Text], Text),
        Function::new_scalar("upper", [Text], Text),
        Function::new_scalar("abs", [Numeric], Numeric),
        Function::new_scalar("abs", [Integer], Integer),
        Function::new_scalar("abs", [Double], Double),
        Function::new_scalar("substring", [Text, Integer], Text),
        Function::new_scalar("substring", [Text, Integer, Integer], Text),
        Function::new_scalar("substring", [Text, Text], Text),
        Function::new_scalar("substring", [Text, Text, Text], Text),
        // Aggregates.
        // - count
        // TODO: consider adding `any` type
        Function::new_aggregate("count", [Integer], Integer),
        Function::new_aggregate("count", [Double], Integer),
        Function::new_aggregate("count", [Numeric], Integer),
        Function::new_aggregate("count", [Text], Integer),
        Function::new_aggregate("count", [Boolean], Integer),
        Function::new_aggregate("count", [Datetime], Integer),
        // - max
        Function::new_aggregate("max", [Integer], Integer),
        Function::new_aggregate("max", [Double], Double),
        Function::new_aggregate("max", [Numeric], Numeric),
        Function::new_aggregate("max", [Text], Text),
        Function::new_aggregate("max", [Boolean], Boolean),
        Function::new_aggregate("max", [Datetime], Datetime),
        // - min
        Function::new_aggregate("min", [Integer], Integer),
        Function::new_aggregate("min", [Double], Double),
        Function::new_aggregate("min", [Numeric], Numeric),
        Function::new_aggregate("min", [Text], Text),
        Function::new_aggregate("min", [Boolean], Boolean),
        Function::new_aggregate("min", [Datetime], Datetime),
        // - sum
        Function::new_aggregate("sum", [Integer], Numeric),
        Function::new_aggregate("sum", [Double], Numeric),
        Function::new_aggregate("sum", [Numeric], Numeric),
        // - total
        Function::new_aggregate("total", [Integer], Double),
        Function::new_aggregate("total", [Double], Double),
        Function::new_aggregate("total", [Numeric], Double),
        // - avg
        Function::new_aggregate("avg", [Integer], Numeric),
        Function::new_aggregate("avg", [Double], Numeric),
        Function::new_aggregate("avg", [Numeric], Numeric),
        // - string_agg & group_concat
        Function::new_aggregate("string_agg", [Text], Text),
        Function::new_aggregate("string_agg", [Text, Text], Text),
        Function::new_aggregate("group_concat", [Text], Text),
        Function::new_aggregate("group_concat", [Text, Text], Text),
        // Windows.
        // - count
        // TODO: consider adding `any` type
        Function::new_window("count", [], Integer),
        Function::new_window("count", [Integer], Integer),
        Function::new_window("count", [Double], Integer),
        Function::new_window("count", [Numeric], Integer),
        Function::new_window("count", [Text], Integer),
        Function::new_window("count", [Boolean], Integer),
        Function::new_window("count", [Datetime], Integer),
        // - max
        Function::new_window("max", [Integer], Integer),
        Function::new_window("max", [Double], Double),
        Function::new_window("max", [Numeric], Numeric),
        Function::new_window("max", [Text], Text),
        Function::new_window("max", [Boolean], Boolean),
        Function::new_window("max", [Datetime], Datetime),
        // - min
        Function::new_window("min", [Integer], Integer),
        Function::new_window("min", [Double], Double),
        Function::new_window("min", [Numeric], Numeric),
        Function::new_window("min", [Text], Text),
        Function::new_window("min", [Boolean], Boolean),
        Function::new_window("min", [Datetime], Datetime),
        // - sum
        Function::new_window("sum", [Integer], Numeric),
        Function::new_window("sum", [Double], Numeric),
        Function::new_window("sum", [Numeric], Numeric),
        // - total
        Function::new_window("total", [Integer], Double),
        Function::new_window("total", [Double], Double),
        Function::new_window("total", [Numeric], Double),
        // - avg
        Function::new_window("avg", [Integer], Numeric),
        Function::new_window("avg", [Double], Numeric),
        Function::new_window("avg", [Numeric], Numeric),
        // - string_agg & group_concat
        Function::new_window("string_agg", [Text], Text),
        Function::new_window("string_agg", [Text, Text], Text),
        Function::new_window("group_concat", [Text], Text),
        Function::new_window("group_concat", [Text, Text], Text),
        // - row_number
        Function::new_window("row_number", [], Integer),
        // - last_value
        Function::new_window("last_value", [Integer], Integer),
        Function::new_window("last_value", [Double], Double),
        Function::new_window("last_value", [Numeric], Numeric),
        Function::new_window("last_value", [Text], Text),
        Function::new_window("last_value", [Boolean], Boolean),
        Function::new_window("last_value", [Datetime], Datetime),
    ];

    TypeSystem::new(functions)
}

pub fn new_analyzer(param_types: &[DerivedType]) -> TypeAnalyzer {
    let param_types = param_types
        .iter()
        .map(|t| t.get().map(|t| t.into()))
        .collect();
    TypeAnalyzer::new(&TYPE_SYSTEM).with_parameters(param_types)
}

/// Analyze expression types and apply type coercions.
pub fn analyze_and_coerce_scalar_expr(
    type_analyzer: &mut TypeAnalyzer,
    expr_id: NodeId,
    desired_type: DerivedType,
    plan: &mut Plan,
    subquery_map: &AHashMap<NodeId, NodeId>,
) -> Result<(), SbroadError> {
    analyze_scalar_expr(type_analyzer, expr_id, desired_type, plan, subquery_map)?;
    coerce_scalar_expr(type_analyzer.get_report(), expr_id, plan)
}

/// Coerce expression types in accordance with the type report.
fn coerce_scalar_expr(
    report: &TypeReport,
    expr_id: NodeId,
    plan: &mut Plan,
) -> Result<(), SbroadError> {
    // At the moment we only coerce string literals.
    fn collect_strings_to_be_coerced(
        expr_id: NodeId,
        plan: &Plan,
        report: &TypeReport,
    ) -> Vec<NodeId> {
        // Explicit casts can be the only way to fix coercion errors,
        // so we shouldn't coerce casted values.
        let mut casted_strings = Vec::new();
        let string_to_be_coerced_filter = |id| {
            let Ok(expr) = plan.get_expression_node(id) else {
                return false;
            };

            match expr {
                Expression::Cast(Cast { child, .. }) if report.get_cast(child).is_some() => {
                    if let Expression::Constant(Constant {
                        value: Value::String(_),
                    }) = plan.get_expression_node(*child).unwrap()
                    {
                        // Collect explicitly casted values.
                        casted_strings.push(*child)
                    }
                    false
                }
                Expression::Constant(Constant {
                    value: Value::String(_),
                }) => report.get_cast(&id).is_some(),
                _ => false,
            }
        };

        let mut post_order = PostOrderWithFilter::with_capacity(
            |node| plan.subtree_iter(node, false),
            0,
            Box::new(string_to_be_coerced_filter),
        );

        post_order.populate_nodes(expr_id);
        let strings = post_order.take_nodes();
        drop(post_order);

        // Filter strings literals that require coercion and aren't casted explicitly.
        strings
            .into_iter()
            .map(|LevelNode(_, id)| id)
            .filter(|id| !casted_strings.contains(id))
            .collect()
    }

    let cannot_parse_error = |str_value: &Value, ty| {
        SbroadError::Other(format_smolstr!(
            "failed to parse \'{}\' as a value of type {}, consider using explicit type casts",
            match str_value {
                Value::String(ref s) => s,
                _ => unreachable!(),
            },
            ty
        ))
    };

    let strings = collect_strings_to_be_coerced(expr_id, plan, report);
    for id in strings {
        if let MutExpression::Constant(Constant { value }) = plan.get_mut_expression_node(id)? {
            let report_type = report.get_cast(&id).expect("Some according to filter");
            let new_type = DerivedType::from(report_type)
                .get()
                .expect("type must be known");
            *value = value
                .clone()
                .cast(new_type)
                .map_err(|_| cannot_parse_error(value, report_type))?;
        }
    }

    Ok(())
}

/// Perform type analysis for a scalar expressions.
/// A scalar expressions is represented as a singe value.
/// Examples are: literal, reference, subquery and etc.
/// Rows are not scalar expressions and when they are used as a standalone value analysis fails
/// with "row value misused" error.
fn analyze_scalar_expr(
    type_analyzer: &mut TypeAnalyzer,
    expr_id: NodeId,
    desired_type: DerivedType,
    plan: &Plan,
    subquery_map: &AHashMap<NodeId, NodeId>,
) -> Result<(), SbroadError> {
    let desired_type = desired_type.get().map(Type::from);
    let type_expr = to_type_expr(expr_id, plan, subquery_map)?;
    type_analyzer.analyze(&type_expr, desired_type)?;
    Ok(())
}

fn analyze_homogeneous_rows(
    type_analyzer: &mut TypeAnalyzer,
    ctx: &'static str,
    rows: &[Vec<TypeExpr>],
    desired_types: &[SbroadType],
) -> Result<(), SbroadError> {
    let ncolumns = rows.first().map(|r| r.len()).unwrap_or(0);
    for row in rows {
        if row.len() != ncolumns {
            return Err(TypeSystemError::ListsMustAllBeTheSameLentgh(ctx).into());
        }
    }

    if !desired_types.is_empty() && desired_types.len() != ncolumns {
        return Err(TypeSystemError::DesiredTypesCannotBeMatchedWithExprs(
            desired_types.len(),
            ncolumns,
        )
        .into());
    }

    // TODO: Avoid temporary allocation by creating a columns iterator that yields ith
    // values from every row. `std::iter::from_fn` seems to be handy for that.
    let mut columns = vec![Vec::with_capacity(rows.len()); ncolumns];
    for row in rows {
        for (idx, value) in row.iter().enumerate() {
            columns[idx].push(value);
        }
    }

    for (idx, column) in columns.iter().enumerate() {
        let desired_type = desired_types.get(idx).cloned().map(Into::into);
        type_analyzer.analyze_homogeneous_exprs(ctx, column, desired_type)?;
    }

    Ok(())
}

/// Perform type analysis for values rows expressions.
/// Rows are expected to have homogeneous types and the same length.
pub fn analyze_values_rows(
    type_analyzer: &mut TypeAnalyzer,
    rows: &[NodeId],
    desired_types: &[SbroadType],
    plan: &mut Plan,
    subquery_map: &AHashMap<NodeId, NodeId>,
) -> Result<(), SbroadError> {
    let mut type_rows = Vec::with_capacity(rows.len());
    for row_id in rows {
        if let Relational::ValuesRow(ValuesRow { data, .. }) = plan.get_relation_node(*row_id)? {
            if let Expression::Row(Row { list, .. }) = plan.get_expression_node(*data)? {
                let row = to_type_expr_many(list, plan, subquery_map)?;
                type_rows.push(row);
            }
        }
    }

    analyze_homogeneous_rows(type_analyzer, "VALUES", &type_rows, desired_types)?;

    let report = type_analyzer.get_report();
    let mut new_list = Vec::new();
    for row_id in rows {
        let data = if let Relational::ValuesRow(ValuesRow { data, .. }) =
            plan.get_relation_node(*row_id)?
        {
            *data
        } else {
            unreachable!();
        };

        if let Expression::Row(Row { list, .. }) = plan.get_expression_node(data)? {
            // Here we clone again...
            new_list.clone_from(list);
        }

        for child in &new_list {
            coerce_scalar_expr(report, *child, plan)?;
        }

        if let MutExpression::Row(Row { list, .. }) = plan.get_mut_expression_node(data)? {
            // Here we clone again...
            new_list.clone_into(list);
        }
    }

    Ok(())
}
