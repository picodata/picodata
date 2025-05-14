use std::collections::HashMap;
use std::fmt::{Display, Formatter, Write as _};

use itertools::Itertools;
use serde::Serialize;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};

use crate::errors::{Entity, SbroadError};
use crate::executor::bucket::Buckets;
use crate::executor::engine::helpers::to_user;
use crate::executor::engine::Router;
use crate::executor::Query;
use crate::ir::explain::execution_info::BucketsInfo;
use crate::ir::expression::cast::Type as CastType;
use crate::ir::expression::TrimKind;
use crate::ir::node::{
    Alias, ArithmeticExpr, BoolExpr, Case, Cast, Constant, Delete, Having, Insert, Join,
    Motion as MotionRel, NodeId, Reference, Row as RowExpr, ScalarFunction, ScanCte, ScanRelation,
    ScanSubQuery, Selection, Trim, UnaryExpr, Update as UpdateRel, Values, ValuesRow,
};
use crate::ir::operator::{ConflictStrategy, JoinKind, OrderByElement, OrderByEntity, OrderByType};
use crate::ir::transformation::redistribution::{
    MotionKey as IrMotionKey, MotionPolicy as IrMotionPolicy, Target as IrTarget,
};
use crate::ir::{node, OptionKind, Plan};

use super::expression::FunctionFeature;
use super::node::expression::Expression;
use super::node::relational::Relational;
use super::node::{Bound, BoundType, Frame, FrameType, Limit, NamedWindows, Over, Window};
use super::operator::{Arithmetic, Bool, Unary};
use super::relation::DerivedType;
use super::tree::traversal::{LevelNode, PostOrder, EXPR_CAPACITY, REL_CAPACITY};
use super::value::Value;

#[derive(Debug, PartialEq, Serialize)]
enum ColExpr {
    Parentheses(Box<ColExpr>),
    Alias(Box<ColExpr>, SmolStr),
    Arithmetic(Box<ColExpr>, Arithmetic, Box<ColExpr>),
    Bool(Box<ColExpr>, Bool, Box<ColExpr>),
    Unary(Unary, Box<ColExpr>),
    Column(String, DerivedType),
    Cast(Box<ColExpr>, CastType),
    Case(
        Option<Box<ColExpr>>,
        Vec<(Box<ColExpr>, Box<ColExpr>)>,
        Option<Box<ColExpr>>,
    ),
    Window(Box<WindowExplain>),
    Over(Box<ColExpr>, Option<Box<ColExpr>>, Box<ColExpr>),
    Concat(Box<ColExpr>, Box<ColExpr>),
    Like(Box<ColExpr>, Box<ColExpr>, Option<Box<ColExpr>>),
    ScalarFunction(
        SmolStr,
        Vec<ColExpr>,
        Option<FunctionFeature>,
        DerivedType,
        bool,
    ),
    Trim(Option<TrimKind>, Option<Box<ColExpr>>, Box<ColExpr>),
    Row(Row),
    None,
}

impl Display for ColExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            ColExpr::Window(window) => window.to_string(),
            ColExpr::Over(stable_func, filter, window) => {
                let ColExpr::ScalarFunction(func_name, args, ..) = stable_func.as_ref() else {
                    panic!("Expected ScalarFunction expression before OVER clause")
                };
                let formatted_args = format!("({})", args.iter().format(", "));

                let prefix = if let Some(filter) = filter {
                    format!("{func_name}{formatted_args} filter (where {filter}) over")
                } else {
                    format!("{func_name}{formatted_args} over")
                };
                if let ColExpr::Window(window_explain) = window.as_ref() {
                    let WindowExplain { name, .. } = window_explain.as_ref();
                    if let Some(name) = name {
                        format!("{prefix} {name}")
                    } else {
                        format!("{prefix} {window}")
                    }
                } else {
                    panic!("Expected Window expression in OVER clause")
                }
            }
            ColExpr::Parentheses(child_expr) => format!("({child_expr})"),
            ColExpr::Alias(expr, name) => format!("{expr} -> \"{name}\""),
            ColExpr::Arithmetic(left, op, right) => format!("{left} {op} {right}"),
            ColExpr::Bool(left, op, right) => format!("{left} {op} {right}"),
            ColExpr::Unary(op, expr) => match op {
                Unary::IsNull => format!("{expr} {op}"),
                Unary::Exists => format!("{op} {expr}"),
                Unary::Not => {
                    if let ColExpr::Bool(_, Bool::And, _) = **expr {
                        format!("{op} ({expr})")
                    } else {
                        format!("{op} {expr}")
                    }
                }
            },
            ColExpr::Column(c, col_type) => format!("{c}::{col_type}"),
            ColExpr::Cast(v, t) => format!("{v}::{t}"),
            ColExpr::Case(search_expr, when_blocks, else_expr) => {
                let mut res = String::from("case");
                if let Some(search_expr) = search_expr {
                    res = format!("{res} {search_expr}");
                }

                for (cond_expr, res_expr) in when_blocks {
                    res = format!("{res} when {cond_expr} then {res_expr}");
                }
                if let Some(else_expr) = else_expr {
                    res = format!("{res} else {else_expr}");
                }
                res = format!("{res} end");
                res
            }
            ColExpr::Concat(l, r) => format!("{l} || {r}"),
            ColExpr::ScalarFunction(name, args, feature, func_type, is_aggr) => {
                let mut name = name.clone();
                if !is_aggr {
                    name = to_user(name);
                }
                let is_distinct = matches!(feature, Some(FunctionFeature::Distinct));
                let formatted_args = format!("({})", args.iter().format(", "));
                let func_type_name = func_type.to_string();
                format!(
                    "{name}({}{formatted_args})::{func_type_name}",
                    if is_distinct { "distinct " } else { "" }
                )
            }
            ColExpr::Trim(kind, pattern, target) => match (kind, pattern) {
                (Some(k), Some(p)) => format!("TRIM({} {p} from {target})", k.as_str()),
                (Some(k), None) => format!("TRIM({} from {target})", k.as_str()),
                (None, Some(p)) => format!("TRIM({p} from {target})"),
                (None, None) => format!("TRIM({target})"),
            },
            ColExpr::Row(row) => row.to_string(),
            ColExpr::None => String::new(),
            ColExpr::Like(l, r, escape) => match escape {
                Some(e) => format!("{l} LIKE {r} ESCAPE {e}"),
                None => format!("{l} LIKE {r}"),
            },
        };

        write!(f, "{s}")
    }
}

impl From<&ColExpr> for String {
    fn from(s: &ColExpr) -> Self {
        s.to_string()
    }
}

impl Default for ColExpr {
    fn default() -> Self {
        Self::None
    }
}

/// Helper struct for constructing ColExpr out of
/// given plan expression node.
struct ColExprStack<'st> {
    /// Vec of (col_expr, corresponding_plan_id).
    inner: Vec<(ColExpr, NodeId)>,
    plan: &'st Plan,
}

impl<'st> ColExprStack<'st> {
    fn new<'plan: 'st>(plan: &'plan Plan) -> Self {
        Self {
            inner: Vec::new(),
            plan,
        }
    }

    fn push(&mut self, pair: (ColExpr, NodeId)) {
        self.inner.push(pair)
    }

    fn pop(&mut self) -> (ColExpr, NodeId) {
        self.inner
            .pop()
            .expect("ColExpr stack should contain expression")
    }

    fn pop_expr(&mut self, top_plan_id: Option<NodeId>) -> ColExpr {
        let (expr, plan_id) = self.pop();
        match top_plan_id {
            None => expr,
            Some(top_plan_id) => expr.covered_with_parentheses(self.plan, top_plan_id, plan_id),
        }
    }

    fn pop_expr_optional(&mut self) -> Option<ColExpr> {
        self.inner.pop().map(|(expr, _)| expr)
    }
}

impl ColExpr {
    fn covered_with_parentheses(
        self,
        plan: &Plan,
        top_plan_id: NodeId,
        self_plan_id: NodeId,
    ) -> Self {
        let should_cover = plan
            .should_cover_with_parentheses(top_plan_id, self_plan_id)
            .expect("top and child nodes should exist");
        if should_cover {
            ColExpr::Parentheses(Box::new(self))
        } else {
            self
        }
    }

    #[allow(dead_code, clippy::too_many_lines)]
    fn new(
        plan: &Plan,
        subtree_top: NodeId,
        sq_ref_map: &SubQueryRefMap,
    ) -> Result<Self, SbroadError> {
        let mut stack: ColExprStack = ColExprStack::new(plan);
        let mut dft_post =
            PostOrder::with_capacity(|node| plan.nodes.expr_iter(node, false), EXPR_CAPACITY);

        for LevelNode(_, id) in dft_post.iter(subtree_top) {
            let current_node = plan.get_expression_node(id)?;

            match &current_node {
                Expression::Window(Window {
                    name,
                    partition,
                    ordering,
                    frame,
                }) => {
                    let frame = frame.as_ref().map(|f| FrameExplain::from_ir(f, &mut stack));

                    let mut o_elems = Vec::new();
                    if let Some(ordering) = ordering {
                        for o_elem in ordering {
                            let expr = match o_elem.entity {
                                OrderByEntity::Expression { .. } => {
                                    let expr = stack.pop_expr(Some(id));
                                    OrderByExpr::Expr { expr }
                                }
                                OrderByEntity::Index { value } => OrderByExpr::Index { value },
                            };
                            o_elems.push(OrderByPair {
                                expr,
                                order_type: o_elem.order_type.clone(),
                            });
                        }
                        o_elems.reverse();
                    };

                    let mut p_elems = Vec::new();
                    if let Some(partition) = partition {
                        p_elems.reserve(partition.len());
                        for _ in partition {
                            let expr = stack.pop_expr(Some(id));
                            p_elems.push(expr)
                        }
                        p_elems.reverse();
                    }

                    let window = WindowExplain {
                        name: name.clone(),
                        partition: p_elems,
                        ordering: o_elems,
                        frame,
                    };
                    let window_expr = ColExpr::Window(Box::new(window));
                    stack.push((window_expr, id));
                }
                Expression::Over(Over { filter, .. }) => {
                    let window = stack.pop_expr(Some(id));

                    let filter = filter.map(|_| {
                        let expr = stack.pop_expr(Some(id));
                        Box::new(expr)
                    });

                    let stable_func = stack.pop_expr(Some(id));

                    let over_expr = ColExpr::Over(Box::new(stable_func), filter, Box::new(window));
                    stack.push((over_expr, id));
                }
                Expression::Cast(Cast { to, .. }) => {
                    let child_expr = stack.pop_expr(Some(id));

                    let cast_expr: ColExpr = ColExpr::Cast(Box::new(child_expr), *to);
                    stack.push((cast_expr, id));
                }
                Expression::Case(Case {
                    search_expr,
                    when_blocks,
                    else_expr,
                }) => {
                    let else_expr_col = if else_expr.is_some() {
                        let expr = stack.pop_expr(Some(id));
                        Some(Box::new(expr))
                    } else {
                        None
                    };

                    let mut match_expr_cols: Vec<(Box<ColExpr>, Box<ColExpr>)> = when_blocks
                        .iter()
                        .map(|_| {
                            let res_expr = stack.pop_expr(Some(id));

                            let cond_expr = stack.pop_expr(Some(id));
                            (Box::new(cond_expr), Box::new(res_expr))
                        })
                        .collect();
                    match_expr_cols.reverse();

                    let search_expr_col = if search_expr.is_some() {
                        let expr = stack.pop_expr(Some(id));
                        Some(Box::new(expr))
                    } else {
                        None
                    };

                    let cast_expr = ColExpr::Case(search_expr_col, match_expr_cols, else_expr_col);
                    stack.push((cast_expr, id));
                }
                Expression::CountAsterisk(_) => {
                    let count_asterisk_expr =
                        ColExpr::Column("*".to_string(), current_node.calculate_type(plan)?);
                    stack.push((count_asterisk_expr, id));
                }
                Expression::Reference(Reference { position, .. }) => {
                    let mut col_name = String::new();

                    let rel_id: NodeId = plan.get_relational_from_reference_node(id)?;

                    if let Some(name) = plan.scan_name(rel_id, *position)? {
                        col_name.push('"');
                        col_name.push_str(name);
                        col_name.push('"');
                        col_name.push('.');
                    }

                    let alias = plan.get_alias_from_reference_node(&current_node)?;
                    col_name.push('"');
                    col_name.push_str(alias);
                    col_name.push('"');

                    let ref_expr = ColExpr::Column(col_name, current_node.calculate_type(plan)?);
                    stack.push((ref_expr, id));
                }
                Expression::Concat(_) => {
                    let right = stack.pop_expr(Some(id));
                    let left = stack.pop_expr(Some(id));
                    let concat_expr = ColExpr::Concat(Box::new(left), Box::new(right));
                    stack.push((concat_expr, id));
                }
                Expression::Like { .. } => {
                    let escape = Some(stack.pop_expr(Some(id)));
                    let right = stack.pop_expr(Some(id));
                    let left = stack.pop_expr(Some(id));
                    let concat_expr =
                        ColExpr::Like(Box::new(left), Box::new(right), escape.map(Box::new));
                    stack.push((concat_expr, id));
                }
                Expression::Constant(Constant { value }) => {
                    let expr =
                        ColExpr::Column(value.to_string(), current_node.calculate_type(plan)?);
                    stack.push((expr, id));
                }
                Expression::Trim(Trim { kind, .. }) => {
                    let target = stack.pop_expr(Some(id));
                    let pattern = stack.pop_expr_optional().map(Box::new);
                    let trim_expr = ColExpr::Trim(kind.clone(), pattern, Box::new(target));
                    stack.push((trim_expr, id));
                }
                Expression::ScalarFunction(ScalarFunction {
                    name,
                    children,
                    feature,
                    func_type,
                    is_system: is_aggr,
                    ..
                }) => {
                    let mut len = children.len();
                    let mut args: Vec<ColExpr> = Vec::with_capacity(len);
                    while len > 0 {
                        let arg = stack.pop_expr(Some(id));
                        args.push(arg);
                        len -= 1;
                    }
                    args.reverse();
                    let func_expr = ColExpr::ScalarFunction(
                        name.clone(),
                        args,
                        feature.clone(),
                        *func_type,
                        *is_aggr,
                    );
                    stack.push((func_expr, id));
                }
                Expression::Row(RowExpr { list, .. }) => {
                    let mut len = list.len();
                    let mut row: ColExprStack = ColExprStack::new(plan);
                    while len > 0 {
                        let expr = stack.pop();
                        row.push(expr);
                        len -= 1;
                    }
                    row.inner.reverse();
                    let row = Row::from_col_expr_stack(plan, row, sq_ref_map)?;
                    let row_expr = ColExpr::Row(row);
                    stack.push((row_expr, id));
                }
                Expression::Arithmetic(ArithmeticExpr { op, .. }) => {
                    let right_expr = stack.pop_expr(Some(id));
                    let left_expr = stack.pop_expr(Some(id));

                    let ar_expr =
                        ColExpr::Arithmetic(Box::new(left_expr), op.clone(), Box::new(right_expr));

                    stack.push((ar_expr, id));
                }
                Expression::Alias(Alias { name, .. }) => {
                    let expr = stack.pop_expr(Some(id));
                    let alias_expr = ColExpr::Alias(Box::new(expr), name.clone());
                    stack.push((alias_expr, id));
                }
                Expression::Bool(BoolExpr { op, .. }) => {
                    let right_expr = stack.pop_expr(Some(id));
                    let left_expr = stack.pop_expr(Some(id));

                    let bool_expr = ColExpr::Bool(Box::new(left_expr), *op, Box::new(right_expr));

                    stack.push((bool_expr, id));
                }
                Expression::Unary(UnaryExpr { op, .. }) => {
                    let child_expr = stack.pop_expr(Some(id));
                    let alias_expr = ColExpr::Unary(op.clone(), Box::new(child_expr));
                    stack.push((alias_expr, id));
                }
                Expression::LocalTimestamp(_) => {
                    let expr = ColExpr::Column(
                        "LocalTimestamp".to_string(),
                        current_node.calculate_type(plan)?,
                    );
                    stack.push((expr, id));
                }
                Expression::Parameter(_) => (),
            }
        }

        let expr = stack.pop_expr(None);
        Ok(expr)
    }
}

/// Alias for map of (`SubQuery` id -> it's offset).
/// Offset = `SubQuery` index (e.g. in case there are several `SubQueries` in Selection WHERE condition
/// index will indicate to which of them Reference is pointing).
type SubQueryRefMap = HashMap<NodeId, usize>;

#[derive(Debug, PartialEq, Serialize)]
struct Projection {
    /// List of colums in sql query
    cols: Vec<ColExpr>,
}

#[derive(Debug, PartialEq, Serialize)]
enum BoundTypeExplain {
    PrecedingUnbounded,
    PrecedingOffset(ColExpr),
    CurrentRow,
    FollowingOffset(ColExpr),
    FollowingUnbounded,
}

impl BoundTypeExplain {
    fn from_ir(b_type: &BoundType, stack: &mut ColExprStack) -> Self {
        match b_type {
            BoundType::PrecedingUnbounded => BoundTypeExplain::PrecedingUnbounded,
            BoundType::PrecedingOffset(_) => {
                let expr: ColExpr = stack.pop_expr(None);
                BoundTypeExplain::PrecedingOffset(expr)
            }
            BoundType::CurrentRow => BoundTypeExplain::CurrentRow,
            BoundType::FollowingOffset(_) => {
                let expr = stack.pop_expr(None);
                BoundTypeExplain::FollowingOffset(expr)
            }
            BoundType::FollowingUnbounded => BoundTypeExplain::FollowingUnbounded,
        }
    }
}

impl Display for BoundTypeExplain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BoundTypeExplain::PrecedingUnbounded => String::from("unbounded preceding"),
            BoundTypeExplain::PrecedingOffset(expr) => format!("{} preceding", expr),
            BoundTypeExplain::CurrentRow => String::from("current row"),
            BoundTypeExplain::FollowingOffset(expr) => format!("{} following", expr),
            BoundTypeExplain::FollowingUnbounded => String::from("unbounded following"),
        };

        write!(f, "{s}")
    }
}

#[derive(Debug, PartialEq, Serialize)]
enum BoundExplain {
    Single(BoundTypeExplain),
    Between(BoundTypeExplain, BoundTypeExplain),
}

impl BoundExplain {
    fn from_ir(bound: &Bound, stack: &mut ColExprStack) -> Self {
        match bound {
            Bound::Single(b_type) => BoundExplain::Single(BoundTypeExplain::from_ir(b_type, stack)),
            Bound::Between(lower, upper) => {
                let upper_bound = BoundTypeExplain::from_ir(upper, stack);
                let lower_bound = BoundTypeExplain::from_ir(lower, stack);
                BoundExplain::Between(lower_bound, upper_bound)
            }
        }
    }
}

impl Display for BoundExplain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BoundExplain::Single(bound) => format!("{bound}"),
            BoundExplain::Between(lower, upper) => format!("between {lower} and {upper}"),
        };

        write!(f, "{s}")
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct FrameExplain {
    ty: FrameType,
    bound: BoundExplain,
}

impl FrameExplain {
    fn from_ir(frame: &Frame, stack: &mut ColExprStack) -> Self {
        let bound = BoundExplain::from_ir(&frame.bound, stack);
        FrameExplain {
            ty: frame.ty.clone(),
            bound,
        }
    }
}

impl Display for FrameExplain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.ty, self.bound)
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct WindowExplain {
    name: Option<SmolStr>,
    partition: Vec<ColExpr>,
    ordering: Vec<OrderByPair>,
    frame: Option<FrameExplain>,
}

impl Display for WindowExplain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = String::from("");

        if let Some(name) = &self.name {
            write!(s, "{name} as ")?;
        }
        write!(s, "(")?;

        if !self.partition.is_empty() {
            write!(s, "partition by ")?;
            let exprs = format!("({})", self.partition.iter().format(", "));
            write!(s, "{exprs} ")?;
        }

        if !self.ordering.is_empty() {
            write!(s, "order by ")?;
            let exprs = format!("({})", self.ordering.iter().format(", "));
            write!(s, "{exprs} ")?;
        }

        if let Some(frame) = &self.frame {
            write!(s, "{frame}")?;
        }

        write!(s, ")")?;
        write!(f, "{s}")
    }
}

impl Projection {
    #[allow(dead_code)]
    fn new(
        plan: &Plan,
        output_id: NodeId,
        sq_ref_map: &SubQueryRefMap,
    ) -> Result<Self, SbroadError> {
        let output = plan.get_expression_node(output_id)?;
        let col_list = output.get_row_list()?;
        let mut result = Projection {
            cols: Vec::with_capacity(col_list.len()),
        };

        for col_id in col_list {
            let col = ColExpr::new(plan, *col_id, sq_ref_map)?;
            result.cols.push(col);
        }
        Ok(result)
    }
}

impl Display for Projection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = String::from("projection ");

        let cols = &self
            .cols
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join(", ");

        write!(s, "({cols})")?;
        write!(f, "{s}")
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct NamedWindowsExplain {
    windows: Vec<ColExpr>,
}

impl NamedWindowsExplain {
    #[allow(dead_code)]
    fn new(
        plan: &Plan,
        windows: &Vec<NodeId>,
        sq_ref_map: &SubQueryRefMap,
    ) -> Result<Self, SbroadError> {
        let mut result = NamedWindowsExplain {
            windows: Vec::with_capacity(windows.len()),
        };
        for window_id in windows {
            let window = ColExpr::new(plan, *window_id, sq_ref_map)?;
            result.windows.push(window);
        }
        Ok(result)
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct GroupBy {
    /// List of colums in sql query
    gr_exprs: Vec<ColExpr>,
    output_cols: Vec<ColExpr>,
}

impl GroupBy {
    #[allow(dead_code)]
    fn new(
        plan: &Plan,
        gr_exprs: &Vec<NodeId>,
        output_id: NodeId,
        sq_ref_map: &SubQueryRefMap,
    ) -> Result<Self, SbroadError> {
        let mut result = GroupBy {
            gr_exprs: vec![],
            output_cols: vec![],
        };

        for col_node_id in gr_exprs {
            let col = ColExpr::new(plan, *col_node_id, sq_ref_map)?;
            result.gr_exprs.push(col);
        }
        let alias_list = plan.get_expression_node(output_id)?;
        for col_node_id in alias_list.get_row_list()? {
            let col = ColExpr::new(plan, *col_node_id, sq_ref_map)?;
            result.output_cols.push(col);
        }
        Ok(result)
    }
}

impl Display for GroupBy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = "group by ".to_string();

        let gr_exprs = &self
            .gr_exprs
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join(", ");

        let output_cols = &self
            .output_cols
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join(", ");

        write!(s, "({gr_exprs})")?;
        write!(s, " output: ({output_cols})")?;
        write!(f, "{s}")
    }
}

#[derive(Debug, PartialEq, Serialize)]
enum OrderByExpr {
    Expr { expr: ColExpr },
    Index { value: usize },
}

impl Display for OrderByExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderByExpr::Expr { expr } => write!(f, "{expr}"),
            OrderByExpr::Index { value } => write!(f, "{value}"),
        }
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct OrderByPair {
    expr: OrderByExpr,
    order_type: Option<OrderByType>,
}

impl Display for OrderByPair {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(order_type) = &self.order_type {
            write!(f, "{} {order_type}", self.expr)
        } else {
            write!(f, "{}", self.expr)
        }
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct OrderBy {
    order_by_elements: Vec<OrderByPair>,
}

impl OrderBy {
    #[allow(dead_code)]
    fn new(
        plan: &Plan,
        order_by_elements: &Vec<OrderByElement>,
        sq_ref_map: &SubQueryRefMap,
    ) -> Result<Self, SbroadError> {
        let mut result = OrderBy {
            order_by_elements: vec![],
        };

        for order_by_element in order_by_elements {
            let expr = match order_by_element.entity {
                OrderByEntity::Expression { expr_id } => OrderByExpr::Expr {
                    expr: ColExpr::new(plan, expr_id, sq_ref_map)?,
                },
                OrderByEntity::Index { value } => OrderByExpr::Index { value },
            };
            result.order_by_elements.push(OrderByPair {
                expr,
                order_type: order_by_element.order_type.clone(),
            });
        }
        Ok(result)
    }
}

impl Display for OrderBy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let order_by_elements = &self
            .order_by_elements
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join(", ");

        write!(f, "order by ({order_by_elements})")
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct Update {
    /// List of columns in sql query
    table: SmolStr,
    update_statements: Vec<(SmolStr, SmolStr)>,
}

impl Update {
    #[allow(dead_code)]
    fn new(plan: &Plan, update_id: NodeId) -> Result<Self, SbroadError> {
        if let Relational::Update(UpdateRel {
            relation: ref rel,
            update_columns_map,
            output: ref output_id,
            ..
        }) = plan.get_relation_node(update_id)?
        {
            let mut update_statements: Vec<(SmolStr, SmolStr)> =
                Vec::with_capacity(update_columns_map.len());
            let table = plan.relations.get(rel).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Node,
                    Some(format_smolstr!("invalid table {rel} in Update node")),
                )
            })?;
            let output_list = plan.get_row_list(*output_id)?;
            for (col_idx, proj_col) in update_columns_map {
                let col_name = table
                    .columns
                    .get(*col_idx)
                    .map(|c| to_user(&c.name))
                    .ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::Node,
                            Some(format_smolstr!(
                                "invalid column index {col_idx} in Update node"
                            )),
                        )
                    })?;
                let proj_alias = {
                    let alias_id = *output_list.get(*proj_col).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::Node,
                            Some(format_smolstr!(
                                "invalid update projection position {proj_col} in Update node"
                            )),
                        )
                    })?;
                    let node = plan.get_expression_node(alias_id)?;
                    if let Expression::Alias(Alias { name, .. }) = node {
                        to_user(name)
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Node,
                            Some(format_smolstr!(
                                "expected alias as top in Update output, got: {node:?}"
                            )),
                        ));
                    }
                };
                update_statements.push((col_name, proj_alias));
            }
            let result = Update {
                table: rel.clone(),
                update_statements,
            };
            return Ok(result);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!(
                "explain: expected Update node on id: {update_id}"
            )),
        ))
    }
}

impl Display for Update {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = "update ".to_string();

        write!(s, "\"{}\"", &self.table)?;
        let update_statements = self
            .update_statements
            .iter()
            .map(|(col, alias)| format!("{col} = {alias}"))
            .join("\n");
        write!(s, "\n{update_statements}")?;
        write!(f, "{s}")
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct Scan {
    /// Table name
    table: SmolStr,

    /// Table alias
    alias: Option<SmolStr>,
}

impl Scan {
    #[allow(dead_code)]
    fn new(table: SmolStr, alias: Option<SmolStr>) -> Self {
        Scan { table, alias }
    }
}

impl Display for Scan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = String::from("scan ");

        write!(s, "\"{}\"", &self.table)?;

        if let Some(a) = &self.alias {
            write!(s, " -> \"{a}\"")?;
        }

        write!(f, "{s}")
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct Ref {
    /// Reference to subquery/window index in `FullExplain` parts
    number: usize,
}

impl Ref {
    #[allow(dead_code)]
    fn new(number: usize) -> Self {
        Ref { number }
    }
}

impl Display for Ref {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "${}", self.number)
    }
}

#[derive(Debug, PartialEq, Serialize)]
enum RowVal {
    ColumnExpr(ColExpr),
    SqRef(Ref),
}

impl Display for RowVal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            RowVal::ColumnExpr(c) => c.to_string(),
            RowVal::SqRef(r) => r.to_string(),
        };

        write!(f, "{s}")
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct Row {
    /// List of sql values in `WHERE` cause
    cols: Vec<RowVal>,
}

impl Row {
    #[allow(dead_code)]
    fn new() -> Self {
        Row { cols: vec![] }
    }

    fn add_col(&mut self, row: RowVal) {
        self.cols.push(row);
    }

    fn from_col_expr_stack(
        plan: &Plan,
        col_expr_stack: ColExprStack,
        sq_ref_map: &SubQueryRefMap,
    ) -> Result<Self, SbroadError> {
        let mut row = Row::new();

        for (col_expr, expr_id) in col_expr_stack.inner {
            let current_node = plan.get_expression_node(expr_id)?;

            match &current_node {
                Expression::Reference { .. } => {
                    let rel_id: NodeId = plan.get_relational_from_reference_node(expr_id)?;

                    let rel_node = plan.get_relation_node(rel_id)?;
                    if plan.is_additional_child(rel_id)? {
                        if let Relational::ScanSubQuery { .. } | Relational::Motion { .. } =
                            rel_node
                        {
                            let sq_offset = sq_ref_map.get(&rel_id).unwrap_or_else(|| {
                                panic!("Not found subquery with index {rel_id} in the map.")
                            });
                            row.add_col(RowVal::SqRef(Ref::new(*sq_offset)));
                        } else {
                            panic!(
                                "Additional child ({rel_id}) is not SQ or Motion: {rel_node:?}."
                            );
                        }
                    } else {
                        let col = ColExpr::new(plan, expr_id, sq_ref_map)?;
                        row.add_col(RowVal::ColumnExpr(col));
                    }
                }
                _ => row.add_col(RowVal::ColumnExpr(col_expr)),
            }
        }

        Ok(row)
    }
}

impl Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let cols = &self
            .cols
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join(", ");

        write!(f, "ROW({cols})")
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct SubQuery {
    /// Subquery alias. For subquery in `WHERE` cause alias is `None`.
    alias: Option<String>,
}

impl SubQuery {
    #[allow(dead_code)]
    fn new(alias: Option<String>) -> Self {
        SubQuery { alias }
    }
}

impl Display for SubQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = String::from("scan");

        if let Some(a) = &self.alias {
            write!(s, " \"{a}\"")?;
        }
        write!(f, "{s}")
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct Motion {
    policy: MotionPolicy,
}

impl Motion {
    fn new(policy: MotionPolicy) -> Self {
        Motion { policy }
    }
}

impl Display for Motion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "motion [policy: {}]", &self.policy)
    }
}

#[derive(Debug, PartialEq, Serialize)]
enum MotionPolicy {
    None,
    Full,
    Segment(MotionKey),
    Local,
    LocalSegment(MotionKey),
}

impl Display for MotionPolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            MotionPolicy::None => write!(f, "none"),
            MotionPolicy::Full => write!(f, "full"),
            MotionPolicy::Segment(mk) => write!(f, "segment({mk})"),
            MotionPolicy::Local => write!(f, "local"),
            MotionPolicy::LocalSegment(mk) => write!(f, "local segment({mk})"),
        }
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct MotionKey {
    pub targets: Vec<Target>,
}

impl Display for MotionKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let targets = &self
            .targets
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join(", ");

        write!(f, "[{targets}]")
    }
}

#[derive(Debug, PartialEq, Serialize)]
enum Target {
    Reference(String),
    Value(Value),
}

impl Display for Target {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            Target::Reference(s) => write!(f, "ref(\"{s}\")"),
            Target::Value(v) => write!(f, "value({v})"),
        }
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct InnerJoin {
    condition: ColExpr,
    kind: JoinKind,
}

impl Display for InnerJoin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let kind = match self.kind {
            JoinKind::LeftOuter => {
                let mut s = self.kind.to_string();
                s.push(' ');
                s
            }
            JoinKind::Inner => String::new(),
        };
        write!(f, "{kind}join on {0}", self.condition)
    }
}

#[derive(Debug, PartialEq, Serialize)]
#[allow(dead_code)]
enum ExplainNode {
    Delete(SmolStr),
    Except,
    Intersect,
    GroupBy(GroupBy),
    OrderBy(OrderBy),
    InnerJoin(InnerJoin),
    ValueRow(ColExpr),
    Value,
    Insert(SmolStr, ConflictStrategy),
    Projection(Projection),
    Scan(Scan),
    Selection(ColExpr),
    Having(ColExpr),
    Union,
    UnionAll,
    Update(Update),
    SubQuery(SubQuery),
    Motion(Motion),
    Cte(SmolStr, Ref),
    Limit(u64),
    NamedWindows(NamedWindowsExplain),
}

impl Display for ExplainNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            ExplainNode::NamedWindows(NamedWindowsExplain { windows }) => {
                let windows_str = windows
                    .iter()
                    .map(ToSmolStr::to_smolstr)
                    .collect::<Vec<SmolStr>>()
                    .join(", ");
                format_smolstr!("windows: {windows_str}")
            }
            ExplainNode::Cte(s, r) => format_smolstr!("scan cte {s}({r})"),
            ExplainNode::Delete(s) => format_smolstr!("delete \"{s}\""),
            ExplainNode::Except => "except".to_smolstr(),
            ExplainNode::InnerJoin(i) => i.to_smolstr(),
            ExplainNode::ValueRow(r) => format_smolstr!("value row (data={r})"),
            ExplainNode::Value => "values".to_smolstr(),
            ExplainNode::Insert(s, conflict) => {
                format_smolstr!("insert \"{s}\" on conflict: {conflict}")
            }
            ExplainNode::Projection(e) => e.to_smolstr(),
            ExplainNode::GroupBy(p) => p.to_smolstr(),
            ExplainNode::OrderBy(o_b) => o_b.to_smolstr(),
            ExplainNode::Scan(s) => s.to_smolstr(),
            ExplainNode::Selection(s) => format_smolstr!("selection {s}"),
            ExplainNode::Having(s) => format_smolstr!("having {s}"),
            ExplainNode::Union => "union".to_smolstr(),
            ExplainNode::UnionAll => "union all".to_smolstr(),
            ExplainNode::Intersect => "intersect".to_smolstr(),
            ExplainNode::Update(u) => u.to_smolstr(),
            ExplainNode::SubQuery(s) => s.to_smolstr(),
            ExplainNode::Motion(m) => m.to_smolstr(),
            ExplainNode::Limit(l) => format_smolstr!("limit {l}"),
        };

        write!(f, "{s}")
    }
}

/// Describe sql query (or subquery) as recursive type
#[derive(Debug, Serialize)]
struct ExplainTreePart {
    /// Level helps to detect count of idents
    #[serde(skip_serializing)]
    level: usize,
    /// Current node of sql query
    current: Option<ExplainNode>,
    /// Children nodes of current sql node
    children: Vec<ExplainTreePart>,
}

impl PartialEq for ExplainTreePart {
    fn eq(&self, other: &Self) -> bool {
        self.current == other.current && self.children == other.children
    }
}

impl Display for ExplainTreePart {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        if let Some(c) = &self.current {
            writeln!(s, "{}", &c.to_string())?;
        }

        let ident = (0..=self.level).map(|_| "    ").collect::<String>();

        for child in &self.children {
            s.push_str(&ident);
            s.push_str(&child.to_string());
        }

        write!(f, "{s}")
    }
}

impl Default for ExplainTreePart {
    fn default() -> Self {
        Self {
            level: 0,
            current: None,
            children: Vec::with_capacity(200),
        }
    }
}

impl ExplainTreePart {
    fn with_level(level: usize) -> Self {
        ExplainTreePart {
            level,
            current: None,
            children: Vec::with_capacity(100),
        }
    }
}

#[derive(Debug, Default)]
struct FullExplain {
    /// Main sql subtree
    main_query: ExplainTreePart,
    /// Independent sub-trees of main sql query (e.g. sub-queries in `WHERE` cause, CTEs, etc.)
    subqueries: Vec<ExplainTreePart>,
    /// Windows under Projection.
    windows: Vec<ExplainTreePart>,
    /// Options imposed during query execution
    exec_options: Vec<(OptionKind, Value)>,
    /// Info related to plan execution
    buckets_info: Option<BucketsInfo>,
}

fn buckets_repr(buckets: &Buckets, bucket_count: u64) -> String {
    match buckets {
        Buckets::All => format!("[1-{bucket_count}]"),
        Buckets::Filtered(buckets_set) => 'f: {
            if buckets_set.is_empty() {
                break 'f "[]".into();
            }

            let mut nums: Vec<u64> = buckets_set.iter().copied().collect();
            nums.sort_unstable();

            let mut ranges = Vec::new();
            let mut l = 0;
            for r in 1..nums.len() {
                if nums[r - 1] + 1 == nums[r] {
                    continue;
                }
                if r - l == 1 {
                    ranges.push(format!("{}", nums[l]));
                } else {
                    ranges.push(format!("{}-{}", nums[l], nums[r - 1]))
                }
                l = r;
            }

            let r = nums.len();
            if r - l == 1 {
                ranges.push(format!("{}", nums[r - 1]));
            } else {
                ranges.push(format!("{}-{}", nums[l], nums[r - 1]))
            }

            format!("[{}]", ranges.join(","))
        }
        Buckets::Any => "any".into(),
    }
}

impl Display for FullExplain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = self.main_query.to_string();

        for (pos, sq) in self.subqueries.iter().enumerate() {
            writeln!(s, "subquery ${pos}:")?;
            s.push_str(&sq.to_string());
        }
        for (pos, window) in self.windows.iter().enumerate() {
            writeln!(s, "window ${pos}:")?;
            s.push_str(&window.to_string());
        }
        if !self.exec_options.is_empty() {
            writeln!(s, "execution options:")?;
            for opt in &self.exec_options {
                writeln!(s, "{:4}{} = {}", "", opt.0, opt.1)?;
            }
        }
        if let Some(info) = &self.buckets_info {
            match info {
                BucketsInfo::Unknown => writeln!(s, "buckets = unknown")?,
                BucketsInfo::Calculated(calculated) => {
                    let repr = buckets_repr(&calculated.buckets, calculated.bucket_count);
                    // For buckets ANY and ALL there is no sense to handle in the
                    // output the case when bucket count is not exact.
                    match calculated.buckets {
                        Buckets::Any | Buckets::All => writeln!(s, "buckets = {repr}",)?,
                        _ if calculated.is_exact => writeln!(s, "buckets = {repr}",)?,
                        _ => writeln!(s, "buckets <= {repr}",)?,
                    }
                }
            }
        }

        write!(f, "{s}")
    }
}

impl FullExplain {
    /// Retrieve SubQueryRefMap from relational node children and update
    /// `current_node` children.
    fn get_sq_ref_map(
        &mut self,
        current_node: &mut ExplainTreePart,
        stack: &mut Vec<ExplainTreePart>,
        children: &[NodeId],
        req_children_number: usize,
    ) -> SubQueryRefMap {
        let mut sq_ref_map: SubQueryRefMap = HashMap::with_capacity(children.len());

        // Note that subqueries are added to the stack in the `children` reversed order
        // because of the PostOrder traversal. That's why we apply `rev` here.
        for sq_id in children.iter().skip(req_children_number).rev() {
            let sq_node = stack
                .pop()
                .unwrap_or_else(|| panic!("Rel node failed to pop a sub-query."));
            self.subqueries.push(sq_node);
            let offset = self.subqueries.len() - 1;
            sq_ref_map.insert(*sq_id, offset);
        }

        let mut children_to_add = Vec::with_capacity(req_children_number);
        for _ in 0..req_children_number {
            children_to_add.push(
                stack.pop().unwrap_or_else(|| {
                    panic!("Expected to pop required child for {current_node:?}")
                }),
            );
        }
        children_to_add.reverse();
        for child in children_to_add {
            current_node.children.push(child);
        }

        sq_ref_map
    }

    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    pub fn new(ir: &Plan, top_id: NodeId) -> Result<Self, SbroadError> {
        let mut stack: Vec<ExplainTreePart> = Vec::new();
        let mut result = FullExplain::default();
        result.exec_options.push((
            OptionKind::VdbeOpcodeMax,
            Value::Unsigned(ir.options.sql_vdbe_opcode_max),
        ));
        result.exec_options.push((
            OptionKind::MotionRowMax,
            Value::Unsigned(ir.options.sql_motion_row_max),
        ));

        let mut dft_post = PostOrder::with_capacity(|node| ir.nodes.rel_iter(node), REL_CAPACITY);
        for LevelNode(level, id) in dft_post.iter(top_id) {
            let mut current_node = ExplainTreePart::with_level(level);
            let node = ir.get_relation_node(id)?;

            current_node.current = match &node {
                Relational::NamedWindows(NamedWindows { child, windows, .. }) => {
                    let mut sq_ref_map =
                        result.get_sq_ref_map(&mut current_node, &mut stack, &[*child], 1);
                    let proj = ir.find_parent_rel(id)?.unwrap();
                    let children = ir.get_relational_children(proj)?;
                    for (k, child) in children[1..].iter().rev().enumerate() {
                        sq_ref_map.insert(*child, k);
                    }
                    let window_exprs = NamedWindowsExplain::new(ir, windows, &sq_ref_map)?;
                    Some(ExplainNode::NamedWindows(window_exprs))
                }
                Relational::Intersect { .. } => {
                    if let (Some(right), Some(left)) = (stack.pop(), stack.pop()) {
                        current_node.children.push(left);
                        current_node.children.push(right);
                    } else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "Intersect node must have exactly two children".into(),
                        ));
                    }
                    Some(ExplainNode::Intersect)
                }
                Relational::Except { .. } => {
                    if let (Some(right), Some(left)) = (stack.pop(), stack.pop()) {
                        current_node.children.push(left);
                        current_node.children.push(right);
                    } else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "Exception node must have exactly two children".into(),
                        ));
                    }
                    Some(ExplainNode::Except)
                }
                Relational::GroupBy(node::GroupBy {
                    gr_exprs,
                    output,
                    children,
                    ..
                }) => {
                    let sq_ref_map =
                        result.get_sq_ref_map(&mut current_node, &mut stack, children, 1);
                    let p = GroupBy::new(ir, gr_exprs, *output, &sq_ref_map)?;
                    Some(ExplainNode::GroupBy(p))
                }
                Relational::OrderBy(node::OrderBy {
                    order_by_elements,
                    children,
                    ..
                }) => {
                    let sq_ref_map =
                        result.get_sq_ref_map(&mut current_node, &mut stack, children, 1);
                    let o_b = OrderBy::new(ir, order_by_elements, &sq_ref_map)?;
                    Some(ExplainNode::OrderBy(o_b))
                }
                Relational::Projection(node::Projection {
                    output, children, ..
                }) => {
                    let sq_ref_map: HashMap<NodeId, usize> =
                        result.get_sq_ref_map(&mut current_node, &mut stack, children, 1);

                    let p = Projection::new(ir, *output, &sq_ref_map)?;
                    Some(ExplainNode::Projection(p))
                }
                Relational::SelectWithoutScan(node::SelectWithoutScan {
                    output, children, ..
                }) => {
                    let sq_ref_map =
                        result.get_sq_ref_map(&mut current_node, &mut stack, children, 0);
                    let p = Projection::new(ir, *output, &sq_ref_map)?;
                    Some(ExplainNode::Projection(p))
                }
                Relational::ScanRelation(ScanRelation {
                    relation, alias, ..
                }) => {
                    let s = Scan::new(
                        relation.to_smolstr(),
                        alias.as_ref().map(ToSmolStr::to_smolstr),
                    );
                    Some(ExplainNode::Scan(s))
                }
                Relational::ScanCte(ScanCte { alias, .. }) => {
                    let child = stack.pop().expect("CTE node must have exactly one child");
                    let existing_pos = result.subqueries.iter().position(|sq| *sq == child);
                    let pos = existing_pos.unwrap_or_else(|| {
                        result.subqueries.push(child);
                        result.subqueries.len() - 1
                    });
                    Some(ExplainNode::Cte(alias.clone(), Ref::new(pos)))
                }
                Relational::Selection(Selection {
                    children, filter, ..
                })
                | Relational::Having(Having {
                    children, filter, ..
                }) => {
                    let sq_ref_map =
                        result.get_sq_ref_map(&mut current_node, &mut stack, children, 1);
                    let filter_id = *ir.undo.get_oldest(filter);
                    let selection = ColExpr::new(ir, filter_id, &sq_ref_map)?;
                    let explain_node = match &node {
                        Relational::Selection { .. } => ExplainNode::Selection(selection),
                        Relational::Having { .. } => ExplainNode::Having(selection),
                        _ => panic!("Expected Selection or Having node."),
                    };
                    Some(explain_node)
                }
                u @ (Relational::UnionAll { .. } | Relational::Union { .. }) => {
                    if let (Some(right), Some(left)) = (stack.pop(), stack.pop()) {
                        current_node.children.push(left);
                        current_node.children.push(right);
                    } else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "Union all node must have exactly two children".into(),
                        ));
                    }
                    if matches!(u, Relational::Union { .. }) {
                        Some(ExplainNode::Union)
                    } else {
                        Some(ExplainNode::UnionAll)
                    }
                }
                Relational::ScanSubQuery(ScanSubQuery { alias, .. }) => {
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "ScanSubQuery node must have exactly one child".into(),
                        )
                    })?;
                    current_node.children.push(child);
                    let s = SubQuery::new(alias.as_ref().map(ToString::to_string));
                    Some(ExplainNode::SubQuery(s))
                }
                Relational::Motion(MotionRel {
                    child: child_id,
                    policy,
                    ..
                }) => {
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Motion node must have exactly one child".into(),
                        )
                    })?;
                    current_node.children.push(child);

                    let collect_targets = |s: &IrMotionKey| -> Result<Vec<Target>, SbroadError> {
                        let child_id = child_id.ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(
                                "current node should have exactly one child".to_smolstr(),
                            )
                        })?;

                        let child_output_id = ir.get_relation_node(child_id)?.output();
                        let col_list = ir.get_row_list(child_output_id)?;

                        let targets = (s.targets)
                            .iter()
                            .map(|r| match r {
                                IrTarget::Reference(pos) => {
                                    let col_id = *col_list.get(*pos).ok_or_else(|| {
                                        SbroadError::NotFound(
                                            Entity::Target,
                                            format_smolstr!("reference with position {pos}"),
                                        )
                                    })?;
                                    let col_name = ir
                                        .get_expression_node(col_id)?
                                        .get_alias_name()?
                                        .to_string();

                                    Ok::<Target, SbroadError>(Target::Reference(col_name))
                                }
                                IrTarget::Value(v) => Ok(Target::Value(v.clone())),
                            })
                            .collect::<Result<Vec<Target>, _>>()?;
                        Ok(targets)
                    };

                    let p = match policy {
                        IrMotionPolicy::None => MotionPolicy::None,
                        IrMotionPolicy::Segment(s) => {
                            let targets = collect_targets(s)?;
                            MotionPolicy::Segment(MotionKey { targets })
                        }
                        IrMotionPolicy::Full => MotionPolicy::Full,
                        IrMotionPolicy::Local => MotionPolicy::Local,
                        IrMotionPolicy::LocalSegment(s) => {
                            let targets = collect_targets(s)?;
                            MotionPolicy::LocalSegment(MotionKey { targets })
                        }
                    };

                    let m = Motion::new(p);
                    Some(ExplainNode::Motion(m))
                }
                Relational::Join(Join {
                    children,
                    condition,
                    kind,
                    ..
                }) => {
                    let sq_ref_map =
                        result.get_sq_ref_map(&mut current_node, &mut stack, children, 2);
                    let condition = ColExpr::new(ir, *condition, &sq_ref_map)?;
                    Some(ExplainNode::InnerJoin(InnerJoin {
                        condition,
                        kind: kind.clone(),
                    }))
                }
                Relational::ValuesRow(ValuesRow { data, children, .. }) => {
                    let sq_ref_map =
                        result.get_sq_ref_map(&mut current_node, &mut stack, children, 0);
                    let row = ColExpr::new(ir, *data, &sq_ref_map)?;

                    Some(ExplainNode::ValueRow(row))
                }
                Relational::Values(Values { children, .. }) => {
                    let mut amount_values = children.len();

                    while amount_values > 0 {
                        let value_row = stack.pop().ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(
                                "Insert node failed to pop a value row.".into(),
                            )
                        })?;

                        current_node.children.insert(0, value_row);
                        amount_values -= 1;
                    }
                    Some(ExplainNode::Value)
                }
                Relational::Insert(Insert {
                    relation,
                    conflict_strategy,
                    ..
                }) => {
                    let values = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Insert node failed to pop a value row.".into(),
                        )
                    })?;

                    current_node.children.push(values);

                    Some(ExplainNode::Insert(
                        relation.to_smolstr(),
                        conflict_strategy.clone(),
                    ))
                }
                Relational::Update { .. } => {
                    let values = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Insert node failed to pop a value row.".into(),
                        )
                    })?;

                    current_node.children.push(values);

                    Some(ExplainNode::Update(Update::new(ir, id)?))
                }
                Relational::Delete(Delete {
                    relation, output, ..
                }) => {
                    if output.is_some() {
                        let values = stack.pop().ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(
                                "Delete node failed to pop a value row.".into(),
                            )
                        })?;

                        current_node.children.push(values);
                    }

                    Some(ExplainNode::Delete(relation.to_smolstr()))
                }
                Relational::Limit(Limit { limit, .. }) => {
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Limit node must have exactly one child".into(),
                        )
                    })?;

                    current_node.children.push(child);

                    Some(ExplainNode::Limit(*limit))
                }
            };

            stack.push(current_node);
        }
        result.main_query = stack
            .pop()
            .ok_or_else(|| SbroadError::NotFound(Entity::Node, "that is explain top".into()))?;
        Ok(result)
    }

    fn add_execution_info(&mut self, info: BucketsInfo) {
        self.buckets_info = Some(info);
    }
}

impl Plan {
    /// Display ir explain
    ///
    /// # Errors
    /// - Failed to get top node
    /// - Failed to build explain
    pub fn as_explain(&self) -> Result<SmolStr, SbroadError> {
        let top_id = self.get_top()?;
        let explain = FullExplain::new(self, top_id)?;
        Ok(explain.to_smolstr())
    }
}

impl<C: Router> Query<'_, C> {
    pub fn as_explain(&mut self) -> Result<SmolStr, SbroadError> {
        let plan = self.get_exec_plan().get_ir_plan();
        let top_id = plan.get_top()?;
        let mut explain = FullExplain::new(plan, top_id)?;

        let info = BucketsInfo::new_from_query(self)?;
        explain.add_execution_info(info);

        Ok(explain.to_smolstr())
    }
}

#[cfg(feature = "mock")]
#[cfg(test)]
mod tests;

mod execution_info;
