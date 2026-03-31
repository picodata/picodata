use std::collections::HashMap;
use std::fmt::{self, Display};

use itertools::Itertools;
use serde::Serialize;
use smol_str::{format_smolstr, SmolStr, SmolStrBuilder, ToSmolStr};

use crate::errors::{Entity, SbroadError};
use crate::executor::engine::Router;
use crate::executor::ExecutingQuery;
use crate::ir::bucket::{BucketSet, Buckets};
use crate::ir::explain::execution_info::BucketsInfo;
use crate::ir::expression::TrimKind;
use crate::ir::node::{
    Alias, ArithmeticExpr, BoolExpr, Case, Cast, Constant, Delete, Having, IndexExpr, Insert, Join,
    Motion as MotionRel, NodeId, Reference, Row as RowExpr, ScalarFunction, ScanCte, ScanRelation,
    ScanSubQuery, Selection, SubQueryReference, Timestamp, Trim, UnaryExpr, Update as UpdateRel,
    Values, ValuesRow,
};
use crate::ir::operator::{ConflictStrategy, JoinKind, OrderByElement, OrderByEntity, OrderByType};
use crate::ir::options::OptionKind;
use crate::ir::transformation::redistribution::{
    MotionKey as IrMotionKey, MotionPolicy as IrMotionPolicy, Program, Target as IrTarget,
};
use crate::ir::{node, Plan};
use crate::utils::OrderedMap;

use super::expression::FunctionFeature;
use super::helpers::RepeatableState;
use super::node::expression::Expression;
use super::node::relational::Relational;
use super::node::{Bound, BoundType, Frame, FrameType, Limit, Over, Window};
use super::operator::{Arithmetic, Bool, Unary};
use super::tree::traversal::{LevelNode, PostOrder, EXPR_CAPACITY, REL_CAPACITY};
use super::types::{CastType, DerivedType};
use super::value::Value;

const INDENT: &str = "  ";

/// Check if a string slice is lowercase alphanumeric.
fn name_requires_quotes(s: &str) -> bool {
    !s.chars()
        .all(|c| c == '_' || c.is_ascii_digit() || c.is_ascii_lowercase())
}

fn smolstr_builder_write_name(builder: &mut SmolStrBuilder, name: &str) {
    let need_quotes = name_requires_quotes(name);
    if need_quotes {
        builder.push('"');
    }
    builder.push_str(name);
    if need_quotes {
        builder.push('"');
    }
}

fn name_to_smolstr(name: &str) -> SmolStr {
    let mut builder = SmolStrBuilder::new();
    smolstr_builder_write_name(&mut builder, name);
    builder.finish()
}

#[derive(Default, Debug, PartialEq, Serialize, Clone)]
enum ColExpr {
    Parentheses(Box<ColExpr>),
    Alias(Box<ColExpr>, SmolStr),
    Arithmetic(Box<ColExpr>, Arithmetic, Box<ColExpr>),
    Bool(Box<ColExpr>, Bool, Box<ColExpr>),
    Unary(Unary, Box<ColExpr>),
    Column(SmolStr, DerivedType),
    Asterisk,
    Index(Box<ColExpr>, Box<ColExpr>),
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
    #[default]
    None,
}

impl Display for ColExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ColExpr::Window(window) => write!(f, "{window}")?,
            ColExpr::Over(stable_func, filter, window) => {
                let ColExpr::ScalarFunction(func_name, args, ..) = stable_func.as_ref() else {
                    panic!("Expected ScalarFunction expression before OVER clause")
                };

                let args = args.iter().format(", ");
                if let Some(filter) = filter {
                    write!(f, "{func_name}({args}) filter (where {filter}) over ")?;
                } else {
                    write!(f, "{func_name}({args}) over ")?;
                };

                if let ColExpr::Window(window_explain) = window.as_ref() {
                    let WindowExplain { .. } = window_explain.as_ref();
                    write!(f, "{window}")?;
                } else {
                    panic!("Expected Window expression in OVER clause");
                }
            }
            ColExpr::Parentheses(child_expr) => write!(f, "({child_expr})")?,
            ColExpr::Alias(expr, name) => {
                write!(f, "{expr} -> {}", name_to_smolstr(name))?;
            }
            ColExpr::Arithmetic(left, op, right) => write!(f, "{left} {op} {right}")?,
            ColExpr::Bool(left, op, right) => write!(f, "{left} {op} {right}")?,
            ColExpr::Unary(op, expr) => match op {
                Unary::IsNull => write!(f, "{expr} {op}")?,
                Unary::Exists => write!(f, "{op} {expr}")?,
                Unary::Not => {
                    if let ColExpr::Bool(_, Bool::And, _) = **expr {
                        write!(f, "{op} ({expr})")?;
                    } else {
                        write!(f, "{op} {expr}")?;
                    }
                }
            },
            ColExpr::Column(c, col_type) => write!(f, "{c}::{col_type}")?,
            ColExpr::Asterisk => write!(f, "*")?,
            ColExpr::Index(v, i) => write!(f, "{v}[{i}]")?,
            ColExpr::Cast(v, t) => write!(f, "{v}::{t}")?,
            ColExpr::Case(search_expr, when_blocks, else_expr) => {
                write!(f, "case ")?;
                if let Some(search_expr) = search_expr {
                    write!(f, "{search_expr} ")?;
                }
                for (cond_expr, res_expr) in when_blocks {
                    write!(f, "when {cond_expr} then {res_expr} ")?;
                }
                if let Some(else_expr) = else_expr {
                    write!(f, "else {else_expr} ")?;
                }
                write!(f, "end")?;
            }
            ColExpr::Concat(l, r) => write!(f, "{l} || {r}")?,
            ColExpr::ScalarFunction(name, args, feature, func_type, ..) => {
                let name = name_to_smolstr(name);
                let distinct = match feature {
                    Some(FunctionFeature::Distinct) => "distinct ",
                    _other => "",
                };
                let args = args.iter().format(", ");
                write!(f, "{name}({distinct}{args})::{func_type}")?;
            }
            ColExpr::Trim(kind, pattern, target) => match (kind, pattern) {
                (Some(k), Some(p)) => write!(f, "TRIM({} {p} from {target})", k.as_str())?,
                (Some(k), None) => write!(f, "TRIM({} from {target})", k.as_str())?,
                (None, Some(p)) => write!(f, "TRIM({p} from {target})")?,
                (None, None) => write!(f, "TRIM({target})")?,
            },
            ColExpr::Row(row) => write!(f, "{row}")?,
            ColExpr::None => {}
            ColExpr::Like(l, r, escape) => match escape {
                Some(e) => write!(f, "{l} LIKE {r} ESCAPE {e}")?,
                None => write!(f, "{l} LIKE {r}")?,
            },
        };

        Ok(())
    }
}

/// Helper struct for constructing ColExpr out of
/// given plan expression node.
#[derive(Debug)]
struct ColExprStack<'a> {
    /// Vec of (col_expr, corresponding_plan_id).
    inner: Vec<(ColExpr, NodeId)>,
    plan: &'a Plan,
}

impl<'a> ColExprStack<'a> {
    fn new(plan: &'a Plan) -> Self {
        Self {
            inner: Vec::new(),
            plan,
        }
    }
}

impl ColExprStack<'_> {
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

    fn scan_column_name(
        plan: &Plan,
        scan: NodeId,
        position: usize,
        reference: &Expression,
    ) -> Result<SmolStr, SbroadError> {
        let mut builder = SmolStrBuilder::new();

        if let Some(name) = plan.scan_name(scan, position)? {
            smolstr_builder_write_name(&mut builder, name);
            builder.push('.');
        }

        let alias = plan.get_alias_from_reference_node(reference)?;
        smolstr_builder_write_name(&mut builder, alias);

        Ok(builder.finish())
    }

    #[allow(clippy::too_many_lines)]
    fn new(
        plan: &Plan,
        subtree_top: NodeId,
        sq_ref_map: &SubQueryRefMap,
    ) -> Result<Self, SbroadError> {
        let mut stack: ColExprStack = ColExprStack::new(plan);
        let dft_post = PostOrder::new(|node| plan.nodes.expr_iter(node, false), EXPR_CAPACITY);

        for LevelNode(_, id) in dft_post.traverse_into_iter(subtree_top) {
            let current_node = plan.get_expression_node(id)?;

            match &current_node {
                Expression::Window(Window {
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
                Expression::Index(IndexExpr { .. }) => {
                    let which_expr = stack.pop_expr(Some(id)).into();
                    let child_expr = stack.pop_expr(Some(id)).into();

                    let index_expr: ColExpr = ColExpr::Index(child_expr, which_expr);
                    stack.push((index_expr, id));
                }
                Expression::Cast(Cast { to, .. }) => {
                    let child_expr = stack.pop_expr(Some(id)).into();

                    let cast_expr: ColExpr = ColExpr::Cast(child_expr, *to);
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
                    let count_asterisk_expr = ColExpr::Asterisk;
                    stack.push((count_asterisk_expr, id));
                }
                Expression::Reference(Reference { position, .. }) => {
                    let rel_id = plan.get_relational_from_reference_node(id)?;
                    let col_name =
                        ColExpr::scan_column_name(plan, rel_id, *position, &current_node)?;
                    let ref_expr = ColExpr::Column(col_name, current_node.calculate_type(plan)?);
                    stack.push((ref_expr, id));
                }
                Expression::SubQueryReference(SubQueryReference {
                    position, rel_id, ..
                }) => {
                    let col_name =
                        ColExpr::scan_column_name(plan, *rel_id, *position, &current_node)?;
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
                        ColExpr::Column(value.to_smolstr(), current_node.calculate_type(plan)?);
                    stack.push((expr, id));
                }
                Expression::Trim(Trim { kind, pattern, .. }) => {
                    let target = stack.pop_expr(Some(id));
                    let pattern = pattern.map(|_| Box::new(stack.pop_expr(Some(id))));
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
                    let alias_expr = ColExpr::Unary(*op, Box::new(child_expr));
                    stack.push((alias_expr, id));
                }
                Expression::Timestamp(timestamp) => {
                    let name = match timestamp {
                        Timestamp::Date => "Date",
                        Timestamp::DateTime(_) => "DateTime",
                    };
                    let expr =
                        ColExpr::Column(name.to_smolstr(), current_node.calculate_type(plan)?);
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

#[derive(Debug, PartialEq, Serialize, Clone)]
struct Projection {
    /// List of colums in sql query
    cols: Vec<ColExpr>,
}

#[derive(Debug, PartialEq, Serialize, Clone)]
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BoundTypeExplain::PrecedingUnbounded => write!(f, "unbounded preceding"),
            BoundTypeExplain::PrecedingOffset(expr) => write!(f, "{expr} preceding"),
            BoundTypeExplain::CurrentRow => write!(f, "current row"),
            BoundTypeExplain::FollowingOffset(expr) => write!(f, "{expr} following"),
            BoundTypeExplain::FollowingUnbounded => write!(f, "unbounded following"),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BoundExplain::Single(bound) => write!(f, "{bound}"),
            BoundExplain::Between(lower, upper) => write!(f, "between {lower} and {upper}"),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.ty, self.bound)
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
struct WindowExplain {
    partition: Vec<ColExpr>,
    ordering: Vec<OrderByPair>,
    frame: Option<FrameExplain>,
}

impl Display for WindowExplain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;

        if !self.partition.is_empty() {
            write!(f, "partition by ")?;
            let exprs = self.partition.iter().format(", ");
            write!(f, "({exprs}) ")?;
        }

        if !self.ordering.is_empty() {
            write!(f, "order by ")?;
            let exprs = self.ordering.iter().format(", ");
            write!(f, "({exprs}) ")?;
        }

        if let Some(frame) = &self.frame {
            write!(f, "{frame}")?;
        }

        write!(f, ")")?;

        Ok(())
    }
}

impl Projection {
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cols = self.cols.iter().format(", ");
        write!(f, "projection ({cols})")
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
struct GroupBy {
    /// List of colums in sql query
    gr_exprs: Vec<ColExpr>,
    output_cols: Vec<ColExpr>,
}

impl GroupBy {
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "group by ")?;

        let gr_exprs = &self.gr_exprs.iter().format(", ");
        write!(f, "({gr_exprs}) ")?;

        let output_cols = &self.output_cols.iter().format(", ");
        write!(f, "output: ({output_cols})")?;

        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
enum OrderByExpr {
    Expr { expr: ColExpr },
    Index { value: usize },
}

impl Display for OrderByExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderByExpr::Expr { expr } => write!(f, "{expr}"),
            OrderByExpr::Index { value } => write!(f, "{value}"),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
struct OrderByPair {
    expr: OrderByExpr,
    order_type: Option<OrderByType>,
}

impl Display for OrderByPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(order_type) = &self.order_type {
            write!(f, "{} {order_type}", self.expr)
        } else {
            write!(f, "{}", self.expr)
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
struct OrderBy {
    order_by_elements: Vec<OrderByPair>,
}

impl OrderBy {
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let elems = self.order_by_elements.iter().format(", ");
        write!(f, "order by ({elems})")
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
struct Update {
    /// List of columns in sql query
    table: SmolStr,
    update_statements: Vec<(SmolStr, SmolStr)>,
}

impl Update {
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
                    .map(|c| name_to_smolstr(&c.name))
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
                        name_to_smolstr(name)
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "update {}", name_to_smolstr(&self.table))?;

        let items = self
            .update_statements
            .iter()
            .map(|(col, alias)| format_smolstr!("{col} = {alias}"))
            .format("\n");

        write!(f, "{items}")?;

        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
struct Scan {
    /// Table name
    table: SmolStr,

    /// Table alias
    alias: Option<SmolStr>,

    /// Index used
    indexed_by: Option<SmolStr>,
}

impl Scan {
    fn new(table: SmolStr, alias: Option<SmolStr>, indexed_by: Option<SmolStr>) -> Self {
        Scan {
            table,
            alias,
            indexed_by,
        }
    }
}

impl Display for Scan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "scan {}", name_to_smolstr(&self.table))?;

        if let Some(alias) = &self.alias {
            write!(f, " -> {}", name_to_smolstr(alias))?;
        }

        if let Some(index) = &self.indexed_by {
            write!(f, " (indexed by {})", name_to_smolstr(index))?;
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
struct Ref {
    /// Reference to subquery/window index in `FullExplain` parts
    position: usize,
}

impl Ref {
    fn new(position: usize) -> Self {
        Ref { position }
    }
}

impl Display for Ref {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${}", self.position)
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
enum RowVal {
    ColumnExpr(ColExpr),
    SqRef(Ref),
}

impl Display for RowVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RowVal::ColumnExpr(c) => write!(f, "{c}")?,
            RowVal::SqRef(r) => write!(f, "{r}")?,
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
struct Row {
    /// List of sql values in `WHERE` cause
    cols: Vec<RowVal>,
}

impl Row {
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
                    let col = ColExpr::new(plan, expr_id, sq_ref_map)?;
                    row.add_col(RowVal::ColumnExpr(col));
                }
                Expression::SubQueryReference(SubQueryReference { rel_id, .. }) => {
                    let rel_node = plan.get_relation_node(*rel_id)?;
                    if let Relational::ScanSubQuery { .. } | Relational::Motion { .. } = rel_node {
                        if let Some(sq_offset) = sq_ref_map.get(rel_id) {
                            row.add_col(RowVal::SqRef(Ref::new(*sq_offset)));
                        } else {
                            return Err(SbroadError::NotFound(
                                Entity::SubQuery,
                                format_smolstr!("with id {rel_id}"),
                            ));
                        }
                    } else {
                        Err(SbroadError::Invalid(
                            Entity::Node,
                            Some(format_smolstr!(
                                "additional child ({rel_id}) is neither SQ nor Motion: {rel_node:?}."
                            )),
                        ))?;
                    }
                }
                _ => row.add_col(RowVal::ColumnExpr(col_expr)),
            }
        }

        Ok(row)
    }
}

impl Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cols = self.cols.iter().format(", ");
        write!(f, "ROW({cols})")
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
struct SubQuery {
    /// Subquery alias. For subquery in `WHERE` cause alias is `None`.
    alias: Option<SmolStr>,
}

impl SubQuery {
    fn new(alias: Option<SmolStr>) -> Self {
        SubQuery { alias }
    }
}

impl Display for SubQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "scan")?;
        if let Some(alias) = &self.alias {
            write!(f, " {}", name_to_smolstr(alias))?;
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
struct Motion {
    policy: MotionPolicy,
    program: Program,
}

impl Motion {
    fn new(policy: MotionPolicy, program: Program) -> Self {
        Motion { policy, program }
    }
}

impl Display for Motion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "motion [policy: {}, program: {}]",
            self.policy, self.program
        )
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
enum MotionPolicy {
    None,
    Full,
    Segment(MotionKey),
    Local,
    LocalSegment(MotionKey),
}

impl Display for MotionPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            MotionPolicy::None => write!(f, "none"),
            MotionPolicy::Full => write!(f, "full"),
            MotionPolicy::Segment(mk) => write!(f, "segment({mk})"),
            MotionPolicy::Local => write!(f, "local"),
            MotionPolicy::LocalSegment(mk) => write!(f, "local segment({mk})"),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
struct MotionKey {
    pub targets: Vec<Target>,
}

impl Display for MotionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let targets = self.targets.iter().format(", ");
        write!(f, "[{targets}]")
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
enum Target {
    Reference(SmolStr),
    Value(Value),
}

impl Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Target::Reference(s) => write!(f, "ref({})", name_to_smolstr(s)),
            Target::Value(v) => write!(f, "value({v})"),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
enum ExplainNode {
    Delete(SmolStr),
    Except,
    Intersect,
    GroupBy(GroupBy),
    OrderBy(OrderBy),
    Join(ColExpr, JoinKind),
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
}

impl Display for ExplainNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ExplainNode::Cte(name, r) => {
                write!(f, "scan cte {name}({r})", name = name_to_smolstr(name))?;
            }

            ExplainNode::Delete(name) => {
                write!(f, "delete {name}", name = name_to_smolstr(name))?;
            }
            ExplainNode::Except => write!(f, "except")?,
            ExplainNode::Join(col_expr, kind) => {
                match kind {
                    JoinKind::LeftOuter => write!(f, "{kind} ")?,
                    JoinKind::Inner => {}
                }
                write!(f, "join on {col_expr}")?;
            }
            ExplainNode::ValueRow(col_expr) => {
                write!(f, "value row (data={col_expr})")?;
            }
            ExplainNode::Value => write!(f, "values")?,
            ExplainNode::Insert(name, conflict) => {
                write!(
                    f,
                    "insert {name} on conflict: {conflict}",
                    name = name_to_smolstr(name)
                )?;
            }
            ExplainNode::Projection(projection) => write!(f, "{projection}")?,
            ExplainNode::GroupBy(group_by) => write!(f, "{group_by}")?,
            ExplainNode::OrderBy(order_by) => write!(f, "{order_by}")?,
            ExplainNode::Scan(scan) => write!(f, "{scan}")?,
            ExplainNode::Selection(col_expr) => write!(f, "selection {col_expr}")?,
            ExplainNode::Having(col_expr) => write!(f, "having {col_expr}")?,
            ExplainNode::Union => write!(f, "union")?,
            ExplainNode::UnionAll => write!(f, "union all")?,
            ExplainNode::Intersect => write!(f, "intersect")?,
            ExplainNode::Update(update) => write!(f, "{update}")?,
            ExplainNode::SubQuery(sub_query) => write!(f, "{sub_query}")?,
            ExplainNode::Motion(motion) => write!(f, "{motion}")?,
            ExplainNode::Limit(limit) => write!(f, "limit {limit}")?,
        };

        Ok(())
    }
}

/// Describe sql query (or subquery) as recursive type
#[derive(Debug, Serialize, Clone)]
struct ExplainTreePart {
    /// Current node of sql query
    current: ExplainNode,
    /// Children nodes of current sql node
    children: Vec<ExplainTreePart>,
}

impl PartialEq for ExplainTreePart {
    fn eq(&self, other: &Self) -> bool {
        self.current == other.current && self.children == other.children
    }
}

impl ExplainTreePart {
    fn do_fmt(&self, f: &mut fmt::Formatter<'_>, level: usize) -> fmt::Result {
        for _ in 0..level {
            write!(f, "{}", INDENT)?;
        }
        writeln!(f, "{}", self.current)?;

        for child in &self.children {
            child.do_fmt(f, level + 1)?;
        }

        Ok(())
    }
}

impl Display for ExplainTreePart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.do_fmt(f, 0)
    }
}

struct FullExplain {
    /// Main sql subtree
    main_query: ExplainTreePart,
    /// Independent sub-trees of main sql query (e.g. sub-queries in `WHERE` cause, CTEs, etc.)
    subqueries: OrderedMap<NodeId, ExplainTreePart, RepeatableState>,
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
        Buckets::Filtered(BucketSet::Exact(buckets_set)) => 'f: {
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
        Buckets::Filtered(BucketSet::Unknown(l, r)) => {
            if l != r {
                format!("unknown({l}..={r})")
            } else {
                format!("unknown({l})")
            }
        }
        Buckets::Any => "any".into(),
    }
}

impl Display for FullExplain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.main_query)?;
        for (pos, (_, sq)) in self.subqueries.iter().enumerate() {
            writeln!(f, "subquery ${pos}:")?;
            sq.do_fmt(f, 1)?;
        }
        for (pos, window) in self.windows.iter().enumerate() {
            writeln!(f, "window ${pos}:")?;
            write!(f, "{window}")?;
        }
        if !self.exec_options.is_empty() {
            writeln!(f, "execution options:")?;
            for opt in &self.exec_options {
                writeln!(f, "{}{} = {}", INDENT, opt.0, opt.1)?;
            }
        }
        if let Some(info) = &self.buckets_info {
            match info {
                BucketsInfo::Unknown => writeln!(f, "buckets = unknown")?,
                BucketsInfo::Calculated(calculated) => {
                    let repr = buckets_repr(&calculated.buckets, calculated.bucket_count);
                    // For buckets ANY and ALL there is no sense to handle in the
                    // output the case when bucket count is not exact.
                    match calculated.buckets {
                        Buckets::Any | Buckets::All => writeln!(f, "buckets = {repr}",)?,
                        _ if calculated.is_exact => writeln!(f, "buckets = {repr}",)?,
                        _ => writeln!(f, "buckets <= {repr}",)?,
                    }
                }
            }
        }

        Ok(())
    }
}

impl FullExplain {
    /// Retrieve SubQueryRefMap from relational node children
    fn get_sq_ref_map(
        stack: &mut Vec<ExplainTreePart>,
        known_subqueries: &mut OrderedMap<NodeId, ExplainTreePart, RepeatableState>,
        subqueries: &[NodeId],
    ) -> SubQueryRefMap {
        let mut sq_ref_map: SubQueryRefMap = HashMap::with_capacity(subqueries.len());

        // Note that subqueries are added to the stack in the `children` reversed order
        // because of the PostOrder traversal. That's why we apply `rev` here.
        for sq_id in subqueries.iter().rev() {
            let sq_node = stack
                .pop()
                .unwrap_or_else(|| panic!("Rel node failed to pop a sub-query."));
            if !known_subqueries.contains_key(sq_id) {
                known_subqueries.insert(*sq_id, sq_node);
            }
            let offset = known_subqueries
                .iter()
                .enumerate()
                .find(|(_, (sq_id_inner, _))| sq_id_inner == sq_id)
                .map(|(offset, _)| offset)
                .expect("sq should be found for explain");
            sq_ref_map.insert(*sq_id, offset);
        }

        sq_ref_map
    }

    #[allow(clippy::too_many_lines)]
    pub fn new(ir: &Plan, top_id: NodeId) -> Result<Self, SbroadError> {
        let exec_options = vec![
            (
                OptionKind::VdbeOpcodeMax,
                Value::Integer(ir.effective_options.sql_vdbe_opcode_max),
            ),
            (
                OptionKind::MotionRowMax,
                Value::Integer(ir.effective_options.sql_motion_row_max),
            ),
        ];

        let mut known_subqueries = OrderedMap::with_hasher(RepeatableState);
        let mut stack: Vec<ExplainTreePart> = Vec::new();

        let dft_post = PostOrder::new(|node| ir.nodes.rel_iter(node), REL_CAPACITY);
        for LevelNode(_, id) in dft_post.traverse_into_iter(top_id) {
            let node = ir.get_relation_node(id)?;
            let (current, children) = match &node {
                Relational::Intersect { .. } => {
                    let right = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Intersect node must have exactly two children".into(),
                        )
                    })?;
                    let left = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Intersect node must have exactly two children".into(),
                        )
                    })?;

                    (ExplainNode::Intersect, vec![left, right])
                }
                Relational::Except { .. } => {
                    let right = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Exception node must have exactly two children".into(),
                        )
                    })?;
                    let left = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Exception node must have exactly two children".into(),
                        )
                    })?;

                    (ExplainNode::Except, vec![left, right])
                }
                Relational::GroupBy(node::GroupBy {
                    gr_exprs,
                    output,
                    subqueries,
                    ..
                }) => {
                    let sq_ref_map =
                        FullExplain::get_sq_ref_map(&mut stack, &mut known_subqueries, subqueries);
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "GroupBy must have exactly one child".into(),
                        )
                    })?;
                    let group_by = GroupBy::new(ir, gr_exprs, *output, &sq_ref_map)?;

                    (ExplainNode::GroupBy(group_by), vec![child])
                }
                Relational::OrderBy(node::OrderBy {
                    order_by_elements,
                    subqueries,
                    ..
                }) => {
                    let sq_ref_map =
                        FullExplain::get_sq_ref_map(&mut stack, &mut known_subqueries, subqueries);
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "OrderBy must have exactly one child".into(),
                        )
                    })?;
                    let order_by = OrderBy::new(ir, order_by_elements, &sq_ref_map)?;

                    (ExplainNode::OrderBy(order_by), vec![child])
                }
                Relational::Projection(node::Projection {
                    output, subqueries, ..
                }) => {
                    let sq_ref_map =
                        FullExplain::get_sq_ref_map(&mut stack, &mut known_subqueries, subqueries);
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Projection must have exactly one child".into(),
                        )
                    })?;
                    let projection = Projection::new(ir, *output, &sq_ref_map)?;

                    (ExplainNode::Projection(projection), vec![child])
                }
                Relational::SelectWithoutScan(node::SelectWithoutScan {
                    output,
                    subqueries,
                    ..
                }) => {
                    let sq_ref_map =
                        FullExplain::get_sq_ref_map(&mut stack, &mut known_subqueries, subqueries);
                    let p = Projection::new(ir, *output, &sq_ref_map)?;

                    (ExplainNode::Projection(p), vec![])
                }
                Relational::ScanRelation(ScanRelation {
                    relation,
                    alias,
                    indexed_by,
                    ..
                }) => {
                    let s = Scan::new(relation.clone(), alias.clone(), indexed_by.clone());

                    (ExplainNode::Scan(s), vec![])
                }
                Relational::ScanCte(ScanCte { alias, child, .. }) => {
                    let child_tree = stack.pop().expect("CTE node must have exactly one child");
                    let existing_pos = known_subqueries
                        .iter()
                        .position(|(_, sq)| *sq == child_tree);
                    let pos = existing_pos.unwrap_or_else(|| {
                        known_subqueries.insert(*child, child_tree);
                        known_subqueries.len() - 1
                    });

                    (ExplainNode::Cte(alias.clone(), Ref::new(pos)), vec![])
                }
                Relational::Selection(Selection {
                    subqueries, filter, ..
                })
                | Relational::Having(Having {
                    subqueries, filter, ..
                }) => {
                    let sq_ref_map =
                        FullExplain::get_sq_ref_map(&mut stack, &mut known_subqueries, subqueries);
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Selection or Having must have exactly one child".into(),
                        )
                    })?;
                    let filter_id = *ir.undo.get_oldest(filter);
                    let selection = ColExpr::new(ir, filter_id, &sq_ref_map)?;
                    let explain_node = match &node {
                        Relational::Selection { .. } => ExplainNode::Selection(selection),
                        Relational::Having { .. } => ExplainNode::Having(selection),
                        _ => panic!("Expected Selection or Having node."),
                    };

                    (explain_node, vec![child])
                }
                u @ (Relational::UnionAll { .. } | Relational::Union { .. }) => {
                    let right = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Union node must have exactly two children".into(),
                        )
                    })?;
                    let left = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Union node must have exactly two children".into(),
                        )
                    })?;

                    let kind = match u {
                        Relational::Union(..) => ExplainNode::Union,
                        _other => ExplainNode::UnionAll,
                    };

                    (kind, vec![left, right])
                }
                Relational::ScanSubQuery(ScanSubQuery { alias, .. }) => {
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "ScanSubQuery node must have exactly one child".into(),
                        )
                    })?;
                    let subquery = SubQuery::new(alias.clone());

                    (ExplainNode::SubQuery(subquery), vec![child])
                }
                Relational::Motion(MotionRel {
                    child: child_id,
                    policy,
                    program,
                    ..
                }) => {
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Motion node must have exactly one child".into(),
                        )
                    })?;

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
                                        .to_smolstr();

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

                    let motion = Motion::new(p, program.clone());

                    (ExplainNode::Motion(motion), vec![child])
                }
                Relational::Join(Join {
                    subqueries,
                    condition,
                    kind,
                    ..
                }) => {
                    let sq_ref_map =
                        FullExplain::get_sq_ref_map(&mut stack, &mut known_subqueries, subqueries);
                    let right = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Join node must have exactly two children".into(),
                        )
                    })?;
                    let left = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Join node must have exactly two children".into(),
                        )
                    })?;

                    let condition = ColExpr::new(ir, *condition, &sq_ref_map)?;
                    let join = ExplainNode::Join(condition, *kind);

                    (join, vec![left, right])
                }
                Relational::ValuesRow(ValuesRow {
                    data, subqueries, ..
                }) => {
                    let sq_ref_map =
                        FullExplain::get_sq_ref_map(&mut stack, &mut known_subqueries, subqueries);
                    let row = ColExpr::new(ir, *data, &sq_ref_map)?;

                    (ExplainNode::ValueRow(row), vec![])
                }
                Relational::Values(Values { children, .. }) => {
                    let Some(start_pos) = stack.len().checked_sub(children.len()) else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "Insert node has insufficient row values.".into(),
                        ));
                    };
                    let children = stack[start_pos..].to_vec();
                    stack.truncate(start_pos);

                    (ExplainNode::Value, children)
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
                    let insert = ExplainNode::Insert(relation.clone(), *conflict_strategy);

                    (insert, vec![values])
                }
                Relational::Update { .. } => {
                    let values = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Insert node failed to pop a value row.".into(),
                        )
                    })?;
                    let update = ExplainNode::Update(Update::new(ir, id)?);

                    (update, vec![values])
                }
                Relational::Delete(Delete {
                    relation, output, ..
                }) => {
                    let mut children = vec![];
                    if output.is_some() {
                        let values = stack.pop().ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(
                                "Delete node failed to pop a value row.".into(),
                            )
                        })?;

                        children.push(values);
                    }
                    let delete = ExplainNode::Delete(relation.clone());

                    (delete, children)
                }
                Relational::Limit(Limit { limit, .. }) => {
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Limit node must have exactly one child".into(),
                        )
                    })?;

                    (ExplainNode::Limit(*limit), vec![child])
                }
            };

            stack.push(ExplainTreePart { current, children });
        }

        let main_query = stack
            .pop()
            .ok_or_else(|| SbroadError::NotFound(Entity::Node, "that is explain top".into()))?;

        let result = Self {
            main_query,
            subqueries: known_subqueries,
            windows: Vec::new(),
            exec_options,
            buckets_info: None,
        };

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

impl<C: Router> ExecutingQuery<'_, C> {
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
