use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Display, Write};

use itertools::Itertools;
use smallvec::SmallVec;
use smol_str::{format_smolstr, SmolStr, SmolStrBuilder, ToSmolStr};

use crate::errors::{Entity, SbroadError};
use crate::ir::bucket::{BucketSet, Buckets};
use crate::ir::expression::TrimKind;
use crate::ir::node::{
    Alias, ArithmeticExpr, BoolExpr, Case, Cast, Constant, Delete, Having, IndexExpr, Insert, Join,
    Motion as MotionRel, NodeId, Reference, Row as RowExpr, ScalarFunction, ScanCte, ScanRelation,
    ScanSubQuery, Selection, SubQueryReference, Timestamp, Trim, UnaryExpr, Update as UpdateRel,
    Values, ValuesRow,
};
use crate::ir::operator::{
    Bool, ConflictStrategy, JoinKind, OrderByElement, OrderByEntity, OrderByType,
};
use crate::ir::options::OptionKind;
use crate::ir::transformation::redistribution::{
    MotionKey as IrMotionKey, MotionPolicy as IrMotionPolicy, Program, Target as IrTarget,
};
use crate::ir::{node, ExplainOptions, Plan};
use crate::utils::OrderedMap;

use super::expression::FunctionFeature;
use super::helpers::RepeatableState;
use super::node::expression::Expression;
use super::node::relational::Relational;
use super::node::{Bound, BoundType, Frame, FrameType, Limit, Over, Window};
use super::operator::Unary;
use super::tree::traversal::{LevelNode, PostOrder, EXPR_CAPACITY, REL_CAPACITY};
use super::types::{CastType, DerivedType};
use super::value::Value;

/// We use this buffer to check if the textual representation
/// is short enough to be printed as a single line.
#[derive(Default)]
struct TinyFmtBuffer(SmallVec<[u8; 46]>);

impl TinyFmtBuffer {
    /// Does the buffer contain `'\n'`?
    fn has_newline(&self) -> bool {
        self.0.contains(&b'\n')
    }

    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap()
    }
}

impl Display for TinyFmtBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl fmt::Write for TinyFmtBuffer {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let this = &mut self.0;
        let new_len = this.len().checked_add(s.len()).ok_or(fmt::Error)?;
        if new_len > this.capacity() {
            return Err(fmt::Error);
        }
        this.extend_from_slice(s.as_bytes());

        Ok(())
    }
}

/// Transform a writer into an indented writer. This effect is additive.
fn indent<'a, D>(f: &'a mut D) -> indenter::Indented<'a, D> {
    const INDENT: &str = "  ";
    indenter::indented(f).with_str(INDENT)
}

fn name_requires_quotes(s: &str) -> bool {
    !s.chars()
        .all(|c| c == '_' || c.is_ascii_digit() || c.is_ascii_lowercase())
}

/// Print the object name in double quotes **if necessary**.
/// We optimize for readability, so the function won't
/// add the double qoutes unless they're required.
fn properly_quoted_name(name: &str) -> SmolStr {
    let mut builder = SmolStrBuilder::new();
    write_name(&mut builder, name).expect("infallible");
    builder.finish()
}

/// Write the object name in double quotes **if necessary**.
fn write_name(writer: &mut impl fmt::Write, name: &str) -> fmt::Result {
    let need_quotes = name_requires_quotes(name);
    if need_quotes {
        writer.write_char('"')?;
    }
    // TODO: we should to escape double quotes as well.
    writer.write_str(name)?;
    if need_quotes {
        writer.write_char('"')?;
    }

    Ok(())
}

/// The concept of list separator in short and long forms.
trait ListSep {
    /// Written between elements of a short list.
    const SHORT: &str;
    /// Written after the last element of a short list.
    const SHORT_FINAL: &str = "";
    /// Written between elements of a long list.
    const LONG: &str;
    /// Written after the last element of a long list.
    const LONG_FINAL: &str = "\n";
}

/// A separator for regular lists.
struct CommaSep;
impl ListSep for CommaSep {
    const SHORT: &str = ", ";
    const LONG: &str = ",\n";
}

/// A separator for AND chains.
struct AndSep;
impl ListSep for AndSep {
    const SHORT: &str = " and ";
    const LONG: &str = "\nand ";
}

/// A separator for complex operators, e.g. `CASE`.
struct BlankSep;
impl ListSep for BlankSep {
    const SHORT: &str = " ";
    const SHORT_FINAL: &str = " ";
    const LONG: &str = "\n";
    const LONG_FINAL: &str = "\n";
}

/// Write all items separated by [`ListSep::LONG`].
///
/// If the list is not empty, it will start and end with newlines;
/// meaning that we should always print some kind of open & close
/// symbols (e.g. parentheses) before calling this function.
fn write_long_list<Sep: ListSep>(
    writer: &mut impl fmt::Write,
    items: impl IntoIterator<Item: Display>,
) -> fmt::Result {
    let mut items = items.into_iter().peekable();
    if items.peek().is_none() {
        return Ok(());
    }

    let mut writer = indent(writer);

    // All non-empty long lists should start a new line.
    // This helps with e.g. `projection (...)`.
    writeln!(writer)?;

    while let Some(item) = items.next() {
        let has_next = items.peek().is_some();
        let sep = if has_next { Sep::LONG } else { Sep::LONG_FINAL };
        write!(writer, "{item}{sep}")?;
    }

    Ok(())
}

/// Write all items separated by [`ListSep::SHORT`].
fn write_short_list<Sep: ListSep>(
    writer: &mut impl fmt::Write,
    items: impl IntoIterator<Item: Display>,
) -> fmt::Result {
    let formatted = items.into_iter().format(Sep::SHORT);
    write!(writer, "{formatted}")?;
    write!(writer, "{}", Sep::SHORT_FINAL)?;

    Ok(())
}

/// Write all items separated by [`ListSep::LONG`] or [`ListSep::SHORT`]
/// depending on the **unspecified heuristics**.
///
/// If the list is not empty, it **may** start and end with newlines;
/// meaning that we should always print some kind of open & close
/// symbols (e.g. parentheses) before calling this function.
fn write_list<Sep: ListSep>(
    writer: &mut impl fmt::Write,
    items: impl IntoIterator<Item: Display>,
    should_fmt: bool,
) -> fmt::Result {
    if !should_fmt {
        return write_short_list::<Sep>(writer, items);
    }

    let mut items = items.into_iter().peekable();

    // Lists containing up to FEW_ITEMS are considered
    // short enough to be printed on a single line.
    const FEW_ITEMS: usize = 6;
    let mut stage = SmallVec::<[_; FEW_ITEMS]>::new();
    while stage.len() < stage.capacity() {
        match items.next() {
            Some(item) => stage.push(item),
            None => break,
        }
    }

    match items.peek() {
        // If there's nothing left, try printing as a short list.
        None => {
            let mut buffer = TinyFmtBuffer::default();
            let ok = write_short_list::<Sep>(&mut buffer, &stage).is_ok();

            // If the "short" list contains a newline, it's long!
            if ok && !buffer.has_newline() {
                write!(writer, "{buffer}")
            } else {
                write_long_list::<Sep>(writer, stage)
            }
        }
        // Otherwise, print as a long list.
        Some(_) => {
            let combined = stage.into_iter().chain(items);
            write_long_list::<Sep>(writer, combined)
        }
    }
}

struct FmtSmartParens<T>(T, bool);

impl<T: Display> fmt::Display for FmtSmartParens<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(item, should_fmt) = self;

        if !should_fmt {
            return write!(f, "({item})");
        }

        let mut buffer = TinyFmtBuffer::default();
        let ok = write!(buffer, "{item}").is_ok();

        write!(f, "(")?;

        // If a "small" item contains a newline, it's large!
        if ok && !buffer.has_newline() {
            write!(f, "{buffer}")?;
        } else {
            writeln!(f)?;
            writeln!(indent(f), "{item}")?;
        }

        write!(f, ")")?;

        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
enum ColExpr {
    // Print expressions wrt precedence.
    Parentheses(Box<ColExpr>, bool),

    // This one is hard to explain...
    Row(Row),

    // Column-ish things.
    Alias(Box<ColExpr>, SmolStr),
    Column(Option<SmolStr>, SmolStr, DerivedType),
    Const(SmolStr, DerivedType),
    Asterisk,

    // Regular operators.
    PrefixOp(SmolStr, Box<ColExpr>),
    InfixOp(Box<ColExpr>, SmolStr, Box<ColExpr>),
    SuffixOp(Box<ColExpr>, SmolStr),

    // Improved readability for long AND chains.
    ListOfAnds(VecDeque<ColExpr>, bool),

    // Unusual or complex operators & functions.
    Cast(Box<ColExpr>, CastType),
    Index(Box<ColExpr>, Box<ColExpr>),
    Trim(Option<TrimKind>, Option<Box<ColExpr>>, Box<ColExpr>),
    Like(Box<ColExpr>, Box<ColExpr>, Option<Box<ColExpr>>),
    Case(
        Option<Box<ColExpr>>,
        Vec<(ColExpr, ColExpr)>,
        Option<Box<ColExpr>>,
        bool,
    ),

    // Regular function calls.
    ScalarFunction(
        SmolStr,
        Vec<ColExpr>,
        Option<FunctionFeature>,
        DerivedType,
        bool,
    ),

    // Window functions.
    Window(Box<WindowExplain>),
    Over(Box<ColExpr>, Option<Box<ColExpr>>, Box<ColExpr>, bool),
}

impl Display for ColExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColExpr::Parentheses(expr, should_fmt) => {
                write!(f, "{}", FmtSmartParens(expr, *should_fmt))?
            }
            ColExpr::Row(row) => write!(f, "{row}")?,

            // Column-ish things.
            ColExpr::Alias(expr, alias_name) => match expr.as_ref() {
                // If the unqualified column name matches the alias, drop the alias.
                ColExpr::Column(None, col_name, _) if col_name == alias_name => {
                    write!(f, "{expr}")?;
                }
                _otherwise => {
                    write!(
                        f,
                        "{expr} -> {alias}",
                        alias = properly_quoted_name(alias_name)
                    )?;
                }
            },
            ColExpr::Column(tbl_name, col_name, col_typ) => match tbl_name {
                Some(tbl_name) => write!(
                    f,
                    "{tbl_name}.{col_name}::{col_typ}",
                    tbl_name = properly_quoted_name(tbl_name),
                    col_name = properly_quoted_name(col_name),
                )?,
                None => write!(
                    f,
                    "{col_name}::{col_typ}",
                    col_name = properly_quoted_name(col_name),
                )?,
            },
            ColExpr::Const(value, typ) => write!(f, "{value}::{typ}")?,
            ColExpr::Asterisk => write!(f, "*")?,

            // Regular operators.
            ColExpr::PrefixOp(op, val) => write!(f, "{op} {val}")?,
            ColExpr::InfixOp(lhs, op, rhs) => write!(f, "{lhs} {op} {rhs}")?,
            ColExpr::SuffixOp(val, op) => write!(f, "{val} {op}")?,

            // Improved readability for long AND chains.
            ColExpr::ListOfAnds(items, should_fmt) => {
                write!(f, "(")?;
                write_list::<AndSep>(f, items, *should_fmt)?;
                write!(f, ")")?;
            }

            // Unusal or complex operators.
            ColExpr::Index(v, i) => write!(f, "{v}[{i}]")?,
            ColExpr::Cast(v, t) => write!(f, "{v}::{t}")?,
            ColExpr::Trim(kind, pattern, target) => match (kind, pattern) {
                (Some(k), Some(p)) => write!(f, "TRIM({} {p} from {target})", k.as_str())?,
                (Some(k), None) => write!(f, "TRIM({} from {target})", k.as_str())?,
                (None, Some(p)) => write!(f, "TRIM({p} from {target})")?,
                (None, None) => write!(f, "TRIM({target})")?,
            },
            ColExpr::Like(l, r, escape) => match escape {
                Some(e) => write!(f, "{l} LIKE {r} ESCAPE {e}")?,
                None => write!(f, "{l} LIKE {r}")?,
            },
            ColExpr::Case(search_expr, when_exprs, else_expr, should_fmt) => {
                write!(f, "case ")?;
                if let Some(search_expr) = search_expr {
                    write!(f, "{search_expr} ")?;
                }
                let when_items = when_exprs
                    .iter()
                    .map(|(cond, res)| format!("when {cond} then {res}"));
                let else_item = else_expr.iter().map(|res| format!("else {res}"));
                let items = when_items.chain(else_item);
                write_list::<BlankSep>(f, items, *should_fmt)?;
                write!(f, "end")?;
            }

            // Regular function calls.
            ColExpr::ScalarFunction(name, args, feature, ret_type, should_fmt) => {
                let name = properly_quoted_name(name);
                let distinct = match feature {
                    Some(FunctionFeature::Distinct) => "distinct ",
                    _other => "",
                };
                write!(f, "{name}({distinct}")?;
                write_list::<CommaSep>(f, args, *should_fmt)?;
                write!(f, ")::{ret_type}")?;
            }

            // Window functions.
            ColExpr::Window(window) => write!(f, "{window}")?,
            ColExpr::Over(stable_func, filter, window, should_fmt) => {
                let ColExpr::ScalarFunction(func_name, args, ..) = stable_func.as_ref() else {
                    panic!("Expected ScalarFunction expression before OVER clause");
                };
                let ColExpr::Window(_) = window.as_ref() else {
                    panic!("Expected Window expression in OVER clause");
                };
                write!(f, "{func_name}(")?;
                write_list::<CommaSep>(f, args, *should_fmt)?;
                write!(f, ") ")?;
                if let Some(filter) = filter {
                    write!(f, "filter (where {filter}) ")?;
                }
                write!(f, "over {window}")?;
            }
        };

        Ok(())
    }
}

/// Helper struct for constructing ColExpr out of
/// given plan expression node.
#[derive(Debug)]
struct ColExprStack<'a> {
    plan: &'a Plan,
    inner: Vec<(ColExpr, NodeId)>,
    should_fmt: bool,
}

impl<'a> ColExprStack<'a> {
    fn new(plan: &'a Plan, should_fmt: bool) -> Self {
        Self {
            plan,
            inner: Vec::new(),
            should_fmt,
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
            Some(top_plan_id) => {
                expr.covered_with_parentheses(self.plan, top_plan_id, plan_id, self.should_fmt)
            }
        }
    }
}

impl ColExpr {
    fn covered_with_parentheses(
        self,
        plan: &Plan,
        top_plan_id: NodeId,
        self_plan_id: NodeId,
        should_fmt: bool,
    ) -> Self {
        let should_cover =
            // XXX: List of ANDs prints its own parentheses.
            // Keep in sync with `impl Display for ColExpr`.
            !matches!(self, ColExpr::ListOfAnds(..))
            // Other nodes follow the common logic.
            && plan
            .should_cover_with_parentheses(top_plan_id, self_plan_id)
            .expect("top and child nodes should exist");

        if should_cover {
            ColExpr::Parentheses(self.into(), should_fmt)
        } else {
            self
        }
    }

    fn join_ands(lhs: Self, rhs: Self, should_fmt: bool) -> Self {
        match (lhs, rhs) {
            (ColExpr::ListOfAnds(mut lhs, _), ColExpr::ListOfAnds(rhs, _)) => {
                lhs.extend(rhs);
                ColExpr::ListOfAnds(lhs, should_fmt)
            }
            (ColExpr::ListOfAnds(mut lhs, _), rhs) => {
                lhs.push_back(rhs);
                ColExpr::ListOfAnds(lhs, should_fmt)
            }
            (lhs, ColExpr::ListOfAnds(mut rhs, _)) => {
                rhs.push_front(lhs);
                ColExpr::ListOfAnds(rhs, should_fmt)
            }
            (lhs, rhs) => {
                let items = [lhs, rhs].into();
                ColExpr::ListOfAnds(items, should_fmt)
            }
        }
    }

    fn scan_column_name(
        plan: &Plan,
        scan: NodeId,
        position: usize,
        reference: &Expression,
    ) -> Result<(Option<SmolStr>, SmolStr), SbroadError> {
        let tbl_name = plan.scan_name(scan, position)?;
        let col_name = plan.get_alias_from_reference_node(reference)?;

        Ok((tbl_name.map(|s| s.into()), col_name.into()))
    }

    #[allow(clippy::too_many_lines)]
    fn new(
        plan: &Plan,
        subtree_top: NodeId,
        sq_ref_map: &SubQueryRefMap,
        should_fmt: bool,
    ) -> Result<Self, SbroadError> {
        let mut stack = ColExprStack::new(plan, should_fmt);
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
                                order_type: o_elem.order_type,
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
                        should_fmt,
                    };

                    let expr = ColExpr::Window(window.into());
                    stack.push((expr, id));
                }
                Expression::Over(Over { filter, .. }) => {
                    let window = stack.pop_expr(Some(id)).into();
                    let filter = filter.map(|_| stack.pop_expr(Some(id)).into());
                    let stable_func = stack.pop_expr(Some(id)).into();
                    let expr = ColExpr::Over(stable_func, filter, window, should_fmt);
                    stack.push((expr, id));
                }
                Expression::Index(IndexExpr { .. }) => {
                    let which_expr = stack.pop_expr(Some(id)).into();
                    let child_expr = stack.pop_expr(Some(id)).into();
                    let expr = ColExpr::Index(child_expr, which_expr);
                    stack.push((expr, id));
                }
                Expression::Cast(Cast { to, .. }) => {
                    let child_expr = stack.pop_expr(Some(id)).into();
                    let expr = ColExpr::Cast(child_expr, *to);
                    stack.push((expr, id));
                }
                Expression::Case(Case {
                    search_expr,
                    when_blocks,
                    else_expr,
                }) => {
                    let else_expr = else_expr.map(|_| stack.pop_expr(Some(id)).into());

                    let mut match_exprs = Vec::with_capacity(when_blocks.len());
                    for _ in when_blocks {
                        let res_expr = stack.pop_expr(Some(id));
                        let cond_expr = stack.pop_expr(Some(id));
                        match_exprs.push((cond_expr, res_expr));
                    }
                    match_exprs.reverse();

                    let search_expr = search_expr.map(|_| stack.pop_expr(Some(id)).into());

                    let expr = ColExpr::Case(search_expr, match_exprs, else_expr, should_fmt);
                    stack.push((expr, id));
                }
                Expression::CountAsterisk(_) => {
                    let expr = ColExpr::Asterisk;
                    stack.push((expr, id));
                }
                Expression::Reference(Reference { position, .. }) => {
                    let rel_id = plan.get_relational_from_reference_node(id)?;
                    let (tbl_name, col_name) =
                        ColExpr::scan_column_name(plan, rel_id, *position, &current_node)?;
                    let expr =
                        ColExpr::Column(tbl_name, col_name, current_node.calculate_type(plan)?);
                    stack.push((expr, id));
                }
                Expression::SubQueryReference(SubQueryReference {
                    position, rel_id, ..
                }) => {
                    let (tbl_name, col_name) =
                        ColExpr::scan_column_name(plan, *rel_id, *position, &current_node)?;
                    let expr =
                        ColExpr::Column(tbl_name, col_name, current_node.calculate_type(plan)?);
                    stack.push((expr, id));
                }
                Expression::Like { .. } => {
                    let escape = Some(stack.pop_expr(Some(id)).into());
                    let right = stack.pop_expr(Some(id)).into();
                    let left = stack.pop_expr(Some(id)).into();
                    let expr = ColExpr::Like(left, right, escape);
                    stack.push((expr, id));
                }
                Expression::Constant(Constant { value }) => {
                    let expr =
                        ColExpr::Const(value.to_smolstr(), current_node.calculate_type(plan)?);
                    stack.push((expr, id));
                }
                Expression::Trim(Trim { kind, pattern, .. }) => {
                    let target = stack.pop_expr(Some(id)).into();
                    let pattern = pattern.map(|_| stack.pop_expr(Some(id)).into());
                    let expr = ColExpr::Trim(kind.clone(), pattern, target);
                    stack.push((expr, id));
                }
                Expression::ScalarFunction(ScalarFunction {
                    name,
                    children,
                    feature,
                    func_type,
                    ..
                }) => {
                    let mut args = Vec::with_capacity(children.len());
                    for _ in children {
                        args.push(stack.pop_expr(Some(id)));
                    }
                    args.reverse();
                    let func_expr = ColExpr::ScalarFunction(
                        name.clone(),
                        args,
                        feature.clone(),
                        *func_type,
                        should_fmt,
                    );
                    stack.push((func_expr, id));
                }
                Expression::Row(RowExpr { list, .. }) => {
                    let mut row = ColExprStack::new(plan, should_fmt);
                    for _ in list {
                        row.push(stack.pop());
                    }
                    row.inner.reverse();
                    let row = Row::from_col_expr_stack(plan, row, sq_ref_map, should_fmt)?;
                    let row_expr = ColExpr::Row(row);
                    stack.push((row_expr, id));
                }
                Expression::Alias(Alias { name, .. }) => {
                    let expr = stack.pop_expr(Some(id)).into();
                    let alias_expr = ColExpr::Alias(expr, name.clone());
                    stack.push((alias_expr, id));
                }
                Expression::Concat(_) => {
                    let rhs = stack.pop_expr(Some(id)).into();
                    let lhs = stack.pop_expr(Some(id)).into();
                    let expr = ColExpr::InfixOp(lhs, "||".to_smolstr(), rhs);
                    stack.push((expr, id));
                }
                Expression::Arithmetic(ArithmeticExpr { op, .. }) => {
                    let rhs = stack.pop_expr(Some(id)).into();
                    let lhs = stack.pop_expr(Some(id)).into();
                    let expr = ColExpr::InfixOp(lhs, op.to_smolstr(), rhs);
                    stack.push((expr, id));
                }
                Expression::Bool(BoolExpr { op, .. }) => {
                    let rhs = stack.pop_expr(Some(id));
                    let lhs = stack.pop_expr(Some(id));
                    let expr = if *op == Bool::And {
                        ColExpr::join_ands(lhs, rhs, should_fmt)
                    } else {
                        ColExpr::InfixOp(lhs.into(), op.to_smolstr(), rhs.into())
                    };
                    stack.push((expr, id));
                }
                Expression::Unary(UnaryExpr { op, .. }) => {
                    let child = stack.pop_expr(Some(id)).into();
                    let op_name = op.to_smolstr();
                    let expr = match op {
                        Unary::Not | Unary::Exists => ColExpr::PrefixOp(op_name, child),
                        Unary::IsNull => ColExpr::SuffixOp(child, op_name),
                    };
                    stack.push((expr, id));
                }
                Expression::Timestamp(timestamp) => {
                    let name = match timestamp {
                        Timestamp::Date => "Date",
                        Timestamp::DateTime(_) => "DateTime",
                    }
                    .to_smolstr();

                    let expr = ColExpr::Const(name, current_node.calculate_type(plan)?);
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

#[derive(Debug, PartialEq, Clone)]
struct Projection {
    cols: Vec<ColExpr>,
    should_fmt: bool,
}

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone)]
struct WindowExplain {
    partition: Vec<ColExpr>,
    ordering: Vec<OrderByPair>,
    frame: Option<FrameExplain>,
    should_fmt: bool,
}

impl Display for WindowExplain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;

        if !self.partition.is_empty() {
            write!(f, "partition by (")?;
            write_list::<CommaSep>(f, &self.partition, self.should_fmt)?;
            write!(f, ") ")?;
        }

        if !self.ordering.is_empty() {
            write!(f, "order by (")?;
            write_list::<CommaSep>(f, &self.ordering, self.should_fmt)?;
            write!(f, ") ")?;
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
        should_fmt: bool,
    ) -> Result<Self, SbroadError> {
        let output = plan.get_expression_node(output_id)?;
        let col_list = output.get_row_list()?;
        let mut result = Projection {
            cols: Vec::with_capacity(col_list.len()),
            should_fmt,
        };

        for col_id in col_list {
            let col = ColExpr::new(plan, *col_id, sq_ref_map, should_fmt)?;
            result.cols.push(col);
        }

        Ok(result)
    }
}

impl Display for Projection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "projection (")?;
        write_list::<CommaSep>(f, &self.cols, self.should_fmt)?;
        write!(f, ")")?;

        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
struct GroupBy {
    /// List of colums in sql query
    gr_exprs: Vec<ColExpr>,
    output_cols: Vec<ColExpr>,
    should_fmt: bool,
}

impl GroupBy {
    fn new(
        plan: &Plan,
        gr_exprs: &Vec<NodeId>,
        output_id: NodeId,
        sq_ref_map: &SubQueryRefMap,
        should_fmt: bool,
    ) -> Result<Self, SbroadError> {
        let mut result = GroupBy {
            gr_exprs: vec![],
            output_cols: vec![],
            should_fmt,
        };

        for col_node_id in gr_exprs {
            let col = ColExpr::new(plan, *col_node_id, sq_ref_map, should_fmt)?;
            result.gr_exprs.push(col);
        }
        let alias_list = plan.get_expression_node(output_id)?;
        for col_node_id in alias_list.get_row_list()? {
            let col = ColExpr::new(plan, *col_node_id, sq_ref_map, should_fmt)?;
            result.output_cols.push(col);
        }
        Ok(result)
    }
}

impl Display for GroupBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "group by (")?;
        write_list::<CommaSep>(f, &self.gr_exprs, self.should_fmt)?;
        write!(f, ") output (")?;
        write_list::<CommaSep>(f, &self.output_cols, self.should_fmt)?;
        write!(f, ")")?;

        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone)]
struct OrderBy {
    cols: Vec<OrderByPair>,
    should_fmt: bool,
}

impl OrderBy {
    fn new(
        plan: &Plan,
        cols: &Vec<OrderByElement>,
        sq_ref_map: &SubQueryRefMap,
        should_fmt: bool,
    ) -> Result<Self, SbroadError> {
        let mut result = OrderBy {
            cols: vec![],
            should_fmt,
        };

        for item in cols {
            let expr = match item.entity {
                OrderByEntity::Expression { expr_id } => OrderByExpr::Expr {
                    expr: ColExpr::new(plan, expr_id, sq_ref_map, should_fmt)?,
                },
                OrderByEntity::Index { value } => OrderByExpr::Index { value },
            };
            result.cols.push(OrderByPair {
                expr,
                order_type: item.order_type,
            });
        }

        Ok(result)
    }
}

impl Display for OrderBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "order by (")?;
        write_list::<CommaSep>(f, &self.cols, self.should_fmt)?;
        write!(f, ")")?;

        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
struct Update {
    table: SmolStr,
    update_statements: Vec<(SmolStr, SmolStr)>,
    should_fmt: bool,
}

impl Update {
    fn new(plan: &Plan, update_id: NodeId, should_fmt: bool) -> Result<Self, SbroadError> {
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
                    .map(|c| properly_quoted_name(&c.name))
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
                        properly_quoted_name(name)
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
                should_fmt,
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
        let name = properly_quoted_name(&self.table);
        let items = self
            .update_statements
            .iter()
            .map(|(col, alias)| format_smolstr!("{col} = {alias}"));

        write!(f, "update {name} (")?;
        write_list::<CommaSep>(f, items, self.should_fmt)?;
        write!(f, ")")?;

        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
struct Scan {
    table: SmolStr,
    alias: Option<SmolStr>,
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
        write!(f, "scan {}", properly_quoted_name(&self.table))?;

        if let Some(alias) = &self.alias {
            if *alias != self.table {
                write!(f, " -> {}", properly_quoted_name(alias))?;
            }
        }

        if let Some(index) = &self.indexed_by {
            write!(f, " (indexed by {})", properly_quoted_name(index))?;
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone)]
struct Row {
    cols: Vec<RowVal>,
    should_fmt: bool,
}

impl Row {
    fn add_col(&mut self, row: RowVal) {
        self.cols.push(row);
    }

    fn from_col_expr_stack(
        plan: &Plan,
        col_expr_stack: ColExprStack,
        sq_ref_map: &SubQueryRefMap,
        should_fmt: bool,
    ) -> Result<Self, SbroadError> {
        let mut row = Row {
            cols: vec![],
            should_fmt,
        };

        for (col_expr, expr_id) in col_expr_stack.inner {
            let current_node = plan.get_expression_node(expr_id)?;

            match &current_node {
                Expression::Reference { .. } => {
                    let col = ColExpr::new(plan, expr_id, sq_ref_map, should_fmt)?;
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
        write!(f, "ROW(")?;
        write_list::<CommaSep>(f, &self.cols, self.should_fmt)?;
        write!(f, ")")?;

        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
struct SubQuery {
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
            write!(f, " {}", properly_quoted_name(alias))?;
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone)]
enum MotionPolicy {
    None,
    Full,
    Segment(MotionKey),
    Local,
    LocalSegment(MotionKey),
}

impl Display for MotionPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MotionPolicy::None => write!(f, "none"),
            MotionPolicy::Full => write!(f, "full"),
            MotionPolicy::Segment(mk) => write!(f, "segment({mk})"),
            MotionPolicy::Local => write!(f, "local"),
            MotionPolicy::LocalSegment(mk) => write!(f, "local segment({mk})"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
struct MotionKey {
    pub targets: Vec<Target>,
}

impl Display for MotionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let targets = self.targets.iter().format(", ");
        write!(f, "[{targets}]")
    }
}

#[derive(Debug, PartialEq, Clone)]
enum Target {
    Reference(SmolStr),
    Value(Value),
}

impl Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Target::Reference(s) => write!(f, "ref({})", properly_quoted_name(s)),
            Target::Value(v) => write!(f, "value({v})"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
enum ExplainNode {
    // Short items.
    Delete(SmolStr),
    Insert(SmolStr, ConflictStrategy),
    Cte(SmolStr, Ref),
    Scan(Scan),
    SubQuery(SubQuery),
    Motion(Motion),
    Limit(u64),

    // Potentially long expression lists.
    Projection(Projection),
    GroupBy(GroupBy),
    OrderBy(OrderBy),
    Update(Update),

    // A single but potentially large expression.
    Join(ColExpr, JoinKind, bool),
    Selection(ColExpr, bool),
    Having(ColExpr, bool),
    ValueRow(ColExpr),

    // Boring.
    Value,
    Union,
    UnionAll,
    Intersect,
    Except,
}

impl Display for ExplainNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Short items.
            ExplainNode::Delete(name) => {
                writeln!(f, "delete from {name}", name = properly_quoted_name(name))?;
            }
            ExplainNode::Insert(name, conflict) => {
                writeln!(
                    f,
                    "insert into {name} on conflict: {conflict}",
                    name = properly_quoted_name(name)
                )?;
            }
            ExplainNode::Cte(name, reference) => {
                writeln!(
                    f,
                    "scan cte {name}({reference})",
                    name = properly_quoted_name(name)
                )?;
            }
            ExplainNode::Scan(scan) => writeln!(f, "{scan}")?,
            ExplainNode::SubQuery(subquery) => writeln!(f, "{subquery}")?,
            ExplainNode::Motion(motion) => writeln!(f, "{motion}")?,
            ExplainNode::Limit(limit) => writeln!(f, "limit {limit}")?,

            // Potentially long expression lists.
            ExplainNode::Projection(projection) => writeln!(f, "{projection}")?,
            ExplainNode::GroupBy(group_by) => writeln!(f, "{group_by}")?,
            ExplainNode::OrderBy(order_by) => writeln!(f, "{order_by}")?,
            ExplainNode::Update(update) => writeln!(f, "{update}")?,

            // A single but potentially large expression.
            ExplainNode::Join(col_expr, kind, should_fmt) => {
                match kind {
                    JoinKind::LeftOuter => write!(f, "{kind} ")?,
                    JoinKind::Inner => {}
                }
                writeln!(f, "join on {}", FmtSmartParens(col_expr, *should_fmt))?;
            }
            ExplainNode::Selection(col_expr, should_fmt) => {
                writeln!(f, "selection {}", FmtSmartParens(col_expr, *should_fmt))?;
            }
            ExplainNode::Having(col_expr, should_fmt) => {
                writeln!(f, "having {}", FmtSmartParens(col_expr, *should_fmt))?;
            }
            ExplainNode::ValueRow(col_expr) => {
                // `col_expr` is a `ROW(...)` which has its own multiline fmt logic.
                writeln!(f, "value {col_expr}")?;
            }

            // Boring.
            ExplainNode::Value => writeln!(f, "values")?,
            ExplainNode::Union => writeln!(f, "union")?,
            ExplainNode::UnionAll => writeln!(f, "union all")?,
            ExplainNode::Intersect => writeln!(f, "intersect")?,
            ExplainNode::Except => writeln!(f, "except")?,
        };

        Ok(())
    }
}

/// Describe sql query (or subquery) as recursive type
#[derive(Debug, Clone, PartialEq)]
struct ExplainTreePart {
    current: ExplainNode,
    children: Vec<ExplainTreePart>,
}

impl Display for ExplainTreePart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.current)?;

        let mut f = indent(f);
        for child in &self.children {
            write!(f, "{child}")?;
        }

        Ok(())
    }
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

pub struct LogicalExplain {
    main_query: ExplainTreePart,
    subqueries: OrderedMap<NodeId, ExplainTreePart, RepeatableState>,
    windows: Vec<ExplainTreePart>,
    exec_options: Vec<(OptionKind, Value)>,
}

impl Display for LogicalExplain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.main_query)?;
        for (pos, (_, sq)) in self.subqueries.iter().enumerate() {
            writeln!(f, "subquery ${pos}:")?;
            write!(indent(f), "{sq}")?;
        }
        for (pos, window) in self.windows.iter().enumerate() {
            writeln!(f, "window ${pos}:")?;
            write!(f, "{window}")?;
        }
        if !self.exec_options.is_empty() {
            writeln!(f)?;
            writeln!(f, "execution options:")?;
            let (key, value) = self.exec_options.first().expect("must be specified");
            write!(indent(f), "{key} = {value}")?;
            for (key, value) in self.exec_options.iter().skip(1) {
                writeln!(f)?;
                write!(indent(f), "{key} = {value}")?;
            }
        }

        Ok(())
    }
}

impl LogicalExplain {
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
        let should_fmt = ir.explain_options.contains(ExplainOptions::Fmt);

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
                    let sq_ref_map = LogicalExplain::get_sq_ref_map(
                        &mut stack,
                        &mut known_subqueries,
                        subqueries,
                    );
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "GroupBy must have exactly one child".into(),
                        )
                    })?;
                    let group_by = GroupBy::new(ir, gr_exprs, *output, &sq_ref_map, should_fmt)?;

                    (ExplainNode::GroupBy(group_by), vec![child])
                }
                Relational::OrderBy(node::OrderBy {
                    order_by_elements,
                    subqueries,
                    ..
                }) => {
                    let sq_ref_map = LogicalExplain::get_sq_ref_map(
                        &mut stack,
                        &mut known_subqueries,
                        subqueries,
                    );
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "OrderBy must have exactly one child".into(),
                        )
                    })?;
                    let order_by = OrderBy::new(ir, order_by_elements, &sq_ref_map, should_fmt)?;

                    (ExplainNode::OrderBy(order_by), vec![child])
                }
                Relational::Projection(node::Projection {
                    output, subqueries, ..
                }) => {
                    let sq_ref_map = LogicalExplain::get_sq_ref_map(
                        &mut stack,
                        &mut known_subqueries,
                        subqueries,
                    );
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Projection must have exactly one child".into(),
                        )
                    })?;
                    let projection = Projection::new(ir, *output, &sq_ref_map, should_fmt)?;

                    (ExplainNode::Projection(projection), vec![child])
                }
                Relational::SelectWithoutScan(node::SelectWithoutScan {
                    output,
                    subqueries,
                    ..
                }) => {
                    let sq_ref_map = LogicalExplain::get_sq_ref_map(
                        &mut stack,
                        &mut known_subqueries,
                        subqueries,
                    );
                    let projection = Projection::new(ir, *output, &sq_ref_map, should_fmt)?;

                    (ExplainNode::Projection(projection), vec![])
                }
                Relational::ScanRelation(ScanRelation {
                    relation,
                    alias,
                    indexed_by,
                    ..
                }) => {
                    let scan = Scan::new(relation.clone(), alias.clone(), indexed_by.clone());

                    (ExplainNode::Scan(scan), vec![])
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
                    let sq_ref_map = LogicalExplain::get_sq_ref_map(
                        &mut stack,
                        &mut known_subqueries,
                        subqueries,
                    );
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Selection or Having must have exactly one child".into(),
                        )
                    })?;
                    let filter_id = *ir.undo.get_oldest(filter);
                    let selection = ColExpr::new(ir, filter_id, &sq_ref_map, should_fmt)?;
                    let explain_node = match &node {
                        Relational::Selection { .. } => {
                            ExplainNode::Selection(selection, should_fmt)
                        }
                        Relational::Having { .. } => ExplainNode::Having(selection, should_fmt),
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
                    let sq_ref_map = LogicalExplain::get_sq_ref_map(
                        &mut stack,
                        &mut known_subqueries,
                        subqueries,
                    );
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

                    let condition = ColExpr::new(ir, *condition, &sq_ref_map, should_fmt)?;
                    let join = ExplainNode::Join(condition, *kind, should_fmt);

                    (join, vec![left, right])
                }
                Relational::ValuesRow(ValuesRow {
                    data, subqueries, ..
                }) => {
                    let sq_ref_map = LogicalExplain::get_sq_ref_map(
                        &mut stack,
                        &mut known_subqueries,
                        subqueries,
                    );
                    let row = ColExpr::new(ir, *data, &sq_ref_map, should_fmt)?;

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

                    let update = ExplainNode::Update(Update::new(ir, id, should_fmt)?);

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

        let result = Self {
            main_query,
            subqueries: known_subqueries,
            windows: Vec::new(),
            exec_options,
        };

        Ok(result)
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
        let explain = LogicalExplain::new(self, top_id)?;
        Ok(explain.to_smolstr())
    }
}

#[cfg(feature = "mock")]
#[cfg(test)]
mod tests;

pub mod execution_info;
