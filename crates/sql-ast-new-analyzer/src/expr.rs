//! Expression analysis: the walk that turns `Expr<Raw>` into `Expr<Analyzed>`,
//! and the type-system mirror it builds alongside.
//!
//! # The mirror
//! The analyzer does no type reasoning of its own — `sql_type_system` does. What
//! this module does is translate, one node kind at a time, an AST node into the
//! type expression standing for it, mapping AST operators onto the names and
//! kinds the type system knows them by. Each mirror node carries the id minted
//! for its AST node, which is the only key for reading inferred types and
//! implicit coercions back afterwards.
//!
//! These are free functions rather than methods: `Expr` and friends are declared
//! in `sql-ast-new-nodes`, so an inherent `impl` on them here would be `E0116`.
//! The analyzer declares a local trait and implements it for the node instead.

use sql_ir::ir::aggregates::AggregateKind;
use sql_type_system::expr::{ComparisonOperator, Type, UnaryOperator};

use smol_str::format_smolstr;

use crate::cast::{can_cast, coerce_text_literal};
use crate::multiset::analyze_subquery;
use crate::{
    analyze_error, analyze_invariant_error, texpr_from_attribute, texpr_from_derived_type, Binder,
    ExprTypeDeriver, Stmt,
};
use sql_ast_new_nodes::error::{AstErr, AstResult};
use sql_ast_new_nodes::expr::{
    ArithmeticOp, ArrayLiteral, Between, BinaryOp, BinaryOperation, BooleanOp, Case, Cast,
    CastSyntax, CmpOp, Exists, Expr, ExprInner, FunctionCall, FunctionCallArgs, InExpr, IndexExpr,
    IsExpr, IsValue, Like, Literal, LiteralKind, RawVar, Similar, Substring, TimeFunction, Trim,
    UnaryOp, UnaryOperation, ValuesRow,
};
use sql_ast_new_nodes::table_expression::{BoundJoinUsingVar, BoundVar, ColumnRoute};
use sql_ast_new_nodes::{Analyzed, AnalyzedExprMeta, AstNodeId, Raw};
use sql_ir::errors::{Entity, SbroadError};
use sql_ir::ir::metadata::Metadata;
use sql_ir::ir::types::{CastType, DerivedType, UnrestrictedType};

use super::{AnalyzerCtx, AstTypeExpr, AstTypeExprKind, AstTypeReport, TypeSystem};

fn literal_to_type_expr(literal: &Literal<'_>, id: AstNodeId) -> AstTypeExpr {
    let kind = match literal_kind_type(literal.kind) {
        Some(ty) => AstTypeExprKind::Literal(ty),
        None => AstTypeExprKind::Null,
    };
    AstTypeExpr::new(id, kind)
}

/// Type-system type of the literal, or `None` for an untyped NULL.
fn literal_kind_type(kind: LiteralKind) -> Option<Type> {
    match kind {
        LiteralKind::Integer => Some(Type::Integer),
        LiteralKind::Numeric => Some(Type::Numeric),
        LiteralKind::Double => Some(Type::Double),
        LiteralKind::Boolean => Some(Type::Boolean),
        LiteralKind::Text => Some(Type::Text),
        LiteralKind::Null => None,
    }
}

/// Map to the type system's comparison operator.
fn cmp_op_to_type_system(op: CmpOp) -> ComparisonOperator {
    match op {
        CmpOp::Eq => ComparisonOperator::Eq,
        CmpOp::Neq => ComparisonOperator::NotEq,
        CmpOp::Lt => ComparisonOperator::Lt,
        CmpOp::Gt => ComparisonOperator::Gt,
        CmpOp::Lte => ComparisonOperator::LtEq,
        CmpOp::Gte => ComparisonOperator::GtEq,
    }
}

/// Operator name as registered in the type system (see `default_type_system`).
fn arithmetic_op_str(op: ArithmeticOp) -> &'static str {
    match op {
        ArithmeticOp::Add => "+",
        ArithmeticOp::Subtract => "-",
        ArithmeticOp::Multiply => "*",
        ArithmeticOp::Divide => "/",
        ArithmeticOp::Modulo => "%",
    }
}

fn boolean_op_str(op: BooleanOp) -> &'static str {
    match op {
        BooleanOp::And => "and",
        BooleanOp::Or => "or",
    }
}

/// Comparisons get the dedicated `Comparison` kind — the type system unifies the
/// two sides instead of resolving a named overload — while arithmetic, boolean
/// and concat ops go through `Operator` lookup by their registered name.
fn binary_op_to_type_expr(
    id: AstNodeId,
    left: AstTypeExpr,
    right: AstTypeExpr,
    op: BinaryOp,
) -> AstTypeExpr {
    let kind = match op {
        BinaryOp::Comparison(cmp) => {
            AstTypeExprKind::Comparison(cmp_op_to_type_system(cmp), Box::new(left), Box::new(right))
        }
        BinaryOp::Arithmetic(arith) => {
            AstTypeExprKind::Operator(arithmetic_op_str(arith).to_string(), vec![left, right])
        }
        BinaryOp::Boolean(bool_op) => {
            AstTypeExprKind::Operator(boolean_op_str(bool_op).to_string(), vec![left, right])
        }
        BinaryOp::Concat => AstTypeExprKind::Operator("||".to_string(), vec![left, right]),
    };
    AstTypeExpr::new(id, kind)
}

fn array_to_type_expr(id: AstNodeId, elems: Vec<AstTypeExpr>) -> AstTypeExpr {
    AstTypeExpr::new(id, AstTypeExprKind::Array(elems))
}

fn cast_to_type_expr(
    cast: &Cast<'_, Analyzed>,
    id: AstNodeId,
    child_type_expr: AstTypeExpr,
) -> AstTypeExpr {
    AstTypeExpr::new(
        id,
        AstTypeExprKind::Cast(Box::new(child_type_expr), cast.ty.into()),
    )
}

fn cannot_resolve_err(var: &RawVar) -> AstErr {
    analyze_error(format_smolstr!("cannot resolve column reference '{var}'",))
}

/// Resolve column reference.
///
/// The level returned is the index of the frame the reference resolved against:
/// scanning is innermost-outward, but an index means the same thing to every
/// query level, which is what `AggrCtx` needs to compare levels across nesting.
pub(crate) fn bind_var<'q, M: Metadata>(
    var: &RawVar,
    meta: &AnalyzerCtx<'q, M>,
    is_local: bool,
) -> AstResult<(usize, BoundVar<'q>)> {
    let to_take = if is_local {
        1
    } else {
        meta.binder.frames.len()
    };
    if let Some((lvl, column_route)) = meta
        .binder
        .frames
        .iter()
        .enumerate()
        .rev()
        .take(to_take)
        .find_map(|(lvl, Stmt { from, .. })| match from.column_route(var) {
            ColumnRoute::NoMatch => None,
            other => Some((lvl, other)),
        })
    {
        let (tbl_factor, col_pos) = match column_route {
            ColumnRoute::NoMatch => {
                return Err(analyze_invariant_error(format_smolstr!(
                    "incorrect handling of no match for column reference"
                )))
            }
            ColumnRoute::ColumnMissing => return Err(cannot_resolve_err(var)),
            ColumnRoute::Resolved(resolved) => resolved,
            ColumnRoute::Ambigious => {
                return Err(analyze_error(format_smolstr!(
                    "column reference '{var}' is ambigious"
                )))
            }
        };
        Ok((lvl, BoundVar::from_parts(tbl_factor, col_pos)))
    } else {
        Err(cannot_resolve_err(var))
    }
}

/// Mirror node for a resolved column reference: the source attribute's type,
/// or `None` when the attribute cannot be read (a subquery output without a
/// derived type yet).
pub(crate) fn texpr_from_bound_var(var: &BoundVar<'_>, id: AstNodeId) -> Option<AstTypeExpr> {
    var.attribute().map(|attr| texpr_from_attribute(attr, id))
}

/// The expression an unqualified reference to a `USING`-merged column binds to.
pub(crate) fn using_merged_expr<'q>(
    type_system: &mut TypeSystem,
    var: &BoundVar<'q>,
    common_type: DerivedType,
    id: AstNodeId,
) -> Option<(Expr<'q, Analyzed>, AstTypeExpr)> {
    let cast_type = CastType::try_from(common_type.get().as_ref()?).ok()?;
    let child_id = type_system.next_expr_id;
    type_system.next_expr_id += 1;

    let child_t_expr = texpr_from_bound_var(var, child_id)?;
    let child = Expr::from_parts(
        ExprInner::Var(var.clone()),
        AnalyzedExprMeta::new_with_id(child_id),
    );
    let cast = Cast::from_parts(Box::new(child), cast_type, CastSyntax::Call);
    let t_expr = cast_to_type_expr(&cast, id, child_t_expr);
    Some((
        Expr::from_parts(ExprInner::Cast(cast), AnalyzedExprMeta::new_with_id(id)),
        t_expr,
    ))
}

/// Shared LIKE/SIMILAR operand analysis: both type through the single
/// `like(text, text, text)` overload. A missing ESCAPE contributes a
/// synthetic text-literal mirror node standing for the implicit `'\'` —
/// the AST keeps `None` (materializing the default is IR lowering's
/// concern), so the synthetic id is minted fresh and never stored.
#[allow(clippy::type_complexity)]
fn analyze_like_operands<'q, M: Metadata>(
    meta: &mut AnalyzerCtx<'q, M>,
    left: Expr<'q, Raw>,
    right: Expr<'q, Raw>,
    escape: Option<Box<Expr<'q, Raw>>>,
) -> AstResult<(
    Box<Expr<'q, Analyzed>>,
    Box<Expr<'q, Analyzed>>,
    Option<Box<Expr<'q, Analyzed>>>,
    Vec<AstTypeExpr>,
)> {
    let (left, left_t_expr) = left.bind(meta)?;
    let (right, right_t_expr) = right.bind(meta)?;
    let (escape, escape_t_expr) = match escape {
        Some(escape) => {
            let (escape, escape_t_expr) = (*escape).bind(meta)?;
            (Some(Box::new(escape)), escape_t_expr)
        }
        None => {
            let synthetic_id = meta.type_system.next_expr_id;
            meta.type_system.next_expr_id += 1;
            (
                None,
                AstTypeExpr::new(synthetic_id, AstTypeExprKind::Literal(Type::Text)),
            )
        }
    };
    Ok((
        Box::new(left),
        Box::new(right),
        escape,
        vec![left_t_expr, right_t_expr, escape_t_expr],
    ))
}

/// SUBSTRING analysis per arity form, mirroring the old pipeline:
/// - `FromFor`/`Regular` → `substring(s, a, b)` — the registry resolves the
///   numeric `(text, int, int)` or the POSIX-regex `(text, text, text)` form;
/// - `From` → `substring(s, from)` — `(text, int)` start-position or
///   `(text, text)` regex;
/// - `For(s, len)` → `substr(s, 1, len)` — NOT 2-arg `substring`, which would
///   wrongly admit a text length via the regex overload; the literal `1`
///   exists only in the mirror;
/// - `Similar` — the grammar's one-argument fallback; the analyzer validates
///   it really is a SIMILAR with an escape (the old pipeline rejects at IR
///   build) and types it `substring(s, pat, esc)`. The rebuilt wrapper
///   carries no id/type on purpose: it is not a predicate, just syntax.
fn analyze_substring<'q, M: Metadata>(
    substring: Substring<'q, Raw>,
    meta: &mut AnalyzerCtx<'q, M>,
    curr_id: AstNodeId,
) -> AstResult<(ExprInner<'q, Analyzed>, AstTypeExpr)> {
    let three_args = |name: &str, args: Vec<AstTypeExpr>| {
        AstTypeExpr::new(curr_id, AstTypeExprKind::Function(name.to_string(), args))
    };
    Ok(match substring {
        Substring::FromFor(s, from, len) => {
            let (s, s_t_expr) = (*s).bind(meta)?;
            let (from, from_t_expr) = (*from).bind(meta)?;
            let (len, len_t_expr) = (*len).bind(meta)?;
            (
                ExprInner::Substring(Substring::FromFor(
                    Box::new(s),
                    Box::new(from),
                    Box::new(len),
                )),
                three_args("substring", vec![s_t_expr, from_t_expr, len_t_expr]),
            )
        }
        Substring::Regular(s, from, len) => {
            let (s, s_t_expr) = (*s).bind(meta)?;
            let (from, from_t_expr) = (*from).bind(meta)?;
            let (len, len_t_expr) = (*len).bind(meta)?;
            (
                ExprInner::Substring(Substring::Regular(
                    Box::new(s),
                    Box::new(from),
                    Box::new(len),
                )),
                three_args("substring", vec![s_t_expr, from_t_expr, len_t_expr]),
            )
        }
        Substring::From(s, from) => {
            let (s, s_t_expr) = (*s).bind(meta)?;
            let (from, from_t_expr) = (*from).bind(meta)?;
            (
                ExprInner::Substring(Substring::From(Box::new(s), Box::new(from))),
                three_args("substring", vec![s_t_expr, from_t_expr]),
            )
        }
        Substring::For(s, len) => {
            let (s, s_t_expr) = (*s).bind(meta)?;
            let (len, len_t_expr) = (*len).bind(meta)?;
            let synthetic_id = meta.type_system.next_expr_id;
            meta.type_system.next_expr_id += 1;
            let one_t_expr =
                AstTypeExpr::new(synthetic_id, AstTypeExprKind::Literal(Type::Integer));
            (
                ExprInner::Substring(Substring::For(Box::new(s), Box::new(len))),
                three_args("substr", vec![s_t_expr, one_t_expr, len_t_expr]),
            )
        }
        Substring::Similar(inner) => match (*inner).into() {
            ExprInner::Similar(similar) => {
                let (is_not, left, right, escape) = similar.into_parts();
                // `NOT SIMILAR` never forms a valid regex-substring (old
                // pipeline parity: NOT wrapped the whole SIMILAR there, so it
                // also fell through to the one-argument rejection).
                let escape = match (is_not, escape) {
                    (false, Some(escape)) => escape,
                    (false, None) => {
                        return Err(analyze_error(format_smolstr!(
                            "missing escape symbol for SIMILAR substring operator"
                        )))
                    }
                    (true, _) => return Err(substring_single_argument_error()),
                };
                let (left, left_t_expr) = (*left).bind(meta)?;
                let (right, right_t_expr) = (*right).bind(meta)?;
                let (escape, escape_t_expr) = (*escape).bind(meta)?;
                let similar = Similar::from_parts(
                    false,
                    Box::new(left),
                    Box::new(right),
                    Some(Box::new(escape)),
                );
                (
                    ExprInner::Substring(Substring::Similar(Box::new(Expr::from_parts(
                        ExprInner::Similar(similar),
                        AnalyzedExprMeta::default(),
                    )))),
                    three_args("substring", vec![left_t_expr, right_t_expr, escape_t_expr]),
                )
            }
            _ => return Err(substring_single_argument_error()),
        },
    })
}

/// Walk an index chain's intermediate wrappers (which deliberately carry no
/// id — the whole bracket run reports on the outermost node) deriving types
/// for every key, then hand the innermost non-index source to the normal
/// per-node derivation.
fn derive_index_chain_types(
    expr: &mut Expr<'_, Analyzed>,
    type_report: &AstTypeReport,
) -> AstResult<()> {
    if matches!(expr.inner_ref(), ExprInner::Index(_)) {
        let (inner, _) = expr.parts_mut();
        let ExprInner::Index(index) = inner else {
            return Err(analyze_invariant_error(format_smolstr!(
                "index chain node changed kind mid-walk"
            )));
        };
        let (child, which) = index.parts_mut();
        which.derive_types(type_report)?;
        derive_index_chain_types(child, type_report)
    } else {
        expr.derive_types(type_report)
    }
}

fn substring_single_argument_error() -> AstErr {
    analyze_error(format_smolstr!(
        "incorrect SUBSTRING parameters. There is no such overload that takes only 1 argument"
    ))
}

impl<'q> Binder<'q> for Expr<'q, Raw> {
    type BoundNode = (Expr<'q, Analyzed>, AstTypeExpr);

    /// One walk, two products: the analyzed expression and its type-system
    /// mirror. Every node stores the mirror id minted for it — the only key
    /// for reading types and implicit casts back after root-level analysis.
    /// Unsupported expression kinds fail here: rejected, never dropped.
    fn bind<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode> {
        let curr_id = meta.type_system.next_expr_id;
        meta.type_system.next_expr_id += 1;

        // Taken before the subtree is bound, so that if this node turns out to be
        // one of the expression grouping keys the columns it registered can be rewound.
        // Not `None` only for SELECT list element and HAVING with expression key to match (compound expr in GROUP BY).
        let grouping_marks = meta.binder.grouping_key_marks();

        let (inner, type_expr) = match self.into() {
            ExprInner::Nil => {
                return Err(analyze_invariant_error(format_smolstr!(
                    "empty expression placeholder reached analysis"
                )))
            }
            ExprInner::BinaryOperation(operation) => {
                let (left, right, op) = operation.into_parts();

                let (left, left_t_expr) = left.bind(meta)?;
                let (right, right_t_expr) = right.bind(meta)?;

                let inner = BinaryOperation::from_parts(
                    Box::new(Expr::from_parts(
                        left.into_inner(),
                        AnalyzedExprMeta::new_with_id(left_t_expr.id()),
                    )),
                    Box::new(Expr::from_parts(
                        right.into_inner(),
                        AnalyzedExprMeta::new_with_id(right_t_expr.id()),
                    )),
                    op,
                );
                (
                    ExprInner::BinaryOperation(inner),
                    binary_op_to_type_expr(curr_id, left_t_expr, right_t_expr, op),
                )
            }
            ExprInner::UnaryOperation(operation) => {
                let (operand, operator) = operation.into_parts();
                let (operand, operand_t_expr) = (*operand).bind(meta)?;
                let t_expr_kind = match operator {
                    UnaryOp::Not => {
                        AstTypeExprKind::Unary(UnaryOperator::Not, Box::new(operand_t_expr))
                    }
                    UnaryOp::Minus => {
                        AstTypeExprKind::Operator("-".to_string(), vec![operand_t_expr])
                    }
                    UnaryOp::Plus => {
                        AstTypeExprKind::Operator("+".to_string(), vec![operand_t_expr])
                    }
                };
                (
                    ExprInner::UnaryOperation(UnaryOperation::from_parts(
                        Box::new(operand),
                        operator,
                    )),
                    AstTypeExpr::new(curr_id, t_expr_kind),
                )
            }
            ExprInner::Var(var) => {
                // Bind column reference to tuple source (FROM clause entry, outer-query, etc.).
                let (lvl, bound_var) = bind_var(&var, meta, false)?;

                // Register column reference usage.
                meta.binder.reg_var(lvl, bound_var.clone())?;

                if let Some(d_type) = var
                    .table_name()
                    .is_none()
                    .then_some(
                        meta.binder
                            .frames
                            .get(lvl)
                            .and_then(|frame| frame.from.join_using_var(&bound_var))
                            .map(BoundJoinUsingVar::data_type),
                    )
                    .flatten()
                {
                    // Some `JOIN/USING` corresponds to this bound var.
                    // Column should be converted to the JOIN/USING common type..
                    match using_merged_expr(&mut meta.type_system, &bound_var, d_type, curr_id) {
                        Some((expr, t_expr)) => (expr.into_inner(), t_expr),
                        None => (
                            ExprInner::<'q, Analyzed>::Var(bound_var),
                            texpr_from_derived_type(d_type, curr_id),
                        ),
                    }
                } else {
                    texpr_from_bound_var(&bound_var, curr_id)
                        .map_or(Err(cannot_resolve_err(&var)), |t_expr| {
                            Ok((ExprInner::<'q, Analyzed>::Var(bound_var), t_expr))
                        })?
                }
            }
            ExprInner::Literal(lit) => {
                let t_expr = literal_to_type_expr(&lit, curr_id);
                (ExprInner::Literal(lit), t_expr)
            }
            ExprInner::SubQuery(subquery) => {
                let analyzed_subquery = analyze_subquery(*subquery, meta)?;

                if analyzed_subquery.result_columns_cnt()? != 1 {
                    return Err(analyze_error(format_smolstr!(
                        "subquery must return only one column"
                    )));
                }

                let types = analyzed_subquery
                    .result_types()?
                    .into_iter()
                    .map(|derived_type| {
                        derived_type.get().map_or(
                            Err(analyze_error(format_smolstr!(
                                "cannot get result data type for scalar subquery"
                            ))),
                            |ty| Ok(Type::from(ty)),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                (
                    ExprInner::SubQuery(Box::new(analyzed_subquery)),
                    AstTypeExpr::new(curr_id, AstTypeExprKind::Subquery(types)),
                )
            }
            ExprInner::Row(row) => {
                let elems = row.into_parts();
                let mut analyzed_elems = Vec::with_capacity(elems.len());
                let mut elem_type_exprs = Vec::with_capacity(elems.len());
                for elem in elems {
                    let (analyzed_elem, elem_type_expr) = elem.bind(meta)?;
                    analyzed_elems.push(analyzed_elem);
                    elem_type_exprs.push(elem_type_expr);
                }
                // A row has no type of its own; the type system only accepts
                // it as a comparison operand and reports its elements.
                (
                    ExprInner::Row(ValuesRow::from_parts(analyzed_elems)),
                    AstTypeExpr::new(curr_id, AstTypeExprKind::Row(elem_type_exprs)),
                )
            }
            ExprInner::Array(array) => {
                let elems = array.into_parts();
                let mut analyzed_elems = Vec::with_capacity(elems.len());
                let mut elem_type_exprs = Vec::with_capacity(elems.len());
                for elem in elems {
                    let (analyzed_elem, elem_type_expr) = elem.bind(meta)?;
                    analyzed_elems.push(analyzed_elem);
                    elem_type_exprs.push(elem_type_expr);
                }
                (
                    ExprInner::Array(ArrayLiteral::from_parts(analyzed_elems)),
                    array_to_type_expr(curr_id, elem_type_exprs),
                )
            }
            ExprInner::WindowFunction(_) => {
                return Err(analyze_error(format_smolstr!(
                    "window functions are not supported yet"
                )))
            }
            ExprInner::FunctionCall(call) => {
                let (name, args) = call.into_parts();

                let is_aggr_func = AggregateKind::from_name_unnorm(name.as_str()).is_some();

                if is_aggr_func {
                    meta.binder.init_aggr()?;
                }

                let (inner, t_expr) = match args {
                    FunctionCallArgs::CountAsterisk => {
                        // count(*) types as count(1).
                        // It means no column references. Implicit binding to the nearest scope.
                        // The literal exists only in the mirror (fresh id, never stored in the AST).
                        // TODO: support qualified asterisk argument.
                        let synthetic_id = meta.type_system.next_expr_id;
                        meta.type_system.next_expr_id += 1;
                        let one_t_expr =
                            AstTypeExpr::new(synthetic_id, AstTypeExprKind::Literal(Type::Integer));
                        (
                            ExprInner::FunctionCall(FunctionCall::from_parts(
                                name,
                                FunctionCallArgs::CountAsterisk,
                            )),
                            AstTypeExpr::new(
                                curr_id,
                                AstTypeExprKind::Function("count".to_string(), vec![one_t_expr]),
                            ),
                        )
                    }
                    FunctionCallArgs::Exprs { distinct, exprs } => {
                        let (bound_args, arg_t_exprs) = exprs
                            .into_iter()
                            .map(|expr| expr.bind(meta))
                            .collect::<AstResult<(Vec<_>, Vec<_>)>>()?;

                        // COALESCE and JSON_EXTRACT_PATH are not registry
                        // functions: they map to dedicated type-expression
                        // kinds (old-pipeline parity). DISTINCT does not
                        // affect typing and stays in the node.
                        let t_expr_kind = match name.as_str() {
                            "coalesce" => AstTypeExprKind::Coalesce(arg_t_exprs),
                            "json_extract_path" => AstTypeExprKind::JsonExtractPath(arg_t_exprs),
                            _ => AstTypeExprKind::Function(name.as_str().to_string(), arg_t_exprs),
                        };
                        (
                            ExprInner::FunctionCall(FunctionCall::from_parts(
                                name,
                                FunctionCallArgs::Exprs {
                                    distinct,
                                    exprs: bound_args,
                                },
                            )),
                            AstTypeExpr::new(curr_id, t_expr_kind),
                        )
                    }
                };

                if is_aggr_func {
                    meta.binder.finalize_aggr()?;
                }

                (inner, t_expr)
            }
            ExprInner::Parameter(parameter) => {
                // The SQL spelling numbers parameters from one (`$1`, and `?` counted positionally).
                // The type system indexes its parameter vector from zero.
                let param_idx = parameter.0.checked_sub(1).ok_or_else(|| {
                    analyze_error(format_smolstr!("$n parameters are indexed from 1"))
                })?;
                let param_t_expr = AstTypeExpr::new(curr_id, AstTypeExprKind::Parameter(param_idx));
                (ExprInner::Parameter(parameter), param_t_expr)
            }
            ExprInner::Cast(cast) => {
                let (child, cast_type, syntax) = cast.into_parts();
                let (child, child_type_expr) = child.bind(meta)?;
                let cast = Cast::from_parts(Box::new(child), cast_type, syntax);
                let type_expr = cast_to_type_expr(&cast, curr_id, child_type_expr);
                (ExprInner::Cast(cast), type_expr)
            }
            ExprInner::Like(like) => {
                let (is_not, left, right, escape, is_ilike) = like.into_parts();
                let (left, right, escape, arg_t_exprs) =
                    analyze_like_operands(meta, *left, *right, escape)?;
                (
                    ExprInner::Like(Like::from_parts(is_not, left, right, escape, is_ilike)),
                    AstTypeExpr::new(
                        curr_id,
                        AstTypeExprKind::Function("like".into(), arg_t_exprs),
                    ),
                )
            }
            ExprInner::Similar(similar) => {
                let (is_not, left, right, escape) = similar.into_parts();
                let (left, right, escape, arg_t_exprs) =
                    analyze_like_operands(meta, *left, *right, escape)?;
                // Old-pipeline parity: SIMILAR types through the same `like`
                // overload, so type errors name `like`.
                (
                    ExprInner::Similar(Similar::from_parts(is_not, left, right, escape)),
                    AstTypeExpr::new(
                        curr_id,
                        AstTypeExprKind::Function("like".into(), arg_t_exprs),
                    ),
                )
            }
            ExprInner::Between(between) => {
                let (is_not, left, center, right) = between.into_parts();
                let (left, left_t_expr) = left.bind(meta)?;
                let (center, center_t_expr) = center.bind(meta)?;
                let (right, right_t_expr) = right.bind(meta)?;
                // `NOT` does not affect typing: the result is boolean either way.
                (
                    ExprInner::Between(Between::from_parts(
                        is_not,
                        Box::new(left),
                        Box::new(center),
                        Box::new(right),
                    )),
                    AstTypeExpr::new(
                        curr_id,
                        AstTypeExprKind::Between(vec![left_t_expr, center_t_expr, right_t_expr]),
                    ),
                )
            }
            ExprInner::In(in_expr) => {
                let (is_not, left, rhs) = in_expr.into_parts();
                let (left, left_t_expr) = left.bind(meta)?;
                // The rhs is a `Row` or a `SubQuery` by construction; either
                // analyzes through its own arm. `NOT` does not affect typing.
                let (rhs, rhs_t_expr) = rhs.bind(meta)?;
                (
                    ExprInner::In(InExpr::from_parts(is_not, Box::new(left), Box::new(rhs))),
                    AstTypeExpr::new(
                        curr_id,
                        AstTypeExprKind::Comparison(
                            ComparisonOperator::In,
                            Box::new(left_t_expr),
                            Box::new(rhs_t_expr),
                        ),
                    ),
                )
            }
            ExprInner::Is(is_expr) => {
                let (is_not, child, value) = is_expr.into_parts();
                let (child, child_t_expr) = child.bind(meta)?;
                // Postgres-strict: IS TRUE/FALSE require a boolean argument,
                // IS NULL (and its synonym IS UNKNOWN) accepts any type.
                // `NOT` does not affect typing.
                let op = match value {
                    IsValue::Null => UnaryOperator::IsNull,
                    IsValue::Unknown => UnaryOperator::IsUnknown,
                    IsValue::Bool(true) => UnaryOperator::IsTrue,
                    IsValue::Bool(false) => UnaryOperator::IsFalse,
                };
                (
                    ExprInner::Is(IsExpr::from_parts(is_not, Box::new(child), value)),
                    AstTypeExpr::new(curr_id, AstTypeExprKind::Unary(op, Box::new(child_t_expr))),
                )
            }
            ExprInner::Index(index) => {
                // The parser nests one `IndexExpr` per bracket; the type
                // system wants the whole bracket run as a single
                // `IndexChain { source, keys }` (an intermediate `a[1]` over
                // a mixed-type container would type as `any`, which cannot
                // be indexed again). Flatten outermost-in.
                let (mut source, which) = index.into_parts();
                let mut keys_raw = vec![which];
                while matches!(source.inner_ref(), ExprInner::Index(_)) {
                    let ExprInner::Index(inner_index) = (*source).into() else {
                        return Err(analyze_invariant_error(format_smolstr!(
                            "index chain node changed kind mid-walk"
                        )));
                    };
                    let (child, which) = inner_index.into_parts();
                    keys_raw.push(which);
                    source = child;
                }
                // Source order: innermost bracket first.
                keys_raw.reverse();

                let (source, source_t_expr) = (*source).bind(meta)?;
                let mut analyzed_keys = Vec::with_capacity(keys_raw.len());
                let mut key_t_exprs = Vec::with_capacity(keys_raw.len());
                for key in keys_raw {
                    let (key, key_t_expr) = (*key).bind(meta)?;
                    analyzed_keys.push(key);
                    key_t_exprs.push(key_t_expr);
                }

                // Rebuild the nested chain; only the outermost node gets the
                // id (attached by the generic tail below) — intermediate
                // wrappers carry no id/type and render bare.
                let mut chain = source;
                for key in analyzed_keys {
                    chain = Expr::from_parts(
                        ExprInner::Index(IndexExpr::from_parts(Box::new(chain), Box::new(key))),
                        AnalyzedExprMeta::default(),
                    );
                }
                let ExprInner::Index(outer_index) = chain.into_inner() else {
                    return Err(analyze_invariant_error(format_smolstr!(
                        "rebuilt index chain has no outermost bracket"
                    )));
                };
                (
                    ExprInner::Index(outer_index),
                    AstTypeExpr::new(
                        curr_id,
                        AstTypeExprKind::IndexChain {
                            source: Box::new(source_t_expr),
                            keys: key_t_exprs,
                        },
                    ),
                )
            }
            ExprInner::Trim(trim) => {
                let (kind, pattern, target) = trim.into_parts();
                // Pattern-first argument order, matching the registered
                // `trim(text[, text])` overloads. LEADING/TRAILING/BOTH does
                // not affect typing.
                let mut arg_t_exprs = Vec::with_capacity(2);
                let pattern = match pattern {
                    Some(pattern) => {
                        let (pattern, pattern_t_expr) = (*pattern).bind(meta)?;
                        arg_t_exprs.push(pattern_t_expr);
                        Some(Box::new(pattern))
                    }
                    None => None,
                };
                let (target, target_t_expr) = (*target).bind(meta)?;
                arg_t_exprs.push(target_t_expr);
                (
                    ExprInner::Trim(Trim::from_parts(kind, pattern, Box::new(target))),
                    AstTypeExpr::new(
                        curr_id,
                        AstTypeExprKind::Function("trim".to_string(), arg_t_exprs),
                    ),
                )
            }
            ExprInner::Substring(substring) => analyze_substring(substring, meta, curr_id)?,
            ExprInner::Case(case) => {
                let Case {
                    search,
                    when_blocks,
                    else_expr,
                } = case;
                // Old-pipeline mapping: the simple-form search expression is
                // handed to the type system separately, so it can unify WHEN
                // expressions with it instead of requiring them to be
                // conditions. THEN results and ELSE form the result group
                // whose unified type is the CASE type.
                let mut when_t_exprs = Vec::with_capacity(when_blocks.len());
                let mut result_t_exprs = Vec::with_capacity(when_blocks.len() + 1);
                let (search, search_t_expr) = match search {
                    Some(search) => {
                        let (search, search_t_expr) = (*search).bind(meta)?;
                        (Some(Box::new(search)), Some(Box::new(search_t_expr)))
                    }
                    None => (None, None),
                };
                let mut analyzed_when_blocks = Vec::with_capacity(when_blocks.len());
                for (condition, result) in when_blocks {
                    let (condition, condition_t_expr) = condition.bind(meta)?;
                    let (result, result_t_expr) = result.bind(meta)?;
                    when_t_exprs.push(condition_t_expr);
                    result_t_exprs.push(result_t_expr);
                    analyzed_when_blocks.push((condition, result));
                }
                let else_expr = match else_expr {
                    Some(else_expr) => {
                        let (else_expr, else_t_expr) = (*else_expr).bind(meta)?;
                        result_t_exprs.push(else_t_expr);
                        Some(Box::new(else_expr))
                    }
                    None => None,
                };
                (
                    ExprInner::Case(Case {
                        search,
                        when_blocks: analyzed_when_blocks,
                        else_expr,
                    }),
                    AstTypeExpr::new(
                        curr_id,
                        AstTypeExprKind::Case {
                            search_expr: search_t_expr,
                            when_exprs: when_t_exprs,
                            result_exprs: result_t_exprs,
                        },
                    ),
                )
            }
            ExprInner::Exists(exists) => {
                let (is_not, subquery) = exists.into_parts();
                let analyzed_subquery = analyze_subquery(*subquery, meta)?;
                // The engine's EXISTS only checks that its child is a
                // subquery and reports boolean; result types are deliberately
                // not queried — `result_types()` fails for set-operation
                // bodies, and EXISTS over a UNION must work. No single-column
                // restriction. `NOT` does not affect typing.
                let synthetic_id = meta.type_system.next_expr_id;
                meta.type_system.next_expr_id += 1;
                let subquery_t_expr =
                    AstTypeExpr::new(synthetic_id, AstTypeExprKind::Subquery(Vec::new()));
                (
                    ExprInner::Exists(Exists::from_parts(is_not, Box::new(analyzed_subquery))),
                    AstTypeExpr::new(
                        curr_id,
                        AstTypeExprKind::Unary(UnaryOperator::Exists, Box::new(subquery_t_expr)),
                    ),
                )
            }
            ExprInner::TimeFunction(time_function) => {
                match &time_function {
                    TimeFunction::CurrentTime(_) => {
                        return Err(SbroadError::NotImplemented(
                            Entity::SQLFunction,
                            format_smolstr!("`CURRENT_TIME`"),
                        )
                        .into())
                    }
                    TimeFunction::LocalTime(_) => {
                        return Err(SbroadError::NotImplemented(
                            Entity::SQLFunction,
                            format_smolstr!("`LOCALTIME`"),
                        )
                        .into())
                    }
                    // Picodata has no date or time-of-day types: CURRENT_DATE
                    // is a midnight datetime and CURRENT_TIMESTAMP equals
                    // LOCALTIMESTAMP (no timezone-less timestamp type). The
                    // precision stays rendering-faithful; the ≤6 clamp is IR
                    // lowering's concern.
                    TimeFunction::CurrentDate
                    | TimeFunction::CurrentTimestamp(_)
                    | TimeFunction::LocalTimestamp(_) => {}
                }
                (
                    ExprInner::TimeFunction(time_function),
                    AstTypeExpr::new(curr_id, AstTypeExprKind::Literal(Type::Datetime)),
                )
            }
        };

        let bound = Expr::from_parts(inner, AnalyzedExprMeta::new_with_id(type_expr.id()));

        // Clean all added variables in current tree if its expression
        // equals to some expression in GROUP BY.
        meta.binder.drop_grouping_key_vars(&grouping_marks, &bound);

        Ok((bound, type_expr))
    }
}

impl ExprTypeDeriver for Expr<'_, Analyzed> {
    /// Read inferred types back from the report by each node's stored mirror
    /// id, wrapping nodes the type system chose to coerce in explicit CAST
    /// calls so coercions are visible in the tree and its rendering. A
    /// missing type is tolerated only for subqueries.
    fn derive_types(&mut self, type_report: &AstTypeReport) -> AstResult<()> {
        let curr_id = self.id();

        let (inner, _) = self.parts_mut();
        match inner {
            ExprInner::BinaryOperation(operation) => {
                let (left, right, _) = operation.parts_mut();
                left.derive_types(type_report)?;
                right.derive_types(type_report)?;
            }
            ExprInner::Var(_) => { /* nothing to do */ }
            ExprInner::Literal(_) => { /* nothing to do */ }
            ExprInner::SubQuery(_) => { /* nothing to do */ }
            ExprInner::Row(row) => {
                for elem in row.parts_mut() {
                    elem.derive_types(type_report)?;
                }
            }
            ExprInner::Between(between) => {
                let (left, center, right) = between.parts_mut();
                left.derive_types(type_report)?;
                center.derive_types(type_report)?;
                right.derive_types(type_report)?;
            }
            ExprInner::Is(is_expr) => {
                let (_is_not, child, _value) = is_expr.parts_mut();
                child.derive_types(type_report)?;
            }
            ExprInner::Like(like) => {
                let (left, right, escape) = like.parts_mut();
                left.derive_types(type_report)?;
                right.derive_types(type_report)?;
                if let Some(escape) = escape {
                    escape.derive_types(type_report)?;
                }
            }
            ExprInner::Similar(similar) => {
                let (left, right, escape) = similar.parts_mut();
                left.derive_types(type_report)?;
                right.derive_types(type_report)?;
                if let Some(escape) = escape {
                    escape.derive_types(type_report)?;
                }
            }
            ExprInner::UnaryOperation(operation) => {
                operation.parts_mut().0.derive_types(type_report)?;
            }
            ExprInner::FunctionCall(call) => match call.parts_mut().1 {
                FunctionCallArgs::CountAsterisk => { /* no expression children */ }
                FunctionCallArgs::Exprs { exprs, .. } => {
                    for expr in exprs {
                        expr.derive_types(type_report)?;
                    }
                }
            },
            ExprInner::Case(case) => {
                if let Some(search) = &mut case.search {
                    search.derive_types(type_report)?;
                }
                for (condition, result) in &mut case.when_blocks {
                    condition.derive_types(type_report)?;
                    result.derive_types(type_report)?;
                }
                if let Some(else_expr) = &mut case.else_expr {
                    else_expr.derive_types(type_report)?;
                }
            }
            // The subquery's expressions typed themselves during its own
            // analysis, and the node itself is boolean from the report.
            ExprInner::Exists(_) => { /* nothing to do */ }
            ExprInner::TimeFunction(_) => { /* a leaf like a literal */ }
            ExprInner::Index(index) => {
                // Only the outermost chain node carries an id; intermediate
                // wrappers are walked manually down to the real source.
                let (child, which) = index.parts_mut();
                which.derive_types(type_report)?;
                derive_index_chain_types(child, type_report)?;
            }
            ExprInner::Trim(trim) => {
                let (pattern, target) = trim.parts_mut();
                if let Some(pattern) = pattern {
                    pattern.derive_types(type_report)?;
                }
                target.derive_types(type_report)?;
            }
            ExprInner::Substring(substring) => match substring {
                Substring::FromFor(s, from, len) | Substring::Regular(s, from, len) => {
                    s.derive_types(type_report)?;
                    from.derive_types(type_report)?;
                    len.derive_types(type_report)?;
                }
                Substring::For(s, other) | Substring::From(s, other) => {
                    s.derive_types(type_report)?;
                    other.derive_types(type_report)?;
                }
                Substring::Similar(wrapper) => {
                    // The wrapper deliberately carries no id (it is syntax,
                    // not a predicate); recurse the SIMILAR children directly.
                    let (wrapper_inner, _) = wrapper.parts_mut();
                    let ExprInner::Similar(similar) = wrapper_inner else {
                        return Err(analyze_invariant_error(format_smolstr!(
                            "SUBSTRING SIMILAR wrapper is not a SIMILAR expression"
                        )));
                    };
                    let (left, right, escape) = similar.parts_mut();
                    left.derive_types(type_report)?;
                    right.derive_types(type_report)?;
                    if let Some(escape) = escape {
                        escape.derive_types(type_report)?;
                    }
                }
            },
            ExprInner::In(in_expr) => {
                let (left, rhs) = in_expr.parts_mut();
                left.derive_types(type_report)?;

                let rhs_inner = rhs.inner_mut();
                match rhs_inner {
                    ExprInner::Row(row) => {
                        for elem in row.parts_mut() {
                            elem.derive_types(type_report)?;
                        }
                        let rhs_meta = rhs.meta_mut();
                        rhs_meta.data_type = DerivedType::unknown();
                    }
                    ExprInner::SubQuery(_) => {
                        let rhs_id = rhs.id();

                        if let Some(d_type) = type_report.get_cast(&rhs_id) {
                            add_cast(rhs, d_type)?;
                        } else {
                            rhs.meta_mut().data_type = type_report
                                .try_get_type(&rhs_id)
                                .map_or(DerivedType::unknown(), Into::into);
                        }
                    }
                    _ => {
                        return Err(analyze_invariant_error(format_smolstr!(
                            "IN right-hand side is neither a row nor a subquery"
                        )))
                    }
                }
            }
            // A leaf like a literal: the type comes from the report, either
            // the client-supplied one or the one inferred from context.
            ExprInner::Parameter(_) => { /* nothing to do */ }
            ExprInner::Array(array) => {
                for elem in array.parts_mut() {
                    elem.derive_types(type_report)?;
                }
            }
            ExprInner::Cast(cast) => {
                let cast_ty: UnrestrictedType = UnrestrictedType::from(cast.ty);
                let (child, _, _) = cast.parts_mut();
                child.derive_types(type_report)?;
                // A subquery or a row has no type of its own, so there is
                // nothing to rule on; everything else must have a cast path.
                if let Some(from) = child.data_type().get() {
                    if !can_cast(*from, cast_ty) {
                        return Err(analyze_error(format_smolstr!(
                            "cannot cast type {from} to {cast_ty}"
                        )));
                    }
                }
                if child.data_type().get().is_some_and(|ty| ty == cast_ty) {
                    // The child's type is similar to CAST type.
                    // So we can get rid of the entire CAST node.
                    *self = std::mem::take(child);
                } else {
                    // The child's eventual type is stated by the CAST itself.
                    // Keeping it would render a second `::type` suffix under the cast.
                    child.parts_mut().1.data_type = DerivedType::unknown();
                }
            }
            // `Expr::analyze` and this whitelist must accept the same
            // expression kinds. Hitting this arm means they drifted apart.
            _ => {
                return Err(analyze_invariant_error(format_smolstr!(
                    "no type derivation for an expression kind produced by the analyzer"
                )))
            }
        };

        let ts_cast = type_report.get_cast(&curr_id);
        if let Some(d_type) = ts_cast {
            // A text literal is folded into the target type here.
            // Only what could not be folded still needs a CAST wrapper.
            if !coerce_text_literal(self, d_type)? {
                add_cast(self, d_type)?;
            }
        }

        let (inner, meta) = self.parts_mut();
        meta.data_type = match ts_cast.or_else(|| type_report.try_get_type(&curr_id)) {
            None => match inner {
                // Subqueries type themselves during their own analysis.
                // A row has no type of its own (there is no tuple type) —
                // only its elements and the comparison enclosing it are reported.
                ExprInner::SubQuery(_) | ExprInner::Row(_) => DerivedType::unknown(),
                _ => {
                    return Err(analyze_error(format_smolstr!(
                        "cannot derive type for expression `{}`",
                        inner.kind_name()
                    )))
                }
            },
            Some(ty) => ty.into(),
        };

        Ok(())
    }
}

/// Wrap this node in `CAST(<node> AS d_type)` in place.
fn add_cast(expr: &mut Expr<'_, Analyzed>, d_type: Type) -> AstResult<()> {
    let (inner, meta) = expr.parts_mut();
    let cast_type = DerivedType::from(d_type)
        .get()
        .as_ref()
        .ok_or_else(|| {
            analyze_invariant_error(format_smolstr!("implicit cast target type is unknown"))
        })?
        .try_into()?;

    let original_inner = std::mem::replace(
        inner,
        ExprInner::Cast(Cast::from_parts(
            Box::new(Expr::default()),
            cast_type,
            CastSyntax::Call,
        )),
    );

    if let ExprInner::Cast(cast) = inner {
        let (child, _, _) = cast.parts_mut();
        *child.as_mut() = Expr::from_parts(
            original_inner,
            std::mem::take(meta), /* filled node id, but empty data type */
                                  // AnalyzedExprMeta::default(), /* empty because of CAST */
        );
    }

    Ok(())
}
