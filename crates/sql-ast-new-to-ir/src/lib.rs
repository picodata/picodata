// `clippy::todo` joins the list once the remaining `todo!()` are translated.
#![cfg_attr(
    not(test),
    deny(
        clippy::unimplemented,
        clippy::panic,
        clippy::unreachable,
        clippy::expect_used,
        clippy::unwrap_used
    )
)]
#![deny(
    rustdoc::broken_intra_doc_links,
    unreachable_pub,
    unused_lifetimes,
    single_use_lifetimes
)]
#![allow(rustdoc::private_intra_doc_links)]

use smol_str::{format_smolstr, SmolStr, ToSmolStr};

use sql_ast_new_nodes::error::{ast_invariant_err, AstErr, AstResult};
use sql_ast_new_nodes::expr::{
    BinaryOp, BooleanOp, CmpOp, Expr, ExprInner, FunctionCallArgs, ResolvedFn, UnaryOp,
};
use sql_ast_new_nodes::multiset::{Cte, MultisetInner, MultisetStmt};
use sql_ast_new_nodes::select::{ProjectionExpr, SelectStmt};
use sql_ast_new_nodes::table_expression::{
    AnalyzedCteOrTable, BoundVar, From, FromEntry, GroupBy, JoinKind, JoinedTable, OrdrByGrpByElem,
    TableFactor, TableFactorInner,
};
use sql_ast_new_nodes::{Analyzed, Ident, Node, SqlOption};

use sql_ast_new_nodes::AbstractSyntaxTree;

use sql_ir::ir::index::Index;
use sql_ir::ir::node::expression::MutExpression;
use sql_ir::ir::node::{BoolExpr, CountAsterisk, NodeId, ReferenceTarget, ScalarFunction};
use sql_ir::ir::operator::{Bool, JoinKind as IrJoinKind, Unary};
use sql_ir::ir::subtree_cloner::SubtreeCloner;
use sql_ir::ir::types::CastType;
use sql_ir::ir::value::Value;
use sql_ir::ir::Plan;
use sql_ir::utils::get_unnamed_column_alias;

/// Error for unsupported by IR functionality.
pub(crate) fn unsupported_err(msg: SmolStr) -> AstErr {
    ast_invariant_err(msg)
}

/// Internal error caused by broken invariants.
pub(crate) fn internal_err(msg: SmolStr) -> AstErr {
    ast_invariant_err(msg)
}

type AnalyzedAst<'q> = AbstractSyntaxTree<'q, Analyzed>;

/// Conversion of `Analyzed` AST into IR tree.
pub trait AstTranslator<'q> {
    /// Translation of AST tree into IR.
    fn translate(self) -> AstResult<Plan>;
}

/// Conversion of `Analyzed` AST into IR tree.
pub(crate) trait NodeTranslator<'q> {
    type ResType;

    /// Translation of AST tree into IR.
    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType>;
}

pub(crate) struct Ctx<'q> {
    /// One frame per statement depth.
    frames: Vec<Frame>,
    /// Select list of the statement in progress, parked while GROUP BY
    /// translates the items its ordinals name.
    proj_exprs: Vec<ProjExpr<'q>>,
}

/// Translation state of one statement depth.
#[derive(Default)]
struct Frame {
    /// CTEs visible at this depth, by position.
    ctes: Vec<NodeId>,
    /// Column offset of each FROM entry already folded into the join chain.
    offsets: Vec<usize>,
    /// The table being joined while its ON/USING condition translates.
    pending: Option<NodeId>,
}

impl Ctx<'_> {
    fn empty() -> Self {
        Self {
            frames: Vec::new(),
            proj_exprs: Vec::new(),
        }
    }

    fn frame_mut(&mut self) -> AstResult<&mut Frame> {
        self.frames
            .last_mut()
            .ok_or_else(|| internal_err(format_smolstr!("expected actual statement frame")))
    }
}

/// Select list item while GROUP BY may still consume it.
enum ProjExpr<'q> {
    Orig(ProjectionExpr<'q, Analyzed>),
    /// Translated as a GROUP BY key; the select list clones its subtree.
    Translated(NodeId, Option<Ident>),
}

/// Lets `mem::take` pull an item out of [`Ctx::proj_exprs`].
impl Default for ProjExpr<'_> {
    fn default() -> Self {
        Self::Orig(ProjectionExpr::new(Expr::default(), None))
    }
}

impl<'q> AstTranslator<'q> for AnalyzedAst<'q> {
    fn translate(self) -> AstResult<Plan> {
        let mut ir = Plan::new();
        let mut ctx = Ctx::empty();
        self.root.translate(&mut ir, &mut ctx)?;
        Ok(ir)
    }
}

impl<'q> NodeTranslator<'q> for Node<'q, Analyzed> {
    type ResType = ();

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        match self {
            Node::Empty => return Err(internal_err(format_smolstr!("expected non-empty tree"))),
            Node::DqlStmt(dql_stmt) => {
                dql_stmt.stmt.translate(ir, ctx)?;
                if let Some(option) = dql_stmt.option {
                    option.translate(ir, ctx)?;
                }
            }
        }
        Ok(())
    }
}

impl<'q> NodeTranslator<'q> for SqlOption {
    type ResType = ();

    fn translate(self, _ir: &mut Plan, _ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        // TODO: enrich IR with query options.
        Ok(())
    }
}

impl<'q> NodeTranslator<'q> for MultisetStmt<'q, Analyzed> {
    type ResType = ();

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        // Expect no top.
        debug_assert!(ir.get_top().is_err());

        // Split the whole statement into parts and handle them separately.
        let (ctes, inner, _order_by, _limit) = self.into_parts();

        // Initialize scope we are entering
        ctx.frames.push(Frame::default());

        for cte in ctes.into() {
            // Translate CTE and save its root in the actual scope.
            let cte_id = cte.translate(ir, ctx)?;
            ctx.frame_mut()?.ctes.push(cte_id);

            // CTE inner nodes should not leak.
            ir.reset_top();
        }

        // Handle each variant of statement inner body.
        match inner {
            MultisetInner::Select(stmt) => stmt.translate(ir, ctx)?,
            MultisetInner::Values(_values) => todo!(),
            MultisetInner::Operation(_operation) => todo!(),
        };

        // TODO: translate `ORDER BY`, `LIMIT`

        // Drop the scope we are leaving.
        ctx.frames.pop();

        Ok(())
    }
}

impl<'q> NodeTranslator<'q> for Cte<'q, Analyzed> {
    type ResType = NodeId;

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        let (name, columns, body) = self.into_parts();

        // Translate CTE body
        body.translate(ir, ctx)?;

        // Align CTE body with its output column aliases
        let cte_id = ir.transform_cte_columns(
            ir.get_top()?,
            name.take(),
            columns.into_iter().map(Ident::take).collect(),
        )?;

        Ok(cte_id)
    }
}

impl<'q> NodeTranslator<'q> for SelectStmt<'q, Analyzed> {
    type ResType = ();

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        let (sel_lst, tbl_expr) = self.into_parts();
        let (exprs, is_distinct) = sel_lst.into_parts();
        let mut proj_exprs: Vec<ProjExpr<'q>> = exprs.0.into_iter().map(ProjExpr::Orig).collect();

        // Relational node the GROUP BY keys read from.
        let grby_child = if let Some(tbl_expr) = tbl_expr {
            let (from, filter, grby, having, _windows) = tbl_expr.into_parts();

            from.translate(ir, ctx)?;

            if let Some(filter) = filter {
                let filter_id = filter.translate(ir, ctx)?;
                let select_id = ir.add_select(ir.get_top()?, filter_id)?;
                ir.set_top(select_id)?;
            }

            // Save child of GROUP BY in IR
            let grby_child = Some(ir.get_top()?);

            // Park the select list: ordinal keys are translated out of it.
            let saved_sel_lst = std::mem::replace(&mut ctx.proj_exprs, proj_exprs);
            grby.translate(ir, ctx)?;
            proj_exprs = std::mem::replace(&mut ctx.proj_exprs, saved_sel_lst);

            if let Some(having) = having {
                let filter_id = having.translate(ir, ctx)?;
                let having_id = ir.add_having(ir.get_top()?, filter_id)?;
                ir.set_top(having_id)?;
            }

            grby_child
        } else {
            None
        };

        // Add IR nodes for each expression in SELECT list.
        let mut out_vars = Vec::with_capacity(proj_exprs.len());

        // Track columns without aliases.
        // For queries `SELECT col_1 FROM (SELECT 1 as col_1, 2)`
        // `col_1` reference is ambiguous. It will be fixed some day.
        let mut noname_col_cnt = 0;
        for proj_expr in proj_exprs {
            // TODO: save subqueries used in SELECT list
            // TODO: keep saved top when diving into branches setting their own IR plan top node.
            let (expr_id, alias) = match proj_expr {
                ProjExpr::Orig(proj) => {
                    let (expr, alias) = proj.into_parts();
                    (expr.translate(ir, ctx)?, alias)
                }
                ProjExpr::Translated(key_id, alias) => {
                    // The key reads the GroupBy child; its projected copy reads the top.
                    let expr_id = SubtreeCloner::clone_subtree(ir, key_id)?;
                    let grby_child = grby_child.ok_or_else(|| {
                        internal_err(format_smolstr!("expected GROUP BY for translated key"))
                    })?;
                    let top_id = ir.get_top()?;
                    ir.replace_target_in_subtree(expr_id, grby_child, top_id)?;
                    (expr_id, alias)
                }
            };

            let alias_id = if let Some(alias) = alias {
                ir.nodes.add_alias(alias.as_str(), expr_id)?
            } else {
                noname_col_cnt += 1;
                ir.nodes
                    .add_alias(get_unnamed_column_alias(noname_col_cnt).as_str(), expr_id)?
            };

            out_vars.push(alias_id);
        }

        let top_id = if let Ok(src_rel) = ir.get_top() {
            // Add projection node over some tuple source.
            let output = ir.nodes.add_row(out_vars);
            ir.add_projection(src_rel, output, vec![], vec![], is_distinct)?
        } else {
            // No tuple source add SelectWithoutScan
            ir.add_select_without_scan(out_vars)?
        };

        ir.set_top(top_id)?;
        Ok(())
    }
}

impl<'q> NodeTranslator<'q> for GroupBy<'q, Analyzed> {
    type ResType = ();

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        let keys = self
            .0
            .into_iter()
            .map(|elem| elem.translate(ir, ctx))
            .collect::<AstResult<Vec<_>>>()?;
        if keys.is_empty() {
            return Ok(());
        }

        let grby_id = ir.add_groupby(ir.get_top()?, &keys)?;
        ir.set_top(grby_id)?;
        Ok(())
    }
}

impl<'q> NodeTranslator<'q> for OrdrByGrpByElem<'q, Analyzed> {
    /// Grouping key node.
    type ResType = NodeId;

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        let pos = match self {
            Self::Expr(expr) => return expr.translate(ir, ctx),
            Self::Ordinal(pos) => pos,
        };

        let proj_entry = ctx.proj_exprs.get_mut(pos).ok_or_else(|| {
            internal_err(format_smolstr!(
                "GROUP BY ordinal {pos} is out of the select list"
            ))
        })?;
        match std::mem::take(proj_entry) {
            ProjExpr::Orig(proj) => {
                let (expr, alias) = proj.into_parts();
                let key_id = expr.translate(ir, ctx)?;
                ctx.proj_exprs[pos] = ProjExpr::Translated(key_id, alias);
                Ok(key_id)
            }
            // Repeated ordinal: group by a copy of the key.
            ProjExpr::Translated(key_id, alias) => {
                ctx.proj_exprs[pos] = ProjExpr::Translated(key_id, alias);
                Ok(SubtreeCloner::clone_subtree(ir, key_id)?)
            }
        }
    }
}

impl<'q> NodeTranslator<'q> for From<'q, Analyzed> {
    type ResType = ();

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        ctx.frame_mut()?.offsets.clear();
        for entry in self.into_entries() {
            entry.translate(ir, ctx)?;
        }

        Ok(())
    }
}

impl<'q> NodeTranslator<'q> for FromEntry<'q, Analyzed> {
    type ResType = ();

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        match self {
            // A bound column reference names its FROM entry by position
            // (`BoundVar { depth, entry, .. }`), so translation is free to
            // record the IR node each entry becomes — one table per statement
            // depth, indexed by entry — and resolve references against that
            // table instead of the tree. That is what lets a future
            // by-value translation consume FROM entries as it goes.
            Self::TableFactor(tbl) => {
                tbl.translate(ir, ctx)?;
                // The leading entry opens the join chain.
                ctx.frame_mut()?.offsets.push(0);
                Ok(())
            }
            Self::JoinedTable(joined) => joined.translate(ir, ctx),
        }
    }
}

impl<'q> NodeTranslator<'q> for JoinedTable<'q, Analyzed> {
    type ResType = ();

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        let (tbl, kind, condition, using_cols) = self.into_parts();
        let is_cross = matches!(kind, JoinKind::Cross);
        let kind = match kind {
            JoinKind::Inner | JoinKind::Cross => IrJoinKind::Inner,
            JoinKind::Left => IrJoinKind::LeftOuter,
        };

        let left = ir.get_top()?;

        // Translate entry to be joined.
        tbl.translate(ir, ctx)?;
        let right = ir.get_top()?;

        // The condition reads the left chain through the top and the joined
        // table through `pending`.
        ir.set_top(left)?;
        ctx.frame_mut()?.pending = Some(right);

        let condition = match condition {
            Some(cond) => cond.translate(ir, ctx)?,
            // `USING (c1, c2)` is `ON l.c1 = r.c1 AND l.c2 = r.c2`.
            None => {
                let mut cond = None;
                for using in using_cols {
                    let (l, r) = using.parts();
                    // shortcut: differently typed columns need casts, lift once Cast translation lands.
                    if l.data_type() != r.data_type() {
                        return Err(unsupported_err(format_smolstr!(
                            "JOIN/USING over differently typed columns is not supported"
                        )));
                    }

                    // Translate both vars and bind them via equality.
                    let l_id = l.translate(ir, ctx)?;
                    let r_id = r.translate(ir, ctx)?;
                    let eq = ir.add_cond(l_id, Bool::Eq, r_id)?;

                    // Chain with `ON` exprs the equality with left and joined table vars.
                    cond = Some(match cond {
                        Some(cond) => ir.add_cond(cond, Bool::And, eq)?,
                        None => eq,
                    });
                }
                match cond {
                    Some(cond) => cond,
                    // `a CROSS JOIN b` is `a INNER JOIN b ON true`, as in the old pipeline.
                    None if is_cross => ir.add_const(Value::Boolean(true)),
                    None => {
                        return Err(internal_err(format_smolstr!(
                            "expected JOIN/ON or JOIN/USING"
                        )))
                    }
                }
            }
        };

        let frame = ctx.frame_mut()?;
        frame.pending = None;

        // Save the offset of joined table columns.
        frame.offsets.push(ir.columns_len(left)?);

        // All nodes we depend on have just been translated. Just add join node into IR.
        let join_id = ir.add_join(left, right, condition, kind)?;

        // Now IR top is newly created join.
        ir.set_top(join_id)?;
        Ok(())
    }
}

impl<'q> NodeTranslator<'q> for TableFactor<'q, Analyzed> {
    type ResType = ();

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        match self.inner {
            TableFactorInner::CteOrTable(cte_or_tbl) => match cte_or_tbl {
                AnalyzedCteOrTable::Cte { name, cte } => {
                    let body_id = *ctx
                        .frames
                        .get(cte.depth)
                        .and_then(|frame| frame.ctes.get(cte.offset))
                        .ok_or_else(|| internal_err(format_smolstr!("expected to find CTE")))?;

                    let scan_id = ir.add_cte_scan(
                        body_id,
                        self.alias.map_or_else(|| name.take(), Ident::take),
                    )?;
                    ir.set_top(scan_id)?;
                }
                AnalyzedCteOrTable::Table(tbl) => {
                    // Need for scan node in IR
                    let tbl_name = tbl.name.clone();
                    let tbl_id = tbl.id;

                    // Add `Table` entity into IR plan
                    ir.add_rel(*tbl);

                    // Add scan node itself.
                    let scan_id = ir.add_scan(
                        tbl_name,
                        self.alias.map(Ident::take),
                        self.indexed_by
                            .as_ref()
                            .map(|idx_by| idx_by.name.to_smolstr()),
                    )?;

                    if let Some(idx) = self.indexed_by {
                        // Add index from `indexed_by` to IR plan
                        ir.indexes
                            .insert(idx.name.take(), Index::from(tbl_id, idx.index_id));
                        std::rc::Rc::make_mut(&mut ir.index_version_map)
                            .insert([tbl_id, idx.index_id], 0);
                    }

                    // Set scan node as a top node
                    ir.set_top(scan_id)?;
                }
            },
            TableFactorInner::SubQuery(sq) => sq.translate(ir, ctx)?,
            TableFactorInner::Function(_) => {
                return Err(unsupported_err(format_smolstr!(
                    "table functions are not supported"
                )))
            }
        }
        Ok(())
    }
}

impl<'q> NodeTranslator<'q> for BoundVar {
    type ResType = NodeId;

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        if self.depth + 1 != ctx.frames.len() {
            return Err(unsupported_err(format_smolstr!(
                "correlated subqueries are not supported"
            )));
        }

        // Entries folded into the join chain are read through the top,
        // the table being joined directly.
        let frame = ctx.frame_mut()?;
        let (target, position) = match frame.offsets.get(self.entry) {
            Some(offset) => {
                let top = ir.get_top().map_err(|_| {
                    internal_err(format_smolstr!(
                        "expected tuple source for bound column reference"
                    ))
                })?;
                (top, offset + self.column_pos)
            }
            None => {
                let pending = frame.pending.ok_or_else(|| {
                    internal_err(format_smolstr!("expected the table being joined"))
                })?;
                (pending, self.column_pos)
            }
        };

        Ok(ir.nodes.add_ref(
            ReferenceTarget::Single(target),
            position,
            self.data_type(),
            None,
            false,
        ))
    }
}

impl<'q> NodeTranslator<'q> for Expr<'q, Analyzed> {
    /// Root id of just added nodes corresponding to AST expression tree.
    type ResType = NodeId;

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        let (inner, meta) = self.into_parts();

        match inner {
            ExprInner::Nil => Err(internal_err(format_smolstr!(
                "expected no undefined expressions"
            ))),
            ExprInner::BinaryOperation(operation) => {
                let (left, right, op) = operation.into_parts();
                let right_is_and = matches!(
                    right.inner_ref(),
                    ExprInner::BinaryOperation(inner) if inner.op == BinaryOp::Boolean(BooleanOp::And)
                );

                // Translate operands into IR
                let left = left.translate(ir, ctx)?;
                let right = right.translate(ir, ctx)?;

                match op {
                    BinaryOp::Arithmetic(op) => Ok(ir.add_arithmetic_to_plan(left, op, right)?),
                    BinaryOp::Comparison(op) => {
                        let op = match op {
                            CmpOp::Eq => Bool::Eq,
                            CmpOp::Neq => Bool::NotEq,
                            CmpOp::Lt => Bool::Lt,
                            CmpOp::Gt => Bool::Gt,
                            CmpOp::Lte => Bool::LtEq,
                            CmpOp::Gte => Bool::GtEq,
                        };
                        Ok(ir.add_cond(left, op, right)?)
                    }
                    BinaryOp::Boolean(BooleanOp::Or) => Ok(ir.add_cond(left, Bool::Or, right)?),
                    // Old pipeline parity: `l AND (rl AND rr)` becomes `(l AND rl) AND rr`.
                    BinaryOp::Boolean(BooleanOp::And) if right_is_and => {
                        let MutExpression::Bool(BoolExpr {
                            left: right_left, ..
                        }) = ir.get_mut_expression_node(right)?
                        else {
                            return Err(internal_err(format_smolstr!("expected AND node")));
                        };
                        let right_left = *right_left;
                        let rotated = ir.add_cond(left, Bool::And, right_left)?;
                        if let MutExpression::Bool(BoolExpr { left, .. }) =
                            ir.get_mut_expression_node(right)?
                        {
                            *left = rotated;
                        }
                        Ok(right)
                    }
                    BinaryOp::Boolean(BooleanOp::And) => Ok(ir.add_cond(left, Bool::And, right)?),
                    BinaryOp::Concat => todo!(),
                }
            }
            ExprInner::UnaryOperation(operation) => {
                let (operand, op) = operation.into_parts();
                match op {
                    UnaryOp::Not => {
                        let child = operand.translate(ir, ctx)?;
                        Ok(ir.add_unary(Unary::Not, child)?)
                    }
                    UnaryOp::Plus | UnaryOp::Minus => todo!(),
                }
            }
            ExprInner::Var(var) => var.translate(ir, ctx),
            ExprInner::Cast(cast) => {
                // An identity cast (the analyzer wraps a bare USING-merged column
                // in one) adds nothing to the plan.
                // shortcut: explicit identity casts are elided too, revisit with full Cast translation.
                let is_identity = cast.identity_child().is_some();
                let (child, ty, _) = cast.into_parts();
                let child_id = child.translate(ir, ctx)?;
                if is_identity {
                    Ok(child_id)
                } else {
                    Ok(ir.add_cast(child_id, ty)?)
                }
            }
            ExprInner::Literal(lit) => {
                let value = Value::try_from(lit)?;
                Ok(ir.add_const(value))
            }
            ExprInner::FunctionCall(call) => {
                let (_, args, resolved_fn) = call.into_parts();
                let ResolvedFn::Aggregate(kind) = resolved_fn else {
                    todo!()
                };

                let (children, targets, is_distinct) = match args {
                    FunctionCallArgs::CountAsterisk => {
                        let child = ir.nodes.push(CountAsterisk {}.into());
                        (vec![child], vec![None], false)
                    }
                    FunctionCallArgs::Exprs { distinct, exprs } => {
                        let mut children = Vec::with_capacity(exprs.len());
                        let mut targets = Vec::with_capacity(exprs.len());
                        for arg in exprs {
                            // Tarantool resolves the overload by the argument's own type.
                            targets.push(
                                arg.data_type()
                                    .get()
                                    .filter(|ty| ty.is_scalar())
                                    .map_or(Ok(None), |ty| CastType::try_from(&ty).map(Some))?,
                            );
                            children.push(arg.translate(ir, ctx)?);
                        }
                        (children, targets, distinct)
                    }
                };

                let fn_id = ir.add_aggregate_function(kind, children, is_distinct)?;
                if let MutExpression::ScalarFunction(ScalarFunction { func_type, .. }) =
                    ir.get_mut_expression_node(fn_id)?
                {
                    *func_type = meta.data_type;
                }
                // After the node exists: `add_aggregate_function` inspects raw arguments.
                ir.cast_fn_args(fn_id, &targets)?;
                Ok(fn_id)
            }
            // ExprInner::SubQuery(_) => "SubQuery",
            // ExprInner::Row(_) => "Row",
            // ExprInner::Array(_) => "Array",
            // ExprInner::WindowFunction(_) => "WindowFunction",
            // ExprInner::Parameter(_) => "Parameter",
            // ExprInner::Like(_) => "Like",
            // ExprInner::Similar(_) => "Similar",
            // ExprInner::Between(_) => "Between",
            // ExprInner::In(_) => "In",
            // ExprInner::Is(_) => "Is",
            // ExprInner::Index(_) => "Index",
            // ExprInner::Trim(_) => "Trim",
            // ExprInner::Substring(_) => "Substring",
            // ExprInner::Case(_) => "Case",
            // ExprInner::Exists(_) => "Exists",
            // ExprInner::TimeFunction(_) => "TimeFunction",
            other => todo!("{}", other.kind_name()),
        }
    }
}

#[cfg(test)]
mod tests;
