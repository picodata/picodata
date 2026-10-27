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

use std::collections::HashSet;

use smol_str::{format_smolstr, SmolStr, ToSmolStr};

use sql_ast_new_nodes::error::{ast_err, ast_invariant_err, AstErr, AstResult};
use sql_ast_new_nodes::expr::{
    BinaryOp, BooleanOp, CmpOp, Expr, ExprInner, FunctionCallArgs, ResolvedFn, UnaryOp,
};
use sql_ast_new_nodes::multiset::{
    Cte, MultisetInner, MultisetStmt, OpDupElimination, Operation, OperationKind,
};
use sql_ast_new_nodes::select::{ProjectionExpr, SelectStmt};
use sql_ast_new_nodes::table_expression::{
    AnalyzedCteOrTable, BoundVar, From, FromEntry, GroupBy, JoinKind, JoinedTable, OrdrByGrpByElem,
    TableFactor, TableFactorInner,
};
use sql_ast_new_nodes::{Analyzed, Ident, Node, SqlOption};

use sql_ast_new_nodes::AbstractSyntaxTree;

use sql_ir::ir::aliases::AliasGenerator;
use sql_ir::ir::index::Index;
use sql_ir::ir::node::expression::MutExpression;
use sql_ir::ir::node::{
    BoolExpr, CountAsterisk, NodeId, ReferenceAsteriskSource, ReferenceTarget, ScalarFunction,
};
use sql_ir::ir::operator::{Bool, JoinKind as IrJoinKind, Unary};
use sql_ir::ir::subtree_cloner::SubtreeCloner;
use sql_ir::ir::types::CastType;
use sql_ir::ir::value::Value;
use sql_ir::ir::Plan;

/// A valid query the translator does not support yet (a limitation, not a bug).
pub(crate) fn unsupported_err(msg: SmolStr) -> AstErr {
    ast_err(msg)
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
    /// FROM aliases and CTE names of the whole query.
    used_aliases: HashSet<SmolStr>,
    /// Unaliased FROM subqueries, named once every alias is known.
    unnamed_subqueries: Vec<NodeId>,
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
    /// Expression subqueries waiting for the relational node that owns them.
    subqueries: Vec<NodeId>,
}

impl Ctx<'_> {
    fn empty() -> Self {
        Self {
            frames: Vec::new(),
            proj_exprs: Vec::new(),
            used_aliases: HashSet::new(),
            unnamed_subqueries: Vec::new(),
        }
    }

    fn frame_mut(&mut self) -> AstResult<&mut Frame> {
        self.frames
            .last_mut()
            .ok_or_else(|| internal_err(format_smolstr!("expected actual statement frame")))
    }

    fn take_subqueries(&mut self) -> AstResult<Vec<NodeId>> {
        Ok(std::mem::take(&mut self.frame_mut()?.subqueries))
    }

    /// Hands the subqueries of just translated expressions to their owner.
    fn attach_subqueries(&mut self, ir: &mut Plan, owner: NodeId) -> AstResult<()> {
        let subqueries = self.take_subqueries()?;
        ir.set_subqueries(owner, subqueries)?;
        Ok(())
    }
}

/// Select list item while GROUP BY may still consume it.
enum ProjExpr<'q> {
    Orig(ProjectionExpr<'q, Analyzed>),
    /// Translated as a GROUP BY key; the select list clones its subtree.
    Translated(NodeId, Option<Ident>, Option<ReferenceAsteriskSource>),
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

        // A generated name must not clash with any name of the query, as in the old pipeline.
        ctx.used_aliases.extend(ir.relations.tables.keys().cloned());
        let mut aliases = AliasGenerator::new(ctx.used_aliases);
        for id in ctx.unnamed_subqueries {
            let name = aliases.subquery_alias(&ir);
            ir.get_mut_relation_node(id)?.set_scan_name(Some(name))?;
        }
        Ok(ir)
    }
}

impl<'q> NodeTranslator<'q> for Node<'q, Analyzed> {
    type ResType = ();

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        match self {
            Node::Empty => return Err(internal_err(format_smolstr!("expected non-empty tree"))),
            Node::DqlStmt(dql_stmt) => {
                let root = dql_stmt.stmt.translate(ir, ctx)?;
                ir.set_top(root)?;
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
    /// Root of the statement; the caller's top is left as it was.
    type ResType = NodeId;

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        // The statement is translated from an empty top.
        let saved_top = ir.reset_top();

        // Split the whole statement into parts and handle them separately.
        let (ctes, inner, order_by, limit) = self.into_parts();

        // shortcut: reject rather than drop ORDER BY/LIMIT, lift once they are translated.
        if order_by.is_some() || limit.is_some() {
            return Err(unsupported_err(format_smolstr!(
                "ORDER BY and LIMIT are not supported"
            )));
        }

        // Initialize scope we are entering
        ctx.frames.push(Frame::default());

        for cte in ctes.into() {
            // Translate CTE and save its root in the actual scope.
            let cte_id = cte.translate(ir, ctx)?;
            ctx.frame_mut()?.ctes.push(cte_id);
        }

        // Handle each variant of statement inner body.
        match inner {
            MultisetInner::Select(stmt) => stmt.translate(ir, ctx)?,
            MultisetInner::Values(_values) => todo!(),
            MultisetInner::Operation(operation) => operation.translate(ir, ctx)?,
        };

        // Drop the scope we are leaving; each of its subqueries has an owner.
        let frame = ctx.frames.pop();
        debug_assert!(frame.is_some_and(|frame| frame.subqueries.is_empty()));

        let root = ir
            .reset_top()
            .ok_or_else(|| internal_err(format_smolstr!("expected statement root")))?;
        if let Some(top) = saved_top {
            ir.set_top(top)?;
        }
        Ok(root)
    }
}

impl<'q> NodeTranslator<'q> for Cte<'q, Analyzed> {
    type ResType = NodeId;

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        let (name, columns, body) = self.into_parts();
        ctx.used_aliases.insert(name.as_str().into());

        // Translate CTE body
        let body_id = body.translate(ir, ctx)?;

        // Align CTE body with its output column aliases
        let cte_id = ir.transform_cte_columns(
            body_id,
            name.take(),
            columns.into_iter().map(Ident::take).collect(),
        )?;

        Ok(cte_id)
    }
}

/// A statement nested in an expression.
struct ExprSubQuery<'q>(Box<MultisetStmt<'q, Analyzed>>);

impl<'q> NodeTranslator<'q> for ExprSubQuery<'q> {
    /// Row of references to the subquery columns.
    type ResType = NodeId;

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        let root = self.0.translate(ir, ctx)?;
        let sq_id = ir.add_sub_query(root, None)?;
        ctx.frame_mut()?.subqueries.push(sq_id);
        Ok(ir.add_row_from_subquery(sq_id)?)
    }
}

impl<'q> NodeTranslator<'q> for Operation<'q, Analyzed> {
    type ResType = ();

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        let Operation {
            left,
            op,
            dup_elimination,
            right,
        } = self;

        let left_id = left.translate(ir, ctx)?;
        let right_id = right.translate(ir, ctx)?;

        let op_id = match (op, dup_elimination) {
            (OperationKind::Union, dup) => {
                ir.add_union(left_id, right_id, dup == OpDupElimination::Distinct)?
            }
            (OperationKind::Except, OpDupElimination::Distinct) => {
                ir.add_except(left_id, right_id)?
            }
            (OperationKind::Except, OpDupElimination::All) => {
                return Err(unsupported_err(format_smolstr!(
                    "EXCEPT ALL is not supported"
                )))
            }
            (OperationKind::Intersect, _) => {
                return Err(unsupported_err(format_smolstr!(
                    "INTERSECT is not supported"
                )))
            }
        };

        ir.set_top(op_id)?;
        Ok(())
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
                ctx.attach_subqueries(ir, select_id)?;
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
                ctx.attach_subqueries(ir, having_id)?;
                ir.set_top(having_id)?;
            }

            grby_child
        } else {
            None
        };

        // Add IR nodes for each expression in SELECT list.
        let mut out_vars = Vec::with_capacity(proj_exprs.len());

        for proj_expr in proj_exprs {
            let (expr_id, name, star) = match proj_expr {
                ProjExpr::Orig(proj) => {
                    let name = proj.output_name().map(Ident::normalized);
                    let star = proj.asterisk_source().cloned();
                    let (expr, _) = proj.into_parts();
                    (expr.translate(ir, ctx)?, name, star)
                }
                ProjExpr::Translated(key_id, name, star) => {
                    // The key reads the GroupBy child; its projected copy reads the top.
                    let expr_id = SubtreeCloner::clone_subtree(ir, key_id)?;
                    let grby_child = grby_child.ok_or_else(|| {
                        internal_err(format_smolstr!("expected GROUP BY for translated key"))
                    })?;
                    let top_id = ir.get_top()?;
                    ir.replace_target_in_subtree(expr_id, grby_child, top_id)?;
                    (expr_id, name, star)
                }
            };

            // Every item of a marked asterisk must be a marked reference: the
            // storage SQL writes one `*` for them, which brings back any item
            // left unmarked.
            if let Some(star) = star {
                ir.set_asterisk_source(expr_id, star)?;
            }

            // The analyzer names every item.
            let name = name.ok_or_else(|| internal_err(format_smolstr!("expected item name")))?;
            out_vars.push(ir.nodes.add_alias(name.as_str(), expr_id)?);
        }

        let top_id = if let Ok(src_rel) = ir.get_top() {
            // Add projection node over some tuple source.
            let output = ir.nodes.add_row(out_vars);
            let subqueries = ctx.take_subqueries()?;
            ir.add_projection(src_rel, output, subqueries, vec![], is_distinct)?
        } else {
            // No tuple source add SelectWithoutScan
            let select_id = ir.add_select_without_scan(out_vars)?;
            ctx.attach_subqueries(ir, select_id)?;
            select_id
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
        ctx.attach_subqueries(ir, grby_id)?;
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
                let name = proj.output_name().map(Ident::normalized);
                let star = proj.asterisk_source().cloned();
                let (expr, _) = proj.into_parts();
                let sq_cnt = ctx.frame_mut()?.subqueries.len();
                let key_id = expr.translate(ir, ctx)?;
                // shortcut: the select-list copy of the key would read a subquery
                // owned by GroupBy, lift once the copy reads the key instead.
                if ctx.frame_mut()?.subqueries.len() != sq_cnt {
                    return Err(unsupported_err(format_smolstr!(
                        "GROUP BY ordinal of an item with a subquery is not supported"
                    )));
                }
                ctx.proj_exprs[pos] = ProjExpr::Translated(key_id, name, star);
                Ok(key_id)
            }
            // Repeated ordinal: group by a copy of the key.
            ProjExpr::Translated(key_id, name, star) => {
                ctx.proj_exprs[pos] = ProjExpr::Translated(key_id, name, star);
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
        ctx.attach_subqueries(ir, join_id)?;

        // Now IR top is newly created join.
        ir.set_top(join_id)?;
        Ok(())
    }
}

impl<'q> NodeTranslator<'q> for TableFactor<'q, Analyzed> {
    type ResType = ();

    fn translate(self, ir: &mut Plan, ctx: &mut Ctx<'q>) -> AstResult<Self::ResType> {
        if let Some(alias) = &self.alias {
            ctx.used_aliases.insert(alias.as_str().into());
        }
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
            TableFactorInner::SubQuery(sq) => {
                let root = sq.translate(ir, ctx)?;
                let unnamed = self.alias.is_none();
                let scan_id = ir.add_sub_query(root, self.alias.map(Ident::take))?;
                if unnamed {
                    ctx.unnamed_subqueries.push(scan_id);
                }
                ir.set_top(scan_id)?;
            }
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
            ExprInner::SubQuery(sq) => ExprSubQuery(sq).translate(ir, ctx),
            ExprInner::Exists(exists) => {
                let (is_not, sq) = exists.into_parts();
                let row = ExprSubQuery(sq).translate(ir, ctx)?;
                let exists_id = ir.add_unary(Unary::Exists, row)?;
                Ok(if is_not {
                    ir.add_unary(Unary::Not, exists_id)?
                } else {
                    exists_id
                })
            }
            ExprInner::In(in_expr) => {
                let (is_not, left, rhs) = in_expr.into_parts();
                // shortcut: CAST(row) is no IN list, lift once the cast moves into the subquery.
                if matches!(rhs.inner_ref(), ExprInner::Cast(_)) {
                    return Err(unsupported_err(format_smolstr!(
                        "IN over a differently typed subquery is not supported"
                    )));
                }
                let left = left.translate(ir, ctx)?;
                let rhs = rhs.translate(ir, ctx)?;
                let in_id = ir.add_cond(left, Bool::In, rhs)?;
                Ok(if is_not {
                    ir.add_unary(Unary::Not, in_id)?
                } else {
                    in_id
                })
            }
            // ExprInner::Row(_) => "Row",
            // ExprInner::Array(_) => "Array",
            // ExprInner::WindowFunction(_) => "WindowFunction",
            // ExprInner::Parameter(_) => "Parameter",
            // ExprInner::Like(_) => "Like",
            // ExprInner::Similar(_) => "Similar",
            // ExprInner::Between(_) => "Between",
            // ExprInner::Is(_) => "Is",
            // ExprInner::Index(_) => "Index",
            // ExprInner::Trim(_) => "Trim",
            // ExprInner::Substring(_) => "Substring",
            // ExprInner::Case(_) => "Case",
            // ExprInner::TimeFunction(_) => "TimeFunction",
            other => todo!("{}", other.kind_name()),
        }
    }
}

#[cfg(test)]
mod tests;
