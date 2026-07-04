use ahash::AHashMap;
use pest::iterators::Pair;
use smol_str::{format_smolstr, SmolStr};
use std::collections::{HashMap, VecDeque};

use crate::errors::SbroadError;
use crate::frontend::sql::ast::{PairToAstIdTranslation, Rule};
use crate::frontend::sql::ir::{SubtreeCloner, Translation};
use crate::ir::expression::{ColumnPositionMap, Position};
use crate::ir::metadata::Metadata;
use crate::ir::node::expression::MutExpression;
use crate::ir::node::{BoolExpr, NodeId};
use crate::ir::operator::Bool;
use crate::ir::types::DerivedType;
use crate::ir::Plan;

use super::try_deconstruct_between_expr;

/// Number of relational nodes we expect to retrieve column positions for.
pub(in crate::frontend::sql) const COLUMN_POSITIONS_CACHE_CAPACITY: usize = 10;
/// Total average number of Reference (`ReferenceContinuation`) rule nodes that we expect to get
/// in the parsing result returned from pest. Used for preallocating `reference_to_name_map`.
pub(in crate::frontend::sql) const REFERENCES_MAP_CAPACITY: usize = 50;

/// Helper struct holding values and references needed for `parse_expr` calls.
/// One LET declaration tracked by the parser's block scope.
///
/// Stored in execution order by [`LetVarScope`]. `used` flips to `true` the
/// first time a downstream block statement resolves to this declaration;
#[derive(Debug, Clone)]
pub(in crate::frontend::sql) struct LetVarDecl {
    /// Id of the RHS query.
    pub(in crate::frontend::sql) query_id: NodeId,
    /// Type derived from the LET RHS (may be `Unknown` when the RHS is e.g.
    /// a plain `SELECT $1` with an unconstrained parameter).
    // FIXME: Infer the type using type system so there will be no variables of unknown type.
    pub(in crate::frontend::sql) ty: DerivedType,
    /// Whether at least one later block statement referenced this
    /// declaration. Resets to `false` on redeclaration.
    pub(in crate::frontend::sql) used: bool,
}

/// LET-variable scope for a single transactional block.
///
/// The parser walks the AST in post-order; we install a new declaration into
/// the scope when the master loop hits the corresponding `BlockLetStatement`,
/// which happens *after* the LET RHS subtree has been planned. Subsequent
/// block-statement subtrees see the updated scope when they run identifier
/// resolution inside `parse_expr_pratt`.
///
/// Shadowing is allowed only when the new RHS type matches the previously
/// recorded type; otherwise the parser rejects the redeclaration.
#[derive(Debug, Default, Clone)]
pub(in crate::frontend::sql) struct LetVarScope {
    /// All declarations in source order. We keep the full history (rather
    /// than a flat name → decl map) so the unused-LET check can point at
    /// the specific shadowed binding that was never referenced.
    pub(in crate::frontend::sql) decls: Vec<LetVarDecl>,
    /// `name → index into decls` of the currently-active binding for that
    /// name. Lookups consult only this map; shadowed entries are reachable
    /// only via `decls` for diagnostics.
    pub(in crate::frontend::sql) by_name: HashMap<SmolStr, usize>,
}

impl LetVarScope {
    pub(in crate::frontend::sql) fn lookup(&self, name: &str) -> Option<&LetVarDecl> {
        self.by_name.get(name).map(|idx| &self.decls[*idx])
    }

    pub(in crate::frontend::sql) fn mark_used(&mut self, name: &str) {
        if let Some(idx) = self.by_name.get(name).copied() {
            self.decls[idx].used = true;
        }
    }

    /// Push a fresh LET declaration. Returns an error if an existing binding
    /// with the same name has a different type — same-type redeclaration is
    /// allowed and shares the runtime aVar slot (see `build_var_slots` in
    /// `src/vdbe/txn.rs`).
    pub(in crate::frontend::sql) fn declare(
        &mut self,
        id: NodeId,
        name: SmolStr,
        ty: DerivedType,
    ) -> Result<(), SbroadError> {
        if let Some(prev) = self.lookup(&name) {
            // Compare only when both sides have a known type. An unknown
            // type matches anything.
            if let (Some(prev_ty), Some(new_ty)) = (prev.ty.get(), ty.get()) {
                if prev_ty != new_ty {
                    return Err(SbroadError::Other(format_smolstr!(
                        "LET variable \"{name}\" cannot be redeclared with a different type \
                         (was {prev_ty}, now {new_ty})"
                    )));
                }
            }
        }
        let idx = self.decls.len();
        self.decls.push(LetVarDecl {
            query_id: id,
            ty,
            used: false,
        });
        self.by_name.insert(name, idx);
        Ok(())
    }
}

pub(in crate::frontend::sql) struct ExpressionWalker<'worker, M>
where
    M: Metadata,
{
    /// Helper map of { sq_pair -> ast_id } used for identifying SQ nodes
    /// during both general and Pratt parsing.
    pub(in crate::frontend::sql) sq_pair_to_ast_ids: &'worker PairToAstIdTranslation<'worker>,
    /// Map of { sq_ast_id -> sq_plan_id }.
    /// Used instead of reference to general `map` using during
    /// parsing in order no to take an immutable reference on it.
    pub(in crate::frontend::sql) sq_ast_to_plan_id: Translation,
    /// Vec of BETWEEN expressions met during parsing.
    /// Used later to fix them as soon as we need to resolve double-linking problem
    /// of left expression.
    pub(in crate::frontend::sql) betweens: Vec<NodeId>,
    /// Map of { subquery_id -> row_id }
    /// that is used to fix `betweens`, which children (references under rows)
    /// may have been changed.
    /// We can't fix between child (clone expression subtree) in the place of their creation,
    /// because we'll copy references without adequate `parent` and `target` that we couldn't fix
    /// later.
    pub(in crate::frontend::sql) subquery_replaces: AHashMap<NodeId, NodeId>,
    /// Vec of { sq_id }
    /// After calling `parse_expr` and creating relational node that can contain SubQuery as
    /// additional child (Selection, Join, Having, OrderBy, GroupBy, Projection) we should pop the
    /// queue till it's not empty and:
    /// * Add subqueries to the list of relational children
    pub(in crate::frontend::sql) sub_queries_to_fix_queue: VecDeque<NodeId>,
    // Tnt parameter pairs positions in the query. This map is used to index tnt parameters.
    // For example, for query `select ? + ?` this vector will contain 2 pairs for `?`, and the pair
    // on the left will be the first, while the right pair will be the second.
    pub(in crate::frontend::sql) tnt_parameters_positions: Vec<Pair<'worker, Rule>>,
    pub(in crate::frontend::sql) metadata: &'worker M,
    /// Map of { reference plan_id -> it's column name}
    /// We have to save column name in order to use it later for alias creation.
    pub(in crate::frontend::sql) reference_to_name_map: HashMap<NodeId, SmolStr>,
    /// Map of (relational_node_id, columns_position_map).
    /// As `ColumnPositionMap` is used for parsing references and as it may be shared for the same
    /// relational node we cache it so that we don't have to recreate it every time.
    pub(in crate::frontend::sql) column_positions_cache: HashMap<NodeId, ColumnPositionMap>,
    /// Inside WindowBody node.
    /// Used to correctly process subqueries inside window body.
    pub(in crate::frontend::sql) inside_window_body: bool,
    /// Vec of window nodes in the current projection context.
    /// Stores window definitions in order of appearance, including both named and inline windows.
    pub(in crate::frontend::sql) curr_windows: Vec<NodeId>,
    /// Named windows nodes, stack of projection contexts for named windows.
    /// Example with >1 context:
    /// select distinct 1 from t group by (select row_number() over w from t window w as () limit 1) window w as ()
    pub(in crate::frontend::sql) named_windows_stack: Vec<HashMap<SmolStr, NodeId>>,
    /// Named windows for current projection.
    pub(in crate::frontend::sql) curr_named_windows: HashMap<SmolStr, NodeId>,
    /// Window node by SubQuery NodeId.
    /// This is used when window contains subquery.
    /// This is used to avoid unnecessary subqueries linked to projection.
    pub(in crate::frontend::sql) named_windows_sqs: HashMap<NodeId, NodeId>,
    /// Subqueries inside current Window node.
    pub(in crate::frontend::sql) curr_window_sqs: Vec<NodeId>,
    /// Are we inside a GroupBy grouping expression.
    pub(in crate::frontend::sql) inside_grouping_expression: bool,
    /// LET-variable scope for the current anonymous block.
    pub(in crate::frontend::sql) let_scope: LetVarScope,
    /// Maps a `BlockLetStatement` AST node id to the LET variable's
    /// normalized name, so `parse_anonymous_block` can recover the name
    /// when it builds `BlockStatement::Let { var, query }` from the
    /// `Translation` map (which carries only the RHS plan id).
    pub(in crate::frontend::sql) let_var_names: HashMap<usize, SmolStr>,
}

impl<'worker, M> ExpressionWalker<'worker, M>
where
    M: Metadata,
{
    pub(in crate::frontend::sql) fn new<'plan: 'worker, 'meta: 'worker>(
        metadata: &'meta M,
        sq_pair_to_ast_ids: &'worker PairToAstIdTranslation,
        tnt_parameters_positions: Vec<Pair<'worker, Rule>>,
    ) -> Self {
        Self {
            sq_pair_to_ast_ids,
            sq_ast_to_plan_id: Translation::with_capacity(sq_pair_to_ast_ids.len()),
            subquery_replaces: AHashMap::new(),
            sub_queries_to_fix_queue: VecDeque::new(),
            metadata,
            tnt_parameters_positions,
            betweens: Vec::new(),
            reference_to_name_map: HashMap::with_capacity(REFERENCES_MAP_CAPACITY),
            column_positions_cache: HashMap::with_capacity(COLUMN_POSITIONS_CACHE_CAPACITY),
            inside_window_body: false,
            curr_windows: Vec::new(),
            named_windows_stack: Vec::new(),
            curr_named_windows: HashMap::new(),
            named_windows_sqs: HashMap::new(),
            curr_window_sqs: Vec::new(),
            inside_grouping_expression: false,
            let_scope: LetVarScope::default(),
            let_var_names: HashMap::new(),
        }
    }

    pub(in crate::frontend::sql) fn build_columns_map(
        &mut self,
        plan: &Plan,
        rel_id: NodeId,
    ) -> Result<(), SbroadError> {
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.column_positions_cache.entry(rel_id)
        {
            let new_map = ColumnPositionMap::new(plan, rel_id)?;
            e.insert(new_map);
        }

        Ok(())
    }

    pub(in crate::frontend::sql) fn columns_map_get_positions(
        &self,
        rel_id: NodeId,
        col_name: &str,
        scan_name: Option<&str>,
    ) -> Result<Position, SbroadError> {
        let col_map = self
            .column_positions_cache
            .get(&rel_id)
            .expect("Columns map should be in the cache already");

        if let Some(scan_name) = scan_name {
            col_map.get_with_scan(col_name, Some(scan_name))
        } else {
            col_map.get(col_name)
        }
    }

    /// Resolve the double linking problem in BETWEEN operator. On the AST to IR step
    /// we transform `left BETWEEN center AND right` construction into
    /// `left >= center AND left <= right`, where the same `left` expression is reused
    /// twice. So, We need to copy the 'left' expression tree from `left >= center` to the
    /// `left <= right` expression.
    ///
    /// Otherwise, we'll have problems on the dispatch stage while taking nodes from the original
    /// plan to build a sub-plan for the storage. If the same `left` subtree is used twice in
    /// the plan, these nodes are taken while traversing the `left >= center` expression and
    /// nothing is left for the `left <= right` sutree.
    pub(in crate::frontend::sql) fn fix_betweens(
        &self,
        plan: &mut Plan,
    ) -> Result<(), SbroadError> {
        for between_id in &self.betweens {
            let between = plan.get_expression_node(*between_id)?;
            let ((_lhs_id, lhs), (rhs_id, _rhs)) =
                try_deconstruct_between_expr(plan, &between).expect("malformed BETWEEN");

            // This pass only clones the BETWEEN lhs subtree. The expression types do not
            // change here; we only create fresh `NodeId`s for the copied nodes.
            let cloned_lower_bound = match self.subquery_replaces.get(&lhs.left) {
                Some(id) => SubtreeCloner::clone_subtree(plan, *id)?,
                None => SubtreeCloner::clone_subtree(plan, lhs.left)?,
            };

            let rhs = plan.get_mut_expression_node(rhs_id)?;
            if let MutExpression::Bool(BoolExpr { ref mut left, .. }) = rhs {
                *left = cloned_lower_bound;
            } else {
                panic!("Expected to see LEQ expression.")
            }

            // Finally, replace `Bool::Between` with `Bool::And`.
            // See the explanation for `ParseExpression::FinalBetween` in pratt parser.
            let between = plan.get_mut_expression_node(*between_id)?;
            if let MutExpression::Bool(BoolExpr { op, .. }) = between {
                *op = Bool::And;
            }
        }

        Ok(())
    }
}
