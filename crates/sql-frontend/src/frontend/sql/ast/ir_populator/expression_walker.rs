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
use ahash::AHashMap;
use pest::iterators::Pair;
use smol_str::{format_smolstr, SmolStr};
use std::collections::{HashMap, HashSet, VecDeque};

use super::try_deconstruct_between_expr;

/// Number of relational nodes we expect to retrieve column positions for.
pub(in crate::frontend::sql) const COLUMN_POSITIONS_CACHE_CAPACITY: usize = 10;
/// Total average number of Reference (`ReferenceContinuation`) rule nodes that we expect to get
/// in the parsing result returned from pest. Used for preallocating `reference_to_name_map`.
pub(in crate::frontend::sql) const REFERENCES_MAP_CAPACITY: usize = 50;

/// One LET declaration tracked by the parser's block scope.
///
/// Stored in execution order by [`LetVarScope`]. `used` flips to `true` the
/// first time a downstream block statement resolves to this declaration.
#[derive(Debug, Clone)]
pub(in crate::frontend::sql) struct LetVarDecl {
    /// Normalized variable name. It is the same name every reference to this
    /// binding compiles to (`:{name}`).
    pub(in crate::frontend::sql) name: SmolStr,
    /// Type derived from the LET RHS (may be `Unknown` when the RHS is e.g.
    /// a plain `SELECT $1` with an unconstrained parameter).
    // FIXME: Infer the type using type system so there will be no variables of unknown type.
    pub(in crate::frontend::sql) ty: DerivedType,
    /// Whether at least one later block statement referenced this
    /// declaration. A redeclaration starts over with a fresh entry, so the
    /// flag always describes one binding rather than a name.
    pub(in crate::frontend::sql) is_used: bool,
}

/// A visibility scope for IF body.
/// Used to invalidate names of its LET variables.
#[derive(Debug, Clone, Default)]
struct LetScopeFrame {
    declared: HashSet<SmolStr>,
}

impl LetScopeFrame {
    fn binds(&self, name: &str) -> bool {
        self.declared.contains(name)
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::frontend::sql) enum LetVarLookup<T> {
    /// A variable that is live here.
    Live(T),
    /// A variable this block declared, but whose scope has since ended.
    OutOfScope,
    /// Not a LET variable at all -- a column, or a typo.
    Unknown,
}

impl<T> LetVarLookup<T> {
    fn into_option(self) -> Option<T> {
        match self {
            LetVarLookup::Live(x) => Some(x),
            LetVarLookup::OutOfScope => None,
            LetVarLookup::Unknown => None,
        }
    }
}

/// Reject a LET name that cannot survive the trip through generated SQL.
///
/// References compile to `:{name}`, so the name has to be something the
/// storage's SQL lexer reads back as one bound-parameter token, and something
/// `ParamName` (in `src/vdbe/txn.rs`) can tell apart from a positional `:N`
/// parameter. Delimited identifiers let a user write anything at all --
/// `LET "my var"`, `LET "1"` -- so the check has to happen here rather than in
/// the grammar.
fn validate_let_var_name(name: &str) -> Result<(), SbroadError> {
    let invalid = |reason: &str| {
        Err(SbroadError::Other(format_smolstr!(
            "LET variable name \"{name}\" is invalid: {reason}. A name must \
             start with a letter or underscore and hold only letters, digits \
             and underscores"
        )))
    };
    match name.chars().next() {
        None => return invalid("it is empty"),
        Some(c) if c.is_ascii_digit() => {
            // `:1` is indistinguishable from positional parameter $1, and
            // `:1x` does not even parse as one.
            return invalid("it starts with a digit");
        }
        Some(c) if !c.is_alphabetic() && c != '_' => {
            return invalid("it starts with a character that is not a letter or underscore")
        }
        Some(_) => (),
    }
    if let Some(c) = name.chars().find(|c| !c.is_alphanumeric() && *c != '_') {
        return invalid(&format!("it contains {c:?}"));
    }
    Ok(())
}

/// LET-variable scope for a single transactional block.
///
/// The parser walks the AST in post-order; we install a new declaration into
/// the scope when the master loop hits the corresponding `BlockLetStatement`,
/// which happens *after* the LET RHS subtree has been planned. Subsequent
/// block-statement subtrees see the updated scope when they run identifier
/// resolution inside `parse_expr_pratt`.
///
/// An IF body opens a nested scope: a LET declared in it is a new variable
/// that disappears at `END IF`. Within one scope a repeated LET is a
/// re-assignment instead, allowed only when the new RHS type matches the
/// recorded one.
///
/// A name may be reused by scopes that do not overlap -- two sibling IF bodies,
/// or a body and the top level after it -- and those are unrelated variables
/// that happen to share a runtime slot. Sharing is safe because a reference
/// only ever resolves to a binding that is live at that point, and a live
/// binding is always assigned earlier in execution order than the reference,
/// with no other assignment to the name in between.
#[derive(Debug, Default, Clone)]
pub(in crate::frontend::sql) struct LetVarScope {
    /// All declarations in source order. We keep the full history (rather
    /// than a flat name → decl map) so the unused-LET check can point at
    /// the specific binding that was never referenced.
    pub(in crate::frontend::sql) decls: Vec<LetVarDecl>,
    /// Every name ever declared in this block, mapped to the index of its
    /// currently-active binding in `decls`, or `None` once that binding went
    /// out of scope. Keeping the dead names lets us tell "never heard of it"
    /// apart from "went out of scope at END IF"; their declarations stay
    /// reachable via `decls` for diagnostics.
    by_name: HashMap<SmolStr, Option<usize>>,
    /// RHS query id → index of its declaration in `decls`. Lets the block
    /// builder recover a binding from the only thing the `Translation` map
    /// gives it -- the RHS plan id. Ids are unique per LET, so entries are
    /// never overwritten, and unlike `by_name` they stay valid after the
    /// binding goes out of scope.
    by_id: HashMap<NodeId, usize>,
    /// Open IF bodies, innermost last.
    frames: Vec<LetScopeFrame>,
}

impl LetVarScope {
    pub(in crate::frontend::sql) fn out_of_scope_error(name: &str) -> SbroadError {
        SbroadError::Other(format_smolstr!("LET variable \"{name}\" is out of scope"))
    }

    pub(in crate::frontend::sql) fn push_frame(&mut self) {
        self.frames.push(LetScopeFrame::default());
    }

    /// Unbinds the names the closing body introduced. The names stay in
    /// `by_name` (pointing at no binding) and their declarations stay in
    /// `decls` for the unused-LET report.
    pub(in crate::frontend::sql) fn pop_frame(&mut self) {
        let Some(frame) = self.frames.pop() else {
            debug_assert!(false, "END IF without a matching IF body scope");
            return;
        };
        for name in &frame.declared {
            if let Some(binding) = self.by_name.get_mut(name) {
                *binding = None;
            }
        }
    }

    /// Check if a LET variable is defined in the innermost frame. Outside any
    /// IF body there is no frame, and every live binding is local.
    pub(in crate::frontend::sql) fn is_local(&self, name: &str) -> bool {
        let innermost_frame = self.frames.last();
        innermost_frame.map(|last| last.binds(name)).unwrap_or(true)
    }

    /// Look a declaration up by its RHS query id. Unlike [`Self::resolve_by_name`],
    /// this works for bindings whose scope has already ended, which is what
    /// the block builder needs: it visits every LET of the block after the
    /// whole AST has been walked.
    pub(in crate::frontend::sql) fn resolve_by_id(&self, id: NodeId) -> Option<&LetVarDecl> {
        self.by_id.get(&id).map(|idx| &self.decls[*idx])
    }

    /// Try resolving a LET variable by name and return a const reference to it.
    pub(in crate::frontend::sql) fn resolve_by_name(
        &self,
        name: &str,
    ) -> LetVarLookup<&LetVarDecl> {
        match self.by_name.get(name) {
            Some(Some(idx)) => LetVarLookup::Live(&self.decls[*idx]),
            Some(None) => LetVarLookup::OutOfScope,
            None => LetVarLookup::Unknown,
        }
    }

    /// Try resolving a LET variable by name and return a mut reference to it.
    pub(in crate::frontend::sql) fn resolve_by_name_mut(
        &mut self,
        name: &str,
    ) -> LetVarLookup<&mut LetVarDecl> {
        match self.by_name.get(name) {
            Some(Some(idx)) => LetVarLookup::Live(&mut self.decls[*idx]),
            Some(None) => LetVarLookup::OutOfScope,
            None => LetVarLookup::Unknown,
        }
    }

    /// Mark a LET variable as used. This affects EXPLAIN output.
    pub(in crate::frontend::sql) fn mark_as_used(&mut self, name: &str) {
        if let Some(decl) = self.resolve_by_name_mut(name).into_option() {
            decl.is_used = true;
        }
    }

    /// Push a fresh LET declaration for `name`.
    ///
    /// Re-declaring a variable of the innermost scope re-assigns it, so the
    /// types have to match. A name nothing else is using introduces a new
    /// variable of any type.
    ///
    /// Re-declaring one that belongs to an *enclosing* scope is rejected. It
    /// could only mean shadowing, and since a scope ends at `END IF`, the
    /// value assigned inside would be silently thrown away there -- while the
    /// obvious reading is that the outer variable was being updated. There is
    /// no separate assignment syntax to say which was meant, so we make the
    /// author rename instead of guessing.
    pub(in crate::frontend::sql) fn declare(
        &mut self,
        id: NodeId,
        name: SmolStr,
        ty: DerivedType,
    ) -> Result<(), SbroadError> {
        validate_let_var_name(&name)?;

        match self.resolve_by_name(&name).into_option() {
            // Local scope has a binding with this name.
            Some(prev) if self.is_local(&name) => {
                // Compare only when both sides have a known type. An unknown type matches anything.
                if let (Some(prev_ty), Some(new_ty)) = (prev.ty.get(), ty.get()) {
                    if prev_ty != new_ty {
                        return Err(SbroadError::Other(format_smolstr!(
                            "LET variable \"{name}\" cannot be redeclared with a different type \
                             (was {prev_ty}, now {new_ty})"
                        )));
                    }
                }
            }
            // Parent scope has a binding with this name.
            Some(_) => {
                return Err(SbroadError::Other(format_smolstr!(
                    "LET variable \"{name}\" is already declared outside IF body"
                )))
            }
            // A name this scope has not bound yet. Record it on the frame so
            // that `END IF` unbinds it again. At the top level there is no
            // frame and nothing to unbind.
            None => {
                if let Some(frame) = self.frames.last_mut() {
                    frame.declared.insert(name.clone());
                }
            }
        }

        let idx = self.decls.len();
        self.by_name.insert(name.clone(), Some(idx));
        let prev = self.by_id.insert(id, idx);
        debug_assert!(prev.is_none(), "two LET declarations share an RHS query");
        self.decls.push(LetVarDecl {
            is_used: false,
            name,
            ty,
        });

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
        }
    }

    pub(in crate::frontend::sql) fn build_columns_map(
        &mut self,
        plan: &Plan,
        rel_id: NodeId,
    ) -> Result<(), SbroadError> {
        use std::collections::hash_map::Entry;
        if let Entry::Vacant(e) = self.column_positions_cache.entry(rel_id) {
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

    pub(in crate::frontend::sql) fn resolves_to_column(
        &mut self,
        plan: &Plan,
        referred_relation_ids: &[NodeId],
        col_name: &str,
    ) -> bool {
        referred_relation_ids.iter().any(|rel_id| {
            self.build_columns_map(plan, *rel_id).is_ok()
                && self
                    .columns_map_get_positions(*rel_id, col_name, None)
                    .is_ok()
        })
    }

    /// Reset the window context accumulated for the projection we have just built.
    /// Otherwise the parsed windows leak into the next `Projection` that is built
    /// by the same worker.
    pub(in crate::frontend::sql) fn reset_windows(&mut self) {
        self.curr_named_windows.clear();
        self.curr_windows.clear();
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
