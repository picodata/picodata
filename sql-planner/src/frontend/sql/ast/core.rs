//! AST core module.
//!
//! Contains the `AstCore` structure (arena of `ParseNode`s built from the
//! `pest` parse tree) and all parsing routines operating on it.

extern crate pest;

use std::mem::swap;

use crate::frontend::sql::type_system;

use crate::ir::node::deallocate::Deallocate;
use crate::ir::node::{
    Alias, AlterColumn, AlterTable, AlterTableOp, AnonymousBlock, Backup, BlockEntries,
    BlockStatement, Bound, BoundType, Frame, FrameType, Reference, ReferenceAsteriskSource,
    RenameIndex, Row, SubQueryReference, TruncateTable, Values, ValuesRow, Window,
};
use crate::ir::types::{DerivedType, NestedType, UnrestrictedType};
use ::core::panic;
use itertools::Itertools;
use pest::iterators::{Pair, Pairs};
use pest::Parser;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};
use tarantool::index::{IndexType, RtreeIndexDistanceType};

use crate::errors::Entity::AST;
use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::{normalize_name_from_sql, to_user};
use crate::executor::engine::Metadata;
use crate::frontend::sql::{error, insert_conflict};

use crate::frontend::sql::ir::Translation;

use crate::ir::helpers::RepeatableState;
use crate::ir::node::relational::Relational;
use crate::ir::node::{
    AuditPolicy, CallProcedure, Constant, CreateIndex, CreateProc, CreateTable, DropIndex,
    DropProc, DropTable, Node, NodeId, RenameRoutine, SetParam, VinylOptions,
};
use crate::ir::operator::{OrderByElement, OrderByEntity, OrderByType, Unary};
use crate::ir::options::{OptionKind, OptionParamValue};
use crate::ir::relation::{ColumnRole, TableKind};
use crate::ir::tree::traversal::{LevelNode, PostOrder, EXPR_CAPACITY, REL_CAPACITY};
use crate::ir::types::{ColumnDefType, DomainType};
use crate::ir::value::Value;
use crate::ir::{node::plugin, Plan};
use crate::warn;
use sql_type_system::error::Error as TypeSystemError;
use tarantool::decimal::Decimal;
use tarantool::space::SpaceEngineType;
use type_system::TypeAnalyzer;

use crate::ir::expression::ColumnPositionMap;

use crate::ir::node::AlterSystem;

use crate::ir::node::expression::Expression;

use crate::ir::node::plugin::AppendServiceToTier;
use crate::ir::node::plugin::ChangeConfig;
use crate::ir::node::plugin::CreatePlugin;
use crate::ir::node::plugin::DisablePlugin;
use crate::ir::node::plugin::DropPlugin;
use crate::ir::node::plugin::EnablePlugin;
use crate::ir::node::plugin::MigrateTo;
use crate::ir::node::plugin::MigrateToOpts;
use crate::ir::node::plugin::RemoveServiceFromTier;
use crate::ir::node::plugin::ServiceSettings;
use crate::ir::node::plugin::SettingsPair;

use crate::ir::acl::AuditPolicyOption;
use crate::ir::acl::GrantRevokeType;
use crate::ir::acl::Privilege;

use crate::ir::ddl::AlterSystemType;
use crate::ir::ddl::ColumnDef;
use crate::ir::ddl::Language;
use crate::ir::ddl::ParamDef;
use crate::ir::ddl::SetParamScopeType;
use crate::ir::ddl::SetParamValue;

use super::ir_populator::{
    can_assign, dql_return_columns, parse_param, parse_parameter_for_option, parse_scalar_expr,
    parse_unsigned, parse_values_rows, ExpressionWalker, OrderNulls,
};
use super::*;
use crate::ir::options::Timeout;
use ahash::AHashSet;

impl PartialEq for AstCore {
    fn eq(&self, other: &Self) -> bool {
        self.nodes == other.nodes && self.top == other.top
    }
}

#[derive(Clone, Debug)]
pub(in crate::frontend::sql) struct AstCore {
    pub(in crate::frontend::sql) nodes: ParseNodes,
    /// Index of top `ParseNode` in `nodes.arena`.
    pub(in crate::frontend::sql) top: Option<usize>,
}

impl AstCore {
    /// Set the top of AST.
    ///
    /// # Errors
    /// - The new top is not in the arena.
    pub fn set_top(&mut self, top: usize) -> Result<(), SbroadError> {
        self.nodes.get_node(top)?;
        self.top = Some(top);
        Ok(())
    }

    /// Get the top of AST.
    ///
    /// # Errors
    /// - AST tree doesn't have a top node.
    pub fn get_top(&self) -> Result<usize, SbroadError> {
        self.top.ok_or_else(|| {
            SbroadError::Invalid(Entity::AST, Some("no top node found in AST".into()))
        })
    }

    pub(in crate::frontend::sql) fn is_empty(&self) -> bool {
        self == &Self::empty()
    }

    /// Constructor.
    /// Builds a tree (nodes are in postorder reverse).
    /// Builds abstract syntax tree (AST) from SQL query.
    ///
    /// # Errors
    /// - Failed to parse an SQL query.
    pub(in crate::frontend::sql) fn fill<'q>(
        &mut self,
        query: &'q str,
        pairs_map: &mut ParsingPairsMap<'q>,
        pos_to_ast_id: &mut SelectChildPairTranslation,
        pair_to_ast_id: &mut PairToAstIdTranslation<'q>,
        tnt_parameters_ordered: &mut Vec<Pair<'q, Rule>>,
    ) -> Result<(), SbroadError> {
        let mut command_pair = match ParseTree::parse(Rule::Command, query) {
            Ok(p) => p,
            Err(e) => {
                return Err(SbroadError::ParsingError(
                    Entity::Rule,
                    format_smolstr!("{e}"),
                ))
            }
        };
        let top_pair = command_pair
            .next()
            .expect("Query expected as a first parsing tree child.");

        if let Rule::EmptyQuery = top_pair.as_rule() {
            *self = Self::empty();
            return Ok(());
        }

        let top = StackParseNode::new(top_pair, None);

        let mut has_tnt_params = false;
        let mut has_pg_params = false;
        let mut tnt_parameters = Vec::new();
        let mut stack: Vec<StackParseNode> = vec![top];
        while !stack.is_empty() {
            let stack_node: StackParseNode = match stack.pop() {
                Some(node) => node,
                None => break,
            };

            // Save node to AST.
            let arena_node_id = self.nodes.push_node(ParseNode::new(
                stack_node.pair.as_rule(),
                Some(SmolStr::from(stack_node.pair.as_str())),
            ));

            // Save procedure body (a special case).
            if stack_node.pair.as_rule() == Rule::ProcBody {
                let span = stack_node.pair.as_span();
                let body = &query[span.start()..span.end()];
                self.nodes
                    .update_value(arena_node_id, Some(SmolStr::from(body)))?;
            }

            // Update parent's node children list.
            self.nodes
                .add_child(stack_node.arena_parent_id, arena_node_id)?;

            // Clean parent values (only leafs and special nodes like
            // procedure body should contain data)
            if let Some(parent) = stack_node.arena_parent_id {
                let parent_node = self.nodes.get_node(parent)?;
                if parent_node.rule != Rule::ProcBody {
                    self.nodes.update_value(parent, None)?;
                }
            }

            match stack_node.pair.as_rule() {
                Rule::Expr
                | Rule::Row
                | Rule::Literal
                | Rule::SelectWithOptionalContinuation
                | Rule::Parameter => {
                    // * `Expr`s are parsed using Pratt parser with a separate `parse_expr`
                    //   function call on the stage of `build_ir`.
                    // * `Row`s are added to support parsing Row expressions under `Values` nodes.
                    // * `Literal`s are added to support procedure calls and
                    //   ALTER SYSTEM which should not contain all possible `Expr`s.
                    // * `SelectWithOptionalContinuation` is also parsed using Pratt parser
                    pairs_map.insert(arena_node_id, stack_node.pair.clone());
                }
                Rule::TntParameter => {
                    tnt_parameters.push(stack_node.pair.clone());
                    has_tnt_params = true;
                }
                Rule::PgParameter => {
                    has_pg_params = true;
                }
                Rule::Projection | Rule::OrderBy => {
                    pos_to_ast_id.insert(stack_node.pair.line_col(), arena_node_id);
                }
                Rule::SubQuery => {
                    pair_to_ast_id.insert(stack_node.pair.clone(), arena_node_id);
                }
                _ => {}
            }

            if has_tnt_params && has_pg_params {
                return Err(SbroadError::UseOfBothParamsStyles);
            }

            for parse_child in stack_node.pair.into_inner() {
                stack.push(StackParseNode::new(parse_child, Some(arena_node_id)));
            }
        }

        // Sort tnt parameters pairs in their appearance order.
        // This allows to enumerate tnt parameters: `select ?, ?` -> `select $1, $2`
        tnt_parameters.sort_by_key(|p| p.line_col());
        *tnt_parameters_ordered = tnt_parameters;

        self.set_top(0)?;

        self.transform_update()?;
        self.transform_delete()?;
        self.transform_select()?;
        Ok(())
    }

    /// Bring join AST to expected kind
    ///
    /// Inner join can be specified as `inner join` or `join` in user query,
    /// add `inner` to join if the second form was used
    pub(in crate::frontend::sql) fn normalize_join_ast(
        &mut self,
        join_id: usize,
    ) -> Result<(), SbroadError> {
        let node = self.nodes.get_node(join_id)?;
        if let Rule::Join = node.rule {
            if node.children.len() < 3 {
                let inner_node = ParseNode {
                    children: vec![],
                    rule: Rule::InnerJoinKind,
                    value: Some("inner".into()),
                };
                let inner_id = self.nodes.push_node(inner_node);
                let mut_node = self.nodes.get_mut_node(join_id)?;
                mut_node.children.insert(0, inner_id);
            }
        } else {
            return Err(SbroadError::Invalid(
                Entity::ParseNode,
                Some(format_smolstr!("expected join parse node, got: {node:?}")),
            ));
        }
        Ok(())
    }

    /// Rewrite `Update` AST to IR friendly one.
    ///
    /// `update t .. from s where expr` is transformed into
    /// ```text
    /// update t ..
    ///     join
    ///         scan t
    ///         s
    ///         condition expr
    /// ```
    ///
    /// `update t .. where expr` ->
    /// ```text
    /// update t ..
    ///     where expr
    ///         scan t
    /// ```
    ///
    /// `update t .. from s` ->
    /// ```text
    /// update t ..
    ///     join
    ///         scan t
    ///         s
    ///         condition true
    /// ```
    ///
    /// # Errors
    /// - invalid number of children for Update
    /// - unexpected rule of some child
    #[allow(clippy::too_many_lines)]
    pub(in crate::frontend::sql) fn transform_update(&mut self) -> Result<(), SbroadError> {
        let mut update_ids = Vec::new();
        for id in 0..self.nodes.arena.len() {
            let node = self.nodes.get_node(id)?;
            if node.rule == Rule::Update {
                update_ids.push(id)
            }
        }
        for update_id in update_ids {
            let node = self.nodes.get_node(update_id)?;
            let update_table_scan_id = *node
                .children
                .first()
                .expect("expected Update AST node to have at least two children");
            let update_list_id = *node
                .children
                .get(1)
                .expect("expected Update AST node to have at least two children");
            let table_scan = self.nodes.get_node(update_table_scan_id)?;
            let table_id = *table_scan.children.first().expect("must exist");
            let upd_table_scan = ParseNode {
                children: table_scan.children.clone(),
                rule: Rule::Scan,
                value: None,
            };
            let upd_table_scan_id = self.nodes.push_node(upd_table_scan);
            let node = self.nodes.get_node(update_id)?;
            match node.children.len() {
                // update t set ..
                2 => {
                    self.nodes.set_children(
                        update_id,
                        vec![upd_table_scan_id, table_id, update_list_id],
                    )?;
                }
                // update t set .. from .. OR update t set .. where ..
                3 => {
                    // update t set a = 1 where id = 1
                    let child_id = *node.children.get(2).unwrap();
                    let is_selection =
                        matches!(self.nodes.get_node(child_id)?.rule, Rule::Selection);
                    let update_child_id = if is_selection {
                        self.nodes.push_front_child(child_id, upd_table_scan_id)?;
                        child_id
                    } else {
                        // update t set a = t.a + t1.b from t1
                        let true_literal_node = ParseNode {
                            children: vec![],
                            rule: Rule::True,
                            value: Some("true".into()),
                        };
                        let true_literal_id = self.nodes.push_node(true_literal_node);
                        let expr_node = ParseNode {
                            children: vec![true_literal_id],
                            rule: Rule::Expr,
                            value: None,
                        };
                        let expr_id = self.nodes.push_node(expr_node);
                        let inner_kind_id = self.nodes.push_node(ParseNode {
                            children: vec![],
                            rule: Rule::InnerJoinKind,
                            value: None,
                        });
                        let join_node = ParseNode {
                            children: vec![upd_table_scan_id, inner_kind_id, child_id, expr_id],
                            rule: Rule::Join,
                            value: None,
                        };
                        self.nodes.push_node(join_node)
                    };
                    self.nodes
                        .set_children(update_id, vec![update_child_id, table_id, update_list_id])?;
                }
                4 => {
                    // update t set a = t.a + t1.b from t1 where expr
                    let expr_id = *node.children.get(3).unwrap();
                    let right_scan_id = *node.children.get(2).unwrap();
                    let inner_kind_id = self.nodes.push_node(ParseNode {
                        children: vec![],
                        rule: Rule::InnerJoinKind,
                        value: None,
                    });
                    let join_node = ParseNode {
                        children: vec![upd_table_scan_id, inner_kind_id, right_scan_id, expr_id],
                        rule: Rule::Join,
                        value: None,
                    };
                    let update_child_id = self.nodes.push_node(join_node);
                    self.nodes
                        .set_children(update_id, vec![update_child_id, table_id, update_list_id])?;
                }
                _ => {
                    return Err(SbroadError::UnexpectedNumberOfValues(
                        "expected Update ast node to have at most 4 children!".into(),
                    ))
                }
            }
        }
        Ok(())
    }

    fn transform_delete_children(
        &mut self,
        table_id: usize,
        filter_id: usize,
    ) -> Result<(), SbroadError> {
        let filter_node = self.nodes.get_node(filter_id)?;
        if filter_node.rule != Rule::DeleteFilter {
            return Err(SbroadError::Invalid(
                Entity::ParseNode,
                Some(format_smolstr!(
                    "expected delete filter as a second child, got: {filter_node:?}"
                )),
            ));
        }
        let mut new_filter_children = Vec::with_capacity(filter_node.children.len() + 1);
        new_filter_children.push(table_id);
        new_filter_children.extend(filter_node.children.iter().copied());
        self.nodes.set_children(filter_id, new_filter_children)?;

        Ok(())
    }

    /// Put delete table under delete filter to support reference resolution.
    pub(in crate::frontend::sql) fn transform_delete(&mut self) -> Result<(), SbroadError> {
        let arena_len = self.nodes.arena.len();
        for id in 0..arena_len {
            let node = self.nodes.get_node(id)?;
            if node.rule != Rule::Delete {
                continue;
            }

            let indexed_scan_id = node.child_n(0);
            let indexed_scan = self.nodes.get_node(indexed_scan_id)?;
            let table_id = indexed_scan.child_n(0);
            let indexed_by_id = indexed_scan.children.get(1);

            let delete_children = if let Some(filter_id) = node.children.get(1) {
                let filter_id = *filter_id;
                let mut children = vec![filter_id];
                if let Some(indexed_by_id) = indexed_by_id {
                    children.push(*indexed_by_id);
                }
                self.transform_delete_children(table_id, filter_id)?;

                children
            } else {
                let mut children = vec![table_id];
                if let Some(indexed_by_id) = indexed_by_id {
                    children.push(*indexed_by_id);
                }

                children
            };

            self.nodes.set_children(id, delete_children)?;
        }
        Ok(())
    }

    /// Transform select AST to IR friendly one. At the end of transformation
    /// all `Select` nodes are replaced with their first children (always `Projection`).
    /// - When some node contains `Select` as a child, that child is replaced with
    ///   `Projection` (`Select`'s first child).
    /// - When `Select` is a top node, its `Projection` (first child) becomes a new top.
    pub(in crate::frontend::sql) fn transform_select(&mut self) -> Result<(), SbroadError> {
        let mut selects: HashSet<usize> = HashSet::new();
        for id in 0..self.nodes.arena.len() {
            let node = self.nodes.get_node(id)?;
            match node.rule {
                Rule::Select => {
                    selects.insert(id);
                }
                Rule::Join => {
                    self.normalize_join_ast(id)?;
                }
                _ => {}
            }
        }
        for node in &selects {
            let select = self.nodes.get_node(*node)?;
            let children: Vec<usize> = select.children.clone();
            self.reorder_select_children(*node, &children)?;
        }

        // Collect select nodes' parents.
        let mut parents: Vec<(usize, usize)> = Vec::new();
        for (id, node) in self.nodes.arena.iter().enumerate() {
            for (children_pos, child_id) in node.children.iter().enumerate() {
                if selects.contains(child_id) {
                    parents.push((id, children_pos));
                }
            }
        }
        // Remove select nodes that have parents.
        for (parent_id, children_pos) in parents {
            let parent = self.nodes.get_node(parent_id)?;
            let child_id = *parent.children.get(children_pos).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Node,
                    format_smolstr!("at expected position {children_pos}"),
                )
            })?;
            let child = self.nodes.get_node(child_id)?;
            let mut node_id = *child.children.first().ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues("Selection node has no children.".into())
            })?;
            let parent = self.nodes.get_mut_node(parent_id)?;
            swap(&mut parent.children[children_pos], &mut node_id);
        }
        // Remove select if it is a top node.
        let top_id = self.get_top()?;
        if selects.contains(&top_id) {
            let top = self.nodes.get_node(top_id)?;
            let child_id = *top.children.first().ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(
                    "Selection node doesn't contain any children.".into(),
                )
            })?;
            self.set_top(child_id)?;
        }
        Ok(())
    }

    fn reorder_select_children(
        &mut self,
        select_id: usize,
        children: &[usize],
    ) -> Result<(), SbroadError> {
        // SQL grammar produces a defined order of children in select node,
        // for a projection with scan:
        // 1. Projection: required
        // 2. Scan: required (bind with Projection)
        // 3. Join: optional (can be repeated multiple times)
        // 4. Selection: optional
        // 5. GroupBy: optional
        // 6. Having: optional
        // 7. NamedWindows: optional
        //
        // But for projection without scan (like `select 1`)
        // Scan is optional, and all other nodes are not required.
        //
        // We need to reorder this sequence to the following:
        // 1. Projection: required
        // 2. NamedWindows: optional
        // 3. Having: optional
        // 4. GroupBy: optional
        // 5. Selection: optional
        // 6. Join: optional (can be repeated multiple times)
        // 7. Scan: required
        let mut proj_id: Option<usize> = None;
        let mut scan_id: Option<usize> = None;
        let mut join_ids = if children.len() > 2 {
            Vec::with_capacity(children.len() - 2)
        } else {
            Vec::new()
        };
        let mut filter_id: Option<usize> = None;
        let mut group_id: Option<usize> = None;
        let mut having_id: Option<usize> = None;
        let mut named_windows_id: Option<usize> = None;

        for child_id in children {
            let child = self.nodes.get_node(*child_id)?;
            match child.rule {
                Rule::Projection => proj_id = Some(*child_id),
                Rule::NamedWindows => {
                    named_windows_id = Some(*child_id);
                }
                Rule::Scan => scan_id = Some(*child_id),
                Rule::Join => join_ids.push(*child_id),
                Rule::Selection => filter_id = Some(*child_id),
                Rule::GroupBy => group_id = Some(*child_id),
                Rule::Having => having_id = Some(*child_id),
                _ => panic!("{} {:?}", "Unexpected rule in select children:", child.rule),
            }
        }

        // Projection and Scan are required. If they are not present, there is an error
        // in the SQL grammar.
        let proj_id = proj_id.expect("Projection node is required in select node");

        // The order of the nodes in the chain is partially reversed.
        // Original nodes from grammar:
        // Projection -> Scan -> Join1 -> ... -> JoinK -> Selection -> GroupBy -> Having -> NamedWindows.
        // We need to change the order of the chain to:
        // Projection -> NamedWindows -> Having -> GroupBy -> Selection -> JoinK -> ... -> Join1
        let mut chain = Vec::with_capacity(children.len() - 1);

        chain.push(proj_id);
        if let Some(named_windows_id) = named_windows_id {
            chain.push(named_windows_id);
        }
        if let Some(having_id) = having_id {
            chain.push(having_id);
        }
        if let Some(group_id) = group_id {
            chain.push(group_id);
        }
        if let Some(filter_id) = filter_id {
            chain.push(filter_id);
        }
        while let Some(join_id) = join_ids.pop() {
            chain.push(join_id);
        }

        let Some(mut child_id) = scan_id else {
            self.nodes.set_children(select_id, vec![proj_id])?;
            return Ok(());
        };

        while let Some(id) = chain.pop() {
            self.nodes.push_front_child(id, child_id)?;
            child_id = id;
        }

        self.nodes.set_children(select_id, vec![child_id])?;

        Ok(())
    }

    pub(in crate::frontend::sql) fn empty() -> Self {
        Self {
            nodes: ParseNodes::new(),
            top: None,
        }
    }

    pub(in crate::frontend::sql) fn parse_projection<M: Metadata>(
        &self,
        plan: &mut Plan,
        node_id: usize,
        type_analyzer: &mut TypeAnalyzer,
        map: &mut Translation,
        pairs_map: &mut ParsingPairsMap,
        worker: &mut ExpressionWalker<M>,
    ) -> Result<(), SbroadError> {
        let node = self.nodes.get_node(node_id)?;

        let (mut rel_child_id, mut other_children_ids) = {
            let Some((first, rest)) = node.children.split_first() else {
                return Err(SbroadError::Invalid(
                    Entity::Query,
                    Some("Projection must have some children".into()),
                ));
            };
            (first, rest.iter().as_slice())
        };

        let rel_child_node = self.nodes.get_node(*rel_child_id)?;
        if matches!(rel_child_node.rule, Rule::NamedWindows) {
            worker.curr_named_windows = worker.named_windows_stack.pop().unwrap_or_default();
            rel_child_id = rel_child_node
                .children
                .first()
                .expect("NamedWindows must have at least one child");
        }

        let mut is_distinct: bool = false;
        let Some(first_col_ast_id) = other_children_ids.first() else {
            return Err(SbroadError::Invalid(
                Entity::Query,
                Some("Projection must have at least a single column".into()),
            ));
        };
        if let Rule::Distinct = self.nodes.get_node(*first_col_ast_id)?.rule {
            is_distinct = true;
            let Some((_, ids)) = other_children_ids.split_first() else {
                return Err(SbroadError::Invalid(
                    Entity::Query,
                    Some("Projection with distinct must have at least a single column".into()),
                ));
            };
            other_children_ids = ids;
        }

        let plan_rel_child_id = map.get(*rel_child_id)?;
        let mut proj_columns: Vec<NodeId> = Vec::with_capacity(other_children_ids.len());

        let mut unnamed_col_pos = 0;
        // Unique identifier for each "*" met under projection. Uniqueness is local
        // for each projection. Used to distinguish the source of asterisk projections
        // like `select *, * from t`, where there are several of them.
        let mut asterisk_id = 0;

        for ast_column_id in other_children_ids {
            let ast_column = self.nodes.get_node(*ast_column_id)?;

            match ast_column.rule {
                Rule::Column => {
                    let expr_ast_id = ast_column
                        .children
                        .first()
                        .expect("Column has no children.");
                    let expr_pair = pairs_map.remove_pair(*expr_ast_id);
                    let expr_plan_node_id = parse_scalar_expr(
                        Pairs::single(expr_pair),
                        type_analyzer,
                        DerivedType::unknown(),
                        &[plan_rel_child_id],
                        worker,
                        plan,
                        true,
                    )?;

                    let alias_name: SmolStr =
                        if let Some(alias_ast_node_id) = ast_column.children.get(1) {
                            parse_normalized_identifier(self, *alias_ast_node_id)?
                        } else {
                            // We don't use `get_expression_node` here, because we may encounter a `Parameter`.
                            if let Node::Expression(Expression::Reference(_)) =
                                plan.get_node(expr_plan_node_id)?
                            {
                                let col_name = worker
                                    .reference_to_name_map
                                    .get(&expr_plan_node_id)
                                    .expect("reference must be in a map");
                                col_name.clone()
                            } else {
                                unnamed_col_pos += 1;
                                get_unnamed_column_alias(unnamed_col_pos)
                            }
                        };

                    let plan_alias_id = plan.nodes.add_alias(&alias_name, expr_plan_node_id)?;
                    proj_columns.push(plan_alias_id);
                }
                Rule::Asterisk => {
                    let table_name_id = ast_column.children.first();
                    let plan_asterisk_id = if let Some(table_name_id) = table_name_id {
                        let table_name = parse_normalized_identifier(self, *table_name_id)?;

                        let col_name_pos_map = ColumnPositionMap::new(plan, plan_rel_child_id)?;
                        let filtered_col_ids = col_name_pos_map.get_by_scan_name(&table_name)?;
                        plan.add_row_by_indices(
                            plan_rel_child_id,
                            filtered_col_ids,
                            false,
                            Some(ReferenceAsteriskSource::new(Some(table_name), asterisk_id)),
                        )?
                    } else {
                        plan.add_row_for_output(
                            plan_rel_child_id,
                            &[],
                            false,
                            Some(ReferenceAsteriskSource::new(None, asterisk_id)),
                        )?
                    };

                    let row_list = plan.get_row_list(plan_asterisk_id)?;
                    for row_id in row_list {
                        proj_columns.push(*row_id);
                    }
                    asterisk_id += 1;
                }
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Type,
                        Some(format_smolstr!(
                            "expected a Column, Asterisk, WindowDef under Projection, got {:?}.",
                            ast_column.rule
                        )),
                    ));
                }
            }
        }

        let windows = worker.curr_windows.clone();
        let projection_id =
            plan.add_proj_internal(plan_rel_child_id, &proj_columns, is_distinct, windows)?;

        plan.fix_subquery_rows(worker, projection_id)?;
        map.add(node_id, projection_id);

        worker.curr_named_windows.clear();
        worker.curr_windows.clear();
        Ok(())
    }

    pub(in crate::frontend::sql) fn parse_named_windows<M: Metadata>(
        &self,
        plan: &mut Plan,
        node_id: usize,
        type_analyzer: &mut TypeAnalyzer,
        map: &mut Translation,
        pairs_map: &mut ParsingPairsMap,
        worker: &mut ExpressionWalker<M>,
    ) -> Result<(), SbroadError> {
        let node = self.nodes.get_node(node_id)?;
        let Some((child_id, window_def_ids)) = node.children.split_first() else {
            panic!("NamedWindow must have at least 1 child and 1 WindowDef");
        };
        let plan_rel_child_id = map.get(*child_id)?;
        worker
            .named_windows_stack
            .push(HashMap::with_capacity(window_def_ids.len()));
        for def_id in window_def_ids {
            worker.curr_window_sqs.clear();
            let def_node = self.nodes.get_node(*def_id)?;
            assert_eq!(def_node.rule, Rule::WindowDef);
            let window_name_id = def_node
                .children
                .first()
                .expect("Window name should be met under WindowDef");
            let window_name = parse_identifier(self, *window_name_id)?;
            let window_body_id = def_node
                .children
                .get(1)
                .expect("Window body should be met under WindowDef");
            worker.inside_window_body = true;
            let window_id = self.parse_named_window_body(
                window_name,
                plan_rel_child_id,
                type_analyzer,
                plan,
                *window_body_id,
                map,
                pairs_map,
                worker,
            )?;
            worker.inside_window_body = false;
            for sq_id in worker.curr_window_sqs.iter() {
                worker.named_windows_sqs.insert(*sq_id, window_id);
            }
        }
        Ok(())
    }

    pub(in crate::frontend::sql) fn parse_select_without_scan<M: Metadata>(
        &self,
        plan: &mut Plan,
        node_id: usize,
        type_analyzer: &mut TypeAnalyzer,
        map: &mut Translation,
        pairs_map: &mut ParsingPairsMap,
        worker: &mut ExpressionWalker<M>,
    ) -> Result<(), SbroadError> {
        let node = self.nodes.get_node(node_id)?;
        let ast_columns_ids = &node.children;
        let mut proj_columns: Vec<NodeId> = Vec::with_capacity(ast_columns_ids.len());

        let mut unnamed_col_pos = 0;
        for ast_column_id in ast_columns_ids {
            let ast_column = self.nodes.get_node(*ast_column_id)?;
            match ast_column.rule {
                Rule::Column => {
                    let expr_ast_id = ast_column
                        .children
                        .first()
                        .expect("Column has no children.");
                    let expr_pair = pairs_map.remove_pair(*expr_ast_id);
                    let expr_plan_node_id = parse_scalar_expr(
                        Pairs::single(expr_pair),
                        type_analyzer,
                        DerivedType::unknown(),
                        &[],
                        worker,
                        plan,
                        true,
                    )?;
                    let alias_name = if let Some(alias_ast_node_id) = ast_column.children.get(1) {
                        parse_normalized_identifier(self, *alias_ast_node_id)?
                    } else {
                        // We don't use `get_expression_node` here, because we may encounter a `Parameter`.
                        if let Node::Expression(Expression::Reference(_)) =
                            plan.get_node(expr_plan_node_id)?
                        {
                            let col_name = worker
                                .reference_to_name_map
                                .get(&expr_plan_node_id)
                                .expect("reference must be in a map");
                            col_name.clone()
                        } else {
                            unnamed_col_pos += 1;
                            get_unnamed_column_alias(unnamed_col_pos)
                        }
                    };

                    let plan_alias_id = plan.nodes.add_alias(&alias_name, expr_plan_node_id)?;
                    proj_columns.push(plan_alias_id);
                }
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Type,
                        Some(format_smolstr!(
                            "expected a Column in SelectWithoutScan, got {:?}.",
                            ast_column.rule
                        )),
                    ));
                }
            }
        }

        let projection_id = plan.add_select_without_scan(&proj_columns)?;
        plan.fix_subquery_rows(worker, projection_id)?;
        map.add(node_id, projection_id);
        Ok(())
    }

    fn parse_order_by_elements<M: Metadata>(
        &self,
        plan: &mut Plan,
        referred_rel_id: NodeId,
        node_ids: &Vec<usize>,
        type_analyzer: &mut TypeAnalyzer,
        pairs_map: &mut ParsingPairsMap,
        worker: &mut ExpressionWalker<M>,
    ) -> Result<Vec<OrderByElement>, SbroadError> {
        let rel_node = plan.get_relation_node(referred_rel_id)?;
        let output_id = rel_node.output();

        let mut order_by_elements: Vec<OrderByElement> = Vec::new();
        for node_child_index in node_ids {
            let order_by_element_node = self.nodes.get_node(*node_child_index)?;
            let order_by_element_expr_id = order_by_element_node
                .children
                .first()
                .expect("OrderByElement must have at least one child");
            let expr_pair = pairs_map.remove_pair(*order_by_element_expr_id);
            let expr_plan_node_id = parse_scalar_expr(
                Pairs::single(expr_pair),
                type_analyzer,
                DerivedType::unknown(),
                &[referred_rel_id],
                worker,
                plan,
                false,
            )?;

            // In case index is specified as ordering element, we have to check that
            // the index (starting from 1) is not bigger than the number of columns in
            // the projection output.
            let expr = plan.get_node(expr_plan_node_id)?;

            let entity = match expr {
                Node::Expression(Expression::Parameter(..)) => return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some(SmolStr::from("Using parameter as a standalone ORDER BY expression doesn't influence sorting."))
                )),
                Node::Expression(expr) => {
                    if let Expression::Reference(Reference {col_type, ..}) = expr {
                        if matches!(col_type.get(), Some(UnrestrictedType::Array(_))) {
                            return Err(SbroadError::Invalid(
                                Entity::Expression,
                                Some(format_smolstr!("Array is not supported as a sort type for ORDER BY"))
                            ));
                        }
                    }

                    if let Expression::Constant(Constant {value: Value::Integer(index)}) = expr {
                        let index_usize = usize::try_from(*index).map_err(|_| {
                            SbroadError::Invalid(
                                Entity::Expression,
                                Some(format_smolstr!(
                            "Unable to convert OrderBy index ({index}) to usize"
                        )),
                            )
                        })?;

                        let output = plan.get_row_list(output_id)?;
                        let output_len = output.len();
                        let output_idx = index_usize.checked_sub(1).ok_or(SbroadError::Invalid(
                                Entity::Expression,
                                Some(format_smolstr!("ORDER BY position 0 is not in select list"))
                            ))?;
                        if let Some(alias_node_id) = output.get(output_idx) {
                            let alias_node = plan.get_expression_node(*alias_node_id)?;
                            if let Expression::Alias(Alias { child, .. }) = alias_node {
                                if let Expression::Reference(Reference { col_type, .. }) = plan.get_expression_node(*child)? {
                                    if matches!(col_type.get(), Some(UnrestrictedType::Array(_))) {
                                        return Err(SbroadError::Invalid(
                                            Entity::Expression,
                                            Some(format_smolstr!("Array is not supported as a sort type for ORDER BY"))
                                        ));
                                    }
                                }
                            }
                        } else {
                            return Err(SbroadError::Invalid(
                                Entity::Expression,
                                Some(format_smolstr!("Ordering index ({index}) is bigger than child projection output length ({output_len})."))
                            ));
                        }
                        OrderByEntity::Index { value: index_usize }
                    } else {
                        // Check that at least one reference is met in expression tree.
                        // Otherwise, ordering expression has no sense.
                        let expr_tree =
                            PostOrder::new(|node| plan.nodes.expr_iter(node, false), EXPR_CAPACITY);
                        let mut reference_met = false;
                        for LevelNode(_, node_id) in expr_tree.traverse_into_iter(expr_plan_node_id) {
							let node = plan.get_expression_node(node_id)?;
							match node {
								Expression::Reference(Reference { target, .. }) => {
									if !target.is_leaf() {
										reference_met = true;
										break;
									}
								},
								Expression::SubQueryReference(SubQueryReference { .. }) => {
									reference_met = true;
									break;
								},
								_ => {}
							}
                        }

                        if !reference_met {
                            return Err(SbroadError::Invalid(
                                Entity::Expression,
                                Some(SmolStr::from("ORDER BY element that is not position and doesn't contain reference doesn't influence ordering."))
                            ))
                        }

                        OrderByEntity::Expression {
                            expr_id: expr_plan_node_id,
                        }
                    }
                }
                _ => unreachable!("Unacceptable node as an ORDER BY element.")
            };

            let mut order_type = None;
            let mut order_nulls = None;

            for child_id in order_by_element_node.children.iter().skip(1) {
                let node = self.nodes.get_node(*child_id)?;
                match node.rule {
                    Rule::Asc => order_type = Some(OrderByType::Asc),
                    Rule::Desc => order_type = Some(OrderByType::Desc),
                    Rule::NullsFirst => order_nulls = Some(OrderNulls::First),
                    Rule::NullsLast => order_nulls = Some(OrderNulls::Last),
                    rule => unreachable!(
                        "{}",
                        format!("Unexpected rule met under OrderByElement: {rule:?}")
                    ),
                }
            }

            if let Some(order_nulls) = order_nulls {
                let entity_expr_id =
                    match entity {
                        OrderByEntity::Expression { expr_id } => expr_id,
                        OrderByEntity::Index { .. } => return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some(SmolStr::from(
                                "Nulls order is not supported for index expressions under ORDER BY",
                            )),
                        )),
                    };

                let is_null_expr_id = plan.add_unary(Unary::IsNull, entity_expr_id)?;
                let top_expr_id = match order_nulls {
                    OrderNulls::Last => is_null_expr_id,
                    OrderNulls::First => plan.add_unary(Unary::Not, is_null_expr_id)?,
                };
                let new_entity_first = OrderByEntity::Expression {
                    expr_id: top_expr_id,
                };
                order_by_elements.push(OrderByElement {
                    entity: new_entity_first,
                    order_type: None,
                });
                order_by_elements.push(OrderByElement { entity, order_type });
                continue;
            }

            order_by_elements.push(OrderByElement { entity, order_type });
        }
        Ok(order_by_elements)
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_order_by<M: Metadata>(
        &self,
        plan: &mut Plan,
        child_rel_id: Option<NodeId>,
        node_id: usize,
        type_analyzer: &mut TypeAnalyzer,
        map: &mut Translation,
        pairs_map: &mut ParsingPairsMap,
        worker: &mut ExpressionWalker<M>,
    ) -> Result<NodeId, SbroadError> {
        let node = self.nodes.get_node(node_id)?;
        let child_rel_id = child_rel_id.expect("Rel id must not be empty!");

        let sq_plan_id = plan.add_sub_query(child_rel_id, None)?;

        let order_by_elements_ids: Vec<usize> = node.children.clone().into_iter().collect();
        let order_by_elements = self.parse_order_by_elements(
            plan,
            child_rel_id,
            &order_by_elements_ids,
            type_analyzer,
            pairs_map,
            worker,
        )?;

        for order_by_element in order_by_elements.as_slice() {
            if let OrderByElement {
                entity: OrderByEntity::Expression { expr_id },
                ..
            } = order_by_element
            {
                plan.replace_target_in_subtree(*expr_id, child_rel_id, sq_plan_id)?
            }
        }

        let (order_by_id, plan_node_id) = plan.add_order_by(sq_plan_id, order_by_elements)?;
        plan.fix_subquery_rows(worker, order_by_id)?;
        map.add(node_id, plan_node_id);
        Ok(plan_node_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_named_window_body<M: Metadata>(
        &self,
        window_name: SmolStr,
        proj_child_id: NodeId,
        type_analyzer: &mut TypeAnalyzer,
        plan: &mut Plan,
        node_id: usize,
        map: &mut Translation,
        pairs_map: &mut ParsingPairsMap,
        worker: &mut ExpressionWalker<M>,
    ) -> Result<NodeId, SbroadError> {
        let node = self.nodes.get_node(node_id)?;

        let mut partition: Option<Vec<NodeId>> = None;
        let mut ordering = None;
        let mut frame = None;

        let err = Err(SbroadError::Invalid(
            Entity::Expression,
            Some(SmolStr::from("Invalid WINDOW body definition. Expected view of [BASE_WINDOW] [PARTITION BY] [ORDER BY] [FRAME]"))
        ));

        for node_child_index in &node.children {
            let body_info_node = self.nodes.get_node(*node_child_index)?;
            match body_info_node.rule {
                Rule::WindowPartition => {
                    if partition.is_some() || ordering.is_some() || frame.is_some() {
                        return err;
                    }

                    for part_expr_node_id in &body_info_node.children {
                        let part_expr_pair = pairs_map.remove_pair(*part_expr_node_id);
                        let part_expr_plan_node_id = parse_scalar_expr(
                            Pairs::single(part_expr_pair),
                            type_analyzer,
                            DerivedType::unknown(),
                            &[proj_child_id],
                            worker,
                            plan,
                            false,
                        )?;
                        if let Some(ref mut partition) = partition {
                            partition.push(part_expr_plan_node_id)
                        } else {
                            partition = Some(vec![part_expr_plan_node_id])
                        }
                    }
                }
                Rule::WindowOrderBy => {
                    if ordering.is_some() || frame.is_some() {
                        return err;
                    }
                    let order_by_elements = self.parse_order_by_elements(
                        plan,
                        proj_child_id,
                        &body_info_node.children,
                        type_analyzer,
                        pairs_map,
                        worker,
                    )?;
                    ordering = Some(order_by_elements)
                }
                Rule::WindowFrame => {
                    if frame.is_some() {
                        return err;
                    }
                    let frame_type_node_id = body_info_node
                        .children
                        .first()
                        .expect("WindowFrame should have WindowFrameType child");
                    let frame_type_node = self.nodes.get_node(*frame_type_node_id)?;
                    let ty = match frame_type_node.rule {
                        Rule::WindowFrameTypeRange => FrameType::Range,
                        Rule::WindowFrameTypeRows => FrameType::Rows,
                        _ => panic!(
                            "Expected FrameTypeRange or FrameTypeRows, got: {frame_type_node:?}"
                        ),
                    };

                    let bound_node_id = body_info_node
                        .children
                        .get(1)
                        .expect("WindowFrame should have WindowFrameBound children");
                    let bound_node = self.nodes.get_node(*bound_node_id)?;

                    let mut parse_frame_bound = |node_id: usize| -> Result<BoundType, SbroadError> {
                        let frame_bound_node = self.nodes.get_node(node_id)?;
                        let bound = match frame_bound_node.rule {
                            Rule::PrecedingUnbounded => BoundType::PrecedingUnbounded,
                            Rule::CurrentRow => BoundType::CurrentRow,
                            Rule::FollowingUnbounded => BoundType::FollowingUnbounded,
                            Rule::PrecedingOffset | Rule::FollowingOffset => {
                                let offset_expr_node_id = frame_bound_node
                                    .children
                                    .first()
                                    .expect("Expr node expected under Offset window bound");
                                let offset_expr_pair = pairs_map.remove_pair(*offset_expr_node_id);
                                let offset_expr_plan_node_id = parse_scalar_expr(
                                    Pairs::single(offset_expr_pair),
                                    type_analyzer,
                                    DerivedType::new(UnrestrictedType::Integer),
                                    &[proj_child_id],
                                    worker,
                                    plan,
                                    true,
                                )?;

                                match frame_bound_node.rule {
                                    Rule::PrecedingOffset => {
                                        BoundType::PrecedingOffset(offset_expr_plan_node_id)
                                    }
                                    Rule::FollowingOffset => {
                                        BoundType::FollowingOffset(offset_expr_plan_node_id)
                                    }
                                    _ => unreachable!("Offset bound expected"),
                                }
                            }
                            _ => panic!(
                                "Unexpected rule met under WindowFrameBound: {frame_bound_node:?}"
                            ),
                        };
                        Ok(bound)
                    };

                    let bounds: Bound = match bound_node.rule {
                        Rule::WindowFrameSingle => {
                            let bound_id = bound_node.children.first().expect(
                                "WindowFrameSingle should contain WindowFrameBound as a child",
                            );
                            let bound: BoundType = parse_frame_bound(*bound_id)?;
                            Bound::Single(bound)
                        }
                        Rule::WindowFrameBetween => {
                            let bound_first_id = bound_node.children.first().expect(
                                "WindowFrameBetween should contain an opening bound as a child",
                            );
                            let bound_first = parse_frame_bound(*bound_first_id)?;

                            let bound_second_id = bound_node.children.get(1).expect(
                                "WindowFrameBetween should contain a closing bound as a child",
                            );
                            let bound_second = parse_frame_bound(*bound_second_id)?;

                            Bound::Between(bound_first, bound_second)
                        }
                        _ => panic!("Unexpected rule met under WindowFrame: {bound_node:?}"),
                    };

                    frame = Some(Frame { ty, bound: bounds })
                }
                _ => panic!("Unexpected rule met under WindowBody: {body_info_node:?}"),
            }
        }

        let window: Window = Window {
            partition,
            ordering,
            frame,
        };

        let plan_window_by_id = plan.nodes.push(window.into());
        map.add(node_id, plan_window_by_id);
        worker
            .named_windows_stack
            .last_mut()
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Node,
                    Some(format_smolstr!(
                        "Window body expected to be inside NamedWindows",
                    )),
                )
            })?
            .insert(window_name, plan_window_by_id);
        Ok(plan_window_by_id)
    }
}

pub(in crate::frontend::sql) fn parse_create_plugin(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<CreatePlugin, SbroadError> {
    let mut if_not_exists = false;
    let first_node_idx = node.first_child();
    let plugin_name_child_idx = if let Rule::IfNotExists = ast.nodes.get_node(first_node_idx)?.rule
    {
        if_not_exists = true;
        1
    } else {
        0
    };

    let plugin_name_idx = node.child_n(plugin_name_child_idx);
    let name = parse_identifier(ast, plugin_name_idx)?;

    let version_idx = node.child_n(plugin_name_child_idx + 1);
    let version = parse_identifier(ast, version_idx)?;

    let mut timeout = plugin::get_default_timeout();
    if let Some(timeout_child_id) = node.children.get(plugin_name_child_idx + 2) {
        timeout = get_timeout(ast, *timeout_child_id)?;
    }

    Ok(CreatePlugin {
        name,
        version,
        if_not_exists,
        timeout,
    })
}

pub(in crate::frontend::sql) fn parse_drop_plugin(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<DropPlugin, SbroadError> {
    let mut if_exists = false;
    let mut with_data = false;
    let mut name = None;
    let mut version = None;
    let mut timeout = plugin::get_default_timeout();
    for child_id in &node.children {
        let child = ast.nodes.get_node(*child_id)?;
        match child.rule {
            Rule::IfExists => if_exists = true,
            Rule::WithData => with_data = true,
            Rule::Identifier => {
                if name.is_none() {
                    name = Some(parse_identifier(ast, *child_id)?);
                    continue;
                }
                return Err(SbroadError::Invalid(
                    Entity::AST,
                    Some("Plugin name is already set.".into()),
                ));
            }
            Rule::PluginVersion => {
                if version.is_none() {
                    version = Some(parse_identifier(ast, *child_id)?);
                    continue;
                }
                return Err(SbroadError::Invalid(
                    Entity::AST,
                    Some("Plugin version is already set.".into()),
                ));
            }
            Rule::Timeout => timeout = get_timeout(ast, *child_id)?,
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::AST,
                    Some(format_smolstr!(
                        "Unexpected AST node in DROP PLUGIN: {:?}",
                        child.rule
                    )),
                ))
            }
        }
    }

    Ok(DropPlugin {
        name: name.ok_or_else(|| {
            SbroadError::Invalid(Entity::AST, Some("Plugin name is not set.".into()))
        })?,
        version: version.ok_or_else(|| {
            SbroadError::Invalid(Entity::AST, Some("Plugin version is not set.".into()))
        })?,
        if_exists,
        with_data,
        timeout,
    })
}

pub(in crate::frontend::sql) fn parse_alter_plugin(
    ast: &AstCore,
    plan: &mut Plan,
    node: &ParseNode,
) -> Result<Option<NodeId>, SbroadError> {
    let plugin_name_idx = node.first_child();
    let plugin_name = parse_identifier(ast, plugin_name_idx)?;

    let idx = node.child_n(1);
    let node = ast.nodes.get_node(idx)?;
    let plugin_expr = match node.rule {
        Rule::EnablePlugin => {
            let version_idx = node.first_child();
            let version = parse_identifier(ast, version_idx)?;

            let mut timeout = plugin::get_default_timeout();
            if let Some(timeout_child_id) = node.children.get(1) {
                timeout = get_timeout(ast, *timeout_child_id)?;
            }

            Some(
                plan.nodes.push(
                    EnablePlugin {
                        name: plugin_name,
                        version,
                        timeout,
                    }
                    .into(),
                ),
            )
        }
        Rule::DisablePlugin => {
            let version_idx = node.first_child();
            let version = parse_identifier(ast, version_idx)?;

            let mut timeout = plugin::get_default_timeout();
            if let Some(timeout_child_id) = node.children.get(1) {
                timeout = get_timeout(ast, *timeout_child_id)?;
            }

            Some(
                plan.nodes.push(
                    DisablePlugin {
                        name: plugin_name,
                        version,
                        timeout,
                    }
                    .into(),
                ),
            )
        }
        Rule::MigrateTo => {
            let version_idx = node.first_child();
            let version = parse_identifier(ast, version_idx)?;
            let opts = parse_plugin_opts::<MigrateToOpts>(ast, node, 1, |opts, node, idx| {
                match node.rule {
                    Rule::Timeout => {
                        opts.timeout = get_timeout(ast, idx)?;
                    }
                    Rule::RollbackTimeout => {
                        opts.rollback_timeout = get_timeout(ast, idx)?;
                    }
                    _ => {}
                };
                Ok(())
            })?;

            Some(
                plan.nodes.push(
                    MigrateTo {
                        name: plugin_name,
                        version,
                        opts,
                    }
                    .into(),
                ),
            )
        }
        Rule::AddServiceToTier => {
            let version_idx = node.first_child();
            let version = parse_identifier(ast, version_idx)?;
            let service_idx = node.child_n(1);
            let service_name = parse_identifier(ast, service_idx)?;
            let tier_idx = node.child_n(2);
            let tier = parse_identifier(ast, tier_idx)?;

            let mut timeout = plugin::get_default_timeout();
            if let Some(timeout_child_id) = node.children.get(3) {
                timeout = get_timeout(ast, *timeout_child_id)?;
            }

            Some(
                plan.nodes.push(
                    AppendServiceToTier {
                        plugin_name,
                        version,
                        service_name,
                        tier,
                        timeout,
                    }
                    .into(),
                ),
            )
        }
        Rule::RemoveServiceFromTier => {
            let version_idx = node.first_child();
            let version = parse_identifier(ast, version_idx)?;
            let service_idx = node.child_n(1);
            let service_name = parse_identifier(ast, service_idx)?;
            let tier_idx = node.child_n(2);
            let tier = parse_identifier(ast, tier_idx)?;

            let mut timeout = plugin::get_default_timeout();
            if let Some(timeout_child_id) = node.children.get(3) {
                timeout = get_timeout(ast, *timeout_child_id)?;
            }

            Some(
                plan.nodes.push(
                    RemoveServiceFromTier {
                        plugin_name,
                        version,
                        service_name,
                        tier,
                        timeout,
                    }
                    .into(),
                ),
            )
        }
        Rule::ChangeConfig => {
            let version_idx = node.first_child();
            let version = parse_identifier(ast, version_idx)?;

            let mut key_value_grouped: HashMap<SmolStr, Vec<SettingsPair>> = HashMap::new();
            let mut timeout = plugin::get_default_timeout();

            for &i in &node.children[1..] {
                let next_node = ast.nodes.get_node(i)?;
                if next_node.rule == Rule::ConfigKV {
                    let svc_idx = next_node.child_n(0);
                    let svc = parse_identifier(ast, svc_idx)?;
                    let key_idx = next_node.child_n(1);
                    let key = parse_identifier(ast, key_idx)?;
                    let value_idx = next_node.child_n(2);
                    let value = retrieve_string_literal(ast, value_idx)?;
                    let entry = key_value_grouped.entry(svc).or_default();
                    entry.push(SettingsPair { key, value });
                } else {
                    timeout = get_timeout(ast, i)?;
                    break;
                }
            }

            Some(
                plan.nodes.push(
                    ChangeConfig {
                        plugin_name,
                        version,
                        key_value_grouped: key_value_grouped
                            .into_iter()
                            .map(|(service, settings)| ServiceSettings {
                                name: service,
                                pairs: settings,
                            })
                            .collect::<Vec<_>>(),
                        timeout,
                    }
                    .into(),
                ),
            )
        }
        _ => None,
    };

    Ok(plugin_expr)
}

pub(in crate::frontend::sql) fn parse_anonymous_block<M: Metadata>(
    ast: &AstCore,
    node_id: usize,
    map: &Translation,
    plan: &Plan,
    worker: &ExpressionWalker<'_, M>,
) -> Result<AnonymousBlock, SbroadError> {
    fn ensure_can_generate_local_sql_for_dml(
        kind: &str,
        top_id: NodeId,
        plan: &Plan,
    ) -> Result<(), SbroadError> {
        let dfs = PostOrder::new(|x| plan.nodes.rel_iter(x), REL_CAPACITY);
        for LevelNode(_, id) in dfs.traverse_into_iter(top_id) {
            match plan.get_relation_node(id)? {
                Relational::ScanSubQuery(_) => {
                    return Err(SbroadError::Other(format_smolstr!(
                        "{kind} in transaction cannot have subqueries"
                    )))
                }
                Relational::Join(_) => {
                    return Err(SbroadError::Other(format_smolstr!(
                        "{kind} in transaction cannot have joins"
                    )))
                }
                _ => (),
            }
        }

        Ok(())
    }

    let node = ast.nodes.get_node(node_id)?;
    let mut statements = Vec::new();
    let mut return_columns: Vec<(String, DerivedType)> = Vec::new();
    for child_id in &node.children {
        let child = ast.nodes.get_node(*child_id)?;
        match child.rule {
            Rule::ExplicitBlockLanguage => (),
            Rule::AnonymousBlockOptions => (),
            Rule::BlockReturnQueryStatement => {
                let query_id = map.get(*child_id)?;
                let columns = dql_return_columns(plan, query_id)?;
                if !return_columns.is_empty() {
                    let return_types: Vec<_> = return_columns.iter().map(|c| c.1).collect();
                    let query_types: Vec<_> = columns.iter().map(|c| c.1).collect();
                    if return_types != query_types {
                        return Err(SbroadError::Other(format_smolstr!(
                            "RETURN QUERY types cannot be matched ([{}] and [{}])",
                            return_types.iter().join(", "),
                            query_types.iter().join(", "),
                        )));
                    }
                } else {
                    return_columns = columns;
                }
                statements.push(BlockStatement::ReturnQuery(query_id));
            }
            Rule::BlockQueryStatement => {
                let query_id = map.get(*child_id)?;
                if !plan.get_relation_node(query_id)?.is_dml() {
                    // So nobody will be confused that `DO $$ BEGIN SELECT 1; END $$`
                    // doesn't return rows.
                    return Err(SbroadError::other(
                        "QUERY statements must execute DML queries, use RETURN QUERY to return rows",
                    ));
                }
                statements.push(BlockStatement::Query(query_id));
            }
            Rule::BlockLetStatement => {
                // The master loop has already pushed the LET decl into
                // `worker.let_scope` and stashed the variable name in
                // `worker.let_var_names`; here we just rebuild the
                // BlockStatement::Let by combining the name with the RHS
                // plan id forwarded through `map`.
                let sub_query_id = map.get(*child_id)?;
                let query_id = plan
                    .get_rel_child(sub_query_id, 0)
                    .expect("subquery cannot miss a child");

                let var = worker.let_var_names.get(child_id).cloned().ok_or_else(|| {
                    SbroadError::other("LET variable name was not recorded during parsing")
                })?;
                statements.push(BlockStatement::Let {
                    var: format_smolstr!(":{var}"),
                    query: query_id,
                });
            }
            Rule::BlockIfStatement => {
                // Children: [BlockIfCondition, BlockIfBodyStatement+].
                let mut children = child.children.iter();
                let cond_ast_id = children.next().expect("IF must have a condition");
                let cond_id = map.get(*cond_ast_id)?;

                let cond_cols = dql_return_columns(plan, cond_id)?;
                if cond_cols.len() != 1 {
                    return Err(SbroadError::other(
                        "IF condition must produce a single column",
                    ));
                }

                let mut body_ids: Vec<NodeId> = Vec::new();
                for body_ast_id in children {
                    let body_ast = ast.nodes.get_node(*body_ast_id)?;
                    let inner_ast_id = body_ast
                        .children
                        .first()
                        .expect("BlockIfBodyStatement must have an inner statement");
                    match ast.nodes.get_node(*inner_ast_id)?.rule {
                        Rule::BlockQueryStatement => {
                            let query_id = map.get(*body_ast_id)?;
                            if !plan.get_relation_node(query_id)?.is_dml() {
                                return Err(SbroadError::other(
                                    "IF body may only contain DML statements",
                                ));
                            }
                            body_ids.push(query_id);
                        }
                        Rule::BlockLetStatement => {
                            return Err(SbroadError::other("LET is not allowed inside IF body"));
                        }
                        Rule::BlockReturnQueryStatement => {
                            return Err(SbroadError::other(
                                "RETURN QUERY is not allowed inside IF body",
                            ));
                        }
                        Rule::BlockIfStatement => {
                            return Err(SbroadError::other("nested IF is not allowed"));
                        }
                        rule => unreachable!("{rule:?} is not a block statement inside IF body"),
                    }
                }

                statements.push(BlockStatement::If {
                    cond: cond_id,
                    body: body_ids,
                });
            }
            rule => unreachable!("{rule:?} is unexpected according to the grammar"),
        }
    }

    // Validate OPTION usage.
    let mut options_count = None;
    for child_id in &node.children {
        let child = ast.nodes.get_node(*child_id)?;
        if let Rule::AnonymousBlockOptions = child.rule {
            let option_rule = child.children[0];
            let options = ast.nodes.get_node(option_rule)?;
            options_count = Some(options.children.len());
        }
    }

    let block_options_count = options_count.unwrap_or(0);
    // All options must be specified for this block.
    if block_options_count != plan.raw_options.len() {
        return Err(SbroadError::other(
            "OPTION cannot be specified for individual queries within a transaction; specify it for the entire DO block instead"
        ));
    }

    for opt in &plan.raw_options {
        if opt.kind == OptionKind::MotionRowMax {
            return Err(SbroadError::other(
                "transaction cannot have any motions; SQL_MOTION_ROW_MAX is not applicable to transactions",
            ));
        }
    }

    // Force users to write statements as they are executed (dql before dml).
    for (id1, id2) in node.children.windows(2).map(|pair| (pair[0], pair[1])) {
        let r1 = ast.nodes.get_node(id1)?.rule;
        let r2 = ast.nodes.get_node(id2)?.rule;
        if let (
            Rule::BlockQueryStatement | Rule::BlockIfStatement,
            Rule::BlockLetStatement | Rule::BlockReturnQueryStatement,
        ) = (r1, r2)
        {
            return Err(SbroadError::Other(format_smolstr!(
                "QUERY and IF statements must follow LET and RETURN QUERY statements",
            )));
        }
    }

    // Ensure tables belong to the same tier.
    let mut block_tier = None;
    for table in plan.relations.tables.values() {
        if let (Some(block_tier), Some(table_tier)) = (block_tier, &table.tier) {
            if block_tier != table_tier {
                // TODO: more context
                return Err(SbroadError::Other(format_smolstr!(
                    "all tables in a transaction must belong to the same tier"
                )));
            }
        }
        block_tier = block_tier.or(table.tier.as_ref());
    }

    // Ensure we don't write to global tables.
    for entry in BlockEntries::new(&statements) {
        let relation: &str = match plan.get_relation_node(*entry.query)? {
            Relational::Delete(d) => &d.relation,
            Relational::Insert(i) => &i.relation,
            Relational::Update(u) => &u.relation,
            _ => continue,
        };

        let relation = plan.relations.get(relation).expect("can't be missed");
        let cannot_modify_error = |kind, name| {
            SbroadError::Other(format_smolstr!(
                "cannot modify {kind} table {name} within transaction",
            ))
        };
        match relation.kind {
            TableKind::GlobalSpace => return Err(cannot_modify_error("global", &relation.name)),
            TableKind::SystemSpace => return Err(cannot_modify_error("system", &relation.name)),
            TableKind::ShardedSpace { .. } => (),
        }
    }

    // Ensure `generate_pattern_with_params_for_block` can handle queries.
    for entry in BlockEntries::new(&statements) {
        let node_id = *entry.query;
        match plan.get_relation_node(node_id)? {
            Relational::Update(_) => {
                ensure_can_generate_local_sql_for_dml("UPDATE", node_id, plan)?
            }
            Relational::Delete(_) => {
                ensure_can_generate_local_sql_for_dml("DELETE", node_id, plan)?
            }
            _ => {}
        }
    }

    let mut unused_lets = HashSet::new();
    for var_decl in &worker.let_scope.decls {
        if !var_decl.used {
            unused_lets.insert(var_decl.query_id);
        }
    }

    Ok(AnonymousBlock {
        statements,
        return_columns,
        unused_lets,
    })
}

/// Get String value under node that is considered to be an identifier
/// (on which rules on name normalization should be applied).
pub(in crate::frontend::sql) fn parse_identifier(
    ast: &AstCore,
    node_id: usize,
) -> Result<SmolStr, SbroadError> {
    Ok(normalize_name_from_sql(parse_string_value_node(
        ast, node_id,
    )?))
}

pub(in crate::frontend::sql) fn parse_optional_identifier(
    ast: &AstCore,
    node_id: usize,
) -> Result<Option<SmolStr>, SbroadError> {
    let string_value = parse_string_value(ast, node_id)?;

    Ok(string_value.map(|val| normalize_name_from_sql(val)))
}

pub(in crate::frontend::sql) fn parse_indexed_by_expr(
    ast: &AstCore,
    node_id: usize,
) -> Result<Option<SmolStr>, SbroadError> {
    let child_id = ast
        .nodes
        .get_node(node_id)?
        .children
        .first()
        .expect("Child must exists");

    parse_optional_identifier(ast, *child_id)
}

pub(in crate::frontend::sql) fn parse_normalized_identifier(
    ast: &AstCore,
    node_id: usize,
) -> Result<SmolStr, SbroadError> {
    Ok(normalize_name_from_sql(parse_string_value_node(
        ast, node_id,
    )?))
}

/// Common logic for parsing GRANT/REVOKE queries.
#[allow(clippy::too_many_lines)]
pub(in crate::frontend::sql) fn parse_grant_revoke(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<(GrantRevokeType, SmolStr, bool, Timeout), SbroadError> {
    let privilege_block_node_id = node
        .children
        .first()
        .expect("Specific privilege block expected under GRANT/REVOKE");
    let privilege_block_node = ast.nodes.get_node(*privilege_block_node_id)?;
    let grant_revoke_type = match privilege_block_node.rule {
        Rule::PrivBlockPrivilege => {
            let privilege_node_id = privilege_block_node
                .children
                .first()
                .expect("Expected to see Privilege under PrivBlockPrivilege");
            let privilege_node = ast.nodes.get_node(*privilege_node_id)?;
            let privilege = match privilege_node.rule {
                Rule::PrivilegeAlter => Privilege::Alter,
                Rule::PrivilegeCreate => Privilege::Create,
                Rule::PrivilegeDrop => Privilege::Drop,
                Rule::PrivilegeExecute => Privilege::Execute,
                Rule::PrivilegeRead => Privilege::Read,
                Rule::PrivilegeSession => Privilege::Session,
                Rule::PrivilegeUsage => Privilege::Usage,
                Rule::PrivilegeWrite => Privilege::Write,
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Privilege,
                        Some(format_smolstr!(
                            "Expected to see Privilege node. Got: {privilege_node:?}"
                        )),
                    ))
                }
            };
            let inner_privilege_block_node_id = privilege_block_node
                .children
                .get(1)
                .expect("Expected to see inner priv block under PrivBlockPrivilege");
            let inner_privilege_block_node = ast.nodes.get_node(*inner_privilege_block_node_id)?;
            match inner_privilege_block_node.rule {
                Rule::PrivBlockUser => GrantRevokeType::user(privilege)?,
                Rule::PrivBlockSpecificUser => {
                    let user_name_node_id = inner_privilege_block_node
                        .children
                        .first()
                        .expect("Expected to see RoleName under PrivBlockSpecificUser");
                    let user_name = parse_identifier(ast, *user_name_node_id)?;
                    GrantRevokeType::specific_user(privilege, user_name)?
                }
                Rule::PrivBlockRole => GrantRevokeType::role(privilege)?,
                Rule::PrivBlockSpecificRole => {
                    let role_name_node_id = inner_privilege_block_node
                        .children
                        .first()
                        .expect("Expected to see RoleName under PrivBlockSpecificUser");
                    let role_name = parse_identifier(ast, *role_name_node_id)?;
                    GrantRevokeType::specific_role(privilege, role_name)?
                }
                Rule::PrivBlockTable => GrantRevokeType::table(privilege)?,
                Rule::PrivBlockSpecificTable => {
                    let table_node_id = inner_privilege_block_node.children.first().expect(
                        "Expected to see TableName as a first child of PrivBlockSpecificTable",
                    );
                    let table_name = parse_identifier(ast, *table_node_id)?;
                    GrantRevokeType::specific_table(privilege, table_name)?
                }
                Rule::PrivBlockProcedure => GrantRevokeType::procedure(privilege)?,
                Rule::PrivBlockSpecificProcedure => {
                    let proc_node_id = inner_privilege_block_node.children.first().expect(
                        "Expected to see Name as a first child of PrivBlockSpecificProcedure",
                    );
                    let proc_node = ast.nodes.get_node(*proc_node_id)?;
                    let (proc_name, proc_params) = parse_proc_with_optional_params(ast, proc_node)?;
                    GrantRevokeType::specific_procedure(privilege, proc_name, proc_params)?
                }
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::ParseNode,
                        Some(format_smolstr!(
                            "Expected specific priv block, got: {inner_privilege_block_node:?}"
                        )),
                    ))
                }
            }
        }
        Rule::PrivBlockRolePass => {
            let role_name_id = privilege_block_node
                .children
                .first()
                .expect("RoleName must be a first child of PrivBlockRolePass");
            let role_name = parse_identifier(ast, *role_name_id)?;
            GrantRevokeType::role_pass(role_name)
        }
        _ => {
            return Err(SbroadError::Invalid(
                Entity::ParseNode,
                Some(format_smolstr!(
                    "Expected specific priv block, got: {privilege_block_node:?}"
                )),
            ))
        }
    };

    let grantee_name_node_id = node
        .children
        .get(1)
        .expect("RoleName must be a second child of GRANT/REVOKE");
    let grantee_name = parse_identifier(ast, *grantee_name_node_id)?;

    let mut timeout = get_default_timeout();
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;
    for child_id in node.children.iter().skip(2) {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::WaitAppliedGlobally => wait_applied_globally = true,
            Rule::WaitAppliedLocally => wait_applied_globally = false,
            Rule::Timeout => timeout = get_timeout(ast, *child_id)?,
            _ => {}
        }
    }

    Ok((
        grant_revoke_type,
        grantee_name,
        wait_applied_globally,
        timeout,
    ))
}

/// Common logic for [`crate::ir::options::OptionKind::VdbeOpcodeMax`]
/// and [`crate::ir::options::OptionKind::MotionRowMax`] parsing.
pub(in crate::frontend::sql) fn parse_option<M: Metadata>(
    ast: &AstCore,
    type_analyzer: &mut TypeAnalyzer,
    option_node_id: usize,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<OptionParamValue, SbroadError> {
    let ast_node = ast.nodes.get_node(option_node_id)?;
    let value = match ast_node.rule {
        Rule::Parameter => parse_parameter_for_option(
            type_analyzer,
            option_node_id,
            pairs_map,
            worker,
            plan,
            UnrestrictedType::Integer,
        )?,
        Rule::Unsigned => {
            let v = parse_unsigned(ast_node)?;
            OptionParamValue::Value {
                val: Value::Integer(v),
            }
        }
        _ => {
            return Err(SbroadError::Invalid(
                AST,
                Some(format_smolstr!(
                    "unexpected child of option. id: {option_node_id}"
                )),
            ))
        }
    };
    Ok(value)
}

pub(in crate::frontend::sql) fn parse_read_preference_option<M: Metadata>(
    ast: &AstCore,
    type_analyzer: &mut TypeAnalyzer,
    option_node_id: usize,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<OptionParamValue, SbroadError> {
    let ast_node = ast.nodes.get_node(option_node_id)?;
    let value = match ast_node.rule {
        Rule::Parameter => parse_parameter_for_option(
            type_analyzer,
            option_node_id,
            pairs_map,
            worker,
            plan,
            UnrestrictedType::String,
        )?,
        Rule::Leader | Rule::Replica | Rule::Any => {
            let value = match ast_node.rule {
                Rule::Leader => "leader",
                Rule::Replica => "replica",
                Rule::Any => "any",
                _ => unreachable!(),
            };
            OptionParamValue::Value {
                val: Value::String(value.into()),
            }
        }
        _ => {
            return Err(SbroadError::Invalid(
                AST,
                Some(format_smolstr!(
                    "unexpected child of read_preference option. id: {option_node_id}"
                )),
            ))
        }
    };

    Ok(value)
}

pub(in crate::frontend::sql) fn parse_forward_option<M: Metadata>(
    ast: &AstCore,
    type_analyzer: &mut TypeAnalyzer,
    option_node_id: usize,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<OptionParamValue, SbroadError> {
    let ast_node = ast.nodes.get_node(option_node_id)?;
    let value = match ast_node.rule {
        Rule::Parameter => parse_parameter_for_option(
            type_analyzer,
            option_node_id,
            pairs_map,
            worker,
            plan,
            UnrestrictedType::String,
        )?,
        Rule::ForwardOn | Rule::ForwardOff | Rule::ForwardROtoRW => {
            let value = match ast_node.rule {
                Rule::ForwardOn => "on",
                Rule::ForwardOff => "off",
                Rule::ForwardROtoRW => "ro_to_rw",
                _ => unreachable!(),
            };
            OptionParamValue::Value {
                val: Value::String(value.into()),
            }
        }
        _ => panic!("unexpected child of forward option. id: {option_node_id}"),
    };

    Ok(value)
}

pub(in crate::frontend::sql) fn parse_audit_policy(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<AuditPolicy, SbroadError> {
    let policy_name_node_id = node
        .children
        .first()
        .expect("PolicyName expected as first child");
    let policy_name = parse_identifier(ast, *policy_name_node_id)?;

    let option_node_id = node
        .children
        .get(1)
        .expect("AuditPolicyOption expected as second child");
    let option_node = ast.nodes.get_node(*option_node_id)?;
    let audit_option = match option_node.rule {
        Rule::AuditPolicyOptionOn => {
            let user_name_node_id = option_node
                .children
                .first()
                .expect("UserName expected as first child");
            let user_name = parse_identifier(ast, *user_name_node_id)?;
            AuditPolicyOption::On { user_name }
        }
        Rule::AuditPolicyOptionOff => {
            let user_name_node_id = option_node
                .children
                .first()
                .expect("UserName expected as first child");
            let user_name = parse_identifier(ast, *user_name_node_id)?;
            AuditPolicyOption::Off { user_name }
        }
        _ => {
            return Err(SbroadError::Invalid(
                Entity::ParseNode,
                Some(SmolStr::from(
                    "Expected to see concrete audit policy option",
                )),
            ))
        }
    };

    let mut timeout = get_default_timeout();
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;
    for child_id in node.children.iter().skip(2) {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::WaitAppliedGlobally => wait_applied_globally = true,
            Rule::WaitAppliedLocally => wait_applied_globally = false,
            Rule::Timeout => timeout = get_timeout(ast, *child_id)?,
            _ => {}
        }
    }

    Ok(AuditPolicy {
        policy_name,
        audit_option,
        wait_applied_globally,
        timeout,
    })
}

pub(in crate::frontend::sql) fn parse_plugin_opts<T: Default>(
    ast: &AstCore,
    node: &ParseNode,
    start_from: usize,
    mut with_opt_node: impl FnMut(&mut T, &ParseNode, usize) -> Result<(), SbroadError>,
) -> Result<T, SbroadError> {
    let mut opts = T::default();

    for &param_idx in &node.children[start_from..] {
        let param_node = ast.nodes.get_node(param_idx)?;
        with_opt_node(&mut opts, param_node, param_idx)?;
    }

    Ok(opts)
}

/// Parse a value from an option node's first child.
/// The node is expected to have a child node containing the value.
pub(in crate::frontend::sql) fn parse_option_value<T>(
    ast: &AstCore,
    node: &ParseNode,
    option_name: &str,
) -> Result<T, SbroadError>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    let child_id = node.children.first().ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Query,
            Some(format_smolstr!("missing child node for {option_name}")),
        )
    })?;
    let child = ast.nodes.get_node(*child_id)?;
    let value_str = child.value.as_ref().ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Value,
            Some(format_smolstr!("missing value for {option_name}")),
        )
    })?;
    value_str.parse().map_err(|e| {
        SbroadError::Invalid(
            Entity::Value,
            Some(format_smolstr!(
                "invalid value '{value_str}' for {option_name}: {e}"
            )),
        )
    })
}

#[allow(clippy::uninlined_format_args)]
pub(in crate::frontend::sql) fn get_timeout(
    ast: &AstCore,
    node_id: usize,
) -> Result<Timeout, SbroadError> {
    let param_node = ast.nodes.get_node(node_id)?;
    if let (Some(duration_id), None) = (param_node.children.first(), param_node.children.get(1)) {
        let duration_node = ast.nodes.get_node(*duration_id)?;
        if duration_node.rule != Rule::Duration {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!(
                    "AST table option duration node {:?} contains unexpected children",
                    duration_node,
                )),
            ));
        }
        if let Some(duration_value) = duration_node.value.as_ref() {
            let secs = Decimal::from_str(duration_value).map_err(|_| {
                SbroadError::Invalid(
                    Entity::Node,
                    Some(format_smolstr!(
                        "AST table duration node {:?} contains invalid value",
                        duration_node,
                    )),
                )
            })?;
            // Convert seconds (Decimal) to microseconds (u64).
            let secs_f64: f64 = secs.to_smolstr().parse().map_err(|_| {
                SbroadError::Invalid(
                    Entity::Node,
                    Some(format_smolstr!(
                        "timeout value too large or negative: {duration_value}"
                    )),
                )
            })?;
            let us = std::time::Duration::try_from_secs_f64(secs_f64)
                .map_err(|_| {
                    SbroadError::Invalid(
                        Entity::Node,
                        Some(format_smolstr!("invalid timeout value: {duration_value}")),
                    )
                })?
                .as_micros() as u64;
            return Ok(Timeout::explicit(us));
        }
        return Err(SbroadError::Invalid(
            Entity::AST,
            Some("Duration node has no value".into()),
        ));
    }
    Err(SbroadError::Invalid(
        Entity::AST,
        Some("expected Timeout node to have exactly one child".into()),
    ))
}

pub(in crate::frontend::sql) fn retrieve_string_literal(
    ast: &AstCore,
    node_id: usize,
) -> Result<SmolStr, SbroadError> {
    let node: &ParseNode = ast.nodes.get_node(node_id)?;
    let str_ref = node
        .value
        .as_ref()
        .expect("Rule node must contain string value.");
    assert!(
        node.rule == Rule::SingleQuotedString,
        "Expected SingleQuotedString, got: {:?}",
        node.rule
    );

    Ok(str_ref[1..str_ref.len() - 1].into())
}

/// Parse node from which we want to get String value.
pub(in crate::frontend::sql) fn parse_string_value(
    ast: &AstCore,
    node_id: usize,
) -> Result<Option<&SmolStr>, SbroadError> {
    let string_value_node = ast.nodes.get_node(node_id)?;
    let string_value = string_value_node.value.as_ref();

    Ok(string_value)
}

/// Parse node from which we want to get String value.
pub(in crate::frontend::sql) fn parse_string_value_node(
    ast: &AstCore,
    node_id: usize,
) -> Result<&str, SbroadError> {
    let string_value_node = ast.nodes.get_node(node_id)?;
    let string_value = string_value_node
        .value
        .as_ref()
        .expect("Rule node must contain string value.");
    Ok(string_value.as_str())
}

pub(in crate::frontend::sql) fn parse_proc_params(
    ast: &AstCore,
    params_node: &ParseNode,
) -> Result<Vec<ParamDef>, SbroadError> {
    let mut params = Vec::with_capacity(params_node.children.len());
    for param_id in &params_node.children {
        let column_def_type_node = ast.nodes.get_node(*param_id)?;
        let type_node_inner_id = column_def_type_node
            .children
            .first()
            .expect("Expected specific type under ColumnDefType node");
        let type_node = ast.nodes.get_node(*type_node_inner_id)?;
        // TODO: support proc(int array).
        debug_assert!(
            column_def_type_node.children.get(1).is_none(),
            "ProcParamsTypes is not expected to carry an ArraySuffix"
        );
        let data_type = UnrestrictedType::try_from(type_node.rule)?;
        params.push(ParamDef { data_type });
    }
    Ok(params)
}

pub(in crate::frontend::sql) fn parse_call_proc<M: Metadata>(
    ast: &AstCore,
    node: &ParseNode,
    type_analyzer: &mut TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<CallProcedure, SbroadError> {
    let proc_name_ast_id = node.children.first().expect("Expected to get Proc name");
    let name = parse_identifier(ast, *proc_name_ast_id)?;

    let proc_values_id = node.children.get(1).expect("Expected to get Proc values");
    let proc_values = ast.nodes.get_node(*proc_values_id)?;
    let mut values: Vec<NodeId> = Vec::with_capacity(proc_values.children.len());
    for proc_value_id in &proc_values.children {
        let proc_value = ast.nodes.get_node(*proc_value_id)?;
        let plan_value_id = match proc_value.rule {
            Rule::Parameter => {
                parse_param(pairs_map.remove_pair(*proc_value_id), &[], worker, plan)?
            }
            Rule::Literal => {
                let literal_pair = pairs_map.remove_pair(*proc_value_id);
                parse_scalar_expr(
                    Pairs::single(literal_pair),
                    type_analyzer,
                    DerivedType::unknown(),
                    &[],
                    worker,
                    plan,
                    true,
                )?
            }
            _ => unreachable!("Unexpected rule met under ProcValue"),
        };
        values.push(plan_value_id);
    }

    let call_proc = CallProcedure { name, values };
    Ok(call_proc)
}

pub(in crate::frontend::sql) fn parse_rename_proc(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<RenameRoutine, SbroadError> {
    if node.rule != Rule::RenameProc {
        return Err(SbroadError::Invalid(
            Entity::Type,
            Some("rename procedure".into()),
        ));
    }

    let mut old_name = SmolStr::default();
    let mut new_name = SmolStr::default();
    let mut params: Option<Vec<ParamDef>> = None;
    let mut timeout = get_default_timeout();
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::NewProc => {
                new_name = parse_identifier(ast, *child_id)?;
            }
            Rule::OldProc => {
                old_name = parse_identifier(ast, *child_id)?;
            }
            Rule::ProcParams => {
                params = Some(parse_proc_params(ast, child_node)?);
            }
            Rule::Timeout => {
                timeout = get_timeout(ast, *child_id)?;
            }
            Rule::WaitAppliedGlobally => wait_applied_globally = true,
            Rule::WaitAppliedLocally => wait_applied_globally = false,
            _ => panic!("Unexpected node: {child_node:?}"),
        }
    }
    Ok(RenameRoutine {
        old_name,
        new_name,
        params,
        timeout,
        wait_applied_globally,
    })
}

pub(in crate::frontend::sql) fn parse_create_proc(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<CreateProc, SbroadError> {
    let mut name = None;
    let mut params = None;
    let language = Language::SQL;
    let mut body = SmolStr::default();
    let mut if_not_exists = DEFAULT_IF_NOT_EXISTS;
    let mut timeout = get_default_timeout();
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Identifier => name = Some(parse_identifier(ast, *child_id)?),
            Rule::ProcParams => {
                let proc_params = ast.nodes.get_node(*child_id)?;
                params = Some(parse_proc_params(ast, proc_params)?);
            }
            Rule::ProcLanguage => {
                // We don't need to parse language node, because we support only SQL.
            }
            Rule::ProcBody => {
                body = child_node
                    .value
                    .as_ref()
                    .expect("procedure body must not be empty")
                    .clone();
            }
            Rule::IfNotExists => if_not_exists = true,
            Rule::Timeout => {
                timeout = get_timeout(ast, *child_id)?;
            }
            Rule::WaitAppliedGlobally => wait_applied_globally = true,
            Rule::WaitAppliedLocally => wait_applied_globally = false,
            _ => unreachable!("Unexpected node: {child_node:?}"),
        }
    }
    let name = name.expect("name expected as a child");
    let params = params.expect("params expected as a child");
    let create_proc = CreateProc {
        name,
        params,
        language,
        body,
        if_not_exists,
        timeout,
        wait_applied_globally,
    };
    Ok(create_proc)
}

pub(in crate::frontend::sql) fn parse_alter_system<M: Metadata>(
    ast: &AstCore,
    node: &ParseNode,
    type_analyzer: &mut TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<AlterSystem, SbroadError> {
    let alter_system_type_node_id = node
        .children
        .first()
        .expect("Alter system type node expected.");
    let alter_system_type_node = ast.nodes.get_node(*alter_system_type_node_id)?;

    let ty = match alter_system_type_node.rule {
        Rule::AlterSystemReset => {
            let param_name =
                if let Some(identifier_node_id) = alter_system_type_node.children.first() {
                    Some(parse_identifier(ast, *identifier_node_id)?)
                } else {
                    None
                };
            AlterSystemType::AlterSystemReset { param_name }
        }
        Rule::AlterSystemSet => {
            let param_name_node_id = alter_system_type_node
                .children
                .first()
                .expect("Param name node expected under Alter system.");
            let param_name = parse_identifier(ast, *param_name_node_id)?;

            if let Some(param_value_node_id) = alter_system_type_node.children.get(1) {
                let expr_pair = pairs_map.remove_pair(*param_value_node_id);
                let expr_plan_node_id = parse_scalar_expr(
                    Pairs::single(expr_pair),
                    type_analyzer,
                    DerivedType::unknown(),
                    &[],
                    worker,
                    plan,
                    true,
                )?;
                let value_node = plan.get_node(expr_plan_node_id)?;
                if let Node::Expression(Expression::Constant(Constant { value })) = value_node {
                    AlterSystemType::AlterSystemSet {
                        param_name,
                        param_value: value.clone(),
                    }
                } else {
                    // TODO: Should be fixed as
                    //       https://git.picodata.io/picodata/picodata/sbroad/-/issues/763
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some(SmolStr::from(
                            "ALTER SYSTEM currently supports only literals as values.",
                        )),
                    ));
                }
            } else {
                // In case of `set <PARAM_NAME> to default` we send `Reset` opcode
                // instead of `Set`.
                AlterSystemType::AlterSystemReset {
                    param_name: Some(param_name),
                }
            }
        }
        _ => unreachable!("Unexpected rule: {:?}", alter_system_type_node.rule),
    };

    let mut tier_name = None;
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;
    let mut timeout = get_default_timeout();
    for child_id in node.children.iter().skip(1) {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::AlterSystemTier => {
                let tier_node_child_id = child_node.first_child();
                let tier_node_child = ast.nodes.get_node(tier_node_child_id)?;
                tier_name = match tier_node_child.rule {
                    Rule::AlterSystemTiersAll => None,
                    Rule::AlterSystemTierSingle => {
                        let node_child_id = tier_node_child.first_child();
                        Some(parse_identifier(ast, node_child_id)?)
                    }
                    _ => panic!("Unexpected rule met under AlterSystemTier."),
                };
            }
            Rule::WaitAppliedGlobally => wait_applied_globally = true,
            Rule::WaitAppliedLocally => wait_applied_globally = false,
            Rule::Timeout => timeout = get_timeout(ast, *child_id)?,
            _ => {}
        }
    }

    Ok(AlterSystem {
        ty,
        tier_name,
        wait_applied_globally,
        timeout,
    })
}

pub(in crate::frontend::sql) fn parse_proc_with_optional_params(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<(SmolStr, Option<Vec<ParamDef>>), SbroadError> {
    let proc_name_id = node.children.first().expect("Expected to get Proc name");
    let proc_name = parse_identifier(ast, *proc_name_id)?;

    let params = if let Some(params_node_id) = node.children.get(1) {
        let params_node = ast.nodes.get_node(*params_node_id)?;
        Some(parse_proc_params(ast, params_node)?)
    } else {
        None
    };

    Ok((proc_name, params))
}

pub(in crate::frontend::sql) fn parse_drop_proc(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<DropProc, SbroadError> {
    let mut name = None;
    let mut params = None;
    let mut timeout = get_default_timeout();
    let mut if_exists = DEFAULT_IF_EXISTS;
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::ProcWithOptionalParams => {
                let (proc_name, proc_params) = parse_proc_with_optional_params(ast, child_node)?;
                name = Some(proc_name);
                params = proc_params;
            }
            Rule::Timeout => timeout = get_timeout(ast, *child_id)?,
            Rule::IfExists => if_exists = true,
            Rule::WaitAppliedGlobally => wait_applied_globally = true,
            Rule::WaitAppliedLocally => wait_applied_globally = false,
            child_node => panic!("unexpected node {child_node:?}"),
        }
    }

    let name = name.expect("drop proc wihtout proc name");
    Ok(DropProc {
        name,
        params,
        if_exists,
        timeout,
        wait_applied_globally,
    })
}

#[allow(clippy::too_many_lines)]
pub(in crate::frontend::sql) fn parse_create_index(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<CreateIndex, SbroadError> {
    assert_eq!(node.rule, Rule::CreateIndex);
    let mut name = SmolStr::default();
    let mut table_name = SmolStr::default();
    let mut columns = Vec::new();
    let mut unique = false;
    let mut index_type = IndexType::Tree;
    let mut vinyl_options = VinylOptions::default();
    let mut dimension = None;
    let mut distance = None;
    let mut hint = None;
    let mut if_not_exists = DEFAULT_IF_NOT_EXISTS;
    let mut timeout = get_default_timeout();
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;

    let first_child = |node: &ParseNode| -> &ParseNode {
        let child_id = node.children.first().expect("Expected to see first child");
        ast.nodes
            .get_node(*child_id)
            .expect("Expected to see first child node")
    };
    let bool_value = |node: &ParseNode| -> bool {
        let node = first_child(node);
        match node.rule {
            Rule::True => true,
            Rule::False => false,
            _ => panic!("expected True or False rule!"),
        }
    };

    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Unique => unique = true,
            Rule::IfNotExists => if_not_exists = true,
            Rule::Identifier => name = parse_identifier(ast, *child_id)?,
            Rule::Table => table_name = parse_identifier(ast, *child_id)?,
            Rule::IndexType => {
                let type_node = first_child(child_node);
                match type_node.rule {
                    Rule::Tree => index_type = IndexType::Tree,
                    Rule::Hash => index_type = IndexType::Hash,
                    Rule::RTree => index_type = IndexType::Rtree,
                    Rule::BitSet => index_type = IndexType::Bitset,
                    _ => panic!("Unexpected type node: {type_node:?}"),
                }
            }
            Rule::Parts => {
                let parts_node = ast.nodes.get_node(*child_id)?;
                columns.reserve(parts_node.children.len());
                for part_id in &parts_node.children {
                    let single_part_node = ast.nodes.get_node(*part_id)?;
                    assert!(
                        single_part_node.rule == Rule::Identifier,
                        "Unexpected part node: {single_part_node:?}"
                    );
                    columns.push(parse_identifier(ast, *part_id)?);
                }
            }
            Rule::IndexOptions => {
                let options_node = ast.nodes.get_node(*child_id)?;
                for option_id in &options_node.children {
                    let option_param_node = ast.nodes.get_node(*option_id)?;
                    assert!(
                        option_param_node.rule == Rule::IndexOptionParam,
                        "Unexpected option node: {option_param_node:?}"
                    );
                    let param_node = first_child(option_param_node);
                    match param_node.rule {
                        Rule::BloomFpr => {
                            vinyl_options.bloom_fpr =
                                Some(parse_option_value(ast, param_node, "bloom_fpr")?)
                        }
                        Rule::PageSize => {
                            vinyl_options.page_size =
                                Some(parse_option_value(ast, param_node, "page_size")?)
                        }
                        Rule::RangeSize => {
                            vinyl_options.range_size =
                                Some(parse_option_value(ast, param_node, "range_size")?)
                        }
                        Rule::RunCountPerLevel => {
                            vinyl_options.run_count_per_level =
                                Some(parse_option_value(ast, param_node, "run_count_per_level")?)
                        }
                        Rule::RunSizeRatio => {
                            vinyl_options.run_size_ratio =
                                Some(parse_option_value(ast, param_node, "run_size_ratio")?)
                        }
                        Rule::CompressionLevel => {
                            vinyl_options.compression_level =
                                Some(parse_option_value(ast, param_node, "compression_level")?)
                        }
                        Rule::Dimension => {
                            dimension = Some(parse_option_value(ast, param_node, "dimension")?)
                        }
                        Rule::Distance => {
                            let distance_node = first_child(param_node);
                            match distance_node.rule {
                                Rule::Euclid => distance = Some(RtreeIndexDistanceType::Euclid),
                                Rule::Manhattan => {
                                    distance = Some(RtreeIndexDistanceType::Manhattan);
                                }
                                _ => panic!("Unexpected distance node: {distance_node:?}"),
                            }
                        }
                        Rule::Hint => {
                            hint = Some(bool_value(param_node));
                            warn!(None, "Hint option is not supported yet");
                        }
                        _ => panic!("Unexpected option param node: {param_node:?}"),
                    }
                }
            }
            Rule::Timeout => timeout = get_timeout(ast, *child_id)?,
            Rule::WaitAppliedGlobally => wait_applied_globally = true,
            Rule::WaitAppliedLocally => wait_applied_globally = false,
            _ => panic!("Unexpected index rule: {child_node:?}"),
        }
    }
    let index = CreateIndex {
        name,
        table_name,
        columns,
        unique,
        if_not_exists,
        index_type,
        vinyl_options,
        dimension,
        distance,
        hint,
        timeout,
        wait_applied_globally,
    };
    Ok(index)
}

pub(in crate::frontend::sql) fn parse_drop_index(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<DropIndex, SbroadError> {
    assert_eq!(node.rule, Rule::DropIndex);
    let mut name = SmolStr::default();
    let mut timeout = get_default_timeout();
    let mut if_exists = DEFAULT_IF_EXISTS;
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Identifier => name = parse_identifier(ast, *child_id)?,
            Rule::Timeout => timeout = get_timeout(ast, *child_id)?,
            Rule::IfExists => if_exists = true,
            Rule::WaitAppliedGlobally => wait_applied_globally = true,
            Rule::WaitAppliedLocally => wait_applied_globally = false,
            _ => panic!("Unexpected drop index node: {child_node:?}"),
        }
    }
    Ok(DropIndex {
        name,
        if_exists,
        timeout,
        wait_applied_globally,
    })
}

pub(in crate::frontend::sql) fn parse_rename_index(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<RenameIndex, SbroadError> {
    assert_eq!(node.rule, Rule::RenameIndex);
    let mut old_name: Option<SmolStr> = None;
    let mut new_name: Option<SmolStr> = None;
    let mut timeout = get_default_timeout();
    let mut if_exists = DEFAULT_IF_EXISTS;
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Identifier => match (&mut old_name, &mut new_name) {
                (None, None) => old_name = Some(parse_identifier(ast, *child_id)?),
                (Some(_), None) => new_name = Some(parse_identifier(ast, *child_id)?),
                _ => unreachable!("Invalid AST for rename index"),
            },
            Rule::Timeout => timeout = get_timeout(ast, *child_id)?,
            Rule::IfExists => if_exists = true,
            Rule::WaitAppliedGlobally => wait_applied_globally = true,
            Rule::WaitAppliedLocally => wait_applied_globally = false,
            _ => panic!("Unexpected drop index node: {child_node:?}"),
        }
    }
    Ok(RenameIndex {
        // It is safe to unwrap old and new index names,
        // as they must be parsed from AST in the code above.
        old_name: old_name.unwrap(),
        new_name: new_name.unwrap(),
        timeout,
        if_exists,
        wait_applied_globally,
    })
}

pub(in crate::frontend::sql) fn parse_domain_type(
    node: &ParseNode,
    ast: &AstCore,
) -> Result<DomainType, SbroadError> {
    let data_type = match node.rule {
        Rule::DomainType => {
            let node_id = node.first_child();
            let node = ast.nodes.get_node(node_id)?;
            match node.rule {
                Rule::TypeUnsigned => DomainType::Unsigned,
                _ => {
                    panic!("Met unexpected rule under DomainType: {:?}.", node.rule);
                }
            }
        }

        Rule::Type => {
            let node_id = node.first_child();
            let node = ast.nodes.get_node(node_id)?;
            match node.rule {
                Rule::TypeBool => DomainType::Boolean,
                Rule::TypeDatetime => DomainType::Datetime,
                Rule::TypeDecimal => DomainType::Decimal,
                Rule::TypeDouble => DomainType::Double,
                Rule::TypeInt => DomainType::Integer,
                Rule::TypeJSON => DomainType::Json,
                Rule::TypeString | Rule::TypeVarchar | Rule::TypeText => DomainType::String,
                Rule::TypeUuid => DomainType::Uuid,
                _ => {
                    panic!("Met unexpected rule under Type: {:?}.", node.rule);
                }
            }
        }
        _ => {
            panic!("Met unexpected rule under ColumnDef: {:?}.", node.rule);
        }
    };
    Ok(data_type)
}

pub(in crate::frontend::sql) fn parse_column_def_type(
    node: &ParseNode,
    ast: &AstCore,
) -> Result<ColumnDefType, SbroadError> {
    debug_assert_eq!(node.rule, Rule::ColumnDefType);
    let data_type_node = ast.nodes.get_node(node.first_child())?;
    let data_type = if node.children.get(1).is_some() {
        let base_rule_node = ast.nodes.get_node(data_type_node.first_child())?;
        let elem = NestedType::try_from(base_rule_node.rule)?;
        DomainType::Array(elem)
    } else {
        parse_domain_type(data_type_node, ast)?
    };
    data_type.try_into()
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::uninlined_format_args)]
pub(in crate::frontend::sql) fn parse_create_table(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<CreateTable, SbroadError> {
    assert_eq!(
        node.rule,
        Rule::CreateTable,
        "Expected rule CreateTable, got {:?}.",
        node.rule
    );
    let mut table_name = SmolStr::default();
    let mut columns: Vec<ColumnDef> = Vec::new();
    let mut pk_keys: Vec<SmolStr> = Vec::new();
    let mut raw_pk_keys = Vec::new();
    let mut shard_key: Vec<SmolStr> = Vec::new();
    let mut engine_type: SpaceEngineType = SpaceEngineType::default();
    let mut explicit_null_columns: AHashSet<SmolStr> = AHashSet::new();
    let mut timeout = get_default_timeout();
    let mut tier = None;
    let mut is_global = false;
    let mut if_not_exists = DEFAULT_IF_NOT_EXISTS;
    let mut unlogged = DEFAULT_UNLOGGED;
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;
    let mut pk_contains_bucket_id = false;
    let mut vinyl_options = VinylOptions::default();

    let nullable_primary_key_column_error = Err(SbroadError::Invalid(
        Entity::Column,
        Some(SmolStr::from(
            "Primary key mustn't contain nullable columns.",
        )),
    ));
    let primary_key_already_declared_error = Err(SbroadError::Invalid(
        Entity::Node,
        Some(format_smolstr!("Primary key has been already declared.",)),
    ));

    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::IfNotExists => if_not_exists = true,
            Rule::Unlogged => unlogged = true,
            Rule::NewTable => {
                table_name = parse_identifier(ast, *child_id)?;
            }
            Rule::Columns => {
                let columns_node = ast.nodes.get_node(*child_id)?;
                for col_id in &columns_node.children {
                    let column_def_node = ast.nodes.get_node(*col_id)?;
                    let column_def_children = &column_def_node.children;

                    let name_node_id = column_def_children
                        .first()
                        .expect("ColumnDef should have a name child node");
                    let name = parse_identifier(ast, *name_node_id)?;

                    let column_ty_node_id = column_def_children
                        .get(1)
                        .expect("ColumnDef should have a type child node");
                    let column_ty_node = ast.nodes.get_node(*column_ty_node_id)?;
                    let data_type = parse_column_def_type(column_ty_node, ast)?;
                    let mut is_nullable = true;

                    for def_child_id in column_def_children.iter().skip(2) {
                        let def_child_node = ast.nodes.get_node(*def_child_id)?;
                        match def_child_node.rule {
                            Rule::ColumnDefIsNull => {
                                is_nullable = parse_column_null_or_not_null(ast, def_child_node)?;
                                if is_nullable {
                                    let name = name.clone();
                                    explicit_null_columns.insert(name);
                                }
                            }
                            Rule::PrimaryKeyMark => {
                                if !pk_keys.is_empty() {
                                    return primary_key_already_declared_error;
                                }

                                let name = name.clone();
                                if is_nullable && explicit_null_columns.contains(&name) {
                                    return nullable_primary_key_column_error;
                                }
                                // Infer not null on primary key column
                                is_nullable = false;
                                pk_keys.push(name);
                            }
                            _ => panic!("Unexpected rules met under ColumnDef."),
                        }
                    }
                    let column_def = ColumnDef {
                        name,
                        data_type,
                        is_nullable,
                    };
                    columns.push(column_def);
                }
            }
            Rule::PrimaryKey => {
                if !pk_keys.is_empty() {
                    return primary_key_already_declared_error;
                }
                let pk_node = ast.nodes.get_node(*child_id)?;
                // First child is a `PrimaryKeyMark` that we should skip.
                for pk_col_id in pk_node.children.iter().skip(1) {
                    let pk_col_name = parse_identifier(ast, *pk_col_id)?;
                    raw_pk_keys.push(pk_col_name);
                }
            }
            Rule::Engine => {
                if let (Some(engine_type_id), None) =
                    (child_node.children.first(), child_node.children.get(1))
                {
                    let engine_type_node = ast.nodes.get_node(*engine_type_id)?;
                    match engine_type_node.rule {
                        Rule::Memtx => {
                            engine_type = SpaceEngineType::Memtx;
                        }
                        Rule::Vinyl => {
                            engine_type = SpaceEngineType::Vinyl;
                        }
                        _ => panic!("Unexpected rule met under Engine."),
                    }
                } else {
                    panic!("Engine rule contains more than one child rule.")
                }
            }
            Rule::Distribution => {
                let distribution_node = ast.nodes.get_node(*child_id)?;
                if let (Some(distribution_type_id), None) = (
                    distribution_node.children.first(),
                    distribution_node.children.get(1),
                ) {
                    let distribution_type_node = ast.nodes.get_node(*distribution_type_id)?;
                    match distribution_type_node.rule {
                        Rule::Global => {
                            is_global = true;
                        }
                        Rule::Sharding => {
                            let sharding_node = ast.nodes.get_node(*distribution_type_id)?;
                            for sharding_node_child in &sharding_node.children {
                                let shard_child = ast.nodes.get_node(*sharding_node_child)?;
                                match shard_child.rule {
                                    Rule::Tier => {
                                        let Some(tier_node_id) = shard_child.children.first()
                                        else {
                                            return Err(SbroadError::Invalid(
                                                Entity::Node,
                                                Some(format_smolstr!(
                                                    "AST table tier node {:?} contains unexpected children",
                                                    distribution_type_node,
                                                )),
                                            ));
                                        };

                                        let tier_name = parse_identifier(ast, *tier_node_id)?;
                                        tier = Some(tier_name);
                                    }
                                    Rule::Identifier => {
                                        let shard_col_name =
                                            parse_identifier(ast, *sharding_node_child)?;

                                        let column_found =
                                            columns.iter().find(|c| c.name == shard_col_name);
                                        if column_found.is_none() {
                                            return Err(SbroadError::Invalid(
                                                Entity::Column,
                                                Some(format_smolstr!(
                                            "Sharding key column {shard_col_name} not found."
                                        )),
                                            ));
                                        }

                                        if let Some(column) = column_found {
                                            if !column.data_type.is_scalar() {
                                                return Err(SbroadError::Invalid(
                                            Entity::Column,
                                            Some(format_smolstr!(
                                                "Sharding key column {shard_col_name} is not of scalar type."
                                            )),
                                        ));
                                            }
                                        }

                                        shard_key.push(shard_col_name);
                                    }
                                    _ => {
                                        return Err(SbroadError::Invalid(
                                            Entity::Node,
                                            Some(format_smolstr!(
                                                "AST table sharding node {:?} contains unexpected children",
                                                distribution_type_node,
                                            )),
                                        ));
                                    }
                                }
                            }
                        }
                        _ => panic!("Unexpected rule met under Distribution."),
                    }
                } else {
                    panic!("Distribution rule contains more than one child rule.")
                }
            }
            Rule::Timeout => {
                timeout = get_timeout(ast, *child_id)?;
            }
            Rule::WaitAppliedGlobally => {
                wait_applied_globally = true;
            }
            Rule::WaitAppliedLocally => {
                wait_applied_globally = false;
            }
            Rule::Partition => {
                warn!(None, "PARTITION BY option is not supported yet.");
            }
            Rule::TableOptions => {
                let table_options_node = ast.nodes.get_node(*child_id)?;
                for option_id in &table_options_node.children {
                    let option_param_node = ast.nodes.get_node(*option_id)?;
                    assert!(
                        option_param_node.rule == Rule::TableOptionParam,
                        "Unexpected option node: {option_param_node:?}"
                    );
                    let param_node_id = option_param_node.first_child();
                    let param_node = ast.nodes.get_node(param_node_id)?;
                    match param_node.rule {
                        Rule::BloomFpr => {
                            vinyl_options.bloom_fpr =
                                Some(parse_option_value(ast, param_node, "bloom_fpr")?);
                        }
                        Rule::PageSize => {
                            vinyl_options.page_size =
                                Some(parse_option_value(ast, param_node, "page_size")?);
                        }
                        Rule::RangeSize => {
                            vinyl_options.range_size =
                                Some(parse_option_value(ast, param_node, "range_size")?);
                        }
                        Rule::RunCountPerLevel => {
                            vinyl_options.run_count_per_level =
                                Some(parse_option_value(ast, param_node, "run_count_per_level")?);
                        }
                        Rule::RunSizeRatio => {
                            vinyl_options.run_size_ratio =
                                Some(parse_option_value(ast, param_node, "run_size_ratio")?);
                        }
                        Rule::CompressionLevel => {
                            vinyl_options.compression_level =
                                Some(parse_option_value(ast, param_node, "compression_level")?);
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::Query,
                                Some(format_smolstr!(
                                    "unexpected table option param: {param_node:?}"
                                )),
                            ));
                        }
                    }
                }
            }
            _ => panic!("Unexpected rule met under CreateTable."),
        }
    }

    for (pk_index, pk_col_name) in raw_pk_keys.into_iter().enumerate() {
        let mut column_found = false;
        for column in &mut columns {
            if column.name == pk_col_name {
                column_found = true;
                if column.is_nullable && explicit_null_columns.contains(&column.name) {
                    return nullable_primary_key_column_error;
                }
                // Infer not null on primary key column
                column.is_nullable = false;
                break;
            }
        }
        if !column_found {
            if !is_global && pk_col_name == DEFAULT_BUCKET_ID_COLUMN_NAME {
                if pk_index == 0 {
                    pk_contains_bucket_id = true;
                    continue;
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::PrimaryKey,
                        Some(format_smolstr!(
                            "Primary key must include {pk_col_name} as first column."
                        )),
                    ));
                }
            }
            return Err(SbroadError::Invalid(
                Entity::Column,
                Some(format_smolstr!(
                    "Primary key column {pk_col_name} not found."
                )),
            ));
        }
        pk_keys.push(pk_col_name);
    }

    if pk_keys.is_empty() {
        if pk_contains_bucket_id {
            return Err(SbroadError::Invalid(
                Entity::PrimaryKey,
                Some(format_smolstr!(
                    "Primary key must include at least one column in addition to {DEFAULT_BUCKET_ID_COLUMN_NAME}.")
                ),
            ));
        }
        return Err(SbroadError::Invalid(
            Entity::PrimaryKey,
            Some(format_smolstr!("Primary key must be declared.")),
        ));
    }
    if !is_global {
        let has_bucket_id_for_sharded = columns
            .iter()
            .any(|c| c.name == DEFAULT_BUCKET_ID_COLUMN_NAME);
        if has_bucket_id_for_sharded {
            return Err(SbroadError::Invalid(
                Entity::Column,
                Some(format_smolstr!(
                    "{DEFAULT_BUCKET_ID_COLUMN_NAME} is reserved for system use in sharded tables. Choose another name."
                )),
            ));
        }
    }
    // infer sharding key from primary key
    if shard_key.is_empty() && !is_global {
        for pk_key in &pk_keys {
            let sharding_column = columns.iter().find(|c| c.name == *pk_key).ok_or_else(|| {
                SbroadError::Other(format_smolstr!("Primary key column {pk_key} not found."))
            })?;
            if !sharding_column.data_type.is_scalar() {
                return Err(SbroadError::Invalid(
                    Entity::Column,
                    Some(format_smolstr!(
                        "Sharding key column {pk_key} is not of scalar type."
                    )),
                ));
            }
        }
        shard_key.clone_from(&pk_keys);
    }

    let sharding_key = if !shard_key.is_empty() {
        Some(shard_key)
    } else {
        if engine_type != SpaceEngineType::Memtx {
            return Err(SbroadError::Unsupported(
                Entity::Query,
                Some("Global tables can use only memtx engine.".into()),
            ));
        };

        if unlogged {
            return Err(SbroadError::Unsupported(
                Entity::Query,
                Some("Global tables can't be unlogged.".into()),
            ));
        }

        None
    };

    if unlogged && engine_type == SpaceEngineType::Vinyl {
        return Err(SbroadError::Unsupported(
            Entity::Query,
            Some("Unlogged tables can use only memtx engine.".into()),
        ));
    }

    Ok(CreateTable {
        name: table_name,
        format: columns,
        primary_key: pk_keys,
        sharding_key,
        engine_type,
        unlogged,
        if_not_exists,
        wait_applied_globally,
        timeout,
        tier,
        pk_contains_bucket_id,
        vinyl_options,
    })
}

/// Parses a `ColumnDefIsNull`, which corresponds to either `NULL` or `NOT NULL` in SQL.
///
/// Returns `true` for `NULL`, `false` for `NOT NULL`.
pub(in crate::frontend::sql) fn parse_column_null_or_not_null(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<bool, SbroadError> {
    debug_assert_eq!(node.rule, Rule::ColumnDefIsNull);

    match (node.children.first(), node.children.get(1)) {
        (None, None) => Ok(true), // NULL explicitly specified
        (Some(child_id), None) => {
            let not_flag_node = ast.nodes.get_node(*child_id)?;
            if let Rule::NotFlag = not_flag_node.rule {
                Ok(false) // NOT NULL specified
            } else {
                panic!("Expected NotFlag rule, got: {:?}.", not_flag_node.rule);
            }
        }
        _ => panic!("Unexpected rule met under ColumnDefIsNull."),
    }
}

pub(in crate::frontend::sql) fn parse_alter_table(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<AlterTable, SbroadError> {
    debug_assert_eq!(
        node.rule,
        Rule::AlterTable,
        "Expected rule AlterTable, got {:?}.",
        node.rule
    );

    let mut table_name = None;
    let mut op = None;
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;
    let mut timeout = get_default_timeout();

    for id in &node.children {
        let node = ast.nodes.get_node(*id)?;
        match node.rule {
            Rule::TableNameIdentifier => {
                table_name = Some(parse_identifier(ast, *id)?);
            }
            Rule::AlterTableColumnActions => {
                let mut column_ops = Vec::new();

                for id in &node.children {
                    let node = ast.nodes.get_node(*id)?;

                    match node.rule {
                        Rule::AlterTableColumnAdd => {
                            let mut if_not_exists = DEFAULT_IF_NOT_EXISTS;
                            for id in &node.children {
                                let node = ast.nodes.get_node(*id)?;
                                match node.rule {
                                    Rule::IfNotExists => if_not_exists = true,
                                    Rule::AlterTableColumnAddParam => {
                                        let name = parse_identifier(ast, node.child_n(0))?;

                                        let column_ty_node = ast.nodes.get_node(node.child_n(1))?;
                                        debug_assert_eq!(column_ty_node.rule, Rule::ColumnDefType);
                                        let data_type = parse_column_def_type(column_ty_node, ast)?;

                                        let is_nullable = if let Some(id) = node.children.get(2) {
                                            let node = ast.nodes.get_node(*id)?;
                                            parse_column_null_or_not_null(ast, node)?
                                        } else {
                                            true // column is nullable by default unless otherwise specified
                                        };

                                        column_ops.push(AlterColumn::Add {
                                            column: ColumnDef {
                                                name,
                                                data_type,
                                                is_nullable,
                                            },
                                            if_not_exists,
                                        });
                                    }
                                    rule => unreachable!("pest should not allow rule: {rule:?}"),
                                }
                            }
                        }
                        Rule::AlterTableColumnRename => {
                            for id in &node.children {
                                let node = ast.nodes.get_node(*id)?;
                                match node.rule {
                                    Rule::AlterTableColumnRenameParam => {
                                        let from = parse_identifier(ast, node.child_n(0))?;
                                        let to = parse_identifier(ast, node.child_n(1))?;

                                        column_ops.push(AlterColumn::Rename { from, to });
                                    }
                                    rule => unreachable!("pest should not allow rule: {rule:?}"),
                                }
                            }
                        }

                        Rule::AlterTableColumnDrop | Rule::AlterTableColumnAlter => {
                            return Err(SbroadError::Unsupported(
                                Entity::Ddl,
                                Some(format_smolstr!(
                                    "`ALTER TABLE _ {}` is not yet supported",
                                    match node.rule {
                                        Rule::AlterTableColumnDrop => {
                                            "DROP COLUMN"
                                        }
                                        Rule::AlterTableColumnAlter => {
                                            "ALTER COLUMN"
                                        }
                                        _ => unreachable!(),
                                    }
                                )),
                            ))
                        }
                        rule => unreachable!("pest should not allow rule: {rule:?}"),
                    }
                }

                op = Some(AlterTableOp::AlterColumn(column_ops));
            }
            Rule::AlterTableRename => {
                // it's the only child
                let id = node.first_child();
                let new_table_name = parse_identifier(ast, id)?;
                op = Some(AlterTableOp::RenameTable { new_table_name });
            }

            Rule::Timeout => {
                timeout = get_timeout(ast, *id)?;
            }
            Rule::WaitAppliedGlobally => {
                wait_applied_globally = true;
            }
            Rule::WaitAppliedLocally => {
                wait_applied_globally = false;
            }
            rule => unreachable!("pest should not allow rule: {rule:?}"),
        }
    }

    Ok(AlterTable {
        name: table_name.expect("should be initialized"),
        wait_applied_globally,
        timeout,
        op: op.expect("should be initialized"),
    })
}

pub(in crate::frontend::sql) fn parse_drop_table(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<DropTable, SbroadError> {
    let mut table_name = SmolStr::default();
    let mut timeout = get_default_timeout();
    let mut if_exists = DEFAULT_IF_EXISTS;
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Table => {
                table_name = parse_identifier(ast, *child_id)?;
            }
            Rule::IfExists => {
                if_exists = true;
            }
            Rule::Timeout => {
                timeout = get_timeout(ast, *child_id)?;
            }
            Rule::WaitAppliedGlobally => {
                wait_applied_globally = true;
            }
            Rule::WaitAppliedLocally => {
                wait_applied_globally = false;
            }
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format_smolstr!(
                        "AST drop table node {:?} contains unexpected children",
                        child_node,
                    )),
                ));
            }
        }
    }
    Ok(DropTable {
        name: table_name,
        if_exists,
        wait_applied_globally,
        timeout,
    })
}

pub(in crate::frontend::sql) fn parse_backup(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<Backup, SbroadError> {
    let mut timeout = get_default_timeout();
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Timeout => {
                timeout = get_timeout(ast, *child_id)?;
            }
            Rule::WaitAppliedGlobally => {
                wait_applied_globally = true;
            }
            Rule::WaitAppliedLocally => {
                wait_applied_globally = false;
            }
            _ => {
                panic!("BACKUP node contains unexpected child: {child_node:?}")
            }
        }
    }
    Ok(Backup {
        wait_applied_globally,
        timeout,
    })
}

pub(in crate::frontend::sql) fn parse_truncate_table(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<TruncateTable, SbroadError> {
    let mut name = SmolStr::default();
    let mut timeout = get_default_timeout();
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Table => {
                name = parse_identifier(ast, *child_id)?;
            }
            Rule::Timeout => {
                timeout = get_timeout(ast, *child_id)?;
            }
            Rule::WaitAppliedGlobally => {
                wait_applied_globally = true;
            }
            Rule::WaitAppliedLocally => {
                wait_applied_globally = false;
            }
            _ => {
                panic!("TRUNCATE node contains unexpected child: {child_node:?}")
            }
        }
    }
    Ok(TruncateTable {
        name,
        wait_applied_globally,
        timeout,
    })
}

pub(in crate::frontend::sql) fn parse_set_param(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<SetParam, SbroadError> {
    let mut scope_type = SetParamScopeType::Session;
    let mut param_value = None;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::SetScope => {
                let set_scope_child_id = child_node
                    .children
                    .first()
                    .expect("SetScope must have child.");
                let set_scope_child = ast.nodes.get_node(*set_scope_child_id)?;
                match set_scope_child.rule {
                    Rule::ScopeSession => {}
                    Rule::ScopeLocal => scope_type = SetParamScopeType::Local,
                    _ => panic!("Unexpected rule met under SetScope."),
                }
            }
            Rule::ConfParam => {
                let conf_param_child_id = child_node
                    .children
                    .first()
                    .expect("ConfParam must have child.");
                let conf_param_child = ast.nodes.get_node(*conf_param_child_id)?;
                match conf_param_child.rule {
                    Rule::NamedParam => {
                        let param_name_id = conf_param_child
                            .children
                            .first()
                            .expect("Param name expected under NamedParam.");
                        let param_name = parse_identifier(ast, *param_name_id)?;
                        param_value = Some(SetParamValue::NamedParam { name: param_name });
                    }
                    Rule::TimeZoneParam => param_value = Some(SetParamValue::TimeZone),
                    _ => panic!("Unexpected rule met under ConfParam."),
                }
            }
            _ => panic!("Unexpected rule met under SetParam."),
        }
    }
    Ok(SetParam {
        scope_type,
        param_value: param_value.unwrap(),
        timeout: get_default_timeout(),
    })
}

pub(in crate::frontend::sql) fn parse_deallocate(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<Deallocate, SbroadError> {
    let param_name = if let Some(identifier_node_id) = node.children.first() {
        Some(parse_identifier(ast, *identifier_node_id)?)
    } else {
        None
    };
    Ok(Deallocate { name: param_name })
}

pub(in crate::frontend::sql) fn parse_select_full<M: Metadata>(
    ast: &AstCore,
    node_id: usize,
    map: &mut Translation,
    plan: &mut Plan,
    type_analyzer: &mut TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
) -> Result<(), SbroadError> {
    let node = ast.nodes.get_node(node_id)?;
    assert_eq!(node.rule, Rule::SelectFull);
    let mut top_id = None;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Cte => continue,
            Rule::SelectStatement => {
                top_id = Some(parse_select_statement(
                    ast,
                    *child_id,
                    map,
                    plan,
                    type_analyzer,
                    pairs_map,
                    worker,
                )?);
            }
            _ => unreachable!("Unexpected node: {child_node:?}"),
        }
    }
    let top_id = top_id.expect("SelectFull must have at least one child");
    map.add(node_id, top_id);
    Ok(())
}

pub(in crate::frontend::sql) fn parse_select_statement<M: Metadata>(
    ast: &AstCore,
    node_id: usize,
    map: &mut Translation,
    plan: &mut Plan,
    type_analyzer: &mut TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
) -> Result<NodeId, SbroadError> {
    let node = ast.nodes.get_node(node_id)?;
    assert_eq!(node.rule, Rule::SelectStatement);
    let mut top_id = None;
    let mut limit = None;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::SelectWithOptionalContinuation => {
                let select_id = map.get(*child_id)?;
                top_id = Some(select_id);
            }
            Rule::Limit => {
                let child_node = ast.nodes.get_node(child_node.children[0])?;
                match child_node.rule {
                    Rule::Unsigned => limit = Some(parse_unsigned(child_node)?),
                    Rule::LimitAll => (), // LIMIT ALL is the same as omitting the LIMIT clause
                    _ => unreachable!("Unexpected limit child: {child_node:?}"),
                }
            }
            Rule::OrderBy => {
                top_id = Some(ast.parse_order_by(
                    plan,
                    top_id,
                    *child_id,
                    type_analyzer,
                    map,
                    pairs_map,
                    worker,
                )?);
            }
            _ => unreachable!("Unexpected node: {child_node:?}"),
        }
    }
    let top_id = top_id.expect("SelectStatement must have at least one child");
    if let Some(limit) = limit {
        // It's guaranteed from `parse_unsigned` that limit > 0, so cast is safe.
        return plan.add_limit(top_id, limit as u64);
    }
    Ok(top_id)
}

pub(in crate::frontend::sql) fn parse_scan_cte_or_table<M>(
    ast: &AstCore,
    metadata: &M,
    node_id: usize,
    map: &mut Translation,
    ctes: &mut CTEs,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    let node = ast.nodes.get_node(node_id)?;
    assert_eq!(node.rule, Rule::ScanCteOrTable);
    let scan_name = parse_normalized_identifier(ast, node_id)?;
    // First we try to find CTE with the given name, cause CTE should have higher precedence
    // over table with the same name
    if let Some(child_id) = ctes.get(&scan_name) {
        let cte_id = plan.add_cte_scan(*child_id, scan_name.clone())?;
        map.add(node_id, cte_id);
        return Ok(());
    }

    let table = metadata.table(scan_name.as_str());
    match table {
        Ok(table) => {
            plan.add_rel(table);
            let scan_id = plan.add_scan(scan_name.as_str(), None)?;
            map.add(node_id, scan_id);

            Ok(())
        }
        Err(SbroadError::NotFound(..)) => Err(SbroadError::NotFound(
            Entity::Table,
            format_smolstr!("with name {}", to_user(scan_name)),
        )),
        Err(e) => Err(e),
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::frontend::sql) fn parse_cte<M: Metadata>(
    ast: &AstCore,
    node_id: usize,
    map: &mut Translation,
    ctes: &mut CTEs,
    plan: &mut Plan,
    type_analyzer: &mut TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
) -> Result<(), SbroadError> {
    let node = ast.nodes.get_node(node_id)?;
    assert_eq!(node.rule, Rule::Cte);
    let mut name = None;
    let mut columns = Vec::with_capacity(node.children.len());
    let mut top_id = None;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Identifier => {
                name = Some(parse_normalized_identifier(ast, *child_id)?);
            }
            Rule::CteColumn => {
                let column_name = parse_normalized_identifier(ast, *child_id)?;
                columns.push(column_name);
            }
            Rule::Values => {
                let select_id = map.get(*child_id)?;
                top_id = Some(select_id);
            }
            Rule::SelectStatement => {
                let select_id = parse_select_statement(
                    ast,
                    *child_id,
                    map,
                    plan,
                    type_analyzer,
                    pairs_map,
                    worker,
                )?;
                top_id = Some(select_id);
            }
            _ => unreachable!("Unexpected node: {child_node:?}"),
        }
    }
    let name = name.expect("CTE must have a name");
    let child_id = top_id.expect("CTE must contain a single child");
    if ctes.get(&name).is_some() {
        return Err(SbroadError::Invalid(
            Entity::Cte,
            Some(format_smolstr!(
                "CTE with name {} is already defined",
                to_user(name)
            )),
        ));
    }
    let child_id = plan.transform_cte_columns(child_id, name.clone(), columns)?;
    ctes.insert(name, child_id);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(in crate::frontend::sql) fn parse_insert<'a, M: Metadata>(
    node: &ParseNode,
    ast: &AstCore,
    map: &Translation,
    type_analyzer: &mut TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap<'a>,
    worker: &mut ExpressionWalker<'a, M>,
    plan: &mut Plan,
) -> Result<NodeId, SbroadError> {
    let ast_table_id = node.children.first().expect("Insert has no children.");
    let relation = parse_normalized_identifier(ast, *ast_table_id)?;

    let ast_child_id = node
        .children
        .get(1)
        .expect("Second child not found among Insert children");
    let ast_child = ast.nodes.get_node(*ast_child_id)?;

    let rel = plan.relations.get(&relation).ok_or_else(|| {
        SbroadError::NotFound(
            Entity::Table,
            format_smolstr!("{relation} among plan relations"),
        )
    })?;

    let (plan_child_id, selected_col_names, conflict_child_idx) =
        if let Rule::TargetColumns = ast_child.rule {
            // insert into t (a, b, c) ...
            let mut selected_col_names: Vec<SmolStr> = Vec::with_capacity(ast_child.children.len());
            for col_id in &ast_child.children {
                selected_col_names.push(parse_normalized_identifier(ast, *col_id)?.to_smolstr());
            }

            if selected_col_names.len() > 1 {
                let mut existing_col_names: HashSet<&str, RepeatableState> =
                    HashSet::with_capacity_and_hasher(selected_col_names.len(), RepeatableState);
                for selected_col in &selected_col_names {
                    if !existing_col_names.insert(selected_col) {
                        return Err(SbroadError::DuplicatedValue(format_smolstr!(
                            "column {} specified more than once",
                            to_user(selected_col)
                        )));
                    }
                }
            }

            for column in &rel.columns {
                if let ColumnRole::Sharding = column.get_role() {
                    continue;
                }
                if !column.is_nullable && !selected_col_names.contains(&column.name) {
                    return Err(SbroadError::Invalid(
                        Entity::Column,
                        Some(format_smolstr!(
                            "NonNull column {} must be specified",
                            to_user(&column.name)
                        )),
                    ));
                }
            }

            let mut column_types = Vec::with_capacity(selected_col_names.len());
            for name in &selected_col_names {
                let column = rel
                    .columns
                    .iter()
                    .find(|c| &c.name == name)
                    .ok_or_else(|| {
                        SbroadError::Other(format_smolstr!(
                            "column {} of table {} does not exist",
                            name,
                            &relation
                        ))
                    })?;
                let col_type = column.r#type.get().expect("column type must be known");
                column_types.push(col_type);
            }

            let ast_rel_child_id = node
                .children
                .get(2)
                .expect("Third child not found among Insert children");
            let plan_rel_child_id = parse_insert_source(
                *ast_rel_child_id,
                ast,
                &column_types,
                map,
                type_analyzer,
                pairs_map,
                worker,
                plan,
            )?;
            (plan_rel_child_id, selected_col_names, 3)
        } else {
            // insert into t ...
            let mut column_types = Vec::with_capacity(rel.columns.len());
            for column in &rel.columns {
                if column.role != ColumnRole::Sharding {
                    let col_type = column.r#type.get().expect("column type must be known");
                    column_types.push(col_type);
                }
            }

            let plan_child_id = parse_insert_source(
                *ast_child_id,
                ast,
                &column_types,
                map,
                type_analyzer,
                pairs_map,
                worker,
                plan,
            )?;
            (plan_child_id, Vec::new(), 2)
        };

    let mut conflict_ctx = insert_conflict::DoUpdateParseContext {
        plan,
        let_scope: &mut worker.let_scope,
        type_analyzer,
        subquery_map: &worker.subquery_replaces,
        pairs_map,
        tnt_parameters_positions: &worker.tnt_parameters_positions,
    };
    let conflict_strategy = insert_conflict::parse_insert_conflict_clause(
        node,
        ast,
        conflict_child_idx,
        &relation,
        &mut conflict_ctx,
    )
    .map_err(error::map_sbroad_error)?;
    plan.add_insert(
        &relation,
        plan_child_id,
        &selected_col_names,
        conflict_strategy,
    )
}

#[allow(clippy::too_many_arguments)]
pub(in crate::frontend::sql) fn parse_insert_source<M: Metadata>(
    node_id: usize,
    ast: &AstCore,
    column_types: &[UnrestrictedType],
    map: &Translation,
    type_analyzer: &mut TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<NodeId, SbroadError> {
    use sql_type_system::expr::Type;

    let node = ast.nodes.get_node(node_id)?;
    match node.rule {
        Rule::SelectFull => map.get(node_id),
        Rule::InsertValues => {
            let values_rows_ids = parse_values_rows(
                &node.children,
                type_analyzer,
                column_types,
                pairs_map,
                worker,
                plan,
            )
            .map_err(|err| match err {
                SbroadError::TypeSystemError(
                    TypeSystemError::DesiredTypesCannotBeMatchedWithExprs(types, exprs),
                ) => SbroadError::Invalid(
                    Entity::Query,
                    Some(format_smolstr!(
                        "INSERT expects {types} columns, got {exprs}"
                    )),
                ),
                other => other,
            })?;

            let values_id = plan.add_values(values_rows_ids)?;

            let Relational::Values(Values { children, .. }) = plan.get_relation_node(values_id)?
            else {
                panic!("Expected to insert Values, got something else");
            };

            let report = type_analyzer.get_report();
            for row_id in children {
                if let Relational::ValuesRow(ValuesRow { data, .. }) =
                    plan.get_relation_node(*row_id)?
                {
                    let Expression::Row(Row { list, .. }) = plan.get_expression_node(*data)? else {
                        panic!("Expected to get Row as ValuesRow child, got something else");
                    };

                    for (idx, expr_id) in list.iter().enumerate() {
                        let coltype = column_types[idx];
                        let exprtype = report.get_type(expr_id);
                        if !can_assign(exprtype.into(), coltype) {
                            return Err(SbroadError::Other(format_smolstr!(
                                "INSERT column at position {} is of type {}, but expression is of type {}",
                                idx + 1,
                                Type::from(coltype),
                                exprtype,
                            )));
                        }
                    }
                }
            }

            Ok(values_id)
        }
        rule => panic!("unexpected insert source {rule:?}"),
    }
}
