//! Abstract syntax tree (AST) module.
//!
//! This module contains a definition of the abstract syntax tree
//! constructed from the nodes of the `pest` tree iterator nodes.

extern crate pest;

pub(in crate::frontend::sql) mod core;
pub(in crate::frontend::sql) mod ir_populator;

use self::core::*;

use crate::frontend::sql::type_system;

use crate::ir::node::tcl::Tcl;

use crate::ir::index::Index;
use crate::ir::node::ddl::DdlOwned;
use crate::ir::types::{DerivedType, UnrestrictedType};
use ::core::panic;
use itertools::Itertools;
use pest::iterators::{Pair, Pairs};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::collections::{HashMap, HashSet};

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::engine::helpers::to_user;
use crate::executor::engine::Metadata;

use crate::frontend::sql::ir::Translation;

use crate::ir::acl::AlterOption;
use crate::ir::expression::NewColumnsSource;
use crate::ir::expression::{ColumnWithScan, ColumnsRetrievalSpec, ExpressionId};
use crate::ir::helpers::RepeatableState;
use crate::ir::node::relational::{MutRelational, Relational};
use crate::ir::node::{
    AlterUser, CreateRole, CreateUser, DropRole, DropUser, GrantPrivilege, NodeAligned, NodeId,
    RevokePrivilege, ScanRelation, SetTransaction,
};
use crate::ir::operator::JoinKind;
use crate::ir::options::{OptionKind, OptionSpec};
use crate::ir::relation::{Column, ColumnRole, Table, TableKind};
use crate::ir::transformation::redistribution::ColumnPosition;
use crate::ir::tree::traversal::PostOrder;
use crate::ir::value::Value;
use crate::ir::{ExplainOptions, Plan};
use tarantool::auth::AuthMethod;
use type_system::get_parameter_derived_types;

use self::ir_populator::{
    can_assign, dql_return_columns, parse_scalar_expr, parse_select, parse_values_rows,
    ExpressionWalker,
};

// TODO: implement and use `AuthMethod::DEFAULT`
// when tarantool-module gets Md5 as default one
const DEFAULT_AUTH_METHOD: AuthMethod = AuthMethod::Md5;

const DEFAULT_IF_EXISTS: bool = false;
const DEFAULT_IF_NOT_EXISTS: bool = false;
const DEFAULT_UNLOGGED: bool = false;

const DEFAULT_WAIT_APPLIED_GLOBALLY: bool = true;

/// The default column name where sharded tables store the `bucket_id`.
/// The sharding key is mapped (via hashing) to a `bucket_id` value stored in this column.
pub const DEFAULT_BUCKET_ID_COLUMN_NAME: &str = "bucket_id";

use ahash::AHashMap;

/// Generate an alias for the unnamed projection expressions.
#[must_use]
pub fn get_unnamed_column_alias(pos: usize) -> SmolStr {
    format_smolstr!("col_{pos}")
}

use crate::frontend::sql::Ast;

/// Total average number of Expr and Row rule nodes that we expect to get
/// in the parsing result returned from pest. Used for preallocating `ParsingPairsMap`.
const PARSING_PAIRS_MAP_CAPACITY: usize = 100;

/// Parse tree
#[derive(Parser)]
#[grammar = "frontend/sql/ast/query.pest"]
pub(super) struct ParseTree;

impl<'q> AbstractSyntaxTree<'q> {
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::uninlined_format_args)]
    pub(super) fn build_ir<M>(
        self,
        metadata: &M,
        param_types: &[DerivedType],
    ) -> Result<Plan, SbroadError>
    where
        M: Metadata,
    {
        if self.is_empty() {
            return Ok(Plan::empty());
        }

        let mut plan = Plan::default();

        let mut ast_meta = self.meta;
        let pairs_map = &mut ast_meta.pairs_map;
        let pos_to_ast_id = &mut ast_meta.pos_to_ast_id;
        let sq_pair_to_ast_ids = &mut ast_meta.pairs_to_ast_id;
        let tnt_parameters_positions = ast_meta.tnt_parameters_ordered;

        let ast = &self.core;

        let mut type_analyzer = type_system::new_analyzer(param_types);

        let Some(top) = ast.top else {
            return Err(SbroadError::Invalid(Entity::AST, None));
        };
        let capacity = ast.nodes.arena.len();
        let dft_post = PostOrder::new(|node| ast.nodes.ast_iter(node), capacity);
        // Map of { ast `ParseNode` id -> plan `Node` id }.
        let mut map = Translation::with_capacity(ast.nodes.next_id());
        let mut worker =
            ExpressionWalker::new(metadata, sq_pair_to_ast_ids, tnt_parameters_positions);
        let mut ctes = CTEs::new();
        // This flag disables resolving of table names for DROP TABLE queries,
        // as it can be used with tables that are not presented in metadata.
        // Unresolved table names are handled in picodata depending in IF EXISTS options.
        let resolve_table_names = ast.nodes.get_node(top)?.rule != Rule::DropTable;

        let mut used_aliases = HashSet::new();
        let mut unnamed_subqueries = Vec::new();

        for level_node in dft_post.traverse_into_iter(top) {
            let id = level_node.1;
            let node = ast.nodes.get_node(id)?;
            match &node.rule {
                // DQL: relation construction, SELECT stages, and VALUES.
                Rule::SelectFull => parse_select_full(
                    ast,
                    id,
                    &mut map,
                    &mut plan,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                )?,
                Rule::Cte => parse_cte(
                    ast,
                    id,
                    &mut map,
                    &mut ctes,
                    &mut plan,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                )?,
                Rule::SelectWithOptionalContinuation => {
                    build_select_with_optional_continuation_ir(
                        id,
                        &mut map,
                        pos_to_ast_id,
                        pairs_map,
                        &mut plan,
                    )?;
                }
                Rule::Projection => build_projection_ir(
                    ast,
                    id,
                    node,
                    &mut map,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                    &mut plan,
                )?,
                Rule::Scan => build_scan_ir(
                    ast,
                    metadata,
                    id,
                    node,
                    &mut map,
                    &mut plan,
                    &mut used_aliases,
                    &mut unnamed_subqueries,
                )?,
                Rule::ScanTable => build_scan_table_ir(ast, id, node, &mut map, &mut plan)?,
                Rule::ScanCteOrTable => {
                    parse_scan_cte_or_table(ast, metadata, id, &mut map, &mut ctes, &mut plan)?;
                }
                Rule::Table if resolve_table_names => {
                    register_table_relation(ast, metadata, id, &mut plan)?;
                }
                Rule::GroupBy => build_group_by_ir(
                    id,
                    node,
                    &mut map,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                    &mut plan,
                )?,
                Rule::Having | Rule::Selection => build_filter_ir(
                    id,
                    node,
                    &mut map,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                    &mut plan,
                )?,
                Rule::Join => build_join_ir(
                    ast,
                    id,
                    node,
                    &mut map,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                    &mut plan,
                )?,
                Rule::NamedWindows => ast.parse_named_windows(
                    &mut plan,
                    id,
                    &mut type_analyzer,
                    &mut map,
                    pairs_map,
                    &mut worker,
                )?,
                Rule::Values => build_values_ir(
                    id,
                    node,
                    &mut map,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                    &mut plan,
                )?,
                Rule::SubQuery => build_sub_query_ir(id, node, &mut map, &mut worker, &mut plan)?,
                Rule::WindowBody => {}

                // DML.
                Rule::Insert => build_insert_ir(
                    ast,
                    id,
                    node,
                    &mut map,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                    &mut plan,
                )?,
                Rule::Update => build_update_ir(
                    ast,
                    id,
                    node,
                    &mut map,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                    &mut plan,
                )?,
                Rule::Delete => build_delete_ir(
                    ast,
                    metadata,
                    id,
                    node,
                    &mut map,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                    &mut plan,
                )?,

                // DDL.
                Rule::CreateIndex => {
                    push_mapped_plan_node(&mut plan, &mut map, id, parse_create_index(ast, node)?);
                }
                Rule::CreateSchema => {
                    push_mapped_plan_node(&mut plan, &mut map, id, DdlOwned::CreateSchema);
                }
                Rule::AlterSystem => build_alter_system_ir(
                    ast,
                    id,
                    node,
                    &mut map,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                    &mut plan,
                )?,
                Rule::CreateProc => {
                    push_mapped_plan_node(&mut plan, &mut map, id, parse_create_proc(ast, node)?);
                }
                Rule::CreateTable => {
                    push_mapped_plan_node(&mut plan, &mut map, id, parse_create_table(ast, node)?);
                }
                Rule::CreatePartition => {
                    return Err(SbroadError::NotImplemented(
                        Entity::Rule,
                        format_smolstr!("PARTITION OF logic is not supported yet."),
                    ));
                }
                Rule::DropIndex => {
                    push_mapped_plan_node(&mut plan, &mut map, id, parse_drop_index(ast, node)?);
                }
                Rule::RenameIndex => {
                    push_mapped_plan_node(&mut plan, &mut map, id, parse_rename_index(ast, node)?);
                }
                Rule::DropSchema => {
                    push_mapped_plan_node(&mut plan, &mut map, id, DdlOwned::DropSchema);
                }
                Rule::DropTable => {
                    push_mapped_plan_node(&mut plan, &mut map, id, parse_drop_table(ast, node)?);
                }
                Rule::Backup => {
                    push_mapped_plan_node(&mut plan, &mut map, id, parse_backup(ast, node)?);
                }
                Rule::TruncateTable => {
                    push_mapped_plan_node(
                        &mut plan,
                        &mut map,
                        id,
                        parse_truncate_table(ast, node)?,
                    );
                }
                Rule::DropProc => {
                    push_mapped_plan_node(&mut plan, &mut map, id, parse_drop_proc(ast, node)?);
                }
                Rule::RenameProc => {
                    push_mapped_plan_node(&mut plan, &mut map, id, parse_rename_proc(ast, node)?);
                }
                Rule::AlterTable => {
                    push_mapped_plan_node(&mut plan, &mut map, id, parse_alter_table(ast, node)?);
                }
                Rule::CreatePlugin => {
                    push_mapped_plan_node(&mut plan, &mut map, id, parse_create_plugin(ast, node)?);
                }
                Rule::DropPlugin => {
                    push_mapped_plan_node(&mut plan, &mut map, id, parse_drop_plugin(ast, node)?);
                }
                Rule::AlterPlugin => {
                    if let Some(node_id) = parse_alter_plugin(ast, &mut plan, node)? {
                        map.add(id, node_id);
                    }
                }
                Rule::SetParam => {
                    push_mapped_plan_node(&mut plan, &mut map, id, parse_set_param(ast, node)?);
                }
                Rule::SetTransaction => {
                    push_mapped_plan_node(
                        &mut plan,
                        &mut map,
                        id,
                        SetTransaction {
                            timeout: get_default_timeout(),
                        },
                    );
                }

                // ACL.
                Rule::GrantPrivilege => {
                    build_grant_privilege_ir(ast, id, node, &mut map, &mut plan)?
                }
                Rule::RevokePrivilege => {
                    build_revoke_privilege_ir(ast, id, node, &mut map, &mut plan)?;
                }
                Rule::CreateRole => build_create_role_ir(ast, id, node, &mut map, &mut plan)?,
                Rule::DropRole => build_drop_role_ir(ast, id, node, &mut map, &mut plan)?,
                Rule::CreateUser => build_create_user_ir(ast, id, node, &mut map, &mut plan)?,
                Rule::AlterUser => build_alter_user_ir(ast, id, node, &mut map, &mut plan)?,
                Rule::DropUser => build_drop_user_ir(ast, id, node, &mut map, &mut plan)?,
                Rule::AuditPolicy => {
                    push_mapped_plan_node(&mut plan, &mut map, id, parse_audit_policy(ast, node)?);
                }

                // Explain.
                Rule::Explain => build_explain_ir(ast, node, &mut map, &mut plan)?,

                // Other statements and query options.
                Rule::MotionRowMax => build_option_ir(
                    ast,
                    node,
                    OptionKind::MotionRowMax,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                    &mut plan,
                )?,
                Rule::ReadPreference => build_option_ir(
                    ast,
                    node,
                    OptionKind::ReadPreference,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                    &mut plan,
                )?,
                Rule::Forward => build_option_ir(
                    ast,
                    node,
                    OptionKind::Forward,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                    &mut plan,
                )?,
                Rule::VdbeOpcodeMax => build_option_ir(
                    ast,
                    node,
                    OptionKind::VdbeOpcodeMax,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                    &mut plan,
                )?,
                Rule::Query => {
                    map_first_child(id, node, &mut map, "no children for Query rule")?;
                }
                Rule::BlockReturnQueryStatement => {
                    map_first_child(
                        id,
                        node,
                        &mut map,
                        "no children for BlockReturnQueryStatement rule",
                    )?;
                }
                Rule::BlockQueryStatement => {
                    map_first_child(
                        id,
                        node,
                        &mut map,
                        "no children for BlockQueryStatement rule",
                    )?;
                }
                Rule::BlockLetStatement => {
                    build_block_let_statement_ir(ast, id, node, &mut map, &mut worker, &mut plan)?;
                }
                Rule::BlockIfCondition => build_block_if_condition_ir(
                    id,
                    node,
                    &mut map,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                    &mut plan,
                )?,
                Rule::BlockIfBodyStatement => {
                    build_block_if_body_statement_ir(ast, id, node, &mut map)?;
                }
                Rule::BlockIfStatement => {}
                Rule::AnonymousBlock => build_anonymous_block_ir(
                    ast,
                    id,
                    &mut map,
                    &mut type_analyzer,
                    &worker,
                    &mut plan,
                )?,
                Rule::CallProc => build_call_proc_ir(
                    ast,
                    id,
                    node,
                    &mut map,
                    &mut type_analyzer,
                    pairs_map,
                    &mut worker,
                    &mut plan,
                )?,
                Rule::Deallocate => {
                    push_mapped_plan_node(&mut plan, &mut map, id, parse_deallocate(ast, node)?);
                }
                Rule::Begin => {
                    push_mapped_plan_node(&mut plan, &mut map, id, Tcl::Begin);
                }
                // END is a Postgres extension that has the same semantics as COMMIT.
                Rule::Commit | Rule::End => {
                    push_mapped_plan_node(&mut plan, &mut map, id, Tcl::Commit);
                }
                Rule::Rollback => {
                    push_mapped_plan_node(&mut plan, &mut map, id, Tcl::Rollback);
                }
                _ => {}
            }
        }

        used_aliases.extend(ctes.into_keys());
        used_aliases.extend(plan.relations.tables.keys().cloned());
        plan.context_mut().set_used_aliases(used_aliases);
        for node in unnamed_subqueries {
            let name = plan.context_mut().get_unnamed_subquery_name();

            let mut scan = plan.get_mut_relation_node(node)?;
            scan.set_scan_name(Some(name))?;
        }

        // get root node id
        let plan_top_id = map
            .get(ast.top.ok_or_else(|| {
                SbroadError::Invalid(Entity::AST, Some("no top in AST".into()))
            })?)?;
        plan.set_top(plan_top_id)?;

        let mut tiers = plan
            .relations
            .tables
            .iter()
            .filter(|(_, table)| matches!(table.kind, TableKind::ShardedSpace { .. }))
            .map(|(_, table)| table.tier.as_ref());

        // check that all tables from query from one vshard group. Ignore global tables.
        if !tiers.clone().all_equal() {
            return Err(SbroadError::Invalid(
                Entity::Query,
                Some("Query cannot use tables from different tiers".into()),
            ));
        }

        plan.tier = tiers.next().flatten().cloned();

        let param_types = get_parameter_derived_types(&type_analyzer);
        plan.set_types_in_parameter_nodes(&param_types)?;

        // Some recalculations that need to be performed after all parameter types are known.
        // TODO: They are likely to be redundant and should be removed because now parameters get
        // their types as soon as they are parsed but it needs to be investigated.
        plan.update_value_rows()?;
        plan.recalculate_ref_types()?;

        // This was the last pass in this pipeline that is allowed to consult
        // type analyzer.
        plan.explicit_cast_func_args(&type_analyzer)?;

        // Prevent accidental use of stale type metadata after this point:
        // the rewrites below create fresh NodeIds absent from the report.
        drop(type_analyzer);

        // The rewrites below must not read type report: they are only safe here
        // because they preserve expression types semantically or copy already known
        // reference types into newly created nodes.

        // This pass only clones the lhs subtree of `BETWEEN` and gives the copy fresh
        // `NodeId`s. It does not retarget references or change expression types, so it
        // is safe to keep it after `recalculate_ref_types()`.
        worker.fix_betweens(&mut plan)?;
        // This rewrite retargets references inside copied subtrees, but the current
        // `Projection -> GroupBy/Having -> child` contract preserves column positions
        // and types, so the expression type should stay the same. Be careful if we
        // ever change this invariant: this pass would likely need extra type refresh.
        plan.fix_groupby_aliases()?;
        // This rewrite creates new references with `col_type` copied from the
        // existing projection-output references, so it does not need to consult
        // type analyzer or refresh ref types after `recalculate_ref_types()`.
        // NB: depends on `recalculate_ref_types` having run before cloning,
        // so that cloned references already carry correct `col_type` values.
        plan.replace_group_by_ordinals_with_references()?;

        Ok(plan)
    }
}

impl<'q> Ast<'q> for AbstractSyntaxTree<'q> {
    type AnalyzedAst = Self;

    fn new(query: &'q str) -> Result<Self, SbroadError> {
        let mut ast = Self::empty();

        let meta = &mut ast.meta;
        let pairs_map = &mut meta.pairs_map;
        let pos_to_ast_id = &mut meta.pos_to_ast_id;
        let pairs_to_ast_id = &mut meta.pairs_to_ast_id;
        let tnt_parameters_ordered = &mut meta.tnt_parameters_ordered;

        let ast_core = &mut ast.core;
        ast_core.fill(
            query,
            pairs_map,
            pos_to_ast_id,
            pairs_to_ast_id,
            tnt_parameters_ordered,
        )?;
        Ok(ast)
    }

    fn analyze(
        self,
        _metadata: &'q impl Metadata,
        _param_types: &'q [DerivedType],
    ) -> Result<Self::AnalyzedAst, SbroadError> {
        Ok(self)
    }
}

fn push_mapped_plan_node<N>(
    plan: &mut Plan,
    map: &mut Translation,
    ast_id: usize,
    node: N,
) -> NodeId
where
    N: Into<NodeAligned>,
{
    let plan_id = plan.nodes.push(node.into());
    map.add(ast_id, plan_id);
    plan_id
}

fn register_table_relation<M>(
    ast: &AstCore,
    metadata: &M,
    table_node_id: usize,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    // The thing is we don't want to normalize name.
    // Should we fix `parse_identifier` or `table` logic?
    let table_name = parse_identifier(ast, table_node_id)?;
    let table = metadata.table(&table_name)?;
    plan.add_rel(table);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_scan_ir<M>(
    ast: &AstCore,
    metadata: &M,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    plan: &mut Plan,
    used_aliases: &mut HashSet<SmolStr>,
    unnamed_subqueries: &mut Vec<NodeId>,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    let rel_child_id_ast = node
        .children
        .first()
        .expect("could not find first child id in scan node");
    let rel_child_id_plan = map.get(*rel_child_id_ast)?;
    let rel_child_node = plan.get_relation_node(rel_child_id_plan)?;
    let rel_child_is_table = matches!(rel_child_node, Relational::ScanRelation(_));
    let rel_child_is_subquery = matches!(rel_child_node, Relational::ScanSubQuery(_));
    map.add(node_id, rel_child_id_plan);

    let alias = node
        .children
        .get(1)
        .map(|ast_id| parse_optional_identifier(ast, *ast_id))
        .transpose()?
        .flatten();

    let indexed_by_id = if alias.is_some() {
        node.children.get(2)
    } else {
        node.children.get(1)
    };
    let indexed_by = indexed_by_id
        .map(|ast_id| parse_indexed_by_expr(ast, *ast_id))
        .transpose()?
        .flatten();

    if indexed_by.is_some() && !rel_child_is_table {
        return Err(SbroadError::Invalid(
            Entity::Index,
            Some("INDEXED BY clause is only supported for tables".to_smolstr()),
        ));
    }

    if let Some(alias_name) = alias {
        used_aliases.insert(alias_name.clone());
        let mut scan = plan.get_mut_relation_node(rel_child_id_plan)?;
        scan.set_scan_name(Some(alias_name.to_smolstr()))?;
    } else if rel_child_is_subquery {
        unnamed_subqueries.push(rel_child_id_plan);
    }

    if let Some(index_name) = indexed_by {
        set_scan_indexed_by(metadata, plan, rel_child_id_plan, index_name)?;
    }

    Ok(())
}

fn set_scan_indexed_by<M>(
    metadata: &M,
    plan: &mut Plan,
    scan_id: NodeId,
    index_name: SmolStr,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    let (table_id, index_id) = {
        let MutRelational::ScanRelation(ScanRelation {
            relation,
            indexed_by,
            ..
        }) = plan.get_mut_relation_node(scan_id)?
        else {
            unreachable!();
        };
        let index_id = metadata.get_index_id(&index_name, relation)?;
        let table = metadata.table(relation)?;
        *indexed_by = Some(index_name.clone());
        (table.id, index_id)
    };

    let index = Index::from(table_id, index_id);
    plan.indexes.insert(index_name, index);
    plan.index_version_map.insert([table_id, index_id], 0);
    Ok(())
}

fn build_scan_table_ir(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    plan: &mut Plan,
) -> Result<(), SbroadError> {
    let ast_table_id = node
        .children
        .first()
        .expect("could not find first child id in scan table node");
    let scan_id = plan.add_scan(
        parse_normalized_identifier(ast, *ast_table_id)?.as_str(),
        None,
    )?;
    map.add(node_id, scan_id);
    Ok(())
}

fn build_sub_query_ir<M>(
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    let ast_child_id = node
        .children
        .first()
        .expect("child node id is not found among sub-query children.");
    let plan_child_id = map.get(*ast_child_id)?;
    let plan_sq_id = plan.add_sub_query(plan_child_id, None)?;
    worker.sq_ast_to_plan_id.add(node_id, plan_sq_id);
    map.add(node_id, plan_sq_id);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_group_by_ir<M>(
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    // Reminder: first GroupBy child in `node.children` is always a relational node.
    let mut children: Vec<NodeId> = Vec::with_capacity(node.children.len());
    let first_relational_child_ast_id = node.children.first().expect("GroupBy has no children");
    let first_relational_child_plan_id = map.get(*first_relational_child_ast_id)?;
    children.push(first_relational_child_plan_id);

    worker.inside_grouping_expression = true;
    for ast_column_id in node.children.iter().skip(1) {
        let expr_pair = pairs_map.remove_pair(*ast_column_id);
        let expr_id = parse_scalar_expr(
            Pairs::single(expr_pair),
            type_analyzer,
            DerivedType::unknown(),
            &[first_relational_child_plan_id],
            worker,
            plan,
            false,
        )?;

        children.push(expr_id);
    }
    worker.inside_grouping_expression = false;

    let groupby_id = plan.add_groupby_from_ast(&children)?;
    plan.fix_subquery_rows(worker, groupby_id)?;
    map.add(node_id, groupby_id);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_join_ir<M>(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    // Reminder: Join structure = [left child, kind, right child, condition expr].
    let ast_left_id = node.children.first().expect("Join has no children.");
    let plan_left_id = map.get(*ast_left_id)?;
    let kind = parse_join_kind(ast, node)?;

    let ast_right_id = node
        .children
        .get(2)
        .expect("Right not found among Join children.");
    let plan_right_id = map.get(*ast_right_id)?;

    let condition_expr_id = build_join_condition_ir(
        ast,
        node,
        plan_left_id,
        plan_right_id,
        type_analyzer,
        pairs_map,
        worker,
        plan,
    )?;

    let plan_join_id = plan.add_join(plan_left_id, plan_right_id, condition_expr_id, kind)?;
    plan.fix_subquery_rows(worker, plan_join_id)?;
    map.add(node_id, plan_join_id);
    Ok(())
}

fn parse_join_kind(ast: &AstCore, node: &ParseNode) -> Result<JoinKind, SbroadError> {
    let ast_kind_id = node
        .children
        .get(1)
        .expect("Kind not found among Join children.");
    let ast_kind_node = ast.nodes.get_node(*ast_kind_id)?;
    match ast_kind_node.rule {
        Rule::LeftJoinKind => Ok(JoinKind::LeftOuter),
        Rule::InnerJoinKind => Ok(JoinKind::Inner),
        _ => Err(SbroadError::Invalid(
            Entity::AST,
            Some(format_smolstr!(
                "expected join kind node as 1 child of join. Got: {:?}",
                ast_kind_node,
            )),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_join_condition_ir<M>(
    ast: &AstCore,
    node: &ParseNode,
    plan_left_id: NodeId,
    plan_right_id: NodeId,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<NodeId, SbroadError>
where
    M: Metadata,
{
    let ast_expr_id = node
        .children
        .get(3)
        .expect("Condition not found among Join children");
    let ast_expr = ast.nodes.get_node(*ast_expr_id)?;
    let cond_expr_child_id = ast_expr
        .children
        .first()
        .expect("Expected to see child under Expr node");
    let cond_expr_child = ast.nodes.get_node(*cond_expr_child_id)?;

    if let Rule::True = cond_expr_child.rule {
        return Ok(plan.add_const(Value::Boolean(true)));
    }

    let expr_pair = pairs_map.remove_pair(*ast_expr_id);
    parse_scalar_expr(
        Pairs::single(expr_pair),
        type_analyzer,
        DerivedType::new(UnrestrictedType::Boolean),
        &[plan_left_id, plan_right_id],
        worker,
        plan,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_filter_ir<M>(
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    let ast_rel_child_id = node
        .children
        .first()
        .expect("Selection or Having has no children.");
    let plan_rel_child_id = map.get(*ast_rel_child_id)?;

    let ast_expr_id = node
        .children
        .get(1)
        .expect("Filter not found among Selection children");
    let expr_pair = pairs_map.remove_pair(*ast_expr_id);
    let expr_plan_node_id = parse_scalar_expr(
        Pairs::single(expr_pair),
        type_analyzer,
        DerivedType::new(UnrestrictedType::Boolean),
        &[plan_rel_child_id],
        worker,
        plan,
        false,
    )?;

    let plan_node_id = match &node.rule {
        Rule::Selection => plan.add_select(plan_rel_child_id, expr_plan_node_id)?,
        Rule::Having => plan.add_having(plan_rel_child_id, expr_plan_node_id)?,
        _ => panic!("Expected to see Selection or Having."),
    };
    plan.fix_subquery_rows(worker, plan_node_id)?;
    map.add(node_id, plan_node_id);
    Ok(())
}

fn build_select_with_optional_continuation_ir(
    node_id: usize,
    map: &mut Translation,
    pos_to_ast_id: &mut SelectChildPairTranslation,
    pairs_map: &mut ParsingPairsMap,
    plan: &mut Plan,
) -> Result<(), SbroadError> {
    let select_pair = pairs_map.remove_pair(node_id);
    let select_plan_node_id = parse_select(Pairs::single(select_pair), pos_to_ast_id, map, plan)?;
    map.add(node_id, select_plan_node_id);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_projection_ir<M>(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    let is_without_scan = {
        let child_node = ast.nodes.get_node(node.first_child())?;
        // Check that child is not relational; distinct and asterisk variants are handled
        // in `parse_select_without_scan`.
        matches!(
            child_node.rule,
            Rule::Column | Rule::Asterisk | Rule::Distinct
        )
    };

    if is_without_scan {
        ast.parse_select_without_scan(plan, node_id, type_analyzer, map, pairs_map, worker)
    } else {
        ast.parse_projection(plan, node_id, type_analyzer, map, pairs_map, worker)
    }
}

#[allow(clippy::too_many_arguments)]
fn build_values_ir<M>(
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    let values_rows_ids =
        parse_values_rows(&node.children, type_analyzer, &[], pairs_map, worker, plan)?;
    let plan_values_id = plan.add_values(values_rows_ids)?;
    map.add(node_id, plan_values_id);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_insert_ir<M>(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    let insert_id = parse_insert(node, ast, map, type_analyzer, pairs_map, worker, plan)?;
    map.add(node_id, insert_id);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_update_ir<M>(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    let rel_child_ast_id = node
        .children
        .first()
        .expect("Update must have at least two children.");
    let rel_child_id = map.get(*rel_child_ast_id)?;
    let scan_relation = update_scan_relation(node, map, plan)?;
    let update_list = update_list_node(ast, node)?;
    let update_defs = build_update_defs(
        ast,
        &scan_relation,
        rel_child_id,
        update_list,
        type_analyzer,
        pairs_map,
        worker,
        plan,
    )?;

    let (proj_id, update_id) = plan.add_update(&scan_relation, &update_defs, rel_child_id)?;
    plan.fix_subquery_rows(worker, proj_id)?;
    map.add(node_id, update_id);
    Ok(())
}

fn update_scan_relation(
    node: &ParseNode,
    map: &Translation,
    plan: &Plan,
) -> Result<SmolStr, SbroadError> {
    let ast_scan_table_id = node
        .children
        .get(1)
        .expect("Update must have at least two children.");
    let plan_scan_id = map.get(*ast_scan_table_id)?;
    let plan_scan_node = plan.get_relation_node(plan_scan_id)?;
    if let Relational::ScanRelation(ScanRelation { relation, .. }) = plan_scan_node {
        Ok(relation.clone())
    } else {
        unreachable!("Scan expected under Update")
    }
}

fn update_list_node<'a>(ast: &'a AstCore, node: &ParseNode) -> Result<&'a ParseNode, SbroadError> {
    let update_list_id = node
        .children
        .get(2)
        .expect("Update list expected as a second child of Update");
    ast.nodes.get_node(*update_list_id)
}

#[allow(clippy::too_many_arguments)]
fn build_update_defs<M>(
    ast: &AstCore,
    scan_relation: &str,
    rel_child_id: NodeId,
    update_list: &ParseNode,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<HashMap<ColumnPosition, ExpressionId, RepeatableState>, SbroadError>
where
    M: Metadata,
{
    // Maps position of column in table to corresponding update expression.
    let mut update_defs: HashMap<ColumnPosition, ExpressionId, RepeatableState> =
        HashMap::with_capacity_and_hasher(update_list.children.len(), RepeatableState);

    let relation = plan
        .relations
        .get(scan_relation)
        .ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Table,
                format_smolstr!("{scan_relation} among plan relations"),
            )
        })?
        .clone();
    let columns: HashMap<&str, (&Column, usize)> = relation
        .columns
        .iter()
        .enumerate()
        .map(|(i, column)| (column.name.as_str(), (column, i)))
        .collect();
    let pk_positions: HashSet<usize> = relation.primary_key.positions.iter().copied().collect();

    for update_item_id in &update_list.children {
        let update_item = ast.nodes.get_node(*update_item_id)?;
        let ast_column_id = update_item
            .children
            .first()
            .expect("Column expected as first child of UpdateItem");
        let expr_ast_id = update_item
            .children
            .get(1)
            .expect("Expression expected as second child of UpdateItem");

        let col_name = parse_normalized_identifier(ast, *ast_column_id)?;
        let &(col, pos) = columns
            .get(col_name.as_str())
            .ok_or_else(|| SbroadError::NotFound(Entity::Column, col_name.clone()))?;
        let expr_plan_node_id = parse_update_expression(
            ast,
            *expr_ast_id,
            col_name.as_str(),
            col,
            rel_child_id,
            type_analyzer,
            pairs_map,
            worker,
            plan,
        )?;
        insert_update_def(
            &mut update_defs,
            &pk_positions,
            col,
            pos,
            col_name.as_str(),
            expr_plan_node_id,
        )?;
    }

    Ok(update_defs)
}

#[allow(clippy::too_many_arguments)]
fn parse_update_expression<M>(
    _ast: &AstCore,
    expr_ast_id: usize,
    col_name: &str,
    col: &Column,
    rel_child_id: NodeId,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<ExpressionId, SbroadError>
where
    M: Metadata,
{
    let col_type = col.r#type.get().expect("column type must be known");
    let derived_type = match col_type {
        // Desired type cannot be any, see comment to `Type::Any` in the type system.
        UnrestrictedType::Any => DerivedType::unknown(),
        t => DerivedType::new(t),
    };
    let expr_pair = pairs_map.remove_pair(expr_ast_id);
    let expr_plan_node_id = parse_scalar_expr(
        Pairs::single(expr_pair),
        type_analyzer,
        derived_type,
        &[rel_child_id],
        worker,
        plan,
        true,
    )?;

    let expr_type = type_analyzer.get_report().get_type(&expr_plan_node_id);
    if !can_assign(expr_type.into(), col_type) {
        return Err(SbroadError::Other(format_smolstr!(
            "column {} is of type {}, but expression is of type {}",
            to_user(col_name),
            // Try to keep type formatting sync with other errors.
            sql_type_system::expr::Type::from(col_type),
            expr_type,
        )));
    }

    if plan.contains_aggregates(expr_plan_node_id, true)? {
        return Err(SbroadError::Invalid(
            Entity::Query,
            Some("aggregate functions are not supported in update expression.".into()),
        ));
    }

    Ok(expr_plan_node_id)
}

fn insert_update_def(
    update_defs: &mut HashMap<ColumnPosition, ExpressionId, RepeatableState>,
    pk_positions: &HashSet<usize>,
    col: &Column,
    pos: usize,
    col_name: &str,
    expr_plan_node_id: ExpressionId,
) -> Result<(), SbroadError> {
    match col.get_role() {
        ColumnRole::User => {
            if pk_positions.contains(&pos) {
                return Err(SbroadError::Invalid(
                    Entity::Query,
                    Some(format_smolstr!(
                        "it is illegal to update primary key column: {}",
                        to_user(col_name)
                    )),
                ));
            }
            if update_defs.contains_key(&pos) {
                return Err(SbroadError::Invalid(
                    Entity::Query,
                    Some(format_smolstr!(
                        "The same column is specified twice in update list: {}",
                        to_user(col_name)
                    )),
                ));
            }
            update_defs.insert(pos, expr_plan_node_id);
            Ok(())
        }
        ColumnRole::Sharding => Err(SbroadError::FailedTo(
            Action::Update,
            Some(Entity::Column),
            format_smolstr!("system column {} cannot be updated", to_user(col_name)),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_delete_ir<M>(
    ast: &AstCore,
    metadata: &M,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    let (proj_child_id, table_name) = delete_source_ir(
        ast,
        metadata,
        node,
        map,
        type_analyzer,
        pairs_map,
        worker,
        plan,
    )?;
    let table = metadata.table(&table_name)?;
    let plan_proj_id = build_delete_pk_projection(&table, proj_child_id, plan)?;
    let plan_delete_id = plan.add_delete(table.name, plan_proj_id)?;
    map.add(node_id, plan_delete_id);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn delete_source_ir<M>(
    ast: &AstCore,
    metadata: &M,
    node: &ParseNode,
    map: &Translation,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(Option<NodeId>, SmolStr), SbroadError>
where
    M: Metadata,
{
    // Reminder: first child of Delete is a `ScanTable` or `DeleteFilter`
    // under which there must be a `ScanTable`.
    let first_child_id = node
        .children
        .first()
        .expect("Delete must have at least one child");
    let first_child_node = ast.nodes.get_node(*first_child_id)?;

    match first_child_node.rule {
        Rule::ScanTable => delete_scan_source_ir(ast, metadata, node, *first_child_id, map, plan),
        Rule::DeleteFilter => delete_filter_source_ir(
            ast,
            metadata,
            node,
            first_child_node,
            map,
            type_analyzer,
            pairs_map,
            worker,
            plan,
        ),
        _ => Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!(
                "AST delete node {:?} contains unexpected children",
                first_child_node,
            )),
        )),
    }
}

fn delete_scan_source_ir<M>(
    ast: &AstCore,
    metadata: &M,
    delete_node: &ParseNode,
    scan_ast_id: usize,
    map: &Translation,
    plan: &mut Plan,
) -> Result<(Option<NodeId>, SmolStr), SbroadError>
where
    M: Metadata,
{
    let plan_scan_id = map.get(scan_ast_id)?;
    let plan_scan_node = plan.get_relation_node(plan_scan_id)?;
    let Relational::ScanRelation(ScanRelation { relation, .. }) = plan_scan_node else {
        unreachable!("Scan expected under ScanTable")
    };
    let relation_name = relation.clone();

    if let Some(indexed_by_id) = delete_node.children.get(1) {
        let index_name =
            parse_indexed_by_expr(ast, *indexed_by_id)?.expect("INDEXED BY must exist");
        let index_id = metadata.get_index_id(&index_name, &relation_name)?;
        let table = metadata.table(&relation_name)?;
        plan.index_version_map.insert([table.id, index_id], 0);
    }

    Ok((None, relation_name))
}

#[allow(clippy::too_many_arguments)]
fn delete_filter_source_ir<M>(
    ast: &AstCore,
    metadata: &M,
    delete_node: &ParseNode,
    filter_node: &ParseNode,
    map: &Translation,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(Option<NodeId>, SmolStr), SbroadError>
where
    M: Metadata,
{
    let ast_table_id = filter_node
        .children
        .first()
        .expect("Table not found among DeleteFilter children");
    let plan_scan_id = map.get(*ast_table_id)?;
    let ast_expr_id = filter_node
        .children
        .get(1)
        .expect("Expr not found among DeleteFilter children");
    let expr_pair = pairs_map.remove_pair(*ast_expr_id);
    let expr_plan_node_id = parse_scalar_expr(
        Pairs::single(expr_pair),
        type_analyzer,
        DerivedType::new(UnrestrictedType::Boolean),
        &[plan_scan_id],
        worker,
        plan,
        false,
    )?;

    let plan_select_id = plan.add_select(plan_scan_id, expr_plan_node_id)?;
    plan.fix_subquery_rows(worker, plan_select_id)?;

    let relation = if let Some(indexed_by_id) = delete_node.children.get(1) {
        let index_name =
            parse_indexed_by_expr(ast, *indexed_by_id)?.expect("INDEXED BY must exist");
        set_delete_filter_indexed_by(metadata, plan, plan_scan_id, index_name)?
    } else {
        scan_relation_name(plan, plan_scan_id)?
    };

    Ok((Some(plan_select_id), relation))
}

fn set_delete_filter_indexed_by<M>(
    metadata: &M,
    plan: &mut Plan,
    plan_scan_id: NodeId,
    index_name: SmolStr,
) -> Result<SmolStr, SbroadError>
where
    M: Metadata,
{
    let (relation_name, table_id, index_id) = {
        let MutRelational::ScanRelation(ScanRelation {
            relation,
            indexed_by,
            ..
        }) = plan.get_mut_relation_node(plan_scan_id)?
        else {
            unreachable!("Scan expected under ScanTable")
        };
        let index_id = metadata.get_index_id(&index_name, relation)?;
        let table = metadata.table(relation)?;
        *indexed_by = Some(index_name.clone());
        (relation.clone(), table.id, index_id)
    };

    let index = Index::from(table_id, index_id);
    plan.indexes.insert(index_name, index);
    plan.index_version_map.insert([table_id, index_id], 0);
    Ok(relation_name)
}

fn scan_relation_name(plan: &Plan, plan_scan_id: NodeId) -> Result<SmolStr, SbroadError> {
    let Relational::ScanRelation(ScanRelation { relation, .. }) =
        plan.get_relation_node(plan_scan_id)?
    else {
        unreachable!("Scan expected under ScanTable")
    };
    Ok(relation.clone())
}

fn build_delete_pk_projection(
    table: &Table,
    proj_child_id: Option<NodeId>,
    plan: &mut Plan,
) -> Result<Option<NodeId>, SbroadError> {
    let Some(proj_child_id) = proj_child_id else {
        return Ok(None);
    };

    // The projection in the delete operator contains only the primary key columns.
    let mut pk_columns = Vec::with_capacity(table.primary_key.positions.len());
    for pos in &table.primary_key.positions {
        let column: &Column = table.columns.get(*pos).ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Table,
                Some(format_smolstr!(
                    "{} {} {pos}",
                    "primary key refers to non-existing column",
                    "at position",
                )),
            )
        })?;
        let col_with_scan = ColumnWithScan::new(column.name.as_str(), None);
        pk_columns.push(col_with_scan);
    }
    let pk_column_ids = plan.new_columns(
        &NewColumnsSource::Other {
            child: proj_child_id,
            columns_spec: Some(ColumnsRetrievalSpec::Names(pk_columns)),
            asterisk_source: None,
        },
        false,
        true,
    )?;
    let mut alias_ids = Vec::with_capacity(pk_column_ids.len());
    for (pk_pos, pk_column_id) in pk_column_ids.iter().enumerate() {
        let pk_alias_id = plan
            .nodes
            .add_alias(&format!("pk_col_{pk_pos}"), *pk_column_id)?;
        alias_ids.push(pk_alias_id);
    }

    Ok(Some(plan.add_proj_internal(
        proj_child_id,
        &alias_ids,
        false,
        vec![],
    )?))
}

#[allow(clippy::too_many_arguments)]
fn build_alter_system_ir<M>(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    let alter_system = parse_alter_system(ast, node, type_analyzer, pairs_map, worker, plan)?;
    push_mapped_plan_node(plan, map, node_id, alter_system);
    Ok(())
}

fn build_grant_privilege_ir(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    plan: &mut Plan,
) -> Result<(), SbroadError> {
    let (grant_type, grantee_name, wait_applied_globally, timeout) = parse_grant_revoke(ast, node)?;
    let grant_privilege = GrantPrivilege {
        grant_type,
        grantee_name,
        wait_applied_globally,
        timeout,
    };
    push_mapped_plan_node(plan, map, node_id, grant_privilege);
    Ok(())
}

fn build_revoke_privilege_ir(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    plan: &mut Plan,
) -> Result<(), SbroadError> {
    let (revoke_type, grantee_name, wait_applied_globally, timeout) =
        parse_grant_revoke(ast, node)?;
    let revoke_privilege = RevokePrivilege {
        revoke_type,
        grantee_name,
        wait_applied_globally,
        timeout,
    };
    push_mapped_plan_node(plan, map, node_id, revoke_privilege);
    Ok(())
}

fn build_drop_role_ir(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    plan: &mut Plan,
) -> Result<(), SbroadError> {
    let (name, if_exists, wait_applied_globally, timeout) =
        parse_acl_drop_options(ast, node, "RoleName expected under DropRole node")?;
    push_mapped_plan_node(
        plan,
        map,
        node_id,
        DropRole {
            name,
            if_exists,
            wait_applied_globally,
            timeout,
        },
    );
    Ok(())
}

fn build_drop_user_ir(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    plan: &mut Plan,
) -> Result<(), SbroadError> {
    let (name, if_exists, wait_applied_globally, timeout) =
        parse_acl_drop_options(ast, node, "RoleName expected under DropUser node")?;
    push_mapped_plan_node(
        plan,
        map,
        node_id,
        DropUser {
            name,
            if_exists,
            wait_applied_globally,
            timeout,
        },
    );
    Ok(())
}

fn parse_acl_drop_options(
    ast: &AstCore,
    node: &ParseNode,
    missing_name_msg: &'static str,
) -> Result<(SmolStr, bool, bool, Timeout), SbroadError> {
    let mut name = None;
    let mut timeout = get_default_timeout();
    let mut if_exists = DEFAULT_IF_EXISTS;
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;

    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Identifier => name = Some(parse_identifier(ast, *child_id)?),
            Rule::IfExists => if_exists = true,
            Rule::WaitAppliedGlobally => wait_applied_globally = true,
            Rule::WaitAppliedLocally => wait_applied_globally = false,
            Rule::Timeout => timeout = get_timeout(ast, *child_id)?,
            _ => return unexpected_acl_child(child_node),
        }
    }

    Ok((
        name.expect(missing_name_msg),
        if_exists,
        wait_applied_globally,
        timeout,
    ))
}

fn build_create_role_ir(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    plan: &mut Plan,
) -> Result<(), SbroadError> {
    let (name, if_not_exists, wait_applied_globally, timeout) =
        parse_create_role_options(ast, node)?;
    push_mapped_plan_node(
        plan,
        map,
        node_id,
        CreateRole {
            name,
            if_not_exists,
            wait_applied_globally,
            timeout,
        },
    );
    Ok(())
}

fn parse_create_role_options(
    ast: &AstCore,
    node: &ParseNode,
) -> Result<(SmolStr, bool, bool, Timeout), SbroadError> {
    let mut name = None;
    let mut if_not_exists = DEFAULT_IF_NOT_EXISTS;
    let mut timeout = get_default_timeout();
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;

    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Identifier => name = Some(parse_identifier(ast, *child_id)?),
            Rule::IfNotExists => if_not_exists = true,
            Rule::WaitAppliedGlobally => wait_applied_globally = true,
            Rule::WaitAppliedLocally => wait_applied_globally = false,
            Rule::Timeout => timeout = get_timeout(ast, *child_id)?,
            _ => return unexpected_acl_child(child_node),
        }
    }

    Ok((
        name.expect("RoleName expected under CreateRole node"),
        if_not_exists,
        wait_applied_globally,
        timeout,
    ))
}

fn build_create_user_ir(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    plan: &mut Plan,
) -> Result<(), SbroadError> {
    let create_user = parse_create_user_ir(ast, node)?;
    push_mapped_plan_node(plan, map, node_id, create_user);
    Ok(())
}

fn parse_create_user_ir(ast: &AstCore, node: &ParseNode) -> Result<CreateUser, SbroadError> {
    let mut name = None;
    let mut password = None;
    let mut if_not_exists = DEFAULT_IF_NOT_EXISTS;
    let mut timeout = get_default_timeout();
    let mut auth_method = DEFAULT_AUTH_METHOD;
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;

    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Identifier => name = Some(parse_identifier(ast, *child_id)?),
            Rule::SingleQuotedString => {
                let password_literal: SmolStr = retrieve_string_literal(ast, *child_id)?;
                password = Some(escape_single_quotes(&password_literal));
            }
            Rule::IfNotExists => if_not_exists = true,
            Rule::WaitAppliedGlobally => wait_applied_globally = true,
            Rule::WaitAppliedLocally => wait_applied_globally = false,
            Rule::Timeout => timeout = get_timeout(ast, *child_id)?,
            method @ (Rule::ScramSha256 | Rule::ChapSha1 | Rule::Md5 | Rule::Ldap) => {
                auth_method = auth_method_from_auth_rule(method);
            }
            _ => return unexpected_acl_child(child_node),
        }
    }

    Ok(CreateUser {
        name: name.expect("username expected as a child"),
        password: password.unwrap_or_default(),
        if_not_exists,
        auth_method,
        wait_applied_globally,
        timeout,
    })
}

fn build_alter_user_ir(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    plan: &mut Plan,
) -> Result<(), SbroadError> {
    let alter_user = parse_alter_user_ir(ast, node)?;
    push_mapped_plan_node(plan, map, node_id, alter_user);
    Ok(())
}

fn parse_alter_user_ir(ast: &AstCore, node: &ParseNode) -> Result<AlterUser, SbroadError> {
    let user_name_node_id = node
        .children
        .first()
        .expect("RoleName expected as a first child");
    let user_name = parse_identifier(ast, *user_name_node_id)?;

    let alter_option_node_id = node
        .children
        .get(1)
        .expect("Some AlterOption expected as a second child");
    let alter_option_node = ast.nodes.get_node(*alter_option_node_id)?;
    let alter_option = parse_alter_user_option(ast, alter_option_node)?;
    let (wait_applied_globally, timeout) =
        parse_acl_wait_timeout_options(ast, node.children.get(2..).unwrap_or(&[]))?;

    Ok(AlterUser {
        name: user_name,
        alter_option,
        wait_applied_globally,
        timeout,
    })
}

fn parse_alter_user_option(
    ast: &AstCore,
    alter_option_node: &ParseNode,
) -> Result<AlterOption, SbroadError> {
    match alter_option_node.rule {
        Rule::AlterLogin => Ok(AlterOption::Login),
        Rule::AlterNoLogin => Ok(AlterOption::NoLogin),
        Rule::AlterPassword => parse_alter_password_option(ast, alter_option_node),
        Rule::AlterRename => {
            let identifier_node_id = alter_option_node
                .children
                .first()
                .expect("Expected to see an identifier node under AlterRename");
            let identifier = parse_identifier(ast, *identifier_node_id)?;
            Ok(AlterOption::Rename {
                new_name: identifier,
            })
        }
        _ => Err(SbroadError::Invalid(
            Entity::ParseNode,
            Some(SmolStr::from("Expected to see concrete alter option")),
        )),
    }
}

fn parse_alter_password_option(
    ast: &AstCore,
    alter_option_node: &ParseNode,
) -> Result<AlterOption, SbroadError> {
    let pwd_or_ldap_node_id = alter_option_node
        .children
        .first()
        .expect("Password expected as a first child");
    let pwd_or_ldap_node = ast.nodes.get_node(*pwd_or_ldap_node_id)?;

    let (password, auth_method) = match pwd_or_ldap_node.rule {
        Rule::Ldap => (SmolStr::default(), AuthMethod::Ldap),
        Rule::SingleQuotedString => {
            let password_literal = retrieve_string_literal(ast, *pwd_or_ldap_node_id)?;
            let password = escape_single_quotes(&password_literal);
            let auth_method =
                parse_alter_password_auth_method(ast, alter_option_node, pwd_or_ldap_node)?;
            (password, auth_method)
        }
        _ => return unexpected_acl_child(pwd_or_ldap_node),
    };

    Ok(AlterOption::Password {
        password,
        auth_method,
    })
}

fn parse_alter_password_auth_method(
    ast: &AstCore,
    alter_option_node: &ParseNode,
    pwd_or_ldap_node: &ParseNode,
) -> Result<AuthMethod, SbroadError> {
    let Some(auth_method_node_id) = alter_option_node.children.get(1) else {
        return Ok(DEFAULT_AUTH_METHOD);
    };

    let auth_method_node = ast.nodes.get_node(*auth_method_node_id)?;
    match auth_method_node.rule {
        method @ (Rule::ScramSha256 | Rule::ChapSha1 | Rule::Md5) => {
            Ok(auth_method_from_auth_rule(method))
        }
        _ => unexpected_acl_child(pwd_or_ldap_node),
    }
}

fn parse_acl_wait_timeout_options(
    ast: &AstCore,
    child_ids: &[usize],
) -> Result<(bool, Timeout), SbroadError> {
    let mut timeout = get_default_timeout();
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;

    for child_id in child_ids {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::WaitAppliedGlobally => wait_applied_globally = true,
            Rule::WaitAppliedLocally => wait_applied_globally = false,
            Rule::Timeout => timeout = get_timeout(ast, *child_id)?,
            _ => {}
        }
    }

    Ok((wait_applied_globally, timeout))
}

fn unexpected_acl_child<T>(child_node: &ParseNode) -> Result<T, SbroadError> {
    Err(SbroadError::Invalid(
        Entity::Node,
        Some(format_smolstr!(
            "ACL node contains unexpected child: {child_node:?}",
        )),
    ))
}

fn build_explain_ir(
    ast: &AstCore,
    node: &ParseNode,
    map: &mut Translation,
    plan: &mut Plan,
) -> Result<(), SbroadError> {
    let mut child_iter = node.children.iter();
    let mut explain_child_id = child_iter.next().expect("explain has no children");
    let explain_child = ast.nodes.get_node(*explain_child_id)?;

    let mut explain_options = ExplainOptions::empty();
    if let Rule::ExplainQueryPlan = explain_child.rule {
        explain_options = parse_explain_query_plan_options(ast, explain_child)?;
        explain_child_id = child_iter.next().expect("explain has no children");
    }

    // Select default facets if there's none.
    if !explain_options.has_facet() {
        explain_options |= ExplainOptions::Logical | ExplainOptions::Buckets;
    }
    plan.explain_options = explain_options;

    map.add(0, map.get(*explain_child_id)?);
    Ok(())
}

fn parse_explain_query_plan_options(
    ast: &AstCore,
    explain_child: &ParseNode,
) -> Result<ExplainOptions, SbroadError> {
    let mut explain_options = ExplainOptions::empty();
    for child in &explain_child.children {
        let explain_option_node = ast.nodes.get_node(*child)?;
        match explain_option_node.rule {
            Rule::ExplainRaw => explain_options |= ExplainOptions::Raw,
            Rule::ExplainFmt => explain_options |= ExplainOptions::Fmt,
            Rule::ExplainBuckets => explain_options |= ExplainOptions::Buckets,
            Rule::ExplainLogical => explain_options |= ExplainOptions::Logical,
            Rule::ExplainForward => explain_options |= ExplainOptions::Forward,
            Rule::ExplainContext => explain_options |= ExplainOptions::Context,
            _ => panic!(
                "unknown explain option rule: {:?}",
                explain_option_node.rule
            ),
        };
    }
    Ok(explain_options)
}

#[allow(clippy::too_many_arguments)]
fn build_option_ir<M>(
    ast: &AstCore,
    node: &ParseNode,
    kind: OptionKind,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    let ast_child_id = node.children.first().expect("no children for query option");
    let val = match kind {
        OptionKind::MotionRowMax | OptionKind::VdbeOpcodeMax => {
            parse_option(ast, type_analyzer, *ast_child_id, pairs_map, worker, plan)?
        }
        OptionKind::ReadPreference => parse_read_preference_option(
            ast,
            type_analyzer,
            *ast_child_id,
            pairs_map,
            worker,
            plan,
        )?,
        OptionKind::Forward => {
            parse_forward_option(ast, type_analyzer, *ast_child_id, pairs_map, worker, plan)?
        }
    };
    plan.raw_options.push(OptionSpec { kind, val });
    Ok(())
}

fn map_first_child(
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    missing_child_msg: &'static str,
) -> Result<(), SbroadError> {
    let child_id = map.get(*node.children.first().expect(missing_child_msg))?;
    map.add(node_id, child_id);
    Ok(())
}

fn build_block_let_statement_ir<M>(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    let var_name_ast_id = node
        .children
        .first()
        .expect("BlockLetStatement must have an Identifier child");
    let var_name = parse_identifier(ast, *var_name_ast_id)?;
    let subquery_ast_id = node
        .children
        .get(1)
        .expect("BlockLetStatement must have a SubQuery child");
    let subquery_plan_id = map.get(*subquery_ast_id)?;
    let rhs = plan.get_rel_child(subquery_plan_id, 0)?;

    // LET requires a single-column RHS; multi-row handling is a runtime concern.
    let columns = dql_return_columns(plan, subquery_plan_id)?;
    if columns.len() != 1 {
        return Err(SbroadError::Other(format_smolstr!(
            "LET RHS must be a single-column query, got {} columns",
            columns.len()
        )));
    }
    let var_type = columns[0].1;

    worker.let_scope.declare(rhs, var_name.clone(), var_type)?;
    worker.let_var_names.insert(node_id, var_name);
    map.add(node_id, subquery_plan_id);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_block_if_condition_ir<M>(
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    // Wrap the condition expression in `SELECT (<expr>) AS "cond"`.
    let expr_ast_id = node
        .children
        .first()
        .expect("BlockIfCondition must have an Expr child");
    let expr_pair = pairs_map.remove_pair(*expr_ast_id);
    let expr_plan_id = parse_scalar_expr(
        Pairs::single(expr_pair),
        type_analyzer,
        DerivedType::new(UnrestrictedType::Boolean),
        &[],
        worker,
        plan,
        false,
    )?;
    let alias_id = plan.nodes.add_alias("cond", expr_plan_id)?;
    let projection_id = plan.add_select_without_scan(&[alias_id])?;
    plan.fix_subquery_rows(worker, projection_id)?;
    map.add(node_id, projection_id);
    Ok(())
}

fn build_block_if_body_statement_ir(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
) -> Result<(), SbroadError> {
    let inner_ast_id = node
        .children
        .first()
        .expect("BlockIfBodyStatement must have a child");
    let inner_rule = ast.nodes.get_node(*inner_ast_id)?.rule;
    if inner_rule != Rule::BlockIfStatement {
        let inner_plan_id = map.get(*inner_ast_id)?;
        map.add(node_id, inner_plan_id);
    }
    Ok(())
}

fn build_anonymous_block_ir<M>(
    ast: &AstCore,
    node_id: usize,
    map: &mut Translation,
    type_analyzer: &mut type_system::TypeAnalyzer,
    worker: &ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    // Set parameter types before `parse_anonymous_block` reads block metadata.
    let param_types = get_parameter_derived_types(type_analyzer);
    plan.set_types_in_parameter_nodes(&param_types)?;

    let block = parse_anonymous_block(ast, node_id, map, plan, worker)?;
    push_mapped_plan_node(plan, map, node_id, block);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_call_proc_ir<M>(
    ast: &AstCore,
    node_id: usize,
    node: &ParseNode,
    map: &mut Translation,
    type_analyzer: &mut type_system::TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionWalker<M>,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    let call_proc = parse_call_proc(ast, node, type_analyzer, pairs_map, worker, plan)?;
    push_mapped_plan_node(plan, map, node_id, call_proc);
    Ok(())
}

/// Parse node is a wrapper over the pest pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseNode {
    pub(in crate::frontend::sql) children: Vec<usize>,
    pub(in crate::frontend::sql) rule: Rule,
    pub(in crate::frontend::sql) value: Option<SmolStr>,
}

#[allow(dead_code)]
impl ParseNode {
    pub(super) fn new(rule: Rule, value: Option<SmolStr>) -> Self {
        ParseNode {
            children: vec![],
            rule,
            value,
        }
    }

    /// Return first child from node children.
    ///
    /// # Panics
    ///
    /// Panics a children array is empty.
    pub(super) fn first_child(&self) -> usize {
        *self
            .children
            .first()
            .expect("could not find first child in node")
    }

    /// Return a nth child from node children.
    ///
    /// # Panics
    ///
    /// Panics if there is no n-child in a children array.
    pub(super) fn child_n(&self, n: usize) -> usize {
        *self
            .children
            .get(n)
            .unwrap_or_else(|| panic!("could find {n} child in node"))
    }
}

/// A storage arena of the parse nodes
/// (a node position in the arena vector acts like a reference).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseNodes {
    pub(crate) arena: Vec<ParseNode>,
}

impl Default for ParseNodes {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl ParseNodes {
    /// Get a node from arena
    ///
    /// # Errors
    /// - Failed to get a node from arena.
    pub fn get_node(&self, node: usize) -> Result<&ParseNode, SbroadError> {
        self.arena.get(node).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("from arena with index {node}"),
            )
        })
    }

    /// Get a mutable node from arena
    ///
    /// # Errors
    /// - Failed to get a node from arena.
    pub fn get_mut_node(&mut self, node: usize) -> Result<&mut ParseNode, SbroadError> {
        self.arena.get_mut(node).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("(mutable) from arena with index {node}"),
            )
        })
    }

    /// Push a new node to arena
    pub fn push_node(&mut self, node: ParseNode) -> usize {
        let id = self.next_id();
        self.arena.push(node);
        id
    }

    /// Push `child_id` to the front of `node_id` children
    ///
    /// # Errors
    /// - Failed to get node from arena
    pub fn push_front_child(&mut self, node_id: usize, child_id: usize) -> Result<(), SbroadError> {
        let node = self.get_mut_node(node_id)?;
        node.children.insert(0, child_id);
        Ok(())
    }

    pub fn push_child_1(&mut self, node_id: usize, child_id: usize) -> Result<(), SbroadError> {
        let node = self.get_mut_node(node_id)?;
        node.children.insert(1, child_id);
        Ok(())
    }

    /// Push `child_id` to the back of `node_id` children
    ///
    /// # Errors
    /// - Failed to get node from arena
    pub fn push_back_child(&mut self, node_id: usize, child_id: usize) -> Result<(), SbroadError> {
        let node = self.get_mut_node(node_id)?;
        node.children.push(child_id);
        Ok(())
    }

    /// Sets node children to given children
    ///
    /// # Errors
    /// - failed to get node from arena
    pub fn set_children(
        &mut self,
        node_id: usize,
        new_children: Vec<usize>,
    ) -> Result<(), SbroadError> {
        let node = self.get_mut_node(node_id)?;
        node.children = new_children;
        Ok(())
    }

    /// Get next node id
    #[must_use]
    pub fn next_id(&self) -> usize {
        self.arena.len()
    }

    /// Constructor
    #[must_use]
    pub fn new() -> Self {
        ParseNodes { arena: Vec::new() }
    }

    /// Adds children to already existing node.
    /// New elements are added to the beginning of the current list
    /// as we use inverted node order.
    ///
    /// # Errors
    /// - Failed to retrieve node from arena.
    pub fn add_child(&mut self, node: Option<usize>, child: usize) -> Result<(), SbroadError> {
        if let Some(parent) = node {
            self.get_node(child)?;
            let parent_node = self.arena.get_mut(parent).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Node,
                    format_smolstr!("(mutable) from arena with index {parent}"),
                )
            })?;
            parent_node.children.insert(0, child);
        }
        Ok(())
    }

    /// Update node's value (string from pairs)
    ///
    /// # Errors
    /// - Target node is present in the arena.
    pub fn update_value(&mut self, node: usize, value: Option<SmolStr>) -> Result<(), SbroadError> {
        let node = self.arena.get_mut(node).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("(mutable) from arena with index {node}"),
            )
        })?;
        node.value = value;
        Ok(())
    }
}

/// A wrapper over the pair to keep its parent as well.
pub(super) struct StackParseNode<'n> {
    pub(super) arena_parent_id: Option<usize>,
    pub(super) pair: Pair<'n, Rule>,
}

impl<'n> StackParseNode<'n> {
    /// Constructor
    pub(super) fn new(pair: Pair<'n, Rule>, parent_id: Option<usize>) -> Self {
        StackParseNode {
            arena_parent_id: parent_id,
            pair,
        }
    }
}

/// Map of { `AbstractSyntaxTree` node id -> parsing pairs copy, corresponding to ast node }.
#[derive(Clone)]
pub(super) struct ParsingPairsMap<'q> {
    inner: HashMap<usize, Pair<'q, Rule>>,
}

impl<'q> ParsingPairsMap<'q> {
    pub(in crate::frontend::sql) fn new() -> Self {
        Self {
            inner: HashMap::with_capacity(PARSING_PAIRS_MAP_CAPACITY),
        }
    }

    pub(super) fn insert(&mut self, key: usize, value: Pair<'q, Rule>) -> Option<Pair<'q, Rule>> {
        self.inner.insert(key, value)
    }

    pub(super) fn remove_pair(&mut self, key: usize) -> Pair<'_, Rule> {
        self.inner
            .remove(&key)
            .expect("pairs_map doesn't contain value for key")
    }
}

// Mapping between pest's Pair and corresponding id
// of the ast node. This map stores only ids for
// possible select child (Projection, OrderBy)
pub(super) type SelectChildPairTranslation = HashMap<(usize, usize), usize>;

/// Hash map of { pair -> ast_id }, where
/// * pair (key) -- inner pest structure for AST node
/// * ast_id (value) -- id of our AST node.
pub(super) type PairToAstIdTranslation<'i> = HashMap<Pair<'i, Rule>, usize>;

/// AST is a tree build on the top of the parse nodes arena.
// #[derive(Clone, Debug)]
pub struct AbstractSyntaxTree<'q> {
    core: AstCore,
    meta: AbstractSyntaxTreeMeta<'q>,
}

struct AbstractSyntaxTreeMeta<'q> {
    pairs_map: ParsingPairsMap<'q>,
    pos_to_ast_id: SelectChildPairTranslation,
    pairs_to_ast_id: PairToAstIdTranslation<'q>,
    tnt_parameters_ordered: Vec<Pair<'q, Rule>>,
}

impl AbstractSyntaxTreeMeta<'_> {
    fn empty() -> Self {
        Self {
            pairs_map: ParsingPairsMap::new(),
            pos_to_ast_id: HashMap::new(),
            pairs_to_ast_id: HashMap::new(),
            tnt_parameters_ordered: Vec::new(),
        }
    }
}

#[allow(dead_code)]
impl<'q> AbstractSyntaxTree<'q> {
    fn set_core(&mut self, core: AstCore) {
        self.core = core
    }

    fn set_metadata(&mut self, meta: AbstractSyntaxTreeMeta<'q>) {
        self.meta = meta
    }

    /// Build an empty AST.
    pub fn empty() -> Self {
        Self {
            core: AstCore::empty(),
            meta: AbstractSyntaxTreeMeta::empty(),
        }
    }

    #[cfg(test)]
    pub(in crate::frontend::sql) fn fill<'query>(
        &mut self,
        query: &'query str,
        pairs_map: &mut ParsingPairsMap<'query>,
        pos_to_ast_id: &mut SelectChildPairTranslation,
        pair_to_ast_id: &mut PairToAstIdTranslation<'query>,
        tnt_parameters_ordered: &mut Vec<Pair<'query, Rule>>,
    ) -> Result<(), SbroadError> {
        self.core.fill(
            query,
            pairs_map,
            pos_to_ast_id,
            pair_to_ast_id,
            tnt_parameters_ordered,
        )
    }

    fn is_empty(&self) -> bool {
        self.core.is_empty()
    }
}

// Helper map to store CTE node ids by their names.
type CTEs = AHashMap<SmolStr, NodeId>;

/// Parse single quote string handling escaping.
/// For input string of "a''b''c" we should get "a'b'c" result.
/// Each two subsequent single quotes are treated as one.
pub(super) fn escape_single_quotes(raw: &str) -> SmolStr {
    raw.replace("''", "'").to_smolstr()
}

use crate::ir::options::Timeout;

fn get_default_timeout() -> Timeout {
    Timeout::default_ddl()
}

/// Matches appropriate [`tarantool::auth::AuthMethod`] with passed `Rule`.
/// Panics as unreachable code if no appropriate method was found.
#[inline(always)]
fn auth_method_from_auth_rule(auth_rule: Rule) -> AuthMethod {
    match auth_rule {
        Rule::ScramSha256 => AuthMethod::ScramSha256,
        Rule::ChapSha1 => AuthMethod::ChapSha1,
        Rule::Ldap => AuthMethod::Ldap,
        Rule::Md5 => AuthMethod::Md5,
        _ => unreachable!("got a non-auth parsing rule"),
    }
}
