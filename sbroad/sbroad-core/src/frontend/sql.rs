//! SQL frontend module.
//!
//! Parses an SQL statement to the abstract syntax tree (AST)
//! and builds the intermediate representation (IR).

use crate::ir::node::ddl::DdlOwned;
use crate::ir::node::deallocate::Deallocate;
use crate::ir::node::tcl::Tcl;
use crate::ir::node::{
    Alias, AlterTable, AlterTableOp, Bound, BoundType, Frame, FrameType, GroupBy, LocalTimestamp,
    NamedWindows, Over, Reference, ReferenceAsteriskSource, ScalarFunction, TruncateTable, Window,
};
use crate::ir::relation::Type;
use ahash::{AHashMap, AHashSet};
use core::panic;
use itertools::Itertools;
use pest::iterators::{Pair, Pairs};
use pest::pratt_parser::PrattParser;
use pest::Parser;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::collections::VecDeque;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};
use tarantool::datetime::Datetime;
use tarantool::index::{IndexType, RtreeIndexDistanceType};
use time::{OffsetDateTime, Time};

use crate::errors::Entity::AST;
use crate::errors::{Action, Entity, SbroadError};
use crate::executor::engine::helpers::{normalize_name_from_sql, to_user};
use crate::executor::engine::Metadata;
use crate::frontend::sql::ast::{
    AbstractSyntaxTree, ParseNode, ParseNodes, ParseTree, Rule, StackParseNode,
};
use crate::frontend::sql::ir::SubtreeCloner;
use crate::frontend::sql::ir::Translation;
use crate::frontend::Ast;
use crate::ir::acl::AlterOption;
use crate::ir::acl::{GrantRevokeType, Privilege};
use crate::ir::aggregates::AggregateKind;
use crate::ir::ddl::{AlterSystemType, ColumnDef, SetParamScopeType, SetParamValue};
use crate::ir::ddl::{Language, ParamDef};
use crate::ir::expression::cast::Type as CastType;
use crate::ir::expression::{
    ColumnPositionMap, ColumnWithScan, ColumnsRetrievalSpec, ExpressionId, FunctionFeature,
    Position, TrimKind, VolatilityType,
};
use crate::ir::expression::{NewColumnsSource, Substring};
use crate::ir::helpers::RepeatableState;
use crate::ir::node::expression::{Expression, MutExpression};
use crate::ir::node::plugin::{
    AppendServiceToTier, ChangeConfig, CreatePlugin, DisablePlugin, DropPlugin, EnablePlugin,
    MigrateTo, MigrateToOpts, RemoveServiceFromTier, ServiceSettings, SettingsPair,
};
use crate::ir::node::relational::{MutRelational, Relational};
use crate::ir::node::{
    AlterSystem, AlterUser, BoolExpr, Constant, CountAsterisk, CreateIndex, CreateProc, CreateRole,
    CreateTable, CreateUser, DropIndex, DropProc, DropRole, DropTable, DropUser, GrantPrivilege,
    Node, NodeId, Procedure, RenameRoutine, RevokePrivilege, ScanCte, ScanRelation, SetParam,
    SetTransaction, Trim,
};
use crate::ir::operator::{
    Arithmetic, Bool, ConflictStrategy, JoinKind, OrderByElement, OrderByEntity, OrderByType, Unary,
};
use crate::ir::relation::{Column, ColumnRole, TableKind, Type as RelationType};
use crate::ir::transformation::redistribution::ColumnPosition;
use crate::ir::tree::traversal::{
    LevelNode, PostOrder, PostOrderWithFilter, EXPR_CAPACITY, REL_CAPACITY,
};
use crate::ir::value::Value;
use crate::ir::{node::plugin, OptionKind, OptionParamValue, OptionSpec, Plan};
use crate::warn;
use tarantool::auth::AuthMethod;
use tarantool::decimal::Decimal;
use tarantool::space::SpaceEngineType;
use type_system::TypeAnalyzer;

// DDL timeout in seconds (1 day).
const DEFAULT_TIMEOUT_F64: f64 = 24.0 * 60.0 * 60.0;
// TODO: implement and use `AuthMethod::DEFAULT`
// when tarantool-module gets Md5 as default one
const DEFAULT_AUTH_METHOD: AuthMethod = AuthMethod::Md5;

const DEFAULT_IF_EXISTS: bool = false;
const DEFAULT_IF_NOT_EXISTS: bool = false;

const DEFAULT_WAIT_APPLIED_GLOBALLY: bool = true;

fn get_default_timeout() -> Decimal {
    Decimal::from_str(&format!("{DEFAULT_TIMEOUT_F64}")).expect("default timeout casting failed")
}

/// Matches appropriate [`tarantool::auth::AuthMethod`] with passed `Rule`.
/// Panics as unreachable code if no appropriate method was found.
#[inline(always)]
fn auth_method_from_auth_rule(auth_rule: Rule) -> AuthMethod {
    match auth_rule {
        Rule::ChapSha1 => AuthMethod::ChapSha1,
        Rule::Ldap => AuthMethod::Ldap,
        Rule::Md5 => AuthMethod::Md5,
        _ => unreachable!("got a non-auth parsing rule"),
    }
}

const NUM_OF_VOLATILE_FUNCTIONS: usize = 1;

/// Names used by the user in SQL.
static USER_FACING_NAMES_OF_FUNCTIONS: [&str; NUM_OF_VOLATILE_FUNCTIONS] = ["instance_uuid"];

/// Names of functions in picodata .rs files, which annotated with `#[tarantool::proc]` and exposed to SQL.
pub static NAMES_OF_FUNCTIONS_IN_SOURCES: [&str; NUM_OF_VOLATILE_FUNCTIONS] =
    ["proc_instance_uuid"];

/// '.' + name in sources. Plain name in sources (which is exported by picodata) doesn't work, because
/// when tarantool executing box.func['proc_name']:call(..) (box.execute("select proc_name()")
/// it try to find dynlib under the 'libproc_name' name with defined `proc_name` in it.
/// With . as first symbol tarantool try to find proc symbol in current executable.
static NAMES_OF_FUNCTIONS_IN_TARANTOOL: [&str; NUM_OF_VOLATILE_FUNCTIONS] = [".proc_instance_uuid"];

// Kind of map from `user-facing names` to names in sources and names
// in `_func` space.
#[rustfmt::skip]
static VOLATILE_FUNCTIONS_NAMINGS: [(&str, &str, &str); NUM_OF_VOLATILE_FUNCTIONS] = [
    (USER_FACING_NAMES_OF_FUNCTIONS[0], NAMES_OF_FUNCTIONS_IN_SOURCES[0], NAMES_OF_FUNCTIONS_IN_TARANTOOL[0]),
];

/// Maps (maybe quoted or uppercased) name from user to real procedure name in tarantool.
/// Real name stands for name in _func space.
pub fn get_real_function_name(name_from_sql: &str) -> Option<&'static str> {
    let normalized_name = normalize_name_from_sql(name_from_sql);
    VOLATILE_FUNCTIONS_NAMINGS
        .iter()
        .find(|(key, _, _)| *key == normalized_name)
        .map(|(_, _, value)| *value)
}

// Helper map to store CTE node ids by their names.
type CTEs = AHashMap<SmolStr, NodeId>;

#[allow(clippy::uninlined_format_args)]
fn get_timeout(ast: &AbstractSyntaxTree, node_id: usize) -> Result<Decimal, SbroadError> {
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
            let res = Decimal::from_str(duration_value).map_err(|_| {
                SbroadError::Invalid(
                    Entity::Node,
                    Some(format_smolstr!(
                        "AST table duration node {:?} contains invalid value",
                        duration_node,
                    )),
                )
            })?;
            return Ok(res);
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

/// Parse single quote string handling escaping.
/// For input string of "a''b''c" we should get "a'b'c" result.
/// Each two subsequent single quotes are treated as one.
fn escape_single_quotes(raw: &str) -> SmolStr {
    raw.replace("''", "'").to_smolstr()
}

fn retrieve_string_literal(
    ast: &AbstractSyntaxTree,
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
fn parse_string_value_node(ast: &AbstractSyntaxTree, node_id: usize) -> Result<&str, SbroadError> {
    let string_value_node = ast.nodes.get_node(node_id)?;
    let string_value = string_value_node
        .value
        .as_ref()
        .expect("Rule node must contain string value.");
    Ok(string_value.as_str())
}

fn parse_proc_params(
    ast: &AbstractSyntaxTree,
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
        let data_type = match type_node.rule {
            Rule::TypeBool => RelationType::Boolean,
            Rule::TypeDecimal => RelationType::Decimal,
            Rule::TypeDouble => RelationType::Double,
            Rule::TypeInt => RelationType::Integer,
            Rule::TypeString | Rule::TypeText | Rule::TypeVarchar => RelationType::String,
            Rule::TypeUuid => RelationType::Uuid,
            Rule::TypeUnsigned => RelationType::Unsigned,
            _ => unreachable!("Unexpected node: {type_node:?}"),
        };
        params.push(ParamDef { data_type });
    }
    Ok(params)
}

fn parse_call_proc<M: Metadata>(
    ast: &AbstractSyntaxTree,
    node: &ParseNode,
    type_analyzer: &mut TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
) -> Result<Procedure, SbroadError> {
    let proc_name_ast_id = node.children.first().expect("Expected to get Proc name");
    let proc_name = parse_identifier(ast, *proc_name_ast_id)?;

    let proc_values_id = node.children.get(1).expect("Expected to get Proc values");
    let proc_values = ast.nodes.get_node(*proc_values_id)?;
    let mut values: Vec<NodeId> = Vec::with_capacity(proc_values.children.len());
    for proc_value_id in &proc_values.children {
        let proc_value = ast.nodes.get_node(*proc_value_id)?;
        let plan_value_id = match proc_value.rule {
            Rule::Parameter => parse_param(pairs_map.remove_pair(*proc_value_id), worker, plan)?,
            Rule::Literal => {
                let literal_pair = pairs_map.remove_pair(*proc_value_id);
                parse_scalar_expr(
                    Pairs::single(literal_pair),
                    type_analyzer,
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

    let call_proc = Procedure {
        name: proc_name,
        values,
    };
    Ok(call_proc)
}

fn parse_rename_proc(
    ast: &AbstractSyntaxTree,
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

fn parse_create_proc(
    ast: &AbstractSyntaxTree,
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

fn parse_alter_system<M: Metadata>(
    ast: &AbstractSyntaxTree,
    node: &ParseNode,
    type_analyzer: &mut TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionsWorker<M>,
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

    let tier_name = if let Some(tier_node_id) = node.children.get(1) {
        let tier_node = ast.nodes.get_node(*tier_node_id)?;
        let tier_node_child_id = tier_node
            .children
            .first()
            .expect("Expected mandatory child node under AlterSystemTier.");
        let tier_node_child = ast.nodes.get_node(*tier_node_child_id)?;
        match tier_node_child.rule {
            Rule::AlterSystemTiersAll => None,
            Rule::AlterSystemTierSingle => {
                let node_child_id = tier_node_child
                    .children
                    .first()
                    .expect("Child node expected under AlterSystemTierSingle.");
                Some(parse_identifier(ast, *node_child_id)?)
            }
            _ => panic!("Unexpected rule met under AlterSystemTier."),
        }
    } else {
        None
    };

    Ok(AlterSystem {
        ty,
        tier_name,
        timeout: get_default_timeout(),
    })
}

fn parse_proc_with_optional_params(
    ast: &AbstractSyntaxTree,
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

fn parse_drop_proc(ast: &AbstractSyntaxTree, node: &ParseNode) -> Result<DropProc, SbroadError> {
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
fn parse_create_index(
    ast: &AbstractSyntaxTree,
    node: &ParseNode,
) -> Result<CreateIndex, SbroadError> {
    assert_eq!(node.rule, Rule::CreateIndex);
    let mut name = SmolStr::default();
    let mut table_name = SmolStr::default();
    let mut columns = Vec::new();
    let mut unique = false;
    let mut index_type = IndexType::Tree;
    let mut bloom_fpr = None;
    let mut page_size = None;
    let mut range_size = None;
    let mut run_count_per_level = None;
    let mut run_size_ratio = None;
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
    let decimal_value = |node: &ParseNode| -> Decimal {
        Decimal::from_str(
            first_child(node)
                .value
                .as_ref()
                .expect("Expected to see Decimal value")
                .as_str(),
        )
        .expect("Expected to parse decimal value")
    };
    let u32_value = |node: &ParseNode| -> u32 {
        first_child(node)
            .value
            .as_ref()
            .expect("Expected to see u32 value")
            .parse()
            .expect("Expected to parse u32 value")
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
                        Rule::BloomFpr => bloom_fpr = Some(decimal_value(param_node)),
                        Rule::PageSize => page_size = Some(u32_value(param_node)),
                        Rule::RangeSize => range_size = Some(u32_value(param_node)),
                        Rule::RunCountPerLevel => run_count_per_level = Some(u32_value(param_node)),
                        Rule::RunSizeRatio => run_size_ratio = Some(decimal_value(param_node)),
                        Rule::Dimension => dimension = Some(u32_value(param_node)),
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
        bloom_fpr,
        page_size,
        range_size,
        run_count_per_level,
        run_size_ratio,
        dimension,
        distance,
        hint,
        timeout,
        wait_applied_globally,
    };
    Ok(index)
}

fn parse_drop_index(ast: &AbstractSyntaxTree, node: &ParseNode) -> Result<DropIndex, SbroadError> {
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

fn parse_column_def_type(node: &ParseNode) -> Result<RelationType, SbroadError> {
    let data_type = match node.rule {
        Rule::TypeBool => RelationType::Boolean,
        Rule::TypeDatetime => RelationType::Datetime,
        Rule::TypeDecimal => RelationType::Decimal,
        Rule::TypeDouble => RelationType::Double,
        Rule::TypeInt => RelationType::Integer,
        Rule::TypeString | Rule::TypeText | Rule::TypeVarchar => RelationType::String,
        Rule::TypeUnsigned => RelationType::Unsigned,
        Rule::TypeUuid => RelationType::Uuid,
        _ => {
            panic!("Met unexpected rule under ColumnDef: {:?}.", node.rule);
        }
    };
    Ok(data_type)
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::uninlined_format_args)]
fn parse_create_table(
    ast: &AbstractSyntaxTree,
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
    let mut shard_key: Vec<SmolStr> = Vec::new();
    let mut engine_type: SpaceEngineType = SpaceEngineType::default();
    let mut explicit_null_columns: AHashSet<SmolStr> = AHashSet::new();
    let mut timeout = get_default_timeout();
    let mut tier = None;
    let mut is_global = false;
    let mut if_not_exists = DEFAULT_IF_NOT_EXISTS;
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;

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
                    let ty_node_id = column_ty_node
                        .children
                        .first()
                        .expect("ColumnDefType must have a type child");
                    let ty_node = ast.nodes.get_node(*ty_node_id)?;
                    let data_type = parse_column_def_type(ty_node)?;
                    let mut is_nullable = true;

                    for def_child_id in column_def_children.iter().skip(2) {
                        let def_child_node = ast.nodes.get_node(*def_child_id)?;
                        match def_child_node.rule {
                            Rule::ColumnDefIsNull => {
                                match (
                                    def_child_node.children.first(),
                                    def_child_node.children.get(1),
                                ) {
                                    (None, None) => {
                                        let name = name.clone();
                                        explicit_null_columns.insert(name);
                                        is_nullable = true;
                                    }
                                    (Some(child_id), None) => {
                                        let not_flag_node = ast.nodes.get_node(*child_id)?;
                                        if let Rule::NotFlag = not_flag_node.rule {
                                            is_nullable = false;
                                        } else {
                                            panic!(
                                                "Expected NotFlag rule, got: {:?}.",
                                                not_flag_node.rule
                                            );
                                        }
                                    }
                                    _ => panic!("Unexpected rule met under ColumnDefIsNull."),
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
                    let mut column_found = false;
                    for column in &mut columns {
                        if column.name == pk_col_name {
                            column_found = true;
                            if column.is_nullable && explicit_null_columns.contains(&column.name) {
                                return nullable_primary_key_column_error;
                            }
                            // Infer not null on primary key column
                            column.is_nullable = false;
                        }
                    }
                    if !column_found {
                        return Err(SbroadError::Invalid(
                            Entity::Column,
                            Some(format_smolstr!(
                                "Primary key column {pk_col_name} not found."
                            )),
                        ));
                    }
                    pk_keys.push(pk_col_name);
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
            _ => panic!("Unexpected rule met under CreateTable."),
        }
    }
    if pk_keys.is_empty() {
        return Err(SbroadError::Invalid(
            Entity::PrimaryKey,
            Some(format_smolstr!("Primary key must be declared.")),
        ));
    }
    // infer sharding key from primary key
    if shard_key.is_empty() && !is_global {
        shard_key.clone_from(&pk_keys);
    }

    let sharding_key = if !shard_key.is_empty() {
        Some(shard_key)
    } else {
        if engine_type != SpaceEngineType::Memtx {
            return Err(SbroadError::Unsupported(
                Entity::Query,
                Some("global spaces can use only memtx engine".into()),
            ));
        };
        None
    };

    Ok(CreateTable {
        name: table_name,
        format: columns,
        primary_key: pk_keys,
        sharding_key,
        engine_type,
        if_not_exists,
        wait_applied_globally,
        timeout,
        tier,
    })
}

fn parse_alter_table(
    ast: &AbstractSyntaxTree,
    node: &ParseNode,
) -> Result<AlterTable, SbroadError> {
    debug_assert_eq!(
        node.rule,
        Rule::AlterTable,
        "Expected rule AlterTable, got {:?}.",
        node.rule
    );

    let mut table_name = parse_identifier(ast, node.first_child())?;
    let mut wait_applied_globally = DEFAULT_WAIT_APPLIED_GLOBALLY;
    let mut timeout = get_default_timeout();

    let mut add_ops = Vec::new();

    for id in &node.children {
        let node = ast.nodes.get_node(*id)?;
        match node.rule {
            Rule::TableNameIdentifier => {
                table_name = parse_identifier(ast, *id)?;
            }
            Rule::AlterTableColumnAdd => {
                let mut if_not_exists = DEFAULT_IF_NOT_EXISTS;
                for id in &node.children {
                    let node = ast.nodes.get_node(*id)?;
                    match node.rule {
                        Rule::IfNotExists => if_not_exists = true,
                        Rule::AlterTableColumnAddParam => {
                            let name = parse_identifier(ast, node.child_n(0))?;

                            let data_type_node = ast.nodes.get_node(node.child_n(1))?;
                            debug_assert_eq!(data_type_node.rule, Rule::ColumnDefType);
                            let data_type_node =
                                ast.nodes.get_node(data_type_node.first_child())?;
                            let data_type = parse_column_def_type(data_type_node)?;

                            let is_nullable = if let Some(id) = node.children.get(2) {
                                let node = ast.nodes.get_node(*id)?;
                                debug_assert_eq!(node.rule, Rule::ColumnDefIsNull);
                                match (node.children.first(), node.children.get(1)) {
                                    (None, None) => true, // NULL explicitly specified
                                    (Some(child_id), None) => {
                                        let not_flag_node = ast.nodes.get_node(*child_id)?;
                                        if let Rule::NotFlag = not_flag_node.rule {
                                            false // NOT NULL specified
                                        } else {
                                            panic!(
                                                "Expected NotFlag rule, got: {:?}.",
                                                not_flag_node.rule
                                            );
                                        }
                                    }
                                    _ => panic!("Unexpected rule met under ColumnDefIsNull."),
                                }
                            } else {
                                true // column is nullable by default unless otherwise specified
                            };

                            add_ops.push(AlterTableOp::Add {
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
            Rule::AlterTableColumnDrop
            | Rule::AlterTableColumnAlter
            | Rule::AlterTableColumnRename => {
                return Err(SbroadError::Unsupported(
                    Entity::Ddl,
                    Some("ADD COLUMN is the only supported action in ALTER TABLE".to_smolstr()),
                ))
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

    let mut ops = Vec::new();
    ops.append(&mut add_ops);

    Ok(AlterTable {
        name: table_name,
        wait_applied_globally,
        timeout,
        ops,
    })
}

fn parse_drop_table(ast: &AbstractSyntaxTree, node: &ParseNode) -> Result<DropTable, SbroadError> {
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

fn parse_truncate_table(
    ast: &AbstractSyntaxTree,
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

fn parse_set_param(ast: &AbstractSyntaxTree, node: &ParseNode) -> Result<SetParam, SbroadError> {
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

fn parse_deallocate(ast: &AbstractSyntaxTree, node: &ParseNode) -> Result<Deallocate, SbroadError> {
    let param_name = if let Some(identifier_node_id) = node.children.first() {
        Some(parse_identifier(ast, *identifier_node_id)?)
    } else {
        None
    };
    Ok(Deallocate { name: param_name })
}

fn parse_select_full(
    ast: &AbstractSyntaxTree,
    node_id: usize,
    map: &mut Translation,
    plan: &mut Plan,
) -> Result<(), SbroadError> {
    let node = ast.nodes.get_node(node_id)?;
    assert_eq!(node.rule, Rule::SelectFull);
    let mut top_id = None;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Cte => continue,
            Rule::SelectStatement => {
                top_id = Some(parse_select_statement(ast, *child_id, map, plan)?);
            }
            _ => unreachable!("Unexpected node: {child_node:?}"),
        }
    }
    let top_id = top_id.expect("SelectFull must have at least one child");
    map.add(node_id, top_id);
    Ok(())
}

fn parse_select_statement(
    ast: &AbstractSyntaxTree,
    node_id: usize,
    map: &mut Translation,
    plan: &mut Plan,
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
            _ => unreachable!("Unexpected node: {child_node:?}"),
        }
    }
    let top_id = top_id.expect("SelectStatement must have at least one child");
    if let Some(limit) = limit {
        return plan.add_limit(top_id, limit);
    }
    Ok(top_id)
}

fn parse_scan_cte_or_table<M>(
    ast: &AbstractSyntaxTree,
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
    let cte = ctes.get(&scan_name).copied();
    match cte {
        Some(cte_id) => {
            map.add(node_id, cte_id);
        }
        None => {
            let table = metadata.table(scan_name.as_str());
            match table {
                Ok(table) => {
                    plan.add_rel(table);
                    let scan_id = plan.add_scan(scan_name.as_str(), None)?;
                    map.add(node_id, scan_id);
                }
                Err(SbroadError::NotFound(..)) => {
                    return Err(SbroadError::NotFound(
                        Entity::Table,
                        format_smolstr!("with name {}", to_user(scan_name)),
                    ))
                }
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}

fn parse_cte(
    ast: &AbstractSyntaxTree,
    node_id: usize,
    map: &mut Translation,
    ctes: &mut CTEs,
    plan: &mut Plan,
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
                let select_id = parse_select_statement(ast, *child_id, map, plan)?;
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
    let cte_id = plan.add_cte(child_id, name.clone(), columns)?;
    ctes.insert(name, cte_id);
    map.add(node_id, cte_id);
    Ok(())
}

/// Get String value under node that is considered to be an identifier
/// (on which rules on name normalization should be applied).
fn parse_identifier(ast: &AbstractSyntaxTree, node_id: usize) -> Result<SmolStr, SbroadError> {
    Ok(normalize_name_from_sql(parse_string_value_node(
        ast, node_id,
    )?))
}

fn parse_normalized_identifier(
    ast: &AbstractSyntaxTree,
    node_id: usize,
) -> Result<SmolStr, SbroadError> {
    Ok(normalize_name_from_sql(parse_string_value_node(
        ast, node_id,
    )?))
}

/// Common logic for parsing GRANT/REVOKE queries.
#[allow(clippy::too_many_lines)]
fn parse_grant_revoke(
    node: &ParseNode,
    ast: &AbstractSyntaxTree,
) -> Result<(GrantRevokeType, SmolStr, Decimal), SbroadError> {
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
    if let Some(timeout_child_id) = node.children.get(2) {
        timeout = get_timeout(ast, *timeout_child_id)?;
    }

    Ok((grant_revoke_type, grantee_name, timeout))
}

fn parse_trim<M: Metadata>(
    pair: Pair<Rule>,
    referred_relation_ids: &[NodeId],
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
) -> Result<ParseExpression, SbroadError> {
    assert_eq!(pair.as_rule(), Rule::Trim);
    let mut kind = None;
    let mut pattern = None;
    let mut target = None;

    let inner_pairs = pair.into_inner();
    for child_pair in &mut inner_pairs.into_iter() {
        match child_pair.as_rule() {
            Rule::TrimKind => {
                let kind_pair = child_pair
                    .into_inner()
                    .next()
                    .expect("Expected child of TrimKind");
                match kind_pair.as_rule() {
                    Rule::TrimKindBoth => kind = Some(TrimKind::Both),
                    Rule::TrimKindLeading => kind = Some(TrimKind::Leading),
                    Rule::TrimKindTrailing => kind = Some(TrimKind::Trailing),
                    _ => {
                        panic!("Unexpected node: {kind_pair:?}");
                    }
                }
            }
            Rule::TrimPattern => {
                let inner_pattern = child_pair.into_inner();
                pattern = Some(Box::new(parse_expr_pratt(
                    inner_pattern,
                    referred_relation_ids,
                    worker,
                    plan,
                    true,
                )?));
            }
            Rule::TrimTarget => {
                let inner_target = child_pair.into_inner();
                target = Some(Box::new(parse_expr_pratt(
                    inner_target,
                    referred_relation_ids,
                    worker,
                    plan,
                    true,
                )?));
            }
            _ => {
                panic!("Unexpected node: {child_pair:?}");
            }
        }
    }
    let trim = ParseExpression::Trim {
        kind,
        pattern,
        target: target.expect("Trim target must be specified"),
    };
    Ok(trim)
}

fn parse_unsigned(ast_node: &ParseNode) -> Result<u64, SbroadError> {
    assert!(matches!(ast_node.rule, Rule::Unsigned));
    if let Some(str_value) = ast_node.value.as_ref() {
        str_value.parse::<u64>().map_err(|_| {
            SbroadError::Invalid(
                Entity::Query,
                Some(format_smolstr!(
                    "option value is not unsigned integer: {str_value}"
                )),
            )
        })
    } else {
        Err(SbroadError::Invalid(
            AST,
            Some("Unsigned node has value".into()),
        ))
    }
}

/// Common logic for [`sbroad::ir::OptionKind::VdbeOpcodeMax`]
/// and [`sbroad::ir::OptionKind::MotionRowMax`] parsing.
fn parse_option<M: Metadata>(
    ast: &AbstractSyntaxTree,
    option_node_id: usize,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
) -> Result<OptionParamValue, SbroadError> {
    let ast_node = ast.nodes.get_node(option_node_id)?;
    let value = match ast_node.rule {
        Rule::Parameter => {
            let plan_id = parse_param(pairs_map.remove_pair(option_node_id), worker, plan)?;
            OptionParamValue::Parameter { plan_id }
        }
        Rule::Unsigned => {
            let v = parse_unsigned(ast_node)?;
            OptionParamValue::Value {
                val: Value::Unsigned(v),
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

fn try_get_param_index(pair: Pair<'_, Rule>) -> Result<Option<usize>, SbroadError> {
    let inner_param = pair
        .clone()
        .into_inner()
        .next()
        .expect("Concrete param expected under Parameter node");
    match inner_param.as_rule() {
        Rule::TntParameter => Ok(None),
        Rule::PgParameter => {
            let inner_unsigned = inner_param
                .into_inner()
                .next()
                .expect("Unsigned not found under PgParameter");
            let value_idx = inner_unsigned.as_str().parse::<usize>().map_err(|_| {
                SbroadError::Invalid(
                    Entity::Query,
                    Some(format_smolstr!(
                        "{inner_unsigned} cannot be parsed as unsigned param number"
                    )),
                )
            })?;
            if value_idx == 0 {
                return Err(SbroadError::Invalid(
                    Entity::Query,
                    Some("$n parameters are indexed from 1!".into()),
                ));
            }
            Ok(Some(value_idx))
        }
        _ => unreachable!("Unexpected Rule met under Parameter"),
    }
}

fn parse_param<M: Metadata>(
    pair: Pair<'_, Rule>,
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
) -> Result<NodeId, SbroadError> {
    let position = worker
        .parameters_positions
        .iter()
        .position(|p| p == &pair)
        .expect("Couldn't find parameter pair in a vec of parameters pairs");

    let index = if let Some(index) = try_get_param_index(pair)? {
        // Postgres parameter.
        if worker.met_tnt_param {
            return Err(SbroadError::UseOfBothParamsStyles);
        }
        worker.met_pg_param = true;
        index
    } else {
        // Tarantool parameter.
        if worker.met_pg_param {
            return Err(SbroadError::UseOfBothParamsStyles);
        }
        worker.met_tnt_param = true;
        position + 1
    };

    Ok(plan.add_param(index))
}

// Helper structure used to resolve expression operators priority.
lazy_static::lazy_static! {
    static ref PRATT_PARSER: PrattParser<Rule> = {
        use pest::pratt_parser::{Assoc::Left, Op};
        use Rule::{Add, And, Between, ConcatInfixOp, Divide, Eq, Escape, Gt, GtEq, In, IsPostfix, CastPostfix, Like, Similar, Lt, LtEq, Multiply, NotEq, Or, Subtract, UnaryNot};

        // Precedence is defined lowest to highest.
        PrattParser::new()
            .op(Op::infix(Or, Left))
            .op(Op::infix(And, Left))
            .op(Op::prefix(UnaryNot))
            // ESCAPE must be followed by LIKE
            .op(Op::infix(Escape, Left))
            .op(Op::infix(Like, Left))
            .op(Op::infix(Similar, Left))
            .op(Op::infix(Between, Left))
            .op(
                Op::infix(Eq, Left) | Op::infix(NotEq, Left)
                | Op::infix(Gt, Left) | Op::infix(GtEq, Left) | Op::infix(Lt, Left)
                | Op::infix(LtEq, Left) | Op::infix(In, Left)
            )
            .op(Op::infix(Add, Left) | Op::infix(Subtract, Left))
            .op(Op::infix(Multiply, Left) | Op::infix(Divide, Left) | Op::infix(ConcatInfixOp, Left))
            .op(Op::postfix(IsPostfix))
            .op(Op::postfix(CastPostfix))
    };
}

lazy_static::lazy_static! {
    static ref SELECT_PRATT_PARSER: PrattParser<Rule> = {
        use pest::pratt_parser::{Assoc::Left, Op};
        use Rule::{UnionOp, UnionAllOp, ExceptOp};

        PrattParser::new()
            .op(
                Op::infix(UnionOp, Left)
            | Op::infix(UnionAllOp, Left)
            | Op::infix(ExceptOp, Left)
        )
    };
}

/// Number of relational nodes we expect to retrieve column positions for.
const COLUMN_POSITIONS_CACHE_CAPACITY: usize = 10;
/// Total average number of Reference (`ReferenceContinuation`) rule nodes that we expect to get
/// in the parsing result returned from pest. Used for preallocating `reference_to_name_map`.
const REFERENCES_MAP_CAPACITY: usize = 50;

/// Total average number of Expr and Row rule nodes that we expect to get
/// in the parsing result returned from pest. Used for preallocating `ParsingPairsMap`.
const PARSING_PAIRS_MAP_CAPACITY: usize = 100;

/// Helper structure to fix the double linking
/// problem in the BETWEEN operator.
/// We transform `left BETWEEN center AND right` to
/// `left >= center AND left <= right`.
struct Between {
    /// Left node id.
    left_id: NodeId,
    /// LEQ node id (`left <= right`)
    leq_id: NodeId,
}

impl Between {
    fn new(left_id: NodeId, less_eq_id: NodeId) -> Self {
        Self {
            left_id,
            leq_id: less_eq_id,
        }
    }
}

/// Helper struct holding values and references needed for `parse_expr` calls.
struct ExpressionsWorker<'worker, M>
where
    M: Metadata,
{
    /// Helper map of { sq_pair -> ast_id } used for identifying SQ nodes
    /// during both general and Pratt parsing.
    sq_pair_to_ast_ids: &'worker PairToAstIdTranslation<'worker>,
    /// Map of { sq_ast_id -> sq_plan_id }.
    /// Used instead of reference to general `map` using during
    /// parsing in order no to take an immutable reference on it.
    sq_ast_to_plan_id: Translation,
    /// Vec of BETWEEN expressions met during parsing.
    /// Used later to fix them as soon as we need to resolve double-linking problem
    /// of left expression.
    betweens: Vec<Between>,
    /// Map of { subquery_id -> row_id }
    /// that is used to fix `betweens`, which children (references under rows)
    /// may have been changed.
    /// We can't fix between child (clone expression subtree) in the place of their creation,
    /// because we'll copy references without adequate `parent` and `target` that we couldn't fix
    /// later.
    subquery_replaces: AHashMap<NodeId, NodeId>,
    /// Vec of { (sq_id, ref_ids_to_fix_parent_and_target) }
    /// After calling `parse_expr` and creating relational node that can contain SubQuery as
    /// additional child (Selection, Join, Having, OrderBy, GroupBy, Projection) we should pop the
    /// queue till it's not empty and:
    /// * Add subqueries to the list of relational children
    /// * Fix References
    sub_queries_to_fix_queue: VecDeque<(NodeId, Vec<NodeId>)>,
    // Parametes pairs positions in the query. This map is used to index tnt parameters.
    // For example, for query `select ? + ?` this vector will contain 2 pairs for `?`, and the pair
    // on the left will be the first, while the right pair will be the second.
    parameters_positions: Vec<Pair<'worker, Rule>>,
    metadata: &'worker M,
    /// Map of { reference plan_id -> (it's column name, whether it's covered with row)}
    /// We have to save column name in order to use it later for alias creation.
    /// We use information about row coverage later when handling Row expression and need to know
    /// whether we should uncover our reference.
    pub reference_to_name_map: HashMap<NodeId, (SmolStr, bool)>,
    /// Flag indicating whether parameter in Tarantool (? mark) style was met.
    met_tnt_param: bool,
    /// Flag indicating whether parameter in Postgres ($<index>) style was met.
    met_pg_param: bool,
    /// Map of (relational_node_id, columns_position_map).
    /// As `ColumnPositionMap` is used for parsing references and as it may be shared for the same
    /// relational node we cache it so that we don't have to recreate it every time.
    column_positions_cache: HashMap<NodeId, ColumnPositionMap>,
    /// Time at the start of the plan building stage with timezone.
    /// It is used to replace CURRENT_DATE to actual value.
    current_time: OffsetDateTime,
    /// Map of named window definitions where key is the window name and value is the corresponding node ID.
    /// Used to store and look up window definitions that can be referenced by name in window functions.
    named_windows_map: HashMap<SmolStr, NodeId>,
    /// Vector of all window nodes in the query plan.
    /// Stores window definitions in order of appearance, including both named and inline windows.
    windows: Vec<NodeId>,
}

impl<'worker, M> ExpressionsWorker<'worker, M>
where
    M: Metadata,
{
    fn new<'plan: 'worker, 'meta: 'worker>(
        metadata: &'meta M,
        sq_pair_to_ast_ids: &'worker PairToAstIdTranslation,
        parameters_positions: Vec<Pair<'worker, Rule>>,
    ) -> Self {
        Self {
            sq_pair_to_ast_ids,
            sq_ast_to_plan_id: Translation::with_capacity(sq_pair_to_ast_ids.len()),
            subquery_replaces: AHashMap::new(),
            sub_queries_to_fix_queue: VecDeque::new(),
            metadata,
            parameters_positions,
            betweens: Vec::new(),
            reference_to_name_map: HashMap::with_capacity(REFERENCES_MAP_CAPACITY),
            met_tnt_param: false,
            met_pg_param: false,
            column_positions_cache: HashMap::with_capacity(COLUMN_POSITIONS_CACHE_CAPACITY),
            current_time: OffsetDateTime::now_utc(),
            named_windows_map: HashMap::new(),
            windows: Vec::new(),
        }
    }

    fn build_columns_map(&mut self, plan: &Plan, rel_id: NodeId) -> Result<(), SbroadError> {
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.column_positions_cache.entry(rel_id)
        {
            let new_map = ColumnPositionMap::new(plan, rel_id)?;
            e.insert(new_map);
        }

        Ok(())
    }

    fn columns_map_get_positions(
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
    pub(super) fn fix_betweens(&self, plan: &mut Plan) -> Result<(), SbroadError> {
        for between in &self.betweens {
            let left_id = if let Some(id) = self.subquery_replaces.get(&between.left_id) {
                SubtreeCloner::clone_subtree(plan, *id)?
            } else {
                SubtreeCloner::clone_subtree(plan, between.left_id)?
            };
            let less_eq_expr = plan.get_mut_expression_node(between.leq_id)?;
            if let MutExpression::Bool(BoolExpr { ref mut left, .. }) = less_eq_expr {
                *left = left_id;
            } else {
                panic!("Expected to see LEQ expression.")
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
enum ParseExpressionInfixOperator {
    InfixBool(Bool),
    InfixArithmetic(Arithmetic),
    Concat,
    Escape,
}

#[derive(Clone, Debug)]
enum ParseExpression {
    PlanId {
        plan_id: NodeId,
    },
    SubQueryPlanId {
        plan_id: NodeId,
    },
    Infix {
        is_not: bool,
        op: ParseExpressionInfixOperator,
        left: Box<ParseExpression>,
        right: Box<ParseExpression>,
    },
    Function {
        name: String,
        args: Vec<ParseExpression>,
        feature: Option<FunctionFeature>,
    },
    Like {
        left: Box<ParseExpression>,
        right: Box<ParseExpression>,
        escape: Option<Box<ParseExpression>>,
        is_ilike: bool,
    },
    Similar {
        left: Box<ParseExpression>,
        right: Box<ParseExpression>,
        escape: Option<Box<ParseExpression>>,
    },
    Row {
        children: Vec<ParseExpression>,
    },
    Prefix {
        op: Unary,
        child: Box<ParseExpression>,
    },
    Exists {
        is_not: bool,
        child: Box<ParseExpression>,
    },
    Is {
        is_not: bool,
        child: Box<ParseExpression>,
        value: Option<bool>,
    },
    Cast {
        cast_type: CastType,
        child: Box<ParseExpression>,
    },
    Case {
        search_expr: Option<Box<ParseExpression>>,
        when_blocks: Vec<(Box<ParseExpression>, Box<ParseExpression>)>,
        else_expr: Option<Box<ParseExpression>>,
    },
    Trim {
        kind: Option<TrimKind>,
        pattern: Option<Box<ParseExpression>>,
        target: Box<ParseExpression>,
    },
    /// Workaround for the mixfix BETWEEN operator breaking the logic of
    /// pratt parsing for infix operators.
    /// For expression `expr_1 BETWEEN expr_2 AND expr_3` we would create ParseExpression tree of
    ///
    /// And
    ///   - left  = InterimBetween
    ///               - left  = expr_1
    ///               - right = expr_2
    ///   - right = expr_3
    ///
    /// because priority of BETWEEN is higher than of AND.
    ///
    /// When we face such a tree, we transform it into Expression::Between. So during parsing AND
    /// operator we have to check whether we have to transform it into BETWEEN.
    InterimBetween {
        is_not: bool,
        left: Box<ParseExpression>,
        right: Box<ParseExpression>,
    },
    /// Fixed version of `InterimBetween`.
    FinalBetween {
        is_not: bool,
        left: Box<ParseExpression>,
        center: Box<ParseExpression>,
        right: Box<ParseExpression>,
    },
}

#[derive(Clone)]
pub enum SelectOp {
    Union,
    UnionAll,
    Except,
}

/// Helper struct denoting any combination of
/// * SELECT
/// * UNION (ALL)
/// * EXCEPT
#[derive(Clone)]
pub enum SelectSet {
    PlanId {
        plan_id: NodeId,
    },
    Infix {
        op: SelectOp,
        left: Box<SelectSet>,
        right: Box<SelectSet>,
    },
}

impl SelectSet {
    fn populate_plan(&self, plan: &mut Plan) -> Result<NodeId, SbroadError> {
        match self {
            SelectSet::PlanId { plan_id } => Ok(*plan_id),
            SelectSet::Infix { op, left, right } => {
                let left_id = left.populate_plan(plan)?;
                let right_id = right.populate_plan(plan)?;
                match op {
                    u @ (SelectOp::Union | SelectOp::UnionAll) => {
                        let remove_duplicates = matches!(u, SelectOp::Union);
                        plan.add_union(left_id, right_id, remove_duplicates)
                    }
                    SelectOp::Except => plan.add_except(left_id, right_id),
                }
            }
        }
    }
}

impl Plan {
    /// Helper function to populate plan with `SubQuery` represented as a Row of its output.
    fn add_replaced_subquery<M: Metadata>(
        &mut self,
        sq_id: NodeId,
        worker: &mut ExpressionsWorker<M>,
    ) -> Result<NodeId, SbroadError> {
        let (sq_row_id, new_refs) = self.add_row_from_subquery(sq_id)?;
        worker.subquery_replaces.insert(sq_id, sq_row_id);
        worker.sub_queries_to_fix_queue.push_back((sq_id, new_refs));
        Ok(sq_row_id)
    }

    /// Fix subqueries (already represented as rows) for newly created relational node:
    /// replace its references' `parent` and `target` field with valid values.
    fn fix_subquery_rows<M: Metadata>(
        &mut self,
        worker: &mut ExpressionsWorker<M>,
        rel_id: NodeId,
    ) -> Result<(), SbroadError> {
        let mut rel_node = self.get_mut_relation_node(rel_id)?;

        let mut rel_node_children_len = rel_node.mut_children().len();
        for (sq_id, _) in &worker.sub_queries_to_fix_queue {
            rel_node.add_sq_child(*sq_id);
        }

        while let Some((_, new_refs)) = worker.sub_queries_to_fix_queue.pop_front() {
            for new_ref_id in new_refs {
                let new_ref = self.get_mut_expression_node(new_ref_id)?;
                if let MutExpression::Reference(Reference {
                    parent, targets, ..
                }) = new_ref
                {
                    *parent = Some(rel_id);
                    *targets = Some(vec![rel_node_children_len]);
                } else {
                    panic!("Expected SubQuery reference to fix.")
                }
            }
            rel_node_children_len += 1;
        }
        Ok(())
    }
}

impl Plan {
    fn replace_group_by_ordinals_with_references(&mut self) -> Result<(), SbroadError> {
        let top = self.get_top()?;
        let mut post_tree =
            PostOrder::with_capacity(|node| self.nodes.rel_iter(node), REL_CAPACITY);
        post_tree.populate_nodes(top);
        let nodes = post_tree.take_nodes();
        for LevelNode(_, id) in nodes {
            if let Ok(Relational::Projection(_)) = self.get_relation_node(id) {
                self.adjust_grouping_exprs(id)?;
            }
        }
        Ok(())
    }

    fn adjust_grouping_exprs(&mut self, final_proj_id: NodeId) -> Result<(), SbroadError> {
        let (_, upper) = self.split_group_by(final_proj_id)?;

        let final_proj_output = self.get_relational_output(final_proj_id)?;
        let final_proj_cols = self.get_row_list(final_proj_output)?.clone();
        let mut grouping_exprs =
            if let Relational::GroupBy(GroupBy { gr_exprs, .. }) = self.get_relation_node(upper)? {
                gr_exprs.clone()
            } else {
                return Ok(());
            };

        for expr in grouping_exprs.iter_mut() {
            match self.get_expression_node(*expr)? {
                Expression::Constant(Constant { value, .. }) => {
                    let pos = match value {
                        Value::Unsigned(val) => *val as usize,
                        Value::Integer(_) => {
                            return Err(SbroadError::Invalid(
                                Entity::Query,
                                Some(format_smolstr!(
                                    "GROUP BY position {value} is not in select list"
                                )),
                            ));
                        }
                        _ => {
                            continue;
                        }
                    };

                    self.replace_const_with_reference(&final_proj_cols, upper, expr, pos)?;
                }
                Expression::ScalarFunction(ScalarFunction { name, .. }) => {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format_smolstr!("aggregate functions are not allowed in GROUP BY. Got aggregate: {name}"))
                    ));
                }
                _ => {
                    self.check_grouping_expr_subtree(*expr)?;
                }
            }
        }

        if let MutRelational::GroupBy(GroupBy { gr_exprs, .. }) =
            self.get_mut_relation_node(upper)?
        {
            *gr_exprs = grouping_exprs;
        }

        Ok(())
    }

    fn check_grouping_expr_subtree(&self, group_expr: NodeId) -> Result<(), SbroadError> {
        let filter = |node_id: NodeId| -> bool {
            matches!(
                self.get_node(node_id),
                Ok(Node::Expression(Expression::ScalarFunction(_)))
            )
        };
        let mut dfs = PostOrderWithFilter::with_capacity(
            |x| self.nodes.expr_iter(x, false),
            EXPR_CAPACITY,
            Box::new(filter),
        );

        for LevelNode(_, node_id) in dfs.iter(group_expr) {
            let node = self.get_expression_node(node_id)?;
            if let Expression::ScalarFunction(ScalarFunction { name, .. }) = node {
                if Expression::is_aggregate_name(name) {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format_smolstr!("aggregate functions are not allowed inside grouping expression. Got aggregate: {name}"))
                    ));
                }
            }
        }

        Ok(())
    }

    fn replace_const_with_reference(
        &mut self,
        final_proj_cols: &[NodeId],
        groupby_id: NodeId,
        expr: &mut NodeId,
        pos: usize,
    ) -> Result<(), SbroadError> {
        let idx = if let Some(idx) = pos.checked_sub(1) {
            idx
        } else {
            return Err(SbroadError::Invalid(
                Entity::Query,
                Some(format_smolstr!("GROUP BY position 0 is not in select list")),
            ));
        };

        let alias_id = if let Some(id) = final_proj_cols.get(idx) {
            *id
        } else {
            return Err(SbroadError::Invalid(
                Entity::Query,
                Some(format_smolstr!(
                    "GROUP BY position {pos} is not in select list"
                )),
            ));
        };
        let alias_node = self.get_expression_node(alias_id)?;
        if let Expression::Alias(Alias { child, .. }) = alias_node {
            let expr_node = self.get_expression_node(*child)?;
            if let Expression::Reference(Reference {
                position, col_type, ..
            }) = expr_node
            {
                let ref_id =
                    self.nodes
                        .add_ref(Some(groupby_id), Some(vec![0]), *position, *col_type, None);

                *expr = ref_id;
            } else if let Expression::ScalarFunction(ScalarFunction { name, .. }) = expr_node {
                return Err(SbroadError::Invalid(
                    Entity::Query,
                    Some(format_smolstr!(
                        "aggregate functions are not allowed in GROUP BY. Got aggregate: {name}"
                    )),
                ));
            }
        }

        Ok(())
    }
}

impl ParseExpression {
    #[allow(clippy::too_many_lines)]
    fn populate_plan<M>(
        &self,
        plan: &mut Plan,
        worker: &mut ExpressionsWorker<M>,
    ) -> Result<NodeId, SbroadError>
    where
        M: Metadata,
    {
        let plan_id = match self {
            ParseExpression::PlanId { plan_id } => *plan_id,
            ParseExpression::SubQueryPlanId { plan_id } => {
                plan.add_replaced_subquery(*plan_id, worker)?
            }
            ParseExpression::Cast { cast_type, child } => {
                let child_plan_id = child.populate_plan(plan, worker)?;
                plan.add_cast(child_plan_id, *cast_type)?
            }
            ParseExpression::Case {
                search_expr,
                when_blocks,
                else_expr,
            } => {
                let search_expr_id = if let Some(search_expr) = search_expr {
                    Some(search_expr.populate_plan(plan, worker)?)
                } else {
                    None
                };
                let when_block_ids = when_blocks
                    .iter()
                    .map(|(cond, res)| {
                        Ok((
                            cond.populate_plan(plan, worker)?,
                            (res.populate_plan(plan, worker)?),
                        ))
                    })
                    .collect::<Result<Vec<(NodeId, NodeId)>, SbroadError>>()?;
                let else_expr_id = if let Some(else_expr) = else_expr {
                    Some(else_expr.populate_plan(plan, worker)?)
                } else {
                    None
                };
                plan.add_case(search_expr_id, when_block_ids, else_expr_id)
            }
            ParseExpression::Trim {
                kind,
                pattern,
                target,
            } => {
                let pattern = match pattern {
                    Some(p) => Some(p.populate_plan(plan, worker)?),
                    None => None,
                };
                let trim_expr = Trim {
                    kind: kind.clone(),
                    pattern,
                    target: target.populate_plan(plan, worker)?,
                };
                plan.nodes.push(trim_expr.into())
            }
            ParseExpression::Like {
                left,
                right,
                escape,
                is_ilike,
            } => {
                let mut plan_left_id = left.populate_plan(plan, worker)?;
                let mut left_covered_with_row = plan.row(plan_left_id)?;

                let mut plan_right_id = right.populate_plan(plan, worker)?;
                let mut right_covered_with_row = plan.row(plan_right_id)?;

                let escape_covered_with_row = if let Some(escape) = escape {
                    let plan_escape_id = escape.populate_plan(plan, worker)?;
                    Some(plan.row(plan_escape_id)?)
                } else {
                    None
                };
                if *is_ilike {
                    let lower_func = worker.metadata.function("lower")?;
                    plan_left_id =
                        plan.add_stable_function(lower_func, vec![plan_left_id], None)?;
                    left_covered_with_row = plan.row(plan_left_id)?;
                    plan_right_id =
                        plan.add_stable_function(lower_func, vec![plan_right_id], None)?;
                    right_covered_with_row = plan.row(plan_right_id)?;
                }
                plan.add_like(
                    left_covered_with_row,
                    right_covered_with_row,
                    escape_covered_with_row,
                )?
            }
            ParseExpression::Similar {
                left,
                right,
                escape,
            } => {
                let plan_left_id = left.populate_plan(plan, worker)?;
                let left_covered_with_row = plan.row(plan_left_id)?;

                let plan_right_id = right.populate_plan(plan, worker)?;
                let right_covered_with_row = plan.row(plan_right_id)?;

                let escape_covered_with_row = if let Some(escape) = escape {
                    let plan_escape_id = escape.populate_plan(plan, worker)?;
                    Some(plan.row(plan_escape_id)?)
                } else {
                    None
                };
                plan.add_like(
                    left_covered_with_row,
                    right_covered_with_row,
                    escape_covered_with_row,
                )?
            }
            ParseExpression::FinalBetween {
                is_not,
                left,
                center,
                right,
            } => {
                let plan_left_id = left.populate_plan(plan, worker)?;
                let left_covered_with_row = plan.row(plan_left_id)?;

                let plan_center_id = center.populate_plan(plan, worker)?;
                let center_covered_with_row = plan.row(plan_center_id)?;

                let plan_right_id = right.populate_plan(plan, worker)?;
                let right_covered_with_row = plan.row(plan_right_id)?;

                let greater_eq_id =
                    plan.add_cond(left_covered_with_row, Bool::GtEq, center_covered_with_row)?;
                let less_eq_id =
                    plan.add_cond(left_covered_with_row, Bool::LtEq, right_covered_with_row)?;
                let and_id = plan.add_cond(greater_eq_id, Bool::And, less_eq_id)?;
                let between_id = if *is_not {
                    plan.add_unary(Unary::Not, and_id)?
                } else {
                    and_id
                };

                worker
                    .betweens
                    .push(Between::new(left_covered_with_row, less_eq_id));

                between_id
            }
            ParseExpression::Infix {
                op,
                is_not,
                left,
                right,
            } => {
                let left_plan_id = left.populate_plan(plan, worker)?;
                let left_row_id = plan.row(left_plan_id)?;

                let right_plan_id = match op {
                    ParseExpressionInfixOperator::InfixBool(op) => match op {
                        Bool::In => {
                            if let ParseExpression::SubQueryPlanId { plan_id } = &**right {
                                plan.add_replaced_subquery(*plan_id, worker)?
                            } else {
                                right.populate_plan(plan, worker)?
                            }
                        }
                        Bool::Eq | Bool::Gt | Bool::GtEq | Bool::Lt | Bool::LtEq | Bool::NotEq => {
                            if let ParseExpression::SubQueryPlanId { plan_id } = &**right {
                                plan.add_replaced_subquery(*plan_id, worker)?
                            } else {
                                right.populate_plan(plan, worker)?
                            }
                        }
                        _ => right.populate_plan(plan, worker)?,
                    },
                    _ => right.populate_plan(plan, worker)?,
                };

                // In case:
                // * `op` = AND (left = `left_plan_id`, right = `right_plan_id`)
                // * `right_plan_id` = AND (left = expr_1, right = expr_2) (resulted from BETWEEN
                //   transformation)
                //                And
                //  left_plan_id        And
                //                expr_1    expr_2
                //
                // we'll end up in a situation when AND is a right child of another AND (We don't
                // expect it later as soon as AND is a left-associative operator).
                // We have to fix it the following way:
                //                      And
                //               And        expr_2
                //  left_plan_id   expr_1
                let right_plan_is_and = {
                    let right_expr = plan.get_node(right_plan_id)?;
                    matches!(
                        right_expr,
                        Node::Expression(Expression::Bool(BoolExpr { op: Bool::And, .. }))
                    )
                };
                if matches!(op, ParseExpressionInfixOperator::InfixBool(Bool::And))
                    && right_plan_is_and
                {
                    let right_expr = plan.get_expression_node(right_plan_id)?;
                    let fixed_left_and_id = if let Expression::Bool(BoolExpr {
                        op: Bool::And,
                        left,
                        ..
                    }) = right_expr
                    {
                        plan.add_cond(left_row_id, Bool::And, *left)?
                    } else {
                        panic!("Expected to see AND operator as right child.");
                    };

                    let right_expr_mut = plan.get_mut_expression_node(right_plan_id)?;
                    if let MutExpression::Bool(BoolExpr {
                        op: Bool::And,
                        left,
                        ..
                    }) = right_expr_mut
                    {
                        *left = fixed_left_and_id;
                        return Ok(right_plan_id);
                    }
                }

                let right_row_id = plan.row(right_plan_id)?;

                let op_plan_id = match op {
                    ParseExpressionInfixOperator::Concat => {
                        plan.add_concat(left_row_id, right_row_id)
                    }
                    ParseExpressionInfixOperator::InfixArithmetic(arith) => {
                        plan.add_arithmetic_to_plan(left_row_id, arith.clone(), right_row_id)?
                    }
                    ParseExpressionInfixOperator::InfixBool(bool) => {
                        plan.add_cond(left_row_id, *bool, right_row_id)?
                    }
                    ParseExpressionInfixOperator::Escape => {
                        unreachable!("escape op is not added to AST")
                    }
                };
                if *is_not {
                    plan.add_unary(Unary::Not, op_plan_id)?
                } else {
                    op_plan_id
                }
            }
            ParseExpression::Prefix { op, child } => {
                let child_plan_id = child.populate_plan(plan, worker)?;
                let child_covered_with_row = plan.row(child_plan_id)?;
                plan.add_unary(op.clone(), child_covered_with_row)?
            }
            ParseExpression::Function {
                name,
                args,
                feature,
            } => {
                let is_distinct = matches!(feature, Some(FunctionFeature::Distinct));
                let mut plan_arg_ids = Vec::new();
                for arg in args {
                    let arg_plan_id = arg.populate_plan(plan, worker)?;
                    plan_arg_ids.push(arg_plan_id);
                }
                if let Some(kind) = AggregateKind::from_name(name) {
                    plan.add_aggregate_function(name, kind, plan_arg_ids, is_distinct)?
                } else if is_distinct {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some("DISTINCT modifier is allowed only for aggregate functions".into()),
                    ));
                } else {
                    let func = worker.metadata.function(name)?;
                    match func.volatility {
                        VolatilityType::Stable => {
                            plan.add_stable_function(func, plan_arg_ids, feature.clone())?
                        }
                        VolatilityType::Volatile => {
                            plan.add_volatile_function(func, plan_arg_ids, feature.clone())?
                        }
                    }
                }
            }
            ParseExpression::Row { children } => {
                let mut plan_children_ids = Vec::new();
                for child in children {
                    let plan_child_id = child.populate_plan(plan, worker)?;

                    // When handling references, we always cover them with rows
                    // (for the unification of the transformation process in case they are met in
                    // the selection of join condition).
                    // But references may also occur under the Row node (like
                    // `select (ref_1, ref_2) ...`). In such case we don't want out References to
                    // be covered with rows. E.g. in `set_distribution` we presume that
                    // references are not covered with additional rows.
                    let reference = worker.reference_to_name_map.get(&plan_child_id);
                    let uncovered_plan_child_id = if let Some((_, is_row)) = reference {
                        if *is_row {
                            let plan_inner_expr = plan.get_expression_node(plan_child_id)?;
                            *plan_inner_expr.get_row_list()?.first().ok_or_else(|| {
                                SbroadError::UnexpectedNumberOfValues(
                                    "There must be a Reference under Row.".into(),
                                )
                            })?
                        } else {
                            plan_child_id
                        }
                    } else {
                        plan_child_id
                    };
                    plan_children_ids.push(uncovered_plan_child_id);
                }
                plan.nodes.add_row(plan_children_ids, None)
            }
            ParseExpression::Exists { is_not, child } => {
                let ParseExpression::SubQueryPlanId { plan_id } = &**child else {
                    panic!("Expected SubQuery under EXISTS.")
                };
                let child_plan_id = plan.add_replaced_subquery(*plan_id, worker)?;
                let op_id = plan.add_unary(Unary::Exists, child_plan_id)?;
                if *is_not {
                    plan.add_unary(Unary::Not, op_id)?
                } else {
                    op_id
                }
            }
            ParseExpression::Is {
                is_not,
                child,
                value,
            } => {
                let child_plan_id = child.populate_plan(plan, worker)?;
                let child_covered_with_row = plan.row(child_plan_id)?;
                let op_id = match value {
                    None => plan.add_unary(Unary::IsNull, child_covered_with_row)?,
                    Some(b) => {
                        let right_operand = plan.add_const(Value::Boolean(*b));
                        plan.add_bool(child_covered_with_row, Bool::Eq, right_operand)?
                    }
                };
                if *is_not {
                    plan.add_unary(Unary::Not, op_id)?
                } else {
                    op_id
                }
            }
            ParseExpression::InterimBetween { .. } => return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(SmolStr::from(
                    "BETWEEN operator should have a view of `expr_1 BETWEEN expr_2 AND expr_3`.",
                )),
            )),
        };
        Ok(plan_id)
    }
}

/// Workaround for the following expressions:
///     * `expr_1 AND expr_2 BETWEEN expr_3 AND expr_4`
///     * `NOT expr_1 BETWEEN expr_2 AND expr_3`
/// which are parsed as:
///     * `(expr_1 AND (expr_2 BETWEEN expr_3)) AND expr_4`
///     * `(NOT (expr_1 BETWEEN expr_2)) AND expr_3`
/// but we should transform them into:
///     * `expr_1 AND (expr_2 BETWEEN expr_3 AND expr_4)`
///     * `NOT (expr_1 BETWEEN expr_2 AND expr_3)`
///
/// Returns:
/// * None in case `expr` doesn't have `InterimBetween` as a child
/// * Expression whose right child is an `InterimBetween` (or `InterimBetween` itself)
fn find_interim_between(mut expr: &mut ParseExpression) -> Option<(&mut ParseExpression, bool)> {
    let mut is_exact_match = true;
    loop {
        match expr {
            ParseExpression::Infix {
                // TODO: why we handle AND, but don't handle OR?
                op: ParseExpressionInfixOperator::InfixBool(Bool::And),
                right,
                ..
            } => {
                expr = right;
                is_exact_match = false;
            }
            ParseExpression::Prefix {
                op: Unary::Not,
                child,
            } => {
                expr = child;
                is_exact_match = false;
            }
            ParseExpression::InterimBetween { .. } => break Some((expr, is_exact_match)),
            _ => break None,
        }
    }
}

fn connect_escape_to_like_node(
    mut lhs: ParseExpression,
    rhs: ParseExpression,
) -> Result<ParseExpression, SbroadError> {
    let (ParseExpression::Like { escape, .. } | ParseExpression::Similar { escape, .. }) = &mut lhs
    else {
        return Err(SbroadError::Invalid(
            Entity::Expression,
            Some(format_smolstr!(
                "ESCAPE can go only after LIKE or SIMILAR expressions, got: {:?}",
                lhs
            )),
        ));
    };
    if escape.is_some() {
        return Err(SbroadError::Invalid(
            Entity::Expression,
            Some(
                "escape specified twice: expr1 LIKE/SIMILAR expr2 ESCAPE expr 3 ESCAPE expr4"
                    .into(),
            ),
        ));
    }
    *escape = Some(Box::new(rhs));
    Ok(lhs)
}

fn cast_type_from_pair(type_pair: Pair<Rule>) -> Result<CastType, SbroadError> {
    if type_pair.as_rule() != Rule::ColumnDefType {
        // TypeAny.
        return CastType::try_from(&type_pair.as_rule());
    }

    let mut column_def_type_pairs = type_pair.into_inner();
    let column_def_type = column_def_type_pairs
        .next()
        .expect("concrete type expected under ColumnDefType");
    if column_def_type.as_rule() != Rule::TypeVarchar {
        return CastType::try_from(&column_def_type.as_rule());
    }

    let mut type_pairs_inner = column_def_type.into_inner();
    let type_cast = type_pairs_inner.next().map_or_else(
        || Ok(CastType::Text),
        |varchar_length| {
            varchar_length
                .as_str()
                .parse::<usize>()
                .map(CastType::Varchar)
                .map_err(|e| {
                    SbroadError::ParsingError(
                        Entity::Value,
                        format_smolstr!("Failed to parse varchar length: {e:?}."),
                    )
                })
        },
    )?;
    Ok(type_cast)
}

///This function converts an SQL pattern into a regex string, validating escape characters.
///
///Taken from PG source. Function similar_escape_internal <https://github.com/postgres/postgres/blob/c623e8593ec4ee6987f3cd9350ced7caf8526ed2/src/backend/utils/adt/regexp.c#L767>
pub fn transform_to_regex_pattern(pat_text: &str, esc_text: &str) -> Result<String, String> {
    let escape = match esc_text {
        e if e.len() == 1 => e,
        "" => "",
        _ => {
            return Err(
                "invalid escape string. Escape string must be empty or one character.".into(),
            )
        }
    };

    let mut result = String::from("^(?:");
    let mut after_escape = false;
    let mut in_char_class = false;
    let mut nquotes = 0;

    let chars = pat_text.chars().peekable();

    for c in chars {
        if after_escape {
            if c == '"' && !in_char_class {
                // escape-double-quote?
                if nquotes == 0 {
                    // First quote: end first part, make it non-greedy
                    result.push_str("){1,1}?");
                    result.push('(');
                } else if nquotes == 1 {
                    // Second quote: end second part, make it greedy
                    result.push_str("){1,1}(");
                    result.push_str("?:");
                } else {
                    return Err("SQL regular expression may not contain more than two escape-double-quote separators".into());
                }
                nquotes += 1;
            } else {
                // Escape any character
                result.push('\\');
                result.push(c);
            }
            after_escape = false;
        } else if c.to_string() == escape {
            // SQL escape character; do not send to output
            after_escape = true;
        } else if in_char_class {
            if c == '\\' {
                result.push('\\');
            }
            result.push(c);
            if c == ']' {
                in_char_class = false;
            }
        } else {
            match c {
                '[' => {
                    result.push(c);
                    in_char_class = true;
                }
                '%' => {
                    result.push_str(".*");
                }
                '_' => {
                    result.push('.');
                }
                '(' => {
                    result.push_str("(?:");
                }
                '\\' | '.' | '^' | '$' => {
                    result.push('\\');
                    result.push(c);
                }
                _ => {
                    result.push(c);
                }
            }
        }
    }

    result.push_str(")$");
    Ok(result)
}

fn parse_window_func<M: Metadata>(
    pair: Pair<Rule>,
    referred_relation_ids: &[NodeId],
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
) -> Result<NodeId, SbroadError> {
    let mut inner = pair.into_inner();
    let mut type_analyzer = type_system::new_analyzer();

    // Parse function name
    let func_name_pair = inner.next().expect("Function name expected under Over");
    let func_name = func_name_pair.as_str().to_smolstr();

    // Parse function arguments
    let args_pair = inner.next().expect("Function args expected under Over");
    let mut func_args = if let Some(args_inner) = args_pair.clone().into_inner().next() {
        match args_inner.as_rule() {
            Rule::CountAsterisk => Vec::with_capacity(1),
            Rule::WindowFunctionArgsInner => {
                let args_count = args_inner.into_inner().count();
                Vec::with_capacity(args_count)
            }
            _ => Vec::new(),
        }
    } else {
        Vec::new()
    };

    if let Some(args_inner) = args_pair.into_inner().next() {
        match args_inner.as_rule() {
            Rule::CountAsterisk => {
                let normalized_name = func_name.to_lowercase();
                if "count" != normalized_name.as_str() {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format_smolstr!(
                            "\"*\" is allowed only inside \"count\" aggregate function. Got: {normalized_name}",
                        ))
                    ));
                }
                let count_asterisk_plan_id = plan.nodes.push(CountAsterisk {}.into());
                func_args.push(count_asterisk_plan_id);
            }
            Rule::WindowFunctionArgsInner => {
                for arg_pair in args_inner.into_inner() {
                    let expr_plan_node_id = parse_scalar_expr(
                        Pairs::single(arg_pair),
                        &mut type_analyzer,
                        referred_relation_ids,
                        worker,
                        plan,
                        true,
                    )?;
                    func_args.push(expr_plan_node_id);
                }
            }
            _ => panic!(
                "Unexpected rule met under WindowFunction: {:?}",
                args_inner.as_rule()
            ),
        }
    }

    // Parse filter
    let filter_pair = inner.next().expect("Filter expected under Over");
    let filter = if let Some(filter_inner) = filter_pair.into_inner().next() {
        let expr = parse_scalar_expr(
            Pairs::single(filter_inner),
            &mut type_analyzer,
            referred_relation_ids,
            worker,
            plan,
            false,
        )?;
        Some(expr)
    } else {
        None
    };

    // Parse window
    let window_pair = inner.next().expect("Window expected under Over");
    let mut ref_by_name = false;
    let window = match window_pair.as_rule() {
        Rule::Identifier => {
            let window_name = window_pair.as_str().to_smolstr();

            let err = Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format_smolstr!("Window with name {window_name} not found")),
            ));

            ref_by_name = true;

            worker.named_windows_map.get(&window_name).map_or(err, Ok)?
        }
        Rule::WindowBody => {
            let mut partition: Option<Vec<NodeId>> = None;
            let mut ordering = None;
            let mut frame = None;

            let err = Err(SbroadError::Invalid(
                Entity::Expression,
                Some(SmolStr::from("Invalid WINDOW body definition. Expected view of [BASE_WINDOW] [PARTITION BY] [ORDER BY] [FRAME]"))
            ));

            for body_part in window_pair.into_inner() {
                match body_part.as_rule() {
                    Rule::WindowPartition => {
                        if partition.is_some() || ordering.is_some() || frame.is_some() {
                            return err;
                        }

                        let mut partition_exprs = Vec::new();
                        for part_expr in body_part.into_inner() {
                            let part_expr_plan_node_id = parse_scalar_expr(
                                Pairs::single(part_expr),
                                &mut type_analyzer,
                                referred_relation_ids,
                                worker,
                                plan,
                                false,
                            )?;
                            partition_exprs.push(part_expr_plan_node_id);
                        }
                        partition = Some(partition_exprs);
                    }
                    Rule::WindowOrderBy => {
                        let mut order_by_elements = Vec::new();
                        for order_item in body_part.into_inner() {
                            let mut order_item_inner = order_item.into_inner();
                            let expr_pair = order_item_inner
                                .next()
                                .expect("Expected expression in ORDER BY");
                            let expr_id = parse_scalar_expr(
                                Pairs::single(expr_pair.clone()),
                                &mut type_analyzer,
                                referred_relation_ids,
                                worker,
                                plan,
                                false,
                            )?;

                            // Check for DESC/ASC
                            let order_type = if order_item_inner.any(|p| p.as_rule() == Rule::Desc)
                            {
                                OrderByType::Desc
                            } else {
                                OrderByType::Asc
                            };

                            order_by_elements.push(OrderByElement {
                                entity: OrderByEntity::Expression { expr_id },
                                order_type: Some(order_type),
                            });
                        }
                        ordering = Some(order_by_elements)
                    }
                    Rule::WindowFrame => {
                        if frame.is_some() {
                            return err;
                        }

                        let mut frame_inner = body_part.into_inner();
                        let frame_type = frame_inner
                            .next()
                            .expect("WindowFrame should have WindowFrameType child");
                        let ty = match frame_type.as_rule() {
                            Rule::WindowFrameTypeRange => FrameType::Range,
                            Rule::WindowFrameTypeRows => FrameType::Rows,
                            _ => panic!(
                                "Expected FrameTypeRange or FrameTypeRows, got: {frame_type:?}"
                            ),
                        };

                        let frame_bound = frame_inner
                            .next()
                            .expect("WindowFrame should have WindowFrameBound children");
                        let bound = match frame_bound.as_rule() {
                            Rule::WindowFrameSingle => {
                                let bound_inner = frame_bound.into_inner().next().expect(
                                    "WindowFrameSingle should contain WindowFrameBound as a child",
                                );
                                let bound_type = parse_frame_bound(
                                    bound_inner,
                                    &mut type_analyzer,
                                    referred_relation_ids,
                                    worker,
                                    plan,
                                )?;
                                Bound::Single(bound_type)
                            }
                            Rule::WindowFrameBetween => {
                                let mut between_bounds = frame_bound.into_inner();
                                let first_bound = between_bounds.next().expect(
                                    "WindowFrameBetween should contain an opening bound as a child",
                                );
                                let second_bound = between_bounds.next().expect(
                                    "WindowFrameBetween should contain a closing bound as a child",
                                );
                                let first_type = parse_frame_bound(
                                    first_bound,
                                    &mut type_analyzer,
                                    referred_relation_ids,
                                    worker,
                                    plan,
                                )?;
                                let second_type = parse_frame_bound(
                                    second_bound,
                                    &mut type_analyzer,
                                    referred_relation_ids,
                                    worker,
                                    plan,
                                )?;
                                Bound::Between(first_type, second_type)
                            }
                            _ => panic!("Unexpected rule met under WindowFrame: {frame_bound:?}"),
                        };

                        frame = Some(Frame { ty, bound });
                    }
                    _ => panic!("Unexpected rule met under WindowBody: {body_part:?}"),
                }
            }

            let window = Window {
                name: None,
                partition,
                ordering,
                frame,
            };
            &plan.nodes.push(window.into())
        }
        _ => panic!("Unexpected rule met under Window: {window_pair:?}"),
    };
    worker.windows.push(*window);
    let over = Over {
        func_name,
        func_args,
        filter,
        window: *window,
        ref_by_name,
    };

    Ok(plan.nodes.push(over.into()))
}

fn parse_frame_bound<M: Metadata>(
    bound: Pair<Rule>,
    type_analyzer: &mut TypeAnalyzer,
    referred_relation_ids: &[NodeId],
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
) -> Result<BoundType, SbroadError> {
    let rule = bound.as_rule();
    match rule {
        Rule::PrecedingUnbounded => Ok(BoundType::PrecedingUnbounded),
        Rule::CurrentRow => Ok(BoundType::CurrentRow),
        Rule::FollowingUnbounded => Ok(BoundType::FollowingUnbounded),
        Rule::PrecedingOffset | Rule::FollowingOffset => {
            let offset_expr = bound
                .into_inner()
                .next()
                .expect("Expr node expected under Offset window bound");
            let offset_expr_id = parse_scalar_expr(
                Pairs::single(offset_expr),
                type_analyzer,
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;

            Ok(match rule {
                Rule::PrecedingOffset => BoundType::PrecedingOffset(offset_expr_id),
                Rule::FollowingOffset => BoundType::FollowingOffset(offset_expr_id),
                _ => unreachable!("Offset bound expected"),
            })
        }
        _ => panic!("Unexpected rule met under WindowFrameBound: {rule:?}"),
    }
}

fn parse_substring<M: Metadata>(
    pair: Pair<Rule>,
    referred_relation_ids: &[NodeId],
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
) -> Result<ParseExpression, SbroadError> {
    assert_eq!(pair.as_rule(), Rule::Substring);

    let mut inner = pair.into_inner();
    let variant = inner.next().ok_or_else(|| {
        SbroadError::ParsingError(Entity::Expression, "no substring variant".into())
    })?;

    match variant.as_rule() {
        Rule::SubstringFromFor => {
            // Handle: substring(expr FROM expr FOR expr)
            let mut pieces = variant.into_inner();
            let string_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;
            let from_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;
            let for_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;

            Ok(ParseExpression::Function {
                name: "substring".to_string(),
                args: vec![string_expr, from_expr, for_expr],
                feature: Some(FunctionFeature::Substring(Substring::FromFor)),
            })
        }
        Rule::SubstringRegular => {
            // Handle: substring(expr, expr, expr)
            let mut pieces = variant.into_inner();
            let string_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;
            let from_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;
            let for_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;

            Ok(ParseExpression::Function {
                name: "substring".to_string(),
                args: vec![string_expr, from_expr, for_expr],
                feature: Some(FunctionFeature::Substring(Substring::Regular)),
            })
        }
        Rule::SubstringFor => {
            // Handle: substring(expr FOR expr)
            let mut pieces = variant.into_inner();
            let string_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;
            let for_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;

            let string_id = string_expr.populate_plan(plan, worker)?;
            let one_literal = plan.add_const(Value::Unsigned(1));
            let for_id = for_expr.populate_plan(plan, worker)?;

            Ok(ParseExpression::Function {
                name: "substr".to_string(),
                args: vec![
                    ParseExpression::PlanId { plan_id: string_id },
                    ParseExpression::PlanId {
                        plan_id: one_literal,
                    },
                    ParseExpression::PlanId { plan_id: for_id },
                ],
                feature: Some(FunctionFeature::Substring(Substring::For)),
            })
        }
        Rule::SubstringFrom => {
            // Handle: substring(expr FROM expr) - both numeric and regexp variants
            let mut pieces = variant.into_inner();
            let string_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;
            let from_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;

            Ok(ParseExpression::Function {
                name: "substring".to_string(),
                args: vec![string_expr, from_expr],
                feature: Some(FunctionFeature::Substring(Substring::From)),
            })
        }
        Rule::SubstringSimilar => {
            // Handle: substring(expr SIMILAR expr ESCAPE expr)
            let mut pieces = variant.into_inner();
            let similar_expr = parse_expr_pratt(
                pieces.next().expect("Expected expression").into_inner(),
                referred_relation_ids,
                worker,
                plan,
                true,
            )?;

            let mut args = vec![];

            if let ParseExpression::Similar {
                left,
                right,
                escape,
            } = similar_expr
            {
                // Provide a better error message when the escape symbol is missing.
                let escape_box = match escape {
                    Some(esc) => esc,
                    None => {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some("missing escape symbol for SIMILAR substring operator".into()),
                        ))
                    }
                };

                // Bind the expressions from the Similar variant.
                let string_expr = *left;
                let pattern_expr = *right;
                let escape_expr = *escape_box;

                args.push(string_expr);
                args.push(pattern_expr);
                args.push(escape_expr);
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some("incorrect SUBSTRING parameters. There is no such overload that takes only 1 argument".into()),
                ));
            }

            Ok(ParseExpression::Function {
                name: "substring".to_string(),
                args,
                feature: Some(FunctionFeature::Substring(Substring::Similar)),
            })
        }
        _ => Err(SbroadError::ParsingError(
            Entity::Expression,
            "Unrecognized SubstringVariant".into(),
        )),
    }
}

pub fn is_negative_number(plan: &Plan, expr_id: NodeId) -> Result<bool, SbroadError> {
    if let Expression::Constant(Constant { value }) = plan.get_expression_node(expr_id)? {
        return Ok(matches!(value, Value::Integer(n) if *n < 0));
    }

    Ok(false)
}

/// Function responsible for parsing expressions using Pratt parser.
///
/// Parameters:
/// * Raw `expression_pair`, resulted from pest parsing. General idea is that we always have to
///   pass `Expr` pair with `into_inner` call.
/// * `ast` resulted from some parsing node transformations
/// * `plan` currently being built
///
/// Returns:
/// * Id of root `Expression` node added into the plan
#[allow(clippy::too_many_lines)]
fn parse_expr_pratt<M>(
    expression_pairs: Pairs<Rule>,
    referred_relation_ids: &[NodeId],
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
    safe_for_volatile_function: bool,
) -> Result<ParseExpression, SbroadError>
where
    M: Metadata,
{
    type WhenBlocks = Vec<(Box<ParseExpression>, Box<ParseExpression>)>;

    PRATT_PARSER
        .map_primary(|primary| {
            let parse_expr = match primary.as_rule() {
                Rule::Expr | Rule::Literal => {
                    parse_expr_pratt(primary.into_inner(), referred_relation_ids, worker, plan, safe_for_volatile_function)?
                }
                Rule::ExpressionInParentheses => {
                    let mut inner_pairs = primary.into_inner();
                    let child_expr_pair = inner_pairs
                        .next()
                        .expect("Expected to see inner expression under parentheses");
                    parse_expr_pratt(
                        Pairs::single(child_expr_pair),
                        referred_relation_ids,
                        worker,
                        plan,
                        safe_for_volatile_function,
                    )?
                }
                Rule::Parameter => {
                    let plan_id = parse_param(primary, worker, plan)?;
                    ParseExpression::PlanId { plan_id }
                }
                Rule::Over => {
                    let plan_id = parse_window_func(primary, referred_relation_ids, worker, plan)?;
                    ParseExpression::PlanId { plan_id }
                }
                Rule::IdentifierWithOptionalContinuation => {
                    let mut inner_pairs = primary.into_inner();
                    let first_identifier = inner_pairs.next().expect(
                        "Identifier expected under IdentifierWithOptionalContinuation"
                    ).as_str();

                    let mut scan_name = None;
                    let mut col_name = normalize_name_from_sql(first_identifier);

                    if inner_pairs.len() != 0 {
                        let continuation = inner_pairs.next().expect("Continuation expected after Identifier");
                        match continuation.as_rule() {
                            Rule::ReferenceContinuation => {
                                let col_name_pair = continuation.into_inner()
                                    .next().expect("Reference continuation must contain an Identifier");
                                let second_identifier = normalize_name_from_sql(col_name_pair.as_str());
                                scan_name = Some(col_name);
                                col_name = second_identifier;
                            }
                            Rule::FunctionInvocationContinuation => {
                                // Handle function invocation case.
                                let mut function_name = String::from(first_identifier);
                                let mut args_pairs = continuation.into_inner();
                                let mut feature = None;
                                let mut parse_exprs_args = Vec::new();
                                let function_args = args_pairs.next();
                                if let Some(function_args) = function_args {
                                    match function_args.as_rule() {
                                        Rule::CountAsterisk => {
                                            let normalized_name = function_name.to_lowercase();
                                            if "count" != normalized_name.as_str() {
                                                return Err(SbroadError::Invalid(
                                                    Entity::Query,
                                                    Some(format_smolstr!(
                                                        "\"*\" is allowed only inside \"count\" aggregate function. Got: {normalized_name}",
                                                    ))
                                                ));
                                            }
                                            let count_asterisk_plan_id = plan.nodes.push(CountAsterisk{}.into());
                                            parse_exprs_args.push(ParseExpression::PlanId { plan_id: count_asterisk_plan_id });
                                        }
                                        Rule::FunctionArgs => {
                                            let mut args_inner = function_args.into_inner();
                                            let mut arg_pairs_to_parse = Vec::new();
                                            let mut volatile = false;

                                            // Exposed by picodata scalar function name should be 
                                            // transformed to real name of representing it stored procedure.
                                            if let Some(name) = get_real_function_name(&function_name) {
                                                if !safe_for_volatile_function {
                                                    return Err(SbroadError::NotImplemented(
                                                        Entity::VolatileFunction, "is not allowed in filter clause".to_smolstr(),
                                                        )
                                                    );
                                                }

                                                function_name = name.to_string();
                                                volatile = true;

                                            }

                                            if let Some(first_arg_pair) = args_inner.next() {
                                                if let Rule::Distinct = first_arg_pair.as_rule() {
                                                    if volatile {
                                                        return Err(SbroadError::Invalid(
                                                            Entity::Query,
                                                            Some(format_smolstr!(
                                                                "\"distinct\" is not allowed inside VOLATILE function call",
                                                            ))
                                                        ));
                                                    }

                                                    feature = Some(FunctionFeature::Distinct);
                                                } else {
                                                    arg_pairs_to_parse.push(first_arg_pair);
                                                }
                                            }

                                            for arg_pair in args_inner {
                                                arg_pairs_to_parse.push(arg_pair);
                                            }

                                            for arg in arg_pairs_to_parse {
                                                let arg_expr = parse_expr_pratt(
                                                    arg.into_inner(),
                                                    referred_relation_ids,
                                                    worker,
                                                    plan,
                                                   safe_for_volatile_function,
                                                )?;
                                                parse_exprs_args.push(arg_expr);
                                            }
                                        }
                                        rule => unreachable!("{}", format!("Unexpected rule under FunctionInvocation: {rule:?}"))
                                    }
                                }
                                return Ok(ParseExpression::Function {
                                    name: function_name,
                                    args: parse_exprs_args,
                                    feature,
                                })
                            }
                            rule => unreachable!("Expr::parse expected identifier continuation, found {:?}", rule)
                        }
                    };

                    if referred_relation_ids.is_empty() {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some(format_smolstr!("Reference {first_identifier} met under Values that is unsupported. For string literals use single quotes."))
                        ))
                    }

                    let plan_left_id = referred_relation_ids
                        .first()
                        .ok_or(SbroadError::Invalid(
                            Entity::Query,
                            Some("Reference must point to some relational node".into())
                        ))?;

                    worker.build_columns_map(plan, *plan_left_id)?;

                    let left_child_col_position = worker.columns_map_get_positions(*plan_left_id, &col_name, scan_name.as_deref());

                    let plan_right_id = referred_relation_ids.get(1);
                    let (ref_id, is_row) = if let Some(plan_right_id) = plan_right_id {
                        // Referencing Join node.
                        worker.build_columns_map(plan, *plan_right_id)?;
                        let right_child_col_position = worker.columns_map_get_positions(*plan_right_id, &col_name, scan_name.as_deref());

                        let present_in_left = left_child_col_position.is_ok();
                        let present_in_right = right_child_col_position.is_ok();

                        let ref_id = if present_in_left && present_in_right {
                            return Err(SbroadError::Invalid(
                                Entity::Column,
                                Some(format_smolstr!(
                                    "column name '{col_name}' is present in both join children",
                                )),
                            ));
                        } else if present_in_left {
                            let col_with_scan = ColumnWithScan::new(&col_name, scan_name.as_deref());
                            plan.add_row_from_left_branch(
                                *plan_left_id,
                                *plan_right_id,
                                &[col_with_scan],
                            )?
                        } else if present_in_right {
                            let col_with_scan = ColumnWithScan::new(&col_name, scan_name.as_deref());
                            plan.add_row_from_right_branch(
                                *plan_left_id,
                                *plan_right_id,
                                &[col_with_scan],
                            )?
                        } else {
                            return Err(SbroadError::NotFound(
                                Entity::Column,
                                format_smolstr!("'{col_name}' in the join children",),
                            ));
                        };
                        (ref_id, true)
                    } else {
                        // Referencing single node.
                        let col_position = match left_child_col_position {
                            Ok(col_position) => col_position,
                            Err(e) => return Err(e)
                        };
                        let child = plan.get_relation_node(*plan_left_id)?;
                        let child_alias_ids = plan.get_row_list(
                            child.output()
                        )?;
                        let child_alias_id = child_alias_ids
                            .get(col_position)
                            .expect("column position is invalid");
                        let col_type = plan
                            .get_expression_node(*child_alias_id)?
                            .calculate_type(plan)?;
                        let ref_id = plan.nodes.add_ref(None, Some(vec![0]), col_position, col_type, None);
                        (ref_id, false)
                    };
                    worker.reference_to_name_map.insert(ref_id, (col_name, is_row));
                    ParseExpression::PlanId { plan_id: ref_id }
                }
                Rule::SubQuery => {
                    let sq_ast_id: &usize = worker
                        .sq_pair_to_ast_ids
                        .get(&primary).expect("SQ ast_id must exist for pest pair");
                    let plan_id = worker
                        .sq_ast_to_plan_id
                        .get(*sq_ast_id)?;
                    ParseExpression::SubQueryPlanId { plan_id }
                }
                Rule::Row => {
                    let mut children = Vec::new();

                    for expr_pair in primary.into_inner() {
                        let child_parse_expr = parse_expr_pratt(
                            expr_pair.into_inner(),
                            referred_relation_ids,
                            worker,
                            plan,
                            safe_for_volatile_function,
                        )?;
                        children.push(child_parse_expr);
                    }
                    ParseExpression::Row { children }
                }
                Rule::Decimal
                | Rule::Double
                | Rule::Unsigned
                | Rule::Null
                | Rule::True
                | Rule::SingleQuotedString
                | Rule::Integer
                | Rule::False => {
                    let val = Value::from_node(&primary)?;
                    let plan_id = plan.add_const(val);
                    ParseExpression::PlanId { plan_id }
                }
                Rule::Exists => {
                    let mut inner_pairs = primary.into_inner();
                    let first_pair = inner_pairs.next()
                        .expect("No child found under Exists node");
                    let first_is_not = matches!(first_pair.as_rule(), Rule::NotFlag);
                    let expr_pair = if first_is_not {
                        inner_pairs.next()
                            .expect("Expr expected next to NotFlag under Exists")
                    } else {
                        first_pair
                    };

                    let child_parse_expr = parse_expr_pratt(
                        Pairs::single(expr_pair),
                        referred_relation_ids,
                        worker,
                        plan,
                      safe_for_volatile_function
                    )?;
                    ParseExpression::Exists { is_not: first_is_not, child: Box::new(child_parse_expr)}
                }
                Rule::Trim => parse_trim(primary, referred_relation_ids, worker, plan)?,
                Rule::Substring => parse_substring(primary, referred_relation_ids, worker, plan)?,
                Rule::CastOp => {
                    let mut inner_pairs = primary.into_inner();
                    let expr_pair = inner_pairs.next().expect("Cast has no expr child.");
                    let child_parse_expr = parse_expr_pratt(
                        expr_pair.into_inner(),
                        referred_relation_ids,
                        worker,
                        plan,
                       safe_for_volatile_function,
                    )?;
                    let type_pair = inner_pairs.next().expect("CastOp has no type child");
                    let cast_type = cast_type_from_pair(type_pair)?;

                    ParseExpression::Cast { cast_type, child: Box::new(child_parse_expr) }
                }
                Rule::Case => {
                    let mut inner_pairs = primary.into_inner();

                    let first_pair = inner_pairs.next().expect("Case must have at least one child");
                    let mut when_block_pairs = Vec::new();
                    let search_expr = if let Rule::Expr = first_pair.as_rule() {
                        let expr = parse_expr_pratt(
                            first_pair.into_inner(),
                            referred_relation_ids,
                            worker,
                            plan,
                            safe_for_volatile_function,
                        )?;
                        Some(Box::new(expr))
                    } else {
                        when_block_pairs.push(first_pair);
                        None
                    };

                    let mut else_expr = None;
                    for pair in inner_pairs {
                        if Rule::CaseElseBlock == pair.as_rule() {
                            let expr = parse_expr_pratt(
                                pair.into_inner(),
                                referred_relation_ids,
                                worker,
                                plan,
                                safe_for_volatile_function,
                            )?;
                            else_expr = Some(Box::new(expr));
                        } else {
                            when_block_pairs.push(pair);
                        }
                    }

                    let when_blocks: Result<WhenBlocks, SbroadError> = when_block_pairs
                        .into_iter()
                        .map(|when_block_pair| {
                            let mut inner_pairs = when_block_pair.into_inner();
                            let condition_expr_pair = inner_pairs.next().expect("When block must contain condition expression.");
                            let condition_expr = parse_expr_pratt(
                                condition_expr_pair.into_inner(),
                                referred_relation_ids,
                                worker,
                                plan,
                               false,
                            )?;

                            let result_expr_pair = inner_pairs.next().expect("When block must contain result expression.");
                            let result_expr = parse_expr_pratt(
                                result_expr_pair.into_inner(),
                                referred_relation_ids,
                                worker,
                                plan,
                               safe_for_volatile_function,
                            )?;

                            Ok::<(Box<ParseExpression>, Box<ParseExpression>), SbroadError>((
                                Box::new(condition_expr),
                                Box::new(result_expr)
                            ))

                        })
                        .collect();

                    ParseExpression::Case {
                        search_expr,
                        when_blocks: when_blocks?,
                        else_expr,
                    }
                }
                Rule::CurrentDate => {
                    let date = worker.current_time.replace_time(Time::MIDNIGHT);
                    let val = Value::Datetime(Datetime::from_inner(date));
                    let plan_id = plan.add_const(val);
                    ParseExpression::PlanId { plan_id }
                }
                Rule::LocalTimestamp => {
                    // Get precision from parsing (max value 6)
                    let precision = primary.into_inner().next()
                        .map(|p| p.as_str().parse::<usize>().unwrap_or(usize::MAX).min(6))
                        .unwrap_or(6); // Default for Postgres is 6
                    let plan_id = plan.nodes.push(LocalTimestamp{precision}.into());
                    ParseExpression::PlanId { plan_id }
                }
                Rule::CountAsterisk => {
                    let plan_id = plan.nodes.push(CountAsterisk{}.into());
                    ParseExpression::PlanId { plan_id }
                }
                rule      => unreachable!("Expr::parse expected atomic rule, found {:?}", rule),
            };
            Ok(parse_expr)
        })
        .map_infix(|lhs, op, rhs| {
            let mut lhs = lhs?;
            let rhs = rhs?;
            let mut is_not = false;
            let op = match op.as_rule() {
                Rule::And => ParseExpressionInfixOperator::InfixBool(Bool::And),
                Rule::Or => ParseExpressionInfixOperator::InfixBool(Bool::Or),
                Rule::Like => {
                    let is_ilike = op.as_str().to_lowercase().contains("ilike");
                    return Ok(ParseExpression::Like {
                        left: Box::new(lhs),
                        right: Box::new(rhs),
                        escape: None,
                        is_ilike
                    })
                },
                Rule::Similar => {
                    return Ok(ParseExpression::Similar {
                        left: Box::new(lhs),
                        right: Box::new(rhs),
                        escape: None,
                    })
                },
                Rule::Between => {
                    let mut op_inner = op.into_inner();
                    is_not = op_inner.next().is_some();
                    return Ok(ParseExpression::InterimBetween {
                        is_not,
                        left: Box::new(lhs),
                        right: Box::new(rhs),
                    })
                },
                Rule::Escape => ParseExpressionInfixOperator::Escape,
                Rule::Eq => ParseExpressionInfixOperator::InfixBool(Bool::Eq),
                Rule::NotEq => ParseExpressionInfixOperator::InfixBool(Bool::NotEq),
                Rule::Lt => ParseExpressionInfixOperator::InfixBool(Bool::Lt),
                Rule::LtEq => ParseExpressionInfixOperator::InfixBool(Bool::LtEq),
                Rule::Gt => ParseExpressionInfixOperator::InfixBool(Bool::Gt),
                Rule::GtEq => ParseExpressionInfixOperator::InfixBool(Bool::GtEq),
                Rule::In => {
                    let mut op_inner = op.into_inner();
                    is_not = op_inner.next().is_some_and(|i| matches!(i.as_rule(), Rule::NotFlag));
                    ParseExpressionInfixOperator::InfixBool(Bool::In)
                }
                Rule::Subtract      => ParseExpressionInfixOperator::InfixArithmetic(Arithmetic::Subtract),
                Rule::Divide        => ParseExpressionInfixOperator::InfixArithmetic(Arithmetic::Divide),
                Rule::Multiply      => ParseExpressionInfixOperator::InfixArithmetic(Arithmetic::Multiply),
                Rule::Add        => ParseExpressionInfixOperator::InfixArithmetic(Arithmetic::Add),
                Rule::ConcatInfixOp => ParseExpressionInfixOperator::Concat,
                rule           => unreachable!("Expr::parse expected infix operation, found {:?}", rule),
            };

            // HACK: InterimBetween(e1, e2) AND e3 => FinalBetween(e1, e2, e3).
            if matches!(op, ParseExpressionInfixOperator::InfixBool(Bool::And)) {
                if let Some((expr, is_exact_match)) = find_interim_between(&mut lhs) {
                    let ParseExpression::InterimBetween { is_not, left, right } = expr else {
                        panic!("expected ParseExpression::InterimBetween");
                    };

                    let fb = ParseExpression::FinalBetween {
                        is_not: *is_not,
                        left: left.clone(),
                        center: right.clone(),
                        right: Box::new(rhs),
                    };

                    if is_exact_match {
                        return Ok(fb);
                    }
                    *expr = fb;
                    return Ok(lhs);
                }
            }
            if matches!(op, ParseExpressionInfixOperator::Escape) {
                return connect_escape_to_like_node(lhs, rhs)
            }

            Ok(ParseExpression::Infix {
                op,
                is_not,
                left: Box::new(lhs),
                right: Box::new(rhs),
            })
        })
        .map_prefix(|op, child| {
            let op = match op.as_rule() {
                Rule::UnaryNot => Unary::Not,
                rule => unreachable!("Expr::parse expected prefix operator, found {:?}", rule),
            };
            Ok(ParseExpression::Prefix { op, child: Box::new(child?)})
        })
        .map_postfix(|child, op| {
            let child = child?;
            match op.as_rule() {
                Rule::CastPostfix => {
                    let ty_pair = op.into_inner().next()
                        .expect("Expected ColumnDefType under CastPostfix.");
                    let cast_type = cast_type_from_pair(ty_pair)?;
                    Ok(ParseExpression::Cast { child: Box::new(child), cast_type })
                }
                Rule::IsPostfix => {
                    let mut inner = op.into_inner();
                    let (is_not, value_index) = match inner.len() {
                        2 => (true, 1),
                        1 => (false, 0),
                        _ => unreachable!("Is must have 1 or 2 children")
                    };
                    let value_rule = inner
                        .nth(value_index)
                        .expect("Value must be present under Is")
                        .as_rule();
                    let value = match value_rule {
                        Rule::True => Some(true),
                        Rule::False => Some(false),
                        Rule::Unknown | Rule::Null => None,
                        _ => unreachable!("Is value must be TRUE, FALSE, NULL or UNKNOWN")
                    };
                    Ok(ParseExpression::Is { is_not, child: Box::new(child), value })
                }
                rule => unreachable!("Expr::parse expected postfix operator, found {:?}", rule),
            }
        })
        .parse(expression_pairs)
}

// Mapping between pest's Pair and corresponding id
// of the ast node. This map stores only ids for
// possible select child (Projection, OrderBy)
pub(crate) type SelectChildPairTranslation = HashMap<(usize, usize), usize>;

fn parse_select_set_pratt(
    select_pairs: Pairs<Rule>,
    pos_to_plan_id: &SelectChildPairTranslation,
    ast_to_plan: &Translation,
) -> Result<SelectSet, SbroadError> {
    SELECT_PRATT_PARSER
        .map_primary(|primary| {
            let select_set = match primary.as_rule() {
                Rule::Select => {
                    let mut pairs = primary.into_inner();
                    let mut select_child_pair =
                        pairs.next().expect("select must have at least one child");
                    assert!(matches!(select_child_pair.as_rule(), Rule::Projection));
                    for pair in pairs {
                        if let Rule::OrderBy = pair.as_rule() {
                            select_child_pair = pair;
                            break;
                        }
                    }
                    let ast_id = pos_to_plan_id.get(&select_child_pair.line_col()).unwrap();
                    let id = ast_to_plan.get(*ast_id)?;
                    SelectSet::PlanId { plan_id: id }
                }
                Rule::SelectWithOptionalContinuation => {
                    parse_select_set_pratt(primary.into_inner(), pos_to_plan_id, ast_to_plan)?
                }
                rule => unreachable!("Select::parse expected atomic rule, found {:?}", rule),
            };
            Ok(select_set)
        })
        .map_infix(|lhs, op, rhs| {
            let op = match op.as_rule() {
                Rule::UnionOp => SelectOp::Union,
                Rule::UnionAllOp => SelectOp::UnionAll,
                Rule::ExceptOp => SelectOp::Except,
                rule => unreachable!("Expr::parse expected infix operation, found {:?}", rule),
            };
            Ok(SelectSet::Infix {
                op,
                left: Box::new(lhs?),
                right: Box::new(rhs?),
            })
        })
        .parse(select_pairs)
}

fn parse_expr_no_type_check<M>(
    expression_pairs: Pairs<Rule>,
    referred_relation_ids: &[NodeId],
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
    safe_for_volatile_function: bool,
) -> Result<NodeId, SbroadError>
where
    M: Metadata,
{
    let parse_expr = parse_expr_pratt(
        expression_pairs,
        referred_relation_ids,
        worker,
        plan,
        safe_for_volatile_function,
    )?;
    parse_expr.populate_plan(plan, worker)
}

/// Parse expression pair and get plan id.
/// * Retrieve expressions tree-like structure using `parse_expr_pratt`
/// * Traverse tree to populate plan with new nodes
/// * Return `plan_id` of root Expression node
fn parse_scalar_expr<M>(
    expression_pairs: Pairs<Rule>,
    type_analyzer: &mut TypeAnalyzer,
    referred_relation_ids: &[NodeId],
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
    safe_for_volatile_function: bool,
) -> Result<NodeId, SbroadError>
where
    M: Metadata,
{
    let expr_id = parse_expr_no_type_check(
        expression_pairs,
        referred_relation_ids,
        worker,
        plan,
        safe_for_volatile_function,
    )?;
    type_system::analyze_scalar_expr(type_analyzer, expr_id, plan, &worker.subquery_replaces)?;
    Ok(expr_id)
}

fn parse_values_rows<M>(
    rows: &[usize],
    type_analyzer: &mut TypeAnalyzer,
    pairs_map: &mut ParsingPairsMap,
    col_idx: &mut usize,
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
) -> Result<Vec<NodeId>, SbroadError>
where
    M: Metadata,
{
    let mut values_rows_ids: Vec<NodeId> = Vec::with_capacity(rows.len());
    for ast_row_id in rows {
        let row_pair = pairs_map.remove_pair(*ast_row_id);
        // Don't check types until we have all rows.
        // Consider the following queries:
        //  - `VALUES (1), ('text')`: both rows are fine, but their types cannot be matched
        //  - `VALUES ($1), (1)`: to infer parameter type in the 1st row we need the 2nd row
        let expr_id = parse_expr_no_type_check(Pairs::single(row_pair), &[], worker, plan, true)?;
        let values_row_id = plan.add_values_row(expr_id, col_idx)?;
        plan.fix_subquery_rows(worker, values_row_id)?;
        values_rows_ids.push(values_row_id);
    }

    type_system::analyze_values_rows(
        type_analyzer,
        &values_rows_ids,
        plan,
        &worker.subquery_replaces,
    )?;

    Ok(values_rows_ids)
}

fn parse_select(
    select_pairs: Pairs<Rule>,
    pos_to_ast_id: &SelectChildPairTranslation,
    ast_to_plan: &Translation,
    plan: &mut Plan,
) -> Result<NodeId, SbroadError> {
    let select_set = parse_select_set_pratt(select_pairs, pos_to_ast_id, ast_to_plan)?;
    select_set.populate_plan(plan)
}

fn parse_create_plugin(
    ast: &AbstractSyntaxTree,
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

fn parse_drop_plugin(
    ast: &AbstractSyntaxTree,
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

fn parse_alter_plugin(
    ast: &AbstractSyntaxTree,
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

/// Generate an alias for the unnamed projection expressions.
#[must_use]
pub fn get_unnamed_column_alias(pos: usize) -> SmolStr {
    format_smolstr!("col_{pos}")
}

/// Map of { `AbstractSyntaxTree` node id -> parsing pairs copy, corresponding to ast node }.
struct ParsingPairsMap<'pairs_map> {
    inner: HashMap<usize, Pair<'pairs_map, Rule>>,
}

impl<'pairs_map> ParsingPairsMap<'pairs_map> {
    fn new() -> Self {
        Self {
            inner: HashMap::with_capacity(PARSING_PAIRS_MAP_CAPACITY),
        }
    }

    fn insert(
        &mut self,
        key: usize,
        value: Pair<'pairs_map, Rule>,
    ) -> Option<Pair<'pairs_map, Rule>> {
        self.inner.insert(key, value)
    }

    fn remove_pair(&mut self, key: usize) -> Pair<Rule> {
        self.inner
            .remove(&key)
            .expect("pairs_map doesn't contain value for key")
    }
}

/// Hash map of { pair -> ast_id }, where
/// * pair (key) -- inner pest structure for AST node
/// * ast_id (value) -- id of our AST node.
type PairToAstIdTranslation<'i> = HashMap<Pair<'i, Rule>, usize>;

impl AbstractSyntaxTree {
    /// Build an empty AST.
    fn empty() -> Self {
        AbstractSyntaxTree {
            nodes: ParseNodes::new(),
            top: None,
            map: HashMap::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self == &Self::empty()
    }

    /// Constructor.
    /// Builds a tree (nodes are in postorder reverse).
    /// Builds abstract syntax tree (AST) from SQL query.
    ///
    /// # Errors
    /// - Failed to parse an SQL query.
    fn fill<'query>(
        &mut self,
        query: &'query str,
        pairs_map: &mut ParsingPairsMap<'query>,
        pos_to_ast_id: &mut SelectChildPairTranslation,
        pairs_to_ast_id: &mut PairToAstIdTranslation<'query>,
        parameters_ordered: &mut Vec<Pair<'query, Rule>>,
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

        let mut parameters = Vec::new();
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
                    //   function call on the stage of `resolve_metadata`.
                    // * `Row`s are added to support parsing Row expressions under `Values` nodes.
                    // * `Literal`s are added to support procedure calls and
                    //   ALTER SYSTEM which should not contain all possible `Expr`s.
                    // * `SelectWithOptionalContinuation` is also parsed using Pratt parser
                    pairs_map.insert(arena_node_id, stack_node.pair.clone());
                    if let Rule::Parameter = stack_node.pair.as_rule() {
                        parameters.push(stack_node.pair.clone());
                    }
                }
                Rule::Projection | Rule::OrderBy => {
                    pos_to_ast_id.insert(stack_node.pair.line_col(), arena_node_id);
                }
                Rule::SubQuery => {
                    pairs_to_ast_id.insert(stack_node.pair.clone(), arena_node_id);
                }
                _ => {}
            }

            for parse_child in stack_node.pair.into_inner() {
                stack.push(StackParseNode::new(parse_child, Some(arena_node_id)));
            }
        }

        // Sort parameters pairs in their appearance order.
        // This allows to enumerate tnt parameters: `select ?, ?` -> `select $1, $2`
        parameters.sort_by_key(|p| p.line_col());
        *parameters_ordered = parameters;

        self.set_top(0)?;

        self.transform_update()?;
        self.transform_delete()?;
        self.transform_select()?;
        Ok(())
    }

    fn parse_projection<M: Metadata>(
        &self,
        plan: &mut Plan,
        node_id: usize,
        type_analyzer: &mut TypeAnalyzer,
        map: &mut Translation,
        pairs_map: &mut ParsingPairsMap,
        worker: &mut ExpressionsWorker<M>,
    ) -> Result<(), SbroadError> {
        let node = self.nodes.get_node(node_id)?;
        let Some((rel_child_id, mut other_children_ids)) = node.children.split_first() else {
            return Err(SbroadError::Invalid(
                Entity::Query,
                Some("Projection must have some children".into()),
            ));
        };
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

        let mut named_windows = HashMap::new();
        let rel_child = plan.get_relation_node(plan_rel_child_id)?;
        if let Relational::NamedWindows(NamedWindows {
            windows: window_ids,
            ..
        }) = rel_child
        {
            named_windows.reserve(window_ids.len());
            for window_id in window_ids {
                let window = plan.get_expression_node(*window_id)?;
                let Expression::Window(Window { name, .. }) = window else {
                    panic!("Expected Window node, got {:?}", window);
                };
                let name = name.as_ref().expect("Window name must be set").clone();
                type_system::analyze_scalar_expr(
                    type_analyzer,
                    *window_id,
                    plan,
                    &worker.subquery_replaces,
                )?;
                named_windows.insert(name, *window_id);
            }
        }
        worker.named_windows_map = named_windows;

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
                                let (col_name, _) = worker
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
        let windows = std::mem::take(&mut worker.windows);

        let projection_id =
            plan.add_proj_internal(vec![plan_rel_child_id], &proj_columns, is_distinct, windows)?;

        plan.fix_subquery_rows(worker, projection_id)?;
        map.add(node_id, projection_id);
        Ok(())
    }

    fn parse_named_windows<M: Metadata>(
        &self,
        plan: &mut Plan,
        node_id: usize,
        type_analyzer: &mut TypeAnalyzer,
        map: &mut Translation,
        pairs_map: &mut ParsingPairsMap,
        worker: &mut ExpressionsWorker<M>,
    ) -> Result<(), SbroadError> {
        let node = self.nodes.get_node(node_id)?;
        let Some((child_id, window_def_ids)) = node.children.split_first() else {
            panic!("NamedWindow must have at least 1 child and 1 WindowDef");
        };
        let plan_rel_child_id = map.get(*child_id)?;
        let mut windows = Vec::with_capacity(window_def_ids.len());
        for def_id in window_def_ids {
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
            let window = self.parse_window_body(
                Some(window_name),
                plan_rel_child_id,
                type_analyzer,
                plan,
                *window_body_id,
                map,
                pairs_map,
                worker,
            )?;
            windows.push(window);
        }
        let named_windows_id = plan.add_named_windows(plan_rel_child_id, windows)?;
        plan.fix_subquery_rows(worker, named_windows_id)?;
        map.add(node_id, named_windows_id);
        Ok(())
    }

    fn parse_select_without_scan<M: Metadata>(
        &self,
        plan: &mut Plan,
        node_id: usize,
        type_analyzer: &mut TypeAnalyzer,
        map: &mut Translation,
        pairs_map: &mut ParsingPairsMap,
        worker: &mut ExpressionsWorker<M>,
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
                            let (col_name, _) = worker
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
        worker: &mut ExpressionsWorker<M>,
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
                        if matches!(col_type.get(), Some(Type::Array)) {
                            return Err(SbroadError::Invalid(
                                Entity::Expression,
                                Some(format_smolstr!("Array is not supported as a sort type for ORDER BY"))
                            ));
                        }
                    }

                    if let Expression::Constant(Constant {value: Value::Unsigned(index)}) = expr {
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
                                    if matches!(col_type.get(), Some(Type::Array)) {
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
                        let mut expr_tree =
                            PostOrder::with_capacity(|node| plan.nodes.expr_iter(node, false), EXPR_CAPACITY);
                        let mut reference_met = false;
                        for LevelNode(_, node_id) in expr_tree.iter(expr_plan_node_id) {
                            if let Expression::Reference(Reference { targets, .. }) = plan.get_expression_node(node_id)? {
                                if targets.is_some() {
                                    // Subquery reference met.
                                    reference_met = true;
                                    break
                                }
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

            let order_type =
                if let Some(order_type_child_node_id) = order_by_element_node.children.get(1) {
                    let order_type_node = self.nodes.get_node(*order_type_child_node_id)?;
                    let order_type = match order_type_node.rule {
                        Rule::Asc => OrderByType::Asc,
                        Rule::Desc => OrderByType::Desc,
                        rule => unreachable!(
                            "{}",
                            format!("Unexpected rule met under OrderByElement: {rule:?}")
                        ),
                    };
                    Some(order_type)
                } else {
                    None
                };

            order_by_elements.push(OrderByElement { entity, order_type });
        }
        Ok(order_by_elements)
    }

    fn parse_order_by<M: Metadata>(
        &self,
        plan: &mut Plan,
        node_id: usize,
        type_analyzer: &mut TypeAnalyzer,
        map: &mut Translation,
        pairs_map: &mut ParsingPairsMap,
        worker: &mut ExpressionsWorker<M>,
    ) -> Result<(), SbroadError> {
        let node = self.nodes.get_node(node_id)?;
        let projection_ast_id = node.children.first().expect("OrderBy has no children.");
        let projection_plan_id = map.get(*projection_ast_id)?;

        let sq_plan_id = plan.add_sub_query(projection_plan_id, None)?;

        let order_by_elements_ids: Vec<usize> = node.children.clone().into_iter().skip(1).collect();
        let order_by_elements = self.parse_order_by_elements(
            plan,
            projection_plan_id,
            &order_by_elements_ids,
            type_analyzer,
            pairs_map,
            worker,
        )?;
        let (order_by_id, plan_node_id) = plan.add_order_by(sq_plan_id, order_by_elements)?;
        plan.fix_subquery_rows(worker, order_by_id)?;
        map.add(node_id, plan_node_id);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_window_body<M: Metadata>(
        &self,
        window_name: Option<SmolStr>,
        proj_child_id: NodeId,
        type_analyzer: &mut TypeAnalyzer,
        plan: &mut Plan,
        node_id: usize,
        map: &mut Translation,
        pairs_map: &mut ParsingPairsMap,
        worker: &mut ExpressionsWorker<M>,
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
            name: window_name,
            partition,
            ordering,
            frame,
        };

        let plan_window_by_id = plan.nodes.push(window.into());
        map.add(node_id, plan_window_by_id);

        Ok(plan_window_by_id)
    }

    /// Function that transforms `AbstractSyntaxTree` into `Plan`.
    /// Build a plan from the AST with parameters as placeholders for the values.
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::uninlined_format_args)]
    fn resolve_metadata<M>(
        &self,
        metadata: &M,
        pairs_map: &mut ParsingPairsMap,
        pos_to_ast_id: &mut SelectChildPairTranslation,
        sq_pair_to_ast_ids: &PairToAstIdTranslation,
        parameters_positions: Vec<Pair<'_, Rule>>,
    ) -> Result<Plan, SbroadError>
    where
        M: Metadata,
    {
        let mut plan = Plan::default();

        let mut type_analyzer = type_system::new_analyzer();

        let Some(top) = self.top else {
            return Err(SbroadError::Invalid(Entity::AST, None));
        };
        let capacity = self.nodes.arena.len();
        let mut dft_post = PostOrder::with_capacity(|node| self.nodes.ast_iter(node), capacity);
        // Map of { ast `ParseNode` id -> plan `Node` id }.
        let mut map = Translation::with_capacity(self.nodes.next_id());
        // Counter for `Expression::ValuesRow` output column name aliases ("COLUMN_<`col_idx`>").
        // Is it global for every `ValuesRow` met in the AST.
        let mut col_idx: usize = 0;
        let mut worker = ExpressionsWorker::new(metadata, sq_pair_to_ast_ids, parameters_positions);
        let mut ctes = CTEs::new();
        // This flag disables resolving of table names for DROP TABLE queries,
        // as it can be used with tables that are not presented in metadata.
        // Unresolved table names are handled in picodata depending in IF EXISTS options.
        let resolve_table_names = self.nodes.get_node(top)?.rule != Rule::DropTable;

        for level_node in dft_post.iter(top) {
            let id = level_node.1;
            let node = self.nodes.get_node(id)?;
            match &node.rule {
                Rule::Scan => {
                    let rel_child_id_ast = node
                        .children
                        .first()
                        .expect("could not find first child id in scan node");
                    let rel_child_id_plan = map.get(*rel_child_id_ast)?;
                    let rel_child_node = plan.get_relation_node(rel_child_id_plan)?;

                    map.add(id, rel_child_id_plan);
                    if let Some(ast_alias_id) = node.children.get(1) {
                        let alias_name = parse_normalized_identifier(self, *ast_alias_id)?;
                        // CTE scans can have different aliases, so clone the CTE scan node,
                        // preserving its subtree.
                        if let Relational::ScanCte(ScanCte { child, .. }) = rel_child_node {
                            let scan_id = plan.add_cte(*child, alias_name, vec![])?;
                            map.add(id, scan_id);
                        } else {
                            let mut scan = plan.get_mut_relation_node(rel_child_id_plan)?;
                            scan.set_scan_name(Some(alias_name.to_smolstr()))?;
                        }
                    }
                }
                Rule::ScanTable => {
                    let ast_table_id = node
                        .children
                        .first()
                        .expect("could not find first child id in scan table node");
                    let scan_id = plan.add_scan(
                        parse_normalized_identifier(self, *ast_table_id)?.as_str(),
                        None,
                    )?;
                    map.add(id, scan_id);
                }
                Rule::ScanCteOrTable => {
                    parse_scan_cte_or_table(self, metadata, id, &mut map, &mut ctes, &mut plan)?;
                }
                Rule::Cte => {
                    parse_cte(self, id, &mut map, &mut ctes, &mut plan)?;
                }
                Rule::Table if resolve_table_names => {
                    // The thing is we don't want to normalize name.
                    // Should we fix `parse_identifier` or `table` logic?
                    let table_name = parse_identifier(self, id)?;
                    let t = metadata.table(&table_name)?;
                    plan.add_rel(t);
                }
                Rule::SubQuery => {
                    let ast_child_id = node
                        .children
                        .first()
                        .expect("child node id is not found among sub-query children.");
                    let plan_child_id = map.get(*ast_child_id)?;
                    let plan_sq_id = plan.add_sub_query(plan_child_id, None)?;
                    worker.sq_ast_to_plan_id.add(id, plan_sq_id);
                    map.add(id, plan_sq_id);
                }
                Rule::WindowBody => {
                    // WindowBody will be parsed under Projection.
                }
                Rule::MotionRowMax => {
                    let ast_child_id = node
                        .children
                        .first()
                        .expect("no children for sql_vdbe_opcode_max option");
                    let val = parse_option(self, *ast_child_id, pairs_map, &mut worker, &mut plan)?;
                    plan.raw_options.push(OptionSpec {
                        kind: OptionKind::MotionRowMax,
                        val,
                    });
                }
                Rule::VdbeOpcodeMax => {
                    let ast_child_id = node
                        .children
                        .first()
                        .expect("no children for sql_vdbe_opcode_max option");
                    let val = parse_option(self, *ast_child_id, pairs_map, &mut worker, &mut plan)?;

                    plan.raw_options.push(OptionSpec {
                        kind: OptionKind::VdbeOpcodeMax,
                        val,
                    });
                }
                Rule::GroupBy => {
                    // Reminder: first GroupBy child in `node.children` is always a relational node.
                    let mut children: Vec<NodeId> = Vec::with_capacity(node.children.len());
                    let first_relational_child_ast_id =
                        node.children.first().expect("GroupBy has no children");
                    let first_relational_child_plan_id = map.get(*first_relational_child_ast_id)?;
                    children.push(first_relational_child_plan_id);
                    for ast_column_id in node.children.iter().skip(1) {
                        let expr_pair = pairs_map.remove_pair(*ast_column_id);
                        let expr_id = parse_scalar_expr(
                            Pairs::single(expr_pair),
                            &mut type_analyzer,
                            &[first_relational_child_plan_id],
                            &mut worker,
                            &mut plan,
                            false,
                        )?;

                        children.push(expr_id);
                    }
                    let groupby_id = plan.add_groupby_from_ast(&children)?;
                    plan.fix_subquery_rows(&mut worker, groupby_id)?;
                    map.add(id, groupby_id);
                }
                Rule::Join => {
                    // Reminder: Join structure = [left child, kind, right child, condition expr]
                    let ast_left_id = node.children.first().expect("Join has no children.");
                    let plan_left_id = map.get(*ast_left_id)?;

                    let ast_kind_id = node
                        .children
                        .get(1)
                        .expect("Kind not found among Join children.");
                    let ast_kind_node = self.nodes.get_node(*ast_kind_id)?;
                    let kind = match ast_kind_node.rule {
                        Rule::LeftJoinKind => JoinKind::LeftOuter,
                        Rule::InnerJoinKind => JoinKind::Inner,
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::AST,
                                Some(format_smolstr!(
                                    "expected join kind node as 1 child of join. Got: {:?}",
                                    ast_kind_node,
                                )),
                            ))
                        }
                    };

                    let ast_right_id = node
                        .children
                        .get(2)
                        .expect("Right not found among Join children.");
                    let plan_right_id = map.get(*ast_right_id)?;

                    let ast_expr_id = node
                        .children
                        .get(3)
                        .expect("Condition not found among Join children");
                    let ast_expr = self.nodes.get_node(*ast_expr_id)?;
                    let cond_expr_child_id = ast_expr
                        .children
                        .first()
                        .expect("Expected to see child under Expr node");
                    let cond_expr_child = self.nodes.get_node(*cond_expr_child_id)?;
                    let condition_expr_id = if let Rule::True = cond_expr_child.rule {
                        plan.add_const(Value::Boolean(true))
                    } else {
                        let expr_pair = pairs_map.remove_pair(*ast_expr_id);
                        parse_scalar_expr(
                            Pairs::single(expr_pair),
                            &mut type_analyzer,
                            &[plan_left_id, plan_right_id],
                            &mut worker,
                            &mut plan,
                            false,
                        )?
                    };

                    let plan_join_id =
                        plan.add_join(plan_left_id, plan_right_id, condition_expr_id, kind)?;
                    plan.fix_subquery_rows(&mut worker, plan_join_id)?;
                    map.add(id, plan_join_id);
                }
                Rule::Having | Rule::Selection => {
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
                        &mut type_analyzer,
                        &[plan_rel_child_id],
                        &mut worker,
                        &mut plan,
                        false,
                    )?;

                    let plan_node_id = match &node.rule {
                        Rule::Selection => {
                            plan.add_select(&[plan_rel_child_id], expr_plan_node_id)?
                        }
                        Rule::Having => plan.add_having(&[plan_rel_child_id], expr_plan_node_id)?,
                        _ => panic!("Expected to see Selection or Having."),
                    };
                    plan.fix_subquery_rows(&mut worker, plan_node_id)?;
                    map.add(id, plan_node_id);
                }
                Rule::OrderBy => {
                    self.parse_order_by(
                        &mut plan,
                        id,
                        &mut type_analyzer,
                        &mut map,
                        pairs_map,
                        &mut worker,
                    )?;
                }
                Rule::SelectWithOptionalContinuation => {
                    let select_pair = pairs_map.remove_pair(id);
                    let select_plan_node_id =
                        parse_select(Pairs::single(select_pair), pos_to_ast_id, &map, &mut plan)?;
                    map.add(id, select_plan_node_id);
                }
                Rule::SelectFull => {
                    parse_select_full(self, id, &mut map, &mut plan)?;
                }
                Rule::Projection => {
                    let is_without_scan = {
                        let child_node = self.nodes.get_node(node.first_child())?;
                        // Check that child is not relational, distinct and asterisk
                        // variants are handled in `parser_select_without_scan`
                        matches!(
                            child_node.rule,
                            Rule::Column | Rule::Asterisk | Rule::Distinct
                        )
                    };
                    if is_without_scan {
                        self.parse_select_without_scan(
                            &mut plan,
                            id,
                            &mut type_analyzer,
                            &mut map,
                            pairs_map,
                            &mut worker,
                        )?;
                    } else {
                        self.parse_projection(
                            &mut plan,
                            id,
                            &mut type_analyzer,
                            &mut map,
                            pairs_map,
                            &mut worker,
                        )?;
                    }
                }
                Rule::NamedWindows => {
                    self.parse_named_windows(
                        &mut plan,
                        id,
                        &mut type_analyzer,
                        &mut map,
                        pairs_map,
                        &mut worker,
                    )?;
                }
                Rule::Values => {
                    let values_rows_ids = parse_values_rows(
                        &node.children,
                        &mut type_analyzer,
                        pairs_map,
                        &mut col_idx,
                        &mut worker,
                        &mut plan,
                    )?;
                    let plan_values_id = plan.add_values(values_rows_ids)?;
                    map.add(id, plan_values_id);
                }
                Rule::Update => {
                    let rel_child_ast_id = node
                        .children
                        .first()
                        .expect("Update must have at least two children.");
                    let rel_child_id = map.get(*rel_child_ast_id)?;

                    let ast_scan_table_id = node
                        .children
                        .get(1)
                        .expect("Update must have at least two children.");
                    let plan_scan_id = map.get(*ast_scan_table_id)?;
                    let plan_scan_node = plan.get_relation_node(plan_scan_id)?;
                    let scan_relation =
                        if let Relational::ScanRelation(ScanRelation { relation, .. }) =
                            plan_scan_node
                        {
                            relation.clone()
                        } else {
                            unreachable!("Scan expected under Update")
                        };

                    let update_list_id = node
                        .children
                        .get(2)
                        .expect("Update lise expected as a second child of Update");
                    let update_list = self.nodes.get_node(*update_list_id)?;

                    // Maps position of column in table to corresponding update expression
                    let mut update_defs: HashMap<ColumnPosition, ExpressionId, RepeatableState> =
                        HashMap::with_capacity_and_hasher(
                            update_list.children.len(),
                            RepeatableState,
                        );
                    // Map of { column_name -> (column_role, column_position) }.
                    let mut col_name_to_position_map: HashMap<&str, (&ColumnRole, usize)> =
                        HashMap::new();

                    let relation = plan
                        .relations
                        .get(&scan_relation)
                        .ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::Table,
                                format_smolstr!("{scan_relation} among plan relations"),
                            )
                        })?
                        .clone();
                    relation.columns.iter().enumerate().for_each(|(i, c)| {
                        col_name_to_position_map.insert(c.name.as_str(), (c.get_role(), i));
                    });

                    let mut pk_positions: HashSet<usize> =
                        HashSet::with_capacity(relation.primary_key.positions.len());
                    relation.primary_key.positions.iter().for_each(|pos| {
                        pk_positions.insert(*pos);
                    });
                    for update_item_id in &update_list.children {
                        let update_item = self.nodes.get_node(*update_item_id)?;
                        let ast_column_id = update_item
                            .children
                            .first()
                            .expect("Column expected as first child of UpdateItem");
                        let expr_ast_id = update_item
                            .children
                            .get(1)
                            .expect("Expression expected as second child of UpdateItem");

                        let expr_pair = pairs_map.remove_pair(*expr_ast_id);
                        let expr_plan_node_id = parse_scalar_expr(
                            Pairs::single(expr_pair),
                            &mut type_analyzer,
                            &[rel_child_id],
                            &mut worker,
                            &mut plan,
                            true,
                        )?;

                        if plan.contains_aggregates(expr_plan_node_id, true)? {
                            return Err(SbroadError::Invalid(
                                Entity::Query,
                                Some(
                                    "aggregate functions are not supported in update expression."
                                        .into(),
                                ),
                            ));
                        }
                        let col_name = parse_normalized_identifier(self, *ast_column_id)?;
                        match col_name_to_position_map.get(col_name.as_str()) {
                            Some((&ColumnRole::User, pos)) => {
                                if pk_positions.contains(pos) {
                                    return Err(SbroadError::Invalid(
                                        Entity::Query,
                                        Some(format_smolstr!(
                                            "it is illegal to update primary key column: {}",
                                            to_user(col_name)
                                        )),
                                    ));
                                }
                                if update_defs.contains_key(pos) {
                                    return Err(SbroadError::Invalid(
                                        Entity::Query,
                                        Some(format_smolstr!(
                                            "The same column is specified twice in update list: {}",
                                            to_user(col_name)
                                        )),
                                    ));
                                }
                                update_defs.insert(*pos, expr_plan_node_id);
                            }
                            Some((&ColumnRole::Sharding, _)) => {
                                return Err(SbroadError::FailedTo(
                                    Action::Update,
                                    Some(Entity::Column),
                                    format_smolstr!(
                                        "system column {} cannot be updated",
                                        to_user(col_name)
                                    ),
                                ))
                            }
                            None => {
                                return Err(SbroadError::NotFound(
                                    Entity::Column,
                                    (*col_name).to_smolstr(),
                                ))
                            }
                        }
                    }
                    let (proj_id, update_id) =
                        plan.add_update(&scan_relation, &update_defs, rel_child_id)?;
                    plan.fix_subquery_rows(&mut worker, proj_id)?;
                    map.add(id, update_id);
                }
                Rule::Delete => {
                    // Get table name and selection plan node id.
                    // Reminder: first child of Delete is a `ScanTable` or `DeleteFilter`
                    //           (under which there must be a `ScanTable`).
                    let first_child_id = node
                        .children
                        .first()
                        .expect("Delete must have at least one child");
                    let first_child_node = self.nodes.get_node(*first_child_id)?;
                    let (proj_child_id, table_name) = match first_child_node.rule {
                        Rule::ScanTable => {
                            let plan_scan_id = map.get(*first_child_id)?;
                            let plan_scan_node = plan.get_relation_node(plan_scan_id)?;
                            let Relational::ScanRelation(ScanRelation { relation, .. }) =
                                plan_scan_node
                            else {
                                unreachable!("Scan expected under ScanTable")
                            };

                            // See below where `proj_child_id`` is used. We don't want to apply such
                            // an optimization for global tables yet (see issue
                            // https://git.picodata.io/picodata/sbroad/-/issues/861).
                            let proj_child_id = if metadata.table(relation)?.is_global() {
                                Some(plan_scan_id)
                            } else {
                                None
                            };
                            (proj_child_id, relation.clone())
                        }
                        Rule::DeleteFilter => {
                            let ast_table_id = first_child_node
                                .children
                                .first()
                                .expect("Table not found among DeleteFilter children");
                            let plan_scan_id = map.get(*ast_table_id)?;
                            let plan_scan_node = plan.get_relation_node(plan_scan_id)?;
                            let relation_name =
                                if let Relational::ScanRelation(ScanRelation { relation, .. }) =
                                    plan_scan_node
                                {
                                    relation.clone()
                                } else {
                                    unreachable!("Scan expected under DeleteFilter")
                                };

                            let ast_expr_id = first_child_node
                                .children
                                .get(1)
                                .expect("Expr not found among DeleteFilter children");
                            let expr_pair = pairs_map.remove_pair(*ast_expr_id);
                            let expr_plan_node_id = parse_scalar_expr(
                                Pairs::single(expr_pair),
                                &mut type_analyzer,
                                &[plan_scan_id],
                                &mut worker,
                                &mut plan,
                                false,
                            )?;

                            let plan_select_id =
                                plan.add_select(&[plan_scan_id], expr_plan_node_id)?;
                            plan.fix_subquery_rows(&mut worker, plan_select_id)?;
                            (Some(plan_select_id), relation_name)
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::Node,
                                Some(format_smolstr!(
                                    "AST delete node {:?} contains unexpected children",
                                    first_child_node,
                                )),
                            ));
                        }
                    };

                    let table = metadata.table(&table_name)?;

                    // In case of DELETE without filter we don't want to store
                    // it's Projection + Scan relational children.
                    let plan_proj_id = if let Some(proj_child_id) = proj_child_id {
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
                            false,
                        )?;
                        let mut alias_ids = Vec::with_capacity(pk_column_ids.len());
                        for (pk_pos, pk_column_id) in pk_column_ids.iter().enumerate() {
                            let pk_alias_id = plan
                                .nodes
                                .add_alias(&format!("pk_col_{pk_pos}"), *pk_column_id)?;
                            alias_ids.push(pk_alias_id);
                        }
                        Some(plan.add_proj_internal(
                            vec![proj_child_id],
                            &alias_ids,
                            false,
                            vec![],
                        )?)
                    } else {
                        None
                    };

                    let plan_delete_id = plan.add_delete(table.name, plan_proj_id)?;
                    map.add(id, plan_delete_id);
                }
                Rule::Insert => {
                    let ast_table_id = node.children.first().expect("Insert has no children.");
                    let relation = parse_normalized_identifier(self, *ast_table_id)?;

                    let ast_child_id = node
                        .children
                        .get(1)
                        .expect("Second child not found among Insert children");
                    let get_conflict_strategy =
                        |child_idx: usize| -> Result<ConflictStrategy, SbroadError> {
                            let Some(child_id) = node.children.get(child_idx).copied() else {
                                return Ok(ConflictStrategy::DoFail);
                            };
                            let rule = &self.nodes.get_node(child_id)?.rule;
                            let res = match rule {
                                Rule::DoNothing => ConflictStrategy::DoNothing,
                                Rule::DoReplace => ConflictStrategy::DoReplace,
                                Rule::DoFail => ConflictStrategy::DoFail,
                                _ => {
                                    return Err(SbroadError::Invalid(
                                        Entity::AST,
                                        Some(format_smolstr!(
                                            "expected conflict strategy on \
                                AST id ({child_id}). Got: {rule:?}"
                                        )),
                                    ))
                                }
                            };
                            Ok(res)
                        };
                    let ast_child = self.nodes.get_node(*ast_child_id)?;
                    let plan_insert_id = if let Rule::TargetColumns = ast_child.rule {
                        // insert into t (a, b, c) ...
                        let mut selected_col_names: Vec<SmolStr> =
                            Vec::with_capacity(ast_child.children.len());
                        for col_id in &ast_child.children {
                            selected_col_names
                                .push(parse_normalized_identifier(self, *col_id)?.to_smolstr());
                        }

                        let mut existing_col_names: HashSet<&SmolStr> =
                            HashSet::with_capacity(selected_col_names.len());
                        for selected_col in &selected_col_names {
                            if !existing_col_names.insert(selected_col) {
                                return Err(SbroadError::DuplicatedValue(format_smolstr!(
                                    "column {} specified more than once",
                                    to_user(selected_col)
                                )));
                            }
                        }

                        let rel = plan.relations.get(&relation).ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::Table,
                                format_smolstr!("{relation} among plan relations"),
                            )
                        })?;
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

                        let ast_rel_child_id = node
                            .children
                            .get(2)
                            .expect("Third child not found among Insert children");
                        let plan_rel_child_id = map.get(*ast_rel_child_id)?;
                        let conflict_strategy = get_conflict_strategy(3)?;
                        plan.add_insert(
                            &relation,
                            plan_rel_child_id,
                            &selected_col_names,
                            conflict_strategy,
                        )?
                    } else {
                        // insert into t ...
                        let plan_child_id = map.get(*ast_child_id)?;
                        let conflict_strategy = get_conflict_strategy(2)?;
                        plan.add_insert(&relation, plan_child_id, &[], conflict_strategy)?
                    };
                    map.add(id, plan_insert_id);
                }
                Rule::Explain => {
                    plan.mark_as_explain();

                    let ast_child_id = node.children.first().expect("Explain has no children.");
                    map.add(0, map.get(*ast_child_id)?);
                }
                Rule::Query => {
                    // Query may have two children:
                    // 1. select | insert | except | ..
                    // 2. Option child - for which no plan node is created
                    let child_id =
                        map.get(*node.children.first().expect("no children for Query rule"))?;
                    map.add(id, child_id);
                }
                Rule::Block => {
                    // Query may have two children:
                    // 1. call
                    // 2. Option child - for which no plan node is created
                    let child_id =
                        map.get(*node.children.first().expect("no children for Block rule"))?;
                    map.add(id, child_id);
                }
                Rule::CallProc => {
                    let call_proc = parse_call_proc(
                        self,
                        node,
                        &mut type_analyzer,
                        pairs_map,
                        &mut worker,
                        &mut plan,
                    )?;
                    let plan_id = plan.nodes.push(call_proc.into());
                    map.add(id, plan_id);
                }
                Rule::CreateIndex => {
                    let create_index = parse_create_index(self, node)?;
                    let plan_id = plan.nodes.push(create_index.into());
                    map.add(id, plan_id);
                }
                Rule::CreateSchema => {
                    let create_schema = DdlOwned::CreateSchema;
                    let plan_id = plan.nodes.push(create_schema.into());
                    map.add(id, plan_id);
                }
                Rule::AlterSystem => {
                    let alter_system = parse_alter_system(
                        self,
                        node,
                        &mut type_analyzer,
                        pairs_map,
                        &mut worker,
                        &mut plan,
                    )?;
                    let plan_id = plan.nodes.push(alter_system.into());
                    map.add(id, plan_id);
                }
                Rule::CreateProc => {
                    let create_proc = parse_create_proc(self, node)?;
                    let plan_id = plan.nodes.push(create_proc.into());
                    map.add(id, plan_id);
                }
                Rule::CreateTable => {
                    let create_sharded_table = parse_create_table(self, node)?;
                    let plan_id = plan.nodes.push(create_sharded_table.into());
                    map.add(id, plan_id);
                }
                Rule::CreatePartition => {
                    return Err(SbroadError::NotImplemented(
                        Entity::Rule,
                        format_smolstr!("PARTITION OF logic is not supported yet."),
                    ));
                }
                Rule::GrantPrivilege => {
                    let (grant_type, grantee_name, timeout) = parse_grant_revoke(node, self)?;
                    let grant_privilege = GrantPrivilege {
                        grant_type,
                        grantee_name,
                        timeout,
                    };
                    let plan_id = plan.nodes.push(grant_privilege.into());
                    map.add(id, plan_id);
                }
                Rule::RevokePrivilege => {
                    let (revoke_type, grantee_name, timeout) = parse_grant_revoke(node, self)?;
                    let revoke_privilege = RevokePrivilege {
                        revoke_type,
                        grantee_name,
                        timeout,
                    };
                    let plan_id = plan.nodes.push(revoke_privilege.into());
                    map.add(id, plan_id);
                }
                Rule::DropIndex => {
                    let drop_index = parse_drop_index(self, node)?;
                    let plan_id = plan.nodes.push(drop_index.into());
                    map.add(id, plan_id);
                }
                Rule::DropSchema => {
                    let drop_schema = DdlOwned::DropSchema;
                    let plan_id = plan.nodes.push(drop_schema.into());
                    map.add(id, plan_id);
                }
                Rule::DropRole => {
                    let mut name = None;
                    let mut timeout = get_default_timeout();
                    let mut if_exists = DEFAULT_IF_EXISTS;
                    for child_id in &node.children {
                        let child_node = self.nodes.get_node(*child_id)?;
                        match child_node.rule {
                            Rule::Identifier => name = Some(parse_identifier(self, *child_id)?),
                            Rule::IfExists => if_exists = true,
                            Rule::Timeout => timeout = get_timeout(self, *child_id)?,
                            _ => {
                                return Err(SbroadError::Invalid(
                                    Entity::Node,
                                    Some(format_smolstr!(
                                        "ACL node contains unexpected child: {child_node:?}",
                                    )),
                                ));
                            }
                        }
                    }
                    let name = name.expect("RoleName expected under DropRole node");
                    let drop_role = DropRole {
                        name,
                        if_exists,
                        timeout,
                    };

                    let plan_id = plan.nodes.push(drop_role.into());
                    map.add(id, plan_id);
                }
                Rule::DropTable => {
                    let drop_table = parse_drop_table(self, node)?;
                    let plan_id = plan.nodes.push(drop_table.into());
                    map.add(id, plan_id);
                }
                Rule::TruncateTable => {
                    let truncate_table = parse_truncate_table(self, node)?;
                    let plan_id = plan.nodes.push(truncate_table.into());
                    map.add(id, plan_id);
                }
                Rule::DropProc => {
                    let drop_proc = parse_drop_proc(self, node)?;
                    let plan_id = plan.nodes.push(drop_proc.into());
                    map.add(id, plan_id);
                }
                Rule::RenameProc => {
                    let rename_proc = parse_rename_proc(self, node)?;
                    let plan_id = plan.nodes.push(rename_proc.into());
                    map.add(id, plan_id);
                }
                Rule::AlterUser => {
                    let user_name_node_id = node
                        .children
                        .first()
                        .expect("RoleName expected as a first child");
                    let user_name = parse_identifier(self, *user_name_node_id)?;

                    let alter_option_node_id = node
                        .children
                        .get(1)
                        .expect("Some AlterOption expected as a second child");
                    let alter_option_node = self.nodes.get_node(*alter_option_node_id)?;
                    let alter_option = match alter_option_node.rule {
                        Rule::AlterLogin => AlterOption::Login,
                        Rule::AlterNoLogin => AlterOption::NoLogin,
                        Rule::AlterPassword => {
                            let pwd_or_ldap_node_id = alter_option_node
                                .children
                                .first()
                                .expect("Password expected as a first child");
                            let pwd_or_ldap_node = self.nodes.get_node(*pwd_or_ldap_node_id)?;

                            let password;
                            let mut auth_method;
                            match pwd_or_ldap_node.rule {
                                Rule::Ldap => {
                                    password = SmolStr::default();
                                    auth_method = AuthMethod::Ldap;
                                }
                                Rule::SingleQuotedString => {
                                    let password_literal =
                                        retrieve_string_literal(self, *pwd_or_ldap_node_id)?;
                                    password = escape_single_quotes(&password_literal);

                                    auth_method = DEFAULT_AUTH_METHOD;
                                    if let Some(auth_method_node_id) =
                                        alter_option_node.children.get(1)
                                    {
                                        let auth_method_node =
                                            self.nodes.get_node(*auth_method_node_id)?;
                                        auth_method = match auth_method_node.rule {
                                            method @ (Rule::ChapSha1 | Rule::Md5) => auth_method_from_auth_rule(method),
                                            _ => {
                                                return Err(SbroadError::Invalid(
                                                    Entity::Node,
                                                    Some(format_smolstr!(
                                                        "ACL node contains unexpected child: {pwd_or_ldap_node:?}",
                                                    )),
                                                ))
                                            }
                                        };
                                    }
                                }
                                _ => {
                                    return Err(SbroadError::Invalid(
                                        Entity::Node,
                                        Some(format_smolstr!(
                                            "ACL node contains unexpected child: {pwd_or_ldap_node:?}",
                                        )),
                                    ));
                                }
                            }

                            AlterOption::Password {
                                password,
                                auth_method,
                            }
                        }
                        Rule::AlterRename => {
                            let identifier_node_id = alter_option_node
                                .children
                                .first()
                                .expect("Expected to see an identifier node under AlterRename");
                            let identifier = parse_identifier(self, *identifier_node_id)?;
                            AlterOption::Rename {
                                new_name: identifier,
                            }
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::ParseNode,
                                Some(SmolStr::from("Expected to see concrete alter option")),
                            ))
                        }
                    };

                    let mut timeout = get_default_timeout();
                    if let Some(timeout_node_id) = node.children.get(2) {
                        timeout = get_timeout(self, *timeout_node_id)?;
                    }

                    let alter_user = AlterUser {
                        name: user_name,
                        alter_option,
                        timeout,
                    };
                    let plan_id = plan.nodes.push(alter_user.into());
                    map.add(id, plan_id);
                }
                Rule::CreateUser => {
                    let mut name = None;
                    let mut password = None;
                    let mut if_not_exists = DEFAULT_IF_NOT_EXISTS;
                    let mut timeout = get_default_timeout();
                    let mut auth_method = DEFAULT_AUTH_METHOD;
                    for child_id in &node.children {
                        let child_node = self.nodes.get_node(*child_id)?;
                        match child_node.rule {
                            Rule::Identifier => name = Some(parse_identifier(self, *child_id)?),
                            Rule::SingleQuotedString => {
                                let password_literal: SmolStr =
                                    retrieve_string_literal(self, *child_id)?;
                                password = Some(escape_single_quotes(&password_literal));
                            }
                            Rule::IfNotExists => if_not_exists = true,
                            Rule::Timeout => {
                                timeout = get_timeout(self, *child_id)?;
                            }
                            method @ (Rule::ChapSha1 | Rule::Md5 | Rule::Ldap) => {
                                auth_method = auth_method_from_auth_rule(method);
                            }
                            _ => {
                                return Err(SbroadError::Invalid(
                                    Entity::Node,
                                    Some(format_smolstr!(
                                        "ACL node contains unexpected child: {child_node:?}",
                                    )),
                                ));
                            }
                        }
                    }

                    let name = name.expect("username expected as a child");
                    let password = password.unwrap_or_default();
                    let create_user = CreateUser {
                        name,
                        password,
                        if_not_exists,
                        auth_method,
                        timeout,
                    };

                    let plan_id = plan.nodes.push(create_user.into());
                    map.add(id, plan_id);
                }
                Rule::AlterTable => {
                    let alter_table = parse_alter_table(self, node)?;
                    let plan_id = plan.nodes.push(alter_table.into());
                    map.add(id, plan_id);
                }
                Rule::DropUser => {
                    let mut name = None;
                    let mut timeout = get_default_timeout();
                    let mut if_exists = DEFAULT_IF_EXISTS;
                    for child_id in &node.children {
                        let child_node = self.nodes.get_node(*child_id)?;
                        match child_node.rule {
                            Rule::Identifier => name = Some(parse_identifier(self, *child_id)?),
                            Rule::IfExists => if_exists = true,
                            Rule::Timeout => timeout = get_timeout(self, *child_id)?,
                            _ => {
                                return Err(SbroadError::Invalid(
                                    Entity::Node,
                                    Some(format_smolstr!(
                                        "ACL node contains unexpected child: {child_node:?}",
                                    )),
                                ));
                            }
                        }
                    }
                    let name = name.expect("RoleName expected under DropUser node");
                    let drop_user = DropUser {
                        name,
                        if_exists,
                        timeout,
                    };
                    let plan_id = plan.nodes.push(drop_user.into());
                    map.add(id, plan_id);
                }
                Rule::CreateRole => {
                    let mut name = None;
                    let mut if_not_exists = DEFAULT_IF_NOT_EXISTS;
                    let mut timeout = get_default_timeout();
                    for child_id in &node.children {
                        let child_node = self.nodes.get_node(*child_id)?;
                        match child_node.rule {
                            Rule::Identifier => name = Some(parse_identifier(self, *child_id)?),
                            Rule::IfNotExists => if_not_exists = true,
                            Rule::Timeout => timeout = get_timeout(self, *child_id)?,
                            _ => {
                                return Err(SbroadError::Invalid(
                                    Entity::Node,
                                    Some(format_smolstr!(
                                        "ACL node contains unexpected child: {child_node:?}",
                                    )),
                                ));
                            }
                        }
                    }

                    let name = name.expect("RoleName expected under CreateRole node");
                    let create_role = CreateRole {
                        name,
                        if_not_exists,
                        timeout,
                    };

                    let plan_id = plan.nodes.push(create_role.into());
                    map.add(id, plan_id);
                }
                Rule::CreatePlugin => {
                    let create_plugin = parse_create_plugin(self, node)?;
                    let plan_id = plan.nodes.push(create_plugin.into());
                    map.add(id, plan_id);
                }
                Rule::DropPlugin => {
                    let drop_plugin = parse_drop_plugin(self, node)?;
                    let plan_id = plan.nodes.push(drop_plugin.into());
                    map.add(id, plan_id);
                }
                Rule::AlterPlugin => {
                    if let Some(node_id) = parse_alter_plugin(self, &mut plan, node)? {
                        map.add(id, node_id);
                    }
                }
                Rule::SetParam => {
                    let set_param_node = parse_set_param(self, node)?;
                    let plan_id = plan.nodes.push(set_param_node.into());
                    map.add(id, plan_id);
                }
                Rule::SetTransaction => {
                    let set_transaction_node = SetTransaction {
                        timeout: get_default_timeout(),
                    };
                    let plan_id = plan.nodes.push(set_transaction_node.into());
                    map.add(id, plan_id);
                }
                Rule::Deallocate => {
                    let deallocate = parse_deallocate(self, node)?;
                    let plan_id = plan.nodes.push(deallocate.into());
                    map.add(id, plan_id);
                }
                Rule::Begin => {
                    let begin = Tcl::Begin;
                    let plan_id = plan.nodes.push(begin.into());
                    map.add(id, plan_id);
                }
                Rule::Commit => {
                    let commit = Tcl::Commit;
                    let plan_id = plan.nodes.push(commit.into());
                    map.add(id, plan_id);
                }
                Rule::Rollback => {
                    let rollback = Tcl::Rollback;
                    let plan_id = plan.nodes.push(rollback.into());
                    map.add(id, plan_id);
                }
                _ => {}
            }
        }
        // get root node id
        let plan_top_id = map
            .get(self.top.ok_or_else(|| {
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

        // The problem is that we crete Between structures before replacing subqueries.
        worker.fix_betweens(&mut plan)?;
        plan.replace_group_by_ordinals_with_references()?;
        Ok(plan)
    }
}

impl Ast for AbstractSyntaxTree {
    fn transform_into_plan<M>(query: &str, metadata: &M) -> Result<Plan, SbroadError>
    where
        M: Metadata + Sized,
    {
        // While traversing pest `Pair`s iterator, we build a tree-like structure in a view of
        // { `rule`, `children`, ... }, where `children` is a vector of `ParseNode` ids that were
        // previously added in ast arena.
        // Children appears after unwrapping `Pair` structure, but in a case of `Expr` nodes, that
        // we'd like to handle separately, we don't want to unwrap it for future use.
        // That's why we:
        // * Add expressions `ParseNode`s into `arena`
        // * Save copy of them into map of { expr_arena_id -> corresponding pair copy }.
        let mut ast_id_to_pairs_map = ParsingPairsMap::new();

        // Helper variables holding mappings useful for parsing.
        // Move out of `AbstractSyntaxTree` so as not to add a lifetime template arguments.
        let mut pos_to_ast_id: SelectChildPairTranslation = HashMap::new();
        let mut sq_pair_to_ast_ids: PairToAstIdTranslation = HashMap::new();

        let mut ast = AbstractSyntaxTree::empty();
        let mut parameters_positions = Vec::new();
        ast.fill(
            query,
            &mut ast_id_to_pairs_map,
            &mut pos_to_ast_id,
            &mut sq_pair_to_ast_ids,
            &mut parameters_positions,
        )?;

        // Empty query.
        if ast.is_empty() {
            return Ok(Plan::empty());
        }

        ast.resolve_metadata(
            metadata,
            &mut ast_id_to_pairs_map,
            &mut pos_to_ast_id,
            &sq_pair_to_ast_ids,
            parameters_positions,
        )
    }
}

impl Plan {
    /// Wrap references, constants, functions, concatenations and casts in the plan into rows.
    /// Leave other nodes (e.g. rows) unchanged.
    ///
    /// Used for unification of expression nodes transformations (e.g. dnf).
    fn row(&mut self, expr_id: NodeId) -> Result<NodeId, SbroadError> {
        let row_id = if let Node::Expression(
            Expression::Reference(_)
            | Expression::Constant(_)
            | Expression::Cast(_)
            | Expression::Case(_)
            | Expression::Concat(_)
            | Expression::ScalarFunction(_),
        ) = self.get_node(expr_id)?
        {
            self.nodes.add_row(vec![expr_id], None)
        } else {
            expr_id
        };
        Ok(row_id)
    }
}

pub mod ast;
pub mod ir;
pub mod tree;
mod type_system;

fn parse_plugin_opts<T: Default>(
    ast: &AbstractSyntaxTree,
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
