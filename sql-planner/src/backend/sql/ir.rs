use crate::debug;
use crate::executor::protocol::VTablesMeta;
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::{RelOwned, Relational};
use crate::ir::node::{
    Alias, BlockEntries, BlockStatement, BoundType, Constant, Delete, FrameType, Join, Motion,
    Node, NodeId, Parameter, Projection, Reference, ReferenceTarget, ScalarFunction, ScanRelation,
    SubQueryReference, Update,
};
use crate::ir::relation::Column;
use crate::ir::types::DerivedType;
use crate::ir::SubtreeHash;
use ahash::{AHashMap, AHashSet};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::fmt::Write as _;
use std::hash::Hasher as _;
use std::io::{self, Write as IoWrite};
use tarantool::msgpack;
use tarantool::msgpack::{Context, DecodeError};
use tarantool::tlua::{self, Push};
use tarantool::tuple::{FunctionArgs, Tuple};
use twox_hash::XxHash3_64;

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::ir::{DqlSubtree, ExecutionPlan, SqlExecutionView, SubtreeViewBuilder};
use crate::ir::operator::{OrderByEntity, OrderByType};
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;

use super::tree::SyntaxData;

#[derive(Debug, Eq, Deserialize, Serialize, Push, Clone)]
pub struct PatternWithParams {
    pub pattern: String,
    pub params: Vec<Value>,
}

impl PatternWithParams {
    #[must_use]
    pub fn into_parts(self) -> (String, Vec<Value>) {
        (self.pattern, self.params)
    }
}

impl PartialEq for PatternWithParams {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern && self.params == other.params
    }
}
impl TryFrom<FunctionArgs> for PatternWithParams {
    type Error = SbroadError;

    fn try_from(value: FunctionArgs) -> Result<Self, Self::Error> {
        (&Tuple::from(value)).try_into()
    }
}

impl TryFrom<&Tuple> for PatternWithParams {
    type Error = SbroadError;

    fn try_from(value: &Tuple) -> Result<Self, Self::Error> {
        debug!(
            Option::from("argument parsing"),
            &format!("Query parameters: {value:?}"),
        );
        match msgpack::decode::<EncodedPatternWithParams>(value.data()) {
            Ok(encoded) => Ok(PatternWithParams::try_from(encoded)?),
            Err(e) => Err(SbroadError::ParsingError(
                Entity::PatternWithParams,
                format_smolstr!("{e:?}"),
            )),
        }
    }
}

struct EncodedPatternWithParams(String, Option<Vec<Value>>);

impl<'de> msgpack::Decode<'de> for EncodedPatternWithParams {
    fn decode(r: &mut &'de [u8], context: &Context) -> Result<Self, DecodeError> {
        let len = rmp::decode::read_array_len(r).map_err(DecodeError::from_vre::<Self>)?;
        if len != 2 {
            return Err(DecodeError::new::<Self>(format!(
                "unexpected length. expected 2, actual {len:?}",
            )));
        }

        let str = tarantool::msgpack::Decode::decode(r, context)?;
        let params: Option<Vec<Value>> = tarantool::msgpack::Decode::decode(r, context)?;

        Ok(EncodedPatternWithParams(str, params))
    }
}

impl From<PatternWithParams> for EncodedPatternWithParams {
    fn from(value: PatternWithParams) -> Self {
        EncodedPatternWithParams(value.pattern, Some(value.params))
    }
}

impl TryFrom<EncodedPatternWithParams> for PatternWithParams {
    type Error = SbroadError;
    fn try_from(mut value: EncodedPatternWithParams) -> Result<Self, Self::Error> {
        let params: Vec<Value> = value.1.take().unwrap_or_default();
        let res = PatternWithParams {
            pattern: value.0,
            params,
        };
        Ok(res)
    }
}

impl PatternWithParams {
    #[must_use]
    pub fn new(pattern: String, params: Vec<Value>) -> Self {
        PatternWithParams { pattern, params }
    }
}

impl From<PatternWithParams> for String {
    fn from(p: PatternWithParams) -> Self {
        format!("pattern: {}, parameters: {:?}", p.pattern, p.params)
    }
}

fn push_identifier(sql: &mut String, identifier: &str) {
    sql.push('\"');
    sql.push_str(identifier);
    sql.push('\"');
}

/// Resolved SQL column reference with an optional table or vtable qualifier.
struct SqlColumnReference {
    name: SmolStr,
    qualifier: Option<SmolStr>,
}

fn push_sql_column_reference(sql: &mut String, column: &SqlColumnReference) {
    if let Some(qualifier) = column.qualifier.as_deref() {
        push_identifier(sql, qualifier);
        sql.push('.');
    }
    push_identifier(sql, &column.name);
}

fn reference_target(
    ir_plan: &crate::ir::Plan,
    ref_id: NodeId,
) -> Result<Option<(NodeId, usize)>, SbroadError> {
    let Ok(rel_id) = ir_plan.get_relational_from_reference_node(ref_id) else {
        return Ok(None);
    };
    let position = match ir_plan.get_expression_node(ref_id)? {
        Expression::Reference(Reference { position, .. })
        | Expression::SubQueryReference(SubQueryReference { position, .. }) => *position,
        _ => {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format_smolstr!(
                    "expected reference node, got {:?}",
                    ir_plan.get_node(ref_id)?
                )),
            ))
        }
    };
    Ok(Some((rel_id, position)))
}

fn output_alias_name(
    ir_plan: &crate::ir::Plan,
    rel_id: NodeId,
    position: usize,
) -> Result<Option<SmolStr>, SbroadError> {
    let rel_node = ir_plan.get_relation_node(rel_id)?;
    let output_row = ir_plan.get_expression_node(rel_node.output())?;
    let Some(col_id) = output_row.get_row_list()?.get(position).copied() else {
        return Ok(None);
    };
    match ir_plan.get_expression_node(col_id)? {
        Expression::Alias(Alias { name, .. }) => Ok(Some(name.clone())),
        _ => Ok(None),
    }
}

fn exposed_or_output_alias(
    exposed: Option<SmolStr>,
    ir_plan: &crate::ir::Plan,
    rel_id: NodeId,
    position: usize,
) -> Result<Option<SmolStr>, SbroadError> {
    match exposed {
        Some(name) => Ok(Some(name)),
        None => output_alias_name(ir_plan, rel_id, position),
    }
}

fn asterisk_exposed_column_name(
    view: &(impl SqlExecutionView + ?Sized),
    ir_plan: &crate::ir::Plan,
    col_id: NodeId,
) -> Result<Option<SmolStr>, SbroadError> {
    let ref_id = match ir_plan.get_expression_node(col_id)? {
        Expression::Alias(Alias { child, .. }) => *child,
        _ => col_id,
    };
    let Expression::Reference(Reference {
        position,
        asterisk_source: Some(_),
        ..
    }) = ir_plan.get_expression_node(ref_id)?
    else {
        return Ok(None);
    };

    let source_rel_id = ir_plan.get_reference_source_relation(ref_id)?;
    relation_exposed_column_name(view, ir_plan, source_rel_id, *position)
}

fn motion_serialize_as_empty_enabled(
    view: &(impl SqlExecutionView + ?Sized),
    motion_id: NodeId,
    motion: &Motion,
) -> bool {
    view.effective_serialize_as_empty_state(motion_id, &motion.program)
        .is_enabled()
}

fn relation_exposed_column_name(
    view: &(impl SqlExecutionView + ?Sized),
    ir_plan: &crate::ir::Plan,
    rel_id: NodeId,
    position: usize,
) -> Result<Option<SmolStr>, SbroadError> {
    let rel_node = ir_plan.get_relation_node(rel_id)?;
    match rel_node {
        Relational::Motion(motion) => {
            if motion_serialize_as_empty_enabled(view, rel_id, motion) {
                return output_alias_name(ir_plan, rel_id, position);
            }
            if let Some(vtable) = view.get_vtables().get(&rel_id) {
                return vtable_column_name(vtable.as_ref(), rel_id, position).map(Some);
            }
            if !motion.policy.is_local() {
                let vtable = view.get_motion_vtable(rel_id)?;
                return vtable_column_name(vtable.as_ref(), rel_id, position).map(Some);
            }
            if let Some(child) = motion.child {
                return relation_exposed_column_name(view, ir_plan, child, position);
            }
            output_alias_name(ir_plan, rel_id, position)
        }
        Relational::ScanCte(scan) => exposed_or_output_alias(
            relation_exposed_column_name(view, ir_plan, scan.child, position)?,
            ir_plan,
            rel_id,
            position,
        ),
        Relational::ScanSubQuery(scan) => exposed_or_output_alias(
            relation_exposed_column_name(view, ir_plan, scan.child, position)?,
            ir_plan,
            rel_id,
            position,
        ),
        Relational::Union(set) => exposed_or_output_alias(
            relation_exposed_column_name(view, ir_plan, set.left, position)?,
            ir_plan,
            rel_id,
            position,
        ),
        Relational::UnionAll(set) => exposed_or_output_alias(
            relation_exposed_column_name(view, ir_plan, set.left, position)?,
            ir_plan,
            rel_id,
            position,
        ),
        Relational::Except(set) => exposed_or_output_alias(
            relation_exposed_column_name(view, ir_plan, set.left, position)?,
            ir_plan,
            rel_id,
            position,
        ),
        Relational::Intersect(set) => exposed_or_output_alias(
            relation_exposed_column_name(view, ir_plan, set.left, position)?,
            ir_plan,
            rel_id,
            position,
        ),
        Relational::Selection(selection) => exposed_or_output_alias(
            relation_exposed_column_name(view, ir_plan, selection.child, position)?,
            ir_plan,
            rel_id,
            position,
        ),
        Relational::Having(having) => exposed_or_output_alias(
            relation_exposed_column_name(view, ir_plan, having.child, position)?,
            ir_plan,
            rel_id,
            position,
        ),
        Relational::OrderBy(order_by) => exposed_or_output_alias(
            relation_exposed_column_name(view, ir_plan, order_by.child, position)?,
            ir_plan,
            rel_id,
            position,
        ),
        Relational::Limit(limit) => exposed_or_output_alias(
            relation_exposed_column_name(view, ir_plan, limit.child, position)?,
            ir_plan,
            rel_id,
            position,
        ),
        Relational::GroupBy(group_by) => exposed_or_output_alias(
            relation_exposed_column_name(view, ir_plan, group_by.child, position)?,
            ir_plan,
            rel_id,
            position,
        ),
        Relational::Projection(Projection { output, .. }) => {
            let output_row = ir_plan.get_expression_node(*output)?;
            let Some(col_id) = output_row.get_row_list()?.get(position).copied() else {
                return Ok(None);
            };
            exposed_or_output_alias(
                asterisk_exposed_column_name(view, ir_plan, col_id)?,
                ir_plan,
                rel_id,
                position,
            )
        }
        _ => output_alias_name(ir_plan, rel_id, position),
    }
}

fn sql_column_reference(
    view: &(impl SqlExecutionView + ?Sized),
    ir_plan: &crate::ir::Plan,
    ref_id: NodeId,
) -> Result<Option<SqlColumnReference>, SbroadError> {
    let Some((mut rel_id, mut position)) = reference_target(ir_plan, ref_id)? else {
        return Ok(None);
    };

    loop {
        let rel_node = ir_plan.get_relation_node(rel_id)?;
        match rel_node {
            Relational::Motion(motion) => {
                if motion_serialize_as_empty_enabled(view, rel_id, motion) {
                    let Some(name) = output_alias_name(ir_plan, rel_id, position)? else {
                        return Ok(None);
                    };
                    return Ok(Some(SqlColumnReference {
                        name,
                        qualifier: None,
                    }));
                }
                if let Some(vtable) = view.get_vtables().get(&rel_id) {
                    return sql_column_reference_from_vtable(vtable.as_ref(), rel_id, position)
                        .map(Some);
                }
                if !motion.policy.is_local() {
                    let vtable = view.get_motion_vtable(rel_id)?;
                    return sql_column_reference_from_vtable(vtable.as_ref(), rel_id, position)
                        .map(Some);
                }
            }
            Relational::ScanCte(scan) => {
                let name = relation_exposed_column_name(view, ir_plan, scan.child, position)?
                    .or(output_alias_name(ir_plan, rel_id, position)?);
                return Ok(name.map(|name| SqlColumnReference {
                    name,
                    qualifier: Some(scan.alias.clone()),
                }));
            }
            Relational::ScanSubQuery(scan) => {
                let name = relation_exposed_column_name(view, ir_plan, scan.child, position)?
                    .or(output_alias_name(ir_plan, rel_id, position)?);
                return Ok(name.map(|name| SqlColumnReference {
                    name,
                    qualifier: scan.alias.clone().filter(|alias| !alias.is_empty()),
                }));
            }
            _ if is_sql_scope_boundary(&rel_node) => return Ok(None),
            _ => {}
        }

        let output_row = ir_plan.get_expression_node(rel_node.output())?;
        let Some(col_id) = output_row.get_row_list()?.get(position).copied() else {
            return Ok(None);
        };
        match ir_plan.get_expression_node(col_id)? {
            Expression::Alias(Alias { child, .. }) => match ir_plan.get_expression_node(*child)? {
                Expression::Reference(Reference {
                    position: child_pos,
                    ..
                })
                | Expression::SubQueryReference(SubQueryReference {
                    position: child_pos,
                    ..
                }) => {
                    let Ok(next_rel_id) = ir_plan.get_relational_from_reference_node(*child) else {
                        return Ok(None);
                    };
                    rel_id = next_rel_id;
                    position = *child_pos;
                }
                _ => return Ok(None),
            },
            Expression::Reference(Reference {
                position: child_pos,
                ..
            })
            | Expression::SubQueryReference(SubQueryReference {
                position: child_pos,
                ..
            }) => {
                let Ok(next_rel_id) = ir_plan.get_relational_from_reference_node(col_id) else {
                    return Ok(None);
                };
                rel_id = next_rel_id;
                position = *child_pos;
            }
            _ => return Ok(None),
        }
    }
}

fn vtable_column_name(
    vtable: &crate::executor::vtable::VirtualTable,
    rel_id: NodeId,
    position: usize,
) -> Result<SmolStr, SbroadError> {
    vtable
        .get_columns()
        .get(position)
        .map(|column| column.name.clone())
        .ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Column,
                format_smolstr!("at position {position} in motion {rel_id:?}"),
            )
        })
}

fn sql_column_reference_from_vtable(
    vtable: &crate::executor::vtable::VirtualTable,
    rel_id: NodeId,
    position: usize,
) -> Result<SqlColumnReference, SbroadError> {
    Ok(SqlColumnReference {
        name: vtable_column_name(vtable, rel_id, position)?,
        qualifier: vtable.get_alias().cloned(),
    })
}

fn is_sql_scope_boundary(rel_node: &Relational<'_>) -> bool {
    matches!(
        rel_node,
        Relational::Projection(_)
            | Relational::SelectWithoutScan(_)
            | Relational::ScanCte(_)
            | Relational::ScanSubQuery(_)
            | Relational::Except(_)
            | Relational::Intersect(_)
            | Relational::Union(_)
            | Relational::UnionAll(_)
            | Relational::Values(_)
            | Relational::ValuesRow(_)
    )
}

fn rendered_reference_name(
    view: &(impl SqlExecutionView + ?Sized),
    ir_plan: &crate::ir::Plan,
    ref_id: NodeId,
) -> Result<SmolStr, SbroadError> {
    if let Some(column) = sql_column_reference(view, ir_plan, ref_id)? {
        return Ok(column.name);
    }

    let expr = ir_plan.get_expression_node(ref_id)?;
    Ok(ir_plan.get_alias_from_reference_node(&expr)?.into())
}

fn write_reference_sql(
    view: &(impl SqlExecutionView + ?Sized),
    ir_plan: &crate::ir::Plan,
    sql: &mut String,
    ref_id: NodeId,
) -> Result<(), SbroadError> {
    let expr = ir_plan.get_expression_node(ref_id)?;
    if !matches!(
        expr,
        Expression::Reference(_) | Expression::SubQueryReference(_)
    ) {
        return Err(SbroadError::Invalid(
            Entity::Expression,
            Some(format_smolstr!("expected reference node, got {expr:?}")),
        ));
    };

    let Some((rel_id, position)) = reference_target(ir_plan, ref_id)? else {
        return Err(SbroadError::Invalid(
            Entity::Expression,
            Some(format_smolstr!("reference node {ref_id:?} has no target")),
        ));
    };
    let rel_node = ir_plan.get_relation_node(rel_id)?;
    let alias = ir_plan.get_alias_from_reference_node(&expr)?;

    if matches!(expr, Expression::Reference(_)) && rel_node.is_insert() {
        push_identifier(sql, alias);
        return Ok(());
    }

    if let Some(column) = sql_column_reference(view, ir_plan, ref_id)? {
        push_sql_column_reference(sql, &column);
        return Ok(());
    }

    let alias = rendered_reference_name(view, ir_plan, ref_id)?;
    if let Some(name) = ir_plan.scan_name(rel_id, position)? {
        push_identifier(sql, name);
        sql.push('.');
        push_identifier(sql, alias.as_str());
        return Ok(());
    }
    push_identifier(sql, alias.as_str());
    Ok(())
}

struct PlanIdHashWriter<'hasher>(&'hasher mut XxHash3_64);

impl IoWrite for PlanIdHashWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug)]
enum HashHeaderSalt {
    Node = 0,
    Constant = 1,
    Parameter = 2,
    Reference = 3,
    Vtable = 4,
    RawExplain = 5,
}

fn hash_plan_id_header(hasher: &mut XxHash3_64, value: HashHeaderSalt) {
    hasher.write_u8(value as u8);
}

fn hash_plan_id_part<T: Serialize + ?Sized>(
    hasher: &mut XxHash3_64,
    value: &T,
) -> Result<(), SbroadError> {
    bincode::serialize_into(PlanIdHashWriter(hasher), value).map_err(|e| {
        SbroadError::FailedTo(
            Action::Serialize,
            Some(Entity::PlanId),
            format_smolstr!("{e}"),
        )
    })
}

/// Key for the router-side cache of rendered block SQL patterns.
///
/// Each plan caches its block hash in a `OnceCell` so repeated calls within
/// the same plan instance are basically free.
pub fn block_pattern_key(
    exec_plan: &ExecutionPlan,
    statements: &[BlockStatement<NodeId>],
) -> Result<u64, SbroadError> {
    let cache = &exec_plan.get_ir_plan().block_cache;
    if let Some(hash) = cache.pattern_hash() {
        return Ok(hash);
    }
    let mut hasher = XxHash3_64::new();
    for entry in BlockEntries::new(statements) {
        entry.with_location(|id, loc| {
            let loc = format_smolstr!("{}", &loc);
            hasher.write(loc.as_bytes());
            let sub = SubtreeViewBuilder::new(exec_plan, *id)?.subtree_hash()?;
            hasher.write_u64(sub.hash);
            Ok(())
        })?;
    }
    let hash = hasher.finish();
    cache.set_pattern_hash(hash);
    Ok(hash)
}

fn to_subtree_node_id(
    id: NodeId,
    node_positions: &AHashMap<NodeId, u32>,
) -> Result<NodeId, SbroadError> {
    let Some(offset) = node_positions.get(&id) else {
        return Err(outside_plan_id_subtree(id));
    };
    Ok(subtree_node_id(id, *offset))
}

fn outside_plan_id_subtree(id: NodeId) -> SbroadError {
    SbroadError::Invalid(
        Entity::PlanId,
        Some(format_smolstr!(
            "node {id} is outside of the plan id subtree"
        )),
    )
}

fn subtree_node_id(id: NodeId, offset: u32) -> NodeId {
    NodeId {
        offset,
        arena_type: id.arena_type,
    }
}

// Reference targets may point to a wrapper node omitted from the SQL subtree.
// Hash such wrappers through their visible output or child, never through raw id.
fn to_subtree_reference_node_id(
    ir_plan: &crate::ir::Plan,
    id: NodeId,
    node_positions: &AHashMap<NodeId, u32>,
) -> Result<NodeId, SbroadError> {
    if let Some(offset) = node_positions.get(&id) {
        return Ok(subtree_node_id(id, *offset));
    }
    let Node::Relational(rel) = ir_plan.get_node(id)? else {
        return Err(outside_plan_id_subtree(id));
    };
    if rel.has_output() {
        let output = rel.output();
        if let Some(offset) = node_positions.get(&output) {
            return Ok(subtree_node_id(id, *offset));
        }
    }
    for child in rel.children().iter() {
        if let Some(offset) = node_positions.get(child) {
            return Ok(subtree_node_id(id, *offset));
        }
    }
    for subquery in rel.subqueries().iter() {
        if let Some(offset) = node_positions.get(subquery) {
            return Ok(subtree_node_id(id, *offset));
        }
    }
    Err(outside_plan_id_subtree(id))
}

fn to_subtree_reference_target(
    ir_plan: &crate::ir::Plan,
    target: &ReferenceTarget,
    node_positions: &AHashMap<NodeId, u32>,
) -> Result<ReferenceTarget, SbroadError> {
    match target {
        ReferenceTarget::Leaf => Ok(ReferenceTarget::Leaf),
        ReferenceTarget::Single(id) => Ok(ReferenceTarget::Single(to_subtree_reference_node_id(
            ir_plan,
            *id,
            node_positions,
        )?)),
        ReferenceTarget::Union(left, right) => Ok(ReferenceTarget::Union(
            to_subtree_reference_node_id(ir_plan, *left, node_positions)?,
            to_subtree_reference_node_id(ir_plan, *right, node_positions)?,
        )),
        ReferenceTarget::Values(values) => Ok(ReferenceTarget::Values(
            values
                .iter()
                .map(|id| to_subtree_reference_node_id(ir_plan, *id, node_positions))
                .collect::<Result<Vec<_>, _>>()?,
        )),
    }
}

fn plan_id_reference_target_has_system_column(
    ir_plan: &crate::ir::Plan,
    target: &ReferenceTarget,
) -> Result<bool, SbroadError> {
    for rel_id in target.iter() {
        let Node::Relational(rel) = ir_plan.get_node(*rel_id)? else {
            continue;
        };
        let output = ir_plan.get_expression_node(rel.output())?;
        for alias_id in output.get_row_list()? {
            let child_id = ir_plan.get_child_under_alias(*alias_id)?;
            if matches!(
                ir_plan.get_expression_node(child_id),
                Ok(Expression::Reference(Reference {
                    is_system: true,
                    ..
                }))
            ) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn normalize_plan_id_optional_node_id(
    id: &mut Option<NodeId>,
    node_positions: &AHashMap<NodeId, u32>,
) -> Result<(), SbroadError> {
    if let Some(id) = id {
        *id = to_subtree_node_id(*id, node_positions)?;
    }
    Ok(())
}

fn normalize_plan_id_relational(
    ir_plan: &crate::ir::Plan,
    rel: &mut RelOwned,
    node_positions: &AHashMap<NodeId, u32>,
) -> Result<(), SbroadError> {
    for child in rel.mut_children() {
        *child = to_subtree_reference_node_id(ir_plan, *child, node_positions)?;
    }
    for subquery in rel.mut_subqueries() {
        *subquery = to_subtree_reference_node_id(ir_plan, *subquery, node_positions)?;
    }
    if rel.has_output() {
        let output = rel.mut_output();
        *output = to_subtree_node_id(*output, node_positions)?;
    }

    match rel {
        RelOwned::Join(join) => {
            join.condition = *ir_plan.undo.get_oldest(&join.condition);
            join.condition = to_subtree_node_id(join.condition, node_positions)?;
        }
        RelOwned::Projection(projection) => {
            for window in &mut projection.windows {
                *window = to_subtree_node_id(*window, node_positions)?;
            }
            normalize_plan_id_optional_node_id(&mut projection.group_by, node_positions)?;
            normalize_plan_id_optional_node_id(&mut projection.having, node_positions)?;
        }
        RelOwned::Selection(selection) => {
            selection.filter = *ir_plan.undo.get_oldest(&selection.filter);
            selection.filter = to_subtree_node_id(selection.filter, node_positions)?;
        }
        RelOwned::GroupBy(group_by) => {
            for expr in &mut group_by.gr_exprs {
                *expr = to_subtree_node_id(*expr, node_positions)?;
            }
        }
        RelOwned::Having(having) => {
            having.filter = *ir_plan.undo.get_oldest(&having.filter);
            having.filter = to_subtree_node_id(having.filter, node_positions)?;
        }
        RelOwned::OrderBy(order_by) => {
            for element in &mut order_by.order_by_elements {
                if let OrderByEntity::Expression { expr_id } = &mut element.entity {
                    *expr_id = to_subtree_node_id(*expr_id, node_positions)?;
                }
            }
        }
        RelOwned::ValuesRow(values_row) => {
            values_row.data = to_subtree_node_id(values_row.data, node_positions)?;
        }
        RelOwned::ScanCte(_)
        | RelOwned::Except(_)
        | RelOwned::Delete(_)
        | RelOwned::Insert(_)
        | RelOwned::Intersect(_)
        | RelOwned::Update(_)
        | RelOwned::Limit(_)
        | RelOwned::Motion(_)
        | RelOwned::ScanRelation(_)
        | RelOwned::ScanSubQuery(_)
        | RelOwned::SelectWithoutScan(_)
        | RelOwned::UnionAll(_)
        | RelOwned::Union(_)
        | RelOwned::Values(_) => {}
    }
    Ok(())
}

impl SubtreeViewBuilder<'_> {
    fn subtree_hash(&self) -> Result<SubtreeHash, SbroadError> {
        let cache = &self.exec_plan().get_ir_plan().subtree_hash_cache;
        if let Some(hash) = cache.get(self.key()) {
            return Ok(hash);
        }

        let hash = self.calculate_subtree_hash()?;
        cache.insert(self.key().clone(), hash);
        Ok(hash)
    }

    fn calculate_subtree_hash(&self) -> Result<SubtreeHash, SbroadError> {
        let ir_plan = self.exec_plan().get_ir_plan();
        let mut hasher = XxHash3_64::new();
        let mut node_positions = self
            .node_ids()
            .iter()
            .enumerate()
            .map(|(pos, id)| (*id, pos as u32))
            .collect::<AHashMap<_, _>>();
        // Motions rendered as SQL leaves hide their child subtree. References
        // to hidden nodes hash as references to the visible motion leaf.
        for node_id in self.node_ids() {
            let Node::Relational(Relational::Motion(Motion {
                child: Some(child), ..
            })) = ir_plan.get_node(*node_id)?
            else {
                continue;
            };
            if node_positions.contains_key(child) {
                continue;
            }
            let motion_pos = *node_positions
                .get(node_id)
                .expect("subtree nodes should have local positions");
            for hidden_id in ir_plan.exec_plan_subtree_iter(*child, Snapshot::Oldest) {
                node_positions.entry(hidden_id).or_insert(motion_pos);
            }
        }

        for (node_id, level) in self.node_ids().iter().zip(self.node_levels()) {
            let node = ir_plan.get_node(*node_id)?;
            hash_plan_id_header(&mut hasher, HashHeaderSalt::Node);
            hash_plan_id_part(&mut hasher, &to_subtree_node_id(*node_id, &node_positions)?)?;
            hash_plan_id_part(&mut hasher, level)?;
            match node {
                Node::Expression(Expression::Constant(Constant { value })) => {
                    hash_plan_id_header(&mut hasher, HashHeaderSalt::Constant);
                    hash_plan_id_part(&mut hasher, &value.get_type())?;
                }
                Node::Expression(Expression::Parameter(Parameter { param_type, .. })) => {
                    hash_plan_id_header(&mut hasher, HashHeaderSalt::Parameter);
                    hash_plan_id_part(&mut hasher, param_type)?;
                }
                Node::Expression(Expression::Reference(Reference {
                    target,
                    position,
                    col_type,
                    asterisk_source,
                    is_system,
                })) => {
                    let target_has_system_column =
                        plan_id_reference_target_has_system_column(ir_plan, target)?;
                    let target = to_subtree_reference_target(ir_plan, target, &node_positions)?;
                    hash_plan_id_header(&mut hasher, HashHeaderSalt::Reference);
                    hash_plan_id_part(&mut hasher, &target)?;
                    hash_plan_id_part(&mut hasher, position)?;
                    hash_plan_id_part(&mut hasher, col_type)?;
                    if !target_has_system_column {
                        hash_plan_id_part(&mut hasher, asterisk_source)?;
                    }
                    hash_plan_id_part(&mut hasher, is_system)?;
                }
                Node::Expression(expr) => {
                    let mut expr = expr.get_expr_owned();
                    expr.try_map_children(|child| {
                        *child = to_subtree_node_id(*child, &node_positions)?;
                        Ok::<(), SbroadError>(())
                    })?;
                    hash_plan_id_part(&mut hasher, &expr)?;
                }
                Node::Relational(rel) => {
                    let mut rel = rel.get_rel_owned();
                    normalize_plan_id_relational(ir_plan, &mut rel, &node_positions)?;
                    hash_plan_id_part(&mut hasher, &rel)?;
                }
                _ => {
                    hash_plan_id_part(&mut hasher, &node.into_owned())?;
                }
            }
        }

        Ok(SubtreeHash {
            hash: hasher.finish(),
            sql_parameter_count: self.constant_ids().len(),
        })
    }
}

impl ExecutionPlan {
    /// Remove vtables from execution plan.
    ///
    /// Because vtables are transfered in required data,
    /// no need to store them in optional data (where
    /// execution plan is stored).
    /// One exception currently is vtable under DML
    /// node, such query is not cacheable (because
    /// we execute dml using tarantool api) and this
    /// vtable is not removed from plan.
    ///
    /// # Errors
    /// - Invalid plan
    pub fn remove_vtables(&mut self) -> Result<VTablesMeta, SbroadError> {
        let mut reserved_motion_id = None;
        {
            let ir = self.get_ir_plan();
            let top_id = ir.get_top()?;
            let top = ir.get_relation_node(top_id)?;

            if top.is_dml() {
                let top_children = ir.children(top_id);

                // DELETE without WHERE clause don't have children.
                if !top_children.is_empty() {
                    reserved_motion_id = Some(top_children[0]);
                }
            }
        }

        let vtables = self.get_mut_vtables();
        let mut vtables_meta = VTablesMeta::with_capacity(vtables.len());

        for (id, vtable) in vtables.iter() {
            vtables_meta.insert(*id, vtable.metadata());
        }

        let reserved_entry = if let Some(id) = reserved_motion_id {
            vtables.remove_entry(&id)
        } else {
            None
        };
        vtables.clear();
        if let Some((key, value)) = reserved_entry {
            vtables.insert(key, value);
        }

        Ok(vtables_meta)
    }

    pub(crate) fn calculate_plan_id_with_parameter_count(
        &self,
        top_id: NodeId,
    ) -> Result<(u64, usize), SbroadError> {
        let subtree_hash = SubtreeViewBuilder::new(self, top_id)?.subtree_hash()?;
        let mut hasher = XxHash3_64::new();
        hasher.write_u64(subtree_hash.hash);

        if self.get_ir_plan().is_raw_explain() {
            hash_plan_id_header(&mut hasher, HashHeaderSalt::RawExplain);
        }

        self.get_vtables()
            .iter()
            .sorted_by_key(|(node_id, _)| node_id.offset)
            .try_for_each(|(node_id, vtable)| -> Result<(), SbroadError> {
                hash_plan_id_header(&mut hasher, HashHeaderSalt::Vtable);
                hash_plan_id_part(&mut hasher, node_id)?;
                for column in vtable.get_columns() {
                    hash_plan_id_part(&mut hasher, column.name.as_str())?;
                    hash_plan_id_part(&mut hasher, &u8::from(column.r#type))?;
                }
                Ok(())
            })?;

        Ok((hasher.finish(), subtree_hash.sql_parameter_count))
    }

    pub fn calculate_plan_id(&self, top_id: NodeId) -> Result<u64, SbroadError> {
        self.calculate_plan_id_with_parameter_count(top_id)
            .map(|(plan_id, _)| plan_id)
    }

    /// Changes `plan_id` by inverting its bits.
    /// This is only useful for plans with `SerializeAsEmptyTable(true)`.
    /// Checkout out `prepare_rs_to_ir_map` for use case.
    ///
    /// # Errors
    /// - If `plan_id` is not set.
    pub fn salt_plan_id(&mut self) -> Result<(), SbroadError> {
        let plan_id = !self.plan_id.ok_or_else(|| {
            SbroadError::FailedTo(
                Action::Get,
                Some(Entity::PlanId),
                format_smolstr!("must be already initialized"),
            )
        })?;
        self.plan_id = Some(plan_id);
        Ok(())
    }

    pub fn get_plan_id(&self) -> Result<u64, SbroadError> {
        let plan_id = self.plan_id.ok_or_else(|| {
            SbroadError::FailedTo(
                Action::Get,
                Some(Entity::PlanId),
                format_smolstr!("must be already initialized"),
            )
        })?;

        // During bucket discovery, new constants may appear
        // if `bucket_id` is used as a part of the primary key.
        //
        // CREATE TABLE t(a INT, b INT, PRIMARY KEY (bucket_id, a));
        // SELECT * FROM t WHERE a = 1 UNION SELECT * FROM t WHERE a = 1;
        // SELECT * FROM t WHERE a = 1 UNION SELECT * FROM t WHERE a = 2;
        //
        // These queries have the same local SQL,
        // but they inject different numbers of constants due to `bucket_id`,
        // so we want to consider these queries different.
        let mut hasher = XxHash3_64::new();
        hasher.write_u64(plan_id);
        hasher.write_usize(self.parameter_count()?);
        let plan_id = hasher.finish();

        Ok(plan_id)
    }

    /// Renders local SQL for the provided syntax nodes.
    ///
    /// When constants are rendered as parameters, `constants` must contain their
    /// node ids in the same order as SQL placeholders. The argument may be any
    /// borrowed or owned collection addressable as a `NodeId` slice.
    pub(crate) fn generate_sql<C>(
        &self,
        nodes: &[impl AsRef<SyntaxData>],
        plan_id: u64,
        table_name: fn(u64, NodeId) -> SmolStr,
        constants: Option<C>,
    ) -> Result<String, SbroadError>
    where
        C: AsRef<[NodeId]>,
    {
        generate_sql_from_view(self, nodes, plan_id, table_name, constants)
    }

    /// Checks if the given query subtree modifies data or not.
    ///
    /// # Errors
    /// - If the subtree top is not a relational node.
    pub fn subtree_modifies_data(&self, top_id: NodeId) -> Result<bool, SbroadError> {
        // Tarantool doesn't support `INSERT`, `UPDATE` and `DELETE` statements
        // with `RETURNING` clause. That is why it is enough to check if the top
        // node is a data modification statement or not.
        let top = self.get_ir_plan().get_relation_node(top_id)?;
        Ok(top.is_dml())
    }
}

impl DqlSubtree<'_> {
    /// Renders local SQL for this effective DQL subtree.
    ///
    /// When constants are rendered as parameters, `constants` must contain their
    /// node ids in the same order as SQL placeholders. Only nodes visible
    /// through this subtree view are eligible for relation and vtable lookup.
    pub fn generate_sql<C>(
        &self,
        nodes: &[impl AsRef<SyntaxData>],
        plan_id: u64,
        table_name: fn(u64, NodeId) -> SmolStr,
        constants: Option<C>,
    ) -> Result<String, SbroadError>
    where
        C: AsRef<[NodeId]>,
    {
        generate_sql_from_view(self, nodes, plan_id, table_name, constants)
    }
}

fn sql_append_bucket_filter<V: SqlExecutionView + ?Sized>(
    view: &V,
    sql: &mut String,
    params_idx: &mut AHashSet<usize>,
    selection_id: NodeId,
    parameter_count: usize,
) -> Result<(), SbroadError> {
    let Some(bucket_ref_id) = view.get_bucket_ref(selection_id) else {
        return Ok(());
    };

    let bucket_ids = view.get_bucket_filter_ids().ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Buckets,
            Some(format_smolstr!(
                "bucket filter target exists for selection {selection_id:?}, but bucket ids are missing"
            )),
        )
    })?;
    if bucket_ids.is_empty() {
        return Err(SbroadError::Invalid(
            Entity::Buckets,
            Some(format_smolstr!(
                "bucket filter target exists for selection {selection_id:?}, but bucket ids are empty"
            )),
        ));
    }

    let ir_plan = view.get_ir_plan();
    let bucket_ref = ir_plan.get_expression_node(bucket_ref_id)?;
    let Expression::Reference(_) = bucket_ref else {
        return Err(SbroadError::Invalid(
            Entity::Expression,
            Some(format_smolstr!(
                "bucket filter target {bucket_ref_id:?} is not a reference"
            )),
        ));
    };

    sql.push(' ');
    write_reference_sql(view, ir_plan, sql, bucket_ref_id)?;
    sql.push_str(" IN (");

    let param_type = ir_plan
        .calculate_expression_type(bucket_ref_id)?
        .map(DerivedType::new)
        .unwrap_or(DerivedType::unknown());
    let first_bucket_param = parameter_count + 1;
    for (pos, _) in bucket_ids.iter().enumerate() {
        if pos > 0 {
            sql.push(',');
        }
        let index = first_bucket_param + pos;
        match param_type.get() {
            Some(ty) => sql.push_str(&format_smolstr!("CAST(${index} AS {ty})")),
            None => sql.push_str(&format_smolstr!("${index}")),
        }
        params_idx.insert(index);
    }
    sql.push_str(") and");

    Ok(())
}

fn generate_sql_from_view<V, N, C>(
    view: &V,
    nodes: &[N],
    plan_id: u64,
    table_name: fn(u64, NodeId) -> SmolStr,
    constants: Option<C>,
) -> Result<String, SbroadError>
where
    V: SqlExecutionView + ?Sized,
    N: AsRef<SyntaxData>,
    C: AsRef<[NodeId]>,
{
    let mut motions = AHashSet::with_capacity(view.get_vtables().len());
    let mut params_idx: AHashSet<usize> = AHashSet::new();

    let mut sql = String::new();
    let delim = " ";

    let need_delim_after = |id: usize| -> bool {
        let mut result: bool = true;
        if id > 0 {
            if let Some(SyntaxData::OpenParenthesis) = nodes.get(id - 1).map(|n| n.as_ref()) {
                result = false;
            }
        }
        if let Some(SyntaxData::Comma | SyntaxData::CloseParenthesis) =
            nodes.get(id).map(|n| n.as_ref())
        {
            result = false;
        }
        if id == 0 {
            result = false;
        }
        result
    };

    let ir_plan = view.get_ir_plan();
    let constants = constants.as_ref().map(|constants| {
        constants
            .as_ref()
            .iter()
            .copied()
            .enumerate()
            .map(|(num, id)| (id, num + 1))
            .collect::<AHashMap<_, _>>()
    });
    let sql_parameter_count = constants.as_ref().map_or_else(
        || {
            nodes
                .iter()
                .filter_map(|node| match node.as_ref() {
                    SyntaxData::Parameter(_, index) => Some(*index),
                    _ => None,
                })
                .max()
                .unwrap_or(0)
        },
        |constants| constants.len(),
    );
    let constants_count =
        sql_parameter_count + view.get_bucket_filter_ids().map_or(0, |ids| ids.len());

    // The starting value of the counter for generating anonymous column names.
    // It emulates Tarantool parser (generated by lemon parser generator).
    // It traverses the parse tree bottom-up from left to right and increments
    // the global counter with the columns that need an auto-generated name.
    // As a result each such column would be named like "COLUMN_%d", where "%d"
    // is the new global counter value.
    let nodes = nodes.iter().map(|n| n.as_ref());
    for (id, data) in nodes.enumerate() {
        if let Some(' ' | '(') = sql.chars().last() {
        } else if need_delim_after(id) {
            sql.push_str(delim);
        }

        match data {
            // TODO: should we care about plans without projections?
            // Or they should be treated as invalid?
            SyntaxData::Empty => {}
            SyntaxData::As => sql.push_str("AS"),
            SyntaxData::Over => sql.push_str("OVER"),
            SyntaxData::PartitionBy => sql.push_str("PARTITION BY"),
            SyntaxData::Filter => sql.push_str("FILTER"),
            SyntaxData::Where => sql.push_str("WHERE"),
            SyntaxData::OrderBy => sql.push_str("ORDER BY"),
            SyntaxData::WindowFrameType(frame_ty) => {
                let str = match frame_ty {
                    FrameType::Range => "RANGE",
                    FrameType::Rows => "ROWS",
                };
                sql.push_str(str)
            }
            SyntaxData::Between => sql.push_str("BETWEEN"),
            SyntaxData::And => sql.push_str("AND"),
            SyntaxData::WindowFrameBound(frame_bound) => {
                let str = match frame_bound {
                    BoundType::PrecedingUnbounded => "UNBOUNDED PRECEDING",
                    BoundType::PrecedingOffset(_) => "PRECEDING",
                    BoundType::CurrentRow => "CURRENT ROW",
                    BoundType::FollowingOffset(_) => "FOLLOWING",
                    BoundType::FollowingUnbounded => "UNBOUNDED FOLLOWING",
                };
                sql.push_str(str)
            }
            SyntaxData::Window => sql.push_str("WINDOW"),
            SyntaxData::Asterisk(relation_name) => {
                if let Some(relation_name) = relation_name {
                    push_identifier(&mut sql, relation_name.as_str());
                    sql.push('.')
                }
                sql.push('*')
            }
            SyntaxData::Alias(s) => {
                sql.push_str("as ");
                push_identifier(&mut sql, s);
            }
            SyntaxData::IndexedBy(name) => {
                sql.push_str("INDEXED BY ");
                push_identifier(&mut sql, name);
            }
            SyntaxData::CastType(s) => {
                sql.push_str("as ");
                sql.push_str(s);
            }
            SyntaxData::Cast => sql.push_str("CAST"),
            SyntaxData::Case => sql.push_str("CASE"),
            SyntaxData::When => sql.push_str("WHEN"),
            SyntaxData::Then => sql.push_str("THEN"),
            SyntaxData::Else => sql.push_str("ELSE"),
            SyntaxData::End => sql.push_str("END"),
            SyntaxData::Concat => sql.push_str("||"),
            SyntaxData::Comma => sql.push(','),
            SyntaxData::Condition => sql.push_str("ON"),
            SyntaxData::Escape => sql.push_str("ESCAPE"),
            SyntaxData::Like => sql.push_str("LIKE"),
            SyntaxData::Distinct => sql.push_str("DISTINCT"),
            SyntaxData::OrderByPosition(index) => sql.push_str(&format_smolstr!("{index}")),
            SyntaxData::OrderByType(order_type) => match order_type {
                OrderByType::Asc => sql.push_str("ASC"),
                OrderByType::Desc => sql.push_str("DESC"),
            },
            SyntaxData::Inline(content) => sql.push_str(content),
            SyntaxData::From => sql.push_str("FROM"),
            SyntaxData::Leading => sql.push_str("LEADING"),
            SyntaxData::Limit(limit) => sql.push_str(&format_smolstr!("LIMIT {limit}")),
            SyntaxData::Both => sql.push_str("BOTH"),
            SyntaxData::Trailing => sql.push_str("TRAILING"),
            SyntaxData::Operator(s) => sql.push_str(s.as_str()),
            SyntaxData::OpenParenthesis => sql.push('('),
            SyntaxData::CloseParenthesis => sql.push(')'),
            SyntaxData::OpenBracket => sql.push('['),
            SyntaxData::CloseBracket => sql.push(']'),
            SyntaxData::Trim => sql.push_str("TRIM"),
            SyntaxData::PlanId(id) => {
                let node = ir_plan.get_node(*id)?;
                match node {
                    Node::Ddl(_) => {
                        return Err(SbroadError::Unsupported(
                            Entity::Node,
                            Some("DDL nodes are not supported in the generated SQL".into()),
                        ));
                    }
                    Node::Acl(_) => {
                        return Err(SbroadError::Unsupported(
                            Entity::Node,
                            Some("ACL nodes are not supported in the generated SQL".into()),
                        ));
                    }
                    Node::Tcl(_) => {
                        return Err(SbroadError::Unsupported(
                            Entity::Node,
                            Some("TCL nodes are not supported in the generated SQL".into()),
                        ));
                    }
                    Node::Block(_) => {
                        return Err(SbroadError::Unsupported(
                            Entity::Node,
                            Some("Code block nodes are not supported in the generated SQL".into()),
                        ));
                    }
                    Node::Expression(Expression::Parameter(..)) => {
                        return Err(SbroadError::Unsupported(
                            Entity::Node,
                            Some("Parameters are not supported in the generated SQL".into()),
                        ));
                    }
                    Node::Invalid(..) => {
                        return Err(SbroadError::Unsupported(
                            Entity::Node,
                            Some("Invalid nodes are not supported in the generated SQL".into()),
                        ));
                    }
                    Node::Plugin(_) => {
                        return Err(SbroadError::Unsupported(
                            Entity::Node,
                            Some("Plugin are not supported in the generated SQL".into()),
                        ));
                    }
                    Node::Deallocate(_) => {
                        return Err(SbroadError::Unsupported(
                            Entity::Node,
                            Some("Deallocate nodes are not supported in the generated SQL".into()),
                        ));
                    }
                    Node::Relational(rel) => match rel {
                        Relational::Except { .. } => sql.push_str("EXCEPT"),
                        Relational::GroupBy { .. } => sql.push_str("GROUP BY"),
                        Relational::Intersect { .. } => sql.push_str("INTERSECT"),
                        Relational::Having { .. } => sql.push_str("HAVING"),
                        Relational::OrderBy { .. } => sql.push_str("ORDER BY"),
                        Relational::Delete(Delete { relation, .. }) => {
                            sql.push_str("DELETE FROM ");
                            push_identifier(&mut sql, relation)
                        }
                        Relational::Update(Update { relation, .. }) => {
                            sql.push_str("UPDATE ");
                            push_identifier(&mut sql, relation);
                        }
                        Relational::Join(Join { kind, .. }) => sql.push_str(&format_smolstr!(
                            "{} JOIN",
                            kind.to_smolstr().to_uppercase()
                        )),
                        Relational::Projection(Projection {
                            is_distinct: true, ..
                        }) => sql.push_str("SELECT DISTINCT"),
                        Relational::Projection { .. } | Relational::SelectWithoutScan { .. } => {
                            sql.push_str("SELECT")
                        }
                        Relational::ScanRelation(ScanRelation { relation, .. }) => {
                            push_identifier(&mut sql, relation)
                        }
                        Relational::ScanSubQuery { .. }
                        | Relational::ScanCte { .. }
                        | Relational::Motion { .. }
                        | Relational::ValuesRow { .. }
                        | Relational::Limit { .. } => {}
                        Relational::Selection { .. } => {
                            sql.push_str("WHERE");
                            sql_append_bucket_filter(
                                view,
                                &mut sql,
                                &mut params_idx,
                                *id,
                                sql_parameter_count,
                            )?;
                        }
                        Relational::Union { .. } => sql.push_str("UNION"),
                        Relational::UnionAll { .. } => sql.push_str("UNION ALL"),
                        Relational::Values { .. } => sql.push_str("VALUES"),
                        Relational::Insert { .. } => {
                            return Err(SbroadError::Invalid(
                                Entity::Node,
                                Some("DML nodes are not supported in local SQL".into()),
                            ));
                        }
                    },
                    Node::Expression(expr) => match expr {
                        Expression::Alias { .. }
                        | Expression::Window { .. }
                        | Expression::Over { .. }
                        | Expression::Bool { .. }
                        | Expression::Arithmetic { .. }
                        | Expression::Index { .. }
                        | Expression::Cast { .. }
                        | Expression::Case { .. }
                        | Expression::Concat { .. }
                        | Expression::Like { .. }
                        | Expression::Row { .. }
                        | Expression::Trim { .. }
                        | Expression::Unary { .. }
                        | Expression::Timestamp { .. }
                        | Expression::Parameter { .. }
                        | Expression::LetVarRef { .. } => {}
                        Expression::Constant(Constant { value, .. }) => {
                            write!(sql, "{value}").map_err(|e| {
                                SbroadError::FailedTo(
                                    Action::Put,
                                    Some(Entity::Value),
                                    format_smolstr!("constant value to SQL: {e}"),
                                )
                            })?;
                        }
                        Expression::Reference(_) => {
                            let rel_id = ir_plan.get_relational_from_reference_node(*id)?;
                            let rel_node = ir_plan.get_relation_node(rel_id)?;
                            let alias = ir_plan.get_alias_from_reference_node(&expr)?;
                            if rel_node.is_insert() {
                                // We expect `INSERT INTO t(a, b) VALUES(1, 2)`
                                // rather then `INSERT INTO t(t.a, t.b) VALUES(1, 2)`.
                                push_identifier(&mut sql, alias);
                                continue;
                            }
                            write_reference_sql(view, ir_plan, &mut sql, *id)?;
                        }
                        Expression::SubQueryReference(_) => {
                            write_reference_sql(view, ir_plan, &mut sql, *id)?;
                        }
                        Expression::ScalarFunction(ScalarFunction {
                            name, is_system, ..
                        }) => {
                            if *is_system {
                                sql.push_str(name);
                            } else {
                                push_identifier(&mut sql, name);
                            }
                        }
                        Expression::CountAsterisk { .. } => {
                            sql.push('*');
                        }
                    },
                }
            }
            SyntaxData::LetVarRef(id, name) => {
                // Render LET-variable reference as `:<name>`; wrap in a
                // CAST when type is known, mirroring the param path.
                let var_type = match ir_plan.get_node(*id) {
                    Ok(Node::Expression(Expression::LetVarRef(let_var_ref))) => {
                        let_var_ref.var_type
                    }
                    node => panic!("let var ref node points to {node:?}"),
                };
                match var_type.get() {
                    Some(ty) => sql.push_str(&format_smolstr!("CAST(:{name} AS {ty})")),
                    None => sql.push_str(&format_smolstr!(":{name}")),
                }
            }
            SyntaxData::Parameter(id, index) | SyntaxData::ParameterColon(id, index) => {
                let (param_type, index) = match ir_plan.get_node(*id) {
                    Ok(Node::Expression(Expression::Parameter(Parameter {
                        param_type, ..
                    }))) if constants.is_none() => (*param_type, *index),
                    Ok(Node::Expression(Expression::Constant(Constant { .. })))
                        if constants.is_some() =>
                    {
                        let param_type = ir_plan.calculate_expression_type(*id)?;
                        let param_type = param_type
                            .map(DerivedType::new)
                            // NULL literal has an unknown type
                            .unwrap_or(DerivedType::unknown());
                        let index = constants
                            .as_ref()
                            .expect("constants list should be some")
                            .get(id)
                            .ok_or_else(|| {
                                SbroadError::NotFound(
                                    Entity::Node,
                                    format_smolstr!("constant node {id} is missing"),
                                )
                            })?;
                        (param_type, *index)
                    }
                    node => panic!("parameter node points to {node:?}"),
                };

                let prefix = if matches!(data, SyntaxData::ParameterColon(..)) {
                    ':'
                } else {
                    '$'
                };

                match param_type.get() {
                    Some(ty) => sql.push_str(&format_smolstr!("CAST({prefix}{index} AS {ty})")),
                    None => sql.push_str(&format_smolstr!("{prefix}{index}")),
                }

                assert!(index <= constants_count);
                params_idx.insert(index);
            }
            SyntaxData::VTable(motion_id) => {
                // Note that in case Motion is a top of the plan it won't be transformed
                // into local SQL. Rather `VirtualTable::to_output` method would be called
                // in order to transform it into result, directly returned to user.
                // See `Query::dispatch` method for usage.
                let name = table_name(plan_id, *motion_id);
                let build_names = |columns: &[Column]| -> String {
                    let cp: usize = columns.iter().map(|c| c.name.len() + 3).sum();
                    let mut col_names = String::with_capacity(cp.saturating_sub(1));
                    if let Some((last, other)) = columns.split_last() {
                        for col in other {
                            push_identifier(&mut col_names, &col.name);
                            col_names.push(',');
                        }
                        push_identifier(&mut col_names, &last.name);
                    }
                    col_names
                };
                let col_names = {
                    let vtable = view.get_motion_vtable(*motion_id)?;
                    build_names(vtable.get_columns())
                };
                write!(sql, "SELECT {col_names} FROM \"{name}\"").map_err(|e| {
                    SbroadError::FailedTo(
                        Action::Serialize,
                        Some(Entity::VirtualTable),
                        format_smolstr!("SQL builder: {e}"),
                    )
                })?;
                // BETWEEN can refer to the same virtual table multiple times.
                motions.insert(*motion_id);
            }
        }
    }

    // Generated sql query can be empty in case of buggy plan.
    // We should not insert `EXPLAIN QUERY PLAN` in such scenario
    // since this will lead to error incostistency. For instance,
    // executing empty query and `EXPLAIN QUERY PLAN ` produces
    // different errors: `Failed to execute an empty SQL statement`
    // and `Syntax error at line 1 near ''` respectively.
    if view.get_ir_plan().is_raw_explain() && !sql.is_empty() {
        sql.insert_str(0, "EXPLAIN QUERY PLAN ");
    }

    assert_eq!(constants_count, params_idx.len());

    Ok(sql)
}

#[cfg(feature = "mock")]
#[cfg(test)]
mod tests;
