use crate::{
    backend::sql::space::{TableGuard, ADMIN_ID},
    ir::node::expression::MutExpression,
};
use ahash::AHashMap;

use crate::{
    error,
    executor::{protocol::VTablesMeta, vtable::vtable_indexed_column_name},
    ir::{
        node::{
            expression::Expression, relational::Relational, Alias, Constant, Limit, Motion, NodeId,
            Update, Values, ValuesRow,
        },
        types::DerivedType,
    },
};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::{
    any::Any,
    cmp::Ordering,
    collections::HashMap,
    rc::Rc,
    str::{from_utf8, FromStr},
    sync::OnceLock,
};
use tarantool::space::Space;

use super::{Metadata, Router, Vshard};
use crate::executor::engine::helpers::vshard::CacheInfo;
use crate::executor::protocol::{EncodedVTables, SchemaInfo};
use crate::executor::Port;
use crate::ir::node::Node;
use crate::ir::value::{EncodedValue, MsgPackValue};
use crate::{
    backend::sql::{
        ir::PatternWithParams,
        tree::{OrderedSyntaxNodes, SyntaxPlan},
    },
    errors::{Action, Entity, SbroadError},
    executor::{
        bucket::Buckets,
        ir::{ExecutionPlan, QueryType},
        protocol::{Binary, EncodedOptionalData, OptionalData, RequiredData},
        result::MetadataColumn,
        vtable::{calculate_unified_types, VTableTuple, VirtualTable},
    },
    ir::{
        relation::{Column, ColumnRole},
        transformation::redistribution::{MotionKey, MotionPolicy},
        tree::Snapshot,
        value::Value,
        Plan,
    },
};
use rmp::encode::{write_array_len, write_map_len, write_pfix, write_str};
use std::io::Write;
use tarantool::msgpack;
use tarantool::msgpack::rmp::{self, decode::RmpRead};
use tarantool::msgpack::{decode_from_read, Context, Decode, DecodeError};
use tarantool::msgpack::{Encode, EncodeError};
use tarantool::session::with_su;
use tarantool::tuple::Tuple;

pub mod vshard;

/// Transform:
///
/// ```text
/// * "AbC" -> AbC (same cased, unquoted)
/// * AbC   -> abc (lowercased, unquoted)
/// ```
#[must_use]
pub fn normalize_name_from_sql(s: &str) -> SmolStr {
    if let (Some('"'), Some('"')) = (s.chars().next(), s.chars().last()) {
        return SmolStr::from(&s[1..s.len() - 1]);
    }
    SmolStr::new(s.to_lowercase())
}

/// Transform:
/// * s -> "s" (same cased, quoted)
///
/// This function is used to convert identifiers
/// to user-friendly format for errors and explain
/// query.
///
/// # Panics
/// - never
#[must_use]
pub fn to_user<T: std::fmt::Display>(from: T) -> SmolStr {
    format_smolstr!("\"{from}\"")
}

fn xx_hash(s: &str) -> SmolStr {
    #[cfg(feature = "mock")]
    {
        SmolStr::from(s)
    }
    #[cfg(not(feature = "mock"))]
    {
        use std::hash::Hasher;
        use twox_hash::XxHash3_64;

        let mut hasher = XxHash3_64::default();
        hasher.write(s.as_bytes());
        let id = hasher.finish();
        format_smolstr!("{id}")
    }
}

/// Generate a temporary table name for the specified motion node.
#[must_use]
pub fn table_name(plan_id: &str, node_id: NodeId) -> SmolStr {
    let base = xx_hash(plan_id);
    format_smolstr!("TMP_{base}_{node_id}")
}

#[must_use]
pub fn new_table_name(plan_id: u64, node_id: NodeId) -> SmolStr {
    format_smolstr!("TMP_{plan_id}_{node_id}")
}

/// Generate a primary key name for the specified motion node.
#[must_use]
pub fn pk_name(plan_id: &str, node_id: NodeId) -> SmolStr {
    let base = xx_hash(plan_id);
    format_smolstr!("PK_{base}_{node_id}")
}

pub fn build_required_binary(exec_plan: &mut ExecutionPlan) -> Result<Binary, SbroadError> {
    let query_type = exec_plan.query_type()?;
    let mut sub_plan_id = None;
    {
        let ir = exec_plan.get_ir_plan();
        let top_id = ir.get_top()?;
        match query_type {
            QueryType::DQL => {
                sub_plan_id = Some(ir.pattern_id(top_id)?);
            }
            QueryType::DML => {
                let top = ir.get_relation_node(top_id)?;
                let top_children = ir.children(top_id);
                if matches!(top, Relational::Delete(_)) && top_children.is_empty() {
                    sub_plan_id = Some(ir.pattern_id(top_id)?);
                } else {
                    let child_id = top_children[0];
                    let is_cacheable = matches!(
                        ir.get_relation_node(child_id)?,
                        Relational::Motion(Motion {
                            policy: MotionPolicy::Local | MotionPolicy::LocalSegment { .. },
                            ..
                        })
                    );
                    if is_cacheable {
                        let cacheable_subtree_root_id =
                            exec_plan.get_motion_subtree_root(child_id)?;
                        sub_plan_id = Some(ir.pattern_id(cacheable_subtree_root_id)?);
                    }
                }
            }
        };
    }
    let sub_plan_id = sub_plan_id.unwrap_or_default();
    let params = exec_plan.to_params().to_vec();
    let vtables = exec_plan.encode_vtables();
    let table_version_map = std::mem::take(&mut exec_plan.get_mut_ir_plan().table_version_map);
    let index_version_map = std::mem::take(&mut exec_plan.get_mut_ir_plan().index_version_map);
    let schema_info = SchemaInfo::new(table_version_map, index_version_map);
    let required = RequiredData::new(
        sub_plan_id,
        params,
        query_type,
        exec_plan.get_ir_plan().effective_options.clone(),
        schema_info,
        vtables,
    );
    let required_as_tuple = required.to_tuple()?;
    Ok(required_as_tuple.into())
}

pub fn build_optional_binary(mut exec_plan: ExecutionPlan) -> Result<Binary, SbroadError> {
    let query_type = exec_plan.query_type()?;
    let ordered = match query_type {
        QueryType::DQL => {
            let sp_top_id = exec_plan.get_ir_plan().get_top()?;
            let sp = SyntaxPlan::new(&exec_plan, sp_top_id, Snapshot::Oldest)?;

            OrderedSyntaxNodes::try_from(sp)?
        }
        QueryType::DML => {
            let plan = exec_plan.get_ir_plan();
            let sp_top_id = plan.get_top()?;
            let sp_top = plan.get_relation_node(sp_top_id)?;
            let sp_top_children = sp_top.children();

            if matches!(sp_top, Relational::Delete(_)) && sp_top_children.is_empty() {
                // We have a case of DELETE without WHERE
                // which we want to execute via local SQL.
                let sp = SyntaxPlan::new(&exec_plan, sp_top_id, Snapshot::Oldest)?;
                OrderedSyntaxNodes::try_from(sp)?
            } else {
                let motion_id = sp_top_children[0];
                let policy = plan.get_motion_policy(motion_id)?;

                // SQL is needed only for the motion node subtree.
                // HACK: we don't actually need SQL when the subtree is already
                //       materialized into a virtual table on the router.
                let already_materialized = exec_plan.contains_vtable_for_motion(motion_id);

                if already_materialized {
                    OrderedSyntaxNodes::empty()
                } else if let MotionPolicy::LocalSegment { .. } | MotionPolicy::Local = policy {
                    let motion_child_id = exec_plan.get_motion_child(motion_id)?;
                    let sp = SyntaxPlan::new(&exec_plan, motion_child_id, Snapshot::Oldest)?;
                    OrderedSyntaxNodes::try_from(sp)?
                } else {
                    // In case we are not dealing with `LocalSegment` and `Local` policies, `exec_plan`
                    // must contain vtable for `motion_id` (See `dispatch` method in `src/executor.rs`)
                    // so we mustn't got here.
                    return Err(SbroadError::Invalid(
                        Entity::Plan,
                        Some(format_smolstr!(
                            "unsupported motion policy under DML node: {policy:?}",
                        )),
                    ));
                }
            }
        }
    };
    let vtables_meta = exec_plan.remove_vtables()?;
    let optional_data = OptionalData::new(exec_plan, ordered, vtables_meta);
    let optional_as_tuple = optional_data.to_tuple()?;
    Ok(optional_as_tuple.into())
}

/// Helper struct for storing optional data extracted
/// from router request.
///
/// It contains None, in case message from router
/// didn't contain optional data.
/// Otherwise it contains encoded optional data.
pub struct OptionalBytes(Option<Vec<u8>>);

impl OptionalBytes {
    const ERR_MSG: &'static str = "expected optional data in request";

    /// # Errors
    /// - Original request didn't contain optinal data
    pub fn get_mut(&mut self) -> Result<&mut Vec<u8>, SbroadError> {
        self.0
            .as_mut()
            .ok_or_else(|| SbroadError::Other(Self::ERR_MSG.into()))
    }

    /// # Errors
    /// - Original request didn't contain optinal data
    pub fn extract(self) -> Result<Vec<u8>, SbroadError> {
        self.0
            .ok_or_else(|| SbroadError::Other(Self::ERR_MSG.into()))
    }
}

pub type DecodeOutput = (Vec<u8>, OptionalBytes, CacheInfo);

/// Decode the execution plan from msgpack into a pair of binary data:
/// * required data (plan id, parameters, etc.)
/// * optional data (execution plan, etc.)
///
/// # Errors
/// - Failed to decode the execution plan.
pub fn decode_msgpack(tuple_buf: &[u8]) -> Result<DecodeOutput, SbroadError> {
    let mut stream = rmp::decode::Bytes::from(tuple_buf);
    let array_len = rmp::decode::read_array_len(&mut stream).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("array length: {e:?}"),
        )
    })? as usize;
    if array_len != 2 && array_len != 3 {
        return Err(SbroadError::Invalid(
            Entity::Tuple,
            Some(format_smolstr!(
                "expected tuple of 2 or 3 elements, got {array_len}"
            )),
        ));
    }

    // Decode required data.
    let req_array_len = rmp::decode::read_array_len(&mut stream).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("required array length: {e:?}"),
        )
    })? as usize;
    if req_array_len != 1 {
        return Err(SbroadError::Invalid(
            Entity::Tuple,
            Some(format_smolstr!(
                "expected array of 1 element in required, got {req_array_len}"
            )),
        ));
    }
    let req_data_len = rmp::decode::read_str_len(&mut stream).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("read required data length: {e:?}"),
        )
    })? as usize;
    let mut data: Vec<u8> = vec![0_u8; req_data_len];
    stream.read_exact_buf(&mut data).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("read required data: {e:?}"),
        )
    })?;

    let mut optional_data = None;
    if array_len == 3 {
        let opt_array_len = rmp::decode::read_array_len(&mut stream).map_err(|e| {
            SbroadError::FailedTo(
                Action::Decode,
                Some(Entity::MsgPack),
                format_smolstr!("optional array length: {e:?}"),
            )
        })? as usize;
        if opt_array_len != 1 {
            return Err(SbroadError::Invalid(
                Entity::Tuple,
                Some(format_smolstr!(
                    "expected array of 1 element in optional, got {opt_array_len}"
                )),
            ));
        }
        let opt_len = rmp::decode::read_str_len(&mut stream).map_err(|e| {
            SbroadError::FailedTo(
                Action::Decode,
                Some(Entity::MsgPack),
                format_smolstr!("read optional data string length: {e:?}"),
            )
        })? as usize;
        let mut optional: Vec<u8> = vec![0_u8; opt_len];
        stream.read_exact_buf(&mut optional).map_err(|e| {
            SbroadError::FailedTo(
                Action::Decode,
                Some(Entity::MsgPack),
                format_smolstr!("read optional data: {e:?}"),
            )
        })?;
        optional_data = Some(optional);
    }

    let cacheable_len = rmp::decode::read_str_len(&mut stream).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("read cacheable string length: {e:?}"),
        )
    })? as usize;
    let mut cacheable_bytes: Vec<u8> = vec![0_u8; cacheable_len];
    stream.read_exact_buf(&mut cacheable_bytes).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("read cacheable string: {e:?}"),
        )
    })?;
    let cache_info_str = from_utf8(&cacheable_bytes).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("cacheable string: {e:?}"),
        )
    })?;
    let cache_info = CacheInfo::from_str(cache_info_str).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("cacheable string: {e:?}"),
        )
    })?;

    Ok((data, OptionalBytes(optional_data), cache_info))
}

/// Decode dispatched optional data (execution plan, etc.) from msgpack
/// and compile it into a pattern with parameters and temporary space map.
///
/// # Errors
/// - Failed to decode or compile optional data.
pub fn compile_encoded_optional(
    raw_optional: &mut Vec<u8>,
    template: &str,
) -> Result<(PatternWithParams, Vec<TableGuard>), SbroadError> {
    let data = std::mem::take(raw_optional);
    let mut optional = OptionalData::try_from(EncodedOptionalData::from(data))?;
    compile_optional(&mut optional, template)
}

/// Compile already decoded optional data into a pattern with parameters
/// and temporary space map
///
/// # Errors
/// - Failed to compile optional data
pub fn compile_optional(
    optional: &mut OptionalData,
    template: &str,
) -> Result<(PatternWithParams, Vec<TableGuard>), SbroadError> {
    let nodes = optional.ordered.to_syntax_data()?;
    let vtables_meta = Some(&optional.vtables_meta);
    let (u, v) = optional.exec_plan.to_sql(&nodes, template, vtables_meta)?;
    Ok((u, v))
}

/// Command to build a tuple suitable to be passed into Tarantool API functions.
/// For more information see `TupleBuilderPattern` docs.
#[derive(Debug)]
pub enum TupleBuilderCommand {
    /// Take a value from the original tuple
    /// at the specified position.
    TakePosition(usize),
    /// Take a value from the original tuple and cast
    /// it into specified type.
    TakeAndCastPosition(usize, DerivedType),
    /// Set a specified value.
    /// Related only to the tuple we are currently constructing and not to the original tuple.
    SetValue(Value),
    /// Calculate a bucket_id for the new tuple
    /// using the specified motion key.
    CalculateBucketId(MotionKey),
    /// Update table column to the value in original tupleon specified position.
    /// Needed only for `Update`.
    UpdateColToPos(usize, usize),
    /// Update table column to the value in original tuple on specified position and cast it
    /// into specifeid type.
    /// Needed only for `Update`.
    UpdateColToCastedPos(usize, usize, DerivedType),
}

impl Encode for TupleBuilderCommand {
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        rmp::encode::write_array_len(w, 2)?;
        match self {
            Self::TakePosition(pos) => {
                write_pfix(w, 0)?;
                pos.encode(w, context)
            }
            Self::TakeAndCastPosition(pos, dt) => {
                write_pfix(w, 1)?;
                (pos, dt).encode(w, context)
            }
            Self::SetValue(val) => {
                write_pfix(w, 2)?;
                val.encode(w, context)
            }
            Self::CalculateBucketId(motion_key) => {
                write_pfix(w, 3)?;
                motion_key.encode(w, context)
            }
            Self::UpdateColToPos(col_pos, tuple_pos) => {
                write_pfix(w, 4)?;
                (*col_pos, *tuple_pos).encode(w, context)
            }
            Self::UpdateColToCastedPos(col_pos, tuple_pos, dt) => {
                write_pfix(w, 5)?;
                (*col_pos, *tuple_pos, dt).encode(w, context)
            }
        }
    }
}

impl<'de> Decode<'de> for TupleBuilderCommand {
    fn decode(r: &mut &'de [u8], context: &Context) -> Result<Self, DecodeError> {
        let len = rmp::decode::read_array_len(r).map_err(DecodeError::from_vre::<Self>)?;
        if len != 2 {
            return Err(DecodeError::new::<Self>(format!(
                "expected array of 2 elements, got {}",
                len
            )));
        }

        let marker = rmp::decode::read_pfix(r).map_err(DecodeError::from_vre::<Self>)?;
        match marker {
            0 => usize::decode(r, context).map(Self::TakePosition),
            1 => decode_from_read(r, context).map(|(pos, dt)| Self::TakeAndCastPosition(pos, dt)),
            2 => Value::decode(r, context).map(Self::SetValue),
            3 => MotionKey::decode(r, context).map(Self::CalculateBucketId),
            4 => decode_from_read(r, context)
                .map(|(col_pos, tuple_pos)| Self::UpdateColToPos(col_pos, tuple_pos)),
            5 => decode_from_read(r, context)
                .map(|(col_pos, tuple_pos, dt)| Self::UpdateColToCastedPos(col_pos, tuple_pos, dt)),
            _ => Err(DecodeError::new::<Self>(format!(
                "unexpected marker: {}",
                marker
            ))),
        }
    }
}

/// Vec of commands that helps us transforming `VTableTuple` into a tuple suitable to be passed
/// into Tarantool API functions (like `delete`, `update`, `replace` and others).
/// Each command in this vec operates on the same `VTableTuple`. E.g. taking some value from
/// it (on specified position) and putting it into the resulting tuple.
pub type TupleBuilderPattern = Vec<TupleBuilderCommand>;

/// Create commands to build the tuple for local update
///
/// # Errors
/// - Invalid update columns map
/// - Invalid primary key positions
pub fn init_local_update_tuple_builder(
    plan: &Plan,
    columns: &[Column],
    update_id: NodeId,
) -> Result<TupleBuilderPattern, SbroadError> {
    if let Relational::Update(Update {
        relation,
        update_columns_map,
        pk_positions,
        ..
    }) = plan.get_relation_node(update_id)?
    {
        let mut commands: TupleBuilderPattern =
            Vec::with_capacity(update_columns_map.len() + pk_positions.len());
        let rel = plan.get_relation_or_error(relation)?;
        for (table_pos, tuple_pos) in update_columns_map {
            let rel_type = &rel
                .columns
                .get(*table_pos)
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "invalid position in update table: {table_pos}"
                    ))
                })?
                .r#type;
            let vtable_type = &columns
                .get(*tuple_pos)
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "invalid position in update vtable: {tuple_pos}"
                    ))
                })?
                .r#type;
            if rel_type == vtable_type {
                commands.push(TupleBuilderCommand::UpdateColToPos(*table_pos, *tuple_pos));
            } else {
                commands.push(TupleBuilderCommand::UpdateColToCastedPos(
                    *table_pos, *tuple_pos, *rel_type,
                ));
            }
        }
        for (idx, pk_pos) in pk_positions.iter().enumerate() {
            let table_pos = *rel.primary_key.positions.get(idx).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Update,
                    Some(format_smolstr!(
                        "invalid primary key positions: len: {}, expected len: {}",
                        pk_positions.len(),
                        rel.primary_key.positions.len()
                    )),
                )
            })?;
            let rel_type = &rel
                .columns
                .get(table_pos)
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "invalid primary key position in table: {table_pos}"
                    ))
                })?
                .r#type;
            let vtable_type = &columns
                .get(*pk_pos)
                .ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Update,
                        Some(format_smolstr!("invalid pk position: {pk_pos}")),
                    )
                })?
                .r#type;
            if rel_type == vtable_type {
                commands.push(TupleBuilderCommand::TakePosition(*pk_pos));
            } else {
                commands.push(TupleBuilderCommand::TakeAndCastPosition(*pk_pos, *rel_type));
            }
        }
        return Ok(commands);
    }
    Err(SbroadError::Invalid(
        Entity::Node,
        Some(format_smolstr!("expected Update on id ({update_id:?})")),
    ))
}

/// Create commands to build the tuple for deletion
///
/// # Errors
/// - plan top is not Delete
pub fn init_delete_tuple_builder(
    plan: &Plan,
    delete_id: NodeId,
) -> Result<TupleBuilderPattern, SbroadError> {
    let table = plan.dml_node_table(delete_id)?;
    let mut commands = Vec::with_capacity(table.primary_key.positions.len());
    // For each query that contains delete we create Projection node
    // which includes only primary keys of table. Consult
    // `resolve_metadata` for more info.
    for pos in 0..table.primary_key.positions.len() {
        commands.push(TupleBuilderCommand::TakePosition(pos));
    }
    Ok(commands)
}

/// Create commands to build the tuple for insertion,
///
/// # Errors
/// - Invalid insert node or plan
pub fn init_insert_tuple_builder(
    plan: &Plan,
    columns: &[Column],
    insert_id: NodeId,
) -> Result<TupleBuilderPattern, SbroadError> {
    let insert_columns = plan.insert_columns(insert_id)?;
    // Revert map of { pos_in_child_node -> pos_in_relation }
    // into map of { pos_in_relation -> pos_in_child_node }.
    let columns_map: AHashMap<usize, usize> = insert_columns
        .iter()
        .enumerate()
        .map(|(pos, id)| (*id, pos))
        .collect::<AHashMap<_, _>>();
    let relation = plan.dml_node_table(insert_id)?;
    let mut commands = Vec::with_capacity(relation.columns.len());
    for (pos, table_col) in relation.columns.iter().enumerate() {
        if table_col.role == ColumnRole::Sharding {
            let motion_key = plan.insert_motion_key(insert_id)?;
            commands.push(TupleBuilderCommand::CalculateBucketId(motion_key));
        } else if columns_map.contains_key(&pos) {
            // It is safe to unwrap here because we have checked that
            // the column is present in the tuple.
            let tuple_pos = columns_map[&pos];
            let vtable_type = &columns
                .get(tuple_pos)
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "invalid index in virtual table: {tuple_pos}"
                    ))
                })?
                .r#type;
            let rel_type = &table_col.r#type;
            if vtable_type == rel_type {
                commands.push(TupleBuilderCommand::TakePosition(tuple_pos));
            } else {
                commands.push(TupleBuilderCommand::TakeAndCastPosition(
                    tuple_pos, *rel_type,
                ));
            }
        } else {
            // FIXME: support default values other then NULL (issue #442).
            commands.push(TupleBuilderCommand::SetValue(Column::default_value()));
        }
    }
    Ok(commands)
}

/// Convert vtable tuple to tuple
/// to be inserted.
///
/// # Errors
/// - Invalid commands to build the insert tuple
///
/// # Panics
/// - Bucket id not provided when inserting into sharded
///   table
pub fn build_insert_args<'t>(
    vt_tuple: &'t VTableTuple,
    builder: &'t TupleBuilderPattern,
    bucket_id: Option<&'t u64>,
) -> Result<Vec<EncodedValue<'t>>, SbroadError> {
    let mut insert_tuple = Vec::with_capacity(builder.len());
    for command in builder {
        // We don't produce any additional allocations as `MsgPackValue` keeps
        // a reference to the original value. The only allocation is for message
        // pack serialization, but it is unavoidable.
        match command {
            TupleBuilderCommand::TakePosition(tuple_pos) => {
                let value = vt_tuple.get(*tuple_pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            "column at position {tuple_pos} not found in virtual table"
                        )),
                    )
                })?;
                insert_tuple.push(EncodedValue::Ref(value.into()));
            }
            TupleBuilderCommand::TakeAndCastPosition(tuple_pos, table_type) => {
                let value = vt_tuple.get(*tuple_pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            "column at position {tuple_pos} not found in virtual table"
                        )),
                    )
                })?;
                insert_tuple.push(value.cast_and_encode(table_type)?);
            }
            TupleBuilderCommand::SetValue(value) => {
                insert_tuple.push(EncodedValue::Ref(MsgPackValue::from(value)));
            }
            TupleBuilderCommand::CalculateBucketId(_) => {
                let bucket_id = i64::try_from(*bucket_id.unwrap()).map_err(|_| {
                    SbroadError::Invalid(
                        Entity::Value,
                        Some(format_smolstr!(
                            "value for column 'bucket_id' is too large to fit in integer type range"
                        )),
                    )
                })?;

                insert_tuple.push(EncodedValue::Owned(Value::Integer(bucket_id)));
            }
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::Tuple,
                    Some(format_smolstr!(
                        "unexpected tuple builder command for insert: {command:?}"
                    )),
                ));
            }
        }
    }
    Ok(insert_tuple)
}

pub fn write_insert_args<'t>(
    vt_tuple: &'t VTableTuple,
    builder: &'t TupleBuilderPattern,
    bucket_id: Option<&'t u64>,
    w: &mut impl Write,
) -> Result<(), SbroadError> {
    for command in builder {
        match command {
            TupleBuilderCommand::TakePosition(tuple_pos) => {
                let value = vt_tuple.get(*tuple_pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            "column at position {tuple_pos} not found in virtual table"
                        )),
                    )
                })?;
                value.encode(w, &Context::DEFAULT).map_err(|e| {
                    SbroadError::Invalid(
                        Entity::Value,
                        Some(format_smolstr!("failed to encode value: {e}")),
                    )
                })?;
            }
            TupleBuilderCommand::TakeAndCastPosition(tuple_pos, table_type) => {
                let value = vt_tuple.get(*tuple_pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            "column at position {tuple_pos} not found in virtual table"
                        )),
                    )
                })?;
                value
                    .cast_and_encode(table_type)?
                    .encode(w, &Context::DEFAULT)
                    .map_err(|e| {
                        SbroadError::Invalid(
                            Entity::Value,
                            Some(format_smolstr!("failed to encode value: {e}")),
                        )
                    })?;
            }
            TupleBuilderCommand::SetValue(value) => {
                value.encode(w, &Context::DEFAULT).map_err(|e| {
                    SbroadError::Invalid(
                        Entity::Value,
                        Some(format_smolstr!("failed to encode value: {e}")),
                    )
                })?;
            }
            TupleBuilderCommand::CalculateBucketId(_) => {
                let bucket_id = i64::try_from(*bucket_id.unwrap()).map_err(|_| {
                    SbroadError::Invalid(
                        Entity::Value,
                        Some(format_smolstr!(
                            "value for column 'bucket_id' is too large to fit in integer type range"
                        )),
                    )
                })?;

                Value::Integer(bucket_id)
                    .encode(w, &Context::DEFAULT)
                    .map_err(|e| {
                        SbroadError::Invalid(
                            Entity::Value,
                            Some(format_smolstr!("failed to encode value: {e}")),
                        )
                    })?;
            }
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::Tuple,
                    Some(format_smolstr!(
                        "unexpected tuple builder command for insert: {command:?}"
                    )),
                ));
            }
        }
    }

    Ok(())
}

/// Create commands to build the tuple for sharded `Update`,
///
/// # Errors
/// - Invalid insert node or plan
pub fn init_sharded_update_tuple_builder(
    plan: &Plan,
    columns: &[Column],
    update_id: NodeId,
) -> Result<TupleBuilderPattern, SbroadError> {
    let Relational::Update(Update {
        update_columns_map, ..
    }) = plan.get_relation_node(update_id)?
    else {
        return Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!(
                "update tuple builder: expected update node on id: {update_id:?}"
            )),
        ));
    };
    let relation = plan.dml_node_table(update_id)?;
    let mut commands = Vec::with_capacity(relation.columns.len());
    for (pos, table_col) in relation.columns.iter().enumerate() {
        if table_col.role == ColumnRole::Sharding {
            // the bucket is taken from the index (see `execute_sharded_update` logic),
            // no need to specify motion key
            commands.push(TupleBuilderCommand::CalculateBucketId(MotionKey {
                targets: vec![],
            }));
        } else if update_columns_map.contains_key(&pos) {
            let tuple_pos = update_columns_map[&pos];
            let vtable_type = &columns
                .get(tuple_pos)
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "invalid index in virtual table: {tuple_pos}"
                    ))
                })?
                .r#type;
            let rel_type = &table_col.r#type;
            if vtable_type == rel_type {
                commands.push(TupleBuilderCommand::TakePosition(tuple_pos));
            } else {
                commands.push(TupleBuilderCommand::TakeAndCastPosition(
                    tuple_pos, *rel_type,
                ));
            }
        } else {
            // Note, that as soon as we're dealing with sharded update, `Projection` output below
            // the `Update` node must contain the same number of values as the updating table.
            // That's why `update_columns_map` must contain value for all the columns present in the
            // `relation`.

            return Err(SbroadError::Invalid(
                Entity::Update,
                Some(format_smolstr!(
                    "user column {pos} not found in update column map"
                )),
            ));
        }
    }
    Ok(commands)
}

pub fn write_shared_update_args<'t>(
    vt_tuple: &'t VTableTuple,
    builder: &'t TupleBuilderPattern,
    bucket_id: u64,
    w: &mut impl Write,
) -> Result<(), SbroadError> {
    for command in builder {
        match command {
            TupleBuilderCommand::TakePosition(tuple_pos) => {
                let value = vt_tuple.get(*tuple_pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            "column at position {tuple_pos} not found in virtual table"
                        )),
                    )
                })?;
                value.encode(w, &msgpack::Context::DEFAULT).map_err(|e| {
                    SbroadError::Invalid(
                        Entity::Value,
                        Some(format_smolstr!("failed to encode value: {e}")),
                    )
                })?;
            }
            TupleBuilderCommand::TakeAndCastPosition(tuple_pos, table_type) => {
                let value = vt_tuple.get(*tuple_pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            "column at position {tuple_pos} not found in virtual table"
                        )),
                    )
                })?;
                value
                    .cast_and_encode(table_type)?
                    .encode(w, &msgpack::Context::DEFAULT)
                    .map_err(|e| {
                        SbroadError::Invalid(
                            Entity::Value,
                            Some(format_smolstr!("failed to encode value: {e}")),
                        )
                    })?;
            }
            TupleBuilderCommand::CalculateBucketId(_) => {
                let bucket_id = i64::try_from(bucket_id).map_err(|_| {
                    SbroadError::Invalid(
                        Entity::Value,
                        Some(format_smolstr!(
                                        "value for column 'bucket_id' is too large to fit in integer type range"
                                    )),
                    )
                })?;

                Value::Integer(bucket_id)
                    .encode(w, &msgpack::Context::DEFAULT)
                    .map_err(|e| {
                        SbroadError::Invalid(
                            Entity::Value,
                            Some(format_smolstr!("failed to encode value: {e}")),
                        )
                    })?;
            }
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::TupleBuilderCommand,
                    Some(format_smolstr!("got command {command:?} for update insert")),
                ));
            }
        }
    }

    Ok(())
}

/// Format explain output into a tuple.
///
/// # Errors
/// - Failed to create a tuple.
pub fn explain_format(explain: &str) -> Result<Box<dyn Any>, SbroadError> {
    let e = explain.lines().collect::<Vec<&str>>();

    match Tuple::new(&[e]) {
        Ok(t) => Ok(Box::new(t)),
        Err(e) => Err(SbroadError::FailedTo(
            Action::Create,
            Some(Entity::Tuple),
            format_smolstr!("{e}"),
        )),
    }
}

/// Check if the plan has a LIMIT 0 clause.
/// For instance, it returns true for a query `SELECT * FROM T LIMIT 0`
/// and false for `SELECT * FROM T LIMIT 1`.
///
/// # Errors
/// - Invalid plan.
fn has_zero_limit_clause(plan: &ExecutionPlan) -> Result<bool, SbroadError> {
    let ir = plan.get_ir_plan();
    let top_id = ir.get_top()?;
    if let Relational::Limit(Limit { limit, .. }) = ir.get_relation_node(top_id)? {
        return Ok(*limit == 0);
    }
    Ok(false)
}

/// A helper function to dispatch the execution plan from the router to the storages.
///
/// # Errors
/// - Internal errors during the execution.
pub fn dispatch_impl<'p>(
    coordinator: &impl Router,
    plan: &mut ExecutionPlan,
    top_id: NodeId,
    buckets: &Buckets,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError> {
    if plan.get_ir_plan().is_empty() {
        empty_plan_write(port, plan)?;
        return Ok(());
    }

    let sub_plan = plan.take_subtree(top_id)?;

    let tier = {
        match sub_plan.get_ir_plan().tier.as_ref() {
            None => coordinator.get_current_tier_name()?,
            tier => tier.cloned(),
        }
    };
    let tier_runtime = coordinator.get_vshard_object_by_tier(tier.as_ref())?;
    if sub_plan.get_ir_plan().is_raw_explain() {
        if sub_plan.get_ir_plan().is_dml()? {
            return Err(SbroadError::Unsupported(
                Entity::Plan,
                Some("EXPLAIN QUERY PLAN is not supported for DML queries".into()),
            ));
        }
        tier_runtime.exec_ir_on_any_node(sub_plan, buckets, port)?;
        return Ok(());
    }

    if has_zero_limit_clause(&sub_plan)? {
        empty_plan_write(port, &sub_plan)?;
        return Ok(());
    }
    dispatch_by_buckets(sub_plan, buckets, &tier_runtime, port)?;
    Ok(())
}

/// Helper function that chooses one of the methods for execution
/// based on buckets.
///
/// # Errors
/// - Failed to dispatch
pub fn dispatch_by_buckets<'p>(
    sub_plan: ExecutionPlan,
    buckets: &Buckets,
    runtime: &impl Vshard,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError> {
    match buckets {
        Buckets::Any => {
            if sub_plan.has_customization_opcodes() {
                return Err(SbroadError::Invalid(
                    Entity::SubTree,
                    Some(
                        "plan customization is needed only when executing on multiple replicasets"
                            .into(),
                    ),
                ));
            }
            // Check that all vtables don't have index. Because if they do,
            // they will be filtered later by filter_vtable
            for (motion_id, vtable) in sub_plan.get_vtables() {
                if !vtable.get_bucket_index().is_empty() {
                    return Err(SbroadError::Invalid(
                        Entity::Motion,
                        Some(format_smolstr!("Motion ({motion_id:?}) in subtree with distribution Single, but policy is not Full.")),
                    ));
                }
            }
            runtime.exec_ir_on_any_node(sub_plan, buckets, port)?;
            Ok(())
        }
        Buckets::All | Buckets::Filtered(_) => {
            runtime.exec_ir_on_buckets(sub_plan, buckets, port)?;
            Ok(())
        }
    }
}

pub fn vtable_columns(plan: &Plan, top_id: NodeId) -> Result<Vec<Column>, SbroadError> {
    let top = plan.get_relation_node(top_id)?;
    let output_id = top.output();
    let columns = plan.get_row_list(output_id)?;
    let mut res = Vec::with_capacity(columns.len());
    for (pos, col_id) in columns.iter().enumerate() {
        let col = plan.get_expression_node(*col_id)?;
        let name = vtable_indexed_column_name(pos);
        let col_type = col.calculate_type(plan)?;
        res.push(Column::new(name.as_str(), col_type, ColumnRole::User, true));
    }
    Ok(res)
}

/// Helper function reused in the router trait method of the same name.
///
/// # Errors
/// - Types mismatch.
///
/// # Panics
/// - Passed node is not Values.
pub fn materialize_values(
    runtime: &impl Router,
    exec_plan: &mut ExecutionPlan,
    values_id: NodeId,
) -> Result<VirtualTable, SbroadError> {
    let child_node = exec_plan.get_ir_plan().get_node(values_id)?;

    let Node::Relational(Relational::Values(Values {
        ref children,
        output,
    })) = child_node
    else {
        panic!("Values node expected. Got {child_node:?}.")
    };

    let children = children.clone();
    let output = *output;

    let mut vtable = VirtualTable::new();
    vtable.get_mut_tuples().reserve(children.len());

    let first_row_id = children
        .first()
        .expect("Values node must contain children.");
    let row_node = exec_plan.get_ir_plan().get_relation_node(*first_row_id)?;
    let Relational::ValuesRow(ValuesRow { data, .. }) = row_node else {
        panic!("Expected ValuesRow, got {row_node:?}.")
    };
    let columns_len = exec_plan
        .get_ir_plan()
        .get_expression_node(*data)?
        .get_row_list()?
        .len();

    // Check if there is only constant children, e.g. `VALUES (1,2,3)`. In that case we can
    // materialize VALUES locally avoiding expensive `dispatch()` call.
    let mut only_constants = true;
    'rows_loop: for row_id in &children {
        let row_node = exec_plan.get_ir_plan().get_relation_node(*row_id)?;
        let Relational::ValuesRow(ValuesRow { data, .. }) = row_node else {
            panic!("Expected ValuesRow under Values. Got {row_node:?}.")
        };
        for column_id in exec_plan.get_ir_plan().get_row_list(*data)? {
            let column_node = exec_plan.get_ir_plan().get_node(*column_id)?;
            if !matches!(column_node, Node::Expression(Expression::Constant(_))) {
                only_constants = false;
                break 'rows_loop;
            }
        }
    }

    if only_constants {
        // All children are constants, can materialize VALUES locally and take all constants.
        vtable.get_mut_columns().reserve(children.len());
        for row_id in &children {
            let row_node = exec_plan.get_mut_ir_plan().get_relation_node(*row_id)?;
            let Relational::ValuesRow(ValuesRow { data, .. }) = row_node else {
                panic!("Expected ValuesRow under Values. Got {row_node:?}.")
            };

            let data = *data;
            let mut row: VTableTuple = Vec::with_capacity(columns_len);
            for idx in 0..columns_len {
                let plan = exec_plan.get_mut_ir_plan();
                let column_id = plan.get_row_list(data)?[idx];
                let column_node = plan.get_mut_expression_node(column_id)?;
                let MutExpression::Constant(Constant { ref mut value, .. }) = column_node else {
                    unreachable!("checked before that there can be only constants");
                };
                // Take the value avoiding cloning.
                row.push(std::mem::replace(value, Value::Null));
            }

            vtable.add_tuple(row);
        }
    }

    let mut column_names: Vec<SmolStr> = Vec::new();
    let output_cols = exec_plan.get_ir_plan().get_row_list(output)?;
    for column_id in output_cols {
        let alias = exec_plan.get_ir_plan().get_expression_node(*column_id)?;
        if let Expression::Alias(Alias { name, .. }) = alias {
            column_names.push(name.clone());
        } else {
            panic!("Output column ({column_id}) is not an alias node.")
        }
    }

    let mut vtable = if only_constants {
        // Create vtable columns with default column field (that will be fixed later).
        let columns = vtable.get_mut_columns();
        columns.reserve(column_names.len());
        for _ in 0..columns_len {
            let column = Column::default();
            columns.push(column);
        }
        vtable
    } else {
        // We need to execute VALUES as a local SQL.
        let columns = vtable_columns(exec_plan.get_ir_plan(), values_id)?;

        let mut port = runtime.new_port();
        runtime.dispatch(exec_plan, values_id, &Buckets::Any, &mut port)?;

        let mut vtable = VirtualTable::with_columns(columns);
        for mp in port.iter().skip(1) {
            vtable.write_all(mp).map_err(|e| {
                SbroadError::FailedTo(
                    Action::Create,
                    Some(Entity::VirtualTable),
                    format_smolstr!("{e}"),
                )
            })?;
        }
        vtable
    };

    // This isn't cheap and should be avoided once we support type coercions.
    // (https://git.picodata.io/core/picodata/-/issues/1812)
    let vtable_types = vtable.get_types();
    let unified_types = calculate_unified_types(vtable_types)?;
    vtable.cast_values(&unified_types)?;

    let _ = exec_plan.get_mut_ir_plan().replace_with_stub(values_id);

    Ok(vtable)
}

/// Materialize a motion subtree into a virtual table.
///
/// # Errors
/// - Internal errors during the execution.
///
/// # Panics
/// - Plan is in inconsistent state.
/// - query is dml
pub fn materialize_motion(
    runtime: &impl Router,
    plan: &mut ExecutionPlan,
    motion_node_id: NodeId,
    buckets: &Buckets,
) -> Result<VirtualTable, SbroadError> {
    let top_id = plan.get_motion_subtree_root(motion_node_id)?;

    let ir = plan.get_ir_plan();
    let top_node = ir.get_relation_node(top_id)?;
    assert!(
        !top_node.is_dml(),
        "materialize motion can be called only for DQL queries"
    );

    // We should get a motion alias name before we take the subtree in `dispatch` method.
    let motion_node = plan.get_ir_plan().get_relation_node(motion_node_id)?;
    let alias = if let Relational::Motion(Motion { alias, .. }) = motion_node {
        alias.clone()
    } else {
        panic!("Expected motion node, got {motion_node:?}");
    };

    let columns = vtable_columns(plan.get_ir_plan(), top_id)?;

    let mut port = runtime.new_port();
    // Dispatch the motion subtree (it will be replaced with invalid values).
    runtime.dispatch(plan, top_id, buckets, &mut port)?;

    if !plan.get_ir_plan().is_dml_on_global_table()? {
        // Unlink motion node's child sub tree (it is already replaced with invalid values).
        plan.unlink_motion_subtree(motion_node_id)?;
    } else {
        // In case of global DML requests we must leave the tree unchanged,
        // because the DML portion of the query may fail due to CAS errors and
        // then the DQL part must be re-executed again
    }

    let mut vtable = VirtualTable::with_columns(columns);
    if let Some(name) = alias {
        vtable.set_alias(name.as_str());
    }
    if plan.get_ir_plan().is_raw_explain() {
        for mp in port.iter() {
            vtable.add_mp_unchecked(mp).map_err(|e| {
                SbroadError::FailedTo(
                    Action::Create,
                    Some(Entity::VirtualTable),
                    format_smolstr!("{e}"),
                )
            })?;
        }
    } else {
        for mp in port.iter().skip(1) {
            vtable.write_all(mp).map_err(|e| {
                SbroadError::FailedTo(
                    Action::Create,
                    Some(Entity::VirtualTable),
                    format_smolstr!("{e}"),
                )
            })?;
        }
    }
    Ok(vtable)
}

/// Function that is called from `exec_ir_on_some_buckets`.
/// Its purpose is to iterate through every vtable presented in `plan` subtree and
/// to replace them by new vtables. New vtables indices (map bucket id -> tuples) will contain
/// only pairs corresponding to buckets, that are presented in given `bucket_ids` (as we are going
/// to execute `plan` subtree only on them).
///
/// # Errors
/// - failed to build a new virtual table with the passed set of buckets
pub fn filter_vtable(plan: &mut ExecutionPlan, bucket_ids: &[u64]) -> Result<(), SbroadError> {
    for rc_vtable in plan.get_mut_vtables().values_mut() {
        // If the virtual table id hashed by the bucket_id, we can filter its tuples.
        // Otherwise (full motion policy) we need to preserve all tuples.
        if !rc_vtable.get_bucket_index().is_empty() {
            *rc_vtable = Rc::new(rc_vtable.new_with_buckets(bucket_ids)?);
        }
    }
    Ok(())
}

/// A common function for all engines to calculate the sharding key value from a tuple.
///
/// # Errors
/// - The space was not found in the metadata.
/// - The sharding key are not present in the space.
pub fn sharding_key_from_tuple<'tuple>(
    conf: &impl Metadata,
    space: &str,
    tuple: &'tuple [Value],
) -> Result<Vec<&'tuple Value>, SbroadError> {
    let sharding_positions = conf.sharding_positions_by_space(space)?;
    let mut sharding_tuple = Vec::with_capacity(sharding_positions.len());
    let table_col_amount = conf.table(space)?.columns.len();
    if table_col_amount == tuple.len() {
        // The tuple contains a "bucket_id" column.
        for position in &sharding_positions {
            let value = tuple.get(*position).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::ShardingKey,
                    format_smolstr!("position {position:?} in the tuple {tuple:?}"),
                )
            })?;
            sharding_tuple.push(value);
        }
        Ok(sharding_tuple)
    } else if table_col_amount == tuple.len() + 1 {
        // The tuple doesn't contain the "bucket_id" column.
        let table = conf.table(space)?;
        let bucket_position = table.get_bucket_id_position()?.ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Space,
                Some("global space does not have a sharding key!".into()),
            )
        })?;

        // If the "bucket_id" splits the sharding key, we need to shift the sharding
        // key positions of the right part by one.
        // For example, we have a table with columns a, bucket_id, b, and the sharding
        // key is (a, b). Then the sharding key positions are (0, 2).
        // If someone gives us a tuple (42, 666) we should tread is as (42, null, 666).
        for position in &sharding_positions {
            let corrected_pos = match position.cmp(&bucket_position) {
                Ordering::Less => *position,
                Ordering::Equal => {
                    return Err(SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            r#"the tuple {tuple:?} contains a "bucket_id" position {position} in a sharding key {sharding_positions:?}"#
                        )),
                    ));
                }
                Ordering::Greater => *position - 1,
            };
            let value = tuple.get(corrected_pos).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::ShardingKey,
                    format_smolstr!("position {corrected_pos:?} in the tuple {tuple:?}"),
                )
            })?;
            sharding_tuple.push(value);
        }
        Ok(sharding_tuple)
    } else {
        Err(SbroadError::Invalid(
            Entity::Tuple,
            Some(format_smolstr!(
                "the tuple {:?} was expected to have {} filed(s), got {}.",
                tuple,
                table_col_amount - 1,
                tuple.len()
            )),
        ))
    }
}

#[deprecated(note = "Remove in next release. Used for smooth upgrade")]
pub fn old_populate_table(
    motion_id: &NodeId,
    plan_id: &SmolStr,
    vtables: &EncodedVTables,
) -> Result<(), SbroadError> {
    let data = vtables.get(motion_id).ok_or_else(|| {
        SbroadError::NotFound(
            Entity::Table,
            format_smolstr!(
                "with id {motion_id} among encoded virtual tables {:?}",
                vtables.keys()
            ),
        )
    })?;
    with_su(ADMIN_ID, || -> Result<(), SbroadError> {
        let name = table_name(plan_id, *motion_id);
        let space = Space::find(&name).ok_or_else(|| {
            // See https://git.picodata.io/core/picodata/-/issues/1859.
            SbroadError::Invalid(
                Entity::Space,
                Some(format_smolstr!(
                    "Temporary SQL table {name} not found. \
                    Probably there are unused motions in the plan"
                )),
            )
        })?;
        for tuple in data.iter() {
            match space.insert(&tuple) {
                Ok(_) => {}
                Err(e) => {
                    // It is possible that the temporary table was recreated by admin
                    // user with a different format. We should not panic in this case.
                    return Err(SbroadError::FailedTo(
                        Action::Insert,
                        Some(Entity::Tuple),
                        format_smolstr!("tuple {tuple:?}, temporary table {name}: {e}"),
                    ));
                }
            }
        }
        Ok(())
    })??;
    Ok(())
}

pub fn drop_tables(tables: Vec<(SmolStr, bool)>) -> Result<(), SbroadError> {
    for (name, already_created) in tables {
        if already_created {
            continue;
        }

        let cleanup = |space: Space| match with_su(ADMIN_ID, || space.drop()) {
            Ok(_) => {}
            Err(e) => {
                error!(
                    Option::from("Temporary space"),
                    &format!("Failed to drop {name}: {e}")
                );
            }
        };

        if let Some(space) = with_su(ADMIN_ID, || Space::find(name.as_str()))? {
            cleanup(space);
        }
    }

    Ok(())
}

#[deprecated(note = "Remove in next release. Used for smooth upgrade")]
pub fn old_truncate_tables(table_ids: &[NodeId], plan_id: &SmolStr) {
    with_su(ADMIN_ID, || {
        for node_id in table_ids {
            let name = table_name(plan_id, *node_id);
            if let Some(space) = Space::find(&name) {
                space
                    .truncate()
                    .expect("failed to truncate temporary table");
            }
        }
    })
    .expect("failed to switch to admin user");
}

pub trait RequiredPlanInfo {
    fn id(&self) -> &SmolStr;
    fn params(&self) -> &Vec<Value>;
    fn schema_info(&self) -> &SchemaInfo;
    fn sql_vdbe_opcode_max(&self) -> u64;
    fn sql_motion_row_max(&self) -> u64;
    fn extract_data(&mut self) -> EncodedVTables;
}

pub trait FullPlanInfo: RequiredPlanInfo {
    /// Extracts the query and vtables meta from the plan.
    ///
    /// # Errors
    /// - Failed to extract query and vtables meta.
    fn take_query_meta(&mut self) -> Result<(String, Vec<NodeId>, VTablesMeta), SbroadError>;
}

pub struct QueryInfo<'data> {
    optional: &'data mut OptionalData,
    required: &'data mut RequiredData,
}

impl<'data> QueryInfo<'data> {
    pub fn new(optional: &'data mut OptionalData, required: &'data mut RequiredData) -> Self {
        Self { optional, required }
    }
}

impl RequiredPlanInfo for QueryInfo<'_> {
    fn id(&self) -> &SmolStr {
        &self.required.plan_id
    }

    fn params(&self) -> &Vec<Value> {
        &self.required.parameters
    }

    fn schema_info(&self) -> &SchemaInfo {
        &self.required.schema_info
    }

    fn sql_vdbe_opcode_max(&self) -> u64 {
        self.required.options.sql_vdbe_opcode_max as u64
    }

    fn sql_motion_row_max(&self) -> u64 {
        self.required.options.sql_motion_row_max as u64
    }

    fn extract_data(&mut self) -> EncodedVTables {
        std::mem::take(&mut self.required.vtables)
    }
}

impl FullPlanInfo for QueryInfo<'_> {
    fn take_query_meta(&mut self) -> Result<(String, Vec<NodeId>, VTablesMeta), SbroadError> {
        let nodes = self.optional.ordered.to_syntax_data()?;
        let (local_sql, motion_ids) = self.optional.exec_plan.generate_sql(
            &nodes,
            self.required.plan_id(),
            Some(&self.optional.vtables_meta),
            |name: &str, id| table_name(name, id),
        )?;

        let meta = std::mem::take(&mut self.optional.vtables_meta);

        Ok((local_sql, motion_ids, meta))
    }
}

pub struct EncodedQueryInfo<'data> {
    optional: Option<OptionalData>,
    required: &'data mut RequiredData,
}

impl<'data> EncodedQueryInfo<'data> {
    pub fn new(optional: Option<OptionalData>, required: &'data mut RequiredData) -> Self {
        Self { optional, required }
    }
}

impl RequiredPlanInfo for EncodedQueryInfo<'_> {
    fn id(&self) -> &SmolStr {
        &self.required.plan_id
    }

    fn params(&self) -> &Vec<Value> {
        &self.required.parameters
    }

    fn schema_info(&self) -> &SchemaInfo {
        &self.required.schema_info
    }

    fn sql_vdbe_opcode_max(&self) -> u64 {
        self.required.options.sql_vdbe_opcode_max as u64
    }

    fn sql_motion_row_max(&self) -> u64 {
        self.required.options.sql_motion_row_max as u64
    }

    fn extract_data(&mut self) -> EncodedVTables {
        std::mem::take(&mut self.required.vtables)
    }
}

impl FullPlanInfo for EncodedQueryInfo<'_> {
    fn take_query_meta(&mut self) -> Result<(String, Vec<NodeId>, VTablesMeta), SbroadError> {
        let optional = self.optional.take().ok_or_else(|| {
            SbroadError::Invalid(
                Entity::OptionalData,
                Some("optional data is missing".into()),
            )
        })?;
        let nodes = optional.ordered.to_syntax_data()?;
        let (local_sql, motion_ids) = optional.exec_plan.generate_sql(
            &nodes,
            self.required.plan_id(),
            Some(&optional.vtables_meta),
            |name: &str, id| table_name(name, id),
        )?;

        Ok((local_sql, motion_ids, optional.vtables_meta))
    }
}

pub struct UpdateArgs<'vtable_tuple> {
    pub key_tuple: Vec<EncodedValue<'vtable_tuple>>,
    pub ops: Vec<[EncodedValue<'vtable_tuple>; 3]>,
}

pub fn eq_op() -> &'static Value {
    // Once lock is used because of concurrent access in tests.
    static EQ: OnceLock<Value> = OnceLock::new();

    EQ.get_or_init(|| Value::String("=".into()))
}

/// Convert vtable tuple to tuple
/// to update args.
///
/// # Errors
/// - Invalid commands to build the insert tuple
pub fn build_update_args<'t>(
    vt_tuple: &'t VTableTuple,
    builder: &TupleBuilderPattern,
) -> Result<UpdateArgs<'t>, SbroadError> {
    let mut ops = Vec::with_capacity(builder.len());
    let mut key_tuple = Vec::with_capacity(builder.len());
    for command in builder {
        match command {
            TupleBuilderCommand::UpdateColToPos(table_col, pos) => {
                let value = vt_tuple.get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            "column at position {pos} not found in virtual table"
                        )),
                    )
                })?;
                let op = [
                    EncodedValue::Ref(MsgPackValue::from(eq_op())),
                    // Use `as i64` quite safe here.
                    EncodedValue::Owned(Value::Integer(*table_col as i64)),
                    EncodedValue::Ref(MsgPackValue::from(value)),
                ];
                ops.push(op);
            }
            TupleBuilderCommand::TakePosition(pos) => {
                let value = vt_tuple.get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            "column at position {pos} not found in virtual table"
                        )),
                    )
                })?;
                key_tuple.push(EncodedValue::Ref(MsgPackValue::from(value)));
            }
            TupleBuilderCommand::TakeAndCastPosition(pos, table_type) => {
                let value = vt_tuple.get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            "column at position {pos} not found in virtual table"
                        )),
                    )
                })?;
                key_tuple.push(value.cast_and_encode(table_type)?);
            }
            TupleBuilderCommand::UpdateColToCastedPos(table_col, pos, table_type) => {
                let value = vt_tuple.get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            "column at position {pos} not found in virtual table"
                        )),
                    )
                })?;
                let op = [
                    EncodedValue::Ref(MsgPackValue::from(eq_op())),
                    // Use `as i64` quite safe here.
                    EncodedValue::Owned(Value::Integer(*table_col as i64)),
                    value.cast_and_encode(table_type)?,
                ];
                ops.push(op);
            }
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::TupleBuilderCommand,
                    Some(format_smolstr!("got command {command:?} for update")),
                ));
            }
        }
    }

    Ok(UpdateArgs { key_tuple, ops })
}

/// Convert vtable tuple to tuple
/// for deletion.
///
/// # Errors
/// - Invalid commands to build the insert tuple
pub fn build_delete_args<'t>(
    vt_tuple: &'t VTableTuple,
    builder: &'t TupleBuilderPattern,
) -> Result<Vec<EncodedValue<'t>>, SbroadError> {
    let mut delete_tuple = Vec::with_capacity(builder.len());
    for cmd in builder {
        if let TupleBuilderCommand::TakePosition(pos) = cmd {
            let value = vt_tuple.get(*pos).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Tuple,
                    Some(format_smolstr!(
                        "column at position {pos} not found in the delete virtual table"
                    )),
                )
            })?;
            delete_tuple.push(EncodedValue::Ref(value.into()));
        } else {
            return Err(SbroadError::Invalid(
                Entity::Tuple,
                Some(format_smolstr!(
                    "unexpected tuple builder cmd for delete primary key: {cmd:?}"
                )),
            ));
        }
    }
    Ok(delete_tuple)
}

/// A common function for all engines to calculate the sharding key value from a `map`
/// of { `column_name` -> value }. Used as a helper function of `extract_sharding_keys_from_map`
/// that is called from `calculate_bucket_id`. `map` must contain a value for each sharding
/// column that is present in `space`.
///
/// # Errors
/// - The space was not found in the metadata.
/// - The sharding key is not present in the space.
pub fn sharding_key_from_map<'rec, S: ::std::hash::BuildHasher>(
    conf: &impl Metadata,
    space: &str,
    map: &'rec HashMap<SmolStr, Value, S>,
) -> Result<Vec<&'rec Value>, SbroadError> {
    let sharding_key = conf.sharding_key_by_space(space)?;
    let quoted_map = map
        .keys()
        .map(|k| (k.to_smolstr(), k.as_str()))
        .collect::<HashMap<SmolStr, &str>>();
    let mut tuple = Vec::with_capacity(sharding_key.len());
    for quoted_column in &sharding_key {
        if let Some(column) = quoted_map.get(quoted_column) {
            let value = map.get(*column).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::ShardingKey,
                    format_smolstr!("column {column:?} in the map {map:?}"),
                )
            })?;
            tuple.push(value);
        } else {
            return Err(SbroadError::NotFound(
                Entity::ShardingKey,
                format_smolstr!(
                    "(quoted) column {quoted_column:?} in the quoted map {quoted_map:?} (original map: {map:?})"
                )));
        }
    }
    Ok(tuple)
}

/// Try to get metadata from the plan. If the plan is not dql, `None` is returned.
///
/// # Errors
/// - Invalid execution plan.
pub fn try_get_metadata_from_plan(
    plan: &ExecutionPlan,
) -> Result<Option<Vec<MetadataColumn>>, SbroadError> {
    fn is_dql_exec_plan(plan: &ExecutionPlan) -> Result<bool, SbroadError> {
        let ir = plan.get_ir_plan();
        Ok(matches!(plan.query_type()?, QueryType::DQL) && !ir.is_explain())
    }

    if !is_dql_exec_plan(plan)? {
        return Ok(None);
    }

    // Get metadata (column types) from the top node's output tuple.
    let ir = plan.get_ir_plan();
    let top_id = ir.get_top()?;
    let top_output_id = ir.get_relation_node(top_id)?.output();
    let columns = ir.get_row_list(top_output_id)?;
    let mut metadata = Vec::with_capacity(columns.len());
    for col_id in columns {
        let column = ir.get_expression_node(*col_id)?;
        let column_type = column.calculate_type(ir)?.to_string();
        let column_name = if let Expression::Alias(Alias { name, .. }) = column {
            name.to_string()
        } else {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(smol_str::format_smolstr!("expected alias, got {column:?}")),
            ));
        };
        metadata.push(MetadataColumn::new(column_name, column_type));
    }
    Ok(Some(metadata))
}

#[inline(always)]
fn to_mp_err(msg: SmolStr) -> SbroadError {
    SbroadError::FailedTo(Action::Encode, Some(Entity::MsgPack), msg)
}

fn metadata_write<'p>(port: &mut impl Port<'p>, plan: &Plan) -> Result<(), SbroadError> {
    let top_id = plan.get_top()?;
    let top_output_id = plan.get_relation_node(top_id)?.output();
    let columns = plan.get_row_list(top_output_id)?;
    let mut mp: Vec<u8> = Vec::new();
    let len = u32::try_from(columns.len()).map_err(|e| {
        SbroadError::Invalid(
            Entity::Plan,
            Some(format_smolstr!("Too many columns to dump metadata: {e}")),
        )
    })?;
    write_array_len(&mut mp, len).map_err(|e| to_mp_err(format_smolstr!("{e}")))?;
    for col_id in columns {
        let column = plan.get_expression_node(*col_id)?;
        let col_type = column.calculate_type(plan)?.to_string();
        let Expression::Alias(Alias { name, .. }) = column else {
            return Err(to_mp_err("Expected column to be an alias".into()));
        };

        write_map_len(&mut mp, 2).map_err(|e| to_mp_err(format_smolstr!("{e}")))?;
        write_str(&mut mp, "name").map_err(|e| to_mp_err(format_smolstr!("{e}")))?;
        write_str(&mut mp, name).map_err(|e| to_mp_err(format_smolstr!("{e}")))?;
        write_str(&mut mp, "type").map_err(|e| to_mp_err(format_smolstr!("{e}")))?;
        write_str(&mut mp, &col_type).map_err(|e| to_mp_err(format_smolstr!("{e}")))?;
    }
    port.add_mp(&mp);
    Ok(())
}

pub fn empty_plan_write<'p>(
    port: &mut impl Port<'p>,
    plan: &ExecutionPlan,
) -> Result<(), SbroadError> {
    let query_type = plan.query_type()?;
    match query_type {
        QueryType::DML => {
            port.add_mp(b"\xcc\x00".as_ref());
        }
        QueryType::DQL => {
            metadata_write(port, plan.get_ir_plan())?;
        }
    }
    Ok(())
}
