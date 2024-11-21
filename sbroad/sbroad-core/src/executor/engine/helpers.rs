use ahash::AHashMap;

use crate::{
    error,
    ir::node::{
        expression::Expression, relational::Relational, Alias, Constant, Limit, Motion, NodeId,
        Update, Values, ValuesRow,
    },
    utils::MutexLike,
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
use tarantool::transaction::transaction;
use tarantool::{
    error::{Error, TarantoolErrorCode},
    tuple::RawBytes,
};
use tarantool::{space::Space, tuple::TupleBuffer};

use crate::backend::sql::space::{TableGuard, ADMIN_ID};
use crate::executor::engine::helpers::storage::{execute_prepared, execute_unprepared, prepare};
use crate::executor::engine::{QueryCache, StorageCache};
use crate::executor::protocol::{EncodedTables, SchemaInfo};
use crate::ir::node::Node;
use crate::ir::operator::ConflictStrategy;
use crate::ir::value::{EncodedValue, LuaValue, MsgPackValue};
use crate::utils::ByteCounter;
use crate::{
    backend::sql::{
        ir::PatternWithParams,
        tree::{OrderedSyntaxNodes, SyntaxPlan},
    },
    debug,
    errors::{Action, Entity, SbroadError},
    executor::{
        bucket::Buckets,
        ir::{ExecutionPlan, QueryType},
        protocol::{Binary, EncodedOptionalData, OptionalData, RequiredData},
        result::{ConsumerResult, MetadataColumn, ProducerResult},
        vtable::{calculate_vtable_unified_types, VTableTuple, VirtualTable},
    },
    ir::{
        relation::{Column, ColumnRole, Type},
        transformation::redistribution::{MotionKey, MotionPolicy},
        tree::Snapshot,
        value::Value,
        Plan,
    },
};
use serde::Serialize;
use tarantool::msgpack::rmp::{self, decode::RmpRead};
use tarantool::session::with_su;
use tarantool::tuple::Tuple;

use self::{
    storage::{dql_cache_miss_result, StorageReturnFormat},
    vshard::CacheInfo,
};

use super::{ConvertToDispatchResult, DispatchReturnFormat, Metadata, Router, Vshard};

pub mod proxy;
pub mod storage;
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

/// Generate a temporary table name for the specified motion node.
#[must_use]
pub fn table_name(plan_id: &str, node_id: NodeId) -> SmolStr {
    let base = {
        #[cfg(feature = "mock")]
        {
            format_smolstr!("TMP_{plan_id}")
        }
        #[cfg(not(feature = "mock"))]
        {
            use hash32::{Hasher, Murmur3Hasher};

            let mut hasher = Murmur3Hasher::default();
            hasher.write(plan_id.as_bytes());
            let id = hasher.finish();
            format_smolstr!("TMP_{id}")
        }
    };
    format_smolstr!("{base}_{node_id}")
}

/// Generate a primary key name for the specified motion node.
#[must_use]
pub fn pk_name(plan_id: &str, node_id: NodeId) -> SmolStr {
    format_smolstr!("PK_{plan_id}_{node_id}")
}

/// # Errors
/// - Invalid plan
/// - Serialization errors
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
    let params = exec_plan.to_params()?;
    let tables = exec_plan.encode_vtables();
    let router_version_map = std::mem::take(&mut exec_plan.get_mut_ir_plan().version_map);
    let schema_info = SchemaInfo::new(router_version_map);
    let required = RequiredData::new(
        sub_plan_id,
        params,
        query_type,
        exec_plan.get_ir_plan().options.clone(),
        schema_info,
        tables,
    );
    let required_as_tuple = required.to_tuple()?;
    Ok(required_as_tuple.into())
}

/// # Errors
/// - Invalid plan
/// - Serialization errors
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
    optional.exec_plan.get_mut_ir_plan().restore_constants()?;
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
    TakeAndCastPosition(usize, Type),
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
    UpdateColToCastedPos(usize, usize, Type),
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
    vtable: &VirtualTable,
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
            let vtable_type = &vtable
                .get_columns()
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
            let vtable_type = &vtable
                .get_columns()
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
    for pos in &table.primary_key.positions {
        commands.push(TupleBuilderCommand::TakePosition(*pos));
    }
    Ok(commands)
}

/// Create commands to build the tuple for insertion,
///
/// # Errors
/// - Invalid insert node or plan
pub fn init_insert_tuple_builder(
    plan: &Plan,
    vtable: &VirtualTable,
    insert_id: NodeId,
) -> Result<TupleBuilderPattern, SbroadError> {
    let columns = plan.insert_columns(insert_id)?;
    // Revert map of { pos_in_child_node -> pos_in_relation }
    // into map of { pos_in_relation -> pos_in_child_node }.
    let columns_map: AHashMap<usize, usize> = columns
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
            let vtable_type = &vtable
                .get_columns()
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
                insert_tuple.push(value.cast(table_type)?);
            }
            TupleBuilderCommand::SetValue(value) => {
                insert_tuple.push(EncodedValue::Ref(MsgPackValue::from(value)));
            }
            TupleBuilderCommand::CalculateBucketId(_) => {
                insert_tuple.push(EncodedValue::Ref(MsgPackValue::Unsigned(
                    bucket_id.unwrap(),
                )));
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

/// Create commands to build the tuple for sharded `Update`,
///
/// # Errors
/// - Invalid insert node or plan
fn init_sharded_update_tuple_builder(
    plan: &Plan,
    vtable: &VirtualTable,
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
            let vtable_type = &vtable
                .get_columns()
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

/// Generate an empty result for the specified plan:
/// * In case of DML (INSERT) return result with `row_count` equal to 0.
/// * In case of DQL (SELECT) return columns metadata with no result tuples.
///   * **Note**: Returns `None` if the plan is with replicated output (another words query
///     that may be executed on any node) of the top node (i.e. `select from values`).
///
/// # Errors
/// - failed to get query type;
/// - failed to get top node;
/// - the top node is not a valid relation node;
pub fn empty_query_result(
    plan: &ExecutionPlan,
    return_format: DispatchReturnFormat,
) -> Result<Box<dyn Any>, SbroadError> {
    let query_type = plan.query_type()?;
    match query_type {
        QueryType::DML => {
            let result = ConsumerResult::default();
            let tuple = Tuple::new(&[result])
                .map_err(|e| SbroadError::Invalid(Entity::Tuple, Some(format_smolstr!("{e:?}"))))?;
            Ok(Box::new(tuple) as Box<dyn Any>)
        }
        QueryType::DQL => {
            // Get metadata (column types) from the top node's output tuple.
            let ir_plan = plan.get_ir_plan();
            let top_id = ir_plan.get_top()?;
            let top_output_id = ir_plan.get_relation_node(top_id)?.output();
            let columns = ir_plan.get_row_list(top_output_id)?;
            let mut metadata = Vec::with_capacity(columns.len());
            for col_id in columns {
                let column = ir_plan.get_expression_node(*col_id)?;
                let column_type = column.calculate_type(ir_plan)?;
                let column_name = if let Expression::Alias(Alias { name, .. }) = column {
                    name.clone()
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some(format_smolstr!("expected alias, got {column:?}")),
                    ));
                };
                metadata.push(MetadataColumn::new(
                    column_name.to_string(),
                    column_type.to_string(),
                ));
            }
            let result = ProducerResult {
                metadata,
                ..Default::default()
            };

            let tuple = Tuple::new(&[result])
                .map_err(|e| SbroadError::Invalid(Entity::Tuple, Some(format_smolstr!("{e:?}"))))?;
            let res = tuple.convert(return_format)?;
            Ok(res)
        }
    }
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

fn empty_query_response() -> Result<Box<dyn Any>, SbroadError> {
    let res = ConsumerResult { row_count: 0 };
    match Tuple::new(&[res]) {
        Ok(t) => Ok(Box::new(t)),
        Err(e) => Err(SbroadError::FailedTo(
            Action::Create,
            Some(Entity::Tuple),
            format_smolstr!("{e}"),
        )),
    }
}

/// A helper function to dispatch the execution plan from the router to the storages.
///
/// # Errors
/// - Internal errors during the execution.
pub fn dispatch_impl(
    coordinator: &impl Router,
    plan: &mut ExecutionPlan,
    top_id: NodeId,
    buckets: &Buckets,
    return_format: DispatchReturnFormat,
) -> Result<Box<dyn Any>, SbroadError> {
    debug!(
        Option::from("dispatch"),
        &format!("dispatching plan: {plan:?}")
    );

    if plan.get_ir_plan().is_empty() {
        return empty_query_response();
    }

    let sub_plan = plan.take_subtree(top_id)?;

    let tier = {
        match sub_plan.get_ir_plan().tier.as_ref() {
            None => coordinator.get_current_tier_name()?,
            tier => tier.cloned(),
        }
    };
    let tier_runtime = coordinator.get_vshard_object_by_tier(tier.as_ref())?;

    debug!(Option::from("dispatch"), &format!("sub plan: {sub_plan:?}"));
    if has_zero_limit_clause(&sub_plan)? {
        return empty_query_result(&sub_plan, return_format.clone());
    }
    dispatch_by_buckets(sub_plan, buckets, &tier_runtime, return_format)
}

/// Helper function that chooses one of the methods for execution
/// based on buckets.
///
/// # Errors
/// - Failed to dispatch
pub fn dispatch_by_buckets(
    sub_plan: ExecutionPlan,
    buckets: &Buckets,
    runtime: &impl Vshard,
    return_format: DispatchReturnFormat,
) -> Result<Box<dyn Any>, SbroadError> {
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
            if let Some(vtables) = &sub_plan.vtables {
                for (motion_id, vtable) in vtables.map() {
                    if !vtable.get_bucket_index().is_empty() {
                        return Err(SbroadError::Invalid(
                            Entity::Motion,
                            Some(format_smolstr!("Motion ({motion_id:?}) in subtree with distribution Single, but policy is not Full.")),
                        ));
                    }
                }
            }
            runtime.exec_ir_on_any_node(sub_plan, return_format)
        }
        Buckets::All | Buckets::Filtered(_) => {
            runtime.exec_ir_on_buckets(sub_plan, buckets, return_format)
        }
    }
}

/// Helper function reused for Cartridge/Picodata `materialize_values` method of Router.
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

    // Flag indicating whether VALUES contains only constants.
    let mut only_constants = true;
    // Ids of constants that we have to replace with parameters.
    // We'll need to use it only in case
    let mut constants_to_erase: Vec<NodeId> = Vec::new();
    for row_id in &children {
        let row_node = exec_plan.get_ir_plan().get_relation_node(*row_id)?;
        let Relational::ValuesRow(ValuesRow { data, .. }) = row_node else {
            panic!("Expected ValuesRow under Values. Got {row_node:?}.")
        };
        let data_row_list: Vec<NodeId> = exec_plan.get_ir_plan().get_row_list(*data)?.to_vec();
        let mut row: VTableTuple = Vec::with_capacity(columns_len);
        for idx in 0..columns_len {
            let column_id = *data_row_list
                .get(idx)
                .unwrap_or_else(|| panic!("Column not found at position {idx} in the row."));
            let column_node = exec_plan.get_ir_plan().get_node(column_id)?;
            if let Node::Expression(Expression::Constant(Constant { value, .. })) = column_node {
                constants_to_erase.push(column_id);
                row.push(value.clone());
            } else {
                only_constants = false;
                break;
            }
        }

        if !only_constants {
            break;
        }
        vtable.add_tuple(row);
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
        // Otherwise `dispatch` call will replace nodes on Parameters.
        for column_id in constants_to_erase {
            let _ = exec_plan.get_mut_ir_plan().replace_with_stub(column_id);
        }

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
        let mut result = runtime
            .dispatch(
                exec_plan,
                values_id,
                &Buckets::Any,
                DispatchReturnFormat::Inner,
            )?
            .downcast::<ProducerResult>()
            .expect("must've failed earlier");
        result.as_virtual_table()?
    };

    let unified_types = calculate_vtable_unified_types(&vtable)?;
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
    {
        let ir = plan.get_ir_plan();
        assert!(
            !ir.get_relation_node(top_id)?.is_dml(),
            "materialize motion can be called only for DQL queries"
        );
    }

    // We should get a motion alias name before we take the subtree in `dispatch` method.
    let motion_node = plan.get_ir_plan().get_relation_node(motion_node_id)?;
    let alias = if let Relational::Motion(Motion { alias, .. }) = motion_node {
        alias.clone()
    } else {
        panic!("Expected motion node, got {motion_node:?}");
    };
    // Dispatch the motion subtree (it will be replaced with invalid values).
    let mut result = *runtime
        .dispatch(plan, top_id, buckets, DispatchReturnFormat::Inner)?
        .downcast::<ProducerResult>()
        .expect("must've failed earlier");
    // Unlink motion node's child sub tree (it is already replaced with invalid values).
    plan.unlink_motion_subtree(motion_node_id)?;
    let mut vtable = result.as_virtual_table()?;

    if let Some(name) = alias {
        vtable.set_alias(name.as_str());
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
    if let Some(vtables) = plan.get_mut_vtables() {
        for rc_vtable in vtables.values_mut() {
            // If the virtual table id hashed by the bucket_id, we can filter its tuples.
            // Otherwise (full motion policy) we need to preserve all tuples.
            if !rc_vtable.get_bucket_index().is_empty() {
                *rc_vtable = Rc::new(rc_vtable.new_with_buckets(bucket_ids)?);
            }
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

fn populate_table(
    node_id: NodeId,
    plan_id: &SmolStr,
    tables: &mut EncodedTables,
) -> Result<(), SbroadError> {
    let data = tables.get_mut(&node_id).ok_or_else(|| {
        SbroadError::NotFound(
            Entity::Table,
            format_smolstr!("encoded table with id {node_id}"),
        )
    })?;
    with_su(ADMIN_ID, || -> Result<(), SbroadError> {
        let name = table_name(plan_id, node_id);
        let space = Space::find(&name).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Space,
                format_smolstr!("temporary SQL table: {name}"),
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
                        format_smolstr!("tuple {tuple:?} into {name}: {e}"),
                    ));
                }
            }
        }
        Ok(())
    })??;
    Ok(())
}

fn truncate_tables(table_ids: &[NodeId], plan_id: &SmolStr) {
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
    fn vdbe_max_steps(&self) -> u64;
    fn vtable_max_rows(&self) -> u64;
    fn extract_data(&mut self) -> EncodedTables;
}

pub trait PlanInfo: RequiredPlanInfo {
    /// Extracts the query and the temporary tables from the plan.
    /// Temporary tables truncate their data in destructor and act
    /// as a guard.
    ///
    /// # Errors
    /// - Failed to extract query and table guard.
    fn extract_query_and_table_guard(
        &mut self,
    ) -> Result<(PatternWithParams, Vec<TableGuard>), SbroadError>;
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

    fn vdbe_max_steps(&self) -> u64 {
        self.required.options.vdbe_max_steps
    }

    fn vtable_max_rows(&self) -> u64 {
        self.required.options.vtable_max_rows
    }

    fn extract_data(&mut self) -> EncodedTables {
        std::mem::take(&mut self.required.tables)
    }
}

impl PlanInfo for QueryInfo<'_> {
    fn extract_query_and_table_guard(
        &mut self,
    ) -> Result<(PatternWithParams, Vec<TableGuard>), SbroadError> {
        compile_optional(self.optional, &self.required.plan_id)
    }
}

pub struct EncodedQueryInfo<'data> {
    optional_bytes: OptionalBytes,
    required: &'data mut RequiredData,
}

impl<'data> EncodedQueryInfo<'data> {
    pub fn new(raw_optional: OptionalBytes, required: &'data mut RequiredData) -> Self {
        Self {
            optional_bytes: raw_optional,
            required,
        }
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

    fn vdbe_max_steps(&self) -> u64 {
        self.required.options.vdbe_max_steps
    }

    fn vtable_max_rows(&self) -> u64 {
        self.required.options.vtable_max_rows
    }

    fn extract_data(&mut self) -> EncodedTables {
        std::mem::take(&mut self.required.tables)
    }
}

impl PlanInfo for EncodedQueryInfo<'_> {
    fn extract_query_and_table_guard(
        &mut self,
    ) -> Result<(PatternWithParams, Vec<TableGuard>), SbroadError> {
        let data = std::mem::take(self.optional_bytes.get_mut()?);
        let mut optional = OptionalData::try_from(EncodedOptionalData::from(data))?;
        compile_optional(&mut optional, &self.required.plan_id)
    }
}

/// If the statement with given `plan_id` is found in the cache,
/// execute it and return result. Otherwise, returns error.
///
/// This function works only for read statements, and it is a
/// responsibility of a caller to ensure it.
///
/// # Errors
/// - Failed to borrow the cache
/// - Failed to execute given statement.
///
/// # Panics
/// - Temporary table could not be truncated.
/// - Temporary table could not be inserted.
pub fn read_from_cache<R: QueryCache, M: MutexLike<R::Cache>>(
    locked_cache: &mut M::Guard<'_>,
    params: &[Value],
    tables: &mut EncodedTables,
    plan_id: &SmolStr,
    vdbe_max_steps: u64,
    max_rows: u64,
    return_format: &StorageReturnFormat,
) -> Result<Box<dyn Any>, SbroadError>
where
    R::Cache: StorageCache,
{
    // Look for the prepared statement in the cache.
    if let Some((stmt, table_ids)) = locked_cache.get(plan_id)? {
        // Transaction rollbacks are very expensive in Tarantool, so we're going to
        // avoid transactions for DQL queries. We can achieve atomicity by truncating
        // temporary tables. Isolation is guaranteed by keeping a lock on the cache.
        let result = 'dql: {
            for node_id in table_ids {
                if let Err(e) = populate_table(*node_id, plan_id, tables) {
                    break 'dql Err(e);
                }
            }
            execute_prepared(stmt, params, vdbe_max_steps, max_rows, return_format)
        };
        truncate_tables(table_ids, plan_id);

        return result;
    };
    Err(SbroadError::FailedTo(
        Action::Find,
        Some(Entity::Statement),
        format_smolstr!("{plan_id} is absent in the storage cache"),
    ))
}

/// Execute a read statement bypassing tarantool cache.
///
/// # Errors
/// - Failed to populate temporary tables.
/// - Failed to execute the statement.
/// - Temporary table could not be truncated.
fn read_bypassing_cache(
    pattern: &str,
    params: &[Value],
    plan_id: &SmolStr,
    tables: &mut EncodedTables,
    vdbe_max_steps: u64,
    max_rows: u64,
    return_format: &StorageReturnFormat,
) -> Result<Box<dyn Any>, SbroadError> {
    // Transaction rollbacks are very expensive in Tarantool, so we're going to
    // avoid transactions for DQL queries. We can achieve atomicity by truncating
    // temporary tables. Isolation is guaranteed by keeping a lock on the cache.
    let result = 'dql: {
        let table_ids = tables.keys().copied().collect::<Vec<NodeId>>();
        for node_id in table_ids {
            if let Err(e) = populate_table(node_id, plan_id, tables) {
                break 'dql Err(e);
            }
        }
        execute_unprepared(pattern, params, vdbe_max_steps, max_rows, return_format)
    };
    // No need to truncate temporary tables as they would be truncated in the parent function.
    result
}

/// # Errors
/// - Execution errors
pub fn prepare_and_read<R: QueryCache, M: MutexLike<R::Cache>>(
    locked_cache: &mut M::Guard<'_>,
    info: &mut impl PlanInfo,
    return_format: &StorageReturnFormat,
) -> Result<Box<dyn Any>, SbroadError>
where
    R::Cache: StorageCache,
{
    let mut tmp_tables = info.extract_data();
    let vdbe_max_steps = info.vdbe_max_steps();
    let max_rows = info.vtable_max_rows();

    // The statement was not found in the cache, so we need to prepare it.
    let (pattern_with_params, mut guards) = info.extract_query_and_table_guard()?;
    let plan_id = info.id();
    let (pattern, params) = pattern_with_params.into_parts();
    match prepare(pattern.clone()) {
        Ok(stmt) => {
            let table_ids: Vec<NodeId> = tmp_tables.keys().copied().collect();
            locked_cache.put(plan_id.clone(), stmt, info.schema_info(), table_ids)?;
            // It is safe to skip truncating temporary tables here as there are no early
            // returns after this point that could leave the tables populated with data.
            guards.iter_mut().for_each(TableGuard::skip_truncate);
            let res = cache_miss::<R, M>(
                locked_cache,
                &params,
                &mut tmp_tables,
                plan_id,
                vdbe_max_steps,
                max_rows,
                return_format,
            )?;
            return Ok(res);
        }
        #[allow(unused_variables)]
        Err(e) => {
            error!(None, &format!("failed to compile stmt: {e:?}"));
        }
    }

    // Possibly the statement is correct, but doesn't fit into
    // Tarantool's prepared statements cache (`sql_cache_size`).
    // So we try to execute it bypassing the cache.

    // We need the cache to be locked though we are not going to use it. If we don't lock it,
    // the prepared statement made from our pattern can be inserted into the cache by some
    // other fiber because we have removed some big statements with LRU and tarantool cache
    // has enough space to store this statement. And it can cause races in the temporary
    // tables.

    read_bypassing_cache(
        &pattern,
        &params,
        plan_id,
        &mut tmp_tables,
        vdbe_max_steps,
        max_rows,
        return_format,
    )
}

/// Execute a DQL statement or prepare it, put it into the cache and execute.
///
/// # Errors
/// - something wrong with the statement in the cache.
pub fn read_or_prepare<R: QueryCache, M: MutexLike<R::Cache>>(
    locked_cache: &mut M::Guard<'_>,
    info: &mut impl PlanInfo,
    return_format: &StorageReturnFormat,
) -> Result<Box<dyn Any>, SbroadError>
where
    R::Cache: StorageCache,
{
    // Look for the statement in the cache.
    if let Some(res) = read_if_in_cache::<R, M>(locked_cache, info, true, return_format)? {
        return Ok(res);
    }

    prepare_and_read::<R, M>(locked_cache, info, return_format)
}

/// Execute DML on the storage.
///
/// # Errors
/// - Failed to execute DML locally.
#[allow(clippy::too_many_lines)]
pub fn execute_dml_on_storage<R: Vshard + QueryCache>(
    runtime: &R,
    raw_optional: &mut Vec<u8>,
    required: &mut RequiredData,
) -> Result<ConsumerResult, SbroadError>
where
    R::Cache: StorageCache,
{
    let data = std::mem::take(raw_optional);
    let mut optional = OptionalData::try_from(EncodedOptionalData::from(data))?;
    optional.exec_plan.get_mut_ir_plan().restore_constants()?;
    let plan = optional.exec_plan.get_ir_plan();
    let top_id = plan.get_top()?;
    let top = plan.get_relation_node(top_id)?;
    match top {
        Relational::Insert(_) => execute_insert_on_storage(runtime, &mut optional, required),
        Relational::Delete(_) => execute_delete_on_storage(runtime, &mut optional, required),
        Relational::Update(_) => execute_update_on_storage(runtime, &mut optional, required),
        _ => Err(SbroadError::Invalid(
            Entity::Plan,
            Some(format_smolstr!(
                "expected DML node on the plan top, got {top:?}"
            )),
        )),
    }
}

/// Helper function to materialize vtable on storage (not on router).
fn materialize_vtable_locally<R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
    child_id: NodeId,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
{
    optional.exec_plan.get_mut_ir_plan().restore_constants()?;
    let mut info = QueryInfo::new(optional, required);
    let mut locked_cache = runtime.cache().lock();
    let result =
        read_or_prepare::<R, R::Mutex>(&mut locked_cache, &mut info, &StorageReturnFormat::DqlRaw)?;
    let bytes = result.downcast::<Vec<u8>>().map_err(|e| {
        SbroadError::FailedTo(
            Action::Deserialize,
            Some(Entity::Tuple),
            format_smolstr!("motion node {child_id:?}. {e:?}"),
        )
    })?;
    let mut reader = bytes.as_ref().as_slice();
    let mut data: Vec<ProducerResult> = rmp_serde::decode::from_read(&mut reader).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::Tuple),
            format_smolstr!("motion node {child_id:?}. {e}"),
        )
    })?;
    let vtable = data
        .get_mut(0)
        .ok_or_else(|| SbroadError::NotFound(Entity::ProducerResult, "from the tuple".into()))?
        // It is a DML query, so we don't need to care about the column types
        // in response. So, simply use scalar type for all the columns.
        .as_virtual_table()?;
    optional
        .exec_plan
        .set_motion_vtable(&child_id, vtable, runtime)?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn execute_update_on_storage<R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
) -> Result<ConsumerResult, SbroadError>
where
    R::Cache: StorageCache,
{
    let plan = optional.exec_plan.get_ir_plan();
    let update_id = plan.get_top()?;
    let update_child_id = plan.dml_child_id(update_id)?;
    let space_name = plan.dml_node_table(update_id)?.name().clone();
    let mut result = ConsumerResult::default();
    let is_sharded = plan.is_sharded_update(update_id)?;
    let build_vtable_locally = !optional
        .exec_plan
        .contains_vtable_for_motion(update_child_id);
    if build_vtable_locally {
        // it is relevant only for local Update.
        if is_sharded {
            return Err(SbroadError::Invalid(
                Entity::Update,
                Some("sharded Update's vtable must be already materialized".into()),
            ));
        }
        materialize_vtable_locally(runtime, optional, required, update_child_id)?;
    }
    let vtable = optional.exec_plan.get_motion_vtable(update_child_id)?;
    let space = Space::find(&space_name).ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Space,
            Some(format_smolstr!("space {space_name} not found")),
        )
    })?;
    transaction(|| -> Result<(), SbroadError> {
        let plan = optional.exec_plan.get_ir_plan();
        if is_sharded {
            let delete_tuple_len = plan.get_update_delete_tuple_len(update_id)?;
            let builder = init_sharded_update_tuple_builder(plan, &vtable, update_id)?;
            execute_sharded_update(&mut result, &vtable, &space, &builder, delete_tuple_len)?;
        } else {
            let builder = init_local_update_tuple_builder(plan, &vtable, update_id)?;
            execute_local_update(&mut result, &builder, &vtable, &space)?;
        }
        Ok(())
    })?;

    Ok(result)
}

/// A working horse for `execute_update_on_storage` in case we're dealing with
/// sharded update.
fn execute_sharded_update(
    result: &mut ConsumerResult,
    vtable: &VirtualTable,
    space: &Space,
    builder: &TupleBuilderPattern,
    delete_tuple_len: usize,
) -> Result<(), SbroadError> {
    for tuple in vtable.get_tuples() {
        if tuple.len() == delete_tuple_len {
            let pk: Vec<EncodedValue> = tuple
                .iter()
                .map(|val| EncodedValue::Ref(MsgPackValue::from(val)))
                .collect();
            if let Err(Error::Tarantool(tnt_err)) = space.delete(&pk) {
                return Err(SbroadError::FailedTo(
                    Action::Delete,
                    Some(Entity::Tuple),
                    format_smolstr!("{tnt_err:?}"),
                ));
            }
        }
    }
    for (bucket_id, positions) in vtable.get_bucket_index() {
        for pos in positions {
            let vt_tuple = vtable.get_tuples().get(*pos).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::VirtualTable,
                    Some(format_smolstr!("invalid tuple position in index: {pos}")),
                )
            })?;

            if vt_tuple.len() != delete_tuple_len {
                let mut insert_tuple: Vec<EncodedValue> = Vec::with_capacity(builder.len());
                for command in builder {
                    match command {
                        TupleBuilderCommand::TakePosition(tuple_pos) => {
                            let value = vt_tuple.get(*tuple_pos).ok_or_else(|| {
                                SbroadError::Invalid(
                                    Entity::Tuple,
                                    Some(format_smolstr!(
                                        "column at position {pos} not found in virtual table"
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
                                        "column at position {pos} not found in virtual table"
                                    )),
                                )
                            })?;
                            insert_tuple.push(value.cast(table_type)?);
                        }
                        TupleBuilderCommand::CalculateBucketId(_) => {
                            insert_tuple.push(EncodedValue::Ref(MsgPackValue::Unsigned(bucket_id)));
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::TupleBuilderCommand,
                                Some(format_smolstr!("got command {command:?} for update insert")),
                            ));
                        }
                    }
                }
                // We can have multiple rows with the same primary key,
                // so replace is used.
                if let Err(e) = space.replace(&insert_tuple) {
                    return Err(SbroadError::FailedTo(
                        Action::Insert,
                        Some(Entity::Tuple),
                        format_smolstr!("{e:?}"),
                    ));
                }
                result.row_count += 1;
            }
        }
    }

    Ok(())
}

pub struct UpdateArgs<'vtable_tuple> {
    pub key_tuple: Vec<EncodedValue<'vtable_tuple>>,
    pub ops: Vec<[EncodedValue<'vtable_tuple>; 3]>,
}

pub fn eq_op() -> &'static Value {
    // Once lock is used because of concurrent access in tests.
    static mut EQ: OnceLock<Value> = OnceLock::new();

    unsafe { EQ.get_or_init(|| Value::String("=".into())) }
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
                    EncodedValue::Owned(LuaValue::Unsigned(*table_col as u64)),
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
                key_tuple.push(value.cast(table_type)?);
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
                    EncodedValue::Owned(LuaValue::Unsigned(*table_col as u64)),
                    value.cast(table_type)?,
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

/// A working horse for `execute_update_on_storage` in case we're dealing with
/// nonsharded update.
fn execute_local_update(
    result: &mut ConsumerResult,
    builder: &TupleBuilderPattern,
    vtable: &VirtualTable,
    space: &Space,
) -> Result<(), SbroadError> {
    for vt_tuple in vtable.get_tuples() {
        let args = build_update_args(vt_tuple, builder)?;
        let update_res = space.update(&args.key_tuple, &args.ops);
        update_res.map_err(|e| {
            SbroadError::FailedTo(Action::Update, Some(Entity::Space), format_smolstr!("{e}"))
        })?;
        result.row_count += 1;
    }
    Ok(())
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

#[allow(clippy::too_many_lines)]
fn execute_delete_on_storage<R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
) -> Result<ConsumerResult, SbroadError>
where
    R::Cache: StorageCache,
{
    let plan = optional.exec_plan.get_ir_plan();
    let delete_id = plan.get_top()?;
    let delete_childen = plan.children(delete_id);

    if delete_childen.is_empty() {
        // We have a deal with a DELETE without WHERE filter
        // and want to execute local SQL instead of space api.

        let mut info = QueryInfo::new(optional, required);
        let mut locked_cache = runtime.cache().lock();
        let res = read_or_prepare::<R, R::Mutex>(
            &mut locked_cache,
            &mut info,
            &StorageReturnFormat::DmlRaw,
        )?;
        let bytes = res.downcast::<Vec<u8>>().map_err(|e| {
            SbroadError::Invalid(
                Entity::Tuple,
                Some(format_smolstr!("Unable to downcast DML result vec: {e:?}")),
            )
        })?;
        let mut reader = bytes.as_ref().as_slice();
        let mut data: Vec<ConsumerResult> =
            rmp_serde::decode::from_read(&mut reader).map_err(|e| {
                SbroadError::Invalid(
                    Entity::Tuple,
                    Some(format_smolstr!("Unable to decode DML ConsumerResult: {e}")),
                )
            })?;
        let res = data.remove(0);
        return Ok(res);
    }

    let delete_child_id = delete_childen[0];
    let builder = init_delete_tuple_builder(plan, delete_id)?;
    let space_name = plan.dml_node_table(delete_id)?.name().clone();
    let mut result = ConsumerResult::default();
    let build_vtable_locally = !optional
        .exec_plan
        .contains_vtable_for_motion(delete_child_id);
    if build_vtable_locally {
        materialize_vtable_locally(runtime, optional, required, delete_child_id)?;
    }
    let vtable = optional.exec_plan.get_motion_vtable(delete_child_id)?;
    let space = Space::find(&space_name).ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Space,
            Some(format_smolstr!("space {space_name} not found")),
        )
    })?;
    transaction(|| -> Result<(), SbroadError> {
        for vt_tuple in vtable.get_tuples() {
            let delete_tuple = build_delete_args(vt_tuple, &builder)?;
            if let Err(Error::Tarantool(tnt_err)) = space.delete(&delete_tuple) {
                return Err(SbroadError::FailedTo(
                    Action::Delete,
                    Some(Entity::Tuple),
                    format_smolstr!("{tnt_err:?}"),
                ));
            }
            result.row_count += 1;
        }
        Ok(())
    })?;

    Ok(result)
}

#[allow(clippy::too_many_lines)]
fn execute_insert_on_storage<R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
) -> Result<ConsumerResult, SbroadError>
where
    R::Cache: StorageCache,
{
    // We always generate a virtual table under the `INSERT` node
    // of the execution plan and prefer to execute it via space API
    // instead of SQL (for performance reasons).
    let plan = optional.exec_plan.get_ir_plan();
    let insert_id = plan.get_top()?;
    let insert_child_id = plan.dml_child_id(insert_id)?;
    let space_name = plan.dml_node_table(insert_id)?.name().clone();
    let mut result = ConsumerResult::default();

    // There are two ways to execute an `INSERT` query:
    // 1. Execute SQL subtree under the `INSERT` node (`INSERT .. SELECT ..`)
    //    and then repack and insert results into the space.
    // 2. A virtual table was dispatched under the `INSERT` node.
    //    Simply insert its tuples into the space.
    // The same for `UPDATE`.

    // Check is we need to execute an SQL subtree (case 1).
    let build_vtable_locally = !optional
        .exec_plan
        .contains_vtable_for_motion(insert_child_id);
    if build_vtable_locally {
        materialize_vtable_locally(runtime, optional, required, insert_child_id)?;
    }

    // Check if the virtual table have been dispatched (case 2) or built locally (case 1).
    let vtable = optional.exec_plan.get_motion_vtable(insert_child_id)?;
    let space = Space::find(&space_name).ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Space,
            Some(format_smolstr!("space {space_name} not found")),
        )
    })?;
    let plan = optional.exec_plan.get_ir_plan();
    // let builder = add_casts_to_builder(builder, &tuple_pos_to_type, vtable.as_ref())?;
    let builder = init_insert_tuple_builder(plan, vtable.as_ref(), insert_id)?;
    let conflict_strategy = optional
        .exec_plan
        .get_ir_plan()
        .insert_conflict_strategy(insert_id)?;
    transaction(|| -> Result<(), SbroadError> {
        for (bucket_id, positions) in vtable.get_bucket_index() {
            for pos in positions {
                let vt_tuple = vtable.get_tuples().get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::VirtualTable,
                        Some(format_smolstr!(
                            "tuple at position {pos} not found in virtual table"
                        )),
                    )
                })?;
                let insert_tuple = build_insert_args(vt_tuple, &builder, Some(bucket_id))?;
                let insert_result = space.insert(&insert_tuple);
                if let Err(Error::Tarantool(tnt_err)) = &insert_result {
                    if tnt_err.error_code() == TarantoolErrorCode::TupleFound as u32 {
                        match conflict_strategy {
                            ConflictStrategy::DoNothing => {
                                debug!(
                                    Option::from("execute_dml"),
                                    &format!(
                                        "{} {:?}. {}",
                                        "failed to insert tuple:",
                                        insert_tuple,
                                        "Skipping according to conflict strategy"
                                    )
                                );
                            }
                            ConflictStrategy::DoReplace => {
                                debug!(
                                    Option::from("execute_dml"),
                                    &format!(
                                        "{} {:?}. {}",
                                        "failed to insert tuple:",
                                        insert_tuple,
                                        "Trying to replace according to conflict strategy"
                                    )
                                );
                                space.replace(&insert_tuple).map_err(|e| {
                                    SbroadError::FailedTo(
                                        Action::ReplaceOnConflict,
                                        Some(Entity::Space),
                                        format_smolstr!("{e}"),
                                    )
                                })?;
                                result.row_count += 1;
                            }
                            ConflictStrategy::DoFail => {
                                return Err(SbroadError::FailedTo(
                                    Action::Insert,
                                    Some(Entity::Space),
                                    format_smolstr!("{tnt_err}"),
                                ));
                            }
                        }
                        // if either DoReplace or DoNothing was done,
                        // jump to next tuple iteration. Otherwise
                        // the error is not DuplicateKey, and we
                        // should throw it back to user.
                        continue;
                    };
                }
                insert_result.map_err(|e| {
                    SbroadError::FailedTo(
                        Action::Insert,
                        Some(Entity::Space),
                        format_smolstr!("{e}"),
                    )
                })?;
                result.row_count += 1;
            }
        }
        Ok(())
    })?;

    Ok(result)
}

/// Execute DML query locally
///
/// # Errors
/// - Failed to execute DML locally.
#[allow(unused_variables)]
pub fn execute_dml<R: Vshard + QueryCache>(
    runtime: &R,
    required: &mut RequiredData,
    raw_optional: &mut Vec<u8>,
) -> Result<Box<dyn Any>, SbroadError>
where
    R::Cache: StorageCache,
{
    if required.query_type != QueryType::DML {
        return Err(SbroadError::Invalid(
            Entity::Plan,
            Some("Expected a DML plan.".to_smolstr()),
        ));
    }

    let result = execute_dml_on_storage(runtime, raw_optional, required)?;
    let tuple = Tuple::new(&[result])
        .map_err(|e| SbroadError::Invalid(Entity::Tuple, Some(format_smolstr!("{e:?}"))))?;
    Ok(Box::new(tuple) as Box<dyn Any>)
}

/// # Errors
/// - Execution errors
pub fn execute_first_cacheable_request<R: Vshard + QueryCache>(
    runtime: &R,
    info: &mut impl RequiredPlanInfo,
) -> Result<Box<dyn Any>, SbroadError>
where
    R::Cache: StorageCache,
{
    let mut locked_cache = runtime.cache().lock();
    if let Some(res) = read_if_in_cache::<R, R::Mutex>(
        &mut locked_cache,
        info,
        true,
        &StorageReturnFormat::DqlVshard,
    )? {
        return Ok(res);
    }

    Ok(Box::new(dql_cache_miss_result()))
}

/// # Errors
/// - Execution errors
pub fn read_if_in_cache<R: QueryCache, M: MutexLike<R::Cache>>(
    locked_cache: &mut M::Guard<'_>,
    info: &mut impl RequiredPlanInfo,
    first_try: bool,
    return_format: &StorageReturnFormat,
) -> Result<Option<Box<dyn Any>>, SbroadError>
where
    R::Cache: StorageCache,
{
    let is_cached = locked_cache.get(info.id())?.is_some();
    if is_cached {
        let mut tmp_tables = info.extract_data();
        let res = if first_try {
            cache_hit::<R, M>(
                locked_cache,
                info.params(),
                &mut tmp_tables,
                info.id(),
                info.vdbe_max_steps(),
                info.vtable_max_rows(),
                return_format,
            )
        } else {
            cache_miss::<R, M>(
                locked_cache,
                info.params(),
                &mut tmp_tables,
                info.id(),
                info.vdbe_max_steps(),
                info.vtable_max_rows(),
                return_format,
            )
        };
        return res.map(Some);
    }
    Ok(None)
}

/// # Errors
/// - Execution errors
pub fn execute_second_cacheable_request<R: Vshard + QueryCache>(
    runtime: &R,
    info: &mut impl PlanInfo,
) -> Result<Box<dyn Any>, SbroadError>
where
    R::Cache: StorageCache,
{
    let mut locked_cache = runtime.cache().lock();
    if let Some(res) = read_if_in_cache::<R, R::Mutex>(
        &mut locked_cache,
        info,
        false,
        &StorageReturnFormat::DqlVshard,
    )? {
        return Ok(res);
    }

    read_or_prepare::<R, R::Mutex>(&mut locked_cache, info, &StorageReturnFormat::DqlVshard)
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
        .iter()
        .map(|(k, _)| (k.to_smolstr(), k.as_str()))
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

fn parse_rows(tuple: &Tuple) -> Result<&[u8], SbroadError> {
    fn skip_string(stream: &mut impl RmpRead) -> Result<(), SbroadError> {
        let colname_len = rmp::decode::read_str_len(stream).map_err(|e| {
            SbroadError::FailedTo(
                Action::Decode,
                Some(Entity::MsgPack),
                format_smolstr!("read str len: {e:?}"),
            )
        })?;
        for _ in 0..colname_len {
            stream.read_u8().map_err(|e| {
                SbroadError::FailedTo(
                    Action::Decode,
                    Some(Entity::MsgPack),
                    format_smolstr!("read u8: {e:?}"),
                )
            })?;
        }
        Ok(())
    }

    fn expect_map_len(stream: &mut impl RmpRead, expected_len: u32) -> Result<(), SbroadError> {
        let actual_len = rmp::decode::read_map_len(stream).map_err(|e| {
            SbroadError::FailedTo(
                Action::Decode,
                Some(Entity::MsgPack),
                format_smolstr!("read map len: {e:?}"),
            )
        })?;
        if actual_len != expected_len {
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "expected map len {expected_len} doesn't match the actual len {actual_len}"
            )));
        }
        Ok(())
    }

    let bytes: &RawBytes = tuple.get(0).ok_or(SbroadError::FailedTo(
        Action::Get,
        Some(Entity::Bytes),
        format_smolstr!("raw bytes from tuple"),
    ))?;
    let mut stream = rmp::decode::Bytes::from(bytes.as_ref());

    // parse { 'metadata': ..., 'rows': ... }
    expect_map_len(&mut stream, 2)?;

    // skip 'metadata'
    skip_string(&mut stream)?;

    // skip metadata (column names and types)
    let metadata_len = rmp::decode::read_array_len(&mut stream).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("read array len: {e:?}"),
        )
    })?;
    for _ in 0..metadata_len {
        expect_map_len(&mut stream, 2)?;

        // skip 'name'
        skip_string(&mut stream)?;

        // skip column name
        skip_string(&mut stream)?;

        // skip 'type'
        skip_string(&mut stream)?;

        // skip column type
        skip_string(&mut stream)?;
    }

    // { 'rows': ... }
    Ok(stream.remaining_slice())
}

fn encode_result_tuple(metadata: &[MetadataColumn], rows: &[u8]) -> Result<Tuple, SbroadError> {
    fn count_mp_bytes(v: impl Serialize) -> usize {
        let mut byte_counter: ByteCounter = ByteCounter::default();
        let mut ser = rmp_serde::Serializer::new(&mut byte_counter);
        v.serialize(&mut ser).expect("counting failed");
        byte_counter.bytes()
    }

    fn calculate_result_tuple_size(metadata: &[MetadataColumn], rows: &[u8]) -> usize {
        let array_header_size = 1 + 4; // array marker + array len
        let map_header_size = 1 + 4; // map marker + map len
        let metadata_str_size = 1 + 4 + 8; // str marker + str len + 'metadata'
        let metadata_size = count_mp_bytes(metadata);
        array_header_size + map_header_size + metadata_str_size + metadata_size + rows.len()
    }

    let res_size = calculate_result_tuple_size(metadata, rows);
    let mut stream = rmp::encode::ByteBuf::with_capacity(res_size);

    // result is an array
    rmp::encode::write_array_len(&mut stream, 1).map_err(|e| {
        SbroadError::FailedTo(
            Action::Encode,
            Some(Entity::MsgPack),
            format_smolstr!("array len: {e:?}"),
        )
    })?;

    // encode map with metadata and rows
    rmp::encode::write_map_len(&mut stream, 2).map_err(|e| {
        SbroadError::FailedTo(
            Action::Encode,
            Some(Entity::MsgPack),
            format_smolstr!("map len: {e:?}"),
        )
    })?;
    rmp::encode::write_str(&mut stream, "metadata").map_err(|e| {
        SbroadError::FailedTo(
            Action::Encode,
            Some(Entity::MsgPack),
            format_smolstr!("string 'metadata': {e:?}"),
        )
    })?;
    rmp_serde::encode::write_named(stream.as_mut_vec(), metadata)
        .map_err(|_| SbroadError::FailedTo(Action::Serialize, Some(Entity::Metadata), "".into()))?;
    stream.as_mut_vec().extend_from_slice(rows);

    let tuple_buf = TupleBuffer::try_from_vec(stream.into_vec())?;
    Ok(Tuple::new(&tuple_buf)?)
}

fn cache_hit<R: QueryCache, M: MutexLike<R::Cache>>(
    locked_cache: &mut M::Guard<'_>,
    params: &[Value],
    tables: &mut EncodedTables,
    plan_id: &SmolStr,
    vdbe_max_steps: u64,
    max_rows: u64,
    return_format: &StorageReturnFormat,
) -> Result<Box<dyn Any>, SbroadError>
where
    R::Cache: StorageCache,
{
    read_from_cache::<R, M>(
        locked_cache,
        params,
        tables,
        plan_id,
        vdbe_max_steps,
        max_rows,
        return_format,
    )
}

fn cache_miss<R: QueryCache, M: MutexLike<R::Cache>>(
    locked_cache: &mut M::Guard<'_>,
    params: &[Value],
    tables: &mut EncodedTables,
    plan_id: &SmolStr,
    vdbe_max_steps: u64,
    max_rows: u64,
    return_format: &StorageReturnFormat,
) -> Result<Box<dyn Any>, SbroadError>
where
    R::Cache: StorageCache,
{
    read_from_cache::<R, M>(
        locked_cache,
        params,
        tables,
        plan_id,
        vdbe_max_steps,
        max_rows,
        return_format,
    )
}

/// Replace metadata in a tuple representing dql result ({rows: ..., metadata: ...}).
///
/// # Errors
/// - Msgpack parsing erros.
/// - Violated format of dql result.
pub fn replace_metadata_in_dql_result(
    tuple: &Tuple,
    metadata: &[MetadataColumn],
) -> Result<Tuple, SbroadError> {
    let rows = parse_rows(tuple)?;
    encode_result_tuple(metadata, rows)
}
