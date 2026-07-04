use crate::{
    backend::sql::{
        ir::block_pattern_key,
        tree::{OrderedSyntaxNodes, SyntaxPlan},
    },
    executor::{
        lru::{Cache, EvictFn, LRUCache, DEFAULT_CAPACITY},
        preemption::Scheduler,
    },
    ir::{
        api::children::Children,
        node::{
            block::BlockOwned,
            expression::{Expression, MutExpression},
            BlockEntries, BlockStatement, Cast,
        },
        tree::Snapshot,
    },
};
use ahash::AHashMap;
use std::cell::RefCell;

use crate::{
    executor::vtable::vtable_indexed_column_name,
    ir::{
        node::{
            relational::Relational, Alias, Constant, Delete, Insert, Limit, Motion, NodeId, Update,
            Values, ValuesRow,
        },
        operator::{
            BlockConflictDoUpdate, ConflictDoUpdate, ConflictStrategy, ConflictUpdateItem,
            ConflictUpdateRhs, ConflictUpdateValue, PreparedConflictUpdateItem,
        },
        types::{DerivedType, UnrestrictedType},
    },
};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::{cmp::Ordering, collections::HashMap, rc::Rc, sync::OnceLock};

use super::{BlockExecData, BlockQuery, BlockRuntimeHook, Metadata, Router, Vshard};
use crate::executor::Port;
use crate::ir::node::Node;
use crate::ir::value::{EncodedValue, MsgPackValue};
use crate::{
    errors::{Action, Entity, SbroadError},
    executor::{
        ir::ExecutionPlan,
        result::MetadataColumn,
        vtable::{calculate_unified_types, VTableTuple, VirtualTable},
    },
    ir::{
        bucket::Buckets,
        relation::{Column, ColumnRole},
        transformation::redistribution::MotionKey,
        value::Value,
        Plan,
    },
};
use rmp::encode::{write_array_len, write_map_len, write_pfix, write_str};
use std::io::Write;
use tarantool::msgpack;
use tarantool::msgpack::rmp;
use tarantool::msgpack::{decode_from_read, Context, Decode, DecodeError};
use tarantool::msgpack::{Encode, EncodeError};

pub mod vshard;

pub use crate::utils::{normalize_name_from_sql, to_user};

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

#[must_use]
pub fn table_name(plan_id: u64, node_id: NodeId) -> SmolStr {
    format_smolstr!("_tmp_{plan_id}_{node_id}")
}

/// Generate a primary key name for the specified motion node.
#[must_use]
pub fn pk_name(plan_id: &str, node_id: NodeId) -> SmolStr {
    let base = xx_hash(plan_id);
    format_smolstr!("PK_{base}_{node_id}")
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

/// Check if the plan has a LIMIT 0 clause.
/// For instance, it returns true for a query `SELECT * FROM T LIMIT 0`
/// and false for `SELECT * FROM T LIMIT 1`.
///
/// # Errors
/// - Invalid plan.
fn has_zero_limit_clause(plan: &ExecutionPlan, top_id: NodeId) -> Result<bool, SbroadError> {
    let ir = plan.get_ir_plan();
    if let Relational::Limit(Limit { limit, .. }) = ir.get_relation_node(top_id)? {
        return Ok(*limit == 0);
    }
    Ok(false)
}

/// Return motion child of DML node if exists
///
/// # Errors
/// - node is not a relational type
fn dml_get_motion_child(
    ex_plan: &ExecutionPlan,
    dml_node_id: NodeId,
) -> Result<Option<NodeId>, SbroadError> {
    let ir_plan = ex_plan.get_ir_plan();
    debug_assert!(matches!(
        ir_plan.get_relation_node(dml_node_id)?,
        Relational::Delete(_) | Relational::Update(_) | Relational::Insert(_)
    ));
    match ir_plan.children(dml_node_id) {
        Children::None => Ok(None),
        Children::Single(child_id) => {
            let child = ir_plan.get_relation_node(*child_id)?;
            if let Relational::Motion(_) = child {
                Ok(ex_plan.effective_motion_subtree_root(*child_id)?)
            } else {
                Ok(None)
            }
        }
        _ => unreachable!("DML node can have no more than one child"),
    }
}

type BlockPatternCache = LRUCache<u64, Rc<Vec<BlockStatement<BlockQuery>>>>;

#[derive(Clone, Copy)]
pub struct BlockPatternCacheHooks {
    pub on_hit: fn(),
    pub on_miss: fn(),
    pub on_added: fn(),
    pub on_evicted: fn(),
}

static BLOCK_PATTERN_CACHE_HOOKS: OnceLock<BlockPatternCacheHooks> = OnceLock::new();

pub fn set_block_pattern_cache_hooks(hooks: BlockPatternCacheHooks) {
    let _ = BLOCK_PATTERN_CACHE_HOOKS.set(hooks);
}

#[inline(always)]
fn fire_block_pattern_cache_hook(pick: fn(&BlockPatternCacheHooks) -> fn()) {
    if let Some(hooks) = BLOCK_PATTERN_CACHE_HOOKS.get() {
        pick(hooks)();
    }
}

fn block_pattern_evict_fn() -> EvictFn<u64, Rc<Vec<BlockStatement<BlockQuery>>>> {
    Box::new(|_key, _value| {
        fire_block_pattern_cache_hook(|h| h.on_evicted);
        Ok(())
    })
}

thread_local! {
    static BLOCK_PATTERN_CACHE: RefCell<BlockPatternCache> = RefCell::new(
        LRUCache::new(DEFAULT_CAPACITY, Some(block_pattern_evict_fn()))
            .expect("non-zero capacity"),
    );
}

/// Compute the params of a single block query without rendering its SQL.
fn generate_params_for_block_query(
    plan: &ExecutionPlan,
    query_id: NodeId,
    bucket_id: Option<u64>,
) -> Result<Vec<Value>, SbroadError> {
    let sql_params = plan.block_sql_params(query_id)?;
    let (_, mut params) = sql_params.into_parts();
    if let Relational::Insert(insert) = plan.get_ir_plan().get_relation_node(query_id)? {
        let bucket_id = bucket_id.ok_or_else(|| {
            SbroadError::Other("INSERT in transaction requires a known target bucket".into())
        })?;
        push_block_param(&mut params, Value::Integer(bucket_id as i64))?;
        append_iocdu_param_values(plan, insert, &mut params, true)?;
    }
    Ok(params)
}

fn next_block_param_index(params_len: usize) -> Result<u16, SbroadError> {
    let index = params_len + 1;
    let index = u16::try_from(index).map_err(|_| {
        SbroadError::Other(format_smolstr!(
            "too many parameters in block query: {index}"
        ))
    })?;
    Ok(index)
}

fn push_block_param(params: &mut Vec<Value>, value: Value) -> Result<u16, SbroadError> {
    let index = next_block_param_index(params.len())?;
    params.push(value);
    Ok(index)
}

pub fn iocdu_param_value_node(plan: &Plan, expr: NodeId) -> Result<NodeId, SbroadError> {
    if matches!(plan.get_expression_node(expr)?, Expression::Constant(_)) {
        Ok(expr)
    } else {
        Err(SbroadError::Invalid(
            Entity::Plan,
            Some("unbound IOCDU parameter".into()),
        ))
    }
}

fn append_iocdu_param_values(
    plan: &ExecutionPlan,
    insert: &Insert,
    params: &mut Vec<Value>,
    bind_values: bool,
) -> Result<AHashMap<NodeId, u16>, SbroadError> {
    let mut parameter_indexes = AHashMap::new();
    let mut next_param_len = params.len();
    let ConflictStrategy::DoUpdate { payload } = &insert.conflict_strategy else {
        return Ok(parameter_indexes);
    };

    for item in &payload.items {
        let Some((expr, _)) = item.rhs.param() else {
            continue;
        };
        let param_node = iocdu_param_value_node(plan.get_ir_plan(), expr)?;
        // Build indexes for the block-local parameter list, not for the original SQL
        // placeholders. Key by the RHS node: each `$N` occurrence gets its own NodeId
        // when parsed, and parameter binding rewrites that node in place.
        let index = if bind_values {
            let value = plan.get_ir_plan().as_const_value_ref(param_node)?.clone();
            push_block_param(params, value)?
        } else {
            let index = next_block_param_index(next_param_len)?;
            next_param_len += 1;
            index
        };
        parameter_indexes.insert(param_node, index);
    }

    Ok(parameter_indexes)
}

fn prepare_iocdu_value(
    plan: &Plan,
    item: &ConflictUpdateItem,
    parameter_indexes: &AHashMap<NodeId, u16>,
) -> Result<ConflictUpdateValue, SbroadError> {
    if let Some((expr, target_type)) = item.rhs.param() {
        let param_node = iocdu_param_value_node(plan, expr)?;
        let index = parameter_indexes.get(&param_node).ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Plan,
                Some(format_smolstr!(
                    "missing ON CONFLICT DO UPDATE parameter {:?}",
                    param_node
                )),
            )
        })?;
        return Ok(ConflictUpdateValue::Param {
            index: *index,
            cast_type: target_type,
        });
    }

    let rhs = item.rhs.expr();
    // Cast(Constant) must have been folded by Plan::cast_constants().
    // Cast(LetVarRef) stays dynamic and is applied after reading the LET value.
    let (rhs, cast_type) = match plan.get_expression_node(rhs)? {
        Expression::Constant(Constant { value }) => {
            return Ok(ConflictUpdateValue::Const(value.clone()));
        }
        Expression::Cast(Cast { child, to }) => (*child, (*to).into()),
        _ => (rhs, UnrestrictedType::Any),
    };

    match plan.get_expression_node(rhs)? {
        Expression::LetVarRef(var) => Ok(ConflictUpdateValue::LetVar {
            var: var.clone(),
            cast_type,
        }),
        other => Err(SbroadError::Invalid(
            Entity::Plan,
            Some(format_smolstr!(
                "unsupported ON CONFLICT DO UPDATE value expression: {other:?}"
            )),
        )),
    }
}

fn prepare_iocdu_hook(
    plan: &ExecutionPlan,
    relation: &crate::ir::relation::Table,
    payload: &ConflictDoUpdate,
    parameter_indexes: &AHashMap<NodeId, u16>,
) -> Result<BlockRuntimeHook, SbroadError> {
    let items = payload
        .items
        .iter()
        .map(|item| {
            Ok(PreparedConflictUpdateItem {
                column: item.column,
                op: item.op,
                value: prepare_iocdu_value(plan.get_ir_plan(), item, parameter_indexes)?,
            })
        })
        .collect::<Result<Vec<_>, SbroadError>>()?;
    let update = BlockConflictDoUpdate {
        target: payload.target.clone(),
        items: items.into(),
    };
    // Rendered only for RAW EXPLAIN — normal executions never read the detail,
    // so don't pay for building, shipping and hashing it on the hot path.
    let raw_explain_detail = if plan.get_ir_plan().is_raw_explain() {
        Some(explain_iocdu_hook(relation, payload, &update)?)
    } else {
        None
    };
    Ok(BlockRuntimeHook::IdxInsertOnConflictDoUpdate {
        table_id: relation.id,
        update,
        raw_explain_detail,
    })
}

fn explain_iocdu_value(value: &ConflictUpdateValue, item: &ConflictUpdateItem) -> String {
    match value {
        ConflictUpdateValue::Const(value) => value.to_string(),
        ConflictUpdateValue::Param { index, .. } => {
            let source_index = match item.rhs {
                ConflictUpdateRhs::Param { source_index, .. } => source_index,
                ConflictUpdateRhs::Expr(_) => *index,
            };
            format!("${source_index}")
        }
        ConflictUpdateValue::LetVar { var, .. } => var.name.to_string(),
    }
}

fn explain_column_name(
    relation: &crate::ir::relation::Table,
    position: usize,
) -> Result<SmolStr, SbroadError> {
    relation
        .columns
        .get(position)
        .map(|column| to_user(&column.name))
        .ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Column,
                format_smolstr!("at position {position} in {}", relation.name),
            )
        })
}

fn explain_iocdu_hook(
    relation: &crate::ir::relation::Table,
    payload: &ConflictDoUpdate,
    update: &BlockConflictDoUpdate,
) -> Result<SmolStr, SbroadError> {
    let target = update
        .target
        .columns()
        .iter()
        .map(|pos| explain_column_name(relation, *pos))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");

    let items = update
        .items
        .iter()
        .zip(&payload.items)
        .map(|(item, source_item)| {
            let column = explain_column_name(relation, item.column)?;
            let value = explain_iocdu_value(&item.value, source_item);
            Ok::<_, SbroadError>(format!("{column} {} {value}", item.op))
        })
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");

    Ok(format_smolstr!(
        "picodata: ON CONFLICT ({target}) UPDATE {items}"
    ))
}

/// Generate pattern with params for block query.
/// Note that this function can generate UPDATE queries,
/// not sure if we should support it elsewhere.
pub fn generate_pattern_with_params_for_block(
    plan: &ExecutionPlan,
    query_id: NodeId,
    bucket_id: Option<u64>,
    use_colon_params: bool,
) -> Result<(BlockQuery, Vec<Value>), SbroadError> {
    use crate::backend::sql::tree::SyntaxData;

    struct Select {
        projection: Vec<Vec<SyntaxData>>,
        filter: Vec<SyntaxData>,
        indexed_by: Option<SyntaxData>,
    }

    /// Extract projection, filter and indexed by from a SELECT query.
    fn parse_select(nodes: &[&SyntaxData], plan: &Plan) -> Result<Select, SbroadError> {
        // We are trying to parse dql part of update query.
        // Arena has the following structure of a SELECT query:
        // [
        //      PlanId(Porjection),
        //          <expr>, <alias>, <comma>,
        //          <expr>, <alias>, <comma>,
        //          ...
        //      FROM, PlanId(ScanRelation),
        //      (OPTIONAL) IndexedBy(_),
        //      PlanId(Selection), <expr>
        //  ]
        //
        //  XXX Thanks to the checks in the parser (see `ensure_can_generate_local_sql_for_update`)
        //  - <expr> cannot have subqueries so it's safe to divide projection exprs by <alias>
        //  - there are no joins, so WHERE comes right after the part with from and scan
        //    (and maybe indexed by)

        let mut projection = Vec::new();
        let mut filter = Vec::new();
        let mut indexed_by = None;

        let mut nodes = nodes.iter();

        // Parse projection.
        while let Some(data) = nodes.next() {
            debug_assert!(matches!(
                data,
                SyntaxData::PlanId(_) | SyntaxData::Comma | SyntaxData::From
            ));

            if let SyntaxData::From = data {
                break;
            }

            let mut expr = Vec::new();
            for data in nodes.by_ref() {
                if let SyntaxData::Alias(_) = data {
                    break;
                }
                expr.push(SyntaxData::clone(*data));
            }

            projection.push(expr);
        }

        // Skip scan.
        let plan_id = nodes.next();
        let Some(SyntaxData::PlanId(scan_id)) = plan_id else {
            panic!("expected PlanId, got {:?}", plan_id);
        };
        let scan = plan.get_relation_node(*scan_id).expect("scan must be here");
        let Relational::ScanRelation(scan) = scan else {
            panic!("expected ScanRelation, got {:?}", scan);
        };

        // Set indexed by.
        if scan.indexed_by.is_some() {
            indexed_by = nodes.next().map(|x| (*x).clone());
        }

        // Parse filter.
        filter.extend(nodes.cloned().cloned());

        let plan_id = filter.first().expect("filter cannot be empty");
        let SyntaxData::PlanId(selection_id) = plan_id else {
            panic!("expected PlanId, got {:?}", plan_id);
        };

        let selection = plan.get_relation_node(*selection_id);
        debug_assert!(matches!(selection, Ok(Relational::Selection(_))));

        Ok(Select {
            projection,
            filter,
            indexed_by,
        })
    }

    let sql_params = plan.block_sql_params(query_id)?;
    let (constant_ids, params) = sql_params.into_parts();

    // TODO: replace with the actual value of `plan_id` when caching is implemented.
    let plan_id = 0;

    // INSERT is special: the generic SyntaxPlan panics on Insert nodes (DML can't
    // be rendered directly). After block-optimization the Insert sits on top of a
    // Values node, so we render the Values subtree and prepend an
    // `INSERT [OR REPLACE|IGNORE] INTO "t" ("c1", ...)` envelope.
    if let Relational::Insert(_) = plan.get_ir_plan().get_relation_node(query_id)? {
        let bucket_id = bucket_id.ok_or_else(|| {
            SbroadError::Other("INSERT in transaction requires a known target bucket".into())
        })?;
        return generate_insert_pattern_for_block(
            plan,
            query_id,
            plan_id,
            constant_ids,
            params,
            bucket_id,
            use_colon_params,
        );
    }

    let sp = SyntaxPlan::new(plan, query_id, Snapshot::Oldest, false)?;
    let mut on = OrderedSyntaxNodes::try_from(sp)?;
    if use_colon_params {
        on.render_params_with_colon();
    }
    let nodes = on.to_syntax_data()?;

    let ir_plan = plan.get_ir_plan();
    let pattern: String = match ir_plan.get_relation_node(query_id)? {
        Relational::Update(update) => {
            // Generate SQL for UPDATE by translating SELECT syntax data.
            let mut update_nodes = Vec::new();
            let select = parse_select(&nodes, ir_plan)?;

            // UPDATE <relation>
            update_nodes.push(SyntaxData::PlanId(query_id));

            let relation = ir_plan
                .relations
                .get(&update.relation)
                .expect("cannot miss UPDATE relation");

            // INDEXED BY
            if let Some(indexed_by) = select.indexed_by {
                update_nodes.push(indexed_by);
            }

            // SET
            update_nodes.push(SyntaxData::Inline(SmolStr::from(" SET ")));

            // col1 = expr1, col2 = expr2
            for (update_col, proj_col) in &update.update_columns_map {
                update_nodes.push(SyntaxData::Inline(format_smolstr!(
                    "\"{}\" = ",
                    relation.columns[*update_col].name,
                )));
                update_nodes.extend(select.projection[*proj_col].clone());
                update_nodes.push(SyntaxData::Comma);
            }
            // Remove last comma
            update_nodes.pop();

            // WHERE <expr>
            update_nodes.extend(select.filter);

            plan.generate_sql(
                &update_nodes,
                plan_id,
                table_name,
                Some(constant_ids.as_slice()),
            )?
        }
        Relational::Delete(Delete { child: Some(_), .. }) => {
            // Generate SQL for DELETE with WHERE by translating SELECT syntax data.
            let mut delete_nodes = Vec::new();
            let select = parse_select(&nodes, ir_plan)?;

            // DELETE FROM <relation>
            delete_nodes.push(SyntaxData::PlanId(query_id));

            // INDEXED BY
            if let Some(indexed_by) = select.indexed_by {
                delete_nodes.push(indexed_by);
            }

            // WHERE <expr>
            delete_nodes.extend(select.filter);

            plan.generate_sql(
                &delete_nodes,
                plan_id,
                table_name,
                Some(constant_ids.as_slice()),
            )?
        }
        _ => plan.generate_sql(&nodes, plan_id, table_name, Some(constant_ids.as_slice()))?,
    };

    Ok((
        BlockQuery {
            pattern,
            hooks: Vec::new(),
        },
        params,
    ))
}

/// Build the SQL pattern for a block-optimized INSERT.
///
/// The motion above Insert has already been eliminated by `optimize_block`, so the
/// child is a Values node. We render the Values subtree via `SyntaxPlan` and prepend
/// an `INSERT [OR REPLACE|IGNORE] INTO "t" ("c1", ...)` envelope inline.
#[allow(clippy::too_many_arguments)]
fn generate_insert_pattern_for_block(
    plan: &ExecutionPlan,
    insert_id: NodeId,
    plan_id: u64,
    constant_ids: Vec<NodeId>,
    mut params: Vec<Value>,
    bucket_id: u64,
    use_colon_params: bool,
) -> Result<(BlockQuery, Vec<Value>), SbroadError> {
    use crate::backend::sql::tree::SyntaxData;

    let Relational::Insert(insert) = plan.get_ir_plan().get_relation_node(insert_id)? else {
        panic!(
            "expected Insert node, got {:?}",
            plan.get_ir_plan().get_relation_node(insert_id)?
        )
    };

    let relation = plan
        .get_ir_plan()
        .relations
        .get(&insert.relation)
        .ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Table,
                format_smolstr!("{} among plan relations", insert.relation),
            )
        })?;

    // Find the bucket_id (Sharding-role) column on the table — local Tarantool
    // SQL won't compute it for us, so we have to provide its value explicitly.
    let bucket_col_name = relation
        .columns
        .iter()
        .find(|c| c.role == ColumnRole::Sharding)
        .map(|c| c.name.clone())
        .ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Table,
                Some(format_smolstr!(
                    "sharded table {} has no bucket_id column",
                    insert.relation
                )),
            )
        })?;

    let mut envelope = String::new();
    let mut hooks = Vec::new();
    envelope.push_str(match &insert.conflict_strategy {
        ConflictStrategy::DoFail => "INSERT INTO ",
        ConflictStrategy::DoReplace => "INSERT OR REPLACE INTO ",
        ConflictStrategy::DoNothing => "INSERT OR IGNORE INTO ",
        ConflictStrategy::DoUpdate { .. } => "INSERT INTO ",
    });
    envelope.push('"');
    envelope.push_str(&insert.relation);
    envelope.push_str("\" (");
    for col_pos in &insert.columns {
        let col_name = &relation
            .columns
            .get(*col_pos)
            .ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Column,
                    format_smolstr!("at position {col_pos} in {}", insert.relation),
                )
            })?
            .name;
        envelope.push('"');
        envelope.push_str(col_name);
        envelope.push_str("\", ");
    }
    envelope.push('"');
    envelope.push_str(&bucket_col_name);
    envelope.push_str("\")");

    let sp = SyntaxPlan::new(plan, insert.child, Snapshot::Oldest, false)?;
    let mut on = OrderedSyntaxNodes::try_from(sp)?;
    if use_colon_params {
        on.render_params_with_colon();
    }
    let values_nodes = on.to_syntax_data()?;

    // Append the bucket_id value to the params array once and reference it via
    // the same `$N` placeholder appended to every row's tuple.
    let bucket_param_idx = push_block_param(&mut params, Value::Integer(bucket_id as i64))?;
    let bucket_placeholder = if use_colon_params {
        format_smolstr!(", :{bucket_param_idx}")
    } else {
        format_smolstr!(", ${bucket_param_idx}")
    };

    let bind_iocdu_params = !plan.get_ir_plan().is_raw_explain();
    let parameter_indexes =
        append_iocdu_param_values(plan, insert, &mut params, bind_iocdu_params)?;
    if let ConflictStrategy::DoUpdate { payload } = &insert.conflict_strategy {
        hooks.push(prepare_iocdu_hook(
            plan,
            relation,
            payload,
            &parameter_indexes,
        )?);
    }

    // The Values syntax data renders as `VALUES (<row1>),(<row2>),...`. Track
    // parenthesis depth so we inject the bucket placeholder only at the row's
    // outer closing `)`, not inside nested function calls or sub-expressions.
    let mut nodes: Vec<SyntaxData> = Vec::with_capacity(values_nodes.len() + 1);
    nodes.push(SyntaxData::Inline(SmolStr::from(envelope)));
    let mut depth: i32 = 0;
    for data in values_nodes.iter() {
        match data {
            SyntaxData::OpenParenthesis => {
                depth += 1;
                nodes.push(SyntaxData::clone(*data));
            }
            SyntaxData::CloseParenthesis => {
                if depth == 1 {
                    nodes.push(SyntaxData::Inline(bucket_placeholder.clone()));
                }
                depth -= 1;
                nodes.push(SyntaxData::clone(*data));
            }
            _ => nodes.push(SyntaxData::clone(*data)),
        }
    }

    let pattern = plan.generate_sql(&nodes, plan_id, table_name, Some(constant_ids.as_slice()))?;
    Ok((BlockQuery { pattern, hooks }, params))
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

    if plan.get_ir_plan().is_block()? {
        let tier = {
            match plan.get_ir_plan().tier.as_ref() {
                None => coordinator.get_current_tier_name()?,
                tier => tier.cloned(),
            }
        };

        let request_id = plan.get_request_id().to_owned();

        let BlockOwned::Anonymous(block) = plan.get_ir_plan().get_owned_block_node(top_id)? else {
            unreachable!("plan.is_block() returned true");
        };

        let metadata = block
            .return_columns
            .iter()
            .map(|(name, ty)| MetadataColumn::new(name.to_string(), ty.to_string()))
            .collect();

        // For INSERT statements we need the concrete bucket id to populate the
        // bucket_id column. The block dispatcher already enforces that all
        // statements target a single bucket; extract it here.
        let block_bucket_id = match buckets {
            Buckets::Filtered(crate::ir::bucket::BucketSet::Exact(set)) => {
                assert!(set.len() == 1);
                set.iter().copied().next()
            }
            _ => None,
        };

        let unused_lets = block.get_unused_lets();

        let use_colon_params = !plan.get_ir_plan().is_raw_explain();
        let cache_key: Option<u64> = if use_colon_params {
            Some(block_pattern_key(plan, &block.statements)?)
        } else {
            None
        };
        let cached_patterns: Option<(u64, Rc<Vec<BlockStatement<BlockQuery>>>)> = match cache_key {
            Some(key) => {
                let cached = BLOCK_PATTERN_CACHE.with(|c| -> Result<_, SbroadError> {
                    Ok(c.borrow_mut().get(&key)?.cloned())
                })?;
                if cached.is_some() {
                    fire_block_pattern_cache_hook(|h| h.on_hit);
                } else {
                    fire_block_pattern_cache_hook(|h| h.on_miss);
                }
                cached.map(|c| (key, c))
            }
            None => None,
        };

        let mut params: Vec<Vec<Value>> = Vec::new();
        let statements: Vec<BlockStatement<BlockQuery>> = if let Some((_, cached)) =
            &cached_patterns
        {
            for entry in BlockEntries::new(&block.statements) {
                entry.with(|id| {
                    let query_params = generate_params_for_block_query(plan, *id, block_bucket_id)?;
                    params.push(query_params);
                    Ok(())
                })?
            }
            (**cached).clone()
        } else {
            let mut statements = Vec::with_capacity(block.statements.len());
            // FIXME: add error location tracing,
            // (e.g. Statement 1 (RETURN QUERY): some shit happened)
            for stmt in block.statements {
                let mapped = stmt.try_map(|id| -> Result<BlockQuery, SbroadError> {
                    let (query, stmt_params) = generate_pattern_with_params_for_block(
                        plan,
                        id,
                        block_bucket_id,
                        use_colon_params,
                    )?;
                    params.push(stmt_params);
                    Ok(query)
                })?;
                statements.push(mapped);
            }
            if let Some(key) = cache_key {
                let cached = Rc::new(statements.clone());
                BLOCK_PATTERN_CACHE.with(|c| -> Result<(), SbroadError> {
                    c.borrow_mut().put(key, cached)?;
                    Ok(())
                })?;
                fire_block_pattern_cache_hook(|h| h.on_added);
            }
            statements
        };

        let vdbe_max_steps = plan.get_ir_plan().effective_options.sql_vdbe_opcode_max as _;
        let table_versions = std::mem::take(&mut plan.get_mut_ir_plan().table_version_map);
        let index_versions = std::mem::take(&mut plan.get_mut_ir_plan().index_version_map);
        let tier_runtime = coordinator.get_vshard_object_by_tier(tier.as_ref())?;
        let exec_block = BlockExecData {
            statements,
            unused_lets,
            params,
            table_versions,
            index_versions,
            vdbe_max_steps,
            returns_rows: !block.return_columns.is_empty(),
            explain_options: plan.get_ir_plan().explain_options,
            bucket_count: tier_runtime.bucket_count(),
        };
        return tier_runtime.exec_block_on_buckets(
            metadata,
            exec_block,
            buckets,
            &request_id,
            port,
        );
    }

    let tier = {
        match plan.get_ir_plan().tier.as_ref() {
            None => coordinator.get_current_tier_name()?,
            tier => tier.cloned(),
        }
    };
    let tier_runtime = coordinator.get_vshard_object_by_tier(tier.as_ref())?;
    plan.prepare_bucket_filter_for_dispatch(top_id, buckets)?;
    let frozen_plan = Rc::new(std::mem::take(plan));
    let dispatch_result = if frozen_plan.get_ir_plan().is_raw_explain() {
        let top = frozen_plan.get_ir_plan().get_relation_node(top_id)?;
        let top_id = if top.is_dml() {
            let Some(dql_child_id) = dml_get_motion_child(&frozen_plan, top_id)? else {
                // Dispatch is called for each motion in `materialize_subtree`.
                // Each dispatch may mark the motion subtree as already materialized.
                // We should not return an error because the child motion may have been
                // removed from the effective execution view by a previous dispatch call.
                let restored = Rc::try_unwrap(frozen_plan).map_err(|_| {
                    SbroadError::Invalid(
                        Entity::Plan,
                        Some("frozen execution plan is still referenced".into()),
                    )
                })?;
                let mut restored = restored;
                restored.clear_bucket_filter();
                *plan = restored;
                return Ok(());
            };
            dql_child_id
        } else {
            top_id
        };
        tier_runtime.exec_ir_on_any_node(Rc::clone(&frozen_plan), top_id, buckets, port)
    } else if has_zero_limit_clause(&frozen_plan, top_id)? {
        empty_plan_write_for_top(port, &frozen_plan, top_id)
    } else {
        dispatch_by_buckets(
            Rc::clone(&frozen_plan),
            top_id,
            buckets,
            &tier_runtime,
            port,
        )
    };
    let restored = Rc::try_unwrap(frozen_plan).map_err(|_| {
        SbroadError::Invalid(
            Entity::Plan,
            Some("frozen execution plan is still referenced".into()),
        )
    })?;
    let mut restored = restored;
    restored.clear_bucket_filter();
    *plan = restored;
    dispatch_result
}

/// Helper function that chooses one of the methods for execution
/// based on buckets.
///
/// # Errors
/// - Failed to dispatch
pub fn dispatch_by_buckets<'p>(
    sub_plan: Rc<ExecutionPlan>,
    top_id: NodeId,
    buckets: &Buckets,
    runtime: &impl Vshard,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError> {
    match buckets {
        Buckets::Any => {
            let flags = sub_plan.subtree_dispatch_flags_at(top_id)?;
            if flags.has_customization_opcodes {
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
            if let Some(motion_id) = flags.segmented_motion_id {
                return Err(SbroadError::Invalid(
                    Entity::Motion,
                    Some(format_smolstr!("Motion ({motion_id:?}) in subtree with distribution Single, but policy is not Full.")),
                ));
            }
            runtime.exec_ir_on_any_node(sub_plan, top_id, buckets, port)?;
            Ok(())
        }
        Buckets::All | Buckets::Filtered(_) => {
            runtime.exec_ir_on_buckets(sub_plan, top_id, buckets, port)?;
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

    let Node::Relational(Relational::Values(Values { ref children, .. })) = child_node else {
        panic!("Values node expected. Got {child_node:?}.")
    };

    // Check if there is only constant children, e.g. `VALUES (1,2,3)`. In that case we can
    // materialize VALUES locally avoiding expensive `dispatch()` call.
    let mut only_constants = true;
    'rows_loop: for row_id in children {
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

    let mut vtable = if !only_constants {
        // We need to execute VALUES as a local SQL.
        let columns = vtable_columns(exec_plan.get_ir_plan(), values_id)?;

        let mut port = runtime.new_port();
        exec_plan.set_plan_id(values_id)?;
        runtime.dispatch(exec_plan, values_id, &Buckets::Any, &mut port)?;

        let mut vtable = VirtualTable::with_columns(columns);
        let scheduler_opts = runtime.get_scheduler_options();
        let mut ys = Scheduler::new(&scheduler_opts);
        for mp in port.iter().skip(1) {
            ys.maybe_yield(&scheduler_opts)
                .map_err(|e| SbroadError::Other(e.to_smolstr()))?;
            vtable.write_all(mp).map_err(|e| {
                SbroadError::FailedTo(
                    Action::Create,
                    Some(Entity::VirtualTable),
                    format_smolstr!("{e}"),
                )
            })?;
        }
        vtable
    } else {
        let limit = exec_plan.get_ir_plan().effective_options.sql_motion_row_max;
        let values_count = children.len();
        if limit > 0 && limit < values_count as i64 {
            return Err(SbroadError::ExecutionError(format_smolstr!(
                "Exceeded maximum number of rows ({}) in virtual table: {}",
                limit,
                values_count,
            )));
        }

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

        // Create vtable columns with default column field (that will be fixed later).
        let mut vtable = VirtualTable::with_columns(vec![Column::default(); columns_len]);
        // All children are constants, can materialize VALUES locally and take all constants.
        vtable.get_mut_tuples().reserve(children.len());
        for row_id in children.clone() {
            let row_node = exec_plan.get_mut_ir_plan().get_relation_node(row_id)?;
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
    let top_id = plan.get_ir_plan().get_motion_subtree_root(motion_node_id)?;

    let ir = plan.get_ir_plan();
    let top_node = ir.get_relation_node(top_id)?;
    assert!(
        !top_node.is_dml(),
        "materialize motion can be called only for DQL queries"
    );
    plan.set_plan_id(top_id)?;

    // We should get a motion alias name before dispatching the subtree.
    let motion_node = plan.get_ir_plan().get_relation_node(motion_node_id)?;
    let alias = if let Relational::Motion(Motion { alias, .. }) = motion_node {
        alias.clone()
    } else {
        panic!("Expected motion node, got {motion_node:?}");
    };

    let columns = vtable_columns(plan.get_ir_plan(), top_id)?;

    let mut port = runtime.new_port();
    // Dispatch the motion subtree.
    runtime.dispatch(plan, top_id, buckets, &mut port)?;

    if !plan.get_ir_plan().is_dml_on_global_table()? {
        // Mark motion node's child subtree as already materialized.
        plan.mark_motion_subtree_unlinked(motion_node_id)?;
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
        let scheduler_opts = runtime.get_scheduler_options();
        let mut ys = Scheduler::new(&scheduler_opts);
        for mp in port.iter().skip(1) {
            ys.maybe_yield(&scheduler_opts)
                .map_err(|e| SbroadError::Other(e.to_smolstr()))?;
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
    let top_id = plan.get_ir_plan().get_top()?;
    try_get_metadata_from_plan_for_top(plan, top_id)
}

/// Try to get metadata from a top node. If the node is not DQL, `None` is returned.
///
/// # Errors
/// - Invalid execution plan.
pub fn try_get_metadata_from_plan_for_top(
    plan: &ExecutionPlan,
    top_id: NodeId,
) -> Result<Option<Vec<MetadataColumn>>, SbroadError> {
    let ir = plan.get_ir_plan();
    if ir.get_relation_node(top_id)?.is_dml() || ir.is_raw_explain() {
        return Ok(None);
    }

    // Get metadata (column types) from the top node's output tuple.
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

fn metadata_write<'p>(
    port: &mut impl Port<'p>,
    plan: &Plan,
    top_id: NodeId,
) -> Result<(), SbroadError> {
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
    let top_id = plan.get_ir_plan().get_top()?;
    empty_plan_write_for_top(port, plan, top_id)
}

pub fn empty_plan_write_for_top<'p>(
    port: &mut impl Port<'p>,
    plan: &ExecutionPlan,
    top_id: NodeId,
) -> Result<(), SbroadError> {
    let top = plan.get_ir_plan().get_relation_node(top_id)?;
    match top.is_dml() {
        true => {
            port.add_mp(b"\xcc\x00".as_ref());
        }
        false => {
            metadata_write(port, plan.get_ir_plan(), top_id)?;
        }
    }
    Ok(())
}
