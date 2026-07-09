//! Runtime support for transactional `INSERT ... ON CONFLICT (...) DO UPDATE`.
//!
//! This module resolves explicit conflict targets to concrete unique indexes
//! and executes the local insert-or-update flow used by transactional blocks.

use crate::schema::{IndexDef, IndexOption, ADMIN_ID};
use crate::sql::router::{get_table_version_by_id, table_by_id};
use crate::storage::{index_by_ids_unchecked, Catalog};
use ahash::{AHashMap, AHashSet};
use smol_str::{format_smolstr, SmolStr};
use sql::errors::{Action, Entity, SbroadError};
use sql::ir::operator::{BlockConflictDoUpdate, ConflictTarget, ConflictUpdateOp};
use sql::ir::relation::Table;
use sql::ir::value::Value;
use std::borrow::Cow;
use std::ffi::{CStr, CString};
use tarantool::error::{Error as TntError, TarantoolErrorCode};
use tarantool::index::Index;
use tarantool::msgpack::MARKER_NULL;
use tarantool::session::with_su;
use tarantool::space::{Space, UpdateOps};
use tarantool::tuple::{FieldType, KeyDef, KeyDefPart, RawBytes, Tuple, TupleBuffer};

type Result<T> = std::result::Result<T, SbroadError>;

#[derive(Debug)]
struct ConflictIndexContext {
    name: SmolStr,
    index: Index,
    key_def: KeyDef,
    nullable_key_fields: Vec<u32>,
}

#[derive(Debug)]
pub(crate) struct DoUpdateRouteTemplate {
    table_name: SmolStr,
    method: DoUpdateMethod,
    index_contexts: Vec<ConflictIndexContext>,
}

/// Storage method used to execute the `DO UPDATE` branch.
#[derive(Clone, Copy, Debug)]
enum DoUpdateMethod {
    /// Insert first, then resolve the target and apply Tarantool `update`.
    Update,
    /// Use Tarantool `upsert` directly.
    Upsert,
}

/// Pins block `DO UPDATE` metadata to storage indexes from the declared table
/// schema version and resolves the concrete storage handles used to execute
/// the local insert-or-update flow.
pub(crate) fn prepare_do_update_from_catalog(
    table_id: u32,
    expected_table_version: u64,
    update: &BlockConflictDoUpdate,
) -> Result<DoUpdateRouteTemplate> {
    validate_table_version(table_id, expected_table_version)?;
    let route = with_su(ADMIN_ID, || -> Result<_> {
        let table = table_by_id(table_id)?;
        resolve_target_indexes(Catalog::get(), &table, &update.target)
    })
    .map_err(|e| SbroadError::FailedTo(Action::Get, None, format_smolstr!("with_su: {}", e)))??;
    validate_table_version(table_id, expected_table_version)?;
    Ok(route)
}

fn validate_table_version(table_id: u32, expected_table_version: u64) -> Result<()> {
    if get_table_version_by_id(table_id)? != expected_table_version {
        return Err(SbroadError::OutdatedStorageSchema);
    }
    Ok(())
}

/// Executes one tuple through a prepared route template using update
/// operations materialized by the caller.
pub(crate) fn run_do_update_with_ops(
    space: &Space,
    insert_tuple: &RawBytes,
    route: &DoUpdateRouteTemplate,
    update_ops: &UpdateOps,
    seen_keys: &mut AHashSet<Vec<u8>>,
) -> Result<()> {
    let tuple = insert_tuple_from_raw(insert_tuple)?;
    for context in &route.index_contexts {
        if target_key_has_null(context, &tuple)? {
            continue;
        }
        let key = extract_update_key(&context.key_def, &tuple)?;
        if !seen_keys.insert(key.into()) {
            return Err(SbroadError::other(
                "ON CONFLICT DO UPDATE command cannot affect row a second time",
            ));
        }
        break;
    }

    match route.method {
        DoUpdateMethod::Upsert => space
            .upsert(insert_tuple, update_ops.as_slice())
            .map_err(|e| do_update_error(Action::Update, Entity::Space, route, e)),
        DoUpdateMethod::Update => match space.insert(insert_tuple) {
            Ok(_) => Ok(()),
            Err(error) if is_tuple_found(&error) => {
                // The conflicting tuple may be deleted between insert failure and
                // conflict-target lookup/update; surface the original insert error.
                match update_target_resolved(insert_tuple, route, update_ops)? {
                    Some(_) => Ok(()),
                    None => Err(do_update_error(Action::Insert, Entity::Space, route, error)),
                }
            }
            Err(error) => Err(do_update_error(Action::Insert, Entity::Space, route, error)),
        },
    }
}

fn is_tuple_found(error: &TntError) -> bool {
    matches!(
        error,
        TntError::Tarantool(error) if error.error_code() == TarantoolErrorCode::TupleFound as u32
    )
}

fn do_update_error(
    action: Action,
    entity: Entity,
    route: &DoUpdateRouteTemplate,
    error: TntError,
) -> SbroadError {
    if is_tuple_found(&error) {
        return SbroadError::other(format_smolstr!(
            "duplicate key exists in table \"{}\"",
            route.table_name
        ));
    }

    SbroadError::FailedTo(action, Some(entity), format_smolstr!("{error}"))
}

fn resolve_target_indexes(
    storage: &Catalog,
    table: &Table,
    target: &ConflictTarget,
) -> Result<DoUpdateRouteTemplate> {
    let target_column_names = collect_column_names(table, target.columns())?;
    let field_positions = collect_column_positions(table);
    let mut indexes = Vec::new();
    let mut primary_index = None;
    let mut total_indexes = 0usize;

    let index_defs = storage.indexes.by_space_id(table.id).map_err(|e| {
        SbroadError::FailedTo(
            Action::Get,
            Some(Entity::Index),
            format_smolstr!("failed to get indexes for table {}: {e}", table.name),
        )
    })?;

    for index in index_defs {
        total_indexes += 1;
        let is_primary = index.id == 0;
        if unique_index_matches_target(&index, &target_column_names) {
            indexes.push(index);
        } else if is_primary {
            primary_index = Some(index);
        }
    }

    if physical_primary_key_matches_target(table, target.columns())
        && !indexes.iter().any(|index| index.id == 0)
    {
        let Some(primary) = primary_index else {
            return Err(SbroadError::OutdatedStorageSchema);
        };
        if !primary.operable || !is_unique_index(&primary.opts) {
            return Err(SbroadError::OutdatedStorageSchema);
        }
        indexes.push(primary);
    }

    if indexes.is_empty() {
        return Err(SbroadError::Invalid(
            Entity::Query,
            Some(
                "there is no unique or exclusion constraint matching the ON CONFLICT specification"
                    .into(),
            ),
        ));
    }

    // Keep first-match conflict resolution deterministic across catalog reloads.
    indexes.sort_by_key(|index| index.id);

    let method = if matches!(indexes.as_slice(), [index] if index.id == 0) && total_indexes == 1 {
        DoUpdateMethod::Upsert
    } else {
        DoUpdateMethod::Update
    };

    let index_contexts = indexes
        .iter()
        .map(|index| build_index_context(index, &field_positions))
        .collect::<Result<Vec<_>>>()?;

    Ok(DoUpdateRouteTemplate {
        table_name: table.name.clone(),
        method,
        index_contexts,
    })
}

fn unique_index_matches_target(index: &IndexDef, target_column_names: &AHashSet<&str>) -> bool {
    index.operable
        && is_unique_index(&index.opts)
        && index.parts.len() == target_column_names.len()
        && index
            .parts
            .iter()
            .all(|part| target_column_names.contains(part.field.as_str()))
}

fn is_unique_index(opts: &[IndexOption]) -> bool {
    opts.iter()
        .any(|opt| matches!(opt, IndexOption::Unique(true)))
}

fn collect_column_positions(table: &Table) -> AHashMap<&str, u32> {
    table
        .columns
        .iter()
        .enumerate()
        .map(|(position, column)| {
            (
                column.name.as_str(),
                u32::try_from(position).expect("table column position should fit into u32"),
            )
        })
        .collect()
}

/// Pins one matched index to concrete storage handles: resolves part field
/// names to positions and builds the `KeyDef` used to extract conflict keys.
fn build_index_context(
    index_def: &IndexDef,
    field_positions: &AHashMap<&str, u32>,
) -> Result<ConflictIndexContext> {
    let mut nullable_key_fields = Vec::new();
    let key_parts = index_def
        .parts
        .iter()
        .map(|part| {
            let Some(field_no) = field_positions.get(part.field.as_str()) else {
                return Err(SbroadError::OutdatedStorageSchema);
            };
            let is_nullable = part.is_nullable.unwrap_or(false);
            if is_nullable && part.path.is_none() {
                nullable_key_fields.push(*field_no);
            }
            Ok(KeyDefPart {
                field_no: *field_no,
                field_type: part.r#type.map(FieldType::from).unwrap_or(FieldType::Any),
                collation: key_part_cstr(part.collation.as_deref(), &index_def.name, "collation")?,
                is_nullable,
                path: key_part_cstr(part.path.as_deref(), &index_def.name, "path")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let key_def = KeyDef::new(&key_parts).map_err(|e| {
        SbroadError::FailedTo(
            Action::Build,
            Some(Entity::Index),
            format_smolstr!(
                "conflict target {} has invalid key definition: {e}",
                index_def.name
            ),
        )
    })?;
    Ok(ConflictIndexContext {
        name: index_def.name.clone(),
        index: index_by_ids_unchecked(index_def.table_id, index_def.id),
        key_def,
        nullable_key_fields,
    })
}

fn key_part_cstr<'a>(
    value: Option<&str>,
    index_name: &str,
    attr: &str,
) -> Result<Option<Cow<'a, CStr>>> {
    value
        .map(|s| {
            CString::new(s).map(Cow::Owned).map_err(|e| {
                SbroadError::FailedTo(
                    Action::Build,
                    Some(Entity::Index),
                    format_smolstr!("conflict target {index_name} has invalid {attr} {s}: {e}"),
                )
            })
        })
        .transpose()
}

fn collect_column_names<'a>(table: &'a Table, positions: &[usize]) -> Result<AHashSet<&'a str>> {
    positions
        .iter()
        .map(|pos| {
            table
                .columns
                .get(*pos)
                .ok_or_else(|| {
                    SbroadError::NotFound(
                        Entity::Column,
                        format_smolstr!("at position {pos} for table {}", table.name),
                    )
                })
                .map(|col| col.name.as_str())
        })
        .collect()
}

fn physical_primary_key_matches_target(table: &Table, target_columns: &[usize]) -> bool {
    table
        .primary_key
        .positions
        .iter()
        .copied()
        .collect::<AHashSet<_>>()
        == target_columns.iter().copied().collect::<AHashSet<_>>()
}

fn extract_update_key(key_def: &KeyDef, insert_tuple: &Tuple) -> Result<TupleBuffer> {
    key_def.extract_key(insert_tuple).map_err(|e| {
        SbroadError::FailedTo(
            Action::Build,
            Some(Entity::Index),
            format_smolstr!("failed to extract conflict update key: {e}"),
        )
    })
}

fn insert_tuple_from_raw(insert_tuple: &RawBytes) -> Result<Tuple> {
    Tuple::try_from_slice(insert_tuple).map_err(|e| {
        SbroadError::FailedTo(
            Action::Encode,
            Some(Entity::Tuple),
            format_smolstr!("failed to encode inserted tuple for conflict update: {e}"),
        )
    })
}

fn target_key_has_null(context: &ConflictIndexContext, tuple: &Tuple) -> Result<bool> {
    for field_no in &context.nullable_key_fields {
        let field = tuple.field::<&RawBytes>(*field_no).map_err(|e| {
            SbroadError::FailedTo(
                Action::Decode,
                Some(Entity::Tuple),
                format_smolstr!("failed to read nullable conflict target field {field_no}: {e}"),
            )
        })?;
        let Some(field) = field else {
            return Ok(true);
        };
        if field.first().copied() == Some(MARKER_NULL) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn find_target_context<'a>(
    insert_tuple: &RawBytes,
    index_contexts: &'a [ConflictIndexContext],
) -> Result<Option<(&'a ConflictIndexContext, TupleBuffer)>> {
    let tuple = insert_tuple_from_raw(insert_tuple)?;
    for context in index_contexts {
        if target_key_has_null(context, &tuple)? {
            continue;
        }
        let key = extract_update_key(&context.key_def, &tuple)?;
        let existing = context.index.get(&key).map_err(|e| {
            SbroadError::FailedTo(
                Action::Get,
                Some(Entity::Tuple),
                format_smolstr!(
                    "failed to get tuple by explicit conflict target {}: {e}",
                    context.name
                ),
            )
        })?;
        if existing.is_some() {
            return Ok(Some((context, key)));
        }
    }
    Ok(None)
}

fn update_target_resolved(
    insert_tuple: &RawBytes,
    route: &DoUpdateRouteTemplate,
    update_ops: &UpdateOps,
) -> Result<Option<Tuple>> {
    let Some((context, key)) = find_target_context(insert_tuple, &route.index_contexts)? else {
        return Ok(None);
    };
    context
        .index
        .update(&key, update_ops.as_slice())
        .map_err(|e| do_update_error(Action::Update, Entity::Tuple, route, e))
}

pub(crate) fn build_update_ops_from_values<'a>(
    len: usize,
    items: impl IntoIterator<Item = (usize, ConflictUpdateOp, &'a Value)>,
) -> Result<UpdateOps> {
    let mut ops = UpdateOps::with_capacity(len);
    for (column, op, value) in items {
        match (&op, value) {
            (ConflictUpdateOp::AddAssign, Value::Integer(v)) => ops.sql_add(column, *v),
            (ConflictUpdateOp::AddAssign, Value::Decimal(v)) => ops.sql_add(column, **v),
            (ConflictUpdateOp::AddAssign, Value::Double(v)) => ops.sql_add(column, v.value),
            (ConflictUpdateOp::AddAssign, Value::Null) => ops.sql_add(column, Option::<i64>::None),
            (ConflictUpdateOp::SubAssign, Value::Integer(v)) => ops.sql_sub(column, *v),
            (ConflictUpdateOp::SubAssign, Value::Decimal(v)) => ops.sql_sub(column, **v),
            (ConflictUpdateOp::SubAssign, Value::Double(v)) => ops.sql_sub(column, v.value),
            (ConflictUpdateOp::SubAssign, Value::Null) => ops.sql_sub(column, Option::<i64>::None),
            (ConflictUpdateOp::AndAssign, Value::Boolean(v)) => ops.sql_and(column, *v),
            (ConflictUpdateOp::AndAssign, Value::Null) => ops.sql_and(column, Option::<bool>::None),
            (ConflictUpdateOp::OrAssign, Value::Boolean(v)) => ops.sql_or(column, *v),
            (ConflictUpdateOp::OrAssign, Value::Null) => ops.sql_or(column, Option::<bool>::None),
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::Value,
                    Some(format_smolstr!(
                        "value {} is not supported for conflict update opcode {}",
                        value,
                        op
                    )),
                ));
            }
        }
        .map(|_| ())
        .map_err(|e| {
            SbroadError::Invalid(
                Entity::TupleBuilderCommand,
                Some(format_smolstr!("failed to build conflict update op: {e}")),
            )
        })?;
    }
    Ok(ops)
}
