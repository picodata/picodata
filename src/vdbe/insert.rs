use super::ffi::{
    decimal_t, sql_insert_hook, sql_insert_hook_args, sql_insert_hook_value,
    sql_insert_hook_var_value, OP_IdxInsert, VdbeOp, OPFLAG_NCHANGE, P4_NOTUSED, P4_PTR,
    SQL_INSERT_HOOK_VALUE_BOOL, SQL_INSERT_HOOK_VALUE_DECIMAL, SQL_INSERT_HOOK_VALUE_DOUBLE,
    SQL_INSERT_HOOK_VALUE_INT, SQL_INSERT_HOOK_VALUE_NULL, SQL_INSERT_HOOK_VALUE_UINT,
    SQL_INSERT_HOOK_VALUE_UNSUPPORTED,
};
use crate::sql::conflict::{
    build_update_ops_from_values, prepare_do_update_from_catalog, run_do_update_with_ops,
    DoUpdateRouteTemplate,
};
use smol_str::{format_smolstr, SmolStr};
use sql::errors::SbroadError;
use sql::executor::engine::{BlockRuntimeHook, VersionMap};
use sql::executor::vdbe::SqlError;
use sql::ir::operator::{ConflictUpdateOp, ConflictUpdateValue};
use sql::ir::types::UnrestrictedType;
use sql::ir::value::double::Double;
use sql::ir::value::Value;
use std::collections::HashMap;
use std::marker::PhantomPinned;
use std::pin::Pin;
use tarantool::decimal::Decimal;
use tarantool::error::TarantoolErrorCode;
use tarantool::ffi::decimal::decNumber;
use tarantool::space::{Space, UpdateOps};
use tarantool::tuple::RawBytes;

/// Runtime context attached to a patched `OP_IdxInsert`.
///
/// Tarantool stores a raw pointer to this value in `OP_IdxInsert.p4.p` with
/// `P4_PTR`. It is owned by the assembled block wrapper and must outlive the
/// VDBE statement whose opcode references it.
pub(super) struct VdbeIdxInsertHookCtx {
    route: DoUpdateRouteTemplate,
    updates: VdbeConflictUpdates,
}

/// `ON CONFLICT DO UPDATE SET` assignments prepared for VDBE execution.
enum VdbeConflictUpdates {
    /// Every RHS is a literal — update ops are built once at attach time.
    Precomputed(UpdateOps),
    /// Params or LET vars present — ops are materialized on every hook run.
    PerRow(Vec<VdbeConflictUpdateItem>),
}

/// One `ON CONFLICT DO UPDATE SET` assignment prepared for VDBE execution.
struct VdbeConflictUpdateItem {
    column: usize,
    op: ConflictUpdateOp,
    value: VdbeConflictUpdateValue,
}

/// Runtime source for a DO UPDATE RHS inside a transaction block.
enum VdbeConflictUpdateValue {
    Const(Value),
    Param {
        index: u16,
        slot: i32,
        cast_type: UnrestrictedType,
    },
    LetVar {
        name: SmolStr,
        slot: i32,
        cast_type: UnrestrictedType,
    },
}

/// C ABI arguments passed by Tarantool's SQL insert hook.
pub(crate) type SqlInsertHookArgs = sql_insert_hook_args;

/// C ABI payload stored in `OP_IdxInsert.p4.p`.
type SqlInsertHookPayload = sql_insert_hook;

/// Owned hook payload attached to a patched `OP_IdxInsert`.
///
/// Stored as `Pin<Box<_>>` because `OP_IdxInsert.p4.p` holds a raw pointer to
/// `hook`; moving the handle would invalidate that pointer.
pub(super) struct VdbeIdxInsertHookHandle {
    hook: SqlInsertHookPayload,
    _context: Box<VdbeIdxInsertHookCtx>,
    // OP_IdxInsert.p4.p stores a pointer to `hook`, so this handle must not move.
    _pin: PhantomPinned,
}

fn lookup_let_slot(
    defined_let_vars: &HashMap<SmolStr, i32>,
    name: &SmolStr,
) -> Result<i32, String> {
    let prefixed = format_smolstr!(":{name}");
    defined_let_vars
        .get(&prefixed)
        .copied()
        .ok_or_else(|| format!("use of variable :{name} before its assignment"))
}

fn convert_hook_decimal(raw: decimal_t) -> Decimal {
    let dec = decNumber {
        digits: raw.digits,
        exponent: raw.exponent,
        bits: raw.bits,
        lsu: raw.lsu,
    };

    // SAFETY: VDBE fills this value from a valid decimal Mem cell when
    // `type_ == SQL_INSERT_HOOK_VALUE_DECIMAL`.
    unsafe { Decimal::from_raw(dec) }
}

fn read_insert_hook_var_value(
    args: &SqlInsertHookArgs,
    slot: i32,
    label: &str,
) -> Result<Value, String> {
    let mut raw = std::mem::MaybeUninit::<sql_insert_hook_value>::zeroed();
    let rc = unsafe { sql_insert_hook_var_value(args.a_var, args.n_var, slot, raw.as_mut_ptr()) };
    if rc != 0 {
        return Err(format!("failed to read SQL insert hook {label}"));
    }
    let raw = unsafe { raw.assume_init() };

    match raw.type_ {
        SQL_INSERT_HOOK_VALUE_NULL => Ok(Value::Null),
        SQL_INSERT_HOOK_VALUE_INT => Ok(Value::Integer(unsafe { raw.u.i })),
        SQL_INSERT_HOOK_VALUE_UINT => {
            let value = unsafe { raw.u.u };
            let value = i64::try_from(value)
                .map_err(|_| format!("SQL insert hook {label} unsigned value is too large"))?;
            Ok(Value::Integer(value))
        }
        SQL_INSERT_HOOK_VALUE_BOOL => Ok(Value::Boolean(unsafe { raw.u.b })),
        SQL_INSERT_HOOK_VALUE_DOUBLE => Ok(Value::Double(Double::from(unsafe { raw.u.d }))),
        SQL_INSERT_HOOK_VALUE_DECIMAL => {
            // SAFETY: `type_ == SQL_INSERT_HOOK_VALUE_DECIMAL`, so reading the
            // decimal union field is valid.
            let raw_dec = unsafe { raw.u.dec };
            let dec = convert_hook_decimal(raw_dec);
            Ok(Value::Decimal(Box::new(dec)))
        }
        SQL_INSERT_HOOK_VALUE_UNSUPPORTED => Err(format!(
            "SQL insert hook {label} has unsupported value type"
        )),
        other => Err(format!(
            "SQL insert hook {label} has unknown value type {other}"
        )),
    }
}

fn materialize_conflict_update_value(
    value: &VdbeConflictUpdateValue,
    args: &SqlInsertHookArgs,
) -> Result<Value, String> {
    let (label, slot, cast_type) = match value {
        VdbeConflictUpdateValue::Const(value) => return Ok(value.clone()),
        VdbeConflictUpdateValue::Param {
            index,
            slot,
            cast_type,
        } => (format!("parameter ${index}"), *slot, *cast_type),
        VdbeConflictUpdateValue::LetVar {
            name,
            slot,
            cast_type,
        } => (format!("LET variable :{name}"), *slot, *cast_type),
    };
    if slot <= 0 || slot > args.n_var {
        return Err(format!(
            "SQL insert hook {label} has invalid aVar slot {slot}"
        ));
    }
    if args.a_var.is_null() {
        return Err(format!(
            "SQL insert hook {label} cannot be read: aVar is null"
        ));
    }
    let raw_value = read_insert_hook_var_value(args, slot, &label)?;
    raw_value
        .cast(cast_type)
        .map_err(|e| format!("failed to cast SQL insert hook {label}: {e}"))
}

/// Invoked from Tarantool's `OP_IdxInsert` when an attached insert hook is
/// present.
unsafe extern "C" fn run_sql_insert_hook(args: *mut SqlInsertHookArgs) -> libc::c_int {
    if args.is_null() {
        tarantool::set_error!(
            TarantoolErrorCode::ProcC,
            "SQL insert hook arguments are null"
        );
        return -1;
    }

    let args = unsafe { &*args };
    if args.ctx.is_null() || args.tuple.is_null() || args.tuple_end.is_null() {
        tarantool::set_error!(TarantoolErrorCode::ProcC, "SQL insert hook context is null");
        return -1;
    }

    let len = unsafe { args.tuple_end.offset_from(args.tuple) };
    if len < 0 {
        tarantool::set_error!(
            TarantoolErrorCode::ProcC,
            "SQL insert hook tuple is invalid"
        );
        return -1;
    }

    let ctx = unsafe { &*(args.ctx as *const VdbeIdxInsertHookCtx) };
    let tuple = unsafe { std::slice::from_raw_parts(args.tuple.cast::<u8>(), len as usize) };
    let space = unsafe { Space::from_id_unchecked(args.space_id) };
    let insert_tuple = RawBytes::new(tuple);

    let per_row_ops;
    let update_ops = match &ctx.updates {
        VdbeConflictUpdates::Precomputed(ops) => ops,
        VdbeConflictUpdates::PerRow(items) => {
            let mut update_values = Vec::with_capacity(items.len());
            for item in items {
                match materialize_conflict_update_value(&item.value, args) {
                    Ok(value) => update_values.push(value),
                    Err(e) => {
                        tarantool::set_error!(TarantoolErrorCode::ProcC, "{e}");
                        return -1;
                    }
                }
            }
            let update_items = items
                .iter()
                .zip(&update_values)
                .map(|(item, value)| (item.column, item.op, value));
            per_row_ops = match build_update_ops_from_values(items.len(), update_items) {
                Ok(ops) => ops,
                Err(e) => {
                    tarantool::set_error!(TarantoolErrorCode::ProcC, "{e}");
                    return -1;
                }
            };
            &per_row_ops
        }
    };

    match run_do_update_with_ops(&space, insert_tuple, &ctx.route, update_ops) {
        Ok(()) => 0,
        Err(e) => {
            tarantool::set_error!(TarantoolErrorCode::ProcC, "{e}");
            -1
        }
    }
}

pub(super) fn attach_idx_insert_hook(
    ops: &mut [VdbeOp],
    hook: &BlockRuntimeHook,
    defined_let_vars: &HashMap<SmolStr, i32>,
    query_param_offset: i32,
    table_versions: &VersionMap,
) -> Result<Pin<Box<VdbeIdxInsertHookHandle>>, SqlError> {
    let BlockRuntimeHook::IdxInsertOnConflictDoUpdate {
        table_id, update, ..
    } = hook;
    let expected_table_version = table_versions
        .get(table_id)
        .copied()
        .ok_or_else(|| format!("missing schema version for table {table_id}"))?;
    let route = prepare_do_update_from_catalog(*table_id, expected_table_version, update)
        .map_err(prepare_iocdu_error)?;
    let updates = update
        .items
        .iter()
        .map(|item| {
            let value = match &item.value {
                ConflictUpdateValue::Const(value) => VdbeConflictUpdateValue::Const(value.clone()),
                ConflictUpdateValue::Param { index, cast_type } => VdbeConflictUpdateValue::Param {
                    index: *index,
                    slot: query_param_offset + i32::from(*index),
                    cast_type: *cast_type,
                },
                ConflictUpdateValue::LetVar { var, cast_type } => VdbeConflictUpdateValue::LetVar {
                    name: var.name.clone(),
                    slot: lookup_let_slot(defined_let_vars, &var.name)?,
                    cast_type: *cast_type,
                },
            };
            Ok(VdbeConflictUpdateItem {
                column: item.column,
                op: item.op,
                value,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    // All-literal updates are loop-invariant: build the ops once at attach
    // time instead of on every hook invocation (i.e. per inserted row).
    let updates = if updates
        .iter()
        .all(|item| matches!(item.value, VdbeConflictUpdateValue::Const(_)))
    {
        let values = updates.iter().map(|item| match &item.value {
            VdbeConflictUpdateValue::Const(value) => (item.column, item.op, value),
            _ => unreachable!("checked to be Const above"),
        });
        let ops =
            build_update_ops_from_values(updates.len(), values).map_err(reassemble_iocdu_error)?;
        VdbeConflictUpdates::Precomputed(ops)
    } else {
        VdbeConflictUpdates::PerRow(updates)
    };
    let mut context = Box::new(VdbeIdxInsertHookCtx { route, updates });
    let context_ptr = (&mut *context as *mut VdbeIdxInsertHookCtx).cast();
    let handle = Box::pin(VdbeIdxInsertHookHandle {
        hook: SqlInsertHookPayload {
            run: Some(run_sql_insert_hook),
            ctx: context_ptr,
        },
        _context: context,
        _pin: PhantomPinned,
    });

    let mut idx_inserts = ops
        .iter_mut()
        .filter(|op| op.opcode as i32 == OP_IdxInsert && (op.p5 & OPFLAG_NCHANGE as u16) != 0);
    let Some(op) = idx_inserts.next() else {
        return Err("expected one final OP_IdxInsert, got 0".into());
    };
    if idx_inserts.next().is_some() {
        return Err("expected one final OP_IdxInsert, got more than one".into());
    }
    if op.p4type != P4_NOTUSED as i8 {
        return Err("final OP_IdxInsert already has P4".into());
    }
    op.p4type = P4_PTR as _;
    op.p4.p = (&handle.as_ref().get_ref().hook as *const SqlInsertHookPayload)
        .cast_mut()
        .cast();

    Ok(handle)
}

fn prepare_iocdu_error(error: SbroadError) -> SqlError {
    match error {
        SbroadError::OutdatedStorageSchema => SqlError::OutdatedStorageSchema,
        error => reassemble_iocdu_error(error),
    }
}

fn reassemble_iocdu_error(error: impl ToString) -> SqlError {
    SqlError::FailedToReassembleProgram(error.to_string())
}
