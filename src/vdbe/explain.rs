//! Helpers for adding Picodata-specific rows to Tarantool RAW EXPLAIN.
//!
//! Tarantool owns VDBE construction and knows where `OP_Explain` rows can be
//! emitted, while Picodata owns the details that should appear in those rows.
//! This module bridges the two sides by passing a provider into statement
//! compilation. Tarantool asks the provider for extra details while the VDBE is
//! still being built, before the prepared statement becomes immutable.

use super::ffi::{
    sql_explain_hook, sql_explain_hook_args, sql_raw_explain_event, sql_raw_explain_provider,
    sql_stmt, sql_stmt_compile_raw_explain, SQL_RAW_EXPLAIN_IDX_INSERT,
};
use sql::executor::vdbe::{SqlError, SqlStmt, VdbeOwnedPayload};
use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use tarantool::error::TarantoolError;

/// One RAW EXPLAIN detail row prepared for Tarantool.
///
/// `hook` is the C payload stored in an `OP_Explain` opcode. `_detail` owns the
/// string returned by that payload, so the raw pointer in `hook.ctx` stays valid
/// for as long as the statement keeps this provider alive.
struct RawExplainHook {
    hook: sql_explain_hook,
    _detail: CString,
}

/// Provider passed to Tarantool while compiling a RAW EXPLAIN statement.
///
/// It owns all hook payloads and records how many construction events Tarantool
/// consumed. After compilation, the caller stores this provider in the compiled
/// statement so pointers embedded into `OP_Explain` remain valid until
/// statement finalization.
pub(crate) struct RawExplainProvider {
    provider: sql_raw_explain_provider,
    hooks: Vec<RawExplainHook>,
    next: usize,
    unexpected_events: usize,
}

impl VdbeOwnedPayload for RawExplainProvider {}

unsafe extern "C" fn run_sql_explain_hook(args: *mut sql_explain_hook_args) -> *const c_char {
    if args.is_null() {
        return ptr::null();
    }
    // SAFETY: Tarantool calls this function with a valid `sql_explain_hook_args`
    // pointer when executing an `OP_Explain` opcode that references this hook.
    let args = unsafe { &*args };
    if args.ctx.is_null() {
        return ptr::null();
    }
    args.ctx.cast::<c_char>()
}

unsafe extern "C" fn resolve_raw_explain_hook(
    event: sql_raw_explain_event,
    ctx: *mut c_void,
) -> *mut sql_explain_hook {
    if ctx.is_null() {
        return ptr::null_mut();
    }
    // SAFETY: `RawExplainProvider::new` stores a pointer to the provider's heap
    // allocation in `sql_raw_explain_provider.ctx`, and that allocation stays
    // alive and unmoved until compilation finishes.
    let provider = unsafe { &mut *ctx.cast::<RawExplainProvider>() };
    provider.resolve_next(event)
}

impl RawExplainProvider {
    /// Build hook payloads for extra RAW EXPLAIN details.
    ///
    /// Returns `None` when there are no details to add, so callers can use the
    /// normal statement compilation path without installing a provider.
    pub(crate) fn new<I, S>(details: I) -> Result<Option<Box<Self>>, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let hooks = details
            .into_iter()
            .map(|detail| {
                let detail = CString::new(detail.as_ref()).map_err(|_| {
                    "SQL explain hook detail contains an embedded NUL byte".to_string()
                })?;
                Ok(RawExplainHook {
                    hook: sql_explain_hook {
                        run: Some(run_sql_explain_hook),
                        ctx: detail.as_ptr().cast_mut().cast::<c_void>(),
                    },
                    _detail: detail,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        if hooks.is_empty() {
            return Ok(None);
        }

        let mut provider = Box::new(Self {
            provider: sql_raw_explain_provider {
                resolve: Some(resolve_raw_explain_hook),
                ctx: ptr::null_mut(),
            },
            hooks,
            next: 0,
            unexpected_events: 0,
        });
        provider.provider.ctx = (&mut *provider as *mut Self).cast::<c_void>();
        Ok(Some(provider))
    }

    /// Compile SQL with this provider installed in Tarantool's parse context.
    ///
    /// The provider must be present during compilation because Tarantool can
    /// only append `OP_Explain` safely while it is still building the VDBE.
    pub(crate) fn compile(&mut self, sql: &str) -> Result<SqlStmt, SqlError> {
        let mut stmt: *mut sql_stmt = ptr::null_mut();
        // SAFETY: `sql` points to `sql.len()` bytes of valid SQL text,
        // `self.provider` stays alive for the whole call, and `stmt` is a valid
        // out pointer for Tarantool to fill.
        let rc = unsafe {
            sql_stmt_compile_raw_explain(
                sql.as_ptr().cast::<c_char>(),
                sql.len() as c_int,
                &self.provider,
                &mut stmt,
            )
        };
        if rc != 0 {
            return Err(SqlError::FailedToCompileStmt(TarantoolError::last()));
        }

        assert!(!stmt.is_null());
        // SAFETY: successful compilation returns a non-null prepared statement
        // pointer owned by the caller.
        Ok(unsafe { SqlStmt::from_raw(stmt.cast::<c_void>()) })
    }

    /// Verify that Tarantool consumed exactly the hooks Picodata prepared.
    ///
    /// A mismatch means Picodata and Tarantool disagree about the VDBE
    /// construction events for this RAW EXPLAIN statement.
    pub(crate) fn finish(&self) -> Result<(), String> {
        if self.unexpected_events != 0 {
            return Err(format!(
                "RAW EXPLAIN provider received {} unexpected VDBE events",
                self.unexpected_events
            ));
        }
        if self.next != self.hooks.len() {
            return Err(format!(
                "expected {} RAW EXPLAIN hooks to be consumed, got {}",
                self.hooks.len(),
                self.next
            ));
        }
        Ok(())
    }

    /// Return the next hook for a Tarantool VDBE construction event.
    ///
    /// Currently Picodata only expects events for transactional block
    /// `OP_IdxInsert` rows, so any other event is treated as a mismatch.
    fn resolve_next(&mut self, event: sql_raw_explain_event) -> *mut sql_explain_hook {
        if event != SQL_RAW_EXPLAIN_IDX_INSERT {
            self.unexpected_events += 1;
            return ptr::null_mut();
        }
        let Some(hook) = self.hooks.get_mut(self.next) else {
            self.unexpected_events += 1;
            return ptr::null_mut();
        };
        self.next += 1;
        &mut hook.hook
    }
}
