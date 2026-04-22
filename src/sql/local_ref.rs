use crate::schema::ADMIN_ID;
use crate::sql::lua::{dispatch_session_id, reference_add, reference_del, reference_use};
use crate::traft::node;
use scopeguard::ScopeGuard;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use sql::errors::SbroadError;
use std::cell::{Cell, OnceCell};
use std::time::Duration;
use tarantool::session::with_su;

pub(crate) type SqlResult<T> = Result<T, SbroadError>;

thread_local! {
    /// Session UUID shared with the Lua dispatch path (pico.dispatch.session_id).
    /// All local fast-path requests reuse this single vshard lref session so that
    /// vshard does not accumulate a dead session per request in its session_map.
    /// Initialized lazily on first use of the local fast-path.
    static LOCAL_LREF_SESSION_ID: OnceCell<String> = const { OnceCell::new() };

    /// Ref ID counter for vshard lref. Must be unique within LOCAL_LREF_SESSION_ID.
    /// Stored per-fiber in Tarantool's TX thread -- no atomics needed.
    static LOCAL_LREF_RID: Cell<i64> = const { Cell::new(1) };
}

/// Returns the vshard lref session UUID, shared with the Lua dispatch path.
/// Calls pico.dispatch.session_id() once and caches the result.
fn local_lref_session_id() -> SqlResult<SmolStr> {
    LOCAL_LREF_SESSION_ID.with(|cell| {
        if let Some(s) = cell.get() {
            return Ok(s.as_str().into());
        }
        let id = dispatch_session_id().map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;
        let _ = cell.set(id);
        Ok(cell.get().unwrap().as_str().into())
    })
}

pub(crate) fn with_admin_su<T>(op: &str, f: impl FnOnce() -> tarantool::Result<T>) -> SqlResult<T> {
    let result = with_su(ADMIN_ID, f).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!("failed to switch user for {op}: {e}"))
    })?;
    result.map_err(|e| SbroadError::DispatchError(format_smolstr!("failed to {op}: {e}")))
}

fn preserve_primary_result<T>(
    result: SqlResult<T>,
    cleanup_result: SqlResult<()>,
    cleanup_context: &str,
) -> SqlResult<T> {
    if let Err(cleanup_err) = cleanup_result {
        crate::tlog!(
            Warning,
            "failed to remove local bucket reference {cleanup_context}: {cleanup_err}"
        );
    }

    result
}

pub(crate) fn with_local_bucket_ref<T>(
    timeout: Duration,
    read_preference: &str,
    f: impl FnOnce() -> SqlResult<T>,
) -> SqlResult<T> {
    let node = node::global().map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;
    if node.is_readonly() {
        return f();
    }

    debug_assert_ne!(read_preference, "replica");

    let sid = local_lref_session_id()?;
    let sid = sid.as_str();
    let rid = LOCAL_LREF_RID.with(|c| {
        let v = c.get();
        c.set(v + 1);
        v
    });
    with_admin_su("add local bucket reference", || {
        reference_add(rid, sid, timeout)
    })?;

    if let Err(error) = with_admin_su("use local bucket reference", || reference_use(rid, sid)) {
        return preserve_primary_result(
            Err(error),
            with_admin_su("remove local bucket reference", || reference_del(rid, sid)),
            "after failed use",
        );
    }

    let rollback_sid = sid.to_owned();
    let cleanup_on_unwind = scopeguard::guard((rid, rollback_sid), |(rid, sid)| {
        let _ = preserve_primary_result::<()>(
            Ok(()),
            with_admin_su("remove local bucket reference", || {
                reference_del(rid, sid.as_str())
            }),
            "during unwind",
        );
    });

    let result = f();
    let result = preserve_primary_result(
        result,
        with_admin_su("remove local bucket reference", || reference_del(rid, sid)),
        "after local request",
    );
    ScopeGuard::into_inner(cleanup_on_unwind);
    result
}
