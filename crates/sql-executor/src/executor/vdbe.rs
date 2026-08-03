use crate::{
    backend::sql::ADMIN_ID,
    errors::SbroadError,
    executor::engine::{BlockQuery, VersionMap},
    ir::node::BlockStatement,
    ir::value::{EncodedValue, Value},
};
use smol_str::ToSmolStr;
use std::{
    borrow::Cow,
    ffi::{c_char, c_int, c_void, CStr},
    fmt,
    mem::ManuallyDrop,
    ptr::{self, NonNull},
};
use tarantool::ffi::sql::PortC;
use tarantool::{error::TarantoolError, session::with_su};
use thiserror::Error;

extern "C" {
    fn sql_stmt_compile_wrapper(
        sql: *const c_char,
        bytes_count: c_int,
        stmt: *mut *mut c_void,
    ) -> c_int;

    fn sql_stmt_finalize(stmt: *mut c_void) -> c_int;

    fn sql_stmt_execute_into_port_ext(
        stmt: *mut c_void,
        mp_params: *const u8,
        vdbe_max_steps: u64,
        port: *mut tarantool::ffi::sql::Port,
    ) -> c_int;

    fn sql_stmt_busy(stmt: *mut c_void) -> bool;

    fn sql_stmt_schema_version_is_valid(stmt: *mut c_void) -> bool;

    fn sql_stmt_query_str(stmt: *const c_void) -> *const c_char;

    fn sql_stmt_est_size(stmt: *const c_void) -> usize;
}

#[derive(Error, Debug)]
pub enum SqlError {
    #[error("Failed to compile SQL statement: {}", .0.message())]
    FailedToCompileStmt(TarantoolError),
    // Tarantool execution error already has "Failed to execute SQL statement" prefix.
    #[error("{}", .0.message())]
    FailedToExecuteStmt(TarantoolError),
    #[error("Failed to encode SQL statement parameters: {0}")]
    FailedToEncodeStmtParams(rmp_serde::encode::Error),
    #[error("Failed to reassemble VDBE program: {0}")]
    FailedToReassembleProgram(String),
    #[error("Outdated storage schema")]
    OutdatedStorageSchema,
}

impl From<SqlError> for SbroadError {
    fn from(value: SqlError) -> Self {
        match value {
            SqlError::OutdatedStorageSchema => Self::OutdatedStorageSchema,
            value => Self::VdbeError(value.to_smolstr()),
        }
    }
}

type SqlResult<T> = Result<T, SqlError>;

/// Opaque payload owned by a prepared VDBE statement.
///
/// Payloads are kept alive until statement finalization because VDBE opcodes
/// can store raw pointers into them. `before_execute` lets a payload reset
/// per-run state while the caller holds exclusive access to the statement.
pub trait VdbeOwnedPayload {
    fn before_execute(&mut self) {}

    /// Release per-run memory right after execution instead of waiting for the
    /// next cached statement call; IOC DU uses this to drop `seen_keys`.
    fn after_execute(&mut self) {}
}

impl From<String> for SqlError {
    fn from(value: String) -> Self {
        Self::FailedToReassembleProgram(value)
    }
}

impl From<&str> for SqlError {
    fn from(value: &str) -> Self {
        Self::FailedToReassembleProgram(value.into())
    }
}

/// Execution result gives some insights into execution process.
/// The only purpose of it is performance troubleshooting and metrics collecting, if the user
/// doesn't care about it this result can be safely ignored.
#[derive(Debug, Clone, Copy)]
pub enum ExecutionInsight {
    /// Statement was executed without any special care.
    Nothing,
    /// Statement was executed by another fiber so a new statement was compiled and executed.
    BusyStmt,
    /// Statement had invalid schema version so it was recompiled before execution.
    StaleStmt,
}

/// Owned tarantool VDBE pointer (struct Vdbe) with its cached size estimate.
/// The recompilation strategy lives in [`SqlStmt`], which wraps this. Public
/// only because it appears in the public [`SqlStmt`] enum; its fields are
/// private, so it stays opaque.
pub struct RawStmt {
    ptr: NonNull<c_void>,
    // Estimated statement size returned by `sql_stmt_est_size()` when it was created.
    // Storing this value in the statement itself ensures that we will get the same value when
    // inserting/removing it from the statement cache and avoid extra computation.
    size_estimate: usize,
    // Opaque payloads that must live until the very end of statement drop.
    owned_payloads: ManuallyDrop<Vec<Box<dyn VdbeOwnedPayload>>>,
}

impl fmt::Debug for RawStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawStmt")
            .field("ptr", &self.ptr)
            .field("size_estimate", &self.size_estimate)
            .field("owned_payloads_len", &self.owned_payloads.len())
            .finish()
    }
}

impl RawStmt {
    /// Compile SQL query into an SQL statement.
    fn compile(sql: &str) -> SqlResult<Self> {
        let mut stmt: *mut c_void = ptr::null_mut();
        // SAFETY: Safe as all arguments are valid.
        let rc = unsafe {
            sql_stmt_compile_wrapper(sql.as_ptr() as _, sql.len() as _, &mut stmt as *mut _)
        };
        if rc != 0 {
            return Err(SqlError::FailedToCompileStmt(TarantoolError::last()));
        }

        assert!(!stmt.is_null());

        // SAFETY: stmt is asserted non-null above.
        Ok(unsafe { Self::wrap_stmt_ptr(stmt) })
    }

    /// # Safety
    /// `ptr` must be a non-null, owned VDBE statement pointer.
    unsafe fn wrap_stmt_ptr(ptr: *mut c_void) -> Self {
        // Loops over VDBE opcodes to accumulate their sizes; never fails.
        let size = sql_stmt_est_size(ptr);
        Self {
            ptr: NonNull::new(ptr).expect("non-null vdbe ptr"),
            size_estimate: size,
            owned_payloads: ManuallyDrop::new(Vec::new()),
        }
    }

    /// Transfer ownership of an opaque payload to this statement.
    ///
    /// The payload is not used by `RawStmt` directly. It is stored so raw pointers
    /// embedded into the underlying VDBE remain valid until statement finalization
    /// completes. Payloads are dropped at the very end of `RawStmt::drop`.
    fn add_owned_payload(&mut self, payload: Box<dyn VdbeOwnedPayload>) {
        self.owned_payloads.push(payload);
    }

    fn execute_raw(
        &mut self,
        param_data: &[u8],
        sql_vdbe_opcode_max: u64,
        port: &mut PortC,
    ) -> SqlResult<()> {
        debug_assert!(
            tarantool::msgpack::skip_value(&mut std::io::Cursor::new(&param_data)).is_ok()
        );
        for payload in self.owned_payloads.iter_mut() {
            payload.before_execute();
        }
        // SAFETY: Safe as all arguments are valid.
        let execute_result = unsafe {
            sql_stmt_execute_into_port_ext(
                self.ptr.as_ptr(),
                param_data.as_ptr(),
                sql_vdbe_opcode_max,
                port.as_mut_ptr(),
            )
        };

        let execute_error = if execute_result < 0 {
            Some(TarantoolError::last())
        } else {
            None
        };
        for payload in self.owned_payloads.iter_mut() {
            payload.after_execute();
        }
        if let Some(error) = execute_error {
            return Err(SqlError::FailedToExecuteStmt(error));
        }
        Ok(())
    }

    /// Check if statement is currently executed by another thread.
    fn is_busy(&self) -> bool {
        // SAFETY: Safe as it is asserted that ptr is not null in `wrap_stmt_ptr`.
        unsafe { sql_stmt_busy(self.ptr.as_ptr()) }
    }

    /// Check if statement schema version is valid.
    fn is_schema_version_valid(&self) -> bool {
        // SAFETY: Safe as it is asserted that ptr is not null in `wrap_stmt_ptr`.
        unsafe { sql_stmt_schema_version_is_valid(self.ptr.as_ptr()) }
    }

    /// Get the query string used for compiling this statement.
    fn as_str(&self) -> &str {
        // SAFETY: Safe as it is asserted that ptr is not null in `wrap_stmt_ptr`.
        let query = unsafe { sql_stmt_query_str(self.ptr.as_ptr()) };
        // SAFETY: Safe as statements stores valid query string.
        let cstr = unsafe { CStr::from_ptr(query) };
        cstr.to_str().expect("bad query encoding")
    }
}

impl Drop for RawStmt {
    fn drop(&mut self) {
        assert!(
            !self.is_busy(),
            "mustn't drop statement that is being executed"
        );

        // SAFETY: safe as it is asserted that ptr is not null in wrap_stmt_ptr
        let rc = unsafe { sql_stmt_finalize(self.ptr.as_ptr()) };
        if rc != 0 {
            match TarantoolError::maybe_last() {
                Ok(_) => tarantool::say_warn!("sql_stmt_finalize failed"),
                Err(err) => tarantool::say_warn!("sql_stmt_finalize failed: {}", err),
            }
        }

        // Keep payloads alive through `sql_stmt_finalize`: finalization may
        // call VDBE hooks whose state is owned by `owned_payloads`.
        unsafe { ManuallyDrop::drop(&mut self.owned_payloads) };
    }
}

/// Re-assembles a block VDBE from its rendered per-statement patterns. Block
/// assembly needs the VDBE FFI, which lives in the picodata crate, so a
/// transactional statement carries a pointer to it set at construction.
type RecompileBlockFn = fn(&[BlockStatement<BlockQuery>], &VersionMap) -> SqlResult<SqlStmt>;

/// An assembled block VDBE plus what it needs to rebuild itself when its schema
/// version goes stale: the rendered patterns and the assembly routine.
#[derive(Debug)]
pub struct TxnStmt {
    raw: RawStmt,
    statements: Vec<BlockStatement<BlockQuery>>,
    table_versions: VersionMap,
    recompile: RecompileBlockFn,
}

/// Compiled SQL statement from tarantool (struct Vdbe). A regular statement was
/// compiled from a SQL source string and recompiles from it; a transactional
/// statement is an assembled block program that recompiles from its patterns.
#[derive(Debug)]
pub enum SqlStmt {
    Regular(RawStmt),
    Transactional(TxnStmt),
}

impl SqlStmt {
    /// Compile SQL query into an SQL statement.
    pub fn compile(sql: &str) -> SqlResult<Self> {
        Ok(Self::Regular(RawStmt::compile(sql)?))
    }

    /// Wrap an externally-compiled regular VDBE pointer.
    ///
    /// Takes ownership: the statement will be finalized on drop.
    ///
    /// # Safety
    /// `ptr` must be a non-null, owned VDBE that has been made ready (e.g.
    /// via `sqlVdbeMakeReady`) and is not currently executing.
    pub unsafe fn from_raw(ptr: *mut c_void) -> Self {
        Self::Regular(RawStmt::wrap_stmt_ptr(ptr))
    }

    /// Wrap an externally-assembled block VDBE pointer together with the
    /// patterns and assembly routine needed to recompile it.
    ///
    /// Takes ownership: the statement will be finalized on drop.
    ///
    /// # Safety
    /// `ptr` must be a non-null, owned VDBE that has been made ready (e.g.
    /// via `sqlVdbeMakeReady`) and is not currently executing.
    pub unsafe fn new_transactional(
        ptr: *mut c_void,
        statements: Vec<BlockStatement<BlockQuery>>,
        table_versions: VersionMap,
        recompile: RecompileBlockFn,
    ) -> Self {
        Self::Transactional(TxnStmt {
            raw: RawStmt::wrap_stmt_ptr(ptr),
            statements,
            table_versions,
            recompile,
        })
    }

    fn raw(&self) -> &RawStmt {
        match self {
            Self::Regular(raw) => raw,
            Self::Transactional(txn) => &txn.raw,
        }
    }

    fn raw_mut(&mut self) -> &mut RawStmt {
        match self {
            Self::Regular(raw) => raw,
            Self::Transactional(txn) => &mut txn.raw,
        }
    }

    /// Transfer ownership of an opaque payload to the underlying VDBE statement.
    ///
    /// The payload is kept alive until statement finalization completes.
    pub fn add_owned_payload(&mut self, payload: Box<dyn VdbeOwnedPayload>) {
        self.raw_mut().add_owned_payload(payload);
    }

    /// Rebuild a fresh statement of the same kind against the current schema
    /// version. A regular statement recompiles from its SQL source; a block
    /// statement re-assembles from its rendered patterns.
    fn recompile(&self) -> SqlResult<Self> {
        match self {
            Self::Regular(raw) => Ok(Self::Regular(RawStmt::compile(raw.as_str())?)),
            Self::Transactional(txn) => (txn.recompile)(&txn.statements, &txn.table_versions),
        }
    }

    /// Execute statement with given parameters into a port, recompiling first
    /// if it is busy or its schema version is stale. Recompilation is
    /// variant-specific (see [`recompile`](Self::recompile)), so an assembled
    /// block VDBE refreshes itself just like a regular statement does.
    pub fn execute<P: AsRef<Value>>(
        &mut self,
        params: &[P],
        opcode_max: u64,
        port: &mut PortC,
    ) -> SqlResult<ExecutionInsight> {
        let encoded: Vec<EncodedValue> = params
            .iter()
            .map(|p| EncodedValue::from(p.as_ref()))
            .collect();
        let param_data =
            Cow::from(rmp_serde::to_vec(&encoded).map_err(SqlError::FailedToEncodeStmtParams)?);
        self.execute_with_raw_params(param_data.iter().as_slice(), opcode_max, port)
    }

    pub fn execute_with_raw_params(
        &mut self,
        params: &[u8],
        opcode_max: u64,
        port: &mut PortC,
    ) -> SqlResult<ExecutionInsight> {
        if self.raw().is_busy() {
            // Compile and execute a new statement that is not busy.
            let mut stmt = self.recompile()?;
            debug_assert!(!stmt.raw().is_busy());
            stmt.execute_with_raw_params(params, opcode_max, port)?;
            return Ok(ExecutionInsight::BusyStmt);
        }

        let is_version_stale = !self.raw().is_schema_version_valid();
        if is_version_stale {
            // Recompile this statement with current version.
            *self = self.recompile()?;
            debug_assert!(self.raw().is_schema_version_valid());
        }

        with_su(ADMIN_ID, || {
            self.raw_mut().execute_raw(params, opcode_max, port)
        })
        .expect("must be able to su into admin")?;

        match is_version_stale {
            true => Ok(ExecutionInsight::StaleStmt),
            false => Ok(ExecutionInsight::Nothing),
        }
    }

    /// Get the query string used for compiling this statement.
    pub fn as_str(&self) -> &str {
        self.raw().as_str()
    }

    /// Get estimated amount of memory occupied by this statement.
    pub fn estimated_size(&self) -> usize {
        self.raw().size_estimate
    }

    /// Raw pointer to the underlying tarantool VDBE statement.
    ///
    /// Returned as `*mut c_void` because the `Vdbe` struct is not visible
    /// to this crate (its bindings live in the picodata crate's bindgen).
    /// Callers that need the typed `*mut Vdbe` cast it themselves.
    pub fn as_ptr(&self) -> *mut c_void {
        self.raw().ptr.as_ptr()
    }
}
