use crate::{
    backend::sql::space::ADMIN_ID,
    ir::value::{EncodedValue, Value},
};
use std::{
    borrow::Cow,
    ffi::{c_char, c_int, c_void, CStr},
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
}

type SqlResult<T> = Result<T, SqlError>;

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

/// Compiled SQL statement from tarantool (struct Vdbe).
#[derive(Debug)]
pub struct SqlStmt {
    ptr: NonNull<c_void>,
    // Estimated statement size returned by `sql_stmt_est_size()` when it was created.
    // Storing this value in the statement itself ensures that we will get the same value when
    // inserting/removing it from the statement cache and avoid extra computation.
    size_estimate: usize,
}

impl SqlStmt {
    /// Compile SQL query into an SQL statement.
    pub fn compile(sql: &str) -> SqlResult<Self> {
        let mut stmt: *mut c_void = ptr::null_mut();
        // SAFETY: Safe as all arguments are valid.
        let rc = unsafe {
            sql_stmt_compile_wrapper(sql.as_ptr() as _, sql.len() as _, &mut stmt as *mut _)
        };
        if rc != 0 {
            return Err(SqlError::FailedToCompileStmt(TarantoolError::last()));
        }

        assert!(!stmt.is_null());

        // Loops over VDBE opcodes to accumulate their sizes; never fails.
        // SAFETY: Safe as stmt is asserted to to have not-null value.
        let size = unsafe { sql_stmt_est_size(stmt) };

        Ok(Self {
            ptr: NonNull::new(stmt).unwrap(),
            size_estimate: size,
        })
    }

    /// Execute statement with given parameters into a port.
    pub fn execute(
        &mut self,
        params: &[Value],
        opcode_max: u64,
        port: &mut PortC,
    ) -> SqlResult<ExecutionInsight> {
        if self.is_busy() {
            // Compile and execute a new statement that is not busy.
            let mut stmt = Self::compile(self.as_str())?;
            debug_assert!(!stmt.is_busy());
            stmt.execute(params, opcode_max, port)?;
            return Ok(ExecutionInsight::BusyStmt);
        }

        let is_version_stale = !self.is_schema_version_valid();
        if is_version_stale {
            // Recompile this statement with current version.
            *self = Self::compile(self.as_str())?;
            debug_assert!(self.is_schema_version_valid());
        }

        let params: Vec<_> = params.iter().map(EncodedValue::from).collect();
        with_su(ADMIN_ID, || self.execute_raw(&params, opcode_max, port))
            .expect("must be able to su into admin")?;

        match is_version_stale {
            true => Ok(ExecutionInsight::StaleStmt),
            false => Ok(ExecutionInsight::Nothing),
        }
    }

    fn execute_raw(
        &mut self,
        bind_params: &[EncodedValue],
        sql_vdbe_opcode_max: u64,
        port: &mut PortC,
    ) -> SqlResult<()> {
        let param_data =
            Cow::from(rmp_serde::to_vec(bind_params).map_err(SqlError::FailedToEncodeStmtParams)?);
        debug_assert!(
            tarantool::msgpack::skip_value(&mut std::io::Cursor::new(&param_data)).is_ok()
        );
        // SAFETY: Safe as all arguments are valid.
        let execute_result = unsafe {
            sql_stmt_execute_into_port_ext(
                self.ptr.as_ptr(),
                param_data.as_ptr(),
                sql_vdbe_opcode_max,
                port.as_mut_ptr(),
            )
        };

        if execute_result < 0 {
            return Err(SqlError::FailedToExecuteStmt(TarantoolError::last()));
        }
        Ok(())
    }

    /// Check if statement is currently executed by another thread.
    fn is_busy(&self) -> bool {
        // SAFETY: Safe as it is asserted that ptr is not null in `Self::new`.
        unsafe { sql_stmt_busy(self.ptr.as_ptr()) }
    }

    /// Check if statement schema version is valid.
    fn is_schema_version_valid(&self) -> bool {
        // SAFETY: Safe as it is asserted that ptr is not null in `Self::new`.
        unsafe { sql_stmt_schema_version_is_valid(self.ptr.as_ptr()) }
    }

    /// Get the query string used for compiling this statement.
    pub fn as_str(&self) -> &str {
        // SAFETY: Safe as it is asserted that ptr is not null in `Self::new`.
        let query = unsafe { sql_stmt_query_str(self.ptr.as_ptr()) };
        // SAFETY: Safe as statements stores valid query string.
        let cstr = unsafe { CStr::from_ptr(query) };
        cstr.to_str().expect("bad query encoding")
    }

    /// Get estimated amount of memory occupied by this statement.
    pub fn estimated_size(&self) -> usize {
        self.size_estimate
    }
}

impl Drop for SqlStmt {
    fn drop(&mut self) {
        assert!(
            !self.is_busy(),
            "mustn't drop statement that is being executed"
        );

        // SAFETY: safe as it is asserted that ptr is not null in Self::new
        let rc = unsafe { sql_stmt_finalize(self.ptr.as_ptr()) };
        if rc != 0 {
            match TarantoolError::maybe_last() {
                Ok(_) => tarantool::say_warn!("sql_stmt_finalize failed"),
                Err(err) => tarantool::say_warn!("sql_stmt_finalize failed: {}", err),
            }
        }
    }
}
