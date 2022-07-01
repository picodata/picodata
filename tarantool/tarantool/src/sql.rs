#![cfg(any(feature = "picodata", doc))]

use std::collections::HashMap;
use std::io::Read;
use std::ffi::CStr;
use std::str;
use std::os::raw::c_char;
use serde::de::DeserializeOwned;
use crate::error::TarantoolError;
use crate::ffi;
use crate::ffi::sql::{Bind, ObufWrapper, Port, PortSql, SqlStatement};
use crate::tuple::AsTuple;

/// Create new SQL prepared statement.
/// query - SQL query.
pub fn prepare(query: &str) -> crate::Result<Statement> {
    let port = Port::zeroed();

    if unsafe {
        ffi::sql::sql_prepare(query.as_ptr() as *const c_char, query.len() as u32, &port as *const Port)
    } < 0 {
        return Err(TarantoolError::last().into());
    }

    let sql_port = &port as *const Port as *const PortSql;
    let stmt = unsafe { (*sql_port).sql_stmt };
    let stmt_id = unsafe {
        ffi::sql::sql_stmt_calculate_id(query.as_ptr() as *const c_char, query.len())
    };

    Ok(Statement {
        inner: stmt,
        id: stmt_id,
    })
}

/// SQL prepared statement.
pub struct Statement {
    inner: *const SqlStatement,
    id: u32,
}

impl Statement {
    /// Returns original query.
    pub fn source(&self) -> Result<&str, std::str::Utf8Error> {
        unsafe {
            let query = ffi::sql::sql_stmt_query_str(self.inner);
            CStr::from_ptr(query)
        }.to_str()
    }

    /// Executes prepared statement and returns a wrapper over the raw msgpack bytes.
    pub fn execute_raw<IN>(&self, bind_params: &IN) -> crate::Result<impl Read>
    where
        IN: AsTuple,
    {
        let mut port = Port::zeroed();

        let execute_result = if std::mem::size_of::<IN>() != 0 {
            let params = rmp_serde::to_vec_named(bind_params)?;
            let mut bind_ptr: *const Bind = unsafe { std::mem::zeroed() };
            let bind_cnt = unsafe { ffi::sql::sql_bind_list_decode(params.as_ptr() as *const c_char, &mut bind_ptr as *mut *const Bind) };
            if bind_cnt < 0 {
                return Err(TarantoolError::last().into());
            }

            unsafe { ffi::sql::sql_execute_prepared_ext(self.id, bind_ptr as *const Bind, bind_cnt as u32, &port as *const Port) }
        } else {
            unsafe { ffi::sql::sql_execute_prepared_ext(self.id, std::ptr::null::<Bind>() as *const Bind, 0, &port as *const Port) }
        };
        if execute_result < 0 {
            // Tarantool has already called `port_destroy()` and has possibly
            // trashed `vtab` pointer. We need to reset it to avoid UB.
            port.vtab = std::ptr::null();
            return Err(TarantoolError::last().into());
        }

        let buf = ObufWrapper::new(1024);

        unsafe { ((*port.vtab).dump_msgpack)(&port as *const Port, buf.obuf()); };
        Ok(buf)
    }


    /// Executes a *returning data* prepared statement with binding variables.
    ///
    /// Example:
    /// ```no_run
    /// #[cfg(feature = "picodata")]
    /// {
    ///     use tarantool::sql;
    ///
    ///     let stmt = sql::prepare("SELECT * FROM S WHERE ID > ?").unwrap();
    ///     let result: Vec<(u8, String)> = stmt.execute(&(100,)).unwrap();
    ///     println!("SQL query result: {:?}", result);
    /// }
    /// ```
    pub fn execute<IN, OUT>(&self, bind_params: &IN) -> crate::Result<OUT>
        where IN: AsTuple,
              OUT: DeserializeOwned
    {
        let buf = self.execute_raw(bind_params)?;
        let mut map = rmp_serde::decode::from_read::<_, HashMap<u32, rmpv::Value>>(buf)?;
        let data = map.remove(&ffi::sql::IPROTO_DATA)
            .ok_or_else(|| rmp_serde::decode::Error::Syntax("Invalid execution result format".to_string()))?;
        let values = rmpv::ext::from_value::<OUT>(data)
            .map_err(|e| rmp_serde::decode::Error::Syntax(e.to_string()))?;

        Ok(values)
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        unsafe {
            if ffi::sql::sql_unprepare(self.id) >= 0 && ffi::sql::sql_stmt_finalize(self.inner) < 0 {
                panic!("{}", TarantoolError::last())
            }
        }
    }
}
