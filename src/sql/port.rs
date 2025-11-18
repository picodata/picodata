use crate::sql::lua::{
    dispatch_dml_dump_lua, dispatch_dql_dump_lua, dispatch_explain_dump_lua, execute_dml_dump_lua,
    execute_dql_dump_lua, execute_miss_dump_lua,
};
use rmp::decode::{read_array_len, read_int};
use rmp::encode::write_bool;
use sql::executor::{Port as SqlPort, PortType as SqlPortType};
use sql_protocol::encode::{
    dispatch_write_dml_response, dispatch_write_dql_response, dispatch_write_explain_response,
    dispatch_write_query_plan_response, execute_write_dml_response, execute_write_dql_response,
    execute_write_miss_response,
};
use std::io::{Cursor, Error as IoError, Result as IoResult, Write};
use std::os::raw::c_int;
use std::ptr::NonNull;
use tarantool::ffi::sql::{obuf_append, Obuf, Port, PortC, PortVTable};

use super::lua::{dispatch_query_plan_dump_lua, escape_bytes};
use sql::executor::vdbe::{ExecutionInsight, SqlError, SqlStmt};
use sql::ir::value::Value;

/// The response of dump_mp callback is used in the IPROTO body
/// to encode the length of the IPTOTO_DATA value:
/// - \x81 - a single element map;
/// - \x30 - IPROTO_DATA;
/// - \xdd - marker for the u32 length array;
/// - IPROTO_DATA_VALUE_LENGTH - the length of the array (1).
///
/// We use the length of 1 because the port is dumped itself
/// as a single element array of msgpack values.
const IPROTO_DATA_VALUE_LENGTH_DISPATCH: c_int = 1;

/// We use the length of 2 as we have to emulate pcall format
/// for vshard on the router while dumping msgpack to obuf.
/// So in addition to the actual payload we add a boolean flag.
/// For example, miss result: [true, {miss: 0}]
const IPROTO_DATA_VALUE_LENGTH_EXECUTE: c_int = 2;

pub static DISPATCH_DML_VTAB: PortVTable =
    PortVTable::new(dispatch_dml_dump_mp, dispatch_dml_dump_lua);
pub static DISPATCH_DQL_VTAB: PortVTable =
    PortVTable::new(dispatch_dql_dump_mp, dispatch_dql_dump_lua);
pub static DISPATCH_EXPLAIN_VTAB: PortVTable =
    PortVTable::new(dispatch_explain_dump_mp, dispatch_explain_dump_lua);
pub static DISPATCH_QUERY_PLAN_VTAB: PortVTable =
    PortVTable::new(dispatch_query_plan_dump_mp, dispatch_query_plan_dump_lua);

pub(crate) fn dispatch_dump_mp(writer: &mut impl Write, port: &PortC) -> IoResult<()> {
    if port.vtab == &DISPATCH_DML_VTAB {
        dispatch_write_dml_response(writer, port.iter())?;
    } else if port.vtab == &DISPATCH_DQL_VTAB {
        // The first msgpack in DQL response is metadata, the remaining ones are tuples.
        dispatch_write_dql_response(writer, port.size().saturating_sub(1) as u32, port.iter())?;
    } else if port.vtab == &DISPATCH_EXPLAIN_VTAB {
        dispatch_write_explain_response(writer, port.size() as u32, port.iter())?;
    } else if port.vtab == &DISPATCH_QUERY_PLAN_VTAB {
        dispatch_write_query_plan_response(writer, port.iter())?;
    } else {
        return Err(IoError::other("Unsupported port vtable for dispatch_dump"));
    }
    Ok(())
}

unsafe extern "C" fn dispatch_dql_dump_mp(port: *mut Port, out: *mut Obuf) -> c_int {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();

    // The first msgpack in DQL response is metadata, the remaining ones are tuples.
    dispatch_write_dql_response(
        &mut ObufWriter(out),
        port_c.size().saturating_sub(1) as u32,
        port_c.iter(),
    )
    .expect("Failed to dump DQL to obuf on router");
    IPROTO_DATA_VALUE_LENGTH_DISPATCH
}

unsafe extern "C" fn dispatch_explain_dump_mp(port: *mut Port, out: *mut Obuf) -> c_int {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    dispatch_write_explain_response(&mut ObufWriter(out), port_c.size() as u32, port_c.iter())
        .expect("Failed to dump EXPLAIN to obuf on router");
    IPROTO_DATA_VALUE_LENGTH_DISPATCH
}

unsafe extern "C" fn dispatch_query_plan_dump_mp(port: *mut Port, out: *mut Obuf) -> c_int {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    dispatch_write_query_plan_response(&mut ObufWriter(out), port_c.iter())
        .expect("Failed to dump QUERY PLAN to obuf on router");
    IPROTO_DATA_VALUE_LENGTH_DISPATCH
}

unsafe extern "C" fn dispatch_dml_dump_mp(port: *mut Port, out: *mut Obuf) -> c_int {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    dispatch_write_dml_response(&mut ObufWriter(out), port_c.iter())
        .expect("Failed to dump DML to obuf on router");
    IPROTO_DATA_VALUE_LENGTH_DISPATCH
}

pub static EXECUTE_DQL_VTAB: PortVTable =
    PortVTable::new(execute_dql_dump_mp, execute_dql_dump_lua);
pub static EXECUTE_DML_VTAB: PortVTable =
    PortVTable::new(execute_dml_dump_mp, execute_dml_dump_lua);
pub static EXECUTE_MISS_VTAB: PortVTable =
    PortVTable::new(execute_miss_dump_mp, execute_miss_dump_lua);

unsafe extern "C" fn execute_dql_dump_mp(port: *mut Port, out: *mut Obuf) -> c_int {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    let mut wr = ObufWriter(out);
    pcall_write_ok(&mut wr).expect("Failed to write pcall prefix for DQL on storage");
    execute_write_dql_response(&mut wr, port_c.size() as u32, port_c.iter())
        .expect("Failed to dump DQL to obuf on storage");
    IPROTO_DATA_VALUE_LENGTH_EXECUTE
}

unsafe extern "C" fn execute_dml_dump_mp(port: *mut Port, out: *mut Obuf) -> c_int {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    let first_mp = port_c.iter().next().unwrap_or(b"\x91\xcc\x00");
    let mut cur = Cursor::new(first_mp);
    let _ = read_array_len(&mut cur).unwrap_or_else(|_| {
        panic!(
            "Failed to decode changed rows, msgpack: {}",
            escape_bytes(first_mp)
        )
    });
    let changed: u64 = read_int(&mut cur).expect("Failed to read DML response");
    let mut wr = ObufWriter(out);
    pcall_write_ok(&mut wr).expect("Failed to write pcall prefix for DML on storage");
    execute_write_dml_response(&mut wr, changed).expect("Failed to dump DML to obuf on storage");
    IPROTO_DATA_VALUE_LENGTH_EXECUTE
}

unsafe extern "C" fn execute_miss_dump_mp(_port: *mut Port, out: *mut Obuf) -> c_int {
    let mut wr = ObufWriter(out);
    // [true, {miss: 0}]
    pcall_write_ok(&mut wr).expect("Failed to write pcall prefix for MISS on storage");
    execute_write_miss_response(&mut wr).expect("Failed to dump MISS to obuf on storage");
    IPROTO_DATA_VALUE_LENGTH_EXECUTE
}

/// Implement pcall format to make vshard work.
#[inline(always)]
fn pcall_write_ok(wr: &mut impl Write) -> IoResult<()> {
    write_bool(wr, true)?;
    Ok(())
}

pub struct PicoPortOwned {
    port: Port,
}

impl Default for PicoPortOwned {
    fn default() -> Self {
        Self::new()
    }
}

impl PicoPortOwned {
    pub fn new() -> Self {
        Self {
            port: Port::new_port_c(),
        }
    }

    #[inline]
    pub fn port_c(&self) -> &PortC {
        unsafe { &*(self.port.as_ptr() as *const PortC) }
    }

    #[inline]
    pub fn port_c_mut(&mut self) -> &mut PortC {
        unsafe { self.port.as_mut_port_c() }
    }
}

impl SqlPort<'_> for PicoPortOwned {
    fn add_mp(&mut self, data: &[u8]) {
        unsafe { self.port.as_mut_port_c().add_mp(data) }
    }

    fn process_stmt(
        &mut self,
        stmt: &mut SqlStmt,
        params: &[Value],
        max_vdbe: u64,
    ) -> Result<ExecutionInsight, SqlError>
    where
        Self: Sized,
    {
        stmt.execute(params, max_vdbe, self.port_c_mut())
    }

    fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.port_c().iter()
    }

    fn set_type(&mut self, port_type: SqlPortType) {
        let p = self.port_c_mut();
        match port_type {
            SqlPortType::DispatchDql => p.vtab = &DISPATCH_DQL_VTAB,
            SqlPortType::DispatchDml => p.vtab = &DISPATCH_DML_VTAB,
            SqlPortType::DispatchExplain => p.vtab = &DISPATCH_EXPLAIN_VTAB,
            SqlPortType::DispatchQueryPlan => p.vtab = &DISPATCH_QUERY_PLAN_VTAB,
            SqlPortType::ExecuteDql => p.vtab = &EXECUTE_DQL_VTAB,
            SqlPortType::ExecuteDml => p.vtab = &EXECUTE_DML_VTAB,
            SqlPortType::ExecuteMiss => p.vtab = &EXECUTE_MISS_VTAB,
        }
    }

    fn size(&self) -> u32 {
        self.port_c().size() as u32
    }
}

impl Write for PicoPortOwned {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.add_mp(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

pub struct PicoPortC<'p> {
    port: &'p mut PortC,
}

impl<'p> From<&'p mut PortC> for PicoPortC<'p> {
    fn from(port: &'p mut PortC) -> Self {
        Self { port }
    }
}

impl<'p> SqlPort<'p> for PicoPortC<'p> {
    fn add_mp(&mut self, data: &[u8]) {
        unsafe { self.port.add_mp(data) };
    }

    fn process_stmt(
        &mut self,
        stmt: &mut SqlStmt,
        params: &[Value],
        max_vdbe: u64,
    ) -> Result<ExecutionInsight, SqlError>
    where
        Self: Sized,
    {
        stmt.execute(params, max_vdbe, self.port)
    }

    fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.port.iter()
    }

    fn set_type(&mut self, port_type: SqlPortType) {
        match port_type {
            SqlPortType::DispatchDql => self.port.vtab = &DISPATCH_DQL_VTAB,
            SqlPortType::DispatchDml => self.port.vtab = &DISPATCH_DML_VTAB,
            SqlPortType::DispatchExplain => self.port.vtab = &DISPATCH_EXPLAIN_VTAB,
            SqlPortType::DispatchQueryPlan => self.port.vtab = &DISPATCH_QUERY_PLAN_VTAB,
            SqlPortType::ExecuteDql => self.port.vtab = &EXECUTE_DQL_VTAB,
            SqlPortType::ExecuteDml => self.port.vtab = &EXECUTE_DML_VTAB,
            SqlPortType::ExecuteMiss => self.port.vtab = &EXECUTE_MISS_VTAB,
        }
    }

    fn size(&self) -> u32 {
        self.port.size() as u32
    }
}

impl Write for PicoPortC<'_> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.add_mp(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

pub struct PortWriter<'p> {
    port: &'p mut PortC,
}

impl<'p> PortWriter<'p> {
    #[inline]
    pub fn new(port: &'p mut PortC) -> Self {
        Self { port }
    }
}

impl Write for PortWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        unsafe { self.port.add_mp(buf) };
        Ok(buf.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

struct ObufWriter(pub *mut Obuf);
impl Write for ObufWriter {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        unsafe {
            obuf_append(self.0, buf).map_err(IoError::other)?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}
