use crate::sql::lua::{
    dispatch_dump_lua, execute_dml_dump_lua, execute_dql_dump_lua, execute_miss_dump_lua,
};
use ::sql_protocol::encode::{
    dispatch_write_dml_response, dispatch_write_dql_response, dispatch_write_explain_response,
    execute_write_dml_response, execute_write_dql_response, execute_write_miss_response,
};
use ::tarantool::ffi::sql::{obuf_append, Obuf, Port, PortC, PortVTable};
use rmp::decode::read_int;
use std::io::{Cursor, Error as IoError, Result as IoResult, Write};
use std::os::raw::c_int;
use std::ptr::NonNull;

pub static DISPATCH_DML_VTAB: PortVTable = PortVTable::new(dispatch_dml_dump_mp, dispatch_dump_lua);
pub static DISPATCH_DQL_VTAB: PortVTable = PortVTable::new(dispatch_dql_dump_mp, dispatch_dump_lua);
pub static DISPATCH_EXPLAIN_VTAB: PortVTable =
    PortVTable::new(dispatch_explain_dump_mp, dispatch_dump_lua);

unsafe extern "C" fn dispatch_dql_dump_mp(port: *mut Port, out: *mut Obuf) -> c_int {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();

    // The first msgpack in DQL response is metadata, the remaining ones are tuples.
    dispatch_write_dql_response(
        &mut ObufWriter(out),
        port_c.size().saturating_sub(1) as u32,
        port_c.iter(),
    )
    .expect("Failed to dump DQL to obuf on router");
    port_c.size()
}

unsafe extern "C" fn dispatch_explain_dump_mp(port: *mut Port, out: *mut Obuf) -> c_int {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    dispatch_write_explain_response(&mut ObufWriter(out), port_c.iter())
        .expect("Failed to dump EXPLAIN to obuf on router");
    port_c.size()
}

unsafe extern "C" fn dispatch_dml_dump_mp(port: *mut Port, out: *mut Obuf) -> c_int {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    dispatch_write_dml_response(&mut ObufWriter(out), port_c.iter())
        .expect("Failed to dump DML to obuf on router");
    port_c.size()
}

pub static EXECUTE_DQL_VTAB: PortVTable =
    PortVTable::new(execute_dql_dump_mp, execute_dql_dump_lua);
pub static EXECUTE_DML_VTAB: PortVTable =
    PortVTable::new(execute_dml_dump_mp, execute_dml_dump_lua);
pub static EXECUTE_MISS_VTAB: PortVTable =
    PortVTable::new(execute_miss_dump_mp, execute_miss_dump_lua);

unsafe extern "C" fn execute_dql_dump_mp(port: *mut Port, out: *mut Obuf) -> c_int {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    execute_write_dql_response(&mut ObufWriter(out), port_c.size() as u32, port_c.iter())
        .expect("Failed to dump DQL to obuf on storage");
    port_c.size()
}

unsafe extern "C" fn execute_dml_dump_mp(port: *mut Port, out: *mut Obuf) -> c_int {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    let first_mp = port_c.iter().next().unwrap_or(b"\xcc\x00");
    let mut cur = Cursor::new(first_mp);
    let changed: u64 = read_int(&mut cur).expect("Failed to read DML response");
    execute_write_dml_response(&mut ObufWriter(out), changed)
        .expect("Failed to dump DML to obuf on storage");
    port_c.size()
}

unsafe extern "C" fn execute_miss_dump_mp(port: *mut Port, out: *mut Obuf) -> c_int {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    execute_write_miss_response(&mut ObufWriter(out))
        .expect("Failed to dump MISS to obuf on storage");
    port_c.size()
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
