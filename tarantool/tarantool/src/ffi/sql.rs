#![cfg(any(feature = "picodata", doc))]

use std::cmp;
use std::io::Read;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::NonNull;
use libc::{iovec, size_t};
use tlua::LuaState;
use crate::ffi::tarantool::BoxTuple;

pub(crate) const IPROTO_DATA: u32 = 0x30;

// sql
extern "C" {
    pub(crate) fn port_destroy(port: *const Port);
    fn cord_slab_cache() -> *const SlabCache;

    fn obuf_create(obuf: *mut Obuf, slab_cache: *const SlabCache, start_cap: size_t);
    fn obuf_destroy(obuf: *mut Obuf);

    pub(crate) fn sql_prepare(sql: *const c_char, len: u32, port: *const Port) -> c_int;
    pub(crate) fn sql_execute_prepared_ext(stmt_id: u32, bind: *const Bind, bind_count: u32, port: *const Port) -> c_int;
    pub(crate) fn sql_unprepare(stmt_id: u32) -> c_int;
    pub(crate) fn sql_stmt_query_str(stmt: *const SqlStatement) -> *const c_char;
    pub(crate) fn sql_stmt_calculate_id(sql_str: *const c_char, len: size_t) -> u32;
    pub(crate) fn sql_bind_list_decode(data: *const c_char, bind: *mut *const Bind) -> c_int;
}

#[repr(C)]
struct SlabCache {
    _unused: [u8; 0],
}

#[repr(C)]
pub(crate) struct SqlStatement {
    _unused: [u8; 0],
}

#[repr(C)]
pub(crate) struct Bind {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct Port {
    pub vtab: *const VTable,
    pub _unused: [u8; 68],
}

impl Port {
    pub(crate) fn zeroed() -> Self {
        unsafe {
            Self { vtab: std::ptr::null(), _unused: std::mem::zeroed() }
        }
    }
}

impl Drop for Port {
    fn drop(&mut self) {
        if !self.vtab.is_null() {
            unsafe { port_destroy(self as *const Port) }
        }
    }
}

#[repr(C)]
pub(crate) struct PortSql {
    _base: PortC,
    pub sql_stmt: *const SqlStatement,
    _serialization_format: u8,
    _do_finalize: bool,
}

#[repr(C)]
struct PortC {
    pub vtab: *const VTable,
    pub _first: *const PortCEntry,
    pub _last: *const PortCEntry,
    pub _first_entry: PortCEntry,
    pub _size: u32,
}

#[repr(C)]
struct PortCEntry {
    _next: *const PortCEntry,
    _data: U,
    _mp_sz: u32,
    _tuple_format: *const c_void,
}

#[repr(C)]
union U {
    tuple: NonNull<BoxTuple>,
    mp: *const c_char,
}

pub(crate) struct ObufWrapper {
    pub inner: Obuf,
    read_pos: usize,
    read_iov_n: usize,
    read_iov_pos: usize,
}

impl ObufWrapper {
    pub fn new(start_capacity: usize) -> Self {
        let inner_buf = unsafe {
            let slab_c = cord_slab_cache();

            let mut buf = Obuf {
                _slab_cache: std::mem::zeroed(),
                pos: 0,
                n_iov: 0,
                used: 0,
                start_capacity: start_capacity as size_t,
                capacity: std::mem::zeroed(),
                iov: std::mem::zeroed(),
            };
            obuf_create(&mut buf as *mut Obuf, slab_c, 1024);
            buf
        };
        Self {
            inner: inner_buf,
            read_pos: 0,
            read_iov_n: 0,
            read_iov_pos: 0,
        }
    }

    pub(crate) fn obuf(&self) -> *const Obuf {
        &self.inner as *const Obuf
    }
}

impl Read for ObufWrapper {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut remains_read = cmp::min(buf.len(), self.inner.used as usize - self.read_pos);
        let mut buf_pos = 0;

        while remains_read > 0 {
            let iov_available_len = self.inner.iov[self.read_iov_n].iov_len - self.read_iov_pos;
            if iov_available_len == 0 {
                self.read_iov_n += 1;
                self.read_iov_pos = 0;
                continue;
            }

            let read_len = if iov_available_len <= remains_read {
                iov_available_len
            } else {
                remains_read
            };

            let cp = unsafe {
                std::slice::from_raw_parts(
                    (self.inner.iov[self.read_iov_n].iov_base as *const u8).add(self.read_iov_pos),
                    read_len,
                )
            };

            buf[buf_pos..buf_pos + read_len].copy_from_slice(cp);

            buf_pos += read_len;
            remains_read -= read_len;
            self.read_iov_pos += read_len;
        }

        self.read_pos += buf_pos;
        Ok(buf_pos)
    }
}

#[repr(C)]
pub(crate) struct Obuf {
    _slab_cache: *const c_void,
    pub pos: i32,
    pub n_iov: i32,
    pub used: size_t,
    pub start_capacity: size_t,
    pub capacity: [size_t; 32],
    pub iov: [iovec; 32],
}

impl Drop for Obuf {
    fn drop(&mut self) {
        unsafe { obuf_destroy(self as *mut Obuf) }
    }
}

#[repr(C)]
pub(crate) struct VTable {
    pub(crate) dump_msgpack: unsafe extern "C" fn(port: *const Port, out: *const Obuf),
    _dump_msgpack_16: unsafe extern "C" fn(port: *const Port, out: *const Obuf),
    _dump_lua: unsafe extern "C" fn(port: *const Port, l: *const LuaState, is_flat: bool),
    _dump_plain: unsafe extern "C" fn(port: *const Port, size: *const u32) -> *const c_char,
    _get_vdbemem: unsafe extern "C" fn(port: *const Port, size: *const u32) -> *const c_void,
    _destroy: unsafe extern "C" fn(port: *const Port),
}
