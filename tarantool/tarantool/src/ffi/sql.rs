#![cfg(any(feature = "picodata", doc))]

use libc::{iovec, size_t};
use std::cmp;
use std::io::Read;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::{null, NonNull};
use tlua::ffi::lua_State;

use crate::error::{TarantoolError, TarantoolErrorCode};
use crate::tuple::Tuple;

use super::tarantool::BoxTuple;

pub const IPROTO_DATA: u8 = 0x30;

// Note that all of the functions defined here are either `pub` or `pub(crate)`
// even if they're only used in this file. This is because the `define_dlsym_reloc`
// macro doesn't support private function declarations because rust's macro syntax is trash.
crate::define_dlsym_reloc! {
    pub fn port_destroy(port: *mut Port);
    pub(crate) fn port_c_add_tuple(port: *mut Port, tuple: *mut BoxTuple);
    pub(crate) fn port_c_add_mp(port: *mut Port, mp: *const c_char, mp_end: *const c_char);
    pub(crate) fn port_c_create(port: *mut Port);
    pub fn port_c_destroy(port: *mut Port);

    pub(crate) fn cord_slab_cache() -> *const SlabCache;

    pub(crate) fn obuf_create(obuf: *mut Obuf, slab_cache: *const SlabCache, start_cap: size_t);
    pub(crate) fn obuf_destroy(obuf: *mut Obuf);
    pub(crate) fn obuf_reset(obuf: *mut Obuf);
    pub(crate) fn obuf_dup(
        obuf: *mut Obuf,
        data: *const c_void,
        size: size_t,
    ) -> size_t;

    /// Free memory allocated by this buffer
    pub fn ibuf_reinit(ibuf: *mut Ibuf);

    pub(crate) fn sql_prepare_ext(
        sql: *const u8,
        len: u32,
        stmt_id: *mut u32,
        session_id: *mut u64,
    ) -> c_int;
    pub(crate) fn sql_execute_prepared_ext(
        stmt_id: u32,
        mp_params: *const u8,
        vdbe_max_steps: u64,
        obuf: *mut Obuf,
    ) -> c_int;
    pub(crate) fn sql_unprepare_ext(stmt_id: u32, session_id: u64) -> c_int;
    pub(crate) fn sql_stmt_calculate_id(sql_str: *const c_char, len: size_t) -> u32;
    pub(crate) fn sql_prepare_and_execute_ext(
        sql: *const u8,
        len: c_int,
        mp_params: *const u8,
        vdbe_max_steps: u64,
        obuf: *mut Obuf,
    ) -> c_int;
    pub(crate) fn stmt_execute_into_port(
        stmt_id: u32,
        mp_params: *const u8,
        vdbe_max_steps: u64,
        port: *mut Port
    ) -> c_int;
    pub(crate) fn sql_execute_into_port(
        sql: *const u8,
        len: c_int,
        mp_params: *const u8,
        vdbe_max_steps: u64,
        port: *mut Port,
    ) -> c_int;
}

#[repr(C)]
pub(crate) struct SlabCache {
    _unused: [u8; 0],
}

#[repr(C)]
pub struct Ibuf {
    _slab_cache: *mut SlabCache,
    pub buf: *mut u8,
    // Start of inpu
    pub rpos: *mut u8,
    // End of useful input
    pub wpos: *mut u8,
    // End of ibuf
    pub epos: *mut u8,
    start_capacity: usize,
}

pub unsafe fn obuf_append(obuf: *mut Obuf, mp: &[u8]) -> crate::Result<()> {
    let size = obuf_dup(obuf, mp.as_ptr() as *const c_void, mp.len() as size_t);
    if size != mp.len() as size_t {
        return Err(TarantoolError::new(
            TarantoolErrorCode::MemoryIssue,
            format!("Failed to allocate {} bytes in obuf for data", mp.len()),
        )
        .into());
    }
    Ok(())
}

pub struct ObufWrapper {
    pub inner: Obuf,
    read_pos: usize,
    read_iov_n: usize,
    read_iov_pos: usize,
}

impl ObufWrapper {
    /// Create a new `ObufWrapper` with the given initial capacity.
    /// The capacity must be greater than 0.
    pub fn new(start_capacity: usize) -> Self {
        assert!(start_capacity > 0);
        let inner_buf = unsafe {
            let slab_c = cord_slab_cache();

            let mut buf = MaybeUninit::<Obuf>::zeroed();
            obuf_create(buf.as_mut_ptr(), slab_c, start_capacity);
            buf.assume_init()
        };

        Self {
            inner: inner_buf,
            read_pos: 0,
            read_iov_n: 0,
            read_iov_pos: 0,
        }
    }

    pub unsafe fn append_mp(&mut self, mp: &[u8]) -> crate::Result<()> {
        obuf_append(self.obuf(), mp)?;
        Ok(())
    }

    pub fn reset(&mut self) {
        unsafe { obuf_reset(self.obuf()) }
        self.read_pos = 0;
        self.read_iov_n = 0;
        self.read_iov_pos = 0;
    }

    pub fn obuf(&mut self) -> *mut Obuf {
        &mut self.inner as *mut Obuf
    }
}

impl Read for ObufWrapper {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut remains_read = cmp::min(buf.len(), self.inner.used - self.read_pos);
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

// TODO: ASan-enabled build has a different layout (obuf_asan.h).
#[repr(C)]
pub struct Obuf {
    _slab_cache: *const c_void,
    pub pos: i32,
    pub n_iov: i32,
    pub used: size_t,
    pub start_capacity: size_t,
    pub capacity: [size_t; 32],
    pub iov: [iovec; 32],
    // This flag is only present in debug builds (!NDEBUG),
    // but it's easier to just add it unconditionally to
    // prevent illegal memory access in obuf_create.
    // TODO: prevent this class of errors using a better solution.
    pub reserved: bool,
}

impl Drop for Obuf {
    fn drop(&mut self) {
        unsafe { obuf_destroy(self as *mut Obuf) }
    }
}

#[repr(C)]
pub struct SqlValue {
    _private: [u8; 0],
}

#[repr(C)]
pub struct PortVTable {
    pub dump_msgpack: unsafe extern "C" fn(port: *mut Port, out: *mut Obuf) -> c_int,
    pub dump_msgpack_16: unsafe extern "C" fn(port: *mut Port, out: *mut Obuf) -> c_int,
    pub dump_lua: unsafe extern "C" fn(port: *mut Port, l: *mut lua_State, is_flat: bool),
    pub dump_plain: unsafe extern "C" fn(port: *mut Port, size: *mut u32) -> *const c_char,
    pub get_msgpack: unsafe extern "C" fn(port: *mut Port, size: *mut u32) -> *const c_char,
    pub get_vdbemem: unsafe extern "C" fn(port: *mut Port, size: *mut u32) -> *mut SqlValue,
    pub destroy: unsafe extern "C" fn(port: *mut Port),
}

impl PortVTable {
    pub const fn new(
        dump_msgpack: unsafe extern "C" fn(port: *mut Port, out: *mut Obuf) -> c_int,
        dump_lua: unsafe extern "C" fn(port: *mut Port, l: *mut lua_State, is_flat: bool),
    ) -> Self {
        Self {
            dump_msgpack,
            dump_msgpack_16,
            dump_lua,
            dump_plain,
            get_msgpack,
            get_vdbemem,
            destroy,
        }
    }
}

#[no_mangle]
unsafe extern "C" fn dump_msgpack_16(_port: *mut Port, _out: *mut Obuf) -> c_int {
    unimplemented!();
}

#[no_mangle]
unsafe extern "C" fn dump_plain(_port: *mut Port, _size: *mut u32) -> *const c_char {
    unimplemented!();
}

#[no_mangle]
unsafe extern "C" fn get_msgpack(_port: *mut Port, _size: *mut u32) -> *const c_char {
    unimplemented!();
}

#[no_mangle]
unsafe extern "C" fn get_vdbemem(_port: *mut Port, _size: *mut u32) -> *mut SqlValue {
    unimplemented!();
}

#[no_mangle]
unsafe extern "C" fn destroy(port: *mut Port) {
    port_c_destroy(port);
}

#[repr(C)]
#[derive(Debug)]
pub struct Port {
    pub vtab: *const PortVTable,
    _pad: [u8; 68],
}

impl Drop for Port {
    fn drop(&mut self) {
        unsafe { port_destroy(self as *mut Port) }
    }
}

impl Port {
    /// Interpret `Port` as a mutable raw pointer to `PortC`.
    ///
    /// # Safety
    ///
    /// The caller must be sure that the port was initialized with `new_port_c`.
    pub unsafe fn as_mut_port_c(&mut self) -> &mut PortC {
        unsafe { NonNull::new_unchecked(self as *mut Port as *mut PortC).as_mut() }
    }

    pub fn as_ptr(&self) -> *const Port {
        self as *const Port
    }

    pub fn as_mut(&mut self) -> *mut Port {
        self as *mut Port
    }
}

impl Port {
    pub unsafe fn zeroed() -> Self {
        unsafe {
            Self {
                vtab: null(),
                _pad: std::mem::zeroed(),
            }
        }
    }

    pub fn new_port_c() -> Self {
        unsafe {
            let mut port = Self::zeroed();
            port_c_create(&mut port as *mut Port);
            port
        }
    }
}

#[repr(C)]
union U {
    tuple: NonNull<BoxTuple>,
    mp: *const u8,
}

#[repr(C)]
struct PortCEntry {
    next: *const PortCEntry,
    data: U,
    mp_sz: u32,
    tuple_format: *const c_void,
}

impl PortCEntry {
    unsafe fn data(&self) -> &[u8] {
        if self.mp_sz == 0 {
            let tuple_data = self.data.tuple.as_ref().data();
            return std::slice::from_raw_parts(tuple_data.as_ptr(), tuple_data.len());
        }
        std::slice::from_raw_parts(self.data.mp, self.mp_sz as usize)
    }
}

#[repr(C)]
pub struct PortC {
    pub vtab: *const PortVTable,
    first: *const PortCEntry,
    last: *const PortCEntry,
    first_entry: PortCEntry,
    size: i32,
}

impl PortC {
    pub fn size(&self) -> i32 {
        self.size
    }

    pub fn add_tuple(&mut self, tuple: &Tuple) {
        unsafe {
            port_c_add_tuple(
                self as *mut PortC as *mut Port,
                tuple.as_ptr() as *mut BoxTuple,
            );
        }
    }

    /// Add a msgpack-encoded data to the C port.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the `mp` slice is valid msgpack data.
    pub unsafe fn add_mp(&mut self, mp: &[u8]) {
        let Range { start, end } = mp.as_ptr_range();
        unsafe {
            port_c_add_mp(
                self as *mut PortC as *mut Port,
                start as *const c_char,
                end as *const c_char,
            );
        }
    }

    pub fn iter(&self) -> PortCIterator {
        PortCIterator::new(self)
    }

    /// Interpret `PortC` as a mutable raw pointer to `Port`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `PortC`:
    /// - occupies the same amount of memory as the `Port` does;
    /// - is properly initialized with `port_c_create`.
    pub unsafe fn as_mut_ptr(&mut self) -> *mut Port {
        self as *mut PortC as *mut Port
    }

    pub fn first_mp(&self) -> Option<&[u8]> {
        if self.first.is_null() {
            return None;
        }
        let entry = unsafe { &*(self.first as *const PortCEntry) };
        Some(unsafe { entry.data() })
    }

    pub fn last_mp(&self) -> Option<&[u8]> {
        if self.last.is_null() {
            return None;
        }
        let entry = unsafe { &*(self.last as *const PortCEntry) };
        Some(unsafe { entry.data() })
    }
}

#[allow(dead_code)]
pub struct PortCIterator<'port> {
    port: &'port PortC,
    entry: *const PortCEntry,
}

impl<'port> PortCIterator<'port> {
    fn new(port: &'port PortC) -> Self {
        Self {
            port,
            entry: port.first,
        }
    }
}

impl<'port> Iterator for PortCIterator<'port> {
    type Item = &'port [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.entry.is_null() {
            return None;
        }

        // The code was inspired by `port_c_dump_msgpack` function from `box/port.c`.
        let entry = unsafe { &*self.entry };
        self.entry = entry.next;
        Some(unsafe { entry.data() })
    }
}

#[cfg(feature = "picodata")]
#[cfg(feature = "internal_test")]
mod tests {
    use super::*;
    use crate::offset_of;

    #[crate::test(tarantool = "crate")]
    pub fn test_port_definition() {
        let lua = crate::lua_state();
        let [size_of_port, offset_of_vtab, offset_of_pad]: [usize; 3] = lua
            .eval(
                "local ffi = require('ffi')
            return {
                ffi.sizeof('struct port'),
                ffi.offsetof('struct port', 'vtab'),
                ffi.offsetof('struct port', 'pad')
            }",
            )
            .unwrap();

        assert_eq!(size_of_port, std::mem::size_of::<Port>());
        assert_eq!(offset_of_vtab, offset_of!(Port, vtab));
        assert_eq!(offset_of_pad, offset_of!(Port, _pad));
    }

    #[crate::test(tarantool = "crate")]
    pub fn test_port_c_definition() {
        let lua = crate::lua_state();
        let [size_of_port_c, offset_of_vtab,
             offset_of_first, offset_of_last,
             offset_of_first_entry, offset_of_size]: [usize; 6] = lua
            .eval(
                "local ffi = require('ffi')
            return {
                ffi.sizeof('struct port_c'),
                ffi.offsetof('struct port_c', 'vtab'),
                ffi.offsetof('struct port_c', 'first'),
                ffi.offsetof('struct port_c', 'last'),
                ffi.offsetof('struct port_c', 'first_entry'),
                ffi.offsetof('struct port_c', 'size')
            }",
            )
            .unwrap();

        assert_eq!(size_of_port_c, std::mem::size_of::<PortC>());
        assert_eq!(offset_of_vtab, offset_of!(PortC, vtab));
        assert_eq!(offset_of_first, offset_of!(PortC, first));
        assert_eq!(offset_of_last, offset_of!(PortC, last));
        assert_eq!(offset_of_first_entry, offset_of!(PortC, first_entry));
        assert_eq!(offset_of_size, offset_of!(PortC, size));
    }

    #[crate::test(tarantool = "crate")]
    pub fn test_obuf() {
        let mut obuf = ObufWrapper::new(1024);

        //Check appending data.
        let mp = b"\x92\x01\x02";
        unsafe {
            obuf.append_mp(mp).unwrap();
        }
        assert_eq!(obuf.read_pos, 0);
        let mut buf = [0u8; 3];
        let read = obuf.read(&mut buf).unwrap();
        assert_eq!(read, 3);
        assert_eq!(&buf, mp);
        // Check that the read position is updated.
        assert_eq!(obuf.read_pos, 3);

        // Check reset.
        obuf.reset();
        assert_eq!(obuf.read_pos, 0);
    }
}
