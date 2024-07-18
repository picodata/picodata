#![cfg(any(feature = "picodata", doc))]

use libc::{iovec, size_t};
use std::cmp;
use std::io::Read;
use std::os::raw::{c_char, c_int, c_void};

pub const IPROTO_DATA: u8 = 0x30;

// Note that all of the functions defined here are either `pub` or `pub(crate)`
// even if they're only used in this file. This is because the `define_dlsym_reloc`
// macro doesn't support private function declarations because rust's macro syntax is trash.
crate::define_dlsym_reloc! {
    pub(crate) fn cord_slab_cache() -> *const SlabCache;

    pub(crate) fn obuf_create(obuf: *mut Obuf, slab_cache: *const SlabCache, start_cap: size_t);
    pub(crate) fn obuf_destroy(obuf: *mut Obuf);

    /// Free memory allocated by this buffer
    pub fn ibuf_reinit(ibuf: *mut Ibuf);

    pub(crate) fn sql_prepare_ext(sql: *const u8, len: u32, stmt_id: *mut u32) -> c_int;
    pub(crate) fn sql_execute_prepared_ext(
        stmt_id: u32,
        mp_params: *const u8,
        vdbe_max_steps: u64,
        obuf: *mut Obuf,
    ) -> c_int;
    pub(crate) fn sql_unprepare(stmt_id: u32) -> c_int;
    pub(crate) fn sql_stmt_calculate_id(sql_str: *const c_char, len: size_t) -> u32;
    pub(crate) fn sql_prepare_and_execute_ext(
        sql: *const u8,
        len: c_int,
        mp_params: *const u8,
        vdbe_max_steps: u64,
        obuf: *mut Obuf,
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

    pub(crate) fn obuf(&mut self) -> *mut Obuf {
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
