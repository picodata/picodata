#![allow(non_camel_case_types)]
/// Module provides FFI bindings for the following constants,
/// types and functios from Tarantool module C API:
/// 1. Clock.
/// 2. COIO.
/// 3. Fibers.
/// 4. Latches.
/// 5. Log.
/// 6. Box - errors, sessions, sequences, transactions, indexes, spaces, tuples.
use std::os::raw::{c_char, c_int, c_uint, c_void};

use va_list::VaList;

// Clock.
extern "C" {
    pub fn clock_realtime() -> f64;
    pub fn clock_monotonic() -> f64;
    pub fn clock_process() -> f64;
    pub fn clock_thread() -> f64;
    pub fn clock_realtime64() -> u64;
    pub fn clock_monotonic64() -> u64;
    pub fn clock_process64() -> u64;
    pub fn clock_thread64() -> u64;
}

// COIO.
bitflags! {
    /// Event type(s) to wait. Can be `READ` or/and `WRITE`
    pub struct CoIOFlags: c_int {
        const READ = 1;
        const WRITE = 2;
    }
}

extern "C" {
    pub fn coio_wait(fd: c_int, event: c_int, timeout: f64) -> c_int;
    pub fn coio_close(fd: c_int) -> c_int;
    pub fn coio_getaddrinfo(
        host: *const c_char,
        port: *const c_char,
        hints: *const libc::addrinfo,
        res: *mut *mut libc::addrinfo,
        timeout: f64,
    ) -> c_int;
    pub fn coio_call(func: Option<unsafe extern "C" fn(VaList) -> c_int>, ...) -> isize;
}

// Fiber.
#[repr(C)]
pub struct Fiber {
    _unused: [u8; 0],
}

#[repr(C)]
pub struct FiberAttr {
    _unused: [u8; 0],
}

#[repr(C)]
pub struct FiberCond {
    _unused: [u8; 0],
}

pub type FiberFunc = Option<unsafe extern "C" fn(VaList) -> c_int>;

extern "C" {
    pub fn fiber_new(name: *const c_char, f: FiberFunc) -> *mut Fiber;
    pub fn fiber_new_ex(
        name: *const c_char,
        fiber_attr: *const FiberAttr,
        f: FiberFunc,
    ) -> *mut Fiber;
    pub fn fiber_yield();
    pub fn fiber_start(callee: *mut Fiber, ...);
    pub fn fiber_wakeup(f: *mut Fiber);
    pub fn fiber_cancel(f: *mut Fiber);
    pub fn fiber_set_cancellable(yesno: bool) -> bool;
    pub fn fiber_set_joinable(fiber: *mut Fiber, yesno: bool);
    pub fn fiber_join(f: *mut Fiber) -> c_int;
    pub fn fiber_sleep(s: f64);
    pub fn fiber_is_cancelled() -> bool;
    pub fn fiber_time() -> f64;
    pub fn fiber_time64() -> u64;
    pub fn fiber_clock() -> f64;
    pub fn fiber_clock64() -> u64;
    pub fn fiber_reschedule();
    pub fn fiber_attr_new() -> *mut FiberAttr;
    pub fn fiber_attr_delete(fiber_attr: *mut FiberAttr);
    pub fn fiber_attr_setstacksize(fiber_attr: *mut FiberAttr, stack_size: usize) -> c_int;
    pub fn fiber_attr_getstacksize(fiber_attr: *mut FiberAttr) -> usize;
    pub fn fiber_cond_new() -> *mut FiberCond;
    pub fn fiber_cond_delete(cond: *mut FiberCond);
    pub fn fiber_cond_signal(cond: *mut FiberCond);
    pub fn fiber_cond_broadcast(cond: *mut FiberCond);
    pub fn fiber_cond_wait_timeout(cond: *mut FiberCond, timeout: f64) -> c_int;
    pub fn fiber_cond_wait(cond: *mut FiberCond) -> c_int;
}

// Latch.
#[repr(C)]
pub struct Latch {
    _unused: [u8; 0],
}

extern "C" {
    pub fn box_latch_new() -> *mut Latch;
    pub fn box_latch_delete(latch: *mut Latch);
    pub fn box_latch_lock(latch: *mut Latch);
    pub fn box_latch_trylock(latch: *mut Latch) -> c_int;
    pub fn box_latch_unlock(latch: *mut Latch);
}

// Log.
pub type SayFunc =
    Option<unsafe extern "C" fn(c_int, *const c_char, c_int, *const c_char, *const c_char, ...)>;

extern "C" {
    #[link_name = "log_level"]
    pub static mut LOG_LEVEL: c_int;

    #[link_name = "_say"]
    pub static mut SAY_FN: SayFunc;
}

// Error.
#[repr(C)]
pub struct BoxError {
    _unused: [u8; 0],
}

extern "C" {
    pub fn box_error_code(error: *const BoxError) -> u32;
    pub fn box_error_message(error: *const BoxError) -> *const c_char;
    pub fn box_error_last() -> *mut BoxError;
    pub fn box_error_type(error: *const BoxError) -> *const c_char;
    pub fn box_error_clear();
    pub fn box_error_set(
        file: *const c_char,
        line: c_uint,
        code: u32,
        format: *const c_char,
        ...
    ) -> c_int;
}

// Session.
extern "C" {
    pub fn box_session_push(data: *const c_char, data_end: *const c_char) -> c_int;
}

// Sequence.
extern "C" {
    pub fn box_sequence_next(seq_id: u32, result: *mut i64) -> c_int;
    pub fn box_sequence_set(seq_id: u32, value: i64) -> c_int;
    pub fn box_sequence_reset(seq_id: u32) -> c_int;
}

// Transaction.
extern "C" {
    pub fn box_txn() -> bool;
    pub fn box_txn_begin() -> c_int;
    pub fn box_txn_commit() -> c_int;
    pub fn box_txn_rollback() -> c_int;
    pub fn box_txn_alloc(size: usize) -> *mut c_void;
}

// Indexes, spaces and tuples.
pub const BOX_ID_NIL: u32 = 2147483647;

extern "C" {
    pub fn box_insert(
        space_id: u32,
        tuple: *const c_char,
        tuple_end: *const c_char,
        result: *mut *mut BoxTuple,
    ) -> c_int;
    pub fn box_update(
        space_id: u32,
        index_id: u32,
        key: *const c_char,
        key_end: *const c_char,
        ops: *const c_char,
        ops_end: *const c_char,
        index_base: c_int,
        result: *mut *mut BoxTuple,
    ) -> c_int;
    pub fn box_upsert(
        space_id: u32,
        index_id: u32,
        tuple: *const c_char,
        tuple_end: *const c_char,
        ops: *const c_char,
        ops_end: *const c_char,
        index_base: c_int,
        result: *mut *mut BoxTuple,
    ) -> c_int;
    pub fn box_replace(
        space_id: u32,
        tuple: *const c_char,
        tuple_end: *const c_char,
        result: *mut *mut BoxTuple,
    ) -> c_int;
    pub fn box_delete(
        space_id: u32,
        index_id: u32,
        key: *const c_char,
        key_end: *const c_char,
        result: *mut *mut BoxTuple,
    ) -> c_int;
    pub fn box_truncate(space_id: u32) -> c_int;
}

extern "C" {
    pub fn box_index_id_by_name(space_id: u32, name: *const c_char, len: u32) -> u32;
    pub fn box_space_id_by_name(name: *const c_char, len: u32) -> u32;
    pub fn box_index_len(space_id: u32, index_id: u32) -> isize;
    pub fn box_index_bsize(space_id: u32, index_id: u32) -> isize;
    pub fn box_index_random(
        space_id: u32,
        index_id: u32,
        rnd: u32,
        result: *mut *mut BoxTuple,
    ) -> c_int;
    pub fn box_index_get(
        space_id: u32,
        index_id: u32,
        key: *const c_char,
        key_end: *const c_char,
        result: *mut *mut BoxTuple,
    ) -> c_int;
    pub fn box_index_min(
        space_id: u32,
        index_id: u32,
        key: *const c_char,
        key_end: *const c_char,
        result: *mut *mut BoxTuple,
    ) -> c_int;
    pub fn box_index_max(
        space_id: u32,
        index_id: u32,
        key: *const c_char,
        key_end: *const c_char,
        result: *mut *mut BoxTuple,
    ) -> c_int;
    pub fn box_index_count(
        space_id: u32,
        index_id: u32,
        type_: c_int,
        key: *const c_char,
        key_end: *const c_char,
    ) -> isize;
}

#[repr(C)]
pub struct BoxIterator {
    _unused: [u8; 0],
}

// Index iterator
extern "C" {
    pub fn box_index_iterator(
        space_id: u32,
        index_id: u32,
        type_: c_int,
        key: *const c_char,
        key_end: *const c_char,
    ) -> *mut BoxIterator;
    pub fn box_iterator_next(iterator: *mut BoxIterator, result: *mut *mut BoxTuple) -> c_int;
    pub fn box_iterator_free(iterator: *mut BoxIterator);
}

#[repr(C)]
pub struct BoxTuple {
    _unused: [u8; 0],
}

#[repr(C)]
pub struct BoxTupleFormat {
    _unused: [u8; 0],
}

// Tuple
extern "C" {
    pub fn box_tuple_extract_key(
        tuple: *const BoxTuple,
        space_id: u32,
        index_id: u32,
        key_size: *mut u32,
    ) -> *mut c_char;
    pub fn box_tuple_new(
        format: *mut BoxTupleFormat,
        data: *const c_char,
        end: *const c_char,
    ) -> *mut BoxTuple;
    pub fn box_tuple_ref(tuple: *mut BoxTuple) -> c_int;
    pub fn box_tuple_unref(tuple: *mut BoxTuple);
    pub fn box_tuple_field_count(tuple: *const BoxTuple) -> u32;
    pub fn box_tuple_bsize(tuple: *const BoxTuple) -> usize;
    pub fn box_tuple_to_buf(tuple: *const BoxTuple, buf: *mut c_char, size: usize) -> isize;
    pub fn box_tuple_format_default() -> *mut BoxTupleFormat;
    pub fn box_tuple_format(tuple: *const BoxTuple) -> *mut BoxTupleFormat;
    pub fn box_tuple_field(tuple: *const BoxTuple, fieldno: u32) -> *const c_char;
    pub fn box_tuple_compare(
        tuple_a: *mut BoxTuple,
        tuple_b: *mut BoxTuple,
        key_def: *mut BoxKeyDef,
    ) -> c_int;
    pub fn box_tuple_compare_with_key(
        tuple_a: *mut BoxTuple,
        key_b: *const c_char,
        key_def: *mut BoxKeyDef,
    ) -> c_int;
}

#[repr(C)]
pub struct BoxTupleIterator {
    _unused: [u8; 0],
}

// Tuple iterator
extern "C" {
    pub fn box_tuple_iterator(tuple: *mut BoxTuple) -> *mut BoxTupleIterator;
    pub fn box_tuple_iterator_free(it: *mut BoxTupleIterator);
    pub fn box_tuple_position(it: *mut BoxTupleIterator) -> u32;
    pub fn box_tuple_rewind(it: *mut BoxTupleIterator);
    pub fn box_tuple_seek(it: *mut BoxTupleIterator, fieldno: u32) -> *const c_char;
    pub fn box_tuple_next(it: *mut BoxTupleIterator) -> *const c_char;
}

#[repr(C)]
pub struct BoxKeyDef {
    _unused: [u8; 0],
}

extern "C" {
    pub fn box_key_def_new(fields: *mut u32, types: *mut u32, part_count: u32) -> *mut BoxKeyDef;
    pub fn box_key_def_delete(key_def: *mut BoxKeyDef);
}

#[repr(C)]
pub struct BoxFunctionCtx {
    _unused: [u8; 0],
}

extern "C" {
    pub fn box_return_tuple(ctx: *mut BoxFunctionCtx, tuple: *mut BoxTuple) -> c_int;
    pub fn box_return_mp(
        ctx: *mut BoxFunctionCtx,
        mp: *const c_char,
        mp_end: *const c_char,
    ) -> c_int;
}
