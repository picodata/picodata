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
    /// Wait until **READ** or **WRITE** event on socket (`fd`). Yields.
    /// - `fd` - non-blocking socket file description
    /// - `events` - requested events to wait.
    /// Combination of `TNT_IO_READ` | `TNT_IO_WRITE` bit flags.
    /// - `timeoout` - timeout in seconds.
    ///
    /// Returns:
    /// - `0` - timeout
    /// - `>0` - returned events. Combination of `TNT_IO_READ` | `TNT_IO_WRITE`
    /// bit flags.
    pub fn coio_wait(fd: c_int, event: c_int, timeout: f64) -> c_int;

/**
 * Close the fd and wake any fiber blocked in
 * coio_wait() call on this fd.
 */
    pub fn coio_close(fd: c_int) -> c_int;

    /// Fiber-friendly version of getaddrinfo(3).
    ///
    /// - `host` host name, i.e. "tarantool.org"
    /// - `port` service name, i.e. "80" or "http"
    /// - `hints` hints, see getaddrinfo(3)
    /// - `res`(out) result, see getaddrinfo(3)
    /// - `timeout` timeout
    ///
    /// Returns:
    /// -  `0` on success, please free @a res using freeaddrinfo(3).
    /// - `-1` on error, check diag.
    ///            Please note that the return value is not compatible with
    ///            getaddrinfo(3).
    pub fn coio_getaddrinfo(
        host: *const c_char,
        port: *const c_char,
        hints: *const libc::addrinfo,
        res: *mut *mut libc::addrinfo,
        timeout: f64,
    ) -> c_int;

    /// Create new eio task with specified function and
    /// arguments. Yield and wait until the task is complete.
    ///
    /// This function doesn't throw exceptions to avoid double error
    /// checking: in most cases it's also necessary to check the return
    /// value of the called function and perform necessary actions. If
    /// func sets errno, the errno is preserved across the call.
    ///
    /// Returns:
    /// - `-1` and `errno = ENOMEM` if failed to create a task
    /// - the function return (errno is preserved).
    ///
    /// # Example
    /// ```c
    ///	static ssize_t openfile_cb(va_list ap)
    ///	{
    ///	         const char *filename = va_arg(ap);
    ///	         int flags = va_arg(ap);
    ///	         return open(filename, flags);
    ///	}
    ///
    ///	if (coio_call(openfile_cb, "/tmp/file", 0) == -1)
    ///		// handle errors.
    ///	...
    /// ```
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
    /// Create a new fiber.
    ///
    /// Takes a fiber from fiber cache, if it's not empty.
    /// Can fail only if there is not enough memory for
    /// the fiber structure or fiber stack.
    ///
    /// The created fiber automatically returns itself
    /// to the fiber cache when its "main" function
    /// completes.
    ///
    /// - `name`       string with fiber name
    /// - `fiber_func` func for run inside fiber
    ///
    /// See also: [fiber_start](#fn.fiber_start)
    pub fn fiber_new(name: *const c_char, f: FiberFunc) -> *mut Fiber;

    /// Create a new fiber with defined attributes.
    ///
    /// Can fail only if there is not enough memory for
    /// the fiber structure or fiber stack.
    ///
    /// The created fiber automatically returns itself
    /// to the fiber cache if has default stack size
    /// when its "main" function completes.
    ///
    /// - `name`       string with fiber name
    /// - `fiber_attr` fiber attributes
    /// - `fiber_func` func for run inside fiber
    ///
    /// See also: [fiber_start](#fn.fiber_start)
    pub fn fiber_new_ex(
        name: *const c_char,
        fiber_attr: *const FiberAttr,
        f: FiberFunc,
    ) -> *mut Fiber;

    /// Return control to another fiber and wait until it'll be woken.
    ///
    /// See also: [fiber_wakeup](#fn.fiber_wakeup)
    pub fn fiber_yield();

    /// Start execution of created fiber.
    ///
    /// - `callee` fiber to start
    /// - `...`    arguments to start the fiber with
    ///
    /// See also: [fiber_new](#fn.fiber_new)
    pub fn fiber_start(callee: *mut Fiber, ...);

    /// Interrupt a synchronous wait of a fiber. Nop for the currently running
    /// fiber.
    ///
    /// - `f` fiber to be woken up
    pub fn fiber_wakeup(f: *mut Fiber);

    /// Cancel the subject fiber. (set FIBER_IS_CANCELLED flag)
    ///
    /// If target fiber's flag FIBER_IS_CANCELLABLE set, then it would
    /// be woken up (maybe prematurely). Then current fiber yields
    /// until the target fiber is dead (or is woken up by
    /// see also: [fiber_wakeup](#fn.fiber_wakeup)).
    ///
    /// - `f` fiber to be cancelled
    pub fn fiber_cancel(f: *mut Fiber);

    /// Make it possible or not possible to wakeup the current
    /// fiber immediately when it's cancelled.
    ///
    /// - `yesno` status to set
    ///
    /// Returns: previous state.
    pub fn fiber_set_cancellable(yesno: bool) -> bool;

    /// Set fiber to be joinable (false by default).
    /// - `yesno` status to set
    pub fn fiber_set_joinable(fiber: *mut Fiber, yesno: bool);

    /// Wait until the fiber is dead and then move its execution
    /// status to the caller.
    /// The fiber must not be detached (See also:
    /// [fiber_set_joinable](#fn.fiber_set_joinable)).
    /// `FIBER_IS_JOINABLE` flag is set.
    ///
    /// - `f` fiber to be woken up
    ///
    /// Returns: fiber function ret code
    pub fn fiber_join(f: *mut Fiber) -> c_int;

    /// Put the current fiber to sleep for at least 's' seconds.
    ///
    /// - `s` time to sleep
    ///
    /// **Note:** this is a cancellation point (\sa fiber_is_cancelled)
    pub fn fiber_sleep(s: f64);

    /// Check current fiber for cancellation (it must be checked manually).
    pub fn fiber_is_cancelled() -> bool;

    /// Report loop begin time as double (cheap).
    /// Uses real time clock.
    pub fn fiber_time() -> f64;

    /// Report loop begin time as 64-bit int.
    /// Uses real time clock.
    pub fn fiber_time64() -> u64;

    /// Report loop begin time as double (cheap).
    /// Uses monotonic clock.
    pub fn fiber_clock() -> f64;

    /// Report loop begin time as 64-bit int.
    /// Uses monotonic clock.
    pub fn fiber_clock64() -> u64;

    /// Reschedule fiber to end of event loop cycle.
    pub fn fiber_reschedule();

    /// Create a new fiber attribute container and initialize it
    /// with default parameters.
    /// Can be used for many fibers creation, corresponding fibers
    /// will not take ownership.
    pub fn fiber_attr_new() -> *mut FiberAttr;

    /// Delete the fiber_attr and free all allocated resources.
    /// This is safe when fibers created with this attribute still exist.
    ///
    /// - `fiber_attr` fiber attribute
    pub fn fiber_attr_delete(fiber_attr: *mut FiberAttr);

    /// Set stack size for the fiber attribute.
    ///
    /// - `fiber_attribute` fiber attribute container
    /// - `stacksize` stack size for new fibers
    pub fn fiber_attr_setstacksize(fiber_attr: *mut FiberAttr, stack_size: usize) -> c_int;

    /// Get stack size from the fiber attribute.
    ///
    /// - `fiber_attribute` fiber attribute container or NULL for default
    ///
    /// Returns: stack size
    pub fn fiber_attr_getstacksize(fiber_attr: *mut FiberAttr) -> usize;

    /// Instantiate a new fiber cond object.
    pub fn fiber_cond_new() -> *mut FiberCond;

    /// Delete the fiber cond object.
    /// Behaviour is undefined if there are fiber waiting for the cond.
    pub fn fiber_cond_delete(cond: *mut FiberCond);

    /// Wake one fiber waiting for the cond.
    /// Does nothing if no one is waiting.
    /// - `cond` condition
    pub fn fiber_cond_signal(cond: *mut FiberCond);

    /// Wake up all fibers waiting for the cond.
    /// - `cond` condition
    pub fn fiber_cond_broadcast(cond: *mut FiberCond);

    /// Suspend the execution of the current fiber (i.e. yield) until
    /// fiber_cond_signal() is called. Like pthread_cond, fiber_cond can issue
    /// spurious wake ups caused by explicit fiber_wakeup() or fiber_cancel()
    /// calls. It is highly recommended to wrap calls to this function into a loop
    /// and check an actual predicate and fiber_testcancel() on every iteration.
    ///
    /// - `cond`    condition
    /// - `timeout` timeout in seconds
    ///
    /// Returns:
    /// -  `0` on fiber_cond_signal() call or a spurious wake up
    /// - `-1` on timeout or fiber cancellation, diag is set
    pub fn fiber_cond_wait_timeout(cond: *mut FiberCond, timeout: f64) -> c_int;

    /// Shortcut for fiber_cond_wait_timeout().
    /// See also: [fiber_cond_wait_timeout](#fn.fiber_cond_wait_timeout)
    pub fn fiber_cond_wait(cond: *mut FiberCond) -> c_int;
}

// Latch.
#[repr(C)]
pub struct Latch {
    _unused: [u8; 0],
}

extern "C" {
    /// Allocate and initialize the new latch.
    ///
    /// Returns: latch
    pub fn box_latch_new() -> *mut Latch;

    /// Destroy and free the latch.
    /// - `latch` latch
    pub fn box_latch_delete(latch: *mut Latch);

    /// Lock a latch. Waits indefinitely until the current fiber can gain access to
    /// the latch.
    ///
    /// - `latch` a latch
    pub fn box_latch_lock(latch: *mut Latch);

    /// Try to lock a latch. Return immediately if the latch is locked.
    /// - `latch` a latch
    ///
    /// Returns:
    /// - `0` - success
    /// - `1` - the latch is locked.
    pub fn box_latch_trylock(latch: *mut Latch) -> c_int;

    /// Unlock a latch. The fiber calling this function must
    /// own the latch.
    ///
    /// - `latch` a latch
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
    /// Return IPROTO error code
    /// - `error` error
    ///
    /// Returns: enum `box_error_code`
    pub fn box_error_code(error: *const BoxError) -> u32;

    /// Return the error message
    /// - `error` error
    ///
    /// Returns: not-null string
    pub fn box_error_message(error: *const BoxError) -> *const c_char;

    /// Get the information about the last API call error.
    ///
    /// The Tarantool error handling works most like libc's errno. All API calls
    /// return -1 or NULL in the event of error. An internal pointer to
    /// box_error_t type is set by API functions to indicate what went wrong.
    /// This value is only significant if API call failed (returned -1 or NULL).
    ///
    /// Successful function can also touch the last error in some
    /// cases. You don't have to clear the last error before calling
    /// API functions. The returned object is valid only until next
    /// call to **any** API function.
    ///
    /// You must set the last error using [box_error_set](#fn.box_error_set) in
    /// your stored C procedures if you want to return a custom error message.
    /// You can re-throw the last API error to IPROTO client by keeping
    /// the current value and returning -1 to Tarantool from your
    /// stored procedure.
    ///
    /// Returns: last error.
    pub fn box_error_last() -> *mut BoxError;

    /// Return the error type, e.g. "ClientError", "SocketError", etc.
    /// - `error`
    ///
    /// Returns: not-null string
    pub fn box_error_type(error: *const BoxError) -> *const c_char;

    /// Clear the last error.
    pub fn box_error_clear();

    /// Set the last error.
    ///
    /// - `code` IPROTO error code (enum \link box_error_code \endlink)
    /// - `format` (const char * ) - printf()-like format string
    /// - ... - format arguments
    ///
    /// Returns: `-1` for convention use
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

use crate::ffi::lua::lua_State;
extern "C" {
    pub fn luaT_state() -> *mut lua_State;
    pub fn luaT_call(l: *mut lua_State, nargs: c_int, nreturns: c_int) -> isize;
}
