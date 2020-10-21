use std::os::raw::{c_char, c_int, c_uint};

use va_list::VaList;

use crate::tuple::ffi::BoxTuple;

// ===========================================================================
// Slab cache

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct SlabCache {
    _unused: [u8; 0],
}

extern "C" {
    /**
     * Return SlabCache suitable to use with tarantool/small library
     */
    pub fn cord_slab_cache() -> *mut SlabCache;
}

// ===========================================================================
// CoIO

extern "C" {
    /**
     * Create new eio task with specified function and
     * arguments. Yield and wait until the task is complete
     * or a timeout occurs.
     *
     * This function doesn't throw exceptions to avoid double error
     * checking: in most cases it's also necessary to check the return
     * value of the called function and perform necessary actions. If
     * func sets errno, the errno is preserved across the call.
     *
     * @retval -1 and errno = ENOMEM if failed to create a task
     * @retval the function return (errno is preserved).
     *
     * @code
     *	static ssize_t openfile_cb(va_list ap)
     *	{
     *	         const char *filename = va_arg(ap);
     *	         int flags = va_arg(ap);
     *	         return open(filename, flags);
     *	}
     *
     *	if (coio_call(openfile_cb, 0.10, "/tmp/file", 0) == -1)
     *		// handle errors.
     *	...
     * @endcode
     */
    pub fn coio_call(func: Option<unsafe extern "C" fn(VaList) -> c_int>, ...) -> isize;
}

// ===========================================================================
// Tuple

extern "C" {
    pub fn box_tuple_update(
        tuple: *const BoxTuple,
        expr: *const c_char,
        expr_end: *const c_char,
    ) -> *mut BoxTuple;
    pub fn box_tuple_upsert(
        tuple: *const BoxTuple,
        expr: *const c_char,
        expr_end: *const c_char,
    ) -> *mut BoxTuple;
}

// ===========================================================================
// Space

pub const BOX_SYSTEM_ID_MIN: u32 = 256;
pub const BOX_SCHEMA_ID: u32 = 272;
pub const BOX_SPACE_ID: u32 = 280;
pub const BOX_VSPACE_ID: u32 = 281;
pub const BOX_INDEX_ID: u32 = 288;
pub const BOX_VINDEX_ID: u32 = 289;
pub const BOX_FUNC_ID: u32 = 296;
pub const BOX_VFUNC_ID: u32 = 297;
pub const BOX_USER_ID: u32 = 304;
pub const BOX_VUSER_ID: u32 = 305;
pub const BOX_PRIV_ID: u32 = 312;
pub const BOX_VPRIV_ID: u32 = 313;
pub const BOX_CLUSTER_ID: u32 = 320;
pub const BOX_SYSTEM_ID_MAX: u32 = 511;
pub const BOX_ID_NIL: u32 = 2147483647;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct BoxFunctionCtx {
    _unused: [u8; 0],
}

extern "C" {
    /**
     * Return a Tuple from stored C procedure.
     *
     * Returned Tuple is automatically reference counted by Tarantool.
     *
     * \param ctx an opaque structure passed to the stored C procedure by
     * Tarantool
     * \param Tuple a Tuple to return
     * \retval -1 on error (perhaps, out of memory; check box_error_last())
     * \retval 0 otherwise
     */
    pub fn box_return_tuple(ctx: *mut BoxFunctionCtx, tuple: *mut BoxTuple) -> c_int;
}

extern "C" {
    /**
     * Clear the last error.
     */
    pub fn box_error_clear();

    /**
     * Set the last error.
     *
     * \param code IPROTO error code (enum \link box_error_code \endlink)
     * \param format (const char * ) - printf()-like format string
     * \param ... - format arguments
     * \returns -1 for convention use
     *
     * \sa enum box_error_code
     */
    pub fn box_error_set(
        file: *const c_char,
        line: c_uint,
        code: u32,
        format: *const c_char,
        ...
    ) -> c_int;
}

// ===========================================================================
// Latch

/**
 * A lock for cooperative multitasking environment
 */
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct BoxLatch {
    _unused: [u8; 0],
}

extern "C" {
    /**
     * Allocate and initialize the new latch.
     * \returns latch
     */
    pub fn box_latch_new() -> *mut BoxLatch;

    /**
     * Destroy and free the latch.
     * \param latch latch
     */
    pub fn box_latch_delete(latch: *mut BoxLatch);

    /**
     * Lock a latch. Waits indefinitely until the current fiber can gain access to
     * the latch.
     *
     * \param latch a latch
     */
    pub fn box_latch_lock(latch: *mut BoxLatch);

    /**
     * Try to lock a latch. Return immediately if the latch is locked.
     * \param latch a latch
     * \retval 0 - success
     * \retval 1 - the latch is locked.
     */
    pub fn box_latch_trylock(latch: *mut BoxLatch) -> c_int;

    /**
     * Unlock a latch. The fiber calling this function must
     * own the latch.
     *
     * \param latch a latch
     */
    pub fn box_latch_unlock(latch: *mut BoxLatch);
}

// ===========================================================================
// Clock

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
