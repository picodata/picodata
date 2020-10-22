use std::os::raw::{c_char, c_int, c_uint};

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
