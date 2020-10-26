use std::os::raw::c_char;

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
