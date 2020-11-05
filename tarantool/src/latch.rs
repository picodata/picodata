/// A lock for cooperative multitasking environment
pub struct Latch {
    inner: *mut ffi::Latch,
}

impl Latch {
    /// Allocate and initialize the new latch.
    pub fn new() -> Self {
        Latch {
            inner: unsafe { ffi::box_latch_new() },
        }
    }

    /// Lock a latch. Waits indefinitely until the current fiber can gain access to the latch.
    pub fn lock(&self) -> LatchGuard {
        unsafe { ffi::box_latch_lock(self.inner) };
        LatchGuard { latch: self }
    }

    /// Try to lock a latch. Return immediately if the latch is locked.
    ///
    /// Returns:
    /// - `Some` - success
    /// - `None` - the latch is locked.
    pub fn try_lock(&self) -> Option<LatchGuard> {
        if unsafe { ffi::box_latch_trylock(self.inner) } == 0 {
            Some(LatchGuard { latch: self })
        } else {
            None
        }
    }
}

impl Drop for Latch {
    fn drop(&mut self) {
        unsafe { ffi::box_latch_delete(self.inner) }
    }
}

/// An RAII implementation of a "scoped lock" of a latch. When this structure is dropped (falls out of scope),
/// the lock will be unlocked.
pub struct LatchGuard<'a> {
    latch: &'a Latch,
}

impl<'a> Drop for LatchGuard<'a> {
    fn drop(&mut self) {
        unsafe { ffi::box_latch_unlock(self.latch.inner) }
    }
}

mod ffi {
    use std::os::raw::c_int;

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
}
