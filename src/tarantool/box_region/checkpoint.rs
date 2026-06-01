#![expect(clippy::new_without_default)] // Checkpoints should be created explicitly

/// A checkpoint of the box region allocator.
///
/// Checkpoints are the main way to free allocations in the box region allocator.
///
/// ```no_run
/// # use picodata::tarantool::box_region::BoxRegionCheckpoint;
///
/// let checkpoint = BoxRegionCheckpoint::new();
///
/// let memory = unsafe { tarantool::ffi::tarantool::box_region_alloc(42) };
///
/// // ... do something with the allocated memory ...
///
/// unsafe { checkpoint.restore(); }
///
/// // Now all objects that were allocated after BoxRegionCheckpoint::new are freed.
/// // Accessing `memory` will lead to a use-after-free
/// ```
#[must_use]
pub struct BoxRegionCheckpoint {
    used_at_creation: usize,
}

impl BoxRegionCheckpoint {
    /// Create a box region allocator checkpoint.
    ///
    /// Restoring the checkpoint will free all the objects allocated after the checkpoint was created.
    pub fn new() -> Self {
        let old_used = unsafe { tarantool::ffi::tarantool::box_region_used() };
        Self {
            used_at_creation: old_used,
        }
    }

    /// Truncate the box region allocator to the checkpoint.
    ///
    /// This will free all the objects allocated after the checkpoint was created.
    ///
    /// # Safety
    ///
    /// - Must be called from the same fiber that [`BoxRegionCheckpoint::new`] was called
    /// - No objects allocated after the checkpoint was created may be used after this function finishes.
    pub unsafe fn restore(self) {
        unsafe { tarantool::ffi::tarantool::box_region_truncate(self.used_at_creation) };
    }
}

mod tests {
    use super::BoxRegionCheckpoint;

    #[tarantool::test]
    fn smoke() {
        fn used() -> usize {
            unsafe { tarantool::ffi::tarantool::box_region_used() }
        }

        let initial_used = used();

        let checkpoint = BoxRegionCheckpoint::new();
        assert_eq!(used(), initial_used);

        unsafe { tarantool::ffi::tarantool::box_region_alloc(42) };
        let new_used = used();
        assert!(new_used > initial_used);

        unsafe { checkpoint.restore() };
        assert_eq!(used(), initial_used);
    }
}
