use crate::tarantool::box_region::BoxRegionCheckpoint;
use std::ptr::NonNull;
use tarantool::ffi::tarantool::box_region_alloc;

// FIXME: we do not have any allocation routines for getting aligned memory.
// These functions can be implemented in principle, but there has not been any need yet.
// API of std::alloc::Allocator could be used as a model for these.

/// Allocate memory from tarantool box region allocator.
///
/// The allocated memory will NOT be aligned.
/// This memory is only suitable for storing bytes.
///
/// # Return value
///
/// Will return `None` if allocation fails.
/// Otherwise, returns a non-null pointer to a slice of requested size.
///
/// The allocated memory is not initialized, so it is not safe
///  to dereference the returned slice right away.
pub fn alloc_unaligned(size: usize) -> Option<NonNull<[u8]>> {
    let ptr = unsafe { box_region_alloc(size) } as *mut u8;
    // Check for allocation error.
    let ptr = NonNull::new(ptr)?;
    let ptr = NonNull::slice_from_raw_parts(ptr, size);

    Some(ptr)
}

/// Grows an allocation previously allocated by [`alloc_unaligned`].
///
/// The allocated memory will NOT be aligned.
/// This memory is only suitable for storing bytes.
///
/// The function will first attempt to expand the provided allocation.
/// The extension is not possible if there were objects allocated
///  from the box region after the allocation we are trying to extend,
///  or if the allocation cannot fit into the current slab block.
///
/// If the provided allocation cannot be extended, a new allocation
///  will be created. The function will transparently copy the first
///  `filled_size` bytes from the old allocation to the new.
///
/// Even when a new allocation is made, the `old` allocation is not invalidated.
///
/// # Safety
///
/// - `old` has to either point to an empty slice, or be a value
///   returned from [`alloc_unaligned`]
/// - must be called from the same fiber that allocated `old`
/// - first `filled_size` bytes of the `old` slice cannot be uninitialized memory
///
/// # Return value
///
/// Will return `None` if allocation fails.
/// Otherwise, returns a non-null pointer to a slice of requested size.
///
/// The allocated memory is not initialized, so it is not safe
///  to dereference the returned slice right away.
pub unsafe fn grow_unaligned(
    old: NonNull<[u8]>,
    filled_size: usize,
    new_size: usize,
) -> Option<NonNull<[u8]>> {
    let old_size = old.len();
    let old_begin = old.cast::<u8>();
    // SAFETY: `old` is either an empty slice (in which case `add` is a no-op),
    // or must come from `alloc_unaligned`, which means that it will span a single allocation.
    let old_end = unsafe { old_begin.add(old_size) };

    debug_assert!(filled_size <= old_size);

    let Some(grow_by) = new_size.checked_sub(old_size) else {
        // If a shrink was requested - just return the old slice as-is.
        // Box region is a bump allocator, so we can't shrink the allocations.
        return Some(old);
    };
    if grow_by == 0 {
        return Some(old);
    }

    // Now try allocating a new piece of memory from box region and see if
    //  it is contiguous with the old allocation. This will be the case if
    //  no allocations were done between the initial allocation of the `old`
    //  and the growth request, and if there is space in the current slab block.

    // Make a checkpoint to cancel the allocation in case it is not contiguous.
    let checkpoint = BoxRegionCheckpoint::new();

    let new_block = unsafe { box_region_alloc(grow_by) } as *mut u8;
    // Check for allocation error.
    let new_block = NonNull::new(new_block)?;

    if old_end == new_block {
        // The allocation was grown in a contiguous way.
        // We don't need to do any additional work.

        Some(NonNull::slice_from_raw_parts(old_begin, new_size))
    } else {
        // Can't grow the block in a contiguous way. Discard the attempted
        //  growth block and allocate a new block to fit the requested allocation fully.

        // SAFETY: the only allocation we discard here is the `new_block`
        //  that we have allocated ourselves.
        unsafe { checkpoint.restore() };

        let new_allocation = unsafe { box_region_alloc(new_size) } as *mut u8;
        // Check for allocation error.
        let new_allocation = NonNull::new(new_allocation)?;

        // Copy the data from the old region to the reallocated one.
        unsafe {
            std::ptr::copy_nonoverlapping(old_begin.as_ptr(), new_allocation.as_ptr(), filled_size);
        }

        Some(NonNull::slice_from_raw_parts(new_allocation, new_size))
    }
}

mod tests {
    use super::{alloc_unaligned, grow_unaligned};
    use crate::tarantool::box_region::BoxRegionCheckpoint;
    use std::ptr::NonNull;

    #[tarantool::test]
    fn alloc() {
        let checkpoint = BoxRegionCheckpoint::new();

        const SZ: usize = 12;

        // We can allocate
        let buf = alloc_unaligned(SZ).unwrap();

        // The returned slice pointer has correct size
        assert_eq!(buf.len(), SZ);

        // We can write to the allocated memory
        unsafe { std::ptr::write_bytes(buf.cast::<u8>().as_ptr(), 42, buf.len()) };
        assert_eq!(unsafe { buf.as_ref() }, &[42; SZ]);

        // Allocations can fail
        assert_eq!(alloc_unaligned(isize::MAX as usize), None);

        // Objects allocated in the box region allocator are not used anymore,
        //  so it's safe to restore the checkpoint now
        unsafe {
            checkpoint.restore();
        }
    }

    #[tarantool::test]
    fn grow() {
        // check that we can grow a region
        {
            let checkpoint = BoxRegionCheckpoint::new();

            const INIT_SZ: usize = 12;
            const NEW_SZ: usize = 32;

            let buf = alloc_unaligned(INIT_SZ).unwrap();
            // SAFETY: buf was allocated through the box region
            let buf_grown = unsafe { grow_unaligned(buf, 0, NEW_SZ) }.unwrap();

            // the returned slice pointer has correct size
            assert_eq!(buf_grown.len(), NEW_SZ);

            // The initial allocation was grown, no copy was made.
            assert_eq!(buf.cast::<u8>(), buf_grown.cast::<u8>());

            // Growing can fail
            // SAFETY: buf was allocated through the box region
            assert_eq!(
                unsafe { grow_unaligned(buf_grown, 0, isize::MAX as usize) },
                None
            );

            // Objects allocated in the box region allocator are not used anymore,
            //  so it's safe to restore the checkpoint now
            unsafe {
                checkpoint.restore();
            }
        }
        // check that a non-contiguous grow works too
        {
            let checkpoint = BoxRegionCheckpoint::new();

            const INIT_SZ: usize = 12;
            const NEW_SZ: usize = 32;

            let buf = alloc_unaligned(12).unwrap();

            unsafe { std::ptr::write_bytes(buf.cast::<u8>().as_ptr(), 42, buf.len()) };
            assert_eq!(unsafe { buf.as_ref() }, &[42; INIT_SZ]);

            // Put another allocation behind `buf`. This will prevent `buf` from being able to grow.
            let _ = alloc_unaligned(12).unwrap();

            // SAFETY: buf was allocated through the box region
            let buf_grown = unsafe { grow_unaligned(buf, 12, NEW_SZ) }.unwrap();

            // The returned slice pointer has correct size
            assert_eq!(buf_grown.len(), NEW_SZ);

            // The initial allocation was NOT grown, a copy was made.
            assert_ne!(buf.cast::<u8>(), buf_grown.cast::<u8>());

            // Check that the initialized portion was actually copied.
            assert_eq!(
                unsafe { NonNull::slice_from_raw_parts(buf_grown.cast::<u8>(), INIT_SZ).as_ref() },
                &[42; INIT_SZ]
            );

            // Objects allocated in the box region allocator are not used anymore,
            //  so it's safe to restore the checkpoint now
            unsafe {
                checkpoint.restore();
            }
        }
    }
}
