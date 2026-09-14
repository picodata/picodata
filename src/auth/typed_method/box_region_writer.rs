use crate::tarantool::box_region;
use std::ptr::NonNull;
use std::{io, ptr};

/// Growable container for incrementally writing bytes to the fiber-local box region allocator.
pub struct BoxRegionWriter {
    ptr: NonNull<[u8]>,
    len: usize,
}

impl BoxRegionWriter {
    /// Create a box region writer.
    ///
    /// # Safety
    ///
    /// Calling this outside of tarantool runtime is unsound.
    ///
    /// Additionally, while [`BoxRegionWriter`] has only safe methods and implements
    ///  [`io::Write`] (which also exposes a bunch of safe methods), one should exercise
    ///  great caution when using it.
    ///
    /// [`BoxRegionWriter`] makes an assumption with its box region allocator usage:
    ///  no observable truncations of the box region allocator happen between any calls to
    ///  [`BoxRegionWriter`].
    ///
    /// A box region truncation can happen through calls to
    ///  [`tarantool::ffi::tarantool::box_region_truncate`], or
    ///  [`crate::tarantool::box_region::BoxRegionCheckpoint::restore`].
    ///
    /// Making a box region truncation that is observable by [`BoxRegionWriter`] WILL lead
    ///  to memory corruption.
    pub unsafe fn with_capacity(capacity: usize) -> Self {
        Self {
            ptr: box_region::alloc_unaligned(capacity).expect("BoxRegionWriter failed to allocate"),
            len: 0,
        }
    }

    /// Get the pointer to the start of the controlled region.
    fn begin_ptr(&self) -> NonNull<u8> {
        self.ptr.cast::<u8>()
    }
    /// Get current size of the controlled region.
    fn capacity(&self) -> usize {
        self.ptr.len()
    }

    /// Make sure that the controlled region has space for `additional` bytes
    ///  to be written without reallocations.
    fn reserve(&mut self, additional: usize) {
        let required_capacity = self.len + additional;

        if self.capacity() >= required_capacity {
            return;
        }

        // This guarantees exponential growth. The doubling cannot overflow
        // because `self.capacity <= isize::MAX` and the type of `self.capacity` is `usize`.
        let new_capacity = std::cmp::max(self.capacity() * 2, required_capacity);
        // Do not allocate less than 8 bytes,
        //  because anything less is not worth the overhead of allocating it.
        let new_capacity = std::cmp::max(new_capacity, 8);
        // Rust doesn't support allocations larger than `isize`:
        //   https://doc.rust-lang.org/core/ptr/index.html#allocation
        // Larger than `isize` sizes will cause issues with many fundamental APIs,
        //   for example slices: https://doc.rust-lang.org/std/slice/fn.from_raw_parts.html#safety
        if new_capacity > isize::MAX as usize {
            panic!("BoxRegionWriter capacity overflow")
        }

        // SAFETY: as per the check above, new_capacity <= isize::MAX
        unsafe {
            self.ptr = box_region::grow_unaligned(self.ptr, self.len, new_capacity)
                .expect("BoxRegionWriter has failed to grow");
        }
    }

    /// Append bytes from `other` to the writer.
    pub fn extend_from_slice(&mut self, other: &[u8]) {
        let count = other.len();
        self.reserve(count);
        unsafe {
            ptr::copy_nonoverlapping(
                other.as_ptr(),
                self.begin_ptr().add(self.len).as_ptr(),
                count,
            )
        };
        self.len += count;
    }

    /// Gets the filled portion of the writer as a byte slice pointer.
    pub fn as_filled_slice(&self) -> NonNull<[u8]> {
        NonNull::slice_from_raw_parts(self.begin_ptr(), self.len)
    }

    /// Destroys the writer and gets the filled portion of the writer as a byte slice.
    pub fn into_filled_slice(self) -> NonNull<[u8]> {
        self.as_filled_slice()
    }
}

impl io::Write for BoxRegionWriter {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.extend_from_slice(buf);
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.extend_from_slice(buf);
        Ok(())
    }
}

mod tests {
    use super::BoxRegionWriter;
    use crate::tarantool::box_region::BoxRegionCheckpoint;
    #[tarantool::test]
    fn smoke() {
        let checkpoint = BoxRegionCheckpoint::new();

        let mut writer = unsafe { BoxRegionWriter::with_capacity(0) };
        assert_eq!(writer.capacity(), 0);
        assert_eq!(writer.len, 0);
        assert_eq!(unsafe { writer.as_filled_slice().as_ref() }, b"");

        writer.reserve(12);
        assert_eq!(writer.capacity(), 12);
        assert_eq!(writer.len, 0);
        assert_eq!(unsafe { writer.as_filled_slice().as_ref() }, b"");

        writer.extend_from_slice(b"42");
        assert_eq!(writer.capacity(), 12);
        assert_eq!(writer.len, 2);
        assert_eq!(unsafe { writer.as_filled_slice().as_ref() }, b"42");

        writer.extend_from_slice(b"XxXxXxXxXx");
        assert_eq!(writer.capacity(), 12);
        assert_eq!(writer.len, 12);
        assert_eq!(
            unsafe { writer.as_filled_slice().as_ref() },
            b"42XxXxXxXxXx"
        );

        writer.extend_from_slice(b"1");
        assert_eq!(writer.capacity(), 24); // reallocated to double the capacity
        assert_eq!(writer.len, 13);
        assert_eq!(
            unsafe { writer.as_filled_slice().as_ref() },
            b"42XxXxXxXxXx1"
        );

        let ptr = writer.into_filled_slice();

        assert_eq!(ptr.len(), 13);
        assert_eq!(unsafe { ptr.as_ref() }, b"42XxXxXxXxXx1");

        // Writer and objects it has allocated are not used anymore,
        //  so it's safe to restore the checkpoint now
        unsafe {
            checkpoint.restore();
        }
    }
}
