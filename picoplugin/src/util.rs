use abi_stable::StableAbi;
use std::ptr::NonNull;
use tarantool::error::BoxError;
use tarantool::error::TarantoolErrorCode;
use tarantool::ffi::tarantool as ffi;

////////////////////////////////////////////////////////////////////////////////
// FfiSafeBytes
////////////////////////////////////////////////////////////////////////////////

/// A helper struct for passing byte slices over the ABI boundary.
#[repr(C)]
#[derive(StableAbi, Clone, Copy, Debug)]
pub struct FfiSafeBytes {
    pointer: NonNull<u8>,
    len: usize,
}

impl FfiSafeBytes {
    #[inline(always)]
    pub fn len(self) -> usize {
        self.len
    }

    #[inline(always)]
    pub unsafe fn from_raw_parts(pointer: NonNull<u8>, len: usize) -> Self {
        Self { pointer, len }
    }

    #[inline(always)]
    pub unsafe fn into_raw_parts(mut self) -> (*mut u8, usize) {
        (&mut *self.pointer.as_mut(), self.len)
    }

    /// Converts `self` back to a borrowed string `&[u8]`.
    ///
    /// # Safety
    /// `FfiSafeBytes` can only be constructed from a valid rust byte slice,
    /// so you only need to make sure that the origial `&[u8]` outlives the lifetime `'a`.
    ///
    /// This should generally be true when borrowing strings owned by the current
    /// function and calling a function via FFI, but borrowing global data or
    /// data stored within a `Rc` for example is probably unsafe.
    pub unsafe fn as_bytes<'a>(self) -> &'a [u8] {
        std::slice::from_raw_parts(self.pointer.as_ptr(), self.len)
    }
}

impl Default for FfiSafeBytes {
    #[inline(always)]
    fn default() -> Self {
        Self {
            pointer: NonNull::dangling(),
            len: 0,
        }
    }
}

impl<'a> From<&'a [u8]> for FfiSafeBytes {
    #[inline(always)]
    fn from(value: &'a [u8]) -> Self {
        Self {
            pointer: as_non_null_ptr(value),
            len: value.len(),
        }
    }
}

impl<'a> From<&'a str> for FfiSafeBytes {
    #[inline(always)]
    fn from(value: &'a str) -> Self {
        Self {
            pointer: as_non_null_ptr(value.as_bytes()),
            len: value.len(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// FfiSafeStr
////////////////////////////////////////////////////////////////////////////////

/// A helper struct for passing rust strings over the ABI boundary.
///
/// This type can only be constructed from a valid rust string, so it's not
/// necessary to validate the utf8 encoding when converting back to `&str`.
#[repr(C)]
#[derive(StableAbi, Clone, Copy, Debug)]
pub struct FfiSafeStr {
    pointer: NonNull<u8>,
    len: usize,
}

impl FfiSafeStr {
    #[inline(always)]
    pub fn len(self) -> usize {
        self.len
    }

    #[inline(always)]
    pub unsafe fn from_raw_parts(pointer: NonNull<u8>, len: usize) -> Self {
        Self { pointer, len }
    }

    /// Converts `self` back to a borrowed string `&str`.
    ///
    /// # Safety
    /// `FfiSafeStr` can only be constructed from a valid rust `str`,
    /// so you only need to make sure that the origial `str` outlives the lifetime `'a`.
    ///
    /// This should generally be true when borrowing strings owned by the current
    /// function and calling a function via FFI, but borrowing global data or
    /// data stored within a `Rc` for example is probably unsafe.
    #[inline]
    pub unsafe fn as_str<'a>(self) -> &'a str {
        if cfg!(debug_assertions) {
            std::str::from_utf8(self.as_bytes()).expect("should only be used with valid utf8")
        } else {
            std::str::from_utf8_unchecked(self.as_bytes())
        }
    }

    #[inline(always)]
    pub unsafe fn as_bytes<'a>(self) -> &'a [u8] {
        std::slice::from_raw_parts(self.pointer.as_ptr(), self.len)
    }
}

impl Default for FfiSafeStr {
    #[inline(always)]
    fn default() -> Self {
        Self {
            pointer: NonNull::dangling(),
            len: 0,
        }
    }
}

impl<'a> From<&'a str> for FfiSafeStr {
    #[inline(always)]
    fn from(value: &'a str) -> Self {
        Self {
            pointer: as_non_null_ptr(value.as_bytes()),
            len: value.len(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// RegionGuard
////////////////////////////////////////////////////////////////////////////////

// TODO: move to tarantool-module https://git.picodata.io/picodata/picodata/tarantool-module/-/issues/210
pub struct RegionGuard {
    save_point: usize,
}

impl RegionGuard {
    /// TODO
    #[inline(always)]
    pub fn new() -> Self {
        // This is safe as long as the function is called within an initialized
        // fiber runtime
        let save_point = unsafe { ffi::box_region_used() };
        Self { save_point }
    }

    /// TODO
    #[inline(always)]
    pub fn used_at_creation(&self) -> usize {
        self.save_point
    }
}

impl Drop for RegionGuard {
    fn drop(&mut self) {
        // This is safe as long as the function is called within an initialized
        // fiber runtime
        unsafe { ffi::box_region_truncate(self.save_point) }
    }
}

////////////////////////////////////////////////////////////////////////////////
// region allocation
////////////////////////////////////////////////////////////////////////////////

// TODO: move to tarantool module https://git.picodata.io/picodata/picodata/tarantool-module/-/issues/210
/// TODO: doc
#[inline]
fn allocate_on_region(size: usize) -> Result<&'static mut [u8], BoxError> {
    // SAFETY: requires initialized fiber runtime
    let pointer = unsafe { ffi::box_region_alloc(size).cast::<u8>() };
    if pointer.is_null() {
        return Err(BoxError::last());
    }
    // SAFETY: safe because pointer is not null
    let region_slice = unsafe { std::slice::from_raw_parts_mut(pointer, size) };
    Ok(region_slice)
}

// TODO: move to tarantool module https://git.picodata.io/picodata/picodata/tarantool-module/-/issues/210
/// Copies the provided `data` to the current fiber's region allocator returning
/// a reference to the new allocation.
///
/// Use this to return dynamically sized values over the ABI boundary, for
/// example in RPC handlers.
///
/// Note that the returned slice's lifetime is not really `'static`, but is
/// determined by the following call to `box_region_truncate`.
#[inline]
pub fn copy_to_region(data: &[u8]) -> Result<&'static [u8], BoxError> {
    let region_slice = allocate_on_region(data.len())?;
    region_slice.copy_from_slice(data);
    Ok(region_slice)
}

////////////////////////////////////////////////////////////////////////////////
// RegionBuffer
////////////////////////////////////////////////////////////////////////////////

// TODO: move to tarantool module https://git.picodata.io/picodata/picodata/tarantool-module/-/issues/210
/// TODO
pub struct RegionBuffer {
    guard: RegionGuard,

    start: *mut u8,
    count: usize,
}

impl RegionBuffer {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            guard: RegionGuard::new(),
            start: NonNull::dangling().as_ptr(),
            count: 0,
        }
    }

    #[track_caller]
    pub fn push(&mut self, data: &[u8]) -> Result<(), BoxError> {
        let added_count = data.len();
        let new_count = self.count + added_count;
        unsafe {
            let save_point = ffi::box_region_used();
            let pointer: *mut u8 = ffi::box_region_alloc(added_count) as _;

            if pointer.is_null() {
                #[rustfmt::skip]
                return Err(BoxError::new(TarantoolErrorCode::MemoryIssue, format!("failed to allocate {added_count} bytes on the region allocator")));
            }

            if self.start.is_null() || pointer == self.start.add(self.count) {
                // New allocation is contiguous with the previous one
                memcpy(pointer, data.as_ptr(), added_count);
                self.count = new_count;
                if self.start.is_null() {
                    self.start = pointer;
                }
            } else {
                // New allocation is in a different slab, need to reallocate
                ffi::box_region_truncate(save_point);

                let new_count = self.count + added_count;
                let pointer: *mut u8 = ffi::box_region_alloc(new_count) as _;
                memcpy(pointer, self.start, self.count);
                memcpy(pointer.add(self.count), data.as_ptr(), added_count);
                self.start = pointer;
                self.count = new_count;
            }
        }

        Ok(())
    }

    #[inline(always)]
    pub fn get(&self) -> &[u8] {
        if self.start.is_null() {
            // Cannot construct a slice from a null pointer even if len is 0
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.start, self.count) }
        }
    }

    #[inline]
    pub fn into_raw_parts(self) -> (&'static [u8], usize) {
        let save_point = self.guard.used_at_creation();
        std::mem::forget(self.guard);
        if self.start.is_null() {
            // Cannot construct a slice from a null pointer even if len is 0
            return (&[], save_point);
        }
        let slice = unsafe { std::slice::from_raw_parts(self.start, self.count) };
        (slice, save_point)
    }
}

impl std::io::Write for RegionBuffer {
    #[inline(always)]
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if let Err(e) = self.push(data) {
            #[rustfmt::skip]
            return Err(std::io::Error::new(std::io::ErrorKind::OutOfMemory, e.message()));
        }

        Ok(data.len())
    }

    #[inline(always)]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[inline(always)]
unsafe fn memcpy(destination: *mut u8, source: *const u8, count: usize) {
    let to = std::slice::from_raw_parts_mut(destination, count);
    let from = std::slice::from_raw_parts(source, count);
    to.copy_from_slice(from)
}

////////////////////////////////////////////////////////////////////////////////
// DisplayErrorLocation
////////////////////////////////////////////////////////////////////////////////

// TODO: move to taratool-module https://git.picodata.io/picodata/picodata/tarantool-module/-/issues/211
pub struct DisplayErrorLocation<'a>(pub &'a BoxError);

impl std::fmt::Display for DisplayErrorLocation<'_> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some((file, line)) = self.0.file().zip(self.0.line()) {
            write!(f, "{file}:{line}: ")?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////
// DisplayAsHexBytesLimitted
////////////////////////////////////////////////////////////////////////////////

// TODO: move to taratool-module https://git.picodata.io/picodata/picodata/tarantool-module/-/merge_requests/523
pub struct DisplayAsHexBytesLimitted<'a>(pub &'a [u8]);

impl std::fmt::Display for DisplayAsHexBytesLimitted<'_> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.0.len() > 512 {
            f.write_str("<too-big-to-display>")
        } else {
            tarantool::util::DisplayAsHexBytes(self.0).fmt(f)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// miscellaneous
////////////////////////////////////////////////////////////////////////////////

#[inline(always)]
fn as_non_null_ptr<T>(data: &[T]) -> NonNull<T> {
    let pointer = data.as_ptr();
    // SAFETY: slice::as_ptr never returns `null`
    // Also I have to cast to `* mut` here even though we're not going to
    // mutate it, because there's no constructor that takes `* const`....
    unsafe { NonNull::new_unchecked(pointer as *mut _) }
}

////////////////////////////////////////////////////////////////////////////////
// test
////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "internal_test")]
mod test {
    use super::*;

    #[tarantool::test]
    fn region_buffer() {
        #[derive(serde::Serialize, Debug)]
        struct S {
            name: String,
            x: f32,
            y: f32,
            array: Vec<(i32, i32, bool)>,
        }

        let s = S {
            name: "foo".into(),
            x: 4.2,
            y: 6.9,
            array: vec![(1, 2, true), (3, 4, false)],
        };

        let vec = rmp_serde::to_vec(&s).unwrap();
        let mut buffer = RegionBuffer::new();
        rmp_serde::encode::write(&mut buffer, &s).unwrap();
        assert_eq!(vec, buffer.get());
    }
}
