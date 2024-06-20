use abi_stable::StableAbi;
use std::ptr::NonNull;

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
