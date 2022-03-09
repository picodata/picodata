/// Rust version of C's `va_list` type from the `stdarg.h` header
#[repr(C)]
pub struct VaList {
    internal: imp::VaList,
}

/// Core type as passed though the FFI
impl VaList {
    /// Read a value from the VaList.
    ///
    /// Users should take care that they are reading the correct type
    pub unsafe fn get<T: VaPrimitive>(&mut self) -> T {
        T::get(&mut self.internal)
    }
}

/// Trait implemented on types that can be read from a va_list
pub trait VaPrimitive: 'static {
    #[doc(hidden)]
    unsafe fn get(list: &mut imp::VaList) -> Self;
}

mod imp {
    use std::ffi::c_void;
    use super::VaPrimitive;

    #[repr(transparent)]
    pub struct VaList(VaListInner);

    #[repr(transparent)]
    pub struct VaListInner {
        ptr: *mut c_void,
    }

    impl VaListInner {
        unsafe fn get<T>(&mut self) -> T {
            let res = std::ptr::read(self.ptr as _);
            self.ptr = self.ptr.add(8);
            res
        }
    }

    impl<T: 'static> VaPrimitive for *const T {
        unsafe fn get(list: &mut VaList) -> Self {
            list.0.get()
        }
    }

    macro_rules! impl_va_prim {
        ($($t:ty),+) => {
            $(
                impl VaPrimitive for $t {
                    unsafe fn get(list: &mut VaList) -> Self {
                        list.0.get()
                    }
                }
            )+
        };
    }

    impl_va_prim!{ usize, isize, u64, i64, u32, i32, f64, f32 }
}
