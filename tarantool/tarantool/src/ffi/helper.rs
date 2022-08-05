use dlopen::symbor::Library;

use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr::NonNull;

#[macro_export]
macro_rules! c_str {
    ($s:expr) => {
        ::std::ffi::CStr::from_bytes_with_nul_unchecked(
            ::std::concat!($s, "\0").as_bytes()
        )
    };
}

#[macro_export]
macro_rules! c_ptr {
    ($s:expr) => {
        $crate::c_str!($s).as_ptr()
    };
}

#[macro_export]
macro_rules! define_dlsym_reloc {
    (
        $(
            $(#[$meta:meta])*
            pub fn $sym:ident ( $( $args:ident: $types:ty ),* $(,)? ) $( -> $ret:ty )?;
        )+
    ) => {
        $(
            $(#[$meta])*
            #[inline(always)]
            pub unsafe fn $sym($($args: $types),*) $(-> $ret)? {
                return RELOC_FN($($args),*);

                type SymType = unsafe fn($($args: $types),*) $(-> $ret)?;
                static mut RELOC_FN: SymType = init;

                unsafe fn init($($args: $types),*) $(-> $ret)? {
                    let sym_name = $crate::c_str!(::std::stringify!($sym));
                    let impl_fn: SymType = $crate::ffi::helper::get_any_symbol(sym_name)
                        .unwrap();
                    RELOC_FN = impl_fn;
                    RELOC_FN($($args),*)
                }
            }
        )+
    };
}

/// Find a symbol using the `tnt_internal_symbol` api.
///
/// This function performs a slow search over all the exported internal
/// tarantool symbols, so don't use it everytime you want to call a given
/// function.
#[inline]
pub unsafe fn tnt_internal_symbol<T>(name: &CStr) -> Option<T> {
    if std::mem::size_of::<T>() != std::mem::size_of::<*mut ()>() {
        return None
    }
    let ptr = (RELOC_FN?)(name.as_ptr())?;
    return Some(std::mem::transmute_copy(&ptr));

    type SymType = unsafe fn(*const c_char) -> Option<NonNull<()>>;
    static mut RELOC_FN: Option<SymType> = Some(init);

    unsafe fn init(name: *const c_char) -> Option<NonNull<()>> {
        let lib = Library::open_self().ok()?;
        match lib.symbol_cstr(c_str!("tnt_internal_symbol")) {
            Ok(sym) => {
                RELOC_FN = Some(*sym);
                (RELOC_FN.unwrap())(name)
            }
            Err(_) => {
                RELOC_FN = None;
                None
            }
        }
    }
}

/// Check if symbol can be found in the current executable using dlsym.
#[inline]
pub unsafe fn has_dyn_symbol(name: &CStr) -> bool {
    get_dyn_symbol::<*const ()>(name).is_ok()
}

/// Find a sybmol in the current executable using dlsym.
#[inline]
pub unsafe fn get_dyn_symbol<T: Copy>(name: &CStr) -> Result<T, dlopen::Error> {
    let lib = Library::open_self()?;
    let sym = lib.symbol_cstr(name)?;
    Ok(*sym)
}

/// Find a symbol either using the `tnt_internal_symbol` api or using dlsym as a
/// fallback.
#[inline]
pub unsafe fn get_any_symbol<T: Copy>(name: &CStr) -> Result<T, dlopen::Error> {
    if let Some(sym) = tnt_internal_symbol(name) {
        return Ok(sym)
    }
    let lib = Library::open_self()?;
    let sym = lib.symbol_cstr(name)?;
    Ok(*sym)
}
