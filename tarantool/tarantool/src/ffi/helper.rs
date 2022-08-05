use dlopen::symbor::Library;

use std::ffi::CStr;

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
                    let impl_fn: SymType = $crate::ffi::helper::get_symbol(sym_name)
                        .unwrap();
                    RELOC_FN = impl_fn;
                    RELOC_FN($($args),*)
                }
            }
        )+
    };
}

#[inline]
pub fn has_symbol(name: &CStr) -> bool {
    check_symbol(name).is_ok()
}

#[inline]
pub fn check_symbol(name: &CStr) -> Result<(), dlopen::Error> {
    let lib = Library::open_self()?;
    let _sym = unsafe { lib.symbol_cstr::<*const ()>(name) }?;
    Ok(())
}

#[inline]
pub fn get_symbol<T: Copy>(name: &CStr) -> Result<T, dlopen::Error> {
    let lib = Library::open_self()?;
    let sym = unsafe { lib.symbol_cstr(name)? };
    Ok(*sym)
}

#[inline]
pub fn get_symbol_or_warn<T: Copy>(name: &CStr) -> Option<T> {
    get_symbol(name)
        .map_err(|e|
            crate::log::say(
                crate::log::SayLevel::Warn,
                file!(),
                line!() as _,
                Some(&e.to_string()),
                &format!("call to {name:?} failed"),
            )
        )
        .ok()
}
