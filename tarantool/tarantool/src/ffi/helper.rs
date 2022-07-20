use dlopen::symbor::Library;

#[macro_export]
macro_rules! c_str {
    ($s:literal) => {
        ::std::ffi::CStr::from_bytes_with_nul_unchecked(
            ::std::concat!($s, "\0").as_bytes()
        )
    };
}

#[macro_export]
macro_rules! c_ptr {
    ($s:literal) => {
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

                type SymType = fn($($args: $types),*) $(-> $ret)?;
                static mut RELOC_FN: SymType = init;

                fn init($($args: $types),*) $(-> $ret)? {
                    let sym_name = ::std::stringify!($sym);
                    let impl_fn: SymType = $crate::ffi::helper::get_symbol(sym_name)
                        .unwrap();
                    unsafe {
                        RELOC_FN = impl_fn;
                        RELOC_FN($($args),*)
                    }
                }
            }
        )+
    };
}

#[inline]
pub fn has_symbol(name: &str) -> bool {
    check_symbol(name).is_ok()
}

#[inline]
pub fn check_symbol(name: &str) -> Result<(), dlopen::Error> {
    let lib = Library::open_self()?;
    let _sym = unsafe { lib.symbol::<*const ()>(name) }?;
    Ok(())
}

#[inline]
pub fn get_symbol<T: Copy>(name: &str) -> Result<T, dlopen::Error> {
    let lib = Library::open_self()?;
    let sym = unsafe { lib.symbol(name)? };
    Ok(*sym)
}

#[inline]
pub fn get_symbol_or_warn<T: Copy>(name: &str) -> Option<T> {
    get_symbol(name)
        .map_err(|e|
            crate::log::say(
                crate::log::SayLevel::Warn,
                file!(),
                line!() as _,
                Some(&e.to_string()),
                &format!("call to `{name}` failed"),
            )
        )
        .ok()
}
