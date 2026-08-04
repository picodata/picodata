use std::{
    cell::RefCell,
    ffi::{c_char, CStr},
    io::Write as _,
};

thread_local! {
    static SYMBOL_NAME_BUFFER: RefCell<[u8; 1024]> = const { RefCell::new([0; _]) };
}

/// Replace tarantool's symbol name demangler (a weak symbol) using this impl (a strong symbol).
/// The returned cstring is statically allocated in a thread-local buffer
/// and will remain valid until the next time the function is called.
#[no_mangle]
extern "C" fn tnt_abi_demangle(mangled_name: *const c_char) -> *const c_char {
    let mangled_name = match unsafe { CStr::from_ptr(mangled_name) }.to_str() {
        Err(_) => return mangled_name,
        Ok(x) => x,
    };

    let name = symbolic_demangle::demangle(mangled_name);

    SYMBOL_NAME_BUFFER.with(|buffer| {
        let mut guard = buffer.borrow_mut();
        let mut buf = &mut guard[..];

        // XXX: the string should be null-terminated.
        let _ = write!(buf, "{name}\0");

        // For simplicity, always write a null character.
        let end = guard.len() - 1;
        guard[end] = 0;

        guard.as_ptr() as _
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        ffi::{CStr, CString},
        str::FromStr,
    };

    fn try_demangle(name: &str) -> String {
        let name = CString::from_str(name).unwrap();
        let ptr = tnt_abi_demangle(name.as_ptr());
        unsafe { CStr::from_ptr(ptr) }.to_str().unwrap().to_owned()
    }

    #[test]
    fn check_demangle() {
        let res = try_demangle("");
        assert_eq!(res, "");

        let large_input: String = std::iter::repeat_n('a', 16384).collect();
        let res = try_demangle(&large_input);
        assert!(large_input.starts_with(res.as_str()));

        // C
        let res = try_demangle("foobar");
        insta::assert_snapshot!(res, @"foobar");

        // C++
        let res = try_demangle("_ZZN3icu13ICUServiceKey16getStaticClassIDEvE7classID");
        insta::assert_snapshot!(res, @"icu::ICUServiceKey::getStaticClassID()::classID");

        // C++
        let res = try_demangle("_ZZN3icu12_GLOBAL__N_113AliasReplacer7replaceERKNS_6LocaleERNS_10CharStringER10UErrorCodeENUlPvE_4_FUNES9_");
        insta::assert_snapshot!(res, @"icu::(anonymous namespace)::AliasReplacer::replace(icu::Locale const&, icu::CharString&, UErrorCode&)::{lambda(void*)#1}::_FUN(void*)");

        // Rust
        let res = try_demangle("_RNvXsq_NtNtCsJjg3s9kj1t_8picodata7pgproto5errorNtB5_13PedanticErrorNtNtCscI6d9CVNmLh_4core3fmt7Display3fmt");
        insta::assert_snapshot!(res, @"<picodata::pgproto::error::PedanticError as core::fmt::Display>::fmt");

        // Rust
        let res = try_demangle("_RNvXsp_NtCscI6d9CVNmLh_4core6resultINtB5_6ResultTINtNtNtCsJjg3s9kj1t_8picodata7pgproto6stream8PgStreamNtNtCs5QvSrEf57G0_9tarantool4coio10CoIOStreamENtNtNtBQ_6client7startup12ClientParamsENtNtBQ_5error7PgErrorENtNtNtB7_3ops9try_trait3Try6branchBS_");
        insta::assert_snapshot!(res, @"<core::result::Result<(picodata::pgproto::stream::PgStream<tarantool::coio::CoIOStream>, picodata::pgproto::client::startup::ClientParams), picodata::pgproto::error::PgError> as core::ops::try_trait::Try>::branch");
    }
}
