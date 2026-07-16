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
