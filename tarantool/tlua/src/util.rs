pub fn hash(s: &str) -> u32 {
    // https://github.com/LuaJIT/LuaJIT/blob/1d7b5029c5ba36870d25c67524034d452b761d27/src/lj_str.c#L76
    let mut h = s.len() as u32;
    let mut a: u32;
    let mut b: u32;
    let len = s.len();
    let s = s.as_ptr();
    unsafe {
        match len {
            0 => return 0,
            1..=3 => {
                a = *s as _;
                h ^= *s.add(len - 1) as u32;
                b = *s.add(len >> 1) as _;
                h ^= b;
                h = h.wrapping_sub(b.rotate_left(14));
            }
            _ => {
                a = s.cast::<u32>().read_unaligned();
                h ^= s.add(len - 4).cast::<u32>().read_unaligned();
                b = s
                    .add((len >> 1).wrapping_sub(2))
                    .cast::<u32>()
                    .read_unaligned();
                h ^= b;
                h = h.wrapping_sub(b.rotate_left(14));
                b += s
                    .add((len >> 2).wrapping_sub(1))
                    .cast::<u32>()
                    .read_unaligned();
            }
        }
    }
    a ^= h;
    a = a.wrapping_sub(h.rotate_left(11));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(25));
    h ^= b;
    h = h.wrapping_sub(b.rotate_left(16));
    h
}

/// Convert `s` into a `CString` replacing any nul-bytes with `'�'` symbols.
///
/// Use this function when you need to unconditionally convert a rust string to
/// a c string without failing for any reason (other then out-of-memory), for
/// example when converting error messages.
pub fn into_cstring_lossy(s: String) -> std::ffi::CString {
    match std::ffi::CString::new(s) {
        Ok(cstring) => cstring,
        Err(e) => {
            // Safety: the already Vec was a String a moment earlier
            let s = unsafe { String::from_utf8_unchecked(e.into_vec()) };
            // The same character String::from_utf8_lossy uses to replace non-utf8 bytes
            let s = s.replace('\0', "�");
            // Safety: s no longer contains any nul bytes.
            unsafe { std::ffi::CString::from_vec_unchecked(s.into()) }
        }
    }
}
