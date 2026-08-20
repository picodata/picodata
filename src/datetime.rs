use sql::frontend::sql::try_parse_datetime;
use std::ffi::c_char;
use tarantool::ffi::datetime::datetime;

/// Replace tarantool's datetime parser.
///
/// Tarantool calls it to cast strings to datetime in SQL and in Lua's `datetime.parse()`,
/// so the text is parsed with [`try_parse_datetime`] there too, the same as on the router.
///
/// Returns `len` if the whole text is a datetime, and -1 otherwise.
#[no_mangle]
extern "C" fn datetime_parse_full(date: *mut datetime, str: *const c_char, len: usize) -> isize {
    if len == 0 {
        return -1;
    }
    // SAFETY: tarantool passes a buffer of `len` bytes, not necessarily 0-terminated.
    let bytes = unsafe { std::slice::from_raw_parts(str.cast::<u8>(), len) };
    let Some(parsed) = std::str::from_utf8(bytes).ok().and_then(try_parse_datetime) else {
        return -1;
    };

    // SAFETY: tarantool passes a valid pointer to the output value.
    unsafe { date.write(parsed.as_ffi_dt()) };
    len as isize
}
