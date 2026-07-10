use crate::ffi::lua::lua_State;
use std::os::raw::{c_char, c_int};

/// MessagePack extension subtype for the native SQL JSON type.
pub const MP_JSON: i8 = 20;

crate::define_dlsym_reloc! {
    /// Check one JSON inner value, with all reads bounded by `len`.
    /// Returns 0 for normal form, 1 for a valid JSON value that needs
    /// normalization, or 2 for invalid JSON. `err_off` may be null.
    pub fn tnt_mp_verify_json(data: *const c_char, len: u32, err_off: *mut u32) -> c_int;

    /// Parse `[text, text + len)` as a JSON document into a normalized inner
    /// value (plain MessagePack, without the MP_EXT/MP_JSON header), allocated
    /// on the box region. On success the result length is written to `out_len`
    /// and a pointer to the bytes is returned; the caller reclaims the region
    /// from a savepoint taken before the call. On a syntax or structural error
    /// NULL is returned with a diag set (see [`box_error_last`]).
    ///
    /// The grammar is strict JSON and numbers follow Tarantool SQL's
    /// numeric-literal rule (dotless integer to MP_UINT/MP_INT, fraction to
    /// MP_DECIMAL, exponent to MP_DOUBLE). This is the same routine storage's
    /// text-to-json cast uses, so a value parsed here is byte-identical to one
    /// storage produces for the same text.
    ///
    /// [`box_error_last`]: crate::ffi::tarantool::box_error_last
    pub fn tnt_json_parse(text: *const c_char, len: u32, out_len: *mut u32) -> *mut c_char;

    /// Push a JSON value onto the Lua stack as the SQL JSON cdata. `data` is the
    /// inner value (plain MessagePack, without the MP_EXT/MP_JSON header) and is
    /// copied into the cdata. It must be in normal form: Lua code trusts a JSON
    /// cdata's bytes and nothing checks them here.
    #[allow(non_snake_case)]
    pub fn luaT_pushjson(l: *mut lua_State, data: *const c_char, len: u32);

    /// Print a JSON inner value's canonical text representation into a buffer,
    /// snprintf-style: writes at most `size` bytes (including a trailing NUL),
    /// and always returns the number of bytes the full, unclipped output would
    /// take, so calling once with `buf = NULL, size = 0` gives the required
    /// buffer size. `data` is advanced past the consumed bytes; pass a fresh
    /// pointer to the start of the value on each call.
    ///
    /// Correctly renders values `serde_json`-over-`rmpv` can't: an embedded
    /// MP_DECIMAL (from a JSON fraction literal) becomes a JSON number, not a
    /// raw ext-type array.
    ///
    /// The value must be in normal form, see [`crate::json`]. Reads never go
    /// past `len`, also for a value that is not in normal form.
    ///
    /// @retval >=0 the number of bytes that were (or would have been) written.
    /// @retval <0  `data` could not be decoded as a JSON inner value.
    pub fn tnt_mp_snprint_json(buf: *mut c_char, size: c_int, data: *mut *const c_char, len: u32) -> c_int;

    /// Compare two JSON inner values the way storage and SQL do, in
    /// PostgreSQL JSONB order: null < string < number < bool < array < object,
    /// numbers by value across integer, decimal and double.
    ///
    /// Both values must be in normal form, see [`crate::json`].
    ///
    /// Returns <0, 0 or >0 as `a` is less than, equal to or greater than `b`.
    pub fn tnt_mp_compare_json(a: *const c_char, a_len: u32, b: *const c_char, b_len: u32) -> c_int;
}
