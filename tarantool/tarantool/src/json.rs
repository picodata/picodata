//! The SQL JSON type: parse, validate, render and compare its values.
//!
//! # Normal form
//! [`parse`] produces normalized JSON; [`is_normalized`] checks an inner value.
//! [`to_string`] and [`compare`] require normalized input, such as a value
//! produced by [`parse`] or read from storage. Their C implementations assert
//! this precondition in debug Tarantool builds and trust it in release builds.
//!
//! Check [`crate::ffi::has_json`] before you call a function of this module.

use std::os::raw::{c_char, c_int};

use crate::error::BoxError;
use crate::ffi::json as ffi;
#[cfg(not(feature = "standalone_decimal"))]
use crate::ffi::tarantool::{box_region_truncate, box_region_used};

/// Whether `inner` is exactly one JSON value in normal form.
///
/// Accepts untrusted bytes and checks their encoding, object key order,
/// allowed value kinds, UTF-8 and nesting depth. Does not modify the input,
/// allocate or access the Tarantool region.
pub fn is_normalized(inner: &[u8]) -> bool {
    let Ok(len) = u32::try_from(inner.len()) else {
        return false;
    };
    // SAFETY: the validator bounds-checks all reads within `inner` and does
    // not require the input to be in normal form. The error offset is optional.
    unsafe { ffi::tnt_mp_verify_json(inner.as_ptr().cast(), len, std::ptr::null_mut()) == 0 }
}

/// Render a JSON inner value as canonical JSON text: the same format
/// storage's own printer produces. `inner` is a single plain MessagePack
/// value (the MP_JSON ext payload, without the ext header).
///
/// Unlike walking the decoded value with a generic MessagePack-to-JSON
/// serializer (e.g. `serde_json` over an `rmpv::Value`), this understands
/// Tarantool's JSON encoding of numbers: a value nested from a JSON fraction
/// literal is stored as an MP_DECIMAL extension, which a generic serializer
/// has no way to know is a number rather than opaque ext bytes. This calls
/// into the same C renderer storage uses, so it's rendered correctly.
///
/// `inner` must be in normal form, see the [module docs](self).
///
/// Returns `None` if `inner` isn't a well-formed value the renderer can
/// decode.
pub fn to_string(inner: &[u8]) -> Option<String> {
    // The text is rarely more than twice as long as the MessagePack, so one
    // call to the renderer is usually enough.
    let mut buf = Vec::with_capacity(inner.len() * 2 + 16);
    let mut len = snprint(inner, &mut buf)?;
    if len >= buf.capacity() {
        buf.reserve_exact(len + 1);
        len = snprint(inner, &mut buf)?;
        if len >= buf.capacity() {
            return None;
        }
    }
    // SAFETY: `snprint` wrote `len` bytes, and `len < buf.capacity()`.
    unsafe { buf.set_len(len) };
    String::from_utf8(buf).ok()
}

/// Render `inner` into the capacity of `buf`. Returns the full length of the
/// text. It fits only if it is less than the capacity, because
/// `tnt_mp_snprint_json` is snprintf-style and always writes a trailing NUL.
fn snprint(inner: &[u8], buf: &mut Vec<u8>) -> Option<usize> {
    debug_assert!(inner.len() <= u32::MAX as usize);
    let mut cursor = inner.as_ptr() as *const c_char;
    let len = unsafe {
        ffi::tnt_mp_snprint_json(
            buf.as_mut_ptr() as *mut c_char,
            buf.capacity() as c_int,
            &mut cursor,
            inner.len() as u32,
        )
    };
    usize::try_from(len).ok()
}

/// Compare two JSON inner values the way storage and SQL do: PostgreSQL JSONB
/// order, with numbers compared by value whether they are stored as an
/// integer, a decimal or a double. This is the comparator storage itself
/// uses, so the result here is the result there.
///
/// Both values must be in normal form, see the [module docs](self).
pub fn compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    debug_assert!(a.len() <= u32::MAX as usize && b.len() <= u32::MAX as usize);
    let res = unsafe {
        ffi::tnt_mp_compare_json(
            a.as_ptr() as *const c_char,
            a.len() as u32,
            b.as_ptr() as *const c_char,
            b.len() as u32,
        )
    };
    res.cmp(&0)
}

/// Parse JSON `text` into a normalized inner value (the MP_JSON ext payload,
/// without the ext header), using Tarantool's strict SQL JSON parser. This is
/// the same routine storage's text-to-json cast uses, so the bytes returned
/// here are identical to what storage would produce for the same text. Returns
/// the parse error as a [`BoxError`] on invalid JSON, an over-range number, or
/// nesting that is too deep.
///
/// Must be called from the tx thread of a tarantool runtime.
#[cfg(not(feature = "standalone_decimal"))]
pub fn parse(text: &[u8]) -> Result<Vec<u8>, BoxError> {
    debug_assert!(text.len() <= u32::MAX as usize);
    let svp = unsafe { box_region_used() };
    let mut out_len: u32 = 0;
    let ptr = unsafe {
        ffi::tnt_json_parse(
            text.as_ptr() as *const c_char,
            text.len() as u32,
            &mut out_len,
        )
    };
    if ptr.is_null() {
        return Err(BoxError::last());
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, out_len as usize) };
    let bytes = bytes.to_vec();
    unsafe { box_region_truncate(svp) };
    Ok(bytes)
}

/// Under `standalone_decimal` this crate links into a plain test binary, where
/// the strict `tnt_json_parse` is unusable: its result lives on the box region,
/// and the `box_region_*` calls that reclaim it are hard extern symbols no such
/// binary provides. So parsing always fails there.
#[cfg(feature = "standalone_decimal")]
pub fn parse(_text: &[u8]) -> Result<Vec<u8>, BoxError> {
    Err(BoxError::new(
        crate::error::TarantoolErrorCode::Unsupported,
        "JSON parsing needs a tarantool runtime",
    ))
}

// Every test needs the strict `tnt_json_parse`, which `standalone_decimal`
// replaces.
#[cfg(all(feature = "internal_test", not(feature = "standalone_decimal")))]
mod tests {
    use super::*;

    fn mp(text: &str) -> Vec<u8> {
        let value: rmpv::Value = serde_json::from_str(text).unwrap();
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &value).unwrap();
        buf
    }

    #[crate::test(tarantool = "crate")]
    fn json_parse_normalizes_and_rejects_invalid() {
        // Parsing normalizes: keys sort length-first, duplicates keep the last.
        assert_eq!(
            parse(br#"{"bb":1,"a":2,"a":3}"#).unwrap(),
            mp(r#"{"a":3,"bb":1}"#)
        );

        // A dotless integer stays an integer in canonical msgpack.
        assert_eq!(parse(b"42").unwrap(), mp("42"));

        // Invalid JSON is rejected rather than wrapped verbatim.
        assert!(parse(b"abc").is_err());
        assert!(parse(br#"{"a":1"#).is_err());
    }

    #[crate::test(tarantool = "crate")]
    fn json_to_string_renders_decimals_and_doubles() {
        // A fraction literal becomes a decimal-typed number under the hood
        // (see `parse`'s docs), preserving its written scale; it must still
        // render as a plain JSON number, not the raw ext bytes a generic
        // serde_json walk would produce.
        assert_eq!(to_string(&parse(b"42.0").unwrap()).unwrap(), "42.0");
        // An exponent literal becomes a double, printed without a forced
        // decimal point; same requirement not to leak raw ext bytes.
        assert_eq!(to_string(&parse(b"4.2e1").unwrap()).unwrap(), "42");
        assert_eq!(
            to_string(&parse(br#"{"a":1.5,"b":[1,2.5,"x"]}"#).unwrap()).unwrap(),
            r#"{"a": 1.5, "b": [1, 2.5, "x"]}"#
        );
    }

    #[crate::test(tarantool = "crate")]
    fn json_compare_matches_storage_order() {
        use std::cmp::Ordering::{Equal, Greater, Less};

        let cmp = |a: &[u8], b: &[u8]| compare(&parse(a).unwrap(), &parse(b).unwrap());

        // Numbers compare by value whichever way they are stored: an integer,
        // a decimal and a double.
        assert_eq!(cmp(b"42", b"42.0"), Equal);
        assert_eq!(cmp(b"42", b"4.2e1"), Equal);
        assert_eq!(cmp(b"42.0", b"4.2e1"), Equal);
        assert_eq!(cmp(b"1", b"2"), Less);
        assert_eq!(cmp(b"2.5", b"2"), Greater);
        assert_eq!(cmp(br#"{"a":42}"#, br#"{"a":42.0}"#), Equal);

        // Classes rank as in PostgreSQL jsonb.
        let ranked: [&[u8]; 6] = [b"null", br#""x""#, b"1", b"true", b"[]", b"{}"];
        for pair in ranked.windows(2) {
            assert_eq!(cmp(pair[0], pair[1]), Less);
            assert_eq!(cmp(pair[1], pair[0]), Greater);
        }
    }
}
