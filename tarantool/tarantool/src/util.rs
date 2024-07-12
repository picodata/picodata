use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::ffi::CString;

pub trait IntoClones<Tuple>: Clone {
    fn into_clones(self) -> Tuple;
}

macro_rules! impl_into_clones {
    // [@clones(self) T (...)] => [(... self,)]
    [@clones($self:ident) $h:ident ($($code:tt)*)] => { ($($code)* $self,) };
    // [@clones(self) T T ... T (...)] => [@clones(self) T ... T (... self.clone(),)]
    [@clones($self:ident) $h:ident $($t:ident)+ ($($code:tt)*)] => {
        impl_into_clones![
            @clones($self) $($t)+ ($($code)* $self.clone(),)
        ]
    };
    {$h:ident $($t:ident)*} => {
        impl<$h: Clone> IntoClones<($h $(, $t)*,)> for $h {
            fn into_clones(self) -> ($h $(, $t)*,) {
                // [@clones(self) T T ... T ()]
                impl_into_clones![@clones(self) $h $($t)* ()]
            }
        }
        impl_into_clones!{$($t)*}
    };
    () => {};
}

impl_into_clones! {T T T T T T T T T T T}

#[macro_export]
macro_rules! tuple_from_box_api {
    ($f:path [ $($args:expr),* , @out ]) => {
        {
            let mut result = ::std::mem::MaybeUninit::uninit();
            #[allow(unused_unsafe)]
            unsafe {
                if $f($($args),*, result.as_mut_ptr()) < 0 {
                    return Err($crate::error::TarantoolError::last().into());
                }
                Ok($crate::tuple::Tuple::try_from_ptr(result.assume_init()))
            }
        }
    }
}

#[macro_export]
macro_rules! expr_count {
    () => { 0 };
    ($head:expr $(, $tail:expr)*) => { 1 + $crate::expr_count!($($tail),*) }
}

/// Return an array reference to the first `N` items in the slice.
/// If the slice is not at least N in length, this will return None.
/// Equivalent to similar slice (as primitive) method (`slice::first_chunk`).
#[inline]
pub const fn slice_first_chunk<const N: usize, T>(slice: &[T]) -> Option<&[T; N]> {
    if slice.len() < N {
        None
    } else {
        // SAFETY: We explicitly check for the correct number
        // of elements, and do not let the reference outlive the slice.
        Some(unsafe { &*(slice.as_ptr().cast::<[T; N]>()) })
    }
}

#[inline]
pub fn rmp_to_vec<T>(val: &T) -> Result<Vec<u8>, Error>
where
    T: Serialize + ?Sized,
{
    Ok(rmp_serde::to_vec(val)?)
}

#[derive(Clone, Debug, Serialize, Deserialize, tlua::Push, tlua::LuaRead, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub enum NumOrStr {
    Num(u32),
    // TODO(gmoshkin): this should be a `&str` instead, but
    // `#[derive(tlua::Push)]` doesn't support generic parameters yet
    Str(String),
}

impl Default for NumOrStr {
    fn default() -> Self {
        Self::Num(0)
    }
}

impl From<u32> for NumOrStr {
    #[inline(always)]
    fn from(n: u32) -> Self {
        Self::Num(n)
    }
}

impl From<String> for NumOrStr {
    #[inline(always)]
    fn from(s: String) -> Self {
        Self::Str(s)
    }
}

impl From<NumOrStr> for String {
    #[inline(always)]
    fn from(s: NumOrStr) -> Self {
        match s {
            NumOrStr::Str(s) => s,
            NumOrStr::Num(n) => n.to_string(),
        }
    }
}

impl<'a> From<&'a str> for NumOrStr {
    #[inline(always)]
    fn from(s: &'a str) -> Self {
        Self::Str(s.into())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum Value<'a> {
    Num(u32),
    Double(f64),
    Str(Cow<'a, str>),
    Bool(bool),
}

impl std::hash::Hash for Value<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Num(v) => v.hash(state),
            Self::Double(v) => v.to_bits().hash(state),
            Self::Str(v) => v.hash(state),
            Self::Bool(v) => v.hash(state),
        }
    }
}

impl Eq for Value<'_> {}

#[rustfmt::skip]
impl From<bool> for Value<'_> { fn from(v: bool) -> Self { Self::Bool(v) } }
#[rustfmt::skip]
impl From<u32> for Value<'_> { fn from(v: u32) -> Self { Self::Num(v) } }
#[rustfmt::skip]
impl From<f64> for Value<'_> { fn from(v: f64) -> Self { Self::Double(v) } }
#[rustfmt::skip]
impl From<String> for Value<'_> { fn from(v: String) -> Self { Self::Str(v.into()) } }
#[rustfmt::skip]
impl<'s> From<&'s str> for Value<'s> { fn from(v: &'s str) -> Self { Self::Str(v.into()) } }

#[macro_export]
macro_rules! unwrap_or {
    ($o:expr, $else:expr) => {
        if let Some(v) = $o {
            v
        } else {
            $else
        }
    };
}

#[macro_export]
macro_rules! unwrap_ok_or {
    ($o:expr, $err:pat => $($else:tt)+) => {
        match $o {
            Ok(v) => v,
            $err => $($else)+,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// DisplayAsHexBytes
////////////////////////////////////////////////////////////////////////////////

/// A wrapper for displaying byte slices as hexadecimal byte slice literals.
/// ```no_run
/// # use tarantool::util::DisplayAsHexBytes;
/// let s = format!("{}", DisplayAsHexBytes(&[1, 2, 3]));
/// assert_eq!(s, r#"b"\x01\x02\x03""#);
/// ```
pub struct DisplayAsHexBytes<'a>(pub &'a [u8]);

impl std::fmt::Display for DisplayAsHexBytes<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "b\"")?;
        for byte in self.0 {
            if matches!(byte, b' '..=b'~') {
                if matches!(byte, b'\\' | b'"') {
                    write!(f, "\\")?;
                }
                write!(f, "{}", *byte as char)?;
            } else {
                write!(f, "\\x{byte:02x}")?;
            }
        }
        write!(f, "\"")?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////
// DisplayAsMPValue
////////////////////////////////////////////////////////////////////////////////

pub(crate) struct DebugAsMPValue<'a>(pub &'a [u8]);

impl std::fmt::Debug for DebugAsMPValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut read = self.0;
        match rmp_serde::from_read::<_, rmpv::Value>(&mut read) {
            Ok(v) => write!(f, "{:?}", v),
            Err(_) => write!(f, "{:?}", self.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// str_eq
////////////////////////////////////////////////////////////////////////////////

/// Compares strings for equality.
///
/// Works at compile time unlike [`std::cmp::Eq`].
pub const fn str_eq(lhs: &str, rhs: &str) -> bool {
    let lhs = lhs.as_bytes();
    let rhs = rhs.as_bytes();
    if lhs.len() != rhs.len() {
        return false;
    }
    let mut i = 0;
    loop {
        if i == lhs.len() {
            return true;
        }
        if lhs[i] != rhs[i] {
            return false;
        }
        i += 1;
    }
}

////////////////////////////////////////////////////////////////////////////////
// to_cstring
////////////////////////////////////////////////////////////////////////////////

/// Convert `s` to a `CString` replacing any nul-bytes with `'�'` symbols.
///
/// Use this function when you need to unconditionally convert a rust string to
/// a c string without failing for any reason (other then out-of-memory), for
/// example when converting error messages.
#[inline(always)]
pub fn to_cstring_lossy(s: &str) -> CString {
    into_cstring_lossy(s.into())
}

/// Convert `s` into a `CString` replacing any nul-bytes with `'�'` symbols.
///
/// Use this function when you need to unconditionally convert a rust string to
/// a c string without failing for any reason (other then out-of-memory), for
/// example when converting error messages.
#[inline]
pub fn into_cstring_lossy(s: String) -> CString {
    match CString::new(s) {
        Ok(cstring) => cstring,
        Err(e) => {
            // Safety: the already Vec was a String a moment earlier
            let s = unsafe { String::from_utf8_unchecked(e.into_vec()) };
            // The same character String::from_utf8_lossy uses to replace non-utf8 bytes
            let s = s.replace('\0', "�");
            // Safety: s no longer contains any nul bytes.
            unsafe { CString::from_vec_unchecked(s.into()) }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// test
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    #[cfg(miri)]
    fn check_slice_first_chunk() {
        let data = &[1u8, 2, 3, 4];
        assert_eq!(slice_first_chunk::<2, _>(data), Some(&[1, 2]));
        assert_eq!(slice_first_chunk::<5, _>(data), None);
    }

    #[test]
    #[allow(clippy::needless_range_loop)]
    fn display_as_hex_bytes() {
        let mut buf = [0_u8; 256];
        for i in 0..256 {
            buf[i] = i as _;
        }

        let s = format!("{}", DisplayAsHexBytes(&buf));
        // Copy paste this into rust (or python) source to make sure it compiles.
        assert_eq!(s, r###"
b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\x7f\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f\x90\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f\xa0\xa1\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf\xb0\xb1\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf\xc0\xc1\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xdb\xdc\xdd\xde\xdf\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xeb\xec\xed\xee\xef\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff"
        "###.trim());
    }

    #[rustfmt::skip]
    #[test]
    fn check_to_cstring_lossy() {
        let message = String::from("hell\0 w\0rld\0");
        assert!(message.as_bytes().contains(&0));
        assert_eq!(to_cstring_lossy(&message).as_ref(), crate::c_str!("hell� w�rld�"));

        assert_eq!(into_cstring_lossy(message).as_ref(), crate::c_str!("hell� w�rld�"));
    }
}
