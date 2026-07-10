//! JSON value module.

use std::cmp::Ordering;
use std::fmt::{self, Display};
use std::io::Write;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tarantool::ffi::json::MP_JSON;
use tarantool::msgpack::{Context, Encode, EncodeError, ExtStruct};

/// A JSON document, held as the bytes of one plain MessagePack value in
/// Tarantool's normal form.
///
/// What is stored here is the document alone. The `MP_JSON` extension header
/// is added on encode and stripped on decode, so the value seen at runtime is
/// always the bare content, never the wrapped form.
///
/// The bytes are kept as they are, not decoded into a tree: the router never
/// looks inside a document. It encodes, hashes, compares and prints it, and
/// each of those works on the bytes directly.
///
/// [`PartialEq`], [`Hash`] and [`Ord`] compare those bytes. That is identity,
/// not SQL equality: `42` and `42.0` are two different documents here, which
/// is what a hash map or a sort of plan constants needs, as one must never be
/// swapped for the other. SQL comparison is [`Json::value_cmp`], which asks the
/// same comparator storage uses.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Json(Box<[u8]>);

impl Json {
    /// Parse JSON `text` into a `Json` with the same strict parser storage
    /// uses (`tnt_json_parse`), so a folded constant is byte-identical to the
    /// value storage stores for the same text, and invalid JSON is rejected
    /// here exactly as storage rejects it.
    ///
    /// # Errors
    /// The parser's message if the text is not valid JSON. It always fails
    /// outside a tarantool runtime (unit tests).
    pub fn parse_text(text: &str) -> Result<Json, String> {
        let bytes = tarantool::json::parse(text.as_bytes()).map_err(|e| e.message().to_owned())?;
        Ok(Json(bytes.into()))
    }

    /// Wrap an `MP_JSON` payload taken off the wire or out of storage. It
    /// must be in normal form already. Debug builds assert this with Tarantool's
    /// JSON validator; release builds check the MessagePack structure.
    /// An owned buffer moves in without a copy.
    ///
    /// # Errors
    /// In release builds, `inner` is not one well-formed MessagePack value.
    ///
    /// # Panics
    /// In debug builds, if `inner` is not in normal form.
    pub fn from_inner(inner: impl Into<Box<[u8]>>) -> Result<Json, String> {
        let inner = inner.into();
        // The debug assertion checks both JSON normal form and that `inner`
        // contains exactly one well-formed MessagePack value.
        debug_assert!(
            tarantool::json::is_normalized(&inner),
            "JSON payload is not in normal form"
        );
        // Without debug assertions, check the MessagePack structure and return
        // errors for malformed payloads. Normal form is a caller precondition.
        // Debug builds skip this block because the validator above already
        // checks the structure.
        #[cfg(not(debug_assertions))]
        {
            let mut cursor = std::io::Cursor::new(&*inner);
            tarantool::msgpack::skip_value(&mut cursor).map_err(|e| e.to_string())?;
            if cursor.position() != inner.len() as u64 {
                return Err("trailing bytes after the json value".into());
            }
        }
        Ok(Json(inner))
    }

    /// The `MP_JSON` payload: the document as plain MessagePack.
    pub fn as_inner(&self) -> &[u8] {
        &self.0
    }

    /// Compare two documents the way SQL does on storage: PostgreSQL JSONB
    /// order, numbers by value whichever way they are stored. This calls the
    /// comparator storage itself uses, so the router gets the same answer.
    ///
    /// Outside a tarantool runtime (unit tests) the comparator is not linked
    /// and documents compare by bytes.
    pub fn value_cmp(&self, other: &Json) -> Ordering {
        if !tarantool::ffi::has_json() {
            return self.cmp(other);
        }
        // Every `Json` is in normal form once tarantool is linked, see
        // `Display` below.
        tarantool::json::compare(&self.0, &other.0)
    }
}

impl fmt::Debug for Json {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Json({self})")
    }
}

impl Display for Json {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Tarantool's own renderer. It needs normal form and does not check
        // it. Every `Json` has it once the renderer is linked: `parse_text`
        // produces it, and `Deserialize` and `Value::decode` read bytes that
        // came in an iproto request, a net.box response or Lua arguments, each
        // checked on the way in, or from local storage. So this fails only
        // outside a tarantool process (unit tests), where it is not linked.
        if tarantool::ffi::has_json() {
            if let Some(s) = tarantool::json::to_string(&self.0) {
                return f.write_str(&s);
            }
        }
        Err(fmt::Error)
    }
}

impl Encode for Json {
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        ExtStruct::new(MP_JSON, &self.0).encode(w, context)
    }
}

impl Serialize for Json {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct _ExtStruct<'a>((i8, &'a serde_bytes::Bytes));

        _ExtStruct((MP_JSON, serde_bytes::Bytes::new(&self.0))).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct _ExtStruct((i8, serde_bytes::ByteBuf));

        let _ExtStruct((tag, bytes)) = Deserialize::deserialize(deserializer)?;
        if tag != MP_JSON {
            return Err(serde::de::Error::custom(format_args!(
                "expected MP_JSON ext (tag {MP_JSON}), got tag {tag}"
            )));
        }
        Json::from_inner(bytes.into_vec()).map_err(serde::de::Error::custom)
    }
}
