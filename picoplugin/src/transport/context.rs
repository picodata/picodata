use crate::util::DisplayAsHexBytesLimitted;
use crate::util::DisplayErrorLocation;
use crate::util::FfiSafeBytes;
use crate::util::FfiSafeStr;
use std::borrow::Cow;
use std::cell::OnceCell;
use std::collections::HashMap;
use std::io::Cursor;
use tarantool::error::BoxError;
use tarantool::error::TarantoolErrorCode;
use tarantool::error::TarantoolErrorCode::InvalidMsgpack;
use tarantool::ffi::uuid::tt_uuid;
use tarantool::msgpack::skip_value;
use tarantool::unwrap_ok_or;
use tarantool::uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////
// Context
////////////////////////////////////////////////////////////////////////////////

/// Context stores request metadata. This includes some builin fields, for example:
/// - [`Context::request_id`],
/// - [`Context::path`],
/// - [`Context::plugin_name`],
/// - [`Context::plugin_version`],
/// - [`Context::service_name`],
///
/// But also supports arbitrary user defined named fields.
pub struct Context<'a> {
    request_id: Uuid,
    path: &'a str,
    plugin_name: &'a str,
    service_name: &'a str,
    plugin_version: &'a str,

    /// Raw context encoded as msgpack.
    raw: &'a [u8],

    /// Custom context fields string keys. This field is constructed on demand.
    named_fields: OnceCell<Result<ContextNamedFields<'a>, BoxError>>,
}

type ContextNamedFields<'a> = HashMap<Cow<'a, str>, ContextValue<'a>>;

impl<'a> Context<'a> {
    #[inline]
    pub(crate) fn new(context: &'a FfiSafeContext) -> Self {
        // SAFETY: these are all safe because lifetime is remembered
        let path = unsafe { context.path.as_str() };
        let plugin_name = unsafe { context.plugin_name.as_str() };
        let service_name = unsafe { context.service_name.as_str() };
        let plugin_version = unsafe { context.plugin_version.as_str() };
        let raw = unsafe { context.raw.as_bytes() };
        Self {
            request_id: Uuid::from_tt_uuid(context.request_id),
            path,
            plugin_name,
            service_name,
            plugin_version,
            raw,
            named_fields: OnceCell::new(),
        }
    }

    #[inline(always)]
    pub fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[inline(always)]
    pub fn path(&self) -> &str {
        self.path
    }

    #[inline(always)]
    pub fn plugin_name(&self) -> &str {
        self.plugin_name
    }

    #[inline(always)]
    pub fn service_name(&self) -> &str {
        self.service_name
    }

    #[inline(always)]
    pub fn plugin_version(&self) -> &str {
        self.plugin_version
    }

    #[inline(always)]
    pub fn raw(&self) -> &[u8] {
        self.raw
    }

    /// Set a named `field` to the `value`. Returns the old value if it was set.
    #[inline(always)]
    pub fn set(
        &mut self,
        field: impl Into<Cow<'static, str>>,
        value: impl Into<ContextValue<'static>>,
    ) -> Result<Option<ContextValue<'a>>, BoxError> {
        // Make sure the map is initialized.
        // TODO: use `OnceCell::get_mut_or_init` when it's stable.
        self.get_named_fields()?;

        let res: &mut Result<ContextNamedFields, BoxError> = self
            .named_fields
            .get_mut()
            .expect("just made sure it's there");
        let named_fields: &mut ContextNamedFields = res
            .as_mut()
            .expect("if it was an error we would've returned early");
        let old_value = named_fields.insert(field.into(), value.into());
        Ok(old_value)
    }

    #[inline(always)]
    pub fn get(&self, field: &str) -> Result<Option<&ContextValue<'a>>, BoxError> {
        let named_fields = self.get_named_fields()?;
        Ok(named_fields.get(field))
    }

    #[inline]
    pub fn get_named_fields(&self) -> Result<&ContextNamedFields<'a>, BoxError> {
        self.named_fields
            .get_or_init(|| decode_msgpack_string_fields(self.raw))
            .as_ref()
            .map_err(Clone::clone)
    }
}

impl std::fmt::Debug for Context<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut s = f.debug_struct("Context");
        let mut s = s
            .field("request_id", &DebugUuid(self.request_id))
            .field("path", &self.path)
            .field("plugin_name", &self.plugin_name)
            .field("service_name", &self.service_name)
            .field("plugin_version", &self.plugin_version);
        if let Ok(named_fields) = self.get_named_fields() {
            for (key, value) in named_fields {
                s = s.field(key, value);
            }
            s.finish()
        } else {
            s.finish_non_exhaustive()
        }
    }
}

tarantool::define_enum_with_introspection! {
    /// Context integer field identifiers (keys in the msgpack mapping).
    pub enum ContextFieldId {
        RequestId = 1,
        PluginName = 2,
        ServiceName = 3,
        PluginVersion = 4,
    }
}

#[inline]
fn decode_msgpack_string_fields<'a>(raw: &'a [u8]) -> Result<ContextNamedFields<'a>, BoxError> {
    decode_msgpack_string_fields_impl(raw).map_err(|e| log_context_decoding_error(raw, e))
}

fn decode_msgpack_string_fields_impl<'a>(
    raw: &'a [u8],
) -> Result<ContextNamedFields<'a>, BoxError> {
    let mut buffer = Cursor::new(raw);

    let count = rmp::decode::read_map_len(&mut buffer).map_err(invalid_msgpack)? as usize;

    let mut named_fields = ContextNamedFields::with_capacity(count);
    for i in 0..count {
        let Ok(marker) = rmp::decode::read_marker(&mut buffer) else {
            #[rustfmt::skip]
            return Err(BoxError::new(InvalidMsgpack, format!("not enough values in mapping: expected {count}, got {i}")));
        };
        if let Some(_field_id) = read_rest_of_uint(marker, &mut buffer)? {
            // Integer field id
            // Skip as it should've already been handled the first time around
            skip_value(&mut buffer).map_err(invalid_msgpack)?;
        } else if let Some(field_name) = read_rest_of_str(marker, &mut buffer)? {
            // String field name
            let value = rmp_serde::decode::from_read(&mut buffer)
                .map_err(|e| decoding_field(field_name, invalid_msgpack(e)))?;
            named_fields.insert(Cow::Borrowed(field_name), value);
        } else {
            // Invalid field key
            #[rustfmt::skip]
            return Err(BoxError::new(InvalidMsgpack, format!("context field must be positive integer or string, got {marker:?}")));
        }
    }

    Ok(named_fields)
}

////////////////////////////////////////////////////////////////////////////////
// ContextValue
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum ContextValue<'a> {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(Cow<'a, str>),
    Array(Vec<ContextValue<'a>>),
}

impl ContextValue<'_> {
    #[inline(always)]
    pub fn bool(&self) -> Option<bool> {
        match self {
            &Self::Bool(v) => Some(v),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn int(&self) -> Option<i64> {
        match self {
            &Self::Int(v) => Some(v),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn float(&self) -> Option<f64> {
        match self {
            &Self::Float(v) => Some(v),
            &Self::Int(v) => Some(v as _),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn str(&self) -> Option<&str> {
        match self {
            Self::String(v) => Some(v),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn array(&self) -> Option<&[Self]> {
        match self {
            Self::Array(v) => Some(&*v),
            _ => None,
        }
    }

    /// Returns true if `&self` does not contain any `&str`.
    ///
    /// See also [`Self::into_owned`].
    pub fn is_owned(&self) -> bool {
        match self {
            Self::Bool { .. } => true,
            Self::Int { .. } => true,
            Self::Float { .. } => true,
            Self::String(v) => matches!(v, Cow::Owned { .. }),
            Self::Array(v) => v.iter().all(Self::is_owned),
        }
    }

    /// Converts `self` into a version with a `'static` lifetime. This means
    /// that any `&str` stored inside will be converted to `String`.
    pub fn into_owned(self) -> ContextValue<'static> {
        match self {
            Self::Bool(v) => ContextValue::Bool(v),
            Self::Int(v) => ContextValue::Int(v),
            Self::Float(v) => ContextValue::Float(v),
            // Note: even though this could be Cow::Borrowed(&'static str) which already has 'static lifetime,
            // rust will not less us know this information, so we must do a redundant allocation.
            // Let's just wait until "specialization" is stabilized (which will never happen by the way).
            Self::String(v) => ContextValue::String(Cow::Owned(v.into())),
            Self::Array(v) => ContextValue::Array(v.into_iter().map(Self::into_owned).collect()),
        }
    }
}

impl From<bool> for ContextValue<'_> {
    #[inline(always)]
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

#[rustfmt::skip]
pub mod impl_int {
    use super::*;
    impl From<i8> for ContextValue<'_> { #[inline(always)] fn from(v: i8) -> Self { Self::Int(v as _) } }
    impl From<i16> for ContextValue<'_> { #[inline(always)] fn from(v: i16) -> Self { Self::Int(v as _) } }
    impl From<i32> for ContextValue<'_> { #[inline(always)] fn from(v: i32) -> Self { Self::Int(v as _) } }
    impl From<i64> for ContextValue<'_> { #[inline(always)] fn from(v: i64) -> Self { Self::Int(v as _) } }
    impl From<isize> for ContextValue<'_> { #[inline(always)] fn from(v: isize) -> Self { Self::Int(v as _) } }
    impl From<u8> for ContextValue<'_> { #[inline(always)] fn from(v: u8) -> Self { Self::Int(v as _) } }
    impl From<u16> for ContextValue<'_> { #[inline(always)] fn from(v: u16) -> Self { Self::Int(v as _) } }
    impl From<u32> for ContextValue<'_> { #[inline(always)] fn from(v: u32) -> Self { Self::Int(v as _) } }
    impl From<u64> for ContextValue<'_> { #[inline(always)] fn from(v: u64) -> Self { Self::Int(v as _) } }
    impl From<usize> for ContextValue<'_> { #[inline(always)] fn from(v: usize) -> Self { Self::Int(v as _) } }
}

impl From<f32> for ContextValue<'_> {
    #[inline(always)]
    fn from(v: f32) -> Self {
        Self::Float(v as _)
    }
}

impl From<f64> for ContextValue<'_> {
    #[inline(always)]
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl<'a> From<&'a str> for ContextValue<'a> {
    #[inline(always)]
    fn from(s: &'a str) -> Self {
        Self::String(s.into())
    }
}

impl From<String> for ContextValue<'_> {
    #[inline(always)]
    fn from(s: String) -> Self {
        Self::String(s.into())
    }
}

impl<'a> From<Cow<'a, str>> for ContextValue<'a> {
    #[inline(always)]
    fn from(s: Cow<'a, str>) -> Self {
        Self::String(s)
    }
}

////////////////////////////////////////////////////////////////////////////////
// FfiSafeContext
////////////////////////////////////////////////////////////////////////////////

/// **For internal use**.
///
/// Use [`Context`] instead.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct FfiSafeContext {
    pub request_id: tt_uuid,
    pub path: FfiSafeStr,
    pub plugin_name: FfiSafeStr,
    pub service_name: FfiSafeStr,
    pub plugin_version: FfiSafeStr,

    /// Raw context encoded as msgpack.
    raw: FfiSafeBytes,
}

impl FfiSafeContext {
    #[inline(always)]
    pub fn decode_msgpack(path: &str, raw: &[u8]) -> Result<Self, ()> {
        Self::decode_msgpack_impl(path, raw)
            // Note: error is passed via box_error_last, because plugin may have
            // a different version of tarantool-module than picodata
            .map_err(|e| e.set_last())
    }

    pub fn decode_msgpack_impl(path: &str, raw: &[u8]) -> Result<Self, BoxError> {
        let mut request_id = None;
        let mut plugin_name = None;
        let mut service_name = None;
        let mut plugin_version = None;

        let mut buffer = Cursor::new(raw);
        let count = unwrap_ok_or!(rmp::decode::read_map_len(&mut buffer),
            Err(e) => return Err(BoxError::new(InvalidMsgpack, format!("expected a map: {e}")))
        );
        for i in 0..count {
            let Ok(marker) = rmp::decode::read_marker(&mut buffer) else {
                #[rustfmt::skip]
                return Err(BoxError::new(InvalidMsgpack, format!("not enough entries in mapping: expected {count}, got {i}")));
            };
            if let Some(field_id) = read_rest_of_uint(marker, &mut buffer)? {
                // Integer field id
                match ContextFieldId::try_from(field_id) {
                    Ok(field @ ContextFieldId::RequestId) => {
                        #[rustfmt::skip]
                        let v: Uuid = rmp_serde::decode::from_read(&mut buffer).map_err(|e| decoding_field(&field, invalid_msgpack(e)))?;
                        request_id = Some(v);
                    }
                    Ok(field @ ContextFieldId::PluginName) => {
                        #[rustfmt::skip]
                        let v = read_str(&mut buffer).map_err(|e| decoding_field(&field, e))?;
                        plugin_name = Some(v);
                    }
                    Ok(field @ ContextFieldId::ServiceName) => {
                        #[rustfmt::skip]
                        let v = read_str(&mut buffer).map_err(|e| decoding_field(&field, e))?;
                        service_name = Some(v);
                    }
                    Ok(field @ ContextFieldId::PluginVersion) => {
                        #[rustfmt::skip]
                        let v = read_str(&mut buffer).map_err(|e| decoding_field(&field, e))?;
                        plugin_version = Some(v);
                    }
                    Err(unknown_field_id) => {
                        #[rustfmt::skip]
                        tarantool::say_verbose!("ignoring unknown context field with integer id {unknown_field_id} (0x{unknown_field_id:02x})");
                        skip_value(&mut buffer).map_err(invalid_msgpack)?;
                    }
                }
            } else if let Some(_field_name) = read_rest_of_str(marker, &mut buffer)? {
                // String field name
                // Skip for now, will be handled later if user requests it
                skip_value(&mut buffer).map_err(invalid_msgpack)?;
            } else {
                // Invalid field key
                #[rustfmt::skip]
                return Err(BoxError::new(InvalidMsgpack, format!("context field must be positive integer or string, got {marker:?}")));
            }
        }

        let end_index = buffer.position() as usize;
        if end_index > raw.len() {
            #[rustfmt::skip]
            return Err(BoxError::new(InvalidMsgpack, format!("expected more data after {}", DisplayAsHexBytesLimitted(raw))));
        } else if end_index < raw.len() {
            let tail = &raw[end_index..];
            #[rustfmt::skip]
            return Err(BoxError::new(InvalidMsgpack, format!("unexpected data after context: {}", DisplayAsHexBytesLimitted(tail))));
        }

        let Some(request_id) = request_id else {
            return Err(invalid_msgpack("context must contain a request_id"));
        };

        let Some(plugin_name) = plugin_name else {
            return Err(invalid_msgpack("context must contain a plugin_name"));
        };

        let Some(service_name) = service_name else {
            return Err(invalid_msgpack("context must contain a service_name"));
        };

        let Some(plugin_version) = plugin_version else {
            return Err(invalid_msgpack("context must contain a plugin_version"));
        };

        Ok(Self {
            request_id: request_id.to_tt_uuid(),
            path: path.into(),
            plugin_name: plugin_name.into(),
            service_name: service_name.into(),
            plugin_version: plugin_version.into(),
            raw: raw.into(),
        })
    }
}

impl std::fmt::Debug for FfiSafeContext {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("FfiSafeContext")
            .field(
                "request_id",
                &DebugUuid(Uuid::from_tt_uuid(self.request_id)),
            )
            .field("path", &self.path)
            .field("plugin_name", &self.plugin_name)
            .field("service_name", &self.service_name)
            .field("plugin_version", &self.plugin_version)
            .finish_non_exhaustive()
    }
}

////////////////////////////////////////////////////////////////////////////////
// miscellaneous
////////////////////////////////////////////////////////////////////////////////

fn read_str<'a>(buffer: &mut Cursor<&'a [u8]>) -> Result<&'a str, BoxError> {
    let length = rmp::decode::read_str_len(buffer).map_err(invalid_msgpack)? as usize;

    str_from_cursor(length, buffer)
}

fn read_rest_of_str<'a>(
    marker: rmp::Marker,
    buffer: &mut Cursor<&'a [u8]>,
) -> Result<Option<&'a str>, BoxError> {
    use rmp::decode::RmpRead as _;

    let length;
    match marker {
        rmp::Marker::FixStr(v) => {
            length = v as usize;
        }
        rmp::Marker::Str8 => {
            length = buffer.read_data_u8().map_err(invalid_msgpack)? as usize;
        }
        rmp::Marker::Str16 => {
            length = buffer.read_data_u16().map_err(invalid_msgpack)? as usize;
        }
        rmp::Marker::Str32 => {
            length = buffer.read_data_u32().map_err(invalid_msgpack)? as usize;
        }
        _ => return Ok(None),
    }

    str_from_cursor(length, buffer).map(Some)
}

#[inline]
fn str_from_cursor<'a>(length: usize, buffer: &mut Cursor<&'a [u8]>) -> Result<&'a str, BoxError> {
    let start_index = buffer.position() as usize;
    let data = *buffer.get_ref();
    let remaining_length = data.len() - start_index;
    if remaining_length < length {
        return Err(BoxError::new(
            TarantoolErrorCode::InvalidMsgpack,
            format!("expected a string of length {length}, got {remaining_length}"),
        ));
    }

    let end_index = start_index + length;
    let res = std::str::from_utf8(&data[start_index..end_index]).map_err(invalid_msgpack)?;
    buffer.set_position(end_index as _);
    Ok(res)
}

fn read_rest_of_uint(
    marker: rmp::Marker,
    buffer: &mut Cursor<&[u8]>,
) -> Result<Option<u64>, BoxError> {
    use rmp::decode::RmpRead as _;
    match marker {
        rmp::Marker::FixPos(v) => Ok(Some(v as _)),
        rmp::Marker::U8 => {
            let v = buffer.read_data_u8().map_err(invalid_msgpack)?;
            Ok(Some(v as _))
        }
        rmp::Marker::U16 => {
            let v = buffer.read_data_u16().map_err(invalid_msgpack)?;
            Ok(Some(v as _))
        }
        rmp::Marker::U32 => {
            let v = buffer.read_data_u32().map_err(invalid_msgpack)?;
            Ok(Some(v as _))
        }
        rmp::Marker::U64 => {
            let v = buffer.read_data_u64().map_err(invalid_msgpack)?;
            Ok(Some(v as _))
        }
        _ => Ok(None),
    }
}

#[track_caller]
fn log_context_decoding_error(raw: &[u8], e: BoxError) -> BoxError {
    let location = DisplayErrorLocation(&e);
    if raw.len() <= 512 {
        let raw = DisplayAsHexBytesLimitted(raw);
        tarantool::say_error!("failed to decode context from msgpack ({raw}): {location}{e}");
    } else {
        tarantool::say_error!(
            "failed to decode context from msgpack (too big to display): {location}{e}"
        );
    }
    e
}

#[inline(always)]
#[track_caller]
fn decoding_field(field: &(impl std::fmt::Debug + ?Sized), error: BoxError) -> BoxError {
    BoxError::new(
        error.error_code(),
        format!("failed decoding field {field:?}: {}", error.message()),
    )
}

#[inline(always)]
#[track_caller]
fn invalid_msgpack(error: impl ToString) -> BoxError {
    BoxError::new(TarantoolErrorCode::InvalidMsgpack, error.to_string())
}

// TODO: fix impl Debug for Uuid in tarantool-module https://git.picodata.io/picodata/picodata/tarantool-module/-/issues/209
struct DebugUuid(Uuid);
impl std::fmt::Debug for DebugUuid {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;

    #[tarantool::test]
    fn decode_context() {
        //
        // Check error cases
        //
        let e = FfiSafeContext::decode_msgpack_impl("path", b"\x90").unwrap_err();
        #[rustfmt::skip]
        assert_eq!(e.to_string(), "InvalidMsgpack: expected a map: the type decoded isn't match with the expected one");

        let e = FfiSafeContext::decode_msgpack_impl("path", b"\x80").unwrap_err();
        #[rustfmt::skip]
        assert_eq!(e.to_string(), "InvalidMsgpack: context must contain a request_id");

        let e = FfiSafeContext::decode_msgpack_impl("path", b"\x87").unwrap_err();
        #[rustfmt::skip]
        assert_eq!(e.to_string(), "InvalidMsgpack: not enough entries in mapping: expected 7, got 0");

        let mut data = Vec::new();
        data.extend(b"\x84");
        data.extend(b"\x01\xd8\x020123456789abcdef");
        data.extend(b"\x02\xa6plugin");
        data.extend(b"\x03\xa7service");
        data.extend(b"\x04\xa51.2.3");
        data.extend(b"unexpected-data");
        let e = FfiSafeContext::decode_msgpack_impl("path", &data).unwrap_err();
        #[rustfmt::skip]
        assert_eq!(e.to_string(), r#"InvalidMsgpack: unexpected data after context: b"unexpected-data""#);

        let mut data = Vec::new();
        data.extend(b"\x85");
        data.extend(b"\x01\xd8\x02");
        let e = FfiSafeContext::decode_msgpack_impl("path", &data).unwrap_err();
        #[rustfmt::skip]
        assert_eq!(e.to_string(), "InvalidMsgpack: failed decoding field RequestId: IO error while reading data: unexpected end of file");

        let mut data = Vec::new();
        data.extend(b"\x85");
        data.extend(b"\x01\xd8\x020123456789abcdef");
        data.extend(b"\x02\xa6plugin");
        data.extend(b"\x03\xa7service");
        data.extend(b"\x04\xa51.2.3");
        data.extend(b"\xa3foo\xa4");
        let e = FfiSafeContext::decode_msgpack_impl("path", &data).unwrap_err();
        #[rustfmt::skip]
        assert_eq!(e.to_string(), "InvalidMsgpack: msgpack read error: failed to read MessagePack data");

        let mut data = Vec::new();
        data.extend(b"\x85");
        data.extend(b"\x01\xd8\x020123456789abcdef");
        data.extend(b"\x02\xa6plugin");
        data.extend(b"\x03\xa7service");
        data.extend(b"\x04\xa51.2.3");
        data.extend(b"\xa3foo\x80");
        let context = FfiSafeContext::decode_msgpack_impl("path", &data).unwrap();
        let context = Context::new(&context);
        let e = context.get("foo").unwrap_err();
        #[rustfmt::skip]
        assert_eq!(e.to_string(), r#"InvalidMsgpack: failed decoding field "foo": data did not match any variant of untagged enum ContextValue"#);

        //
        // Check sucess case
        //
        let mut data = Vec::new();
        data.extend(b"\x89");
        data.extend(b"\x01\xd8\x020123456789abcdef");
        data.extend(b"\x04\xa51.2.3");
        data.extend(b"\x03\xa7service");
        data.extend(b"\x02\xa6plugin");
        data.extend(b"\xa3foo\xa3bar");
        data.extend(b"\xa4bool\xc3");
        data.extend(b"\xa3int\x45");
        data.extend(b"\xa5float\xcb\x40\x09\x1e\xb8\x51\xeb\x85\x1f");
        data.extend(b"\xa5array\x93\x01\xa3two\x03");

        let context = FfiSafeContext::decode_msgpack_impl("path", &data).unwrap();
        let context = Context::new(&context);
        assert_eq!(
            context.request_id,
            Uuid::try_from_slice(b"0123456789abcdef").unwrap()
        );
        assert_eq!(context.path, "path");
        assert_eq!(context.plugin_name, "plugin");
        assert_eq!(context.service_name, "service");
        assert_eq!(context.plugin_version, "1.2.3");
        assert!(context.named_fields.get().is_none());

        assert_eq!(
            *context.get("foo").unwrap().unwrap(),
            ContextValue::from("bar")
        );
        assert_eq!(
            *context.get("bool").unwrap().unwrap(),
            ContextValue::from(true)
        );
        assert_eq!(
            *context.get("int").unwrap().unwrap(),
            ContextValue::from(69)
        );
        assert_eq!(
            *context.get("float").unwrap().unwrap(),
            ContextValue::from(3.14)
        );
        assert_eq!(
            *context.get("array").unwrap().unwrap(),
            ContextValue::Array(vec![
                ContextValue::from(1),
                ContextValue::from("two"),
                ContextValue::from(3)
            ])
        );
        assert!(context.get("bar").unwrap().is_none());
    }

    #[tarantool::test]
    fn context_get_set() {
        let mut data = Vec::new();
        data.extend(b"\x84");
        data.extend(b"\x01\xd8\x020123456789abcdef");
        data.extend(b"\x04\xa51.2.3");
        data.extend(b"\x03\xa7service");
        data.extend(b"\x02\xa6plugin");
        let context = FfiSafeContext::decode_msgpack_impl("path", &data).unwrap();
        let mut context = Context::new(&context);

        assert!(context.get("bar").unwrap().is_none());

        let old_value = context.set("bar", "string").unwrap();
        assert!(old_value.is_none());
        assert_eq!(
            *context.get("bar").unwrap().unwrap(),
            ContextValue::from("string")
        );

        let old_value = context.set("bar", 0xba5).unwrap().unwrap();
        assert_eq!(old_value, ContextValue::from("string"));
        assert_eq!(
            *context.get("bar").unwrap().unwrap(),
            ContextValue::from(0xba5)
        );

        assert!(!old_value.is_owned());
        assert!(old_value.into_owned().is_owned());
    }
}
