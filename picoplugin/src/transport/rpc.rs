use crate::error_code::ErrorCode;
use crate::util::copy_to_region;
use crate::util::DisplayAsHexBytesLimitted;
use crate::util::RegionBuffer;
use std::borrow::Cow;
use tarantool::error::BoxError;
use tarantool::error::TarantoolErrorCode;
use tarantool::unwrap_ok_or;

pub mod client;
pub mod server;

pub use client::RequestBuilder;
pub use client::RequestTarget;
pub use server::RouteBuilder;

////////////////////////////////////////////////////////////////////////////////
// Request
////////////////////////////////////////////////////////////////////////////////

/// A container for raw data used as arguments of a RPC request.
#[derive(Clone)]
pub struct Request<'a> {
    raw: Cow<'a, [u8]>,
}

impl<'a> Request<'a> {
    /// Constructs arguments for an RPC request from provided raw bytes.
    ///
    /// The bytes are sent as is.
    ///
    /// This method should be used on the **client side** of the RPC request.
    #[inline(always)]
    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        Self { raw: bytes.into() }
    }

    /// Encodes the provided data into msgpack using the [`rmp_serde`]
    /// implementation.
    ///
    /// Note: The structs are encoded as msgpack mappings (string keys).
    ///
    /// This method should be used on the **client side** of the RPC request.
    #[inline]
    #[track_caller]
    pub fn encode_rmp<T>(v: &T) -> Result<Self, BoxError>
    where
        T: serde::Serialize,
    {
        let bytes = unwrap_ok_or!(rmp_serde::encode::to_vec_named(v),
            Err(e) => {
                // Note: not using `.map_err()` so that `#[track_caller]` works
                // and we can capture the caller's source location
                return Err(BoxError::new(ErrorCode::Other, e.to_string()));
            }
        );
        Ok(Self { raw: bytes.into() })
    }

    /// Encodes the provided data into msgpack using the [`rmp_serde`]
    /// implementation.
    ///
    /// Note: The structs are encoded as msgpack arrays.
    ///
    /// This method should be used on the **client side** of the RPC request.
    #[inline]
    #[track_caller]
    pub fn encode_rmp_unnamed<T>(v: &T) -> Result<Self, BoxError>
    where
        T: serde::Serialize,
    {
        let bytes = unwrap_ok_or!(rmp_serde::encode::to_vec(v),
            Err(e) => {
                // Note: not using `.map_err()` so that `#[track_caller]` works
                // and we can capture the caller's source location
                return Err(BoxError::new(ErrorCode::Other, e.to_string()));
            }
        );
        Ok(Self { raw: bytes.into() })
    }

    /// Returns the raw bytes of the request arguments.
    ///
    /// This method should be used on the **server side** of the RPC request.
    #[inline(always)]
    pub fn as_bytes(&'a self) -> &'a [u8] {
        &*self.raw
    }

    /// Decodes the request arguments from msgpack using the [`rmp_serde`]
    /// implementation.
    ///
    /// This method should be used on the **server side** of the RPC request.
    #[inline(always)]
    #[track_caller]
    pub fn decode_rmp<T>(&'a self) -> Result<T, BoxError>
    where
        T: serde::Deserialize<'a>,
    {
        match rmp_serde::from_slice(self.as_bytes()) {
            Ok(r) => Ok(r),
            Err(e) => {
                // Note: not using `.map_err()` so that `#[track_caller]` works
                // and we can capture the caller's source location
                Err(BoxError::new(
                    TarantoolErrorCode::InvalidMsgpack,
                    e.to_string(),
                ))
            }
        }
    }
}

impl AsRef<[u8]> for Request<'_> {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl std::fmt::Debug for Request<'_> {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Display::fmt(&DisplayAsHexBytesLimitted(self.as_bytes()), f)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Response
////////////////////////////////////////////////////////////////////////////////

/// A container for raw data received in response to a RPC request.
#[derive(Clone)]
pub struct Response {
    inner: ResponseImpl,
}

#[derive(Debug, Clone)]
enum ResponseImpl {
    RegionSlice(&'static [u8]),
    Owned(Box<[u8]>),
}

impl Response {
    /// This method is for **internal use only**.
    ///
    /// Constructs an owned version of the response which is returned to the
    /// caller of the RPC request.
    ///
    /// This method should be used on the **client side** of the RPC request.
    #[inline(always)]
    pub(crate) fn new_owned(bytes: &[u8]) -> Self {
        Self {
            inner: ResponseImpl::Owned(bytes.into()),
        }
    }

    /// This method is for **internal use only**.
    ///
    /// Returns the slice of bytes allocated on the region. If `self.inner` is
    /// `Owned` the data will be copied to the region.
    ///
    /// This method should be used on the **client side** of the RPC request.
    pub(crate) fn to_region_slice(&self) -> Result<&'static [u8], BoxError> {
        match &self.inner {
            ResponseImpl::RegionSlice(region_slice) => Ok(region_slice),
            ResponseImpl::Owned(boxed_slice) => copy_to_region(boxed_slice),
        }
    }

    /// Returns the raw data in the response.
    ///
    /// This method should be used on the **client side** of the RPC request.
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        match &self.inner {
            ResponseImpl::RegionSlice(region_slice) => region_slice,
            ResponseImpl::Owned(boxed_slice) => &boxed_slice,
        }
    }

    /// Decodes the response data from msgpack using the [`rmp_serde`] implementation.
    ///
    /// This method should be used on the **client side** of the RPC request.
    #[inline(always)]
    #[track_caller]
    pub fn decode_rmp<'a, T>(&'a self) -> Result<T, BoxError>
    where
        T: serde::Deserialize<'a>,
    {
        match rmp_serde::from_slice(self.as_bytes()) {
            Ok(r) => Ok(r),
            Err(e) => {
                // Note: not using `.map_err()` so that `#[track_caller]` works
                // and we can capture the caller's source location
                Err(BoxError::new(
                    TarantoolErrorCode::InvalidMsgpack,
                    e.to_string(),
                ))
            }
        }
    }

    /// Constructs a response to the RPC request from provided raw bytes.
    /// The bytes are sent as is.
    ///
    /// This method should be used on the **server side** of the RPC request.
    #[inline(always)]
    #[track_caller]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BoxError> {
        let region_slice = unwrap_ok_or!(copy_to_region(bytes),
            Err(e) => {
                // Note: recreating the error to capture the caller's source location
                return Err(BoxError::new(e.error_code(), e.message()));
            }
        );
        Ok(Self {
            inner: ResponseImpl::RegionSlice(region_slice),
        })
    }

    /// Constructs a response to the RPC request from provided raw bytes.
    /// The bytes are sent as is.
    ///
    /// This method is an optimization which allows to avoid redundant copies
    /// when the data's lifetime is static.
    ///
    /// This method should be used on the **server side** of the RPC request.
    #[inline(always)]
    pub fn from_static(static_slice: &'static [u8]) -> Self {
        Self {
            // Note: It's not really on region but it's still ok
            inner: ResponseImpl::RegionSlice(static_slice),
        }
    }

    /// Encodes the provided data into msgpack using the [`rmp_serde`]
    /// implementation.
    ///
    /// Note: The structs are encoded as msgpack mappings (string keys).
    ///
    /// This method should be used on the **server side** of the RPC request.
    #[inline]
    #[track_caller]
    pub fn encode_rmp<T>(v: &T) -> Result<Self, BoxError>
    where
        T: serde::Serialize,
    {
        let mut buffer = RegionBuffer::new();
        if let Err(e) = rmp_serde::encode::write_named(&mut buffer, v) {
            // Note: not using `.map_err()` so that `#[track_caller]` works
            // and we can capture the caller's source location
            return Err(BoxError::new(ErrorCode::Other, e.to_string()));
        }
        let (region_slice, _) = buffer.into_raw_parts();
        Ok(Self {
            inner: ResponseImpl::RegionSlice(region_slice),
        })
    }

    /// Encodes the provided data into msgpack using the [`rmp_serde`]
    /// implementation.
    ///
    /// Note: The structs are encoded as msgpack arrays.
    ///
    /// This method should be used on the **server side** of the RPC request.
    #[inline]
    #[track_caller]
    pub fn encode_rmp_unnamed<T>(v: &T) -> Result<Self, BoxError>
    where
        T: serde::Serialize,
    {
        let mut buffer = RegionBuffer::new();
        if let Err(e) = rmp_serde::encode::write(&mut buffer, v) {
            // Note: not using `.map_err()` so that `#[track_caller]` works
            // and we can capture the caller's source location
            return Err(BoxError::new(ErrorCode::Other, e.to_string()));
        }
        let (region_slice, _) = buffer.into_raw_parts();
        Ok(Self {
            inner: ResponseImpl::RegionSlice(region_slice),
        })
    }
}

impl std::fmt::Debug for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Response::")?;
        match self.inner {
            ResponseImpl::Owned { .. } => f.write_str("Owned(")?,
            ResponseImpl::RegionSlice { .. } => f.write_str("RegionSlice(")?,
        }
        std::fmt::Display::fmt(&DisplayAsHexBytesLimitted(self.as_bytes()), f)?;
        f.write_str(")")?;
        Ok(())
    }
}

impl AsRef<[u8]> for Response {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl TryFrom<&[u8]> for Response {
    type Error = BoxError;

    #[inline(always)]
    fn try_from(bytes: &[u8]) -> Result<Self, BoxError> {
        Self::from_bytes(bytes)
    }
}

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;

    #[tarantool::test]
    fn check_error_location() {
        // Request
        let request = Request::from_bytes(b"\xa3foo");
        let e = request.decode_rmp::<i32>().unwrap_err();
        let error_line = line!() - 1;

        assert_eq!(e.file(), Some(file!()));
        assert_eq!(e.line(), Some(error_line));
        // sanity check
        let s: String = request.decode_rmp().unwrap();
        assert_eq!(s, "foo");

        // Response
        let response = Response::from_static(b"\xa3foo");
        let e = response.decode_rmp::<i32>().unwrap_err();
        let error_line = line!() - 1;

        assert_eq!(e.file(), Some(file!()));
        assert_eq!(e.line(), Some(error_line));
        // sanity check
        let s: String = response.decode_rmp().unwrap();
        assert_eq!(s, "foo");
    }
}
