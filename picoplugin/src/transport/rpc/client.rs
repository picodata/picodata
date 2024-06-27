use crate::error_code::ErrorCode;
use crate::internal::ffi;
use crate::plugin::interface::PicoContext;
use crate::util::FfiSafeBytes;
use crate::util::FfiSafeStr;
use crate::util::RegionGuard;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::error::TarantoolErrorCode;
use tarantool::fiber;
use tarantool::time::Instant;
use tarantool::unwrap_ok_or;
use tarantool::util::DisplayAsHexBytes;

////////////////////////////////////////////////////////////////////////////////
// RequestBuilder
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct RequestBuilder<'a> {
    target: FfiSafeRpcTargetSpecifier,
    plugin_service: Option<(&'a str, &'a str)>,
    version: Option<&'a str>,
    path: Option<&'a str>,
    input: Option<Cow<'a, [u8]>>,
    timeout: Option<Duration>,
}

impl<'a> RequestBuilder<'a> {
    #[inline]
    pub fn new(target: RequestTarget<'a>) -> Self {
        let target = match target {
            RequestTarget::Any => FfiSafeRpcTargetSpecifier::Any,
            RequestTarget::InstanceId(instance_id) => {
                FfiSafeRpcTargetSpecifier::InstanceId(instance_id.into())
            }
            RequestTarget::BucketId(bucket_id, to_master) => FfiSafeRpcTargetSpecifier::BucketId {
                bucket_id,
                to_master,
            },
            RequestTarget::ReplicasetId(replicaset_id, to_master) => {
                FfiSafeRpcTargetSpecifier::Replicaset {
                    replicaset_id: replicaset_id.into(),
                    to_master,
                }
            }
        };
        Self {
            target,
            plugin_service: None,
            version: None,
            path: None,
            input: None,
            timeout: None,
        }
    }

    #[inline]
    #[track_caller]
    pub fn pico_context(self, context: &'a PicoContext) -> Self {
        self.plugin_service(context.plugin_name(), context.service_name())
            .plugin_version(context.plugin_version())
    }

    #[inline]
    pub fn plugin_service(mut self, plugin: &'a str, service: &'a str) -> Self {
        let new = (plugin, service);
        if let Some(old) = self.plugin_service.take() {
            #[rustfmt::skip]
            tarantool::say_warn!("RequestBuilder plugin.service is silently changed from {old:?} to {new:?}");
        }
        self.plugin_service = Some(new);
        self
    }

    #[inline]
    pub fn plugin_version(mut self, version: &'a str) -> Self {
        if let Some(old) = self.version.take() {
            #[rustfmt::skip]
            tarantool::say_warn!("RequestBuilder service version is silently changed from {old:?} to {version:?}");
        }
        self.version = Some(version);
        self
    }

    #[inline]
    pub fn path(mut self, path: &'a str) -> Self {
        if let Some(old) = self.path.take() {
            #[rustfmt::skip]
            tarantool::say_warn!("RequestBuilder path is silently changed from {old:?} to {path:?}");
        }
        self.path = Some(path);
        self
    }

    #[inline]
    pub fn raw_input(mut self, input: &'a [u8]) -> Self {
        if let Some(old) = self.input.take() {
            #[rustfmt::skip]
            tarantool::say_warn!("RequestBuilder input is silently changed from {} to {}", DisplayAsHexBytes(&old), DisplayAsHexBytes(input));
        }
        self.input = Some(input.into());
        self
    }

    #[inline]
    pub fn input_rmp<T>(mut self, input: &T) -> Result<Self, BoxError>
    where
        T: serde::Serialize + ?Sized,
    {
        let data = unwrap_ok_or!(rmp_serde::to_vec(input),
            Err(e) => {
                #[rustfmt::skip]
                return Err(BoxError::new(ErrorCode::Other, format!("failed encoding RPC request inputs: {e}")));
            }
        );
        if let Some(old) = self.input.take() {
            #[rustfmt::skip]
            tarantool::say_warn!("RequestBuilder input is silently changed from {} to {}", DisplayAsHexBytes(&old), DisplayAsHexBytes(&data));
        }
        self.input = Some(data.into());
        Ok(self)
    }

    #[inline]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        if let Some(old) = self.timeout.take() {
            #[rustfmt::skip]
            tarantool::say_warn!("RequestBuilder timeout is silently changed from {old:?} to {timeout:?}");
        }
        self.timeout = Some(timeout);
        self
    }

    #[inline(always)]
    pub fn deadline(self, deadline: Instant) -> Self {
        self.timeout(deadline.duration_since(fiber::clock()))
    }

    fn to_ffi(&self) -> Result<FfiSafeRpcRequestArguments<'a>, BoxError> {
        let Some((plugin, service)) = self.plugin_service else {
            #[rustfmt::skip]
            return Err(BoxError::new(TarantoolErrorCode::IllegalParams, "plugin.service must be specified for RPC request"));
        };

        let Some(version) = self.version else {
            #[rustfmt::skip]
            return Err(BoxError::new(TarantoolErrorCode::IllegalParams, "service version must be specified for RPC request"));
        };

        let Some(path) = self.path else {
            #[rustfmt::skip]
            return Err(BoxError::new(TarantoolErrorCode::IllegalParams, "path must be specified for RPC request"));
        };

        let Some(input) = self.input.as_deref() else {
            #[rustfmt::skip]
            return Err(BoxError::new(TarantoolErrorCode::IllegalParams, "input must be specified for RPC request"));
        };

        let target = self.target;

        Ok(FfiSafeRpcRequestArguments {
            plugin: plugin.into(),
            service: service.into(),
            version: version.into(),
            target,
            path: path.into(),
            input: input.into(),
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn send(&self) -> Result<Vec<u8>, BoxError> {
        let arguments = self.to_ffi()?;
        let res = send_rpc_request(&arguments, self.timeout)?;
        Ok(res)
    }
}

/// An enumeration of possible target specifiers for RPC requests.
/// Determines which instance in the picodata cluster the request should be sent to.
#[derive(Default, Debug, Clone, Copy)]
#[non_exhaustive]
pub enum RequestTarget<'a> {
    /// Any instance running the corresponding service.
    #[default]
    Any,

    /// The specific instance with a given instance id.
    InstanceId(&'a str),

    /// An instance in the replicaset which currently stores the bucket with
    /// the specified id.
    ///
    /// If the boolean parameter is `true`, then send the request to the replicaset master,
    /// otherwise any replica.
    BucketId(u64, bool),

    /// An instance in the replicaset determined by the explicit replicaset id.
    ///
    /// If the boolean parameter is `true`, then send the request to the replicaset master,
    /// otherwise any replica.
    ReplicasetId(&'a str, bool),
}

////////////////////////////////////////////////////////////////////////////////
// ffi wrappers
////////////////////////////////////////////////////////////////////////////////

/// **For internal use**.
fn send_rpc_request(
    arguments: &FfiSafeRpcRequestArguments,
    timeout: Option<Duration>,
) -> Result<Vec<u8>, BoxError> {
    let mut output = MaybeUninit::uninit();

    let _guard = RegionGuard::new();

    // SAFETY: always safe to call picodata FFI
    let rc = unsafe {
        ffi::pico_ffi_rpc_request(
            arguments,
            timeout.unwrap_or(tarantool::clock::INFINITY).as_secs_f64(),
            output.as_mut_ptr(),
        )
    };
    if rc == -1 {
        return Err(BoxError::last());
    }

    let output = unsafe { output.assume_init().as_bytes() };
    Ok(output.into())
}

/// **For internal use**.
///
/// Use [`RequestBuilder`] instead.
#[derive(Debug, Clone)]
#[repr(C)]
pub struct FfiSafeRpcRequestArguments<'a> {
    pub plugin: FfiSafeStr,
    pub service: FfiSafeStr,
    pub version: FfiSafeStr,
    pub target: FfiSafeRpcTargetSpecifier,
    pub path: FfiSafeStr,
    pub input: FfiSafeBytes,
    _marker: PhantomData<&'a ()>,
}

/// **For internal use**.
///
/// Use [`RequestTarget`] instead.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub enum FfiSafeRpcTargetSpecifier {
    Any,
    InstanceId(FfiSafeStr),
    Replicaset {
        replicaset_id: FfiSafeStr,
        to_master: bool,
    },
    BucketId {
        bucket_id: u64,
        to_master: bool,
    },
}
