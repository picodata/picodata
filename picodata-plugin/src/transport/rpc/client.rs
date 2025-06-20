use super::Request;
use super::Response;
use crate::internal::ffi;
use crate::plugin::interface::PicoContext;
#[allow(unused_imports)]
use crate::transport::rpc::server::RouteBuilder;
use crate::util::FfiSafeBytes;
use crate::util::FfiSafeStr;
use crate::util::RegionGuard;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::error::TarantoolErrorCode;
use tarantool::fiber;
use tarantool::time::Instant;

////////////////////////////////////////////////////////////////////////////////
// RequestBuilder
////////////////////////////////////////////////////////////////////////////////

/// A helper struct for sending RPC requests.
///
/// See also [`RouteBuilder`] for the server side of the RPC communication.
#[derive(Debug)]
pub struct RequestBuilder<'a> {
    target: FfiSafeRpcTargetSpecifier,
    plugin_service: Option<(&'a str, &'a str)>,
    version: Option<&'a str>,
    path: Option<&'a str>,
    input: Option<Request<'a>>,
    timeout: Option<Duration>,
}

impl<'a> RequestBuilder<'a> {
    #[inline]
    pub fn new(target: RequestTarget<'a>) -> Self {
        let target = match target {
            RequestTarget::Any => FfiSafeRpcTargetSpecifier::Any,
            RequestTarget::InstanceName(instance_name) => {
                FfiSafeRpcTargetSpecifier::InstanceName(instance_name.into())
            }
            RequestTarget::BucketId(bucket_id, to_master) => FfiSafeRpcTargetSpecifier::BucketId {
                bucket_id,
                to_master,
            },
            RequestTarget::TierAndBucketId(tier, bucket_id, to_master) => {
                FfiSafeRpcTargetSpecifier::TierAndBucketId {
                    tier: tier.into(),
                    bucket_id,
                    to_master,
                }
            }
            RequestTarget::ReplicasetName(replicaset_name, to_master) => {
                FfiSafeRpcTargetSpecifier::Replicaset {
                    replicaset_name: replicaset_name.into(),
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

    /// Use service info from `context`.
    /// The request will be sent to an endpoint registered by the specified service.
    #[inline]
    #[track_caller]
    pub fn pico_context(self, context: &'a PicoContext) -> Self {
        self.plugin_service(context.plugin_name(), context.service_name())
            .plugin_version(context.plugin_version())
    }

    /// The request will be sent to an endpoint registered by the specified service.
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

    /// The request will be sent to an endpoint registered by the specified service.
    #[inline]
    pub fn plugin_version(mut self, version: &'a str) -> Self {
        if let Some(old) = self.version.take() {
            #[rustfmt::skip]
            tarantool::say_warn!("RequestBuilder service version is silently changed from {old:?} to {version:?}");
        }
        self.version = Some(version);
        self
    }

    /// The request will be sent to the endpoint at the given `path`.
    #[inline]
    pub fn path(mut self, path: &'a str) -> Self {
        if let Some(old) = self.path.take() {
            #[rustfmt::skip]
            tarantool::say_warn!("RequestBuilder path is silently changed from {old:?} to {path:?}");
        }
        self.path = Some(path);
        self
    }

    /// Specify request arguments.
    #[inline]
    pub fn input(mut self, input: Request<'a>) -> Self {
        if let Some(old) = self.input.take() {
            #[rustfmt::skip]
            tarantool::say_warn!("RequestBuilder input is silently changed from {old:?} to {input:?}");
        }
        self.input = Some(input);
        self
    }

    /// Specify request timeout.
    #[inline]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        if let Some(old) = self.timeout.take() {
            #[rustfmt::skip]
            tarantool::say_warn!("RequestBuilder timeout is silently changed from {old:?} to {timeout:?}");
        }
        self.timeout = Some(timeout);
        self
    }

    /// Specify request deadline.
    #[inline(always)]
    pub fn deadline(self, deadline: Instant) -> Self {
        self.timeout(deadline.duration_since(fiber::clock()))
    }

    #[track_caller]
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

        let Some(input) = &self.input else {
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
            input: input.as_bytes().into(),
            _marker: PhantomData,
        })
    }

    /// Send the request with the current parameters.
    ///
    /// Note that if the request target specification (see [`RequestTarget`])
    /// matches the current instance then the request will be performed fully
    /// locally, i.e. the request handler will be executed in the current
    /// process without switching the fiber (although the fiber's name may
    /// change).
    ///
    /// If the request target doesn't match the current instance, an iproto
    /// request will be sent out and the current fiber will be blocked
    /// until the response is received or the timeout is reached.
    ///
    /// Returns an error if some of the parameters are invalid.
    #[inline]
    #[track_caller]
    pub fn send(&self) -> Result<Response, BoxError> {
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

    /// The specific instance with a given instance name.
    InstanceName(&'a str),

    /// An instance in the replicaset in tier of target instance which currently stores the bucket with
    /// the specified id.
    ///
    /// If the boolean parameter is `true`, then send the request to the replicaset master,
    /// otherwise any replica.
    BucketId(u64, bool),

    /// An instance in the replicaset in the tier which currently stores the bucket with
    /// the specified id.
    ///
    /// If the boolean parameter is `true`, then send the request to the replicaset master,
    /// otherwise any replica.
    TierAndBucketId(&'a str, u64, bool),

    /// An instance in the replicaset determined by the explicit replicaset name.
    ///
    /// If the boolean parameter is `true`, then send the request to the replicaset master,
    /// otherwise any replica.
    ReplicasetName(&'a str, bool),
}

////////////////////////////////////////////////////////////////////////////////
// ffi wrappers
////////////////////////////////////////////////////////////////////////////////

/// **For internal use**.
fn send_rpc_request(
    arguments: &FfiSafeRpcRequestArguments,
    timeout: Option<Duration>,
) -> Result<Response, BoxError> {
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
    Ok(Response::new_owned(output))
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
    InstanceName(FfiSafeStr),
    Replicaset {
        replicaset_name: FfiSafeStr,
        to_master: bool,
    },
    BucketId {
        bucket_id: u64,
        to_master: bool,
    },
    TierAndBucketId {
        tier: FfiSafeStr,
        bucket_id: u64,
        to_master: bool,
    },
}
