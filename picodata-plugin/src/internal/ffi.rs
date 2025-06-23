use crate::background::FfiBackgroundJobCancellationToken;
use crate::background::JobCancellationResult;
use crate::internal::types;
use crate::metrics::FfiMetricsHandler;
use crate::sql::types::SqlValue;
use crate::transport::rpc::client::FfiSafeRpcRequestArguments;
use crate::transport::rpc::server::FfiRpcHandler;
use crate::util::{FfiSafeBytes, FfiSafeStr};
use abi_stable::derive_macro_reexports::{ROption, RResult};
use abi_stable::std_types::{RDuration, RString, RVec};
use abi_stable::RTuple;
use tarantool::ffi::tarantool::BoxTuple;

// TODO: It just occured to me that we could probably simplify this by defining
// all `pico_ffi_*` functions in picoplugin. This will work, because picodata
// includes picoplugin anyway. The only problem (unless I'm missing something)
// would be that there'll be duplicate symbol definitions, which we could work
// around by introducing a cfg(feature = "picodata_executable") which will be
// off when compiling plugin code.
extern "C" {
    #[allow(improper_ctypes)]
    pub fn pico_ffi_cas(
        op: types::Op,
        predicate: types::Predicate,
        timeout: RDuration,
    ) -> RResult<ROption<RTuple!(u64, u64)>, ()>;

    pub fn pico_ffi_wait_index(index: u64, timeout: RDuration) -> RResult<ROption<u64>, ()>;

    pub fn pico_ffi_version() -> RTuple!(*const u8, usize);
    pub fn pico_ffi_rpc_version() -> RTuple!(*const u8, usize);

    #[allow(improper_ctypes)]
    pub fn pico_ffi_instance_info() -> RResult<types::InstanceInfo, ()>;

    #[allow(improper_ctypes)]
    pub fn pico_ffi_cluster_uuid() -> RResult<RString, ()>;

    pub fn pico_ffi_raft_info() -> types::RaftInfo;

    // There is false positive warning by `improper_ctypes` here.
    // We assume that this function is ffi safe because using only
    // `stable_abi` and repr "C" types here.
    #[allow(improper_ctypes)]
    pub fn pico_ffi_sql_query(
        query: *const u8,
        query_len: usize,
        params: RVec<SqlValue>,
    ) -> RResult<*mut BoxTuple, ()>;

    pub fn pico_ffi_register_rpc_handler(handler: FfiRpcHandler) -> i32;

    pub fn pico_ffi_rpc_request(
        arguments: &FfiSafeRpcRequestArguments,
        timeout: f64,
        output: *mut FfiSafeBytes,
    ) -> i32;

    pub fn pico_ffi_register_metrics_handler(handler: FfiMetricsHandler) -> i32;

    pub fn pico_ffi_background_register_job_cancellation_token(
        plugin: FfiSafeStr,
        service: FfiSafeStr,
        version: FfiSafeStr,
        job_tag: FfiSafeStr,
        token: FfiBackgroundJobCancellationToken,
    ) -> i32;

    pub fn pico_ffi_background_cancel_jobs_by_tag(
        plugin: FfiSafeStr,
        service: FfiSafeStr,
        version: FfiSafeStr,
        job_tag: FfiSafeStr,
        timeout: f64,
        result: *mut JobCancellationResult,
    ) -> i32;

    pub fn pico_ffi_background_set_jobs_shutdown_timeout(
        plugin: FfiSafeStr,
        service: FfiSafeStr,
        version: FfiSafeStr,
        timeout: f64,
    ) -> i32;

    /// See [`crate::internal::authenticate`] wrapper for more information.
    pub fn pico_ffi_authenticate(name: FfiSafeStr, password: FfiSafeBytes) -> i32;
}
