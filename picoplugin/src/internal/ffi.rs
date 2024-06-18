use crate::internal::types;
use crate::sql::types::SqlValue;
use abi_stable::derive_macro_reexports::{ROption, RResult};
use abi_stable::std_types::{RDuration, RVec};
use abi_stable::RTuple;
use tarantool::ffi::tarantool::BoxTuple;

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
}
