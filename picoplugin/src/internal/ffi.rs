use crate::internal::types;
use crate::internal::types::{InstanceInfo, Op, Predicate};
use abi_stable::derive_macro_reexports::{ROption, RResult};
use abi_stable::std_types::RDuration;
use abi_stable::RTuple;

extern "C" {
    #[allow(improper_ctypes)]
    pub fn pico_ffi_cas(
        op: Op,
        predicate: Predicate,
        timeout: RDuration,
    ) -> RResult<ROption<RTuple!(u64, u64)>, ()>;

    pub fn pico_ffi_wait_index(index: u64, timeout: RDuration) -> RResult<ROption<u64>, ()>;

    pub fn pico_ffi_version() -> RTuple!(*const u8, usize);
    pub fn pico_ffi_rpc_version() -> RTuple!(*const u8, usize);

    #[allow(improper_ctypes)]
    pub fn pico_ffi_instance_info() -> RResult<InstanceInfo, ()>;

    #[allow(improper_ctypes)]
    pub fn pico_ffi_raft_info() -> types::RaftInfo;
}
