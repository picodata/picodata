#![allow(non_local_definitions)] // `#[sabi_trait]` problem

use abi_stable::{rstr, std_types::RStr};

pub mod authentication;
pub mod background;
pub mod error_code;
pub mod internal;
pub mod interplay;
pub mod log;
pub mod metrics;
pub mod plugin;
pub mod sql;
pub mod system;
pub mod transport;
pub mod util;

#[no_mangle]
pub static PICOPLUGIN_VERSION: RStr<'static> = rstr!(env!("CARGO_PKG_VERSION"));

// XXX: Tests relying on `tarantool::test` cannot work in standalone
// test binaries produced by `#[cfg(test)]` due to missing symbols,
// which means that `feature = "internal_test"` and `cfg(test)`
// are mutually exclusive.
//
// In case of picodata-plugin specifically, these tests will be included in
// picodata main binary (see picodata's dependency record for picodata-plugin),
// which means that `picodata test` command will be the one running them.
#[cfg(all(feature = "internal_test", not(test)))]
mod test_macros {
    use super::system::tarantool;

    #[tarantool::test]
    fn test() {}

    #[tarantool::proc]
    fn example() {}
}
