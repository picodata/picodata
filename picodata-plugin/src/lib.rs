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

#[cfg(feature = "internal_test")]
mod test_macros {
    use super::system::tarantool;

    #[tarantool::test]
    fn test() {}

    #[tarantool::proc]
    fn example() {}
}
