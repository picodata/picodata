//! Box schema: function.
//!
//! Helpers to create and modify functions in tarantool.
//!
//! Picodata fork of tarantool has an extended range of function identifiers.
//! The first 32_000 identifiers (the maximum possible value in vanilla tarantool)
//! are reserved for local functions. The rest of the identifiers (up to i32::MAX)
//! are used for SQL procedures in picodata.

use crate::error::{Error, TarantoolError};
use crate::ffi::tarantool::box_generate_func_id;

fn next_id(use_reserved_range: bool) -> Result<u32, Error> {
    unsafe {
        let mut id: u32 = 0;
        let res = box_generate_func_id(&mut id, use_reserved_range);
        if res != 0 {
            return Err(TarantoolError::last().into());
        }
        Ok(id)
    }
}

/// Generate next function id from reserved range
/// (used for stored procedures in picodata).
pub fn func_next_reserved_id() -> Result<u32, Error> {
    next_id(true)
}

/// Generate next function id from default range
/// (used for local tarantool functions).
pub fn func_next_id() -> Result<u32, Error> {
    next_id(false)
}

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;
    use std::convert::TryInto;

    #[crate::test(tarantool = "crate")]
    pub fn test_func_next_id() {
        let id = func_next_id().unwrap();
        assert!(id > 0 && id <= 32_000);
    }

    #[crate::test(tarantool = "crate")]
    pub fn test_func_next_reserved_id() {
        let id = func_next_reserved_id().unwrap();
        assert!(id > 32_000 && id <= i32::MAX.try_into().unwrap());
    }
}
