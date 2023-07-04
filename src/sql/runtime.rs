//! Picodata runtime module for the clusterwide SQL.
//! Implements traits from the `sbroad` crate.

pub mod meta;
pub mod router;
pub mod storage;

pub const DEFAULT_BUCKET_COUNT: u64 = 3000;
