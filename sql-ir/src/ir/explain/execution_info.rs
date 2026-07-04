use super::buckets_repr;
use std::fmt::Display;

use crate::ir::bucket::Buckets;

#[derive(Debug)]
pub struct BoundedBuckets {
    /// Estimated buckets on which whole plan will be executed.
    pub buckets: Buckets,
    /// Total number of buckets in cluster
    pub bucket_count: u64,
}

#[derive(Debug)]
pub enum BucketsInfo {
    /// We can't calculate buckets for this query,
    /// see `can_estimate_buckets`
    Unknown,
    Calculated {
        bounded_buckets: BoundedBuckets,
        /// True if estimation is correct, otherwise
        /// it means this is an upper bound.
        is_exact: bool,
    },
}

impl Display for BucketsInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            BucketsInfo::Unknown => {
                write!(f, "buckets = unknown")
            }
            BucketsInfo::Calculated {
                bounded_buckets,
                is_exact,
            } => {
                let repr = buckets_repr(&bounded_buckets.buckets, bounded_buckets.bucket_count);
                // For buckets ANY and ALL there is no sense to handle in the
                // output the case when bucket count is not exact.
                match bounded_buckets.buckets {
                    Buckets::Any | Buckets::All => write!(f, "buckets = {repr}",),
                    _ if *is_exact => write!(f, "buckets = {repr}",),
                    _ => write!(f, "buckets <= {repr}",),
                }
            }
        }
    }
}

impl BucketsInfo {
    pub fn new_calculated(buckets: Buckets, is_exact: bool, bucket_count: u64) -> Self {
        BucketsInfo::Calculated {
            bounded_buckets: BoundedBuckets {
                buckets,
                bucket_count,
            },
            is_exact,
        }
    }
}
