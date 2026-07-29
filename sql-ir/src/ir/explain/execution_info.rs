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
    Calculated(BoundedBuckets),
}

impl Display for BucketsInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            BucketsInfo::Unknown => {
                write!(f, "buckets = unknown")
            }
            BucketsInfo::Calculated(bounded_buckets) => {
                let repr = buckets_repr(&bounded_buckets.buckets, bounded_buckets.bucket_count);
                match bounded_buckets.buckets {
                    Buckets::All => write!(f, "buckets <= {repr}"),
                    Buckets::Any | Buckets::Filtered(_) => write!(f, "buckets = {repr}"),
                }
            }
        }
    }
}

impl BucketsInfo {
    pub fn new_calculated(buckets: Buckets, bucket_count: u64) -> Self {
        BucketsInfo::Calculated(BoundedBuckets {
            buckets,
            bucket_count,
        })
    }
}
