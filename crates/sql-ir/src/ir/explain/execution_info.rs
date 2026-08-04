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

impl Display for BoundedBuckets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let repr = buckets_repr(&self.buckets, self.bucket_count);
        match self.buckets {
            Buckets::All => write!(f, "buckets <= {repr}"),
            Buckets::Any | Buckets::Filtered(_) => write!(f, "buckets = {repr}"),
        }
    }
}

impl BoundedBuckets {
    pub fn new(buckets: Buckets, bucket_count: u64) -> Self {
        BoundedBuckets {
            buckets,
            bucket_count,
        }
    }
}
