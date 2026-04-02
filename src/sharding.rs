use crate::catalog::pico_bucket::BucketRecord;
use crate::error_or_panic;
use crate::replicaset::Replicaset;
use smol_str::SmolStr;
use std::collections::HashMap;

////////////////////////////////////////////////////////////////////////////////
// BucketsInfo
////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug)]
pub struct BucketsInfo {
    /// Mapping from tier name to info about tier's buckets
    pub buckets_info_by_tier_name: HashMap<SmolStr, TierBucketsInfo>,
}

impl BucketsInfo {
    #[inline(always)]
    pub fn get(&self, tier_name: &str) -> Option<&TierBucketsInfo> {
        self.buckets_info_by_tier_name.get(tier_name)
    }

    #[inline(always)]
    pub fn iter(&self) -> impl Iterator<Item = &TierBucketsInfo> {
        self.buckets_info_by_tier_name.values()
    }

    /// Should only be called when updating [`crate::topology_cache::TopologyCache`].
    pub fn set_total_bucket_count(&mut self, tier_name: &str, count: u64) {
        let tier_name = SmolStr::new(tier_name);
        let buckets = self
            .buckets_info_by_tier_name
            .entry(tier_name.clone())
            .or_insert_with(|| TierBucketsInfo::empty(tier_name.clone()));
        buckets.total_count = count;
    }

    /// Should only be called when updating [`crate::topology_cache::TopologyCache`].
    pub fn handle_new_replicaset(&mut self, replicaset: &Replicaset) {
        let tier_name = SmolStr::new(&replicaset.tier);
        let buckets = self
            .buckets_info_by_tier_name
            .entry(tier_name.clone())
            .or_insert_with(|| TierBucketsInfo::empty(tier_name.clone()));
        buckets.add_replicaset(&replicaset.name);
    }

    /// Should only be called when updating [`crate::topology_cache::TopologyCache`].
    pub fn handle_distribution_change(
        &mut self,
        old: Option<BucketRecord>,
        new: Option<BucketRecord>,
    ) {
        if let Some(new) = new {
            // Create new or update existing bucket record
            if let Some(old) = &old {
                debug_assert_eq!(old.tier_name, new.tier_name, "primary key");
                debug_assert_eq!(old.bucket_id_start, new.bucket_id_start, "primary key");
            }

            let buckets = self
                .buckets_info_by_tier_name
                .entry(new.tier_name.clone())
                .or_insert_with(|| TierBucketsInfo::empty(new.tier_name.clone()));

            if let Some(old) = &old {
                buckets.remove_buckets(old.target_replicaset_name(), old.count())
            }

            buckets.add_buckets(new.target_replicaset_name(), new.count());

            let old_cached = buckets.ranges.insert(new);
            debug_assert_eq!(old_cached, old);
        } else if let Some(old) = old {
            // Delete bucket record
            let Some(buckets) = self.buckets_info_by_tier_name.get_mut(&old.tier_name) else {
                // Weird, but ok
                return;
            };

            buckets.remove_buckets(old.target_replicaset_name(), old.count());

            let old_cached = buckets.ranges.remove_starting_at(old.bucket_id_start);
            debug_assert_eq!(old_cached, Some(old));
        } else {
            unreachable!()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// TierBucketsInfo
////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug)]
pub struct TierBucketsInfo {
    pub tier_name: SmolStr,
    /// Total number of buckets in the tier.
    pub total_count: u64,
    /// Bucket distribution among the tier's replicasets
    pub ranges: BucketRanges,
    /// A mapping from replicaset name to bucket count. This info is derived
    /// from the bucket ranges and is stored here as optimization and updated
    /// automatically in response to even _pico_bucket update.
    pub bucket_count_by_target_replicaset_name: HashMap<SmolStr, u64>,
}

impl TierBucketsInfo {
    #[inline]
    fn empty(tier_name: SmolStr) -> Self {
        Self {
            tier_name,
            ..Default::default()
        }
    }

    fn remove_buckets(&mut self, replicaset_name: &str, to_remove: u64) {
        let Some(curr_count) = self
            .bucket_count_by_target_replicaset_name
            .get_mut(replicaset_name)
        else {
            error_or_panic!("missing bucket count info when trying to remove {to_remove} buckets from replicaset {replicaset_name}");
            return;
        };

        if *curr_count < to_remove {
            error_or_panic!("attempt to subtract with overflow: replicaset_name: {replicaset_name}, curr_count: {curr_count}, to_remove: {to_remove}");
            *curr_count = 0;
            return;
        }

        *curr_count -= to_remove;
    }

    pub(crate) fn add_replicaset(&mut self, replicaset_name: &str) {
        let old = self
            .bucket_count_by_target_replicaset_name
            .insert(SmolStr::new(replicaset_name), 0);
        if old.is_some() {
            error_or_panic!("replicaset {replicaset_name} was added more than once");
            return;
        }
    }

    pub(crate) fn add_buckets(&mut self, replicaset_name: &str, to_add: u64) {
        let Some(curr_count) = self
            .bucket_count_by_target_replicaset_name
            .get_mut(replicaset_name)
        else {
            error_or_panic!("missing bucket count info when trying to add {to_add} buckets from replicaset {replicaset_name}");
            return;
        };

        *curr_count += to_add;
    }
}

////////////////////////////////////////////////////////////////////////////////
// BucketRanges
////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone)]
pub struct BucketRanges {
    /// An array of records ordered by [`BucketRecord::bucket_id_start`].
    inner: Vec<BucketRecord>,
}

impl BucketRanges {
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline(always)]
    pub fn iter(&self) -> std::slice::Iter<'_, BucketRecord> {
        self.inner.iter()
    }

    /// Inserts the new range. Returns `Some(old)` if there was already a range
    /// with the same `bucket_id_start` value. Otherwise returns `None`.
    pub fn insert(&mut self, new_range: BucketRecord) -> Option<BucketRecord> {
        let res = self
            .inner
            .binary_search_by_key(&new_range.bucket_id_start, |r| r.bucket_id_start);
        match res {
            Ok(found_at) => {
                // A record with the same `bucket_id_start` already exists.
                // Replace it with the new one, return the old one to the
                // caller.
                let mut slot = new_range;
                std::mem::swap(&mut self.inner[found_at], &mut slot);
                return Some(slot);
            }
            Err(insert_at) => {
                // Record with exact `bucket_id_start` match is not present.
                self.inner.insert(insert_at, new_range);
                return None;
            }
        }
    }

    /// Find the bucket range with the same `bucket_id_start` and remove it from
    /// the collection. Returns `Some(old)` if such range was found and remove.
    /// Otherwise returns `None`.
    pub fn remove_starting_at(&mut self, bucket_id_start: u64) -> Option<BucketRecord> {
        let Ok(index) = self
            .inner
            .binary_search_by_key(&bucket_id_start, |r| r.bucket_id_start)
        else {
            return None;
        };
        Some(self.inner.remove(index))
    }
}
