use crate::simulation::instance::PretendInstance;
use crate::simulation::sharded_wal::ShardedEntry;
use crate::vshard::VshardBucketRecord;
use crate::vshard::VshardBucketState;

/// Ported from `vshard/storage/init.lua` `bucket_on_commit_or_rollback_f`
pub fn pretend_vshard_on_commit_trigger(instance: &PretendInstance, entry: &ShardedEntry) {
    match entry {
        ShardedEntry::WriteBuckets { buckets } => {
            for bucket in buckets {
                bucket_commit_update(instance, bucket);
            }
        }
    }

    instance.vshard_bucket_generation.update(|v| v + 1);
}

/// Ported from `vshard/storage/init.lua` `bucket_commit_update`
fn bucket_commit_update(instance: &PretendInstance, bucket: &VshardBucketRecord) {
    let mut bucket_refs = instance.bucket_refs.borrow_mut();

    let Some(bucket_ref) = bucket_refs.get_mut(&bucket.bucket_id) else {
        // Ref doesn't exist, nothing to do.
        return;
    };

    if bucket.state == VshardBucketState::Receiving {
        // It shouldn't have a ref, but prepare to anything. Could be, for
        // example, ACTIVE changed into RECEIVING manually via
        // _bucket:replace().
        bucket_refs.remove(&bucket.bucket_id);
        return;
    }

    bucket_ref.ro_lock = !bucket.state.is_readable();
    bucket_ref.rw_lock = !bucket.state.is_writable();
}
