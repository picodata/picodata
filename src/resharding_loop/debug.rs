use crate::catalog_cache::CatalogCacheRef;
use crate::simulation::platform::Platform;
use crate::tlog;
use crate::vshard::VshardBucketRecord;
use crate::vshard::VshardBucketState;
use crate::Result;
use smol_str::format_smolstr;
use smol_str::SmolStr;

pub fn log_bucket_changes(
    changes: &[VshardBucketRecord],
    topology_ref: &CatalogCacheRef,
) -> Result<()> {
    let mut iter = changes.iter().cloned();

    let Some(mut curr_bucket) = iter.next() else {
        return Ok(());
    };

    let mut end = curr_bucket.bucket_id;

    for bucket in iter {
        assert!(bucket.bucket_id > end);
        if bucket.bucket_id > end + 1
            || (bucket.state, &bucket.peer) != (curr_bucket.state, &curr_bucket.peer)
        {
            // Flush current range
            inspect_vshard_bucket_range("replace", &curr_bucket, end, topology_ref)?;
            curr_bucket.clone_from(&bucket);
        }

        end = bucket.bucket_id;
    }

    inspect_vshard_bucket_range("replace", &curr_bucket, end, topology_ref)?;

    Ok(())
}

pub fn inspect_space_bucket(platform: &impl Platform) -> Result<()> {
    let topology_ref = platform.catalog_cache().get();
    let mut iter = read_all_local_buckets(platform)?.into_iter();

    let Some(bucket) = iter.next() else {
        tlog!(Debug, "space '_bucket' is empty!");
        return Ok(());
    };

    tlog!(Debug, "space '_bucket' contents:");

    let mut curr_bucket = bucket;
    let mut end = curr_bucket.bucket_id;

    for bucket in iter {
        assert!(bucket.bucket_id > end);
        if bucket.bucket_id > end + 1
            || (bucket.state, &bucket.peer) != (curr_bucket.state, &curr_bucket.peer)
        {
            // Flush range
            inspect_vshard_bucket_range("", &curr_bucket, end, &topology_ref)?;
            curr_bucket.clone_from(&bucket);
        }

        end = bucket.bucket_id;
    }

    inspect_vshard_bucket_range("", &curr_bucket, end, &topology_ref)?;

    Ok(())
}

pub fn inspect_vshard_bucket_range(
    prefix: &str,
    bucket: &VshardBucketRecord,
    end: u64,
    topology_ref: &CatalogCacheRef,
) -> Result<()> {
    let start = bucket.bucket_id;
    let range = if start == end {
        format_smolstr!("{start}")
    } else {
        format_smolstr!("{start}..{end}")
    };

    let peer = if let Some(peer_uuid) = &bucket.peer {
        let peer_name = &topology_ref.replicaset_by_uuid(peer_uuid)?.name;
        format_smolstr!(", {peer_name}, ({peer_uuid})")
    } else {
        SmolStr::default()
    };

    let state = bucket.state;
    tlog!(Debug, "_bucket:{prefix} [{range}, {state:?}{peer}]");

    Ok(())
}

/// Reads the whole local `_bucket` space. Only used by the debugging helpers
/// below, which report on all of it.
fn read_all_local_buckets(platform: &impl Platform) -> Result<Vec<VshardBucketRecord>> {
    platform.read_local_buckets(0, u64::MAX)
}

pub fn assert_all_buckets_rw(platform: &impl Platform) -> Result<()> {
    const NON_RW_STATUSES: &[VshardBucketState] = &[
        VshardBucketState::Pinned,
        VshardBucketState::Sending,
        VshardBucketState::Sent,
        VshardBucketState::Receiving,
        VshardBucketState::Garbage,
    ];
    for bucket in read_all_local_buckets(platform)? {
        if NON_RW_STATUSES.contains(&bucket.state) {
            panic!("non rw tuple {bucket:?}");
        }
    }

    Ok(())
}
