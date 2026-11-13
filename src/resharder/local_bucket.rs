use crate::simulation::platform::Platform;
use crate::vshard::VshardBucketRecord;
use crate::vshard::SPACE_BUCKET;
use crate::Result;
use tarantool::index::IteratorType;
use tarantool::transaction::transaction;

/// Reads the whole local `_bucket` space.
pub fn read_all_local_buckets(platform: &impl Platform) -> Result<Vec<VshardBucketRecord>> {
    platform.read_local_buckets(0, u64::MAX)
}

pub fn read_local_buckets(start: u64, end: u64) -> Result<Vec<VshardBucketRecord>> {
    let mut res = vec![];
    for tuple in SPACE_BUCKET.select_v2(IteratorType::GE, &[start])? {
        let bucket: VshardBucketRecord = tuple?.decode()?;
        if bucket.bucket_id > end {
            break;
        }
        res.push(bucket);
    }
    Ok(res)
}

pub fn lookup_local_bucket(bucket_id: u64) -> Result<Option<VshardBucketRecord>> {
    let Some(tuple) = SPACE_BUCKET.get(&[bucket_id])? else {
        return Ok(None);
    };
    let res = tuple.decode()?;
    Ok(Some(res))
}

pub fn write_local_buckets(buckets: Vec<VshardBucketRecord>) -> Result<()> {
    transaction(|| -> Result<_> {
        for bucket in buckets {
            SPACE_BUCKET.replace(&bucket)?;
        }

        // TODO(sharding): update _schema.local_bucket_state_version so
        // that replicas can synchronize global _pico_bucket updates
        // with sharded table updates

        Ok(())
    })?;
    Ok(())
}
