use crate::vshard::VshardBucketRecord;
use crate::vshard::SPACE_BUCKET;
use crate::Result;
use tarantool::index::IteratorType;
use tarantool::transaction::transaction;

pub fn read_local_buckets(start: u64, end: u64) -> Result<Vec<VshardBucketRecord>> {
    let mut res = vec![];
    for tuple in SPACE_BUCKET.select(IteratorType::GE, &[start])? {
        let bucket: VshardBucketRecord = tuple.decode()?;
        if bucket.bucket_id > end {
            break;
        }
        res.push(bucket);
    }
    Ok(res)
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
