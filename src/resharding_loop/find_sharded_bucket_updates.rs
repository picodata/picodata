use crate::catalog::pico_bucket::BucketIdRange;
use crate::tlog;
use crate::vshard::VshardBucketRecord;
use crate::vshard::VshardBucketState;
use crate::vshard::SPACE_BUCKET;
use crate::Result;
use smol_str::SmolStr;
use tarantool::index::IteratorType;

////////////////////////////////////////////////////////////////////////////////
// find_sharded_bucket_updates
////////////////////////////////////////////////////////////////////////////////

pub fn find_sharded_bucket_updates(
    range: BucketIdRange,
    from_state: Option<VshardBucketState>,
    to_state: VshardBucketState,
    expected_peer: Option<SmolStr>,
) -> Result<Vec<VshardBucketRecord>> {
    let (start, end) = (*range.start(), *range.end());
    let mut actual_buckets = vec![];
    for tuple in SPACE_BUCKET.select(IteratorType::GE, &[start])? {
        let bucket: VshardBucketRecord = tuple.decode()?;
        if bucket.bucket_id > end {
            break;
        }
        actual_buckets.push(bucket);
    }

    Ok(find_sharded_bucket_updates_impl(
        range,
        from_state,
        to_state,
        expected_peer,
        &actual_buckets,
    ))
}

fn find_sharded_bucket_updates_impl(
    range: BucketIdRange,
    from_state: Option<VshardBucketState>,
    to_state: VshardBucketState,
    expected_peer: Option<SmolStr>,
    actual_buckets: &[VshardBucketRecord],
) -> Vec<VshardBucketRecord> {
    let (start, end) = range.into_inner();

    #[rustfmt::skip]
    tlog!(Debug, "per _pico_bucket: [{start}..{end}, {to_state:?}, {expected_peer:?}]");

    let handle_no_buckets = |changes: &mut Vec<_>, start, end| {
        debug_assert!(start <= end, "{start}, {end}");
        tlog!(
            Debug,
            "adding change _bucket {start}..{end}, {to_state} -> {expected_peer:?}"
        );
        if from_state.is_none() {
            debug_assert_eq!(to_state, VshardBucketState::Receiving);

            for bucket_id in start..=end {
                changes.push(VshardBucketRecord::new(
                    bucket_id,
                    to_state,
                    expected_peer.clone(),
                ));
            }
        }
    };

    let mut changes = vec![];

    let Some(first) = actual_buckets.first() else {
        // No buckets in _bucket so far, must insert new ones
        handle_no_buckets(&mut changes, start, end);

        return changes;
    };

    let first_bucket_id = first.bucket_id;

    // Callers always select with IteratorType::GE on the range start, so the
    // slice never holds buckets before the range.
    debug_assert!(first_bucket_id >= start, "{first_bucket_id} < {start}");

    if first_bucket_id > end {
        // No _bucket buckets in target range, must insert new ones
        handle_no_buckets(&mut changes, start, end);

        return changes;
    }

    if first_bucket_id > start {
        // Some buckets missing from _bucket, must insert new ones
        handle_no_buckets(&mut changes, start, first_bucket_id - 1);
    }

    let mut last_bucket_id = first_bucket_id;

    for bucket in actual_buckets {
        let bucket_id = bucket.bucket_id;
        debug_assert!(bucket_id >= start, "{bucket_id} < {start}");

        if bucket_id > end {
            break;
        }

        if bucket_id > last_bucket_id + 1 {
            // Found gap, must fill with new buckets
            handle_no_buckets(&mut changes, last_bucket_id + 1, bucket_id - 1);
        }

        last_bucket_id = bucket_id;

        let actual_state = bucket.state;
        if Some(actual_state) == from_state {
            if actual_state != to_state {
                // Such buckets already exists, but have a different state, must update
                changes.push(VshardBucketRecord::new(
                    bucket_id,
                    to_state,
                    expected_peer.clone(),
                ));
            }
        }
    }

    if last_bucket_id < end {
        // Handle missing buckets at the end of the range
        handle_no_buckets(&mut changes, last_bucket_id + 1, end);
    }

    changes
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

mod test {
    use super::*;
    use VshardBucketState::*;

    fn call_find_sharded_bucket_updates(
        range: std::ops::RangeInclusive<u64>,
        from_state: Option<VshardBucketState>,
        to_state: VshardBucketState,
        peer: Option<&str>,
        actual_buckets: Vec<VshardBucketRecord>,
    ) -> Vec<VshardBucketRecord> {
        find_sharded_bucket_updates_impl(
            range,
            from_state,
            to_state,
            peer.map(Into::into),
            &actual_buckets,
        )
    }

    fn active(id: u64) -> VshardBucketRecord {
        VshardBucketRecord::new(id, Active, Some("rs".into()))
    }

    fn sending(id: u64) -> VshardBucketRecord {
        VshardBucketRecord::new(id, Sending, Some("rs".into()))
    }

    fn sent(id: u64) -> VshardBucketRecord {
        VshardBucketRecord::new(id, Sent, Some("rs".into()))
    }

    fn receiving(id: u64) -> VshardBucketRecord {
        VshardBucketRecord::new(id, Receiving, Some("rs".into()))
    }

    #[tarantool::test]
    fn test_find_sharded_bucket_updates() {
        //
        // No buckets -> all buckets
        //
        let existing = vec![];
        let res = call_find_sharded_bucket_updates(1..=5, None, Receiving, Some("rs"), existing);
        assert_eq!(
            &res,
            &[
                receiving(1),
                receiving(2),
                receiving(3),
                receiving(4),
                receiving(5)
            ]
        );
        let receiving_5 = res;

        //
        // Already applied
        //
        let existing = receiving_5.clone();
        let res = call_find_sharded_bucket_updates(1..=5, None, Receiving, Some("rs"), existing);
        assert_eq!(res, vec![]);

        //
        // Next transition
        //
        let existing = receiving_5;
        let res =
            call_find_sharded_bucket_updates(1..=5, Some(Receiving), Active, Some("rs"), existing);
        assert_eq!(
            res,
            vec![active(1), active(2), active(3), active(4), active(5)]
        );

        //
        // Gap at the start
        //
        let existing = vec![receiving(4), receiving(5), receiving(6)];
        let res = call_find_sharded_bucket_updates(1..=5, None, Receiving, Some("rs"), existing);
        assert_eq!(res, vec![receiving(1), receiving(2), receiving(3)]);

        //
        // Gap at the end
        //
        let existing = vec![receiving(1), receiving(2)];
        let res = call_find_sharded_bucket_updates(1..=5, None, Receiving, Some("rs"), existing);
        assert_eq!(res, vec![receiving(3), receiving(4), receiving(5)]);

        //
        // Gap in the middle
        //
        let existing = vec![receiving(1), receiving(5)];
        let res = call_find_sharded_bucket_updates(1..=5, None, Receiving, Some("rs"), existing);
        assert_eq!(res, vec![receiving(2), receiving(3), receiving(4)]);

        //
        // Multiple gaps
        //
        let existing = vec![receiving(2), receiving(4), receiving(6)];
        let res = call_find_sharded_bucket_updates(1..=7, None, Receiving, Some("rs"), existing);
        assert_eq!(
            res,
            vec![receiving(1), receiving(3), receiving(5), receiving(7)]
        );

        //
        // No buckets in range but some after
        //
        let existing = (11..=20).map(receiving).collect();
        let res = call_find_sharded_bucket_updates(1..=3, None, Receiving, Some("rs"), existing);
        assert_eq!(res, vec![receiving(1), receiving(2), receiving(3)]);

        //
        // Single element add
        //
        let existing = vec![];
        let res = call_find_sharded_bucket_updates(5..=5, None, Receiving, Some("rs"), existing);
        assert_eq!(res, vec![receiving(5)]);

        //
        // Single element ignore
        //
        let existing = vec![active(5)];
        let res = call_find_sharded_bucket_updates(5..=5, None, Receiving, Some("rs"), existing);
        assert_eq!(res, vec![]);

        //
        // Change state of all
        //
        let existing = vec![sending(1), sending(2), sending(3), sending(4)];
        let res =
            call_find_sharded_bucket_updates(1..=4, Some(Sending), Sent, Some("rs"), existing);
        assert_eq!(res, vec![sent(1), sent(2), sent(3), sent(4)]);
    }
}
