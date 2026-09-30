use crate::catalog::pico_bucket::BucketIdRange;
use crate::catalog::pico_bucket::BucketRecord;
use crate::catalog::pico_bucket::BucketState;
use crate::catalog::pico_bucket::PicoBucket;
use crate::column_name;
use crate::storage::SystemTable;
use crate::traft::error::to_error_other;
use crate::traft::op::Dml;
use crate::Result;
use smol_str::SmolStr;
use tarantool::error::BoxError;
use tarantool::space::UpdateOps;

////////////////////////////////////////////////////////////////////////////////
// BucketRanges
////////////////////////////////////////////////////////////////////////////////

/// Represents the distribution of bucket ranges among replicasets of a given
/// tier.
///
/// The distribution of bucket ranges follows a set of invariants, which are
/// explicitly checked in [`BucketRanges::validate_finished_state`]. See that
/// function's doc-comments for the list of invariants.
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

    #[inline]
    pub fn full_range(&self) -> Option<BucketIdRange> {
        let min = self.inner.first()?.bucket_id_start;
        let max = self.inner.last()?.bucket_id_end;
        Some(min..=max)
    }

    /// Check if there is a bucket range with state `Sent` and a given `current_replicaset_name`.
    pub fn contains_sent_from(&self, current_replicaset_name: &str) -> bool {
        for range in &self.inner {
            if !range.state.is_sent() {
                continue;
            }

            if range.current_replicaset_name == current_replicaset_name {
                return true;
            }
        }

        false
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

    /// Returns index of range which contains the given `bucket_id`.
    /// Returns `None` if `bucket_id` is less than the minimum bucket id.
    ///
    /// Note that if `bucket_id` is greater than the maximum bucket id, the
    /// index of the last range is returned.
    fn lookup_index(&self, bucket_id: u64) -> Option<usize> {
        let res = self
            .inner
            .binary_search_by_key(&bucket_id, |r| r.bucket_id_start);
        match res {
            Ok(found_at) => Some(found_at),
            Err(insert_at) => insert_at.checked_sub(1),
        }
    }

    /// Returns the range which contains the given `bucket_id`, see
    /// [`Self::lookup_index`] for the edge cases.
    #[inline(always)]
    pub fn lookup_range(&self, bucket_id: u64) -> Option<&BucketRecord> {
        let index = self.lookup_index(bucket_id)?;
        self.inner.get(index)
    }

    /// Applies a change to the bucket ranges' distribution such that there is
    /// a range of buckets with given `bucket_ids` which has state `new_state`
    /// and given `current_replicaset_name` & `target_replicaset_name`.
    ///
    /// In the process of making such range some of the existing ranges in
    /// `self` may be split, or removed.
    ///
    /// It may happen that after this function there's a pair of neighbouring
    /// ranges with the same state. [`Self::normalize_ranges`] should be called
    /// to combine such ranges and reduce the overall number of ranges.
    ///
    /// Also records all changes to the ranges into `changes` in form of DML
    /// operations which should be applied to `_pico_bucket`, so that it becomes
    /// the same as `self`.
    ///
    /// # Idempotency
    ///
    /// The function is deliberately **not** idempotent.
    /// The call must change something.
    /// This is checked via a debug assertion.
    pub fn change_range_state(
        &mut self,
        bucket_ids: &BucketIdRange,
        new_state: BucketState,
        current_replicaset_name: &SmolStr,
        target_replicaset_name: &SmolStr,
        changes: &mut Vec<Dml>,
    ) -> Result<()> {
        let bucket_id_start = *bucket_ids.start();
        let bucket_id_end = *bucket_ids.end();

        let target_replicaset_name =
            (current_replicaset_name != target_replicaset_name).then_some(target_replicaset_name);

        let index = self
            .lookup_index(bucket_id_start)
            .expect("should always be called with correct arguments");

        let range = &mut self.inner[index];

        #[cfg(debug_assertions)]
        debug_assert_ne!(
            (
                &range.state,
                &range.current_replicaset_name,
                range.target_replicaset_name.as_ref()
            ),
            (&new_state, current_replicaset_name, target_replicaset_name)
        );

        #[rustfmt::skip]
        debug_assert!(range.bucket_id_start <= bucket_id_start, "{range} {bucket_id_start}");
        #[rustfmt::skip]
        debug_assert!(bucket_id_end <= range.bucket_id_end, "{range} {bucket_id_end}");
        if new_state == BucketState::Sent {
            debug_assert_eq!(range.state, BucketState::Sending);
            #[rustfmt::skip]
            debug_assert_ne!(&range.current_replicaset_name, range.target_replicaset_name());
        }

        let mut postfix_range = None;
        if bucket_id_end < range.bucket_id_end {
            // New sub-range is not the old range's postfix, need to insert a new
            // postfix sub-range.
            postfix_range = Some(range.clone());
        }
        let postfix_index;

        if range.bucket_id_start < bucket_id_start {
            // New sub-range is not the old range's prefix, update the old record
            // with new bucket_id_end and insert a new transferred sub-range.
            let mut new_range = range.clone();
            let prefix_range = range;

            let prefix_end = bucket_id_start - 1;
            changes.push(PicoBucket::dml_update(
                &prefix_range.primary_key(),
                UpdateOps::new()
                    .into_assign(column_name!(BucketRecord, bucket_id_end), prefix_end)?,
            ));
            prefix_range.bucket_id_end = prefix_end;

            new_range.bucket_id_start = bucket_id_start;
            new_range.current_replicaset_name = current_replicaset_name.clone();
            new_range.target_replicaset_name = target_replicaset_name.cloned();
            new_range.state = new_state;
            if bucket_id_end < new_range.bucket_id_end {
                new_range.bucket_id_end = bucket_id_end;
            }
            changes.push(PicoBucket::dml_insert(&new_range));
            self.inner.insert(index + 1, new_range);
            postfix_index = index + 2;
        } else {
            postfix_index = index + 1;
            // New sub-range is the old range's prefix, update the old record
            // with new state, owner info and bucket_id_end if needed.
            let new_range = range;

            let mut update_ops = UpdateOps::new();

            if &new_range.current_replicaset_name != current_replicaset_name {
                #[rustfmt::skip]
                update_ops.assign(column_name!(BucketRecord, current_replicaset_name), current_replicaset_name)?;
                new_range.current_replicaset_name = current_replicaset_name.clone();
            }
            if new_range.target_replicaset_name.as_ref() != target_replicaset_name {
                #[rustfmt::skip]
                update_ops.assign(column_name!(BucketRecord, target_replicaset_name), target_replicaset_name)?;
                new_range.target_replicaset_name = target_replicaset_name.cloned();
            }

            update_ops.assign(column_name!(BucketRecord, state), &new_state)?;
            new_range.state = new_state;

            if bucket_id_end < new_range.bucket_id_end {
                update_ops.assign(column_name!(BucketRecord, bucket_id_end), bucket_id_end)?;
                new_range.bucket_id_end = bucket_id_end;
            }

            changes.push(PicoBucket::dml_update(&new_range.primary_key(), update_ops));
        }

        let Some(mut postfix_range) = postfix_range else {
            return Ok(());
        };

        // Insert a new postfix sub-range after the transferred one

        postfix_range.bucket_id_start = bucket_id_end + 1;
        changes.push(PicoBucket::dml_insert(&postfix_range));

        self.inner.insert(postfix_index, postfix_range);

        Ok(())
    }

    /// Reduces all adjacent ranges with the same state, such that as the result
    /// there will not be pair of adjacent ranges with the same
    /// `current_replicaset_name`, `target_replicaset_name` & `state`.
    ///
    /// Also records all changes to the ranges into `changes` in form of DML
    /// operations which should be applied to `_pico_bucket`, so that it becomes
    /// the same as `self`.
    ///
    /// Note that this function assumes invariants which are checked in
    /// [`Self::validate_finished_state`].
    pub fn normalize_ranges(&mut self, changes: &mut Vec<Dml>) -> Result<()> {
        if self.inner.is_empty() {
            return Ok(());
        }

        let mut read_cursor = 0;
        let mut write_cursor = 0;
        while read_cursor < self.inner.len() {
            let start = &self.inner[read_cursor];
            let mut bucket_id_end = start.bucket_id_end;

            let mut read_end = read_cursor + 1;
            while read_end < self.inner.len() {
                let end = &self.inner[read_end];

                debug_assert_eq!(bucket_id_end + 1, end.bucket_id_start, "invariant");
                bucket_id_end = end.bucket_id_end;

                if !start.is_mergeable_with(end) {
                    break;
                }

                read_end += 1;
            }

            // Found a mergeable run of ranges
            if read_end > read_cursor + 1 {
                let new_bucket_id_end = self.inner[read_end - 1].bucket_id_end;

                // Bump the first range's upper bound
                let first_range = &mut self.inner[read_cursor];
                first_range.bucket_id_end = new_bucket_id_end;
                changes.push(PicoBucket::dml_update(
                    &first_range.primary_key(),
                    UpdateOps::new().into_assign(
                        column_name!(BucketRecord, bucket_id_end),
                        new_bucket_id_end,
                    )?,
                ));

                // Drop the remaining adjacent ranges
                for range in &self.inner[read_cursor + 1..read_end] {
                    changes.push(PicoBucket::dml_delete(&range.primary_key()));
                }
            }

            // Write cursor always goes up by 1, but read cursor sometimes jumps
            // ahead. If read cursor has advance past writer cursor, we start
            // moving the elements back overwriting the reduced ranges.
            if write_cursor != read_cursor {
                // Semantically this is `self.inner[write_cursor] = self.inner[read_cursor]`.
                // But by using `swap` we shave off some cycles on the `drop` calls
                // which will still be called via `truncate` at the end of the function.
                self.inner.swap(write_cursor, read_cursor);
            }
            write_cursor += 1;
            read_cursor = read_end;
        }

        // The tail now contains all the reduced ranges. Drop them
        self.inner.truncate(write_cursor);

        Ok(())
    }

    /// Checks the invariants:
    /// - all ranges are ordered in ascending order
    /// - all ranges are non-overlapping
    /// - all ranges are contiguous (no gaps)
    /// - `full_range` is covered
    /// - ranges are irreducible (no 2 adjacent ranges with same state & owners)
    /// - all ranges state/owner invariants hold:
    ///     - Active <=> current_master_name = target_master_name
    ///     - Sending | Sent <=> current_master_name != target_master_name
    pub fn validate_finished_state(&self, full_range: BucketIdRange) -> Result<()> {
        let mut curr_start = *full_range.start();
        let mut prev: Option<(&BucketRecord, u64)> = None;

        for (range, i) in self.inner.iter().zip(0..) {
            if let Some((prev_range, prev_i)) = prev {
                if prev_range.is_mergeable_with(range) {
                    return Err(invalid_bucket_ranges(
                        self,
                        format!(
                            "ranges #{prev_i} and #{i} are not merged: {prev_range} and {range}"
                        ),
                    )
                    .into());
                }
            }

            let bucket_id_start = range.bucket_id_start;
            let bucket_id_end = range.bucket_id_end;
            if bucket_id_start != curr_start {
                return Err(invalid_bucket_ranges(
                    self,
                    format!("range #{i} start {curr_start} expected, got {bucket_id_start}"),
                )
                .into());
            }

            if range.bucket_id_start > range.bucket_id_end {
                return Err(invalid_bucket_ranges(
                    self,
                    format!("range #{i} start {bucket_id_start} > end {bucket_id_end}"),
                )
                .into());
            }

            match range.state {
                BucketState::Active if range.target_replicaset_name.is_some() => {
                    // Active buckets must don't have a target_replicaset_name,
                    // only current_replicaset_name
                    return Err(invalid_bucket_ranges(
                        self,
                        format!("range #{i} state & target replicaset mismatch"),
                    )
                    .into());
                }
                BucketState::Sending | BucketState::Sent
                    if range.target_replicaset_name() == &range.current_replicaset_name =>
                {
                    // Sending & Sent always have
                    // target_replicaset_name != None & != current_replicaset_name,
                    // because the buckets are being transferred
                    return Err(invalid_bucket_ranges(
                        self,
                        format!("range #{i} state & target/current replicaset mismatch"),
                    )
                    .into());
                }
                _ => {}
            }

            curr_start = range.bucket_id_end + 1;
            prev = Some((range, i));
        }

        let end = *full_range.end();
        if curr_start != end + 1 {
            return Err(invalid_bucket_ranges(
                self,
                format!("last range end {end} expected, got {}", curr_start - 1),
            )
            .into());
        }

        Ok(())
    }
}

#[track_caller]
fn invalid_bucket_ranges(ranges: &BucketRanges, message: String) -> BoxError {
    to_error_other(format!(
        "invalid bucket ranges: {message} [ranges: {ranges}]"
    ))
}

impl std::fmt::Display for BucketRanges {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[")?;
        let mut iter = self.inner.iter();
        if let Some(range) = iter.next() {
            write!(f, "{range}")?;
        }
        for range in iter {
            write!(f, ", {range}")?;
        }
        write!(f, "]")?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

mod test {
    use super::*;
    use crate::storage::do_dml_on_space;
    use crate::storage::ToEntryIter;
    use crate::tlog;
    use crate::util::get_env;
    use crate::util::test_rng_seed;
    use rand::rngs::StdRng;
    use rand::RngExt;
    use rand::SeedableRng;
    use smol_str::format_smolstr;
    use tarantool::space::Space;
    use tarantool::space::SpaceType;
    use BucketState::*;

    const TIER: &str = "tier";

    fn active(bucket_ids: BucketIdRange, current: &str) -> BucketRecord {
        BucketRecord::new_active(
            TIER.into(),
            *bucket_ids.start(),
            *bucket_ids.end(),
            current.into(),
        )
    }

    fn sending(bucket_ids: BucketIdRange, current: &str, target: &str) -> BucketRecord {
        BucketRecord {
            state: Sending,
            target_replicaset_name: Some(target.into()),
            ..active(bucket_ids, current)
        }
    }

    fn sent(bucket_ids: BucketIdRange, current: &str, target: &str) -> BucketRecord {
        BucketRecord {
            state: Sent,
            ..sending(bucket_ids, current, target)
        }
    }

    fn ranges(records: &[BucketRecord]) -> BucketRanges {
        let mut ranges = BucketRanges::default();
        for record in records {
            let old = ranges.insert(record.clone());
            assert_eq!(old, None);
        }
        ranges
    }

    /// A temporary copy of `_pico_bucket` which the tests apply the [`Dml`]s
    /// produced by [`BucketRanges`] to, so that we check that these DMLs
    /// actually turn the table into the same thing the in-memory `BucketRanges`
    /// became.
    struct PretendPicoBucket {
        space: Space,
    }

    impl PretendPicoBucket {
        fn new(records: &[BucketRecord]) -> Self {
            let (space, _) = PicoBucket::create_space(
                "_pico_bucket_bucket_ranges_test",
                None,
                SpaceType::Temporary,
            )
            .unwrap();
            space.truncate().unwrap();
            for record in records {
                space.insert(record).unwrap();
            }
            Self { space }
        }

        #[track_caller]
        fn apply(&self, changes: &[Dml]) {
            for dml in changes {
                if let Err(e) = do_dml_on_space(&self.space, dml, false) {
                    panic!("failed to apply {dml:?}: {e}");
                }
            }
        }

        fn contents(&self) -> Vec<BucketRecord> {
            PicoBucket::with_id(self.space.id())
                .iter()
                .unwrap()
                .collect()
        }
    }

    impl Drop for PretendPicoBucket {
        fn drop(&mut self) {
            self.space.drop().unwrap();
        }
    }

    /// Calls [`BucketRanges::change_range_state`] on `before`, applies the
    /// resulting DMLs to a table which contains `before`, checks that the
    /// table and `BucketRanges` end up the same and returns the result.
    #[track_caller]
    fn change_range_state(
        before: &[BucketRecord],
        bucket_ids: BucketIdRange,
        new_state: BucketState,
        current: &str,
        target: &str,
    ) -> (Vec<BucketRecord>, Vec<Dml>) {
        let table = PretendPicoBucket::new(before);
        let mut ranges = ranges(before);
        let mut changes = vec![];
        ranges
            .change_range_state(
                &bucket_ids,
                new_state,
                &current.into(),
                &target.into(),
                &mut changes,
            )
            .unwrap();
        table.apply(&changes);

        let after: Vec<_> = ranges.iter().cloned().collect();
        assert_eq!(table.contents(), after);
        (after, changes)
    }

    /// Same as [`change_range_state`] but for [`BucketRanges::normalize_ranges`].
    #[track_caller]
    fn normalize_ranges(before: &[BucketRecord]) -> (Vec<BucketRecord>, Vec<Dml>) {
        let table = PretendPicoBucket::new(before);
        let mut ranges = ranges(before);
        let mut changes = vec![];
        ranges.normalize_ranges(&mut changes).unwrap();
        table.apply(&changes);

        let after: Vec<_> = ranges.iter().cloned().collect();
        assert_eq!(table.contents(), after);
        (after, changes)
    }

    #[tarantool::test]
    fn lookup_range() {
        let ranges = ranges(&[
            active(1..=10, "r1"),
            active(11..=11, "r2"),
            active(12..=20, "r3"),
        ]);

        assert_eq!(ranges.lookup_range(0), None);
        assert_eq!(ranges.lookup_range(1), Some(&active(1..=10, "r1")));
        assert_eq!(ranges.lookup_range(5), Some(&active(1..=10, "r1")));
        assert_eq!(ranges.lookup_range(10), Some(&active(1..=10, "r1")));
        assert_eq!(ranges.lookup_range(11), Some(&active(11..=11, "r2")));
        assert_eq!(ranges.lookup_range(12), Some(&active(12..=20, "r3")));
        assert_eq!(ranges.lookup_range(20), Some(&active(12..=20, "r3")));
        // Past the end the last range is returned
        assert_eq!(ranges.lookup_range(21), Some(&active(12..=20, "r3")));

        assert_eq!(BucketRanges::default().lookup_range(1), None);
    }

    #[tarantool::test]
    fn full_range() {
        assert_eq!(BucketRanges::default().full_range(), None);

        let ranges = ranges(&[active(1..=10, "r1"), active(11..=20, "r2")]);
        assert_eq!(ranges.full_range(), Some(1..=20));
    }

    #[tarantool::test]
    fn change_range_state_whole_range() {
        let before = [active(1..=10, "r1"), active(11..=20, "r2")];

        // Active -> Sending
        let (after, changes) = change_range_state(&before, 1..=10, Sending, "r1", "r2");
        assert_eq!(after, [sending(1..=10, "r1", "r2"), active(11..=20, "r2")]);
        assert_eq!(changes.len(), 1);

        // Sending -> Sent
        let (after, changes) = change_range_state(&after, 1..=10, Sent, "r1", "r2");
        assert_eq!(after, [sent(1..=10, "r1", "r2"), active(11..=20, "r2")]);
        assert_eq!(changes.len(), 1);

        // Sent -> Active on the target
        let (after, changes) = change_range_state(&after, 1..=10, Active, "r2", "r2");
        assert_eq!(after, [active(1..=10, "r2"), active(11..=20, "r2")]);
        assert_eq!(changes.len(), 1);

        // Not merged with the neighbour, that's `normalize_ranges`'s job
        let (after, changes) = normalize_ranges(&after);
        assert_eq!(after, [active(1..=20, "r2")]);
        assert_eq!(changes.len(), 2);
    }

    #[tarantool::test]
    fn change_range_state_prefix() {
        let before = [active(1..=10, "r1"), active(11..=20, "r2")];
        let (after, changes) = change_range_state(&before, 1..=3, Sending, "r1", "r2");
        assert_eq!(
            after,
            [
                sending(1..=3, "r1", "r2"),
                active(4..=10, "r1"),
                active(11..=20, "r2"),
            ]
        );
        // update + insert postfix
        assert_eq!(changes.len(), 2);
    }

    #[tarantool::test]
    fn change_range_state_suffix() {
        let before = [active(1..=10, "r1"), active(11..=20, "r2")];
        let (after, changes) = change_range_state(&before, 8..=10, Sending, "r1", "r2");
        assert_eq!(
            after,
            [
                active(1..=7, "r1"),
                sending(8..=10, "r1", "r2"),
                active(11..=20, "r2"),
            ]
        );
        // update prefix + insert new
        assert_eq!(changes.len(), 2);
    }

    #[tarantool::test]
    fn change_range_state_middle() {
        let before = [active(1..=10, "r1"), active(11..=20, "r2")];
        let (after, changes) = change_range_state(&before, 4..=6, Sending, "r1", "r2");
        assert_eq!(
            after,
            [
                active(1..=3, "r1"),
                sending(4..=6, "r1", "r2"),
                active(7..=10, "r1"),
                active(11..=20, "r2"),
            ]
        );
        // update prefix + insert new + insert postfix
        assert_eq!(changes.len(), 3);
    }

    #[tarantool::test]
    fn change_range_state_single_bucket() {
        let before = [active(1..=10, "r1")];

        let (after, _) = change_range_state(&before, 1..=1, Sending, "r1", "r2");
        assert_eq!(after, [sending(1..=1, "r1", "r2"), active(2..=10, "r1")]);

        let (after, _) = change_range_state(&before, 10..=10, Sending, "r1", "r2");
        assert_eq!(after, [active(1..=9, "r1"), sending(10..=10, "r1", "r2")]);

        let (after, _) = change_range_state(&before, 5..=5, Sending, "r1", "r2");
        #[rustfmt::skip]
        assert_eq!(after, [active(1..=4, "r1"), sending(5..=5, "r1", "r2"), active(6..=10, "r1")]);

        let before = [active(1..=1, "r1"), active(2..=2, "r2")];
        let (after, _) = change_range_state(&before, 2..=2, Sending, "r2", "r1");
        assert_eq!(after, [active(1..=1, "r1"), sending(2..=2, "r2", "r1")]);
    }

    #[tarantool::test]
    fn change_range_state_not_first_range() {
        let before = [
            active(1..=10, "r1"),
            active(11..=20, "r2"),
            active(21..=30, "r3"),
        ];
        let (after, _) = change_range_state(&before, 15..=16, Sending, "r2", "r3");
        assert_eq!(
            after,
            [
                active(1..=10, "r1"),
                active(11..=14, "r2"),
                sending(15..=16, "r2", "r3"),
                active(17..=20, "r2"),
                active(21..=30, "r3"),
            ]
        );
    }

    #[tarantool::test]
    fn change_range_state_part_of_sending_range() {
        let before = [sending(1..=10, "r1", "r2")];

        // A part of the range is sent, the rest is still sending
        let (after, _) = change_range_state(&before, 1..=4, Sent, "r1", "r2");
        assert_eq!(
            after,
            [sent(1..=4, "r1", "r2"), sending(5..=10, "r1", "r2")]
        );

        // The sent part is activated on the target
        let (after, _) = change_range_state(&after, 1..=4, Active, "r2", "r2");
        assert_eq!(after, [active(1..=4, "r2"), sending(5..=10, "r1", "r2")]);

        // The rest of the range gets sent in the middle of the range
        let (after, _) = change_range_state(&after, 7..=8, Sent, "r1", "r2");
        assert_eq!(
            after,
            [
                active(1..=4, "r2"),
                sending(5..=6, "r1", "r2"),
                sent(7..=8, "r1", "r2"),
                sending(9..=10, "r1", "r2"),
            ]
        );
    }

    #[tarantool::test]
    fn change_range_state_nothing_to_merge() {
        // `change_range_state` does not merge the adjacent ranges even if
        // they're mergeable after the change
        let before = [active(1..=5, "r2"), sent(6..=10, "r1", "r2")];
        let (after, _) = change_range_state(&before, 6..=10, Active, "r2", "r2");
        assert_eq!(after, [active(1..=5, "r2"), active(6..=10, "r2")]);

        let before = [sending(1..=5, "r1", "r2"), active(6..=10, "r1")];
        let (after, _) = change_range_state(&before, 6..=7, Sending, "r1", "r2");
        assert_eq!(
            after,
            [
                sending(1..=5, "r1", "r2"),
                sending(6..=7, "r1", "r2"),
                active(8..=10, "r1"),
            ]
        );
    }

    #[tarantool::test]
    fn normalize_ranges_no_changes() {
        let (after, changes) = normalize_ranges(&[]);
        assert_eq!(after, []);
        assert_eq!(changes.len(), 0);

        let before = [active(1..=10, "r1")];
        let (after, changes) = normalize_ranges(&before);
        assert_eq!(after, before);
        assert_eq!(changes.len(), 0);

        // Adjacent ranges which differ in exactly one of the fields
        let before = [
            active(1..=10, "r1"),
            active(11..=20, "r2"),
            sending(21..=30, "r2", "r1"),
            sending(31..=40, "r2", "r3"),
            sent(41..=50, "r2", "r3"),
            sent(51..=60, "r1", "r3"),
            active(61..=70, "r1"),
        ];
        let (after, changes) = normalize_ranges(&before);
        assert_eq!(after, before);
        assert_eq!(changes.len(), 0);
    }

    #[tarantool::test]
    fn normalize_ranges_everything_merged() {
        let before = [
            active(1..=1, "r1"),
            active(2..=10, "r1"),
            active(11..=20, "r1"),
            active(21..=21, "r1"),
        ];
        let (after, changes) = normalize_ranges(&before);
        assert_eq!(after, [active(1..=21, "r1")]);
        // update the first + delete the rest
        assert_eq!(changes.len(), 4);
    }

    #[tarantool::test]
    fn normalize_ranges_several_groups() {
        let before = [
            active(1..=10, "r1"),
            active(11..=20, "r1"),
            sending(21..=30, "r1", "r2"),
            active(31..=40, "r1"),
            sent(41..=50, "r1", "r2"),
            sent(51..=60, "r1", "r2"),
            sent(61..=70, "r1", "r2"),
            active(71..=80, "r2"),
            active(81..=90, "r2"),
        ];
        let (after, changes) = normalize_ranges(&before);
        assert_eq!(
            after,
            [
                active(1..=20, "r1"),
                sending(21..=30, "r1", "r2"),
                active(31..=40, "r1"),
                sent(41..=70, "r1", "r2"),
                active(71..=90, "r2"),
            ]
        );
        assert_eq!(changes.len(), 2 + 3 + 2);

        // Normalizing twice changes nothing
        let (again, changes) = normalize_ranges(&after);
        assert_eq!(again, after);
        assert_eq!(changes.len(), 0);
    }

    #[tarantool::test]
    fn validate_finished_state() {
        #[track_caller]
        fn check(records: &[BucketRecord], full_range: BucketIdRange) -> Result<()> {
            ranges(records).validate_finished_state(full_range)
        }

        #[track_caller]
        fn check_err(records: &[BucketRecord], full_range: BucketIdRange, expected: &str) {
            let e = check(records, full_range).unwrap_err().to_string();
            assert!(e.contains(expected), "{e}");
        }

        check(&[active(1..=10, "r1")], 1..=10).unwrap();
        #[rustfmt::skip]
        check(&[active(1..=10, "r1"), sending(11..=12, "r1", "r2"), sent(13..=15, "r1", "r2"), active(16..=20, "r2")], 1..=20).unwrap();

        // Empty
        check_err(&[], 1..=10, "last range end 10 expected, got 0");

        // Gap at the start
        check_err(
            &[active(2..=10, "r1")],
            1..=10,
            "range #0 start 1 expected, got 2",
        );

        // Gap in the middle
        #[rustfmt::skip]
        check_err(&[active(1..=4, "r1"), active(6..=10, "r2")], 1..=10, "range #1 start 5 expected, got 6");

        // Overlap
        #[rustfmt::skip]
        check_err(&[active(1..=5, "r1"), active(5..=10, "r2")], 1..=10, "range #1 start 6 expected, got 5");

        // Short at the end
        check_err(
            &[active(1..=9, "r1")],
            1..=10,
            "last range end 10 expected, got 9",
        );

        // Too long at the end
        check_err(
            &[active(1..=11, "r1")],
            1..=10,
            "last range end 10 expected, got 11",
        );

        // Empty range
        #[rustfmt::skip]
        check_err(&[active(1..=5, "r1"), BucketRecord { bucket_id_start: 6, bucket_id_end: 5, ..active(1..=1, "r2") }], 1..=5, "range #1 start 6 > end 5");

        // Not merged
        #[rustfmt::skip]
        check_err(&[active(1..=5, "r1"), active(6..=10, "r1")], 1..=10, "ranges #0 and #1 are not merged");

        // Active with a target
        #[rustfmt::skip]
        check_err(&[BucketRecord { target_replicaset_name: Some("r2".into()), ..active(1..=10, "r1") }], 1..=10, "range #0 state & target replicaset mismatch");

        // Sending/Sent without a target
        #[rustfmt::skip]
        check_err(&[sending(1..=10, "r1", "r1")], 1..=10, "range #0 state & target/current replicaset mismatch");
        #[rustfmt::skip]
        check_err(&[BucketRecord { target_replicaset_name: None, ..sent(1..=10, "r1", "r2") }], 1..=10, "range #0 state & target/current replicaset mismatch");
    }

    /// Transfers random sub-ranges between random replicasets the way the
    /// resharding loop does it (`Active` -> `Sending` -> `Sent` -> `Active` on
    /// the target) and checks after each step that the in-memory ranges, the
    /// table the DMLs are applied to and a per-bucket model all agree.
    fn do_change_range_state_random_transfers(seed: u64) {
        let bucket_count = get_env("BUCKET_COUNT").unwrap_or(60000);
        let num_replicasets = get_env("NUM_REPLICASETS").unwrap_or(30);
        let num_steps = get_env("NUM_STEPS").unwrap_or(1000);
        let normalize_run_max = get_env("NORMALIZE_RUN_MAX").unwrap_or(100);
        let normalize_run_min = get_env("NORMALIZE_RUN_MIN").unwrap_or(1);

        let replicasets: Vec<_> = (1..=num_replicasets)
            .into_iter()
            .map(|i| format_smolstr!("r{i}"))
            .collect();

        let mut prev_normalize_step = 0;
        let mut run_count = 0;
        let mut run_min = usize::MAX;
        let mut run_sum = 0;
        let mut run_max = 0;

        let mut num_inserts = 0;
        let mut num_updates = 0;
        let mut num_deletes = 0;

        let initial = [active(1..=bucket_count, &replicasets[0])];
        let table = PretendPicoBucket::new(&initial);
        let mut ranges = ranges(&initial);

        tlog!(Info, "random seed: {seed}");
        let mut rng = StdRng::seed_from_u64(seed);

        let roll_normalize_run =
            |rng: &mut StdRng| rng.random_range(normalize_run_min..=normalize_run_max);
        let mut next_normalize_step = roll_normalize_run(&mut rng).min(num_steps);

        for step in 1..=num_steps {
            let ctx = format!("seed: {seed}, step: {step}, ranges: {ranges}");

            // Pick a sub-range of a random range
            let index = rng.random_range(0..ranges.len());
            let range = ranges.iter().nth(index).unwrap().clone();
            let start = rng.random_range(range.bucket_id_start..=range.bucket_id_end);
            let end = rng.random_range(start..=range.bucket_id_end);

            let current = &range.current_replicaset_name;
            let target = range.target_replicaset_name();
            let (new_state, new_current, new_target) = match range.state {
                Active => {
                    let i = rng.random_range(0..(replicasets.len() - 1));
                    let mut new_target = &replicasets[i];
                    if new_target == current {
                        new_target = &replicasets[i + 1];
                    }
                    (Sending, current, new_target)
                }
                Sending => (Sent, current, target),
                Sent => (Active, target, target),
                Unknown(_) => unreachable!(),
            };

            let mut changes = vec![];
            ranges
                .change_range_state(
                    &(start..=end),
                    new_state.clone(),
                    new_current,
                    new_target,
                    &mut changes,
                )
                .unwrap();

            // Check that requested changes very applied
            let i_start = ranges.lookup_index(start).unwrap();
            let i_end = ranges.lookup_index(end).unwrap();
            assert_eq!(
                i_start, i_end,
                "change_range_state always makes a single range from the provided parameters"
            );
            let range = &ranges.inner[i_start];
            assert_eq!(range.state, new_state);
            assert_eq!(&range.current_replicaset_name, new_current);
            assert_eq!(range.target_replicaset_name(), new_target);

            // Maybe normalize. Don't do it every time to check what happens when
            // we normalize after several changes in a row.
            //
            // On the last iteration always noramlize and validate
            if step == next_normalize_step {
                ranges.normalize_ranges(&mut changes).unwrap();
                ranges.validate_finished_state(1..=bucket_count).unwrap();

                next_normalize_step += roll_normalize_run(&mut rng);
                next_normalize_step = next_normalize_step.min(num_steps);

                let run = step - prev_normalize_step;
                prev_normalize_step = step;
                run_sum += run;
                run_count += 1;
                run_max = run_max.max(run);
                run_min = run_min.min(run);
            }

            for dml in &changes {
                match dml {
                    Dml::Insert { .. } => num_inserts += 1,
                    Dml::Replace { .. } => unreachable!(),
                    Dml::Update { .. } => num_updates += 1,
                    Dml::Delete { .. } => num_deletes += 1,
                }
            }

            // Apply the DMLs and make sure they result in the same outcome
            table.apply(&changes);
            let contents = table.contents();
            assert_eq!(
                contents,
                ranges.iter().cloned().collect::<Vec<_>>(),
                "{ctx}"
            );
        }

        let num_ranges = ranges.len();
        let run_avg = run_sum as f64 / (run_count as f64);
        tlog!(
            Info,
            "statistics:
        changes: {num_steps}
        final ranges: {num_ranges}
        inserts: {num_inserts}
        updates: {num_updates}
        deletes: {num_deletes}
        normalize runs: min={run_min}, avg={run_avg:.1}, max={run_max}, count={run_count}"
        );
    }

    #[tarantool::test]
    fn change_range_state_random_transfers_fixed() {
        let seed = 1791213525013812;
        do_change_range_state_random_transfers(seed);
    }

    #[tarantool::test]
    fn change_range_state_random_transfers_random() {
        let seed = test_rng_seed();
        do_change_range_state_random_transfers(seed);
    }
}
