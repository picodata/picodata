use crate::catalog::pico_bucket::BucketIdRange;
use crate::resharding_loop::plan::ReshardingPlanContext;
use crate::resharding_loop::sharded_tables::ShardedTableInfo;
use crate::resharding_loop::sharded_tables::TableData;
use crate::simulation::platform::Platform;
use crate::storage::cached_key_def;
use crate::tlog;
use crate::tlog_ratelimited;
use crate::traft::error::with_modified_message;
use crate::traft::error::Error;
use crate::vshard::VshardBucketState as VBS;
use crate::Result;
use picodata_plugin::util::RegionGuard;
use std::collections::HashMap;
use tarantool::error::BoxError;
use tarantool::index::TuplePosition;
use tarantool::space::SpaceId;
use tarantool::tuple::KeyDef;
use tarantool::tuple::Tuple;
use tarantool::tuple::TupleBuffer;

/// Walks the data of the sharded tables in the buckets with ids in the given
/// `range` and handles the data differently depending on the given `mode`.
/// `range` must not be empty.
///
/// If `mode` is [`WalkShardedDataMode::DetermineRangeToSend`] then we only
/// determine how much data in the `range` doesn't have any `rw` refs and fits
/// into a chunk. Returns an error if all the buckets in `range` have `rw` refs.
///
/// If `mode` is [`WalkShardedDataMode::GetDataChunkToSend`] then we assume the
/// range was already checked for `rw` refs and locked, so we collect the tuple
/// data into a chunk to be sent to another replicaset.
///
/// If `mode` is [`WalkShardedDataMode::GetKeysChunkToDelete`] then we assume
/// the data has already been sent and we only collect the keys of the tuples
/// for deletion.
///
/// Returns an error if the index iterator returned an error before any tuples
/// were added to the chunk.
///
/// In all modes the resulting "chunk" of the data is limited in size in the
/// following ways. The chunk's size in bytes is at most
/// [`AlterSystemParameters::resharding_chunk_size_max`] unless it consists of a
/// single tuple which exceeds that limit. The chunk contains data of at most
/// [`AlterSystemParameters::resharding_bucket_count_max`] or `left_to_send`
/// buckets (depending on which one is smaller).
///
/// The walk starts at `saved_offset` if it's provided, i.e. it continues
/// where the previous walk stopped (see [`WalkShardedDataResult::next_offset`]).
/// Note that the caller must make sure to call with the same `saved_offset` in
/// case this function returned an error.
///
/// [`AlterSystemParameters::resharding_chunk_size_max`]: crate::config::AlterSystemParameters::resharding_chunk_size_max
/// [`AlterSystemParameters::resharding_bucket_count_max`]: crate::config::AlterSystemParameters::resharding_bucket_count_max
pub fn walk_sharded_data_in_range(
    platform: &impl Platform,
    range: BucketIdRange,
    mode: WalkShardedDataMode,
    mut saved_offset: Option<&WalkShardedDataOffset>,
    left_to_send: u64,
    context: &ReshardingPlanContext<'_>,
) -> Result<WalkShardedDataResult> {
    debug_assert!(!range.is_empty());
    if mode != WalkShardedDataMode::GetKeysChunkToDelete {
        debug_assert_ne!(left_to_send, 0);
    }

    let chunk_size_max = context.resharding_chunk_size_max as usize;
    let bucket_count_max = context.resharding_bucket_count_max;

    let mut bucket_id_first = *range.start();

    if let Some(offset) = saved_offset {
        debug_assert!(range.contains(&offset.bucket_id));

        let v_expected = offset.schema_version;
        let v_actual = platform.box_schema_version();
        if v_expected != v_actual {
            tlog!(Warning, "schema changed while resharding is in progress, schema version {v_expected} -> {v_actual}");
            // Can't use the saved offset, as TuplePosition may have become
            // out of date, or there may be new tables we would miss, etc.
            saved_offset = None;
        } else {
            bucket_id_first = offset.bucket_id;
        }
    }

    // The end boundary is going to be adjusted based on several criteria
    let mut bucket_id_last_complete = None;
    let mut bucket_id_last_touched = None;
    let mut next_offset = None;

    let mut table_dumps = HashMap::new();
    let mut total_size = 0;
    let mut tuples_count = 0;

    let mut buckets_with_data = 0;
    let mut iterator_error = None;

    let t0 = std::time::Instant::now();

    // `bucket_id_first` is bumped inside the loop to skip the leading buckets
    // which can't be sent, not to affect this range, which is fixed for the
    // duration of the loop.
    #[allow(clippy::mut_range_bound)]
    'buckets_loop: for bucket_id in bucket_id_first..=*range.end() {
        let old_total_size = total_size;

        if mode == WalkShardedDataMode::DetermineRangeToSend {
            let bucket_ref = platform.get_bucket_ref(bucket_id)?;
            if bucket_ref.is_some_and(|r| !r.allows_state(VBS::Sending)) {
                if bucket_id_last_complete.is_some() {
                    // Found some buckets that we can send, let's start
                    // transferring them
                    tlog!(Debug, "ending chunk because of RW ref: {bucket_ref:?}");
                    break;
                }
                debug_assert_eq!(buckets_with_data, 0);

                // Didn't see any usable buckets yet, lets adjust our search
                // area and keep going
                bucket_id_first = bucket_id + 1;
                bucket_id_last_touched = None;
                continue;
            }
        }

        for table_info in context.sharded_tables {
            if saved_offset.is_some_and(|offset| table_info.id != offset.table_id) {
                continue;
            }

            // NOTE: default position = beginning of the table
            let mut saved_position = TuplePosition::default();
            if let Some(offset) = saved_offset.take() {
                debug_assert_eq!(offset.table_id, table_info.id);
                saved_position = offset.position.clone();
            }

            let mut iter = platform
                .index_bucket_id_select_after(
                    table_info.id,
                    table_info.sharding_index_id,
                    bucket_id,
                    &saved_position,
                )?
                .peekable();

            if iter.peek().is_none() {
                // No data in this bucket of this table
                continue;
            }

            let mut primary_key_def = None;
            if mode == WalkShardedDataMode::GetKeysChunkToDelete {
                let primary_key_id = 0;
                primary_key_def = Some(cached_key_def(table_info.id, primary_key_id)?);
            }

            let mut array_writer = None;
            if mode != WalkShardedDataMode::DetermineRangeToSend {
                array_writer = Some(
                    table_dumps
                        .entry(table_info.id)
                        .or_insert_with(|| ArrayWriter::from_vec(vec![])),
                );
            }

            // The last tuple of this table and this bucket added to the chunk
            let mut last_tuple: Option<Tuple> = None;

            for maybe_tuple in iter {
                let mut tuple = None;
                match maybe_tuple {
                    Ok(v) => tuple = Some(v),
                    Err(e) => {
                        if tuples_count == 0 {
                            // No tuples added to chunk
                            return Err(chunking_failed(e, mode));
                        }

                        // We already have some tuples in the chunk, the error
                        // may not be that critical, so let's try handling the
                        // chunk.
                        iterator_error = Some(e);
                    }
                }

                let guard;
                let mut data = b"".as_slice();
                if let Some(tuple) = &tuple {
                    debug_assert_eq!(
                        tuple.field::<u64>(table_info.bucket_id_column_index)?,
                        Some(bucket_id)
                    );

                    if let Some(primary_key_def) = &primary_key_def {
                        guard = RegionGuard::new();
                        // SAFETY: safe as long as nobody truncates the region allocator
                        // before we copy the data into the array_writer below
                        data = unsafe { extract_key(primary_key_def, tuple, &guard)? };
                    } else {
                        data = tuple.data();
                    }
                }

                let bytes_to_add = data.len();
                let chunk_overflow = total_size + bytes_to_add > chunk_size_max && total_size != 0;
                if iterator_error.is_some() || chunk_overflow {
                    // The current chunk threshold has been hit. Break out of
                    // the loop, don't update `bucket_id_last_complete`, because
                    // the bucket may not have been checked fully, we'll
                    // continue checking it next time
                    next_offset = Some(make_saved_offset(
                        platform,
                        saved_position,
                        bucket_id,
                        table_info,
                        &last_tuple,
                    ));

                    if let Some(e) = &iterator_error {
                        tlog_ratelimited!(Warning, "ending chunk due to iterator error: {e}");
                    } else {
                        tlog!(
                            Debug,
                            "ending chunk by size: {total_size} + {bytes_to_add} > {chunk_size_max}"
                        );
                    }

                    break 'buckets_loop;
                }

                if let Some(array_writer) = &mut array_writer {
                    array_writer.push_raw(data)?;
                }

                total_size += bytes_to_add;
                tuples_count += 1;

                bucket_id_last_touched = Some(bucket_id);
                last_tuple = Some(tuple.expect("can only get here if there was a tuple"));
            }
        }

        bucket_id_last_touched = Some(bucket_id);
        bucket_id_last_complete = Some(bucket_id);

        if total_size != old_total_size {
            buckets_with_data += 1;

            if buckets_with_data >= bucket_count_max {
                tlog!(Debug, "hit resharding bucket count threshold: {buckets_with_data} >= {bucket_count_max}, ending chunk");
                break;
            }
        }

        if mode != WalkShardedDataMode::GetKeysChunkToDelete {
            if let Some(bucket_id_last_complete) = bucket_id_last_complete {
                let bucket_count = bucket_id_last_complete - bucket_id_first + 1;
                if bucket_count >= left_to_send {
                    tlog!(
                        Debug,
                        "hit requested transfer bucket count: {bucket_count} >= {left_to_send}"
                    );
                    break;
                }
            }
        }
    }

    let Some(bucket_id_last_touched) = bucket_id_last_touched else {
        // Range is never empty so we always touch at least one bucket.
        // The only expection is DetermineRangeToSend mode and all buckets having rw refs.
        debug_assert_eq!(mode, WalkShardedDataMode::DetermineRangeToSend);

        return Err(Error::other(format!(
            "all buckets in range {range:?} have rw refs, can't start sending any of them"
        )));
    };

    if total_size == 0 {
        debug_assert!(table_dumps.values().all(|writer| writer.is_empty()));
    }

    let mut table_data = Vec::with_capacity(table_dumps.len());
    let mut total_bytes = 0;
    for (table_id, array_writer) in table_dumps {
        let num_tuples = array_writer.len();
        if num_tuples == 0 {
            // Could have an empty writer in case of iterator error
            continue;
        }

        let table_name = &context
            .sharded_tables
            .iter()
            .find(|info| info.id == table_id)
            .unwrap()
            .name;

        let tuples = array_writer_into_tuple_buffer(array_writer)?;
        let num_bytes = tuples.len();
        total_bytes += num_bytes;
        tlog!(Debug, "prepared table dump chunk for table #{table_id} '{table_name}': {num_tuples} tuples ({num_bytes} bytes)");

        table_data.push(TableData { table_id, tuples })
    }

    if tuples_count > 0 {
        let tables_count = table_data.len();
        let reason = match mode {
            WalkShardedDataMode::DetermineRangeToSend => "locking",
            WalkShardedDataMode::GetDataChunkToSend => "sending",
            WalkShardedDataMode::GetKeysChunkToDelete => "removing",
        };
        tlog!(
            Debug,
            "preparing buckets for {reason} took {:?} [{tables_count} tables, {tuples_count} tuples, {total_bytes} bytes]",
            t0.elapsed(),
        );
    }

    Ok(WalkShardedDataResult {
        table_data,
        tuples_count,
        bucket_id_first,
        bucket_id_last_touched,
        bucket_id_last_complete,
        next_offset,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WalkShardedDataMode {
    DetermineRangeToSend,
    GetDataChunkToSend,
    GetKeysChunkToDelete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkShardedDataResult {
    /// Sharded table data collected.
    /// In [`WalkShardedDataMode::DetermineRangeToSend`] mode this is empty.
    /// In [`WalkShardedDataMode::GetDataChunkToSend`] mode this is tuples.
    /// In [`WalkShardedDataMode::GetKeysChunkToDelete`] mode this is keys.
    pub table_data: Vec<TableData>,

    /// Number of tuples covered by `table_data`
    pub tuples_count: u64,

    /// `table_data` covers bucket id range starting from this bucket id. Note
    /// that it's not necessarily all of the data in this bucket.
    pub bucket_id_first: u64,

    /// `table_data` at least paritally covers bucket id range ending with this
    /// bucket id. Note that it's not necessarily all of the data in this bucket.
    pub bucket_id_last_touched: u64,

    /// After `table_data` there's no more sharded data in this bucket, so it
    /// may be marked as `sent` after the data is successfully transferred.
    /// This is `None` if the data only partially covers a single bucket.
    pub bucket_id_last_complete: Option<u64>,

    /// Where the next walk should start from, if the current chunk doesn't
    /// cover the whole bucket.
    pub next_offset: Option<WalkShardedDataOffset>,
}

impl WalkShardedDataResult {
    pub fn complete_range(&self) -> Option<BucketIdRange> {
        Some(self.bucket_id_first..=self.bucket_id_last_complete?)
    }

    pub fn touched_range(&self) -> BucketIdRange {
        self.bucket_id_first..=self.bucket_id_last_touched
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WalkShardedDataOffset {
    pub bucket_id: u64,
    pub table_id: SpaceId,
    pub schema_version: u64,
    pub position: TuplePosition,
}

fn make_saved_offset(
    platform: &impl Platform,
    saved_position: TuplePosition,
    bucket_id: u64,
    table_info: &ShardedTableInfo,
    last_tuple: &Option<Tuple>,
) -> WalkShardedDataOffset {
    // Save the position of the last tuple added to the chunk.
    // Next chunk will start right after this position.
    //
    // Fallback to the position at which the current iteration
    // started.
    let mut position = saved_position;
    if let Some(last_tuple) = &last_tuple {
        // SAFETY: tuple came from space, so no UB
        let res = unsafe {
            platform.tuple_position(
                table_info.id,
                table_info.sharding_index_id,
                last_tuple.data(),
            )
        };
        position = res.expect("cannot fail for valid tuple");
    }

    WalkShardedDataOffset {
        bucket_id,
        table_id: table_info.id,
        position,
        schema_version: platform.box_schema_version(),
    }
}

type ArrayWriter = tarantool::msgpack::ArrayWriter<std::io::Cursor<Vec<u8>>>;

fn array_writer_into_tuple_buffer(writer: ArrayWriter) -> Result<TupleBuffer> {
    let tuples = writer.finish()?.into_inner();
    // SAFETY: ArrayWriter guarantees the result is a valid msgpack array
    Ok(unsafe { TupleBuffer::from_vec_unchecked(tuples) })
}

/// Extract the key described by `key_def` from `tuple` and returns a slice of
/// bytes allocated on tarantool's region allocator.
///
/// # SAFETY
///
/// The `guard` parameter is our best effort way to guarantee that the returned
/// slice become dangling. It's unfortunately not possible the caller to call
/// `box_region_truncate` directly and foil our efforts.
unsafe fn extract_key<'r>(
    key_def: &KeyDef,
    tuple: &Tuple,
    _guard: &'r RegionGuard,
) -> Result<&'r [u8]> {
    #[cfg(debug_assertions)]
    key_def
        .validate_tuple(tuple)
        .expect("came from the same space");

    // SAFETY: validate_tuple call above guarantees the safety. Also we use
    // the `guard` parameter to pretend that the returned data is protected
    let data = unsafe { key_def.extract_key_raw(tuple, -1)? };
    Ok(data)
}

fn chunking_failed(e: BoxError, mode: WalkShardedDataMode) -> Error {
    let what = match mode {
        WalkShardedDataMode::DetermineRangeToSend => {
            "failed to determine bucket range for transfer"
        }
        WalkShardedDataMode::GetDataChunkToSend => "failed to generate a data chunk for resharding",
        WalkShardedDataMode::GetKeysChunkToDelete => "failed to generate a keys chunk for clean up",
    };

    let message = format!("{what}: {}", e.message());
    with_modified_message(e, message).into()
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

mod test {
    use super::*;
    use crate::resharding_loop::test::setup_by_spec;
    use crate::simulation::cluster::PretendCluster;
    use crate::simulation::instance::PretendInstance;
    use crate::storage::index_by_ids_unchecked;
    use crate::storage::space_by_id_unchecked;
    use crate::util::test_rng_seed;
    use rand::rngs::StdRng;
    use rand::RngExt;
    use rand::SeedableRng;
    use std::rc::Rc;
    use tarantool::index::IteratorType;
    use tarantool::msgpack::ValueIter;
    use tarantool::space::Field;
    use tarantool::space::FieldType;
    use tarantool::space::Space;
    use tarantool::space::SpaceEngineType;
    use tarantool::space::SpaceId;
    use tarantool::space::SpaceType;
    use WalkShardedDataMode::*;

    /// A temporary table with the same layout as a sharded user table:
    /// `(id, bucket_id, payload)`, primary key on `id` and a non-unique
    /// `bucket_id` index.
    struct PretendShardedTable {
        space: Space,
        info: ShardedTableInfo,
    }

    #[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
    struct TestTableRow {
        id: u64,
        bucket_id: u64,
        payload: String,
    }
    impl tarantool::tuple::Encode for TestTableRow {}

    impl TestTableRow {
        const FIELD_ID: u32 = 0;
        const FIELD_BUCKET_ID: u32 = 1;
    }

    impl PretendShardedTable {
        fn new(name: &str) -> Self {
            let name = format!("_walk_sharded_data_test_{name}");
            if let Some(space) = Space::find(&name) {
                space.drop().unwrap();
            }

            let space = Space::builder(&name)
                .space_type(SpaceType::Temporary)
                .engine(SpaceEngineType::Memtx)
                .format([
                    Field::from(("id", FieldType::Unsigned)),
                    Field::from(("bucket_id", FieldType::Unsigned)),
                    Field::from(("payload", FieldType::String)),
                ])
                .create()
                .unwrap();
            space
                .index_builder("primary")
                .id(0)
                .unique(true)
                .part("id")
                .create()
                .unwrap();
            space
                .index_builder("bucket_id")
                .id(1)
                .unique(false)
                .part("bucket_id")
                .create()
                .unwrap();

            let info = ShardedTableInfo {
                id: space.id(),
                name: name.into(),
                sharding_index_id: 1,
                bucket_id_column_index: 1,
            };

            Self { space, info }
        }

        fn insert(&self, id: u64, bucket_id: u64, payload_len: usize) -> Tuple {
            self.space
                .insert(&TestTableRow {
                    id,
                    bucket_id,
                    payload: "x".repeat(payload_len),
                })
                .unwrap()
        }

        fn ids(&self) -> Vec<u64> {
            let mut result = vec![];
            let iter = self.space.select_v2(IteratorType::All, &()).unwrap();
            for tuple in iter {
                let id = tuple
                    .unwrap()
                    .field(TestTableRow::FIELD_ID)
                    .unwrap()
                    .unwrap();
                result.push(id);
            }
            result
        }
    }

    impl Drop for PretendShardedTable {
        fn drop(&mut self) {
            self.space.drop().unwrap();
        }
    }

    fn infos(tables: &[&PretendShardedTable]) -> Vec<ShardedTableInfo> {
        let mut infos: Vec<_> = tables.iter().map(|t| t.info.clone()).collect();
        infos.sort_by_key(|info| info.id);
        infos
    }

    /// Contents of the table in `range` as `(table_id, tuple)`, in the
    /// order of the `bucket_id` index.
    fn data_in_range(
        info: &ShardedTableInfo,
        range: &BucketIdRange,
    ) -> Vec<(SpaceId, TupleBuffer)> {
        let index = index_by_ids_unchecked(info.id, info.sharding_index_id);
        let iter = index
            .select_v2(IteratorType::GE, &[*range.start()])
            .unwrap();

        let mut result = vec![];
        for tuple in iter {
            let tuple = tuple.unwrap();
            let bucket_id = tuple
                .field::<u64>(info.bucket_id_column_index)
                .unwrap()
                .unwrap();
            if bucket_id > *range.end() {
                break;
            }

            result.push((info.id, TupleBuffer::from(tuple)));
        }

        result
    }

    fn array_len(tuple: &TupleBuffer) -> u64 {
        let iter = ValueIter::from_array(tuple.as_ref()).unwrap();
        iter.len().unwrap() as _
    }

    /// Flattens the chunk into `(table_id, tuple)` pairs. The order of tables
    /// in the chunk is arbitrary, so they're ordered by id here, the tuples of
    /// each table stay in the order of the `bucket_id` index.
    fn tuples(table_data: &[TableData]) -> Vec<(SpaceId, TupleBuffer)> {
        let mut result = vec![];
        for data in table_data {
            for tuple in ValueIter::from_array(data.tuples.as_ref()).unwrap() {
                result.push((
                    data.table_id,
                    TupleBuffer::try_from_vec(tuple.into()).unwrap(),
                ));
            }
        }
        result
    }

    fn decode_tuples<'a, T>(table_data: &'a [TableData]) -> Vec<(SpaceId, T)>
    where
        T: tarantool::tuple::Decode<'a>,
    {
        let mut result = vec![];
        for data in table_data {
            let mut iter = ValueIter::from_array(data.tuples.as_ref()).unwrap();
            while let Some(v) = iter.decode_next() {
                result.push((data.table_id, v.unwrap()));
            }
        }
        result
    }

    fn decode_field<'a, T, S>(tuple: &'a S, field: u32) -> T
    where
        T: tarantool::tuple::Decode<'a>,
        S: AsRef<[u8]> + ?Sized,
    {
        let mut iter = ValueIter::from_array(tuple.as_ref()).unwrap();
        // Skip all fields before the one we're looking for
        for _ in 0..field {
            _ = iter.next().unwrap();
        }
        iter.decode_next().unwrap().unwrap()
    }

    /// Contents of all the tables in the environment. Returns an array of pairs
    /// `(table_id, tuple)`.
    fn all_table_data(env: &PretendEnvironment) -> Vec<(SpaceId, TupleBuffer)> {
        let mut result = vec![];
        for table in &env.tables {
            let space = space_by_id_unchecked(table.id);
            let iter = space.select_v2(IteratorType::All, &()).unwrap();
            for tuple in iter {
                result.push((table.id, TupleBuffer::from(tuple.unwrap())));
            }
        }
        result
    }

    const TIER: &str = "storage";
    const INSTANCE: &str = "storage_1_1";

    /// A single instance of a pretend cluster, which owns all the buckets.
    struct PretendEnvironment {
        _cluster: Rc<PretendCluster>,
        instance: Rc<PretendInstance>,
        tables: Vec<ShardedTableInfo>,
        left_to_send: u64,
    }

    impl PretendEnvironment {
        fn new(mut tables: Vec<ShardedTableInfo>) -> Self {
            let replicaset = ("storage_1", (1, INSTANCE), &[][..], 1..=100);
            let cluster = setup_by_spec(0, &[(TIER, 100, &[replicaset])]);
            let instance = cluster.instance(INSTANCE);
            // Normalize table info so the order is always deterministic.
            // Tests rely on that when checking invariants.
            tables.sort_by_key(|info| info.id);
            Self {
                _cluster: cluster,
                instance,
                tables,
                left_to_send: NO_LIMIT,
            }
        }

        fn set_bucket_rw_refs(&self, bucket_id: u64, rw: u32) {
            let mut bucket_refs = self.instance.bucket_refs.borrow_mut();
            bucket_refs.entry(bucket_id).or_default().rw = rw;
        }

        /// The iterator over `table_id` in `bucket_id` yields an error after
        /// `tuples_before_error` tuples.
        fn set_iterator_error(
            &self,
            table_id: SpaceId,
            bucket_id: u64,
            tuples_before_error: usize,
        ) {
            let mut errors = self.instance.index_select_error_after.borrow_mut();
            errors.insert((table_id, bucket_id), tuples_before_error);
        }

        fn clear_iterator_errors(&self) {
            self.instance.index_select_error_after.borrow_mut().clear();
        }
    }

    fn do_walk_sharded_data_unchecked(
        env: &PretendEnvironment,
        range: BucketIdRange,
        mode: WalkShardedDataMode,
        offset: Option<&WalkShardedDataOffset>,
        chunk_size_max: u64,
        bucket_count_max: u64,
    ) -> Result<WalkShardedDataResult> {
        let context = ReshardingPlanContext {
            sharded_tables: &env.tables,
            resharding_chunk_size_max: chunk_size_max,
            resharding_bucket_count_max: bucket_count_max,
        };

        let mut res = walk_sharded_data_in_range(
            &*env.instance,
            range,
            mode,
            offset,
            env.left_to_send,
            &context,
        )?;

        // `walk_sharded_data_in_range` returns data in arbitrary order due to
        // the use of `HashMap`. We sort that data for determinism in tests.
        res.table_data.sort_by_key(|data| data.table_id);

        Ok(res)
    }

    fn do_walk_sharded_data(
        env: &PretendEnvironment,
        range: BucketIdRange,
        mode: WalkShardedDataMode,
        saved_offset: Option<&WalkShardedDataOffset>,
        chunk_size_max: u64,
        bucket_count_max: u64,
    ) -> WalkShardedDataResult {
        try_do_walk_sharded_data(
            env,
            range,
            mode,
            saved_offset,
            chunk_size_max,
            bucket_count_max,
        )
        .unwrap()
    }

    /// Same as [`do_walk_sharded_data`], but returns the error instead of
    /// panicking. The invariants are checked in either case.
    fn try_do_walk_sharded_data(
        env: &PretendEnvironment,
        range: BucketIdRange,
        mode: WalkShardedDataMode,
        saved_offset: Option<&WalkShardedDataOffset>,
        chunk_size_max: u64,
        bucket_count_max: u64,
    ) -> Result<WalkShardedDataResult> {
        let all_data_before = all_table_data(env);

        let res = do_walk_sharded_data_unchecked(
            env,
            range.clone(),
            mode,
            saved_offset,
            chunk_size_max,
            bucket_count_max,
        );

        // The walk doesn't modify anything, so walking again with the same
        // arguments gives the same result.
        assert_eq!(all_table_data(env), all_data_before);
        let again = do_walk_sharded_data_unchecked(
            env,
            range.clone(),
            mode,
            saved_offset,
            chunk_size_max,
            bucket_count_max,
        );
        let res = match (res, again) {
            (Ok(res), Ok(again)) => {
                assert_eq!(again, res);
                res
            }
            (Err(e), Err(again)) => {
                assert_eq!(again.to_string(), e.to_string());
                return Err(e);
            }
            (res, again) => panic!("walk is not idempotent: {res:?} vs {again:?}"),
        };

        let tuples_count = res.tuples_count;
        let bucket_id_first = res.bucket_id_first;
        let bucket_id_last_touched = res.bucket_id_last_touched;
        let bucket_id_last_complete = res.bucket_id_last_complete;

        // Invariants which hold for any walk
        let start = *range.start();
        let start = saved_offset.map_or(start, |s| s.bucket_id);
        if mode == DetermineRangeToSend {
            // Leading buckets with rw refs are skipped
            assert!(bucket_id_first >= start);
        } else {
            assert_eq!(bucket_id_first, start);
        }

        assert!(range.contains(&bucket_id_last_touched));
        assert!(bucket_id_first <= bucket_id_last_touched);
        if let Some(last_complete) = bucket_id_last_complete {
            assert!(range.contains(&last_complete));
            assert!(bucket_id_first <= last_complete);
            assert!(
                last_complete == bucket_id_last_touched
                    || last_complete + 1 == bucket_id_last_touched,
                "last complete: {last_complete}, last touched: {bucket_id_last_touched}"
            );
        }

        if let Some(offset) = &res.next_offset {
            assert!(range.contains(&offset.bucket_id));
            assert!(env.tables.iter().any(|t| t.id == offset.table_id));
        }

        if mode == DetermineRangeToSend {
            assert!(res.table_data.is_empty());
        } else {
            let tuples_count_actual: u64 = res
                .table_data
                .iter()
                .map(|data| array_len(&data.tuples))
                .sum();
            assert_eq!(tuples_count, tuples_count_actual);
        }

        Ok(res)
    }

    const NO_LIMIT: u64 = u64::MAX;

    #[tarantool::test]
    fn walk_range_without_data() {
        // No tables at all
        let env = PretendEnvironment::new(vec![]);
        let range = 1..=10;
        let res = do_walk_sharded_data(
            &env,
            range.clone(),
            GetDataChunkToSend,
            None,
            NO_LIMIT,
            NO_LIMIT,
        );
        assert_eq!(res.complete_range(), Some(range));
        assert!(res.table_data.is_empty());
        assert!(res.next_offset.is_none());

        // Tables with data outside of the range only
        let t1 = PretendShardedTable::new("t1");
        let t2 = PretendShardedTable::new("t2");
        t1.insert(1, 5, 10);
        t2.insert(1, 21, 10);
        let env = PretendEnvironment::new(infos(&[&t1, &t2]));

        let range = 10..=20;
        for mode in [
            DetermineRangeToSend,
            GetDataChunkToSend,
            GetKeysChunkToDelete,
        ] {
            let res = do_walk_sharded_data(&env, range.clone(), mode, None, NO_LIMIT, NO_LIMIT);
            assert_eq!(res.touched_range(), range);
            assert_eq!(res.complete_range().as_ref(), Some(&range));
            assert_eq!(res.tuples_count, 0);
            assert!(res.table_data.is_empty());
            assert!(res.next_offset.is_none());
        }
    }

    #[tarantool::test]
    fn walk_all_data_fits_in_one_chunk() {
        let t1 = PretendShardedTable::new("t1");
        let t2 = PretendShardedTable::new("t2");
        // Outside of the range
        t1.insert(100, 1, 10);
        t2.insert(100, 11, 10);
        // Inside of the range
        t1.insert(1, 3, 10);
        t1.insert(2, 3, 10);
        t1.insert(3, 7, 10);
        t2.insert(1, 5, 10);
        t2.insert(2, 7, 10);
        let env = PretendEnvironment::new(infos(&[&t1, &t2]));

        let range = 2..=10;
        let res = do_walk_sharded_data(
            &env,
            range.clone(),
            GetDataChunkToSend,
            None,
            NO_LIMIT,
            NO_LIMIT,
        );
        assert_eq!(res.complete_range(), Some(range.clone()));
        assert_eq!(res.touched_range(), range);
        assert!(res.next_offset.is_none());

        let expected: Vec<_> = env
            .tables
            .iter()
            .flat_map(|t| data_in_range(t, &range))
            .collect();
        let actual = tuples(&res.table_data);
        assert_eq!(actual, expected);
        assert_eq!(res.tuples_count, 5);

        // Only one table has data
        let sub_range = 2..=4;
        let res = do_walk_sharded_data(
            &env,
            sub_range.clone(),
            GetDataChunkToSend,
            None,
            NO_LIMIT,
            NO_LIMIT,
        );
        assert_eq!(res.complete_range(), Some(sub_range.clone().clone()));
        assert_eq!(tuples(&res.table_data), data_in_range(&t1.info, &sub_range));
        assert!(res.next_offset.is_none());

        // Primary keys instead of whole tuples
        let res = do_walk_sharded_data(
            &env,
            range.clone(),
            GetKeysChunkToDelete,
            None,
            NO_LIMIT,
            NO_LIMIT,
        );
        assert_eq!(res.complete_range(), Some(range.clone()));
        assert!(res.next_offset.is_none());
        let keys = decode_tuples::<(u64,)>(&res.table_data);
        assert_eq!(
            keys,
            &[
                (t1.info.id, (1,)),
                (t1.info.id, (2,)),
                (t1.info.id, (3,)),
                (t2.info.id, (1,)),
                (t2.info.id, (2,)),
            ]
        );
    }

    #[tarantool::test]
    fn walk_bucket_count_max() {
        let t1 = PretendShardedTable::new("t1");
        let t2 = PretendShardedTable::new("t2");
        t1.insert(1, 2, 10);
        t2.insert(1, 2, 10);
        t1.insert(2, 4, 10);
        t2.insert(2, 6, 10);
        t1.insert(3, 8, 10);
        let env = PretendEnvironment::new(infos(&[&t1, &t2]));

        // Only buckets with data count
        let res = do_walk_sharded_data(&env, 1..=10, GetDataChunkToSend, None, NO_LIMIT, 2);
        assert_eq!(res.complete_range(), Some(1..=4));
        assert_eq!(res.touched_range(), 1..=4);
        assert_eq!(res.tuples_count, 3);
        assert!(res.next_offset.is_none());

        let res = do_walk_sharded_data(&env, 5..=10, GetDataChunkToSend, None, NO_LIMIT, 2);
        assert_eq!(res.complete_range(), Some(5..=8));
        assert_eq!(res.tuples_count, 2);
        assert!(res.next_offset.is_none());

        let res = do_walk_sharded_data(&env, 9..=10, GetDataChunkToSend, None, NO_LIMIT, 2);
        assert_eq!(res.complete_range(), Some(9..=10));
        assert_eq!(res.tuples_count, 0);
        assert!(res.next_offset.is_none());

        let res = do_walk_sharded_data(&env, 1..=10, DetermineRangeToSend, None, NO_LIMIT, 2);
        assert_eq!(res.complete_range(), Some(1..=4));
        assert!(res.next_offset.is_none());
    }

    #[tarantool::test]
    fn walk_chunk_size_max_within_one_bucket() {
        let t1 = PretendShardedTable::new("t1");
        let size = t1.insert(1, 5, 100).data().len() as u64;
        t1.insert(2, 5, 100);
        t1.insert(3, 5, 100);
        t1.insert(4, 6, 100);
        let env = PretendEnvironment::new(infos(&[&t1]));

        // Two tuples fit in a chunk
        let chunk_size_max = 2 * size;

        let res = do_walk_sharded_data(
            &env,
            1..=10,
            GetDataChunkToSend,
            None,
            chunk_size_max,
            NO_LIMIT,
        );
        // Bucket 5 is not complete yet, everything before it is
        assert_eq!(res.complete_range(), Some(1..=4));
        assert_eq!(res.touched_range(), 1..=5);
        assert_eq!(res.tuples_count, 2);
        let offset = res.next_offset.unwrap();
        assert_eq!(offset.bucket_id, 5);
        assert_eq!(offset.table_id, t1.info.id);

        // The sender marks 1..=4 as sent, and continues with the rest of
        // bucket 5 and then bucket 6
        let res = do_walk_sharded_data(
            &env,
            5..=10,
            GetDataChunkToSend,
            Some(&offset),
            chunk_size_max,
            NO_LIMIT,
        );
        assert_eq!(res.complete_range(), Some(5..=10));
        assert_eq!(res.touched_range(), 5..=10);
        let mut expected = data_in_range(&t1.info, &(5..=6));
        expected.drain(..2);
        assert_eq!(tuples(&res.table_data), expected);
        assert!(res.next_offset.is_none());

        // When only determining the range, the partially covered bucket is included
        let res = do_walk_sharded_data(
            &env,
            1..=10,
            DetermineRangeToSend,
            None,
            chunk_size_max,
            NO_LIMIT,
        );
        assert_eq!(res.complete_range(), Some(1..=4));
        assert_eq!(res.touched_range(), 1..=5);
        // The iterator isn't needed in this mode, but it's still correct
        let offset = res.next_offset.unwrap();
        assert_eq!(offset.bucket_id, 5);
        assert_eq!(offset.table_id, t1.info.id);

        // A tuple larger than the limit is still sent, one per chunk
        let res = do_walk_sharded_data(&env, 1..=10, GetDataChunkToSend, None, 1, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(1..=4));
        assert_eq!(res.touched_range(), 1..=5);
        assert_eq!(res.tuples_count, 1);
        assert_eq!(res.next_offset.unwrap().bucket_id, 5);
    }

    #[tarantool::test]
    fn walk_chunk_size_max_at_bucket_boundary() {
        let t1 = PretendShardedTable::new("t1");
        let size = t1.insert(1, 5, 100).data().len() as u64;
        t1.insert(2, 6, 100);
        let env = PretendEnvironment::new(infos(&[&t1]));

        // The chunk ends right before the first tuple of bucket 6, so the next
        // walk must start from the beginning of bucket 6 in the same table
        let res = do_walk_sharded_data(&env, 1..=10, GetDataChunkToSend, None, size, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(1..=5));
        assert_eq!(tuples(&res.table_data), data_in_range(&t1.info, &(5..=5)));
        let offset = res.next_offset.unwrap();
        assert_eq!(offset.bucket_id, 6);
        assert_eq!(offset.table_id, t1.info.id);

        let res = do_walk_sharded_data(
            &env,
            6..=10,
            GetDataChunkToSend,
            Some(&offset),
            size,
            NO_LIMIT,
        );
        assert_eq!(res.complete_range(), Some(6..=10));
        assert_eq!(tuples(&res.table_data), data_in_range(&t1.info, &(6..=6)));
        assert!(res.next_offset.is_none());

        // Bucket 6 contributed nothing to the chunk, so it isn't locked
        let res = do_walk_sharded_data(&env, 1..=10, DetermineRangeToSend, None, size, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(1..=5));
    }

    #[tarantool::test]
    fn walk_chunk_size_max_at_table_boundary() {
        let t1 = PretendShardedTable::new("t1");
        let t2 = PretendShardedTable::new("t2");
        let size = t1.insert(1, 5, 100).data().len() as u64;
        t2.insert(1, 5, 100);
        t2.insert(2, 5, 100);
        let env = PretendEnvironment::new(infos(&[&t1, &t2]));
        let (first, second) = if t1.info.id < t2.info.id {
            (&t1, &t2)
        } else {
            (&t2, &t1)
        };

        // Each chunk fits one tuple, so the first one ends right before the
        // first tuple of the second table
        let res = do_walk_sharded_data(&env, 1..=10, GetDataChunkToSend, None, size, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(1..=4));
        assert_eq!(
            tuples(&res.table_data),
            vec![data_in_range(&first.info, &(5..=5))[0].clone()]
        );
        let offset = res.next_offset.unwrap();
        assert_eq!(offset.bucket_id, 5);
        assert_eq!(offset.table_id, second.info.id);

        // Collect the rest of bucket 5
        let mut collected = tuples(&res.table_data);
        let mut offset = Some(offset);
        loop {
            let res = do_walk_sharded_data(
                &env,
                5..=10,
                GetDataChunkToSend,
                offset.as_ref(),
                size,
                NO_LIMIT,
            );
            collected.extend(tuples(&res.table_data));
            offset = res.next_offset.clone();
            if offset.is_none() {
                assert_eq!(res.complete_range(), Some(5..=10));
                break;
            }
            assert_eq!(res.complete_range(), None);
        }

        let mut expected = data_in_range(&first.info, &(5..=5));
        expected.extend(data_in_range(&second.info, &(5..=5)));
        assert_eq!(collected, expected);
    }

    /// Every [`do_walk_sharded_data`] checks that walking again gives the same
    /// result, this test spells out the scenarios where the sender actually
    /// does that.
    #[tarantool::test]
    fn walk_is_idempotent() {
        let t1 = PretendShardedTable::new("t1");
        let size = t1.insert(1, 5, 100).data().len() as u64;
        t1.insert(2, 5, 100);
        t1.insert(3, 5, 100);
        let env = PretendEnvironment::new(infos(&[&t1]));
        let data = data_in_range(&t1.info, &(5..=5));

        // The first chunk fails to be sent, so the sender walks again from
        // the same (empty) iterator and gets the same chunk
        let first = do_walk_sharded_data(&env, 1..=10, GetDataChunkToSend, None, size, NO_LIMIT);
        let retry = do_walk_sharded_data(&env, 1..=10, GetDataChunkToSend, None, size, NO_LIMIT);
        assert_eq!(&retry, &first);
        assert_eq!(tuples(&first.table_data), data[..1]);

        // Same for a chunk which starts from a saved offset
        let offset = first.next_offset.unwrap();
        let offset_before = Some(offset.clone());
        let second = do_walk_sharded_data(
            &env,
            5..=10,
            GetDataChunkToSend,
            Some(&offset),
            size,
            NO_LIMIT,
        );
        let retry = do_walk_sharded_data(
            &env,
            5..=10,
            GetDataChunkToSend,
            Some(&offset),
            size,
            NO_LIMIT,
        );
        assert_eq!(retry, second);
        assert_eq!(tuples(&second.table_data), data[1..2]);
        assert_eq!(Some(&offset), offset_before.as_ref());

        // The keys fail to be deleted, so the sender gets the same keys again
        let first =
            do_walk_sharded_data(&env, 1..=10, GetKeysChunkToDelete, None, NO_LIMIT, NO_LIMIT);
        let retry =
            do_walk_sharded_data(&env, 1..=10, GetKeysChunkToDelete, None, NO_LIMIT, NO_LIMIT);
        assert_eq!(retry, first);
        assert_eq!(
            decode_tuples::<(u64,)>(&first.table_data),
            [(t1.info.id, (1,)), (t1.info.id, (2,)), (t1.info.id, (3,))]
        );
        assert!(first.next_offset.is_none());
        assert_eq!(t1.ids(), [1, 2, 3]);
    }

    #[tarantool::test]
    fn walk_ignores_offset_after_schema_change() {
        let t1 = PretendShardedTable::new("t1");
        let size = t1.insert(1, 5, 100).data().len() as u64;
        t1.insert(2, 5, 100);
        t1.insert(3, 5, 100);
        let env = PretendEnvironment::new(infos(&[&t1]));
        let data = data_in_range(&t1.info, &(5..=5));

        let first = do_walk_sharded_data(&env, 1..=10, GetDataChunkToSend, None, size, NO_LIMIT);
        assert_eq!(tuples(&first.table_data), data[..1]);
        let offset = first.next_offset.unwrap();
        assert_eq!(offset.bucket_id, 5);

        let schema_version = env.instance.box_schema_version.get();
        assert_eq!(offset.schema_version, schema_version);

        env.instance.box_schema_version.set(schema_version + 1);

        let with_stale_offset = do_walk_sharded_data(
            &env,
            5..=10,
            GetDataChunkToSend,
            Some(&offset),
            size,
            NO_LIMIT,
        );
        let without_offset =
            do_walk_sharded_data(&env, 5..=10, GetDataChunkToSend, None, size, NO_LIMIT);
        assert_eq!(with_stale_offset, without_offset);
        // The first tuple is collected again
        assert_eq!(tuples(&with_stale_offset.table_data), data[..1]);
        let next_offset = with_stale_offset.next_offset.unwrap();
        assert_eq!(next_offset.schema_version, schema_version + 1);
    }

    #[tarantool::test]
    fn walk_and_delete_chunk_by_chunk() {
        let t1 = PretendShardedTable::new("t1");
        let t2 = PretendShardedTable::new("t2");
        let size = t1.insert(1, 1, 50).data().len() as u64;
        for id in 2..=20 {
            t1.insert(id, 2 + id % 3, 50);
            t2.insert(id, 2 + id % 4, 50);
        }
        t2.insert(100, 11, 50);
        let env = PretendEnvironment::new(infos(&[&t1, &t2]));

        // This is what the sender does with "garbage" buckets: delete
        // the keys and walk again from scratch
        let range = 2..=10;
        loop {
            let res = do_walk_sharded_data(
                &env,
                range.clone(),
                GetKeysChunkToDelete,
                None,
                3 * size,
                NO_LIMIT,
            );
            if res.table_data.is_empty() {
                assert_eq!(res.complete_range(), Some(range.clone()));
                assert!(res.next_offset.is_none());
                break;
            }
            let keys = decode_tuples::<(u64,)>(&res.table_data);
            for (table_id, id) in keys {
                let table = [&t1, &t2]
                    .into_iter()
                    .find(|t| t.info.id == table_id)
                    .unwrap();
                table.space.delete(&id).unwrap().unwrap();
            }
        }

        assert_eq!(t1.ids(), [1]);
        assert_eq!(t2.ids(), [100]);
    }

    #[tarantool::test]
    fn walk_skips_buckets_with_rw_refs() {
        let t1 = PretendShardedTable::new("t1");
        for bucket_id in 1..=10 {
            t1.insert(bucket_id, bucket_id, 10);
        }
        let env = PretendEnvironment::new(infos(&[&t1]));

        // Leading buckets with rw refs are skipped, and the range ends before
        // the next one
        env.set_bucket_rw_refs(1, 1);
        env.set_bucket_rw_refs(2, 3);
        env.set_bucket_rw_refs(6, 1);
        let res =
            do_walk_sharded_data(&env, 1..=10, DetermineRangeToSend, None, NO_LIMIT, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(3..=5));
        assert!(res.next_offset.is_none());

        // A bucket ref without rw refs doesn't get in the way
        env.set_bucket_rw_refs(2, 0);
        let res =
            do_walk_sharded_data(&env, 1..=10, DetermineRangeToSend, None, NO_LIMIT, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(2..=5));
        assert!(res.next_offset.is_none());

        // The range ends before a bucket with rw refs even if it has no data
        // yet, so that it never contains such a bucket
        let empty = PretendEnvironment::new(vec![]);
        empty.set_bucket_rw_refs(4, 1);
        let res = do_walk_sharded_data(
            &empty,
            1..=10,
            DetermineRangeToSend,
            None,
            NO_LIMIT,
            NO_LIMIT,
        );
        assert_eq!(res.complete_range(), Some(1..=3));
        assert!(res.next_offset.is_none());

        // Buckets are only skipped when determining the range, after that
        // they're locked for sending
        for mode in [GetDataChunkToSend, GetKeysChunkToDelete] {
            let res = do_walk_sharded_data(&env, 1..=10, mode, None, NO_LIMIT, NO_LIMIT);
            assert_eq!(res.complete_range(), Some(1..=10));
            assert_eq!(res.tuples_count, 10);
            assert!(res.next_offset.is_none());
        }

        // All buckets have rw refs
        let res = do_walk_sharded_data_unchecked(
            &env,
            6..=6,
            DetermineRangeToSend,
            None,
            NO_LIMIT,
            NO_LIMIT,
        );
        assert!(res.is_err());
        env.set_bucket_rw_refs(7, 1);
        let res = do_walk_sharded_data_unchecked(
            &env,
            6..=7,
            DetermineRangeToSend,
            None,
            NO_LIMIT,
            NO_LIMIT,
        );
        assert!(res.is_err());
    }

    #[tarantool::test]
    fn walk_left_to_send() {
        let t1 = PretendShardedTable::new("t1");
        t1.insert(1, 2, 10);
        t1.insert(2, 7, 10);
        let mut env = PretendEnvironment::new(infos(&[&t1]));
        env.left_to_send = 4;

        let res =
            do_walk_sharded_data(&env, 1..=10, DetermineRangeToSend, None, NO_LIMIT, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(1..=4));
        assert!(res.next_offset.is_none());

        let res = do_walk_sharded_data(&env, 1..=10, GetDataChunkToSend, None, NO_LIMIT, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(1..=4));
        assert_eq!(tuples(&res.table_data), data_in_range(&t1.info, &(1..=4)));
        assert!(res.next_offset.is_none());

        // Skipped buckets don't count
        env.set_bucket_rw_refs(1, 1);
        env.set_bucket_rw_refs(2, 1);
        let res =
            do_walk_sharded_data(&env, 1..=10, DetermineRangeToSend, None, NO_LIMIT, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(3..=6));
        assert!(res.next_offset.is_none());

        // Doesn't matter when deleting
        let res =
            do_walk_sharded_data(&env, 1..=10, GetKeysChunkToDelete, None, NO_LIMIT, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(1..=10));
        assert_eq!(res.tuples_count, 2);
        assert!(res.next_offset.is_none());
    }

    #[tarantool::test]
    fn walk_iterator_error_before_any_data() {
        let t1 = PretendShardedTable::new("t1");
        t1.insert(1, 5, 100);
        t1.insert(2, 5, 100);
        t1.insert(3, 6, 100);
        let env = PretendEnvironment::new(infos(&[&t1]));
        env.set_iterator_error(t1.info.id, 5, 0);

        // Nothing was added to the chunk before the error, so there's nothing
        // to handle and the error is returned. This holds even if some empty
        // buckets were walked completely before the error (1..=4).
        for range in [1..=10, 5..=10] {
            for mode in [
                DetermineRangeToSend,
                GetDataChunkToSend,
                GetKeysChunkToDelete,
            ] {
                let res = do_walk_sharded_data_unchecked(
                    &env,
                    range.clone(),
                    mode,
                    None,
                    NO_LIMIT,
                    NO_LIMIT,
                );
                let Err(e) = res else {
                    panic!("{mode:?} {range:?}: expected an error, got {res:?}");
                };
                assert!(
                    e.to_string().contains("injected iterator error"),
                    "{mode:?} {range:?}: {e}"
                );
            }
        }

        // After the error goes away everything is collected
        env.clear_iterator_errors();
        let res = do_walk_sharded_data(&env, 5..=10, GetDataChunkToSend, None, NO_LIMIT, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(5..=10));
        assert_eq!(tuples(&res.table_data), data_in_range(&t1.info, &(5..=6)));
    }

    #[tarantool::test]
    fn walk_iterator_error_after_some_data() {
        let t1 = PretendShardedTable::new("t1");
        t1.insert(1, 5, 100);
        t1.insert(2, 5, 100);
        t1.insert(3, 5, 100);
        t1.insert(4, 6, 100);
        let env = PretendEnvironment::new(infos(&[&t1]));
        let data = data_in_range(&t1.info, &(5..=6));
        env.set_iterator_error(t1.info.id, 5, 1);

        let res = do_walk_sharded_data(&env, 1..=10, GetDataChunkToSend, None, NO_LIMIT, NO_LIMIT);
        // Bucket 5 is not complete yet, everything before it is
        assert_eq!(res.complete_range(), Some(1..=4));
        assert_eq!(res.touched_range(), 1..=5);
        assert_eq!(tuples(&res.table_data), data[..1]);
        let offset = res.next_offset.unwrap();
        assert_eq!(offset.bucket_id, 5);
        assert_eq!(offset.table_id, t1.info.id);

        let res =
            do_walk_sharded_data(&env, 1..=10, GetKeysChunkToDelete, None, NO_LIMIT, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(1..=4));
        assert_eq!(
            decode_tuples::<(u64,)>(&res.table_data),
            [(t1.info.id, (1,))]
        );
        assert_eq!(res.next_offset.unwrap().bucket_id, 5);

        let res =
            do_walk_sharded_data(&env, 1..=10, DetermineRangeToSend, None, NO_LIMIT, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(1..=4));
        assert_eq!(res.next_offset.unwrap().bucket_id, 5);

        // The next walk continues right after the last collected tuple
        env.clear_iterator_errors();
        let res = do_walk_sharded_data(
            &env,
            5..=10,
            GetDataChunkToSend,
            Some(&offset),
            NO_LIMIT,
            NO_LIMIT,
        );
        assert_eq!(res.complete_range(), Some(5..=10));
        assert_eq!(tuples(&res.table_data), data[1..]);
        assert!(res.next_offset.is_none());
    }

    #[tarantool::test]
    fn walk_iterator_error_after_data_in_previous_bucket() {
        let t1 = PretendShardedTable::new("t1");
        t1.insert(1, 3, 100);
        t1.insert(2, 5, 100);
        let env = PretendEnvironment::new(infos(&[&t1]));
        env.set_iterator_error(t1.info.id, 5, 0);

        let res = do_walk_sharded_data(&env, 1..=10, GetDataChunkToSend, None, NO_LIMIT, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(1..=4));
        assert_eq!(tuples(&res.table_data), data_in_range(&t1.info, &(3..=3)));
        let offset = res.next_offset.unwrap();
        assert_eq!(offset.bucket_id, 5);
        assert_eq!(offset.position, TuplePosition::default());

        let res =
            do_walk_sharded_data(&env, 1..=10, GetKeysChunkToDelete, None, NO_LIMIT, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(1..=4));
        assert_eq!(
            decode_tuples::<(u64,)>(&res.table_data),
            [(t1.info.id, (1,))]
        );

        let res =
            do_walk_sharded_data(&env, 1..=10, DetermineRangeToSend, None, NO_LIMIT, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(1..=4));
    }

    #[tarantool::test]
    fn walk_iterator_error_after_data_in_previous_table() {
        let t1 = PretendShardedTable::new("t1");
        let t2 = PretendShardedTable::new("t2");
        t1.insert(1, 5, 100);
        t2.insert(1, 5, 100);
        let env = PretendEnvironment::new(infos(&[&t1, &t2]));
        let (first, second) = if t1.info.id < t2.info.id {
            (&t1, &t2)
        } else {
            (&t2, &t1)
        };
        env.set_iterator_error(second.info.id, 5, 0);

        // Only the first table's data is in the chunk, the next walk starts
        // from the beginning of the second table in the same bucket
        for mode in [GetDataChunkToSend, GetKeysChunkToDelete] {
            let res = do_walk_sharded_data(&env, 1..=10, mode, None, NO_LIMIT, NO_LIMIT);
            assert_eq!(res.complete_range(), Some(1..=4), "{mode:?}");
            assert_eq!(res.tuples_count, 1, "{mode:?}");
            assert_eq!(res.table_data.len(), 1, "{mode:?}");
            assert_eq!(res.table_data[0].table_id, first.info.id, "{mode:?}");
            let offset = res.next_offset.unwrap();
            assert_eq!(offset.bucket_id, 5, "{mode:?}");
            assert_eq!(offset.table_id, second.info.id, "{mode:?}");
            assert_eq!(offset.position, TuplePosition::default(), "{mode:?}");
        }

        let res =
            do_walk_sharded_data(&env, 1..=10, DetermineRangeToSend, None, NO_LIMIT, NO_LIMIT);
        assert_eq!(res.complete_range(), Some(1..=4));
    }

    #[tarantool::test]
    fn walk_iterator_error_right_after_saved_offset() {
        let t1 = PretendShardedTable::new("t1");
        let size = t1.insert(1, 5, 100).data().len() as u64;
        t1.insert(2, 5, 100);
        t1.insert(3, 5, 100);
        let env = PretendEnvironment::new(infos(&[&t1]));
        let data = data_in_range(&t1.info, &(5..=5));

        let first = do_walk_sharded_data(&env, 1..=10, GetDataChunkToSend, None, size, NO_LIMIT);
        assert_eq!(tuples(&first.table_data), data[..1]);
        let offset = first.next_offset.unwrap();

        // The error happens on the first tuple after the saved offset, so
        // nothing is collected and the error is returned. The caller keeps
        // the saved offset and retries with it.
        env.set_iterator_error(t1.info.id, 5, 0);
        let res = do_walk_sharded_data_unchecked(
            &env,
            5..=10,
            GetDataChunkToSend,
            Some(&offset),
            size,
            NO_LIMIT,
        );
        assert!(res.is_err());

        // The retry continues from the saved offset, nothing is sent twice
        env.clear_iterator_errors();
        let res = do_walk_sharded_data(
            &env,
            5..=10,
            GetDataChunkToSend,
            Some(&offset),
            size,
            NO_LIMIT,
        );
        assert_eq!(res.complete_range(), None);
        assert_eq!(tuples(&res.table_data), data[1..2]);

        // Same when some data was collected after the saved offset
        env.set_iterator_error(t1.info.id, 5, 1);
        let res = do_walk_sharded_data(
            &env,
            5..=10,
            GetDataChunkToSend,
            Some(&offset),
            NO_LIMIT,
            NO_LIMIT,
        );
        assert_eq!(res.complete_range(), None);
        assert_eq!(tuples(&res.table_data), data[1..2]);
        let offset = res.next_offset.unwrap();

        env.clear_iterator_errors();
        let res = do_walk_sharded_data(
            &env,
            5..=10,
            GetDataChunkToSend,
            Some(&offset),
            NO_LIMIT,
            NO_LIMIT,
        );
        assert_eq!(res.complete_range(), Some(5..=10));
        assert_eq!(tuples(&res.table_data), data[2..]);
    }

    /// Simulates what the sender does: walk chunk by chunk, after each chunk
    /// shrink the range by the complete part and continue from the saved
    /// offset. Then delete everything chunk by chunk.
    ///
    /// Iterator errors are injected at random. When a walk returns an error the
    /// sender retries with the same range and offset, the end result must be
    /// the same as without errors.
    fn do_walk_random_data(seed: u64) {
        const BUCKET_COUNT: u64 = 3000;

        let mut rng = StdRng::seed_from_u64(seed);

        let table_count = rng.random_range(1..=3);
        let tables: Vec<_> = (0..table_count)
            .map(|i| PretendShardedTable::new(&format!("t{i}")))
            .collect();
        let table_refs: Vec<_> = tables.iter().collect();
        let env = PretendEnvironment::new(infos(&table_refs));

        let mut max_tuple_size = 0;
        for table in &tables {
            for id in 0..rng.random_range(0..60) {
                // Some buckets get lots of data, some none at all
                let bucket_id = if rng.random_bool(0.3) {
                    rng.random_range(1..=3)
                } else {
                    rng.random_range(1..=BUCKET_COUNT)
                };
                let tuple = table.insert(id, bucket_id, rng.random_range(0..200));
                max_tuple_size = max_tuple_size.max(tuple.data().len() as u64);
            }
        }

        let start = rng.random_range(1..=BUCKET_COUNT);
        let end = rng.random_range(start..=BUCKET_COUNT);
        let full_range = start..=end;
        let chunk_size_max = rng.random_range(1..=4 * max_tuple_size.max(1));
        let bucket_count_max = rng.random_range(1..=5);
        let ctx = format!("seed: {seed}, range: {full_range:?}, chunk_size_max: {chunk_size_max}, bucket_count_max: {bucket_count_max}");

        // Pairs `(bucket_id, table_id)` which have data in the range, ordered
        // by bucket id. Only injecting errors here, otherwise they're never hit.
        let mut buckets_with_data: Vec<(u64, SpaceId)> = tables
            .iter()
            .flat_map(|t| data_in_range(&t.info, &full_range))
            .map(|(table_id, t)| (decode_field(&t, TestTableRow::FIELD_BUCKET_ID), table_id))
            .collect();
        buckets_with_data.sort();
        buckets_with_data.dedup();

        // Before each walk, with some probability, injects an iterator error
        // in one of the first buckets with data in `range`, i.e. the ones
        // likely to be reached by the walk.
        let inject_iterator_error = |rng: &mut StdRng, range: &BucketIdRange| {
            env.clear_iterator_errors();
            if !rng.random_bool(0.5) {
                return;
            }
            let candidates: Vec<_> = buckets_with_data
                .iter()
                .filter(|(bucket_id, _)| range.contains(bucket_id))
                .take(3)
                .collect();
            if candidates.is_empty() {
                return;
            }
            let &(bucket_id, table_id) = candidates[rng.random_range(0..candidates.len())];
            env.set_iterator_error(table_id, bucket_id, rng.random_range(0..=2));
        };
        let assert_injected_error = |e: &Error| {
            assert!(
                e.to_string().contains("injected iterator error"),
                "{ctx}: {e}"
            );
        };
        let mut errors_returned = 0;

        //
        // Determine the range to send with random buckets having rw refs
        //
        let busy: Vec<u64> = (1..=BUCKET_COUNT)
            .filter(|_| rng.random_bool(0.2))
            .collect();
        for &bucket_id in &busy {
            env.set_bucket_rw_refs(bucket_id, 1);
        }
        let first_free = full_range.clone().find(|b| !busy.contains(b));
        for step in 0.. {
            assert!(step < 10_000, "{ctx}: the walk isn't making progress");

            inject_iterator_error(&mut rng, &full_range);
            let res = try_do_walk_sharded_data(
                &env,
                full_range.clone(),
                DetermineRangeToSend,
                None,
                chunk_size_max,
                bucket_count_max,
            );
            let Some(first_free) = first_free else {
                // All buckets are busy, the iterator is never reached
                assert!(res.is_err(), "{ctx}, busy: {busy:?}");
                break;
            };
            let res = match res {
                Ok(res) => res,
                Err(e) => {
                    assert_injected_error(&e);
                    errors_returned += 1;
                    continue;
                }
            };

            // The offset is only there if the walk stopped because of the
            // chunk size or an iterator error, either in the middle of the
            // last touched bucket, or at the start of the next one
            if let Some(offset) = &res.next_offset {
                let last_touched = res.bucket_id_last_touched;
                assert!(
                    offset.bucket_id == last_touched || offset.bucket_id == last_touched + 1,
                    "{ctx}"
                );
            }
            let touched = res.touched_range();
            assert_eq!(*touched.start(), first_free, "{ctx}, busy: {busy:?}");
            assert!(
                touched.clone().all(|b| !busy.contains(&b)),
                "{ctx}, busy: {busy:?}"
            );
            break;
        }
        env.instance.bucket_refs.borrow_mut().clear();

        //
        // Send
        //
        let mut range = full_range.clone();
        let mut offset = None;
        let mut collected = vec![];
        for step in 0.. {
            assert!(step < 10_000, "{ctx}: the walk isn't making progress");

            let mode = GetDataChunkToSend;
            inject_iterator_error(&mut rng, &range);
            let res = try_do_walk_sharded_data(
                &env,
                range.clone(),
                mode,
                offset.as_ref(),
                chunk_size_max,
                bucket_count_max,
            );
            let res = match res {
                Ok(res) => res,
                Err(e) => {
                    // Retry with the same range and offset
                    assert_injected_error(&e);
                    errors_returned += 1;
                    continue;
                }
            };
            let chunk = tuples(&res.table_data);

            let chunk_size: u64 = chunk.iter().map(|(_, t)| t.len() as u64).sum();
            assert!(chunk_size <= chunk_size_max || chunk.len() == 1, "{ctx}");
            let mut buckets_with_data: Vec<u64> = chunk
                .iter()
                .map(|(_, t)| decode_field(&t, TestTableRow::FIELD_BUCKET_ID))
                .collect();
            buckets_with_data.sort();
            buckets_with_data.dedup();
            assert!(buckets_with_data.len() as u64 <= bucket_count_max, "{ctx}");

            // Chunks are grouped by table, but the whole stream is ordered
            // by bucket first
            let mut chunk = chunk;
            chunk.sort_by_key(|(table_id, t)| {
                let id: u64 = decode_field(&t, TestTableRow::FIELD_ID);
                let bucket_id: u64 = decode_field(&t, TestTableRow::FIELD_BUCKET_ID);
                (bucket_id, *table_id, id)
            });
            collected.extend(chunk);

            offset = res.next_offset.clone();
            let Some(complete) = res.complete_range() else {
                // Only a part of a single bucket was collected
                assert!(offset.is_some(), "{ctx}");
                continue;
            };
            assert_eq!(complete.start(), range.start(), "{ctx}");
            if complete.end() == range.end() {
                assert!(offset.is_none(), "{ctx}");
                break;
            }
            range = complete.end() + 1..=*range.end();
            if let Some(offset) = &offset {
                assert_eq!(offset.bucket_id, *range.start(), "{ctx}");
            }
        }

        let mut expected: Vec<_> = tables
            .iter()
            .flat_map(|t| data_in_range(&t.info, &full_range))
            .collect();
        expected.sort_by_key(|(table_id, t)| {
            let id: u64 = decode_field(&t, TestTableRow::FIELD_ID);
            let bucket_id: u64 = decode_field(&t, TestTableRow::FIELD_BUCKET_ID);
            (bucket_id, *table_id, id)
        });
        assert_eq!(collected, expected, "{ctx}");

        //
        // Delete
        //
        let outside: Vec<_> = tables
            .iter()
            .map(|t| {
                let all = data_in_range(&t.info, &(0..=u64::MAX));
                let inside = data_in_range(&t.info, &full_range);
                all.into_iter()
                    .filter(|x| !inside.contains(x))
                    .collect::<Vec<_>>()
            })
            .collect();
        for step in 0.. {
            assert!(step < 10_000, "{ctx}: the walk isn't making progress");

            let mode = GetKeysChunkToDelete;
            inject_iterator_error(&mut rng, &full_range);
            let res = try_do_walk_sharded_data(
                &env,
                full_range.clone(),
                mode,
                None,
                chunk_size_max,
                bucket_count_max,
            );
            let res = match res {
                Ok(res) => res,
                Err(e) => {
                    assert_injected_error(&e);
                    errors_returned += 1;
                    continue;
                }
            };
            // An empty chunk means there's no data left in the range, even if
            // an iterator error was injected
            if res.table_data.is_empty() {
                assert_eq!(res.complete_range(), Some(full_range.clone()), "{ctx}");
                assert!(res.next_offset.is_none(), "{ctx}");
                break;
            }
            let keys = decode_tuples::<(u64,)>(&res.table_data);
            for (table_id, id) in keys {
                let table = tables.iter().find(|t| t.info.id == table_id).unwrap();
                table.space.delete(&id).unwrap().unwrap();
            }
        }
        for (table, outside) in tables.iter().zip(outside) {
            assert_eq!(
                data_in_range(&table.info, &(0..=u64::MAX)),
                outside,
                "{ctx}"
            );
        }

        tlog!(Info, "{ctx}: iterator errors returned: {errors_returned}");
    }

    #[tarantool::test]
    fn walk_random_data_random() {
        let seed = test_rng_seed();
        do_walk_random_data(seed);
    }
}
