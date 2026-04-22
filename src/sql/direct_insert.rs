use crate::tlog;
use smol_str::format_smolstr;
use sql::errors::{Action, Entity, SbroadError};
use sql::executor::engine::helpers::{write_insert_args, TupleBuilderCommand, TupleBuilderPattern};
use sql::executor::engine::{QueryCache, Vshard};
use sql::executor::vtable::{VTableTuple, VirtualTable};
use sql::ir::helpers::RepeatableState;
use sql::ir::transformation::redistribution::{MotionKey, Target};
use sql_protocol::dml::insert::ConflictPolicy;
use std::any::Any;
use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};
use tarantool::error::{Error, TarantoolErrorCode};
use tarantool::space::Space;
use tarantool::transaction::TransactionError;
use tarantool::tuple::RawBytes;

#[derive(Debug)]
pub(crate) struct PreparedDirectInsert {
    table_id: u32,
    schema_version: u64,
    builder: TupleBuilderPattern,
    conflict_policy: ConflictPolicy,
    motion_key: Option<MotionKey>,
}

impl PreparedDirectInsert {
    pub(crate) fn new(
        table_id: u32,
        schema_version: u64,
        builder: TupleBuilderPattern,
        conflict_policy: ConflictPolicy,
    ) -> Self {
        let motion_key = find_insert_motion_key(&builder).cloned();
        Self {
            table_id,
            schema_version,
            builder,
            conflict_policy,
            motion_key,
        }
    }

    pub(crate) fn insert_encoded_slices<'a, R, I>(
        &self,
        runtime: &R,
        tuples: I,
    ) -> Result<usize, SbroadError>
    where
        R: QueryCache,
        I: IntoIterator<Item = &'a [u8]>,
    {
        run_insert_transaction(|| -> Result<usize, SbroadError> {
            // Keep schema validation and tuple writes in the same transaction so
            // a concurrent DDL cannot slip in between them.
            let space = self.ensure_target_space(runtime)?;
            let mut inserted = 0usize;
            for tuple in tuples {
                if insert_encoded_tuple(&space, tuple, self.conflict_policy)? {
                    inserted = inserted.saturating_add(1);
                }
            }
            Ok(inserted)
        })
    }

    pub(crate) fn insert_vtable<R: Vshard + QueryCache>(
        &self,
        runtime: &R,
        vtable: &VirtualTable,
    ) -> Result<u64, SbroadError> {
        insert_vtable_impl(
            runtime,
            self.table_id,
            self.schema_version,
            self.conflict_policy,
            &self.builder,
            self.motion_key.as_ref(),
            vtable,
        )
    }

    fn ensure_target_space<R: QueryCache>(&self, runtime: &R) -> Result<Space, SbroadError> {
        ensure_target_space(runtime, self.table_id, self.schema_version)
    }

    pub(crate) fn bucket_id_for_row<R: Vshard>(
        &self,
        runtime: &R,
        values: &VTableTuple,
    ) -> Result<Option<u64>, SbroadError> {
        self.motion_key
            .as_ref()
            .map(|motion_key| {
                determine_insert_bucket_id(runtime, self.table_id, values, motion_key)
            })
            .transpose()
    }

    pub(crate) fn encode_row_with_bucket(
        &self,
        values: &VTableTuple,
        bucket_id: Option<&u64>,
    ) -> Result<Vec<u8>, SbroadError> {
        encode_row_with_builder(&self.builder, values, bucket_id)
    }
}

pub(crate) fn insert_vtable<R: Vshard + QueryCache>(
    runtime: &R,
    table_id: u32,
    schema_version: u64,
    conflict_policy: ConflictPolicy,
    builder: &TupleBuilderPattern,
    vtable: &VirtualTable,
) -> Result<u64, SbroadError> {
    insert_vtable_impl(
        runtime,
        table_id,
        schema_version,
        conflict_policy,
        builder,
        find_insert_motion_key(builder),
        vtable,
    )
}

fn insert_vtable_impl<R: Vshard + QueryCache>(
    runtime: &R,
    table_id: u32,
    schema_version: u64,
    conflict_policy: ConflictPolicy,
    builder: &TupleBuilderPattern,
    motion_key: Option<&MotionKey>,
    vtable: &VirtualTable,
) -> Result<u64, SbroadError> {
    let computed_bucket_index = build_bucket_index(runtime, table_id, motion_key, vtable)?;
    let bucket_index = computed_bucket_index
        .as_ref()
        .unwrap_or_else(|| vtable.get_bucket_index());
    let mut row_count = 0u64;

    run_insert_transaction(|| -> Result<(), SbroadError> {
        // Re-check the target schema inside the same transaction that performs
        // the inserts to avoid stale-schema races with concurrent DDL.
        let space = ensure_target_space(runtime, table_id, schema_version)?;
        let mut insert_one = |tuple_data: Vec<u8>| -> Result<(), SbroadError> {
            if insert_encoded_tuple(&space, &tuple_data, conflict_policy)? {
                row_count += 1;
            }
            Ok(())
        };

        if bucket_index.is_empty() {
            for vt_tuple in vtable.get_tuples() {
                insert_one(encode_row_with_builder(builder, vt_tuple, None)?)?;
            }
            return Ok(());
        }

        for (bucket_id, positions) in bucket_index {
            for pos in positions {
                let vt_tuple = vtable.get_tuples().get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::VirtualTable,
                        Some(format_smolstr!(
                            "tuple at position {pos} not found in virtual table"
                        )),
                    )
                })?;
                insert_one(encode_row_with_builder(builder, vt_tuple, Some(bucket_id))?)?;
            }
        }

        Ok(())
    })?;

    Ok(row_count)
}

fn run_insert_transaction<T>(f: impl FnOnce() -> Result<T, SbroadError>) -> Result<T, SbroadError> {
    tarantool::transaction::begin().map_err(begin_insert_transaction_error)?;

    match panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(value)) => {
            tarantool::transaction::commit().map_err(|err| {
                SbroadError::from(TransactionError::<SbroadError>::FailedToCommit(err))
            })?;
            Ok(value)
        }
        Ok(Err(error)) => {
            tarantool::transaction::rollback()
                .map_err(|rollback_err| rollback_insert_transaction_error(&error, rollback_err))?;
            Err(error)
        }
        Err(payload) => {
            if let Err(rollback_err) = tarantool::transaction::rollback() {
                resume_insert_transaction_panic(payload, rollback_err);
            }
            panic::resume_unwind(payload);
        }
    }
}

fn begin_insert_transaction_error(err: tarantool::error::TarantoolError) -> SbroadError {
    SbroadError::Invalid(
        Entity::Transaction,
        Some(format_smolstr!("failed to begin: {err}")),
    )
}

fn rollback_insert_transaction_error(
    primary_error: &SbroadError,
    rollback_err: tarantool::error::TarantoolError,
) -> SbroadError {
    let rollback_error = TransactionError::<SbroadError>::FailedToRollback(rollback_err);
    SbroadError::Invalid(
        Entity::Transaction,
        Some(format_smolstr!(
            "{rollback_error}; primary error: {primary_error}"
        )),
    )
}

fn resume_insert_transaction_panic(
    payload: Box<dyn Any + Send>,
    rollback_err: tarantool::error::TarantoolError,
) -> ! {
    let rollback_error = TransactionError::<SbroadError>::FailedToRollback(rollback_err);
    panic!(
        "insert transaction panicked with {}; rollback also failed: {rollback_error}",
        panic_payload_description(payload.as_ref())
    );
}

fn panic_payload_description(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_owned();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "non-string panic payload".to_owned()
}

fn build_bucket_index<R: Vshard>(
    runtime: &R,
    table_id: u32,
    motion_key: Option<&MotionKey>,
    vtable: &VirtualTable,
) -> Result<Option<HashMap<u64, Vec<usize>, RepeatableState>>, SbroadError> {
    if !vtable.get_bucket_index().is_empty() {
        return Ok(None);
    }

    let Some(motion_key) = motion_key else {
        return Ok(None);
    };

    let mut bucket_index: HashMap<u64, Vec<usize>, RepeatableState> =
        HashMap::with_hasher(RepeatableState);
    for (pos, vt_tuple) in vtable.get_tuples().iter().enumerate() {
        let bucket_id = determine_insert_bucket_id(runtime, table_id, vt_tuple, motion_key)?;
        bucket_index.entry(bucket_id).or_default().push(pos);
    }
    Ok(Some(bucket_index))
}

fn encode_row_with_builder(
    builder: &TupleBuilderPattern,
    values: &VTableTuple,
    bucket_id: Option<&u64>,
) -> Result<Vec<u8>, SbroadError> {
    let mut encoded = Vec::new();
    rmp::encode::write_array_len(&mut encoded, builder.len() as u32).map_err(|e| {
        SbroadError::FailedTo(
            Action::Encode,
            Some(Entity::MsgPack),
            format_smolstr!("{e}"),
        )
    })?;
    write_insert_args(values, builder, bucket_id, &mut encoded)?;
    Ok(encoded)
}

pub(crate) fn ensure_target_space<R: QueryCache>(
    runtime: &R,
    table_id: u32,
    version: u64,
) -> Result<Space, SbroadError> {
    if runtime.get_table_version_by_id(table_id)? != version {
        return Err(SbroadError::OutdatedStorageSchema);
    }

    // SAFETY: `table_id` already exists. Checked by `get_table_version_by_id`.
    Ok(unsafe { Space::from_id_unchecked(table_id) })
}

pub(crate) fn find_insert_motion_key(builder: &TupleBuilderPattern) -> Option<&MotionKey> {
    builder.iter().find_map(|command| match command {
        TupleBuilderCommand::CalculateBucketId(motion_key) if !motion_key.targets.is_empty() => {
            Some(motion_key)
        }
        _ => None,
    })
}

pub(crate) fn determine_insert_bucket_id<R: Vshard>(
    runtime: &R,
    table_id: u32,
    vt_tuple: &VTableTuple,
    motion_key: &MotionKey,
) -> Result<u64, SbroadError> {
    let mut shard_key_tuple = Vec::with_capacity(motion_key.targets.len());
    for target in &motion_key.targets {
        match target {
            Target::Reference(col_idx) => {
                let value = vt_tuple.get(*col_idx).ok_or_else(|| {
                    SbroadError::NotFound(
                        Entity::DistributionKey,
                        missing_distribution_key_detail(
                            table_id,
                            motion_key,
                            *col_idx,
                            vt_tuple.len(),
                        ),
                    )
                })?;
                shard_key_tuple.push(value);
            }
            Target::Value(value) => shard_key_tuple.push(value),
        }
    }

    runtime.determine_bucket_id(&shard_key_tuple)
}

fn distribution_key_column_positions(motion_key: Option<&MotionKey>) -> Vec<usize> {
    motion_key
        .into_iter()
        .flat_map(|motion_key| motion_key.targets.iter())
        .filter_map(|target| match target {
            Target::Reference(col_idx) => Some(*col_idx),
            Target::Value(_) => None,
        })
        .collect()
}

fn missing_distribution_key_detail(
    table_id: u32,
    motion_key: &MotionKey,
    col_idx: usize,
    tuple_len: usize,
) -> smol_str::SmolStr {
    let distribution_key_columns = distribution_key_column_positions(Some(motion_key));
    format_smolstr!(
        "missing distribution key column index {col_idx} for table id {table_id}; distribution key columns {distribution_key_columns:?}; distribution key target count {}; tuple column count {tuple_len}",
        motion_key.targets.len(),
    )
}

pub(crate) fn apply_insert_with_conflict(
    insert_result: Result<(), Error>,
    conflict_strategy: ConflictPolicy,
    replace_on_conflict: impl FnOnce() -> Result<(), SbroadError>,
) -> Result<bool, SbroadError> {
    match insert_result {
        Ok(()) => Ok(true),
        Err(Error::Tarantool(tnt_err))
            if tnt_err.error_code() == TarantoolErrorCode::TupleFound as u32 =>
        {
            match conflict_strategy {
                ConflictPolicy::DoNothing => {
                    tlog!(
                        Debug,
                        "duplicate insert conflict: {tnt_err}. Skipping according to conflict strategy",
                    );
                    Ok(false)
                }
                ConflictPolicy::DoReplace => {
                    tlog!(
                        Debug,
                        "duplicate insert conflict: {tnt_err}. Trying to replace according to conflict strategy"
                    );
                    replace_on_conflict()?;
                    Ok(true)
                }
                ConflictPolicy::DoFail => Err(SbroadError::FailedTo(
                    Action::Insert,
                    Some(Entity::Space),
                    format_smolstr!("{tnt_err}"),
                )),
            }
        }
        Err(e) => Err(SbroadError::FailedTo(
            Action::Insert,
            Some(Entity::Space),
            format_smolstr!("{e}"),
        )),
    }
}

pub(crate) fn insert_encoded_tuple(
    space: &Space,
    tuple_data: &[u8],
    conflict_strategy: ConflictPolicy,
) -> Result<bool, SbroadError> {
    let insert_tuple = RawBytes::new(tuple_data);
    let insert_result = space.insert(insert_tuple).map(|_| ());
    apply_insert_with_conflict(insert_result, conflict_strategy, || {
        space
            .replace(RawBytes::new(tuple_data))
            .map(|_| ())
            .map_err(|e| {
                SbroadError::FailedTo(
                    Action::ReplaceOnConflict,
                    Some(Entity::Space),
                    format_smolstr!("{e}"),
                )
            })
    })
}
