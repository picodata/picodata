#[path = "target_error.rs"]
mod error;
#[path = "target_prepare.rs"]
mod prepare;

use super::pending::{PendingCopyBatch, ShardedCopyDestination};
use super::routing::{CopyWriteMode, ShardedCopyRouting};
use crate::cas;
use crate::schema::ADMIN_ID;
use crate::sql::direct_insert::PreparedDirectInsert;
use crate::sql::local_ref::with_local_bucket_ref;
use crate::storage::Catalog;
use crate::traft::op::Dml;
use crate::util::effective_user_id;
use smol_str::SmolStr;
use sql::executor::engine::protocol::InsertCoreData;
use sql::executor::engine::Vshard;
use sql::executor::result::ConsumerResult;
use sql::executor::vtable::VTableTuple;
use sql::ir::operator::ConflictStrategy;
use sql::ir::types::UnrestrictedType as SbroadType;
use sql_protocol::dml::insert::ConflictPolicy;
use std::collections::HashMap;
use tarantool::session::with_su;
use tarantool::tuple::RawByteBuf;

pub(crate) use error::CopyTargetError;
pub(crate) use prepare::prepare_copy_target;

#[derive(Debug)]
pub(crate) struct PreparedCopyTarget {
    table_id: u32,
    table_name: SmolStr,
    field_types: Vec<SbroadType>,
    conflict_policy: ConflictPolicy,
    insert: PreparedDirectInsert,
    write_mode: CopyWriteMode,
}

impl PreparedCopyTarget {
    pub(crate) fn table_name(&self) -> &SmolStr {
        &self.table_name
    }

    pub(crate) fn field_types(&self) -> &[SbroadType] {
        &self.field_types
    }

    pub(crate) fn is_global(&self) -> bool {
        matches!(&self.write_mode, CopyWriteMode::Global)
    }

    pub(crate) fn prepare_pending_row<R: Vshard>(
        &self,
        runtime: &R,
        values: &VTableTuple,
    ) -> Result<(Vec<u8>, Option<ShardedCopyDestination>), CopyTargetError> {
        let bucket_id = self.insert.bucket_id_for_row(runtime, values)?;
        let encoded_row = self
            .insert
            .encode_row_with_bucket(values, bucket_id.as_ref())?;
        match &self.write_mode {
            CopyWriteMode::Global => Ok((encoded_row, None)),
            CopyWriteMode::Sharded(_) => {
                let destination = self.destination_for_bucket(bucket_id)?;
                Ok((encoded_row, Some(destination)))
            }
        }
    }

    pub(crate) fn flush_sharded_batches<'a>(
        &self,
        runtime: &crate::sql::storage::StorageRuntime,
        destinations: impl IntoIterator<Item = (&'a ShardedCopyDestination, &'a PendingCopyBatch)>,
    ) -> Result<usize, CopyTargetError> {
        let mut destinations = destinations
            .into_iter()
            .filter(|(_, batch)| !batch.is_empty());
        let Some(first_destination) = destinations.next() else {
            return Ok(0);
        };
        self.ensure_operable()?;
        let CopyWriteMode::Sharded(routing) = &self.write_mode else {
            return Err(CopyTargetError::internal(
                "sharded COPY batch requires sharded routing",
            ));
        };
        routing.ensure_current()?;

        let mut row_count = 0usize;
        let mut remote_batches = None;
        for (destination, batch) in std::iter::once(first_destination).chain(destinations) {
            match destination {
                ShardedCopyDestination::Local => {
                    row_count = row_count
                        .saturating_add(self.insert_sharded_local_batch(runtime, batch, routing)?);
                }
                ShardedCopyDestination::Replicaset(replicaset_uuid) => {
                    remote_batches
                        .get_or_insert_with(HashMap::new)
                        .insert(replicaset_uuid.to_string(), batch.encoded_rows.as_slice());
                }
            }
        }

        if let Some(remote_batches) = remote_batches {
            routing.ensure_current()?;
            row_count =
                row_count.saturating_add(self.dispatch_remote_batches(remote_batches, routing)?);
        }

        Ok(row_count)
    }

    pub(super) fn destination_for_bucket(
        &self,
        bucket_id: Option<u64>,
    ) -> Result<ShardedCopyDestination, CopyTargetError> {
        match &self.write_mode {
            CopyWriteMode::Global => Err(CopyTargetError::internal(
                "global COPY does not have bucket destinations",
            )),
            CopyWriteMode::Sharded(routing) => {
                let bucket_id = bucket_id.ok_or(CopyTargetError::MissingBucketId)?;
                routing.destination_for_bucket(bucket_id)
            }
        }
    }

    fn ensure_operable(&self) -> Result<(), CopyTargetError> {
        let storage = Catalog::try_get(false).expect("storage should be initialized");
        with_su(ADMIN_ID, || {
            cas::check_table_operable(storage, self.table_id)
        })??;
        Ok(())
    }

    fn insert_sharded_local_batch(
        &self,
        runtime: &crate::sql::storage::StorageRuntime,
        batch: &PendingCopyBatch,
        routing: &ShardedCopyRouting,
    ) -> Result<usize, CopyTargetError> {
        with_local_bucket_ref(routing.dispatch_timeout, "leader", || {
            self.insert
                .insert_encoded_slices(runtime, batch.encoded_rows.iter().map(Vec::as_slice))
        })
        .map_err(CopyTargetError::from)
    }

    fn dispatch_remote_batches(
        &self,
        remote_batches: HashMap<String, &[Vec<u8>]>,
        routing: &ShardedCopyRouting,
    ) -> Result<usize, CopyTargetError> {
        let remote_row_count = crate::sql::dispatch::dispatch_encoded_insert_batches(
            InsertCoreData {
                request_id: uuid::Uuid::new_v4().to_string().into(),
                space_id: self.table_id,
                space_version: routing.schema_version,
                conflict_policy: self.conflict_policy,
            },
            remote_batches,
            Some(routing.tier_name.as_str()),
            routing.dispatch_timeout,
        )?;
        Ok(remote_row_count as usize)
    }

    pub(crate) fn flush_global_batch(
        &self,
        batch: &PendingCopyBatch,
    ) -> Result<usize, CopyTargetError> {
        if batch.is_empty() {
            return Ok(0);
        }
        self.ensure_operable()?;
        let current_user = effective_user_id();
        let conflict_strategy = conflict_strategy_from_policy(self.conflict_policy);
        let row_count = batch.rows();
        let mut ops = Vec::with_capacity(row_count);
        for row in &batch.encoded_rows {
            let row = RawByteBuf::from(row.clone());
            ops.push(Dml::insert_with_on_conflict(
                self.table_id,
                &row,
                current_user,
                conflict_strategy,
            )?);
        }

        let ConsumerResult { row_count } = crate::sql::execute_global_dml_batch_with_retries(
            current_user,
            ops,
            conflict_strategy,
            row_count,
            None,
        )?;
        Ok(row_count as usize)
    }

    #[cfg(test)]
    pub(super) fn for_test_with_write_mode(write_mode: CopyWriteMode) -> Self {
        Self {
            table_id: 1,
            table_name: "test".into(),
            field_types: Vec::new(),
            conflict_policy: ConflictPolicy::DoFail,
            insert: PreparedDirectInsert::new(1, 1, Vec::new(), ConflictPolicy::DoFail),
            write_mode,
        }
    }
}

fn conflict_strategy_from_policy(policy: ConflictPolicy) -> ConflictStrategy {
    match policy {
        ConflictPolicy::DoNothing => ConflictStrategy::DoNothing,
        ConflictPolicy::DoReplace => ConflictStrategy::DoReplace,
        ConflictPolicy::DoFail => ConflictStrategy::DoFail,
    }
}
