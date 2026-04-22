use smol_str::SmolStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum CopyTargetError {
    #[error("table does not exist: {table}")]
    TableDoesNotExist { table: SmolStr },

    #[error("column \"{column}\" specified more than once")]
    DuplicateColumn { column: SmolStr },

    #[error("column does not exist: {column}")]
    ColumnDoesNotExist { column: SmolStr },

    #[error("system column \"{column}\" cannot be inserted")]
    SystemColumnInsertNotAllowed { column: SmolStr },

    #[error("NonNull column \"{column}\" must be specified")]
    MissingRequiredColumn { column: SmolStr },

    #[error("{0}")]
    Internal(SmolStr),

    #[error("feature is not supported: {0}")]
    FeatureNotSupported(SmolStr),

    #[error("sharded COPY expected a computed bucket_id")]
    MissingBucketId,

    #[error("no bucket route for bucket {bucket_id} in tier {tier_name}")]
    MissingBucketRoute { tier_name: SmolStr, bucket_id: u64 },

    #[error("tier does not exist: {tier_name}")]
    NoSuchTier { tier_name: SmolStr },

    #[error(
        "bucket routing changed during execution in tier {tier_name}: prepared version {prepared_bucket_state_version}, live current {live_current_bucket_state_version}, live target {live_target_bucket_state_version}"
    )]
    BucketRoutingStale {
        tier_name: SmolStr,
        prepared_bucket_state_version: u64,
        live_current_bucket_state_version: u64,
        live_target_bucket_state_version: u64,
    },

    #[error(
        "cannot start sharded COPY in tier {tier_name}: bucket rebalancing is in progress, current version {current_bucket_state_version}, target version {target_bucket_state_version}"
    )]
    BucketRebalancingInProgress {
        tier_name: SmolStr,
        current_bucket_state_version: u64,
        target_bucket_state_version: u64,
    },

    #[error(transparent)]
    Picodata(#[from] crate::traft::error::Error),

    #[error(transparent)]
    Storage(#[from] sql::errors::SbroadError),

    #[error(transparent)]
    Tarantool(#[from] tarantool::error::Error),
}

impl CopyTargetError {
    pub(super) fn table_does_not_exist(table: &SmolStr) -> Self {
        Self::TableDoesNotExist {
            table: table.clone(),
        }
    }

    pub(super) fn duplicate_column(column: &SmolStr) -> Self {
        Self::DuplicateColumn {
            column: column.clone(),
        }
    }

    pub(super) fn column_does_not_exist(column: &SmolStr) -> Self {
        Self::ColumnDoesNotExist {
            column: column.clone(),
        }
    }

    pub(super) fn system_column_insert_not_allowed(column: impl AsRef<str>) -> Self {
        Self::SystemColumnInsertNotAllowed {
            column: column.as_ref().into(),
        }
    }

    pub(super) fn missing_required_column(column: impl AsRef<str>) -> Self {
        Self::MissingRequiredColumn {
            column: column.as_ref().into(),
        }
    }

    pub(crate) fn internal(message: impl Into<SmolStr>) -> Self {
        Self::Internal(message.into())
    }
}
