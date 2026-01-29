use crate::catalog::pico_bucket::BucketIdRange;
use crate::schema::IndexDef;
use crate::schema::IndexOption;
use crate::schema::INITIAL_SCHEMA_VERSION;
use crate::storage::index_by_ids_unchecked;
use crate::storage::space_by_id_unchecked;
use crate::storage::SystemTable;
use crate::storage::ToEntryIter;
use crate::storage::MP_SERDE;
use serde::Deserialize;
use serde::Serialize;
use smol_str::SmolStr;
use tarantool::index::FieldType as IndexFieldType;
use tarantool::index::Index;
use tarantool::index::IndexIterator;
use tarantool::index::IndexType;
use tarantool::index::IteratorType;
use tarantool::index::Part;
use tarantool::space::Field;
use tarantool::space::FieldType;
use tarantool::space::Space;
use tarantool::space::SpaceId;
use tarantool::space::SpaceType;
use tarantool::tuple::Encode;

////////////////////////////////////////////////////////////////////////////////
// ReshardingStateRecord
////////////////////////////////////////////////////////////////////////////////

/// Describes contents of the `_pico_resharding_state` system table record.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReshardingStateRecord {
    /// A resharding action is needed in this tier.
    pub tier_name: SmolStr,

    /// Describes the type of action required.
    pub r#type: ReshardingStateRecordType,

    /// A bucket transfer is required from `current_replicaset_name` to `target_replicaset_name`.
    pub current_replicaset_name: SmolStr,

    /// A bucket transfer is required from `current_replicaset_name` to `target_replicaset_name`.
    pub target_replicaset_name: SmolStr,

    /// Marks the start of the bucket id range affected by this resharding state.
    pub bucket_id_start: Option<u64>,

    /// Number of buckets. The meaning depends on `r#type`:
    ///
    /// If `r#type` == [`LeftToTransfer`], then this is the number of buckets
    /// `current_replicaset_name` needs to send to `target_replicaset_name`.
    ///
    /// Otherwise it marks the end of the bucket it range starting at
    /// `bucket_id_start`.
    ///
    /// [`LeftToTransfer`]: ReshardingStateRecordType::LeftToTransfer
    pub bucket_count: u64,
}

impl Encode for ReshardingStateRecord {}

impl ReshardingStateRecord {
    /// Format of the `_pico_resharding_state` global table.
    #[inline]
    pub fn format() -> Vec<::tarantool::space::Field> {
        vec![
            Field::from(("tier_name", FieldType::String)).is_nullable(false),
            Field::from(("type", FieldType::String)).is_nullable(false),
            Field::from(("current_replicaset_name", FieldType::String)).is_nullable(false),
            Field::from(("target_replicaset_name", FieldType::String)).is_nullable(false),
            Field::from(("bucket_id_start", FieldType::Unsigned)).is_nullable(true),
            Field::from(("bucket_count", FieldType::Unsigned)).is_nullable(false),
        ]
    }

    #[inline]
    pub fn bucket_ids(&self) -> Option<BucketIdRange> {
        let bucket_id_start = self.bucket_id_start?;
        let bucket_id_end = bucket_id_start + self.bucket_count - 1;
        Some(bucket_id_start..=bucket_id_end)
    }
}

////////////////////////////////////////////////////////////////////////////////
// PicoReshardingState
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing `_pico_resharding_state` system table contents.
#[derive(Debug, Clone)]
pub struct PicoReshardingState {
    pub space: Space,
    pub index_primary: Index,
}

impl SystemTable for PicoReshardingState {
    const TABLE_NAME: &'static str = "_pico_resharding_state";
    const TABLE_ID: SpaceId = 534;

    fn format() -> Vec<tarantool::space::Field> {
        ReshardingStateRecord::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            // Primary index
            id: 0,
            name: "_pico_resharding_state_index_primary".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![
                Part::from(("tier_name", IndexFieldType::String)).is_nullable(false),
                Part::from(("type", IndexFieldType::String)).is_nullable(false),
                Part::from(("current_replicaset_name", IndexFieldType::String)).is_nullable(false),
                Part::from(("target_replicaset_name", IndexFieldType::String)).is_nullable(false),
            ],
            operable: true,
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
        }]
    }
}

impl PicoReshardingState {
    pub const SQL_CREATE: &'static str = "
        CREATE TABLE _pico_resharding_state (
            tier_name                TEXT      NOT NULL,
            type                     TEXT      NOT NULL,
            current_replicaset_name  TEXT      NOT NULL,
            target_replicaset_name   TEXT      NOT NULL,
            bucket_id_start          UNSIGNED  NULL,
            bucket_count             UNSIGNED  NOT NULL,

            PRIMARY KEY (tier_name, type, current_replicaset_name, target_replicaset_name)
        )
        DISTRIBUTED GLOBALLY
    ";

    pub const fn new() -> Self {
        Self {
            space: space_by_id_unchecked(Self::TABLE_ID),
            index_primary: index_by_ids_unchecked(Self::TABLE_ID, 0),
        }
    }

    pub fn create(&self) -> tarantool::Result<()> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index_primary = space
            .index_builder("_pico_resharding_state_index_primary")
            .unique(true)
            .part("tier_name")
            .part("type")
            .part("current_replicaset_name")
            .part("target_replicaset_name")
            .if_not_exists(true)
            .create()?;

        debug_assert_eq!(self.space.id(), space.id());
        debug_assert_eq!(self.index_primary.id(), index_primary.id());

        Ok(())
    }
}

impl Default for PicoReshardingState {
    fn default() -> Self {
        Self::new()
    }
}

impl ToEntryIter<MP_SERDE> for PicoReshardingState {
    type Entry = ReshardingStateRecord;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// ReshardingStateRecordType
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ReshardingStateRecordType {
    LeftToTransfer,
    LockForSending,
    LockForDeletion,
    Cleanup,
    /// For future compatibility
    Unknown(SmolStr),
}

impl Default for ReshardingStateRecordType {
    #[inline]
    fn default() -> Self {
        Self::Unknown(SmolStr::new_static("unknown"))
    }
}

impl ReshardingStateRecordType {
    pub fn as_str(&self) -> &str {
        match self {
            Self::LeftToTransfer => "left-to-transfer",
            Self::LockForSending => "lock-for-sending",
            Self::LockForDeletion => "lock-for-deletion",
            Self::Cleanup => "cleanup",
            Self::Unknown(s) => s,
        }
    }
}

impl From<ReshardingStateRecordType> for SmolStr {
    fn from(state: ReshardingStateRecordType) -> Self {
        type T = ReshardingStateRecordType;
        match state {
            T::LeftToTransfer => SmolStr::new_static("left-to-transfer"),
            T::LockForSending => SmolStr::new_static("lock-for-sending"),
            T::LockForDeletion => SmolStr::new_static("lock-for-deletion"),
            T::Cleanup => SmolStr::new_static("cleanup"),
            T::Unknown(s) => s,
        }
    }
}

impl std::str::FromStr for ReshardingStateRecordType {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let res = match s {
            "left-to-transfer" => Self::LeftToTransfer,
            "lock-for-sending" => Self::LockForSending,
            "lock-for-deletion" => Self::LockForDeletion,
            "cleanup" => Self::Cleanup,
            unknown => Self::Unknown(unknown.into()),
        };
        Ok(res)
    }
}

impl serde::Serialize for ReshardingStateRecordType {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for ReshardingStateRecordType {
    #[inline]
    fn deserialize<D>(deserializer: D) -> ::std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(tarantool::define_str_enum::FromStrVisitor::<Self>::default())
    }
}
