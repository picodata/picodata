use crate::schema::IndexDef;
use crate::schema::IndexOption;
use crate::schema::INITIAL_SCHEMA_VERSION;
use crate::storage::index_by_ids_unchecked;
use crate::storage::space_by_id_unchecked;
use crate::storage::SystemTable;
use crate::storage::ToEntryIter;
use crate::storage::MP_SERDE;
use crate::tier::Tier;
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

/// The first bucket_id
pub const BUCKET_ID_MIN: u64 = 1;
/// Column name in which sharded tables store the bucket_id by default.
pub const DEFAULT_BUCKET_ID_COLUMN_NAME: &'static str = "bucket_id";

pub type BucketIdRange = std::ops::RangeInclusive<u64>;

////////////////////////////////////////////////////////////////////////////////
// BucketRecord
////////////////////////////////////////////////////////////////////////////////

/// This struct describes the contents of a `_pico_bucket` system table tuple.
///
/// More specifically each tuple in `_pico_bucket` describes a contiguous range
/// of buckets of the same ownership and state.
///
/// This is different from vanilla vshard's `_bucket` space, which contains a
/// tuple for each bucket, because picodata's `_pico_bucket` stores info for
/// each replicaset of each tier in the cluster whereas `_bucket` only stores
/// info for the current replicaset. Which means that we need to store
/// potentially much more data, so some optimization is needed.
#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketRecord {
    /// Which tier the buckets belong to.
    pub tier_name: SmolStr,

    /// Id of first bucket in the range.
    pub bucket_id_start: u64,

    /// Id of last bucket in the range.
    pub bucket_id_end: u64,

    /// Current state of the buckets in this range.
    ///
    /// If `state` is `Active` then `target_replicaset_name` must be `None`.
    pub state: BucketState,

    /// This replicaset currently owns the buckets in this range.
    ///
    /// If `target_replicaset_name` is not `None` then buckets are currently
    /// being transferred between the two replicasets.
    pub current_replicaset_name: SmolStr,

    /// This replicaset is the next ownere of the buckets in this range.
    ///
    /// `None` is equivalent to `target_replicaset_name == current_replicaset_name`.
    pub target_replicaset_name: Option<SmolStr>,
}

impl BucketRecord {
    /// All ranges are non-overlapping, therefore each tuple is uniquely
    /// identifed by a tier name and the start of the range.
    #[inline(always)]
    pub fn primary_key(&self) -> (&str, u64) {
        (&self.tier_name, self.bucket_id_start)
    }

    /// Returns number of buckets in the range.
    #[inline(always)]
    pub fn count(&self) -> u64 {
        self.bucket_id_end - self.bucket_id_start + 1
    }

    #[inline(always)]
    pub fn target_replicaset_name(&self) -> &SmolStr {
        if let Some(target_replicaset_name) = &self.target_replicaset_name {
            target_replicaset_name
        } else {
            &self.current_replicaset_name
        }
    }

    /// Returns `self.bucket_id_start..=self.bucket_id_end`
    #[inline(always)]
    pub fn bucket_ids(&self) -> BucketIdRange {
        self.bucket_id_start..=self.bucket_id_end
    }

    /// Returns `true` if reading data from these buckets is allowed on `replicaset_name`.
    #[inline]
    pub fn can_read_on(&self, replicaset_name: &str) -> bool {
        self.current_replicaset_name == replicaset_name
            && matches!(
                self.state,
                BucketState::Active | BucketState::Sending | BucketState::Sent
            )
    }

    /// Returns `true` if writing data into these buckets is allowed on `replicaset_name`.
    #[inline]
    pub fn can_write_on(&self, replicaset_name: &str) -> bool {
        self.current_replicaset_name == replicaset_name && matches!(self.state, BucketState::Active)
    }

    #[inline]
    pub fn is_transfering_to_or_from(&self, replicaset_name: &str) -> bool {
        matches!(self.state, BucketState::Sending | BucketState::Sent)
            && (self.current_replicaset_name == replicaset_name
                || self.target_replicaset_name.as_deref() == Some(replicaset_name))
    }
}

impl Encode for BucketRecord {}

impl BucketRecord {
    /// Format of the `_pico_bucket` global table.
    #[inline(always)]
    pub fn format() -> Vec<::tarantool::space::Field> {
        vec![
            Field::from(("tier_name", FieldType::String)).is_nullable(false),
            Field::from(("bucket_id_start", FieldType::Unsigned)).is_nullable(false),
            Field::from(("bucket_id_end", FieldType::Unsigned)).is_nullable(false),
            Field::from(("state", FieldType::String)).is_nullable(false),
            Field::from(("current_replicaset_name", FieldType::String)).is_nullable(false),
            Field::from(("target_replicaset_name", FieldType::String)).is_nullable(true),
        ]
    }

    #[inline]
    pub fn initial_distribution_for(tier: &Tier, replicaset_name: &str) -> Self {
        Self {
            tier_name: SmolStr::new(&tier.name),
            bucket_id_start: BUCKET_ID_MIN,
            bucket_id_end: tier.bucket_count,
            state: BucketState::Active,
            current_replicaset_name: SmolStr::new(replicaset_name),
            target_replicaset_name: None,
        }
    }
}

impl std::fmt::Display for BucketRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "BucketRecord(")?;
        write!(f, "{}, ", self.tier_name)?;
        if self.bucket_id_start == self.bucket_id_end {
            write!(f, "{}, ", self.bucket_id_start)?;
        } else {
            write!(f, "{}..{}, ", self.bucket_id_start, self.bucket_id_end)?;
        }
        write!(f, "{}, ", self.state.as_str())?;
        if let Some(target_replicaset_name) = &self.target_replicaset_name {
            write!(
                f,
                "{} -> {}",
                self.current_replicaset_name, target_replicaset_name
            )?;
        } else {
            write!(f, "{}", self.current_replicaset_name)?;
        }

        write!(f, ")")?;

        Ok(())
    }
}

impl std::fmt::Debug for BucketRecord {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

////////////////////////////////////////////////////////////////////////////////
// BucketState
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub enum BucketState {
    /// The bucket(s) are fully operable and data is stored only on the owner.
    ///
    /// Reading and writing data in such buckets is allowed.
    ///
    /// Legal transitions:
    /// - `Active` -> `Sending`
    /// - `Sent` -> `Active`
    #[default]
    Active,

    /// The bucket(s) are being sent to the new owner.
    ///
    /// Nobody is allowed to write data in such buckets.
    /// Reading is allowed.
    ///
    /// Legal transitions:
    /// - `Active` -> `Sending`
    /// - `Sending` -> `Sent`
    Sending,

    /// The bucket(s) is fully sent to the new owner, but there's still a full
    /// copy of the data on the old owner.
    ///
    /// Nobody is allowed to write data in such buckets.
    /// Nobody is allowed to read data in such buckets unless they've been
    /// reading it before the state changed to `Sent`.
    ///
    /// Legal transitions:
    /// - `Sending` -> `Sent`
    /// - `Sent` -> `Active`
    Sent,

    /// For future compatibility
    Unknown(SmolStr),
}

impl BucketState {
    pub fn as_str(&self) -> &str {
        match self {
            BucketState::Active => "active",
            BucketState::Sending => "sending",
            BucketState::Sent => "sent",
            BucketState::Unknown(s) => s,
        }
    }

    #[inline(always)]
    pub fn is_sending(&self) -> bool {
        matches!(self, BucketState::Sending)
    }

    #[inline(always)]
    pub fn is_sent(&self) -> bool {
        matches!(self, BucketState::Sent)
    }

    #[inline(always)]
    pub fn is_active(&self) -> bool {
        matches!(self, BucketState::Active)
    }
}

impl From<BucketState> for SmolStr {
    fn from(state: BucketState) -> Self {
        match state {
            BucketState::Active => SmolStr::new_static("active"),
            BucketState::Sending => SmolStr::new_static("sending"),
            BucketState::Sent => SmolStr::new_static("sent"),
            BucketState::Unknown(s) => s,
        }
    }
}

impl std::str::FromStr for BucketState {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let res = match s {
            "active" => Self::Active,
            "sending" => Self::Sending,
            "sent" => Self::Sent,
            unknown => Self::Unknown(unknown.into()),
        };
        Ok(res)
    }
}

impl serde::Serialize for BucketState {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for BucketState {
    #[inline]
    fn deserialize<D>(deserializer: D) -> ::std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(tarantool::define_str_enum::FromStrVisitor::<Self>::default())
    }
}

////////////////////////////////////////////////////////////////////////////////
// PicoBucket
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing `_pico_bucket` system table contents.
#[derive(Debug, Clone)]
pub struct PicoBucket {
    pub space: Space,
    pub index_primary: Index,
}

impl SystemTable for PicoBucket {
    const TABLE_NAME: &'static str = "_pico_bucket";
    const TABLE_ID: SpaceId = 533;

    fn format() -> Vec<tarantool::space::Field> {
        BucketRecord::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![IndexDef {
            table_id: Self::TABLE_ID,
            // Primary index
            id: 0,
            name: "_pico_bucket_index_primary".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![
                Part::from(("tier_name", IndexFieldType::String)).is_nullable(false),
                Part::from(("bucket_id_start", IndexFieldType::Unsigned)).is_nullable(false),
            ],
            operable: true,
            // This means the local schema is already up to date and main loop doesn't need to do anything
            schema_version: INITIAL_SCHEMA_VERSION,
        }]
    }
}

impl PicoBucket {
    pub const SQL_CREATE: &'static str = "
        CREATE TABLE _pico_bucket (
            tier_name               TEXT      NOT NULL,
            bucket_id_start         UNSIGNED  NOT NULL,
            bucket_id_end           UNSIGNED  NOT NULL,
            state                   TEXT      NOT NULL,
            current_replicaset_name TEXT      NOT NULL,
            target_replicaset_name  TEXT      NULL,

            PRIMARY KEY (tier_name, bucket_id_start)
        )
        DISTRIBUTED GLOBALLY
    ";

    pub fn new() -> tarantool::Result<Self> {
        Ok(Self {
            space: space_by_id_unchecked(Self::TABLE_ID),
            index_primary: index_by_ids_unchecked(Self::TABLE_ID, 0),
        })
    }

    pub fn create(&self) -> tarantool::Result<()> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index_primary = space
            .index_builder("_pico_bucket_index_primary")
            .unique(true)
            .part("tier_name")
            .part("bucket_id_start")
            .if_not_exists(true)
            .create()?;

        debug_assert_eq!(self.space.id(), space.id());
        debug_assert_eq!(self.index_primary.id(), index_primary.id());

        Ok(())
    }
}

impl ToEntryIter<MP_SERDE> for PicoBucket {
    type Entry = BucketRecord;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}

////////////////////////////////////////////////////////////////////////////////
// miscellaneous
////////////////////////////////////////////////////////////////////////////////

/// Returns the index of last primary key part + 1.
pub fn index_of_bucket_id_column(
    primary_index_def: &IndexDef,
    format: &[tarantool::space::Field],
) -> u32 {
    let mut last_pk_part_index = 0;
    for part in &primary_index_def.parts {
        let name = &part.field;
        let field_index = format.iter().zip(0..).find(|(f, _)| f.name == *name);
        let Some((_, index)) = field_index else {
            crate::warn_or_panic!("invalid primary key part: field '{name}' not found");
            continue;
        };
        last_pk_part_index = last_pk_part_index.max(index);
    }

    (last_pk_part_index + 1) as _
}
